// Features around the capture, compared with sqlite-sync's own triggers: rollbacks and savepoints, foreign-key actions, triggers that write other tracked tables, upserts,
// updates that change nothing, primary-key updates, values of every type, a table with 80 columns, a value that spills to overflow pages. The same script runs on a sqlite-sync
// database and on a multi-writer one; afterwards every cell (column, version) of every row of every table must be the same. (A row changed twice in one transaction is the one thing
// that differs by design: sqlite-sync counts a version per statement, we count one per commit; the scripts avoid it.)
#include <stdint.h>
#include <stdbool.h>
#include "mw_test.h"
#include "multiwriter.h"
#include "multiwriter_meta.h"
#include "multiwriter_catalog.h"
#include "crdt.h"

extern int sqlite3_cloudsync_init (sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi);

static const char *SCHEMA =
    "CREATE TABLE p(id TEXT PRIMARY KEY NOT NULL, a TEXT, b INTEGER);"
    "CREATE TABLE ch(id TEXT PRIMARY KEY NOT NULL, pid TEXT REFERENCES p(id) ON DELETE CASCADE ON UPDATE CASCADE, v INTEGER);"
    "CREATE TABLE lg(id TEXT PRIMARY KEY NOT NULL, what TEXT);"
    "CREATE TRIGGER p_ins AFTER INSERT ON p BEGIN INSERT INTO lg(id, what) VALUES('ins_' || NEW.id, NEW.a); END;"
    "CREATE TABLE ty(id TEXT PRIMARY KEY NOT NULL, i INTEGER, r REAL, t TEXT, bl BLOB, n);"
    "CREATE TABLE big(id TEXT PRIMARY KEY NOT NULL, v TEXT, w INTEGER);";
static char *wide_ddl (void) { static char b[4000]; char *p = b; p += sprintf(p, "CREATE TABLE wd(id TEXT PRIMARY KEY NOT NULL"); for (int i = 0; i < 80; i++) p += sprintf(p, ", c%d INTEGER", i); sprintf(p, ")"); return b; }
static const char *TABLES[] = { "p", "ch", "lg", "ty", "big", "wd" };
#define NT 6

static int cmp_s (const void *a, const void *b) { return strcmp(*(char *const *)a, *(char *const *)b); }
// all cells of a table as sorted "hex(pk)|col|cv", from sqlite-sync's meta table and from ours (ids -> names through the schema)
static int oracle_cells (sqlite3 *o, const char *tab, char **out, int max) {
    char sql[200]; snprintf(sql, sizeof sql, "SELECT hex(pk), col_name, col_version FROM %s_cloudsync", tab);
    sqlite3_stmt *st; sqlite3_prepare_v2(o, sql, -1, &st, NULL); int n = 0;
    while (sqlite3_step(st) == SQLITE_ROW && n < max) { char b[200]; snprintf(b, sizeof b, "%s|%s|%lld", sqlite3_column_text(st, 0), sqlite3_column_text(st, 1), (long long)sqlite3_column_int64(st, 2)); out[n++] = strdup(b); }
    sqlite3_finalize(st); qsort(out, (size_t)n, sizeof *out, cmp_s); return n;
}
static int our_cells (sqlite3 *p, mw_meta *m, const char *tab, char **out, int max) {
    // the rows that exist, plus the ones that were deleted (their sentinel stays): ask the file tables after a flush
    mw_meta_flush(m);
    sqlite3_stmt *cols; char q[200]; snprintf(q, sizeof q, "PRAGMA table_info(%s)", tab); sqlite3_prepare_v2(p, q, -1, &cols, NULL);
    char *names[100]; uint32_t ids[100]; int nn = 0;
    while (sqlite3_step(cols) == SQLITE_ROW && nn < 100) { names[nn] = strdup((const char *)sqlite3_column_text(cols, 1)); ids[nn] = mw_name_id(names[nn]); nn++; }
    sqlite3_finalize(cols);
    sqlite3_stmt *st; sqlite3_prepare_v2(p, "SELECT hex(pk), col, cv FROM mw_cells WHERE tbl = ?1", -1, &st, NULL); sqlite3_bind_int64(st, 1, mw_name_id(tab));
    int n = 0;
    while (sqlite3_step(st) == SQLITE_ROW && n < max) {
        int64_t col = sqlite3_column_int64(st, 1); const char *name = col == -1 ? CRDT_SENTINEL : "?";
        for (int i = 0; i < nn && col != -1; i++) if (ids[i] == (uint32_t)col) name = names[i];
        char b[200]; snprintf(b, sizeof b, "%s|%s|%lld", sqlite3_column_text(st, 0), name, (long long)sqlite3_column_int64(st, 2)); out[n++] = strdup(b);
    }
    sqlite3_finalize(st); for (int i = 0; i < nn; i++) free(names[i]);
    qsort(out, (size_t)n, sizeof *out, cmp_s); return n;
}
static int compare_all (sqlite3 *o, sqlite3 *p, mw_meta *m, const char *when) {
    int bad = 0;
    for (int t = 0; t < NT; t++) {
        char *a[1200], *b[1200]; int na = oracle_cells(o, TABLES[t], a, 1200), nb = our_cells(p, m, TABLES[t], b, 1200);
        bool same = na == nb; for (int i = 0; same && i < na; i++) if (strcmp(a[i], b[i])) same = false;
        if (!same) { bad++; printf("  %s: table %s differs (%d vs %d cells)\n", when, TABLES[t], na, nb);
            for (int i = 0, sh = 0; i < na && sh < 5; i++) { bool f = false; for (int j = 0; j < nb; j++) if (!strcmp(a[i], b[j])) f = true; if (!f) { printf("      only sqlite-sync: %s\n", a[i]); sh++; } }
            for (int j = 0, sh = 0; j < nb && sh < 5; j++) { bool f = false; for (int i = 0; i < na; i++) if (!strcmp(a[i], b[j])) f = true; if (!f) { printf("      only ours:        %s\n", b[j]); sh++; } } }
        for (int i = 0; i < na; i++) free(a[i]); for (int i = 0; i < nb; i++) free(b[i]);
    }
    return bad;
}

typedef struct { const char *name; const char *sql; } step;
static const step STEPS[] = {
    { "inserts, trigger writes the log table", "INSERT INTO p VALUES('p1','one',1),('p2','two',2),('p3','three',3)" },
    { "rolled back transaction", "BEGIN; INSERT INTO p VALUES('zz','gone',9); UPDATE p SET a='nope' WHERE id='p1'; ROLLBACK" },
    { "savepoint rolled back, the rest committed", "BEGIN; INSERT INTO p VALUES('p4','four',4); SAVEPOINT s1; INSERT INTO p VALUES('p5','five',5); DELETE FROM p WHERE id='p2'; ROLLBACK TO s1; RELEASE s1; COMMIT" },
    { "children", "INSERT INTO ch VALUES('c1','p1',10),('c2','p1',20),('c3','p3',30),('c4','p4',40)" },
    { "foreign key cascade delete", "DELETE FROM p WHERE id='p1'" },
    { "foreign key cascade update of the key", "UPDATE p SET id='p3x' WHERE id='p3'" },
    { "an update that changes nothing", "UPDATE p SET a = a, b = b WHERE id='p2'" },
    { "upsert (insert path)", "INSERT INTO p VALUES('p9','nine',9) ON CONFLICT(id) DO UPDATE SET a='changed'" },
    { "upsert (update path)", "INSERT INTO p VALUES('p9','again',99) ON CONFLICT(id) DO UPDATE SET a='changed', b=b+1" },
    { "values of every type", "INSERT INTO ty VALUES('t1', 42, 3.25, 'text', x'00ff10', NULL), ('t2', -7, -0.5, '', x'', 5)" },
    { "values change type, to NULL", "UPDATE ty SET i = 'now text', r = NULL, t = NULL, bl = 'blob as text', n = 1.5 WHERE id='t1'" },
    { "primary key updates", "UPDATE ty SET id = 't1b' WHERE id = 't1'" },
    { "delete and insert again", "DELETE FROM ty WHERE id='t2'" },
    { "(again)", "INSERT INTO ty VALUES('t2', 1, 1.0, 'back', x'01', NULL)" },
    { "a value in overflow pages", "INSERT INTO big VALUES('b1', hex(randomblob(40000)), 1)" },
    { "update of another column of it", "UPDATE big SET w = 2 WHERE id='b1'" },
    { "update of the overflow value", "UPDATE big SET v = hex(randomblob(50000)) WHERE id='b1'" },
    { "an 80-column table", "INSERT INTO wd(id, c0, c40, c79) VALUES('w1', 1, 2, 3)" },
    { "update of some of its columns", "UPDATE wd SET c1 = 5, c64 = 6, c79 = 7 WHERE id='w1'" },
};

int main (void) {
    char path[256]; mw_tmpdb(path, sizeof path, "ofeat");
    sqlite3 *s; CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL"), SQLITE_OK); CHECK_RC(mw_exec(s, SCHEMA), SQLITE_OK); CHECK_RC(mw_exec(s, wide_ddl()), SQLITE_OK); sqlite3_close(s);
    char uri[300]; snprintf(uri, sizeof uri, "file:%s?mw=2&mw_cdc=1%s", path, getenv("MW_TEST_MP") ? "&mw_mp=1" : "");
    sqlite3 *p; CHECK_RC(sqlite3_open_v2(uri, &p, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL), SQLITE_OK);
    mw_meta *m = NULL; CHECK_RC(sqlite3_file_control(p, "main", MW_FCNTL_META, &m), SQLITE_OK);
    sqlite3 *o; sqlite3_open(":memory:", &o); sqlite3_cloudsync_init(o, NULL, NULL);
    CHECK_RC(mw_exec(o, "PRAGMA foreign_keys=ON"), SQLITE_OK); CHECK_RC(mw_exec(p, "PRAGMA foreign_keys=ON"), SQLITE_OK);
    CHECK_RC(mw_exec(o, SCHEMA), SQLITE_OK); CHECK_RC(mw_exec(o, wide_ddl()), SQLITE_OK);
    for (int t = 0; t < NT; t++) { char q[100]; snprintf(q, sizeof q, "SELECT cloudsync_init('%s')", TABLES[t]); CHECK_RC(mw_exec(o, q), SQLITE_OK); }
    int total_bad = 0;
    for (size_t i = 0; i < sizeof STEPS / sizeof *STEPS; i++) {
        int ro = mw_exec(o, STEPS[i].sql), rp = mw_exec(p, STEPS[i].sql);
        if (ro != rp) { printf("  step '%s': oracle rc %d, ours rc %d\n", STEPS[i].name, ro, rp); total_bad++; }
        int bad = compare_all(o, p, m, STEPS[i].name);
        printf("%-52s %s\n", STEPS[i].name, bad ? "DIFFERENT" : "same cells");
        total_bad += bad;
    }
    CHECK(total_bad == 0);
    sqlite3_close(p); sqlite3_close(o); mw_rmdb(path);
    MW_DONE();
}
