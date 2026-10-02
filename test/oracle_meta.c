// Differential test of the live metadata store against sqlite-sync. The same random statements run on a real sqlite-sync database (metadata by triggers) and on a database opened
// through the Multi-Writer VFS (metadata captured from the pages): after every transaction the cells of every row (column id, column version) must be identical. Each transaction
// touches a row at most once (sqlite-sync counts versions per statement, we count them per commit: the same row changed twice in a transaction is the one place they differ).
#include <stdint.h>
#include <stdbool.h>
#include "mw_test.h"
#include "multiwriter.h"
#include "multiwriter_meta.h"
#include "multiwriter_catalog.h"
#include "crdt.h"

extern int sqlite3_cloudsync_init (sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi);
static uint64_t rng = 0x2545F4914F6CDD1Dull;
static uint64_t rnd (void) { rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17; return rng; }

typedef struct { const char *name; const char *ddl; int kind; } tdef;      // kind: 0 TEXT key, 1 INTEGER PRIMARY KEY, 2 composite, 3 WITHOUT ROWID text key
static const tdef TABS[] = {
    { "t", "CREATE TABLE t(id TEXT PRIMARY KEY NOT NULL, a TEXT, b INTEGER, c REAL)", 0 },
    { "u", "CREATE TABLE u(id INTEGER PRIMARY KEY NOT NULL, a TEXT, b INTEGER)", 1 },
    { "w", "CREATE TABLE w(x INTEGER NOT NULL, y TEXT NOT NULL, a TEXT, b INTEGER, PRIMARY KEY(x, y))", 2 },
    { "z", "CREATE TABLE z(id TEXT PRIMARY KEY NOT NULL, a TEXT, b INTEGER) WITHOUT ROWID", 3 },
};
#define NT 4
#define NK 8

static void keyval (int kind, int k, char *where, char *vals, size_t cap, uint8_t *pk, size_t *pklen) {
    crdt_value v[2]; char kb[32]; int n = 1;
    switch (kind) {
        case 1: snprintf(where, cap, "id=%d", k + 1); snprintf(vals, cap, "%d", k + 1); v[0] = (crdt_value){ CRDT_INTEGER, k + 1, 0, NULL, 0 }; break;
        case 2: snprintf(kb, sizeof kb, "s%d", k % 3); snprintf(where, cap, "x=%d AND y='%s'", k / 3, kb); snprintf(vals, cap, "%d,'%s'", k / 3, kb); v[0] = (crdt_value){ CRDT_INTEGER, k / 3, 0, NULL, 0 }; v[1] = (crdt_value){ CRDT_TEXT, 0, 0, kb, strlen(kb) }; n = 2; break;
        default: snprintf(kb, sizeof kb, "k%d", k); snprintf(where, cap, "id='%s'", kb); snprintf(vals, cap, "'%s'", kb); v[0] = (crdt_value){ CRDT_TEXT, 0, 0, kb, strlen(kb) };
    }
    *pklen = crdt_pk_encode(v, n, pk, 64);
}
static const char *keycols (int kind) { return kind == 1 ? "id" : kind == 2 ? "x,y" : "id"; }

static bool alive[NT][NK];

// the cells of a row as "col_id:cv" sorted, on the oracle and on ours
static int cmp_s (const void *a, const void *b) { return strcmp(*(char *const *)a, *(char *const *)b); }
static int oracle_cells (sqlite3 *o, const char *tab, const uint8_t *pk, size_t pklen, char **out) {
    char sql[200]; snprintf(sql, sizeof sql, "SELECT col_name, col_version FROM %s_cloudsync WHERE pk = ?", tab);
    sqlite3_stmt *st; sqlite3_prepare_v2(o, sql, -1, &st, NULL); sqlite3_bind_blob(st, 1, pk, (int)pklen, SQLITE_STATIC);
    int n = 0;
    while (sqlite3_step(st) == SQLITE_ROW && n < 16) {
        const char *cn = (const char *)sqlite3_column_text(st, 0); uint32_t id = strcmp(cn, CRDT_SENTINEL) ? mw_name_id(cn) : CRDT_COL_SENTINEL;
        char b[64]; snprintf(b, sizeof b, "%08x:%lld", id, (long long)sqlite3_column_int64(st, 1)); out[n++] = strdup(b);
    }
    sqlite3_finalize(st); qsort(out, (size_t)n, sizeof *out, cmp_s); return n;
}
static int our_cells (mw_meta *m, const char *tab, const uint8_t *pk, size_t pklen, char **out) {
    mw_mcell *c; int n; mw_meta_row(m, mw_name_id(tab), pk, pklen, &c, &n);
    for (int i = 0; i < n; i++) { char b[64]; snprintf(b, sizeof b, "%08x:%lld", c[i].col, (long long)c[i].cv); out[i] = strdup(b); }
    free(c); qsort(out, (size_t)n, sizeof *out, cmp_s); return n;
}

int main (void) {
    char path[256]; mw_tmpdb(path, sizeof path, "ometa");
    char uri[300]; snprintf(uri, sizeof uri, "file:%s?mw=2&mw_cdc=1", path);
    sqlite3 *s; CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL"), SQLITE_OK);
    sqlite3 *o; sqlite3_open(":memory:", &o); sqlite3_cloudsync_init(o, NULL, NULL);
    for (int t = 0; t < NT; t++) { CHECK_RC(mw_exec(s, TABS[t].ddl), SQLITE_OK); CHECK_RC(mw_exec(o, TABS[t].ddl), SQLITE_OK); char q[100]; snprintf(q, sizeof q, "SELECT cloudsync_init('%s', NULL, 1)", TABS[t].name); CHECK_RC(mw_exec(o, q), SQLITE_OK); }
    sqlite3_close(s);
    sqlite3 *db; CHECK_RC(sqlite3_open_v2(uri, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL), SQLITE_OK);
    mw_meta *m = NULL; CHECK_RC(sqlite3_file_control(db, "main", MW_FCNTL_META, &m), SQLITE_OK);

    long txns = 0, stmts = 0, bad = 0, uniq = 0;
    for (int txn = 0; txn < 4000 && bad < 3; txn++) {
        char sql[4000] = "BEGIN;"; bool used[NT][NK] = {{0}};
        int nst = 1 + (int)(rnd() % 4); int nreal = 0;
        for (int i = 0; i < nst; i++) {
            int t = (int)(rnd() % NT), k = (int)(rnd() % NK); if (used[t][k]) continue;
            char wh[100], vals[100], st[400]; uint8_t pk[64]; size_t pl;
            keyval(TABS[t].kind, k, wh, vals, sizeof wh, pk, &pl);
            int what = (int)(rnd() % 10); uniq++;
            bool rekey_ok = false; char nwh[100], nvals[100]; uint8_t npk[64]; size_t npl; int nk = (k + 1 + (int)(rnd() % (NK - 1))) % NK;
            if (what == 9 && alive[t][k] && TABS[t].kind != 1 && !alive[t][nk] && !used[t][nk]) { keyval(TABS[t].kind, nk, nwh, nvals, sizeof nwh, npk, &npl); rekey_ok = true; }
            if (!alive[t][k]) {
                if (TABS[t].kind == 1) snprintf(st, sizeof st, "INSERT INTO %s(%s,a,b) VALUES(%s,'v%lld',%lld);", TABS[t].name, keycols(1), vals, (long long)uniq, (long long)uniq);
                else if (TABS[t].kind == 0) snprintf(st, sizeof st, "INSERT INTO t(id,a,b,c) VALUES(%s,'v%lld',%lld,%lld.5);", vals, (long long)uniq, (long long)uniq, (long long)uniq);
                else snprintf(st, sizeof st, "INSERT INTO %s(%s,a,b) VALUES(%s,'v%lld',%lld);", TABS[t].name, keycols(TABS[t].kind), vals, (long long)uniq, (long long)uniq);
                alive[t][k] = true;
            } else if (what < 2) { snprintf(st, sizeof st, "DELETE FROM %s WHERE %s;", TABS[t].name, wh); alive[t][k] = false; }
            else if (rekey_ok) {
                const char *kc = TABS[t].kind == 2 ? "x=" : "id=";
                if (TABS[t].kind == 2) snprintf(st, sizeof st, "UPDATE %s SET x=%d, y='s%d' WHERE %s;", TABS[t].name, nk / 3, nk % 3, wh);
                else snprintf(st, sizeof st, "UPDATE %s SET %s%s WHERE %s;", TABS[t].name, kc, nvals, wh);
                alive[t][k] = false; alive[t][nk] = true; used[t][nk] = true;
            } else {
                int which = (int)(rnd() % 3);
                if (which == 0) snprintf(st, sizeof st, "UPDATE %s SET a='u%lld' WHERE %s;", TABS[t].name, (long long)uniq, wh);
                else if (which == 1) snprintf(st, sizeof st, "UPDATE %s SET b=%lld WHERE %s;", TABS[t].name, (long long)uniq, wh);
                else snprintf(st, sizeof st, "UPDATE %s SET a='w%lld', b=%lld WHERE %s;", TABS[t].name, (long long)uniq, (long long)uniq, wh);
            }
            used[t][k] = true; strcat(sql, st); nreal++; stmts++;
        }
        strcat(sql, "COMMIT;");
        int ro = mw_exec(o, sql), rm = mw_exec(db, sql);
        if (ro != SQLITE_OK || rm != SQLITE_OK) { printf("  statement failed (oracle %d, ours %d): %s\n", ro, rm, sql); bad++; break; }
        txns++;
        for (int t = 0; t < NT; t++) for (int k = 0; k < NK; k++) {
            char wh[100], vals[100]; uint8_t pk[64]; size_t pl; keyval(TABS[t].kind, k, wh, vals, sizeof wh, pk, &pl);
            char *a[16], *b[16]; int na = oracle_cells(o, TABS[t].name, pk, pl, a), nb = our_cells(m, TABS[t].name, pk, pl, b);
            bool same = na == nb; for (int i = 0; same && i < na; i++) if (strcmp(a[i], b[i])) same = false;
            if (!same) {
                bad++; printf("  txn %d: %s row %d differs\n    sql: %s\n    sqlite-sync:", txn, TABS[t].name, k, sql); for (int i = 0; i < na; i++) printf(" %s", a[i]); printf("\n    ours:       "); for (int i = 0; i < nb; i++) printf(" %s", b[i]); printf("\n");
            }
            for (int i = 0; i < na; i++) free(a[i]); for (int i = 0; i < nb; i++) free(b[i]);
        }
    }
    printf("metadata store vs sqlite-sync: %ld transactions, %ld statements, %ld divergences\n", txns, stmts, bad);
    CHECK(bad == 0);
    sqlite3_close(db); sqlite3_close(o); mw_rmdb(path);
    MW_DONE();
}
