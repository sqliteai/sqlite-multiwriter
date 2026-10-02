// The metadata in runs, differentially: one random workload (inserts, updates, deletes, deletes and inserts again, key changes, DROP and CREATE of a table, reopening the database) is run
// twice with the same seed: once with the defaults (the rows live in memory, one flush at the close) and once with the store under pressure (a cache of one megabyte, so that rows are read from
// the runs, a flush after every row, merges of two runs in parts of 40 rows). The cells of the two databases (mw_cells: causal lengths, column versions, db_versions, sequences) must be identical at
// every reopening, and so must what an export holds.
#include <stdlib.h>
#include <stdbool.h>
#include "mw_test.h"
#include "multiwriter.h"
#include "multiwriter_sync.h"

static uint64_t rng;
static uint64_t rnd (void) { rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17; return rng; }

static int open_db (const char *path, sqlite3 **db) {
    char uri[400]; snprintf(uri, sizeof uri, "file:%s?mw=2&mw_cdc=1%s", path, getenv("MW_TEST_MP") ? "&mw_mp=1" : "");
    int rc = sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
    if (rc == SQLITE_OK) { sqlite3_extended_result_codes(*db, 1); sqlite3_busy_timeout(*db, 0); }
    return rc;
}
static int exec_retry (sqlite3 *db, const char *sql) { for (int i = 0; i < 1000; i++) { int rc = mw_exec(db, sql); if ((rc & 0xff) != SQLITE_BUSY) return rc; } return SQLITE_BUSY; }

// the whole of mw_cells as text, and the export of everything as a hash
static char *dump_cells (sqlite3 *db) {
    sqlite3_stmt *st; size_t cap = 1 << 16, n = 0; char *out = malloc(cap); out[0] = 0;
    if (sqlite3_prepare_v2(db, "SELECT tbl, hex(pk), col, cv, dv, seq, site FROM mw_cells ORDER BY tbl, pk, col", -1, &st, NULL) != SQLITE_OK) return out;
    while (sqlite3_step(st) == SQLITE_ROW) {
        char line[200]; int l = snprintf(line, sizeof line, "%lld|%s|%lld|%lld|%lld|%lld|%lld\n", (long long)sqlite3_column_int64(st, 0), sqlite3_column_text(st, 1), (long long)sqlite3_column_int64(st, 2), (long long)sqlite3_column_int64(st, 3), (long long)sqlite3_column_int64(st, 4), (long long)sqlite3_column_int64(st, 5), (long long)sqlite3_column_int64(st, 6));
        if (n + (size_t)l + 1 > cap) { cap *= 2; out = realloc(out, cap); }
        memcpy(out + n, line, (size_t)l); n += (size_t)l; out[n] = 0;
    }
    sqlite3_finalize(st); return out;
}
// the rows of the three tables as text
static char *dump_tables (sqlite3 *db) {
    const char *q[3] = { "SELECT id, a, b, hex(c) FROM t ORDER BY id", "SELECT k, v FROM u ORDER BY k", "SELECT x, y, z FROM w ORDER BY x, y" };
    size_t cap = 1 << 16, n = 0; char *out = malloc(cap); out[0] = 0;
    for (int t = 0; t < 3; t++) {
        sqlite3_stmt *st; if (sqlite3_prepare_v2(db, q[t], -1, &st, NULL) != SQLITE_OK) continue;
        int nc = sqlite3_column_count(st);
        while (sqlite3_step(st) == SQLITE_ROW) {
            char line[300]; int l = snprintf(line, sizeof line, "%d:", t);
            for (int c = 0; c < nc; c++) l += snprintf(line + l, sizeof line - (size_t)l, "%s|", sqlite3_column_text(st, c) ? (const char *)sqlite3_column_text(st, c) : "NULL");
            line[l++] = '\n'; if (n + (size_t)l + 1 > cap) { cap *= 2; out = realloc(out, cap); }
            memcpy(out + n, line, (size_t)l); n += (size_t)l; out[n] = 0;
        }
        sqlite3_finalize(st);
    }
    return out;
}
static uint64_t hash_bytes (const uint8_t *p, size_t n) { uint64_t h = 1469598103934665603ull; for (size_t i = 0; i < n; i++) { h ^= p[i]; h *= 1099511628211ull; } return h; }

#define PHASES 8
#define OPS 400
static char *snapshot[2][PHASES]; static uint64_t exp_hash[2][PHASES]; static size_t exp_len[2][PHASES];

static void run (int variant, const char *path) {
    rng = 0x2545F4914F6CDD1Dull;
    if (variant) { setenv("MW_META_CACHE_MB", "1", 1); setenv("MW_META_FANOUT", "2", 1); setenv("MW_META_PART_ROWS", "40", 1); setenv("MW_META_FLUSH_ROWS", "1", 1); setenv("MW_META_FLUSH_MS", "1", 1); }
    for (int phase = 0; phase < PHASES; phase++) {
        sqlite3 *db; CHECK_RC(open_db(path, &db), SQLITE_OK);
        if (phase == 0) CHECK_RC(mw_exec(db, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c BLOB); CREATE TABLE u(k TEXT PRIMARY KEY NOT NULL, v INTEGER); CREATE TABLE w(x INTEGER, y INTEGER, z TEXT, PRIMARY KEY(x, y)) WITHOUT ROWID"), SQLITE_OK);
        for (int i = 0; i < OPS; i++) {
            char sql[400]; int id = (int)(rnd() % 300), k = (int)(rnd() % 120); uint64_t r = rnd() % 100;
            if (phase == 4 && i == 0) { CHECK_RC(exec_retry(db, "DROP TABLE u"), SQLITE_OK); CHECK_RC(exec_retry(db, "CREATE TABLE u(k TEXT PRIMARY KEY NOT NULL, v INTEGER)"), SQLITE_OK); }
            if (r < 30) snprintf(sql, sizeof sql, "INSERT OR IGNORE INTO t VALUES(%d, %d, 'x%d', zeroblob(%d))", id, id * 7, id, (int)(rnd() % 40));
            else if (r < 55) snprintf(sql, sizeof sql, "UPDATE t SET a = a + 1 WHERE id = %d", id);
            else if (r < 62) snprintf(sql, sizeof sql, "UPDATE t SET b = 'y%d', c = NULL WHERE id = %d", i, id);
            else if (r < 70) snprintf(sql, sizeof sql, "DELETE FROM t WHERE id = %d", id);
            else if (r < 73) snprintf(sql, sizeof sql, "UPDATE t SET id = %d WHERE id = %d", 1000 + id, id);            // (a key change)
            else if (r < 85) snprintf(sql, sizeof sql, "INSERT OR REPLACE INTO u VALUES('key-%d', %d)", k, i);
            else if (r < 90) snprintf(sql, sizeof sql, "DELETE FROM u WHERE k = 'key-%d'", k);
            else if (r < 97) snprintf(sql, sizeof sql, "INSERT OR REPLACE INTO w VALUES(%d, %d, 'z%d')", id % 20, k % 15, i);
            else snprintf(sql, sizeof sql, "DELETE FROM w WHERE x = %d", id % 20);
            { int rc = exec_retry(db, sql); CHECK(rc == SQLITE_OK || (rc & 0xff) == SQLITE_CONSTRAINT); }       // (a key change onto a key that is there: the same in both runs)
        }
        // what an export of everything holds (peers apply it): its size and its hash, with the db_versions the commits got
        {
            uint8_t *pl = NULL; size_t n = 0; int64_t upto = 0;
            CHECK_RC(mw_sync_export(db, 0, &pl, &n, &upto), SQLITE_OK);
            exp_len[variant][phase] = n;
            char pc[256]; mw_tmpdb(pc, sizeof pc, variant ? "rsCB" : "rsCA"); sqlite3 *peer; CHECK_RC(open_db(pc, &peer), SQLITE_OK);
            CHECK_RC(mw_exec(peer, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c BLOB); CREATE TABLE u(k TEXT PRIMARY KEY NOT NULL, v INTEGER); CREATE TABLE w(x INTEGER, y INTEGER, z TEXT, PRIMARY KEY(x, y)) WITHOUT ROWID"), SQLITE_OK);
            mw_sync_stats st; int arc = n ? mw_sync_apply(peer, pl, n, &st) : SQLITE_OK; CHECK_RC(arc, SQLITE_OK);
            char *a = dump_tables(db), *b = dump_tables(peer);
            exp_hash[variant][phase] = hash_bytes((const uint8_t *)a, strlen(a));
            if (strcmp(a, b)) { printf("   phase %d: the peer that applied the export does not hold the tables of its source\n", phase); CHECK(0); }
            free(a); free(b); sqlite3_close(peer); mw_rmdb(pc); mw_sync_free(pl);
        }
        sqlite3_close(db);
        // reopen: the cells as the file holds them (a flush at the close)
        CHECK_RC(open_db(path, &db), SQLITE_OK);
        snapshot[variant][phase] = dump_cells(db);
        sqlite3_close(db);
    }
    if (variant) { unsetenv("MW_META_CACHE_MB"); unsetenv("MW_META_FANOUT"); unsetenv("MW_META_PART_ROWS"); unsetenv("MW_META_FLUSH_ROWS"); unsetenv("MW_META_FLUSH_MS"); }
}

int main (void) {
    char pa[256], pb[256];
    mw_tmpdb(pa, sizeof pa, "rsA"); run(0, pa); mw_rmdb(pa);
    mw_tmpdb(pb, sizeof pb, "rsB"); run(1, pb); mw_rmdb(pb);
    int same = 0;
    for (int ph = 0; ph < PHASES; ph++) {
        bool eq = !strcmp(snapshot[0][ph], snapshot[1][ph]);
        size_t lines = 0; for (const char *p = snapshot[0][ph]; *p; p++) if (*p == '\n') lines++;
        printf("phase %d: %zu cells, defaults vs store under pressure: %s; tables: %s; the export of each rebuilds its tables on a peer\n", ph, lines, eq ? "identical" : "DIFFERENT", exp_hash[0][ph] == exp_hash[1][ph] ? "identical" : "DIFFERENT");
        CHECK(eq); CHECK(exp_hash[0][ph] == exp_hash[1][ph]); CHECK(lines > 50);
        same += eq;
    }
    MW_DONE();
}
