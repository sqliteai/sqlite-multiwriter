// Multi-process mode: phase 1 of a relocation (the private copies of the pages, the references to the new pages renumbered for the end of the file as it was) is made before the
// publication lock. With `mw_prep_delay_us` the commits wait between the preparation and the lock, so that other commits extend the file in between and the references have to be rewritten
// under the lock with the new end. Every run ends with integrity_check and a data check: a reference that is not rewritten corrupts the database.
//  1. several connections insert rows (small, with overflow chains, with an index) on disjoint keys: all the rows are there, the index agrees, the prepared phases were used and rewritten;
//  2. the same without the delay (the end moves less): the prepared phases are used, the data is right;
//  3. the same with updates of one hot row next to the inserts (commits that conflict for other reasons than the end of the file).
#include <pthread.h>
#include "mw_test.h"
#include "multiwriter.h"

static int open_mp (const char *path, sqlite3 **db, int delay_us) {
    char uri[300]; snprintf(uri, sizeof uri, "file:%s?mw=1&mw_mp=1&mw_gc=16&mw_prep_delay_us=%d", path, delay_us);
    int rc = sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
    if (rc == SQLITE_OK) { sqlite3_extended_result_codes(*db, 1); sqlite3_busy_timeout(*db, 0); }
    return rc;
}
static mw_db_stats stats (sqlite3 *db) { mw_db_stats s; memset(&s, 0, sizeof s); sqlite3_file_control(db, "main", MW_FCNTL_DBSTATS, &s); return s; }
static int integrity_ok (sqlite3 *db) {
    sqlite3_stmt *st; int ok = 0;
    if (sqlite3_prepare_v2(db, "PRAGMA integrity_check", -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW) ok = strcmp((const char *)sqlite3_column_text(st, 0), "ok") == 0;
    sqlite3_finalize(st); return ok;
}
static int64_t scalar (sqlite3 *db, const char *sql) { sqlite3_stmt *st; int64_t v = -1; if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW) v = sqlite3_column_int64(st, 0); sqlite3_finalize(st); return v; }

enum { T = 6, TXNS = 100 };
static struct { int64_t rows, bytes; } tot[T];
static const char *g_path; static int g_delay, g_hot;
static void *worker (void *arg) {
    int id = (int)(intptr_t)arg; sqlite3 *db; if (open_mp(g_path, &db, g_delay) != SQLITE_OK) { mw_failures++; return NULL; }
    unsigned rng = 4242u + (unsigned)id * 7919u;
    for (int n = 0; n < TXNS; n++) {
        rng = rng * 1103515245u + 12345u; int rows = 1 + (int)(rng >> 16) % 10;
        rng = rng * 1103515245u + 12345u; int len = ((rng >> 16) % 6 == 0) ? 5000 + (int)(rng >> 8) % 15000 : (int)(rng >> 16) % 700;     // (one in six has an overflow chain)
        char sql[500];
        snprintf(sql, sizeof sql, "WITH RECURSIVE r(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM r WHERE i<%d) INSERT INTO t SELECT (%lld<<32)+%d*100+i, 'k%d-'||printf('%%08d',%d*100+i)||hex(randomblob(16)), zeroblob(%d) FROM r",
                 rows - 1, (long long)id, n, id, n, len);
        char tx[900]; snprintf(tx, sizeof tx, "BEGIN; %s; %s COMMIT", sql, g_hot && n % 4 == 0 ? "UPDATE hot SET n = n + 1 WHERE id = 1;" : "");       // (one transaction: a refused commit is rolled back whole and run again)
        for (;;) { int rc = mw_exec(db, tx); if (rc == SQLITE_OK) break; mw_exec(db, "ROLLBACK"); if ((rc & 0xff) != SQLITE_BUSY) { printf("unexpected rc %d\n", rc); mw_failures++; sqlite3_close(db); return NULL; } }
        tot[id].rows += rows; tot[id].bytes += (int64_t)rows * len;
    }
    sqlite3_close(db); return NULL;
}

static void run (const char *name, int delay_us, int hot, int need_rewrite) {
    char path[256]; mw_tmpdb(path, sizeof path, "relocprep");
    sqlite3 *s; CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, MW_PLAIN_VFS), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, k TEXT, v BLOB); CREATE INDEX ik ON t(k); CREATE TABLE hot(id INTEGER PRIMARY KEY, n INTEGER); INSERT INTO hot VALUES(1, 0)"), SQLITE_OK);
    sqlite3_close(s);
    g_path = path; g_delay = delay_us; g_hot = hot; memset(tot, 0, sizeof tot);
    sqlite3 *obs; CHECK_RC(open_mp(path, &obs, 0), SQLITE_OK);
    pthread_t th[T]; for (intptr_t i = 0; i < T; i++) pthread_create(&th[i], NULL, worker, (void *)i);
    for (int i = 0; i < T; i++) pthread_join(th[i], NULL);
    int64_t want_rows = 0, want_bytes = 0; for (int i = 0; i < T; i++) { want_rows += tot[i].rows; want_bytes += tot[i].bytes; }
    mw_db_stats st = stats(obs);
    int64_t rows = scalar(obs, "SELECT count(*) FROM t"), bytes = scalar(obs, "SELECT sum(length(v)) FROM t"), idx = scalar(obs, "SELECT count(*) FROM t INDEXED BY ik WHERE k >= ''");
    printf("%s: rows %lld (want %lld), bytes %lld (want %lld), index %lld; relocations %llu, prepared used %llu, dropped %llu, rewritten %llu\n", name, (long long)rows, (long long)want_rows, (long long)bytes, (long long)want_bytes, (long long)idx,
           (unsigned long long)st.relocations, (unsigned long long)st.reloc_prep_used, (unsigned long long)st.reloc_prep_dropped, (unsigned long long)st.reloc_prep_rewrote);
    CHECK(rows == want_rows); CHECK(bytes == want_bytes); CHECK(idx == want_rows); CHECK(integrity_ok(obs));
    CHECK(st.reloc_prep_used > 0);
    if (need_rewrite) CHECK(st.reloc_prep_rewrote > 0);
    sqlite3_close(obs);
    { sqlite3 *r; CHECK_RC(open_mp(path, &r, 0), SQLITE_OK); CHECK(scalar(r, "SELECT count(*) FROM t") == want_rows); CHECK(integrity_ok(r)); sqlite3_close(r); }       // (and after a reopen: the log replays what was published)
    mw_rmdb(path);
}

int main (void) {
    setvbuf(stdout, NULL, _IOLBF, 0);
    run("1. the end of the file moves between the preparation and the lock", 400, 0, 1);
    run("2. no delay", 0, 0, 0);
    run("3. with a hot row next to the inserts", 200, 1, 0);
    MW_DONE();
}
