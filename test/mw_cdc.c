// CDC prototype (docs §55): the cell versions captured in the VFS for tables with an INTEGER PRIMARY KEY, checked against what the application did. A cell's column
// version starts at 1 with the insert and goes up by one for every committed transaction that changed that column of that row; the delete marker counts too; the
// db_version of a cell is the epoch of the commit that last changed it. Other tables (here: one with a TEXT key, and a WITHOUT ROWID one) are left alone.
#include <pthread.h>
#include "mw_test.h"
#include "multiwriter.h"

static int open_lane (const char *path, sqlite3 **db) {
    char uri[300]; snprintf(uri, sizeof uri, "file:%s?mw=2&mw_gc=16&mw_cdc=1", path);
    int rc = sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
    if (rc == SQLITE_OK) { sqlite3_extended_result_codes(*db, 1); sqlite3_busy_timeout(*db, 0); }
    return rc;
}
static int cell (sqlite3 *db, uint32_t root, int col, int64_t rowid, uint32_t *cv, uint32_t *dv) {
    mw_cdc_cell c = { root, col, rowid, 0, 0, 0 };
    CHECK_RC(sqlite3_file_control(db, "main", MW_FCNTL_CDC_GET, &c), SQLITE_OK);
    if (cv) *cv = c.cv; if (dv) *dv = c.dv;
    return c.found;
}
static int retry (sqlite3 *db, const char *sql) {
    int rc = SQLITE_BUSY; long us = 1;
    for (int a = 0; a < 100000 && (rc & 0xff) == SQLITE_BUSY; a++) {
        rc = mw_exec(db, sql); if (rc != SQLITE_OK && !sqlite3_get_autocommit(db)) mw_exec(db, "ROLLBACK");
        if ((rc & 0xff) == SQLITE_BUSY && a > 4) { struct timespec ts = { 0, us * 1000 }; nanosleep(&ts, NULL); if (us < 1000) us *= 2; }
    }
    return rc;
}

static const char *g_path; static uint32_t g_root;
typedef struct { int id; int n; } worker_t;
static void *worker (void *arg) {          // each worker updates column a of its own row, 4 workers, one page; and every 3rd time column c too
    worker_t *w = arg; sqlite3 *db; if (open_lane(g_path, &db) != SQLITE_OK) return NULL;
    for (int i = 0; i < w->n; i++) {
        char sql[200];
        if (i % 3 == 2) snprintf(sql, sizeof sql, "UPDATE t SET a=a+1, c=c+1 WHERE id=%d", 100 + w->id); else snprintf(sql, sizeof sql, "UPDATE t SET a=a+1 WHERE id=%d", 100 + w->id);
        retry(db, sql);
    }
    sqlite3_close(db); return NULL;
}

int main (void) {
    char path[256]; mw_tmpdb(path, sizeof path, "cdc"); g_path = path;
    char dir[300]; snprintf(dir, sizeof dir, "%s-mwvs", path);
    sqlite3 *s; CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c INTEGER);"
                        "CREATE TABLE k(id TEXT PRIMARY KEY NOT NULL, v INTEGER); CREATE TABLE w(id INTEGER PRIMARY KEY, v INTEGER) WITHOUT ROWID;"), SQLITE_OK);
    sqlite3_close(s);
    sqlite3 *db; CHECK_RC(open_lane(path, &db), SQLITE_OK);
    g_root = (uint32_t)mw_scalar(db, "SELECT rootpage FROM sqlite_schema WHERE name='t'");
    uint32_t kroot = (uint32_t)mw_scalar(db, "SELECT rootpage FROM sqlite_schema WHERE name='k'");
    CHECK(g_root > 1);
    uint32_t cv, dv, dv0;

    // inserts: every non-key column gets version 1, at the db_version of the commit
    CHECK_RC(mw_exec(db, "INSERT INTO t VALUES(1, 10, 'x', 100), (2, 20, 'y', 200)"), SQLITE_OK);
    CHECK(cell(db, g_root, 1, 1, &cv, &dv0) && cv == 1);
    CHECK(cell(db, g_root, 2, 1, &cv, NULL) && cv == 1 && cell(db, g_root, 3, 1, &cv, NULL) && cv == 1);
    CHECK(cell(db, g_root, 1, 2, &cv, &dv) && cv == 1 && dv == dv0);                         // (same commit: same db_version)
    CHECK(!cell(db, g_root, 0, 1, NULL, NULL));                                              // (the INTEGER PRIMARY KEY column has no cell)

    // updates: only the changed column goes up
    CHECK_RC(mw_exec(db, "UPDATE t SET a=11 WHERE id=1"), SQLITE_OK);
    CHECK(cell(db, g_root, 1, 1, &cv, &dv) && cv == 2 && dv > dv0);
    CHECK(cell(db, g_root, 2, 1, &cv, &dv) && cv == 1 && dv == dv0);
    CHECK_RC(mw_exec(db, "UPDATE t SET a=12, b='z' WHERE id=1"), SQLITE_OK);
    CHECK(cell(db, g_root, 1, 1, &cv, NULL) && cv == 3 && cell(db, g_root, 2, 1, &cv, NULL) && cv == 2 && cell(db, g_root, 3, 1, &cv, NULL) && cv == 1);
    CHECK_RC(mw_exec(db, "UPDATE t SET a=12 WHERE id=1"), SQLITE_OK);                         // (same value: no change, no new version)
    CHECK(cell(db, g_root, 1, 1, &cv, NULL) && cv == 3);
    CHECK_RC(mw_exec(db, "BEGIN; UPDATE t SET a=1 WHERE id=2; UPDATE t SET a=2 WHERE id=2; UPDATE t SET a=3 WHERE id=2; COMMIT"), SQLITE_OK);     // (three updates in one transaction: one version)
    CHECK(cell(db, g_root, 1, 2, &cv, NULL) && cv == 2);

    // delete: the marker
    CHECK_RC(mw_exec(db, "DELETE FROM t WHERE id=2"), SQLITE_OK);
    CHECK(cell(db, g_root, 0xFFFF, 2, &cv, NULL) && cv == 1);

    // other tables: not captured
    CHECK_RC(mw_exec(db, "INSERT INTO k VALUES('a', 1); INSERT INTO w VALUES(1, 1)"), SQLITE_OK);
    CHECK(!cell(db, kroot, 1, 1, NULL, NULL));

    // bulk: 2000 rows (splits, interior pages), then the versions of rows spread over the table
    CHECK_RC(mw_exec(db, "WITH RECURSIVE n(i) AS (SELECT 1000 UNION ALL SELECT i+1 FROM n WHERE i<3999) INSERT INTO t SELECT i, i, 'row ' || i, i*2 FROM n"), SQLITE_OK);
    int bad = 0; for (int id = 1000; id < 4000; id += 37) if (!(cell(db, g_root, 1, id, &cv, NULL) && cv == 1 && cell(db, g_root, 3, id, &cv, NULL) && cv == 1)) bad++;
    CHECK(bad == 0);
    CHECK_RC(mw_exec(db, "UPDATE t SET c=c+1 WHERE id BETWEEN 2000 AND 2999 AND id % 2 = 0"), SQLITE_OK);
    bad = 0; for (int id = 1000; id < 4000; id += 11) { int want = (id >= 2000 && id <= 2999 && id % 2 == 0) ? 2 : 1; if (!(cell(db, g_root, 3, id, &cv, NULL) && (int)cv == want)) bad++; }
    CHECK(bad == 0);

    // four workers on one page, retrying conflicts: no update is lost or counted twice
    CHECK_RC(mw_exec(db, "INSERT INTO t VALUES(100,0,'',0),(101,0,'',0),(102,0,'',0),(103,0,'',0)"), SQLITE_OK);
    pthread_t th[4]; worker_t w[4]; const int N = 300;
    for (int i = 0; i < 4; i++) { w[i] = (worker_t){ i, N }; pthread_create(&th[i], NULL, worker, &w[i]); }
    for (int i = 0; i < 4; i++) pthread_join(th[i], NULL);
    bad = 0;
    for (int i = 0; i < 4; i++) {
        uint32_t cva = 0, cvc = 0; cell(db, g_root, 1, 100 + i, &cva, NULL); cell(db, g_root, 3, 100 + i, &cvc, NULL);
        char q[100]; snprintf(q, sizeof q, "SELECT a FROM t WHERE id=%d", 100 + i);
        if ((int)cva != 1 + N || (int)cvc != 1 + N / 3 || mw_scalar(db, q) != N) bad++;
    }
    printf("workers: %d of 4 rows with a wrong version\n", bad);
    CHECK(bad == 0);
    sqlite3_close(db);
    mw_rmdb(path);
    MW_DONE();
}
