// Phase 5: private write lanes. Several connections write concurrently without BUSY and their
// commits are captured as physical write sets. (Since phase 6 they are also published to the page
// store; conflict detection between them is phase 8, so this test does not assert on merged
// contents.) The real database file must never change: only the compactor writes it.
#include <pthread.h>
#include "mw_test.h"
#include "multiwriter.h"

static int open_lane (const char *path, sqlite3 **db) {
    char uri[300];
    snprintf(uri, sizeof uri, "file:%s?mw=1", path);
    return sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
}
static mw_tx_info txinfo (sqlite3 *db) { mw_tx_info i; memset(&i, 0, sizeof i); sqlite3_file_control(db, "main", MW_FCNTL_TXINFO, &i); return i; }

static uint64_t file_hash (const char *path) {
    FILE *f = fopen(path, "rb"); uint64_t h = 1469598103934665603ull; int c;
    while (f && (c = fgetc(f)) != EOF) { h ^= (uint64_t)c; h *= 1099511628211ull; }
    if (f) fclose(f);
    return h;
}

static int in_set (const uint32_t *v, int n, uint32_t x) { for (int i = 0; i < n; i++) if (v[i] == x) return 1; return 0; }

int main (void) {
    char path[256];
    mw_tmpdb(path, sizeof path, "lanes");

    // schema + base data through a stock connection on the underlying VFS
    sqlite3 *s;
    CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, MW_PLAIN_VFS), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); CREATE TABLE big(id INTEGER PRIMARY KEY, v TEXT);"
                        "INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c')"), SQLITE_OK);
    sqlite3_close(s);
    uint64_t h0 = file_hash(path);

    sqlite3 *a, *b;
    CHECK_RC(open_lane(path, &a), SQLITE_OK);
    CHECK_RC(open_lane(path, &b), SQLITE_OK);
    CHECK(mw_scalar(a, "SELECT count(*) FROM t") == 3);       // base data is readable through a lane

    // two writers, overlapping in time: no BUSY, no lock wait, each sees only its own change
    sqlite3_busy_timeout(a, 0); sqlite3_busy_timeout(b, 0);
    CHECK_RC(mw_exec(a, "BEGIN IMMEDIATE"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "BEGIN IMMEDIATE"), SQLITE_OK);       // stock WAL: SQLITE_BUSY here
    CHECK_RC(mw_exec(a, "UPDATE t SET v='A' WHERE id=1"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "UPDATE t SET v='B' WHERE id=2"), SQLITE_OK);
    CHECK(mw_scalar(a, "SELECT v='A' AND (SELECT v FROM t WHERE id=2)='b' FROM t WHERE id=1") == 1);
    CHECK(mw_scalar(b, "SELECT v='B' AND (SELECT v FROM t WHERE id=1)='a' FROM t WHERE id=2") == 1);
    CHECK_RC(mw_exec(a, "COMMIT"), SQLITE_OK);
    mw_tx_info ta = txinfo(a);
    CHECK(ta.state == MW_TX_COMMITTED && ta.is_writer);
    CHECK(ta.ws_pages == 1);                                   // one leaf page; page 1 untouched
    CHECK((mw_exec(b, "COMMIT") & 0xff) == SQLITE_BUSY);      // same leaf page as A: refused (phase 8), the txn is rolled back
    CHECK(txinfo(b).ws_pages == 1);                            // ... but its write set was captured

    // captured write set = {2} (root page of t, the first table), not page 1
    CHECK_RC(mw_exec(a, "BEGIN IMMEDIATE; UPDATE t SET v='A2' WHERE id=3; COMMIT"), SQLITE_OK);
    uint32_t pg[8]; mw_writeset ws = {pg, 8, 0};
    sqlite3_file_control(a, "main", MW_FCNTL_WRITESET, &ws);
    CHECK(ws.n == 1 && in_set(pg, ws.n, 2) && !in_set(pg, ws.n, 1));

    // the real file is untouched by lane commits
    CHECK(file_hash(path) == h0);

    // rollback: no frames reach the private WAL commit path
    CHECK_RC(mw_exec(a, "BEGIN; UPDATE t SET v='R' WHERE id=1; ROLLBACK"), SQLITE_OK);
    mw_tx_info tr = txinfo(a);
    CHECK(tr.state == MW_TX_ABORTED && tr.ws_pages == 0);
    CHECK(file_hash(path) == h0);

    // savepoint rollback inside one transaction
    CHECK_RC(mw_exec(a, "BEGIN; UPDATE t SET v='x' WHERE id=1; SAVEPOINT s; UPDATE t SET v='y' WHERE id=2; ROLLBACK TO s; RELEASE s; COMMIT"), SQLITE_OK);
    CHECK(txinfo(a).ws_pages == 1);

    // large transaction: the pager cache spills dirty pages into the private WAL before commit
    CHECK_RC(mw_exec(a, "PRAGMA cache_size=8"), SQLITE_OK);
    CHECK_RC(mw_exec(a, "BEGIN"), SQLITE_OK);
    for (int i = 0; i < 400; i++) {
        char sql[200];
        snprintf(sql, sizeof sql, "INSERT INTO big(v) VALUES(zeroblob(1500))");
        CHECK_RC(mw_exec(a, sql), SQLITE_OK);
    }
    CHECK(mw_scalar(a, "SELECT count(*) FROM big") == 400);   // own writes visible, including spilled pages
    CHECK_RC(mw_exec(a, "COMMIT"), SQLITE_OK);
    mw_tx_info tb = txinfo(a);
    CHECK(tb.ws_pages > 100);
    CHECK(file_hash(path) == h0);
    printf("big txn write set: %u pages\n", tb.ws_pages);

    // unsupported modes are rejected, mmap is normalised to 0 (xFetch never used)
    CHECK(mw_exec(a, "PRAGMA journal_mode=DELETE") != SQLITE_OK);
    CHECK(mw_exec(a, "PRAGMA journal_mode=MEMORY") != SQLITE_OK);
    CHECK(mw_exec(a, "PRAGMA locking_mode=EXCLUSIVE") != SQLITE_OK);
    CHECK(mw_exec(a, "PRAGMA auto_vacuum=FULL") != SQLITE_OK);
    CHECK_RC(mw_exec(a, "PRAGMA journal_mode=WAL"), SQLITE_OK);
    mw_vfs_events_reset();
    CHECK_RC(mw_exec(a, "PRAGMA mmap_size=1048576"), SQLITE_OK);
    mw_scalar(a, "SELECT count(*) FROM t");
    CHECK(mw_vfs_event_count(MW_EV_FETCH) == 0);

    // a checkpoint request must not touch the real file
    CHECK_RC(mw_exec(a, "PRAGMA wal_checkpoint(TRUNCATE)"), SQLITE_OK);
    CHECK(file_hash(path) == h0);

    sqlite3_close(a); sqlite3_close(b);
    CHECK(file_hash(path) != h0);            // the last close compacted the committed state into the real file (phase 18)
    sqlite3 *chk;                             // ... which is an ordinary database again, readable by stock SQLite
    CHECK_RC(sqlite3_open_v2(path, &chk, SQLITE_OPEN_READONLY, MW_PLAIN_VFS), SQLITE_OK);
    CHECK(mw_scalar(chk, "SELECT count(*) FROM big") == 400);
    CHECK(mw_scalar(chk, "PRAGMA integrity_check") != -1);
    sqlite3_close(chk);
    mw_rmdb(path);
    MW_DONE();
}
