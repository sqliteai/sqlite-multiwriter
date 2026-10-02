// Phase 6: committed page versions, snapshot reads. Writers here are sequential (conflict
// handling is phase 8); committed state lives only in the page store, the real file must not change.
#include "mw_test.h"
#include "multiwriter.h"

static int open_lane (const char *path, sqlite3 **db) {
    char uri[300];
    snprintf(uri, sizeof uri, "file:%s?mw=2", path);
    return sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
}
static mw_tx_info txinfo (sqlite3 *db) { mw_tx_info i; memset(&i, 0, sizeof i); sqlite3_file_control(db, "main", MW_FCNTL_TXINFO, &i); return i; }
static mw_db_stats stats (sqlite3 *db) { mw_db_stats s; memset(&s, 0, sizeof s); sqlite3_file_control(db, "main", MW_FCNTL_DBSTATS, &s); return s; }
static uint64_t file_hash (const char *path) {
    FILE *f = fopen(path, "rb"); uint64_t h = 1469598103934665603ull; int c;
    while (f && (c = fgetc(f)) != EOF) { h ^= (uint64_t)c; h *= 1099511628211ull; }
    if (f) fclose(f);
    return h;
}
static int integrity_ok (sqlite3 *db) {
    sqlite3_stmt *st; int ok = 0;
    if (sqlite3_prepare_v2(db, "PRAGMA integrity_check", -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW) ok = strcmp((const char *)sqlite3_column_text(st, 0), "ok") == 0;
    sqlite3_finalize(st);
    return ok;
}

int main (void) {
    char path[256];
    mw_tmpdb(path, sizeof path, "store");
    sqlite3 *s;
    CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); CREATE INDEX t_v ON t(v);"
                        "INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c')"), SQLITE_OK);
    sqlite3_close(s);
    uint64_t h0 = file_hash(path);

    sqlite3 *a, *b;
    CHECK_RC(open_lane(path, &a), SQLITE_OK);
    CHECK_RC(open_lane(path, &b), SQLITE_OK);
    uint64_t e0 = stats(a).epoch;

    // A commits; B and A itself see it on their next transaction; the real file does not change
    CHECK_RC(mw_exec(a, "UPDATE t SET v='A' WHERE id=1"), SQLITE_OK);
    mw_tx_info t = txinfo(a);
    CHECK(t.state == MW_TX_COMMITTED && t.commit_epoch == e0 + 1);
    CHECK(stats(a).epoch == e0 + 1);
    CHECK(mw_scalar(b, "SELECT v='A' FROM t WHERE id=1") == 1);
    CHECK(mw_scalar(a, "SELECT v='A' FROM t WHERE id=1") == 1);
    CHECK(file_hash(path) == h0);

    // snapshot isolation: R pins the current state while W commits
    CHECK_RC(mw_exec(b, "BEGIN"), SQLITE_OK);
    CHECK(mw_scalar(b, "SELECT count(*) FROM t") == 3);
    uint64_t snap = txinfo(b).snapshot_epoch;
    CHECK_RC(mw_exec(a, "INSERT INTO t VALUES(4,'d'); UPDATE t SET v='A2' WHERE id=1"), SQLITE_OK);
    CHECK(stats(a).epoch > snap);
    CHECK(mw_scalar(b, "SELECT count(*) FROM t") == 3);                  // same snapshot
    CHECK(mw_scalar(b, "SELECT v='A' FROM t WHERE id=1") == 1);
    CHECK(mw_scalar(b, "SELECT count(*) FROM t WHERE v='d'") == 0);      // via the index as well
    CHECK(txinfo(b).snapshot_epoch == snap);
    CHECK_RC(mw_exec(b, "COMMIT"), SQLITE_OK);
    CHECK(mw_scalar(b, "SELECT count(*) FROM t") == 4);                  // new snapshot sees it
    CHECK(mw_scalar(b, "SELECT v='A2' FROM t WHERE id=1") == 1);

    // file growth: many pages (tables + index), page_count grows, structure stays valid
    int64_t pc0 = mw_scalar(a, "PRAGMA page_count");
    CHECK_RC(mw_exec(a, "WITH RECURSIVE n(i) AS (SELECT 10 UNION ALL SELECT i+1 FROM n WHERE i<3009) INSERT INTO t SELECT i, hex(randomblob(60)) FROM n"), SQLITE_OK);
    CHECK(mw_scalar(a, "SELECT count(*) FROM t") == 3004);
    int64_t pc1 = mw_scalar(b, "PRAGMA page_count");
    CHECK(pc1 > pc0 + 20);
    printf("page_count %lld -> %lld\n", (long long)pc0, (long long)pc1);
    CHECK(integrity_ok(a));
    CHECK(integrity_ok(b));
    CHECK(file_hash(path) == h0);

    // alternating writers read each other's commits
    for (int i = 0; i < 50; i++) {
        sqlite3 *w = (i % 2) ? a : b, *r = (i % 2) ? b : a;
        char sql[100];
        snprintf(sql, sizeof sql, "UPDATE t SET v='k%d' WHERE id=%d", i, 10 + i);
        CHECK_RC(mw_exec(w, sql), SQLITE_OK);
        snprintf(sql, sizeof sql, "SELECT v='k%d' FROM t WHERE id=%d", i, 10 + i);
        CHECK(mw_scalar(r, sql) == 1);
    }
    // deletes shrink structure: freelist pages come back through the store
    CHECK_RC(mw_exec(a, "DELETE FROM t WHERE id>100"), SQLITE_OK);
    CHECK(mw_scalar(b, "SELECT count(*) FROM t") == 95);       // ids 1-4 and 10..100
    CHECK(mw_scalar(b, "SELECT count(*) FROM t WHERE id>100") == 0);
    CHECK(integrity_ok(b));

    sqlite3_close(a); sqlite3_close(b);
    mw_rmdb(path);
    MW_DONE();
}
