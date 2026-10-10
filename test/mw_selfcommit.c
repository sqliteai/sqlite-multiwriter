// Several commits on one connection while a read statement keeps the read transaction open (each commit must see the previous one).
#include "mw_test.h"
#include "multiwriter.h"

static int open_lane (const char *path, sqlite3 **db) {
    char uri[300];
    snprintf(uri, sizeof uri, "file:%s?mw=1", path);
    return sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
}

int main (void) {
    char path[256]; mw_tmpdb(path, sizeof path, "selfc");
    sqlite3 *s;
    CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, MW_PLAIN_VFS), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER); INSERT INTO t VALUES(1,0),(2,0),(3,0),(4,0),(5,0)"), SQLITE_OK);
    sqlite3_close(s);

    sqlite3 *a, *b;
    CHECK_RC(open_lane(path, &a), SQLITE_OK);
    CHECK_RC(open_lane(path, &b), SQLITE_OK);

    // a SELECT is still stepping while the same connection updates rows
    sqlite3_stmt *sel;
    CHECK_RC(sqlite3_prepare_v2(a, "SELECT id FROM t ORDER BY id", -1, &sel, NULL), SQLITE_OK);
    CHECK_RC(sqlite3_step(sel), SQLITE_ROW);
    int ok = 0;
    for (int i = 0; i < 5; i++) {
        char sql[80]; snprintf(sql, sizeof sql, "UPDATE t SET v=v+1 WHERE id=%d", i + 1);
        if (mw_exec(a, sql) == SQLITE_OK) ok++;
    }
    printf("updates committed inside one read snapshot: %d of 5\n", ok);
    CHECK(ok == 5);
    sqlite3_finalize(sel);
    CHECK(mw_scalar(a, "SELECT sum(v) FROM t") == 5);

    // another connection commits on the same page in between: that is a real conflict (as in stock WAL, the stale reader cannot upgrade)
    CHECK_RC(sqlite3_prepare_v2(a, "SELECT id FROM t ORDER BY id", -1, &sel, NULL), SQLITE_OK);
    CHECK_RC(sqlite3_step(sel), SQLITE_ROW);
    CHECK_RC(mw_exec(a, "UPDATE t SET v=v+1 WHERE id=1"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "UPDATE t SET v=v+1 WHERE id=5"), SQLITE_OK);
    int rc2 = mw_exec(a, "UPDATE t SET v=v+1 WHERE id=2");
    CHECK((rc2 & 0xff) == SQLITE_BUSY);
    sqlite3_finalize(sel);
    CHECK(mw_scalar(a, "SELECT sum(v) FROM t") == 7);              // a's first + b's; the refused one left no trace

    // many pages: a commit of more than 64 pages, then single rows (pages already in the list of what the snapshot wrote, and new ones), in one read snapshot; every one of them must be
    // found in the list of the pages this connection wrote (a page missing from it is a conflict with itself)
    CHECK_RC(mw_exec(a, "CREATE TABLE big(id INTEGER PRIMARY KEY, v INTEGER, pad TEXT)"), SQLITE_OK);
    CHECK_RC(mw_exec(a, "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 3000) INSERT INTO big SELECT x, 0, printf('%0100d', x) FROM c"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "SELECT 1"), SQLITE_OK);
    CHECK_RC(sqlite3_prepare_v2(a, "SELECT id FROM big ORDER BY id", -1, &sel, NULL), SQLITE_OK);
    CHECK_RC(sqlite3_step(sel), SQLITE_ROW);
    CHECK_RC(mw_exec(a, "UPDATE big SET v=v+1"), SQLITE_OK);                                       // (about 100 pages: the list is built from a commit of more than 64)
    ok = 0;
    for (int i = 0; i < 40; i++) {
        char sql[80]; snprintf(sql, sizeof sql, "UPDATE big SET v=v+1 WHERE id=%d", 1 + (i * 73) % 3000);
        if (mw_exec(a, sql) == SQLITE_OK) ok++;
    }
    CHECK(ok == 40);
    for (int i = 0; i < 40; i++) {                                                                  // (the same rows again: the pages are in the list: one entry for each)
        char sql[80]; snprintf(sql, sizeof sql, "UPDATE big SET v=v+1 WHERE id=%d", 1 + (i * 73) % 3000);
        if (mw_exec(a, sql) == SQLITE_OK) ok++;
    }
    CHECK(ok == 80);
    sqlite3_finalize(sel);
    CHECK(mw_scalar(a, "SELECT sum(v) FROM big") == 3000 + 80);

    sqlite3_close(a); sqlite3_close(b);
    mw_rmdb(path);
    MW_DONE();
}
