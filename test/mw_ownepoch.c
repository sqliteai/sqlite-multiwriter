// One snapshot that commits more than once (a statement is still stepping on the connection): a page that it wrote is validated against the commit that wrote it, not against its latest commit.
// A commits page X, B commits X after that, A commits another page, A writes X again: this one must conflict (it was computed on a copy of X without B's change); it used to pass and overwrite B's commit.
#include "mw_test.h"
#include "multiwriter.h"

static int open_lane (const char *path, sqlite3 **db) {
    char uri[400]; snprintf(uri, sizeof uri, "file:%s?mw=2", path);
    int rc = sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
    if (rc == SQLITE_OK) { sqlite3_extended_result_codes(*db, 1); sqlite3_busy_timeout(*db, 0); }
    return rc;
}
int main (void) {
    char path[300]; mw_tmpdb(path, sizeof path, "ownepoch");
    { sqlite3 *s; CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, MW_PLAIN_VFS), SQLITE_OK);
      CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t1(id INTEGER PRIMARY KEY, v INTEGER, w INTEGER); CREATE TABLE t2(id INTEGER PRIMARY KEY, v INTEGER); CREATE TABLE big(id INTEGER PRIMARY KEY, x TEXT);"
                          "INSERT INTO t1 VALUES(1, 0, 0); INSERT INTO t2 VALUES(1, 0); INSERT INTO big VALUES(1, 'a'), (2, 'b'), (3, 'c')"), SQLITE_OK); sqlite3_close(s); }
    sqlite3 *a, *b; CHECK_RC(open_lane(path, &a), SQLITE_OK); CHECK_RC(open_lane(path, &b), SQLITE_OK);
    sqlite3_stmt *rd = NULL; CHECK_RC(sqlite3_prepare_v2(a, "SELECT id FROM big ORDER BY id", -1, &rd, NULL), SQLITE_OK);
    CHECK(sqlite3_step(rd) == SQLITE_ROW);                                              // (the snapshot of A stays open from here)
    CHECK_RC(mw_exec(a, "UPDATE t1 SET v = v + 1 WHERE id = 1"), SQLITE_OK);              // commit 1 of A: page of t1
    CHECK_RC(mw_exec(b, "UPDATE t1 SET w = w + 1 WHERE id = 1"), SQLITE_OK);              // B commits the same page after it
    int r2 = mw_exec(a, "UPDATE t2 SET v = v + 1 WHERE id = 1");                          // commit 2 of A: another page (the page of t1 that A read is now stale: this may already be refused)
    int rc = mw_exec(a, "UPDATE t1 SET v = v + 1 WHERE id = 1");                          // commit 3 of A: the page of t1 again
    printf("second commit of A: rc=%d, third: rc=%d\n", r2, rc);
    CHECK(r2 == SQLITE_OK || (r2 & 0xff) == SQLITE_BUSY);
    CHECK((rc & 0xff) == SQLITE_BUSY);                                                    // (BUSY_SNAPSHOT: it cannot go through)
    sqlite3_finalize(rd);
    CHECK(mw_scalar(b, "SELECT w FROM t1 WHERE id = 1") == 1);                            // B's change is there
    sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);
    MW_DONE();
}
