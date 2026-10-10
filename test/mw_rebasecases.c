// Ways in which a rebased commit used to lose something (found by a review of the sources, each reproduced before it was fixed):
//   1. after a commit of a snapshot was rebased, a later commit of the same snapshot was built on the connection's own page and overwrote what the other writer had committed (lost update);
//   2. a comment in front of a SELECT ("/* req:1 */ SELECT ...") hid that the transaction had read: write skew was let through with the rebase;
//   3. a field of the header of the file that the application sets (PRAGMA user_version) was dropped by the replay, which has the row changes only, and the commit was reported as done.
#include "mw_test.h"
#include "multiwriter.h"

static int is_busy (int rc) { return (rc & 0xff) == SQLITE_BUSY; }
static sqlite3 *open_uri (const char *uri) {
    sqlite3 *d = NULL;
    CHECK_RC(sqlite3_open_v2(uri, &d, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL), SQLITE_OK);
    return d;
}
static void two (const char *tag, char *path, char *uri, size_t un, sqlite3 **a, sqlite3 **b) {
    mw_tmpdb(path, 256, tag);
    snprintf(uri, un, "file:%s?vfs=multiwriter&mw_rebase=1&mw_rebase_backoff=0%s", path, getenv("MW_TEST_MP") ? "&mw_mp=1" : "");
    *a = open_uri(uri); *b = open_uri(uri);
}

int main (void) {
    char path[256], uri[400]; sqlite3 *a, *b;

    // 1. a SELECT cursor keeps the snapshot while a writer commits a row of the page and the connection updates two rows of it, one statement at a time
    two("rc1", path, uri, sizeof uri, &a, &b);
    CHECK_RC(mw_exec(a, "CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER); INSERT INTO t VALUES (1,0),(2,0),(3,0),(4,0),(5,0)"), SQLITE_OK);
    sqlite3_stmt *cur = NULL; CHECK_RC(sqlite3_prepare_v2(a, "SELECT id FROM t", -1, &cur, NULL), SQLITE_OK); CHECK_RC(sqlite3_step(cur), SQLITE_ROW);
    CHECK_RC(mw_exec(b, "UPDATE t SET n = 100 WHERE id = 2"), SQLITE_OK);
    int u1 = mw_exec(a, "UPDATE t SET n = n + 1 WHERE id = 1");        // conflicts on the page: rebased
    int u2 = mw_exec(a, "UPDATE t SET n = n + 1 WHERE id = 3");        // built on the page of this connection: must not commit over row 2
    printf("1. cursor open: updates rc=%d, %d; row 2 = %ld\n", u1, u2, (long)mw_scalar(b, "SELECT n FROM t WHERE id = 2"));
    CHECK(u1 == SQLITE_OK || is_busy(u1));
    CHECK(u2 != SQLITE_OK || mw_scalar(b, "SELECT n FROM t WHERE id = 2") == 100);
    sqlite3_finalize(cur);
    CHECK(mw_scalar(b, "SELECT n FROM t WHERE id = 2") == 100);       // (the row of the other writer is there in every case)
    if (u2 != SQLITE_OK) { CHECK(is_busy(u2)); CHECK_RC(mw_exec(a, "UPDATE t SET n = n + 1 WHERE id = 3"), SQLITE_OK); }     // (and the retry works)
    CHECK(mw_scalar(a, "SELECT n FROM t WHERE id = 3") == 1 && mw_scalar(a, "SELECT n FROM t WHERE id = 2") == 100);
    sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);

    // 2. the two doctors on call, with the SELECT as the plain text, behind a block comment, and behind a line comment: the second to commit is refused in the three cases
    const char *pre[] = { "", "/* req:1 */ ", "-- trace\n", "/* a */ /* b */ -- c\n  ", "/* w */ WITH x AS (SELECT 1) " };
    for (unsigned k = 0; k < sizeof pre / sizeof *pre; k++) {
        two("rc2", path, uri, sizeof uri, &a, &b);
        CHECK_RC(mw_exec(a, "CREATE TABLE d(id INTEGER PRIMARY KEY, oncall INTEGER); INSERT INTO d VALUES (1,1),(2,1)"), SQLITE_OK);
        char q[200]; snprintf(q, sizeof q, "%s%s", pre[k], k == 4 ? "SELECT count(*) FROM d, x WHERE oncall = 1" : "SELECT count(*) FROM d WHERE oncall = 1");
        CHECK_RC(mw_exec(a, "BEGIN"), SQLITE_OK); CHECK_RC(mw_exec(b, "BEGIN"), SQLITE_OK);
        CHECK_RC(mw_exec(a, q), SQLITE_OK); CHECK_RC(mw_exec(b, q), SQLITE_OK);
        CHECK_RC(mw_exec(a, "UPDATE d SET oncall = 0 WHERE id = 1"), SQLITE_OK); CHECK_RC(mw_exec(b, "UPDATE d SET oncall = 0 WHERE id = 2"), SQLITE_OK);
        int r1 = mw_exec(a, "COMMIT"), r2 = mw_exec(b, "COMMIT");
        printf("2. prefix %u: commits rc=%d, %d; on call afterwards %ld\n", k, r1, r2, (long)mw_scalar(a, "SELECT count(*) FROM d WHERE oncall = 1"));
        CHECK_RC(r1, SQLITE_OK); CHECK(is_busy(r2));
        mw_exec(b, "ROLLBACK");
        sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);
    }
    // a DDL statement with a comment in front takes the schema barrier like one without (the statement hook used to look at the first byte)
    two("rc2d", path, uri, sizeof uri, &a, &b);
    CHECK_RC(mw_exec(a, "/* migration 7 */ CREATE TABLE t(id INTEGER PRIMARY KEY, v)"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "-- add rows\nINSERT INTO t VALUES (1, 'x')"), SQLITE_OK);
    CHECK(mw_scalar(a, "SELECT count(*) FROM t") == 1);
    sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);

    // 3. user_version in the transaction: the row change conflicts on the page; the commit must not be replayed without the header field (refused, and the retry carries both)
    two("rc3", path, uri, sizeof uri, &a, &b);
    CHECK_RC(mw_exec(a, "CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER); INSERT INTO t VALUES (1,0),(2,0)"), SQLITE_OK);
    CHECK_RC(mw_exec(a, "BEGIN"), SQLITE_OK); CHECK_RC(mw_exec(b, "BEGIN"), SQLITE_OK);
    CHECK_RC(mw_exec(a, "UPDATE t SET n = n + 1 WHERE id = 1"), SQLITE_OK); CHECK_RC(mw_exec(a, "PRAGMA user_version = 7"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "UPDATE t SET n = n + 1 WHERE id = 2"), SQLITE_OK); CHECK_RC(mw_exec(b, "COMMIT"), SQLITE_OK);
    int rv = mw_exec(a, "COMMIT");
    int64_t uv = mw_scalar(a, "PRAGMA user_version");
    printf("3. user_version: commit rc=%d, user_version=%ld\n", rv, (long)uv);
    CHECK(rv != SQLITE_OK ? (is_busy(rv) && uv == 0) : uv == 7);    // never "done" with the field lost
    if (rv != SQLITE_OK) {
        mw_exec(a, "ROLLBACK");
        CHECK_RC(mw_exec(a, "BEGIN; UPDATE t SET n = n + 1 WHERE id = 1; PRAGMA user_version = 7; COMMIT"), SQLITE_OK);
        CHECK(mw_scalar(a, "PRAGMA user_version") == 7 && mw_scalar(a, "SELECT n FROM t WHERE id = 1") == 1);
    }
    sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);
        // 4. the answer "this statement changes one row" depends on the schema: the same text with a UNIQUE index is a point statement, and without it (the index is dropped) it reads the table.
    //    A row that another writer inserts meanwhile has to be seen by it: the commit is refused, not replayed without that row.
    two("rc4", path, uri, sizeof uri, &a, &b);
    CHECK_RC(mw_exec(a, "CREATE TABLE t(id INTEGER PRIMARY KEY, email TEXT, n INTEGER); CREATE UNIQUE INDEX ie ON t(email); INSERT INTO t VALUES (1,'a',0),(2,'b',0),(3,'c',0)"), SQLITE_OK);
    const char *upd = "UPDATE t SET n = n + 1 WHERE email = 'a'";
    CHECK_RC(mw_exec(a, upd), SQLITE_OK);
    CHECK_RC(mw_exec(a, "DROP INDEX ie"), SQLITE_OK);
    CHECK_RC(mw_exec(a, "BEGIN"), SQLITE_OK); CHECK_RC(mw_exec(b, "BEGIN"), SQLITE_OK);
    CHECK_RC(mw_exec(a, upd), SQLITE_OK);
    CHECK_RC(mw_exec(b, "INSERT INTO t VALUES (4,'a',0)"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "COMMIT"), SQLITE_OK);
    int r4 = mw_exec(a, "COMMIT");
    printf("4. after DROP INDEX: commit rc=%d\n", r4);
    CHECK(is_busy(r4));
    mw_exec(a, "ROLLBACK");
    CHECK_RC(mw_exec(a, upd), SQLITE_OK);                                   // (the retry sees both rows)
    CHECK(mw_scalar(a, "SELECT n FROM t WHERE id = 4") == 1 && mw_scalar(a, "SELECT n FROM t WHERE id = 1") == 2);
    sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);
    MW_DONE();
}
