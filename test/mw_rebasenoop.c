// What the page-level replay cannot see: a statement that changes a row to what it already is (counted as a change, writes no page), and a statement that is still open (RETURNING stepped once) when the
// transaction commits. Both read a row that the replay would not check; the transaction must not be rebased. Two connections of one process; B commits first while A is open.
#include "mw_test.h"
#include "multiwriter.h"

static int open_mw (const char *path, sqlite3 **db) {
    char uri[300]; snprintf(uri, sizeof uri, "file:%s?mw=2&mw_rebase=1&mw_rebase_backoff=0%s", path, getenv("MW_TEST_MP") ? "&mw_mp=1" : "");
    int rc = sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
    if (rc == SQLITE_OK) { sqlite3_extended_result_codes(*db, 1); sqlite3_busy_timeout(*db, 0); }
    return rc;
}
static mw_db_stats stats (sqlite3 *db) { mw_db_stats s; memset(&s, 0, sizeof s); sqlite3_file_control(db, "main", MW_FCNTL_DBSTATS, &s); return s; }
static int is_conflict (int rc) { return (rc & 0xff) == SQLITE_BUSY; }
static void make (const char *path) {
    sqlite3 *s; CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT);"
        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<40) INSERT INTO t SELECT i, i, 'row'||i FROM n; UPDATE t SET a = 1000 WHERE id = 3"), SQLITE_OK);
    sqlite3_close(s);
}

int main (void) {
    char path[256]; sqlite3 *a, *b; int rc;

    // 1. write skew through a no-op: A sets row 3 to 1000 (it is 1000) and changes row 4; B reads row 4 and changes row 3. No serial order gives this result: refused.
    mw_tmpdb(path, sizeof path, "noop1"); make(path);
    CHECK_RC(open_mw(path, &a), SQLITE_OK); CHECK_RC(open_mw(path, &b), SQLITE_OK);
    CHECK_RC(mw_exec(a, "BEGIN; UPDATE t SET a = 1000 WHERE id = 3; UPDATE t SET a = a + 1 WHERE id = 4"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "BEGIN"), SQLITE_OK); CHECK(mw_scalar(b, "SELECT a FROM t WHERE id = 4") == 4);
    CHECK_RC(mw_exec(b, "UPDATE t SET a = 99 WHERE id = 3; COMMIT"), SQLITE_OK);
    rc = mw_exec(a, "COMMIT");
    printf("1. a no-op update and a read of the other: rc=%d, rebases %llu\n", rc, (unsigned long long)stats(a).rebases);
    CHECK(is_conflict(rc)); CHECK(stats(a).rebases == 0); if (rc != SQLITE_OK) mw_exec(a, "ROLLBACK");
    CHECK(mw_scalar(a, "SELECT a FROM t WHERE id = 3") == 99 && mw_scalar(a, "SELECT a FROM t WHERE id = 4") == 4);
    sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);

    // 2. the same statement, but it does change the row: replayed (the control)
    mw_tmpdb(path, sizeof path, "noop2"); make(path);
    CHECK_RC(open_mw(path, &a), SQLITE_OK); CHECK_RC(open_mw(path, &b), SQLITE_OK);
    CHECK_RC(mw_exec(a, "BEGIN; UPDATE t SET a = 1001 WHERE id = 3; UPDATE t SET a = a + 1 WHERE id = 4"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "BEGIN; UPDATE t SET a = 99 WHERE id = 30; COMMIT"), SQLITE_OK);
    rc = mw_exec(a, "COMMIT");
    printf("2. the same, a real change: rc=%d, rebases %llu\n", rc, (unsigned long long)stats(a).rebases);
    CHECK_RC(rc, SQLITE_OK); CHECK(stats(a).rebases == 1);
    sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);

    // 3. a row changed twice (counted twice, one change in the pages) is not replayed either: it cannot be told from a no-op
    mw_tmpdb(path, sizeof path, "noop3"); make(path);
    CHECK_RC(open_mw(path, &a), SQLITE_OK); CHECK_RC(open_mw(path, &b), SQLITE_OK);
    CHECK_RC(mw_exec(a, "BEGIN; UPDATE t SET a = a + 1 WHERE id = 5; UPDATE t SET a = a + 1 WHERE id = 5"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "BEGIN; UPDATE t SET a = 99 WHERE id = 30; COMMIT"), SQLITE_OK);
    rc = mw_exec(a, "COMMIT");
    printf("3. a row changed twice: rc=%d, rebases %llu\n", rc, (unsigned long long)stats(a).rebases);
    CHECK(is_conflict(rc)); if (rc != SQLITE_OK) mw_exec(a, "ROLLBACK");
    sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);

    // 4. a statement with RETURNING, stepped once and still open at the COMMIT: SQLite does not even let the transaction commit (SQLITE_BUSY, "SQL statements in progress"), so no commit reaches the engine with one open;
    //    (and it is not a point statement anyway: the transaction would not be rebased)
    mw_tmpdb(path, sizeof path, "noop4"); make(path);
    CHECK_RC(open_mw(path, &a), SQLITE_OK); CHECK_RC(open_mw(path, &b), SQLITE_OK);
    CHECK_RC(mw_exec(a, "BEGIN"), SQLITE_OK);
    sqlite3_stmt *st; CHECK_RC(sqlite3_prepare_v2(a, "UPDATE t SET a = a + 1 WHERE id IN (6, 7) RETURNING a", -1, &st, NULL), SQLITE_OK);
    CHECK_RC(sqlite3_step(st), SQLITE_ROW);                                                   // (one row seen, the statement is open)
    CHECK_RC(mw_exec(b, "BEGIN; UPDATE t SET a = 99 WHERE id = 30; COMMIT"), SQLITE_OK);
    rc = mw_exec(a, "COMMIT");
    printf("4. RETURNING open at the commit: rc=%d, rebases %llu\n", rc, (unsigned long long)stats(a).rebases);
    CHECK_RC(rc, SQLITE_BUSY); CHECK(stats(a).rebases == 0);
    sqlite3_finalize(st); if (rc != SQLITE_OK) mw_exec(a, "ROLLBACK");
    sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);

    MW_DONE();
}
