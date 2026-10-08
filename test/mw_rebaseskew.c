// Write skew and the rebase. Invariant: the rows 1 and 2 of t never both have a = 0 ("someone stays on call"). Each transaction reads both rows, then clears its own. The rows are on ONE page, so the second
// commit is a page conflict on a page that it wrote: exactly what the rebase replays. Without the rebase it is refused (the read set is validated by page). With it, the replay compares only the rows that the
// transaction changed (row 1 or 2, not the one it read), so the row it read and did not change must be validated too: the commit must be refused, and the application's retry sees the other change.
// Controls: blind writes of different rows are still replayed, and so is a transaction that read rows nobody changed.
#include "mw_test.h"
#include "multiwriter.h"

static int open_mw (const char *path, sqlite3 **db, int rebase) {
    char uri[300]; snprintf(uri, sizeof uri, "file:%s?mw=2&mw_rebase=%d&mw_rebase_backoff=0&mw_gc=0", path, rebase);
    int rc = sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
    if (rc == SQLITE_OK) { sqlite3_extended_result_codes(*db, 1); sqlite3_busy_timeout(*db, 0); }
    return rc;
}
static mw_db_stats stats (sqlite3 *db) { mw_db_stats s; memset(&s, 0, sizeof s); sqlite3_file_control(db, "main", MW_FCNTL_DBSTATS, &s); return s; }
static int is_conflict (int rc) { return (rc & 0xff) == SQLITE_BUSY; }
static void make (const char *path) {
    sqlite3 *s; CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT);"
        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<40) INSERT INTO t SELECT i, 1, 'row'||i FROM n"), SQLITE_OK);
    sqlite3_close(s);
}

int main (void) {
    char path[256];
    for (int rebase = 0; rebase <= 1; rebase++) {
        // 1. the write skew: both read rows 1 and 2 (a = 1 each), A clears row 1, B clears row 2, B commits first
        mw_tmpdb(path, sizeof path, "skew"); make(path);
        sqlite3 *a, *b; CHECK_RC(open_mw(path, &a, rebase), SQLITE_OK); CHECK_RC(open_mw(path, &b, rebase), SQLITE_OK);
        CHECK_RC(mw_exec(a, "BEGIN"), SQLITE_OK); CHECK_RC(mw_exec(b, "BEGIN"), SQLITE_OK);
        CHECK(mw_scalar(a, "SELECT sum(a) FROM t WHERE id IN (1,2)") == 2);
        CHECK(mw_scalar(b, "SELECT sum(a) FROM t WHERE id IN (1,2)") == 2);
        CHECK_RC(mw_exec(a, "UPDATE t SET a = 0 WHERE id = 1"), SQLITE_OK);
        CHECK_RC(mw_exec(b, "UPDATE t SET a = 0 WHERE id = 2"), SQLITE_OK);
        CHECK_RC(mw_exec(b, "COMMIT"), SQLITE_OK);
        int rc = mw_exec(a, "COMMIT");
        int64_t sum = mw_scalar(b, "SELECT sum(a) FROM t WHERE id IN (1,2)");
        mw_db_stats st = stats(a);
        printf("1. write skew on one page, rebase %d: commit rc=%d, sum(a) of rows 1,2 = %lld, rebases %llu\n", rebase, rc, (long long)sum, (unsigned long long)st.rebases);
        CHECK(is_conflict(rc)); CHECK(sum >= 1);
        if (rc != SQLITE_OK) mw_exec(a, "ROLLBACK");
        // the retry sees B's change and keeps row 1 (the application's rule)
        CHECK(mw_scalar(a, "SELECT sum(a) FROM t WHERE id IN (1,2)") == 1);
        sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);
    }

    // 2. conservative: A read rows with a SELECT (that B did not touch) and changed another: the pages cannot say what it depended on, so it is refused, not replayed
    mw_tmpdb(path, sizeof path, "skew2"); make(path);
    sqlite3 *a, *b; CHECK_RC(open_mw(path, &a, 1), SQLITE_OK); CHECK_RC(open_mw(path, &b, 1), SQLITE_OK);
    CHECK_RC(mw_exec(a, "BEGIN"), SQLITE_OK); CHECK_RC(mw_exec(b, "BEGIN"), SQLITE_OK);
    CHECK(mw_scalar(a, "SELECT sum(a) FROM t WHERE id IN (1,2)") == 2);
    CHECK_RC(mw_exec(a, "UPDATE t SET a = 0 WHERE id = 1"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "UPDATE t SET a = 0 WHERE id = 30"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "COMMIT"), SQLITE_OK);
    CHECK(is_conflict(mw_exec(a, "COMMIT"))); mw_exec(a, "ROLLBACK");
    CHECK(stats(a).rebases == 0);
    sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);

    // 2b. one statement that reads more than it changes (a condition on other rows, a scan that skips rows) in autocommit mode: refused too; a statement that changes every row it visits is replayed
    const char *bad[] = { "UPDATE t SET a = 0 WHERE id = 1 AND (SELECT sum(a) FROM t WHERE id IN (1,2)) > 1",
                          "UPDATE t SET a = 0 WHERE id <= 3 AND b <> 'row2'",
                          "UPDATE t SET a = (SELECT max(a) + 1 FROM t) WHERE id = 1" };
    const char *good[] = { "UPDATE t SET a = a + 1 WHERE id = 1", "UPDATE t SET a = 7 WHERE id BETWEEN 1 AND 3", "DELETE FROM t WHERE id = 1", "INSERT INTO t VALUES(100, 1, 'x')" };
    for (int k = 0; k < 7; k++) {
        const char *sql = k < 3 ? bad[k] : good[k - 3];
        mw_tmpdb(path, sizeof path, "skew2b"); make(path);
        CHECK_RC(open_mw(path, &a, 1), SQLITE_OK); CHECK_RC(open_mw(path, &b, 1), SQLITE_OK);
        CHECK_RC(mw_exec(b, "BEGIN; UPDATE t SET a = 9 WHERE id = 30"), SQLITE_OK);
        // A's statement runs (as the open transaction) while B's change is not yet committed; B commits, then A
        CHECK_RC(mw_exec(a, "BEGIN"), SQLITE_OK); CHECK_RC(mw_exec(a, sql), SQLITE_OK);
        CHECK_RC(mw_exec(b, "COMMIT"), SQLITE_OK);
        int rc = mw_exec(a, "COMMIT");
        printf("2b. %s: %s\n", sql, rc == SQLITE_OK ? "replayed" : "refused");
        if (k < 3) CHECK(is_conflict(rc)); else CHECK_RC(rc, SQLITE_OK);
        sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);
    }

    // 3. control: blind writes of different rows (no reads) are replayed
    mw_tmpdb(path, sizeof path, "skew3"); make(path);
    CHECK_RC(open_mw(path, &a, 1), SQLITE_OK); CHECK_RC(open_mw(path, &b, 1), SQLITE_OK);
    CHECK_RC(mw_exec(a, "BEGIN; UPDATE t SET a = 5 WHERE id = 1"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "BEGIN; UPDATE t SET a = 5 WHERE id = 2"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "COMMIT"), SQLITE_OK);
    CHECK_RC(mw_exec(a, "COMMIT"), SQLITE_OK);
    CHECK(stats(a).rebases == 1);
    sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);

    MW_DONE();
}
