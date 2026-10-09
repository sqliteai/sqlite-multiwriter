// Write skew. A transaction that reads a page that another commit rewrote after its snapshot is refused, which is what stops the pair "T1 reads A and writes B, T2 reads B and writes A":
// the second to commit read a page that the first rewrote. Rows of 3000 bytes: every row has a page of its own, so A and B are on different pages (the pages are not what conflicts).
// With mw_readcheck=0 the validation of the reads is off, and the pair goes through (the documented price of that option).
#include "mw_test.h"
#include "multiwriter.h"

static int is_busy (int rc) { return (rc & 0xff) == SQLITE_BUSY; }
static sqlite3 *open_uri (const char *uri) {
    sqlite3 *d = NULL;
    CHECK_RC(sqlite3_open_v2(uri, &d, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL), SQLITE_OK);
    return d;
}
static void fill (sqlite3 *a) {
    CHECK_RC(mw_exec(a, "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER, pad BLOB); CREATE TABLE u(id INTEGER PRIMARY KEY, seen INTEGER)"), SQLITE_OK);
    CHECK_RC(mw_exec(a, "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<20) INSERT INTO t SELECT i, 0, zeroblob(3000) FROM n"), SQLITE_OK);
}

// T1 reads row 1 and writes row 20; T2 reads row 20 and writes row 1. `how`: 0 point reads, 1 reads of a list of rows, 2 reads the range.
static void pair (const char *tag, const char *extra, int warm, int how, int want_skew) {
    char path[256], uri[400]; mw_tmpdb(path, sizeof path, tag);
    snprintf(uri, sizeof uri, "file:%s?vfs=multiwriter%s%s", path, extra, getenv("MW_TEST_MP") ? "&mw_mp=1" : "");
    sqlite3 *a = open_uri(uri), *b = open_uri(uri);
    fill(a);
    if (warm) { mw_scalar(a, "SELECT sum(v) FROM t"); mw_scalar(b, "SELECT sum(v) FROM t"); }       // (the page cache of both holds both rows)
    CHECK_RC(mw_exec(a, "BEGIN"), SQLITE_OK); CHECK_RC(mw_exec(b, "BEGIN"), SQLITE_OK);
    const char *ra = how == 0 ? "SELECT v FROM t WHERE id=1" : how == 1 ? "SELECT sum(v) FROM t WHERE id IN (1,2)" : "SELECT sum(v) FROM t WHERE id BETWEEN 1 AND 2";
    const char *rb = how == 0 ? "SELECT v FROM t WHERE id=20" : how == 1 ? "SELECT sum(v) FROM t WHERE id IN (19,20)" : "SELECT sum(v) FROM t WHERE id BETWEEN 19 AND 20";
    mw_scalar(a, ra); mw_scalar(b, rb);
    CHECK_RC(mw_exec(a, "UPDATE t SET v = 1 WHERE id = 20"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "UPDATE t SET v = 1 WHERE id = 1"), SQLITE_OK);
    int r1 = mw_exec(a, "COMMIT"), r2 = mw_exec(b, "COMMIT");
    printf("%-12s %-34s reads %d: T1 rc=%d, T2 rc=%d\n", tag, extra, how, r1, r2);
    CHECK_RC(r1, SQLITE_OK);
    if (want_skew) CHECK_RC(r2, SQLITE_OK);
    else { CHECK(is_busy(r2)); mw_exec(b, "ROLLBACK"); CHECK_RC(mw_exec(b, "UPDATE t SET v = 1 WHERE id = 1"), SQLITE_OK); }      // (and the retry works)
    sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);
}

// A row that is read as absent: T1 reads that `u` is empty and appends a row to `t` (the new row opens a page); T2 reads max(id) of `t` and writes `u`.
static void phantom (const char *extra, int rowsize) {
    char path[256], uri[400], sql[300]; mw_tmpdb(path, sizeof path, "wsph");
    snprintf(uri, sizeof uri, "file:%s?vfs=multiwriter%s%s", path, extra, getenv("MW_TEST_MP") ? "&mw_mp=1" : "");
    sqlite3 *a = open_uri(uri), *b = open_uri(uri);
    CHECK_RC(mw_exec(a, "CREATE TABLE t(id INTEGER PRIMARY KEY, pad BLOB); CREATE TABLE u(id INTEGER PRIMARY KEY, seen INTEGER)"), SQLITE_OK);
    snprintf(sql, sizeof sql, "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<20) INSERT INTO t SELECT i, zeroblob(%d) FROM n", rowsize);
    CHECK_RC(mw_exec(a, sql), SQLITE_OK);
    CHECK_RC(mw_exec(a, "BEGIN"), SQLITE_OK); CHECK_RC(mw_exec(b, "BEGIN"), SQLITE_OK);
    CHECK(mw_scalar(a, "SELECT count(*) FROM u") == 0);
    long mx = mw_scalar(b, "SELECT max(id) FROM t");
    snprintf(sql, sizeof sql, "INSERT INTO t VALUES (21, zeroblob(%d))", rowsize); CHECK_RC(mw_exec(a, sql), SQLITE_OK);
    snprintf(sql, sizeof sql, "INSERT INTO u VALUES (1, %ld)", mx); CHECK_RC(mw_exec(b, sql), SQLITE_OK);
    int r1 = mw_exec(a, "COMMIT"), r2 = mw_exec(b, "COMMIT");
    printf("append, rows of %d bytes%s: T1 rc=%d, T2 rc=%d\n", rowsize, extra, r1, r2);
    CHECK_RC(r1, SQLITE_OK); CHECK(is_busy(r2));
    sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);
}

int main (void) {
    pair("wsdefault", "", 0, 0, 0);
    pair("wswarm", "", 1, 0, 0);
    pair("wsrebase", "&mw_rebase=1", 1, 0, 0);
    pair("wslist", "", 0, 1, 0);
    pair("wsrange", "", 0, 2, 0);
    pair("wsoff", "&mw_readcheck=0", 0, 0, 1);               // the validation off: the pair commits
    phantom("", 3000);                                       // the append opens a new page
    phantom("", 40);                                         // the append fits in the page that was read
    phantom("&mw_rebase=1", 3000);
    MW_DONE();
}
