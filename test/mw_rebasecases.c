// Ways in which a rebased commit used to lose something (found by a review of the sources, each reproduced before it was fixed):
//   1. after a commit of a snapshot was rebased, a later commit of the same snapshot was built on the connection's own page and overwrote what the other writer had committed (lost update);
//   2. a comment in front of a SELECT ("/* req:1 */ SELECT ...") hid that the transaction had read: write skew was let through with the rebase;
//   3. a field of the header of the file that the application sets (PRAGMA user_version) was dropped by the replay, which has the row changes only, and the commit was reported as done.
#include "mw_test.h"
#include "multiwriter.h"
#include "multiwriter_internal.h"

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

    // 5. the helper that replays a commit has the synchronous level of the connection (OFF was never applied: the level that it was given at the start, 0, was taken for "set already")
    for (int level = 0; level <= 2; level++) {
        two("rc5", path, uri, sizeof uri, &a, &b);
        CHECK_RC(mw_exec(a, "CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER); INSERT INTO t VALUES (1,0),(2,0)"), SQLITE_OK);
        char q[40]; snprintf(q, sizeof q, "PRAGMA synchronous = %d", level);
        CHECK_RC(mw_exec(a, q), SQLITE_OK); CHECK_RC(mw_exec(b, q), SQLITE_OK);
        CHECK_RC(mw_exec(a, "BEGIN"), SQLITE_OK); CHECK_RC(mw_exec(b, "BEGIN"), SQLITE_OK);
        CHECK_RC(mw_exec(a, "UPDATE t SET n = n + 1 WHERE id = 1"), SQLITE_OK); CHECK_RC(mw_exec(b, "UPDATE t SET n = n + 1 WHERE id = 2"), SQLITE_OK);
        CHECK_RC(mw_exec(b, "COMMIT"), SQLITE_OK);
        CHECK_RC(mw_exec(a, "COMMIT"), SQLITE_OK);                          // rebased
        void *lp = NULL; CHECK_RC(sqlite3_file_control(a, "main", MW_FCNTL_LANE_PTR, &lp), SQLITE_OK);
        mw_lane *lane = lp;
        int64_t got = lane && lane->rb_db ? mw_scalar(lane->rb_db, "PRAGMA synchronous") : -1;
        printf("5. synchronous=%d: helper has %lld\n", level, (long long)got);
        CHECK(got == level);
        sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);
    }
    // 6. INTEGER PRIMARY KEY DESC is not the alias of the rowid: x is a column of the record (it was taken for the rowid, and stored as NULL by the replay, with the UNIQUE conflict lost)
    two("rc6", path, uri, sizeof uri, &a, &b);
    CHECK_RC(mw_exec(a, "CREATE TABLE t(x INTEGER PRIMARY KEY DESC, a, b DEFAULT 'd'); WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<30) INSERT INTO t(x, a) SELECT i*10, i FROM n"), SQLITE_OK);
    CHECK_RC(mw_exec(a, "BEGIN"), SQLITE_OK); CHECK_RC(mw_exec(b, "BEGIN"), SQLITE_OK);
    CHECK_RC(mw_exec(a, "INSERT INTO t(rowid, x, a) VALUES (101, 1001, 1)"), SQLITE_OK); CHECK_RC(mw_exec(b, "INSERT INTO t(rowid, x, a) VALUES (102, 1002, 2)"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "COMMIT"), SQLITE_OK);
    int r6 = mw_exec(a, "COMMIT");
    printf("6. DESC key, two inserts: rc=%d, x of the row of A = %lld\n", r6, (long long)mw_scalar(a, "SELECT x FROM t WHERE rowid = 101"));
    if (r6 != SQLITE_OK) CHECK_RC(mw_exec(a, "INSERT INTO t(rowid, x, a) VALUES (101, 1001, 1)"), SQLITE_OK);
    CHECK(mw_scalar(a, "SELECT count(*) FROM t WHERE rowid = 101 AND x = 1001 AND a = 1") == 1);
    CHECK(mw_scalar(a, "SELECT count(*) FROM t WHERE x IS NULL") == 0);
    sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);
    two("rc6u", path, uri, sizeof uri, &a, &b);
    CHECK_RC(mw_exec(a, "CREATE TABLE t(x INTEGER PRIMARY KEY DESC, a); WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<30) INSERT INTO t(x, a) SELECT i*10, i FROM n"), SQLITE_OK);
    CHECK_RC(mw_exec(a, "BEGIN"), SQLITE_OK); CHECK_RC(mw_exec(b, "BEGIN"), SQLITE_OK);
    CHECK_RC(mw_exec(a, "INSERT INTO t(rowid, x, a) VALUES (101, 1001, 1)"), SQLITE_OK); CHECK_RC(mw_exec(b, "INSERT INTO t(rowid, x, a) VALUES (102, 1001, 2)"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "COMMIT"), SQLITE_OK);
    r6 = mw_exec(a, "COMMIT");
    CHECK(r6 != SQLITE_OK);                                                  // the same x: a conflict, never a row with x NULL
    mw_exec(a, "ROLLBACK");
    CHECK((mw_exec(a, "INSERT INTO t(rowid, x, a) VALUES (101, 1001, 1)") & 0xff) == SQLITE_CONSTRAINT);
    sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);

    // 7. a column with no affinity: 1 and 1.0 are IS-equal and are not the same value (B changes 1 into 1.0, A divides it by two as it saw it)
    two("rc7", path, uri, sizeof uri, &a, &b);
    CHECK_RC(mw_exec(a, "CREATE TABLE t(id INTEGER PRIMARY KEY, a, c); INSERT INTO t VALUES (1, NULL, 1), (2, NULL, 2)"), SQLITE_OK);
    CHECK_RC(mw_exec(a, "BEGIN"), SQLITE_OK); CHECK_RC(mw_exec(b, "BEGIN"), SQLITE_OK);
    CHECK_RC(mw_exec(a, "UPDATE t SET a = c / 2 WHERE id = 1"), SQLITE_OK); CHECK_RC(mw_exec(b, "UPDATE t SET c = 1.0 WHERE id = 1"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "COMMIT"), SQLITE_OK);
    int r7 = mw_exec(a, "COMMIT");
    printf("7. 1 against 1.0: rc=%d\n", r7);
    CHECK(is_busy(r7));                                                     // the row is not the one that A saw
    mw_exec(a, "ROLLBACK");
    CHECK_RC(mw_exec(a, "UPDATE t SET a = c / 2 WHERE id = 1"), SQLITE_OK);
    CHECK(mw_scalar(a, "SELECT a * 10 FROM t WHERE id = 1") == 5);          // 0.5, as in the serial order
    sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);

    // 8. WITHOUT ROWID is what SQLite says it is, not what the text of the CREATE TABLE contains
    for (int k = 0; k < 3; k++) {
        const char *ddl[] = {
            "CREATE TABLE w(a, b TEXT DEFAULT 'WITHOUT ROWID', c, PRIMARY KEY(b, a))",        // a rowid table that says so in a default
            "CREATE TABLE w(a, b, c, PRIMARY KEY(b, a)) WITHOUT  ROWID",                       // a real one, with two blanks
            "CREATE TABLE w(a, b, c, PRIMARY KEY(b, a)) WITHOUT /* c */ ROWID",               // and a comment in it
        };
        two("rc8", path, uri, sizeof uri, &a, &b);
        char q[400]; snprintf(q, sizeof q, "%s; INSERT INTO w(a, b, c) VALUES (1, 'x', 'c1'), (2, 'y', 'c2'), (3, 'z', 'c3')", ddl[k]);
        CHECK_RC(mw_exec(a, q), SQLITE_OK);
        CHECK_RC(mw_exec(a, "BEGIN"), SQLITE_OK); CHECK_RC(mw_exec(b, "BEGIN"), SQLITE_OK);
        // (a rowid table gets explicit rowids: two inserts that take the next one are a true conflict)
        CHECK_RC(mw_exec(a, k == 0 ? "INSERT INTO w(rowid, a, b, c) VALUES (110, 10, 'p', 'A')" : "INSERT INTO w(a, b, c) VALUES (10, 'p', 'A')"), SQLITE_OK);
        CHECK_RC(mw_exec(b, k == 0 ? "INSERT INTO w(rowid, a, b, c) VALUES (120, 20, 'q', 'B')" : "INSERT INTO w(a, b, c) VALUES (20, 'q', 'B')"), SQLITE_OK);
        CHECK_RC(mw_exec(b, "COMMIT"), SQLITE_OK);
        int r8 = mw_exec(a, "COMMIT");
        mw_db_stats st; memset(&st, 0, sizeof st); sqlite3_file_control(a, "main", MW_FCNTL_DBSTATS, &st);
        printf("8. table %d: commit rc=%d, rebases %llu\n", k, r8, (unsigned long long)st.rebases);
        CHECK_RC(r8, SQLITE_OK);
        CHECK(st.rebases >= 1);                                             // replayed (it was refused when the text was misread)
        CHECK(mw_scalar(a, "SELECT count(*) FROM w") == 5 && mw_scalar(a, "SELECT count(*) FROM w WHERE (a=10 AND b='p' AND c='A') OR (a=20 AND b='q' AND c='B')") == 2);
        sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);
    }

    // 9. pages with reserved bytes (a checksum or an encryption extension): the cells that the decoder reads would not end where SQLite's do; the rebase is not used, whatever the size of the row
    mw_tmpdb(path, 256, "rc9");
    {
        sqlite3 *s; CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, MW_PLAIN_VFS), SQLITE_OK);
        int rsv = 32; CHECK_RC(sqlite3_file_control(s, "main", SQLITE_FCNTL_RESERVE_BYTES, &rsv), SQLITE_OK);
        CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL"), SQLITE_OK);
        CHECK_RC(mw_exec(s, "CREATE TABLE t(id INTEGER PRIMARY KEY, b BLOB); INSERT INTO t VALUES (1, zeroblob(10)), (2, zeroblob(10)), (3, zeroblob(10))"), SQLITE_OK);
        CHECK_RC(mw_exec(s, "VACUUM"), SQLITE_OK);
        CHECK_RC(mw_exec(s, "PRAGMA wal_checkpoint(TRUNCATE)"), SQLITE_OK);
        sqlite3_close(s);
    }
    snprintf(uri, sizeof uri, "file:%s?vfs=multiwriter&mw_rebase=1&mw_rebase_backoff=0", path);
    a = open_uri(uri); b = open_uri(uri);
    CHECK_RC(mw_exec(a, "BEGIN"), SQLITE_OK); CHECK_RC(mw_exec(b, "BEGIN"), SQLITE_OK);
    CHECK_RC(mw_exec(a, "UPDATE t SET b = zeroblob(10) || x'01' WHERE id = 1"), SQLITE_OK); CHECK_RC(mw_exec(b, "UPDATE t SET b = zeroblob(10) || x'02' WHERE id = 2"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "COMMIT"), SQLITE_OK);
    int r9 = mw_exec(a, "COMMIT");
    mw_db_stats st9; memset(&st9, 0, sizeof st9); sqlite3_file_control(a, "main", MW_FCNTL_DBSTATS, &st9);
    printf("9. reserved bytes: commit rc=%d, rebases %llu\n", r9, (unsigned long long)st9.rebases);
    CHECK(is_busy(r9));
    CHECK(st9.rebases == 0);
    sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);
    // 10. the catalog that the replay uses is built once for the database and kept for its schema cookie: a connection that rebases after another one changed the schema (ADD COLUMN) must have the
    //     new one, not the one that the others built (a stale one refuses every rebase, for ever), and a connection that has not rebased before gets the shared one
    {
        two("rc10", path, uri, sizeof uri, &a, &b);
        sqlite3 *c = open_uri(uri), *d = open_uri(uri);
        CHECK_RC(mw_exec(a, "CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER); INSERT INTO t VALUES (1,0),(2,0),(3,0),(4,0),(5,0)"), SQLITE_OK);
        mw_db_stats st0; memset(&st0, 0, sizeof st0);
        for (int round = 0; round < 2; round++) {
            sqlite3 *x = round == 0 ? a : c, *y = round == 0 ? b : d;                              // (round 1: connections that have never rebased, after the schema change)
            CHECK_RC(mw_exec(x, "BEGIN"), SQLITE_OK); CHECK_RC(mw_exec(x, "UPDATE t SET n = n + 1 WHERE id = 1"), SQLITE_OK);
            CHECK_RC(mw_exec(y, "UPDATE t SET n = n + 1 WHERE id = 5"), SQLITE_OK);
            CHECK_RC(mw_exec(x, "COMMIT"), SQLITE_OK);
            if (round == 0) CHECK_RC(mw_exec(b, "ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'z'"), SQLITE_OK);
        }
        CHECK(mw_scalar(a, "SELECT n FROM t WHERE id = 1") == 2 && mw_scalar(a, "SELECT n FROM t WHERE id = 5") == 2);
        // a again, with the new schema (its catalog is the old one: the cookie changed)
        CHECK_RC(mw_exec(a, "BEGIN"), SQLITE_OK); CHECK_RC(mw_exec(a, "UPDATE t SET n = n + 1, extra = 'q' WHERE id = 2"), SQLITE_OK);
        CHECK_RC(mw_exec(b, "UPDATE t SET n = n + 1 WHERE id = 4"), SQLITE_OK);
        CHECK_RC(mw_exec(a, "COMMIT"), SQLITE_OK);
        CHECK(mw_scalar(a, "SELECT n FROM t WHERE id = 2") == 1 && mw_scalar(a, "SELECT n FROM t WHERE id = 4") == 1);
        mw_db_stats st; memset(&st, 0, sizeof st); sqlite3_file_control(a, "main", MW_FCNTL_DBSTATS, &st);
        printf("10. shared catalog: rebases %llu\n", (unsigned long long)st.rebases);
        CHECK(st.rebases >= 3);
        sqlite3_close(a); sqlite3_close(b); sqlite3_close(c); sqlite3_close(d); mw_rmdb(path);
    }
    MW_DONE();
}
