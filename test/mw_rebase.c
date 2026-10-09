// The rebase (mw_rebase=1): a commit that lost only on pages that it wrote is replayed row by row at the latest snapshot; a true conflict is refused. Two connections of one process, the second commits while
// the first one is open: deterministic cases of what is replayed, what is refused and what is never rebased. The randomised check that the result is serializable is mw_serial.
#include <stdbool.h>
#include <pthread.h>
#include "mw_test.h"
#include "multiwriter.h"

static int open_mw (const char *path, sqlite3 **db, int rebase) {
    char uri[300]; snprintf(uri, sizeof uri, "file:%s?mw=1&mw_rebase=%d&mw_rebase_backoff=0%s", path, rebase, getenv("MW_TEST_MP") ? "&mw_mp=1" : "");
    int rc = sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
    if (rc == SQLITE_OK) { sqlite3_extended_result_codes(*db, 1); sqlite3_busy_timeout(*db, 0); }
    return rc;
}
static mw_db_stats stats (sqlite3 *db) { mw_db_stats s; memset(&s, 0, sizeof s); sqlite3_file_control(db, "main", MW_FCNTL_DBSTATS, &s); return s; }
static int integrity_ok (sqlite3 *db) {
    sqlite3_stmt *st; int ok = 0;
    if (sqlite3_prepare_v2(db, "PRAGMA integrity_check", -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW) ok = strcmp((const char *)sqlite3_column_text(st, 0), "ok") == 0;
    sqlite3_finalize(st); return ok;
}
static int is_conflict (int rc) { return (rc & 0xff) == SQLITE_BUSY; }
static void make (const char *path, const char *ddl) {
    sqlite3 *s; CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, MW_PLAIN_VFS), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL"), SQLITE_OK);
    CHECK_RC(mw_exec(s, ddl), SQLITE_OK);
    sqlite3_close(s);
}
#define FILL "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<40) INSERT INTO t(id, a, b) SELECT i, i, 'row'||i FROM n"
static const char *TBL = "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, u INTEGER UNIQUE, c TEXT COLLATE NOCASE, r REAL); " FILL;
static const char *TBL_WR = "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, u INTEGER UNIQUE, c TEXT COLLATE NOCASE, r REAL) WITHOUT ROWID; " FILL;     // (MW_TEST_WR=1: all the cases with a WITHOUT ROWID table)

// A opens a transaction and changes something; B does the same and commits; then A commits: the result of A's commit is returned.
static int race (sqlite3 *a, sqlite3 *b, const char *sa, const char *sb) {
    CHECK_RC(mw_exec(a, "BEGIN"), SQLITE_OK); CHECK_RC(mw_exec(a, sa), SQLITE_OK);
    CHECK_RC(mw_exec(b, "BEGIN"), SQLITE_OK); CHECK_RC(mw_exec(b, sb), SQLITE_OK); CHECK_RC(mw_exec(b, "COMMIT"), SQLITE_OK);
    int rc = mw_exec(a, "COMMIT");
    if (rc != SQLITE_OK) mw_exec(a, "ROLLBACK");
    return rc;
}

// 9. the group replay: threads, each with its own rows on one page, run at once; the replays that queue together commit as one: nothing is lost, and the commits of one epoch have distinct places in it
static const char *g_path9; enum { T9 = 8, N9 = 150 };
typedef struct { int id; int ok; uint64_t ep[N9]; uint32_t ord[N9]; } w9_t;
static void *worker9 (void *arg) {
    w9_t *w = arg; sqlite3 *db; if (open_mw(g_path9, &db, 1) != SQLITE_OK) { mw_failures++; return NULL; }
    for (int i = 0; i < N9; i++) {
        char sql[100]; snprintf(sql, sizeof sql, "UPDATE t SET a = a + 1 WHERE id = %d", 1 + w->id);
        for (;;) { int rc = mw_exec(db, sql); if (rc == SQLITE_OK) break; if (!is_conflict(rc)) { mw_failures++; sqlite3_close(db); return NULL; } }
        mw_tx_info ti; memset(&ti, 0, sizeof ti); sqlite3_file_control(db, "main", MW_FCNTL_TXINFO, &ti); w->ep[w->ok] = ti.commit_epoch; w->ord[w->ok++] = ti.commit_order;
    }
    sqlite3_close(db); return NULL;
}
static int cmp9 (const void *a, const void *b) { const uint64_t *x = a, *y = b; return x[0] < y[0] ? -1 : x[0] > y[0] ? 1 : x[1] < y[1] ? -1 : x[1] > y[1]; }

int main (void) {
    char path[256];
    if (getenv("MW_TEST_WR")) TBL = TBL_WR;

    {
        mw_tmpdb(path, sizeof path, "rebase9"); make(path, TBL); g_path9 = path;
        sqlite3 *obs; CHECK_RC(open_mw(path, &obs, 1), SQLITE_OK);
        pthread_t th[T9]; static w9_t w[T9]; memset(w, 0, sizeof w);
        for (int i = 0; i < T9; i++) { w[i].id = i; pthread_create(&th[i], NULL, worker9, &w[i]); }
        for (int i = 0; i < T9; i++) pthread_join(th[i], NULL);
        mw_db_stats st = stats(obs);
        int64_t sum = mw_scalar(obs, "SELECT sum(a) FROM t WHERE id <= 8") - 36;
        static uint64_t keys[T9 * N9][2]; int nk = 0, dup = 0; for (int i = 0; i < T9; i++) for (int k = 0; k < w[i].ok; k++) { keys[nk][0] = w[i].ep[k]; keys[nk][1] = w[i].ord[k]; nk++; }
        qsort(keys, (size_t)nk, sizeof keys[0], cmp9); for (int i = 1; i < nk; i++) if (keys[i][0] == keys[i - 1][0] && keys[i][1] == keys[i - 1][1] && keys[i][0]) dup++;
        printf("9. %d threads x %d increments of own rows on one page: sum %lld, rebases %llu of which in groups %llu, lost races %llu, commits with the same epoch and place %d\n", T9, N9, (long long)sum, (unsigned long long)st.rebases, (unsigned long long)st.rebases_grouped, (unsigned long long)st.rebase_retries, dup);
        CHECK(sum == T9 * N9); CHECK(dup == 0); CHECK(st.rebases > 0); CHECK(st.rebases_grouped > 0); CHECK(integrity_ok(obs));
        sqlite3_close(obs); mw_rmdb(path);
    }


    // 1. different rows of one page: without the rebase the second commit is refused, with it it is replayed and both changes are there
    for (int rebase = 0; rebase <= 1; rebase++) {
        mw_tmpdb(path, sizeof path, "rebase1"); make(path, TBL);
        sqlite3 *a, *b; CHECK_RC(open_mw(path, &a, rebase), SQLITE_OK); CHECK_RC(open_mw(path, &b, rebase), SQLITE_OK);
        int rc = race(a, b, "UPDATE t SET a = 1000 WHERE id = 3", "UPDATE t SET a = 2000 WHERE id = 30");
        mw_db_stats st = stats(a);
        printf("1. different rows of a page, rebase %d: commit rc=%d, rebases %llu, page conflicts %llu\n", rebase, rc, (unsigned long long)st.rebases, (unsigned long long)st.page_conflicts);
        if (rebase) { CHECK_RC(rc, SQLITE_OK); CHECK(st.rebases == 1); CHECK(mw_scalar(a, "SELECT a FROM t WHERE id=3") == 1000); CHECK(mw_scalar(a, "SELECT a FROM t WHERE id=30") == 2000); }
        else { CHECK(is_conflict(rc)); CHECK(st.rebases == 0); CHECK(mw_scalar(a, "SELECT a FROM t WHERE id=3") == 3); }
        CHECK(integrity_ok(a)); sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);
    }

    // 2. the same row, other cells: refused (the whole row is compared: the transaction may have read the cells that it did not write); then the retry works
    {
        mw_tmpdb(path, sizeof path, "rebase2"); make(path, TBL);
        sqlite3 *a, *b; CHECK_RC(open_mw(path, &a, 1), SQLITE_OK); CHECK_RC(open_mw(path, &b, 1), SQLITE_OK);
        int rc = race(a, b, "UPDATE t SET a = 1000 WHERE id = 5", "UPDATE t SET b = 'other' WHERE id = 5");
        printf("2. the same row, other cells: commit rc=%d\n", rc);
        CHECK(is_conflict(rc)); CHECK(stats(a).rebases == 0);
        CHECK_RC(mw_exec(a, "UPDATE t SET a = 1000 WHERE id = 5"), SQLITE_OK);
        CHECK(mw_scalar(a, "SELECT a FROM t WHERE id=5") == 1000 && mw_scalar(a, "SELECT count(*) FROM t WHERE id=5 AND b='other'") == 1);
        sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);
    }

    // 2b. a statement with several rows in VALUES is not a point statement (the check says so without looking at its bytecode): the page conflict is refused, as without the rebase, and the retry works.
    // A single row with "),(" in a string is taken for one too: refused or replayed, never wrong.
    {
        mw_tmpdb(path, sizeof path, "rebase2b"); make(path, TBL);
        sqlite3 *a, *b; CHECK_RC(open_mw(path, &a, 1), SQLITE_OK); CHECK_RC(open_mw(path, &b, 1), SQLITE_OK);
        int rc = race(a, b, "INSERT INTO t(id, a, b) VALUES (101, 1, 'x'), (102, 2, 'y')", "UPDATE t SET a = 2000 WHERE id = 30");
        printf("2b. several rows in VALUES against another row of the page: commit rc=%d, rebases %llu\n", rc, (unsigned long long)stats(a).rebases);
        CHECK(is_conflict(rc)); CHECK(stats(a).rebases == 0);
        CHECK_RC(mw_exec(a, "INSERT INTO t(id, a, b) VALUES (101, 1, 'x'), (102, 2, 'y')"), SQLITE_OK);
        CHECK(mw_scalar(a, "SELECT count(*) FROM t WHERE id IN (101, 102)") == 2 && mw_scalar(a, "SELECT a FROM t WHERE id=30") == 2000);
        int rc2 = race(a, b, "INSERT INTO t(id, a, b) VALUES (103, 3, 'p),(q')", "UPDATE t SET a = 3000 WHERE id = 31");
        CHECK(rc2 == SQLITE_OK || is_conflict(rc2));
        if (rc2 != SQLITE_OK) CHECK_RC(mw_exec(a, "INSERT INTO t(id, a, b) VALUES (103, 3, 'p),(q')"), SQLITE_OK);
        CHECK(mw_scalar(a, "SELECT count(*) FROM t WHERE id = 103") == 1 && mw_scalar(a, "SELECT a FROM t WHERE id=31") == 3000);
        CHECK(integrity_ok(a)); sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);
    }

    // 3. a row that was deleted meanwhile, a row that both deleted, an increment of one counter by both (no lost update): refused
    {
        mw_tmpdb(path, sizeof path, "rebase3"); make(path, TBL);
        sqlite3 *a, *b; CHECK_RC(open_mw(path, &a, 1), SQLITE_OK); CHECK_RC(open_mw(path, &b, 1), SQLITE_OK);
        int rc1 = race(a, b, "UPDATE t SET a = 77 WHERE id = 7", "DELETE FROM t WHERE id = 7");
        int rc2 = race(a, b, "DELETE FROM t WHERE id = 8", "DELETE FROM t WHERE id = 8");
        int rc3 = race(a, b, "UPDATE t SET a = a + 1 WHERE id = 9", "UPDATE t SET a = a + 1 WHERE id = 9");
        printf("3. update of a deleted row rc=%d, delete of a deleted row rc=%d, two increments rc=%d\n", rc1, rc2, rc3);
        CHECK(is_conflict(rc1)); CHECK(is_conflict(rc2)); CHECK(is_conflict(rc3));
        CHECK(mw_scalar(a, "SELECT count(*) FROM t WHERE id IN (7, 8)") == 0); CHECK(mw_scalar(a, "SELECT a FROM t WHERE id=9") == 10);       // (one increment: the loser runs again)
        CHECK(stats(a).rebases == 0);
        sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);
    }

    // 4. inserts: other keys are both there, the same key is refused, a UNIQUE value that the other took is refused
    {
        mw_tmpdb(path, sizeof path, "rebase4"); make(path, TBL);
        sqlite3 *a, *b; CHECK_RC(open_mw(path, &a, 1), SQLITE_OK); CHECK_RC(open_mw(path, &b, 1), SQLITE_OK);
        int rc1 = race(a, b, "INSERT INTO t(id, a) VALUES(101, 1)", "INSERT INTO t(id, a) VALUES(102, 2)");
        int rc2 = race(a, b, "INSERT INTO t(id, a) VALUES(103, 1)", "INSERT INTO t(id, a) VALUES(103, 2)");
        int rc3 = race(a, b, "INSERT INTO t(id, a, u) VALUES(104, 1, 555)", "INSERT INTO t(id, a, u) VALUES(105, 2, 555)");
        printf("4. inserts: other keys rc=%d, same key rc=%d, same UNIQUE value rc=%d\n", rc1, rc2, rc3);
        CHECK_RC(rc1, SQLITE_OK); CHECK(is_conflict(rc2)); CHECK(is_conflict(rc3));
        CHECK(mw_scalar(a, "SELECT count(*) FROM t WHERE id IN (101, 102)") == 2); CHECK(mw_scalar(a, "SELECT a FROM t WHERE id=103") == 2); CHECK(mw_scalar(a, "SELECT count(*) FROM t WHERE u = 555") == 1);
        CHECK(mw_scalar(a, "SELECT count(*) FROM t WHERE id = 104") == 0);
        CHECK(stats(a).rebases >= 1); CHECK(integrity_ok(a));
        sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);
    }

    // 5. what the comparison of the row must not get wrong: a collation that calls 'abc' and 'ABC' the same, a REAL that the file stores as an integer, NULL
    {
        mw_tmpdb(path, sizeof path, "rebase5"); make(path, TBL);
        sqlite3 *a, *b; CHECK_RC(open_mw(path, &a, 1), SQLITE_OK); CHECK_RC(open_mw(path, &b, 1), SQLITE_OK);
        CHECK_RC(mw_exec(a, "UPDATE t SET c = 'abc', r = 5.0 WHERE id IN (5, 6, 20)"), SQLITE_OK);
        int rc1 = race(a, b, "UPDATE t SET a = 1000 WHERE id = 5", "UPDATE t SET c = 'ABC' WHERE id = 5");                // (equal under NOCASE, not the same row)
        int rc2 = race(a, b, "UPDATE t SET a = 1000 WHERE id = 6", "UPDATE t SET a = 2000 WHERE id = 20");                // (a REAL 5.0 is stored as the integer 5: not a difference)
        int rc3 = race(a, b, "UPDATE t SET a = NULL WHERE id = 10", "UPDATE t SET a = 3000 WHERE id = 25");               // (NULL is a value)
        printf("5. NOCASE rc=%d, REAL stored as an integer rc=%d, NULL rc=%d\n", rc1, rc2, rc3);
        CHECK(is_conflict(rc1)); CHECK_RC(rc2, SQLITE_OK); CHECK_RC(rc3, SQLITE_OK);
        CHECK(mw_scalar(a, "SELECT a FROM t WHERE id=6") == 1000 && mw_scalar(a, "SELECT a FROM t WHERE id=20") == 2000);
        CHECK(mw_scalar(a, "SELECT count(*) FROM t WHERE id=10 AND a IS NULL") == 1);
        CHECK(integrity_ok(a)); sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);
    }

    // 6. values of every size: a value that spills into an overflow chain is replaced whole, another row of its page is changed meanwhile
    {
        mw_tmpdb(path, sizeof path, "rebase6"); make(path, TBL);
        sqlite3 *a, *b; CHECK_RC(open_mw(path, &a, 1), SQLITE_OK); CHECK_RC(open_mw(path, &b, 1), SQLITE_OK);
        CHECK_RC(mw_exec(a, "UPDATE t SET b = hex(randomblob(2050)) WHERE id = 12"), SQLITE_OK);           // (4100 characters: 489 in the leaf, the rest in one overflow page: the row shares its leaf with the others)
        int rc = race(a, b, "UPDATE t SET b = hex(randomblob(2050)) WHERE id = 12", "UPDATE t SET a = 5 WHERE id = 13");           // (the same size: no growth of the file, so the relocation does not save it)
        printf("6. an overflow value replaced, another row of the page changed: rc=%d, rebases %llu\n", rc, (unsigned long long)stats(a).rebases);
        CHECK_RC(rc, SQLITE_OK); CHECK(stats(a).rebases == 1); CHECK(mw_scalar(a, "SELECT length(b) FROM t WHERE id=12") == 4100); CHECK(mw_scalar(a, "SELECT a FROM t WHERE id=13") == 5);
        CHECK(integrity_ok(a)); sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);
    }

    // 7. never rebased (refused as without the rebase, and the database stays right): DDL, a trigger, AUTOINCREMENT
    {
        struct { const char *name, *ddl, *sa, *sb; } cases[] = {
            { "a trigger", "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT); CREATE TRIGGER tg AFTER UPDATE ON t BEGIN SELECT 1; END; " FILL, "UPDATE t SET a = 1000 WHERE id = 3", "UPDATE t SET a = 2000 WHERE id = 30" },
            { "AUTOINCREMENT", "CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, a INTEGER, b TEXT); " FILL, "INSERT INTO t(a) VALUES(1)", "INSERT INTO t(a) VALUES(2)" },
        };
        for (unsigned i = 0; i < sizeof cases / sizeof *cases; i++) {
            mw_tmpdb(path, sizeof path, "rebase7"); make(path, cases[i].ddl);
            sqlite3 *a, *b; CHECK_RC(open_mw(path, &a, 1), SQLITE_OK); CHECK_RC(open_mw(path, &b, 1), SQLITE_OK);
            int rc = race(a, b, cases[i].sa, cases[i].sb); mw_db_stats st = stats(a);
            printf("7. %s: rc=%d, rebases %llu, refused to replay %llu\n", cases[i].name, rc, (unsigned long long)st.rebases, (unsigned long long)st.unrebasable);
            CHECK(is_conflict(rc)); CHECK(st.rebases == 0); CHECK(integrity_ok(a));
            sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);
        }
        mw_tmpdb(path, sizeof path, "rebase7"); make(path, TBL);                                                         // DDL while the other writes: the cookie
        sqlite3 *a, *b; CHECK_RC(open_mw(path, &a, 1), SQLITE_OK); CHECK_RC(open_mw(path, &b, 1), SQLITE_OK);
        int rc = race(a, b, "UPDATE t SET a = 1000 WHERE id = 3", "CREATE INDEX ia ON t(a)");
        printf("7. DDL by the other: rc=%d, rebases %llu\n", rc, (unsigned long long)stats(a).rebases);
        CHECK(is_conflict(rc)); CHECK(stats(a).rebases == 0); CHECK(integrity_ok(a));
        sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);
    }

    // 8. many rounds: a counter per row, each connection increments its own rows and shares the pages (no lost update, every increment counted)
    {
        mw_tmpdb(path, sizeof path, "rebase8"); make(path, TBL);
        sqlite3 *a, *b; CHECK_RC(open_mw(path, &a, 1), SQLITE_OK); CHECK_RC(open_mw(path, &b, 1), SQLITE_OK);
        int ra = 0, rb = 0;
        for (int i = 0; i < 300; i++) {
            char sa[100], sb[100]; snprintf(sa, sizeof sa, "UPDATE t SET a = a + 1 WHERE id = %d", 1 + i % 10); snprintf(sb, sizeof sb, "UPDATE t SET a = a + 1 WHERE id = %d", 21 + i % 10);
            int rc = race(a, b, sa, sb); if (rc == SQLITE_OK) ra++; else { CHECK(is_conflict(rc)); }
            rb++;
        }
        int64_t suma = mw_scalar(a, "SELECT sum(a) FROM t WHERE id <= 10"), sumb = mw_scalar(a, "SELECT sum(a) FROM t WHERE id BETWEEN 21 AND 30");
        printf("8. 300 rounds of two connections, own rows on shared pages: A committed %d, B %d; sums %lld and %lld; rebases %llu\n", ra, rb, (long long)(suma - 55), (long long)(sumb - 255), (unsigned long long)stats(a).rebases);
        CHECK(suma - 55 == ra); CHECK(sumb - 255 == rb); CHECK(ra == 300);                                          // (disjoint rows: every commit of A is saved by the rebase)
        CHECK(integrity_ok(a)); sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);
    }
    // 10. rows written before an ALTER TABLE ADD COLUMN have fewer columns than the table: the missing ones are the defaults, in the comparison and in the replay
    {
        mw_tmpdb(path, sizeof path, "rebase10"); make(path, TBL);
        sqlite3 *x; CHECK_RC(sqlite3_open_v2(path, &x, SQLITE_OPEN_READWRITE, MW_PLAIN_VFS), SQLITE_OK);
        CHECK_RC(mw_exec(x, "ALTER TABLE t ADD COLUMN z INTEGER DEFAULT 7; ALTER TABLE t ADD COLUMN s TEXT DEFAULT 'hi'; ALTER TABLE t ADD COLUMN f REAL DEFAULT 2.5; ALTER TABLE t ADD COLUMN k BLOB"), SQLITE_OK);
        sqlite3_close(x);
        sqlite3 *a, *b; CHECK_RC(open_mw(path, &a, 1), SQLITE_OK); CHECK_RC(open_mw(path, &b, 1), SQLITE_OK);
        int rc = race(a, b, "UPDATE t SET a = 1000 WHERE id = 3", "UPDATE t SET a = 2000 WHERE id = 30");           // both rows are short; the first becomes full
        mw_db_stats st = stats(a);
        printf("10. short rows, update: rc=%d, rebases %llu\n", rc, (unsigned long long)st.rebases);
        CHECK_RC(rc, SQLITE_OK); CHECK(st.rebases == 1); CHECK(mw_scalar(a, "SELECT a FROM t WHERE id=3") == 1000); CHECK(mw_scalar(a, "SELECT a FROM t WHERE id=30") == 2000);
        CHECK(mw_scalar(a, "SELECT z FROM t WHERE id=3") == 7 && mw_scalar(a, "SELECT s = 'hi' AND f = 2.5 AND k IS NULL FROM t WHERE id=3") == 1);
        rc = race(a, b, "DELETE FROM t WHERE id = 5", "UPDATE t SET a = a + 1 WHERE id = 31");                          // a short row deleted
        printf("10. short rows, delete: rc=%d\n", rc);
        CHECK_RC(rc, SQLITE_OK); CHECK(mw_scalar(a, "SELECT count(*) FROM t WHERE id=5") == 0); CHECK(mw_scalar(a, "SELECT a FROM t WHERE id=31") == 32);
        rc = race(a, b, "UPDATE t SET a = 77 WHERE id = 8", "UPDATE t SET z = 8 WHERE id = 8");                          // the other changed a default column of the same short row: a true conflict
        printf("10. short rows, true conflict: rc=%d\n", rc);
        CHECK(is_conflict(rc)); CHECK(mw_scalar(a, "SELECT a FROM t WHERE id=8") == 8); CHECK(mw_scalar(a, "SELECT z FROM t WHERE id=8") == 8);
        rc = race(a, b, "UPDATE t SET a = 66 WHERE id = 9", "UPDATE t SET z = 7, s = 'hi' WHERE id = 10");                // the other writes the defaults explicitly into another row: no conflict
        CHECK_RC(rc, SQLITE_OK); CHECK(mw_scalar(a, "SELECT a FROM t WHERE id=9") == 66);
        rc = race(a, b, "INSERT INTO t(id, a, b, z) VALUES(100, 1, 'n', 3)", "UPDATE t SET a = 3 WHERE id = 20");          // a new row beside short ones
        CHECK_RC(rc, SQLITE_OK); CHECK(mw_scalar(a, "SELECT z FROM t WHERE id=100") == 3);
        CHECK(integrity_ok(a)); sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);
    }
    MW_DONE();
}
