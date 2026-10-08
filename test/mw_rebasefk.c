// The rebase with foreign keys: the replay runs with them enforced when the application's connection does (and without them when it does not), the rows that the actions take are deleted children first, a child that
// appeared meanwhile is dealt with by SQLite as in a serial execution. Two connections of one process; the second commits while the first one is open (as in mw_rebase.c).
#include "mw_test.h"
#include "multiwriter.h"

static int open_mw (const char *path, sqlite3 **db, int fk) {
    char uri[300]; snprintf(uri, sizeof uri, "file:%s?mw=2&mw_rebase=1&mw_rebase_backoff=0%s", path, getenv("MW_TEST_MP") ? "&mw_mp=1" : "");
    int rc = sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
    if (rc == SQLITE_OK) { sqlite3_extended_result_codes(*db, 1); sqlite3_busy_timeout(*db, 0); if (fk) mw_exec(*db, "PRAGMA foreign_keys=ON"); }
    return rc;
}
static mw_db_stats stats (sqlite3 *db) { mw_db_stats s; memset(&s, 0, sizeof s); sqlite3_file_control(db, "main", MW_FCNTL_DBSTATS, &s); return s; }
static int is_conflict (int rc) { return (rc & 0xff) == SQLITE_BUSY; }
static int64_t violations (sqlite3 *db) { return mw_scalar(db, "SELECT count(*) FROM pragma_foreign_key_check"); }
static void make (const char *path, const char *ddl) {
    sqlite3 *s; CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, MW_PLAIN_VFS), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL"), SQLITE_OK);
    CHECK_RC(mw_exec(s, ddl), SQLITE_OK);
    sqlite3_close(s);
}
static int race (sqlite3 *a, sqlite3 *b, const char *sa, const char *sb) {
    CHECK_RC(mw_exec(a, "BEGIN"), SQLITE_OK); CHECK_RC(mw_exec(a, sa), SQLITE_OK);
    CHECK_RC(mw_exec(b, "BEGIN"), SQLITE_OK); CHECK_RC(mw_exec(b, sb), SQLITE_OK); CHECK_RC(mw_exec(b, "COMMIT"), SQLITE_OK);
    int rc = mw_exec(a, "COMMIT"); if (rc != SQLITE_OK) mw_exec(a, "ROLLBACK");
    return rc;
}
#define FILLP "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<20) INSERT INTO p(id, x) SELECT i, 0 FROM n; "
#define FILLC(t) "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<30) INSERT INTO " t "(id, pid, v) SELECT i, 1 + i % 10, 0 FROM n; "
static const char *DDL = "CREATE TABLE p(id INTEGER PRIMARY KEY, x INTEGER); CREATE TABLE c(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id), v INTEGER); " FILLP FILLC("c");
static const char *DDL_CASCADE = "CREATE TABLE p(id INTEGER PRIMARY KEY, x INTEGER); CREATE TABLE cc(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) ON DELETE CASCADE, v INTEGER); " FILLP FILLC("cc");
static const char *DDL_NULL = "CREATE TABLE p(id INTEGER PRIMARY KEY, x INTEGER); CREATE TABLE cn(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) ON DELETE SET NULL, v INTEGER); " FILLP FILLC("cn");

int main (void) {
    char path[256]; sqlite3 *a, *b; int rc;

    // 1. blind writes to a table with a foreign key, on one page: replayed
    mw_tmpdb(path, sizeof path, "fk1"); make(path, DDL);
    CHECK_RC(open_mw(path, &a, 1), SQLITE_OK); CHECK_RC(open_mw(path, &b, 1), SQLITE_OK);
    rc = race(a, b, "UPDATE c SET v = v + 1 WHERE id = 3", "UPDATE c SET v = v + 1 WHERE id = 20");
    printf("1. update of a child row: rc=%d, rebases %llu\n", rc, (unsigned long long)stats(a).rebases);
    CHECK_RC(rc, SQLITE_OK); CHECK(stats(a).rebases == 1);
    rc = race(a, b, "INSERT INTO c VALUES(100, 5, 1)", "UPDATE c SET v = 9 WHERE id = 20");                    // a new child: its parent is looked up (a read that the replay repeats)
    printf("1. insert of a child row: rc=%d, rebases %llu\n", rc, (unsigned long long)stats(a).rebases);
    CHECK_RC(rc, SQLITE_OK); CHECK(violations(a) == 0);
    rc = race(a, b, "DELETE FROM c WHERE id = 4", "UPDATE c SET v = 9 WHERE id = 21");
    CHECK_RC(rc, SQLITE_OK); CHECK(violations(a) == 0);
    rc = race(a, b, "UPDATE c SET pid = 15 WHERE id = 6", "UPDATE c SET v = 9 WHERE id = 22");                // the foreign key column changes to another parent
    CHECK_RC(rc, SQLITE_OK); CHECK(mw_scalar(a, "SELECT pid FROM c WHERE id = 6") == 15); CHECK(violations(a) == 0);
    sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);

    // 2. the parent is deleted by the other while this one inserts a child of it: the child needs the parent, so the commit is refused (by the read of the parent's page, or by the replay), never an orphan
    mw_tmpdb(path, sizeof path, "fk2"); make(path, DDL);
    CHECK_RC(open_mw(path, &a, 1), SQLITE_OK); CHECK_RC(open_mw(path, &b, 1), SQLITE_OK);
    rc = race(a, b, "INSERT INTO c VALUES(101, 15, 1)", "DELETE FROM p WHERE id = 15; UPDATE c SET v = 9 WHERE id = 20");
    printf("2. child of a parent that is deleted: rc=%d\n", rc);
    CHECK(is_conflict(rc)); CHECK(violations(a) == 0); CHECK(mw_scalar(a, "SELECT count(*) FROM c WHERE id = 101") == 0);
    sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);

    // 2b. the same, but this one also writes the page of the parents, so that the read of the parent is not validated by its page: the replay itself must find that the parent is gone
    mw_tmpdb(path, sizeof path, "fk2b"); make(path, DDL);
    CHECK_RC(open_mw(path, &a, 1), SQLITE_OK); CHECK_RC(open_mw(path, &b, 1), SQLITE_OK);
    rc = race(a, b, "UPDATE p SET x = 1 WHERE id = 1; INSERT INTO c VALUES(101, 15, 1)", "DELETE FROM p WHERE id = 15; UPDATE c SET v = 9 WHERE id = 20");
    printf("2b. child of a parent that is deleted, the replay decides: rc=%d, refused to replay %llu\n", rc, (unsigned long long)stats(a).unrebasable);
    CHECK(is_conflict(rc)); CHECK(violations(a) == 0); CHECK(mw_scalar(a, "SELECT count(*) FROM c WHERE id = 101") == 0);
    sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);

    // 3. ON DELETE CASCADE: this one deletes a parent and its children; the other inserted another child meanwhile: in a serial execution that child goes too
    mw_tmpdb(path, sizeof path, "fk3"); make(path, DDL_CASCADE);
    CHECK_RC(open_mw(path, &a, 1), SQLITE_OK); CHECK_RC(open_mw(path, &b, 1), SQLITE_OK);
    rc = race(a, b, "DELETE FROM p WHERE id = 2", "INSERT INTO cc VALUES(200, 2, 0)");
    printf("3. cascade, a child appeared: rc=%d, rebases %llu\n", rc, (unsigned long long)stats(a).rebases);
    if (rc == SQLITE_OK) { CHECK(stats(a).rebases == 1); CHECK(mw_scalar(a, "SELECT count(*) FROM cc WHERE pid = 2") == 0); CHECK(mw_scalar(a, "SELECT count(*) FROM p WHERE id = 2") == 0); }
    else CHECK(mw_scalar(a, "SELECT count(*) FROM p WHERE id = 2") == 1);
    CHECK(violations(a) == 0);
    rc = race(a, b, "DELETE FROM p WHERE id = 3", "UPDATE cc SET v = 5 WHERE id = 25");                     // the other changed a child of its own, not one of this parent's
    CHECK_RC(rc, SQLITE_OK); CHECK(mw_scalar(a, "SELECT count(*) FROM cc WHERE pid = 3") == 0); CHECK(violations(a) == 0);
    rc = race(a, b, "DELETE FROM p WHERE id = 4", "UPDATE cc SET v = 5 WHERE id = 13");                     // the other changed one of the children that this one deletes: a true conflict
    printf("3. cascade, a child changed: rc=%d\n", rc);
    CHECK(is_conflict(rc)); CHECK(mw_scalar(a, "SELECT count(*) FROM p WHERE id = 4") == 1); CHECK(violations(a) == 0);
    sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);

    // 4. ON DELETE SET NULL
    mw_tmpdb(path, sizeof path, "fk4"); make(path, DDL_NULL);
    CHECK_RC(open_mw(path, &a, 1), SQLITE_OK); CHECK_RC(open_mw(path, &b, 1), SQLITE_OK);
    rc = race(a, b, "DELETE FROM p WHERE id = 2", "INSERT INTO cn VALUES(200, 2, 0)");
    printf("4. set null, a child appeared: rc=%d\n", rc);
    if (rc == SQLITE_OK) CHECK(mw_scalar(a, "SELECT count(*) FROM cn WHERE pid = 2") == 0);
    CHECK(violations(a) == 0);
    sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);

    // 5. a parent with children cannot be deleted (NO ACTION): refused, as the statement would have been
    mw_tmpdb(path, sizeof path, "fk5"); make(path, DDL);
    CHECK_RC(open_mw(path, &a, 1), SQLITE_OK); CHECK_RC(open_mw(path, &b, 1), SQLITE_OK);
    rc = race(a, b, "DELETE FROM p WHERE id = 15", "INSERT INTO c VALUES(300, 15, 0)");                      // the other gave it a child
    printf("5. parent that got a child: rc=%d\n", rc);
    CHECK(is_conflict(rc)); CHECK(mw_scalar(a, "SELECT count(*) FROM p WHERE id = 15") == 1); CHECK(violations(a) == 0);
    sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);

    // 6. the application does not enforce foreign keys (the default of SQLite): the replay does not either, an orphan that it wrote is its business
    mw_tmpdb(path, sizeof path, "fk6"); make(path, DDL);
    CHECK_RC(open_mw(path, &a, 0), SQLITE_OK); CHECK_RC(open_mw(path, &b, 0), SQLITE_OK);
    rc = race(a, b, "INSERT INTO c VALUES(102, 999, 1)", "UPDATE c SET v = 9 WHERE id = 20");
    printf("6. foreign keys off, an orphan: rc=%d, rebases %llu\n", rc, (unsigned long long)stats(a).rebases);
    CHECK_RC(rc, SQLITE_OK); CHECK(stats(a).rebases == 1); CHECK(mw_scalar(a, "SELECT count(*) FROM c WHERE id = 102") == 1);
    sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);

    // 7. a key that a foreign key refers to is changed: not replayed (the ON UPDATE action), refused
    mw_tmpdb(path, sizeof path, "fk7"); make(path, "CREATE TABLE p(id INTEGER PRIMARY KEY, k INTEGER UNIQUE, x INTEGER); CREATE TABLE c(id INTEGER PRIMARY KEY, pk INTEGER REFERENCES p(k) ON UPDATE CASCADE, v INTEGER); "
        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<20) INSERT INTO p SELECT i, i, 0 FROM n; INSERT INTO c VALUES(1, 3, 0), (2, 4, 0);");
    CHECK_RC(open_mw(path, &a, 1), SQLITE_OK); CHECK_RC(open_mw(path, &b, 1), SQLITE_OK);
    rc = race(a, b, "UPDATE p SET k = 33 WHERE id = 3", "UPDATE p SET x = 9 WHERE id = 15");
    printf("7. referenced key changed: rc=%d\n", rc);
    CHECK(is_conflict(rc)); CHECK(violations(a) == 0);
    rc = race(a, b, "UPDATE p SET x = 5 WHERE id = 3", "UPDATE p SET x = 9 WHERE id = 15");               // a column that nothing refers to
    CHECK_RC(rc, SQLITE_OK);
    sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);

    MW_DONE();
}
