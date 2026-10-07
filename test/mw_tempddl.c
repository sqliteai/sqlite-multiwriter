// A schema change that does not touch the main file (CREATE TEMP TABLE / VIEW / TRIGGER, DROP of a temporary object) raises the schema barrier when the statement starts, and the barrier
// was given back only at the end of a snapshot of the main file, which such a statement never opens: every other connection was refused ("database is locked") for ever
// (found by SQLite's temptable test). The barrier is given back when the statement ends.
#include "mw_test.h"
#include "multiwriter.h"

static int open_lane (const char *path, sqlite3 **db) {
    char uri[300]; snprintf(uri, sizeof uri, "file:%s?mw=2", path);
    int rc = sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
    if (rc == SQLITE_OK) sqlite3_extended_result_codes(*db, 1);
    return rc;
}

int main (void) {
    char path[256]; mw_tmpdb(path, sizeof path, "tempddl"); sqlite3 *a, *b;
    open_lane(path, &a); open_lane(path, &b);
    CHECK_RC(mw_exec(a, "CREATE TABLE t1(x); INSERT INTO t1 VALUES(1)"), SQLITE_OK);
    CHECK(mw_scalar(b, "SELECT count(*) FROM t1") == 1);
    const char *ddl[] = { "CREATE TEMP TABLE tt(x, y)", "CREATE TEMP VIEW tv AS SELECT 1", "CREATE TEMP TRIGGER ttr AFTER INSERT ON tt BEGIN SELECT 1; END", "DROP TRIGGER ttr", "DROP VIEW tv",
                          "CREATE TEMP TABLE tt2(x)", "ALTER TABLE tt2 RENAME TO tt3", "DROP TABLE tt3", "CREATE TABLE IF NOT EXISTS t1(x)", "CREATE INDEX temp.ti ON tt(x)", NULL };
    for (int i = 0; ddl[i]; i++) {
        CHECK_RC(mw_exec(b, ddl[i]), SQLITE_OK);
        char sql[100]; snprintf(sql, sizeof sql, "INSERT INTO t1 VALUES(%d)", 100 + i);
        int rc = mw_exec(a, sql);                                   // the other connection can still write after it
        if (rc != SQLITE_OK) printf("  after '%s': INSERT = %d\n", ddl[i], rc);
        CHECK_RC(rc, SQLITE_OK);
    }
    CHECK_RC(mw_exec(a, "CREATE TABLE t2(x)"), SQLITE_OK);          // and run a schema change of its own
    CHECK_RC(mw_exec(b, "BEGIN; CREATE TEMP TABLE tz(x); INSERT INTO tz VALUES(1); COMMIT"), SQLITE_OK);
    CHECK_RC(mw_exec(a, "CREATE TABLE t3(x)"), SQLITE_OK);
    CHECK(mw_scalar(b, "SELECT count(*) FROM t1") == 11);
    sqlite3_close(a); sqlite3_close(b); mw_rmfiles(path);
    printf("test/mw_tempddl.c: %d failure(s)\n", mw_failures);
    return mw_failures ? 1 : 0;
}
