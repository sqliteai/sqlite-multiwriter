// A schema change of a temporary object does not touch the database file: it must not raise the exclusive schema barrier (it waited for every writer of the other connections to end,
// and the writers that came after it were refused until it was over). A change of the main schema still does.
#include "mw_test.h"
#include "multiwriter.h"
#include <time.h>
static double mw_now (void) { struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t); return (double)t.tv_sec + (double)t.tv_nsec * 1e-9; }

static sqlite3 *openc (const char *path, const char *extra) {
    char uri[400]; snprintf(uri, sizeof uri, "file:%s?vfs=multiwriter%s", path, extra); sqlite3 *d = NULL;
    if (sqlite3_open_v2(uri, &d, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL) != SQLITE_OK) return NULL;
    sqlite3_busy_timeout(d, 0); return d;
}
static void run (const char *extra) {
    char path[256]; mw_tmpdb(path, sizeof path, "tempddl");
    sqlite3 *a = openc(path, extra), *b = openc(path, extra); CHECK(a && b);
    CHECK_RC(mw_exec(a, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, v); CREATE TABLE u(id INTEGER PRIMARY KEY, v)"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "BEGIN IMMEDIATE"), SQLITE_OK);                      // B is in the middle of a write transaction
    CHECK_RC(mw_exec(b, "INSERT INTO t VALUES (1, 'b')"), SQLITE_OK);
    const char *temp[] = {
        "CREATE TEMP TABLE x1(a)", "create temporary table x2(a)", "  /* c */ CREATE TEMP TABLE x3 AS SELECT 1", "CREATE TABLE temp.x4(a)", "CREATE TABLE \"temp\".x5(a)",
        "CREATE TEMP VIEW x6 AS SELECT 1", "CREATE UNIQUE INDEX temp.i1 ON x1(a)", "CREATE TEMP TRIGGER g1 AFTER INSERT ON x1 BEGIN SELECT 1; END",
        "CREATE TABLE IF NOT EXISTS temp.x7(a)", "ALTER TABLE temp.x7 ADD COLUMN b",
        "DROP TABLE temp.x4", "DROP TABLE IF EXISTS temp.x5", "DROP VIEW temp.x6", "DROP TRIGGER temp.g1", "DROP INDEX temp.i1", NULL };
    double t0 = mw_now();
    for (int i = 0; temp[i]; i++) { int rc = mw_exec(a, temp[i]); if (rc != SQLITE_OK) printf("[%s] rc=%d\n", temp[i], rc); CHECK_RC(rc, SQLITE_OK); }
    CHECK_RC(mw_exec(a, "INSERT INTO x1 VALUES (1)"), SQLITE_OK);
    double dt = mw_now() - t0;
    printf("%s: 15 statements on the temporary schema while another connection writes: %.0f ms\n", extra, dt * 1000);
    CHECK(dt < 1.0);                                                       // (each of them waited 2 s for B before)
    // a change of the main schema still waits for the writers of the others (it gives up after 2 s: validation protects it)
    t0 = mw_now();
    CHECK_RC(mw_exec(a, "CREATE TABLE w(a)"), SQLITE_OK);
    dt = mw_now() - t0;
    printf("%s: CREATE TABLE while another connection writes: %.0f ms\n", extra, dt * 1000);
    CHECK(dt > 1.0);
    CHECK_RC(mw_exec(b, "ROLLBACK"), SQLITE_OK);
    CHECK(mw_scalar(b, "SELECT count(*) FROM t") == 0);
    sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);
}
int main (void) {
    run("");
    run("&mw_mp=1");
    MW_DONE();
}
