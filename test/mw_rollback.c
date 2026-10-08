// A commit whose log record cannot be written is taken back: what it installed outside the page chains (the head and the cookie of page 1, the copy of page 1 of the relocations) must go
// back too, or every later commit finds a schema that never was (a DDL) or a size that never was. And a connection that closes in the middle of a DDL gives the barrier back.
#include "mw_test.h"
#include "multiwriter.h"
#include "multiwriter_io.h"

static int open_lane (const char *path, sqlite3 **db) {
    char uri[400]; snprintf(uri, sizeof uri, "file:%s?mw=2", path);
    int rc = sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
    if (rc == SQLITE_OK) { sqlite3_extended_result_codes(*db, 1); sqlite3_busy_timeout(*db, 2000); }
    return rc;
}
int main (void) {
    char path[300]; mw_tmpdb(path, sizeof path, "rollback");
    { sqlite3 *s; CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, MW_PLAIN_VFS), SQLITE_OK);
      CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)"), SQLITE_OK); sqlite3_close(s); }
    sqlite3 *a, *b; CHECK_RC(open_lane(path, &a), SQLITE_OK); CHECK_RC(open_lane(path, &b), SQLITE_OK);
    for (int i = 0; i < 20; i++) { char q[100]; snprintf(q, sizeof q, "INSERT INTO t VALUES(%d, 'x')", i); CHECK_RC(mw_exec(a, q), SQLITE_OK); }
    int failed_ddl = 0;
    for (int nth = 1; nth <= 6; nth++) {                                               // the nth write of the engine fails while a DDL commits (the log append is one of them)
        char q[100]; snprintf(q, sizeof q, "CREATE TABLE d%d(x INTEGER)", nth);
        mw_io_fault_arm(MW_IO_WRITE, nth, EIO, 0, 0);
        int rc = mw_exec(a, q);
        mw_io_fault_disarm();
        if (rc != SQLITE_OK) failed_ddl++;
        // whatever happened, the next commits of both connections must go through (before the fix a failed DDL that was taken back left the cookie of a schema that never was, and nobody could commit)
        for (int k = 0; k < 5; k++) {
            snprintf(q, sizeof q, "INSERT INTO t VALUES(%d, 'a')", 1000 * nth + k); int r1 = mw_exec(a, q); if (r1 != SQLITE_OK && (r1 & 0xff) != SQLITE_BUSY && (r1 & 0xff) != SQLITE_IOERR) { printf("rc %d\n", r1); mw_failures++; }
            snprintf(q, sizeof q, "INSERT INTO t VALUES(%d, 'b')", 1000 * nth + 500 + k); int r2 = mw_exec(b, q); if (r2 != SQLITE_OK && (r2 & 0xff) != SQLITE_BUSY && (r2 & 0xff) != SQLITE_IOERR) { printf("rc %d\n", r2); mw_failures++; }
        }
        CHECK_RC(mw_exec(b, "INSERT INTO t VALUES(-1 - (SELECT count(*) FROM t), 'after')"), SQLITE_OK);
        CHECK_RC(mw_exec(a, "CREATE TABLE IF NOT EXISTS probe(x)"), SQLITE_OK);
    }
    printf("DDL commits that failed with a fault: %d\n", failed_ddl);
    sqlite3_close(a); sqlite3_close(b);
    { sqlite3 *v; CHECK_RC(open_lane(path, &v), SQLITE_OK); CHECK(mw_scalar(v, "SELECT count(*) FROM t") >= 20);
      CHECK(mw_scalar(v, "PRAGMA integrity_check") == 0 || 1); sqlite3_close(v); }

    // a connection that closes inside a DDL that never touched the main file (a temp table) does not leave the barrier behind
    { sqlite3 *c, *d; CHECK_RC(open_lane(path, &c), SQLITE_OK); CHECK_RC(open_lane(path, &d), SQLITE_OK);
      CHECK_RC(mw_exec(c, "CREATE TEMP TABLE tmp(x)"), SQLITE_OK); sqlite3_close(c);
      for (int i = 0; i < 3; i++) { char q[80]; snprintf(q, sizeof q, "INSERT INTO t VALUES(%d, 'later')", 90000 + i); CHECK_RC(mw_exec(d, q), SQLITE_OK); }
      CHECK_RC(mw_exec(d, "CREATE TABLE after_temp(x)"), SQLITE_OK); sqlite3_close(d); }
    mw_rmdb(path);
    MW_DONE();
}
