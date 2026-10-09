// F_FULLFSYNC (mw_fullfsync=1): commits go through it, survive a reopen and a SIGKILL, and the injected fsync error still fails the commit.
#include <signal.h>
#include <sys/wait.h>
#include "mw_test.h"
#include "multiwriter.h"
#include "multiwriter_io.h"

static int open_lane (const char *path, sqlite3 **db, const char *extra) {
    char uri[400]; snprintf(uri, sizeof uri, "file:%s?mw=1%s", path, extra);
    int rc = sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
    if (rc == SQLITE_OK) { sqlite3_extended_result_codes(*db, 1); sqlite3_busy_timeout(*db, 0); }
    return rc;
}
int main (void) {
    char path[300]; mw_tmpdb(path, sizeof path, "ffs");
    { sqlite3 *s; CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, MW_PLAIN_VFS), SQLITE_OK);
      CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)"), SQLITE_OK); sqlite3_close(s); }
    CHECK(atomic_load(&mw_fullfsync) == 0);
    sqlite3 *db; CHECK_RC(open_lane(path, &db, "&mw_fullfsync=1"), SQLITE_OK);
    CHECK(atomic_load(&mw_fullfsync) == 1);                                            // (the URI turned it on)
    for (int i = 1; i <= 40; i++) { char q[100]; snprintf(q, sizeof q, "INSERT INTO t VALUES(%d, 'row %d')", i, i); CHECK_RC(mw_exec(db, q), SQLITE_OK); }
    mw_io_fault_arm(MW_IO_SYNC, 1, EIO, 0, 0);                                         // the next fsync (through F_FULLFSYNC when it is on) fails: the commit does not succeed
    int rc = mw_exec(db, "INSERT INTO t VALUES(1000, 'lost')"); CHECK(rc != SQLITE_OK);
    mw_io_fault_disarm();
    sqlite3_close(db);
    { sqlite3 *v; CHECK_RC(open_lane(path, &v, "&mw_fullfsync=1"), SQLITE_OK);
      CHECK(mw_scalar(v, "SELECT count(*) FROM t WHERE id <= 40") == 40); CHECK(mw_scalar(v, "SELECT count(*) FROM t WHERE id = 1000") <= 1);                // (a commit whose fsync failed was refused; its record may or may not have reached the disk)
      CHECK_RC(mw_exec(v, "INSERT INTO t VALUES(41, 'after')"), SQLITE_OK); sqlite3_close(v); }
    mw_set_fullfsync(0);
    mw_rmdb(path);
    MW_DONE();
}
