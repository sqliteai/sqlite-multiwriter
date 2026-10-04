// Shared mode: an fsync of the log that fails in one process must fail the commits of the others that are not durable yet. A later fsync that succeeds (the kernel forgets the error of a write-back that
// failed) must not acknowledge data that may never have reached the disk.
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#include "mw_test.h"
#include "multiwriter.h"
#include "multiwriter_io.h"

static int open_lane (const char *path, sqlite3 **db) {
    char uri[400]; snprintf(uri, sizeof uri, "file:%s?mw=2&mw_mp=1", path);
    int rc = sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
    if (rc == SQLITE_OK) { sqlite3_extended_result_codes(*db, 1); sqlite3_busy_timeout(*db, 3000); }
    return rc;
}
int main (void) {
    char path[300]; mw_tmpdb(path, sizeof path, "syfail");
    { sqlite3 *s; CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), SQLITE_OK);
      CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)"), SQLITE_OK); sqlite3_close(s); }
    sqlite3 *p; CHECK_RC(open_lane(path, &p), SQLITE_OK);
    CHECK_RC(mw_exec(p, "INSERT INTO t VALUES(1, 'a')"), SQLITE_OK);
    int go[2], res[2]; CHECK(pipe(go) == 0 && pipe(res) == 0);
    pid_t c = fork();
    if (c == 0) {
        sqlite3 *d; if (open_lane(path, &d) != SQLITE_OK) _exit(1);
        if (mw_exec(d, "INSERT INTO t VALUES(10, 'child before')") != SQLITE_OK) _exit(2);
        char b; if (read(go[0], &b, 1) != 1) _exit(3);                                 // (the parent's fsync fails now)
        int rc = mw_exec(d, "INSERT INTO t VALUES(11, 'child after')");
        _exit(rc == SQLITE_OK ? 10 : 11);
    }
    usleep(300000);
    mw_io_fault_arm(MW_IO_SYNC, 1, EIO, 0, 0);                                         // (this process only: the child was forked before)
    int rc = mw_exec(p, "INSERT INTO t VALUES(2, 'lost?')"); CHECK(rc != SQLITE_OK);
    mw_io_fault_disarm();
    CHECK(write(go[1], "x", 1) == 1);
    int st; waitpid(c, &st, 0);
    CHECK(WIFEXITED(st)); CHECK(WEXITSTATUS(st) == 11);                                // (the commit of the child that came after is refused: its durability cannot be promised)
    sqlite3_close(p);
    mw_rmdb(path);
    MW_DONE();
}
