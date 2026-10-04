// Opening a log: a header that does not check out, with records behind it, is an error (the commits are not thrown away); a read that fails while the log is replayed is an error (the
// records after it are not a torn tail: the log is not cut there); a log that only has a header, or nothing, is a new log.
#include <signal.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "mw_test.h"
#include "multiwriter.h"
#include "multiwriter_io.h"

static int open_lane (const char *path, sqlite3 **db) {
    char uri[400]; snprintf(uri, sizeof uri, "file:%s?mw=2", path);
    int rc = sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
    if (rc == SQLITE_OK) { sqlite3_extended_result_codes(*db, 1); sqlite3_busy_timeout(*db, 0); }
    return rc;
}
static off_t fsize (const char *p) { struct stat sb; return stat(p, &sb) == 0 ? sb.st_size : -1; }

int main (void) {
    char path[300]; mw_tmpdb(path, sizeof path, "logopen"); char lp[330]; snprintf(lp, sizeof lp, "%s-mw", path);
    { sqlite3 *s; CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), SQLITE_OK);
      CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)"), SQLITE_OK); sqlite3_close(s); }
    pid_t c = fork();                                                                  // a process that commits and dies (no close: the log keeps its records)
    if (c == 0) { sqlite3 *d; if (open_lane(path, &d) != SQLITE_OK) _exit(1); for (int i = 1; i <= 50; i++) { char q[100]; snprintf(q, sizeof q, "INSERT INTO t VALUES(%d, 'row %d')", i, i); if (mw_exec(d, q) != SQLITE_OK) _exit(2); } _exit(0); }
    int st; waitpid(c, &st, 0); CHECK(WIFEXITED(st) && WEXITSTATUS(st) == 0);
    off_t sz = fsize(lp); CHECK(sz > 4096);

    // 1. a damaged header with the records behind it
    { int fd = open(lp, O_RDWR); CHECK(fd >= 0); uint8_t b; CHECK(pread(fd, &b, 1, 20) == 1); uint8_t bad = (uint8_t)(b ^ 0x5a); CHECK(pwrite(fd, &bad, 1, 20) == 1);
      sqlite3 *v = NULL; int rc = open_lane(path, &v); if (rc == SQLITE_OK) { rc = mw_exec(v, "SELECT count(*) FROM t"); } CHECK(rc != SQLITE_OK); if (v) sqlite3_close(v);
      CHECK(fsize(lp) == sz);                                                          // (the log was not emptied)
      CHECK(pwrite(fd, &b, 1, 20) == 1); close(fd); }

    // 2. a read that fails during the replay
    mw_io_fault_arm(MW_IO_READ, 3, EIO, 0, 0);
    { sqlite3 *v = NULL; int rc = open_lane(path, &v); if (rc == SQLITE_OK) rc = mw_exec(v, "SELECT count(*) FROM t"); CHECK(rc != SQLITE_OK); if (v) sqlite3_close(v); }
    mw_io_fault_disarm();
    CHECK(fsize(lp) == sz);                                                            // (nor cut)

    // 3. all of it is there when the log is read
    { sqlite3 *v; CHECK_RC(open_lane(path, &v), SQLITE_OK); CHECK(mw_scalar(v, "SELECT count(*) FROM t") == 50); sqlite3_close(v); }

    // 4. a log that has only a header, or nothing, is a new one
    { char p2[300]; mw_tmpdb(p2, sizeof p2, "logopen2"); char l2[330]; snprintf(l2, sizeof l2, "%s-mw", p2);
      { sqlite3 *s; CHECK_RC(sqlite3_open_v2(p2, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), SQLITE_OK); CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY)"), SQLITE_OK); sqlite3_close(s); }
      int fd = open(l2, O_RDWR | O_CREAT, 0644); CHECK(fd >= 0); close(fd);              // an empty file
      sqlite3 *v; CHECK_RC(open_lane(p2, &v), SQLITE_OK); CHECK_RC(mw_exec(v, "INSERT INTO t VALUES(1)"), SQLITE_OK); sqlite3_close(v); mw_rmdb(p2); }
    mw_rmdb(path);
    MW_DONE();
}
