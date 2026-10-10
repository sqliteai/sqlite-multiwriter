// Opening a connection must not drop the locks that the open connections of the process hold on the database file. POSIX record locks belong to the process and to the file, and closing any
// descriptor of the file releases all of them: the engine read the header of the file with fopen/fclose at every open, and another process (a stock SQLite, the sqlite3 CLI) then took the file
// although a lane was inside a read transaction. The lock is looked at from another process with F_GETLK on the shared range of SQLite (PENDING_BYTE + 2 .. + 512).
#include "mw_test.h"
#include "multiwriter.h"
#include <fcntl.h>
#include <sys/wait.h>

static int held (const char *path) {
    pid_t p = fork();
    if (p == 0) {
        int fd = open(path, O_RDWR);
        struct flock fl = { .l_type = F_WRLCK, .l_whence = SEEK_SET, .l_start = 0x40000000 + 2, .l_len = 510 };
        int r = fd >= 0 ? fcntl(fd, F_GETLK, &fl) : -1;
        _exit(r == 0 && fl.l_type != F_UNLCK ? 10 : 11);
    }
    int st = 0; waitpid(p, &st, 0);
    return WEXITSTATUS(st) == 10;
}

int main (void) {
    char path[256], uri[400]; mw_tmpdb(path, sizeof path, "openlocks");
    snprintf(uri, sizeof uri, "file:%s?vfs=multiwriter", path);
    sqlite3 *a = NULL, *b = NULL, *c = NULL;
    CHECK_RC(sqlite3_open_v2(uri, &a, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL), SQLITE_OK);
    CHECK_RC(mw_exec(a, "CREATE TABLE t(x); INSERT INTO t VALUES (1)"), SQLITE_OK);
    CHECK_RC(mw_exec(a, "BEGIN"), SQLITE_OK); CHECK(mw_scalar(a, "SELECT count(*) FROM t") == 1);
    CHECK(held(path));                                                        // a lane in a read transaction holds the shared lock
    CHECK_RC(sqlite3_open_v2(uri, &b, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL), SQLITE_OK);
    CHECK(held(path));                                                        // a second connection of the process opens: the lock is still there
    CHECK_RC(sqlite3_open_v2(uri, &c, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL), SQLITE_OK);
    CHECK_RC(mw_exec(b, "BEGIN"), SQLITE_OK); CHECK(mw_scalar(b, "SELECT count(*) FROM t") == 1);
    sqlite3_close(c);
    CHECK(held(path));                                                        // and one that opens and closes
    CHECK_RC(mw_exec(a, "ROLLBACK"), SQLITE_OK);
    CHECK(held(path));                                                        // b is still inside its transaction
    CHECK_RC(mw_exec(b, "ROLLBACK"), SQLITE_OK);
    sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);
    MW_DONE();
}
