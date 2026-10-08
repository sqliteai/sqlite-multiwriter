// The first process to open a database in the shared mode finishes the shared header after it has opened it; the others must not start before that. They did: one that committed in between
// had its epoch overwritten by the first one (the epoch went back to the one found on disk, the versions that the commit had installed stayed ahead of it) and from then on every commit of every
// process conflicted with a version that nobody could see: a stall for ever (found by mw_serial with 24 copies of it at once, one run in 300).
#include <sys/wait.h>
#include "mw_test.h"
#include "multiwriter.h"

static int open_mp (const char *path, sqlite3 **db) {
    char uri[300]; snprintf(uri, sizeof uri, "file:%s?mw=2&mw_mp=1", path);
    return sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
}

int main (void) {
    char path[256]; mw_tmpdb(path, sizeof path, "openrace");
    sqlite3 *s; CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, MW_PLAIN_VFS), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, v)"), SQLITE_OK); sqlite3_close(s);
    pid_t a = fork();
    if (a == 0) {                                                   // the first process: it takes 600 ms to finish the header
        setenv("MW_TEST_OPEN_DELAY_US", "600000", 1);
        sqlite3 *db; if (open_mp(path, &db) != SQLITE_OK) _exit(2);
        usleep(1500000); _exit(0);
    }
    usleep(150000);                                                 // (the first one is in its open now)
    sqlite3 *b; CHECK_RC(open_mp(path, &b), SQLITE_OK);
    sqlite3_busy_timeout(b, 0);
    int failed = 0;
    for (int i = 1; i <= 40; i++) {
        char sql[100]; snprintf(sql, sizeof sql, "INSERT INTO t VALUES(%d, 'x')", i);
        int ok = 0; for (int attempt = 0; attempt < 5 && !ok; attempt++) { ok = mw_exec(b, sql) == SQLITE_OK; if (!ok) usleep(2000); }
        if (!ok) failed++;
        if (i == 5) usleep(900000);                                 // (the first one is done with its header by now)
    }
    CHECK(failed == 0);
    CHECK(mw_scalar(b, "SELECT count(*) FROM t") == 40);
    sqlite3_close(b);
    int st; waitpid(a, &st, 0);
    mw_rmfiles(path);
    printf("test/mw_openrace.c: %d failure(s)\n", mw_failures);
    return mw_failures ? 1 : 0;
}
