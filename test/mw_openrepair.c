// The fate of a commit that a dead process had in flight is decided when the next process OPENS the database, not when some publisher finds the lock of the dead one. A process dies at the
// point where its record is complete in the shared log and nothing is installed (or installed and not visible); a process that is started afterwards and looks at once (a restarted
// process asking what became of its commit) must find the commit made. (SQLite's fts and `mw_serial` found it: a restarted process looked at the database before any publisher had
// repaired it, saw the commit absent, went on, and the commit appeared afterwards.)
#include <sys/wait.h>
#include <signal.h>
#include "mw_test.h"
#include "multiwriter.h"

static int open_mp (const char *path, sqlite3 **db) {
    char uri[300]; snprintf(uri, sizeof uri, "file:%s?mw=1&mw_mp=1", path);
    return sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
}

static void try_point (const char *path, mw_fault_t pt, const char *tag) {
    mw_rmfiles(path);
    sqlite3 *a; CHECK_RC(open_mp(path, &a), SQLITE_OK);                    // (the database stays open here: the child does not start it)
    CHECK_RC(mw_exec(a, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, v)"), SQLITE_OK);
    CHECK_RC(mw_exec(a, "INSERT INTO t VALUES(1, 'before')"), SQLITE_OK);
    pid_t p = fork();
    if (p == 0) {                                                           // the process that dies in the middle of its commit
        sqlite3 *db; if (open_mp(path, &db) != SQLITE_OK) _exit(2);
        mw_fault_arm(pt, 1);
        mw_exec(db, "INSERT INTO t VALUES(2, 'in flight')");
        _exit(3);                                                           // (not reached: it dies at the point)
    }
    int st; waitpid(p, &st, 0);
    CHECK(WIFSIGNALED(st) || (WIFEXITED(st) && WEXITSTATUS(st) != 3));
    pid_t q = fork();
    if (q == 0) {                                                           // the process started afterwards: it opens and looks at once, before anybody publishes
        sqlite3 *db; if (open_mp(path, &db) != SQLITE_OK) _exit(2);
        sqlite3_stmt *s; sqlite3_prepare_v2(db, "SELECT count(*) FROM t WHERE id = 2", -1, &s, NULL);
        int n = sqlite3_step(s) == SQLITE_ROW ? sqlite3_column_int(s, 0) : -1;
        _exit(n == 1 ? 10 : n == 0 ? 11 : 12);
    }
    waitpid(q, &st, 0);
    int code = WIFEXITED(st) ? WEXITSTATUS(st) : -1;
    // for the points where the record is complete the commit is rolled forward at once; for the points before it, it is undone: either way it is decided, and the same for every process afterwards
    sqlite3_stmt *s; sqlite3_prepare_v2(a, "SELECT count(*) FROM t WHERE id = 2", -1, &s, NULL);
    int na = sqlite3_step(s) == SQLITE_ROW ? sqlite3_column_int(s, 0) : -1; sqlite3_finalize(s);
    if (getenv("MW_VERBOSE")) printf("  %s: the process that looked at once saw %d (10 = made, 11 = not made); the first process sees %d\n", tag, code, na);
    CHECK((code == 10 && na == 1) || (code == 11 && na == 0));
    CHECK(code == 10);                                                      // (these points are after the record was complete)
    sqlite3_close(a); mw_rmfiles(path);
}

int main (void) {
    char path[256]; mw_tmpdb(path, sizeof path, "openrepair");
    try_point(path, MW_CRASH_SHARED_APPENDED, "record complete, nothing installed");
    try_point(path, MW_CRASH_SHARED_INSTALLED, "installed, not visible");
    printf("test/mw_openrepair.c: %d failure(s)\n", mw_failures);
    return mw_failures ? 1 : 0;
}
