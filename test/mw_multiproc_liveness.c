// Multi-process liveness: a killed process that nobody has reaped (a zombie) must not be treated as alive, and a process that dies right after
// renaming a rewritten log into place (before the shared header names it) must not leave the others appending to the old, unlinked file.
#include <signal.h>
#include <time.h>
#include <sys/wait.h>
#include <sys/mman.h>
#include "mw_test.h"
#include "multiwriter.h"

static int open_mp (const char *path, sqlite3 **db) {
    char uri[400];
    snprintf(uri, sizeof uri, "file:%s?mw=2&mw_mp=1&mw_gc=32&mw_compact_ms=5&mw_log_max_mb=1", path);
    int rc = sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
    if (rc == SQLITE_OK) { sqlite3_extended_result_codes(*db, 1); sqlite3_busy_timeout(*db, 0); }
    return rc;
}
static mw_db_stats stats (sqlite3 *db) { mw_db_stats s; memset(&s, 0, sizeof s); sqlite3_file_control(db, "main", MW_FCNTL_DBSTATS, &s); return s; }
static void setup (const char *path) {
    char p[400]; snprintf(p, sizeof p, "%s-mw", path); unlink(p); snprintf(p, sizeof p, "%s-mwlock", path); unlink(p); snprintf(p, sizeof p, "%s-mwlk", path); unlink(p);
    sqlite3 *s;
    CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER, pad BLOB);"
                        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<64) INSERT INTO t SELECT i, 0, zeroblob(3000) FROM n"), SQLITE_OK);
    sqlite3_close(s);
}

int main (void) {
    char path[256]; mw_tmpdb(path, sizeof path, "mplive");

    // ---- 1. a zombie that pinned a snapshot must not hold compaction back
    setup(path);
    sqlite3 *a; CHECK_RC(open_mp(path, &a), SQLITE_OK);
    int fds[2]; CHECK(pipe(fds) == 0);
    pid_t c = fork();
    if (c == 0) {
        sqlite3 *b; if (open_mp(path, &b) != SQLITE_OK) _exit(1);
        mw_exec(b, "BEGIN"); mw_scalar(b, "SELECT count(*) FROM t");                       // holds a snapshot ...
        char ok = 1; if (write(fds[1], &ok, 1) != 1) _exit(2);
        for (;;) pause();                                                                  // ... until it is killed
    }
    char ok = 0; CHECK(read(fds[0], &ok, 1) == 1 && ok == 1);
    kill(c, SIGKILL);                                                                       // NOT reaped: it stays a zombie for the whole test
    struct timespec ts = { 0, 50 * 1000000L }; nanosleep(&ts, NULL);
    for (int i = 0; i < 6000; i++) { char q[80]; snprintf(q, sizeof q, "UPDATE t SET v=v+1 WHERE id=%d", 1 + i % 64); CHECK_RC(mw_exec(a, q), SQLITE_OK); }
    nanosleep(&(struct timespec){ 0, 300 * 1000000L }, NULL);
    mw_db_stats s = stats(a);
    printf("zombie holding a snapshot: epoch=%llu base=%llu backlog=%llu\n", (unsigned long long)s.epoch, (unsigned long long)s.base_epoch, (unsigned long long)s.compaction_backlog);
    CHECK(s.epoch - s.base_epoch < 3000);                                                   // the compactor moved on past the dead snapshot
    CHECK(mw_scalar(a, "SELECT sum(v) FROM t") == 6000);
    int st; waitpid(c, &st, 0);
    sqlite3_close(a);
    mw_rmdb(path);

    // ---- 2. (the private-store mode: the shared mode has no log to rewrite)
    setenv("MW_MP_PRIVATE", "1", 1);
    // ---- 2. a process dies right after renaming the rewritten log into place
    setup(path);
    CHECK_RC(open_mp(path, &a), SQLITE_OK);
    pid_t d = fork();
    if (d == 0) {
        sqlite3 *b; if (open_mp(path, &b) != SQLITE_OK) _exit(1);
        mw_fault_arm(MW_CRASH_LOG_RENAME, 1);
        for (int i = 0; i < 200000; i++) { char q[80]; snprintf(q, sizeof q, "UPDATE t SET v=v+1 WHERE id=%d", 33 + i % 32); mw_exec(b, q); }
        _exit(3);                                                                           // (the crash point never fired)
    }
    int status = 0; waitpid(d, &status, 0);
    printf("rewriter child status: %s %d\n", WIFEXITED(status) ? "exit" : "signal", WIFEXITED(status) ? WEXITSTATUS(status) : WTERMSIG(status));
    CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 9);                                    // died at the crash point
    int64_t before = mw_scalar(a, "SELECT sum(v) FROM t");
    int good = 0;
    for (int i = 0; i < 500; i++) { char q[80]; snprintf(q, sizeof q, "UPDATE t SET v=v+1 WHERE id=%d", 1 + i % 32); if (mw_exec(a, q) == SQLITE_OK) good++; }
    CHECK(good == 500);
    CHECK(mw_scalar(a, "SELECT sum(v) FROM t") == before + 500);
    sqlite3_close(a);
    // everything committed (by the survivor after the crash) is in the database a fresh process sees
    CHECK_RC(open_mp(path, &a), SQLITE_OK);
    CHECK(mw_scalar(a, "SELECT sum(v) FROM t") == before + 500);
    CHECK(mw_scalar(a, "SELECT count(*) FROM t") == 64);
    sqlite3_close(a);
    mw_rmdb(path);
    MW_DONE();
}
