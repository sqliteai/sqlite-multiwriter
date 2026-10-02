// Shared mode: a process dies inside the publication lock, between appending its record and making the commit visible. Whoever takes the
// lock next finishes the commit (the record is complete) or undoes it; later commits must work, the log must hold one record per epoch,
// every acknowledged commit must survive, and the database must stay consistent.
#include <signal.h>
#include <sys/wait.h>
#include "mw_test.h"
#include "multiwriter.h"

static int open_mp (const char *path, sqlite3 **db) {
    char uri[400];
    snprintf(uri, sizeof uri, "file:%s?mw=2&mw_mp=1", path);
    int rc = sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
    if (rc == SQLITE_OK) { sqlite3_extended_result_codes(*db, 1); sqlite3_busy_timeout(*db, 2000); }
    return rc;
}
static long scalar (sqlite3 *db, const char *sql) {
    sqlite3_stmt *st; long v = -1;
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW) v = (long)sqlite3_column_int64(st, 0);
    sqlite3_finalize(st);
    return v;
}

int main (void) {
    mw_fault_t pts[] = { MW_CRASH_SHARED_APPENDED, MW_CRASH_SHARED_INSTALLED, MW_CRASH_MID_LOG, MW_CRASH_BEFORE_LOG, MW_CRASH_AFTER_LOG };
    const char *names[] = { "record appended", "pages installed", "record half written", "before the record", "record durable" };
    for (int p = 0; p < 5; p++) for (int nth = 1; nth <= 3; nth += 2) {
        char path[256], l1[300], l2[300];
        mw_tmpdb(path, sizeof path, "shw");
        snprintf(l1, sizeof l1, "%s-mwlock", path); snprintf(l2, sizeof l2, "%s-mw", path); unlink(l1); unlink(l2);
        sqlite3 *a; CHECK_RC(open_mp(path, &a), SQLITE_OK);
        CHECK_RC(mw_exec(a, "CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER); INSERT INTO t VALUES(1,0);"), SQLITE_OK);
        int fd[2]; if (pipe(fd)) return 1;
        pid_t pid = fork();
        if (pid == 0) {
            sqlite3 *b; if (open_mp(path, &b) != SQLITE_OK) _exit(2);
            mw_fault_arm(pts[p], nth);
            int acked = 0;
            for (int i = 0; i < 20; i++) {
                if (mw_exec(b, "UPDATE t SET n=n+1 WHERE id=1") == SQLITE_OK) { acked++; (void)!write(fd[1], "x", 1); }
            }
            _exit(0);                              // (the fault point did not fire)
        }
        close(fd[1]);
        int st; waitpid(pid, &st, 0);
        char buf[64]; ssize_t got = 0, r; while ((r = read(fd[0], buf, sizeof buf)) > 0) got += r; close(fd[0]);
        long n0 = scalar(a, "SELECT n FROM t WHERE id=1");
        // the survivor goes on: its writes must not hang or fail, and nothing acknowledged may be lost
        int fails = 0;
        for (int i = 0; i < 50; i++) {
            int rc = SQLITE_BUSY;
            for (int tries = 0; tries < 5 && (rc & 0xff) == SQLITE_BUSY; tries++) rc = mw_exec(a, "UPDATE t SET n=n+1 WHERE id=1");   // (a conflict with the finished commit is an ordinary retry)
            if (rc != SQLITE_OK) { fails++; printf("   write %d failed: rc=%d (%s)\n", i, rc, sqlite3_errmsg(a)); }
        }
        long n1 = scalar(a, "SELECT n FROM t WHERE id=1");
        int ok = fails == 0 && n0 >= got && n0 <= got + 1 && (n1 == n0 + 50 || n1 == n0 + 51) && n1 >= got + 50 && n1 <= got + 51;      // (n0 was read before the lock was taken over: a complete record of the dead one is finished then, +1)
        printf("%-22s (hit %d): child acked %ld, survivor sees %ld, then %ld (50 more), %d failures: %s\n", names[p], nth, (long)got, n0, n1, fails, ok ? "ok" : "WRONG");
        CHECK(ok);
        sqlite3_close(a);
        // a fresh open after everything
        sqlite3 *c; CHECK_RC(open_mp(path, &c), SQLITE_OK);
        CHECK(scalar(c, "SELECT n FROM t WHERE id=1") == n1);
        CHECK(scalar(c, "SELECT count(*) FROM pragma_integrity_check WHERE integrity_check<>'ok'") == 0);
        sqlite3_close(c);
    }
    MW_DONE();
}
