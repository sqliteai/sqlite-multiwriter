// The process that has claimed the next compaction dies while it compacts (kill, crash). The claim is void only after 2 s, and what the dead one announced (the versions that the compaction is
// reading must not be freed) stays until another compaction ends: with a small index of versions the writers had no room for those 2 s and every commit was refused with SQLITE_FULL (found by
// mw_serial under load: 5700 refusals in one run). Another process takes the claim as soon as it sees that the claimant is gone.
#include <sys/wait.h>
#include <signal.h>
#include <time.h>
#include "mw_test.h"
#include "multiwriter.h"

static double now_s (void) { struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t); return (double)t.tv_sec + (double)t.tv_nsec / 1e9; }
static int open_mp (const char *path, int compact_ms, sqlite3 **db) {
    char uri[300]; snprintf(uri, sizeof uri, "file:%s?mw=1&mw_mp=1&mw_gc=16&mw_compact_ms=%d", path, compact_ms);
    int rc = sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
    if (rc == SQLITE_OK) { sqlite3_extended_result_codes(*db, 1); sqlite3_busy_timeout(*db, 0); }
    return rc;
}

int main (void) {
    setenv("MW_IDX_ENTRIES", "1500", 1);                              // (a small index: it is full 100 ms after the compaction stops)
    char path[256]; mw_tmpdb(path, sizeof path, "compdeath");
    sqlite3 *s; CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, MW_PLAIN_VFS), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)"), SQLITE_OK);
    for (int i = 1; i <= 40; i++) { char q[80]; snprintf(q, sizeof q, "INSERT INTO t VALUES(%d, 0)", i); mw_exec(s, q); }
    sqlite3_close(s);
    sqlite3 *a; CHECK_RC(open_mp(path, 1000, &a), SQLITE_OK);        // the parent: its compactor wakes up once a second, or when a commit is refused for want of room
    pid_t c = fork();
    if (c == 0) {                                                    // the claimant: it compacts at once, and dies after the pages are written
        sqlite3 *db; if (open_mp(path, 5, &db) != SQLITE_OK) _exit(2);
        mw_fault_arm(MW_CRASH_COMPACT_PAGES, 1);
        for (int i = 0; ; i++) { char q[80]; snprintf(q, sizeof q, "UPDATE t SET v = v + 1 WHERE id = %d", 1 + i % 40); mw_exec(db, q); usleep(200); }
    }
    double t0 = now_s(), stall = 0, worst = 0, last_ok = t0; int full = 0, ok = 0, died = 0;
    while (now_s() - t0 < 6.0) {
        char q[80]; snprintf(q, sizeof q, "UPDATE t SET v = v + 1 WHERE id = %d", 1 + ok % 40);
        int rc = mw_exec(a, q);
        if (rc == SQLITE_OK) { ok++; last_ok = now_s(); stall = 0; }
        else { if ((rc & 0xff) == SQLITE_FULL) full++; stall = now_s() - last_ok; if (stall > worst) worst = stall; usleep(500); }
        if (!died) { int st; if (waitpid(c, &st, WNOHANG) == c) died = 1; }
    }
    if (!died) { kill(c, SIGKILL); int st; waitpid(c, &st, 0); }
    if (getenv("MW_VERBOSE")) printf("  claimant died: %d; commits %d, refusals for want of room %d, longest time without a commit %.2f s\n", died, ok, full, worst);
    CHECK(died);
    CHECK(ok > 1000);
    CHECK(worst < 1.0);                                              // (2 s and more when the claim of the dead one has to expire)
    sqlite3_close(a); mw_rmfiles(path);
    printf("test/mw_compactdeath.c: %d failure(s)\n", mw_failures);
    return mw_failures ? 1 : 0;
}
