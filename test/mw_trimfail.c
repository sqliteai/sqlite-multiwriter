// Compaction in the multi-process mode makes the next segment's header carry the new base epoch, and only then unlinks the older segment. When that header could not be written (or synced) the
// older segment was unlinked all the same: the header kept the old base, the epochs that follow it were no longer contiguous with it, and recovery cut the log at that point: the commits
// that were acknowledged after the compaction were gone after a crash. The compaction is made to fail at every one of its writes in turn (a child per write, crashing at the end); in
// every case, what was acknowledged has to be there after the reopen.
#include <signal.h>
#include <errno.h>
#include <sys/wait.h>
#include "mw_test.h"
#include "multiwriter.h"

static int open_mp (const char *path, sqlite3 **db) {
    char uri[400]; snprintf(uri, sizeof uri, "file:%s?vfs=multiwriter&mw_mp=1&mw_gc=0", path);
    int rc = sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
    if (rc == SQLITE_OK) sqlite3_busy_timeout(*db, 2000);
    return rc;
}
typedef struct { int acked; long calls; int rc; } report;

// One run: the compaction fails at its nth write (0: no failure, count the writes). Returns what the child acknowledged and what the database holds after it crashed.
static void run (long nth, report *out, long long *after) {
    char path[300]; mw_tmpdb(path, sizeof path, "trimfail");
    sqlite3 *s; CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, MW_PLAIN_VFS), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER, pad BLOB); INSERT INTO t VALUES (1, 0, NULL)"), SQLITE_OK);
    sqlite3_close(s);
    int pp[2]; CHECK(pipe(pp) == 0);
    pid_t pid = fork();
    if (pid == 0) {
        alarm(60);
        sqlite3 *a; if (open_mp(path, &a) != SQLITE_OK) _exit(2);
        int acked = 0;
        for (int i = 0; i < 700; i++) if (mw_exec(a, "UPDATE t SET n = n + 1, pad = randomblob(2000) WHERE id = 1") == SQLITE_OK) acked++;
        mw_io_fault_arm(MW_IO_WRITE, nth ? nth : 1000000, EIO, 0, 0);
        mw_compact_result r; int rc = sqlite3_file_control(a, "main", MW_FCNTL_COMPACT, &r);
        long calls = mw_io_fault_calls(); mw_io_fault_disarm();
        for (int i = 0; i < 40; i++) if (mw_exec(a, "UPDATE t SET n = n + 1, pad = randomblob(2000) WHERE id = 1") == SQLITE_OK) acked++;
        report m = { acked, calls, rc };
        (void)!write(pp[1], &m, sizeof m);
        _exit(9);                                           // a crash: no clean close
    }
    report m = { 0, 0, 0 };
    CHECK(read(pp[0], &m, sizeof m) == (ssize_t)sizeof m);
    waitpid(pid, NULL, 0);
    close(pp[0]); close(pp[1]);
    sqlite3 *b; int rc = open_mp(path, &b);
    CHECK_RC(rc, SQLITE_OK);
    *after = rc == SQLITE_OK ? (long long)mw_scalar(b, "SELECT n FROM t") : -1;
    if (rc == SQLITE_OK) sqlite3_close(b);
    mw_rmdb(path);
    *out = m;
}

int main (void) {
    setenv("MW_SEG_MB", "1", 1);                            // segments of 1 MB: the 700 updates make several, and the compaction has older ones to unlink
    report base; long long after;
    run(0, &base, &after);
    printf("without failure: acked %d, writes in the compaction %ld, after the crash %lld\n", base.acked, base.calls, after);
    CHECK(after == base.acked);
    CHECK(base.calls >= 2);
    for (long nth = 1; nth <= base.calls; nth++) {
        report m;
        run(nth, &m, &after);
        printf("write %ld fails: compaction rc=%d, acked %d, after the crash %lld%s\n", nth, m.rc, m.acked, after, after == m.acked ? "" : "   <-- acknowledged commits lost");
        CHECK(after == m.acked);
    }
    MW_DONE();
}
