// A segment of the log that cannot be mapped (descriptors, memory) is not a segment that is gone. The readers took the first for the second: the page came from the real file, older than the
// version in the index, and the read succeeded. And the process that repaired what a dead publisher left, finding no record at the end of the log because it could not look at it, took the
// segment for empty and put the cursor back to its start: the next commit overwrote the records that were there. Now the read fails, and the repair stops the commits (the database is
// left as it is until the next open replays the log).
#include <sys/wait.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <fcntl.h>
#include <signal.h>
#include "mw_test.h"
#include "multiwriter.h"
#include "multiwriter_internal.h"

// No descriptor can be opened from here on: the soft limit is the number of the next one.
static void no_more_descriptors (void) {
    int fd = dup(0); if (fd < 0) return; close(fd);
    struct rlimit rl; if (getrlimit(RLIMIT_NOFILE, &rl) != 0) return;
    rl.rlim_cur = (rlim_t)fd; (void)setrlimit(RLIMIT_NOFILE, &rl);
}

static void uri_of (const char *path, char *uri, size_t n) { snprintf(uri, n, "file:%s?vfs=multiwriter&mw_mp=1&mw_gc=1&mw_compact_ms=100000", path); }

int main (void) {
    char path[256], uri[400]; mw_tmpdb(path, sizeof path, "mapfail"); uri_of(path, uri, sizeof uri);
    int p[2], q2[2]; CHECK(pipe(p) == 0 && pipe(q2) == 0);
    pid_t holder = fork();                                              // keeps the database open (the segments and the header exist)
    if (holder == 0) {
        sqlite3 *a; int orc = sqlite3_open_v2(uri, &a, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
        if (orc != SQLITE_OK) { fprintf(stderr, "holder: open rc=%d\n", orc); _exit(2); }
        mw_exec(a, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, v); CREATE TABLE u(id INTEGER PRIMARY KEY, v)");
        for (int i = 1; i <= 10; i++) { char q[100]; snprintf(q, sizeof q, "INSERT INTO t VALUES (%d, 't')", i); mw_exec(a, q); snprintf(q, sizeof q, "INSERT INTO u VALUES (%d, 'u')", i); mw_exec(a, q); }
        mw_compact_result cr; memset(&cr, 0, sizeof cr);
        sqlite3_file_control(a, "main", MW_FCNTL_COMPACT, &cr);         // what is in the file leaves the index
        for (int i = 11; i <= 14; i++) { char q[100]; snprintf(q, sizeof q, "INSERT INTO u VALUES (%d, 'u')", i); mw_exec(a, q); }
        char c = 'r'; (void)!write(p[1], &c, 1);
        (void)!read(q2[0], &c, 1);                                      // (the test is over)
        _exit(0);
    }
    char c; CHECK(read(p[0], &c, 1) == 1);

    // 1. a reader in another process, whose descriptors are all taken: the read of a page that has a version in the log is an error, not the page of the file
    pid_t reader = fork();
    if (reader == 0) {
        sqlite3 *d; if (sqlite3_open_v2(uri, &d, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL) != SQLITE_OK) _exit(2);
        mw_scalar(d, "SELECT count(*) FROM t");                          // (it has its descriptor of the database)
        struct rlimit saved; getrlimit(RLIMIT_NOFILE, &saved);
        no_more_descriptors();
        int rc = mw_exec(d, "SELECT count(*), sum(length(v)) FROM u");
        long long got = (long long)mw_scalar(d, "SELECT count(*) FROM u");
        setrlimit(RLIMIT_NOFILE, &saved);
        long long after = (long long)mw_scalar(d, "SELECT count(*) FROM u");
        // the rows of u are 14: with no descriptors the read is an error or (the maps that it has already) 14, never fewer
        _exit((rc != SQLITE_OK || got < 0 || got == 14) && after == 14 ? 0 : 5);
    }
    int st = 0; waitpid(reader, &st, 0);
    CHECK(WIFEXITED(st) && WEXITSTATUS(st) == 0);

    // 2. the process that takes the lock from a publisher that died before its append cannot map the current segment: it stops the commits, it does not put the cursor back
    char *mp = mw_sidecar_path(path, "mwlock"); int fd = open(mp, O_RDWR);
    mw_shm *sh = fd >= 0 ? mmap(NULL, sizeof(mw_shm), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0) : MAP_FAILED;
    CHECK(sh != MAP_FAILED);
    if (sh == MAP_FAILED) return 1;
    pid_t dead = fork();
    if (dead == 0) {
        sqlite3 *d; if (sqlite3_open_v2(uri, &d, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL) != SQLITE_OK) _exit(2);
        mw_fault_arm(MW_CRASH_BEFORE_LOG, 1); mw_exec(d, "INSERT INTO u VALUES (100, 'dead')"); _exit(3);
    }
    waitpid(dead, &st, 0);
    CHECK(WIFEXITED(st) && WEXITSTATUS(st) == 9);
    uint64_t end_before = atomic_load(&sh->sl_end);
    int pw[2]; CHECK(pipe(pw) == 0);
    pid_t w = fork();
    if (w == 0) {
        sqlite3 *d; if (sqlite3_open_v2(uri, &d, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL) != SQLITE_OK) _exit(2);
        sqlite3_busy_timeout(d, 0);
        mw_scalar(d, "SELECT count(*) FROM t");
        no_more_descriptors(); int n = 0;
        int rc = mw_exec(d, "INSERT INTO t VALUES (1000, 'W')");
        (void)!write(pw[1], &rc, sizeof rc);
        _exit(0);
    }
    close(pw[1]);                                                       // (so that a W that dies before it reports is an end of file, not a wait)
    waitpid(w, &st, 0);
    int wrc = -1; CHECK(read(pw[0], &wrc, sizeof wrc) == (ssize_t)sizeof wrc);
    uint64_t end_after = atomic_load(&sh->sl_end);
    printf("the cursor of the log: %llu before, %llu after the process that could not look at the segment; its insert rc=%d, broken=%u\n",
           (unsigned long long)end_before, (unsigned long long)end_after, wrc, (unsigned)atomic_load(&sh->broken));
    CHECK(end_after >= end_before);                                     // (it was put back to the start of the segment: every record in it was going to be overwritten)

    char q = 'q'; (void)!write(q2[1], &q, 1);
    waitpid(holder, NULL, 0);
    // the database after everybody is gone: the log is replayed, and what was acknowledged is there
    sqlite3 *f = NULL; CHECK_RC(sqlite3_open_v2(uri, &f, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL), SQLITE_OK);
    if (f) {
        CHECK(mw_scalar(f, "SELECT count(*) FROM u") == 14);
        CHECK(mw_scalar(f, "SELECT count(*) FROM t") == 10 + (wrc == SQLITE_OK ? 1 : 0));       // (what was acknowledged, and nothing else)
        sqlite3_close(f);
    }
    sqlite3_free(mp); munmap(sh, sizeof(mw_shm)); close(fd);
    mw_rmfiles(path);
    MW_DONE();
}
