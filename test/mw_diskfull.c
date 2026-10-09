// A full disk, for real: the database lives on a small disk image (macOS: hdiutil, no privileges needed; the test is skipped where the image cannot be made).
//
// A ballast file takes the free space of the volume down to a few megabytes, a child process then runs transactions that grow the database, the commit log, the segments and the maps
// until the volume is full (writes fail with ENOSPC, and a write into a hole of a mapped file would be a SIGBUS). Checks, after the ballast is deleted:
//   - the child was not killed by a signal (no SIGBUS), and did not hang;
//   - every acknowledged transaction is there, none is half there (each adds 2 to a counter and one row to a second table), the metadata agrees with the rows, the integrity check passes;
//   - the database takes transactions again;
//   - a database that is opened when there is no room at all fails to open, or opens, but never damages what is there.
// Thread mode and the shared multi-process mode. The same is done with a second child that keeps going when the space comes back (in-place recovery is reported, not required).
#include <stdint.h>
#include <errno.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <time.h>
#include <stdbool.h>
#include "mw_test.h"
#include "multiwriter.h"

static char img[256], mnt[256], ballast[300];

static bool vol_make (long mb) {
    char cmd[900];
    snprintf(img, sizeof img, "/tmp/mw_full_%d.img", (int)getpid()); snprintf(mnt, sizeof mnt, "/tmp/mw_full_mnt_%d", (int)getpid());
    snprintf(ballast, sizeof ballast, "%s/ballast", mnt);
#ifdef __APPLE__
    snprintf(img, sizeof img, "/tmp/mw_full_%d.sparseimage", (int)getpid());
    snprintf(cmd, sizeof cmd, "hdiutil create -size %ldm -fs APFS -type SPARSE -volname mwfull '%s' >/dev/null 2>&1", mb, img);
    if (system(cmd) != 0) return false;
    mkdir(mnt, 0755);
    snprintf(cmd, sizeof cmd, "hdiutil attach '%s' -nobrowse -mountpoint '%s' >/dev/null 2>&1", img, mnt);
    if (system(cmd) != 0) { unlink(img); return false; }
    return true;
#else
    // Linux: MW_DF_FS=tmpfs (default; sparse files, a write into a hole of a mapped file is a SIGBUS on a full volume) or ext4 on a loop device. Needs root (a container with --privileged).
    const char *fs = getenv("MW_DF_FS") ? getenv("MW_DF_FS") : "tmpfs";
    mkdir(mnt, 0755);
    if (!strcmp(fs, "tmpfs")) snprintf(cmd, sizeof cmd, "mount -t tmpfs -o size=%ldm tmpfs '%s' >/dev/null 2>&1", mb, mnt);
    else snprintf(cmd, sizeof cmd, "dd if=/dev/zero of='%s' bs=1M count=0 seek=%ld >/dev/null 2>&1 && mkfs.%s -q -F '%s' >/dev/null 2>&1 && mount -o loop '%s' '%s' >/dev/null 2>&1", img, mb, fs, img, img, mnt);
    if (system(cmd) != 0) { rmdir(mnt); unlink(img); return false; }
    return true;
#endif
}
static void vol_drop (void) {
    char cmd[900];
#ifdef __APPLE__
    snprintf(cmd, sizeof cmd, "hdiutil detach '%s' -force >/dev/null 2>&1", mnt);
#else
    snprintf(cmd, sizeof cmd, "umount '%s' >/dev/null 2>&1", mnt);
#endif
    (void)system(cmd);
    rmdir(mnt); unlink(img);
}

// Fills the volume with ballast files of 1 MB until a write fails with ENOSPC, then deletes `leave` bytes' worth of them (deleting frees the space at once; cutting a file back does not,
// on APFS). The volume keeps a reserve of its own that df counts as free and a write cannot use.
#define BALLAST_MB 1
static void ballast_drop (void) {
    char p[400]; for (int i = 0; i < 4000; i++) { snprintf(p, sizeof p, "%s.%d", ballast, i); if (unlink(p) != 0) break; }
}
static void ballast_fill (long leave) {
    static char buf[1 << 16]; memset(buf, 0x5a, sizeof buf);
    int n = 0;
    for (;; n++) {
        char p[400]; snprintf(p, sizeof p, "%s.%d", ballast, n);
        int fd = open(p, O_WRONLY | O_CREAT | O_TRUNC, 0644); if (fd < 0) break;
        bool full = false;
        for (int k = 0; k < (BALLAST_MB << 20) / (int)sizeof buf && !full; k++) { ssize_t w = write(fd, buf, sizeof buf); if (w != (ssize_t)sizeof buf) full = true; }
        close(fd);
        if (full) { n++; break; }
    }
    for (int i = n - 1; i >= 0 && leave >= 0; i--) {                      // (the last file is partial: it goes first, with whatever it held)
        char p[400]; snprintf(p, sizeof p, "%s.%d", ballast, i); unlink(p);
        if (i == n - 1) continue;
        leave -= (long)BALLAST_MB << 20; if (leave < 0) break;
    }
    if (getenv("MW_VERBOSE")) { char cmd[400]; snprintf(cmd, sizeof cmd, "df -k '%s' | tail -1 >&2", mnt); (void)system(cmd); }
}

static int open_db (const char *path, sqlite3 **db, bool shared) {
    char uri[600]; snprintf(uri, sizeof uri, "file:%s?mw=1&mw_log_max_mb=1%s", path, shared ? "&mw_mp=1" : "");
    int rc = sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
    if (rc == SQLITE_OK) { sqlite3_extended_result_codes(*db, 1); sqlite3_busy_timeout(*db, 0); }
    return rc;
}
static int integrity_ok (sqlite3 *db) {
    sqlite3_stmt *st; int ok = 0;
    if (sqlite3_prepare_v2(db, "PRAGMA integrity_check", -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW) ok = strcmp((const char *)sqlite3_column_text(st, 0), "ok") == 0;
    sqlite3_finalize(st); return ok;
}
static int exec_retry (sqlite3 *db, const char *sql) { for (int i = 0; i < 1000; i++) { int rc = mw_exec(db, sql); if ((rc & 0xff) != SQLITE_BUSY) return rc; } return SQLITE_BUSY; }

#define NROWS 100
static void make_db (const char *path) {
    sqlite3 *s; CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, MW_PLAIN_VFS), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER NOT NULL, s TEXT); CREATE TABLE big(id INTEGER PRIMARY KEY, v BLOB)"), SQLITE_OK);
    sqlite3_close(s);
    sqlite3 *db; CHECK_RC(open_db(path, &db, false), SQLITE_OK);
    CHECK_RC(mw_exec(db, "BEGIN"), SQLITE_OK);
    for (int i = 1; i <= NROWS; i++) { char q[100]; snprintf(q, sizeof q, "INSERT INTO t VALUES(%d, 0, NULL)", i); mw_exec(db, q); }
    CHECK_RC(mw_exec(db, "COMMIT"), SQLITE_OK);
    sqlite3_close(db);
}
static int check_rows (sqlite3 *db, int64_t *sum, int *rows) {                      // returns -1 when the table cannot be read; the counters themselves are checked against the commits that were acknowledged
    sqlite3_stmt *st; if (sqlite3_prepare_v2(db, "SELECT id, n FROM t", -1, &st, NULL) != SQLITE_OK) return -1;
    *sum = 0; *rows = 0;
    while (sqlite3_step(st) == SQLITE_ROW) { *sum += sqlite3_column_int64(st, 1); (*rows)++; }
    sqlite3_finalize(st); return 0;
}

// The child: transactions of two counter updates and one 2 KB row. `ms`: for how long it keeps trying after the first failure (the parent frees the space meanwhile when relief is set).
// Reports each acknowledged transaction, then (-1, failures, successes after the first failure).
static void child_run (const char *path, bool shared, int ackfd, int ms_after_fail, int max_txn, const char *gate) {
    alarm(120);
    sqlite3 *db = NULL; int failures = 0, after = 0; bool failed_once = false; struct timespec t_fail = {0};
    int orc = open_db(path, &db, shared);
    if (orc != SQLITE_OK && getenv("MW_VERBOSE")) fprintf(stderr, "child: open failed: %d %s (errno %d)\n", orc, db ? sqlite3_errmsg(db) : "", db ? sqlite3_system_errno(db) : 0);
    if (orc == SQLITE_OK) {
        if (gate) {                                                       // the parent fills the volume now, with the database open and its maps in place
            int ev = -3; if (write(ackfd, &ev, sizeof ev) != sizeof ev) _exit(2);
            for (int k = 0; k < 30000 && access(gate, F_OK) != 0; k++) usleep(1000);
        }
        unsigned rng = 7u;
        for (int i = 1; i <= max_txn; i++) {
            char sql[300]; rng = rng * 1103515245u + 12345u; int a = 1 + (int)(rng >> 16) % NROWS; rng = rng * 1103515245u + 12345u; int b = 1 + (int)(rng >> 16) % NROWS; if (a == b) continue;
            snprintf(sql, sizeof sql, "BEGIN; UPDATE t SET n=n+1 WHERE id=%d; UPDATE t SET n=n+1 WHERE id=%d; INSERT INTO big(v) VALUES(zeroblob(2000)); COMMIT", a, b);
            if (exec_retry(db, sql) == SQLITE_OK) { if (write(ackfd, &i, sizeof i) != sizeof i) _exit(2); if (failed_once) after++; if (after >= 2000) break; }
            else {
                failures++; mw_exec(db, "ROLLBACK");
                struct timespec now; clock_gettime(CLOCK_MONOTONIC, &now);
                if (!failed_once) { failed_once = true; t_fail = now; int ev = -2; if (write(ackfd, &ev, sizeof ev) != sizeof ev) _exit(2); }       // (tells the parent: now free the space)
                if ((now.tv_sec - t_fail.tv_sec) * 1000 + (now.tv_nsec - t_fail.tv_nsec) / 1000000 > ms_after_fail) break;
                usleep(2000);
            }
            if (i % 50 == 0) { mw_compact_result r; sqlite3_file_control(db, "main", MW_FCNTL_COMPACT, &r); }
        }
        if (getenv("MW_VERBOSE")) { int64_t sm; int rw; ballast_drop(); int b = check_rows(db, &sm, &rw); fprintf(stderr, "child, before close (space freed): %d rows, %d disagree\n", rw, b); }
        sqlite3_close(db);
    } else failures = -1;
    int tag = -1; if (write(ackfd, &tag, sizeof tag) != sizeof tag || write(ackfd, &failures, sizeof failures) != sizeof failures || write(ackfd, &after, sizeof after) != sizeof after) _exit(2);
    _exit(0);
}
static void verify (const char *path, bool shared, int acked, const char *what) {
    if (getenv("MW_DF_KEEP")) { char cmd[600]; snprintf(cmd, sizeof cmd, "rm -rf /tmp/dfdump; mkdir -p /tmp/dfdump && cp %s* /tmp/dfdump/ 2>/dev/null; ls -la /tmp/dfdump >&2", path); (void)system(cmd); }
    sqlite3 *r; int rc = open_db(path, &r, shared);
    if (rc != SQLITE_OK) { printf("FAIL %s: cannot open the database again (%d)\n", what, rc); mw_failures++; return; }
    int64_t sum; int rows; int bad = check_rows(r, &sum, &rows); int64_t nbig = mw_scalar(r, "SELECT count(*) FROM big");
    if (bad != 0 || rows != NROWS || (sum & 1) || sum < 2 * acked || sum != 2 * nbig) {
        printf("FAIL %s: %d rows (want %d), %d disagree with the metadata, sum(n)=%lld, big rows %lld, acknowledged %d\n", what, rows, NROWS, bad, (long long)sum, (long long)nbig, acked); mw_failures++;
    }
    if (!integrity_ok(r)) { printf("FAIL %s: integrity_check\n", what); mw_failures++; }
    int ok = 0, lastrc = 0; for (int i = 0; i < 30; i++) { lastrc = exec_retry(r, "BEGIN; UPDATE t SET n=n+1 WHERE id=1; UPDATE t SET n=n+1 WHERE id=2; INSERT INTO big(v) VALUES(zeroblob(2000)); COMMIT"); if (lastrc == SQLITE_OK) ok++; else mw_exec(r, "ROLLBACK"); }
    if (ok != 30) printf("    last result %d (%s)\n", lastrc, sqlite3_errmsg(r));
    if (ok != 30) { printf("FAIL %s: after the space came back %d of 30 transactions committed\n", what, ok); mw_failures++; }
    sqlite3_close(r);
}

static int g_only = -1, g_seq = 0;                       // (MW_DF_ONLY=n runs only the nth scenario, to look at one)
static void scenario_run (bool shared, long leave, int relief_ms, const char *name, bool fill_after_open) {
    if (g_only >= 0 && g_seq++ != g_only) return;
    char path[400]; snprintf(path, sizeof path, "%s/db", mnt);
    ballast_drop(); mw_rmfiles(path);
    make_db(path);
    char gate[420]; snprintf(gate, sizeof gate, "%s.gate", ballast); unlink(gate);
    if (!fill_after_open) ballast_fill(leave);
    int pfd[2]; CHECK(pipe(pfd) == 0);
    pid_t c = fork();
    if (c == 0) { close(pfd[0]); child_run(path, shared, pfd[1], relief_ms ? relief_ms * 6 + 1000 : 200, 100000, fill_after_open ? gate : NULL); _exit(3); }
    close(pfd[1]);
    int acked = 0, v, failures = 0, after = 0;
    for (;;) {                                                              // (the child's reports: acknowledged transactions, -2 at its first failure, -1 and the counts at the end)
        ssize_t r = read(pfd[0], &v, sizeof v); if (r != (ssize_t)sizeof v) break;
        if (v == -3) { ballast_fill(leave); close(open(gate, O_CREAT | O_WRONLY, 0644)); continue; }
        if (v == -2) { if (relief_ms) { usleep(relief_ms * 1000); ballast_drop(); } continue; }
        if (v == -1) { if (read(pfd[0], &failures, sizeof failures) != sizeof failures || read(pfd[0], &after, sizeof after) != sizeof after) {} break; }
        acked++;
    }
    int st; waitpid(c, &st, 0);
    int sig = WIFSIGNALED(st) ? WTERMSIG(st) : 0, code = WIFEXITED(st) ? WEXITSTATUS(st) : -1;
    close(pfd[0]);
    ballast_drop(); unlink(gate);
    if (sig || code) { printf("FAIL %s, %s: the child %s %d\n", shared ? "shared" : "thread", name, sig ? "was killed by signal" : "exited with code", sig ? sig : code); mw_failures++; }
    else {
        verify(path, shared, acked, name);
        if (relief_ms && after < 1000) { printf("FAIL %s, %s: the space came back and only %d transactions committed on the open connection (%d failed): not recovered in place\n", shared ? "shared" : "thread", name, after, failures); mw_failures++; }
    }
    printf("%-6s mode, %-44s: %5d transactions acknowledged, %4d failed, %4d committed after the first failure%s\n", shared ? "shared" : "thread", name, acked, failures, after, relief_ms ? " (space freed while running)" : "");
    mw_rmfiles(path);
}

int main (void) {
    setvbuf(stdout, NULL, _IOLBF, 0);
    if (getenv("MW_DF_ONLY")) g_only = atoi(getenv("MW_DF_ONLY"));
    if (!vol_make(160)) { printf("test/mw_diskfull.c: skipped (no disk image can be made here)\n"); return 0; }
    for (int mode = 0; mode < 2; mode++) {
        bool shared = mode == 1;
        // (the shared mode lays out its files at the open: a segment of 16 MB, written, and maps of some hundred MB, sparse: it needs room for the first)
        // (the volume keeps a reserve of its own, a few MB, that df counts as free and a write cannot use: the amounts are what the ballast leaves, not what is usable)
        const long base = shared ? 18L << 20 : 0;
        scenario_run(shared, base + (9L << 20), 0, shared ? "27 MB left, the volume fills up" : "9 MB left, the volume fills up", false);
        scenario_run(shared, base + (6L << 20), 0, shared ? "24 MB left" : "6 MB left", false);
        scenario_run(shared, base + (7L << 20), 300, shared ? "25 MB left, space returns" : "7 MB left, space returns", false);
        scenario_run(shared, 0, 0, "no room at all", false);
        scenario_run(shared, 0, 0, "the volume fills up after the open", true);
        scenario_run(shared, 0, 300, "full after the open, space returns", true);
    }
    vol_drop();
    MW_DONE();
}
