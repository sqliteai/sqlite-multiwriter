// Minimal helpers shared by the Multi-Writer test programs.
#ifndef MW_TEST_H
#define MW_TEST_H
#include <glob.h>
#include <stdint.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "sqlite3.h"


#ifdef __linux__
// kill -USR1 <pid> prints the stack of every thread (a hung test in a container with no debugger)
#include <execinfo.h>
#include <signal.h>
#include <dirent.h>
#include <sys/syscall.h>
static volatile sig_atomic_t mw_dump_fan;
static void mw_dump_handler (int sig) {
    (void)sig; void *bt[40]; int n = backtrace(bt, 40); char hdr[64]; int l = snprintf(hdr, sizeof hdr, "--- thread %ld ---\n", (long)syscall(SYS_gettid)); (void)!write(2, hdr, (size_t)l);
    backtrace_symbols_fd(bt, n, 2);
    if (!mw_dump_fan) {
        mw_dump_fan = 1; DIR *d = opendir("/proc/self/task");
        if (d) { struct dirent *e; while ((e = readdir(d))) { long t = atol(e->d_name); if (t > 0 && t != (long)syscall(SYS_gettid)) syscall(SYS_tgkill, (long)getpid(), t, SIGUSR1); } closedir(d); }
    }
}
__attribute__((constructor)) static void mw_dump_init (void) { signal(SIGUSR1, mw_dump_handler); }
#endif

static int mw_failures = 0;
#define CHECK(c) do { if (!(c)) { printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #c); mw_failures++; } } while (0)
#define CHECK_RC(rc, want) do { int _r = (rc); if (_r != (want)) { printf("FAIL %s:%d: %s = %d (want %d)\n", __FILE__, __LINE__, #rc, _r, (int)(want)); mw_failures++; } } while (0)

static inline int mw_exec (sqlite3 *db, const char *sql) {
    char *err = NULL;
    int rc = sqlite3_exec(db, sql, NULL, NULL, &err);
    if (err) { if (getenv("MW_VERBOSE")) printf("  exec error: %s (%s)\n", err, sql); sqlite3_free(err); }
    return rc;
}

static inline int64_t mw_scalar (sqlite3 *db, const char *sql) {
    sqlite3_stmt *st = NULL;
    int64_t v = -1;
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW) v = sqlite3_column_int64(st, 0);
    sqlite3_finalize(st);
    return v;
}

// every file that belongs to a database: it, SQLite's own and the engine's (log, segments, index, locks, owner maps): a log that outlives its database is replayed on the next one of the same name
static inline void mw_rmfiles (const char *path) {
    char p[600]; const char *sfx[] = {"", "-wal", "-shm", "-journal"};
    for (int i = 0; i < 4; i++) { snprintf(p, sizeof p, "%s%s", path, sfx[i]); unlink(p); }
    glob_t g; snprintf(p, sizeof p, "%s-mw*", path);
    if (glob(p, 0, NULL, &g) == 0) { for (size_t i = 0; i < g.gl_pathc; i++) unlink(g.gl_pathv[i]); globfree(&g); }
#ifdef __linux__
    { uint64_t h = 1469598103934665603ull; for (const char *c = path; *c; c++) { h ^= (unsigned char)*c; h *= 1099511628211ull; }          // (the files of the shared mode live in /dev/shm there: mw_sidecar_path)
      snprintf(p, sizeof p, "/dev/shm/mw-%016llx-*", (unsigned long long)h);
      if (glob(p, 0, NULL, &g) == 0) { for (size_t i = 0; i < g.gl_pathc; i++) unlink(g.gl_pathv[i]); globfree(&g); } }
#endif
}
static inline char *mw_tmpdb (char *buf, size_t n, const char *tag) {
    snprintf(buf, n, "/tmp/mw_%s_%d.db", tag, (int)getpid());
    mw_rmfiles(buf);
    return buf;
}

static inline void mw_rmdb (const char *path) { mw_rmfiles(path); }

#define MW_DONE() do { printf("%s: %d failure(s)\n", __FILE__, mw_failures); return mw_failures ? 1 : 0; } while (0)
#endif
