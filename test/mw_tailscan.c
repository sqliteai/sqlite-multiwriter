// The log is rewritten at a compaction so that it holds only the records newer than the new base. The place where those begin was found by reading the headers of the records through the
// mapping up to the end of the log, before the writes that were in flight had finished: a record that has its place assigned and is not in the file yet starts at the end of the file,
// and when that is on a page boundary its header is in a page beyond the file: the process died of SIGBUS. Here a commit is held between the assignment of its place and its write
// (MW_FAULT_APPEND_STALL) while the compaction runs, the log being a whole number of pages.
#include <pthread.h>
#include <stdbool.h>
#include <sys/stat.h>
#include <unistd.h>
#include "mw_test.h"
#include "multiwriter.h"

static char g_uri[600];
static int g_held_rc = -1;
static void *held (void *a) {
    (void)a; sqlite3 *w = NULL;
    if (sqlite3_open_v2(g_uri, &w, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL) != SQLITE_OK) return NULL;
    sqlite3_busy_timeout(w, 3000);
    g_held_rc = mw_exec(w, "UPDATE t SET n = n + 1 WHERE id = 1");
    sqlite3_close(w);
    return NULL;
}

int main (void) {
    setenv("MW_FAULT_DELAY_US", "600000", 1);
    long page = sysconf(_SC_PAGESIZE);
    char path[300], lp[330]; mw_tmpdb(path, sizeof path, "tailscan"); snprintf(lp, sizeof lp, "%s-mw", path);
    snprintf(g_uri, sizeof g_uri, "file:%s?vfs=multiwriter&mw_gc=0&mw_log_max_mb=512", path);
    sqlite3 *s; CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, MW_PLAIN_VFS), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER, pad BLOB); INSERT INTO t VALUES (1, 0, NULL)"), SQLITE_OK);
    sqlite3_close(s);
    sqlite3 *w = NULL; CHECK_RC(sqlite3_open_v2(g_uri, &w, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL), SQLITE_OK);
    sqlite3_busy_timeout(w, 3000);
    // commit until the log is more than 8 pages and its size is a whole number of pages: the next record starts at the end of the file, on a page boundary
    long size = 0; int commits = 0; bool aligned = false;
    for (int i = 0; i < 30000 && !aligned; i++) {
        CHECK_RC(mw_exec(w, "UPDATE t SET n = n + 1 WHERE id = 1"), SQLITE_OK); commits++;
        struct stat sb; if (stat(lp, &sb) != 0) continue;
        size = (long)sb.st_size;
        aligned = size > 8 * page && size % page == 0;
    }
    printf("log of %ld bytes after %d commits (page %ld): %s\n", size, commits, page, aligned ? "on a page boundary" : "NOT aligned");
    CHECK(aligned);
    mw_fault_arm(MW_FAULT_APPEND_STALL, 1);                     // the next append is held
    pthread_t t; pthread_create(&t, NULL, held, NULL);
    usleep(150000);                                             // (its place is assigned; nothing is written)
    mw_compact_result r; memset(&r, 0, sizeof r);
    int rc = sqlite3_file_control(w, "main", MW_FCNTL_COMPACT, &r);
    printf("compaction rc=%d (no crash)\n", rc);
    pthread_join(t, NULL);
    CHECK(rc == SQLITE_OK || (rc & 0xff) == SQLITE_BUSY);
    CHECK_RC(g_held_rc, SQLITE_OK);
    long long n = (long long)mw_scalar(w, "SELECT n FROM t WHERE id = 1");
    CHECK(n == commits + 1);
    sqlite3_close(w);
    sqlite3 *r2 = NULL; CHECK_RC(sqlite3_open_v2(g_uri, &r2, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL), SQLITE_OK);
    CHECK((long long)mw_scalar(r2, "SELECT n FROM t WHERE id = 1") == commits + 1);
    sqlite3_close(r2);
    mw_rmdb(path);
    MW_DONE();
}
