// The last connection of a database takes its state down (a compaction of the log into the file, an fsync, the unlink of the sidecar files). That was done with the mutex of the registry
// held, the one that SQLite takes in every sqlite3_open: a database being closed held up the open of any other in the process for as long as the close took (the size of the log), and a
// fork() at that moment left the child with the mutex locked by a thread that does not exist there (its first open hung). Now the close does not hold it, an open of the same path waits
// for the close to end (the log is locked until then), and fork() takes the mutex across the fork.
#include <pthread.h>
#include <signal.h>
#include <sys/wait.h>
#include <time.h>
#include "mw_test.h"
#include "multiwriter.h"

static double now_s (void) { struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts); return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9; }
static sqlite3 *g_closing;
static double g_close_took;
static void *closer (void *a) { (void)a; double t = now_s(); sqlite3_close(g_closing); g_close_took = now_s() - t; return NULL; }
static sqlite3 *mk (const char *path) {
    char uri[400]; snprintf(uri, sizeof uri, "file:%s?vfs=multiwriter", path);
    sqlite3 *d = NULL;
    if (sqlite3_open_v2(uri, &d, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL) != SQLITE_OK) return NULL;
    mw_exec(d, "CREATE TABLE IF NOT EXISTS t(a)"); mw_exec(d, "INSERT INTO t VALUES (1)");
    return d;
}

int main (void) {
    setenv("MW_TEST_CLOSE_DELAY_US", "400000", 1);                 // the close of a database takes 400 ms (it is a compaction in life)
    char pa[256], pb[256]; mw_tmpdb(pa, sizeof pa, "closestall_a"); mw_tmpdb(pb, sizeof pb, "closestall_b");
    char ub[400]; snprintf(ub, sizeof ub, "file:%s?vfs=multiwriter", pb);

    // 1. another database is opened while one is being closed: it does not wait for the close
    sqlite3 *b0 = mk(pb); CHECK(b0 != NULL);                        // (it exists: the open of the other is an open, not a creation)
    sqlite3_close(b0);
    g_closing = mk(pa); CHECK(g_closing != NULL);
    pthread_t t; pthread_create(&t, NULL, closer, NULL);
    usleep(50000);
    double t0 = now_s();
    sqlite3 *b = NULL; int rc = sqlite3_open_v2(ub, &b, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL);
    double open_took = now_s() - t0;
    CHECK_RC(rc, SQLITE_OK);
    CHECK(mw_scalar(b, "SELECT count(*) FROM t") == 1);
    sqlite3_close(b);
    pthread_join(t, NULL);
    printf("close took %.0f ms; the open of another database, started 50 ms into it, took %.0f ms\n", g_close_took * 1000, open_took * 1000);
    CHECK(g_close_took > 0.3);
    CHECK(open_took < 0.2);

    // 2. the same database is opened while it is being closed: it waits for the close to end, and finds the database whole
    g_closing = mk(pa); CHECK(g_closing != NULL);
    pthread_create(&t, NULL, closer, NULL);
    usleep(50000);
    char ua[400]; snprintf(ua, sizeof ua, "file:%s?vfs=multiwriter", pa);
    sqlite3 *a2 = NULL; rc = sqlite3_open_v2(ua, &a2, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL);
    CHECK_RC(rc, SQLITE_OK);
    if (rc == SQLITE_OK) CHECK(mw_scalar(a2, "SELECT count(*) FROM t") >= 1);
    sqlite3_close(a2);
    pthread_join(t, NULL);

    // 3. a fork while a database is being closed: the child can open a database
    g_closing = mk(pa); CHECK(g_closing != NULL);
    pthread_create(&t, NULL, closer, NULL);
    usleep(50000);
    pid_t c = fork();
    if (c == 0) {
        alarm(10);
        sqlite3 *d = NULL;
        int r = sqlite3_open_v2(ub, &d, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL);
        _exit(r == SQLITE_OK && mw_scalar(d, "SELECT count(*) FROM t") == 1 ? 0 : 4);
    }
    int st = 0; waitpid(c, &st, 0);
    CHECK(WIFEXITED(st) && WEXITSTATUS(st) == 0);
    pthread_join(t, NULL);
    mw_rmdb(pa); mw_rmdb(pb);
    MW_DONE();
}
