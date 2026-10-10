// A commit whose append to the log fails, when others are behind it. The commits that follow have their records written in the ring, and wait for the prefix of the log to be written up to them
// before they sync: the failed record never is, so the failure has to wake them (they used to wait forever: the database was hung until the process ended). Each of them returns, with success
// or with an error, and what succeeded is there after a reopen.
#include <pthread.h>
#include "mw_test.h"
#include "multiwriter.h"
enum { NT = 16, PER = 120 };
static char g_path[300];
static _Atomic int g_done, g_ok, g_fail;
static void *worker (void *a) {
    long id = (long)a; sqlite3 *db = NULL; char uri[400], sql[200];
    snprintf(uri, sizeof uri, "file:%s?vfs=multiwriter&mw_gc=0", g_path);
    if (sqlite3_open_v2(uri, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL) != SQLITE_OK) { g_done++; return NULL; }
    sqlite3_busy_timeout(db, 0);
    for (int i = 0; i < PER; i++) {
        snprintf(sql, sizeof sql, "INSERT INTO t%ld VALUES (%d, zeroblob(100))", id, i);
        if (mw_exec(db, sql) == SQLITE_OK) g_ok++; else g_fail++;
    }
    sqlite3_close(db);
    g_done++;
    return NULL;
}
static void *watchdog (void *a) { (void)a; for (int i = 0; i < 300; i++) { usleep(100000); if (g_done == NT) return NULL; } printf("FAIL: hung, %d of %d threads finished\n", (int)g_done, NT); fflush(stdout); _exit(1); }

int main (void) {
    setenv("MW_FAULT_DELAY_US", "200000", 1);              // the failing append takes 200 ms to fail: the others queue up behind it
    mw_tmpdb(g_path, sizeof g_path, "appendfail");
    sqlite3 *s; CHECK_RC(sqlite3_open_v2(g_path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, MW_PLAIN_VFS), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL"), SQLITE_OK);
    for (int i = 0; i < NT; i++) { char q[100]; snprintf(q, sizeof q, "CREATE TABLE t%d(a INTEGER PRIMARY KEY, b BLOB)", i); CHECK_RC(mw_exec(s, q), SQLITE_OK); }
    sqlite3_close(s);
    pthread_t wd; pthread_create(&wd, NULL, watchdog, NULL);
    mw_fault_arm(MW_FAULT_LOG_WRITE_ERR, 150);              // one append fails, the 150th of the run
    pthread_t th[NT];
    for (long i = 0; i < NT; i++) pthread_create(&th[i], NULL, worker, (void *)i);
    for (int i = 0; i < NT; i++) pthread_join(th[i], NULL);
    pthread_join(wd, NULL);
    printf("committed %d, refused %d\n", (int)g_ok, (int)g_fail);
    CHECK(g_done == NT);
    CHECK(g_fail >= 1);
    // what was acknowledged is in the database after a reopen
    char uri[400]; snprintf(uri, sizeof uri, "file:%s?vfs=multiwriter", g_path);
    sqlite3 *r = NULL; CHECK_RC(sqlite3_open_v2(uri, &r, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL), SQLITE_OK);
    long rows = 0; for (int i = 0; i < NT; i++) { char q[100]; snprintf(q, sizeof q, "SELECT count(*) FROM t%d", i); rows += (long)mw_scalar(r, q); }
    CHECK(rows == g_ok);
    sqlite3_close(r); mw_rmdb(g_path);
    MW_DONE();
}
