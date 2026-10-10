// A database whose first open fails (the log is damaged) while other connections open it at once. The first one leaves the state that it had made (the descriptor of the log, with its
// lock), the others found the structure of the database in the process and ran the opening again on top of it: the second descriptor could not take the lock (SQLITE_BUSY, which hid
// the real error), the first one was never closed, and the database could not be opened again in this process after the log was repaired. Now they all get the result of the first, the
// descriptors are the same after, and the database opens when the log is gone.
#include <pthread.h>
#include <stdatomic.h>
#include "mw_test.h"
#include "multiwriter.h"
#ifndef _WIN32
#include <dirent.h>
static char g_uri[600];
static atomic_int g_arrived; static int g_nt;
static int g_rc[16];
static void *opener (void *a) {
    int i = (int)(long)a; sqlite3 *db = NULL;
    atomic_fetch_add(&g_arrived, 1); while (atomic_load(&g_arrived) < g_nt) {}
    int rc = sqlite3_open_v2(g_uri, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL);
    if (rc == SQLITE_OK) rc = mw_exec(db, "SELECT count(*) FROM sqlite_master");
    g_rc[i] = rc;
    sqlite3_close(db);
    return NULL;
}
static int open_fds (void) { DIR *d = opendir("/dev/fd"); int n = 0; if (!d) return -1; while (readdir(d)) n++; closedir(d); return n; }
#endif

int main (void) {
#ifndef _WIN32
    char path[256]; mw_tmpdb(path, sizeof path, "openfail"); snprintf(g_uri, sizeof g_uri, "file:%s?vfs=multiwriter", path);
    sqlite3 *db; CHECK_RC(sqlite3_open_v2(g_uri, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL), SQLITE_OK);
    CHECK_RC(mw_exec(db, "CREATE TABLE t(a); INSERT INTO t VALUES (1)"), SQLITE_OK);
    sqlite3_close(db);
    char lp[320]; snprintf(lp, sizeof lp, "%s-mw", path);
    g_nt = 8;
    int fds0 = open_fds();
    for (int round = 0; round < 12; round++) {
        FILE *f = fopen(lp, "wb"); CHECK(f != NULL); if (!f) break;
        for (int i = 0; i < 8192; i++) fputc('x', f);
        fclose(f);
        atomic_store(&g_arrived, 0);
        pthread_t th[16];
        for (long i = 0; i < g_nt; i++) pthread_create(&th[i], NULL, opener, (void *)i);
        for (int i = 0; i < g_nt; i++) pthread_join(th[i], NULL);
        int fail = 0, busy = 0, same = 1;
        for (int i = 0; i < g_nt; i++) { if (g_rc[i] != SQLITE_OK) fail++; if ((g_rc[i] & 0xff) == SQLITE_BUSY) busy++; if (g_rc[i] != g_rc[0]) same = 0; }
        if (round == 0) printf("results of the %d openers: %d %d %d %d ...; descriptors before %d, after %d\n", g_nt, g_rc[0], g_rc[1], g_rc[2], g_rc[3], fds0, open_fds());
        CHECK(fail == g_nt);                                       // the log is damaged: none of them opens it
        CHECK(busy == 0);                                          // and the error is the one of the log, not "busy"
        CHECK(same);
        CHECK(open_fds() == fds0);                                 // nothing is left open
        unlink(lp);                                                // the log is gone (the database is intact: it is plain)
        int rc = sqlite3_open_v2(g_uri, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL);
        CHECK_RC(rc, SQLITE_OK);
        if (rc == SQLITE_OK) CHECK(mw_scalar(db, "SELECT a FROM t") == 1);
        sqlite3_close(db);
        if (mw_failures) break;
    }
    mw_rmdb(path);
#endif
    MW_DONE();
}
