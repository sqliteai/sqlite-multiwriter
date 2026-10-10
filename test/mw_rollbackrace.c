// A commit whose append to the log fails, while others commit pages that grow the file (their relocations build on the heads of the chains, page 1 among them). The failed commit gives its
// epoch back and takes its versions out of the chains. Between the one and the other another commit could take that epoch: its own version of the epoch was then the one that was freed,
// or it was built on the version of the commit that never was (a page that nobody referenced, two references to one page). The time between them is widened here
// (MW_TEST_ROLLBACK_DELAY_US); in every run what each thread acknowledged is in the database, the file is sound, and the same after a reopen.
#include <pthread.h>
#include "mw_test.h"
#include "multiwriter.h"

enum { NT = 8, PER = 80 };
static char g_path[300];
static int g_ok[NT];

static void *worker (void *a) {
    long id = (long)a; sqlite3 *db = NULL; char uri[400], sql[200];
    snprintf(uri, sizeof uri, "file:%s?vfs=multiwriter&mw_gc=0", g_path);
    if (sqlite3_open_v2(uri, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL) != SQLITE_OK) return NULL;
    sqlite3_busy_timeout(db, 0);
    for (int i = 0; i < PER; i++) {
        snprintf(sql, sizeof sql, "INSERT INTO t%ld VALUES (%d, zeroblob(3000))", id, i);
        if (mw_exec(db, sql) == SQLITE_OK) g_ok[id]++;
    }
    sqlite3_close(db);
    return NULL;
}
static int check (const char *label) {
    char uri[400]; snprintf(uri, sizeof uri, "file:%s?vfs=multiwriter&mw_gc=0", g_path);
    sqlite3 *db = NULL; int bad = 0;
    if (sqlite3_open_v2(uri, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL) != SQLITE_OK) return 1;
    for (int i = 0; i < NT; i++) { char q[100]; snprintf(q, sizeof q, "SELECT count(*) FROM t%d", i); long c = (long)mw_scalar(db, q); if (c != g_ok[i]) { printf("%s: t%d has %ld rows, %d acknowledged\n", label, i, c, g_ok[i]); bad++; } }
    sqlite3_stmt *st = NULL; char msg[200] = "";
    if (sqlite3_prepare_v2(db, "PRAGMA integrity_check", -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW) snprintf(msg, sizeof msg, "%s", (const char *)sqlite3_column_text(st, 0));
    sqlite3_finalize(st);
    if (strcmp(msg, "ok") != 0) { printf("%s: integrity_check: %s\n", label, msg); bad++; }
    sqlite3_close(db);
    return bad;
}

int main (void) {
    setenv("MW_TEST_ROLLBACK_DELAY_US", "300", 1);
    int bad = 0;
    for (int round = 0; round < 6; round++) {
        mw_tmpdb(g_path, sizeof g_path, "rbrace");
        sqlite3 *s; CHECK_RC(sqlite3_open_v2(g_path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, MW_PLAIN_VFS), SQLITE_OK);
        CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL"), SQLITE_OK);
        for (int i = 0; i < NT; i++) { char q[100]; snprintf(q, sizeof q, "CREATE TABLE t%d(a INTEGER PRIMARY KEY, b BLOB)", i); CHECK_RC(mw_exec(s, q), SQLITE_OK); }
        sqlite3_close(s);
        memset(g_ok, 0, sizeof g_ok);
        mw_fault_arm(MW_FAULT_LOG_WRITE_ERR, 30 + round * 17);                    // one append fails, at a different place in each round
        pthread_t th[NT];
        for (long i = 0; i < NT; i++) pthread_create(&th[i], NULL, worker, (void *)i);
        for (int i = 0; i < NT; i++) pthread_join(th[i], NULL);
        mw_fault_disarm_all();
        int b = check("after the run") + check("after a reopen");
        printf("round %d: %d rows acknowledged, %d problems\n", round, ({ int t = 0; for (int i = 0; i < NT; i++) t += g_ok[i]; t; }), b);
        bad += b;
        mw_rmdb(g_path);
    }
    CHECK(bad == 0);
    MW_DONE();
}
