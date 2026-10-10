// The group sync of the segmented log. The leader of a sync makes durable what is written up to a point, and publishes it (sy_done); a commit whose end is below it returns without
// a sync of its own. So the point must be one where the log is complete and stays: it used to be taken from the append cursor (sl_seg and sl_end, two words), and
//   1. while the log rolled to the next segment, a leader could see the new segment with the end of the old one (about a megabyte that was then taken for synced);
//   2. a record that was appended and then taken out of the log (the install of its versions failed) was below the point that a leader had already declared durable, and the commit that
//      took its place was acknowledged without a sync.
// Each commit is checked by the number of syncs of the log that were made while it ran (the I/O injection counts them): a commit with synchronous=FULL that returns with none was
// covered by a sync that finished before its record was written.
#include <pthread.h>
#include <errno.h>
#include "mw_test.h"
#include "multiwriter.h"

enum { NT = 4 };
static char g_path[300];
static _Atomic int g_viol, g_ok, g_fail;
static int g_per;
static void *worker (void *a) {
    long id = (long)a; sqlite3 *db = NULL; char uri[400], sql[200];
    snprintf(uri, sizeof uri, "file:%s?vfs=multiwriter&mw_mp=1&mw_gc=0", g_path);
    if (sqlite3_open_v2(uri, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL) != SQLITE_OK) return NULL;
    sqlite3_busy_timeout(db, 2000);
    snprintf(sql, sizeof sql, "UPDATE t%ld SET b = randomblob(8000) WHERE a = 1", id);
    for (int i = 0; i < g_per; i++) {
        long before = mw_io_fault_calls();
        int rc = mw_exec(db, sql);
        long after = mw_io_fault_calls();
        if (rc == SQLITE_OK) { g_ok++; if (after == before) g_viol++; } else g_fail++;
    }
    sqlite3_close(db);
    return NULL;
}
static void scenario (const char *label, const char *env, const char *val, const char *env2, const char *val2, int fault_nth, int per) {
    setenv(env, val, 1); if (env2) setenv(env2, val2, 1);
    g_viol = g_ok = g_fail = 0; g_per = per;
    mw_tmpdb(g_path, sizeof g_path, "syncwindow");
    sqlite3 *s; CHECK_RC(sqlite3_open_v2(g_path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, MW_PLAIN_VFS), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL"), SQLITE_OK);
    for (int i = 0; i < NT; i++) { char q[200]; snprintf(q, sizeof q, "CREATE TABLE t%d(a INTEGER PRIMARY KEY, b BLOB); INSERT INTO t%d VALUES (1, zeroblob(10))", i, i); CHECK_RC(mw_exec(s, q), SQLITE_OK); }
    sqlite3_close(s);
    if (fault_nth) mw_fault_arm(MW_FAULT_INSTALL_ERR, fault_nth);
    mw_io_fault_arm(MW_IO_SYNC, 1000000000L, EIO, 0, 0);              // counts the syncs, fails none
    pthread_t th[NT];
    for (long i = 0; i < NT; i++) pthread_create(&th[i], NULL, worker, (void *)i);
    for (int i = 0; i < NT; i++) pthread_join(th[i], NULL);
    mw_io_fault_disarm();
    printf("%s: %d commits, %d refused, %d acknowledged without a sync of the log\n", label, (int)g_ok, (int)g_fail, (int)g_viol);
    CHECK(g_ok > 20);
    CHECK(g_viol == 0);
    mw_rmdb(g_path);
    unsetenv(env); if (env2) unsetenv(env2);
}

int main (void) {
    setenv("MW_SEG_MB", "1", 1);
    // 1. segments of 1 MB (a roll every 120 commits); the roll takes 20 ms between its two words, and the leaders take their look 10 ms late
    setenv("MW_TEST_SYNC_DELAY_US", "10000", 1);
    scenario("roll", "MW_TEST_ROLL_DELAY_US", "20000", NULL, NULL, 0, 300);
    // 2. the install of the 60th commit fails after 30 ms, the leaders take their look 15 ms late
    setenv("MW_TEST_SYNC_DELAY_US", "15000", 1);
    scenario("install failure", "MW_FAULT_DELAY_US", "30000", NULL, NULL, 60, 200);
    MW_DONE();
}
