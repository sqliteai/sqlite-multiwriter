// Slots of a process that is killed with a pool of reserved slots come back: a database used by several processes, one of which is killed (SIGKILL) while it holds slots; another process that merges finds them (they are
// in no run, not free, and the register of reservations names a process that is gone), and the slots of the processes that are alive are left alone (the data of every process stays readable).
#include <signal.h>
#include <time.h>
#include <sys/wait.h>
#include "mw_test.h"
#include "multiwriter.h"
#include "multiwriter_meta.h"

static int open_mp (const char *path, sqlite3 **db) {
    char uri[400]; snprintf(uri, sizeof uri, "file:%s?mw=2&mw_mp=1&mw_cdc=1", path);
    int rc = sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
    if (rc == SQLITE_OK) { sqlite3_extended_result_codes(*db, 1); sqlite3_busy_timeout(*db, 0); }
    return rc;
}
static int retry (sqlite3 *db, const char *sql) { for (int i = 0; i < 1000; i++) { int rc = mw_exec(db, sql); if ((rc & 0xff) != SQLITE_BUSY) return rc; } return SQLITE_BUSY; }

int main (void) {
    char path[256]; mw_tmpdb(path, sizeof path, "slsw");
    setenv("MW_META_SWEEP_MS", "300", 1); setenv("MW_META_FLUSH_ROWS", "1", 1); setenv("MW_META_FLUSH_MS", "1", 1);
    sqlite3 *a; CHECK_RC(open_mp(path, &a), SQLITE_OK);
    CHECK_RC(mw_exec(a, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER)"), SQLITE_OK);
    for (int i = 0; i < 50; i++) { char q[100]; snprintf(q, sizeof q, "INSERT INTO t VALUES(%d, 0)", i); CHECK_RC(retry(a, q), SQLITE_OK); }
    // a child that flushes (it takes a pool of slots), then is killed with the pool
    int pfd[2]; CHECK(pipe(pfd) == 0);
    pid_t c = fork();
    if (c == 0) {
        sqlite3 *b; if (open_mp(path, &b) != SQLITE_OK) _exit(2);
        for (int i = 0; i < 400; i++) { char q[100]; snprintf(q, sizeof q, "UPDATE t SET n = n + 1 WHERE id = %d", i % 50); retry(b, q); }
        for (int w = 0; w < 100; w++) {                                                           // (until its flusher has run: it has slots, and the register says so)
            char q[120]; snprintf(q, sizeof q, "SELECT count(*) FROM mw_resv WHERE pid = %d", (int)getpid());
            if (mw_scalar(b, q) > 0) break;
            struct timespec ts = { 0, 50 * 1000000L }; nanosleep(&ts, NULL); retry(b, "UPDATE t SET n = n + 1 WHERE id = 3");
        }
        (void)!write(pfd[1], "x", 1);
        for (;;) pause();
    }
    char ch; CHECK(read(pfd[0], &ch, 1) == 1);
    kill(c, SIGKILL); int st; waitpid(c, &st, 0);
    // this process goes on: its flushes start its merge thread, which sweeps
    for (int i = 0; i < 300; i++) { char q[100]; snprintf(q, sizeof q, "UPDATE t SET n = n + 1 WHERE id = %d", i % 50); CHECK_RC(retry(a, q), SQLITE_OK); }
    mw_meta *m = NULL; CHECK_RC(sqlite3_file_control(a, "main", MW_FCNTL_META, &m), SQLITE_OK);
    uint64_t rs[13] = {0};
    for (int w = 0; w < 100; w++) { mw_meta_run_stats(m, rs); { char q2[160]; snprintf(q2, sizeof q2, "SELECT count(*) FROM mw_resv WHERE pid = %d", (int)c); if (mw_scalar(a, q2) == 0) break; } struct timespec ts = { 0, 100 * 1000000L }; nanosleep(&ts, NULL); retry(a, "UPDATE t SET n = n + 1 WHERE id = 1"); }
    { sqlite3_stmt *q; sqlite3_prepare_v2(a, "SELECT pid, length(slots) FROM mw_resv", -1, &q, NULL); while (sqlite3_step(q) == SQLITE_ROW) printf("   resv pid %lld (%lld bytes), child was %d, me %d\n", (long long)sqlite3_column_int64(q, 0), (long long)sqlite3_column_int64(q, 1), (int)c, (int)getpid()); sqlite3_finalize(q); printf("   runs written %llu, merges %llu, runs now %llu\n", (unsigned long long)rs[7], (unsigned long long)rs[5], (unsigned long long)rs[8]); }
    printf("slots found that nobody had after the kill: %llu\n", (unsigned long long)rs[12]);
    char qq[160]; snprintf(qq, sizeof qq, "SELECT count(*) FROM mw_resv WHERE pid = %d", (int)c);
    CHECK(mw_scalar(a, qq) == 0);                                                              // (the entry of the dead process is gone: its slots were given back, however many it had left)
    int64_t sum = mw_scalar(a, "SELECT sum(n) FROM t"); CHECK(sum >= 700);
    sqlite3_close(a); mw_rmdb(path);
    MW_DONE();
}
