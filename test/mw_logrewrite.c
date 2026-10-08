// The rewrite of the log (the compactor keeps only the records newer than its target): the bulk is copied while commits go on, the rest under the sequence lock.
// With a 1 MB limit the log is rewritten all the time; many committers, then SIGKILL at different moments (during a copy, a fsync, the rename): every commit that was
// acknowledged survives, nothing is torn, the file is consistent.
#include <pthread.h>
#include <stdatomic.h>
#include <time.h>
#include <signal.h>
#include <sys/wait.h>
#include "mw_test.h"
#include "multiwriter.h"

static int open_lane (const char *path, sqlite3 **db, const char *extra) {
    char uri[400];
    snprintf(uri, sizeof uri, "file:%s?mw=2&mw_gc=32%s", path, extra ? extra : "");
    int rc = sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
    if (rc == SQLITE_OK) { sqlite3_extended_result_codes(*db, 1); sqlite3_busy_timeout(*db, 0); }
    return rc;
}
static int integrity_ok (sqlite3 *db) {
    sqlite3_stmt *st; int ok = 0;
    if (sqlite3_prepare_v2(db, "PRAGMA integrity_check", -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW) ok = strcmp((const char *)sqlite3_column_text(st, 0), "ok") == 0;
    sqlite3_finalize(st);
    return ok;
}
static void make_db (const char *path) {
    sqlite3 *s;
    CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, MW_PLAIN_VFS), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)"), SQLITE_OK);
    sqlite3_close(s);
}

static const char *g_path;
enum { T = 8, N = 1500 };
static void *worker (void *arg) {
    int id = (int)(intptr_t)arg; sqlite3 *db; if (open_lane(g_path, &db, "&mw_log_max_mb=1") != SQLITE_OK) { mw_failures++; return NULL; }
    for (int n = 0; n < N; n++) {
        char sql[200]; snprintf(sql, sizeof sql, "INSERT INTO t VALUES((%d<<32)+%d, zeroblob(%d))", id, n, 300 + (n * 61) % 6000);
        for (;;) { int rc = mw_exec(db, sql); if (rc == SQLITE_OK) break; if ((rc & 0xff) != SQLITE_BUSY) { printf("unexpected rc %d\n", rc); mw_failures++; return NULL; } }
    }
    sqlite3_close(db);
    return NULL;
}

int main (void) {
    char path[300];
    // ---- 1. many committers while the log is rewritten again and again
    mw_tmpdb(path, sizeof path, "logrw"); make_db(path); g_path = path;
    { sqlite3 *v; CHECK_RC(open_lane(path, &v, "&mw_log_max_mb=1"), SQLITE_OK);
      pthread_t th[T]; for (int i = 0; i < T; i++) pthread_create(&th[i], NULL, worker, (void *)(intptr_t)i);
      for (int i = 0; i < T; i++) pthread_join(th[i], NULL);
      mw_db_stats s; memset(&s, 0, sizeof s); sqlite3_file_control(v, "main", MW_FCNTL_DBSTATS, &s);
      printf("rewrite stress: rows=%lld compactions=%llu\n", (long long)mw_scalar(v, "SELECT count(*) FROM t"), (unsigned long long)s.compactions);
      CHECK(mw_scalar(v, "SELECT count(*) FROM t") == (long)T * N);
      CHECK(mw_scalar(v, "SELECT count(*) FROM t WHERE length(v) < 300") == 0);
      CHECK(s.compactions >= 3);
      CHECK(integrity_ok(v)); sqlite3_close(v); }
    { sqlite3 *v; CHECK_RC(open_lane(path, &v, ""), SQLITE_OK);                           // reopened: the rewritten log replays to the same state
      CHECK(mw_scalar(v, "SELECT count(*) FROM t") == (long)T * N);
      CHECK(integrity_ok(v)); sqlite3_close(v); }
    mw_rmdb(path);

    // ---- 2. SIGKILL at different moments of a run in which the log is rewritten all the time
    for (int round = 0; round < 10; round++) {
        mw_tmpdb(path, sizeof path, "logrw"); make_db(path);
        int pfd[2]; CHECK(pipe(pfd) == 0);
        pid_t c = fork();
        if (c == 0) {
            sqlite3 *w[3]; for (int k = 0; k < 3; k++) if (open_lane(path, &w[k], "&mw_log_max_mb=1") != SQLITE_OK) _exit(1);
            for (int i = 1; i < 1000000; i++) {
                int k = i % 3; char sql[120];
                snprintf(sql, sizeof sql, "INSERT INTO t VALUES(%d, zeroblob(%d))", i, 200 + (i * 37) % 7000);
                if (mw_exec(w[k], sql) == SQLITE_OK) { if (write(pfd[1], &i, sizeof i) != sizeof i) _exit(2); }
            }
            _exit(0);
        }
        close(pfd[1]);
        struct timespec ts = { 0, (long)(80 + round * 53) * 1000000L }; nanosleep(&ts, NULL);
        kill(c, SIGKILL); int st; waitpid(c, &st, 0);
        int last = 0, v; while (read(pfd[0], &v, sizeof v) == sizeof v) last = v;
        close(pfd[0]);
        sqlite3 *r; CHECK_RC(open_lane(path, &r, ""), SQLITE_OK);
        int missing = 0; for (int i = 1; i <= last; i++) { char q[100]; snprintf(q, sizeof q, "SELECT length(v) FROM t WHERE id=%d", i); if (mw_scalar(r, q) != 200 + (i * 37) % 7000) missing++; }
        printf("SIGKILL round %d: acknowledged=%d rows=%lld missing=%d\n", round, last, (long long)mw_scalar(r, "SELECT count(*) FROM t"), missing);
        CHECK(missing == 0);
        CHECK(integrity_ok(r));
        sqlite3_close(r); mw_rmdb(path);
    }
    MW_DONE();
}
