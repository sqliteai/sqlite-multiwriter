// Leader-batched log writes (staged mode, synchronous=FULL): records larger than the staging ring, mixed synchronous levels, wrap-around of the ring under
// concurrent committers, and durability of acknowledged commits across SIGKILL.
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
    CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB); CREATE TABLE c(id INTEGER PRIMARY KEY, n INTEGER); INSERT INTO c VALUES(1,0)"), SQLITE_OK);
    sqlite3_close(s);
}

static const char *g_path;
enum { T = 8, N = 250 };
static _Atomic long g_pages[T];
static void *worker (void *arg) {
    int id = (int)(intptr_t)arg; sqlite3 *db; if (open_lane(g_path, &db, "&mw_log_max_mb=512") != SQLITE_OK) return NULL;   // (no compaction: offsets keep growing past the ring size)
    unsigned rng = 99u + (unsigned)id * 7919u;
    for (int n = 0; n < N; n++) {
        rng = rng * 1103515245u + 12345u; int len = 200 + (int)(rng >> 16) % 30000;          // 1 to ~8 pages per commit, some with overflow chains
        char sql[200]; snprintf(sql, sizeof sql, "INSERT INTO t VALUES((%d<<32)+%d, zeroblob(%d))", id, n, len);
        for (;;) { int rc = mw_exec(db, sql); if (rc == SQLITE_OK) break; if ((rc & 0xff) != SQLITE_BUSY) { printf("unexpected rc %d\n", rc); mw_failures++; return NULL; } }
        atomic_fetch_add(&g_pages[id], 1);
    }
    sqlite3_close(db); return NULL;
}

int main (void) {
    setvbuf(stdout, NULL, _IOLBF, 0);
    char path[256];

    // ---- 1. records larger than the ring's quarter (2 MB): written to the file directly, in order with the staged ones
    mw_tmpdb(path, sizeof path, "staged"); make_db(path); g_path = path;
    { sqlite3 *a, *b; CHECK_RC(open_lane(path, &a, ""), SQLITE_OK); CHECK_RC(open_lane(path, &b, ""), SQLITE_OK);
      CHECK_RC(mw_exec(a, "INSERT INTO t VALUES(1, zeroblob(300))"), SQLITE_OK);
      CHECK_RC(mw_exec(a, "INSERT INTO t VALUES(2, zeroblob(6000000))"), SQLITE_OK);           // ~1500 pages in one record
      CHECK_RC(mw_exec(b, "INSERT INTO t VALUES(3, zeroblob(300))"), SQLITE_OK);
      CHECK_RC(mw_exec(b, "INSERT INTO t VALUES(4, zeroblob(9000000))"), SQLITE_OK);
      CHECK_RC(mw_exec(a, "INSERT INTO t VALUES(5, zeroblob(300))"), SQLITE_OK);
      CHECK(mw_scalar(b, "SELECT sum(length(v)) FROM t") == 300 + 6000000 + 300 + 9000000 + 300);
      CHECK(integrity_ok(a));
      sqlite3_close(a); sqlite3_close(b); }
    { sqlite3 *v; CHECK_RC(open_lane(path, &v, ""), SQLITE_OK);                           // reopened: recovered/compacted state is complete
      CHECK(mw_scalar(v, "SELECT sum(length(v)) FROM t") == 300 + 6000000 + 300 + 9000000 + 300);
      CHECK(integrity_ok(v)); sqlite3_close(v); }
    mw_rmdb(path);

    // ---- 2. many committers, log offsets far beyond the 8 MB ring (it wraps many times); every row present with its exact length
    mw_tmpdb(path, sizeof path, "staged"); make_db(path); g_path = path;
    { sqlite3 *v; CHECK_RC(open_lane(path, &v, "&mw_log_max_mb=512"), SQLITE_OK);
      pthread_t th[T]; for (int i = 0; i < T; i++) pthread_create(&th[i], NULL, worker, (void *)(intptr_t)i);
      for (int i = 0; i < T; i++) pthread_join(th[i], NULL);
      mw_db_stats s; memset(&s, 0, sizeof s); sqlite3_file_control(v, "main", MW_FCNTL_DBSTATS, &s);
      printf("staged stress: rows=%lld log_bytes=%llu syncs=%llu commits=%llu\n", (long long)mw_scalar(v, "SELECT count(*) FROM t"), (unsigned long long)s.log_bytes, (unsigned long long)s.log_syncs, (unsigned long long)s.commits);
      CHECK(mw_scalar(v, "SELECT count(*) FROM t") == (long)T * N);
      CHECK(s.log_bytes > 3 * (8ull << 20));                           // the ring wrapped several times
      CHECK(s.log_syncs < s.commits);                                   // group commit: fewer syncs than commits
      CHECK(mw_scalar(v, "SELECT count(*) FROM t WHERE length(v) < 200") == 0);
      CHECK(integrity_ok(v)); sqlite3_close(v); }
    mw_rmdb(path);

    // ---- 3. SIGKILL under load with mixed synchronous levels: every commit acknowledged by a FULL connection survives; the file is never left inconsistent
    for (int round = 0; round < 4; round++) {
        mw_tmpdb(path, sizeof path, "staged"); make_db(path);
        int pfd[2]; CHECK(pipe(pfd) == 0);
        pid_t c = fork();
        if (c == 0) {
            sqlite3 *full, *off;
            if (open_lane(path, &full, "") != SQLITE_OK || open_lane(path, &off, "") != SQLITE_OK) _exit(1);
            mw_exec(off, "PRAGMA synchronous=OFF");
            for (int i = 1; i < 100000; i++) {
                char sql[120];
                snprintf(sql, sizeof sql, "INSERT INTO t VALUES(%d, zeroblob(%d))", i * 2, 100 + (i * 37) % 9000);
                if (mw_exec(full, sql) == SQLITE_OK) { if (write(pfd[1], &i, sizeof i) != sizeof i) _exit(2); }        // acknowledged (FULL)
                snprintf(sql, sizeof sql, "INSERT INTO t VALUES(%d, zeroblob(500))", i * 2 + 1);
                mw_exec(off, sql);
            }
            _exit(0);
        }
        close(pfd[1]);                                                                              // (so that read() sees end-of-file once the child is dead)
        struct timespec ts = { 0, (long)(60 + round * 45) * 1000000L }; nanosleep(&ts, NULL);
        kill(c, SIGKILL); int st; waitpid(c, &st, 0);
        int last = 0, v; while (read(pfd[0], &v, sizeof v) == sizeof v) last = v;                  // (the pipe is drained after the child is dead)
        close(pfd[0]);
        sqlite3 *r; CHECK_RC(open_lane(path, &r, ""), SQLITE_OK);
        int64_t have = mw_scalar(r, "SELECT count(*) FROM t WHERE id % 2 = 0");                     // FULL rows
        int missing = 0; for (int i = 1; i <= last; i++) { char q[100]; snprintf(q, sizeof q, "SELECT length(v) FROM t WHERE id=%d", i * 2); if (mw_scalar(r, q) != 100 + (i * 37) % 9000) missing++; }
        printf("SIGKILL round %d: acknowledged FULL commits=%d, FULL rows recovered=%lld, missing=%d\n", round, last, (long long)have, missing);
        CHECK(missing == 0);
        CHECK(have >= last);
        CHECK(integrity_ok(r));
        sqlite3_close(r); mw_rmdb(path);
    }
    MW_DONE();
}
