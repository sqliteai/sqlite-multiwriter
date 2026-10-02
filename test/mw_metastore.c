// The metadata store across restarts and crashes (docs/design.md, "Atomicity and durability of the metadata").
//  1. clean close and reopen: every cell survives (flush at close, read through the file tables);
//  2. SIGKILL under load, flushes very frequent / rare, the log compacting: after recovery every row's cell version agrees with the row's content
//     (a counter that goes up by one per update: cv == 1 + 2 * n), i.e. the metadata is exactly as atomic as the commit that made it;
//  3. a database that is opened again while a flush is racing with writers (several writers, flushes every few rows) stays consistent.
#include <stdint.h>
#include <signal.h>
#include <sys/wait.h>
#include <time.h>
#include <pthread.h>
#include "mw_test.h"
#include "multiwriter.h"
#include "multiwriter_meta.h"
#include "multiwriter_catalog.h"
#include "crdt.h"

static int open_cdc (const char *path, sqlite3 **db, const char *extra) {
    char uri[400]; snprintf(uri, sizeof uri, "file:%s?mw=2&mw_cdc=1%s", path, extra ? extra : "");
    int rc = sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
    if (rc == SQLITE_OK) { sqlite3_extended_result_codes(*db, 1); sqlite3_busy_timeout(*db, 0); }
    return rc;
}
static int integrity_ok (sqlite3 *db) {
    sqlite3_stmt *st; int ok = 0;
    if (sqlite3_prepare_v2(db, "PRAGMA integrity_check", -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW) ok = strcmp((const char *)sqlite3_column_text(st, 0), "ok") == 0;
    sqlite3_finalize(st); return ok;
}
static void pk_int (int64_t id, uint8_t *pk, size_t *n) { crdt_value v = { CRDT_INTEGER, id, 0, NULL, 0 }; *n = crdt_pk_encode(&v, 1, pk, 32); }
static int exec_retry (sqlite3 *db, const char *sql) { for (int i = 0; i < 100000; i++) { int rc = mw_exec(db, sql); if ((rc & 0xff) != SQLITE_BUSY) return rc; } return SQLITE_BUSY; }
static void make_db (const char *path) {
    sqlite3 *s; CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER NOT NULL, s TEXT)"), SQLITE_OK);
    sqlite3_close(s);
}

// every row of t: the cell of n must have cv == 1 + 2 * n; returns the number of rows that disagree (and the sum of n)
static int check_rows (sqlite3 *db, int64_t *sum, int *rows) {
    mw_meta *m = NULL; if (sqlite3_file_control(db, "main", MW_FCNTL_META, &m) != SQLITE_OK) return -1;
    sqlite3_stmt *st; sqlite3_prepare_v2(db, "SELECT id, n FROM t", -1, &st, NULL);
    int bad = 0; *sum = 0; *rows = 0; uint32_t ncol = mw_name_id("n");
    while (sqlite3_step(st) == SQLITE_ROW) {
        int64_t id = sqlite3_column_int64(st, 0), n = sqlite3_column_int64(st, 1); uint8_t pk[32]; size_t pl; pk_int(id, pk, &pl);
        mw_mcell *c; int nc; mw_meta_row(m, mw_name_id("t"), pk, pl, &c, &nc);
        int64_t cv = -1; for (int i = 0; i < nc; i++) if (c[i].col == ncol) cv = c[i].cv;
        if (cv != 1 + 2 * n) { if (bad < 5) printf("    row %lld: n=%lld cv=%lld (cells %d)\n", (long long)id, (long long)n, (long long)cv, nc); bad++; }
        free(c); *sum += n; (*rows)++;
    }
    sqlite3_finalize(st); return bad;
}

typedef struct { const char *path; int id; int nrows; _Atomic int *stop; long done; } wctx;
static void *writer (void *arg) {
    wctx *w = arg; sqlite3 *db; if (open_cdc(w->path, &db, "&mw_log_max_mb=2") != SQLITE_OK) return NULL;
    unsigned rng = 77u + (unsigned)w->id * 1013u;
    while (!atomic_load(w->stop)) {
        char sql[200]; rng = rng * 1103515245u + 12345u; int a = 1 + (int)(rng >> 16) % w->nrows; rng = rng * 1103515245u + 12345u; int b = 1 + (int)(rng >> 16) % w->nrows;
        if (a == b) continue;
        snprintf(sql, sizeof sql, "BEGIN; UPDATE t SET n=n+1 WHERE id=%d; UPDATE t SET n=n+1 WHERE id=%d; COMMIT", a, b);
        int rc = mw_exec(db, sql); if (rc != SQLITE_OK) { mw_exec(db, "ROLLBACK"); continue; }
        w->done++;
    }
    sqlite3_close(db); return NULL;
}

int main (void) {
    setvbuf(stdout, NULL, _IOLBF, 0);
    char path[256];

    // ---- 1. clean close and reopen
    mw_tmpdb(path, sizeof path, "mstore"); make_db(path);
    { sqlite3 *db; CHECK_RC(open_cdc(path, &db, ""), SQLITE_OK);
      CHECK_RC(mw_exec(db, "BEGIN"), SQLITE_OK);
      for (int i = 1; i <= 3000; i++) { char q[120]; snprintf(q, sizeof q, "INSERT INTO t VALUES(%d, 0, 'row%d')", i, i); CHECK_RC(mw_exec(db, q), SQLITE_OK); }
      CHECK_RC(mw_exec(db, "COMMIT"), SQLITE_OK);
      for (int r = 0; r < 4; r++) CHECK_RC(mw_exec(db, "UPDATE t SET n = n + 1 WHERE id % 3 = 0"), SQLITE_OK);
      CHECK_RC(mw_exec(db, "DELETE FROM t WHERE id % 50 = 0"), SQLITE_OK);
      int64_t sum; int rows; int bad = check_rows(db, &sum, &rows); printf("before close: %d rows, %d disagree\n", rows, bad); CHECK(bad == 0);
      sqlite3_close(db); }
    { sqlite3 *db; CHECK_RC(open_cdc(path, &db, ""), SQLITE_OK);
      int64_t cells = mw_scalar(db, "SELECT count(*) FROM mw_cells"), epoch = mw_scalar(db, "SELECT v FROM mw_state WHERE k='meta_epoch'");
      printf("after reopen: mw_cells has %lld cells, meta_epoch %lld\n", (long long)cells, (long long)epoch);
      CHECK(cells > 3000); CHECK(epoch > 1);
      int64_t sum; int rows; int bad = check_rows(db, &sum, &rows); printf("after reopen: %d rows, %d disagree\n", rows, bad); CHECK(bad == 0);
      CHECK(rows == 3000 - 60);
      CHECK(integrity_ok(db)); sqlite3_close(db); }
    mw_rmdb(path);

    // ---- 2. SIGKILL under load
    static const char *ENVS[][2] = { { "1", "1" }, { "7", "5" }, { "50", "30" }, { "100000", "100000" } };      // flush after N rows / M ms: constantly ... practically never
    for (int round = 0; round < 12; round++) {
        mw_tmpdb(path, sizeof path, "mstore"); make_db(path);
        { sqlite3 *db; CHECK_RC(open_cdc(path, &db, ""), SQLITE_OK);
          CHECK_RC(mw_exec(db, "BEGIN"), SQLITE_OK); for (int i = 1; i <= 200; i++) { char q[100]; snprintf(q, sizeof q, "INSERT INTO t VALUES(%d, 0, NULL)", i); mw_exec(db, q); } CHECK_RC(mw_exec(db, "COMMIT"), SQLITE_OK); sqlite3_close(db); }
        int pfd[2]; CHECK(pipe(pfd) == 0);
        pid_t c = fork();
        if (c == 0) {
            setenv("MW_META_FLUSH_ROWS", ENVS[round % 4][0], 1); setenv("MW_META_FLUSH_MS", ENVS[round % 4][1], 1);
            sqlite3 *db; if (open_cdc(path, &db, "&mw_log_max_mb=1") != SQLITE_OK) _exit(1);
            unsigned rng = 5u + (unsigned)round;
            for (int i = 1; i < 10000000; i++) {
                char sql[200]; rng = rng * 1103515245u + 12345u; int a = 1 + (int)(rng >> 16) % 200; rng = rng * 1103515245u + 12345u; int b = 1 + (int)(rng >> 16) % 200; if (a == b) continue;
                snprintf(sql, sizeof sql, "BEGIN; UPDATE t SET n=n+1 WHERE id=%d; UPDATE t SET n=n+1 WHERE id=%d; COMMIT", a, b);
                if (exec_retry(db, sql) == SQLITE_OK) { if (write(pfd[1], &i, sizeof i) != sizeof i) _exit(2); } else mw_exec(db, "ROLLBACK");
            }
            _exit(0);
        }
        close(pfd[1]);
        struct timespec ts = { 0, (long)(100 + (round * 53) % 400) * 1000000L }; nanosleep(&ts, NULL);
        kill(c, SIGKILL); int stt; waitpid(c, &stt, 0);
        int acked = 0, v; while (read(pfd[0], &v, sizeof v) == sizeof v) acked++;
        close(pfd[0]);
        sqlite3 *r; CHECK_RC(open_cdc(path, &r, ""), SQLITE_OK);
        int64_t sum; int rows; int bad = check_rows(r, &sum, &rows);
        printf("SIGKILL round %2d (flush %s rows / %s ms): acknowledged %d commits, sum(n) = %lld, rows %d, cells disagreeing with the rows: %d\n", round, ENVS[round % 4][0], ENVS[round % 4][1], acked, (long long)sum, rows, bad);
        CHECK(bad == 0); CHECK(sum >= 2 * acked); CHECK(rows == 200);
        // writing on top of the recovered state keeps it consistent
        CHECK_RC(exec_retry(r, "UPDATE t SET n = n + 1"), SQLITE_OK);
        bad = check_rows(r, &sum, &rows); CHECK(bad == 0);
        CHECK(integrity_ok(r)); sqlite3_close(r);
        { sqlite3 *r2; CHECK_RC(open_cdc(path, &r2, ""), SQLITE_OK); bad = check_rows(r2, &sum, &rows); CHECK(bad == 0); sqlite3_close(r2); }
        mw_rmdb(path);
    }

    // ---- 3. concurrent writers, flushes racing with them, then everything agrees
    for (int round = 0; round < 3; round++) {
        setenv("MW_META_FLUSH_ROWS", round == 0 ? "1" : "16", 1); setenv("MW_META_FLUSH_MS", "3", 1);
        mw_tmpdb(path, sizeof path, "mstore"); make_db(path);
        sqlite3 *db; CHECK_RC(open_cdc(path, &db, ""), SQLITE_OK);
        CHECK_RC(mw_exec(db, "BEGIN"), SQLITE_OK); for (int i = 1; i <= 100; i++) { char q[100]; snprintf(q, sizeof q, "INSERT INTO t VALUES(%d, 0, NULL)", i); mw_exec(db, q); } CHECK_RC(mw_exec(db, "COMMIT"), SQLITE_OK);
        enum { W = 6 }; pthread_t th[W]; wctx ctx[W]; _Atomic int stop = 0;
        for (int i = 0; i < W; i++) { ctx[i] = (wctx){ path, i, 100, &stop, 0 }; pthread_create(&th[i], NULL, writer, &ctx[i]); }
        struct timespec ts = { 1, 0 }; nanosleep(&ts, NULL); atomic_store(&stop, 1);
        long done = 0; for (int i = 0; i < W; i++) { pthread_join(th[i], NULL); done += ctx[i].done; }
        int64_t sum; int rows; int bad = check_rows(db, &sum, &rows);
        printf("concurrent round %d: %ld commits, sum(n)=%lld (expected %lld), %d rows disagree\n", round, done, (long long)sum, (long long)(2 * done), bad);
        CHECK(bad == 0); CHECK(sum == 2 * done);
        sqlite3_close(db);
        { sqlite3 *r2; CHECK_RC(open_cdc(path, &r2, ""), SQLITE_OK); bad = check_rows(r2, &sum, &rows); CHECK(bad == 0); CHECK(sum == 2 * done); CHECK(integrity_ok(r2)); sqlite3_close(r2); }
        mw_rmdb(path);
    }
    MW_DONE();
}
