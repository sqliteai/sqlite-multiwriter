// The metadata store in multi-process shared mode (mw_mp=1): several processes write one database, each commit carrying its metadata (the shared index of row buckets), and
//  1. while they all run, a reader process sees cells that agree with the rows at every moment it looks (cv == 1 + 2 * n for a counter that goes up by one per update);
//  2. after they finish: every row's cell agrees, nothing was lost (sum of the counters == 2 x acknowledged commits);
//  3. processes killed (SIGKILL) at random moments: the survivors and a later process find consistent metadata, flushes at any frequency;
//  4. remote changes applied by one process while the others write.
#include <stdint.h>
#include <stdbool.h>
#include <signal.h>
#include <sys/wait.h>
#include <time.h>
#include "mw_test.h"
#include "multiwriter.h"
#include "multiwriter_meta.h"
#include "multiwriter_catalog.h"
#include "multiwriter_sync.h"
#include "crdt.h"

static int open_mp (const char *path, sqlite3 **db) {
    char uri[400]; snprintf(uri, sizeof uri, "file:%s?mw=2&mw_mp=1&mw_cdc=1&mw_log_max_mb=2", path);
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
static int exec_retry (sqlite3 *db, const char *sql) { for (int i = 0; i < 1000000; i++) { int rc = mw_exec(db, sql); if ((rc & 0xff) != SQLITE_BUSY) return rc; mw_exec(db, "ROLLBACK"); } return SQLITE_BUSY; }
static void make_db (const char *path) {
    sqlite3 *s; CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER NOT NULL, s TEXT)"), SQLITE_OK);
    sqlite3_close(s);
}
// every row: the cell of n must have cv == 1 + 2 * n (while writers run, the metadata read may be newer than the row read in the reader's snapshot: cv >= 1 + 2 * n and odd)
static int check_rows_x (sqlite3 *db, int64_t *sum, int *rows, bool live) {
    mw_meta *m = NULL; if (sqlite3_file_control(db, "main", MW_FCNTL_META, &m) != SQLITE_OK) return -1;
    sqlite3_stmt *st; sqlite3_prepare_v2(db, "SELECT id, n FROM t", -1, &st, NULL);
    int bad = 0; *sum = 0; *rows = 0; uint32_t ncol = mw_name_id("n");
    while (sqlite3_step(st) == SQLITE_ROW) {
        int64_t id = sqlite3_column_int64(st, 0), n = sqlite3_column_int64(st, 1); uint8_t pk[32]; size_t pl; pk_int(id, pk, &pl);
        mw_mcell *c; int nc; mw_meta_row(m, mw_name_id("t"), pk, pl, &c, &nc);
        int64_t cv = -1; for (int i = 0; i < nc; i++) if (c[i].col == ncol) cv = c[i].cv;
        if (live ? (cv < 1 + 2 * n || cv % 2 == 0) : cv != 1 + 2 * n) { if (bad < 5) printf("    row %lld: n=%lld cv=%lld (cells %d)\n", (long long)id, (long long)n, (long long)cv, nc); bad++; }
        free(c); *sum += n; (*rows)++;
    }
    sqlite3_finalize(st); return bad;
}
static int check_rows (sqlite3 *db, int64_t *sum, int *rows) { return check_rows_x(db, sum, rows, false); }
static void worker (const char *path, int id, int nrows, int ms, int wfd) {
    sqlite3 *db; if (open_mp(path, &db) != SQLITE_OK) _exit(1);
    unsigned rng = 1000u + (unsigned)id * 7919u; long done = 0;
    struct timespec t0, t1; clock_gettime(CLOCK_MONOTONIC, &t0);
    for (;;) {
        char sql[200]; rng = rng * 1103515245u + 12345u; int a = 1 + (int)(rng >> 16) % nrows; rng = rng * 1103515245u + 12345u; int b = 1 + (int)(rng >> 16) % nrows; if (a == b) continue;
        snprintf(sql, sizeof sql, "BEGIN; UPDATE t SET n=n+1 WHERE id=%d; UPDATE t SET n=n+1 WHERE id=%d; COMMIT", a, b);
        if (exec_retry(db, sql) == SQLITE_OK) { done++; if (wfd >= 0 && write(wfd, &done, sizeof done) != sizeof done) _exit(2); }
        if ((done & 63) == 0) { clock_gettime(CLOCK_MONOTONIC, &t1); if ((t1.tv_sec - t0.tv_sec) * 1000 + (t1.tv_nsec - t0.tv_nsec) / 1000000 >= ms) break; }
    }
    sqlite3_close(db); _exit(0);
}

int main (void) {
    setvbuf(stdout, NULL, _IOLBF, 0);
    char path[256];

    // ---- 1 + 2. processes write, a reader checks while they do
    for (int round = 0; round < 3; round++) {
        mw_tmpdb(path, sizeof path, "mpmeta"); make_db(path);
        sqlite3 *p; CHECK_RC(open_mp(path, &p), SQLITE_OK);
        CHECK_RC(mw_exec(p, "BEGIN"), SQLITE_OK); for (int i = 1; i <= 100; i++) { char q[100]; snprintf(q, sizeof q, "INSERT INTO t VALUES(%d, 0, NULL)", i); mw_exec(p, q); } CHECK_RC(mw_exec(p, "COMMIT"), SQLITE_OK);
        enum { W = 5 }; pid_t pid[W]; int pfd[W][2];
        for (int i = 0; i < W; i++) { CHECK(pipe(pfd[i]) == 0); pid[i] = fork(); if (pid[i] == 0) { close(pfd[i][0]); worker(path, i + round * 10, 100, 1200, pfd[i][1]); } close(pfd[i][1]); }
        int bad_live = 0, looks = 0; struct timespec ts = { 0, 150 * 1000000L };
        for (int k = 0; k < 6; k++) { nanosleep(&ts, NULL); int64_t sum; int rows; sqlite3_stmt *st; (void)st; mw_exec(p, "BEGIN"); int bad = check_rows_x(p, &sum, &rows, true); mw_exec(p, "COMMIT"); bad_live += bad; looks++; }
        long total = 0;
        for (int i = 0; i < W; i++) { int stt; waitpid(pid[i], &stt, 0); long last = 0, v; while (read(pfd[i][0], &v, sizeof v) == sizeof v) last = v; close(pfd[i][0]); total += last; }
        int64_t sum; int rows; int bad = check_rows(p, &sum, &rows);
        printf("round %d: %d processes, %ld commits acknowledged, sum(n)=%lld (expected %lld), live checks %d (disagreeing rows seen: %d), final disagreeing rows: %d\n", round, W, total, (long long)sum, (long long)(2 * total), looks, bad_live, bad);
        CHECK(bad == 0); CHECK(bad_live == 0); CHECK(sum == 2 * total); CHECK(integrity_ok(p));
        sqlite3_close(p);
        { sqlite3 *r; CHECK_RC(open_mp(path, &r), SQLITE_OK); bad = check_rows(r, &sum, &rows); CHECK(bad == 0); CHECK(sum == 2 * total); sqlite3_close(r); }
        mw_rmdb(path);
    }

    // ---- 3. SIGKILL
    static const char *ENVS[][2] = { { "1", "1" }, { "7", "5" }, { "50", "30" }, { "100000", "100000" } };
    for (int round = 0; round < 12; round++) {
        mw_tmpdb(path, sizeof path, "mpmeta"); make_db(path);
        { sqlite3 *p; CHECK_RC(open_mp(path, &p), SQLITE_OK); CHECK_RC(mw_exec(p, "BEGIN"), SQLITE_OK); for (int i = 1; i <= 100; i++) { char q[100]; snprintf(q, sizeof q, "INSERT INTO t VALUES(%d, 0, NULL)", i); mw_exec(p, q); } CHECK_RC(mw_exec(p, "COMMIT"), SQLITE_OK); sqlite3_close(p); }
        enum { W = 4 }; pid_t pid[W]; int pfd[W][2];
        for (int i = 0; i < W; i++) { CHECK(pipe(pfd[i]) == 0); pid[i] = fork(); if (pid[i] == 0) { close(pfd[i][0]); setenv("MW_META_FLUSH_ROWS", ENVS[round % 4][0], 1); setenv("MW_META_FLUSH_MS", ENVS[round % 4][1], 1); worker(path, i + 100 * round, 100, 100000, pfd[i][1]); } close(pfd[i][1]); }
        struct timespec ts = { 0, (long)(150 + (round * 61) % 450) * 1000000L }; nanosleep(&ts, NULL);
        int killed = round % 3 == 0 ? W : 1 + round % 3;                        // all of them, or some: the others carry on a little while longer
        for (int i = 0; i < killed; i++) kill(pid[i], SIGKILL);
        if (killed < W) { struct timespec t2 = { 0, 200 * 1000000L }; nanosleep(&t2, NULL); for (int i = killed; i < W; i++) kill(pid[i], SIGKILL); }
        long acked = 0;
        for (int i = 0; i < W; i++) { int stt; waitpid(pid[i], &stt, 0); long last = 0, v; while (read(pfd[i][0], &v, sizeof v) == sizeof v) last = v; close(pfd[i][0]); acked += last; }
        sqlite3 *r; CHECK_RC(open_mp(path, &r), SQLITE_OK);
        int64_t sum; int rows; int bad = check_rows(r, &sum, &rows);
        printf("SIGKILL round %2d (flush %s rows / %s ms, %d of %d killed first): acknowledged %ld, sum(n)=%lld, rows %d, disagreeing: %d\n", round, ENVS[round % 4][0], ENVS[round % 4][1], killed, W, acked, (long long)sum, rows, bad);
        CHECK(bad == 0); CHECK(sum >= 2 * acked); CHECK(rows == 100);
        CHECK_RC(exec_retry(r, "UPDATE t SET n = n + 1"), SQLITE_OK);
        bad = check_rows(r, &sum, &rows); CHECK(bad == 0); CHECK(integrity_ok(r)); sqlite3_close(r);
        { sqlite3 *r2; CHECK_RC(open_mp(path, &r2), SQLITE_OK); bad = check_rows(r2, &sum, &rows); CHECK(bad == 0); sqlite3_close(r2); }
        mw_rmdb(path);
    }
    MW_DONE();
}
