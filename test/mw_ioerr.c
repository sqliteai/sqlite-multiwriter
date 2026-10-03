// I/O errors and a full disk (docs/design.md, "Errors of the file system").
//
// The engine's own file calls (commit log, segments, compaction into the database, the maps of the shared mode) go through wrappers that a test can arm: the nth call, from the arm on,
// fails with EIO or ENOSPC, or writes half of its bytes and then the disk is full. For every n up to the number of calls the workload makes, a child process opens a tracked
// database, runs updates (two rows of a counter table per transaction, with the log rewriting, the compaction and the metadata flushes going), and the fault fires wherever that
// n falls. Whatever the child does (it may fail commits, give up on the connection or fail to open it), the parent then opens the database again, with no fault, and checks:
//   - the child was not killed by a signal and did not hang;
//   - every acknowledged transaction is there, and no transaction is half of one (the counters add up in pairs);
//   - the metadata agrees with the rows (the cell version of the counter is 1 + 2 * n for every row);
//   - the integrity check passes, and the database takes more transactions afterwards.
// Thread mode and the multi-process shared mode (one process), three kinds of fault: a single EIO, a disk that fills up (ENOSPC from there on, for writes and extensions), and a torn write.
#include <stdint.h>
#include <errno.h>
#include <signal.h>
#include <sys/wait.h>
#include <time.h>
#include "mw_test.h"
#include "multiwriter.h"
#include "multiwriter_meta.h"
#include "multiwriter_catalog.h"
#include "crdt.h"

static int open_cdc (const char *path, sqlite3 **db, bool shared) {
    char uri[400]; snprintf(uri, sizeof uri, "file:%s?mw=2&mw_cdc=1&mw_log_max_mb=1%s", path, shared ? "&mw_mp=1" : "");
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
static int exec_retry (sqlite3 *db, const char *sql) { for (int i = 0; i < 1000; i++) { int rc = mw_exec(db, sql); if ((rc & 0xff) != SQLITE_BUSY) return rc; } return SQLITE_BUSY; }

#define NROWS 200
static void make_db (const char *path) {
    sqlite3 *s; CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER NOT NULL, s TEXT)"), SQLITE_OK);
    sqlite3_close(s);
    sqlite3 *db; CHECK_RC(open_cdc(path, &db, false), SQLITE_OK);
    CHECK_RC(mw_exec(db, "BEGIN"), SQLITE_OK);
    for (int i = 1; i <= NROWS; i++) { char q[100]; snprintf(q, sizeof q, "INSERT INTO t VALUES(%d, 0, NULL)", i); mw_exec(db, q); }
    CHECK_RC(mw_exec(db, "COMMIT"), SQLITE_OK);
    sqlite3_close(db);
}

// the cell of n of every row must have cv == 1 + 2 * n; returns the rows that disagree, -1 when the metadata cannot be read
static int check_rows (sqlite3 *db, int64_t *sum, int *rows) {
    mw_meta *m = NULL; if (sqlite3_file_control(db, "main", MW_FCNTL_META, &m) != SQLITE_OK) return -1;
    sqlite3_stmt *st; if (sqlite3_prepare_v2(db, "SELECT id, n FROM t", -1, &st, NULL) != SQLITE_OK) return -1;
    int bad = 0; *sum = 0; *rows = 0; uint32_t ncol = mw_name_id("n");
    while (sqlite3_step(st) == SQLITE_ROW) {
        int64_t id = sqlite3_column_int64(st, 0), n = sqlite3_column_int64(st, 1); uint8_t pk[32]; size_t pl; pk_int(id, pk, &pl);
        mw_mcell *c; int nc; mw_meta_row(m, mw_name_id("t"), pk, pl, &c, &nc);
        int64_t cv = -1; for (int i = 0; i < nc; i++) if (c[i].col == ncol) cv = c[i].cv;
        if (cv != 1 + 2 * n) { if (bad < 3) printf("    row %lld: n=%lld cv=%lld (cells %d)\n", (long long)id, (long long)n, (long long)cv, nc); bad++; }
        free(c); *sum += n; (*rows)++;
    }
    sqlite3_finalize(st); return bad;
}

typedef struct { int kinds; int err; int sticky; int shortw; const char *name; } scenario;
#define NTXN 120

// the child: arm the fault, open, run the workload. Reports on `ackfd` every acknowledged transaction, and at the end the number of calls the fault saw.
static void child_run (const char *path, bool shared, const scenario *sc, long nth, int ackfd) {
    alarm(120);                                                       // (a hang is a failure: SIGALRM ends the child)
    setenv("MW_META_FLUSH_ROWS", "7", 1); setenv("MW_META_FLUSH_MS", "5", 1);
    mw_io_fault_arm(sc->kinds, nth, sc->err, sc->sticky, sc->shortw);
    sqlite3 *db = NULL;
    if (open_cdc(path, &db, shared) == SQLITE_OK) {
        unsigned rng = 99u; int failures = 0;
        for (int i = 1; i <= NTXN && failures < 25; i++) {
            char sql[200]; rng = rng * 1103515245u + 12345u; int a = 1 + (int)(rng >> 16) % NROWS; rng = rng * 1103515245u + 12345u; int b = 1 + (int)(rng >> 16) % NROWS; if (a == b) continue;
            snprintf(sql, sizeof sql, "BEGIN; UPDATE t SET n=n+1 WHERE id=%d; UPDATE t SET n=n+1 WHERE id=%d; COMMIT", a, b);
            if (exec_retry(db, sql) == SQLITE_OK) { if (write(ackfd, &i, sizeof i) != sizeof i) _exit(2); }
            else { failures++; mw_exec(db, "ROLLBACK"); }
            if (i % 30 == 0) { mw_compact_result r; sqlite3_file_control(db, "main", MW_FCNTL_COMPACT, &r); }
        }
        sqlite3_close(db);
    }
    long calls = mw_io_fault_calls(); int tag = -1; if (write(ackfd, &tag, sizeof tag) != sizeof tag || write(ackfd, &calls, sizeof calls) != sizeof calls) _exit(2);
    _exit(0);
}

// returns the number of calls the fault saw (from the child's last words), or -1; *acked: acknowledged transactions; *sig: the signal that ended the child (0: none)
static long run_once (const char *path, bool shared, const scenario *sc, long nth, int *acked, int *sig, int *code) {
    int pfd[2]; CHECK(pipe(pfd) == 0);
    pid_t c = fork();
    if (c == 0) { close(pfd[0]); child_run(path, shared, sc, nth, pfd[1]); _exit(3); }
    close(pfd[1]);
    int st; waitpid(c, &st, 0);
    *sig = WIFSIGNALED(st) ? WTERMSIG(st) : 0; *code = WIFEXITED(st) ? WEXITSTATUS(st) : -1;
    *acked = 0; long calls = -1; int v;
    while (read(pfd[0], &v, sizeof v) == sizeof v) { if (v == -1) { if (read(pfd[0], &calls, sizeof calls) != sizeof calls) calls = -1; break; } (*acked)++; }
    close(pfd[0]);
    return calls;
}

// the parent's check, with no fault armed
static void verify (const char *path, bool shared, int acked, const char *what, long nth) {
    sqlite3 *r; int rc = open_cdc(path, &r, shared);
    if (rc != SQLITE_OK) { printf("FAIL %s n=%ld: cannot open the database again (%d)\n", what, nth, rc); mw_failures++; return; }
    int64_t sum; int rows; int bad = check_rows(r, &sum, &rows);
    if (bad != 0 || rows != NROWS || (sum & 1) || sum < 2 * acked) {
        printf("FAIL %s n=%ld: %d rows (want %d), %d disagree with the metadata, sum(n)=%lld, acknowledged %d\n", what, nth, rows, NROWS, bad, (long long)sum, acked);
        mw_failures++;
    }
    if (!integrity_ok(r)) { printf("FAIL %s n=%ld: integrity_check\n", what, nth); mw_failures++; }
    if (exec_retry(r, "UPDATE t SET n = n + 1") != SQLITE_OK) { printf("FAIL %s n=%ld: the recovered database takes no updates\n", what, nth); mw_failures++; }
    sqlite3_close(r);
}

int main (int argc, char **argv) {
    setvbuf(stdout, NULL, _IOLBF, 0);
    int maxiter = argc > 1 ? atoi(argv[1]) : 60;
    long only_n = argc > 3 ? atol(argv[3]) : 0; int only_sc = argc > 2 ? atoi(argv[2]) : -1;      // (to look at one case: mw_ioerr 0 <scenario> <n>; thread mode, or shared with MW_IOERR_SHARED=1)                      // faults tried per scenario and mode
    const scenario sc[] = {
        { MW_IO_ALL,                                 EIO,    0, 0, "a single EIO in any call" },
        { MW_IO_WRITE | MW_IO_TRUNC | MW_IO_MAP,     ENOSPC, 1, 0, "disk full for writes and extensions from the nth call" },
        { MW_IO_WRITE,                               ENOSPC, 0, 1, "torn write, then disk full" },
        { MW_IO_SYNC | MW_IO_RENAME,                 EIO,    1, 0, "fsync and rename fail from the nth call" },
    };
    char path[256];
    if (only_n > 0 && only_sc >= 0) {
        bool shared = getenv("MW_IOERR_SHARED") != NULL;
        mw_tmpdb(path, sizeof path, "ioerr"); make_db(path);
        int acked, sig, code; long calls = run_once(path, shared, &sc[only_sc], only_n, &acked, &sig, &code);
        printf("child: signal %d, code %d, %ld calls, %d acknowledged\n", sig, code, calls, acked);
        verify(path, shared, acked, sc[only_sc].name, only_n);
        return mw_failures ? 1 : 0;
    }
    for (int mode = 0; mode < 2; mode++) {
        bool shared = mode == 1;
        for (size_t s = 0; s < sizeof sc / sizeof sc[0]; s++) {
            mw_tmpdb(path, sizeof path, "ioerr"); make_db(path);
            int acked, sig, code; long total = run_once(path, shared, &sc[s], 1L << 40, &acked, &sig, &code);      // (counting run: the fault never fires)
            CHECK(sig == 0 && code == 0 && total > 0);
            verify(path, shared, acked, "counting run", 0);
            mw_rmdb(path);
            long step = total / maxiter; if (step < 1) step = 1;
            int tried = 0, failed_cases = 0, children_failed = 0;
            for (long n = 1; n <= total; n += (n < 20 ? 1 : step)) {
                mw_tmpdb(path, sizeof path, "ioerr"); make_db(path);
                int before = mw_failures;
                long calls = run_once(path, shared, &sc[s], n, &acked, &sig, &code);
                if (sig != 0 || code != 0 || calls < 0) { printf("FAIL %s, %s, n=%ld: the child %s %d (acknowledged %d)\n", shared ? "shared" : "thread", sc[s].name, n, sig ? "was killed by signal" : "exited with code", sig ? sig : code, acked); mw_failures++; children_failed++; }
                else verify(path, shared, acked, sc[s].name, n);
                if (mw_failures != before) failed_cases++;
                mw_rmdb(path); tried++;
            }
            printf("%-6s mode, %-55s: %ld calls in the workload, %d faults tried, %d failed (%d children)\n", shared ? "shared" : "thread", sc[s].name, total, tried, failed_cases, children_failed);
        }
    }
    MW_DONE();
}
