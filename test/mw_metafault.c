// Faults during commits that carry metadata: the write of the log record fails, its sync fails, an allocation fails, the process dies in the middle of the record. Whatever happened, after
// the database is reopened every row's cell agrees with the row (cv == 1 + 2 * n), the commits that were acknowledged are all there, and a failed commit left neither pages nor metadata.
#include <stdint.h>
#include <signal.h>
#include <sys/wait.h>
#include "mw_test.h"
#include "multiwriter.h"
#include "multiwriter_meta.h"
#include "multiwriter_catalog.h"
#include "crdt.h"

static int open_cdc (const char *path, sqlite3 **db) {
    char uri[400]; snprintf(uri, sizeof uri, "file:%s?mw=2&mw_cdc=1%s", path, getenv("MW_TEST_MP") ? "&mw_mp=1" : "");
    int rc = sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
    if (rc == SQLITE_OK) { sqlite3_extended_result_codes(*db, 1); sqlite3_busy_timeout(*db, 0); }
    return rc;
}
static void pk_int (int64_t id, uint8_t *pk, size_t *n) { crdt_value v = { CRDT_INTEGER, id, 0, NULL, 0 }; *n = crdt_pk_encode(&v, 1, pk, 32); }
static int check_rows (sqlite3 *db, int64_t *sum) {
    mw_meta *m = NULL; if (sqlite3_file_control(db, "main", MW_FCNTL_META, &m) != SQLITE_OK) return -1;
    sqlite3_stmt *st; sqlite3_prepare_v2(db, "SELECT id, n FROM t", -1, &st, NULL); int bad = 0; *sum = 0; uint32_t ncol = mw_name_id("n");
    while (sqlite3_step(st) == SQLITE_ROW) {
        int64_t id = sqlite3_column_int64(st, 0), n = sqlite3_column_int64(st, 1); uint8_t pk[32]; size_t pl; pk_int(id, pk, &pl);
        mw_mcell *c; int nc; mw_meta_row(m, mw_name_id("t"), pk, pl, &c, &nc);
        int64_t cv = -1; for (int i = 0; i < nc; i++) if (c[i].col == ncol) cv = c[i].cv;
        if (cv != 1 + 2 * n) bad++;
        free(c); *sum += n;
    }
    sqlite3_finalize(st); return bad;
}

// the child: commits, arms a fault at commit number `at`, goes on; reports how many commits succeeded (acknowledged) through the pipe
static void child (const char *path, mw_fault_t fault, int at, int wfd, bool die) {
    sqlite3 *db; if (open_cdc(path, &db) != SQLITE_OK) _exit(1);
    int acked = 0;
    for (int i = 1; i <= 60; i++) {
        if (i == at) mw_fault_arm(fault, 1);
        char sql[160]; snprintf(sql, sizeof sql, "UPDATE t SET n = n + 1 WHERE id = %d", 1 + i % 20);
        int rc = mw_exec(db, sql);
        if (rc == SQLITE_OK) { acked++; if (write(wfd, &acked, sizeof acked) != sizeof acked) _exit(2); }
        else if (!die && i < at + 3) { mw_fault_disarm_all(); }
    }
    if (die) _exit(9);
    sqlite3_close(db); _exit(0);
}

int main (void) {
    char path[256];
    static const struct { const char *name; mw_fault_t f; } F[] = {
        { "log write error", MW_FAULT_LOG_WRITE_ERR }, { "log sync error", MW_FAULT_LOG_SYNC_ERR }, { "allocation error", MW_FAULT_ALLOC_ERR },
        { "crash in the middle of the record", MW_CRASH_MID_LOG }, { "crash before the record", MW_CRASH_BEFORE_LOG }, { "crash after the record is written", MW_CRASH_AFTER_LOG }, { "crash right after the commit is visible", MW_CRASH_AFTER_VISIBLE } };
    for (size_t k = 0; k < sizeof F / sizeof *F; k++) for (int at = 5; at <= 25; at += 10) {
        mw_tmpdb(path, sizeof path, "mfault");
        { sqlite3 *s; CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), SQLITE_OK);
          CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER NOT NULL)"), SQLITE_OK); sqlite3_close(s); }
        { sqlite3 *db; CHECK_RC(open_cdc(path, &db), SQLITE_OK); CHECK_RC(mw_exec(db, "BEGIN"), SQLITE_OK); for (int i = 1; i <= 20; i++) { char q[80]; snprintf(q, sizeof q, "INSERT INTO t VALUES(%d,0)", i); mw_exec(db, q); } CHECK_RC(mw_exec(db, "COMMIT"), SQLITE_OK); sqlite3_close(db); }
        int pfd[2]; CHECK(pipe(pfd) == 0); bool die = F[k].f >= MW_CRASH_MID_LOG;
        pid_t c = fork();
        if (c == 0) { close(pfd[0]); child(path, F[k].f, at, pfd[1], die); }
        close(pfd[1]); int stt; waitpid(c, &stt, 0);
        int acked = 0, v; while (read(pfd[0], &v, sizeof v) == sizeof v) acked = v; close(pfd[0]);
        sqlite3 *r; CHECK_RC(open_cdc(path, &r), SQLITE_OK);
        int64_t sum; int bad = check_rows(r, &sum);
        printf("%-34s at commit %2d: acknowledged %2d, sum(n) = %lld, cells disagreeing: %d\n", F[k].name, at, acked, (long long)sum, bad);
        CHECK(bad == 0); CHECK(sum >= acked);
        if (F[k].f == MW_FAULT_LOG_SYNC_ERR) CHECK(sum == acked || sum == acked + 1);   // (the record was written and its sync failed: the outcome is unknown to the caller, as with any failed fsync; the metadata agrees either way)
        else if (F[k].f < MW_CRASH_MID_LOG) CHECK(sum == acked);                // (a fault that fails the commit: exactly the acknowledged ones are there)
        CHECK_RC(mw_exec(r, "UPDATE t SET n = n + 1"), SQLITE_OK); bad = check_rows(r, &sum); CHECK(bad == 0);
        sqlite3_close(r); mw_rmdb(path);
    }
    MW_DONE();
}
