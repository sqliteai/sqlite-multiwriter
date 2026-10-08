// Multi-process: every process is SIGKILLed at once under load (recovery from the shared log), and a schema
// change from one process while the others keep writing.
#include <pthread.h>
#include <stdatomic.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include "mw_test.h"
#include "multiwriter.h"

static int open_mp (const char *path, sqlite3 **db) {
    char uri[400];
    snprintf(uri, sizeof uri, "file:%s?mw=2&mw_mp=1&mw_gc=32&mw_compact_ms=5", path);
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
#define NPROC 4
#define NTHR 4
static _Atomic long *acks;            // shared across fork(): one counter per (process, thread), bumped after each COMMIT
typedef struct { const char *path; int proc, thr, mode; } job_t;

static void *worker (void *arg) {
    job_t *j = arg;
    sqlite3 *db; if (open_mp(j->path, &db) != SQLITE_OK) return NULL;
    int row = 1 + j->proc * NTHR + j->thr;
    while (!atomic_load(&acks[NPROC * NTHR])) {
        char sql[160];
        if (j->mode == 0) snprintf(sql, sizeof sql, "UPDATE t SET v=v+1 WHERE id=%d", row);
        else snprintf(sql, sizeof sql, "INSERT INTO items VALUES(%d, %ld)", row * 1000000 + (int)atomic_load(&acks[j->proc * NTHR + j->thr]), 1L);
        int rc = mw_exec(db, sql);
        if (rc == SQLITE_OK) atomic_fetch_add(&acks[j->proc * NTHR + j->thr], 1);
        else if ((rc & 0xff) != SQLITE_BUSY) { if (j->mode == 0) return NULL; }
    }
    return NULL;
}
static void child (const char *path, int proc, int mode) {
    pthread_t th[NTHR]; job_t jobs[NTHR];
    for (int t = 0; t < NTHR; t++) { jobs[t] = (job_t){ path, proc, t, mode }; pthread_create(&th[t], NULL, worker, &jobs[t]); }
    for (int t = 0; t < NTHR; t++) pthread_join(th[t], NULL);
    _exit(0);
}

int main (void) {
    char path[256], lp[300], mp[300];
    mw_tmpdb(path, sizeof path, "mpc"); snprintf(lp, sizeof lp, "%s-mw", path); snprintf(mp, sizeof mp, "%s-mwlock", path);
    unlink(lp); unlink(mp);
    sqlite3 *s;
    CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, MW_PLAIN_VFS), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER, pad BLOB);"
                        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<64) INSERT INTO t SELECT i, 0, zeroblob(3000) FROM n;"
                        "CREATE TABLE items(id INTEGER PRIMARY KEY, n INTEGER); INSERT INTO items VALUES(1,1);"), SQLITE_OK);
    sqlite3_close(s);
    acks = mmap(NULL, sizeof(long) * (NPROC * NTHR + 1), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);
    memset((void *)acks, 0, sizeof(long) * NPROC * NTHR);

    // ---- kill everything at once, several times over the same database (each round recovers from the log)
    long total_prev = 0;
    for (int round = 0; round < 4; round++) {
        pid_t pid[NPROC];
        for (int p = 0; p < NPROC; p++) { pid[p] = fork(); if (pid[p] == 0) child(path, p, 0); }
        struct timespec ts = { 0, (long)(120 + round * 90) * 1000000L }; nanosleep(&ts, NULL);
        for (int p = 0; p < NPROC; p++) kill(pid[p], SIGKILL);
        for (int p = 0; p < NPROC; p++) { int st; waitpid(pid[p], &st, 0); }
        sqlite3 *a; CHECK_RC(open_mp(path, &a), SQLITE_OK);                    // this process recovers the database
        long total = 0; int bad = 0;
        for (int p = 0; p < NPROC * NTHR; p++) {
            char q[80]; snprintf(q, sizeof q, "SELECT v FROM t WHERE id=%d", 1 + p);
            int64_t v = mw_scalar(a, q);
            long ack = atomic_load(&acks[p]);
            if (v < ack || v > ack + 1) { bad++; printf("  row %d: v=%lld acked=%ld\n", 1 + p, (long long)v, ack); }   // every acknowledged commit survives; at most one in flight
            total += v;
        }
        printf("kill-all round %d: %ld committed updates recovered (acked %ld), bad rows %d\n", round, total, total_prev, bad);
        CHECK(bad == 0);
        CHECK(integrity_ok(a));
        // the survivors' acks continue from the recovered values: realign the counters
        for (int p = 0; p < NPROC * NTHR; p++) { char q[80]; snprintf(q, sizeof q, "SELECT v FROM t WHERE id=%d", 1 + p); atomic_store(&acks[p], (long)mw_scalar(a, q)); }
        total_prev = total;
        sqlite3_close(a);
    }
    { char uri[300]; snprintf(uri, sizeof uri, "file:%s?immutable=1", path); sqlite3 *c; CHECK_RC(sqlite3_open_v2(uri, &c, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, MW_PLAIN_VFS), SQLITE_OK);
      CHECK(mw_scalar(c, "SELECT sum(v) FROM t") == total_prev); CHECK(integrity_ok(c)); sqlite3_close(c); }

    // ---- DDL from this process while three others insert: writers see retryable errors and continue; the index is complete
    memset((void *)acks, 0, sizeof(long) * (NPROC * NTHR + 1));
    pid_t pid[3];
    for (int p = 0; p < 3; p++) { pid[p] = fork(); if (pid[p] == 0) child(path, p, 1); }
    struct timespec ts = { 0, 150 * 1000000L }; nanosleep(&ts, NULL);
    sqlite3 *a; CHECK_RC(open_mp(path, &a), SQLITE_OK);
    sqlite3_busy_timeout(a, 3000);
    int ddl = SQLITE_BUSY;
    if (!getenv("MP_NODDL")) {
        for (int attempt = 0; attempt < 200 && (ddl & 0xff) == SQLITE_BUSY; attempt++) ddl = mw_exec(a, "CREATE INDEX items_n ON items(n)");
        if (ddl != SQLITE_OK) printf("CREATE INDEX failed rc=%d ext=%d: %s\n", ddl, sqlite3_extended_errcode(a), sqlite3_errmsg(a));
        CHECK_RC(ddl, SQLITE_OK);
    }
    nanosleep(&ts, NULL);
    if (getenv("MP_GRACEFUL")) { atomic_store(&acks[NPROC * NTHR], 1); }
    else for (int p = 0; p < 3; p++) kill(pid[p], SIGKILL);
    for (int p = 0; p < 3; p++) { int st; waitpid(pid[p], &st, 0); }
    long inserted = 1;
    for (int p = 0; p < 3 * NTHR; p++) inserted += atomic_load(&acks[p]);
    sqlite3_close(a);
    CHECK_RC(open_mp(path, &a), SQLITE_OK);
    int64_t rows = mw_scalar(a, "SELECT count(*) FROM items");
    if (rows < 0) { sqlite3_stmt *q; if (sqlite3_prepare_v2(a, "PRAGMA integrity_check", -1, &q, NULL) == SQLITE_OK) { int n = 0; while (sqlite3_step(q) == SQLITE_ROW && n++ < 8) printf("  integrity: %s\n", sqlite3_column_text(q, 0)); sqlite3_finalize(q); }
     printf("  page_count=%lld\n", (long long)mw_scalar(a, "PRAGMA page_count")); char *e = NULL; int rc = sqlite3_exec(a, "SELECT count(*) FROM items", NULL, NULL, &e); printf("final count failed: rc=%d %s | %s\n", rc, e ? e : "", sqlite3_errmsg(a)); sqlite3_free(e); }
    printf("DDL under load: %ld acked inserts, %lld rows, index rows %lld\n", inserted, (long long)rows, (long long)mw_scalar(a, "SELECT count(*) FROM items INDEXED BY items_n"));
    CHECK(rows >= inserted && rows <= inserted + 3 * NTHR);                    // acked survive; at most one in flight per thread
    if (!getenv("MP_NODDL")) CHECK(mw_scalar(a, "SELECT count(*) FROM items INDEXED BY items_n") == rows);
    CHECK(integrity_ok(a));
    sqlite3_close(a);
    mw_rmdb(path); unlink(lp); unlink(mp);
    MW_DONE();
}
