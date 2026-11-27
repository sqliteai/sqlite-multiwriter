// Speculative snapshots (one process). A write transaction reads at the newest installed epoch, which may include commits whose record is not durable yet; a reader that does not write
// reads only what is durable. Built with multiwriter_log.c compiled with -Dfsync=inject_fsync, so that the fsync of the log can be held or slowed.
//   1. While the fsync of commit A is held: a plain reader does not see A; a writer sees it (its rowid comes after A's) and is acknowledged only after the fsync; a write statement that
//      changes nothing (it read A) ends only after the fsync too.
//   2. A commit whose record cannot be written is taken back: a writer that read it (speculatively) is refused, and its retry commits on the state without it.
//   4. A connection opened with mw_spec=0 does not read the commit whose fsync is held: its insert takes the same rowid as that commit.
//   3. Appends with SQLite's own rowids from several threads (each depends on the one before): every row is there once, and the commits share the fsyncs of the log (group commit).
#include <pthread.h>
#include <stdatomic.h>
#include <time.h>
#include "mw_test.h"
#include "multiwriter.h"

static pthread_mutex_t mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t cv = PTHREAD_COND_INITIALIZER;
static int armed, entered, released, slow_us;
int inject_fsync (int fd) {
    pthread_mutex_lock(&mu);
    if (armed) { armed = 0; entered = 1; pthread_cond_broadcast(&cv); while (!released) pthread_cond_wait(&cv, &mu); }
    int us = slow_us;
    pthread_mutex_unlock(&mu);
    if (us) usleep((useconds_t)us);
    return fsync(fd);
}

static char g_path[300];
static sqlite3 *open_mw_uri (const char *extra) {
    char uri[400]; sqlite3 *db = NULL;
    snprintf(uri, sizeof uri, "file:%s?vfs=multiwriter%s", g_path, extra);
    if (sqlite3_open_v2(uri, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL) != SQLITE_OK) { printf("FAIL open\n"); exit(1); }
    sqlite3_extended_result_codes(db, 1);
    sqlite3_busy_timeout(db, 0);
    return db;
}
static sqlite3 *open_mw (void) { return open_mw_uri(""); }
static void fresh (const char *tag) {
    mw_tmpdb(g_path, sizeof g_path, tag);
    sqlite3 *s; CHECK_RC(sqlite3_open_v2(g_path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, MW_PLAIN_VFS), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t(v) VALUES ('seed')"), SQLITE_OK);
    sqlite3_close(s);
}
static uint64_t now_ms (void) { struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts); return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000; }

typedef struct { sqlite3 *db; const char *sql; int rc; int64_t rowid; _Atomic int done; } job_t;
static void *run_job (void *a) {
    job_t *j = a;
    j->rc = mw_exec(j->db, j->sql);
    j->rowid = sqlite3_last_insert_rowid(j->db);
    atomic_store(&j->done, 1);
    return NULL;
}

static void case_held_fsync (void) {
    fresh("spec1");
    sqlite3 *a = open_mw(), *r = open_mw(), *b = open_mw(), *c = open_mw();
    CHECK(mw_scalar(r, "SELECT count(*) FROM t") == 1);                       // (every connection has the database open: the store exists)
    CHECK(mw_scalar(b, "SELECT count(*) FROM t") == 1);
    CHECK(mw_scalar(c, "SELECT count(*) FROM t") == 1);
    pthread_mutex_lock(&mu); armed = 1; entered = 0; released = 0; pthread_mutex_unlock(&mu);
    job_t ja = { a, "INSERT INTO t(v) VALUES ('a')" };
    pthread_t ta; pthread_create(&ta, NULL, run_job, &ja);
    pthread_mutex_lock(&mu); while (!entered) pthread_cond_wait(&cv, &mu); pthread_mutex_unlock(&mu);
    // A is installed, its fsync is held
    CHECK(mw_scalar(r, "SELECT count(*) FROM t") == 1);                       // a reader sees only what is durable
    CHECK(mw_scalar(r, "SELECT count(*) FROM t WHERE v = 'a'") == 0);
    job_t jb = { b, "INSERT INTO t(v) VALUES ('b')" }, jc = { c, "UPDATE t SET v = 'never' WHERE id = -5" };
    pthread_t tb, tc; pthread_create(&tb, NULL, run_job, &jb); pthread_create(&tc, NULL, run_job, &jc);
    usleep(150000);
    CHECK(!atomic_load(&ja.done));
    CHECK(!atomic_load(&jb.done));                                            // B committed on top of A: acknowledged only when durable
    CHECK(!atomic_load(&jc.done));                                            // C changed nothing, and it read A: it ends when A is durable
    CHECK(mw_scalar(r, "SELECT count(*) FROM t") == 1);
    pthread_mutex_lock(&mu); released = 1; pthread_cond_broadcast(&cv); pthread_mutex_unlock(&mu);
    pthread_join(ta, NULL); pthread_join(tb, NULL); pthread_join(tc, NULL);
    CHECK_RC(ja.rc, SQLITE_OK); CHECK_RC(jb.rc, SQLITE_OK); CHECK_RC(jc.rc, SQLITE_OK);
    CHECK(jb.rowid == ja.rowid + 1);                                          // B's rowid was taken after A's: it read A before A was durable
    CHECK(mw_scalar(r, "SELECT count(*) FROM t") == 3);
    sqlite3_close(a); sqlite3_close(b); sqlite3_close(c); sqlite3_close(r);
    mw_rmdb(g_path);
}

static void case_taken_back (void) {
    fresh("spec2");
    setenv("MW_FAULT_DELAY_US", "400000", 1);                                 // the failing append of A waits 400 ms before it fails: B reads A meanwhile
    sqlite3 *a = open_mw(), *b = open_mw();
    CHECK(mw_scalar(b, "SELECT count(*) FROM t") == 1);
    mw_fault_arm(MW_FAULT_LOG_WRITE_ERR, 1);
    job_t ja = { a, "INSERT INTO t(v) VALUES ('a')" };
    pthread_t ta; pthread_create(&ta, NULL, run_job, &ja);
    usleep(100000);
    CHECK_RC(mw_exec(b, "BEGIN"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "INSERT INTO t(v) VALUES ('b')"), SQLITE_OK);
    int64_t spec_id = sqlite3_last_insert_rowid(b);
    pthread_join(ta, NULL);
    CHECK(ja.rc != SQLITE_OK);                                                // A's record could not be written: A was taken back
    CHECK(spec_id == 3);                                                      // B read A (seed = 1, A = 2)
    int rc = mw_exec(b, "COMMIT");
    CHECK(rc != SQLITE_OK);                                                   // what B built on is gone: refused
    mw_exec(b, "ROLLBACK");
    for (int i = 0; i < 100 && (rc = mw_exec(b, "INSERT INTO t(v) VALUES ('b')")) != SQLITE_OK; i++) usleep(10000);
    CHECK_RC(rc, SQLITE_OK);
    CHECK(sqlite3_last_insert_rowid(b) == 2);
    CHECK(mw_scalar(b, "SELECT count(*) FROM t WHERE v = 'a'") == 0);
    CHECK(mw_scalar(b, "SELECT count(*) FROM t") == 2);
    sqlite3_close(a); sqlite3_close(b);
    unsetenv("MW_FAULT_DELAY_US");
    char uri[400]; snprintf(uri, sizeof uri, "file:%s?vfs=multiwriter", g_path);
    sqlite3 *v; CHECK_RC(sqlite3_open_v2(uri, &v, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL), SQLITE_OK);
    CHECK(mw_scalar(v, "SELECT count(*) FROM t") == 2);
    CHECK(mw_scalar(v, "SELECT count(*) FROM t WHERE v = 'a'") == 0);
    sqlite3_close(v);
    mw_rmdb(g_path);
}

enum { NT = 8, PER = 40, ROWS = 10 };
static _Atomic int g_ok, g_retries;
static void *appender (void *arg) {
    long id = (long)arg; sqlite3 *db = open_mw(); sqlite3_stmt *st;
    sqlite3_prepare_v2(db, "INSERT INTO t(v) VALUES (?1)", -1, &st, NULL);
    for (int k = 0; k < PER; k++) {
        for (;;) {
            int rc = mw_exec(db, "BEGIN");
            for (int i = 0; rc == SQLITE_OK && i < ROWS; i++) {
                char v[40]; snprintf(v, sizeof v, "%ld:%d:%d", id, k, i);
                sqlite3_bind_text(st, 1, v, -1, SQLITE_TRANSIENT);
                rc = sqlite3_step(st); sqlite3_reset(st);
                if (rc == SQLITE_DONE) rc = SQLITE_OK;
            }
            if (rc == SQLITE_OK) rc = mw_exec(db, "COMMIT");
            if (rc == SQLITE_OK) { g_ok++; break; }
            mw_exec(db, "ROLLBACK");
            if ((rc & 0xff) != SQLITE_BUSY) { printf("FAIL appender rc %d\n", rc); mw_failures++; break; }
            g_retries++;
        }
    }
    sqlite3_finalize(st); sqlite3_close(db);
    return NULL;
}
static void case_chain (void) {
    fresh("spec3");
    sqlite3 *s = open_mw();
    CHECK(mw_scalar(s, "SELECT count(*) FROM t") == 1);
    pthread_mutex_lock(&mu); slow_us = 3000; pthread_mutex_unlock(&mu);       // a disk whose fsync takes 3 ms
    mw_db_stats s0; memset(&s0, 0, sizeof s0); sqlite3_file_control(s, "main", MW_FCNTL_DBSTATS, &s0);
    uint64_t t0 = now_ms();
    pthread_t th[NT];
    for (long i = 0; i < NT; i++) pthread_create(&th[i], NULL, appender, (void *)i);
    for (int i = 0; i < NT; i++) pthread_join(th[i], NULL);
    uint64_t ms = now_ms() - t0;
    pthread_mutex_lock(&mu); slow_us = 0; pthread_mutex_unlock(&mu);
    mw_db_stats s1; memset(&s1, 0, sizeof s1); sqlite3_file_control(s, "main", MW_FCNTL_DBSTATS, &s1);
    uint64_t syncs = s1.log_syncs - s0.log_syncs;
    printf("chain: %d commits, %d retries, %llu fsyncs, %llu ms\n", (int)g_ok, (int)g_retries, (unsigned long long)syncs, (unsigned long long)ms);
    CHECK(g_ok == NT * PER);
    CHECK(mw_scalar(s, "SELECT count(*) FROM t") == 1 + NT * PER * ROWS);
    CHECK(mw_scalar(s, "SELECT count(DISTINCT v) FROM t") == 1 + NT * PER * ROWS);
    CHECK(mw_scalar(s, "SELECT max(id) FROM t") == 1 + NT * PER * ROWS);    // (no rowid was used twice or skipped: every commit took the ones after the one before)
    CHECK(syncs * 2 < (uint64_t)g_ok);                                        // the dependent commits shared the fsyncs (one each: as many fsyncs as commits)
    sqlite3_stmt *st; sqlite3_prepare_v2(s, "PRAGMA integrity_check", -1, &st, NULL);
    CHECK(sqlite3_step(st) == SQLITE_ROW && strcmp((const char *)sqlite3_column_text(st, 0), "ok") == 0);
    sqlite3_finalize(st);
    sqlite3_close(s);
    mw_rmdb(g_path);
}

static void case_spec_off (void) {
    fresh("spec4");
    sqlite3 *a = open_mw(), *b = open_mw_uri("&mw_spec=0");
    CHECK(mw_scalar(b, "SELECT count(*) FROM t") == 1);
    pthread_mutex_lock(&mu); armed = 1; entered = 0; released = 0; pthread_mutex_unlock(&mu);
    job_t ja = { a, "INSERT INTO t(v) VALUES ('a')" };
    pthread_t ta; pthread_create(&ta, NULL, run_job, &ja);
    pthread_mutex_lock(&mu); while (!entered) pthread_cond_wait(&cv, &mu); pthread_mutex_unlock(&mu);
    CHECK_RC(mw_exec(b, "BEGIN"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "INSERT INTO t(v) VALUES ('b')"), SQLITE_OK);
    CHECK(sqlite3_last_insert_rowid(b) == 2);                                 // (seed = 1: it did not see A, which took 2 too)
    mw_exec(b, "ROLLBACK");                                                   // (a transaction that is not speculative ends at once)
    pthread_mutex_lock(&mu); released = 1; pthread_cond_broadcast(&cv); pthread_mutex_unlock(&mu);
    pthread_join(ta, NULL);
    CHECK_RC(ja.rc, SQLITE_OK);
    CHECK(ja.rowid == 2);
    sqlite3_close(a); sqlite3_close(b);
    mw_rmdb(g_path);
}

int main (void) {
    case_held_fsync();
    case_spec_off();
    case_taken_back();
    case_chain();
    MW_DONE();
}
