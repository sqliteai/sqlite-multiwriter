// Phase 8: fast commit + page-level conflict detection. A conflicting commit is refused with a
// retryable SQLITE_BUSY_SNAPSHOT (CRDT rebase replaces this in phase 10).
#include <pthread.h>
#include "mw_test.h"
#include "multiwriter.h"

static int open_lane (const char *path, sqlite3 **db) {
    char uri[300];
    snprintf(uri, sizeof uri, "file:%s?mw=2", path);
    return sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
}
static mw_tx_info txinfo (sqlite3 *db) { mw_tx_info i; memset(&i, 0, sizeof i); sqlite3_file_control(db, "main", MW_FCNTL_TXINFO, &i); return i; }
static mw_db_stats stats (sqlite3 *db) { mw_db_stats s; memset(&s, 0, sizeof s); sqlite3_file_control(db, "main", MW_FCNTL_DBSTATS, &s); return s; }

#define ROWS 64
static const char *g_path;

typedef struct { int id; int ok, retries; } worker_t;
static void *own_row_worker (void *arg) {
    worker_t *w = arg;
    sqlite3 *db;
    open_lane(g_path, &db);
    for (int i = 0; i < 300; i++) {
        char sql[120];
        snprintf(sql, sizeof sql, "UPDATE t SET v=v+1 WHERE id=%d", w->id);
        for (;;) {
            int rc = mw_exec(db, sql);
            if (rc == SQLITE_OK) { w->ok++; break; }
            w->retries++;
        }
    }
    sqlite3_close(db);
    return NULL;
}
static void *hot_row_worker (void *arg) {
    worker_t *w = arg;
    sqlite3 *db;
    open_lane(g_path, &db);
    for (int i = 0; i < 200; i++) {
        for (;;) {
            int rc = mw_exec(db, "UPDATE t SET v=v+1 WHERE id=1");     // the same page and row for everybody
            if (rc == SQLITE_OK) { w->ok++; break; }
            CHECK((rc & 0xff) == SQLITE_BUSY);
            w->retries++;
        }
    }
    sqlite3_close(db);
    return NULL;
}

int main (void) {
    char path[256];
    mw_tmpdb(path, sizeof path, "fast");
    g_path = path;
    sqlite3 *s;
    CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), SQLITE_OK);
    // one row per leaf page (pad ~3000 bytes): rows are physically disjoint
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER, pad BLOB);"
                        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<64) INSERT INTO t SELECT i, 0, zeroblob(3000) FROM n;"
                        "CREATE TABLE small(id INTEGER PRIMARY KEY, v INTEGER); INSERT INTO small VALUES(1,0),(2,0);"), SQLITE_OK);
    sqlite3_close(s);

    sqlite3 *a, *b;
    CHECK_RC(open_lane(path, &a), SQLITE_OK);
    CHECK_RC(open_lane(path, &b), SQLITE_OK);
    sqlite3_extended_result_codes(b, 1);

    // ---- disjoint pages: both overlapping transactions commit (fast path)
    CHECK_RC(mw_exec(a, "BEGIN; UPDATE t SET v=1 WHERE id=1"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "BEGIN; UPDATE t SET v=2 WHERE id=40"), SQLITE_OK);
    CHECK_RC(mw_exec(a, "COMMIT"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "COMMIT"), SQLITE_OK);
    CHECK(mw_scalar(a, "SELECT v FROM t WHERE id=1") == 1 && mw_scalar(a, "SELECT v FROM t WHERE id=40") == 2);
    CHECK(stats(a).fast_commits == 2 && stats(a).page_conflicts == 0);

    // ---- same page: the second committer must not overwrite the first (no lost update)
    CHECK_RC(mw_exec(a, "BEGIN; UPDATE small SET v=10 WHERE id=1"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "BEGIN; UPDATE small SET v=20 WHERE id=2"), SQLITE_OK);     // same leaf page, other row
    CHECK_RC(mw_exec(a, "COMMIT"), SQLITE_OK);
    int rc = mw_exec(b, "COMMIT");
    CHECK((rc & 0xff) == SQLITE_BUSY);
    CHECK(rc == SQLITE_BUSY_SNAPSHOT);
    CHECK(sqlite3_get_autocommit(b) == 1);                  // rolled back, connection usable
    CHECK(txinfo(b).state == MW_TX_ABORTED);
    CHECK(stats(a).page_conflicts == 1);
    CHECK(mw_scalar(b, "SELECT v FROM small WHERE id=1") == 10);   // A's commit intact
    CHECK(mw_scalar(b, "SELECT v FROM small WHERE id=2") == 0);    // B's discarded
    CHECK_RC(mw_exec(b, "BEGIN; UPDATE small SET v=20 WHERE id=2; COMMIT"), SQLITE_OK);   // retry on fresh snapshot
    CHECK(mw_scalar(a, "SELECT v FROM small WHERE id=1") == 10 && mw_scalar(a, "SELECT v FROM small WHERE id=2") == 20);

    // ---- stale schema: B started before A's DDL; even a write to unrelated pages must not publish
    CHECK_RC(mw_exec(b, "BEGIN; UPDATE t SET v=99 WHERE id=7"), SQLITE_OK);
    CHECK_RC(mw_exec(a, "CREATE TABLE other(x)"), SQLITE_OK);
    rc = mw_exec(b, "COMMIT");
    CHECK(rc == SQLITE_BUSY_SNAPSHOT);
    CHECK(stats(a).schema_conflicts == 1);
    CHECK(mw_scalar(b, "SELECT count(*) FROM sqlite_master WHERE name='other'") == 1);
    CHECK(mw_scalar(b, "SELECT v FROM t WHERE id=7") == 0);
    sqlite3_close(b);

    // ---- many threads on disjoint rows: every commit is a fast commit, zero retries
    mw_db_stats st0 = stats(a);
    int64_t initial[16];
    for (int i = 0; i < 16; i++) { char q[100]; snprintf(q, sizeof q, "SELECT v FROM t WHERE id=%d", 10 + i * 3); initial[i] = mw_scalar(a, q); }
    enum { NT = 16 };
    pthread_t th[NT]; worker_t w[NT];
    for (int i = 0; i < NT; i++) { w[i] = (worker_t){ .id = 10 + i * 3 }; pthread_create(&th[i], NULL, own_row_worker, &w[i]); }
    int ok = 0, retries = 0;
    for (int i = 0; i < NT; i++) { pthread_join(th[i], NULL); ok += w[i].ok; retries += w[i].retries; }
    mw_db_stats st1 = stats(a);
    CHECK(ok == NT * 300);
    CHECK(retries == 0);
    CHECK(st1.fast_commits - st0.fast_commits == (uint64_t)NT * 300);
    CHECK(st1.page_conflicts == st0.page_conflicts);
    for (int i = 0; i < NT; i++) {
        char sql[100]; snprintf(sql, sizeof sql, "SELECT v FROM t WHERE id=%d", 10 + i * 3);
        CHECK(mw_scalar(a, sql) == initial[i] + 300);
    }
    printf("disjoint: %d commits, %d retries, %llu fast\n", ok, retries, (unsigned long long)(st1.fast_commits - st0.fast_commits));

    // ---- all threads on ONE row: conflicts are refused, retried, and no update is lost
    int base = (int)mw_scalar(a, "SELECT v FROM t WHERE id=1");
    for (int i = 0; i < NT; i++) { w[i] = (worker_t){ .id = 0 }; pthread_create(&th[i], NULL, hot_row_worker, &w[i]); }
    ok = retries = 0;
    for (int i = 0; i < NT; i++) { pthread_join(th[i], NULL); ok += w[i].ok; retries += w[i].retries; }
    CHECK(ok == NT * 200);
    CHECK(mw_scalar(a, "SELECT v FROM t WHERE id=1") == base + NT * 200);    // serializable outcome: no lost update
    CHECK((int)(stats(a).page_conflicts - st1.page_conflicts) == retries);
    printf("hot row: %d commits, %d retries (conflicts)\n", ok, retries);
    CHECK(mw_scalar(a, "PRAGMA integrity_check") != -1);
    sqlite3_close(a);
    mw_rmdb(path);
    MW_DONE();
}
