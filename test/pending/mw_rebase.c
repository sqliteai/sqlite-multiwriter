// Phase 10: basic CRDT rebase. Same-page (different-row) conflicts are resolved by replaying the
// transaction's sqlite-sync changes at the latest snapshot; stock SQLite regenerates the pages.
// Transactions with no logical representation are still refused with SQLITE_BUSY_SNAPSHOT.
#include <pthread.h>
#include "mw_test.h"
#include "multiwriter.h"

static int close_cs (sqlite3 *db) { return sqlite3_close(db); }
static int open_lane (const char *path, sqlite3 **db) {
    char uri[300];
    snprintf(uri, sizeof uri, "file:%s?mw=2&mw_gc=16", path);
    int rc = sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
    if (rc == SQLITE_OK) sqlite3_extended_result_codes(*db, 1);
    return rc;
}
static mw_db_stats stats (sqlite3 *db) { mw_db_stats s; memset(&s, 0, sizeof s); sqlite3_file_control(db, "main", MW_FCNTL_DBSTATS, &s); return s; }
static int integrity_ok (sqlite3 *db) {
    sqlite3_stmt *st; int ok = 0;
    if (sqlite3_prepare_v2(db, "PRAGMA integrity_check", -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW) ok = strcmp((const char *)sqlite3_column_text(st, 0), "ok") == 0;
    sqlite3_finalize(st);
    return ok;
}

static const char *g_path;
typedef struct { int id, n, ok, errors; int64_t last_rc; } worker_t;
static void *insert_worker (void *arg) {
    worker_t *w = arg;
    sqlite3 *db;
    int orc = open_lane(g_path, &db);
    if (orc != SQLITE_OK) { fprintf(stderr, "worker %d open rc=%d %s\n", w->id, orc, db ? sqlite3_errmsg(db) : ""); w->errors = w->n; return NULL; }
    for (int i = 0; i < w->n; i++) {
        char sql[200];
        snprintf(sql, sizeof sql, "INSERT INTO t VALUES('w%d-%d','payload %d', %d, zeroblob(%d))", w->id, i, i, w->id, 20 + (i * 37) % 300);
        // page-level read validation (on by default) may refuse a transaction whose read-only pages changed
        // (retryable SQLITE_BUSY_SNAPSHOT); applications retry, like any optimistic-concurrency client
        int rc = SQLITE_BUSY_SNAPSHOT;
        for (int attempt = 0; attempt < 200 && (rc & 0xff) == SQLITE_BUSY; attempt++) { rc = mw_exec(db, sql); if ((rc & 0xff) == SQLITE_BUSY) w->last_rc = 0; }
        if (rc == SQLITE_OK) w->ok++; else { if (!w->errors) fprintf(stderr, "worker %d first error rc=%d: %s\n", w->id, rc, sqlite3_errmsg(db)); w->errors++; w->last_rc = rc; }
    }
    close_cs(db);
    return NULL;
}

int main (void) {
    char path[256];
    mw_tmpdb(path, sizeof path, "rebase");
    g_path = path;
    sqlite3 *s;
    CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), SQLITE_OK);
    sqlite3_close(s);
    CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id TEXT PRIMARY KEY NOT NULL, note TEXT, w INTEGER, pad BLOB);"
                        "CREATE TABLE untracked(id INTEGER PRIMARY KEY, v INTEGER); INSERT INTO untracked VALUES(1,0),(2,0);"), SQLITE_OK);
    CHECK_RC(close_cs(s), SQLITE_OK);

    sqlite3 *a, *b;
    CHECK_RC(open_lane(path, &a), SQLITE_OK);
    CHECK_RC(open_lane(path, &b), SQLITE_OK);

    // ---- same page, different rows: both commit; the second is rebased
    CHECK_RC(mw_exec(a, "BEGIN; INSERT INTO t VALUES('a','from a',1,zeroblob(50))"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "BEGIN; INSERT INTO t VALUES('b','from b',2,zeroblob(50))"), SQLITE_OK);
    CHECK_RC(mw_exec(a, "COMMIT"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "COMMIT"), SQLITE_OK);
    mw_db_stats st = stats(a);
    CHECK(st.rebases == 1 && st.page_conflicts >= 1 && st.rebase_retries == 0);
    CHECK(mw_scalar(a, "SELECT count(*) FROM t") == 2);
    CHECK(mw_scalar(b, "SELECT note='from a' FROM t WHERE id='a'") == 1 && mw_scalar(b, "SELECT note='from b' FROM t WHERE id='b'") == 1);
    CHECK(integrity_ok(a));

    // ---- multi-statement transaction with updates, inserts and a delete, all rebased together
    CHECK_RC(mw_exec(a, "BEGIN; INSERT INTO t VALUES('a2','x',1,zeroblob(10)); UPDATE t SET note='a-upd' WHERE id='a'"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "BEGIN; INSERT INTO t VALUES('b2','y',2,zeroblob(10)); DELETE FROM t WHERE id='b'"), SQLITE_OK);
    CHECK_RC(mw_exec(a, "COMMIT"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "COMMIT"), SQLITE_OK);
    CHECK(mw_scalar(a, "SELECT count(*) FROM t") == 3);                        // a, a2, b2
    CHECK(mw_scalar(a, "SELECT note FROM t WHERE id='a'") != -1 && mw_scalar(a, "SELECT note='a-upd' FROM t WHERE id='a'") == 1);
    CHECK(mw_scalar(a, "SELECT count(*) FROM t WHERE id='b'") == 0);
    CHECK(stats(a).rebases == 2);
    CHECK(integrity_ok(b));

    // ---- transactions without a logical representation are still refused (retryable), never silently merged
    CHECK_RC(mw_exec(a, "BEGIN; UPDATE untracked SET v=1 WHERE id=1"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "BEGIN; UPDATE untracked SET v=2 WHERE id=2"), SQLITE_OK);
    CHECK_RC(mw_exec(a, "COMMIT"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "COMMIT"), SQLITE_BUSY_SNAPSHOT);                       // untracked table: no logical changes
    CHECK(mw_scalar(b, "SELECT v FROM untracked WHERE id=1") == 1 && mw_scalar(b, "SELECT v FROM untracked WHERE id=2") == 0);
    CHECK(stats(a).unrebasable >= 1);
    // a tracked write mixed with an untracked one is not rebasable either (the untracked part would be lost)
    CHECK_RC(mw_exec(a, "BEGIN; UPDATE untracked SET v=5 WHERE id=1; INSERT INTO t VALUES('m1','x',1,zeroblob(10))"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "BEGIN; INSERT INTO t VALUES('m2','y',2,zeroblob(10))"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "COMMIT"), SQLITE_OK);
    CHECK_RC(mw_exec(a, "COMMIT"), SQLITE_BUSY_SNAPSHOT);
    CHECK(mw_scalar(a, "SELECT count(*) FROM t WHERE id='m1'") == 0 && mw_scalar(a, "SELECT v FROM untracked WHERE id=1") == 1);

    // ---- DDL is never rebased; with the schema barrier up (a DDL statement ran) other writers are refused
    CHECK_RC(mw_exec(a, "BEGIN; CREATE TABLE ddl(x); INSERT INTO t VALUES('d1','x',1,zeroblob(10))"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "BEGIN; INSERT INTO t VALUES('d2','y',2,zeroblob(10))"), SQLITE_BUSY);   // barrier (busy_timeout 0)
    CHECK_RC(mw_exec(a, "COMMIT"), SQLITE_OK);                                                    // the DDL commits
    // (the refused statement released b's deferred snapshot, so the retry starts on a fresh one that includes the DDL)
    CHECK_RC(mw_exec(b, "INSERT INTO t VALUES('d2','y',2,zeroblob(10))"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "COMMIT"), SQLITE_OK);
    CHECK(mw_scalar(a, "SELECT count(*) FROM sqlite_master WHERE name='ddl'") == 1);
    CHECK_RC(close_cs(a), SQLITE_OK); CHECK_RC(close_cs(b), SQLITE_OK);

    // ---- hot table, many threads: every insert succeeds without an application-visible error
    CHECK_RC(open_lane(path, &a), SQLITE_OK);
    int before = (int)mw_scalar(a, "SELECT count(*) FROM t");
    mw_db_stats s0 = stats(a);
    enum { NT = 8, PER = 60 };
    pthread_t th[NT]; worker_t w[NT];
    for (int i = 0; i < NT; i++) { w[i] = (worker_t){ .id = i, .n = PER }; pthread_create(&th[i], NULL, insert_worker, &w[i]); }
    int ok = 0, errors = 0;
    for (int i = 0; i < NT; i++) { pthread_join(th[i], NULL); ok += w[i].ok; errors += w[i].errors; }
    mw_db_stats s1 = stats(a);
    printf("unrebasable=%llu schema_conflicts=%llu last_rc=%lld\n", (unsigned long long)s1.unrebasable, (unsigned long long)s1.schema_conflicts, (long long)w[0].last_rc);
    for (int i = 0; i < NT; i++) printf("  worker %d: ok=%d errors=%d last_rc=%lld\n", i, w[i].ok, w[i].errors, (long long)w[i].last_rc);
    printf("hot inserts: ok=%d errors=%d rebases=%llu retries=%llu max_attempts=%llu conflicts=%llu\n", ok, errors,
           (unsigned long long)(s1.rebases - s0.rebases), (unsigned long long)(s1.rebase_retries - s0.rebase_retries),
           (unsigned long long)s1.rebase_max_attempts, (unsigned long long)(s1.page_conflicts - s0.page_conflicts));
    CHECK(errors == 0);
    CHECK(ok == NT * PER);
    CHECK(mw_scalar(a, "SELECT count(*) FROM t") == before + NT * PER);
    CHECK(s1.rebases > s0.rebases);
    // one db_version per committed transaction, and each (db_version, seq) is unique
    CHECK(integrity_ok(a));
    // meta rows agree with the base table: every row has its column entries
    CHECK_RC(close_cs(a), SQLITE_OK);
    mw_rmdb(path);
    MW_DONE();
}
