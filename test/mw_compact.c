// Phase 18: compaction. Committed page versions are materialised into the real database file, which
// then is a valid stock SQLite database at the compacted epoch, while writers keep committing.
#include <pthread.h>
#include "mw_test.h"
#include "multiwriter.h"

static int open_lane (const char *path, sqlite3 **db, const char *extra) {
    char uri[400];
    snprintf(uri, sizeof uri, "file:%s?mw=1&mw_gc=16%s", path, extra);
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
static int64_t stock_scalar (const char *path, const char *sql, int *integrity) {
    char uri[400]; snprintf(uri, sizeof uri, "file:%s?immutable=1", path);      // the real file only: no -wal/-shm, no log
    sqlite3 *c; int64_t v = -1;
    if (sqlite3_open_v2(uri, &c, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, MW_PLAIN_VFS) == SQLITE_OK) {
        v = mw_scalar(c, sql);
        if (integrity) *integrity = integrity_ok(c);
    }
    sqlite3_close(c);
    return v;
}

static const char *g_path;
static _Atomic int stop_flag;
typedef struct { int id; long commits; } worker_t;
static void *writer (void *arg) {
    worker_t *w = arg;
    sqlite3 *db; open_lane(g_path, &db, "");
    sqlite3_busy_timeout(db, 5000);
    while (!stop_flag) {
        char sql[120]; snprintf(sql, sizeof sql, "UPDATE t SET v=v+1 WHERE id=%d", w->id);
        if (mw_exec(db, sql) == SQLITE_OK) w->commits++;
    }
    sqlite3_close(db);
    return NULL;
}

int main (void) {
    char path[256];
    mw_tmpdb(path, sizeof path, "compact");
    g_path = path;
    char lp[300]; snprintf(lp, sizeof lp, "%s-mw", path); unlink(lp);
    sqlite3 *s;
    CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, MW_PLAIN_VFS), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER, pad BLOB);"
                        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<64) INSERT INTO t SELECT i, 0, zeroblob(3000) FROM n"), SQLITE_OK);
    sqlite3_close(s);
    int integrity = 0;
    CHECK(stock_scalar(path, "SELECT sum(v) FROM t", &integrity) == 0);

    sqlite3 *a, *r;
    CHECK_RC(open_lane(path, &a, ""), SQLITE_OK);
    for (int i = 0; i < 200; i++) { char sql[100]; snprintf(sql, sizeof sql, "UPDATE t SET v=v+1 WHERE id=%d", 1 + i % 64); CHECK_RC(mw_exec(a, sql), SQLITE_OK); }
    mw_db_stats st = stats(a);
    CHECK(st.compaction_backlog == 200 && st.log_bytes > 64);
    CHECK(stock_scalar(path, "SELECT sum(v) FROM t", NULL) == 0);                      // real file untouched so far

    // ---- full compaction: the real file now holds everything and is a valid stock database
    mw_compact_result cr;
    CHECK_RC(sqlite3_file_control(a, "main", MW_FCNTL_COMPACT, &cr), SQLITE_OK);
    st = stats(a);
    printf("compaction #1: target=%llu pages=%llu freed=%llu %.2f ms; backlog=%llu log=%llu B\n", (unsigned long long)cr.target_epoch,
           (unsigned long long)cr.pages_written, (unsigned long long)cr.versions_freed, (double)cr.duration_ns / 1e6, (unsigned long long)st.compaction_backlog, (unsigned long long)st.log_bytes);
    CHECK(cr.target_epoch == st.epoch && cr.pages_written > 0 && cr.pages_written <= 64);
    CHECK(st.compaction_backlog == 0 && st.base_epoch == st.epoch && st.log_bytes == 64);
    CHECK(st.page_versions <= 64);                                                     // versions <= base were freed
    CHECK(stock_scalar(path, "SELECT sum(v) FROM t", &integrity) == 200 && integrity);
    CHECK(mw_scalar(a, "SELECT sum(v) FROM t") == 200);                                // lanes keep working off the file + store

    // ---- a long-lived reader limits the target epoch to its snapshot
    CHECK_RC(open_lane(path, &r, ""), SQLITE_OK);
    CHECK_RC(mw_exec(r, "BEGIN"), SQLITE_OK);
    CHECK(mw_scalar(r, "SELECT sum(v) FROM t") == 200);
    mw_tx_info ti; sqlite3_file_control(r, "main", MW_FCNTL_TXINFO, &ti);
    for (int i = 0; i < 100; i++) { char sql[100]; snprintf(sql, sizeof sql, "UPDATE t SET v=v+1 WHERE id=%d", 1 + i % 64); CHECK_RC(mw_exec(a, sql), SQLITE_OK); }
    CHECK_RC(sqlite3_file_control(a, "main", MW_FCNTL_COMPACT, &cr), SQLITE_OK);
    CHECK(cr.target_epoch == 0 || cr.target_epoch <= ti.snapshot_epoch);               // never past the pinned snapshot
    CHECK(stock_scalar(path, "SELECT sum(v) FROM t", NULL) == 200);                    // file still at the reader's state
    CHECK(mw_scalar(r, "SELECT sum(v) FROM t") == 200);                                // reader unaffected
    st = stats(a);
    CHECK(st.compaction_backlog == 100);
    CHECK_RC(mw_exec(r, "COMMIT"), SQLITE_OK);
    CHECK_RC(sqlite3_file_control(a, "main", MW_FCNTL_COMPACT, &cr), SQLITE_OK);       // released: catches up
    CHECK(stats(a).compaction_backlog == 0);
    CHECK(stock_scalar(path, "SELECT sum(v) FROM t", &integrity) == 300 && integrity);
    sqlite3_close(r);

    // ---- background compactor while writers commit
    sqlite3_close(a);
    CHECK_RC(open_lane(path, &a, "&mw_compact_ms=2"), SQLITE_OK);
    stop_flag = 0;
    enum { NT = 6 };
    pthread_t th[NT]; worker_t w[NT];
    for (int i = 0; i < NT; i++) { w[i] = (worker_t){ .id = 1 + i * 10 }; pthread_create(&th[i], NULL, writer, &w[i]); }
    #ifdef _WIN32
    struct timespec ts = { 10, 0 };       // (FlushFileBuffers of a disk takes tens of milliseconds: a second is not enough commits)
#else
    struct timespec ts = { 1, 0 };
#endif
    nanosleep(&ts, NULL);
    stop_flag = 1;
    long commits = 0;
    for (int i = 0; i < NT; i++) { pthread_join(th[i], NULL); commits += w[i].commits; }
    st = stats(a);
    printf("concurrent: %ld commits, %llu compactions, %llu pages written, backlog now %llu, versions=%llu\n", commits, (unsigned long long)st.compactions,
           (unsigned long long)st.compacted_pages, (unsigned long long)st.compaction_backlog, (unsigned long long)st.page_versions);
    CHECK(commits > 100 && st.compactions > 5);
    CHECK(mw_scalar(a, "SELECT sum(v) FROM t") == 300 + commits);
    CHECK(integrity_ok(a));
    sqlite3_close(a);                                                                  // final compaction + log removal
    CHECK(access(lp, F_OK) != 0);
    CHECK(stock_scalar(path, "SELECT sum(v) FROM t", &integrity) == 300 + commits && integrity);

    // ---- file growth and shrink through compaction (page count follows the logical size)
    CHECK_RC(open_lane(path, &a, ""), SQLITE_OK);
    CHECK_RC(mw_exec(a, "CREATE TABLE big(id INTEGER PRIMARY KEY, v BLOB); WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<400) INSERT INTO big SELECT i, zeroblob(2000) FROM n"), SQLITE_OK);
    int64_t pages = mw_scalar(a, "PRAGMA page_count");
    sqlite3_file_control(a, "main", MW_FCNTL_COMPACT, &cr);
    CHECK(stock_scalar(path, "PRAGMA page_count", NULL) == pages);
    CHECK_RC(mw_exec(a, "DROP TABLE big; VACUUM"), SQLITE_OK);
    int64_t pages2 = mw_scalar(a, "PRAGMA page_count");
    CHECK(pages2 < pages);
    sqlite3_file_control(a, "main", MW_FCNTL_COMPACT, &cr);
    CHECK(stock_scalar(path, "PRAGMA page_count", &integrity) == pages2 && integrity);   // file shrank
    sqlite3_close(a);
    mw_rmdb(path); unlink(lp);
    MW_DONE();
}
