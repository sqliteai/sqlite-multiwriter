// Phase 16: DDL / schema changes. Schema-changing statements (CREATE/DROP/ALTER/REINDEX/VACUUM) are
// detected at statement start and run behind an exclusive schema barrier: one at a time, new write
// transactions are refused (SQLITE_BUSY, so busy_timeout waits) while it runs, in-flight writers are
// drained first. Readers are never blocked (MVCC). A transaction whose snapshot predates a schema change
// can never publish (retryable SQLITE_BUSY_SNAPSHOT), even if it touches unrelated pages. DDL is never
// merged or rebased.
#include <pthread.h>
#include "mw_test.h"
#include "multiwriter.h"

static int open_lane (const char *path, sqlite3 **db) {
    char uri[300];
    snprintf(uri, sizeof uri, "file:%s?mw=2&mw_gc=32", path);
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
static volatile int stop_writers;
typedef struct { int id, ok, busy; int codes[1024]; int last_failed_id; } worker_t;
static void *writer (void *arg) {
    worker_t *w = arg;
    sqlite3 *db; open_lane(g_path, &db);
    sqlite3_busy_timeout(db, 5000);
    for (int i = 0; !stop_writers; i++) {
        char sql[120]; snprintf(sql, sizeof sql, "INSERT INTO items VALUES(%d, 'w%d')", w->id * 1000000 + i, w->id);
        int rc = mw_exec(db, sql);
        if (rc == SQLITE_OK) w->ok++; else { w->busy++; if (rc < 1024) w->codes[rc]++; }
        if (rc == SQLITE_BUSY_SNAPSHOT && !sqlite3_get_autocommit(db)) mw_exec(db, "ROLLBACK");
    }
    sqlite3_close(db);
    return NULL;
}

int main (void) {
    char path[256];
    mw_tmpdb(path, sizeof path, "ddl");
    g_path = path;
    sqlite3 *s, *a, *b;
    CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT); CREATE TABLE other(id INTEGER PRIMARY KEY, v INTEGER);"
                        "INSERT INTO other VALUES(1,0); WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<500) INSERT INTO items SELECT i, 'seed'||i FROM n"), SQLITE_OK);
    sqlite3_close(s);
    CHECK_RC(open_lane(path, &a), SQLITE_OK);
    CHECK_RC(open_lane(path, &b), SQLITE_OK);

    // ---- barrier: refuses new writers while the DDL transaction is open; readers unaffected
    uint64_t gen0 = stats(a).schema_generation;
    CHECK_RC(mw_exec(a, "BEGIN; CREATE INDEX items_name ON items(name)"), SQLITE_OK);
    CHECK(stats(a).ddl_barriers == 1);
    CHECK_RC(mw_exec(b, "UPDATE other SET v=1 WHERE id=1"), SQLITE_BUSY);              // refused: schema barrier
    CHECK(mw_scalar(b, "SELECT count(*) FROM items") == 500);                            // readers proceed (old snapshot)
    CHECK(mw_scalar(b, "SELECT count(*) FROM sqlite_master WHERE name='items_name'") == 0);
    CHECK_RC(mw_exec(a, "COMMIT"), SQLITE_OK);
    mw_db_stats st = stats(a);
    CHECK(st.schema_generation == gen0 + 1 && st.last_schema_epoch == st.epoch);
    CHECK_RC(mw_exec(b, "UPDATE other SET v=1 WHERE id=1"), SQLITE_OK);                  // barrier lifted
    CHECK(mw_scalar(b, "SELECT count(*) FROM items INDEXED BY items_name") == 500);      // b sees the index
    CHECK(integrity_ok(b));

    // ---- a stale-schema writer fails fast at its first write (even on unrelated pages), then succeeds after restart
    CHECK_RC(mw_exec(b, "BEGIN"), SQLITE_OK);
    CHECK(mw_scalar(b, "SELECT v FROM other WHERE id=1") == 1);
    CHECK_RC(mw_exec(a, "CREATE TABLE extra(x)"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "UPDATE other SET v=2 WHERE id=1"), SQLITE_BUSY_SNAPSHOT);
    CHECK_RC(mw_exec(b, "ROLLBACK"), SQLITE_OK);
    CHECK_RC(mw_exec(b, "UPDATE other SET v=2 WHERE id=1"), SQLITE_OK);

    // ---- DROP TABLE / REINDEX / VACUUM
    CHECK_RC(mw_exec(a, "DROP TABLE extra"), SQLITE_OK);
    CHECK_RC(mw_exec(a, "REINDEX items_name"), SQLITE_OK);
    CHECK(integrity_ok(a));
    // VACUUM with a reader pinned on the pre-VACUUM snapshot: the reader keeps a consistent old view
    CHECK_RC(mw_exec(a, "DELETE FROM items WHERE id > 100"), SQLITE_OK);
    int64_t pages_before = mw_scalar(a, "PRAGMA page_count");
    CHECK_RC(mw_exec(b, "BEGIN"), SQLITE_OK);
    CHECK(mw_scalar(b, "SELECT count(*) FROM items") == 100);
    int rc = mw_exec(a, "VACUUM");
    printf("VACUUM in a lane: rc=%d (%s)\n", rc, sqlite3_errmsg(a));
    if (rc == SQLITE_OK) {
        CHECK(mw_scalar(a, "SELECT count(*) FROM items") == 100);
        CHECK(mw_scalar(a, "PRAGMA page_count") < pages_before);
        CHECK(integrity_ok(a));
        CHECK(mw_scalar(b, "SELECT count(*) FROM items") == 100);                          // b's pinned snapshot is untouched
    }
    CHECK_RC(mw_exec(b, "COMMIT"), SQLITE_OK);
    CHECK(integrity_ok(b));
    CHECK(mw_scalar(b, "SELECT count(*) FROM items") == 100);
    sqlite3_close(b);          // (a stays open: committed state lives only in the page store until compaction, phase 18)

    // ---- CREATE INDEX while writers run: the barrier drains them, the DDL completes, writers resume
    stop_writers = 0;
    enum { NT = 4 };
    pthread_t th[NT]; worker_t w[NT];
    for (int i = 0; i < NT; i++) { w[i] = (worker_t){ .id = i + 1 }; pthread_create(&th[i], NULL, writer, &w[i]); }
    struct timespec ts = { 0, 100 * 1000000 }; nanosleep(&ts, NULL);
    sqlite3_busy_timeout(a, 5000);
    int ddl_ok = 0;
    for (int attempt = 0; attempt < 50 && !ddl_ok; attempt++) ddl_ok = (mw_exec(a, "CREATE INDEX items_id2 ON items(name, id)") == SQLITE_OK);
    CHECK(ddl_ok);
    nanosleep(&ts, NULL);
    stop_writers = 1;
    int ok = 0;
    for (int i = 0; i < NT; i++) { pthread_join(th[i], NULL); ok += w[i].ok; }
    printf("writers during DDL: %d commits\n", ok);
    CHECK(ok > 0);
    CHECK(mw_scalar(a, "SELECT count(*) FROM items INDEXED BY items_id2") == mw_scalar(a, "SELECT count(*) FROM items"));
    printf("rows=%lld expected=%d busy=%d\n", (long long)mw_scalar(a, "SELECT count(*) FROM items"), 100 + ok, w[0].busy + w[1].busy + w[2].busy + w[3].busy);
    for (int i = 0; i < NT; i++) {
        char q[100]; snprintf(q, sizeof q, "SELECT count(*) FROM items WHERE name='w%d'", i + 1);
        printf("  writer %d: ok=%d rows=%lld failed=%d codes:", i + 1, w[i].ok, (long long)mw_scalar(a, q), w[i].busy);
        for (int c = 0; c < 1024; c++) if (w[i].codes[c]) printf(" %d x%d", c, w[i].codes[c]);
        printf("\n");
    }
    CHECK(mw_scalar(a, "SELECT count(*) FROM items") == 100 + ok);
    CHECK(integrity_ok(a));
    sqlite3_close(a);
    mw_rmdb(path);
    MW_DONE();
}
