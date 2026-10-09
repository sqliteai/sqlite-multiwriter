// Phase 3/4: transaction identity, commit epochs and the active-snapshot registry
// (still on the stock shared WAL: physical write behaviour is unchanged).
#include <pthread.h>
#include "mw_test.h"
#include "multiwriter.h"

static int open_mw (const char *path, sqlite3 **db) {
    char uri[300];
    snprintf(uri, sizeof uri, "file:%s?mw=2", path);
    return sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
}
static mw_tx_info txinfo (sqlite3 *db) { mw_tx_info i; memset(&i, 0, sizeof i); sqlite3_file_control(db, "main", MW_FCNTL_TXINFO, &i); return i; }
static mw_db_stats stats (sqlite3 *db) { mw_db_stats s; memset(&s, 0, sizeof s); sqlite3_file_control(db, "main", MW_FCNTL_DBSTATS, &s); return s; }

typedef struct { const char *path; int id; int commits; } worker_t;
static void *worker (void *arg) {
    worker_t *w = arg;
    sqlite3 *db;
    if (open_mw(w->path, &db) != SQLITE_OK) return NULL;
    sqlite3_busy_timeout(db, 30000);
    for (int i = 0; i < 100; i++) {
        char sql[128];
        snprintf(sql, sizeof sql, "INSERT INTO t(v) VALUES('w%d-%d')", w->id, i);
        mw_tx_info before = txinfo(db);
        if (mw_exec(db, sql) == SQLITE_OK) w->commits++;
        mw_tx_info after = txinfo(db);
        CHECK(after.tx_id >= before.tx_id);
        CHECK(after.writer_id == before.writer_id);
    }
    sqlite3_close(db);
    return NULL;
}

int main (void) {
    char path[256];
    mw_tmpdb(path, sizeof path, "ident");
    sqlite3 *a, *b;
    CHECK_RC(open_mw(path, &a), SQLITE_OK);
    CHECK_RC(open_mw(path, &b), SQLITE_OK);
    CHECK_RC(mw_exec(a, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)"), SQLITE_OK);

    mw_tx_info ia = txinfo(a), ib = txinfo(b);
    CHECK(ia.writer_id != 0 && ib.writer_id != 0 && ia.writer_id != ib.writer_id);   // one lane per connection
    CHECK(ia.state == MW_TX_COMMITTED || ia.state == MW_TX_ABORTED || ia.state == MW_TX_NONE);

    // A pins a snapshot
    CHECK_RC(mw_exec(a, "BEGIN"), SQLITE_OK);
    CHECK(mw_scalar(a, "SELECT count(*) FROM t") == 0);
    ia = txinfo(a);
    CHECK(ia.state == MW_TX_ACTIVE && ia.snapshot_epoch > 0 && ia.commit_epoch == 0 && ia.tx_id > 0);
    uint64_t snap = ia.snapshot_epoch;
    mw_db_stats st = stats(a);
    CHECK(st.snapshot_count == 1);
    CHECK(st.oldest_active_snapshot == snap);
    CHECK(st.epoch == snap);

    // B commits three times; the epoch advances, A's snapshot does not
    for (int i = 0; i < 3; i++) {
        CHECK_RC(mw_exec(b, "INSERT INTO t(v) VALUES('x')"), SQLITE_OK);
        mw_tx_info t = txinfo(b);
        CHECK(t.commit_epoch == snap + 1 + i);
        CHECK(t.state == MW_TX_COMMITTED && t.is_writer);
    }
    st = stats(a);
    CHECK(st.epoch == snap + 3);
    CHECK(st.oldest_active_snapshot == snap);            // pinned by A
    CHECK(txinfo(a).snapshot_epoch == snap);
    CHECK(mw_scalar(a, "SELECT count(*) FROM t") == 0);  // snapshot isolation of the stock WAL

    CHECK_RC(mw_exec(a, "COMMIT"), SQLITE_OK);
    st = stats(a);
    CHECK(st.snapshot_count == 0);
    CHECK(st.oldest_active_snapshot == st.epoch);        // nothing active: GC horizon = now
    CHECK(txinfo(a).state == MW_TX_COMMITTED);           // read-only txn

    // rollback is an abort and does not advance the epoch
    uint64_t e = stats(a).epoch;
    CHECK_RC(mw_exec(b, "BEGIN; INSERT INTO t(v) VALUES('r'); ROLLBACK"), SQLITE_OK);
    CHECK(txinfo(b).state == MW_TX_ABORTED);
    CHECK(stats(a).epoch == e);
    CHECK(stats(a).aborts >= 1);

    // tx ids are unique and increasing per connection
    uint64_t last = 0;
    for (int i = 0; i < 20; i++) {
        mw_scalar(a, "SELECT count(*) FROM t");
        uint64_t id = txinfo(a).tx_id;
        CHECK(id > last);
        last = id;
    }

    // two overlapping readers: horizon is the older one
    mw_exec(a, "BEGIN"); mw_scalar(a, "SELECT count(*) FROM t");
    uint64_t sa = txinfo(a).snapshot_epoch;
    mw_exec(b, "INSERT INTO t(v) VALUES('y')");
    mw_exec(b, "BEGIN"); mw_scalar(b, "SELECT count(*) FROM t");
    CHECK(txinfo(b).snapshot_epoch == sa + 1);
    CHECK(stats(a).snapshot_count == 2 && stats(a).oldest_active_snapshot == sa);
    mw_exec(a, "COMMIT");
    CHECK(stats(a).oldest_active_snapshot == sa + 1);
    mw_exec(b, "COMMIT");
    CHECK(stats(a).snapshot_count == 0);
    sqlite3_close(a); sqlite3_close(b);

    // concurrent threads (registry under contention); epochs must equal the number of commits
    sqlite3 *c; open_mw(path, &c);
    uint64_t e0 = stats(c).epoch;
    enum { N = 6 };
    pthread_t th[N]; worker_t w[N];
    for (int i = 0; i < N; i++) { w[i] = (worker_t){path, i, 0}; pthread_create(&th[i], NULL, worker, &w[i]); }
    int total = 0;
    for (int i = 0; i < N; i++) { pthread_join(th[i], NULL); total += w[i].commits; }
    CHECK(total == N * 100);
    CHECK(stats(c).epoch == e0 + (uint64_t)total);
    CHECK(mw_scalar(c, "SELECT count(*) FROM t") >= total);
    CHECK(stats(c).snapshot_count == 0);
    sqlite3_close(c);

    mw_rmdb(path);
    MW_DONE();
}
