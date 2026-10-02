// Spec item 50 (CloudSync interoperability), tested offline with payload exchange instead of a backend:
//   Multi-Writer lane DB  -> payload -> stock replica      (and back)
//   Multi-Writer lane DB  <-> Multi-Writer lane DB
// Speculative / aborted / unresolved-reservation changes never leave a lane database; after resolution
// every replica converges to the same state. (Sync against SQLite Cloud / PostgreSQL needs those services
// and is not run here.)
#include <pthread.h>
#include "mw_test.h"
#include "multiwriter.h"

static int close_cs (sqlite3 *db) { return sqlite3_close(db); }
static int open_lane (const char *path, sqlite3 **db) {
    char uri[300];
    snprintf(uri, sizeof uri, "file:%s?mw=2&mw_gc=32", path);
    int rc = sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
    if (rc == SQLITE_OK) sqlite3_extended_result_codes(*db, 1);
    return rc;
}
static int open_stock (const char *path, sqlite3 **db) {
    return sqlite3_open_v2(path, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix");
}

static void make_db (const char *path, int stock_init) {
    sqlite3 *s;
    CHECK_RC(open_stock(path, &s), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE docs(id TEXT PRIMARY KEY NOT NULL, title TEXT, body TEXT, n INTEGER);"), SQLITE_OK);
    (void)stock_init;
    CHECK_RC(close_cs(s), SQLITE_OK);
}

// Exports what the database may currently send (local changes below the send ceiling) as one payload; applies it to `to`.
static int64_t transfer (sqlite3 *from, sqlite3 *to) {
    sqlite3_stmt *st;
                      "WHERE site_id=cloudsync_siteid() AND db_version<=cloudsync_send_ceiling()";
    int is_lane = 0;
    { mw_tx_info ti; memset(&ti, 0, sizeof ti); is_lane = sqlite3_file_control(from, "main", MW_FCNTL_TXINFO, &ti) == SQLITE_OK; }
    CHECK_RC(sqlite3_prepare_v2(from, is_lane ? enc : enc_stock, -1, &st, NULL), SQLITE_OK);
    int64_t bytes = 0;
    if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_type(st, 0) == SQLITE_BLOB) {
        sqlite3_stmt *ap;
        CHECK_RC(sqlite3_prepare_v2(to, "SELECT cloudsync_payload_apply(?)", -1, &ap, NULL), SQLITE_OK);
        sqlite3_bind_blob(ap, 1, sqlite3_column_blob(st, 0), sqlite3_column_bytes(st, 0), SQLITE_TRANSIENT);
        bytes = sqlite3_column_bytes(st, 0);
        int rc = SQLITE_BUSY_SNAPSHOT;
        for (int attempt = 0; attempt < 50 && (rc & 0xff) == SQLITE_BUSY; attempt++) { sqlite3_reset(ap); rc = sqlite3_step(ap); if (rc == SQLITE_ROW || rc == SQLITE_DONE) rc = SQLITE_OK; }
        CHECK_RC(rc, SQLITE_OK);
        sqlite3_finalize(ap);
    }
    sqlite3_finalize(st);
    return bytes;
}
static char *dump (sqlite3 *db) {
    static char buf[4][8192]; static int k;
    char *b = buf[k++ & 3]; b[0] = 0;
    sqlite3_stmt *st;
    if (sqlite3_prepare_v2(db, "SELECT group_concat(id||'|'||coalesce(title,'')||'|'||coalesce(body,'')||'|'||coalesce(n,''), ';') FROM (SELECT * FROM docs ORDER BY id)", -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW && sqlite3_column_text(st, 0))
        snprintf(b, sizeof buf[0], "%s", (const char *)sqlite3_column_text(st, 0));
    sqlite3_finalize(st);
    return b;
}

static const char *g_path;
typedef struct { int id; } worker_t;
static void *writer (void *arg) {
    worker_t *w = arg;
    sqlite3 *db; open_lane(g_path, &db);
    for (int i = 0; i < 25; i++) {
        char sql[300];
        snprintf(sql, sizeof sql, "INSERT INTO docs VALUES('w%d-%d','title %d','body %d',%d)", w->id, i, i, i, w->id);
        for (int a = 0; a < 200; a++) { int rc = mw_exec(db, sql); if ((rc & 0xff) != SQLITE_BUSY) break; }
        if (i % 5 == 0) {                                            // an aborted transaction: must never sync
            snprintf(sql, sizeof sql, "BEGIN; INSERT INTO docs VALUES('ABORTED-%d-%d','x','x',0); ROLLBACK", w->id, i);
            mw_exec(db, sql);
        }
    }
    close_cs(db);
    return NULL;
}

int main (void) {
    char p1[256], p2[256], p3[256];
    mw_tmpdb(p1, sizeof p1, "sync1"); mw_tmpdb(p2, sizeof p2, "sync2"); mw_tmpdb(p3, sizeof p3, "sync3");
    make_db(p1, 0); make_db(p2, 0); make_db(p3, 1);
    sqlite3 *a, *b, *c;                                       // a, b: Multi-Writer lane databases; c: stock replica
    CHECK_RC(open_lane(p1, &a), SQLITE_OK);
    CHECK_RC(open_lane(p2, &b), SQLITE_OK);
    CHECK_RC(open_stock(p3, &c), SQLITE_OK);
    CHECK(mw_scalar(c, "SELECT count(*) FROM docs") == 0);

    // ---- A: concurrent writers (rebases) + an unresolved transaction holding its db_version
    g_path = p1;
    pthread_t th[4]; worker_t w[4];
    sqlite3 *held; CHECK_RC(open_lane(p1, &held), SQLITE_OK);
    CHECK_RC(mw_exec(held, "BEGIN; INSERT INTO docs VALUES('HELD','speculative','x',9)"), SQLITE_OK);   // reserves a low db_version, not committed
    for (int i = 0; i < 4; i++) { w[i].id = i; pthread_create(&th[i], NULL, writer, &w[i]); }
    for (int i = 0; i < 4; i++) pthread_join(th[i], NULL);
    CHECK(mw_scalar(a, "SELECT count(*) FROM docs") == 100);

    // while 'held' is unresolved, nothing above its reservation may be exported: the replica gets nothing yet
    int64_t sent = transfer(a, c);
    printf("export while a transaction holds its db_version: %lld bytes, replica rows=%lld\n", (long long)sent, (long long)mw_scalar(c, "SELECT count(*) FROM docs"));
    CHECK(mw_scalar(c, "SELECT count(*) FROM docs") == 0);
    CHECK(mw_scalar(c, "SELECT count(*) FROM docs WHERE id='HELD'") == 0);

    // it aborts: everything committed becomes exportable, nothing speculative ever left
    CHECK_RC(mw_exec(held, "ROLLBACK"), SQLITE_OK);
    close_cs(held);
    transfer(a, c);
    CHECK(mw_scalar(c, "SELECT count(*) FROM docs") == 100);
    CHECK(mw_scalar(c, "SELECT count(*) FROM docs WHERE id LIKE 'ABORTED%' OR id='HELD'") == 0);
    CHECK(strcmp(dump(a), dump(c)) == 0);

    // ---- stock replica edits (different columns of the same rows) and sends back to the lane database
    CHECK_RC(mw_exec(c, "UPDATE docs SET body='edited on stock' WHERE id LIKE 'w0-%'"), SQLITE_OK);
    CHECK_RC(mw_exec(a, "UPDATE docs SET title='edited on lane' WHERE id LIKE 'w0-%'"), SQLITE_OK);
    transfer(c, a);
    transfer(a, c);
    CHECK(strcmp(dump(a), dump(c)) == 0);
    CHECK(mw_scalar(a, "SELECT count(*) FROM docs WHERE id LIKE 'w0-%' AND body='edited on stock' AND title='edited on lane'") == 25);   // column-level merge

    // ---- lane database B <-> lane database A (two Multi-Writer replicas)
    CHECK_RC(mw_exec(b, "INSERT INTO docs VALUES('b1','from b','x',1),('shared','B title','B body',5)"), SQLITE_OK);
    CHECK_RC(mw_exec(a, "INSERT INTO docs VALUES('shared','A title','A body',7)"), SQLITE_OK);
    // (only *local* changes are exported: there is no relaying, as with a real backend in the middle,
    //  so every origin sends to every other replica)
    for (int round = 0; round < 2; round++) {
        transfer(a, b); transfer(a, c);
        transfer(b, a); transfer(b, c);
        transfer(c, a); transfer(c, b);
    }
    CHECK(mw_scalar(b, "SELECT count(*) FROM docs") == 102);
    CHECK(strcmp(dump(a), dump(b)) == 0);
    CHECK(strcmp(dump(a), dump(c)) == 0);                                      // three replicas, one state

    close_cs(a); close_cs(b); close_cs(c);
    mw_rmdb(p1); mw_rmdb(p2); mw_rmdb(p3);
    MW_DONE();
}
