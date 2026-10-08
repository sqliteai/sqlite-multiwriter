// Phase 14: ROWID / INTEGER PRIMARY KEY / AUTOINCREMENT under concurrent writers.
// Concurrent transactions independently allocate the same "next" rowid from their snapshots. The
// guarantee tested: an id that was returned to an application (RETURNING / last_insert_rowid()) for a
// *successfully committed* insert is never reused and never merged with another insert; the losing
// transaction fails retryably and its retry gets a fresh id. Rows of a rolled-back/failed attempt vanish.
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
static mw_db_stats stats (sqlite3 *db) { mw_db_stats s; memset(&s, 0, sizeof s); sqlite3_file_control(db, "main", MW_FCNTL_DBSTATS, &s); return s; }

#define NT 12
#define PER 10
static const char *g_path, *g_table;
typedef struct { int worker; int64_t id[PER]; int ok, retries, bad_last_rowid; } worker_t;

static void *worker (void *arg) {
    worker_t *w = arg;
    sqlite3 *db;
    open_lane(g_path, &db);
    char sql[200];
    snprintf(sql, sizeof sql, "INSERT INTO %s(v) VALUES(?1) RETURNING id", g_table);
    for (int i = 0; i < PER; i++) {
        char val[32]; snprintf(val, sizeof val, "w%d-%d", w->worker, i);
        for (int attempt = 0; attempt < 1000; attempt++) {
            sqlite3_stmt *st; int64_t got = -1;
            sqlite3_prepare_v2(db, sql, -1, &st, NULL);
            sqlite3_bind_text(st, 1, val, -1, SQLITE_STATIC);
            int rc;
            while ((rc = sqlite3_step(st)) == SQLITE_ROW) got = sqlite3_column_int64(st, 0);
            int64_t lri = sqlite3_last_insert_rowid(db);
            sqlite3_finalize(st);
            if (rc == SQLITE_DONE) {
                if (got != lri) w->bad_last_rowid++;               // RETURNING and last_insert_rowid() must agree
                w->id[w->ok++] = got;
                break;
            }
            w->retries++;
        }
    }
    close_cs(db);
    return NULL;
}

static void run (const char *table, int tracked) {
    char path[256];
    mw_tmpdb(path, sizeof path, "rowid");
    g_path = path; g_table = table;
    sqlite3 *s;
    CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, MW_PLAIN_VFS), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL;"
        "CREATE TABLE tr(id INTEGER PRIMARY KEY, v TEXT); CREATE TABLE ta(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT);"
        "CREATE TABLE tu(id INTEGER PRIMARY KEY, v TEXT);"
        "INSERT INTO tr(v) VALUES('seed'); INSERT INTO ta(v) VALUES('seed'); INSERT INTO tu(v) VALUES('seed');"), SQLITE_OK);
    CHECK_RC(close_cs(s), SQLITE_OK);

    sqlite3 *a;
    CHECK_RC(open_lane(path, &a), SQLITE_OK);
    pthread_t th[NT]; worker_t w[NT];
    for (int i = 0; i < NT; i++) { memset(&w[i], 0, sizeof w[i]); w[i].worker = i; pthread_create(&th[i], NULL, worker, &w[i]); }
    int ok = 0, retries = 0, bad = 0;
    for (int i = 0; i < NT; i++) { pthread_join(th[i], NULL); ok += w[i].ok; retries += w[i].retries; bad += w[i].bad_last_rowid; }
    mw_db_stats st = stats(a);
    printf("%-3s (%s): inserts=%d retries=%d rebases=%llu\n", table, tracked ? "tracked" : "untracked", ok, retries, (unsigned long long)st.rebases);
    CHECK(ok == NT * PER);
    CHECK(bad == 0);

    char q[300];
    snprintf(q, sizeof q, "SELECT count(*) FROM %s", table);
    CHECK(mw_scalar(a, q) == NT * PER + 1);                                     // + the seed row
    snprintf(q, sizeof q, "SELECT count(DISTINCT id) FROM %s", table);
    CHECK(mw_scalar(a, q) == NT * PER + 1);
    // every id returned to an application is still bound to the value it was returned with
    int mismatched = 0;
    for (int i = 0; i < NT; i++) for (int j = 0; j < w[i].ok; j++) {
        char val[32]; snprintf(val, sizeof val, "w%d-%d", i, j);
        snprintf(q, sizeof q, "SELECT count(*) FROM %s WHERE id=%lld AND v='%s'", table, (long long)w[i].id[j], val);
        if (mw_scalar(a, q) != 1) mismatched++;
    }
    CHECK(mismatched == 0);
    if (!strcmp(table, "ta")) {
        CHECK(mw_scalar(a, "SELECT seq FROM sqlite_sequence WHERE name='ta'") == mw_scalar(a, "SELECT max(id) FROM ta"));
    }
    CHECK(mw_scalar(a, "PRAGMA integrity_check") != -1);
    close_cs(a);
    mw_rmdb(path);
}

int main (void) {

    // rollback after a generated id: the id is simply reused, nothing leaks
    {
        char path[256]; mw_tmpdb(path, sizeof path, "rowid0");
        sqlite3 *s, *a;
        sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, MW_PLAIN_VFS);
        mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE tr(id INTEGER PRIMARY KEY, v TEXT)");
        close_cs(s);
        open_lane(path, &a);
        mw_exec(a, "BEGIN; INSERT INTO tr(v) VALUES('x')");
        int64_t id1 = sqlite3_last_insert_rowid(a);
        mw_exec(a, "ROLLBACK");
        mw_exec(a, "INSERT INTO tr(v) VALUES('y')");
        CHECK(id1 == 1 && sqlite3_last_insert_rowid(a) == 1);
        CHECK(mw_scalar(a, "SELECT count(*) FROM tr") == 1);
        close_cs(a); mw_rmdb(path);
    }

    run("tr", 1);       // tracked INTEGER PRIMARY KEY: rebase + collision detection
    run("ta", 1);       // tracked AUTOINCREMENT: sqlite_sequence is a hot untracked page => retryable, never rebased
    run("tu", 0);       // untracked: no logical representation => retryable
    MW_DONE();
}
