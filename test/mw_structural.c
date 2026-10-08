// Phase 13: structural conflicts. Concurrent transactions force B-tree splits, index page changes,
// page allocation, overflow chains, deletions (page merges, freelist) on the same pages. Conflicting
// transactions are rebased by logical replay; stock SQLite regenerates valid structure, which
// PRAGMA integrity_check (tables *and* indexes) verifies.
#include <pthread.h>
#include "mw_test.h"
#include "multiwriter.h"

static int close_cs (sqlite3 *db) { return sqlite3_close(db); }
static int open_lane (const char *path, sqlite3 **db) {
    char uri[300];
    snprintf(uri, sizeof uri, "file:%s?mw=2&mw_gc=32&mw_readcheck=0", path);   // structural rebases are the subject here
    int rc = sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
    if (rc == SQLITE_OK) sqlite3_extended_result_codes(*db, 1);
    return rc;
}
static mw_db_stats stats (sqlite3 *db) { mw_db_stats s; memset(&s, 0, sizeof s); sqlite3_file_control(db, "main", MW_FCNTL_DBSTATS, &s); return s; }
static char *icheck (sqlite3 *db) {
    static char buf[256]; buf[0] = 0;
    sqlite3_stmt *st;
    if (sqlite3_prepare_v2(db, "PRAGMA integrity_check", -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW) snprintf(buf, sizeof buf, "%s", (const char *)sqlite3_column_text(st, 0));
    sqlite3_finalize(st);
    return buf;
}

static const char *g_path;
typedef struct { int id, rounds, ok, err; } worker_t;

// each round: one transaction inserting rows of very different sizes (small, page-filling, overflow), an
// index-affecting update, and deleting some of its own older rows
static void *worker (void *arg) {
    worker_t *w = arg;
    sqlite3 *db;
    if (open_lane(g_path, &db) != SQLITE_OK) { w->err = w->rounds; return NULL; }
    for (int r = 0; r < w->rounds; r++) {
        char sql[1500];
        int base = w->id * 100000 + r * 10;
        snprintf(sql, sizeof sql,
            "BEGIN;"
            "INSERT INTO docs VALUES('d%d','small %d',%d,zeroblob(30));"
            "INSERT INTO docs VALUES('d%d','medium',%d,zeroblob(1800));"
            "INSERT INTO docs VALUES('d%d','overflow %d',%d,zeroblob(%d));"
            "UPDATE docs SET title='moved-'||title WHERE id='d%d';"
            "DELETE FROM docs WHERE id='d%d';"
            "COMMIT;",
            base, r, w->id, base + 1, w->id, base + 2, r, w->id, 6000 + (r * 977) % 30000, base, base - 10 >= w->id * 100000 ? base - 10 : -1);
        int rc = SQLITE_BUSY; long us = 1;                                 // (a conflicting transaction is refused: the application retries it, patiently)
        for (int a = 0; a < 100000 && (rc & 0xff) == SQLITE_BUSY; a++) {
            rc = mw_exec(db, sql);
            if (rc != SQLITE_OK && !sqlite3_get_autocommit(db)) mw_exec(db, "ROLLBACK");
            if ((rc & 0xff) == SQLITE_BUSY && a > 4) { struct timespec ts = { 0, us * 1000 }; nanosleep(&ts, NULL); if (us < 1000) us *= 2; }
        }
        if (rc == SQLITE_OK) w->ok++; else w->err++;
    }
    close_cs(db);
    return NULL;
}

int main (void) {
    char path[256];
    mw_tmpdb(path, sizeof path, "struct");
    g_path = path;
    sqlite3 *s;
    CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, MW_PLAIN_VFS), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE docs(id TEXT PRIMARY KEY NOT NULL, title TEXT, owner INTEGER, body BLOB);"
                        "CREATE INDEX docs_title ON docs(title); CREATE INDEX docs_owner ON docs(owner);"), SQLITE_OK);
    // seed so that splits/merges happen inside an existing tree, not only at the right edge
    CHECK_RC(mw_exec(s, "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<300) INSERT INTO docs SELECT 'seed'||printf('%04d',i), 'seed title '||i, 0, zeroblob(200) FROM n"), SQLITE_OK);
    CHECK_RC(close_cs(s), SQLITE_OK);

    sqlite3 *a;
    CHECK_RC(open_lane(path, &a), SQLITE_OK);
    int64_t pages0 = mw_scalar(a, "PRAGMA page_count");
    enum { NT = 6, ROUNDS = 25 };
    pthread_t th[NT]; worker_t w[NT];
    for (int i = 0; i < NT; i++) { w[i] = (worker_t){ .id = i + 1, .rounds = ROUNDS }; pthread_create(&th[i], NULL, worker, &w[i]); }
    int ok = 0, err = 0;
    for (int i = 0; i < NT; i++) { pthread_join(th[i], NULL); ok += w[i].ok; err += w[i].err; }
    mw_db_stats st = stats(a);
    printf("structural: ok=%d err=%d rebases=%llu retries=%llu max_attempts=%llu page_conflicts=%llu pages %lld -> %lld\n", ok, err,
           (unsigned long long)st.rebases, (unsigned long long)st.rebase_retries, (unsigned long long)st.rebase_max_attempts,
           (unsigned long long)st.page_conflicts, (long long)pages0, (long long)mw_scalar(a, "PRAGMA page_count"));
    CHECK(err == 0 && ok == NT * ROUNDS);
    CHECK(st.page_conflicts > 0);                                      // (the workload does conflict)

    // expected rows: per worker, each round inserts 3 rows and deletes at most one of its own previous first rows
    // (round r deletes row (r*10-10) which existed for r>=1) and moves one title: check by construction
    int64_t rows = mw_scalar(a, "SELECT count(*) FROM docs");
    int64_t expected = 300 + NT * (ROUNDS * 3 - (ROUNDS - 1));
    CHECK(rows == expected);
    CHECK(mw_scalar(a, "SELECT count(*) FROM docs WHERE title LIKE 'moved-%'") > 0);
    // indexes agree with the table (integrity_check covers it; also probe through each index explicitly)
    CHECK(mw_scalar(a, "SELECT count(*) FROM docs INDEXED BY docs_title") == rows);
    CHECK(mw_scalar(a, "SELECT count(*) FROM docs INDEXED BY docs_owner") == rows);
    CHECK(strcmp(icheck(a), "ok") == 0);
    CHECK(mw_scalar(a, "PRAGMA page_count") > pages0);                 // the file grew through the store: allocations happened
    // sqlite-sync metadata is consistent with the base table
    close_cs(a);
    mw_rmdb(path);
    MW_DONE();
}
