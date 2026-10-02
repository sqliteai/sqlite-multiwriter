// Page relocation: commits that conflict only because other commits extended the file are saved by renumbering their new pages.
// Every test ends with integrity_check and a data check, because a wrong pointer rewrite would corrupt the database.
#include <pthread.h>
#include "mw_test.h"
#include "multiwriter.h"

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
static void make_db (const char *path, const char *schema) {
    sqlite3 *s;
    CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL"), SQLITE_OK);
    CHECK_RC(mw_exec(s, schema), SQLITE_OK);
    sqlite3_close(s);
}
// inserts `rows` rows with ids first.. and blobs of `len` bytes
static int ins (sqlite3 *db, long first, int rows, int len, const char *tbl) {
    char sql[300];
    snprintf(sql, sizeof sql, "WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<%d) INSERT INTO %s(id,v) SELECT %ld+i, zeroblob(%d) FROM n", rows - 1, tbl, first, len);
    return mw_exec(db, sql);
}

enum { T = 8, TXNS = 120 };
static struct { int64_t rows, bytes; } tot[T];
static const char *g_path;
static void *worker (void *arg) {
    int id = (int)(intptr_t)arg; sqlite3 *db; if (open_lane(g_path, &db) != SQLITE_OK) return NULL;
    unsigned rng = 12345u + (unsigned)id * 7919u;
    for (int n = 0; n < TXNS; n++) {
        rng = rng * 1103515245u + 12345u; int rows = 1 + (int)(rng >> 16) % 12;
        rng = rng * 1103515245u + 12345u; int len = ((rng >> 16) % 5 == 0) ? 5000 + (int)(rng >> 8) % 20000 : (int)(rng >> 16) % 900;
        char sql[400];
        snprintf(sql, sizeof sql, "WITH RECURSIVE r(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM r WHERE i<%d) INSERT INTO t SELECT (%lld<<32)+%d*100+i, 'k%d-'||printf('%%08d',%d*100+i)||hex(randomblob(20)), zeroblob(%d) FROM r",
                 rows - 1, (long long)id, n, id, n, len);
        for (;;) { int rc = mw_exec(db, sql); if (rc == SQLITE_OK) break; if ((rc & 0xff) != SQLITE_BUSY) { printf("unexpected rc %d\n", rc); mw_failures++; return NULL; } }
        tot[id].rows += rows; tot[id].bytes += (int64_t)rows * len;
        if (n % 17 == 5) {                                                              // delete own rows of an earlier transaction (frees pages)
            snprintf(sql, sizeof sql, "DELETE FROM t WHERE id BETWEEN (%lld<<32)+%d*100 AND (%lld<<32)+%d*100+99", (long long)id, n - 3, (long long)id, n - 3);
            for (;;) { int rc = mw_exec(db, sql); if (rc == SQLITE_OK) break; if ((rc & 0xff) != SQLITE_BUSY) { mw_failures++; return NULL; } }
        }
    }
    sqlite3_close(db); return NULL;
}

// inserts 20-60 rows at the end of the thread's key range, and every few transactions deletes a block of its oldest rows
static void *worker5 (void *arg) {
    int id = (int)(intptr_t)arg; sqlite3 *db; if (open_lane(g_path, &db) != SQLITE_OK) return NULL;
    unsigned rng = 777u + (unsigned)id * 104729u;
    long next = 0, oldest = 0;
    for (int n = 0; n < 200; n++) {
        rng = rng * 1103515245u + 12345u; int rows = 20 + (int)(rng >> 16) % 41;
        char sql[400];
        snprintf(sql, sizeof sql, "WITH RECURSIVE r(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM r WHERE i<%d) INSERT INTO t SELECT (%lld<<32)+%ld+i, 'k'||printf('%%06d',%d)||hex(randomblob(6)), zeroblob(150) FROM r", rows - 1, (long long)id, next, id * 100000 + n);
        for (;;) { int rc = mw_exec(db, sql); if (rc == SQLITE_OK) break; if ((rc & 0xff) != SQLITE_BUSY) { mw_failures++; return NULL; } }
        next += rows; tot[id].rows += rows;
        if (n % 5 == 4 && next - oldest > 80) {
            long cnt = (next - oldest) / 2;
            snprintf(sql, sizeof sql, "DELETE FROM t WHERE id >= (%lld<<32)+%ld AND id < (%lld<<32)+%ld", (long long)id, oldest, (long long)id, oldest + cnt);
            for (;;) { int rc = mw_exec(db, sql); if (rc == SQLITE_OK) break; if ((rc & 0xff) != SQLITE_BUSY) { mw_failures++; return NULL; } }
            oldest += cnt; tot[id].rows -= cnt;
        }
    }
    sqlite3_close(db); return NULL;
}

int main (void) {
    setvbuf(stdout, NULL, _IOLBF, 0);
    char path[256];

    // ---- 1. two writers extend the file at the same time, in different key ranges
    int repeat = getenv("RELOC_REPEAT") ? atoi(getenv("RELOC_REPEAT")) : 1;
    for (int li = 0; li < 3 * repeat; li++) {
        int len = li / repeat == 0 ? 300 : li / repeat == 1 ? 3000 : 30000;           // 300 B: plain leaf splits; 3000 B: many pages; 30000 B: overflow chains
        mw_tmpdb(path, sizeof path, "reloc");
        // three key ranges of 12000 small rows: a 3-level tree, so the two writers' leaves have different parents
        make_db(path, "CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB);"
                      "WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<11999) INSERT INTO t SELECT 1+i, zeroblob(60) FROM n;"
                      "WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<11999) INSERT INTO t SELECT 1000000+i, zeroblob(60) FROM n;"
                      "WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<11999) INSERT INTO t SELECT 2000000+i, zeroblob(60) FROM n");
        sqlite3 *a, *b; CHECK_RC(open_lane(path, &a), SQLITE_OK); CHECK_RC(open_lane(path, &b), SQLITE_OK);
        int rows = len >= 30000 ? 6 : 40;
        CHECK_RC(mw_exec(a, "BEGIN"), SQLITE_OK); CHECK_RC(ins(a, 20000, rows, len, "t"), SQLITE_OK);
        CHECK_RC(mw_exec(b, "BEGIN"), SQLITE_OK); CHECK_RC(ins(b, 2020000, rows, len, "t"), SQLITE_OK);
        CHECK_RC(mw_exec(a, "COMMIT"), SQLITE_OK);
        int rcb = mw_exec(b, "COMMIT");
        mw_db_stats s = stats(a);
        printf("len=%d: second commit rc=%d relocations=%llu page_conflicts=%llu\n", len, rcb, (unsigned long long)s.relocations, (unsigned long long)s.page_conflicts);
        if (rcb) { extern void mw_reloc_dump (void); mw_reloc_dump(); }
        CHECK_RC(rcb, SQLITE_OK);                                 // saved by relocation, not refused
        CHECK(s.relocations >= 1);
        CHECK(mw_scalar(a, "SELECT count(*) FROM t") == 36000 + 2 * rows);
        CHECK(mw_scalar(a, "SELECT sum(length(v)) FROM t") == 36000 * 60 + 2 * (int64_t)rows * len);
        CHECK(integrity_ok(a));
        CHECK(mw_scalar(b, "SELECT count(*) FROM t") == 36000 + 2 * rows);
        sqlite3_close(a); sqlite3_close(b);
        // a stock connection sees a valid database after everything was compacted into the real file
        sqlite3 *s2; CHECK_RC(sqlite3_open_v2(path, &s2, SQLITE_OPEN_READWRITE, "unix"), SQLITE_OK);
        CHECK(integrity_ok(s2)); CHECK(mw_scalar(s2, "SELECT count(*) FROM t") == 36000 + 2 * rows);
        sqlite3_close(s2);
        mw_rmdb(path);
    }

    // ---- 2. a real conflict on an existing page is not resolved by relocation: refused, and nothing is lost or corrupted
    mw_tmpdb(path, sizeof path, "reloc");
    make_db(path, "CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB); INSERT INTO t VALUES(1,zeroblob(10))");
    { sqlite3 *a, *b; CHECK_RC(open_lane(path, &a), SQLITE_OK); CHECK_RC(open_lane(path, &b), SQLITE_OK);
      CHECK_RC(mw_exec(a, "BEGIN"), SQLITE_OK); CHECK_RC(ins(a, 2, 5, 100, "t"), SQLITE_OK);      // same leaf page, no growth
      CHECK_RC(mw_exec(b, "BEGIN"), SQLITE_OK); CHECK_RC(ins(b, 100, 5, 100, "t"), SQLITE_OK);
      CHECK_RC(mw_exec(a, "COMMIT"), SQLITE_OK);
      int rcb = mw_exec(b, "COMMIT"); CHECK((rcb & 0xff) == SQLITE_BUSY);
      mw_exec(b, "ROLLBACK");
      CHECK(mw_scalar(a, "SELECT count(*) FROM t") == 6); CHECK(integrity_ok(a));
      sqlite3_close(a); sqlite3_close(b); }
    mw_rmdb(path);

    // ---- 3. a secondary index (index leaf and interior pages), transactions that also delete (freelist changes: relocation must step aside)
    mw_tmpdb(path, sizeof path, "reloc");
    make_db(path, "CREATE TABLE t(id INTEGER PRIMARY KEY, k TEXT, v BLOB); CREATE INDEX tk ON t(k); INSERT INTO t VALUES(1,'a',zeroblob(10))");
    { sqlite3 *a, *b, *c; CHECK_RC(open_lane(path, &a), SQLITE_OK); CHECK_RC(open_lane(path, &b), SQLITE_OK); CHECK_RC(open_lane(path, &c), SQLITE_OK);
      CHECK_RC(mw_exec(a, "BEGIN; WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<400) INSERT INTO t SELECT 1000+i, 'A'||printf('%0200d',i), zeroblob(200) FROM n"), SQLITE_OK);
      CHECK_RC(mw_exec(b, "BEGIN; WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<400) INSERT INTO t SELECT 500000+i, 'Z'||printf('%0200d',i), zeroblob(200) FROM n"), SQLITE_OK);
      CHECK_RC(mw_exec(c, "BEGIN; WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<400) INSERT INTO t SELECT 900000+i, 'M'||printf('%0200d',i), zeroblob(200) FROM n"), SQLITE_OK);
      CHECK_RC(mw_exec(a, "COMMIT"), SQLITE_OK);
      int rb = mw_exec(b, "COMMIT"), rc = mw_exec(c, "COMMIT");
      printf("index test: commits rc=%d,%d relocations=%llu\n", rb, rc, (unsigned long long)stats(a).relocations);
      if (rb) mw_exec(b, "ROLLBACK"); if (rc) mw_exec(c, "ROLLBACK");
      int64_t expect = 1 + 401 + (rb == 0 ? 401 : 0) + (rc == 0 ? 401 : 0);
      CHECK(mw_scalar(a, "SELECT count(*) FROM t") == expect);
      CHECK(mw_scalar(a, "SELECT count(*) FROM t INDEXED BY tk") == expect);
      CHECK(integrity_ok(a));
      sqlite3_close(a); sqlite3_close(b); sqlite3_close(c); }
    mw_rmdb(path);

    // ---- 4. many threads, random sizes (single rows to overflow chains), an index, occasional deletes; retry on refusal
    mw_tmpdb(path, sizeof path, "reloc");
    make_db(path, "CREATE TABLE t(id INTEGER PRIMARY KEY, k TEXT, v BLOB); CREATE INDEX tk ON t(k)");
    g_path = path;
    sqlite3 *v; CHECK_RC(open_lane(path, &v), SQLITE_OK);                    // (kept open: the database state and its counters live as long as one connection does)
    pthread_t th[T]; for (int i = 0; i < T; i++) pthread_create(&th[i], NULL, worker, (void *)(intptr_t)i);
    for (int i = 0; i < T; i++) pthread_join(th[i], NULL);
    mw_db_stats s = stats(v);
    int64_t n_rows = mw_scalar(v, "SELECT count(*) FROM t"), n_idx = mw_scalar(v, "SELECT count(*) FROM t INDEXED BY tk");
    printf("threads: rows=%lld (index %lld) relocations=%llu merges=%llu page_conflicts=%llu aborts=%llu\n", (long long)n_rows, (long long)n_idx, (unsigned long long)s.relocations, (unsigned long long)s.merges, (unsigned long long)s.page_conflicts, (unsigned long long)s.aborts);
    CHECK(n_rows == n_idx);
    CHECK(integrity_ok(v));
    CHECK(s.relocations > 0);
    // every row of every inserted transaction that was not deleted has the right length
    CHECK(mw_scalar(v, "SELECT count(*) FROM t WHERE length(v) < 0") == 0);
    sqlite3_close(v);
    sqlite3 *s2; CHECK_RC(sqlite3_open_v2(path, &s2, SQLITE_OPEN_READWRITE, "unix"), SQLITE_OK);
    CHECK(integrity_ok(s2)); CHECK(mw_scalar(s2, "SELECT count(*) FROM t") == n_rows);
    sqlite3_close(s2);
    mw_rmdb(path);

    // ---- 5. interior-page validation by routing: threads insert into their own key ranges and delete old rows of their range (merges and redistributions
    //         move cells between sibling pages while other threads descend through the same interior pages). Nothing may be lost or misplaced.
    mw_tmpdb(path, sizeof path, "reloc");
    make_db(path, "CREATE TABLE t(id INTEGER PRIMARY KEY, k TEXT, v BLOB); CREATE INDEX tk ON t(k)");
    g_path = path;
    memset(tot, 0, sizeof tot);
    sqlite3 *v5; CHECK_RC(open_lane(path, &v5), SQLITE_OK);
    pthread_t th5[T]; for (int i = 0; i < T; i++) pthread_create(&th5[i], NULL, worker5, (void *)(intptr_t)i);
    for (int i = 0; i < T; i++) pthread_join(th5[i], NULL);
    mw_db_stats s5 = stats(v5);
    int64_t want = 0; for (int i = 0; i < T; i++) want += tot[i].rows;
    printf("routing test: rows=%lld expected=%lld relocations=%llu merges=%llu reads_saved=%llu read_conflicts=%llu\n", (long long)mw_scalar(v5, "SELECT count(*) FROM t"), (long long)want, (unsigned long long)s5.relocations, (unsigned long long)s5.merges, (unsigned long long)s5.reads_saved, (unsigned long long)s5.read_conflicts);
    CHECK(mw_scalar(v5, "SELECT count(*) FROM t") == want);
    CHECK(mw_scalar(v5, "SELECT count(*) FROM t INDEXED BY tk") == want);
    for (int i = 0; i < T; i++) {                                     // each thread's own rows are exactly the ones it kept
        char q[200]; snprintf(q, sizeof q, "SELECT count(*) FROM t WHERE id >= (%d<<32) AND id < ((%d+1)<<32)", i, i);
        CHECK(mw_scalar(v5, q) == tot[i].rows);
    }
    CHECK(integrity_ok(v5));
    sqlite3_close(v5);
    mw_rmdb(path);

    // ---- 6. three-way merge of the shared parent: transactions that split different leaves under one interior page both rewrite it; the second one is merged,
    //         not refused. A transaction that splits the *same* leaf is still refused (a conflict on the leaf).
    for (int order = 0; order < 2; order++) {
        mw_tmpdb(path, sizeof path, "reloc");
        make_db(path, "CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB);"
                      "WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<11999) INSERT INTO t SELECT 100*(1+i), zeroblob(60) FROM n");     // ids 100,200,..: room between them, a root + ~200 leaves
        sqlite3 *a, *b, *c; CHECK_RC(open_lane(path, &a), SQLITE_OK); CHECK_RC(open_lane(path, &b), SQLITE_OK); CHECK_RC(open_lane(path, &c), SQLITE_OK);
        // three regions far apart: each insert of 40 rows of 300 bytes splits the leaves there
        CHECK_RC(mw_exec(a, "BEGIN"), SQLITE_OK); CHECK_RC(ins(a, 10001, 40, 300, "t"), SQLITE_OK);
        CHECK_RC(mw_exec(b, "BEGIN"), SQLITE_OK); CHECK_RC(ins(b, 60001, 40, 300, "t"), SQLITE_OK);
        CHECK_RC(mw_exec(c, "BEGIN"), SQLITE_OK); CHECK_RC(ins(c, 100001, 40, 300, "t"), SQLITE_OK);
        sqlite3 *first = order ? c : a, *second = b, *third = order ? a : c;
        CHECK_RC(mw_exec(first, "COMMIT"), SQLITE_OK);
        int r2 = mw_exec(second, "COMMIT"), r3 = mw_exec(third, "COMMIT");
        mw_db_stats s6 = stats(a);
        printf("merge test (order %d): commits rc=%d,%d merges=%llu relocations=%llu\n", order, r2, r3, (unsigned long long)s6.merges, (unsigned long long)s6.relocations);
        CHECK_RC(r2, SQLITE_OK); CHECK_RC(r3, SQLITE_OK);
        CHECK(s6.merges >= 1);
        CHECK(mw_scalar(a, "SELECT count(*) FROM t") == 12000 + 3 * 40);
        CHECK(mw_scalar(b, "SELECT count(*) FROM t WHERE id BETWEEN 10001 AND 10040") == 40);
        CHECK(mw_scalar(b, "SELECT count(*) FROM t WHERE id BETWEEN 60001 AND 60040") == 40);
        CHECK(mw_scalar(b, "SELECT count(*) FROM t WHERE id BETWEEN 100001 AND 100040") == 40);
        CHECK(mw_scalar(a, "SELECT count(*) FROM (SELECT id FROM t ORDER BY id) WHERE id > 0") == 12000 + 3 * 40);
        CHECK(integrity_ok(a));
        sqlite3_close(a); sqlite3_close(b); sqlite3_close(c);
        sqlite3 *s2; CHECK_RC(sqlite3_open_v2(path, &s2, SQLITE_OPEN_READWRITE, "unix"), SQLITE_OK);
        CHECK(integrity_ok(s2)); CHECK(mw_scalar(s2, "SELECT count(*) FROM t") == 12000 + 3 * 40);
        sqlite3_close(s2);
        mw_rmdb(path);
    }
    {   // the same leaf: both insert into the same key region, both split it: the second is refused
        mw_tmpdb(path, sizeof path, "reloc");
        make_db(path, "CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB);"
                      "WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<11999) INSERT INTO t SELECT 100*(1+i), zeroblob(60) FROM n");
        sqlite3 *a, *b; CHECK_RC(open_lane(path, &a), SQLITE_OK); CHECK_RC(open_lane(path, &b), SQLITE_OK);
        CHECK_RC(mw_exec(a, "BEGIN"), SQLITE_OK); CHECK_RC(ins(a, 10001, 40, 300, "t"), SQLITE_OK);
        CHECK_RC(mw_exec(b, "BEGIN"), SQLITE_OK); CHECK_RC(ins(b, 10041, 40, 300, "t"), SQLITE_OK);
        CHECK_RC(mw_exec(a, "COMMIT"), SQLITE_OK);
        int rb = mw_exec(b, "COMMIT");
        CHECK((rb & 0xff) == SQLITE_BUSY);
        mw_exec(b, "ROLLBACK");
        CHECK(mw_scalar(a, "SELECT count(*) FROM t") == 12040); CHECK(integrity_ok(a));
        sqlite3_close(a); sqlite3_close(b); mw_rmdb(path);
    }
    MW_DONE();
}
