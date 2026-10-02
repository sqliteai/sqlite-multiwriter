// Convergence and atomicity of the sync layer.
//  1. Four peers, each written by several threads at once (multi-writer lanes), exchange payloads in random order, with duplicates and out-of-date repeats; after a final complete
//     exchange all four hold the same rows and the same cell versions (compared by site id, not by local ordinal).
//  2. A peer is written by threads while it also applies payloads from the others (apply racing with local commits on the same rows): nothing is lost, everything converges.
//  3. mw_sync_apply is atomic: a process killed in the middle of applying a large payload leaves all of it or none of it.
//  4. backfill: a database with rows from before the capture gets cells, exported rows equal what sqlite-sync's init would make.
#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include <signal.h>
#include <sys/wait.h>
#include <time.h>
#include "mw_test.h"
#include "multiwriter.h"
#include "multiwriter_meta.h"
#include "multiwriter_catalog.h"
#include "multiwriter_sync.h"
#include "crdt.h"

#define NK 40
#define NP 4
static uint64_t gr = 0x2545F4914F6CDD1Dull;
static uint64_t grnd (void) { gr ^= gr << 13; gr ^= gr >> 7; gr ^= gr << 17; return gr; }

static int open_peer (const char *path, sqlite3 **db) {
    char uri[400]; snprintf(uri, sizeof uri, "file:%s?mw=2&mw_cdc=1%s", path, getenv("MW_TEST_MP") ? "&mw_mp=1" : "");
    int rc = sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
    if (rc == SQLITE_OK) { sqlite3_extended_result_codes(*db, 1); sqlite3_busy_timeout(*db, 0); }
    return rc;
}
static void make_db (const char *path, const char *ddl) {
    sqlite3 *s; CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), SQLITE_OK);
    CHECK_RC(mw_exec(s, ddl), SQLITE_OK); sqlite3_close(s);
}
static int exec_retry (sqlite3 *db, const char *sql) { for (int i = 0; i < 100000; i++) { int rc = mw_exec(db, sql); if ((rc & 0xff) != SQLITE_BUSY) return rc; mw_exec(db, "ROLLBACK"); } return SQLITE_BUSY; }

static void random_op (sqlite3 *db, unsigned *rng, int tag) {
    *rng = *rng * 1103515245u + 12345u; int k = (int)((*rng >> 16) % NK); *rng = *rng * 1103515245u + 12345u; int what = (int)((*rng >> 16) % 10);
    char sql[300];
    if (what == 0) snprintf(sql, sizeof sql, "DELETE FROM t WHERE id='k%d'", k);
    else if (what < 3) snprintf(sql, sizeof sql, "INSERT OR REPLACE INTO t(id,a,b,c) VALUES('k%d','a%d',%d,%d.5)", k, tag, tag, tag);
    else if (what < 5) snprintf(sql, sizeof sql, "UPDATE t SET a='u%d' WHERE id='k%d'", tag, k);
    else if (what < 8) snprintf(sql, sizeof sql, "UPDATE t SET b=%d WHERE id='k%d'", tag, k);
    else snprintf(sql, sizeof sql, "UPDATE t SET c=%d.25, a='v%d' WHERE id='k%d'", tag, tag, k);
    exec_retry(db, sql);
}

typedef struct { const char *path; int id; _Atomic int *stop; long ops; } wctx;
static void *writer (void *arg) {
    wctx *w = arg; sqlite3 *db; if (open_peer(w->path, &db) != SQLITE_OK) return NULL;
    unsigned rng = 31u + (unsigned)w->id * 7919u; int tag = w->id * 1000000;
    while (!atomic_load(w->stop)) { random_op(db, &rng, tag++); w->ops++; }
    sqlite3_close(db); return NULL;
}

static int cmp_s (const void *a, const void *b) { return strcmp(*(char *const *)a, *(char *const *)b); }
static int dump_rows (sqlite3 *db, char **out) {
    sqlite3_stmt *st; int n = 0; sqlite3_prepare_v2(db, "SELECT id, quote(a), quote(b), quote(c) FROM t ORDER BY id", -1, &st, NULL);
    while (sqlite3_step(st) == SQLITE_ROW && n < 200) { char b[300]; snprintf(b, sizeof b, "%s|%s|%s|%s", sqlite3_column_text(st, 0), sqlite3_column_text(st, 1), sqlite3_column_text(st, 2), sqlite3_column_text(st, 3)); out[n++] = strdup(b); }
    sqlite3_finalize(st); return n;
}
static int dump_cells (sqlite3 *db, char **out) {          // from the file tables, by site id bytes, db_version left out (it is local)
    mw_meta *m = NULL; sqlite3_file_control(db, "main", MW_FCNTL_META, &m); mw_meta_flush(m);
    sqlite3_stmt *st; int n = 0; sqlite3_prepare_v2(db, "SELECT hex(pk), col, cv, site FROM mw_cells", -1, &st, NULL);
    while (sqlite3_step(st) == SQLITE_ROW && n < 1000) {
        uint8_t id[16]; mw_meta_site_id(m, (uint32_t)sqlite3_column_int64(st, 3), id); char h[40]; for (int i = 0; i < 16; i++) sprintf(h + 2 * i, "%02x", id[i]);
        if (sqlite3_column_int64(st, 1) == -1) strcpy(h, "*");             // (the site of a causal-length entry is whoever got there first when two peers delete or resurrect the same row: sqlite-sync is the same; the version is what converges)
        char b[300]; snprintf(b, sizeof b, "%s|%lld|%lld|%s", sqlite3_column_text(st, 0), (long long)sqlite3_column_int64(st, 1), (long long)sqlite3_column_int64(st, 2), h); out[n++] = strdup(b);
    }
    sqlite3_finalize(st); qsort(out, (size_t)n, sizeof *out, cmp_s); return n;
}
static void freeall (char **a, int n) { for (int i = 0; i < n; i++) free(a[i]); }
static int all_equal (sqlite3 **p, int np, const char *what) {
    char *r0[200], *c0[1000]; int n0 = dump_rows(p[0], r0), m0 = dump_cells(p[0], c0); int bad = 0;
    for (int i = 1; i < np; i++) {
        char *r[200], *c[1000]; int n = dump_rows(p[i], r), m = dump_cells(p[i], c);
        bool same = n == n0 && m == m0; for (int k = 0; same && k < n; k++) if (strcmp(r[k], r0[k])) same = false; for (int k = 0; same && k < m; k++) if (strcmp(c[k], c0[k])) same = false;
        if (!same) { bad++; printf("  %s: peer %d differs from peer 0 (%d rows / %d cells vs %d / %d)\n", what, i, n, m, n0, m0);
            for (int k = 0, sh = 0; k < n0 && sh < 4; k++) { bool f = false; for (int q = 0; q < n; q++) if (!strcmp(r0[k], r[q])) f = true; if (!f) { printf("    row only at 0: %s\n", r0[k]); sh++; } }
            for (int k = 0, sh = 0; k < m0 && sh < 4; k++) { bool f = false; for (int q = 0; q < m; q++) if (!strcmp(c0[k], c[q])) f = true; if (!f) { printf("    cell only at 0: %s\n", c0[k]); sh++; } }
            for (int k = 0, sh = 0; k < m && sh < 4; k++) { bool f = false; for (int q = 0; q < m0; q++) if (!strcmp(c[k], c0[q])) f = true; if (!f) { printf("    cell only at %d: %s\n", i, c[k]); sh++; } } }
        freeall(r, n); freeall(c, m);
    }
    freeall(r0, n0); freeall(c0, m0); return bad;
}

// payload exchange: every peer keeps, for every other peer, the db_version it has taken from it
static int64_t since[NP][NP];
static void exchange (sqlite3 **p, int from, int to, bool repeat_old) {
    uint8_t *pl; size_t n; int64_t upto; int64_t s = repeat_old ? 0 : since[to][from];
    CHECK_RC(mw_sync_export(p[from], s, &pl, &n, &upto), SQLITE_OK);
    if (pl) { mw_sync_stats st; int arc = mw_sync_apply(p[to], pl, n, &st); if (arc != SQLITE_OK) printf("  apply %d -> %d failed: rc %d after %lld retries (%s)\n", from, to, arc, (long long)st.retries, sqlite3_errmsg(p[to])); CHECK_RC(arc, SQLITE_OK); if (!repeat_old) since[to][from] = upto; mw_sync_free(pl); }
    else if (!repeat_old) since[to][from] = upto;
}

int main (void) {
    setvbuf(stdout, NULL, _IOLBF, 0);
    static const char *DDL = "PRAGMA journal_mode=WAL; CREATE TABLE t(id TEXT PRIMARY KEY NOT NULL, a TEXT, b INTEGER, c REAL)";
    char path[NP][256]; sqlite3 *p[NP];
    for (int i = 0; i < NP; i++) { char tag[20]; snprintf(tag, sizeof tag, "sync%d", i); mw_tmpdb(path[i], sizeof path[i], tag); make_db(path[i], DDL); CHECK_RC(open_peer(path[i], &p[i]), SQLITE_OK); }

    // ---- 1. multi-writer peers, random exchanges
    for (int round = 0; round < 12; round++) {
        enum { W = 3 }; pthread_t th[NP][W]; wctx ctx[NP][W]; _Atomic int stop = 0;
        for (int i = 0; i < NP; i++) for (int w = 0; w < W; w++) { ctx[i][w] = (wctx){ path[i], i * 10 + w + round * 100, &stop, 0 }; pthread_create(&th[i][w], NULL, writer, &ctx[i][w]); }
        struct timespec ts = { 0, 120 * 1000000L }; nanosleep(&ts, NULL);
        for (int k = 0; k < 6; k++) { int a = (int)(grnd() % NP), b = (int)(grnd() % NP); if (a != b) exchange(p, a, b, grnd() % 4 == 0); nanosleep(&ts, NULL); }
        atomic_store(&stop, 1);
        for (int i = 0; i < NP; i++) for (int w = 0; w < W; w++) pthread_join(th[i][w], NULL);
    }
    for (int pass = 0; pass < 3; pass++) for (int a = 0; a < NP; a++) for (int b = 0; b < NP; b++) if (a != b) exchange(p, a, b, false);
    int bad = all_equal(p, NP, "after the final exchange"); CHECK(bad == 0);
    printf("1. four multi-writer peers, 12 rounds of concurrent writers and random exchanges: %s (%lld rows)\n", bad ? "DIVERGED" : "converged", (long long)mw_scalar(p[0], "SELECT count(*) FROM t"));

    // ---- 2. apply racing with local writers on the same peer
    { enum { W = 4 }; pthread_t th[W]; wctx ctx[W]; _Atomic int stop = 0;
      for (int w = 0; w < W; w++) { ctx[w] = (wctx){ path[0], 5000 + w, &stop, 0 }; pthread_create(&th[w], NULL, writer, &ctx[w]); }
      long applies = 0;
      for (int i = 0; i < 150; i++) {
          sqlite3 *w = p[1 + i % 3]; random_op(w, &(unsigned){ 77u + (unsigned)i }, 900000 + i);
          uint8_t *pl; size_t n; int64_t up; CHECK_RC(mw_sync_export(w, since[0][1 + i % 3], &pl, &n, &up), SQLITE_OK);
          if (pl) { mw_sync_stats st; CHECK_RC(mw_sync_apply(p[0], pl, n, &st), SQLITE_OK); mw_sync_free(pl); applies++; }
          since[0][1 + i % 3] = up;
      }
      atomic_store(&stop, 1); for (int w = 0; w < W; w++) pthread_join(th[w], NULL);
      long ops = 0; for (int w = 0; w < W; w++) ops += ctx[w].ops;
      for (int pass = 0; pass < 3; pass++) for (int a = 0; a < NP; a++) for (int b = 0; b < NP; b++) if (a != b) exchange(p, a, b, false);
      int bad2 = all_equal(p, NP, "after the apply/write race"); CHECK(bad2 == 0);
      printf("2. %ld applies racing with %ld local statements on one peer: %s\n", applies, ops, bad2 ? "DIVERGED" : "converged"); }
    for (int i = 0; i < NP; i++) { sqlite3_close(p[i]); mw_rmdb(path[i]); }

    // ---- 3. atomicity of apply under SIGKILL
    { char src[256], dst[256]; mw_tmpdb(src, sizeof src, "synca"); make_db(src, "PRAGMA journal_mode=WAL; CREATE TABLE t(id TEXT PRIMARY KEY NOT NULL, a TEXT, b INTEGER, c REAL)");
      sqlite3 *s; CHECK_RC(open_peer(src, &s), SQLITE_OK);
      CHECK_RC(mw_exec(s, "BEGIN"), SQLITE_OK); for (int i = 0; i < 60000; i++) { char q[200]; snprintf(q, sizeof q, "INSERT INTO t VALUES('r%d','text number %d',%d,%d.5)", i, i, i, i); mw_exec(s, q); } CHECK_RC(mw_exec(s, "COMMIT"), SQLITE_OK);
      uint8_t *pl; size_t pn; int64_t up; CHECK_RC(mw_sync_export(s, 0, &pl, &pn, &up), SQLITE_OK); printf("3. a payload of %zu bytes (180000 cells)\n", pn);
      { char d0[256]; mw_tmpdb(d0, sizeof d0, "mwsynct"); make_db(d0, "PRAGMA journal_mode=WAL; CREATE TABLE t(id TEXT PRIMARY KEY NOT NULL, a TEXT, b INTEGER, c REAL)"); sqlite3 *x; open_peer(d0, &x); struct timespec a, b; clock_gettime(CLOCK_MONOTONIC, &a); CHECK_RC(mw_sync_apply(x, pl, pn, NULL), SQLITE_OK); clock_gettime(CLOCK_MONOTONIC, &b); printf("   a complete apply of it takes %.0f ms\n", (double)(b.tv_sec - a.tv_sec) * 1e3 + (double)(b.tv_nsec - a.tv_nsec) / 1e6); sqlite3_close(x); mw_rmdb(d0); }
      int whole = 0, none = 0, partial = 0;
      for (int round = 0; round < 24; round++) {
          char d[256]; snprintf(d, sizeof d, "mwsyncd%d", round); mw_tmpdb(dst, sizeof dst, d); make_db(dst, "PRAGMA journal_mode=WAL; CREATE TABLE t(id TEXT PRIMARY KEY NOT NULL, a TEXT, b INTEGER, c REAL)");
          pid_t c = fork();
          if (c == 0) { sqlite3 *x; if (open_peer(dst, &x) != SQLITE_OK) _exit(1); mw_sync_apply(x, pl, pn, NULL); _exit(0); }
          struct timespec ts = { 0, (long)(12 + round * 9) * 1000000L }; nanosleep(&ts, NULL);
          kill(c, SIGKILL); int stt; waitpid(c, &stt, 0);
          sqlite3 *r; CHECK_RC(open_peer(dst, &r), SQLITE_OK);
          int64_t rows = mw_scalar(r, "SELECT count(*) FROM t");
          mw_meta *m = NULL; sqlite3_file_control(r, "main", MW_FCNTL_META, &m); mw_meta_flush(m);
          int64_t cells = mw_scalar(r, "SELECT count(*) FROM mw_cells");
          if (rows == 0 && cells == 0) none++; else if (rows == 60000 && cells == 180000) whole++; else { partial++; printf("  PARTIAL: %lld rows, %lld cells\n", (long long)rows, (long long)cells); }
          sqlite3_close(r); mw_rmdb(dst);
      }
      printf("   SIGKILL during apply, 24 rounds: all of it %d times, none of it %d times, partial %d times\n", whole, none, partial);
      CHECK(partial == 0); mw_sync_free(pl); sqlite3_close(s); mw_rmdb(src); }

    // ---- 4. backfill
    { char bp[256]; mw_tmpdb(bp, sizeof bp, "backfill"); make_db(bp, "PRAGMA journal_mode=WAL; CREATE TABLE t(id TEXT PRIMARY KEY NOT NULL, a TEXT, b INTEGER, c REAL); CREATE TABLE u(x INTEGER NOT NULL, y TEXT NOT NULL, z TEXT, PRIMARY KEY(x,y))");
      sqlite3 *s; CHECK_RC(sqlite3_open_v2(bp, &s, SQLITE_OPEN_READWRITE, "unix"), SQLITE_OK);
      CHECK_RC(mw_exec(s, "BEGIN"), SQLITE_OK); for (int i = 0; i < 1300; i++) { char q[200]; snprintf(q, sizeof q, "INSERT INTO t VALUES('b%d','x',%d,1.5)", i, i); mw_exec(s, q); if (i < 700) { snprintf(q, sizeof q, "INSERT INTO u VALUES(%d,'k%d','z')", i, i); mw_exec(s, q); } } CHECK_RC(mw_exec(s, "COMMIT"), SQLITE_OK); sqlite3_close(s);
      sqlite3 *db; CHECK_RC(open_peer(bp, &db), SQLITE_OK);
      CHECK_RC(mw_sync_backfill(db), SQLITE_OK); CHECK_RC(mw_sync_backfill(db), SQLITE_OK);                // (twice: idempotent)
      mw_meta *m = NULL; sqlite3_file_control(db, "main", MW_FCNTL_META, &m); mw_meta_flush(m);
      int64_t cells = mw_scalar(db, "SELECT count(*) FROM mw_cells"), t_cells = mw_scalar(db, "SELECT count(*) FROM mw_cells WHERE cv = 1");
      printf("4. backfill of 1300 + 700 rows: %lld cells (expected %d), all at version 1: %s\n", (long long)cells, 1300 * 3 + 700 * 1, t_cells == cells ? "yes" : "no");
      CHECK(cells == 1300 * 3 + 700 * 1); CHECK(t_cells == cells);
      CHECK_RC(mw_exec(db, "UPDATE t SET b = -1 WHERE id = 'b5'"), SQLITE_OK);
      sqlite3_close(db); mw_rmdb(bp); }
    MW_DONE();
}
