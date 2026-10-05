// Randomised serializability check of the commit protocol.
// Concurrent transactions (inserts, updates, deletes, changes of a UNIQUE column, growth of a payload that splits and frees pages) run on a small set of keys; each one records what it
// read and what it did, and the epoch it read from and the epoch it committed at (MW_FCNTL_TXINFO). Afterwards the committed transactions are replayed on a model, one at a time in the order
// of their commit epochs, and the test checks that
//   1. every read-write transaction saw exactly the state that the model has just before it: the database behaved as the serial execution in commit order (every key that a transaction reads it
//      also writes, so under snapshot isolation with first-committer-wins this must hold exactly: a lost update, a write that goes through on a row that was deleted meanwhile, a UNIQUE value
//      held by two rows, would all show here);
//   2. a read-only transaction saw exactly the state after the commits up to the epoch it read from (a consistent snapshot);
//   3. the final content of the table is the model's, the UNIQUE column has no duplicate (checked with a table scan, not through its index), and integrity_check is ok.
// Write skew (a transaction that reads a key it does not write) is not generated: snapshot isolation does not prevent it, and the test must pass on the engine as it is. It is the check that a
// finer-grained conflict detection (a logical replay of the commits that conflict only on a page) would have to keep passing.
#include <pthread.h>
#include <stdatomic.h>
#include "mw_test.h"
#include "multiwriter.h"

enum { MAXOPS = 4, ROKEYS = 8 };
typedef struct { int kind, key, a, b; int sex, sv, su, spl, sw; } op_t;                 // kinds: 0 touch (w += 1: a write that the page really sees), 1 v += a, 2 delete, 3 u = b (-1: NULL), 4 insert (v = a, u = b, payload b2 below), 5 payload = a bytes
typedef struct { uint64_t snap, commit; int n; op_t op[MAXOPS]; int ins_pl; } rw_t;
typedef struct { uint64_t snap; int n; int key[ROKEYS], ex[ROKEYS], v[ROKEYS], u[ROKEYS], pl[ROKEYS], w[ROKEYS]; } ro_t;
typedef struct { rw_t *rw; size_t nrw, caprw; ro_t *ro; size_t nro, capro; long busy, constraint, other; } rec_t;

static const char *g_path; static int g_keys, g_threads; static double g_secs;
static int open_db (const char *path, sqlite3 **db) {
    char uri[300]; snprintf(uri, sizeof uri, "file:%s?mw=2&mw_gc=16%s", path, getenv("MW_TEST_MP") ? "&mw_mp=1" : "");
    int rc = sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
    if (rc == SQLITE_OK) { sqlite3_extended_result_codes(*db, 1); sqlite3_busy_timeout(*db, 0); }
    return rc;
}
static uint64_t rnd (unsigned *s) { *s = *s * 1103515245u + 12345u; uint64_t x = *s >> 8; *s = *s * 1103515245u + 12345u; return (x << 16) ^ (*s >> 8); }
static int ucol (unsigned *s) { uint64_t r = rnd(s) % ((uint64_t)g_keys * 2 + 3); return r > (uint64_t)g_keys * 2 ? -1 : (int)r; }      // (a domain about twice the keys: collisions are common)

static int read_row (sqlite3_stmt *sel, int key, int *ex, int *v, int *u, int *pl, int *w) {
    sqlite3_reset(sel); sqlite3_bind_int(sel, 1, key);
    int rc = sqlite3_step(sel);
    if (rc == SQLITE_ROW) { *ex = 1; *v = sqlite3_column_int(sel, 0); *u = sqlite3_column_type(sel, 1) == SQLITE_NULL ? -1 : sqlite3_column_int(sel, 1); *pl = sqlite3_column_int(sel, 2); *w = sqlite3_column_int(sel, 3); rc = SQLITE_OK; }
    else if (rc == SQLITE_DONE) { *ex = 0; *v = *u = *pl = *w = 0; rc = SQLITE_OK; }
    sqlite3_reset(sel); return rc;
}
static uint64_t snap_epoch (sqlite3 *db) { mw_tx_info ti; memset(&ti, 0, sizeof ti); sqlite3_file_control(db, "main", MW_FCNTL_TXINFO, &ti); return ti.snapshot_epoch; }
static uint64_t commit_epoch (sqlite3 *db) { mw_tx_info ti; memset(&ti, 0, sizeof ti); sqlite3_file_control(db, "main", MW_FCNTL_TXINFO, &ti); return ti.commit_epoch; }

static void *worker (void *arg) {
    rec_t *R = arg; sqlite3 *db; if (open_db(g_path, &db) != SQLITE_OK) { mw_failures++; return NULL; }
    static _Atomic int ids; unsigned rng = 9001u + 7919u * (unsigned)atomic_fetch_add(&ids, 1);
    sqlite3_stmt *sel; sqlite3_prepare_v2(db, "SELECT v, u, length(p), w FROM t WHERE id = ?", -1, &sel, NULL);
    struct timespec t0; clock_gettime(CLOCK_MONOTONIC, &t0);
    for (;;) {
        struct timespec t1; clock_gettime(CLOCK_MONOTONIC, &t1);
        if ((double)(t1.tv_sec - t0.tv_sec) + (double)(t1.tv_nsec - t0.tv_nsec) / 1e9 > g_secs) break;
        if (rnd(&rng) % 6 == 0) {                                                                     // a read-only transaction: a few keys, one snapshot
            ro_t ro; memset(&ro, 0, sizeof ro); ro.n = ROKEYS;
            if (mw_exec(db, "BEGIN") != SQLITE_OK) continue;
            int bad = 0;
            for (int i = 0; i < ROKEYS && !bad; i++) { ro.key[i] = 1 + (int)(rnd(&rng) % (uint64_t)g_keys); bad = read_row(sel, ro.key[i], &ro.ex[i], &ro.v[i], &ro.u[i], &ro.pl[i], &ro.w[i]); }
            ro.snap = snap_epoch(db); mw_exec(db, "COMMIT");
            if (bad) { R->other++; continue; }
            if (R->nro == R->capro) { R->capro = R->capro ? R->capro * 2 : 1024; R->ro = realloc(R->ro, R->capro * sizeof(ro_t)); }
            R->ro[R->nro++] = ro; continue;
        }
        rw_t tx; memset(&tx, 0, sizeof tx); tx.n = 1 + (int)(rnd(&rng) % MAXOPS);
        if (mw_exec(db, "BEGIN") != SQLITE_OK) continue;
        int ok = 1; char sql[200];
        for (int i = 0; i < tx.n && ok; i++) {
            op_t *o = &tx.op[i]; int again;
            do { again = 0; o->key = 1 + (int)(rnd(&rng) % (uint64_t)g_keys); for (int j = 0; j < i; j++) if (tx.op[j].key == o->key) again = 1; } while (again);       // (distinct keys: the reads and writes of a transaction do not overlap)
            if (read_row(sel, o->key, &o->sex, &o->sv, &o->su, &o->spl, &o->sw) != SQLITE_OK) { ok = 0; break; }
            uint64_t r = rnd(&rng) % 10;
            if (!o->sex) { o->kind = 4; o->a = (int)(rnd(&rng) % 1000); o->b = ucol(&rng); tx.ins_pl = (int)(rnd(&rng) % 400);
                if (o->b < 0) snprintf(sql, sizeof sql, "INSERT INTO t(id,v,u,p) VALUES(%d,%d,NULL,zeroblob(%d))", o->key, o->a, tx.ins_pl);
                else snprintf(sql, sizeof sql, "INSERT INTO t(id,v,u,p) VALUES(%d,%d,%d,zeroblob(%d))", o->key, o->a, o->b, tx.ins_pl);
                o->spl = tx.ins_pl; }
            else if (r < 3) { o->kind = 1; o->a = 1 + (int)(rnd(&rng) % 9); snprintf(sql, sizeof sql, "UPDATE t SET v = v + %d WHERE id = %d", o->a, o->key); }
            else if (r < 5) { o->kind = 2; snprintf(sql, sizeof sql, "DELETE FROM t WHERE id = %d", o->key); }
            else if (r < 7) { o->kind = 3; o->b = ucol(&rng); if (o->b < 0) snprintf(sql, sizeof sql, "UPDATE t SET u = NULL WHERE id = %d", o->key); else snprintf(sql, sizeof sql, "UPDATE t SET u = %d WHERE id = %d", o->b, o->key); }
            else if (r < 9) { o->kind = 5; o->a = (int)(rnd(&rng) % 600); snprintf(sql, sizeof sql, "UPDATE t SET p = zeroblob(%d) WHERE id = %d", o->a, o->key); }
            else { o->kind = 0; snprintf(sql, sizeof sql, "UPDATE t SET w = w + 1 WHERE id = %d", o->key); }
            int rc = mw_exec(db, sql);
            if (rc != SQLITE_OK) { ok = 0; if ((rc & 0xff) == SQLITE_CONSTRAINT) R->constraint++; else if ((rc & 0xff) == SQLITE_BUSY) R->busy++; else { R->other++; if (getenv("MW_VERBOSE")) printf("  other rc %d on: %s\n", rc, sql); } }
        }
        if (ok) { tx.snap = snap_epoch(db); int rc = mw_exec(db, "COMMIT"); if (rc != SQLITE_OK) { ok = 0; if ((rc & 0xff) == SQLITE_BUSY) R->busy++; else { R->other++; if (getenv("MW_VERBOSE")) printf("  commit rc %d\n", rc); } } else tx.commit = commit_epoch(db); }
        if (!ok) { mw_exec(db, "ROLLBACK"); continue; }
        if (tx.commit == 0) { R->other++; if (getenv("MW_VERBOSE")) { printf("  commit epoch 0 (snapshot %llu) n=%d kinds", (unsigned long long)tx.snap, tx.n); for (int q = 0; q < tx.n; q++) printf(" %d", tx.op[q].kind); printf("\n"); } continue; }
        if (R->nrw == R->caprw) { R->caprw = R->caprw ? R->caprw * 2 : 1024; R->rw = realloc(R->rw, R->caprw * sizeof(rw_t)); }
        R->rw[R->nrw++] = tx;
    }
    sqlite3_finalize(sel); sqlite3_close(db); return NULL;
}

typedef struct { int ex, v, u, pl, w; } mrow;
static int cmp_rw (const void *a, const void *b) { uint64_t x = (*(rw_t *const *)a)->commit, y = (*(rw_t *const *)b)->commit; return x < y ? -1 : x > y; }
static int cmp_ro (const void *a, const void *b) { uint64_t x = (*(ro_t *const *)a)->snap, y = (*(ro_t *const *)b)->snap; return x < y ? -1 : x > y; }
static int same (int ex, int v, int u, int pl, int w, const mrow *m) { return ex == m->ex && (!ex || (v == m->v && u == m->u && pl == m->pl && w == m->w)); }

static void run (const char *name, int keys, int threads, double secs) {
    char path[256]; mw_tmpdb(path, sizeof path, "serial");
    sqlite3 *s; CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER NOT NULL, u INTEGER UNIQUE, p BLOB NOT NULL, w INTEGER NOT NULL DEFAULT 0)"), SQLITE_OK);
    sqlite3_close(s);
    g_path = path; g_keys = keys; g_threads = threads; g_secs = secs;
    sqlite3 *obs; CHECK_RC(open_db(path, &obs), SQLITE_OK);                                               // (kept open: the statistics belong to the database object, which lives while a connection does)
    rec_t *R = calloc((size_t)threads, sizeof *R); pthread_t th[64];
    for (int i = 0; i < threads; i++) pthread_create(&th[i], NULL, worker, &R[i]);
    for (int i = 0; i < threads; i++) pthread_join(th[i], NULL);
    mw_db_stats ds; memset(&ds, 0, sizeof ds); sqlite3_file_control(obs, "main", MW_FCNTL_DBSTATS, &ds); sqlite3_close(obs);
    size_t nrw = 0, nro = 0; long busy = 0, cons = 0, oth = 0;
    for (int i = 0; i < threads; i++) { nrw += R[i].nrw; nro += R[i].nro; busy += R[i].busy; cons += R[i].constraint; oth += R[i].other; }
    rw_t **rw = malloc((nrw + 1) * sizeof *rw); ro_t **ro = malloc((nro + 1) * sizeof *ro); size_t a = 0, b = 0;
    for (int i = 0; i < threads; i++) { for (size_t k = 0; k < R[i].nrw; k++) rw[a++] = &R[i].rw[k]; for (size_t k = 0; k < R[i].nro; k++) ro[b++] = &R[i].ro[k]; }
    qsort(rw, nrw, sizeof *rw, cmp_rw); qsort(ro, nro, sizeof *ro, cmp_ro);

    int un = 2 * keys + 4; mrow *m = calloc((size_t)keys + 2, sizeof *m); int *owner = malloc((size_t)un * sizeof *owner); for (int i = 0; i < un; i++) owner[i] = -1;
    long bad_read = 0, bad_unique = 0, bad_ro = 0, bad_dup_epoch = 0; size_t ri = 0;
    #define REPORT(counter, ...) do { if ((counter)++ < 4) { printf("    "); printf(__VA_ARGS__); printf("\n"); } } while (0)
    for (size_t i = 0; i < nrw; i++) {
        rw_t *t = rw[i];
        if (i && t->commit == rw[i - 1]->commit) REPORT(bad_dup_epoch, "two commits at epoch %llu", (unsigned long long)t->commit);
        while (ri < nro && ro[ri]->snap < t->commit) {                                                 // read-only transactions whose snapshot lies before this commit: they saw the state so far
            ro_t *r = ro[ri++];
            for (int k = 0; k < r->n; k++) if (!same(r->ex[k], r->v[k], r->u[k], r->pl[k], r->w[k], &m[r->key[k]])) REPORT(bad_ro, "read-only at snapshot %llu: key %d saw (%d,%d,%d,%d), model (%d,%d,%d,%d)", (unsigned long long)r->snap, r->key[k], r->ex[k], r->v[k], r->u[k], r->pl[k], m[r->key[k]].ex, m[r->key[k]].v, m[r->key[k]].u, m[r->key[k]].pl);
        }
        for (int k = 0; k < t->n; k++) { op_t *o = &t->op[k]; mrow *x = &m[o->key];
            if (!same(o->sex, o->sv, o->su, o->spl, o->sw, x) && !(o->kind == 4 && !x->ex && !o->sex)) REPORT(bad_read, "tx committed at %llu (snapshot %llu), key %d, op %d: saw (%d,%d,%d,%d), model (%d,%d,%d,%d)", (unsigned long long)t->commit, (unsigned long long)t->snap, o->key, o->kind, o->sex, o->sv, o->su, o->spl, x->ex, x->v, x->u, x->pl); }
        for (int k = 0; k < t->n; k++) { op_t *o = &t->op[k]; mrow *x = &m[o->key];
            if (o->kind == 4 || o->kind == 3) { int nu = o->b; if (nu >= 0 && owner[nu] != -1 && owner[nu] != o->key) REPORT(bad_unique, "tx committed at %llu: u = %d already held by key %d", (unsigned long long)t->commit, nu, owner[nu]); }
            switch (o->kind) {
                case 1: x->v += o->a; break;
                case 2: if (x->ex && x->u >= 0 && owner[x->u] == o->key) owner[x->u] = -1; x->ex = 0; x->v = x->u = x->pl = x->w = 0; break;
                case 3: if (x->u >= 0 && owner[x->u] == o->key) owner[x->u] = -1; x->u = o->b; if (o->b >= 0) owner[o->b] = o->key; break;
                case 4: x->ex = 1; x->v = o->a; x->u = o->b; x->pl = o->spl; x->w = 0; if (o->b >= 0) owner[o->b] = o->key; break;
                case 5: x->pl = o->a; break;
                default: x->w++; break;
            }
        }
    }
    while (ri < nro) { ro_t *r = ro[ri++]; for (int k = 0; k < r->n; k++) if (!same(r->ex[k], r->v[k], r->u[k], r->pl[k], r->w[k], &m[r->key[k]])) REPORT(bad_ro, "read-only at snapshot %llu: key %d saw (%d,%d,%d,%d), model (%d,%d,%d,%d)", (unsigned long long)r->snap, r->key[k], r->ex[k], r->v[k], r->u[k], r->pl[k], m[r->key[k]].ex, m[r->key[k]].v, m[r->key[k]].u, m[r->key[k]].pl); }

    // the final table against the model, the UNIQUE column by a scan, integrity
    sqlite3 *chk; CHECK_RC(open_db(path, &chk), SQLITE_OK);
    long bad_final = 0, rows = 0; sqlite3_stmt *st; sqlite3_prepare_v2(chk, "SELECT id, v, u, length(p), w FROM t NOT INDEXED ORDER BY id", -1, &st, NULL);
    int seen_key[1 << 16]; memset(seen_key, 0, sizeof(int) * (size_t)(keys + 2 < (1 << 16) ? keys + 2 : (1 << 16)));
    while (sqlite3_step(st) == SQLITE_ROW) {
        int id = sqlite3_column_int(st, 0), v = sqlite3_column_int(st, 1), u = sqlite3_column_type(st, 2) == SQLITE_NULL ? -1 : sqlite3_column_int(st, 2), pl = sqlite3_column_int(st, 3), w = sqlite3_column_int(st, 4); rows++;
        if (id < 1 || id > keys || !same(1, v, u, pl, w, &m[id])) REPORT(bad_final, "final row %d = (%d,%d,%d) differs from the model", id, v, u, pl); else seen_key[id] = 1;
    }
    sqlite3_finalize(st);
    long model_rows = 0; for (int k = 1; k <= keys; k++) { if (m[k].ex) { model_rows++; if (!seen_key[k]) REPORT(bad_final, "key %d is in the model and not in the table", k); } }
    long dup = 0; sqlite3_prepare_v2(chk, "SELECT count(*) FROM (SELECT u FROM t NOT INDEXED WHERE u IS NOT NULL GROUP BY u HAVING count(*) > 1)", -1, &st, NULL); if (sqlite3_step(st) == SQLITE_ROW) dup = sqlite3_column_int(st, 0); sqlite3_finalize(st);
    int integ = 0; sqlite3_prepare_v2(chk, "PRAGMA integrity_check", -1, &st, NULL); if (sqlite3_step(st) == SQLITE_ROW) integ = strcmp((const char *)sqlite3_column_text(st, 0), "ok") == 0; sqlite3_finalize(st);
    printf("%s: %d keys, %d threads: %zu read-write and %zu read-only transactions committed, %ld refused (busy), %ld constraint errors, %ld other; page conflicts %llu, relocations %llu, merges %llu; table %ld rows (model %ld)\n",
           name, keys, threads, nrw, nro, busy, cons, oth, (unsigned long long)ds.page_conflicts, (unsigned long long)ds.relocations, (unsigned long long)ds.merges, rows, model_rows);
    printf("   reads that differ from the serial order %ld, UNIQUE broken %ld, read-only inconsistent %ld, duplicate epochs %ld, final differs %ld, duplicate u in the table %ld, integrity %s\n", bad_read, bad_unique, bad_ro, bad_dup_epoch, bad_final, dup, integ ? "ok" : "BAD");
    CHECK(bad_read == 0); CHECK(bad_unique == 0); CHECK(bad_ro == 0); CHECK(bad_dup_epoch == 0); CHECK(bad_final == 0); CHECK(dup == 0); CHECK(integ); CHECK(rows == model_rows);
    CHECK(nrw >= 500);                                                                                // (not vacuous)
    sqlite3_close(chk);
    for (int i = 0; i < threads; i++) { free(R[i].rw); free(R[i].ro); } free(R); free(rw); free(ro); free(m); free(owner);
    mw_rmdb(path);
}

int main (void) {
    setvbuf(stdout, NULL, _IOLBF, 0);
    run("hot (few keys, many conflicts)", 12, 8, 3.0);
    run("medium", 200, 8, 3.0);
    run("wide (many pages, pages freed and reused)", 3000, 8, 4.0);
    MW_DONE();
}
