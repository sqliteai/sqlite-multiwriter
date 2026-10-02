//
//  multiwriter_runstore.c
//  The metadata of the database file as sorted runs in ordinary tables (see multiwriter_runstore.h).
//
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include "multiwriter_meta_priv.h"
#include "multiwriter_runstore.h"

#define CSTRIPES 32

typedef struct cblk { _Atomic int refs; size_t len; uint8_t d[]; } cblk;
typedef struct { int64_t run; uint32_t blk; cblk *b; } cslot;
typedef struct { uint32_t tbl; int64_t dv; } rdrop;

struct mw_rman { _Atomic int refs; int64_t ver; rs_run **runs; int n; rdrop *drops; int nd; };

struct mw_rstore {
    pthread_mutex_t mu; mw_rman *cur;
    cslot *slots; size_t nslots; pthread_mutex_t cmu[CSTRIPES];
    _Atomic uint64_t gets, run_probes, bloom_skips, blk_reads, cache_hits, merges, merged_rows, runs_written;
};

static bool busyish (int rc) { int p = rc & 0xff; return p == SQLITE_BUSY || p == SQLITE_LOCKED; }

// ---- small things ----
static int64_t state_get (sqlite3 *c, const char *k, int64_t dflt) {
    sqlite3_stmt *st = NULL; int64_t v = dflt;
    if (sqlite3_prepare_v2(c, "SELECT v FROM mw_state WHERE k = ?1", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, k, -1, SQLITE_STATIC);
        if (sqlite3_step(st) == SQLITE_ROW) v = sqlite3_column_int64(st, 0);
    }
    sqlite3_finalize(st);
    return v;
}
static int state_put (sqlite3 *c, const char *k, int64_t v) {
    sqlite3_stmt *st = NULL; int rc = sqlite3_prepare_v2(c, "INSERT OR REPLACE INTO mw_state(k, v) VALUES(?1, ?2)", -1, &st, NULL);
    if (rc != SQLITE_OK) return rc;
    sqlite3_bind_text(st, 1, k, -1, SQLITE_STATIC); sqlite3_bind_int64(st, 2, v);
    rc = sqlite3_step(st); sqlite3_finalize(st);
    return rc == SQLITE_DONE ? SQLITE_OK : (rc == SQLITE_ROW ? SQLITE_ERROR : rc);
}
const char *rsx_blk_sql (void) { return "SELECT data FROM mw_blocks WHERE run = ?1 AND blk = ?2"; }

// ---- the manifest ----
static void man_ref (mw_rman *m) { atomic_fetch_add(&m->refs, 1); }
void rsx_man_release (mw_rman *m) {
    if (!m || atomic_fetch_sub(&m->refs, 1) != 1) return;
    for (int i = 0; i < m->n; i++) rs_run_unref(m->runs[i]);
    free(m->runs); free(m->drops); free(m);
}
static int run_newest_first (const void *a, const void *b) { const rs_run *x = *(rs_run *const *)a, *y = *(rs_run *const *)b; if (x->age != y->age) return x->age < y->age ? 1 : -1; return x->id < y->id ? -1 : x->id > y->id; }
static int64_t drop_of (const mw_rman *m, uint32_t tbl) { int64_t d = 0; for (int i = 0; i < m->nd; i++) if (m->drops[i].tbl == tbl && m->drops[i].dv > d) d = m->drops[i].dv; return d; }

static mw_rman *man_load (sqlite3 *c, const mw_rman *old, int64_t ver) {
    mw_rman *m = calloc(1, sizeof *m); if (!m) return NULL;
    atomic_init(&m->refs, 1); m->ver = ver;
    sqlite3_stmt *st = NULL, *mt = NULL; int cap = 0;
    if (sqlite3_prepare_v2(c, "SELECT run, age, lvl, nrows, nblk, dvmax FROM mw_runs ORDER BY age DESC, run", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_prepare_v2(c, "SELECT meta FROM mw_runs WHERE run = ?1", -1, &mt, NULL);
        int r;
        while ((r = sqlite3_step(st)) == SQLITE_ROW) {
            int64_t id = sqlite3_column_int64(st, 0); rs_run *run = NULL;
            if (old) for (int i = 0; i < old->n; i++) if (old->runs[i]->id == id) { run = old->runs[i]; rs_run_ref(run); break; }
            if (!run && mt) {
                sqlite3_bind_int64(mt, 1, id);
                if (sqlite3_step(mt) == SQLITE_ROW) run = rs_run_decode(id, sqlite3_column_int64(st, 1), (int)sqlite3_column_int64(st, 2), (uint64_t)sqlite3_column_int64(st, 3), (uint32_t)sqlite3_column_int64(st, 4), sqlite3_column_int64(st, 5), sqlite3_column_blob(mt, 0), (size_t)sqlite3_column_bytes(mt, 0));
                sqlite3_reset(mt);
            }
            if (!run) { sqlite3_finalize(st); sqlite3_finalize(mt); rsx_man_release(m); return NULL; }
            if (m->n == cap) { int nc = cap ? cap * 2 : 16; rs_run **nr = realloc(m->runs, (size_t)nc * sizeof *nr); if (!nr) { rs_run_unref(run); sqlite3_finalize(st); sqlite3_finalize(mt); rsx_man_release(m); return NULL; } m->runs = nr; cap = nc; }
            m->runs[m->n++] = run;
        }
        if (r != SQLITE_DONE) { sqlite3_finalize(st); sqlite3_finalize(mt); rsx_man_release(m); return NULL; }
    }
    sqlite3_finalize(st); sqlite3_finalize(mt); st = NULL;
    int dcap = 0;
    if (sqlite3_prepare_v2(c, "SELECT tbl, dv FROM mw_drops", -1, &st, NULL) == SQLITE_OK) {
        while (sqlite3_step(st) == SQLITE_ROW) {
            if (m->nd == dcap) { int nc = dcap ? dcap * 2 : 8; rdrop *nd = realloc(m->drops, (size_t)nc * sizeof *nd); if (!nd) break; m->drops = nd; dcap = nc; }
            m->drops[m->nd++] = (rdrop){ (uint32_t)sqlite3_column_int64(st, 0), sqlite3_column_int64(st, 1) };
        }
    }
    sqlite3_finalize(st);
    qsort(m->runs, (size_t)m->n, sizeof *m->runs, run_newest_first);
    return m;
}

int rsx_man (mw_rstore *s, sqlite3 *c, mw_rman **out) {
    int64_t ver = state_get(c, "runs_ver", 0);
    pthread_mutex_lock(&s->mu);
    mw_rman *cur = s->cur;
    if (cur && cur->ver == ver) { man_ref(cur); pthread_mutex_unlock(&s->mu); *out = cur; return 0; }
    if (cur) man_ref(cur);
    pthread_mutex_unlock(&s->mu);
    mw_rman *m = man_load(c, cur, ver);
    if (cur) rsx_man_release(cur);
    if (!m) return -1;
    pthread_mutex_lock(&s->mu);
    if (!s->cur || s->cur->ver < ver) { man_ref(m); mw_rman *o = s->cur; s->cur = m; pthread_mutex_unlock(&s->mu); rsx_man_release(o); }
    else pthread_mutex_unlock(&s->mu);
    *out = m;
    return 0;
}

// ---- the store ----
mw_rstore *rsx_new (size_t cache_bytes) {
    mw_rstore *s = calloc(1, sizeof *s); if (!s) return NULL;
    pthread_mutex_init(&s->mu, NULL);
    for (int i = 0; i < CSTRIPES; i++) pthread_mutex_init(&s->cmu[i], NULL);
    s->nslots = cache_bytes / (RS_BLOCK_TARGET + 1024); if (s->nslots < 64) s->nslots = 64;
    s->slots = calloc(s->nslots, sizeof *s->slots);
    if (!s->slots) { free(s); return NULL; }
    return s;
}
static void cblk_unref (cblk *b) { if (b && atomic_fetch_sub(&b->refs, 1) == 1) free(b); }
void rsx_free (mw_rstore *s) {
    if (!s) return;
    for (size_t i = 0; i < s->nslots; i++) cblk_unref(s->slots[i].b);
    free(s->slots); rsx_man_release(s->cur);
    for (int i = 0; i < CSTRIPES; i++) pthread_mutex_destroy(&s->cmu[i]);
    pthread_mutex_destroy(&s->mu); free(s);
}
void rsx_stats_get (mw_rstore *s, rsx_stats *o) {
    o->gets = atomic_load(&s->gets); o->run_probes = atomic_load(&s->run_probes); o->bloom_skips = atomic_load(&s->bloom_skips); o->blk_reads = atomic_load(&s->blk_reads);
    o->cache_hits = atomic_load(&s->cache_hits); o->merges = atomic_load(&s->merges); o->merged_rows = atomic_load(&s->merged_rows); o->runs_written = atomic_load(&s->runs_written);
    pthread_mutex_lock(&s->mu); o->nruns = s->cur ? s->cur->n : 0; pthread_mutex_unlock(&s->mu);
}

// a block of the cache or of the file (a counted copy; NULL: it is not there)
static cblk *fetch_block (mw_rstore *s, sqlite3_stmt *st, int64_t run, uint32_t blk) {
    uint64_t h = ((uint64_t)run * 0x9E3779B97F4A7C15ull) ^ ((uint64_t)blk * 0xC2B2AE3D27D4EB4Full); h ^= h >> 31;
    size_t idx = (size_t)(h % s->nslots); pthread_mutex_t *mu = &s->cmu[idx % CSTRIPES];
    pthread_mutex_lock(mu);
    cslot *sl = &s->slots[idx];
    if (sl->b && sl->run == run && sl->blk == blk) { cblk *b = sl->b; atomic_fetch_add(&b->refs, 1); pthread_mutex_unlock(mu); atomic_fetch_add(&s->cache_hits, 1); return b; }
    pthread_mutex_unlock(mu);
    sqlite3_bind_int64(st, 1, run); sqlite3_bind_int64(st, 2, blk);
    cblk *b = NULL;
    if (sqlite3_step(st) == SQLITE_ROW) {
        size_t len = (size_t)sqlite3_column_bytes(st, 0);
        b = malloc(sizeof *b + len);
        if (b) { atomic_init(&b->refs, 2); b->len = len; memcpy(b->d, sqlite3_column_blob(st, 0), len); }                // (one count for the caller, one for the slot)
    }
    sqlite3_reset(st); sqlite3_clear_bindings(st);
    atomic_fetch_add(&s->blk_reads, 1);
    if (!b) return NULL;
    pthread_mutex_lock(mu);
    cblk *old = sl->b; sl->b = b; sl->run = run; sl->blk = blk;
    pthread_mutex_unlock(mu);
    cblk_unref(old);
    return b;
}

// ---- reading ----
static int get_one (mw_rstore *s, const mw_rman *m, sqlite3_stmt *st, uint32_t tbl, const uint8_t *pk, size_t pklen, mw_mcell **cells, int *n) {
    *cells = NULL; *n = 0;
    rs_key k = { tbl, pk, (uint32_t)pklen };
    atomic_fetch_add(&s->gets, 1);
    for (int i = 0; i < m->n; i++) {
        const rs_run *r = m->runs[i];
        atomic_fetch_add(&s->run_probes, 1);
        if (!rs_run_maybe(r, &k)) { atomic_fetch_add(&s->bloom_skips, 1); continue; }
        int b = rs_run_block_of(r, &k); if (b < 0) continue;
        cblk *cb = fetch_block(s, st, r->id, (uint32_t)b); if (!cb) return -1;
        rs_blk blk; int64_t dv; const uint8_t *c; uint32_t nc; int f = rs_blk_open(&blk, cb->d, cb->len) ? rs_blk_find(&blk, &k, &dv, &c, &nc) : -1;
        if (f < 0) { cblk_unref(cb); return -1; }
        if (f == 0) { cblk_unref(cb); continue; }
        int rc = 0;
        if (nc) {
            if (!mw_meta_row_cells(c, nc, cells, n)) rc = -1;
            else { int64_t dd = drop_of(m, tbl); if (dd) { int q = 0; for (int x = 0; x < *n; x++) if ((*cells)[x].dv >= dd) (*cells)[q++] = (*cells)[x]; *n = q; } if (!*n) { free(*cells); *cells = NULL; } }
        }
        cblk_unref(cb);
        return rc;
    }
    return 0;
}
int rsx_get_many (mw_rstore *s, mw_rman *m, sqlite3_stmt *blk, int n, const uint32_t *tbl, const uint8_t *const *pk, const size_t *pklen, mw_mcell **cells, int *ncells) {
    for (int i = 0; i < n; i++) { cells[i] = NULL; ncells[i] = 0; }
    for (int i = 0; i < n; i++) if (get_one(s, m, blk, tbl[i], pk[i], pklen[i], &cells[i], &ncells[i]) != 0) { for (int j = 0; j <= i; j++) { free(cells[j]); cells[j] = NULL; ncells[j] = 0; } return -1; }
    return 0;
}

int rsx_scan_keys (mw_rstore *s, sqlite3 *c, void (*cb)(void *, uint32_t, const uint8_t *, size_t), void *ctx) {
    (void)s;
    sqlite3_stmt *st = NULL; if (sqlite3_prepare_v2(c, "SELECT data FROM mw_blocks", -1, &st, NULL) != SQLITE_OK) { sqlite3_finalize(st); return 0; }
    int r, rc = 0;
    while ((r = sqlite3_step(st)) == SQLITE_ROW) {
        rs_blk b; if (!rs_blk_open(&b, sqlite3_column_blob(st, 0), (size_t)sqlite3_column_bytes(st, 0))) { rc = -1; break; }
        for (uint32_t i = 0; i < b.nrows; i++) { rs_key k; int64_t dv; const uint8_t *cells; uint32_t nc; if (!rs_blk_row(&b, i, &k, &dv, &cells, &nc)) { rc = -1; break; } if (nc) cb(ctx, k.tbl, k.pk, k.pklen); }
        if (rc) break;
    }
    if (!rc && r != SQLITE_DONE) rc = -1;
    sqlite3_finalize(st);
    return rc;
}
int rsx_scan_since (mw_rstore *s, sqlite3 *c, mw_rman *m, int64_t since, void (*cb)(void *, uint32_t, const uint8_t *, size_t), void *ctx) {
    sqlite3_stmt *st = NULL; if (sqlite3_prepare_v2(c, rsx_blk_sql(), -1, &st, NULL) != SQLITE_OK) { sqlite3_finalize(st); return m->n ? -1 : 0; }
    int rc = 0;
    for (int ri = 0; ri < m->n && !rc; ri++) {
        const rs_run *r = m->runs[ri]; if (r->dvmax <= since) continue;
        for (uint32_t bi = 0; bi < r->nfence && !rc; bi++) {
            if (r->fdv[bi] <= since) continue;
            cblk *cb_ = fetch_block(s, st, r->id, bi); if (!cb_) { rc = -1; break; }
            rs_blk b; if (!rs_blk_open(&b, cb_->d, cb_->len)) rc = -1;
            for (uint32_t i = 0; !rc && i < b.nrows; i++) { rs_key k; int64_t dv; const uint8_t *cells; uint32_t nc; if (!rs_blk_row(&b, i, &k, &dv, &cells, &nc)) { rc = -1; break; } if (nc && dv > since) cb(ctx, k.tbl, k.pk, k.pklen); }
            cblk_unref(cb_);
        }
    }
    sqlite3_finalize(st);
    return rc;
}

// ---- reading blocks outside the cache (merges, full scans) ----
typedef struct { sqlite3_stmt *st; } plainctx;
static int plain_read (void *ctx, const rs_run *r, uint32_t blk, uint8_t **data, size_t *len) {
    plainctx *p = ctx; sqlite3_bind_int64(p->st, 1, r->id); sqlite3_bind_int64(p->st, 2, blk);
    int rc = -1;
    if (sqlite3_step(p->st) == SQLITE_ROW) { *len = (size_t)sqlite3_column_bytes(p->st, 0); *data = malloc(*len ? *len : 1); if (*data) { memcpy(*data, sqlite3_column_blob(p->st, 0), *len); rc = 0; } }
    sqlite3_reset(p->st); sqlite3_clear_bindings(p->st);
    return rc;
}
static bool keep_alive (void *ctx, const rs_key *k, int64_t dv, const uint8_t *cells, uint32_t nc) { (void)cells; (void)nc; const mw_rman *m = ctx; return dv >= drop_of(m, k->tbl); }

typedef struct { int (*cb)(void *, uint32_t, const uint8_t *, size_t, const mw_mcell *, int); void *ctx; int err; const mw_rman *m; } scanctx;
static int scan_emit (void *ctx, uint32_t blkno, const uint8_t *data, size_t len) {
    (void)blkno; scanctx *sc = ctx; rs_blk b; if (!rs_blk_open(&b, data, len)) return -1;
    for (uint32_t i = 0; i < b.nrows; i++) {
        rs_key k; int64_t dv; const uint8_t *cells; uint32_t nc; if (!rs_blk_row(&b, i, &k, &dv, &cells, &nc)) return -1;
        mw_mcell *c; int n; if (!mw_meta_row_cells(cells, nc, &c, &n)) return -1;
        int64_t dd = drop_of(sc->m, k.tbl); if (dd) { int q = 0; for (int x = 0; x < n; x++) if (c[x].dv >= dd) c[q++] = c[x]; n = q; }
        int rc = n ? sc->cb(sc->ctx, k.tbl, k.pk, k.pklen, c, n) : 0; free(c); if (rc) return rc;
    }
    return 0;
}
static int scan_begin (void *sctx, uint64_t hint, rs_emit_fn *emit, void **ec) { (void)hint; *emit = scan_emit; *ec = sctx; return 0; }
static int scan_end (void *sctx, const uint8_t *meta, size_t ml, uint64_t nr, uint32_t nb, int64_t dv) { (void)sctx; (void)meta; (void)ml; (void)nr; (void)nb; (void)dv; return 0; }
int rsx_scan_all (mw_rstore *s, sqlite3 *c, int (*cb)(void *, uint32_t, const uint8_t *, size_t, const mw_mcell *, int), void *ctx) {
    mw_rman *m; if (rsx_man(s, c, &m) != 0) return -1;
    if (!m->n) { rsx_man_release(m); return 0; }
    plainctx pc; if (sqlite3_prepare_v2(c, rsx_blk_sql(), -1, &pc.st, NULL) != SQLITE_OK) { rsx_man_release(m); return -1; }
    scanctx sc = { cb, ctx, 0, m };
    rs_merge_opts o = { plain_read, &pc, true, keep_alive, m, 0, scan_begin, scan_end, &sc };
    int rc = rs_merge(m->runs, m->n, &o, NULL);
    sqlite3_finalize(pc.st); rsx_man_release(m);
    return rc;
}

// ---- writing ----
struct rsx_tx {
    mw_rstore *s; mw_rman *base; int64_t next_run, next_age;
    rs_run **add; int nadd, capadd; int64_t *rem; int nrem, caprem; rdrop *drops; int nd;
    sqlite3_stmt *ins_blk, *ins_run;
};
rsx_tx *rsx_tx_begin (mw_rstore *s, sqlite3 *c) {
    rsx_tx *t = calloc(1, sizeof *t); if (!t) return NULL;
    t->s = s;
    if (rsx_man(s, c, &t->base) != 0) { free(t); return NULL; }
    int64_t maxid = 0, maxage = 0; for (int i = 0; i < t->base->n; i++) { if (t->base->runs[i]->id > maxid) maxid = t->base->runs[i]->id; if (t->base->runs[i]->age > maxage) maxage = t->base->runs[i]->age; }
    t->next_run = state_get(c, "next_run", 1); if (t->next_run <= maxid) t->next_run = maxid + 1;
    t->next_age = state_get(c, "next_age", 1); if (t->next_age <= maxage) t->next_age = maxage + 1;
    return t;
}
static int tx_prepare (rsx_tx *t, sqlite3 *c) {
    if (!t->ins_blk && sqlite3_prepare_v2(c, "INSERT INTO mw_blocks(run, blk, data) VALUES(?1, ?2, ?3)", -1, &t->ins_blk, NULL) != SQLITE_OK) return sqlite3_errcode(c);
    if (!t->ins_run && sqlite3_prepare_v2(c, "INSERT INTO mw_runs(run, age, lvl, nrows, nblk, dvmax, meta) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)", -1, &t->ins_run, NULL) != SQLITE_OK) return sqlite3_errcode(c);
    return SQLITE_OK;
}
typedef struct { rsx_tx *t; int64_t run; } wctx;
static int emit_db (void *ctx, uint32_t blk, const uint8_t *data, size_t len) {
    wctx *w = ctx; sqlite3_stmt *st = w->t->ins_blk;
    sqlite3_bind_int64(st, 1, w->run); sqlite3_bind_int64(st, 2, blk); sqlite3_bind_blob(st, 3, data, (int)len, SQLITE_STATIC);
    int rc = sqlite3_step(st); sqlite3_reset(st);
    return rc == SQLITE_DONE ? 0 : (rc ? rc : -1);
}
static int tx_push_run (rsx_tx *t, rs_run *r) {
    if (t->nadd == t->capadd) { int nc = t->capadd ? t->capadd * 2 : 8; rs_run **na = realloc(t->add, (size_t)nc * sizeof *na); if (!na) return -1; t->add = na; t->capadd = nc; }
    t->add[t->nadd++] = r; return 0;
}
static int tx_insert_run (rsx_tx *t, sqlite3 *c, int64_t id, int64_t age, int lvl, const uint8_t *meta, size_t ml, uint64_t nr, uint32_t nb, int64_t dvm) {
    sqlite3_stmt *st = t->ins_run;
    sqlite3_bind_int64(st, 1, id); sqlite3_bind_int64(st, 2, age); sqlite3_bind_int64(st, 3, lvl); sqlite3_bind_int64(st, 4, (int64_t)nr); sqlite3_bind_int64(st, 5, nb); sqlite3_bind_int64(st, 6, dvm); sqlite3_bind_blob(st, 7, meta, (int)ml, SQLITE_STATIC);
    int rc = sqlite3_step(st); sqlite3_reset(st);
    if (rc != SQLITE_DONE) return rc ? rc : -1;
    rs_run *r = rs_run_decode(id, age, lvl, nr, nb, dvm, meta, ml); if (!r) return -1;
    if (tx_push_run(t, r)) { rs_run_unref(r); return -1; }
    atomic_fetch_add(&t->s->runs_written, 1);
    (void)c;
    return 0;
}
int rsx_tx_add_items (rsx_tx *t, sqlite3 *c, const fitem *v, int i0, int i1) {
    if (i1 <= i0) return 0;
    int rc = tx_prepare(t, c); if (rc) return rc;
    wctx w = { t, t->next_run++ };
    rs_builder *b = rs_builder_new((uint64_t)(i1 - i0), emit_db, &w); if (!b) return SQLITE_NOMEM;
    for (int i = i0; i < i1; i++) {
        if (i + 1 < i1 && v[i].tbl == v[i + 1].tbl && v[i].pklen == v[i + 1].pklen && !memcmp(v[i].pk, v[i + 1].pk, v[i].pklen)) continue;           // (the same key twice: the later one stands)
        rs_key k = { v[i].tbl, v[i].pk, v[i].pklen };
        rc = rs_builder_add(b, &k, v[i].n ? v[i].dv : 0, v[i].n ? v[i].blob : NULL, v[i].n ? v[i].bloblen : 0);
        if (rc) { rs_builder_free(b); return rc; }
    }
    uint8_t *meta; size_t ml; uint64_t nr; uint32_t nb; int64_t dvm;
    rc = rs_builder_finish(b, &meta, &ml, &nr, &nb, &dvm); if (rc) return rc;
    rc = tx_insert_run(t, c, w.run, t->next_age++, 0, meta, ml, nr, nb, dvm); free(meta);
    return rc;
}
int rsx_tx_drop_table (rsx_tx *t, sqlite3 *c, uint32_t tbl, int64_t dv) {
    sqlite3_stmt *st = NULL;
    int rc = sqlite3_prepare_v2(c, "INSERT INTO mw_drops(tbl, dv) VALUES(?1, ?2) ON CONFLICT(tbl) DO UPDATE SET dv = max(dv, excluded.dv)", -1, &st, NULL);
    if (rc != SQLITE_OK) return rc;
    sqlite3_bind_int64(st, 1, tbl); sqlite3_bind_int64(st, 2, dv);
    rc = sqlite3_step(st); sqlite3_finalize(st);
    if (rc != SQLITE_DONE) return rc ? rc : -1;
    rdrop *nd = realloc(t->drops, (size_t)(t->nd + 1) * sizeof *nd); if (!nd) return SQLITE_NOMEM;
    t->drops = nd; t->drops[t->nd++] = (rdrop){ tbl, dv };
    return SQLITE_OK;
}
static int tx_remove_run (rsx_tx *t, sqlite3 *c, int64_t id) {
    sqlite3_stmt *st = NULL; int rc = sqlite3_prepare_v2(c, "DELETE FROM mw_blocks WHERE run = ?1", -1, &st, NULL);
    if (rc == SQLITE_OK) { sqlite3_bind_int64(st, 1, id); rc = sqlite3_step(st); sqlite3_finalize(st); if (rc != SQLITE_DONE) return rc ? rc : -1; }
    else return rc;
    st = NULL; rc = sqlite3_prepare_v2(c, "DELETE FROM mw_runs WHERE run = ?1", -1, &st, NULL);
    if (rc == SQLITE_OK) { sqlite3_bind_int64(st, 1, id); rc = sqlite3_step(st); sqlite3_finalize(st); if (rc != SQLITE_DONE) return rc ? rc : -1; }
    else return rc;
    if (t->nrem == t->caprem) { int nc = t->caprem ? t->caprem * 2 : 8; int64_t *nr = realloc(t->rem, (size_t)nc * sizeof *nr); if (!nr) return SQLITE_NOMEM; t->rem = nr; t->caprem = nc; }
    t->rem[t->nrem++] = id;
    return SQLITE_OK;
}
int rsx_tx_finish (rsx_tx *t, sqlite3 *c) {
    if (!t->nadd && !t->nrem && !t->nd) return SQLITE_OK;
    int rc = state_put(c, "runs_ver", t->base->ver + 1); if (rc) return rc;
    rc = state_put(c, "next_run", t->next_run); if (rc) return rc;
    return state_put(c, "next_age", t->next_age);
}
void rsx_tx_end (rsx_tx *t, bool committed) {
    if (!t) return;
    sqlite3_finalize(t->ins_blk); sqlite3_finalize(t->ins_run);
    if (committed && (t->nadd || t->nrem || t->nd)) {
        mw_rman *nm = calloc(1, sizeof *nm); mw_rman *b = t->base;
        if (nm) {
            atomic_init(&nm->refs, 1); nm->ver = b->ver + 1;
            nm->runs = malloc((size_t)(b->n + t->nadd + 1) * sizeof *nm->runs); nm->drops = malloc((size_t)(b->nd + t->nd + 1) * sizeof *nm->drops);
            if (nm->runs && nm->drops) {
                for (int i = 0; i < b->n; i++) { bool gone = false; for (int j = 0; j < t->nrem; j++) if (t->rem[j] == b->runs[i]->id) gone = true; if (gone) continue; rs_run_ref(b->runs[i]); nm->runs[nm->n++] = b->runs[i]; }
                for (int i = 0; i < t->nadd; i++) nm->runs[nm->n++] = t->add[i], t->add[i] = NULL;
                for (int i = 0; i < b->nd; i++) nm->drops[nm->nd++] = b->drops[i];
                for (int i = 0; i < t->nd; i++) nm->drops[nm->nd++] = t->drops[i];
                qsort(nm->runs, (size_t)nm->n, sizeof *nm->runs, run_newest_first);
                pthread_mutex_lock(&t->s->mu);
                if (!t->s->cur || t->s->cur->ver < nm->ver) { mw_rman *o = t->s->cur; t->s->cur = nm; pthread_mutex_unlock(&t->s->mu); rsx_man_release(o); nm = NULL; }
                else pthread_mutex_unlock(&t->s->mu);
            }
            if (nm) rsx_man_release(nm);
        }
    }
    for (int i = 0; i < t->nadd; i++) rs_run_unref(t->add[i]);
    free(t->add); free(t->rem); free(t->drops); rsx_man_release(t->base); free(t);
}

// ---- merging ----
typedef struct { uint32_t blk; uint8_t *d; size_t len; } oblk;
typedef struct {
    mw_rstore *s; sqlite3 *wr; int out_lvl; int64_t out_age; const mw_rman *man;
    oblk *blks; int nblks, capblks; int err;
} mctx;
static void mctx_clear (mctx *m) { for (int i = 0; i < m->nblks; i++) free(m->blks[i].d); m->nblks = 0; }
static int m_emit (void *ctx, uint32_t blk, const uint8_t *d, size_t len) {
    mctx *m = ctx;
    if (m->nblks == m->capblks) { int nc = m->capblks ? m->capblks * 2 : 64; oblk *nb = realloc(m->blks, (size_t)nc * sizeof *nb); if (!nb) return -1; m->blks = nb; m->capblks = nc; }
    uint8_t *cp = malloc(len); if (!cp) return -1; memcpy(cp, d, len);
    m->blks[m->nblks++] = (oblk){ blk, cp, len };
    return 0;
}
static int m_begin (void *sctx, uint64_t hint, rs_emit_fn *emit, void **ec) { (void)hint; mctx_clear(sctx); *emit = m_emit; *ec = sctx; return 0; }
static int m_end (void *sctx, const uint8_t *meta, size_t ml, uint64_t nr, uint32_t nb, int64_t dvm) {
    mctx *m = sctx; sqlite3 *c = m->wr; int rc = SQLITE_BUSY;
    for (int attempt = 0; attempt < 400 && busyish(rc); attempt++) {
        if (attempt) usleep(1000u * (unsigned)(attempt < 20 ? attempt : 20));
        rc = sqlite3_exec(c, "BEGIN", NULL, NULL, NULL); if (rc) continue;
        rsx_tx *t = rsx_tx_begin(m->s, c);
        if (!t) { sqlite3_exec(c, "ROLLBACK", NULL, NULL, NULL); rc = SQLITE_NOMEM; break; }
        rc = tx_prepare(t, c);
        int64_t id = t->next_run++;
        wctx w = { t, id };
        for (int i = 0; !rc && i < m->nblks; i++) rc = emit_db(&w, m->blks[i].blk, m->blks[i].d, m->blks[i].len);
        if (!rc) rc = tx_insert_run(t, c, id, m->out_age, m->out_lvl, meta, ml, nr, nb, dvm);
        if (!rc) rc = rsx_tx_finish(t, c);
        if (!rc) rc = sqlite3_exec(c, "COMMIT", NULL, NULL, NULL);
        if (rc) sqlite3_exec(c, "ROLLBACK", NULL, NULL, NULL);
        rsx_tx_end(t, rc == SQLITE_OK);
    }
    mctx_clear(m);
    return rc;
}
int rsx_merge (mw_rstore *s, sqlite3 *rd, sqlite3 *wr, int fanout, uint64_t part_rows) {
    mw_rman *man = NULL;
    sqlite3_exec(rd, "BEGIN", NULL, NULL, NULL);
    int rc = rsx_man(s, rd, &man);
    sqlite3_exec(rd, "COMMIT", NULL, NULL, NULL);
    if (rc) return -1;
    // the lowest level with enough age groups
    int lvl = -1;
    for (int L = 0; L < 24 && lvl < 0; L++) { int groups = 0; int64_t last = -1; for (int i = 0; i < man->n; i++) if (man->runs[i]->lvl == L && man->runs[i]->age != last) { groups++; last = man->runs[i]->age; } if (groups >= fanout) lvl = L; }
    if (lvl < 0) { rsx_man_release(man); return 0; }
    rs_run **in = malloc((size_t)man->n * sizeof *in); int nin = 0; int64_t minage = INT64_MAX;
    if (!in) { rsx_man_release(man); return -1; }
    for (int i = 0; i < man->n; i++) if (man->runs[i]->lvl == lvl) { in[nin++] = man->runs[i]; if (man->runs[i]->age < minage) minage = man->runs[i]->age; }
    bool bottom = true; for (int i = 0; i < man->n; i++) if (man->runs[i]->age < minage) bottom = false;
    plainctx pc; if (sqlite3_prepare_v2(rd, rsx_blk_sql(), -1, &pc.st, NULL) != SQLITE_OK) { free(in); rsx_man_release(man); return -1; }
    mctx mc = { s, wr, lvl + 1, in[0]->age, man, NULL, 0, 0, 0 };
    rs_merge_opts o = { plain_read, &pc, bottom, keep_alive, man, part_rows, m_begin, m_end, &mc };
    uint64_t rows = 0;
    rc = rs_merge(in, nin, &o, &rows);
    mctx_clear(&mc); free(mc.blks); sqlite3_finalize(pc.st);
    if (!rc) {                                                                                  // the inputs go last: until then the rows are in both
        rc = SQLITE_BUSY;
        for (int attempt = 0; attempt < 400 && busyish(rc); attempt++) {
            if (attempt) usleep(1000u * (unsigned)(attempt < 20 ? attempt : 20));
            rc = sqlite3_exec(wr, "BEGIN", NULL, NULL, NULL); if (rc) continue;
            rsx_tx *t = rsx_tx_begin(s, wr);
            if (!t) { sqlite3_exec(wr, "ROLLBACK", NULL, NULL, NULL); rc = SQLITE_NOMEM; break; }
            for (int i = 0; !rc && i < nin; i++) rc = tx_remove_run(t, wr, in[i]->id);
            if (!rc) rc = rsx_tx_finish(t, wr);
            if (!rc) rc = sqlite3_exec(wr, "COMMIT", NULL, NULL, NULL);
            if (rc) sqlite3_exec(wr, "ROLLBACK", NULL, NULL, NULL);
            rsx_tx_end(t, rc == SQLITE_OK);
        }
    }
    free(in); rsx_man_release(man);
    if (rc) return -1;
    atomic_fetch_add(&s->merges, 1); atomic_fetch_add(&s->merged_rows, rows);
    return 1;
}
