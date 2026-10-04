//
//  multiwriter_runstore.c
//  The metadata of the database file as sorted runs in ordinary tables (see multiwriter_runstore.h).
//
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <signal.h>
#include <errno.h>
#include "multiwriter_meta_priv.h"
#include "multiwriter_runstore.h"

#define CSTRIPES 32
#define SLOT_HDR 16                                // every slot starts with who it belongs to: [run id u64][block u32][part u32] (a slot that is read for another run is an error, not a wrong answer)
static void hdr_put (uint8_t *p, int64_t run, uint32_t blk, uint32_t part) { uint64_t r = (uint64_t)run; memcpy(p, &r, 8); memcpy(p + 8, &blk, 4); memcpy(p + 12, &part, 4); }

typedef struct cblk { _Atomic int refs; size_t len; uint8_t d[]; } cblk;
typedef struct { int64_t run; uint32_t blk; cblk *b; } cslot;                       // an entry of the cache of blocks
typedef struct { uint32_t tbl; int64_t dv; } rdrop;

struct mw_rman { _Atomic int refs; int64_t ver; rs_run **runs; int n; rdrop *drops; int nd; };

struct mw_rstore {
    pthread_mutex_t mu, wmu; mw_rman *cur;
    cslot *cs; size_t ncs; pthread_mutex_t cmu[CSTRIPES];
    uint32_t *hslot; uintptr_t *howner; size_t nh, caph;                      // slots taken out of the pool by a transaction or a part that has not finished (so that the register of reservations covers them)
    bool exclusive;                                                          // only this process merges and removes runs (one thread): the runs a merge works from cannot go while it reads them
    pthread_mutex_t pmu; uint32_t *pool; size_t npool, cappool;               // slots taken from the free table (or fresh) and not used yet
    _Atomic uint32_t slot_bytes;                                             // the size of a slot: the file's, found or decided at the first use
    _Atomic int bl0, ball;                                                   // age groups at level 0 and in all, of the current list
    _Atomic uint64_t merge_retries, merge_ns, swept_slots; _Atomic uint64_t gets, run_probes, bloom_skips, blk_reads, cache_hits, merges, merged_rows, runs_written;
};

// a retry after a conflict: at once the first times (the other commit is over by now), then a little longer, never the tens of milliseconds a bulk writer would sleep
static void backoff (int attempt) { if (attempt > 2) usleep(100u * (unsigned)(attempt < 30 ? attempt : 30)); }

static size_t sl_put (uint8_t *p, uint64_t v) { size_t n = 0; while (v >= 0x80) { p[n++] = (uint8_t)(v | 0x80); v >>= 7; } p[n++] = (uint8_t)v; return n; }
static bool sl_get (const uint8_t **p, const uint8_t *end, uint64_t *v) { uint64_t r = 0; int sh = 0; while (*p < end && sh < 64) { uint8_t b = *(*p)++; r |= (uint64_t)(b & 0x7f) << sh; if (!(b & 0x80)) { *v = r; return true; } sh += 7; } return false; }

static bool busyish (int rc) { int p = rc & 0xff; return p == SQLITE_BUSY || p == SQLITE_LOCKED; }

// ---- small things ----
// A value of mw_state. A key that is not there (or no table yet: the store is created on its first use) gives the default; a failure of the read (an I/O error, no memory, a lock) is an
// error, never a default: taking the default for the manifest would have the next flush replace the whole list of runs by one.
static bool hard_err (int rc) { return rc != SQLITE_OK && rc != SQLITE_DONE && rc != SQLITE_ROW && (rc & 0xff) != SQLITE_ERROR; }
static int state_get (sqlite3 *c, const char *k, int64_t dflt, int64_t *v) {
    sqlite3_stmt *st = NULL; *v = dflt;
    int rc = sqlite3_prepare_v2(c, "SELECT v FROM mw_state WHERE k = ?1", -1, &st, NULL);
    if (rc == SQLITE_OK) {
        sqlite3_bind_text(st, 1, k, -1, SQLITE_STATIC);
        rc = sqlite3_step(st);
        if (rc == SQLITE_ROW) { *v = sqlite3_column_int64(st, 0); rc = SQLITE_OK; } else if (rc == SQLITE_DONE) rc = SQLITE_OK;
    }
    sqlite3_finalize(st);
    return hard_err(rc) ? rc : SQLITE_OK;
}
static int state_put (sqlite3 *c, const char *k, int64_t v) {
    sqlite3_stmt *st = NULL; int rc = sqlite3_prepare_v2(c, "INSERT OR REPLACE INTO mw_state(k, v) VALUES(?1, ?2)", -1, &st, NULL);
    if (rc != SQLITE_OK) return rc;
    sqlite3_bind_text(st, 1, k, -1, SQLITE_STATIC); sqlite3_bind_int64(st, 2, v);
    rc = sqlite3_step(st); sqlite3_finalize(st);
    return rc == SQLITE_DONE ? SQLITE_OK : (rc == SQLITE_ROW ? SQLITE_ERROR : rc);
}
const char *rsx_blk_sql (void) { return "SELECT data FROM mw_slots WHERE slot = ?1"; }

// ---- the manifest ----
static void man_ref (mw_rman *m) { atomic_fetch_add(&m->refs, 1); }
void rsx_man_release (mw_rman *m) {
    if (!m || atomic_fetch_sub(&m->refs, 1) != 1) return;
    for (int i = 0; i < m->n; i++) rs_run_unref(m->runs[i]);
    free(m->runs); free(m->drops); free(m);
}
static void backlog_update (mw_rstore *s, const mw_rman *m);
static int run_newest_first (const void *a, const void *b) { const rs_run *x = *(rs_run *const *)a, *y = *(rs_run *const *)b; if (x->age != y->age) return x->age < y->age ? 1 : -1; return x->id < y->id ? -1 : x->id > y->id; }
static int64_t drop_of (const mw_rman *m, uint32_t tbl) { int64_t d = 0; for (int i = 0; i < m->nd; i++) if (m->drops[i].tbl == tbl && m->drops[i].dv > d) d = m->drops[i].dv; return d; }

static mw_rman *man_load (sqlite3 *c, const mw_rman *old, int64_t ver, uint32_t sb_in) {
    mw_rman *m = calloc(1, sizeof *m); if (!m) return NULL;
    atomic_init(&m->refs, 1); m->ver = ver;
    sqlite3_stmt *st = NULL, *mt = NULL; int cap = 0;
    sqlite3_stmt *sl = NULL;
    int prc = sqlite3_prepare_v2(c, "SELECT run, age, lvl, nrows, nblk, dvmax, metalen, metaloc FROM mw_runs ORDER BY age DESC, run", -1, &st, NULL);
    if (hard_err(prc)) { sqlite3_finalize(st); rsx_man_release(m); return NULL; }
    if (prc == SQLITE_OK) {
        sqlite3_prepare_v2(c, "SELECT data FROM mw_slots WHERE slot = ?1", -1, &sl, NULL); mt = sl;
        int r;
        while ((r = sqlite3_step(st)) == SQLITE_ROW) {
            int64_t id = sqlite3_column_int64(st, 0); rs_run *run = NULL;
            if (old) for (int i = 0; i < old->n; i++) if (old->runs[i]->id == id) { run = old->runs[i]; rs_run_ref(run); break; }
            if (!run && mt) {                                                                    // (a run this list has not met: its meta is in slots)
                int64_t mlen = sqlite3_column_int64(st, 6); const uint8_t *ml = sqlite3_column_blob(st, 7); int mll = sqlite3_column_bytes(st, 7);
                uint32_t sb = sb_in; uint8_t *meta = (sb && mlen > 0 && mlen < (1 << 28)) ? malloc((size_t)mlen) : NULL;
                size_t got = 0; const uint8_t *p = ml, *pe = ml + mll; bool ok = meta != NULL;
                while (ok && got < (size_t)mlen) {
                    uint64_t slot; if (!sl_get(&p, pe, &slot)) { ok = false; break; }
                    sqlite3_bind_int64(mt, 1, (int64_t)slot);
                    if (sqlite3_step(mt) == SQLITE_ROW && (uint32_t)sqlite3_column_bytes(mt, 0) == sb) {
                        const uint8_t *d = sqlite3_column_blob(mt, 0); uint8_t want[SLOT_HDR]; hdr_put(want, id, 0xFFFFFFFFu, (uint32_t)(got / (sb - SLOT_HDR)));
                        if (memcmp(d, want, SLOT_HDR)) { ok = false; if (getenv("MW_DBG_HDR")) fprintf(stderr, "STALE META SLOT %llu for run %lld\n", (unsigned long long)slot, (long long)id); }
                        else { size_t n = (size_t)mlen - got < sb - SLOT_HDR ? (size_t)mlen - got : sb - SLOT_HDR; memcpy(meta + got, d + SLOT_HDR, n); got += n; }
                    } else ok = false;
                    sqlite3_reset(mt);
                }
                if (ok) { run = rs_run_decode(id, sqlite3_column_int64(st, 1), (int)sqlite3_column_int64(st, 2), (uint64_t)sqlite3_column_int64(st, 3), (uint32_t)sqlite3_column_int64(st, 4), sqlite3_column_int64(st, 5), meta, (size_t)mlen);
                       if (run) { run->mloc = malloc(mll ? (size_t)mll : 1); if (run->mloc) { memcpy(run->mloc, ml, (size_t)mll); run->mlocl = (uint32_t)mll; } } }
                free(meta);
            }
            if (!run) { sqlite3_finalize(st); sqlite3_finalize(mt); rsx_man_release(m); return NULL; }
            if (m->n == cap) { int nc = cap ? cap * 2 : 16; rs_run **nr = realloc(m->runs, (size_t)nc * sizeof *nr); if (!nr) { rs_run_unref(run); sqlite3_finalize(st); sqlite3_finalize(mt); rsx_man_release(m); return NULL; } m->runs = nr; cap = nc; }
            m->runs[m->n++] = run;
        }
        if (r != SQLITE_DONE) { sqlite3_finalize(st); sqlite3_finalize(mt); rsx_man_release(m); return NULL; }
    }
    sqlite3_finalize(st); sqlite3_finalize(mt); st = NULL;
    int dcap = 0;
    prc = sqlite3_prepare_v2(c, "SELECT tbl, dv FROM mw_drops", -1, &st, NULL);
    if (hard_err(prc)) { sqlite3_finalize(st); rsx_man_release(m); return NULL; }
    if (prc == SQLITE_OK) {
        int dr;
        while ((dr = sqlite3_step(st)) == SQLITE_ROW) {
            if (m->nd == dcap) { int nc = dcap ? dcap * 2 : 8; rdrop *nd = realloc(m->drops, (size_t)nc * sizeof *nd); if (!nd) { dr = SQLITE_NOMEM; break; } m->drops = nd; dcap = nc; }
            m->drops[m->nd++] = (rdrop){ (uint32_t)sqlite3_column_int64(st, 0), sqlite3_column_int64(st, 1) };
        }
        if (dr != SQLITE_DONE) { sqlite3_finalize(st); rsx_man_release(m); return NULL; }
    }
    sqlite3_finalize(st);
    qsort(m->runs, (size_t)m->n, sizeof *m->runs, run_newest_first);
    return m;
}

static uint32_t compute_slot_bytes (sqlite3 *c);
int rsx_man (mw_rstore *s, sqlite3 *c, mw_rman **out) {
    int64_t ver; if (state_get(c, "runs_ver", 0, &ver)) return -1;
    if (!atomic_load(&s->slot_bytes)) { int64_t sb; if (state_get(c, "slot_bytes", 0, &sb)) return -1; if (!sb) sb = compute_slot_bytes(c); atomic_store(&s->slot_bytes, (uint32_t)sb); }
    pthread_mutex_lock(&s->mu);
    mw_rman *cur = s->cur;
    if (cur && cur->ver == ver) { man_ref(cur); pthread_mutex_unlock(&s->mu); *out = cur; return 0; }
    if (cur) man_ref(cur);
    pthread_mutex_unlock(&s->mu);
    mw_rman *m = man_load(c, cur, ver, atomic_load(&s->slot_bytes));
    if (cur) rsx_man_release(cur);
    if (!m) return -1;
    pthread_mutex_lock(&s->mu);
    if (!s->cur || s->cur->ver < ver) { man_ref(m); mw_rman *o = s->cur; s->cur = m; backlog_update(s, m); pthread_mutex_unlock(&s->mu); rsx_man_release(o); }
    else pthread_mutex_unlock(&s->mu);
    *out = m;
    return 0;
}

// ---- the store ----
mw_rstore *rsx_new (size_t cache_bytes) {
    mw_rstore *s = calloc(1, sizeof *s); if (!s) return NULL;
    pthread_mutex_init(&s->mu, NULL); pthread_mutex_init(&s->wmu, NULL);
    for (int i = 0; i < CSTRIPES; i++) pthread_mutex_init(&s->cmu[i], NULL);
    pthread_mutex_init(&s->pmu, NULL);
    s->ncs = cache_bytes / (RS_BLOCK_TARGET + 1024); if (s->ncs < 64) s->ncs = 64;
    s->cs = calloc(s->ncs, sizeof *s->cs);
    if (!s->cs) { free(s); return NULL; }
    return s;
}
static void cblk_unref (cblk *b) { if (b && atomic_fetch_sub(&b->refs, 1) == 1) free(b); }
// The file tables went back to an earlier state (recovery in place rolled the database back to its last durable commit): the list of runs and the blocks that were read, the slots this process had taken
// (the transactions that took them were rolled back too) and the numbers it had taken for runs are of a timeline that no longer exists, and run numbers and versions will be used again. Nothing else runs on the store.
void rsx_reset (mw_rstore *s) {
    if (!s) return;
    pthread_mutex_lock(&s->mu); mw_rman *o = s->cur; s->cur = NULL; atomic_store(&s->bl0, 0); atomic_store(&s->ball, 0); pthread_mutex_unlock(&s->mu);
    rsx_man_release(o);
    for (size_t i = 0; i < s->ncs; i++) { pthread_mutex_t *mu = &s->cmu[i % CSTRIPES]; pthread_mutex_lock(mu); cblk *b = s->cs[i].b; s->cs[i].b = NULL; pthread_mutex_unlock(mu); cblk_unref(b); }
    pthread_mutex_lock(&s->pmu); s->npool = 0; s->nh = 0; pthread_mutex_unlock(&s->pmu);
}
void rsx_free (mw_rstore *s) {
    if (!s) return;
    for (size_t i = 0; i < s->ncs; i++) cblk_unref(s->cs[i].b);
    free(s->cs); free(s->pool); free(s->hslot); free(s->howner); pthread_mutex_destroy(&s->pmu); rsx_man_release(s->cur);
    for (int i = 0; i < CSTRIPES; i++) pthread_mutex_destroy(&s->cmu[i]);
    pthread_mutex_destroy(&s->mu); pthread_mutex_destroy(&s->wmu); free(s);
}
void rsx_backlog (mw_rstore *s, int *l0, int *total) { *l0 = atomic_load_explicit(&s->bl0, memory_order_relaxed); *total = atomic_load_explicit(&s->ball, memory_order_relaxed); }
static void backlog_update (mw_rstore *s, const mw_rman *m) {                 // (the caller holds s->mu)
    int a = 0, t = 0; int64_t last0 = -1, last = -1;
    for (int i = 0; i < m->n; i++) {                                           // (newest first: equal ages are next to each other)
        const rs_run *r = m->runs[i];
        if (r->age != last) { t++; last = r->age; }
        if (r->lvl == 0 && r->age != last0) { a++; last0 = r->age; }
    }
    atomic_store_explicit(&s->bl0, a, memory_order_relaxed); atomic_store_explicit(&s->ball, t, memory_order_relaxed);
}
void rsx_set_exclusive (mw_rstore *s, bool on) { s->exclusive = on; }
void rsx_wlock (mw_rstore *s) { pthread_mutex_lock(&s->wmu); }
void rsx_wunlock (mw_rstore *s) { pthread_mutex_unlock(&s->wmu); }
void rsx_stats_get (mw_rstore *s, rsx_stats *o) {
    o->gets = atomic_load(&s->gets); o->run_probes = atomic_load(&s->run_probes); o->bloom_skips = atomic_load(&s->bloom_skips); o->blk_reads = atomic_load(&s->blk_reads);
    o->cache_hits = atomic_load(&s->cache_hits); o->merges = atomic_load(&s->merges); o->merged_rows = atomic_load(&s->merged_rows); o->runs_written = atomic_load(&s->runs_written);
    pthread_mutex_lock(&s->mu); o->nruns = s->cur ? s->cur->n : 0; o->merge_retries = atomic_load(&s->merge_retries); o->swept_slots = atomic_load(&s->swept_slots); pthread_mutex_unlock(&s->mu);
}

// ---- where the blocks are: slots ----
// A block lives in a few slots: rows of mw_slots, each of exactly slot_bytes bytes (the last one padded), so that a slot that is written again is overwritten where it is (SQLite does that when the
// new record has the size of the old one): no page of the file is allocated or given back to the free list, which the writers of the application would meet at every commit. A run that goes
// gives its slots to mw_free; the next run takes them. The locator of a block (in the meta of its run) is the varints of its slots.

// a whole block, as stored (compressed or not), from its slots; false: a slot is missing
static bool read_block (mw_rstore *s, sqlite3_stmt *st, const rs_run *r, uint32_t blk, uint8_t **data, size_t *len) {
    uint32_t stored, loclen; const uint8_t *loc; rs_run_block_info(r, blk, &stored, &loc, &loclen);
    uint32_t sb = atomic_load(&s->slot_bytes); if (!sb) return false;
    uint8_t *buf = malloc(stored ? stored : 1); if (!buf) return false;
    const uint8_t *p = loc, *end = loc + loclen; size_t got = 0; bool ok = true;
    while (ok && got < stored) {
        uint64_t slot; if (!sl_get(&p, end, &slot)) { ok = false; break; }
        sqlite3_bind_int64(st, 1, (int64_t)slot);
        if (sqlite3_step(st) == SQLITE_ROW && (uint32_t)sqlite3_column_bytes(st, 0) == sb) {
            const uint8_t *d = sqlite3_column_blob(st, 0); uint8_t want[SLOT_HDR]; hdr_put(want, r->id, blk, (uint32_t)(got / (sb - SLOT_HDR)));
            if (memcmp(d, want, SLOT_HDR)) { ok = false; if (getenv("MW_DBG_HDR")) { uint64_t rr; uint32_t bb, pp; memcpy(&rr, d, 8); memcpy(&bb, d + 8, 4); memcpy(&pp, d + 12, 4); fprintf(stderr, "STALE SLOT %llu read for run %lld block %u part %zu: it holds run %llu block %u part %u\n", (unsigned long long)slot, (long long)r->id, blk, got / (sb - SLOT_HDR), (unsigned long long)rr, bb, pp); } }
            else { size_t n = stored - got < sb - SLOT_HDR ? stored - got : sb - SLOT_HDR; memcpy(buf + got, d + SLOT_HDR, n); got += n; }
        } else ok = false;
        sqlite3_reset(st); sqlite3_clear_bindings(st);
    }
    if (!ok) { free(buf); return false; }
    *data = buf; *len = stored;
    return true;
}

// a block of the cache or of the file (a counted copy; NULL: it is not there)
static cblk *fetch_block (mw_rstore *s, sqlite3_stmt *st, const rs_run *run, uint32_t blk) {
    uint64_t h = ((uint64_t)run->id * 0x9E3779B97F4A7C15ull) ^ ((uint64_t)blk * 0xC2B2AE3D27D4EB4Full); h ^= h >> 31;
    size_t idx = (size_t)(h % s->ncs); pthread_mutex_t *mu = &s->cmu[idx % CSTRIPES];
    pthread_mutex_lock(mu);
    cslot *sl = &s->cs[idx];
    if (sl->b && sl->run == run->id && sl->blk == blk) { cblk *b = sl->b; atomic_fetch_add(&b->refs, 1); pthread_mutex_unlock(mu); atomic_fetch_add(&s->cache_hits, 1); return b; }
    pthread_mutex_unlock(mu);
    cblk *b = NULL; uint8_t *raw = NULL; size_t rawlen = 0;
    if (read_block(s, st, run, blk, &raw, &rawlen)) {
        rs_blk tmp; uint8_t *own = NULL;
        if (rs_blk_unpack(&tmp, raw, rawlen, &own)) {                                                                    // (the cache holds blocks as they are used: decompressed)
            b = malloc(sizeof *b + tmp.len);
            if (b) { atomic_init(&b->refs, 2); b->len = tmp.len; memcpy(b->d, tmp.data, tmp.len); }                    // (one count for the caller, one for the entry)
        }
        free(own); free(raw);
    }
    atomic_fetch_add(&s->blk_reads, 1);
    if (!b) return NULL;
    pthread_mutex_lock(mu);
    cblk *old = sl->b; sl->b = b; sl->run = run->id; sl->blk = blk;
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
        cblk *cb = fetch_block(s, st, r, (uint32_t)b); if (!cb) return -1;
        rs_blk blk; int64_t dv; const uint8_t *c; uint32_t nc; int f = rs_blk_open(&blk, cb->d, cb->len) ? rs_blk_find(&blk, &k, &dv, &c, &nc) : -1;
        rs_blk_close(&blk);
        if (f < 0) { cblk_unref(cb); return -1; }
        if (f == 0) { cblk_unref(cb); continue; }
        int rc = 0;
        if (nc) {
            if (!mw_meta_row_cells(c, nc, dv, cells, n)) rc = -1;
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
    bool own = sqlite3_get_autocommit(c) != 0; if (own) sqlite3_exec(c, "BEGIN", NULL, NULL, NULL);            // (the list of runs and their slots from one snapshot)
    mw_rman *m; if (rsx_man(s, c, &m) != 0) { if (own) sqlite3_exec(c, "COMMIT", NULL, NULL, NULL); return -1; }
    if (!m->n) { rsx_man_release(m); if (own) sqlite3_exec(c, "COMMIT", NULL, NULL, NULL); return 0; }
    sqlite3_stmt *st = NULL; if (sqlite3_prepare_v2(c, rsx_blk_sql(), -1, &st, NULL) != SQLITE_OK) { rsx_man_release(m); if (own) sqlite3_exec(c, "COMMIT", NULL, NULL, NULL); return -1; }
    int rc = 0;
    for (int ri = 0; ri < m->n && !rc; ri++) {
        const rs_run *r = m->runs[ri];
        for (uint32_t bi = 0; bi < r->nblk && !rc; bi++) {
            uint8_t *raw; size_t rl; if (!read_block(s, st, r, bi, &raw, &rl)) { rc = -1; break; }
            rs_blk b; uint8_t *own; if (!rs_blk_unpack(&b, raw, rl, &own)) { free(raw); rc = -1; break; }
            for (uint32_t i = 0; i < b.nrows; i++) { rs_key k; int64_t dv; const uint8_t *cells; uint32_t nc; if (!rs_blk_row(&b, i, &k, &dv, &cells, &nc)) { rc = -1; break; } if (nc) cb(ctx, k.tbl, k.pk, k.pklen); }
            rs_blk_close(&b); free(own); free(raw);
        }
    }
    sqlite3_finalize(st); rsx_man_release(m); if (own) sqlite3_exec(c, "COMMIT", NULL, NULL, NULL);
    return rc;
}
int rsx_scan_since (mw_rstore *s, sqlite3 *c, mw_rman *m, int64_t since, void (*cb)(void *, uint32_t, const uint8_t *, size_t), void *ctx) {
    sqlite3_stmt *st = NULL; if (sqlite3_prepare_v2(c, rsx_blk_sql(), -1, &st, NULL) != SQLITE_OK) { sqlite3_finalize(st); return m->n ? -1 : 0; }
    int rc = 0;
    for (int ri = 0; ri < m->n && !rc; ri++) {
        const rs_run *r = m->runs[ri]; if (r->dvmax <= since) continue;
        for (uint32_t bi = 0; bi < r->nfence && !rc; bi++) {
            if (r->fdv[bi] <= since) continue;
            cblk *cb_ = fetch_block(s, st, r, bi); if (!cb_) { rc = -1; break; }
            rs_blk b; if (!rs_blk_open(&b, cb_->d, cb_->len)) rc = -1;
            for (uint32_t i = 0; !rc && i < b.nrows; i++) { rs_key k; int64_t dv; const uint8_t *cells; uint32_t nc; if (!rs_blk_row(&b, i, &k, &dv, &cells, &nc)) { rc = -1; break; } if (nc && dv > since) cb(ctx, k.tbl, k.pk, k.pklen); }
            rs_blk_close(&b); cblk_unref(cb_);
        }
    }
    sqlite3_finalize(st);
    return rc;
}

// ---- reading blocks outside the cache (merges, full scans) ----
// A block of a run for a merge: read in a transaction that also finds the run in the list (the slots of a run that went are free, and somebody may have written another run into them: the manifest the merge
// works from is older than this read, and in between, in another process, the run may have been removed)
typedef struct { mw_rstore *s; sqlite3_stmt *st; sqlite3 *c; sqlite3_stmt *exists; bool own_txn; } plainctx;
static int plain_read (void *ctx, const rs_run *r, uint32_t blk, uint8_t **data, size_t *len) {
    plainctx *p = ctx; int rc = -1;
    if (!p->own_txn) return read_block(p->s, p->st, r, blk, data, len) ? 0 : -1;                    // (inside a transaction of the caller: one snapshot already)
    if (p->s->exclusive) {                                                                          // (nobody else removes runs: no need to find the run in the list, only one snapshot for the slots of the block)
        if (sqlite3_exec(p->c, "BEGIN", NULL, NULL, NULL) != SQLITE_OK) return -1;
        rc = read_block(p->s, p->st, r, blk, data, len) ? 0 : -1;
        sqlite3_exec(p->c, "COMMIT", NULL, NULL, NULL);
        return rc;
    }
    if (sqlite3_exec(p->c, "BEGIN", NULL, NULL, NULL) != SQLITE_OK) return -1;
    if (!p->exists) sqlite3_prepare_v2(p->c, "SELECT 1 FROM mw_runs WHERE run = ?1", -1, &p->exists, NULL);
    if (p->exists) {
        sqlite3_bind_int64(p->exists, 1, r->id);
        bool there = sqlite3_step(p->exists) == SQLITE_ROW; sqlite3_reset(p->exists);
        if (there && read_block(p->s, p->st, r, blk, data, len)) rc = 0;
    }
    sqlite3_exec(p->c, "COMMIT", NULL, NULL, NULL);
    return rc;
}
static bool keep_alive (void *ctx, const rs_key *k, int64_t dv, const uint8_t *cells, uint32_t nc) { (void)cells; (void)nc; const mw_rman *m = ctx; return dv >= drop_of(m, k->tbl); }

typedef struct { int (*cb)(void *, uint32_t, const uint8_t *, size_t, const mw_mcell *, int); void *ctx; int err; const mw_rman *m; } scanctx;
static int scan_emit (void *ctx, uint32_t blkno, const uint8_t *data, size_t len, const uint8_t **loc, size_t *loclen) {
    *loc = NULL; *loclen = 0;
    (void)blkno; scanctx *sc = ctx; rs_blk b; uint8_t *own; if (!rs_blk_unpack(&b, data, len, &own)) return -1;
    int rcx = 0;
    for (uint32_t i = 0; !rcx && i < b.nrows; i++) {
        rs_key k; int64_t dv; const uint8_t *cells; uint32_t nc; if (!rs_blk_row(&b, i, &k, &dv, &cells, &nc)) { rcx = -1; break; }
        mw_mcell *c; int n; if (!mw_meta_row_cells(cells, nc, dv, &c, &n)) { rcx = -1; break; }
        int64_t dd = drop_of(sc->m, k.tbl); if (dd) { int q = 0; for (int x = 0; x < n; x++) if (c[x].dv >= dd) c[q++] = c[x]; n = q; }
        rcx = n ? sc->cb(sc->ctx, k.tbl, k.pk, k.pklen, c, n) : 0; free(c);
    }
    rs_blk_close(&b); free(own);
    return rcx;
}
static int scan_begin (void *sctx, uint64_t hint, rs_emit_fn *emit, void **ec) { (void)hint; *emit = scan_emit; *ec = sctx; return 0; }
static int scan_end (void *sctx, const uint8_t *meta, size_t ml, uint64_t nr, uint32_t nb, int64_t dv) { (void)sctx; (void)meta; (void)ml; (void)nr; (void)nb; (void)dv; return 0; }
int rsx_scan_all (mw_rstore *s, sqlite3 *c, int (*cb)(void *, uint32_t, const uint8_t *, size_t, const mw_mcell *, int), void *ctx) {
    mw_rman *m; if (rsx_man(s, c, &m) != 0) return -1;
    if (!m->n) { rsx_man_release(m); return 0; }
    plainctx pc = { s, NULL, c, NULL, false }; if (sqlite3_prepare_v2(c, rsx_blk_sql(), -1, &pc.st, NULL) != SQLITE_OK) { rsx_man_release(m); return -1; }
    scanctx sc = { cb, ctx, 0, m };
    rs_merge_opts o = { plain_read, &pc, true, keep_alive, m, 0, scan_begin, scan_end, &sc };
    int rc = rs_merge(m->runs, m->n, &o, NULL);
    sqlite3_finalize(pc.st); rsx_man_release(m);
    return rc;
}

// ---- small transactions ----
// A transaction of the merge that is not the one that changes the list of runs: no transaction of the flush is held up by it, and when it meets a commit of the writers it is small enough to be
// done again at no cost. The retry loop of a transaction `fn` on `c`; a flush takes turns with it (they write into the same b-trees).
typedef int (*small_fn)(void *ctx, sqlite3 *c);
static int small_txn (mw_rstore *s, sqlite3 *c, small_fn fn, void *ctx) {
    int rc = SQLITE_BUSY;
    for (int attempt = 0; attempt < 5000 && busyish(rc); attempt++) {
        if (attempt) atomic_fetch_add(&s->merge_retries, 1);
        backoff(attempt);
        rsx_wlock(s);
        rc = sqlite3_exec(c, "BEGIN", NULL, NULL, NULL); if (rc) { rsx_wunlock(s); continue; }
        rc = fn(ctx, c);
        if (!rc) rc = sqlite3_exec(c, "COMMIT", NULL, NULL, NULL);
        if (rc) sqlite3_exec(c, "ROLLBACK", NULL, NULL, NULL);
        rsx_wunlock(s);
    }
    return rc;
}

// ---- slots: taking and giving back ----
#define RSX_NEED_SLOTS 0x7e5e01                       // (inside: a transaction ran out of the slots that were taken for it)
static uint32_t compute_slot_bytes (sqlite3 *c) {
    sqlite3_stmt *st = NULL; int pg = 4096;
    if (sqlite3_prepare_v2(c, "PRAGMA page_size", -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW) pg = sqlite3_column_int(st, 0);
    sqlite3_finalize(st);
    uint32_t sb = ((uint32_t)(pg - 8) / 4 - 12) & ~7u;       // four slots fill a leaf page of the file (their cells, pointers and the header of the page): nothing is wasted, and no slot overflows
    return sb < 64 ? 64 : sb;
}
static void pool_put (mw_rstore *s, const uint32_t *sl, int n) {
    pthread_mutex_lock(&s->pmu);
    if (s->npool + (size_t)n > s->cappool) { size_t nc = (s->npool + (size_t)n) * 2 + 256; uint32_t *np = realloc(s->pool, nc * sizeof *np); if (!np) { pthread_mutex_unlock(&s->pmu); return; } s->pool = np; s->cappool = nc; }
    memcpy(s->pool + s->npool, sl, (size_t)n * sizeof *sl); s->npool += (size_t)n;
    pthread_mutex_unlock(&s->pmu);
}
static int pool_take (mw_rstore *s, int n, uint32_t *out) {                  // 0, or -1 when there are not n (none is taken)
    pthread_mutex_lock(&s->pmu);
    if (s->npool < (size_t)n) { pthread_mutex_unlock(&s->pmu); return -1; }
    s->npool -= (size_t)n; memcpy(out, s->pool + s->npool, (size_t)n * sizeof *out);
    pthread_mutex_unlock(&s->pmu);
    return 0;
}
static int held_add (mw_rstore *s, const void *owner, const uint32_t *sl, int k) {
    pthread_mutex_lock(&s->pmu);
    if (s->nh + (size_t)k > s->caph) { size_t nc = (s->nh + (size_t)k) * 2 + 256; uint32_t *a = realloc(s->hslot, nc * sizeof *a); uintptr_t *b = realloc(s->howner, nc * sizeof *b); if (a) s->hslot = a; if (b) s->howner = b; if (!a || !b) { pthread_mutex_unlock(&s->pmu); return -1; } s->caph = nc; }
    for (int i = 0; i < k; i++) { s->hslot[s->nh] = sl[i]; s->howner[s->nh] = (uintptr_t)owner; s->nh++; }
    pthread_mutex_unlock(&s->pmu);
    return 0;
}
static void held_drop (mw_rstore *s, const void *owner) {
    pthread_mutex_lock(&s->pmu);
    size_t j = 0; for (size_t i = 0; i < s->nh; i++) if (s->howner[i] != (uintptr_t)owner) { s->hslot[j] = s->hslot[i]; s->howner[j] = s->howner[i]; j++; }
    s->nh = j;
    pthread_mutex_unlock(&s->pmu);
}
// every slot this process has (pool and held), plus `extra`, as sorted ranges [gap from the end of the last, length] in varints: the entry of this process in the register of reservations
static int slot_cmp (const void *a, const void *b);
static int ledger_encode (mw_rstore *s, const uint32_t *extra, int nextra, uint8_t **out, size_t *outlen) {
    pthread_mutex_lock(&s->pmu);
    size_t n = s->npool + s->nh + (size_t)nextra; uint32_t *all = malloc((n ? n : 1) * sizeof *all);
    if (!all) { pthread_mutex_unlock(&s->pmu); return -1; }
    size_t k = 0; memcpy(all, s->pool, s->npool * sizeof *all); k = s->npool; memcpy(all + k, s->hslot, s->nh * sizeof *all); k += s->nh; memcpy(all + k, extra, (size_t)nextra * sizeof *all); k += (size_t)nextra;
    pthread_mutex_unlock(&s->pmu);
    qsort(all, k, sizeof *all, slot_cmp);
    uint8_t *buf = malloc(k * 10 + 16); if (!buf) { free(all); return -1; }
    size_t w = 0; uint64_t prev_end = 0;
    for (size_t i = 0; i < k; ) {
        size_t j = i + 1; while (j < k && (all[j] == all[j - 1] || all[j] == all[j - 1] + 1)) j++;
        w += sl_put(buf + w, all[i] - prev_end); w += sl_put(buf + w, (uint64_t)all[j - 1] - all[i] + 1); prev_end = (uint64_t)all[j - 1] + 1; i = j;
    }
    free(all); *out = buf; *outlen = w; return 0;
}
static int ledger_put (mw_rstore *s, sqlite3 *c, const uint32_t *extra, int nextra) {
    uint8_t *b; size_t bl; if (ledger_encode(s, extra, nextra, &b, &bl)) return SQLITE_NOMEM;
    sqlite3_stmt *st = NULL; int rc = sqlite3_prepare_v2(c, "INSERT OR REPLACE INTO mw_resv(pid, slots) VALUES(?1, ?2)", -1, &st, NULL);
    if (!rc) { sqlite3_bind_int64(st, 1, (int64_t)getpid()); sqlite3_bind_blob(st, 2, b, (int)bl, SQLITE_STATIC); rc = sqlite3_step(st); rc = rc == SQLITE_DONE ? 0 : rc; }
    sqlite3_finalize(st); free(b); return rc;
}
// the slots of the free table first (the lowest), then fresh ones past the last in use: taken out of the table (or counted) in one small transaction, and then they are ours
// The free slots are a bitmap in rows of FREE_CHUNK_BYTES bytes (FREE_CHUNK_SLOTS slots each), every row the same size: freeing and taking slots overwrites rows in place, so this table never allocates
// or gives back a page of the file either.
#define FREE_CHUNK_BYTES 512
#define FREE_CHUNK_SLOTS (FREE_CHUNK_BYTES * 8)
typedef struct { mw_rstore *s; int want; uint32_t *got; int n; } rsvctx;
static int rsv_fn (void *ctx, sqlite3 *c) {
    rsvctx *x = ctx; x->n = 0; sqlite3_stmt *st = NULL, *up = NULL;
    int rc = sqlite3_prepare_v2(c, "SELECT chunk, bits FROM mw_free ORDER BY chunk", -1, &st, NULL); if (rc) return rc;
    rc = sqlite3_prepare_v2(c, "UPDATE mw_free SET bits = ?2 WHERE chunk = ?1", -1, &up, NULL);
    uint8_t bits[FREE_CHUNK_BYTES];
    while (!rc && x->n < x->want && sqlite3_step(st) == SQLITE_ROW) {
        int64_t chunk = sqlite3_column_int64(st, 0); if (sqlite3_column_bytes(st, 1) != FREE_CHUNK_BYTES) continue;
        memcpy(bits, sqlite3_column_blob(st, 1), FREE_CHUNK_BYTES); bool changed = false;
        for (int i = 0; i < FREE_CHUNK_BYTES && x->n < x->want; i++) if (bits[i]) for (int b = 0; b < 8 && x->n < x->want; b++) if (bits[i] & (1u << b)) {
            int64_t slot = chunk * FREE_CHUNK_SLOTS + i * 8 + b; if (slot <= 0) continue;
            x->got[x->n++] = (uint32_t)slot; bits[i] &= (uint8_t)~(1u << b); changed = true;
        }
        if (changed) { sqlite3_bind_int64(up, 1, chunk); sqlite3_bind_blob(up, 2, bits, FREE_CHUNK_BYTES, SQLITE_STATIC); int r2 = sqlite3_step(up); sqlite3_reset(up); if (r2 != SQLITE_DONE) rc = r2; }
    }
    sqlite3_finalize(st); sqlite3_finalize(up);
    if (rc) return rc;
    if (x->n < x->want) {
        int64_t next; rc = state_get(c, "next_slot", 1, &next); if (rc) return rc;
        for (int i = x->n; i < x->want; i++) x->got[i] = (uint32_t)(next + (i - x->n));
        rc = state_put(c, "next_slot", next + (x->want - x->n)); if (rc) return rc;
        x->n = x->want;
    }
    { int64_t sb; rc = state_get(c, "slot_bytes", 0, &sb); if (rc) return rc; if (!sb) { rc = state_put(c, "slot_bytes", compute_slot_bytes(c)); if (rc) return rc; } }
    return ledger_put(x->s, c, x->got, x->n);                                                    // (what this process holds, written with the reservation: a sweep of another process knows whose they are)
}
static int slot_cmp (const void *a, const void *b) { uint32_t x = *(const uint32_t *)a, y = *(const uint32_t *)b; return x < y ? -1 : x > y; }
// the slots become free: their bits are set (one read and one write of a row for all the slots of its chunk)
static int free_slots (sqlite3 *c, uint32_t *sl, size_t n) {
    if (!n) return 0;
    qsort(sl, n, sizeof *sl, slot_cmp);
    sqlite3_stmt *sel = NULL, *up = NULL, *ins = NULL;
    int rc = sqlite3_prepare_v2(c, "SELECT bits FROM mw_free WHERE chunk = ?1", -1, &sel, NULL);
    if (!rc) rc = sqlite3_prepare_v2(c, "UPDATE mw_free SET bits = ?2 WHERE chunk = ?1", -1, &up, NULL);
    if (!rc) rc = sqlite3_prepare_v2(c, "INSERT INTO mw_free(chunk, bits) VALUES(?1, ?2)", -1, &ins, NULL);
    uint8_t bits[FREE_CHUNK_BYTES];
    for (size_t i = 0; !rc && i < n; ) {
        int64_t chunk = (int64_t)(sl[i] / FREE_CHUNK_SLOTS); bool have = false;
        sqlite3_bind_int64(sel, 1, chunk);
        if (sqlite3_step(sel) == SQLITE_ROW && sqlite3_column_bytes(sel, 0) == FREE_CHUNK_BYTES) { memcpy(bits, sqlite3_column_blob(sel, 0), FREE_CHUNK_BYTES); have = true; } else memset(bits, 0, sizeof bits);
        sqlite3_reset(sel);
        while (i < n && (int64_t)(sl[i] / FREE_CHUNK_SLOTS) == chunk) { uint32_t o = sl[i] % FREE_CHUNK_SLOTS; bits[o / 8] |= (uint8_t)(1u << (o % 8)); i++; }
        sqlite3_stmt *w = have ? up : ins; sqlite3_bind_int64(w, 1, chunk); sqlite3_bind_blob(w, 2, bits, FREE_CHUNK_BYTES, SQLITE_STATIC);
        int r2 = sqlite3_step(w); sqlite3_reset(w); if (r2 != SQLITE_DONE) rc = r2;
    }
    sqlite3_finalize(sel); sqlite3_finalize(up); sqlite3_finalize(ins);
    return rc;
}
static int pool_reserve (mw_rstore *s, sqlite3 *c, int want) {            // (no transaction of c is open; the writers' turn is taken inside)
    rsvctx x = { s, want, malloc((size_t)want * sizeof(uint32_t)), 0 }; if (!x.got) return SQLITE_NOMEM;
    int rc = small_txn(s, c, rsv_fn, &x);
    if (!rc) pool_put(s, x.got, x.n);
    free(x.got); return rc;
}
static size_t pool_batch (void) { static size_t b; if (!b) { const char *e = getenv("MW_POOL_BATCH"); b = e && atol(e) > 0 ? (size_t)atol(e) : 8192; } return b; }
static int pool_ensure (mw_rstore *s, sqlite3 *c, size_t need) {
    pthread_mutex_lock(&s->pmu); size_t have = s->npool; pthread_mutex_unlock(&s->pmu);
    if (have >= need) return 0;
    size_t want = need - have + 64; if (want < pool_batch()) want = pool_batch();                          // (a transaction for every few slots would be all the writing of the flush and the merge: take them by the thousand)
    return pool_reserve(s, c, (int)want);
}

// writing the slots of a block (in place when the slot exists: the same size)
typedef struct { sqlite3_stmt *upd, *ins; uint8_t *pad; uint32_t sb; } slotw;
static int slotw_open (slotw *w, sqlite3 *c, uint32_t sb) {
    memset(w, 0, sizeof *w); w->sb = sb; w->pad = malloc(sb); if (!w->pad) return SQLITE_NOMEM;
    if (sqlite3_prepare_v2(c, "UPDATE mw_slots SET data = ?2 WHERE slot = ?1", -1, &w->upd, NULL) != SQLITE_OK) return sqlite3_errcode(c);
    if (sqlite3_prepare_v2(c, "INSERT INTO mw_slots(slot, data) VALUES(?1, ?2)", -1, &w->ins, NULL) != SQLITE_OK) return sqlite3_errcode(c);
    return 0;
}
static void slotw_close (slotw *w) { sqlite3_finalize(w->upd); sqlite3_finalize(w->ins); free(w->pad); memset(w, 0, sizeof *w); }
static int slotw_write (slotw *w, sqlite3 *c, uint32_t slot, int64_t run, uint32_t blk, uint32_t part, const uint8_t *chunk, size_t n) {
    hdr_put(w->pad, run, blk, part); memcpy(w->pad + SLOT_HDR, chunk, n); if (n + SLOT_HDR < w->sb) memset(w->pad + SLOT_HDR + n, 0, w->sb - SLOT_HDR - n);
    sqlite3_bind_int64(w->upd, 1, slot); sqlite3_bind_blob(w->upd, 2, w->pad, (int)w->sb, SQLITE_STATIC);
    int rc = sqlite3_step(w->upd); sqlite3_reset(w->upd);
    if (rc != SQLITE_DONE) return rc ? rc : -1;
    if (sqlite3_changes(c) > 0) return 0;
    sqlite3_bind_int64(w->ins, 1, slot); sqlite3_bind_blob(w->ins, 2, w->pad, (int)w->sb, SQLITE_STATIC);
    rc = sqlite3_step(w->ins); sqlite3_reset(w->ins);
    return rc == SQLITE_DONE ? 0 : (rc ? rc : -1);
}
// the locator of a block: its slots as varints (in `*buf`, grown as needed)
static int loc_encode (const uint32_t *sl, int n, uint8_t **buf, size_t *cap, size_t *len) {
    if (*cap < (size_t)n * 5 + 8) { size_t nc = (size_t)n * 5 + 64; uint8_t *nb = realloc(*buf, nc); if (!nb) return -1; *buf = nb; *cap = nc; }
    size_t w = 0; for (int i = 0; i < n; i++) w += sl_put(*buf + w, sl[i]);
    *len = w; return 0;
}

// ---- writing ----
struct rsx_tx {
    mw_rstore *s; mw_rman *base; int64_t next_run, next_age;
    rs_run **add; int nadd, capadd; int64_t *rem; int nrem, caprem; rdrop *drops; int nd;
    sqlite3_stmt *ins_run; slotw sw; bool sw_open;
    uint32_t *taken; int ntaken, captaken;                                   // slots taken from the pool for this transaction: back to it if the transaction does not commit
};
rsx_tx *rsx_tx_begin (mw_rstore *s, sqlite3 *c) {
    rsx_tx *t = calloc(1, sizeof *t); if (!t) return NULL;
    t->s = s;
    if (rsx_man(s, c, &t->base) != 0) { free(t); return NULL; }
    int64_t maxid = 0, maxage = 0; for (int i = 0; i < t->base->n; i++) { if (t->base->runs[i]->id > maxid) maxid = t->base->runs[i]->id; if (t->base->runs[i]->age > maxage) maxage = t->base->runs[i]->age; }
    if (state_get(c, "next_run", 1, &t->next_run) || state_get(c, "next_age", 1, &t->next_age)) { rsx_man_release(t->base); free(t); return NULL; }
    if (t->next_run <= maxid) t->next_run = maxid + 1;
    if (t->next_age <= maxage) t->next_age = maxage + 1;
    return t;
}
static int tx_prepare (rsx_tx *t, sqlite3 *c) {
    if (!t->ins_run && sqlite3_prepare_v2(c, "INSERT INTO mw_runs(run, age, lvl, nrows, nblk, dvmax, metalen, metaloc) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)", -1, &t->ins_run, NULL) != SQLITE_OK) return sqlite3_errcode(c);
    if (!t->sw_open) { int rc = slotw_open(&t->sw, c, atomic_load(&t->s->slot_bytes)); if (rc) return rc; t->sw_open = true; }
    return SQLITE_OK;
}
// k slots from the pool for this transaction (they go back to the pool if it does not commit)
static int tx_take (rsx_tx *t, int k, uint32_t *sl) {
    if (pool_take(t->s, k, sl) != 0) return RSX_NEED_SLOTS;
    if (held_add(t->s, t, sl, k)) { pool_put(t->s, sl, k); return -1; }
    if (t->ntaken + k > t->captaken) { int nc = (t->ntaken + k) * 2 + 64; uint32_t *nt = realloc(t->taken, (size_t)nc * sizeof *nt); if (!nt) { pool_put(t->s, sl, k); return -1; } t->taken = nt; t->captaken = nc; }
    memcpy(t->taken + t->ntaken, sl, (size_t)k * sizeof *sl); t->ntaken += k;
    return 0;
}
typedef struct { rsx_tx *t; sqlite3 *c; uint8_t *loc; size_t loccap; int64_t run; } wctx;
static int emit_db (void *ctx, uint32_t blk, const uint8_t *data, size_t len, const uint8_t **loc, size_t *loclen) {
    wctx *w = ctx; rsx_tx *t = w->t; uint32_t sb = t->sw.sb - SLOT_HDR;
    int k = (int)((len + sb - 1) / sb); uint32_t small[16]; uint32_t *sl = k <= 16 ? small : malloc((size_t)k * sizeof *sl); if (!sl) return -1;
    int rc = tx_take(t, k, sl); if (rc) goto out;
    for (int j = 0; !rc && j < k; j++) { size_t off = (size_t)j * sb, n = len - off < sb ? len - off : sb; rc = slotw_write(&t->sw, w->c, sl[j], w->run, blk, (uint32_t)j, data + off, n); }
    if (!rc) { size_t ll; rc = loc_encode(sl, k, &w->loc, &w->loccap, &ll); *loc = w->loc; *loclen = ll; }
out:
    if (sl != small) free(sl);
    return rc;
}
static int tx_push_run (rsx_tx *t, rs_run *r) {
    if (t->nadd == t->capadd) { int nc = t->capadd ? t->capadd * 2 : 8; rs_run **na = realloc(t->add, (size_t)nc * sizeof *na); if (!na) return -1; t->add = na; t->capadd = nc; }
    t->add[t->nadd++] = r; return 0;
}
static int tx_insert_run (rsx_tx *t, sqlite3 *c, int64_t id, int64_t age, int lvl, const uint8_t *meta, size_t ml, uint64_t nr, uint32_t nb, int64_t dvm) {
    uint32_t sb = t->sw.sb - SLOT_HDR; int k = (int)((ml + sb - 1) / sb); uint32_t small[32]; uint32_t *sl = k <= 32 ? small : malloc((size_t)k * sizeof *sl); if (!sl) return -1;
    uint8_t *loc = NULL; size_t loccap = 0, loclen = 0;
    int rc = tx_take(t, k, sl);                                                                   // (the meta of the run is kept in slots too: a row with a blob of tens of kilobytes would take pages from the free list)
    for (int j = 0; !rc && j < k; j++) { size_t off = (size_t)j * sb, n = ml - off < sb ? ml - off : sb; rc = slotw_write(&t->sw, c, sl[j], id, 0xFFFFFFFFu, (uint32_t)j, meta + off, n); }
    if (!rc) rc = loc_encode(sl, k, &loc, &loccap, &loclen);
    if (!rc) {
        sqlite3_stmt *st = t->ins_run;
        sqlite3_bind_int64(st, 1, id); sqlite3_bind_int64(st, 2, age); sqlite3_bind_int64(st, 3, lvl); sqlite3_bind_int64(st, 4, (int64_t)nr); sqlite3_bind_int64(st, 5, nb); sqlite3_bind_int64(st, 6, dvm);
        sqlite3_bind_int64(st, 7, (int64_t)ml); sqlite3_bind_blob(st, 8, loc, (int)loclen, SQLITE_STATIC);
        int r2 = sqlite3_step(st); sqlite3_reset(st);
        if (r2 != SQLITE_DONE) rc = r2 ? r2 : -1;
    }
    if (!rc) {
        rs_run *r = rs_run_decode(id, age, lvl, nr, nb, dvm, meta, ml);
        if (!r) rc = -1;
        else { r->mloc = malloc(loclen ? loclen : 1); if (r->mloc) { memcpy(r->mloc, loc, loclen); r->mlocl = (uint32_t)loclen; } if (tx_push_run(t, r)) { rs_run_unref(r); rc = -1; } else atomic_fetch_add(&t->s->runs_written, 1); }
    }
    free(loc); if (sl != small) free(sl);
    return rc;
}
// The slots a flush of the items [i0, i1) can need (the rows as they are, uncompressed, and the end of every block), taken before the transaction starts
int rsx_reserve_items (mw_rstore *s, sqlite3 *c, const fitem *v, int i0, int i1) {
    uint32_t sb = atomic_load(&s->slot_bytes);
    if (!sb) { sb = compute_slot_bytes(c); atomic_store(&s->slot_bytes, sb); }
    size_t bytes = 0; for (int i = i0; i < i1; i++) bytes += v[i].pklen + v[i].bloblen + 16;
    size_t rows = (size_t)(i1 - i0), meta = rows * 2 + (bytes / RS_BLOCK_TARGET + 1) * 64 + 4096;       // (the meta of the run: a Bloom filter of 10 bits a row and the fence keys)
    size_t need = (bytes + meta) / (sb - SLOT_HDR) + bytes / RS_BLOCK_TARGET + 24;
    return pool_ensure(s, c, need);
}
int rsx_tx_add_items (rsx_tx *t, sqlite3 *c, const fitem *v, int i0, int i1) {
    if (i1 <= i0) return 0;
    int rc = tx_prepare(t, c); if (rc) return rc;
    int64_t run = t->next_run++; wctx w = { t, c, NULL, 0, run };
    rs_builder *b = rs_builder_new((uint64_t)(i1 - i0), emit_db, &w); if (!b) return SQLITE_NOMEM;
    for (int i = i0; i < i1; i++) {
        if (i + 1 < i1 && v[i].tbl == v[i + 1].tbl && v[i].pklen == v[i + 1].pklen && !memcmp(v[i].pk, v[i + 1].pk, v[i].pklen)) continue;           // (the same key twice: the later one stands)
        rs_key k = { v[i].tbl, v[i].pk, v[i].pklen };
        rc = rs_builder_add(b, &k, v[i].n ? v[i].dv : 0, v[i].n ? v[i].blob : NULL, v[i].n ? v[i].bloblen : 0);
        if (rc) { rs_builder_free(b); free(w.loc); return rc; }
    }
    uint8_t *meta; size_t ml; uint64_t nr; uint32_t nb; int64_t dvm;
    rc = rs_builder_finish(b, &meta, &ml, &nr, &nb, &dvm); free(w.loc); if (rc) return rc;
    rc = tx_insert_run(t, c, run, t->next_age++, 0, meta, ml, nr, nb, dvm); free(meta);
    return rc;
}
// ---- runs built before the transaction (the CPU of a flush, outside the write lock and in several threads) ----
struct rsx_prebuilt { rs_builder *b; };
rsx_prebuilt *rsx_prebuild (const fitem *v, int i0, int i1) {
    if (i1 <= i0) return NULL;
    rsx_prebuilt *pb = calloc(1, sizeof *pb); if (!pb) return NULL;
    pb->b = rs_builder_new_deferred((uint64_t)(i1 - i0)); if (!pb->b) { free(pb); return NULL; }
    for (int i = i0; i < i1; i++) {
        if (i + 1 < i1 && v[i].tbl == v[i + 1].tbl && v[i].pklen == v[i + 1].pklen && !memcmp(v[i].pk, v[i + 1].pk, v[i].pklen)) continue;           // (the same key twice: the later one stands)
        rs_key k = { v[i].tbl, v[i].pk, v[i].pklen };
        if (rs_builder_add(pb->b, &k, v[i].n ? v[i].dv : 0, v[i].n ? v[i].blob : NULL, v[i].n ? v[i].bloblen : 0)) { rs_builder_free(pb->b); free(pb); return NULL; }
    }
    if (rs_builder_seal(pb->b)) { rs_builder_free(pb->b); free(pb); return NULL; }
    return pb;
}
void rsx_prebuilt_free (rsx_prebuilt *pb) { if (!pb) return; rs_builder_free(pb->b); free(pb); }
// the slots the run needs, to be taken before the transaction starts
int rsx_reserve_prebuilt (mw_rstore *s, sqlite3 *c, const rsx_prebuilt *pb) {
    uint32_t sb = atomic_load(&s->slot_bytes);
    if (!sb) { sb = compute_slot_bytes(c); atomic_store(&s->slot_bytes, sb); }
    size_t need = 16, data = sb - SLOT_HDR;
    for (uint32_t i = 0; i < rs_builder_nblocks(pb->b); i++) { size_t len; rs_builder_block(pb->b, i, &len); need += (len + data - 1) / data; }
    need += rs_builder_meta_bound(pb->b) / data + 2;
    return pool_ensure(s, c, need);
}
int rsx_tx_add_prebuilt (rsx_tx *t, sqlite3 *c, rsx_prebuilt *pb) {
    int rc = tx_prepare(t, c); if (rc) return rc;
    uint32_t sb = t->sw.sb - SLOT_HDR, nb = rs_builder_nblocks(pb->b);
    int64_t run = t->next_run++;
    uint8_t **locs = calloc(nb ? nb : 1, sizeof *locs); uint32_t *llens = calloc(nb ? nb : 1, sizeof *llens); if (!locs || !llens) { free(locs); free(llens); return SQLITE_NOMEM; }
    for (uint32_t i = 0; i < nb && !rc; i++) {
        size_t len; const uint8_t *data = rs_builder_block(pb->b, i, &len);
        int k = (int)((len + sb - 1) / sb); uint32_t small[16]; uint32_t *sl = k <= 16 ? small : malloc((size_t)k * sizeof *sl); if (!sl) { rc = SQLITE_NOMEM; break; }
        rc = tx_take(t, k, sl);
        for (int j = 0; !rc && j < k; j++) { size_t off = (size_t)j * sb, n = len - off < sb ? len - off : sb; rc = slotw_write(&t->sw, c, sl[j], run, i, (uint32_t)j, data + off, n); }
        if (!rc) { uint8_t *lb = NULL; size_t lcap = 0, ll = 0; rc = loc_encode(sl, k, &lb, &lcap, &ll); locs[i] = lb; llens[i] = (uint32_t)ll; }
        if (sl != small) free(sl);
    }
    uint8_t *meta = NULL; size_t ml = 0; uint64_t nr = 0; uint32_t nbk = 0; int64_t dvm = 0;
    if (!rc) rc = rs_builder_finish_locs(pb->b, (const uint8_t *const *)locs, llens, &meta, &ml, &nr, &nbk, &dvm);
    for (uint32_t i = 0; i < nb; i++) free(locs[i]);
    free(locs); free(llens);
    if (!rc) rc = tx_insert_run(t, c, run, t->next_age++, 0, meta, ml, nr, nbk, dvm);
    free(meta);
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
// the run stops existing here, and its slots are free: the next run takes them
static int tx_remove_run (rsx_tx *t, sqlite3 *c, const rs_run *r) {
    sqlite3_stmt *st = NULL; int rc = sqlite3_prepare_v2(c, "DELETE FROM mw_runs WHERE run = ?1", -1, &st, NULL);
    if (rc != SQLITE_OK) return rc;
    sqlite3_bind_int64(st, 1, r->id); rc = sqlite3_step(st); sqlite3_finalize(st);
    if (rc != SQLITE_DONE) return rc ? rc : -1;
    size_t cap = 256, n = 0; uint32_t *sl = malloc(cap * sizeof *sl); if (!sl) return SQLITE_NOMEM;
    for (uint32_t b = 0; b <= r->nblk; b++) {                                                      // (the slots of its blocks, then of its meta)
        const uint8_t *loc; uint32_t stored, loclen;
        if (b < r->nblk) rs_run_block_info(r, b, &stored, &loc, &loclen); else { loc = r->mloc; loclen = r->mlocl; }
        const uint8_t *p = loc, *end = loc + loclen; uint64_t slot;
        while (loc && sl_get(&p, end, &slot)) { if (n == cap) { cap *= 2; uint32_t *ns = realloc(sl, cap * sizeof *sl); if (!ns) { free(sl); return SQLITE_NOMEM; } sl = ns; } sl[n++] = (uint32_t)slot; }
    }
    rc = free_slots(c, sl, n); free(sl);
    if (rc) return rc;
    if (t->nrem == t->caprem) { int nc = t->caprem ? t->caprem * 2 : 8; int64_t *nr = realloc(t->rem, (size_t)nc * sizeof *nr); if (!nr) return SQLITE_NOMEM; t->rem = nr; t->caprem = nc; }
    t->rem[t->nrem++] = r->id;
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
    sqlite3_finalize(t->ins_run); if (t->sw_open) slotw_close(&t->sw);
    held_drop(t->s, t);
    if (!committed && t->ntaken) pool_put(t->s, t->taken, t->ntaken);                      // (the slots were not used: they stay ours)
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
                if (!t->s->cur || t->s->cur->ver < nm->ver) { mw_rman *o = t->s->cur; t->s->cur = nm; backlog_update(t->s, nm); pthread_mutex_unlock(&t->s->mu); rsx_man_release(o); nm = NULL; }
                else pthread_mutex_unlock(&t->s->mu);
            }
            if (nm) rsx_man_release(nm);
        }
    }
    for (int i = 0; i < t->nadd; i++) rs_run_unref(t->add[i]);
    free(t->add); free(t->rem); free(t->drops); free(t->taken); rsx_man_release(t->base); free(t);
}

// ---- merging ----
typedef struct { uint32_t blk; uint8_t *d; size_t len; uint32_t *sl; int nsl; } oblk;
static int id_fn (void *ctx, sqlite3 *c);
typedef struct {
    mw_rstore *s; sqlite3 *wr; int out_lvl; int64_t out_age; const mw_rman *man;
    oblk *blks; int nblks, capblks; uint8_t *loc; size_t loccap; int64_t part_id;      // part_id: the number of the run being written (taken before its first block)
    int64_t id_next, id_end;                                                           // run numbers taken for the parts of this merge (recorded in the file when they were taken)
    struct pwriter *pw;                                                                // the thread that writes the finished parts while the next one is being merged (NULL: the merge writes them itself)
} mctx;
static void mctx_clear (mctx *m) { for (int i = 0; i < m->nblks; i++) { free(m->blks[i].d); free(m->blks[i].sl); } m->nblks = 0; held_drop(m->s, m); }
// a block of the output: its slots are chosen now (the locator goes into the meta), the data is written later, a few blocks to a transaction
static int m_emit (void *ctx, uint32_t blk, const uint8_t *d, size_t len, const uint8_t **loc, size_t *loclen) {
    mctx *m = ctx; uint32_t sb = atomic_load(&m->s->slot_bytes) - SLOT_HDR; int k = (int)((len + sb - 1) / sb);
    if (m->nblks == m->capblks) { int nc = m->capblks ? m->capblks * 2 : 64; oblk *nb = realloc(m->blks, (size_t)nc * sizeof *nb); if (!nb) return -1; m->blks = nb; m->capblks = nc; }
    if (pool_ensure(m->s, m->wr, (size_t)k + 16) != 0) return -1;
    uint32_t *sl = malloc((size_t)k * sizeof *sl); uint8_t *cp = malloc(len); if (!sl || !cp) { free(sl); free(cp); return -1; }
    if (pool_take(m->s, k, sl) != 0) { free(sl); free(cp); return -1; }
    memcpy(cp, d, len);
    if (held_add(m->s, m, sl, k)) { pool_put(m->s, sl, k); free(sl); free(cp); return -1; }
    m->blks[m->nblks++] = (oblk){ blk, cp, len, sl, k };
    size_t ll; if (loc_encode(sl, k, &m->loc, &m->loccap, &ll) != 0) return -1;
    *loc = m->loc; *loclen = ll;
    return 0;
}
typedef struct { mw_rstore *s; int64_t id; int64_t count; } idctx;
static int m_begin (void *sctx, uint64_t hint, rs_emit_fn *emit, void **ec) {
    (void)hint; mctx *m = sctx; mctx_clear(m);
    mw_rstore *s = m->s; int rc;
    if (m->id_next < m->id_end) m->part_id = m->id_next++;
    else { idctx ic = { s, 0, 16 }; rc = small_txn(s, m->wr, id_fn, &ic); if (rc) return rc; m->part_id = ic.id; m->id_next = ic.id + 1; m->id_end = ic.id + ic.count; }
    *emit = m_emit; *ec = sctx; return 0;
}
// the number of a new run is taken (and recorded) before anything is written under it: whoever flushes meanwhile gets another one
static int id_fn (void *ctx, sqlite3 *c) {
    idctx *x = ctx; mw_rman *man = NULL; int rc = rsx_man(x->s, c, &man); if (rc) return -1;
    int64_t maxid = 0; for (int i = 0; i < man->n; i++) if (man->runs[i]->id > maxid) maxid = man->runs[i]->id;
    rsx_man_release(man);
    int64_t next; if (state_get(c, "next_run", 1, &next)) return -1;
    if (next <= maxid) next = maxid + 1;
    x->id = next;
    return state_put(c, "next_run", next + (x->count ? x->count : 1));
}
// the slots of blocks [i0, i1) of the part
typedef struct { const oblk *b; int i0, i1; uint32_t sb; int64_t run; } bctx;
static int blocks_fn (void *ctx, sqlite3 *c) {
    bctx *x = ctx; slotw w; int rc = slotw_open(&w, c, x->sb);
    uint32_t pl = x->sb - SLOT_HDR;
    for (int i = x->i0; !rc && i < x->i1; i++)
        for (int j = 0; !rc && j < x->b[i].nsl; j++) { size_t off = (size_t)j * pl, n = x->b[i].len - off < pl ? x->b[i].len - off : pl; rc = slotw_write(&w, c, x->b[i].sl[j], x->run, x->b[i].blk, (uint32_t)j, x->b[i].d + off, n); }
    slotw_close(&w); return rc;
}
static void part_slots_back (mctx *m) { for (int i = 0; i < m->nblks; i++) pool_put(m->s, m->blks[i].sl, m->blks[i].nsl); }

static int part_write (mctx *m, sqlite3 *c, const uint8_t *meta, size_t ml, uint64_t nr, uint32_t nb, int64_t dvm) {
    int rc;
    idctx ic = { m->s, m->part_id, 0 }; rc = 0;
    uint32_t sb = atomic_load(&m->s->slot_bytes);
    for (int i = 0; i < m->nblks && !rc; i += 24) {                                              // the slots, a few blocks to a transaction: nobody can see them until the run has a row
        bctx b = { m->blks, i, i + 24 < m->nblks ? i + 24 : m->nblks, sb, ic.id };
        rc = small_txn(m->s, c, blocks_fn, &b);
    }
    if (!rc) rc = pool_ensure(m->s, c, ml / (sb - SLOT_HDR) + 16);
    for (int attempt = 0; !rc && attempt < 5000 && (attempt == 0 || busyish(rc)); attempt++) {     // the row of the run and the version of the list: one small transaction
        if (attempt) atomic_fetch_add(&m->s->merge_retries, 1);
        backoff(attempt);
        rsx_wlock(m->s);
        rc = sqlite3_exec(c, "BEGIN", NULL, NULL, NULL); if (rc) { rsx_wunlock(m->s); continue; }
        rsx_tx *t = rsx_tx_begin(m->s, c);
        if (!t) { sqlite3_exec(c, "ROLLBACK", NULL, NULL, NULL); rsx_wunlock(m->s); rc = SQLITE_NOMEM; break; }
        rc = tx_prepare(t, c);
        if (!rc) rc = tx_insert_run(t, c, ic.id, m->out_age, m->out_lvl, meta, ml, nr, nb, dvm);
        if (!rc) rc = rsx_tx_finish(t, c);
        if (!rc) rc = sqlite3_exec(c, "COMMIT", NULL, NULL, NULL);
        if (rc) sqlite3_exec(c, "ROLLBACK", NULL, NULL, NULL);
        rsx_tx_end(t, rc == SQLITE_OK);
        rsx_wunlock(m->s);
        if (rc == RSX_NEED_SLOTS) { rc = pool_ensure(m->s, c, ml / (sb - SLOT_HDR) + 64) ? -1 : SQLITE_BUSY; }
    }
    if (rc) part_slots_back(m);                                                                   // (no run came of it: its slots stay ours)
    mctx_clear(m);
    return rc;
}

// ---- the thread that writes the parts ----
// A merge alternates between merging a part (reading blocks, merging rows, compressing: CPU) and writing it (small transactions of the engine: waiting for its commits). The finished part is
// handed to a thread of its own, with a connection of its own, and the merge goes on with the next part; one part is written while the next is merged (the parts go in order, and the
// inputs are removed only when all of them are in).
typedef struct { mctx *job; uint8_t *meta; size_t ml; uint64_t nr; uint32_t nb; int64_t dvm; } pjob;
struct pwriter { sqlite3 *c; pthread_t th; pthread_mutex_t mu; pthread_cond_t cv; pjob j; bool has, busy, stop; int rc; };
static void held_rekey (mw_rstore *s, const void *from, const void *to) { pthread_mutex_lock(&s->pmu); for (size_t i = 0; i < s->nh; i++) if (s->howner[i] == (uintptr_t)from) s->howner[i] = (uintptr_t)to; pthread_mutex_unlock(&s->pmu); }
static void *pw_main (void *arg) {
    struct pwriter *pw = arg;
    pthread_mutex_lock(&pw->mu);
    for (;;) {
        while (!pw->has && !pw->stop) pthread_cond_wait(&pw->cv, &pw->mu);
        if (!pw->has) break;
        pjob j = pw->j; pw->has = false; pw->busy = true; pthread_mutex_unlock(&pw->mu);
        int rc = pw->rc ? pw->rc : part_write(j.job, pw->c, j.meta, j.ml, j.nr, j.nb, j.dvm);
        if (pw->rc) { part_slots_back(j.job); mctx_clear(j.job); }                          // (an earlier part failed: this one is not written, its slots stay ours)
        free(j.job->blks); free(j.job->loc); free(j.job); free(j.meta);
        pthread_mutex_lock(&pw->mu); if (rc && !pw->rc) pw->rc = rc; pw->busy = false; pthread_cond_broadcast(&pw->cv);
    }
    pthread_mutex_unlock(&pw->mu);
    return NULL;
}
static struct pwriter *pw_start (sqlite3 *c) {
    struct pwriter *pw = calloc(1, sizeof *pw); if (!pw) return NULL;
    pw->c = c; pthread_mutex_init(&pw->mu, NULL); pthread_cond_init(&pw->cv, NULL);
    if (pthread_create(&pw->th, NULL, pw_main, pw)) { pthread_mutex_destroy(&pw->mu); pthread_cond_destroy(&pw->cv); free(pw); return NULL; }
    return pw;
}
static int pw_wait (struct pwriter *pw) { pthread_mutex_lock(&pw->mu); while (pw->has || pw->busy) pthread_cond_wait(&pw->cv, &pw->mu); int rc = pw->rc; pthread_mutex_unlock(&pw->mu); return rc; }
static int pw_finish (struct pwriter *pw) {
    int rc = pw_wait(pw);
    pthread_mutex_lock(&pw->mu); pw->stop = true; pthread_cond_broadcast(&pw->cv); pthread_mutex_unlock(&pw->mu);
    pthread_join(pw->th, NULL); pthread_mutex_destroy(&pw->mu); pthread_cond_destroy(&pw->cv); free(pw);
    return rc;
}
static int m_end (void *sctx, const uint8_t *meta, size_t ml, uint64_t nr, uint32_t nb, int64_t dvm) {
    mctx *m = sctx; int rc;
    if (!m->pw) return part_write(m, m->wr, meta, ml, nr, nb, dvm);
    struct pwriter *pw = m->pw;
    if ((rc = pw_wait(pw)) != 0) { part_slots_back(m); mctx_clear(m); return rc; }              // (the part before this one could not be written)
    mctx *job = malloc(sizeof *job); uint8_t *mc = malloc(ml ? ml : 1);
    if (!job || !mc) { free(job); free(mc); part_slots_back(m); mctx_clear(m); return -1; }
    memcpy(mc, meta, ml); *job = *m; job->loc = NULL; job->loccap = 0;
    held_rekey(m->s, m, job);
    m->blks = NULL; m->nblks = 0; m->capblks = 0;
    pthread_mutex_lock(&pw->mu); pw->j = (pjob){ job, mc, ml, nr, nb, dvm }; pw->has = true; pthread_cond_broadcast(&pw->cv); pthread_mutex_unlock(&pw->mu);
    return 0;
}
// At the close: the slots this process holds go back to the free ones and its entry in the register of reservations is removed
static int release_fn (void *ctx, sqlite3 *c) {
    mw_rstore *s = ctx;
    pthread_mutex_lock(&s->pmu); size_t n = s->npool; uint32_t *cp = malloc((n ? n : 1) * sizeof *cp); if (cp) memcpy(cp, s->pool, n * sizeof *cp); pthread_mutex_unlock(&s->pmu);
    if (!cp) return SQLITE_NOMEM;
    int rc = free_slots(c, cp, n); free(cp);
    if (!rc) { sqlite3_stmt *d = NULL; rc = sqlite3_prepare_v2(c, "DELETE FROM mw_resv WHERE pid = ?1", -1, &d, NULL); if (!rc) { sqlite3_bind_int64(d, 1, (int64_t)getpid()); rc = sqlite3_step(d); rc = rc == SQLITE_DONE ? 0 : rc; } sqlite3_finalize(d); }
    return rc;
}
int rsx_release_pool (mw_rstore *s, sqlite3 *c) {
    int rc = small_txn(s, c, release_fn, s);
    if (!rc) { pthread_mutex_lock(&s->pmu); s->npool = 0; pthread_mutex_unlock(&s->pmu); }
    return rc;
}
static bool pid_alive (int64_t pid) { return pid > 0 && (kill((pid_t)pid, 0) == 0 || errno == EPERM); }
// Slots that nobody has: in the table, in no run, not free and not held by a process that is alive. The register of reservations says what every process holds (it is written in the
// transaction that takes the slots); the processes that are gone (killed with a pool, or in the middle of a merge) are the ones whose slots come back. The caller is the one that merges
// (one process at a time) and takes the turn of the writers; our own slots are in memory.
int rsx_sweep (mw_rstore *s, sqlite3 *rd, sqlite3 *wr) {
    (void)rd;
    rsx_wlock(s);
    int rc = sqlite3_exec(wr, "BEGIN", NULL, NULL, NULL); int leaked = 0;
    mw_rman *man = NULL; uint8_t *mark = NULL; int64_t maxslot = 0; int64_t dead[64]; int ndead = 0;
    if (!rc) rc = rsx_man(s, wr, &man);
    if (!rc) { sqlite3_stmt *q = NULL; if (sqlite3_prepare_v2(wr, "SELECT coalesce(max(slot), 0) FROM mw_slots", -1, &q, NULL) == SQLITE_OK && sqlite3_step(q) == SQLITE_ROW) maxslot = sqlite3_column_int64(q, 0); sqlite3_finalize(q);
        int64_t nx; rc = state_get(wr, "next_slot", 1, &nx); nx -= 1; if (!rc && nx > maxslot) maxslot = nx; }                              // (a number that was taken and never written is a slot too)
    if (!rc && maxslot > 0) {
        mark = calloc((size_t)maxslot + 2, 1); if (!mark) rc = SQLITE_NOMEM;
        for (int ri = 0; !rc && ri < man->n; ri++) for (uint32_t b = 0; b < man->runs[ri]->nblk; b++) {
            uint32_t stored, ll; const uint8_t *loc; rs_run_block_info(man->runs[ri], b, &stored, &loc, &ll);
            const uint8_t *p = loc, *end = loc + ll; uint64_t sl; while (sl_get(&p, end, &sl)) if ((int64_t)sl <= maxslot) mark[sl] = 1;
        }
        for (int ri = 0; !rc && ri < man->n; ri++) if (man->runs[ri]->mloc) { const uint8_t *p = man->runs[ri]->mloc, *end = p + man->runs[ri]->mlocl; uint64_t sl; while (sl_get(&p, end, &sl)) if ((int64_t)sl <= maxslot) mark[sl] = 1; }
        sqlite3_stmt *q = NULL;
        if (!rc && sqlite3_prepare_v2(wr, "SELECT chunk, bits FROM mw_free", -1, &q, NULL) == SQLITE_OK) {
            while (sqlite3_step(q) == SQLITE_ROW) { int64_t chunk = sqlite3_column_int64(q, 0); const uint8_t *bits = sqlite3_column_blob(q, 1); if (sqlite3_column_bytes(q, 1) != FREE_CHUNK_BYTES) continue;
                for (int i = 0; i < FREE_CHUNK_BYTES; i++) if (bits[i]) for (int b = 0; b < 8; b++) if (bits[i] & (1u << b)) { int64_t sl = chunk * FREE_CHUNK_SLOTS + i * 8 + b; if (sl > 0 && sl <= maxslot) mark[sl] = 1; } }
        }
        sqlite3_finalize(q);
        // what the processes that are alive hold (the register), and what this one holds (memory)
        q = NULL;
        if (!rc && sqlite3_prepare_v2(wr, "SELECT pid, slots FROM mw_resv", -1, &q, NULL) == SQLITE_OK) {
            while (sqlite3_step(q) == SQLITE_ROW) {
                int64_t pid = sqlite3_column_int64(q, 0);
                if (pid == (int64_t)getpid()) continue;
                if (!pid_alive(pid)) { if (ndead < 64) dead[ndead++] = pid; continue; }
                const uint8_t *p = sqlite3_column_blob(q, 1), *end = p + sqlite3_column_bytes(q, 1); uint64_t gap, len, pos = 0;
                while (p && sl_get(&p, end, &gap) && sl_get(&p, end, &len)) { pos += gap; for (uint64_t k = 0; k < len; k++) if ((int64_t)(pos + k) <= maxslot) mark[pos + k] = 1; pos += len; }
            }
        }
        sqlite3_finalize(q);
        pthread_mutex_lock(&s->pmu);
        for (size_t i = 0; i < s->npool; i++) if (s->pool[i] <= (uint64_t)maxslot) mark[s->pool[i]] = 1;
        for (size_t i = 0; i < s->nh; i++) if (s->hslot[i] <= (uint64_t)maxslot) mark[s->hslot[i]] = 1;
        pthread_mutex_unlock(&s->pmu);
        size_t lcap = 256; uint32_t *lk = malloc(lcap * sizeof *lk); size_t ln = 0; if (!lk) rc = SQLITE_NOMEM;
        for (int64_t sl = 1; !rc && sl <= maxslot; sl++) if (!mark[sl]) { if (ln == lcap) { lcap *= 2; uint32_t *nl = realloc(lk, lcap * sizeof *lk); if (!nl) { rc = SQLITE_NOMEM; break; } lk = nl; } lk[ln++] = (uint32_t)sl; }
        if (!rc) { rc = free_slots(wr, lk, ln); leaked = (int)ln; }
        free(lk);
    }
    for (int i = 0; !rc && i < ndead; i++) { sqlite3_stmt *d = NULL; if (sqlite3_prepare_v2(wr, "DELETE FROM mw_resv WHERE pid = ?1", -1, &d, NULL) == SQLITE_OK) { sqlite3_bind_int64(d, 1, dead[i]); sqlite3_step(d); } sqlite3_finalize(d); }
    free(mark); rsx_man_release(man);
    if (!rc) rc = sqlite3_exec(wr, "COMMIT", NULL, NULL, NULL);
    if (rc) sqlite3_exec(wr, "ROLLBACK", NULL, NULL, NULL);
    rsx_wunlock(s);
    if (!rc) atomic_fetch_add(&s->swept_slots, (uint64_t)leaked);
    return rc ? -1 : leaked;
}
int rsx_merge (mw_rstore *s, sqlite3 *rd, sqlite3 *wr, sqlite3 *wr2, int fanout, uint64_t part_rows) {
    mw_rman *man = NULL;
    sqlite3_exec(rd, "BEGIN", NULL, NULL, NULL);
    int rc = rsx_man(s, rd, &man);
    sqlite3_exec(rd, "COMMIT", NULL, NULL, NULL);
    if (rc) return -1;
    // the lowest level with enough age groups
    int lvl = -1;
    for (int L = 0; L < 24 && lvl < 0; L++) { int groups = 0; int64_t last = -1; for (int i = 0; i < man->n; i++) if (man->runs[i]->lvl == L && man->runs[i]->age != last) { groups++; last = man->runs[i]->age; } if (groups >= fanout) lvl = L; }
    if (lvl < 0) { rsx_man_release(man); return 0; }
    // the `fanout` oldest age groups of that level (the manifest is newest first, so they are at its end): a merge has a bounded size however many runs have piled up
    rs_run **in = malloc((size_t)man->n * sizeof *in); int nin = 0; int64_t minage = INT64_MAX;
    if (!in) { rsx_man_release(man); return -1; }
    { int groups = 0; int64_t last = -1;
      for (int i = man->n - 1; i >= 0 && groups <= fanout; i--) {
          if (man->runs[i]->lvl != lvl) continue;
          if (man->runs[i]->age != last) { groups++; last = man->runs[i]->age; if (groups > fanout) break; }
          in[nin++] = man->runs[i]; if (man->runs[i]->age < minage) minage = man->runs[i]->age;
      }
      // (collected oldest first: the merge wants the newest first)
      for (int a = 0, b = nin - 1; a < b; a++, b--) { rs_run *t = in[a]; in[a] = in[b]; in[b] = t; } }
    // The output is the oldest data (it may drop what shows that a row is gone) only when no run outside the merge could hold an older version: none with an age up to the output's. (After a merge that
    // was interrupted a run of the same age as an input, or in between, can exist: comparing with the oldest input would drop a mark that still hides something.)
    bool bottom = true;
    for (int i = 0; i < man->n; i++) { bool is_in = false; for (int j = 0; j < nin; j++) if (in[j] == man->runs[i]) is_in = true; if (!is_in && man->runs[i]->age <= in[0]->age) bottom = false; }
    (void)minage;
    plainctx pc = { s, NULL, rd, NULL, true }; if (sqlite3_prepare_v2(rd, rsx_blk_sql(), -1, &pc.st, NULL) != SQLITE_OK) { free(in); rsx_man_release(man); return -1; }
    struct pwriter *pw = wr2 ? pw_start(wr2) : NULL;                                               // (the finished parts are written by a thread of its own while the next one is merged)
    mctx mc = { .s = s, .wr = wr, .out_lvl = lvl + 1, .out_age = in[0]->age, .man = man, .pw = pw };
    rs_merge_opts o = { plain_read, &pc, bottom, keep_alive, man, part_rows, m_begin, m_end, &mc };
    uint64_t rows = 0;
    rc = rs_merge(in, nin, &o, &rows);
    if (pw) { int prc = pw_finish(pw); if (!rc) rc = prc; }
    mctx_clear(&mc); free(mc.blks); free(mc.loc); sqlite3_finalize(pc.st); sqlite3_finalize(pc.exists);
    // The inputs go last, one run to a transaction (its row goes and its slots are free), the OLDEST first: if the merge is interrupted what is left of the inputs is always the newest of them. The output
    // has the age of the newest input, so it still hides what is left (which it holds); a later merge of what is left with newer runs makes an output that is not older than anything it is missing.
    // (Newest first, the oldest would be left: merged with newer runs they would give an output that hides the newer versions that the first output holds.)
    for (int i = nin - 1; !rc && i >= 0; i--) {
        rc = SQLITE_BUSY;
        for (int attempt = 0; attempt < 5000 && busyish(rc); attempt++) {
            if (attempt) atomic_fetch_add(&s->merge_retries, 1);
            backoff(attempt);
            rsx_wlock(s);
            rc = sqlite3_exec(wr, "BEGIN", NULL, NULL, NULL); if (rc) { rsx_wunlock(s); continue; }
            rsx_tx *t = rsx_tx_begin(s, wr);
            if (!t) { sqlite3_exec(wr, "ROLLBACK", NULL, NULL, NULL); rsx_wunlock(s); rc = SQLITE_NOMEM; break; }
            rc = tx_remove_run(t, wr, in[i]);
            if (!rc) rc = rsx_tx_finish(t, wr);
            if (!rc) rc = sqlite3_exec(wr, "COMMIT", NULL, NULL, NULL);
            if (rc) sqlite3_exec(wr, "ROLLBACK", NULL, NULL, NULL);
            rsx_tx_end(t, rc == SQLITE_OK);
            rsx_wunlock(s);
        }
    }
    free(in); rsx_man_release(man);
    if (rc) return -1;
    atomic_fetch_add(&s->merges, 1); atomic_fetch_add(&s->merged_rows, rows);
    return 1;
}



