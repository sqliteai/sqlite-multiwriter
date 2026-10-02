//
//  multiwriter_mmeta.c
//
//  The CRDT metadata in the shared mode of multi-process Multi-Writer (docs/design.md, "Multi-process"). The state of a row lives in no process: its newest version is in the log
//  (the extension of the commit that made it) and is found through a shared index of its own (a second multiwriter_shidx, keyed by the *bucket* of the row instead of a page
//  number). A bucket holds the rows whose key hashes to it (almost always one); a commit that changes a row writes the whole new state of its bucket, so the newest version of a
//  bucket is complete and older versions are only needed by nobody. Readers take the head version of the bucket (one index lookup and a copy from a mapped segment); the writer
//  is the publisher, under the publication lock, which validates that the buckets it writes were not changed since the transaction read them. What is older than the last flush
//  is in the file tables (mw_cells) and the index forgets it. The site ids (ord -> id) are in the shared header.
//
#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include "multiwriter_meta_priv.h"
#include "multiwriter_internal.h"
#include "multiwriter_seglog.h"

// ---- sites ----
static pthread_mutex_t g_site_mu = PTHREAD_MUTEX_INITIALIZER;            // (in-process: threads of one process do not exclude each other with fcntl locks)
static uint32_t sh_nsites (mw_meta *m) { return atomic_load_explicit(&m->db->shm->nsites, memory_order_acquire); }

bool mm_site_id (mw_meta *m, uint32_t ord, uint8_t out[16]) {
    mw_shm *sh = m->db->shm;
    if (ord >= sh_nsites(m)) return false;
    memcpy(out, sh->sites[ord], 16);
    return true;
}

uint32_t mm_site_ord (mw_meta *m, const uint8_t id[16]) {
    mw_shm *sh = m->db->shm;
    uint32_t n = sh_nsites(m);
    for (uint32_t i = 0; i < n; i++) if (!memcmp(sh->sites[i], id, 16)) return i;
    pthread_mutex_lock(&g_site_mu);
    mw_mp_meta_lock(m->db, 0, true);
    n = sh_nsites(m);
    uint32_t r = 0; bool found = false;
    for (uint32_t i = 0; i < n && !found; i++) if (!memcmp(sh->sites[i], id, 16)) { r = i; found = true; }
    if (!found && n < MW_MAX_SITES) { memcpy(sh->sites[n], id, 16); atomic_store_explicit(&sh->nsites, n + 1, memory_order_release); r = n; }
    mw_mp_meta_unlock(m->db, 0);
    pthread_mutex_unlock(&g_site_mu);
    return r;
}

void mm_site_install_db (mw_db *db, uint32_t ord, const uint8_t id[16]) {
    mw_shm *sh = db->shm;
    if (ord >= MW_MAX_SITES) return;
    pthread_mutex_lock(&g_site_mu);
    mw_mp_meta_lock(db, 0, true);
    uint32_t n = atomic_load_explicit(&sh->nsites, memory_order_acquire);
    while (n <= ord) { memset(sh->sites[n], 0, 16); n++; atomic_store_explicit(&sh->nsites, n, memory_order_release); }
    static const uint8_t zero[16] = {0};
    if (!memcmp(sh->sites[ord], zero, 16)) memcpy(sh->sites[ord], id, 16);
    mw_mp_meta_unlock(db, 0);
    pthread_mutex_unlock(&g_site_mu);
}
void mm_site_install (mw_meta *m, uint32_t ord, const uint8_t id[16]) { mm_site_install_db(m->db, ord, id); }

// ---- groups ----
void mm_group_free (mm_group *g) {
    for (int i = 0; i < g->n; i++) free(g->rows[i].c);
    free(g->rows); free(g->raw); memset(g, 0, sizeof *g);
}

static int rv (const uint8_t **p, const uint8_t *end, uint64_t *v) {
    uint64_t r = 0; int sh = 0;
    while (*p < end && sh < 64) { uint8_t b = *(*p)++; r |= (uint64_t)(b & 0x7f) << sh; if (!(b & 0x80)) { *v = r; return 0; } sh += 7; }
    return -1;
}

// the group that starts at `loc` of the log (a version installed at `epoch`): its rows. false: the segment is gone (the state it held is older than the base, so it is in the file)
static bool read_group (mw_meta *m, uint64_t loc, uint64_t epoch, uint32_t bucket_hint, mm_group *g) {
    mw_db *db = m->db; (void)bucket_hint;
    uint8_t hb[5]; if (!mw_seglog_read(db, loc, 0, 5, hb)) return false;
    const uint8_t *p = hb; uint64_t glen; if (rv(&p, hb + 5, &glen) || glen > (64u << 20)) return false;
    size_t hl = (size_t)(p - hb), total = hl + (size_t)glen;
    uint8_t *raw = malloc(total ? total : 1); if (!raw) return false;
    if (!mw_seglog_read(db, loc, 0, (uint32_t)total, raw)) { free(raw); return false; }
    const uint8_t *q = raw + hl, *end = raw + total; uint64_t bucket, nrows;
    if (rv(&q, end, &bucket) || rv(&q, end, &nrows) || nrows > (1u << 24)) { free(raw); return false; }
    mm_row *rows = calloc((size_t)(nrows ? nrows : 1), sizeof *rows); if (!rows) { free(raw); return false; }
    for (uint64_t i = 0; i < nrows; i++) {
        uint64_t tbl, pklen, nc;
        if (rv(&q, end, &tbl) || rv(&q, end, &pklen) || q + pklen > end) goto bad;
        rows[i].tbl = (uint32_t)tbl; rows[i].pk = q; rows[i].pklen = (uint32_t)pklen; q += pklen;
        if (rv(&q, end, &nc) || nc > (1u << 20)) goto bad;
        rows[i].c = malloc((size_t)(nc ? nc : 1) * sizeof(mw_mcell)); if (!rows[i].c) goto bad;
        rows[i].n = (int)nc;
        for (uint64_t k = 0; k < nc; k++) {
            uint64_t col, cv, dvp, site, seq;
            if (rv(&q, end, &col) || rv(&q, end, &cv) || rv(&q, end, &dvp) || rv(&q, end, &site) || rv(&q, end, &seq)) goto bad;
            rows[i].c[k] = (mw_mcell){ (int64_t)cv, dvp ? (int64_t)(dvp - 1) : (int64_t)epoch, (uint32_t)col, (uint32_t)site, (uint32_t)seq };
        }
        continue;
    bad:
        for (uint64_t j = 0; j <= i; j++) free(rows[j].c);
        free(rows); free(raw); return false;
    }
    g->raw = raw; g->rawlen = total; g->rows = rows; g->n = (int)nrows; g->bucket = (uint32_t)bucket; g->epoch = epoch;
    return true;
}

int mm_head (mw_meta *m, uint32_t bucket, mm_group *g) {
    memset(g, 0, sizeof *g);
    uint64_t ep, loc;
    if (!shidx_lookup(m->db->rx, bucket, UINT64_MAX, &ep, &loc)) return 0;
    if (!read_group(m, loc, ep, bucket, g)) { memset(g, 0, sizeof *g); return 0; }
    return 0;
}

// ---- publication (the publication lock is held) ----
// The buckets this commit writes must be as the transaction read them: a change of the bucket since means another commit changed a row the transaction took its versions from.
int mm_validate (mw_db *db, mw_lane *lane) {
    if (!lane || !lane->cdc_ng) return SQLITE_OK;
    for (int i = 0; i < lane->cdc_ng; i++) if (shidx_head_epoch(db->rx, lane->cdc_gbucket[i]) != lane->cdc_gseen[i]) return MW_CONFLICT;
    return SQLITE_OK;
}

static void shared_purge_add (mw_shm *sh, uint32_t tbl, uint64_t epoch) {
    uint32_t n = atomic_load(&sh->npurge);
    for (uint32_t i = 0; i < n; i++) if (atomic_load(&sh->purge[i].tbl) == tbl) { if (atomic_load(&sh->purge[i].epoch) < epoch) atomic_store(&sh->purge[i].epoch, epoch); return; }
    if (n < MW_MAX_PURGE) { atomic_store(&sh->purge[n].epoch, epoch); atomic_store(&sh->purge[n].tbl, tbl); atomic_store_explicit(&sh->npurge, n + 1, memory_order_release); }
}
static void purge_cb (void *arg, uint32_t tbl, uint64_t epoch) { shared_purge_add(arg, tbl, epoch); }

int mm_install (mw_db *db, mw_lane *lane, uint64_t epoch, uint64_t ext_loc) {
    mw_shm *sh = db->shm;
    if (!lane || !lane->cdc_ext_len) return SQLITE_OK;
    int ng = lane->cdc_ng;
    if (ng) {
        uint64_t locs_small[16]; uint64_t *locs = ng <= 16 ? locs_small : malloc((size_t)ng * sizeof *locs);
        if (!locs) return SQLITE_NOMEM;
        for (int i = 0; i < ng; i++) locs[i] = MW_LOC(MW_LOC_SEG(ext_loc), MW_LOC_OFF(ext_loc) + lane->cdc_goff[i]);
        int rc = shidx_install(db->rx, epoch, 0, ng, lane->cdc_gbucket, locs);
        if (ng > 16) free(locs);
        if (rc != 0) return SQLITE_FULL;
        if (atomic_fetch_add(&sh->meta_dirty, (uint64_t)ng) + (uint64_t)ng >= 2048) mw_cdc_kick_flush(db);
    }
    mw_ext_purges(lane->cdc_ext, lane->cdc_ext_len, purge_cb, epoch, sh);
    return SQLITE_OK;
}

// recovery: the groups of a replayed record
typedef struct { mw_db *db; uint64_t epoch, ext_loc; uint32_t *b; uint64_t *l; int n, cap; } rep_ctx;
static int rep_group (void *arg, uint32_t bucket, uint32_t off) {
    rep_ctx *r = arg;
    if (r->n == r->cap) { r->cap = r->cap ? r->cap * 2 : 8; r->b = realloc(r->b, (size_t)r->cap * 4); r->l = realloc(r->l, (size_t)r->cap * 8); if (!r->b || !r->l) return -1; }
    r->b[r->n] = bucket; r->l[r->n] = MW_LOC(MW_LOC_SEG(r->ext_loc), MW_LOC_OFF(r->ext_loc) + off); r->n++;
    return 0;
}
static void rep_site (void *arg, uint32_t ord, const uint8_t id[16]) { mm_site_install_db(arg, ord, id); }
int mm_replay (mw_db *db, uint64_t epoch, const uint8_t *ext, uint32_t len, uint64_t ext_loc) {
    mw_ext_walk(ext, len, epoch, NULL, NULL, rep_site, db);
    rep_ctx r = { db, epoch, ext_loc, NULL, NULL, 0, 0 };
    int rc = SQLITE_OK;
    if (mw_ext_groups(ext, len, rep_group, &r) != 0) rc = SQLITE_CORRUPT;
    else {
        if (r.n && shidx_room(db->rx) < (uint32_t)r.n + 2) shidx_gc_floor(db->rx, epoch - 1, atomic_load(&db->shm->meta_flushed));
        if (r.n && shidx_install(db->rx, epoch, 0, r.n, r.b, r.l) != 0) rc = SQLITE_FULL;
        mw_ext_purges(ext, len, purge_cb, epoch, db->shm);
        if (r.n) atomic_fetch_add(&db->shm->meta_dirty, (uint64_t)r.n);
    }
    free(r.b); free(r.l);
    return rc;
}

// ---- the file: first use, flush ----
int mm_ready (mw_meta *m) {
    mw_shm *sh = m->db->shm;
    if (atomic_load_explicit(&sh->meta_state, memory_order_acquire) == 2) return 0;
    pthread_mutex_lock(&m->file_mu);
    mw_mp_meta_lock(m->db, 1, true);                                         // (the flusher's lock: nobody flushes before the file's state is known)
    if (atomic_load(&sh->meta_state) != 2) {
        uint64_t F = 0; uint32_t flushed_sites = 0; uint8_t own[16]; bool have_own = false;
        mw_metafile_load_state(m, &F, &flushed_sites, &have_own, own);
        if (!have_own) arc4random_buf(own, 16);
        mm_site_install_db(m->db, 0, own);
        if (!have_own) { /* a fresh id: it is written with the first flush */ }
        atomic_store(&sh->sites_flushed, flushed_sites);
        atomic_store(&sh->meta_flushed, F);
        atomic_store_explicit(&sh->meta_state, 2, memory_order_release);
    }
    mw_mp_meta_unlock(m->db, 1);
    pthread_mutex_unlock(&m->file_mu);
    return 0;
}

typedef struct { mw_meta *m; uint64_t F; fitem *v; int n, cap; int err; } col_ctx;
static void col_cb (void *arg, uint32_t bucket, uint64_t epoch, uint64_t loc) {
    col_ctx *c = arg; mm_group g; (void)bucket;
    if (c->err) return;
    if (!read_group(c->m, loc, epoch, bucket, &g)) { c->err = 1; return; }                     // (a segment that is gone held a state older than the base: it is in the file)
    for (int i = 0; i < g.n; i++) {
        const mm_row *r = &g.rows[i]; int nc = 0; bool has_sentinel = false;
        for (int k = 0; k < r->n; k++) { if (r->c[k].col == CRDT_COL_SENTINEL) has_sentinel = true; if (r->c[k].dv > (int64_t)c->F) nc++; }
        if (!nc) continue;
        bool drop = has_sentinel;                                                              // a row that was ever deleted: its cells in the file may be stale ones: rewrite them all
        if (drop) { nc = r->n; }
        if (c->n == c->cap) { c->cap = c->cap ? c->cap * 2 : 256; fitem *nv = realloc(c->v, (size_t)c->cap * sizeof *nv); if (!nv) { c->err = 1; break; } c->v = nv; }
        fitem *it = &c->v[c->n]; it->tbl = r->tbl; it->pklen = r->pklen; it->drop = drop; it->n = nc;
        it->pk = malloc(r->pklen ? r->pklen : 1); it->c = malloc((size_t)(nc ? nc : 1) * sizeof(mw_mcell));
        if (!it->pk || !it->c) { free(it->pk); free(it->c); c->err = 1; break; }
        memcpy(it->pk, r->pk, r->pklen);
        int k2 = 0; for (int k = 0; k < r->n; k++) if (drop || r->c[k].dv > (int64_t)c->F) it->c[k2++] = r->c[k];
        c->n++;
    }
    mm_group_free(&g);
}
int mm_collect (mw_meta *m, uint64_t F, uint64_t V, fitem **out, int *n) {
    col_ctx c = { m, F, NULL, 0, 0, 0 };
    (void)V; shidx_scan(m->db->rx, F, UINT64_MAX, col_cb, &c);                  // (the newest state of every bucket changed since F, whatever its epoch: it holds the older ones)
    if (c.err) { for (int i = 0; i < c.n; i++) { free(c.v[i].pk); free(c.v[i].c); } free(c.v); return -1; }
    *out = c.v; *n = c.n; return 0;
}
