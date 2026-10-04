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
    uint32_t r = UINT32_MAX; bool found = false;                              // (UINT32_MAX: the table is full - ordinal 0 is this database, and a remote site must not be taken for it)
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
    while (n < ord) { memset(sh->sites[n], 0, 16); n++; atomic_store_explicit(&sh->nsites, n, memory_order_release); }
    static const uint8_t zero[16] = {0};
    if (n == ord) { memcpy(sh->sites[ord], id, 16); atomic_store_explicit(&sh->nsites, ord + 1, memory_order_release); }     // (the id is there before the count says that it is: a reader without the lock must not see a zero id)
    else if (!memcmp(sh->sites[ord], zero, 16)) memcpy(sh->sites[ord], id, 16);
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

// the cells of a table that was dropped (and not yet forgotten by the index and the file) older than the drop are dead
static int purge_filter (mw_db *db, uint32_t tbl, mw_mcell *c, int n) {
    mw_shm *sh = db->shm; uint32_t np = atomic_load_explicit(&sh->npurge, memory_order_acquire);
    for (uint32_t q = 0; q < np; q++) if (atomic_load(&sh->purge[q].tbl) == tbl) { uint64_t ep = atomic_load(&sh->purge[q].epoch); int k = 0; for (int i = 0; i < n; i++) if (c[i].dv >= (int64_t)ep) c[k++] = c[i]; n = k; }
    return n;
}

// the group that starts at `loc` of the log (a version installed at `epoch`): its rows. false: the segment is gone (the state it held is older than the base, so it is in the file)
// 1: read; 0: the segment is gone (below the base of the log); -1: it could not be read or it is damaged (a bucket taken for empty would make the commit rewrite it from nothing and lose the newest cells of the others)
static int read_group (mw_meta *m, uint64_t loc, uint64_t epoch, uint32_t bucket_hint, mm_group *g) {
    mw_db *db = m->db; (void)bucket_hint;
    const uint64_t dvres = epoch + (uint64_t)mw_meta_origin(m);                 // (the db_version of the commit whose cells say "this commit")
    uint8_t hb[5]; if (!mw_seglog_read(db, loc, 0, 5, hb)) return MW_LOC_SEG(loc) < atomic_load(&db->shm->seg_min) ? 0 : -1;
    const uint8_t *p = hb; uint64_t glen; if (rv(&p, hb + 5, &glen) || glen > (64u << 20)) return -1;
    size_t hl = (size_t)(p - hb), total = hl + (size_t)glen;
    uint8_t *raw = malloc(total ? total : 1); if (!raw) return -1;
    if (!mw_seglog_read(db, loc, 0, (uint32_t)total, raw)) { free(raw); return MW_LOC_SEG(loc) < atomic_load(&db->shm->seg_min) ? 0 : -1; }
    const uint8_t *q = raw + hl, *end = raw + total; uint64_t bucket, nrows;
    if (rv(&q, end, &bucket) || rv(&q, end, &nrows) || nrows > (1u << 24)) { free(raw); return -1; }
    mm_row *rows = calloc((size_t)(nrows ? nrows : 1), sizeof *rows); if (!rows) { free(raw); return -1; }
    for (uint64_t i = 0; i < nrows; i++) {
        uint64_t tbl, pklen, nc;
        if (rv(&q, end, &tbl) || rv(&q, end, &pklen) || pklen > (uint64_t)(end - q)) goto bad;
        rows[i].tbl = (uint32_t)tbl; rows[i].pk = q; rows[i].pklen = (uint32_t)pklen; q += pklen;
        if (rv(&q, end, &nc) || nc > (1u << 20)) goto bad;
        rows[i].c = malloc((size_t)(nc ? nc : 1) * sizeof(mw_mcell)); if (!rows[i].c) goto bad;
        rows[i].n = (int)nc;
        czd zd; cz_dinit(&zd);
        for (uint64_t k = 0; k < nc; k++) {
            uint64_t v[5];
            if (cz_dget(&zd, &q, end, v)) goto bad;
            rows[i].c[k] = (mw_mcell){ (int64_t)v[1], v[2] ? (int64_t)(v[2] - 1) : (int64_t)dvres, (uint32_t)v[0], (uint32_t)v[3], (uint32_t)v[4] };
        }
        if (zd.rep) goto bad;
        if (nc) { int kept = purge_filter(db, rows[i].tbl, rows[i].c, (int)nc); if (kept == 0) { free(rows[i].c); rows[i].c = NULL; rows[i].tbl = 0xFFFFFFFFu; rows[i].n = 0; } else rows[i].n = kept; }
        continue;
    bad:
        for (uint64_t j = 0; j <= i; j++) free(rows[j].c);
        free(rows); free(raw); return -1;
    }
    int w = 0; for (uint64_t i = 0; i < nrows; i++) if (rows[i].tbl != 0xFFFFFFFFu) rows[w++] = rows[i];       // (the rows of dropped tables are gone)
    g->raw = raw; g->rawlen = total; g->rows = rows; g->n = w; g->bucket = (uint32_t)bucket; g->epoch = epoch;
    return 1;
}

int mm_head (mw_meta *m, uint32_t bucket, mm_group *g) {
    memset(g, 0, sizeof *g);
    uint64_t ep, loc;
    if (!shidx_lookup(m->db->rx, bucket, UINT64_MAX, &ep, &loc)) return 0;
    int rg = read_group(m, loc, ep, bucket, g);
    if (rg < 0) { memset(g, 0, sizeof *g); return -1; }
    if (rg == 0) { memset(g, 0, sizeof *g); g->epoch = ep; return 0; }     // (unreadable: its segment is gone, so it is older than the base and its rows are in the file; the epoch is still the one the transaction read)
    return 0;
}

// ---- publication (the publication lock is held) ----
// The buckets this commit writes must be as the transaction read them: a change of the bucket since means another commit changed a row the transaction took its versions from.
int mm_validate (mw_db *db, mw_lane *lane) {
    if (!lane || !lane->cdc_ng) return SQLITE_OK;
    for (int i = 0; i < lane->cdc_ng; i++) {
        uint64_t head = shidx_head_epoch(db->rx, lane->cdc_gbucket[i]);
        if (head == lane->cdc_gseen[i]) continue;
        if (head == 0) continue;           // the versions of the bucket were retired (everything up to the base is in the file, which is where a later read finds it): nobody changed it, or there would be a newer head
        return MW_CONFLICT;
    }
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
    atomic_store(&sh->meta_last, epoch);
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
    mw_ovl_bloom_update(lane->cdc_decl ? lane->cdc_decl : lane->cdc_ovl);
    mw_ext_purges(lane->cdc_ext, lane->cdc_ext_len, purge_cb, epoch + atomic_load(&sh->dv_origin), sh);
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
static int rep_row (void *arg, uint32_t bucket, uint32_t tbl, const uint8_t *pk, size_t pklen, const mw_mcell *c, int n) {
    (void)bucket; mw_db *db = arg; extern mw_meta *mw_cdc_meta (mw_db *);
    for (int k = 0; k < n; k++) if (c[k].col == CRDT_COL_SENTINEL) { mw_meta *m = mw_cdc_meta(db); if (m) mw_meta_bloom_add(m, tbl, pk, pklen); break; }
    return 0;
}
int mm_replay (mw_db *db, uint64_t epoch, const uint8_t *ext, uint32_t len, uint64_t ext_loc) {
    mw_ext_walk(ext, len, epoch, rep_row, NULL, rep_site, db);
    rep_ctx r = { db, epoch, ext_loc, NULL, NULL, 0, 0 };
    int rc = SQLITE_OK;
    if (mw_ext_groups(ext, len, rep_group, &r) != 0) rc = SQLITE_CORRUPT;
    else {
        if (r.n && shidx_room(db->rx) < (uint32_t)r.n + 2) shidx_gc_floor(db->rx, epoch - 1, 0);
        if (r.n && shidx_install(db->rx, epoch, 0, r.n, r.b, r.l) != 0) rc = SQLITE_FULL;
        mw_ext_purges(ext, len, purge_cb, epoch + atomic_load(&db->shm->dv_origin), db->shm);
        if (r.n) atomic_fetch_add(&db->shm->meta_dirty, (uint64_t)r.n);
        atomic_store(&db->shm->meta_last, epoch);
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
        uint64_t F = 0, hwm = 0; uint32_t flushed_sites = 0; uint8_t own[16]; bool have_own = false;
        int lrc = mw_metafile_load_state(m, &F, &hwm, &flushed_sites, &have_own, own);
        if (lrc != SQLITE_OK) { mw_mp_meta_unlock(m->db, 1); pthread_mutex_unlock(&m->file_mu); return lrc; }
        atomic_store(&sh->dv_origin, hwm); atomic_store(&sh->dv_hwm, hwm);
        if (!have_own) sqlite3_randomness(16, own);
        mm_site_install_db(m->db, 0, own);
        if (!have_own) { /* a fresh id: it is written with the first flush */ }
        atomic_store(&sh->sites_flushed, flushed_sites);
        atomic_store(&sh->meta_flushed, F);
        if (mw_metafile_load_tombstones(m) != 0) { mw_mp_meta_unlock(m->db, 1); pthread_mutex_unlock(&m->file_mu); return SQLITE_IOERR_READ; }
        atomic_store_explicit(&sh->meta_state, 2, memory_order_release);
    }
    mw_mp_meta_unlock(m->db, 1);
    pthread_mutex_unlock(&m->file_mu);
    return 0;
}

// The flush reads every dirty bucket once: the group is parsed in place into buffers that live for the whole scan (no allocation per bucket).
typedef struct { mw_meta *m; uint64_t F; fbatch *out; int err; uint8_t *raw; size_t rawcap; mw_mcell *cells; int ccap; } col_ctx;
static void col_cb (void *arg, uint32_t bucket, uint64_t epoch, uint64_t loc) {
    col_ctx *c = arg; mw_db *db = c->m->db; (void)bucket;
    if (c->err) return;
    const uint64_t dvres = epoch + (uint64_t)mw_meta_origin(c->m);
    uint8_t hb[5]; if (!mw_seglog_read(db, loc, 0, 5, hb)) { c->err = 1; return; }          // (a segment that is gone held a state older than the base: it is in the file; but the scan only meets versions newer than the flushed point)
    const uint8_t *p = hb; uint64_t glen; if (rv(&p, hb + 5, &glen) || glen > (64u << 20)) { c->err = 1; return; }
    size_t hl = (size_t)(p - hb), total = hl + (size_t)glen;
    if (total > c->rawcap) { uint8_t *nr = realloc(c->raw, total + 256); if (!nr) { c->err = 1; return; } c->raw = nr; c->rawcap = total + 256; }
    if (!mw_seglog_read(db, loc, 0, (uint32_t)total, c->raw)) { c->err = 1; return; }
    const uint8_t *q = c->raw + hl, *end = c->raw + total; uint64_t bk, nrows;
    if (rv(&q, end, &bk) || rv(&q, end, &nrows)) { c->err = 1; return; }
    for (uint64_t i = 0; i < nrows; i++) {
        uint64_t tbl, pklen, nc;
        if (rv(&q, end, &tbl) || rv(&q, end, &pklen) || q + pklen > end) { c->err = 1; return; }
        const uint8_t *pk = q; q += pklen;
        if (rv(&q, end, &nc) || nc > (1u << 20)) { c->err = 1; return; }
        if ((int)nc > c->ccap) { int cc = (int)nc * 2 + 8; mw_mcell *nm = realloc(c->cells, (size_t)cc * sizeof *nm); if (!nm) { c->err = 1; return; } c->cells = nm; c->ccap = cc; }
        czd zd; cz_dinit(&zd);
        for (uint64_t k = 0; k < nc; k++) {
            uint64_t v[5];
            if (cz_dget(&zd, &q, end, v)) { c->err = 1; return; }
            c->cells[k] = (mw_mcell){ (int64_t)v[1], v[2] ? (int64_t)(v[2] - 1) : (int64_t)dvres, (uint32_t)v[0], (uint32_t)v[3], (uint32_t)v[4] };
        }
        if (zd.rep) { c->err = 1; return; }
        int n = purge_filter(db, (uint32_t)tbl, c->cells, (int)nc);
        int dirtyc = 0;
        for (int k = 0; k < n; k++) if (c->cells[k].dv > (int64_t)c->F) dirtyc++;
        if (!dirtyc) continue;
        if (!mw_fbatch_add_row(c->out, (uint32_t)tbl, pk, (uint32_t)pklen, c->cells, n)) { c->err = 1; return; }         // (the whole state of the row: the file replaces its copy)
    }
}
int mm_collect (mw_meta *m, uint64_t F, uint64_t Fe, fbatch *out) {
    col_ctx c = { m, F, out, 0, NULL, 0, NULL, 0 };
    shidx_scan(m->db->rx, Fe, UINT64_MAX, col_cb, &c);                          // (the newest state of every bucket changed since F, whatever its epoch: it holds the older ones)
    free(c.raw); free(c.cells);
    if (c.err) { mw_fbatch_free(out); return -1; }
    return 0;
}
