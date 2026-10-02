//
//  multiwriter_meta.c
//
//  See multiwriter_meta.h. The store is a hash table of rows in memory, striped by row; an entry holds the cells of one row. Rows that changed after the last flush to the file are
//  "dirty" (never evicted); the others are a cache that is filled from the file when a row is asked for and dropped when the table grows over its budget.
//
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <stdatomic.h>
#include "multiwriter_meta_priv.h"
#include "multiwriter_internal.h"

typedef struct { mw_mcell *c; int n, cap; } cellvec;
typedef struct {
    uint32_t tbl; uint8_t *pk; uint32_t pklen; uint8_t flags; mw_mcell *c; int n, cap; int bk;      // bk: index in the overlay's buckets (shared mode), -1 otherwise
} orow;
typedef struct { uint32_t id; uint64_t seen; mm_group g; } obk;      // a bucket as the transaction read it: its head epoch and the rows it held
struct mw_ovl {
    mw_meta *m; orow *rows; int n, cap; int *hash; int hcap;
    mw_value_fn vfn; void *varg;
    uint32_t *purge; int npurge, cappurge;
    obk *bk; int nbk, capbk; int *bkh; int bkhcap;               // the buckets the transaction read (hash: bucket id -> index)
    int ng; uint32_t *gbucket, *goff; uint64_t *gseen;           // after an encode: the groups of the extension (bucket, offset of the group in the extension, head epoch the state was read at)
};

uint64_t mw_meta_hash (uint32_t tbl, const void *pk, size_t n) {
    uint64_t h = 1469598103934665603ull ^ tbl;
    h *= 1099511628211ull;
    const uint8_t *p = pk;
    for (size_t i = 0; i < n; i++) { h ^= p[i]; h *= 1099511628211ull; }
    return h ^ (h >> 29);
}

// ---- file (the tables of the database): not there yet ----
#define file_load mw_metafile_load

// ---- the table ----
mw_meta *mw_meta_new (struct mw_db *db) {
    mw_meta *m = calloc(1, sizeof *m);
    if (!m) return NULL;
    m->db = db; m->shared = db->shared; m->cap_rows = 1u << 20;
    const char *e = getenv("MW_META_CACHE_ROWS"); if (e && atol(e) > 0) m->cap_rows = (size_t)atol(e);
    for (int i = 0; i < STRIPES; i++) {
        pthread_mutex_init(&m->st[i].mu, NULL);
        m->st[i].nb = 64; m->st[i].b = calloc(m->st[i].nb, sizeof(mentry *));
        if (!m->st[i].b) { mw_meta_free(m); return NULL; }
    }
    pthread_mutex_init(&m->site_mu, NULL); pthread_mutex_init(&m->purge_mu, NULL); pthread_mutex_init(&m->file_mu, NULL); pthread_mutex_init(&m->th_mu, NULL); pthread_cond_init(&m->th_cv, NULL);
    for (int i = 0; i < MW_RDN; i++) pthread_mutex_init(&m->rdmu[i], NULL);
    m->capsites = 16; m->sites = calloc(m->capsites, 16); m->nsites = 1;
    if (!m->sites) { mw_meta_free(m); return NULL; }
    if (!m->shared) arc4random_buf(m->sites[0], 16);             // this database's own id: replaced by the one in the file when it has one (shared mode: it is in the shared header)
    return m;
}

void mw_meta_free (mw_meta *m) {
    if (!m) return;
    for (int i = 0; i < STRIPES; i++) {
        for (size_t k = 0; m->st[i].b && k < m->st[i].nb; k++) for (mentry *e = m->st[i].b[k], *nx; e; e = nx) { nx = e->next; free(e->cells); free(e); }
        free(m->st[i].b); pthread_mutex_destroy(&m->st[i].mu);
    }
    mw_metafile_free(m); free(m->purge);
    pthread_mutex_destroy(&m->site_mu); free(m->sites); free(m);
}

static size_t entry_bytes (const mentry *e) { return sizeof *e + e->pklen + (size_t)e->cap * sizeof(mw_mcell); }

static mentry *find (stripe *s, uint64_t h, uint32_t tbl, const void *pk, size_t pklen) {
    for (mentry *e = s->b[h & (s->nb - 1)]; e; e = e->next) if (e->h == h && e->tbl == tbl && e->pklen == pklen && !memcmp(e->pk, pk, pklen)) return e;
    return NULL;
}

static void grow (stripe *s) {
    size_t nn = s->nb * 2; mentry **nb = calloc(nn, sizeof *nb);
    if (!nb) return;
    for (size_t k = 0; k < s->nb; k++) for (mentry *e = s->b[k], *nx; e; e = nx) { nx = e->next; size_t j = e->h & (nn - 1); e->next = nb[j]; nb[j] = e; }
    free(s->b); s->b = nb; s->nb = nn;
}

// drops clean entries of the stripe until it is under its share of the budget (the caller holds the lock)
static void evict (mw_meta *m, stripe *s) {
    size_t cap = m->cap_rows / STRIPES + 1;
    size_t scanned = 0;
    while (s->n > cap && scanned < s->nb) {
        size_t k = s->hand++ & (s->nb - 1); scanned++;
        mentry **pp = &s->b[k];
        while (*pp) {
            mentry *e = *pp;
            if (!e->in_dirty && s->n > cap) { *pp = e->next; s->n--; s->bytes -= entry_bytes(e); atomic_fetch_sub(&m->bytes, entry_bytes(e)); atomic_fetch_sub(&m->rows, 1); free(e->cells); free(e); s->gen++; }
            else pp = &e->next;
        }
    }
}

static mentry *entry_new (uint64_t h, uint32_t tbl, const void *pk, size_t pklen, const mw_mcell *c, int n) {
    mentry *e = calloc(1, sizeof *e + pklen);
    if (!e) return NULL;
    e->h = h; e->tbl = tbl; e->pklen = (uint32_t)pklen; memcpy(e->pk, pk, pklen);
    e->cap = n; e->n = n;
    if (n) { e->cells = malloc((size_t)n * sizeof *c); if (!e->cells) { free(e); return NULL; } memcpy(e->cells, c, (size_t)n * sizeof *c); }
    return e;
}

static void insert_entry (mw_meta *m, stripe *s, mentry *e) {
    if (s->n * 2 > s->nb) grow(s);
    size_t j = e->h & (s->nb - 1); e->next = s->b[j]; s->b[j] = e; s->n++; s->bytes += entry_bytes(e);
    atomic_fetch_add(&m->bytes, entry_bytes(e)); atomic_fetch_add(&m->rows, 1);
}

// a copy of the cells of a row: the table first, the file on a miss (and the row is cached)
static int load_row (mw_meta *m, uint32_t tbl, const void *pk, size_t pklen, mw_mcell **out, int *n) {
    uint64_t h = mw_meta_hash(tbl, pk, pklen); stripe *s = &m->st[h % STRIPES];
    pthread_mutex_lock(&s->mu);
    mentry *e = find(s, h, tbl, pk, pklen);
    if (e) {
        atomic_fetch_add(&m->hits, 1);
        *n = e->n; *out = NULL;
        if (e->n) { *out = malloc((size_t)e->n * sizeof **out); if (!*out) { pthread_mutex_unlock(&s->mu); return -1; } memcpy(*out, e->cells, (size_t)e->n * sizeof **out); }
        pthread_mutex_unlock(&s->mu); return 0;
    }
    uint64_t gen = s->gen;
    pthread_mutex_unlock(&s->mu);
    atomic_fetch_add(&m->misses, 1);
    mw_mcell *fc = NULL; int fn = 0;
    if (file_load(m, tbl, pk, pklen, &fc, &fn) != 0) return -1;
    pthread_mutex_lock(&s->mu);
    if (s->gen == gen && !find(s, h, tbl, pk, pklen)) {                  // nobody changed this stripe meanwhile: cache what the file said (a row without cells is cached too: "known empty")
        mentry *ne = entry_new(h, tbl, pk, pklen, fc, fn);
        if (ne) { insert_entry(m, s, ne); evict(m, s); }
    }
    pthread_mutex_unlock(&s->mu);
    *out = fc; *n = fn;
    return 0;
}

int mw_meta_row (mw_meta *m, uint32_t tbl, const void *pk, size_t pklen, mw_mcell **cells, int *n) {
    if (m->shared) {
        mw_meta_ready(m);
        mm_group g; mm_head(m, mw_bucket_of(tbl, pk, pklen), &g);
        for (int q = 0; q < g.n; q++) if (g.rows[q].tbl == tbl && g.rows[q].pklen == pklen && !memcmp(g.rows[q].pk, pk, pklen)) {
            *n = g.rows[q].n; *cells = malloc((size_t)(*n ? *n : 1) * sizeof(mw_mcell)); if (!*cells) { mm_group_free(&g); return -1; }
            memcpy(*cells, g.rows[q].c, (size_t)*n * sizeof(mw_mcell)); mm_group_free(&g); return 0;
        }
        mm_group_free(&g);
        return file_load(m, tbl, pk, pklen, cells, n);
    }
    if (!atomic_load(&m->ready)) mw_meta_ready(m);
    return load_row(m, tbl, pk, pklen, cells, n);
}

// ---- sites ----
uint32_t mw_meta_site_ord (mw_meta *m, const uint8_t id[16]) {
    if (m->shared) return mm_site_ord(m, id);
    pthread_mutex_lock(&m->site_mu);
    uint32_t r = 0;
    for (uint32_t i = 0; i < m->nsites; i++) if (!memcmp(m->sites[i], id, 16)) { r = i; goto out; }
    if (m->nsites == m->capsites) { uint8_t (*ns)[16] = realloc(m->sites, (size_t)m->capsites * 2 * 16); if (!ns) { r = 0; goto out; } m->sites = ns; m->capsites *= 2; }
    memcpy(m->sites[m->nsites], id, 16); r = m->nsites++;
out:
    pthread_mutex_unlock(&m->site_mu);
    return r;
}
bool mw_meta_site_id (mw_meta *m, uint32_t ord, uint8_t out[16]) {
    if (m->shared) return mm_site_id(m, ord, out);
    pthread_mutex_lock(&m->site_mu);
    bool ok = ord < m->nsites; if (ok) memcpy(out, m->sites[ord], 16);
    pthread_mutex_unlock(&m->site_mu);
    return ok;
}
void mw_meta_site_install (mw_meta *m, uint32_t ord, const uint8_t id[16]) {          // replay: the ord is the one the commit used
    if (m->shared) { mm_site_install(m, ord, id); return; }
    pthread_mutex_lock(&m->site_mu);
    while (ord >= m->capsites) { uint8_t (*ns)[16] = realloc(m->sites, (size_t)m->capsites * 2 * 16); if (!ns) { pthread_mutex_unlock(&m->site_mu); return; } m->sites = ns; m->capsites *= 2; }
    while (m->nsites <= ord) { memset(m->sites[m->nsites], 0, 16); m->nsites++; }
    memcpy(m->sites[ord], id, 16);
    pthread_mutex_unlock(&m->site_mu);
}

// ---- the overlay ----
mw_ovl *mw_ovl_new (mw_meta *m) { mw_ovl *o = calloc(1, sizeof *o); if (o) o->m = m; return o; }
void mw_ovl_clear (mw_ovl *o) {
    for (int i = 0; i < o->n; i++) { free(o->rows[i].pk); free(o->rows[i].c); }
    for (int i = 0; i < o->nbk; i++) mm_group_free(&o->bk[i].g);
    o->nbk = 0; o->ng = 0; if (o->bkh) memset(o->bkh, 0xff, (size_t)o->bkhcap * sizeof(int));
    o->npurge = 0;
    o->n = 0; if (o->hash) memset(o->hash, 0xff, (size_t)o->hcap * sizeof(int));
}
void mw_ovl_free (mw_ovl *o) { if (!o) return; mw_ovl_clear(o); free(o->purge); free(o->bk); free(o->bkh); free(o->gbucket); free(o->goff); free(o->gseen); free(o->rows); free(o->hash); free(o); }
void mw_ovl_set_value_fn (mw_ovl *o, mw_value_fn fn, void *arg) { o->vfn = fn; o->varg = arg; }
bool mw_ovl_empty (const mw_ovl *o) { return o->n == 0 && o->npurge == 0; }
void mw_ovl_purge (mw_ovl *o, uint32_t tbl) {
    for (int i = 0; i < o->npurge; i++) if (o->purge[i] == tbl) return;
    if (o->npurge == o->cappurge) { int nc = o->cappurge ? o->cappurge * 2 : 8; uint32_t *np = realloc(o->purge, (size_t)nc * sizeof *np); if (!np) return; o->purge = np; o->cappurge = nc; }
    o->purge[o->npurge++] = tbl;
    for (int i = 0; i < o->n; i++) if (o->rows[i].tbl == tbl) { o->rows[i].n = 0; o->rows[i].flags = 0; }          // (what the commit did to that table before is moot)
}

static void ovl_rehash (mw_ovl *o, int ncap) {
    int *nh = malloc((size_t)ncap * sizeof(int)); if (!nh) return;
    memset(nh, 0xff, (size_t)ncap * sizeof(int));
    for (int i = 0; i < o->n; i++) { uint64_t h = mw_meta_hash(o->rows[i].tbl, o->rows[i].pk, o->rows[i].pklen); size_t j = h & (size_t)(ncap - 1); while (nh[j] >= 0) j = (j + 1) & (size_t)(ncap - 1); nh[j] = i; }
    free(o->hash); o->hash = nh; o->hcap = ncap;
}

// the overlay row of (tbl, pk): loaded from the store on first touch
static orow *ovl_row (mw_ovl *o, uint32_t tbl, const void *pk, size_t pklen, bool create) {
    if (o->hcap == 0 || (o->n + 1) * 2 > o->hcap) ovl_rehash(o, o->hcap ? o->hcap * 2 : 64);
    if (!o->hash) return NULL;
    uint64_t h = mw_meta_hash(tbl, pk, pklen); size_t j = h & (size_t)(o->hcap - 1);
    while (o->hash[j] >= 0) { orow *r = &o->rows[o->hash[j]]; if (r->tbl == tbl && r->pklen == pklen && !memcmp(r->pk, pk, pklen)) return r; j = (j + 1) & (size_t)(o->hcap - 1); }
    (void)create;
    if (o->n == o->cap) { int nc = o->cap ? o->cap * 2 : 64; orow *nr = realloc(o->rows, (size_t)nc * sizeof *nr); if (!nr) return NULL; o->rows = nr; o->cap = nc; }
    orow *r = &o->rows[o->n]; memset(r, 0, sizeof *r);
    r->tbl = tbl; r->pklen = (uint32_t)pklen; r->pk = malloc(pklen ? pklen : 1); if (!r->pk) return NULL; memcpy(r->pk, pk, pklen);
    r->bk = -1;
    if (o->m->shared) {                                                    // shared mode: the row is in its bucket's newest state, or in the file
        uint32_t b = mw_bucket_of(tbl, pk, pklen); int bi = -1;
        if (o->bkhcap == 0 || (o->nbk + 1) * 2 > o->bkhcap) {                // (bucket id -> index in o->bk, open addressing)
            int nc = o->bkhcap ? o->bkhcap * 2 : 64; int *nh = malloc((size_t)nc * sizeof(int)); if (!nh) { free(r->pk); return NULL; }
            memset(nh, 0xff, (size_t)nc * sizeof(int));
            for (int q = 0; q < o->nbk; q++) { size_t h = (size_t)(o->bk[q].id * 2654435761u) & (size_t)(nc - 1); while (nh[h] >= 0) h = (h + 1) & (size_t)(nc - 1); nh[h] = q; }
            free(o->bkh); o->bkh = nh; o->bkhcap = nc;
        }
        size_t hb = (size_t)(b * 2654435761u) & (size_t)(o->bkhcap - 1);
        while (o->bkh[hb] >= 0) { if (o->bk[o->bkh[hb]].id == b) { bi = o->bkh[hb]; break; } hb = (hb + 1) & (size_t)(o->bkhcap - 1); }
        if (bi < 0) {
            if (o->nbk == o->capbk) { int nc = o->capbk ? o->capbk * 2 : 8; obk *nb = realloc(o->bk, (size_t)nc * sizeof *nb); if (!nb) { free(r->pk); return NULL; } o->bk = nb; o->capbk = nc; }
            obk *x = &o->bk[o->nbk]; memset(x, 0, sizeof *x); x->id = b;
            if (mm_head(o->m, b, &x->g) != 0) { free(r->pk); return NULL; }
            x->seen = x->g.epoch; bi = o->nbk++; o->bkh[hb] = bi;
        }
        r->bk = bi;
        const mm_group *g = &o->bk[bi].g; bool found = false;
        for (int q = 0; q < g->n && !found; q++) if (g->rows[q].tbl == tbl && g->rows[q].pklen == pklen && !memcmp(g->rows[q].pk, pk, pklen)) {
            r->n = g->rows[q].n; r->c = malloc((size_t)(r->n ? r->n : 1) * sizeof(mw_mcell)); if (!r->c) { free(r->pk); return NULL; }
            memcpy(r->c, g->rows[q].c, (size_t)r->n * sizeof(mw_mcell)); found = true;
        }
        if (!found && file_load(o->m, tbl, pk, pklen, &r->c, &r->n) != 0) { free(r->pk); return NULL; }
    } else if (load_row(o->m, tbl, pk, pklen, &r->c, &r->n) != 0) { free(r->pk); return NULL; }
    r->cap = r->n;
    o->hash[j] = o->n++;
    return r;
}

static mw_mcell *cell_find (orow *r, uint32_t col) { for (int i = 0; i < r->n; i++) if (r->c[i].col == col) return &r->c[i]; return NULL; }

static bool ops_get (void *st, uint32_t tbl, const void *pk, size_t pklen, uint32_t col, crdt_cell *out) {
    orow *r = ovl_row(st, tbl, pk, pklen, true); if (!r) return false;
    mw_mcell *c = cell_find(r, col); if (!c) return false;
    *out = (crdt_cell){ c->cv, c->dv == OV_CHG ? 0 : c->dv, c->seq, c->site };
    return true;
}
static void ops_put (void *st, uint32_t tbl, const void *pk, size_t pklen, uint32_t col, const crdt_cell *cc) {
    orow *r = ovl_row(st, tbl, pk, pklen, true); if (!r) return;
    mw_mcell *c = cell_find(r, col);
    if (!c) {
        if (r->n == r->cap) { int nc = r->cap ? r->cap * 2 : 4; mw_mcell *nm = realloc(r->c, (size_t)nc * sizeof *nm); if (!nm) return; r->c = nm; r->cap = nc; }
        c = &r->c[r->n++];
    }
    *c = (mw_mcell){ cc->cv, OV_CHG, col, cc->site, (uint32_t)cc->seq };
}
static void ops_drop_cols (void *st, uint32_t tbl, const void *pk, size_t pklen) {
    orow *r = ovl_row(st, tbl, pk, pklen, true); if (!r) return;
    int k = 0; for (int i = 0; i < r->n; i++) if (r->c[i].col == SEN) r->c[k++] = r->c[i];
    r->n = k; r->flags |= F_DROP;
}
static void ops_zero_cols (void *st, uint32_t tbl, const void *pk, size_t pklen, int64_t dv) {
    (void)dv;
    orow *r = ovl_row(st, tbl, pk, pklen, true); if (!r) return;
    for (int i = 0; i < r->n; i++) if (r->c[i].col != SEN) { r->c[i].cv = 0; r->c[i].dv = OV_CHG; }         // (version 0 and the db_version of this commit)
    r->flags |= F_ZERO;
}
static bool ops_row_known (void *st, uint32_t tbl, const void *pk, size_t pklen) { orow *r = ovl_row(st, tbl, pk, pklen, true); return r && r->n > 0; }
static bool ops_value (void *st, uint32_t tbl, const void *pk, size_t pklen, uint32_t col, crdt_value *out) { mw_ovl *o = st; return o->vfn && o->vfn(o->varg, tbl, pk, pklen, col, out); }
static uint32_t ops_site_ord (void *st, const uint8_t site[16]) { mw_ovl *o = st; uint32_t ord = mw_meta_site_ord(o->m, site); return ord; }
static bool ops_site_bytes (void *st, uint32_t ord, uint8_t out[16]) { mw_ovl *o = st; return mw_meta_site_id(o->m, ord, out); }

static const crdt_ops OPS = { ops_get, ops_put, ops_drop_cols, ops_zero_cols, ops_row_known, ops_value, ops_site_ord, ops_site_bytes };
const crdt_ops *mw_ovl_ops (void) { return &OPS; }

// ---- the extension ----
typedef struct { uint8_t *p; size_t n, cap; bool bad; } wbuf;
static void w_bytes (wbuf *w, const void *d, size_t n) {
    if (w->bad) return;
    if (w->n + n > w->cap) { size_t nc = w->cap ? w->cap * 2 : 256; while (nc < w->n + n) nc *= 2; uint8_t *np = realloc(w->p, nc); if (!np) { w->bad = true; return; } w->p = np; w->cap = nc; }
    memcpy(w->p + w->n, d, n); w->n += n;
}
static void w_var (wbuf *w, uint64_t v) { uint8_t b[10]; int k = 0; while (v >= 0x80) { b[k++] = (uint8_t)(v | 0x80); v >>= 7; } b[k++] = (uint8_t)v; w_bytes(w, b, (size_t)k); }
static int r_var (const uint8_t **p, const uint8_t *end, uint64_t *v) {
    uint64_t r = 0; int sh = 0;
    while (*p < end && sh < 64) { uint8_t b = *(*p)++; r |= (uint64_t)(b & 0x7f) << sh; if (!(b & 0x80)) { *v = r; return 0; } sh += 7; }
    return -1;
}

static bool touched (const orow *r) { if (r->flags) return true; for (int k = 0; k < r->n; k++) if (r->c[k].dv == OV_CHG) return true; return false; }

// The extension of a commit: the sites its cells name, the tables it drops, then the rows it touched with their complete new state (an application of it is idempotent and needs
// nothing else: not the old state, not the order). The rows come in groups, each framed by its length: in shared mode a group is one bucket of the shared index with all the rows
// the bucket holds (the touched ones in their new state, the others as the transaction read them); otherwise there is one group.
static void put_row (wbuf *w, const uint32_t tbl, const uint8_t *pk, uint32_t pklen, const mw_mcell *c, int n) {
    w_var(w, tbl); w_var(w, pklen); w_bytes(w, pk, pklen); w_var(w, (uint64_t)n);
    for (int k = 0; k < n; k++) { w_var(w, c[k].col); w_var(w, (uint64_t)c[k].cv); w_var(w, c[k].dv == OV_CHG ? 0 : (uint64_t)c[k].dv + 1); w_var(w, c[k].site); w_var(w, c[k].seq); }
}
static void put_group (wbuf *w, uint32_t bucket, wbuf *body, int nrows) {            // body: the rows; framed as [length][bucket][nrows][rows]
    wbuf h = {0}; w_var(&h, bucket); w_var(&h, (uint64_t)nrows);
    w_var(w, h.n + body->n); w_bytes(w, h.p, h.n); w_bytes(w, body->p, body->n);
    free(h.p);
}

int mw_ovl_encode (mw_ovl *o, uint8_t **ext, uint32_t *len) {
    *ext = NULL; *len = 0; o->ng = 0;
    int nrows = 0; for (int i = 0; i < o->n; i++) if (touched(&o->rows[i])) nrows++;
    if (!nrows && !o->npurge) return 0;
    wbuf w = {0};
    uint8_t ver = EXT_VERSION; w_bytes(&w, &ver, 1);
    uint32_t *ords = NULL; int no = 0, cap = 0;                                       // the ords > 0 the cells of the touched rows use (a small set: linear)
    for (int i = 0; i < o->n; i++) { if (!touched(&o->rows[i])) continue; for (int k = 0; k < o->rows[i].n; k++) {
        uint32_t site = o->rows[i].c[k].site; if (site == 0) continue;
        int f = 0; for (int q = 0; q < no; q++) if (ords[q] == site) { f = 1; break; }
        if (!f) { if (no == cap) { cap = cap ? cap * 2 : 8; uint32_t *nw = realloc(ords, (size_t)cap * sizeof *ords); if (!nw) { free(ords); free(w.p); return -1; } ords = nw; } ords[no++] = site; }
    } }
    w_var(&w, (uint64_t)no);
    for (int q = 0; q < no; q++) { uint8_t id[16]; if (!mw_meta_site_id(o->m, ords[q], id)) memset(id, 0, 16); w_var(&w, ords[q]); w_bytes(&w, id, 16); }
    free(ords);
    w_var(&w, (uint64_t)o->npurge);                                       // (dropped tables first: a table dropped and created again in one commit starts empty)
    for (int i = 0; i < o->npurge; i++) w_var(&w, o->purge[i]);
    if (!o->m->shared) {
        w_var(&w, nrows ? 1 : 0);
        if (nrows) {
            wbuf body = {0};
            for (int i = 0; i < o->n; i++) { orow *r = &o->rows[i]; if (touched(r)) put_row(&body, r->tbl, r->pk, r->pklen, r->c, r->n); }
            if (body.bad) { free(body.p); free(w.p); return -1; }
            put_group(&w, 0xFFFFFFFFu, &body, nrows); free(body.p);
        }
    } else {
        // the touched rows by bucket (counting sort on the bucket index), so that building the groups is linear in the number of rows
        int *start = calloc((size_t)o->nbk + 1, sizeof(int)), *ord = malloc((size_t)(nrows ? nrows : 1) * sizeof(int));
        if (!start || !ord) { free(start); free(ord); free(w.p); return -1; }
        for (int i = 0; i < o->n; i++) if (touched(&o->rows[i])) start[o->rows[i].bk + 1]++;
        int ngroups = 0; for (int b = 0; b < o->nbk; b++) { if (start[b + 1]) ngroups++; start[b + 1] += start[b]; }
        { int *fill = malloc(((size_t)o->nbk + 1) * sizeof(int)); if (!fill) { free(start); free(ord); free(w.p); return -1; } memcpy(fill, start, ((size_t)o->nbk + 1) * sizeof(int));
          for (int i = 0; i < o->n; i++) if (touched(&o->rows[i])) ord[fill[o->rows[i].bk]++] = i; free(fill); }
        w_var(&w, (uint64_t)ngroups);
        if (ngroups) { o->gbucket = realloc(o->gbucket, (size_t)ngroups * 4); o->goff = realloc(o->goff, (size_t)ngroups * 4); o->gseen = realloc(o->gseen, (size_t)ngroups * 8); }
        for (int b = 0; b < o->nbk; b++) {
            if (start[b] == start[b + 1]) continue;
            wbuf body = {0}; int cnt = 0; const mm_group *g = &o->bk[b].g;
            for (int q = 0; q < g->n; q++) {                                    // the rows the bucket held: the transaction's version of them if it has one
                const orow *r = NULL;
                for (int k = start[b]; k < start[b + 1]; k++) { const orow *x = &o->rows[ord[k]]; if (x->tbl == g->rows[q].tbl && x->pklen == g->rows[q].pklen && !memcmp(x->pk, g->rows[q].pk, x->pklen)) { r = x; break; } }
                if (r) put_row(&body, r->tbl, r->pk, r->pklen, r->c, r->n); else put_row(&body, g->rows[q].tbl, g->rows[q].pk, g->rows[q].pklen, g->rows[q].c, g->rows[q].n);
                cnt++;
            }
            for (int k = start[b]; k < start[b + 1]; k++) {                     // touched rows that were not in the bucket yet
                const orow *r = &o->rows[ord[k]];
                bool in = false; for (int q = 0; q < g->n && !in; q++) if (g->rows[q].tbl == r->tbl && g->rows[q].pklen == r->pklen && !memcmp(g->rows[q].pk, r->pk, r->pklen)) in = true;
                if (!in) { put_row(&body, r->tbl, r->pk, r->pklen, r->c, r->n); cnt++; }
            }
            if (body.bad) { free(body.p); free(start); free(ord); free(w.p); return -1; }
            o->gbucket[o->ng] = o->bk[b].id; o->gseen[o->ng] = o->bk[b].seen; o->goff[o->ng] = (uint32_t)w.n; o->ng++;
            put_group(&w, o->bk[b].id, &body, cnt); free(body.p);
        }
        free(start); free(ord);
    }
    if (w.bad) { free(w.p); return -1; }
    *ext = w.p; *len = (uint32_t)w.n;
    return 0;
}
int mw_ovl_groups (const mw_ovl *o, const uint32_t **bucket, const uint32_t **off, const uint64_t **seen) { *bucket = o->gbucket; *off = o->goff; *seen = o->gseen; return o->ng; }

// ---- applying ----
// the row's state becomes `c` (n cells; those with dv == OV_CHG are of this commit: epoch)
static int install_row (mw_meta *m, uint32_t tbl, const uint8_t *pk, size_t pklen, const mw_mcell *c, int n, uint64_t epoch) {
    uint64_t h = mw_meta_hash(tbl, pk, pklen); stripe *s = &m->st[h % STRIPES];
    pthread_mutex_lock(&s->mu);
    mentry *e = find(s, h, tbl, pk, pklen);
    if (!e) { e = entry_new(h, tbl, pk, pklen, NULL, 0); if (!e) { pthread_mutex_unlock(&s->mu); return -1; } insert_entry(m, s, e); }
    size_t before = entry_bytes(e);
    bool removed = false;                                              // a cell of the old state is not in the new one: the file has to forget it
    for (int i = 0; i < e->n && !removed; i++) { bool f = false; for (int k = 0; k < n; k++) if (c[k].col == e->cells[i].col) { f = true; break; } if (!f) removed = true; }
    if (n > e->cap) { mw_mcell *nm = realloc(e->cells, (size_t)n * sizeof *nm); if (!nm) { pthread_mutex_unlock(&s->mu); return -1; } e->cells = nm; e->cap = n; }
    for (int k = 0; k < n; k++) { e->cells[k] = c[k]; if (c[k].dv == OV_CHG) e->cells[k].dv = (int64_t)epoch; }
    e->n = n;
    if (removed) e->drop_ver = epoch;
    e->ver = epoch;
    if (!e->in_dirty) { e->in_dirty = true; e->dnext = s->dirty; s->dirty = e; atomic_fetch_add(&m->ndirty, 1); }
    size_t after = entry_bytes(e); s->bytes += after - before; atomic_fetch_add(&m->bytes, after - before);
    evict(m, s);
    pthread_mutex_unlock(&s->mu);
    return 0;
}

static void purge_table (mw_meta *m, uint32_t tbl, uint64_t epoch) {
    for (int s = 0; s < STRIPES; s++) {
        stripe *st = &m->st[s]; pthread_mutex_lock(&st->mu);
        for (size_t k = 0; k < st->nb; k++) { mentry **pp = &st->b[k]; while (*pp) { mentry *e = *pp; if (e->tbl == tbl) { *pp = e->next; st->n--; st->bytes -= entry_bytes(e); atomic_fetch_sub(&m->bytes, entry_bytes(e)); atomic_fetch_sub(&m->rows, 1); if (e->in_dirty) e->tbl = 0xFFFFFFFFu; else { free(e->cells); free(e); } } else pp = &e->next; } }
        // dirty entries of the table: unlinked from the dirty list too (the list owns them now)
        mentry **pd = &st->dirty; while (*pd) { mentry *e = *pd; if (e->tbl == 0xFFFFFFFFu && e->in_dirty) { *pd = e->dnext; atomic_fetch_sub(&m->ndirty, 1); free(e->cells); free(e); } else pd = &e->dnext; }
        st->gen++; pthread_mutex_unlock(&st->mu);
    }
    pthread_mutex_lock(&m->purge_mu);
    for (int i = 0; i < m->npurge; i++) if (m->purge[i].tbl == tbl) { if (m->purge[i].epoch < epoch) m->purge[i].epoch = epoch; pthread_mutex_unlock(&m->purge_mu); return; }
    if (m->npurge == m->cappurge) { int nc = m->cappurge ? m->cappurge * 2 : 8; struct mw_purge *np = realloc(m->purge, (size_t)nc * sizeof *np); if (np) { m->purge = np; m->cappurge = nc; } }
    if (m->npurge < m->cappurge) m->purge[m->npurge++] = (struct mw_purge){ tbl, epoch };
    pthread_mutex_unlock(&m->purge_mu);
}

int mw_meta_apply (mw_meta *m, mw_ovl *o, uint64_t epoch) {
    if (m->shared) return 0;
    epoch += (uint64_t)m->origin;                                       // (from here on: the db_version of the commit)                                           // (the publisher installed the commit's buckets in the shared index)
    for (int i = 0; i < o->npurge; i++) purge_table(m, o->purge[i], epoch);
    for (int i = 0; i < o->n; i++) { orow *r = &o->rows[i]; if (!touched(r)) continue; if (install_row(m, r->tbl, r->pk, r->pklen, r->c, r->n, epoch) != 0) return -1; }
    return 0;
}

// Walks an extension: sites, purges, rows (with the cells they carry, dv resolved to `epoch`). Any callback may be NULL.
int mw_ext_walk (const uint8_t *ext, uint32_t len, uint64_t epoch, mw_ext_row_fn row_cb, mw_ext_purge_fn purge_cb, mw_ext_site_fn site_cb, void *arg) {
    const uint8_t *p = ext, *end = ext + len; uint64_t v;
    if (len < 1 || *p++ != EXT_VERSION) return -1;
    if (r_var(&p, end, &v)) return -1;
    for (uint64_t i = 0; i < v; i++) { uint64_t ord; if (r_var(&p, end, &ord) || p + 16 > end) return -1; if (site_cb) site_cb(arg, (uint32_t)ord, p); p += 16; }
    uint64_t npg; if (r_var(&p, end, &npg)) return -1;
    for (uint64_t i = 0; i < npg; i++) { uint64_t t; if (r_var(&p, end, &t)) return -1; if (purge_cb) purge_cb(arg, (uint32_t)t, epoch); }
    uint64_t ngroups; if (r_var(&p, end, &ngroups)) return -1;
    mw_mcell *cells = NULL; int ccap = 0;
    for (uint64_t g = 0; g < ngroups; g++) {
        uint64_t glen, bucket, nrows; if (r_var(&p, end, &glen) || glen > (uint64_t)(end - p)) { free(cells); return -1; }
        if (!row_cb) { p += glen; continue; }
        if (r_var(&p, end, &bucket) || r_var(&p, end, &nrows)) { free(cells); return -1; }
        for (uint64_t i = 0; i < nrows; i++) {
            uint64_t tbl, pklen, nc; if (r_var(&p, end, &tbl) || r_var(&p, end, &pklen) || p + pklen > end) { free(cells); return -1; }
            const uint8_t *pk = p; p += pklen;
            if (r_var(&p, end, &nc) || nc > (1u << 20)) { free(cells); return -1; }
            if ((int)nc > ccap) { ccap = (int)nc * 2 + 4; mw_mcell *nm = realloc(cells, (size_t)ccap * sizeof *nm); if (!nm) { free(cells); return -1; } cells = nm; }
            for (uint64_t k = 0; k < nc; k++) {
                uint64_t col, cv, dvp, site, seq;
                if (r_var(&p, end, &col) || r_var(&p, end, &cv) || r_var(&p, end, &dvp) || r_var(&p, end, &site) || r_var(&p, end, &seq)) { free(cells); return -1; }
                cells[k] = (mw_mcell){ (int64_t)cv, dvp ? (int64_t)(dvp - 1) : (int64_t)epoch, (uint32_t)col, (uint32_t)site, (uint32_t)seq };
            }
            if (row_cb && row_cb(arg, (uint32_t)bucket, (uint32_t)tbl, pk, (size_t)pklen, cells, (int)nc) != 0) { free(cells); return -1; }
        }
    }
    free(cells);
    return 0;
}

int mw_ext_groups (const uint8_t *ext, uint32_t len, mw_ext_group_fn cb, void *arg) {
    const uint8_t *p = ext, *end = ext + len; uint64_t v;
    if (len < 1 || *p++ != EXT_VERSION) return -1;
    if (r_var(&p, end, &v)) return -1;
    for (uint64_t i = 0; i < v; i++) { uint64_t ord; if (r_var(&p, end, &ord) || p + 16 > end) return -1; p += 16; }
    uint64_t npg; if (r_var(&p, end, &npg)) return -1;
    for (uint64_t i = 0; i < npg; i++) { uint64_t t; if (r_var(&p, end, &t)) return -1; }
    uint64_t ngroups; if (r_var(&p, end, &ngroups)) return -1;
    for (uint64_t g = 0; g < ngroups; g++) {
        const uint8_t *start = p; uint64_t glen, bucket; if (r_var(&p, end, &glen) || glen > (uint64_t)(end - p)) return -1;
        const uint8_t *q = p; if (r_var(&q, end, &bucket)) return -1;
        if (cb(arg, (uint32_t)bucket, (uint32_t)(start - ext)) != 0) return -1;
        p += glen;
    }
    return 0;
}

int mw_ext_purges (const uint8_t *ext, uint32_t len, mw_ext_purge_fn cb, uint64_t epoch, void *arg) {
    const uint8_t *p = ext, *end = ext + len; uint64_t v;
    if (len < 1 || *p++ != EXT_VERSION) return -1;
    if (r_var(&p, end, &v)) return -1;
    for (uint64_t i = 0; i < v; i++) { uint64_t ord; if (r_var(&p, end, &ord) || p + 16 > end) return -1; p += 16; }
    uint64_t npg; if (r_var(&p, end, &npg)) return -1;
    for (uint64_t i = 0; i < npg; i++) { uint64_t t; if (r_var(&p, end, &t)) return -1; cb(arg, (uint32_t)t, epoch); }
    return 0;
}

typedef struct { mw_meta *m; uint64_t epoch; } rctx;
static int replay_row (void *arg, uint32_t bucket, uint32_t tbl, const uint8_t *pk, size_t pklen, const mw_mcell *c, int n) { (void)bucket; rctx *r = arg; return install_row(r->m, tbl, pk, pklen, c, n, r->epoch); }
static void replay_purge (void *arg, uint32_t tbl, uint64_t epoch) { purge_table(((rctx *)arg)->m, tbl, epoch); }
static void replay_site (void *arg, uint32_t ord, const uint8_t id[16]) { mw_meta_site_install(((rctx *)arg)->m, ord, id); }

int mw_meta_replay (mw_meta *m, uint64_t epoch, const uint8_t *ext, uint32_t len) {
    epoch += (uint64_t)mw_meta_origin(m);
    rctx r = { m, epoch };
    return mw_ext_walk(ext, len, epoch, replay_row, replay_purge, replay_site, &r);
}

void mw_meta_stats (mw_meta *m, uint64_t *rows, uint64_t *bytes, uint64_t *hits, uint64_t *misses) {
    if (rows) *rows = atomic_load(&m->rows); if (bytes) *bytes = atomic_load(&m->bytes);
    if (hits) *hits = atomic_load(&m->hits); if (misses) *misses = atomic_load(&m->misses);
}
