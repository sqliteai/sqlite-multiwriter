//
//  multiwriter_meta.c
//
//  See multiwriter_meta.h. The store is a hash table of rows in memory, striped by row; an entry holds the cells of one row. Rows that changed after the last flush to the file are
//  "dirty" (never evicted); the others are a cache that is filled from the file when a row is asked for and dropped when the table grows over its budget.
//
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <stdatomic.h>
#include "multiwriter_meta.h"

#define STRIPES 64
#define SEN CRDT_COL_SENTINEL
#define OV_CHG INT64_MIN                 // in an overlay cell: written by this commit (its db_version is the commit's epoch, not known yet)
#define F_DROP 1                         // the non-sentinel cells of the row are removed
#define F_ZERO 2                         // the non-sentinel cells get version 0 and the db_version of the commit

typedef struct mentry {
    struct mentry *next, *dnext;
    uint64_t h, ver, drop_ver;           // ver: epoch of the last change; drop_ver: epoch of the last DROP (the file must lose the old cells too)
    uint32_t tbl, pklen; int n, cap; bool in_dirty;
    mw_mcell *cells;
    uint8_t pk[];
} mentry;

typedef struct { pthread_mutex_t mu; mentry **b; size_t nb, n; mentry *dirty; size_t bytes; size_t hand; uint64_t gen; } stripe;

struct mw_meta {
    struct mw_db *db;
    stripe st[STRIPES];
    size_t cap_rows;                                            // the cache budget (rows) before clean entries are dropped
    pthread_mutex_t site_mu; uint8_t (*sites)[16]; uint32_t nsites, capsites;     // ord -> site id (0 = this database)
    _Atomic uint64_t flushed;                                    // epoch up to which the cells are in the file
    _Atomic uint64_t hits, misses, rows, bytes;
};

typedef struct { mw_mcell *c; int n, cap; } cellvec;
typedef struct {
    uint32_t tbl; uint8_t *pk; uint32_t pklen; uint8_t flags; mw_mcell *c; int n, cap;
} orow;
struct mw_ovl {
    mw_meta *m; orow *rows; int n, cap; int *hash; int hcap;
    mw_value_fn vfn; void *varg;
    uint32_t new_sites_lo;                                       // ords >= this are not in the file yet: the extension names them
};

static uint64_t hash_row (uint32_t tbl, const void *pk, size_t n) {
    uint64_t h = 1469598103934665603ull ^ tbl;
    h *= 1099511628211ull;
    const uint8_t *p = pk;
    for (size_t i = 0; i < n; i++) { h ^= p[i]; h *= 1099511628211ull; }
    return h ^ (h >> 29);
}

// ---- file (the tables of the database): not there yet ----
static int file_load (mw_meta *m, uint32_t tbl, const void *pk, size_t pklen, mw_mcell **cells, int *n) { (void)m; (void)tbl; (void)pk; (void)pklen; *cells = NULL; *n = 0; return 0; }

// ---- the table ----
mw_meta *mw_meta_new (struct mw_db *db) {
    mw_meta *m = calloc(1, sizeof *m);
    if (!m) return NULL;
    m->db = db; m->cap_rows = 1u << 20;
    const char *e = getenv("MW_META_CACHE_ROWS"); if (e && atol(e) > 0) m->cap_rows = (size_t)atol(e);
    for (int i = 0; i < STRIPES; i++) {
        pthread_mutex_init(&m->st[i].mu, NULL);
        m->st[i].nb = 64; m->st[i].b = calloc(m->st[i].nb, sizeof(mentry *));
        if (!m->st[i].b) { mw_meta_free(m); return NULL; }
    }
    pthread_mutex_init(&m->site_mu, NULL);
    m->capsites = 16; m->sites = calloc(m->capsites, 16); m->nsites = 1;
    if (!m->sites) { mw_meta_free(m); return NULL; }
    arc4random_buf(m->sites[0], 16);                             // this database's own id: replaced by the one in the file when it has one
    return m;
}

void mw_meta_free (mw_meta *m) {
    if (!m) return;
    for (int i = 0; i < STRIPES; i++) {
        for (size_t k = 0; m->st[i].b && k < m->st[i].nb; k++) for (mentry *e = m->st[i].b[k], *nx; e; e = nx) { nx = e->next; free(e->cells); free(e); }
        free(m->st[i].b); pthread_mutex_destroy(&m->st[i].mu);
    }
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
    uint64_t h = hash_row(tbl, pk, pklen); stripe *s = &m->st[h % STRIPES];
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

int mw_meta_row (mw_meta *m, uint32_t tbl, const void *pk, size_t pklen, mw_mcell **cells, int *n) { return load_row(m, tbl, pk, pklen, cells, n); }

// ---- sites ----
uint32_t mw_meta_site_ord (mw_meta *m, const uint8_t id[16]) {
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
    pthread_mutex_lock(&m->site_mu);
    bool ok = ord < m->nsites; if (ok) memcpy(out, m->sites[ord], 16);
    pthread_mutex_unlock(&m->site_mu);
    return ok;
}
static void site_install (mw_meta *m, uint32_t ord, const uint8_t id[16]) {          // replay: the ord is the one the commit used
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
    o->n = 0; if (o->hash) memset(o->hash, 0xff, (size_t)o->hcap * sizeof(int));
    o->new_sites_lo = 0;
}
void mw_ovl_free (mw_ovl *o) { if (!o) return; mw_ovl_clear(o); free(o->rows); free(o->hash); free(o); }
void mw_ovl_set_value_fn (mw_ovl *o, mw_value_fn fn, void *arg) { o->vfn = fn; o->varg = arg; }
bool mw_ovl_empty (const mw_ovl *o) { return o->n == 0; }

static void ovl_rehash (mw_ovl *o, int ncap) {
    int *nh = malloc((size_t)ncap * sizeof(int)); if (!nh) return;
    memset(nh, 0xff, (size_t)ncap * sizeof(int));
    for (int i = 0; i < o->n; i++) { uint64_t h = hash_row(o->rows[i].tbl, o->rows[i].pk, o->rows[i].pklen); size_t j = h & (size_t)(ncap - 1); while (nh[j] >= 0) j = (j + 1) & (size_t)(ncap - 1); nh[j] = i; }
    free(o->hash); o->hash = nh; o->hcap = ncap;
}

// the overlay row of (tbl, pk): loaded from the store on first touch
static orow *ovl_row (mw_ovl *o, uint32_t tbl, const void *pk, size_t pklen, bool create) {
    if (o->hcap == 0 || (o->n + 1) * 2 > o->hcap) ovl_rehash(o, o->hcap ? o->hcap * 2 : 64);
    if (!o->hash) return NULL;
    uint64_t h = hash_row(tbl, pk, pklen); size_t j = h & (size_t)(o->hcap - 1);
    while (o->hash[j] >= 0) { orow *r = &o->rows[o->hash[j]]; if (r->tbl == tbl && r->pklen == pklen && !memcmp(r->pk, pk, pklen)) return r; j = (j + 1) & (size_t)(o->hcap - 1); }
    (void)create;
    if (o->n == o->cap) { int nc = o->cap ? o->cap * 2 : 64; orow *nr = realloc(o->rows, (size_t)nc * sizeof *nr); if (!nr) return NULL; o->rows = nr; o->cap = nc; }
    orow *r = &o->rows[o->n]; memset(r, 0, sizeof *r);
    r->tbl = tbl; r->pklen = (uint32_t)pklen; r->pk = malloc(pklen ? pklen : 1); if (!r->pk) return NULL; memcpy(r->pk, pk, pklen);
    if (load_row(o->m, tbl, pk, pklen, &r->c, &r->n) != 0) { free(r->pk); return NULL; }
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
    for (int i = 0; i < r->n; i++) if (r->c[i].col != SEN) r->c[i].cv = 0;
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

int mw_ovl_encode (mw_ovl *o, uint8_t **ext, uint32_t *len) {
    *ext = NULL; *len = 0;
    int nrows = 0; for (int i = 0; i < o->n; i++) { orow *r = &o->rows[i]; bool any = r->flags != 0; for (int k = 0; k < r->n && !any; k++) if (r->c[k].dv == OV_CHG) any = true; if (any) nrows++; }
    if (!nrows) return 0;
    wbuf w = {0};
    uint8_t ver = 0x4d; w_bytes(&w, &ver, 1);
    // the sites the cells name (the extension carries their ids: a replay must be able to rebuild the ord -> id map)
    // collect the ords > 0 used by puts (a small set: linear)
    uint32_t *ords = NULL; int no = 0, cap = 0;
    for (int i = 0; i < o->n; i++) for (int k = 0; k < o->rows[i].n; k++) {
        mw_mcell *c = &o->rows[i].c[k]; if (c->dv != OV_CHG || c->site == 0) continue;
        int f = 0; for (int q = 0; q < no; q++) if (ords[q] == c->site) { f = 1; break; }
        if (!f) { if (no == cap) { cap = cap ? cap * 2 : 8; ords = realloc(ords, (size_t)cap * sizeof *ords); if (!ords) { free(w.p); return -1; } } ords[no++] = c->site; }
    }
    w_var(&w, (uint64_t)no);
    for (int q = 0; q < no; q++) { uint8_t id[16]; if (!mw_meta_site_id(o->m, ords[q], id)) memset(id, 0, 16); w_var(&w, ords[q]); w_bytes(&w, id, 16); }
    free(ords);
    w_var(&w, (uint64_t)nrows);
    for (int i = 0; i < o->n; i++) {
        orow *r = &o->rows[i]; int np = 0;
        for (int k = 0; k < r->n; k++) if (r->c[k].dv == OV_CHG) np++;
        if (!np && !r->flags) continue;
        w_var(&w, r->tbl); w_var(&w, r->pklen); w_bytes(&w, r->pk, r->pklen);
        uint8_t fl = r->flags; w_bytes(&w, &fl, 1);
        w_var(&w, (uint64_t)np);
        for (int k = 0; k < r->n; k++) { mw_mcell *c = &r->c[k]; if (c->dv != OV_CHG) continue; w_var(&w, c->col); w_var(&w, (uint64_t)c->cv); w_var(&w, c->site); w_var(&w, c->seq); }
    }
    if (w.bad) { free(w.p); return -1; }
    *ext = w.p; *len = (uint32_t)w.n;
    return 0;
}

// ---- applying ----
typedef struct { uint32_t col, site, seq; int64_t cv; } put;

static int apply_row (mw_meta *m, uint32_t tbl, const uint8_t *pk, size_t pklen, uint8_t flags, const put *puts, int np, uint64_t epoch) {
    uint64_t h = hash_row(tbl, pk, pklen); stripe *s = &m->st[h % STRIPES];
    {
        pthread_mutex_lock(&s->mu);
        mentry *e = find(s, h, tbl, pk, pklen);
        if (!e) {                                                      // (evicted since the transaction read it, or a replay): bring the file's state in first
            pthread_mutex_unlock(&s->mu);
            mw_mcell *fc = NULL; int fn = 0;
            if (file_load(m, tbl, pk, pklen, &fc, &fn) != 0) return -1;
            pthread_mutex_lock(&s->mu);
            if (!find(s, h, tbl, pk, pklen)) { mentry *ne = entry_new(h, tbl, pk, pklen, fc, fn); if (!ne) { free(fc); pthread_mutex_unlock(&s->mu); return -1; } insert_entry(m, s, ne); }
            free(fc);
            e = find(s, h, tbl, pk, pklen);
        }
        size_t before = entry_bytes(e);
        if (flags & F_DROP) { int k = 0; for (int i = 0; i < e->n; i++) if (e->cells[i].col == SEN) e->cells[k++] = e->cells[i]; e->n = k; e->drop_ver = epoch; }
        if (flags & F_ZERO) for (int i = 0; i < e->n; i++) if (e->cells[i].col != SEN) { e->cells[i].cv = 0; e->cells[i].dv = (int64_t)epoch; }
        for (int q = 0; q < np; q++) {
            mw_mcell *c = NULL; for (int i = 0; i < e->n; i++) if (e->cells[i].col == puts[q].col) { c = &e->cells[i]; break; }
            if (!c) {
                if (e->n == e->cap) { int nc = e->cap ? e->cap * 2 : 4; mw_mcell *nm = realloc(e->cells, (size_t)nc * sizeof *nm); if (!nm) { pthread_mutex_unlock(&s->mu); return -1; } e->cells = nm; e->cap = nc; }
                c = &e->cells[e->n++];
            }
            *c = (mw_mcell){ puts[q].cv, (int64_t)epoch, puts[q].col, puts[q].site, puts[q].seq };
        }
        e->ver = epoch;
        if (!e->in_dirty) { e->in_dirty = true; e->dnext = s->dirty; s->dirty = e; }
        size_t after = entry_bytes(e); s->bytes += after - before; atomic_fetch_add(&m->bytes, after - before);
        evict(m, s);
        pthread_mutex_unlock(&s->mu);
        return 0;
    }
}

int mw_meta_apply (mw_meta *m, mw_ovl *o, uint64_t epoch) {
    put *puts = NULL; int pcap = 0, rc = 0;
    for (int i = 0; i < o->n && rc == 0; i++) {
        orow *r = &o->rows[i]; int np = 0;
        for (int k = 0; k < r->n; k++) if (r->c[k].dv == OV_CHG) { if (np == pcap) { pcap = pcap ? pcap * 2 : 16; puts = realloc(puts, (size_t)pcap * sizeof *puts); if (!puts) return -1; } puts[np++] = (put){ r->c[k].col, r->c[k].site, r->c[k].seq, r->c[k].cv }; }
        if (!np && !r->flags) continue;
        rc = apply_row(m, r->tbl, r->pk, r->pklen, r->flags, puts, np, epoch);
    }
    free(puts);
    return rc;
}

int mw_meta_replay (mw_meta *m, uint64_t epoch, const uint8_t *ext, uint32_t len) {
    const uint8_t *p = ext, *end = ext + len; uint64_t v;
    if (len < 1 || *p++ != 0x4d) return -1;
    if (r_var(&p, end, &v)) return -1;
    for (uint64_t i = 0; i < v; i++) { uint64_t ord; if (r_var(&p, end, &ord) || p + 16 > end) return -1; site_install(m, (uint32_t)ord, p); p += 16; }
    uint64_t nrows; if (r_var(&p, end, &nrows)) return -1;
    put *puts = NULL; int pcap = 0;
    for (uint64_t i = 0; i < nrows; i++) {
        uint64_t tbl, pklen, np; if (r_var(&p, end, &tbl) || r_var(&p, end, &pklen) || p + pklen + 1 > end) { free(puts); return -1; }
        const uint8_t *pk = p; p += pklen; uint8_t fl = *p++;
        if (r_var(&p, end, &np)) { free(puts); return -1; }
        if ((int)np > pcap) { pcap = (int)np * 2; puts = realloc(puts, (size_t)pcap * sizeof *puts); if (!puts) return -1; }
        for (uint64_t k = 0; k < np; k++) {
            uint64_t col, cv, site, seq;
            if (r_var(&p, end, &col) || r_var(&p, end, &cv) || r_var(&p, end, &site) || r_var(&p, end, &seq)) { free(puts); return -1; }
            puts[k] = (put){ (uint32_t)col, (uint32_t)site, (uint32_t)seq, (int64_t)cv };
        }
        if (apply_row(m, (uint32_t)tbl, pk, (size_t)pklen, fl, puts, (int)np, epoch) != 0) { free(puts); return -1; }
    }
    free(puts);
    return 0;
}

void mw_meta_stats (mw_meta *m, uint64_t *rows, uint64_t *bytes, uint64_t *hits, uint64_t *misses) {
    if (rows) *rows = atomic_load(&m->rows); if (bytes) *bytes = atomic_load(&m->bytes);
    if (hits) *hits = atomic_load(&m->hits); if (misses) *misses = atomic_load(&m->misses);
}
