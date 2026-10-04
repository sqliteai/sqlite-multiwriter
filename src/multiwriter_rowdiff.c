//
//  multiwriter_rowdiff.c
//
//  Row-level capture in the VFS (docs/design.md, docs/engine-history.md §51-55): from the page images a transaction wrote and the images the same pages had at its snapshot, the
//  net change of the rows of every table b-tree, without triggers or hooks. It decodes SQLite's file format directly: b-tree page header, cell pointer array, the cells of
//  table leaves (payload size, rowid, record), of index leaves and of index interior pages (the rows of WITHOUT ROWID tables), records with their serial types, overflow chains.
//  The net change over the whole write set makes splits and merges disappear (a row that moved between two written pages is no change); the freelist is walked because a
//  page the transaction frees is not rewritten (its old rows are only reachable through the freelist) and a page on the freelist holds stale rows that are not rows.
//  With the catalog (multiwriter_catalog.c) every row is a (table, key) with its primary key encoded as sqlite-sync does and the set of cells that changed.
//
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include <strings.h>
#include "multiwriter_internal.h"
#include "multiwriter_catalog.h"
#include "crdt.h"

#define MW_MAXCOLS 2048                                                       // columns of a record (SQLite's own limit is 2000)

static mw_rowdiff_sink_fn g_sink;
static void *g_sink_arg;
void mw_rowdiff_set_sink (mw_rowdiff_sink_fn fn, void *arg) { g_sink_arg = arg; g_sink = fn; }
bool mw_rowdiff_enabled (void) { return g_sink != NULL; }

typedef struct {
    uint32_t root; const mw_tab *tab; int64_t rowid;
    const uint8_t *rec; uint32_t reclen;       // the whole record (for a row with overflow: the local part and the chain, copied into `own`)
    uint8_t *own;
    uint8_t *key; uint32_t keylen;             // the identity of a WITHOUT ROWID row: its primary key encoded as sqlite-sync does (NULL for rowid tables)
    bool bad;                                  // the record could not be read in full
    uint32_t leaf; uint32_t *chain; int nchain; // (new state) the page the cell is in and the overflow pages of its record: the overflow owner map is kept from these
} rd_row;

static int be16 (const uint8_t *p) { return (p[0] << 8) | p[1]; }
static uint32_t be32 (const uint8_t *p) { return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) | ((uint32_t)p[2] << 8) | p[3]; }
// SQLite varint (1..9 bytes, big endian, 7 bits per byte, the 9th byte has 8)
static int varint (const uint8_t *p, const uint8_t *end, uint64_t *out) {
    uint64_t v = 0;
    for (int i = 0; i < 9 && p + i < end; i++) {
        if (i == 8) { *out = (v << 8) | p[i]; return 9; }
        v = (v << 7) | (p[i] & 0x7f);
        if (!(p[i] & 0x80)) { *out = v; return i + 1; }
    }
    return 0;
}

// Page kind: 'L' table leaf, 'T' table interior, 'I' index leaf or interior, 'S' page 1 (schema), 'O' anything else (overflow, freelist, lock-byte...)
static char page_kind (const uint8_t *pg, uint32_t pgno) {
    if (pgno == 1) return 'S';
    switch (pg[0]) { case 0x0d: return 'L'; case 0x05: return 'T'; case 0x0a: case 0x02: return 'I'; default: return 'O'; }
}

// Page `pgno` at the transaction's snapshot into `dst`; false if it does not exist there.
bool mw_rd_snap_page (mw_lane *lane, uint32_t pgno, uint8_t *dst) {
    mw_store *st = lane->db->store;
    uint32_t pgsz = (uint32_t)st->pgsz;
    if (lane->cdc_over) { for (int i = 0; i < lane->ws_n; i++) if (lane->ws_pgnos[i] == pgno) { memcpy(dst, lane->cdc_over[i], pgsz); return true; } }
    if (pgno == 0 || pgno > lane->dsz_val) return false;
    if (mw_store_read(st, pgno, lane->tx.snapshot_epoch, 0, pgsz, dst)) return true;
    mw_file *f = lane->file;
    return f->real->pMethods->xRead(f->real, dst, (int)pgsz, (sqlite3_int64)(pgno - 1) * pgsz) == SQLITE_OK;
}
static const uint8_t *ws_image (mw_lane *lane, const uint8_t *const *imgs, uint32_t pgno) {
    for (int i = 0; i < lane->ws_n; i++) if (lane->ws_pgnos[i] == pgno) return imgs[i];
    return NULL;
}
static int cmp_u32 (const void *a, const void *b) { uint32_t x = *(const uint32_t *)a, y = *(const uint32_t *)b; return x < y ? -1 : x > y; }

// The freelist (trunk pages and the leaf pages they list) of the snapshot (new = false) or of the committed state of the transaction (new = true: its images win).
static int freelist_of (mw_lane *lane, const uint8_t *const *imgs, bool new_state, const uint8_t *page1, uint32_t **out, int *n, int *cap, uint8_t *tmp) {
    uint32_t pgsz = (uint32_t)lane->db->store->pgsz;
    uint32_t trunk = be32(page1 + 32);
    for (int guard = 0; trunk && guard < 1000000; guard++) {
        const uint8_t *tp = new_state ? ws_image(lane, imgs, trunk) : NULL;
        if (!tp) { if (!mw_rd_snap_page(lane, trunk, tmp)) return -1; tp = tmp; }
        uint32_t next = be32(tp), cnt = be32(tp + 4);
        if (cnt > (pgsz - 8) / 4) return -1;
        if (*n + (int)cnt + 1 > *cap) { *cap = (*n + (int)cnt + 1) * 2; uint32_t *p = realloc(*out, (size_t)*cap * sizeof **out); if (!p) return -1; *out = p; }
        (*out)[(*n)++] = trunk;
        for (uint32_t k = 0; k < cnt; k++) (*out)[(*n)++] = be32(tp + 8 + 4 * k);
        trunk = next;
    }
    return 0;
}

// pgno -> root, for the children of the interior pages the transaction wrote (the owner of a page it allocated is the owner of the interior page that lists it)
typedef struct { uint32_t *pg, *root; uint32_t cap, n; } ovmap;
static void ov_put (ovmap *m, uint32_t pg, uint32_t root) {
    if ((m->n + 1) * 2 > m->cap) {
        uint32_t nc = m->cap ? m->cap * 2 : 64; uint32_t *np = calloc(nc, sizeof *np), *nr = calloc(nc, sizeof *nr);
        for (uint32_t i = 0; i < m->cap; i++) if (m->pg[i]) { uint32_t h = (m->pg[i] * 2654435761u) & (nc - 1); while (np[h]) h = (h + 1) & (nc - 1); np[h] = m->pg[i]; nr[h] = m->root[i]; }
        free(m->pg); free(m->root); m->pg = np; m->root = nr; m->cap = nc;
    }
    uint32_t h = (pg * 2654435761u) & (m->cap - 1);
    while (m->pg[h] && m->pg[h] != pg) h = (h + 1) & (m->cap - 1);
    if (!m->pg[h]) { m->pg[h] = pg; m->n++; }
    m->root[h] = root;
}
static uint32_t ov_get (const ovmap *m, uint32_t pg) {
    if (!m->cap) return 0;
    uint32_t h = (pg * 2654435761u) & (m->cap - 1);
    while (m->pg[h]) { if (m->pg[h] == pg) return m->root[h]; h = (h + 1) & (m->cap - 1); }
    return 0;
}
static uint32_t owner_new (const mw_rd_owner *own, const ovmap *ov, uint32_t pg) { if (own && own->cat && mw_cat_by_root(own->cat, pg)) return pg; uint32_t r = ov_get(ov, pg); return r ? r : (own && own->old_owner ? own->old_owner(own->ctx, pg) : 0); }

static bool rd_debug (void) { static int on = -1; if (on < 0) on = getenv("MW_ROWDIFF_DEBUG") != NULL; return on; }
// ---- records ----
// Column spans of a record: serial type, offset and length of every column; returns the number of columns, -1 if malformed
static int rec_cols (const uint8_t *rec, uint32_t reclen, uint64_t *ty, uint32_t *off, uint32_t *len, int max) {
    uint64_t hs; int h = varint(rec, rec + reclen, &hs); if (!h || hs > reclen) return -1;
    uint32_t pos = (uint32_t)hs, hp = (uint32_t)h; int n = 0;
    while (hp < hs && n < max) {
        uint64_t t; int k = varint(rec + hp, rec + hs, &t); if (!k) return -1; hp += (uint32_t)k;
        uint32_t l = t < 12 ? ((const uint8_t[]){0, 1, 2, 3, 4, 6, 8, 8, 0, 0, 0, 0})[t] : (uint32_t)((t - 12) / 2);
        ty[n] = t; off[n] = pos; len[n] = l; pos += l; n++;
    }
    return pos <= reclen ? n : -1;
}
static bool rec_value (const uint8_t *rec, uint64_t t, uint32_t off, uint32_t l, crdt_value *v) {
    memset(v, 0, sizeof *v);
    if (t == 0) { v->type = CRDT_NULL; return true; }
    if (t == 8 || t == 9) { v->type = CRDT_INTEGER; v->i = t == 9; return true; }
    if (t >= 1 && t <= 6) { int64_t x = (int8_t)rec[off]; for (uint32_t b = 1; b < l; b++) x = (x << 8) | rec[off + b]; v->type = CRDT_INTEGER; v->i = x; return true; }
    if (t == 7) { uint64_t u = 0; for (int b = 0; b < 8; b++) u = (u << 8) | rec[off + (uint32_t)b]; memcpy(&v->d, &u, 8); v->type = CRDT_FLOAT; return true; }
    if (t >= 12) { v->type = (t & 1) ? CRDT_TEXT : CRDT_BLOB; v->p = rec + off; v->n = l; return true; }
    return false;
}
// the primary key of a row, encoded as sqlite-sync does (the rowid itself for an INTEGER PRIMARY KEY or a table without a key)
static bool row_pk (const mw_tab *t, const rd_row *r, uint8_t **out, size_t *outn) {
    crdt_value v[MW_CAT_MAXPK]; int n = 0;
    if (t->alias_pk || !t->has_pk) { v[0] = (crdt_value){ CRDT_INTEGER, r->rowid, 0, NULL, 0 }; n = 1; }
    else {
        uint64_t ty[MW_MAXCOLS]; uint32_t off[MW_MAXCOLS], len[MW_MAXCOLS]; int nc = rec_cols(r->rec, r->reclen, ty, off, len, MW_MAXCOLS);
        if (nc < 0) return false;
        for (int k = 0; k < t->npk; k++) { int ri = t->pk_rec[k]; if (ri < 0 || ri >= nc || !rec_value(r->rec, ty[ri], off[ri], len[ri], &v[k])) return false; n++; }
    }
    uint8_t small[96]; size_t need = crdt_pk_encode(v, n, small, sizeof small);          // (one pass for the usual short keys)
    uint8_t *b = malloc(need ? need : 1); if (!b) return false;
    if (need <= sizeof small) memcpy(b, small, need); else crdt_pk_encode(v, n, b, need);
    *outn = need; *out = b; return true;
}

// ---- rows of a page ----
typedef struct { rd_row *a; int n, cap; } rowset;
// The payload of a cell: the local part, then (if it spilled) the overflow chain read from the images of the transaction (new state) or from its snapshot.
static bool full_payload (mw_lane *lane, const uint8_t *const *imgs, bool new_state, const uint8_t *local, uint32_t locallen, uint64_t total, uint32_t ovfl, uint8_t **own, uint32_t **chain, int *nchain) {
    uint32_t pgsz = (uint32_t)lane->db->store->pgsz;
    uint8_t *buf = malloc((size_t)total + 1), *tmp = malloc(pgsz); if (!buf || !tmp) { free(buf); free(tmp); return false; }
    memcpy(buf, local, locallen); size_t got = locallen;
    for (int guard = 0; ovfl && got < total && guard < 100000000; guard++) {
        const uint8_t *pg = new_state ? ws_image(lane, imgs, ovfl) : NULL;
        if (!pg) { if (!mw_rd_snap_page(lane, ovfl, tmp)) { free(buf); free(tmp); return false; } pg = tmp; }
        size_t take = total - got < pgsz - 4 ? (size_t)(total - got) : pgsz - 4;
        if (chain) { *chain = realloc(*chain, (size_t)(*nchain + 1) * sizeof **chain); (*chain)[(*nchain)++] = ovfl; }
        memcpy(buf + got, pg + 4, take); got += take; ovfl = be32(pg);
    }
    free(tmp);
    if (got < total) { free(buf); return false; }
    *own = buf; return true;
}

// Adds the rows of one page. kind: the page header byte (0x0d table leaf, 0x0a index leaf, 0x02 index interior). Rows of an index b-tree only count when the table of the page is
// a WITHOUT ROWID table (the others are indexes: derived data).
static int rows_of (mw_lane *lane, const uint8_t *const *imgs, bool new_state, const uint8_t *pg, uint32_t pgno, uint32_t pgsz, rowset *rs, uint32_t root, const mw_cat *cat, bool only_overflow) {
    uint8_t kind = pg[0];
    const mw_tab *tab = root && cat ? mw_cat_by_root(cat, root) : NULL;
    if (cat && root && (!tab || !tab->tracked)) return 0;                          // a table we cannot read (or an index): not captured
    if (tab && lane->cdc_nskip) for (int q = 0; q < lane->cdc_nskip; q++) if (!strcasecmp(lane->cdc_skip[q], tab->name)) return 0;     // reshaped by this commit's DDL
    if (tab && !new_state && lane->cdc_nskip_old) for (int q = 0; q < lane->cdc_nskip_old; q++) if (!strcasecmp(lane->cdc_skip_old[q], tab->name)) return 0;
    if (cat && kind != 0x0d && !(tab && tab->without_rowid)) return 0;
    if (cat && kind == 0x0d && tab && tab->without_rowid) return 0;
    const bool interior = kind == 0x02;
    const int hdr = interior ? 12 : 8;
    int nc = be16(pg + 3);
    const uint32_t U = pgsz, M = ((U - 12) * 32 / 255) - 23, X = kind == 0x0d ? U - 35 : ((U - 12) * 64 / 255) - 23;
    if (rs->n + nc > rs->cap) { int ncap = rs->cap ? rs->cap : 64; while (ncap < rs->n + nc) ncap *= 2; rd_row *p = realloc(rs->a, (size_t)ncap * sizeof *p); if (!p) return -1; rs->a = p; rs->cap = ncap; }      // (room for the cells of the page: not a growth every time the array doubles)
    for (int i = 0; i < nc; i++) {
        uint32_t off = (uint32_t)be16(pg + hdr + 2 * i);
        if (off < (uint32_t)hdr || off >= pgsz) return -1;
        const uint8_t *c = pg + off, *end = pg + pgsz;
        if (interior) c += 4;                                                       // (the child pointer)
        uint64_t P, rid = 0; int a = varint(c, end, &P); if (!a) return -1;
        int b = 0; if (kind == 0x0d) { b = varint(c + a, end, &rid); if (!b) return -1; }
        uint32_t local = (uint32_t)P, ovfl = 0;
        if (P > X) { uint32_t K = M + (uint32_t)((P - M) % (U - 4)); local = K <= X ? K : M; if (c + a + b + local + 4 > end) return -1; ovfl = be32(c + a + b + local); }
        else if (c + a + b + local > end) return -1;
        if (only_overflow && !ovfl) continue;
        if (rs->n == rs->cap) { rs->cap = rs->cap ? rs->cap * 2 : 64; rd_row *p = realloc(rs->a, (size_t)rs->cap * sizeof *p); if (!p) return -1; rs->a = p; }
        rd_row *r = &rs->a[rs->n++]; memset(r, 0, sizeof *r);
        r->root = root; r->tab = tab; r->rowid = (int64_t)rid; r->rec = c + a + b; r->reclen = local;
        r->leaf = pgno;
        if (ovfl) { if (full_payload(lane, imgs, new_state, r->rec, local, P, ovfl, &r->own, new_state ? &r->chain : NULL, &r->nchain)) { r->rec = r->own; r->reclen = (uint32_t)P; } else r->bad = true; }
        if (tab && tab->without_rowid && !r->bad) { size_t kl; if (!row_pk(tab, r, &r->key, &kl)) r->bad = true; else r->keylen = (uint32_t)kl; }
    }
    return 0;
}

static uint64_t row_table (const rd_row *r) { return r->tab ? (uint64_t)r->tab->tid : (1ull << 40) | r->root; }       // the table a row belongs to, by name (a rebuilt table keeps its rows whatever its root page is now)
static int row_cmp (const void *a, const void *b) {
    const rd_row *p = a, *q = b;
    uint64_t tp = row_table(p), tq = row_table(q);
    if (tp != tq) return tp < tq ? -1 : 1;
    if (p->key || q->key) {
        uint32_t m = p->keylen < q->keylen ? p->keylen : q->keylen; int c = (p->key && q->key) ? memcmp(p->key, q->key, m) : 0;
        return c ? c : (int)((int64_t)p->keylen - (int64_t)q->keylen);
    }
    return p->rowid < q->rowid ? -1 : p->rowid > q->rowid;
}
static void rows_free (rowset *rs) { for (int i = 0; i < rs->n; i++) { free(rs->a[i].own); free(rs->a[i].key); free(rs->a[i].chain); } free(rs->a); }

// the cells that differ between two records of the row, as a bit mask over the cells of the new definition (bit 63: all of them / not decidable); *pk_changed: the key columns differ.
// The two records may be laid out by different definitions of the table (ADD / DROP COLUMN in this commit): cells are matched by column name then.
static uint64_t cells_differ (const mw_tab *t, const rd_row *a, const rd_row *b, bool *pk_changed, uint64_t **wide) {
    *pk_changed = false; *wide = NULL;
    if (a->reclen == b->reclen && memcmp(a->rec, b->rec, a->reclen) == 0 && a->tab == b->tab) return 0;
    uint64_t ta[MW_MAXCOLS], tb[MW_MAXCOLS]; uint32_t oa[MW_MAXCOLS], ob[MW_MAXCOLS], la[MW_MAXCOLS], lb[MW_MAXCOLS];
    int na = rec_cols(a->rec, a->reclen, ta, oa, la, MW_MAXCOLS), nb = rec_cols(b->rec, b->reclen, tb, ob, lb, MW_MAXCOLS);
    if (na < 0 || nb < 0 || !t) return 1ull << 63;
    const mw_tab *ot = a->tab ? a->tab : t;
    bool same_layout = ot == t || (ot->ncells == t->ncells && ot->npk == t->npk && ot->without_rowid == t->without_rowid && ot->alias_pk == t->alias_pk);
    if (same_layout && ot != t) for (int i = 0; i < t->ncells && same_layout; i++) if (ot->cell_rec[i] != t->cell_rec[i] || strcasecmp(ot->cell_name[i], t->cell_name[i])) same_layout = false;
    uint64_t mask = 0;
    for (int i = 0; i < t->ncells; i++) {
        int oi = i;
        if (!same_layout) { oi = -1; for (int j = 0; j < ot->ncells; j++) if (!strcasecmp(ot->cell_name[j], t->cell_name[i])) { oi = j; break; } }
        int ri = t->cell_rec[i], rj = oi >= 0 ? ot->cell_rec[oi] : -1;
        bool ha = rj >= 0 && rj < na, hb = ri < nb;
        bool changed;
        if (ha != hb) changed = hb ? !(tb[ri] == 0) : !(ta[rj] == 0);              // (an absent column reads as NULL)
        else if (!ha) changed = false;
        else changed = ta[rj] != tb[ri] || (la[rj] && memcmp(a->rec + oa[rj], b->rec + ob[ri], la[rj]) != 0);
        if (changed) {
            if (i < 63) mask |= 1ull << i; else mask |= 1ull << 63;
            if (t->ncells > 63) { if (!*wide) *wide = calloc(((size_t)t->ncells + 63) / 64, sizeof(uint64_t)); if (*wide) (*wide)[i / 64] |= 1ull << (i % 64); }
        }
    }
    if (!t->alias_pk && t->has_pk && !t->without_rowid) {
        if (!same_layout && (ot->npk != t->npk || ot->alias_pk != t->alias_pk)) *pk_changed = true;
        else for (int k = 0; k < t->npk; k++) {
            int ri = t->pk_rec[k], rj = ot->pk_rec[k]; if (ri >= nb || rj >= na) continue;
            if (ta[rj] != tb[ri] || (la[rj] && memcmp(a->rec + oa[rj], b->rec + ob[ri], la[rj]) != 0)) *pk_changed = true;
        }
    }
    return mask;
}

// Computes the row changes of the commit. `own` (optional) gives the table of every page as it was before the commit and the catalog; without it the rows of all tables are pooled
// by rowid (the rowid tables only: the experiment of docs §51). The caller frees res->chg (and each pk / oldpk in it) and res->freed.
int mw_rowdiff_compute (mw_lane *lane, const uint8_t *const *imgs, const mw_rd_owner *own, mw_rd_result *res) {
    mw_store *st = lane->db->store;
    uint32_t pgsz = (uint32_t)st->pgsz;
    uint64_t t0 = 0; { struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts); t0 = (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec; }
    memset(res, 0, sizeof *res);
    const mw_cat *cat = own ? own->cat : NULL, *ocat = own && own->old_cat ? own->old_cat : cat;
    rowset oldr = {0}, newr = {0};
    uint8_t **keeps = NULL; int nkeeps = 0;                                           // copies of the old pages: the old rows point into them
    int opaque = 0, index_pages = 0, interior = 0, schema = 0, bad = 0;
    uint8_t *oldimg = malloc(pgsz);
    uint32_t *fo = NULL; int nfo = 0, cfo = 0;                                        // the freelist at the snapshot: those pages hold stale rows that are not rows
    uint8_t *p1old = malloc(pgsz);
    bool have_p1 = p1old && mw_rd_snap_page(lane, 1, p1old);
    if (have_p1 && oldimg && be32(p1old + 36) > 0) { if (freelist_of(lane, imgs, false, p1old, &fo, &nfo, &cfo, oldimg) < 0) bad++; else qsort(fo, (size_t)nfo, sizeof *fo, cmp_u32); }
    uint32_t *fn = NULL; int nfn = 0, cfn = 0;                                        // the freelist of the committed state: pages on it hold garbage, whatever their image says
    {
        const uint8_t *p1new = ws_image(lane, imgs, 1);
        bool touched = p1new != NULL;                                              // page 1 holds the head and the count; a trunk page rewritten is a change too (one page freed, one allocated: same count)
        for (int i = 0; i < lane->ws_n && !touched && nfo; i++) { uint32_t pg = lane->ws_pgnos[i]; if (bsearch(&pg, fo, (size_t)nfo, sizeof *fo, cmp_u32)) touched = true; }
        if (!touched) { if (nfo) { fn = malloc((size_t)nfo * sizeof *fn); if (fn) { memcpy(fn, fo, (size_t)nfo * sizeof *fn); nfn = nfo; } } }
        else if (have_p1 && oldimg) { if (freelist_of(lane, imgs, true, p1new ? p1new : p1old, &fn, &nfn, &cfn, oldimg) < 0) bad++; else qsort(fn, (size_t)nfn, sizeof *fn, cmp_u32); }
    }
    // owners of the pages of the committed state that the interior pages written by the commit list (a few passes: an interior page may itself be new)
    ovmap ov = {0};
    if (own) {
        // only the pages the transaction wrote can be asked for an owner: the children of an interior page that are not among them are not recorded (a root page lists hundreds of children and is rewritten by most commits)
        uint32_t wcap = 16; while (wcap < (uint32_t)lane->ws_n * 2) wcap <<= 1;
        uint32_t wsbuf[256], *wset = wcap <= 256 ? wsbuf : malloc((size_t)wcap * sizeof *wset);
        if (wset) {
            memset(wset, 0, (size_t)wcap * sizeof *wset);
            for (int i = 0; i < lane->ws_n; i++) { uint32_t h = (lane->ws_pgnos[i] * 2654435761u) & (wcap - 1); while (wset[h]) h = (h + 1) & (wcap - 1); wset[h] = lane->ws_pgnos[i]; }
        }
        for (int pass = 0; pass < 3; pass++) {
            int unresolved = 0; uint32_t before = ov.n;
            for (int i = 0; i < lane->ws_n; i++) {
                const uint8_t *pg = imgs[i]; if (lane->ws_pgnos[i] == 1 || (pg[0] != 0x05 && pg[0] != 0x02)) continue;
                uint32_t r = owner_new(own, &ov, lane->ws_pgnos[i]); if (!r) { unresolved++; continue; }
                int nc = be16(pg + 3);
                for (int c = -1; c < nc; c++) {
                    uint32_t ch;
                    if (c < 0) ch = be32(pg + 8);
                    else { uint32_t off = (uint32_t)be16(pg + 12 + 2 * c); if (off + 4 > pgsz) continue; ch = be32(pg + off); }
                    if (wset) { uint32_t h = (ch * 2654435761u) & (wcap - 1); while (wset[h] && wset[h] != ch) h = (h + 1) & (wcap - 1); if (!wset[h]) continue; }
                    ov_put(&ov, ch, r);
                }
            }
            if (!unresolved || ov.n == before) break;                                // (nothing left to find, or the last pass found nothing new)
        }
        if (wset != wsbuf) free(wset);
    }
    for (int i = 0; i < lane->ws_n && oldimg; i++) {
        uint32_t pgno = lane->ws_pgnos[i];
        char kn = page_kind(imgs[i], pgno);
        // the same page as the snapshot had it (a page that did not exist yet has no old rows)
        char ko = 'O'; bool have_old = false;
        if (pgno <= lane->dsz_val && mw_store_read(st, pgno, lane->tx.snapshot_epoch, 0, pgsz, oldimg)) have_old = true;
        else if (pgno <= lane->dsz_val) { mw_file *f = lane->file; if (f->real->pMethods->xRead(f->real, oldimg, (int)pgsz, (sqlite3_int64)(pgno - 1) * pgsz) == SQLITE_OK) have_old = true; }
        if (have_old) ko = page_kind(oldimg, pgno);
        if (kn == 'O') opaque++; else if (kn == 'I') index_pages++; else if (kn == 'T') interior++; else if (kn == 'S') schema++;
        bool in_fn = nfn && bsearch(&pgno, fn, (size_t)nfn, sizeof *fn, cmp_u32), in_fo = nfo && bsearch(&pgno, fo, (size_t)nfo, sizeof *fo, cmp_u32);
        if ((kn == 'L' || (own && kn == 'I')) && !in_fn) { if (rows_of(lane, imgs, true, imgs[i], pgno, pgsz, &newr, own ? owner_new(own, &ov, pgno) : 0, cat, false) < 0) bad++; }
        if (have_old && (ko == 'L' || (own && ko == 'I')) && !in_fo) { uint8_t *nxt = malloc(pgsz); uint8_t **nk = nxt ? realloc(keeps, (size_t)(nkeeps + 1) * sizeof *keeps) : NULL;           // (the buffer that was read is kept as it is - the old rows point into it - and the next page is read into a new one: no copy)
          if (nk) { keeps = nk; uint8_t *cp = oldimg; keeps[nkeeps++] = cp; oldimg = nxt; if (rows_of(lane, imgs, false, cp, pgno, pgsz, &oldr, own && own->old_owner ? own->old_owner(own->ctx, pgno) : 0, ocat, false) < 0) bad++; }
          else { free(nxt); bad++; } }
    }
    // pages the transaction freed (on the freelist now, not before) and not rewritten: their old rows are old rows that are gone
    for (int k = 0; k < nfn && oldimg; k++) {
        uint32_t pg = fn[k];
        if (nfo && bsearch(&pg, fo, (size_t)nfo, sizeof *fo, cmp_u32)) continue;                                  // already free
        res->freed = realloc(res->freed, (size_t)(res->nfreed + 1) * sizeof *res->freed); res->freed[res->nfreed++] = pg;      // newly freed (rewritten or not)
        if (ws_image(lane, imgs, pg)) continue;                                                                    // rewritten: handled in the loop above
        if (!mw_rd_snap_page(lane, pg, oldimg)) continue;
        char kk = page_kind(oldimg, pg);
        if (kk == 'L' || (own && kk == 'I')) { uint8_t *cp = malloc(pgsz); if (cp) { memcpy(cp, oldimg, pgsz); keeps = realloc(keeps, (size_t)(nkeeps + 1) * sizeof *keeps); keeps[nkeeps++] = cp; if (rows_of(lane, imgs, false, cp, pg, pgsz, &oldr, own && own->old_owner ? own->old_owner(own->ctx, pg) : 0, ocat, false) < 0) bad++; } }
    }
    // overflow pages written without their cell: an update that rewrites only the tail of a big value leaves the leaf page alone. The owner of an overflow page (the leaf its record is in)
    // comes from the overflow owner map; the rows of that leaf that have overflow chains are taken in both states, and netting finds the one that changed.
    if (own && own->ovfl_owner && oldimg) {
        uint32_t seen[64]; int nseen = 0;
        uint32_t *nch = NULL; int nnch = 0;                                                                          // the overflow pages of the records written by this commit: not orphans
        for (int k = 0; k < newr.n; k++) for (int q = 0; q < newr.a[k].nchain; q++) { nch = realloc(nch, (size_t)(nnch + 1) * sizeof *nch); nch[nnch++] = newr.a[k].chain[q]; }
        qsort(nch, (size_t)nnch, sizeof *nch, cmp_u32);
        for (int i = 0; i < lane->ws_n; i++) {
            uint32_t pg = lane->ws_pgnos[i];
            if (pg == 1 || page_kind(imgs[i], pg) != 'O') continue;
            if ((nfn && bsearch(&pg, fn, (size_t)nfn, sizeof *fn, cmp_u32)) || (nfo && bsearch(&pg, fo, (size_t)nfo, sizeof *fo, cmp_u32))) continue;     // a freelist page
            uint32_t leaf = own->ovfl_owner(own->ctx, pg);
            if (!leaf) { if (!(nnch && bsearch(&pg, nch, (size_t)nnch, sizeof *nch, cmp_u32))) res->unknown_ovfl++; continue; }
            if (ws_image(lane, imgs, leaf)) continue;                                                              // its record is being written: decoded in full above
            bool dup = false; for (int q = 0; q < nseen; q++) if (seen[q] == leaf) dup = true;
            if (dup || nseen == 64) continue;
            seen[nseen++] = leaf;
            if (!mw_rd_snap_page(lane, leaf, oldimg)) continue;
            uint8_t *cp = malloc(pgsz); if (!cp) continue; memcpy(cp, oldimg, pgsz); keeps = realloc(keeps, (size_t)(nkeeps + 1) * sizeof *keeps); keeps[nkeeps++] = cp;
            uint32_t root = own->old_owner ? own->old_owner(own->ctx, leaf) : 0;
            if (rows_of(lane, imgs, false, cp, leaf, pgsz, &oldr, root, ocat, true) < 0) bad++;
            if (rows_of(lane, imgs, true, cp, leaf, pgsz, &newr, root, cat, true) < 0) bad++;
        }
        free(nch);
    }
    free(fo); free(fn); free(p1old); free(oldimg); free(ov.pg); free(ov.root);
    for (int i = 0; i < oldr.n; i++) if (oldr.a[i].bad) bad++;
    for (int i = 0; i < newr.n; i++) if (newr.a[i].bad) bad++;
    qsort(oldr.a, (size_t)oldr.n, sizeof *oldr.a, row_cmp);
    qsort(newr.a, (size_t)newr.n, sizeof *newr.a, row_cmp);
    mw_chg *out = calloc((size_t)oldr.n + (size_t)newr.n + 1, sizeof *out);
    int nout = 0, i = 0, j = 0;
    while (out && (i < oldr.n || j < newr.n)) {
        int c = i >= oldr.n ? 1 : j >= newr.n ? -1 : row_cmp(&oldr.a[i], &newr.a[j]);
        const rd_row *r = c <= 0 ? &oldr.a[i] : &newr.a[j];
        mw_chg *x = &out[nout];
        if (c < 0 || c > 0) {
            x->kind = c < 0 ? 3 : 1; x->tab = r->tab; x->root = r->root; x->rowid = r->rowid;
            if (r->tab && !r->bad) { if (!row_pk(r->tab, r, &x->pk, &x->pklen)) x->pk = NULL; }
            nout++; if (c < 0) i++; else j++;
        } else {
            bool pkc = false; uint64_t *wide = NULL; uint64_t m = cells_differ(newr.a[j].tab, &oldr.a[i], &newr.a[j], &pkc, &wide);
            if (rd_debug() && (m || pkc)) { const rd_row *o = &oldr.a[i], *w = &newr.a[j]; fprintf(stderr, "rowdiff: update root %u rowid %lld mask %llx: old len %u new len %u; old:", r->root, (long long)o->rowid, (unsigned long long)m, o->reclen, w->reclen); for (uint32_t q = 0; q < o->reclen && q < 24; q++) fprintf(stderr, " %02x", o->rec[q]); fprintf(stderr, " | new:"); for (uint32_t q = 0; q < w->reclen && q < 24; q++) fprintf(stderr, " %02x", w->rec[q]); fprintf(stderr, "\n"); }
            if (m || pkc) {
                x->kind = 2; x->tab = newr.a[j].tab; x->root = newr.a[j].root; x->rowid = newr.a[j].rowid; x->changed = m; x->wide = wide; wide = NULL;
                if (x->tab && !newr.a[j].bad) { if (!row_pk(x->tab, &newr.a[j], &x->pk, &x->pklen)) x->pk = NULL; }
                if (pkc && x->tab && !oldr.a[i].bad) { if (!row_pk(x->tab, &oldr.a[i], &x->oldpk, &x->oldpklen)) x->oldpk = NULL; }
                nout++;
            }
            free(wide);
            i++; j++;
        }
    }
    uint64_t t1; { struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts); t1 = (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec; }
    for (int k = 0; k < newr.n; k++) for (int q = 0; q < newr.a[k].nchain; q++) { res->ovupd = realloc(res->ovupd, (size_t)(res->novupd + 1) * sizeof *res->ovupd); res->ovupd[res->novupd++] = (mw_ovupd){ newr.a[k].chain[q], newr.a[k].leaf }; }
    res->chg = out; res->n = out ? nout : 0;
    res->info = (mw_rowdiff_info){ .pages = lane->ws_n, .opaque = opaque, .index_pages = index_pages, .interior = interior, .schema = schema, .undecodable = bad, .ns = t1 - t0 };
    rows_free(&oldr); rows_free(&newr);
    for (int k = 0; k < nkeeps; k++) free(keeps[k]);
    free(keeps);
    return 0;
}

void mw_rd_result_free (mw_rd_result *r) {
    for (int i = 0; i < r->n; i++) { free(r->chg[i].pk); free(r->chg[i].oldpk); free(r->chg[i].wide); }
    free(r->chg); free(r->freed); free(r->ovupd); memset(r, 0, sizeof *r);
}

// the experiment of docs §51: no catalog, rows pooled by rowid, the changes handed to a sink as mw_rowchg
void mw_rowdiff_commit (mw_lane *lane, const uint8_t *const *imgs) {
    mw_rd_result r;
    mw_rowdiff_compute(lane, imgs, NULL, &r);
    if (g_sink && r.chg) {
        mw_rowchg *o = malloc((size_t)(r.n + 1) * sizeof *o);
        for (int i = 0; i < r.n; i++) o[i] = (mw_rowchg){ .kind = r.chg[i].kind, .rowid = r.chg[i].rowid, .changed = (uint32_t)r.chg[i].changed, .root = r.chg[i].root, .ncols = 0 };
        for (int i = 0; i < r.n; i++) if (r.chg[i].changed >> 63) o[i].changed |= 0x80000000u;
        g_sink(g_sink_arg, o, r.n, &r.info); free(o);
    }
    mw_rd_result_free(&r);
}
