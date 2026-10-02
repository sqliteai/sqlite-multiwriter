//
//  multiwriter_rowdiff.c
//  cloudsync
//
//  EXPERIMENT (docs §51): can the row-level changes of a transaction be derived in the VFS, from nothing but the page images it wrote and the images the same
//  pages had at its snapshot? No triggers, no update hook, no pre-update hook, no sqlite-sync tables. This file decodes SQLite's b-tree pages (file format:
//  header at offset 0, or 100 on page 1; cell pointer array; table-leaf cells = payload size, rowid, record, overflow pointer) and computes the *net* change of
//  the table rows over the whole write set: a row that moved from one page to another by a split is no change; a row only in the new images is an insert, only
//  in the old ones a delete, in both with a different record an update (with the columns that differ). Index pages are not needed (they follow from the table),
//  interior pages are only used to count. Nothing here is used by the engine unless a sink is registered (tests, measurements).
//
//  Known limits of this prototype, all measured by the test: the rows of different tables are pooled (the table of a page is not known: rowids of different
//  tables must not collide in the experiment; docs §51 describes how the owner of a page would be tracked), a value that lives in an overflow page can only be
//  compared through the cell's local part and its overflow pointer, WITHOUT ROWID tables are index-type b-trees and are not decoded.
//

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include "multiwriter_internal.h"

static mw_rowdiff_sink_fn g_sink;

static void *g_sink_arg;
void mw_rowdiff_set_sink (mw_rowdiff_sink_fn fn, void *arg) { g_sink_arg = arg; g_sink = fn; }
bool mw_rowdiff_enabled (void) { return g_sink != NULL; }

typedef struct {
    int64_t rowid;
    const uint8_t *rec;            // record bytes available in the cell (the local part)
    uint32_t reclen;               // local bytes
    uint32_t total;                // payload size of the whole record
    uint32_t ovfl;                 // first overflow page (0 = none)
    uint32_t root;                 // the table (root page) the row belongs to, 0 if unknown / not asked
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

static int rows_of (const uint8_t *pg, uint32_t pgsz, rd_row **arr, int *n, int *cap, uint32_t root) {
    int nc = be16(pg + 3);
    const uint32_t U = pgsz, X = U - 35, M = ((U - 12) * 32 / 255) - 23;
    for (int i = 0; i < nc; i++) {
        uint32_t off = (uint32_t)be16(pg + 8 + 2 * i);
        if (off < 8 || off >= pgsz) return -1;
        const uint8_t *c = pg + off, *end = pg + pgsz;
        uint64_t P, rid;
        int a = varint(c, end, &P); if (!a) return -1;
        int b = varint(c + a, end, &rid); if (!b) return -1;
        uint32_t local = (uint32_t)P, ovfl = 0;
        if (P > X) { uint32_t K = M + (uint32_t)((P - M) % (U - 4)); local = K <= X ? K : M; if (c + a + b + local + 4 > end) return -1; ovfl = be32(c + a + b + local); }
        else if (c + a + b + local > end) return -1;
        if (*n == *cap) { *cap = *cap ? *cap * 2 : 64; rd_row *p = realloc(*arr, (size_t)*cap * sizeof **arr); if (!p) return -1; *arr = p; }
        (*arr)[(*n)++] = (rd_row){ .rowid = (int64_t)rid, .rec = c + a + b, .reclen = local, .total = (uint32_t)P, .ovfl = ovfl, .root = root };
    }
    return 0;
}

static int row_cmp (const void *a, const void *b) {
    const rd_row *p = a, *q = b;
    if (p->root != q->root) return p->root < q->root ? -1 : 1;
    return p->rowid < q->rowid ? -1 : p->rowid > q->rowid;
}

// Column spans of a record's local part: types[] and offsets[]; returns the number of columns, -1 if malformed
static int rec_cols (const rd_row *r, uint64_t *ty, uint32_t *off, uint32_t *len, int max) {
    const uint8_t *p = r->rec, *end = r->rec + r->reclen;
    uint64_t hs;
    int h = varint(p, end, &hs); if (!h || hs > r->reclen) return -1;
    uint32_t pos = (uint32_t)hs, hp = (uint32_t)h; int n = 0;
    while (hp < hs && n < max) {
        uint64_t t; int k = varint(p + hp, p + hs, &t); if (!k) return -1; hp += (uint32_t)k;
        uint32_t l = t < 12 ? ((const uint8_t[]){0, 1, 2, 3, 4, 6, 8, 8, 0, 0, 0, 0})[t] : (uint32_t)((t - 12) / 2);
        if (t == 8 || t == 9) l = 0;
        ty[n] = t; off[n] = pos; len[n] = l; pos += l; n++;
    }
    return n;
}

// 0 = identical, else a bitmask of the differing columns (bit 31: more columns than the mask holds / data beyond the local part)
static uint32_t rows_differ (const rd_row *a, const rd_row *b) {
    if (a->total == b->total && a->reclen == b->reclen && a->ovfl == b->ovfl && memcmp(a->rec, b->rec, a->reclen) == 0) return 0;
    uint64_t ta[32], tb[32]; uint32_t oa[32], ob[32], la[32], lb[32];
    int na = rec_cols(a, ta, oa, la, 32), nb = rec_cols(b, tb, ob, lb, 32);
    if (na < 0 || nb < 0) return 0x80000000u;
    uint32_t mask = 0;
    int n = na > nb ? na : nb;
    for (int i = 0; i < n && i < 31; i++) {
        if (i >= na || i >= nb) { mask |= 1u << i; continue; }
        bool in_a = oa[i] + la[i] <= a->reclen, in_b = ob[i] + lb[i] <= b->reclen;
        if (!in_a || !in_b) { mask |= 1u << i; continue; }                    // (beyond the local part: only the overflow pointer says something; treat as changed)
        if (ta[i] != tb[i] || (la[i] && memcmp(a->rec + oa[i], b->rec + ob[i], la[i]) != 0)) mask |= 1u << i;
    }
    if (n > 31) mask |= 0x80000000u;
    return mask ? mask : 0x80000000u;                                          // (cells differ but no visible column does: the hidden part changed)
}

// Page `pgno` at the transaction's snapshot into `dst`; false if it does not exist there.
bool mw_rd_snap_page (mw_lane *lane, uint32_t pgno, uint8_t *dst) {
    mw_store *st = lane->db->store;
    uint32_t pgsz = (uint32_t)st->pgsz;
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
// A page that is freed by the transaction is not rewritten: its old rows are only reachable through the freelist.
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
static uint32_t owner_new (const mw_rd_owner *own, const ovmap *ov, uint32_t pg) { uint32_t r = ov_get(ov, pg); return r ? r : (own && own->old_owner ? own->old_owner(own->ctx, pg) : 0); }

// Computes the row changes of the commit. `own` (optional) gives the table of every page as it was before the commit; the owners of the pages the commit allocates are taken
// from the interior pages it wrote. The caller frees res->chg and res->freed.
int mw_rowdiff_compute (mw_lane *lane, const uint8_t *const *imgs, const mw_rd_owner *own, mw_rd_result *res) {
    mw_store *st = lane->db->store;
    uint32_t pgsz = (uint32_t)st->pgsz;
    uint64_t t0 = 0; { struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts); t0 = (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec; }
    memset(res, 0, sizeof *res);
    rd_row *oldr = NULL, *newr = NULL; int no = 0, nn = 0, co = 0, cn = 0;
    int opaque = 0, index_pages = 0, interior = 0, schema = 0, bad = 0;
    uint8_t *oldimg = malloc(pgsz);
    uint8_t **freed_keeps = NULL; int nfreed = 0;
    uint32_t *fo = NULL; int nfo = 0, cfo = 0;                                        // the freelist at the snapshot: those pages hold stale rows that are not rows
    uint8_t *p1old = malloc(pgsz);
    bool have_p1 = p1old && mw_rd_snap_page(lane, 1, p1old);
    uint8_t **keeps = calloc((size_t)(lane->ws_n ? lane->ws_n : 1), sizeof *keeps);        // (copies of the old leaf pages: the old rows point into them)
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
    if (own) for (int pass = 0; pass < 3; pass++) for (int i = 0; i < lane->ws_n; i++) {
        const uint8_t *pg = imgs[i]; if (lane->ws_pgnos[i] == 1 || pg[0] != 0x05) continue;
        uint32_t r = owner_new(own, &ov, lane->ws_pgnos[i]); if (!r) continue;
        int nc = be16(pg + 3);
        ov_put(&ov, be32(pg + 8), r);
        for (int c = 0; c < nc; c++) { uint32_t off = (uint32_t)be16(pg + 12 + 2 * c); if (off + 4 <= pgsz) ov_put(&ov, be32(pg + off), r); }
    }
    for (int i = 0; i < lane->ws_n && oldimg && keeps; i++) {
        uint32_t pgno = lane->ws_pgnos[i];
        char kn = page_kind(imgs[i], pgno);
        // the same page as the snapshot had it (a page that did not exist yet has no old rows)
        char ko = 'O'; bool have_old = false;
        if (pgno <= lane->dsz_val && mw_store_read(st, pgno, lane->tx.snapshot_epoch, 0, pgsz, oldimg)) have_old = true;
        else if (pgno <= lane->dsz_val) { mw_file *f = lane->file; if (f->real->pMethods->xRead(f->real, oldimg, (int)pgsz, (sqlite3_int64)(pgno - 1) * pgsz) == SQLITE_OK) have_old = true; }
        if (have_old) ko = page_kind(oldimg, pgno);
        if (kn == 'O') opaque++; else if (kn == 'I') index_pages++; else if (kn == 'T') interior++; else if (kn == 'S') schema++;
        if (kn == 'L' && !(nfn && bsearch(&pgno, fn, (size_t)nfn, sizeof *fn, cmp_u32))) { uint8_t *copy = (uint8_t *)imgs[i]; if (rows_of(copy, pgsz, &newr, &nn, &cn, own ? owner_new(own, &ov, pgno) : 0) < 0) bad++; }
        if (have_old && ko == 'L' && !(nfo && bsearch(&pgno, fo, (size_t)nfo, sizeof *fo, cmp_u32))) {
            uint8_t *keep = malloc(pgsz);
            if (keep) { memcpy(keep, oldimg, pgsz); keeps[i] = keep; if (rows_of(keep, pgsz, &oldr, &no, &co, own && own->old_owner ? own->old_owner(own->ctx, pgno) : 0) < 0) bad++; }
        }
    }
    // pages the transaction freed (on the freelist now, not before) and not rewritten: their old leaf rows are old rows that are gone
    for (int k = 0; k < nfn && oldimg; k++) {
        uint32_t pg = fn[k];
        if (nfo && bsearch(&pg, fo, (size_t)nfo, sizeof *fo, cmp_u32)) continue;                                  // already free
        res->freed = realloc(res->freed, (size_t)(res->nfreed + 1) * sizeof *res->freed); res->freed[res->nfreed++] = pg;      // newly freed (rewritten or not)
        if (ws_image(lane, imgs, pg)) continue;                                                                    // rewritten: handled in the loop above
        if (!mw_rd_snap_page(lane, pg, oldimg) || page_kind(oldimg, pg) != 'L') continue;
        uint8_t *keep = malloc(pgsz);
        if (keep) { memcpy(keep, oldimg, pgsz); freed_keeps = realloc(freed_keeps, (size_t)(nfreed + 1) * sizeof *freed_keeps); freed_keeps[nfreed++] = keep; if (rows_of(keep, pgsz, &oldr, &no, &co, own && own->old_owner ? own->old_owner(own->ctx, pg) : 0) < 0) bad++; }
    }
    free(fo); free(fn); free(p1old); free(oldimg); free(ov.pg); free(ov.root);
    qsort(oldr, (size_t)no, sizeof *oldr, row_cmp);
    qsort(newr, (size_t)nn, sizeof *newr, row_cmp);
    mw_rowchg *out = malloc(((size_t)no + (size_t)nn + 1) * sizeof *out);
    int nout = 0, i = 0, j = 0;
    uint64_t ty[32]; uint32_t of[32], le[32];
    while (out && (i < no || j < nn)) {
        int c = i >= no ? 1 : j >= nn ? -1 : row_cmp(&oldr[i], &newr[j]);
        if (c < 0) { int nc = rec_cols(&oldr[i], ty, of, le, 32); out[nout++] = (mw_rowchg){ .kind = 3, .rowid = oldr[i].rowid, .root = oldr[i].root, .ncols = nc < 0 ? 0 : nc }; i++; }
        else if (c > 0) { int nc = rec_cols(&newr[j], ty, of, le, 32); out[nout++] = (mw_rowchg){ .kind = 1, .rowid = newr[j].rowid, .root = newr[j].root, .ncols = nc < 0 ? 0 : nc }; j++; }
        else { uint32_t m = rows_differ(&oldr[i], &newr[j]); if (m) { int nc = rec_cols(&newr[j], ty, of, le, 32); out[nout++] = (mw_rowchg){ .kind = 2, .rowid = newr[j].rowid, .changed = m, .root = newr[j].root, .ncols = nc < 0 ? 0 : nc }; } i++; j++; }
    }
    uint64_t t1; { struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts); t1 = (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec; }
    res->chg = out; res->n = out ? nout : 0;
    res->info = (mw_rowdiff_info){ .pages = lane->ws_n, .opaque = opaque, .index_pages = index_pages, .interior = interior, .schema = schema, .undecodable = bad, .ns = t1 - t0 };
    for (int k = 0; k < lane->ws_n; k++) free(keeps[k]);
    for (int k = 0; k < nfreed; k++) free(freed_keeps[k]);
    free(freed_keeps); free(keeps); free(oldr); free(newr);
    return 0;
}

void mw_rowdiff_commit (mw_lane *lane, const uint8_t *const *imgs) {
    mw_rd_result r;
    mw_rowdiff_compute(lane, imgs, NULL, &r);
    if (g_sink && r.chg) g_sink(g_sink_arg, r.chg, r.n, &r.info);
    free(r.chg); free(r.freed);
}
