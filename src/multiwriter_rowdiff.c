//
//  multiwriter_rowdiff.c
//
//  The net row changes of a write set, read from the page images (docs/design.md, "The rebase"): from the pages a transaction wrote and the pages the same numbers had at its snapshot, the
//  rows of every table b-tree that were inserted, changed or deleted, without triggers or hooks. It decodes SQLite's file format directly: the b-tree page header, the cell pointer array,
//  the cells of table leaves (payload size, rowid, record), the records with their serial types, the overflow chains. The net change over the whole write set makes splits and merges
//  disappear (a row that moved between two written pages is no change); the freelist is walked because a page that the transaction frees is not rewritten (its old rows are only reachable
//  through the freelist) and a page on the freelist holds stale rows that are not rows.
//  Which table a page belongs to is not kept anywhere: it is found when it is needed (only for a transaction that is being rebased): a page that the transaction allocated is listed by an
//  interior page that it wrote (and that is a root, or a child of one it wrote...), a page that existed at the snapshot is found by walking down from the root of every table with a key that
//  the page holds. Only the tables with a rowid are decoded (a database with a WITHOUT ROWID table is not rebased: the catalog says so); the pages of indexes are derived data and are not rows.
//  Whatever cannot be decoded exactly (a page of unknown owner, a record that does not parse, an overflow page written without its cell) makes the result "unsupported": the caller refuses.
//
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <strings.h>
#include "multiwriter_internal.h"
#include "multiwriter_catalog.h"

typedef struct {
    uint32_t root; const mw_tab *tab; int64_t rowid;
    const uint8_t *rec; uint32_t reclen;       // the whole record (for a row with overflow: the local part and the chain, copied into `own`)
    uint8_t *own;
    bool bad;                                  // the record could not be read in full
    uint32_t *chain; int nchain;               // (new state) the overflow pages of the record
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

// The table that owns a page that existed at the snapshot: walk down from the root of every table with a key that the page holds, and see whether the page is on the way.
// (The pages of a table that were in a tree are found; a page that was on the freelist, or an empty one, has no owner.)
typedef struct { uint32_t pg, root; } own_cache;
static uint32_t old_owner (mw_lane *lane, const mw_cat *cat, uint32_t pgno, uint8_t *tmp, uint8_t *tmp2) {
    uint32_t pgsz = (uint32_t)lane->db->store->pgsz;
    if (!mw_rd_snap_page(lane, pgno, tmp)) return 0;
    uint64_t key = 0; int nc = be16(tmp + 3);
    if (!nc) return 0;
    uint32_t off = (uint32_t)be16(tmp + (tmp[0] == 0x05 ? 12 : 8));
    if (off + 4 >= pgsz) return 0;
    if (tmp[0] == 0x0d) { uint64_t P; int a = varint(tmp + off, tmp + pgsz, &P); if (!a || !varint(tmp + off + a, tmp + pgsz, &key)) return 0; }
    else if (tmp[0] == 0x05) { if (!varint(tmp + off + 4, tmp + pgsz, &key)) return 0; }
    else return 0;
    for (int t = 0; t < cat->n; t++) {
        const mw_tab *tab = &cat->tabs[t]; if (!tab->ok || tab->without_rowid || tab->root <= 1) continue;
        uint32_t page = tab->root;
        for (int depth = 0; depth < 24 && page; depth++) {
            if (page == pgno) return tab->root;
            if (!mw_rd_snap_page(lane, page, tmp2) || tmp2[0] != 0x05) break;
            int n2 = be16(tmp2 + 3); uint32_t next = be32(tmp2 + 8);
            for (int c = 0; c < n2; c++) {
                uint32_t co = (uint32_t)be16(tmp2 + 12 + 2 * c); if (co + 5 > pgsz) { next = 0; break; }
                uint64_t k; if (!varint(tmp2 + co + 4, tmp2 + pgsz, &k)) { next = 0; break; }
                if (key <= k) { next = be32(tmp2 + co); break; }
            }
            page = next;
        }
    }
    return 0;
}

// ---- records ----
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

// Adds the rows of one table leaf page (0x0d).
static int rows_of (mw_lane *lane, const uint8_t *const *imgs, bool new_state, const uint8_t *pg, uint32_t pgsz, rowset *rs, uint32_t root, const mw_cat *cat) {
    const mw_tab *tab = mw_cat_by_root(cat, root);
    int nc = be16(pg + 3);
    const uint32_t U = pgsz, M = ((U - 12) * 32 / 255) - 23, X = U - 35;
    if (rs->n + nc > rs->cap) { int ncap = rs->cap ? rs->cap : 64; while (ncap < rs->n + nc) ncap *= 2; rd_row *p = realloc(rs->a, (size_t)ncap * sizeof *p); if (!p) return -1; rs->a = p; rs->cap = ncap; }
    for (int i = 0; i < nc; i++) {
        uint32_t off = (uint32_t)be16(pg + 8 + 2 * i);
        if (off < 8 || off >= pgsz) return -1;
        const uint8_t *c = pg + off, *end = pg + pgsz;
        uint64_t P, rid = 0; int a = varint(c, end, &P); if (!a) return -1;
        int b = varint(c + a, end, &rid); if (!b) return -1;
        uint32_t local = (uint32_t)P, ovfl = 0;
        if (P > X) { uint32_t K = M + (uint32_t)((P - M) % (U - 4)); local = K <= X ? K : M; if (c + a + b + local + 4 > end) return -1; ovfl = be32(c + a + b + local); }
        else if (c + a + b + local > end) return -1;
        rd_row *r = &rs->a[rs->n++]; memset(r, 0, sizeof *r);
        r->root = root; r->tab = tab; r->rowid = (int64_t)rid; r->rec = c + a + b; r->reclen = local;
        if (ovfl) { if (full_payload(lane, imgs, new_state, r->rec, local, P, ovfl, &r->own, new_state ? &r->chain : NULL, &r->nchain)) { r->rec = r->own; r->reclen = (uint32_t)P; } else r->bad = true; }
    }
    return 0;
}

static int row_cmp (const void *a, const void *b) {
    const rd_row *p = a, *q = b;
    if (p->root != q->root) return p->root < q->root ? -1 : 1;
    return p->rowid < q->rowid ? -1 : p->rowid > q->rowid;
}
static void rows_free (rowset *rs) { for (int i = 0; i < rs->n; i++) { free(rs->a[i].own); free(rs->a[i].chain); } free(rs->a); }
static uint8_t *dup_bytes (const uint8_t *p, uint32_t n) { uint8_t *b = malloc(n ? n : 1); if (b) memcpy(b, p, n); return b; }

// Computes the row changes of the write set. The caller frees the result with mw_rd_result_free. res->unsupported: not NULL when the write set holds something that is not decoded exactly.
int mw_rowdiff_compute (mw_lane *lane, const uint8_t *const *imgs, const mw_cat *cat, mw_rd_result *res) {
    mw_store *st = lane->db->store;
    uint32_t pgsz = (uint32_t)st->pgsz;
    memset(res, 0, sizeof *res);
    rowset oldr = {0}, newr = {0};
    uint8_t **keeps = NULL; int nkeeps = 0;                                           // copies of the old pages: the old rows point into them
    const char *why = NULL;
    uint8_t *oldimg = malloc(pgsz), *t1 = malloc(pgsz), *t2 = malloc(pgsz);
    uint32_t *fo = NULL; int nfo = 0, cfo = 0;                                        // the freelist at the snapshot: those pages hold stale rows that are not rows
    uint8_t *p1old = malloc(pgsz);
    bool have_p1 = p1old && oldimg && mw_rd_snap_page(lane, 1, p1old);
    if (!have_p1 || !oldimg || !t1 || !t2) why = "no memory or no page 1";
    if (!why && be32(p1old + 36) > 0) { if (freelist_of(lane, imgs, false, p1old, &fo, &nfo, &cfo, oldimg) < 0) why = "the freelist cannot be read"; else qsort(fo, (size_t)nfo, sizeof *fo, cmp_u32); }
    uint32_t *fn = NULL; int nfn = 0, cfn = 0;                                        // the freelist of the committed state: pages on it hold garbage, whatever their image says
    if (!why) {
        const uint8_t *p1new = ws_image(lane, imgs, 1);
        bool touched = p1new != NULL;
        for (int i = 0; i < lane->ws_n && !touched && nfo; i++) { uint32_t pg = lane->ws_pgnos[i]; if (bsearch(&pg, fo, (size_t)nfo, sizeof *fo, cmp_u32)) touched = true; }
        if (!touched) { if (nfo) { fn = malloc((size_t)nfo * sizeof *fn); if (fn) { memcpy(fn, fo, (size_t)nfo * sizeof *fn); nfn = nfo; } } }
        else if (freelist_of(lane, imgs, true, p1new ? p1new : p1old, &fn, &nfn, &cfn, oldimg) < 0) why = "the freelist cannot be read"; else qsort(fn, (size_t)nfn, sizeof *fn, cmp_u32);
    }
    // owners of the pages of the committed state that the interior pages written by the commit list (a few passes: an interior page may itself be new)
    ovmap ov = {0};
    own_cache oc[64]; int noc = 0;
    #define OLD_OWNER(pgno, out) do { (out) = 0; for (int _q = 0; _q < noc; _q++) if (oc[_q].pg == (pgno)) (out) = oc[_q].root; if (!(out) && !why) { (out) = old_owner(lane, cat, (pgno), t1, t2); if ((out) && noc < 64) oc[noc++] = (own_cache){ (pgno), (out) }; } } while (0)
    #define NEW_OWNER(pgno, out) do { (out) = 0; if (mw_cat_by_root(cat, (pgno))) (out) = (pgno); else { (out) = ov_get(&ov, (pgno)); if (!(out)) OLD_OWNER((pgno), (out)); } } while (0)
    if (!why) {
        for (int pass = 0; pass < 3; pass++) {
            int unresolved = 0; uint32_t before = ov.n;
            for (int i = 0; i < lane->ws_n; i++) {
                const uint8_t *pg = imgs[i]; if (lane->ws_pgnos[i] == 1 || pg[0] != 0x05) continue;
                uint32_t r; NEW_OWNER(lane->ws_pgnos[i], r); if (!r) { unresolved++; continue; }
                int nc = be16(pg + 3);
                for (int c = -1; c < nc; c++) {
                    uint32_t ch;
                    if (c < 0) ch = be32(pg + 8);
                    else { uint32_t off = (uint32_t)be16(pg + 12 + 2 * c); if (off + 4 > pgsz) continue; ch = be32(pg + off); }
                    ov_put(&ov, ch, r);
                }
            }
            if (!unresolved || ov.n == before) break;
        }
    }
    for (int i = 0; i < lane->ws_n && !why; i++) {
        uint32_t pgno = lane->ws_pgnos[i];
        char kn = page_kind(imgs[i], pgno);
        if (kn == 'S' || kn == 'T') continue;
        bool in_fn = nfn && bsearch(&pgno, fn, (size_t)nfn, sizeof *fn, cmp_u32), in_fo = nfo && bsearch(&pgno, fo, (size_t)nfo, sizeof *fo, cmp_u32);
        if (kn == 'L' && !in_fn) {
            uint32_t root; NEW_OWNER(pgno, root);
            if (!root) { why = "a page of a table that the transaction wrote has no known owner"; break; }
            const mw_tab *tab = mw_cat_by_root(cat, root); if (!tab || !tab->ok) { why = "a table that cannot be replayed was written"; break; }
            if (rows_of(lane, imgs, true, imgs[i], pgsz, &newr, root, cat) < 0) { why = "a leaf page does not decode"; break; }
        }
        // the same page as the snapshot had it (a page that did not exist yet has no old rows)
        bool have_old = false;
        if (pgno <= lane->dsz_val && mw_rd_snap_page(lane, pgno, oldimg)) have_old = true;
        if (have_old && page_kind(oldimg, pgno) == 'L' && !in_fo) {
            uint32_t root; OLD_OWNER(pgno, root);
            if (!root) { why = "a page of a table that the transaction wrote has no known owner"; break; }
            const mw_tab *tab = mw_cat_by_root(cat, root); if (!tab || !tab->ok) { why = "a table that cannot be replayed was written"; break; }
            uint8_t *cp = dup_bytes(oldimg, pgsz); uint8_t **nk = cp ? realloc(keeps, (size_t)(nkeeps + 1) * sizeof *keeps) : NULL;       // (the old rows point into the copy)
            if (!nk) { free(cp); why = "no memory"; break; }
            keeps = nk; keeps[nkeeps++] = cp;
            if (rows_of(lane, imgs, false, cp, pgsz, &oldr, root, cat) < 0) { why = "a leaf page does not decode"; break; }
        }
    }
    // pages the transaction freed (on the freelist now, not before) and not rewritten: their old rows are old rows that are gone
    for (int k = 0; k < nfn && !why; k++) {
        uint32_t pg = fn[k];
        if (nfo && bsearch(&pg, fo, (size_t)nfo, sizeof *fo, cmp_u32)) continue;                                  // already free
        if (ws_image(lane, imgs, pg)) continue;                                                                    // rewritten: handled in the loop above
        if (!mw_rd_snap_page(lane, pg, oldimg)) continue;
        if (page_kind(oldimg, pg) != 'L') continue;
        uint32_t root; OLD_OWNER(pg, root);
        if (!root) { why = "a page that the transaction freed has no known owner"; break; }
        const mw_tab *tab = mw_cat_by_root(cat, root); if (!tab || !tab->ok) { why = "a table that cannot be replayed was written"; break; }
        uint8_t *cp = dup_bytes(oldimg, pgsz); uint8_t **nk = cp ? realloc(keeps, (size_t)(nkeeps + 1) * sizeof *keeps) : NULL;
        if (!nk) { free(cp); why = "no memory"; break; }
        keeps = nk; keeps[nkeeps++] = cp;
        if (rows_of(lane, imgs, false, cp, pgsz, &oldr, root, cat) < 0) { why = "a leaf page does not decode"; break; }
    }
    // overflow pages written that no cell of the write set owns: an update that rewrites only the tail of a big value. Their owner is not known: unsupported.
    if (!why) {
        uint32_t *nch = NULL; int nnch = 0;
        for (int k = 0; k < newr.n; k++) for (int q = 0; q < newr.a[k].nchain; q++) { nch = realloc(nch, (size_t)(nnch + 1) * sizeof *nch); nch[nnch++] = newr.a[k].chain[q]; }
        qsort(nch, (size_t)nnch, sizeof *nch, cmp_u32);
        for (int i = 0; i < lane->ws_n && !why; i++) {
            uint32_t pg = lane->ws_pgnos[i];
            if (pg == 1 || page_kind(imgs[i], pg) != 'O') continue;
            if ((nfn && bsearch(&pg, fn, (size_t)nfn, sizeof *fn, cmp_u32)) || (nfo && bsearch(&pg, fo, (size_t)nfo, sizeof *fo, cmp_u32))) continue;     // a freelist page
            if (nnch && bsearch(&pg, nch, (size_t)nnch, sizeof *nch, cmp_u32)) continue;
            why = "an overflow page was written without its cell";
        }
        free(nch);
    }
    free(fo); free(fn); free(p1old); free(oldimg); free(t1); free(t2); free(ov.pg); free(ov.root);
    for (int i = 0; i < oldr.n && !why; i++) if (oldr.a[i].bad) why = "a record does not decode";
    for (int i = 0; i < newr.n && !why; i++) if (newr.a[i].bad) why = "a record does not decode";
    if (why) { res->unsupported = why; rows_free(&oldr); rows_free(&newr); for (int k = 0; k < nkeeps; k++) free(keeps[k]); free(keeps); return 0; }
    if (getenv("MW_RD_DEBUG")) { fprintf(stderr, "ROWDIFF ws:"); for (int i = 0; i < lane->ws_n; i++) fprintf(stderr, " %u%c", lane->ws_pgnos[i], page_kind(imgs[i], lane->ws_pgnos[i])); fprintf(stderr, " | old rows:"); for (int i = 0; i < oldr.n; i++) fprintf(stderr, " %u/%lld", oldr.a[i].root, (long long)oldr.a[i].rowid); fprintf(stderr, " | new rows:"); for (int i = 0; i < newr.n; i++) fprintf(stderr, " %u/%lld", newr.a[i].root, (long long)newr.a[i].rowid); fprintf(stderr, "\n"); }
    qsort(oldr.a, (size_t)oldr.n, sizeof *oldr.a, row_cmp);
    qsort(newr.a, (size_t)newr.n, sizeof *newr.a, row_cmp);
    mw_chg *out = calloc((size_t)oldr.n + (size_t)newr.n + 1, sizeof *out);
    int nout = 0, i = 0, j = 0;
    if (!out) res->unsupported = "no memory";
    while (out && (i < oldr.n || j < newr.n)) {
        int c = i >= oldr.n ? 1 : j >= newr.n ? -1 : row_cmp(&oldr.a[i], &newr.a[j]);
        mw_chg *x = &out[nout];
        if (c < 0) { const rd_row *o = &oldr.a[i++]; x->kind = 3; x->tab = o->tab; x->rowid = o->rowid; x->old_rec = dup_bytes(o->rec, o->reclen); x->old_len = o->reclen; nout++; }
        else if (c > 0) { const rd_row *w = &newr.a[j++]; x->kind = 1; x->tab = w->tab; x->rowid = w->rowid; x->new_rec = dup_bytes(w->rec, w->reclen); x->new_len = w->reclen; nout++; }
        else {
            const rd_row *o = &oldr.a[i++], *w = &newr.a[j++];
            if (o->reclen != w->reclen || memcmp(o->rec, w->rec, o->reclen) != 0) {
                x->kind = 2; x->tab = w->tab; x->rowid = w->rowid;
                x->old_rec = dup_bytes(o->rec, o->reclen); x->old_len = o->reclen; x->new_rec = dup_bytes(w->rec, w->reclen); x->new_len = w->reclen; nout++;
            }
        }
    }
    res->chg = out; res->n = out ? nout : 0;
    rows_free(&oldr); rows_free(&newr);
    for (int k = 0; k < nkeeps; k++) free(keeps[k]);
    free(keeps);
    return 0;
}

void mw_rd_result_free (mw_rd_result *r) {
    for (int i = 0; i < r->n; i++) { free(r->chg[i].old_rec); free(r->chg[i].new_rec); }
    free(r->chg); memset(r, 0, sizeof *r);
}
