//
//  multiwriter_btree.c
//  cloudsync
//
//  Validation of a read of an *interior* b-tree page by what the transaction actually used from it.
//
//  Page-level read validation refuses a transaction whenever a page it read has a newer committed version. For the pages near the root of a busy
//  b-tree that is almost always: every insert reads the root, and any commit that adds a divider to it invalidates every transaction in flight,
//  although none of them depends on the divider that was added. What a descent takes from an interior page is only *which child to follow*: the
//  child whose key interval contains the key. If the newer version of the page still lists every child the transaction went through, with the same
//  divider keys on both sides, then the same key is routed to the same child (the cells are sorted, so the neighbours bound the interval), and the
//  read is still valid. The children themselves are validated as pages of their own.
//
//  This applies to interior pages that the transaction only read (a page it wrote is a write-write conflict). If the transaction used no child of
//  the page (it looked at the cells of the interior page itself: an index entry that lives in an interior cell) or the page changed type, the read
//  is a conflict as before.
//

#include <stdlib.h>
#include <string.h>
#include "multiwriter_internal.h"

static inline uint32_t rd32 (const uint8_t *p) { return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) | ((uint32_t)p[2] << 8) | p[3]; }
static inline uint16_t rd16 (const uint8_t *p) { return (uint16_t)(((uint32_t)p[0] << 8) | p[1]); }

static int rd_varint (const uint8_t *p, const uint8_t *end, uint64_t *out) {
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) {
        if (p + i >= end) return 0;
        v = (v << 7) | (p[i] & 0x7f);
        if (!(p[i] & 0x80)) { *out = v; return i + 1; }
    }
    if (p + 8 >= end) return 0;
    *out = (v << 8) | p[8];
    return 9;
}

typedef struct { uint32_t child; const uint8_t *key; int keylen; } icell;
typedef struct { uint8_t type; int n; uint32_t right; icell *cells; } inode;

// Parses an interior page: the child pointer and the key bytes of every cell (plus the right-most child). False if it is not a well-formed interior page.
static bool parse_interior (const uint8_t *pg, int pgsz, int reserved, inode *out) {
    out->cells = NULL;
    uint8_t type = pg[0];
    if (type != 0x02 && type != 0x05) return false;
    int ncell = rd16(pg + 3);
    if (12 + 2 * ncell > pgsz) return false;
    out->type = type; out->n = ncell; out->right = rd32(pg + 8);
    out->cells = malloc((size_t)(ncell ? ncell : 1) * sizeof(icell));
    if (!out->cells) return false;
    int U = pgsz - reserved;
    int minLocal = (U - 12) * 32 / 255 - 23, maxLocal = (U - 12) * 64 / 255 - 23;
    const uint8_t *end = pg + pgsz;
    for (int i = 0; i < ncell; i++) {
        int off = rd16(pg + 12 + 2 * i);
        if (off < 12 + 2 * ncell || off + 5 > pgsz) { free(out->cells); out->cells = NULL; return false; }
        const uint8_t *cp = pg + off;
        out->cells[i].child = rd32(cp);
        const uint8_t *k = cp + 4;
        uint64_t v = 0;
        int n = rd_varint(k, end, &v);
        if (!n) { free(out->cells); out->cells = NULL; return false; }
        int len = n;                                                   // table interior cell: the rowid varint is the whole key
        if (type == 0x02) {                                            // index interior cell: payload size, then the payload (local part) and maybe an overflow pointer
            int local = (int)v;
            if (v > (uint64_t)maxLocal) { local = minLocal + (int)((v - (uint64_t)minLocal) % (uint64_t)(U - 4)); if (local > maxLocal) local = minLocal; local += 4; }
            len = n + local;
        }
        if (k + len > end) { free(out->cells); out->cells = NULL; return false; }
        out->cells[i].key = k; out->cells[i].keylen = len;
        (void)v;
    }
    return true;
}

static bool same_key (const icell *a, const icell *b) { return a->keylen == b->keylen && memcmp(a->key, b->key, (size_t)a->keylen) == 0; }

// Position of child `c` in the node (cells 0..n-1, right-most = n), or -1.
static int find_child (const inode *nd, uint32_t c) {
    for (int i = 0; i < nd->n; i++) if (nd->cells[i].child == c) return i;
    return nd->right == c ? nd->n : -1;
}

// True if the newer version `nw` of an interior page routes every child of `used` (sorted page numbers) that `old` lists exactly as `old` did.
bool mw_interior_routes_same (const uint8_t *old, const uint8_t *nw, int pgsz, int reserved, const uint32_t *used, int nused) {
    inode a, b;
    if (!parse_interior(old, pgsz, reserved, &a)) return false;
    if (!parse_interior(nw, pgsz, reserved, &b)) { free(a.cells); return false; }
    bool ok = a.type == b.type, any = false;
    for (int i = 0; ok && i <= a.n; i++) {
        uint32_t c = i < a.n ? a.cells[i].child : a.right;
        int lo = 0, hi = nused - 1, hit = 0;                          // is this child one the transaction went through?
        while (lo <= hi) { int mid = (lo + hi) / 2; if (used[mid] == c) { hit = 1; break; } if (used[mid] < c) lo = mid + 1; else hi = mid - 1; }
        if (!hit) continue;
        any = true;
        int j = find_child(&b, c);
        if (j < 0) { ok = false; break; }
        // the interval of the child is bounded by the key of the previous cell and its own key (the right-most child: no upper bound)
        bool a_has_lo = i > 0, b_has_lo = j > 0, a_has_hi = i < a.n, b_has_hi = j < b.n;
        if (a_has_lo != b_has_lo || a_has_hi != b_has_hi) { ok = false; break; }
        if (a_has_lo && !same_key(&a.cells[i - 1], &b.cells[j - 1])) { ok = false; break; }
        if (a_has_hi && !same_key(&a.cells[i], &b.cells[j])) { ok = false; break; }
    }
    free(a.cells); free(b.cells);
    return ok && any;
}

// ---------------------------------------------------------------------------------------------------------------------------------------------------
// Three-way merge of an interior *table* b-tree page that two transactions rewrote from the same base.
//
// A leaf split adds a divider to the parent: cell (child, rowid key) list changes by "child A now ends at k1, new child A' ends at k" (or a new right-most
// child). Two transactions that split different children of the same parent both rewrite the parent, and page-level validation refuses the second one, although
// their changes are disjoint. The parent is a sorted list of (child, upper bound) pairs, so the merge is done on that model:
//     - a child's *upper bound* is its cell key (the right-most child has none: +infinity);
//     - a child is changed by a side if its upper bound differs from the base's, and is new if the base does not list it;
//     - the merged page lists every child of `ours`, with `theirs`' upper bound where only `theirs` changed it, plus the children new in `theirs`;
//     - it refuses (returns false) if any child is changed by both sides, if a child of the base is missing on either side, if the merged keys are not strictly
//       increasing with exactly one right-most child, or if the cells do not fit the page.
// The children themselves are pages of their own: a child both transactions modified is a conflict on that page, detected by the normal validation.
// Only pages of the transaction's write set that also have a newer committed version come here. `ours` must already use final page numbers for its new children.
// ---------------------------------------------------------------------------------------------------------------------------------------------------

typedef struct { uint32_t child; int64_t key; bool inf; const uint8_t *kb; int kl; } mcell;

// Parses an interior table page into `n+1` entries (cells, then the right-most child as +infinity). Returns the count or -1.
static int parse_table_interior (const uint8_t *pg, int pgsz, mcell *out, int cap) {
    if (pg[0] != 0x05) return -1;
    int ncell = rd16(pg + 3);
    if (12 + 2 * ncell > pgsz || ncell + 1 > cap) return -1;
    const uint8_t *end = pg + pgsz;
    for (int i = 0; i < ncell; i++) {
        int off = rd16(pg + 12 + 2 * i);
        if (off < 12 + 2 * ncell || off + 5 > pgsz) return -1;
        const uint8_t *cp = pg + off;
        uint64_t v = 0;
        int n = rd_varint(cp + 4, end, &v);
        if (!n) return -1;
        out[i].child = rd32(cp); out[i].key = (int64_t)v; out[i].inf = false; out[i].kb = cp + 4; out[i].kl = n;
    }
    out[ncell].child = rd32(pg + 8); out[ncell].key = 0; out[ncell].inf = true; out[ncell].kb = NULL; out[ncell].kl = 0;
    return ncell + 1;
}

// child -> index into an mcell array (open addressing, sized for the array)
typedef struct { uint32_t *key; int *idx; uint32_t cap; } chmap;
static bool chmap_build (chmap *m, const mcell *c, int n) {
    uint32_t cap = 64; while (cap < (uint32_t)n * 2) cap *= 2;
    m->key = calloc(cap, sizeof(uint32_t)); m->idx = malloc(cap * sizeof(int)); m->cap = cap;
    if (!m->key || !m->idx) return false;
    for (int i = 0; i < n; i++) {
        uint32_t k = (c[i].child * 2654435761u) & (cap - 1);
        while (m->key[k]) { if (m->key[k] == c[i].child) return false; k = (k + 1) & (cap - 1); }    // (a child listed twice: malformed)
        m->key[k] = c[i].child; m->idx[k] = i;
    }
    return true;
}
static int chmap_find (const chmap *m, uint32_t child) {
    uint32_t k = (child * 2654435761u) & (m->cap - 1);
    while (m->key[k]) { if (m->key[k] == child) return m->idx[k]; k = (k + 1) & (m->cap - 1); }
    return -1;
}
static void chmap_free (chmap *m) { free(m->key); free(m->idx); m->key = NULL; m->idx = NULL; }

static bool same_hi (const mcell *a, const mcell *b) { return a->inf == b->inf && (a->inf || (a->kl == b->kl && memcmp(a->kb, b->kb, (size_t)a->kl) == 0)); }
static int cmp_mcell (const void *pa, const void *pb) {
    const mcell *a = pa, *b = pb;
    if (a->inf != b->inf) return a->inf ? 1 : -1;
    return a->key < b->key ? -1 : a->key > b->key;
}

bool mw_interior_merge (const uint8_t *base, const uint8_t *ours, const uint8_t *theirs, int pgsz, int reserved, uint8_t *out) {
    int cap = (pgsz - 12) / 6 + 2;                                    // (a cell is at least 4 + 1 bytes and a pointer of 2)
    mcell *B = malloc(3 * (size_t)cap * sizeof(mcell)), *O = B ? B + cap : NULL, *H = B ? B + 2 * cap : NULL;
    mcell *M = malloc(2 * (size_t)cap * sizeof(mcell));
    chmap mb = {0}, mh = {0};
    bool ok = B && M;
    int nb = 0, no = 0, nh = 0, nm = 0;
    if (ok) { nb = parse_table_interior(base, pgsz, B, cap); no = parse_table_interior(ours, pgsz, O, cap); nh = parse_table_interior(theirs, pgsz, H, cap); ok = nb > 0 && no > 0 && nh > 0; }
    if (ok) ok = chmap_build(&mb, B, nb) && chmap_build(&mh, H, nh);
    chmap mo = {0};
    if (ok) ok = chmap_build(&mo, O, no);
    // every child of the base must still be listed on both sides
    for (int i = 0; ok && i < nb; i++) if (chmap_find(&mo, B[i].child) < 0 || chmap_find(&mh, B[i].child) < 0) ok = false;
    // ours, with theirs' change applied where only theirs changed the child
    for (int j = 0; ok && j < no; j++) {
        int ib = chmap_find(&mb, O[j].child), ih = chmap_find(&mh, O[j].child);
        mcell c = O[j];
        if (ib >= 0) {
            bool changed_o = !same_hi(&O[j], &B[ib]);
            bool changed_h = ih >= 0 && !same_hi(&H[ih], &B[ib]);
            if (changed_o && changed_h) ok = false;                   // both moved the same child's upper bound
            else if (changed_h) c = H[ih];
        } else if (ih >= 0) ok = false;                               // a child new in ours cannot exist in theirs
        M[nm++] = c;
    }
    // children new in theirs
    for (int j = 0; ok && j < nh; j++) if (chmap_find(&mb, H[j].child) < 0) { if (chmap_find(&mo, H[j].child) >= 0) ok = false; else M[nm++] = H[j]; }
    if (ok) {
        qsort(M, (size_t)nm, sizeof(mcell), cmp_mcell);
        int inf = 0;
        for (int i = 0; i < nm; i++) {
            if (M[i].inf) inf++;
            if (i > 0 && !M[i].inf && cmp_mcell(&M[i - 1], &M[i]) >= 0) ok = false;         // keys strictly increasing
        }
        if (inf != 1 || !M[nm - 1].inf) ok = false;                   // exactly one right-most child, and it is the last
    }
    if (ok) {
        // serialise: header, cell pointers, cells packed from the end of the usable area
        int usable = pgsz - reserved, ncell = nm - 1, total = 0;
        for (int i = 0; i < ncell; i++) total += 4 + M[i].kl;
        if (12 + 2 * ncell + total > usable) ok = false;               // does not fit: the parent would have to split
        else {
            memset(out, 0, (size_t)pgsz);
            out[0] = 0x05; out[3] = (uint8_t)(ncell >> 8); out[4] = (uint8_t)ncell;
            int content = usable - total;
            out[5] = (uint8_t)((content & 0xffff) >> 8); out[6] = (uint8_t)content;        // (65536 is stored as 0)
            uint32_t r = M[nm - 1].child; out[8] = (uint8_t)(r >> 24); out[9] = (uint8_t)(r >> 16); out[10] = (uint8_t)(r >> 8); out[11] = (uint8_t)r;
            int pos = usable;
            for (int i = 0; i < ncell; i++) {
                int sz = 4 + M[i].kl; pos -= sz;
                uint32_t ch = M[i].child; out[pos] = (uint8_t)(ch >> 24); out[pos + 1] = (uint8_t)(ch >> 16); out[pos + 2] = (uint8_t)(ch >> 8); out[pos + 3] = (uint8_t)ch;
                memcpy(out + pos + 4, M[i].kb, (size_t)M[i].kl);
                out[12 + 2 * i] = (uint8_t)(pos >> 8); out[13 + 2 * i] = (uint8_t)pos;
            }
        }
    }
    chmap_free(&mb); chmap_free(&mh); chmap_free(&mo);
    free(M); free(B);
    return ok;
}
