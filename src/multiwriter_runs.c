//
//  multiwriter_runs.c
//  Sorted runs of packed rows (see multiwriter_runs.h).
//
#include <stdlib.h>
#include <string.h>
#include "multiwriter_runs.h"
#include "lz4.h"

// ---- varints ----
static size_t put_var (uint8_t *p, uint64_t v) { size_t n = 0; while (v >= 0x80) { p[n++] = (uint8_t)(v | 0x80); v >>= 7; } p[n++] = (uint8_t)v; return n; }
static bool get_var (const uint8_t **p, const uint8_t *end, uint64_t *v) {
    uint64_t r = 0; int sh = 0;
    while (*p < end && sh < 64) { uint8_t b = *(*p)++; r |= (uint64_t)(b & 0x7f) << sh; if (!(b & 0x80)) { *v = r; return true; } sh += 7; }
    return false;
}
static uint32_t rd32 (const uint8_t *p) { uint32_t v; memcpy(&v, p, 4); return v; }
static uint64_t rd64 (const uint8_t *p) { uint64_t v; memcpy(&v, p, 8); return v; }

// ---- keys ----
int rs_key_cmp (const rs_key *a, const rs_key *b) {
    if (a->tbl != b->tbl) return a->tbl < b->tbl ? -1 : 1;
    uint32_t m = a->pklen < b->pklen ? a->pklen : b->pklen;
    int c = m ? memcmp(a->pk, b->pk, m) : 0;
    if (c) return c < 0 ? -1 : 1;
    return a->pklen < b->pklen ? -1 : a->pklen > b->pklen;
}
uint64_t rs_key_hash (const rs_key *k) {
    uint64_t h = 1469598103934665603ull ^ k->tbl; h *= 1099511628211ull;
    for (uint32_t i = 0; i < k->pklen; i++) { h ^= k->pk[i]; h *= 1099511628211ull; }
    h ^= h >> 32; h *= 0x9E3779B97F4A7C15ull; h ^= h >> 29;
    return h;
}

// ---- Bloom ----
#define BLOOM_K 7
#define BLOOM_BITS_PER_ROW 10
static uint64_t bloom_pos (uint64_t nbits, uint64_t h1, uint64_t h2, int i) { uint64_t h = h1 + (uint64_t)i * h2; return (uint64_t)(((__uint128_t)h * nbits) >> 64); }
static void bloom_add (uint64_t *bits, uint64_t nbits, uint64_t h) {
    uint64_t h1 = h, h2 = (h >> 17) | (h << 47) | 1;
    for (int i = 0; i < BLOOM_K; i++) { uint64_t p = bloom_pos(nbits, h1, h2, i); bits[p >> 6] |= 1ull << (p & 63); }
}
static bool bloom_test (const uint64_t *bits, uint64_t nbits, uint64_t h) {
    uint64_t h1 = h, h2 = (h >> 17) | (h << 47) | 1;
    for (int i = 0; i < BLOOM_K; i++) { uint64_t p = bloom_pos(nbits, h1, h2, i); if (!(bits[p >> 6] & (1ull << (p & 63)))) return false; }
    return true;
}

// ---- a block ----
bool rs_blk_unpack (rs_blk *b, const uint8_t *data, size_t len, uint8_t **owned) {
    *owned = NULL;
    if (len < 1) return false;
    if (data[0] == 1 || data[0] == 3) return rs_blk_open(b, data, len);
    if (data[0] != 2 || len < 6) return false;
    uint32_t raw = rd32(data + 1);
    if (raw < 5 || raw > (64u << 20)) return false;
    uint8_t *buf = malloc(raw); if (!buf) return false;
    int n = LZ4_decompress_safe((const char *)data + 5, (char *)buf, (int)(len - 5), (int)raw);
    if (n != (int)raw || !rs_blk_open(b, buf, raw)) { free(buf); return false; }
    *owned = buf;
    return true;
}
bool rs_blk_open (rs_blk *b, const uint8_t *data, size_t len) {
    b->kbuf = b->kinl; b->kcap = sizeof b->kinl; b->klen = 0; b->ktbl = 0; b->kdv = 0; b->cur = -1; b->next_off = 0; b->ver = 0; b->nrest = 0; b->rows_at = 0;
    if (len < 5 || (data[0] != 1 && data[0] != 3)) return false;
    uint32_t n = rd32(data + 1);
    if (data[0] == 1) {
        if ((uint64_t)n * 4 + 5 > len) return false;
        b->data = data; b->len = len; b->nrows = n; b->ver = 1;
        return true;
    }
    uint64_t nr = ((uint64_t)n + RS_RESTART - 1) / RS_RESTART;
    if (n == 0 || 5 + nr * 4 > len) return false;
    b->data = data; b->len = len; b->nrows = n; b->ver = 3; b->nrest = (uint32_t)nr; b->rows_at = 5 + (size_t)nr * 4;
    return true;
}
void rs_blk_close (rs_blk *b) { if (b->kbuf && b->kbuf != b->kinl) free(b->kbuf); b->kbuf = b->kinl; b->kcap = sizeof b->kinl; }
// decodes the row at *off (form 3) into the state of the block; `restart`: it is a restart row (whole key, dv as it is)
static bool blk_next (rs_blk *b, size_t *off, bool restart) {
    const uint8_t *p = b->data + *off, *end = b->data + b->len;
    uint64_t h, un, d, cl, t;
    if (*off < b->rows_at || *off >= b->len || !get_var(&p, end, &h)) return false;
    uint64_t shared = h >> 1; bool newtbl = h & 1;
    if (restart && (shared || !newtbl)) return false;
    if (newtbl) { if (!get_var(&p, end, &t)) return false; b->ktbl = (uint32_t)t; } else if (b->cur < 0 && !restart) return false;
    if (!get_var(&p, end, &un) || shared > b->klen || un > (uint64_t)(end - p) || shared + un > (1u << 30)) return false;
    uint64_t nl = shared + un;
    if (nl > b->kcap) { uint32_t nc = (uint32_t)(nl * 2 + 64); uint8_t *nb = malloc(nc); if (!nb) return false; memcpy(nb, b->kbuf, b->klen); if (b->kbuf != b->kinl) free(b->kbuf); b->kbuf = nb; b->kcap = nc; }
    memcpy(b->kbuf + shared, p, (size_t)un); p += un; b->klen = (uint32_t)nl;
    if (!get_var(&p, end, &d)) return false;
    if (restart) b->kdv = (int64_t)d; else b->kdv += (int64_t)(d >> 1) ^ -(int64_t)(d & 1);
    if (!get_var(&p, end, &cl) || cl > (uint64_t)(end - p)) return false;
    b->kcells = p; b->knc = (uint32_t)cl; p += cl;
    *off = (size_t)(p - b->data);
    return true;
}
bool rs_blk_row (rs_blk *b, uint32_t i, rs_key *k, int64_t *dv, const uint8_t **cells, uint32_t *ncells) {
    if (i >= b->nrows) return false;
    if (b->ver == 1) {
        uint32_t off = rd32(b->data + 5 + (size_t)i * 4);
        if (off < 5 + (size_t)b->nrows * 4 || off >= b->len) return false;
        const uint8_t *p = b->data + off, *end = b->data + b->len;
        uint64_t tbl, pkl, d, cl;
        if (!get_var(&p, end, &tbl) || !get_var(&p, end, &pkl) || pkl > (uint64_t)(end - p)) return false;
        const uint8_t *pk = p; p += pkl;
        if (!get_var(&p, end, &d) || !get_var(&p, end, &cl) || cl > (uint64_t)(end - p)) return false;
        k->tbl = (uint32_t)tbl; k->pk = pk; k->pklen = (uint32_t)pkl; *dv = (int64_t)d; *cells = p; *ncells = (uint32_t)cl;
        return true;
    }
    if (b->cur != (int64_t)i) {
        if (b->cur >= 0 && (int64_t)i == b->cur + 1) {                                  // the next row: where the last one ended
            if (!blk_next(b, &b->next_off, i % RS_RESTART == 0)) { b->cur = -1; return false; }
            b->cur = (int64_t)i;
        } else {                                                                         // from the restart row before it
            uint32_t r = i / RS_RESTART; uint32_t off32 = rd32(b->data + 5 + (size_t)r * 4); size_t off = off32;
            b->cur = -1; b->klen = 0;
            if (!blk_next(b, &off, true)) return false;
            b->cur = (int64_t)r * RS_RESTART; b->next_off = off;
            while (b->cur < (int64_t)i) { if (!blk_next(b, &b->next_off, false)) { b->cur = -1; return false; } b->cur++; }
        }
    }
    k->tbl = b->ktbl; k->pk = b->kbuf; k->pklen = b->klen; *dv = b->kdv; *cells = b->kcells; *ncells = b->knc;
    return true;
}
int rs_blk_find (rs_blk *b, const rs_key *k, int64_t *dv, const uint8_t **cells, uint32_t *ncells) {
    uint32_t lo = 0, hi = b->nrows;
    if (b->ver == 3) {                                                                   // the last restart row that is not after the key, then the rows up to the next one
        uint32_t a = 0, z = b->nrest;                                                  // restarts [a, z): number of restart keys <= k
        while (a < z) {
            uint32_t mid = a + (z - a) / 2; rs_key x; int64_t d; const uint8_t *c; uint32_t nc;
            if (!rs_blk_row(b, mid * RS_RESTART, &x, &d, &c, &nc)) return -1;
            if (rs_key_cmp(&x, k) <= 0) a = mid + 1; else z = mid;
        }
        if (a == 0) return 0;
        lo = (a - 1) * RS_RESTART; hi = lo + RS_RESTART < b->nrows ? lo + RS_RESTART : b->nrows;
        for (uint32_t i = lo; i < hi; i++) {
            rs_key x; int64_t d; const uint8_t *c; uint32_t nc;
            if (!rs_blk_row(b, i, &x, &d, &c, &nc)) return -1;
            int r = rs_key_cmp(&x, k);
            if (r == 0) { *dv = d; *cells = c; *ncells = nc; return 1; }
            if (r > 0) return 0;
        }
        return 0;
    }
    while (lo < hi) {
        uint32_t mid = lo + (hi - lo) / 2; rs_key x; int64_t d; const uint8_t *c; uint32_t nc;
        if (!rs_blk_row(b, mid, &x, &d, &c, &nc)) return -1;
        int r = rs_key_cmp(&x, k);
        if (r == 0) { *dv = d; *cells = c; *ncells = nc; return 1; }
        if (r < 0) lo = mid + 1; else hi = mid;
    }
    return 0;
}

// ---- a run ----
rs_run *rs_run_decode (int64_t id, int64_t age, int lvl, uint64_t nrows, uint32_t nblk, int64_t dvmax, const uint8_t *meta, size_t len) {
    const uint8_t *p = meta, *end = meta + len; uint64_t v, nf;
    if (len < 2 || *p++ != 2 || !get_var(&p, end, &nf) || nf == 0 || nf > (1u << 26) || nf != nblk) return NULL;
    rs_run *r = calloc(1, sizeof *r); if (!r) return NULL;
    r->id = id; r->age = age; r->lvl = lvl; r->nrows = nrows; r->nblk = nblk; r->dvmax = dvmax; r->nfence = (uint32_t)nf; atomic_init(&r->refs, 1);
    r->ftbl = malloc(nf * 4); r->foff = malloc(nf * 4); r->flen = malloc(nf * 4); r->fdv = malloc(nf * 8); r->farena = malloc((size_t)(end - p) + 1);
    r->slen = malloc(nf * 4); r->loff = malloc(nf * 4); r->llen = malloc(nf * 4); r->larena = malloc((size_t)(end - p) + 1);
    if (!r->ftbl || !r->foff || !r->flen || !r->fdv || !r->farena || !r->slen || !r->loff || !r->llen || !r->larena) goto bad;
    size_t fa = 0, la = 0;
    for (uint64_t i = 0; i < nf; i++) {
        uint64_t t, l, d, sl, ll;
        if (!get_var(&p, end, &t) || !get_var(&p, end, &l) || l > (uint64_t)(end - p)) goto bad;
        memcpy(r->farena + fa, p, l); p += l;
        if (!get_var(&p, end, &d) || !get_var(&p, end, &sl) || !get_var(&p, end, &ll) || ll > (uint64_t)(end - p) || sl > (1u << 30)) goto bad;
        memcpy(r->larena + la, p, ll); p += ll; r->slen[i] = (uint32_t)sl; r->loff[i] = (uint32_t)la; r->llen[i] = (uint32_t)ll; la += ll;
        r->ftbl[i] = (uint32_t)t; r->foff[i] = (uint32_t)fa; r->flen[i] = (uint32_t)l; r->fdv[i] = (int64_t)d; fa += l;
    }
    uint64_t kt, kl;
    if (!get_var(&p, end, &kt) || !get_var(&p, end, &kl) || kl > (uint64_t)(end - p)) goto bad;
    r->kmaxk = malloc(kl ? kl : 1); if (!r->kmaxk) goto bad;
    memcpy(r->kmaxk, p, kl); p += kl; r->kmaxtbl = (uint32_t)kt; r->kmaxl = (uint32_t)kl;
    if (!get_var(&p, end, &v) || v == 0 || (v & 63) || v / 8 > (uint64_t)(end - p) || v > (1ull << 40)) goto bad;
    r->nbits = v; r->bloom = malloc((size_t)(v / 8));
    if (!r->bloom) goto bad;
    memcpy(r->bloom, p, (size_t)(v / 8));
    (void)rd64;
    return r;
bad:
    rs_run_unref(r);
    return NULL;
}
void rs_run_ref (rs_run *r) { atomic_fetch_add(&r->refs, 1); }
void rs_run_unref (rs_run *r) {
    if (!r || atomic_fetch_sub(&r->refs, 1) != 1) return;
    free(r->mloc); free(r->ftbl); free(r->foff); free(r->flen); free(r->fdv); free(r->farena); free(r->slen); free(r->loff); free(r->llen); free(r->larena); free(r->kmaxk); free(r->bloom); free(r);
}
void rs_run_block_info (const rs_run *r, uint32_t blk, uint32_t *stored_len, const uint8_t **loc, uint32_t *loclen) { *stored_len = r->slen[blk]; *loc = r->larena + r->loff[blk]; *loclen = r->llen[blk]; }
static rs_key fence_key (const rs_run *r, uint32_t i) { return (rs_key){ r->ftbl[i], r->farena + r->foff[i], r->flen[i] }; }
bool rs_run_range (const rs_run *r, rs_key *lo, rs_key *hi) { *lo = fence_key(r, 0); *hi = (rs_key){ r->kmaxtbl, r->kmaxk, r->kmaxl }; return true; }
int rs_run_block_of (const rs_run *r, const rs_key *k) {
    uint32_t lo = 0, hi = r->nfence;                                          // the number of fences <= k
    while (lo < hi) { uint32_t mid = lo + (hi - lo) / 2; rs_key f = fence_key(r, mid); if (rs_key_cmp(&f, k) <= 0) lo = mid + 1; else hi = mid; }
    return (int)lo - 1;
}
bool rs_run_maybe (const rs_run *r, const rs_key *k) {
    rs_key lo, hi; rs_run_range(r, &lo, &hi);
    if (rs_key_cmp(k, &lo) < 0 || rs_key_cmp(k, &hi) > 0) return false;
    return bloom_test(r->bloom, r->nbits, rs_key_hash(k));
}

// ---- building ----
struct rs_builder {
    rs_emit_fn emit; void *ctx;
    uint8_t *rows; size_t nbytes, cap;                 // the rows of the block being built
    uint32_t *offs; uint32_t noffs, capoffs;
    uint8_t *first; size_t firstcap; uint32_t firsttbl, firstl; int64_t bdv;
    uint32_t blkno; uint64_t total; int64_t dvmax;
    uint32_t nf, capf; uint32_t *ftbl, *foff, *flen; int64_t *fdv; uint8_t *farena; size_t farn, farcap;
    uint32_t *slen, *loff, *llen; uint8_t *larena; size_t larn, larcap;
    uint8_t *last; size_t lastcap; uint32_t lasttbl, lastl; int64_t prevdv;
    uint64_t *bloom; uint64_t nbits;
    bool defer; uint8_t **dblk; uint32_t *dlen, ndblk, capdblk;      // deferred: the finished blocks are kept (their places are given when the run is finished)
};
rs_builder *rs_builder_new_deferred (uint64_t nrows_hint) {
    rs_builder *b = rs_builder_new(nrows_hint, NULL, NULL); if (b) b->defer = true; return b;
}
rs_builder *rs_builder_new (uint64_t nrows_hint, rs_emit_fn emit, void *ctx) {
    rs_builder *b = calloc(1, sizeof *b); if (!b) return NULL;
    b->emit = emit; b->ctx = ctx;
    uint64_t bits = nrows_hint * BLOOM_BITS_PER_ROW; if (bits < 1024) bits = 1024; bits = (bits + 63) & ~63ull;
    b->nbits = bits; b->bloom = calloc((size_t)(bits / 64), 8);
    if (!b->bloom) { free(b); return NULL; }
    return b;
}
void rs_builder_free (rs_builder *b) {
    if (!b) return;
    for (uint32_t i = 0; i < b->ndblk; i++) free(b->dblk[i]);
    free(b->dblk); free(b->dlen);
    free(b->rows); free(b->offs); free(b->first); free(b->ftbl); free(b->foff); free(b->flen); free(b->fdv); free(b->farena); free(b->slen); free(b->loff); free(b->llen); free(b->larena); free(b->last); free(b->bloom); free(b);
}
uint64_t rs_builder_rows (const rs_builder *b) { return b->total; }
static int flush_block (rs_builder *b) {
    if (!b->noffs) return 0;
    uint32_t n = b->noffs, nr = (n + RS_RESTART - 1) / RS_RESTART;
    size_t base = 5 + (size_t)nr * 4, len = base + b->nbytes;
    uint8_t *buf = malloc(len); if (!buf) return -1;
    buf[0] = 3; memcpy(buf + 1, &n, 4);
    for (uint32_t i = 0; i < nr; i++) { uint32_t o = b->offs[i] + (uint32_t)base; memcpy(buf + 5 + (size_t)i * 4, &o, 4); }
    memcpy(buf + base, b->rows, b->nbytes);
    static int nocomp = -1; if (nocomp < 0) nocomp = getenv("MW_META_NOCOMPRESS") != NULL;
    const uint8_t *stored = buf; size_t slen = len; uint8_t *cmp = NULL;
    if (!nocomp && len >= 512 && len < (1u << 26)) {                                 // compressed when that gains a tenth or more
        int bound = LZ4_compressBound((int)len); cmp = malloc((size_t)bound + 5);
        if (cmp) {
            static int accel = -1; if (accel < 0) { const char *e = getenv("MW_META_LZ4_ACCEL"); accel = e && atoi(e) > 0 ? atoi(e) : 1; }
            int cl = LZ4_compress_fast((const char *)buf, (char *)cmp + 5, (int)len, bound, accel);
            if (cl > 0 && (size_t)cl + 5 <= len - len / 10) { cmp[0] = 2; uint32_t rl = (uint32_t)len; memcpy(cmp + 1, &rl, 4); stored = cmp; slen = (size_t)cl + 5; }
        }
    }
    const uint8_t *loc = NULL; size_t loclen = 0;
    int rc;
    if (b->defer) {
        rc = 0;
        if (b->ndblk == b->capdblk) { uint32_t nc = b->capdblk ? b->capdblk * 2 : 16; uint8_t **nd = realloc(b->dblk, nc * sizeof *nd); uint32_t *nl = realloc(b->dlen, nc * sizeof *nl); if (nd) b->dblk = nd; if (nl) b->dlen = nl; if (!nd || !nl) rc = -1; else b->capdblk = nc; }
        if (!rc) { uint8_t *copy = malloc(slen); if (!copy) rc = -1; else { memcpy(copy, stored, slen); b->dblk[b->ndblk] = copy; b->dlen[b->ndblk] = (uint32_t)slen; b->ndblk++; } }
    } else rc = b->emit(b->ctx, b->blkno, stored, slen, &loc, &loclen);
    if (!rc) {
        if (b->nf == b->capf) {
            uint32_t nc = b->capf ? b->capf * 2 : 64;
            uint32_t *a1 = realloc(b->ftbl, nc * 4), *c1 = realloc(b->foff, nc * 4), *d1 = realloc(b->flen, nc * 4); int64_t *e1 = realloc(b->fdv, nc * 8);
            uint32_t *s1 = realloc(b->slen, nc * 4), *o1 = realloc(b->loff, nc * 4), *l1 = realloc(b->llen, nc * 4);
            if (a1) b->ftbl = a1; if (c1) b->foff = c1; if (d1) b->flen = d1; if (e1) b->fdv = e1; if (s1) b->slen = s1; if (o1) b->loff = o1; if (l1) b->llen = l1;
            if (!a1 || !c1 || !d1 || !e1 || !s1 || !o1 || !l1) rc = -1; else b->capf = nc;
        }
        if (!rc && b->farn + b->firstl > b->farcap) { size_t nc = (b->farn + b->firstl) * 2 + 4096; uint8_t *na = realloc(b->farena, nc); if (!na) rc = -1; else { b->farena = na; b->farcap = nc; } }
        if (!rc && b->larn + loclen > b->larcap) { size_t nc = (b->larn + loclen) * 2 + 4096; uint8_t *na = realloc(b->larena, nc); if (!na) rc = -1; else { b->larena = na; b->larcap = nc; } }
        if (!rc) {
            memcpy(b->farena + b->farn, b->first, b->firstl);
            if (loclen) memcpy(b->larena + b->larn, loc, loclen);
            b->ftbl[b->nf] = b->firsttbl; b->foff[b->nf] = (uint32_t)b->farn; b->flen[b->nf] = b->firstl; b->fdv[b->nf] = b->bdv;
            b->slen[b->nf] = (uint32_t)slen; b->loff[b->nf] = (uint32_t)b->larn; b->llen[b->nf] = (uint32_t)loclen;
            b->nf++; b->farn += b->firstl; b->larn += loclen;
            b->blkno++; b->noffs = 0; b->nbytes = 0; b->bdv = 0;
        }
    }
    free(cmp); free(buf);
    return rc;
}
int rs_builder_add (rs_builder *b, const rs_key *k, int64_t dv, const uint8_t *cells, uint32_t ncells) {
    size_t need = b->nbytes + 40 + k->pklen + ncells;
    if (need > b->cap) { size_t nc = need * 2 + 4096; uint8_t *nr = realloc(b->rows, nc); if (!nr) return -1; b->rows = nr; b->cap = nc; }
    bool restart = b->noffs % RS_RESTART == 0;
    if (restart && b->noffs / RS_RESTART == b->capoffs) { uint32_t nc = b->capoffs ? b->capoffs * 2 : 64; uint32_t *no = realloc(b->offs, nc * 4); if (!no) return -1; b->offs = no; b->capoffs = nc; }
    if (!b->noffs) {                                                                   // the first row of a block: its key is the fence
        if (k->pklen > b->firstcap) { size_t nc = k->pklen * 2 + 64; uint8_t *nf = realloc(b->first, nc); if (!nf) return -1; b->first = nf; b->firstcap = nc; }
        memcpy(b->first, k->pk, k->pklen); b->firsttbl = k->tbl; b->firstl = k->pklen; b->bdv = 0;
    }
    uint8_t *w = b->rows + b->nbytes;
    if (restart) b->offs[b->noffs / RS_RESTART] = (uint32_t)b->nbytes;
    b->noffs++;
    bool newtbl = restart || k->tbl != b->lasttbl;
    uint32_t shared = 0; if (!newtbl) { uint32_t m = b->lastl < k->pklen ? b->lastl : k->pklen; while (shared < m && b->last[shared] == k->pk[shared]) shared++; }
    w += put_var(w, ((uint64_t)shared << 1) | (newtbl ? 1u : 0u)); if (newtbl) w += put_var(w, k->tbl);
    w += put_var(w, k->pklen - shared); memcpy(w, k->pk + shared, k->pklen - shared); w += k->pklen - shared;
    if (restart) w += put_var(w, (uint64_t)dv); else { int64_t df = dv - b->prevdv; w += put_var(w, ((uint64_t)df << 1) ^ (uint64_t)(df >> 63)); }
    b->prevdv = dv;
    w += put_var(w, ncells); if (ncells) memcpy(w, cells, ncells); w += ncells;
    b->nbytes = (size_t)(w - b->rows);
    if (dv > b->bdv) b->bdv = dv;
    if (dv > b->dvmax) b->dvmax = dv;
    if (k->pklen > b->lastcap) { size_t nc = k->pklen * 2 + 64; uint8_t *nl = realloc(b->last, nc); if (!nl) return -1; b->last = nl; b->lastcap = nc; }
    memcpy(b->last, k->pk, k->pklen); b->lasttbl = k->tbl; b->lastl = k->pklen;
    bloom_add(b->bloom, b->nbits, rs_key_hash(k));
    b->total++;
    if (b->nbytes + (size_t)((b->noffs + RS_RESTART - 1) / RS_RESTART) * 4 >= RS_BLOCK_TARGET) return flush_block(b);
    return 0;
}
static int builder_finish (rs_builder *b, bool keep, const uint8_t *const *locs, const uint32_t *llens, uint8_t **meta, size_t *metalen, uint64_t *nrows, uint32_t *nblk, int64_t *dvmax) {
    int rc = flush_block(b);
    if (rc || !b->nf) { if (!keep) rs_builder_free(b); return rc ? rc : -1; }
    size_t lsum = b->larn; if (locs) { lsum = 0; for (uint32_t i = 0; i < b->nf; i++) lsum += llens[i]; }
    size_t cap = 16 + (size_t)b->nf * 40 + b->farn + lsum + b->lastl + 24 + (size_t)(b->nbits / 8);
    uint8_t *m = malloc(cap); if (!m) { if (!keep) rs_builder_free(b); return -1; }
    size_t w = 0; m[w++] = 2; w += put_var(m + w, b->nf);
    for (uint32_t i = 0; i < b->nf; i++) {
        w += put_var(m + w, b->ftbl[i]); w += put_var(m + w, b->flen[i]); memcpy(m + w, b->farena + b->foff[i], b->flen[i]); w += b->flen[i]; w += put_var(m + w, (uint64_t)b->fdv[i]);
        const uint8_t *lp = locs ? locs[i] : b->larena + b->loff[i]; uint32_t ll = locs ? llens[i] : b->llen[i];
        w += put_var(m + w, b->slen[i]); w += put_var(m + w, ll); memcpy(m + w, lp, ll); w += ll;
    }
    w += put_var(m + w, b->lasttbl); w += put_var(m + w, b->lastl); memcpy(m + w, b->last, b->lastl); w += b->lastl;
    w += put_var(m + w, b->nbits); memcpy(m + w, b->bloom, (size_t)(b->nbits / 8)); w += (size_t)(b->nbits / 8);
    *meta = m; *metalen = w; *nrows = b->total; *nblk = b->nf; *dvmax = b->dvmax;
    if (!keep) rs_builder_free(b);
    return 0;
}
int rs_builder_finish (rs_builder *b, uint8_t **meta, size_t *metalen, uint64_t *nrows, uint32_t *nblk, int64_t *dvmax) { return builder_finish(b, false, NULL, NULL, meta, metalen, nrows, nblk, dvmax); }
// Deferred builders: the last block is flushed, then the blocks can be looked at (to be stored somewhere that gives each its place), then the run is finished with those places.
int rs_builder_seal (rs_builder *b) { return flush_block(b); }
uint32_t rs_builder_nblocks (const rs_builder *b) { return b->ndblk; }
const uint8_t *rs_builder_block (const rs_builder *b, uint32_t i, size_t *len) { *len = b->dlen[i]; return b->dblk[i]; }
int rs_builder_finish_locs (rs_builder *b, const uint8_t *const *locs, const uint32_t *llens, uint8_t **meta, size_t *metalen, uint64_t *nrows, uint32_t *nblk, int64_t *dvmax) { return builder_finish(b, true, locs, llens, meta, metalen, nrows, nblk, dvmax); }       // (the builder stays: a transaction that fails is tried again with the same blocks; rs_builder_free)
size_t rs_builder_meta_bound (const rs_builder *b) { return 16 + (size_t)b->nf * 40 + b->farn + b->lastl + 24 + (size_t)(b->nbits / 8) + (size_t)b->nf * 24 + 1024; }

// ---- merging ----
typedef struct { const rs_run *run; uint32_t blk; uint8_t *buf; size_t buflen; uint8_t *raw; rs_blk b; uint32_t i; bool done; rs_key k; uint64_t pfx; int64_t dv; const uint8_t *cells; uint32_t nc; } mit;
static int mit_advance (mit *it, const rs_merge_opts *o) {
    for (;;) {
        if (it->buf && it->i < it->b.nrows) {
            if (!rs_blk_row(&it->b, it->i, &it->k, &it->dv, &it->cells, &it->nc)) return -2;
            { uint64_t p = 0; uint32_t n = it->k.pklen < 8 ? it->k.pklen : 8; for (uint32_t q = 0; q < 8; q++) p = (p << 8) | (q < n ? it->k.pk[q] : 0); it->pfx = p; }
            it->i++; return 0;
        }
        if (it->buf) { rs_blk_close(&it->b); free(it->buf); free(it->raw); it->buf = NULL; it->raw = NULL; it->blk++; }
        if (it->blk >= it->run->nblk) { it->done = true; return 0; }
        int rc = o->read(o->rctx, it->run, it->blk, &it->buf, &it->buflen); if (rc) return rc;
        if (!rs_blk_unpack(&it->b, it->buf, it->buflen, &it->raw)) return -2;
        it->i = 0;
    }
}
// The order of the iterators in the merge: by key (the table, the first 8 bytes of the key as a number, and only then the whole key), the newer run first among equal keys.
static inline int mit_cmp (const mit *x, const mit *y) {
    if (x->k.tbl != y->k.tbl) return x->k.tbl < y->k.tbl ? -1 : 1;
    if (x->pfx != y->pfx) return x->pfx < y->pfx ? -1 : 1;
    return rs_key_cmp(&x->k, &y->k);
}
static inline bool mit_before (const mit *its, int a, int b) { int c = mit_cmp(&its[a], &its[b]); return c ? c < 0 : a < b; }
static void heap_down (const mit *its, int *h, int n, int i) {
    int x = h[i];
    for (;;) { int c = 2 * i + 1; if (c >= n) break; if (c + 1 < n && mit_before(its, h[c + 1], h[c])) c++; if (!mit_before(its, h[c], x)) break; h[i] = h[c]; i = c; }
    h[i] = x;
}
static void heap_up (const mit *its, int *h, int i) {
    int x = h[i];
    while (i > 0) { int p = (i - 1) / 2; if (!mit_before(its, x, h[p])) break; h[i] = h[p]; i = p; }
    h[i] = x;
}
int rs_merge (rs_run *const *runs, int nruns, const rs_merge_opts *o, uint64_t *rows_out) {
    mit *its = calloc((size_t)nruns, sizeof *its); int *heap = malloc((size_t)nruns * sizeof *heap); if (!its || !heap) { free(its); free(heap); return -1; }
    int rc = 0; uint64_t total = 0, out = 0; int hn = 0;
    for (int i = 0; i < nruns; i++) { its[i].run = runs[i]; total += runs[i]->nrows; }
    for (int i = 0; i < nruns && !rc; i++) { rc = mit_advance(&its[i], o); if (!rc && !its[i].done) { heap[hn] = i; heap_up(its, heap, hn); hn++; } }
    rs_builder *b = NULL; uint64_t inpart = 0; bool inside = false;
    while (!rc && hn > 0) {
        int best = heap[0];                                                  // (the smallest key; of equal keys the newest run)
        const mit *w = &its[best];
        bool keep = !(w->nc == 0 && o->drop_deleted);
        if (keep && o->keep && w->nc && !o->keep(o->kctx, &w->k, w->dv, w->cells, w->nc)) keep = false;
        if (keep) {
            if (!b) {
                rs_emit_fn emit; void *ectx; uint64_t hint = o->part_rows && o->part_rows < total ? o->part_rows : total;
                rc = o->begin_part(o->sctx, hint, &emit, &ectx); if (rc) break;
                b = rs_builder_new(hint, emit, ectx); if (!b) { rc = -1; break; }
                inside = true; inpart = 0;
            }
            rc = rs_builder_add(b, &w->k, w->dv, w->cells, w->nc); if (rc) break;
            out++; inpart++;
        }
        // the older runs that hold the same key: skipped (the winner's key is still in its block: it is advanced last)
        hn--; heap[0] = heap[hn]; if (hn) heap_down(its, heap, hn, 0);
        while (!rc && hn > 0 && its[heap[0]].k.tbl == w->k.tbl && its[heap[0]].pfx == w->pfx && rs_key_cmp(&its[heap[0]].k, &w->k) == 0) {
            int j = heap[0]; hn--; heap[0] = heap[hn]; if (hn) heap_down(its, heap, hn, 0);
            rc = mit_advance(&its[j], o); if (!rc && !its[j].done) { heap[hn] = j; heap_up(its, heap, hn); hn++; }
        }
        if (!rc) { rc = mit_advance(&its[best], o); if (!rc && !its[best].done) { heap[hn] = best; heap_up(its, heap, hn); hn++; } }
        if (!rc && b && o->part_rows && inpart >= o->part_rows) {
            uint8_t *meta; size_t ml; uint64_t nr; uint32_t nb; int64_t dvm;
            rc = rs_builder_finish(b, &meta, &ml, &nr, &nb, &dvm); b = NULL; inside = false;
            if (!rc) { rc = o->end_part(o->sctx, meta, ml, nr, nb, dvm); free(meta); }
        }
    }
    if (!rc && b) {
        uint8_t *meta; size_t ml; uint64_t nr; uint32_t nb; int64_t dvm;
        rc = rs_builder_finish(b, &meta, &ml, &nr, &nb, &dvm); b = NULL; inside = false;
        if (!rc) { rc = o->end_part(o->sctx, meta, ml, nr, nb, dvm); free(meta); }
    }
    (void)inside;
    if (b) rs_builder_free(b);
    for (int i = 0; i < nruns; i++) { if (its[i].buf) rs_blk_close(&its[i].b); free(its[i].buf); free(its[i].raw); }
    free(its); free(heap);
    if (rows_out) *rows_out = out;
    return rc;
}
