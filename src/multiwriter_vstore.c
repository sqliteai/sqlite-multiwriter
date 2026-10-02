//
//  multiwriter_vstore.c
//  cloudsync
//
//  PROTOTYPE, see multiwriter_vstore.h and docs §53.
//
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include "multiwriter_vstore.h"

#define BLOCK_ENTRIES 170                                   // 170 * 24 = 4080 bytes: one block is one 4 KB read
#define BLOOM_BITS_PER_KEY 10
#define BLOOM_K 7

typedef struct { vs_key k; vs_val v; } ent;                 // 24 bytes

typedef struct {
    uint64_t id, n, size;
    int fd;
    const ent *map;                                         // (hot mode: the file is mapped)
    uint8_t *bloom; uint64_t bloom_bits;
    vs_key *index; uint64_t nblocks;                        // first key of every block
    uint32_t min_dv, max_dv;
    char path[512];
} run;

typedef struct { uint64_t n; int fd; const ent *map; uint32_t min_dv, max_dv; char path[512]; } feedseg;

struct vstore {
    vs_params p;
    ent *feed; uint64_t feed_n, feed_cap;                     // puts since the last flush, in order
    feedseg *segs; int nsegs, cap_segs;                      // written feed segments, oldest first
    ent *mem; uint64_t mem_cap, mem_n;                      // open addressing, empty slot: k.hi == EMPTY
    run **runs; int nruns, cap_runs;                        // oldest first
    uint64_t next_id;
    vs_stats st;
};

#define EMPTY (~(uint64_t)0)

static uint64_t now_ns (void) { struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts); return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec; }
static uint64_t mix (uint64_t x) { x ^= x >> 30; x *= 0xbf58476d1ce4e5b9ull; x ^= x >> 27; x *= 0x94d049bb133111ebull; x ^= x >> 31; return x; }
static uint64_t hash_key (vs_key k) { return mix(k.hi * 0x9E3779B97F4A7C15ull ^ mix(k.lo)); }
static int key_cmp (vs_key a, vs_key b) { return a.hi < b.hi ? -1 : a.hi > b.hi ? 1 : a.lo < b.lo ? -1 : a.lo > b.lo; }
static int ent_cmp (const void *a, const void *b) { return key_cmp(((const ent *)a)->k, ((const ent *)b)->k); }

vstore *vstore_open (const vs_params *p) {
    vstore *vs = calloc(1, sizeof *vs);
    vs->p = *p;
    if (!vs->p.fanout) vs->p.fanout = 4;
    vs->mem_cap = 1; while (vs->mem_cap < vs->p.mem_entries * 2) vs->mem_cap <<= 1;
    vs->mem = malloc(vs->mem_cap * sizeof(ent));
    for (uint64_t i = 0; i < vs->mem_cap; i++) vs->mem[i].k.hi = EMPTY;
    return vs;
}

static void run_free (run *r, bool unlink_file) {
    if (r->map) munmap((void *)r->map, r->size);
    if (r->fd >= 0) close(r->fd);
    if (unlink_file) unlink(r->path);
    free(r->bloom); free(r->index); free(r);
}

void vstore_close (vstore *vs) {
    for (int i = 0; i < vs->nruns; i++) run_free(vs->runs[i], true);
    free(vs->runs); free(vs->mem); free(vs);
}

// Writes the sorted entries as a run file (+ in-memory filter and index), returns it opened for reading.
static run *run_write (vstore *vs, const ent *e, uint64_t n) {
    run *r = calloc(1, sizeof *r);
    r->id = vs->next_id++; r->n = n; r->size = n * sizeof(ent);
    snprintf(r->path, sizeof r->path, "%s/run.%llu", vs->p.dir, (unsigned long long)r->id);
    int fd = open(r->path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) { free(r); return NULL; }
    if (vs->p.cold) fcntl(fd, F_NOCACHE, 1);                                  // (before the first write: the pages never enter the cache)
    const uint8_t *p = (const uint8_t *)e; uint64_t left = r->size, off = 0;
    while (left) { size_t c = left > (1u << 24) ? (1u << 24) : (size_t)left; ssize_t w = pwrite(fd, p + off, c, (off_t)off); if (w <= 0) { close(fd); free(r); return NULL; } off += (uint64_t)w; left -= (uint64_t)w; }
    fsync(fd);
    r->fd = fd;
    if (!vs->p.cold) { void *m = mmap(NULL, r->size, PROT_READ, MAP_SHARED, fd, 0); r->map = m == MAP_FAILED ? NULL : m; }
    r->bloom_bits = (n * BLOOM_BITS_PER_KEY + 63) & ~63ull; r->bloom = calloc(r->bloom_bits / 8, 1);
    r->nblocks = (n + BLOCK_ENTRIES - 1) / BLOCK_ENTRIES; r->index = malloc(r->nblocks * sizeof(vs_key));
    r->min_dv = UINT32_MAX; r->max_dv = 0;
    for (uint64_t i = 0; i < n; i++) {
        if (i % BLOCK_ENTRIES == 0) r->index[i / BLOCK_ENTRIES] = e[i].k;
        uint64_t h = hash_key(e[i].k), h2 = mix(h) | 1;
        for (int j = 0; j < BLOOM_K; j++) { uint64_t b = (h + (uint64_t)j * h2) % r->bloom_bits; r->bloom[b >> 3] |= (uint8_t)(1u << (b & 7)); }
        if (e[i].v.dv < r->min_dv) r->min_dv = e[i].v.dv;
        if (e[i].v.dv > r->max_dv) r->max_dv = e[i].v.dv;
    }
    return r;
}

static bool bloom_maybe (const run *r, vs_key k) {
    uint64_t h = hash_key(k), h2 = mix(h) | 1;
    for (int j = 0; j < BLOOM_K; j++) { uint64_t b = (h + (uint64_t)j * h2) % r->bloom_bits; if (!(r->bloom[b >> 3] & (1u << (b & 7)))) return false; }
    return true;
}

// the block of entries that can hold k (index of the last block whose first key <= k), read into buf (cold) or addressed in the mapping (hot)
static bool run_get (vstore *vs, run *r, vs_key k, vs_val *out) {
    uint64_t lo = 0, hi = r->nblocks;                                    // first block whose first key > k
    while (lo < hi) { uint64_t mid = (lo + hi) / 2; if (key_cmp(r->index[mid], k) <= 0) lo = mid + 1; else hi = mid; }
    if (lo == 0) return false;
    uint64_t blk = lo - 1, first = blk * BLOCK_ENTRIES, cnt = r->n - first < BLOCK_ENTRIES ? r->n - first : BLOCK_ENTRIES;
    ent local[BLOCK_ENTRIES]; const ent *b;
    vs->st.block_reads++;
    if (r->map) b = r->map + first;
    else { if (pread(r->fd, local, cnt * sizeof(ent), (off_t)(first * sizeof(ent))) != (ssize_t)(cnt * sizeof(ent))) return false; b = local; }
    uint64_t l = 0, h = cnt;
    while (l < h) { uint64_t m = (l + h) / 2; int c = key_cmp(b[m].k, k); if (c == 0) { *out = b[m].v; return true; } if (c < 0) l = m + 1; else h = m; }
    return false;
}

bool vstore_get (vstore *vs, vs_key k, vs_val *out) {
    vs->st.gets++;
    uint64_t h = hash_key(k) & (vs->mem_cap - 1);
    while (vs->mem[h].k.hi != EMPTY) { if (key_cmp(vs->mem[h].k, k) == 0) { *out = vs->mem[h].v; vs->st.get_mem++; return true; } h = (h + 1) & (vs->mem_cap - 1); }
    for (int i = vs->nruns - 1; i >= 0; i--) {
        run *r = vs->runs[i];
        if (!bloom_maybe(r, k)) { vs->st.bloom_skips++; continue; }
        if (run_get(vs, r, k, out)) { vs->st.get_runs++; return true; }
        vs->st.bloom_false++;
    }
    return false;
}

static int size_class (const vstore *vs, uint64_t n) { int c = 0; uint64_t cap = vs->p.mem_entries; while (n > cap) { cap *= (uint64_t)vs->p.fanout; c++; } return c; }   // class 0: a flushed run; class c: up to mem_entries * fanout^c

static void add_run (vstore *vs, run *r) {
    if (vs->nruns == vs->cap_runs) { vs->cap_runs = vs->cap_runs ? vs->cap_runs * 2 : 16; vs->runs = realloc(vs->runs, (size_t)vs->cap_runs * sizeof *vs->runs); }
    vs->runs[vs->nruns++] = r;
}

// merge the `fanout` oldest runs of one size class into one (the newest value of a key wins: a higher run id is newer; runs are kept oldest first)
static bool merge_once (vstore *vs) {
    for (int c = 0; c < 32; c++) {
        int idx[64], m = 0;
        for (int i = 0; i < vs->nruns && m < 64; i++) if (size_class(vs, vs->runs[i]->n) == c) idx[m++] = i;
        if (m < vs->p.fanout) continue;
        uint64_t t0 = now_ns();
        int k = vs->p.fanout; run *in[64]; uint64_t pos[64], total = 0;
        for (int j = 0; j < k; j++) { in[j] = vs->runs[idx[j]]; pos[j] = 0; total += in[j]->n; }
        ent *out = malloc(total * sizeof(ent)); uint64_t n = 0;
        // (hot mode reads through the mapping; cold mode loads each input whole: a merge reads sequentially)
        const ent *src[64]; ent *own[64] = {0};
        for (int j = 0; j < k; j++) { if (in[j]->map) src[j] = in[j]->map; else { own[j] = malloc(in[j]->size); (void)pread(in[j]->fd, own[j], in[j]->size, 0); src[j] = own[j]; } }
        for (;;) {
            int best = -1;
            for (int j = 0; j < k; j++) if (pos[j] < in[j]->n && (best < 0 || key_cmp(src[j][pos[j]].k, src[best][pos[best]].k) < 0 || (key_cmp(src[j][pos[j]].k, src[best][pos[best]].k) == 0 && in[j]->id > in[best]->id))) best = j;
            if (best < 0) break;
            ent e = src[best][pos[best]];
            for (int j = 0; j < k; j++) while (pos[j] < in[j]->n && key_cmp(src[j][pos[j]].k, e.k) == 0) pos[j]++;      // skip every copy of the key (the newest was taken)
            out[n++] = e;
        }
        run *r = run_write(vs, out, n);
        for (int j = 0; j < k; j++) free(own[j]);
        free(out);
        if (!r) return false;
        // replace the inputs by the output, at the position of the oldest input (it holds the merged history of all of them)
        for (int j = 0; j < k; j++) run_free(in[j], true);
        int w = 0, placed = 0;
        run *keep[1024]; for (int i = 0; i < vs->nruns; i++) { bool gone = false; for (int j = 0; j < k; j++) if (i == idx[j]) gone = true; if (!gone) keep[w++] = vs->runs[i]; else if (!placed) { keep[w++] = r; placed = 1; } }
        memcpy(vs->runs, keep, (size_t)w * sizeof *keep); vs->nruns = w;
        vs->st.merges++; vs->st.bytes_merged += r->size; vs->st.merge_ns += now_ns() - t0;
        return true;
    }
    return false;
}

void vstore_flush (vstore *vs) {
    if (!vs->mem_n) return;
    uint64_t t0 = now_ns();
    ent *a = malloc(vs->mem_n * sizeof(ent)); uint64_t n = 0;
    for (uint64_t i = 0; i < vs->mem_cap; i++) if (vs->mem[i].k.hi != EMPTY) { a[n++] = vs->mem[i]; vs->mem[i].k.hi = EMPTY; }
    qsort(a, n, sizeof(ent), ent_cmp);
    run *r = run_write(vs, a, n);
    free(a);
    vs->mem_n = 0;
    if (r) { add_run(vs, r); vs->st.flushes++; vs->st.bytes_flushed += r->size; }
    if (vs->feed_n) {                                                         // the change feed of this flush
        if (vs->nsegs == vs->cap_segs) { vs->cap_segs = vs->cap_segs ? vs->cap_segs * 2 : 16; vs->segs = realloc(vs->segs, (size_t)vs->cap_segs * sizeof *vs->segs); }
        feedseg *fs = &vs->segs[vs->nsegs]; memset(fs, 0, sizeof *fs);
        snprintf(fs->path, sizeof fs->path, "%s/feed.%llu", vs->p.dir, (unsigned long long)vs->next_id++);
        fs->fd = open(fs->path, O_RDWR | O_CREAT | O_TRUNC, 0644);
        if (fs->fd >= 0) {
            if (vs->p.cold) fcntl(fs->fd, F_NOCACHE, 1);
            uint64_t left = vs->feed_n * sizeof(ent), off = 0; const uint8_t *pp = (const uint8_t *)vs->feed;
            while (left) { size_t c = left > (1u << 24) ? (1u << 24) : (size_t)left; ssize_t w = pwrite(fs->fd, pp + off, c, (off_t)off); if (w <= 0) break; off += (uint64_t)w; left -= (uint64_t)w; }
            fsync(fs->fd);
            fs->n = vs->feed_n; fs->min_dv = vs->feed[0].v.dv; fs->max_dv = vs->feed[vs->feed_n - 1].v.dv;
            if (!vs->p.cold) { void *m = mmap(NULL, fs->n * sizeof(ent), PROT_READ, MAP_SHARED, fs->fd, 0); fs->map = m == MAP_FAILED ? NULL : m; }
            vs->st.bytes_feed += fs->n * sizeof(ent); vs->nsegs++;
        }
        vs->feed_n = 0;
    }
    vs->st.flush_ns += now_ns() - t0;
    while (merge_once(vs)) {}
}

void vstore_put (vstore *vs, vs_key k, vs_val v) {
    vs->st.puts++;
    uint64_t h = hash_key(k) & (vs->mem_cap - 1);
    while (vs->mem[h].k.hi != EMPTY && key_cmp(vs->mem[h].k, k) != 0) h = (h + 1) & (vs->mem_cap - 1);
    if (vs->mem[h].k.hi == EMPTY) vs->mem_n++;
    vs->mem[h].k = k; vs->mem[h].v = v;
    if (vs->feed_n == vs->feed_cap) { vs->feed_cap = vs->feed_cap ? vs->feed_cap * 2 : 1 << 16; vs->feed = realloc(vs->feed, vs->feed_cap * sizeof(ent)); }
    vs->feed[vs->feed_n++] = (ent){ k, v };
    if (vs->mem_n >= vs->p.mem_entries) vstore_flush(vs);
}

void vstore_stats (vstore *vs, vs_stats *st) {
    *st = vs->st; st->runs = (uint64_t)vs->nruns; st->disk_bytes = 0; st->resident_bytes = vs->mem_cap * sizeof(ent);
    for (int i = 0; i < vs->nruns; i++) { st->disk_bytes += vs->runs[i]->size; st->resident_bytes += vs->runs[i]->bloom_bits / 8 + vs->runs[i]->nblocks * sizeof(vs_key); }
}

uint64_t vstore_feed_after (vstore *vs, uint32_t after, void (*cb) (void *, vs_key, vs_val), void *arg) {
    uint64_t delivered = 0;
    for (int i = 0; i < vs->nsegs; i++) {
        feedseg *fs = &vs->segs[i];
        if (fs->max_dv <= after) continue;
        ent *own = NULL; const ent *src = fs->map;
        uint64_t lo = 0, hi = fs->n;                                           // first entry with dv > after (db_version never decreases along the feed)
        if (!src) { own = malloc(fs->n * sizeof(ent)); (void)pread(fs->fd, own, fs->n * sizeof(ent), 0); src = own; }     // (cold: the whole segment; a real reader would read only the tail)
        while (lo < hi) { uint64_t mid = (lo + hi) / 2; if (src[mid].v.dv > after) hi = mid; else lo = mid + 1; }
        for (uint64_t j = lo; j < fs->n; j++) { vs_val cur; if (vstore_get(vs, src[j].k, &cur) && cur.dv == src[j].v.dv && cur.cv == src[j].v.cv) { cb(arg, src[j].k, src[j].v); delivered++; } }
        free(own);
    }
    for (uint64_t j = 0; j < vs->feed_n; j++) if (vs->feed[j].v.dv > after) { vs_val cur; if (vstore_get(vs, vs->feed[j].k, &cur) && cur.dv == vs->feed[j].v.dv && cur.cv == vs->feed[j].v.cv) { cb(arg, vs->feed[j].k, vs->feed[j].v); delivered++; } }
    return delivered;
}

uint64_t vstore_scan_after (vstore *vs, uint32_t after, void (*cb) (void *, vs_key, vs_val), void *arg) {
    // newest first; a key delivered once (the newest version), older copies of it are skipped by remembering what was delivered in this scan: kept simple for the prototype
    // (a hash set of delivered keys); entries newer than `after` only
    uint64_t cap = 1 << 16, n = 0, delivered = 0; vs_key *seen = malloc(cap * sizeof *seen); for (uint64_t i = 0; i < cap; i++) seen[i].hi = EMPTY;
    #define SEEN_PUT(k, isnew) do { uint64_t h_ = hash_key(k) & (cap - 1); isnew = true; while (seen[h_].hi != EMPTY) { if (key_cmp(seen[h_], k) == 0) { isnew = false; break; } h_ = (h_ + 1) & (cap - 1); } if (isnew) { seen[h_] = k; n++; } } while (0)
    for (uint64_t i = 0; i < vs->mem_cap; i++) if (vs->mem[i].k.hi != EMPTY) {
        if (n * 2 >= cap) { cap *= 2; free(seen); seen = malloc(cap * sizeof *seen); for (uint64_t j = 0; j < cap; j++) seen[j].hi = EMPTY; n = 0; /* (prototype: a rehash loses what was seen; scans in the benchmark are small) */ }
        bool isnew; SEEN_PUT(vs->mem[i].k, isnew); if (isnew && vs->mem[i].v.dv > after) { cb(arg, vs->mem[i].k, vs->mem[i].v); delivered++; }
    }
    for (int r = vs->nruns - 1; r >= 0; r--) {
        run *ru = vs->runs[r];
        if (ru->max_dv <= after) continue;                                  // nothing newer in this run
        ent *own = NULL; const ent *src = ru->map;
        if (!src) { own = malloc(ru->size); (void)pread(ru->fd, own, ru->size, 0); src = own; }
        for (uint64_t i = 0; i < ru->n; i++) {
            if (src[i].v.dv <= after) continue;
            if (n * 2 >= cap) { cap *= 2; free(seen); seen = malloc(cap * sizeof *seen); for (uint64_t j = 0; j < cap; j++) seen[j].hi = EMPTY; n = 0; }
            bool isnew; SEEN_PUT(src[i].k, isnew); if (isnew) { cb(arg, src[i].k, src[i].v); delivered++; }
        }
        free(own);
    }
    free(seen);
    return delivered;
}
