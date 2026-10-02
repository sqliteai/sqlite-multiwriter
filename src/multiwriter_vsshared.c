//
//  multiwriter_vsshared.c
//  cloudsync
//
//  PROTOTYPE, see multiwriter_vsshared.h and docs §54.
//
#include <fcntl.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include "multiwriter_vsshared.h"

#define BLOCK_ENTRIES 170
#define BLOOM_BITS_PER_KEY 10
#define BLOOM_K 7
#define MAXRUNS 64
#define MAXSEGS 512
#define EMPTY (~(uint64_t)0)
#define SHM_MAGIC 0x56534831ull

typedef struct { _Atomic uint64_t hi, lo, val; } slot;                       // val = cv << 32 | dv; a slot is claimed by storing hi last (release)
typedef struct { vsh_key k; vsh_val v; } ent;
typedef struct { uint64_t id, n; uint32_t min_dv, max_dv, cls; } runinfo;
typedef struct { uint64_t id, n; uint32_t min_dv, max_dv; } seginfo;

enum { MT_FREE = 0, MT_ACTIVE = 1, MT_IMMUTABLE = 2, MT_FLUSHING = 3 };

typedef struct {
    _Atomic uint64_t magic;
    uint64_t mem_entries, mt_cap, feed_cap, mt_off[2], feed_off[2], total;
    _Atomic uint32_t lock, ver;                                              // ver: odd while the manifest or the tables are being switched
    _Atomic uint32_t active, mt_state[2];
    _Atomic uint64_t mt_n[2], feed_n[2], next_id;
    _Atomic uint32_t merge_busy; uint32_t nruns, nsegs;
    runinfo runs[MAXRUNS];                                                   // oldest first (written under the lock, read under the sequence counter)
    seginfo segs[MAXSEGS];
    _Atomic uint64_t pending_flush;                                          // table index + 1 owed a flush by the process that switched it
} shdr;

typedef struct { uint64_t id; int fd; const ent *map; uint64_t n; const uint8_t *meta; uint64_t meta_size; const uint8_t *bloom; uint64_t bloom_bits; const vsh_key *index; uint64_t nblocks; } rhandle;

struct vsh {
    vsh_params p; shdr *sh; size_t shm_len; slot *mt[2]; ent *feed[2];
    rhandle *rh; int nrh, caprh;                                             // this process's open runs
    runinfo cache[MAXRUNS]; int ncache; uint32_t cache_ver;                  // the manifest as of cache_ver
    vsh_stats st; int owed;                                                  // a flush this process owes after unlocking (-1 none)
    bool bg; pthread_t bg_thread; pthread_mutex_t bg_mu; pthread_cond_t bg_cv; _Atomic int bg_mask, bg_stop;
};

static uint64_t now_ns (void) { struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts); return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec; }
static uint64_t mix (uint64_t x) { x ^= x >> 30; x *= 0xbf58476d1ce4e5b9ull; x ^= x >> 27; x *= 0x94d049bb133111ebull; x ^= x >> 31; return x; }
static uint64_t hash_key (vsh_key k) { return mix(k.hi * 0x9E3779B97F4A7C15ull ^ mix(k.lo)); }
static int key_cmp (vsh_key a, vsh_key b) { return a.hi < b.hi ? -1 : a.hi > b.hi ? 1 : a.lo < b.lo ? -1 : a.lo > b.lo; }
static int ent_cmp (const void *a, const void *b) { return key_cmp(((const ent *)a)->k, ((const ent *)b)->k); }
static uint64_t pack (vsh_val v) { return (uint64_t)v.cv << 32 | v.dv; }
static vsh_val unpack (uint64_t x) { return (vsh_val){ (uint32_t)(x >> 32), (uint32_t)x }; }

static void path_of (const vsh *h, const char *kind, uint64_t id, char *out, size_t n) { snprintf(out, n, "%s/%s.%llu", h->p.dir, kind, (unsigned long long)id); }

vsh *vsh_open (const vsh_params *p) {
    vsh *h = calloc(1, sizeof *h); h->p = *p; h->owed = -1;
    char path[600]; snprintf(path, sizeof path, "%s/shared.map", p->dir);
    uint64_t cap = 1; while (cap < p->mem_entries * 2) cap <<= 1;
    uint64_t feed_cap = cap;                                                 // (a table is handed over when its feed is full too)
    size_t hdr = (sizeof(shdr) + 4095) & ~(size_t)4095, mt = cap * sizeof(slot), fd_ = feed_cap * sizeof(ent), total = hdr + 2 * mt + 2 * fd_;
    void *m;
    if (p->anon) { m = mmap(NULL, total, PROT_READ | PROT_WRITE, MAP_ANON | MAP_SHARED, -1, 0); if (m == MAP_FAILED) { free(h); return NULL; } }
    else {
        int fd = open(path, p->create ? (O_RDWR | O_CREAT | O_TRUNC) : O_RDWR, 0644);
        if (fd < 0) { free(h); return NULL; }
        if (p->create && ftruncate(fd, (off_t)total) != 0) { close(fd); free(h); return NULL; }
        struct stat sb; if (fstat(fd, &sb) != 0 || (size_t)sb.st_size < total) { close(fd); free(h); return NULL; }
        m = mmap(NULL, total, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0); close(fd);
        if (m == MAP_FAILED) { free(h); return NULL; }
    }
    h->sh = m; h->shm_len = total;
    if (p->create) {
        shdr *s = h->sh; memset(s, 0, sizeof *s);
        s->mem_entries = p->mem_entries; s->mt_cap = cap; s->feed_cap = feed_cap; s->total = total;
        for (int i = 0; i < 2; i++) { s->mt_off[i] = hdr + (uint64_t)i * mt; s->feed_off[i] = hdr + 2 * mt + (uint64_t)i * fd_; }
        for (int i = 0; i < 2; i++) { slot *t = (slot *)((uint8_t *)m + s->mt_off[i]); for (uint64_t j = 0; j < cap; j++) atomic_store_explicit(&t[j].hi, EMPTY, memory_order_relaxed); }
        atomic_store(&s->active, 0); atomic_store(&s->mt_state[0], MT_ACTIVE); atomic_store(&s->next_id, 1);
        atomic_store_explicit(&s->magic, SHM_MAGIC, memory_order_release);
    } else while (atomic_load_explicit(&h->sh->magic, memory_order_acquire) != SHM_MAGIC) sched_yield();
    for (int i = 0; i < 2; i++) { h->mt[i] = (slot *)((uint8_t *)m + h->sh->mt_off[i]); h->feed[i] = (ent *)((uint8_t *)m + h->sh->feed_off[i]); }
    h->cache_ver = UINT32_MAX;
    return h;
}

static void rh_close (rhandle *r) { if (r->map) munmap((void *)r->map, r->n * sizeof(ent)); if (r->meta) munmap((void *)r->meta, r->meta_size); if (r->fd >= 0) close(r->fd); memset(r, 0, sizeof *r); r->fd = -1; }
static void do_flush (vsh *h, int t);
static void *bg_main (void *arg) {
    vsh *h = arg;
    for (;;) {
        pthread_mutex_lock(&h->bg_mu);
        while (!atomic_load(&h->bg_mask) && !atomic_load(&h->bg_stop)) pthread_cond_wait(&h->bg_cv, &h->bg_mu);
        pthread_mutex_unlock(&h->bg_mu);
        int m = atomic_exchange(&h->bg_mask, 0);
        for (int t = 0; t < 2; t++) if (m & (1 << t)) do_flush(h, t);
        if (atomic_load(&h->bg_stop) && !atomic_load(&h->bg_mask)) break;
    }
    return NULL;
}
void vsh_enable_bg_flush (vsh *h) {
    if (h->bg) return;
    pthread_mutex_init(&h->bg_mu, NULL); pthread_cond_init(&h->bg_cv, NULL); h->bg = true;
    pthread_create(&h->bg_thread, NULL, bg_main, h);
}
void vsh_close (vsh *h) {
    if (h->bg) { atomic_store(&h->bg_stop, 1); pthread_mutex_lock(&h->bg_mu); pthread_cond_signal(&h->bg_cv); pthread_mutex_unlock(&h->bg_mu); pthread_join(h->bg_thread, NULL); }
 for (int i = 0; i < h->nrh; i++) rh_close(&h->rh[i]); free(h->rh); munmap(h->sh, h->shm_len); free(h); }

// ---- lock ----
void vsh_lock (vsh *h) {
    uint64_t t0 = now_ns();
    for (unsigned spin = 0; ; spin++) {
        uint32_t e = 0;
        if (atomic_load_explicit(&h->sh->lock, memory_order_relaxed) == 0 && atomic_compare_exchange_strong_explicit(&h->sh->lock, &e, 1, memory_order_acquire, memory_order_relaxed)) break;
        if (spin > 200) sched_yield();
    }
    h->st.lock_wait_ns += now_ns() - t0;
}
void vsh_unlock (vsh *h) { atomic_store_explicit(&h->sh->lock, 0, memory_order_release); }

// ---- runs ----
typedef struct { uint64_t n, nblocks, bloom_bits; uint32_t min_dv, max_dv, pad; } metahdr;

static bool rh_open (vsh *h, uint64_t id, uint64_t n) {
    if (h->nrh == h->caprh) { h->caprh = h->caprh ? h->caprh * 2 : 16; h->rh = realloc(h->rh, (size_t)h->caprh * sizeof *h->rh); }
    rhandle *r = &h->rh[h->nrh]; memset(r, 0, sizeof *r); r->id = id; r->n = n; r->fd = -1;
    char path[600]; path_of(h, "run", id, path, sizeof path);
    r->fd = open(path, O_RDONLY); if (r->fd < 0) return false;
    void *m = mmap(NULL, n * sizeof(ent), PROT_READ, MAP_SHARED, r->fd, 0); if (m == MAP_FAILED) { close(r->fd); return false; } r->map = m;
    char mp[620]; snprintf(mp, sizeof mp, "%s.meta", path); int mfd = open(mp, O_RDONLY); if (mfd < 0) { rh_close(r); return false; }
    struct stat sb; fstat(mfd, &sb); r->meta_size = (uint64_t)sb.st_size;
    void *mm = mmap(NULL, r->meta_size, PROT_READ, MAP_SHARED, mfd, 0); close(mfd); if (mm == MAP_FAILED) { rh_close(r); return false; } r->meta = mm;
    const metahdr *mh = (const metahdr *)r->meta; r->bloom_bits = mh->bloom_bits; r->nblocks = mh->nblocks;
    r->bloom = r->meta + sizeof(metahdr); r->index = (const vsh_key *)(r->bloom + r->bloom_bits / 8);
    h->nrh++;
    return true;
}
static rhandle *rh_find (vsh *h, uint64_t id) { for (int i = 0; i < h->nrh; i++) if (h->rh[i].id == id) return &h->rh[i]; return NULL; }

// bring this process's cache of the manifest and its open runs up to date; returns the (even) version it describes
static uint32_t manifest_sync (vsh *h) {
    for (;;) {
        uint32_t v1 = atomic_load_explicit(&h->sh->ver, memory_order_acquire);
        if (v1 & 1) { sched_yield(); continue; }
        if (v1 == h->cache_ver) return v1;
        runinfo tmp[MAXRUNS]; uint32_t nr = h->sh->nruns; if (nr > MAXRUNS) continue;
        memcpy(tmp, h->sh->runs, nr * sizeof(runinfo));
        atomic_thread_fence(memory_order_acquire);
        if (atomic_load_explicit(&h->sh->ver, memory_order_relaxed) != v1) continue;
        memcpy(h->cache, tmp, nr * sizeof(runinfo)); h->ncache = (int)nr; h->cache_ver = v1;
        for (int i = 0; i < h->nrh; i++) { bool live = false; for (uint32_t j = 0; j < nr; j++) if (tmp[j].id == h->rh[i].id) live = true; if (!live) { rh_close(&h->rh[i]); h->rh[i] = h->rh[--h->nrh]; i--; } }
        for (uint32_t j = 0; j < nr; j++) if (!rh_find(h, tmp[j].id)) (void)rh_open(h, tmp[j].id, tmp[j].n);
        return v1;
    }
}

static bool bloom_maybe (const rhandle *r, vsh_key k) {
    uint64_t x = hash_key(k), h2 = mix(x) | 1;
    for (int j = 0; j < BLOOM_K; j++) { uint64_t b = (x + (uint64_t)j * h2) % r->bloom_bits; if (!(r->bloom[b >> 3] & (1u << (b & 7)))) return false; }
    return true;
}
static bool run_get (vsh *h, const rhandle *r, vsh_key k, vsh_val *out) {
    uint64_t lo = 0, hi = r->nblocks;
    while (lo < hi) { uint64_t mid = (lo + hi) / 2; if (key_cmp(r->index[mid], k) <= 0) lo = mid + 1; else hi = mid; }
    if (lo == 0) return false;
    uint64_t blk = lo - 1, first = blk * BLOCK_ENTRIES, cnt = r->n - first < BLOCK_ENTRIES ? r->n - first : BLOCK_ENTRIES;
    const ent *b = r->map + first; uint64_t l = 0, u = cnt; h->st.block_reads++;
    while (l < u) { uint64_t m = (l + u) / 2; int c = key_cmp(b[m].k, k); if (c == 0) { *out = b[m].v; return true; } if (c < 0) l = m + 1; else u = m; }
    return false;
}
static bool mt_get (const vsh *h, int t, vsh_key k, vsh_val *out) {
    slot *tab = h->mt[t]; uint64_t mask = h->sh->mt_cap - 1, i = hash_key(k) & mask;
    for (;;) {
        uint64_t hi = atomic_load_explicit(&tab[i].hi, memory_order_acquire);
        if (hi == EMPTY) return false;
        if (hi == k.hi && atomic_load_explicit(&tab[i].lo, memory_order_relaxed) == k.lo) { *out = unpack(atomic_load_explicit(&tab[i].val, memory_order_relaxed)); return true; }
        i = (i + 1) & mask;
    }
}

bool vsh_get (vsh *h, vsh_key k, vsh_val *out) {
    h->st.gets++;
    for (;;) {
        uint32_t v = manifest_sync(h);
        uint32_t a = atomic_load_explicit(&h->sh->active, memory_order_acquire);
        bool found = false; int where = 0;
        if (mt_get(h, (int)a, k, out)) { found = true; where = 1; }
        else if (atomic_load_explicit(&h->sh->mt_state[a ^ 1], memory_order_acquire) != MT_FREE && mt_get(h, (int)(a ^ 1), k, out)) { found = true; where = 1; }
        else for (int i = h->ncache - 1; i >= 0; i--) {
            rhandle *r = rh_find(h, h->cache[i].id); if (!r) { found = false; break; }
            if (!bloom_maybe(r, k)) { h->st.bloom_skips++; continue; }
            if (run_get(h, r, k, out)) { found = true; where = 2; break; }
        }
        if (atomic_load_explicit(&h->sh->ver, memory_order_acquire) != v) { h->st.retries++; continue; }       // the manifest or the tables were switched under us
        if (found) { if (where == 1) h->st.get_mem++; else h->st.get_runs++; }
        return found;
    }
}

// ---- writing ----
static void mt_put (vsh *h, int t, vsh_key k, vsh_val v, bool feed) {
    slot *tab = h->mt[t]; uint64_t mask = h->sh->mt_cap - 1, i = hash_key(k) & mask;
    for (;;) {
        uint64_t hi = atomic_load_explicit(&tab[i].hi, memory_order_relaxed);
        if (hi == EMPTY) {
            atomic_store_explicit(&tab[i].val, pack(v), memory_order_relaxed); atomic_store_explicit(&tab[i].lo, k.lo, memory_order_relaxed);
            atomic_store_explicit(&tab[i].hi, k.hi, memory_order_release);
            atomic_store_explicit(&h->sh->mt_n[t], atomic_load_explicit(&h->sh->mt_n[t], memory_order_relaxed) + 1, memory_order_release);
            break;
        }
        if (hi == k.hi && atomic_load_explicit(&tab[i].lo, memory_order_relaxed) == k.lo) { atomic_store_explicit(&tab[i].val, pack(v), memory_order_relaxed); break; }
        i = (i + 1) & mask;
    }
    if (feed) { uint64_t n = atomic_load_explicit(&h->sh->feed_n[t], memory_order_relaxed); h->feed[t][n] = (ent){ k, v }; atomic_store_explicit(&h->sh->feed_n[t], n + 1, memory_order_release); }
}

// the active table is full (or would be with n more puts): switch to the other one; false if the other is still being flushed
static bool maybe_switch (vsh *h, int n) {
    shdr *s = h->sh; uint32_t a = atomic_load_explicit(&s->active, memory_order_relaxed);
    if (atomic_load_explicit(&s->mt_n[a], memory_order_relaxed) + (uint64_t)n < s->mem_entries && atomic_load_explicit(&s->feed_n[a], memory_order_relaxed) + (uint64_t)n < s->feed_cap) return true;
    if (atomic_load_explicit(&s->mt_state[a ^ 1], memory_order_acquire) != MT_FREE) return false;
    atomic_fetch_add(&s->ver, 1);                                                       // odd: the tables are being switched
    atomic_store(&s->mt_state[a], MT_IMMUTABLE); atomic_store(&s->mt_state[a ^ 1], MT_ACTIVE); atomic_store(&s->active, a ^ 1);
    atomic_fetch_add(&s->ver, 1);
    h->owed = (int)a;
    return true;
}

// false (nothing done, lock still held) if the active table is full and the other one is still being flushed: the caller drops the lock, vsh_wait_room()s and starts again
bool vsh_commit_locked (vsh *h, const vsh_key *k, const vsh_val *v, int n) {
    if (!maybe_switch(h, n)) return false;
    uint32_t a = atomic_load_explicit(&h->sh->active, memory_order_relaxed);
    for (int i = 0; i < n; i++) mt_put(h, (int)a, k[i], v[i], true);
    h->st.commits++;
    return true;
}
void vsh_wait_room (vsh *h) {
    uint64_t t0 = now_ns(); bool waited = false;
    while (atomic_load_explicit(&h->sh->mt_state[atomic_load(&h->sh->active) ^ 1], memory_order_acquire) != MT_FREE) { waited = true; struct timespec ts = { 0, 100000 }; nanosleep(&ts, NULL); }
    if (waited) { h->st.stalls++; h->st.stall_ns += now_ns() - t0; }
}

static void do_flush (vsh *h, int t);
void vsh_unlock_flush (vsh *h) {
    int owed = h->owed; h->owed = -1; vsh_unlock(h);
    if (owed < 0) return;
    if (!h->bg) { do_flush(h, owed); return; }
    atomic_fetch_or(&h->bg_mask, 1 << owed); pthread_mutex_lock(&h->bg_mu); pthread_cond_signal(&h->bg_cv); pthread_mutex_unlock(&h->bg_mu);
}
void vsh_commit (vsh *h, const vsh_key *k, const vsh_val *v, int n) { for (;;) { vsh_lock(h); if (vsh_commit_locked(h, k, v, n)) break; vsh_unlock(h); vsh_wait_room(h); } vsh_unlock_flush(h); }

static void write_all (int fd, const void *buf, size_t len) { const uint8_t *p = buf; size_t off = 0; while (off < len) { size_t c = len - off > (1u << 24) ? (1u << 24) : len - off; ssize_t w = pwrite(fd, p + off, c, (off_t)off); if (w <= 0) return; off += (size_t)w; } }

// writes the sorted entries as run <id> (+ its meta file); returns false on error
static bool write_run (vsh *h, uint64_t id, const ent *e, uint64_t n, uint32_t *min_dv, uint32_t *max_dv) {
    char path[600]; path_of(h, "run", id, path, sizeof path);
    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644); if (fd < 0) return false;
    write_all(fd, e, n * sizeof(ent)); fsync(fd); close(fd);
    uint64_t bits = (n * BLOOM_BITS_PER_KEY + 63) & ~63ull, nblocks = (n + BLOCK_ENTRIES - 1) / BLOCK_ENTRIES;
    size_t msize = sizeof(metahdr) + bits / 8 + nblocks * sizeof(vsh_key); uint8_t *mb = calloc(1, msize);
    metahdr *mh = (metahdr *)mb; uint8_t *bloom = mb + sizeof(metahdr); vsh_key *index = (vsh_key *)(bloom + bits / 8);
    uint32_t mn = UINT32_MAX, mx = 0;
    for (uint64_t i = 0; i < n; i++) {
        if (i % BLOCK_ENTRIES == 0) index[i / BLOCK_ENTRIES] = e[i].k;
        uint64_t x = hash_key(e[i].k), h2 = mix(x) | 1;
        for (int j = 0; j < BLOOM_K; j++) { uint64_t b = (x + (uint64_t)j * h2) % bits; bloom[b >> 3] |= (uint8_t)(1u << (b & 7)); }
        if (e[i].v.dv < mn) mn = e[i].v.dv; if (e[i].v.dv > mx) mx = e[i].v.dv;
    }
    *mh = (metahdr){ n, nblocks, bits, mn, mx, 0 }; *min_dv = mn; *max_dv = mx;
    char mp[620]; snprintf(mp, sizeof mp, "%s.meta", path);
    int mfd = open(mp, O_RDWR | O_CREAT | O_TRUNC, 0644); if (mfd < 0) { free(mb); return false; }
    write_all(mfd, mb, msize); fsync(mfd); close(mfd); free(mb);
    return true;
}

static uint32_t cls_of (const shdr *s, uint64_t n) { uint32_t c = 0; uint64_t cap = s->mem_entries; while (n > cap) { cap *= 4; c++; } return c; }

static void do_merge (vsh *h);
static void do_flush (vsh *h, int t) {
    shdr *s = h->sh; uint64_t t0 = now_ns();
    atomic_store(&s->mt_state[t], MT_FLUSHING);
    uint64_t cap = s->mt_cap, n = 0; slot *tab = h->mt[t];
    ent *a = malloc((atomic_load(&s->mt_n[t]) + 1) * sizeof(ent));
    for (uint64_t i = 0; i < cap; i++) { uint64_t hi = atomic_load_explicit(&tab[i].hi, memory_order_relaxed); if (hi != EMPTY) { a[n].k = (vsh_key){ hi, atomic_load_explicit(&tab[i].lo, memory_order_relaxed) }; a[n].v = unpack(atomic_load_explicit(&tab[i].val, memory_order_relaxed)); n++; } }
    qsort(a, n, sizeof(ent), ent_cmp);
    uint64_t id = atomic_fetch_add(&s->next_id, 1), fid = atomic_fetch_add(&s->next_id, 1); uint32_t mn = 0, mx = 0;
    bool ok = write_run(h, id, a, n, &mn, &mx);
    uint64_t fn = atomic_load(&s->feed_n[t]); uint32_t fmin = 0, fmax = 0;
    if (fn) {
        char fp[600]; path_of(h, "feed", fid, fp, sizeof fp); int ffd = open(fp, O_RDWR | O_CREAT | O_TRUNC, 0644);
        if (ffd >= 0) { write_all(ffd, h->feed[t], fn * sizeof(ent)); fsync(ffd); close(ffd); fmin = h->feed[t][0].v.dv; fmax = h->feed[t][fn - 1].v.dv; }
    }
    free(a);
    if (ok) {
        vsh_lock(h);
        atomic_fetch_add(&s->ver, 1);
        if (s->nruns < MAXRUNS) s->runs[s->nruns++] = (runinfo){ id, n, mn, mx, cls_of(s, n) };
        if (fn && s->nsegs < MAXSEGS) s->segs[s->nsegs++] = (seginfo){ fid, fn, fmin, fmax };
        atomic_fetch_add(&s->ver, 1);
        vsh_unlock(h);
        h->st.flushes++; h->st.bytes_flushed += n * sizeof(ent); h->st.bytes_feed += fn * sizeof(ent);
    }
    for (uint64_t i = 0; i < cap; i++) atomic_store_explicit(&tab[i].hi, EMPTY, memory_order_relaxed);        // (readers that saw the old manifest retry on the version change)
    atomic_store(&s->mt_n[t], 0); atomic_store(&s->feed_n[t], 0);
    atomic_store_explicit(&s->mt_state[t], MT_FREE, memory_order_release);
    h->st.flush_ns += now_ns() - t0;
    do_merge(h);
}

static void do_merge (vsh *h) {
    shdr *s = h->sh; uint32_t exp = 0;
    if (!atomic_compare_exchange_strong(&s->merge_busy, &exp, 1)) return;
    for (;;) {
        vsh_lock(h); runinfo cur[MAXRUNS]; int nr = (int)s->nruns; memcpy(cur, s->runs, (size_t)nr * sizeof(runinfo)); vsh_unlock(h);
        int idx[8], m = 0, c = -1;
        for (int cl = 0; cl < 16 && c < 0; cl++) { m = 0; for (int i = 0; i < nr && m < 4; i++) if (cur[i].cls == (uint32_t)cl) idx[m++] = i; if (m == 4) c = cl; }
        if (c < 0) break;
        uint64_t t0 = now_ns(); rhandle in[4]; uint64_t pos[4] = {0}, total = 0; bool ok = true;
        for (int j = 0; j < 4; j++) { memset(&in[j], 0, sizeof in[j]); in[j].fd = -1; char path[600]; path_of(h, "run", cur[idx[j]].id, path, sizeof path); in[j].fd = open(path, O_RDONLY); in[j].n = cur[idx[j]].n; in[j].id = cur[idx[j]].id;
            if (in[j].fd < 0) { ok = false; break; } void *mp = mmap(NULL, in[j].n * sizeof(ent), PROT_READ, MAP_SHARED, in[j].fd, 0); if (mp == MAP_FAILED) { ok = false; break; } in[j].map = mp; total += in[j].n; }
        if (!ok) { for (int j = 0; j < 4; j++) if (in[j].fd >= 0 || in[j].map) { if (in[j].map) munmap((void *)in[j].map, in[j].n * sizeof(ent)); if (in[j].fd >= 0) close(in[j].fd); } break; }
        ent *out = malloc(total * sizeof(ent)); uint64_t n = 0;
        for (;;) {
            int best = -1;
            for (int j = 0; j < 4; j++) if (pos[j] < in[j].n && (best < 0 || key_cmp(in[j].map[pos[j]].k, in[best].map[pos[best]].k) < 0 || (key_cmp(in[j].map[pos[j]].k, in[best].map[pos[best]].k) == 0 && in[j].id > in[best].id))) best = j;
            if (best < 0) break;
            ent e = in[best].map[pos[best]];
            for (int j = 0; j < 4; j++) while (pos[j] < in[j].n && key_cmp(in[j].map[pos[j]].k, e.k) == 0) pos[j]++;
            out[n++] = e;
        }
        uint64_t id = atomic_fetch_add(&s->next_id, 1); uint32_t mn, mx; bool wok = write_run(h, id, out, n, &mn, &mx); free(out);
        for (int j = 0; j < 4; j++) { munmap((void *)in[j].map, in[j].n * sizeof(ent)); close(in[j].fd); }
        if (!wok) break;
        vsh_lock(h);
        atomic_fetch_add(&s->ver, 1);
        runinfo keep[MAXRUNS]; int w = 0; bool placed = false;
        for (uint32_t i = 0; i < s->nruns; i++) { bool gone = false; for (int j = 0; j < 4; j++) if (s->runs[i].id == cur[idx[j]].id) gone = true; if (!gone) keep[w++] = s->runs[i]; else if (!placed) { keep[w++] = (runinfo){ id, n, mn, mx, cls_of(s, n) }; placed = true; } }
        memcpy(s->runs, keep, (size_t)w * sizeof(runinfo)); s->nruns = (uint32_t)w;
        atomic_fetch_add(&s->ver, 1);
        vsh_unlock(h);
        for (int j = 0; j < 4; j++) { char path[600], mp[620]; path_of(h, "run", cur[idx[j]].id, path, sizeof path); snprintf(mp, sizeof mp, "%s.meta", path); unlink(path); unlink(mp); }      // (processes that still map them keep working: the files live until the last unmap)
        h->st.merges++; h->st.bytes_merged += n * sizeof(ent); h->st.merge_ns += now_ns() - t0;
    }
    atomic_store(&s->merge_busy, 0);
}

void vsh_flush_all (vsh *h) {
    vsh_lock(h);
    shdr *s = h->sh; uint32_t a = atomic_load(&s->active);
    if (atomic_load(&s->mt_n[a]) == 0 && atomic_load(&s->mt_state[a ^ 1]) == MT_FREE) { vsh_unlock(h); return; }
    while (atomic_load(&s->mt_state[a ^ 1]) != MT_FREE) { vsh_unlock(h); struct timespec ts = { 0, 100000 }; nanosleep(&ts, NULL); vsh_lock(h); }
    atomic_fetch_add(&s->ver, 1); atomic_store(&s->mt_state[a], MT_IMMUTABLE); atomic_store(&s->mt_state[a ^ 1], MT_ACTIVE); atomic_store(&s->active, a ^ 1); atomic_fetch_add(&s->ver, 1);
    vsh_unlock(h);
    do_flush(h, (int)a);
}

uint64_t vsh_feed_after (vsh *h, uint32_t after, void (*cb) (void *, vsh_key, vsh_val), void *arg) {
    shdr *s = h->sh; uint64_t delivered = 0; seginfo segs[MAXSEGS]; uint32_t ns;
    for (;;) { uint32_t v1 = atomic_load_explicit(&s->ver, memory_order_acquire); if (v1 & 1) { sched_yield(); continue; } ns = s->nsegs; memcpy(segs, s->segs, ns * sizeof(seginfo)); if (atomic_load_explicit(&s->ver, memory_order_acquire) == v1) break; }
    for (uint32_t i = 0; i < ns; i++) {
        if (segs[i].max_dv <= after) continue;
        char fp[600]; path_of(h, "feed", segs[i].id, fp, sizeof fp); int fd = open(fp, O_RDONLY); if (fd < 0) continue;
        const ent *src = mmap(NULL, segs[i].n * sizeof(ent), PROT_READ, MAP_SHARED, fd, 0); close(fd); if ((void *)src == MAP_FAILED) continue;
        uint64_t lo = 0, hi = segs[i].n; while (lo < hi) { uint64_t mid = (lo + hi) / 2; if (src[mid].v.dv > after) hi = mid; else lo = mid + 1; }
        for (uint64_t j = lo; j < segs[i].n; j++) { vsh_val cur; if (vsh_get(h, src[j].k, &cur) && cur.cv == src[j].v.cv && cur.dv == src[j].v.dv) { cb(arg, src[j].k, src[j].v); delivered++; } }
        munmap((void *)src, segs[i].n * sizeof(ent));
    }
    for (int t = 0; t < 2; t++) { uint64_t n = atomic_load_explicit(&s->feed_n[t], memory_order_acquire); for (uint64_t j = 0; j < n; j++) if (h->feed[t][j].v.dv > after) { vsh_val cur; if (vsh_get(h, h->feed[t][j].k, &cur) && cur.cv == h->feed[t][j].v.cv && cur.dv == h->feed[t][j].v.dv) { cb(arg, h->feed[t][j].k, h->feed[t][j].v); delivered++; } } }
    return delivered;
}

void vsh_stats_get (vsh *h, vsh_stats *st) {
    *st = h->st; shdr *s = h->sh; st->runs = s->nruns; st->disk_bytes = 0; for (uint32_t i = 0; i < s->nruns; i++) st->disk_bytes += s->runs[i].n * sizeof(ent);
}
