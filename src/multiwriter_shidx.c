//
//  multiwriter_shidx.c
//  cloudsync
//
//  Shared version index: see multiwriter_shidx.h.
//

#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/file.h>
#include "multiwriter_io.h"
#include "multiwriter_shidx.h"

#define SHIDX_MAGIC   0x58444953574d4357ull         // "WCMWSIDX"
#define SHIDX_VERSION 1
#define BLOCK_BITS    12
#define BLOCK_PAGES   (1u << BLOCK_BITS)
#define NONE          UINT64_MAX
#define QBIT          0x80000000u          // in a head word: the page is in the GC candidate list
#define HIDX(w)       ((w) & ~QBIT)

// One version: immutable once the head that points to it was published (the fields are atomics only so that a reader racing with the reuse of a freed entry, which the
// protocol rules out, is not undefined behaviour; the checks in lookup would catch it).
typedef struct {
    _Atomic uint64_t epoch;
    _Atomic uint64_t loc;
    _Atomic uint32_t prev;          // next older version (arena index), or the next free entry when free
    _Atomic uint32_t pgno;
    uint8_t          pad[8];
} ver;                              // 32 bytes

typedef struct {
    _Atomic uint64_t snap;          // snapshot epoch held by the read transaction using this slot, NONE if idle
    _Atomic int32_t  pid;           // owner, 0 = free
    uint8_t          pad[52];
} slot;                             // 64 bytes: one cache line each

typedef struct {
    uint64_t magic;
    uint32_t version, pad0;
    uint32_t max_pages_log2, max_entries, nslots, max_cands, dir_n;
    uint32_t pad1;
    uint64_t off_dir, off_blocks, off_arena, off_slots, off_cand, total;
    _Atomic uint64_t committed;     // newest published epoch
    _Atomic uint64_t floor;         // no snapshot below this may be taken any more (GC announces it before it looks at the registry)
    _Atomic uint32_t slot_hint;     // where the next slot search starts
    _Atomic uint32_t pad2;
    // writer only
    uint64_t installed;             // newest installed epoch
    uint32_t arena_top, free_head, n_free, blocks_top, cand_n, cand_overflow;
    uint64_t st_installs, st_freed, st_live, st_gc_runs, st_gc_full;
    _Atomic uint64_t st_hazard;     // lookups that met a recycled entry (must stay 0)
} hdr;

struct shidx {
    int       fd;
    uint8_t  *base;
    size_t    len;
    hdr      *h;
    _Atomic uint32_t *dir;
    _Atomic uint32_t *blocks;       // block b = blocks + (b) * BLOCK_PAGES, b is 0-based
    ver      *arena;                // index 0 unused
    slot     *slots;
    uint32_t *cand;                 // pgno + 1
};

static inline _Atomic uint32_t *head_ref (shidx *ix, uint32_t pgno, bool create) {
    uint32_t b = pgno >> BLOCK_BITS;
    if (b >= ix->h->dir_n) return NULL;
    uint32_t bi = atomic_load_explicit(&ix->dir[b], memory_order_acquire);
    if (!bi) {
        if (!create) return NULL;
        if (ix->h->blocks_top >= ix->h->dir_n) return NULL;
        bi = ++ix->h->blocks_top;                                   // (1-based; writer only)
        atomic_store_explicit(&ix->dir[b], bi, memory_order_release);
    }
    return &ix->blocks[(size_t)(bi - 1) * BLOCK_PAGES + (pgno & (BLOCK_PAGES - 1))];
}

// MARK: - open / close -

static size_t align_up (size_t v, size_t a) { return (v + a - 1) & ~(a - 1); }

shidx *shidx_open (const char *path, const shidx_params *params) {
    int fd = open(path, O_RDWR | O_CREAT, 0644);
    if (fd < 0) return NULL;
    if (flock(fd, LOCK_EX) != 0) { close(fd); return NULL; }
    struct stat sb;
    if (fstat(fd, &sb) != 0) { flock(fd, LOCK_UN); close(fd); return NULL; }
    if (sb.st_size == 0) {                                          // first opener: lay the file out
        shidx_params p = { 24, 4u << 20, 4096, 0 };
        if (params) { if (params->max_pages_log2) p.max_pages_log2 = params->max_pages_log2; if (params->max_entries) p.max_entries = params->max_entries;
                      if (params->nslots) p.nslots = params->nslots; if (params->max_cands) p.max_cands = params->max_cands; }
        if (!p.max_cands) p.max_cands = p.max_entries + 1;                   // (a page is queued once: never more candidates than versions)
        if (p.max_pages_log2 < BLOCK_BITS + 1 || p.max_pages_log2 > 32) { errno = EINVAL; flock(fd, LOCK_UN); close(fd); return NULL; }
        hdr h;
        memset(&h, 0, sizeof h);
        h.version = SHIDX_VERSION;
        h.max_pages_log2 = p.max_pages_log2; h.max_entries = p.max_entries; h.nslots = p.nslots; h.max_cands = p.max_cands;
        h.dir_n = 1u << (p.max_pages_log2 - BLOCK_BITS);
        h.off_dir = 4096;
        h.off_blocks = align_up(h.off_dir + (size_t)h.dir_n * 4, 4096);
        h.off_arena = align_up(h.off_blocks + (size_t)h.dir_n * BLOCK_PAGES * 4, 4096);
        h.off_slots = align_up(h.off_arena + ((size_t)p.max_entries + 1) * sizeof(ver), 4096);
        h.off_cand = align_up(h.off_slots + (size_t)p.nslots * sizeof(slot), 4096);
        h.total = align_up(h.off_cand + (size_t)p.max_cands * 4, 4096);
        if (mw_io_ftruncate(fd, (off_t)h.total) != 0) { int e = errno; flock(fd, LOCK_UN); close(fd); errno = e; return NULL; }
        uint8_t *m = mw_io_mmap(NULL, h.total, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (m == MAP_FAILED) { int e = errno; flock(fd, LOCK_UN); close(fd); errno = e; return NULL; }
        hdr *hp = (hdr *)m;
        memcpy(hp, &h, sizeof h);
        slot *sl = (slot *)(m + h.off_slots);
        for (uint32_t i = 0; i < p.nslots; i++) atomic_init(&sl[i].snap, NONE);
        atomic_init(&hp->committed, 1); atomic_init(&hp->floor, 0);
        atomic_thread_fence(memory_order_seq_cst);
        hp->magic = SHIDX_MAGIC;                                    // last: an opener that sees the magic sees everything
        mw_io_msync(m, 4096, MS_ASYNC);
        munmap(m, h.total);
    }
    if (fstat(fd, &sb) != 0) { flock(fd, LOCK_UN); close(fd); return NULL; }
    uint8_t *m = mw_io_mmap(NULL, (size_t)sb.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    flock(fd, LOCK_UN);
    if (m == MAP_FAILED) { close(fd); return NULL; }
    hdr *h = (hdr *)m;
    if (h->magic != SHIDX_MAGIC || h->version != SHIDX_VERSION || h->total != (uint64_t)sb.st_size) { munmap(m, (size_t)sb.st_size); close(fd); errno = EINVAL; return NULL; }
    shidx *ix = calloc(1, sizeof *ix);
    if (!ix) { munmap(m, (size_t)sb.st_size); close(fd); return NULL; }
    ix->fd = fd; ix->base = m; ix->len = (size_t)sb.st_size; ix->h = h;
    ix->dir = (_Atomic uint32_t *)(m + h->off_dir);
    ix->blocks = (_Atomic uint32_t *)(m + h->off_blocks);
    ix->arena = (ver *)(m + h->off_arena);
    ix->slots = (slot *)(m + h->off_slots);
    ix->cand = (uint32_t *)(m + h->off_cand);
    return ix;
}

void shidx_close (shidx *ix) {
    if (!ix) return;
    munmap(ix->base, ix->len);
    close(ix->fd);
    free(ix);
}

void shidx_unlink (const char *path) { unlink(path); }

// MARK: - readers -

int shidx_slot_alloc (shidx *ix, int32_t pid) {
    uint32_t n = ix->h->nslots;
    uint32_t start = atomic_load_explicit(&ix->h->slot_hint, memory_order_relaxed) % n;
    for (uint32_t k = 0; k < n; k++) {
        uint32_t i = (start + k) % n;
        int32_t exp = 0;
        if (atomic_load_explicit(&ix->slots[i].pid, memory_order_relaxed) == 0 && atomic_compare_exchange_strong(&ix->slots[i].pid, &exp, pid)) {
            atomic_store(&ix->slots[i].snap, NONE);
            atomic_store_explicit(&ix->h->slot_hint, i + 1, memory_order_relaxed);
            return (int)i;
        }
    }
    return -1;
}

void shidx_slot_free (shidx *ix, int slot_i) {
    if (slot_i < 0) return;
    atomic_store(&ix->slots[slot_i].snap, NONE);
    atomic_store(&ix->slots[slot_i].pid, 0);
}

uint64_t shidx_pin (shidx *ix, int slot_i) {
    slot *s = &ix->slots[slot_i];
    for (;;) {
        uint64_t e = atomic_load_explicit(&ix->h->committed, memory_order_acquire);
        atomic_store(&s->snap, e);                                  // seq_cst: before the floor is read
        if (e >= atomic_load(&ix->h->floor)) return e;              // the GC has not gone past it
        // (a GC announced a floor above our epoch between the two loads: take a newer snapshot)
    }
}

void shidx_unpin (shidx *ix, int slot_i) { atomic_store_explicit(&ix->slots[slot_i].snap, NONE, memory_order_release); }

bool shidx_lookup (shidx *ix, uint32_t pgno, uint64_t snap, uint64_t *epoch, uint64_t *loc) {
    _Atomic uint32_t *hr = head_ref(ix, pgno, false);
    if (!hr) return false;
    for (int attempt = 0; attempt < 4; attempt++) {
        uint32_t e = HIDX(atomic_load_explicit(hr, memory_order_acquire));
        bool bad = false;
        while (e) {
            ver *v = &ix->arena[e];
            uint64_t ep = atomic_load_explicit(&v->epoch, memory_order_relaxed);
            if (atomic_load_explicit(&v->pgno, memory_order_relaxed) != pgno) { bad = true; break; }      // a recycled entry: cannot happen under the protocol
            if (ep <= snap) {
                *epoch = ep;
                *loc = atomic_load_explicit(&v->loc, memory_order_relaxed);
                atomic_thread_fence(memory_order_acquire);                     // (seqlock read: the value above is only good if the entry still has the identity below; without this fence the load of `loc` may complete after the re-check on ARM)
                if (atomic_load_explicit(&v->pgno, memory_order_acquire) != pgno || atomic_load_explicit(&v->epoch, memory_order_relaxed) != ep) { bad = true; break; }
                return true;
            }
            e = atomic_load_explicit(&v->prev, memory_order_acquire);
        }
        if (!bad) return false;
        // The entry we were standing on was freed under us: a chain dropped because its newest version is in the real file (the reader's snapshot is at or above
        // the floor, so it is that version it wanted). Looking again finds the empty chain, or a newer version it does not want: the real file.
        atomic_fetch_add_explicit(&ix->h->st_hazard, 1, memory_order_relaxed);
    }
    return false;
}

bool shidx_dbsize (shidx *ix, uint64_t snap, uint32_t *pages) {
    uint64_t ep, loc;
    if (!shidx_lookup(ix, 0, snap, &ep, &loc)) return false;
    *pages = (uint32_t)loc;
    return true;
}

uint64_t shidx_committed (shidx *ix) { return atomic_load_explicit(&ix->h->committed, memory_order_acquire); }
uint64_t shidx_oldest (shidx *ix) { return atomic_load_explicit(&ix->h->floor, memory_order_acquire); }

// MARK: - writer -

static uint32_t arena_alloc (shidx *ix) {
    hdr *h = ix->h;
    uint32_t e;
    if (h->free_head) { e = h->free_head; h->free_head = atomic_load_explicit(&ix->arena[e].prev, memory_order_relaxed); h->n_free--; }
    else if (h->arena_top < h->max_entries) e = ++h->arena_top;
    else return 0;
    h->st_live++;
    return e;
}

static void arena_free (shidx *ix, uint32_t e) {
    hdr *h = ix->h;
    ver *v = &ix->arena[e];
    atomic_store_explicit(&v->pgno, 0xFFFFFFFFu, memory_order_relaxed);          // poison: a stale reader would notice
    atomic_thread_fence(memory_order_release);                                    // (before the fields are overwritten: a reader that sees an overwritten `loc` must also see the poison)
    atomic_store_explicit(&v->epoch, 0, memory_order_relaxed);
    atomic_store_explicit(&v->loc, 0, memory_order_relaxed);
    atomic_store_explicit(&v->prev, h->free_head, memory_order_relaxed);
    h->free_head = e; h->n_free++; h->st_live--; h->st_freed++;
}

static void cand_push (shidx *ix, uint32_t pgno) {
    hdr *h = ix->h;
    if (h->cand_n < h->max_cands) ix->cand[h->cand_n++] = pgno + 1; else h->cand_overflow = 1;
}

int shidx_install (shidx *ix, uint64_t epoch, uint32_t dbsize, int n, const uint32_t *pgnos, const uint64_t *locs) {
    hdr *h = ix->h;
    uint32_t need = (uint32_t)n + 1;
    uint32_t room = h->n_free + (h->max_entries - h->arena_top);
    if (room < need) return -1;
    for (int i = 0; i < n; i++) if ((pgnos[i] >> BLOCK_BITS) >= h->dir_n || pgnos[i] == 0) return -2;
    if (!head_ref(ix, 0, true)) return -2;
    for (int i = 0; i < n; i++) if (!head_ref(ix, pgnos[i], true)) return -2;       // (blocks first: a failure here must leave no half-installed commit; an allocated block is harmless)
    // (the size record first, then the pages: all with the same epoch, invisible to snapshots until shidx_publish)
    for (int i = -1; i < n; i++) {
        uint32_t pgno = i < 0 ? 0 : pgnos[i];
        uint64_t loc = i < 0 ? dbsize : locs[i];
        _Atomic uint32_t *hr = head_ref(ix, pgno, true);
        if (!hr) return -2;
        uint32_t e = arena_alloc(ix);
        ver *v = &ix->arena[e];
        uint32_t oldw = atomic_load_explicit(hr, memory_order_relaxed), old = HIDX(oldw);
        atomic_thread_fence(memory_order_release);                              // (a recycled entry: the poison of its previous life is visible before the new fields)
        atomic_store_explicit(&v->epoch, epoch, memory_order_relaxed);
        atomic_store_explicit(&v->loc, loc, memory_order_relaxed);
        atomic_store_explicit(&v->pgno, pgno, memory_order_relaxed);
        atomic_store_explicit(&v->prev, old, memory_order_relaxed);
        uint32_t q = oldw & QBIT;
        if (!q) { cand_push(ix, pgno); q = QBIT; }                              // every page with a version waits for GC: a second version, or the base passing the first
        atomic_store_explicit(hr, e | q, memory_order_release);                 // publishes the entry
    }
    h->installed = epoch;
    h->st_installs++;
    return 0;
}

void shidx_publish (shidx *ix, uint64_t epoch) { atomic_store_explicit(&ix->h->committed, epoch, memory_order_release); }

uint64_t shidx_head_epoch (shidx *ix, uint32_t pgno) {
    _Atomic uint32_t *hr = head_ref(ix, pgno, false);
    if (!hr) return 0;
    uint32_t e = HIDX(atomic_load_explicit(hr, memory_order_acquire));
    return e ? atomic_load_explicit(&ix->arena[e].epoch, memory_order_relaxed) : 0;
}

// The floor: announce, fence, re-scan the registry (the compaction protocol): no snapshot below the result can exist or be taken any more.
static uint64_t compute_floor (shidx *ix) {
    uint64_t c = atomic_load_explicit(&ix->h->committed, memory_order_acquire);
    atomic_store(&ix->h->floor, c);                                 // seq_cst
    uint64_t m = c;
    for (uint32_t i = 0; i < ix->h->nslots; i++) {
        uint64_t s = atomic_load(&ix->slots[i].snap);
        if (s < m) m = s;
    }
    if (m < c) atomic_store(&ix->h->floor, m);                      // (readers that validated against c hold epochs >= c >= m: still safe)
    return m;
}

// Frees what is older than the newest version <= f; returns freed count. Pushes the page back as a candidate while it still has several versions.
static uint64_t chain_gc (shidx *ix, uint32_t pgno, uint64_t f, uint64_t base, bool *more) {
    *more = false;
    _Atomic uint32_t *hr = head_ref(ix, pgno, false);
    if (!hr) return 0;
    uint32_t e = HIDX(atomic_load_explicit(hr, memory_order_relaxed)), head = e, keeper = 0;
    uint32_t above = 0;
    while (e) {
        if (atomic_load_explicit(&ix->arena[e].epoch, memory_order_relaxed) <= f) { keeper = e; break; }
        above++;
        e = atomic_load_explicit(&ix->arena[e].prev, memory_order_relaxed);
    }
    uint64_t freed = 0;
    if (keeper) {
        uint32_t old = atomic_load_explicit(&ix->arena[keeper].prev, memory_order_relaxed);
        if (old) {
            atomic_store_explicit(&ix->arena[keeper].prev, 0, memory_order_release);       // cut first, then free
            while (old) { uint32_t nx = atomic_load_explicit(&ix->arena[old].prev, memory_order_relaxed); arena_free(ix, old); freed++; old = nx; }
        }
        if (keeper == head && pgno != 0 && atomic_load_explicit(&ix->arena[keeper].epoch, memory_order_relaxed) <= base) {
            atomic_store_explicit(hr, 0, memory_order_release);                            // the real file has this page
            arena_free(ix, keeper); freed++;
        }
    }
    *more = above > 0 || (keeper && keeper != head);
    if (keeper && keeper == head && atomic_load_explicit(&ix->arena[keeper].epoch, memory_order_relaxed) > base && pgno != 0) *more = true;   // waits for the base to pass it
    if (!*more && atomic_load_explicit(hr, memory_order_relaxed) != 0) atomic_store_explicit(hr, head, memory_order_release);          // (nothing left to wait for: out of the list)
    return freed;
}

static uint64_t gc_run (shidx *ix, uint64_t f, uint64_t base);
uint64_t shidx_gc (shidx *ix, uint64_t base) { return gc_run(ix, compute_floor(ix), base); }
uint64_t shidx_gc_floor (shidx *ix, uint64_t floor, uint64_t base) { return gc_run(ix, floor, base); }

uint32_t shidx_room (shidx *ix) { return ix->h->n_free + (ix->h->max_entries - ix->h->arena_top); }

void shidx_scan (shidx *ix, uint64_t base, uint64_t upto, shidx_scan_fn cb, void *ctx) {
    hdr *h = ix->h;
    for (uint32_t b = 0; b < h->dir_n; b++) {
        uint32_t bi = atomic_load_explicit(&ix->dir[b], memory_order_acquire);
        if (!bi) continue;
        _Atomic uint32_t *heads = &ix->blocks[(size_t)(bi - 1) * BLOCK_PAGES];
        for (uint32_t k = 0; k < BLOCK_PAGES; k++) {
            uint32_t e = HIDX(atomic_load_explicit(&heads[k], memory_order_acquire));
            uint32_t pgno = (b << BLOCK_BITS) | k;
            if (!e || pgno == 0) continue;
            while (e) {
                ver *v = &ix->arena[e];
                uint64_t ep = atomic_load_explicit(&v->epoch, memory_order_relaxed);
                if (atomic_load_explicit(&v->pgno, memory_order_relaxed) != pgno) break;       // freed under us (cannot happen while the caller holds the floor): skip
                if (ep <= upto) { if (ep > base) cb(ctx, pgno, ep, atomic_load_explicit(&v->loc, memory_order_relaxed)); break; }
                e = atomic_load_explicit(&v->prev, memory_order_acquire);
            }
        }
    }
}

static uint64_t gc_run (shidx *ix, uint64_t f, uint64_t base) {
    hdr *h = ix->h;
    uint64_t freed = 0;
    h->st_gc_runs++;
    if (h->cand_overflow) {                                         // the candidate list lost entries: look at every page
        h->st_gc_full++;
        h->cand_overflow = 0; h->cand_n = 0;
        for (uint32_t b = 0; b < h->dir_n; b++) {
            uint32_t bi = atomic_load_explicit(&ix->dir[b], memory_order_relaxed);
            if (!bi) continue;
            for (uint32_t k = 0; k < BLOCK_PAGES; k++) {
                uint32_t pgno = (b << BLOCK_BITS) | k;
                _Atomic uint32_t *hr = &ix->blocks[(size_t)(bi - 1) * BLOCK_PAGES + k];
                uint32_t w = atomic_load_explicit(hr, memory_order_relaxed);
                if (!w) continue;
                atomic_store_explicit(hr, HIDX(w), memory_order_release);          // (the list is rebuilt: clear the flag)
                bool more;
                freed += chain_gc(ix, pgno, f, base, &more);
                if (more) { uint32_t w2 = atomic_load_explicit(hr, memory_order_relaxed); if (w2 && !(w2 & QBIT)) { cand_push(ix, pgno); atomic_store_explicit(hr, w2 | QBIT, memory_order_release); } }
            }
        }
        return freed;
    }
    uint32_t n = h->cand_n;
    h->cand_n = 0;
    uint32_t keep = 0;
    for (uint32_t i = 0; i < n; i++) {                              // (pages pushed back by this run are appended behind the unread ones: copy forward)
        uint32_t pgno = ix->cand[i] - 1;
        bool more;
        freed += chain_gc(ix, pgno, f, base, &more);
        if (more) ix->cand[keep++] = pgno + 1;
    }
    h->cand_n = keep;
    return freed;
}

int shidx_reap (shidx *ix, bool (*alive)(int32_t pid, void *ctx), void *ctx) {
    int n = 0;
    for (uint32_t i = 0; i < ix->h->nslots; i++) {
        int32_t pid = atomic_load(&ix->slots[i].pid);
        if (pid > 0 && !alive(pid, ctx) && atomic_compare_exchange_strong(&ix->slots[i].pid, &pid, -1)) {
            atomic_store(&ix->slots[i].snap, NONE);
            atomic_store(&ix->slots[i].pid, 0);
            n++;
        }
    }
    return n;
}

void shidx_stats_get (shidx *ix, shidx_stats *out) {
    hdr *h = ix->h;
    out->installs = h->st_installs; out->versions_live = h->st_live; out->versions_freed = h->st_freed; out->blocks = h->blocks_top;
    out->gc_runs = h->st_gc_runs; out->gc_full_scans = h->st_gc_full; out->entries_cap = h->max_entries; out->entries_used = h->arena_top;
    out->hazards = atomic_load_explicit(&h->st_hazard, memory_order_relaxed);
}
