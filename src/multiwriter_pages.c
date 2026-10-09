//
//  multiwriter_pages.c
//  sqlite-multiwriter
//
//  The versioned committed page store and the commit protocol on top of it.
//
//  Structure: a two-level directory of per-page version chains (ascending epoch). Locking is striped:
//  the chains of pages with the same (pgno % MW_STRIPES) share one mutex.
//    - readers  : lock one stripe, copy the bytes they need, unlock (a version can therefore never be
//                 freed under a reader);
//    - publish  : lock the stripes of all pages it wrote or only read, ascending, validate, install the
//                 new versions invisibly (tagged with the new epoch, db->epoch not yet advanced), unlock.
//                 Two commits touching a common page share a stripe, so their validate+install steps are
//                 serialised; commits on disjoint stripes run in parallel;
//    - epochs are assigned (and log offsets reserved) under seq_mu, a tiny critical section;
//    - the commit becomes visible only after its log record is persisted and its predecessor is visible
//      (multiwriter_log.c).
//  Memory ordering: db->epoch is stored with release semantics after all versions of that epoch are
//  installed; a reader takes its snapshot with acquire semantics and only looks at versions <= snapshot.
//

#include <string.h>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include "multiwriter_os.h"
#include "multiwriter_internal.h"

static uint64_t now_ns (void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

// MARK: - store lifecycle and chain access -

mw_store *mw_store_create (int pgsz, uint32_t base_dbsize) {
    mw_store *st = sqlite3_malloc(sizeof(*st));
    if (!st) return NULL;
    memset(st, 0, sizeof(*st));
    st->dir = calloc(MW_DIR_SIZE, sizeof(*st->dir));           // lazily zero-filled by the OS
    if (!st->dir) { sqlite3_free(st); return NULL; }
    for (int i = 0; i < MW_STRIPES; i++) pthread_mutex_init(&st->stripes[i].mu, NULL);
    pthread_mutex_init(&st->seq_mu, NULL);
    pthread_mutex_init(&st->list_mu, NULL);
    st->pgsz = pgsz;
    st->base_dbsize = base_dbsize;
    st->base_limit = 64ull << 20;
    return st;
}

mw_store *mw_store_create_light (int pgsz, uint32_t base_dbsize) {
    mw_store *st = sqlite3_malloc(sizeof(*st));
    if (!st) return NULL;
    memset(st, 0, sizeof(*st));
    for (int i = 0; i < MW_STRIPES; i++) pthread_mutex_init(&st->stripes[i].mu, NULL);
    pthread_mutex_init(&st->seq_mu, NULL);
    pthread_mutex_init(&st->list_mu, NULL);
    st->pgsz = pgsz;
    st->base_dbsize = base_dbsize;
    return st;
}

void mw_store_free (mw_store *st) {
    if (!st) return;
    for (uint32_t b = 0; st->dir && b < MW_DIR_SIZE; b++) {
        mw_chain *blk = atomic_load(&st->dir[b]);
        if (!blk) continue;
        for (uint32_t i = 0; i < MW_CHAIN_BLOCK; i++) {
            for (int j = 0; j < blk[i].n; j++) free(blk[i].v[j].data);
            free(blk[i].v);
        }
        free(blk);
    }
    free(st->dir);
    for (int i = 0; i < MW_STRIPES; i++) pthread_mutex_destroy(&st->stripes[i].mu);
    pthread_mutex_destroy(&st->seq_mu);
    pthread_mutex_destroy(&st->list_mu);
    sqlite3_free(st->sizes);
    sqlite3_free(st);
}

static int chain_reserve (mw_chain *c);
static uint64_t snap_for (const mw_validate *v, uint32_t pgno);
static int cmp_pg (const void *a, const void *b) { uint32_t x = *(const uint32_t *)a, y = *(const uint32_t *)b; return x < y ? -1 : x > y; }
static bool mw_debug_on (void) { static _Atomic int c = MW_KNOB_UNSET; return mw_knob_flag(&c, "MW_DEBUG"); }
static inline pthread_mutex_t *stripe_of (mw_store *st, uint32_t pgno) { return &st->stripes[pgno % MW_STRIPES].mu; }

// The chain of `pgno`; NULL if its block does not exist and !create (or on OOM). Blocks are never freed
// or moved, so the pointer stays valid; its contents are protected by the page's stripe.
static mw_chain *chain_get (mw_store *st, uint32_t pgno, bool create) {
    uint32_t b = pgno >> MW_CHAIN_BLOCK_BITS;
    if (b >= MW_DIR_SIZE) return NULL;
    mw_chain *blk = atomic_load_explicit(&st->dir[b], memory_order_acquire);
    if (!blk) {
        if (!create) return NULL;
        mw_chain *nb = calloc(MW_CHAIN_BLOCK, sizeof(mw_chain));
        if (!nb) return NULL;
        mw_chain *expected = NULL;
        if (!atomic_compare_exchange_strong(&st->dir[b], &expected, nb)) { free(nb); blk = expected; } else blk = nb;
    }
    return &blk[pgno & (MW_CHAIN_BLOCK - 1)];
}

mw_chain *mw_store_chain (mw_store *st, uint32_t pgno) { return chain_get(st, pgno, false); }

// index of the newest version with epoch <= snap, or -1
static int chain_find (const mw_chain *c, uint64_t snap) {
    int lo = 0, hi = c->n - 1, best = -1;
    while (lo <= hi) {
        int mid = (lo + hi) / 2;
        if (c->v[mid].epoch <= snap) { best = mid; lo = mid + 1; } else hi = mid - 1;
    }
    return best;
}

static inline uint64_t chain_head_epoch (const mw_chain *c) { return c && c->n > 0 ? c->v[c->n - 1].epoch : 0; }

int mw_store_read (mw_store *st, uint32_t pgno, uint64_t snap, uint32_t poff, uint32_t n, void *dst) {
    if (st->shared_db) return mw_shared_read(st->shared_db, pgno, snap, poff, n, dst);
    mw_chain *c = chain_get(st, pgno, false);
    if (!c) return 0;
    int found = 0;
    pthread_mutex_t *mu = stripe_of(st, pgno);
    mw_spinlock(mu);
    int i = chain_find(c, snap);
    if (i >= 0) { memcpy(dst, c->v[i].data + poff, n); found = 1; }
    pthread_mutex_unlock(mu);
    return found;
}

// A page read from the real file is remembered as an epoch-0 version so the next transaction (whose page
// cache is cold by design) copies it from memory instead of issuing a pread. Only when the chain is empty:
// a chain with real versions already answers every snapshot. GC drops it like any version; bounded by base_limit.
void mw_store_cache_base (mw_store *st, uint32_t pgno, const void *image) {
    if (st->shared_db) return;                                  // (shared mode: no private copies of anything)
    if (atomic_load_explicit(&st->base_bytes, memory_order_relaxed) + (uint64_t)st->pgsz > st->base_limit) return;
    mw_chain *c = chain_get(st, pgno, true);
    uint8_t *copy = malloc((size_t)st->pgsz);
    if (!c || !copy) { free(copy); return; }
    memcpy(copy, image, (size_t)st->pgsz);
    pthread_mutex_t *mu = stripe_of(st, pgno);
    mw_spinlock(mu);
    if (c->n == 0 && chain_reserve(c) == SQLITE_OK) {
        c->v[0].epoch = 0;
        c->v[0].data = copy;
        c->n = 1;
        copy = NULL;
        atomic_fetch_add(&st->base_bytes, (uint64_t)st->pgsz);
        atomic_fetch_add(&st->versions, 1);
        atomic_fetch_add(&st->bytes, (uint64_t)st->pgsz);
    }
    pthread_mutex_unlock(mu);
    free(copy);
}

static inline void size_note (mw_store *st, uint64_t epoch, uint32_t size) {       // (seq_mu held)
    uint32_t i = (uint32_t)(epoch & (MW_SIZE_RING - 1));
    atomic_store_explicit(&st->size_ring_size[i], size, memory_order_relaxed);
    atomic_store_explicit(&st->size_ring_epoch[i], epoch, memory_order_release);
}

uint32_t mw_store_dbsize (mw_store *st, uint64_t snap) {
    if (st->shared_db) return mw_shared_dbsize(st->shared_db, snap);
    uint32_t i = (uint32_t)(snap & (MW_SIZE_RING - 1));
    if (atomic_load_explicit(&st->size_ring_epoch[i], memory_order_acquire) == snap) {            // (lock-free: the slot still describes this epoch)
        uint32_t s = atomic_load_explicit(&st->size_ring_size[i], memory_order_relaxed);
        atomic_thread_fence(memory_order_acquire);
        if (atomic_load_explicit(&st->size_ring_epoch[i], memory_order_relaxed) == snap) return s;
    }
    mw_spinlock(&st->seq_mu);
    uint32_t size = st->base_dbsize;             // (records <= the compacted epoch were folded into base_dbsize)
    int lo = 0, hi = st->nsizes - 1, best = -1;
    while (lo <= hi) {
        int mid = (lo + hi) / 2;
        if (st->sizes[mid].epoch <= snap) { best = mid; lo = mid + 1; } else hi = mid - 1;
    }
    if (best >= 0) size = st->sizes[best].dbsize;
    pthread_mutex_unlock(&st->seq_mu);
    return size;
}

// MARK: - intrusive lists (no allocation: cannot fail) -

#define LIST_PUSH(st, headfield, linkfield, pgno, chain) do { \
    pthread_mutex_lock(&(st)->list_mu); \
    (chain)->linkfield = (st)->headfield; \
    (st)->headfield = (pgno) + 1; \
    pthread_mutex_unlock(&(st)->list_mu); } while (0)

// MARK: - publication gate -
//
// Fairness. A rebase replays a transaction at the latest snapshot and can lose the race against ordinary
// commits over and over (measured: hundreds of attempts under contention). After a few losses it *closes*
// the gate: publishers already inside validate+install finish, new ones wait at the entrance, and the
// rebase's next attempt runs against a frozen state and therefore succeeds. The open-path cost is two
// atomic operations per commit.

void mw_gate_enter (mw_db *db, mw_lane *lane) {
    for (unsigned spins = 0;; spins++) {
        if (atomic_load(&db->gate_closing) == 0 || (lane && db->gate_owner == lane)) {
            atomic_fetch_add(&db->gate_active, 1);                       // (seq_cst: pairs with the closer's store/load order)
            if (atomic_load(&db->gate_closing) == 0 || (lane && db->gate_owner == lane)) return;
            atomic_fetch_sub(&db->gate_active, 1);                       // lost the race with the closer: back off
        }
        if (spins < 400) { sched_yield(); continue; }                      // (the gate is closed for a replay of some tens of microseconds: a sleep of 20 us on a machine that rounds it up would be longer than the closure)
        struct timespec ts = { 0, 20000 };
        nanosleep(&ts, NULL);
    }
}

void mw_gate_exit (mw_db *db) { atomic_fetch_sub(&db->gate_active, 1); }

void mw_gate_close (mw_db *db, mw_lane *owner) {
    db->gate_owner = owner;
    atomic_store(&db->gate_closing, 1);
    uint64_t tg0 = now_ns(); bool warned = false;
    while (atomic_load(&db->gate_active) > 0) { sched_yield(); if (!warned && now_ns() - tg0 > 3000000000ull && mw_debug_on()) { warned = true; fprintf(stderr, "gate_close: stuck waiting for publishers (active=%d)\n", atomic_load(&db->gate_active)); } }             // let in-flight publishers finish
    // ... and let every epoch already assigned become visible, so the next snapshot is the final state
    // shared mode: commits are visible in one step under the publication lock; the local epoch counters do not follow other processes'
    while (!db->mp_req && atomic_load(&db->epoch) != atomic_load(&db->next_epoch) && !atomic_load(&db->failed)) { sched_yield(); if (!warned && now_ns() - tg0 > 3000000000ull && mw_debug_on()) { warned = true; fprintf(stderr, "gate_close: stuck waiting for epoch %llu to become visible (next_epoch %llu)\n", (unsigned long long)atomic_load(&db->epoch), (unsigned long long)atomic_load(&db->next_epoch)); } }
    atomic_fetch_add(&db->n_gate_closures, 1);
}

void mw_gate_open (mw_db *db) {
    atomic_store(&db->gate_closing, 0);
    db->gate_owner = NULL;
}

// MARK: - install / commit protocol -

// Room for one more version in each chain, allocated *before* validation so the install step cannot fail.
static int chain_reserve (mw_chain *c) {
    if (c->n < c->cap) return SQLITE_OK;
    int cap = c->cap ? c->cap * 2 : 2;
    mw_pv *p = realloc(c->v, (size_t)cap * sizeof(mw_pv));
    if (!p) return SQLITE_NOMEM;
    c->v = p;
    c->cap = cap;
    return SQLITE_OK;
}

// Append a version (capacity reserved). Caller holds the page's stripe.
// Pages to add to the store's lists (GC candidates, dirty since the last compaction) are chained locally and spliced in with one lock of list_mu per commit
// (they used to be pushed one by one: ~14 acquisitions of a process-wide mutex per commit, under the stripe locks and the relocation mutex).
typedef struct { uint32_t head; mw_chain *tail; bool cand_tail_dirty; } lpush;
static void chain_install_batched (mw_store *st, uint32_t pgno, mw_chain *c, uint64_t epoch, uint8_t *data, lpush *cand, lpush *dirty) {
    if (c->n >= 1 && !c->queued) { c->queued = 1; c->cand_next = cand->head; if (!cand->head) cand->tail = c; cand->head = pgno + 1; }     // 2nd version: GC candidate
    if (!c->dirty) { c->dirty = 1; c->dirty_next = dirty->head; if (!dirty->head) dirty->tail = c; dirty->head = pgno + 1; }
    if (pgno == 1) { atomic_store_explicit(&st->p1_cookie, (((uint32_t)data[40] << 24) | ((uint32_t)data[41] << 16) | ((uint32_t)data[42] << 8) | (uint32_t)data[43]), memory_order_relaxed); atomic_store_explicit(&st->p1_head_epoch, epoch, memory_order_release); }
    c->v[c->n].epoch = epoch;
    c->v[c->n].data = data;
    c->n++;
    atomic_fetch_add_explicit(&st->versions, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&st->versions_allocated, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&st->bytes, (uint64_t)st->pgsz, memory_order_relaxed);
}
static void lists_splice (mw_store *st, lpush *cand, lpush *dirty) {
    if (!cand->head && !dirty->head) return;
    mw_spinlock(&st->list_mu);
    if (cand->head) { cand->tail->cand_next = st->cand_head; st->cand_head = cand->head; }
    if (dirty->head) { dirty->tail->dirty_next = st->dirty_head; st->dirty_head = dirty->head; }
    pthread_mutex_unlock(&st->list_mu);
}
static void chain_install (mw_store *st, uint32_t pgno, mw_chain *c, uint64_t epoch, uint8_t *data) {
    if (c->n >= 1 && !c->queued) { c->queued = 1; LIST_PUSH(st, cand_head, cand_next, pgno, c); }   // 2nd version: GC candidate
    if (!c->dirty) { c->dirty = 1; LIST_PUSH(st, dirty_head, dirty_next, pgno, c); }
    if (pgno == 1) { atomic_store_explicit(&st->p1_cookie, (((uint32_t)data[40] << 24) | ((uint32_t)data[41] << 16) | ((uint32_t)data[42] << 8) | (uint32_t)data[43]), memory_order_relaxed); atomic_store_explicit(&st->p1_head_epoch, epoch, memory_order_release); }
    c->v[c->n].epoch = epoch;
    c->v[c->n].data = data;
    c->n++;
    atomic_fetch_add(&st->versions, 1);
    atomic_fetch_add(&st->versions_allocated, 1);
    atomic_fetch_add(&st->bytes, (uint64_t)st->pgsz);
}

// Locks the stripes of `n1` + `n2` pages in ascending stripe order. Returns a bitmap of what was locked.
typedef struct { uint64_t bits[MW_STRIPES / 64]; } stripe_set;
static void stripes_lock (mw_store *st, stripe_set *ss, const uint32_t *a, int na, const uint32_t *b, int nb) {
    memset(ss, 0, sizeof *ss);
    for (int i = 0; i < na; i++) { unsigned s = a[i] % MW_STRIPES; ss->bits[s / 64] |= 1ull << (s % 64); }
    for (int i = 0; i < nb; i++) { unsigned s = b[i] % MW_STRIPES; ss->bits[s / 64] |= 1ull << (s % 64); }
    for (unsigned w = 0; w < MW_STRIPES / 64; w++)
        for (uint64_t m = ss->bits[w]; m; m &= m - 1) mw_spinlock(&st->stripes[w * 64 + (unsigned)__builtin_ctzll(m)].mu);
}
static void stripes_unlock (mw_store *st, const stripe_set *ss) {
    for (unsigned w = 0; w < MW_STRIPES / 64; w++)
        for (uint64_t m = ss->bits[w]; m; m &= m - 1) pthread_mutex_unlock(&st->stripes[w * 64 + (unsigned)__builtin_ctzll(m)].mu);
}
static void pub_release (mw_db *db, mw_store *st, const stripe_set *ss) { stripes_unlock(st, ss); if (!db->mp) mw_gate_exit(db); }

static int publish_impl (mw_db *db, mw_lane *lane, const mw_validate *v, const uint32_t *pgnos, const uint8_t *const *images, int n, uint32_t ws_dbsize, uint32_t snap_dbsize, int sync, uint64_t *out_epoch) {
    mw_store *st = db->store;
    int rc = SQLITE_OK;
    const bool adopt = v && v->adopt_images;                    // the caller hands over its images (individually malloc'ed): they become the store's copies, or are freed
    #define ADOPT_FREE_ALL(from) do { if (adopt) for (int _i = (from); _i < n; _i++) free((void *)images[_i]); } while (0)
    if (atomic_load(&db->failed)) { ADOPT_FREE_ALL(0); return SQLITE_IOERR; }          // sticky failure: recover by reopening

    // allocate and copy the images, and resolve the chains, outside any lock
    uint8_t *copies_stack[16]; mw_chain *chains_stack[16];                           // (small commits: no heap allocation for the bookkeeping arrays)
    uint8_t **copies = n <= 16 ? copies_stack : malloc((size_t)n * sizeof(uint8_t *));
    mw_chain **chains = n <= 16 ? chains_stack : malloc((size_t)n * sizeof(mw_chain *));
    if (!copies || !chains) { if (n > 16) { free(copies); free(chains); } ADOPT_FREE_ALL(0); return SQLITE_NOMEM; }
    int ncopied = 0;
    for (; ncopied < n; ncopied++) {
        copies[ncopied] = adopt ? (uint8_t *)images[ncopied] : malloc((size_t)st->pgsz);
        chains[ncopied] = chain_get(st, pgnos[ncopied], true);
        if (!copies[ncopied] || !chains[ncopied]) { rc = SQLITE_NOMEM; ncopied++; break; }
        if (!adopt) memcpy(copies[ncopied], images[ncopied], (size_t)st->pgsz);
    }
    if (rc == SQLITE_OK && mw_fault_hit(MW_FAULT_ALLOC_ERR)) rc = SQLITE_NOMEM;
    if (rc == SQLITE_OK && db->has_log) { mw_log_decide_mode(db, sync); rc = mw_log_ensure_room(db, mw_log_record_size(db, n, 0)); }       // (no room on the disk: this commit fails, before it takes an epoch)
    if (rc != SQLITE_OK) goto fail_free;

    // ---- lock the stripes of every page written or read (ascending) --------------------------------
    stripe_set locked;
    uint64_t tl0 = MW_T0();
    if (!db->mp) mw_gate_enter(db, lane);                       // (multi-process: the wrapper enters the gate BEFORE the publication lock)
    stripes_lock(st, &locked, pgnos, n, v ? v->read_pgnos : NULL, v ? v->n_read : 0);
    MW_T1(MW_ST_LOCKS, tl0);

    for (int i = 0; i < n && rc == SQLITE_OK; i++) rc = chain_reserve(chains[i]);   // the install below cannot fail
    if (rc != SQLITE_OK) { pub_release(db, st, &locked); goto fail_free; }
    if (lane && lane->holds_reloc) MW_T1(MW_ST_H_STRIPES, tl0);
    uint64_t tval0 = MW_T0();

    // ---- validate ----------------------------------------------------------------------------------
    if (v) {
        // read dependencies first: a page the transaction only *read* changed => its computation may be
        // stale. That is never rebased (the replay would not redo the reads); the caller must retry.
        uint32_t *used = NULL; int nused = 0, reserved = 0; bool used_built = false;        // pages the transaction went through (read or wrote), sorted; built when first needed
        for (int i = 0; i < v->n_read && rc == SQLITE_OK; i++) {
            mw_chain *c = chain_get(st, v->read_pgnos[i], false);
            if (chain_head_epoch(c) > snap_for(v, v->read_pgnos[i])) {
                if (lane && lane->file && !lane->noroute && c && c->n > 0) {
                    // Interior page changed: is what this transaction took from it (which child to follow) unchanged?
                    if (!used_built) {
                        used_built = true;
                        reserved = st->reserved;
                        used = malloc((size_t)(v->n_read + n + 1) * sizeof(uint32_t));
                        if (used) { for (int k = 0; k < v->n_read; k++) used[nused++] = v->read_pgnos[k]; for (int k = 0; k < n; k++) used[nused++] = pgnos[k]; qsort(used, (size_t)nused, sizeof(uint32_t), cmp_pg); }
                    }
                    int oi = chain_find(c, snap_for(v, v->read_pgnos[i]));
                    uint8_t *oldimg = NULL; const uint8_t *oldp = NULL;
                    if (oi >= 0) oldp = c->v[oi].data;
                    else if ((oldimg = malloc((size_t)st->pgsz)) != NULL) {                 // no version at the snapshot in memory: the real file's page
                        mw_file *f = lane->file;
                        if (f->real->pMethods->xRead(f->real, oldimg, st->pgsz, (sqlite3_int64)(v->read_pgnos[i] - 1) * st->pgsz) == SQLITE_OK) oldp = oldimg;
                    }
                    bool same = used && oldp && mw_interior_routes_same(oldp, c->v[c->n - 1].data, st->pgsz, reserved, used, nused);
                    free(oldimg);
                    if (same) { atomic_fetch_add(&db->n_reads_saved, 1); continue; }
                }
                if (mw_debug_on()) fprintf(stderr, "read conflict on page %u (newest %llu > snapshot %llu)\n", v->read_pgnos[i], (unsigned long long)chain_head_epoch(c), (unsigned long long)v->snapshot_epoch);
                atomic_fetch_add(&db->n_read_conflicts, 1);
                rc = MW_CONFLICT_READ;
            }
        }
        if (mw_debug_on()) { for (int i = 0; i < n; i++) if (chain_head_epoch(chains[i]) > snap_for(v, pgnos[i])) fprintf(stderr, "CPAGE %u snapdb=%u ws_db=%u newpage=%d\n", pgnos[i], snap_dbsize, ws_dbsize, pgnos[i] > snap_dbsize); }
        for (int i = 0; i < n && rc == SQLITE_OK; i++) {
            if (chain_head_epoch(chains[i]) > snap_for(v, pgnos[i])) { atomic_fetch_add(&db->n_page_conflicts, 1); rc = MW_CONFLICT; if (mw_debug_on()) fprintf(stderr, "CONFLICT page %u of %d\n", pgnos[i], n); }
        }
        free(used);
        // a schema change invalidates everything the transaction did, even on other pages
        if (rc == SQLITE_OK && v->check_cookie) {
            // (page 1's chain is not read here: its stripe is not necessarily held, and a publisher installing page 1, or the GC, reallocates or shifts it)
            if (atomic_load_explicit(&st->p1_head_epoch, memory_order_acquire) > snap_for(v, 1)) {
                uint32_t latest = atomic_load_explicit(&st->p1_cookie, memory_order_relaxed);
                if (latest != v->cookie) { atomic_fetch_add(&db->n_schema_conflicts, 1); rc = MW_CONFLICT_SCHEMA; }
            }
        }
        if (rc != SQLITE_OK) { pub_release(db, st, &locked); goto fail_free; }
    }

    if (lane && lane->holds_reloc) MW_T1(MW_ST_H_VALIDATE, tval0);
    // ---- assign the epoch (and the log offset, and the size record): the serialisation point --------
    uint64_t tsq0 = MW_T0();
    if (db->has_log) mw_log_decide_mode(db, sync);
    mw_spinlock(&st->seq_mu);
    uint64_t epoch = atomic_load(&db->next_epoch) + 1;
    if (st->nsizes == st->sizes_cap) {
        int cap = st->sizes_cap ? st->sizes_cap * 2 : 64;
        mw_sizerec *p = sqlite3_realloc64(st->sizes, (sqlite3_uint64)cap * sizeof(mw_sizerec));
        if (!p) { pthread_mutex_unlock(&st->seq_mu); pub_release(db, st, &locked); rc = SQLITE_NOMEM; goto fail_free; }
        st->sizes = p;
        st->sizes_cap = cap;
    }
    // A transaction that did not change the file size must not roll the recorded size back to its
    // (possibly stale) view: growth/shrink always writes page 1, which is what conflicts.
    uint32_t last_size = st->nsizes ? st->sizes[st->nsizes - 1].dbsize : st->base_dbsize;
    uint32_t new_dbsize = (ws_dbsize == snap_dbsize) ? last_size : ws_dbsize;
    st->sizes[st->nsizes].epoch = epoch;
    st->sizes[st->nsizes].dbsize = new_dbsize;
    size_note(st, epoch, new_dbsize);
    uint64_t log_off = 0, log_end = 0;                           // log_end: the log's size after our record, taken under seq_mu (the compaction check below must not read db->log_off unlocked)
    if (db->has_log) { log_off = db->log_off; __atomic_store_n(&db->log_off, log_off + mw_log_record_size(db, n, 0), __ATOMIC_RELAXED); mw_log_reserve_space(db); log_end = db->log_off; }
    st->sizes[st->nsizes].log_off = log_off;
    st->nsizes++;
    atomic_store(&db->next_epoch, epoch);                        // assigned; NOT visible yet (db->epoch is untouched)
    pthread_mutex_unlock(&st->seq_mu);
    if (lane && lane->holds_reloc) MW_T1(MW_ST_H_SEQ, tsq0);
    uint64_t tin0 = MW_T0();

    // (what the install changes outside the chains, for the case that the record cannot be written and the commit is taken back: the head of page 1 and its cookie are the ones the next commits validate against)
    const uint64_t p1_prev_head = atomic_load_explicit(&st->p1_head_epoch, memory_order_acquire); const uint32_t p1_prev_cookie = atomic_load_explicit(&st->p1_cookie, memory_order_relaxed);
    bool wrote_p1 = false; for (int i = 0; i < n; i++) if (pgnos[i] == 1) wrote_p1 = true;
    lpush lc = {0}, ld = {0};
    for (int i = 0; i < n; i++) chain_install_batched(st, pgnos[i], chains[i], epoch, copies[i], &lc, &ld);
    lists_splice(st, &lc, &ld);                                  // (still under the stripe locks: the lists never miss a page a compactor could look for)
    if (lane && lane->holds_reloc && !db->mp) {                  // hand the new page 1 to the next relocation (it is the newest version: nobody else wrote page 1 since)
        for (int i = 0; i < n; i++) if (pgnos[i] == 1 && db->p1_cache) { memcpy(db->p1_cache, copies[i], (size_t)st->pgsz); db->p1_cache_epoch = epoch; break; }
    }
    pub_release(db, st, &locked);
    if (lane && lane->holds_reloc) MW_T1(MW_ST_H_INSTALL, tin0);
    if (lane && lane->holds_reloc) { lane->holds_reloc = false; MW_T1(MW_ST_RELOCHOLD, lane->reloc_t0); pthread_mutex_unlock(&db->reloc_mu); }      // (a relocation: the next one may go, our pages are installed)
    if (n > 16) { free(copies); free(chains); }

    // ---- outside the locks: persist, then become visible (in epoch order) --------------------------
    mw_fault_hit(MW_CRASH_BEFORE_LOG);
    uint64_t ta0 = MW_T0();
    rc = db->has_log ? mw_log_append(db, log_off, epoch, new_dbsize, n, pgnos, images, NULL, 0, sync) : SQLITE_OK;
    MW_T1(MW_ST_APPEND, ta0);
    if (rc != SQLITE_OK) {
        // The record never made it. If nothing was assigned after us we can take the commit back cleanly:
        // uninstall, return the log space. Otherwise a successor may already sit behind the hole: the
        // database is failed (sticky) and recovers by reopening (the log stops at the hole).
        mw_spinlock(&st->seq_mu);
        bool latest = atomic_load(&db->next_epoch) == epoch && MW_LOG_OFF(db) == log_off + mw_log_record_size(db, n, 0);
        if (latest) {
            st->nsizes--;
            atomic_store(&db->next_epoch, epoch - 1);
            __atomic_store_n(&db->log_off, log_off, __ATOMIC_RELAXED);
        } else {
            atomic_store(&db->failed, 1);
        }
        pthread_mutex_unlock(&st->seq_mu);
        if (latest) {
            stripe_set again;
            stripes_lock(st, &again, pgnos, n, NULL, 0);
            for (int i = 0; i < n; i++) {
                mw_chain *c = chain_get(st, pgnos[i], false);
                if (c && c->n > 0 && c->v[c->n - 1].epoch == epoch) {
                    free(c->v[--c->n].data);
                    atomic_fetch_sub(&st->versions, 1); atomic_fetch_sub(&st->versions_allocated, 1); atomic_fetch_sub(&st->bytes, (uint64_t)st->pgsz);
                }
            }
            if (wrote_p1 && atomic_load_explicit(&st->p1_head_epoch, memory_order_acquire) == epoch) {       // (page 1 of the commit that never was must not be the head that later commits see, or a relocation's copy of it)
                atomic_store_explicit(&st->p1_cookie, p1_prev_cookie, memory_order_relaxed);
                atomic_store_explicit(&st->p1_head_epoch, p1_prev_head, memory_order_release);
            }
            if (db->p1_cache_epoch == epoch) db->p1_cache_epoch = 0;
            stripes_unlock(st, &again);
        }
        mw_db_wake_all_visibility(db);
        return rc;
    }
    if (sync && !db->mp) {                                       // (multi-process: the wrapper fsyncs after releasing the publication lock)
        uint64_t s0 = now_ns();
        uint64_t ts0 = MW_T0();
        rc = mw_log_sync(db, epoch, log_off + mw_log_record_size(db, n, 0));
        MW_T1(MW_ST_SYNC, ts0);
        atomic_fetch_add(&db->n_log_sync_ns, now_ns() - s0);
        if (rc != SQLITE_OK) {                                   // outcome uncertain: stop accepting commits
            atomic_store(&db->failed, 1);
            mw_db_wake_all_visibility(db);
            return rc;
        }
    }
    mw_fault_hit(MW_CRASH_AFTER_LOG);
    uint64_t tv0 = MW_T0();
    rc = mw_db_make_visible(db, epoch);
    MW_T1(MW_ST_VISIBLE, tv0);
    if (rc != SQLITE_OK) return rc;
    mw_fault_hit(MW_CRASH_AFTER_VISIBLE);

    // the log only shrinks through compaction: ask the compactor, and throttle if it cannot keep up
    // (e.g. a long-lived reader pins the compaction target)
    if (db->log_max_bytes && log_end > mw_log_limit(db)) {
        mw_db_compactor_kick(db);
        if (log_end > db->log_max_bytes * 16) {
            // in proportion to the overshoot (a fixed half millisecond does not slow eight writers at all): ratio 1 = 0.5 ms, 2 = 4 ms, 3 = 13 ms, at most 50 ms; the mapping of the
            // log is 1 GB and the compaction can be held back by the metadata flush, so the writers must not outrun it
            atomic_fetch_add(&db->n_backpressure, 1);
            double r = (double)log_end / ((double)db->log_max_bytes * 16.0);
            double us = 500.0 * r * r * r; if (us > 50000.0) us = 50000.0;
            struct timespec ts = { 0, (long)(us * 1000.0) };
            nanosleep(&ts, NULL);
        }
        // the mapping of the log is 1 GB and the code that reads the log through it does not know of records beyond: past 768 MB a commit waits until the compaction has made room (the compactor is asked every time round); a compaction that cannot run for 20 s lets the commit through (the writers must not wait for ever)
        if (log_end > MW_LOG_HARD_BYTES) {
            for (int w = 0; w < 10000; w++) {
                mw_db_compactor_kick(db);
                struct timespec ts = { 0, 2000000 }; nanosleep(&ts, NULL);
                if (mw_log_end_locked(db) <= MW_LOG_HARD_BYTES - (64ull << 20)) break;
            }
        }
    }
    atomic_fetch_add(&db->n_commits, 1);
    atomic_fetch_add(&db->n_fast_commits, 1);
    atomic_fetch_add(&db->n_pages_published, (uint64_t)n);
    if (out_epoch) *out_epoch = epoch;
    if (db->gc_interval > 0 && (atomic_fetch_add(&db->publishes_since_gc, 1) + 1) >= (uint64_t)db->gc_interval) mw_db_gc(db);
    return SQLITE_OK;

fail_free:
    for (int i = 0; i < ncopied; i++) free(copies[i]);
    ADOPT_FREE_ALL(ncopied);
    if (n > 16) { free(copies); free(chains); }
    return rc;
}

// Admission limit for synchronous<FULL (MW_ADMIT; 0 = unlimited). Default: the number of cores, at most 16 and at least 4 (measured: 64 agents 34k -> 49k with 16 on 18 cores).
static int admit_cap (void) { static _Atomic int c = MW_KNOB_UNSET; long n = sysconf(_SC_NPROCESSORS_ONLN); return mw_knob_int(&c, "MW_ADMIT", n < 4 ? 4 : n > 16 ? 16 : (int)n); }

int mw_db_publish (mw_db *db, mw_lane *lane, const mw_validate *v, const uint32_t *pgnos, const uint8_t *const *images, int n, uint32_t ws_dbsize, uint32_t snap_dbsize, int sync, uint64_t *out_epoch) {
    if (!db->mp) {
        int cap = admit_cap();
        if (sync || cap <= 0) return publish_impl(db, lane, v, pgnos, images, n, ws_dbsize, snap_dbsize, sync, out_epoch);
        // synchronous=off/normal has no group commit to pace the committers: with more runnable committers than cores they all fight for the relocation mutex and the
        // stripes (§32). Let at most `cap` of them into publish; the rest sleep here. Every epoch holder is inside, so the visibility chain cannot deadlock on us.
        pthread_mutex_lock(&db->admit_mu);
        while (db->admit_in >= cap) { db->admit_waiting++; pthread_cond_wait(&db->admit_cv, &db->admit_mu); db->admit_waiting--; }
        db->admit_in++;
        pthread_mutex_unlock(&db->admit_mu);
        int rc = publish_impl(db, lane, v, pgnos, images, n, ws_dbsize, snap_dbsize, sync, out_epoch);
        pthread_mutex_lock(&db->admit_mu);
        db->admit_in--;
        if (db->admit_waiting > 0) pthread_cond_signal(&db->admit_cv);
        pthread_mutex_unlock(&db->admit_mu);
        return rc;
    }
    if (lane && lane->mp_held) {                                 // (relocation: gate and lock are ours already; the caller finishes with mw_db_publish_finish)
        uint64_t my_epoch = 0;
        int rc = mw_shared_publish(db, lane, v, pgnos, images, n, ws_dbsize, snap_dbsize, sync, &my_epoch);
        if (rc == SQLITE_OK && out_epoch) *out_epoch = my_epoch;
        return rc;
    }
    // A doomed attempt must not use the lock: if a page we wrote has a newer committed version than our snapshot, the validation inside would refuse us. With 32 processes
    // that was 4-5 of every 5-6 lock rounds (a growing transaction always writes page 1, which every commit changes).
    for (int i = 0; i < n; i++) {
        if (mw_store_head_epoch(db->store, pgnos[i]) > snap_for(v, pgnos[i])) {
            atomic_fetch_add(&db->n_page_conflicts, 1);
            if (v && v->adopt_images) for (int k = 0; k < n; k++) free((void *)images[k]);
            return MW_CONFLICT;
        }
    }
    mw_gate_enter(db, lane);                                     // gate first, then the lock: a starving rebase closes the gate and its helper needs the lock
    uint64_t tm0 = MW_T0();
    mw_mp_lock(db);                                              // multi-process: one publisher at a time across all processes
    MW_T1(MW_ST_MP_WAIT, tm0);
    tm0 = MW_T0();
    uint64_t my_epoch = 0;
    int rc = mw_shared_publish(db, lane, v, pgnos, images, n, ws_dbsize, snap_dbsize, sync, &my_epoch);
    MW_T1(MW_ST_MP_HELD, tm0);
    mw_mp_unlock(db);
    mw_gate_exit(db);
    mw_log_prefill_bg(db);                                       // keep the log file written ahead of its end (outside the lock)
    if (out_epoch && rc == SQLITE_OK) *out_epoch = my_epoch;
    return mw_db_publish_finish(db, lane, rc, my_epoch, sync);
}

int mw_db_publish_finish (mw_db *db, mw_lane *lane, int rc, uint64_t my_epoch, int sync) {
    if (lane && lane->bp_wait_us) { struct timespec bp = { 0, (long)lane->bp_wait_us * 1000L }; lane->bp_wait_us = 0; nanosleep(&bp, NULL); }      // back-pressure: the log is far ahead of its compaction; wait here, not under the publication lock
    if (rc == SQLITE_OK) {
        // Cross-process group commit: the record is already visible to the other processes; this commit is acknowledged
        // only after an fsync that started after it was written, which also covers every earlier record of the file.
        // (A power failure can therefore lose a commit another process has *read*, never one that was acknowledged.)
        if (sync) {
            uint64_t s0 = now_ns();
            rc = db->mp_req ? mw_shared_sync(db, lane) : mw_log_sync(db, my_epoch, 0);
            atomic_fetch_add(&db->n_log_sync_ns, now_ns() - s0);
            if (rc != SQLITE_OK) { atomic_store(&db->failed, 1); mw_db_wake_all_visibility(db); }
        }
    }
    return rc;
}

// Recovery: install a commit read back from the log (single-threaded, before the database is used).
int mw_store_install_recovered (mw_store *st, uint64_t epoch, uint32_t dbsize, int n, const uint32_t *pgnos, const uint8_t *const *images) {
    for (int i = 0; i < n; i++) {
        mw_chain *c = chain_get(st, pgnos[i], true);
        uint8_t *copy = malloc((size_t)st->pgsz);
        if (!c || !copy) { free(copy); return SQLITE_NOMEM; }
        memcpy(copy, images[i], (size_t)st->pgsz);
        pthread_mutex_t *mu = stripe_of(st, pgnos[i]);
        mw_spinlock(mu);
        if (chain_reserve(c) != SQLITE_OK) { pthread_mutex_unlock(mu); free(copy); return SQLITE_NOMEM; }
        chain_install(st, pgnos[i], c, epoch, copy);
        pthread_mutex_unlock(mu);
    }
    mw_spinlock(&st->seq_mu);
    if (st->nsizes == st->sizes_cap) {
        int cap = st->sizes_cap ? st->sizes_cap * 2 : 64;
        mw_sizerec *p = sqlite3_realloc64(st->sizes, (sqlite3_uint64)cap * sizeof(mw_sizerec));
        if (!p) { pthread_mutex_unlock(&st->seq_mu); return SQLITE_NOMEM; }
        st->sizes = p;
        st->sizes_cap = cap;
    }
    st->sizes[st->nsizes].epoch = epoch;
    st->sizes[st->nsizes].dbsize = dbsize;
    size_note(st, epoch, dbsize);
    st->sizes[st->nsizes].log_off = 0;                           // (recovery knows the offsets; rewrite needs them only for new commits)
    st->nsizes++;
    pthread_mutex_unlock(&st->seq_mu);
    return SQLITE_OK;
}

// The epoch a page is validated against: the snapshot, or our own latest commit for a page this snapshot already committed.
uint64_t mw_snap_for (const mw_validate *v, uint32_t pgno);
static uint64_t snap_for (const mw_validate *v, uint32_t pgno) { return mw_snap_for(v, pgno); }
uint64_t mw_snap_for (const mw_validate *v, uint32_t pgno) {
    int lo = 0, hi = v->own_n - 1;
    while (lo <= hi) {
        int mid = (lo + hi) / 2;
        if (v->own_pgnos[mid] == pgno) return v->own_epochs ? v->own_epochs[mid] : v->own_epoch;
        if (v->own_pgnos[mid] < pgno) lo = mid + 1; else hi = mid - 1;
    }
    return v->snapshot_epoch;
}

uint64_t mw_store_head_epoch (mw_store *st, uint32_t pgno) {
    if (st->shared_db) return mw_shared_head_epoch(st->shared_db, pgno);
    mw_chain *c = chain_get(st, pgno, false);
    if (!c) return 0;
    pthread_mutex_t *mu = stripe_of(st, pgno);
    mw_spinlock(mu);
    uint64_t e = chain_head_epoch(c);
    pthread_mutex_unlock(mu);
    return e;
}

// The newest committed image of `pgno` and the epoch it was committed at (0 = the real file's page), read atomically.
bool mw_store_head_image (mw_store *st, uint32_t pgno, uint8_t *dst, uint64_t *epoch) {
    if (st->shared_db) return mw_shared_head_image(st->shared_db, pgno, dst, epoch);
    mw_chain *c = chain_get(st, pgno, false);
    if (!c) return false;
    bool found = false;
    pthread_mutex_t *mu = stripe_of(st, pgno);
    mw_spinlock(mu);
    if (c->n > 0) { memcpy(dst, c->v[c->n - 1].data, (size_t)st->pgsz); *epoch = c->v[c->n - 1].epoch; found = true; }
    pthread_mutex_unlock(mu);
    return found;
}

// MARK: - garbage collection -

// A version is reclaimable when a newer version is visible to every snapshot that can still exist
// (epoch <= oldest_active): nobody can then observe it. If the newest version itself is already
// materialised in the real file (epoch <= compacted_epoch) and old enough, the whole chain goes.
//
// `oldest` is computed *before* touching the store. It never decreases (new snapshots take the current
// visible epoch, which is >= any earlier oldest), so a stale value is merely conservative.
uint64_t mw_db_gc (mw_db *db) {
    mw_store *st = db->store;
    if (!st || db->mp_req) return 0;                          // (shared mode: the publisher collects the index, shared_gc)
    atomic_store(&db->publishes_since_gc, 0);
    uint64_t t0 = now_ns();
    uint64_t oldest = mw_db_oldest_active_snapshot(db);
    uint64_t reclaimed = 0;

    mw_spinlock(&st->seq_mu);
    uint64_t compacted = st->compacted_epoch;
    pthread_mutex_unlock(&st->seq_mu);

    mw_spinlock(&st->list_mu);                            // detach the candidate list, then work on it lock-free
    uint32_t head = st->cand_head;
    st->cand_head = 0;
    pthread_mutex_unlock(&st->list_mu);

    uint32_t def_head = 0; mw_chain *def_tail = NULL;               // (chains whose stripe a committer holds right now: not waited for, they are looked at by the next collection)
    while (head) {
        uint32_t pgno = head - 1;
        mw_chain *c = chain_get(st, pgno, false);
        pthread_mutex_t *mu = stripe_of(st, pgno);
        if (pthread_mutex_trylock(mu) != 0) {
            head = c->cand_next;                                    // (the chain is queued, so the list link is ours alone while we hold the detached list)
            c->cand_next = def_head; def_head = pgno + 1; if (!def_tail) def_tail = c;
            continue;
        }
        head = c->cand_next;
        int i = chain_find(c, oldest);                           // newest version visible to the oldest snapshot
        int drop = i > 0 ? i : 0;                                // versions [0, drop) are invisible to everyone
        if (c->n > 0 && c->v[c->n - 1].epoch <= oldest && c->v[c->n - 1].epoch <= compacted) drop = c->n;
        for (int j = 0; j < drop; j++) {
            if (c->v[j].epoch == 0) atomic_fetch_sub(&st->base_bytes, (uint64_t)st->pgsz);
            if (c->v[j].data) { free(c->v[j].data); atomic_fetch_sub(&st->bytes, (uint64_t)st->pgsz); }
            atomic_fetch_sub(&st->versions, 1);
            atomic_fetch_add(&st->versions_reclaimed, 1);
            reclaimed++;
        }
        if (drop) { memmove(c->v, c->v + drop, (size_t)(c->n - drop) * sizeof(mw_pv)); c->n -= drop; }
        if (c->n > 0) { c->cand_next = def_head; def_head = pgno + 1; if (!def_tail) def_tail = c; }    // (kept on the local list, given back with one lock at the end) still pinned by an old snapshot, or not yet materialised in the real file: stay a candidate
        else { c->queued = 0; free(c->v); c->v = NULL; c->cap = 0; }   // (a page with no version in memory keeps no array of them: a database of millions of pages that were each written once held 150-200 MB of them)
        pthread_mutex_unlock(mu);
    }
    if (def_head) { mw_spinlock(&st->list_mu); def_tail->cand_next = st->cand_head; st->cand_head = def_head; pthread_mutex_unlock(&st->list_mu); }
    // size records: keep the newest one visible to `oldest` and everything after it
    mw_spinlock(&st->seq_mu);
    int si = -1;
    for (int i = 0; i < st->nsizes && st->sizes[i].epoch <= oldest; i++) si = i;
    if (si > 0) { memmove(st->sizes, st->sizes + si, (size_t)(st->nsizes - si) * sizeof(mw_sizerec)); st->nsizes -= si; }
    pthread_mutex_unlock(&st->seq_mu);
    atomic_fetch_add(&st->gc_runs, 1);
    atomic_fetch_add(&st->gc_ns, now_ns() - t0);
    return reclaimed;
}

