//
//  multiwriter_reloc.c
//  sqlite-multiwriter
//
//  Page relocation: a commit that conflicts *only because other commits extended the file*.
//
//  Two transactions that start from the same snapshot and both append to the database (new leaf pages after a split, new overflow pages,
//  new index pages) allocate the same new page numbers (dbsize + 1, ...) and both rewrite page 1 (file change counter, in-header size).
//  Physically they conflict, although what they changed is disjoint: stock SQLite avoids it only by running one writer at a time.
//
//  Everything that refers to a page this transaction *created* lives in pages this transaction wrote (a page nobody has committed yet cannot
//  be referenced by anything else), so the new pages can be renumbered above the current end of the file if the references are rewritten:
//      - child pointers and the right-most pointer of interior b-tree pages,
//      - overflow pointers at the end of a cell's local payload, and the "next" pointer of an overflow page,
//  and page 1 is merged: the latest page 1 with the file change counter incremented and the in-header size extended.
//
//  Safety: this is a physical rewrite of pages, so it refuses (and the transaction is retried / rebased as before) unless every check holds:
//      - the transaction changed page 1 only in the change counter, the size and the version-valid-for fields (no freelist change, no schema change),
//      - the new pages are exactly the contiguous range (snapshot size, transaction size], none of them the lock-byte page,
//      - no page that existed in the snapshot conflicts (a real conflict is never resolved here),
//      - every page of the range is referenced exactly once by the pages that were parsed (a reference that the parser missed would show up as a
//        page with no reference, so an incomplete parse cannot go unnoticed),
//      - the schema cookie of the latest page 1 equals the snapshot's.
//  The result is published through the normal validation, so a commit that extended the file in the meantime is detected there and the attempt repeats.
//

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "multiwriter_internal.h"

static inline uint32_t rd32 (const uint8_t *p) { return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) | ((uint32_t)p[2] << 8) | p[3]; }
static inline uint16_t rd16 (const uint8_t *p) { return (uint16_t)(((uint32_t)p[0] << 8) | p[1]); }
static inline void wr32 (uint8_t *p, uint32_t v) { p[0] = (uint8_t)(v >> 24); p[1] = (uint8_t)(v >> 16); p[2] = (uint8_t)(v >> 8); p[3] = (uint8_t)v; }

// SQLite varint: returns the length (1..9) or 0 if it runs past `end`.
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

static _Atomic uint64_t reloc_why[64];
static int NA (int why) { atomic_fetch_add(&reloc_why[why], 1); return MW_RELOC_NA; }
uint64_t mw_spin_ns = 50000;
bool mw_timing_on;
void mw_timing_dump (void);
__attribute__((constructor)) static void timing_init (void) { mw_timing_on = getenv("MW_TIMING") != NULL; if (getenv("MW_SPIN_US")) mw_spin_ns = (uint64_t)atoi(getenv("MW_SPIN_US")) * 1000ull; }
static _Atomic uint64_t st_ns[MW_ST_COUNT], st_n[MW_ST_COUNT], st_slow[MW_ST_COUNT], st_max[MW_ST_COUNT];
uint64_t mw_stage_now (void) { struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts); return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec; }
void mw_stage_add (int s, uint64_t ns) {
    atomic_fetch_add_explicit(&st_ns[s], ns, memory_order_relaxed); atomic_fetch_add_explicit(&st_n[s], 1, memory_order_relaxed);
    if (ns >= 500000) atomic_fetch_add_explicit(&st_slow[s], 1, memory_order_relaxed);
    uint64_t m = atomic_load_explicit(&st_max[s], memory_order_relaxed);
    while (ns > m && !atomic_compare_exchange_weak(&st_max[s], &m, ns)) {}
}
_Atomic uint64_t mw_grants[2];
static _Atomic uint64_t cnt[MW_C_COUNT];
void mw_count_add (int c, uint64_t v) { atomic_fetch_add_explicit(&cnt[c], v, memory_order_relaxed); }
void mw_timing_dump (void) {
    if (cnt[MW_C_SY_LEADER] + cnt[MW_C_SY_FOLLOWER]) fprintf(stderr, "TIMING sync: leaders=%llu followers=%llu  records/group=%.2f  KB/group=%.1f  wake-ups per waiting commit=%.2f\n", (unsigned long long)cnt[MW_C_SY_LEADER], (unsigned long long)cnt[MW_C_SY_FOLLOWER], cnt[MW_C_SY_LEADER] ? (double)cnt[MW_C_SY_RECS] / (double)cnt[MW_C_SY_LEADER] : 0.0, cnt[MW_C_SY_LEADER] ? (double)cnt[MW_C_SY_BYTES] / 1024.0 / (double)cnt[MW_C_SY_LEADER] : 0.0, cnt[MW_C_SY_FOLLOWER] ? (double)cnt[MW_C_SY_WAKES] / (double)cnt[MW_C_SY_FOLLOWER] : 0.0);
    if (cnt[MW_C_ADM_SAMPLES]) fprintf(stderr, "TIMING admission: at a release, %.2f of the slots were busy and %.1f waiters were queued (%llu samples)\n", (double)cnt[MW_C_ADM_BUSY] / (double)cnt[MW_C_ADM_SAMPLES], (double)cnt[MW_C_ADM_WAITERS] / (double)cnt[MW_C_ADM_SAMPLES], (unsigned long long)cnt[MW_C_ADM_SAMPLES]);
    if (cnt[MW_C_LAZY_READ]) fprintf(stderr, "TIMING lazy versions read: %llu\n", (unsigned long long)cnt[MW_C_LAZY_READ]);
    if (cnt[MW_C_VIS_IMMEDIATE] + cnt[MW_C_VIS_SPUN] + cnt[MW_C_VIS_PARKED]) fprintf(stderr, "TIMING visible: predecessor already visible=%llu  became visible while spinning=%llu  had to park=%llu\n", (unsigned long long)cnt[MW_C_VIS_IMMEDIATE], (unsigned long long)cnt[MW_C_VIS_SPUN], (unsigned long long)cnt[MW_C_VIS_PARKED]);
    fprintf(stderr, "TIMING turn grants: tracked/rebasable=%llu starving(>=16 refusals)=%llu\n", (unsigned long long)mw_grants[0], (unsigned long long)mw_grants[1]);
    static const char *nm[MW_ST_COUNT] = { "publish(total)", "stripe/gate locks", "log append", "log sync", "make visible", "relocation(total)", "hot-turn wait", "snapshot begin", "reloc mutex wait", "reloc mutex HELD", "interior merge", "hold: head image p1", "hold: prepare", "hold: stripes+reserve", "hold: validate", "hold: seq_mu section", "hold: install+release", "sync: wait for prefix", "sync: wait behind a leader", "sync: leader cycle", "sync: leader pwritev", "sync: leader fsync", "visible: spin", "visible: park", "mp: catch-up before lock", "mp: lock wait", "mp: lock HELD", "mp: catch-up apply (in lock)", "mp: generation change (total)", "mp:   reopen", "mp:   scan_after", "mp: log rewrite (holding the lock)", "mp: log rewrite (lock wait)", "mp: admission wait", "mp: admission hold (slot to release)", "shared: wait for the group fsync", "mp: admission slot idle gap (release -> next grab)", "shared publish: validate", "shared publish: shidx_install", "shared publish: owner maps", "shared publish: mm_install", "shared publish: apply cells", "shared publish: the rest after install" };
    for (int i = 0; i < MW_ST_COUNT; i++) if (st_n[i]) fprintf(stderr, "TIMING %-18s calls=%-8llu avg=%7.1f us  slow(>=0.5ms)=%-6llu (%.1f%%)  max=%.0f us  total=%.0f ms\n", nm[i], (unsigned long long)st_n[i], (double)st_ns[i] / (double)st_n[i] / 1000.0, (unsigned long long)st_slow[i], 100.0 * (double)st_slow[i] / (double)st_n[i], (double)st_max[i] / 1000.0, (double)st_ns[i] / 1e6);
}
void mw_reloc_dump (void) {
    mw_timing_dump(); for (int i = 0; i < 64; i++) if (reloc_why[i]) fprintf(stderr, "RELOC_NA reason %d: %llu\n", i, (unsigned long long)reloc_why[i]); }
enum { K_NONE = 0, K_BTREE = 1, K_OVERFLOW = 2 };

typedef struct {
    uint32_t lo, hi, delta;           // old new-page range (lo, hi], and the offset to add
    int pgsz, usable;
    uint8_t *kind;                    // per new page: how it is referenced
    uint32_t *refs;                   // per new page: number of references found
    uint32_t *queue; int qn, qcap;    // new pages still to process
    bool ok;
    // every rewritten reference: (image slot, byte offset, value before renumbering). A relocation is prepared outside the mutex with the delta known at that
    // moment; if other commits extended the file in between, the references are simply rewritten with the delta found under the mutex.
    struct { int slot; uint32_t off, orig; } *rec; int nrec, caprec;
    const uint8_t *cur_base; int cur_slot;
} rctx;

static void enqueue (rctx *c, uint32_t p, int kind) {
    uint32_t i = p - c->lo - 1;
    c->refs[i]++;
    if (c->refs[i] > 1 || (c->kind[i] != K_NONE && c->kind[i] != kind)) { c->ok = false; return; }
    c->kind[i] = (uint8_t)kind;
    if (c->qn == c->qcap) { c->ok = false; return; }
    c->queue[c->qn++] = p;
}

// Rewrites the reference stored at `loc` if it points into the old new-page range.
static void patch (rctx *c, uint8_t *loc, int kind) {
    uint32_t p = rd32(loc);
    if (p > c->lo && p <= c->hi) {
        if (c->nrec == c->caprec) {
            int cap = c->caprec ? c->caprec * 2 : 32;
            void *nr = realloc(c->rec, (size_t)cap * sizeof(*c->rec));
            if (!nr) { c->ok = false; return; }
            c->rec = nr; c->caprec = cap;
        }
        c->rec[c->nrec].slot = c->cur_slot; c->rec[c->nrec].off = (uint32_t)(loc - c->cur_base); c->rec[c->nrec].orig = p; c->nrec++;
        wr32(loc, p + c->delta); enqueue(c, p, kind);
    }
}

// Rewrites the references held by one b-tree page (`pg` = a private copy of its image).
static void patch_btree_page (rctx *c, uint8_t *pg) {
    uint8_t type = pg[0];
    if (type != 0x02 && type != 0x05 && type != 0x0a && type != 0x0d) { c->ok = false; return; }
    bool interior = type == 0x02 || type == 0x05;
    int hdr = interior ? 12 : 8;
    int ncell = rd16(pg + 3);
    if (hdr + 2 * ncell > c->pgsz) { c->ok = false; return; }
    if (interior) patch(c, pg + 8, K_BTREE);
    int U = c->usable;
    int minLocal = (U - 12) * 32 / 255 - 23;
    int maxLocal = type == 0x0d ? U - 35 : (U - 12) * 64 / 255 - 23;       // (SQLite: minLocal is the same for every page type)
    const uint8_t *end = pg + c->pgsz;
    for (int i = 0; i < ncell && c->ok; i++) {
        int off = rd16(pg + hdr + 2 * i);
        if (off < hdr + 2 * ncell || off + 4 > c->pgsz) { c->ok = false; return; }
        uint8_t *cp = pg + off;
        if (type == 0x05) { patch(c, cp, K_BTREE); continue; }              // interior table cell: child pointer + rowid, no payload
        uint8_t *q = cp;
        if (type == 0x02) { patch(c, q, K_BTREE); q += 4; }
        uint64_t payload = 0, rowid = 0;
        int n = rd_varint(q, end, &payload);
        if (!n) { c->ok = false; return; }
        q += n;
        if (type == 0x0d) { n = rd_varint(q, end, &rowid); if (!n) { c->ok = false; return; } q += n; }
        if (payload > (uint64_t)maxLocal) {
            int local = minLocal + (int)((payload - (uint64_t)minLocal) % (uint64_t)(U - 4));
            if (local > maxLocal) local = minLocal;
            if (q + local + 4 > end) { c->ok = false; return; }
            patch(c, q + local, K_OVERFLOW);
        }
    }
}

#define RELOC_UNLOCK do { if (lane->holds_reloc) { lane->holds_reloc = false; MW_T1(MW_ST_RELOCHOLD, lane->reloc_t0); pthread_mutex_unlock(&db->reloc_mu); } } while (0)

// What a relocation prepares before it takes the mutex (and, in the processes mode, before the publication lock): the private copies of the pages with the references to the new pages
// renumbered for the end of the file as it is now.
typedef struct {
    uint32_t cur0, delta0; uint64_t e_merge;
    int ncf; int *cf;                              // the interior pages that somebody else also wrote (merged three-way under the mutex)
    uint8_t **nim; uint32_t *npg;                  // the private copies and their final page numbers (filled under the mutex)
    rctx c; int *slot_of_new; uint8_t *mbuf; int made;
    int n; uint32_t snap_dbsize, ws_dbsize, growth; int i1, reserved; uint8_t snap1[100];
} rprep;
static void rprep_free (rprep *P) {
    for (int i = 0; i < P->made; i++) free(P->nim[i]);
    free(P->nim); free(P->npg); free(P->c.kind); free(P->c.refs); free(P->c.queue); free(P->slot_of_new); free(P->c.rec); free(P->cf); free(P->mbuf);
    memset(P, 0, sizeof *P);
}

// Phase 1: everything that does not depend on the exact end of the file. Returns SQLITE_OK (P is filled and owns what it allocated), or the final result of the relocation (MW_RELOC_NA, an error).
static int reloc_phase1 (mw_lane *lane, const uint32_t *pgnos, const uint8_t *const *imgs, int n, uint32_t ws_dbsize, uint32_t snap_dbsize, rprep *P) {
    mw_db *db = lane->db; (void)db;
    mw_store *st = lane->db->store;
    int pgsz = st->pgsz;
    memset(P, 0, sizeof *P);
    if (lane->own_n != 0 || ws_dbsize <= snap_dbsize) return NA(1);
    uint32_t growth = ws_dbsize - snap_dbsize;
    int i1 = -1, nnew = 0;
    for (int i = 0; i < n; i++) { if (pgnos[i] == 1) i1 = i; if (pgnos[i] > snap_dbsize) nnew++; }
    if (i1 < 0 || (uint32_t)nnew != growth) return NA(2);          // page 1 must be written, and the new pages must be the whole range
    // page 1 may differ from the snapshot's only in the change counter, the in-header size and version-valid-for
    uint8_t *snap1 = P->snap1;
    if (!mw_store_read(st, 1, lane->tx.snapshot_epoch, 0, 100, snap1)) {           // (page 1 never written since the file was created: the real file's)
        mw_file *f = lane->file;
        if (!f || f->real->pMethods->xRead(f->real, snap1, 100, 0) != SQLITE_OK) return NA(3);
    }
    const uint8_t *our1 = imgs[i1];
    for (int b = 0; b < 100; b++) {
        if ((b >= 24 && b < 32) || (b >= 92 && b < 96)) continue;
        if (snap1[b] != our1[b]) return NA(4);
    }
    int reserved = our1[20];
    uint8_t *latest = malloc((size_t)pgsz);
    if (!latest) return SQLITE_NOMEM;
    int result = SQLITE_OK;
    uint64_t e_merge = 0;
    if (!mw_store_head_image(st, 1, latest, &e_merge)) { free(latest); return NA(5); }
    uint32_t cur0 = rd32(latest + 28), ctr0 = rd32(latest + 24);
    if (rd32(latest + 92) != ctr0 || cur0 < snap_dbsize || memcmp(latest + 40, snap1 + 40, 4) != 0 || memcmp(latest + 16, snap1 + 16, 6) != 0) { free(latest); return NA(6); }
    if (cur0 == snap_dbsize) { free(latest); return NA(7); }               // nobody extended the file: the conflict is somewhere else
    free(latest);
    // Pages that existed in the snapshot and have a newer committed version. An interior *table* page (typically the parent of leaves both transactions split)
    // is merged three-way under the mutex; anything else is a real conflict, which is not resolved here.
    int ncf = 0;
    bool real_conflict = false;
    int *cf = malloc((size_t)n * sizeof(int));                          // (one pass, capacity for every page: heads may change between two passes)
    if (!cf) return SQLITE_NOMEM;
    for (int i = 0; i < n && !real_conflict; i++)
        if (pgnos[i] != 1 && pgnos[i] <= snap_dbsize && mw_store_head_epoch(st, pgnos[i]) > lane->tx.snapshot_epoch) { if (imgs[i][0] == 0x05 && !lane->nomerge) cf[ncf++] = i; else real_conflict = true; }
    if (real_conflict) { free(cf); return NA(9); }
    uint32_t delta0 = cur0 - snap_dbsize;
    P->cf = cf; P->ncf = ncf; P->cur0 = cur0; P->delta0 = delta0; P->e_merge = e_merge;
    P->nim = malloc((size_t)n * sizeof(uint8_t *));
    P->npg = malloc((size_t)n * sizeof(uint32_t));
    P->c = (rctx){ .lo = snap_dbsize, .hi = ws_dbsize, .delta = delta0, .pgsz = pgsz, .usable = pgsz - reserved, .ok = true, .qcap = (int)growth };
    rctx *c = &P->c;
    c->kind = calloc(growth, 1); c->refs = calloc(growth, sizeof(uint32_t)); c->queue = malloc((size_t)growth * sizeof(uint32_t));
    P->slot_of_new = malloc((size_t)growth * sizeof(int));                 // index in the write set of each new page
    bool fail = !P->nim || !P->npg || !c->kind || !c->refs || !c->queue || !P->slot_of_new;
    for (int i = 0; i < n && !fail; i++, P->made++) {                            // private copies, one allocation each: the store adopts them (no second copy under the mutex)
        P->nim[i] = malloc((size_t)pgsz);
        if (!P->nim[i]) { fail = true; break; }
        memcpy(P->nim[i], imgs[i], (size_t)pgsz);
        if (pgnos[i] > snap_dbsize) P->slot_of_new[pgnos[i] - snap_dbsize - 1] = i;
    }
    if (!fail) {
        uint8_t **nim = P->nim;
        for (int i = 0; i < n && c->ok; i++) if (pgnos[i] != 1 && pgnos[i] <= snap_dbsize) { c->cur_base = nim[i]; c->cur_slot = i; patch_btree_page(c, nim[i]); }
        for (int qi = 0; qi < c->qn && c->ok; qi++) {                           // the new pages, in reference order
            uint32_t pq = c->queue[qi];
            int slot = P->slot_of_new[pq - snap_dbsize - 1];
            c->cur_base = nim[slot]; c->cur_slot = slot;
            if (c->kind[pq - snap_dbsize - 1] == K_BTREE) patch_btree_page(c, nim[slot]);
            else patch(c, nim[slot], K_OVERFLOW);                            // overflow page: its first 4 bytes point to the next one
        }
        for (uint32_t k = 0; k < growth && c->ok; k++) if (c->refs[k] != 1) c->ok = false;      // each new page referenced exactly once
    }
    if (fail) result = SQLITE_NOMEM; else if (!c->ok) result = NA(30);
    if (result == SQLITE_OK && ncf) { P->mbuf = malloc(3 * (size_t)pgsz); if (!P->mbuf) result = SQLITE_NOMEM; }          // (3 page buffers for the merges: theirs, base, result)
    if (result != SQLITE_OK) { rprep_free(P); return result; }
    P->n = n; P->snap_dbsize = snap_dbsize; P->ws_dbsize = ws_dbsize; P->growth = growth; P->i1 = i1; P->reserved = reserved;
    return SQLITE_OK;
}

// Before the publication lock (processes mode): phase 1 of the relocation that the commit will most likely need. mw_lane_relocate takes it if it still fits.
typedef struct { rprep p; } rprep_box;
void mw_lane_reloc_prepare (mw_lane *lane, const uint32_t *pgnos, const uint8_t *const *imgs, int n, uint32_t ws_dbsize, uint32_t snap_dbsize) {
    mw_lane_reloc_discard(lane);
    if (lane->noreloc || lane->own_n != 0 || ws_dbsize <= snap_dbsize) return;
    rprep_box *b = malloc(sizeof *b); if (!b) return;
    if (reloc_phase1(lane, pgnos, imgs, n, ws_dbsize, snap_dbsize, &b->p) != SQLITE_OK) { free(b); return; }
    lane->rprep = b;
}
void mw_lane_reloc_discard (mw_lane *lane) {
    rprep_box *b = lane->rprep; if (!b) return;
    atomic_fetch_add(&lane->db->n_prep_dropped, 1);
    lane->rprep = NULL; rprep_free(&b->p); free(b);
}

// Publishes the transaction again with its new pages moved above the current end of the file. Returns SQLITE_OK if it committed,
// MW_RELOC_NA if the conflict is not a growth-only one (or a check failed), MW_CONFLICT if the attempts were beaten by other extensions,
// or another code for a real error.
int mw_lane_relocate (mw_lane *lane, const mw_validate *v0, const uint32_t *pgnos, const uint8_t *const *imgs, int n,
                      uint32_t ws_dbsize, uint32_t snap_dbsize, int sync, uint64_t *out_epoch) {
    mw_db *db = lane->db;
    mw_store *st = db->store;
    int pgsz = st->pgsz;
    uint8_t *latest = malloc((size_t)pgsz);
    if (!latest) { mw_lane_reloc_discard(lane); return SQLITE_NOMEM; }
    int result = MW_CONFLICT;
    for (int attempt = 0; attempt < 4; attempt++) {
        rprep P0, *P = &P0;
        rprep_box *pre = attempt == 0 ? lane->rprep : NULL; bool used_pre = false;
        if (pre && pre->p.n == n && pre->p.ws_dbsize == ws_dbsize && pre->p.snap_dbsize == snap_dbsize) { P0 = pre->p; free(pre); lane->rprep = NULL; atomic_fetch_add(&db->n_prep_used, 1); used_pre = true; }      // (prepared before the lock: the copies and the references renumbered for the end of the file as it was then)
        else { mw_lane_reloc_discard(lane); int prc = reloc_phase1(lane, pgnos, imgs, n, ws_dbsize, snap_dbsize, P); if (prc != SQLITE_OK) { result = prc; break; } }
        const uint32_t growth = P->growth; (void)growth;
        const int i1 = P->i1, ncf = P->ncf, reserved = P->reserved;
        const uint32_t delta0 = P->delta0, snap_dbsize_ = snap_dbsize; (void)snap_dbsize_;
        const uint8_t *snap1 = P->snap1;
        uint8_t **nim = P->nim; uint32_t *npg = P->npg; int *cf = P->cf; uint8_t *mbuf = P->mbuf; rctx c = P->c;
        uint64_t e_merge = P->e_merge;
        int rc = SQLITE_OK;
        uint32_t pending = (uint32_t)(1073741824u / (uint32_t)pgsz) + 1;
        // ---- 2. Under the mutex: the exact end of the file, the references rewritten with the final delta, page 1 merged, then the publication up to the install.
        {
            uint64_t trl0 = MW_T0();
            mw_spinlock(&db->reloc_mu);                                // one relocation at a time: they would only beat each other
            MW_T1(MW_ST_RELOCLOCK, trl0);
            lane->reloc_t0 = MW_T0();
            lane->holds_reloc = true;
            if (db->mp) { if (lane->mp_held) mw_mp_catchup_locked(db); else mw_mp_catchup(db); }      // (multi-process: lane_publish holds the publication lock for the whole commit)
            uint64_t th0 = MW_T0();
            // Page 1 (the newest version): the previous relocation left its installed image in db->p1_cache; if nobody wrote page 1 since, that *is* the newest
            // version and there is no need to take page 1's stripe (which every transaction's header read also wants) and copy it again.
            if (!db->mp && !db->p1_cache) db->p1_cache = malloc((size_t)pgsz);
            if (!db->mp && db->p1_cache && db->p1_cache_epoch != 0 && db->p1_cache_epoch == atomic_load_explicit(&st->p1_head_epoch, memory_order_acquire)) { memcpy(latest, db->p1_cache, (size_t)pgsz); e_merge = db->p1_cache_epoch; }
            else if (!mw_store_head_image(st, 1, latest, &e_merge)) rc = NA(5);
            MW_T1(MW_ST_H_HEAD, th0);
            uint64_t thp0 = MW_T0();
            uint32_t cur = 0, ctr = 0, new_size = 0;
            if (rc == SQLITE_OK) {
                cur = rd32(latest + 28); ctr = rd32(latest + 24); new_size = cur + (ws_dbsize - snap_dbsize);
                if (rd32(latest + 92) != ctr || cur < snap_dbsize || memcmp(latest + 40, snap1 + 40, 4) != 0 || memcmp(latest + 16, snap1 + 16, 6) != 0) rc = NA(6);
                else if (cur == snap_dbsize) rc = NA(7);
                else if ((snap_dbsize < pending && ws_dbsize >= pending) || (cur < pending && new_size >= pending)) rc = NA(8);
            }
            if (rc == SQLITE_OK) {
                uint32_t delta = cur - snap_dbsize;
                if (delta != delta0 && used_pre) atomic_fetch_add(&db->n_prep_rewrote, 1);
                if (delta != delta0) for (int k = 0; k < c.nrec; k++) wr32(nim[c.rec[k].slot] + c.rec[k].off, c.rec[k].orig + delta);
                for (int i = 0; i < n; i++) npg[i] = pgnos[i] > snap_dbsize ? pgnos[i] + delta : pgnos[i];
                // page 1: the latest one, counter + 1, size extended (the transaction changed nothing else on it)
                memcpy(nim[i1], latest, (size_t)pgsz);
                wr32(nim[i1] + 24, ctr + 1); wr32(nim[i1] + 92, ctr + 1); wr32(nim[i1] + 28, new_size);
                // Interior pages both transactions rewrote: merge with the newest committed version (ours already has final numbers for its new children).
                // Each is validated at publication against the epoch of the version it was merged with.
                uint32_t own_pg[1 + 64]; uint64_t own_ep[1 + 64]; int nown = 0, nmerged = 0;
                own_pg[nown] = 1; own_ep[nown] = e_merge; nown++;
                for (int k = 0; k < ncf && rc == SQLITE_OK; k++) {
                    int slot = cf[k];
                    uint8_t *theirs = mbuf, *basei = mbuf + pgsz, *merged = mbuf + 2 * (size_t)pgsz;
                    uint64_t eh = 0;
                    if (nown >= 65 || !mw_store_head_image(st, pgnos[slot], theirs, &eh)) { rc = NA(31); break; }
                    if (eh <= lane->tx.snapshot_epoch) continue;                // (no longer newer than our snapshot)
                    if (!mw_store_read(st, pgnos[slot], lane->tx.snapshot_epoch, 0, (uint32_t)pgsz, basei)) {
                        mw_file *f = lane->file;
                        if (!f || f->real->pMethods->xRead(f->real, basei, pgsz, (sqlite3_int64)(pgnos[slot] - 1) * pgsz) != SQLITE_OK) { rc = NA(31); break; }
                    }
                    uint64_t tm0 = MW_T0();
                    bool merged_ok = mw_interior_merge(basei, nim[slot], theirs, pgsz, reserved, merged);
                    MW_T1(MW_ST_MERGE, tm0);
                    if (!merged_ok) { rc = NA(32); break; }
                    memcpy(nim[slot], merged, (size_t)pgsz);
                    own_pg[nown] = pgnos[slot]; own_ep[nown] = eh; nown++; nmerged++;
                }
                for (int a = 1; a < nown; a++) { uint32_t pg = own_pg[a]; uint64_t ep = own_ep[a]; int b = a - 1; while (b >= 0 && own_pg[b] > pg) { own_pg[b + 1] = own_pg[b]; own_ep[b + 1] = own_ep[b]; b--; } own_pg[b + 1] = pg; own_ep[b + 1] = ep; }
                MW_T1(MW_ST_H_PREP, thp0);
                mw_validate v = *v0;
                v.own_pgnos = own_pg; v.own_epochs = own_ep; v.own_n = nown;    // page 1 and the merged pages were merged against the state at their epochs
                v.adopt_images = true;                                          // the store keeps our private copies (and frees them on failure)
                if (rc == SQLITE_OK) {
                    if (db->cdc) mw_cdc_relocated(lane);
                    uint64_t *rch = NULL;                                       // (multi-process: the hashes made before the lock hold for the pages that the relocation did not change; the others are made by the append)
                    if (db->mp && lane->pre_ch && n == lane->ws_n && (rch = malloc((size_t)n * sizeof(uint64_t)))) {
                        memcpy(rch, lane->pre_ch, (size_t)n * sizeof(uint64_t)); rch[i1] = 0;
                        for (int k = 0; k < c.nrec; k++) rch[c.rec[k].slot] = 0;
                        for (int k = 0; k < nown; k++) for (int j = 0; j < n; j++) if (pgnos[j] == own_pg[k]) rch[j] = 0;
                        lane->pre_use = rch;
                    }
                    rc = mw_db_publish(db, lane, &v, npg, (const uint8_t *const *)nim, n, new_size, cur, sync, out_epoch);
                    lane->pre_use = NULL; free(rch);
                    P->made = 0;                                                // (ownership went to the publisher whatever the outcome)
                    if (rc == SQLITE_OK) { atomic_fetch_add(&db->n_relocations, 1); if (nmerged) atomic_fetch_add(&db->n_merges, (uint64_t)nmerged); }
                }
            }
            RELOC_UNLOCK;
        }
        rprep_free(P);
        if (rc != MW_CONFLICT) { result = rc; break; }                        // committed, not applicable, or a real error
        // MW_CONFLICT: another commit extended the file (or changed page 1) since we read it: read the latest again
    }
    free(latest);
    return result;
}
