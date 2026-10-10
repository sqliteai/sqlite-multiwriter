//
//  multiwriter_shared.c
//  sqlite-multiwriter
//
//  Multi-process Multi-Writer (URI mw_mp=1): the version index and the page images are shared by every process, nobody keeps a private store.
//
//    "<db>-mwidx"   the shared version index (multiwriter_shidx.h): page, snapshot epoch -> where the page image is
//    "<db>-mw.<N>"  the segmented log (multiwriter_seglog.h): the page images, durable, appended once, deleted when compacted
//    "<db>-mwlock"  the shared header (multiwriter_mp.c): publication lock, committed epoch, registry of snapshots, compaction target, liveness
//
//  A read is an index lookup and a copy from a mapped segment; a commit is validated against the index and appended to the log by the publisher, under the publication
//  lock, once, for everybody; nobody has to apply anything.
//  Compaction, the registration of snapshots (the floor under which versions may be freed) and process liveness are in multiwriter_mp.c.
//

#include "multiwriter_os.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "multiwriter_io.h"
#include "multiwriter_internal.h"
#include "multiwriter_seglog.h"

static uint64_t now_ns (void) { struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts); return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec; }

uint64_t mw_shared_visible_epoch (mw_db *db) { return atomic_load_explicit(&db->shm->committed_epoch, memory_order_acquire); }

// MARK: - reads -

// The real database file, opened on first use (a descriptor of our own: the VFS of a lane never writes it, and we only read what no version in the index covers).
static int real_fd (mw_db *db) {
    if (db->fd_real < 0) {
        sqlite3_mutex_enter(db->mu);
        if (db->fd_real < 0) db->fd_real = open(db->path, O_RDWR);
        sqlite3_mutex_leave(db->mu);
    }
    return db->fd_real;
}

int mw_shared_read (mw_db *db, uint32_t pgno, uint64_t snap, uint32_t poff, uint32_t n, void *dst) {
    uint64_t ep, loc;
    if (!shidx_lookup(db->ix, pgno, snap, &ep, &loc)) return 0;                 // no version: the real file
    return mw_seglog_read(db, loc, poff, n, dst);                               // 1 read; 0 the segment is gone (the version is in the real file, which holds the same page); -1 it could not be read: an error, not the real file's page
}

uint32_t mw_shared_dbsize (mw_db *db, uint64_t snap) {
    uint32_t pages;
    if (shidx_dbsize(db->ix, snap, &pages)) return pages;
    return atomic_load_explicit(&db->shm->base_dbsize, memory_order_acquire);
}

uint64_t mw_shared_head_epoch (mw_db *db, uint32_t pgno) { return shidx_head_epoch(db->ix, pgno); }

bool mw_shared_head_image (mw_db *db, uint32_t pgno, uint8_t *dst, uint64_t *epoch) {
    uint64_t ep, loc;
    if (!shidx_lookup(db->ix, pgno, UINT64_MAX, &ep, &loc)) return false;
    int rr = mw_seglog_read(db, loc, 0, (uint32_t)db->store->pgsz, dst);
    if (rr < 0) return false;                                                       // (not known to be anywhere else: the caller refuses what it wanted it for)
    if (rr == 0) {                                                                  // its segment was deleted: the real file has it
        int fd = real_fd(db);
        size_t pgsz = (size_t)db->store->pgsz;
        if (fd < 0 || pread(fd, dst, pgsz, (off_t)(pgno - 1) * (off_t)pgsz) != (ssize_t)pgsz) return false;
    }
    *epoch = ep;
    return true;
}

// MARK: - open / close -

typedef struct { mw_db *db; } replay_ctx;

static int replay_cb (void *ctx, uint64_t epoch, uint32_t dbsize, int n, const uint32_t *pgnos, const uint64_t *locs, const uint8_t *ext, uint32_t ext_len, uint64_t ext_loc) {
    mw_db *db = ((replay_ctx *)ctx)->db;
    if (shidx_room(db->ix) < (uint32_t)n + 2) shidx_gc_floor(db->ix, epoch - 1, db->base_epoch);        // (nobody else exists yet)
    int rc = shidx_install(db->ix, epoch, dbsize, n, pgnos, locs);
    if (rc != 0) return SQLITE_FULL;
    shidx_publish(db->ix, epoch);
    return SQLITE_OK;
}

static void gc_crash_point (void) { mw_fault_hit(MW_CRASH_SHARED_GC); }
int mw_shared_open (mw_db *db) {
    char *ixp = mw_sidecar_path(db->path, "mwidx");
    if (!ixp) return SQLITE_NOMEM;
    if (db->mp_first) shidx_unlink(ixp);                                        // volatile: rebuilt from the log below
    // The index covers the page numbers below 2^max_pages_log2 (the directory costs 2^(n-12) words, the table of blocks 2^n of them, a sparse file: 64 GB of 4 KB pages at 24). A database that
    // is bigger than that, or that is about to be, gets an index that covers twice its size: with the fixed 24 the commit that needed a page above it failed at the publication with
    // SQLITE_FULL, after its work was done, for ever.
    uint32_t lg = 24;
    for (uint64_t want = (uint64_t)db->store->base_dbsize * 2; lg < 32 && ((uint64_t)1 << lg) < want; ) lg++;
    shidx_params p = { lg, 4u << 20, MW_MP_SLOTS, 0, (uint32_t)mw_file_mode(db->path) };
    const char *e = getenv("MW_IDX_ENTRIES");
    if (e && atoi(e) > 1000) p.max_entries = (uint32_t)atoi(e);
    shidx_gc_hook = gc_crash_point;
    db->ix = shidx_open(ixp, &p);
    sqlite3_free(ixp);
    if (!db->ix) return SQLITE_CANTOPEN;
    db->store->shared_db = db;
    uint64_t base = 1, last = 1;
    replay_ctx rc_ctx = { db };
    if (db->mp_first) db->base_epoch = 1;
    int rc = mw_seglog_open(db, db->mp_first ? replay_cb : NULL, &rc_ctx, &base, &last);
    if (rc != SQLITE_OK) return rc;
    if (db->mp_first) {
        db->base_epoch = base;
        if (shidx_committed(db->ix) < last) shidx_publish(db->ix, last);
        atomic_store(&db->epoch, last);                                          // (mw_mp_finish_open copies these into the shared header)
        atomic_store(&db->next_epoch, last);
        atomic_store(&db->shm->base_dbsize, db->store->base_dbsize);
    }
    return SQLITE_OK;
}

int mw_shared_open_finish (mw_db *db) {
    mw_shm *sh = db->shm;
    // A commit that a process that is gone had in flight is finished or undone before this open returns, not when some publisher happens to find the lock of the dead one: whoever opens the
    // database (a restarted process, which then asks what became of the commit that it was making) must see its fate decided, and not decided after it has looked.
    if (db->mp_stale_owner || (!db->mp_first && atomic_load(&sh->pend_epoch))) { db->mp_stale_owner = false; mw_mp_lock(db); mw_shared_repair(db); mw_mp_unlock(db); }       // (the publisher that died had our pid: what it left half done is finished or undone)
    if (db->mp_first) {
        { static _Atomic int dly = MW_KNOB_UNSET; const int us = mw_knob_int(&dly, "MW_TEST_OPEN_DELAY_US", 0); if (us > 0) usleep((useconds_t)us); }        // (tests: the time that the first process takes to finish the header)
        atomic_store(&sh->log_pos, MW_LOG_POS(atomic_load(&sh->sl_seg), atomic_load(&sh->sl_end)));
        atomic_store(&sh->committed_epoch, atomic_load(&db->epoch));
        atomic_store(&sh->ready, 1);
        flock(db->mp_lockfd, LOCK_SH);                                           // downgrade: others may proceed, now that the header is complete (before, a process that committed in between had its epoch overwritten by the one above: the epoch went back, the versions it installed stayed ahead of it, and every commit after that conflicted for ever)
    }
    return SQLITE_OK;
}

void mw_shared_close (mw_db *db, bool sole) {
    char *ixp = sole ? mw_sidecar_path(db->path, "mwidx") : NULL;
    if (db->sl) {
        if (sole) {                                                              // the last process: everything is in the real file now (or the log stays for recovery)
            uint32_t mn = atomic_load(&db->shm->seg_min), cur = atomic_load(&db->shm->sl_seg);
            bool clean = db->open_done && atomic_load(&db->shm->base_epoch) == atomic_load(&db->shm->committed_epoch) && !atomic_load(&db->failed);       // (an open that failed, for a file of another format for instance, leaves the segments as they are)
            mw_seglog_close(db);
            if (clean) {
                for (uint32_t s = mn; s <= cur + 1; s++) { char p[700]; snprintf(p, sizeof p, "%s-mw.%u", db->path, s); unlink(p); }     // (and the next one, prepared ahead)
                char p[700]; snprintf(p, sizeof p, "%s-mw.%u.new", db->path, cur + 1); unlink(p);
            }
        } else mw_seglog_close(db);
    }
    if (db->ix) { shidx_close(db->ix); db->ix = NULL; }
    if (ixp) { shidx_unlink(ixp); sqlite3_free(ixp); }
}

// MARK: - a publisher died inside the publication lock -
// The lock is taken from a process that is gone. If it had appended its record and not yet made the commit visible, the record is complete (finish the commit: install its pages, its
// metadata, publish) or torn (undo: the cursor goes back, the header is cleared). Either way nobody else saw the commit, and the log has exactly one record for every epoch.
static void shared_gc (mw_db *db);
void mw_shared_repair (mw_db *db) {
    mw_shm *sh = db->shm;
    if (db->ix) shidx_gc_repair(db->ix);
    {   // a holder that died in the roll to a new segment: the cursor is in the new segment but still at the offset of the old one; there is no record at the head of a segment that was just rolled to
        uint32_t cs = atomic_load(&sh->sl_seg); uint64_t ce = atomic_load(&sh->sl_end);
        if (ce > MW_SEG_HDR && !atomic_load(&sh->pend_epoch)) {
            uint64_t e2 = 0, s2 = 0, l2 = 0; uint32_t d2 = 0, x2 = 0; int n2 = 0; uint32_t *pg2 = NULL; uint64_t *lc2 = NULL;
            int prc = mw_seglog_peek(db, cs, MW_SEG_HDR, &e2, &d2, &n2, &pg2, &lc2, &x2, &l2, &s2);
            free(pg2); free(lc2);
            if (prc == SQLITE_NOTFOUND) { atomic_store_explicit(&sh->sl_end, MW_SEG_HDR, memory_order_release); }
            else if (prc != SQLITE_OK) { atomic_store(&sh->broken, 1); return; }                       // (the segment could not be looked at: not known to be empty, and the next append would overwrite it)
        }
    }
    uint64_t pe = atomic_load_explicit(&sh->pend_epoch, memory_order_acquire);
    if (!pe) return;
    uint64_t committed = atomic_load(&sh->committed_epoch);
    uint32_t seg = atomic_load(&sh->pend_seg); uint64_t off = atomic_load(&sh->pend_off);
    if (pe != committed + 1) { atomic_store(&sh->pend_epoch, 0); return; }                         // (it did get visible: nothing to do)
    uint64_t epoch = 0, size = 0, ext_loc = 0; uint32_t dbsize = 0, ext_len = 0; int n = 0; uint32_t *pgnos = NULL; uint64_t *locs = NULL;
    int rc = mw_seglog_peek(db, seg, off, &epoch, &dbsize, &n, &pgnos, &locs, &ext_len, &ext_loc, &size);
    if (rc != SQLITE_OK && rc != SQLITE_NOTFOUND) { atomic_store(&sh->broken, 1); return; }          // (could not be read: not known to be torn, and to undo it could undo a record that was complete)
    if (rc != SQLITE_OK || epoch != pe) {                                                         // torn or never written: undo
        mw_seglog_discard(db, seg, off);
        atomic_store(&sh->sl_seg, seg); atomic_store_explicit(&sh->sl_end, off, memory_order_release);
        atomic_store(&sh->pend_epoch, 0);
        return;
    }
    // complete: finish it (installing twice what the dead one had already installed only adds equal versions)
    // (what the dead one had installed already is kept, and the rest is installed: it is the one record that is in the log for this epoch, and what is in the index has to be all of it. The room
    // that is left is the room that was there before it began, less the entries that it took: the check of room of the publication does not cover installing it again from the start)
    int irc = shidx_install_missing(db->ix, epoch, dbsize, n, pgnos, locs);
    if (irc == -1) { shared_gc(db); irc = shidx_install_missing(db->ix, epoch, dbsize, n, pgnos, locs); }
    if (irc != 0) {                                                                                  // no room even then: not published half, the commits stop until the next open replays the log
        atomic_store(&sh->broken, 1);
        free(pgnos); free(locs);
        return;
    }
                                                // (the owner maps follow commits by their pages: this one's were not applied: rebuilt at the next use)
    atomic_store(&sh->sl_seg, seg); atomic_store_explicit(&sh->sl_end, off + size, memory_order_release);
    atomic_store(&db->next_epoch, epoch);
    atomic_store_explicit(&sh->log_pos, MW_LOG_POS(seg, off + size), memory_order_release);
    shidx_publish(db->ix, epoch);
    atomic_store_explicit(&sh->committed_epoch, epoch, memory_order_release);
    atomic_store(&db->epoch, epoch);
    atomic_store(&sh->pend_epoch, 0);
    free(pgnos); free(locs);
}

// MARK: - publishing -

static int cmp_pg (const void *a, const void *b) { uint32_t x = *(const uint32_t *)a, y = *(const uint32_t *)b; return x < y ? -1 : x > y; }

// The GC of the index (writer-side: the publication lock is held). The floor is the compaction target of the registry protocol (no snapshot below it exists or can be taken),
// held back by a compaction that is reading versions.
static void shared_gc (mw_db *db) {
    mw_shm *sh = db->shm;
    uint64_t floor = mw_mp_compaction_target(db);
    uint64_t busy = atomic_load(&sh->compact_busy_T);
    if (busy && busy < floor) floor = busy;
    shidx_gc_floor(db->ix, floor, atomic_load(&sh->base_epoch));
}

// One commit, under the publication lock: validate against the index, append to the log, install in the index, publish.
int mw_shared_publish (mw_db *db, mw_lane *lane, const mw_validate *v, const uint32_t *pgnos, const uint8_t *const *images, int n, uint32_t ws_dbsize, uint32_t snap_dbsize, int sync, uint64_t *out_epoch) {
    mw_shm *sh = db->shm;
    shidx *ix = db->ix;
    size_t pgsz = (size_t)db->store->pgsz;
    const bool adopt = v && v->adopt_images;
    #define ADOPT_FREE() do { if (adopt) for (int _i = 0; _i < n; _i++) free((void *)images[_i]); } while (0)
    int rc = SQLITE_OK;
    if (atomic_load(&db->failed) || atomic_load(&sh->broken)) { ADOPT_FREE(); return SQLITE_IOERR; }

    uint64_t tv0 = MW_T0();
    // ---- validate -----------------------------------------------------------------------------------
    if (v) {
        uint32_t *used = NULL; int nused = 0; bool used_built = false;
        uint8_t *bufs = NULL;
        for (int i = 0; i < v->n_read && rc == SQLITE_OK; i++) {
            uint32_t pg = v->read_pgnos[i];
            uint64_t he = shidx_head_epoch(ix, pg), sn = mw_snap_for(v, pg);
            if (he <= sn) continue;
            if (lane && lane->file && !lane->noroute) {
                // an interior page changed: is what this transaction took from it (which child to follow) unchanged?
                if (!used_built) {
                    used_built = true;
                    used = malloc((size_t)(v->n_read + n + 1) * sizeof(uint32_t));
                    if (used) { for (int k = 0; k < v->n_read; k++) used[nused++] = v->read_pgnos[k]; for (int k = 0; k < n; k++) used[nused++] = pgnos[k]; qsort(used, (size_t)nused, sizeof(uint32_t), cmp_pg); }
                    bufs = malloc(2 * pgsz);
                }
                if (used && bufs) {
                    uint8_t *oldp = bufs, *newp = bufs + pgsz;
                    bool have_old = mw_shared_read(db, pg, sn, 0, (uint32_t)pgsz, oldp) > 0;
                    if (!have_old) { mw_file *f = lane->file; have_old = f->real->pMethods->xRead(f->real, oldp, (int)pgsz, (sqlite3_int64)(pg - 1) * (sqlite3_int64)pgsz) == SQLITE_OK; }
                    uint64_t he2;
                    if (have_old && mw_shared_head_image(db, pg, newp, &he2) && mw_interior_routes_same(oldp, newp, (int)pgsz, db->store->reserved, used, nused)) {
                        atomic_fetch_add(&db->n_reads_saved, 1);
                        continue;
                    }
                }
            }
            atomic_fetch_add(&db->n_read_conflicts, 1);
            rc = MW_CONFLICT_READ;
        }
        free(used); free(bufs);
        for (int i = 0; i < n && rc == SQLITE_OK; i++)
            if (shidx_head_epoch(ix, pgnos[i]) > mw_snap_for(v, pgnos[i])) { atomic_fetch_add(&db->n_page_conflicts, 1); rc = MW_CONFLICT; }
        if (rc == SQLITE_OK && v->check_cookie && shidx_head_epoch(ix, 1) > mw_snap_for(v, 1)) {           // a schema change invalidates everything, on any page
            uint8_t c[4] = {0}; uint64_t he;
            uint8_t *tmp = malloc(pgsz);
            if (tmp && mw_shared_head_image(db, 1, tmp, &he)) memcpy(c, tmp + 40, 4);
            free(tmp);
            uint32_t latest = ((uint32_t)c[0] << 24) | ((uint32_t)c[1] << 16) | ((uint32_t)c[2] << 8) | c[3];
            if (latest != v->cookie) { atomic_fetch_add(&db->n_schema_conflicts, 1); rc = MW_CONFLICT_SCHEMA; }
        }
        if (rc != SQLITE_OK) { ADOPT_FREE(); return rc; }
    }
    MW_T1(MW_ST_SH_VALIDATE, tv0);

    // ---- room in the index (it is full only if compaction cannot keep up): a record must not reach the log if its versions cannot be installed ----
    if (shidx_room(ix) < (uint32_t)n + 2) {
        shared_gc(db);
        if (shidx_room(ix) < (uint32_t)n + 2) { mw_db_compactor_kick(db); ADOPT_FREE(); return SQLITE_FULL; }
    }

    // ---- assign the epoch, append to the log, install, publish ------------------------------------------
    uint64_t epoch = atomic_load_explicit(&sh->committed_epoch, memory_order_acquire) + 1;
    uint32_t last_size = mw_shared_dbsize(db, epoch - 1);
    uint32_t new_dbsize = (ws_dbsize == snap_dbsize) ? last_size : ws_dbsize;       // (a transaction that did not change the size must not roll it back to its view)
    uint64_t locs_small[16]; uint64_t *locs = n <= 16 ? locs_small : malloc((size_t)n * sizeof(uint64_t));
    if (!locs) { ADOPT_FREE(); return SQLITE_NOMEM; }
    mw_fault_hit(MW_CRASH_BEFORE_LOG);
    uint64_t ta0 = MW_T0();
    uint32_t seg = 0; uint64_t end = 0;
    uint64_t ext_loc = 0;
    const uint64_t *ch = lane ? lane->pre_use : NULL; if (lane) lane->pre_use = NULL;          // (the hashes of the pages made before the lock: for this publication only, the images of the next one may be others)
    rc = mw_seglog_append(db, epoch, new_dbsize, n, pgnos, images, NULL, 0, ch, locs, &ext_loc, &seg, &end);
    mw_fault_hit(MW_CRASH_SHARED_APPENDED);                                           // (the record is complete, nothing is installed)
    MW_T1(MW_ST_APPEND, ta0);
    if (rc != SQLITE_OK) { if (n > 16) free(locs); ADOPT_FREE(); return rc; }          // (the append failed before it touched anything the others can see: this commit fails, the database does not)
    uint64_t ti0 = MW_T0();
    if (mw_fault_hit(MW_FAULT_INSTALL_ERR) || shidx_install(ix, epoch, new_dbsize, n, pgnos, locs) != 0) {                // (nothing was installed: the record is taken out of the log again, so that the log has exactly one record for the epoch that the next commit takes)
        mw_seglog_discard(db, atomic_load(&sh->pend_seg), atomic_load(&sh->pend_off));
        atomic_store(&sh->sl_seg, atomic_load(&sh->pend_seg)); atomic_store_explicit(&sh->sl_end, atomic_load(&sh->pend_off), memory_order_release);
        atomic_store_explicit(&sh->pend_epoch, 0, memory_order_release);
        if (n > 16) free(locs); ADOPT_FREE(); return SQLITE_FULL;
    }
    MW_T1(MW_ST_SH_INSTALL, ti0);
    if (n > 16) free(locs);
    ADOPT_FREE();
    if (lane) { lane->sl_seg = seg; lane->sl_end = end; }
    mw_fault_hit(MW_CRASH_SHARED_INSTALLED);                                          // (installed, not visible)
    atomic_store(&db->next_epoch, epoch);
    atomic_store_explicit(&sh->log_pos, MW_LOG_POS(seg, end), memory_order_release);
    shidx_publish(ix, epoch);
    atomic_store_explicit(&sh->committed_epoch, epoch, memory_order_release);        // (the record is visible to the other processes before it is durable: as in the other mode, an acknowledged commit is never lost)
    atomic_store(&db->epoch, epoch);
    atomic_store_explicit(&sh->pend_epoch, 0, memory_order_release);                 // (visible: nothing is pending any more)
    mw_fault_hit(MW_CRASH_AFTER_LOG);
    mw_fault_hit(MW_CRASH_AFTER_VISIBLE);
    if (!lane && sync) { int src = mw_seglog_sync(db, seg, end); if (src != SQLITE_OK) { atomic_store(&db->failed, 1); return src; } }
    if (db->gc_interval > 0 && (atomic_fetch_add(&db->publishes_since_gc, 1) + 1) >= (uint64_t)db->gc_interval) { atomic_store(&db->publishes_since_gc, 0); shared_gc(db); }
    if (db->log_max_bytes && mw_seglog_bytes(db) > mw_log_limit(db)) {
        mw_db_compactor_kick(db);
        if (mw_seglog_bytes(db) > db->log_max_bytes * 8) {                                         // the log is far ahead of its compaction: commits slow down in proportion to the overshoot (after the lock is released: mw_db_publish_finish)
            atomic_fetch_add(&db->n_backpressure, 1);
            uint64_t over = mw_seglog_bytes(db) / (db->log_max_bytes * 8);
            if (lane) { lane->bp_wait_us = (uint32_t)(over > 20 ? 20000 : over * 1000); } else { struct timespec ts = { 0, 500000 }; nanosleep(&ts, NULL); }
        }
    }
    atomic_fetch_add(&db->n_commits, 1);
    atomic_fetch_add(&db->n_fast_commits, 1);
    atomic_fetch_add(&db->n_pages_published, (uint64_t)n);
    if (out_epoch) *out_epoch = epoch;
    return SQLITE_OK;
    #undef ADOPT_FREE
}

// After the publication lock was released: the durability the commit asked for (group commit: one fsync per process for everybody waiting).
int mw_shared_sync (mw_db *db, mw_lane *lane) {
    uint64_t s0 = now_ns();
    uint64_t tsf = MW_T0();
    int rc = mw_seglog_sync(db, lane->sl_seg, lane->sl_end);
    MW_T1(MW_ST_SYNC_FOLLOW, tsf);
    atomic_fetch_add(&db->n_log_sync_ns, now_ns() - s0);
    return rc;
}

// MARK: - compaction -

typedef struct { mw_db *db; int fd; size_t pgsz; uint8_t *page; int rc; uint64_t pages; uint32_t *list; uint64_t *locs; uint32_t n, cap; } cmp_ctx;

static void cmp_collect (void *p, uint32_t pgno, uint64_t epoch, uint64_t loc) {
    cmp_ctx *c = p;
    (void)epoch;
    if (c->rc != SQLITE_OK) return;
    if (c->n == c->cap) {
        uint32_t cap = c->cap ? c->cap * 2 : 1024;
        uint32_t *l = realloc(c->list, cap * sizeof *l); uint64_t *o = realloc(c->locs, cap * sizeof *o);
        if (!l || !o) { c->rc = SQLITE_NOMEM; if (l) c->list = l; if (o) c->locs = o; return; }
        c->list = l; c->locs = o; c->cap = cap;
    }
    c->list[c->n] = pgno; c->locs[c->n] = loc; c->n++;
}

// Which process compacts: the first one whose compactor thread claims it. Every process has such a thread, and with 1000 of them a timer or a full log
// would send all of them to the compaction lock at once (a syscall each, for nothing). A claim is one CAS in the shared header; a claimant that died is
// replaced after 2 s, and the periodic ones respect one shared interval whichever process runs them.
bool mw_shared_compact_claim (mw_db *db, bool by_size) {
    mw_shm *sh = db->shm;
    uint64_t now = now_ns(), req = atomic_load(&sh->compact_req_ns);
    if (req && now - req < 2000000000ull) {
        int32_t who = atomic_load(&sh->compact_req_pid);                                   // (0: the claimant has not written its number yet)
        if (who <= 0 || mw_mp_pid_alive(db, who)) return false;                           // the claimant is gone (a kill, a crash): its claim is not waited for, nor what it announced to be reading
    }
    if (!by_size) {
        uint64_t iv = (uint64_t)(db->compact_interval_ms > 0 ? db->compact_interval_ms : 1000) * 1000000ull, end = atomic_load(&sh->compact_end_ns);
        if (end && now - end < iv / 2) return false;
    }
    if (!atomic_compare_exchange_strong(&sh->compact_req_ns, &req, now)) return false;
    atomic_store(&sh->compact_req_pid, (int32_t)getpid());
    db->compact_claimed = true;
    return true;
}

int mw_shared_compact (mw_db *db, mw_compact_result *out) {
    mw_compact_result local;
    if (!out) out = &local;
    memset(out, 0, sizeof *out);
    mw_shm *sh = db->shm;
    if (!db->ix || !db->sl || atomic_load(&db->failed)) return SQLITE_OK;
    pthread_mutex_lock(&db->compact_mu);
    uint64_t t0 = now_ns();
    int rc = SQLITE_OK;
    cmp_ctx c; memset(&c, 0, sizeof c);
    bool locked = false;
    if (!mw_mp_compaction_lock(db)) goto done;                                   // one compactor at a time across processes
    locked = true;
    // the target is chosen under the publication lock: the GC (which runs there) and we agree on what must not be freed
    mw_mp_reap_dead_slots(db);
    uint64_t T, base;
    mw_mp_lock(db);
    T = mw_mp_compaction_target(db);
    uint64_t visible = atomic_load(&sh->committed_epoch);
    if (T > visible) T = visible;
    base = atomic_load(&sh->base_epoch);
    atomic_store(&sh->compact_busy_T, T > base ? T : 0);                       // (we hold the compaction lock: a value that is there is the one of a compactor that died)
    const uint64_t lp = atomic_load_explicit(&sh->log_pos, memory_order_acquire);        // (where the log ends with the commits up to T in it: they are visible before their fsync, so what goes into the file must first be durable in the log)
    mw_mp_unlock(db);
    if (T <= base) goto done;
    int fd = real_fd(db);
    if (fd < 0) { rc = SQLITE_CANTOPEN; goto done_busy; }
    rc = mw_seglog_sync(db, (uint32_t)MW_LOG_GEN(lp), MW_LOG_END(lp));                    // (the pages of the commits up to T go into the file only when the log has them on the disk: otherwise a power failure leaves the file ahead of the log, and a b-tree of pages of different commits)
    if (rc != SQLITE_OK) goto done_busy;
    c.db = db; c.fd = fd; c.pgsz = (size_t)db->store->pgsz;
    c.page = malloc(c.pgsz);
    if (!c.page) { rc = SQLITE_NOMEM; goto done_busy; }
    shidx_scan(db->ix, base, T, cmp_collect, &c);
    rc = c.rc;
    for (uint32_t i = 0; i < c.n && rc == SQLITE_OK; i++) {
        if (mw_seglog_read(db, c.locs[i], 0, (uint32_t)c.pgsz, c.page) != 1) { rc = SQLITE_IOERR_READ; break; }
        ssize_t w = mw_io_pwrite(fd, c.page, c.pgsz, (off_t)(c.list[i] - 1) * (off_t)c.pgsz);
        if (w != (ssize_t)c.pgsz) rc = SQLITE_IOERR_WRITE; else out->pages_written++;
    }
    uint32_t size_pages = 0;
    if (rc == SQLITE_OK && shidx_dbsize(db->ix, T, &size_pages) && size_pages && mw_io_ftruncate(fd, (off_t)size_pages * (off_t)c.pgsz) != 0) rc = SQLITE_IOERR_TRUNCATE;
    if (rc == SQLITE_OK && mw_io_fsync(fd) != 0) rc = SQLITE_IOERR_FSYNC;
    if (rc == SQLITE_OK) mw_fault_hit(MW_CRASH_COMPACT_PAGES);
    if (rc == SQLITE_OK) {                                                      // the base becomes durable, then visible
        rc = mw_seglog_set_base(db, T);
        if (rc == SQLITE_OK) {
            if (size_pages) atomic_store(&sh->base_dbsize, size_pages);
            atomic_store(&sh->base_epoch, T);
            mw_fault_hit(MW_CRASH_COMPACT_BASE);
        }
    }
done_busy:
    atomic_store(&sh->compact_busy_T, 0);
    if (rc == SQLITE_OK) {
        mw_seglog_trim(db, T);
        out->target_epoch = T;
        atomic_fetch_add(&db->n_compactions, 1);
        atomic_fetch_add(&db->n_compacted_pages, out->pages_written);
    }
done:
    if (locked) mw_mp_compaction_unlock(db);
    free(c.page); free(c.list); free(c.locs);
    out->duration_ns = now_ns() - t0;
    atomic_fetch_add(&db->n_compaction_ns, out->duration_ns);
    if (getenv("MW_COMPACT_TRACE")) fprintf(stderr, "compact: end t=%.0fms pid=%d took=%.0fms pages=%llu rc=%d\n", (double)(now_ns() / 1000000ull % 100000), (int)getpid(), (double)(now_ns() - t0) / 1e6, (unsigned long long)out->pages_written, rc);
    atomic_store(&sh->compact_end_ns, now_ns());
    if (db->compact_claimed) { db->compact_claimed = false; atomic_store(&sh->compact_req_pid, 0); atomic_store(&sh->compact_req_ns, 0); }
    pthread_mutex_unlock(&db->compact_mu);
    return rc;
}
