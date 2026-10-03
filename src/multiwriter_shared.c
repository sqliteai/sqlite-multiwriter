//
//  multiwriter_shared.c
//  cloudsync
//
//  Shared mode of multi-process Multi-Writer (URI mw_mp=2): the version index and the page images are shared by every process, nobody keeps a private store.
//
//    "<db>-mwidx"   the shared version index (multiwriter_shidx.h): page, snapshot epoch -> where the page image is
//    "<db>-mw.<N>"  the segmented log (multiwriter_seglog.h): the page images, durable, appended once, deleted when compacted
//    "<db>-mwlock"  the shared header (multiwriter_mp.c): publication lock, committed epoch, registry of snapshots, compaction target, liveness
//
//  A read is an index lookup and a copy from a mapped segment; a commit is validated against the index and appended to the log by the publisher, under the publication
//  lock, once, for everybody; nobody has to apply anything: the work per commit that every process used to do (and the memory of its copy of the versions) is gone.
//  Compaction, snapshots registration (the floor under which versions may be freed) and process liveness are those of the other multi-process mode.
//

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include "multiwriter_io.h"
#include "multiwriter_internal.h"
#include "multiwriter_seglog.h"
#include "multiwriter_meta_priv.h"

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
    return mw_seglog_read(db, loc, poff, n, dst) ? 1 : 0;                       // (the segment is gone: the version is in the real file, which holds the same page)
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
    if (!mw_seglog_read(db, loc, 0, (uint32_t)db->store->pgsz, dst)) {            // its segment was deleted: the real file has it
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
    if (ext_len && ((replay_ctx *)ctx)->db->rx) { int mrc = mm_replay(((replay_ctx *)ctx)->db, epoch, ext, ext_len, ext_loc); if (mrc != SQLITE_OK) return mrc; }
    mw_db *db = ((replay_ctx *)ctx)->db;
    if (shidx_room(db->ix) < (uint32_t)n + 2) shidx_gc_floor(db->ix, epoch - 1, db->base_epoch);        // (nobody else exists yet)
    int rc = shidx_install(db->ix, epoch, dbsize, n, pgnos, locs);
    if (rc != 0) return SQLITE_FULL;
    shidx_publish(db->ix, epoch);
    return SQLITE_OK;
}

int mw_shared_open (mw_db *db) {
    char *ixp = mw_sidecar_path(db->path, "mwidx");
    if (!ixp) return SQLITE_NOMEM;
    if (db->mp_first) shidx_unlink(ixp);                                        // volatile: rebuilt from the log below
    shidx_params p = { 24, 4u << 20, MW_MP_SLOTS, 0 };
    const char *e = getenv("MW_IDX_ENTRIES");
    if (e && atoi(e) > 1000) p.max_entries = (uint32_t)atoi(e);
    db->ix = shidx_open(ixp, &p);
    sqlite3_free(ixp);
    if (!db->ix) return SQLITE_CANTOPEN;
    db->store->shared_db = db;
    if (db->mp_first) atomic_store(&db->shm->cdc_on, db->cdc ? 1u : 0u);
    else if (atomic_load(&db->shm->cdc_on) && !db->cdc) { int crc = mw_cdc_open(db); if (crc != SQLITE_OK) return crc; }           // the database captures metadata: so does every process
    else if (db->cdc && !atomic_load(&db->shm->cdc_on)) return SQLITE_MISUSE;                                                   // ... and one that was opened without cannot start to
    if (db->cdc) {                                                              // the shared index of the CRDT metadata (volatile: rebuilt from the log below), and the owner maps
        char *rxp = mw_sidecar_path(db->path, "mwrow"); if (!rxp) return SQLITE_NOMEM;
        if (db->mp_first) { shidx_unlink(rxp); int crc = mw_cdc_shared_create(db); if (crc != SQLITE_OK) { sqlite3_free(rxp); return crc; } }
        shidx_params rp = { 21, 4u << 20, 16, 0 };
        const char *re = getenv("MW_ROWIDX_ENTRIES"); if (re && atoi(re) > 1000) rp.max_entries = (uint32_t)atoi(re);
        db->rx = shidx_open(rxp, &rp); sqlite3_free(rxp);
        if (!db->rx) return SQLITE_CANTOPEN;
    }
    uint64_t base = 1, last = 1;
    replay_ctx rc_ctx = { db };
    if (db->mp_first) db->base_epoch = 1;
    int rc = mw_seglog_open(db, db->mp_first ? replay_cb : NULL, &rc_ctx, &base, &last);
    if (rc != SQLITE_OK) return rc;
    if (db->mp_first) {
        db->base_epoch = base;
        if (shidx_committed(db->ix) < last) shidx_publish(db->ix, last);
        if (db->rx && shidx_committed(db->rx) < last) shidx_publish(db->rx, last);
        atomic_store(&db->epoch, last);                                          // (mw_mp_finish_open copies these into the shared header)
        atomic_store(&db->next_epoch, last);
        atomic_store(&db->shm->base_dbsize, db->store->base_dbsize);
    }
    return SQLITE_OK;
}

int mw_shared_open_finish (mw_db *db) {
    mw_shm *sh = db->shm;
    if (db->mp_first) {
        atomic_store(&sh->log_pos, MW_LOG_POS(atomic_load(&sh->sl_seg), atomic_load(&sh->sl_end)));
        atomic_store(&sh->committed_epoch, atomic_load(&db->epoch));
    }
    return SQLITE_OK;
}

void mw_shared_close (mw_db *db, bool sole) {
    char *ixp = sole ? mw_sidecar_path(db->path, "mwidx") : NULL;
    if (db->sl) {
        if (sole) {                                                              // the last process: everything is in the real file now (or the log stays for recovery)
            uint32_t mn = atomic_load(&db->shm->seg_min), cur = atomic_load(&db->shm->sl_seg);
            bool clean = atomic_load(&db->shm->base_epoch) == atomic_load(&db->shm->committed_epoch) && !atomic_load(&db->failed);
            mw_seglog_close(db);
            if (clean) {
                for (uint32_t s = mn; s <= cur + 1; s++) { char p[700]; snprintf(p, sizeof p, "%s-mw.%u", db->path, s); unlink(p); }     // (and the next one, prepared ahead)
                char p[700]; snprintf(p, sizeof p, "%s-mw.%u.new", db->path, cur + 1); unlink(p);
            }
        } else mw_seglog_close(db);
    }
    if (db->ix) { shidx_close(db->ix); db->ix = NULL; }
    if (db->rx) { shidx_close(db->rx); db->rx = NULL; }
    if (ixp) { shidx_unlink(ixp); sqlite3_free(ixp); char *rxp = mw_sidecar_path(db->path, "mwrow"); if (rxp) { shidx_unlink(rxp); sqlite3_free(rxp); } mw_cdc_shared_unlink(db); }
}

// MARK: - a publisher died inside the publication lock -
// The lock is taken from a process that is gone. If it had appended its record and not yet made the commit visible, the record is complete (finish the commit: install its pages, its
// metadata, publish) or torn (undo: the cursor goes back, the header is cleared). Either way nobody else saw the commit, and the log has exactly one record for every epoch.
void mw_shared_repair (mw_db *db) {
    mw_shm *sh = db->shm;
    uint64_t pe = atomic_load_explicit(&sh->pend_epoch, memory_order_acquire);
    if (!pe) return;
    uint64_t committed = atomic_load(&sh->committed_epoch);
    uint32_t seg = atomic_load(&sh->pend_seg); uint64_t off = atomic_load(&sh->pend_off);
    if (pe != committed + 1) { atomic_store(&sh->pend_epoch, 0); return; }                         // (it did get visible: nothing to do)
    uint64_t epoch = 0, size = 0, ext_loc = 0; uint32_t dbsize = 0, ext_len = 0; int n = 0; uint32_t *pgnos = NULL; uint64_t *locs = NULL;
    int rc = mw_seglog_peek(db, seg, off, &epoch, &dbsize, &n, &pgnos, &locs, &ext_len, &ext_loc, &size);
    if (rc != SQLITE_OK || epoch != pe) {                                                         // torn or never written: undo
        mw_seglog_discard(db, seg, off);
        atomic_store(&sh->sl_seg, seg); atomic_store_explicit(&sh->sl_end, off, memory_order_release);
        atomic_store(&sh->pend_epoch, 0);
        return;
    }
    // complete: finish it (installing twice what the dead one had already installed only adds equal versions)
    if (shidx_room(db->ix) >= (uint32_t)n + 2) shidx_install(db->ix, epoch, dbsize, n, pgnos, locs);
    if (db->rx && ext_len) {
        uint8_t *ext = malloc(ext_len);
        if (ext && mw_seglog_read(db, ext_loc, 0, ext_len, ext)) mm_replay(db, epoch, ext, ext_len, ext_loc);
        free(ext);
    }
    if (db->cdc) atomic_store(&sh->own_cookie, 0);                                                // (the owner maps follow commits by their pages: this one's were not applied: rebuilt at the next use)
    atomic_store(&sh->sl_seg, seg); atomic_store_explicit(&sh->sl_end, off + size, memory_order_release);
    atomic_store(&db->next_epoch, epoch);
    atomic_store_explicit(&sh->log_pos, MW_LOG_POS(seg, off + size), memory_order_release);
    shidx_publish(db->ix, epoch);
    if (db->rx) shidx_publish(db->rx, epoch);
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
    if (db->rx) {
        uint64_t org = atomic_load(&sh->dv_origin), fl = atomic_load(&sh->meta_flushed), fle = fl > org ? fl - org : 0;      // (the flushed point as an epoch of this incarnation)
        // the index of the metadata is collected when a flush moved the point it can forget to (a few times a second), or when it is filling up: the chains of the buckets are many and
        // this runs under the publication lock
        if (fl != atomic_load(&sh->rx_gc_base) || shidx_room(db->rx) < (1u << 20)) {
            shidx_gc_floor(db->rx, floor, fle);                                      // (the buckets older than the last flush are in the file)
            atomic_store(&sh->rx_gc_base, fl);
        } else goto rx_done;
        uint32_t np = atomic_load(&sh->npurge), k = 0;                               // a dropped table is forgotten once the file and the index both are clean of it
        for (uint32_t q = 0; q < np; q++) {
            uint64_t e = atomic_load(&sh->purge[q].epoch);
            if (e <= fl && e <= floor + org) continue;                                       // (the floor too: a head older than a snapshot's epoch stays in its chain until the snapshot is gone)
            if (k != q) { atomic_store(&sh->purge[k].epoch, e); atomic_store(&sh->purge[k].tbl, atomic_load(&sh->purge[q].tbl)); }
            k++;
        }
        if (k != np) atomic_store_explicit(&sh->npurge, k, memory_order_release);
    rx_done:;
    }
}

// One commit, under the publication lock: validate against the index, append to the log, install in the index, publish.
int mw_shared_publish (mw_db *db, mw_lane *lane, const mw_validate *v, const uint32_t *pgnos, const uint8_t *const *images, int n, uint32_t ws_dbsize, uint32_t snap_dbsize, int sync, uint64_t *out_epoch) {
    mw_shm *sh = db->shm;
    shidx *ix = db->ix;
    size_t pgsz = (size_t)db->store->pgsz;
    const bool adopt = v && v->adopt_images;
    #define ADOPT_FREE() do { if (adopt) for (int _i = 0; _i < n; _i++) free((void *)images[_i]); } while (0)
    int rc = SQLITE_OK;
    if (atomic_load(&db->failed)) { ADOPT_FREE(); return SQLITE_IOERR; }

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
                    bool have_old = mw_shared_read(db, pg, sn, 0, (uint32_t)pgsz, oldp) != 0;
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
        if (rc == SQLITE_OK && db->rx) rc = mm_validate(db, lane);               // the buckets of the metadata this commit writes: unchanged since the transaction read them
        if (rc != SQLITE_OK) { ADOPT_FREE(); return rc; }
    }
    MW_T1(MW_ST_SH_VALIDATE, tv0);

    // ---- room in the index (it is full only if compaction cannot keep up): a record must not reach the log if its versions cannot be installed ----
    if (shidx_room(ix) < (uint32_t)n + 2) {
        shared_gc(db);
        if (shidx_room(ix) < (uint32_t)n + 2) { mw_db_compactor_kick(db); ADOPT_FREE(); return SQLITE_FULL; }
    }

    if (db->rx && lane && lane->cdc_ng && shidx_room(db->rx) < (uint32_t)lane->cdc_ng + 2) {            // the metadata index is full: only a flush frees it (the flusher is asked; this commit waits for it a little)
        for (int w = 0; w < 4000 && shidx_room(db->rx) < (uint32_t)lane->cdc_ng + 2; w++) { shared_gc(db); mw_cdc_kick_flush(db); if (shidx_room(db->rx) >= (uint32_t)lane->cdc_ng + 2) break; struct timespec ts = { 0, 500000 }; nanosleep(&ts, NULL); }
        if (shidx_room(db->rx) < (uint32_t)lane->cdc_ng + 2) { ADOPT_FREE(); return SQLITE_FULL; }
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
    rc = mw_seglog_append(db, epoch, new_dbsize, n, pgnos, images, lane ? lane->cdc_ext : NULL, lane ? lane->cdc_ext_len : 0, locs, &ext_loc, &seg, &end);
    mw_fault_hit(MW_CRASH_SHARED_APPENDED);                                           // (the record is complete, nothing is installed)
    MW_T1(MW_ST_APPEND, ta0);
    if (rc != SQLITE_OK) { if (n > 16) free(locs); ADOPT_FREE(); return rc; }          // (the append failed before it touched anything the others can see: this commit fails, the database does not)
    uint64_t ti0 = MW_T0();
    if (shidx_install(ix, epoch, new_dbsize, n, pgnos, locs) != 0) { if (n > 16) free(locs); ADOPT_FREE(); atomic_store(&db->failed, 1); return SQLITE_FULL; }
    MW_T1(MW_ST_SH_INSTALL, ti0);
    if (n > 16) free(locs);
    if (db->cdc && lane) {                                                       // the metadata of the commit: its buckets in the shared index, the owner maps follow the commit's pages
        uint64_t to0 = MW_T0();
        mw_cdc_apply_owner(db, lane, pgnos, images, n);
        MW_T1(MW_ST_SH_OWNER, to0); to0 = MW_T0();
        if (db->rx) { int mrc = mm_install(db, lane, epoch, ext_loc); if (mrc != SQLITE_OK) { ADOPT_FREE(); atomic_store(&db->failed, 1); return mrc; } }
        MW_T1(MW_ST_SH_MM, to0);
    }
    ADOPT_FREE();
    if (lane) { lane->sl_seg = seg; lane->sl_end = end; }
    mw_fault_hit(MW_CRASH_SHARED_INSTALLED);                                          // (installed, not visible)
    atomic_store(&db->next_epoch, epoch);
    atomic_store_explicit(&sh->log_pos, MW_LOG_POS(seg, end), memory_order_release);
    shidx_publish(ix, epoch);
    if (db->rx) shidx_publish(db->rx, epoch);
    if (db->cdc && lane) { uint64_t tc0 = MW_T0(); mw_cdc_apply_cells(db, lane, epoch); MW_T1(MW_ST_SH_CELLS, tc0); }
    atomic_store_explicit(&sh->committed_epoch, epoch, memory_order_release);        // (the record is visible to the other processes before it is durable: as in the other mode, an acknowledged commit is never lost)
    atomic_store(&db->epoch, epoch);
    atomic_store_explicit(&sh->pend_epoch, 0, memory_order_release);                 // (visible: nothing is pending any more)
    mw_fault_hit(MW_CRASH_AFTER_LOG);
    mw_fault_hit(MW_CRASH_AFTER_VISIBLE);
    if (!lane && sync) { int src = mw_seglog_sync(db, seg, end); if (src != SQLITE_OK) { atomic_store(&db->failed, 1); return src; } }
    atomic_store(&sh->procs[db->mp_proc].applied, epoch);
    if (db->gc_interval > 0 && (atomic_fetch_add(&db->publishes_since_gc, 1) + 1) >= (uint64_t)db->gc_interval) { atomic_store(&db->publishes_since_gc, 0); shared_gc(db); }
    if (db->log_max_bytes && mw_seglog_bytes(db) > mw_log_limit(db)) {
        mw_db_compactor_kick(db);
        if (mw_seglog_bytes(db) > db->log_max_bytes * 8) {                                         // the log is far ahead of its compaction: commits slow down in proportion to the overshoot (after the lock is released: mw_db_publish_finish)
            atomic_fetch_add(&db->n_backpressure, 1);
            uint64_t over = mw_seglog_bytes(db) / (db->log_max_bytes * 8);
            if (lane) { lane->bp_wait_us = (uint32_t)(over > 20 ? 20000 : over * 1000); extern _Atomic uint64_t mw_bp[4]; atomic_fetch_add(&mw_bp[0], lane->bp_wait_us); atomic_fetch_add(&mw_bp[1], 1); } else { struct timespec ts = { 0, 500000 }; nanosleep(&ts, NULL); }
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
    if (req && now - req < 2000000000ull) return false;
    if (!by_size) {
        uint64_t iv = (uint64_t)(db->compact_interval_ms > 0 ? db->compact_interval_ms : 1000) * 1000000ull, end = atomic_load(&sh->compact_end_ns);
        if (end && now - end < iv / 2) return false;
    }
    if (!atomic_compare_exchange_strong(&sh->compact_req_ns, &req, now)) return false;
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
    if (db->cdc) { uint64_t lim = mw_cdc_safe_epoch(db); if (T > lim) T = lim; }       // (the metadata of the commits above the last flush lives in the log records only)
    base = atomic_load(&sh->base_epoch);
    if (T > base) atomic_store(&sh->compact_busy_T, T);
    if (getenv("MW_COMPACT_TRACE")) fprintf(stderr, "compact: t=%.0fms pid=%d waited=%.0fms base=%llu T=%llu visible=%llu safe=%llu log=%.1f MB\n", (double)(now_ns() / 1000000ull % 100000), (int)getpid(), (double)(now_ns() - t0) / 1e6, (unsigned long long)base, (unsigned long long)T, (unsigned long long)visible, (unsigned long long)(db->cdc ? mw_cdc_safe_epoch(db) : 0), (double)mw_seglog_bytes(db) / 1048576.0);
    mw_mp_unlock(db);
    if (T <= base) goto done;
    int fd = real_fd(db);
    if (fd < 0) { rc = SQLITE_CANTOPEN; goto done_busy; }
    c.db = db; c.fd = fd; c.pgsz = (size_t)db->store->pgsz;
    c.page = malloc(c.pgsz);
    if (!c.page) { rc = SQLITE_NOMEM; goto done_busy; }
    shidx_scan(db->ix, base, T, cmp_collect, &c);
    rc = c.rc;
    for (uint32_t i = 0; i < c.n && rc == SQLITE_OK; i++) {
        if (!mw_seglog_read(db, c.locs[i], 0, (uint32_t)c.pgsz, c.page)) { rc = SQLITE_IOERR_READ; break; }
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
    if (db->compact_claimed) { db->compact_claimed = false; atomic_store(&sh->compact_req_ns, 0); }
    pthread_mutex_unlock(&db->compact_mu);
    return rc;
}
