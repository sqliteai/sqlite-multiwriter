//
//  multiwriter_lane.c
//  cloudsync
//
//  Private write lanes. In private-lane mode ("mw=2") each connection gets:
//    - its own in-memory "-wal" (no real file): SQLite believes it is the only writer, so
//      there is no WAL write-lock contention between connections;
//    - its own private wal-index (xShmMap memory) and no-op shm locks;
//    - a main-database file whose writes are swallowed (the real file is only ever written
//      by the compactor).
//  The physical write set of a transaction is the set of page numbers of the frames
//  SQLite appended to the private WAL up to the commit frame. When the snapshot ends the
//  private WAL is emptied and the wal-index header invalidated: SQLite's own recovery path
//  then rebuilds an empty WAL and reports "changed", which resets the page cache. Every
//  transaction therefore starts with a cold page cache; this is also what makes the set of
//  pages read through xRead a complete read-set.
//

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/stat.h>
#include "multiwriter_internal.h"

#define MW_HOT_CREDIT 16     // serialised transactions after a conflict, decaying by one per successful commit

static _Thread_local mw_lane *tls_lane;      // lane whose main file this thread touched last (pairs a new WAL with its lane)
static sqlite3_io_methods lane_main_io;
static sqlite3_io_methods lane_wal_io;
static const sqlite3_io_methods *pass_io;

#define FILE_LANE(pf) (((mw_file *)(pf))->lane)
#define REAL(pf)      (((mw_file *)(pf))->real)

static uint32_t be32 (const uint8_t *p) { return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) | ((uint32_t)p[2] << 8) | p[3]; }

static int cmp_u32 (const void *a, const void *b) { uint32_t x = *(const uint32_t *)a, y = *(const uint32_t *)b; return x < y ? -1 : x > y; }

// MARK: - write set -

// Adds `pgno` (image in frame `frame`) to the write set; a later frame of the same page supersedes.
static inline uint32_t ws_slot (uint32_t pgno, uint32_t cap) { return (pgno * 2654435761u) & (cap - 1); }
static int ws_hash_rebuild (mw_lane *lane, uint32_t cap) {
    int *h = sqlite3_malloc64((sqlite3_uint64)cap * sizeof(int));
    if (!h) return SQLITE_NOMEM;
    memset(h, 0, (size_t)cap * sizeof(int));
    for (int i = 0; i < lane->ws_n; i++) { uint32_t k = ws_slot(lane->ws_pgnos[i], cap); while (h[k]) k = (k + 1) & (cap - 1); h[k] = i + 1; }
    sqlite3_free(lane->ws_hash);
    lane->ws_hash = h;
    lane->ws_hash_cap = cap;
    return SQLITE_OK;
}
static int ws_add (mw_lane *lane, uint32_t pgno, int frame) {
    if ((uint32_t)(lane->ws_n + 1) * 2 > lane->ws_hash_cap) {
        uint32_t cap = lane->ws_hash_cap ? lane->ws_hash_cap : 64;
        while (cap < (uint32_t)(lane->ws_n + 1) * 4) cap *= 2;
        int rc = ws_hash_rebuild(lane, cap);
        if (rc != SQLITE_OK) return rc;
    }
    uint32_t hk = ws_slot(pgno, lane->ws_hash_cap);
    while (lane->ws_hash[hk]) {
        int i = lane->ws_hash[hk] - 1;
        if (lane->ws_pgnos[i] == pgno) { lane->ws_frame[i] = frame; return SQLITE_OK; }
        hk = (hk + 1) & (lane->ws_hash_cap - 1);
    }
    if (lane->ws_n == lane->ws_cap) {
        int cap = lane->ws_cap ? lane->ws_cap * 2 : 16;
        uint32_t *p = sqlite3_realloc64(lane->ws_pgnos, (sqlite3_uint64)cap * sizeof(uint32_t));
        if (!p) return SQLITE_NOMEM;
        lane->ws_pgnos = p;
        int *q = sqlite3_realloc64(lane->ws_frame, (sqlite3_uint64)cap * sizeof(int));
        if (!q) return SQLITE_NOMEM;
        lane->ws_frame = q;
        lane->ws_cap = cap;
    }
    lane->ws_pgnos[lane->ws_n] = pgno;
    lane->ws_frame[lane->ws_n++] = frame;
    lane->ws_hash[hk] = lane->ws_n;
    return SQLITE_OK;
}

// Schema cookie (page 1, bytes 40..43) as seen at the lane's snapshot.
static uint32_t lane_schema_cookie (mw_lane *lane) {
    uint8_t c[4] = {0};
    mw_store *st = lane->db->store;
    if (!mw_store_read(st, 1, lane->tx.snapshot_epoch, 40, 4, c)) {
        mw_file *f = lane->file;
        f->real->pMethods->xRead(f->real, c, 4, 40);
    }
    return ((uint32_t)c[0] << 24) | ((uint32_t)c[1] << 16) | ((uint32_t)c[2] << 8) | c[3];
}

// A conflicting transaction can be rebased only if everything it did has a logical (sqlite-sync)
// representation: no DDL (its page 1 carries a different schema cookie than the snapshot's), no write to
// an untracked table or by a remote apply (both flagged through the preupdate hook), at least one
// reserved db_version, and it is not itself a rebase helper.
static bool lane_can_rebase (mw_lane *lane, const uint8_t *pg1, uint32_t snapshot_cookie) {
    if (lane->norebase || !lane->rebasable || lane->nresv == 0) return false;
    if (lane->commit_base > 0) return false;                     // a later commit inside one read snapshot: the overlay would miss the earlier one's pages
    if (pg1) {
        uint32_t c = ((uint32_t)pg1[40] << 24) | ((uint32_t)pg1[41] << 16) | ((uint32_t)pg1[42] << 8) | pg1[43];
        if (c != snapshot_cookie) return false;                  // DDL in this transaction
    }
    return true;
}

// Publishes the captured write set as one atomic commit at a new epoch.
// Consecutive refusals after which a long transaction takes the turn (MW_LONG_STARVE).
static int long_starve (void) { static _Atomic int c = MW_KNOB_UNSET; return mw_knob_int(&c, "MW_LONG_STARVE", 2); }

static int lane_publish (mw_lane *lane) {
    mw_db *db = lane->db;
    mw_memwal *w = &lane->wal;
    size_t fs = (size_t)w->pgsz + 24;
    const uint8_t **imgs = malloc((size_t)lane->ws_n * sizeof(uint8_t *));
    if (!imgs) return SQLITE_NOMEM;
    const uint8_t *pg1 = NULL;                                   // page 1 image if the transaction wrote it
    for (int i = 0; i < lane->ws_n; i++) {
        imgs[i] = w->buf + 32 + (size_t)lane->ws_frame[i] * fs + 24;
        if (lane->ws_pgnos[i] == 1) pg1 = imgs[i];
    }
    if (!lane->dsz_valid || lane->dsz_epoch != lane->tx.snapshot_epoch) { lane->dsz_val = mw_store_dbsize(db->store, lane->tx.snapshot_epoch); lane->dsz_epoch = lane->tx.snapshot_epoch; lane->dsz_valid = true; }
    uint32_t snap_size = lane->dsz_val;
    uint32_t cookie = lane_schema_cookie(lane);
    // pages read but not written: mark the write set in the (sorted) bitmap, walk the read list
    uint32_t *ro = NULL;
    int nro = 0;
    if (lane->readcheck && lane->rs_n > 0) {
        ro = malloc((size_t)lane->rs_n * sizeof(uint32_t));
        if (!ro) { free(imgs); return SQLITE_NOMEM; }
        for (int i = 0; i < lane->ws_n; i++) if (lane->ws_pgnos[i] < lane->rs_bits_cap) lane->rs_bits[lane->ws_pgnos[i]] |= 2;
        for (int i = 0; i < lane->rs_n; i++) if (!(lane->rs_bits[lane->rs_list[i]] & 2)) ro[nro++] = lane->rs_list[i];
        for (int i = 0; i < lane->ws_n; i++) if (lane->ws_pgnos[i] < lane->rs_bits_cap) lane->rs_bits[lane->ws_pgnos[i]] &= (uint8_t)~2;
    }
    { static _Atomic int tr = MW_KNOB_UNSET; if (mw_knob_flag(&tr, "MW_PAGE_TRACE")) { char line[2048]; int o = snprintf(line, sizeof line, "PAGES n=%d:", lane->ws_n); for (int i = 0; i < lane->ws_n && o < (int)sizeof line - 12; i++) o += snprintf(line + o, sizeof line - (size_t)o, " %u", (unsigned)lane->ws_pgnos[i]); fprintf(stderr, "%s\n", line); } }   // (diagnostic: the page numbers of every commit, to map them to B-trees offline)
    if (mw_rowdiff_enabled() && !lane->norebase) mw_rowdiff_commit(lane, imgs);
    if (db->cdc && !lane->norebase) mw_cdc_prepare(lane, imgs);
    mw_validate v = { .snapshot_epoch = lane->tx.snapshot_epoch, .check_cookie = true, .cookie = cookie, .read_pgnos = ro, .n_read = nro,
                      .own_pgnos = lane->own_pg, .own_n = lane->own_n, .own_epoch = lane->own_epoch };
    uint64_t epoch = 0;
    uint64_t tp0 = MW_T0();
    // How long did this transaction stay open? A per-database turn (hot-spot escalation) is held from the snapshot to the commit: it pays for transactions that last
    // microseconds and serialises everything for those that last milliseconds (an application working inside the transaction: 5 ms think time made the median 32 ms
    // and the p99 130 ms with 8 agents on 4 hot rows). Long transactions only get the turn when they are starving (see `starving` below).
    if (lane->tx_t0) {
        struct timespec t1; clock_gettime(CLOCK_MONOTONIC, &t1);
        uint64_t d = (uint64_t)t1.tv_sec * 1000000000ull + (uint64_t)t1.tv_nsec - lane->tx_t0;
        lane->tx_long_run = d > 2000000 ? (lane->tx_long_run < 8 ? lane->tx_long_run + 1 : 8) : 0;   // (a run of them, not an average: one descheduled thread must not change the class)
    }
    const bool long_tx = lane->tx_long_run >= 3;                 // > 2 ms from snapshot to commit three times in a row (a 100-row insert is ~0.5 ms; this is an application working inside the transaction)
    // Multi-process: ONE round of the publication lock per commit. The plain attempt, and the relocation if it is refused because other commits extended the file, both run
    // inside the same hold (the relocation is prepared against the state it publishes on), instead of queueing again for the relocation: with N processes every wait for the
    // lock is long, somebody commits meanwhile and the prepared relocation would be refused again (3.7 lock rounds per commit at 128 processes, 335 us of lock per commit).
    // The rebase (tracked tables) replays SQL and runs without the lock.
    bool mp_hold = db->mp && !(lane->rs_overflow && lane->readcheck);
    if (mp_hold) { uint64_t tw0 = MW_T0(); mw_gate_enter(db, lane); mw_mp_lock(db); MW_T1(MW_ST_MP_WAIT, tw0); lane->mp_held = true; lane->mp_t0 = MW_T0(); }
    int rc = lane->rs_overflow && lane->readcheck ? MW_CONFLICT_READ
           : mw_db_publish(db, lane, &v, lane->ws_pgnos, imgs, lane->ws_n, lane->ws_dbsize, snap_size, lane->sync_level >= 2, &epoch);
#define MP_RELEASE_NOFINISH() do { if (mp_hold) { mp_hold = false; lane->mp_held = false; MW_T1(MW_ST_MP_HELD, lane->mp_t0); mw_mp_unlock(db); mw_gate_exit(db); mw_log_prefill_bg(db); } } while (0)
#define MP_RELEASE() do { if (mp_hold) { mp_hold = false; lane->mp_held = false; MW_T1(MW_ST_MP_HELD, lane->mp_t0); mw_mp_unlock(db); mw_gate_exit(db); mw_log_prefill_bg(db); \
                          if (rc == SQLITE_OK) rc = mw_db_publish_finish(db, lane, rc, epoch, lane->sync_level >= 2); } } while (0)
    if (rc != MW_CONFLICT) MP_RELEASE();

    if (rc == SQLITE_OK) {
        lane->tx.commit_epoch = epoch;
        lane->tx.state = MW_TX_COMMITTED;
        lane->consec_aborts = 0;
        if (lane->retry_credit > 0) lane->retry_credit--;
        if (pg1 && ((uint32_t)pg1[40] << 24 | (uint32_t)pg1[41] << 16 | (uint32_t)pg1[42] << 8 | pg1[43]) != cookie) {   // schema change
            atomic_store(&db->last_schema_epoch, epoch);
            if (db->mp) atomic_store(&db->shm->schema_epoch, epoch);
            atomic_fetch_add(&db->schema_generation, 1);
        }
    } else if (rc == MW_CONFLICT) {
        // 1. Was the conflict only that other commits extended the file? Then the transaction's new pages are renumbered above the new end and it commits.
        uint64_t tr0 = MW_T0();
        int rr = lane->noreloc ? MW_RELOC_NA : mw_lane_relocate(lane, &v, lane->ws_pgnos, imgs, lane->ws_n, lane->ws_dbsize, snap_size, lane->sync_level >= 2, &epoch);
        if (!lane->noreloc && rr != MW_RELOC_NA) MW_T1(MW_ST_RELOC, tr0);
        if (rr == SQLITE_OK) rc = SQLITE_OK; else MP_RELEASE_NOFINISH();
        if (rr == SQLITE_OK) {
            MP_RELEASE();                                        // (multi-process: unlock, then wait for the group fsync; rc may turn into an I/O error)
            if (rc == SQLITE_OK) { lane->tx.commit_epoch = epoch; lane->tx.state = MW_TX_COMMITTED; lane->consec_aborts = 0; if (lane->retry_credit > 0) lane->retry_credit--; }
        }
        else if (rr != MW_RELOC_NA) rc = rr;
        else if (lane_can_rebase(lane, pg1, cookie)) {
            // 2. Physical conflict, but the transaction is fully described by sqlite-sync changes:
            // discard its pages and replay them at the latest snapshot; stock SQLite regenerates the pages.
            mw_overlay ov = { .images = imgs, .n = lane->ws_n, .dbsize = lane->ws_dbsize, .snapshot = lane->tx.snapshot_epoch };
            if (!long_tx) lane->retry_credit = lane->hot_credit;      // a physical conflict: run the next transactions serialised, on the fast path (not the long ones, see above)
            rc = mw_lane_rebase(lane, &ov);
            if (rc == SQLITE_OK) { lane->tx.state = MW_TX_COMMITTED; lane->consec_aborts = 0; }   // commit_epoch was set by the rebase (the credit granted above is spent by the next commits)
            else if (rc != MW_CONFLICT) { lane->tx.state = MW_TX_ABORTED; atomic_fetch_add(&db->n_aborts, 1); }   // (any other failure also rolls the transaction back)
        }
    }
    free(imgs);
    free(ro);                                                    // (the read set is used again by the relocation above: freed only now)
    MW_T1(MW_ST_PUBLISH, tp0);
    if (rc == MW_CONFLICT || rc == MW_CONFLICT_SCHEMA || rc == MW_CONFLICT_READ) {
        // Not rebasable (or the rebase gave up): the transaction is rolled back, the caller retries.
        if (rc == MW_CONFLICT && !lane->norebase) atomic_fetch_add(&db->n_unrebasable, 1);
        lane->tx.state = MW_TX_ABORTED;
        atomic_fetch_add(&db->n_aborts, 1);
        // A write-write conflict on the same pages repeats, so the next transactions run serialised. A schema change is not a hot spot, and neither is a
        // *read* conflict: an interior b-tree page read on the way down that somebody else split says nothing about the next transaction's pages.
        // Serialising pays off where a conflict is expensive: a tracked (rebasable) transaction that loses costs a whole rebase. An untracked one just runs
        // its statement again, and waiting for a turn costs more than the occasional retry (measured: 8-32 concurrent bulk / append writers, 1.3-1.5x faster without).
        lane->consec_aborts++;
        const bool starving = lane->consec_aborts >= (long_tx ? long_starve() : 16);          // (a long transaction that lost twice in a row takes the turn and keeps it for hot_credit*16 transactions: FIFO queue, bounded tail)
        if (!lane->norebase && rc != MW_CONFLICT_SCHEMA && rc != MW_CONFLICT_READ && ((lane->rebasable && lane->nresv > 0 && !long_tx) || starving)) {
            lane->retry_credit = long_tx ? lane->hot_credit * 16 : lane->hot_credit;
            if (mw_timing_on) { extern _Atomic uint64_t mw_grants[2]; atomic_fetch_add(&mw_grants[(lane->rebasable && lane->nresv > 0) ? 0 : 1], 1); }
        }
        rc = SQLITE_BUSY_SNAPSHOT;
    }
    return rc;
}

// Called when the commit frame (frame index k, db size after commit = dbsize) is complete.
static int lane_commit_frame (mw_lane *lane, int k, uint32_t dbsize) {
    mw_memwal *w = &lane->wal;
    size_t fs = (size_t)w->pgsz + 24;
    if (lane->ws_hash_cap > 4096 && (uint32_t)lane->ws_n * 16 < lane->ws_hash_cap) { sqlite3_free(lane->ws_hash); lane->ws_hash = NULL; lane->ws_hash_cap = 0; }   // (a bulk load must not leave a huge table)
    else if (lane->ws_hash_cap) memset(lane->ws_hash, 0, (size_t)lane->ws_hash_cap * sizeof(int));
    lane->ws_n = 0;
    lane->ws_dbsize = dbsize;
    if (k < lane->commit_base) lane->commit_base = 0;            // (the log was restarted: earlier frames are gone)
    for (int i = lane->commit_base; i <= k; i++) {
        size_t off = 32 + (size_t)i * fs;
        if (off + fs > w->size) return SQLITE_IOERR;
        int rc = ws_add(lane, be32(w->buf + off), i);
        if (rc != SQLITE_OK) return rc;
    }
    w->commit_seen = true;
    lane->tx.ws_pages = (uint32_t)lane->ws_n;
    lane->tx.state = MW_TX_PREPARED;
    int rc = lane_publish(lane);
    if (rc == SQLITE_OK) {
        // The read snapshot may outlive this commit (a statement still stepping): the next commit starts after these frames and must not
        // count this commit's pages as somebody else's change.
        lane->commit_base = k + 1;
        if (lane->own_n + lane->ws_n > lane->own_cap) {
            int cap = lane->own_n + lane->ws_n + 16;
            uint32_t *p = sqlite3_realloc64(lane->own_pg, (sqlite3_uint64)cap * sizeof(uint32_t));
            if (!p) { lane->commit_base = 0; return rc; }
            lane->own_pg = p;
            lane->own_cap = cap;
        }
        for (int i = 0; i < lane->ws_n; i++) lane->own_pg[lane->own_n++] = lane->ws_pgnos[i];
        qsort(lane->own_pg, (size_t)lane->own_n, sizeof(uint32_t), cmp_u32);
        int u = 0;
        for (int i = 0; i < lane->own_n; i++) if (i == 0 || lane->own_pg[i] != lane->own_pg[u - 1]) lane->own_pg[u++] = lane->own_pg[i];
        lane->own_n = u;
        lane->own_epoch = lane->tx.commit_epoch;
    }
    return rc;
}

// MARK: - private WAL (memory file) -

static int wal_close (sqlite3_file *pf) {
    mw_file *f = (mw_file *)pf;
    mw_ev(MW_EV_CLOSE, f, 0, 0, 0);
    if (f->lane) { f->lane->wal_bound = false; f->lane->wal.size = 0; }
    f->base.pMethods = NULL;
    return SQLITE_OK;
}
static int wal_read (sqlite3_file *pf, void *buf, int n, sqlite3_int64 off) {
    mw_memwal *w = &FILE_LANE(pf)->wal;
    mw_ev(MW_EV_READ, (mw_file *)pf, off, n, 1);
    if ((size_t)off >= w->size) { memset(buf, 0, (size_t)n); return SQLITE_IOERR_SHORT_READ; }
    size_t avail = w->size - (size_t)off;
    size_t take = avail < (size_t)n ? avail : (size_t)n;
    memcpy(buf, w->buf + off, take);
    if (take < (size_t)n) { memset((uint8_t *)buf + take, 0, (size_t)n - take); return SQLITE_IOERR_SHORT_READ; }
    return SQLITE_OK;
}
static int wal_write (sqlite3_file *pf, const void *buf, int n, sqlite3_int64 off) {
    mw_lane *lane = FILE_LANE(pf);
    mw_memwal *w = &lane->wal;
    mw_ev(MW_EV_WRITE, (mw_file *)pf, off, n, 1);
    size_t end = (size_t)off + (size_t)n;
    if (end > w->cap) {
        size_t cap = w->cap ? w->cap : 65536;
        while (cap < end) cap *= 2;
        uint8_t *p = sqlite3_realloc64(w->buf, cap);
        if (!p) return SQLITE_NOMEM;
        w->buf = p;
        w->cap = cap;
    }
    memcpy(w->buf + off, buf, (size_t)n);
    if (end > w->size) w->size = end;

    // WAL layout: 32-byte header, then frames of (24-byte header + page). SQLite writes the
    // header and the page of a frame with two calls (walWriteOneFrame); the page write ends it.
    if (off == 0 && n == 32) { w->pgsz = (int)be32(w->buf + 8); return SQLITE_OK; }
    if (n == 24) return SQLITE_OK;                                       // frame header, or checksum rewrite of one
    if (w->pgsz == 0 || n != w->pgsz || off < 32 || ((size_t)off - 32) % ((size_t)w->pgsz + 24) != 24) return SQLITE_IOERR_WRITE;   // unexpected layout: fail loudly
    int k = (int)(((size_t)off - 32) / ((size_t)w->pgsz + 24));
    uint32_t ntrunc = be32(w->buf + 32 + (size_t)k * ((size_t)w->pgsz + 24) + 4);
    if (ntrunc != 0) return lane_commit_frame(lane, k, ntrunc);          // commit frame complete
    return SQLITE_OK;
}
static int wal_truncate (sqlite3_file *pf, sqlite3_int64 size) {
    mw_memwal *w = &FILE_LANE(pf)->wal;
    if ((size_t)size < w->size) w->size = (size_t)size;
    return SQLITE_OK;
}
static int wal_sync (sqlite3_file *pf, int flags) { mw_ev(MW_EV_SYNC, (mw_file *)pf, 0, 0, flags | 0x100); return SQLITE_OK; }
static int wal_filesize (sqlite3_file *pf, sqlite3_int64 *size) { *size = (sqlite3_int64)FILE_LANE(pf)->wal.size; return SQLITE_OK; }
static int wal_nolock (sqlite3_file *pf, int l) { return SQLITE_OK; }
static int wal_check_reserved (sqlite3_file *pf, int *out) { *out = 0; return SQLITE_OK; }
static int wal_file_control (sqlite3_file *pf, int op, void *arg) { return SQLITE_NOTFOUND; }
static int wal_sector_size (sqlite3_file *pf) { return 512; }
static int wal_device_char (sqlite3_file *pf) { return 0; }

int mw_lane_open_wal (mw_file *f, const char *name) {
    mw_lane *lane = tls_lane;
    if (!lane || !lane->private_mode || lane->wal_bound) return SQLITE_CANTOPEN;
    size_t pn = strlen(lane->db->path);
    if (strncmp(name, lane->db->path, pn) != 0 || strcmp(name + pn, "-wal") != 0) return SQLITE_CANTOPEN;   // not the lane we think
    f->lane = lane;
    f->is_wal = 1;
    lane->wal_bound = true;
    lane->wal.size = 0;
    f->base.pMethods = &lane_wal_io;
    return SQLITE_OK;
}

// MARK: - lane reset -

// Header copies live at bytes 0..95 of the first wal-index region. An all-zero header has
// isInit == 0, which SQLite treats as "malformed": it takes the (private) write lock, runs
// walIndexRecover over the (now empty) WAL and reports changed=1 => page cache reset.
void mw_lane_reset (mw_lane *lane) {
    lane->commit_base = 0;
    lane->own_n = 0;
    lane->wal.size = 0;
    lane->wal.commit_seen = false;
    lane->wal.pgsz = 0;
    if (lane->nshm > 0 && lane->shm[0]) memset(lane->shm[0], 0, 96);
}

void mw_lane_free (mw_lane *lane) {
    if (!lane) return;
    if (lane->mp_slot >= 0 && lane->db && lane->db->mp) mw_mp_slot_free(lane->db, lane->mp_slot);
    for (int i = 0; i < lane->nshm; i++) sqlite3_free(lane->shm[i]);
    sqlite3_free(lane->wal.buf);
    sqlite3_free(lane->ws_pgnos);
    sqlite3_free(lane->ws_frame);
    sqlite3_free(lane->own_pg);
    sqlite3_free(lane->ws_hash);
    sqlite3_free(lane->resv);
    sqlite3_free(lane->rs_bits);
    sqlite3_free(lane->rs_list);
    mw_lane_rebase_free(lane);
    free(lane->cdc_keys); free(lane->cdc_vals); free(lane->cdc_freed);
    sqlite3_free(lane);
}

// MARK: - main database file (private lane) -

// Read set: every page fetched through xRead during the snapshot (page 1 excluded: its schema cookie is
// validated separately, and it changes with every file growth). Complete because each transaction
// starts with a cold page cache (see the header comment).
static void rs_mark (mw_lane *lane, uint32_t pgno) {
    if (pgno >= lane->rs_bits_cap) {
        size_t cap = lane->rs_bits_cap ? lane->rs_bits_cap : 4096;
        while (cap <= pgno) cap *= 2;
        uint8_t *p = sqlite3_realloc64(lane->rs_bits, cap);
        if (!p) { lane->rs_overflow = true; return; }                            // cannot track: the commit is refused as a read conflict
        memset(p + lane->rs_bits_cap, 0, cap - lane->rs_bits_cap);
        lane->rs_bits = p;
        lane->rs_bits_cap = cap;
    }
    if (lane->rs_bits[pgno]) return;
    if (lane->rs_n == lane->rs_cap) {
        int cap = lane->rs_cap ? lane->rs_cap * 2 : 64;
        uint32_t *p = sqlite3_realloc64(lane->rs_list, (sqlite3_uint64)cap * sizeof(uint32_t));
        if (!p) { lane->rs_overflow = true; return; }
        lane->rs_list = p;
        lane->rs_cap = cap;
    }
    lane->rs_bits[pgno] = 1;
    lane->rs_list[lane->rs_n++] = pgno;
}

static uint64_t lane_snapshot (mw_lane *lane) {
    return lane->snapshot_held ? lane->tx.snapshot_epoch : mw_db_visible_epoch(lane->db);
}

// Reads are served page by page: newest committed version <= the snapshot, else the real file.
static int lm_read (sqlite3_file *pf, void *buf, int n, sqlite3_int64 off) {
    mw_lane *lane = FILE_LANE(pf);
    tls_lane = lane;
    mw_ev(MW_EV_READ, (mw_file *)pf, off, n, 2);
    mw_store *st = lane->db->store;
    uint64_t snap = lane_snapshot(lane);
    const mw_overlay *ov = lane->overlay;
    uint8_t *dst = buf;
    int remaining = n;
    sqlite3_int64 o = off;
    while (remaining > 0) {
        uint32_t pgno = (uint32_t)(o / st->pgsz) + 1;
        uint32_t poff = (uint32_t)(o % st->pgsz);
        uint32_t take = (uint32_t)st->pgsz - poff;
        if ((int)take > remaining) take = (uint32_t)remaining;
        const uint8_t *oimg = NULL;
        if (ov) {                                   // rebase read view: the transaction's own page images win
            int lo = 0, hi = ov->n - 1;
            while (lo <= hi) {
                int mid = (lo + hi) / 2;
                if (ov->pgnos[mid] == pgno) { oimg = ov->images[mid]; break; }
                if (ov->pgnos[mid] < pgno) lo = mid + 1; else hi = mid - 1;
            }
        }
        if (lane->readcheck && lane->snapshot_held && !ov && pgno != 1) rs_mark(lane, pgno);
        if (oimg) memcpy(dst, oimg + poff, take);
        else if (!mw_store_read(st, pgno, snap, poff, take, dst)) {
            int rc = pass_io->xRead(pf, dst, (int)take, o);
            if (rc == SQLITE_OK && poff == 0 && take == (uint32_t)st->pgsz && !ov) mw_store_cache_base(st, pgno, dst);
            if (rc != SQLITE_OK) {
                if (rc == SQLITE_IOERR_SHORT_READ) memset(dst + take, 0, (size_t)remaining - take);
                return rc;
            }
        }
        dst += take; o += take; remaining -= (int)take;
    }
    return SQLITE_OK;
}
static int lm_write (sqlite3_file *pf, const void *buf, int n, sqlite3_int64 off) {
    mw_ev(MW_EV_WRITE, (mw_file *)pf, off, n, 2);      // swallowed: only the compactor writes the real file
    return SQLITE_OK;
}
static int lm_truncate (sqlite3_file *pf, sqlite3_int64 size) { return SQLITE_OK; }
static int lm_sync (sqlite3_file *pf, int flags) { return SQLITE_OK; }
static int lm_filesize (sqlite3_file *pf, sqlite3_int64 *size) {
    mw_lane *lane = FILE_LANE(pf);
    tls_lane = lane;
    uint32_t pages;
    if (lane->overlay) pages = lane->overlay->dbsize;
    else {
        uint64_t snap = lane_snapshot(lane);
        if (!lane->dsz_valid || lane->dsz_epoch != snap) { lane->dsz_val = mw_store_dbsize(lane->db->store, snap); lane->dsz_epoch = snap; lane->dsz_valid = true; }
        pages = lane->dsz_val;
    }
    *size = (sqlite3_int64)pages * lane->db->store->pgsz;
    return SQLITE_OK;
}
// File locks are virtualised above SHARED. A lane never writes the real file, so it needs no
// RESERVED/PENDING/EXCLUSIVE lock; taking them for real would also make every closing connection's
// checkpoint attempt (PENDING) fail *other* connections' opens with SQLITE_BUSY. The real SHARED lock
// is kept: it stops an unaware stock connection from checkpointing/rewriting the file under us.
static int lm_lock (sqlite3_file *pf, int l) {
    tls_lane = FILE_LANE(pf);
    if (l <= SQLITE_LOCK_SHARED) return pass_io->xLock(pf, l);
    return SQLITE_OK;
}
static int lm_unlock (sqlite3_file *pf, int l) {
    if (l >= SQLITE_LOCK_SHARED) return SQLITE_OK;             // back to SHARED: the real lock never left it
    return pass_io->xUnlock(pf, l);
}
static int lm_check_reserved (sqlite3_file *pf, int *out) { *out = 0; return SQLITE_OK; }

static int pragma_is (const char *name, const char *want) { return name && sqlite3_stricmp(name, want) == 0; }

static int lm_file_control (sqlite3_file *pf, int op, void *arg) {
    mw_lane *lane = FILE_LANE(pf);
    tls_lane = lane;
    if (op == MW_FCNTL_TXINFO) { *(mw_tx_info *)arg = lane->tx; return SQLITE_OK; }
    if (op == MW_FCNTL_WRITESET) {
        mw_writeset *ws = (mw_writeset *)arg;
        ws->n = lane->ws_n;
        for (int i = 0; i < lane->ws_n && i < ws->cap; i++) ws->pgnos[i] = lane->ws_pgnos[i];
        return SQLITE_OK;
    }
    if (op == MW_FCNTL_DBSTATS) { mw_lane_fill_stats(lane, (mw_db_stats *)arg); return SQLITE_OK; }
    if (op == MW_FCNTL_RESERVE_DBV) { int64_t *v = (int64_t *)arg; *v = mw_lane_reserve_version(lane, *v); return SQLITE_OK; }
    if (op == MW_FCNTL_SEND_CEILING) {
        // committed rows only exist above the real file's; nothing published at all => no ceiling constraint beyond the counter
        int64_t c = mw_db_send_ceiling_of(lane->db);
        *(int64_t *)arg = c;
        return SQLITE_OK;
    }
    if (op == MW_FCNTL_DDL_BEGIN) { mw_lane_ddl_begin(lane); return SQLITE_OK; }
    if (op == MW_FCNTL_MARK_UNREBASABLE) { lane->rebasable = false; return SQLITE_OK; }
    if (op == MW_FCNTL_SET_OVERLAY) {
        lane->overlay = (mw_overlay *)arg;
        lane->forced_snapshot = lane->overlay ? lane->overlay->snapshot : 0;
        return SQLITE_OK;
    }
    if (op == MW_FCNTL_COMPACT) { mw_compact_result *r = (mw_compact_result *)arg; return mw_db_compact(lane->db, r); }
    if (op == MW_FCNTL_LANE_PTR) { *(void **)arg = lane; return SQLITE_OK; }
    if (op == MW_FCNTL_CDC_GET) return mw_cdc_get(lane->db, (mw_cdc_cell *)arg);
    if (op == MW_FCNTL_GC) { *(uint64_t *)arg = mw_db_gc(lane->db); return SQLITE_OK; }
    if (op == SQLITE_FCNTL_MMAP_SIZE) { *(sqlite3_int64 *)arg = 0; return SQLITE_OK; }   // mmap disabled: reports 0 disabled: reads must go through xRead
    if (op == SQLITE_FCNTL_SIZE_HINT) return SQLITE_OK;         // we never grow the real file from a lane
    if (op == SQLITE_FCNTL_PRAGMA) {
        char **az = (char **)arg;
        const char *name = az[1], *val = az[2];
        if (pragma_is(name, "journal_mode") && val && sqlite3_stricmp(val, "wal") != 0) {
            az[0] = sqlite3_mprintf("multiwriter: journal_mode=%s is not supported (WAL only)", val);
            return SQLITE_ERROR;
        }
        if (pragma_is(name, "locking_mode") && val && sqlite3_stricmp(val, "exclusive") == 0) {
            az[0] = sqlite3_mprintf("multiwriter: locking_mode=EXCLUSIVE is not supported");
            return SQLITE_ERROR;
        }
        if (pragma_is(name, "auto_vacuum") && val && sqlite3_stricmp(val, "none") != 0 && strcmp(val, "0") != 0) {
            az[0] = sqlite3_mprintf("multiwriter: auto_vacuum=%s is not supported (NONE only)", val);
            return SQLITE_ERROR;
        }
        if (pragma_is(name, "synchronous") && val) {          // observed, not handled: SQLite still executes it
            int lv = (int)strtol(val, NULL, 10);
            if (!strcmp(val, "0") || !sqlite3_stricmp(val, "off")) lv = 0;
            else if (!sqlite3_stricmp(val, "normal")) lv = 1;
            else if (!sqlite3_stricmp(val, "full")) lv = 2;
            else if (!sqlite3_stricmp(val, "extra")) lv = 3;
            lane->sync_level = lv;
            return SQLITE_NOTFOUND;
        }
        if (pragma_is(name, "mmap_size") && val) { az[0] = sqlite3_mprintf("0"); return SQLITE_OK; }   // normalised to 0
        return SQLITE_NOTFOUND;
    }
    return pass_io->xFileControl(pf, op, arg);
}

static int lm_shm_map (sqlite3_file *pf, int page, int size, int extend, void volatile **pp) {
    mw_lane *lane = FILE_LANE(pf);
    tls_lane = lane;
    mw_ev(MW_EV_SHMMAP, (mw_file *)pf, page, size, extend);
    if (size != MW_SHM_REGION_BYTES || page >= MW_SHM_MAX_REGIONS) return SQLITE_IOERR_SHMSIZE;
    if (page >= lane->nshm) {
        if (!extend) { *pp = NULL; return SQLITE_OK; }
        while (lane->nshm <= page) {
            uint8_t *r = sqlite3_malloc64(MW_SHM_REGION_BYTES);
            if (!r) return SQLITE_NOMEM;
            memset(r, 0, MW_SHM_REGION_BYTES);
            lane->shm[lane->nshm++] = r;
        }
    }
    *pp = lane->shm[page];
    return SQLITE_OK;
}

// Locks are private (no other connection can observe this wal-index): always granted. They
// still delimit the lane's transaction exactly as measured on the stock WAL.
static int lm_shm_lock (sqlite3_file *pf, int ofst, int n, int flags) {
    mw_lane *lane = FILE_LANE(pf);
    mw_ev(MW_EV_SHMLOCK, (mw_file *)pf, ofst, n, flags);
    if (n == 1) return mw_lane_on_shm_lock(lane, ofst, flags);
    return SQLITE_OK;
}
static void lm_shm_barrier (sqlite3_file *pf) { mw_ev(MW_EV_SHMBARRIER, (mw_file *)pf, 0, 0, 0); atomic_signal_fence(memory_order_seq_cst); }     // (the wal-index is private to the lane: only the compiler must not reorder)
static int lm_shm_unmap (sqlite3_file *pf, int del) {
    mw_ev(MW_EV_SHMUNMAP, (mw_file *)pf, del, 0, 0);
    return SQLITE_OK;                                          // regions are freed with the lane
}
static int lm_fetch (sqlite3_file *pf, sqlite3_int64 off, int n, void **pp) { *pp = NULL; return SQLITE_OK; }   // no mmap
static int lm_unfetch (sqlite3_file *pf, sqlite3_int64 off, void *p) { return SQLITE_OK; }

void mw_lane_methods_init (const sqlite3_io_methods *pass) {
    pass_io = pass;
    lane_main_io = *pass;
    lane_main_io.iVersion = 3;
    lane_main_io.xRead = lm_read;
    lane_main_io.xWrite = lm_write;
    lane_main_io.xTruncate = lm_truncate;
    lane_main_io.xSync = lm_sync;
    lane_main_io.xFileSize = lm_filesize;
    lane_main_io.xLock = lm_lock;
    lane_main_io.xUnlock = lm_unlock;
    lane_main_io.xCheckReservedLock = lm_check_reserved;
    lane_main_io.xFileControl = lm_file_control;
    lane_main_io.xShmMap = lm_shm_map;
    lane_main_io.xShmLock = lm_shm_lock;
    lane_main_io.xShmBarrier = lm_shm_barrier;
    lane_main_io.xShmUnmap = lm_shm_unmap;
    lane_main_io.xFetch = lm_fetch;
    lane_main_io.xUnfetch = lm_unfetch;

    memset(&lane_wal_io, 0, sizeof(lane_wal_io));
    lane_wal_io.iVersion = 1;
    lane_wal_io.xClose = wal_close;
    lane_wal_io.xRead = wal_read;
    lane_wal_io.xWrite = wal_write;
    lane_wal_io.xTruncate = wal_truncate;
    lane_wal_io.xSync = wal_sync;
    lane_wal_io.xFileSize = wal_filesize;
    lane_wal_io.xLock = wal_nolock;
    lane_wal_io.xUnlock = wal_nolock;
    lane_wal_io.xCheckReservedLock = wal_check_reserved;
    lane_wal_io.xFileControl = wal_file_control;
    lane_wal_io.xSectorSize = wal_sector_size;
    lane_wal_io.xDeviceCharacteristics = wal_device_char;
}

// The first lane of a database creates its page store from the real file's header.
int mw_lane_open_main (mw_file *f, mw_lane *lane) {
    mw_db *db = lane->db;
    uint8_t h[100];
    sqlite3_int64 size = 0;
    int rc = f->real->pMethods->xRead(f->real, h, sizeof h, 0);
    if (rc == SQLITE_OK) rc = f->real->pMethods->xFileSize(f->real, &size);
    if (rc != SQLITE_OK) return rc;
    if (h[52] || h[53] || h[54] || h[55]) return SQLITE_CANTOPEN;          // auto_vacuum / incremental_vacuum: page relocation is not supported
    int pgsz = (h[16] << 8) | h[17];
    if (pgsz == 1) pgsz = 65536;
    if (pgsz < 512 || (pgsz & (pgsz - 1))) return SQLITE_NOTADB;

    sqlite3_mutex_enter(db->mu);
    if (!db->store) {
        db->store = db->shared ? mw_store_create_light(pgsz, (uint32_t)(size / pgsz)) : mw_store_create(pgsz, (uint32_t)(size / pgsz));
        if (!db->store) rc = SQLITE_NOMEM;
        else {
            db->store->reserved = h[20];
            if (db->mp_req) rc = mw_mp_open(db);                // shared header + lock file (first process initialises it)
            if (rc == SQLITE_OK) rc = db->shared ? mw_shared_open(db) : mw_log_open(db, pgsz);    // create the commit log or recover committed state from it
            if (rc == SQLITE_OK && db->mp) rc = mw_mp_finish_open(db);
            if (rc == SQLITE_OK && db->shared) rc = mw_shared_open_finish(db);
            if (rc != SQLITE_OK) { mw_store_free(db->store); db->store = NULL; }
        }
    } else if (db->store->pgsz != pgsz) rc = SQLITE_MISUSE;
    sqlite3_mutex_leave(db->mu);
    if (rc != SQLITE_OK) return rc;
    if (db->mp) { lane->mp_slot = mw_mp_slot_alloc(db); if (lane->mp_slot < 0) return SQLITE_FULL; }
    f->base.pMethods = &lane_main_io;
    lane->file = f;
    tls_lane = lane;
    return SQLITE_OK;
}

// MARK: - bootstrap -

// Private lanes require the database file to be in WAL mode (the pager then never rewrites the
// header from a lane). New/rollback-mode files are converted with a private stock connection on
// the underlying VFS; if a real WAL is left over from stock use it is checkpointed by that close.
int mw_ensure_wal_db (const char *path) {
    // A real -wal left by stock use (a crashed process, the sqlite3 CLI) holds committed frames the lane store cannot see; and the compactor would
    // later overwrite the main file under it, so a stock connection replaying that WAL would corrupt newer pages. Checkpoint it first.
    bool stale_wal = false;
    {
        size_t n = strlen(path);
        char *wp = malloc(n + 5);
        if (!wp) return SQLITE_NOMEM;
        memcpy(wp, path, n); memcpy(wp + n, "-wal", 5);
        struct stat sb;
        stale_wal = stat(wp, &sb) == 0 && sb.st_size > 0;
        free(wp);
    }
    FILE *fp = fopen(path, "rb");
    if (fp) {
        uint8_t h[100];
        size_t n = fread(h, 1, sizeof h, fp);
        fclose(fp);
        if (n == sizeof h && h[18] == 2 && h[19] == 2 && !stale_wal) return SQLITE_OK;
    }
    sqlite3 *c = NULL;
    int rc = sqlite3_open_v2(path, &c, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, mw_root_vfs()->zName);
    if (rc == SQLITE_OK) {
        sqlite3_busy_timeout(c, 5000);                             // (two openers of a new database may convert it at the same time)
        rc = sqlite3_exec(c, "PRAGMA journal_mode=WAL", NULL, NULL, NULL);
        if (rc == SQLITE_OK && stale_wal) rc = sqlite3_exec(c, "PRAGMA wal_checkpoint(TRUNCATE)", NULL, NULL, NULL);
    }
    sqlite3_close(c);
    return rc;
}
