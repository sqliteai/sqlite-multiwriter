//
//  multiwriter_internal.h
//  sqlite-multiwriter
//
//  Internal structures shared by the Multi-Writer translation units.
//  Locking rules (also documented at each field):
//    - mw_db.mu protects the snapshot registry and the lane list.
//    - epoch / counters are C11 atomics; epoch is only *advanced* under the publication lock.
//

#ifndef MW_INTERNAL_H
#define MW_INTERNAL_H

#include <pthread.h>
#include <stdatomic.h>
#include <limits.h>
#include <stdlib.h>
#include "multiwriter_shidx.h"
#include <stdbool.h>
#include "multiwriter.h"
#include "multiwriter_catalog.h"

// One row of the net change of a transaction (multiwriter_rowdiff.c): kind 1 inserted (new_rec), 2 changed (old_rec -> new_rec), 3 deleted (old_rec). The records are SQLite's own (header, serial types, values).
typedef struct { int kind; const mw_tab *tab; int64_t rowid; uint8_t *old_rec; uint32_t old_len; uint8_t *new_rec; uint32_t new_len; } mw_chg;
typedef struct { mw_chg *chg; int n; const char *unsupported; } mw_rd_result;

typedef struct mw_db   mw_db;
#define MW_LOG_OFF(db) __atomic_load_n(&(db)->log_off, __ATOMIC_RELAXED)      // (log_off is written under the store's seq_mu and read by the log's threads under log_mu: both sides are atomic accesses)
typedef struct { uint64_t epoch, end; } mw_pend;   // a record that was copied out of order: its epoch and the file offset where it ends
typedef struct mw_lane mw_lane;

// Multi-process shared state ("<db>-mwlock", mmapped MAP_SHARED by every process; see multiwriter_mp.c).
#define MW_MP_SLOTS 8192                 // lanes of all processes (a process has its connections and the metadata store's: a flusher and up to four readers)
#define MW_MP_PROCS 1024
#define MW_MP_NONE  (~0ull)
typedef struct {
    _Atomic int32_t  pid;                 // owner process (0 = free); a dead owner's slot is ignored and reclaimed
    _Atomic uint64_t snap;                // snapshot epoch held by this lane, MW_MP_NONE if none
    _Atomic int64_t  minres;              // lowest unresolved db_version reserved by this lane, 0 if none
    _Atomic int32_t  writing;             // the lane holds its (private) write lock: a DDL in another process waits for these
} mw_mp_slot;
typedef struct {
    _Atomic int32_t  pid;
    _Atomic uint64_t applied;             // (not used any more; kept for the layout of the header)
} mw_mp_proc;
#define MW_LOG_END(p) ((p) & (((uint64_t)1 << 40) - 1))
#define MW_LOG_GEN(p) ((p) >> 40)
#define MW_LOG_POS(gen, end) (((uint64_t)(gen) << 40) | (end))
// The shared header is used through atomics by several processes: they must be lock-free (a lock-based fallback would not be address-free and would not work across processes).
_Static_assert(ATOMIC_LLONG_LOCK_FREE == 2 && ATOMIC_INT_LOCK_FREE == 2, "the multi-process header needs lock-free 32/64-bit atomics");
typedef struct mw_shm {
    uint64_t          magic;
    _Atomic uint32_t  ready;
    uint32_t          pgsz;
    _Atomic uint64_t  committed_epoch;    // last commit appended to the log (durable per its synchronous level)
    _Atomic uint64_t  log_pos;            // (log generation << 40) | offset just after the last committed record: one word, so a reset/rewrite is a single store
    _Atomic uint64_t  base_epoch;         // state <= this is in the real file
    _Atomic uint64_t  compact_T;          // compaction target announced (a new snapshot must be >= this)
    _Atomic uint64_t  schema_epoch;       // epoch of the newest schema change
    _Atomic int32_t   pub_owner;          // publication lock: pid of the holder, 0 = free (a dead holder's lock is stolen)
    _Atomic uint64_t  log_ready;          // bytes of the log file that were written (zero-filled or records): records are appended only below it, the prefiller writes above it
    _Atomic int32_t   log_fill_pid;       // process zero-filling the next chunk (0 = none)
    _Atomic uint64_t  pub_ticket, pub_serving;   // FIFO queue for the publication lock: waiters far from the head sleep instead of polling
    _Atomic uint32_t  pub_wake[1024];      // the publication-lock waiter with ticket t sleeps on [t % 1024]; woken when it comes within 2 of the head
    _Atomic int32_t   pub_tk_pid[1024];    // pid that took each ticket (a queued process that died is skipped)
    _Atomic uint64_t  adm_ticket, adm_admitted;  // admission control of writer transactions across processes (see mw_mp_admit): FIFO tickets, highest ticket processed
    _Atomic uint32_t  adm_wake[1024];      // admission: the word the waiter with ticket t sleeps on at [t % 1024]; changed + woken when it is its turn
    _Atomic int32_t   adm_tk_pid[1024];    // pid that took each ticket (a queued process that died is skipped)
    _Atomic uint64_t  adm_slot_rel_ns[64];   // (not used any more; kept for the layout of the header)
    _Atomic int32_t   adm_slot_pid[64];   // slot holders (pid, 0 = free); a dead holder's slot is freed by the waiters
    _Atomic int32_t   ddl_pid;            // process running a schema change (exclusive schema barrier across processes), 0 = none
    _Atomic int64_t   dbv_counter;        // highest db_version ever reserved
    _Atomic uint64_t  hot_ticket, hot_serving;   // the turn among long transactions, across processes (same protocol as mw_db.hot_*)
    _Atomic uint64_t  hot_aband[256];
    _Atomic int32_t   hot_tk_pid[256];    // pid that took each ticket (a queued process that died is skipped)
    _Atomic int       hot_held;
    _Atomic int32_t   hot_owner;          // pid of the process holding the turn (a dead holder's turn is released by the waiters)
    // multi-process mode (segmented log + shared index)
    _Atomic uint32_t  seg_min;            // oldest segment file that still exists
    _Atomic uint32_t  seg_next_ready;     // id of the next segment when it is fully prepared (0 = not yet)
    _Atomic uint32_t  sl_seg;             // writer cursor: segment the next record goes to
    uint32_t          sl_pad;
    _Atomic uint64_t  sl_end;             // ... and the offset in it (moved only under the publication lock)
    _Atomic uint64_t  seg_first_epoch[256];   // the epoch segment s was opened for, at [s % 256]
    _Atomic uint32_t  seg_first_id[256];      // ... and the segment that it belongs to: with more than 256 live segments a slot is taken by a later one, and then the epoch is read from the file of the segment
    _Atomic uint64_t  pend_epoch, pend_off; _Atomic uint32_t pend_seg;       // the record being published (epoch, where it starts): set when it is appended, cleared when it is visible
    _Atomic uint64_t  sy_done;            // everything up to this log position (segment << 40 | offset) is durable: the group commit across processes
    _Atomic uint32_t  sy_wake;            // changed + woken when a sync finishes: the commits waiting for durability sleep on it
    _Atomic int32_t   sy_leader;          // process running the fsync everybody waits for (0 = none)
    _Atomic uint32_t  broken;             // a commit failed after its pages were installed in the index and before it was published (its versions sit at an epoch that the next commit would reuse): no process publishes any more until all have closed
    _Atomic uint32_t  sy_failed;          // an fsync of the log failed: the data it should have covered may never reach the disk, and a later fsync that succeeds (the kernel forgets the error) must not acknowledge it: every commit that is not durable yet fails, in every process, until the log is closed by all
    _Atomic uint32_t  base_dbsize;        // database size in pages at the compaction base (what the real file holds)
    _Atomic uint64_t  compact_req_ns;     // shared mode: a process has claimed the next compaction at this time (0 = none; a claim older than 2 s is void)
    _Atomic int32_t   compact_req_pid;    // ... and this is the process (0 = not written yet): another one takes the claim over at once if it is gone, instead of waiting for the 2 s
    _Atomic uint64_t  compact_end_ns;     // when the last compaction ended (periodic compactions of all processes share one interval)
    _Atomic uint64_t  compact_busy_T;     // a compaction is reading versions <= this: the index GC must not free them (0 = none)
    mw_mp_proc        procs[MW_MP_PROCS];
    mw_mp_slot        slots[MW_MP_SLOTS];
} mw_shm;

// Wrapper file object. `real` points right after this struct (allocated by xOpen via szOsFile).
typedef struct {
    sqlite3_file  base;         // must be first
    sqlite3_file *real;         // underlying file
    const char   *name;         // filename as passed to xOpen (owned by SQLite, valid until close); NULL for temp files
    mw_lane      *lane;         // main database file (lane tracking or private lane) and its private WAL
    int           is_wal;       // private in-memory WAL of a lane (no real file behind it)
} mw_file;

// In-memory private WAL of one lane. SQLite appends frames here; the physical write set of the
// transaction is read back from it at the commit frame. Reset when the snapshot ends.
typedef struct {
    uint8_t *buf;
    size_t   size, cap;
    int      pgsz;              // from the WAL header (offset 8, big endian)
    bool     commit_seen;
} mw_memwal;

#define MW_SHM_REGION_BYTES 32768
#define MW_SHM_MAX_REGIONS  1024

// Versioned committed page store: (page number, commit epoch) -> page image.
//
// Locking (see multiwriter_pages.c): one mutex per stripe of pages (pgno % MW_STRIPES) protects the
// version chains of those pages. A reader takes one stripe mutex, copies bytes out and releases it. A
// publisher locks the stripes of every page it writes or only read (ascending order) and holds them across
// validation + install, so overlapping commits serialise while disjoint ones proceed in parallel.
// Lock order: stripes (ascending) -> seq_mu, list_mu. GC/compaction take one stripe at a time.
#define MW_STRIPES 256
#define MW_CHAIN_BLOCK_BITS 12
#define MW_CHAIN_BLOCK (1u << MW_CHAIN_BLOCK_BITS)
#define MW_DIR_SIZE (1u << 20)                       // x 4096 pages = 4G pages

typedef struct { uint64_t epoch; uint8_t *data; } mw_pv;
typedef struct {
    mw_pv    *v;                                     // ascending epoch
    int       n, cap;
    uint32_t  dirty_next, cand_next;                 // intrusive list links (pgno + 1, 0 = end)
    uint8_t   dirty, queued;                         // in the compaction / GC list (protected by the page's stripe)
} mw_chain;
typedef struct { uint64_t epoch; uint32_t dbsize; uint64_t log_off; } mw_sizerec;   // log_off: offset of the commit's log record (0 = none)   // db size (pages) as of an epoch, ascending

typedef struct { pthread_mutex_t mu; char pad[64]; } mw_stripe;

typedef struct {
    int                   pgsz;
    mw_stripe             stripes[MW_STRIPES];
    _Atomic(mw_chain *)  *dir;                       // dir[pgno >> 12] -> block of chains, allocated on first touch
    pthread_mutex_t       seq_mu;                    // epoch/log-offset assignment + size records + base
    mw_sizerec           *sizes;
    int                   nsizes, sizes_cap;
#define MW_SIZE_RING 1024
    _Atomic uint64_t      size_ring_epoch[MW_SIZE_RING];   // the database size at each recent epoch (slot = epoch % ring): a snapshot reads its size without taking seq_mu
    _Atomic uint32_t      size_ring_size[MW_SIZE_RING];
    uint32_t              base_dbsize;               // pages in the real file at the compacted epoch
    int                   reserved;                  // bytes reserved at the end of every page (database header byte 20)
    uint64_t              compacted_epoch;           // state <= this epoch is materialised in the real file (phase 18)
    pthread_mutex_t       list_mu;                   // heads of the two intrusive lists
    _Atomic uint32_t      p1_cookie;                 // schema cookie (header bytes 40..43) of the newest committed version of page 1, stored before p1_head_epoch: the commit-time schema check reads it without taking page 1's stripe
    _Atomic uint64_t      p1_head_epoch;             // epoch of the newest committed version of page 1 (set at install: lets a relocation know its cached copy is still the latest)
    uint32_t              cand_head, dirty_head;     // GC candidates (pages with >1 version) / pages changed since the last compaction
    _Atomic uint64_t      versions, bytes;           // currently held
    _Atomic uint64_t      versions_allocated, versions_reclaimed, gc_runs, gc_ns;
    _Atomic uint64_t      base_bytes;                // clean pages of the real file cached as epoch-0 versions
    uint64_t              base_limit;                // cache cap (default 64 MB)
    struct mw_db         *shared_db;                 // shared mode: the page versions are in the shared index of this database, not here
} mw_store;

// The bytes of a version: its private copy, or the page image inside the shared log mapping.

struct mw_lane {
    mw_db      *db;
    mw_file    *file;           // the main database file object (private mode)
    bool        readcheck;      // validate pages read (not written) by the transaction (default on; mw_readcheck=0 turns it off)
    uint8_t    *rs_bits;        // pages read by the current transaction (xRead on the main file), except page 1
    size_t      rs_bits_cap;
    uint32_t   *rs_list;
    int         rs_n, rs_cap;
    bool        norebase;       // this connection is itself a rebase helper: never rebase recursively
    bool        rebase_on;      // URI mw_rebase=1: a commit that conflicts on pages only is replayed at the latest snapshot (multiwriter_rebase.c)
    bool        rb_nobackoff;   // URI mw_rebase_backoff=0 (tests): replay every conflict, whatever happened to the last ones
    sqlite3_stmt *rd_cur;   // the statement that is running (mw_rebase=1 connections; set by the statement hook), and what the transaction did before it:
    int         rd_cur_kind, rd_cache_next; bool rd_dep; void *rd_cache; sqlite3 *rd_db; int rd_chg_base;   // rd_dep: it read rows that it may not have changed (see mw_lane_reads_unchanged)
    int         rb_streak, rb_skip;  // refused replays in a row, and conflicts to refuse without trying (a hot row: the replay would fail again at the cost of decoding the pages)
    sqlite3    *rb_db;          // the helper connection that replays (opened at the first rebase)
    void       *rb_state;       // what the helper caches: catalog, statements (multiwriter_rebase.c)
    int         consec_aborts;   // refusals since this lane last committed (a starving lane is granted a turn even when its transactions are untracked)
    int         adm_slot;            // multi-process admission slot held by the current transaction (-1 none)
    bool        recent_writer;       // the previous transaction wrote: the next one goes through admission
    uint32_t    adm_check;           // transactions since the process count was last looked at
    bool        adm_crowded;         // more processes than admission slots
    uint64_t    tx_t0;               // start of the current transaction's snapshot
    bool        hot_shared;          // the turn we hold is the one in the shared header (multi-process, long transaction)
    int         tx_long_run;         // consecutive transactions that stayed open > 2 ms from snapshot to commit attempt (>= 3: a "long" lane, see lane_publish)
    int         hot_credit;      // serialised transactions granted after a conflict (URI mw_hot_credit, default 16)
    uint64_t    reloc_t0;        // (timing) when the relocation mutex was taken
    bool        holds_reloc;     // this lane holds db->reloc_mu (released by the publisher right after its pages are installed)
    bool        noroute;         // URI mw_noroute=1: validate every read page as a whole (measurement)
    int         prep_delay_us;   // URI mw_prep_delay_us=N (tests): wait N us between the preparation of a relocation and the publication lock, so that the end of the file moves in between
    bool        nomerge;         // URI mw_nomerge=1: never merge interior pages (measurement)
    uint64_t    dsz_epoch;       // database size at this snapshot epoch, cached (the size at a given epoch never changes): xFileSize is called several times per transaction
    uint32_t    dsz_val;
    bool        dsz_valid;
    bool        noreloc;         // URI mw_noreloc=1: never renumber new pages after a growth conflict (measurement)
    uint32_t    reads_since_write; // snapshots since this connection last asked for the write lock: a connection that keeps reading is a reader, and only a reader keeps its cache warm (the read set that it would inherit makes the commits of a writer conflict for pages it did not read)
    bool        tried_write;     // this snapshot asked for the WAL write lock (whether or not it got it)
    bool        warm;            // the last snapshot of this connection was read-only: its page cache and read set were kept (see mw_lane_snapshot_begin)
    uint64_t    warm_epoch;      // ... and was taken at this epoch
    bool        rs_overflow;     // read-set tracking ran out of memory in this snapshot: the read set is incomplete
    uint32_t    bp_wait_us;                      // back-pressure: sleep this long after the publication lock is released
    uint64_t    writer_id;      // unique per connection (lane) in this process; NOT the sqlite-sync site_id
    mw_tx_info  tx;             // current/last transaction
    uint64_t    mp_t0, adm_t0;
    uint32_t    sl_seg;         // shared mode: where this lane's last commit record ends (for the group fsync after the publication lock was released)
    uint64_t    sl_end;
    bool        mp_held;        // multi-process: the caller already holds the gate and the publication lock (relocation: it prepares its images against the state it will publish on)
    int         mp_slot;        // slot in the shared registry (multi-process mode), -1 otherwise
    int         sync_level;     // PRAGMA synchronous as observed on this connection (default FULL = 2)
    int         retry_credit;   // >0: the lane recently conflicted; its next transactions are serialised (hot spots); decays per successful commit
    bool        holds_hot;      // this lane's current snapshot holds db->hot_mu
    bool        ddl_active;     // this lane owns the schema barrier (released when its transaction ends)
    _Atomic bool write_locked;   // WAL write lock held inside the current snapshot
    bool        snapshot_held;  // registered in db->lanes as an active snapshot
    mw_lane    *next_active;    // intrusive list of lanes holding a snapshot (protected by db->mu)
    mw_lane    *prev_active;
    bool        private_mode;   // mw=1: private WAL + private wal-index (true lane)
    // private lane state (only valid when private_mode)
    mw_memwal   wal;
    bool        wal_bound;
    uint8_t    *shm[MW_SHM_MAX_REGIONS];
    int         nshm;
    // write set captured at the commit frame (valid until the next snapshot begins)
    uint32_t   *ws_pgnos;
    int         ws_n, ws_cap;
    uint32_t    ws_dbsize;
    int        *ws_hash;             // page number -> ws index + 1 (open addressing), so building the write set is linear
    uint32_t    ws_hash_cap;
    int         commit_base;         // first WAL frame of the current write transaction (>0: an earlier commit in this read snapshot)
    uint32_t   *own_pg;              // pages committed by this connection inside the current snapshot (sorted)
    int         own_n, own_cap;
    uint64_t    own_epoch;           // epoch of its latest commit
    uint64_t   *own_ep;              // (parallel to own_pg) the epoch of the latest commit of this snapshot that wrote each page: a page is validated against the commit that wrote it, not against the latest commit of the lane
    void       *rprep;               // multi-process: phase 1 of the relocation, made before the publication lock (reloc.c)
    uint64_t   *pre_ch;              // multi-process: the hashes of the pages of the commit, made before the publication lock (parallel to the write set; freed at the end of the commit)
    const uint64_t *pre_use;         // the ones the next shared publication uses (set for one publication: the relocation clears the entries of the pages it changes)
    bool        poisoned;            // a commit of this snapshot was relocated (its pages went to other numbers): the lane's private WAL still has the old ones, so the snapshot takes no more commits
    int        *ws_frame;       // frame index of the last image of each page (parallel to ws_pgnos)
    // passthrough-mode commit detection (wal-index header watch)
    volatile uint8_t *shm0;
    uint32_t    hdr_change_at_lock;
    uint32_t    hdr_frames_at_lock;
};

#define MW_VIS_SLOTS 256
struct mw_vis_slot { pthread_mutex_t mu; pthread_cond_t cv; _Atomic int waiters; _Atomic uint64_t ready; };

struct mw_db {
    char             *path;
    int               mode;             // 1 = lane tracking on the stock WAL, 2 = private lanes
    int               refs;             // protected by the global registry mutex
    mw_db            *next;             // global registry list
    sqlite3_mutex    *mu;               // snapshot registry + lane list
    mw_lane          *active;           // lanes holding a snapshot
    _Atomic(mw_store *) store;         // private-lane mode only; created by the first lane (atomic: a recovery in place replaces it)
    _Atomic uint64_t  epoch;            // last committed epoch
    _Atomic int       vis_parked;       // committers parked in mw_db_make_visible
    _Atomic uint64_t  next_tx_id;
    _Atomic uint64_t  next_writer_id;
    _Atomic uint32_t  schema_generation;
    _Atomic uint64_t  last_schema_epoch;   // commit epoch of the newest DDL
    _Atomic uint64_t  n_ddl_barriers;
    pthread_mutex_t   ddl_mu;              // exclusive schema barrier: one DDL statement at a time,
    pthread_cond_t    ddl_cv;              //   new write transactions are refused while it runs
    _Atomic(mw_lane *) ddl_owner;      //   (written under ddl_mu; read without it by the lanes that start a write: only an optimisation of when they start)
    _Atomic uint64_t  n_commits, n_aborts, n_snapshots;
    int64_t           dbv_counter;      // highest db_version ever reserved (protected by mu)
    _Atomic uint64_t  n_rebases, n_rebase_grouped, n_rebase_retries, n_rebase_max_attempts, n_rebase_ns, n_unrebasable;
    uint8_t          *p1_cache;            // the page 1 image the last relocation installed and its epoch: the next one starts from it (reloc_mu)
    uint64_t          p1_cache_epoch;
    pthread_mutex_t   reloc_mu;            // serialises page relocations (they all chase the same end of the file); not held across the log fsync
    _Atomic uint64_t  n_merges;            // interior pages merged three-way instead of refusing the transaction
    _Atomic uint64_t  n_reads_saved;       // read conflicts on interior pages whose routing for the pages the transaction used was unchanged
    _Atomic uint64_t  n_read_conflicts, n_pk_collisions, n_cell_conflicts, n_relocations, n_prep_used, n_prep_dropped, n_prep_rewrote;
    // durable log (multiwriter_log.c)
    char             *logpath;
    int               logfd;               // -1 = no log (never for a private-lane db)
    struct mw_rext   *rext; int nrext, caprext;   // extensions found in the log at recovery (epoch, bytes), consumed once by the change capture
    uint64_t          log_salt;            // per-database: stored in the log header, seeds the record checksums
    uint64_t          log_off;             // next append offset (protected by store->seq_mu)
    uint8_t          *logmap;              // the log file mapped MAP_SHARED (records are memcpy'd, no syscall); NULL = pwrite fallback
    uint64_t          logsync_off;         // everything below this offset has been msync'ed (log_mu)
    uint64_t          logsync_low1;        // 1 + lowest offset of a record completed since the last sync began although below logsync_off (0 = none; log_mu)
    _Atomic uint64_t  log_res_end;         // staged log: the file's blocks are reserved up to here (a write below it cannot fail for want of space)
    uint64_t          logfile_size;        // current file size (grown ahead of log_off under seq_mu)
    uint64_t          base_epoch;          // compacted epoch (log header), protected by log_mu
    pthread_mutex_t   log_mu;              // fsync leader election + written-prefix bookkeeping
    // Leader-batched log writes (single process, synchronous>=FULL): committers copy their record into a staging ring at their file offset; the
    // group-commit leader writes the contiguous prefix of finished records with one pwritev and one fsync (no mmap page faults, no msync).
    _Atomic int       log_mode;            // 0 = undecided, 1 = mapped log (mmap memcpy + msync + fsync), 2 = staged
    uint8_t          *stage_buf;
    uint64_t          stage_r;             // ring size (power of two)
    uint64_t          written_end;         // end offset of the contiguous prefix of records that were copied (log_mu)
    uint64_t          flushed_off;         // everything below is in the file (log_mu)
    uint64_t          base_cache_bytes;    // cap of the pages of the real file kept in memory as epoch-0 versions (0: the default, 64 MB); mw_base_cache_mb / mw_profile=small
    uint64_t          synced_off;          // everything below is durable (log_mu)
    int               sync_waiters;        // threads waiting on sync_cv (log_mu)
    uint64_t          written_upto;        // all records with epoch <= this have been pwritten
    uint64_t          synced_upto;         // ... and fsynced
    mw_pend          *written_pending;     // completed out-of-order epochs > written_upto (and the file offset where each record ends)
    int               npending, pending_cap;
    pthread_cond_t    sync_cv;             // group-commit generation counters (protected by log_mu)
    uint64_t          sync_started, sync_done;
    bool              sync_running;        // mapped log: a group-commit leader is working; staged log: the log file is being swapped (quiesce)
    bool              stage_writing;       // staged log: a thread is in pwritev (one writer at a time)
    bool              stage_fsyncing;      // staged log: a thread is in fsync (one at a time; it overlaps the next group's pwritev)
    uint64_t          flushed_epoch;       // epoch of the last record written to the file (log_mu; set together with flushed_off)
    atomic_int        saw_sync;            // staged log: a record that waits for fsync (synchronous>=FULL) was appended at some time; group visibility without fsync is then unsafe
    // (kept apart from the bools above: gcc reads sync_running/stage_writing/stage_fsyncing with one 8-byte load, which overlapped this atomic and was reported by TSan)
    // publication gate (fairness): a starving rebase closes it so its next attempt cannot lose a race
    _Atomic int       gate_closing;        // 1 while a rebase holds (or is acquiring) the exclusive gate
    _Atomic int       gate_active;         // publishers currently inside validate+install
    _Atomic(mw_lane *) gate_owner;         // the helper lane allowed to publish while the gate is closed (read by every publisher)
    pthread_mutex_t   hot_mu;              // pessimistic escalation: conflicting lanes' transactions run one at a time
    pthread_cond_t    hot_cv;
    pthread_mutex_t   admit_mu;            // admission control for synchronous<FULL: at most admit_max committers inside publish at once
    pthread_cond_t    admit_cv;
    int               admit_in, admit_waiting;
    _Atomic uint64_t  hot_ticket, hot_serving;   // FIFO queue for the turn among long transactions (a polling race starved some of them for seconds)
    _Atomic uint64_t  hot_aband[256];            // ticket+1 of a waiter that gave up (the queue skips it)
    _Atomic int       hot_held;            // the turn (0/1); waiters spin briefly on it, then park on hot_cv
    _Atomic int       hot_parked;
    _Atomic int       hot_spinners;
    _Atomic uint64_t  n_hot_serialised;
    pthread_mutex_t   rb_qmu;              // the queue of the rebase (multiwriter_rebase.c): requests waiting for a leader, the leader flag
    pthread_cond_t    rb_qcv;
    void             *rb_qhead, *rb_qtail;
    bool              rb_leader;
    pthread_mutex_t   cat_mu;              // the catalog of the schema as it was last built by any lane of this database (multiwriter_rebase.c): one build for all of them, not one for each connection
    mw_cat           *cat_shared;          //   (its cookie says which schema; immutable once built)
    _Atomic uint64_t  n_gate_closures, n_backpressure;
    struct mw_vis_slot *vis;               // visibility ordering: epoch E becomes visible only after E-1; waiters are woken one by one

    // multi-process mode (mw_mp=1)
    bool              mp_req, mp;          // requested at open / active
    bool              has_log;             // a log file exists (set once when it is opened, before any commit): read by publishers instead of the logfd that a rewrite replaces
    _Atomic int       inflight;            // commits inside lane_publish (the recovery of a failed log waits for them to leave)
    _Atomic int       recovering;          // a recovery in place is running
    mw_store         *retired[8]; int nretired;   // stores replaced by a recovery: readers that were inside them may still look (freed with the database)
    _Atomic bool      open_done;           // the first connection finished opening (a database whose open failed has nothing to flush)
    _Atomic int       mp_nprocs;           // processes registered on this database, as last counted by the admission control
    bool              compact_claimed;     // this process holds the compaction claim (shared mode)
    struct mw_seglog *sl;                  // the segmented log (shared mode)
    shidx            *ix;                  // the shared version index (shared mode)
    bool              mp_stale_owner;      // the header said that the publication lock was held by our own pid when we registered: a process that died with it (repaired once the database is open)
    int               open_rc;             // the first open of the database in this process failed with this result and other connections still hold the struct: they get it too (see mw_lane_open_main)
    bool              mp_first;            // this process initialised the shared state
    bool              orphaned;            // inherited through fork(): the child must not use (or tear down) the parent's state; it opens its own
    bool              closing;             // the last connection is gone and the state is being taken down (outside the registry's mutex): an opener of the same path waits until it is out
    int               mp_lockfd, mp_pubfd, mp_proc;   // mp_lockfd: flock membership + shared header; mp_pubfd: fcntl byte locks only (BSD mixes the two kinds)
    char             *mp_path, *mp_pubpath;
    mw_shm           *shm;
    pthread_mutex_t   mp_mu;               // serialises publication inside this process
    _Atomic int       failed;              // sticky I/O failure: no further commits
    _Atomic uint64_t  next_epoch;          // last *assigned* epoch (>= epoch; the difference is in-flight commits)
    _Atomic uint64_t  n_pages_published, n_log_sync_ns;
    _Atomic uint64_t  n_compactions, n_compaction_ns, n_compacted_pages, n_log_syncs;
    int               fd_real;             // real database file (compaction), -1 when closed
    pthread_t         compactor;
    bool              compactor_running;
    _Atomic int       compact_interval_ms;
    _Atomic int       compactor_stop;
    pthread_mutex_t   compact_mu;
    _Atomic uint64_t  log_max_bytes;       // compaction is requested when the log grows past this (default 32 MB)
    pthread_mutex_t   compactor_mu;        // wakes the background compactor
    pthread_cond_t    compactor_cv;
    bool              compact_on_close;
    _Atomic uint64_t  n_fast_commits, n_page_conflicts, n_schema_conflicts;
    _Atomic uint64_t  publishes_since_gc;
    _Atomic int       gc_interval;      // run GC every N publications (0 = manual only); set by every open that names it, hence atomic
};

// VFS plumbing shared with the lane implementation
void        mw_ev (mw_event_t e, mw_file *f, int64_t a, int64_t b, int flags);
sqlite3_vfs *mw_root_vfs (void);
mw_db      *mw_db_find_by_wal (const char *wal_name);
// The format of the files of the engine that outlive a process (the log "<db>-mw" and the segments "<db>-mw.N": they hold commits that are not in the database file yet). A change of their layout
// that a reader of the old one would misread raises MW_FORMAT_VERSION; a change that an old reader can ignore does not exist: every addition is an incompatible feature, a bit of `features`
// in the header, that this library refuses when it does not know it. A file of another format is never read, never replaced and never dropped as damaged: the open fails.
#define MW_FORMAT_VERSION  1u
#define MW_FORMAT_FEATURES 0ull           // the bits of `features` that this library knows
int mw_format_check (const char *what, const char *path, const char magic[8], const char *expected, uint32_t version, uint64_t features);

int         mw_ensure_wal_db (const char *path);
bool        mw_path_is_clean_wal_db (const char *path);

// private lanes (multiwriter_lane.c)
void mw_lane_methods_init (const sqlite3_io_methods *pass);   // once, from mw_vfs_register()
const sqlite3_io_methods *mw_vfs_pass_io (void);
int  mw_lane_open_main (mw_file *f, mw_lane *lane);          // sets f->base.pMethods
int  mw_lane_open_wal (mw_file *f, const char *name);        // no real file: memory only
void mw_lane_trim (mw_lane *lane);
void mw_lane_reset (mw_lane *lane, bool keep_header);                          // drop the private WAL + invalidate the wal-index header
void mw_lane_free (mw_lane *lane);

// page store (multiwriter_pages.c)
mw_store *mw_store_create (int pgsz, uint32_t base_dbsize);
void      mw_store_free (mw_store *st);
// Copies bytes [poff, poff+n) of the newest version of `pgno` with epoch <= snap. 0 if none.
int       mw_store_read (mw_store *st, uint32_t pgno, uint64_t snap, uint32_t poff, uint32_t n, void *dst);   // 1 read, 0 no version (the real file's page), -1 could not be read
uint32_t  mw_store_dbsize (mw_store *st, uint64_t snap);          // pages, as of snap
void      mw_store_cache_base (mw_store *st, uint32_t pgno, const void *image);   // real-file page -> epoch-0 version (bounded)
mw_chain *mw_store_chain (mw_store *st, uint32_t pgno);            // (compaction) existing chain; caller locks the stripe
// Atomically publishes a transaction: new epoch = db->epoch + 1. `images[i]` is the page image of pgnos[i].
// Reclaims versions no active or future snapshot can observe. Returns the number reclaimed.
uint64_t  mw_db_gc (mw_db *db);
#define MW_RELOC_NA 0x4d5a            // internal result: the conflict is not a growth-only one (page relocation does not apply)
#define MW_CONFLICT 0x4d57            // internal result: the write set changed since the snapshot
#define MW_CONFLICT_READ 0x4d59       // internal result: a page the transaction only read changed (read dependency)
#define MW_CONFLICT_SCHEMA 0x4d58     // internal result: the schema changed since the snapshot (never rebased)
typedef struct {
    uint64_t        snapshot_epoch;
    bool            check_cookie;     // compare the schema cookie (page 1, bytes 40..43) seen at the snapshot
    uint32_t        cookie;
    const uint32_t *read_pgnos;       // pages read but not written (validated like the write set, but never rebased)
    int             n_read;
    // pages this same snapshot already committed itself (sorted): their head version is our own commit `own_epoch`, not a conflict
    const uint32_t *own_pgnos;
    int             own_n;
    uint64_t        own_epoch;
    const uint64_t *own_epochs;       // optional: the epoch each own page was merged against (parallel to own_pgnos); own_epoch when NULL
    bool            adopt_images;     // the images are individually malloc'ed blocks the store may keep: it frees them if the publication fails
} mw_validate;
// Validates against `v` and installs atomically under the store write lock (the only serialization point).
int       mw_store_install_recovered (mw_store *st, uint64_t epoch, uint32_t dbsize, int n, const uint32_t *pgnos, const uint8_t *const *images);
// `sync`: fsync the log before the commit becomes visible (synchronous=FULL semantics).
// Locks of the page store are held for fractions of a microsecond. A contended acquisition of a plain pthread mutex sleeps in the kernel (a wake-up costs tens
// of microseconds on macOS), which under 32 threads turned a 4 us relocation critical section into 18 us: spin a little first.
extern uint64_t mw_spin_ns;                 // longest spin before blocking (env MW_SPIN_US, default 50 us; 0 = never spin)
uint64_t mw_stage_now (void);
static inline void mw_spinlock (pthread_mutex_t *m) {
    if (mw_spin_ns) {
        if (pthread_mutex_trylock(m) == 0) return;
        uint64_t t0 = 0;
        for (unsigned i = 1; ; i++) {
            if (pthread_mutex_trylock(m) == 0) return;
#if defined(__aarch64__)
            __asm__ __volatile__("yield");
#elif defined(__x86_64__)
            __asm__ __volatile__("pause");
#endif
            if ((i & 15) == 0) { uint64_t t = mw_stage_now(); if (!t0) t0 = t; else if (t - t0 > mw_spin_ns) break; }
        }
    }
    pthread_mutex_lock(m);
}

// Optional per-stage timing (env MW_TIMING): nanoseconds, calls, calls slower than 500 us, worst call. Off by default: one predictable branch.
enum { MW_ST_PUBLISH, MW_ST_LOCKS, MW_ST_APPEND, MW_ST_SYNC, MW_ST_VISIBLE, MW_ST_RELOC, MW_ST_TURN, MW_ST_SNAPBEGIN, MW_ST_RELOCLOCK, MW_ST_RELOCHOLD, MW_ST_MERGE, MW_ST_H_HEAD, MW_ST_H_PREP, MW_ST_H_STRIPES, MW_ST_H_VALIDATE, MW_ST_H_SEQ, MW_ST_H_INSTALL, MW_ST_SY_PREFIX, MW_ST_SY_BEHIND, MW_ST_SY_CYCLE, MW_ST_SY_WRITE, MW_ST_SY_FSYNC, MW_ST_VIS_SPIN, MW_ST_VIS_PARK, MW_ST_MP_CATCH, MW_ST_MP_WAIT, MW_ST_MP_HELD, MW_ST_MP_APPLY, MW_ST_MP_GEN, MW_ST_MP_REOPEN, MW_ST_MP_SCAN, MW_ST_MP_REWRITE, MW_ST_MP_REWRITE_WAIT, MW_ST_ADM_WAIT, MW_ST_ADM_HOLD, MW_ST_SYNC_FOLLOW, MW_ST_ADM_GAP, MW_ST_SH_VALIDATE, MW_ST_SH_INSTALL, MW_ST_SH_OWNER, MW_ST_SH_MM, MW_ST_SH_CELLS, MW_ST_SH_REST, MW_ST_COUNT };
extern bool mw_timing_on;
void mw_stage_add (int stage, uint64_t ns);
void mw_count_add (int counter, uint64_t v);      // (timing) named counters: see mw_timing_dump
enum { MW_C_SY_LEADER, MW_C_SY_FOLLOWER, MW_C_SY_RECS, MW_C_SY_BYTES, MW_C_SY_WAKES, MW_C_VIS_IMMEDIATE, MW_C_VIS_SPUN, MW_C_VIS_PARKED, MW_C_ADM_BUSY, MW_C_ADM_SAMPLES, MW_C_ADM_WAITERS, MW_C_COUNT };
uint64_t mw_stage_now (void);
#define MW_T0() (mw_timing_on ? mw_stage_now() : 0)
#define MW_T1(stage, t0) do { if (mw_timing_on) mw_stage_add((stage), mw_stage_now() - (t0)); } while (0)
bool      mw_interior_merge (const uint8_t *base, const uint8_t *ours, const uint8_t *theirs, int pgsz, int reserved, uint8_t *out);   // three-way merge of an interior table page
bool      mw_interior_routes_same (const uint8_t *old, const uint8_t *nw, int pgsz, int reserved, const uint32_t *used, int nused);
uint64_t  mw_store_head_epoch (mw_store *st, uint32_t pgno);                                   // newest committed version's epoch (0: none)
bool      mw_store_head_image (mw_store *st, uint32_t pgno, uint8_t *dst, uint64_t *epoch);   // newest committed image of a page, and its epoch, atomically
void      mw_lane_reloc_prepare (mw_lane *lane, const uint32_t *pgnos, const uint8_t *const *imgs, int n, uint32_t ws_dbsize, uint32_t snap_dbsize);   // before the lock: the private copies of a relocation
void      mw_lane_reloc_discard (mw_lane *lane);
int       mw_lane_relocate (mw_lane *lane, const mw_validate *v0, const uint32_t *pgnos, const uint8_t *const *imgs, int n, uint32_t ws_dbsize, uint32_t snap_dbsize, int sync, uint64_t *out_epoch);
int       mw_db_publish (mw_db *db, mw_lane *lane, const mw_validate *v, const uint32_t *pgnos, const uint8_t *const *images, int n, uint32_t ws_dbsize, uint32_t snap_dbsize, int sync, uint64_t *out_epoch);
int       mw_store_install_recovered (mw_store *st, uint64_t epoch, uint32_t dbsize, int n, const uint32_t *pgnos, const uint8_t *const *images);

// durable log (multiwriter_log.c)
int       mw_log_open (mw_db *db, int pgsz);                       // create or recover; installs recovered commits in the store
void      mw_log_close (mw_db *db, bool remove_file);
int       mw_log_append (mw_db *db, uint64_t off, uint64_t epoch, uint32_t dbsize, int n, const uint32_t *pgnos, const uint8_t *const *images, const uint8_t *ext, uint32_t ext_len, int sync);
void      mw_log_decide_mode (mw_db *db, int sync);                  // how records are written is decided once, before the first offset is assigned
void      mw_log_stage_reset (mw_db *db, uint64_t off);              // the log restarts at `off`: nothing staged or unflushed
uint64_t  mw_log_record_size (mw_db *db, int n, uint32_t ext_len);
typedef struct mw_rext { uint64_t epoch; uint8_t *data; uint32_t len; } mw_rext;
int       mw_recovered_ext_add (mw_db *db, uint64_t epoch, const uint8_t *ext, uint32_t len);          // recovery: an extension seen in the log, kept for the change capture to replay
int       mw_log_sync (mw_db *db, uint64_t epoch, uint64_t my_end);   // my_end: file offset where this commit's record ends (staged mode)
int       mw_log_set_base (mw_db *db, uint64_t base_epoch);
char     *mw_sidecar_path (const char *dbpath, const char *suffix);        // where the files the processes of the shared mode map together live (sqlite3_free the result)
int       mw_db_recover (mw_db *db);                                 // a failed log (single process, staged): roll back to the last durable commit and go on, without a reopen
int       mw_log_ensure_room (mw_db *db, uint64_t record_size);      // staged log: reserve the disk for the next records before an offset is assigned (SQLITE_FULL when there is no room: nothing was assigned yet)
void      mw_log_reserve_space (mw_db *db);                          // caller holds store->seq_mu: grow the file ahead of log_off
void      mw_log_remap (mw_db *db);                                  // after the file was replaced/truncated (seq_mu held)
typedef struct mw_log_prep mw_log_prep;
mw_log_prep *mw_log_rewrite_prepare (mw_db *db, uint64_t base_epoch);   // staged log: copies the durable records newer than base_epoch into the new file while commits go on (NULL: not possible)
void      mw_log_rewrite_abort (mw_log_prep *p);
int       mw_log_rewrite_tail (mw_db *db, uint64_t base_epoch, mw_log_prep *prep);      // caller holds store->seq_mu; consumes `prep` (may be NULL)
bool      mw_fault_hit (mw_fault_t f);                              // error points: true = fail now; crash points never return
int       mw_db_make_visible (mw_db *db, uint64_t epoch);
void      mw_db_wake_all_visibility (mw_db *db);           // epoch E becomes visible only after E-1
// compaction (multiwriter_compact.c)
int       mw_db_compact (mw_db *db, mw_compact_result *out);
void      mw_db_compactor_start (mw_db *db, int interval_ms);
void      mw_db_compactor_stop (mw_db *db);
void      mw_db_compactor_kick (mw_db *db);
// multi-process (multiwriter_mp.c)
int       mw_mp_open (mw_db *db);                      // lock file + shared header (first process initialises it)
int       mw_mp_finish_open (mw_db *db);               // after the log was opened/recovered
void      mw_mp_close (mw_db *db, bool *sole);         // *sole: this was the last process
void      mw_mp_lock (mw_db *db);                      // publication lock (in-process mutex + fcntl)
void      mw_mp_unlock (mw_db *db);
bool      mw_mp_pid_alive (mw_db *db, int32_t pid);
int       mw_db_publish_finish (mw_db *db, mw_lane *lane, int rc, uint64_t epoch, int sync);   // after a publish with lane->mp_held: unlock, gate exit, cross-process group sync
void      mw_log_prefill_bg (mw_db *db);               // multi-process: keep the log file written ahead of its end, outside the publication lock
int       mw_mp_admit (mw_lane *lane);                 // wait for an admission slot (bounded); the slot is released by mw_mp_admit_release
void      mw_mp_admit_release (mw_lane *lane);
int       mw_mp_slot_alloc (mw_db *db);
void      mw_mp_slot_free (mw_db *db, int slot);
uint64_t  mw_mp_global_oldest (mw_db *db);             // min snapshot over every live process (compaction target)
int64_t   mw_mp_reserve (mw_lane *lane, int64_t at_least);
int64_t   mw_mp_ceiling (mw_db *db);
void      mw_mp_snapshot_register (mw_lane *lane);     // publishes lane->tx.snapshot_epoch in the slot (after the epoch was read)
void      mw_mp_ddl_begin (mw_db *db);
void      mw_mp_ddl_end (mw_db *db);
bool      mw_mp_ddl_blocked (mw_db *db);               // another live process owns the schema barrier
void      mw_mp_writing (mw_lane *lane, bool on);
void      mw_mp_rebase_lock (mw_db *db);               // the replays of the rebase, one at a time in all the processes
void      mw_mp_rebase_unlock (mw_db *db);
bool      mw_mp_compaction_lock (mw_db *db);
bool      mw_mp_meta_lock (mw_db *db, int which, bool wait);     // cross-process byte locks of the metadata store: 0 = sites, 1 = flusher
void      mw_mp_meta_unlock (mw_db *db, int which);
void      mw_mp_compaction_unlock (mw_db *db);
uint64_t  mw_mp_compaction_target (mw_db *db);
void      mw_mp_reap_dead_slots (mw_db *db);          // frees the registry slots of dead processes (their snapshots no longer hold anything back)
uint64_t  mw_log_scan_after (mw_db *db, uint64_t epoch, uint64_t end);

// registry
mw_db  *mw_db_acquire (const char *path, int mode, bool mp, int *err);   // mp: several processes (shared mode)   // NULL on OOM or if the file is already open in another mode
void    mw_db_release (mw_db *db);

// snapshots (multiwriter_tx.c)
void     mw_lane_init (mw_lane *lane, mw_db *db);
void     mw_lane_snapshot_begin (mw_lane *lane);
void     mw_lane_snapshot_end (mw_lane *lane);
void     mw_gate_enter (mw_db *db, mw_lane *lane);       // publishers: shared side
void     mw_gate_exit (mw_db *db);
void     mw_gate_close (mw_db *db, mw_lane *owner);       // starving rebase: exclusive side
void     mw_gate_open (mw_db *db);   // multiwriter_rebase.c: replay the logical changes at the latest snapshot
// One row of the net change of a commit. pk: the key as sqlite-sync encodes it (NULL if the row could not be decoded); oldpk: an update that changed the key; changed: bit i = cell i of the
void     mw_rd_result_free (mw_rd_result *r);
int      mw_lane_rebase (mw_lane *lane, const uint8_t *const *imgs, uint32_t cookie, uint64_t *out_epoch);    // multiwriter_rebase.c: replay the row changes at the latest snapshot
void     mw_lane_rebase_free (mw_lane *lane);
void     mw_lane_stmt_note (mw_lane *lane, mw_stmt_note *n);
bool     mw_lane_reads_unchanged (mw_lane *lane, int nnet);   // multiwriter_lane.c: did the transaction read rows that it did not change? (then it is not rebased)
int      mw_rowdiff_compute (mw_lane *lane, const uint8_t *const *imgs, const mw_cat *cat, mw_rd_result *res);   // multiwriter_rowdiff.c
bool     mw_rd_snap_page (mw_lane *lane, uint32_t pgno, uint8_t *dst);
void     mw_shared_repair (mw_db *db);
void     mw_lane_fill_stats (mw_lane *lane, mw_db_stats *st);
int      mw_lane_on_shm_lock (mw_lane *lane, int ofst, int flags);
void     mw_lane_ddl_begin (mw_lane *lane);
void     mw_lane_ddl_end (mw_lane *lane);   // drives the above from the stock WAL locks
uint64_t mw_db_oldest_active_snapshot (mw_db *db);   // == current epoch when nothing is active



// Tuning knobs and debug switches come from environment variables and are read once. The cache is an atomic with a sentinel: two threads initialising it at the same time
// both store the same value (a plain `static int v = -1` is a data race, which ThreadSanitizer reports).
#define MW_KNOB_UNSET INT_MIN
static inline int mw_knob_int (_Atomic int *cache, const char *name, int dflt) {
    int v = atomic_load_explicit(cache, memory_order_relaxed);
    if (v == MW_KNOB_UNSET) { const char *e = getenv(name); v = e ? atoi(e) : dflt; atomic_store_explicit(cache, v, memory_order_relaxed); }
    return v;
}
static inline bool mw_knob_flag (_Atomic int *cache, const char *name) {          // true when the variable is set
    int v = atomic_load_explicit(cache, memory_order_relaxed);
    if (v == MW_KNOB_UNSET) { v = getenv(name) != NULL; atomic_store_explicit(cache, v, memory_order_relaxed); }
    return v != 0;
}

// The log's size, read under seq_mu (it is written there): for the threads that only look at it (compactor, statistics).
uint64_t mw_seglog_bytes (mw_db *db);
static inline uint64_t mw_log_end_locked (mw_db *db) {
    if (!db->store) return 0;                                       // (no private lanes, no log)
    if (db->mp_req) return mw_seglog_bytes(db);                     // (the shared mode keeps its log in segments: log_off is not used there)
    mw_spinlock(&db->store->seq_mu);
    uint64_t v = db->log_off;
    pthread_mutex_unlock(&db->store->seq_mu);
    return v;
}

// The first token of an SQL text as sqlite3_sql() gives it: after white space and comments ("/* req:1 */ SELECT ...", "-- note\nUPDATE ..."): the text of a statement keeps them.
static inline const char *mw_sql_skip (const char *p) {
    for (;;) {
        while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r' || *p == '\f') p++;
        if (p[0] == '-' && p[1] == '-') { while (*p && *p != '\n') p++; continue; }
        if (p[0] == '/' && p[1] == '*') { p += 2; while (*p && !(p[0] == '*' && p[1] == '/')) p++; if (*p) p += 2; continue; }
        return p;
    }
}

// ---- multi-process mode (mw_mp=1): multiwriter_shared.c ----
mw_store *mw_store_create_light (int pgsz, uint32_t base_dbsize);       // a store without version chains: only pgsz / reserved / base_dbsize
int       mw_shared_open (mw_db *db);                                    // index + segmented log (after mw_mp_open); the first opener recovers
int       mw_shared_open_finish (mw_db *db);                             // after mw_mp_finish_open: publishes the log position in the header
void      mw_shared_close (mw_db *db, bool sole);
int       mw_shared_read (mw_db *db, uint32_t pgno, uint64_t snap, uint32_t poff, uint32_t n, void *dst);
uint32_t  mw_shared_dbsize (mw_db *db, uint64_t snap);
uint64_t  mw_shared_head_epoch (mw_db *db, uint32_t pgno);
bool      mw_shared_head_image (mw_db *db, uint32_t pgno, uint8_t *dst, uint64_t *epoch);
uint64_t  mw_shared_visible_epoch (mw_db *db);
int       mw_shared_publish (mw_db *db, mw_lane *lane, const mw_validate *v, const uint32_t *pgnos, const uint8_t *const *images, int n, uint32_t ws_dbsize, uint32_t snap_dbsize, int sync, uint64_t *out_epoch);
int       mw_shared_sync (mw_db *db, mw_lane *lane);
int       mw_shared_compact (mw_db *db, mw_compact_result *out);
bool      mw_shared_compact_claim (mw_db *db, bool by_size);        // election: one process (the first to claim) runs the next background compaction
uint64_t  mw_snap_for (const mw_validate *v, uint32_t pgno);              // the epoch a page is validated against (pages.c)
static inline uint64_t mw_db_visible_epoch (mw_db *db) { return db->mp_req ? mw_shared_visible_epoch(db) : atomic_load(&db->epoch); }

// The log size at which compaction is requested. Many processes (> 16): 8 MB instead of the configured size: measured 8 MB vs 32 MB, 32 processes 10.2k -> 11.2k,
// 64: 7.3k -> 8.2k tx/s (the mapping's working set stays in the caches), but 8 processes -8%.
#define MW_LOG_HARD_BYTES (768ull << 20)         // the log (one mapping of 1 GB) is not allowed to grow past this: commits wait for the compaction
static inline uint64_t mw_log_limit (const mw_db *db) {
    static _Atomic int ovr = MW_KNOB_UNSET;                         // (MW_LOG_LIMIT_MB: experiments)
    int mb = mw_knob_int(&ovr, "MW_LOG_LIMIT_MB", 0);
    if (mb > 0) return (uint64_t)mb << 20;
    uint64_t m = db->log_max_bytes;
    return m;
}

#endif
