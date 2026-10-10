//
//  multiwriter_tx.c
//  sqlite-multiwriter
//
//  Transaction identity and the active-snapshot registry.
//
//  A transaction = the interval during which a connection holds a read snapshot
//  (measured VFS mapping: first read acquires a WAL read-mark, its release ends it).
//

#include <string.h>
#include <assert.h>
#include <stdlib.h>
#include <time.h>
#include "multiwriter_os.h"

#include "multiwriter_internal.h"

// Concurrent holders of the turn among long transactions (MW_TURN_SLOTS, default 1).
static int turn_slots (void) { static _Atomic int c = MW_KNOB_UNSET; int v = mw_knob_int(&c, "MW_TURN_SLOTS", 1); return v < 1 ? 1 : v; }

static inline bool db_mp_flag (mw_lane *lane) { return lane->db->mp; }

void mw_lane_init (mw_lane *lane, mw_db *db) {
    memset(lane, 0, sizeof(*lane));
    lane->db = db;
    lane->writer_id = atomic_fetch_add(&db->next_writer_id, 1);
    lane->mp_slot = -1;
    lane->hot_credit = 16;
    lane->adm_slot = -1;
    lane->sync_level = 2;                                   // SQLite's default (FULL)
    lane->tx.writer_id = lane->writer_id;
}

// The snapshot epoch is read *while holding the registry mutex* and the lane is registered
// before the mutex is released: a concurrent oldest_active_snapshot() therefore either sees
// this lane or observes an epoch <= the one taken here. Version GC relies on this.
#define MW_WARM_AFTER 64      // read-only snapshots in a row before a connection keeps its cache (one that writes now and then must not: see reads_since_write)

void mw_lane_snapshot_begin (mw_lane *lane) {
    mw_db *db = lane->db;
    if (lane->snapshot_held) return;
    uint64_t tsb0 = MW_T0();
    // Hot-spot escalation. Optimistic execution wastes work when the same pages are updated by many writers
    // (measured: 70-80% of attempts refused on 4 hot rows). A transaction that was refused once is retried by the
    // application; that retry takes the per-database turn *before* its snapshot, so retries of a contended spot
    // run one after the other on fresh state -- the behaviour of a single-writer lock, but only for the
    // transactions that proved to conflict. Bounded wait: never blocks longer than 20 ms.
    // Hot-spot escalation. Optimistic execution wastes work when the same pages are updated by many writers
    // (measured: 60-75% of attempts refused on hot rows / concurrent appends). A lane that recently conflicted
    // (retry_credit) takes a per-database turn *before* its snapshot, so its next transactions on a contended spot run
    // one after the other on fresh state -- a single-writer lock only for the transactions that proved to conflict.
    // The wait *sleeps* (50 us polls, at most 20 ms, then goes optimistic): measured against a spinning turn and a
    // condition-variable turn, sleeping is clearly best (10 agents: appends 192k vs 108k tx/s, hot rows 228k vs 130k),
    // because lanes that are not contending keep running instead of being woken in lock-step.
    if (db->mp && lane->recent_writer && lane->adm_slot < 0 && lane->tx_long_run < 3) mw_mp_admit(lane);      // (a writer of the previous transaction waits for a slot when there are many processes)
    uint64_t tturn0 = MW_T0();
    if (lane->retry_credit > 0 && lane->private_mode && !lane->holds_hot) {      // (the metadata store's connections never queue for a turn behind the writers: the writers wait for them)
        if (lane->tx_long_run >= 3) {
            // Long transaction (a retry costs the whole body): wait in a FIFO queue, so the wait is bounded by the queue length and nobody starves. A waiter that
            // gives up after 1 s (a holder that never released) marks its ticket abandoned and goes optimistic; the queue skips the mark. With several processes the
            // queue lives in the shared header, and a turn held by a dead process is released by whoever notices.
            _Atomic uint64_t *ticket = db->mp ? &db->shm->hot_ticket : &db->hot_ticket, *serving = db->mp ? &db->shm->hot_serving : &db->hot_serving;
            _Atomic uint64_t *aband = db->mp ? db->shm->hot_aband : db->hot_aband;
            _Atomic int *held = db->mp ? &db->shm->hot_held : &db->hot_held;
            uint64_t t = atomic_fetch_add(ticket, 1);
            if (db->mp) atomic_store(&db->shm->hot_tk_pid[t % 256], (int32_t)getpid());
            uint64_t t0 = 0;
            for (unsigned polls = 0; ; polls++) {
                uint64_t s = atomic_load(serving);
                if (s == t) {
                    int expected = atomic_load(held);
                    if (expected < turn_slots() && atomic_compare_exchange_strong(held, &expected, expected + 1)) {
                        lane->holds_hot = true; lane->hot_shared = db->mp; atomic_fetch_add(&db->n_hot_serialised, 1); atomic_store(serving, t + 1);
                        if (db->mp) atomic_store(&db->shm->hot_owner, (int32_t)getpid());
                        break;
                    }
                    if (db->mp && (polls & 127) == 127) {
                        int32_t owner = atomic_load(&db->shm->hot_owner);
                        if (owner > 0 && owner != (int32_t)getpid() && !mw_mp_pid_alive(db, owner) && atomic_compare_exchange_strong(&db->shm->hot_owner, &owner, 0)) atomic_store(held, 0);
                    }
                } else if (s < t && atomic_load(&aband[s % 256]) == s + 1) {
                    atomic_compare_exchange_strong(serving, &s, s + 1);
                    continue;
                } else if (s < t && db->mp && (polls & 127) == 127) {
                    int32_t pid = atomic_load(&db->shm->hot_tk_pid[s % 256]);
                    if (pid > 0 && pid != (int32_t)getpid() && !mw_mp_pid_alive(db, pid)) { atomic_compare_exchange_strong(serving, &s, s + 1); continue; }
                }
                struct timespec ts = { 0, 50000 };
                nanosleep(&ts, NULL);
                if ((polls & 63) == 63) {
                    struct timespec n; clock_gettime(CLOCK_MONOTONIC, &n);
                    uint64_t now = (uint64_t)n.tv_sec * 1000000000ull + (uint64_t)n.tv_nsec;
                    if (!t0) t0 = now;
                    else if (now - t0 > 1000000000ull) { atomic_store(&aband[t % 256], t + 1); break; }
                }
            }
        } else for (int i = 0; i < 400; i++) {
            int expected = 0;
            if (atomic_compare_exchange_strong(&db->hot_held, &expected, 1)) { lane->holds_hot = true; atomic_fetch_add(&db->n_hot_serialised, 1); break; }
            struct timespec ts = { 0, 50000 };
            nanosleep(&ts, NULL);
        }
        MW_T1(MW_ST_TURN, tturn0);
    }
    for (;;) {
        sqlite3_mutex_enter(db->mu);
        lane->tx.snapshot_epoch = mw_db_visible_epoch(db);
        lane->next_active = db->active;
        lane->prev_active = NULL;
        if (db->active) db->active->prev_active = lane;
        db->active = lane;
        lane->snapshot_held = true;
        { struct timespec t0; clock_gettime(CLOCK_MONOTONIC, &t0); lane->tx_t0 = (uint64_t)t0.tv_sec * 1000000000ull + (uint64_t)t0.tv_nsec; }
        sqlite3_mutex_leave(db->mu);
        if (!db->mp) break;
        mw_mp_snapshot_register(lane);
        // a compactor in another process may have announced a target above this snapshot: refresh and retry
        if (lane->tx.snapshot_epoch >= atomic_load(&db->shm->compact_T)) break;
        sqlite3_mutex_enter(db->mu);
        if (lane->prev_active) lane->prev_active->next_active = lane->next_active; else db->active = lane->next_active;
        if (lane->next_active) lane->next_active->prev_active = lane->prev_active;
        lane->next_active = lane->prev_active = NULL;
        lane->snapshot_held = false;
        sqlite3_mutex_leave(db->mu);
        atomic_store(&db->shm->slots[lane->mp_slot].snap, MW_MP_NONE);
    }

    lane->tx.tx_id = atomic_fetch_add(&db->next_tx_id, 1);
    lane->tx.commit_epoch = 0;
    lane->tx.commit_order = 0;
    lane->tx.schema_generation = atomic_load(&db->schema_generation);
    lane->tx.state = MW_TX_ACTIVE;
    lane->tx.is_writer = 0;
    lane->tx.ws_pages = 0;
    lane->tried_write = false;
    if (lane->reads_since_write < UINT32_MAX) lane->reads_since_write++;
    if (lane->warm && lane->warm_epoch == lane->tx.snapshot_epoch) {
        // the same epoch as the read-only snapshot before: the cached pages are right, and the read set stays (a superset of what this transaction reads: its cache hits never reach the VFS)
        lane->warm = false;
    } else {
        if (lane->warm && lane->nshm > 0 && lane->shm[0]) memset(lane->shm[0], 0, 96);       // another epoch: the header is invalidated before SQLite looks at it again, and it resets its cache
        lane->warm = false;
        for (int i = 0; i < lane->rs_n; i++) lane->rs_bits[lane->rs_list[i]] = 0;
        lane->rs_n = 0;
        lane->rs_overflow = false;
    }
    lane->ws_n = 0;                 // write set of the previous transaction stays inspectable until now
    mw_lane_trim(lane);
    atomic_fetch_add(&db->n_snapshots, 1);
    MW_T1(MW_ST_SNAPBEGIN, tsb0);
}

void mw_lane_snapshot_end (mw_lane *lane) {
    mw_db *db = lane->db;
    if (!lane->snapshot_held) return;
    sqlite3_mutex_enter(db->mu);
    if (lane->prev_active) lane->prev_active->next_active = lane->next_active;
    else db->active = lane->next_active;
    if (lane->next_active) lane->next_active->prev_active = lane->prev_active;
    lane->next_active = lane->prev_active = NULL;
    lane->snapshot_held = false;
    if (lane->holds_hot) {
        lane->holds_hot = false;
        if (db->mp && lane->hot_shared) { atomic_store(&db->shm->hot_owner, 0); atomic_fetch_sub(&db->shm->hot_held, 1); }
        else atomic_fetch_sub(&db->hot_held, 1);
        lane->hot_shared = false;
    }
    if (lane->ddl_active) mw_lane_ddl_end(lane);
    if (db->mp) { lane->recent_writer = lane->tx.is_writer != 0; mw_mp_admit_release(lane); }
    if (db->mp && lane->mp_slot >= 0) { atomic_store(&db->shm->slots[lane->mp_slot].writing, 0); atomic_store(&db->shm->slots[lane->mp_slot].snap, MW_MP_NONE); atomic_store(&db->shm->slots[lane->mp_slot].minres, 0); }
    if (!lane->tx.is_writer && lane->retry_credit > 0) lane->retry_credit--;      // (a read-only transaction spends hot-spot credit too: it must not keep others waiting for ever)
    sqlite3_mutex_leave(db->mu);

    if (lane->tx.state == MW_TX_ACTIVE) lane->tx.state = lane->tx.is_writer ? MW_TX_ABORTED : MW_TX_COMMITTED;
}

uint64_t mw_db_oldest_active_snapshot (mw_db *db) {
    sqlite3_mutex_enter(db->mu);
    uint64_t oldest = mw_db_visible_epoch(db);
    for (mw_lane *l = db->active; l; l = l->next_active) {
        if (l->tx.snapshot_epoch < oldest) oldest = l->tx.snapshot_epoch;
    }
    sqlite3_mutex_leave(db->mu);
    return oldest;
}

// WAL-index header fields (native byte order): iChange at 8, mxFrame at 16.
static void hdr_read (mw_lane *lane, uint32_t *change, uint32_t *frames) {
    *change = *frames = 0;
    if (!lane->shm0) return;
    memcpy(change, (const void *)(lane->shm0 + 8), 4);
    memcpy(frames, (const void *)(lane->shm0 + 16), 4);
}

// Lane bookkeeping driven by the stock WAL locks (docs/multiwriter.md, section 3.1).
// Slot 0 = WAL write lock; slots >= 3 = read-marks; a SHARED read-mark is the snapshot.
//
// Two subtleties measured with tracing:
//  - the first write of a transaction on an empty WAL swaps read-mark 0 for a real read-mark
//    (walRestartLog) *while the write lock is held*: that unlock/lock pair is not a snapshot end;
//  - WAL recovery (also run at the start of every private-lane transaction) takes and
//    releases the write lock before any read-mark is held: not a writer.
int mw_lane_on_shm_lock (mw_lane *lane, int ofst, int flags) {
    bool lock = (flags & SQLITE_SHM_LOCK) != 0;
    if (ofst >= 3 && (flags & SQLITE_SHM_SHARED)) {
        if (lock) {
            mw_lane_snapshot_begin(lane);                     // no-op if already held
        } else if (!lane->write_locked) {
            // A connection that has not written for MW_WARM_AFTER snapshots keeps its page cache from one snapshot to the next if that is taken at the same epoch (the header of the wal-index stays valid, so SQLite
            // does not reset the cache): nothing has changed. A snapshot that wrote, or that could not track its reads, starts the next one cold. The next snapshot_begin
            // invalidates the header if its epoch is another one.
            const bool warm = lane->private_mode && !lane->tx.is_writer && lane->reads_since_write >= MW_WARM_AFTER && !lane->tried_write && lane->ws_n == 0 && !lane->rs_overflow && lane->tx.state == MW_TX_ACTIVE;
            const uint64_t ep = lane->tx.snapshot_epoch;
            mw_lane_snapshot_end(lane);
            if (lane->private_mode) { mw_lane_reset(lane, warm); lane->warm = warm; lane->warm_epoch = ep; }
        }
    } else if (ofst == 0 && (flags & SQLITE_SHM_EXCLUSIVE) && lane->snapshot_held) {
        if (lock) {
            lane->tried_write = true; lane->reads_since_write = 0;
            if (lane->private_mode) {
                mw_db *db = lane->db;
                // A snapshot older than the newest schema change can never publish: fail fast (retryable)
                // instead of doing the work and failing at commit on the schema cookie.
                if (lane->tx.snapshot_epoch < atomic_load(&db->last_schema_epoch) && !lane->ddl_active) return SQLITE_BUSY_SNAPSHOT;
                // Schema barrier: no new write transaction while a DDL statement runs (SQLite's busy handler waits).
                if (db->ddl_owner && db->ddl_owner != lane) return SQLITE_BUSY;
                if (db->mp && !lane->ddl_active && mw_mp_ddl_blocked(db)) return SQLITE_BUSY;      // another process runs a schema change
            }
            lane->write_locked = true;
            if (db_mp_flag(lane)) mw_mp_writing(lane, true);
            lane->tx.is_writer = 1;
            hdr_read(lane, &lane->hdr_change_at_lock, &lane->hdr_frames_at_lock);
        } else if (lane->write_locked) {
            lane->write_locked = false;
            if (db_mp_flag(lane)) mw_mp_writing(lane, false);
            if (lane->private_mode) return SQLITE_OK;         // commit = the commit frame, not the header
            uint32_t change, frames;
            hdr_read(lane, &change, &frames);
            if (change != lane->hdr_change_at_lock || frames != lane->hdr_frames_at_lock) {   // WAL grew: a commit happened
                lane->tx.commit_epoch = atomic_fetch_add(&lane->db->epoch, 1) + 1;
                lane->tx.state = MW_TX_COMMITTED;
                atomic_fetch_add(&lane->db->n_commits, 1);
            } else {
                lane->tx.state = MW_TX_ABORTED;
                atomic_fetch_add(&lane->db->n_aborts, 1);
            }
        }
    }
    return SQLITE_OK;
}

void mw_lane_fill_stats (mw_lane *lane, mw_db_stats *st) {
    mw_db *db = lane->db;
    memset(st, 0, sizeof(*st));
    st->epoch = mw_db_visible_epoch(db);
    st->oldest_active_snapshot = mw_db_oldest_active_snapshot(db);
    sqlite3_mutex_enter(db->mu);
    for (mw_lane *l = db->active; l; l = l->next_active) st->snapshot_count++;
    sqlite3_mutex_leave(db->mu);
    st->commits = atomic_load(&db->n_commits);
    st->aborts = atomic_load(&db->n_aborts);
    st->schema_generation = atomic_load(&db->schema_generation);
    st->last_schema_epoch = atomic_load(&db->last_schema_epoch);
    st->ddl_barriers = atomic_load(&db->n_ddl_barriers);
    st->rebases = atomic_load(&db->n_rebases); st->rebases_grouped = atomic_load(&db->n_rebase_grouped);
    st->relocations = atomic_load(&db->n_relocations);
    st->reloc_prep_used = atomic_load(&db->n_prep_used); st->reloc_prep_dropped = atomic_load(&db->n_prep_dropped); st->reloc_prep_rewrote = atomic_load(&db->n_prep_rewrote);
    st->reads_saved = atomic_load(&db->n_reads_saved);
    st->merges = atomic_load(&db->n_merges);
    st->rebase_retries = atomic_load(&db->n_rebase_retries);
    st->rebase_max_attempts = atomic_load(&db->n_rebase_max_attempts);
    st->rebase_ns = atomic_load(&db->n_rebase_ns);
    st->unrebasable = atomic_load(&db->n_unrebasable);
    st->read_conflicts = atomic_load(&db->n_read_conflicts);
    pthread_mutex_lock(&db->log_mu);
    const uint64_t base_epoch = db->base_epoch;                     // (written under log_mu)
    pthread_mutex_unlock(&db->log_mu);
    if (db->mp_req && db->shm) { st->epoch = atomic_load(&db->shm->committed_epoch); st->base_epoch = atomic_load(&db->shm->base_epoch); }   // (shared mode: the state is the shared one, not this process's)
    else st->base_epoch = base_epoch;
    st->compaction_backlog = st->epoch - st->base_epoch;
    st->log_bytes = mw_log_end_locked(db);
    st->compactions = atomic_load(&db->n_compactions);
    st->compaction_ns = atomic_load(&db->n_compaction_ns);
    st->compacted_pages = atomic_load(&db->n_compacted_pages);
    st->log_syncs = atomic_load(&db->n_log_syncs);
    st->pages_published = atomic_load(&db->n_pages_published);
    st->gate_closures = atomic_load(&db->n_gate_closures);
    st->hot_serialised = atomic_load(&db->n_hot_serialised);
    st->backpressure_stalls = atomic_load(&db->n_backpressure);
    st->log_sync_ns = atomic_load(&db->n_log_sync_ns);
    st->fast_commits = atomic_load(&db->n_fast_commits);
    st->page_conflicts = atomic_load(&db->n_page_conflicts);
    st->schema_conflicts = atomic_load(&db->n_schema_conflicts);
    mw_store *store = db->store;
    if (store) {
        st->page_versions = atomic_load(&store->versions);
        st->bytes_retained = atomic_load(&store->bytes);
        st->versions_allocated = atomic_load(&store->versions_allocated);
        st->versions_reclaimed = atomic_load(&store->versions_reclaimed);
        st->gc_runs = atomic_load(&store->gc_runs);
        st->gc_ns = atomic_load(&store->gc_ns);
    }
}

// MARK: - db_version reservation and the sync frontier -
//
// MARK: - exclusive schema barrier -
//
// Sequence for a DDL statement (detected at statement start by the trace hook): take the schema mutex
// (one DDL at a time), then wait until no other lane has a write transaction in flight, then run. While
// the barrier is up, other lanes' attempts to start writing get SQLITE_BUSY (the busy handler retries).
// The barrier is released when the DDL transaction ends. It is an optimisation of *when* the schema
// change executes: correctness never depends on it, because any transaction that overlaps a schema
// change fails validation on the schema cookie (multiwriter_pages.c) and is retried.

#define MW_DDL_WAIT_MS 2000

static void ms_to_abs (struct timespec *ts, int ms) {
    clock_gettime(CLOCK_REALTIME, ts);
    ts->tv_sec += ms / 1000;
    ts->tv_nsec += (long)(ms % 1000) * 1000000L;
    if (ts->tv_nsec >= 1000000000L) { ts->tv_sec++; ts->tv_nsec -= 1000000000L; }
}

void mw_lane_ddl_begin (mw_lane *lane) {
    mw_db *db = lane->db;
    if (lane->ddl_active || !lane->private_mode) return;
    struct timespec until;
    ms_to_abs(&until, MW_DDL_WAIT_MS);
    pthread_mutex_lock(&db->ddl_mu);
    while (db->ddl_owner && db->ddl_owner != lane) {
        if (pthread_cond_timedwait(&db->ddl_cv, &db->ddl_mu, &until) != 0) break;    // give up waiting: validation still protects us
    }
    if (db->ddl_owner == NULL) { db->ddl_owner = lane; lane->ddl_active = true; }
    pthread_mutex_unlock(&db->ddl_mu);
    if (!lane->ddl_active) return;
    atomic_fetch_add(&db->n_ddl_barriers, 1);
    if (db->mp) mw_mp_ddl_begin(db);

    // drain: wait for other lanes' in-flight write transactions
    for (int waited = 0; waited < MW_DDL_WAIT_MS * 5; waited++) {
        int writers = 0;
        sqlite3_mutex_enter(db->mu);
        for (mw_lane *l = db->active; l; l = l->next_active) if (l != lane && l->write_locked) writers++;
        sqlite3_mutex_leave(db->mu);
        if (writers == 0) return;
        struct timespec ts = { 0, 200000 };                      // 200 us
        nanosleep(&ts, NULL);
    }
}

void mw_lane_ddl_end (mw_lane *lane) {
    mw_db *db = lane->db;
    if (db->mp) mw_mp_ddl_end(db);
    pthread_mutex_lock(&db->ddl_mu);
    if (db->ddl_owner == lane) db->ddl_owner = NULL;
    lane->ddl_active = false;
    pthread_cond_broadcast(&db->ddl_cv);
    pthread_mutex_unlock(&db->ddl_mu);
}
