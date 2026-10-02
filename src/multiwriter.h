//
//  multiwriter.h
//  cloudsync
//
//  Experimental Multi-Writer mode for SQLite (compile-time: CLOUDSYNC_MULTIWRITER).
//  See docs/multiwriter.md. Not part of the normal SQLite/PostgreSQL builds.
//

#ifndef __CLOUDSYNC_MULTIWRITER__
#define __CLOUDSYNC_MULTIWRITER__

#include <stddef.h>
#include <stdint.h>
#include "sqlite3.h"

#ifdef __cplusplus
extern "C" {
#endif

#define MW_VFS_NAME "multiwriter"

// Registers the Multi-Writer VFS as a wrapper over the platform default VFS.
// It must be called before the first sqlite3_open(). make_default != 0 makes it
// the default VFS. Idempotent. Returns SQLITE_OK on success.
int mw_vfs_register (int make_default);

// Unregisters the VFS (only valid when no connection is open on it).
int mw_vfs_unregister (void);

// Enables lane tracking for every database opened through the VFS (default 0). A single
// database can be opted in/out with the URI parameter mw=1 / mw=0.
void mw_vfs_set_enabled_default (int enabled);

// Transaction identity of the connection's current (or last) transaction.
typedef enum { MW_TX_NONE = 0, MW_TX_ACTIVE, MW_TX_PREPARED, MW_TX_COMMITTED, MW_TX_ABORTED } mw_tx_state;

typedef struct {
    uint64_t    tx_id;              // unique per transaction, per database, per process run
    uint64_t    writer_id;          // the connection's lane; distinct from the sqlite-sync site_id
    uint64_t    snapshot_epoch;     // committed epoch this transaction reads from
    uint64_t    commit_epoch;       // epoch assigned at publication (0 while unpublished / read-only)
    uint32_t    schema_generation;
    mw_tx_state state;
    int         is_writer;
    uint32_t    ws_pages;           // distinct pages in the physical write set (set at the commit frame)
} mw_tx_info;

typedef struct { uint32_t *pgnos; int cap; int n; } mw_writeset;
#define MW_FCNTL_WRITESET 0x4d570003   // sqlite3_file_control(db, "main", MW_FCNTL_WRITESET, mw_writeset *)

#define MW_FCNTL_TXINFO   0x4d570001   // sqlite3_file_control(db, "main", MW_FCNTL_TXINFO, mw_tx_info *)
#define MW_FCNTL_DBSTATS  0x4d570002   // sqlite3_file_control(db, "main", MW_FCNTL_DBSTATS, mw_db_stats *)

typedef struct {
    uint64_t epoch;                 // last committed epoch
    uint64_t oldest_active_snapshot;
    uint64_t snapshot_count;        // snapshots currently held
    uint64_t commits, aborts;
    uint32_t schema_generation;
    uint64_t last_schema_epoch;     // epoch of the newest schema change (older snapshots cannot write)
    uint64_t ddl_barriers;
    uint64_t page_versions;         // versions currently held in the page store
    uint64_t bytes_retained;
    uint64_t versions_allocated, versions_reclaimed;
    uint64_t gc_runs, gc_ns;
    uint64_t fast_commits, page_conflicts, schema_conflicts;
    uint64_t base_epoch;            // state up to this epoch is in the real database file
    uint64_t compaction_backlog;    // commits not yet materialised (epoch - base_epoch)
    uint64_t pages_published, log_sync_ns, gate_closures, backpressure_stalls, hot_serialised;
    uint64_t log_bytes, compactions, compaction_ns, compacted_pages, log_syncs;
    uint64_t rebases, rebase_retries, rebase_max_attempts, rebase_ns, unrebasable, read_conflicts, pk_collisions;
    uint64_t relocations;           // commits saved by renumbering their new pages after other commits extended the file
    uint64_t merges;                // interior b-tree pages rewritten by two transactions that were merged instead of refusing the second
    uint64_t reads_saved;           // read conflicts on interior pages avoided because the pages the transaction went through were routed unchanged
} mw_db_stats;

#define MW_FCNTL_RESERVE_DBV 0x4d570005   // in/out int64_t: in = "at least", out = a unique db_version for this transaction
#define MW_FCNTL_SEND_CEILING 0x4d570006  // out int64_t: highest db_version that may be exported (sync frontier)
#define MW_FCNTL_MARK_UNREBASABLE 0x4d570007  // the transaction wrote something with no logical representation
#define MW_FCNTL_SET_OVERLAY 0x4d570008   // internal: mw_overlay * (read view used by the rebase)
#define MW_FCNTL_DDL_BEGIN 0x4d570009   // internal: a schema-changing statement is about to run (exclusive schema barrier)
#define MW_FCNTL_LANE_PTR 0x4d57000b   // internal: void ** (the connection's lane)
#define MW_FCNTL_GC             0x4d570004   // sqlite3_file_control(db, "main", MW_FCNTL_GC, uint64_t *reclaimed)

// Read view of a transaction that is being rebased: its own page images over the snapshot.
typedef struct {
    const uint32_t *pgnos;                  // ascending
    const uint8_t *const *images;
    int             n;
    uint32_t        dbsize;                 // pages
    uint64_t        snapshot;               // epoch the transaction executed against
} mw_overlay;

// db_version reservation / sync frontier for the connection's database. Both return the
// "not a private lane" answer (-1 / INT64_MAX) for a database that is not in lane mode.
int64_t mw_db_reserve_version (sqlite3 *db, int64_t at_least);
int64_t mw_db_send_ceiling (sqlite3 *db);

// Fault / crash injection (tests). arm(f, nth): the nth time the point is reached, error points fail once
// (SQLITE_IOERR); crash points _exit(9) on the spot, simulating a kill at exactly that instant.
typedef enum {
    MW_FAULT_NONE = 0,
    MW_FAULT_LOG_WRITE_ERR, MW_FAULT_LOG_SYNC_ERR, MW_FAULT_ALLOC_ERR,
    MW_CRASH_MID_LOG,           // half of the commit record written (torn tail)
    MW_CRASH_BEFORE_LOG,        // validated + installed, nothing written yet (crash "during prepare")
    MW_CRASH_AFTER_LOG,         // record durable, transaction not yet visible (crash "after durable write, before publish")
    MW_CRASH_AFTER_VISIBLE,     // just after the transaction became visible
    MW_CRASH_COMPACT_PAGES,     // compaction: after writing the pages into the real file, before recording the new base
    MW_CRASH_COMPACT_BASE,      // compaction: after the new base epoch was recorded
    MW_CRASH_LOG_RENAME,        // log rewrite: new log file renamed into place, header (log_pos) not yet updated
    MW_FAULT_COUNT
} mw_fault_t;
void mw_fault_arm (mw_fault_t f, int nth);
void mw_fault_disarm_all (void);

// EXPERIMENT (docs §51): row-level changes of every commit, derived from the page images alone. A sink is called once per commit of a private lane, from the
// committing thread, before the commit is published. Changes are the net change of the table rows over the whole write set (rows of different tables are pooled).
typedef struct { int kind; int64_t rowid; uint32_t changed; uint32_t root; int ncols; } mw_rowchg;      // root = the table (its root page) when the page owners are known, ncols = columns of the record           // kind 1 insert, 2 update (changed = bitmask of columns, bit 31: not decidable per column), 3 delete
typedef struct { int pages, opaque, index_pages, interior, schema, undecodable; uint64_t ns; } mw_rowdiff_info;   // what the write set held, and the decoding time
typedef void (*mw_rowdiff_sink_fn) (void *arg, const mw_rowchg *chg, int n, const mw_rowdiff_info *info);
void mw_rowdiff_set_sink (mw_rowdiff_sink_fn fn, void *arg);

// Change capture (URI mw_cdc=1; docs/design.md). The row-level changes of every commit of the connection's database, derived in the VFS from the pages: the table, the primary key as
// sqlite-sync encodes it, what happened (1 insert, 2 update, 3 delete) and for an update the cells that changed (bit i = the i-th non-key column; bit 63: all of them) and, if the key itself
// changed, the old key. Called from the committing thread before the commit is published, for every attempt (a refused commit is reported again when it is retried).
typedef struct { int kind; const char *table; const uint8_t *pk; size_t pklen; const uint8_t *oldpk; size_t oldpklen; uint64_t changed; int64_t rowid; } mw_capture_row;
typedef void (*mw_capture_fn) (void *arg, const mw_capture_row *rows, int n);
#define MW_FCNTL_VACUUM_BEGIN 0x4d57000e   // internal: a VACUUM statement is about to run (the change capture must not take the rebuilt rows for changes)
#define MW_FCNTL_DECLARE  0x4d57000f       // sqlite3_file_control(db, "main", MW_FCNTL_DECLARE, mw_ovl *): the commit of this transaction carries exactly this metadata (a merge of remote changes), not what the capture would derive; NULL clears
#define MW_FCNTL_GATE     0x4d570010       // sqlite3_file_control(db, "main", MW_FCNTL_GATE, int *close): hold the commit gate of this process (1) / release it (0); a transaction that keeps losing takes the commits of the others out of its way
#define MW_FCNTL_META     0x4d57000d       // sqlite3_file_control(db, "main", MW_FCNTL_META, mw_meta **): the CRDT metadata store of the database (mw_cdc=1), for tests and the sync API
#define MW_FCNTL_CDC_SINK 0x4d57000c       // sqlite3_file_control(db, "main", MW_FCNTL_CDC_SINK, &(mw_capture_sink){ fn, arg })
typedef struct { mw_capture_fn fn; void *arg; } mw_capture_sink;

#define MW_FCNTL_COMPACT  0x4d57000a   // sqlite3_file_control(db, "main", MW_FCNTL_COMPACT, mw_compact_result *)
typedef struct {
    uint64_t target_epoch;          // epoch materialised into the real file (0 = nothing to do)
    uint64_t pages_written;
    uint64_t versions_freed;
    uint64_t duration_ns;
} mw_compact_result;

// Debug tracing: events are counted always (cheap atomic increments) and, if a
// sink is installed, also reported. The sink is called from any thread.
typedef enum {
    MW_EV_OPEN, MW_EV_CLOSE, MW_EV_READ, MW_EV_WRITE, MW_EV_SYNC, MW_EV_TRUNCATE,
    MW_EV_LOCK, MW_EV_UNLOCK, MW_EV_SHMMAP, MW_EV_SHMLOCK, MW_EV_SHMBARRIER,
    MW_EV_SHMUNMAP, MW_EV_FETCH, MW_EV_UNFETCH, MW_EV_DELETE, MW_EV_FILECONTROL,
    MW_EV_COUNT
} mw_event_t;

typedef void (*mw_trace_fn) (void *arg, mw_event_t ev, const char *file, int64_t a, int64_t b, int flags);

void        mw_vfs_set_trace (mw_trace_fn fn, void *arg);
uint64_t    mw_vfs_event_count (mw_event_t ev);
void        mw_vfs_events_reset (void);
const char *mw_event_name (mw_event_t ev);

#ifdef __cplusplus
}
#endif

#endif
