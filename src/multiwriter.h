//
//  multiwriter.h
//  sqlite-multiwriter
//
//  Multi-Writer SQLite as a wrapper VFS.
//  See docs/multiwriter.md. Not part of the normal SQLite/PostgreSQL builds.
//

#ifndef MW_H
#define MW_H

#include <stddef.h>
#include <stdint.h>
#ifdef MW_LOADABLE                    // built as a loadable extension: every sqlite3_* call goes through the host's table of routines (sqlite3ext.h)
#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT3
#else
#include "sqlite3.h"
#endif

#ifdef __cplusplus
extern "C" {
#endif

// The version of the extension: the single place it is written (the Makefile, the packages and the release read it from here).
#define MW_VERSION "0.5.0"
#define MW_VERSION_MAJOR 0
#define MW_VERSION_MINOR 5
#define MW_VERSION_PATCH 0
#define MW_VERSION_NUMBER (MW_VERSION_MAJOR * 1000000 + MW_VERSION_MINOR * 1000 + MW_VERSION_PATCH)
const char *mw_version (void);                // "0.5.0"

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
    uint64_t    writer_id;          // the connection's lane
    uint64_t    snapshot_epoch;     // committed epoch this transaction reads from
    uint64_t    commit_epoch;       // epoch assigned at publication (0 while unpublished / read-only)
    uint32_t    schema_generation;
    uint32_t    commit_order;       // among the commits that share one epoch (a group replay of the rebase), the place of this one: they are equivalent to a serial execution in this order (0 for any other commit)
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
    uint64_t rebases, rebases_grouped, rebase_retries, rebase_max_attempts, rebase_ns, unrebasable, read_conflicts;   // rebases: commits saved by replaying their row changes at the latest snapshot
    uint64_t reloc_prep_used, reloc_prep_dropped, reloc_prep_rewrote;   // processes mode: phase 1 of a relocation made before the publication lock: used / thrown away (the plain publication succeeded, or it did not fit) / used with the references rewritten because the end of the file moved meanwhile
    uint64_t relocations;           // commits saved by renumbering their new pages after other commits extended the file
    uint64_t merges;                // interior b-tree pages rewritten by two transactions that were merged instead of refusing the second
    uint64_t reads_saved;           // read conflicts on interior pages avoided because the pages the transaction went through were routed unchanged
} mw_db_stats;

#define MW_FCNTL_DDL_RELEASE_IDLE 0x4d57000f   // internal: a statement ended; the schema barrier it raised is given back if no snapshot of the main file is open
#define MW_FCNTL_STMT 0x4d570011   // internal: mw_stmt_note * (a statement of a mw_rebase=1 connection starts or ends)
typedef struct { int ending; int autocommit; sqlite3_stmt *stmt; } mw_stmt_note;
#define MW_FCNTL_DDL_BEGIN 0x4d570009   // internal: a schema-changing statement is about to run (exclusive schema barrier)
#define MW_FCNTL_LANE_PTR 0x4d57000b   // internal: void ** (the connection's lane)
#define MW_FCNTL_GC             0x4d570004   // sqlite3_file_control(db, "main", MW_FCNTL_GC, uint64_t *reclaimed)

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
    MW_CRASH_SHARED_APPENDED,   // shared mode: the record is in the segment, nothing installed yet
    MW_CRASH_SHARED_INSTALLED,  // shared mode: pages installed, the commit not yet visible
    MW_CRASH_SHARED_GC,         // shared mode: in the middle of a collection of the index (the candidate list is half rebuilt)
    MW_FAULT_COUNT
} mw_fault_t;
void mw_fault_arm (mw_fault_t f, int nth);
void mw_fault_disarm_all (void);

// I/O fault injection (tests): the nth call, from now on, among the engine's own file calls of the given kinds (a mask of 1 write, 2 sync, 4 truncate, 8 rename, 16 mmap, 32 read) fails with err
// (EIO, ENOSPC...). sticky: every matching call after it fails too (a disk that stays full); shortw: a failing write of more than 512 bytes first writes about half of them (whole 512-byte blocks), and the disk is full from there on.
// mw_io_fault_calls() counts the matching calls since the arm (arm with a huge nth to count a workload's calls).
enum { MW_IO_WRITE = 1, MW_IO_SYNC = 2, MW_IO_TRUNC = 4, MW_IO_RENAME = 8, MW_IO_MAP = 16, MW_IO_READ = 32, MW_IO_ALL = 63 };
void mw_io_fault_arm (int kinds, long nth, int err, int sticky, int shortw);
void mw_io_fault_disarm (void);
long mw_io_fault_calls (void);

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
