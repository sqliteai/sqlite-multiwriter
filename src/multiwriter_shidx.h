//
//  multiwriter_shidx.h
//  sqlite-multiwriter
//
//  Shared version index for multi-process Multi-Writer (phase 1: the data structure on its own).
//
//  Why. Today every process of a multi-process database rebuilds, in private memory, the version chains of every page written by every other process (it applies every
//  commit of everybody): CPU per commit grows with the number of processes and so does memory (110-160 MB per process measured at 16-128 processes; 1000 processes would
//  need >100 GB). This index lives in one shared file mapped by all processes: a commit is installed *once*, by whoever publishes it, readers look pages up in it
//  (page number, snapshot epoch -> where the page image is) and nobody keeps a private copy of the versions. The page images themselves stay where they are (the log).
//
//  What it is. A directory of blocks of heads, page number -> newest version; a version is (epoch, loc, prev): an immutable record in a shared arena, newest first.
//  `loc` is opaque here (the log position of the page image). Page 0 is the database size: a version whose loc is the size in pages.
//
//  Concurrency. One writer at a time (the caller's publication lock; the index takes none). Readers take no lock at all: a version is fully written before the head
//  that points to it is published (release/acquire), and is never modified afterwards. A reader works at a snapshot epoch S <= committed(); it registers S in a slot
//  of the shared registry (shidx_pin) so that the garbage collector, which runs in the writer, can compute the oldest snapshot: it frees versions that no registered
//  snapshot can reach (everything older than the newest version <= the floor). The pin/floor handshake is the one used for compaction (announce, fence, re-scan).
//
//  Not here (later phases): the log (segments), recovery (the index is volatile: rebuilt from the log by the first opener), the liveness of the pids of the registry
//  (the caller reaps dead slots with shidx_reap), integration with mw_db.
//
#ifndef MULTIWRITER_SHIDX_H
#define MULTIWRITER_SHIDX_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

typedef struct shidx shidx;

typedef struct {
    uint32_t max_pages_log2;      // the index covers page numbers < 2^max_pages_log2 (default 24 = 64 GB of 4 KB pages); the directory costs 2^(n-12) x 4 bytes
    uint32_t max_entries;         // versions held at once (default 4M x 32 bytes of address space; the file is sparse)
    uint32_t nslots;              // snapshot registry size = most concurrent read transactions over all processes (default 4096)
    uint32_t max_cands;           // pages with several versions waiting for GC (default max_entries / 4; on overflow the next GC scans everything)
    uint32_t mode;                // the mode of the file when it is created (0: 0600)
} shidx_params;

typedef struct {
    uint64_t installs, versions_live, versions_freed, blocks, gc_runs, gc_full_scans, entries_cap, entries_used;
    uint64_t hazards;             // lookups that met a recycled entry: the reclamation protocol is broken if this is not 0
} shidx_stats;

// Opens (and creates) the index file. The first opener initialises it (flock); `params` only matter then, a later opener adopts what is in the file. NULL params = defaults.
// Returns NULL on error (errno is set).
shidx *shidx_open (const char *path, const shidx_params *params);
void   shidx_close (shidx *ix);
void   shidx_unlink (const char *path);

// ---- readers (any number, any process, no lock) ----
// Registry slots: one per concurrent read transaction. alloc returns -1 when full. The slot belongs to `pid`.
int      shidx_slot_alloc (shidx *ix, int32_t pid);
void     shidx_slot_free (shidx *ix, int slot);
// Takes a snapshot: the newest published epoch, registered in the slot (the GC will keep what it can see) and validated against the GC floor. Returns the epoch.
uint64_t shidx_pin (shidx *ix, int slot);
void     shidx_unpin (shidx *ix, int slot);
// The newest version of `pgno` with epoch <= snap. Returns true and the epoch and loc, or false: the page has no version in the index (it is in the real file).
// A caller must hold a pin with snap >= its slot's epoch (it is what keeps the version from being freed).
bool     shidx_lookup (shidx *ix, uint32_t pgno, uint64_t snap, uint64_t *epoch, uint64_t *loc);
// Database size in pages at `snap` (the newest size record <= snap); false if none (the real file's size).
bool     shidx_dbsize (shidx *ix, uint64_t snap, uint32_t *pages);
uint64_t shidx_committed (shidx *ix);

// ---- writer (one at a time, serialised by the caller) ----
// Installs the pages of one commit (and the database size) at `epoch` (must be committed()+1) but does not publish it yet. Returns 0, or -1 when the arena is full
// (run shidx_gc, or compact and raise the base) -- nothing was installed then.
int      shidx_install (shidx *ix, uint64_t epoch, uint32_t dbsize, int n, const uint32_t *pgnos, const uint64_t *locs);
void     shidx_publish (shidx *ix, uint64_t epoch);             // makes everything installed up to `epoch` visible to new snapshots
uint64_t shidx_head_epoch (shidx *ix, uint32_t pgno);           // newest version's epoch, 0 if none (commit validation)
// Frees what no snapshot can see. `base`: state <= base is in the real file, a chain whose only version is <= base is dropped altogether (and so is page `pgno`'s
// entry in the index). Returns the number of versions freed.
extern void (*shidx_gc_hook)(void);                  // called in the middle of a collection (tests: a crash point)
uint64_t shidx_gc (shidx *ix, uint64_t base);
// The same, with the floor decided by the caller (it runs its own registry of snapshots and guarantees that none below `floor` exists or can be taken).
uint64_t shidx_gc_floor (shidx *ix, uint64_t floor, uint64_t base);
// Free entries (versions that fit); `need` more than that: the caller must GC or compact first.
void shidx_gc_repair (shidx *ix);        // a publisher died inside the lock: a collection that was running is redone from every page
uint32_t shidx_room (shidx *ix);
// Lock-free scan (any process, while the writer works): for every page whose newest version <= `upto` is newer than `base`, calls cb(pgno, epoch, loc). The caller must keep the
// writer's GC from freeing versions <= upto (a floor that does not pass it) while it scans.
typedef void (*shidx_scan_fn)(void *ctx, uint32_t pgno, uint64_t epoch, uint64_t loc);
void     shidx_scan (shidx *ix, uint64_t base, uint64_t upto, shidx_scan_fn cb, void *ctx);
uint64_t shidx_oldest (shidx *ix);                              // the floor: oldest snapshot any reader can still hold
// Frees the slots whose owner `alive(pid)` says is gone (the caller knows how to tell). Returns how many.
int      shidx_reap (shidx *ix, bool (*alive)(int32_t pid, void *ctx), void *ctx);
void     shidx_stats_get (shidx *ix, shidx_stats *out);

#endif
