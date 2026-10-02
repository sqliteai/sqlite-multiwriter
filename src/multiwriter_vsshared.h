//
//  multiwriter_vsshared.h
//  cloudsync
//
//  PROTOTYPE (docs §54): the per-cell version store of multiwriter_vstore.c in a form that several processes share. Everything the store holds is either an
//  immutable file (sorted runs, their filters, change-feed segments: mapped read-only by every process) or lives in one shared mapping (the manifest of live runs and
//  two memory tables with their change feeds). One writer at a time (the caller holds the engine's publication lock around vsh_commit's puts: here a spin lock in the
//  mapping stands for it); any number of readers, lock-free: a lookup asks the active memory table, the one being flushed, then the runs newest first, and repeats
//  if the manifest changed under it (a sequence counter). A full memory table is handed to the process whose commit filled it, which writes it out *after* releasing
//  the lock; commits go on in the other table. Not wired into the engine.
//
#ifndef MULTIWRITER_VSSHARED_H
#define MULTIWRITER_VSSHARED_H

#include <stdbool.h>
#include <stdint.h>

typedef struct { uint64_t hi, lo; } vsh_key;           // hi = table << 32 | column, lo = the row's key
typedef struct { uint32_t cv, dv; } vsh_val;           // column version, db_version

typedef struct { const char *dir; uint64_t mem_entries; bool create; bool anon; } vsh_params;       // anon: the shared mapping is anonymous memory (no file, no write-back to the drive; only this process and its forks can attach)
typedef struct {
    uint64_t gets, get_mem, get_runs, retries, bloom_skips, block_reads;          // (this process)
    uint64_t commits, lock_wait_ns, stall_ns, stalls, flushes, flush_ns, merges, merge_ns, bytes_flushed, bytes_merged, bytes_feed;
    uint64_t runs, disk_bytes;                                                     // (shared state now)
} vsh_stats;

typedef struct vsh vsh;
vsh *vsh_open (const vsh_params *p);                    // create = true initialises the directory and the mapping (once); the others attach
void vsh_close (vsh *h);
void vsh_enable_bg_flush (vsh *h);                      // flushes (and merges) run in a thread of this process instead of in the committing call
bool vsh_get (vsh *h, vsh_key k, vsh_val *out);         // lock-free, any process
void vsh_lock (vsh *h);                                 // the writer lock (stands for the engine's publication lock)
void vsh_unlock (vsh *h);
// Puts n cells. The caller holds the writer lock (or, with `lock` set, vsh_commit takes and drops it itself) so that the puts of one commit appear together in the order
// of the commits. If the active memory table is full it is handed over and flushed by this process after the lock is released; if the other table is still being
// flushed, the call waits for it (a stall, counted) before it takes the lock.
void vsh_commit (vsh *h, const vsh_key *k, const vsh_val *v, int n);
bool vsh_commit_locked (vsh *h, const vsh_key *k, const vsh_val *v, int n);   // the lock is already held (lookup + put as one step); false = no room yet: unlock, vsh_wait_room(), start again. A flush it owes runs in vsh_unlock_flush
void vsh_wait_room (vsh *h);
void vsh_unlock_flush (vsh *h);
void vsh_flush_all (vsh *h);                            // everything in memory into runs (single caller)
uint64_t vsh_feed_after (vsh *h, uint32_t after, void (*cb) (void *, vsh_key, vsh_val), void *arg);
void vsh_stats_get (vsh *h, vsh_stats *s);

#endif
