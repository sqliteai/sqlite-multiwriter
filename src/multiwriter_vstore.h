//
//  multiwriter_vstore.h
//  cloudsync
//
//  PROTOTYPE (docs §53): a persistent, log-structured store of per-cell CRDT versions, standing in for sqlite-sync's metadata tables. Key = (table, column, row key),
//  value = (column version, db_version). Writes go to an in-memory table; when it is full it is written as an immutable sorted run (24-byte entries, a Bloom filter
//  and a sparse block index kept in memory); runs of similar size are merged. A lookup asks the memory table, then the runs from the newest to the oldest, a Bloom
//  filter first, so a key that is not in a run costs no read. There is no write-ahead log of its own: the commit log already carries every change (the store is
//  rebuilt from it after a crash, from the last flushed commit). Not wired into the engine.
//
#ifndef MULTIWRITER_VSTORE_H
#define MULTIWRITER_VSTORE_H

#include <stdbool.h>
#include <stdint.h>

typedef struct { uint64_t hi, lo; } vs_key;           // hi = table << 32 | column, lo = the row's key (a hash of the primary key, or the rowid)
typedef struct { uint32_t cv, dv; } vs_val;           // column version, db_version

typedef struct {
    const char *dir;                 // runs are files <dir>/run.<id>
    uint64_t mem_entries;            // flush the memory table at this many entries
    int fanout;                      // merge when this many runs have the same size class (default 4)
    bool cold;                       // read blocks with pread and F_NOCACHE instead of the page cache (a pessimistic bound: every read goes to the drive)
} vs_params;

typedef struct {
    uint64_t gets, get_mem, get_runs, bloom_skips, bloom_false, block_reads;     // lookups, answered by the memory table, by a run; runs skipped by the filter; filter said maybe and the key was absent
    uint64_t puts, flushes, merges, bytes_flushed, bytes_merged;
    uint64_t runs, disk_bytes, resident_bytes;                                    // current number of runs, their size, memory held (memory table + filters + indexes)
    uint64_t flush_ns, merge_ns, bytes_feed;
} vs_stats;

typedef struct vstore vstore;
vstore *vstore_open (const vs_params *p);
void vstore_close (vstore *vs);
bool vstore_get (vstore *vs, vs_key k, vs_val *out);
void vstore_put (vstore *vs, vs_key k, vs_val v);          // flushes (and merges) by itself when the memory table is full
void vstore_flush (vstore *vs);
void vstore_stats (vstore *vs, vs_stats *st);
// Entries with db_version > `after` (what a peer that has seen up to `after` is missing). Every put is also appended to a change feed, which is written as one file
// per flush in the order of the puts (db_version never decreases), so this reads only the feed entries newer than `after` (a binary search per segment) and delivers each
// one that is still the current value of its key (one lookup each). Returns the number of entries delivered.
uint64_t vstore_feed_after (vstore *vs, uint32_t after, void (*cb) (void *, vs_key, vs_val), void *arg);
// Same result by scanning the sorted runs (no feed): reads every run that holds a version newer than `after`, whole.
uint64_t vstore_scan_after (vstore *vs, uint32_t after, void (*cb) (void *, vs_key, vs_val), void *arg);

#endif
