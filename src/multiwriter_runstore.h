//
//  multiwriter_runstore.h
//
//  The CRDT metadata of the database file as sorted runs (multiwriter_runs.h) kept in ordinary tables of the file itself:
//    mw_runs(run, age, lvl, nrows, nblk, dvmax, meta)   one row per run: its meta (fence keys, Bloom filter) is loaded into memory
//    mw_slots(slot, data), mw_free(slot)                the blocks of the runs (about 16 KB, compressed), in rows of a fixed size that are reused in place; the slots nobody uses
//    mw_drops(tbl, dv)                                  tables that were dropped: their cells older than dv are dead
//    mw_state: runs_ver (changes with every transaction that adds or removes runs), next_run, next_age
//  A flush writes one run for every chunk of rows; runs of one level are merged into the next one (every merge is a few ordinary transactions: the new parts first, the inputs removed last,
//  so a crash in between leaves the same rows twice, which does no harm). A reader finds a key in the newest run that has it.
//
#ifndef MW_RUNSTORE_H
#define MW_RUNSTORE_H

#include <stdint.h>
#include <stdbool.h>
#include "sqlite3.h"
#include "multiwriter_meta.h"
#include "multiwriter_runs.h"

typedef struct mw_rstore mw_rstore;
typedef struct mw_rman mw_rman;                        // a version of the list of runs (immutable, counted)

mw_rstore *rsx_new (size_t cache_bytes);
void rsx_set_exclusive (mw_rstore *s, bool on);   // this process is the only one that removes runs, and one thread merges: the merge reads its inputs without checking that they are still listed
void rsx_free (mw_rstore *s);

// ---- reading (the caller has a transaction open on `c`: the runs and the blocks must be of one snapshot) ----
int rsx_man (mw_rstore *s, sqlite3 *c, mw_rman **out);                     // the list of runs as of the snapshot of c; release with rsx_man_release
void rsx_man_release (mw_rman *m);
// n keys at once; cells[i] / ncells[i] as mw_meta_row (cells of dropped tables removed). blk: a prepared "SELECT data FROM mw_blocks WHERE run = ?1 AND blk = ?2" of c
int rsx_get_many (mw_rstore *s, mw_rman *m, sqlite3_stmt *blk, int n, const uint32_t *tbl, const uint8_t *const *pk, const size_t *pklen, mw_mcell **cells, int *ncells);
const char *rsx_blk_sql (void);
// every key that has a row (for the filter of known keys)
int rsx_scan_keys (mw_rstore *s, sqlite3 *c, void (*cb)(void *ctx, uint32_t tbl, const uint8_t *pk, size_t pklen), void *ctx);
// the keys of rows with a cell newer than `since` (possibly more than once: the same key in several runs)
int rsx_scan_since (mw_rstore *s, sqlite3 *c, mw_rman *m, int64_t since, void (*cb)(void *ctx, uint32_t tbl, const uint8_t *pk, size_t pklen), void *ctx);
// the current state of all rows, in key order (for the virtual table and for tests)
int rsx_scan_all (mw_rstore *s, sqlite3 *c, int (*cb)(void *ctx, uint32_t tbl, const uint8_t *pk, size_t pklen, const mw_mcell *cells, int n), void *ctx);

// ---- writing (the flusher; one thread at a time) ----
void rsx_wlock (mw_rstore *s);                                              // one write transaction on the runs at a time (a flush's, a merge's): held from BEGIN to COMMIT
void rsx_wunlock (mw_rstore *s);
#define RSX_NEED_SLOTS 0x7e5e01                       // returned by rsx_tx_add_items when the slots taken for it ran out: roll back, reserve more, try again
int rsx_reserve_items (mw_rstore *s, sqlite3 *c, const fitem *v, int i0, int i1);   // slots for a flush of the items, taken before its transaction starts (outside any transaction of c)
typedef struct rsx_tx rsx_tx;
rsx_tx *rsx_tx_begin (mw_rstore *s, sqlite3 *c);                           // in an open transaction of c; NULL: error
// a run of level 0 from the items [i0, i1) of a batch in key order
int rsx_tx_add_items (rsx_tx *t, sqlite3 *c, const fitem *v, int i0, int i1);
// the same in two steps: the blocks are built (any thread, no database), then written in a transaction (the same blocks again if the transaction is tried again)
typedef struct rsx_prebuilt rsx_prebuilt;
rsx_prebuilt *rsx_prebuild (const fitem *v, int i0, int i1);                // NULL: no items, or no memory
void rsx_prebuilt_free (rsx_prebuilt *pb);
int rsx_reserve_prebuilt (mw_rstore *s, sqlite3 *c, const rsx_prebuilt *pb);   // the slots it needs, before the transaction
int rsx_tx_add_prebuilt (rsx_tx *t, sqlite3 *c, rsx_prebuilt *pb);
int rsx_tx_drop_table (rsx_tx *t, sqlite3 *c, uint32_t tbl, int64_t dv);
int rsx_tx_finish (rsx_tx *t, sqlite3 *c);                                  // the version and counters into mw_state; call before COMMIT
void rsx_tx_end (rsx_tx *t, bool committed);                                // after COMMIT (or ROLLBACK): publishes the new list of runs, or forgets it; frees t
// one round of merging: the runs of the lowest level that has `fanout` or more age groups into the next level. Returns 1 if it merged, 0 if there was nothing to do, <0 an error.
void rsx_reset (mw_rstore *s);        // the cached list of runs, the blocks and the slots taken are forgotten (the file tables went back in time)
int rsx_merge (mw_rstore *s, sqlite3 *rd, sqlite3 *wr, sqlite3 *wr2, int fanout, uint64_t part_rows);   // wr2: a connection of its own for the thread that writes the finished parts (NULL: the merge writes them itself)

// how far the merges are behind: the age groups at level 0 and in all
void rsx_backlog (mw_rstore *s, int *l0, int *total);

int rsx_sweep (mw_rstore *s, sqlite3 *rd, sqlite3 *wr);   // gives back the slots that nobody has (those of dead processes); returns how many
int rsx_release_pool (mw_rstore *s, sqlite3 *c);            // at the close: the slots we hold become free again

typedef struct { uint64_t gets, run_probes, bloom_skips, blk_reads, cache_hits, merges, merged_rows, runs_written, merge_retries, swept_slots; int nruns; } rsx_stats;
void rsx_stats_get (mw_rstore *s, rsx_stats *out);

#endif
