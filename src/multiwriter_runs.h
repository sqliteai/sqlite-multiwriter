//
//  multiwriter_runs.h
//
//  Sorted runs of packed rows: the storage of the CRDT metadata in the database file. A run is immutable: rows in key order (table, then key bytes) in blocks of about 16 KB, with the first
//  key of every block and a Bloom filter kept in memory (the "meta" of the run). This module is the logic only (building, searching, merging); where the blocks live is the caller's business
//  (multiwriter_metafile.c keeps them as rows of an ordinary table, so the file stays one self-contained database).
//
#ifndef MW_RUNS_H
#define MW_RUNS_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdatomic.h>

#define RS_BLOCK_TARGET 16384

typedef struct { uint32_t tbl; const uint8_t *pk; uint32_t pklen; } rs_key;
int rs_key_cmp (const rs_key *a, const rs_key *b);                       // table, then the key bytes, then their length
uint64_t rs_key_hash (const rs_key *k);

// ---- a block ----
// raw form 3: [3][nrows u32][offsets u32 of every RS_RESTART-th row][rows]; a row: varint (shared << 1 | new table), the table if new, varint length of the rest of the key, those bytes (the first `shared` bytes
// are those of the row before: a key is stored as the difference from the previous one), varint dv (the difference from the previous row, zigzag), varint length of the cells, the cells (length 0: the row was
// deleted). A restart row has its whole key and its dv as they are: a row is found from the restart before it. (Form 1, read still: [1][nrows u32][offsets u32 x nrows], rows with the table, the whole key, dv.)
#define RS_RESTART 32
typedef struct {
    const uint8_t *data; size_t len; uint32_t nrows;
    int ver; uint32_t nrest; size_t rows_at;                                          // (form 3)
    uint8_t *kbuf; uint32_t kcap, klen, ktbl; int64_t kdv; const uint8_t *kcells; uint32_t knc;      // the row last decoded: its key is rebuilt in kbuf
    int64_t cur; size_t next_off; uint8_t kinl[96];
} rs_blk;
void rs_blk_close (rs_blk *b);                                                         // frees what the reading of rows took (call it when done with the block)
// A stored block is raw (format 1, as built) or compressed with LZ4 (format 2: [2][raw length u32][LZ4 data]); the builder compresses a block when it gains a tenth or more.
bool rs_blk_unpack (rs_blk *b, const uint8_t *data, size_t len, uint8_t **owned);                                      // a stored block opened; *owned: a buffer to free when done (NULL: b points into data)
bool rs_blk_open (rs_blk *b, const uint8_t *data, size_t len);                                                            // false: not a block (nothing is read past its end)
bool rs_blk_row (rs_blk *b, uint32_t i, rs_key *k, int64_t *dv, const uint8_t **cells, uint32_t *ncells);           // the i-th row
int  rs_blk_find (rs_blk *b, const rs_key *k, int64_t *dv, const uint8_t **cells, uint32_t *ncells);                // 1 found, 0 not, -1 corrupt

// ---- a run (what is kept in memory) ----
typedef struct rs_run {
    int64_t id, age; int lvl; uint64_t nrows; uint32_t nblk; int64_t dvmax;
    uint32_t nfence; uint32_t *ftbl, *foff, *flen; int64_t *fdv; uint8_t *farena;
    uint32_t *slen, *loff, *llen; uint8_t *larena;                                     // per block: its stored length and the locator the store gave it (opaque here), in larena     // first key of every block (table, offset and length in farena) and the largest dv in it
    uint8_t *kmaxk; uint32_t kmaxtbl, kmaxl;                                           // the last key of the run
    uint64_t *bloom; uint64_t nbits;
    uint8_t *mloc; uint32_t mlocl;                                                      // where the store keeps the meta of the run itself (opaque; set by the store)
    _Atomic int refs;
} rs_run;
rs_run *rs_run_decode (int64_t id, int64_t age, int lvl, uint64_t nrows, uint32_t nblk, int64_t dvmax, const uint8_t *meta, size_t len);   // NULL: not a meta
void rs_run_ref (rs_run *r);
void rs_run_unref (rs_run *r);
bool rs_run_maybe (const rs_run *r, const rs_key *k);                    // inside its key range and passes the filter
void rs_run_block_info (const rs_run *r, uint32_t blk, uint32_t *stored_len, const uint8_t **loc, uint32_t *loclen);   // where the store put the block
int  rs_run_block_of (const rs_run *r, const rs_key *k);                 // the block that would hold the key (-1: before the first)
bool rs_run_range (const rs_run *r, rs_key *lo, rs_key *hi);             // first and last key (pointers into the run)

// ---- building ----
// Rows are added in key order. A finished block goes to emit(); at the end the meta of the run comes out.
// emit stores the block and may return a locator for it (kept in the meta of the run, so that the store can find the block again; loc is read at once, before the next call)
typedef int (*rs_emit_fn)(void *ctx, uint32_t blkno, const uint8_t *data, size_t len, const uint8_t **loc, size_t *loclen);
typedef struct rs_builder rs_builder;
rs_builder *rs_builder_new (uint64_t nrows_hint, rs_emit_fn emit, void *ctx);
int  rs_builder_add (rs_builder *b, const rs_key *k, int64_t dv, const uint8_t *cells, uint32_t ncells);       // 0, or the error of emit / -1 memory
int  rs_builder_finish (rs_builder *b, uint8_t **meta, size_t *metalen, uint64_t *nrows, uint32_t *nblk, int64_t *dvmax);   // flushes the last block; the builder is freed
void rs_builder_free (rs_builder *b);
rs_builder *rs_builder_new_deferred (uint64_t nrows_hint);                  // no emit: the blocks are kept until the run is finished with the places they were given
int  rs_builder_seal (rs_builder *b);                                         // the last block is made
uint32_t rs_builder_nblocks (const rs_builder *b);
const uint8_t *rs_builder_block (const rs_builder *b, uint32_t i, size_t *len);
size_t rs_builder_meta_bound (const rs_builder *b);                          // the most the run's meta takes, once the builder is sealed
int  rs_builder_finish_locs (rs_builder *b, const uint8_t *const *locs, const uint32_t *llens, uint8_t **meta, size_t *metalen, uint64_t *nrows, uint32_t *nblk, int64_t *dvmax);      // the builder is not freed
uint64_t rs_builder_rows (const rs_builder *b);

// ---- merging ----
// The newest version of every key among the runs (rs[0] is the newest; equal ages never hold different rows of one key), written through a builder that is cut after `part_rows` rows.
typedef int (*rs_read_fn)(void *ctx, const rs_run *r, uint32_t blk, uint8_t **data, size_t *len);      // a malloc'ed copy of the block
typedef bool (*rs_keep_fn)(void *ctx, const rs_key *k, int64_t dv, const uint8_t *cells, uint32_t ncells);   // false: the row is dropped
typedef struct {
    rs_read_fn read; void *rctx;
    bool drop_deleted;                   // the output is the oldest data: a deleted row leaves nothing to hide
    rs_keep_fn keep; void *kctx;         // (optional) rows that are dead for another reason (a dropped table)
    uint64_t part_rows;                  // 0: one part
    // sink: a new part starts (ctx for its blocks is returned), a part ends with its meta
    int (*begin_part)(void *sctx, uint64_t rows_hint, rs_emit_fn *emit, void **emit_ctx);
    int (*end_part)(void *sctx, const uint8_t *meta, size_t metalen, uint64_t nrows, uint32_t nblk, int64_t dvmax);
    void *sctx;
} rs_merge_opts;
int rs_merge (rs_run *const *runs, int nruns, const rs_merge_opts *o, uint64_t *rows_out);

#endif
