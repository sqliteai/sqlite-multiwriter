//
//  multiwriter_seglog.h
//  sqlite-multiwriter
//
//  Segmented commit log for the shared mode of multi-process Multi-Writer (mw_mp=2): "<db>-mw.<N>", N = 1, 2, ...
//
//  A segment is a file of fixed size (16 MB by default; a bigger commit gets a segment of its own), written once through a shared mapping and never rewritten:
//    header (64 B): same layout as the single-file log (magic, page size, base_epoch, salt, cksum)
//    records:        same layout as the single-file log (32 B header + npages x (4 + pgsz), checksummed)
//  When the active segment is full the writer starts the next one (prepared ahead, zero-filled, by whoever finishes a commit: see mw_seglog_prefill_bg); nothing is ever
//  copied, there is no rewrite and no process has to re-open anything: a process maps a segment the first time one of its readers needs a page that lives in it.
//  The index (multiwriter_shidx.h) points into the segments: loc = (segment << 32) | offset of the page image.
//
//  A segment is deleted when every record in it is <= the compaction base (its pages are in the real file). A reader that finds the segment of a version gone reads the
//  real file instead: a version <= base that a snapshot >= the GC floor can still ask for is, by the compaction rules (the target is never above the oldest snapshot),
//  the page the real file now holds.
//
#ifndef MULTIWRITER_SEGLOG_H
#define MULTIWRITER_SEGLOG_H

#include "multiwriter_internal.h"

#define MW_SEG_HDR 64
#define MW_SEG_MAPS 128

typedef struct mw_seglog mw_seglog;

#define MW_LOC(seg, off) (((uint64_t)(seg) << 32) | (uint64_t)(off))
#define MW_LOC_SEG(loc)  ((uint32_t)((loc) >> 32))
#define MW_LOC_OFF(loc)  ((uint32_t)(loc))

// Replay callback of recovery: one valid record newer than the base, in epoch order.
typedef int (*mw_seglog_replay_fn)(void *ctx, uint64_t epoch, uint32_t dbsize, int n, const uint32_t *pgnos, const uint64_t *locs, const uint8_t *ext, uint32_t ext_len, uint64_t ext_loc);

// Opens the log of `db` (db->path, the page size of db->store, the shared header db->shm): the first opener of the database recovers it (replaying what is newer than the
// base through `fn`, truncating a torn tail, removing leftovers) or creates it; the others only attach. *base_out: the compaction base found.
int  mw_seglog_open (mw_db *db, mw_seglog_replay_fn fn, void *ctx, uint64_t *base_out, uint64_t *last_epoch_out);
void mw_seglog_close (mw_db *db);

// ---- the writer (publication lock held) ----
// Appends one commit record (the pages, then the metadata extension), rolling to a new segment when it does not fit. locs[i] = where the image of page i is; *ext_loc = where the extension is. *seg / *end: where the record ends (for mw_seglog_sync).
// The writer cursor in the shared header moves only when the record is complete.
int  mw_seglog_append (mw_db *db, uint64_t epoch, uint32_t dbsize, int n, const uint32_t *pgnos, const uint8_t *const *images, const uint8_t *ext, uint32_t ext_len, const uint64_t *ch, uint64_t *locs, uint64_t *ext_loc, uint32_t *seg, uint64_t *end);
uint64_t mw_seglog_content_hash (const void *img, size_t pgsz);      // the hash of a page that the record's checksum is made of (never 0): can be made before the publication lock
// Group commit: returns when everything up to (seg, end) is durable (one leader per process syncs the active segment for everybody waiting).
int  mw_seglog_sync (mw_db *db, uint32_t seg, uint64_t end);
// Called outside the publication lock after a commit: keeps the next segment prepared. Cheap when there is nothing to do.
void mw_seglog_prefill_bg (mw_db *db);
// The published end of the log, (segment, offset): for the back-pressure check.
uint64_t mw_seglog_bytes (mw_db *db);                                   // bytes of live log (segments from the oldest one to the cursor)

// ---- readers (any thread, any process) ----
// Copies n bytes at `off` of the page image at `loc` into dst. false: the segment does not exist (any more).
bool mw_seglog_read (mw_db *db, uint64_t loc, uint32_t off, uint32_t n, void *dst);

// ---- what a dead publisher left ----
int  mw_seglog_peek (mw_db *db, uint32_t seg, uint64_t off, uint64_t *epoch, uint32_t *dbsize, int *n, uint32_t **pgnos, uint64_t **locs, uint32_t *ext_len, uint64_t *ext_loc, uint64_t *size);
void mw_seglog_discard (mw_db *db, uint32_t seg, uint64_t off);

// ---- maintenance (the compactor) ----
int  mw_seglog_set_base (mw_db *db, uint64_t base);                    // persists the base in the oldest segment's header
void mw_seglog_trim (mw_db *db, uint64_t base);                        // deletes the segments that hold nothing newer than base; unmaps the ones that are gone

#endif
