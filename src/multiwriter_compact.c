//
//  multiwriter_compact.c
//  cloudsync
//
//  Compaction (checkpoint): materialises committed page versions into the real database file so it
//  becomes an ordinary stock SQLite database again. It never takes part in transaction publication:
//  writers only ever wait for it on the short store read/write locks.
//
//  Target epoch T = min(visible epoch, oldest active snapshot). Choosing T <= every live snapshot means
//  no reader can still need the old contents of a page that is overwritten, and any reader (existing or
//  future) can use either the store or the file for state <= T.
//
//  Crash-safe ordering:
//    1. write the newest version <= T of every page changed since the last base into the file, resize
//       the file to dbsize(T), fsync                                        [crash here: old base + log replay]
//    2. record base_epoch = T in the log header, fsync                      [crash here: skips records <= T]
//    3. publish compacted_epoch = T in the store (lets GC drop versions), reset the log if it holds no
//       record newer than T
//  Steps 1-2 are idempotent: re-running them after a crash rewrites the same bytes.
//

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include "multiwriter_internal.h"

static uint64_t now_ns (void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

int mw_db_compact (mw_db *db, mw_compact_result *out) {
    if (db->shared) return mw_shared_compact(db, out);
    mw_compact_result local;
    if (!out) out = &local;
    memset(out, 0, sizeof *out);
    mw_store *st = db->store;
    if (!st || atomic_load(&db->failed)) return SQLITE_OK;
    pthread_mutex_lock(&db->compact_mu);
    if (db->logfd < 0) { pthread_mutex_unlock(&db->compact_mu); return SQLITE_OK; }       // (read under compact_mu: the log is swapped only inside a compaction)
    uint64_t t0 = now_ns();
    int rc = SQLITE_OK;

    bool mp_locked = false;
    uint64_t T, base;
    if (db->mp) {                                                   // one compactor at a time across processes; target from the shared registry
        if (!mw_mp_compaction_lock(db)) goto done;
        mp_locked = true;
        mw_mp_catchup(db);
        uint64_t visible_mp = atomic_load(&db->epoch);
        T = mw_mp_compaction_target(db);
        if (T > visible_mp) T = visible_mp;
        base = atomic_load(&db->shm->base_epoch);
    } else {
        uint64_t visible = atomic_load(&db->epoch);
        uint64_t oldest = mw_db_oldest_active_snapshot(db);
        T = oldest < visible ? oldest : visible;
        base = db->base_epoch;
    }
    if (db->cdc) { uint64_t lim = mw_cdc_safe_epoch(db); if (T > lim) T = lim; }       // (the metadata of the commits above the last flush lives in the log records only)
    if (T <= base) goto done;                                      // nothing new to materialise (or pinned by an old reader)

    if (db->fd_real < 0) {
        db->fd_real = open(db->path, O_RDWR);
        if (db->fd_real < 0) { rc = SQLITE_CANTOPEN; goto done; }
    }
    size_t pgsz = (size_t)st->pgsz;
    uint8_t *page = malloc(pgsz);
    if (!page) { rc = SQLITE_NOMEM; goto done; }

    // 1. write pages: detach the dirty list, copy each page's image out under its stripe, write it without
    //    holding anything. The list is only re-pushed / cleared after the base was durably recorded, so an
    //    I/O error leaves every page marked dirty for the next attempt.
    mw_spinlock(&st->list_mu);
    uint32_t head = st->dirty_head;
    st->dirty_head = 0;
    pthread_mutex_unlock(&st->list_mu);
    uint32_t detached = head;                                      // (kept for step 3)
    for (uint32_t h = head; h && rc == SQLITE_OK; ) {
        uint32_t pgno = h - 1;
        bool have = false;
        pthread_mutex_t *mu = &st->stripes[pgno % MW_STRIPES].mu;
        mw_chain *c = mw_store_chain(st, pgno);
        mw_spinlock(mu);
        h = c->dirty_next;
        int k = -1;
        for (int lo = 0, hi = c->n - 1; lo <= hi; ) { int mid = (lo + hi) / 2; if (c->v[mid].epoch <= T) { k = mid; lo = mid + 1; } else hi = mid - 1; }
        if (k >= 0 && c->v[k].epoch > base) { memcpy(page, mw_pv_data(db->store, &c->v[k]), pgsz); have = true; }
        pthread_mutex_unlock(mu);
        if (!have) continue;
        ssize_t w = pwrite(db->fd_real, page, pgsz, (off_t)(pgno - 1) * (off_t)pgsz);
        if (w != (ssize_t)pgsz) rc = SQLITE_IOERR_WRITE;
        else out->pages_written++;
    }
    free(page);
    // File size at T from the in-header database size of page 1 as of T (exact: SQLite maintains it on every commit that
    // changes the size). The size *records* cannot be used here: in multi-process mode T can be older than this
    // process's oldest snapshot, and local GC may already have pruned the record <= T (a stale fallback truncated
    // the file below pages that were still needed). Unknown => leave the file size alone.
    uint32_t size_pages = 0;
    {
        mw_chain *c1 = mw_store_chain(st, 1);
        if (c1) {
            pthread_mutex_t *mu1 = &st->stripes[1 % MW_STRIPES].mu;
            mw_spinlock(mu1);
            int k1 = -1;
            for (int lo = 0, hi = c1->n - 1; lo <= hi; ) { int mid = (lo + hi) / 2; if (c1->v[mid].epoch <= T) { k1 = mid; lo = mid + 1; } else hi = mid - 1; }
            if (k1 >= 0) { const uint8_t *q = mw_pv_data(db->store, &c1->v[k1]) + 28; size_pages = ((uint32_t)q[0] << 24) | ((uint32_t)q[1] << 16) | ((uint32_t)q[2] << 8) | q[3]; }
            pthread_mutex_unlock(mu1);
        }
    }
    if (rc == SQLITE_OK && size_pages && ftruncate(db->fd_real, (off_t)size_pages * (off_t)pgsz) != 0) rc = SQLITE_IOERR_TRUNCATE;
    if (rc == SQLITE_OK && fsync(db->fd_real) != 0) rc = SQLITE_IOERR_FSYNC;
    if (rc == SQLITE_OK) mw_fault_hit(MW_CRASH_COMPACT_PAGES);

    // 2. durable new base
    if (rc == SQLITE_OK) { rc = mw_log_set_base(db, T); if (rc == SQLITE_OK) { if (db->mp) atomic_store(&db->shm->base_epoch, T); mw_fault_hit(MW_CRASH_COMPACT_BASE); } }

    // 3. settle the dirty list: clean pages drop out, pages with newer versions (or everything, on failure) are re-pushed
    for (uint32_t h = detached; h; ) {
        uint32_t pgno = h - 1;
        pthread_mutex_t *mu = &st->stripes[pgno % MW_STRIPES].mu;
        mw_chain *c = mw_store_chain(st, pgno);
        mw_spinlock(mu);
        h = c->dirty_next;
        if (rc != SQLITE_OK || (c->n > 0 && c->v[c->n - 1].epoch > T)) {
            mw_spinlock(&st->list_mu); c->dirty_next = st->dirty_head; st->dirty_head = pgno + 1; pthread_mutex_unlock(&st->list_mu);
        } else {
            c->dirty = 0;
            // now in the real file: a page written only once (an appended page) has a single version and never became a GC candidate
            // (candidates were queued when a 2nd version arrived): queue it, so the collector can drop the copy held in memory
            if (c->n > 0 && !c->queued) { c->queued = 1; mw_spinlock(&st->list_mu); c->cand_next = st->cand_head; st->cand_head = pgno + 1; pthread_mutex_unlock(&st->list_mu); }
        }
        pthread_mutex_unlock(mu);
    }
    if (rc != SQLITE_OK) goto done;

    //    tell the store: versions <= T are redundant once no snapshot needs them; reset the log when it holds
    //    nothing newer than T (epoch/offset assignment happens under seq_mu, so "nothing in flight" is exact)
    mw_spinlock(&st->seq_mu);
    st->compacted_epoch = T;
    if (size_pages) st->base_dbsize = size_pages;
    int drop = 0;                                                  // size records <= T are subsumed by base_dbsize
    while (drop < st->nsizes && st->sizes[drop].epoch <= T) drop++;
    if (drop) { memmove(st->sizes, st->sizes + drop, (size_t)(st->nsizes - drop) * sizeof(mw_sizerec)); st->nsizes -= drop; }
    if (db->mp) {
        /* handled after seq_mu is released (needs the publication lock) */
    } else if (atomic_load(&db->next_epoch) == T && atomic_load(&db->epoch) == T) {
        if (ftruncate(db->logfd, 64) == 0) { db->log_off = 64; mw_log_stage_reset(db, 64); mw_log_remap(db); }
    } else if (db->log_off > 8 * 4096) {
        mw_log_rewrite_tail(db, T);                                // busy: keep only the records newer than T
    }
    pthread_mutex_unlock(&st->seq_mu);
    if (db->mp) mw_mp_rewrite_log(db, T);
    out->target_epoch = T;
    out->versions_freed = mw_db_gc(db);
    atomic_fetch_add(&db->n_compactions, 1);
    atomic_fetch_add(&db->n_compacted_pages, out->pages_written);

done:
    if (mp_locked) mw_mp_compaction_unlock(db);
    out->duration_ns = now_ns() - t0;
    atomic_fetch_add(&db->n_compaction_ns, out->duration_ns);
    pthread_mutex_unlock(&db->compact_mu);
    return rc;
}

// MARK: - background compactor -
//
// One thread per database. It runs when the log outgrew log_max_bytes (publishers kick it) and, if
// mw_compact_ms > 0, periodically. It takes no lock that a publisher needs for longer than a copy of
// one page image, so it never becomes the writers' bottleneck.

static void *compactor_main (void *arg) {
    mw_db *db = arg;
    for (;;) {
        pthread_mutex_lock(&db->compactor_mu);
        if (!atomic_load(&db->compactor_stop)) {
            struct timespec until;
            clock_gettime(CLOCK_REALTIME, &until);
            int ms = db->compact_interval_ms > 0 ? db->compact_interval_ms : 1000;
            until.tv_sec += ms / 1000;
            until.tv_nsec += (long)(ms % 1000) * 1000000L;
            if (until.tv_nsec >= 1000000000L) { until.tv_sec++; until.tv_nsec -= 1000000000L; }
            pthread_cond_timedwait(&db->compactor_cv, &db->compactor_mu, &until);
        }
        pthread_mutex_unlock(&db->compactor_mu);
        if (atomic_load(&db->compactor_stop)) break;
        bool by_size = db->log_max_bytes && mw_log_end_locked(db) > mw_log_limit(db);
        if (db->compact_interval_ms > 0 || by_size) {
            if (db->shared && !mw_shared_compact_claim(db, by_size)) continue;       // another process has the next one
            mw_db_compact(db, NULL);
        }
    }
    return NULL;
}

void mw_db_compactor_kick (mw_db *db) {
    if (!db->compactor_running) return;
    pthread_mutex_lock(&db->compactor_mu);
    pthread_cond_signal(&db->compactor_cv);
    pthread_mutex_unlock(&db->compactor_mu);
}

void mw_db_compactor_start (mw_db *db, int interval_ms) {
    sqlite3_mutex_enter(db->mu);                                   // (two first opens at once must not both start a thread)
    if (db->compactor_running) { if (interval_ms > 0) db->compact_interval_ms = interval_ms; sqlite3_mutex_leave(db->mu); return; }
    db->compact_interval_ms = interval_ms;
    atomic_store(&db->compactor_stop, 0);
    if (pthread_create(&db->compactor, NULL, compactor_main, db) == 0) db->compactor_running = true;
    sqlite3_mutex_leave(db->mu);
}

void mw_db_compactor_stop (mw_db *db) {
    if (!db->compactor_running) return;
    atomic_store(&db->compactor_stop, 1);
    pthread_mutex_lock(&db->compactor_mu);
    pthread_cond_broadcast(&db->compactor_cv);
    pthread_mutex_unlock(&db->compactor_mu);
    pthread_join(db->compactor, NULL);
    db->compactor_running = false;
}
