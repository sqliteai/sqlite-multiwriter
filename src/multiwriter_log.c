//
//  multiwriter_log.c
//  cloudsync
//
//  Durable commit log ("<db>-mw"). Committed state = real database file + committed page versions;
//  the versions live in memory, so every commit is first appended here as one checksummed record:
//
//    header (64 B): LOG_MAGIC, version, page size, base_epoch, salt, cksum
//    record (32 B header + npages * (4 + pgsz)): magic, npages, epoch, dbsize, pgsz, cksum
//
//  Durability ordering (documented in docs/multiwriter.md):
//    1. the commit is validated and installed in the store *invisibly* (epoch not advanced) under the
//       store write lock, which also reserves the record's log offset (epoch order == offset order);
//    2. outside any lock the record is written; with synchronous>=FULL it is fsynced (group commit);
//    3. only then does the epoch become visible, and only after the previous epoch did.
//  A commit is therefore never visible before its record is persisted according to the requested
//  synchronous level. Recovery replays the longest prefix of valid, epoch-contiguous records past
//  base_epoch (the compacted epoch), ignoring a torn tail.
//
//  The log file is flock()ed for the lifetime of the database: a second Multi-Writer process opening
//  the same database is refused (multi-process is a separate milestone).
//

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/uio.h>
#include <sched.h>
#include <sys/stat.h>
#include <unistd.h>
#include "multiwriter_internal.h"
#include "multiwriter_seglog.h"

#define LOG_MAP_BYTES (1ull << 30)         /* virtual size of the mapping */
#define LOG_GROW_BYTES (32ull << 20)
#define LOG_MAGIC "MWLOG002"                /* one constant for create, rewrite and recovery */
#define LOG_HDR_SIZE 64
#define REC_HDR_SIZE 32
#define REC_MAGIC    0x3143574du      /* "MWC1" */

typedef struct {
    char     magic[8];
    uint32_t version, pgsz;
    uint64_t base_epoch, salt;
    uint64_t reserved[3];
    uint64_t cksum;
} log_hdr;

typedef struct {
    uint32_t magic, npages;
    uint64_t epoch;
    uint32_t dbsize, pgsz;
    uint64_t cksum;
} rec_hdr;

_Static_assert(sizeof(log_hdr) == LOG_HDR_SIZE, "log header layout");
_Static_assert(sizeof(rec_hdr) == REC_HDR_SIZE, "record header layout");

// MARK: - fault injection -

static _Atomic int fault_left[MW_FAULT_COUNT];

void mw_fault_arm (mw_fault_t f, int nth) { if (f > 0 && f < MW_FAULT_COUNT) atomic_store(&fault_left[f], nth); }
void mw_fault_disarm_all (void) { for (int i = 0; i < MW_FAULT_COUNT; i++) atomic_store(&fault_left[i], 0); }

static bool fault_fires (mw_fault_t f) {
    int left = atomic_load_explicit(&fault_left[f], memory_order_relaxed);
    if (left <= 0) return false;                       // (fast path: nothing armed)
    return atomic_fetch_sub(&fault_left[f], 1) == 1;
}

bool mw_fault_hit (mw_fault_t f) {
    if (!fault_fires(f)) return false;
    if (f >= MW_CRASH_MID_LOG) _exit(9);               // crash point: die right here, no cleanup
    return true;
}

// MARK: - checksums and I/O helpers -

// 64-bit word-at-a-time multiply/xor mixer (about 10x faster than byte-wise FNV; profiling showed the log
// checksum was the largest single CPU cost of a small commit). Not cryptographic: it only has to detect
// torn or stale records.
static uint64_t fnv64 (uint64_t h, const void *p, size_t n) {
    const unsigned char *b = p;
    while (n >= 8) {
        uint64_t w;
        memcpy(&w, b, 8);
        h ^= w;
        h *= 0x9E3779B97F4A7C15ull;
        h ^= h >> 32;
        b += 8; n -= 8;
    }
    while (n--) { h ^= *b++; h *= 1099511628211ull; }
    return h;
}

static int pwrite_all (int fd, const void *buf, size_t n, off_t off) {
    const char *p = buf;
    while (n > 0) {
        ssize_t w = pwrite(fd, p, n, off);
        if (w < 0) { if (errno == EINTR) continue; return SQLITE_IOERR_WRITE; }
        p += w; off += w; n -= (size_t)w;
    }
    return SQLITE_OK;
}

static int pread_all (int fd, void *buf, size_t n, off_t off) {
    char *p = buf;
    while (n > 0) {
        ssize_t r = pread(fd, p, n, off);
        if (r < 0) { if (errno == EINTR) continue; return SQLITE_IOERR_READ; }
        if (r == 0) return SQLITE_IOERR_SHORT_READ;
        p += r; off += r; n -= (size_t)r;
    }
    return SQLITE_OK;
}

static uint64_t hdr_cksum (const log_hdr *h) {
    log_hdr c = *h;
    c.cksum = 0;
    return fnv64(1469598103934665603ull, &c, sizeof c);
}

static int hdr_write (mw_db *db, uint32_t pgsz, uint64_t base, uint64_t salt) {
    log_hdr h;
    memset(&h, 0, sizeof h);
    memcpy(h.magic, LOG_MAGIC, 8);
    h.version = 1; h.pgsz = pgsz; h.base_epoch = base; h.salt = salt;
    h.cksum = hdr_cksum(&h);
    int rc = pwrite_all(db->logfd, &h, sizeof h, 0);
    if (rc == SQLITE_OK && fsync(db->logfd) != 0) rc = SQLITE_IOERR_FSYNC;
    return rc;
}


static uint64_t rec_cksum (uint64_t salt, const rec_hdr *h, const void *body, size_t body_len) {
    rec_hdr c = *h;
    c.cksum = 0;
    uint64_t x = fnv64(salt ^ 1469598103934665603ull, &c, sizeof c);
    return fnv64(x, body, body_len);
}

uint64_t mw_log_record_size (mw_db *db, int n) {
    return REC_HDR_SIZE + (uint64_t)n * (4 + (uint64_t)db->store->pgsz);
}

// MARK: - open / recovery -

int mw_log_open (mw_db *db, int pgsz) {
    db->logpath = sqlite3_mprintf("%s-mw", db->path);
    if (!db->logpath) return SQLITE_NOMEM;
    db->logfd = open(db->logpath, O_RDWR | O_CREAT, 0644);
    db->has_log = db->logfd >= 0;
    if (db->logfd < 0) return SQLITE_CANTOPEN;
    // single-process mode owns the log exclusively; multi-process mode shares it (a non-mp opener asking for EX is refused)
    if (flock(db->logfd, (db->mp_req ? LOCK_SH : LOCK_EX) | LOCK_NB) != 0) {          // another process owns this database
        close(db->logfd); db->logfd = -1; db->has_log = false;
        return SQLITE_BUSY;
    }
    struct stat sb;
    if (fstat(db->logfd, &sb) != 0) return SQLITE_IOERR;

    log_hdr h;
    bool valid = sb.st_size >= LOG_HDR_SIZE && pread_all(db->logfd, &h, sizeof h, 0) == SQLITE_OK &&
                 memcmp(h.magic, LOG_MAGIC, 8) == 0 && h.cksum == hdr_cksum(&h) && h.pgsz == (uint32_t)pgsz;
    uint64_t base = 1;
    if (!valid) {                                            // new (or unusable) log: start empty from the real file
        sqlite3_randomness(sizeof db->log_salt, &db->log_salt);
        if (ftruncate(db->logfd, 0) != 0) return SQLITE_IOERR;
        int rc = hdr_write(db, (uint32_t)pgsz, base, db->log_salt);
        if (rc != SQLITE_OK) return rc;
        db->log_off = LOG_HDR_SIZE;
        mw_log_stage_reset(db, db->log_off);
    } else {
        base = h.base_epoch;
        db->log_salt = h.salt;
    }
    db->base_epoch = base;
    atomic_store(&db->epoch, base);
    atomic_store(&db->next_epoch, base);
    db->written_upto = db->synced_upto = base;
    if (!valid) { pthread_mutex_lock(&db->store->seq_mu); mw_log_remap(db); pthread_mutex_unlock(&db->store->seq_mu); return SQLITE_OK; }

    // ---- recovery: replay the valid, contiguous prefix of records
    mw_store *st = db->store;
    off_t off = LOG_HDR_SIZE;
    uint64_t last = base, prev_epoch = 0;
    uint64_t limit = (db->mp && !db->mp_first) ? MW_LOG_END(atomic_load(&db->shm->log_pos)) : UINT64_MAX;   // a later process replays exactly what is committed
    size_t rec_cap = 0;
    uint8_t *buf = NULL;
    int rc = SQLITE_OK;
    struct stat fsb;
    uint64_t fsz = fstat(db->logfd, &fsb) == 0 ? (uint64_t)fsb.st_size : UINT64_MAX;
    for (;;) {
        rec_hdr r;
        if ((uint64_t)off >= limit) break;
        if (pread_all(db->logfd, &r, sizeof r, off) != SQLITE_OK) break;
        if (r.magic != REC_MAGIC || r.pgsz != (uint32_t)pgsz || r.npages == 0 || r.npages > (1u << 24)) break;
        size_t body = (size_t)r.npages * (4 + (size_t)pgsz);
        if ((uint64_t)off + REC_HDR_SIZE + body > fsz) break;                          // a torn / garbage header must not size an allocation
        if (body > rec_cap) {
            uint8_t *nb = realloc(buf, body);
            if (!nb) { rc = SQLITE_NOMEM; break; }
            buf = nb; rec_cap = body;
        }
        if (pread_all(db->logfd, buf, body, off + REC_HDR_SIZE) != SQLITE_OK) break;
        if (r.cksum != rec_cksum(db->log_salt, &r, buf, body)) break;                       // torn / stale
        if (prev_epoch && r.epoch != prev_epoch + 1) break;                             // gap: stop at the prefix
        prev_epoch = r.epoch;
        if (r.epoch > base) {
            if (r.epoch != last + 1) break;                                             // must continue right after the base
            uint32_t *pgnos = malloc((size_t)r.npages * sizeof(uint32_t));
            const uint8_t **imgs = malloc((size_t)r.npages * sizeof(uint8_t *));
            if (!pgnos || !imgs) { free(pgnos); free(imgs); rc = SQLITE_NOMEM; break; }
            for (uint32_t i = 0; i < r.npages; i++) {
                const uint8_t *e = buf + (size_t)i * (4 + (size_t)pgsz);
                memcpy(&pgnos[i], e, 4);
                imgs[i] = e + 4;
            }
            rc = mw_store_install_recovered(st, r.epoch, r.dbsize, (int)r.npages, pgnos, imgs);
            if (rc == SQLITE_OK) st->sizes[st->nsizes - 1].log_off = (uint64_t)off;
            free(pgnos); free(imgs);
            if (rc != SQLITE_OK) break;
            last = r.epoch;
        }
        off += (off_t)REC_HDR_SIZE + (off_t)body;
    }
    free(buf);
    if (rc != SQLITE_OK) return rc;
    if (limit == UINT64_MAX && sb.st_size > off && ftruncate(db->logfd, off) != 0) return SQLITE_IOERR;       // drop the torn tail (only the first process)
    db->log_off = (uint64_t)off;
    mw_log_stage_reset(db, db->log_off);
    pthread_mutex_lock(&st->seq_mu); mw_log_remap(db); pthread_mutex_unlock(&st->seq_mu);
    atomic_store(&db->epoch, last);
    atomic_store(&db->next_epoch, last);
    db->written_upto = db->synced_upto = last;
    return SQLITE_OK;
}

// The mapped log is replaced or unmapped only through these two: lazy store versions (multi-process catch-up) point into the mapping, so they are copied out first,
// and the store is told where the current mapping is.
static void log_unmap (mw_db *db, bool materialize) {
    if (!db->logmap) return;
    if (materialize && db->store) mw_store_materialize_lazy(db->store);
    munmap(db->logmap, LOG_MAP_BYTES);
    db->logmap = NULL;
    if (db->store) atomic_store(&db->store->lazy_base, NULL);
}
static void log_mapped (mw_db *db, void *m) {
    db->logmap = m == MAP_FAILED ? NULL : m;
    if (db->store) atomic_store(&db->store->lazy_base, db->logmap);
}

void mw_log_close (mw_db *db, bool remove_file) {
    log_unmap(db, false);                                                // (the store is about to be freed: nothing to materialise)
    if (db->logfd >= 0) { flock(db->logfd, LOCK_UN); close(db->logfd); db->logfd = -1; }
    if (remove_file && db->logpath) unlink(db->logpath);
    sqlite3_free(db->logpath);
    db->logpath = NULL;
    free(db->written_pending);
    db->written_pending = NULL;
    if (db->stage_buf) { munmap(db->stage_buf, (size_t)db->stage_r); db->stage_buf = NULL; }
}

// MARK: - mapped log -

// The log is written through a MAP_SHARED mapping: appending a record is a memcpy (a process kill cannot lose it,
// the kernel owns the pages), which avoids a write syscall per commit -- profiling showed pwrite was the largest
// cost of a small commit and serialises on the file inode when many threads commit. The file is grown ahead of
// the append offset (sparse); records are validated by checksum at recovery, so the unwritten tail is harmless.
// Multi-process: a record may only go where the file was written ahead (shm->log_ready); the size the file has while somebody is still zero-filling it does not count.
static uint64_t log_usable (mw_db *db, uint64_t st_size) {
    if (!db->mp || !db->shm) return st_size;
    uint64_t r = atomic_load_explicit(&db->shm->log_ready, memory_order_acquire);
    return r && r < st_size ? r : st_size;
}

void mw_log_remap (mw_db *db) {
    db->logsync_off = 0; db->logsync_low1 = 0;
    log_unmap(db, true);
    struct stat sb;
    db->logfile_size = fstat(db->logfd, &sb) == 0 ? log_usable(db, (uint64_t)sb.st_size) : 0;
    void *m = mmap(NULL, LOG_MAP_BYTES, PROT_READ | PROT_WRITE, MAP_SHARED, db->logfd, 0);
    log_mapped(db, m);
    mw_log_reserve_space(db);
}

// Readers (other processes' commits): map what exists, never grow or shrink the file.
void mw_log_remap_ro (mw_db *db, uint64_t need_end) {
    if (db->logmap && need_end <= db->logfile_size) return;
    struct stat sb;
    db->logfile_size = fstat(db->logfd, &sb) == 0 ? log_usable(db, (uint64_t)sb.st_size) : 0;
    if (db->logmap) return;                          // (the mapping always spans LOG_MAP_BYTES: only the file size changed, and other threads may be using the mapping)
    void *m = mmap(NULL, LOG_MAP_BYTES, PROT_READ | PROT_WRITE, MAP_SHARED, db->logfd, 0);
    log_mapped(db, m);
}

// Installs the epoch-contiguous record at `off` of the mapped log into the local store. Records below log_end were
// published by a process that finished writing them (the header is updated last, inside the publication lock), so
// they are complete by construction: no checksum on this hot path (recovery of an unclean log still verifies).
int mw_log_apply_at (mw_db *db, uint64_t off, uint64_t *out_size, uint64_t *out_epoch) {
    static _Atomic int dbg_c = MW_KNOB_UNSET;
    const bool dbg = mw_knob_flag(&dbg_c, "MW_DEBUG");
    if (!db->logmap || off + REC_HDR_SIZE > db->logfile_size) return SQLITE_IOERR_SHORT_READ;
    const uint8_t *base = db->logmap + off;
    rec_hdr r;
    memcpy(&r, base, sizeof r);
    size_t pgsz = (size_t)db->store->pgsz;
    if (r.magic != REC_MAGIC || r.pgsz != (uint32_t)pgsz || r.npages == 0 || r.npages > (1u << 24)) { if (dbg) fprintf(stderr, "apply_at: bad header at %llu (magic %x npages %u)\n", (unsigned long long)off, r.magic, r.npages); return SQLITE_CORRUPT; }
    size_t body = (size_t)r.npages * (4 + pgsz);
    if (off + REC_HDR_SIZE + body > db->logfile_size) { if (dbg) fprintf(stderr, "apply_at: record at %llu beyond mapped size %llu\n", (unsigned long long)off, (unsigned long long)db->logfile_size); return SQLITE_IOERR_SHORT_READ; }
    if (r.epoch != atomic_load(&db->epoch) + 1) { if (dbg) fprintf(stderr, "apply_at: epoch %llu after %llu\n", (unsigned long long)r.epoch, (unsigned long long)atomic_load(&db->epoch)); return SQLITE_CORRUPT; }
    uint32_t pg_small[16]; const uint8_t *im_small[16];
    uint32_t *pgnos = r.npages <= 16 ? pg_small : malloc((size_t)r.npages * sizeof(uint32_t));
    const uint8_t **imgs = r.npages <= 16 ? im_small : malloc((size_t)r.npages * sizeof(uint8_t *));
    if (!pgnos || !imgs) { if (r.npages > 16) { free(pgnos); free((void *)imgs); } return SQLITE_NOMEM; }
    for (uint32_t i = 0; i < r.npages; i++) {
        const uint8_t *e = base + REC_HDR_SIZE + (size_t)i * (4 + pgsz);
        memcpy(&pgnos[i], e, 4);
        imgs[i] = e + 4;
    }
    int rc;
    if (db->mp && db->mp_lazy) {                                      // multi-process catch-up: point at the images in the mapping instead of copying them
        uint64_t off_small[16]; uint64_t *offs = r.npages <= 16 ? off_small : malloc((size_t)r.npages * sizeof(uint64_t));
        if (!offs) { if (r.npages > 16) { free(pgnos); free((void *)imgs); } return SQLITE_NOMEM; }
        for (uint32_t i = 0; i < r.npages; i++) offs[i] = off + REC_HDR_SIZE + (uint64_t)i * (4 + pgsz) + 4;
        rc = mw_store_install_lazy(db->store, r.epoch, r.dbsize, (int)r.npages, pgnos, offs);
        if (r.npages > 16) free(offs);
    } else rc = mw_store_install_recovered(db->store, r.epoch, r.dbsize, (int)r.npages, pgnos, imgs);
    if (rc == SQLITE_OK) db->store->sizes[db->store->nsizes - 1].log_off = off;
    if (r.npages > 16) { free(pgnos); free((void *)imgs); }
    *out_size = REC_HDR_SIZE + body;
    *out_epoch = r.epoch;
    return rc;
}

// Multi-process mapped log: the file is extended with *written zeros*, not with ftruncate. Appending a record through the mapping into a hole makes the file system
// allocate blocks while another process's fsync of the same file is running, and the append then stalls on it (measured in isolation, 30 KB record: 40-65 us with a
// concurrent fsync into a sparse file, 3-8 us into a file whose blocks were written, and the same 6-10 us without any fsync). shm->log_ready is the written extent:
// records go below it, the prefiller (one process at a time, claimed in shm->log_fill_pid, outside the publication lock) writes zeros above it and then raises it.
#define LOG_PREFILL_AHEAD (3ull << 20)
#define LOG_PREFILL_CHUNK (1ull << 20)
static uint8_t *prefill_zeros (void) {
    static uint8_t *z;
    if (!z) z = calloc(1, (size_t)LOG_PREFILL_CHUNK);
    return z;
}
// Writes one chunk above log_ready. The caller owns shm->log_fill_pid.
static void prefill_chunk (mw_db *db) {
    mw_shm *sh = db->shm;
    uint64_t ready = atomic_load(&sh->log_ready);
    uint8_t *z = prefill_zeros();
    if (!z || ready + LOG_PREFILL_CHUNK > LOG_MAP_BYTES) return;
    if (pwrite_all(db->logfd, z, (size_t)LOG_PREFILL_CHUNK, (off_t)ready) == SQLITE_OK) atomic_store_explicit(&sh->log_ready, ready + LOG_PREFILL_CHUNK, memory_order_release);
}
static bool prefill_claim (mw_db *db, bool wait) {
    int32_t me = (int32_t)getpid();
    for (unsigned spin = 0; ; spin++) {
        int32_t exp = 0;
        if (atomic_compare_exchange_strong(&db->shm->log_fill_pid, &exp, me)) return true;
        if (!wait) return false;
        if ((spin & 63) == 63 && exp != me && !mw_mp_pid_alive(db, exp) && atomic_compare_exchange_strong(&db->shm->log_fill_pid, &exp, me)) return true;   // the filler died
        struct timespec ts = { 0, 50000 };
        nanosleep(&ts, NULL);
    }
}
static void log_prefill (mw_db *db) {                                      // (under the publication lock)
    mw_shm *sh = db->shm;
    uint64_t need = db->log_off + LOG_PREFILL_AHEAD / 3;                      // the space this record needs, plus a margin for the next ones
    uint64_t ready = atomic_load_explicit(&sh->log_ready, memory_order_acquire);
    if (ready < need) {                                                       // the background filler is behind (or nobody filled yet): do it here
        prefill_claim(db, true);
        while (atomic_load(&sh->log_ready) < need) { uint64_t before = atomic_load(&sh->log_ready); prefill_chunk(db); if (atomic_load(&sh->log_ready) == before) break; }
        atomic_store(&sh->log_fill_pid, 0);
        ready = atomic_load_explicit(&sh->log_ready, memory_order_acquire);
    }
    struct stat cur;
    uint64_t size = fstat(db->logfd, &cur) == 0 ? (uint64_t)cur.st_size : db->logfile_size;
    db->logfile_size = ready < size ? ready : size;
}
void mw_log_prefill_bg (mw_db *db) {
    if (db->shared) { mw_seglog_prefill_bg(db); return; }
    mw_shm *sh = db->shm;
    if (!db->mp || !sh || !db->logmap) return;
    uint64_t end = MW_LOG_END(atomic_load_explicit(&sh->log_pos, memory_order_relaxed));
    if (end + LOG_PREFILL_AHEAD <= atomic_load_explicit(&sh->log_ready, memory_order_relaxed)) return;
    if (!prefill_claim(db, false)) return;
    // (the generation only changes while the claim is held, see mw_log_fill_hold: if ours is current, the file we write to is the one log_ready describes)
    if (MW_LOG_GEN(atomic_load(&sh->log_pos)) == db->mp_gen && end + LOG_PREFILL_AHEAD > atomic_load(&sh->log_ready)) prefill_chunk(db);
    atomic_store(&sh->log_fill_pid, 0);
}
// The log is about to be replaced by another file: no prefiller may be writing into the old one with the new file's extent.
void mw_log_fill_hold (mw_db *db) { prefill_claim(db, true); }
void mw_log_fill_release (mw_db *db) { atomic_store(&db->shm->log_fill_pid, 0); }

void mw_log_reserve_space (mw_db *db) {
    // Only the mapped log grows ahead of time. A staged log grows by append: extending a file with ftruncate and then writing into the hole makes the file system
    // allocate blocks inside every write (measured: group sync 70-290 us against 40-50 us when the file simply grows, 2 writers 5-11k against 17k tx/s).
    if (!db->logmap || atomic_load(&db->log_mode) != 1) return;
    if (db->mp) { log_prefill(db); return; }
    uint64_t need = db->log_off + LOG_GROW_BYTES / 4;
    if (need <= db->logfile_size) return;
    struct stat cur;
    if (fstat(db->logfd, &cur) == 0 && (uint64_t)cur.st_size >= need) { db->logfile_size = (uint64_t)cur.st_size; return; }     // someone else already grew it
    uint64_t size = ((need + LOG_GROW_BYTES - 1) / LOG_GROW_BYTES) * LOG_GROW_BYTES;
    if (size > LOG_MAP_BYTES || ftruncate(db->logfd, (off_t)size) != 0) return;     // cannot map further: records past logfile_size are pwritten (the mapping stays: appenders may be using it)
    db->logfile_size = size;
}

// Excludes the group-commit leader (which syncs a mapping and an fd it copied) while the log's file and mapping are replaced.
static void sync_quiesce (mw_db *db) {
    pthread_mutex_lock(&db->log_mu);
    while (db->sync_running || db->stage_writing || db->stage_fsyncing) pthread_cond_wait(&db->sync_cv, &db->log_mu);
    db->sync_running = true;
    pthread_mutex_unlock(&db->log_mu);
}
static void sync_resume (mw_db *db) {
    pthread_mutex_lock(&db->log_mu);
    db->sync_running = false;
    pthread_cond_broadcast(&db->sync_cv);
    pthread_mutex_unlock(&db->log_mu);
}


// MARK: - staged (leader-batched) log writes -
//
// Single process, synchronous >= FULL. Every commit owns a byte range of the log file (assigned under seq_mu). Instead of writing its record into the
// mapped file (page faults on fresh pages, a msync per group), the committer copies it into a staging ring at (file offset mod ring size). When it is
// done, the record joins the *contiguous prefix* of finished records (written_end). The group-commit leader writes that whole prefix with one pwritev and
// one fsync, then every committer whose record is inside it is released. Ring space is reusable once the bytes are in the file (flushed_off), so
// a committer waits for space only when the ring is full (a record is at most a quarter of it). Recovery needs nothing new: the file holds exactly the
// records the mapped path would have written, each with its checksum, and a torn write of the last group is ignored like a torn record.

// Streaming version of fnv64 for data that arrives in pieces whose sizes are not multiples of 8 (a 4-byte page number, then a page): it gives the same
// value as fnv64 over the concatenation, so the on-disk checksum does not change.
typedef struct { uint64_t h; uint8_t tail[8]; int tn; } ckstream;
static inline void ck_word (ckstream *c, uint64_t w) { c->h ^= w; c->h *= 0x9E3779B97F4A7C15ull; c->h ^= c->h >> 32; }
static void ck_feed (ckstream *c, const void *p, size_t n) {
    const unsigned char *b = p;
    if (c->tn) {
        while (n && c->tn < 8) { c->tail[c->tn++] = *b++; n--; }
        if (c->tn < 8) return;
        uint64_t w; memcpy(&w, c->tail, 8); ck_word(c, w); c->tn = 0;
    }
    while (n >= 8) { uint64_t w; memcpy(&w, b, 8); ck_word(c, w); b += 8; n -= 8; }
    while (n) { c->tail[c->tn++] = *b++; n--; }
}
static uint64_t ck_final (ckstream *c) { for (int i = 0; i < c->tn; i++) { c->h ^= c->tail[i]; c->h *= 1099511628211ull; } return c->h; }

static bool stage_alloc (mw_db *db) {
    if (db->stage_buf) return true;
    uint64_t R = 8ull << 20;
    void *m = mmap(NULL, (size_t)R, PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE, -1, 0);
    if (m == MAP_FAILED) return false;
    db->stage_buf = m; db->stage_r = R;
    return true;
}

// Decides once per database how records are written (before the first one): staged when the first commit asks for FULL durability.
static bool staged_mode (mw_db *db, int sync) {
    int m = atomic_load_explicit(&db->log_mode, memory_order_acquire);
    if (m == 0) {
        pthread_mutex_lock(&db->log_mu);
        m = atomic_load(&db->log_mode);
        if (m == 0) {
            bool on = !db->mp && db->logfd >= 0 && !getenv("MW_LOG_STAGING_OFF") && stage_alloc(db);      // (every sync level; MW_LOG_STAGING_OFF selects the mapped log)
            m = on ? 2 : 1;
            atomic_store_explicit(&db->log_mode, m, memory_order_release);
        }
        pthread_mutex_unlock(&db->log_mu);
    }
    return m == 2;
}

void mw_log_decide_mode (mw_db *db, int sync) { (void)staged_mode(db, sync); }

static void ring_copy (mw_db *db, uint64_t file_off, const void *src, size_t len) {
    uint64_t R = db->stage_r, pos = file_off & (R - 1);
    size_t first = len < R - pos ? len : (size_t)(R - pos);
    memcpy(db->stage_buf + pos, src, first);
    if (len > first) memcpy(db->stage_buf, (const uint8_t *)src + first, len - first);
}

// Writes ring bytes [from, from+len) (file offsets) to the file.
static int ring_write_file (mw_db *db, uint64_t from, uint64_t len) {
    while (len) {
        uint64_t R = db->stage_r, pos = from & (R - 1);
        size_t first = len < R - pos ? (size_t)len : (size_t)(R - pos);
        struct iovec iov[2] = { { db->stage_buf + pos, first }, { db->stage_buf, (size_t)len - first } };
        ssize_t w = pwritev(db->logfd, iov, len > first ? 2 : 1, (off_t)from);
        if (w < 0) { if (errno == EINTR) continue; return SQLITE_IOERR_WRITE; }
        from += (uint64_t)w; len -= (uint64_t)w;
    }
    return SQLITE_OK;
}

// Writes the contiguous prefix of finished records that is not in the file yet (no fsync). Does nothing if a flush or sync is already running.
static bool leader_vis_on (void);
static bool nosync_group_vis_on (void) { static _Atomic int c = MW_KNOB_UNSET; return mw_knob_flag(&c, "MW_NOSYNC_GROUP_VIS"); }   // opt-in: measured slower at 32 agents (§32)
static void wake_vis_range (mw_db *db, uint64_t from, uint64_t to);
static bool pipeline_on (void) { static _Atomic int c = MW_KNOB_UNSET; return !mw_knob_flag(&c, "MW_NO_PIPELINE"); }

static int stage_flush (mw_db *db, bool publish) {
    pthread_mutex_lock(&db->log_mu);
    if (db->sync_running || db->stage_writing || (!pipeline_on() && db->stage_fsyncing) || db->written_end <= db->flushed_off) { pthread_mutex_unlock(&db->log_mu); return SQLITE_OK; }
    db->stage_writing = true;
    uint64_t from = db->flushed_off, hi = db->written_end, hi_epoch = db->written_upto;
    pthread_mutex_unlock(&db->log_mu);
    int rc = ring_write_file(db, from, hi - from);
    pthread_mutex_lock(&db->log_mu);
    db->stage_writing = false;
    if (rc == SQLITE_OK) { db->flushed_off = hi; db->flushed_epoch = hi_epoch; } else atomic_store(&db->failed, 1);
    pthread_cond_broadcast(&db->sync_cv);
    bool wake_vis = false; uint64_t vis_from = 0;
    if (rc == SQLITE_OK && publish && nosync_group_vis_on() && leader_vis_on() && !atomic_load(&db->saw_sync)) {
        // synchronous=off/normal: "visible" only needs the record in the file. Everything up to hi_epoch is installed and written (the prefix is contiguous), so the
        // writer publishes the whole group at once instead of every committer waiting for its predecessor (the chain of §29, worst at 64 agents).
        uint64_t cur = atomic_load_explicit(&db->epoch, memory_order_acquire);
        vis_from = cur;
        while (cur < hi_epoch && !atomic_compare_exchange_weak_explicit(&db->epoch, &cur, hi_epoch, memory_order_release, memory_order_acquire)) {}
        atomic_thread_fence(memory_order_seq_cst);
        wake_vis = vis_from < hi_epoch && atomic_load(&db->vis_parked) > 0;
    }
    pthread_mutex_unlock(&db->log_mu);
    if (wake_vis) wake_vis_range(db, vis_from + 1, hi_epoch + 1);       // (only the slots of the epochs that just became publishable, and the one after)
    return rc;
}

void mw_log_stage_reset (mw_db *db, uint64_t off) {
    pthread_mutex_lock(&db->log_mu);
    db->written_end = db->flushed_off = db->synced_off = off;
    db->flushed_epoch = db->synced_upto = db->written_upto;
    pthread_mutex_unlock(&db->log_mu);
}

// Marks a finished record (epoch, ending at `end`) as copied: advances the contiguous prefix. log_mu held. Returns false on NOMEM.
static bool mark_written (mw_db *db, uint64_t epoch, uint64_t end) {
    if (epoch == db->written_upto + 1) {
        db->written_upto = epoch; db->written_end = end;
        for (bool moved = true; moved; ) {                              // absorb completed out-of-order epochs
            moved = false;
            for (int i = 0; i < db->npending; i++) {
                if (db->written_pending[i].epoch == db->written_upto + 1) {
                    db->written_upto++; db->written_end = db->written_pending[i].end;
                    db->written_pending[i] = db->written_pending[--db->npending];
                    moved = true;
                    break;
                }
            }
        }
        return true;
    }
    if (db->npending == db->pending_cap) {
        int cap = db->pending_cap ? db->pending_cap * 2 : 16;
        mw_pend *np = realloc(db->written_pending, (size_t)cap * sizeof(mw_pend));
        if (!np) return false;
        db->written_pending = np;
        db->pending_cap = cap;
    }
    db->written_pending[db->npending].epoch = epoch; db->written_pending[db->npending].end = end; db->npending++;
    return true;
}

static int append_staged (mw_db *db, uint64_t off, uint64_t epoch, uint32_t dbsize, int n, const uint32_t *pgnos, const uint8_t *const *images, int sync) {
    size_t pgsz = (size_t)db->store->pgsz;
    size_t body = (size_t)n * (4 + pgsz);
    uint64_t size = REC_HDR_SIZE + (uint64_t)body, end = off + size, R = db->stage_r;
    if (mw_fault_hit(MW_FAULT_LOG_WRITE_ERR)) return SQLITE_IOERR_WRITE;
    rec_hdr r = { .magic = REC_MAGIC, .npages = (uint32_t)n, .epoch = epoch, .dbsize = dbsize, .pgsz = (uint32_t)pgsz, .cksum = 0 };
    // checksum straight from the sources (same value as over the contiguous record)
    { rec_hdr c = r; ckstream ck; ck.h = fnv64(db->log_salt ^ 1469598103934665603ull, &c, sizeof c); ck.tn = 0;
      for (int i = 0; i < n; i++) { ck_feed(&ck, &pgnos[i], 4); ck_feed(&ck, images[i], pgsz); }
      r.cksum = ck_final(&ck); }
    bool big = size > R / 4;                                             // (a record that cannot fit the ring comfortably goes to the file directly)
    // 1. room: the ring bytes of file offset x are reused by x + R, so everything below end - R must be in the file
    pthread_mutex_lock(&db->log_mu);
    while (big ? db->flushed_off < off : end > db->flushed_off + R) {
        if (atomic_load(&db->failed)) { pthread_mutex_unlock(&db->log_mu); return SQLITE_IOERR; }
        if (!db->sync_running && !db->stage_writing && db->written_end > db->flushed_off) {
            pthread_mutex_unlock(&db->log_mu);
            int rc = stage_flush(db, false);
            if (rc != SQLITE_OK) return rc;
            pthread_mutex_lock(&db->log_mu);
            continue;
        }
        db->sync_waiters++;
        pthread_cond_wait(&db->sync_cv, &db->log_mu);
        db->sync_waiters--;
    }
    pthread_mutex_unlock(&db->log_mu);
    if (big) {
        // direct: everything before us is in the file (flushed_off == off), nobody else writes this range
        uint8_t *buf = malloc((size_t)size);
        if (!buf) return SQLITE_NOMEM;
        memcpy(buf, &r, sizeof r);
        uint8_t *p = buf + REC_HDR_SIZE;
        for (int i = 0; i < n; i++) { memcpy(p, &pgnos[i], 4); memcpy(p + 4, images[i], pgsz); p += 4 + pgsz; }
        if (fault_fires(MW_CRASH_MID_LOG)) { pwrite_all(db->logfd, buf, (size_t)size / 2, (off_t)off); _exit(9); }
        int rc = pwrite_all(db->logfd, buf, (size_t)size, (off_t)off);
        free(buf);
        if (rc != SQLITE_OK) return rc;
        pthread_mutex_lock(&db->log_mu);
        db->flushed_off = end; db->flushed_epoch = epoch;                 // (set together with the prefix so a writer never rewrites this range from the ring)
        if (!mark_written(db, epoch, end)) { pthread_mutex_unlock(&db->log_mu); return SQLITE_NOMEM; }
        pthread_cond_broadcast(&db->sync_cv);
        pthread_mutex_unlock(&db->log_mu);
        return SQLITE_OK;
    }
    // 2. copy into the ring
    ring_copy(db, off, &r, sizeof r);
    uint64_t pos = off + REC_HDR_SIZE;
    for (int i = 0; i < n; i++) { ring_copy(db, pos, &pgnos[i], 4); ring_copy(db, pos + 4, images[i], pgsz); pos += 4 + pgsz; }
    if (fault_fires(MW_CRASH_MID_LOG)) { ring_write_file(db, off, size / 2); _exit(9); }        // torn-write crash: half the record reaches the file
    // 3. join the prefix
    if (sync) atomic_store(&db->saw_sync, 1);
    pthread_mutex_lock(&db->log_mu);
    uint64_t before = db->written_end;
    if (!mark_written(db, epoch, end)) { pthread_mutex_unlock(&db->log_mu); return SQLITE_NOMEM; }
    if (db->written_end != before && db->sync_waiters > 0 && !db->sync_running && !db->stage_writing) pthread_cond_broadcast(&db->sync_cv);   // somebody may be able to write now
    pthread_mutex_unlock(&db->log_mu);
    if (!sync) return stage_flush(db, true);                                    // no durability wait: the record still goes to the file promptly (a killed process keeps it)
    return SQLITE_OK;
}

static bool leader_vis_on (void) { static _Atomic int c = MW_KNOB_UNSET; return !mw_knob_flag(&c, "MW_NO_LEADER_VIS"); }

// Staged group commit: the first committer whose record is inside the prefix and finds no sync running becomes the leader: it writes the prefix and fsyncs.
static int sync_staged (mw_db *db, uint64_t my_end) {
    int rc = SQLITE_OK;
    bool did_io = false; int wakes = 0;
    const bool pipe = pipeline_on();
    pthread_mutex_lock(&db->log_mu);
    while (db->synced_off < my_end) {
        if (atomic_load(&db->failed)) { rc = SQLITE_IOERR_FSYNC; break; }
        // Two roles, each held by one thread at a time, and (unless MW_NO_PIPELINE) working at the same time: the *writer* pwritev's the contiguous prefix of
        // finished records that is not in the file yet; the *syncer* fsyncs everything that was written before its fsync started. While one thread is in
        // fsync for group n another writes group n+1; group n+1 then needs its own fsync after the first one ends (an fsync covers only what was written before it began).
        bool can_write = my_end > db->flushed_off && db->written_end >= my_end && !db->sync_running && !db->stage_writing && (pipe || !db->stage_fsyncing);
        bool can_sync  = my_end <= db->flushed_off && !db->sync_running && !db->stage_fsyncing && (pipe || !db->stage_writing);
        if (can_write) {
            db->stage_writing = true;
            uint64_t from = db->flushed_off, hi = db->written_end, hi_epoch = db->written_upto;
            pthread_mutex_unlock(&db->log_mu);
            uint64_t tw = MW_T0();
            int wrc = ring_write_file(db, from, hi - from);
            MW_T1(MW_ST_SY_WRITE, tw);
            pthread_mutex_lock(&db->log_mu);
            db->stage_writing = false;
            did_io = true;
            if (wrc != SQLITE_OK) { atomic_store(&db->failed, 1); rc = SQLITE_IOERR_WRITE; pthread_cond_broadcast(&db->sync_cv); break; }
            db->flushed_off = hi; db->flushed_epoch = hi_epoch;
            pthread_cond_broadcast(&db->sync_cv);                        // the syncer may go; ring space was freed
            continue;
        }
        if (can_sync) {
            db->stage_fsyncing = true;
            uint64_t target = db->flushed_off, target_epoch = db->flushed_epoch;
            pthread_mutex_unlock(&db->log_mu);
            uint64_t tc0 = MW_T0();
            int frc = mw_fault_hit(MW_FAULT_LOG_SYNC_ERR) ? -1 : fsync(db->logfd);
            MW_T1(MW_ST_SY_FSYNC, tc0);
            MW_T1(MW_ST_SY_CYCLE, tc0);
            pthread_mutex_lock(&db->log_mu);
            db->stage_fsyncing = false;
            did_io = true;
            if (frc != 0) { atomic_store(&db->failed, 1); rc = SQLITE_IOERR_FSYNC; pthread_cond_broadcast(&db->sync_cv); break; }
            if (mw_timing_on) { mw_count_add(MW_C_SY_LEADER, 1); mw_count_add(MW_C_SY_RECS, target_epoch - db->synced_upto); mw_count_add(MW_C_SY_BYTES, target - db->synced_off); }
            if (target > db->synced_off) { db->synced_off = target; db->synced_upto = target_epoch; }
            atomic_fetch_add(&db->n_log_syncs, 1);
            bool wake_vis = false;
            if (leader_vis_on()) {
                // Every epoch up to target_epoch is durable and installed (the prefix is contiguous): publish them all now. Without this each committer of the group makes
                // its own epoch visible after its predecessor, a chain of wake-ups (measured 141 us per commit with 32 committers).
                uint64_t cur = atomic_load_explicit(&db->epoch, memory_order_acquire);
                while (cur < target_epoch && !atomic_compare_exchange_weak_explicit(&db->epoch, &cur, target_epoch, memory_order_release, memory_order_acquire)) {}
                wake_vis = atomic_load(&db->vis_parked) > 0;
            }
            pthread_cond_broadcast(&db->sync_cv);
            if (wake_vis) { pthread_mutex_unlock(&db->log_mu); mw_db_wake_all_visibility(db); pthread_mutex_lock(&db->log_mu); }
            continue;
        }
        int stage = (db->sync_running || db->stage_writing || db->stage_fsyncing) ? MW_ST_SY_BEHIND : MW_ST_SY_PREFIX;
        uint64_t tw0 = MW_T0();
        db->sync_waiters++;
        pthread_cond_wait(&db->sync_cv, &db->log_mu);
        db->sync_waiters--;
        MW_T1(stage, tw0); wakes++;
    }
    if (mw_timing_on && !did_io) { mw_count_add(MW_C_SY_FOLLOWER, 1); mw_count_add(MW_C_SY_WAKES, (uint64_t)wakes); }
    pthread_mutex_unlock(&db->log_mu);
    return rc;
}

// Waits (bounded) until every assigned record is in the file: compaction needs to read the log through the mapping. Staged mode only.
static bool stage_drained (mw_db *db) {
    stage_flush(db, false);
    pthread_mutex_lock(&db->log_mu);
    bool done = db->written_upto >= atomic_load(&db->next_epoch) && db->flushed_off >= db->log_off;
    pthread_mutex_unlock(&db->log_mu);
    return done;
}

// MARK: - append / group commit -

int mw_log_append (mw_db *db, uint64_t off, uint64_t epoch, uint32_t dbsize, int n, const uint32_t *pgnos, const uint8_t *const *images, int sync) {
    size_t pgsz = (size_t)db->store->pgsz;
    size_t body = (size_t)n * (4 + pgsz);
    if (mw_fault_hit(MW_FAULT_ALLOC_ERR)) return SQLITE_NOMEM;
    if (staged_mode(db, sync)) return append_staged(db, off, epoch, dbsize, n, pgnos, images, sync);
    rec_hdr r = { .magic = REC_MAGIC, .npages = (uint32_t)n, .epoch = epoch, .dbsize = dbsize, .pgsz = (uint32_t)pgsz, .cksum = 0 };
    int rc = SQLITE_OK;
    uint8_t *map = db->logmap;
    if (mw_fault_hit(MW_FAULT_LOG_WRITE_ERR)) {
        rc = SQLITE_IOERR_WRITE;
    } else if (map && off + REC_HDR_SIZE + body <= db->logfile_size) {
        // body first, header (with the magic) last: a record that was never completed does not validate
        uint8_t *p = map + off + REC_HDR_SIZE;
        for (int i = 0; i < n; i++) { memcpy(p, &pgnos[i], 4); memcpy(p + 4, images[i], pgsz); p += 4 + pgsz; }
        if (fault_fires(MW_CRASH_MID_LOG)) _exit(9);                        // body written, header not: torn record
        r.cksum = rec_cksum(db->log_salt, &r, map + off + REC_HDR_SIZE, body);
        memcpy(map + off, &r, sizeof r);
    } else {
        uint8_t *buf = malloc(REC_HDR_SIZE + body);
        if (!buf) return SQLITE_NOMEM;
        uint8_t *p = buf + REC_HDR_SIZE;
        for (int i = 0; i < n; i++) { memcpy(p, &pgnos[i], 4); memcpy(p + 4, images[i], pgsz); p += 4 + pgsz; }
        r.cksum = rec_cksum(db->log_salt, &r, buf + REC_HDR_SIZE, body);
        memcpy(buf, &r, sizeof r);
        if (fault_fires(MW_CRASH_MID_LOG)) {                                // torn-write crash: half the record reaches the file
            pwrite_all(db->logfd, buf, (REC_HDR_SIZE + body) / 2, (off_t)off);
            _exit(9);
        }
        rc = pwrite_all(db->logfd, buf, REC_HDR_SIZE + body, (off_t)off);
        free(buf);
    }
    if (rc != SQLITE_OK) return rc;

    // written-prefix bookkeeping (needed to know what an fsync covers)
    pthread_mutex_lock(&db->log_mu);
    if ((off < db->logsync_off || db->sync_running) && (db->logsync_low1 == 0 || off + 1 < db->logsync_low1)) db->logsync_low1 = off + 1;   // completed after the sync watermark passed it: the next msync must start here
    if (!mark_written(db, epoch, off + REC_HDR_SIZE + body)) {
        pthread_mutex_unlock(&db->log_mu);
        // the record is complete and would validate after a crash although the commit is reported as failed: destroy its header
        if (map && off + REC_HDR_SIZE + body <= db->logfile_size) memset(map + off, 0, REC_HDR_SIZE);
        else { uint8_t z[REC_HDR_SIZE]; memset(z, 0, sizeof z); pwrite_all(db->logfd, z, sizeof z, (off_t)off); }
        return SQLITE_NOMEM;
    }
    pthread_mutex_unlock(&db->log_mu);
    return SQLITE_OK;
}

// Group commit: an fsync that *starts* after this record was written makes it durable, so concurrent
// committers share one fsync (leader/follower on a generation counter).
int mw_log_sync (mw_db *db, uint64_t epoch, uint64_t my_end) {
    (void)epoch;
    if (atomic_load_explicit(&db->log_mode, memory_order_acquire) == 2) return sync_staged(db, my_end);
    int rc = SQLITE_OK;
    pthread_mutex_lock(&db->log_mu);
    uint64_t need = db->sync_started + 1;           // the first fsync to begin after our pwrite completed
    while (db->sync_done < need) {
        if (!db->sync_running) {
            db->sync_running = true;
            uint64_t mine = ++db->sync_started;
            uint8_t *map = db->logmap;                                 // (one consistent view of the mapping, the fd and the range for the whole sync)
            int fd = db->logfd;
            uint64_t hi = db->log_off, lo = db->logsync_off;
            if (db->logsync_low1 && db->logsync_low1 - 1 < lo) lo = db->logsync_low1 - 1;
            db->logsync_low1 = 0;
            pthread_mutex_unlock(&db->log_mu);
            int frc = -1;
            if (!mw_fault_hit(MW_FAULT_LOG_SYNC_ERR)) {
                if (map) {                            // only the range written since the last sync
                    static uint64_t osp = 0;
                    if (!osp) osp = (uint64_t)sysconf(_SC_PAGESIZE);                   // msync needs OS-page alignment (16 KB on Apple silicon)
                    uint64_t from = lo & ~(osp - 1), to = (hi + osp - 1) & ~(osp - 1);
                    if (to > LOG_MAP_BYTES) to = LOG_MAP_BYTES;                      // (records past the mapping were pwritten: fsync covers them)
                    if (to > from) frc = msync(map + from, (size_t)(to - from), MS_SYNC); else frc = 0;
                    if (frc == 0) { pthread_mutex_lock(&db->log_mu); if (hi > db->logsync_off) db->logsync_off = hi; pthread_mutex_unlock(&db->log_mu); }
                } else frc = 0;
                if (frc == 0) frc = fsync(fd);
            }
            pthread_mutex_lock(&db->log_mu);
            db->sync_running = false;
            if (frc != 0) { rc = SQLITE_IOERR_FSYNC; pthread_cond_broadcast(&db->sync_cv); break; }
            db->sync_done = mine;
            atomic_fetch_add(&db->n_log_syncs, 1);
            pthread_cond_broadcast(&db->sync_cv);
        } else {
            pthread_cond_wait(&db->sync_cv, &db->log_mu);
        }
    }
    pthread_mutex_unlock(&db->log_mu);
    return rc;
}

int mw_log_set_base (mw_db *db, uint64_t base_epoch) {
    int rc = hdr_write(db, (uint32_t)db->store->pgsz, base_epoch, db->log_salt);
    if (rc == SQLITE_OK) { pthread_mutex_lock(&db->log_mu); db->base_epoch = base_epoch; pthread_mutex_unlock(&db->log_mu); }
    return rc;
}

// MARK: - visibility ordering -

// Epoch E becomes visible only after E-1 did, so readers never observe a hole. The waiter for E parks on
// the slot E % MW_VIS_SLOTS and is woken by whoever makes E-1 visible (one wake-up per commit instead of a
// broadcast to every in-flight committer), after a short spin because the predecessor is usually about to
// finish. If an earlier commit failed (db->failed) nobody can wait for it any more.

static void wake_vis_range (mw_db *db, uint64_t from, uint64_t to) {
    if (to - from >= MW_VIS_SLOTS) { mw_db_wake_all_visibility(db); return; }
    for (uint64_t e = from; e <= to; e++) {
        struct mw_vis_slot *sl = &db->vis[e % MW_VIS_SLOTS];
        if (atomic_load(&sl->waiters) == 0) continue;
        pthread_mutex_lock(&sl->mu);
        pthread_cond_broadcast(&sl->cv);
        pthread_mutex_unlock(&sl->mu);
    }
}

void mw_db_wake_all_visibility (mw_db *db) {
    for (int i = 0; i < MW_VIS_SLOTS; i++) {
        pthread_mutex_lock(&db->vis[i].mu);
        pthread_cond_broadcast(&db->vis[i].cv);
        pthread_mutex_unlock(&db->vis[i].mu);
    }
}

#if defined(__aarch64__)
#define CPU_RELAX() __asm__ __volatile__("yield")
#elif defined(__x86_64__)
#define CPU_RELAX() __asm__ __volatile__("pause")
#else
#define CPU_RELAX() ((void)0)
#endif

int mw_db_make_visible (mw_db *db, uint64_t epoch) {
    if (atomic_load_explicit(&db->epoch, memory_order_acquire) >= epoch) return SQLITE_OK;   // (already visible: the sync leader publishes the whole group it made durable)
    if (mw_timing_on && atomic_load_explicit(&db->epoch, memory_order_acquire) == epoch - 1) mw_count_add(MW_C_VIS_IMMEDIATE, 1);
    uint64_t tvs0 = MW_T0();
    // The predecessor is usually microseconds from finishing: spin briefly (a futex park/wake costs 10-20 us,
    // which measured as the dominant cost with 10+ committers) and only then sleep on our slot.
    for (int spin = 0; spin < 20000 && atomic_load_explicit(&db->epoch, memory_order_acquire) < epoch - 1; spin++) {
        CPU_RELAX();
        if ((spin & 1023) == 1023 && atomic_load(&db->failed)) return SQLITE_IOERR;
    }
    if (mw_timing_on && atomic_load_explicit(&db->epoch, memory_order_acquire) >= epoch - 1) { MW_T1(MW_ST_VIS_SPIN, tvs0); if (tvs0) mw_count_add(MW_C_VIS_SPUN, 1); }
    if (atomic_load_explicit(&db->epoch, memory_order_acquire) < epoch - 1) {
        MW_T1(MW_ST_VIS_SPIN, tvs0);
        uint64_t tvp0 = MW_T0();
        if (mw_timing_on) mw_count_add(MW_C_VIS_PARKED, 1);
        struct mw_vis_slot *me = &db->vis[epoch % MW_VIS_SLOTS];
        atomic_fetch_add(&me->waiters, 1);
        atomic_fetch_add(&db->vis_parked, 1);
        pthread_mutex_lock(&me->mu);
        while (atomic_load_explicit(&db->epoch, memory_order_acquire) < epoch - 1) {
            if (atomic_load(&db->failed)) { pthread_mutex_unlock(&me->mu); atomic_fetch_sub(&me->waiters, 1); atomic_fetch_sub(&db->vis_parked, 1); return SQLITE_IOERR; }
            pthread_cond_wait(&me->cv, &me->mu);
        }
        pthread_mutex_unlock(&me->mu);
        atomic_fetch_sub(&me->waiters, 1);
        atomic_fetch_sub(&db->vis_parked, 1);
        MW_T1(MW_ST_VIS_PARK, tvp0);
    }
    uint64_t expect = epoch - 1;
    atomic_compare_exchange_strong_explicit(&db->epoch, &expect, epoch, memory_order_release, memory_order_relaxed);   // (fails only if a leader already published it)
    atomic_thread_fence(memory_order_seq_cst);                                 // store(epoch) before load(waiters): no lost wake-up
    struct mw_vis_slot *next = &db->vis[(epoch + 1) % MW_VIS_SLOTS];
    if (atomic_load(&next->waiters) > 0) {                                     // wake the successor only if it parked
        pthread_mutex_lock(&next->mu);
        pthread_cond_broadcast(&next->cv);                                     // (broadcast: two epochs may share a slot)
        pthread_mutex_unlock(&next->mu);
    }
    return SQLITE_OK;
}

// MARK: - log rewrite (keeps the log bounded under continuous load) -

// Replaces the log by a new file holding only the records newer than `base_epoch` (header base = base_epoch).
// Called from compaction with store->seq_mu held: no epoch/offset can be assigned meanwhile; records already
// assigned are waited for (their pwrite runs outside seq_mu). crash safety: the new file is complete and
// fsynced before rename() atomically replaces the old one.
int mw_log_rewrite_tail (mw_db *db, uint64_t base_epoch) {
    mw_store *st = db->store;
    // first record newer than base_epoch, from the size records
    // The size records cannot be used to find it: local GC prunes them up to this process's oldest snapshot, which in
    // multi-process mode can be newer than base_epoch (a missing record would make the new log start after base+1: a gap).
    // Scan the headers instead.
    uint64_t tail_off = db->logmap ? mw_log_scan_after(db, base_epoch, db->log_off) : 0;
    if (tail_off == 0) return SQLITE_OK;                                  // no map: leave the log alone
    // wait for in-flight record writes (bounded)
    for (int spin = 0; spin < 200000; spin++) {
        pthread_mutex_lock(&db->log_mu);
        bool done = db->written_upto >= atomic_load(&db->next_epoch);
        pthread_mutex_unlock(&db->log_mu);
        if (done && atomic_load(&db->log_mode) == 2) done = stage_drained(db);        // (staged records must be in the file: the copy below reads it)
        else if (!done && atomic_load(&db->log_mode) == 2) stage_flush(db, false);
        if (done) break;
        if (spin == 199999) return SQLITE_BUSY;
        sched_yield();
    }
    uint64_t end = db->log_off;
    if (tail_off > end) return SQLITE_OK;
    char *tmp = sqlite3_mprintf("%s.new", db->logpath);
    if (!tmp) return SQLITE_NOMEM;
    int nfd = open(tmp, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (nfd < 0) { sqlite3_free(tmp); return SQLITE_CANTOPEN; }
    int rc = SQLITE_OK;
    if (flock(nfd, (db->mp ? LOCK_SH : LOCK_EX) | LOCK_NB) != 0) rc = SQLITE_BUSY;
    log_hdr h;
    memset(&h, 0, sizeof h);
    memcpy(h.magic, LOG_MAGIC, 8);
    h.version = 1; h.pgsz = (uint32_t)st->pgsz; h.base_epoch = base_epoch; h.salt = db->log_salt;
    h.cksum = hdr_cksum(&h);
    if (rc == SQLITE_OK) rc = pwrite_all(nfd, &h, sizeof h, 0);
    size_t chunk = 1 << 20;
    uint8_t *buf = malloc(chunk);
    if (!buf) rc = SQLITE_NOMEM;
    uint64_t pos = tail_off, out = LOG_HDR_SIZE;
    while (rc == SQLITE_OK && pos < end) {
        size_t n = (size_t)(end - pos < chunk ? end - pos : chunk);
        rc = pread_all(db->logfd, buf, n, (off_t)pos);
        if (rc == SQLITE_OK) rc = pwrite_all(nfd, buf, n, (off_t)out);
        pos += n; out += n;
    }
    free(buf);
    if (rc == SQLITE_OK && fsync(nfd) != 0) rc = SQLITE_IOERR_FSYNC;
    if (rc == SQLITE_OK && rename(tmp, db->logpath) != 0) rc = SQLITE_IOERR;
    if (rc == SQLITE_OK) mw_fault_hit(MW_CRASH_LOG_RENAME);
    if (rc != SQLITE_OK) { close(nfd); unlink(tmp); sqlite3_free(tmp); return rc; }
    sqlite3_free(tmp);
    // swap: offsets shift by (LOG_HDR_SIZE - tail_off)
    sync_quiesce(db);                                                    // (a group-commit leader may be inside msync/fsync on the old mapping and fd)
    log_unmap(db, true);
    close(db->logfd);                                                    // (releases the old inode's lock; the new one is held)
    db->logfd = nfd;
    int64_t shift = (int64_t)LOG_HDR_SIZE - (int64_t)tail_off;
    for (int i = 0; i < st->nsizes; i++) if (st->sizes[i].log_off >= tail_off) st->sizes[i].log_off = (uint64_t)((int64_t)st->sizes[i].log_off + shift); else st->sizes[i].log_off = 0;
    db->log_off = out;
    mw_log_stage_reset(db, out);
    mw_log_remap(db);
    pthread_mutex_lock(&db->log_mu); db->base_epoch = base_epoch; pthread_mutex_unlock(&db->log_mu);
    sync_resume(db);
    return SQLITE_OK;
}

// Byte offset of the first record whose epoch is > `epoch` in the (freshly opened) log, scanning from the header.
// Only headers are read (records were validated by whoever wrote them). Used after the log was reset/rewritten.
uint64_t mw_log_scan_after (mw_db *db, uint64_t epoch, uint64_t end) {
    uint64_t off = LOG_HDR_SIZE;
    size_t pgsz = (size_t)db->store->pgsz;
    while (off + REC_HDR_SIZE <= end && db->logmap) {
        rec_hdr r;
        memcpy(&r, db->logmap + off, sizeof r);
        if (r.magic != REC_MAGIC || r.pgsz != (uint32_t)pgsz || r.npages == 0) break;
        if (r.epoch > epoch) return off;
        off += REC_HDR_SIZE + (uint64_t)r.npages * (4 + pgsz);
    }
    return off;
}

uint64_t mw_log_header_base (mw_db *db) {
    log_hdr h;
    if (!db->logmap) return 0;
    memcpy(&h, db->logmap, sizeof h);
    return memcmp(h.magic, LOG_MAGIC, 8) == 0 ? h.base_epoch : 0;
}

// Switches to the log file currently at db->logpath (another process replaced it), keeping a shared lock on it.
int mw_log_reopen (mw_db *db) {
    int nfd = open(db->logpath, O_RDWR);
    if (nfd < 0) return SQLITE_CANTOPEN;
    if (flock(nfd, LOCK_SH | LOCK_NB) != 0) { close(nfd); return SQLITE_BUSY; }
    sync_quiesce(db);
    db->logsync_off = 0; db->logsync_low1 = 0;
    log_unmap(db, true);
    if (db->logfd >= 0) { flock(db->logfd, LOCK_UN); close(db->logfd); }
    db->logfd = nfd;
    struct stat sb;
    db->logfile_size = fstat(nfd, &sb) == 0 ? (uint64_t)sb.st_size : 0;     // (a new file: the caller of the replacement publishes its size as shm->log_ready)
    void *m = mmap(NULL, LOG_MAP_BYTES, PROT_READ | PROT_WRITE, MAP_SHARED, nfd, 0);
    log_mapped(db, m);
    sync_resume(db);
    return db->logmap ? SQLITE_OK : SQLITE_IOERR;
}
