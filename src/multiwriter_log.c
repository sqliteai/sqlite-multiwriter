//
//  multiwriter_log.c
//  sqlite-multiwriter
//
//  Durable commit log ("<db>-mw"). Committed state = real database file + committed page versions;
//  the versions live in memory, so every commit is first appended here as one checksummed record:
//
//    header (64 B): LOG_MAGIC, version, page size, base_epoch, salt, cksum
//    record (40 B header + npages * (4 + pgsz) + ext_len): magic, npages, epoch, dbsize, pgsz, ext_len, cksum
//
//  Durability ordering (documented in docs/design.md):
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
#if defined(__has_include)
#if __has_include(<execinfo.h>) && !(defined(__ANDROID__) && __ANDROID_API__ < 33)
#include <execinfo.h>         // (debugging aid of the fault injection; musl and old Android have none)
#define MW_HAVE_BACKTRACE 1
#endif
#endif
#include "multiwriter_os.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "multiwriter_io.h"
#include "multiwriter_internal.h"
#include "multiwriter_seglog.h"

#define LOG_MAP_BYTES (1ull << 30)         /* virtual size of the mapping */
#define LOG_GROW_BYTES (32ull << 20)
#define LOG_MAGIC "MWLOG002"                /* one constant for create, rewrite and recovery */
#define LOG_HDR_SIZE 64
#define REC_HDR_SIZE 40
#define REC_MAGIC    0x3243574du      /* "MWC2": records carry a metadata extension (the change-capture cells of the commit) after the pages */

typedef struct {
    char     magic[8];
    uint32_t version, pgsz;
    uint64_t base_epoch, salt;
    uint64_t features;               // incompatible features that the file uses (MW_FORMAT_FEATURES): a library that does not know one refuses the file
    uint64_t reserved[2];
    uint64_t cksum;
} log_hdr;

typedef struct {
    uint32_t magic, npages;
    uint64_t epoch;
    uint32_t dbsize, pgsz;
    uint32_t ext_len, pad;           // bytes of the extension that follows the pages (0 without change capture)
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

// Tests: the append that is armed waits here, after its place was assigned and before its bytes are written (as one that is slow to be scheduled does).
static void fault_stall (mw_fault_t f) {
    if (!fault_fires(f)) return;
    static _Atomic int d = MW_KNOB_UNSET; int us = mw_knob_int(&d, "MW_FAULT_DELAY_US", 0);
    if (us > 0) usleep((useconds_t)us);
}

// 0: this library can read the file. The header of a file of the engine whose checksum does not check out is a damaged one (the caller decides), not one of another format.
int mw_format_check (const char *what, const char *path, const char magic[8], const char *expected, uint32_t version, uint64_t features) {
    if (memcmp(magic, expected, 8) != 0) {
        if (memcmp(magic, expected, 5) == 0) { sqlite3_log(SQLITE_WARNING, "multiwriter: the %s %s is of another format (%.8s, this library reads %.8s): it was made by another version of the engine and is left as it is", what, path, magic, expected); return SQLITE_CANTOPEN; }
        return SQLITE_OK;
    }
    if (version != MW_FORMAT_VERSION) { sqlite3_log(SQLITE_WARNING, "multiwriter: the %s %s is of format version %u, this library reads %u: it was made by another version of the engine and is left as it is", what, path, (unsigned)version, (unsigned)MW_FORMAT_VERSION); return SQLITE_CANTOPEN; }
    if (features & ~(uint64_t)MW_FORMAT_FEATURES) { sqlite3_log(SQLITE_WARNING, "multiwriter: the %s %s uses features (0x%llx) that this library does not know: it was made by a newer version of the engine and is left as it is", what, path, (unsigned long long)(features & ~(uint64_t)MW_FORMAT_FEATURES)); return SQLITE_CANTOPEN; }
    return SQLITE_OK;
}

bool mw_fault_hit (mw_fault_t f) {
    if (!fault_fires(f)) return false;
    if (f >= MW_CRASH_MID_LOG) _exit(9);               // crash point: die right here, no cleanup
    // MW_FAULT_DELAY_US (tests): the failure comes after a while, as that of a write that is tried again for a full disk (MW_ENOSPC_WAIT_MS) does; the commits behind it are then in the queue
    static _Atomic int d = MW_KNOB_UNSET; int us = mw_knob_int(&d, "MW_FAULT_DELAY_US", 0);
    if (us > 0) usleep((useconds_t)us);
    return true;
}


// ---- I/O fault injection (multiwriter_io.h) ----
_Atomic int mw_io_armed;
_Atomic int mw_fullfsync;
void mw_set_fullfsync (int on) { atomic_store(&mw_fullfsync, on != 0); }
static _Atomic long io_left, io_calls; static _Atomic int io_kinds, io_err, io_sticky, io_short, io_fired;
void mw_io_fault_arm (int kinds, long nth, int err, int sticky, int shortw) {
    atomic_store(&io_calls, 0); atomic_store(&io_fired, 0); atomic_store(&io_kinds, kinds); atomic_store(&io_err, err ? err : EIO);
    atomic_store(&io_sticky, sticky || shortw); atomic_store(&io_short, shortw); atomic_store(&io_left, nth);
    atomic_store(&mw_io_armed, 1);
}
void mw_io_fault_disarm (void) { atomic_store(&mw_io_armed, 0); atomic_store(&io_fired, 0); }
long mw_io_fault_calls (void) { return atomic_load(&io_calls); }
int mw_io_fault (int kind, size_t *partial) {
    if (!(kind & atomic_load(&io_kinds))) return 0;
    atomic_fetch_add(&io_calls, 1);
    if (atomic_load(&io_fired)) return atomic_load(&io_sticky) ? (kind == MW_IO_MAP ? ENOMEM : atomic_load(&io_err)) : 0;
    if (atomic_fetch_sub(&io_left, 1) != 1) return 0;
    atomic_store(&io_fired, 1);
#ifdef MW_HAVE_BACKTRACE
    if (getenv("MW_IO_TRACE")) { void *bt[24]; int n = backtrace(bt, 24); fprintf(stderr, "io fault fires (kind %d):\n", kind); backtrace_symbols_fd(bt, n, 2); }
#endif
    if (partial && atomic_load(&io_short)) *partial = 1;
    return kind == MW_IO_MAP ? ENOMEM : atomic_load(&io_err);
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

// A file that was created or renamed is in its directory only once the directory is flushed.
static void sync_dir_of (const char *path) {
    char *dir = sqlite3_mprintf("%s", path); if (!dir) return;
    char *sl = mw_last_sep(dir); if (sl) { if (sl == dir) sl[1] = 0; else *sl = 0; } else { sqlite3_free(dir); dir = sqlite3_mprintf("."); if (!dir) return; }
    int fd = open(dir, O_RDONLY);
    if (fd >= 0) { (void)mw_sys_fsync(fd); close(fd); }
    sqlite3_free(dir);
}

static int pwrite_all (int fd, const void *buf, size_t n, off_t off) {
    const char *p = buf;
    while (n > 0) {
        ssize_t w = mw_io_pwrite(fd, p, n, off);
        if (w < 0) { if (errno == EINTR) continue; return mw_io_rc(errno, SQLITE_IOERR_WRITE); }
        p += w; off += w; n -= (size_t)w;
    }
    return SQLITE_OK;
}

static int pread_all (int fd, void *buf, size_t n, off_t off) {
    char *p = buf;
    while (n > 0) {
        ssize_t r = mw_io_pread(fd, p, n, off);
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
    h.version = MW_FORMAT_VERSION; h.features = MW_FORMAT_FEATURES; h.pgsz = pgsz; h.base_epoch = base; h.salt = salt;
    h.cksum = hdr_cksum(&h);
    int rc = pwrite_all(db->logfd, &h, sizeof h, 0);
    if (rc == SQLITE_OK && mw_io_fsync(db->logfd) != 0) rc = SQLITE_IOERR_FSYNC;
    return rc;
}


static uint64_t rec_cksum (uint64_t salt, const rec_hdr *h, const void *body, size_t body_len) {
    rec_hdr c = *h;
    c.cksum = 0;
    uint64_t x = fnv64(salt ^ 1469598103934665603ull, &c, sizeof c);
    return fnv64(x, body, body_len);
}

int mw_recovered_ext_add (mw_db *db, uint64_t epoch, const uint8_t *ext, uint32_t len) {
    if (db->nrext == db->caprext) {
        int nc = db->caprext ? db->caprext * 2 : 16;
        mw_rext *n = realloc(db->rext, (size_t)nc * sizeof(mw_rext));
        if (!n) return SQLITE_NOMEM;
        db->rext = n; db->caprext = nc;
    }
    uint8_t *c = malloc(len);
    if (!c) return SQLITE_NOMEM;
    memcpy(c, ext, len);
    db->rext[db->nrext++] = (mw_rext){ epoch, c, len };
    return SQLITE_OK;
}

uint64_t mw_log_record_size (mw_db *db, int n, uint32_t ext_len) {
    return REC_HDR_SIZE + (uint64_t)n * (4 + (uint64_t)db->store->pgsz) + ext_len;
}

// MARK: - open / recovery -

// Replays the valid, contiguous prefix of the log's records newer than `base` into `st` (and collects their metadata extensions in db->rext). *off_out: where the prefix ends,
// *last_out: the epoch of its last record.
static int log_replay (mw_db *db, mw_store *st, int pgsz, uint64_t base, uint64_t limit, uint64_t *last_out, off_t *off_out) {
    off_t off = LOG_HDR_SIZE;
    uint64_t last = base, prev_epoch = 0;
    size_t rec_cap = 0;
    uint8_t *buf = NULL;
    int rc = SQLITE_OK;
    struct stat fsb;
    uint64_t fsz = fstat(db->logfd, &fsb) == 0 ? (uint64_t)fsb.st_size : UINT64_MAX;
    for (;;) {
        rec_hdr r;
        if ((uint64_t)off >= limit) break;
        { int prc = pread_all(db->logfd, &r, sizeof r, off); if (prc == SQLITE_IOERR_SHORT_READ) break; if (prc != SQLITE_OK) { rc = prc; break; } }       // (the end of the file ends the prefix; a read that failed does not: the records after it are not garbage)
        if (r.magic != REC_MAGIC || r.pgsz != (uint32_t)pgsz || r.npages == 0 || r.npages > (1u << 24)) break;
        size_t body = (size_t)r.npages * (4 + (size_t)pgsz) + r.ext_len;
        if ((uint64_t)off + REC_HDR_SIZE + body > fsz) break;                          // a torn / garbage header must not size an allocation
        if (body > rec_cap) {
            uint8_t *nb = realloc(buf, body);
            if (!nb) { rc = SQLITE_NOMEM; break; }
            buf = nb; rec_cap = body;
        }
        { int prc = pread_all(db->logfd, buf, body, off + REC_HDR_SIZE); if (prc == SQLITE_IOERR_SHORT_READ) break; if (prc != SQLITE_OK) { rc = prc; break; } }
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
            if (rc == SQLITE_OK && r.ext_len) rc = mw_recovered_ext_add(db, r.epoch, buf + (size_t)r.npages * (4 + (size_t)pgsz), r.ext_len);
            free(pgnos); free(imgs);
            if (rc != SQLITE_OK) break;
            last = r.epoch;
        }
        off += (off_t)REC_HDR_SIZE + (off_t)body;
    }
    free(buf);
    *last_out = last; *off_out = off;
    return rc;
}

int mw_log_open (mw_db *db, int pgsz) {
    db->logpath = sqlite3_mprintf("%s-mw", db->path);
    if (!db->logpath) return SQLITE_NOMEM;
    db->logfd = open(db->logpath, O_RDWR | O_CREAT | O_NOFOLLOW, mw_file_mode(db->path));
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
    if (sb.st_size >= LOG_HDR_SIZE && pread_all(db->logfd, &h, sizeof h, 0) == SQLITE_OK && (memcmp(h.magic, LOG_MAGIC, 5) == 0)) {       // a log of this engine: of this format?
        int frc = (memcmp(h.magic, LOG_MAGIC, 8) == 0 && h.cksum != hdr_cksum(&h)) ? SQLITE_OK : mw_format_check("log", db->logpath, h.magic, LOG_MAGIC, h.version, h.features);
        if (frc != SQLITE_OK) { close(db->logfd); db->logfd = -1; db->has_log = false; return frc; }
    }
    bool valid = sb.st_size >= LOG_HDR_SIZE && pread_all(db->logfd, &h, sizeof h, 0) == SQLITE_OK &&
                 memcmp(h.magic, LOG_MAGIC, 8) == 0 && h.cksum == hdr_cksum(&h) && h.pgsz == (uint32_t)pgsz;
    uint64_t base = 1;
    if (!valid && sb.st_size > (off_t)LOG_HDR_SIZE) {         // a log with records after a header that does not check out (another page size, another format, a damaged header) is not a new log: the commits in it are not thrown away in silence
        return SQLITE_CORRUPT;
    }
    if (!valid) {                                            // new log (empty, or a header that was being written): start empty from the real file
        sqlite3_randomness(sizeof db->log_salt, &db->log_salt);
        if (mw_io_ftruncate(db->logfd, 0) != 0) return SQLITE_IOERR;
        int rc = hdr_write(db, (uint32_t)pgsz, base, db->log_salt);
        if (rc != SQLITE_OK) return rc;
        sync_dir_of(db->logpath);                                                       // (the new file itself must be there after a power failure)
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
    uint64_t limit = UINT64_MAX;
    uint64_t last = base; off_t off = LOG_HDR_SIZE;
    int rc = log_replay(db, st, pgsz, base, limit, &last, &off);
    if (rc != SQLITE_OK) return rc;
    if (limit == UINT64_MAX && sb.st_size > off) {                                      // drop the torn tail (only the first process); durably: a record of it that survived a power failure could otherwise rejoin the log behind new commits that reuse its epochs
        if (mw_io_ftruncate(db->logfd, off) != 0) return SQLITE_IOERR;
        if (mw_io_fsync(db->logfd) != 0) return SQLITE_IOERR_FSYNC;
    }
    db->log_off = (uint64_t)off;
    mw_log_stage_reset(db, db->log_off);
    pthread_mutex_lock(&st->seq_mu); mw_log_remap(db); pthread_mutex_unlock(&st->seq_mu);
    atomic_store(&db->epoch, last);
    atomic_store(&db->next_epoch, last);
    db->written_upto = db->synced_upto = last;
    return SQLITE_OK;
}

static void log_unmap (mw_db *db) {
    if (!db->logmap) return;
    munmap(db->logmap, LOG_MAP_BYTES);
    db->logmap = NULL;
}
static void log_mapped (mw_db *db, void *m) { db->logmap = m == MAP_FAILED ? NULL : m; }

void mw_log_close (mw_db *db, bool remove_file) {
    log_unmap(db);
    if (db->logfd >= 0) { flock(db->logfd, LOCK_UN); close(db->logfd); db->logfd = -1; }
    if (remove_file && db->logpath) unlink(db->logpath);
    sqlite3_free(db->logpath);
    db->logpath = NULL;
    free(db->written_pending);
    db->written_pending = NULL;
    if (db->stage_buf) { munmap(db->stage_buf, (size_t)db->stage_r); db->stage_buf = NULL; }
}


static void sync_quiesce (mw_db *db);
static void sync_resume (mw_db *db);
// ---- recovery in place ----
// A failed log (a write or an fsync that did not succeed: the outcome of the commits of the group is unknown, and they were told so) does not need the database to be opened again.
// Nothing is visible beyond the last durable commit (a commit becomes visible when its fsync is done), so the state to go back to is exactly that one:
//   - the log is cut to the end of the last durable record (what lies beyond may be on the disk in part; the commits it holds were refused, they must not come back);
//   - the page store is rebuilt from the log, as an open does (the old store stays allocated until the database is closed: a reader that was inside it finishes there);
//   - the memory table of the metadata is dropped, and the next commit brings the state in from the file and the log, as an open does;
//   - the counters of the log and of the epochs go back to the durable point.
// It runs when the next commit finds the database failed and every other commit has left (a bounded wait); if anything does not fit (the log is not what it was) the database stays
// failed and the application reopens it, as before.
int mw_db_recover (mw_db *db) {
    if (db->mp || db->mp_req || db->logfd < 0 || !db->store || atomic_load(&db->log_mode) != 2 || db->nretired >= 8) return SQLITE_IOERR;
    int expect = 0; if (!atomic_compare_exchange_strong(&db->recovering, &expect, 1)) return SQLITE_BUSY;
    int rc = SQLITE_IOERR;
    for (int i = 0; i < 3000 && atomic_load(&db->inflight) > 1; i++) usleep(1000);
    if (atomic_load(&db->inflight) > 1) { atomic_store(&db->recovering, 0); return rc; }
    pthread_mutex_lock(&db->compact_mu);
    sync_quiesce(db);
    mw_store *old = db->store, *nst = NULL;
    uint64_t V = atomic_load(&db->epoch), E, D;
    pthread_mutex_lock(&db->log_mu); E = db->synced_off; D = db->synced_upto; pthread_mutex_unlock(&db->log_mu);
    if (V != D || E < LOG_HDR_SIZE) goto out;
    if (mw_io_ftruncate(db->logfd, (off_t)E) != 0) goto out;
    if (mw_io_fsync(db->logfd) != 0) goto out;                                          // (the records of the commits that were refused must not come back after a power failure, behind the commits that reuse their epochs)
    nst = mw_store_create(old->pgsz, old->base_dbsize);
    if (nst) nst->base_limit = old->base_limit;
    if (!nst) { rc = SQLITE_NOMEM; goto out; }
    nst->reserved = old->reserved;
    for (int i = 0; i < db->nrext; i++) free(db->rext[i].data);
    db->nrext = 0;
    uint64_t last = db->base_epoch; off_t off = LOG_HDR_SIZE;
    rc = log_replay(db, nst, old->pgsz, db->base_epoch, E, &last, &off);
    if (rc != SQLITE_OK || last != V || (uint64_t)off != E) { mw_store_free(nst); nst = NULL; rc = SQLITE_IOERR; goto out; }
    sqlite3_mutex_enter(db->mu);
    db->retired[db->nretired++] = old;
    db->store = nst;
    sqlite3_mutex_leave(db->mu);
    free(db->p1_cache); db->p1_cache = NULL; db->p1_cache_epoch = 0;
    pthread_mutex_lock(&nst->seq_mu);
    __atomic_store_n(&db->log_off, E, __ATOMIC_RELAXED);
    atomic_store(&db->next_epoch, V);
    pthread_mutex_unlock(&nst->seq_mu);
    pthread_mutex_lock(&db->log_mu);
    db->written_upto = db->synced_upto = db->flushed_epoch = V;
    db->npending = 0;
    pthread_mutex_unlock(&db->log_mu);
    mw_log_stage_reset(db, E);
    atomic_store(&db->failed, 0);
    rc = SQLITE_OK;
out:
    sync_resume(db);
    pthread_mutex_unlock(&db->compact_mu);
    mw_db_wake_all_visibility(db);
    atomic_store(&db->recovering, 0);
    return rc;
}

// MARK: - mapped log -

// The log is written through a MAP_SHARED mapping: appending a record is a memcpy (a process kill cannot lose it,
// the kernel owns the pages), which avoids a write syscall per commit -- profiling showed pwrite was the largest
// cost of a small commit and serialises on the file inode when many threads commit. The file is grown ahead of
// the append offset (sparse); records are validated by checksum at recovery, so the unwritten tail is harmless.
void mw_log_remap (mw_db *db) {
    db->logsync_off = 0; db->logsync_low1 = 0;
    log_unmap(db);
    struct stat sb;
    db->logfile_size = fstat(db->logfd, &sb) == 0 ? (uint64_t)sb.st_size : 0;
#ifdef _WIN32
    if (!db->mp) { log_mapped(db, MAP_FAILED); return; }     // (no mapping of the log of one process: the file with a mapped view cannot be truncated, which compaction does; it is written with pwritev and read with pread)
#endif
    void *m = mw_io_mmap(NULL, LOG_MAP_BYTES, PROT_READ | PROT_WRITE, MAP_SHARED, db->logfd, 0);
    log_mapped(db, m);
    mw_log_reserve_space(db);
}

void mw_log_prefill_bg (mw_db *db) { if (db->mp_req) mw_seglog_prefill_bg(db); }


// Staged log: the disk for the records is reserved ahead (in steps of LOG_RES_STEP), before the commit takes its offset. A full disk then fails the commit that asked for the
// room, cleanly (nothing was assigned, installed or written), and the next one finds the room when the space is back; the group write of records inside the reservation cannot
// fail for want of space, so the database is not left failed. (Measured on APFS: the reservation makes the log faster, 23 against 33-40 us per write and fsync.)
#define LOG_RES_STEP (4ull << 20)
#define LOG_RES_SLACK (2ull << 20)
int mw_log_ensure_room (mw_db *db, uint64_t record_size) {
    if (db->mp || atomic_load_explicit(&db->log_mode, memory_order_acquire) != 2) return SQLITE_OK;
    uint64_t need = __atomic_load_n(&db->log_off, __ATOMIC_RELAXED) + record_size + LOG_RES_SLACK;                       // (log_off read without its lock: a little behind at worst, the slack covers it)
    if (need <= atomic_load_explicit(&db->log_res_end, memory_order_acquire)) return SQLITE_OK;
    pthread_mutex_lock(&db->log_mu);
    uint64_t have = atomic_load(&db->log_res_end), want = need + LOG_RES_STEP;
    if (have < db->written_end) have = db->written_end;                               // (the data already in the file is its own reservation)
    int rc = SQLITE_OK;
    if (db->logfd >= 0 && need > atomic_load(&db->log_res_end)) {
        const int fd = db->logfd;
        { static _Atomic int dly = MW_KNOB_UNSET; const int us = mw_knob_int(&dly, "MW_TEST_ROOM_DELAY_US", 0); if (us > 0) usleep((useconds_t)us); }      // (tests: the descriptor is read, and used a moment later)
        int e = mw_io_reserve(fd, have, want);
        if (e) rc = mw_io_rc(e, SQLITE_IOERR_WRITE); else atomic_store(&db->log_res_end, want);
    }
    pthread_mutex_unlock(&db->log_mu);
    return rc;
}

void mw_log_reserve_space (mw_db *db) {
    // Only the mapped log grows ahead of time. A staged log grows by append: extending a file with ftruncate and then writing into the hole makes the file system
    // allocate blocks inside every write (measured: group sync 70-290 us against 40-50 us when the file simply grows, 2 writers 5-11k against 17k tx/s).
    if (!db->logmap || atomic_load(&db->log_mode) != 1) return;
    uint64_t need = MW_LOG_OFF(db) + LOG_GROW_BYTES / 4;
    if (need <= db->logfile_size) return;
    struct stat cur;
    if (fstat(db->logfd, &cur) == 0 && (uint64_t)cur.st_size >= need) { db->logfile_size = (uint64_t)cur.st_size; return; }     // someone else already grew it
    uint64_t size = ((need + LOG_GROW_BYTES - 1) / LOG_GROW_BYTES) * LOG_GROW_BYTES;
    if (size > LOG_MAP_BYTES || mw_io_ftruncate(db->logfd, (off_t)size) != 0) return;     // cannot map further: records past logfile_size are pwritten (the mapping stays: appenders may be using it)
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
    void *m = mw_io_mmap(NULL, (size_t)R, PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE, -1, 0);
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
            #ifdef _WIN32
            bool on = !db->mp && db->logfd >= 0 && stage_alloc(db);                                      // (always staged: the mapped log is not there)
#else
            bool on = !db->mp && db->logfd >= 0 && !getenv("MW_LOG_STAGING_OFF") && stage_alloc(db);
#endif      // (every sync level; MW_LOG_STAGING_OFF selects the mapped log)
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
        ssize_t w = mw_io_pwritev(db->logfd, iov, len > first ? 2 : 1, (off_t)from);
        if (w < 0) { if (errno == EINTR) continue; return mw_io_rc(errno, SQLITE_IOERR_WRITE); }
        from += (uint64_t)w; len -= (uint64_t)w;
    }
    return SQLITE_OK;
}


// A write that finds the disk full is tried again for a while (MW_ENOSPC_WAIT_MS, default 2000) before it is given up: the bytes are still in the ring, nothing was told to the
// committers, and the space that another process took between the reservation and the write may well be back in a moment. Only after that the log is failed.
static int enospc_wait_ms (void) { static _Atomic int c = MW_KNOB_UNSET; return mw_knob_int(&c, "MW_ENOSPC_WAIT_MS", 2000); }
static int ring_write_file (mw_db *db, uint64_t from, uint64_t len);
static int ring_write_retry (mw_db *db, uint64_t from, uint64_t len) {
    int rc = ring_write_file(db, from, len);
    for (int waited = 0; rc == SQLITE_FULL && waited < enospc_wait_ms(); waited += 10) { usleep(10000); rc = ring_write_file(db, from, len); }
    return rc;
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
    int rc = ring_write_retry(db, from, hi - from);
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
    atomic_store(&db->log_res_end, off);                                       // (the file was cut or replaced: whatever was reserved is gone)
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

static int append_staged (mw_db *db, uint64_t off, uint64_t epoch, uint32_t dbsize, int n, const uint32_t *pgnos, const uint8_t *const *images, const uint8_t *ext, uint32_t ext_len, int sync) {
    size_t pgsz = (size_t)db->store->pgsz;
    size_t body = (size_t)n * (4 + pgsz) + ext_len;
    uint64_t size = REC_HDR_SIZE + (uint64_t)body, end = off + size, R = db->stage_r;
    fault_stall(MW_FAULT_APPEND_STALL);
    if (mw_fault_hit(MW_FAULT_LOG_WRITE_ERR)) return SQLITE_IOERR_WRITE;
    rec_hdr r = { .magic = REC_MAGIC, .npages = (uint32_t)n, .epoch = epoch, .dbsize = dbsize, .pgsz = (uint32_t)pgsz, .ext_len = ext_len, .cksum = 0 };
    // checksum straight from the sources (same value as over the contiguous record)
    { rec_hdr c = r; ckstream ck; ck.h = fnv64(db->log_salt ^ 1469598103934665603ull, &c, sizeof c); ck.tn = 0;
      for (int i = 0; i < n; i++) { ck_feed(&ck, &pgnos[i], 4); ck_feed(&ck, images[i], pgsz); }
      if (ext_len) ck_feed(&ck, ext, ext_len);
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
        if (ext_len) memcpy(p, ext, ext_len);
        if (fault_fires(MW_CRASH_MID_LOG)) { pwrite_all(db->logfd, buf, (size_t)size / 2, (off_t)off); _exit(9); }
        int rc = pwrite_all(db->logfd, buf, (size_t)size, (off_t)off);
        for (int waited = 0; rc == SQLITE_FULL && waited < enospc_wait_ms(); waited += 10) { usleep(10000); rc = pwrite_all(db->logfd, buf, (size_t)size, (off_t)off); }
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
    if (ext_len) ring_copy(db, pos, ext, ext_len);
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
static int sync_staged (mw_db *db, uint64_t my_epoch, uint64_t my_end) {
    int rc = SQLITE_OK;
    bool did_io = false; int wakes = 0;
    const bool pipe = pipeline_on();
    pthread_mutex_lock(&db->log_mu);
    // (done when the file is synced up to the record, or when everything up to its epoch is durable: a compaction that finds nothing in flight cuts the log back to its header and the
    // offsets restart, while a commit whose epoch the group leader has just made visible is still on its way out of this loop)
    while (db->synced_off < my_end && db->synced_upto < my_epoch) {
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
            int wrc = ring_write_retry(db, from, hi - from);
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
            int frc = mw_fault_hit(MW_FAULT_LOG_SYNC_ERR) ? -1 : mw_io_fsync(db->logfd);
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
    bool done = db->written_upto >= atomic_load(&db->next_epoch) && db->flushed_off >= MW_LOG_OFF(db);
    pthread_mutex_unlock(&db->log_mu);
    return done;
}

// MARK: - append / group commit -

int mw_log_append (mw_db *db, uint64_t off, uint64_t epoch, uint32_t dbsize, int n, const uint32_t *pgnos, const uint8_t *const *images, const uint8_t *ext, uint32_t ext_len, int sync) {
    size_t pgsz = (size_t)db->store->pgsz;
    size_t body = (size_t)n * (4 + pgsz) + ext_len;
    if (mw_fault_hit(MW_FAULT_ALLOC_ERR)) return SQLITE_NOMEM;
    if (staged_mode(db, sync)) return append_staged(db, off, epoch, dbsize, n, pgnos, images, ext, ext_len, sync);
    rec_hdr r = { .magic = REC_MAGIC, .npages = (uint32_t)n, .epoch = epoch, .dbsize = dbsize, .pgsz = (uint32_t)pgsz, .ext_len = ext_len, .cksum = 0 };
    int rc = SQLITE_OK;
    uint8_t *map = db->logmap;
    if (mw_fault_hit(MW_FAULT_LOG_WRITE_ERR)) {
        rc = SQLITE_IOERR_WRITE;
    } else if (map && off + REC_HDR_SIZE + body <= db->logfile_size) {
        // body first, header (with the magic) last: a record that was never completed does not validate
        uint8_t *p = map + off + REC_HDR_SIZE;
        for (int i = 0; i < n; i++) { memcpy(p, &pgnos[i], 4); memcpy(p + 4, images[i], pgsz); p += 4 + pgsz; }
        if (ext_len) memcpy(p, ext, ext_len);
        if (fault_fires(MW_CRASH_MID_LOG)) _exit(9);                        // body written, header not: torn record
        r.cksum = rec_cksum(db->log_salt, &r, map + off + REC_HDR_SIZE, body);
        memcpy(map + off, &r, sizeof r);
    } else {
        uint8_t *buf = malloc(REC_HDR_SIZE + body);
        if (!buf) return SQLITE_NOMEM;
        uint8_t *p = buf + REC_HDR_SIZE;
        for (int i = 0; i < n; i++) { memcpy(p, &pgnos[i], 4); memcpy(p + 4, images[i], pgsz); p += 4 + pgsz; }
        if (ext_len) memcpy(p, ext, ext_len);
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
    if (atomic_load_explicit(&db->log_mode, memory_order_acquire) == 2) return sync_staged(db, epoch, my_end);
    int rc = SQLITE_OK;
    pthread_mutex_lock(&db->log_mu);
    uint64_t need = db->sync_started + 1;           // the first fsync to begin after our pwrite completed
    while (db->sync_done < need) {
        if (!db->sync_running) {
            db->sync_running = true;
            uint64_t mine = ++db->sync_started;
            uint8_t *map = db->logmap;                                 // (one consistent view of the mapping, the fd and the range for the whole sync)
            int fd = db->logfd;
            uint64_t hi = MW_LOG_OFF(db), lo = db->logsync_off;
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
                    if (to > from) frc = mw_io_msync(map + from, (size_t)(to - from), MS_SYNC); else frc = 0;
                    if (frc == 0) { pthread_mutex_lock(&db->log_mu); if (hi > db->logsync_off) db->logsync_off = hi; pthread_mutex_unlock(&db->log_mu); }
                } else frc = 0;
                if (frc == 0) frc = mw_io_fsync(fd);
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

// The tail of the log is copied while commits go on, and only what came in meanwhile is copied under seq_mu (the whole copy, the fsync of it and the wait for the writes in flight took 5.5 ms
// there, with every committer waiting for the lock while holding the stripes of its pages: the root page of a table was held for that long, 550 times in 20 seconds).
// Staged log only. Phase 1 (no lock held) copies the records that are durable now; phase 2 (mw_log_rewrite_tail with the same `prep`) the rest.
struct mw_log_prep { int nfd; char *tmp; uint64_t tail_off, copied_end; };
static uint64_t scan_fd_after (mw_db *db, uint64_t epoch, uint64_t from, uint64_t end, uint64_t *first_after) {       // the first record newer than `epoch`, and the end of the last whole record before `end` (headers read from the file)
    uint64_t off = from; size_t pgsz = (size_t)db->store->pgsz; if (first_after) *first_after = 0;
    while (off + REC_HDR_SIZE <= end) {
        rec_hdr r;
        if (pread_all(db->logfd, &r, sizeof r, (off_t)off) != SQLITE_OK) break;
        if (r.magic != REC_MAGIC || r.pgsz != (uint32_t)pgsz || r.npages == 0) break;
        uint64_t next = off + REC_HDR_SIZE + (uint64_t)r.npages * (4 + pgsz) + r.ext_len;
        if (next > end) break;
        if (r.epoch > epoch && first_after && !*first_after) *first_after = off;
        off = next;
    }
    return off;
}
static int copy_range (int from_fd, int to_fd, uint64_t from, uint64_t to, uint64_t out) {
    size_t chunk = 1 << 20; uint8_t *buf = malloc(chunk); if (!buf) return SQLITE_NOMEM;
    int rc = SQLITE_OK;
    for (uint64_t pos = from; rc == SQLITE_OK && pos < to; ) {
        size_t n = (size_t)(to - pos < chunk ? to - pos : chunk);
        rc = pread_all(from_fd, buf, n, (off_t)pos);
        if (rc == SQLITE_OK) rc = pwrite_all(to_fd, buf, n, (off_t)out);
        pos += n; out += n;
    }
    free(buf);
    return rc;
}
void mw_log_rewrite_abort (mw_log_prep *p) { if (!p) return; if (p->nfd >= 0) close(p->nfd); if (p->tmp) { unlink(p->tmp); sqlite3_free(p->tmp); } free(p); }
mw_log_prep *mw_log_rewrite_prepare (mw_db *db, uint64_t base_epoch) {
    if (db->mp || db->mp_req || db->logfd < 0 || !db->store || atomic_load(&db->log_mode) != 2) return NULL;
    uint64_t E; pthread_mutex_lock(&db->log_mu); E = db->flushed_off; pthread_mutex_unlock(&db->log_mu);          // (everything below is in the file, whole records: not the durable part, synced_off, which with synchronous<FULL lags far behind and left a copy of all that came in meanwhile for the part under the lock)
    uint64_t first = 0; uint64_t bound = scan_fd_after(db, base_epoch, LOG_HDR_SIZE, E, &first);
    if (!first) return NULL;
    mw_log_prep *p = calloc(1, sizeof *p); if (!p) return NULL;
    p->nfd = -1; p->tail_off = first;
    p->tmp = sqlite3_mprintf("%s.new", db->logpath); if (!p->tmp) { free(p); return NULL; }
    p->nfd = open(p->tmp, O_RDWR | O_CREAT | O_TRUNC | O_NOFOLLOW, mw_file_mode(db->path));
    if (p->nfd < 0) { p->nfd = -1; mw_log_rewrite_abort(p); return NULL; }
    int rc = flock(p->nfd, LOCK_EX | LOCK_NB) != 0 ? SQLITE_BUSY : SQLITE_OK;
    log_hdr h; memset(&h, 0, sizeof h);
    memcpy(h.magic, LOG_MAGIC, 8); h.version = MW_FORMAT_VERSION; h.features = MW_FORMAT_FEATURES; h.pgsz = (uint32_t)db->store->pgsz; h.base_epoch = base_epoch; h.salt = db->log_salt; h.cksum = hdr_cksum(&h);
    if (rc == SQLITE_OK) rc = pwrite_all(p->nfd, &h, sizeof h, 0);
    if (rc == SQLITE_OK) rc = copy_range(db->logfd, p->nfd, first, bound, LOG_HDR_SIZE);
    if (rc == SQLITE_OK) p->copied_end = bound;
    for (int round = 0; rc == SQLITE_OK && bound > first && round < 4; round++) {                  // more rounds: what came in while the last one was copied (what is left is copied under the lock, which stops the commits)
        pthread_mutex_lock(&db->log_mu); E = db->flushed_off; pthread_mutex_unlock(&db->log_mu);
        uint64_t b2 = scan_fd_after(db, UINT64_MAX, bound, E, NULL);
        if (b2 <= bound) break;
        if (copy_range(db->logfd, p->nfd, bound, b2, LOG_HDR_SIZE + (bound - first)) != SQLITE_OK) break;
        const bool small = b2 - bound < ((uint64_t)1 << 20);
        p->copied_end = bound = b2;
        if (small) break;
    }
    if (rc == SQLITE_OK && mw_io_fsync(p->nfd) != 0) rc = SQLITE_IOERR_FSYNC;                      // (the bulk is on the disk before the lock is taken)
    if (rc != SQLITE_OK) { mw_log_rewrite_abort(p); return NULL; }
    return p;
}

// Replaces the log by a new file holding only the records newer than `base_epoch` (header base = base_epoch).
// Called from compaction with store->seq_mu held: no epoch/offset can be assigned meanwhile; records already
// assigned are waited for (their pwrite runs outside seq_mu). crash safety: the new file is complete and
// fsynced before rename() atomically replaces the old one.
int mw_log_rewrite_tail (mw_db *db, uint64_t base_epoch, mw_log_prep *prep) {
    mw_store *st = db->store;
    // first record newer than base_epoch, from the size records
    // The size records cannot be used to find it: local GC prunes them up to this process's oldest snapshot, which in
    // multi-process mode can be newer than base_epoch (a missing record would make the new log start after base+1: a gap).
    // Scan the headers instead.
    uint64_t tail_off = prep ? prep->tail_off : 0;
    if (!prep && !db->logmap) { return SQLITE_OK; }                          // no map: leave the log alone
    // wait for in-flight record writes (bounded)
    for (int spin = 0; spin < 200000; spin++) {
        pthread_mutex_lock(&db->log_mu);
        bool done = db->written_upto >= atomic_load(&db->next_epoch);
        pthread_mutex_unlock(&db->log_mu);
        if (done && atomic_load(&db->log_mode) == 2) done = stage_drained(db);        // (staged records must be in the file: the copy below reads it)
        else if (!done && atomic_load(&db->log_mode) == 2) stage_flush(db, false);
        if (done) break;
        if (spin == 199999) { mw_log_rewrite_abort(prep); return SQLITE_BUSY; }
        sched_yield();
    }
    uint64_t end = MW_LOG_OFF(db);
    // (the scan after the wait: a record that is assigned and not written yet starts at the end of the file, and the scan reads its header through the mapping, which is beyond the file when
    // that is on a page boundary: the process died of SIGBUS. Now every record that is assigned is in the file)
    if (!prep) tail_off = mw_log_scan_after(db, base_epoch, end);
    if (tail_off == 0) { mw_log_rewrite_abort(prep); return SQLITE_OK; }
    if (tail_off > end || (prep && prep->copied_end > end)) { mw_log_rewrite_abort(prep); return SQLITE_OK; }
    char *tmp; int nfd; int rc = SQLITE_OK;
    uint64_t pos = tail_off, out = LOG_HDR_SIZE;
    if (prep) {                                                           // (the bulk was copied and fsynced before the lock: only what came in since is left)
        tmp = prep->tmp; nfd = prep->nfd; pos = prep->copied_end; out = LOG_HDR_SIZE + (prep->copied_end - tail_off);
        free(prep); prep = NULL;
    } else {
        tmp = sqlite3_mprintf("%s.new", db->logpath);
        if (!tmp) return SQLITE_NOMEM;
        nfd = open(tmp, O_RDWR | O_CREAT | O_TRUNC | O_NOFOLLOW, mw_file_mode(db->path));
        if (nfd < 0) { sqlite3_free(tmp); return SQLITE_CANTOPEN; }
        if (flock(nfd, (db->mp ? LOCK_SH : LOCK_EX) | LOCK_NB) != 0) rc = SQLITE_BUSY;
        log_hdr h;
        memset(&h, 0, sizeof h);
        memcpy(h.magic, LOG_MAGIC, 8);
        h.version = MW_FORMAT_VERSION; h.features = MW_FORMAT_FEATURES; h.pgsz = (uint32_t)st->pgsz; h.base_epoch = base_epoch; h.salt = db->log_salt;
        h.cksum = hdr_cksum(&h);
        if (rc == SQLITE_OK) rc = pwrite_all(nfd, &h, sizeof h, 0);
    }
    if (rc == SQLITE_OK) { rc = copy_range(db->logfd, nfd, pos, end, out); out += end - pos; }
    if (rc == SQLITE_OK && mw_io_fsync(nfd) != 0) rc = SQLITE_IOERR_FSYNC;
    if (rc == SQLITE_OK && mw_io_rename(tmp, db->logpath) != 0) rc = SQLITE_IOERR;
    if (rc == SQLITE_OK) sync_dir_of(db->logpath);                                       // (the rename is durable before commits are acknowledged on the new file)
    if (rc == SQLITE_OK) mw_fault_hit(MW_CRASH_LOG_RENAME);
    if (rc != SQLITE_OK) { close(nfd); unlink(tmp); sqlite3_free(tmp); return rc; }
    sqlite3_free(tmp);
    // swap: offsets shift by (LOG_HDR_SIZE - tail_off)
    sync_quiesce(db);                                                    // (a group-commit leader may be inside msync/fsync on the old mapping and fd)
    log_unmap(db);
    { static _Atomic int dly = MW_KNOB_UNSET; const int us = mw_knob_int(&dly, "MW_TEST_SWAP_DELAY_US", 0); if (us > 0) usleep((useconds_t)us); }      // (tests: widens the gap between the quiesce and the swap of the descriptor)
    pthread_mutex_lock(&db->log_mu);                                     // (mw_log_ensure_room uses the descriptor under this lock, outside of the group-commit quiesce: it must not find it closed, or reused by another open)
    close(db->logfd);                                                    // (releases the old inode's lock; the new one is held)
    db->logfd = nfd;
    pthread_mutex_unlock(&db->log_mu);
    int64_t shift = (int64_t)LOG_HDR_SIZE - (int64_t)tail_off;
    for (int i = 0; i < st->nsizes; i++) if (st->sizes[i].log_off >= tail_off) st->sizes[i].log_off = (uint64_t)((int64_t)st->sizes[i].log_off + shift); else st->sizes[i].log_off = 0;
    __atomic_store_n(&db->log_off, out, __ATOMIC_RELAXED);
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
        off += REC_HDR_SIZE + (uint64_t)r.npages * (4 + pgsz) + r.ext_len;
    }
    return off;
}

