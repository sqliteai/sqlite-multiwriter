//
//  multiwriter_seglog.c
//  sqlite-multiwriter
//
//  Segmented commit log of the shared mode: see multiwriter_seglog.h.
//

#include <errno.h>
#include "multiwriter_os.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "multiwriter_io.h"
#include "multiwriter_seglog.h"
#include "multiwriter_wait.h"

#define SEG_MAGIC "MWLOG002"
#define REC_HDR_SIZE 40
#define REC_MAGIC 0x3243574du                         // v2: the record carries a metadata extension after the pages
#define SEG_PREFILL_CHUNK (1u << 20)

#if defined(__aarch64__)
#define mw_cpu_relax() __asm__ __volatile__("yield")
#elif defined(__x86_64__)
#define mw_cpu_relax() __asm__ __volatile__("pause")
#else
#define mw_cpu_relax() ((void)0)
#endif

typedef struct { char magic[8]; uint32_t version, pgsz; uint64_t base_epoch, salt; uint64_t features; uint64_t reserved[2]; uint64_t cksum; } seg_hdr;       // (features: see mw_format_check)
typedef struct { uint32_t magic, npages; uint64_t epoch; uint32_t dbsize, pgsz; uint32_t ext_len, pad; uint64_t cksum; } rec_hdr;
_Static_assert(sizeof(seg_hdr) == MW_SEG_HDR, "segment header layout");
_Static_assert(sizeof(rec_hdr) == REC_HDR_SIZE, "record header layout");

typedef struct {
    _Atomic uint32_t seg;           // 0 = free
    _Atomic int      users;
    _Atomic int      dying;
    uint8_t         *base;
    size_t           len;
    int              fd;
} segmap;

struct mw_seglog {
    char            *prefix;        // "<db>-mw."
    uint32_t         pgsz;
    uint64_t         salt;
    uint64_t         seg_bytes;
    mode_t           mode;          // of the files it creates (that of the database)
    mw_shm          *shm;
    pthread_mutex_t  map_mu;
    segmap           maps[MW_SEG_MAPS];
    uint8_t         *zeros;         // SEG_PREFILL_CHUNK zero bytes
};

// MARK: - helpers -

static void first_epoch_set (mw_shm *sh, uint32_t seg, uint64_t epoch) {
    atomic_store_explicit(&sh->seg_first_epoch[seg % 256], epoch, memory_order_release);
    atomic_store_explicit(&sh->seg_first_id[seg % 256], seg, memory_order_release);
}
static uint64_t fnv64 (uint64_t h, const void *p, size_t n) {
    const unsigned char *b = p;
    while (n >= 8) { uint64_t w; memcpy(&w, b, 8); h ^= w; h *= 0x9E3779B97F4A7C15ull; h ^= h >> 32; b += 8; n -= 8; }
    while (n--) { h ^= *b++; h *= 1099511628211ull; }
    return h;
}
static uint64_t hdr_cksum (const seg_hdr *h) { seg_hdr c = *h; c.cksum = 0; return fnv64(1469598103934665603ull, &c, sizeof c); }
// The checksum of a record: the header (with the salt), and a hash of the body made of one hash per page (its content, not its number: a page that a relocation renumbers keeps its hash) folded in order with the page number, then the
// extension. The hashes of the pages can be made before the publication lock is taken (mw_seglog_content_hash); only the ones of pages that the relocation changed are made inside it.
uint64_t mw_seglog_content_hash (const void *img, size_t pgsz) { return fnv64(0xA24BAED4963EE407ull, img, pgsz) | 1; }
static inline uint64_t bh_fold (uint64_t b, uint32_t pgno, uint64_t ch) { b = (b ^ ch ^ ((uint64_t)pgno * 0xC2B2AE3D27D4EB4Full)) * 0x9E3779B97F4A7C15ull; return b ^ (b >> 29); }
#define BH_SEED 0x2545F4914F6CDD1Dull
#define BH_EXT_PGNO 0xFFFFFFFFu
static uint64_t body_hash_bytes (const uint8_t *body, uint32_t n, size_t pgsz, uint32_t ext_len) {
    uint64_t b = BH_SEED; const uint8_t *p = body;
    for (uint32_t k = 0; k < n; k++, p += 4 + pgsz) { uint32_t pg; memcpy(&pg, p, 4); b = bh_fold(b, pg, mw_seglog_content_hash(p + 4, pgsz)); }
    if (ext_len) b = bh_fold(b, BH_EXT_PGNO, fnv64(0x9FB21C651E98DF25ull, p, ext_len));
    return b;
}
static uint64_t rec_cksum_h (uint64_t salt, const rec_hdr *h, uint64_t body_hash) { rec_hdr c = *h; c.cksum = 0; return fnv64(fnv64(salt ^ 1469598103934665603ull, &c, sizeof c), &body_hash, sizeof body_hash); }
static uint64_t rec_cksum (uint64_t salt, const rec_hdr *h, const void *body, size_t n) { (void)n; return rec_cksum_h(salt, h, body_hash_bytes(body, h->npages, h->pgsz, h->ext_len)); }

static int pwrite_all (int fd, const void *buf, size_t n, off_t off) {
    const char *p = buf;
    while (n > 0) { ssize_t w = mw_io_pwrite(fd, p, n, off); if (w < 0) { if (errno == EINTR) continue; return mw_io_rc(errno, SQLITE_IOERR_WRITE); } p += w; off += w; n -= (size_t)w; }
    return SQLITE_OK;
}
static int pread_all (int fd, void *buf, size_t n, off_t off) {
    char *p = buf;
    while (n > 0) { ssize_t r = mw_io_pread(fd, p, n, off); if (r < 0) { if (errno == EINTR) continue; return SQLITE_IOERR_READ; } if (r == 0) return SQLITE_IOERR_SHORT_READ; p += r; off += r; n -= (size_t)r; }
    return SQLITE_OK;
}

static void seg_path (mw_seglog *sl, uint32_t seg, char *buf, size_t n) { snprintf(buf, n, "%s%u", sl->prefix, seg); }
static uint64_t rec_size (mw_seglog *sl, int n, uint32_t ext_len) { return REC_HDR_SIZE + (uint64_t)n * (4 + (uint64_t)sl->pgsz) + ext_len; }

static int write_hdr (mw_seglog *sl, int fd, uint64_t base) {
    seg_hdr h; memset(&h, 0, sizeof h);
    memcpy(h.magic, SEG_MAGIC, 8);
    h.version = MW_FORMAT_VERSION; h.features = MW_FORMAT_FEATURES; h.pgsz = sl->pgsz; h.base_epoch = base; h.salt = sl->salt;
    h.cksum = hdr_cksum(&h);
    return pwrite_all(fd, &h, sizeof h, 0);
}

// MARK: - mappings -

static void map_drop (segmap *m) {                                  // (map_mu held, no users)
    if (m->base) munmap(m->base, m->len);
    if (m->fd >= 0) close(m->fd);
    m->base = NULL; m->fd = -1; m->len = 0;
    atomic_store(&m->seg, 0);
    atomic_store(&m->dying, 0);
}

// The mapping of `seg` with a use counted (release with map_release), or NULL: the file does not exist, or the slot is taken by a segment that is still in use.
static segmap *map_acquire (mw_seglog *sl, uint32_t seg) {
    segmap *m = &sl->maps[seg % MW_SEG_MAPS];
    for (int attempt = 0; attempt < 2; attempt++) {
        if (atomic_load_explicit(&m->seg, memory_order_acquire) == seg && !atomic_load(&m->dying)) {
            atomic_fetch_add(&m->users, 1);                                           // seq_cst: before dying is read
            if (atomic_load_explicit(&m->seg, memory_order_acquire) == seg && !atomic_load(&m->dying)) return m;
            atomic_fetch_sub(&m->users, 1);
        }
        pthread_mutex_lock(&sl->map_mu);
        if (atomic_load(&m->seg) != seg || atomic_load(&m->dying)) {
            if (atomic_load(&m->seg) != 0) {
                // (dying first, users second, both seq_cst: a reader that counted itself in before sees dying, or we see its count; reading users alone let a reader
                // slip in between the check and the unmap)
                atomic_store(&m->dying, 1);
                if (atomic_load(&m->users) != 0) { atomic_store(&m->dying, 0); pthread_mutex_unlock(&sl->map_mu); return NULL; }     // the slot's segment is still being read
                map_drop(m);
            }
            char path[600]; seg_path(sl, seg, path, sizeof path);
            int fd = open(path, O_RDWR);
            if (fd < 0) { pthread_mutex_unlock(&sl->map_mu); return NULL; }
            struct stat sb;
            if (fstat(fd, &sb) != 0 || sb.st_size < MW_SEG_HDR) { close(fd); pthread_mutex_unlock(&sl->map_mu); return NULL; }
            void *p = mw_io_mmap(NULL, (size_t)sb.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
            if (p == MAP_FAILED) { close(fd); pthread_mutex_unlock(&sl->map_mu); return NULL; }
            m->base = p; m->len = (size_t)sb.st_size; m->fd = fd;
            atomic_store(&m->dying, 0);
            atomic_store_explicit(&m->seg, seg, memory_order_release);
        }
        pthread_mutex_unlock(&sl->map_mu);
    }
    return NULL;
}
static void map_release (segmap *m) { atomic_fetch_sub(&m->users, 1); }

// MARK: - segment files -

// Creates "<prefix><seg>" (or "...new" for a segment being prepared) with its header, zero-filled to `size`: written blocks, not a sparse file (appending into a hole while
// another process fsyncs the file stalls: see the log of the other multi-process mode).
// A segment that was created or renamed is in its directory only once the directory is flushed (ext4 and xfs also flush it with the fsync of the new file; the standard does not promise it).
static void sync_dir_of (const char *path) {
    char dir[700]; snprintf(dir, sizeof dir, "%s", path); char *sl = strrchr(dir, '/'); if (sl) { if (sl == dir) sl[1] = 0; else *sl = 0; } else snprintf(dir, sizeof dir, ".");
    int fd = open(dir, O_RDONLY); if (fd >= 0) { (void)mw_sys_fsync(fd); close(fd); }
}
static int seg_create (mw_seglog *sl, uint32_t seg, uint64_t size, uint64_t base, bool tmp, bool fill) {
    char path[620]; seg_path(sl, seg, path, sizeof path);
    if (tmp) strncat(path, ".new", sizeof path - strlen(path) - 1);
    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC | O_NOFOLLOW, sl->mode);
    if (fd < 0) return SQLITE_CANTOPEN;
    int rc = write_hdr(sl, fd, base);
    if (rc == SQLITE_OK && fill) {
        uint8_t *z = calloc(1, SEG_PREFILL_CHUNK);
        if (!z) rc = SQLITE_NOMEM;
        for (uint64_t off = MW_SEG_HDR; rc == SQLITE_OK && off < size; ) {
            uint64_t k = size - off < SEG_PREFILL_CHUNK ? size - off : SEG_PREFILL_CHUNK;
            rc = pwrite_all(fd, z, (size_t)k, (off_t)off); off += k;
        }
        free(z);
    } else if (rc == SQLITE_OK && mw_io_ftruncate(fd, (off_t)size) != 0) rc = mw_io_rc(errno, SQLITE_IOERR);
    close(fd);
    if (rc != SQLITE_OK) unlink(path);                         // (a segment that could not be made whole is not left behind)
    else if (!tmp) sync_dir_of(path);
    return rc;
}

static uint64_t seg_size_for (mw_seglog *sl, uint64_t need) {
    uint64_t s = sl->seg_bytes;
    if (MW_SEG_HDR + need > s) s = ((MW_SEG_HDR + need) + (1u << 20) - 1) & ~((uint64_t)(1u << 20) - 1);
    return s;
}

// MARK: - open / close -

static uint64_t env_seg_bytes (void) {
    const char *e = getenv("MW_SEG_MB");
    long mb = e ? atol(e) : 16;
    if (mb < 1) mb = 1;
    return (uint64_t)mb << 20;
}

typedef struct { uint32_t id; } segid;
static int cmp_segid (const void *a, const void *b) { uint32_t x = ((const segid *)a)->id, y = ((const segid *)b)->id; return x < y ? -1 : x > y; }

// Lists the existing segment ids (ascending) and removes "*.new" leftovers.
static int list_segments (mw_seglog *sl, const char *dbpath, segid **out, int *nout, bool clean) {
    char dir[600], base[300];
    const char *slash = strrchr(dbpath, '/');
    if (slash) { size_t n = (size_t)(slash - dbpath); if (n >= sizeof dir) return SQLITE_CANTOPEN; memcpy(dir, dbpath, n); dir[n] = 0; snprintf(base, sizeof base, "%s", slash + 1); }
    else { snprintf(dir, sizeof dir, "."); snprintf(base, sizeof base, "%s", dbpath); }
    char pre[320]; snprintf(pre, sizeof pre, "%s-mw.", base);
    size_t pl = strlen(pre);
    DIR *d = opendir(dir);
    if (!d) return SQLITE_CANTOPEN;
    segid *v = NULL; int n = 0, cap = 0;
    struct dirent *e;
    while ((e = readdir(d)) != NULL) {
        if (strncmp(e->d_name, pre, pl) != 0) continue;
        const char *s = e->d_name + pl;
        char *end = NULL;
        unsigned long id = strtoul(s, &end, 10);
        if (end == s) continue;
        if (*end == 0) {
            if (n == cap) { cap = cap ? cap * 2 : 16; segid *nv = realloc(v, (size_t)cap * sizeof *v); if (!nv) { free(v); closedir(d); return SQLITE_NOMEM; } v = nv; }
            v[n++].id = (uint32_t)id;
        } else if (clean && strcmp(end, ".new") == 0) { char p[900]; snprintf(p, sizeof p, "%s/%s", dir, e->d_name); unlink(p); }
    }
    closedir(d);
    if (n) qsort(v, (size_t)n, sizeof *v, cmp_segid);
    *out = v; *nout = n;
    (void)sl;
    return SQLITE_OK;
}

int mw_seglog_open (mw_db *db, mw_seglog_replay_fn fn, void *ctx, uint64_t *base_out, uint64_t *last_epoch_out) {
    mw_seglog *sl = calloc(1, sizeof *sl);
    if (!sl) return SQLITE_NOMEM;
    size_t pl = strlen(db->path) + 8;
    sl->prefix = malloc(pl);
    if (!sl->prefix) { free(sl); return SQLITE_NOMEM; }
    snprintf(sl->prefix, pl, "%s-mw.", db->path);
    sl->mode = mw_file_mode(db->path);
    sl->pgsz = (uint32_t)db->store->pgsz;
    sl->seg_bytes = env_seg_bytes();
    sl->shm = db->shm;
    sl->zeros = calloc(1, SEG_PREFILL_CHUNK);
    if (!sl->zeros) { free(sl->prefix); free(sl); return SQLITE_NOMEM; }
    pthread_mutex_init(&sl->map_mu, NULL);
    for (int i = 0; i < MW_SEG_MAPS; i++) sl->maps[i].fd = -1;
    db->sl = sl;
    mw_shm *sh = db->shm;
    uint64_t base = 1, last = 1;
    if (!db->mp_first) {                                              // attach: the first opener has recovered everything
        // the salt is in the first live segment's header
        // (a compaction may trim that segment between reading seg_min and opening it: read it again, the salt is the same in every segment)
        int fd = -1; seg_hdr h; bool ok = false;
        for (int tries = 0; tries < 200 && !ok; tries++) {
            uint32_t s = atomic_load(&sh->seg_min);
            char path[620]; seg_path(sl, s, path, sizeof path);
            fd = open(path, O_RDONLY);
            ok = fd >= 0 && pread_all(fd, &h, sizeof h, 0) == SQLITE_OK && memcmp(h.magic, SEG_MAGIC, 8) == 0;
            if (!ok && fd >= 0) { close(fd); fd = -1; }
            if (!ok) usleep(500);
        }
        if (!ok) return SQLITE_CANTOPEN;
        close(fd);
        if (h.cksum == hdr_cksum(&h)) { char path[620]; seg_path(sl, atomic_load(&sh->seg_min), path, sizeof path); int frc = mw_format_check("segment", path, h.magic, SEG_MAGIC, h.version, h.features); if (frc != SQLITE_OK) return frc; }
        sl->salt = h.salt;
        if (base_out) *base_out = atomic_load(&sh->base_epoch);
        if (last_epoch_out) *last_epoch_out = atomic_load(&sh->committed_epoch);
        return SQLITE_OK;
    }
    // ---- the first opener: recovery
    segid *ids = NULL; int nids = 0;
    int rc = list_segments(sl, db->path, &ids, &nids, true);
    if (rc != SQLITE_OK) return rc;
    uint32_t cur_seg = 1; uint64_t cur_end = MW_SEG_HDR;
    uint64_t first_epoch_of_cur = 2;
    if (nids == 0) {
        sqlite3_randomness(sizeof sl->salt, &sl->salt);
        rc = seg_create(sl, 1, sl->seg_bytes, base, false, true);
        if (rc != SQLITE_OK) { free(ids); return rc; }
        first_epoch_set(sh, 1, 2);
        atomic_store(&sh->seg_min, 1);
    } else {
        // base = the largest of the headers; salt = the first one's
        bool have_salt = false;
        for (int i = 0; i < nids; i++) {
            char path[620]; seg_path(sl, ids[i].id, path, sizeof path);
            int fd = open(path, O_RDONLY); seg_hdr h;
            if (fd >= 0 && pread_all(fd, &h, sizeof h, 0) == SQLITE_OK && memcmp(h.magic, SEG_MAGIC, 5) == 0 && (memcmp(h.magic, SEG_MAGIC, 8) != 0 || h.cksum == hdr_cksum(&h))) {       // (a segment of this engine: of this format?)
                int frc = mw_format_check("segment", path, h.magic, SEG_MAGIC, h.version, h.features);
                if (frc != SQLITE_OK) { close(fd); free(ids); return frc; }
            }
            if (fd >= 0 && pread_all(fd, &h, sizeof h, 0) == SQLITE_OK && memcmp(h.magic, SEG_MAGIC, 8) == 0 && h.cksum == hdr_cksum(&h) && h.pgsz == sl->pgsz) {
                if (!have_salt) { sl->salt = h.salt; have_salt = true; }
                if (h.base_epoch > base) base = h.base_epoch;
            }
            if (fd >= 0) close(fd);
        }
        if (!have_salt) { free(ids); return SQLITE_CORRUPT; }
        // replay the valid prefix of records newer than the base, segment by segment
        uint64_t prev_epoch = 0; last = base; bool stop = false;
        uint32_t *pg = NULL; uint64_t *locs = NULL; uint32_t cap = 0;
        uint32_t good_seg = ids[0].id; uint64_t good_end = MW_SEG_HDR;                 // where the valid prefix ends
        for (int i = 0; i < nids && !stop && rc == SQLITE_OK; i++) {
            char path[620]; seg_path(sl, ids[i].id, path, sizeof path);
            int fd = open(path, O_RDWR);
            if (fd < 0) { if (errno != ENOENT) rc = SQLITE_CANTOPEN; stop = true; break; }           // (a segment that is not there ends the log; one that cannot be opened does not)
            struct stat sb; if (fstat(fd, &sb) != 0) { close(fd); rc = SQLITE_IOERR; break; }
            uint64_t off = MW_SEG_HDR; uint8_t *buf = NULL; size_t bcap = 0;
            uint64_t first_in_seg = 0;
            for (;;) {                                                                    // records until the first one that is not valid: the end of this segment (zero padding after the last one that fitted)
                rec_hdr r;
                if (off + REC_HDR_SIZE > (uint64_t)sb.st_size) break;
                { int prc = pread_all(fd, &r, sizeof r, (off_t)off); if (prc == SQLITE_IOERR_SHORT_READ) break; if (prc != SQLITE_OK) { rc = prc; break; } }       // (a read that fails is not the end of the log: what is behind it is not a torn tail)
                if (r.magic != REC_MAGIC || r.pgsz != sl->pgsz || r.npages == 0 || r.npages > (1u << 24)) break;
                size_t body = (size_t)r.npages * (4 + (size_t)sl->pgsz) + r.ext_len;
                if (off + REC_HDR_SIZE + body > (uint64_t)sb.st_size) break;
                if (body > bcap) { uint8_t *nb = realloc(buf, body); if (!nb) { rc = SQLITE_NOMEM; break; } buf = nb; bcap = body; }
                { int prc = pread_all(fd, buf, body, (off_t)(off + REC_HDR_SIZE)); if (prc == SQLITE_IOERR_SHORT_READ) break; if (prc != SQLITE_OK) { rc = prc; break; } }
                if (r.cksum != rec_cksum(sl->salt, &r, buf, body)) break;
                if (prev_epoch && r.epoch != prev_epoch + 1) break;                       // a gap (or stale bytes): the prefix ends here
                if (r.epoch > base && r.epoch != last + 1) break;                          // must continue right after the base
                prev_epoch = r.epoch;
                if (!first_in_seg) first_in_seg = r.epoch;
                if (r.epoch > base) {
                    if (r.npages > cap) { uint32_t *np = realloc(pg, r.npages * sizeof *np); uint64_t *nl = realloc(locs, r.npages * sizeof *nl); if (!np || !nl) { rc = SQLITE_NOMEM; break; } pg = np; locs = nl; cap = r.npages; }
                    for (uint32_t k = 0; k < r.npages; k++) {
                        memcpy(&pg[k], buf + (size_t)k * (4 + sl->pgsz), 4);
                        locs[k] = MW_LOC(ids[i].id, off + REC_HDR_SIZE + (uint64_t)k * (4 + sl->pgsz) + 4);
                    }
                    if (fn) rc = fn(ctx, r.epoch, r.dbsize, (int)r.npages, pg, locs, buf + (size_t)r.npages * (4 + sl->pgsz), r.ext_len, MW_LOC(ids[i].id, off + REC_HDR_SIZE + (uint64_t)r.npages * (4 + sl->pgsz)));
                    if (rc != SQLITE_OK) break;
                    last = r.epoch;
                }
                off += REC_HDR_SIZE + body;
            }
            free(buf);
            close(fd);
            if (first_in_seg) { first_epoch_set(sh, ids[i].id, first_in_seg); good_seg = ids[i].id; good_end = off; }
            else if (i == 0) { first_epoch_set(sh, ids[i].id, base + 1); good_seg = ids[i].id; good_end = MW_SEG_HDR; }
            else { stop = true; }                                                          // a segment without a valid first record: nothing after the previous one belongs to the log
        }
        cur_seg = good_seg; cur_end = good_end;
        for (int j = 0; j < nids; j++) if (ids[j].id > cur_seg) { char p2[620]; seg_path(sl, ids[j].id, p2, sizeof p2); unlink(p2); }     // segments after the end of the prefix
        for (int j = 0; j < nids; j++) if (ids[j].id > cur_seg) nids = j;
        free(pg); free(locs);
        if (rc != SQLITE_OK) { free(ids); return rc; }
        atomic_store(&sh->seg_min, ids[0].id);
    }
    if (nids > 0) {                                                   // the tail of the last segment may hold a torn record and what a crash left behind: clear it, so that stale records can never join the log again
        char path[620]; seg_path(sl, cur_seg, path, sizeof path);
        int fd = open(path, O_RDWR); struct stat sb; int clr = SQLITE_OK;
        if (fd >= 0 && fstat(fd, &sb) == 0) {
            for (uint64_t off = cur_end; off < (uint64_t)sb.st_size; ) { uint64_t k = (uint64_t)sb.st_size - off < SEG_PREFILL_CHUNK ? (uint64_t)sb.st_size - off : SEG_PREFILL_CHUNK; if (pwrite_all(fd, sl->zeros, (size_t)k, (off_t)off) != SQLITE_OK) { clr = SQLITE_IOERR_WRITE; break; } off += k; }
            if (clr == SQLITE_OK && mw_io_fsync(fd) != 0) clr = SQLITE_IOERR_FSYNC;
        }
        if (fd >= 0) close(fd);
        if (clr != SQLITE_OK) { free(ids); return clr; }                                // (stale records that could not be cleared would rejoin the log behind the commits that reuse their epochs)
    }
    atomic_store(&sh->sl_seg, cur_seg);
    atomic_store(&sh->sl_end, cur_end);
    atomic_store(&sh->seg_next_ready, 0);
    atomic_store(&sh->log_ready, 0);
    free(ids);
    (void)first_epoch_of_cur;
    atomic_store(&sh->sy_done, MW_LOG_POS(cur_seg, cur_end));                         // (what recovery found is on disk)
    atomic_store(&sh->sy_leader, 0);
    if (base_out) *base_out = base;
    if (last_epoch_out) *last_epoch_out = last;
    return SQLITE_OK;
}

void mw_seglog_close (mw_db *db) {
    mw_seglog *sl = db->sl;
    if (!sl) return;
    for (int i = 0; i < MW_SEG_MAPS; i++) { segmap *m = &sl->maps[i]; if (m->base) munmap(m->base, m->len); if (m->fd >= 0) close(m->fd); }
    pthread_mutex_destroy(&sl->map_mu);
    free(sl->zeros);
    free(sl->prefix);
    free(sl);
    db->sl = NULL;
}

// MARK: - writer -

// Moves the cursor to a new segment (publication lock held). The previous one is made durable first: a commit that waits for a sync of the new segment must not lose
// an earlier one that sits in the old.
static int seg_roll (mw_db *db, mw_seglog *sl, uint32_t cur, uint64_t need, uint64_t first_epoch) {
    mw_shm *sh = db->shm;
    segmap *m = map_acquire(sl, cur);
    if (m) { mw_io_msync(m->base, m->len, MS_SYNC); if (mw_io_fsync(m->fd) != 0) { map_release(m); return SQLITE_IOERR_FSYNC; } map_release(m); }
    uint32_t next = cur + 1;
    uint64_t base = atomic_load(&sh->base_epoch);
    char final_[620]; seg_path(sl, next, final_, sizeof final_);
    uint64_t size = seg_size_for(sl, need);
    if (size == sl->seg_bytes && atomic_load(&sh->seg_next_ready) == next) {
        // prepared ahead: complete and in place already
    } else {
        // not ready (cold start, a fill that is behind, or a record that needs a segment of its own): make it here, under the lock
        for (int spin = 0; ; spin++) {                              // a filler is at work on the next segment: it finishes within a chunk; one that died is taken over
            int32_t owner = atomic_load(&sh->log_fill_pid);
            if (owner == 0) break;
            if (spin % 10 == 9 && owner != (int32_t)getpid() && !mw_mp_pid_alive(db, owner)) atomic_compare_exchange_strong(&sh->log_fill_pid, &owner, 0);
            struct timespec ts = { 0, 100000 }; nanosleep(&ts, NULL);
        }
        if (!(atomic_load(&sh->seg_next_ready) == next && size == sl->seg_bytes)) {
            unlink(final_);
            int rc = seg_create(sl, next, size, base, false, true);
            if (rc != SQLITE_OK) { atomic_store(&sh->seg_next_ready, 0); atomic_store(&sh->log_ready, 0); return rc; }       // (no room: the commit fails and the next one tries again)
        }
    }
    // the header of the new segment carries the current base (recovery takes the largest over the segments)
    { char p[620]; seg_path(sl, next, p, sizeof p); int fd = open(p, O_RDWR); if (fd >= 0) { write_hdr(sl, fd, atomic_load(&sh->base_epoch)); close(fd); } }
    first_epoch_set(sh, next, first_epoch);
    atomic_store(&sh->seg_next_ready, 0);
    atomic_store(&sh->log_ready, 0);
    // (the segment first, then the offset in it: a holder that dies between the two leaves the new segment with the old offset, which the repair of the next holder sees - there is no record at the head of the
    // new segment - and puts right; the other order would have left the old segment with an offset at its head, where the next record would overwrite the first ones)
    atomic_store_explicit(&sh->sl_seg, next, memory_order_release);
    atomic_store_explicit(&sh->sl_end, MW_SEG_HDR, memory_order_release);
    return SQLITE_OK;
}

int mw_seglog_append (mw_db *db, uint64_t epoch, uint32_t dbsize, int n, const uint32_t *pgnos, const uint8_t *const *images, const uint8_t *ext, uint32_t ext_len, const uint64_t *ch, uint64_t *locs, uint64_t *ext_loc, uint32_t *seg_out, uint64_t *end_out) {
    mw_seglog *sl = db->sl;
    mw_shm *sh = db->shm;
    if (mw_fault_hit(MW_FAULT_LOG_WRITE_ERR)) return SQLITE_IOERR_WRITE;
    uint32_t seg = atomic_load_explicit(&sh->sl_seg, memory_order_acquire);
    uint64_t off = atomic_load_explicit(&sh->sl_end, memory_order_acquire);
    uint64_t size = rec_size(sl, n, ext_len);
    segmap *m = map_acquire(sl, seg);
    if (!m) return SQLITE_CANTOPEN;
    if (off + size > m->len) {
        map_release(m);
        int rc = seg_roll(db, sl, seg, size, epoch);
        if (rc != SQLITE_OK) return rc;
        seg = atomic_load(&sh->sl_seg); off = MW_SEG_HDR;
        m = map_acquire(sl, seg);
        if (!m) return SQLITE_CANTOPEN;
        if (off + size > m->len) { map_release(m); return SQLITE_FULL; }
    }
    atomic_store_explicit(&sh->pend_seg, seg, memory_order_relaxed); atomic_store_explicit(&sh->pend_off, off, memory_order_relaxed);
    atomic_store_explicit(&sh->pend_epoch, epoch, memory_order_release);                 // (a publisher that dies from here until it has published is finished or undone by whoever takes the lock after it)
    size_t pgsz = sl->pgsz;
    uint8_t *p = m->base + off + REC_HDR_SIZE;
    uint64_t bh = BH_SEED;
    for (int i = 0; i < n; i++) {                                                  // body first, header (with the magic) last: an unfinished record does not validate
        memcpy(p, &pgnos[i], 4); memcpy(p + 4, images[i], pgsz);
        bh = bh_fold(bh, pgnos[i], ch && ch[i] ? ch[i] : mw_seglog_content_hash(images[i], pgsz));
        locs[i] = MW_LOC(seg, (uint64_t)(p - m->base) + 4);
        p += 4 + pgsz;
    }
    if (ext_len) { memcpy(p, ext, ext_len); bh = bh_fold(bh, BH_EXT_PGNO, fnv64(0x9FB21C651E98DF25ull, ext, ext_len)); }
    if (ext_loc) *ext_loc = MW_LOC(seg, (uint64_t)(p - m->base));
    if (mw_fault_hit(MW_CRASH_MID_LOG)) _exit(9);                                  // body written, header not: a torn record
    rec_hdr r = { .magic = REC_MAGIC, .npages = (uint32_t)n, .epoch = epoch, .dbsize = dbsize, .pgsz = (uint32_t)pgsz, .ext_len = ext_len, .cksum = 0 };
    r.cksum = rec_cksum_h(sl->salt, &r, bh);
    memcpy(m->base + off, &r, sizeof r);
    map_release(m);
    atomic_store_explicit(&sh->sl_end, off + size, memory_order_release);          // the cursor moves when the record is complete
    *seg_out = seg; *end_out = off + size;
    return SQLITE_OK;
}

uint64_t mw_seglog_bytes (mw_db *db) {
    mw_shm *sh = db->shm;
    uint32_t seg = atomic_load_explicit(&sh->sl_seg, memory_order_acquire), mn = atomic_load_explicit(&sh->seg_min, memory_order_relaxed);
    return (uint64_t)(seg - mn) * db->sl->seg_bytes + atomic_load_explicit(&sh->sl_end, memory_order_relaxed);
}

// MARK: - durability -

int mw_seglog_sync (mw_db *db, uint32_t seg, uint64_t end) {
    mw_seglog *sl = db->sl;
    mw_shm *sh = db->shm;
    const uint64_t target = MW_LOG_POS(seg, end);
    const int32_t me = (int32_t)getpid();
    for (;;) {
        if (atomic_load_explicit(&sh->sy_done, memory_order_acquire) >= target) return SQLITE_OK;
        if (atomic_load_explicit(&sh->sy_failed, memory_order_acquire)) return SQLITE_IOERR_FSYNC;
        int32_t exp = 0;
        if (atomic_compare_exchange_strong(&sh->sy_leader, &exp, me)) {
            // The leader: one fsync for every commit written so far, by any process (the processes share one active segment).
            uint32_t tseg = atomic_load_explicit(&sh->sl_seg, memory_order_acquire);
            uint64_t tend = atomic_load_explicit(&sh->sl_end, memory_order_acquire);          // (complete records only: the cursor moves after the header)
            uint64_t tpos = MW_LOG_POS(tseg, tend);
            if (tpos < target) { tseg = seg; tend = end; tpos = target; }
            int frc = 0;
            if (atomic_load_explicit(&sh->sy_done, memory_order_acquire) >= target) frc = 0;     // (somebody finished while we were taking over)
            else if (mw_fault_hit(MW_FAULT_LOG_SYNC_ERR)) frc = -1;
            else {
                segmap *m = map_acquire(sl, tseg);
                if (m) {
                    static long osp = 0; if (!osp) osp = sysconf(_SC_PAGESIZE);
                    uint64_t done = atomic_load(&sh->sy_done);
                    uint64_t from = MW_LOG_GEN(done) == tseg ? MW_LOG_END(done) : MW_SEG_HDR;         // (what was synced before is on disk)
                    from &= ~((uint64_t)osp - 1);
                    uint64_t to = (tend + (uint64_t)osp - 1) & ~((uint64_t)osp - 1); if (to > m->len) to = m->len;
                    frc = to > from ? mw_io_msync(m->base + from, (size_t)(to - from), MS_SYNC) : 0;
                    if (frc == 0) frc = mw_io_fsync(m->fd);
                    map_release(m);
                } else frc = -1;
            }
            if (frc == 0) {
                uint64_t cur = atomic_load(&sh->sy_done);
                while (cur < tpos && !atomic_compare_exchange_weak(&sh->sy_done, &cur, tpos)) {}
                atomic_fetch_add(&db->n_log_syncs, 1);
            }
            atomic_store_explicit(&sh->sy_leader, 0, memory_order_release);
            atomic_fetch_add_explicit(&sh->sy_wake, 1, memory_order_release);
            mw_wake_u32(&sh->sy_wake, true);
            if (frc != 0) { atomic_store_explicit(&sh->sy_failed, 1, memory_order_release); return SQLITE_IOERR_FSYNC; }
            continue;                                                                    // (re-check: normally done now)
        }
        // A follower: the leader's fsync (started after our record was written) covers us, or the next one will: sleep until a sync finishes.
        {
            uint32_t w = atomic_load_explicit(&sh->sy_wake, memory_order_acquire);
            if (atomic_load_explicit(&sh->sy_done, memory_order_acquire) >= target) return SQLITE_OK;
            if (atomic_load_explicit(&sh->sy_leader, memory_order_acquire) == 0) continue;               // (the leader finished between our two looks)
            mw_wait_u32(&sh->sy_wake, w, 2000);
            if (atomic_load_explicit(&sh->sy_wake, memory_order_acquire) == w) {                          // timed out: is the leader alive?
                int32_t ld = atomic_load(&sh->sy_leader);
                if (ld > 0 && ld != me && !mw_mp_pid_alive(db, ld)) atomic_compare_exchange_strong(&sh->sy_leader, &ld, 0);
            }
        }
    }
}

// MARK: - preparing the next segment -

void mw_seglog_prefill_bg (mw_db *db) {
    mw_seglog *sl = db->sl;
    mw_shm *sh = db->shm;
    uint32_t seg = atomic_load_explicit(&sh->sl_seg, memory_order_relaxed);
    uint64_t end = atomic_load_explicit(&sh->sl_end, memory_order_relaxed);
    if (end < sl->seg_bytes / 2 || atomic_load_explicit(&sh->seg_next_ready, memory_order_relaxed) == seg + 1) return;       // not yet, or done
    int32_t me = (int32_t)getpid(), exp = 0;
    if (!atomic_compare_exchange_strong(&sh->log_fill_pid, &exp, me)) return;                         // somebody else is filling
    uint32_t next = seg + 1;
    char tmp[640]; seg_path(sl, next, tmp, sizeof tmp); strncat(tmp, ".new", sizeof tmp - strlen(tmp) - 1);
    if (atomic_load(&sh->sl_seg) == seg && atomic_load(&sh->seg_next_ready) != next) {
        uint64_t done = atomic_load(&sh->log_ready);
        int fd = open(tmp, O_RDWR | O_NOFOLLOW | (done == 0 ? (O_CREAT | O_TRUNC) : 0), sl->mode);
        if (fd >= 0) {
            int rc = SQLITE_OK;
            if (done == 0) { rc = write_hdr(sl, fd, atomic_load(&sh->base_epoch)); done = MW_SEG_HDR; }
            const uint8_t *z = sl->zeros;
            if (rc == SQLITE_OK && done < sl->seg_bytes) {
                uint64_t k = sl->seg_bytes - done < SEG_PREFILL_CHUNK ? sl->seg_bytes - done : SEG_PREFILL_CHUNK;
                rc = pwrite_all(fd, z, (size_t)k, (off_t)done);
                if (rc == SQLITE_OK) done += k;
            }
            close(fd);
            if (rc == SQLITE_OK) {
                atomic_store(&sh->log_ready, done);
                if (done >= sl->seg_bytes) {
                    char fin[640]; seg_path(sl, next, fin, sizeof fin);
                    if (mw_io_rename(tmp, fin) == 0) { sync_dir_of(fin); atomic_store(&sh->seg_next_ready, next); }
                }
            }
        }
    }
    atomic_store(&sh->log_fill_pid, 0);
}

// MARK: - readers -

bool mw_seglog_read (mw_db *db, uint64_t loc, uint32_t off, uint32_t n, void *dst) {
    mw_seglog *sl = db->sl;
    segmap *m = map_acquire(sl, MW_LOC_SEG(loc));
    if (!m) return false;
    uint64_t at = (uint64_t)MW_LOC_OFF(loc) + off;
    if (at + n > m->len) { map_release(m); return false; }
    memcpy(dst, m->base + at, n);
    map_release(m);
    return true;
}

// A record at (seg, off), if it is complete and valid. Used to finish or discard what a publisher that died inside the publication lock left behind.
int mw_seglog_peek (mw_db *db, uint32_t seg, uint64_t off, uint64_t *epoch, uint32_t *dbsize, int *n, uint32_t **pgnos, uint64_t **locs, uint32_t *ext_len, uint64_t *ext_loc, uint64_t *size) {
    mw_seglog *sl = db->sl; int rc = SQLITE_NOTFOUND;
    segmap *m = map_acquire(sl, seg); if (!m) return SQLITE_NOTFOUND;
    rec_hdr r;
    if (off + REC_HDR_SIZE > m->len) goto out;
    memcpy(&r, m->base + off, sizeof r);
    if (r.magic != REC_MAGIC || r.pgsz != sl->pgsz || r.npages == 0 || r.npages > (1u << 24)) goto out;
    size_t body = (size_t)r.npages * (4 + (size_t)sl->pgsz) + r.ext_len;
    if (off + REC_HDR_SIZE + body > m->len) goto out;
    if (r.cksum != rec_cksum(sl->salt, &r, m->base + off + REC_HDR_SIZE, body)) goto out;
    uint32_t *pg = malloc((size_t)r.npages * sizeof *pg); uint64_t *lc = malloc((size_t)r.npages * sizeof *lc);
    if (!pg || !lc) { free(pg); free(lc); rc = SQLITE_NOMEM; goto out; }
    for (uint32_t k = 0; k < r.npages; k++) {
        const uint8_t *e = m->base + off + REC_HDR_SIZE + (size_t)k * (4 + sl->pgsz);
        memcpy(&pg[k], e, 4); lc[k] = MW_LOC(seg, (uint64_t)(e - m->base) + 4);
    }
    *epoch = r.epoch; *dbsize = r.dbsize; *n = (int)r.npages; *pgnos = pg; *locs = lc; *ext_len = r.ext_len;
    *ext_loc = MW_LOC(seg, off + REC_HDR_SIZE + (uint64_t)r.npages * (4 + sl->pgsz)); *size = REC_HDR_SIZE + body;
    rc = SQLITE_OK;
out:
    map_release(m);
    return rc;
}
// Forgets a record that was started and not finished (the header is cleared so that a recovery cannot take it for a commit).
void mw_seglog_discard (mw_db *db, uint32_t seg, uint64_t off) {
    segmap *m = map_acquire(db->sl, seg); if (!m) return;
    if (off + REC_HDR_SIZE <= m->len) memset(m->base + off, 0, REC_HDR_SIZE);
    map_release(m);
}

// MARK: - maintenance -

int mw_seglog_set_base (mw_db *db, uint64_t base) {
    mw_seglog *sl = db->sl;
    uint32_t s = atomic_load(&db->shm->seg_min);
    char path[620]; seg_path(sl, s, path, sizeof path);
    int fd = open(path, O_RDWR);
    if (fd < 0) return SQLITE_CANTOPEN;
    int rc = write_hdr(sl, fd, base);
    if (rc == SQLITE_OK && mw_io_fsync(fd) != 0) rc = SQLITE_IOERR_FSYNC;
    close(fd);
    return rc;
}

// The epoch that segment `seg` starts at: from the table when its slot is still its own, from the first record of its file when a later segment took the slot (more than 256 segments live). 0: unknown.
static uint64_t first_epoch_get (mw_db *db, uint32_t seg) {
    mw_shm *sh = db->shm;
    if (atomic_load_explicit(&sh->seg_first_id[seg % 256], memory_order_acquire) == seg) { uint64_t v = atomic_load_explicit(&sh->seg_first_epoch[seg % 256], memory_order_acquire); if (v) return v; }
    char path[620]; seg_path(db->sl, seg, path, sizeof path);
    int fd = open(path, O_RDONLY); if (fd < 0) return 0;
    rec_hdr r; uint64_t e = 0;
    if (pread_all(fd, &r, sizeof r, (off_t)MW_SEG_HDR) == SQLITE_OK && r.magic == REC_MAGIC && r.pgsz == db->sl->pgsz) e = r.epoch;
    close(fd);
    return e;
}

void mw_seglog_trim (mw_db *db, uint64_t base) {
    mw_seglog *sl = db->sl;
    mw_shm *sh = db->shm;
    uint32_t mn = atomic_load(&sh->seg_min), cur = atomic_load(&sh->sl_seg);
    // a segment holds nothing newer than `base` when the next one starts at an epoch <= base + 1
    while (mn < cur) {
        uint64_t next_first = first_epoch_get(db, mn + 1);
        if (next_first == 0 || next_first > base + 1) break;
        // the new oldest segment's header must carry the base before the old one goes (recovery reads it from there)
        char path[620]; seg_path(sl, mn + 1, path, sizeof path);
        int fd = open(path, O_RDWR);
        if (fd >= 0) { write_hdr(sl, fd, base); mw_io_fsync(fd); close(fd); }
        seg_path(sl, mn, path, sizeof path);
        unlink(path);
        mn++;
        atomic_store(&sh->seg_min, mn);
    }
    pthread_mutex_lock(&sl->map_mu);
    for (int i = 0; i < MW_SEG_MAPS; i++) {                                   // unmap what is gone: a use in flight blocks it until the next round
        segmap *m = &sl->maps[i];
        uint32_t s = atomic_load(&m->seg);
        if (s && s < mn) { atomic_store(&m->dying, 1); if (atomic_load(&m->users) == 0) map_drop(m); }
    }
    pthread_mutex_unlock(&sl->map_mu);
}
