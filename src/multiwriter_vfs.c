//
//  multiwriter_vfs.c
//  sqlite-multiwriter
//
//  Wrapper VFS: delegates every operation to the platform default VFS and, for now,
//  only counts/traces events. Follows the pattern of SQLite's own vfstrace.
//

#include <stdatomic.h>
#include <string.h>
#include "multiwriter_internal.h"
#include "multiwriter_io.h"

static sqlite3_vfs  mw_vfs;
static sqlite3_vfs *mw_root = NULL;
sqlite3_vfs *mw_root_vfs (void) { return mw_root; }
static _Atomic uint64_t mw_counts[MW_EV_COUNT];
static int          mw_default_enabled = 0;
static mw_trace_fn  mw_sink = NULL;
static void        *mw_sink_arg = NULL;

static const char *mw_names[MW_EV_COUNT] = {
    "open", "close", "read", "write", "sync", "truncate", "lock", "unlock", "shmmap",
    "shmlock", "shmbarrier", "shmunmap", "fetch", "unfetch", "delete", "filecontrol"
};

const char *mw_event_name (mw_event_t ev) { return (ev < MW_EV_COUNT) ? mw_names[ev] : "?"; }
// Event counters are batched per thread (a shared atomic per VFS call bounced its cache line between all lanes): a thread folds its counts into the
// global ones every 1024 events and when it exits; a reader sees the global counts plus its own pending ones.
static __thread uint64_t tl_cnt[MW_EV_COUNT];
static __thread unsigned tl_n;
static __thread int tl_registered;
static pthread_key_t tl_key;
static pthread_once_t tl_once = PTHREAD_ONCE_INIT;
static void tl_flush (void) {
    for (int i = 0; i < MW_EV_COUNT; i++) if (tl_cnt[i]) { atomic_fetch_add_explicit(&mw_counts[i], tl_cnt[i], memory_order_relaxed); tl_cnt[i] = 0; }
    tl_n = 0;
}
static void tl_exit (void *unused) { (void)unused; tl_flush(); }
static void tl_init (void) { pthread_key_create(&tl_key, tl_exit); }
uint64_t mw_vfs_event_count (mw_event_t ev) { return (ev < MW_EV_COUNT) ? atomic_load(&mw_counts[ev]) + tl_cnt[ev] : 0; }
void mw_vfs_events_reset (void) { for (int i = 0; i < MW_EV_COUNT; ++i) { atomic_store(&mw_counts[i], 0); tl_cnt[i] = 0; } tl_n = 0; }
// Statement hook (sqlite3_trace_v2): recognises DDL/VACUUM, raises the schema barrier before it runs and gives it back, if it is still idle, when the statement ends.
static bool mw_is_ddl_sql (const char *sql) {
    sql = mw_sql_skip(sql);
    static const char *kw[] = { "CREATE", "DROP", "ALTER", "REINDEX", "VACUUM" };
    for (unsigned i = 0; i < sizeof(kw) / sizeof(kw[0]); i++) if (sqlite3_strnicmp(sql, kw[i], (int)strlen(kw[i])) == 0) return true;
    return false;
}
static int mw_trace_cb (unsigned type, void *ctx, void *p, void *x) {
    (void)x;
    if (type != SQLITE_TRACE_STMT && type != SQLITE_TRACE_PROFILE) return 0;
    sqlite3_stmt *st = (sqlite3_stmt *)p;
    if (ctx) {                                                  // a mw_rebase=1 connection: what the transaction reads (mw_lane_reads_unchanged)
        mw_stmt_note n = { type == SQLITE_TRACE_PROFILE, sqlite3_get_autocommit(sqlite3_db_handle(st)), st };
        sqlite3_file_control(sqlite3_db_handle(st), "main", MW_FCNTL_STMT, &n);
    }
    const char *sql = sqlite3_sql(st);
    if (!sql || !mw_is_ddl_sql(sql)) return 0;
    // at the start of a schema change the barrier is raised; at its end it is given back if no snapshot of the main file is open (a change of a temporary object never opens one, and
    // nothing else would end the barrier)
    sqlite3_file_control(sqlite3_db_handle(st), "main", type == SQLITE_TRACE_STMT ? MW_FCNTL_DDL_BEGIN : MW_FCNTL_DDL_RELEASE_IDLE, NULL);
    return 0;
}

static int mw_connection_init (sqlite3 *db, char **err, const sqlite3_api_routines *api) {
    (void)err; (void)api;
    const char *fn = sqlite3_db_filename(db, "main");
    sqlite3_trace_v2(db, SQLITE_TRACE_STMT | SQLITE_TRACE_PROFILE, mw_trace_cb, (fn && *fn && sqlite3_uri_boolean(fn, "mw_rebase", 0)) ? (void *)1 : NULL);
    return SQLITE_OK;
}

void mw_vfs_set_enabled_default (int enabled) { mw_default_enabled = enabled; }
void mw_vfs_set_trace (mw_trace_fn fn, void *arg) { mw_sink_arg = arg; mw_sink = fn; }

void mw_ev (mw_event_t e, mw_file *f, int64_t a, int64_t b, int flags) {
    if (!tl_registered) { pthread_once(&tl_once, tl_init); pthread_setspecific(tl_key, &tl_registered); tl_registered = 1; }
    tl_cnt[e]++;
    if (++tl_n >= 1024) tl_flush();
    mw_trace_fn fn = mw_sink;
    if (fn) fn(mw_sink_arg, e, f ? f->name : NULL, a, b, flags);
}

#define REAL(p) (((mw_file *)(p))->real)

// MARK: - sqlite3_io_methods -

static int mw_close (sqlite3_file *pf) {
    mw_file *f = (mw_file *)pf;
    mw_ev(MW_EV_CLOSE, f, 0, 0, 0);
    if (f->lane) {
        mw_lane_snapshot_end(f->lane);
        mw_db *db = f->lane->db;
        mw_lane_free(f->lane);
        f->lane = NULL;
        mw_db_release(db);
    }
    int rc = f->real->pMethods ? f->real->pMethods->xClose(f->real) : SQLITE_OK;
    f->base.pMethods = NULL;
    return rc;
}
static int mw_read (sqlite3_file *pf, void *buf, int n, sqlite3_int64 off) {
    mw_ev(MW_EV_READ, (mw_file *)pf, off, n, 0);
    return REAL(pf)->pMethods->xRead(REAL(pf), buf, n, off);
}
static int mw_write (sqlite3_file *pf, const void *buf, int n, sqlite3_int64 off) {
    mw_ev(MW_EV_WRITE, (mw_file *)pf, off, n, 0);
    return REAL(pf)->pMethods->xWrite(REAL(pf), buf, n, off);
}
static int mw_truncate (sqlite3_file *pf, sqlite3_int64 size) {
    mw_ev(MW_EV_TRUNCATE, (mw_file *)pf, size, 0, 0);
    return REAL(pf)->pMethods->xTruncate(REAL(pf), size);
}
static int mw_sync (sqlite3_file *pf, int flags) {
    mw_ev(MW_EV_SYNC, (mw_file *)pf, 0, 0, flags);
    return REAL(pf)->pMethods->xSync(REAL(pf), flags);
}
static int mw_filesize (sqlite3_file *pf, sqlite3_int64 *size) {
    return REAL(pf)->pMethods->xFileSize(REAL(pf), size);
}
static int mw_lock (sqlite3_file *pf, int l) {
    mw_ev(MW_EV_LOCK, (mw_file *)pf, l, 0, 0);
    return REAL(pf)->pMethods->xLock(REAL(pf), l);
}
static int mw_unlock (sqlite3_file *pf, int l) {
    mw_ev(MW_EV_UNLOCK, (mw_file *)pf, l, 0, 0);
    return REAL(pf)->pMethods->xUnlock(REAL(pf), l);
}
static int mw_check_reserved (sqlite3_file *pf, int *out) {
    return REAL(pf)->pMethods->xCheckReservedLock(REAL(pf), out);
}
static int mw_file_control (sqlite3_file *pf, int op, void *arg) {
    mw_ev(MW_EV_FILECONTROL, (mw_file *)pf, op, 0, 0);
    mw_lane *lane = ((mw_file *)pf)->lane;
    if (lane && op == MW_FCNTL_TXINFO) { *(mw_tx_info *)arg = lane->tx; return SQLITE_OK; }
    if (lane && op == MW_FCNTL_DBSTATS) { mw_lane_fill_stats(lane, (mw_db_stats *)arg); return SQLITE_OK; }
    int rc = REAL(pf)->pMethods->xFileControl(REAL(pf), op, arg);
    // SQLITE_FCNTL_VFSNAME returns a string owned by the caller: prefix it with ours.
    if (op == SQLITE_FCNTL_VFSNAME && rc == SQLITE_OK) {
        char **pz = (char **)arg;
        *pz = sqlite3_mprintf("%s/%z", mw_vfs.zName, *pz);
    }
    return rc;
}
static int mw_sector_size (sqlite3_file *pf) { return REAL(pf)->pMethods->xSectorSize(REAL(pf)); }
static int mw_device_char (sqlite3_file *pf) { return REAL(pf)->pMethods->xDeviceCharacteristics(REAL(pf)); }

static int mw_shm_map (sqlite3_file *pf, int page, int size, int extend, void volatile **pp) {
    mw_ev(MW_EV_SHMMAP, (mw_file *)pf, page, size, extend);
    int rc = REAL(pf)->pMethods->xShmMap(REAL(pf), page, size, extend, pp);
    mw_lane *lane = ((mw_file *)pf)->lane;
    if (lane && page == 0 && rc == SQLITE_OK) lane->shm0 = (volatile uint8_t *)*pp;
    return rc;
}
static int mw_shm_lock (sqlite3_file *pf, int ofst, int n, int flags) {
    mw_ev(MW_EV_SHMLOCK, (mw_file *)pf, ofst, n, flags);
    int rc = REAL(pf)->pMethods->xShmLock(REAL(pf), ofst, n, flags);
    mw_lane *lane = ((mw_file *)pf)->lane;
    if (lane && rc == SQLITE_OK && n == 1) rc = mw_lane_on_shm_lock(lane, ofst, flags);
    return rc;
}
static void mw_shm_barrier (sqlite3_file *pf) {
    mw_ev(MW_EV_SHMBARRIER, (mw_file *)pf, 0, 0, 0);
    REAL(pf)->pMethods->xShmBarrier(REAL(pf));
}
static int mw_shm_unmap (sqlite3_file *pf, int del) {
    mw_ev(MW_EV_SHMUNMAP, (mw_file *)pf, del, 0, 0);
    return REAL(pf)->pMethods->xShmUnmap(REAL(pf), del);
}
static int mw_fetch (sqlite3_file *pf, sqlite3_int64 off, int n, void **pp) {
    mw_ev(MW_EV_FETCH, (mw_file *)pf, off, n, 0);
    return REAL(pf)->pMethods->xFetch(REAL(pf), off, n, pp);
}
static int mw_unfetch (sqlite3_file *pf, sqlite3_int64 off, void *p) {
    mw_ev(MW_EV_UNFETCH, (mw_file *)pf, off, 0, 0);
    return REAL(pf)->pMethods->xUnfetch(REAL(pf), off, p);
}

static const sqlite3_io_methods mw_io = {
    3, mw_close, mw_read, mw_write, mw_truncate, mw_sync, mw_filesize, mw_lock, mw_unlock,
    mw_check_reserved, mw_file_control, mw_sector_size, mw_device_char,
    mw_shm_map, mw_shm_lock, mw_shm_barrier, mw_shm_unmap, mw_fetch, mw_unfetch
};

// MARK: - sqlite3_vfs -

const sqlite3_io_methods *mw_vfs_pass_io (void) { return &mw_io; }

static const sqlite3_io_methods *mw_pass_methods (sqlite3_file *real) {
    // Expose only the io-method version the real file supports: xShm* needs >=2, xFetch >=3.
    // A wrapper must not advertise methods the underlying file lacks.
    static sqlite3_io_methods v1, v2;
    static int init = 0;
    if (!init) { v1 = mw_io; v1.iVersion = 1; v2 = mw_io; v2.iVersion = 2; init = 1; }
    if (real->pMethods == NULL) return &mw_io;
    return (real->pMethods->iVersion >= 3) ? &mw_io : (real->pMethods->iVersion == 2) ? &v2 : &v1;
}

// URI mw=0: stock passthrough; mw=1: the engine (private lanes); mw=2: experimental (lane tracking on the stock shared WAL). true/on/yes and false/off/no are accepted for 1 and 0.
// Inside, the engine is mode 2 and the tracking mode 1 (the numbers the code compares against). Returns -1 for a value that is none of these: the open fails, a typo must not give stock SQLite.
static int mw_mode_for (const char *name, int flags) {
    if (!name || !(flags & SQLITE_OPEN_MAIN_DB)) return 0;
    const char *v = sqlite3_uri_parameter(name, "mw");
    int uri_mode;
    if (!v) {
        // vfs=multiwriter in the URI turns the engine on (mw=0 opts out). Not every open through the VFS: the database that VACUUM INTO or ATTACH opens has a plain name and stays as stock.
        const char *vfs = sqlite3_uri_parameter(name, "vfs");
        uri_mode = (vfs && !strcmp(vfs, "multiwriter")) ? 1 : mw_default_enabled;
    }
    else if (!strcmp(v, "0") || !sqlite3_stricmp(v, "false") || !sqlite3_stricmp(v, "off") || !sqlite3_stricmp(v, "no")) uri_mode = 0;
    else if (!strcmp(v, "1") || !sqlite3_stricmp(v, "true") || !sqlite3_stricmp(v, "on") || !sqlite3_stricmp(v, "yes")) uri_mode = 1;
    else if (!strcmp(v, "2")) uri_mode = 2;
    else return -1;
    return uri_mode == 1 ? 2 : uri_mode == 2 ? 1 : 0;
}

static int mw_open (sqlite3_vfs *vfs, const char *name, sqlite3_file *pf, int flags, int *pout) {
    mw_file *f = (mw_file *)pf;
    memset(f, 0, sizeof(*f));
    f->real = (sqlite3_file *)&f[1];
    f->name = name;

    // the WAL of a private-lane database lives in memory only
    if ((flags & SQLITE_OPEN_WAL) && name && mw_db_find_by_wal(name)) {
        mw_ev(MW_EV_OPEN, f, 0, 0, flags);
        if (pout) *pout = flags;
        return mw_lane_open_wal(f, name);
    }

    int mode = mw_mode_for(name, flags);
    if (mode < 0) { sqlite3_log(SQLITE_CANTOPEN, "multiwriter: mw=%s is not 0, 1 or 2", sqlite3_uri_parameter(name, "mw")); return SQLITE_CANTOPEN; }
    if (mode >= 2 && name) {
        const char *cache = sqlite3_uri_parameter(name, "cache");
        if (cache && !strcmp(cache, "shared")) return SQLITE_CANTOPEN;          // shared-cache shares one pager between connections: no private lanes
    }
    if (mode >= 2 && name && (flags & SQLITE_OPEN_READONLY) && !mw_path_is_clean_wal_db(name)) mode = 0;       // (a read-only connection must not write to the file: it cannot be converted to WAL, and it reads as stock does)
    if (mode >= 2 && name && !(flags & SQLITE_OPEN_CREATE) && access(name, F_OK) != 0) return SQLITE_CANTOPEN;      // (the conversion to WAL below would create the file that the application asked not to create)
    if (mode >= 2) {
        int rc = mw_ensure_wal_db(name);      // private lanes require a WAL-mode database file
        if (rc != SQLITE_OK) return rc;
    }
    int rc = mw_root->xOpen(mw_root, name, f->real, flags, pout);
    if (rc != SQLITE_OK) return rc;
    mw_ev(MW_EV_OPEN, f, 0, 0, flags);
    f->base.pMethods = mw_pass_methods(f->real);

    if (mode >= 1) {
        // mw_mp: 0/1 and true/on/yes, false/off/no, as mw= (a value that is none of them fails the open: "true" read as 0 made the process run its own engine on a file that others share)
        bool mpmode = false;
        if (mode >= 2) {
            const char *mv = sqlite3_uri_parameter(name, "mw_mp");
            if (mv) {
                if (!strcmp(mv, "0") || !sqlite3_stricmp(mv, "false") || !sqlite3_stricmp(mv, "off") || !sqlite3_stricmp(mv, "no")) mpmode = false;
                else if (!sqlite3_stricmp(mv, "true") || !sqlite3_stricmp(mv, "on") || !sqlite3_stricmp(mv, "yes") || (*mv && !mv[strspn(mv, "0123456789")])) mpmode = true;   // (any number but 0)
                else { sqlite3_log(SQLITE_CANTOPEN, "multiwriter: mw_mp=%s is not a number or a boolean", mv); f->real->pMethods->xClose(f->real); f->base.pMethods = NULL; return SQLITE_CANTOPEN; }
            }
        }   // (only the engine has the shared header and the segmented log: the tracking mode, mw=2, is one process)
        int aerr = SQLITE_NOMEM;
        mw_db *db = mw_db_acquire(name, mode, mpmode, &aerr);
        mw_lane *lane = db ? sqlite3_malloc(sizeof(mw_lane)) : NULL;
        if (!lane) {
            mw_db_release(db);
            f->real->pMethods->xClose(f->real);
            f->base.pMethods = NULL;
            return db ? SQLITE_NOMEM : aerr;
        }
        mw_lane_init(lane, db);
        if (sqlite3_uri_parameter(name, "mw_fullfsync") ? sqlite3_uri_boolean(name, "mw_fullfsync", 0) : (getenv("MW_FULLFSYNC") != NULL)) mw_set_fullfsync(1);
        // mw_profile=small (or MW_PROFILE=small): for a phone or a small server, where 300 MB for a process is too much: the caches (memory table of the metadata, pages of the real file kept in memory) at 8 MB
        // instead of 64, filters of 8 bits a row, a log of 16 MB before it is compacted. Every one of them can still be set by its own parameter.
        const char *prof = sqlite3_uri_parameter(name, "mw_profile"); if (!prof) prof = getenv("MW_PROFILE");
        const bool small = prof && !strcmp(prof, "small");
        if (small || sqlite3_uri_parameter(name, "mw_base_cache_mb")) { db->base_cache_bytes = (uint64_t)sqlite3_uri_int64(name, "mw_base_cache_mb", small ? 8 : 64) << 20; if (db->store) db->store->base_limit = db->base_cache_bytes; }
        if (small && !sqlite3_uri_parameter(name, "mw_log_max_mb")) db->log_max_bytes = 16ull << 20;
        lane->norebase = sqlite3_uri_boolean(name, "mw_norebase", 0) != 0;
        lane->rebase_on = sqlite3_uri_boolean(name, "mw_rebase", 0) != 0 && !lane->norebase;
        lane->rb_nobackoff = !sqlite3_uri_boolean(name, "mw_rebase_backoff", 1);
        lane->noreloc = sqlite3_uri_boolean(name, "mw_noreloc", 0) != 0;
        lane->noroute = sqlite3_uri_boolean(name, "mw_noroute", 0) != 0;
        lane->nomerge = sqlite3_uri_boolean(name, "mw_nomerge", 0) != 0;
        lane->prep_delay_us = (int)sqlite3_uri_int64(name, "mw_prep_delay_us", 0);
        if (sqlite3_uri_parameter(name, "mw_hot_credit")) lane->hot_credit = (int)sqlite3_uri_int64(name, "mw_hot_credit", 16);
        lane->readcheck = sqlite3_uri_boolean(name, "mw_readcheck", 1) != 0;
        f->lane = lane;
        // GC every N publications (default 64); mw_gc=0 disables it. Only an explicit parameter changes it.
        if (sqlite3_uri_parameter(name, "mw_gc")) db->gc_interval = (int)sqlite3_uri_int64(name, "mw_gc", 64);
        if (mode >= 2) {
            // The schema-barrier trace hook must be on every private-lane connection. Auto-extensions run right
            // after xOpen inside sqlite3_open_v2(), so registering at the first private-lane open is in time for it
            // (and keeps stock/passthrough users free of the global registration).
            static int hook_registered = 0;
            if (!hook_registered) { sqlite3_auto_extension((void (*)(void))mw_connection_init); hook_registered = 1; }
            lane->private_mode = true;
            rc = mw_lane_open_main(f, lane);
            if (rc == SQLITE_OK) {
                db->open_done = true;
                int ms = (int)sqlite3_uri_int64(name, "mw_compact_ms", db->mp_req ? 1000 : 0);   // multi-process: the log only shrinks through compaction
                mw_db_compactor_start(db, ms);                       // always: event-driven by log size; periodic if mw_compact_ms > 0
                if (sqlite3_uri_parameter(name, "mw_log_max_mb")) db->log_max_bytes = (uint64_t)sqlite3_uri_int64(name, "mw_log_max_mb", 0) << 20;
            }
            if (rc != SQLITE_OK) {
                f->lane = NULL;
                mw_lane_free(lane);
                mw_db_release(db);
                f->real->pMethods->xClose(f->real);
                f->base.pMethods = NULL;
                return rc;
            }
        }
    }
    return SQLITE_OK;
}

static int mw_delete (sqlite3_vfs *vfs, const char *name, int sync) {
    mw_ev(MW_EV_DELETE, NULL, sync, 0, 0);
    if (name && mw_db_find_by_wal(name)) return SQLITE_OK;
    return mw_root->xDelete(mw_root, name, sync);
}
static int mw_access (sqlite3_vfs *vfs, const char *name, int flags, int *out) {
    return mw_root->xAccess(mw_root, name, flags, out);
}
static int mw_fullpath (sqlite3_vfs *vfs, const char *name, int n, char *out) {
    return mw_root->xFullPathname(mw_root, name, n, out);
}
static void *mw_dlopen (sqlite3_vfs *vfs, const char *name) { return mw_root->xDlOpen(mw_root, name); }
static void mw_dlerror (sqlite3_vfs *vfs, int n, char *msg) { mw_root->xDlError(mw_root, n, msg); }
static void (*mw_dlsym (sqlite3_vfs *vfs, void *h, const char *s))(void) { return mw_root->xDlSym(mw_root, h, s); }
static void mw_dlclose (sqlite3_vfs *vfs, void *h) { mw_root->xDlClose(mw_root, h); }
static int mw_randomness (sqlite3_vfs *vfs, int n, char *out) { return mw_root->xRandomness(mw_root, n, out); }
static int mw_sleep (sqlite3_vfs *vfs, int us) { return mw_root->xSleep(mw_root, us); }
static int mw_currenttime (sqlite3_vfs *vfs, double *t) { return mw_root->xCurrentTime(mw_root, t); }
static int mw_lasterror (sqlite3_vfs *vfs, int n, char *msg) { return mw_root->xGetLastError(mw_root, n, msg); }
static int mw_currenttime64 (sqlite3_vfs *vfs, sqlite3_int64 *t) {
    return mw_root->iVersion >= 2 && mw_root->xCurrentTimeInt64 ? mw_root->xCurrentTimeInt64(mw_root, t) : SQLITE_ERROR;
}

int mw_vfs_register (int make_default) {
    if (mw_root) return sqlite3_vfs_register(&mw_vfs, make_default);   // idempotent
    mw_lane_methods_init(&mw_io);
    mw_root = sqlite3_vfs_find(NULL);
    if (!mw_root) return SQLITE_ERROR;
    mw_vfs = *mw_root;
    mw_vfs.zName = MW_VFS_NAME;
    mw_vfs.pNext = NULL;
    mw_vfs.pAppData = NULL;
    mw_vfs.szOsFile = (int)(sizeof(mw_file) + mw_root->szOsFile);
    mw_vfs.xOpen = mw_open;
    mw_vfs.xDelete = mw_delete;
    mw_vfs.xAccess = mw_access;
    mw_vfs.xFullPathname = mw_fullpath;
    mw_vfs.xDlOpen = mw_dlopen; mw_vfs.xDlError = mw_dlerror; mw_vfs.xDlSym = mw_dlsym; mw_vfs.xDlClose = mw_dlclose;
    mw_vfs.xRandomness = mw_randomness; mw_vfs.xSleep = mw_sleep;
    mw_vfs.xCurrentTime = mw_currenttime; mw_vfs.xGetLastError = mw_lasterror;
    mw_vfs.xCurrentTimeInt64 = mw_currenttime64;
    // SetSystemCall & co. are left as inherited from the root VFS.
    return sqlite3_vfs_register(&mw_vfs, make_default);
}

int mw_vfs_unregister (void) {
    return sqlite3_vfs_unregister(&mw_vfs);
}
