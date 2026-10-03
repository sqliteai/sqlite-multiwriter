//
//  multiwriter_db.c
//  cloudsync
//
//  Per-database state shared by every connection (lane) of this process that opened the
//  same file. The registry is keyed by the full path SQLite hands to xOpen.
//

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <sys/mman.h>
#include <stdio.h>
#include <unistd.h>
#include "multiwriter_internal.h"

static mw_db *mw_dbs = NULL;                 // protected by the SQLITE_MUTEX_STATIC_MAIN mutex

// fork(): only the calling thread survives in the child, so the compactor thread of every inherited database is gone, its mutexes may be held by
// threads that no longer exist, and the shared-memory / lock file descriptors are shared with the parent. The child must therefore never reuse an
// inherited database: they are marked orphaned (skipped by the registry, never torn down: closing an inherited connection must not compact, unlink or
// unlock what the parent still uses) and connections opened in the child get a fresh state of their own. Connections inherited across fork() must
// not be used in the child (as with any SQLite connection).
static void mw_atfork_child (void) { for (mw_db *d = mw_dbs; d; d = d->next) d->orphaned = true; }
static int mw_atfork_done;

// sys: the connection belongs to the metadata store. It is counted together with the reference, under the same mutex: the last connection of the application that closes meanwhile
// must see both or neither (a count that lags makes it miss that only helper connections are left, and the database is never torn down).
mw_db *mw_db_acquire (const char *path, int mode, int mpmode, bool sys) {
    const bool mp = mpmode != 0, shared = mpmode == 2;
    sqlite3_mutex *g = sqlite3_mutex_alloc(SQLITE_MUTEX_STATIC_MAIN);
    sqlite3_mutex_enter(g);
    if (!mw_atfork_done) { pthread_atfork(NULL, NULL, mw_atfork_child); mw_atfork_done = 1; }
    mw_db *db = mw_dbs;
    while (db && (db->orphaned || strcmp(db->path, path) != 0)) db = db->next;
    if (db) {
        if (db->mode != mode || (mode >= 2 && (db->mp_req != mp || db->shared != shared))) db = NULL;     // one database, one mode (multi-process is a property of the database)
        else { db->refs++; if (sys) atomic_fetch_add(&db->sys_refs, 1); }
    } else if ((db = sqlite3_malloc(sizeof(*db))) != NULL) {
        memset(db, 0, sizeof(*db));
        db->path = sqlite3_mprintf("%s", path);
        db->mu = sqlite3_mutex_alloc(SQLITE_MUTEX_FAST);
        if (!db->path || !db->mu) {
            sqlite3_free(db->path);
            sqlite3_free(db);
            db = NULL;
        } else {
            db->refs = 1;
            if (sys) atomic_fetch_add(&db->sys_refs, 1);
            db->gc_interval = 64;                    // (the default is set here, not by whichever open sees refs == 1: with concurrent first opens none might, and GC would stay off)
            db->mode = mode;
            db->mp_req = mp;
            db->shared = shared;
            atomic_init(&db->epoch, 1);           // epoch 1 = the state found on disk at first open
            atomic_init(&db->next_tx_id, 1);
            atomic_init(&db->next_writer_id, 1);
            atomic_init(&db->schema_generation, 1);
            db->logfd = -1;
            db->mp_lockfd = -1;
            db->mp_pubfd = -1;
            db->mp_proc = -1;
            db->fd_real = -1;
            pthread_mutex_init(&db->log_mu, NULL);
            pthread_mutex_init(&db->compact_mu, NULL);
            pthread_mutex_init(&db->rebase_mu, NULL);
            pthread_mutex_init(&db->hot_mu, NULL);
            pthread_mutex_init(&db->reloc_mu, NULL);
            pthread_cond_init(&db->hot_cv, NULL);
            pthread_mutex_init(&db->admit_mu, NULL);
            pthread_cond_init(&db->admit_cv, NULL);
            pthread_mutex_init(&db->compactor_mu, NULL);
            pthread_cond_init(&db->compactor_cv, NULL);
            db->log_max_bytes = 32ull << 20;
            pthread_cond_init(&db->sync_cv, NULL);
            db->vis = sqlite3_malloc(MW_VIS_SLOTS * sizeof(struct mw_vis_slot));
            for (int i = 0; db->vis && i < MW_VIS_SLOTS; i++) { pthread_mutex_init(&db->vis[i].mu, NULL); pthread_cond_init(&db->vis[i].cv, NULL); atomic_init(&db->vis[i].waiters, 0); atomic_init(&db->vis[i].ready, 0); }
            pthread_mutex_init(&db->ddl_mu, NULL);
            pthread_cond_init(&db->ddl_cv, NULL);
            db->next = mw_dbs;
            mw_dbs = db;
        }
    }
    sqlite3_mutex_leave(g);
    return db;
}


// The path of one of the files the processes of the shared mode map and lock together (suffix "mwidx", "mwrow", "mwown", "mwlock", "mwlk"). They are sparse files of some hundred MB of which a few
// pages are touched, and what they hold is rebuilt by the first process that opens the database. On Linux a first touch of a page of a sparse file on a full file system is a SIGBUS (not an
// error), so there they live in /dev/shm (MW_SIDECAR_DIR changes it, "db" puts them next to the database, as on macOS, where the same test did not fault); the name carries a hash of the
// database's path. The caller frees the result with sqlite3_free.
char *mw_sidecar_path (const char *dbpath, const char *suffix) {
    const char *dir = getenv("MW_SIDECAR_DIR");
#if defined(__linux__)
    if (!dir) dir = access("/dev/shm", W_OK) == 0 ? "/dev/shm" : "db";
#else
    if (!dir) dir = "db";
#endif
    if (!strcmp(dir, "db")) return sqlite3_mprintf("%s-%s", dbpath, suffix);
    uint64_t h = 1469598103934665603ull; for (const char *c = dbpath; *c; c++) { h ^= (unsigned char)*c; h *= 1099511628211ull; }
    return sqlite3_mprintf("%s/mw-%016llx-%s", dir, (unsigned long long)h, suffix);
}

void mw_db_release_ex (mw_db *db, bool sys);
void mw_db_release (mw_db *db) { mw_db_release_ex(db, false); }

// sys: the connection being closed belongs to the metadata store. When the last connection of the application goes, the metadata store flushes and closes its own first
// (while this reference still keeps the database alive): the log is about to be dropped, and the metadata of the commits in it exists nowhere else.
void mw_db_release_ex (mw_db *db, bool sys) {
    if (!db) return;
    sqlite3_mutex *g = sqlite3_mutex_alloc(SQLITE_MUTEX_STATIC_MAIN);
    sqlite3_mutex_enter(g);
    if (!sys && db->cdc && db->open_done && !db->orphaned && db->refs - 1 == atomic_load(&db->sys_refs)) {
        sqlite3_mutex_leave(g);
        mw_cdc_quiesce(db);
        sqlite3_mutex_enter(g);
    }
    if (sys) atomic_fetch_sub(&db->sys_refs, 1);
    if (--db->refs == 0) {
        for (mw_db **pp = &mw_dbs; *pp; pp = &(*pp)->next) {
            if (*pp == db) { *pp = db->next; break; }
        }
        if (db->orphaned) { sqlite3_mutex_leave(g); return; }        // (inherited through fork(): unlinked from the registry, deliberately leaked)
        assert(db->active == NULL);
        // last connection gone: stop the compactor, materialise everything into the real file (it is then an
        // ordinary SQLite database again) and drop the log. If the database failed, the log is kept for recovery.
        mw_db_compactor_stop(db);
        mw_cdc_close(db);
        for (int i = 0; i < db->nrext; i++) free(db->rext[i].data);
        free(db->rext); db->rext = NULL; db->nrext = db->caprext = 0;
        bool clean = false, sole = true;
        if (db->shared) {
            mw_mp_close(db, &sole);
            // the last process leaves a log that is entirely in the real file (nobody else exists: no snapshot holds the target back)
            for (int tries = 0; sole && tries < 3 && db->store && db->ix && db->sl && !atomic_load(&db->failed) && atomic_load(&db->shm->base_epoch) < atomic_load(&db->shm->committed_epoch); tries++) mw_db_compact(db, NULL);
            mw_shared_close(db, sole);
            if (db->fd_real >= 0) { close(db->fd_real); db->fd_real = -1; }
            mw_log_close(db, false);
            goto shared_cleanup;
        }
        if (db->mp) {
            mw_mp_close(db, &sole);                                     // *sole: we are the last process (and hold the lock exclusively)
            if (sole) {
                pthread_mutex_lock(&db->mp_mu); mw_mp_catchup_locked(db); pthread_mutex_unlock(&db->mp_mu);
                db->base_epoch = atomic_load(&db->shm->base_epoch);
                db->mp = false;                                          // from here on: the ordinary single-process close
                mw_store_materialize_lazy(db->store);                     // (the ordinary close truncates the log in place; lazy versions point into it)
            }
        }
        if (sole && db->store && db->logfd >= 0 && !atomic_load(&db->failed)) {
            mw_compact_result r;
            clean = mw_db_compact(db, &r) == SQLITE_OK && db->base_epoch == atomic_load(&db->epoch) && atomic_load(&db->next_epoch) == atomic_load(&db->epoch);
        }
        if (db->fd_real >= 0) { close(db->fd_real); db->fd_real = -1; }
        mw_log_close(db, sole && clean);
    shared_cleanup:
        if (db->mp_lockfd >= 0) {
            if (sole && db->mp_path) unlink(db->mp_path);
            if (sole && db->mp_pubpath) unlink(db->mp_pubpath);
            if (db->mp_pubfd >= 0) close(db->mp_pubfd);
            sqlite3_free(db->mp_pubpath);
            if (db->shm) munmap(db->shm, (sizeof(mw_shm) + 4095) & ~(size_t)4095);
            close(db->mp_lockfd);
            sqlite3_free(db->mp_path);
            pthread_mutex_destroy(&db->mp_mu);
        }
        pthread_mutex_destroy(&db->compact_mu);
        pthread_mutex_destroy(&db->rebase_mu);
        pthread_mutex_destroy(&db->hot_mu);
        pthread_mutex_destroy(&db->reloc_mu);
        pthread_cond_destroy(&db->hot_cv);
        pthread_mutex_destroy(&db->admit_mu);
        pthread_cond_destroy(&db->admit_cv);
        pthread_mutex_destroy(&db->compactor_mu);
        pthread_cond_destroy(&db->compactor_cv);
        pthread_mutex_destroy(&db->log_mu);
        pthread_cond_destroy(&db->sync_cv);
        for (int i = 0; db->vis && i < MW_VIS_SLOTS; i++) { pthread_mutex_destroy(&db->vis[i].mu); pthread_cond_destroy(&db->vis[i].cv); }
        sqlite3_free(db->vis);
        free(db->p1_cache);
        for (int i = 0; i < db->nretired; i++) mw_store_free(db->retired[i]);
        mw_store_free(db->store);
        pthread_cond_destroy(&db->ddl_cv);
        pthread_mutex_destroy(&db->ddl_mu);
        sqlite3_mutex_free(db->mu);
        sqlite3_free(db->path);
        sqlite3_free(db);
    }
    sqlite3_mutex_leave(g);
}

// The private WAL of a database opened in private-lane mode is memory only: xOpen/xDelete use
// this to recognise "<path>-wal".
mw_db *mw_db_find_by_wal (const char *wal_name) {
    size_t n = strlen(wal_name);
    mw_db *found = NULL;
    sqlite3_mutex *g = sqlite3_mutex_alloc(SQLITE_MUTEX_STATIC_MAIN);
    sqlite3_mutex_enter(g);
    for (mw_db *db = mw_dbs; db; db = db->next) {
        size_t pn = strlen(db->path);
        if (!db->orphaned && db->mode >= 2 && pn + 4 == n && memcmp(wal_name, db->path, pn) == 0 && memcmp(wal_name + pn, "-wal", 4) == 0) { found = db; break; }
    }
    sqlite3_mutex_leave(g);
    return found;
}
