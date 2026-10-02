// Phase 1/2: pass-through VFS + instrumentation. Establishes (by experiment, not assumption)
// which VFS events correspond to SQLite transaction lifecycle events.
#include "mw_test.h"
#include "multiwriter.h"

#define LOGMAX 4096
typedef struct { mw_event_t ev; int64_t a, b; int flags; char file[16]; } rec_t;
static rec_t log_[LOGMAX];
static int nlog = 0;

static void sink (void *arg, mw_event_t ev, const char *file, int64_t a, int64_t b, int flags) {
    (void)arg;
    if (nlog >= LOGMAX) return;
    rec_t *r = &log_[nlog++];
    r->ev = ev; r->a = a; r->b = b; r->flags = flags;
    const char *sfx = file ? strrchr(file, '-') : NULL;
    snprintf(r->file, sizeof r->file, "%s", !file ? "temp" : (sfx && sfx[1] != 0 && strchr(sfx, '/') == NULL) ? sfx : "db");
    if (getenv("MW_VERBOSE")) printf("    [%s] %s a=%lld b=%lld fl=%d\n", r->file, mw_event_name(ev), (long long)a, (long long)b, flags);
}

static void mark (const char *s) { nlog = 0; if (getenv("MW_VERBOSE")) printf("  --- %s\n", s); }

static int count (mw_event_t ev, int64_t a, int flags) {
    int n = 0;
    for (int i = 0; i < nlog; i++) if (log_[i].ev == ev && (a < 0 || log_[i].a == a) && (flags < 0 || log_[i].flags == flags)) n++;
    return n;
}

// SQLite WAL shm lock slots (wal.c): 0 = WRITE, 1 = CKPT, 2 = RECOVER, 3.. = READ marks
#define SHM_SHARED_LOCK (SQLITE_SHM_LOCK | SQLITE_SHM_SHARED)
#define SHM_EXCL_LOCK   (SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE)
#define SHM_SHARED_UNLK (SQLITE_SHM_UNLOCK | SQLITE_SHM_SHARED)
#define SHM_EXCL_UNLK   (SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE)

static int shm_read_locks (int flags) {
    int n = 0;
    for (int i = 0; i < nlog; i++) if (log_[i].ev == MW_EV_SHMLOCK && log_[i].a >= 3 && log_[i].flags == flags) n++;
    return n;
}

int main (void) {
    char path[256];
    mw_tmpdb(path, sizeof path, "trace");
    mw_vfs_set_trace(sink, NULL);

    // The default VFS must be ours: registered from SQLITE_EXTRA_INIT before the first open.
    sqlite3 *db = NULL, *db2 = NULL;
    CHECK_RC(sqlite3_open(path, &db), SQLITE_OK);
    char *vfsname = NULL;
    sqlite3_file_control(db, "main", SQLITE_FCNTL_VFSNAME, &vfsname);
    CHECK(vfsname && strncmp(vfsname, MW_VFS_NAME, strlen(MW_VFS_NAME)) == 0);
    printf("VFS chain: %s\n", vfsname ? vfsname : "?");
    sqlite3_free(vfsname);

    CHECK_RC(mw_exec(db, "PRAGMA journal_mode=WAL"), SQLITE_OK);
    CHECK_RC(mw_exec(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'a'),(2,'b')"), SQLITE_OK);
    CHECK_RC(sqlite3_open(path, &db2), SQLITE_OK);
    CHECK_RC(mw_exec(db2, "SELECT 1 FROM t"), SQLITE_OK);

    // ---- read transaction: snapshot pinned by a WAL read-mark SHARED lock, held until the txn ends
    mark("read txn");
    CHECK_RC(mw_exec(db2, "BEGIN"), SQLITE_OK);
    CHECK(shm_read_locks(SHM_SHARED_LOCK) == 0);              // BEGIN (deferred) touches nothing
    CHECK(mw_scalar(db2, "SELECT count(*) FROM t") == 2);
    int locked = shm_read_locks(SHM_SHARED_LOCK);
    CHECK(locked == 1);                                       // first read acquires a read-mark
    CHECK(shm_read_locks(SHM_SHARED_UNLK) == 0);
    CHECK(mw_scalar(db2, "SELECT count(*) FROM t") == 2);
    CHECK(shm_read_locks(SHM_SHARED_LOCK) == 1);              // second read reuses the same snapshot
    CHECK_RC(mw_exec(db2, "COMMIT"), SQLITE_OK);
    CHECK(shm_read_locks(SHM_SHARED_UNLK) == 1);              // COMMIT/end of txn releases it
    printf("read txn: snapshot start = first read (read-mark shm lock), end = its release\n");

    // ---- write transaction: the WAL write lock (shm slot 0, exclusive) marks the writer
    mark("write txn commit");
    CHECK_RC(mw_exec(db, "BEGIN"), SQLITE_OK);
    CHECK_RC(mw_exec(db, "UPDATE t SET v='x' WHERE id=1"), SQLITE_OK);
    CHECK(count(MW_EV_SHMLOCK, 0, SHM_EXCL_LOCK) == 1);       // write lock taken at first write
    CHECK(count(MW_EV_WRITE, -1, -1) == 0);                   // ...and nothing hit the WAL yet (page cache)
    CHECK_RC(mw_exec(db, "COMMIT"), SQLITE_OK);
    CHECK(count(MW_EV_WRITE, -1, -1) >= 1);                   // commit appends frames to the WAL
    CHECK(count(MW_EV_SHMLOCK, 0, SHM_EXCL_UNLK) == 1);       // write lock released at commit
    printf("write txn: writer lock at first write, WAL frames appended at COMMIT (xWrite on -wal), lock released\n");

    // ---- rollback: no WAL writes at all
    mark("rollback");
    CHECK_RC(mw_exec(db, "BEGIN; UPDATE t SET v='y' WHERE id=2; ROLLBACK"), SQLITE_OK);
    CHECK(count(MW_EV_WRITE, -1, -1) == 0);
    CHECK(count(MW_EV_SHMLOCK, 0, SHM_EXCL_UNLK) == 1);

    // ---- second concurrent writer: stock SQLite single-writer limit is visible at the shm write lock
    mark("two writers");
    CHECK_RC(mw_exec(db, "BEGIN IMMEDIATE"), SQLITE_OK);
    sqlite3_busy_timeout(db2, 0);
    CHECK_RC(mw_exec(db2, "BEGIN IMMEDIATE"), SQLITE_BUSY);
    CHECK_RC(mw_exec(db, "COMMIT"), SQLITE_OK);

    // ---- savepoint: purely in-memory; visible on the VFS only as an eventual (single) commit
    mark("savepoint");
    CHECK_RC(mw_exec(db, "BEGIN; UPDATE t SET v='a' WHERE id=1; SAVEPOINT s1; UPDATE t SET v='b' WHERE id=2; ROLLBACK TO s1; RELEASE s1; COMMIT"), SQLITE_OK);
    CHECK(count(MW_EV_SYNC, -1, -1) >= 0);
    CHECK(mw_scalar(db, "SELECT v='a' AND (SELECT v FROM t WHERE id=2)='b' FROM t WHERE id=1") == 1);

    // ---- checkpoint: copies WAL frames into the db file (xWrite on main db) and syncs
    mark("checkpoint");
    CHECK_RC(mw_exec(db, "PRAGMA wal_checkpoint(TRUNCATE)"), SQLITE_OK);
    CHECK(count(MW_EV_SYNC, -1, -1) >= 1);
    int dbwrites = 0;
    for (int i = 0; i < nlog; i++) if (log_[i].ev == MW_EV_WRITE && strcmp(log_[i].file, "db") == 0) dbwrites++;
    CHECK(dbwrites >= 1);

    // ---- mmap / xFetch: with mmap_size>0 the pager bypasses xRead through xFetch
    sqlite3_close(db2);
    mark("mmap off");
    CHECK_RC(sqlite3_open(path, &db2), SQLITE_OK);
    CHECK_RC(mw_exec(db2, "PRAGMA mmap_size=0"), SQLITE_OK);
    mw_scalar(db2, "SELECT count(*) FROM t");
    CHECK(count(MW_EV_FETCH, -1, -1) == 0);
    sqlite3_close(db2);
    mark("mmap on");
    CHECK_RC(sqlite3_open(path, &db2), SQLITE_OK);
    CHECK_RC(mw_exec(db2, "PRAGMA mmap_size=1048576"), SQLITE_OK);
    mw_scalar(db2, "SELECT count(*) FROM t");
    printf("mmap on: xFetch=%llu xRead=%d (mmap serves reads via xFetch)\n", (unsigned long long)mw_vfs_event_count(MW_EV_FETCH), count(MW_EV_READ, -1, -1));
    CHECK(count(MW_EV_FETCH, -1, -1) >= 1);

    sqlite3_close(db2);
    sqlite3_close(db);
    mw_vfs_set_trace(NULL, NULL);
    mw_rmdb(path);
    MW_DONE();
}
