// Two things that the run of SQLite's own test suite showed:
//  1. a VFS stacked over the engine that reports a device that is not power-safe and has a big sector: SQLite then pads the commit of its WAL to a sector boundary with a
//     frame header and a part of a page (and, with small pages, whole frames); the engine used to answer that write with SQLITE_IOERR_WRITE.
//  2. out of memory while the engine records the pages that a transaction reads: the commit must fail with SQLITE_NOMEM (the application must not retry it as a conflict).
#include "mw_test.h"
#include "multiwriter.h"

// ---- 1. a pass-through VFS with its own sector size and no device characteristics
typedef struct { sqlite3_file base; sqlite3_file *real; } shim_file;
static sqlite3_vfs shim_vfs, *under;
static int sector = 4096;
#define R(f) (((shim_file *)(f))->real)
static int s_close (sqlite3_file *f) { int rc = R(f)->pMethods ? R(f)->pMethods->xClose(R(f)) : SQLITE_OK; return rc; }
static int s_read (sqlite3_file *f, void *b, int n, sqlite3_int64 o) { return R(f)->pMethods->xRead(R(f), b, n, o); }
static int s_write (sqlite3_file *f, const void *b, int n, sqlite3_int64 o) { return R(f)->pMethods->xWrite(R(f), b, n, o); }
static int s_trunc (sqlite3_file *f, sqlite3_int64 n) { return R(f)->pMethods->xTruncate(R(f), n); }
static int s_sync (sqlite3_file *f, int fl) { return R(f)->pMethods->xSync(R(f), fl); }
static int s_size (sqlite3_file *f, sqlite3_int64 *p) { return R(f)->pMethods->xFileSize(R(f), p); }
static int s_lock (sqlite3_file *f, int l) { return R(f)->pMethods->xLock(R(f), l); }
static int s_unlock (sqlite3_file *f, int l) { return R(f)->pMethods->xUnlock(R(f), l); }
static int s_chk (sqlite3_file *f, int *p) { return R(f)->pMethods->xCheckReservedLock(R(f), p); }
static int s_fc (sqlite3_file *f, int op, void *a) { return R(f)->pMethods->xFileControl(R(f), op, a); }
static int s_sector (sqlite3_file *f) { (void)f; return sector; }
static int s_dev (sqlite3_file *f) { (void)f; return 0; }
static int s_shmmap (sqlite3_file *f, int r, int sz, int w, void volatile **pp) { return R(f)->pMethods->xShmMap(R(f), r, sz, w, pp); }
static int s_shmlock (sqlite3_file *f, int o, int n, int fl) { return R(f)->pMethods->xShmLock(R(f), o, n, fl); }
static void s_shmbar (sqlite3_file *f) { R(f)->pMethods->xShmBarrier(R(f)); }
static int s_shmunmap (sqlite3_file *f, int d) { return R(f)->pMethods->xShmUnmap(R(f), d); }
static const sqlite3_io_methods shim_io = { 2, s_close, s_read, s_write, s_trunc, s_sync, s_size, s_lock, s_unlock, s_chk, s_fc, s_sector, s_dev, s_shmmap, s_shmlock, s_shmbar, s_shmunmap };
static int v_open (sqlite3_vfs *v, const char *name, sqlite3_file *f, int flags, int *out) {
    shim_file *p = (shim_file *)f; p->real = (sqlite3_file *)&p[1]; p->base.pMethods = NULL;
    int rc = under->xOpen(under, name, p->real, flags, out);
    if (rc == SQLITE_OK) p->base.pMethods = &shim_io;
    return rc;
}
static int v_delete (sqlite3_vfs *v, const char *n, int s) { return under->xDelete(under, n, s); }
static int v_access (sqlite3_vfs *v, const char *n, int fl, int *o) { return under->xAccess(under, n, fl, o); }
static int v_full (sqlite3_vfs *v, const char *n, int len, char *o) { return under->xFullPathname(under, n, len, o); }

static void stacked_run (int sect, int pgsz) {
    sector = sect;
    char path[256], uri[400]; mw_tmpdb(path, sizeof path, "stacked");
    snprintf(uri, sizeof uri, "file:%s?mw=2", path);
    sqlite3 *s; CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"), SQLITE_OK);
    char q[100]; snprintf(q, sizeof q, "PRAGMA page_size=%d; PRAGMA journal_mode=WAL;", pgsz);
    CHECK_RC(mw_exec(s, q), SQLITE_OK); CHECK_RC(sqlite3_close(s), SQLITE_OK);
    sqlite3 *db; CHECK_RC(sqlite3_open_v2(uri, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, "shim"), SQLITE_OK);
    CHECK_RC(mw_exec(db, "CREATE TABLE t(a, b); CREATE INDEX i ON t(a)"), SQLITE_OK);
    int bad = 0;
    for (int i = 0; i < 60; i++) { char sql[200]; snprintf(sql, sizeof sql, "INSERT INTO t VALUES(%d, hex(randomblob(300)))", i); if (mw_exec(db, sql) != SQLITE_OK) bad++; }
    CHECK_RC(mw_exec(db, "BEGIN; INSERT INTO t VALUES(-1, 'x'); UPDATE t SET a = a + 1000 WHERE a > 50; COMMIT"), SQLITE_OK);
    CHECK(bad == 0);
    CHECK(mw_scalar(db, "SELECT count(*) FROM t") == 61);
    sqlite3_close(db);
    CHECK_RC(sqlite3_open_v2(uri, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL), SQLITE_OK);
    CHECK(mw_scalar(db, "SELECT count(*) FROM t") == 61);
    CHECK(mw_scalar(db, "SELECT count(*) FROM t WHERE a >= 1000") == 9);
    sqlite3_stmt *st; sqlite3_prepare_v2(db, "PRAGMA integrity_check", -1, &st, NULL); CHECK(sqlite3_step(st) == SQLITE_ROW && !strcmp((const char *)sqlite3_column_text(st, 0), "ok")); sqlite3_finalize(st);
    sqlite3_close(db); mw_rmfiles(path);
}

// ---- 2. out of memory
static sqlite3_mem_methods real_mem;
static int fail_at = 0, alloc_n = 0;                    // fail the allocation number fail_at after arming (0: never)
static void *m_malloc (int n) { if (fail_at && ++alloc_n == fail_at) return NULL; return real_mem.xMalloc(n); }
static void m_free (void *p) { real_mem.xFree(p); }
static void *m_realloc (void *p, int n) { if (fail_at && ++alloc_n == fail_at) return NULL; return real_mem.xRealloc(p, n); }
static int m_size (void *p) { return real_mem.xSize(p); }
static int m_round (int n) { return real_mem.xRoundup(n); }
static int m_init (void *a) { return real_mem.xInit(a); }
static void m_shut (void *a) { real_mem.xShutdown(a); }

static void oom_run (void) {
    int busy = 0, nomem = 0, other = 0, ok = 0;
    for (int n = 1; n <= 400; n++) for (int body = 0; body < 2; body++) {
        char path[256], uri[300]; mw_tmpdb(path, sizeof path, "oom"); snprintf(uri, sizeof uri, "file:%s?mw=2", path);
        sqlite3 *db; fail_at = 0;
        if (sqlite3_open_v2(uri, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL) != SQLITE_OK) { CHECK(0); return; }
        sqlite3_db_config(db, SQLITE_DBCONFIG_LOOKASIDE, NULL, 0, 0);           // (every allocation goes through the allocator that fails)
        mw_exec(db, "CREATE TABLE a(x, y)");                  // (the first read of the first INSERT is what allocates the read set: nothing may read before)
        if (body) mw_exec(db, "INSERT INTO a VALUES(1, 'x'), (2, 'y')");
        alloc_n = 0; fail_at = n;
        int rc = body ? mw_exec(db, "BEGIN; UPDATE a SET y = y || 'z'; INSERT INTO a SELECT x + 10, y FROM a; COMMIT") : mw_exec(db, "INSERT INTO a VALUES(1, 2)");
        fail_at = 0;
        if (rc == SQLITE_OK) ok++; else if ((rc & 0xff) == SQLITE_NOMEM) nomem++; else if ((rc & 0xff) == SQLITE_BUSY) busy++; else other++;
        mw_exec(db, "ROLLBACK");
        CHECK_RC(mw_exec(db, "INSERT INTO a VALUES(99, 'after')"), SQLITE_OK);               // (the connection is usable after the failure)
        sqlite3_close(db); mw_rmfiles(path);
    }
    printf("  out of memory: %d ok, %d NOMEM, %d BUSY, %d other\n", ok, nomem, busy, other);
    CHECK(busy == 0);
    CHECK(nomem > 20);
}

int main (void) {
    sqlite3_config(SQLITE_CONFIG_GETMALLOC, &real_mem);
    sqlite3_mem_methods mm = { m_malloc, m_free, m_realloc, m_size, m_round, m_init, m_shut, NULL };
    sqlite3_config(SQLITE_CONFIG_MALLOC, &mm);
    sqlite3_initialize();
    under = sqlite3_vfs_find(NULL);
    shim_vfs = *under; shim_vfs.zName = "shim"; shim_vfs.pNext = NULL; shim_vfs.szOsFile = (int)sizeof(shim_file) + under->szOsFile;
    shim_vfs.xOpen = v_open; shim_vfs.xDelete = v_delete; shim_vfs.xAccess = v_access; shim_vfs.xFullPathname = v_full;
    CHECK_RC(sqlite3_vfs_register(&shim_vfs, 0), SQLITE_OK);
    static const int sects[] = { 512, 4096, 8192 }, pgs[] = { 1024, 4096 };
    for (int i = 0; i < 3; i++) for (int j = 0; j < 2; j++) stacked_run(sects[i], pgs[j]);
    oom_run();
    printf("test/mw_stacked.c: %d failure(s)\n", mw_failures);
    return mw_failures ? 1 : 0;
}
