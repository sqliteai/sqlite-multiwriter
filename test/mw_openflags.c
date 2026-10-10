// How the open flags of the application are honoured by a database that the engine converts to WAL when it opens it.
#include "mw_test.h"
#include "multiwriter.h"

int main (void) {
    char path[256]; mw_tmpdb(path, sizeof path, "openflags"); char uri[300]; snprintf(uri, sizeof uri, "file:%s?mw=1", path);
    sqlite3 *db = NULL;
    // 0. the value of mw= is checked: a typo must not give stock SQLite. 1, true and on are the engine, 0 and off are stock, 2 is the experimental mode; anything else fails to open
    {
        const char *bad[] = { "banana", "3", "", "-1" }, *good[] = { "1", "true", "on", "yes", "0", "off", "2" };
        for (unsigned i = 0; i < sizeof bad / sizeof *bad; i++) {
            char u[400]; snprintf(u, sizeof u, "file:%s-v?mw=%s", path, bad[i]);
            CHECK_RC(sqlite3_open_v2(u, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL), SQLITE_CANTOPEN); sqlite3_close(db); db = NULL;
        }
        for (unsigned i = 0; i < sizeof good / sizeof *good; i++) {
            char u[400]; snprintf(u, sizeof u, "file:%s-v%u?mw=%s", path, i, good[i]);
            CHECK_RC(sqlite3_open_v2(u, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL), SQLITE_OK);
            CHECK_RC(mw_exec(db, "CREATE TABLE t(a); INSERT INTO t VALUES(1)"), SQLITE_OK); sqlite3_close(db); db = NULL;
            char p[400]; snprintf(p, sizeof p, "%s-v%u", path, i); mw_rmfiles(p);
        }
    }
    // 0b. mw_mp belongs to the engine (mw=1): in the experimental mode (mw=2) there is no shared header, and the database opens as one process (it used to crash)
    {
        char u[400]; snprintf(u, sizeof u, "file:%s-t?vfs=multiwriter&mw=2&mw_mp=1", path);
        CHECK_RC(sqlite3_open_v2(u, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL), SQLITE_OK);
        CHECK_RC(mw_exec(db, "CREATE TABLE t(a); INSERT INTO t VALUES (1)"), SQLITE_OK);
        CHECK(mw_scalar(db, "SELECT count(*) FROM t") == 1);
        mw_db_stats st; memset(&st, 0, sizeof st);
        CHECK_RC(sqlite3_file_control(db, "main", MW_FCNTL_DBSTATS, &st), SQLITE_OK);
        sqlite3_close(db); db = NULL;
        char p[400]; snprintf(p, sizeof p, "%s-t", path); mw_rmfiles(p);
    }
    // 1. no SQLITE_OPEN_CREATE and no file: it fails, and nothing is created
    CHECK_RC(sqlite3_open_v2(uri, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL), SQLITE_CANTOPEN); sqlite3_close(db); db = NULL;
    CHECK(access(path, F_OK) != 0);
    char side[400]; snprintf(side, sizeof side, "%s-mw", path); CHECK(access(side, F_OK) != 0);
    // 2. with it, the file is created
    CHECK_RC(sqlite3_open_v2(uri, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL), SQLITE_OK);
    CHECK_RC(mw_exec(db, "CREATE TABLE t(a); INSERT INTO t VALUES(1)"), SQLITE_OK); sqlite3_close(db); db = NULL;
    // 3. an existing file opens without it
    CHECK_RC(sqlite3_open_v2(uri, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL), SQLITE_OK);
    CHECK(mw_scalar(db, "SELECT count(*) FROM t") == 1);
    CHECK_RC(mw_exec(db, "INSERT INTO t VALUES(2)"), SQLITE_OK); sqlite3_close(db); db = NULL;
    // 4. read-only: reads, refuses to write
    CHECK_RC(sqlite3_open_v2(uri, &db, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, NULL), SQLITE_OK);
    CHECK(mw_scalar(db, "SELECT count(*) FROM t") == 2);
    CHECK_RC(mw_exec(db, "INSERT INTO t VALUES(3)"), SQLITE_READONLY); sqlite3_close(db); db = NULL;
    mw_rmfiles(path);
    // 5. a rollback-mode file that is opened read-only is not changed (the conversion to WAL writes the file)
    { sqlite3 *s; CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, MW_PLAIN_VFS), SQLITE_OK);
      CHECK_RC(mw_exec(s, "CREATE TABLE t(a); INSERT INTO t VALUES(1)"), SQLITE_OK); sqlite3_close(s); }
    struct stat a0, a1; stat(path, &a0); unsigned char h0[100], h1[100]; { FILE *f = fopen(path, "rb"); size_t n = fread(h0, 1, 100, f); (void)n; fclose(f); }
    int rc = sqlite3_open_v2(uri, &db, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, NULL);
    if (rc == SQLITE_OK) mw_scalar(db, "SELECT count(*) FROM t");
    sqlite3_close(db);
    stat(path, &a1); { FILE *f = fopen(path, "rb"); size_t n = fread(h1, 1, 100, f); (void)n; fclose(f); }
    CHECK(memcmp(h0, h1, 100) == 0);
    CHECK(a0.st_mtime == a1.st_mtime);
    mw_rmfiles(path);
    printf("test/mw_openflags.c: %d failure(s)\n", mw_failures);
    return mw_failures ? 1 : 0;
}
