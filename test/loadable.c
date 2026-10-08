// The extension as it is shipped: dist/multiwriter.{so,dylib,dll} loaded into a stock SQLite (this program links the SQLite of the amalgamation WITHOUT the engine, so everything the engine does here
// comes from the loaded library, through the table of routines of the host). usage: loadable <path to the extension>
#include <pthread.h>
#include "mw_test.h"
#include "multiwriter.h"

static const char *g_path_db;
enum { NT = 4, ROWS = 300 };

static void *writer (void *arg) {
    long id = (long)arg; sqlite3 *db; char uri[400];
    snprintf(uri, sizeof uri, "file:%s?vfs=multiwriter&mw=2&mw_rebase=1", g_path_db);
    if (sqlite3_open_v2(uri, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL) != SQLITE_OK) { mw_failures++; return NULL; }
    sqlite3_busy_timeout(db, 0);
    for (int i = 0; i < ROWS; i++) {
        char sql[200]; snprintf(sql, sizeof sql, "INSERT INTO t(id, w, v) VALUES(%ld, %ld, %d)", id * 100000 + i, id, i);
        int rc; int tries = 0;
        do { rc = mw_exec(db, sql); } while ((rc & 0xff) == SQLITE_BUSY && ++tries < 100000);
        if (rc != SQLITE_OK) { mw_failures++; break; }
    }
    sqlite3_close(db); return NULL;
}

int main (int argc, char **argv) {
    if (argc < 2) { fprintf(stderr, "usage: loadable <extension>\n"); return 2; }
    sqlite3 *db; char *err = NULL;
    CHECK_RC(sqlite3_open(":memory:", &db), SQLITE_OK);
    CHECK_RC(sqlite3_enable_load_extension(db, 1), SQLITE_OK);
    CHECK(sqlite3_vfs_find("multiwriter") == NULL);                                           // (not there until the extension is loaded)
    CHECK_RC(sqlite3_load_extension(db, argv[1], "sqlite3_multiwriter_init", &err), SQLITE_OK);
    if (err) { printf("load: %s\n", err); sqlite3_free(err); }
    sqlite3_stmt *st; CHECK_RC(sqlite3_prepare_v2(db, "SELECT mw_version(), mw_version_number()", -1, &st, NULL), SQLITE_OK);
    CHECK_RC(sqlite3_step(st), SQLITE_ROW);
    printf("mw_version() = %s, mw_version_number() = %d\n", (const char *)sqlite3_column_text(st, 0), sqlite3_column_int(st, 1));
    CHECK(strcmp((const char *)sqlite3_column_text(st, 0), MW_VERSION) == 0); CHECK(sqlite3_column_int(st, 1) == MW_VERSION_NUMBER);
    sqlite3_finalize(st);
    sqlite3_vfs *v = sqlite3_vfs_find("multiwriter");
    CHECK(v != NULL); CHECK(sqlite3_vfs_find(NULL) != v);                                     // (registered, not the default)

    char path[300]; mw_tmpdb(path, sizeof path, "loadable"); g_path_db = path;
    sqlite3 *s; CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, w INTEGER, v INTEGER)"), SQLITE_OK);
    sqlite3_close(s);
    pthread_t th[NT];
    for (long i = 0; i < NT; i++) pthread_create(&th[i], NULL, writer, (void *)i);
    for (int i = 0; i < NT; i++) pthread_join(th[i], NULL);
    sqlite3 *c; CHECK_RC(sqlite3_open_v2(path, &c, SQLITE_OPEN_READWRITE, NULL), SQLITE_OK);
    CHECK(mw_scalar(c, "SELECT count(*) FROM t") == NT * ROWS);
    CHECK(mw_scalar(c, "SELECT count(DISTINCT id) FROM t") == NT * ROWS);
    sqlite3_stmt *ic; int ok = 0; sqlite3_prepare_v2(c, "PRAGMA integrity_check", -1, &ic, NULL); if (sqlite3_step(ic) == SQLITE_ROW) ok = strcmp((const char *)sqlite3_column_text(ic, 0), "ok") == 0; sqlite3_finalize(ic);
    CHECK(ok);
    sqlite3_close(c);

    // the second entry point makes it the default VFS
    CHECK_RC(sqlite3_load_extension(db, argv[1], "sqlite3_multiwriter_default_init", &err), SQLITE_OK);
    CHECK(sqlite3_vfs_find(NULL) == sqlite3_vfs_find("multiwriter"));
    sqlite3_close(db);
    mw_rmdb(path);
    MW_DONE();
}
