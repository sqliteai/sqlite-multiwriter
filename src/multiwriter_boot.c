//
//  multiwriter_boot.c
//  sqlite-multiwriter
//
//  Bootstrap of the Multi-Writer build, in two forms:
//    - embedded (the default build, the tests, the static library): SQLite is compiled with
//      -DSQLITE_EXTRA_INIT=mw_extra_init, so this runs at the end of sqlite3_initialize():
//      the VFS is registered (as default) before any sqlite3_open(), and sqlite3_config()
//      remains usable before initialization;
//    - loadable (MW_LOADABLE: dist/multiwriter.so, .dylib, .dll, the frameworks, the AAR):
//      SELECT load_extension('multiwriter') calls sqlite3_multiwriter_init, which registers
//      the VFS (open a database with vfs=multiwriter, or call it the default with the second
//      entry point, sqlite3_multiwriter_default_init) and the SQL function mw_version().
//

#include "multiwriter.h"
#include <stdlib.h>

MW_API const char *mw_version (void) { return MW_VERSION; }

#ifdef MW_LOADABLE
SQLITE_EXTENSION_INIT1
#include <string.h>

static void sql_version (sqlite3_context *ctx, int argc, sqlite3_value **argv) { (void)argc; (void)argv; sqlite3_result_text(ctx, MW_VERSION, -1, SQLITE_STATIC); }
static void sql_version_number (sqlite3_context *ctx, int argc, sqlite3_value **argv) { (void)argc; (void)argv; sqlite3_result_int(ctx, MW_VERSION_NUMBER); }

static int load (sqlite3 *db, char **err, const sqlite3_api_routines *api, int make_default) {
    SQLITE_EXTENSION_INIT2(api);
    int rc = mw_vfs_register(make_default);                      // (idempotent: a second load, or a second connection, does the same)
    if (rc != SQLITE_OK) { if (err) *err = sqlite3_mprintf("multiwriter: cannot register the VFS (%d)", rc); return rc; }
    rc = sqlite3_create_function(db, "mw_version", 0, SQLITE_UTF8 | SQLITE_DETERMINISTIC, NULL, sql_version, NULL, NULL);
    if (rc == SQLITE_OK) rc = sqlite3_create_function(db, "mw_version_number", 0, SQLITE_UTF8 | SQLITE_DETERMINISTIC, NULL, sql_version_number, NULL, NULL);
    return rc == SQLITE_OK ? SQLITE_OK_LOAD_PERMANENTLY : rc;    // (the VFS lives in this library: it must never be unloaded)
}
MW_API int sqlite3_multiwriter_init (sqlite3 *db, char **err, const sqlite3_api_routines *api) { return load(db, err, api, 0); }
MW_API int sqlite3_multiwriter_default_init (sqlite3 *db, char **err, const sqlite3_api_routines *api) { return load(db, err, api, 1); }
#else
int mw_extra_init (const char *unused) {
    (void)unused;
    const char *e = getenv("MW_DEFAULT_MODE");                 // (for running the SQLite test suite through the engine: every database opens with this mw= mode)
    if (e) mw_vfs_set_enabled_default(atoi(e));
    return mw_vfs_register(1);
}
#endif
