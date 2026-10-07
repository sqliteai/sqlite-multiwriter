//
//  multiwriter_boot.c
//  sqlite-multiwriter
//
//  Bootstrap for the Multi-Writer build. SQLite is compiled with
//  -DSQLITE_EXTRA_INIT=mw_extra_init, so this runs at the end of sqlite3_initialize():
//  the Multi-Writer VFS is therefore registered (as default) before any sqlite3_open(),
//  and sqlite3_config() remains usable before initialization.
//

#include "multiwriter.h"
#include <stdlib.h>

int mw_extra_init (const char *unused) {
    (void)unused;
    const char *e = getenv("MW_DEFAULT_MODE");                 // (for running the SQLite test suite through the engine: every database opens with this mw= mode)
    if (e) mw_vfs_set_enabled_default(atoi(e));
    return mw_vfs_register(1);
}
