//
//  multiwriter_boot.c
//  cloudsync
//
//  Bootstrap for the Multi-Writer build. SQLite is compiled with
//  -DSQLITE_EXTRA_INIT=mw_extra_init, so this runs at the end of sqlite3_initialize():
//  the Multi-Writer VFS is therefore registered (as default) before any sqlite3_open(),
//  and sqlite3_config() remains usable before initialization.
//

#include "multiwriter.h"

int mw_extra_init (const char *unused) {
    (void)unused;
    return mw_vfs_register(1);
}
