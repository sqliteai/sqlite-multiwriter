//  MultiWriter.h
//  sqlite-multiwriter: SQLite with more writers, as a loadable extension.
//
//  The framework holds the extension; it is loaded into a SQLite that allows extensions:
//      sqlite3_enable_load_extension(db, 1);
//      sqlite3_load_extension(db, "<path to the framework's binary>", "sqlite3_multiwriter_init", &err);
//  then a database is opened with the VFS:  file:app.db?vfs=multiwriter&mw=1   (see the README).
#ifndef MULTIWRITER_H
#define MULTIWRITER_H

#define MULTIWRITER_VERSION "@VERSION@"

#ifdef __cplusplus
extern "C" {
#endif

/// Entry points (the names SQLite looks for): they register the VFS "multiwriter" (the second one also makes it the default) and the SQL functions mw_version() and mw_version_number().
int sqlite3_multiwriter_init (void *db, char **errmsg, const void *api);
int sqlite3_multiwriter_default_init (void *db, char **errmsg, const void *api);
const char *mw_version (void);

#ifdef __cplusplus
}
#endif

#endif
