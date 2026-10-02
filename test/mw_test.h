// Minimal helpers shared by the Multi-Writer test programs.
#ifndef MW_TEST_H
#define MW_TEST_H
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "sqlite3.h"

static int mw_failures = 0;
#define CHECK(c) do { if (!(c)) { printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #c); mw_failures++; } } while (0)
#define CHECK_RC(rc, want) do { int _r = (rc); if (_r != (want)) { printf("FAIL %s:%d: %s = %d (want %d)\n", __FILE__, __LINE__, #rc, _r, (int)(want)); mw_failures++; } } while (0)

static inline int mw_exec (sqlite3 *db, const char *sql) {
    char *err = NULL;
    int rc = sqlite3_exec(db, sql, NULL, NULL, &err);
    if (err) { if (getenv("MW_VERBOSE")) printf("  exec error: %s (%s)\n", err, sql); sqlite3_free(err); }
    return rc;
}

static inline int64_t mw_scalar (sqlite3 *db, const char *sql) {
    sqlite3_stmt *st = NULL;
    int64_t v = -1;
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW) v = sqlite3_column_int64(st, 0);
    sqlite3_finalize(st);
    return v;
}

static inline char *mw_tmpdb (char *buf, size_t n, const char *tag) {
    snprintf(buf, n, "/tmp/mw_%s_%d.db", tag, (int)getpid());
    char p[512];
    const char *sfx[] = {"", "-wal", "-shm", "-journal"};
    for (int i = 0; i < 4; i++) { snprintf(p, sizeof p, "%s%s", buf, sfx[i]); unlink(p); }
    return buf;
}

static inline void mw_rmdb (const char *path) {
    char p[512];
    const char *sfx[] = {"", "-wal", "-shm", "-journal"};
    for (int i = 0; i < 4; i++) { snprintf(p, sizeof p, "%s%s", path, sfx[i]); unlink(p); }
}

#define MW_DONE() do { printf("%s: %d failure(s)\n", __FILE__, mw_failures); return mw_failures ? 1 : 0; } while (0)
#endif
