// The first open of a private lane registers the trace hook of the schema barrier as an auto-extension. When that registration fails (SQLITE_NOMEM), the open must fail with it, not
// go on without the hook and remember the registration as done: the next open registers again, and the connections that follow do not register twice.
// Compile multiwriter_vfs.c with -Dsqlite3_auto_extension=inject_auto_extension.
#include "mw_test.h"

static int calls, fail_calls;
int inject_auto_extension (void (*entry)(void)) {
    if (fail_calls > 0) { fail_calls--; calls++; return SQLITE_NOMEM; }
    calls++;
    return sqlite3_auto_extension(entry);
}

static int open_private (const char *uri, sqlite3 **db) {
    return sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
}

int main (void) {
    char path[256]; mw_tmpdb(path, sizeof path, "autoextfail");
    char uri[400]; snprintf(uri, sizeof uri, "file:%s?vfs=multiwriter&mw=1&mw_mp=0&mw_rebase=0", path);

    // two failed registrations: both opens fail with it, and each of them asked again
    fail_calls = 2;
    for (int i = 1; i <= 2; i++) {
        sqlite3 *db = NULL;
        CHECK_RC(open_private(uri, &db), SQLITE_NOMEM);
        sqlite3_close(db);
        CHECK_RC(calls, i);
    }

    // the registration works: the open succeeds and the connection is usable
    sqlite3 *a = NULL;
    CHECK_RC(open_private(uri, &a), SQLITE_OK);
    CHECK_RC(calls, 3);
    CHECK_RC(mw_exec(a, "CREATE TABLE t(x); INSERT INTO t VALUES (1)"), SQLITE_OK);

    // and it is remembered: the next connections do not register again
    sqlite3 *b = NULL;
    CHECK_RC(open_private(uri, &b), SQLITE_OK);
    CHECK_RC(calls, 3);
    CHECK_RC(mw_exec(b, "INSERT INTO t VALUES (2)"), SQLITE_OK);
    sqlite3_close(b);
    sqlite3_close(a);

    printf("test/mw_autoextfail.c: %d failure(s)\n", mw_failures);
    return mw_failures ? 1 : 0;
}
