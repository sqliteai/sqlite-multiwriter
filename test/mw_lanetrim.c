// What a connection grew to for one big transaction (the buffer of the frames of its private WAL, the lists of the pages it wrote and read) is given back when its next snapshot begins: it
// kept it for as long as it lived (66 MB of heap after a transaction of 600000 rows and fifty small ones, in each connection that had ever done that).
#include "mw_test.h"
#include "multiwriter.h"

static void run (const char *extra) {
    char path[256]; mw_tmpdb(path, sizeof path, "lanetrim"); char uri[400]; snprintf(uri, sizeof uri, "file:%s?vfs=multiwriter%s", path, extra);
    sqlite3 *a = NULL; CHECK_RC(sqlite3_open_v2(uri, &a, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL), SQLITE_OK);
    CHECK_RC(mw_exec(a, "PRAGMA journal_mode=WAL; PRAGMA synchronous=OFF; CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)"), SQLITE_OK);
    CHECK_RC(mw_exec(a, "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 250000) INSERT INTO t SELECT x, printf('%0100d', x) FROM c"), SQLITE_OK);
    long long big = (long long)sqlite3_memory_used();
    for (int i = 1; i <= 20; i++) { char q[100]; snprintf(q, sizeof q, "UPDATE t SET v = 'x' WHERE id = %d", i); CHECK_RC(mw_exec(a, q), SQLITE_OK); }
    CHECK(mw_scalar(a, "SELECT count(*) FROM t") == 250000);
    long long small = (long long)sqlite3_memory_used();
    printf("%s: heap after the big transaction %lld KB, after 20 small ones %lld KB\n", extra, big / 1024, small / 1024);
    CHECK(big > (16ll << 20));                                    // (what the test is about is there)
    CHECK(small < (8ll << 20));
    sqlite3_close(a); mw_rmfiles(path);
}
int main (void) {
    run("");
    run("&mw_mp=1");
    MW_DONE();
}
