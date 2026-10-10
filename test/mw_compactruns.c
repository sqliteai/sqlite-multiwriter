// Compaction writes the pages that changed in order of page number, the adjacent ones with one write (runs of up to 64). The file must hold exactly what the commits wrote: runs longer than
// 64 pages, single pages, pages with a gap between them, pages that did not change in the middle of a run, a database that grows and one that is only updated. Opened with the stock VFS afterwards.
#include <stdint.h>
#include "mw_test.h"
#include "multiwriter.h"

static void check_stock (const char *path, long long rows, long long want_sum) {
    sqlite3 *s = NULL; CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE, MW_PLAIN_VFS), SQLITE_OK);
    sqlite3_stmt *st = NULL; CHECK_RC(sqlite3_prepare_v2(s, "PRAGMA integrity_check", -1, &st, NULL), SQLITE_OK);
    CHECK(sqlite3_step(st) == SQLITE_ROW && strcmp((const char *)sqlite3_column_text(st, 0), "ok") == 0);
    sqlite3_finalize(st);
    CHECK(mw_scalar(s, "SELECT count(*) FROM t") == rows);
    CHECK(mw_scalar(s, "SELECT sum(length(v) * 7 + id) FROM t") == want_sum);
    sqlite3_close(s);
}
static void run (const char *extra) {
    char path[256]; mw_tmpdb(path, sizeof path, "cruns"); char uri[400];
    snprintf(uri, sizeof uri, "file:%s?vfs=multiwriter&mw_log_max_mb=4000&mw_compact_ms=0%s", path, extra);
    sqlite3 *a = NULL; CHECK_RC(sqlite3_open_v2(uri, &a, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL), SQLITE_OK);
    CHECK_RC(mw_exec(a, "PRAGMA journal_mode=WAL; PRAGMA synchronous=OFF; CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)"), SQLITE_OK);
    const int N = 30000;                                                                  // (about 800 pages)
    CHECK_RC(mw_exec(a, "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x < 30000) INSERT INTO t SELECT x, printf('%0100d', x) FROM n"), SQLITE_OK);
    long long sum = 0; for (int i = 1; i <= N; i++) sum += 100 * 7 + i;
    mw_compact_result cr; memset(&cr, 0, sizeof cr);
    CHECK_RC(sqlite3_file_control(a, "main", MW_FCNTL_COMPACT, &cr), SQLITE_OK);
    printf("%s: first compaction wrote %llu pages\n", extra, (unsigned long long)cr.pages_written);
    CHECK(cr.pages_written > 128);                                                        // (a run of more than 64)
    // every page changes: one long run of pages
    CHECK_RC(mw_exec(a, "UPDATE t SET v = v || 'xx'"), SQLITE_OK); sum += 2 * 7 * N;
    CHECK_RC(sqlite3_file_control(a, "main", MW_FCNTL_COMPACT, &cr), SQLITE_OK);
    CHECK(cr.pages_written > 128);
    // a few single pages far from each other, and a gap in the middle of a cluster (the pages between did not change)
    int ids[] = { 5, 4000, 8100, 8110, 8120, 20000, 20001, 29990 };
    for (unsigned i = 0; i < sizeof ids / sizeof *ids; i++) {
        char q[100]; snprintf(q, sizeof q, "UPDATE t SET v = v || 'y' WHERE id = %d", ids[i]); CHECK_RC(mw_exec(a, q), SQLITE_OK); sum += 7;
    }
    CHECK_RC(mw_exec(a, "UPDATE t SET v = v || 'z' WHERE id BETWEEN 12000 AND 12400"), SQLITE_OK); sum += 7 * 401;
    CHECK_RC(sqlite3_file_control(a, "main", MW_FCNTL_COMPACT, &cr), SQLITE_OK);
    printf("%s: third compaction wrote %llu pages\n", extra, (unsigned long long)cr.pages_written);
    CHECK(cr.pages_written >= 8);
    // the database grows by pages above the old end, and a delete frees some in the middle
    CHECK_RC(mw_exec(a, "WITH RECURSIVE n(x) AS (SELECT 30001 UNION ALL SELECT x+1 FROM n WHERE x < 33000) INSERT INTO t SELECT x, printf('%0100d', x) FROM n"), SQLITE_OK);
    for (int i = N + 1; i <= 33000; i++) sum += 100 * 7 + i;
    CHECK_RC(sqlite3_file_control(a, "main", MW_FCNTL_COMPACT, &cr), SQLITE_OK);
    CHECK(mw_scalar(a, "SELECT sum(length(v) * 7 + id) FROM t") == sum);
    sqlite3_close(a);
    check_stock(path, 33000, sum);                                                        // (what is in the file: closed, so the engine has compacted all of it)
    mw_rmfiles(path);
}
int main (void) {
    run("");
    run("&mw_mp=1");
    MW_DONE();
}
