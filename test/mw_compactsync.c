// Commits with synchronous=OFF are visible before they are on the disk; the compaction must have the log durable before it writes their pages into the file (or a power failure leaves the file ahead of the log).
#include "mw_test.h"
#include "multiwriter.h"

int main (void) {
    char path[300]; mw_tmpdb(path, sizeof path, "compactsync");
    { sqlite3 *s; CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, MW_PLAIN_VFS), SQLITE_OK);
      CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)"), SQLITE_OK); sqlite3_close(s); }
    char uri[400]; snprintf(uri, sizeof uri, "file:%s?mw=2&mw_compact_ms=0", path);
    sqlite3 *db; CHECK_RC(sqlite3_open_v2(uri, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL), SQLITE_OK);
    CHECK_RC(mw_exec(db, "PRAGMA synchronous=OFF"), SQLITE_OK);
    for (int i = 0; i < 40; i++) { char q[100]; snprintf(q, sizeof q, "INSERT INTO t VALUES(%d, 'x%d')", i, i); CHECK_RC(mw_exec(db, q), SQLITE_OK); }
    mw_db_stats a; memset(&a, 0, sizeof a); sqlite3_file_control(db, "main", MW_FCNTL_DBSTATS, &a);
    mw_compact_result cr; memset(&cr, 0, sizeof cr); sqlite3_file_control(db, "main", MW_FCNTL_COMPACT, &cr);
    mw_db_stats b; memset(&b, 0, sizeof b); sqlite3_file_control(db, "main", MW_FCNTL_DBSTATS, &b);
    printf("log syncs before the compaction %llu, after %llu, pages written %llu\n", (unsigned long long)a.log_syncs, (unsigned long long)b.log_syncs, (unsigned long long)cr.pages_written);
    CHECK(cr.pages_written > 0);
    CHECK(b.log_syncs > a.log_syncs);                                                   // the compaction made the log durable first
    CHECK(mw_scalar(db, "SELECT count(*) FROM t") == 40);
    sqlite3_close(db); mw_rmdb(path);
    MW_DONE();
}
