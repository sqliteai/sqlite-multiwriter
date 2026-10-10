// A database whose page numbers go beyond 2^24 (64 GB of 4 KB pages, 8 GB of 512-byte ones). The index of the versions of the multi-process mode covered 2^24 pages whatever the file was:
// a commit that needed a page above that failed at the publication with SQLITE_FULL, after the work was done, and so did every one after it. The index is made to cover twice the size of
// the database when it is created. A sparse file of 8 GB is made here with 512-byte pages (nothing is written to it).
#include <fcntl.h>
#include <sys/stat.h>
#include "mw_test.h"
#include "multiwriter.h"

int main (void) {
#ifndef _WIN32
    char path[256]; mw_tmpdb(path, sizeof path, "bigdb");
    sqlite3 *p; CHECK_RC(sqlite3_open_v2(path, &p, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, MW_PLAIN_VFS), SQLITE_OK);
    CHECK_RC(mw_exec(p, "PRAGMA page_size=512; PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, v)"), SQLITE_OK);
    for (int i = 1; i <= 50; i++) { char q[80]; snprintf(q, sizeof q, "INSERT INTO t VALUES (%d, '0123456789')", i); CHECK_RC(mw_exec(p, q), SQLITE_OK); }
    CHECK_RC(mw_exec(p, "PRAGMA wal_checkpoint(TRUNCATE)"), SQLITE_OK);
    sqlite3_close(p);
    uint32_t pages = (1u << 24) + 100;
    uint8_t be[4] = { (uint8_t)(pages >> 24), (uint8_t)(pages >> 16), (uint8_t)(pages >> 8), (uint8_t)pages };
    int fd = open(path, O_RDWR); CHECK(fd >= 0);
    CHECK(pwrite(fd, be, 4, 28) == 4);                           // the size in the header: the last page is 2^24 + 100
    CHECK(ftruncate(fd, (off_t)pages * 512) == 0);
    close(fd);
    char uri[400]; snprintf(uri, sizeof uri, "file:%s?vfs=multiwriter&mw_mp=1", path);
    sqlite3 *a = NULL; CHECK_RC(sqlite3_open_v2(uri, &a, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL), SQLITE_OK);
    CHECK(mw_scalar(a, "SELECT count(*) FROM t") == 50);
    CHECK(mw_scalar(a, "PRAGMA page_count") == (int64_t)pages);
    // rows that need new pages (the file grows by pages above 2^24)
    int bad = 0;
    for (int i = 100; i < 400; i++) {
        char q[100]; snprintf(q, sizeof q, "INSERT INTO t VALUES (%d, 'abcdefghijklmnopqrstuvwxyz0123456789abcdefghij')", i);
        int rc = mw_exec(a, q);
        if (rc != SQLITE_OK) { bad++; if (bad == 1) printf("insert %d: rc=%d\n", i, rc); }
    }
    CHECK(bad == 0);
    CHECK(mw_scalar(a, "SELECT count(*) FROM t") == 350);
    CHECK(mw_scalar(a, "PRAGMA page_count") > (int64_t)pages);
    sqlite3_close(a);
    mw_rmfiles(path);
#endif
    MW_DONE();
}
