// The format of the files that outlive a process (the log "<db>-mw", the segments "<db>-mw.N") is versioned (docs/format.md): a file of another version, one that uses a feature that this library
// does not know, and one with the magic of another format are refused when the database is opened, with a message, and are left exactly as they were (the commits that they hold are not dropped as
// damaged and not replaced by a new log). The same file with its own header opens and holds every commit. A process that runs another version of the engine and has the database open is refused too.
#include <fcntl.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include "mw_test.h"
#include "multiwriter.h"

char *mw_sidecar_path (const char *dbpath, const char *suffix);

static uint64_t fnv64 (uint64_t h, const void *p, size_t n) {            // (the checksum of the header: the same as the engine's)
    const unsigned char *b = p;
    while (n >= 8) { uint64_t w; memcpy(&w, b, 8); h ^= w; h *= 0x9E3779B97F4A7C15ull; h ^= h >> 32; b += 8; n -= 8; }
    while (n--) { h ^= *b++; h *= 1099511628211ull; }
    return h;
}
typedef struct { char magic[8]; uint32_t version, pgsz; uint64_t base_epoch, salt, features, reserved[2], cksum; } hdr_t;      // (the header of the log and of a segment: 64 bytes)
_Static_assert(sizeof(hdr_t) == 64, "header");

static int read_hdr (const char *file, hdr_t *h) { FILE *f = fopen(file, "rb"); if (!f) return -1; size_t n = fread(h, 1, sizeof *h, f); fclose(f); return n == sizeof *h ? 0 : -1; }
static int write_hdr (const char *file, hdr_t *h, int fix_cksum) {
    if (fix_cksum) { hdr_t c = *h; c.cksum = 0; h->cksum = fnv64(1469598103934665603ull, &c, sizeof c); }
    FILE *f = fopen(file, "r+b"); if (!f) return -1; size_t n = fwrite(h, 1, sizeof *h, f); fclose(f); return n == sizeof *h ? 0 : -1;
}
static long file_hash (const char *file) { FILE *f = fopen(file, "rb"); if (!f) return -1; unsigned long h = 5381; int c; while ((c = fgetc(f)) != EOF) h = h * 33 + (unsigned)c; fclose(f); return (long)h; }

static int open_db (const char *path, int mp, sqlite3 **db) {
    char uri[300]; snprintf(uri, sizeof uri, "file:%s?mw=2%s", path, mp ? "&mw_mp=1" : "");
    int rc = sqlite3_open_v2(uri, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
    if (rc == SQLITE_OK) { sqlite3_extended_result_codes(*db, 1); }
    return rc;
}
// a database with 30 commits in its log, left by a process that died (no close: nothing is folded into the file)
static void make_log (const char *path, int mp) {
    mw_rmfiles(path);
    sqlite3 *s; sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix"); mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, v)"); sqlite3_close(s);
    pid_t p = fork();
    if (p == 0) { sqlite3 *db; if (open_db(path, mp, &db) != SQLITE_OK) _exit(2); for (int i = 1; i <= 30; i++) { char q[80]; snprintf(q, sizeof q, "INSERT INTO t VALUES(%d, 'x%d')", i, i); if (mw_exec(db, q) != SQLITE_OK) _exit(3); } _exit(0); }
    int st; waitpid(p, &st, 0); CHECK(WIFEXITED(st) && WEXITSTATUS(st) == 0);
}
static int count_rows (const char *path, int mp) { sqlite3 *db; if (open_db(path, mp, &db) != SQLITE_OK) { sqlite3_close(db); return -1; } int64_t n = mw_scalar(db, "SELECT count(*) FROM t"); sqlite3_close(db); return (int)n; }

static void refused (const char *path, int mp, const char *file, const char *what, void (*tamper)(hdr_t *)) {
    make_log(path, mp);
    hdr_t good, h; CHECK(read_hdr(file, &good) == 0); h = good;
    CHECK(memcmp(good.magic, mp ? "MWLOG002" : "MWLOG002", 8) == 0);
    tamper(&h); CHECK(write_hdr(file, &h, strncmp(h.magic, good.magic, 8) == 0) == 0);
    long before = file_hash(file);
    sqlite3 *db = NULL; int rc = open_db(path, mp, &db);
    if (rc == SQLITE_OK) rc = mw_exec(db, "SELECT count(*) FROM t");      // (an open that succeeded and then fails at the first statement is as good)
    if (getenv("MW_VERBOSE")) printf("  %s: rc %d\n", what, rc);
    CHECK((rc & 0xff) == SQLITE_CANTOPEN); sqlite3_close(db);
    CHECK(file_hash(file) == before);                                      // (left as it was: not replaced, not truncated)
    CHECK(write_hdr(file, &good, 0) == 0);                                 // the header that the file had: it opens, with every commit
    CHECK(count_rows(path, mp) == 30);
    mw_rmfiles(path);
}
static void t_version (hdr_t *h) { h->version = 2; }
static void t_version0 (hdr_t *h) { h->version = 0; }
static void t_feature (hdr_t *h) { h->features = 1; }
static void t_magic (hdr_t *h) { memcpy(h->magic, "MWLOG003", 8); }

int main (void) {
    char path[256], file[300]; mw_tmpdb(path, sizeof path, "format");
    snprintf(file, sizeof file, "%s-mw", path);
    refused(path, 0, file, "log, version 2", t_version);
    refused(path, 0, file, "log, version 0", t_version0);
    refused(path, 0, file, "log, a feature that is not known", t_feature);
    refused(path, 0, file, "log, magic of another format", t_magic);
    snprintf(file, sizeof file, "%s-mw.1", path);
    refused(path, 1, file, "segment, version 2", t_version);
    refused(path, 1, file, "segment, a feature that is not known", t_feature);
    refused(path, 1, file, "segment, magic of another format", t_magic);
    // a process that runs another version has the database open: the header of the shared state has another magic
    {
        mw_rmfiles(path); sqlite3 *a; CHECK_RC(open_db(path, 1, &a), SQLITE_OK); mw_exec(a, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, v)");
        char *lk = mw_sidecar_path(path, "mwlock"); int fd = lk ? open(lk, O_RDWR) : -1; CHECK(fd >= 0);
        if (fd >= 0) {
            uint64_t *magic = mmap(NULL, 4096, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0); CHECK(magic != MAP_FAILED);
            if (magic != MAP_FAILED) {
                uint64_t keep = *magic; *magic = (keep & 0xffffffff00000000ull) | 0x31303032ull;       // "MWMP1002"
                pid_t p = fork();
                if (p == 0) { sqlite3 *b; int rc = open_db(path, 1, &b); _exit((rc & 0xff) == SQLITE_CANTOPEN ? 10 : 11); }
                int st; waitpid(p, &st, 0); CHECK(WIFEXITED(st) && WEXITSTATUS(st) == 10);
                *magic = keep;
                p = fork();
                if (p == 0) { sqlite3 *b; int rc = open_db(path, 1, &b); _exit(rc == SQLITE_OK ? 10 : 11); }
                waitpid(p, &st, 0); CHECK(WIFEXITED(st) && WEXITSTATUS(st) == 10);                      // (and the right one is let in)
                munmap(magic, 4096);
            }
            close(fd);
        }
        sqlite3_free(lk); sqlite3_close(a); mw_rmfiles(path);
    }
    printf("test/mw_format.c: %d failure(s)\n", mw_failures);
    return mw_failures ? 1 : 0;
}
