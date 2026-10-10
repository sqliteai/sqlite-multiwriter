// A process that is killed while it cleans the slot of a dead one left the slot at -1 for ever: nobody takes a slot that is negative, and the slots (8192 of them) are lost one by one. The cleaner
// marks the slot with its own pid now, and the next one that finds the cleaner dead takes the slot over.
#include <sys/wait.h>
#include <sys/mman.h>
#include <fcntl.h>
#include "mw_test.h"
#include "multiwriter.h"
#include "multiwriter_internal.h"
char *mw_sidecar_path (const char *db, const char *sfx);

int main (void) {
    char path[256]; mw_tmpdb(path, sizeof path, "slotstuck"); char uri[400];
    snprintf(uri, sizeof uri, "file:%s?vfs=multiwriter&mw_mp=1", path);
    sqlite3 *a = NULL; CHECK_RC(sqlite3_open_v2(uri, &a, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL), SQLITE_OK);
    CHECK_RC(mw_exec(a, "PRAGMA journal_mode=WAL; CREATE TABLE t(x)"), SQLITE_OK);
    CHECK_RC(mw_exec(a, "INSERT INTO t VALUES (1)"), SQLITE_OK);
    char *mp = mw_sidecar_path(path, "mwlock"); int fd = open(mp, O_RDWR);
    mw_shm *sh = fd >= 0 ? mmap(NULL, sizeof(mw_shm), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0) : MAP_FAILED;
    CHECK(sh != MAP_FAILED); if (sh == MAP_FAILED) return 1;
    pid_t dead = fork(); if (dead == 0) _exit(0);
    int st; waitpid(dead, &st, 0);
    // every free slot is taken by this (live) process but one, which a cleaner that died left half cleaned
    int me = (int)getpid(), stuck = -1; static unsigned char ours[MW_MP_SLOTS];
    for (int i = 0; i < MW_MP_SLOTS; i++) {
        int32_t z = 0;
        if (atomic_compare_exchange_strong(&sh->slots[i].pid, &z, stuck < 0 ? -((int32_t)dead + 1) : me)) { ours[i] = 1; if (stuck < 0) stuck = i; }
    }
    CHECK(stuck >= 0);
    sqlite3 *b = NULL; CHECK_RC(sqlite3_open_v2(uri, &b, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL), SQLITE_OK);
    int rc = mw_exec(b, "INSERT INTO t VALUES (2)");
    long long n = mw_scalar(b, "SELECT count(*) FROM t");
    printf("slot %d left by a cleaner that died (pid %d): insert rc=%d, rows %lld\n", stuck, (int)dead, rc, n);
    CHECK_RC(rc, SQLITE_OK);
    CHECK(n == 2);
    sqlite3_close(b);
    for (int i = 0; i < MW_MP_SLOTS; i++) if (ours[i]) { int32_t v = atomic_load(&sh->slots[i].pid); if (v == me || v < 0) atomic_store(&sh->slots[i].pid, 0); }
    (void)me; sqlite3_close(a); munmap(sh, sizeof(mw_shm)); close(fd); sqlite3_free(mp); mw_rmfiles(path);
    MW_DONE();
}
