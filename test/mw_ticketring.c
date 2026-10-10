// The ring of the tickets of the publication lock has 1024 places. With 1024 or more tickets outstanding (as many dead ones in front that nobody has skipped yet) a new ticket shared its place with the
// one at the head: its owner is alive, so the dead owner of the head was never skipped and the lock was never given again (every commit of the database hung). No more than 1024 are out now, and a
// newcomer that finds the ring full skips the dead head itself.
#include <sys/wait.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <signal.h>
#include "mw_test.h"
#include "multiwriter.h"
#include "multiwriter_internal.h"
char *mw_sidecar_path (const char *db, const char *sfx);

int main (void) {
    char path[256]; mw_tmpdb(path, sizeof path, "ticketring"); char uri[400];
    snprintf(uri, sizeof uri, "file:%s?vfs=multiwriter&mw_mp=1", path);
    sqlite3 *a = NULL; CHECK_RC(sqlite3_open_v2(uri, &a, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL), SQLITE_OK);
    CHECK_RC(mw_exec(a, "PRAGMA journal_mode=WAL; CREATE TABLE t(x)"), SQLITE_OK);
    CHECK_RC(mw_exec(a, "INSERT INTO t VALUES (1)"), SQLITE_OK);
    char *mp = mw_sidecar_path(path, "mwlock"); int fd = open(mp, O_RDWR);
    mw_shm *sh = fd >= 0 ? mmap(NULL, sizeof(mw_shm), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0) : MAP_FAILED;
    CHECK(sh != MAP_FAILED); if (sh == MAP_FAILED) return 1;
    pid_t dead = fork(); if (dead == 0) _exit(0);
    int st; waitpid(dead, &st, 0);
    // 1024 tickets taken by a process that is gone, none served
    uint64_t s = atomic_load(&sh->pub_serving);
    CHECK(atomic_load(&sh->pub_owner) == 0);
    for (int i = 0; i < 1024; i++) atomic_store(&sh->pub_tk_pid[(s + (uint64_t)i) % 1024], (int32_t)dead);
    atomic_store(&sh->pub_ticket, s + 1024);
    pid_t c = fork();
    if (c == 0) {
        alarm(60);
        sqlite3 *d; if (sqlite3_open_v2(uri, &d, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL) != SQLITE_OK) _exit(2);
        int rc = mw_exec(d, "INSERT INTO t VALUES (2)");
        _exit(rc == SQLITE_OK && mw_scalar(d, "SELECT count(*) FROM t") == 2 ? 0 : 3);
    }
    waitpid(c, &st, 0);
    printf("1024 dead tickets in front: the commit %s\n", WIFEXITED(st) && WEXITSTATUS(st) == 0 ? "went through" : WIFSIGNALED(st) ? "hung (alarm)" : "failed");
    CHECK(WIFEXITED(st) && WEXITSTATUS(st) == 0);
    CHECK(atomic_load(&sh->pub_ticket) - atomic_load(&sh->pub_serving) < 1024);
    sqlite3_close(a); munmap(sh, sizeof(mw_shm)); close(fd); sqlite3_free(mp); mw_rmfiles(path);
    MW_DONE();
}
