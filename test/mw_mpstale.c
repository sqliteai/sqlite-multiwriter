// A process that is started gets the pid of one that died while it held something in the shared header: the sync leader, the process that fills the next segment, the turn of the long
// transactions, the publication lock. Those fields name a pid that is now alive (this process, registered in the header), so nobody reaps them: every commit waited for a leader that
// never finishes, and the next roll of a segment looped under the publication lock. A process that registers clears what carries its own pid.
// The state is made here: a process holds the database open (the header exists), the test writes its own pid in the fields and opens the database, as the new process would have.
#include <pthread.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <fcntl.h>
#include "mw_test.h"
#include "multiwriter.h"
#include "multiwriter_internal.h"

static void *watchdog (void *a) { (void)a; for (int i = 0; i < 400; i++) { usleep(100000); if (*(volatile int *)a == 1) return NULL; } printf("FAIL: the commits hang\n"); fflush(stdout); _exit(1); }

int main (void) {
    setenv("MW_SEG_MB", "1", 1);                                   // a roll every few hundred commits: the filling of the next segment is needed
    setenv("MW_SIDECAR_DIR", "db", 1);                             // the header next to the database, where the test can find it
    char path[300], uri[400]; mw_tmpdb(path, sizeof path, "mpstale");
    snprintf(uri, sizeof uri, "file:%s?vfs=multiwriter&mw_mp=1&mw_gc=0", path);
    sqlite3 *s; CHECK_RC(sqlite3_open_v2(path, &s, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, MW_PLAIN_VFS), SQLITE_OK);
    CHECK_RC(mw_exec(s, "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER, pad BLOB); INSERT INTO t VALUES (1, 0, NULL)"), SQLITE_OK);
    sqlite3_close(s);
    int up[2], down[2]; CHECK(pipe(up) == 0 && pipe(down) == 0);
    pid_t holder = fork();
    if (holder == 0) {                                              // holds the database open: the header and the segments exist while the test registers
        sqlite3 *a; if (sqlite3_open_v2(uri, &a, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL) != SQLITE_OK) _exit(2);
        mw_exec(a, "UPDATE t SET n = n + 1 WHERE id = 1");
        char c = 'r'; (void)!write(up[1], &c, 1);
        (void)!read(down[0], &c, 1);
        sqlite3_close(a);
        _exit(0);
    }
    char c; CHECK(read(up[0], &c, 1) == 1);
    char *hp = mw_sidecar_path(path, "mwlock");
    int fd = open(hp, O_RDWR);
    CHECK(fd >= 0);
    size_t len = (sizeof(mw_shm) + 4095) & ~(size_t)4095;
    mw_shm *sh = fd >= 0 ? mmap(NULL, len, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0) : MAP_FAILED;
    CHECK(sh != MAP_FAILED);
    if (sh == MAP_FAILED) return 1;
    const int32_t me = (int32_t)getpid();
    // as a process that died holding these, and whose pid this one got
    atomic_store(&sh->sy_leader, me);
    atomic_store(&sh->log_fill_pid, me);
    atomic_store(&sh->hot_owner, me); atomic_store(&sh->hot_held, 1);
    atomic_store(&sh->compact_req_pid, me);
    atomic_store(&sh->adm_slot_pid[5], me);
    atomic_store(&sh->pub_owner, me);                               // (and the publication lock, with no record pending)
    volatile int finished = 0;
    pthread_t wd; pthread_create(&wd, NULL, watchdog, (void *)&finished);
    sqlite3 *a = NULL; CHECK_RC(sqlite3_open_v2(uri, &a, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL), SQLITE_OK);
    sqlite3_busy_timeout(a, 5000);
    CHECK(atomic_load(&sh->sy_leader) == 0 || atomic_load(&sh->sy_leader) != me);
    CHECK(atomic_load(&sh->log_fill_pid) != me);
    CHECK(atomic_load(&sh->hot_owner) != me);
    CHECK(atomic_load(&sh->compact_req_pid) != me);
    CHECK(atomic_load(&sh->adm_slot_pid[5]) != me);
    int ok = 0;
    for (int i = 0; i < 700; i++) if (mw_exec(a, "UPDATE t SET n = n + 1, pad = randomblob(3000) WHERE id = 1") == SQLITE_OK) ok++;
    CHECK(ok == 700);                                               // (more than a segment: a roll and the filling of the next one)
    CHECK(mw_scalar(a, "SELECT n FROM t") == 701);
    finished = 1;
    pthread_join(wd, NULL);
    sqlite3_close(a);
    char q = 'q'; (void)!write(down[1], &q, 1);
    waitpid(holder, NULL, 0);
    munmap(sh, len); close(fd); sqlite3_free(hp);
    mw_rmdb(path);
    MW_DONE();
}
