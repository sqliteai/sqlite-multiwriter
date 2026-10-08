// The operating system calls of the engine (src/multiwriter_os.h): what the engine relies on, checked one by one on the platform it runs on. On POSIX it checks the system calls; on Windows, the layer
// that stands for them.
#include "mw_test.h"
#include "multiwriter_io.h"

static void rd (int fd, off_t off, size_t n, char *out) { memset(out, 0, n); (void)pread(fd, out, n, off); }

int main (void) {
    char base[300]; mw_tmpdb(base, sizeof base, "os"); char a[340], b[340];
    snprintf(a, sizeof a, "%s-a", base); snprintf(b, sizeof b, "%s-b", base);
    unlink(a); unlink(b);

    // 1. open, write and read at an offset, size, truncate (shrink and grow)
    int fd = open(a, O_RDWR | O_CREAT | O_TRUNC, 0600);
    CHECK(fd >= 0);
    CHECK(pwrite(fd, "hello", 5, 0) == 5);
    CHECK(pwrite(fd, "world", 5, 4096) == 5);                                   // (a hole of zeros between)
    char buf[16]; rd(fd, 0, 5, buf); CHECK(memcmp(buf, "hello", 5) == 0);
    rd(fd, 4096, 5, buf); CHECK(memcmp(buf, "world", 5) == 0);
    rd(fd, 100, 4, buf); CHECK(buf[0] == 0 && buf[3] == 0);
    struct stat sb; CHECK(fstat(fd, &sb) == 0 && sb.st_size == 4101);
    CHECK(ftruncate(fd, 10) == 0); CHECK(fstat(fd, &sb) == 0 && sb.st_size == 10);
    CHECK(ftruncate(fd, 8192) == 0); CHECK(fstat(fd, &sb) == 0 && sb.st_size == 8192);
    rd(fd, 0, 5, buf); CHECK(memcmp(buf, "hello", 5) == 0);
    struct iovec iv[2] = { { (void *)"ab", 2 }, { (void *)"cdef", 4 } };
    CHECK(pwritev(fd, iv, 2, 100) == 6); rd(fd, 100, 6, buf); CHECK(memcmp(buf, "abcdef", 6) == 0);
    CHECK(fsync(fd) == 0);
    CHECK(sysconf(_SC_PAGESIZE) >= 4096);

    // 2. the file is replaced by a rename while it is open (the engine's atomic replace of its log), and the old one stays readable through its descriptor
    int nfd = open(b, O_RDWR | O_CREAT | O_TRUNC, 0600); CHECK(nfd >= 0); CHECK(pwrite(nfd, "NEW", 3, 0) == 3);
    int rc = rename(b, a);
    printf("rename over an open file: rc=%d errno=%d\n", rc, errno);
    CHECK(rc == 0);
    rd(fd, 0, 5, buf); CHECK(memcmp(buf, "hello", 5) == 0);                      // the old file, through the old descriptor
    int again = open(a, O_RDWR, 0600); CHECK(again >= 0); rd(again, 0, 3, buf); CHECK(memcmp(buf, "NEW", 3) == 0); close(again);
    close(fd); close(nfd);

    // 3. a file that is open is deleted, and its name is free at once
    fd = open(a, O_RDWR, 0600); CHECK(fd >= 0);
    rc = unlink(a);
    printf("unlink of an open file: rc=%d errno=%d\n", rc, errno);
    CHECK(rc == 0);
    int fresh = open(a, O_RDWR | O_CREAT | O_EXCL, 0600);
    printf("the name again: fd=%d errno=%d\n", fresh, errno);
    CHECK(fresh >= 0);
    if (fresh >= 0) { CHECK(pwrite(fresh, "X", 1, 0) == 1); close(fresh); }
    close(fd); unlink(a);

    // 4. flock between two descriptors of one file: exclusive without waiting, shared, the conversion from exclusive to shared, and the last one to leave
    int f1 = open(a, O_RDWR | O_CREAT, 0600), f2 = open(a, O_RDWR, 0600); CHECK(f1 >= 0 && f2 >= 0);
    CHECK(flock(f1, LOCK_EX | LOCK_NB) == 0);
    CHECK(flock(f2, LOCK_EX | LOCK_NB) != 0);
    CHECK(flock(f2, LOCK_SH | LOCK_NB) != 0);                                   // (the first one is initialising: the others wait)
    CHECK(flock(f1, LOCK_SH) == 0);                                             // the conversion: now the others may come in
    CHECK(flock(f2, LOCK_SH | LOCK_NB) == 0);
    CHECK(flock(f1, LOCK_EX | LOCK_NB) != 0);                                   // (not alone)
    CHECK(flock(f1, LOCK_UN) == 0);
    CHECK(flock(f2, LOCK_EX | LOCK_NB) == 0);                                   // alone: the upgrade works
    CHECK(flock(f1, LOCK_SH | LOCK_NB) != 0);
    CHECK(flock(f2, LOCK_UN) == 0);
    CHECK(flock(f1, LOCK_EX) == 0);                                             // (blocking, as a mutex)
    CHECK(flock(f2, LOCK_EX | LOCK_NB) != 0);
    CHECK(flock(f1, LOCK_UN) == 0);
    CHECK(flock(f2, LOCK_EX | LOCK_NB) == 0);
    CHECK(pwrite(f1, "data", 4, 0) == 4);                                       // the lock stops no write of data
    rd(f2, 0, 4, buf); CHECK(memcmp(buf, "data", 4) == 0);
    close(f1); close(f2); unlink(a);

    // 5. a shared mapping of a file: what is stored through it is read from the file, and the other way
    fd = open(a, O_RDWR | O_CREAT | O_TRUNC, 0600); CHECK(fd >= 0); CHECK(ftruncate(fd, 65536) == 0);
    char *m = mmap(NULL, 65536, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    CHECK(m != MAP_FAILED);
    if (m != MAP_FAILED) {
        memcpy(m + 10, "mapped", 6); CHECK(msync(m, 65536, MS_SYNC) == 0);
        rd(fd, 10, 6, buf); CHECK(memcmp(buf, "mapped", 6) == 0);
        CHECK(pwrite(fd, "pwrit", 5, 200) == 5); CHECK(memcmp(m + 200, "pwrit", 5) == 0);
        CHECK(munmap(m, 65536) == 0);
    }
    void *an = mmap(NULL, 1 << 20, PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE, -1, 0);
    CHECK(an != MAP_FAILED); if (an != MAP_FAILED) { memset(an, 7, 1 << 20); CHECK(munmap(an, 1 << 20) == 0); }
    close(fd); unlink(a);

#ifdef _WIN32       // (POSIX locks belong to the process, so two descriptors of one process never exclude each other: the Windows layer is tested here, the POSIX one by the tests of processes)
    // 6. byte-range locks (the liveness bytes of the processes): one lock excludes another handle, and a free byte tests free
    f1 = open(a, O_RDWR | O_CREAT, 0600); f2 = open(a, O_RDWR, 0600); CHECK(f1 >= 0 && f2 >= 0);
    struct flock fl; memset(&fl, 0, sizeof fl); fl.l_type = F_WRLCK; fl.l_whence = SEEK_SET; fl.l_start = 1000; fl.l_len = 1;
    CHECK(fcntl(f1, F_SETLK, &fl) == 0);
    struct flock q = fl; CHECK(fcntl(f2, F_SETLK, &q) != 0);
    q = fl; q.l_type = F_WRLCK; CHECK(fcntl(f2, F_GETLK, &q) == 0);
    printf("F_GETLK of a byte locked by the other descriptor: l_type=%d (F_UNLCK is %d)\n", q.l_type, F_UNLCK);
    fl.l_type = F_UNLCK; CHECK(fcntl(f1, F_SETLK, &fl) == 0);
    q = fl; q.l_type = F_WRLCK; CHECK(fcntl(f2, F_GETLK, &q) == 0 && q.l_type == F_UNLCK);
    close(f1); close(f2); unlink(a);
#endif

    mw_rmdb(base);
    MW_DONE();
}
