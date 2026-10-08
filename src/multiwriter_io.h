// The engine's own file I/O (the commit log, the segments, the compaction into the database file, the shared maps) goes through these wrappers, so that tests can make the n-th
// call fail the way a full disk or a failing device does (ENOSPC, EIO), or write only part of what was asked. Nothing is armed in normal use: the cost of a wrapper is one
// relaxed load of a flag.
#ifndef MULTIWRITER_IO_H
#define MULTIWRITER_IO_H

#include <stdatomic.h>
#include <errno.h>
#include "multiwriter_os.h"
#include <stdio.h>
#if defined(__linux__)
#include <linux/falloc.h>
#endif

#include "multiwriter.h"

extern _Atomic int mw_io_armed;
int mw_io_fault (int kind, size_t *partial);                        // slow path: 0, or the errno the call fails with (*partial set: a write takes about half of what it was given first, in whole 512-byte blocks, and the rest fails)

// Arms the injection. kinds: the calls that count (MW_IO_*). The nth matching call from now on fails with err; with `sticky` every matching call after it fails too (a disk that stays
// full); with `shortw` a failing write first writes half of what it was given (a torn write). mw_io_fault_calls() counts the matching calls since the last arm.
void mw_io_fault_arm (int kinds, long nth, int err, int sticky, int shortw);
void mw_io_fault_disarm (void);
long mw_io_fault_calls (void);

static inline int mw_io_hit (int kind, size_t *partial) { return atomic_load_explicit(&mw_io_armed, memory_order_relaxed) ? mw_io_fault(kind, partial) : 0; }

static inline ssize_t mw_io_pwrite (int fd, const void *p, size_t n, off_t off) {
    size_t part = 0; int e = mw_io_hit(MW_IO_WRITE, &part);
    if (e) {                                                         // (a torn write is a whole number of 512-byte blocks: what is inside one block is written or not, never half)
        if (part && n > 512) { size_t h = (n / 2) & ~(size_t)511; ssize_t w = pwrite(fd, p, h ? h : 512, off); if (w > 0) return w; }
        errno = e; return -1;
    }
    return pwrite(fd, p, n, off);
}
static inline ssize_t mw_io_pwritev (int fd, const struct iovec *iov, int cnt, off_t off) {
    size_t part = 0; int e = mw_io_hit(MW_IO_WRITE, &part);
    if (e) { errno = e; return -1; }
    return pwritev(fd, iov, cnt, off);
}
static inline ssize_t mw_io_pread (int fd, void *p, size_t n, off_t off) { int e = mw_io_hit(MW_IO_READ, NULL); if (e) { errno = e; return -1; } return pread(fd, p, n, off); }
// Where `fsync` leaves the data in the disk's own cache (macOS: the drive may still lose it at a power failure) the data goes to the platform with F_FULLFSYNC when `mw_fullfsync` is set (URI mw_fullfsync=1,
// MW_FULLFSYNC=1; the same promise as PRAGMA fullfsync of SQLite, which is off by default there too). Linux's fsync already asks the device to flush.
extern _Atomic int mw_fullfsync;
static inline int mw_sys_fsync (int fd) {
#if defined(__APPLE__) && defined(F_FULLFSYNC)
    if (atomic_load_explicit(&mw_fullfsync, memory_order_relaxed) && fcntl(fd, F_FULLFSYNC) == 0) return 0;      // (when the file system does not know it: the plain fsync, as SQLite does)
#endif
    return fsync(fd);
}
void mw_set_fullfsync (int on);
// The mode of the files that the engine creates next to a database (the log, the segments, the maps): that of the database file without the bits of execution, 0600 when it is not there. A database that only
// its owner can read must not have its pages in a log that anybody can read, and one that a group shares must have its sidecars open to the group.
static inline mode_t mw_file_mode (const char *dbpath) { struct stat sb; return (dbpath && stat(dbpath, &sb) == 0) ? (mode_t)(sb.st_mode & 0666) : (mode_t)0600; }
static inline int mw_io_fsync (int fd) { int e = mw_io_hit(MW_IO_SYNC, NULL); if (e) { errno = e; return -1; } return mw_sys_fsync(fd); }
static inline int mw_io_msync (void *a, size_t n, int fl) { int e = mw_io_hit(MW_IO_SYNC, NULL); if (e) { errno = e; return -1; } return msync(a, n, fl); }
static inline int mw_io_ftruncate (int fd, off_t n) { int e = mw_io_hit(MW_IO_TRUNC, NULL); if (e) { errno = e; return -1; } return ftruncate(fd, n); }
static inline int mw_io_rename (const char *a, const char *b) { int e = mw_io_hit(MW_IO_RENAME, NULL); if (e) { errno = e; return -1; } return rename(a, b); }
// The SQLite result for a failed file call: a full disk is SQLITE_FULL (the application can wait for room and go on), anything else the given I/O error.
static inline int mw_io_rc (int err, int dflt) { return (err == ENOSPC || err == EDQUOT) ? SQLITE_FULL : dflt; }

// Reserves the disk blocks for [from, to) of a file whose data ends at `from` (the size of the file does not change; a later write into the range cannot fail for want of space).
// 0, or the errno. The reservation is of whole blocks and all or nothing. Where the system has no such call nothing is reserved (and nothing can be promised).
static inline int mw_io_reserve (int fd, uint64_t from, uint64_t to) {
    int e = mw_io_hit(MW_IO_TRUNC, NULL); if (e) return e;
    if (to <= from) return 0;
#if defined(__APPLE__)
    fstore_t fs = { F_ALLOCATEALL, F_PEOFPOSMODE, 0, (off_t)(to - from), 0 };
    return fcntl(fd, F_PREALLOCATE, &fs) == 0 ? 0 : (errno ? errno : EIO);
#elif defined(__linux__)
    if (fallocate(fd, FALLOC_FL_KEEP_SIZE, (off_t)from, (off_t)(to - from)) == 0) return 0;
    return (errno == EOPNOTSUPP || errno == ENOSYS) ? 0 : errno;
#elif defined(_WIN32)
    return mw_win_reserve(fd, to);
#else
    (void)fd; return 0;
#endif
}
static inline void *mw_io_mmap (void *a, size_t n, int prot, int fl, int fd, off_t off) { int e = mw_io_hit(MW_IO_MAP, NULL); if (e) { errno = e; return MAP_FAILED; } return mmap(a, n, prot, fl, fd, off); }

#endif
