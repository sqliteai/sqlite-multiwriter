// The engine's own file I/O (the commit log, the segments, the compaction into the database file, the shared maps) goes through these wrappers, so that tests can make the n-th
// call fail the way a full disk or a failing device does (ENOSPC, EIO), or write only part of what was asked. Nothing is armed in normal use: the cost of a wrapper is one
// relaxed load of a flag.
#ifndef MULTIWRITER_IO_H
#define MULTIWRITER_IO_H

#include <stdatomic.h>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/mman.h>
#include <stdio.h>

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
static inline int mw_io_fsync (int fd) { int e = mw_io_hit(MW_IO_SYNC, NULL); if (e) { errno = e; return -1; } return fsync(fd); }
static inline int mw_io_msync (void *a, size_t n, int fl) { int e = mw_io_hit(MW_IO_SYNC, NULL); if (e) { errno = e; return -1; } return msync(a, n, fl); }
static inline int mw_io_ftruncate (int fd, off_t n) { int e = mw_io_hit(MW_IO_TRUNC, NULL); if (e) { errno = e; return -1; } return ftruncate(fd, n); }
static inline int mw_io_rename (const char *a, const char *b) { int e = mw_io_hit(MW_IO_RENAME, NULL); if (e) { errno = e; return -1; } return rename(a, b); }
static inline void *mw_io_mmap (void *a, size_t n, int prot, int fl, int fd, off_t off) { int e = mw_io_hit(MW_IO_MAP, NULL); if (e) { errno = e; return MAP_FAILED; } return mmap(a, n, prot, fl, fd, off); }

#endif
