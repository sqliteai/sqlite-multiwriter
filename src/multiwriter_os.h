//
//  multiwriter_os.h
//  sqlite-multiwriter
//
//  The operating system calls of the engine. On POSIX systems this includes the system headers and nothing else. On Windows (MinGW-w64) it declares the same calls, implemented in multiwriter_os_win.c on top of
//  the Win32 API with the semantics that the engine relies on and that Windows does not give its C runtime: files opened so that they can be deleted and renamed while they are open (the engine swaps the
//  files of the log under the feet of its own threads), a delete and a rename that take effect at once (POSIX semantics: the name is free for the next file), pread/pwrite at an offset, shared mappings of
//  files, and flock() and fcntl() byte-range locks.
//
//  What the Windows build does not have: the mode of several processes (URI mw_mp=1: the log is a mapped file that all of them append to and truncate, which Windows does not allow); a database opened with it
//  is refused (SQLITE_CANTOPEN). Threads of one process, the whole engine, work.
//
#ifndef MULTIWRITER_OS_H
#define MULTIWRITER_OS_H

#ifndef _WIN32

#include <unistd.h>
#include <fcntl.h>
#include <sched.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/file.h>
#include <sys/uio.h>
#include <dirent.h>
#define MW_OS_POSIX 1

#else   // ---- Windows ----

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <io.h>
#include <fcntl.h>
#include <process.h>
#include <sched.h>
#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <strings.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <direct.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#ifndef EDQUOT
#define EDQUOT 122
#endif
#ifndef EWOULDBLOCK
#define EWOULDBLOCK EAGAIN
#endif
static inline char *strcasestr (const char *hay, const char *needle) {
    size_t n = strlen(needle);
    if (!n) return (char *)hay;
    for (; *hay; hay++) if (strncasecmp(hay, needle, n) == 0) return (char *)hay;
    return NULL;
}

#ifdef __cplusplus
extern "C" {
#endif

// files: open() creates them with every share mode (read, write, delete) and opens the C runtime descriptor on the handle
#ifndef O_NOFOLLOW
#define O_NOFOLLOW 0
#endif
#ifndef O_CLOEXEC
#define O_CLOEXEC 0
#endif
int mw_win_open (const char *path, int flags, ...);
#undef open
#define open mw_win_open

ssize_t mw_win_pread (int fd, void *buf, size_t n, off_t off);
ssize_t mw_win_pwrite (int fd, const void *buf, size_t n, off_t off);
#undef pread
#undef pwrite
#define pread mw_win_pread
#define pwrite mw_win_pwrite

struct iovec { void *iov_base; size_t iov_len; };
ssize_t mw_win_pwritev (int fd, const struct iovec *iov, int cnt, off_t off);
#define pwritev mw_win_pwritev

int mw_win_fsync (int fd);
int mw_win_ftruncate (int fd, off_t len);
int mw_win_rename (const char *from, const char *to);       // replaces the destination, even if it is open (the engine's atomic replace of a file)
int mw_win_unlink (const char *path);                      // the name is free at once, even if the file is open
#undef fsync
#undef ftruncate
#undef rename
#undef unlink
#define fsync mw_win_fsync
#define fdatasync mw_win_fsync
#define ftruncate mw_win_ftruncate
#define rename mw_win_rename
#define unlink mw_win_unlink
#define mkdir(path, mode) _mkdir(path)

// sysconf: the page size and the number of processors
#define _SC_PAGESIZE 1
#define _SC_NPROCESSORS_ONLN 2
long mw_win_sysconf (int name);
#define sysconf mw_win_sysconf

// mappings of files and of memory (MAP_ANON)
#define PROT_NONE 0
#define PROT_READ 1
#define PROT_WRITE 2
#define MAP_SHARED 1
#define MAP_PRIVATE 2
#define MAP_ANON 4
#define MAP_ANONYMOUS MAP_ANON
#define MAP_FAILED ((void *)-1)
#define MS_ASYNC 1
#define MS_SYNC 4
void *mw_win_mmap (void *addr, size_t len, int prot, int flags, int fd, off_t off);
int mw_win_munmap (void *addr, size_t len);
int mw_win_msync (void *addr, size_t len, int flags);
#define mmap mw_win_mmap
#define munmap mw_win_munmap
#define msync mw_win_msync

struct flock { short l_type; short l_whence; off_t l_start; off_t l_len; int l_pid; };   // (before the macro below renames flock)

// flock(): advisory whole-file locks with the conversion that the engine needs (exclusive to shared without a moment in which somebody else can take it)
#define LOCK_SH 1
#define LOCK_EX 2
#define LOCK_NB 4
#define LOCK_UN 8
int mw_win_flock (int fd, int op);
#define flock(fd, op) mw_win_flock(fd, op)            // (a macro with arguments: `struct flock` below stays what it is)

// fcntl(): the byte-range locks (F_SETLK, F_SETLKW, F_GETLK) and F_FULLFSYNC does not exist
#define F_GETLK 5
#define F_SETLK 6
#define F_SETLKW 7
#define F_RDLCK 0
#define F_WRLCK 1
#define F_UNLCK 2
int mw_win_fcntl (int fd, int cmd, struct flock *fl);
#define fcntl(fd, cmd, arg) mw_win_fcntl(fd, cmd, arg)

// a reservation of disk space (0 or an errno), and: a process is alive
int mw_win_reserve (int fd, uint64_t to);
int mw_win_pid_alive (int pid);

#ifdef __cplusplus
}
#endif

#endif   // _WIN32
#endif
