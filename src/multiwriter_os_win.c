//
//  multiwriter_os_win.c
//  sqlite-multiwriter
//
//  The POSIX calls of the engine on Windows (see multiwriter_os.h). Only compiled on Windows.
//
#ifdef _WIN32

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <wchar.h>
#include "multiwriter_os.h"

// the macros of multiwriter_os.h must not rewrite the calls of this file to themselves
#undef open
#undef pread
#undef pwrite
#undef fsync
#undef ftruncate
#undef rename
#undef unlink
#undef mmap
#undef munmap
#undef msync
#undef flock
#undef fcntl
#undef sysconf
#undef nanosleep
#undef usleep

// ---- errors ----
static int win_errno (DWORD e) {
    switch (e) {
        case ERROR_FILE_NOT_FOUND: case ERROR_PATH_NOT_FOUND: case ERROR_INVALID_DRIVE: return ENOENT;
        case ERROR_ACCESS_DENIED: case ERROR_SHARING_VIOLATION: case ERROR_LOCK_VIOLATION: case ERROR_USER_MAPPED_FILE: case ERROR_DELETE_PENDING: return EACCES;
        case ERROR_FILE_EXISTS: case ERROR_ALREADY_EXISTS: return EEXIST;
        case ERROR_DISK_FULL: case ERROR_HANDLE_DISK_FULL: return ENOSPC;
        case ERROR_NOT_ENOUGH_MEMORY: case ERROR_OUTOFMEMORY: case ERROR_COMMITMENT_LIMIT: return ENOMEM;
        case ERROR_INVALID_HANDLE: return EBADF;
        case ERROR_INVALID_PARAMETER: return EINVAL;
        case ERROR_NOT_SUPPORTED: return ENOSYS;
        case ERROR_HANDLE_EOF: return 0;
        default: return EIO;
    }
}
static int os_trace = -1;
static void tr (const char *fmt, ...) {         // MW_OS_TRACE=1: to stderr; MW_OS_TRACE=<path>: appended to that file (a child process may not have a stderr)
    const char *t = getenv("MW_OS_TRACE"); va_list ap; va_start(ap, fmt);
    FILE *f = (t && t[0] && strcmp(t, "1") != 0) ? fopen(t, "a") : stderr;
    if (f) { fprintf(f, "[pid %lu] ", (unsigned long)GetCurrentProcessId()); vfprintf(f, fmt, ap); if (f != stderr) fclose(f); }
    va_end(ap);
}
static int fail_at (const char *what) {
    DWORD e = GetLastError();
    if (os_trace < 0) os_trace = getenv("MW_OS_TRACE") != NULL;
    if (os_trace) tr("multiwriter os: %s failed, Windows error %lu\n", what, (unsigned long)e);
    errno = win_errno(e); return -1;
}
#define fail() fail_at(__func__)

// ---- names: UTF-8 to UTF-16, with the prefix that lifts the limit of 260 characters on an absolute path ----
static wchar_t *wide_path (const char *p) {
    int n = MultiByteToWideChar(CP_UTF8, 0, p, -1, NULL, 0);
    if (n <= 0) return NULL;
    wchar_t *w = malloc(((size_t)n + 8) * sizeof(wchar_t));
    if (!w) return NULL;
    size_t off = 0;
    if (n > 240 && ((p[0] && p[1] == ':') || (p[0] == '\\' && p[1] == '\\'))) {
        if (p[0] == '\\' && p[1] == '\\') { wcscpy(w, L"\\\\?\\UNC\\"); off = 8; p += 2; } else { wcscpy(w, L"\\\\?\\"); off = 4; }
        n = MultiByteToWideChar(CP_UTF8, 0, p, -1, w + off, n);
    } else n = MultiByteToWideChar(CP_UTF8, 0, p, -1, w, n);
    if (n <= 0) { free(w); return NULL; }
    if (off) for (wchar_t *c = w + off; *c; c++) if (*c == L'/') *c = L'\\';
    return w;
}
#define HANDLE_OF(fd) ((HANDLE)_get_osfhandle(fd))

// ---- open / close ----
int mw_win_open (const char *path, int flags, ...) {
    if (os_trace < 0) os_trace = getenv("MW_OS_TRACE") != NULL;
    if (os_trace > 0) tr("multiwriter os: open(%s, %#x)\n", path, flags);
    wchar_t *w = wide_path(path);
    if (!w) { errno = ENOMEM; return -1; }
    DWORD access = (flags & O_RDWR) ? (GENERIC_READ | GENERIC_WRITE) : (flags & O_WRONLY) ? GENERIC_WRITE : GENERIC_READ;
    DWORD disp = (flags & O_CREAT) ? ((flags & O_EXCL) ? CREATE_NEW : OPEN_ALWAYS) : OPEN_EXISTING;
    HANDLE h = CreateFileW(w, access, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL, disp, FILE_ATTRIBUTE_NORMAL, NULL);
    free(w);
    if (h == INVALID_HANDLE_VALUE) { int r = fail(); if (os_trace > 0) tr("multiwriter os:   open(%s, flags %#x)\n", path, flags); return r; }
    if ((flags & O_TRUNC) && (access & GENERIC_WRITE)) {
        LARGE_INTEGER z; z.QuadPart = 0;
        if (!SetFilePointerEx(h, z, NULL, FILE_BEGIN) || !SetEndOfFile(h)) { int e = win_errno(GetLastError()); CloseHandle(h); errno = e; return -1; }
    }
    int fd = _open_osfhandle((intptr_t)h, _O_BINARY | ((access & GENERIC_WRITE) ? 0 : _O_RDONLY) | ((flags & O_APPEND) ? _O_APPEND : 0));
    if (fd < 0) { CloseHandle(h); errno = EMFILE; return -1; }
    return fd;
}

// ---- reads and writes at an offset ----
ssize_t mw_win_pread (int fd, void *buf, size_t n, off_t off) {
    HANDLE h = HANDLE_OF(fd); if (h == INVALID_HANDLE_VALUE) { errno = EBADF; return -1; }
    OVERLAPPED ov; memset(&ov, 0, sizeof ov);
    ov.Offset = (DWORD)((uint64_t)off & 0xffffffffu); ov.OffsetHigh = (DWORD)((uint64_t)off >> 32);
    DWORD got = 0, want = n > 0x40000000u ? 0x40000000u : (DWORD)n;
    if (!ReadFile(h, buf, want, &got, &ov)) { DWORD e = GetLastError(); if (e == ERROR_HANDLE_EOF) return 0; errno = win_errno(e); return -1; }
    return (ssize_t)got;
}
ssize_t mw_win_pwrite (int fd, const void *buf, size_t n, off_t off) {
    HANDLE h = HANDLE_OF(fd); if (h == INVALID_HANDLE_VALUE) { errno = EBADF; return -1; }
    OVERLAPPED ov; memset(&ov, 0, sizeof ov);
    ov.Offset = (DWORD)((uint64_t)off & 0xffffffffu); ov.OffsetHigh = (DWORD)((uint64_t)off >> 32);
    DWORD put = 0, want = n > 0x40000000u ? 0x40000000u : (DWORD)n;
    if (!WriteFile(h, buf, want, &put, &ov)) return fail();
    return (ssize_t)put;
}
ssize_t mw_win_pwritev (int fd, const struct iovec *iov, int cnt, off_t off) {
    ssize_t total = 0;
    for (int i = 0; i < cnt; i++) {
        const char *p = iov[i].iov_base; size_t left = iov[i].iov_len;
        while (left) { ssize_t w = mw_win_pwrite(fd, p, left, off + total); if (w < 0) return total ? total : -1; if (w == 0) return total; p += w; left -= (size_t)w; total += w; }
    }
    return total;
}

int mw_win_fsync (int fd) {
    HANDLE h = HANDLE_OF(fd); if (h == INVALID_HANDLE_VALUE) { errno = EBADF; return -1; }
    return FlushFileBuffers(h) ? 0 : fail();
}
int mw_win_ftruncate (int fd, off_t len) {
    HANDLE h = HANDLE_OF(fd); if (h == INVALID_HANDLE_VALUE) { errno = EBADF; return -1; }
    LARGE_INTEGER z; z.QuadPart = len;
    if (!SetFilePointerEx(h, z, NULL, FILE_BEGIN)) return fail();
    return SetEndOfFile(h) ? 0 : fail();                       // (fails with a mapping of the file open: the engine unmaps before it shrinks a file)
}

// ---- delete and rename with the semantics of POSIX: the name goes at once, whoever has the file open ----
typedef struct { DWORD Flags; } mw_disposition_ex;
typedef struct { DWORD Flags; HANDLE RootDirectory; DWORD FileNameLength; WCHAR FileName[1]; } mw_rename_ex;
enum { MW_FileDispositionInfoEx = 21, MW_FileRenameInfoEx = 22 };
enum { MW_DELETE = 0x1, MW_POSIX_SEMANTICS = 0x2, MW_REPLACE_IF_EXISTS = 0x1 };

int mw_win_unlink (const char *path) {
    if (os_trace < 0) os_trace = getenv("MW_OS_TRACE") != NULL;
    if (os_trace > 0) tr("multiwriter os: unlink(%s)\n", path);
    wchar_t *w = wide_path(path);
    if (!w) { errno = ENOMEM; return -1; }
    HANDLE h = CreateFileW(w, DELETE | SYNCHRONIZE, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL, OPEN_EXISTING, FILE_FLAG_OPEN_REPARSE_POINT, NULL);
    if (h != INVALID_HANDLE_VALUE) {
        mw_disposition_ex d = { MW_DELETE | MW_POSIX_SEMANTICS };
        BOOL ok = SetFileInformationByHandle(h, (FILE_INFO_BY_HANDLE_CLASS)MW_FileDispositionInfoEx, &d, sizeof d);
        CloseHandle(h);
        if (ok) { free(w); return 0; }
    } else if (GetLastError() == ERROR_FILE_NOT_FOUND || GetLastError() == ERROR_PATH_NOT_FOUND) { free(w); errno = ENOENT; return -1; }
    BOOL ok = DeleteFileW(w);                                 // (before Windows 10 1709: the name stays until the last handle is closed)
    free(w);
    return ok ? 0 : fail();
}
int mw_win_rename (const char *from, const char *to) {
    if (os_trace < 0) os_trace = getenv("MW_OS_TRACE") != NULL;
    if (os_trace > 0) tr("multiwriter os: rename(%s, %s)\n", from, to);
    wchar_t *wf = wide_path(from), *wt = wide_path(to);
    int rc = -1;
    if (!wf || !wt) { errno = ENOMEM; goto out; }
    HANDLE h = CreateFileW(wf, DELETE | SYNCHRONIZE, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL, OPEN_EXISTING, FILE_FLAG_OPEN_REPARSE_POINT, NULL);
    if (h != INVALID_HANDLE_VALUE) {
        // the name of the destination must be a full path for FileRenameInfoEx
        wchar_t full[32768]; DWORD n = GetFullPathNameW(wt, 32768, full, NULL);
        if (n > 0 && n < 32768) {
            size_t bytes = sizeof(mw_rename_ex) + (size_t)n * sizeof(WCHAR);
            mw_rename_ex *ri = calloc(1, bytes);
            if (ri) {
                ri->Flags = MW_REPLACE_IF_EXISTS | MW_POSIX_SEMANTICS; ri->FileNameLength = (DWORD)(n * sizeof(WCHAR)); memcpy(ri->FileName, full, (size_t)n * sizeof(WCHAR));
                BOOL ok = SetFileInformationByHandle(h, (FILE_INFO_BY_HANDLE_CLASS)MW_FileRenameInfoEx, ri, (DWORD)bytes);
                free(ri);
                if (ok) { CloseHandle(h); rc = 0; goto out; }
            }
        }
        CloseHandle(h);
    }
    if (MoveFileExW(wf, wt, MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) rc = 0; else fail();
out:
    free(wf); free(wt);
    return rc;
}

// a reservation of disk space for [0, to) without changing the size of the file
int mw_win_reserve (int fd, uint64_t to) {
    HANDLE h = HANDLE_OF(fd); if (h == INVALID_HANDLE_VALUE) return EBADF;
    FILE_ALLOCATION_INFO ai; ai.AllocationSize.QuadPart = (LONGLONG)to;
    if (SetFileInformationByHandle(h, FileAllocationInfo, &ai, sizeof ai)) return 0;
    if (GetLastError() == ERROR_USER_MAPPED_FILE) return 0;   // (a file with a mapped view cannot have its allocation changed: nothing is reserved, as on a system that has no such call)
    fail(); return errno;
}

long mw_win_sysconf (int name) {
    SYSTEM_INFO si; GetSystemInfo(&si);
    if (name == _SC_PAGESIZE) return (long)si.dwPageSize;
    if (name == _SC_NPROCESSORS_ONLN) return (long)si.dwNumberOfProcessors;
    errno = EINVAL; return -1;
}

// ---- sleeping ----
#ifndef CREATE_WAITABLE_TIMER_HIGH_RESOLUTION
#define CREATE_WAITABLE_TIMER_HIGH_RESOLUTION 0x00000002
#endif
static DWORD timer_slot = FLS_OUT_OF_INDEXES; static INIT_ONCE timer_once = INIT_ONCE_STATIC_INIT;
static VOID WINAPI timer_free (PVOID h) { if (h) CloseHandle((HANDLE)h); }
static BOOL CALLBACK timer_init (PINIT_ONCE o, PVOID p, PVOID *c) { (void)o; (void)p; (void)c; timer_slot = FlsAlloc(timer_free); return TRUE; }
int mw_win_nanosleep (const struct timespec *req, struct timespec *rem) {
    if (rem) { rem->tv_sec = 0; rem->tv_nsec = 0; }
    InitOnceExecuteOnce(&timer_once, timer_init, NULL, NULL);
    HANDLE t = timer_slot == FLS_OUT_OF_INDEXES ? NULL : (HANDLE)FlsGetValue(timer_slot);
    if (!t) {
        t = CreateWaitableTimerExW(NULL, NULL, CREATE_WAITABLE_TIMER_HIGH_RESOLUTION, TIMER_ALL_ACCESS);      // (Windows 10 1803 and later)
        if (!t) t = CreateWaitableTimerW(NULL, TRUE, NULL);
        if (t && timer_slot != FLS_OUT_OF_INDEXES) FlsSetValue(timer_slot, t);
    }
    long long ns = (long long)req->tv_sec * 1000000000LL + req->tv_nsec;
    if (!t) { Sleep((DWORD)((ns + 999999) / 1000000)); return 0; }
    LARGE_INTEGER due; due.QuadPart = -(ns / 100 > 0 ? ns / 100 : 1);                  // (relative, in 100 ns)
    if (!SetWaitableTimer(t, &due, 0, NULL, NULL, FALSE)) { Sleep((DWORD)((ns + 999999) / 1000000)); return 0; }
    WaitForSingleObject(t, INFINITE);
    return 0;
}
int mw_win_usleep (unsigned long us) { struct timespec ts = { (time_t)(us / 1000000UL), (long)(us % 1000000UL) * 1000L }; return mw_win_nanosleep(&ts, NULL); }

// ---- mappings ----
// A view of a file is released with UnmapViewOfFile(base); memory (MAP_ANON) with VirtualFree: the second kind is remembered.
static CRITICAL_SECTION anon_cs; static INIT_ONCE anon_once = INIT_ONCE_STATIC_INIT;
static void *anon[1024]; static int nanon;
static BOOL CALLBACK anon_init (PINIT_ONCE o, PVOID p, PVOID *c) { (void)o; (void)p; (void)c; InitializeCriticalSection(&anon_cs); return TRUE; }
static void anon_add (void *a) { InitOnceExecuteOnce(&anon_once, anon_init, NULL, NULL); EnterCriticalSection(&anon_cs); if (nanon < 1024) anon[nanon++] = a; LeaveCriticalSection(&anon_cs); }
static int anon_take (void *a) {
    InitOnceExecuteOnce(&anon_once, anon_init, NULL, NULL);
    int found = 0; EnterCriticalSection(&anon_cs);
    for (int i = 0; i < nanon; i++) if (anon[i] == a) { anon[i] = anon[--nanon]; found = 1; break; }
    LeaveCriticalSection(&anon_cs);
    return found;
}
void *mw_win_mmap (void *addr, size_t len, int prot, int flags, int fd, off_t off) {
    (void)addr;
    if (flags & MAP_ANON) {
        void *p = VirtualAlloc(NULL, len, MEM_RESERVE | MEM_COMMIT, (prot & PROT_WRITE) ? PAGE_READWRITE : PAGE_READONLY);
        if (!p) { errno = ENOMEM; return MAP_FAILED; }
        anon_add(p); return p;
    }
    HANDLE h = HANDLE_OF(fd); if (h == INVALID_HANDLE_VALUE) { errno = EBADF; return MAP_FAILED; }
    bool wr = (prot & PROT_WRITE) != 0;
    uint64_t maxsz = (uint64_t)off + len;
    LARGE_INTEGER fsz; if (!GetFileSizeEx(h, &fsz)) { fail(); return MAP_FAILED; }
    if (maxsz > (uint64_t)fsz.QuadPart) { if (os_trace < 0) os_trace = getenv("MW_OS_TRACE") != NULL; if (os_trace > 0) tr("multiwriter os: mmap of %llu bytes beyond the end of the file (%lld): refused\n", (unsigned long long)maxsz, (long long)fsz.QuadPart); errno = ENXIO; return MAP_FAILED; }     // (a mapping beyond the end of the file would make the file longer here; POSIX leaves the rest unusable)
    HANDLE m = CreateFileMappingW(h, NULL, wr ? PAGE_READWRITE : PAGE_READONLY, (DWORD)(maxsz >> 32), (DWORD)(maxsz & 0xffffffffu), NULL);
    if (!m) { fail(); return MAP_FAILED; }
    void *p = MapViewOfFile(m, wr ? FILE_MAP_WRITE : FILE_MAP_READ, (DWORD)((uint64_t)off >> 32), (DWORD)((uint64_t)off & 0xffffffffu), len);
    DWORD e = GetLastError();
    CloseHandle(m);                                            // (the view keeps the section alive)
    if (!p) { errno = win_errno(e); return MAP_FAILED; }
    return p;
}
int mw_win_munmap (void *addr, size_t len) {
    (void)len;
    if (anon_take(addr)) return VirtualFree(addr, 0, MEM_RELEASE) ? 0 : fail();
    return UnmapViewOfFile(addr) ? 0 : fail();
}
int mw_win_msync (void *addr, size_t len, int flags) {
    (void)flags;
    return FlushViewOfFile(addr, len) ? 0 : fail();            // (to the file; the engine follows with fsync(fd) where it needs the disk)
}

// ---- flock(): whole-file locks, advisory ----
// The engine takes a file exclusively without waiting (to know whether it is alone), takes it shared (waiting while somebody initialises it), and turns exclusive into shared (the first process has
// initialised the database and lets the others in) with nobody able to slip in between. LockFileEx has no conversion, so two bytes far beyond the end of the file stand for the file:
//   G, the gate: whoever is taking or converting the lock holds it, exclusively when it takes the file exclusively, shared for the moment it takes it shared;
//   P, the presence: every holder keeps it, shared; the exclusive holder keeps it exclusively.
// exclusive: G exclusive, P exclusive.  shared: G shared (waits while an exclusive holder is converting or initialising), P shared, G released.  exclusive to shared: P exclusive released, P shared taken
// (G is still ours), G released.  The bytes are far from the data: a lock on them stops no read or write.
enum { FL_MAXFD = 4096 };
static unsigned char fl_state[FL_MAXFD];                       // 0 none, 1 shared, 2 exclusive (indexed by descriptor)
static const LONGLONG FL_G = (LONGLONG)1 << 41, FL_P = ((LONGLONG)1 << 41) + 1;

static BOOL lock_byte (HANDLE h, LONGLONG at, BOOL exclusive, BOOL wait) {
    OVERLAPPED ov; memset(&ov, 0, sizeof ov); ov.Offset = (DWORD)(at & 0xffffffffu); ov.OffsetHigh = (DWORD)(at >> 32);
    return LockFileEx(h, (exclusive ? LOCKFILE_EXCLUSIVE_LOCK : 0) | (wait ? 0 : LOCKFILE_FAIL_IMMEDIATELY), 0, 1, 0, &ov);
}
static void unlock_byte (HANDLE h, LONGLONG at) {
    OVERLAPPED ov; memset(&ov, 0, sizeof ov); ov.Offset = (DWORD)(at & 0xffffffffu); ov.OffsetHigh = (DWORD)(at >> 32);
    UnlockFileEx(h, 0, 1, 0, &ov);
}
static int would_block (void) { DWORD e = GetLastError(); if (os_trace < 0) os_trace = getenv("MW_OS_TRACE") != NULL; if (os_trace) tr("multiwriter os: flock would block (Windows error %lu)\n", (unsigned long)e); errno = (e == ERROR_LOCK_VIOLATION || e == ERROR_IO_PENDING) ? EWOULDBLOCK : win_errno(e); return -1; }

int mw_win_flock (int fd, int op) {
    if (os_trace < 0) os_trace = getenv("MW_OS_TRACE") != NULL;
    if (os_trace > 0) tr("multiwriter os: flock(fd %d, op %d)\n", fd, op);
    HANDLE h = HANDLE_OF(fd); if (h == INVALID_HANDLE_VALUE || fd < 0 || fd >= FL_MAXFD) { errno = EBADF; return -1; }
    unsigned char *st = &fl_state[fd];
    BOOL wait = !(op & LOCK_NB);
    if (op & LOCK_UN) {
        if (*st) { unlock_byte(h, FL_P); if (*st == 2) unlock_byte(h, FL_G); }
        *st = 0; return 0;
    }
    if (op & LOCK_EX) {
        if (*st == 2) return 0;
        unsigned char was = *st;
        if (was == 1) unlock_byte(h, FL_P);                      // (a shared lock asked to become exclusive: as flock() does, it is released first)
        if (!lock_byte(h, FL_G, TRUE, wait)) { int rc = would_block(); if (was == 1 && lock_byte(h, FL_P, FALSE, FALSE)) *st = 1; else *st = 0; return rc; }
        if (!lock_byte(h, FL_P, TRUE, wait)) { int rc = would_block(); unlock_byte(h, FL_G); if (was == 1 && lock_byte(h, FL_P, FALSE, FALSE)) *st = 1; else *st = 0; return rc; }
        *st = 2; return 0;
    }
    // shared
    if (*st == 1) return 0;
    if (*st == 2) {                                              // conversion: the gate stays ours until the presence is shared
        unlock_byte(h, FL_P);
        if (!lock_byte(h, FL_P, FALSE, TRUE)) { int rc = would_block(); unlock_byte(h, FL_G); *st = 0; return rc; }
        unlock_byte(h, FL_G); *st = 1; return 0;
    }
    if (!lock_byte(h, FL_G, FALSE, wait)) return would_block();
    if (!lock_byte(h, FL_P, FALSE, wait)) { int rc = would_block(); unlock_byte(h, FL_G); return rc; }
    unlock_byte(h, FL_G); *st = 1; return 0;
}

// ---- fcntl(): byte-range locks ----
int mw_win_fcntl (int fd, int cmd, struct flock *fl) {
    HANDLE h = HANDLE_OF(fd); if (h == INVALID_HANDLE_VALUE) { errno = EBADF; return -1; }
    uint64_t start = (uint64_t)fl->l_start, len = fl->l_len ? (uint64_t)fl->l_len : ((uint64_t)1 << 62);
    OVERLAPPED ov; memset(&ov, 0, sizeof ov); ov.Offset = (DWORD)(start & 0xffffffffu); ov.OffsetHigh = (DWORD)(start >> 32);
    DWORD lo = (DWORD)(len & 0xffffffffu), hi = (DWORD)(len >> 32);
    if (cmd == F_SETLK || cmd == F_SETLKW) {
        if (fl->l_type == F_UNLCK) return UnlockFileEx(h, 0, lo, hi, &ov) ? 0 : (GetLastError() == ERROR_NOT_LOCKED ? 0 : fail());
        DWORD fl_ = (fl->l_type == F_WRLCK ? LOCKFILE_EXCLUSIVE_LOCK : 0) | (cmd == F_SETLK ? LOCKFILE_FAIL_IMMEDIATELY : 0);
        if (LockFileEx(h, fl_, 0, lo, hi, &ov)) return 0;
        DWORD e = GetLastError(); errno = (e == ERROR_LOCK_VIOLATION) ? EAGAIN : win_errno(e); return -1;
    }
    if (cmd == F_GETLK) {                                       // is there a lock that would stop this one? (try it, and give it back)
        DWORD fl_ = (fl->l_type == F_WRLCK ? LOCKFILE_EXCLUSIVE_LOCK : 0) | LOCKFILE_FAIL_IMMEDIATELY;
        if (LockFileEx(h, fl_, 0, lo, hi, &ov)) { UnlockFileEx(h, 0, lo, hi, &ov); fl->l_type = F_UNLCK; return 0; }
        fl->l_pid = 1; return 0;                                // (somebody holds it; the type stays what was asked)
    }
    errno = EINVAL; return -1;
}

int mw_same_file (int fd, const char *path) {
    HANDLE h = HANDLE_OF(fd); BY_HANDLE_FILE_INFORMATION a, b;
    if (h == INVALID_HANDLE_VALUE || !GetFileInformationByHandle(h, &a)) return -1;
    wchar_t *w = wide_path(path); if (!w) return -1;
    HANDLE p = CreateFileW(w, 0, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL, OPEN_EXISTING, FILE_FLAG_BACKUP_SEMANTICS, NULL);
    free(w);
    if (p == INVALID_HANDLE_VALUE) return -1;
    BOOL ok = GetFileInformationByHandle(p, &b); CloseHandle(p);
    if (!ok) return -1;
    return a.dwVolumeSerialNumber == b.dwVolumeSerialNumber && a.nFileIndexHigh == b.nFileIndexHigh && a.nFileIndexLow == b.nFileIndexLow;
}

int mw_win_pid_alive (int pid) {
    HANDLE p = OpenProcess(SYNCHRONIZE | PROCESS_QUERY_LIMITED_INFORMATION, FALSE, (DWORD)pid);
    if (!p) return GetLastError() == ERROR_ACCESS_DENIED;       // (denied: it exists)
    DWORD r = WaitForSingleObject(p, 0); CloseHandle(p);
    return r == WAIT_TIMEOUT;
}

#endif   // _WIN32
