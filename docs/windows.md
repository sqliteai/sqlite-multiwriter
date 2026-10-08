# Windows

The engine runs on Windows (MinGW-w64, `make extension PLATFORM=windows` in MSYS2) with the threads of one process. The mode of several processes (`mw_mp=1`) is not available there and a database opened with it is refused with `SQLITE_CANTOPEN`
(and a line in the SQLite log).

## Why not several processes

In that mode the commit log is a file mapped by every process, which they append to through the mapping, and which the compaction truncates and the processes map again; the segments of the log are deleted and renamed while the others
have them open. Windows does not let a file with a mapped view be truncated, and the C runtime does not let a file that is open be deleted or renamed. The single process mode (the log is written with `pwritev`, one process owns it) does not need
any of that.

## What the Windows build does

`src/multiwriter_os.h` and `src/multiwriter_os_win.c` give the engine the POSIX calls it uses:

- `open` creates files with every share mode, including delete, and gives a C descriptor on the handle; `pread`, `pwrite` and `pwritev` use `OVERLAPPED` offsets; `fsync` is `FlushFileBuffers`; `ftruncate` is `SetEndOfFile`.
- `unlink` and `rename` use the POSIX semantics of Windows 10 1709 and later (`FileDispositionInfoEx` and `FileRenameInfoEx`): the name goes at once, and a rename replaces a file that is open (the engine swaps the log under its own
  threads). On an older Windows they fall back to `DeleteFile` and `MoveFileEx`, which keep the name until the last handle is closed.
- `flock` is made of two byte locks far beyond the end of the file, so that the conversion of an exclusive lock to a shared one (the engine does it when the first user has initialised the database) leaves nobody a moment to slip in.
  `fcntl` byte-range locks, `mmap`, `msync`, `munmap` and `sysconf` are there too, for the code of the several-processes mode, which is compiled but cannot be reached.

## What is tested

On a Windows 10 (22H2, build 19045) machine, cross-built with MinGW-w64 and run there: the 31 test programs that do not fork pass (twice in a row), and the extension (`multiwriter.dll`) loads into a stock SQLite, registers the VFS and
runs four writing threads (`test/loadable.c`). The program `mw_oslayer` checks the calls of `multiwriter_os_win.c` one by one: positional reads and writes, `ftruncate`, a rename over an open file, the delete of an open file and
the reuse of its name at once, `flock` with its conversion, shared mappings, byte-range locks.

Two things that Windows does differently showed up and are handled: a log that is mapped cannot be truncated (so the log of one process is not mapped on Windows), and `Sleep` rounds to the 15.6 ms tick of the system (the engine waits 50
microseconds at a time in its back-off, so `nanosleep` and `usleep` use a high resolution waitable timer). The disks have a latency: a commit that is durable waits for `FlushFileBuffers`, tens of milliseconds on the machine used,
and the tests that count commits in a second have a longer second there.


The tests that do not use `fork()` run on Windows (`make test-ci PLATFORM=windows` in MSYS2); the tests of processes, of crashes and of kills (about twenty programs) use `fork`/`waitpid` and are not built there. The CI runs them on `windows-2022`.
