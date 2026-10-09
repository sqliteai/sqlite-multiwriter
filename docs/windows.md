# Windows

The whole engine runs on Windows (MinGW-w64, `make extension PLATFORM=windows` in MSYS2): the threads of one process (`mw=1`) and the processes mode (`mw_mp=1`, the shared mode: one version index and segmented log shared by all the processes).
There is only one processes mode, on every platform (the private-store mode that mapped a single log file was removed in 0.5.0).

## What the Windows build does

`src/multiwriter_os.h` and `src/multiwriter_os_win.c` give the engine the POSIX calls it uses:

- `open` creates files with every share mode, including delete, and gives a C descriptor on the handle (binary, `O_APPEND` honoured); `pread`, `pwrite` and `pwritev` use `OVERLAPPED` offsets; `fsync` is `FlushFileBuffers`;
  `ftruncate` is `SetEndOfFile`; `fstat`/`stat` are the C runtime's, and the one place that needs the identity of a file (has another process replaced it?) uses `mw_same_file` (volume and file index).
- `unlink` and `rename` use the POSIX semantics of Windows 10 1709 and later (`FileDispositionInfoEx` and `FileRenameInfoEx`): the name goes at once, whoever has the file open, and a rename replaces a file that is open or
  mapped. On an older Windows they fall back to `DeleteFile` and `MoveFileEx`, which keep the name until the last handle is closed.
- `flock` is made of two byte locks far beyond the end of the file, so that the conversion of an exclusive lock to a shared one (the first process has initialised the database and lets the others in) leaves nobody a moment to slip in.
  `fcntl` byte-range locks (the publication lock and the liveness bytes of the processes: Windows drops them when the process ends, at once even for `TerminateProcess`), `mmap` of files and of memory, `msync` and
  `sysconf` are there too.
- `nanosleep` and `usleep` use a high resolution waitable timer: `Sleep` rounds to the 15.6 ms tick of the system and the engine waits 50 microseconds at a time in its back-off.
- The log of a process alone (mw=1, no mw_mp) is not mapped: a mapped file cannot be truncated, which the compaction does. It is written with `pwritev` and read with `pread`, as on the other systems when it is staged.
- The path of a database is the full path of SQLite, with backslashes; the engine finds the directory of its segments with the last of `/` or `\` (a bug that only the crash recovery shows: after all the processes were killed the
  first one to open the database found no segment and began an empty log).

## What is tested

On a Windows 10 machine (22H2, build 19045), cross-built with MinGW-w64 and run there:

- the 33 test programs that run on Windows pass, twice in a row: the whole suite of threads, the rebase tests also with `MW_TEST_MP=1`, and `mw_oslayer` (the calls of the layer one by one: positional I/O, truncate, a rename over an open
  file, the delete of an open file and the reuse of its name at once, `flock` with its conversion, shared mappings, byte-range locks);
- `mw_procs`: four processes write one database, one is killed, then all are killed at once and the database is opened: every insert that a process had acknowledged is there (a byte for each in a file of its own);
- `mw_serial` with the processes started by `CreateProcess` and killed by `TerminateProcess`: the randomised serializability check, with and without the rebase, with a WITHOUT ROWID table, passes: 0 reads that differ from the
  serial order, the transactions in doubt after a kill are placed, the final content is the model's;
- the extension (`multiwriter.dll`) loads into a stock SQLite, registers the VFS and runs four writing threads (`test/loadable.c`).

The tests that fork and wait for children (about twenty: the ones of crashes at a point of the publication, of a full disk, of the liveness of the processes) are POSIX only and not built on Windows; the crash points of `mw_serial`
(`MW_CRASH_*`) and the power-loss phases are not run there either. A durable commit waits for `FlushFileBuffers`: tens of milliseconds on the disk used, so the tests that count commits in a second have a longer second.
Not tested: Windows older than 10 1709 (the delete and the rename without POSIX semantics), a network share, a database on a drive without NTFS.
