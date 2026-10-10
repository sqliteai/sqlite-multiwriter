# Changelog

All notable changes of sqlite-multiwriter are recorded here, newest first. The version is `MW_VERSION` in `src/multiwriter.h`; a version that is not released yet is pushed to `main` and released by the CI.
The format of the files on disk (`-mwlock`, `-mwidx`, `-mw.N`, `-mw`) is the same in the versions listed here.

## Unreleased

### Fixed
- **Slots of dead processes.** A process killed while it cleaned the slot of a dead one left that slot at -1 for ever (the slots, 8192 of them, were lost one by one); the cleaner marks the slot with its own pid now and the next one takes it over, and it checks the owner again after it took the slot (a process that was given the same pid keeps it). The same in the registry of the shared index.
- **Ticket ring of the publication lock.** With 1024 or more tickets outstanding a new ticket shared its place with the one at the head, the dead owner of the head was never skipped and every commit hung. No more than 1024 are out now. A dead ticket in the queue also cost 160 ms to skip (a thousand of them minutes); a waiter that wakes up or has just skipped one looks at the head at once.

### Added
- Tests `mw_slotstuck`, `mw_ticketring`, and `mw_rebasecases` 11 (the ON CONFLICT clauses of a table give what a serial execution gives, or the commit is refused).
- README: what a schema change waits for and what a transaction that overlapped it gets.

### Looked at, no change
- `PRAGMA data_version` changes at every read transaction in the engine (every snapshot starts with a cold page cache): it was already in the limits of the README; it cannot be used to detect the commits of other connections.
- ON CONFLICT clauses (`REPLACE`, `IGNORE`, `NOT NULL ON CONFLICT REPLACE`) in rebased commits: the result is what a serial execution gives (the other writer first); no defect found.
- Entries of the shared index that a process killed inside the garbage collection leaves between cutting a chain and freeing it: they are not on the free list until the index is rebuilt (the next open after every process has left); the loss is bounded by one chain per kill.

## 0.6.0 - 2026-10-10

A release of fixes found by a review of every source file; each one was reproduced first and has a test that fails without it. No change of the interface.

### Fixed
- **Locks lost on open.** Opening a connection read the database header with `fopen`/`fclose`, and closing any descriptor of the file released the POSIX locks of the process on it, so another connection could lose its lock. The header is read through the VFS now; the open fails if the conversion to WAL, or the checkpoint of a stale `-wal`, did not happen.
- **Rebase.** A rebased commit poisons its snapshot like a relocation (a later commit of the same snapshot overwrote the rows of the other writer); a comment in front of a statement no longer hides a read (write skew passed with `mw_rebase=1`); a header field set by the application (`user_version`, `application_id`, ...) refuses the rebase instead of being dropped by the replay; the cache of point statements is kept per schema generation; a batch replays a request only with a catalog of its own schema; the catalog uses the right rules for `INTEGER PRIMARY KEY DESC`, `WITHOUT ROWID` (as `PRAGMA table_list` says), columns with no affinity and pages with reserved bytes.
- **Open.** `mw_mp` is read like `mw` (`true/on/yes`, `false/off/no`, numbers; anything else fails the open); `mw_mp` with `mw=2` no longer crashes; a conflict of modes is `SQLITE_CANTOPEN`, not `SQLITE_NOMEM`; `auto_vacuum` is read as SQLite reads it; a first open that fails releases its descriptors and locks, and later opens report the same error.
- **Commit path.** A failed append wakes the commits parked behind it; the rollback after it takes the stripes before `seq_mu` and compares epochs exactly; the ring of database sizes is emptied before it is written; the group sync of the processes takes its target from the log position (a segment roll and a discarded record were acknowledged without a sync); the allocation of the entries of the shared index takes all of them before it links any.
- **Recovery and errors.** The repair of a half-published commit is idempotent (installs what is missing, collects garbage when short of room, stops commits if it still cannot); a read error or a failed mapping of the log is told apart from "gone" and is no longer taken for an empty tail; the scan of the tail of the log after a drain no longer reads past a truncated segment; a process that registers clears every header field that carries its pid, so a dead process whose pid was reused no longer holds the publication lock, a snapshot slot or a ticket for ever.
- **Large databases.** The shared index covers twice the size of the database at the time it is created (it was fixed at 2^24 pages: a commit that needed a page above it failed with `SQLITE_FULL` after its work was done, for ever).
- **Teardown.** The last close of a database frees its state outside the global mutex, and `fork` holds that mutex so the child never inherits it locked.

### Changed
- A schema change of the temporary schema (`CREATE TEMP ...`, `CREATE TABLE temp.x`, `DROP/ALTER temp.x`) no longer raises the exclusive schema barrier, which waited up to 2 s for the write transactions of the other connections. Changes of the main schema are as before.
- The catalog that the rebase replay uses is built once for the database and kept for its schema cookie, not by every connection (first rebase on a new connection with 400 tables: 9.3 ms -> 1.3 ms).
- A connection gives back the buffers that one big transaction made it grow (the frames of its private WAL, the lists of pages): 66 MB of heap after a transaction of 600000 rows, now 0.8 MB.
- The list of the pages that a read snapshot wrote is merged at each commit, not sorted again (27.2 -> 19.5 us per commit over 3000 commits in one snapshot); `sysconf` is read once; two stores that nobody read are gone.

### Added
- Tests: `mw_openfail`, `mw_rollbackrace`, `mw_repairinstall`, `mw_mapfail`, `mw_closestall`, `mw_tailscan`, `mw_bigdb`, `mw_tempbarrier`, `mw_lanetrim`, `mw_openlocks`, and cases in `mw_rebasecases`, `mw_selfcommit`, `mw_appendfail`, `mw_sizering`, `mw_trimfail`, `mw_syncwindow`, `mw_mpstale`.
- Fault points for the tests: `MW_FAULT_INSTALL_ERR` and `MW_FAULT_APPEND_STALL`.
- Android: `make all PLATFORM=android ARCH=...` builds the test programs for an emulator or a device (the suite passes on Android 14 arm64); iOS simulator: the whole suite passes.

### Known and not changed
- Write skew is possible (snapshot isolation with read validation, not serializable); see the README.
- Small items from the review that were open are in the Unreleased section above.

## 0.5.2 - 2026-10-09

The first public release.

- Wrapper VFS that gives SQLite several concurrent writers: threads (`vfs=multiwriter`) and processes (`&mw_mp=1`, one shared version index, a segmented log and a header file), snapshot isolation with page-level first-committer-wins and read validation, relocation of pages, merge of interior pages, a durable group-commit log compacted into the database file.
- Optional row-level rebase (`mw_rebase=1`) of commits that lost only on pages.
- URI modes: `mw=1` the engine (the default with `vfs=multiwriter`), `mw=0` stock, `mw=2` experimental lane tracking.
- Platforms: Linux (glibc, musl), macOS, Windows, Android, iOS, Mac Catalyst; a loadable extension, an XCFramework and an AAR.
