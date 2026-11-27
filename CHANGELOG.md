# Changelog

All notable changes of sqlite-multiwriter are recorded here, newest first. The version is `MW_VERSION` in `src/multiwriter.h`; a version that is not released yet is pushed to `main` and released by the CI.
The format of the files on disk (`-mwlock`, `-mwidx`, `-mw.N`, `-mw`) is the same in the versions listed here.

## 0.7.0 - 2026-11-27

### Changed
- **Speculative snapshots of the writers** (one process). A write transaction (its first statement writes, or `BEGIN IMMEDIATE`/`EXCLUSIVE`) reads at the newest *installed* commit instead of the newest durable one, and is still acknowledged only when its own record is durable (the log is durable as a prefix, so never without the commits it read). Commits that depend on each other, above all inserts with the rowid that SQLite picks (each one takes the key after the last one it sees), used to follow each other one fsync at a time: with 24 writers and an fsync of about 3 ms, 333 tx/s, p99 694 ms, 545 retries per 100 tx; now 2,547 tx/s, p99 20 ms, 7 retries (the workload of the external comparison "SQLite vs WAL vs Turso MVCC vs sqlite-multiwriter", `bench/events_bench.c`, `MW_FULLFSYNC=1` on this Mac). A transaction that does not write reads only durable commits as before; one that read speculatively and ends without a commit waits until what it read is durable; a commit whose record could not be written is taken back and the transactions that read it are refused (`SQLITE_BUSY_SNAPSHOT`). URI `mw_spec=0` turns it off for a connection, `MW_NO_SPEC=1` for the process (the default stays on). The processes mode is unchanged.
- The rebase: its helper reads speculatively too (at least what the transaction it replays read), and the publication gate that it closes after a lost replay waits until every assigned commit is installed, not durable (waiting for their fsync held every publisher: `samepage` with the rebase, 64 threads on Linux, 15.5k -> 20.6k tx/s against 17.6k before this change).
- Hot spots (one process): with speculative snapshots the turn is taken after the first refusal (it was after 16) and given back as soon as the commit is installed or conflicts (it was at the end of the transaction, after the fsync). Retries per 100 tx of the appends above: 131-143 -> 7 with a fast fsync, 105-111 -> 13-18 with `synchronous=OFF`, and the p99.9 latency 60-175 ms -> 2-56 ms.
- `mw_bench`, threads, `synchronous=FULL`, 4/16/64 writers: `hot` +9%..+160%, `groups`, `samepage`, `cols`, `insert-autoinc`, `crdtinsert` without the rebase +13%..+500%, with it mostly higher too; `bulk`, `independent`, `mixed` unchanged (within ±5%). Lower: `samepage` with the rebase at 4-16 writers on macOS (-9..-11%: more commits take the fast path, one fsync each, instead of the batched replays). On Linux (Docker, fsync about 0.3 ms) the contended workloads go from 1-4k to 17-34k tx/s (the engine without the change collapses under contention), the appends of the comparison with 24 writers from 1,820 to 7,625 tx/s; with no durable fsync (tmpfs, `synchronous=OFF`) they are 3-7% slower from 8 writers up, with a p99 4-5 times lower.

### Fixed
- **Processes mode: a commit could be acknowledged without an fsync of the log.** A committer that becomes the leader of the group sync and finds its own record made durable already (by the leader before it, which finished between its look and its turn) syncs nothing, but it still moved the durable point to the end of the log as it read it, after the others had installed more records: those were acknowledged with nothing synced, and a power failure could lose them. It now moves the durable point only after its own `msync` and `fsync`. Found by `mw_syncwindow` in the CI (linux-x86_64, about 1 run in 20 on a Linux of 4 CPUs, also before 0.7); its new scenario 3 (`MW_TEST_SYNC_PRELEAD_US`: a delay before a committer tries to be the leader) shows it in every run: 459-531 of 800 commits acknowledged without a sync with the old code, 0 now.
- A data race that ThreadSanitizer built with gcc reported in `mw_log_rewrite_prepare` against `lane_publish` (the tests `mw_rebasewr` and `mw_rebasefkstress`, about 7 runs in 8 on Linux arm64): gcc read the flags `mp_req`, `mp` and `has_log` with one 8-byte load that took in the atomic counter `inflight` next to them. The atomics that follow plain fields of the database (`inflight`, `open_done`) start a word of their own. The CI builds with clang, which reads the flags one by one, and did not see it.

- Test `mw_multiproc`, scenario 4: the process to kill was killed 150 ms after it was started, and on a slow machine (ThreadSanitizer in a Linux container: 1 run in 15) it had not committed anything yet (`killed > 0` failed). It is killed once it has acknowledged 50 commits (through a pipe, at most a minute), as `mw_procs` does since 0.6.3. Under ThreadSanitizer the test also needs `llvm-symbolizer`: without it the race of SQLite on `randomnessPid` in the children is not matched by `test/tsan.supp`, and the children exit with 66.

- The build has no warnings with gcc 12 (Linux), clang (macOS, Linux) and MinGW-w64: lines with several `if` on one line (`multiwriter_shidx.c`, `multiwriter_shared.c`, `mw_reloc`) are split; `install_impl` of the shared index refuses a negative count of pages (gcc could not exclude that its allocations wrap) and the repair of a commit that a publisher left half installed initialises its arrays; in the tests, a buffer of `mw_diskfull` that could truncate a command, values that `mw_ioerr` printed uninitialised, a counter that `mw_sharedwindow` did not use, variables of `mw_serial` used only outside Windows.
- CI: the job of the Android x86_64 emulator gives the runner's user access to `/dev/kvm`. Without it the emulator ran with `-accel off`, which the emulator refuses since version 37 (the action installs the latest): the emulator never started and the job timed out.
- `mw_multiproc` printed its lines more than once under ThreadSanitizer: each child wrote again what the parent had printed and not yet flushed. The parent flushes before it forks.

### Added
- `bench/events_bench.c` (`make bench`): the workload of the external comparison (100 inserts of 64 bytes per transaction with SQLite's rowid and an index, 4 readers, `busy_timeout=5000`, retries counted in the latency).
- Test `mw_spec`: a reader does not see a commit whose fsync is held, a writer does and is acknowledged after it; a commit taken back refuses the writer that read it; dependent appends share the fsyncs.

## 0.6.5 - 2026-10-31

### Fixed
- The decoders of the rebase (the rows of a page, the schema, the records) trusted the sizes in the page they read. A payload size near 2^64 wrapped the size of the buffer for its overflow chain (`malloc(size + 1)`) and the local part was copied into a buffer that was too small; a serial type could make `pos + len` wrap in 32 bits and pass the bounds check; a cell count larger than the page could read past the page. A payload larger than SQLite allows (2^31-1) is now corruption, the lengths are compared in 64 bits against what is left of the record, and the cell pointer array is checked against the page size. A page that does not decode makes the rebase refuse the transaction, as every other page that does not decode.

## 0.6.4 - 2026-10-30

### Fixed
- The first open of a private lane ignored the result of `sqlite3_auto_extension()` that registers the trace hook of the schema barrier, and remembered the registration as done: after an error (`SQLITE_NOMEM`) the connection opened without the hook, and no later open registered it. The open now fails with the error and the next one tries again (issue #5).
- `mw_ensure_wal_db()` read a `stat()` of the `-wal` that fails as "no WAL": the first look could take the clean-header success path, and the look after the checkpoint could accept a WAL that a reader had kept from being checkpointed. An absent file is still "none"; any other failure is `SQLITE_IOERR_FSTAT` at both looks, and `mw_path_is_clean_wal_db()` answers false when it cannot tell (issue #6).
- Closing the last connection of a database freed its path while it was still in the list of the open databases, and an open of a database walks that list comparing the paths: it could read the freed memory (a use after free). The path is freed once the database is out of the list. Found by `mw_closestall` under ThreadSanitizer on Linux.
- Data races, found with ThreadSanitizer. The cached CPU count of the admission limit (`admit_cap`, and the one of the processes mode) is atomic: concurrent first commits wrote it with no synchronisation (issue #7). The flag of the hook registration and the tables of the passthrough io methods are made safely when several connections open at once. `db->p1_cache_epoch` is atomic: the rollback of a publish cleared it without `reloc_mu` while a relocation read it, and the relocation read it twice, so that a clear in between gave a merge epoch of 0.

### Added
- A job of the CI builds the tests with ThreadSanitizer and runs them (`make test-tsan`: every test but the long ones). `test/sanitize.sh` takes `CC`, kills a test that runs longer than `MW_SAN_TIMEOUT` seconds (600), sets `die_after_fork=0` and `detect_deadlocks=0` for `tsan` (the stripes are taken together, more than the 64 locks that its detector can hold), and builds SQLite with FTS4 like the Makefile. `test/tsan.supp` suppresses the races inside SQLite itself (the read of the wal-index without a lock, `randomnessPid`) for the two tests that meet them.
- Tests: `mw_autoextfail` (the registration of the hook fails), `mw_walstatfail` (the `stat` of the `-wal` fails), `mw_admitcap` (concurrent first calls of the admission limit; it sees the race only under `test/sanitize.sh tsan`).

## 0.6.3 - 2026-10-11

### Fixed
- Rebase: a statement's own conflict policy (`INSERT` or `UPDATE` with `OR ...`, and the `ON CONFLICT` clause of an UPSERT) is not in the row images, and replaying it with the policy of the schema could delete or ignore a row that another writer had committed (a table with `UNIQUE ... ON CONFLICT REPLACE`, an `INSERT OR ABORT` or `ON CONFLICT DO NOTHING` that the other writer's row should have refused). A transaction with such a statement is refused (`SQLITE_BUSY`) instead of rebased.
- Rebase: a column of type `ANY` in a `STRICT` table has no affinity: 1 and 1.0 are different values there, and the check of the old value compares the type too.
- Rebase: decoding a negative integer no longer shifts a negative value (undefined behaviour in C).
- `synchronous=NORMAL` and `OFF`: a commit returned while another writer was still writing an earlier part of the log, with its record in the memory of the process only; it returns when its record is in the file. Cost, measured with 16 connections on 8 threads (`mw_bench`, workload `independent`): about 25% fewer commits per second (206k -> 157k) and a median latency of 48 us against 35 us.
- `PRAGMA synchronous`: the level that the engine takes follows SQLite (the names, the boolean words, hexadecimal and out-of-range values, the mask of the pager), and a PRAGMA that SQLite rejects inside a transaction does not change it. An attached database is bound to its connection when its b-tree opens, so this holds there too, with the trace hook of the engine replaced or not.
- Processes mode: a record of the log that cannot be read when the segments are scanned leaves the end of the log unknown; the segments after it are no longer deleted, so that a later open can try again without losing data.
- Row diff of the rebase: an allocation that fails (the map of pages, the chain of overflow pages, the copy of the freelist, the list of changes) refuses the transaction instead of going on with a null pointer.
- A commit that an `fsync` already in flight covers is no longer refused when the log is failed meanwhile. The append of one commit failed with a later commit already behind it (the log is failed for good); a commit parked in the sync, whose record the running `fsync` was making durable, saw the failure first and was told that it had failed, and then the `fsync` finished and made it visible: it was refused and in the database (and in the log after a reopen). It now waits for the `fsync` that is running and is acknowledged if that one covers it; the ones that it does not cover are refused and stay invisible, as before. Found as a failure of `mw_rollbackrace` on a runner of two CPUs (2 of 150 runs on a machine of two, with and without the other changes); it is reproduced in every run by `mw_holesync`.

### Changed
- Documentation: the wording of the isolation limits in the README and in the changelog says what is prevented (the classic write skew, by the validation of the reads) and where it is still possible, instead of "write skew is possible".
- `test/sanitize.sh` runs every test and exits with the status of the first one that failed (it used to take the status of `tail`); the tests with a function replaced for a test (`-Dpwritev`, `-Dfsync`, `-Dmalloc`) are built there too, and `test/sanitize_exit.sh`, run by `make test` and `make test-ci`, checks the exit status.
- Test `mw_procs` kills the process after it has acknowledged inserts, not after a fixed time (on a slow runner it was killed before its first insert).

### Added
- Tests `mw_strictany`, `mw_rebasepolicy`, `mw_rebaseneg`, `mw_syncpragma`, `mw_stagednormal`, `mw_recoverread`, `mw_rowdiffoom`, `mw_attachsync` and `mw_holesync`, each of them failing without the change that it is for.

## 0.6.2 - 2026-10-10

### Changed
- Compaction writes the changed pages in order of page number and the adjacent ones with one write (runs of up to 64 pages): 19000 pages (75 MB) in 16 ms against 54 ms with threads and 11 ms against 57 ms with processes (page cache, `fsync` included); scattered pages 39 -> 20 ms and 31 -> 8 ms.
- The rewrite of the log by compaction (one process) copies what came in meanwhile up to the part that is written to the file, not the part that is durable (which with `synchronous` below FULL lags far behind), and in up to four rounds: the part copied with the commits stopped went from about 100 MB to a few, and the stop from 12 ms to 4 ms for each compaction under 8 writer threads (190k commits/s). The rest of the stop (3.7 ms) is the swap of the mapping.

### Added
- Test `mw_compactruns` (runs longer than 64 pages, single pages, gaps, a growing database; both modes; the file opened with the stock VFS).

## 0.6.1 - 2026-10-10

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
- Isolation is snapshot isolation plus the validation of the pages a transaction read, which refuses the classic write skew (two transactions that each read what the other writes). Write skew is still possible with `mw_readcheck=0`, for the reads of page 1 (the header of the file), and across attached databases, which are not atomic together. Serializability is tested (`mw_writeskew`, `mw_serial`), not proved; see the README and `docs/design.md`.
- Small items from the review that were open are in the Unreleased section above.

## 0.5.2 - 2026-10-09

The first public release.

- Wrapper VFS that gives SQLite several concurrent writers: threads (`vfs=multiwriter`) and processes (`&mw_mp=1`, one shared version index, a segmented log and a header file), snapshot isolation with page-level first-committer-wins and read validation, relocation of pages, merge of interior pages, a durable group-commit log compacted into the database file.
- Optional row-level rebase (`mw_rebase=1`) of commits that lost only on pages.
- URI modes: `mw=1` the engine (the default with `vfs=multiwriter`), `mw=0` stock, `mw=2` experimental lane tracking.
- Platforms: Linux (glibc, musl), macOS, Windows, Android, iOS, Mac Catalyst; a loadable extension, an XCFramework and an AAR.
