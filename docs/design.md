# sqlite-multiwriter: design

## What it is

A wrapper VFS that lets many connections (threads and processes) write one SQLite database at the same time. A transaction runs on a private view of the database (a snapshot), its pages are checked against
the commits that were published meanwhile when it commits, and a commit that loses is refused (`SQLITE_BUSY_SNAPSHOT`: the application runs it again) or, if the connection asked for it (`mw_rebase=1`), replayed at the latest state
when it lost only because of pages that it shares with a commit that changed other rows. Every commit is durable when it is acknowledged (a commit log with group commit and `synchronous=FULL`
semantics), and the log is compacted into the ordinary database file, which stays a plain SQLite database. No SQLite source is changed, no SQL syntax is added, triggers and hooks are not used.

The project does not synchronise databases and keeps no metadata of its own about rows. (An earlier version captured the CRDT state of sqlite-sync for every table: `history-crdt-design.md`. It was removed; to synchronise
databases use sqlite-sync as an extension of its own.) SQLite 3.53.4 is vendored in `third_party/sqlite`; nothing else is needed to build and test.

Open a database through the VFS with `file:db?mw=2` (threads of one process) or `file:db?mw=2&mw_mp=1` (processes: the shared mode); add `&mw_rebase=1` for the rebase. The VFS registers itself when SQLite initialises
(`SQLITE_EXTRA_INIT`). `docs/engine-history.md` and `docs/history-crdt-design.md` are history, with the measurements behind older choices.

## Layers (`src/`)

| files | what they do |
|---|---|
| `multiwriter_vfs.c`, `multiwriter_lane.c`, `multiwriter_tx.c` | the VFS and the *lane*, the engine's side of a connection: a private in-memory WAL (SQLite believes it is the only writer), the snapshot, the write set and the read set, the commit (`lane_publish`) and its outcomes |
| `multiwriter_pages.c`, `multiwriter_btree.c`, `multiwriter_reloc.c` | the page store (versioned chains, striped locks), validation and installation of a commit, the merge of interior b-tree pages, the relocation of pages after concurrent growth of the file |
| `multiwriter_log.c`, `multiwriter_compact.c`, `multiwriter_db.c`, `multiwriter_boot.c` | the commit log of a process (a staged ring written and fsynced by a group leader), compaction into the real file, garbage collection of versions, open, close, recovery |
| `multiwriter_shared.c`, `multiwriter_seglog.c`, `multiwriter_shidx.c`, `multiwriter_mp.c` | the shared mode: segmented log mapped by every process, shared index of page versions, the publication lock and its repair when a holder dies, the registry of snapshots, admission control |
| `multiwriter_rebase.c`, `multiwriter_rowdiff.c`, `multiwriter_catalog.c` | the rebase: the row changes of a write set (decoded from the pages), the schema as the replay needs it, the replay on a helper connection |
| `multiwriter_io.h` | the file calls of the engine (fsync with `F_FULLFSYNC`, fault injection for the tests, the mode of the files it creates) |

## Transactions and isolation

- **Snapshot.** The first read of a transaction takes the newest *visible* epoch. Every commit takes the next epoch and becomes visible only after the one before it (readers see a prefix of the commits, never a hole). A page read at
  a snapshot is the newest version with an epoch not above it, else the page of the real file. Versions are kept while an active snapshot can still see them (`oldest_active_snapshot`), and dropped by the garbage collector
  that the committing threads run every few dozens of commits.
- **Commit.** Under the locks of the pages it wrote, a commit is validated: (1) the schema cookie is the one the transaction began with; (2) no page that it *read and did not write* has a newer version (a read of an interior b-tree page
  is forgiven if the path that the transaction took through it is unchanged: *routing*); (3) no page that it wrote has a newer version than its snapshot. Then its record (pages) goes to the log, its versions are installed, and
  it becomes visible when the log is durable (group commit). A commit that fails (2) or the cookie is refused; one that fails (3) may be saved:
  - **relocation** (`multiwriter_reloc.c`): the conflict is only that other commits grew the file. The pages the transaction allocated are renumbered above the new end (the references to them are rewritten), page 1 is
    merged, and the commit is published again, inside the same hold of the locks;
  - **merge of interior pages** (`multiwriter_btree.c`): both commits rewrote an interior table page (typically the parent of leaves both split): the page is merged three-way with the newest version;
  - **rebase** (below), if `mw_rebase=1`.
  Otherwise the commit is refused and the lane's transaction ends; hot spots are served by a turn (a connection that lost serialises its next few transactions).
- **What is guaranteed.** Snapshot isolation with first-committer-wins at the level of pages, plus the validation of the pages read: no lost update, no write over a row that was deleted meanwhile, no UNIQUE value held by two rows,
  a read-only transaction sees one consistent prefix of the commits, a transaction that reads a page that somebody changed does not commit. It is **not serializable**: write skew between rows that live on different pages
  (a transaction reads A and writes B, another reads B and writes A) is allowed, as in any snapshot isolation. `test/mw_serial.c` checks the guarantee (below).
- **Interior pages of an index.** A transaction that read a page and finds it changed is not refused when the page is an interior page of a table b-tree that still routes the children the transaction went through (`mw_interior_routes_same`). That is not done for the interior pages of an index: their cells are entries (for a WITHOUT ROWID table, whole rows), and what a transaction read from one is not covered by the routes of its children. Found with `mw_serial` on a WITHOUT ROWID table in the processes mode (stale reads accepted, with and without the rebase), and measured: with the shortcut off for index pages the runs are clean. The cost is a retry where an index page near the root was rewritten (a split): about 7-20% of the throughput of the bench workloads that update through an index of a table under contention, none for tables with an integer key.
- **DDL** takes an exclusive schema barrier (one schema change at a time, writers drain); any transaction that overlaps a schema change fails on the cookie and is run again.
- **Not supported:** `journal_mode` other than WAL, `locking_mode=EXCLUSIVE`, `auto_vacuum` other than none.

## The rebase (`mw_rebase=1`)

A page-level conflict is not a logical conflict: two writers may change different rows of one page. When a commit lost only on pages that it wrote (not on a page it only read, not on the schema), and the connection has
`mw_rebase=1`, the engine does not refuse it: it decodes the row changes of the transaction from its pages and replays them with ordinary SQL on a helper connection at the latest snapshot; stock SQLite builds every page again
(splits, allocation, indexes, freelist, record encoding), and that commit is what is published. The original SQL is never run again (no side effects twice). Nothing is kept for it between transactions: the work is done when
a commit conflicts, not at every commit.

1. **The changes** (`multiwriter_rowdiff.c`). From the page images of the write set and the same pages at the snapshot, the rows of every table b-tree that were inserted, changed (old record, new record) or deleted. It decodes SQLite's file
   format directly (table leaves, records, overflow chains), takes the net change over the whole write set (a row that moved between two pages is no change), and walks the freelist (a page that the transaction freed is not rewritten, and a page
   on the freelist holds stale rows). Which table a page belongs to is found when it is needed: a page that the transaction allocated is listed by an interior page that it wrote; one that existed at the snapshot is found by walking down from the
   root of every table with a key that the page holds. The layout of the tables (`multiwriter_catalog.c`) comes from `sqlite_schema` at the snapshot, parsed by SQLite itself in a scratch database.
2. **The replay** (`multiwriter_rebase.c`), on a helper connection of the lane (`mw_norebase=1`: never recursive; triggers off; foreign keys as the application's connection has them; same `synchronous`), in one transaction at the latest snapshot: the schema cookie is
   the one the transaction had; for every row that was changed or deleted the row is *exactly what the transaction saw* (the whole row, with `COLLATE BINARY IS`, so the type affinities and collations of the columns do not hide a difference);
   the deletes, then the updates, then the inserts are run (a key that one frees and another takes); a row that was inserted must not be there, and the constraints (UNIQUE, CHECK, NOT NULL, the key) are evaluated by SQLite on the rows as
   they are now. Any of these that fails is a **true conflict**: the transaction is refused (`SQLITE_BUSY_SNAPSHOT`) and the application runs it again, as without the rebase. If the helper's commit loses a race (a commit that was assigned an epoch but is not yet visible has already installed its pages, so the snapshot of the replay is always older than the page heads) the publication gate is closed after the **first** lost attempt (`MW_REBASE_GATE_AFTER 1`), so that the next attempt runs against a frozen state. The replays are serialised and **batched** (a group replay): the connections whose commit conflicted queue a request (their decoded changes), the first becomes the leader and replays everything that is queued (up to 64) in ONE transaction of its helper, in the order the requests came, a savepoint for each (a request that is a true conflict is rolled back alone, the others see the rows as the ones before them left them), and the successful ones commit together at one epoch (`mw_tx_info.commit_order`: their order in it; the result is a serial execution in that order). One commit and one install of the pages for the group instead of a lost race and a commit for each. In the processes mode the batches are serialised across all the processes by an `fcntl` lock. The decoding of the pages is not serialised: every connection decodes its own.
   The `UPDATE` of a replay sets only the columns that changed (naming an unchanged indexed column rewrites its index, which refuses the transactions that only read that page). A connection whose replay was refused skips the replay of its next 1, 3, 7... conflicts (the true conflicts of a hot row would fail again
   at the cost of the decoding; `mw_rebase_backoff=0` turns it off, for the tests), and a conflict that was rebased does not make the connection take the hot-spot turn.
3. **What the result is.** The state that a serial execution of the two commits could have produced: the replay has the epoch of the helper's commit, and every row that the transaction touched is, in that order, what it was when it read it.
   The commit is a *row-level* first-committer-wins: two transactions that change different cells of the same row conflict (the whole row is compared: the transaction may have read the cells it did not write).
4. **Rows read and not changed.** A page that the transaction only read is validated as before (a change to it refuses the commit). A row that it read from a page that it also wrote cannot be told from a row it changed by the
   pages (a blind `UPDATE` of one row also reads and writes its page), and the replay checks only the rows it changed: a transaction that decided on such a row would commit over a concurrent change of it (write skew,
   the case of two doctors on call: each reads both rows, clears its own). So the statements say it (`mw_lane_reads_unchanged`; the statement hook of a `mw_rebase=1` connection). A transaction is **not rebased**, the commit
   is refused and the application retries, if it ran a `SELECT` (or `WITH`/`VALUES`), or a write statement that is not a *point statement*, or an `UPDATE`/`DELETE` that changed no row (it read that the row is absent).
   A point statement is an `INSERT ... VALUES`, or an `UPDATE`/`DELETE` of one row found by its rowid or by a unique index; it is recognised by its bytecode (`EXPLAIN` of the statement, cached by its text), which has no loop
   (`Next`, `Rewind`...), no cursor opened for reading (another table, a subquery, an IN list), no trigger program, no virtual table. This needs nothing from the build of SQLite (the first version used
   `SQLITE_ENABLE_STMT_SCANSTATUS`: an extension cannot ask for compile options); if `EXPLAIN` is not available the statement is not a point statement (no rebase, never a wrong one). What is replayed is the blind
   transaction: `UPDATE t SET n = n + 1 WHERE id = ?`, `INSERT ... VALUES`, `DELETE ... WHERE id = ?`. The price: a read-modify-write with a `SELECT` before the `UPDATE`, and an update of a range, are not rebased (use
   the expression form). Test `mw_rebaseskew`; `mw_serial` has a transaction that reads a row it does not write (mutation-tested: with the check off it reports 298 to 1278 reads that differ from the serial order). The hook is the connection's one trace callback and SQLite cannot return the current one, so it cannot be saved and put back or chained: an application's own `sqlite3_trace_v2` replaces it. This
   is detected, not prevented: the statement that commits must be the one the hook saw start and that is running now (`sqlite3_next_stmt` + `sqlite3_stmt_busy`); if not, the transaction counts as one that read (no
   rebase; test `mw_rebaseskew`, case 4). The DDL barrier uses the same hook (without it a schema change is still caught by the cookie, with a retry). **A no-op `UPDATE`** (it sets the value a row already has, or `SET a = a`) is counted as a change by SQLite and writes no page (SQLite compares before it overwrites), so the pages do not show it: the row it read, and
   **A no-op `UPDATE`** (it sets the value a row already has, or `SET a = a`) is counted as a change by SQLite and writes no page (SQLite compares before it overwrites), so the pages do not show it: the row it read, and
   thought it wrote, would go unchecked. Example without a serial order: A sets row 3 to 1000 (it is) and changes row 4; B reads row 4 and changes row 3; B commits first; replaying only row 4 for A gives a state in
   which neither order fits. The hook counts the rows changed by the statements of the transaction (`sqlite3_total_changes` from the statement that began it; the committing statement, an autocommit point statement,
   counts one) and the rebase requires that to be exactly the number of net row changes decoded from the pages: then every counted change is a row that changed. A row changed twice, an insert and a delete of the
   same row, a rolled back savepoint make the numbers differ and the transaction is not replayed (it cannot be told from a no-op); a transaction that runs `REPLACE` (which deletes rows that no counter counts) is
   not either. Test `mw_rebasenoop` (with a mutation: the check off, case 1 fails), and `mw_serial`, whose blind updates now may set the value a row has (the check off: it reports reads that differ from the serial
   order in about one scenario of eight).
   **A statement still open at the commit** (`RETURNING` stepped once) is not possible for a write: SQLite refuses the COMMIT with `SQLITE_BUSY` ("SQL statements in progress"); and such a statement is not a point
   statement anyway (it has an ephemeral table in its bytecode), so the transaction would not be replayed.
5. **Not rebased** (refused as before, counted in `unrebasable`): DDL, a database with a trigger or a virtual table, a table with a foreign key to itself or a circle of them, a WITHOUT ROWID table whose key has another
   collation than BINARY or is descending, a write to an internal table (`sqlite_sequence`: AUTOINCREMENT), a page of unknown owner, an overflow page written without its cell, a record that does not decode, a database that
   is not UTF-8, a commit that is not the first of its snapshot.
6. **Foreign keys.** If the application's connection has them on (`PRAGMA foreign_keys`, read from its handle), the helper has them on too, and SQLite checks and runs the actions as in a serial execution: a child whose
   parent was deleted meanwhile is refused; the delete of a parent cascades to a child that appeared meanwhile; one that got a child it cannot have (NO ACTION) is refused. The request is replayed updates first, then the deletes
   from the children to the parents and the inserts from the parents to the children (the rank of the table in the graph of the keys), so that the rows that an action would take were taken already by the replay. A
   parent table in which one request both deletes and inserts is refused (a key that changes is a delete and an insert: the ON DELETE action would run), and so is an update of a key that a foreign key refers to (the ON
   UPDATE action). If the application does not enforce them, nor does the replay. A statement of the transaction that checks a foreign key is still a point statement (the lookup of the parent, the scan for the children
   and the sub-programs of the actions are recognised in the bytecode). Test `mw_rebasefk`.
7. **Short rows.** A row written before an `ALTER TABLE ADD COLUMN` has fewer columns than its table; SQLite reads the missing ones as the default of the column. The catalog evaluates the defaults (with SQLite, in the
   scratch database) and the replay completes the record with them, in SQLite's own encoding, before it compares and binds. Test `mw_rebase` case 10.
8. **WITHOUT ROWID tables.** Their pages are index b-trees, and the cells of their interior pages are rows. The rows are identified by their key (the serial types and bytes of the key columns, which come first in the
   record) instead of the rowid, and the replay finds them by the key. The pages do not say whether they belong to such a table or to an index: the interior pages of the trees of the WITHOUT ROWID tables are walked at the
   snapshot (the leaves are not read) to know every page they list; in the committed state of the transaction a page that one of its written interior pages lists belongs where that page belongs, any other page is where the
   snapshot had it, and a page in neither is an index's (derived data). Tests `mw_rebase` with `MW_TEST_WR=1`, `mw_rebasewr` (threads inserting, updating and deleting their own keys in the same pages) and the
   WITHOUT ROWID scenarios of `mw_serial`.
6. **Statistics** (`MW_FCNTL_DBSTATS`): `rebases` (commits saved), `rebases_grouped` (of them, in a group of two or more), `rebase_retries` (lost races), `rebase_max_attempts`, `rebase_ns`, `unrebasable`.

The test that it must keep passing is the randomised serializability test (below), which runs with and without it.

## Where the state lives

### One process (`mw_mp=0`)
The page store in memory (chains of versions per page, striped locks, `seq_mu` for the assignment of epochs), the commit log `<db>-mw` (a staged ring: the committers copy their record into it, the first one whose record
is covered becomes the leader that writes the prefix and fsyncs it, writing the next group while the previous one syncs; an acknowledged commit is durable), compaction (a thread writes the pages of the log into the real file up to the
oldest snapshot and rewrites the log), and the garbage collection of versions (run by a committing thread every `mw_gc` commits; it skips a chain whose stripe a committer holds and gives the survivors back with one lock).

### Several processes (`mw_mp=1`, the shared mode)
Nothing about pages is kept in a process. The log is a set of mapped segments `<db>-mw.N` (`multiwriter_seglog.c`), the index of versions is a shared hash (`<db>-mwidx`, `multiwriter_shidx.c`) rebuilt from the log by the first
process that opens the database, and a **publication lock** (`<db>-mwlock`, an atomic flag in a shared header with the pid of the holder, plus a ticket lock) serialises validation, append, install and publish; it is held about 40 us per
commit, which sets the ceiling (see the measurements). A holder that dies is detected (the pid is not alive), its half-made commit is finished or undone (`mw_shared_repair`: the record is complete or torn; a death in the roll to a new segment or in a
collection of the index is repaired too), and the log has one record for every epoch. The checksum of a record is made of one hash per page, computed before the lock (and for the pages that a relocation changes, inside it), and the private
copies of a relocation are made before the lock too. The group fsync is shared by the processes (one leader). The sidecars on Linux live in `/dev/shm` (`MW_SIDECAR_DIR`), created with the mode of the database file and `O_NOFOLLOW`.

## Durability and recovery

- A commit is durable when its record is in the log and the log is synced; it is visible after that (one process) or before (several processes: an acknowledged commit is never lost, and a failed fsync fails every commit that is not durable yet,
  in every process, until the log is closed by all).
- Recovery replays the valid prefix of the log (a record is valid as a whole: its checksum covers the pages), cuts a torn tail durably (`fsync` of the file and of the directory), refuses a header that does not check out
  with records behind it (`SQLITE_CORRUPT`) and a read that fails (it is not the end of the log).
- Compaction makes the log durable up to the commits whose pages it writes before it writes them, then moves the base; a crash in between (`MW_CRASH_COMPACT_*`) is tested.
- `mw_fullfsync=1` (or `MW_FULLFSYNC=1`) uses `F_FULLFSYNC` on macOS for the log and the compaction (Linux's `fsync` already flushes the device); off by default, like SQLite's `fullfsync`.

## Errors of the file system (I/O errors, a full disk)

- **A full disk is not the end of a connection.** The staged log reserves disk ahead in steps of 4 MB (`F_PREALLOCATE`, `fallocate` with KEEP_SIZE) before a commit takes its epoch and offset: with no room the commit fails with
  `SQLITE_FULL`, nothing was assigned, installed or written, and the next commit finds the room when the space is back. A write inside the reservation cannot fail for want of space; a write that finds the disk full is retried for up to
  2 s (`MW_ENOSPC_WAIT_MS`). The shared mode appends through segments that are written whole beforehand: a segment that cannot be made fails the commit that needed it and leaves nothing behind.
- **A failed log recovers in place (one process)** (`mw_db_recover`): nothing is visible beyond the last durable commit, so the next commit goes back to that point without a reopen: the log is cut to the end of the last durable record, the
  page store is rebuilt from it, the counters go back. If anything does not fit, the database stays failed until it is reopened. The shared mode does not roll back (see above).
- **Linux.** The sparse files that the processes map together are in `/dev/shm`: on a full file system a first touch of a page of a sparse file is a SIGBUS, not an error.
- Tests: `make test-io` (`mw_ioerr`: every file call of a workload fails in turn, once or from there on, or writes half; `mw_diskfull`: a real volume filled to the brim): after each, the database is opened again and holds every acknowledged
  transaction, none half, with an intact b-tree, and takes new ones.

## Known limit: a reader that never ends

The compactor writes the pages of the log into the file only up to the oldest snapshot still in use (a reader older than the file would read a page newer than its snapshot), and the garbage collector cannot drop the versions
that it needs: with a reader pinned at an old epoch nothing is compacted, the log grows (commits are delayed in proportion to the overshoot, and wait for room at 768 MB), the retained versions fill the memory. SQLite has the same pin (its
WAL cannot be checkpointed) but lets the WAL grow without slowing down. Removing it needs the pre-images of the pages that the compactor overwrites, or ending a reader that stays too long; neither is done.

## Tests

**`mw_serial` under load (2026-10-07).** A run on an idle machine passed almost always; with 14 copies of it at once (`test/serial_stress.sh`) 36 of 70 runs failed. What it showed, in the order it was found:
- A commit that a dead process had in flight is finished by whoever finds the lock of the dead one, and a process that was started afterwards and looked at once saw it absent, went on (a restarted client asks what became of its commit and takes the answer for good) and the commit appeared afterwards: 40-55% of the runs when the death was at "record complete, nothing installed" or "installed, not visible". Now `mw_shared_open_finish` repairs the pending commit of a dead process before an open returns (it also gives back what a holder that died in a collection of the index left, which is why the `SQLITE_FULL` of the scenario with a small index, 30 of 36 runs, became 1 of 14; the rest is below). Test `mw_openrepair` (it fails on the old code).
- The test itself placed the transactions that were in doubt (killed after they had committed, or not) by taking the first one that fitted an epoch nobody accounted for; two of them could fit (two processes killed within a few epochs), and a wrong guess made the model wrong from there on: reports of reads that differ, of a commit that nobody accounts for. The placements that fit are searched now (`replay`), the sorts are total orders (two with the same snapshot always come in the same order: `qsort` is not stable, and a placement that fitted in one try did not in the next).
- The search of the placements ran out of tries when many transactions were in doubt (a `SIGKILL` every 80 ms made 70 of them in a run): it stops a try at its first violation and goes back to the latest choice now (chronological backtracking), 5000 tries at most. The record files of a killed run (`<db>.rec<n>`) stayed in `/tmp`, and a process number that was used again gave the same path: their transactions were replayed into the next run (violations at epoch 14); `make_db` removes them.
- A stall of the writers, one run in 300 with 24 copies at once (a run where no transaction commits for the 6 seconds, the readers go on): the first process to open a database in the shared mode let the others in (`ready`, downgrade of the `flock`) and only then finished the header (`committed_epoch` = the epoch found on disk, in `mw_shared_open_finish`). A process that committed in that interval (epoch 2) had its epoch overwritten: `committed_epoch` went back to 1, the versions that the commit had installed stayed at 2, and every commit of every process conflicted for ever with a version that nobody could see (`SHCONFLICT page 2 head 2 snap 1 committed 1`). The first process lets the others in at the end of `mw_shared_open_finish` now. Test `mw_openrace` (deterministic with a delay in the first process: it fails on the old code with 40 commits that do not go through). 0 stalls in 960 runs.
- Result: 0 violations in 384 runs with 24 at once and a kill every 80 ms in the scenario with the rebase (before: 3 in 340, then 2 in 380). `test/serial_stress.sh` and the knobs `MW_SERIAL_ONLY`, `MW_SERIAL_NO_KILL`, `MW_SERIAL_NO_CRASH`, `MW_SERIAL_PT`, `MW_SERIAL_GC`, `MW_SERIAL_KILL_MS`, `MW_SERIAL_DEBUG`, `MW_SERIAL_STALL_SAMPLE` (the header of the shared state and the stacks of the processes when the writers stop) are in the repository.
- One period of the session showed 42 of 112 and 26 of 112 failures with the death at one point of the publication: the record files of killed runs (above) are the likely cause (a process number used again); not shown.
- **The long hunt before the first release** (`test/hunt.sh`, 2026-10-07): 1128 runs of `mw_serial` with 24 copies at once in rotation of eight configurations (default; a kill every 60 ms; `MW_TEST_MP=1` with kills every 100 and 60 ms; the scenarios with the rebase with a kill every 60 ms; the death at "record complete", at "installed, not visible" and inside a collection of the index): 0 failures; the 72 runs of the build with AddressSanitizer: no report, no failure; the power-loss test in the container (ext4 on a disk with a volatile cache) with 6 seeds in three modes (processes, processes with the rebase, threads with the rebase): 17 runs with 0 failures, 1 that did not start (the container failed to partition the disk). One earlier run of the hunt (among about 350) could not open the database for its verification (`SQLITE_CORRUPT`, with 805 processes that died at once at the start of a scenario): it was not seen again in 1300 runs; the test prints the files and the header when this happens now. This is a few thousand runs, not days: it says the known failures are gone, not that there are no others.
- **The hunt after the rebase of foreign keys, short rows and WITHOUT ROWID tables** (2026-10-08): 336 runs of the new `mw_serial` (it has WITHOUT ROWID scenarios, with and without the rebase, with threads and with processes) with 24 copies at once, the same eight configurations: no violation of the serial order, no broken file. Three runs reported a failed *guard* (the rebase took part in a WITHOUT ROWID scenario of a few seconds under that load: 0 rebases; the guard is now `mw_rebasewr`'s, not `mw_serial`'s) or the number of kills (the configurations without kills). The new tests of this step: `mw_rebasefk` (the cases by hand), `mw_rebasefkstress` (parents and children with ON DELETE CASCADE in the same pages, 4 threads, with the rebase: the rows, the foreign keys and the file are right), `mw_rebasewr`, and `mw_routes` (the shortcut of the read validation is never used for an interior page of an index: pages built by hand; with the shortcut on for indexes it fails). A test of the stale read itself with real transactions (`mw_readdeps`) did not reproduce it (the shortcut was not reached): what shows it is `mw_serial` on a WITHOUT ROWID table in the processes mode (about a run in three failed before the change).
- The residual failures with the rebase and a kill every 60 ms (3 in the first 40 runs: reads that saw rows that the model did not have, from epoch 2 on) were in the test: the replay started at the epoch of the first recorded commit, and a transaction in doubt that had committed before it (the first of the database, epoch 2, killed at once) had no epoch to be placed in, so the model lacked what it had written. It starts at epoch 2 now. 0 failures in 480 runs of the scenarios with the rebase with 24 at once and a kill every 60 ms. The minimum of read-write transactions of a scenario with threads is 100 (it was 500: the hot rows with the rebase, 24 copies at once, give a few hundred).
- `make test-mp` (`MW_TEST_MP=1`: the scenarios with threads use the shared mode too) under the same load: the variable `MW_IDX_ENTRIES=3000` of the scenario with the small index stayed set, and the scenarios with the rebase that follow it ran with an index of 3000 versions (refusals with `SQLITE_FULL` counted as errors, 100-800 of them); it is unset after that scenario now. The small-index scenario itself refused 4-6 commits in a few runs (the window between the death of a compactor and another process taking its claim): the check is `full_errors < 500` now (the stall that was the bug refused thousands). 0 failures in 192 runs of the whole test with `MW_TEST_MP=1` and 24 at once.
- `SQLITE_FULL` of the scenario with the small index under load (still 1 run in 70 with a kill every 60 ms, 5700 commits refused in the run): the process that had claimed the next compaction was killed while it compacted. Its claim (`compact_req_ns`) is void only after 2 s, and `compact_busy_T`, what the compaction announces to be reading so that the collection of the index does not free it, stayed at the value of the dead one: with 3000 entries the index is full in a tenth of a second, and nothing could be freed for those 2 s. The claim carries the process number now (`compact_req_pid`): another process takes it over at once when the claimant is gone, and a compaction that starts resets `compact_busy_T` (it holds the compaction lock: what is there is the value of a dead one). Test `mw_compactdeath` (the claimant dies after writing the pages; the longest time without a commit was 2 s, it is 0 now). 0 refusals in 240 runs with 24 at once.

`make test` (about 35 programs: the page store, lanes, relocation, merge, read dependencies, DDL, savepoints, the log and its recovery, compaction, garbage collection, the shared mode, crashes at every point of the publication, I/O errors,
the relocation prepared before the lock, the rebase (`mw_rebase`: what is replayed, what is refused and what is never rebased, case by case), the serializability test), `make test-mp` (the transaction tests with `mw_mp=1`), `make test-io` (minutes), `test/sanitize.sh asan|ubsan|tsan [tests]`, a Linux container for all of it
(`docker run --rm -v "$PWD":/src gcc:14 ...`, `--privileged` for the size-limited tmpfs of `mw_diskfull`), and `test/power/` (loss of power).

- **`mw_serial`: a randomised serializability test.** Eight threads run random transactions (insert, update, delete, a change of a UNIQUE column, growth of a payload that splits and frees pages, a counter) on 12, 200 and 3000 keys; each one
  records what it read and did, the epoch it read from and the epoch it committed at (`MW_FCNTL_TXINFO`). Afterwards the committed transactions are replayed on a model in the order of their commit epochs and it checks (1) every read-write
  transaction saw exactly the model's state just before it (every row that it reads it also writes, so under snapshot isolation with first-committer-wins this must hold exactly: a lost update, a write over a deleted row, two rows with
  one UNIQUE value would show), (2) every read-only transaction saw the state after the commits up to its snapshot, (3) the final table is the model's, the UNIQUE column has no duplicate (a table scan), `integrity_check` is ok. A change that
  changes nothing writes no page, so the generator never makes one (the row would be read and not written: write skew). The scenarios run twice, without and with `mw_rebase=1` (and then the check that rebases took part). With the rebase, half of the transactions read no row (blind statements, guarded so that they really change the row: the ones the rebase replays) and the others sometimes read a row that they do not write.
  **With real processes and SIGKILL** (`mw_mp=1`, one connection per process): 6 processes, a random one killed with SIGKILL every 125-375 ms and replaced, most of them also dying by themselves at a crash point of the publication
  (`MW_CRASH_MID_LOG`, `BEFORE_LOG`, `SHARED_APPENDED`, `SHARED_INSTALLED`, `AFTER_LOG`, `AFTER_VISIBLE`, `SHARED_GC`). A transaction killed between its intent (written to a record file before COMMIT) and its outcome is in doubt: every
  transaction updates the row of its process in `txlog` (a page of its own) with its sequence number, so the database says whether it committed; its commit epoch is not known and the model places it at an epoch that no recorded
  transaction has. More checks: no epoch without a transaction to account for it (a commit nobody recorded, or a lost one), every in-doubt transaction that committed fits an epoch, the last sequence number of each process in the database is
  the records', and (small index scenario, `MW_IDX_ENTRIES=3000`) no commit refused with `SQLITE_FULL` (a collection of the index that dies half way must not leave pages nobody collects).
  *Mutation tests* (the code was broken on purpose, then restored): ignoring one write-write conflict in 50 (threads: dozens of reads that differ from the serial order and broken UNIQUE values; shared mode: hundreds of thousands, a table of 763 rows against
  126 in the model, a failing `integrity_check`); ignoring the read conflicts (hundreds of inconsistent reads, duplicate UNIQUE values, a failing `integrity_check`); a repair of a dead publisher that finishes a commit without installing its pages (4-5 epochs
  per run that nobody accounts for) or undoes a complete record (a table of 93 rows against 227); `shidx_gc_repair` turned into a no-op (537 thousand commits refused with `SQLITE_FULL` in the small-index scenario); and, for the rebase,
  a replay that does not check that the row is what the transaction saw (13 thousand reads that differ from the serial order, 244 broken UNIQUE values, in the hot scenario).
  A bug of the test itself that it found about the rebase: a transaction that reads a row on a page that it writes and does not write the row (a change that changes nothing) is write skew; the rebase allows it (see above), so the generator avoids it.
- **`test/power/`: durability against a loss of power, without a machine to switch off.** The kernel of Docker Desktop has no `dm-flakey`, so the disk is emulated: `nbdsrv.py` serves over NBD a disk whose write cache is volatile
  (a write reaches the image at a FLUSH, with FUA, or by chance at the cut, in blocks of 4 KB written whole or not at all), `nbdcli.c` attaches it to `/dev/nbdN` with the kernel's ioctls, and `run.sh` (image of `test/power/Dockerfile`:
  gcc, e2fsprogs, python3; `docker run --privileged`) runs `mw_serial` in phase `run` on ext4 on that disk (processes or, with `MW_SERIAL_MODE=threads`, six threads of one process: the staged log), stops everything, cuts the power of the disk,
  abandons the device (the page cache and the cache of the disk are lost), forgets the memory (`/dev/shm`), attaches the image on another device, mounts ext4 again (journal replay) and runs phase `verify`: the engine recovers the database and the
  same checks run, so every acknowledged transaction must be there and what is there a prefix of the commits (`MW_SERIAL_REBASE=1` for the rebase). 0 violations in the runs; a group commit that does not sync, and a staged log written and not
  synced, are caught (acknowledged transactions lost). Limits: 4 KB atomic blocks, ext4's barriers; not a disk that reorders around a FLUSH, not a cut during a flush, not other file systems. The flush of the directory that the engine does after creating or
  renaming a segment could not be shown to matter (ext4, with or without a journal, flushes the entry of a file that is fsynced while it is still new); it is hygiene the standard asks for.

## Parameters (URI and environment)

URI parameters (read at open): `mw` (0 off, 2 lanes: the engine), `mw_mp` (0 one process, 1 processes: the shared mode), **`mw_rebase`** (1: the rebase), `mw_fullfsync`, `mw_profile` (`small`: the cache of pages of the
real file at 8 MB, a log of 16 MB before it is compacted), `mw_base_cache_mb`, `mw_log_max_mb`, `mw_gc` (commits between two collections of versions), `mw_hot_credit`, `mw_readcheck` (0: no validation of the pages read), and, for tests and
measurements, `mw_noreloc`, `mw_noroute`, `mw_nomerge`, `mw_norebase` (marks a connection that never rebases: the helper), `mw_rebase_backoff` (0: replay every conflict), `mw_prep_delay_us` (waits between the preparation of a relocation and the publication lock).

Environment: `MW_FULLFSYNC`, `MW_PROFILE`, `MW_SEG_MB` (size of a segment of the shared log), `MW_IDX_ENTRIES`, `MW_SIDECAR_DIR` (where the shared maps go: `/dev/shm` on Linux), `MW_SPIN_US`, `MW_POOL_BATCH`, `MW_ENOSPC_WAIT_MS`;
diagnostics: `MW_DEBUG`, `MW_TIMING` (time spent in each stage of a commit), `MW_IO_TRACE`, `MW_COMPACT_TRACE`, `MW_PAGE_TRACE`. Measurement only (`make EXPERIMENTS=1`): `MW_EXP_*`.

## Security of the files

The log, the segments, the shared maps and the lock files are created with the mode of the database file (without execute bits; 0600 if it does not exist yet) and opened with `O_NOFOLLOW`. A sidecar in `/dev/shm` therefore has the same readers as
the database. Not covered: a hostile local user who owns the directory of the database.

## What is covered, what is not

Covered and tested: rowid tables with and without INTEGER PRIMARY KEY, values of every type, overflow values, indexes, UNIQUE constraints, DDL (as above), savepoints and rollbacks, `VACUUM` (as DDL), threads and processes, crashes (SIGKILL at any moment and at
every point of the publication and of the compaction), I/O errors and a full disk, a loss of power (ext4 on an emulated disk). Not covered: `journal_mode` other than WAL, exclusive locking, attached databases (each file is its own database
object: the transactions that span them are not atomic across the files), virtual tables with the rebase (it refuses), a database of another text encoding with the rebase, serializability (write skew), a reader that never ends (above), a machine
that reorders writes around a flush. Platforms: macOS and Linux (arm64 tested); iOS, Windows, Android later.

## Third party and license

The code is under the Apache License 2.0 (`LICENSE`, `NOTICE`). `third_party/sqlite` is SQLite 3.53.4 (public domain), unchanged. The measurements are in `bench/results/`.

## Measurements

The tables for the readers are in `docs/benchmarks.md` (2026-10-07, this Mac, 18 cores, SQLite 3.53.4, `synchronous=FULL`, one run of 8 s per point; `bench/results/simple_*_2026-10-07.jsonl`, made by `bench/compare_sqlite.py` and `bench/make_report.py`).
For this engine a retry is a commit that was refused and that the application ran again; the replays that the rebase lost to other commits and repeated inside the engine are the "lost replays per merged commit". "Gave up" is a transaction that did not commit after 1000 retries.
The workloads (`bench/mw_bench.c`): **bulk** = 100-row inserts on disjoint keys; **groups** = each agent updates its own row, four groups whose rows share a page; **same page** = each agent its own row, all on one page; **hot rows** = `UPDATE a = a + 1` on 4 rows (true conflicts);
**columns** = different columns of rows spread over the table (true conflicts where rows coincide, the rebase compares whole rows).

Summary (16 writers, tx/s: SQLite / engine without rebase / engine with rebase):

| workload | threads | processes |
|---|---|---|
| bulk | 8.5k / 46k / 46k | 8.4k / 24k / 24k |
| groups | 10k / 33k (8 retries per 100 tx) / 51k (0) | 13k / 43k (106) / 47k (0) |
| same page | 17k / 33k (19) / 62k (0) | 13k / 43k (119) / 35k (0) |
| hot rows | 15k / 27k (21) / 28k (39) | 13k / 42k (119) / 43k (108) |

What the rebase does, measured. Where the conflicts are on pages and not on rows (groups, same page) the application sees **no retry at all**, 68-86% of the commits with threads (19-31% with processes) are saved by a merge, and with threads the throughput is higher
(same page 16 threads 62k against 33k; 64 threads 38k against 32k). With 4 threads it can be slower (groups 29k against 40k): a replay costs a decode, a replay and a second commit. With processes on one single page it is lower (16 processes 35k against 43k, 0 retries against 119 per 100 tx),
because the batches are serialised across processes. Where the conflicts are on the same rows (hot rows, columns) it cannot save them (it refuses, correctly); the back-off that skips the replay after a refusal limits the loss, but with 64 threads on hot rows it is still lower (22k against 28k) and has more retries (156 against 101 per 100 tx). It is for an application that cannot retry, or whose retries are costly.

**The group replay, and why the replays are not parallel (measured).** The first version, a mutex per database around each replay, was 13.7k tx/s at 64 threads on one page against 31k for the engine without the rebase. Two of the causes were not the mutex: (1) an `UPDATE` that named
every column rewrote the index of an indexed column that had not changed, so every reader of that index page was refused (the replay now sets only the columns that changed: read conflicts 254 thousand -> 0); (2) a rebased conflict granted the connection a turn of hot-spot serialisation
(it no longer does). With those fixed, replays that ran in parallel (no mutex) were 20-40% slower than serialised ones (same page: 18-24k against 31-36k): the replays of one page can only commit one at a time, and the ones that run together lose to each other and to the ordinary commits; with the gate they could also deadlock.
What can run together is the decoding of the pages (it does) and, for the commit, the **group**: the replay of several requests in one transaction is what raised the contended workloads from 30-33k to 37-62k tx/s. The commits of one group share an epoch, so `mw_serial` orders the commits of an epoch by `commit_order`.

**Lost replays.** A helper replay used to lose its race to an ordinary commit 97% of the time (the snapshot is always older than the page heads, see above). Closing the gate after the first lost attempt (it stops the other committers only for the length of one replay, and `mw_gate_enter` spins with `sched_yield` before it sleeps)
brought the lost replays to about 5% for the mixed workloads (the table above shows 0.0-1.3 lost replays per merged commit, the high ones on hot rows where the true conflicts are replayed and refused too).

**The replay on one page with processes (measured, 2026-10-07, 16 processes, no change made).** 8.5-9k merges a second, 1.77 attempts per merge, each attempt about 12 us of begin, replay and decode and about 32 us of commit (it queues for the publication lock like any other commit); the waiting for the rebase lock is 700 us per attempt (16 processes queue). Tried and not kept:
(1) closing the gate in the shared mode is no use because the gate is per process: the other processes keep committing, and 77% of the first attempts lose; (2) taking the publication lock from before the `BEGIN` of the helper (the replay then cannot lose, 0 lost attempts) made it 18k tx/s against 35k: every ordinary commit of the other processes
conflicts too and becomes a merge, and the merges are serialised (55 us each); (3) the rebase lock in the shared header (spinning, or sleeping 20 us) instead of the `fcntl` lock: 22-27k against 35k (the waiters that poll take the cores of the ones that work; the kernel's lock keeps them asleep).
What is left is the cost of one commit through the publication lock for each merged commit; only a batch that spans the processes (the leader replaying the requests of other processes) would remove it, and it needs a record in the log that tells a requester whose leader died whether its request committed.
