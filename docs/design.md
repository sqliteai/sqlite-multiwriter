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
2. **The replay** (`multiwriter_rebase.c`), on a helper connection of the lane (`mw_norebase=1`: never recursive; triggers and foreign keys off; same `synchronous`), in one transaction at the latest snapshot: the schema cookie is
   the one the transaction had; for every row that was changed or deleted the row is *exactly what the transaction saw* (the whole row, with `COLLATE BINARY IS`, so the type affinities and collations of the columns do not hide a difference);
   the deletes, then the updates, then the inserts are run (a key that one frees and another takes); a row that was inserted must not be there, and the constraints (UNIQUE, CHECK, NOT NULL, the key) are evaluated by SQLite on the rows as
   they are now. Any of these that fails is a **true conflict**: the transaction is refused (`SQLITE_BUSY_SNAPSHOT`) and the application runs it again, as without the rebase. If the helper's commit loses a race it replays again on the
   newer snapshot; after three losses the publication gate is closed after 32 lost attempts so that the next one runs against a frozen state. The replays are serialised (per database, and in the processes mode across all the processes by an `fcntl` lock: measured faster than parallel, see the measurements); the decoding of the pages is not.
   The `UPDATE` of a replay sets only the columns that changed (naming an unchanged indexed column rewrites its index, which refuses the transactions that only read that page). A connection whose replay was refused skips the replay of its next 1, 3, 7... conflicts (the true conflicts of a hot row would fail again
   at the cost of the decoding; `mw_rebase_backoff=0` turns it off, for the tests), and a conflict that was rebased does not make the connection take the hot-spot turn.
3. **What the result is.** The state that a serial execution of the two commits could have produced: the replay has the epoch of the helper's commit, and every row that the transaction touched is, in that order, what it was when it read it.
   The commit is a *row-level* first-committer-wins: two transactions that change different cells of the same row conflict (the whole row is compared: the transaction may have read the cells it did not write).
4. **What is not covered.** The rows that the transaction read and did *not* change: a page that it only read is validated as before, but a row that it read from a page that it also wrote is not looked at. That is snapshot isolation
   (write skew is possible between a read of such a row and a concurrent write of it), and what the page-level validation did not allow: this is why the rebase is opt-in. A transaction that does `UPDATE ... SET x = x` (no change, no page written)
   next to other writes on the same page is such a read.
5. **Not rebased** (refused as before, counted in `unrebasable`): DDL, a database with a trigger, a foreign key, a virtual table or a WITHOUT ROWID table, a write to an internal table (`sqlite_sequence`: AUTOINCREMENT), a page of
   unknown owner, an overflow page written without its cell, a record that does not decode, a row with fewer columns than its table (`ALTER TABLE ADD COLUMN`), a database that is not UTF-8, a commit that is not the first of its snapshot.
6. **Statistics** (`MW_FCNTL_DBSTATS`): `rebases` (commits saved), `rebase_retries` (lost races), `rebase_max_attempts`, `rebase_ns`, `unrebasable`.

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

`make test` (about 35 programs: the page store, lanes, relocation, merge, read dependencies, DDL, savepoints, the log and its recovery, compaction, garbage collection, the shared mode, crashes at every point of the publication, I/O errors,
the relocation prepared before the lock, the rebase (`mw_rebase`: what is replayed, what is refused and what is never rebased, case by case), the serializability test), `make test-mp` (the transaction tests with `mw_mp=1`), `make test-io` (minutes), `test/sanitize.sh asan|ubsan|tsan [tests]`, a Linux container for all of it
(`docker run --rm -v "$PWD":/src gcc:14 ...`, `--privileged` for the size-limited tmpfs of `mw_diskfull`), and `test/power/` (loss of power).

- **`mw_serial`: a randomised serializability test.** Eight threads run random transactions (insert, update, delete, a change of a UNIQUE column, growth of a payload that splits and frees pages, a counter) on 12, 200 and 3000 keys; each one
  records what it read and did, the epoch it read from and the epoch it committed at (`MW_FCNTL_TXINFO`). Afterwards the committed transactions are replayed on a model in the order of their commit epochs and it checks (1) every read-write
  transaction saw exactly the model's state just before it (every row that it reads it also writes, so under snapshot isolation with first-committer-wins this must hold exactly: a lost update, a write over a deleted row, two rows with
  one UNIQUE value would show), (2) every read-only transaction saw the state after the commits up to its snapshot, (3) the final table is the model's, the UNIQUE column has no duplicate (a table scan), `integrity_check` is ok. A change that
  changes nothing writes no page, so the generator never makes one (the row would be read and not written: write skew). The scenarios run twice, without and with `mw_rebase=1` (and then the check that rebases took part).
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

URI parameters (read at open): `mw` (0 off, 2 lanes: the engine), `mw_mp` (0 one process, 1 processes: the shared mode, 3 processes with private stores), **`mw_rebase`** (1: the rebase), `mw_fullfsync`, `mw_profile` (`small`: the cache of pages of the
real file at 8 MB, a log of 16 MB before it is compacted), `mw_base_cache_mb`, `mw_log_max_mb`, `mw_gc` (commits between two collections of versions), `mw_hot_credit`, `mw_readcheck` (0: no validation of the pages read), and, for tests and
measurements, `mw_noreloc`, `mw_noroute`, `mw_nomerge`, `mw_norebase` (marks a connection that never rebases: the helper), `mw_rebase_backoff` (0: replay every conflict), `mw_prep_delay_us` (waits between the preparation of a relocation and the publication lock).

Environment: `MW_FULLFSYNC`, `MW_PROFILE`, `MW_SEG_MB` (size of a segment of the shared log), `MW_IDX_ENTRIES`, `MW_SIDECAR_DIR` (where the shared maps go: `/dev/shm` on Linux), `MW_MP_LAZY`, `MW_MP_PRIVATE`, `MW_SPIN_US`, `MW_POOL_BATCH`, `MW_ENOSPC_WAIT_MS`;
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

The code is under `LICENSE.md` (Elastic License 2.0, modified). `third_party/sqlite` is SQLite 3.53.4 (public domain), unchanged. The measurements are in `bench/results/`.

## Measurements (2026-10-06, this Mac, 18 cores, SQLite 3.53.4, `synchronous=FULL`, one run per point of 8-10 s; `bench/results/rebase2_*_2026-10-06b.jsonl`)

`bench/compare_sqlite.py`: "no rebase" and "rebase" are this engine without and with `mw_rebase=1`; "SQLite" is stock WAL with `busy_timeout 0` and the application retrying a refused transaction (its retries are counted).
For this engine a retry is a commit that was refused and that the application ran again; the replays that the rebase lost to other commits and tried again inside the engine are not counted here (they are `rebase_retries`
in the statistics: about 3 per rebase on one hot page). "Gave up" is a transaction that did not commit after 1000 retries (none unless written). The workloads (`bench/mw_bench.c`): **bulk** = 100-row inserts on disjoint keys;
**groups** = each agent updates its own row, the rows of four groups of agents share a page each (four contended pages); **same page** = each agent its own row, all rows on one page; **hot rows** = `UPDATE a = a + 1` on 4 rows
(true conflicts: the same rows); **columns** = agents update different columns of shared rows (the same rows: true conflicts for the rebase, which compares the whole row); **unique inserts** = inserts of unique keys into one table.

**Bulk inserts on disjoint keys, threads**

| N | no rebase tx/s | retries | rebase tx/s | retries | SQLite tx/s | retries |
|---|---|---|---|---|---|---|
| 1 | 14664 | 0 | 14885 | 0 | 12893 | 0 |
| 2 | 21511 | 14 | 20991 | 10 | 9278 | 106858 |
| 4 | 32261 | 92 | 32145 | 63 | 8783 | 115434 |
| 8 | 38324 | 202 | 38378 | 234 | 8546 | 128976 |
| 16 | 46259 | 625 | 47010 | 645 | 8650 | 165730 |
| 32 | 49655 | 1887 | 49370 | 1641 | 8093 | 226797 |
| 64 | 41351 | 5134 | 41698 | 3098 | 8137 | 344395 |

**Bulk, processes (`mw_mp=1`)**

| N | no rebase tx/s | retries | rebase tx/s | retries | SQLite tx/s | retries |
|---|---|---|---|---|---|---|
| 1 | 14166 | 0 | 14250 | 0 | 13144 | 0 |
| 2 | 19932 | 9 | 20016 | 7 | 9659 | 108399 |
| 4 | 28280 | 33 | 28277 | 27 | 8932 | 118165 |
| 8 | 26760 | 89 | 26915 | 76 | 8683 | 128762 |
| 16 | 24782 | 346 | 24714 | 324 | 8392 | 155666 |
| 32 | 22888 | 877 | 22734 | 561 | 8166 | 200971 |
| 64 | 20585 | 1045 | 20460 | 578 | 7884 | 286828 |

The rebase changes nothing here (few conflicts: the pages that the agents share are grown by the file, which the relocation saves). With one writer the engine is above SQLite (14.2-14.9k against 12.9-13.1k).
The throughput of the processes mode is bounded by the publication lock (held about 40 us per commit).

**Four contended pages (groups), threads**

| N | no rebase tx/s | retries | rebase tx/s | retries | SQLite tx/s | retries |
|---|---|---|---|---|---|---|
| 4 | 36551 | 2194 | 35597 | 0 | 15830 | 5301 |
| 16 | 37344 (6 gave up) | 23970 | 42257 | 0 | 15328 | 27714 |
| 64 | 35769 | 204770 | 45274 | 0 | 15217 | 124678 |

**Groups, processes**

| N | no rebase tx/s | retries | rebase tx/s | retries | SQLite tx/s | retries |
|---|---|---|---|---|---|---|
| 4 | 38082 | 55541 | 36296 | 0 | 20763 | 7080 |
| 16 | 34149 | 333872 | 37438 | 0 | 22628 | 39546 |

**One contended page (same page), threads**

| N | no rebase tx/s | retries | rebase tx/s | retries | SQLite tx/s | retries |
|---|---|---|---|---|---|---|
| 4 | 31752 | 9028 | 23075 | 0 | 14891 | 5189 |
| 16 | 28852 | 46493 | 30546 | 0 | 18304 | 29389 |
| 64 | 32237 | 253015 | 32946 | 0 | 15314 | 123083 |

**Same page, processes**

| N | no rebase tx/s | retries | rebase tx/s | retries | SQLite tx/s | retries |
|---|---|---|---|---|---|---|
| 4 | 32872 | 149820 | 30896 | 0 | 20913 | 6142 |
| 16 | 35303 | 396507 | 25157 | 0 | 24048 | 38498 |

**Four hot rows (true conflicts), threads**

| N | no rebase tx/s | retries | rebase tx/s | retries | SQLite tx/s | retries |
|---|---|---|---|---|---|---|
| 4 | 34761 | 9377 | 24810 | 19531 | 15760 | 5591 |
| 16 | 27107 | 45279 | 28921 | 86505 | 15155 | 27979 |
| 64 | 31921 | 260089 | 30017 | 339802 | 14844 | 119131 |

**Hot rows, processes**

| N | no rebase tx/s | retries | rebase tx/s | retries | SQLite tx/s | retries |
|---|---|---|---|---|---|---|
| 4 | 34384 | 171686 | 33779 | 119766 | 18823 | 6356 |
| 16 | 37482 | 409355 | 35312 | 360909 | 15425 | 35921 |

**Different columns of shared rows (columns), threads**

| N | no rebase tx/s | retries | rebase tx/s | retries | SQLite tx/s | retries |
|---|---|---|---|---|---|---|
| 4 | 25936 | 10063 | 29674 | 9930 | 15710 | 7806 |
| 16 | 34175 | 58624 | 27349 | 51336 | 15221 | 32309 |
| 64 | 25120 | 234469 | 19398 | 62907 | 14028 | 140740 |

**Unique inserts into one table, threads**

| N | no rebase tx/s | retries | rebase tx/s | retries | SQLite tx/s | retries |
|---|---|---|---|---|---|---|
| 4 | 30277 | 8237 | 30286 | 8027 | 12899 | 57954 |
| 16 | 30060 | 48237 | 29946 | 48402 | 10566 | 101588 |
| 64 | 25999 | 245325 | 26311 | 248249 | 7368 | 168344 |

(The processes runs of "columns" and "unique inserts" are not shown: the benchmark cannot verify them with several processes, and reports them as not valid.)

What the rebase does, measured: where the conflicts are on pages and not on rows (groups, same page) the application sees **no retry at all** and the throughput is the same or higher (groups, 64 threads: 45k against 36k and 205 thousand
retries; same page, 4 processes: 47k against 41k and 216 thousand retries); the repeated replays inside the engine are a cost, not a gain. Where the conflicts are on the same rows (hot rows, columns) it cannot save them (it refuses, correctly), it
costs a decode and a failed check for each, and the back-off that skips the replay after a refusal keeps the throughput near the engine's without it (hot rows 64 threads 30k against 32k) with more retries
(340 thousand against 260 thousand) in some runs and fewer in others (columns, 64 threads: 63 thousand against 234 thousand). It is for an application that cannot retry, or whose retries are costly. In the processes mode with 16 processes on one
or four pages the throughput is now 33-48k only because the replays are serialised across all the processes (an `fcntl` lock): without it the replays of the processes beat each other (15k and 27k).

**Why the replays are serialised, and not parallel (measured).** The first version with a mutex per database was 13.7k tx/s at 64 threads on one page against 31k for the engine without the rebase. Two of the causes were not the mutex: (1) an `UPDATE` that named
every column rewrote the index of an indexed column that had not changed, so every reader of that index page was refused (the replay now sets only the columns that changed: read conflicts 254 thousand -> 0); (2) a rebased conflict granted
the connection a turn of hot-spot serialisation (it no longer does). With those fixed, replays that run in parallel (no mutex, the gate only for the starving) were 20-40% *slower* than serialised ones (same page: 18-24k against 31-36k;
lost races 280 thousand against 148 thousand): the replays of one page can only commit one at a time, and the ones that run together lose to each other and to the ordinary commits. Parallel replays without a gate also hang in the
worst case (two replays closing the gate). What does run in parallel: the decoding of the pages (before the lock), the ordinary commits, and the replays of nothing else. The publication gate is closed only after 32 lost attempts (it was 3: it stops every
committer; 20-60% slower). A replay that could run in parallel for different pages would have to be a group replay (several transactions in one helper commit, one epoch): not done.
