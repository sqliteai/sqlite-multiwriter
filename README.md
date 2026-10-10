# sqlite-multiwriter

**With 16 threads writing one database, 49,277 transactions per second against 8,630 for SQLite (5.7x), and the slowest 1 in 1000 commits takes 2.08 ms instead of 157 ms.**
**With 16 processes, 25,987 against 8,636 (3.0x), and 1.54 ms against 233 ms.**

SQLite is the most widely used database in the world, and it has one well-known limit: a database accepts only one writer at a time. With many threads, or many processes (several agents working on the
same database, for example), the writers queue up behind one lock, or fail with `SQLITE_BUSY` and have to try again.

sqlite-multiwriter removes that limit without touching SQLite. It is a VFS that you load as an extension (or link in): your SQLite, your SQL and your database file stay the same, and many connections
write the same database at once. Every commit is durable, and the file remains an ordinary SQLite database.

- **More writes per second.** Writers that touch different rows (or different pages) do not wait for each other.
- **Almost no `SQLITE_BUSY`.** With writers on their own rows the application retries about 2 in 1000 transactions.
- **Threads or processes.** One process with many connections, or many processes on one file.
- **Nothing to change in the schema or in the SQL.** No triggers, no new syntax, no special tables.

## In numbers

16 writers on one database against stock SQLite in WAL mode, `synchronous=FULL` (every commit is on disk when it returns). Latency is the time from the start of a transaction to its commit, retries included. Best result in bold; how it was measured is under the tables below.

| 16 writers | SQLite tx/s | multiwriter tx/s | Gain | Retries per 100 tx | Slowest 1 in 1000 commits |
|---|---:|---:|---:|---:|---:|
| Threads, each inserting its own rows | 8,630 | **49,277** | 5.7x | 189 → **0.2** | 157 → **2.08** ms |
| Threads, own rows on shared pages (with rebase) | 13,562 | **55,098** | 4.1x | 35 → **0** | 584 → **0.93** ms |
| Processes, each inserting its own rows | 8,636 | **25,987** | 3.0x | 189 → **0.1** | 233 → **1.54** ms |
| Processes, own rows on shared pages (with rebase) | 13,225 | **51,472** | 3.9x | 30 → **0** | 664 → **16.6** ms |
| Threads, all on the same 4 rows | 13,277 | **29,976** | 2.3x | 24 → **17** | 609 → **122** ms |

One writer is not slower than SQLite (15,959 against 13,746 tx/s). When writers change the same rows they really conflict: the gain is smaller and the retries stay.

## How it works

Each writer has its own WAL. Its transactions run there on a snapshot of the database, and at commit they are checked, ordered and published; the log is merged into the database file in the background:

    writer 1 --> private WAL --+
    writer 2 --> private WAL --+--> check and publish --> commit log --> database file
    writer 3 --> private WAL --+    (first committer wins)               (compaction)

Each transaction runs on a snapshot of the database. At commit, its pages are checked against the commits published in the meantime. If nobody changed what it wrote or read, it commits: writers do not wait for
each other. If another commit got there first on the same page, the transaction is refused with `SQLITE_BUSY_SNAPSHOT` and the application runs it again (first committer wins). Commits go through a log with group commit
(the durability of WAL with `synchronous=FULL`) and are compacted into the database file in the background.

**The rebase** (`mw_rebase=1`, optional). Two writers that change different rows can still land on the same page, and that is the usual cause of a refused commit. With the rebase the engine does not refuse the
loser: it takes the row changes of its transaction and applies them again on top of the latest state, then commits. The application sees no error. A transaction that really conflicts (the same row changed by both)
is still refused. The rebase is limited to simple transactions: `INSERT`s and `UPDATE`/`DELETE`s of a row by its key. See Limits.

Isolation is snapshot isolation plus validation of the pages a transaction read: if another commit rewrote one of them after the snapshot, the commit is refused, which rules out the usual write skew (`docs/design.md`, "What is guaranteed"). It is tested, not proved serializable. The application must retry a transaction that fails with `SQLITE_BUSY_SNAPSHOT`, the whole transaction.

## Two ways to use it

| | Threads | Processes (`mw_mp=1`) |
|---|---|---|
| Who writes | many connections in one process | many processes (agents, workers, a CLI and a server) on the same file |
| Shared state | memory of the process | shared memory and a log in segments next to the database |
| Platforms | all | all (little use in an iOS app: one process) |

Add `mw_rebase=1` to either. `vfs=multiwriter` is all it takes to turn the engine on (the SQLite connection has to be opened with `SQLITE_OPEN_URI`).

Choosing the VFS with the `zVfs` argument of `sqlite3_open_v2` and a plain file name does not turn the engine on: the database opens as stock SQLite, because SQLite also opens the databases of `ATTACH` and `VACUUM INTO` through the VFS and those must stay as they are. Write the name as a URI with the VFS or with `mw=1`: `file:app.db?vfs=multiwriter` or `file:app.db?mw=1` (with `SQLITE_OPEN_URI`).

## Install

Every release has the extension for each platform ([releases](https://github.com/sqliteai/sqlite-multiwriter/releases/latest); `make extension` builds it for the machine you are on). Load it into a SQLite that
allows extensions, then open the database with the VFS:

    .load ./multiwriter                              -- the sqlite3 shell (multiwriter.so, .dylib or .dll)
    SELECT mw_version();                             -- 0.6.2

From C:

    sqlite3_enable_load_extension(db, 1);
    sqlite3_load_extension(db, "./multiwriter", "sqlite3_multiwriter_init", &err);

`sqlite3_multiwriter_default_init` as the entry point also makes it the default VFS. The extension calls SQLite through the routines of the host (SQLite 3.14 or later). To build the engine and SQLite into one
library, run `make`: the VFS then registers itself when SQLite starts and the URI needs no `vfs=`.

## Multi-threading

Open every connection with the same URI and retry on `SQLITE_BUSY_SNAPSHOT`:

    #define URI "file:app.db?vfs=multiwriter&mw_rebase=1"

    /* load the extension once (see Install), then in each thread: */
    sqlite3 *db;
    sqlite3_open_v2(URI, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);

    for (;;) {
        sqlite3_exec(db, "BEGIN", 0, 0, 0);
        sqlite3_exec(db, "UPDATE account SET balance = balance + 10 WHERE id = 7", 0, 0, 0);
        int rc = sqlite3_exec(db, "COMMIT", 0, 0, 0);
        if (rc == SQLITE_OK) break;
        sqlite3_exec(db, "ROLLBACK", 0, 0, 0);
        if (rc != SQLITE_BUSY_SNAPSHOT && rc != SQLITE_BUSY) break;       /* a real error */
        /* otherwise run the whole transaction again */
    }

Options (URI parameters, read when the database is opened):

| Parameter | Meaning |
|---|---|
| `mw` | `1`: the engine, and the default when the URI has `vfs=multiwriter`; `0`: off for that database; any other value fails the open |
| `mw_rebase=1` | replay a commit that lost only on shared pages instead of refusing it |
| `mw_profile=small` | smaller caches (8 MB of pages, 16 MB of log before compaction), for a phone or a small server |
| `mw_log_max_mb` | size of the log before it is compacted into the database |
| `mw_fullfsync=1` | `F_FULLFSYNC` for the log and the compaction on macOS |
| `mw_readcheck=0` | do not validate the pages a transaction read: fewer retries, but write skew becomes possible |

The complete list is in `docs/design.md`.

### Against stock SQLite, threads

Apple M5 Pro (18 cores, 64 GB, SSD, APFS), SQLite 3.53.4 in WAL mode, `synchronous=FULL`, 8-second runs on a fresh database. "Retries" is how many times, per 100 committed transactions, the application had to
run a transaction again after `SQLITE_BUSY`; the engine refuses a commit that conflicts and the application runs it again, up to 1000 times. Best result of each row in bold. More scenarios and the rows written:
`docs/benchmarks.md`. To repeat the runs: `make bench`, then `VARIANTS=mw,mwr,sqlite WORKLOAD=bulk python3 bench/compare_sqlite.py threads out.jsonl 8 1 4 16 64` (`WORKLOAD` is `bulk`, `groups` or `hot`).

**Each thread inserts its own rows** (100 rows per transaction)

| Threads | SQLite tx/s | retries | multiwriter tx/s | retries | with rebase tx/s | retries |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 13,746 | **0** | **15,959** | **0** | 15,687 | **0** |
| 4 | 9,307 | 128 | **33,258** | **0.0** | 32,898 | **0.0** |
| 16 | 8,630 | 189 | **49,277** | **0.2** | 48,275 | **0.2** |
| 64 | 8,128 | 398 | **45,906** | 1.1 | 45,689 | **1.0** |

**Each thread updates its own row, rows share pages** (one row per transaction)

| Threads | SQLite tx/s | retries | multiwriter tx/s | retries | with rebase tx/s | retries |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 13,401 | **0** | 34,911 | **0** | **35,885** | **0** |
| 4 | 13,632 | 4.6 | **49,173** | 0.6 | 36,628 | **0** |
| 16 | 13,562 | 35 | 37,789 * | 8.2 | **55,098** | **0** |
| 64 | 13,623 | 132 | **40,012** | 64 | 39,500 | **0** |

\* a few transactions gave up after 1000 retries.

**Every thread updates the same 4 rows** (`a = a + 1`): a real conflict, which no engine can merge

| Threads | SQLite tx/s | retries | multiwriter tx/s | retries | with rebase tx/s | retries |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 13,770 | **0** | **37,196** | **0** | 32,939 | **0** |
| 4 | 14,248 | 6.7 | **36,895** | **2.5** | 27,225 | 7.6 |
| 16 | 13,277 | 24 | **29,976** | **17** | 28,014 | 34 |
| 64 | 14,057 | 101 | **32,186** | **81** | 26,782 | 142 |

**Commit latency** (milliseconds, each thread inserting its own rows): time from the start of a transaction to its commit, retries included

| Threads | SQLite p50 | p99 | p99.9 | multiwriter p50 | p99 | p99.9 |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 0.07 | 0.09 | 1.26 | **0.06** | **0.08** | **0.13** |
| 4 | 0.21 | 1.54 | 55.8 | **0.11** | **0.24** | **0.69** |
| 16 | **0.23** | 53.9 | 157 | 0.28 | **1.15** | **2.08** |
| 64 | **0.26** | 146 | 271 | 1.18 | **3.97** | **31.4** |

SQLite's median can be lower with many writers: a writer that finds the database busy fails at once and the application retries, and the cost shows in the 99.9th percentile. When writers fight over the
same rows the gain is about 2x and the rebase does not help.

## Multi-process

Use it when several agents write to the same database. Every process opens the same file with `mw_mp=1`; the code is the one above with a different URI:

    #define URI "file:agents.db?vfs=multiwriter&mw_mp=1&mw_rebase=1"

The processes share one index of page versions and a log (`agents.db-mw*` files next to the database). A process that is killed does not block the others: its unfinished transaction is discarded and its
committed ones are kept. The last process to close leaves a plain SQLite file. Databases must be on a local file system.

### Against stock SQLite, processes

Same machine and settings as above; every writer is a separate process (`WORKLOAD=bulk python3 bench/compare_sqlite.py procs out.jsonl 8 1 4 16`).

**Each process inserts its own rows** (100 rows per transaction)

| Processes | SQLite tx/s | retries | multiwriter tx/s | retries | with rebase tx/s | retries |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 13,570 | **0** | **15,005** | **0** | 14,560 | **0** |
| 4 | 9,176 | 125 | **28,489** | **0.0** | 27,918 | **0.0** |
| 16 | 8,636 | 189 | 25,987 | **0.1** | **26,744** | **0.1** |

**Each process updates its own row, rows share pages** (one row per transaction)

| Processes | SQLite tx/s | retries | multiwriter tx/s | retries | with rebase tx/s | retries |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 13,392 | **0** | **37,130** | **0** | 36,376 | **0** |
| 4 | 13,865 | 5.2 | 49,527 | 20 | **49,854** | **0** |
| 16 | 13,225 | 30 | 50,389 | 102 | **51,472** | **0** |

**Every process updates the same 4 rows** (`a = a + 1`)

| Processes | SQLite tx/s | retries | multiwriter tx/s | retries | with rebase tx/s | retries |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 13,136 | **0** | 39,550 | **0** | **39,592** | **0** |
| 4 | 13,385 | **4.4** | 43,638 | 68 | **43,940** | 49 |
| 16 | 13,818 | **24** | **46,352** | 110 | 46,327 | 100 |

**Commit latency** (milliseconds, each process inserting its own rows)

| Processes | SQLite p50 | p99 | p99.9 | multiwriter p50 | p99 | p99.9 |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 0.07 | **0.09** | 1.27 | **0.06** | 0.21 | **0.28** |
| 4 | 0.22 | 1.69 | 69.3 | **0.15** | **0.30** | **0.41** |
| 16 | **0.23** | 77.9 | 233 | 0.60 | **0.95** | **1.54** |

The rebase is what removes the retries of the middle table: the processes never change the same row, only the same pages.

## Platforms

Archives and packages are on the [current release](https://github.com/sqliteai/sqlite-multiwriter/releases/latest).

| Platform | File of the release | Notes |
|---|---|---|
| Linux glibc, x86_64 and arm64 | `multiwriter-linux-<arch>-<version>.tar.gz` | threads and processes |
| Linux musl (Alpine), x86_64 and arm64 | `multiwriter-linux-musl-<arch>-...` | threads and processes |
| macOS, x86_64 and arm64 | `multiwriter-macos-<version>` (universal), `multiwriter-macos-x86_64-...`, `multiwriter-macos-arm64-...` | threads and processes |
| iOS, iOS simulator, Mac Catalyst | `multiwriter-ios-...`, `-ios-sim-...`, `-mac-catalyst-...` | threads |
| Apple XCFramework | `multiwriter-apple-xcframework-<version>.zip`, and `Package.swift` (Swift Package Manager) | iOS, simulator, Catalyst, macOS |
| Android arm64-v8a, armeabi-v7a, x86_64, x86 | `multiwriter-android-<abi>-...`, and `multiwriter-android-aar-<version>.aar` | API 26 or later, 16 KB pages |
| Windows x86_64 | `multiwriter-windows-x86_64-<version>.zip` | threads and processes; Windows 10 1709 or later (`docs/windows.md`) |

Build one yourself: `make extension [PLATFORM=macos|ios|ios-sim|mac-catalyst|android|linux|linux-musl|windows] [ARCH=...]`, `make xcframework`, `make aar`, `make package`
(`mk/extension.mk`, `mk/package.mk`). The version is `MW_VERSION` in `src/multiwriter.h`; a push to `main` builds and tests every platform and, if that version has no release yet, publishes it
(`.github/workflows/main.yml`).

## Build and test

    git clone https://github.com/sqliteai/sqlite-multiwriter && cd sqlite-multiwriter
    make test           # the test suite (about 50 programs, a few minutes)
    make test-mp        # the transaction tests with processes (mw_mp=1)
    make test-io        # errors of the file system and a full disk (minutes)
    make bench          # dist/mw_bench;  bench/compare_sqlite.py: against stock SQLite
    test/sanitize.sh asan|ubsan|tsan [tests]

SQLite 3.53.4 is vendored in `third_party/sqlite`; nothing else is needed. Design, guarantees and limits: `docs/design.md`. This project does not synchronise databases; for that see [sqlite-sync](https://github.com/sqliteai/sqlite-sync).

## Limits

What to know before relying on it (details and measurements in `docs/design.md`).

**Database and platform**
- WAL only: `journal_mode` other than WAL, `locking_mode=EXCLUSIVE` and `auto_vacuum` other than none are not supported. `PRAGMA page_size` on a new database is ignored. Not on a network file system.
- The database file is only usable through the engine while it is open (the log `<db>-mw` and its segments hold commits that are not yet in the file); `<db>-mw*` files are part of the database. They are versioned: another version of the format is refused, never read wrongly.
- A VFS stacked above this one must forward the shared-memory methods (`xShm*`); one that keeps its own shared memory makes the commit fail with `SQLITE_IOERR`.
- `PRAGMA data_version` changes with every transaction (also of the connection itself).

**Isolation and retries**
- Snapshot isolation with first-committer-wins on pages, plus validation of the pages read, which refuses the classic write skew (two transactions that each read what the other writes). It is still possible in three cases: `mw_readcheck=0` (the validation is off), the reads of page 1 (the header of the file: not validated), and transactions on attached databases (not atomic across the files). Serializability is tested, not proved. To protect an invariant that a transaction only reads, make both transactions write a shared row with a real change (`UPDATE guard SET n = n + 1`): they then conflict whatever they read. The application must retry a transaction that fails with `SQLITE_BUSY_SNAPSHOT` (the whole transaction).
- A schema change waits up to 2 s for the write transactions of the other connections to end, then goes ahead; a transaction that overlapped it fails on the schema cookie with `SQLITE_BUSY_SNAPSHOT` when it tries to write, and is retried (as a stock WAL reader that tries to write after a commit). A change of the temporary schema does not wait.
- Throughput of one database is bounded by the publication of a commit (about 40 us in the processes mode) and by true conflicts: many writers on the same row serialise, and the rebase does not help them.

**The rebase (`mw_rebase=1`, opt in)** replays a commit that lost only on pages it shares with others, row by row, instead of refusing it. It is never wrong, but it often does not apply, and then the commit is refused and retried as without it:
- the transaction read rows (`SELECT`, `WITH`, `VALUES`) or ran a statement that is not a *point statement*: an `INSERT ... VALUES` or an `UPDATE`/`DELETE` of one row by its rowid or a unique index, with no subquery, no scan, no join; an `UPDATE`/`DELETE` that found no row counts as a read. So `UPDATE t SET n = n + 1 WHERE id = ?` is replayed, a `SELECT` followed by an `UPDATE` is not;
- DDL, a trigger or a virtual table in the database, a foreign key of a table to itself or a circle of them, a key that other tables refer to being changed, a WITHOUT ROWID table with a key that is not BINARY or is descending, `AUTOINCREMENT` (every insert writes the same row of `sqlite_sequence`), a database that is not UTF-8, a commit that is not the first of its snapshot;
- an application that installs its own `sqlite3_trace_v2` on the connection replaces the statement hook of the engine: the rebase then never applies on that connection (the engine detects it);
- a transaction whose counted row changes (`sqlite3_total_changes`) are not exactly the row changes found in the pages is not replayed: that is how a no-op `UPDATE` (it sets the value a row already has: counted, no page written) is found, and also a row changed twice in the transaction, `REPLACE`, a savepoint rolled back.

**Resources**
- A large transaction needs two to three times its size in memory. A reader that never ends holds back the garbage collection of old page versions (no timeout, no warning yet). `sqlite3_backup_step` with small steps only finishes on a connection that does not write.
- The index of versions in the shared mode has a fixed size (`MW_IDX_ENTRIES`); when compaction cannot keep up a commit fails with `SQLITE_FULL`.
- Interior pages of an index (and so of a WITHOUT ROWID table) that were rewritten make the transactions that read through them retry: the shortcut that spares that retry for table b-trees is not safe for indexes.

**Verification** (`make test`, `make test-mp`, `make test-io`, `test/sanitize.sh`, `test/hunt.sh`; SQLite's own Tcl suite through the VFS in `docs/sqlite-test-suite.md`): a randomised serializability check with threads and killed processes, a power-loss test on ext4 (Docker only), thousands of runs of the hunt, not days. Not verified: other file systems, TSan with processes, a machine that loses power on hardware.

## Changelog

What changed in each version is in [CHANGELOG.md](CHANGELOG.md).

## License

Apache License 2.0: see `LICENSE` and `NOTICE`. The vendored SQLite (`third_party/sqlite`) is in the public domain.
