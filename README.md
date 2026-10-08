# sqlite-multiwriter

SQLite is a remarkable piece of software, and it has one well-known limit: a database accepts a single writer at a time. With many threads, or many processes (a group of agents working on the same
database, for example), the writers queue up behind one lock, or fail with `SQLITE_BUSY` and have to try again.

sqlite-multiwriter removes that limit without touching SQLite. It is a VFS that you load as an extension (or link in): your SQLite, your SQL and your database file stay the same, and many connections
write the same database at once. Every commit is durable, and the file remains an ordinary SQLite database.

- **More writes per second.** Writers that touch different rows (or different pages) do not wait for each other. In the tables below: 2 to 5 times the transactions per second of stock SQLite with 4 to 16 writers.
- **Almost no `SQLITE_BUSY`.** With writers on their own rows, the application sees a retry in 0.2 of 100 transactions, where stock SQLite needs 2 retries per transaction at 16 writers.
- **Threads or processes.** One process with many connections, or many processes on one file.
- **Nothing to change in the schema or in the SQL.** No triggers, no new syntax, no special tables.

## How it works

Each transaction runs on a snapshot of the database. At commit, its pages are checked against the commits published in the meantime. If nobody changed what it wrote or read, it commits: writers do not wait for
each other. If another commit got there first on the same page, the transaction is refused with `SQLITE_BUSY_SNAPSHOT` and the application runs it again (first committer wins). Commits go through a log with group commit
(the durability of WAL with `synchronous=FULL`) and are compacted into the database file in the background.

**The rebase** (`mw_rebase=1`, optional). Two writers that change different rows can still land on the same page, and that is the usual cause of a refused commit. With the rebase the engine does not refuse the
loser: it takes the row changes of its transaction and applies them again on top of the latest state, then commits. The application sees no error. A transaction that really conflicts (the same row changed by both)
is still refused. The rebase is limited to simple transactions: `INSERT`s and `UPDATE`/`DELETE`s of a row by its key. See Limits.

Isolation is snapshot isolation, not serializable: write skew is possible (`docs/design.md`). The application must retry a transaction that fails with `SQLITE_BUSY_SNAPSHOT`, the whole transaction.

## Two ways to use it

| | Threads (`mw=2`) | Processes (`mw=2&mw_mp=1`) |
|---|---|---|
| Who writes | many connections in one process | many processes (agents, workers, a CLI and a server) on the same file |
| Shared state | memory of the process | shared memory and a log in segments next to the database |
| Platforms | all | all (little use in an iOS app: one process) |

Add `mw_rebase=1` to either.

## Install

Every release has the extension for each platform ([releases](https://github.com/sqliteai/sqlite-multiwriter/releases/latest); `make extension` builds it for the machine you are on). Load it into a SQLite that
allows extensions, then open the database with the VFS:

    .load ./multiwriter                              -- the sqlite3 shell (multiwriter.so, .dylib or .dll)
    SELECT mw_version();                             -- 0.5.0

From C:

    sqlite3_enable_load_extension(db, 1);
    sqlite3_load_extension(db, "./multiwriter", "sqlite3_multiwriter_init", &err);

`sqlite3_multiwriter_default_init` as the entry point also makes it the default VFS. The extension calls SQLite through the routines of the host (SQLite 3.14 or later). To build the engine and SQLite into one
library, run `make`: the VFS then registers itself when SQLite starts and the URI needs no `vfs=`.

## Multi-threading

Open every connection with the same URI and retry on `SQLITE_BUSY_SNAPSHOT`:

    #define URI "file:app.db?vfs=multiwriter&mw=2&mw_rebase=1"

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
| `mw=2` | use the engine (0: off) |
| `mw_rebase=1` | replay a commit that lost only on shared pages instead of refusing it |
| `mw_profile=small` | smaller caches (8 MB of pages, 16 MB of log before compaction), for a phone or a small server |
| `mw_log_max_mb` | size of the log before it is compacted into the database |
| `mw_fullfsync=1` | `F_FULLFSYNC` for the log and the compaction on macOS |

The complete list is in `docs/design.md`.

### Against stock SQLite, threads

WAL, `synchronous=FULL` (every commit is on disk when it returns), 8-second runs, an 18-core Mac, SQLite 3.53.4. "Retries" is the number of times the application had to run a transaction again after `SQLITE_BUSY`,
per 100 committed transactions. Stock SQLite: one writer at a time. Full tables, with more scenarios and the rows written: `docs/benchmarks.md`.

**Each thread inserts its own rows** (100 rows per transaction)

| Threads | SQLite tx/s | retries | multiwriter tx/s | retries | with rebase tx/s | retries |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 13,099 | 0 | 14,877 | 0 | 14,780 | 0 |
| 4 | 9,049 | 130 | 30,984 | 0.0 | 31,094 | 0.0 |
| 16 | 8,539 | 197 | 46,449 | 0.2 | 46,113 | 0.1 |
| 64 | 7,890 | 430 | 40,336 | 1.0 | 40,770 | 0.7 |

**Each thread updates its own row, rows share pages** (one row per transaction)

| Threads | SQLite tx/s | retries | multiwriter tx/s | retries | with rebase tx/s | retries |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 17,683 | 0 | 33,010 | 0 | 33,167 | 0 |
| 4 | 11,128 | 5.6 | 39,510 | 0.7 | 29,104 | 0 |
| 16 | 10,376 | 32 | 32,977 * | 8.2 | 51,061 | 0 |
| 64 | 10,884 | 134 | 39,871 | 65 | 45,244 | 0 |

\* a few transactions gave up after 1000 retries.

**Every thread updates the same 4 rows** (`a = a + 1`): a real conflict, which no engine can merge

| Threads | SQLite tx/s | retries | multiwriter tx/s | retries | with rebase tx/s | retries |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 13,624 | 0 | 29,799 | 0 | 33,795 | 0 |
| 4 | 13,642 | 5.1 | 29,247 | 3.9 | 20,524 | 11 |
| 16 | 14,684 | 24 | 26,849 | 21 | 28,201 | 39 |
| 64 | 13,812 | 109 | 28,126 | 101 | 22,484 | 156 |

When writers fight over the same rows, the gain is about 2x and the retries are those of SQLite; the rebase does not help there.

## Multi-process (agents on one database)

Every process opens the same file with `mw_mp=1`; the code is the one above with a different URI:

    #define URI "file:agents.db?vfs=multiwriter&mw=2&mw_mp=1&mw_rebase=1"

The processes share one index of page versions and a log (`agents.db-mw*` files next to the database). A process that is killed does not block the others: its unfinished transaction is discarded and its
committed ones are kept. The last process to close leaves a plain SQLite file. Databases must be on a local file system.

### Against stock SQLite, processes

Same machine and settings as above; every writer is a separate process.

**Each process inserts its own rows** (100 rows per transaction)

| Processes | SQLite tx/s | retries | multiwriter tx/s | retries | with rebase tx/s | retries |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 13,122 | 0 | 14,167 | 0 | 14,208 | 0 |
| 4 | 9,051 | 126 | 27,626 | 0.0 | 28,180 | 0.0 |
| 16 | 8,419 | 180 | 24,257 | 0.2 | 24,454 | 0.2 |

**Each process updates its own row, rows share pages** (one row per transaction)

| Processes | SQLite tx/s | retries | multiwriter tx/s | retries | with rebase tx/s | retries |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 13,976 | 0 | 30,848 | 0 | 30,752 | 0 |
| 4 | 13,738 | 5.7 | 45,304 | 20 | 46,301 | 0 |
| 16 | 12,722 | 28 | 43,487 | 106 | 47,184 | 0 |

**Every process updates the same 4 rows** (`a = a + 1`)

| Processes | SQLite tx/s | retries | multiwriter tx/s | retries | with rebase tx/s | retries |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 14,233 | 0 | 31,715 | 0 | 32,707 | 0 |
| 4 | 14,062 | 5.0 | 40,909 | 67 | 41,709 | 48 |
| 16 | 12,716 | 26 | 41,619 | 119 | 43,381 | 108 |

The rebase is what removes the retries of the middle table: the processes never change the same row, only the same pages.

## Platforms

Archives and packages are on the [current release](https://github.com/sqliteai/sqlite-multiwriter/releases/latest).

| Platform | File of the release | Notes |
|---|---|---|
| Linux glibc, x86_64 and arm64 | `multiwriter-linux-<arch>-<version>.tar.gz` | threads and processes |
| Linux musl (Alpine), x86_64 and arm64 | `multiwriter-linux-musl-<arch>-...` | threads and processes |
| macOS, x86_64 and arm64 | `multiwriter-macos-universal-...`, and each alone | threads and processes |
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

SQLite 3.53.4 is vendored in `third_party/sqlite`; nothing else is needed. Design, guarantees and limits: `docs/design.md`. The files `docs/history-crdt-design.md` and `docs/engine-history.md`
are history of earlier versions. This project does not synchronise databases; for that see [sqlite-sync](https://github.com/sqliteai/sqlite-sync).

## Limits

What to know before relying on it (details and measurements in `docs/design.md`).

**Database and platform**
- WAL only: `journal_mode` other than WAL, `locking_mode=EXCLUSIVE` and `auto_vacuum` other than none are not supported. `PRAGMA page_size` on a new database is ignored. Not on a network file system.
- The database file is only usable through the engine while it is open (the log `<db>-mw` and its segments hold commits that are not yet in the file); `<db>-mw*` files are part of the database. They are versioned (`docs/format.md`): another version of the format is refused, never read wrongly.
- A VFS stacked above this one must forward the shared-memory methods (`xShm*`); one that keeps its own shared memory makes the commit fail with `SQLITE_IOERR`.
- `PRAGMA data_version` changes with every transaction (also of the connection itself).

**Isolation and retries**
- Snapshot isolation with first-committer-wins on pages, plus validation of the pages read. Not serializable in general. The application must retry a transaction that fails with `SQLITE_BUSY_SNAPSHOT` (the whole transaction).
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

## License

Apache License 2.0: see `LICENSE` and `NOTICE`. The vendored SQLite (`third_party/sqlite`) is in the public domain.
