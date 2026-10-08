# sqlite-multiwriter

Multi-writer SQLite as a wrapper VFS: many connections (threads and processes) write one database at the same time. Each transaction runs on a snapshot and is validated at commit; a commit that lost is refused
(`SQLITE_BUSY_SNAPSHOT`, run it again) or, with `mw_rebase=1`, replayed at the latest state when it lost only because it shares pages with a commit that changed other rows. Nothing in SQLite is modified, no SQL syntax is
added, and every commit is ACID (the durability of the WAL with `synchronous=FULL`: a commit log with group commit, compacted into the ordinary database file, which stays a plain SQLite database).

The project builds and tests on its own: SQLite 3.53.4 is vendored in `third_party/sqlite`. It does not synchronise databases (for that use [sqlite-sync](https://github.com/sqliteai/sqlite-sync) as an extension of its own).

    #include "multiwriter.h"        /* the VFS registers itself when SQLite initialises (SQLITE_EXTRA_INIT) */

    sqlite3_open_v2("file:app.db?mw=2", &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);                /* threads of one process        */
    sqlite3_open_v2("file:app.db?mw=2&mw_mp=1", ...);                                                                          /* processes (the shared mode)   */
    sqlite3_open_v2("file:app.db?mw=2&mw_rebase=1", ...);                                                                      /* ... with the rebase           */

The application retries a transaction that fails with `SQLITE_BUSY_SNAPSHOT` (the whole transaction, not the last statement). Isolation: snapshot isolation with first-committer-wins and validation of the pages read; not serializable
(write skew is possible: `docs/design.md`, "What is guaranteed").

Status: macOS and Linux (arm64 tested). `docs/design.md` is the design, its guarantees and its limits; `docs/history-crdt-design.md` and `docs/engine-history.md` are history (an earlier version captured the CRDT metadata of sqlite-sync: removed).

    git clone <url> && cd sqlite-multiwriter
    make test           # the test suite (about 40 programs, a few minutes)
    make test-mp        # the transaction tests with processes (mw_mp=1)
    make test-io        # errors of the file system and a full disk (minutes)
    make bench          # dist/mw_bench;  bench/compare_sqlite.py: against stock SQLite
    test/sanitize.sh asan|ubsan|tsan [tests]
    # durability against a loss of power (Docker, privileged; see test/power/run.sh)
    docker build -t mw-power test/power && docker run --rm --privileged -v "$PWD":/src mw-power bash /src/test/power/run.sh

## Limits

What to know before relying on it (details and measurements in `docs/design.md`).

**Database and platform**
- WAL only: `journal_mode` other than WAL, `locking_mode=EXCLUSIVE` and `auto_vacuum` other than none are not supported. `PRAGMA page_size` on a new database is ignored. macOS and Linux; not on a network file system.
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
- not covered: a no-op `UPDATE` (it sets the value a row already has) and a statement still open (`RETURNING` stepped halfway) at commit.

**Resources**
- A large transaction needs two to three times its size in memory. A reader that never ends holds back the garbage collection of old page versions (no timeout, no warning yet). `sqlite3_backup_step` with small steps only finishes on a connection that does not write.
- The index of versions in the shared mode has a fixed size (`MW_IDX_ENTRIES`); when compaction cannot keep up a commit fails with `SQLITE_FULL`.
- Interior pages of an index (and so of a WITHOUT ROWID table) that were rewritten make the transactions that read through them retry: the shortcut that spares that retry for table b-trees is not safe for indexes.

**Verification** (`make test`, `make test-mp`, `make test-io`, `test/sanitize.sh`, `test/hunt.sh`; SQLite's own Tcl suite through the VFS in `docs/sqlite-test-suite.md`): a randomised serializability check with threads and killed processes, a power-loss test on ext4 (Docker only), thousands of runs of the hunt, not days. Not verified: other file systems, TSan with processes, a machine that loses power on hardware.

## License

Apache License 2.0: see `LICENSE` and `NOTICE`. The vendored SQLite (`third_party/sqlite`) is in the public domain.
