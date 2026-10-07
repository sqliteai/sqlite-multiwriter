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

## License

Apache License 2.0: see `LICENSE` and `NOTICE`. The vendored SQLite (`third_party/sqlite`) is in the public domain.
