# sqlite-multiwriter

Multi-writer SQLite as a wrapper VFS: many connections (threads and processes) write one database at the same time, each transaction is validated at
commit, nothing in SQLite is modified, no SQL syntax is added, and every write is ACID (the durability of SQLite's WAL with `synchronous=FULL`).

The CRDT metadata of [sqlite-sync](https://github.com/sqliteai/sqlite-sync) (a causal length per row, a version per cell) is captured **in the VFS, for every table**,
and travels inside the commit record of the transaction that made it: no triggers, no update hook, no metadata tables written by SQL, no change to the application's
tables. The database is always ready to be synchronised: `mw_sync_export` / `mw_sync_apply` speak sqlite-sync's wire format. sqlite-sync is a submodule
(`deps/sqlite-sync`): its CRDT algorithms and payload container are the reference and the oracle of the differential tests; its SQL-level API is not used.

    #include "multiwriter.h"        /* the VFS registers itself when SQLite initialises (SQLITE_EXTRA_INIT) */
    #include "multiwriter_sync.h"

    sqlite3_open_v2("file:app.db?mw=2&mw_cdc=1", &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);   /* threads of one process */
    sqlite3_open_v2("file:app.db?mw=2&mw_mp=1&mw_cdc=1", ...);                                                              /* processes (shared mode)  */
    ...
    mw_sync_export(db, since, &payload, &len, &upto);      /* every change after db_version `since` */
    mw_sync_apply(db, payload, len, &stats);               /* merge a peer's payload, atomically   */

Status: macOS first (Linux, iOS, Windows, Android next). `docs/design.md` is the design and its limits, `docs/engine-history.md` the measurements and decisions of the engine.

    git clone --recurse-submodules <url> && cd sqlite-multiwriter
    make test           # engine + metadata test suite
    make oracle-test    # differential tests against sqlite-sync itself
    make test-mp        # the metadata tests with processes (mw_mp=1)
    make bench          # dist/mw_bench;  bench/compare_sqlite.py: against stock SQLite
    test/sanitize.sh asan|ubsan|tsan [tests]
