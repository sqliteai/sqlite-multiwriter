# sqlite-multiwriter

Multi-writer SQLite as a wrapper VFS: many connections (threads and processes) write to one database at the same time, each transaction is validated at
commit, nothing in SQLite is modified, no SQL syntax is added, and every write is ACID (the same durability as SQLite's WAL with `synchronous=FULL`).
CRDT metadata (per-row causal length, per-cell versions) is captured in the VFS for every table, so a database is always ready to be synchronised
with the algorithms of [sqlite-sync](https://github.com/sqliteai/sqlite-sync) (`deps/sqlite-sync`, a submodule: its CRDT algorithms and wire
format are the reference, its SQL-level API is not used).

Status: macOS first. See `docs/design.md` (what this is and the plan) and `docs/engine-history.md` (the measurements and decisions that led here,
written while this was a branch of sqlite-sync).

    git clone --recurse-submodules <url> && cd sqlite-multiwriter
    make test        # the test suite
    make bench       # dist/mw_bench
