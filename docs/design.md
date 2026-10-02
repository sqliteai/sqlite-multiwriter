# sqlite-multiwriter: design and plan

## What it is

A wrapper VFS that lets many connections (threads and processes) write one SQLite database concurrently (page-level optimistic concurrency, a commit log with group
commit, versioned pages, compaction into the database file: docs/engine-history.md has the measurements behind each choice), and that captures, for every table, the
row-level changes of every commit so that the database always carries the state a CRDT (the algorithms of sqlite-sync) needs. No SQLite source is changed, no SQL syntax
is added, triggers and hooks are not used (they do not exist for WITHOUT ROWID tables, the pre-update hook needs a compile option, and an application can replace
the update hook of its connection).

sqlite-sync (`deps/sqlite-sync`, a submodule) is the reference for the algorithms and the wire format, and it is the oracle of the differential tests. Its SQL-level
API (database.h, the metadata tables built by triggers, the `cloudsync_*` functions) is not used.

## Layers

1. **Engine** (`src/multiwriter_*.c`): lanes (one per connection), page store, commit log (staged ring in one process, segmented log shared by processes), shared version
   index, compaction, relocation of pages after concurrent growth, admission control, recovery after a crash. Unchanged by what follows except for one addition: a commit
   record can carry a *metadata extension* (below).
2. **Capture** (`multiwriter_rowdiff.c`, `multiwriter_cdc.c`): from the pages a transaction wrote and the same pages at its snapshot, the net row changes of every table
   b-tree: inserts, updates (with the columns that changed), deletes. The table of a page comes from the owner map (page -> root page), kept up to date from the commits.
   The schema (columns, primary key, WITHOUT ROWID, INTEGER PRIMARY KEY alias) comes from `sqlite_schema` through a helper connection, refreshed when the schema cookie moves.
3. **CRDT core** (`src/crdt/`): the algorithms of sqlite-sync as pure functions over a small state interface: causal length per row (odd = alive), column version
   per cell (odd = alive, +2 per local update, +1 when it is even), db_version and seq per change, site id; local change generation from the captured row changes; merge of
   a remote change (causal length first, then column version, then the value, then the site id); export of "changes since db_version"; primary-key and payload encoding.
4. **Metadata store**: where the state lives. A shared memory table (per cell: version, db_version, site, seq) in front of an ordinary table inside the database file
   (`mw_cells`), written in batches by a flusher in ordinary, logged transactions. The database file is therefore self-contained: copy it and the CRDT state comes along.
5. **Sync API** (`src/sync/`): site id, db_version, export a payload since a version, apply a payload atomically, the commit hook for "this commit was made by a merge".

## Atomicity and durability of the metadata (the rule that keeps it ACID)

The metadata of a commit is a function of the commit's record (its pages, and for a merge its declared cell versions) and of the state before it. So it is never
logged separately: a commit is durable when its record is in the log, and its metadata is recomputed from the record wherever it is missing.
- Applying a commit puts its cells in the shared memory table *before the commit becomes visible* (a transaction that sees it finds its versions).
- The flusher writes the cells to `mw_cells` in one transaction together with the row `meta_epoch = F` of `mw_state`: F is the visible epoch at the moment the table
  was switched, so every commit <= F is in the batch. The batch and F are atomic (one commit).
- Recovery (the first process to open, after a crash): the pages come back from the log as today; the metadata of every commit with epoch > meta_epoch is recomputed by replaying
  its record through capture and the CRDT core in epoch order (the old images are the versions in the page store). Replay is idempotent: a cell whose stored db_version is
  >= the epoch being replayed is skipped (the flush batch may contain cells of commits above F).
- Compaction may not move the base above meta_epoch (the old images of the records to replay would be gone): a compaction that needs it asks for a flush first.
- A commit made by a merge (remote changes) carries its cell versions in the record extension: replay must not take its row changes for local edits.

## Multi-process

The owner map, the schema catalog, the memory table and its change feed are in shared memory (one writer at a time under the publication lock; lock-free readers; the flusher
elected like the compactor). Every process applies the commits of the others to what it keeps privately (nothing in the shared design needs that: the shared tables are the state).

## What is covered, what is not

Covered by design: rowid tables with and without INTEGER PRIMARY KEY, tables with a TEXT/composite/BLOB primary key, WITHOUT ROWID tables, tables without a primary key (tracked
locally by rowid, not exported: no stable identity), overflow values, DDL (the capture stops at a schema change and re-reads the catalog), VACUUM (a barrier), savepoints, rollbacks,
triggers and foreign-key actions (they are rows like any other), attached databases (not supported), virtual tables (their shadow tables are tables; the virtual table itself is not tracked).
Not covered in v1: block-level (text) merging, the DWS/AWS algorithms, filters/RLS, the network layer.

## Milestones

M0 repository, standalone build, engine and its suite (done). M1 capture for every table kind (catalog, WITHOUT ROWID, overflow, DDL) with randomized differential tests.
M2 CRDT core + differential tests against sqlite-sync. M3 metadata store, flusher, recovery; crash tests. M4 sync API (export/apply), two-peer convergence tests incl. concurrent
writers. M5 multi-process. M6 benchmarks against the previous version (tracked and untracked), memory.
