# Format of the files and compatibility

What the engine writes next to a database, which of it must survive a process, and what a version of the library does with a file that another version made.

## The files

| file | holds | survives a process | versioned |
|---|---|---|---|
| `<db>` | the database, in WAL mode (the header says so), written by the compactor | yes | it is SQLite's own format |
| `<db>-mw` | the commit log of the single-process mode: a header of 64 bytes, then one record per commit (page images, checksummed) | yes: the commits that are not in `<db>` yet | magic `MWLOG002`, `version`, `features` |
| `<db>-mw.N` | the segments of the log of the multi-process mode (shared mode), the same header and records | yes | magic `MWLOG002`, `version`, `features` |
| `<db>-mwlock`, `-mwlk` (in `/dev/shm` on Linux) | the shared header of the processes that have the database open: lock of the publication, tickets, epochs, slots of snapshots | no: it is rebuilt by the first process | magic `MWMP1001` |
| `<db>-mwidx` and the other `-mw*` maps | the index of versions of the shared mode | no: it is rebuilt from the segments by the first process | its own magic and version |

Only the log and the segments matter for the compatibility between versions: they hold commits that exist nowhere else. The files of the other rows are volatile, they live only while at least one process has the database open.

## The header of the log and of a segment (64 bytes)

`magic[8]` (`MWLOG002`), `version` (u32), `pgsz` (u32), `base_epoch` (u64), `salt` (u64), `features` (u64), 16 reserved bytes, `cksum` (u64, FNV over the header with `cksum` = 0). `MWLOG002` is the family of the format (it changes only if the whole layout does); `version` is `MW_FORMAT_VERSION` of the library that made the file (1 today).

## What a library does with a file

Rules, checked when the database is opened (`mw_format_check`):

1. a header whose magic is `MWLOG` followed by something else: another family. The open fails with `SQLITE_CANTOPEN` and a message (`sqlite3_log`, level warning) saying so.
2. the right magic, a good checksum and another `version`: the open fails with `SQLITE_CANTOPEN` and a message with both numbers. Older and newer are the same case: a library reads the version that it was built for (and the next version that is released will say how to read the previous one, or will refuse it, in its own notes).
3. the right magic, a good checksum and a bit of `features` that the library does not know: refused the same way. A bit is a change that an old library would misread; it is how a version adds something to a record or to the header without a new `version` for every addition.
4. a header whose checksum does not check out: a damaged file (or one being written when a process died): a log with records after it is `SQLITE_CORRUPT`; an empty one is started again.

In every refusal the file is **left exactly as it was**: it is not replaced by a new log, not truncated, not deleted when the process that failed to open it closes (`mw_shared_close` does not remove the segments of an open that did not complete), and not taken for damaged and dropped. The commits that it holds are in it for the version that can read them. Test `mw_format`.

The shared header is checked against a process that runs another version of the engine and has the database open: its magic is `MWMP` followed by another number, and the new process is refused (`SQLITE_CANTOPEN`, a message) instead of waiting for a header that will never be `ready` for it.

## Compatibility policy

- Version 1 is the first one that is released. The files that the development builds wrote carry the same `version` 1 and open with it (the layout did not change).
- A change of the layout of the log or of a segment is a new `MW_FORMAT_VERSION`, or a feature bit when an old library cannot misread it silently. It comes with the way to open a database of the previous version (a log is replayed and folded into the database file by the new library, and the old files are then not needed): a database whose process closed cleanly has no log at all, so the usual upgrade is "close the database with the old library, open it with the new one". A migration of a log that a crash left is done by the *old* library (open, close), then the new one; the new library does not read a log of another version.
- The database file itself is a normal SQLite database in WAL mode: any SQLite can read it when no log is pending (a clean close leaves none).
