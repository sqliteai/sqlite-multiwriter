#!/bin/zsh
# The comparison asked for: from a clean database every time, stock SQLite (WAL, synchronous=FULL; busy_timeout 60 s: "sqlite", busy_timeout 0 with the application retrying: "sqlite0")
# against the multi-writer engine with the CRDT capture on ("mw") and off ("mw0"); threads up to 64, processes up to 1000. Bulk insert, 100 rows per transaction.
#   bench/run_compare_all.sh [duration_threads] [duration_procs]       (from the repository root, after `make bench`)
cd "$(dirname "$0")/.." || exit 1
DT=${1:-8}; DP=${2:-6}; D=$(date +%Y-%m-%d)
python3 bench/compare_sqlite.py threads bench/results/new_compare_threads_$D.jsonl $DT 1 2 4 8 16 32 64
python3 bench/compare_sqlite.py procs bench/results/new_compare_procs_$D.jsonl $DP 1 2 4 8 16 32 64 128 256 512 1000
python3 bench/summarize_compare.py bench/results/new_compare_threads_$D.jsonl threads > bench/results/new_compare_threads_$D.md
python3 bench/summarize_compare.py bench/results/new_compare_procs_$D.jsonl procs > bench/results/new_compare_procs_$D.md
