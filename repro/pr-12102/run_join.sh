#!/usr/bin/env bash
# Times an MVCC equality join on a non-unique index. All rows are checkpointed.
#   SELECT count(*) FROM a JOIN b ON a.k = b.k
#   200,000 x 200,000 rows, 2 matches for each key, plan: SEARCH b USING COVERING INDEX b_k (k=?)
# The variants run in alternation. Set JOIN_RUNS to change the number of runs (default 15).
set -euo pipefail
source "$(dirname "$0")/common.sh"
require_binaries tursodb
D=$OUT/join
mkdir -p "$D"
JOIN_RUNS=${JOIN_RUNS:-15}

rm -f "$D"/join.db "$D"/join.db-log "$D"/join.db-wal
"$BIN/tursodb_base" -m list "$D/join.db" < "$KIT/sql/setup_join.sql" > "$D/setup.out" 2>&1
echo "EXPLAIN QUERY PLAN SELECT count(*) FROM a JOIN b ON a.k = b.k;" \
  | "$BIN/tursodb_pr" -m list "$D/join.db" 2>&1 | sed 's/^/plan: /'

for i in $(seq 1 "$JOIN_RUNS"); do
  if [ $((i % 2)) -eq 1 ]; then order="base pr"; else order="pr base"; fi
  for v in $order; do
    out=$("$BIN/tursodb_$v" -m list "$D/join.db" < "$KIT/sql/join_query.sql" 2>&1)
    echo "$v $(echo "$out" | grep -E '^[0-9]+$') $(echo "$out" | timer_seconds)"
  done
done > "$D/join_times.out"

for v in base pr; do
  grep "^$v " "$D/join_times.out" | awk '{print $2, $3}' | sort -k2 -n | awk -v v=$v '
    { rows[NR] = $1; t[NR] = $2 }
    END {
      m = (NR % 2) ? t[(NR + 1) / 2] : (t[NR / 2] + t[NR / 2 + 1]) / 2
      printf "%s: runs=%d median=%.0f ms fastest=%.0f ms result=%s\n", v, NR, m * 1000, t[1] * 1000, rows[1]
    }'
done | tee "$D/join_summary.out"
