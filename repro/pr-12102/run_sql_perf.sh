#!/usr/bin/env bash
# SQL-only reproductions with tursodb, without the sync engine. Both use the table
# of the pull benchmark: 50,000 checkpointed rows, 1,400-byte payloads, five indexes.
#
# Part 1: one transaction deletes N rows, then inserts them again. This is the
# order of pull replay. Prints the time of the DELETE, the INSERT, the
# integrity check after the commit, and the checkpoint.
#
# Part 2: 2,000 equality lookups on a non-unique index, after a committed
# transaction deleted and inserted again 4,000 rows, before a checkpoint.
#
# Set SQL_SIZES to change the values of N, for example SQL_SIZES="1000 2000".
set -euo pipefail
source "$(dirname "$0")/common.sh"
require_binaries tursodb
D=$OUT/sql_perf
mkdir -p "$D"
SQL_SIZES=${SQL_SIZES:-1000 2000 4000 8000}

rm -f "$D"/items.db "$D"/items.db-log "$D"/items.db-wal
"$BIN/tursodb_base" -m list "$D/items.db" < "$KIT/sql/setup_items.sql" > "$D/setup.out" 2>&1

echo "Part 1: delete N rows, then insert them again, in one transaction"
echo "n,variant,delete_s,insert_s,integrity_check_s,checkpoint_s,count|sum(version)|sum(c0),integrity_check"
for n in $SQL_SIZES; do
  sed "s/@N@/$n/g" "$KIT/sql/delete_insert.sql.in" > "$D/delete_insert_$n.sql"
  for v in base pr; do
    copy_db "$D/items.db" "$D/t.db"
    out=$("$BIN/tursodb_$v" -m list "$D/t.db" < "$D/delete_insert_$n.sql" 2>&1)
    echo "$out" > "$D/delete_insert_$n.$v.out"
    # Timed statements: BEGIN, DELETE, INSERT, COMMIT, SELECT, integrity_check, checkpoint.
    t=($(echo "$out" | timer_seconds || true))
    result=$(echo "$out" | grep -E '^[0-9]+\|[0-9]+\|[0-9]+$' | head -1 || true)
    check=$(echo "$out" | grep -E '^ok$' | head -1 || true)
    echo "$n,$v,${t[1]},${t[2]},${t[5]},${t[6]},$result,${check:-NOT OK}"
  done
done | tee "$D/delete_insert.csv"

echo
echo "Part 2: 2,000 lookups on a non-unique index among hidden entries"
{
  cat "$KIT/sql/lookups_prefix.sql.in"
  for i in $(seq 1 2 4000); do echo "SELECT count(*) FROM items WHERE c2 = $((i + 2));"; done
} > "$D/lookups.sql"
for v in base pr; do
  copy_db "$D/items.db" "$D/t.db"
  out=$("$BIN/tursodb_$v" -m list "$D/t.db" < "$D/lookups.sql" 2>&1)
  wrong=$(echo "$out" | grep -E '^[0-9]+$' | grep -vc '^1$' || true)
  echo "$out" | timer_seconds | awk -v v=$v -v wrong="$wrong" '{s += $1; n++} END {
    printf "%s: %d lookups, total %.3f s, mean %.2f ms, results other than 1: %d\n", v, n, s, 1000 * s / n, wrong
  }'
done | tee "$D/lookups.out"
