#!/usr/bin/env bash
# Runs 15 plain SQL queries and writes on a mixed state (rows in the B-tree and rows
# only in MVCC) with both variants. The output of both variants must be the same,
# and it must be the same as results/mixed_sql_expected.out. I examined each row
# of that file by hand.
set -euo pipefail
source "$(dirname "$0")/common.sh"
require_binaries tursodb
D=$OUT/mixed_sql
mkdir -p "$D"

for v in base pr; do
  rm -f "$D/m_$v.db" "$D/m_$v.db-log" "$D/m_$v.db-wal"
  "$BIN/tursodb_$v" -m list "$D/m_$v.db" < "$KIT/sql/mixed.sql" > "$D/mixed.$v.out" 2>&1
done
status=0
for v in base pr; do
  if diff -u "$KIT/results/mixed_sql_expected.out" "$D/mixed.$v.out"; then
    echo "$v: same as results/mixed_sql_expected.out"
  else
    echo "$v: DIFFERENT from results/mixed_sql_expected.out"
    status=1
  fi
done
exit $status
