#!/bin/bash
# Materialized views over the scale factor 1 TPC-H database.
#
# Creates views for TPC-H Q3 (three-way join with GROUP BY) and Q19 (join
# with an OR of filters), writes single rows to every joined table, and
# checks after each step that each view equals a fresh run of its query,
# in the same database and after reopening it. Every step has a time limit,
# so a join that reads a whole table per changed row fails here instead of
# passing slowly.
#
# Usage: perf/tpc-h/matview.sh
#   TURSODB=path/to/tursodb      binary to test (default: release build)
#   CREATE_LIMIT, WRITE_LIMIT,   time limits in seconds per step
#   CHECK_LIMIT

set -euo pipefail

REPO_ROOT=$(git rev-parse --show-toplevel)
TPCH_DIR="$REPO_ROOT/perf/tpc-h"
DB_FILE="$TPCH_DIR/TPC-H.db"
DB_URL="https://github.com/lovasoa/TPCH-sqlite/releases/download/v1.0/TPC-H.db"
CREATE_LIMIT=${CREATE_LIMIT:-300}
WRITE_LIMIT=${WRITE_LIMIT:-10}
CHECK_LIMIT=${CHECK_LIMIT:-300}

if [ -z "${TURSODB:-}" ]; then
    TURSODB="$("$REPO_ROOT/scripts/cargo-target-dir")/release/tursodb"
    if [ ! -f "$TURSODB" ]; then
        cargo build --bin tursodb --release
    fi
fi

if [ ! -f "$DB_FILE" ]; then
    echo "Downloading TPC-H database..."
    curl -sL --fail -o "$DB_FILE" "$DB_URL"
fi

WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT
DB="$WORK_DIR/tpch.db"
cp "$DB_FILE" "$DB"

Q3="SELECT l_orderkey, sum(l_extendedprice * (1 - l_discount)) AS revenue, o_orderdate, o_shippriority
FROM customer JOIN orders ON c_custkey = o_custkey JOIN lineitem ON l_orderkey = o_orderkey
WHERE c_mktsegment = 'FURNITURE' AND o_orderdate < '1995-03-29' AND l_shipdate > '1995-03-29'
GROUP BY l_orderkey, o_orderdate, o_shippriority"

Q19="SELECT sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM lineitem JOIN part ON l_partkey = p_partkey
WHERE (p_brand = 'Brand#22' AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
       AND l_quantity >= 8 AND l_quantity <= 18 AND p_size BETWEEN 1 AND 5
       AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON')
   OR (p_brand = 'Brand#23' AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
       AND l_quantity >= 10 AND l_quantity <= 20 AND p_size BETWEEN 1 AND 10
       AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON')
   OR (p_brand = 'Brand#12' AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
       AND l_quantity >= 24 AND l_quantity <= 34 AND p_size BETWEEN 1 AND 15
       AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON')"

# Rows of the view or the query that have no equal row on the other side.
# Revenue is a sum of REAL values that the view adds in a different order
# than the query, so it is compared with a small relative tolerance.
Q3_MISMATCHES="SELECT count(*) FROM (
  SELECT 1 FROM q3 v LEFT JOIN ($Q3) q
    ON v.l_orderkey = q.l_orderkey AND v.o_orderdate = q.o_orderdate AND v.o_shippriority = q.o_shippriority
    AND abs(v.revenue - q.revenue) <= 1e-9 * abs(q.revenue) + 1e-6
  WHERE q.l_orderkey IS NULL
  UNION ALL
  SELECT 1 FROM ($Q3) q LEFT JOIN q3 v
    ON v.l_orderkey = q.l_orderkey AND v.o_orderdate = q.o_orderdate AND v.o_shippriority = q.o_shippriority
    AND abs(v.revenue - q.revenue) <= 1e-9 * abs(q.revenue) + 1e-6
  WHERE v.l_orderkey IS NULL
)"
Q19_MISMATCHES="SELECT count(*) FROM q19 v, ($Q19) q
  WHERE NOT (abs(v.revenue - q.revenue) <= 1e-9 * abs(q.revenue) + 1e-6)"

# Run SQL with a time limit; fail on errors, a timeout, or a crash.
run() {
    local limit=$1 label=$2 sql=$3
    local start end output
    start=$(date +%s.%N)
    if ! output=$(timeout "$limit" "$TURSODB" -q -m list --experimental-views "$DB" "$sql" 2>&1); then
        echo "FAIL $label (exit or timeout after ${limit}s): $output"
        exit 1
    fi
    if grep -q -i "error\|panicked" <<<"$output"; then
        echo "FAIL $label: $output"
        exit 1
    fi
    end=$(date +%s.%N)
    printf "%-45s %8.2fs\n" "$label" "$(echo "$end - $start" | bc)"
    LAST_OUTPUT=$output
}

check_views() {
    run "$CHECK_LIMIT" "check views ($1)" "$Q3_MISMATCHES; $Q19_MISMATCHES; SELECT count(*) FROM q3;"
    local q3_bad q19_bad q3_rows
    { read -r q3_bad; read -r q19_bad; read -r q3_rows; } <<<"$LAST_OUTPUT"
    if [ "$q3_bad" != 0 ] || [ "$q19_bad" != 0 ] || [ "$q3_rows" = 0 ]; then
        echo "FAIL views differ from their queries ($1): q3 mismatches=$q3_bad rows=$q3_rows, q19 mismatches=$q19_bad"
        exit 1
    fi
}

run "$CREATE_LIMIT" "create Q3 view" "CREATE MATERIALIZED VIEW q3 AS $Q3"
run "$CREATE_LIMIT" "create Q19 view" "CREATE MATERIALIZED VIEW q19 AS $Q19"
check_views "after create"

FURNITURE_CUSTOMER="(SELECT c_custkey FROM customer WHERE c_mktsegment = 'FURNITURE' ORDER BY c_custkey LIMIT 1)"
run "$WRITE_LIMIT" "insert order" "INSERT INTO orders VALUES (
  6000001, $FURNITURE_CUSTOMER, 'O', 100, '1995-03-20', '1-URGENT', 'Clerk#1', 0, 'matview check')"
run "$WRITE_LIMIT" "insert lineitem into new order" "INSERT INTO lineitem VALUES (
  6000001, 1, 1, 1, 5, 1000, 0.1, 0, 'N', 'O', '1995-04-01', '1995-04-01', '1995-04-01',
  'DELIVER IN PERSON', 'AIR', 'matview check')"
run "$WRITE_LIMIT" "update lineitem of a view row" "UPDATE lineitem SET l_discount = l_discount + 0.01
  WHERE l_orderkey = (SELECT l_orderkey FROM q3 ORDER BY l_orderkey LIMIT 1) AND l_linenumber = 1"
run "$WRITE_LIMIT" "delete lineitem of a view row" "DELETE FROM lineitem
  WHERE l_orderkey = (SELECT l_orderkey FROM q3 ORDER BY l_orderkey DESC LIMIT 1)"
run "$WRITE_LIMIT" "insert part matching Q19" "INSERT INTO part VALUES (
  300001, 'matview part', 'Manufacturer#1', 'Brand#22', 'STANDARD', 3, 'SM CASE', 900, 'matview check')"
run "$WRITE_LIMIT" "insert lineitem for that part" "INSERT INTO lineitem VALUES (
  6000001, 300001, 1, 2, 10, 2000, 0.05, 0, 'N', 'O', '1995-04-01', '1995-04-01', '1995-04-01',
  'DELIVER IN PERSON', 'AIR REG', 'matview check')"
run "$WRITE_LIMIT" "update a joined part" "UPDATE part SET p_brand = 'Brand#22', p_container = 'SM BOX', p_size = 2
  WHERE p_partkey = (SELECT min(l_partkey) FROM lineitem WHERE l_shipmode = 'AIR'
                     AND l_shipinstruct = 'DELIVER IN PERSON' AND l_quantity BETWEEN 8 AND 18)"
run "$WRITE_LIMIT" "update a customer's segment" "UPDATE customer SET c_mktsegment = 'FURNITURE'
  WHERE c_custkey = (SELECT c_custkey FROM customer WHERE c_mktsegment = 'BUILDING' ORDER BY c_custkey LIMIT 1)"
run "$WRITE_LIMIT" "multi-statement transaction" "BEGIN;
  DELETE FROM orders WHERE o_orderkey = 6000001;
  DELETE FROM lineitem WHERE l_orderkey = 6000001;
  COMMIT"
check_views "after writes, reopened"

run "$CHECK_LIMIT" "integrity check" "PRAGMA quick_check"
if [ "$LAST_OUTPUT" != ok ]; then
    echo "FAIL integrity check: $LAST_OUTPUT"
    exit 1
fi
echo "All materialized view checks passed"
