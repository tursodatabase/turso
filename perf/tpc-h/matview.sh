#!/bin/bash
# Materialized views over the scale factor 1 TPC-H database.
#
# Creates views for TPC-H Q3 (three-way join with GROUP BY), Q19 (join with
# an OR of filters), Q5 (six tables) and Q8 (eight tables), writes single rows
# to the joined tables, and checks that each view equals a fresh run of its
# query, after creation and after the writes in a reopened database. Every
# step has a time limit, so a join that reads a whole table per changed row
# fails here instead of passing slowly.
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

Q5="SELECT n_name, sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM customer, orders, lineitem, supplier, nation, region
WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey
  AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
  AND r_name = 'ASIA' AND o_orderdate >= '1994-01-01' AND o_orderdate < '1995-01-01'
GROUP BY n_name"

Q8="SELECT substr(o_orderdate, 1, 4) AS o_year,
  sum(CASE WHEN n2.n_name = 'BRAZIL' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) AS brazil,
  sum(l_extendedprice * (1 - l_discount)) AS volume
FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region
WHERE p_partkey = l_partkey AND s_suppkey = l_suppkey AND l_orderkey = o_orderkey
  AND o_custkey = c_custkey AND c_nationkey = n1.n_nationkey AND n1.n_regionkey = r_regionkey
  AND r_name = 'AMERICA' AND s_nationkey = n2.n_nationkey
  AND o_orderdate BETWEEN '1995-01-01' AND '1996-12-31' AND p_type = 'ECONOMY ANODIZED STEEL'
GROUP BY o_year"

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
Q5_MISMATCHES="SELECT count(*) FROM (
  SELECT 1 FROM q5 v LEFT JOIN ($Q5) q
    ON v.n_name = q.n_name AND abs(v.revenue - q.revenue) <= 1e-9 * abs(q.revenue) + 1e-6
  WHERE q.n_name IS NULL
  UNION ALL
  SELECT 1 FROM ($Q5) q LEFT JOIN q5 v
    ON v.n_name = q.n_name AND abs(v.revenue - q.revenue) <= 1e-9 * abs(q.revenue) + 1e-6
  WHERE v.n_name IS NULL
)"
Q8_MISMATCHES="SELECT count(*) FROM (
  SELECT 1 FROM q8 v LEFT JOIN ($Q8) q
    ON v.o_year = q.o_year AND abs(v.brazil - q.brazil) <= 1e-9 * abs(q.brazil) + 1e-6
    AND abs(v.volume - q.volume) <= 1e-9 * abs(q.volume) + 1e-6
  WHERE q.o_year IS NULL
  UNION ALL
  SELECT 1 FROM ($Q8) q LEFT JOIN q8 v
    ON v.o_year = q.o_year AND abs(v.brazil - q.brazil) <= 1e-9 * abs(q.brazil) + 1e-6
    AND abs(v.volume - q.volume) <= 1e-9 * abs(q.volume) + 1e-6
  WHERE v.o_year IS NULL
)"

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
    run "$CHECK_LIMIT" "check views ($1)" "$Q3_MISMATCHES; $Q19_MISMATCHES; $Q5_MISMATCHES; $Q8_MISMATCHES;
      SELECT count(*) FROM q3; SELECT count(*) FROM q5; SELECT count(*) FROM q8;"
    local q3_bad q19_bad q5_bad q8_bad q3_rows q5_rows q8_rows
    { read -r q3_bad; read -r q19_bad; read -r q5_bad; read -r q8_bad;
      read -r q3_rows; read -r q5_rows; read -r q8_rows; } <<<"$LAST_OUTPUT"
    if [ "$q3_bad" != 0 ] || [ "$q19_bad" != 0 ] || [ "$q5_bad" != 0 ] || [ "$q8_bad" != 0 ] ||
        [ "$q3_rows" = 0 ] || [ "$q5_rows" = 0 ] || [ "$q8_rows" = 0 ]; then
        echo "FAIL views differ from their queries ($1): mismatches q3=$q3_bad q19=$q19_bad" \
            "q5=$q5_bad q8=$q8_bad, rows q3=$q3_rows q5=$q5_rows q8=$q8_rows"
        exit 1
    fi
}

run "$CREATE_LIMIT" "create Q3 view (3 tables)" "CREATE MATERIALIZED VIEW q3 AS $Q3"
run "$CREATE_LIMIT" "create Q19 view (2 tables)" "CREATE MATERIALIZED VIEW q19 AS $Q19"
run "$CREATE_LIMIT" "create Q5 view (6 tables)" "CREATE MATERIALIZED VIEW q5 AS $Q5"
run "$CREATE_LIMIT" "create Q8 view (8 tables)" "CREATE MATERIALIZED VIEW q8 AS $Q8"
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
run "$WRITE_LIMIT" "move a supplier to another nation" "UPDATE supplier
  SET s_nationkey = (s_nationkey + 1) % 25 WHERE s_suppkey = 1"
run "$WRITE_LIMIT" "move a customer to another nation" "UPDATE customer
  SET c_nationkey = (c_nationkey + 1) % 25 WHERE c_custkey = 1"
run "$WRITE_LIMIT" "rename a nation" "UPDATE nation SET n_name = 'BRAZIL' WHERE n_nationkey = 1"
run "$WRITE_LIMIT" "insert a Q8 part and lineitem" "BEGIN;
  INSERT INTO part VALUES (300002, 'matview part 2', 'Manufacturer#1', 'Brand#13',
    'ECONOMY ANODIZED STEEL', 4, 'LG BOX', 900, 'matview check');
  INSERT INTO lineitem VALUES (
    (SELECT o_orderkey FROM orders WHERE o_orderdate = '1995-06-17' ORDER BY o_orderkey LIMIT 1),
    300002, 2, 9, 3, 3000, 0.02, 0, 'N', 'O', '1995-07-01', '1995-07-01', '1995-07-01',
    'NONE', 'MAIL', 'matview check');
  COMMIT"
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
