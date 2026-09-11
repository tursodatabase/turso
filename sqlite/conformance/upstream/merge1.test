# 2021-12-29
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
#
# Testing the compound-SELECT merge algorithm to ensure that it works
# when it tries to balance the merge tree.

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix merge1

ifcapable !vtab {
  finish_test
  return
}

load_static_extension db series


optimization_control db all on
do_execsql_test 100 {
  WITH data(v) AS (
    SELECT value FROM generate_series(1,35,3)
    UNION ALL
    SELECT value FROM generate_series(10,30,4)
    UNION ALL
    SELECT value FROM generate_series(20,50,5)
    UNION ALL
    SELECT value FROM generate_series(30,60,6)
    UNION ALL
    SELECT value FROM generate_series(1,50,7)
    UNION ALL
    SELECT value FROM generate_series(10,80,8)
  )
  SELECT v FROM data ORDER BY v;
} {1 1 4 7 8 10 10 10 13 14 15 16 18 18 19 20 22 22 22 25 25 26 26 28 29 30 30 30 31 34 34 35 36 36 40 42 42 43 45 48 50 50 50 54 58 60 66 74}
do_eqp_test 101 {
  WITH data(v) AS (
    SELECT value FROM generate_series(1,35,3)
    UNION ALL
    SELECT value FROM generate_series(10,30,4)
    UNION ALL
    SELECT value FROM generate_series(20,50,5)
    UNION ALL
    SELECT value FROM generate_series(30,60,6)
    UNION ALL
    SELECT value FROM generate_series(1,50,7)
    UNION ALL
    SELECT value FROM generate_series(10,80,8)
  )
  SELECT v FROM data ORDER BY v;
} {
  QUERY PLAN
  `--MERGE (UNION ALL)
     |--LEFT
     |  `--MERGE (UNION ALL)
     |     |--LEFT
     |     |  `--MERGE (UNION ALL)
     |     |     |--LEFT
     |     |     |  `--SCAN generate_series VIRTUAL TABLE INDEX 0x17:
     |     |     `--RIGHT
     |     |        `--SCAN generate_series VIRTUAL TABLE INDEX 0x17:
     |     `--RIGHT
     |        `--SCAN generate_series VIRTUAL TABLE INDEX 0x17:
     `--RIGHT
        `--MERGE (UNION ALL)
           |--LEFT
           |  `--MERGE (UNION ALL)
           |     |--LEFT
           |     |  `--SCAN generate_series VIRTUAL TABLE INDEX 0x17:
           |     `--RIGHT
           |        `--SCAN generate_series VIRTUAL TABLE INDEX 0x17:
           `--RIGHT
              `--SCAN generate_series VIRTUAL TABLE INDEX 0x17:
}

# Same test with the blanced-merge optimization
# disabled.  Should give the exact same answer.
#
optimization_control db balanced-merge off
db cache flush
do_execsql_test 110 {
  WITH data(v) AS (
    SELECT value FROM generate_series(1,35,3)
    UNION ALL
    SELECT value FROM generate_series(10,30,4)
    UNION ALL
    SELECT value FROM generate_series(20,50,5)
    UNION ALL
    SELECT value FROM generate_series(30,60,6)
    UNION ALL
    SELECT value FROM generate_series(1,50,7)
    UNION ALL
    SELECT value FROM generate_series(10,80,8)
  )
  SELECT v FROM data ORDER BY v;
} {1 1 4 7 8 10 10 10 13 14 15 16 18 18 19 20 22 22 22 25 25 26 26 28 29 30 30 30 31 34 34 35 36 36 40 42 42 43 45 48 50 50 50 54 58 60 66 74}
do_eqp_test 111 {
  WITH data(v) AS (
    SELECT value FROM generate_series(1,35,3)
    UNION ALL
    SELECT value FROM generate_series(10,30,4)
    UNION ALL
    SELECT value FROM generate_series(20,50,5)
    UNION ALL
    SELECT value FROM generate_series(30,60,6)
    UNION ALL
    SELECT value FROM generate_series(1,50,7)
    UNION ALL
    SELECT value FROM generate_series(10,80,8)
  )
  SELECT v FROM data ORDER BY v;
} {
  QUERY PLAN
  `--MERGE (UNION ALL)
     |--LEFT
     |  `--MERGE (UNION ALL)
     |     |--LEFT
     |     |  `--MERGE (UNION ALL)
     |     |     |--LEFT
     |     |     |  `--MERGE (UNION ALL)
     |     |     |     |--LEFT
     |     |     |     |  `--MERGE (UNION ALL)
     |     |     |     |     |--LEFT
     |     |     |     |     |  `--SCAN generate_series VIRTUAL TABLE INDEX 0x17:
     |     |     |     |     `--RIGHT
     |     |     |     |        `--SCAN generate_series VIRTUAL TABLE INDEX 0x17:
     |     |     |     `--RIGHT
     |     |     |        `--SCAN generate_series VIRTUAL TABLE INDEX 0x17:
     |     |     `--RIGHT
     |     |        `--SCAN generate_series VIRTUAL TABLE INDEX 0x17:
     |     `--RIGHT
     |        `--SCAN generate_series VIRTUAL TABLE INDEX 0x17:
     `--RIGHT
        `--SCAN generate_series VIRTUAL TABLE INDEX 0x17:
}

finish_test
