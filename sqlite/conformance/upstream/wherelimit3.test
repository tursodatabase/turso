# 2024-06-06
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
# Test cases for query plans using LIMIT
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix wherelimit3

do_execsql_test 1.0 {
  CREATE TABLE t1(a INT, b INT);
  WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<1000)
    INSERT INTO t1 SELECT n, n FROM c;
  CREATE INDEX t1a ON t1(a);
  CREATE INDEX t1b ON t1(b);
  ANALYZE;
}

do_eqp_test 1.1 {
  SELECT * FROM t1 WHERE a>=100 AND a<300 ORDER BY b LIMIT 5;
} {
  QUERY PLAN
  |--SEARCH t1 USING INDEX t1a (a>? AND a<?)
  `--USE TEMP B-TREE FOR ORDER BY
}
ifcapable stat4 {
  do_eqp_test 1.2 {
    SELECT * FROM t1 WHERE a>=100 AND a<300 ORDER BY b LIMIT -1;
  } {
    QUERY PLAN
    `--SCAN t1 USING INDEX t1b
  }
}

set N [expr 5]
do_eqp_test 1.3 {
  SELECT * FROM t1 WHERE a>=100 AND a<300 ORDER BY b LIMIT $::N;
} {
  QUERY PLAN
  |--SEARCH t1 USING INDEX t1a (a>? AND a<?)
  `--USE TEMP B-TREE FOR ORDER BY
}

ifcapable stat4 {
  set N [expr -1]
  do_eqp_test 1.4 {
    SELECT * FROM t1 WHERE a>=100 AND a<300 ORDER BY b LIMIT $::N;
  } {
    QUERY PLAN
    `--SCAN t1 USING INDEX t1b
  }
}





finish_test
