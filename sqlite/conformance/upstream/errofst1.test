# 2024-11-20
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
# Test cases for sqlite3_error_offset()
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl

do_execsql_test errofst1-1.1 {
  CREATE TABLE t1 as select 1 as aa;
  CREATE VIEW t2 AS
     WITH t3 AS (SELECT 1 FROM t1 AS bb, t1 AS cc WHERE cc.aa <= sts.aa)
     SELECT 1 FROM t3 AS dd;
}
do_catchsql_test errofst1-1.2 {
  SELECT * FROM t2;
} {1 {no such column: sts.aa}}
do_test errofst1-1.3 {
  sqlite3_error_offset db
} {-1}

finish_test
