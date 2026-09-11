# 2024-03-06
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
# Invariant tests for JSON built around the randomjson extension
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix json108

# These tests require virtual table "json_tree" to run.
ifcapable !vtab { finish_test ; return }

load_static_extension db randomjson
db eval {
  CREATE TEMP TABLE t1(j0,j5);
  WITH RECURSIVE c(n) AS (VALUES(0) UNION ALL SELECT n+1 FROM c WHERE n<9)
  INSERT INTO t1 SELECT random_json(n), random_json5(n) FROM c;
}

do_execsql_test 1.1 {
  SELECT count(*) FROM t1 WHERE json(j0)==json(json_pretty(j0,NULL));
} 10
do_execsql_test 1.2 {
  SELECT count(*) FROM t1 WHERE json(j0)==json(json_pretty(j0,NULL));
} 10
do_execsql_test 1.3 {
  SELECT count(*) FROM t1 WHERE json(j0)==json(json_pretty(j0,''));
} 10
do_execsql_test 1.4 {
  SELECT count(*) FROM t1 WHERE json(j0)==json(json_pretty(j0,char(9)));
} 10
do_execsql_test 1.5 {
  SELECT count(*) FROM t1 WHERE json(j0)==json(json_pretty(j0,'/*hello*/'));
} 10


finish_test
