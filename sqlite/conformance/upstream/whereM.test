# 2021 May 27
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
# Tests focused on the "constant propagation" that occurs within the
# WHERE clause of a SELECT statemente.
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
source $testdir/malloc_common.tcl
set testprefix whereM


do_execsql_test 1.0 {
  CREATE TABLE t1(a, b INTEGER, c TEXT, d REAL, e BLOB);
  INSERT INTO t1 VALUES(10.0, 10.0, 10.0, 10.0, 10.0);
  SELECT * FROM t1;
} {
  10.0 10 10.0 10.0 10.0
}

do_execsql_test 1.1.1 {
  SELECT a=10, a = '10.0', a LIKE '10.0' FROM t1;
} {1 0 1}
do_execsql_test 1.1.2 {
  SELECT count(*) FROM t1 WHERE a=10 AND a = '10.0'
} {0}
do_execsql_test 1.1.3 {
  SELECT count(*) FROM t1 WHERE a=10 AND a LIKE '10.0'
} {1}
do_execsql_test 1.1.4 {
  SELECT count(*) FROM t1 WHERE a='10.0' AND a LIKE '10.0'
} {0}

do_execsql_test 1.2.1 {
  SELECT b=10, b = '10.0', b LIKE '10.0', b LIKE '10' FROM t1;
} {1 1 0 1}
do_execsql_test 1.2.2 {
  SELECT count(*) FROM t1 WHERE b=10 AND b = '10.0'
} {1}
do_execsql_test 1.2.3 {
  SELECT count(*) FROM t1 WHERE b=10 AND b LIKE '10.0'
} {0}
do_execsql_test 1.2.4 {
  SELECT count(*) FROM t1 WHERE b='10.0' AND b LIKE '10.0'
} {0}
do_execsql_test 1.2.3 {
  SELECT count(*) FROM t1 WHERE b=10 AND b LIKE '10'
} {1}
do_execsql_test 1.2.4 {
  SELECT count(*) FROM t1 WHERE b='10.0' AND b LIKE '10'
} {1}

do_execsql_test 1.3.1 {
  SELECT c=10, c = 10.0, c = '10.0', c LIKE '10.0' FROM t1;
} {0 1 1 1}
do_execsql_test 1.3.2 {
  SELECT count(*) FROM t1 WHERE c=10 AND c = '10.0'
} {0}
do_execsql_test 1.3.3 {
  SELECT count(*) FROM t1 WHERE c=10 AND c LIKE '10.0'
} {0}
do_execsql_test 1.3.4 {
  SELECT count(*) FROM t1 WHERE c='10.0' AND c LIKE '10.0'
} {1}
do_execsql_test 1.3.5 {
  SELECT count(*) FROM t1 WHERE c=10.0 AND c = '10.0'
} {1}
do_execsql_test 1.3.6 {
  SELECT count(*) FROM t1 WHERE c=10.0 AND c LIKE '10.0'
} {1}

do_execsql_test 1.4.1 {
  SELECT d=10, d = 10.0, d = '10.0', d LIKE '10.0', d LIKE '10' FROM t1;
} {1 1 1 1 0}
do_execsql_test 1.4.2 {
  SELECT count(*) FROM t1 WHERE d=10 AND d = '10.0'
} {1}
do_execsql_test 1.4.3 {
  SELECT count(*) FROM t1 WHERE d=10 AND d LIKE '10.0'
} {1}
do_execsql_test 1.4.4 {
  SELECT count(*) FROM t1 WHERE d='10.0' AND d LIKE '10.0'
} {1}
do_execsql_test 1.4.5 {
  SELECT count(*) FROM t1 WHERE d='10' AND d LIKE '10.0'
} {1}

do_execsql_test 1.5.1 {
  SELECT e=10, e = '10.0', e LIKE '10.0', e LIKE '10' FROM t1;
} {1 0 1 0}
do_execsql_test 1.5.2 {
  SELECT count(*) FROM t1 WHERE e=10 AND e = '10.0'
} {0}
do_execsql_test 1.5.3 {
  SELECT count(*) FROM t1 WHERE e=10 AND e LIKE '10.0'
} {1}
do_execsql_test 1.5.4 {
  SELECT count(*) FROM t1 WHERE e='10.0' AND e LIKE '10.0'
} {0}
do_execsql_test 1.5.5 {
  SELECT count(*) FROM t1 WHERE e=10.0 AND e LIKE '10.0'
} {1}

finish_test
