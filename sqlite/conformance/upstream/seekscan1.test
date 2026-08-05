# 2022 October 7
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
# This file implements regression tests for SQLite library.
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix seekscan1

do_execsql_test 1.0 {
  CREATE TABLE t1(a TEXT, b INT, c INT NOT NULL, PRIMARY KEY(a,b,c));
  WITH RECURSIVE c(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM c WHERE x<1997)
    INSERT INTO t1(a,b,c) SELECT printf('xyz%d',x/10),x/6,x FROM c;
  INSERT INTO t1 VALUES('abc',234,6);
  INSERT INTO t1 VALUES('abc',345,7);
  ANALYZE;
}


do_execsql_test 1.1 {
  SELECT a,b,c FROM t1 
  WHERE b IN (234, 345) AND c BETWEEN 6 AND 6.5 AND a='abc' 
  ORDER BY a, b;
} {
  abc 234 6
}

do_execsql_test 1.2 {
  SELECT a,b,c FROM t1 
  WHERE b IN (234, 345) AND c BETWEEN 6 AND 7 AND a='abc' 
  ORDER BY a, b;
} {
  abc 234 6
  abc 345 7
}

do_execsql_test 1.3 {
  SELECT a,b,c FROM t1 
  WHERE b IN (234, 345) AND c >=6 AND a='abc' 
  ORDER BY a, b;
} {
  abc 234 6
  abc 345 7
}

do_execsql_test 1.4 {
  SELECT a,b,c FROM t1 
  WHERE b IN (234, 345) AND c<=7 AND a='abc' 
  ORDER BY a, b;
} {
  abc 234 6
  abc 345 7
}

do_execsql_test 1.5 {
  SELECT a,b,c FROM t1 WHERE b IN (235, 345) AND c<=3 AND a='abc' ORDER BY a, b;
}


finish_test
