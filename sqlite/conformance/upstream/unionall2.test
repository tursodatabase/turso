# 2020-12-22
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
# This file implements regression tests for SQLite library.  The
# focus of this file is flattening UNION ALL sub-queries.
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix unionall2

do_execsql_test 1.0 {
  CREATE TABLE t1(a, b);
  CREATE TABLE t2(c, d);

  CREATE VIEW v1 AS SELECT * FROM t1, t2;
  CREATE VIEW v2 AS SELECT * FROM t1, t2;

  CREATE VIEW vA AS
    SELECT * FROM v1, (
      SELECT * FROM t1 LEFT JOIN t2 ON (a=c)
    )
    UNION ALL
    SELECT * FROM v1, v2
}

do_execsql_test 1.1 {
  SELECT 1 FROM vA, vA, vA, vA, vA, vA, vA, vA, vA, vA
}

#-------------------------------------------------------------------------

do_execsql_test 2.1 {
  CREATE TABLE y1(a INTEGER, b);
  CREATE TABLE y2(c INTEGER, d);

  CREATE TABLE x3_a(a INTEGER PRIMARY KEY, b TEXT);
  CREATE TABLE x3_b(c INTEGER PRIMARY KEY, d TEXT);
}

do_execsql_test 2.2 {

  SELECT * FROM y1 CROSS JOIN y2 WHERE y1.a=y2.c AND y2.c IN (
    SELECT a FROM x3_a UNION ALL
    SELECT c FROM x3_b ORDER BY 1
  )
}



finish_test
