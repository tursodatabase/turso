# 2023 January 28
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

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix quickcheck

do_execsql_test 1.0 {
  CREATE TABLE t1(
    a INTEGER NOT NULL, b INTEGER NOT NULL, c AS (a+1),
    PRIMARY KEY(b, a)
  ) WITHOUT ROWID;

  INSERT INTO t1 VALUES(1, 2);
  INSERT INTO t1 VALUES(3, 4);
}

do_execsql_test 1.1 {
  PRAGMA quick_check
} {
  ok
}

finish_test
