# 2021 May 20
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
# focus of this file is testing VIEW statements.
#
# $Id: view.test,v 1.39 2008/12/14 14:45:21 danielk1977 Exp $
set testdir [file dirname $argv0]
source $testdir/tester.tcl

# Omit this entire file if the library is not configured with views enabled.
ifcapable !view {
  finish_test
  return
}
set testprefix view2

do_execsql_test 1.0 {
  CREATE TABLE t1(x, y);
  INSERT INTO t1 VALUES(1, 2);
  CREATE VIEW v1 AS SELECT * FROM (
    WITH x1 AS (SELECT y, x FROM t1)
    SELECT * FROM x1
  );
}

do_execsql_test 1.1 {
  SELECT * FROM v1
} {2 1}

do_execsql_test 1.2 {
  CREATE VIEW v3 AS SELECT * FROM main.t1;
  WITH t1(a, b) AS ( SELECT 3, 4 ) SELECT * FROM v3;
} {1 2}

breakpoint
do_execsql_test 1.3 {
  CREATE VIEW v2 AS SELECT * FROM t1;
  WITH t1(a, b) AS ( SELECT 3, 4 ) SELECT * FROM v2;
} {1 2}

finish_test
