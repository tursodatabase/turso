# 2016-08-19
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
# This file implements a test for VACUUM on attached databases.
#
# TESTRUNNER: slow

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix vacuum6

# If the VACUUM statement is disabled in the current build, skip all
# the tests in this file.
#
ifcapable !vacuum {
  finish_test
  return
}


do_execsql_test 1.0 {
  CREATE TABLE t1(x INTEGER PRIMARY KEY, y);
  INSERT INTO t1 VALUES(1, 1);
} {}

do_execsql_test 1.1 {
  VACUUM
}

reset_db
do_execsql_test 1.2 {
  CREATE TABLE t1(x,b);
  CREATE INDEX x1 ON t1(x);
  CREATE INDEX x2 ON t1(x);
  CREATE INDEX x3 ON t1(x);
  INSERT INTO t1 SELECT 2,'';
  VACUUM;
}

#-------------------------------------------------------------------------
#
reset_db
foreach {tn sz} {1 400 2 4000 3 9999} {
  reset_db
  do_execsql_test 2.$tn.1 {
    CREATE TABLE t1(a INTEGER PRIMARY KEY, b);
    WITH s(i) AS (
        SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i<100
    )
    INSERT INTO t1 SELECT i, randomblob($sz) FROM s;
  }

  do_execsql_test 2.$tn.2 {
    vacuum;
  }

  do_execsql_test 2.$tn.3 {
    PRAGMA integrity_check;
  } {ok}
}

reset_db
do_execsql_test 3.0 {
  PRAGMA page_size = 1024;
  CREATE TABLE t1(x INTEGER PRIMARY KEY, y);
  INSERT INTO t1 VALUES(2, randomblob(1200));
} {}
do_execsql_test 3.1 {
  PRAGMA page_size = 512;
  VACUUM;
}
do_execsql_test 3.2 {
  PRAGMA integrity_check
} {ok}

#-------------------------------------------------------------------------
#
reset_db
do_execsql_test 4.0 {
  CREATE TABLE tx(a, b);
  CREATE INDEX i1 ON tx(b);
  WITH s(i) AS (
      SELECT 8000 UNION ALL SELECT i+1 FROM s WHERE i<10000
  )
  INSERT INTO tx SELECT i, randomblob(i) FROM s;

  SELECT sum(length(b)) FROM tx;
} {18009000}
foreach {tn pgsz av} {
  1 2048   0
  2 1024   1
  3 65536  0
  4 8192   1
  5 512    0
  6 4096   1
} {
  do_execsql_test 4.1.$tn.1 "
    PRAGMA page_size = $pgsz;
    PRAGMA auto_vacuum = $av;
  "
  do_execsql_test 4.1.$tn.2 VACUUM
  integrity_check 4.1.$tn.3
}

finish_test
