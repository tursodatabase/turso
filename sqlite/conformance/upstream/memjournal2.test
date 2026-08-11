# 2022 Jan 01
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
# Tests focused on the in-memory journal.
#
# TESTRUNNER: slow

set testdir [file dirname $argv0]
source $testdir/tester.tcl
source $testdir/malloc_common.tcl
set testprefix memjournal2

do_execsql_test 1.0 {
  PRAGMA journal_mode = memory;
  CREATE TABLE t1(a INTEGER PRIMARY KEY, b UNIQUE);
} {memory}

set nRow [expr 2000]

do_execsql_test 1.1 {
  BEGIN;
    WITH s(i) AS (
      SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i<$nRow
    )
    INSERT INTO t1 SELECT NULL, randomblob(700) FROM s;
}

for {set jj 200} {$jj <= 300} {incr jj} {
  do_execsql_test 1.2.$jj.1 {
    SAVEPOINT one; 
      UPDATE t1 SET b=randomblob(700) WHERE a<=$jj;
  }
  do_execsql_test 1.2.$jj.2 {
      SAVEPOINT two;
        UPDATE t1 SET b=randomblob(700) WHERE a==1;
      ROLLBACK TO two;
      RELEASE two;
  }
  do_execsql_test 1.2.$jj.3 {
      SAVEPOINT two;
        UPDATE t1 SET b=randomblob(700) WHERE a==1;
      ROLLBACK TO two;
      RELEASE two;
  }

  do_execsql_test 1.2.$jj.4 {
    PRAGMA integrity_check;
    ROLLBACK TO one;
    RELEASE one;
  } {ok}
} 


finish_test
