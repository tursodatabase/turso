# 2021 May 24
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

set testdir [file dirname $argv0]
source $testdir/tester.tcl
source $testdir/malloc_common.tcl
set testprefix memjournal


do_execsql_test 1.0 {
  PRAGMA journal_mode = memory;
  CREATE TABLE t1(a);
} {memory}

set nRow [expr 1]

do_execsql_test 1.1 {
  BEGIN;
    INSERT INTO t1 VALUES( randomblob(500) );
} {}

do_test 1.2 {
  for {set i 1} {$i <= 500} {incr i} {
    execsql {
      SAVEPOINT one;
      UPDATE t1 SET a=randomblob(500);
    }
    execsql { SAVEPOINT abc } 
    execsql { UPDATE t1 SET a=randomblob(500) WHERE rowid<=$i AND 0 }
    execsql { RELEASE abc }
  } 
} {}

do_execsql_test 1.3 {
  COMMIT;
}

finish_test
