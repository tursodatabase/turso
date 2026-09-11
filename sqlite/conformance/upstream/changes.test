# 2021 June 22
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
# Tests for the sqlite3_changes() and sqlite3_total_changes() APIs.
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix changes

# To test that the change-counters do not suffer from 32-bit signed integer 
# rollover, add the following line to the array of test cases below. The
# test will take will over an hour to run.
#
#   7 (1<<31)+10 ""
#

foreach {tn nRow wor} {
  1 50 ""
  2 50 "WITHOUT ROWID"

  3 5000 ""
  4 5000 "WITHOUT ROWID"

  5 50000 ""
  6 50000 "WITHOUT ROWID"
} {
  reset_db
  set nBig [expr $nRow]
  
  do_execsql_test 1.$tn.0 "
    PRAGMA journal_mode = off;
    CREATE TABLE t1(x INTEGER PRIMARY KEY) $wor;
  " {off}
  
  do_execsql_test 1.$tn.1 {
    WITH s(i) AS (
      SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i < $nBig
    )
    INSERT INTO t1 SELECT i FROM s;
  }
  
  do_test 1.$tn.2 {
    db changes
  } [expr $nBig]
  
  do_test 1.$tn.3 {
    db total_changes
  } [expr $nBig]
  
  do_execsql_test 1.$tn.4 {
    INSERT INTO t1 VALUES(-1)
  }
  
  do_test 1.$tn.5 {
    db changes
  } [expr 1]
  
  do_test 1.$tn.6 {
    db total_changes
  } [expr {$nBig+1}]
  
  do_execsql_test 1.$tn.7a {
    SELECT count(*) FROM t1
  } [expr {$nBig+1}]
  
  do_execsql_test 1.$tn.7 {
    DELETE FROM t1
  }
  
  do_test 1.$tn.8 {
    db changes
  } [expr {$nBig+1}]
  
  do_test 1.$tn.9 {
    db total_changes
  } [expr {2*($nBig+1)}]
}

finish_test
