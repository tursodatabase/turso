# 2021 April 2
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
source $testdir/lock_common.tcl
set testprefix external_reader

ifcapable !wal {
  finish_test 
  return 
}
if {$::tcl_platform(os) eq "Windows NT"} {
  finish_test 
  return
}

do_multiclient_test tn {

  set bExternal 1
  if {[info commands db3]!=""} { set bExternal 0 }

  do_test 1.$tn.0 {
    sql1 {
      PRAGMA journal_mode = wal;
      CREATE TABLE t1(a, b);
      INSERT INTO t1 VALUES(1, 2);
    }
  } {wal}

  do_test 1.$tn.1 {
    sql2 { SELECT * FROM t1 }
  } {1 2}

  do_test 1.$tn.2 {
    code1 {
      file_control_external_reader db
    } 
  } {0}

  do_test 1.$tn.3 {
    sql2 { 
      BEGIN;
        SELECT * FROM t1;
    }
  } {1 2}

  do_test 1.$tn.4 {
    code1 {
      file_control_external_reader db
    } 
  } $bExternal

  do_test 1.$tn.5 {
    sql2 { COMMIT }
  } {}

  do_test 1.$tn.6 {
    code1 { file_control_external_reader db } 
  } 0

}


finish_test
