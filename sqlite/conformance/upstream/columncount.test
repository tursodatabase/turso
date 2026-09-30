# 2021 February 15
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
# focus of this file is testing the sqlite3_column_count() API.
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix columncount

# If SQLITE_OMIT_ALTERTABLE is defined, omit this file.
ifcapable !altertable {
  finish_test
  return
}

proc do_ccsql_test {tn sql res} {

  uplevel [list do_test $tn [subst -nocommands {
    set stmt [sqlite3_prepare_v2 db {$sql} -1 dummy]
    set res [sqlite3_column_count [set stmt]]
    while {[sqlite3_step [set stmt]]=="SQLITE_ROW"} {
      for {set i 0} {[set i] < [sqlite3_data_count [set stmt]]} {incr i} {
        lappend res [sqlite3_column_text [set stmt] [set i]]
      }
    }
  
    set rc [sqlite3_finalize [set stmt]]
    if {[set rc]!="SQLITE_OK"} {
      error [sqlite3_errmsg db]
    }

    set res
  }] [list {*}$res]]

}

do_execsql_test 1.0 {
  CREATE TABLE t1(x, y, z);
  INSERT INTO t1 VALUES('a', 'b', 'c');
}

do_ccsql_test 1.1 { SELECT * FROM t1 }      {3    a b c}
do_ccsql_test 1.2 { CREATE TABLE t2(a, b) } {0}

do_ccsql_test 1.3 { ALTER TABLE t2 RENAME TO t3 } {0}
do_ccsql_test 1.4 { ALTER TABLE t3 RENAME b TO ccc } {0}
do_ccsql_test 1.5 { ALTER TABLE t3 ADD COLUMN d } {0}

do_ccsql_test 1.6 { DROP TABLE t3 } {0}



finish_test
