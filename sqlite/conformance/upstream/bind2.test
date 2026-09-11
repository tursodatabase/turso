# 2022 Feb 10
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
set testprefix bind2


# Test that using bind_value() on a REAL sqlite3_value that was stored
# as an INTEGER works properly.
#
#   1.1: An IntReal value read from a table,
#   1.2: IntReal values obtained via the sqlite3_preupdate_old|new() 
#        interfaces.
# 
do_execsql_test 1.0 { 
  CREATE TABLE t1(a REAL);
  INSERT INTO t1 VALUES(42.0);
  SELECT * FROM t1;
} {42.0}

do_test 1.1 {
  set stmt [sqlite3_prepare db "SELECT ?" -1 tail]
  sqlite3_bind_value_from_select $stmt 1 "SELECT a FROM t1"
  sqlite3_step $stmt
  sqlite3_column_text $stmt 0
} {42.0}
sqlite3_finalize $stmt

ifcapable !preupdate {
  finish_test
  return
}

proc preup {args} {
  set stmt [sqlite3_prepare db "SELECT ?" -1 tail]
  sqlite3_bind_value_from_preupdate $stmt 1 old 0
  sqlite3_step $stmt
  lappend ::reslist [sqlite3_column_text $stmt 0]
  sqlite3_reset $stmt
  sqlite3_bind_value_from_preupdate $stmt 1 new 0
  sqlite3_step $stmt
  lappend ::reslist [sqlite3_column_text $stmt 0]
  sqlite3_finalize $stmt
}
db preupdate hook preup

do_test 1.2 {
  set ::reslist [list]
  execsql { UPDATE t1 SET a=43; }
  set ::reslist
} {42.0 43.0}

finish_test
