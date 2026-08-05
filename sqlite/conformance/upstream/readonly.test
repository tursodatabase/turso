# 2024 March 21
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
# This file contains tests for using databases in read-only mode on
# unix.
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
if {$tcl_platform(platform) eq "windows"} {
  finish_test
  return
}
source $testdir/lock_common.tcl
source $testdir/wal_common.tcl
set ::testprefix readonly

do_execsql_test 1.0 {
  CREATE TABLE t1(a, b);
  INSERT INTO t1 VALUES(1, 2), (3, 4), (5, 6);
}

db close
file attributes test.db -permissions r--r--r--

sqlite3 db test.db

do_catchsql_test 1.1 {
  INSERT INTO t1 VALUES(7, 8);
} {1 {attempt to write a readonly database}}

do_execsql_test 1.2 {
  BEGIN;
    SELECT * FROM t1;
} {1 2 3 4 5 6}

# The following attempts to open a read/write fd on the database 20,000 
# times. And each time instead opens a read-only fd. At one point this
# was failing to reuse cached fds, causing a "too many open file-descriptors"
# error.
do_test 1.3 {
  for {set ii 0} {$ii < 20000} {incr ii} {
    sqlite3 db2 test.db
    db2 close
  }
  set {} {} 
} {}


finish_test
