# 2021-10-15
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
# focus of this file is testing the sqlite3_autovacuum_pages() interface
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl

# If this build of the library does not support auto-vacuum, omit this
# whole file.
ifcapable {!autovacuum || !pragma} {
  finish_test
  return
}

# Demonstrate basic sqlite3_autovacuum_pages functionality
#
do_execsql_test autovacuum2-1.0 {
  PRAGMA page_size=1024;
  PRAGMA auto_vacuum=FULL;
  CREATE TABLE t1(x);
  VACUUM;
  INSERT INTO t1(x) VALUES(zeroblob(10000));
  PRAGMA page_count;
} {12}
proc autovac_page_callback {schema filesize freesize pagesize} {
  global autovac_callback_data
  lappend autovac_callback_data $schema $filesize $freesize $pagesize
  return [expr {$freesize/2}]
}
sqlite3_autovacuum_pages db autovac_page_callback
set autovac_callback_data {}
do_execsql_test autovacuum2-1.1 {
  BEGIN;
  DELETE FROM t1;
  PRAGMA freelist_count;
  PRAGMA page_count;
} {9 12}
do_execsql_test autovacuum2-1.2 {
  COMMIT;
} {}
do_test autovacuum2-1.3 {
  set autovac_callback_data
} {main 12 9 1024}
do_execsql_test autovacuum2-1.4 {
  PRAGMA freelist_count;
  PRAGMA page_count;
} {5 8}
do_execsql_test autovacuum2-1.5 {
  PRAGMA integrity_check;
} {ok}

# Disable the autovacuum-pages callback.  Then do any transaction.
# The database should shrink to minimal size
#
sqlite3_autovacuum_pages db
do_execsql_test autovacuum2-1.10 {
  CREATE TABLE t2(x);
  PRAGMA freelist_count;
} {0}

# Rig the autovacuum-pages callback to always return zero.  No
# autovacuum will happen.
#
proc autovac_page_callback_off {schema filesize freesize pagesize} {
  return 0
}
sqlite3_autovacuum_pages db autovac_page_callback_off
do_execsql_test autovacuum2-1.20 {
  BEGIN;
  INSERT INTO t1(x) VALUES(zeroblob(10000));
  DELETE FROM t1;
  PRAGMA freelist_count;
  COMMIT;
  PRAGMA freelist_count;
} {9 9}

finish_test
