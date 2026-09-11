# 2024 January 23
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
# Test special cases of corrupt database handling in mmap-mode.
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix mmapcorrupt

ifcapable !mmap {
  finish_test
  return
}
database_may_be_corrupt

db close
sqlite3_shutdown
sqlite3_config_lookaside 0 0
sqlite3_initialize

reset_db
do_execsql_test 1.0 {
  PRAGMA page_size = 16384;
  CREATE TABLE tn1(a PRIMARY KEY) WITHOUT ROWID;
  CREATE TABLE t0(a PRIMARY KEY) WITHOUT ROWID;
  CREATE TABLE t1(a PRIMARY KEY) WITHOUT ROWID;
  INSERT INTO t1 VALUES('B');
}
db close

set sz [file size test.db]
hexio_write test.db [expr $sz-3] 800380

sqlite3 db test.db
do_execsql_test 2.1 {
  PRAGMA mmap_size = 1000000;
  SELECT sql FROM sqlite_schema LIMIT 1;
  SELECT * FROM t0;
} {1000000 {CREATE TABLE tn1(a PRIMARY KEY) WITHOUT ROWID}}

do_execsql_test 2.2 {
  INSERT INTO t0 SELECT * FROM t1;
}

finish_test

