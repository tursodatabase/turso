# 2025 May 30
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
# TESTRUNNER: slow
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
source $testdir/lock_common.tcl
set testprefix walsetlk_recover

ifcapable !wal {finish_test ; return }
# ifcapable !setlk_timeout {finish_test ; return }

do_execsql_test 1.0 {
  PRAGMA journal_mode = wal;
  CREATE TABLE t1(a, b);
  INSERT INTO t1 VALUES(1, 2);
  INSERT INTO t1 VALUES(3, 4);
  INSERT INTO t1 VALUES(5, 6);
} {wal}

db_save_and_close
db_restore

testfixture_nb myvar {

  testvfs tvfs -fullshm 1
  sqlite3 db test.db -vfs tvfs
  tvfs script vfs_callback
  tvfs filter xRead

  set ::done 0
  proc vfs_callback {method file args} {
    if {$::done==0 && [string match *wal $file]} {
      after 4000
      set ::done 1
    }
    return "SQLITE_OK"
  }

  db eval {
    SELECT * FROM t1
  }

  db close
}

# Give the [testfixture_nb] command time to start
after 1000 {set xyz 1}
vwait xyz

testvfs tvfs -fullshm 1
sqlite3 db test.db -vfs tvfs

tvfs script sleep_callback
tvfs filter xSleep
set ::sleep_count 0
proc sleep_callback {args} {
  incr ::sleep_count
}

sqlite3 db test.db -vfs tvfs
db timeout 500
set tm [lindex [time {
  catch {
    db eval {SELECT * FROM t1}
  } msg
}] 0]

do_test 1.2         { set ::msg } {database is locked}
do_test 1.3.($::tm) { expr $::tm>400000 && $::tm<2000000 } 1

vwait myvar

do_execsql_test 1.4 {
  SELECT * FROM t1
} {1 2 3 4 5 6}

db close
tvfs delete

# All SQLite builds should pass the tests above. SQLITE_ENABLE_SETLK_TIMEOUT=1
# builds do so without calling the VFS xSleep method.
if {$::sqlite_options(setlk_timeout)==1} {
  do_test 1.5.1 {
    set ::sleep_count
  } 0
} else {
  do_test 1.5.2 {
    expr $::sleep_count>0
  } 1
}

finish_test

