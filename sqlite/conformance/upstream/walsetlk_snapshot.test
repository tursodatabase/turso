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
set testprefix walsetlk_snapshot

ifcapable !wal {finish_test ; return }
ifcapable !snapshot {finish_test; return}

db close
testvfs tvfs -fullshm 1
sqlite3 db test.db -vfs tvfs
tvfs script sleep_callback
tvfs filter xSleep
set ::sleep_count 0
proc sleep_callback {args} {
  incr ::sleep_count
}

do_execsql_test 1.0 {
  PRAGMA journal_mode = wal;
  CREATE TABLE t1(a, b);
  INSERT INTO t1 VALUES(1, 2);
  INSERT INTO t1 VALUES(3, 4);
  INSERT INTO t1 VALUES(5, 6);
} {wal}

do_test 1.1 {
  db eval BEGIN
  set ::snap [sqlite3_snapshot_get db main]
  db eval {
    INSERT INTO t1 VALUES(7, 8);
    COMMIT;
  }
} {}

testfixture_nb myvar {

  testvfs tvfs -fullshm 1
  sqlite3 db test.db -vfs tvfs
  tvfs script vfs_callback
  tvfs filter {xWrite}

  set ::done 0
  proc vfs_callback {args} {
    if {$::done==0} {
      after 4000
      set ::done 1
    }
    return "SQLITE_OK"
  }

  db eval {
    PRAGMA wal_checkpoint;
  }

  db close
}

# Give the [testfixture_nb] command time to start
after 1000 {set xyz 1}
vwait xyz

db timeout 500
set tm [lindex [time {
  catch {
    db eval BEGIN
      sqlite3_snapshot_open db main $::snap
  } msg
}] 0]

do_test 1.2 { set ::msg } {SQLITE_BUSY}
do_test 1.3.($::tm) { expr $::tm<2000000 } 1

do_execsql_test 1.4 {
  SELECT * FROM t1
} {1 2 3 4 5 6 7 8}

sqlite3_snapshot_free $::snap

vwait myvar

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

