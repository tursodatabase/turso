# 2020 May 06
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
set testprefix walsetlk3

ifcapable !wal {finish_test ; return }
ifcapable !setlk_timeout {finish_test ; return }

do_execsql_test 1.0 {
  CREATE TABLE t1(x, y);
  PRAGMA journal_mode = wal;
  INSERT INTO t1 VALUES(1, 2);
  INSERT INTO t1 VALUES(3, 4);
} {wal}

db close

proc sql_block_on_close {sql} {
  testfixture_nb done [string map [list %SQL% $sql] {
    testvfs tvfs
    tvfs script xWrite
    tvfs filter xWrite
  
    set ::delay_done 0
    proc xWrite {method fname args} {
      if {[file tail $fname]=="test.db" && $::delay_done==0} {
        after 3000
        set ::delay_done 1
      }
      return 0
    }
  
    sqlite3 db test.db -vfs tvfs
    db eval {%SQL%}
    db close
  }]
}

# Start a second process that writes to the db, then blocks within the
# [db close] holding an EXCLUSIVE on the db in order to checkpoint and 
# delete the wal file. Then try to read the db.
#
# Without the SQLITE_SETLK_BLOCK_ON_CONNECT flag, this should fail with
# SQLITE_BUSY.
#
sql_block_on_close {
  INSERT INTO t1 VALUES(5, 6);
  INSERT INTO t1 VALUES(7, 8);
}
after 500 {set ok 1}
vwait ok
sqlite3 db test.db
sqlite3_setlk_timeout db 2000
do_catchsql_test 1.1 {
  SELECT * FROM t1
} {1 {database is locked}}

vwait ::done

# But with SQLITE_SETLK_BLOCK_ON_CONNECT flag, it should succeed.
#
sql_block_on_close {
  INSERT INTO t1 VALUES(9, 10);
  INSERT INTO t1 VALUES(11, 12);
}
after 500 {set ok 1}
vwait ok
sqlite3 db test.db
sqlite3_setlk_timeout -block db 2000
do_catchsql_test 1.2 {
  SELECT * FROM t1
} {0 {1 2 3 4 5 6 7 8 9 10 11 12}}

vwait ::done

#-------------------------------------------------------------------------
# Check that the SQLITE_SETLK_BLOCK_ON_CONNECT does not cause connections
# to block when taking a SHARED lock on a rollback mode database.
#
reset_db
do_execsql_test 2.1 {
  CREATE TABLE x1(a);
  INSERT INTO x1 VALUES(1), (2), (3);
}

proc sql_block_on_write {sql} {
  testfixture_nb done [string map [list %SQL% $sql] {
    sqlite3 db test.db 
    db eval "BEGIN EXCLUSIVE"
    db eval {%SQL%}
    after 3000
    db eval COMMIT
    db close
  }]
}

db close
sql_block_on_write {
  INSERT INTO x1 VALUES(4);
}

after 500 {set ok 1}
vwait ok

sqlite3 db test.db
sqlite3_setlk_timeout -block db 2000

do_catchsql_test 2.2 {
  SELECT * FROM x1
} {1 {database is locked}}

vwait ::done
after 500 {set ok 1}
vwait ok

do_catchsql_test 2.3 {
  SELECT * FROM x1
} {0 {1 2 3 4}}

finish_test

