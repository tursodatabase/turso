# 2025 Jan 24
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
set testprefix walsetlk2

ifcapable !wal {finish_test ; return }
ifcapable !setlk_timeout {finish_test ; return }

#-------------------------------------------------------------------------
# Check that xShmLock calls are as expected for write transactions in
# setlk mode.
#
reset_db

do_execsql_test 1.0 {
  PRAGMA journal_mode = wal;
  CREATE TABLE t1(a, b, c);
  INSERT INTO t1 VALUES(1, 2, 3);
} {wal}
db close

testvfs tvfs
tvfs script xShmLock_callback
tvfs filter xShmLock

set ::xshmlock [list]
proc xShmLock_callback {method path name detail} {
  lappend ::xshmlock $detail
}

sqlite3 db test.db -vfs tvfs
db timeout 1000

do_execsql_test 1.1 {
  SELECT * FROM t1
} {1 2 3}

do_execsql_test 1.2 {
  INSERT INTO t1 VALUES(4, 5, 6);
}

set ::xshmlock [list]
do_execsql_test 1.3 {
  INSERT INTO t1 VALUES(7, 8, 9);
}

do_test 1.4 {
  set ::xshmlock
} [list \
  {0 1 lock exclusive}                             \
  {4 1 lock exclusive} {4 1 unlock exclusive}      \
  {4 1 lock shared}                                \
  {0 1 unlock exclusive}                           \
  {4 1 unlock shared}
]

do_execsql_test 1.5.1 { SELECT * FROM t1 } {1 2 3 4 5 6 7 8 9}
set ::xshmlock [list]
do_execsql_test 1.5.2 {
  INSERT INTO t1 VALUES(10, 11, 12);
}
do_test 1.5.3 {
  set ::xshmlock
} [list \
  {0 1 lock exclusive}                             \
  {4 1 lock shared}                                \
  {0 1 unlock exclusive}                           \
  {4 1 unlock shared}
]

db close
tvfs delete

#-------------------------------------------------------------------------
# Check that if sqlite3_setlk_timeout() is used, blocking locks timeout
# but other operations do not use the retry mechanism.
#
reset_db
db close
sqlite3 db test.db -fullmutex 1

do_execsql_test 2.0 {
  CREATE TABLE t1(a, b);
  INSERT INTO t1 VALUES(1, 2), (3, 4);
}

sqlite3_setlk_timeout db 2000

# Launch a non-blocking testfixture process to write-lock the 
# database for 2000 ms.
testfixture_nb done {
  sqlite3 db test.db
  db eval {
    BEGIN EXCLUSIVE;
      INSERT INTO t1 VALUES(5, 6);
  }
  after 2000
  db eval {
    COMMIT
  }
  db close
}

after 500 {set ok 1}
vwait ok

do_catchsql_test 2.1 {
  INSERT INTO t1 VALUES(7, 8);
} {1 {database is locked}}

sqlite3_busy_timeout db 2000

do_catchsql_test 2.2 {
  INSERT INTO t1 VALUES(7, 8);
} {0 {}}

do_execsql_test 2.3 {
  SELECT * FROM t1
} {1 2 3 4 5 6 7 8}

do_execsql_test 2.4 {
  PRAGMA journal_mode = wal;
} {wal}

db close
sqlite3 db test.db

do_execsql_test 2.5 {
  INSERT INTO t1 VALUES(9, 10);
}

if {$::sqlite_options(setlk_timeout)==1} {

sqlite3_setlk_timeout db 2000

# Launch a non-blocking testfixture process to write-lock the 
# database for 2000 ms.
testfixture_nb done {
  sqlite3 db test.db
  db eval {
    BEGIN EXCLUSIVE;
      INSERT INTO t1 VALUES(11, 12);
  }
  after 2000
  db eval {
    COMMIT
  }
  db close
}

after 500 {set ok 1}
vwait ok

do_catchsql_test 2.6 {
  INSERT INTO t1 VALUES(13, 14);
} {0 {}}

do_execsql_test 2.7 {
  SELECT * FROM t1
} {1 2 3 4 5 6 7 8 9 10 11 12 13 14}

}


#-------------------------------------------------------------------------
# Check that if sqlite3_setlk_timeout(-1) is called, blocking locks are
# enabled and last for a few seconds at least. Difficult to test that they
# really do block indefinitely.
#
reset_db

if {$::sqlite_options(setlk_timeout)==1} {
do_execsql_test 3.0 {
  PRAGMA journal_mode = wal;
  CREATE TABLE t1(a INTEGER PRIMARY KEY, b);
  INSERT INTO t1 VALUES(1, 'one'), (3, 'three');
} {wal}

sqlite3_setlk_timeout db -1

# Launch a non-blocking testfixture process to write-lock the 
# database for 2000 ms.
testfixture_nb done {
  sqlite3 db test.db
  db eval {
    BEGIN EXCLUSIVE;
      INSERT INTO t1 VALUES(5, 'five');
  }
  after 2000
  db eval {
    COMMIT
  }
  db close
}

after 500 {set ok 1}
vwait ok

breakpoint
do_catchsql_test 3.1 {
  INSERT INTO t1 VALUES(7, 'seven');
} {0 {}}

# Launch another non-blocking testfixture process to write-lock the 
# database for 2000 ms.
testfixture_nb done {
  sqlite3 db test.db
  db eval {
    BEGIN EXCLUSIVE;
      INSERT INTO t1 VALUES(9, 'nine');
  }
  after 2000
  db eval {
    COMMIT
  }
  db close
}

after 500 {set ok 1}
vwait ok

do_catchsql_test 3.2 {
  INSERT INTO t1 VALUES(9, 'ten');
} {1 {UNIQUE constraint failed: t1.a}}

do_execsql_test 3.3 {
  SELECT * FROM t1
} {1 one 3 three 5 five 7 seven 9 nine}

db close

# Launch another non-blocking testfixture process to write-lock the 
# database for 2000 ms.
testfixture_nb done {
  sqlite3 db test.db
  db eval {
    BEGIN EXCLUSIVE;
      INSERT INTO t1 VALUES(11, 'eleven');
  }
  after 2000
  db eval {
    COMMIT
  }
}

sqlite3 db test.db
sqlite3_setlk_timeout db -1
do_catchsql_test 3.4 {
  INSERT INTO t1 VALUES(13, 'thirteen');
} {0 {}}

}

finish_test

