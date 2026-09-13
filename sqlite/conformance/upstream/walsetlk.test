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
set testprefix walsetlk

ifcapable !wal {finish_test ; return }
db timeout 1000

#-------------------------------------------------------------------------
# 1.*: Test that nothing goes wrong if recovery is forced while opening
#      a write transaction or performing a checkpoint with blocking locks.
#

do_execsql_test 1.0 {
  CREATE TABLE t1(x, y);
  PRAGMA journal_mode = wal;
  INSERT INTO t1 VALUES(1, 2);
  INSERT INTO t1 VALUES(3, 4);
  INSERT INTO t1 VALUES(5, 6);
  INSERT INTO t1 VALUES(7, 8);
} {wal}

sqlite3 db2 test.db
db2 timeout 1000

do_execsql_test -db db2 1.1 {
  SELECT * FROM t1
} {1 2 3 4 5 6 7 8}

set fd [open test.db-shm r+]
puts $fd "blahblahblahblah"
flush $fd

do_execsql_test 1.2 {
  BEGIN;
    INSERT INTO t1 VALUES(9, 10);
}

do_execsql_test -db db2 1.3 {
  SELECT * FROM t1
} {1 2 3 4 5 6 7 8}

do_test 1.4 {
  list [catch {db2 eval { BEGIN EXCLUSIVE }} msg] $msg
} {1 {database is locked}}

do_execsql_test 1.5 { COMMIT }
do_execsql_test -db db2 1.6 {
  SELECT * FROM t1
} {1 2 3 4 5 6 7 8 9 10}

puts $fd "blahblahblahblah"
flush $fd

do_execsql_test -db db2 1.7 {
  PRAGMA wal_checkpoint = TRUNCATE
} {0 0 0}

do_test 1.8 {
  file size test.db-wal
} 0

close $fd
db close
db2 close
#-------------------------------------------------------------------------

do_multiclient_test tn {

  testvfs tvfs -fullshm 1
  db close
  sqlite3 db test.db -vfs tvfs
  tvfs script xSleep_callback
  tvfs filter xSleep

  set ::sleep_count 0
  proc xSleep_callback {xSleep nMs} {
    after [expr $nMs / 1000]
    incr ::sleep_count
  }

  do_test 2.$tn.1 {
    sql1 {
      PRAGMA journal_mode = wal;
      CREATE TABLE t1(s, v);
      INSERT INTO t1 VALUES(1, 2);
      INSERT INTO t1 VALUES(3, 4);
      INSERT INTO t1 VALUES(5, 6);
    }
    code1 { db timeout 1100 }
  } {}

  do_test 2.$tn.2 {
    sql2 {
      BEGIN;
        INSERT INTO t1 VALUES(7, 8);
    }
  } {}

  do_test 2.$tn.3 {
    set us [lindex [time { catch {db eval "BEGIN EXCLUSIVE"} }] 0]
    expr $us>1000000 && $us<4000000
  } {1}

  do_test 2.$tn.4 {
    sql2 { COMMIT }
    sql1 { SELECT * FROM t1 }
  } {1 2 3 4 5 6 7 8}

  do_test 2.$tn.5 {
    sql2 {
      BEGIN;
        INSERT INTO t1 VALUES(9, 10);
    }
  } {}

  do_test 2.$tn.6 {
    set us [lindex [time { catch {db eval "PRAGMA wal_checkpoint=RESTART"} }] 0]
    expr $us>1000000 && $us<4000000
  } {1}

  do_test 2.$tn.7 {
    sql2 {
      COMMIT;
      BEGIN;
        SELECT * FROM t1;
    }
  } {1 2 3 4 5 6 7 8 9 10}

  do_test 2.$tn.8 {
    set us [lindex [time { catch {db eval "PRAGMA wal_checkpoint=RESTART"} }] 0]
    expr $us>1000000 && $us<4000000
  } {1}
  do_test 2.$tn.9 {
    sql3 {
      INSERT INTO t1 VALUES(11, 12);
    }
    sql2 {
      COMMIT;
      BEGIN;
        SELECT * FROM t1;
    }
    sql3 {
      INSERT INTO t1 VALUES(13, 14);
    }
  } {}

  do_test 2.$tn.10 {
    set us [lindex [time { catch {db eval "PRAGMA wal_checkpoint=RESTART"} }] 0]
    expr $us>1000000 && $us<4000000
  } {1}

  do_test 2.$tn.11 {
    sql3 {
      BEGIN;
        SELECT * FROM t1;
    }
    sql1 { INSERT INTO t1 VALUES(15, 16); }
  } {}

  do_test 2.$tn.12 {
    set us [lindex [time { catch {db eval "PRAGMA wal_checkpoint=RESTART"} }] 0]
    expr $us>1000000 && $us<4000000
  } {1}

  do_test 2.$tn.13 {
    sql2 {
      COMMIT;
      BEGIN;
        SELECT * FROM t1;
    }
    sql1 { INSERT INTO t1 VALUES(17, 18); }
  } {}

  do_test 2.$tn.14 {
    set us [lindex [time { catch {db eval "PRAGMA wal_checkpoint=RESTART"} }] 0]
    expr $us>1000000 && $us<4000000
  } {1}

  db close
  tvfs delete

  # Set bSleep to true if it is expected that the above used xSleep() to
  # wait for locks. bSleep is true unless SQLITE_ENABLE_SETLK_TIMEOUT is
  # set to 1 and either:
  #
  #   * the OS is windows, or
  #   * the OS is unix and the tests were run with each connection 
  #     in a separate process.
  #
  set bSleep 1
  if {$::sqlite_options(setlk_timeout)==1} {
    if {$::tcl_platform(platform) eq "windows"} {
      set bSleep 0
    }
    if {$::tcl_platform(platform) eq "unix"} {
      set bSleep [expr $tn==2]
    }
  }

  do_test 2.$tn.15.$bSleep {
    expr $::sleep_count > 0
  } $bSleep
}

#-------------------------------------------------------------------------
reset_db

testvfs tvfs -fullshm 1
tvfs script xSleep_callback
tvfs filter xSleep

set ::sleep_count 0
proc xSleep_callback {xSleep nMs} {
  after [expr $nMs / 1000]
  incr ::sleep_count
  breakpoint
}

sqlite3 db2 test.db -vfs tvfs
db2 timeout 1000

do_execsql_test 3.0 {
  PRAGMA journal_mode = wal;
  CREATE TABLE x1(x, y);
  BEGIN;
    INSERT INTO x1 VALUES(1, 2);
} {wal}

do_execsql_test -db db2 3.1a {
  SELECT * FROM x1
} {}

do_test 3.1b {
  list [catch { db2 eval {BEGIN EXCLUSIVE} } msg] $msg
} {1 {database is locked}}

# Set bExpect to true if calls to xSleep() are expected. Such calls are
# expected unless this is an SQLITE_ENABLE_SETLK_TIMEOUT=1 build.
set bExpect 1
if {$tcl_platform(platform) eq "windows" && $::sqlite_options(setlk_timeout)==1} {
  set bExpect 0
}
do_test 3.2 {
  expr {$::sleep_count > 0}
} $bExpect
set ::sleep_count 0

do_execsql_test 3.3 {
  COMMIT;
}

# Launch a non-blocking testfixture process to write-lock the 
# database for 2000 ms.
testfixture_nb done {
  sqlite3 db test.db
  db eval {
    BEGIN EXCLUSIVE;
     INSERT INTO x1 VALUES(3, 4);
  }
  after 2000
  db eval {
    COMMIT
  }
}

after 500 {set ok 1}
vwait ok

db2 timeout 5000
do_test 3.4 {
  set t [lindex [time { db2 eval { BEGIN EXCLUSIVE } }] 0]
  expr ($t>1000000)
} {1}

# Set bExpect to true if calls to xSleep() are expected. Such calls are
# expected unless this is an SQLITE_ENABLE_SETLK_TIMEOUT=1 build.
set bExpect 1
if {$::sqlite_options(setlk_timeout)==1} {
  set bExpect 0
}
do_test 3.5 {
  expr {$::sleep_count > 0}
} $bExpect

do_execsql_test -db db2 3.6 {
  INSERT INTO x1 VALUES(5, 6);
  COMMIT;
  SELECT * FROM x1;
} {1 2 3 4 5 6}

# Launch a non-blocking testfixture process to write-lock the 
# database for 2000 ms.
testfixture_nb done {
  sqlite3 db test.db
  db eval {
    BEGIN EXCLUSIVE;
     INSERT INTO x1 VALUES(7, 8);
  }
  after 2000
  db eval {
    COMMIT
  }
  db close
}

after 500 {set ok 1}
vwait ok

db2 timeout 0x7FFFFFFF
do_test 3.7 {
  set t [lindex [time { db2 eval { BEGIN EXCLUSIVE } }] 0]
  expr ($t>1000000)
} {1}

# Set bExpect to true if calls to xSleep() are expected. Such calls are
# expected unless this is an SQLITE_ENABLE_SETLK_TIMEOUT=1 build.
set bExpect 1
if {$::sqlite_options(setlk_timeout)==1} {
  set bExpect 0
}
do_test 3.8 {
  expr {$::sleep_count > 0}
} $bExpect

do_execsql_test -db db2 3.9 {
  INSERT INTO x1 VALUES(9, 10);
  COMMIT;
  SELECT * FROM x1;
} {1 2 3 4 5 6 7 8 9 10}

db2 close
tvfs delete

finish_test

