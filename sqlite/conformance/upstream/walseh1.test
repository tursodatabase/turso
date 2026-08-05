# 2021 August 16
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
source $testdir/malloc_common.tcl
set testprefix walseh1

set ::seh_countdown 0 
set ::seh_errno 10
proc seh_faultsim_callback {iFault} {
  if {$iFault==650} {
    incr ::seh_countdown -1
    if {$::seh_countdown==0} { return $::seh_errno }
  }
  return 0
}

proc seh_injectinstall {} {
  sqlite3_test_control_fault_install seh_faultsim_callback
}
proc seh_injectuninstall {} {
  sqlite3_test_control_fault_install
}
proc seh_injectstart {iFail} {
  set ::seh_errno [expr 2+$iFail*10]
  set ::seh_countdown $iFail
}
proc seh_injectstop {} {
  set res [expr $::seh_countdown<=0]
  set ::seh_countdown 0
  set res
}

set FAULTSIM(seh) [list                   \
  -injectinstall   seh_injectinstall   \
  -injectstart     seh_injectstart     \
  -injectstop      seh_injectstop      \
  -injecterrlist   {{1 {disk I/O error}}} \
  -injectuninstall seh_injectuninstall \
]

proc test_system_errno {db expect} {
  set serrno [sqlite3_system_errno $db]
  if {$serrno!=$expect} {
    error "surprising system_errno. Expected $expect, got $serrno"
  }  
}

do_execsql_test 1.0 {
  PRAGMA journal_mode = wal;
  CREATE TABLE t1(x, y);
  INSERT INTO t1 VALUES(1, 2);
  INSERT INTO t1 VALUES(3, 4);
} {wal}
faultsim_save_and_close

do_faultsim_test 1 -faults seh -prep {
  catch { db2 close }
  faultsim_restore_and_reopen
  execsql { SELECT * FROM sqlite_schema } 
  sqlite3 db2 test.db
} -body {
  execsql { SELECT * FROM t1 } db2
} -test {
  faultsim_test_result {0 {1 2 3 4}} 
  if {$testrc} { test_system_errno db2 $::seh_errno }
}
catch { db2 close }

faultsim_save_and_close

do_faultsim_test 2 -faults seh -prep {
  catch { db close }
  faultsim_restore_and_reopen
} -body {
  execsql { SELECT * FROM t1 } 
} -test {
  faultsim_test_result {0 {1 2 3 4}} 
  if {$testrc} { test_system_errno db $::seh_errno }
}

do_faultsim_test 3 -faults seh -prep {
  catch { db close }
  faultsim_restore_and_reopen
} -body {
  execsql { INSERT INTO t1 VALUES(5, 6) }
  execsql { SELECT * FROM t1 }
} -test {
  faultsim_test_result {0 {1 2 3 4 5 6}} 
  if {$testrc} { test_system_errno db $::seh_errno }
}

do_faultsim_test 4 -faults seh -prep {
  catch { db close }
  faultsim_restore_and_reopen
} -body {
  execsql { PRAGMA wal_checkpoint }
  execsql { INSERT INTO t1 VALUES(7, 8) }
  execsql { SELECT * FROM t1 }
} -test {
  faultsim_test_result {0 {1 2 3 4 7 8}} 
  if {$testrc} { test_system_errno db $::seh_errno }
}
catch { db close }

do_faultsim_test 5 -faults seh -prep {
  catch { db close }
  faultsim_restore_and_reopen
  execsql {
    PRAGMA cache_size = 5;
    BEGIN;
      WITH s(i) AS (
        SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i<50
      )
      INSERT INTO t1 SELECT randomblob(500), randomblob(500) FROM s;
  }
} -body {
  execsql ROLLBACK
} -test {
  faultsim_test_result {0 {}}
  if {$testrc} { test_system_errno db $::seh_errno }
}
catch { db close }

do_faultsim_test 6 -faults seh -prep {
  catch { db close }
  faultsim_restore_and_reopen
} -body {
  execsql { PRAGMA wal_checkpoint = TRUNCATE }
  execsql { INSERT INTO t1 VALUES(7, 8) }
  execsql { SELECT * FROM t1 }
} -test {
  faultsim_test_result {0 {1 2 3 4 7 8}} 
  if {$testrc} { test_system_errno db $::seh_errno }
}
catch { db close }

finish_test
