# 2008 June 18
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
# This file implements regression tests for SQLite library.  
#
# This file is devoted to testing the sqlite3_next_stmt and
# sqlite3_stmt_readonly and sqlite3_stmt_busy interfaces.
#
# $Id: capi3d.test,v 1.2 2008/07/14 15:11:20 drh Exp $
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl

# Create N prepared statements against database connection db
# and return a list of all the generated prepared statements.
#
proc make_prepared_statements {N} {
  set plist {}
  for {set i 0} {$i<$N} {incr i} {
    set sql "SELECT $i FROM sqlite_master WHERE name LIKE '%$i%'"
    if {rand()<0.33} {    
      set s [sqlite3_prepare_v2 db $sql -1 notused]
    } else {
      ifcapable utf16 {
        if {rand()<0.5} {
          set sql [encoding convertto unicode $sql]\x00\x00
          set s [sqlite3_prepare16 db $sql -1 notused]
        } else {
          set s [sqlite3_prepare db $sql -1 notused]
        }
      }
      ifcapable !utf16 {
        set s [sqlite3_prepare db $sql -1 notused]
      }
    }
    lappend plist $s
  }
  return $plist
}


# Scramble the $inlist into a random order.
#
proc scramble {inlist} {
  set y {}
  foreach x $inlist {
    lappend y [list [expr {rand()}] $x]
  }
  set y [lsort $y]
  set outlist {}
  foreach x $y {
    lappend outlist [lindex $x 1]
  }
  return $outlist
}

# Database initially has no prepared statements.
#
do_test capi3d-1.1 {
  db cache flush
  sqlite3_next_stmt db 0
} {}

# Run the following tests for between 1 and 100 prepared statements.
#
for {set i 1} {$i<=100} {incr i} {
  set stmtlist [make_prepared_statements $i]
  do_test capi3d-1.2.$i.1 {
    set p [sqlite3_next_stmt db 0]
    set x {}
    while {$p!=""} {
      lappend x $p
      set p [sqlite3_next_stmt db $p]
    }
    lsort $x
  } [lsort $stmtlist]
  do_test capi3-1.2.$i.2 {
    foreach p [scramble $::stmtlist] {
      sqlite3_finalize $p
    }
    sqlite3_next_stmt db 0
  } {}
}

# Tests for the is-read-only interface.
#
proc test_is_readonly {testname sql truth} {
  do_test $testname [format {
    set DB [sqlite3_connection_pointer db]
    set STMT [sqlite3_prepare $DB {%s} -1 TAIL]
    set rc [sqlite3_stmt_readonly $STMT]
    sqlite3_finalize $STMT
    set rc
  } $sql] $truth

  # EVIDENCE-OF: R-61212-30018 If prepared statement X is an EXPLAIN or
  # EXPLAIN QUERY PLAN statement, then sqlite3_stmt_readonly(X) returns
  # the same value as if the EXPLAIN or EXPLAIN QUERY PLAN prefix were
  # omitted.
  #
  do_test $testname.explain [format {
    set DB [sqlite3_connection_pointer db]
    set STMT [sqlite3_prepare $DB {EXPLAIN %s} -1 TAIL]
    set rc [sqlite3_stmt_readonly $STMT]
    sqlite3_finalize $STMT
    set rc
  } $sql] $truth
  do_test $testname.eqp [format {
    set DB [sqlite3_connection_pointer db]
    set STMT [sqlite3_prepare $DB {EXPLAIN QUERY PLAN %s} -1 TAIL]
    set rc [sqlite3_stmt_readonly $STMT]
    sqlite3_finalize $STMT
    set rc
  } $sql] $truth
}

# EVIDENCE-OF: R-23332-64992 The sqlite3_stmt_readonly(X) interface
# returns true (non-zero) if and only if the prepared statement X makes
# no direct changes to the content of the database file.
#
test_is_readonly capi3d-2.1 {SELECT * FROM sqlite_master} 1
test_is_readonly capi3d-2.2 {CREATE TABLE t1(x)} 0
db eval {CREATE TABLE t1(x)}
test_is_readonly capi3d-2.3 {INSERT INTO t1 VALUES(5)} 0
test_is_readonly capi3d-2.4 {UPDATE t1 SET x=x+1 WHERE x<0} 0
test_is_readonly capi3d-2.5 {SELECT * FROM t1} 1
ifcapable wal {
  test_is_readonly capi3d-2.6 {PRAGMA journal_mode=WAL} 0
  test_is_readonly capi3d-2.7 {PRAGMA wal_checkpoint} 0
}
test_is_readonly capi3d-2.8 {PRAGMA application_id=1234} 0
test_is_readonly capi3d-2.9 {VACUUM} 0
test_is_readonly capi3d-2.10 {PRAGMA integrity_check} 1
do_test capi3-2.49 {
  sqlite3_stmt_readonly 0
} 1


# EVIDENCE-OF: R-04929-09147 This routine returns false if there is any
# possibility that the statement might change the database file.
#
# EVIDENCE-OF: R-13288-53765 A false return does not guarantee that the
# statement will change the database file.
#
# EVIDENCE-OF: R-22182-18548 For example, an UPDATE statement might have
# a WHERE clause that makes it a no-op, but the sqlite3_stmt_readonly()
# result would still be false.
#
# EVIDENCE-OF: R-50998-48593 Similarly, a CREATE TABLE IF NOT EXISTS
# statement is a read-only no-op if the table already exists, but
# sqlite3_stmt_readonly() still returns false for such a statement.
#
db eval {
  CREATE TABLE t2(a,b,c);
  INSERT INTO t2 VALUES(1,2,3);
}
test_is_readonly capi3d-2.11 {UPDATE t2 SET a=a+1 WHERE false} 0
test_is_readonly capi3d-2.12 {CREATE TABLE IF NOT EXISTS t2(x,y)} 0


# EVIDENCE-OF: R-37014-01401 The ATTACH and DETACH statements also cause
# sqlite3_stmt_readonly() to return true since, while those statements
# change the configuration of a database connection, they do not make
# changes to the content of the database files on disk.
#
test_is_readonly capi3d-2.13 {ATTACH ':memory:' AS mem1} 1
db eval {ATTACH ':memory:' AS mem1}
test_is_readonly capi3d-2.14 {DETACH mem1} 1
db eval {DETACH mem1}

# EVIDENCE-OF: R-07474-04783 Transaction control statements such as
# BEGIN, COMMIT, ROLLBACK, SAVEPOINT, and RELEASE cause
# sqlite3_stmt_readonly() to return true, since the statements
# themselves do not actually modify the database but rather they control
# the timing of when other statements modify the database.
#
test_is_readonly capi3d-2.15 {BEGIN} 1
test_is_readonly capi3d-2.16 {COMMIT} 1
test_is_readonly capi3d-2.17 {SAVEPOINT one} 1
test_is_readonly capi3d-2.18 {RELEASE one} 1

# EVIDENCE-OF: R-36961-63052 The sqlite3_stmt_readonly() interface
# returns true for BEGIN since BEGIN merely sets internal flags, but the
# BEGIN IMMEDIATE and BEGIN EXCLUSIVE commands do touch the database and
# so sqlite3_stmt_readonly() returns false for those commands.
#
test_is_readonly capi3d-2.19 {BEGIN IMMEDIATE} 0
test_is_readonly capi3d-2.20 {BEGIN EXCLUSIVE} 0

# EVIDENCE-OF: R-21769-42523 For example, if an application defines a
# function "eval()" that calls sqlite3_exec(), then the following SQL
# statement would change the database file through side-effects: SELECT
# eval('DELETE FROM t1') FROM t2; But because the SELECT statement does
# not change the database file directly, sqlite3_stmt_readonly() would
# still return true.
#
proc evalsql {sql} {db eval $sql}
db func eval evalsql
test_is_readonly capi3d-2.21 {SELECT eval('DELETE FROM t1') FROM t2} 1

# Tests for the is-explain interface.
#
proc test_is_explain {testname sql truth} {
  do_test $testname [format {
    set DB [sqlite3_connection_pointer db]
    set STMT [sqlite3_prepare $DB {%s} -1 TAIL]
    set rc [sqlite3_stmt_isexplain $STMT]
    sqlite3_finalize $STMT
    set rc
  } $sql] $truth
}

test_is_explain capi3d-2.51 {SELECT * FROM sqlite_master} 0
test_is_explain capi3d-2.52 { explain SELECT * FROM sqlite_master} 1
test_is_explain capi3d-2.53 {  Explain Query Plan select * FROM sqlite_master} 2
do_test capi3-2.99 {
  sqlite3_stmt_isexplain 0
} 0

# Tests for sqlite3_stmt_busy
#
do_test capi3d-3.1 {
  db eval {INSERT INTO t1 VALUES(6); INSERT INTO t1 VALUES(7);}
  set STMT [sqlite3_prepare db {SELECT * FROM t1} -1 TAIL]
  sqlite3_stmt_busy $STMT
} {0}
do_test capi3d-3.2 {
  sqlite3_step $STMT
  sqlite3_stmt_busy $STMT
} {1}
do_test capi3d-3.3 {
  sqlite3_step $STMT
  sqlite3_stmt_busy $STMT
} {1}
do_test capi3d-3.4 {
  sqlite3_reset $STMT
  sqlite3_stmt_busy $STMT
} {0}

do_test capi3d-3.99 {
  sqlite3_finalize $STMT
  sqlite3_stmt_busy 0
} {0}

#--------------------------------------------------------------------------
# Test the sqlite3_stmt_busy() function with ROLLBACK statements.
#
reset_db

do_execsql_test capi3d-4.1 {
  CREATE TABLE t4(x,y);
  BEGIN;
}

do_test capi3d-4.2.1 {
  set ::s1 [sqlite3_prepare_v2 db "ROLLBACK" -1 notused]
  sqlite3_step $::s1
} {SQLITE_DONE}

do_test capi3d-4.2.2 {
  sqlite3_stmt_busy $::s1
} {0}

do_catchsql_test capi3d-4.2.3 {
  VACUUM
} {0 {}}

do_test capi3d-4.2.4 {
  sqlite3_reset $::s1
} {SQLITE_OK}

do_catchsql_test capi3d-4.2.5 {
  VACUUM
} {0 {}}

do_test capi3d-4.2.6 {
  sqlite3_finalize $::s1
} {SQLITE_OK}


finish_test
