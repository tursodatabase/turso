# 2007 Sept 7
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
# This file implements tests to make sure SQLite does not crash or
# segfault if it sees a corrupt database file.
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix corrupt4

# This module uses hard-coded offsets which do not work if the reserved_bytes
# value is nonzero.
if {[nonzero_reserved_bytes]} {finish_test; return;}

# These tests deal with corrupt database files
#
database_may_be_corrupt

# We must have the page_size pragma for these tests to work.
#
ifcapable !pager_pragmas {
  finish_test
  return
}

# Create a database with a freelist containing at least two pages.
#
do_test corrupt4-1.1 {
  set bigstring [string repeat 0123456789 200]
  execsql {
    PRAGMA auto_vacuum=OFF;
    PRAGMA page_size=1024;
    CREATE TABLE t1(x);
    INSERT INTO t1 VALUES($bigstring);
    CREATE TABLE t2(y);
    INSERT INTO t2 VALUES(1);
    DROP TABLE t1;
  }
  file size test.db
} [expr {1024*4}]

# Verify that there are two pages on the freelist.
#
do_test corrupt4-1.2 {
  execsql {PRAGMA freelist_count}
} {2}

# Get the page number for the trunk of the freelist.
#
set trunkpgno [hexio_get_int [hexio_read test.db 32 4]]
set baseaddr [expr {($trunkpgno-1)*1024}]

# Verify that the trunk of the freelist has exactly one
# leaf.
#
do_test corrupt4-1.3 {
  hexio_get_int [hexio_read test.db [expr {$::baseaddr+4}] 4]
} {1}

# Insert a negative number as the number of leaves on the trunk.
# Then try to add a new element to the freelist.
#
do_test corrupt4-1.4 {
  hexio_write test.db [expr {$::baseaddr+4}] [hexio_render_int32 -100000000]
  db close
  sqlite3 db test.db
  catchsql {
    DROP TABLE t2
  }
} {1 {database disk image is malformed}}

#-------------------------------------------------------------------------

reset_db
do_execsql_test 2.0 {
  PRAGMA page_size = 512;
  CREATE TABLE t1(a, b, c);
}

# Create a database with a schema so large that the root of the 
# sqlite_schema table is the grandparent of its leaves.
#
set nView 1000
do_test 2.1 {
  execsql BEGIN
  for {set ii 0} {$ii<$nView} {incr ii} {
    execsql " CREATE VIEW v$ii AS SELECT a, b, c FROM t1 "
  }
  execsql COMMIT
} {}
db close

proc get2byte {fd offset} {
  seek $fd $offset
  set bin [read $fd 2]
  binary scan $bin S val
  set val
}
proc get4byte {fd offset} {
  seek $fd $offset
  set bin [read $fd 4]
  binary scan $bin I val
  set val
}
proc put4byte {fd offset val} {
  seek $fd $offset
  set bin [binary format I $val]
  puts -nonewline $fd $bin
}

# Page 1 is now the grandparent of its leaves. Corrupt the database by setting 
# the second rightmost child page number of page 1 to 1.
#
set fd [open test.db r+]
fconfigure $fd -translation binary
set nChild [get2byte $fd 103]
set offChild [get2byte $fd [expr 100+12+($nChild-2)*2]]
set pgnoChild [get4byte $fd $offChild]
put4byte $fd $offChild 1
close $fd

if {![info exists ::G(perm:presql)]} {
  sqlite3 db test.db

  do_catchsql_test 2.2 {
    PRAGMA writable_schema = 1;
    SELECT * FROM sqlite_schema;
  } {1 {database disk image is malformed}}

  do_test 2.3 {
    list [catch {
      for {set ii $nView} {$ii<$nView*2} {incr ii} {
        execsql "INSERT INTO sqlite_master VALUES(1, 2, 3, 4, 5)"
      }
    } msg] $msg
  } {1 {database disk image is malformed}}
}

finish_test
