# 2023 January 31
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
set testprefix pendingrace

# This test file tests that a race condition surrounding hot-journal
# rollback that once existed has been resolved. The problem was that
# if, when attempting to upgrade from a SHARED to EXCLUSIVE lock in
# order to roll back a hot journal, a connection failed to take the
# lock, the file-descriptor was left holding a PENDING lock for 
# a very short amount of time. In a multi-threaded deployment, this
# could allow a second connection to read the database without rolling
# back the hot journal.
#

testvfs tvfs 
db close
sqlite3 db test.db -vfs tvfs

# Create a 20 page database using connection [db]. Connection [db] uses
# Tcl VFS wrapper "tvfs", but it is configured to do straight pass-through
# for now.
#
do_execsql_test 1.0 {
  PRAGMA cache_size = 5;
  CREATE TABLE t1(a, b);
  CREATE INDEX i1 ON t1(a, b);
  WITH s(i) AS (
    SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i<10
  )
  INSERT INTO t1 SELECT hex(randomblob(100)), hex(randomblob(100)) FROM s;
} {}
do_test 1.1a {
  set nPg [db one { PRAGMA page_count }]
  expr ($nPg==20 || $nPg==21)
} 1

# Simulate a crash in another process. This leaves the db with a hot-journal.
# Without the journal the db is corrupt.
#
sqlite3 db2 test.db
do_execsql_test -db db2 1.1 {
  PRAGMA cache_size = 5;
  BEGIN;
    UPDATE t1 SET b=hex(randomblob(100));
}
db_save
db2 close
proc my_db_restore {} {
  forcecopy sv_test.db-journal test.db-journal

  set fd1 [open sv_test.db r]
  fconfigure $fd1 -translation binary
  set data [read $fd1]
  close $fd1

  set fd1 [open test.db w]
  fconfigure $fd1 -translation binary
  puts -nonewline $fd1 $data
  close $fd1
}
my_db_restore
do_test 1.2 {
  file exists test.db-journal
} {1}

# Set up connection [db2] to use Tcl VFS wrapper [tvfs2]. Which is configured
# so that the first call to xUnlock() fails. And then all VFS calls thereafter
# fail as well.
#
testvfs tvfs2
tvfs2 filter xUnlock
tvfs2 script xUnlock
set ::seen_unlock 0
proc xUnlock {args} {
  if {$::seen_unlock==0} {
    set ::seen_unlock 1
    tvfs2 ioerr 1 1
    tvfs2 filter {xLock xUnlock}
  }
  return ""
}
sqlite3 db2 test.db -vfs tvfs2

# Configure [tvfs] (used by [db]) so that within the first call to xAccess,
# [db2] attempts to read the db. This causes [db2] to fail to upgrade to
# EXCLUSIVE, leaving it with a PENDING lock. Which it holds on to, 
# as the xUnlock() and all subsequent VFS calls fail.
#
tvfs filter xAccess
tvfs script xAccess
set ::seen_access 0
proc xAccess {args} {
  if {$::seen_access==0} {
    set ::seen_access 1
    catch { db2 eval { SELECT count(*)+0 FROM t1 } }
    breakpoint
  }
  return ""
}

# Run an integrity check using [db].
do_catchsql_test 1.3 {
  PRAGMA integrity_check
} {1 {database is locked}}

db close
db2 close
tvfs delete
tvfs2 delete

finish_test
