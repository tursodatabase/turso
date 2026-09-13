# 2008 June 28
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
# This file implements regression tests for SQLite library.  The
# focus of this script is database locks.
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix lock5

# This file is only run if using the unix backend compiled with the
# SQLITE_ENABLE_LOCKING_STYLE macro.
db close
if {[catch {sqlite3 db test.db -vfs unix-none} msg]} {
  finish_test
  return
}
db close
forcedelete test.db.lock

ifcapable lock_proxy_pragmas {
  set ::using_proxy 0
  foreach {name value} [array get env SQLITE_FORCE_PROXY_LOCKING] {
    set ::using_proxy $value
  }
  # Disable the proxy locking for these tests
  set env(SQLITE_FORCE_PROXY_LOCKING) "0"
}


do_test lock5-dotfile.1 {
  sqlite3 db test.db -vfs unix-dotfile
  execsql {
    BEGIN;
    CREATE TABLE t1(a, b);
  }
} {}

do_test lock5-dotfile.2 {
  file exists test.db.lock
} {1}

do_test lock5-dotfile.3 {
  execsql COMMIT
  file exists test.db.lock
} {0}

do_test lock5-dotfile.4 {
  sqlite3 db2 test.db -vfs unix-dotfile
  execsql {
    INSERT INTO t1 VALUES('a', 'b');
    SELECT * FROM t1;
  } db2
} {a b}

do_test lock5-dotfile.5 {
  execsql {
    BEGIN;
    SELECT * FROM t1;
  } db2
} {a b}

do_test lock5-dotfile.6 {
  file exists test.db.lock
} {1}

do_test lock5-dotfile.7 {
  catchsql { SELECT * FROM t1; }
} {1 {database is locked}}

do_test lock5-dotfile.8 {
  execsql {
    SELECT * FROM t1;
    ROLLBACK;
  } db2
} {a b}

do_test lock5-dotfile.9 {
  catchsql { SELECT * FROM t1; }
} {0 {a b}}

do_test lock5-dotfile.10 {
  file exists test.db.lock
} {0}

do_test lock5-dotfile.X {
  db2 close
  execsql {BEGIN EXCLUSIVE}
  db close
  file exists test.db.lock
} {0}

#####################################################################

forcedelete test.db
if {0==[catch {sqlite3 db test.db -vfs unix-flock} msg]} {

do_test lock5-flock.1 {
  sqlite3 db test.db -vfs unix-flock
  execsql {
    CREATE TABLE t1(a, b);
    BEGIN;
    INSERT INTO t1 VALUES(1, 2);
  }
} {}

# Make sure we are not accidentally using the dotfile locking scheme.
do_test lock5-flock.2 {
  file exists test.db.lock
} {0}

do_test lock5-flock.3 {
  catch { sqlite3 db2 test.db -vfs unix-flock }
  catchsql { SELECT * FROM t1 } db2
} {1 {database is locked}}

do_test lock5-flock.4 {
  execsql COMMIT
  catchsql { SELECT * FROM t1 } db2
} {0 {1 2}}

do_test lock5-flock.5 {
  execsql BEGIN
  catchsql { SELECT * FROM t1 } db2
} {0 {1 2}}

do_test lock5-flock.6 {
  execsql {SELECT * FROM t1}
  catchsql { SELECT * FROM t1 } db2
} {1 {database is locked}}

do_test lock5-flock.7 {
  db close
  catchsql { SELECT * FROM t1 } db2
} {0 {1 2}}

do_test lock5-flock.8 {
  db2 close
} {}

do_test lock5-flock.9 {
  sqlite3 db test.db -vfs unix-flock
  execsql {
    SELECT * FROM t1
  }
} {1 2}

do_test lock5-flock.10 {
  sqlite3 db2 test.db -vfs unix-flock
  execsql {
    SELECT * FROM t1
  } db2
} {1 2}

do_test lock5-flock.10 {
  execsql {
    PRAGMA cache_size = 1;
    BEGIN;
      WITH s(i) AS (
        SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i<10000
      )
      INSERT INTO t1 SELECT i, i+1 FROM s;
  }

  catchsql {
    SELECT * FROM t1
  } db2
} {1 {database is locked}}

if {[permutation]!="inmemory_journal"} {
  do_test lock5-flock.11 {
    forcecopy test.db test.db2
    forcecopy test.db-journal test.db2-journal
    db2 close
    sqlite3 db2 test.db2 -vfs unix-flock
    catchsql {
      SELECT * FROM t1
    } db2
  } {0 {1 2}}
  
  do_test lock5-flock.12 {
    file exists test.db2-journal
  } 0
}

db close
db2 close

}

#####################################################################

reset_db

do_test lock5-none.1 {
  sqlite3 db test.db -vfs unix-none
  sqlite3 db2 test.db -vfs unix-none
  execsql { PRAGMA mmap_size = 0 } db2
  execsql {
    CREATE TABLE t1(a, b);
    INSERT INTO t1 VALUES(1, 2);
    BEGIN;
    INSERT INTO t1 VALUES(3, 4);
  }
} {}
do_test lock5-none.2 {
  execsql { SELECT * FROM t1 }
} {1 2 3 4}
do_test lock5-none.3 {
  execsql { SELECT * FROM t1; } db2
} {1 2}
do_test lock5-none.4 {
  execsql { 
    BEGIN;
    SELECT * FROM t1;
  } db2
} {1 2}
do_test lock5-none.5 {
  execsql COMMIT
  execsql {SELECT * FROM t1} db2
} {1 2}

ifcapable memorymanage {
  if {[permutation]!="memsubsys1" && [permutation]!="memsubsys2"} { 
    do_test lock5-none.6 {
      sqlite3_release_memory 1000000
      execsql {SELECT * FROM t1} db2
    } {1 2 3 4}
  }
}

do_test lock5-none.X {
  db close
  db2 close
} {}

ifcapable lock_proxy_pragmas {
  set env(SQLITE_FORCE_PROXY_LOCKING) $::using_proxy
}

#####################################################################
reset_db
if {[permutation]!="inmemory_journal"} {
  
  # 1. Create a large database using the unix-dotfile VFS
  # 2. Write a large transaction to the db, so that the cache spills, but do
  #    not commit it.
  # 3. Make a copy of the database files on disk.
  # 4. Try to read from the copy using unix-dotfile VFS. This fails because
  #    the dotfile still exists, so SQLite thinks the database is locked.
  # 5. Remove the dotfile.
  # 6. Try to read the db again. This time, the old transaction is rolled
  #    back and the read permitted.
  #
  do_test 2.dotfile.1 {
    sqlite3 db test.db -vfs unix-dotfile
    execsql {
      PRAGMA cache_size = 10;
      CREATE TABLE t1(x, y, z);
      CREATE INDEX t1x ON t1(x);
      WITH s(i) AS (
        SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i<1000
      )
      INSERT INTO t1 SELECT hex(randomblob(20)), hex(randomblob(500)), i FROM s;
    }
  } {}
  
  do_execsql_test 2.dotfile.2 {
    BEGIN;
      UPDATE t1 SET z=z+1, x=hex(randomblob(20));
  }
  
  do_test 2.dotfile.3 {
    list                            \
      [file exists test.db]         \
      [file exists test.db-journal] \
      [file exists test.db.lock]
  } {1 1 1}
  
  do_test 2.dotfile.4 {
    forcecopy test.db test.db2
    forcecopy test.db-journal test.db2-journal
    file mkdir test.db2.lock
  
    sqlite3 db2 test.db2 -vfs unix-dotfile
    catchsql {
      SELECT count(*) FROM t1;
    } db2
  } {1 {database is locked}}
  
  do_test 2.dotfile.5 {
    file delete test.db2.lock
    execsql {
      PRAGMA integrity_check
    } db2
  } {ok}
  
  db2 close
  
  do_test 2.dotfile.6 {
    forcecopy test.db test.db2
    forcecopy test.db-journal test.db2-journal
  
    sqlite3 db2 file:test.db2?nolock=1 -vfs unix-dotfile -uri 1
    catchsql {
      SELECT count(*) FROM t1;
    } db2
  } {0 1000}
}

finish_test
