# 2022 June 6
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
set testprefix changes2

do_execsql_test 1.0 {
  CREATE TABLE some_table ( 
    id INTEGER NOT NULL, value VARCHAR(40) NOT NULL, PRIMARY KEY (id)
  );
  INSERT INTO some_table (id, value) VALUES (1, 'v1');
} {}

set ::stmt [sqlite3_prepare_v2 db {
  UPDATE some_table SET value='v2' WHERE id=1 RETURNING id
} -1 dummy]

do_test 1.1 {
  list [sqlite3_step $::stmt] [db changes]
} {SQLITE_ROW 1}

do_test 1.2 {
  list [sqlite3_step $::stmt] [db changes]
} {SQLITE_DONE 1}

sqlite3_reset $::stmt

do_execsql_test 1.2 {
  DROP TABLE some_table;
  CREATE TABLE some_table ( 
    id INTEGER NOT NULL, value VARCHAR(40) NOT NULL, PRIMARY KEY (id)
  );
  INSERT INTO some_table (id, value) VALUES (1, 'v1');
} {}

do_test 1.3 {
  list [sqlite3_step $::stmt] [db changes]
} {SQLITE_ROW 1}

do_test 1.4 {
  list [sqlite3_step $::stmt] [db changes]
} {SQLITE_DONE 1}

sqlite3_finalize $::stmt

#-------------------------------------------------------------------------
reset_db

do_execsql_test 2.0 {
  CREATE TABLE t1(a, b);
  CREATE TABLE log(t);
} {}

set ::stmt [sqlite3_prepare_v2 db {
  INSERT INTO log VALUES(changes() || ' changes')
} -1 dummy]

do_execsql_test 2.1 {
  INSERT INTO t1 VALUES (1, 'v1'), (2, 'v2');
} {}

do_test 2.2 {
  list [sqlite3_step $::stmt] [sqlite3_reset $::stmt]
} {SQLITE_DONE SQLITE_OK}

do_execsql_test 2.3 {
  CREATE TABLE t3(x);
}

do_execsql_test 2.2 {
  INSERT INTO t1 VALUES (3, 'v1'), (4, 'v2');
} {}

do_test 2.3 {
  list [sqlite3_step $::stmt] [sqlite3_reset $::stmt]
} {SQLITE_DONE SQLITE_OK}

sqlite3_finalize $::stmt

do_execsql_test 2.4 {
  SELECT * FROM log;
} {{2 changes} {2 changes}}

finish_test
