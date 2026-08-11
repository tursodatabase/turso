# 2021-08-19
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
# This file implements regression tests for SQLite library.  The
# focus of this file is testing STRICT tables.
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix strict2

# PRAGMA integrity_check on a STRICT table should verify that
# all of the values are of the correct type.
#
do_execsql_test strict2-1.1 {
  CREATE TABLE t1(
    a INT,
    b INTEGER,
    c TEXT,
    d REAL,
    e BLOB
  ) STRICT;
  CREATE TABLE t1nn(
    a INT NOT NULL,
    b INTEGER NOT NULL,
    c TEXT NOT NULL,
    d REAL NOT NULL,
    e BLOB NOT NULL
  ) STRICT;
  CREATE TABLE t2(a,b,c,d,e);
  INSERT INTO t1(a,b,c,d,e) VALUES(1,1,'one',1.0,x'b1'),(2,2,'two',2.25,x'b2b2b2');
  PRAGMA writable_schema=on;
  UPDATE sqlite_schema SET rootpage=(SELECT rootpage FROM sqlite_schema WHERE name='t1');
} {}
db close
sqlite3 db test.db
do_execsql_test strict2-1.2 {
  PRAGMA quick_check('t1');
} {ok}
do_execsql_test strict2-1.3 {
  UPDATE t2 SET a=2.5 WHERE b=2;
  PRAGMA quick_check('t1');
} {{non-INT value in t1.a}}
do_execsql_test strict2-1.4 {
  UPDATE t2 SET a='xyz' WHERE b=2;
  PRAGMA quick_check('t1');
} {{non-INT value in t1.a}}
do_execsql_test strict2-1.5 {
  UPDATE t2 SET a=x'445566' WHERE b=2;
  PRAGMA quick_check('t1');
} {{non-INT value in t1.a}}
do_execsql_test strict2-1.6 {
  UPDATE t2 SET a=2.5 WHERE b=2;
  PRAGMA quick_check('t1nn');
} {{non-INT value in t1nn.a}}
do_execsql_test strict2-1.7 {
  UPDATE t2 SET a='xyz' WHERE b=2;
  PRAGMA quick_check('t1nn');
} {{non-INT value in t1nn.a}}
do_execsql_test strict2-1.8 {
  UPDATE t2 SET a=x'445566' WHERE b=2;
  PRAGMA quick_check('t1nn');
} {{non-INT value in t1nn.a}}

do_execsql_test strict2-1.13 {
  UPDATE t2 SET a=2 WHERE b=2;
  UPDATE t2 SET b=2.5 WHERE a=2;
  PRAGMA quick_check('t1');
} {{non-INTEGER value in t1.b}}
do_execsql_test strict2-1.14 {
  UPDATE t2 SET b='two' WHERE a=2;
  PRAGMA quick_check('t1');
} {{non-INTEGER value in t1.b}}
do_execsql_test strict2-1.15 {
  UPDATE t2 SET b=x'b0b1b2b3b4' WHERE a=2;
  PRAGMA quick_check('t1');
} {{non-INTEGER value in t1.b}}
do_execsql_test strict2-1.16 {
  UPDATE t2 SET b=NULL WHERE a=2;
  PRAGMA quick_check('t1');
} {ok}
do_execsql_test strict2-1.17 {
  UPDATE t2 SET b=2.5 WHERE a=2;
  PRAGMA quick_check('t1nn');
} {{non-INTEGER value in t1nn.b}}
do_execsql_test strict2-1.18 {
  UPDATE t2 SET b=NULL WHERE a=2;
  PRAGMA quick_check('t1nn');
} {{NULL value in t1nn.b}}

do_execsql_test strict2-1.23 {
  UPDATE t2 SET b=2 WHERE a=2;
  UPDATE t2 SET c=9 WHERE a=2;
  PRAGMA quick_check('t1');
} {{non-TEXT value in t1.c}}
do_execsql_test strict2-1.24 {
  UPDATE t2 SET c=9.5 WHERE a=2;
  PRAGMA quick_check('t1');
} {{non-TEXT value in t1.c}}
do_execsql_test strict2-1.25 {
  UPDATE t2 SET c=x'b0b1b2b3b4' WHERE a=2;
  PRAGMA quick_check('t1');
} {{non-TEXT value in t1.c}}

do_execsql_test strict2-1.33 {
  UPDATE t2 SET c='two' WHERE a=2;
  UPDATE t2 SET d=9 WHERE a=2;
  PRAGMA quick_check('t1');
} {ok}
do_execsql_test strict2-1.34 {
  UPDATE t2 SET d='nine' WHERE a=2;
  PRAGMA quick_check('t1');
} {{non-REAL value in t1.d}}
do_execsql_test strict2-1.35 {
  UPDATE t2 SET d=x'b0b1b2b3b4' WHERE a=2;
  PRAGMA quick_check('t1');
} {{non-REAL value in t1.d}}

do_execsql_test strict2-1.43 {
  UPDATE t2 SET d=2.5 WHERE a=2;
  UPDATE t2 SET e=9 WHERE a=2;
  PRAGMA quick_check('t1');
} {{non-BLOB value in t1.e}}
do_execsql_test strict2-1.44 {
  UPDATE t2 SET e=9.5 WHERE a=2;
  PRAGMA quick_check('t1');
} {{non-BLOB value in t1.e}}
do_execsql_test strict2-1.45 {
  UPDATE t2 SET e='hello' WHERE a=2;
  PRAGMA quick_check('t1');
} {{non-BLOB value in t1.e}}

do_execsql_test strict2-2.0 {
  DROP TABLE IF EXISTS t2;
  CREATE TABLE t2(a INT, b ANY) STRICT;
  INSERT INTO t2(a,b) VALUES(1,2),(3,4.5),(5,'six'),(7,x'8888'),(9,NULL);
  PRAGMA integrity_check(t2);
} {ok}

do_execsql_test strict2-3.0 {
  DROP TABLE IF EXISTS t1;
  CREATE TABLE t1(id ANY PRIMARY KEY, x TEXT);
  INSERT INTO t1 VALUES(1,2),('three','four'),(x'5555','six'),(NULL,'eight');
  PRAGMA writable_schema=ON;
  UPDATE sqlite_schema SET sql=(sql||'STRICT') WHERE name='t1';
  PRAGMA writable_schema=RESET;
  PRAGMA integrity_check(t1);
} {{NULL value in t1.id}}

finish_test
