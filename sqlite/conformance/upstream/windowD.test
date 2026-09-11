# 2022 June 2
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
set testprefix windowD


do_execsql_test 1.0 {
  CREATE TABLE t0(c0 TEXT);
  CREATE VIEW v0(c0, c1) 
    AS SELECT CUME_DIST() OVER (PARTITION BY t0.c0), TRUE FROM t0;
  INSERT INTO t0 VALUES ('x');
}

do_execsql_test 1.1 {
  SELECT ('500') IS (v0.c1) FROM v0;
} {
  0
}

do_execsql_test 1.2 {
  SELECT (('500') IS (v0.c1)) FROM v0, t0;
} {
  0
}

do_execsql_test 1.2 {
  SELECT (('500') IS (v0.c1)) IS FALSE FROM v0;
} {
  1
}

do_execsql_test 1.3 {
  SELECT * FROM v0;
} {
  1.0 1
}

do_execsql_test 1.4 {
  SELECT * FROM v0 WHERE ('500' IS v0.c1) IS FALSE;
} {
  1.0 1
}

#-------------------------------------------------------------------------

reset_db
do_execsql_test 2.0 {
  CREATE TABLE t1(x);
  INSERT INTO t1 VALUES('value');
  CREATE VIEW v1(a, b, c, d) AS SELECT 1, 2, TRUE, FALSE FROM t1;
}

do_execsql_test 2.1 {
  SELECT 500 IS a, 500 IS b, 500 IS c, 500 IS d FROM v1
} {0 0 0 0}

do_execsql_test 2.2 {
  SELECT * FROM v1 WHERE 500 IS c;
} {}

do_execsql_test 2.3 {
  SELECT * FROM v1 WHERE 500 IS d;
} {}

do_execsql_test 2.4 {
  CREATE VIEW v2 AS SELECT max(x) OVER () AS a, TRUE AS c FROM t1;
}

do_execsql_test 2.5 {
  SELECT 500 IS c FROM v2;
} 0

do_execsql_test 2.6 {
  SELECT * FROM v2 WHERE 500 IS c;
} {}






finish_test

