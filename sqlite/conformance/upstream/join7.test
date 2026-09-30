# 2022-04-09
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
# This file implements tests for RIGHT and FULL OUTER JOINs.

set testdir [file dirname $argv0]
source $testdir/tester.tcl

foreach {id schema} {
  1 {
    CREATE TABLE t1(a INT, b INT);
    INSERT INTO t1 VALUES(1,2),(1,3),(1,4);
    CREATE INDEX t1a ON t1(a);
    CREATE TABLE t2(c INT, d INT);
    INSERT INTO t2 VALUES(3,33),(4,44),(5,55);
    CREATE INDEX t2c ON t2(c);
    CREATE VIEW dual(dummy) AS VALUES('x');
  }
  2 {
    CREATE TABLE t1(a INT, b INT);
    INSERT INTO t1 VALUES(1,2),(1,3),(1,4);
    CREATE INDEX t1ab ON t1(a,b);
    CREATE TABLE t2(c INT, d INT);
    INSERT INTO t2 VALUES(3,33),(4,44),(5,55);
    CREATE INDEX t2cd ON t2(c,d);
    CREATE VIEW dual(dummy) AS VALUES('x');
  }
  3 {
    CREATE TABLE t1(a INT, b INT);
    INSERT INTO t1 VALUES(1,2),(1,3),(1,4);
    CREATE INDEX t1a ON t1(a);
    CREATE TABLE t2(c INT, d INT PRIMARY KEY) WITHOUT ROWID;
    INSERT INTO t2 VALUES(3,33),(4,44),(5,55);
    CREATE INDEX t2c ON t2(c);
    CREATE VIEW dual(dummy) AS VALUES('x');
  }
  4 {
    CREATE TABLE t1(a INT, b INT);
    INSERT INTO t1 VALUES(1,2),(1,3),(1,4);
    CREATE TABLE t2(c INTEGER PRIMARY KEY, d INT);
    INSERT INTO t2 VALUES(3,33),(4,44),(5,55);
    CREATE VIEW dual(dummy) AS VALUES('x');
  }
  5 {
    CREATE TABLE t1(a INT, b INT);
    INSERT INTO t1 VALUES(1,2),(1,3),(1,4);
    CREATE TABLE t2(c INT PRIMARY KEY, d INT) WITHOUT ROWID;
    INSERT INTO t2 VALUES(3,33),(4,44),(5,55);
    CREATE VIEW dual(dummy) AS VALUES('x');
  }
  6 {
    CREATE TABLE t1(a INT, b INT);
    INSERT INTO t1 VALUES(1,2),(1,3),(1,4);
    CREATE VIEW t2(c,d) AS VALUES(3,33),(4,44),(5,55);
    CREATE VIEW dual(dummy) AS VALUES('x');
  }
  7 {
    CREATE VIEW t1(a,b) AS VALUES(1,2),(1,3),(1,4);
    CREATE TABLE t2(c INTEGER PRIMARY KEY, d INT);
    INSERT INTO t2 VALUES(3,33),(4,44),(5,55);
    CREATE VIEW dual(dummy) AS VALUES('x');
  }
  8 {
    CREATE TABLE t1(a INT, b INT);
    INSERT INTO t1 VALUES(1,2),(1,3),(1,4);
    CREATE TABLE t2(c INT, d INT);
    INSERT INTO t2 VALUES(3,33),(4,44),(5,55);
    CREATE VIEW dual(dummy) AS VALUES('x');
  }
  9 {
    CREATE TABLE t1(a INT, b INT);
    INSERT INTO t1 VALUES(1,2),(1,3),(1,4);
    CREATE TABLE t2a(c INTEGER PRIMARY KEY, i1 INT);
    CREATE TABLE t2b(i1 INTEGER PRIMARY KEY, d INT);
    CREATE VIEW t2(c,d) AS SELECT c, d FROM t2a NATURAL JOIN t2b;
    INSERT INTO t2a VALUES(3,93),(4,94),(5,95),(6,96),(7,97);
    INSERT INTO t2b VALUES(91,11),(92,22),(93,33),(94,44),(95,55);
    CREATE TABLE dual(dummy TEXT);
    INSERT INTO dual(dummy) VALUES('x');
  }
  10 {
    CREATE TABLE t1(a INT, b INT, PRIMARY KEY(a,b)) WITHOUT ROWID;
    INSERT INTO t1 VALUES(1,2),(1,3),(1,4);
    CREATE TABLE t2a(c INTEGER PRIMARY KEY, i1 INT);
    CREATE TABLE t2b(i1 INTEGER PRIMARY KEY, d INT);
    CREATE VIEW t2(c,d) AS SELECT c, d FROM t2a NATURAL JOIN t2b;
    INSERT INTO t2a VALUES(3,93),(4,94),(5,95),(6,96),(7,97);
    INSERT INTO t2b VALUES(91,11),(92,22),(93,33),(94,44),(95,55);
    CREATE TABLE dual(dummy TEXT);
    INSERT INTO dual(dummy) VALUES('x');
  }
} {
  reset_db
  db nullvalue NULL
  do_execsql_test join7-$id.setup $schema {}

  # Verified against PG-14 for case 1
  do_execsql_test join7-$id.10 {
    SELECT b, d FROM t1 FULL OUTER JOIN t2 ON b=c ORDER BY +b;
  } {
    NULL 55
    2    NULL
    3    33
    4    44
  }

  # Verified against PG-14 for case 1
  do_execsql_test join7-$id.20 {
    SELECT a, c FROM t1 FULL OUTER JOIN t2 ON b=c ORDER BY +b;
  } {
    NULL  5
    1     NULL
    1     3
    1     4
  }

  do_execsql_test join7-$id.30 {
    SELECT * FROM t1 FULL OUTER JOIN t2 ON b=c ORDER BY +b;
  } {
    NULL NULL 5    55
    1    2    NULL NULL
    1    3    3    33
    1    4    4    44
  }
  do_execsql_test join7-$id.31 {
    SELECT t1.*, t2.* FROM t2 FULL OUTER JOIN t1 ON b=c ORDER BY +b;
  } {
    NULL NULL 5    55
    1    2    NULL NULL
    1    3    3    33
    1    4    4    44
  }
  do_execsql_test join7-$id.32 {
    SELECT t1.*, t2.* FROM t2 FULL OUTER JOIN t1 ON b=c
     WHERE b=c
     ORDER BY +b;
  } {
    1    3    3    33
    1    4    4    44
  }
  do_execsql_test join7-$id.33 {
    SELECT t1.*, t2.* FROM t2 FULL OUTER JOIN t1 ON b=c
     WHERE b>0
     ORDER BY +b;
  } {
    1    2    NULL NULL
    1    3    3    33
    1    4    4    44
  }
  do_execsql_test join7-$id.34 {
    SELECT t1.*, t2.* FROM t2 FULL OUTER JOIN t1 ON b=c
     WHERE b>0 OR b IS NULL
     ORDER BY +b;
  } {
    NULL NULL 5    55
    1    2    NULL NULL
    1    3    3    33
    1    4    4    44
  }
  do_execsql_test join7-$id.35 {
    SELECT t1.*, t2.* FROM t2 FULL OUTER JOIN t1 ON b=c AND b>3 AND c>4
     ORDER BY coalesce(b,c,0);
  } {
    1    2    NULL NULL
    NULL NULL 3    33
    1    3    NULL NULL
    NULL NULL 4    44
    1    4    NULL NULL
    NULL NULL 5    55
  }
  do_execsql_test join7-$id.36 {
    SELECT t1.*, t2.* FROM t2 FULL OUTER JOIN t1 ON b=c AND b>3 WHERE c>4
     ORDER BY coalesce(b,c,0);
  } {
    NULL NULL 5    55
  }
  do_execsql_test join7-$id.37 {
    SELECT t1.*, t2.* FROM t2 FULL OUTER JOIN t1 ON b=c WHERE b>3 AND c>4
     ORDER BY coalesce(b,c,0);
  } {
  }
  do_execsql_test join7-$id.38 {
    SELECT t1.*, t2.* FROM t2 FULL OUTER JOIN t1 ON b=c WHERE b>3 OR c>4
     ORDER BY coalesce(b,c,0);
  } {
    1    4    4    44
    NULL NULL 5    55
  }
  do_execsql_test join7-$id.39 {
    SELECT t1.*, t2.* FROM t2 FULL OUTER JOIN t1 ON b=c AND (b>3 OR c>4)
     ORDER BY coalesce(b,c,0);
  } {
    1    2    NULL NULL
    NULL NULL 3    33
    1    3    NULL NULL
    1    4    4    44
    NULL NULL 5    55
  }
  do_execsql_test join7-$id.40 {
    SELECT * FROM t1 RIGHT OUTER JOIN t2 ON b=c ORDER BY +b;
  } {
    NULL NULL 5    55
    1    3    3    33
    1    4    4    44
  }
  do_execsql_test join7-$id.50 {
    SELECT t1.*, t2.* FROM t2 LEFT OUTER JOIN t1 ON b=c ORDER BY +b;
  } {
    NULL NULL 5    55
    1    3    3    33
    1    4    4    44
  }
  do_execsql_test join7-$id.60 {
    SELECT * FROM dual JOIN t1 ON true RIGHT OUTER JOIN t2 ON b=c ORDER BY +b;
  } {
    NULL NULL NULL 5    55
    x    1    3    3    33
    x    1    4    4    44
  }
  do_execsql_test join7-$id.70 {
    SELECT t1.*, t2.* 
      FROM t2 LEFT JOIN (dual JOIN t1 ON true) ON b=c ORDER BY +b;
  } {
    NULL NULL 5    55
    1    3    3    33
    1    4    4    44
  }
  do_execsql_test join7-$id.80 {
    SELECT * FROM dual CROSS JOIN t1 RIGHT OUTER JOIN t2 ON b=c ORDER BY +b;
  } {
    NULL NULL NULL 5    55
    x    1    3    3    33
    x    1    4    4    44
  }
  do_execsql_test join7-$id.81 {
    SELECT dual.*, t1.*, t2.*
      FROM t1 CROSS JOIN dual RIGHT OUTER JOIN t2 ON b=c ORDER BY +b;
  } {
    NULL NULL NULL 5    55
    x    1    3    3    33
    x    1    4    4    44
  }
  do_execsql_test join7-$id.90 {
    SELECT * FROM t1 LEFT OUTER JOIN t2 ON b=c ORDER BY +b;
  } {
    1    2    NULL NULL
    1    3    3    33
    1    4    4    44
  }
  do_execsql_test join7-$id.100 {
    SELECT * FROM t1 FULL OUTER JOIN t2 ON b=c AND a=1 ORDER BY +b;
  } {
    NULL NULL 5    55
    1    2    NULL NULL
    1    3    3    33
    1    4    4    44
  }
  do_execsql_test join7-$id.101 {
    SELECT t1.*, t2.* FROM t2 FULL OUTER JOIN t1 ON b=c AND a=1 ORDER BY +b;
  } {
    NULL NULL 5    55
    1    2    NULL NULL
    1    3    3    33
    1    4    4    44
  }

  # Verified against PG-14 for case 1
  do_execsql_test join7-$id.110 {
    SELECT * FROM t1 FULL OUTER JOIN t2 ON b=c WHERE a=1 ORDER BY +b;
  } {
    1    2    NULL NULL
    1    3    3    33
    1    4    4    44
  }

  do_execsql_test join7-$id.111 {
    SELECT t1.*, t2.* FROM t2 FULL OUTER JOIN t1 ON b=c WHERE a=1 ORDER BY +b;
  } {
    1    2    NULL NULL
    1    3    3    33
    1    4    4    44
  }

  # Verified against PG-14 for case 1
  do_execsql_test join7-$id.115 {
    SELECT * FROM t1 FULL OUTER JOIN t2 ON b=c
     WHERE a=1 OR a IS NULL ORDER BY +b;
  } {
    NULL NULL 5    55
    1    2    NULL NULL
    1    3    3    33
    1    4    4    44
  }

  do_execsql_test join7-$id.116 {
    SELECT t1.*, t2.* FROM t2 FULL OUTER JOIN t1 ON b=c
     WHERE a=1 OR a IS NULL ORDER BY +b;
  } {
    NULL NULL 5    55
    1    2    NULL NULL
    1    3    3    33
    1    4    4    44
  }

  # Verified against PG-14 for case 1:
  do_execsql_test join7-$id.120 {
    SELECT * FROM t1 FULL OUTER JOIN t2 ON b=c WHERE a IS NULL ORDER BY +d;
  } {
    NULL NULL 5    55
  }

  # Verified against PG-14 for case 1:
  do_execsql_test join7-$id.130 {
    SELECT * FROM t1 FULL OUTER JOIN t2 ON b=c AND d<=0 ORDER BY +b, +d;
  } {
    NULL NULL 3    33
    NULL NULL 4    44
    NULL NULL 5    55
    1    2    NULL NULL
    1    3    NULL NULL
    1    4    NULL NULL
  }

  # Verified against PG-14 for case 1:
  do_execsql_test join7-$id.140 {
    SELECT a, b, c, d
      FROM t2 FULL OUTER JOIN t1 ON b=c AND d<=0 ORDER BY +b, +d;
  } {
    NULL NULL 3    33
    NULL NULL 4    44
    NULL NULL 5    55
    1    2    NULL NULL
    1    3    NULL NULL
    1    4    NULL NULL
  }

  do_execsql_test join7-$id.141 {
    SELECT a, b, c, d
      FROM t2 FULL OUTER JOIN t1 ON b=c AND d<=0
     ORDER BY +b, +d LIMIT 2 OFFSET 2
  } {
    NULL NULL 5    55
    1    2    NULL NULL
  }
}  
finish_test
