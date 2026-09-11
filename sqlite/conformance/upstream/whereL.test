# 2018-07-26
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
# focus of this file is testing the WHERE-clause constant propagation
# optimization.
#
set testdir [file dirname $argv0]
source $testdir/tester.tcl
set ::testprefix whereL

do_execsql_test 100 {
  CREATE TABLE t1(a INT PRIMARY KEY, b, c, d, e);
  CREATE TABLE t2(a INT PRIMARY KEY, f, g, h, i);
  CREATE TABLE t3(a INT PRIMARY KEY, j, k, l, m);
  CREATE VIEW v4 AS SELECT * FROM t2 UNION ALL SELECT * FROM t3;
}
do_eqp_test 110 {
  SELECT * FROM t1, v4 WHERE t1.a=?1 AND v4.a=t1.a;
} {
  QUERY PLAN
  `--COMPOUND QUERY
     |--LEFT-MOST SUBQUERY
     |  |--SEARCH t1 USING INDEX sqlite_autoindex_t1_1 (a=?)
     |  `--SEARCH t2 USING INDEX sqlite_autoindex_t2_1 (a=?)
     `--UNION ALL
        |--SEARCH t1 USING INDEX sqlite_autoindex_t1_1 (a=?)
        `--SEARCH t3 USING INDEX sqlite_autoindex_t3_1 (a=?)
}

# The scan of the t1 table goes first since that enables the ORDER BY
# sort to be omitted.  This would not be possible without constant
# propagation because without it the t1 table would depend on t3.
#
do_eqp_test 120 {
  SELECT * FROM t1, t2, t3
   WHERE t1.a=t2.a AND t2.a=t3.j AND t3.j=5
  ORDER BY t1.a;
} {
  QUERY PLAN
  |--SEARCH t1 USING INDEX sqlite_autoindex_t1_1 (a=?)
  |--SEARCH t2 USING INDEX sqlite_autoindex_t2_1 (a=?)
  `--SCAN t3
}
do_eqp_test 121 {
  SELECT * FROM t1, t2, t3
   WHERE t1.a=t2.a AND t2.a=t3.j AND t3.j=abs(5)
  ORDER BY t1.a;
} {
  QUERY PLAN
  |--SEARCH t1 USING INDEX sqlite_autoindex_t1_1 (a=?)
  |--SEARCH t2 USING INDEX sqlite_autoindex_t2_1 (a=?)
  `--SCAN t3
}

# The sqlite3ExprIsConstant() routine does not believe that
# the expression "coalesce(5,random())" is constant.  So the
# optimization does not apply in this case.
# 
sqlite3_create_function db
do_eqp_test 122 {
  SELECT * FROM t1, t2, t3
   WHERE t1.a=t2.a AND t2.a=t3.j AND t3.j=coalesce(5,random())
  ORDER BY t1.a;
} {
  QUERY PLAN
  |--SCAN t3
  |--SEARCH t1 USING INDEX sqlite_autoindex_t1_1 (a=?)
  |--SEARCH t2 USING INDEX sqlite_autoindex_t2_1 (a=?)
  `--USE TEMP B-TREE FOR ORDER BY
}

# Constant propagation in the face of collating sequences:
#
do_execsql_test 200 {
  CREATE TABLE c3(x COLLATE binary, y COLLATE nocase, z COLLATE binary);
  CREATE INDEX c3x ON c3(x);
  INSERT INTO c3 VALUES('ABC', 'ABC', 'abc');
  SELECT * FROM c3 WHERE x=y AND y=z AND z='abc';
} {ABC ABC abc}

# If the constants are blindly propagated, as shown in the following
# query, the wrong answer results:
#
do_execsql_test 201 {
  SELECT * FROM c3 WHERE x='abc' AND y='abc' AND z='abc';
} {}

# Constant propagation caused an incorrect answer in the following
# query.  (Reported by Bentley system on 2018-08-09.)
#
do_execsql_test 300 {
  CREATE TABLE A(id INTEGER PRIMARY KEY, label TEXT);
  CREATE TABLE B(id INTEGER PRIMARY KEY, label TEXT, Aid INTEGER);
  CREATE TABLE C(
    id INTEGER PRIMARY KEY,
    xx INTEGER NOT NULL,
    yy INTEGER,
    zz INTEGER
  );
  CREATE UNIQUE INDEX x2 ON C(yy);
  CREATE UNIQUE INDEX x4 ON C(yy, zz);
  INSERT INTO A(id) VALUES(1);
  INSERT INTO B(id) VALUES(2);
  INSERT INTO C(id,xx,yy,zz) VALUES(99,50,1,2);
  SELECT 1
    FROM A,
         (SELECT id,xx,yy,zz FROM C) subq,
         B
   WHERE A.id='1'
     AND A.id=subq.yy
     AND B.id=subq.zz;
} {1}  
do_execsql_test 301 {
  SELECT 1
    FROM A,
         (SELECT id,xx,yy,zz FROM C) subq,
         B
   WHERE A.id=1
     AND A.id=subq.yy
     AND B.id=subq.zz;
} {1}  
do_execsql_test 302 {
  SELECT 1
    FROM A,
         (SELECT id,yy,zz FROM C) subq,
         B
   WHERE A.id='1'
     AND A.id=subq.yy
     AND B.id=subq.zz;
} {1}  

# 2018-10-25: Ticket [cf5ed20f]
# Incorrect join result with duplicate WHERE clause constraint.
#
do_execsql_test 400 {
  CREATE TABLE x(a, b, c);
  CREATE TABLE y(a, b);
  INSERT INTO x VALUES (1, 0, 1);
  INSERT INTO y VALUES (1, 2);
  SELECT x.a FROM x JOIN y ON x.c = y.a WHERE x.b = 1 AND x.b = 1;
} {}

# 2020-01-07: ticket 82ac75ba0093e5dc
# Incorrect join result due to mishandling of affinity in constant
# propagation.
#
reset_db
do_execsql_test 500 {
  PRAGMA automatic_index=OFF;
  CREATE TABLE t0(c0);
  INSERT INTO t0 VALUES('0');
  CREATE VIEW v0(c0) AS SELECT CAST(0 AS INT) FROM t0;
  SELECT 200, * FROM t0, v0 WHERE 0 = t0.c0 AND t0.c0 = v0.c0;
} {}
do_execsql_test 510 {
  SELECT 200, * FROM t0, v0 WHERE t0.c0 = 0 AND t0.c0 = v0.c0;
} {}
do_execsql_test 520 {
  SELECT 200, * FROM t0, v0 WHERE 0 = t0.c0 AND v0.c0 = t0.c0;
} {}
do_execsql_test 530 {
  SELECT 200, * FROM t0, v0 WHERE t0.c0 = 0 AND v0.c0 = t0.c0;
} {}

# 2020-02-13: ticket 1dcb4d44964846ad
# A problem introduced while making optimizations on the fixes above.
#
reset_db
do_execsql_test 600 {
  CREATE TABLE t1(x TEXT);
  CREATE TABLE t2(y TEXT);
  INSERT INTO t1 VALUES('good'),('bad');
  INSERT INTO t2 VALUES('good'),('bad');
  SELECT * FROM t1 JOIN t2 ON x=y
   WHERE x='good' AND y='good';
} {good good}

# 2020-04-24: Another test case for the previous (1dcb4d44964846ad)
# ticket.  The test case comes from 
# https://stackoverflow.com/questions/61399253/sqlite3-different-result-in-console-compared-to-python-script/
# Output verified against postgresql.
#
do_execsql_test 610 {
  CREATE TABLE tableA(
    ID           int,
    RunYearMonth int
  );
  INSERT INTO tableA VALUES(1,202003),(2,202003),(3,202003),(4,202004),
    (5,202004),(6,202004),(7,202004),(8,202004);
  CREATE TABLE tableB (
    ID           int,
    RunYearMonth int
  );
  INSERT INTO tableB VALUES(1,202004),(2,202004),(3,202004),(4,202004),
  (5,202004);
  SELECT *
   FROM (
        SELECT *
        FROM tableA
        WHERE RunYearMonth = 202004
    ) AS A
    INNER JOIN (
        SELECT *
        FROM tableB
        WHERE RunYearMonth = 202004
    ) AS B
    ON A.ID = B.ID
    AND A.RunYearMonth = B.RunYearMonth;
} {4 202004 4 202004 5 202004 5 202004}

# 2023-02-10 https://sqlite.org/forum/forumpost/0a539c76db3b9e29
# The original constant propagation implementation caused a performance
# regression.  Because "abs(v)" was rewritten into "abs(1)" it no longer
# matches the indexed column and the index is not used.
# 
reset_db
do_execsql_test 700 {
  CREATE TABLE t1(v INTEGER);
  WITH RECURSIVE c(x) AS (VALUES(-10) UNION ALL SELECT x+1 FROM c WHERE x<10)
    INSERT INTO t1(v) SELECT x FROM c;
  CREATE INDEX idx ON t1( abs(v) );
  SELECT v FROM t1 WHERE abs(v)=1 and v=1;
} 1
do_eqp_test 710 {
  SELECT v FROM t1 WHERE abs(v)=1 and v=1;
} {
  QUERY PLAN
  `--SEARCH t1 USING INDEX idx (<expr>=?)
}

# 2024-03-07 https://sqlite.org/forum/forumpost/ecdfc02339
# A refinement is needed to the enhancements tested by the prior test case
# to avoid another problem with indexes on constant expressions.
#
reset_db
db null NULL
do_execsql_test 800 {
  CREATE TABLE t0(c0, c1);
  CREATE TABLE t1(c2);
  CREATE INDEX i0 ON t1(NULL);
  INSERT INTO t1(c2) VALUES (0.2);
  CREATE VIEW v0(c3) AS SELECT DISTINCT c2 FROM t1;
  SELECT * FROM v0 LEFT JOIN t0 ON c3<NULL LEFT JOIN t1 ON 1;
} {0.2 NULL NULL 0.2}
do_execsql_test 810 {
  SELECT * FROM v0 LEFT JOIN t0 ON c3<NULL LEFT JOIN t1 ON 1 WHERE c2/0.1;
} {0.2 NULL NULL 0.2}

#-------------------------------------------------------------------------
# 2025-04-10 https://sqlite.org/forum/forumpost/0109bca824
reset_db

do_execsql_test 900 {
  SELECT * FROM (SELECT 1.0 AS abc) WHERE abc=1;
} {1.0}
do_execsql_test 910 {
  SELECT * FROM (SELECT 1.0 AS abc) WHERE abc LIKE '1.0';
} {1.0}
do_execsql_test 920 {
  SELECT * FROM (SELECT 1.0 AS abc) WHERE abc=1 AND abc LIKE '1.0';
} {1.0}

do_execsql_test 930 {
  CREATE TABLE IF NOT EXISTS t0 (c0 BLOB);
  CREATE TABLE IF NOT EXISTS t1 (c0 INTEGER);

  INSERT INTO t1 VALUES ('1');
  INSERT INTO t0 VALUES (''), (''), ('2');
}

do_execsql_test 940 {
  SELECT *
    FROM (SELECT 0.0 AS col_0) as subQuery
    LEFT JOIN t0 ON ((CASE ''
          WHEN t0.c0 THEN subQuery.col_0
          ELSE (t0.c0) END) LIKE (((((subQuery.col_0))))))
    LEFT JOIN t1 ON ((subQuery.col_0) == (false));
} {0.0 {} 1 0.0 {} 1}

do_execsql_test 950 {
  SELECT *
    FROM (SELECT 0.0 AS col_0) as subQuery
    LEFT JOIN t0 ON ((CASE ''
          WHEN t0.c0 THEN subQuery.col_0
          ELSE (t0.c0) END) LIKE (((((subQuery.col_0))))))
    LEFT JOIN t1 ON ((subQuery.col_0) == (false)) WHERE t1.c0;
} {0.0 {} 1 0.0 {} 1}

finish_test
