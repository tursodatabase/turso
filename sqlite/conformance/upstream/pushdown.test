# 2017-04-29
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
# Test cases for the push-down optimizations.
#
#
# There are two different meanings for "push-down optimization".
#
#   (1)  "MySQL push-down" means that WHERE clause terms that can be
#        evaluated using only the index and without reference to the
#        table are run first, so that if they are false, unnecessary table
#        seeks are avoided.  See https://sqlite.org/src/info/d7bb79ed3a40419d
#        from 2017-04-29.
#
#   (2)  "WHERE-clause pushdown" means to push WHERE clause terms in
#        outer queries down into subqueries.  See
#        https://sqlite.org/src/info/6df18e949d367629 from 2015-06-02.
#
# This module started out as tests for MySQL push-down only.  But because
# of naming ambiguity, it has picked up test cases for WHERE-clause push-down
# over the years.
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix pushdown

do_execsql_test 1.0 {
  CREATE TABLE t1(a, b, c);
  INSERT INTO t1 VALUES(1, 'b1', 'c1');
  INSERT INTO t1 VALUES(2, 'b2', 'c2');
  INSERT INTO t1 VALUES(3, 'b3', 'c3');
  INSERT INTO t1 VALUES(4, 'b4', 'c4');
  CREATE INDEX i1 ON t1(a, c);
}

proc f {val} {
  lappend ::L $val
  return 0
}
db func f f 

do_test 1.1 {
  set L [list]
  execsql { SELECT * FROM t1 WHERE a=2 AND f(b) AND f(c) }
  set L
} {c2}

do_test 1.2 {
  set L [list]
  execsql { SELECT * FROM t1 WHERE a=3 AND f(c) AND f(b) }
  set L
} {c3}

do_execsql_test 1.3 {
  DROP INDEX i1;
  CREATE INDEX i1 ON t1(a, b);
}
do_test 1.4 {
  set L [list]
  execsql { SELECT * FROM t1 WHERE a=2 AND f(b) AND f(c) }
  set L
} {b2}

do_test 1.5 {
  set L [list]
  execsql { SELECT * FROM t1 WHERE a=3 AND f(c) AND f(b) }
  set L
} {b3}

#-----------------------------------------------

do_execsql_test 2.0 {
  CREATE TABLE u1(a, b, c);
  CREATE TABLE u2(x, y, z);

  INSERT INTO u1 VALUES('a1', 'b1', 'c1');
  INSERT INTO u2 VALUES('a1', 'b1', 'c1');
}

do_test 2.1 {
  set L [list]
  execsql {
    SELECT * FROM u1 WHERE f('one')=123 AND 123=(
      SELECT x FROM u2 WHERE x=a AND f('two')
    )
  }
  set L
} {one}

do_test 2.2 {
  set L [list]
  execsql {
    SELECT * FROM u1 WHERE 123=(
      SELECT x FROM u2 WHERE x=a AND f('two')
    ) AND f('three')=123
  }
  set L
} {three}

# 2022-11-25 dbsqlfuzz crash-3a548de406a50e896c1bf7142692d35d339d697f
# Disable the WHERE-clause push-down optimization for compound subqueries
# if any arm of the compound has an incompatible affinity.
#
reset_db
do_execsql_test 3.1 {
  CREATE TABLE t0(c0 INT);
  INSERT INTO t0 VALUES(0);
  CREATE TABLE t1_a(a INTEGER PRIMARY KEY, b TEXT);
  INSERT INTO t1_a VALUES(1,'one');
  CREATE TABLE t1_b(c INTEGER PRIMARY KEY, d TEXT);
  INSERT INTO t1_b VALUES(2,'two');
  CREATE VIEW v0 AS SELECT CAST(t0.c0 AS INTEGER) AS c0 FROM t0;
  CREATE VIEW v1(a,b) AS SELECT a, b FROM t1_a UNION ALL SELECT c, 0 FROM t1_b;
  SELECT v1.a, quote(v1.b), t0.c0 AS cd FROM t0 LEFT JOIN v0 ON v0.c0!=0,v1;
} {
  1 'one' 0
  2 0     0
}
do_execsql_test 3.2 {
  SELECT a, quote(b), cd FROM (
    SELECT v1.a, v1.b, t0.c0 AS cd FROM t0 LEFT JOIN v0 ON v0.c0!=0, v1
  ) WHERE a=2 AND b='0' AND cd=0;
} {}
do_execsql_test 3.3 {
  SELECT a, quote(b), cd FROM (
    SELECT v1.a, v1.b, t0.c0 AS cd FROM t0 LEFT JOIN v0 ON v0.c0!=0, v1
  ) WHERE a=1 AND b='one' AND cd=0;
} {1 'one' 0}
do_execsql_test 3.4 {
  SELECT a, quote(b), cd FROM (
    SELECT v1.a, v1.b, t0.c0 AS cd FROM t0 LEFT JOIN v0 ON v0.c0!=0, v1
  ) WHERE a=2 AND b=0 AND cd=0;
} {
  2 0     0
}

# 2023-02-22 https://sqlite.org/forum/forumpost/bcc4375032
# Performance regression caused by check-in [1ad41840c5e0fa70] from 2022-11-25.
# That check-in added a new restriction on push-down.  The new restriction is
# no longer necessary after check-in [27655c9353620aa5] from 2022-12-14.
#
do_execsql_test 3.5 {
  DROP TABLE IF EXISTS t1;
  CREATE TABLE t1(a INT, b INT, c TEXT, PRIMARY KEY(a,b)) WITHOUT ROWID;
  INSERT INTO t1(a,b,c) VALUES
    (1,100,'abc'),
    (2,200,'def'),
    (3,300,'abc');
  DROP TABLE IF EXISTS t2;
  CREATE TABLE t2(a INT, b INT, c TEXT, PRIMARY KEY(a,b)) WITHOUT ROWID;
  INSERT INTO t2(a,b,c) VALUES
    (1,110,'efg'),
    (2,200,'hij'),
    (3,330,'klm');
  CREATE VIEW v3 AS
    SELECT a, b, c FROM t1
    UNION ALL
    SELECT a, b, 'xyz' FROM t2;
  SELECT * FROM v3 WHERE a=2 AND b=200;
} {2 200 def 2 200 xyz}
do_eqp_test 3.6 {
  SELECT * FROM v3 WHERE a=2 AND b=200;
} {
  QUERY PLAN
  |--CO-ROUTINE v3
  |  `--COMPOUND QUERY
  |     |--LEFT-MOST SUBQUERY
  |     |  `--SEARCH t1 USING PRIMARY KEY (a=? AND b=?)
  |     `--UNION ALL
  |        `--SEARCH t2 USING PRIMARY KEY (a=? AND b=?)
  `--SCAN v3
}
#                       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
# We want both arms of the compound subquery to use the
# primary key.

# The following is a test of the count-of-view optimization.  This does
# not have anything to do with push-down.  It is here because this is a
# convenient place to put the test.
#
do_execsql_test 3.7 {
  SELECT count(*) FROM v3;
} 6
do_eqp_test 3.8 {
  SELECT count(*) FROM v3;
} {
  QUERY PLAN
  |--SCAN CONSTANT ROW
  |--SCALAR SUBQUERY xxxxxx
  |  `--SCAN t1
  `--SCALAR SUBQUERY xxxxxx
     `--SCAN t2
}
# ^^^^^^^^^^^^^^^^^^^^
# The query should be converted into:
#   SELECT (SELECT count(*) FROM t1)+(SELECT count(*) FROM t2)

# 2023-05-09 https://sqlite.org/forum/forumpost/a7d4be7fb6
# Restriction (9) on the WHERE-clause push-down optimization.
# 
reset_db
db null -
do_execsql_test 4.1 {
  CREATE TABLE t1(a INT);
  CREATE TABLE t2(b INT);
  CREATE TABLE t3(c INT);
  INSERT INTO t3(c) VALUES(3);
  CREATE TABLE t4(d INT);
  CREATE TABLE t5(e INT);
  INSERT INTO t5(e) VALUES(5);
  CREATE VIEW v6(f,g) AS SELECT d, e FROM t4 RIGHT JOIN t5 ON true;
  SELECT * FROM  t1 JOIN t2 ON false RIGHT JOIN t3 ON true CROSS JOIN v6;
} {- - 3 - 5}
do_execsql_test 4.2 {
  SELECT * FROM v6 JOIN t5 ON false RIGHT JOIN t3 ON true;
} {- - - 3}
do_execsql_test 4.3 {
  SELECT * FROM t1 JOIN t2 ON false JOIN v6 ON true RIGHT JOIN t3 ON true;
} {- - - - 3}

# 2023-05-15 https://sqlite.org/forum/forumpost/f3f546025a
# This is restriction (6) on sqlite3ExprIsSingleTableConstraint().
# That restriction (now) used to implement restriction (9) on push-down.
# It is used for other things too, so it is not purely a push-down
# restriction.  But it seems convenient to put it here.
#
reset_db
db null -
do_execsql_test 5.0 {
  CREATE TABLE t1(a INT);  INSERT INTO t1 VALUES(1);
  CREATE TABLE t2(b INT);  INSERT INTO t2 VALUES(2);
  CREATE TABLE t3(c INT);  INSERT INTO t3 VALUES(3);
  CREATE TABLE t4(d INT);  INSERT INTO t4 VALUES(4);
  CREATE TABLE t5(e INT);  INSERT INTO t5 VALUES(5);
  SELECT *
    FROM t1 JOIN t2 ON null RIGHT JOIN t3 ON true
          LEFT JOIN (t4 JOIN t5 ON d+1=e) ON d=4
   WHERE e>0;
} {- - 3 4 5}


# 2024-04-05
# Allow push-down of operators of the form "expr IN table".
#
reset_db
do_execsql_test 6.0 {
  CREATE TABLE t01(w,x,y,z);
  CREATE TABLE t02(w,x,y,z);
  CREATE VIEW t0(w,x,y,z) AS
    SELECT w,x,y,z FROM t01 UNION ALL SELECT w,x,y,z FROM t02;
  CREATE INDEX t01x ON t01(w,x,y);
  CREATE INDEX t02x ON t02(w,x,y);
  CREATE VIEW v1(k) AS VALUES(77),(88),(99);
  CREATE TABLE k1(k);
  INSERT INTO k1 SELECT * FROM v1;
}
do_eqp_test 6.1 {
  WITH k(n) AS (VALUES(77),(88),(99))
  SELECT max(z) FROM t0 WHERE w=123 AND x IN k AND y BETWEEN 44 AND 55;
} {
  QUERY PLAN
  |--CO-ROUTINE t0
  |  `--COMPOUND QUERY
  |     |--LEFT-MOST SUBQUERY
  |     |  |--SEARCH t01 USING INDEX t01x (w=? AND x=? AND y>? AND y<?)
  |     |  `--LIST SUBQUERY xxxxxx
  |     |     |--MATERIALIZE k
  |     |     |  `--SCAN 3 CONSTANT ROWS
  |     |     |--SCAN k
  |     |     `--CREATE BLOOM FILTER
  |     `--UNION ALL
  |        |--SEARCH t02 USING INDEX t02x (w=? AND x=? AND y>? AND y<?)
  |        `--REUSE LIST SUBQUERY xxxxxx
  |--SEARCH t0
  `--REUSE LIST SUBQUERY xxxxxx
}
# ^^^^--- The key feature above is that the SEARCH for each subquery
# uses all three fields of the index w, x, and y.  Prior to the push-down
# of "expr IN table", only the w term of the index would be used.  Similar
# for the following tests:
#
do_eqp_test 6.2 {
  SELECT max(z) FROM t0 WHERE w=123 AND x IN v1 AND y BETWEEN 44 AND 55;
} {
  QUERY PLAN
  |--CO-ROUTINE t0
  |  `--COMPOUND QUERY
  |     |--LEFT-MOST SUBQUERY
  |     |  |--SEARCH t01 USING INDEX t01x (w=? AND x=? AND y>? AND y<?)
  |     |  `--LIST SUBQUERY xxxxxx
  |     |     |--CO-ROUTINE v1
  |     |     |  `--SCAN 3 CONSTANT ROWS
  |     |     |--SCAN v1
  |     |     `--CREATE BLOOM FILTER
  |     `--UNION ALL
  |        |--SEARCH t02 USING INDEX t02x (w=? AND x=? AND y>? AND y<?)
  |        `--REUSE LIST SUBQUERY xxxxxx
  |--SEARCH t0
  `--REUSE LIST SUBQUERY xxxxxx
}
do_eqp_test 6.3 {
  SELECT max(z) FROM t0 WHERE w=123 AND x IN k1 AND y BETWEEN 44 AND 55;
} {
  QUERY PLAN
  |--CO-ROUTINE t0
  |  `--COMPOUND QUERY
  |     |--LEFT-MOST SUBQUERY
  |     |  |--SEARCH t01 USING INDEX t01x (w=? AND x=? AND y>? AND y<?)
  |     |  `--LIST SUBQUERY xxxxxx
  |     |     |--SCAN k1
  |     |     `--CREATE BLOOM FILTER
  |     `--UNION ALL
  |        |--SEARCH t02 USING INDEX t02x (w=? AND x=? AND y>? AND y<?)
  |        `--REUSE LIST SUBQUERY xxxxxx
  |--SEARCH t0
  `--REUSE LIST SUBQUERY xxxxxx
}

#-------------------------------------------------------------------------
reset_db
do_execsql_test 7.0 {
  CREATE  TABLE  t0_1(a INT , b INT, c INT);
  CREATE  TABLE  t0_2(a INT , b INT, c INT);

  INSERT INTO t0_1 (a, b, c) VALUES (1, 0, 1);
  INSERT INTO t0_2 (a, b, c) VALUES (1, 0, 1);

  CREATE  TABLE  empty1(x);
  CREATE  TABLE  empty2(y);
}

do_execsql_test 7.1 {
  SELECT t0_2.c
    FROM (SELECT '0000' AS c0 FROM empty2 RIGHT JOIN t0_1 ON 1) AS v0 
    LEFT JOIN empty1 ON v0.c0, t0_2 
    RIGHT JOIN (
           SELECT 5678 AS col0 FROM (SELECT 0)
    ) AS sub1 ON 1;
} {1}

do_execsql_test 7.2 {
  SELECT t0_2.c
    FROM (SELECT '0000' AS c0 FROM empty2 RIGHT JOIN t0_1 ON 1) AS v0 
    LEFT JOIN empty1 ON v0.c0, t0_2 
    RIGHT JOIN (
           SELECT 5678 AS col0 FROM (SELECT 0)
    ) AS sub1 ON 1 WHERE +t0_2.c;
} {1}

finish_test
