# 2021-02-22
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
# focus of this file is the MATERIALIZED hint to common table expressions
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set ::testprefix with6

ifcapable {!cte} {
  finish_test
  return
}

do_execsql_test 100 {
  WITH c(x) AS (VALUES(0),(1))
  SELECT c1.x||c2.x||c3.x FROM c c1, c c2, c c3;
} {000 001 010 011 100 101 110 111}
do_eqp_test 101 {
  WITH c(x) AS (VALUES(0),(1))
  SELECT c1.x||c2.x||c3.x FROM c c1, c c2, c c3;
} {
  QUERY PLAN
  |--MATERIALIZE c
  |  `--SCAN 2 CONSTANT ROWS
  |--SCAN c1
  |--SCAN c2
  `--SCAN c3
}

do_execsql_test 110 {
  WITH c(x) AS MATERIALIZED (VALUES(0),(1))
  SELECT c1.x||c2.x||c3.x FROM c c1, c c2, c c3;
} {000 001 010 011 100 101 110 111}
do_eqp_test 111 {
  WITH c(x) AS MATERIALIZED (VALUES(0),(1))
  SELECT c1.x||c2.x||c3.x FROM c c1, c c2, c c3;
} {
  QUERY PLAN
  |--MATERIALIZE c
  |  `--SCAN 2 CONSTANT ROWS
  |--SCAN c1
  |--SCAN c2
  `--SCAN c3
}

# Even though the CTE is not materialized, the self-join optimization
# kicks in and does the materialization for us.
#
do_execsql_test 120 {
  WITH c(x) AS NOT MATERIALIZED (VALUES(0),(1))
  SELECT c1.x||c2.x||c3.x FROM c c1, c c2, c c3;
} {000 001 010 011 100 101 110 111}
do_eqp_test 121 {
  WITH c(x) AS NOT MATERIALIZED (VALUES(0),(1))
  SELECT c1.x||c2.x||c3.x FROM c c1, c c2, c c3;
} {
  QUERY PLAN
  |--MATERIALIZE c
  |  `--SCAN 2 CONSTANT ROWS
  |--SCAN c1
  |--SCAN c2
  `--SCAN c3
}

do_execsql_test 130 {
  WITH c(x) AS NOT MATERIALIZED (VALUES(0),(1))
  SELECT c1.x||c2.x||c3.x
    FROM (SELECT x FROM c LIMIT 5) AS c1,
         (SELECT x FROM c LIMIT 5) AS c2,
         (SELECT x FROM c LIMIT 5) AS c3;
} {000 001 010 011 100 101 110 111}
do_eqp_test 131 {
  WITH c(x) AS NOT MATERIALIZED (VALUES(0),(1))
  SELECT c1.x||c2.x||c3.x
    FROM (SELECT x FROM c LIMIT 5) AS c1,
         (SELECT x FROM c LIMIT 5) AS c2,
         (SELECT x FROM c LIMIT 5) AS c3;
} {
  QUERY PLAN
  |--CO-ROUTINE c1
  |  |--CO-ROUTINE c
  |  |  `--SCAN 2 CONSTANT ROWS
  |  `--SCAN c
  |--MATERIALIZE c2
  |  |--CO-ROUTINE c
  |  |  `--SCAN 2 CONSTANT ROWS
  |  `--SCAN c
  |--MATERIALIZE c3
  |  |--CO-ROUTINE c
  |  |  `--SCAN 2 CONSTANT ROWS
  |  `--SCAN c
  |--SCAN c1
  |--SCAN c2
  `--SCAN c3
}

# The (SELECT x FROM c LIMIT N) subqueries get materialized once each.
# Show multiple materializations are shown.  But there is only one
# materialization for c, shown by the "SCAN 2 CONSTANT ROWS" line.
#
do_execsql_test 140 {
  WITH c(x) AS MATERIALIZED (VALUES(0),(1))
  SELECT c1.x||c2.x||c3.x
    FROM (SELECT x FROM c LIMIT 5) AS c1,
         (SELECT x FROM c LIMIT 6) AS c2,
         (SELECT x FROM c LIMIT 7) AS c3;
} {000 001 010 011 100 101 110 111}
do_eqp_test 141 {
  WITH c(x) AS MATERIALIZED (VALUES(0),(1))
  SELECT c1.x||c2.x||c3.x
    FROM (SELECT x FROM c LIMIT 5) AS c1,
         (SELECT x FROM c LIMIT 6) AS c2,
         (SELECT x FROM c LIMIT 7) AS c3;
} {
  QUERY PLAN
  |--CO-ROUTINE c1
  |  |--MATERIALIZE c
  |  |  `--SCAN 2 CONSTANT ROWS
  |  `--SCAN c
  |--MATERIALIZE c2
  |  `--SCAN c
  |--MATERIALIZE c3
  |  `--SCAN c
  |--SCAN c1
  |--SCAN c2
  `--SCAN c3
}

do_execsql_test 150 {
  WITH c(x) AS (VALUES(0),(1))
  SELECT c1.x||c2.x||c3.x
    FROM (SELECT x FROM c LIMIT 5) AS c1,
         (SELECT x FROM c LIMIT 6) AS c2,
         (SELECT x FROM c LIMIT 7) AS c3;
} {000 001 010 011 100 101 110 111}
do_eqp_test 151 {
  WITH c(x) AS (VALUES(0),(1))
  SELECT c1.x||c2.x||c3.x
    FROM (SELECT x FROM c LIMIT 5) AS c1,
         (SELECT x FROM c LIMIT 6) AS c2,
         (SELECT x FROM c LIMIT 7) AS c3;
} {
  QUERY PLAN
  |--CO-ROUTINE c1
  |  |--MATERIALIZE c
  |  |  `--SCAN 2 CONSTANT ROWS
  |  `--SCAN c
  |--MATERIALIZE c2
  |  `--SCAN c
  |--MATERIALIZE c3
  |  `--SCAN c
  |--SCAN c1
  |--SCAN c2
  `--SCAN c3
}

do_execsql_test 160 {
  WITH c(x) AS (VALUES(0),(1))
  SELECT c2.x + 100*(SELECT sum(x+1) FROM c WHERE c.x<=c2.x)
    FROM c AS c2 WHERE c2.x<10;
} {100 301}
do_eqp_test 161 {
  WITH c(x) AS (VALUES(0),(1))
  SELECT c2.x + 100*(SELECT sum(x+1) FROM c WHERE c.x<=c2.x)
    FROM c AS c2 WHERE c2.x<10;
} {
  QUERY PLAN
  |--MATERIALIZE c
  |  `--SCAN 2 CONSTANT ROWS
  |--SCAN c2
  `--CORRELATED SCALAR SUBQUERY xxxxxx
     `--SCAN c
}

do_execsql_test 170 {
  WITH c(x) AS NOT MATERIALIZED (VALUES(0),(1))
  SELECT c2.x + 100*(SELECT sum(x+1) FROM c WHERE c.x<=c2.x)
    FROM c AS c2 WHERE c2.x<10;
} {100 301}
do_eqp_test 171 {
  WITH c(x) AS NOT MATERIALIZED (VALUES(0),(1))
  SELECT c2.x + 100*(SELECT sum(x+1) FROM c WHERE c.x<=c2.x)
    FROM c AS c2 WHERE c2.x<10;
} {
  QUERY PLAN
  |--CO-ROUTINE c
  |  `--SCAN 2 CONSTANT ROWS
  |--SCAN c2
  `--CORRELATED SCALAR SUBQUERY xxxxxx
     |--CO-ROUTINE c
     |  `--SCAN 2 CONSTANT ROWS
     `--SCAN c
}


do_execsql_test 200 {
  CREATE TABLE t1(x);
  INSERT INTO t1(x) VALUES(4);
  CREATE VIEW t2(y) AS
    WITH c(z) AS (VALUES(4),(5),(6))
    SELECT c1.z+c2.z*100+t1.x*10000
      FROM t1,
           (SELECT z FROM c LIMIT 5) AS c1,
           (SELECT z FROM c LIMIT 5) AS c2;
  SELECT y FROM t2 ORDER BY y;
} {40404 40405 40406 40504 40505 40506 40604 40605 40606}
do_execsql_test 210 {
  DROP VIEW t2;
  CREATE VIEW t2(y) AS
    WITH c(z) AS NOT MATERIALIZED (VALUES(4),(5),(6))
    SELECT c1.z+c2.z*100+t1.x*10000
      FROM t1,
           (SELECT z FROM c LIMIT 5) AS c1,
           (SELECT z FROM c LIMIT 5) AS c2;
  SELECT y FROM t2 ORDER BY y;
} {40404 40405 40406 40504 40505 40506 40604 40605 40606}
do_eqp_test 211 {
  SELECT y FROM t2 ORDER BY y;
} {
  QUERY PLAN
  |--CO-ROUTINE c1
  |  |--CO-ROUTINE c
  |  |  `--SCAN 3 CONSTANT ROWS
  |  `--SCAN c
  |--MATERIALIZE c2
  |  |--CO-ROUTINE c
  |  |  `--SCAN 3 CONSTANT ROWS
  |  `--SCAN c
  |--SCAN c1
  |--SCAN c2
  |--SCAN t1
  `--USE TEMP B-TREE FOR ORDER BY
}
do_execsql_test 220 {
  DROP VIEW t2;
  CREATE VIEW t2(y) AS
    WITH c(z) AS MATERIALIZED (VALUES(4),(5),(6))
    SELECT c1.z+c2.z*100+t1.x*10000
      FROM t1,
           (SELECT z FROM c LIMIT 5) AS c1,
           (SELECT z FROM c LIMIT 5) AS c2;
  SELECT y FROM t2 ORDER BY y;
} {40404 40405 40406 40504 40505 40506 40604 40605 40606}

# 2022-04-22: Do not allow flattening of a MATERIALIZED CTE into
# an outer query.
#
reset_db
db null -
do_execsql_test 300 {
  CREATE TABLE t2(a INT,b INT,d INT); INSERT INTO t2 VALUES(4,5,6),(7,8,9);
  CREATE TABLE t3(a INT,b INT,e INT); INSERT INTO t3 VALUES(3,3,3),(8,8,8);
} {}
do_execsql_test 310 {
  WITH t23 AS MATERIALIZED (SELECT * FROM t2 FULL JOIN t3 USING(b))
  SELECT * FROM t23;
} {
  4 5 6 - -
  7 8 9 8 8
  - 3 - 3 3
}
do_eqp_test 311 {
  WITH t23 AS MATERIALIZED (SELECT * FROM t2 FULL JOIN t3 USING(b))
  SELECT * FROM t23;
} {
  QUERY PLAN
  |--MATERIALIZE t23
  |  |--SCAN t2
  |  |--SCAN t3 LEFT-JOIN
  |  `--RIGHT-JOIN t3
  |     `--SCAN t3
  `--SCAN t23
}
do_execsql_test 320 {
  WITH t23 AS NOT MATERIALIZED (SELECT * FROM t2 FULL JOIN t3 USING(b))
  SELECT * FROM t23;
} {
  4 5 6 - -
  7 8 9 8 8
  - 3 - 3 3
}
do_eqp_test 321 {
  WITH t23 AS NOT MATERIALIZED (SELECT * FROM t2 FULL JOIN t3 USING(b))
  SELECT * FROM t23;
} {
  QUERY PLAN
  |--SCAN t2
  |--SCAN t3 LEFT-JOIN
  `--RIGHT-JOIN t3
     `--SCAN t3
}
do_execsql_test 330 {
  WITH t23 AS (SELECT * FROM t2 FULL JOIN t3 USING(b))
  SELECT * FROM t23;
} {
  4 5 6 - -
  7 8 9 8 8
  - 3 - 3 3
}
do_eqp_test 331 {
  WITH t23 AS (SELECT * FROM t2 FULL JOIN t3 USING(b))
  SELECT * FROM t23;
} {
  QUERY PLAN
  |--SCAN t2
  |--SCAN t3 LEFT-JOIN
  `--RIGHT-JOIN t3
     `--SCAN t3
}

# 2023-02-01
# https://sqlite.org/forum/forumpost/1d571c02963355ed
#
# Just because a CTE is used more than once, does not mean it should be
# marked with M10d_Yes and hence prohibited from participating in the
# query flattening optimization.
#
# Updated 2025-01-02.
# https://sqlite.org/forum/forumpost/8f38fc9878a92aa9
#
# The same optimization that made Grunthos's query fast made
# Jean-Noël Mayor's query slow.  Bummer.
#
reset_db
db eval {
  CREATE TABLE raw(country,date,total,delta, UNIQUE(country,date));
}
do_eqp_test 400 {
  with recursive
    init(country, date, fin) AS (SELECT country, min(date), max(date) FROM raw WHERE total > 0 GROUP BY country),
    src(country, date) AS (SELECT raw.country, raw.date
        FROM raw JOIN init i on raw.country = i.country AND raw.date > i.date
        ORDER BY raw.country, raw.date),
    vals(country, date, x, y) AS (SELECT src.country, src.date, julianday(raw.date) - julianday(src.date), log(delta+1)
        FROM src JOIN raw on raw.country = src.country AND raw.date > date(src.date,'-7 days') AND raw.date <= src.date AND delta >= 0),
    sums(country, date, x2, x, n, xy, y) AS (SELECT country, date, sum(x*x*1.0), sum(x*1.0), sum(1.0), sum(x*y*1.0), sum(y*1.0) FROM vals GROUP BY 1, 2),
    mult(country, date, m) AS (SELECT country, date, 1.0/(x2 * n - x * x) FROM sums),
    inv(country, date, a,b,c,d) AS (SELECT mult.country, mult.date, n * m, -x * m, -x * m, x2 * m
        FROM mult JOIN sums on sums.country=mult.country AND mult.date=sums.date),
    fit(country, date, a, b) AS (SELECT inv.country, inv.date, a * xy + b * y, c * xy + d * y
        FROM inv
        JOIN mult on mult.country = inv.country AND mult.date = inv.date
        JOIN sums on sums.country = mult.country AND sums.date = mult.date
  )
  SELECT *, nFin/nPrev - 1 AS growth, log(2)/log(nFin/nPrev) AS doubling
    FROM (SELECT f.*, exp(b) - 1 AS nFin, exp(a* (-1) + b) - 1 AS nPrev
            FROM fit f JOIN init i on i.country = f.country AND f.date <= date(i.fin,'-3 days'))
   WHERE nPrev > 0 AND nFin > 0;
} {
  QUERY PLAN
  |--MATERIALIZE sums
  |  |--MATERIALIZE src
  |  |  |--MATERIALIZE init
  |  |  |  `--SCAN raw USING INDEX sqlite_autoindex_raw_1
  |  |  |--SCAN i
  |  |  |--SEARCH raw USING COVERING INDEX sqlite_autoindex_raw_1 (country=? AND date>?)
  |  |  `--USE TEMP B-TREE FOR ORDER BY
  |  |--SCAN src
  |  `--SEARCH raw USING INDEX sqlite_autoindex_raw_1 (country=? AND date>? AND date<?)
  |--SCAN sums
  |--BLOOM FILTER ON sums (country=? AND date=?)
  |--SEARCH sums USING AUTOMATIC COVERING INDEX (country=? AND date=?)
  |--BLOOM FILTER ON sums (country=? AND date=?)
  |--SEARCH sums USING AUTOMATIC COVERING INDEX (country=? AND date=?)
  |--BLOOM FILTER ON sums (country=? AND date=?)
  |--SEARCH sums USING AUTOMATIC COVERING INDEX (country=? AND date=?)
  |--BLOOM FILTER ON i (country=?)
  `--SEARCH i USING AUTOMATIC COVERING INDEX (country=?)
}
optimization_control db order-by-subquery off
db cache flush
do_eqp_test 410 {
  with recursive
    init(country, date, fin) AS (SELECT country, min(date), max(date) FROM raw WHERE total > 0 GROUP BY country),
    src(country, date) AS (SELECT raw.country, raw.date
        FROM raw JOIN init i on raw.country = i.country AND raw.date > i.date
        ORDER BY raw.country, raw.date),
    vals(country, date, x, y) AS (SELECT src.country, src.date, julianday(raw.date) - julianday(src.date), log(delta+1)
        FROM src JOIN raw on raw.country = src.country AND raw.date > date(src.date,'-7 days') AND raw.date <= src.date AND delta >= 0),
    sums(country, date, x2, x, n, xy, y) AS (SELECT country, date, sum(x*x*1.0), sum(x*1.0), sum(1.0), sum(x*y*1.0), sum(y*1.0) FROM vals GROUP BY 1, 2),
    mult(country, date, m) AS (SELECT country, date, 1.0/(x2 * n - x * x) FROM sums),
    inv(country, date, a,b,c,d) AS (SELECT mult.country, mult.date, n * m, -x * m, -x * m, x2 * m
        FROM mult JOIN sums on sums.country=mult.country AND mult.date=sums.date),
    fit(country, date, a, b) AS (SELECT inv.country, inv.date, a * xy + b * y, c * xy + d * y
        FROM inv
        JOIN mult on mult.country = inv.country AND mult.date = inv.date
        JOIN sums on sums.country = mult.country AND sums.date = mult.date
  )
  SELECT *, nFin/nPrev - 1 AS growth, log(2)/log(nFin/nPrev) AS doubling
    FROM (SELECT f.*, exp(b) - 1 AS nFin, exp(a* (-1) + b) - 1 AS nPrev
            FROM fit f JOIN init i on i.country = f.country AND f.date <= date(i.fin,'-3 days'))
   WHERE nPrev > 0 AND nFin > 0;
} {
  QUERY PLAN
  |--MATERIALIZE sums
  |  |--MATERIALIZE src
  |  |  |--MATERIALIZE init
  |  |  |  `--SCAN raw USING INDEX sqlite_autoindex_raw_1
  |  |  |--SCAN i
  |  |  |--SEARCH raw USING COVERING INDEX sqlite_autoindex_raw_1 (country=? AND date>?)
  |  |  `--USE TEMP B-TREE FOR ORDER BY
  |  |--SCAN src
  |  |--SEARCH raw USING INDEX sqlite_autoindex_raw_1 (country=? AND date>? AND date<?)
  |  `--USE TEMP B-TREE FOR GROUP BY
  |--SCAN sums
  |--BLOOM FILTER ON sums (country=? AND date=?)
  |--SEARCH sums USING AUTOMATIC COVERING INDEX (country=? AND date=?)
  |--BLOOM FILTER ON sums (country=? AND date=?)
  |--SEARCH sums USING AUTOMATIC COVERING INDEX (country=? AND date=?)
  |--BLOOM FILTER ON sums (country=? AND date=?)
  |--SEARCH sums USING AUTOMATIC COVERING INDEX (country=? AND date=?)
  |--BLOOM FILTER ON i (country=?)
  `--SEARCH i USING AUTOMATIC COVERING INDEX (country=?)
}


finish_test
