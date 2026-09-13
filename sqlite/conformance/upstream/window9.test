# 2019 June 8
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

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix window9

ifcapable !windowfunc {
  finish_test
  return
}

do_execsql_test 1.0 {
  CREATE TABLE fruits(
     name TEXT COLLATE NOCASE,
     color TEXT COLLATE NOCASE
  );
}

do_execsql_test 1.1 {
  INSERT INTO fruits (name, color) VALUES ('apple', 'RED');
  INSERT INTO fruits (name, color) VALUES ('APPLE', 'yellow');
  INSERT INTO fruits (name, color) VALUES ('pear', 'YELLOW');
  INSERT INTO fruits (name, color) VALUES ('PEAR', 'green');
}

do_execsql_test 1.2 {
  SELECT name, color, dense_rank() OVER (ORDER BY name) FROM fruits;
} {
  apple RED    1
  APPLE yellow 1
  pear  YELLOW 2
  PEAR  green  2
}

do_execsql_test 1.3 {
  SELECT name, color,
    dense_rank() OVER (PARTITION BY name ORDER BY color)
  FROM fruits;
} {
  apple RED    1 
  APPLE yellow 2 
  PEAR green   1 
  pear YELLOW  2
}

do_execsql_test 1.4 {
  SELECT name, color,
    dense_rank() OVER (ORDER BY name),
    dense_rank() OVER (PARTITION BY name ORDER BY color)
  FROM fruits;
} {
  apple RED    1 1 
  APPLE yellow 1 2 
  PEAR  green  2 1 
  pear  YELLOW 2 2
}

do_execsql_test 1.5 {
  SELECT name, color,
    dense_rank() OVER (ORDER BY name),
    dense_rank() OVER (PARTITION BY name ORDER BY color)
  FROM fruits ORDER BY color;
} {
  PEAR  green  2 1 
  apple RED    1 1 
  APPLE yellow 1 2 
  pear  YELLOW 2 2
}

do_execsql_test 2.0 {
  CREATE TABLE t1(a BLOB, b INTEGER, c COLLATE nocase);
  INSERT INTO t1 VALUES(1, 2, 'abc');
  INSERT INTO t1 VALUES(3, 4, 'ABC');
}

do_execsql_test 2.1.1 {
  SELECT c=='Abc' FROM t1
} {1     1}
do_execsql_test 2.1.2 {
  SELECT c=='Abc', rank() OVER (ORDER BY b) FROM t1
} {1 1   1 2}

do_execsql_test 2.2.1 {
  SELECT b=='2' FROM t1
} {1     0}
do_execsql_test 2.2.2 {
  SELECT b=='2', rank() OVER (ORDER BY a) FROM t1
} {1 1   0 2}

#-------------------------------------------------------------------------
reset_db
do_execsql_test 3.0 {
  CREATE TABLE t1(a);
  CREATE TABLE t2(a,b,c);
}

do_execsql_test 3.1 {
  SELECT EXISTS(SELECT 1 FROM t1 ORDER BY sum(a) OVER ()) FROM t1;
}

do_execsql_test 3.2 {
  SELECT sum(a) OVER () FROM t2
   ORDER BY EXISTS(SELECT 1 FROM t2 ORDER BY sum(a) OVER ());
}

do_catchsql_test 3.3 {
  SELECT a, sum(a) OVER (ORDER BY a DESC) FROM t2 
  ORDER BY EXISTS(
    SELECT 1 FROM t2 ORDER BY sum(a) OVER (ORDER BY a)
  ) OVER (ORDER BY a);
} {1 {near "OVER": syntax error}}

do_catchsql_test 3.4 {
  SELECT y, y+1, y+2 FROM (
      SELECT c IN (
        SELECT min(a) OVER (),
        (abs(row_number() OVER())+22)/19,
        max(a) OVER () FROM t1
        ) AS y FROM t2
      );
} {1 {sub-select returns 3 columns - expected 1}}

#-------------------------------------------------------------------------
reset_db
do_execsql_test 4.0 {
  CREATE TABLE t1(a, b TEXT);
  INSERT INTO t1 VALUES('A', 1), ('A', 2), ('2', 1), ('2', 2);
}

do_execsql_test 4.1.1 {
  SELECT b, b=count(*), '1,2'                   FROM t1 GROUP BY b;
} {1 0 1,2 2 1 1,2}
do_execsql_test 4.1.2 {
  SELECT b, b=count(*), group_concat(b) OVER () FROM t1 GROUP BY b;
} {1 0 1,2 2 1 1,2}

#--------------------------------------------------------------------------
reset_db
do_execsql_test 5.0 {
  CREATE TABLE t1(a, b, c, d, e);
  CREATE INDEX i1 ON t1(a, b, c, d, e);
}

foreach {tn sql} {
  1 {
    SELECT 
      sum(e) OVER (),
      sum(e) OVER (ORDER BY a),
      sum(e) OVER (PARTITION BY a ORDER BY b),
      sum(e) OVER (PARTITION BY a, b ORDER BY c),
      sum(e) OVER (PARTITION BY a, b, c ORDER BY d)
    FROM t1;
  }
  2 {
    SELECT sum(e) OVER (PARTITION BY a ORDER BY b) FROM t1 ORDER BY a;
  }
} {
  do_test 5.1.$tn {
    execsql "EXPLAIN QUERY PLAN $sql"
  } {~/ORDER/}
}

#-------------------------------------------------------------------------
reset_db
do_execsql_test 6.0 {
  CREATE TABLE t0(c0);
  INSERT INTO t0(c0) VALUES (0);
}

do_execsql_test 6.1 {
  SELECT * FROM t0 WHERE 
  EXISTS (
    SELECT MIN(c0) OVER (), CUME_DIST() OVER () FROM t0
  ) >=1 AND 
  EXISTS (
    SELECT MIN(c0) OVER (), CUME_DIST() OVER () FROM t0
  ) <=1;
} {0}

do_execsql_test 6.2 {
  SELECT * FROM t0 WHERE EXISTS (
    SELECT MIN(c0) OVER (), CUME_DIST() OVER () FROM t0
  ) 
  BETWEEN 1 AND 1;
} {0}

#-------------------------------------------------------------------------
reset_db
do_execsql_test 7.0 {
  DROP TABLE IF EXISTS t1;
  CREATE TABLE t1(x, y);
  INSERT INTO t1 VALUES(10, 1);
  INSERT INTO t1 VALUES(20, 2);
  INSERT INTO t1 VALUES(3, 3);
  INSERT INTO t1 VALUES(2, 4);
  INSERT INTO t1 VALUES(1, 5);
} {}


do_execsql_test 7.1 {
  SELECT avg(x) OVER (ORDER BY y) AS z FROM t1 ORDER BY z
} {
  7.2 8.75 10.0 11.0 15.0
}

do_execsql_test 7.2 {
  SELECT avg(x) OVER (ORDER BY y) z FROM t1 ORDER BY (z IS y);
} {
  10.0 15.0 11.0 8.75 7.2
}

do_execsql_test 7.3 {
  SELECT avg(x) OVER (ORDER BY y) z FROM t1 ORDER BY (y IS z);
} {
  10.0 15.0 11.0 8.75 7.2
}

do_execsql_test 7.4 {
  SELECT avg(x) OVER (ORDER BY y) z FROM t1 ORDER BY z + 0.0;
} {
  7.2 8.75 10.0 11.0 15.0
}

#-------------------------------------------------------------------------
reset_db
do_execsql_test 8.1.1 {
  CREATE TABLE t1(a, b);
  INSERT INTO t1 VALUES(1, 2), (3, 4);
  SELECT min( sum(a) ) OVER () FROM t1;
} {4}

do_execsql_test 8.1.2 {
  SELECT min( sum(a) ) OVER () FROM t1 GROUP BY a;
} {1 1}

do_execsql_test 8.2 {
  CREATE VIEW v1 AS 
    SELECT 0 AS x
      UNION 
    SELECT count() OVER() FROM (SELECT 0) 
    ORDER BY 1
  ;
}

do_catchsql_test 8.3 {
  SELECT min( max((SELECT x FROM v1)) ) OVER()
} {0 0}

do_execsql_test 8.4 {
  SELECT(
      SELECT x UNION 
      SELECT sum( avg((SELECT x FROM v1)) ) OVER()
  )
  FROM v1;
} {0.0 0.0}

#--------------------------------------------------------------------------
reset_db
do_execsql_test 9.0 {
  CREATE TABLE t1(a, b, c);
  INSERT INTO t1 VALUES(NULL,'bb',356);
  INSERT INTO t1 VALUES('CB','aa',158);
  INSERT INTO t1 VALUES('BB','aa',399);
  INSERT INTO t1 VALUES('FF','bb',938);
}

do_catchsql_test 9.1 {
  SELECT sum(c) OVER (
    ORDER BY c RANGE BETWEEN 0 PRECEDING AND '-700' PRECEDING
  )
  FROM t1
} {1 {frame ending offset must be a non-negative number}}

#--------------------------------------------------------------------------
reset_db

do_execsql_test 10.0 {
  CREATE TABLE t1(a, b);
  INSERT INTO t1 VALUES(1, 'a');
  INSERT INTO t1 VALUES(2, 'b');
  INSERT INTO t1 VALUES(3, 'c');
  INSERT INTO t1 VALUES(4, 'd');
  INSERT INTO t1 VALUES(5, 'e');
  INSERT INTO t1 VALUES(6, 'f');
}

do_execsql_test 10.1 {
  SELECT a, min(b) OVER win
  FROM t1
  WINDOW win AS (ORDER BY a ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING)
} {
  1 a
  2 a
  3 a
  4 b
  5 c
  6 d
}

do_execsql_test 10.2 {
  SELECT a, min(b) FILTER (WHERE a%2) OVER win
  FROM t1
  WINDOW win AS (ORDER BY a ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING)
} {
  1 a
  2 a
  3 a
  4 c
  5 c
  6 e
}

do_execsql_test 10.3 {
  SELECT a, min(b) FILTER (WHERE (a%2)=0) OVER win
  FROM t1
  WINDOW win AS (ORDER BY a ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING)
} {
  1 b
  2 b
  3 b
  4 b
  5 d
  6 d
}

do_catchsql_test 10.4 {
  SELECT a, nth_value(b, 1) FILTER (WHERE (a%2)=0) OVER win
  FROM t1
  WINDOW win AS (ORDER BY a ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING)
} {1 {FILTER clause may only be used with aggregate window functions}}

finish_test
