# 2020-12-16
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
# focus of this file is flattening UNION ALL sub-queries.
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix unionall

do_execsql_test 1.0 {
  CREATE TABLE t1_a(a INTEGER PRIMARY KEY, b TEXT);
  CREATE TABLE t1_b(c INTEGER PRIMARY KEY, d TEXT);
  CREATE TABLE t1_c(e INTEGER PRIMARY KEY, f TEXT);

  INSERT INTO t1_a VALUES(1, 'one'), (4, 'four');
  INSERT INTO t1_b VALUES(2, 'two'), (5, 'five');
  INSERT INTO t1_c VALUES(3, 'three'), (6, 'six');

  CREATE VIEW t1 AS 
    SELECT a, b FROM t1_a   UNION ALL
    SELECT c, d FROM t1_b   UNION ALL
    SELECT e, f FROM t1_c;

  CREATE TABLE i1(x);
  INSERT INTO i1 VALUES(2), (5), (6), (1);
}

do_execsql_test 1.1 {
  SELECT a, b FROM (
    SELECT a, b FROM t1_a   UNION ALL
    SELECT c, d FROM t1_b   UNION ALL
    SELECT e, f FROM t1_c
  ) ORDER BY a
} {
  1 one 2 two 3 three 4 four 5 five 6 six
}

do_execsql_test 1.2 {
  SELECT a, b FROM t1 ORDER BY a
} {
  1 one 2 two 3 three 4 four 5 five 6 six
}

do_execsql_test 1.3 {
  SELECT a, b FROM i1, t1 WHERE a=x ORDER BY a
} {1 one 2 two 5 five 6 six}


# 2022-10-31 part of ticket 57c47526c34f01e8
# The queries below were causing an assertion fault in
# the comparison operators of the VDBE.
#
reset_db
database_never_corrupt
optimization_control db all 0
do_execsql_test 1.10 {
  CREATE TABLE t0(c0 INT);
  INSERT INTO t0 VALUES(0);
  CREATE TABLE t1_a(a INTEGER PRIMARY KEY, b TEXT);
  INSERT INTO t1_a VALUES(1,'one');
  CREATE TABLE t1_b(c INTEGER PRIMARY KEY, d TEXT);
  INSERT INTO t1_b VALUES(2,'two');
  CREATE VIEW t1 AS SELECT a, b FROM t1_a UNION ALL SELECT c, c FROM t1_b;
  SELECT * FROM (SELECT t1.a, t1.b AS b, t0.c0 FROM t0, t1);
} {1 one 0 2 2 0}
do_execsql_test 1.11 {
  SELECT * FROM (SELECT t1.a, t1.b AS b, t0.c0 FROM t0, t1) WHERE b=2;
} {2 2 0}

#-------------------------------------------------------------------------
reset_db

do_execsql_test 2.1.0 {
  CREATE TABLE t1(x, y);
  INSERT INTO t1 VALUES(1, 'one');
  INSERT INTO t1 VALUES(1, 'ONE');
  INSERT INTO t1 VALUES(2, 'two');
  INSERT INTO t1 VALUES(2, 'TWO');
  INSERT INTO t1 VALUES(3, 'three');
  INSERT INTO t1 VALUES(3, 'THREE');
}

do_execsql_test 2.1.1 {
  WITH s(i) AS (
      SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i<3
  )
  SELECT * FROM (
    SELECT 0 AS i UNION ALL SELECT i FROM s UNION ALL SELECT 0
  ), t1 WHERE x=i;
} {
  1 1 one 1 1 ONE 2 2 two 2 2 TWO 3 3 three 3 3 THREE
}

do_catchsql_test 2.1.2 {
  WITH s(i) AS (
      SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i<3 UNION ALL SELECT 4
  )
  SELECT * FROM s, t1 WHERE x=i;
} {1 {circular reference: s}}

do_execsql_test 2.2.0 {
  CREATE TABLE t2_a(k INTEGER PRIMARY KEY, v TEXT);
  CREATE TABLE t2_b(k INTEGER PRIMARY KEY, v TEXT);

  CREATE VIEW t2 AS 
    SELECT * FROM t2_a 
    UNION ALL 
    SELECT * FROM t2_b;

  CREATE TRIGGER t2_insert INSTEAD OF INSERT ON t2 BEGIN
    INSERT INTO t2_a SELECT new.k, new.v WHERE (new.k%2)==0;
    INSERT INTO t2_b SELECT new.k, new.v WHERE (new.k%2)==1;
  END;

  INSERT INTO t2 VALUES(5, 'v'), (4, 'iv'), (3, 'iii'), (2, 'ii');
}

do_execsql_test 2.2.1 {
  SELECT * FROM t1, t2 WHERE x=k;
} {
  2 two 2 ii 2 TWO 2 ii 3 three 3 iii 3 THREE 3 iii
}

do_execsql_test 2.2.2 {
  SELECT * FROM t1 LEFT JOIN t2 ON (x=k);
} {
  1 one {} {}
  1 ONE {} {}
  2 two 2 ii 2 TWO 2 ii 3 three 3 iii 3 THREE 3 iii
}

do_execsql_test 2.2.3 {
  SELECT x1.*, x2.* FROM t2 AS x1, t2 AS x2 WHERE x1.k=x2.k+1
} {
  4 iv   3 iii 
  3 iii  2 ii 
  5 v    4 iv
}

do_execsql_test 2.2.4 {
  SELECT * FROM t1, t2 WHERE x=k ORDER BY y;
} {
  3 THREE 3 iii 
  2 TWO 2 ii 
  3 three 3 iii 
  2 two 2 ii
}
do_execsql_test 2.2.5 {
  SELECT * FROM t1, t2 WHERE x=k ORDER BY y||'';
} {
  3 THREE 3 iii 
  2 TWO 2 ii 
  3 three 3 iii 
  2 two 2 ii
}
do_execsql_test 2.2.6 {
  SELECT * FROM t1, t2 WHERE x=k ORDER BY v
} {
  2 two   2 ii
  2 TWO   2 ii 
  3 three 3 iii 
  3 THREE 3 iii 
}
do_execsql_test 2.2.7 {
  SELECT * FROM t1, t2 WHERE x=k ORDER BY v||''
} {
  2 two   2 ii
  2 TWO   2 ii 
  3 three 3 iii 
  3 THREE 3 iii 
}
do_execsql_test 2.2.8 {
  SELECT * FROM t1, t2 WHERE x=k ORDER BY k,v||''
} {
  2 two   2 ii
  2 TWO   2 ii 
  3 three 3 iii 
  3 THREE 3 iii 
}
do_execsql_test 2.2.9a {
  SELECT * FROM t1, t2 ORDER BY +k
} {
  1 one 2 ii 1 ONE 2 ii 2 two 2 ii 
  2 TWO 2 ii 3 three 2 ii 3 THREE 2 ii 
  
  1 one 3 iii 1 ONE 3 iii 2 two 3 iii 
  2 TWO 3 iii 3 three 3 iii 3 THREE 3 iii 

  1 one 4 iv 1 ONE 4 iv 2 two 4 iv 
  2 TWO 4 iv 3 three 4 iv 3 THREE 4 iv 

  1 one 5 v 1 ONE 5 v 2 two 5 v 
  2 TWO 5 v 3 three 5 v 3 THREE 5 v
}

do_execsql_test 2.2.9b {
  SELECT * FROM t1, t2 ORDER BY k
} {
  1 one 2 ii 1 ONE 2 ii 2 two 2 ii 
  2 TWO 2 ii 3 three 2 ii 3 THREE 2 ii 
  
  1 one 3 iii 1 ONE 3 iii 2 two 3 iii 
  2 TWO 3 iii 3 three 3 iii 3 THREE 3 iii 

  1 one 4 iv 1 ONE 4 iv 2 two 4 iv 
  2 TWO 4 iv 3 three 4 iv 3 THREE 4 iv 

  1 one 5 v 1 ONE 5 v 2 two 5 v 
  2 TWO 5 v 3 three 5 v 3 THREE 5 v
}

#-------------------------------------------------------------------------
reset_db
do_execsql_test 3.0 {
  CREATE TABLE t1(c INTEGER PRIMARY KEY, d TEXT);
  INSERT INTO t1 VALUES(1,2);
  CREATE TABLE t3_a(k INTEGER PRIMARY KEY, v TEXT);
  INSERT INTO t3_a VALUES(2,'ii');
  CREATE TABLE t3_b(k INTEGER PRIMARY KEY, v TEXT);
  CREATE VIEW t3 AS
    SELECT * FROM t3_a
    UNION ALL
    SELECT * FROM t3_b;
} {}

do_execsql_test 3.1 {
  SELECT * FROM t1, t3 ORDER BY k;
} {1 2 2 ii}

reset_db
do_execsql_test 4.0 {

  CREATE TABLE t1_a(a INTEGER PRIMARY KEY, b TEXT);
  INSERT INTO t1_a VALUES(123, 't1_a');
  CREATE TABLE t1_b(c INTEGER PRIMARY KEY, d TEXT);

  CREATE VIEW t1 AS
    SELECT a, b FROM t1_a
    UNION ALL
    SELECT c, d FROM t1_b;

  CREATE TABLE t3_a(k INTEGER PRIMARY KEY, v TEXT);
  INSERT INTO t3_a VALUES(456, 't3_a');
  CREATE TABLE t3_b(k INTEGER PRIMARY KEY, v TEXT);

  CREATE VIEW t3 AS
    SELECT * FROM t3_a
    UNION ALL
    SELECT * FROM t3_b;
}

do_execsql_test 4.1 {
  SELECT * FROM t1, t3 ORDER BY k;
} {123 t1_a 456 t3_a}

do_execsql_test 4.2 {
  SELECT * FROM (SELECT * FROM t1, t3) ORDER BY k;
} {123 t1_a 456 t3_a}

do_execsql_test 4.3 {
  SELECT * FROM (SELECT * FROM t1, t3), (
    SELECT max(a) OVER () FROM t1
      UNION ALL
    SELECT min(a) OVER () FROM t1
  )
  ORDER BY k;
} {
  123 t1_a 456 t3_a 123
  123 t1_a 456 t3_a 123
}

do_execsql_test 4.3 {
  SELECT * FROM (SELECT * FROM t1, t3), (
    SELECT group_concat(a) OVER (ORDER BY a), 
           group_concat(a) OVER (ORDER BY a),
           group_concat(a) OVER (ORDER BY a),
           group_concat(a) OVER (ORDER BY a),
           group_concat(a) OVER (ORDER BY a),
           group_concat(a) OVER (ORDER BY a),
           group_concat(a) OVER (ORDER BY a),
           group_concat(a) OVER (ORDER BY a),
           group_concat(a) OVER (ORDER BY a)
    FROM t1
  )
  ORDER BY k;
} {
  123 t1_a 456 t3_a 123 123 123 123 123 123 123 123 123
}

do_execsql_test 4.3 {
  SELECT * FROM (SELECT * FROM t1, t3) AS o, (
    SELECT * FROM t1 LEFT JOIN t3 ON a=k
  );
} {
  123 t1_a 456 t3_a 123 t1_a {} {}
}

# 2020-12-30: dbsqlfuzz find
reset_db
do_execsql_test 5.1 {
  CREATE TABLE t1_a(a INTEGER PRIMARY KEY, b TEXT);
  INSERT INTO t1_a VALUES(1,'one');
  INSERT INTO t1_a VALUES(0,NULL);
  CREATE TABLE t1_b(c INTEGER PRIMARY KEY, d TEXT);
  INSERT INTO t1_b VALUES(2,'two');
  INSERT INTO t1_b VALUES(5,'five');
  CREATE TABLE t1_c(e INTEGER PRIMARY KEY, f TEXT);
  INSERT INTO t1_c VALUES(3,'three');
  INSERT INTO t1_c VALUES(6,'six');
  CREATE TABLE t2(k,v);
  INSERT INTO t2 VALUES(5,'v');
  INSERT INTO t2 VALUES(4,'iv');
  INSERT INTO t2 VALUES(3,'iii');
  INSERT INTO t2 VALUES(2,'ii');
  CREATE TABLE t3_a(k INTEGER PRIMARY KEY, v TEXT);
  INSERT INTO t3_a VALUES(2,'ii');
  INSERT INTO t3_a VALUES(4,'iv');
  CREATE TABLE t3_b(k INTEG5R PRIMARY KEY, v TEXT);
  INSERT INTO t3_b VALUES(NULL,'iii');
  INSERT INTO t3_b VALUES(NULL,'v');
  CREATE VIEW t1 AS 
    SELECT a, b FROM t1_a   UNION ALL
    SELECT c, d FROM t1_b   UNION ALL
    SELECT e, f FROM t1_c;
  CREATE VIEW t3 AS 
      SELECT * FROM t3_a 
      UNION ALL 
      SELECT * FROM t3_b;
  CREATE TRIGGER t3_insert INSTEAD OF INSERT ON t3 BEGIN
      INSERT INTO t3_a SELECT new.k, new.v WHERE (new.k%2)==0;
      INSERT INTO t3_b SELECT new.k, new.v WHERE (new.k%2)==1;
  END;
} {}
do_execsql_test 5.10 {
  SELECT *, '+' FROM t1 LEFT JOIN t2 ON (a NOT IN(SELECT v FROM t1, t3 WHERE a=k)=NOT EXISTS(SELECT 1 FROM t1 LEFT JOIN t3 ON (a=k)));
} {0 {} {} {} + 1 one {} {} + 2 two {} {} + 5 five {} {} + 3 three {} {} + 6 six {} {} +}
do_execsql_test 5.20 {
  SELECT *, '+' FROM t1 LEFT JOIN t3 ON (a NOT IN(SELECT v FROM t1 LEFT JOIN t2 ON (a=k))=k);
} {0 {} {} {} + 1 one {} {} + 2 two {} {} + 5 five {} {} + 3 three {} {} + 6 six {} {} +}
ifcapable vtab {
do_catchsql_test 5.30 {
  SELECT * FROM (t1 NATURAL JOIN pragma_table_xinfo('t1_a') NATURAL JOIN t3) t1
                NATURAL JOIN t2 NATURAL JOIN t3
   WHERE rowid ISNULL>0 AND 0%y;
} {1 {ambiguous column name: rowid}}
}

reset_db
do_execsql_test 6.0 {
  CREATE TABLE t1(a,b);
  INSERT INTO t1 VALUES(1,2);
  CREATE TABLE t2(a,b);
  INSERT INTO t2 VALUES(3,4);

  CREATE TABLE t3(a,b);
  INSERT INTO t3 VALUES(5,6);
  CREATE TABLE t4(a,b);
  INSERT INTO t4 VALUES(7,8);

  CREATE TABLE t5(a,b);
  INSERT INTO t5 VALUES(9,10);
}

do_execsql_test 6.1 {
  WITH x(c) AS (
    SELECT 1000 FROM t1 UNION ALL SELECT 800 FROM t2
  ),
  y(d) AS (
    SELECT  100 FROM t3 UNION ALL SELECT 400 FROM t4
  )
  SELECT * FROM t5, x, y;
} {
  9 10 1000 100     9 10 1000 400
  9 10 800 100      9 10 800 400
}

# 2021-04-26 dbsqlfuzz 88ed5c66789fced139d148aed823cba7c0926dd7
reset_db
do_execsql_test 7.1 {
  WITH c1(x) AS (VALUES(0) UNION ALL SELECT 100+x FROM c1 WHERE x<100 UNION ALL SELECT 1+x FROM c1 WHERE x<1)
  SELECT x, y, '|'
    FROM c1 AS x1, (SELECT x+1 AS y FROM c1 WHERE x<1 UNION ALL SELECT 1+x FROM c1 WHERE 1<x) AS x2
   ORDER BY x, y;
} {0 1 | 0 101 | 0 102 | 1 1 | 1 101 | 1 102 | 100 1 | 100 101 | 100 102 | 101 1 | 101 101 | 101 102 |}

# 2022-10-31 ticket https://sqlite.org/src/info/57c47526c34f01e8
# dbsqlfuzz 37230460b46b3b6049f0d768eb801f3428189382
# UNION ALL subqueries or views which have arms with different
# affinities should not be flattened.
#
reset_db
do_execsql_test 8.1 {
  CREATE TABLE t0(c0 INT);
  INSERT INTO t0 VALUES(0);
  CREATE TABLE t1_a(a INTEGER PRIMARY KEY, b TEXT);
  INSERT INTO t1_a VALUES(1,'one');
  INSERT INTO t1_a VALUES(4,'four');
  CREATE TABLE t1_b(c INTEGER PRIMARY KEY, d TEXT);
  INSERT INTO t1_b VALUES(2,'two');
  INSERT INTO t1_b VALUES(5,'five');
  CREATE TABLE t1_c(e INTEGER PRIMARY KEY, f TEXT);
  INSERT INTO t1_c VALUES(3,'three');
  INSERT INTO t1_c VALUES(6,'six');
  CREATE VIEW v0(c0) AS SELECT CAST(t0.c0 AS INTEGER) FROM t0;
  CREATE VIEW t1 AS 
    SELECT a, b FROM t1_a   UNION ALL
    SELECT c, c FROM t1_b   UNION ALL
    SELECT e, f FROM t1_c;
  SELECT t1.a, t1.b, t0.c0 AS c, v0.c0 AS d FROM t0 LEFT JOIN v0 ON v0.c0>'0',t1;
} {1 one 0 {} 4 four 0 {} 2 2 0 {} 5 5 0 {} 3 three 0 {} 6 six 0 {}}

optimization_control db all 1
do_execsql_test 8.2 {
  SELECT * FROM (SELECT t1.a, t1.b, t0.c0 AS c, v0.c0 AS d FROM t0 LEFT JOIN v0 ON v0.c0>'0',t1) WHERE b=2;
} {2 2 0 {}}
do_execsql_test 8.3 {
  SELECT * FROM (SELECT t1.a, t1.b, t0.c0 AS c, v0.c0 AS d FROM t0 LEFT JOIN v0 ON v0.c0>'0',t1) WHERE b=2.0;
} {2 2 0 {}}
do_execsql_test 8.4 {
  SELECT * FROM (SELECT t1.a, t1.b, t0.c0 AS c, v0.c0 AS d FROM t0 LEFT JOIN v0 ON v0.c0>'0',t1) WHERE b='2';
} {}
optimization_control db query-flattener,push-down 0
do_execsql_test 8.5 {
  SELECT * FROM (SELECT t1.a, t1.b, t0.c0 AS c, v0.c0 AS d FROM t0 LEFT JOIN v0 ON v0.c0>'0',t1) WHERE b=2;
} {2 2 0 {}}
do_execsql_test 8.6 {
  SELECT * FROM (SELECT t1.a, t1.b, t0.c0 AS c, v0.c0 AS d FROM t0 LEFT JOIN v0 ON v0.c0>'0',t1) WHERE b=2.0;
} {2 2 0 {}}
do_execsql_test 8.7 {
  SELECT * FROM (SELECT t1.a, t1.b, t0.c0 AS c, v0.c0 AS d FROM t0 LEFT JOIN v0 ON v0.c0>'0',t1) WHERE b='2';
} {}
optimization_control db all 0
do_execsql_test 8.8 {
  SELECT * FROM (SELECT t1.a, t1.b, t0.c0 AS c, v0.c0 AS d FROM t0 LEFT JOIN v0 ON v0.c0>'0',t1) WHERE b=2;
} {2 2 0 {}}
do_execsql_test 8.9 {
  SELECT * FROM (SELECT t1.a, t1.b, t0.c0 AS c, v0.c0 AS d FROM t0 LEFT JOIN v0 ON v0.c0>'0',t1) WHERE b=2.0;
} {2 2 0 {}}
do_execsql_test 8.10 {
  SELECT * FROM (SELECT t1.a, t1.b, t0.c0 AS c, v0.c0 AS d FROM t0 LEFT JOIN v0 ON v0.c0>'0',t1) WHERE b='2';
} {}


finish_test
