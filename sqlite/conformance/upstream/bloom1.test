# 2022 October 06
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
# Tests for queries that use bloom filters

set testdir [file dirname $argv0]
source $testdir/tester.tcl
source $testdir/lock_common.tcl
source $testdir/malloc_common.tcl

set testprefix bloom1

# Tests 1.*  verify that the bloom filter code correctly handles the
# case where the RHS of an (<ipk-column> = ?) expression must be coerced
# to an integer before the comparison made.
#
do_execsql_test 1.0 {
  CREATE TABLE t1(a, b);
  CREATE TABLE t2(c INTEGER PRIMARY KEY, d);
}

do_execsql_test 1.1 {
  INSERT INTO t1 VALUES('hello', 'world');
  INSERT INTO t2 VALUES(14, 'fourteen');
}

do_execsql_test 1.2 {
  ANALYZE sqlite_schema;
  INSERT INTO sqlite_stat1 VALUES('t2','idx1','6 6');
  ANALYZE sqlite_schema;
}

do_execsql_test 1.3 {
  SELECT 'affinity!' FROM t1 CROSS JOIN t2 WHERE t2.c = '14';
} {affinity!}


reset_db
do_execsql_test 1.4 {
  CREATE TABLE t1(a, b TEXT);
  CREATE TABLE t2(c INTEGER PRIMARY KEY, d);
  CREATE TABLE t3(e INTEGER PRIMARY KEY, f);

  ANALYZE sqlite_schema;
  INSERT INTO sqlite_stat1 VALUES('t1','idx1','600 6');
  INSERT INTO sqlite_stat1 VALUES('t2','idx1','6 6');
  INSERT INTO sqlite_stat1 VALUES('t3','idx2','6 6');
  ANALYZE sqlite_schema;

  INSERT INTO t1 VALUES(1, '123');
  INSERT INTO t2 VALUES(123, 'one');
  INSERT INTO t3 VALUES(123, 'two');
}

do_execsql_test 1.5 {
  SELECT 'result' FROM t1, t2, t3 
  WHERE t2.c=t1.b AND t2.d!='silly'
    AND t3.e=t1.b AND t3.f!='silly'
} {result}

# 2023-02-05
# https://sqlite.org/forum/forumpost/56de336385
#
# Do not employ a Bloom filter if the table being filtered or any table
# wo the left of the table being filtered lacks STAT1 data, since we
# cannot make a good Bloom filter usefulness determination without STAT1
# data.
#
reset_db
do_execsql_test 2.0 {
  CREATE TABLE objs(c INTEGER, s INTEGER, p INTEGER, o INTEGER);
  CREATE UNIQUE INDEX objs_cspo ON objs(o,p,c,s);
  ANALYZE;
  DELETE FROM sqlite_stat1;
  INSERT INTO sqlite_stat1 VALUES('objs','objs_cspo','520138 21 20 19 1');
  ANALYZE sqlite_schema;
}
do_eqp_test 2.1 {
  WITH RECURSIVE transit(x) AS (
     SELECT s FROM objs WHERE p=9 AND o=32805
     UNION
     SELECT objs.s FROM objs, transit WHERE objs.p=9 AND objs.o=transit.x
  )
  SELECT x FROM transit;
} {
  QUERY PLAN
  |--CO-ROUTINE transit
  |  |--SETUP
  |  |  `--SEARCH objs USING COVERING INDEX objs_cspo (o=? AND p=?)
  |  `--RECURSIVE STEP
  |     |--SCAN transit
  |     `--SEARCH objs USING COVERING INDEX objs_cspo (o=? AND p=?)
  `--SCAN transit
}

# 2023-02-28
# https://sqlite.org/forum/forumpost/0846211821
#
# Bloom filter gives an incorrect result if the collating sequence is
# anything other than binary.
#
reset_db
do_execsql_test 3.1 {
  CREATE TABLE t0(x TEXT COLLATE rtrim);
  INSERT INTO t0(x) VALUES ('a'), ('b'), ('c');
  CREATE VIEW v0(y) AS SELECT DISTINCT x FROM t0;
  SELECT count(*) FROM t0, v0 WHERE x='b ';
} 3
do_eqp_test 3.2 {
  SELECT count(*) FROM t0, v0 WHERE x='b ';
} {
  QUERY PLAN
  |--CO-ROUTINE v0
  |  |--SCAN t0
  |  `--USE TEMP B-TREE FOR DISTINCT
  |--SCAN v0
  `--SEARCH t0 USING AUTOMATIC PARTIAL COVERING INDEX (x=?)
}
# ^^^^^--- The key feature in the previous result is that no Bloom filter
# is used.  In the following, a Bloom filter is used because the data type
# is INT instead of TEXT.
do_execsql_test 3.3 {
  CREATE TABLE t1(x INT COLLATE rtrim);
  INSERT INTO t1(x) VALUES ('a'), ('b'), ('c');
  CREATE VIEW v1(y) AS SELECT DISTINCT x FROM t1;
  SELECT count(*) FROM t1, v1 WHERE x='b ';
} 3
do_eqp_test 3.4 {
  SELECT count(*) FROM t1, v1 WHERE x='b ';
} {
  QUERY PLAN
  |--CO-ROUTINE v1
  |  |--SCAN t1
  |  `--USE TEMP B-TREE FOR DISTINCT
  |--SCAN v1
  |--BLOOM FILTER ON t1 (x=?)
  `--SEARCH t1 USING AUTOMATIC PARTIAL COVERING INDEX (x=?)
}

# 2023-03-14 
# https://sqlite.org/forum/forumpost/d47a0e8e3a
# https://sqlite.org/forum/forumpost/2e427099d5
#
# Both reports are for the same problem - using a Bloom filter on an
# expression index can cause issues.
#
reset_db
do_execsql_test 4.1 {
  CREATE TABLE t1(x TEXT, y INT, z TEXT);
  INSERT INTO t1(rowid,x,y,z) VALUES(12,'aa','bb','aa');
  CREATE INDEX i1x ON t1(1 IS true,z);
  CREATE TABLE t0(x TEXT);
  INSERT INTO t0(rowid,x) VALUES(4,'aa');
  ANALYZE sqlite_schema;
  INSERT INTO sqlite_stat1 VALUES('t0',NULL,'20');
  INSERT INTO sqlite_stat1 VALUES('t1','i1x','18 18 2');
  ANALYZE sqlite_schema;
}
do_execsql_test 4.2 {
  SELECT * FROM t0 NATURAL JOIN t1 WHERE z=t1.x;
} {aa bb aa}
do_execsql_test 4.3 {
  DROP TABLE t0;
  CREATE TABLE t0(a TEXT);
  INSERT INTO t0 VALUES ('xyz');
  CREATE INDEX t0x ON t0(a IS FALSE) WHERE false;
  DROP TABLE t1;
  CREATE TABLE t1(b INT);
  INSERT INTO t1 VALUES('aaa'),('bbb'),('ccc'),('ddd'),(NULL);
  CREATE TABLE t2(c REAL);
  INSERT INTO t2 VALUES(7);
  ANALYZE;
  CREATE INDEX t2x ON t2(true IN ());
}
do_execsql_test 4.4 {
  SELECT * FROM t0 LEFT JOIN t1 LEFT JOIN t2 ON (b NOTNULL)==(c IN ()) WHERE c;
} {xyz {} 7.0}

reset_db
do_execsql_test 5.0 {
  CREATE TABLE t1 (c1);
  INSERT INTO t1 VALUES (101);
  CREATE TABLE t2 ( x );
  INSERT INTO t2 VALUES(404);
}

do_execsql_test 5.1 {
  SELECT 'val' in (
      select 'val' from ( select 'valueB' from t1 order by 1 ) 
      union all 
      select 'val'
  );
} {1}

do_execsql_test 5.2 {
  select * from t2
    where 'val' in (
        select 'val' from ( select 'valueB' from t1 order by 1 ) 
        union all 
        select 'val'
    );
} {404}

do_execsql_test 5.3 {
  SELECT subq_1.c_0 as c_0 
  FROM ( SELECT 0 as c_0) as subq_1
  WHERE (subq_1.c_0) IN (
    SELECT subq_2.c_0 as c_0
    FROM (
      SELECT 0 as c_0
      FROM t1 as ref_1
      WHERE (ref_1.c1) = (2)
      ORDER BY c_0 desc
    ) as subq_2
    UNION ALL
    SELECT 0 as c_0
  );
} {0}

# 2025-04-30 https://sqlite.org/forum/forumpost/792a09cb3df9e69f
# A continuation of the above.
#
do_execsql_test 6.1 {
  DROP TABLE IF EXISTS t1;
  CREATE TABLE t1(a);
  SELECT 111 IN (
    SELECT 222 FROM (SELECT 333 ORDER BY 1)
    UNION ALL
    SELECT 444 FROM (SELECT 555 FROM t1 ORDER BY 1)
  );
} 0

finish_test
