# 2018-08-04
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
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
source $testdir/malloc_common.tcl
set testprefix countofview

do_execsql_test 1.0 {
  CREATE TABLE t2(c);
  CREATE TABLE t3(f);

  INSERT INTO t2 VALUES(1), (2);
  INSERT INTO t3 VALUES(3);
}

do_execsql_test 1.1 {
  select c from t2 union all select f from t3 limit 1 offset 1
} {2}

do_execsql_test 1.2 {
  select count(*) from (
    select c from t2 union all select f from t3 limit 1 offset 1
  )
} {1}

do_execsql_test 1.3 {
  select count(*) from (
    select c from t2 union all select f from t3
  )
} {3}

# 2019-05-15
do_execsql_test 2.0 {
  CREATE TABLE t1(x);
  INSERT INTO t1 VALUES(1),(99),('abc');
  CREATE VIEW v1(x,y) AS SELECT x,1 FROM t1 UNION ALL SELECT x,2 FROM t1;
  SELECT count(*) FROM v1 WHERE x<>1;
} {4}
do_execsql_test 2.1 {
  SELECT count(*) FROM v1 GROUP BY y;
} {3 3}

# 2023-03-01 dbsqlfuzz ef8623915d843b150c159166ee4548c78cc6895a
# count-of-view should not apply to CTEs.
#
ifcapable progress {
  proc progress_stop args {return 1}
  db progress 1000 progress_stop
  do_catchsql_test 3.1 {
    WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c)
    SELECT count(*) FROM c;
  } {1 interrupted}
}

# 2023-03-07 dbsqlfuzz 23d782160b71c3f8f535ccb2da313dfc8eb8c631
#
do_execsql_test 4.1 {
  DROP TABLE t1;
  DROP TABLE t2;
  DROP TABLE t3;
  CREATE TABLE t1(a INTEGER PRIMARY KEY, b TEXT);
  INSERT INTO t1 VALUES(4,'four');
  CREATE TABLE t2(c INTEGER PRIMARY KEY, d TEXT);
  CREATE VIEW t3 AS SELECT a, b FROM t1 UNION ALL SELECT c, d FROM t2;
  SELECT count(*) FROM t3 ORDER BY sum(a);
} 1

# 2023-03-31 dbsqlfuzz 6a107e3055bd22afab31cfddabc2d9d54fcbaf69
# Having clauses should disqualify count-of-view
#
reset_db
do_execsql_test 5.1 {
  CREATE TABLE t1(a INTEGER PRIMARY KEY, b TEXT);
  INSERT INTO t1 VALUES(1,'one'),(4,'four');
  CREATE TABLE t2(c INTEGER PRIMARY KEY, d TEXT);
  INSERT INTO t2 VALUES(2,'two'),(5,'five');
  CREATE VIEW t3 AS SELECT a, b FROM t1 UNION ALL SELECT c, d FROM t2;
  SELECT count(*) FROM t3 HAVING count(*)>0;
} 4
do_execsql_test 5.2 {
  SELECT count(*) FROM t3 HAVING count(*)>5;
} {}
do_execsql_test 5.3 {
  SELECT count(*) FROM t3 HAVING max(b)>'mmm';
} 4
do_execsql_test 5.4 {
  SELECT count(*) FROM t3 HAVING min(b)>'mmm';
} {}
do_execsql_test 5.5 {
  SELECT count(*) FROM (
     SELECT a, max(b) FROM t1 HAVING a<100 UNION ALL SELECT c, d FROM t2
  )
} 3


finish_test
