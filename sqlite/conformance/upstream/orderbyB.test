# 2024-08-15
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
# Specifically, it tests cases with order-by-subquery optimization in which
# an ORDER BY in a subquery is used to help resolve an ORDER BY in the
# outer query without having to do an extra sort.
# 

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set ::testprefix orderbyb

db null NULL
do_execsql_test 1.0 {
  CREATE TABLE t1(a TEXT, b TEXT, c INT);
  INSERT INTO t1 VALUES(NULL,NULL,NULL);
  WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<7)
    INSERT INTO t1(a,b,c) SELECT char(p,p), char(q,q), n FROM
            (SELECT ((n-1)%4)+0x61 AS p, abs(n*2-9+(n>=5))+0x60 AS q, n FROM c);
  UPDATE t1 SET b=upper(b) WHERE c=1;
  CREATE TABLE t2(k TEXT PRIMARY KEY, v INT) WITHOUT ROWID;
  WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<7)
    INSERT INTO t2(k,v) SELECT char(0x60+n,0x60+n), n FROM c;
  WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<7)
    INSERT INTO t2(k,v) SELECT char(0x40+n,0x40+n), n FROM c;
  SELECT a,b,c,tx.v AS 'v-a', ty.v AS 'v-b'
    FROM t1 LEFT JOIN t2 AS tx ON tx.k=a
            LEFT JOIN t2 AS ty ON ty.k=b
   ORDER BY c;
} {
  NULL  NULL  NULL  NULL  NULL
  aa    GG    1     1     7
  bb    ee    2     2     5
  cc    cc    3     3     3
  dd    aa    4     4     1
  aa    bb    5     1     2
  bb    dd    6     2     4
  cc    ff    7     3     6
}

do_eqp_execsql_test 1.1 {
  WITH t3(x,y) AS (SELECT a, b FROM t1 ORDER BY a, b LIMIT 8)
    SELECT x, y, v FROM t3 LEFT JOIN t2 ON k=t3.y ORDER BY x, y COLLATE nocase;
} {
  QUERY PLAN
  |--CO-ROUTINE t3
  |  |--SCAN t1
  |  `--USE TEMP B-TREE FOR ORDER BY
  |--SCAN t3
  |--SEARCH t2 USING PRIMARY KEY (k=?) LEFT-JOIN
  `--USE TEMP B-TREE FOR LAST TERM OF ORDER BY
} {
  NULL  NULL  NULL
  aa    bb    2
  aa    GG    7
  bb    dd    4
  bb    ee    5
  cc    cc    3
  cc    ff    6
  dd    aa    1
}

do_eqp_execsql_test 1.2 {
  WITH t3(x,y) AS MATERIALIZED (SELECT a, b COLLATE nocase FROM t1 ORDER BY 1,2)
    SELECT x, y, v FROM t3 LEFT JOIN t2 ON k=t3.y ORDER BY x,y;
} {
  QUERY PLAN
  |--MATERIALIZE t3
  |  |--SCAN t1
  |  `--USE TEMP B-TREE FOR ORDER BY
  |--SCAN t3
  `--SEARCH t2 USING PRIMARY KEY (k=?) LEFT-JOIN
} {
  NULL  NULL  NULL
  aa    bb    2
  aa    GG    7
  bb    dd    4
  bb    ee    5
  cc    cc    3
  cc    ff    6
  dd    aa    1
}


finish_test
