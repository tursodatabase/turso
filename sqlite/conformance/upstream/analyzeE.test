# 2014-10-08
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
# This file implements tests for using STAT4 information
# on a descending index in a range query.
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set ::testprefix analyzeE

ifcapable {!stat4} {
  finish_test
  return
}

# Verify that range queries on an ASCENDING index will use the
# index only if the range covers only a small fraction of the
# entries.
#
do_execsql_test analyzeE-1.0 {
  CREATE TABLE t1(a,b);
  WITH RECURSIVE
    cnt(x) AS (VALUES(1000) UNION ALL SELECT x+1 FROM cnt WHERE x<2000)
  INSERT INTO t1(a,b) SELECT x, x FROM cnt;
  CREATE INDEX t1a ON t1(a);
  ANALYZE;
} {}
do_execsql_test analyzeE-1.1 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a BETWEEN 500 AND 2500;
} {/SCAN t1/}
do_execsql_test analyzeE-1.2 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a BETWEEN 2900 AND 3000;
} {/SEARCH t1 USING INDEX t1a/}
do_execsql_test analyzeE-1.3 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a BETWEEN 1700 AND 1750;
} {/SEARCH t1 USING INDEX t1a/}
do_execsql_test analyzeE-1.4 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a BETWEEN 1 AND 500
} {/SEARCH t1 USING INDEX t1a/}
do_execsql_test analyzeE-1.5 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a BETWEEN 3000 AND 3000000
} {/SEARCH t1 USING INDEX t1a/}
do_execsql_test analyzeE-1.6 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a<500
} {/SEARCH t1 USING INDEX t1a/}
do_execsql_test analyzeE-1.7 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a>2500
} {/SEARCH t1 USING INDEX t1a/}
do_execsql_test analyzeE-1.8 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a>1900
} {/SEARCH t1 USING INDEX t1a/}
do_execsql_test analyzeE-1.9 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a>1100
} {/SCAN t1/}
do_execsql_test analyzeE-1.10 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a<1100
} {/SEARCH t1 USING INDEX t1a/}
do_execsql_test analyzeE-1.11 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a<1900
} {/SCAN t1/}

# Verify that everything works the same on a DESCENDING index.
#
do_execsql_test analyzeE-2.0 {
  DROP INDEX t1a;
  CREATE INDEX t1a ON t1(a DESC);
  ANALYZE;
} {}
do_execsql_test analyzeE-2.1 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a BETWEEN 500 AND 2500;
} {/SCAN t1/}
do_execsql_test analyzeE-2.2 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a BETWEEN 2900 AND 3000;
} {/SEARCH t1 USING INDEX t1a/}
do_execsql_test analyzeE-2.3 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a BETWEEN 1700 AND 1750;
} {/SEARCH t1 USING INDEX t1a/}
do_execsql_test analyzeE-2.4 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a BETWEEN 1 AND 500
} {/SEARCH t1 USING INDEX t1a/}
do_execsql_test analyzeE-2.5 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a BETWEEN 3000 AND 3000000
} {/SEARCH t1 USING INDEX t1a/}
do_execsql_test analyzeE-2.6 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a<500
} {/SEARCH t1 USING INDEX t1a/}
do_execsql_test analyzeE-2.7 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a>2500
} {/SEARCH t1 USING INDEX t1a/}
do_execsql_test analyzeE-2.8 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a>1900
} {/SEARCH t1 USING INDEX t1a/}
do_execsql_test analyzeE-2.9 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a>1100
} {/SCAN t1/}
do_execsql_test analyzeE-2.10 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a<1100
} {/SEARCH t1 USING INDEX t1a/}
do_execsql_test analyzeE-2.11 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a<1900
} {/SCAN t1/}

# Now do a range query on the second term of an ASCENDING index
# where the first term is constrained by equality.
#
do_execsql_test analyzeE-3.0 {
  DROP TABLE t1;
  CREATE TABLE t1(a,b,c);
  WITH RECURSIVE
    cnt(x) AS (VALUES(1000) UNION ALL SELECT x+1 FROM cnt WHERE x<2000)
  INSERT INTO t1(a,b,c) SELECT x, x, 123 FROM cnt;
  CREATE INDEX t1ca ON t1(c,a);
  ANALYZE;
} {}
do_execsql_test analyzeE-3.1 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a BETWEEN 500 AND 2500 AND c=123;
} {/SCAN t1/}
do_execsql_test analyzeE-3.2 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a BETWEEN 2900 AND 3000 AND c=123;
} {/SEARCH t1 USING INDEX t1ca/}
do_execsql_test analyzeE-3.3 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a BETWEEN 1700 AND 1750 AND c=123;
} {/SEARCH t1 USING INDEX t1ca/}
do_execsql_test analyzeE-3.4 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a BETWEEN 1 AND 500 AND c=123
} {/SEARCH t1 USING INDEX t1ca/}
do_execsql_test analyzeE-3.5 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a BETWEEN 3000 AND 3000000 AND c=123
} {/SEARCH t1 USING INDEX t1ca/}
do_execsql_test analyzeE-3.6 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a<500 AND c=123
} {/SEARCH t1 USING INDEX t1ca/}
do_execsql_test analyzeE-3.7 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a>2500 AND c=123
} {/SEARCH t1 USING INDEX t1ca/}
do_execsql_test analyzeE-3.8 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a>1900 AND c=123
} {/SEARCH t1 USING INDEX t1ca/}
do_execsql_test analyzeE-3.9 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a>1100 AND c=123
} {/SCAN t1/}
do_execsql_test analyzeE-3.10 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a<1100 AND c=123
} {/SEARCH t1 USING INDEX t1ca/}
do_execsql_test analyzeE-3.11 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a<1900 AND c=123
} {/SCAN t1/}

# Repeat the 3.x tests using a DESCENDING index
#
do_execsql_test analyzeE-4.0 {
  DROP INDEX t1ca;
  CREATE INDEX t1ca ON t1(c ASC,a DESC);
  ANALYZE;
} {}
do_execsql_test analyzeE-4.1 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a BETWEEN 500 AND 2500 AND c=123;
} {/SCAN t1/}
do_execsql_test analyzeE-4.2 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a BETWEEN 2900 AND 3000 AND c=123;
} {/SEARCH t1 USING INDEX t1ca/}
do_execsql_test analyzeE-4.3 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a BETWEEN 1700 AND 1750 AND c=123;
} {/SEARCH t1 USING INDEX t1ca/}
do_execsql_test analyzeE-4.4 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a BETWEEN 1 AND 500 AND c=123
} {/SEARCH t1 USING INDEX t1ca/}
do_execsql_test analyzeE-4.5 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a BETWEEN 3000 AND 3000000 AND c=123
} {/SEARCH t1 USING INDEX t1ca/}
do_execsql_test analyzeE-4.6 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a<500 AND c=123
} {/SEARCH t1 USING INDEX t1ca/}
do_execsql_test analyzeE-4.7 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a>2500 AND c=123
} {/SEARCH t1 USING INDEX t1ca/}
do_execsql_test analyzeE-4.8 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a>1900 AND c=123
} {/SEARCH t1 USING INDEX t1ca/}
do_execsql_test analyzeE-4.9 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a>1100 AND c=123
} {/SCAN t1/}
do_execsql_test analyzeE-4.10 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a<1100 AND c=123
} {/SEARCH t1 USING INDEX t1ca/}
do_execsql_test analyzeE-4.11 {
  EXPLAIN QUERY PLAN
  SELECT * FROM t1 WHERE a<1900 AND c=123
} {/SCAN t1/}

# 2023-03-23 https://sqlite.org/forum/forumpost/dc4854437b
#
reset_db
do_execsql_test analyzeE-5.0 {
  PRAGMA encoding = 'UTF-16';
  CREATE TABLE t0 (c1 TEXT);
  INSERT INTO t0 VALUES ('');
  CREATE INDEX i0 ON t0(c1);
  ANALYZE;
  SELECT * FROM t0 WHERE t0.c1 BETWEEN '' AND (ABS(''));
} {{}}

# 2023-03-24 https://sqlite.org/forum/forumpost/bc39e531e5
#
reset_db
do_execsql_test analyzeE-6.0 {
  CREATE TABLE t1(x);
  CREATE INDEX i1 ON t1(x,x,x,x,x||2);
  CREATE INDEX i2 ON t1(1<2);
  WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<1000)
    INSERT INTO t1(x) SELECT x FROM c;
  ANALYZE;
} {}
do_execsql_test analyzeE-6.1 {
  SELECT count(*)>1 FROM sqlite_stat4 WHERE idx='i2' AND neq='1000 1';
} 1
do_execsql_test analyzeE-6.2 {
  SELECT count(*) FROM sqlite_stat4 WHERE idx='i2' AND neq<>'1000 1';
} 0
do_execsql_test analyzeE-6.3 {
  SELECT count(*)>1 FROM sqlite_stat4 WHERE idx='i1' AND neq='1 1 1 1 1 1';
} 1
do_execsql_test analyzeE-6.4 {
  SELECT count(*) FROM sqlite_stat4 WHERE idx='i1' AND neq<>'1 1 1 1 1 1';
} 0

# 2023-03-25 https://sqlite.org/forum/forumpost/5275207102
# Correctly expand zeroblobs while processing STAT4 information
# during query planning.
#
reset_db
do_execsql_test analyzeE-7.0 {
  CREATE TABLE t1(a TEXT COLLATE binary);
  CREATE INDEX t1x ON t1(a);
  INSERT INTO t1(a) VALUES(0),('apple'),(NULL),(''),('banana');
  ANALYZE;
  SELECT format('(%s)',a) FROM t1 WHERE t1.a > CAST(zeroblob(5) AS TEXT);
} {(0) (apple) (banana)}
do_execsql_test analyzeE-7.1 {
  SELECT format('(%s)',a) FROM t1 WHERE t1.a <= CAST(zeroblob(5) AS TEXT);
} {()}

finish_test
