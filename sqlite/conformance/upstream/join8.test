# 2022-04-12
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
# This file implements tests for RIGHT and FULL OUTER JOINs.

set testdir [file dirname $argv0]
source $testdir/tester.tcl

ifcapable !vtab {
  finish_test
  return
}

db null NULL
# EVIDENCE-OF: R-33754-02880 you can say "LEFT RIGHT JOIN" which is the
# same as "FULL JOIN".
do_execsql_test join8-10 {
  CREATE TABLE t1(a,b,c);
  CREATE TABLE t2(x,y);
  CREATE INDEX t2x ON t2(x);
  SELECT avg(DISTINCT b) FROM (SELECT * FROM t2 LEFT RIGHT JOIN t1 ON c);
} {NULL}

# Pending optimization opportunity:
# Row-value initialization subroutines must be called from with the
# RIGHT JOIN body subroutine before the first use of any register containing
# the results of that subroutine.  This seems dodgy.  Test case:
#
reset_db
do_execsql_test join8-1000 {
  CREATE TABLE t1(a INTEGER PRIMARY KEY AUTOINCREMENT,b,c,d,e,f,g,h,j,k,l,m,n,o,p,q,r,s);
  CREATE INDEX t1x1 ON t1(g+h,j,k);
  CREATE INDEX t1x2 ON t1(b);
  INSERT INTO t1 DEFAULT VALUES;
} {}
do_catchsql_test join8-1010 {
  SELECT a
    FROM (
          SELECT a
            FROM (
                  SELECT a
                    FROM (
                          SELECT a FROM t1 NATURAL LEFT JOIN t1
                           WHERE (b, 2 ) IS ( SELECT 2 IN(2,2),2)
                         )
                    NATURAL LEFT FULL JOIN t1
                   WHERE ( rowid , 1 )<=(CASE 5 WHEN 619 THEN 841 ELSE 3374391096 END,0)
                   ORDER BY a ASC
                 )
            NATURAL LEFT JOIN t1
           WHERE (b, 2 ) IS ( SELECT 3 IN(3,3),3)
         )
    NATURAL LEFT FULL JOIN t1
   WHERE ( rowid , 1 )<=(CASE 5 WHEN 619 THEN 841 ELSE 3374391096 END,0)
   ORDER BY a ASC;
} {0 1}

# Pending issue #2: (now resolved)
# Jump to addrHalt inside the RIGHT JOIN body subroutine bypasses the
# OP_Return, resulting in a subroutine loop.  Test case:
#
reset_db
do_execsql_test join8-2000 {
  CREATE TABLE t1(a int, b int, c int);
  INSERT INTO t1 VALUES(1,2,3),(4,5,6);
  CREATE TABLE t2(d int, e int);
  INSERT INTO t2 VALUES(3,333),(4,444);
  CREATE TABLE t3(f int, g int);
  PRAGMA automatic_index=off;
} {}
do_catchsql_test join8-2010 {
  SELECT * FROM t1 RIGHT JOIN t2 ON c=d JOIN t3 ON f=e;
} {0 {}}

# Demonstrate that nested FULL JOINs and USING clauses work
#
reset_db
load_static_extension db series
do_execsql_test join8-3000 {
  CREATE TABLE t1(id INTEGER PRIMARY KEY, a INT);
  CREATE TABLE t2(id INTEGER PRIMARY KEY, b INT);
  CREATE TABLE t3(id INTEGER PRIMARY KEY, c INT);
  CREATE TABLE t4(id INTEGER PRIMARY KEY, d INT);
  CREATE TABLE t5(id INTEGER PRIMARY KEY, e INT);
  CREATE TABLE t6(id INTEGER PRIMARY KEY, f INT);
  CREATE TABLE t7(id INTEGER PRIMARY KEY, g INT);
  CREATE TABLE t8(id INTEGER PRIMARY KEY, h INT);
  INSERT INTO t1 SELECT value, 1 FROM generate_series(1,256) WHERE value & 1;
  INSERT INTO t2 SELECT value, 1 FROM generate_series(1,256) WHERE value & 2;
  INSERT INTO t3 SELECT value, 1 FROM generate_series(1,256) WHERE value & 4;
  INSERT INTO t4 SELECT value, 1 FROM generate_series(1,256) WHERE value & 8;
  INSERT INTO t5 SELECT value, 1 FROM generate_series(1,256) WHERE value & 16;
  INSERT INTO t6 SELECT value, 1 FROM generate_series(1,256) WHERE value & 32;
  INSERT INTO t7 SELECT value, 1 FROM generate_series(1,256) WHERE value & 64;
  INSERT INTO t8 SELECT value, 1 FROM generate_series(1,256) WHERE value & 128;
  CREATE TABLE t9 AS
    SELECT id, h, g, f, e, d, c, b, a
      FROM t1
      NATURAL FULL JOIN t2
      NATURAL FULL JOIN t3
      NATURAL FULL JOIN t4
      NATURAL FULL JOIN t5
      NATURAL FULL JOIN t6
      NATURAL FULL JOIN t7
      NATURAL FULL JOIN t8;
} {}
do_execsql_test join8-3010 {
  SELECT count(*) FROM t9;
} {255}
do_execsql_test join8-3020 {
  SELECT id, count(*) FROM t9 GROUP BY id HAVING count(*)!=1;
} {}
do_execsql_test join8-3030 {
  UPDATE t9 SET a=0 WHERE a IS NULL;
  UPDATE t9 SET b=0 WHERE b IS NULL;
  UPDATE t9 SET c=0 WHERE c IS NULL;
  UPDATE t9 SET d=0 WHERE d IS NULL;
  UPDATE t9 SET e=0 WHERE e IS NULL;
  UPDATE t9 SET f=0 WHERE f IS NULL;
  UPDATE t9 SET g=0 WHERE g IS NULL;
  UPDATE t9 SET h=0 WHERE h IS NULL;
  SELECT count(*) FROM t9 WHERE id=128*h+64*g+32*f+16*e+8*d+4*c+2*b+a;
} {255}
do_execsql_test join8-3040 {
  SELECT * FROM t9 WHERE id<>128*h+64*g+32*f+16*e+8*d+4*c+2*b+a;
} {}

# 2022-04-21 dbsqlfuzz find
#
reset_db
do_execsql_test join8-4000 {
  CREATE TABLE t1(x INTEGER PRIMARY KEY, a, b);
  INSERT INTO t1 VALUES(1,5555,4);
  CREATE INDEX i1a ON t1(a);
  CREATE INDEX i1b ON t1(b);
  SELECT a FROM t1 NATURAL RIGHT JOIN t1 WHERE a=5555 OR (1,b)==(SELECT 2 IN (2,2),4);
} {5555}

# 2022-04-23 dbsqlfuzz c7ee5500e3abddec3557016de777713b80c790d3
# Escape from the right-join body subroutine via the ORDER BY LIMIT optimization.
#
reset_db
db null -
do_catchsql_test join8-5000 {
  CREATE TABLE t1(x);
  INSERT INTO t1(x) VALUES(NULL),(NULL);
  CREATE TABLE t2(c, d);
  INSERT INTO t2(c,d) SELECT x, x FROM t1;
  CREATE INDEX t2dc ON t2(d, c);
  SELECT (SELECT c FROM sqlite_temp_schema FULL JOIN t2 ON d IN (1,2,3) ORDER BY d) AS x FROM t1;
} {0 {- -}}

# 2022-04-29 dbsqlfuzz 19f1102a70cf966ab249de56d944fc20dbebcfcf
# Verification of testtag-20230227b and testtag-20230227c
#
reset_db
do_execsql_test join8-6000 {
  CREATE TABLE t1(a INTEGER PRIMARY KEY, b TEXT, c TEXT, d REAL);
  INSERT INTO t1 VALUES(1,'A','aa',2.5);
  SELECT * FROM t1 AS t2 NATURAL RIGHT JOIN t1 AS t3
   WHERE (a,b) IN (SELECT rowid, b FROM t1);
} {1 A aa 2.5}
do_execsql_test join8-6010 {
  DROP TABLE IF EXISTS t1;
  CREATE TABLE t1(a INT PRIMARY KEY, b TEXT, c TEXT, d INT) WITHOUT ROWID;
  INSERT INTO t1 VALUES(15,'xray','baker',42);
  SELECT value, t1.* FROM json_each('7') NATURAL RIGHT JOIN t1
   WHERE (a,b) IN (SELECT a, b FROM t1);
} {7 15 xray baker 42}
do_execsql_test join8-6020 {
  DROP TABLE IF EXISTS t1;
  CREATE TABLE t1(a INTEGER PRIMARY KEY,b);
  INSERT INTO t1 VALUES(0,NULL),(1,2);
  SELECT value, t1.* FROM json_each('17') NATURAL RIGHT JOIN t1
   WHERE (a,b) IN (SELECT rowid, b FROM t1);
} {17 1 2}
do_execsql_test join8-6021 {
  SELECT value, t1.* FROM json_each('null') NATURAL RIGHT JOIN t1
   WHERE (a,b) IN (SELECT rowid, b FROM t1);
} {{} 1 2}
do_execsql_test join8-6022 {
  CREATE TABLE a(key TEXT);
  INSERT INTO a(key) VALUES('a'),('b');
  SELECT quote(a.key), b.value
    FROM a RIGHT JOIN json_each('["a","c"]') AS b ON a.key=b.value;
} {'a' a NULL c}

# Bloom filter usage by RIGHT and FULL JOIN
#
reset_db
do_execsql_test join8-7000 {
CREATE TABLE t1(a INT, b INT, c INT, d INT);
  WITH RECURSIVE c(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM c WHERE x<10)
    INSERT INTO t1(a,b,c,d) SELECT x, x+100, x+200, x+300 FROM c;
  CREATE TABLE t2(b INT, x INT);
  INSERT INTO t2(b,x) SELECT b, a FROM t1 WHERE a%2=0;
  CREATE INDEX t2b ON t2(b);
  CREATE TABLE t3(c INT, y INT);
  INSERT INTO t3(c,y) SELECT c, a FROM t1 WHERE a%3=0;
  CREATE INDEX t3c ON t3(c);
  CREATE TABLE t4(d INT, z INT);
  INSERT INTO t4(d,z) SELECT d, a FROM t1 WHERE a%5=0;
  CREATE INDEX t4d ON t4(d);
  INSERT INTO t1(a,b,c,d) VALUES
    (96,NULL,296,396),
    (97,197,NULL,397),
    (98,198,298,NULL),
    (99,NULL,NULL,NULL);
  ANALYZE sqlite_schema;
  INSERT INTO sqlite_stat1 VALUES('t4','t4d','20 1');
  INSERT INTO sqlite_stat1 VALUES('t3','t3c','32 1');
  INSERT INTO sqlite_stat1 VALUES('t2','t2b','48 1');
  INSERT INTO sqlite_stat1 VALUES('t1',NULL,'100');
  ANALYZE sqlite_schema;
} {}
db null -
do_execsql_test join8-7010 {
  WITH t0 AS MATERIALIZED (
    SELECT t1.*, t2.*, t3.*
      FROM t1 INNER JOIN t2 ON t1.b=t2.b AND t2.x>0
        RIGHT JOIN t3 ON t1.c=t3.c AND t3.y>0
  )
  SELECT * FROM t0 FULL JOIN t4 ON t0.a=t4.d AND t4.z>0
   ORDER BY coalesce(t0.a, t0.y+200, t4.d);
} {
  6  106  206  306  106  6  206  6    -   -
  -    -    -    -    -  -  200  0    -   -
  -    -    -    -    -  -  203  3    -   -
  -    -    -    -    -  -  209  9    -   -
  -    -    -    -    -  -    -  -  300   0
  -    -    -    -    -  -    -  -  305   5
  -    -    -    -    -  -    -  -  310  10
}

# EVIDENCE-OF: R-33754-02880 you can say "LEFT RIGHT JOIN" which is the
# same as "FULL JOIN".
do_execsql_test join8-7011 {
  WITH t0 AS MATERIALIZED (
    SELECT t1.*, t2.*, t3.*
      FROM t1 INNER JOIN t2 ON t1.b=t2.b AND t2.x>0
        RIGHT JOIN t3 ON t1.c=t3.c AND t3.y>0
  )
  SELECT * FROM t0 LEFT RIGHT JOIN t4 ON t0.a=t4.d AND t4.z>0
   ORDER BY coalesce(t0.a, t0.y+200, t4.d);
} {
  6  106  206  306  106  6  206  6    -   -
  -    -    -    -    -  -  200  0    -   -
  -    -    -    -    -  -  203  3    -   -
  -    -    -    -    -  -  209  9    -   -
  -    -    -    -    -  -    -  -  300   0
  -    -    -    -    -  -    -  -  305   5
  -    -    -    -    -  -    -  -  310  10
}

do_execsql_test join8-7020 {
  EXPLAIN QUERY PLAN
  WITH t0 AS MATERIALIZED (
    SELECT t1.*, t2.*, t3.*
      FROM t1 INNER JOIN t2 ON t1.b=t2.b AND t2.x>0
        RIGHT JOIN t3 ON t1.c=t3.c AND t3.y>0
  )
  SELECT * FROM t0 FULL JOIN t4 ON t0.a=t4.d AND t4.z>0
   ORDER BY coalesce(t0.a, t0.y+200, t4.d);
} {/.*BLOOM FILTER ON t2.*BLOOM FILTER ON t3.*/}

# 2022-05-12 Difference with PG found (by Dan) while exploring
# https://sqlite.org/forum/forumpost/677a0ab93fcd9ccd
#
reset_db
do_execsql_test join8-8000 {
  CREATE TABLE t1(a INT, b INT);
  CREATE TABLE t2(c INT, d INT);
  CREATE TABLE t3(e INT, f INT);
  INSERT INTO t1 VALUES(1, 2);
  INSERT INTO t2 VALUES(3, 4);
  INSERT INTO t3 VALUES(5, 6);
} {}
do_execsql_test join8-8010 {
  SELECT *
    FROM t3 LEFT JOIN t2 ON true
            JOIN t1 ON (t3.e IS t2.c);
} {}
do_execsql_test join8-8020 {
  SELECT *
    FROM t3 LEFT JOIN t2 ON true
            JOIN t1 ON (t3.e IS NOT DISTINCT FROM t2.c);
} {}

# 2022-05-13 The idea of reusing subquery cursors does not
# work, if the cursors are used both for scanning and lookups.
#
reset_db
db null -
do_execsql_test join8-9000 {
  CREATE TABLE t1(a INTEGER PRIMARY KEY, b TEXT, c TEXT, d REAL);
  INSERT INTO t1 VALUES(1,'E','bb',NULL),(2,NULL,NULL,NULL);
  SELECT * FROM t1 NATURAL RIGHT JOIN t1 AS t2 WHERE (a,b) IN (SELECT a+0, b FROM t1);
} {1 E bb -}

# 2022-05-14 https://sqlite.org/forum/forumpost/c06b10ad7e
#
reset_db
db null -
do_execsql_test join8-10000 {
  CREATE TABLE t1(c0 INT UNIQUE);
  CREATE TABLE t2(c0);
  CREATE TABLE t2i(c0 INT);
  CREATE TABLE t3(c0 INT);
  INSERT INTO t1 VALUES(1);
  INSERT INTO t2 VALUES(2);
  INSERT INTO t2i VALUES(2);
  INSERT INTO t3 VALUES(3);
} {}
do_execsql_test join8-10010 {
  SELECT DISTINCT t1.c0, t3.c0
    FROM t2 NATURAL JOIN t1 RIGHT JOIN t3 ON t1.c0;
} {- 3}
do_execsql_test join8-10020 {
  SELECT t1.c0, t3.c0
    FROM t2 NATURAL JOIN t1 RIGHT JOIN t3 ON t1.c0;
} {- 3}
do_execsql_test join8-10030 {
  SELECT DISTINCT t1.c0, t3.c0
    FROM t2 NATURAL CROSS JOIN t1 RIGHT JOIN t3 ON t1.c0;
} {- 3}
do_execsql_test join8-10040 {
  SELECT t1.c0, t3.c0
    FROM t1 NATURAL CROSS JOIN t2 RIGHT JOIN t3 ON t1.c0;
} {- 3}
do_execsql_test join8-10050 {
  SELECT DISTINCT t1.c0, t3.c0
    FROM t2i NATURAL JOIN t1 RIGHT JOIN t3 ON t1.c0;
} {- 3}
do_execsql_test join8-10060 {
  SELECT DISTINCT +t1.c0, t3.c0
    FROM t2 NATURAL JOIN t1 RIGHT JOIN t3 ON t1.c0;
} {- 3}
do_execsql_test join8-10070 {
  SELECT DISTINCT +t1.c0, t3.c0
    FROM t1 NATURAL CROSS JOIN t2 RIGHT JOIN t3 ON t1.c0;
} {- 3}
do_execsql_test join8-10080 {
  SELECT DISTINCT t1.c0, t3.c0
    FROM t2 NATURAL JOIN t1 RIGHT JOIN t3 ON t1.c0<>0;
} {- 3}

# 2022-05-14
# index-on-expr scan on a RIGHT JOIN
# dbsqlfuzz 39ee60004ff027a9e2846cf76e02cd5ac0953739
#
reset_db
db null -
do_execsql_test join8-11000 {
  CREATE TABLE t1(a);
  CREATE TABLE t2(b);
  INSERT INTO t2 VALUES(0),(1),(2);
  SELECT * FROM t1 RIGHT JOIN t2 ON (a=b) WHERE 99+(b+1)!=99;
} {- 0 - 1 - 2}
do_execsql_test join8-11010 {
  CREATE INDEX t2b ON t2(b+1) WHERE b IS NOT NULL;
  SELECT * FROM t1 RIGHT JOIN t2 ON (a=b) WHERE 99+(b+1)!=99;
} {- 0 - 1 - 2}
do_execsql_test join8-11020 {
  DROP TABLE t1;
  DROP TABLE t2;
  CREATE TABLE t1(a);
  CREATE TABLE t2(b, c, d);
  INSERT INTO t2 VALUES(1, 3, 'not-4');
  SELECT b, d FROM t1 RIGHT JOIN t2 WHERE (b+0)=1 AND d!=4;
} {1 not-4}
do_execsql_test join8-11030 {
  CREATE INDEX i2 ON t2((b+0), d);
  SELECT b, d FROM t1 RIGHT JOIN t2 WHERE (b+0)=1 AND d!=4;
} {1 not-4}
do_execsql_test join8-11040 {
  DROP INDEX i2;
  CREATE INDEX i2 ON t2((b+0), d) WHERE d IS NOT NULL;
  SELECT b, d FROM t1 RIGHT JOIN t2 WHERE (b+0)=1 AND d!=4;
} {1 not-4}

# 2022-05-23
# NATURAL JOIN name resolution is more forgiving with LEFT JOIN
# https://sqlite.org/forum/forumpost/e90a8e6e6f
#
reset_db
db null -
do_execsql_test join8-12000 {
  CREATE TABLE t1(a INT);  INSERT INTO t1 VALUES(0),(1);
  CREATE TABLE t2(a INT);  INSERT INTO t2 VALUES(0),(2);
  CREATE TABLE t3(a INT);  INSERT INTO t3 VALUES(0),(3);
} {}
do_catchsql_test join8-12010 {
  SELECT * FROM t1 RIGHT JOIN t2 ON t2.a<>0 NATURAL RIGHT JOIN t3;
} {1 {ambiguous reference to a in USING()}}
do_catchsql_test join8-12020 {
  SELECT * FROM t1 RIGHT JOIN t2 ON t2.a<>0 NATURAL LEFT JOIN t3;
} {1 {ambiguous reference to a in USING()}}
do_catchsql_test join8-12030 {
  SELECT * FROM t1 LEFT JOIN t2 ON t2.a<>0 NATURAL RIGHT JOIN t3;
} {1 {ambiguous reference to a in USING()}}

# The following query should probably also return the same error as the
# previous three cases.  However, historical versions of SQLite have always
# let it pass.  We will not "fix" this, since to do so might break legacy
# applications.
#
do_catchsql_test join8-12040 {
  SELECT * FROM t1 LEFT JOIN t2 ON t2.a<>0 NATURAL LEFT JOIN t3;
} {0 {0 2 1 2}}

# 2022-05-24
# https://sqlite.org/forum/forumpost/687b0bf563a1d4f1
#
reset_db
do_execsql_test join8-13000 {
  CREATE TABLE t0(t TEXT, u TEXT);  INSERT INTO t0 VALUES('t', 'u');
  CREATE TABLE t1(v TEXT, w TEXT);  INSERT INTO t1 VALUES('v', 'w');
  CREATE TABLE t2(x TEXT, y TEXT);  INSERT INTO t2 VALUES('x', 'y');
  SELECT * FROM t0 JOIN t1 ON (t2.x NOTNULL) LEFT JOIN t2 ON false;
  SELECT * FROM t0 JOIN t1 ON (t2.x NOTNULL) LEFT JOIN t2 ON false
   WHERE t2.y ISNULL;
} {}

# 2022-05-25
# https://sqlite.org/forum/forumpost/5cfe08eed6
#
reset_db
do_execsql_test join8-14000 {
  CREATE TABLE t0(a TEXT, b TEXT, c TEXT);
  CREATE TABLE t1(a TEXT);
  INSERT INTO t1 VALUES('1');
  CREATE VIEW v0 AS SELECT 'xyz' AS d;
  SELECT * FROM v0 RIGHT JOIN t1 ON t1.a<>'' INNER JOIN t0 ON t0.c<>'';
  SELECT * FROM v0 RIGHT JOIN t1 ON t1.a<>'' INNER JOIN t0 ON t0.c<>'' WHERE b ISNULL;
} {}
do_execsql_test join8-14010 {
  CREATE TABLE y0(a INT);
  CREATE TABLE y1(b INT); INSERT INTO y1 VALUES(1), (2);
  CREATE TABLE y2(c INT); INSERT INTO y2 VALUES(3), (4);
} {}
db null -
do_execsql_test join8-14020 {
  SELECT * FROM y0 RIGHT JOIN y1 ON true INNER JOIN y2 ON true WHERE y2.c!=99 AND y2.c!=98;
} {
  - 1 3
  - 1 4
  - 2 3
  - 2 4
}

# 2022-05-30
# https://sqlite.org/forum/forumpost/3902c7b833
#
reset_db
do_execsql_test join8-15000 {
  CREATE TABLE t1(x INT);
  CREATE TABLE t2(y INT);
  CREATE TABLE t3(z INT);
  INSERT INTO t1 VALUES(10);
  INSERT INTO t3 VALUES(20),(30);
}
do_execsql_test join8-15010 {
  SELECT * FROM t1 LEFT JOIN t2 ON true JOIN t3 ON t2.y IS NOT NULL;
} {}
do_execsql_test join8-15020 {
  SELECT * FROM t1 LEFT JOIN t2 ON true JOIN t3 ON t2.y IS NOT NULL
   WHERE (t3.z!=400 AND t3.z!=500 AND t3.z!=600);
} {}
do_execsql_test join8-15100 {
  PRAGMA automatic_index = 0;
  CREATE TABLE t4(x TEXT);
  CREATE TABLE t5(y TEXT);
  CREATE TABLE t6(z TEXT);
  INSERT INTO t4 VALUES('a'), ('b');
  INSERT INTO t5 VALUES('b'), ('c');
  INSERT INTO t6 VALUES('a'), ('d');
} {}
db null -
do_execsql_test join8-15110 {
  SELECT * FROM t4 LEFT JOIN t5 ON x=y LEFT JOIN t6 ON (x=z) ORDER BY +x;
} {a - a b b -}
do_execsql_test join8-15120 {
  SELECT * FROM t4 LEFT JOIN t5 ON x=y LEFT JOIN t6 ON (x=z)
   WHERE t5.y!='x' AND t4.x!='x';
} {b b -}

# 2022-05-31
# https://sqlite.org/forum/forumpost/c2554d560b
reset_db
do_execsql_test join8-16000 {
  CREATE TABLE t1(a TEXT);
  CREATE TABLE t2(b TEXT);
  CREATE TABLE t3(c TEXT);
  INSERT INTO t2(b) VALUES ('x');
  INSERT INTO t3(c) VALUES ('y'), ('z');
} {}
db null -
do_execsql_test join8-16010 {
  SELECT * FROM t1 RIGHT JOIN t2 ON true LEFT JOIN t3 ON a<>'';
} {- x -}
do_execsql_test join8-16020 {
  SELECT * FROM t1 RIGHT JOIN t2 ON true LEFT JOIN t3 ON a<>'' WHERE c IS NULL;
} {- x -}
do_execsql_test join8-16020 {
  SELECT * FROM t1 RIGHT JOIN t2 ON true JOIN t3 ON a<>'' WHERE c IS NULL;
} {}
do_execsql_test join8-16030 {
  SELECT * FROM t1 RIGHT JOIN t2 ON true JOIN t3 ON a<>'';
} {}
do_execsql_test join8-16040 {
  SELECT * FROM t1 RIGHT JOIN t2 ON true LEFT JOIN t3 ON a<>'' WHERE c<>'';
} {}
do_execsql_test join8-16050 {
  SELECT * FROM t1 RIGHT JOIN t2 ON true LEFT JOIN t3 ON a<>'' WHERE c IS NOT NULL;
} {}
do_execsql_test join8-16060 {
  SELECT * FROM t1 RIGHT JOIN t2 ON true JOIN t3 ON a<>'' WHERE c<>'';
} {}
do_execsql_test join8-16070 {
  SELECT * FROM t1 RIGHT JOIN t2 ON true JOIN t3 ON a<>'' WHERE c IS NOT NULL;
} {}

# 2022-06-01
# https://sqlite.org/forum/forumpost/087de2d9ec
#
reset_db
do_execsql_test join8-17000 {
  CREATE TABLE t1(id INTEGER PRIMARY KEY, x INT, y INT);
  CREATE TABLE t2(z INT);
  INSERT INTO t1(id,x,y) VALUES(1, 0, 0);
} {}
db null NULL
do_execsql_test join8-17010 {
  SELECT * FROM t2 RIGHT JOIN t1 ON true;
} {NULL 1 0 0}
do_execsql_test join8-17020 {
  SELECT 99=id AND 0=y AS "truth" FROM t2 RIGHT JOIN t1 ON true;
} {0}
do_execsql_test join8-17030 {
  SELECT (99, 0)==(id, y) AS "truth" FROM t2 RIGHT JOIN t1;
} {0}
do_execsql_test join8-17040 {
  SELECT * FROM t2 RIGHT JOIN t1 WHERE 99=id AND 0=y;
} {}
do_execsql_test join8-17041 {
  SELECT * FROM t2 RIGHT JOIN t1 WHERE 99=+id AND 0=y;
} {}
do_execsql_test join8-17050 {
  SELECT * FROM t2 RIGHT JOIN t1 WHERE (99, 0)==(id,y);
} {}
do_execsql_test join8-17051 {
  SELECT * FROM t2 RIGHT JOIN t1 WHERE (99, 0)==(+id,y);
} {}
do_execsql_test join8-17060 {
  SELECT * FROM t2 RIGHT JOIN t1 WHERE 1=id AND 0=y;
} {NULL 1 0 0}
do_execsql_test join8-17061 {
  SELECT * FROM t2 RIGHT JOIN t1 WHERE 1=+id AND 0=y;
} {NULL 1 0 0}
do_execsql_test join8-17070 {
  SELECT * FROM t2 RIGHT JOIN t1 WHERE (1, 0)==(id,y);
} {NULL 1 0 0}
do_execsql_test join8-17071 {
  SELECT * FROM t2 RIGHT JOIN t1 WHERE (1, 0)==(+id,y);
} {NULL 1 0 0}
do_execsql_test join8-17080 {
  CREATE TABLE t3(a INTEGER PRIMARY KEY, b INT);
  CREATE TABLE t4(x INT, y INT);
  INSERT INTO t3(a,b) VALUES(1, 3);
} {}
do_execsql_test join8-17090 {
  SELECT t3.a FROM t4 RIGHT JOIN t3 ON (x=a) WHERE (b, 4)=(SELECT 3, 4);
} {1}
do_execsql_test join8-17091 {
  SELECT t3.a FROM t4 RIGHT JOIN t3 ON (x=a) WHERE (b, 4) IS (SELECT 3, 4);
} {1}

# 2022-06-06
# https://sqlite.org/forum/forumpost/206d99a16dd9212f
# tag-20191211-001
#
reset_db
do_execsql_test join8-18000 {
  CREATE TABLE t1(a BOOLEAN); INSERT INTO t1 VALUES (false);
  CREATE TABLE t2(x INT);     INSERT INTO t2 VALUES (0);
  SELECT *, x NOTNULL, (x NOTNULL)=a FROM t2 RIGHT JOIN t1 ON true WHERE (x NOTNULL)=a;
} {}
do_execsql_test join8-18010 {
  CREATE INDEX t1a ON t1(a);
  SELECT *, x NOTNULL, (x NOTNULL)=a FROM t2 RIGHT JOIN t1 ON true WHERE (x NOTNULL)=a;
} {}

do_execsql_test join8-18020 {
  CREATE TABLE t3(z);
  INSERT INTO t3 VALUES('t3value');
  SELECT *, x NOTNULL, (x NOTNULL)=a FROM t2 RIGHT JOIN t1 ON true INNER JOIN t3 ON (x NOTNULL)=a;
} {}

ifcapable rtree {
  do_execsql_test join8-18030 {
    CREATE VIRTUAL TABLE rtree1 USING rtree(a, x1, x2);
    INSERT INTO rtree1 VALUES(0, 0, 0);
  }
  do_execsql_test join8-18040 {
    SELECT *, x NOTNULL, (x NOTNULL)=a FROM t2 
      RIGHT JOIN rtree1 ON true INNER JOIN t3 ON (x NOTNULL)=+a;
  } {}
  do_execsql_test join8-18050 {
    SELECT *, x NOTNULL, (x NOTNULL)=a FROM t2 
      RIGHT JOIN rtree1 ON true INNER JOIN t3 ON (x NOTNULL)=a;
  } {}
}


reset_db
do_execsql_test join8-19000 {
  CREATE TABLE t1(a INT);
  CREATE TABLE t2(b INT, c INT);
  CREATE TABLE t3(d INT);

  INSERT INTO t1 VALUES(10);
  INSERT INTO t2 VALUES(50,51);
  INSERT INTO t3 VALUES(299);

  CREATE INDEX t2b ON t2( (b IS NOT NULL) );
}

do_execsql_test join8-19010 {
  SELECT * FROM t1 LEFT JOIN t2 ON true INNER JOIN t3 ON (b IS NOT NULL)=0;
}

# 2022-06-07
# https://sqlite.org/forum/forumpost/323f86cc30
reset_db
do_execsql_test join8-20000 {
  CREATE TABLE t1(x TEXT);
  INSERT INTO t1(x) VALUES('aaa');
  CREATE VIEW v0(y) AS SELECT x FROM t1;
  CREATE TABLE t2(z TEXT);
} {}
db null -
do_execsql_test join8-20010 {
  SELECT * FROM t2 JOIN v0 ON z<>'bbb' RIGHT JOIN t1 ON z<>'ccc';
} {- - aaa}
do_execsql_test join8-20020 {
  SELECT * FROM t2 JOIN v0 ON z<>'bbb' RIGHT JOIN t1 ON z<>'ccc' ORDER BY z;
} {- - aaa}
do_execsql_test join8-20030 {
  SELECT 99 as "m" FROM t2 JOIN v0 ON z<>'bbb' RIGHT JOIN t1 ON z<>'ccc';
} {99}
do_execsql_test join8-20040 {
  SELECT 99 as "m" FROM t2 JOIN v0 ON z<>'bbb' RIGHT JOIN t1 ON z<>'ccc' ORDER BY z;
} {99}
do_execsql_test join8-20050 {
  SELECT count(*)
    FROM (SELECT 99 as "m" FROM t2 JOIN v0 ON z<>'' RIGHT JOIN t1 ON z<>'') AS "t3";
} {1}
do_execsql_test join8-20060 {
  SELECT count(*) 
    FROM (SELECT 99 as "m" FROM t2 JOIN v0 ON z<>'' RIGHT JOIN t1 ON z<>'' ORDER BY z) AS "t3";
} {1}

# 2022-06-10
# https://sqlite.org/forum/forumpost/8e4c352937e82929
#
# Do not allow constant propagation between ON and WHERE clause terms.
# (Updated 2022-06-20) See also https://sqlite.org/forum/forumpost/57bdf2217d
#
reset_db
do_execsql_test join8-21000 {
  CREATE TABLE t1(a INT,b BOOLEAN);
  CREATE TABLE t2(c INT);  INSERT INTO t2 VALUES(NULL);
  CREATE TABLE t3(d INT);
}
do_execsql_test join8-21010 {
  SELECT (b IS TRUE) FROM t1 JOIN t3 ON (b=TRUE) RIGHT JOIN t2 ON TRUE;
} {0}
do_execsql_test join8-22020 {
  SELECT * FROM t1 JOIN t3 ON (b=TRUE) RIGHT JOIN t2 ON TRUE WHERE (b IS TRUE);
} {}
do_execsql_test join8-22030 {
  DROP TABLE t1;
  DROP TABLE t2;
  DROP TABLE t3;
  CREATE TABLE t1(a INT);
  CREATE TABLE t2(b INT);
  CREATE TABLE t3(c INTEGER PRIMARY KEY, d INT);
  CREATE INDEX t3d ON t3(d);
  INSERT INTO t3 VALUES(0, 0);
}
do_catchsql_test join8-22031 {
  SELECT * FROM t1 JOIN t2 ON d>b RIGHT JOIN t3 ON true WHERE +d = 0;
} {1 {ON clause references tables to its right}}
do_catchsql_test join8-22040 {
  SELECT * FROM t1 JOIN t2 ON d>b RIGHT JOIN t3 ON true WHERE d = 0;
} {1 {ON clause references tables to its right}}


# 2022-06-10
# https://sqlite.org/forum/forumpost/51e6959f61
#
# Restrictions on the usage of WHERE clause constraints by joins that are
# involved with a RIGHT JOIN must also be applied to automatic indexes.
#
reset_db
do_execsql_test join8-22000 {
  CREATE TABLE t1(a INT);
  CREATE TABLE t2(b INT);
  CREATE TABLE t3(c TEXT);  INSERT INTO t3 VALUES('x');
  CREATE TABLE t4(d TEXT);  INSERT INTO t4 VALUES('y');
  SELECT 99
    FROM t1
         LEFT JOIN t2 ON true
         RIGHT JOIN t3 ON true
         RIGHT JOIN t4 ON true
   WHERE a=b;
} {}

# 2022-06-13
# https://sqlite.org/forum/forumpost/b40696f501
#
# This optimization that converts "x ISNULL" into "FALSE" when column "x" has a
# NOT NULL constraint is too aggresive if the query contains RIGHT JOIN.
#
reset_db
db null -
do_execsql_test join8-23000 {
  CREATE TABLE t1(a TEXT);
  INSERT INTO t1 VALUES('c');
  CREATE TABLE t2(b TEXT, c TEXT NOT NULL);
  INSERT INTO t2 VALUES('a', 'b');
  CREATE TABLE t3(d TEXT);
  INSERT INTO t3 VALUES('x');
  CREATE TABLE t4(e TEXT);
  INSERT INTO t4 VALUES('y');
}
do_execsql_test join8-23010 {
  SELECT *
    FROM t1
         LEFT JOIN t2 ON TRUE
         JOIN t3 ON c=''
         RIGHT JOIN t4 ON b='';
} {- - - - y}
do_execsql_test join8-23020 {
  SELECT *
    FROM t1
         LEFT JOIN t2 ON TRUE
         JOIN t3 ON c=''
         RIGHT JOIN t4 ON b=''
   WHERE d ISNULL
} {- - - - y}

# 2022-06-14
# dbsqlfuzz 2f3101834d14325a976f601b9267a0fd323d6bbd
#
# When the OP_NullRow opcode creates a new cursor, it must
# set the cursor to no-reuse so that an OP_OpenEphemeral in
# a subroutine does not try to reuse it.
#
reset_db
db null -
do_execsql_test join8-24000 {
  CREATE TABLE t4(b INT, c INT);
  CREATE TABLE t5(a INT, f INT);
  INSERT INTO t5 VALUES(1,2);
  WITH t7(x, y) AS (SELECT 100, 200 FROM t5)
    SELECT * FROM t4 JOIN t7 ON true RIGHT JOIN (SELECT y AS z FROM t7) AS t6 ON (x=z);
} {- - - - 200}

# 2022-06-20
# forum/forumpost/6650cd40b5634f35
#
reset_db
do_execsql_test join8-25000 {
  CREATE TABLE t1(a1 INT);
  CREATE TABLE t2(b2 INT);
  CREATE TABLE t3(c3 INT, d3 INT UNIQUE);
  CREATE TABLE t4(e4 INT, f4 TEXT);
  INSERT INTO t3(c3, d3) VALUES (2, 1);
  INSERT INTO t4(f4) VALUES ('x');
  CREATE INDEX i0 ON t3(c3) WHERE d3 ISNULL;
  ANALYZE main;
}
db null -
do_execsql_test join8-25010 {
  SELECT * FROM t1 LEFT JOIN t2 ON true JOIN t3 ON (b2 IN (a1)) FULL JOIN t4 ON true;
} {- - - - - x}
do_execsql_test join8-25020 {
  SELECT 1 FROM t1 LEFT JOIN t2 ON true JOIN t3 ON (b2 IN (a1)) FULL JOIN t4 ON true;
} {1}

# 2022-07-13
# forum/forumpost/174afeae57
#
reset_db
db null -
do_execsql_test join8-26000 {
  CREATE TABLE t1(a INT);
  CREATE TABLE t2(b INT, c INT);
  CREATE VIEW t3(d) AS SELECT NULL FROM t2 FULL OUTER JOIN t1 ON c=a UNION ALL SELECT b FROM t2;
  INSERT INTO t1(a) VALUES (NULL);
  INSERT INTO t2(b, c) VALUES (99, NULL);
  SELECT DISTINCT b, c, d FROM t2, t3 WHERE b<>0
   UNION SELECT DISTINCT b, c, d FROM t2, t3 WHERE b ISNULL;
} {99 - - 99 - 99}

finish_test
