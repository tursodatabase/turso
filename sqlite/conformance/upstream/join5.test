# 2005 September 19
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
# This file implements tests for left outer joins containing ON
# clauses that restrict the scope of the left term of the join.
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix join5


do_test join5-1.1 {
  execsql {
    BEGIN;
    CREATE TABLE t1(a integer primary key, b integer, c integer);
    CREATE TABLE t2(x integer primary key, y);
    CREATE TABLE t3(p integer primary key, q);
    INSERT INTO t3 VALUES(11,'t3-11');
    INSERT INTO t3 VALUES(12,'t3-12');
    INSERT INTO t2 VALUES(11,'t2-11');
    INSERT INTO t2 VALUES(12,'t2-12');
    INSERT INTO t1 VALUES(1, 5, 0);
    INSERT INTO t1 VALUES(2, 11, 2);
    INSERT INTO t1 VALUES(3, 12, 1);
    COMMIT;
  }
} {}
do_test join5-1.2 {
  execsql {
    select * from t1 left join t2 on t1.b=t2.x and t1.c=1
  }
} {1 5 0 {} {} 2 11 2 {} {} 3 12 1 12 t2-12}
do_test join5-1.3 {
  execsql {
    select * from t1 left join t2 on t1.b=t2.x where t1.c=1
  }
} {3 12 1 12 t2-12}
do_test join5-1.4 {
  execsql {
    select * from t1 left join t2 on t1.b=t2.x and t1.c=1
                     left join t3 on t1.b=t3.p and t1.c=2
  }
} {1 5 0 {} {} {} {} 2 11 2 {} {} 11 t3-11 3 12 1 12 t2-12 {} {}}
do_test join5-1.5 {
  execsql {
    select * from t1 left join t2 on t1.b=t2.x and t1.c=1
                     left join t3 on t1.b=t3.p where t1.c=2
  }
} {2 11 2 {} {} 11 t3-11}

# Ticket #2403
#
do_test join5-2.1 {
  execsql {
    CREATE TABLE ab(a,b);
    INSERT INTO "ab" VALUES(1,2);
    INSERT INTO "ab" VALUES(3,NULL);

    CREATE TABLE xy(x,y);
    INSERT INTO "xy" VALUES(2,3);
    INSERT INTO "xy" VALUES(NULL,1);
  }
  execsql {SELECT * FROM xy LEFT JOIN ab ON 0}
} {2 3 {} {} {} 1 {} {}}
do_test join5-2.2 {
  execsql {SELECT * FROM xy LEFT JOIN ab ON 1}
} {2 3 1 2 2 3 3 {} {} 1 1 2 {} 1 3 {}}
do_test join5-2.3 {
  execsql {SELECT * FROM xy LEFT JOIN ab ON NULL}
} {2 3 {} {} {} 1 {} {}}
do_test join5-2.4 {
  execsql {SELECT * FROM xy LEFT JOIN ab ON 0 WHERE 0}
} {}
do_test join5-2.5 {
  execsql {SELECT * FROM xy LEFT JOIN ab ON 1 WHERE 0}
} {}
do_test join5-2.6 {
  execsql {SELECT * FROM xy LEFT JOIN ab ON NULL WHERE 0}
} {}
do_test join5-2.7 {
  execsql {SELECT * FROM xy LEFT JOIN ab ON 0 WHERE 1}
} {2 3 {} {} {} 1 {} {}}
do_test join5-2.8 {
  execsql {SELECT * FROM xy LEFT JOIN ab ON 1 WHERE 1}
} {2 3 1 2 2 3 3 {} {} 1 1 2 {} 1 3 {}}
do_test join5-2.9 {
  execsql {SELECT * FROM xy LEFT JOIN ab ON NULL WHERE 1}
} {2 3 {} {} {} 1 {} {}}
do_test join5-2.10 {
  execsql {SELECT * FROM xy LEFT JOIN ab ON 0 WHERE NULL}
} {}
do_test join5-2.11 {
  execsql {SELECT * FROM xy LEFT JOIN ab ON 1 WHERE NULL}
} {}
do_test join5-2.12 {
  execsql {SELECT * FROM xy LEFT JOIN ab ON NULL WHERE NULL}
} {}

# Ticket https://sqlite.org/src/tktview/6f2222d550f5b0ee7ed37601
# Incorrect output on a LEFT JOIN.
#
do_execsql_test join5-3.1 {
  DROP TABLE IF EXISTS t1;
  DROP TABLE IF EXISTS t2;
  DROP TABLE IF EXISTS t3;
  CREATE TABLE x1(a);
  INSERT INTO x1 VALUES(1);
  CREATE TABLE x2(b NOT NULL);
  CREATE TABLE x3(c, d);
  INSERT INTO x3 VALUES('a', NULL);
  INSERT INTO x3 VALUES('b', NULL);
  INSERT INTO x3 VALUES('c', NULL);
  SELECT * FROM x1 LEFT JOIN x2 LEFT JOIN x3 ON x3.d = x2.b;
} {1 {} {} {}}
do_execsql_test join5-3.2 {
  DROP TABLE IF EXISTS t1;
  DROP TABLE IF EXISTS t2;
  DROP TABLE IF EXISTS t3;
  DROP TABLE IF EXISTS t4;
  DROP TABLE IF EXISTS t5;
  CREATE TABLE t1(x text NOT NULL, y text);
  CREATE TABLE t2(u text NOT NULL, x text NOT NULL);
  CREATE TABLE t3(w text NOT NULL, v text);
  CREATE TABLE t4(w text NOT NULL, z text NOT NULL);
  CREATE TABLE t5(z text NOT NULL, m text);
  INSERT INTO t1 VALUES('f6d7661f-4efe-4c90-87b5-858e61cd178b',NULL);
  INSERT INTO t1 VALUES('f6ea82c3-2cad-45ce-ae8f-3ddca4fb2f48',NULL);
  INSERT INTO t1 VALUES('f6f47499-ecb4-474b-9a02-35be73c235e5',NULL);
  INSERT INTO t1 VALUES('56f47499-ecb4-474b-9a02-35be73c235e5',NULL);
  INSERT INTO t3 VALUES('007f2033-cb20-494c-b135-a1e4eb66130c',
                        'f6d7661f-4efe-4c90-87b5-858e61cd178b');
  SELECT *
    FROM t3
         INNER JOIN t1 ON t1.x= t3.v AND t1.y IS NULL
         LEFT JOIN t4  ON t4.w = t3.w
         LEFT JOIN t5  ON t5.z = t4.z
         LEFT JOIN t2  ON t2.u = t5.m
         LEFT JOIN t1 xyz ON xyz.y = t2.x;
} {007f2033-cb20-494c-b135-a1e4eb66130c f6d7661f-4efe-4c90-87b5-858e61cd178b f6d7661f-4efe-4c90-87b5-858e61cd178b {} {} {} {} {} {} {} {} {}}
do_execsql_test join5-3.3 {
  DROP TABLE IF EXISTS x1;
  DROP TABLE IF EXISTS x2;
  DROP TABLE IF EXISTS x3;
  CREATE TABLE x1(a);
  INSERT INTO x1 VALUES(1);
  CREATE TABLE x2(b NOT NULL);
  CREATE TABLE x3(c, d);
  INSERT INTO x3 VALUES('a', NULL);
  INSERT INTO x3 VALUES('b', NULL);
  INSERT INTO x3 VALUES('c', NULL);
  SELECT * FROM x1 LEFT JOIN x2 JOIN x3 WHERE x3.d = x2.b;
} {}

# Ticket https://sqlite.org/src/tktview/c2a19d81652f40568c770c43 on
# 2015-08-20.  LEFT JOIN and the push-down optimization.
#
do_execsql_test join5-4.1 {
  SELECT *
  FROM (
      SELECT 'apple' fruit
      UNION ALL SELECT 'banana'
  ) a
  JOIN (
      SELECT 'apple' fruit
      UNION ALL SELECT 'banana'
  ) b ON a.fruit=b.fruit
  LEFT JOIN (
      SELECT 1 isyellow
  ) c ON b.fruit='banana';
} {apple apple {} banana banana 1}
do_execsql_test join5-4.2 {
  SELECT *
    FROM (SELECT 'apple' fruit UNION ALL SELECT 'banana')
         LEFT JOIN (SELECT 1) ON fruit='banana';
} {apple {} banana 1}

#-------------------------------------------------------------------------
do_execsql_test 5.0 {
  CREATE TABLE y1(x, y, z);
  INSERT INTO y1 VALUES(0, 0, 1);
  CREATE TABLE y2(a);
}

do_execsql_test 5.1 {
  SELECT count(z) FROM y1 LEFT JOIN y2 ON x GROUP BY y;
} 1

do_execsql_test 5.2 {
  SELECT count(z) FROM ( SELECT * FROM y1 ) LEFT JOIN y2 ON x GROUP BY y;
} 1

do_execsql_test 5.3 {
  CREATE VIEW v1 AS SELECT x, y, z FROM y1;
  SELECT count(z) FROM v1 LEFT JOIN y2 ON x GROUP BY y;
} 1

do_execsql_test 5.4 {
  SELECT count(z) FROM ( SELECT * FROM y1 ) LEFT JOIN y2 ON x
} 1

do_execsql_test 5.5 {
  SELECT * FROM ( SELECT * FROM y1 ) LEFT JOIN y2 ON x
} {0 0 1 {}}

#-------------------------------------------------------------------------
#
reset_db
do_execsql_test 6.1 {
  CREATE TABLE t1(x); 
  INSERT INTO t1 VALUES(1);

  CREATE TABLE t2(y INTEGER PRIMARY KEY,a,b);
  INSERT INTO t2 VALUES(1,2,3);
  CREATE INDEX t2a ON t2(a); 
  CREATE INDEX t2b ON t2(b); 
}

do_execsql_test 6.2 {
  SELECT * FROM t1 LEFT JOIN t2 ON a=2 OR b=3 WHERE y IS NULL;
} {}

do_execsql_test 6.3.1 {
  CREATE TABLE t3(x);
  INSERT INTO t3 VALUES(1);
  CREATE TABLE t4(y, z);
  SELECT ifnull(z, '!!!') FROM t3 LEFT JOIN t4 ON (x=y);
} {!!!}

do_execsql_test 6.3.2 {
  CREATE INDEX t4i ON t4(y, ifnull(z, '!!!'));
  SELECT ifnull(z, '!!!') FROM t3 LEFT JOIN t4 ON (x=y);
} {!!!}

# 2019-02-08 https://sqlite.org/src/info/4e8e4857d32d401f
reset_db
do_execsql_test 6.100 {
  CREATE TABLE t1(aa, bb);
  CREATE INDEX t1x1 on t1(abs(aa), abs(bb));
  INSERT INTO t1 VALUES(-2,-3),(+2,-3),(-2,+3),(+2,+3);
  SELECT * FROM (t1) 
   WHERE ((abs(aa)=1 AND 1=2) OR abs(aa)=2)
     AND abs(bb)=3
  ORDER BY +1, +2;
} {-2 -3 -2 3 2 -3 2 3}

#-------------------------------------------------------------------------
#
reset_db
do_execsql_test 7.0 {
  CREATE TABLE t1(x);
  INSERT INTO t1 VALUES(1);
}

do_execsql_test 7.1 {
  CREATE TABLE t2(x, y, z);
  CREATE INDEX t2xy ON t2(x, y);
  WITH s(i) AS (
    SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i<50000
  )
  INSERT INTO t2 SELECT i/10, i, NULL FROM s;
  ANALYZE;
}

do_eqp_test 7.2 {
  SELECT * FROM t1 LEFT JOIN t2 ON (
    t2.x = t1.x AND (t2.y=? OR (t2.y=? AND t2.z IS NOT NULL))
  );
} {
  QUERY PLAN
  |--SCAN t1
  `--MULTI-INDEX OR
     |--INDEX 1
     |  `--SEARCH t2 USING INDEX t2xy (x=? AND y=?) LEFT-JOIN
     `--INDEX 2
        `--SEARCH t2 USING INDEX t2xy (x=? AND y=?) LEFT-JOIN
}

do_execsql_test 7.3 {
  CREATE TABLE t3(x);
  INSERT INTO t3(x) VALUES(1);
  CREATE INDEX t3x ON t3(x);

  CREATE TABLE t4(x, y, z);
  CREATE INDEX t4xy ON t4(x, y);
  CREATE INDEX t4xz ON t4(x, z);

  WITH s(i) AS ( SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i<50000)
  INSERT INTO t4 SELECT i/10, i, i FROM s;

  ANALYZE;
  UPDATE sqlite_stat1 SET stat='1000000 10 1' WHERE idx='t3x';
  ANALYZE sqlite_schema;
}

# If both sides of the OR reference the right-hand side of the LEFT JOIN
# then simplify the LEFT JOIN.
#
do_eqp_test 7.4 {
  SELECT * FROM t3 LEFT JOIN t4 ON (t4.x = t3.x) WHERE (t4.y = ? OR t4.z = ?);
} {
  QUERY PLAN
  |--SCAN t4
  `--SEARCH t3 USING COVERING INDEX t3x (x=?)
} 
# If only one side of the OR references the right-hand side of the LEFT JOIN
# then do not do the simplification
#
do_eqp_test 7.4b {
  SELECT * FROM t3 LEFT JOIN t4 ON (t4.x = t3.x) WHERE (t4.y = ? OR t3.x = ?);
} {
  QUERY PLAN
  |--SCAN t3
  `--SEARCH t4 USING INDEX t4xz (x=?) LEFT-JOIN
} 
do_eqp_test 7.4c {
  SELECT * FROM t3 LEFT JOIN t4 ON (t4.x = t3.x) WHERE (t3.x = ? OR t4.z = ?);
} {
  QUERY PLAN
  |--SCAN t3
  `--SEARCH t4 USING INDEX t4xz (x=?) LEFT-JOIN
} 
do_eqp_test 7.4d {
  SELECT * FROM t3 CROSS JOIN t4 ON (t4.x = t3.x) WHERE (+t4.y = ? OR t4.z = ?);
} {
  QUERY PLAN
  |--SCAN t3
  |--BLOOM FILTER ON t4 (x=?)
  `--SEARCH t4 USING INDEX t4xz (x=?)
} 

reset_db
do_execsql_test 8.0 {
  CREATE TABLE t0 (c0, c1, PRIMARY KEY (c0, c1));
  CREATE TABLE t1 (c0);

  INSERT INTO t1 VALUES (2);

  INSERT INTO t0 VALUES(0, 10);
  INSERT INTO t0 VALUES(1, 10);
  INSERT INTO t0 VALUES(2, 10);
  INSERT INTO t0 VALUES(3, 10);
}

do_execsql_test 8.1 {
  SELECT * FROM t0, t1 
  WHERE (t0.c1 >= 1 OR t0.c1 < 1) AND t0.c0 IN (1, t1.c0) ORDER BY 1;
} {
  1 10 2
  2 10 2
}


# 2022-01-31 dbsqlfuzz 787d9bd73164c6f0c85469e2e48b2aff19af6938
#
reset_db
do_execsql_test 9.1 {
  CREATE TABLE t1(a ,b FLOAT);
  INSERT INTO t1 VALUES(1,1);
  CREATE INDEX t1x1 ON t1(a,b,a,a,a,a,a,a,a,a,a,b);
  ANALYZE sqlite_schema;
  INSERT INTO sqlite_stat1 VALUES('t1','t1x1','648 324 81 81 81 81 81 81 81081 81 81 81');
  ANALYZE sqlite_schema;
}
do_catchsql_test 9.2 {
  SELECT a FROM 
      (SELECT a FROM t1 NATURAL LEFT JOIN t1) NATURAL LEFT JOIN t1 
  WHERE (rowid,1)<=(5,0);
} {0 1}

# 2022-03-02 https://sqlite.org/forum/info/50a1bbe08ce4c29c
# Bloom-filter pulldown is incompatible with skip-scan.
#
reset_db
do_execsql_test 10.1 {
  CREATE TABLE t1(x INT);
  WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<20)
    INSERT INTO t1(x) SELECT 0 FROM c;
  CREATE INDEX t1x1 ON t1(x BETWEEN 0 AND 10, x);
  ANALYZE;
  DELETE FROM t1;
  INSERT INTO t1 VALUES(0),(0);
  CREATE VIEW v1 AS SELECT * FROM t1 NATURAL JOIN t1 WHERE (x BETWEEN 0 AND 10) OR true;
  CREATE VIEW v2 AS SELECT * FROM v1 NATURAL JOIN v1;
  CREATE VIEW v3 AS SELECT * FROM v2, v1 USING (x) GROUP BY x;
  SELECT x FROM v3; 
} {0}

# 2022-03-24 https://sqlite.org/forum/forumpost/031e262a89b6a9d2
# Bloom-filter on a LEFT JOIN with NULL-based WHERE constraints.
#
reset_db
do_execsql_test 11.1 {
  CREATE TABLE t1(a INTEGER PRIMARY KEY, b INT);
  CREATE TABLE t2(c INTEGER PRIMARY KEY, d INT);
  WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<8)
  INSERT INTO t1(a,b) SELECT x, 10*x FROM c;
  INSERT INTO t2(c,d) SELECT b*2, 100*a FROM t1;
  ANALYZE;
  DELETE FROM sqlite_stat1;
  INSERT INTO sqlite_stat1(tbl,idx,stat) VALUES
    ('t1',NULL,150105),('t2',NULL,98747);
  ANALYZE sqlite_schema;
} {}
do_execsql_test 11.2 {
  SELECT count(*) FROM t1 LEFT JOIN t2 ON c=b WHERE d IS NULL;
} {4}
do_execsql_test 11.3 {
  SELECT count(*) FROM t1 LEFT JOIN t2 ON c=b WHERE d=100;
} {1}
do_execsql_test 11.4 {
  SELECT count(*) FROM t1 LEFT JOIN t2 ON c=b WHERE d>=300;
} {2}

# 2022-05-03 https://sqlite.org/forum/forumpost/2482b32700384a0f
# Bloom-filter pull-down does not handle NOT NULL constraints correctly.
#
reset_db
do_execsql_test 12.1 {
  CREATE TABLE t1(a INT, b INT, c INT);
  WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<100)
    INSERT INTO t1(a,b,c) SELECT x, x*1000, x*1000000 FROM c;
  CREATE TABLE t2(b INT, x INT);
  INSERT INTO t2(b,x) SELECT b, a FROM t1 WHERE a%3==0;
  CREATE INDEX t2b ON t2(b);
  CREATE TABLE t3(c INT, y INT);
  INSERT INTO t3(c,y) SELECT c, a FROM t1 WHERE a%4==0;
  CREATE INDEX t3c ON t3(c);
  INSERT INTO t1(a,b,c) VALUES(200, 200000, NULL);
  ANALYZE;
} {}
do_execsql_test 12.2 {
  SELECT * FROM t1 NATURAL JOIN t2 NATURAL JOIN t3 WHERE x>0 AND y>0
  ORDER BY +a;
} {
  12  12000  12000000  12  12
  24  24000  24000000  24  24
  36  36000  36000000  36  36
  48  48000  48000000  48  48
  60  60000  60000000  60  60
  72  72000  72000000  72  72
  84  84000  84000000  84  84
  96  96000  96000000  96  96
}




finish_test
