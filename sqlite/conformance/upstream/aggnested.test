# 2012-08-23
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
# This file implements tests for processing aggregate queries with 
# subqueries in which the subqueries hold the aggregate functions
# or in which the subqueries are themselves aggregate queries
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix aggnested

do_test aggnested-1.1 {
  db eval {
    CREATE TABLE t1(a1 INTEGER);
    INSERT INTO t1 VALUES(1), (2), (3);
    CREATE TABLE t2(b1 INTEGER);
    INSERT INTO t2 VALUES(4), (5);
    SELECT (SELECT string_agg(a1,'x') FROM t2) FROM t1;
  }
} {1x2x3}
do_test aggnested-1.2 {
  db eval {
    SELECT
     (SELECT string_agg(a1,'x') || '-' || string_agg(b1,'y') FROM t2)
    FROM t1;
  }
} {1x2x3-4y5}
do_test aggnested-1.3 {
  db eval {
    SELECT (SELECT string_agg(b1,a1) FROM t2) FROM t1;
  }
} {415 425 435}
do_test aggnested-1.4 {
  db eval {
    SELECT (SELECT group_concat(a1,b1) FROM t2) FROM t1;
  }
} {151 252 353}


# This test case is a copy of the one in
# http://www.mail-archive.com/sqlite-users@sqlite.org/msg70787.html
#
do_test aggnested-2.0 {
  sqlite3 db2 :memory:
  db2 eval {
    CREATE TABLE t1 (A1 INTEGER NOT NULL,A2 INTEGER NOT NULL,A3 INTEGER NOT 
    NULL,A4 INTEGER NOT NULL,PRIMARY KEY(A1));
    REPLACE INTO t1 VALUES(1,11,111,1111);
    REPLACE INTO t1 VALUES(2,22,222,2222);
    REPLACE INTO t1 VALUES(3,33,333,3333);
    CREATE TABLE t2 (B1 INTEGER NOT NULL,B2 INTEGER NOT NULL,B3 INTEGER NOT 
    NULL,B4 INTEGER NOT NULL,PRIMARY KEY(B1));
    REPLACE INTO t2 VALUES(1,88,888,8888);
    REPLACE INTO t2 VALUES(2,99,999,9999);
    SELECT (SELECT GROUP_CONCAT(CASE WHEN a1=1 THEN'A' ELSE 'B' END) FROM t2),
            t1.* 
    FROM t1;
  }
} {A,B,B 1 11 111 1111}
db2 close

##################### Test cases for ticket [bfbf38e5e9956ac69f] ############
#
# This first test case is the original problem report:
do_test aggnested-3.0 {
  db eval {
    CREATE TABLE AAA (
      aaa_id       INTEGER PRIMARY KEY AUTOINCREMENT
    );
    CREATE TABLE RRR (
      rrr_id      INTEGER     PRIMARY KEY AUTOINCREMENT,
      rrr_date    INTEGER     NOT NULL,
      rrr_aaa     INTEGER
    );
    CREATE TABLE TTT (
      ttt_id      INTEGER PRIMARY KEY AUTOINCREMENT,
      target_aaa  INTEGER NOT NULL,
      source_aaa  INTEGER NOT NULL
    );
    insert into AAA (aaa_id) values (2);
    insert into TTT (ttt_id, target_aaa, source_aaa)
    values (4469, 2, 2);
    insert into TTT (ttt_id, target_aaa, source_aaa)
    values (4476, 2, 1);
    insert into RRR (rrr_id, rrr_date, rrr_aaa)
    values (0, 0, NULL);
    insert into RRR (rrr_id, rrr_date, rrr_aaa)
    values (2, 4312, 2);
    SELECT i.aaa_id,
      (SELECT sum(CASE WHEN (t.source_aaa == i.aaa_id) THEN 1 ELSE 0 END)
         FROM TTT t
      ) AS segfault
    FROM
     (SELECT curr.rrr_aaa as aaa_id
        FROM RRR curr
          -- you also can comment out the next line
          -- it causes segfault to happen after one row is outputted
          INNER JOIN AAA a ON (curr.rrr_aaa = aaa_id)
          LEFT JOIN RRR r ON (r.rrr_id <> 0 AND r.rrr_date < curr.rrr_date)
       GROUP BY curr.rrr_id
      HAVING r.rrr_date IS NULL
    ) i;
  }
} {2 1}

# Further variants of the test case, as found in the ticket
#
do_test aggnested-3.1 {
  db eval {
    DROP TABLE IF EXISTS t1;
    DROP TABLE IF EXISTS t2;
    CREATE TABLE t1 (
      id1 INTEGER PRIMARY KEY AUTOINCREMENT,
      value1 INTEGER
    );
    INSERT INTO t1 VALUES(4469,2),(4476,1);
    CREATE TABLE t2 (
      id2 INTEGER PRIMARY KEY AUTOINCREMENT,
      value2 INTEGER
    );
    INSERT INTO t2 VALUES(0,1),(2,2);
    SELECT
     (SELECT sum(value2==xyz) FROM t2)
    FROM
     (SELECT curr.value1 as xyz
        FROM t1 AS curr LEFT JOIN t1 AS other
       GROUP BY curr.id1);
  }
} {1 1}
do_test aggnested-3.1-rj {
  db eval {
    SELECT
     (SELECT sum(value2==xyz) FROM t2)
    FROM
     (SELECT curr.value1 as xyz
        FROM t1 AS other RIGHT JOIN t1 AS curr
       GROUP BY curr.id1);
  }
} {1 1}

do_test aggnested-3.2 {
  db eval {
    DROP TABLE IF EXISTS t1;
    DROP TABLE IF EXISTS t2;
    CREATE TABLE t1 (
      id1 INTEGER,
      value1 INTEGER,
      x1 INTEGER
    );
    INSERT INTO t1 VALUES(4469,2,98),(4469,1,99),(4469,3,97);
    CREATE TABLE t2 (
      value2 INTEGER
    );
    INSERT INTO t2 VALUES(1);
    SELECT
     (SELECT sum(value2==xyz) FROM t2)
    FROM
     (SELECT value1 as xyz, max(x1) AS pqr
        FROM t1
       GROUP BY id1);
    SELECT
     (SELECT sum(value2<>xyz) FROM t2)
    FROM
     (SELECT value1 as xyz, max(x1) AS pqr
        FROM t1
       GROUP BY id1);
  }
} {1 0}
do_test aggnested-3.3 {
  db eval {
    DROP TABLE IF EXISTS t1;
    DROP TABLE IF EXISTS t2;
    CREATE TABLE t1(id1, value1);
    INSERT INTO t1 VALUES(4469,2),(4469,1);
    CREATE TABLE t2 (value2);
    INSERT INTO t2 VALUES(1);
    SELECT (SELECT sum(value2=value1) FROM t2), max(value1)
      FROM t1
     GROUP BY id1;
  }
} {0 2}

# A batch of queries all doing approximately the same operation involving
# two nested aggregate queries.
#
do_test aggnested-3.11 {
  db eval {
    DROP TABLE IF EXISTS t1;
    DROP TABLE IF EXISTS t2;
    CREATE TABLE t1(id1, value1);
    INSERT INTO t1 VALUES(4469,12),(4469,11),(4470,34);
    CREATE INDEX t1id1 ON t1(id1);
    CREATE TABLE t2 (value2);
    INSERT INTO t2 VALUES(12),(34),(34);
    INSERT INTO t2 SELECT value2 FROM t2;

    SELECT max(value1), (SELECT count(*) FROM t2 WHERE value2=max(value1))
      FROM t1
     GROUP BY id1;
  }
} {12 2 34 4}
do_test aggnested-3.12 {
  db eval {
    SELECT max(value1), (SELECT count(*) FROM t2 WHERE value2=value1)
      FROM t1
     GROUP BY id1;
  }
} {12 2 34 4}
do_test aggnested-3.13 {
  db eval {
    SELECT value1, (SELECT sum(value2=value1) FROM t2)
      FROM t1;
  }
} {12 2 11 0 34 4}
do_test aggnested-3.14 {
  db eval {
    SELECT value1, (SELECT sum(value2=value1) FROM t2)
      FROM t1
     WHERE value1 IN (SELECT max(value1) FROM t1 GROUP BY id1);
  }
} {12 2 34 4}
do_test aggnested-3.15 {
  # FIXME:  If case 3.16 works, then this case really ought to work too...
  catchsql {
    SELECT max(value1), (SELECT sum(value2=max(value1)) FROM t2)
      FROM t1
     GROUP BY id1;
  }
} {1 {misuse of aggregate function max()}}
do_test aggnested-3.16 {
  db eval {
    SELECT max(value1), (SELECT sum(value2=value1) FROM t2)
      FROM t1
     GROUP BY id1;
  }
} {12 2 34 4}

# 2019-08-31
# Problem found by dbsqlfuzz
#
do_execsql_test aggnested-4.1 {
  DROP TABLE IF EXISTS aa;
  DROP TABLE IF EXISTS bb;
  CREATE TABLE aa(x INT);  INSERT INTO aa(x) VALUES(123);
  CREATE TABLE bb(y INT);  INSERT INTO bb(y) VALUES(456);
  SELECT (SELECT sum(x+(SELECT y)) FROM bb) FROM aa;
} {579}
do_execsql_test aggnested-4.2 {
  SELECT (SELECT sum(x+y) FROM bb) FROM aa;
} {579}
do_execsql_test aggnested-4.3 {
  DROP TABLE IF EXISTS tx;
  DROP TABLE IF EXISTS ty;
  CREATE TABLE tx(x INT);
  INSERT INTO tx VALUES(1),(2),(3),(4),(5);
  CREATE TABLE ty(y INT);
  INSERT INTO ty VALUES(91),(92),(93);
  SELECT min((SELECT count(y) FROM ty)) FROM tx;
} {3}
do_execsql_test aggnested-4.4 {
  SELECT max((SELECT a FROM (SELECT count(*) AS a FROM ty) AS s)) FROM tx;
} {3}

#--------------------------------------------------------------------------
#
reset_db
do_execsql_test 5.0 {
  CREATE TABLE x1(a, b);
  INSERT INTO x1 VALUES(1, 2);
  CREATE TABLE x2(x);
  INSERT INTO x2 VALUES(NULL), (NULL), (NULL);
}

# At one point, aggregate "total()" in the query below was being processed
# as part of the outer SELECT, not as part of the sub-select with no FROM
# clause.
do_execsql_test 5.1 {
  SELECT ( SELECT total( (SELECT b FROM x1) ) ) FROM x2;
} {2.0 2.0 2.0}

do_execsql_test 5.2 {
  SELECT ( SELECT total( (SELECT 2 FROM x1) ) ) FROM x2;
} {2.0 2.0 2.0}

do_execsql_test 5.3 {
  CREATE TABLE t1(a);
  CREATE TABLE t2(b);
}

do_execsql_test 5.4 {
  SELECT(
    SELECT max(b) LIMIT (
      SELECT total( (SELECT a FROM t1) )
    )
  )
  FROM t2;
} {{}}

do_execsql_test 5.5 {
  CREATE TABLE a(b);
  WITH c AS(SELECT a)
    SELECT(SELECT(SELECT string_agg(b, b)
          LIMIT(SELECT 0.100000 *
            AVG(DISTINCT(SELECT 0 FROM a ORDER BY b, b, b))))
        FROM a GROUP BY b,
        b, b) FROM a EXCEPT SELECT b FROM a ORDER BY b,
    b, b;
}

#-------------------------------------------------------------------------
# dbsqlfuzz a779227f721a834df95f4f42d0c31550a1f8b8a2 
#
reset_db
do_execsql_test 6.0 {
  CREATE TABLE t1(a);
  CREATE TABLE t2(b);

  INSERT INTO t1 VALUES('x');
  INSERT INTO t2 VALUES(1);
}

do_execsql_test 6.1.1 {
  SELECT ( 
    SELECT t2.b FROM (SELECT t2.b AS c FROM t1) GROUP BY 1 HAVING t2.b
  )
  FROM t2 GROUP BY 'constant_string';
} {1}
do_execsql_test 6.1.2 {
  SELECT ( 
    SELECT c FROM (SELECT t2.b AS c FROM t1) GROUP BY c HAVING t2.b
  )
  FROM t2 GROUP BY 'constant_string';
} {1}

do_execsql_test 6.2.0 {
  UPDATE t2 SET b=0
}
do_execsql_test 6.2.1 {
  SELECT ( 
    SELECT t2.b FROM (SELECT t2.b AS c FROM t1) GROUP BY 1 HAVING t2.b
  )
  FROM t2 GROUP BY 'constant_string';
} {{}}
do_execsql_test 6.2.2 {
  SELECT ( 
    SELECT c FROM (SELECT t2.b AS c FROM t1) GROUP BY c HAVING t2.b
  )
  FROM t2 GROUP BY 'constant_string';
} {{}}

#-------------------------------------------------------------------------
reset_db

do_execsql_test 7.0 {
  CREATE TABLE invoice (
      id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
      amount DOUBLE PRECISION DEFAULT NULL,
      name VARCHAR(100) DEFAULT NULL
  );

  INSERT INTO invoice (amount, name) VALUES 
      (4.0, 'Michael'), (15.0, 'Bara'), (4.0, 'Michael'), (6.0, 'John');
}

do_execsql_test 7.1 {
  SELECT sum(amount), name
    from invoice
  group by name
  having (select v > 6 from (select sum(amount) v) t)
} {
  15.0 Bara
  8.0 Michael
}

do_execsql_test 7.2 {
  SELECT (select 1 from (select sum(amount))) FROM invoice
} {1}

do_execsql_test 8.0 {
  CREATE TABLE t1(x INT);
  INSERT INTO t1 VALUES(100);
  INSERT INTO t1 VALUES(20);
  INSERT INTO t1 VALUES(3);
  SELECT (SELECT y FROM (SELECT sum(x) AS y) AS t2 ) FROM t1;
} {123}

do_execsql_test 8.1 {
  SELECT (
    SELECT y FROM (
      SELECT z AS y FROM (SELECT sum(x) AS z) AS t2 
    ) 
  ) FROM t1;
} {123}

do_execsql_test 8.2 {
  SELECT (
    SELECT a FROM (
      SELECT y AS a FROM (
        SELECT z AS y FROM (SELECT sum(x) AS z) AS t2 
      ) 
    )
  ) FROM t1;
} {123}

#-------------------------------------------------------------------------
# dbsqlfuzz 04408efc51ae46897c4c122b407412045ed221b4
#
reset_db

do_execsql_test 9.1 {
  WITH out(i, j, k) AS ( 
      VALUES(1234, 5678, 9012) 
  )
  SELECT (
    SELECT (
      SELECT min(abc) = ( SELECT ( SELECT 1234 fROM (SELECT abc) ) ) 
      FROM (
        SELECT sum( out.i ) + ( SELECT sum( out.i ) ) AS abc FROM (SELECT out.j)
      )
    ) 
  ) FROM out;
} {0}

do_execsql_test 9.2 {
  CREATE TABLE t1(a);
  CREATE TABLE t2(b);
  INSERT INTO t1 VALUES(1), (2), (3);
  INSERT INTO t2 VALUES(4), (5), (6);

  SELECT ( 
    SELECT min(y) + (SELECT x) FROM (
      SELECT sum(a) AS x, b AS y FROM t2
    )
  )
  FROM t1;
} {10}

do_execsql_test 9.3 {
  SELECT ( 
    SELECT min(y) + (SELECT (SELECT x)) FROM (
      SELECT sum(a) AS x, b AS y FROM t2
    )
  )
  FROM t1;
} {10}

do_execsql_test 9.4 {
  SELECT (
    SELECT (SELECT x) FROM (
      SELECT sum(a) AS x, b AS y FROM t2
      ) GROUP BY y
    )
  FROM t1;
} {6}

do_execsql_test 9.5 {
  SELECT (
    SELECT (SELECT (SELECT x)) FROM (
      SELECT sum(a) AS x, b AS y FROM t2
      ) GROUP BY y
    )
  FROM t1;
} {6}

# 2023-12-16
# New test case for check-in [4470f657d2069972] from 2023-11-02
# https://bugs.chromium.org/p/chromium/issues/detail?id=1511689
#
do_execsql_test 10.1 {
  DROP TABLE IF EXISTS t0;
  DROP TABLE IF EXISTS t1;
  CREATE TABLE t0(c1, c2);  INSERT INTO t0 VALUES(1,2);
  CREATE TABLE t1(c3, c4);  INSERT INTO t1 VALUES(3,4);
  SELECT * FROM t0 WHERE EXISTS (SELECT 1 FROM t1 GROUP BY c3 HAVING ( SELECT count(*) FROM (SELECT 1 UNION ALL SELECT sum(DISTINCT c1) ) ) ) BETWEEN 1 AND 1;
} {1 2}

finish_test
