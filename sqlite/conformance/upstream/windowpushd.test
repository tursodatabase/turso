# 2021 February 23
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
# focus of this file is testing the push-down optimization when
# WHERE constraints are pushed down into a sub-query that uses
# window functions.
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix windowpushd

do_execsql_test 1.0 {
  CREATE TABLE t1(id INTEGER PRIMARY KEY, grp_id);
  CREATE INDEX i1 ON t1(grp_id);
  CREATE VIEW lll AS SELECT
    row_number() OVER (PARTITION BY grp_id), 
    grp_id, id 
  FROM t1
}

do_execsql_test 1.1 {
  INSERT INTO t1 VALUES
    (1, 2), (2, 3), (3, 3), (4, 1), (5, 1),
    (6, 1), (7, 1), (8, 1), (9, 3), (10, 3), 
    (11, 2), (12, 3), (13, 3), (14, 2), (15, 1),
    (16, 2), (17, 1), (18, 2), (19, 3), (20, 2)
}

do_execsql_test 1.2 {
  SELECT * FROM lll
} {
  1 1 4 2 1 5 3 1 6 4 1 7 5 1 8 6 1 15 7 1 17 
  1 2 1 2 2 11 3 2 14 4 2 16 5 2 18 6 2 20 
  1 3 2 2 3 3 3 3 9 4 3 10 5 3 12 6 3 13 7 3 19
}

do_execsql_test 1.3 {
  SELECT * FROM lll WHERE grp_id=2
} {
  1 2 1 2 2 11 3 2 14 4 2 16 5 2 18 6 2 20 
}

do_eqp_test 1.4 {
  SELECT * FROM lll WHERE grp_id=2
} {SEARCH t1 USING COVERING INDEX i1 (grp_id=?)}

#-------------------------------------------------------------------------
reset_db
do_execsql_test 2.0 {
  CREATE TABLE t1(a, b, c, d);
  INSERT INTO t1 VALUES('A', 'C', 1,  0.1);
  INSERT INTO t1 VALUES('A', 'D', 2,  0.2);
  INSERT INTO t1 VALUES('A', 'E', 3,  0.3);
  INSERT INTO t1 VALUES('A', 'C', 4,  0.4);
  INSERT INTO t1 VALUES('B', 'D', 5,  0.5);
  INSERT INTO t1 VALUES('B', 'E', 6,  0.6);
  INSERT INTO t1 VALUES('B', 'C', 7,  0.7);
  INSERT INTO t1 VALUES('B', 'D', 8,  0.8);
  INSERT INTO t1 VALUES('C', 'E', 9,  0.9);
  INSERT INTO t1 VALUES('C', 'C', 10, 1.0);
  INSERT INTO t1 VALUES('C', 'D', 11, 1.1);
  INSERT INTO t1 VALUES('C', 'E', 12, 1.2);

  CREATE INDEX i1 ON t1(a);
  CREATE INDEX i2 ON t1(b);

  CREATE VIEW v1 AS SELECT a, c, max(c) OVER (PARTITION BY a) FROM t1;

  CREATE VIEW v2 AS SELECT a, c, 
      max(c) OVER (PARTITION BY a),
      row_number() OVER ()
  FROM t1;

  CREATE VIEW v3 AS SELECT b, d, 
      max(d) OVER (PARTITION BY b),
      row_number() OVER (PARTITION BY b)
  FROM t1;

  CREATE TABLE t2(x, y, z);
  INSERT INTO t2 VALUES('W', 3, 1);
  INSERT INTO t2 VALUES('W', 2, 2);
  INSERT INTO t2 VALUES('X', 1, 4);
  INSERT INTO t2 VALUES('X', 5, 7);
  INSERT INTO t2 VALUES('Y', 1, 9);
  INSERT INTO t2 VALUES('Y', 4, 2);
  INSERT INTO t2 VALUES('Z', 3, 3);
  INSERT INTO t2 VALUES('Z', 3, 4);
}

foreach tn {0 1} {
  if {$tn} {
    optimization_control db all on
  } else {
    optimization_control db push-down off
  }

  do_execsql_test 2.$tn.1.1 {
    SELECT * FROM v1;
  } {
    A 1 4   A 2 4   A 3 4   A 4 4
    B 5 8   B 6 8   B 7 8   B 8 8
    C 9 12  C 10 12 C 11 12 C 12 12
  }

  do_execsql_test 2.$tn.1.2 {
    SELECT * FROM v1 WHERE a IN ('A', 'B');
  } {
    A 1 4   A 2 4   A 3 4   A 4 4
    B 5 8   B 6 8   B 7 8   B 8 8
  }

  do_execsql_test 2.$tn.1.3 {
    SELECT * FROM v1 WHERE a IS 'C'
  } {
    C 9 12  C 10 12 C 11 12 C 12 12
  }

  if {$tn==1} {
    do_eqp_test 2.$tn.1.4 {
      SELECT * FROM v1 WHERE a IN ('A', 'B');
    } {USING INDEX i1 (a=?)}

    do_eqp_test 2.$tn.1.5 {
      SELECT * FROM v1 WHERE a = 'c' COLLATE nocase
    } {USING INDEX i1}
  }

  do_execsql_test 2.$tn.2.1 {
    SELECT * FROM v2;
  } {
    A 1 4 1    A 2 4 2     A 3 4 3      A 4 4 4
    B 5 8 5    B 6 8 6     B 7 8 7      B 8 8 8
    C 9 12 9   C 10 12 10  C 11 12 11   C 12 12 12
  }

  do_execsql_test 2.$tn.2.2 {
    SELECT * FROM v2 WHERE a = 'C';
  } {
    C 9 12 9   C 10 12 10  C 11 12 11   C 12 12 12
  }

  do_execsql_test 2.$tn.3.1 { SELECT * FROM v3; } { 
    C 0.1 1.0 1 C 0.4 1.0 2 C 0.7 1.0 3 C 1.0 1.0 4 
    D 0.2 1.1 1 D 0.5 1.1 2 D 0.8 1.1 3 D 1.1 1.1 4 
    E 0.3 1.2 1 E 0.6 1.2 2 E 0.9 1.2 3 E 1.2 1.2 4
  }

  do_execsql_test 2.$tn.3.2 { SELECT * FROM v3 WHERE b<'E' } { 
    C 0.1 1.0 1 C 0.4 1.0 2 C 0.7 1.0 3 C 1.0 1.0 4 
    D 0.2 1.1 1 D 0.5 1.1 2 D 0.8 1.1 3 D 1.1 1.1 4 
  }

  if {$tn==1} {
    do_eqp_test 2.$tn.3.3 {
      SELECT * FROM v3 WHERE b='E'
    } {SEARCH t1 USING INDEX i2 (b=?)}
    do_eqp_test 2.$tn.3.4 {
      SELECT * FROM v3 WHERE b>'C'
    } {SEARCH t1 USING INDEX i2 (b>?)}
  }

  do_execsql_test 2.$tn.3.5 { SELECT * FROM v3 WHERE d<0.55; } { 
    C 0.1 1.0 1 C 0.4 1.0 2
    D 0.2 1.1 1 D 0.5 1.1 2
    E 0.3 1.2 1
  }
  if {$tn==1} {
    do_eqp_test 2.$tn.3.6 {
      SELECT * FROM v3 WHERE d<0.55
    } {SCAN t1 USING INDEX i2}
  }

  do_execsql_test 2.$tn.4.1 {
    SELECT * FROM (
      SELECT x, sum(y) AS s, max(z) AS m 
      FROM t2 GROUP BY x
    )
  } {
    W 5 2
    X 6 7
    Y 5 9
    Z 6 4
  }

  do_execsql_test 2.$tn.4.1 {
    SELECT * FROM (
      SELECT x, sum(y) AS s, max(z) AS m,
        max( max(z) ) OVER (PARTITION BY sum(y) 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )
      FROM t2 GROUP BY x
    )
  } {
    W 5 2   9
    Y 5 9   9
    X 6 7   7
    Z 6 4   7
  }

  do_execsql_test 2.$tn.4.2 {
    SELECT * FROM (
      SELECT x, sum(y) AS s, max(z) AS m,
        max( max(z) ) OVER (PARTITION BY sum(y) 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )
      FROM t2 GROUP BY x
    ) WHERE s=6
  } {
    X 6 7   7
    Z 6 4   7
  }

  do_execsql_test 2.$tn.4.3 {
    SELECT * FROM (
      SELECT x, sum(y) AS s, max(z) AS m,
        max( max(z) ) OVER (PARTITION BY sum(y) 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )
      FROM t2 GROUP BY x
    ) WHERE s<6
  } {
    W 5 2   9
    Y 5 9   9
  }

}




finish_test
