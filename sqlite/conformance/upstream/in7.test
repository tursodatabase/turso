# 2024-05-01
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
set testprefix in7

do_execsql_test 1.0 {
  CREATE TABLE t1(a, b, c PRIMARY KEY);
  CREATE TABLE t2(x, y, z);
}

foreach {tn nNext idx sql} {
  1 1 {
    CREATE INDEX i1 ON t1(a, b);
  } {
    SELECT * FROM t1 WHERE (a, b) IN (SELECT x, y FROM t2)
  } 

  2 0 {
    CREATE UNIQUE INDEX i1 ON t1(a, b);
  } {
    SELECT * FROM t1 WHERE (a, b) IN (SELECT x, y FROM t2)
  } 

  3 0 {
    CREATE UNIQUE INDEX i1 ON t1(a, b);
  } {
    SELECT * FROM t1 WHERE a = ? AND b = ?
  } 

  3 1 {
    CREATE UNIQUE INDEX i1 ON t1(a, b);
  } {
    SELECT * FROM t1 WHERE a = ? AND b IS ?
  } 

  4 0 {
    CREATE UNIQUE INDEX i1 ON t1(a, b);
  } {
    SELECT * FROM t1 WHERE a = ? AND b IN (?, ?, ?);
  } 

  5 1 {
    CREATE UNIQUE INDEX i1 ON t1(a, b, c);
  } {
    SELECT * FROM t1 WHERE a = ? AND b = ?
  } 

  6 0 {
  } {
    SELECT * FROM t1 WHERE c IN (SELECT z FROM t2)
  } 

  7 0 {
  } {
    SELECT * FROM t1 WHERE (a, c) IN (SELECT z, x FROM t2)
  } 

  8 1 {
  } {
    SELECT * FROM t1 WHERE a IN (SELECT z FROM t2)
  } 

  9 1 {
    CREATE UNIQUE INDEX i1 ON t1(a, b);
  } {
    SELECT * FROM t1 WHERE a IN (SELECT z FROM t2) AND b IS ?
  } 
  10 0 {
    CREATE UNIQUE INDEX i1 ON t1(a, b);
  } {
    SELECT * FROM t1 WHERE a IN (SELECT z FROM t2) AND b = ?
  } 
  11 1 {
    CREATE UNIQUE INDEX i1 ON t1(a, b);
  } {
    SELECT * FROM t1 WHERE a IS NULL AND b IN (SELECT z FROM t2)
  } 
  12 0 {
    CREATE UNIQUE INDEX i1 ON t1(a, b);
  } {
    SELECT * FROM t1 WHERE a = ? AND b IN (SELECT z FROM t2)
  } 
} {
  do_test 1.1.$tn {
    execsql BEGIN
    execsql $idx

    catch { array unset root_to_tbl }
    catch { array unset csr_to_root }

    db eval {SELECT rootpage, tbl_name FROM sqlite_schema} {
      set root_to_tbl($rootpage) $tbl_name
    }

    set nSeen 0
    db eval "explain $sql" {
      if {$opcode=="OpenRead"} {
        set csr_to_root($p1) $p2
      }
      if {$opcode=="Next"} {
        catch {
          set root $csr_to_root($p1)
          set tbl $root_to_tbl($root)
          if {$tbl=="t1"} {incr nSeen}
        }
      }
    }

    execsql ROLLBACK

    set nSeen
  } $nNext
}

#-------------------------------------------------------------------------
reset_db
do_execsql_test 2.0 {
  CREATE TABLE t1(a TEXT PRIMARY KEY, b TEXT) WITHOUT ROWID;
  INSERT INTO t1 VALUES('1', 'one');
  INSERT INTO t1 VALUES('2', NULL);
  INSERT INTO t1 VALUES('3', 'three');
}

do_execsql_test 2.1 {
  SELECT b FROM t1 WHERE a IN (1,2,3) ORDER BY b ASC NULLS LAST;
} {one three {}}

#-------------------------------------------------------------------------
reset_db
do_execsql_test 3.0 {
  CREATE TABLE x1(a);
  INSERT INTO x1 VALUES(1), (2), (3);

  CREATE TABLE x2(b);
  INSERT INTO x2 VALUES(4), (5), (6);

  CREATE TABLE t1(u);
  INSERT INTO t1 VALUES(1), (2), (3), (4), (5), (6);

  CREATE VIEW v1 AS SELECT u FROM t1 WHERE u IN (
    SELECT a FROM x1
  );
  CREATE VIEW v2 AS SELECT u FROM t1 WHERE u IN (
    SELECT b FROM x2
  );
}

do_execsql_test 3.1 {
  SELECT * FROM v1
} {
  1 2 3
}

do_execsql_test 3.2 {
  SELECT * FROM v2
} {
  4 5 6 
}

do_execsql_test 3.3 {
  SELECT * FROM v2
  UNION ALL
  SELECT * FROM v1
} {
  4 5 6
  1 2 3 
}

do_execsql_test 3.4 {
  WITH w1 AS (
    SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3
  ),
  w2 AS (
    SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6
  )
  SELECT * FROM v1 WHERE u IN w1
  UNION ALL
  SELECT * FROM v2 WHERE u IN w2
} {
  1 2 3 4 5 6
}

# 2024-11-20 https://sqlite.org/forum/forumpost/0b9ded2f8428ac00
#
# Bug in SubrtnSig logic.  If a SELECT statement is copied and the copy
# is subsequently modified, we need to change the Select.selId on the
# copy so that when the copy is used to generate code, the SubrtnSig
# logic won't try to substitute the original SELECT in place of the
# copy which is now different.
#
do_execsql_test 3.5 {
  DROP TABLE IF EXISTS t1;
  DROP TABLE IF EXISTS t2;
  CREATE TABLE t1 (a int UNIQUE);
  CREATE TABLE t2 (b int UNIQUE);
  INSERT INTO t1 VALUES (1);
  INSERT INTO t2 VALUES (1), (2);
  SELECT t1.a, t2.b FROM t1, t2 WHERE (t1.a, t2.b) = (1, 1);
} {1 1}
do_execsql_test 3.6 {
  SELECT t1.a, t2.b FROM t1, t2 WHERE (t1.a, t2.b) IN ((1, 1));
} {1 1}
do_execsql_test 3.7 {
  SELECT t1.a, t2.b FROM t1, t2 WHERE (t1.a, t2.b) = (1, 2);
} {1 2}
do_execsql_test 3.8 {
  SELECT t1.a, t2.b FROM t1, t2 WHERE (t1.a, t2.b) IN ((1, 2));
} {1 2}

# 2025-01-30 Inifinite loop in byte-code discovered by dbsqlfuzz
# having to do with SubrtnSig logic.  The code was using a Subroutine
# from within itself resulting in infinite recursion.
#
# This test will spin forever if the bug has not been fixed, or if
# it reappears.
#
reset_db
do_execsql_test 4.0 {
  CREATE TABLE t1(a INTEGER PRIMARY KEY, b);
  INSERT INTO t1 VALUES(1,x'1111');
  CREATE TABLE t2(c);
  CREATE TABLE t3(d);
  CREATE TRIGGER t1tr UPDATE ON t1 BEGIN
    UPDATE t1 SET b=x'2222' FROM t2;
    UPDATE t1
       SET b = (SELECT a IN (SELECT a
                               FROM t1
                              WHERE (b,a) IN (SELECT rowid, d
                                                FROM t3
                                             )
                            )
                  FROM t1 NATURAL RIGHT JOIN t1
               );
  END;
  UPDATE t1 SET b=x'3333';
  SELECT quote(b) FROM t1;
} {X'3333'}

finish_test
