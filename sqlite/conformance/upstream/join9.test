# 2022-04-16
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
# This file implements tests for RIGHT and FULL OUTER JOINs.

set testdir [file dirname $argv0]
source $testdir/tester.tcl

foreach {id schema} {
  1 {
    CREATE TABLE t3(id INTEGER PRIMARY KEY, w TEXT);
    CREATE TABLE t4(id INTEGER PRIMARY KEY, x TEXT);
    CREATE TABLE t5(id INTEGER PRIMARY KEY, y TEXT);
    CREATE TABLE t6(id INTEGER PRIMARY KEY, z INT);
    CREATE VIEW dual(dummy) AS VALUES('x');
    INSERT INTO t3(id,w) VALUES(2,'two'),(3,'three'),(6,'six'),(7,'seven');
    INSERT INTO t4(id,x) VALUES(2,'alice'),(4,'bob'),(6,'cindy'),(8,'dave');
    INSERT INTO t5(id,y) VALUES(1,'red'),(2,'orange'),(3,'yellow'),(4,'green'),
                               (5,'blue');
    INSERT INTO t6(id,z) VALUES(3,333),(4,444),(5,555),(0,1000),(9,999);
  }
  2 {
    CREATE TABLE t3(id INT PRIMARY KEY, w TEXT) WITHOUT ROWID;
    CREATE TABLE t4(id INT PRIMARY KEY, x TEXT) WITHOUT ROWID;
    CREATE TABLE t5(id INT PRIMARY KEY, y TEXT) WITHOUT ROWID;
    CREATE TABLE t6(id INT PRIMARY KEY, z INT) WITHOUT ROWID;
    CREATE TABLE dual(dummy TEXT);
    INSERT INTO dual(dummy) VALUES('x');
    INSERT INTO t3(id,w) VALUES(2,'two'),(3,'three'),(6,'six'),(7,'seven');
    INSERT INTO t4(id,x) VALUES(2,'alice'),(4,'bob'),(6,'cindy'),(8,'dave');
    INSERT INTO t5(id,y) VALUES(1,'red'),(2,'orange'),(3,'yellow'),(4,'green'),
                               (5,'blue');
    INSERT INTO t6(id,z) VALUES(3,333),(4,444),(5,555),(0,1000),(9,999);
  }
  3 {
    CREATE TABLE t3x(id INTEGER PRIMARY KEY, w TEXT);
    CREATE TABLE t4x(id INTEGER PRIMARY KEY, x TEXT);
    CREATE TABLE t5x(id INTEGER PRIMARY KEY, y TEXT);
    CREATE TABLE t6x(id INTEGER PRIMARY KEY, z INT);
    CREATE VIEW dual(dummy) AS VALUES('x');
    INSERT INTO t3x(id,w) VALUES(2,'two'),(3,'three'),(6,'six'),(7,'seven');
    INSERT INTO t4x(id,x) VALUES(2,'alice'),(4,'bob'),(6,'cindy'),(8,'dave');
    INSERT INTO t5x(id,y) VALUES(1,'red'),(2,'orange'),(3,'yellow'),(4,'green'),
                               (5,'blue');
    INSERT INTO t6x(id,z) VALUES(3,333),(4,444),(5,555),(0,1000),(9,999);
    CREATE VIEW t3 AS SELECT * FROM t3x LIMIT 1000;
    CREATE VIEW t4 AS SELECT * FROM t4x LIMIT 1000;
    CREATE VIEW t5 AS SELECT * FROM t5x LIMIT 1000;
    CREATE VIEW t6 AS SELECT * FROM t6x LIMIT 1000;
  }
  4 {
    CREATE TABLE t3a(id INTEGER PRIMARY KEY, w TEXT);
    CREATE TABLE t3b(id INTEGER PRIMARY KEY, w TEXT);
    CREATE TABLE t4a(id INTEGER PRIMARY KEY, x TEXT);
    CREATE TABLE t4b(id INTEGER PRIMARY KEY, x TEXT);
    CREATE TABLE t5a(id INTEGER PRIMARY KEY, y TEXT);
    CREATE TABLE t5b(id INTEGER PRIMARY KEY, y TEXT);
    CREATE TABLE t6a(id INTEGER PRIMARY KEY, z INT);
    CREATE TABLE t6b(id INTEGER PRIMARY KEY, z INT);
    CREATE VIEW dual(dummy) AS VALUES('x');
    INSERT INTO t3a(id,w) VALUES(2,'two'),(3,'three');
    INSERT INTO t3b(id,w) VALUES(6,'six'),(7,'seven');
    INSERT INTO t4a(id,x) VALUES(2,'alice'),(4,'bob');
    INSERT INTO t4b(id,x) VALUES(6,'cindy'),(8,'dave');
    INSERT INTO t5a(id,y) VALUES(1,'red'),(2,'orange'),(3,'yellow');
    INSERT INTO t5b(id,y) VALUES(4,'green'),(5,'blue');
    INSERT INTO t6a(id,z) VALUES(3,333),(4,444);
    INSERT INTO t6b(id,z) VALUES(5,555),(0,1000),(9,999);
    CREATE VIEW t3 AS SELECT * FROM t3a UNION ALL SELECT * FROM t3b;
    CREATE VIEW t4 AS SELECT * FROM t4a UNION ALL SELECT * FROM t4b;
    CREATE VIEW t5 AS SELECT * FROM t5a UNION ALL SELECT * FROM t5b;
    CREATE VIEW t6 AS SELECT * FROM t6a UNION ALL SELECT * FROM t6b;
  }
  5 {
    CREATE TABLE t3a(id INTEGER PRIMARY KEY, w TEXT) WITHOUT ROWID;
    CREATE TABLE t3b(id INTEGER PRIMARY KEY, w TEXT);
    CREATE TABLE t4a(id INTEGER PRIMARY KEY, x TEXT) WITHOUT ROWID;
    CREATE TABLE t4b(id INTEGER PRIMARY KEY, x TEXT) WITHOUT ROWID;
    CREATE TABLE t5a(id INTEGER PRIMARY KEY, y TEXT);
    CREATE TABLE t5b(id INTEGER PRIMARY KEY, y TEXT) WITHOUT ROWID;
    CREATE TABLE t6a(id INTEGER PRIMARY KEY, z INT);
    CREATE TABLE t6b(id INTEGER PRIMARY KEY, z INT);
    CREATE VIEW dual(dummy) AS VALUES('x');
    INSERT INTO t3a(id,w) VALUES(2,'two'),(3,'three');
    INSERT INTO t3b(id,w) VALUES(6,'six'),(7,'seven');
    INSERT INTO t4a(id,x) VALUES(2,'alice'),(4,'bob');
    INSERT INTO t4b(id,x) VALUES(6,'cindy'),(8,'dave');
    INSERT INTO t5a(id,y) VALUES(1,'red'),(2,'orange'),(3,'yellow');
    INSERT INTO t5b(id,y) VALUES(4,'green'),(5,'blue');
    INSERT INTO t6a(id,z) VALUES(3,333),(4,444);
    INSERT INTO t6b(id,z) VALUES(5,555),(0,1000),(9,999);
    CREATE VIEW t3 AS SELECT * FROM t3a UNION ALL SELECT * FROM t3b;
    CREATE VIEW t4 AS SELECT * FROM t4a UNION ALL SELECT * FROM t4b LIMIT 50;
    CREATE VIEW t5 AS SELECT * FROM t5a UNION ALL SELECT * FROM t5b LIMIT 100;
    CREATE VIEW t6 AS SELECT * FROM t6a UNION ALL SELECT * FROM t6b;
  }
} {
  reset_db
  db nullvalue -
  do_execsql_test join9-$id.setup $schema {}

  # Verifid by PG-14 for case 1
  do_execsql_test join9-$id.100 {
    SELECT *, t4.id, t5.id, t6.id
      FROM t4 NATURAL LEFT JOIN t5 NATURAL LEFT JOIN t6
     ORDER BY 1;
  } {
    2   alice  orange  -    2   2   - 
    4   bob    green   444  4   4   4 
    6   cindy  -       -    6   -   - 
    8   dave   -       -    8   -   - 
  }

  do_execsql_test join9-$id.101 {
    SELECT *, t4.id, t5.id, t6.id
      FROM t4 NATURAL LEFT JOIN t5 NATURAL LEFT JOIN t6
     ORDER BY id;
  } {
    2   alice  orange  -    2   2   - 
    4   bob    green   444  4   4   4 
    6   cindy  -       -    6   -   - 
    8   dave   -       -    8   -   - 
  }
  do_execsql_test join9-$id.102 {
    SELECT *, t4.id, t5.id, t6.id
      FROM t4 LEFT JOIN t5 USING(id) LEFT JOIN t6 USING(id)
     ORDER BY id;
  } {
    2   alice  orange  -    2   2   - 
    4   bob    green   444  4   4   4 
    6   cindy  -       -    6   -   - 
    8   dave   -       -    8   -   - 
  }

  # Verifid by PG-14 using case 1
  do_execsql_test join9-$id.200 {
    SELECT id, x, y, z, t4.id, t5.id, t6.id
      FROM t5 NATURAL RIGHT JOIN t4 NATURAL LEFT JOIN t6
     ORDER BY 1;
  } {
    2   alice  orange  -    2   2   - 
    4   bob    green   444  4   4   4 
    6   cindy  -       -    6   -   - 
    8   dave   -       -    8   -   - 
  }

  do_execsql_test join9-$id.201 {
    SELECT id, x, y, z, t4.id, t5.id, t6.id
      FROM t5 NATURAL RIGHT JOIN t4 NATURAL LEFT JOIN t6
     ORDER BY id;
  } {
    2   alice  orange  -    2   2   - 
    4   bob    green   444  4   4   4 
    6   cindy  -       -    6   -   - 
    8   dave   -       -    8   -   - 
  }

  # Verified by PG-14 using case 1
  do_execsql_test join9-$id.300 {
    SELECT *, t4.id, t5.id, t6.id
      FROM t4 NATURAL RIGHT JOIN t5 NATURAL RIGHT JOIN t6
     ORDER BY 1;
  } {
    0   -    -       1000  -   -   0 
    3   -    yellow  333   -   3   3 
    4   bob  green   444   4   4   4 
    5   -    blue    555   -   5   5 
    9   -    -       999   -   -   9 
  }

  do_execsql_test join9-$id.301 {
    SELECT *, t4.id, t5.id, t6.id
      FROM t4 NATURAL RIGHT JOIN t5 NATURAL RIGHT JOIN t6
     ORDER BY id;
  } {
    0   -    -       1000  -   -   0 
    3   -    yellow  333   -   3   3 
    4   bob  green   444   4   4   4 
    5   -    blue    555   -   5   5 
    9   -    -       999   -   -   9 
  }

  # Verified by PG-14 for case 1
  do_execsql_test join9-$id.400 {
    SELECT *, t4.id, t5.id, t6.id
      FROM t4 NATURAL FULL JOIN t5 NATURAL FULL JOIN t6
     ORDER BY 1;
  } {
    0    -      -       1000  -   -   0 
    1    -      red     -     -   1   - 
    2    alice  orange  -     2   2   - 
    3    -      yellow  333   -   3   3 
    4    bob    green   444   4   4   4 
    5    -      blue    555   -   5   5 
    6    cindy  -       -     6   -   - 
    8    dave   -       -     8   -   - 
    9    -      -       999   -   -   9 
  }

  do_execsql_test join9-$id.401 {
    SELECT *, t4.id, t5.id, t6.id
      FROM t4 NATURAL FULL JOIN t5 NATURAL FULL JOIN t6
     ORDER BY id;
  } {
    0    -      -       1000  -   -   0 
    1    -      red     -     -   1   - 
    2    alice  orange  -     2   2   - 
    3    -      yellow  333   -   3   3 
    4    bob    green   444   4   4   4 
    5    -      blue    555   -   5   5 
    6    cindy  -       -     6   -   - 
    8    dave   -       -     8   -   - 
    9    -      -       999   -   -   9 
  }
  do_execsql_test join9-$id.402 {
    SELECT id, x, y, z, t4.id, t5.id, t6.id
      FROM t4 NATURAL FULL JOIN t6 NATURAL FULL JOIN t5
     ORDER BY id;
  } {
    0    -      -       1000  -   -   0 
    1    -      red     -     -   1   - 
    2    alice  orange  -     2   2   - 
    3    -      yellow  333   -   3   3 
    4    bob    green   444   4   4   4 
    5    -      blue    555   -   5   5 
    6    cindy  -       -     6   -   - 
    8    dave   -       -     8   -   - 
    9    -      -       999   -   -   9 
  }
  do_execsql_test join9-$id.403 {
    SELECT id, x, y, z, t4.id, t5.id, t6.id
      FROM t5 NATURAL FULL JOIN t4 NATURAL FULL JOIN t6
     ORDER BY id;
  } {
    0    -      -       1000  -   -   0 
    1    -      red     -     -   1   - 
    2    alice  orange  -     2   2   - 
    3    -      yellow  333   -   3   3 
    4    bob    green   444   4   4   4 
    5    -      blue    555   -   5   5 
    6    cindy  -       -     6   -   - 
    8    dave   -       -     8   -   - 
    9    -      -       999   -   -   9 
  }
  do_execsql_test join9-$id.404 {
    SELECT id, x, y, z, t4.id, t5.id, t6.id
      FROM t5 NATURAL FULL JOIN t6 NATURAL FULL JOIN t4
     ORDER BY id;
  } {
    0    -      -       1000  -   -   0 
    1    -      red     -     -   1   - 
    2    alice  orange  -     2   2   - 
    3    -      yellow  333   -   3   3 
    4    bob    green   444   4   4   4 
    5    -      blue    555   -   5   5 
    6    cindy  -       -     6   -   - 
    8    dave   -       -     8   -   - 
    9    -      -       999   -   -   9 
  }
  do_execsql_test join9-$id.405 {
    SELECT id, x, y, z, t4.id, t5.id, t6.id
      FROM t6 NATURAL FULL JOIN t4 NATURAL FULL JOIN t5
     ORDER BY id;
  } {
    0    -      -       1000  -   -   0 
    1    -      red     -     -   1   - 
    2    alice  orange  -     2   2   - 
    3    -      yellow  333   -   3   3 
    4    bob    green   444   4   4   4 
    5    -      blue    555   -   5   5 
    6    cindy  -       -     6   -   - 
    8    dave   -       -     8   -   - 
    9    -      -       999   -   -   9 
  }
  do_execsql_test join9-$id.406 {
    SELECT id, x, y, z, t4.id, t5.id, t6.id
      FROM t6 NATURAL FULL JOIN t5 NATURAL FULL JOIN t4
     ORDER BY id;
  } {
    0    -      -       1000  -   -   0 
    1    -      red     -     -   1   - 
    2    alice  orange  -     2   2   - 
    3    -      yellow  333   -   3   3 
    4    bob    green   444   4   4   4 
    5    -      blue    555   -   5   5 
    6    cindy  -       -     6   -   - 
    8    dave   -       -     8   -   - 
    9    -      -       999   -   -   9 
  }

  # Verified by PG-14 using case 1
  do_execsql_test join9-$id.500 {
    SELECT id, w, x, y, z
      FROM t3 FULL JOIN t4 USING(id)
              NATURAL FULL JOIN t5
              FULL JOIN t6 USING(id)
      ORDER BY 1;
  } {
    0   -      -      -       1000
    1   -      -      red     -   
    2   two    alice  orange  -   
    3   three  -      yellow  333 
    4   -      bob    green   444 
    5   -      -      blue    555 
    6   six    cindy  -       -   
    7   seven  -      -       -   
    8   -      dave   -       -   
    9   -      -      -       999 
  }

  # Verified by PG-14 using case 1
  do_execsql_test join9-$id.600 {
    SELECT id, w, x, y, z
       FROM t3 JOIN dual AS d1 ON true
               FULL JOIN t4 USING(id)
               JOIN dual AS d2 ON true
               NATURAL FULL JOIN t5
               JOIN dual AS d3 ON true
               FULL JOIN t6 USING(id)
               CROSS JOIN dual AS d4
      ORDER BY 1;
  } {
    0   -      -      -       1000
    1   -      -      red     -   
    2   two    alice  orange  -   
    3   three  -      yellow  333 
    4   -      bob    green   444 
    5   -      -      blue    555 
    6   six    cindy  -       -   
    7   seven  -      -       -   
    8   -      dave   -       -   
    9   -      -      -       999 
  }

  # Verified by PG-14 using case 1
  do_execsql_test join9-$id.700 {
    SELECT id, w, x, y, z
       FROM t3 JOIN dual AS d1 ON true
               FULL JOIN t4 USING(id)
               JOIN dual AS d2 ON true
               NATURAL FULL JOIN t5
               JOIN dual AS d3 ON true
               FULL JOIN t6 USING(id)
               CROSS JOIN dual AS d4
      WHERE x<>'bob' OR x IS NULL
      ORDER BY 1;
  } {
    0   -      -      -       1000
    1   -      -      red     -   
    2   two    alice  orange  -   
    3   three  -      yellow  333 
    5   -      -      blue    555 
    6   six    cindy  -       -   
    7   seven  -      -       -   
    8   -      dave   -       -   
    9   -      -      -       999 
  }

  # Verified by PG-14 using case 1
  do_execsql_test join9-$id.800 {
    WITH t7(id,a) AS MATERIALIZED (SELECT * FROM t4 WHERE false)
    SELECT *
      FROM t7 
           JOIN t7 AS t7b USING(id)
           FULL JOIN t3 USING(id);
  } {
    2   -  -  two  
    3   -  -  three
    6   -  -  six  
    7   -  -  seven
  }

  # Verified by PG-14
  do_execsql_test join9-$id.900 {
    SELECT *
      FROM (t3 NATURAL FULL JOIN t4)
           NATURAL FULL JOIN
           (t5 NATURAL FULL JOIN t6)
    ORDER BY 1;
  } {
    0   -      -      -       1000
    1   -      -      red     -   
    2   two    alice  orange  -   
    3   three  -      yellow  333 
    4   -      bob    green   444 
    5   -      -      blue    555 
    6   six    cindy  -       -   
    7   seven  -      -       -   
    8   -      dave   -       -   
    9   -      -      -       999 
  }
  do_execsql_test join9-$id.910 {
    SELECT *
      FROM t3 NATURAL FULL JOIN 
           (t4 NATURAL FULL JOIN
            (t5 NATURAL FULL JOIN t6))
    ORDER BY 1;
  } {
    0   -      -      -       1000
    1   -      -      red     -   
    2   two    alice  orange  -   
    3   three  -      yellow  333 
    4   -      bob    green   444 
    5   -      -      blue    555 
    6   six    cindy  -       -   
    7   seven  -      -       -   
    8   -      dave   -       -   
    9   -      -      -       999 
  }
  do_execsql_test join9-$id.920 {
    SELECT *
      FROM t3 FULL JOIN (
                t4 FULL JOIN (
                    t5 FULL JOIN t6 USING (id)
                ) USING(id)
           ) USING(id)
    ORDER BY 1;
  } {
    0   -      -      -       1000
    1   -      -      red     -   
    2   two    alice  orange  -   
    3   three  -      yellow  333 
    4   -      bob    green   444 
    5   -      -      blue    555 
    6   six    cindy  -       -   
    7   seven  -      -       -   
    8   -      dave   -       -   
    9   -      -      -       999 
  }
  do_execsql_test join9-$id.920 {
    SELECT *
      FROM t3 FULL JOIN (
                t4 FULL JOIN (
                    t5 FULL JOIN t6 USING (id)
                ) USING(id)
           ) USING(id)
    ORDER BY 1;
  } {
    0   -      -      -       1000
    1   -      -      red     -   
    2   two    alice  orange  -   
    3   three  -      yellow  333 
    4   -      bob    green   444 
    5   -      -      blue    555 
    6   six    cindy  -       -   
    7   seven  -      -       -   
    8   -      dave   -       -   
    9   -      -      -       999 
  }

  # Verified by PG-14
  do_execsql_test join9-$id.930 {
    SELECT *
      FROM t3 FULL JOIN (
               t4 FULL JOIN (
                   t5 FULL JOIN t6 USING(id)
               ) USING(id)
           ) AS j1 ON j1.id=t3.id
     ORDER BY coalesce(t3.id,j1.id);
  } {
    -   -      0   -      -       1000
    -   -      1   -      red     -   
    2   two    2   alice  orange  -   
    3   three  3   -      yellow  333 
    -   -      4   bob    green   444 
    -   -      5   -      blue    555 
    6   six    6   cindy  -       -   
    7   seven  -   -      -       -   
    -   -      8   dave   -       -   
    -   -      9   -      -       999 
  }

  # Verified by PG-14
  do_execsql_test join9-$id.940 {
    SELECT *
      FROM t3 FULL JOIN (
                t4 RIGHT JOIN (
                    t5 FULL JOIN t6 USING(id)
                ) USING(id)
           ) AS j1 ON j1.id=t3.id
     ORDER BY coalesce(t3.id,j1.id);
  } {
    -   -      0   -      -       1000
    -   -      1   -      red     -   
    2   two    2   alice  orange  -   
    3   three  3   -      yellow  333 
    -   -      4   bob    green   444 
    -   -      5   -      blue    555 
    6   six    -   -      -       -   
    7   seven  -   -      -       -   
    -   -      9   -      -       999 
  }

  # Verified by PG-14
  do_execsql_test join9-$id.950 {
    SELECT *
      FROM t3 FULL JOIN (
                t4 LEFT JOIN (
                    t5 FULL JOIN t6 USING(id)
                ) USING(id)
           ) AS j1 ON j1.id=t3.id
     ORDER BY coalesce(t3.id,j1.id);
  } {
    2   two    2   alice  orange  -  
    3   three  -   -      -       -  
    -   -      4   bob    green   444
    6   six    6   cindy  -       -  
    7   seven  -   -      -       -  
    -   -      8   dave   -       -  
  }

  # Restriction (27) in the query flattener
  # Verified by PG-14
  do_execsql_test join9-$id.1000 {
    WITH t56(id,y,z) AS (SELECT * FROM t5 FULL JOIN t6 USING(id) LIMIT 50)
    SELECT id,x,y,z FROM t4 JOIN t56 USING(id)
    ORDER BY 1;
  } {
    2   alice  orange  -  
    4   bob    green   444
  }

  # Verified by PG-14
  do_execsql_test join9-$id.1010 {
    SELECT id,x,y,z
      FROM t4 INNER JOIN (t5 FULL JOIN t6 USING(id)) USING(id)
     ORDER BY 1;
  } {
    2   alice  orange  -  
    4   bob    green   444
  }

  # Verified by PG-14
  do_execsql_test join9-$id.1020 {
    SELECT id,x,y,z
      FROM t4 FULL JOIN t5 USING(id) INNER JOIN t6 USING(id)
     ORDER BY 1;
  } {
    3   -    yellow  333
    4   bob  green   444
    5   -    blue    555
  }

  # Verified by PG-14
  do_execsql_test join9-$id.1030 {
    WITH t45(id,x,y) AS (SELECT * FROM t4 FULL JOIN t5 USING(id) LIMIT 50)
    SELECT id,x,y,z FROM t45 JOIN t6 USING(id)
    ORDER BY 1;
  } {
    3   -    yellow  333
    4   bob  green   444
    5   -    blue    555
  }

}
finish_test
