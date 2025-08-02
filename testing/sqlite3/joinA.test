# 2022-04-18
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
    CREATE TABLE t1(a INT, b INT, c INT, d INT);
    CREATE TABLE t2(c INT, d INT, e INT, f INT);
    CREATE TABLE t3(a INT, b INT, e INT, f INT);
    CREATE TABLE t4(a INT, c INT, d INT, f INT);
    INSERT INTO t1 VALUES(11,21,31,41),(12,22,32,42),(15,25,35,45),(18,28,38,48);
    INSERT INTO t2 VALUES(12,22,32,42),(13,23,33,43),(15,25,35,45),(17,27,37,47);
    INSERT INTO t3 VALUES(14,24,34,44),(15,25,35,45),(16,26,36,46);
    INSERT INTO t4 VALUES(11,21,31,41),(13,23,33,43),(16,26,36,46),(19,29,39,49);
  }
  2 {
    CREATE TABLE t1(a INTEGER PRIMARY KEY, b INT, c INT, d INT);
    CREATE TABLE t2(c INT, d INTEGER PRIMARY KEY, e INT, f INT);
    CREATE TABLE t3(a INT, b INT, e INTEGER PRIMARY KEY, f INT);
    CREATE TABLE t4(a INT, c INT, d INT, f INT PRIMARY KEY) WITHOUT ROWID;
    INSERT INTO t1 VALUES(11,21,31,41),(12,22,32,42),(15,25,35,45),(18,28,38,48);
    INSERT INTO t2 VALUES(12,22,32,42),(13,23,33,43),(15,25,35,45),(17,27,37,47);
    INSERT INTO t3 VALUES(14,24,34,44),(15,25,35,45),(16,26,36,46);
    INSERT INTO t4 VALUES(11,21,31,41),(13,23,33,43),(16,26,36,46),(19,29,39,49);
  }
  3 {
    CREATE TABLE t1a(a INT, b INT, c INT, d INT);
    CREATE TABLE t2a(c INT, d INT, e INT, f INT);
    CREATE TABLE t3a(a INT, b INT, e INT, f INT);
    CREATE TABLE t4a(a INT, c INT, d INT, f INT);
    INSERT INTO t1a VALUES(11,21,31,41),(12,22,32,42);
    INSERT INTO t2a VALUES(12,22,32,42),(13,23,33,43);
    INSERT INTO t3a VALUES(14,24,34,44),(15,25,35,45);
    INSERT INTO t4a VALUES(11,21,31,41),(13,23,33,43);
    CREATE TABLE t1b(a INT, b INT, c INT, d INT);
    CREATE TABLE t2b(c INT, d INT, e INT, f INT);
    CREATE TABLE t3b(a INT, b INT, e INT, f INT);
    CREATE TABLE t4b(a INT, c INT, d INT, f INT);
    INSERT INTO t1b VALUES(15,25,35,45),(18,28,38,48);
    INSERT INTO t2b VALUES(15,25,35,45),(17,27,37,47);
    INSERT INTO t3b VALUES(15,25,35,45),(16,26,36,46);
    INSERT INTO t4b VALUES(16,26,36,46),(19,29,39,49);
    CREATE VIEW t1 AS SELECT * FROM t1a UNION SELECT * FROM t1b;
    CREATE VIEW t2 AS SELECT * FROM t2a UNION SELECT * FROM t2b;
    CREATE VIEW t3 AS SELECT * FROM t3a UNION SELECT * FROM t3b;
    CREATE VIEW t4 AS SELECT * FROM t4a UNION SELECT * FROM t4b;
  }
} {
  reset_db
  db nullvalue -
  do_execsql_test joinA-$id.setup $schema {}

  # Verified by PG-14
  do_execsql_test joinA-$id.100 {
    SELECT a,b,c,d,t2.e,f,t3.e
      FROM t1
           INNER JOIN t2 USING(c,d)
           INNER JOIN t3 USING(a,b,f)
           INNER JOIN t4 USING(a,c,d,f)
    ORDER BY 1 nulls first, 3 nulls first;
  } {}


  # Verified by PG-14
  do_execsql_test joinA-$id.110 {
    SELECT a,b,c,d,t2.e,f,t3.e
      FROM t1
           LEFT JOIN t2 USING(c,d)
           LEFT JOIN t3 USING(a,b,f)
           LEFT JOIN t4 USING(a,c,d,f)
    ORDER BY 1 nulls first, 3 nulls first;
  } {
    11  21  31  41  -  -  -
    12  22  32  42  -  -  -
    15  25  35  45  -  -  -
    18  28  38  48  -  -  -
  }

  # Verified by PG-14
  do_execsql_test joinA-$id.120 {
    SELECT a,b,c,d,t2.e,f,t3.e
      FROM t1
           LEFT JOIN t2 USING(c,d)
           RIGHT JOIN t3 USING(a,b,f)
           LEFT JOIN t4 USING(a,c,d,f)
    ORDER BY 1 nulls first, 3 nulls first;
  } {
    14  24  -  -  -  44  34
    15  25  -  -  -  45  35
    16  26  -  -  -  46  36
  }

  # Verified by PG-14
  do_execsql_test joinA-$id.130 {
    SELECT a,b,c,d,t2.e,f,t3.e
      FROM t1
           RIGHT JOIN t2 USING(c,d)
           LEFT JOIN t3 USING(a,b,f)
           RIGHT JOIN t4 USING(a,c,d,f)
    ORDER BY 1 nulls first, 3 nulls first;
  } {
    11  -  21  31  -  41  -
    13  -  23  33  -  43  -
    16  -  26  36  -  46  -
    19  -  29  39  -  49  -
  }

  # Verified by PG-14
  do_execsql_test joinA-$id.140 {
    SELECT a,b,c,d,t2.e,f,t3.e
      FROM t1
           FULL JOIN t2 USING(c,d)
           LEFT JOIN t3 USING(a,b,f)
           RIGHT JOIN t4 USING(a,c,d,f)
    ORDER BY 1 nulls first, 3 nulls first;
  } {
    11  -  21  31  -  41  -
    13  -  23  33  -  43  -
    16  -  26  36  -  46  -
    19  -  29  39  -  49  -
  }

  # Verified by PG-14
  do_execsql_test joinA-$id.150 {
    SELECT a,b,c,d,t2.e,f,t3.e
      FROM t1
           RIGHT JOIN t2 USING(c,d)
           FULL JOIN t3 USING(a,b,f)
           RIGHT JOIN t4 USING(a,c,d,f)
    ORDER BY 1 nulls first, 3 nulls first;
  } {
    11  -  21  31  -  41  -
    13  -  23  33  -  43  -
    16  -  26  36  -  46  -
    19  -  29  39  -  49  -
  }

  # Verified by PG-14
  do_execsql_test joinA-$id.160 {
    SELECT a,b,c,d,t2.e,f,t3.e
      FROM t1
           RIGHT JOIN t2 USING(c,d)
           LEFT JOIN t3 USING(a,b,f)
           FULL JOIN t4 USING(a,c,d,f)
    ORDER BY 1 nulls first, 3 nulls first;
  } {
    -   -  12  22  32  42  -
    -   -  13  23  33  43  -
    -   -  15  25  35  45  -
    -   -  17  27  37  47  -
    11  -  21  31  -   41  -
    13  -  23  33  -   43  -
    16  -  26  36  -   46  -
    19  -  29  39  -   49  -
  }

  # Verified by PG-14
  do_execsql_test joinA-$id.170 {
    SELECT a,b,c,d,t2.e,f,t3.e
      FROM t1
           LEFT JOIN t2 USING(c,d)
           RIGHT JOIN t3 USING(a,b,f)
           FULL JOIN t4 USING(a,c,d,f)
    ORDER BY 1 nulls first, 3 nulls first;
  } {
    11  -   21  31  -  41  - 
    13  -   23  33  -  43  - 
    14  24  -   -   -  44  34
    15  25  -   -   -  45  35
    16  26  -   -   -  46  36
    16  -   26  36  -  46  - 
    19  -   29  39  -  49  - 
  }

  # Verified by PG-14
  do_execsql_test joinA-$id.200 {
    SELECT a,b,c,d,t2.e,f,t3.e
      FROM t1
           FULL JOIN t2 USING(c,d)
           FULL JOIN t3 USING(a,b,f)
           FULL JOIN t4 USING(a,c,d,f)
    ORDER BY 1 nulls first, 3 nulls first;
  } {
    -   -   12  22  32  42  - 
    -   -   13  23  33  43  - 
    -   -   15  25  35  45  - 
    -   -   17  27  37  47  - 
    11  -   21  31  -   41  - 
    11  21  31  41  -   -   - 
    12  22  32  42  -   -   - 
    13  -   23  33  -   43  - 
    14  24  -   -   -   44  34
    15  25  -   -   -   45  35
    15  25  35  45  -   -   - 
    16  26  -   -   -   46  36
    16  -   26  36  -   46  - 
    18  28  38  48  -   -   - 
    19  -   29  39  -   49  - 
  }

  # Verified by PG-14
  do_execsql_test joinA-$id.201 {
    SELECT a,b,c,d,t2.e,f,t3.e,t1.a
      FROM t1
           FULL JOIN t2 USING(c,d)
           FULL JOIN t3 USING(a,b,f)
           FULL JOIN t4 USING(a,c,d,f)
     WHERE t1.a!=0
    ORDER BY 1 nulls first, 3 nulls first;
  } {
    11  21  31  41  -   -   -  11
    12  22  32  42  -   -   -  12
    15  25  35  45  -   -   -  15
    18  28  38  48  -   -   -  18
  }

  # Verified by PG-14
  do_execsql_test joinA-$id.202 {
    SELECT a,b,c,d,t2.e,f,t3.e,t3.a
      FROM t1
           FULL JOIN t2 USING(c,d)
           FULL JOIN t3 USING(a,b,f)
           FULL JOIN t4 USING(a,c,d,f)
     WHERE t3.a!=0
    ORDER BY 1 nulls first, 3 nulls first;
  } {
    14  24  -   -   -   44  34  14
    15  25  -   -   -   45  35  15
    16  26  -   -   -   46  36  16
  }

  # Verified by PG-14
  do_execsql_test joinA-$id.203 {
    SELECT a,b,c,d,t2.e,f,t3.e,t4.a
      FROM t1
           FULL JOIN t2 USING(c,d)
           FULL JOIN t3 USING(a,b,f)
           FULL JOIN t4 USING(a,c,d,f)
     WHERE t4.a!=0
    ORDER BY 1 nulls first, 3 nulls first;
  } {
    11  -   21  31  -   41  -  11
    13  -   23  33  -   43  -  13
    16  -   26  36  -   46  -  16
    19  -   29  39  -   49  -  19
  }

  # Verified by PG-14
  do_execsql_test joinA-$id.204 {
    SELECT a,b,c,d,t2.e,f,t3.e
      FROM t1
           FULL JOIN t2 USING(c,d)
           FULL JOIN t3 USING(a,b,f)
           FULL JOIN t4 USING(a,c,d,f)
     WHERE t2.e!=0
    ORDER BY 1 nulls first, 3 nulls first;
  } {
    -   -   12  22  32  42  - 
    -   -   13  23  33  43  - 
    -   -   15  25  35  45  - 
    -   -   17  27  37  47  - 
  }
}
finish_test
