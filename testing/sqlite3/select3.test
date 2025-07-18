# 2001 September 15
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
# focus of this file is testing aggregate functions and the
# GROUP BY and HAVING clauses of SELECT statements.
#
# $Id: select3.test,v 1.23 2008/01/16 18:20:42 danielk1977 Exp $

set testdir [file dirname $argv0]
source $testdir/tester.tcl

# Build some test data
#
do_test select3-1.0 {
  execsql {
    CREATE TABLE t1(n int, log int);
    BEGIN;
  }
  for {set i 1} {$i<32} {incr i} {
    for {set j 0} {(1<<$j)<$i} {incr j} {}
    execsql "INSERT INTO t1 VALUES($i,$j)"
  }
  execsql {
    COMMIT
  }
  execsql {SELECT DISTINCT log FROM t1 ORDER BY log}
} {0 1 2 3 4 5}

# Basic aggregate functions.
#
do_test select3-1.1 {
  execsql {SELECT count(*) FROM t1}
} {31}
do_test select3-1.2 {
  execsql {
    SELECT min(n),min(log),max(n),max(log),sum(n),sum(log),avg(n),avg(log)
    FROM t1
  }
} {1 0 31 5 496 124 16.0 4.0}
do_test select3-1.3 {
  execsql {SELECT max(n)/avg(n), max(log)/avg(log) FROM t1}
} {1.9375 1.25}

# Try some basic GROUP BY clauses
#
do_test select3-2.1 {
  execsql {SELECT log, count(*) FROM t1 GROUP BY log ORDER BY log}
} {0 1 1 1 2 2 3 4 4 8 5 15}
do_test select3-2.2 {
  execsql {SELECT log, min(n) FROM t1 GROUP BY log ORDER BY log}
} {0 1 1 2 2 3 3 5 4 9 5 17}
do_test select3-2.3.1 {
  execsql {SELECT log, avg(n) FROM t1 GROUP BY log ORDER BY log}
} {0 1.0 1 2.0 2 3.5 3 6.5 4 12.5 5 24.0}
do_test select3-2.3.2 {
  execsql {SELECT log, avg(n)+1 FROM t1 GROUP BY log ORDER BY log}
} {0 2.0 1 3.0 2 4.5 3 7.5 4 13.5 5 25.0}
do_test select3-2.4 {
  execsql {SELECT log, avg(n)-min(n) FROM t1 GROUP BY log ORDER BY log}
} {0 0.0 1 0.0 2 0.5 3 1.5 4 3.5 5 7.0}
do_test select3-2.5 {
  execsql {SELECT log*2+1, avg(n)-min(n) FROM t1 GROUP BY log ORDER BY log}
} {1 0.0 3 0.0 5 0.5 7 1.5 9 3.5 11 7.0}
do_test select3-2.6 {
  execsql {
    SELECT log*2+1 as x, count(*) FROM t1 GROUP BY x ORDER BY x
  }
} {1 1 3 1 5 2 7 4 9 8 11 15}
do_test select3-2.7 {
  execsql {
    SELECT log*2+1 AS x, count(*) AS y FROM t1 GROUP BY x ORDER BY y, x
  }
} {1 1 3 1 5 2 7 4 9 8 11 15}
do_test select3-2.8 {
  execsql {
    SELECT log*2+1 AS x, count(*) AS y FROM t1 GROUP BY x ORDER BY 10-(x+y)
  }
} {11 15 9 8 7 4 5 2 3 1 1 1}
#do_test select3-2.9 {
#  catchsql {
#    SELECT log, count(*) FROM t1 GROUP BY 'x' ORDER BY log;
#  }
#} {1 {GROUP BY terms must not be non-integer constants}}
do_test select3-2.10 {
  catchsql {
    SELECT log, count(*) FROM t1 GROUP BY 0 ORDER BY log;
  }
} {1 {1st GROUP BY term out of range - should be between 1 and 2}}
do_test select3-2.11 {
  catchsql {
    SELECT log, count(*) FROM t1 GROUP BY 3 ORDER BY log;
  }
} {1 {1st GROUP BY term out of range - should be between 1 and 2}}
do_test select3-2.12 {
  catchsql {
    SELECT log, count(*) FROM t1 GROUP BY 1 ORDER BY log;
  }
} {0 {0 1 1 1 2 2 3 4 4 8 5 15}}

# Cannot have an empty GROUP BY
do_test select3-2.13 {
  catchsql {
    SELECT log, count(*) FROM t1 GROUP BY ORDER BY log;
  }
} {1 {near "ORDER": syntax error}}
do_test select3-2.14 {
  catchsql {
    SELECT log, count(*) FROM t1 GROUP BY;
  }
} {1 {near ";": syntax error}}

# Cannot have a HAVING without a GROUP BY
# 
# Update: As of 3.39.0, you can.
#
do_execsql_test select3-3.1 {
  SELECT log, count(*) FROM t1 HAVING log>=4
} {}
do_execsql_test select3-3.2 {
  SELECT count(*) FROM t1 HAVING log>=4
} {}
do_execsql_test select3-3.3 {
  SELECT count(*) FROM t1 HAVING log!=400
} {31}

# Toss in some HAVING clauses
#
do_test select3-4.1 {
  execsql {SELECT log, count(*) FROM t1 GROUP BY log HAVING log>=4 ORDER BY log}
} {4 8 5 15}
do_test select3-4.2 {
  execsql {
    SELECT log, count(*) FROM t1 
    GROUP BY log 
    HAVING count(*)>=4 
    ORDER BY log
  }
} {3 4 4 8 5 15}
do_test select3-4.3 {
  execsql {
    SELECT log, count(*) FROM t1 
    GROUP BY log 
    HAVING count(*)>=4 
    ORDER BY max(n)+0
  }
} {3 4 4 8 5 15}
do_test select3-4.4 {
  execsql {
    SELECT log AS x, count(*) AS y FROM t1 
    GROUP BY x
    HAVING y>=4 
    ORDER BY max(n)+0
  }
} {3 4 4 8 5 15}
do_test select3-4.5 {
  execsql {
    SELECT log AS x FROM t1 
    GROUP BY x
    HAVING count(*)>=4 
    ORDER BY max(n)+0
  }
} {3 4 5}

do_test select3-5.1 {
  execsql {
    SELECT log, count(*), avg(n), max(n+log*2) FROM t1 
    GROUP BY log 
    ORDER BY max(n+log*2)+0, avg(n)+0
  }
} {0 1 1.0 1 1 1 2.0 4 2 2 3.5 8 3 4 6.5 14 4 8 12.5 24 5 15 24.0 41}
do_test select3-5.2 {
  execsql {
    SELECT log, count(*), avg(n), max(n+log*2) FROM t1 
    GROUP BY log 
    ORDER BY max(n+log*2)+0, min(log,avg(n))+0
  }
} {0 1 1.0 1 1 1 2.0 4 2 2 3.5 8 3 4 6.5 14 4 8 12.5 24 5 15 24.0 41}

# Test sorting of GROUP BY results in the presence of an index
# on the GROUP BY column.
#
do_test select3-6.1 {
  execsql {
    SELECT log, min(n) FROM t1 GROUP BY log ORDER BY log;
  }
} {0 1 1 2 2 3 3 5 4 9 5 17}
do_test select3-6.2 {
  execsql {
    SELECT log, min(n) FROM t1 GROUP BY log ORDER BY log DESC;
  }
} {5 17 4 9 3 5 2 3 1 2 0 1}
do_test select3-6.3 {
  execsql {
    SELECT log, min(n) FROM t1 GROUP BY log ORDER BY 1;
  }
} {0 1 1 2 2 3 3 5 4 9 5 17}
do_test select3-6.4 {
  execsql {
    SELECT log, min(n) FROM t1 GROUP BY log ORDER BY 1 DESC;
  }
} {5 17 4 9 3 5 2 3 1 2 0 1}
do_test select3-6.5 {
  execsql {
    CREATE INDEX i1 ON t1(log);
    SELECT log, min(n) FROM t1 GROUP BY log ORDER BY log;
  }
} {0 1 1 2 2 3 3 5 4 9 5 17}
do_test select3-6.6 {
  execsql {
    SELECT log, min(n) FROM t1 GROUP BY log ORDER BY log DESC;
  }
} {5 17 4 9 3 5 2 3 1 2 0 1}
do_test select3-6.7 {
  execsql {
    SELECT log, min(n) FROM t1 GROUP BY log ORDER BY 1;
  }
} {0 1 1 2 2 3 3 5 4 9 5 17}
do_test select3-6.8 {
  execsql {
    SELECT log, min(n) FROM t1 GROUP BY log ORDER BY 1 DESC;
  }
} {5 17 4 9 3 5 2 3 1 2 0 1}

# Sometimes an aggregate query can return no rows at all.
#
do_test select3-7.1 {
  execsql {
    CREATE TABLE t2(a,b);
    INSERT INTO t2 VALUES(1,2);
    SELECT a, sum(b) FROM t2 WHERE b=5 GROUP BY a;
  }
} {}
do_test select3-7.2 {
  execsql {
    SELECT a, sum(b) FROM t2 WHERE b=5;
  }
} {{} {}}

# If a table column is of type REAL but we are storing integer values
# in it, the values are stored as integers to take up less space.  The
# values are converted by to REAL as they are read out of the table.
# Make sure the GROUP BY clause does this conversion correctly.
# Ticket #2251.
#
do_test select3-8.1 {
  execsql {
    CREATE TABLE A (
      A1 DOUBLE,
      A2 VARCHAR COLLATE NOCASE,
      A3 DOUBLE
    );
    INSERT INTO A VALUES(39136,'ABC',1201900000);
    INSERT INTO A VALUES(39136,'ABC',1207000000);
    SELECT typeof(sum(a3)) FROM a;
  }
} {real}
do_test select3-8.2 {
  execsql {
    SELECT typeof(sum(a3)) FROM a GROUP BY a1;
  }
} {real}

# 2019-05-09 ticket https://sqlite.org/src/tktview/6c1d3febc00b22d457c7
#
unset -nocomplain x
foreach {id x} {
  100 127
  101 128
  102 -127
  103 -128
  104 -129
  110 32767
  111 32768
  112 -32767
  113 -32768
  114 -32769
  120 2147483647
  121 2147483648
  122 -2147483647
  123 -2147483648
  124 -2147483649
  130 140737488355327
  131 140737488355328
  132 -140737488355327
  133 -140737488355328
  134 -140737488355329
  140 9223372036854775807
  141 -9223372036854775807
  142 -9223372036854775808
  143 9223372036854775806
  144 9223372036854775805
  145 -9223372036854775806
  146 -9223372036854775805

} {
  set x [expr {$x+0}]
  do_execsql_test select3-8.$id {
     DROP TABLE IF EXISTS t1;
     CREATE TABLE t1 (c0, c1 REAL PRIMARY KEY);
     INSERT INTO t1(c0, c1) VALUES (0, $x), (0, 0);
     UPDATE t1 SET c0 = NULL;
     UPDATE OR REPLACE t1 SET c1 = 1;
     SELECT DISTINCT * FROM t1 WHERE (t1.c0 IS NULL);
     PRAGMA integrity_check;
  } {{} 1.0 ok}
}

# 2020-03-10 ticket e0c2ad1aa8a9c691
reset_db
do_execsql_test select3-9.100 {
  CREATE TABLE t0(c0 REAL, c1 REAL GENERATED ALWAYS AS (c0));
  INSERT INTO t0(c0) VALUES (1);
  SELECT * FROM t0 GROUP BY c0;
} {1.0 1.0}

reset_db
do_execsql_test select3.10.100 {
  CREATE TABLE t1(a, b);
  CREATE TABLE t2(c, d);
  SELECT max(t1.a), 
         (SELECT 'xyz' FROM (SELECT * FROM t2 WHERE 0) WHERE t1.b=1) 
  FROM t1;
} {{} {}}

#-------------------------------------------------------------------------
# dbsqlfuzz crash-8e17857db2c5a9294c975123ac807156a6559f13.txt
# Associated with the flatten-left-join branch circa 2022-06-23.
#
foreach {tn sql} {
  1 {
    CREATE TABLE t1(a TEXT);
    CREATE TABLE t2(x INT);
    CREATE INDEX t2x ON t2(x);
    INSERT INTO t1 VALUES('abc');
  }
  2 {
    CREATE TABLE t1(a TEXT);
    CREATE TABLE t2(x INT);
    INSERT INTO t1 VALUES('abc');
  }
  3 {
    CREATE TABLE t1(a TEXT);
    CREATE TABLE t2(x INT);
    INSERT INTO t1 VALUES('abc');
    PRAGMA automatic_index=OFF;
  }
} {
  reset_db
  do_execsql_test select3-11.$tn.1 $sql 
  do_execsql_test select3.11.$tn.2 {
    SELECT max(a), val FROM t1 LEFT JOIN (
        SELECT 'constant' AS val FROM t2 WHERE x=1234
    )
  } {abc {}}
  do_execsql_test select3.11.$tn.3 {
    INSERT INTO t2 VALUES(123);
    SELECT max(a), val FROM t1 LEFT JOIN (
        SELECT 'constant' AS val FROM t2 WHERE x=1234
    )
  } {abc {}}
  do_execsql_test select3.11.$tn.4 {
    INSERT INTO t2 VALUES(1234);
    SELECT max(a), val FROM t1 LEFT JOIN (
        SELECT 'constant' AS val FROM t2 WHERE x=1234
    )
  } {abc constant}
}

reset_db
do_execsql_test 12.0 {
  CREATE TABLE t1(a);
  CREATE TABLE t2(x);
}
do_execsql_test 12.1 {
  SELECT count(x), m FROM t1 LEFT JOIN (SELECT x, 59 AS m FROM t2) GROUP BY a;
}
do_execsql_test 12.2 {
  INSERT INTO t1 VALUES(1), (1), (2), (3);
  SELECT count(x), m FROM t1 LEFT JOIN (SELECT x, 59 AS m FROM t2) GROUP BY a;
} {
  0 {}
  0 {}
  0 {}
}
do_execsql_test 12.3 {
  INSERT INTO t2 VALUES(45);
  SELECT count(x), m FROM t1 LEFT JOIN (SELECT x, 59 AS m FROM t2) GROUP BY a;
} {
  2 59
  1 59
  1 59
}
do_execsql_test 12.4 {
  INSERT INTO t2 VALUES(210);
  SELECT count(x), m FROM t1 LEFT JOIN (SELECT x, 59 AS m FROM t2) GROUP BY a;
} {
  4 59
  2 59
  2 59
}
do_execsql_test 12.5 {
  INSERT INTO t2 VALUES(NULL);
  SELECT count(x), m FROM t1 LEFT JOIN (SELECT x, 59 AS m FROM t2) GROUP BY a;
} {
  4 59
  2 59
  2 59
}
do_execsql_test 12.6 {
  DELETE FROM t2;
  DELETE FROM t1;
  INSERT INTO t1 VALUES('value');
  INSERT INTO t2 VALUES('hello');
} {}
do_execsql_test 12.7 {
  SELECT group_concat(x), m FROM t1 
    LEFT JOIN (SELECT x, 59 AS m FROM t2) GROUP BY a;
} {
  hello 59
}
do_execsql_test 12.8 {
  SELECT group_concat(x), m, n FROM t1 
    LEFT JOIN (SELECT x, 59 AS m, 60 AS n FROM t2) GROUP BY a;
} {
  hello 59 60
}

finish_test
