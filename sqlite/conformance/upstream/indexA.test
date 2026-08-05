# 2023 September 23
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
set testprefix indexA

do_execsql_test 1.0 {
  CREATE TABLE t1(a TEXT, b, c);
  CREATE INDEX i1 ON t1(b, c) WHERE a='abc';
  INSERT INTO t1 VALUES('abc', 1, 2);
}

do_execsql_test 1.1 {
  SELECT * FROM t1 WHERE a='abc'
} {abc 1 2}

do_eqp_test 1.2 {
  SELECT * FROM t1 WHERE a='abc'
} {USING COVERING INDEX i1}

do_execsql_test 1.3 {
  CREATE INDEX i2 ON t1(b, c) WHERE a=5;
  INSERT INTO t1 VALUES(5, 4, 3);

  SELECT a, typeof(a), b, c FROM t1 WHERE a=5;
} {5 text 4 3}

do_execsql_test 1.4 {
  CREATE TABLE t2(x);
  INSERT INTO t2 VALUES('v');
}

do_execsql_test 1.5 {
  SELECT x, a, b, c FROM t2 LEFT JOIN t1 ON (a=5 AND b=x)
} {v {} {} {}}

do_execsql_test 1.6 {
  SELECT x, a, b, c FROM t2 RIGHT JOIN t1 ON (t1.a=5 AND t1.b=t2.x)
} {{} abc 1 2   {} 5 4 3}

do_eqp_test 1.7 {
  SELECT x, a, b, c FROM t2 RIGHT JOIN t1 ON (t1.a=5 AND t1.b=t2.x)
} {USING INDEX i2}

#-------------------------------------------------------------------------
reset_db

do_execsql_test 2.0 {
  CREATE TABLE x1(a TEXT, b, c);
  INSERT INTO x1 VALUES('2', 'two', 'ii');
  INSERT INTO x1 VALUES('2.0', 'twopointoh', 'ii.0');

  CREATE TABLE x2(a NUMERIC, b, c);
  INSERT INTO x2 VALUES('2', 'two', 'ii');
  INSERT INTO x2 VALUES('2.0', 'twopointoh', 'ii.0');

  CREATE TABLE x3(a REAL, b, c);
  INSERT INTO x3 VALUES('2', 'two', 'ii');
  INSERT INTO x3 VALUES('2.0', 'twopointoh', 'ii.0');
}

foreach {tn idx} {
  0 {
  }
  1 {
    CREATE INDEX i1 ON x1(b, c) WHERE a=2;
    CREATE INDEX i2 ON x2(b, c) WHERE a=2;
    CREATE INDEX i3 ON x3(b, c) WHERE a=2;
  }
  2 {
    CREATE INDEX i1 ON x1(b, c) WHERE a=2.0;
    CREATE INDEX i2 ON x2(b, c) WHERE a=2.0;
    CREATE INDEX i3 ON x3(b, c) WHERE a=2.0;
  }
  3 {
    CREATE INDEX i1 ON x1(b, c) WHERE a='2.0';
    CREATE INDEX i2 ON x2(b, c) WHERE a='2.0';
    CREATE INDEX i3 ON x3(b, c) WHERE a='2.0';
  }
  4 {
    CREATE INDEX i1 ON x1(b, c) WHERE a='2';
    CREATE INDEX i2 ON x2(b, c) WHERE a='2';
    CREATE INDEX i3 ON x3(b, c) WHERE a='2';
  }
} {
  execsql { DROP INDEX IF EXISTS i1 }
  execsql { DROP INDEX IF EXISTS i2 }
  execsql { DROP INDEX IF EXISTS i3 }

  execsql $idx
  do_execsql_test 2.1.$tn.1 {
    SELECT *, typeof(a) FROM x1 WHERE a=2
  } {2 two ii text}
  do_execsql_test 2.1.$tn.2 {
    SELECT *, typeof(a) FROM x1 WHERE a=2.0
  } {2.0 twopointoh ii.0 text}
  do_execsql_test 2.1.$tn.3 {
    SELECT *, typeof(a) FROM x1 WHERE a='2'
  } {2 two ii text}
  do_execsql_test 2.1.$tn.4 {
    SELECT *, typeof(a) FROM x1 WHERE a='2.0'
  } {2.0 twopointoh ii.0 text}

  do_execsql_test 2.1.$tn.5 {
    SELECT *, typeof(a) FROM x2 WHERE a=2
  } {2 two ii integer 2 twopointoh ii.0 integer}
  do_execsql_test 2.1.$tn.6 {
    SELECT *, typeof(a) FROM x2 WHERE a=2.0
  } {2 two ii integer 2 twopointoh ii.0 integer}
  do_execsql_test 2.1.$tn.7 {
    SELECT *, typeof(a) FROM x2 WHERE a='2'
  } {2 two ii integer 2 twopointoh ii.0 integer}
  do_execsql_test 2.1.$tn.8 {
    SELECT *, typeof(a) FROM x2 WHERE a='2.0'
  } {2 two ii integer 2 twopointoh ii.0 integer}

  do_execsql_test 2.1.$tn.9 {
    SELECT *, typeof(a) FROM x3 WHERE a=2
  } {2.0 two ii real 2.0 twopointoh ii.0 real}
  do_execsql_test 2.1.$tn.10 {
    SELECT *, typeof(a) FROM x3 WHERE a=2.0
  } {2.0 two ii real 2.0 twopointoh ii.0 real}
  do_execsql_test 2.1.$tn.11 {
    SELECT *, typeof(a) FROM x3 WHERE a='2'
  } {2.0 two ii real 2.0 twopointoh ii.0 real}
  do_execsql_test 2.1.$tn.12 {
    SELECT *, typeof(a) FROM x3 WHERE a='2.0'
  } {2.0 two ii real 2.0 twopointoh ii.0 real}

}

reset_db
do_execsql_test 3.0 {
  CREATE TABLE x1(a TEXT, d PRIMARY KEY, b, c) WITHOUT ROWID;
  INSERT INTO x1 VALUES('2', 1, 'two', 'ii');
  INSERT INTO x1 VALUES('2.0', 2, 'twopointoh', 'ii.0');

  CREATE TABLE x2(a NUMERIC, b, c, d PRIMARY KEY) WITHOUT ROWID;
  INSERT INTO x2 VALUES('2', 'two', 'ii', 1);
  INSERT INTO x2 VALUES('2.0', 'twopointoh', 'ii.0', 2);

  CREATE TABLE x3(d PRIMARY KEY, a REAL, b, c) WITHOUT ROWID;
  INSERT INTO x3 VALUES(34, '2', 'two', 'ii');
  INSERT INTO x3 VALUES(35, '2.0', 'twopointoh', 'ii.0');
}

foreach {tn idx} {
  0 {
  }
  1 {
    CREATE INDEX i1 ON x1(b, c) WHERE a=2;
    CREATE INDEX i2 ON x2(b, c) WHERE a=2;
    CREATE INDEX i3 ON x3(b, c) WHERE a=2;
  }
  2 {
    CREATE INDEX i1 ON x1(b, c) WHERE a=2.0;
    CREATE INDEX i2 ON x2(b, c) WHERE a=2.0;
    CREATE INDEX i3 ON x3(b, c) WHERE a=2.0;
  }
  3 {
    CREATE INDEX i1 ON x1(b, c) WHERE a='2.0';
    CREATE INDEX i2 ON x2(b, c) WHERE a='2.0';
    CREATE INDEX i3 ON x3(b, c) WHERE a='2.0';
  }
  4 {
    CREATE INDEX i1 ON x1(b, c) WHERE a='2';
    CREATE INDEX i2 ON x2(b, c) WHERE a='2';
    CREATE INDEX i3 ON x3(b, c) WHERE a='2';
  }
} {
  execsql { DROP INDEX IF EXISTS i1 }
  execsql { DROP INDEX IF EXISTS i2 }
  execsql { DROP INDEX IF EXISTS i3 }

  execsql $idx
  do_execsql_test 3.1.$tn.1 {
    SELECT a, b, c, typeof(a) FROM x1 WHERE a=2
  } {2 two ii text}
  do_execsql_test 3.1.$tn.2 {
    SELECT a, b, c, typeof(a) FROM x1 WHERE a=2.0
  } {2.0 twopointoh ii.0 text}
  do_execsql_test 3.1.$tn.3 {
    SELECT a, b, c, typeof(a) FROM x1 WHERE a='2'
  } {2 two ii text}
  do_execsql_test 3.1.$tn.4 {
    SELECT a, b, c, typeof(a) FROM x1 WHERE a='2.0'
  } {2.0 twopointoh ii.0 text}

  do_execsql_test 3.1.$tn.5 {
    SELECT a, b, c, typeof(a) FROM x2 WHERE a=2
  } {2 two ii integer 2 twopointoh ii.0 integer}
  do_execsql_test 3.1.$tn.6 {
    SELECT a, b, c, typeof(a) FROM x2 WHERE a=2.0
  } {2 two ii integer 2 twopointoh ii.0 integer}
  do_execsql_test 3.1.$tn.7 {
    SELECT a, b, c, typeof(a) FROM x2 WHERE a='2'
  } {2 two ii integer 2 twopointoh ii.0 integer}
  do_execsql_test 3.1.$tn.8 {
    SELECT a, b, c, typeof(a) FROM x2 WHERE a='2.0'
  } {2 two ii integer 2 twopointoh ii.0 integer}

  do_execsql_test 3.1.$tn.9 {
    SELECT a, b, c, typeof(a) FROM x3 WHERE a=2
  } {2.0 two ii real 2.0 twopointoh ii.0 real}
  do_execsql_test 3.1.$tn.10 {
    SELECT a, b, c, typeof(a) FROM x3 WHERE a=2.0
  } {2.0 two ii real 2.0 twopointoh ii.0 real}
  do_execsql_test 3.1.$tn.11 {
    SELECT a, b, c, typeof(a) FROM x3 WHERE a='2'
  } {2.0 two ii real 2.0 twopointoh ii.0 real}
  do_execsql_test 3.1.$tn.12 {
    SELECT a, b, c, typeof(a) FROM x3 WHERE a='2.0'
  } {2.0 two ii real 2.0 twopointoh ii.0 real}
}

#-------------------------------------------------------------------------
reset_db
do_execsql_test 4.0 {
  CREATE TABLE t2(a INTEGER, b TEXT);
  INSERT INTO t2 VALUES(1, 'two');
  INSERT INTO t2 VALUES(2, 'two');
  INSERT INTO t2 VALUES(3, 'two');
  INSERT INTO t2 VALUES(1, 'three');
  INSERT INTO t2 VALUES(2, 'three');
  INSERT INTO t2 VALUES(3, 'three');

  CREATE INDEX t2a_two ON t2(a) WHERE b='two';
}

# explain_i { SELECT sum(a), b FROM t2 WHERE b='two' }
do_execsql_test 4.1.1 {
  SELECT sum(a), b FROM t2 WHERE b='two'
} {6 two}
do_eqp_test 4.1.2 {
  SELECT sum(a), b FROM t2 WHERE b='two'
} {USING COVERING INDEX t2a_two}

#-------------------------------------------------------------------------
reset_db
do_execsql_test 5.0 {
  CREATE TABLE t1(a INTEGER PRIMQRY KEY, b, c);
}
do_catchsql_test 5.1 {
  CREATE INDEX ex1 ON t1(c) WHERE b IS 'abc' COLLATE g;
} {1 {no such collation sequence: g}}

proc xyz {lhs rhs} {
  return [string compare $lhs $rhs]
}
db collate xyz xyz
do_execsql_test 5.2 {
  CREATE INDEX ex1 ON t1(c) WHERE b IS 'abc' COLLATE xyz;
}
db close
sqlite3 db test.db
do_execsql_test 5.3 {
  SELECT * FROM t1
}

#-------------------------------------------------------------------------
reset_db
do_execsql_test 6.0 {
  CREATE TABLE t1(a INTEGER PRIMARY KEY, b, c);
  CREATE TABLE t2(x INTEGER PRIMARY KEY, y INTEGER, z INTEGER);
  INSERT INTO t1 VALUES(1, 1, 1);
  INSERT INTO t1 VALUES(2, 1, 2);
  INSERT INTO t2 VALUES(1, 5, 1);
  INSERT INTO t2 VALUES(2, 5, 2);

  CREATE INDEX t2z ON t2(z) WHERE y=5;
}

do_execsql_test 6.1 {
  ANALYZE;
  UPDATE sqlite_stat1 SET stat = '50 1' WHERE idx='t2z';
  UPDATE sqlite_stat1 SET stat = '50' WHERE tbl='t2' AND idx IS NULL;
  UPDATE sqlite_stat1 SET stat = '5000' WHERE tbl='t1' AND idx IS NULL;
  ANALYZE sqlite_schema;
}

do_execsql_test 6.2 {
  SELECT * FROM t1, t2 WHERE b=1 AND z=c AND y=5;
} {
  1 1 1  1 5 1
  2 1 2  2 5 2
}

do_eqp_test 6.3 {
  SELECT * FROM t1, t2 WHERE b=1 AND z=c AND y=5;
} {BLOOM FILTER ON t2}

do_execsql_test 6.4 {
  SELECT * FROM t1 LEFT JOIN t2 ON (y=5) WHERE b=1 AND z IS c;
} {
  1 1 1  1 5 1
  2 1 2  2 5 2
}

do_eqp_test 6.5 {
  SELECT * FROM t1 LEFT JOIN t2 ON (y=5) WHERE b=1 AND z IS c;
} {BLOOM FILTER ON t2}

do_execsql_test 6.6 {
  CREATE INDEX t2yz ON t2(y, z) WHERE y=5;
}

do_execsql_test 6.7 {
  SELECT * FROM t1 LEFT JOIN t2 ON (y=5) WHERE b=1 AND z IS c;
} {
  1 1 1  1 5 1
  2 1 2  2 5 2
}

#-------------------------------------------------------------------------
reset_db
do_execsql_test 7.0 {
  CREATE TABLE t1(i INTEGER PRIMARY KEY, b TEXT, c TEXT);
  CREATE INDEX i1 ON t1(c) WHERE b='abc' AND i=5;
  INSERT INTO t1 VALUES(5, 'abc', 'xyz');
  SELECT * FROM t1 INDEXED BY i1 WHERE b='abc' AND i=5 ORDER BY c;
} {5 abc xyz}

#-------------------------------------------------------------------------
reset_db
do_execsql_test 8.0 {
  CREATE TABLE t1(a, b, c);
  CREATE INDEX ex2 ON t1(a, 4);
  CREATE INDEX ex1 ON t1(a) WHERE 4=b;
  INSERT INTO t1 VALUES(1, 4, 1);
  INSERT INTO t1 VALUES(1, 5, 1);
  INSERT INTO t1 VALUES(2, 4, 2);
}
do_execsql_test 8.1 {
  SELECT * FROM t1 WHERE b=4;
} {
  1 4 1  2 4 2
}

finish_test
