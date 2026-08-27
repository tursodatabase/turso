# 2021 February 16
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#*************************************************************************
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix alterdropcol

# If SQLITE_OMIT_ALTERTABLE is defined, omit this file.
ifcapable !altertable {
  finish_test
  return
}

do_execsql_test 1.0 {
  CREATE TABLE t1(a, b, c);
  CREATE VIEW v1 AS SELECT * FROM t1;

  CREATE TABLE t2(x INTEGER PRIMARY KEY, y, z UNIQUE);
  CREATE INDEX t2y ON t2(y);

  CREATE TABLE t3(q, r, s);
  CREATE INDEX t3rs ON t3(r+s);
}

do_catchsql_test 1.1 {
  ALTER TABLE nosuch DROP COLUMN z;
} {1 {no such table: nosuch}}

do_catchsql_test 1.2 {
  ALTER TABLE v1 DROP COLUMN c;
} {1 {cannot drop column from view "v1"}}

ifcapable fts5 {
  do_execsql_test 1.3.1 {
    CREATE VIRTUAL TABLE ft1 USING fts5(one, two);
  }
  do_catchsql_test 1.3.2 {
    ALTER TABLE ft1 DROP COLUMN two;
  } {1 {cannot drop column from virtual table "ft1"}}
}

do_catchsql_test 1.4 {
  ALTER TABLE sqlite_schema DROP COLUMN sql;
} {1 {table sqlite_master may not be altered}}

do_catchsql_test 1.5 {
  ALTER TABLE t1 DROP COLUMN d;
} {1 {no such column: "d"}}

do_execsql_test 1.6.1 {
  ALTER TABLE t1 DROP COLUMN b;
}
do_execsql_test 1.6.2 {
  SELECT sql FROM sqlite_schema WHERE name = 't1'
} {{CREATE TABLE t1(a, c)}}

do_execsql_test 1.7.1 {
  ALTER TABLE t1 DROP COLUMN c;
}
do_execsql_test 1.7.2 {
  SELECT sql FROM sqlite_schema WHERE name = 't1'
} {{CREATE TABLE t1(a)}}

do_catchsql_test 1.7.3 {
  ALTER TABLE t1 DROP COLUMN a;
} {1 {cannot drop column "a": no other columns exist}}


do_catchsql_test 1.8 {
  ALTER TABLE t2 DROP COLUMN z
} {1 {cannot drop UNIQUE column: "z"}}

do_catchsql_test 1.9 {
  ALTER TABLE t2 DROP COLUMN x
} {1 {cannot drop PRIMARY KEY column: "x"}}

do_catchsql_test 1.10 {
  ALTER TABLE t2 DROP COLUMN y
} {1 {error in index t2y after drop column: no such column: y}}

do_catchsql_test 1.11 {
  ALTER TABLE t3 DROP COLUMN s
} {1 {error in index t3rs after drop column: no such column: s}}

#-------------------------------------------------------------------------

foreach {tn wo} {
  1 {}
  2 {WITHOUT ROWID}
} { eval [string map [list %TN% $tn %WO% $wo] {

  reset_db
  do_execsql_test 2.%TN%.0 {
    CREATE TABLE t1(x, y INTEGER PRIMARY KEY, z) %WO% ;
    INSERT INTO t1 VALUES(1, 2, 3);
    INSERT INTO t1 VALUES(4, 5, 6);
    INSERT INTO t1 VALUES(7, 8, 9);
  }
  
  do_execsql_test 2.%TN%.1 {
    ALTER TABLE t1 DROP COLUMN x;
    SELECT * FROM t1;
  } {
    2 3  5 6  8 9
  }
  do_execsql_test 2.%TN%.2 {
    ALTER TABLE t1 DROP COLUMN z;
    SELECT * FROM t1;
  } {
    2 5 8
  }
}]}

#-------------------------------------------------------------------------
reset_db

do_execsql_test 3.0 {
  CREATE TABLE t12(a, b, c, CHECK(c>10));
  CREATE TABLE t13(a, b, c CHECK(c>10));
}
do_catchsql_test 3.1 {
  ALTER TABLE t12 DROP COLUMN c;
} {1 {error in table t12 after drop column: no such column: c}}

do_catchsql_test 3.2 {
  ALTER TABLE t13 DROP COLUMN c;
} {0 {}}

#-------------------------------------------------------------------------
# Test that generated columns can be dropped. And that other columns from
# tables that contain generated columns can be dropped.
#
foreach {tn wo vs} {
  1 ""              ""
  2 ""              VIRTUAL
  3 ""              STORED
  4 "WITHOUT ROWID" STORED
  5 "WITHOUT ROWID" VIRTUAL
} {
  reset_db

  do_execsql_test 4.$tn.0 "
    CREATE TABLE 'my table'(a, b PRIMARY KEY, c AS (a+b) $vs, d) $wo
  "
  do_execsql_test 4.$tn.1 {
    INSERT INTO "my table"(a, b, d) VALUES(1, 2, 'hello');
    INSERT INTO "my table"(a, b, d) VALUES(3, 4, 'world');

    SELECT * FROM "my table"
  } {
    1 2 3 hello
    3 4 7 world
  }

  do_execsql_test 4.$tn.2 {
    ALTER TABLE "my table" DROP COLUMN c;
  }
  do_execsql_test 4.$tn.3 {
    SELECT * FROM "my table"
  } {
    1 2 hello
    3 4 world
  }
  
  do_execsql_test 4.$tn.4 "
    CREATE TABLE x1(a, b, c PRIMARY KEY, d AS (b+c) $vs, e) $wo
  "
  do_execsql_test 4.$tn.5 {
    INSERT INTO x1(a, b, c, e) VALUES(1, 2, 3, 4);
    INSERT INTO x1(a, b, c, e) VALUES(5, 6, 7, 8);
    INSERT INTO x1(a, b, c, e) VALUES(9, 10, 11, 12);
    SELECT * FROM x1;
  } {
    1 2 3 5 4
    5 6 7 13 8
    9 10 11 21 12
  }

  do_execsql_test 4.$tn.6 {
    ALTER TABLE x1 DROP COLUMN a
  }
  do_execsql_test 4.$tn.7 {
    SELECT * FROM x1
  } {
    2 3 5 4
    6 7 13 8
    10 11 21 12
  }
  do_execsql_test 4.$tn.8 {
    ALTER TABLE x1 DROP COLUMN e
  }
  do_execsql_test 4.$tn.9 {
    SELECT * FROM x1
  } {
    2 3 5
    6 7 13
    10 11 21
  }
}

#-------------------------------------------------------------------------
reset_db
do_execsql_test 5.0 {
  CREATE TABLE p1(a PRIMARY KEY, b UNIQUE);
  CREATE TABLE c1(x, y, z REFERENCES p1(c));
  CREATE TABLE c2(x, y, z, w REFERENCES p1(b));
}
do_execsql_test 5.1 {
  ALTER TABLE c1 DROP COLUMN z;
  ALTER TABLE c2 DROP COLUMN z;
  SELECT sql FROM sqlite_schema WHERE name IN ('c1', 'c2');
} {
  {CREATE TABLE c1(x, y)} 
  {CREATE TABLE c2(x, y, w REFERENCES p1(b))}
}

do_execsql_test 5.2.1 {
  CREATE VIEW v1 AS SELECT d, e FROM p1
}
do_catchsql_test 5.2.2 {
  ALTER TABLE c1 DROP COLUMN x
} {1 {error in view v1: no such column: d}}
do_execsql_test 5.3.1 {
  DROP VIEW v1;
  CREATE VIEW v1 AS SELECT x, y FROM c1;
}
do_catchsql_test 5.3.2 {
  ALTER TABLE c1 DROP COLUMN x
} {1 {error in view v1 after drop column: no such column: x}}

do_execsql_test 5.4.1 {
  CREATE TRIGGER tr AFTER INSERT ON c1 BEGIN
    INSERT INTO p1 VALUES(new.y, new.xyz);
  END;
}
do_catchsql_test 5.4.2 {
  ALTER TABLE c1 DROP COLUMN y
} {1 {error in trigger tr: no such column: new.xyz}}
do_execsql_test 5.5.1 {
  DROP TRIGGER tr;
  CREATE TRIGGER tr AFTER INSERT ON c1 BEGIN
    INSERT INTO p1 VALUES(new.y, new.z);
  END;
}
do_catchsql_test 5.5.2 {
  ALTER TABLE c1 DROP COLUMN y
} {1 {error in trigger tr: no such column: new.z}}

# 2021-03-06 dbsqlfuzz crash-419aa525df93db6e463772c686ac6da27b46da9e
reset_db
do_catchsql_test 6.0 {
  CREATE TABLE t1(a,b,c);
  CREATE TABLE t2(x,y,z);
  PRAGMA writable_schema=ON;
  UPDATE sqlite_schema SET sql='CREATE INDEX t1b ON t1(b)' WHERE name='t2';
  PRAGMA writable_schema=OFF;
  ALTER TABLE t2 DROP COLUMN z;
} {1 {database disk image is malformed}}
reset_db
do_catchsql_test 6.1 {
  CREATE TABLE t1(a,b,c);
  CREATE TABLE t2(x,y,z);
  PRAGMA writable_schema=ON;
  UPDATE sqlite_schema SET sql='CREATE VIEW t2(x,y,z) AS SELECT b,a,c FROM t1'
   WHERE name='t2';
  PRAGMA writable_schema=OFF;
  ALTER TABLE t2 DROP COLUMN z;
} {1 {database disk image is malformed}}

# 2021-04-06 dbsqlfuzz crash-331c5c29bb76257b198f1318eef3288f9624c8ce
reset_db
do_execsql_test 7.0 {
  CREATE TABLE t1(a, b, c, PRIMARY KEY(a COLLATE nocase, a)) WITHOUT ROWID;
  INSERT INTO t1 VALUES(1, 2, 3);
  INSERT INTO t1 VALUES(4, 5, 6);
}
do_execsql_test 7.1 {
  ALTER TABLE t1 DROP COLUMN c;                
}
do_execsql_test 7.2 {
  SELECT sql FROM sqlite_schema;
} {{CREATE TABLE t1(a, b, PRIMARY KEY(a COLLATE nocase, a)) WITHOUT ROWID}}
do_execsql_test 7.3 {
  SELECT * FROM t1;
} {1 2 4 5}

reset_db
do_execsql_test 8.0 {
  CREATE TABLE t1(a INTEGER PRIMARY KEY, b);
  PRAGMA writable_schema = 1;
  UPDATE sqlite_schema 
  SET sql = 'CREATE TABLE t1(a INTEGER PRIMARY KEY AUTOINCREMENT, b)'
}
db close
sqlite3 db test.db
do_execsql_test 8.1 {
  ALTER TABLE t1 DROP COLUMN b;                
}
do_execsql_test 8.2 {
  SELECT sql FROM sqlite_schema;
} {{CREATE TABLE t1(a INTEGER PRIMARY KEY AUTOINCREMENT)}}

#-------------------------------------------------------------------------

foreach {tn wo} {
  1 {}
  2 {WITHOUT ROWID}
} {
  reset_db
  do_execsql_test 9.$tn.0 "
    CREATE TABLE t1(a INTEGER PRIMARY KEY, b, c) $wo;
  "
  do_execsql_test 9.$tn.1 {
    WITH s(i) AS (
        SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i<50000
    )
    INSERT INTO t1(a, b, c) SELECT i, 123, 456 FROM s;
  }
  do_execsql_test 9.$tn.2 {
    ALTER TABLE t1 DROP COLUMN b;
  }

  do_execsql_test 9.$tn.3 {
    SELECT count(*), c FROM t1 GROUP BY c;
  } {50000 456}
}



finish_test
