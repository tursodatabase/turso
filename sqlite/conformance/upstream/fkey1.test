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
# This file implements regression tests for SQLite library.
#
# This file implements tests for foreign keys.
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix fkey1

ifcapable {!foreignkey} {
  finish_test
  return
}

# Create a table and some data to work with.
#
do_test fkey1-1.0 {
  execsql {
    CREATE TABLE t1(
      a INTEGER PRIMARY KEY,
      b INTEGER
           REFERENCES t1 ON DELETE CASCADE
           REFERENCES t2,
      c TEXT,
      FOREIGN KEY (b,c) REFERENCES t2(x,y) ON UPDATE CASCADE
    );
  }
} {}
do_test fkey1-1.1 {
  execsql {
    CREATE TABLE t2(
      x INTEGER PRIMARY KEY,
      y TEXT
    );
  }
} {}
do_test fkey1-1.2 {
  execsql {
    CREATE TABLE t3(
      a INTEGER REFERENCES t2,
      b INTEGER REFERENCES t1,
      FOREIGN KEY (a,b) REFERENCES t2(x,y)
    );
  }
} {}

do_test fkey1-2.1 {
  execsql {
    CREATE TABLE t4(a integer primary key);
    CREATE TABLE t5(x references t4);
    CREATE TABLE t6(x references t4);
    CREATE TABLE t7(x references t4);
    CREATE TABLE t8(x references t4);
    CREATE TABLE t9(x references t4);
    CREATE TABLE t10(x references t4);
    DROP TABLE t7;
    DROP TABLE t9;
    DROP TABLE t5;
    DROP TABLE t8;
    DROP TABLE t6;
    DROP TABLE t10;
  }
} {}

do_test fkey1-3.1 {
  execsql {
    CREATE TABLE t5(a PRIMARY KEY, b, c);
    CREATE TABLE t6(
      d REFERENCES t5,
      e REFERENCES t5(c)
    );
    PRAGMA foreign_key_list(t6);
  }
} [concat                                         \
  {0 0 t5 e c {NO ACTION} {NO ACTION} NONE}       \
  {1 0 t5 d {} {NO ACTION} {NO ACTION} NONE}      \
]
do_test fkey1-3.2 {
  execsql {
    CREATE TABLE t7(d, e, f,
      FOREIGN KEY (d, e) REFERENCES t5(a, b)
    );
    PRAGMA foreign_key_list(t7);
  }
} [concat                                   \
  {0 0 t5 d a {NO ACTION} {NO ACTION} NONE} \
  {0 1 t5 e b {NO ACTION} {NO ACTION} NONE} \
]
do_test fkey1-3.3 {
  execsql {
    CREATE TABLE t8(d, e, f,
      FOREIGN KEY (d, e) REFERENCES t5 ON DELETE CASCADE ON UPDATE SET NULL
    );
    PRAGMA foreign_key_list(t8);
  }
} [concat                        \
  {0 0 t5 d {} {SET NULL} CASCADE NONE} \
  {0 1 t5 e {} {SET NULL} CASCADE NONE} \
]
do_test fkey1-3.4 {
  execsql {
    CREATE TABLE t9(d, e, f,
      FOREIGN KEY (d, e) REFERENCES t5 ON DELETE CASCADE ON UPDATE SET DEFAULT
    );
    PRAGMA foreign_key_list(t9);
  }
} [concat                        \
  {0 0 t5 d {} {SET DEFAULT} CASCADE NONE} \
  {0 1 t5 e {} {SET DEFAULT} CASCADE NONE} \
]
do_test fkey1-3.5 {
  sqlite3_db_status db DBSTATUS_DEFERRED_FKS 0
} {0 0 0}

# Stress the dequoting logic.  The first test is not so bad.
do_execsql_test fkey1-4.0 {
  PRAGMA foreign_keys=ON;
  CREATE TABLE "xx1"("xx2" TEXT PRIMARY KEY, "xx3" TEXT);
  INSERT INTO "xx1"("xx2","xx3") VALUES('abc','def');
  CREATE TABLE "xx4"("xx5" TEXT REFERENCES "xx1" ON DELETE CASCADE);
  INSERT INTO "xx4"("xx5") VALUES('abc');
  INSERT INTO "xx1"("xx2","xx3") VALUES('uvw','xyz');
  SELECT 1, "xx5" FROM "xx4";
  DELETE FROM "xx1";
  SELECT 2, "xx5" FROM "xx4";
} {1 abc}

# This case is identical to the previous except the "xx" in each name
# is changed to a single escaped double-quote character.
do_execsql_test fkey1-4.1 {
  PRAGMA foreign_keys=ON;
  CREATE TABLE """1"("""2" TEXT PRIMARY KEY, """3" TEXT);
  INSERT INTO """1"("""2","""3") VALUES('abc','def');
  CREATE TABLE """4"("""5" TEXT REFERENCES """1" ON DELETE CASCADE);
  INSERT INTO """4"("""5") VALUES('abc');
  INSERT INTO """1"("""2","""3") VALUES('uvw','xyz');
  SELECT 1, """5" FROM """4";
  DELETE FROM """1";
  SELECT 2, """5" FROM """4";
} {1 abc}
do_execsql_test fkey1-4.2 {
  PRAGMA table_info="""1";
} {0 {"2} TEXT 0 {} 1 1 {"3} TEXT 0 {} 0}

#-------------------------------------------------------------------------
#
do_execsql_test fkey1-5.1 {
  CREATE TABLE t11(
    x INTEGER PRIMARY KEY, 
    parent REFERENCES t11 ON DELETE CASCADE
  );
  INSERT INTO t11 VALUES (1, NULL), (2, 1), (3, 2);
} {}

# The REPLACE part of this statement deletes the row (2, 1). Then the 
# DELETE CASCADE caused by deleting that row removes the (3, 2) row. Which
# would have been the parent of the new row being inserted. Causing an
# FK violation.
#
do_catchsql_test fkey1-5.2 {
  INSERT OR REPLACE INTO t11 VALUES (2, 3);
} {1 {FOREIGN KEY constraint failed}}

# Make sure sqlite3_trace() output works with triggers used to implement
# FK constraints
#
ifcapable trace {
  proc sqltrace {txt} {
    global traceoutput
    lappend traceoutput $txt
  }
  do_test fkey1-5.2.1 {
    unset -nocomplain traceoutput
    db trace sqltrace
    catch {db eval {INSERT OR REPLACE INTO t11 VALUES(2,3);}}
    set traceoutput
  } {{INSERT OR REPLACE INTO t11 VALUES(2,3);} {INSERT OR REPLACE INTO t11 VALUES(2,3);} {INSERT OR REPLACE INTO t11 VALUES(2,3);}}
}

# A similar test to the above.
do_execsql_test fkey1-5.3 {
  CREATE TABLE Foo (
    Id INTEGER PRIMARY KEY, 
    ParentId INTEGER REFERENCES Foo(Id) ON DELETE CASCADE, C1
  );
  INSERT OR REPLACE INTO Foo(Id, ParentId, C1) VALUES (1, null, 'A');
  INSERT OR REPLACE INTO Foo(Id, ParentId, C1) VALUES (2, 1, 'A-2-1');
  INSERT OR REPLACE INTO Foo(Id, ParentId, C1) VALUES (3, 2, 'A-3-2');
  INSERT OR REPLACE INTO Foo(Id, ParentId, C1) VALUES (4, 3, 'A-4-3');
}
do_catchsql_test fkey1-5.4 {
  INSERT OR REPLACE INTO Foo(Id, ParentId, C1) VALUES (2, 3, 'A-2-3');
} {1 {FOREIGN KEY constraint failed}}

#-------------------------------------------------------------------------
# Check that foreign key processing is not fooled by partial indexes
# on the parent table.
#
do_execsql_test 6.0 {
  CREATE TABLE p1(x, y);
  CREATE UNIQUE INDEX p1x ON p1(x) WHERE y<2;
  INSERT INTO p1 VALUES(1, 1);
  CREATE TABLE c1(a REFERENCES p1(x));
}

do_catchsql_test 6.1 {
  INSERT INTO c1 VALUES(1);
} {1 {foreign key mismatch - "c1" referencing "p1"}}

do_execsql_test 6.2 {
  CREATE UNIQUE INDEX p1x2 ON p1(x);
  INSERT INTO c1 VALUES(1);
} {}

# 2021-07-03 https://sqlite.org/forum/forumpost/a6b0c05277
# 2021-07-07 https://sqlite.org/forum/forumpost/79c9e4797d
# Failure to allocate enough registers in the VDBE for a
# PRAGMA foreign_key_check when the foreign key has more
# columns than the table.
#
reset_db
do_execsql_test 7.1 {
  PRAGMA foreign_keys=OFF;
  CREATE TABLE t1(a,b,c,FOREIGN KEY(a,a,a,a,a,a,a,a,a,a,a,a,a,a) REFERENCES t0);
  INSERT INTO t1 VALUES(1,2,3);
  PRAGMA foreign_key_check;
} {t1 1 t0 0}
do_execsql_test 7.2 {
  DROP TABLE t1;
  CREATE TABLE t1(a,b,c AS(1),d, FOREIGN KEY(c,d,b,a,b,d,b,c) REFERENCES t0);
  PRAGMA foreign_key_check;
} {}

# 2021-12-31 forum https://sqlite.org/forum/forumpost/24bd1fef7e9323ef
# Memory leak caused by sqlite3NestedParse() running on a corrupt system
# table.  Discovered by Jingzhou Fu.
#
reset_db
do_execsql_test 8.1 {
  PRAGMA writable_schema=ON;
  PRAGMA foreign_keys = ON;
  CREATE TABLE sqlite_stat1 (tbl INTEGER PRIMARY KEY DESC, idx UNIQUE DEFAULT NULL) WITHOUT ROWID;
  PRAGMA writable_schema=OFF;
  CREATE TABLE sqlsim4(stat PRIMARY KEY);;
  CREATE TABLE t1(sqlsim7 REFERENCES sqlite_stat1 ON DELETE CASCADE);
  DROP table "sqlsim4";
} {}
# 2022-01-01 dbsqlfuzz 1c57440219f6f0aedf5e8f72a8ddd75f15aea381
# Follow-up case to the above.  Assertion is not true if the schema
# is corrupt.
reset_db
database_may_be_corrupt
do_execsql_test 8.2 {
  CREATE TABLE t1(a REFERENCES sqlite_stat1 ON DELETE CASCADE);
  CREATE TABLE t2(a TEXT PRIMARY KEY);
  PRAGMA writable_schema=ON;
  CREATE TABLE sqlite_stat1(tbl INTEGER PRIMARY KEY DESC, idx UNIQUE DEFAULT NULL) WITHOUT ROWID;
  UPDATE sqlite_schema SET name='sqlite_autoindex_sqlite_stat1_1' WHERE name='sqlite_autoindex_sqlite_stat1_2';
  PRAGMA writable_schema=RESET;
} {}
do_catchsql_test 8.3 {
  REINDEX;
} {1 {database disk image is malformed}}

# 2023-04-13 https://bugs.chromium.org/p/chromium/issues/detail?id=1405220
# Avoid double-de-quoting of table names when processing foreign keys.
#
reset_db
do_execsql_test 9.1 {
  PRAGMA foreign_keys = ON;
  CREATE TABLE """1"("""2", """3" PRIMARY KEY);
  CREATE TABLE """4"("""5" REFERENCES """1" ON DELETE RESTRICT);
  DELETE FROM """1";
}

finish_test
