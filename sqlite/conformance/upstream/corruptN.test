# 2020-12-16
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
set testprefix corruptN

# These tests deal with corrupt database files
#
database_may_be_corrupt

reset_db
do_test 1.0 {
  sqlite3 db {}
  db deserialize [decode_hexdb {
.open --hexdb
| size 4096 pagesize 512 filename sql024239.txt.db
| page 1 offset 0
|      0: 53 51 4c 69 74 65 20 66 6f 72 6d 61 74 20 33 00   SQLite format 3.
|     16: 02 00 01 01 00 40 20 20 00 00 00 0c 00 00 00 07   .....@  ........
|     32: 00 00 00 00 00 00 00 00 00 00 00 08 00 00 00 04   ................
|     48: 00 00 00 00 89 00 00 04 00 10 00 01 0a 00 00 01   ................
|     80: 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 0c   ................
|     96: 00 2e 2c 50 0d 00 00 00 06 01 06 00 01 da 01 b0   ..,P............
|    112: 01 56 01 86 01 2a 01 06 00 00 00 00 00 00 00 00   .V...*..........
|    256: 00 00 00 00 00 00 22 07 06 17 11 11 01 31 74 61   .............1ta
|    272: 62 6c 65 74 34 74 34 07 43 52 45 41 54 45 20 54   blet4t4.CREATE T
|    288: 41 42 4c 45 20 74 34 28 78 29 2a 06 06 17 13 11   ABLE t4(x)*.....
|    304: 01 3f 69 6e 64 65 78 74 33 78 74 33 05 43 52 45   .?indext3xt3.CRE
|    320: 41 54 45 20 49 4e 44 45 58 20 74 33 78 20 4f 4e   ATE INDEX t3x ON
|    336: 20 74 33 28 78 29 2e 04 06 17 15 11 01 45 69 6e    t3(x).......Ein
|    352: 64 65 78 74 32 63 64 74 32 05 43 52 45 41 54 45   dext2cdt2.CREATE
|    368: 20 49 4e 44 45 58 20 74 32 63 64 20 4f 4e 20 74    INDEX t2cd ON t
|    384: 32 28 63 2c 64 29 28 05 06 17 11 11 01 3d 74 61   2(c,d)(......=ta
|    400: 62 6c 65 74 33 74 33 07 43 52 45 41 54 45 20 54   blet3t3.CREATE T
|    416: 41 42 4c 45 20 74 33 28 63 2c 78 2c 65 2c 66 29   ABLE t3(c,x,e,f)
|    432: 28 02 06 17 11 11 01 3d 74 61 62 6c 65 74 32 74   (......=tablet2t
|    448: 32 03 43 52 45 41 54 45 20 54 41 42 4c 45 20 74   2.CREATE TABLE t
|    464: 32 28 63 2c 64 2c 65 2c 66 29 24 01 06 17 11 11   2(c,d,e,f)$.....
|    480: 01 35 74 61 62 6c 65 74 31 74 31 02 43 52 45 41   .5tablet1t1.CREA
|    496: 54 45 20 54 41 42 4c 45 20 74 31 28 61 2c 62 29   TE TABLE t1(a,b)
| page 2 offset 512
|      0: 0d 00 00 00 04 01 41 00 01 fa 01 f3 01 de 01 cf   ......A.........
|    160: 00 00 20 00 00 00 00 00 00 00 00 00 00 00 00 00   .. .............
|    448: 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 0d   ................
|    464: 04 03 17 17 73 65 76 65 6e 65 69 67 68 74 13 03   ....seveneight..
|    480: 03 07 07 40 14 00 00 00 00 00 00 40 18 00 00 00   ...@.......@....
|    496: 00 00 00 05 02 03 01 01 03 04 04 01 03 09 01 02   ................
| page 3 offset 1024
|      0: 0d 00 00 00 08 01 54 00 01 f7 01 ec 01 c5 01 aa   ......T.........
|     16: 01 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00   ................
|    112: 00 00 dd 00 00 00 00 00 00 00 00 00 00 00 00 00   ................
|    336: 00 00 00 00 19 08 05 17 17 17 17 65 69 67 68 74   ...........eight
|    352: 65 69 67 68 74 73 65 76 65 6e 73 65 76 65 6e 25   eightsevenseven%
|    368: 07 05 07 00 00 00 00 00 00 00 00 00 00 00 00 00   ................
|    432: 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 08   ................
|    480: 00 00 0f 04 17 17 01 65 69 67 68 74 65 69 67 68   .......eighteigh
|    496: 74 08 15 04 07 07 01 40 18 00 00 00 00 00 00 40   t......@.......@
| page 4 offset 1536
|      0: 18 00 00 00 00 00 00 07 07 04 01 01 01 04 04 06   ................
|     16: 07 04 01 01 01 02 02 05 0f 04 17 17 01 73 6d 76   .............smv
|     32: 65 6e 65 69 67 68 74 04 15 04 07 07 01 40 14 00   eneight......@..
| page 5 offset 2048
|      0: 0a 00 00 00 08 01 96 00 01 fa 01 c4 01 f2 01 bc   ................
|     16: 01 dc 01 e1 01 96 01 cc 00 00 00 00 00 00 00 00   ................
|    160: 00 00 00 00 00 00 32 00 00 00 00 00 00 00 00 00   ......2.........
|    368: 00 00 00 08 00 00 00 00 00 00 00 00 00 00 00 00   ................
|    400: 00 00 00 00 00 00 0f 04 17 17 01 85 69 67 68 74   ............ight
|    416: 65 69 67 68 74 08 15 04 07 07 01 40 18 00 00 00   eight......@....
|    432: 00 00 00 40 18 00 00 00 00 00 00 07 07 04 01 01   ...@............
|    448: 01 04 04 06 07 04 01 01 01 02 02 05 0f 04 17 17   ................
|    464: 01 73 6d 76 65 6e 65 69 67 68 74 04 15 04 07 07   .smveneight.....
|    480: 01 40 14 00 00 00 00 00 00 40 18 00 00 00 00 00   .@.......@......
|    496: 00 03 07 04 01 01 01 03 04 02 05 04 03 01 09 02   ................
| page 6 offset 2560
|      0: 0a 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00   ................
|     16: 00 00 00 00 00 00 00 00 00 00 00 00 00 00 10 00   ................
|    304: 00 00 00 26 00 00 00 00 00 00 00 00 00 00 00 00   ...&............
| page 7 offset 3072
|      0: 0d 00 00 00 08 01 c2 00 01 fb 01 f6 01 f1 01 ec   ................
|     16: 01 e0 01 d4 01 cb 01 c2 00 00 00 00 00 00 00 00   ................
|    128: 00 00 00 00 00 00 00 00 00 00 00 00 00 20 00 04   ............. ..
|    384: 00 00 00 00 00 00 00 00 00 07 08 02 17 65 69 fc   .............ei.
|    400: 68 74 07 07 02 17 65 69 67 68 74 0a fb fd f8 bf   ht....eight.....
|    416: e7 ff ff ff 00 00 00 0a 05 02 07 40 18 00 00 00   ...........@....
|    432: 00 00 00 03 04 02 01 04 03 03 02 01 04 03 02 01   ................
|    448: ff ff ff ff ff ff 00 00 00 00 00 00 00 00 00 00   ................
| end sql024239.txt.db
}]} {}

do_catchsql_test 1.1 {
  VACUUM;
} {1 {database disk image is malformed}}

# 2021-04-05 dbsqlfuzz b92b72e4de80b5140c30ab71372ca719b8feb618
do_test 2.0 {
  sqlite3 db {}
  db deserialize [decode_hexdb {
| size 16384 pagesize 4096 filename c-b92b.txt.db
| page 1 offset 0
|      0: 53 51 4c 69 74 65 20 66 6f 72 6d 61 74 20 33 00   SQLite format 3.
|     16: 10 00 01 01 00 40 20 20 00 00 00 00 00 00 00 04   .....@  ........
|     32: 00 00 00 00 00 00 00 00 00 00 00 03 00 00 00 04   ................
|     48: 00 00 00 00 00 00 00 00 00 00 00 01 00 00 00 00   ................
|     96: 00 00 00 00 0d 0f f8 00 04 0f 12 00 0f 91 0f d3   ................
|    112: 0f 67 0f 12 00 00 00 00 00 00 00 00 00 00 00 00   .g..............
|   3856: 00 00 53 04 07 1b 13 11 08 81 0d 74 72 69 67 67   ..S........trigg
|   3872: 65 72 74 72 30 74 31 43 52 45 41 54 45 20 54 52   ertr0t1CREATE TR
|   3888: 49 47 47 45 52 20 74 72 30 20 44 45 4c 45 54 45   IGGER tr0 DELETE
|   3904: 20 4f 4e 20 74 31 20 42 45 47 49 4e 0a 20 20 55    ON t1 BEGIN.  U
|   3920: 50 44 41 54 45 20 74 31 20 53 45 54 20 62 20 3d   PDATE t1 SET b =
|   3936: 20 61 3b 0a 45 4e 44 28 03 06 17 11 11 01 3d 69    a;.END(......=i
|   3952: 6e 64 65 78 69 30 74 31 04 43 52 45 41 54 45 20   ndexi0t1.CREATE 
|   3968: 49 4e 44 45 58 20 69 30 20 4f 4e 20 74 31 28 62   INDEX i0 ON t1(b
|   3984: 29 40 01 06 17 11 11 01 6d 74 61 62 6c 65 74 31   )@......mtablet1
|   4000: 74 31 02 43 52 45 41 54 45 20 54 41 42 4c 45 20   t1.CREATE TABLE 
|   4016: 74 31 28 61 20 55 4e 49 51 55 45 20 4f 4e 20 43   t1(a UNIQUE ON C
|   4032: 4f 4e 46 4c 49 43 54 20 52 45 50 4c 41 43 45 2c   ONFLICT REPLACE,
|   4048: 20 62 29 23 02 06 17 37 11 01 00 69 6e 64 65 78    b)#...7...index
|   4064: 73 71 6c 69 74 65 5f 61 75 74 6f 69 6e 64 65 78   sqlite_autoindex
|   4080: 5f 74 31 5f 31 74 31 03 00 00 00 08 00 00 00 00   _t1_1t1.........
| page 2 offset 4096
|      0: 0d 00 00 00 02 0f 00 00 00 00 00 00 00 00 00 00   ................
|   4080: 00 00 05 02 03 01 01 09 0d 05 01 03 01 01 04 0c   ................
| page 3 offset 8192
|      0: 0a 00 00 00 02 0f f5 00 0f fb 0f f5 00 00 00 00   ................
|   4080: 00 00 00 00 00 05 03 01 01 09 02 04 03 01 09 04   ................
| page 4 offset 12288
|      0: 0a 00 00 00 02 0f f5 00 0f fb 0f f5 00 00 00 00   ................
|   4080: 00 00 00 00 00 05 03 01 01 0d 02 04 03 00 00 00   ................
| end c-b92b.txt.db
}]} {}


reset_db
if {![info exists ::G(perm:presql)]} {
  do_execsql_test 3.0 {
    CREATE TABLE t1(x INTEGER PRIMARY KEY AUTOINCREMENT, y);
    PRAGMA writable_schema = 1;
    UPDATE sqlite_schema 
      SET sql = 'CREATE TABLE sqlite_sequence(name-seq)' 
      WHERE name = 'sqlite_sequence';
  }
  db close
  sqlite3 db test.db
  do_catchsql_test 3.1 {
    PRAGMA writable_schema = 1;
    INSERT INTO t1(y) VALUES('abc');
  } {1 {database disk image is malformed}}
  reset_db

  do_execsql_test 4.1 {
    CREATE TABLE x1(a INTEGER PRIMARY KEY, b UNIQUE, c UNIQUE);
    INSERT INTO x1 VALUES(1, 1, 2);
    INSERT INTO x1 VALUES(2, 2, 3);
    INSERT INTO x1 VALUES(3, 3, 4);
    INSERT INTO x1 VALUES(4, 5, 6);
    PRAGMA writable_schema = 1;

    UPDATE sqlite_schema SET rootpage = (
      SELECT rootpage FROM sqlite_schema WHERE name = 'sqlite_autoindex_x1_2'
    ) WHERE name = 'sqlite_autoindex_x1_1';
  }

  db close
  sqlite3 db test.db
  breakpoint
  do_catchsql_test 4.2 {
    PRAGMA writable_schema = 1;
    REPLACE INTO x1 VALUES(5, 2, 3);
  } {0 {}}

}

#-------------------------------------------------------------------------

reset_db

ifcapable json1&&vtab {
  db func strreplace strreplace
  proc strreplace {orig a b} {
    string map [list $a $b] $orig
  }

  do_execsql_test 5.0 {
    CREATE TABLE t1(a, b);
    CREATE INDEX t1a ON t1(a);
    CREATE INDEX t1b ON t1(b);

    PRAGMA writable_schema = 1;
    UPDATE sqlite_schema 
      SET sql = strreplace(sql, 't1', 'json_each') 
      WHERE type='index';
  }

  # Do not run this tests if there is any presql (SQL run from within
  # the [sqlite3] command) configured. In this case the schema is parsed
  # before the "PRAGMA writable_schema" command is executed and the 
  # script throws and exception.
  if {[info exists ::G(perm:presql)]==0 || $::G(perm:presql)==""} {
    db close
    sqlite3 db test.db

    do_execsql_test 5.1 {
      PRAGMA writable_schema = 1;
      SELECT * FROM t1
    }
  }
}; # ifcapable json1&&vtab

#-------------------------------------------------------------------------
reset_db

do_execsql_test 6.0 {
  PRAGMA auto_vacuum = 0;
  PRAGMA page_size=1024;
  CREATE TABLE t1(a INTEGER PRIMARY KEY, b);
  INSERT INTO t1(b) VALUES(zeroblob(300)),(zeroblob(300)),(zeroblob(300)),(zeroblob(300));
  CREATE TABLE t2(a);
  CREATE TRIGGER t1tr BEFORE UPDATE ON t1 BEGIN DELETE FROM t2; END;
  PRAGMA writable_schema=ON;
  UPDATE sqlite_schema SET rootpage=3 WHERE rowid=2;
  PRAGMA writable_schema=RESET;
  INSERT INTO t2 VALUES('active'),('boomer'),('atom'),('atomic'),
         ('alpha channel backup abandon test aback boomer atom alpha active');
}
do_catchsql_test 6.1 {
  UPDATE t1 SET b=zeroblob(299);
} {1 {database disk image is malformed}}

reset_db
do_execsql_test 6.2 {
  -- Make "t1" a large table. Large enough that the children of the root
  -- node are interior nodes.
  PRAGMA page_size = 1024;
  PRAGMA auto_vacuum = 0;
  CREATE TABLE t1(x);
  WITH s(i) AS (
    SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i<500
  )
  INSERT INTO t1 SELECT zeroblob(300) FROM s;
  
  CREATE TABLE t2(y);
  CREATE TRIGGER tr BEFORE UPDATE ON t1 BEGIN
    DELETE FROM t2;
  END;
  
  -- Set the root of table t2 to 137 - the leftmost child of the root of t1.
  PRAGMA writable_schema = ON;
  UPDATE sqlite_schema SET rootpage = 137 WHERE name='t2';
  PRAGMA writable_schema = RESET;
}
  
do_catchsql_test 6.3 {
  -- Run an UPDATE on t1 that will hit a child of page 136. Have the trigger
  -- clear page 136 and its children. Assert fails.
  UPDATE t1 SET x='hello world' WHERE rowid=1;
} {1 {database disk image is malformed}}

#-------------------------------------------------------------------------
reset_db
do_execsql_test 7.0 {
  BEGIN;
  CREATE TABLE p1(x PRIMARY KEY);
  CREATE TABLE c1(y);

  PRAGMA schema_version = 0;
  PRAGMA writable_schema = RESET;

  INSERT INTO c1 VALUES(1000);
  ROLLBACK;
}

do_execsql_test 7.1 {
  PRAGMA table_info = p1;
} {0 x {} 0 {} 1}

do_catchsql_test 7.2 {
  SELECT * FROM p1;
} {1 {database disk image is malformed}}

do_catchsql_test 7.3 {
  PRAGMA integrity_check
} {1 {database disk image is malformed}}


finish_test
