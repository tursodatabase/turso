# 2017-10-11
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
# focus of this file is testing the sqlite_dbpage virtual table.
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix dbpage

ifcapable !vtab||!compound {
  finish_test
  return
}

sqlite3_db_config db DEFENSIVE 0
do_test 100 {
  execsql {
    PRAGMA auto_vacuum=0;
    PRAGMA page_size=4096;
    PRAGMA journal_mode=WAL;
  }
  execsql { 
    CREATE TABLE t1(a,b);
    WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<100)
      INSERT INTO t1(a,b) SELECT x, printf('%d-x%.*c',x,x,'x') FROM c;
    PRAGMA integrity_check;
  }
} {ok}
do_execsql_test 110 {
  SELECT pgno, quote(substr(data,1,5)) FROM sqlite_dbpage('main') ORDER BY pgno;
} {1 X'53514C6974' 2 X'0500000001' 3 X'0D0000004E' 4 X'0D00000016'}
do_execsql_test 120 {
  SELECT pgno, quote(substr(data,1,5)) FROM sqlite_dbpage WHERE pgno=2;
} {2 X'0500000001'}
do_execsql_test 130 {
  SELECT pgno, quote(substr(data,1,5)) FROM sqlite_dbpage WHERE pgno=4;
} {4 X'0D00000016'}
do_execsql_test 140 {
  SELECT pgno, quote(substr(data,1,5)) FROM sqlite_dbpage WHERE pgno=5;
} {}
do_execsql_test 150 {
  SELECT pgno, quote(substr(data,1,5)) FROM sqlite_dbpage WHERE pgno=0;
} {}
do_execsql_test 160 {
  ATTACH ':memory:' AS aux1;
  PRAGMA aux1.page_size=4096;
  CREATE TABLE aux1.t2(a,b,c);
  INSERT INTO t2 VALUES(11,12,13);
  SELECT pgno, quote(substr(data,1,5)) FROM sqlite_dbpage('aux1');
} {1 X'53514C6974' 2 X'0D00000001'}
do_execsql_test 170 {
  CREATE TABLE aux1.x3(x,y,z);
  INSERT INTO x3(x,y,z) VALUES(1,'main',1),(2,'aux1',1);
  SELECT pgno, schema, substr(data,1,6)
    FROM sqlite_dbpage, x3
   WHERE sqlite_dbpage.schema=x3.y AND sqlite_dbpage.pgno=x3.z
   ORDER BY x3.x;
} {1 main SQLite 1 aux1 SQLite}

do_execsql_test 200 {
  CREATE TEMP TABLE saved_content(x);
  INSERT INTO saved_content(x) SELECT data FROM sqlite_dbpage WHERE pgno=4;
  UPDATE sqlite_dbpage SET data=zeroblob(4096) WHERE pgno=4;
} {}
do_catchsql_test 210 {
  PRAGMA integrity_check;
} {1 {database disk image is malformed}}
do_execsql_test 220 {
  SELECT pgno, quote(substr(data,1,5)) FROM sqlite_dbpage('main') ORDER BY pgno;
} {1 X'53514C6974' 2 X'0500000001' 3 X'0D0000004E' 4 X'0000000000'}
do_execsql_test 230 {
  UPDATE sqlite_dbpage SET data=(SELECT x FROM saved_content) WHERE pgno=4;
} {}
do_catchsql_test 230 {
  PRAGMA integrity_check;
} {0 ok}
do_execsql_test 240 {
  DELETE FROM saved_content;
  INSERT INTO saved_content(x) 
     SELECT data FROM sqlite_dbpage WHERE schema='aux1' AND pgno=2;
} {}
do_execsql_test 241 {
  UPDATE sqlite_dbpage SET data=zeroblob(4096) WHERE pgno=2 AND schema='aux1';
} {}
do_catchsql_test 250 {
  PRAGMA aux1.integrity_check;
} {1 {database disk image is malformed}}
do_execsql_test 260 {
  UPDATE sqlite_dbpage SET data=(SELECT x FROM saved_content)
   WHERE pgno=2 AND schema='aux1';
} {}
do_catchsql_test 270 {
  PRAGMA aux1.integrity_check;
} {0 ok}

db close
sqlite3 db :memory:
do_execsql_test 300 {
  SELECT * FROM sqlite_temp_schema, sqlite_dbpage;
} {}

#-------------------------------------------------------------------------
reset_db
do_execsql_test 400 {
  ATTACH ':memory:' AS aux1;
  BEGIN;
    CREATE VIRTUAL TABLE aux1.t1 USING sqlite_dbpage;
    INSERT INTO t1 VALUES(17, NULL);
  COMMIT;
}

#-------------------------------------------------------------------------
reset_db
forcedelete test.db2
sqlite3 db2 test.db2
db2 eval {
  PRAGMA auto_vacuum=NONE;
  CREATE TABLE t1(x, y);
}

do_execsql_test 500 {
  PRAGMA auto_vacuum=NONE;
  CREATE TABLE x1(a);
  INSERT INTO x1 VALUES( hex(randomblob(2000)) );
  INSERT INTO x1 VALUES( hex(randomblob(2000)) );
  INSERT INTO x1 VALUES( hex(randomblob(2000)) );
  INSERT INTO x1 VALUES( hex(randomblob(2000)) );
  PRAGMA page_count;
} {18}

do_test 510 {
  db eval  BEGIN 
  db2 eval { PRAGMA page_count } {
    db eval {
      INSERT INTO sqlite_dbpage values($page_count, NULL);
    }
  }
  db2 eval { SELECT pgno, data FROM sqlite_dbpage } {
    db eval {
      INSERT INTO sqlite_dbpage values($pgno, $data);
    }
  }

  db eval COMMIT
} {}

db close
sqlite3 db test.db

do_execsql_test 520 {
  PRAGMA page_count;
  SELECT * FROM t1;
} {2}

db2 close

#-------------------------------------------------------------------------
reset_db
forcedelete test.db2
do_execsql_test 610 {
  ATTACH 'test.db2' AS aux;
  CREATE TABLE t1(x);
  CREATE TABLE t2(y);
  INSERT INTO t1 VALUES(1234);
  CREATE TABLE aux.x1(z);
}

set pgno [db one {SELECT max(rootpage) FROM sqlite_schema}]
sqlite3 db2 test.db2
db2 eval {
  BEGIN;
    SELECT * FROM x1;
}

do_catchsql_test 620 {
  UPDATE sqlite_dbpage SET data = (
    SELECT data FROM sqlite_dbpage WHERE pgno=$pgno-1
  ) WHERE pgno = $pgno;
} {1 {database is locked}}

db2 eval {
  COMMIT;
}

do_catchsql_test 630 {
  UPDATE sqlite_dbpage SET data = (
    SELECT data FROM sqlite_dbpage WHERE pgno=$pgno-1
  ) WHERE pgno = $pgno;
} {0 {}}

db close
sqlite3 db test.db

do_execsql_test 640 {
  SELECT * FROM t2;
} {1234}

db2 close

#-------------------------------------------------------------------------
reset_db
do_execsql_test 700 {
  CREATE TABLE t1(x);
  INSERT INTO t1 VALUES( hex(randomblob(1000)) );
  INSERT INTO t1 VALUES( hex(randomblob(1000)) );
  INSERT INTO t1 VALUES( hex(randomblob(1000)) );
}

forcedelete test.db2
sqlite3 db2 test.db2
db2 eval {
  CREATE TABLE y1(y);
  INSERT INTO y1 VALUES( hex(randomblob(1000)) );
}

set max [db2 one {PRAGMA page_count}]

do_test 710 {
  execsql {
    BEGIN;
  }

  for {set ii 1} {$ii <= $max} {incr ii} {
    set data [db2 one {SELECT data FROM sqlite_dbpage WHERE pgno=$ii}]
    execsql {
      UPDATE sqlite_dbpage SET data=$data WHERE pgno=$ii
    }
  }

  execsql {
      SAVEPOINT abc;
        INSERT INTO sqlite_dbpage VALUES(2, NULL);
      ROLLBACK TO abc;
    COMMIT;
  }
} {}

db close
sqlite3 db test.db

do_execsql_test 720 {
  PRAGMA integrity_check
} {ok}


finish_test
