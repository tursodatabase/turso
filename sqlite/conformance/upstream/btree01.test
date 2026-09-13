# 2014-11-27
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
# This file contains test cases for b-tree logic.
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix btree01

# The refactoring on the b-tree balance() routine in check-in
# http://sqlite.org/src/info/face33bea1ba3a (2014-10-27)
# caused the integrity_check on the following SQL to fail.
#
do_execsql_test btree01-1.1 {
  PRAGMA page_size=65536;
  CREATE TABLE t1(a INTEGER PRIMARY KEY, b BLOB);
  WITH RECURSIVE
     c(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM c WHERE i<30)
  INSERT INTO t1(a,b) SELECT i, zeroblob(6500) FROM c;
  UPDATE t1 SET b=zeroblob(3000);
  UPDATE t1 SET b=zeroblob(64000) WHERE a=2;
  PRAGMA integrity_check;
} {ok}

# The previous test is sufficient to prevent a regression.  But we
# add a number of additional tests to stress the balancer in similar
# ways, looking for related problems.
#
for {set i 1} {$i<=30} {incr i} {
  do_test btree01-1.2.$i {
    db eval {
      DELETE FROM t1;
      WITH RECURSIVE
        c(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM c WHERE i<30)
      INSERT INTO t1(a,b) SELECT i, zeroblob(6500) FROM c;
      UPDATE t1 SET b=zeroblob(3000);
      UPDATE t1 SET b=zeroblob(64000) WHERE a=$::i;
      PRAGMA integrity_check;
    }
  } {ok}
}
for {set i 1} {$i<=30} {incr i} {
  do_test btree01-1.3.$i {
    db eval {
      DELETE FROM t1;
      WITH RECURSIVE
        c(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM c WHERE i<30)
      INSERT INTO t1(a,b) SELECT i, zeroblob(6500) FROM c;
      UPDATE t1 SET b=zeroblob(2000);
      UPDATE t1 SET b=zeroblob(64000) WHERE a=$::i;
      PRAGMA integrity_check;
    }
  } {ok}
}
for {set i 1} {$i<=30} {incr i} {
  do_test btree01-1.4.$i {
    db eval {
      DELETE FROM t1;
      WITH RECURSIVE
        c(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM c WHERE i<30)
      INSERT INTO t1(a,b) SELECT i, zeroblob(6500) FROM c;
      UPDATE t1 SET b=zeroblob(6499) WHERE (a%3)==0;
      UPDATE t1 SET b=zeroblob(6499) WHERE (a%3)==1;
      UPDATE t1 SET b=zeroblob(6499) WHERE (a%3)==2;
      UPDATE t1 SET b=zeroblob(64000) WHERE a=$::i;
      PRAGMA integrity_check;
    }
  } {ok}
}
for {set i 1} {$i<=30} {incr i} {
  do_test btree01-1.5.$i {
    db eval {
      DELETE FROM t1;
      WITH RECURSIVE
        c(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM c WHERE i<30)
      INSERT INTO t1(a,b) SELECT i, zeroblob(6542) FROM c;
      UPDATE t1 SET b=zeroblob(2331);
      UPDATE t1 SET b=zeroblob(65496) WHERE a=$::i;
      PRAGMA integrity_check;
    }
  } {ok}
}
for {set i 1} {$i<=30} {incr i} {
  do_test btree01-1.6.$i {
    db eval {
      DELETE FROM t1;
      WITH RECURSIVE
        c(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM c WHERE i<30)
      INSERT INTO t1(a,b) SELECT i, zeroblob(6542) FROM c;
      UPDATE t1 SET b=zeroblob(2332);
      UPDATE t1 SET b=zeroblob(65496) WHERE a=$::i;
      PRAGMA integrity_check;
    }
  } {ok}
}
for {set i 1} {$i<=30} {incr i} {
  do_test btree01-1.7.$i {
    db eval {
      DELETE FROM t1;
      WITH RECURSIVE
        c(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM c WHERE i<30)
      INSERT INTO t1(a,b) SELECT i, zeroblob(6500) FROM c;
      UPDATE t1 SET b=zeroblob(1);
      UPDATE t1 SET b=zeroblob(65000) WHERE a=$::i;
      PRAGMA integrity_check;
    }
  } {ok}
}
for {set i 1} {$i<=31} {incr i} {
  do_test btree01-1.8.$i {
    db eval {
      DELETE FROM t1;
      WITH RECURSIVE
        c(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM c WHERE i<31)
      INSERT INTO t1(a,b) SELECT i, zeroblob(6500) FROM c;
      UPDATE t1 SET b=zeroblob(4000);
      UPDATE t1 SET b=zeroblob(65000) WHERE a=$::i;
      PRAGMA integrity_check;
    }
  } {ok}
}

# 2022-03-06 OSSFuzz issue 45329
# An assertion fault due to the failure to clear a flag in an optimization
# committed last night.
#
# When the stay-on-last page optimization of sqlite3BtreeIndexMoveto() is
# invoked, it needs to clear the BTCF_ValidOvfl flag.
#
db close
sqlite3 db :memory:
do_execsql_test btree01-2.1 {
  PRAGMA page_size=1024;
  CREATE TABLE t1(a INT PRIMARY KEY, b BLOB, c INT) WITHOUT ROWID;
  WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<100)
    INSERT INTO t1(a,b,c) SELECT x*2, zeroblob(100), x FROM c;
  UPDATE t1 SET b=zeroblob(1000) WHERE a=198;
  CREATE TABLE t2(x INTEGER PRIMARY KEY, y INT);
  INSERT INTO t2(y) VALUES(198),(187),(100);
  SELECT y, c FROM t2 LEFT JOIN t1 ON y=a ORDER BY x;
} {198 99 187 {} 100 50}
do_execsql_test btree01-2.2 {
  SELECT y, c FROM t1 RIGHT JOIN t2 ON y=a ORDER BY x;
} {198 99 187 {} 100 50}


finish_test
