# 2022 July 19
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
# focus of this file is testing VIEW statements.
#
set testdir [file dirname $argv0]
source $testdir/tester.tcl

# Omit this entire file if the library is not configured with views enabled.
ifcapable !view {
  finish_test
  return
}
set testprefix view3

# Ticket [d58ccbb3f1b]: Prevent Table.nRef overflow.
db close
sqlite3 db :memory:
do_test 1.1 {
  catchsql {
    CREATE TABLE t1(x);
    INSERT INTO t1 VALUES(5);
    CREATE VIEW v1 AS SELECT x*2 FROM t1;
    CREATE VIEW v2 AS SELECT * FROM v1 UNION SELECT * FROM v1;
    CREATE VIEW v4 AS SELECT * FROM v2 UNION SELECT * FROM v2;
    CREATE VIEW v8 AS SELECT * FROM v4 UNION SELECT * FROM v4;
    CREATE VIEW v16 AS SELECT * FROM v8 UNION SELECT * FROM v8;
    CREATE VIEW v32 AS SELECT * FROM v16 UNION SELECT * FROM v16;
    CREATE VIEW v64 AS SELECT * FROM v32 UNION SELECT * FROM v32;
    CREATE VIEW v128 AS SELECT * FROM v64 UNION SELECT * FROM v64;
    CREATE VIEW v256 AS SELECT * FROM v128 UNION SELECT * FROM v128;
    CREATE VIEW v512 AS SELECT * FROM v256 UNION SELECT * FROM v256;
    CREATE VIEW v1024 AS SELECT * FROM v512 UNION SELECT * FROM v512;
    CREATE VIEW v2048 AS SELECT * FROM v1024 UNION SELECT * FROM v1024;
    CREATE VIEW v4096 AS SELECT * FROM v2048 UNION SELECT * FROM v2048;
    CREATE VIEW v8192 AS SELECT * FROM v4096 UNION SELECT * FROM v4096;
    CREATE VIEW v16384 AS SELECT * FROM v8192 UNION SELECT * FROM v8192;
    CREATE VIEW v32768 AS SELECT * FROM v16384 UNION SELECT * FROM v16384;
    SELECT * FROM v32768 UNION SELECT * FROM v32768;
  }
} {1 {too many references to "v1": max 65535}}
ifcapable progress {
  do_test 1.2 {
    db progress 1000 {expr 1}
    catchsql {
      SELECT * FROM v32768;
    }
  } {1 interrupted}
}


finish_test
