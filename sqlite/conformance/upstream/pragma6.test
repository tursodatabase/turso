# 2024 February 27
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
# This file implements tests for PRAGMAs quick_check and integrity_check.
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix pragma6

database_may_be_corrupt

#-------------------------------------------------------------------------
#
do_test 1.0 {
  sqlite3 db {}
  db deserialize [decode_hexdb {
    .open --hexdb
    | size 12288 pagesize 4096 filename crash-540f4c1eb1e7ac.db
    | page 1 offset 0
    |      0: 53 51 4c 69 74 65 20 66 6f 72 6d 61 74 20 33 00   SQLite format 3.
    |     16: 10 00 01 01 00 40 20 20 00 00 00 00 00 00 00 03   .....@  ........
    |     32: 00 bb 00 00 00 00 00 00 00 00 00 00 00 00 00 00   ................
    |     96: 00 00 00 00 0d 00 00 00 02 0f 7f 00 0f c3 0f 7f   ................
    |   3952: 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 42   ...............B
    |   3968: 02 06 17 11 11 01 71 74 61 62 6c 65 74 32 74 32   ......qtablet2t2
    |   3984: 03 43 52 45 41 54 45 20 54 41 42 4c 45 20 74 32   .CREATE TABLE t2
    |   4000: 28 61 20 49 4e 54 2c 20 62 20 41 53 20 28 61 2a   (a INT, b AS (a*
    |   4016: 32 29 20 53 54 4f 52 45 44 20 4e 4f 54 20 4e 55   2) STORED NOT NU
    |   4032: 4c 4c 29 3b 01 06 17 11 11 01 63 74 61 62 6c 65   LL);......ctable
    |   4048: 74 31 74 31 02 43 52 45 41 54 45 20 54 41 42 4c   t1t1.CREATE TABL
    |   4064: 45 20 74 31 28 61 20 49 4e 54 2c 20 62 20 41 53   E t1(a INT, b AS
    |   4080: 20 28 61 2a 32 29 20 4e 4f 54 20 4e 55 4c 4c 29    (a*2) NOT NULL)
    | page 2 offset 4096
    |      0: 0d 00 00 00 05 0f e7 00 00 00 00 00 00 00 00 00   ................
    |   4064: 00 00 00 00 00 00 00 00 03 05 02 01 05 03 04 02   ................
    |   4080: 01 04 03 03 02 01 03 03 02 02 01 02 02 01 02 09   ................
    | page 3 offset 8192
    |      0: 0d 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00   ................
    |   4048: 00 00 00 00 00 00 00 00 00 00 00 00 00 00 05 05   ................
    |   4064: 03 01 01 05 0a 05 04 03 01 01 04 08 05 03 03 01   ................
    |   4080: 01 03 06 05 02 03 00 00 00 00 00 00 00 00 00 00   ................
    | end crash-540f4c1eb1e7ac.db
  }]
} {}

do_test 1.1 {
  execsql {
    CREATE TEMP TABLE t2(
        a t1 PRIMARY KEY default 27,
        b default(current_timestamp),
        d TEXT UNIQUE DEFAULT 'ch`arlie',
        c TEXT UNIQUE DEFAULT 084,
        UNIQUE(c,b,b,a,b)
    ) WITHOUT ROWID;
  }
  catchsql { INSERT INTO t1(a) VALUES(zeroblob(40000)) }
  set {} {}
} {}

do_test 1.2 {
  execsql { PRAGMA integrity_check; }
  execsql { PRAGMA quick_check; }
  set {} {}
} {}

finish_test
