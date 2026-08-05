# 2022-10-24
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
# This file implements test cases for wide table (tables with more than
# 64 columns) and indexes that reference columns beyond the 63rd or 64th
# column.
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix widetab1


# In order to pick the better index in the following query, SQLite needs to
# be able to detect when an index that references later columns in a wide
# table is a covering index.
#
do_execsql_test 100 {
  CREATE TABLE a(
   a00, a01, a02, a03, a04, a05, a06, a07, a08, a09,
   a10, a11, a12, a13, a14, a15, a16, a17, a18, a19,
   a20, a21, a22, a23, a24, a25, a26, a27, a28, a29,
   a30, a31, a32, a33, a34, a35, a36, a37, a38, a39,
   a40, a41, a42, a43, a44, a45, a46, a47, a48, a49,
   a50, a51, a52, a53, a54, a55, a56, a57, a58, a59,
   pd, bn, vb, bc, cn, ie, qm);
  CREATE INDEX a1 on a(pd, bn, vb, bc, cn); -- preferred index
  CREATE INDEX a2 on a(pd, bc, ie, qm);     -- suboptimal index
  CREATE TABLE b(bg, bc, bn, iv, ln, mg);
  CREATE INDEX b1 on b(bn, iv, bg);
}
do_eqp_test 110 {
  SELECT dc, count(cn)
    FROM (SELECT coalesce(b.bg, a.bc) as dc, cn
            FROM a LEFT JOIN b
                  ON a.bn = b.bn
                 AND CASE WHEN a.vb IS NOT NULL THEN 1 ELSE 0 END = b.iv
           WHERE pd BETWEEN 0 AND 10)
   GROUP BY dc;
} {
  QUERY PLAN
  |--SEARCH a USING COVERING INDEX a1 (pd>? AND pd<?)
  |--SEARCH b USING COVERING INDEX b1 (bn=? AND iv=?) LEFT-JOIN
  `--USE TEMP B-TREE FOR GROUP BY
}

reset_db
do_execsql_test 200 {
  CREATE TABLE t1(
    c00,c01,c02,c03,c04,c05,c06,c07,c08,c09,
    c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,
    c20,c21,c22,c23,c24,c25,c26,c27,c28,c29,
    c30,c31,c32,c33,c34,c35,c36,c37,c38,c39,
    c40,c41,c42,c43,c44,c45,c46,c47,c48,c49,
    c50,c51,c52,c53,c54,c55,c56,c57,c58,c59,
    c60,c61,c62,c63,c64,c65,c66,c67,c68,c69,
    c70,c71,c72,c73,c74,c75,c76,c77,c78,c79,
    c80,c81,c82,c83,c84,c85,c86,c87,c88,c89,
    c90,c91,c92,c93,c94,c95,c96,c97,c98,c99,
    a,b,c,d,e
  );
  CREATE INDEX t1x1 on t1(c00,a,b,
        c01,c02,c03,c04,c05,c06,c07,c08,c09,
    c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,
    c20,c21,c22,c23,c24,c25,c26,c27,c28,c29,
    c30,c31,c32,c33,c34,c35,c36,c37,c38,c39,
    c40,c41,c42,c43,c44,c45,c46,c47,c48,c49,
    c50,c51,c52,c53,c54,c55,c56,c57,c58,c59,
    c60,c61,c62,c63,c64,c65,c66,c67,c68,c69,
    c70,c71,c72,c73,c74,c75,c76,c77,c78,c79,
    c80,c81,c82,c83,c84,c85,c86,c87,c88,c89,
    c90,c91,c92,c93,c94,c00,c96,c97,c98,c99
  );
  CREATE INDEX t1cd ON t1(c,d);
  CREATE INDEX t1x2 ON t1(c01,c02,c03,a,b);
  WITH RECURSIVE c(x) AS (VALUES(0) UNION ALL SELECT x+1000 FROM c WHERE x<9000)
  INSERT INTO t1 SELECT
    x+00, x+01, x+02, x+03, x+04, x+05, x+06, x+07, x+08, x+09,
    x+10, x+11, x+12, x+13, x+14, x+15, x+16, x+17, x+18, x+19,
    x+20, x+21, x+22, x+23, x+24, x+25, x+26, x+27, x+28, x+29,
    x+30, x+31, x+32, x+33, x+34, x+35, x+36, x+37, x+38, x+39,
    x+40, x+41, x+42, x+43, x+44, x+45, x+46, x+47, x+48, x+49,
    x+50, x+51, x+52, x+53, x+54, x+55, x+56, x+57, x+58, x+59,
    x+60, x+61, x+62, x+63, x+64, x+65, x+66, x+67, x+68, x+69,
    x+70, x+71, x+72, x+73, x+74, x+75, x+76, x+77, x+78, x+79,
    x+80, x+81, x+82, x+83, x+84, x+85, x+86, x+87, x+88, x+89,
    x+90, x+91, x+92, x+93, x+94, x+95, x+96, x+97, x+98, x+99,
    x+100, x+101, x+102, x+103, x+104 FROM c;
}

do_execsql_test 210 {SELECT sum(c62) FROM t1;} 45620
do_execsql_test 220 {SELECT sum(c63) FROM t1;} 45630
do_execsql_test 230 {SELECT sum(c64) FROM t1;} 45640
do_execsql_test 240 {SELECT sum(c65) FROM t1;} 45650

do_execsql_test 300 {
  BEGIN;
  SELECT sum(c62) FROM t1;
  UPDATE t1 SET c62=c62+1 WHERE c00=1000;
  SELECT sum(c62) FROM t1;
} {45620 45621}
do_execsql_test 310 {
  SELECT sum(c65) FROM t1;
  UPDATE t1 SET c65=c65+1 WHERE c00=1000;
  SELECT sum(c65) FROM t1;
  ROLLBACK;
} {45650 45651}

do_execsql_test 320 {
  BEGIN;
  SELECT count(*) FROM t1;
  DELETE FROM t1 WHERE c=3102;
  SELECT COUNT(*) FROM t1;
  ROLLBACK;
} {10 9}
do_execsql_test 330 {
  BEGIN;
  SELECT count(*) FROM t1;
  DELETE FROM t1 WHERE c=3102 AND d=3103;
  SELECT COUNT(*) FROM t1;
  ROLLBACK;
} {10 9}
do_execsql_test 340 {
  BEGIN;
  DELETE FROM t1 WHERE (c,d) IN (VALUES(3102,3103),(4102,4103),(5102,5103),(1,2));
  SELECT count(*) FROM t1;
  ROLLBACK;
} {7}

do_execsql_test 400 {
  DROP INDEX t1cd;
  DROP INDEX t1x1;
  DROP INDEX t1x2;
  CREATE INDEX t1x3 ON t1(c00,c05,c08);
}
do_execsql_test 410 {SELECT sum(c08) FROM t1 WHERE c00 IN (1000,5000);} 6016
do_execsql_test 420 {SELECT sum(c63) FROM t1 WHERE c00 IN (1000,5000);} 6126
do_execsql_test 430 {SELECT sum(c64) FROM t1 WHERE c00 IN (1000,5000);} 6128

do_execsql_test 500 {
  DROP INDEX t1x3;
  CREATE TABLE t2 AS SELECT * FROM t1;
  CREATE INDEX t1x4 ON t1(c00, c62, a, b);
  CREATE INDEX t2x4 ON t2(c01, c62, c63, b, c);
  SELECT t1.b, t2.b FROM t1 JOIN t2 ON t2.c01=t1.c00+1 WHERE +t1.b<7000
   ORDER BY +t1.b;
} {101 101 1101 1101 2101 2101 3101 3101 4101 4101 5101 5101 6101 6101}

finish_test
