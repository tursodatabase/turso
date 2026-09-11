# 2024-05-28
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
# Test cases for the ability of the query planner to cope with
# star-schema queries.
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set ::testprefix starschema1

do_execsql_test 1.1 {
CREATE TABLE t1(
  a01 INT, a02 INT, a03 INT, a04 INT, a05 INT, a06 INT, a07 INT, a08 INT,
  a09 INT, a10 INT, a11 INT, a12 INT, a13 INT, a14 INT, a15 INT, a16 INT,
  a17 INT, a18 INT, a19 INT, a20 INT, a21 INT, a22 INT, a23 INT, a24 INT,
  a25 INT, a26 INT, a27 INT, a28 INT, a29 INT, a30 INT, a31 INT, a32 INT,
  a33 INT, a34 INT, a35 INT, a36 INT, a37 INT, a38 INT, a39 INT, a40 INT,
  a41 INT, a42 INT, a43 INT, a44 INT, a45 INT, a46 INT, a47 INT, a48 INT,
  a49 INT, a50 INT, a51 INT, a52 INT, a53 INT, a54 INT, a55 INT, a56 INT,
  a57 INT, a58 INT, a59 INT, a60 INT, a61 INT, a62 INT, a63 INT, d TEXT);
CREATE TABLE x01(b01 INT, c01 TEXT);
CREATE TABLE x02(b02 INT, c02 TEXT);
CREATE TABLE x03(b03 INT, c03 TEXT);
CREATE TABLE x04(b04 INT, c04 TEXT);
CREATE TABLE x05(b05 INT, c05 TEXT);
CREATE TABLE x06(b06 INT, c06 TEXT);
CREATE TABLE x07(b07 INT, c07 TEXT);
CREATE TABLE x08(b08 INT, c08 TEXT);
CREATE TABLE x09(b09 INT, c09 TEXT);
CREATE TABLE x10(b10 INT, c10 TEXT);
CREATE TABLE x11(b11 INT, c11 TEXT);
CREATE TABLE x12(b12 INT, c12 TEXT);
CREATE TABLE x13(b13 INT, c13 TEXT);
CREATE TABLE x14(b14 INT, c14 TEXT);
CREATE TABLE x15(b15 INT, c15 TEXT);
CREATE TABLE x16(b16 INT, c16 TEXT);
CREATE TABLE x17(b17 INT, c17 TEXT);
CREATE TABLE x18(b18 INT, c18 TEXT);
CREATE TABLE x19(b19 INT, c19 TEXT);
CREATE TABLE x20(b20 INT, c20 TEXT);
CREATE TABLE x21(b21 INT, c21 TEXT);
CREATE TABLE x22(b22 INT, c22 TEXT);
CREATE TABLE x23(b23 INT, c23 TEXT);
CREATE TABLE x24(b24 INT, c24 TEXT);
CREATE TABLE x25(b25 INT, c25 TEXT);
CREATE TABLE x26(b26 INT, c26 TEXT);
CREATE TABLE x27(b27 INT, c27 TEXT);
CREATE TABLE x28(b28 INT, c28 TEXT);
CREATE TABLE x29(b29 INT, c29 TEXT);
CREATE TABLE x30(b30 INT, c30 TEXT);
CREATE TABLE x31(b31 INT, c31 TEXT);
CREATE TABLE x32(b32 INT, c32 TEXT);
CREATE TABLE x33(b33 INT, c33 TEXT);
CREATE TABLE x34(b34 INT, c34 TEXT);
CREATE TABLE x35(b35 INT, c35 TEXT);
CREATE TABLE x36(b36 INT, c36 TEXT);
CREATE TABLE x37(b37 INT, c37 TEXT);
CREATE TABLE x38(b38 INT, c38 TEXT);
CREATE TABLE x39(b39 INT, c39 TEXT);
CREATE TABLE x40(b40 INT, c40 TEXT);
CREATE TABLE x41(b41 INT, c41 TEXT);
CREATE TABLE x42(b42 INT, c42 TEXT);
CREATE TABLE x43(b43 INT, c43 TEXT);
CREATE TABLE x44(b44 INT, c44 TEXT);
CREATE TABLE x45(b45 INT, c45 TEXT);
CREATE TABLE x46(b46 INT, c46 TEXT);
CREATE TABLE x47(b47 INT, c47 TEXT);
CREATE TABLE x48(b48 INT, c48 TEXT);
CREATE TABLE x49(b49 INT, c49 TEXT);
CREATE TABLE x50(b50 INT, c50 TEXT);
CREATE TABLE x51(b51 INT, c51 TEXT);
CREATE TABLE x52(b52 INT, c52 TEXT);
CREATE TABLE x53(b53 INT, c53 TEXT);
CREATE TABLE x54(b54 INT, c54 TEXT);
CREATE TABLE x55(b55 INT, c55 TEXT);
CREATE TABLE x56(b56 INT, c56 TEXT);
CREATE TABLE x57(b57 INT, c57 TEXT);
CREATE TABLE x58(b58 INT, c58 TEXT);
CREATE TABLE x59(b59 INT, c59 TEXT);
CREATE TABLE x60(b60 INT, c60 TEXT);
CREATE TABLE x61(b61 INT, c61 TEXT);
CREATE TABLE x62(b62 INT, c62 TEXT);
CREATE TABLE x63(b63 INT, c63 TEXT);
/****  Uncomment to generate actual data ************************************
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<172800)
  INSERT INTO t1
    SELECT stmtrand()%12, stmtrand()%13, stmtrand()%14, stmtrand()%15,
           stmtrand()%16, stmtrand()%17, stmtrand()%18, stmtrand()%19,
           stmtrand()%20, stmtrand()%21, stmtrand()%22, stmtrand()%23,
           stmtrand()%24, stmtrand()%25, stmtrand()%26, stmtrand()%27,
           stmtrand()%28, stmtrand()%29, stmtrand()%30, stmtrand()%31,
           stmtrand()%32, stmtrand()%33, stmtrand()%34, stmtrand()%35,
           stmtrand()%36, stmtrand()%37, stmtrand()%38, stmtrand()%39,
           stmtrand()%40, stmtrand()%41, stmtrand()%42, stmtrand()%43,
           stmtrand()%28, stmtrand()%29, stmtrand()%30, stmtrand()%31,
           stmtrand()%32, stmtrand()%33, stmtrand()%34, stmtrand()%35,
           stmtrand()%36, stmtrand()%37, stmtrand()%38, stmtrand()%39,
           stmtrand()%40, stmtrand()%41, stmtrand()%42, stmtrand()%43,
           stmtrand()%28, stmtrand()%29, stmtrand()%30, stmtrand()%31,
           stmtrand()%32, stmtrand()%33, stmtrand()%34, stmtrand()%35,
           stmtrand()%36, stmtrand()%37, stmtrand()%38, stmtrand()%39,
           stmtrand()%40, stmtrand()%41, stmtrand()%42, stmtrand() FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<8)
  INSERT INTO x01 SELECT n%4, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<12)
  INSERT INTO x02 SELECT n%6, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<16)
  INSERT INTO x03 SELECT n%8, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<20)
  INSERT INTO x04 SELECT n%10, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<24)
  INSERT INTO x05 SELECT n%12, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<32)
  INSERT INTO x06 SELECT n%16, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<36)
  INSERT INTO x07 SELECT n%18, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<40)
  INSERT INTO x08 SELECT n%20, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<44)
  INSERT INTO x09 SELECT n%22, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<48)
  INSERT INTO x10 SELECT n%24, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<52)
  INSERT INTO x11 SELECT n%26, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<56)
  INSERT INTO x12 SELECT n%28, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<60)
  INSERT INTO x13 SELECT n%30, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<64)
  INSERT INTO x14 SELECT n%32, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<72)
  INSERT INTO x15 SELECT n%36, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<80)
  INSERT INTO x16 SELECT n%40, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<8)
  INSERT INTO x17 SELECT n%4, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<12)
  INSERT INTO x18 SELECT n%6, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<16)
  INSERT INTO x19 SELECT n%8, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<20)
  INSERT INTO x20 SELECT n%10, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<24)
  INSERT INTO x21 SELECT n%12, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<32)
  INSERT INTO x22 SELECT n%16, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<36)
  INSERT INTO x23 SELECT n%18, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<40)
  INSERT INTO x24 SELECT n%20, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<44)
  INSERT INTO x25 SELECT n%22, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<48)
  INSERT INTO x26 SELECT n%24, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<52)
  INSERT INTO x27 SELECT n%26, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<56)
  INSERT INTO x28 SELECT n%28, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<60)
  INSERT INTO x29 SELECT n%30, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<64)
  INSERT INTO x30 SELECT n%32, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<72)
  INSERT INTO x31 SELECT n%36, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<80)
  INSERT INTO x32 SELECT n%40, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<8)
  INSERT INTO x33 SELECT n%4, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<12)
  INSERT INTO x34 SELECT n%6, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<16)
  INSERT INTO x35 SELECT n%8, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<20)
  INSERT INTO x36 SELECT n%10, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<24)
  INSERT INTO x37 SELECT n%12, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<32)
  INSERT INTO x38 SELECT n%16, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<36)
  INSERT INTO x39 SELECT n%18, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<40)
  INSERT INTO x40 SELECT n%20, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<44)
  INSERT INTO x41 SELECT n%22, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<48)
  INSERT INTO x42 SELECT n%24, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<52)
  INSERT INTO x43 SELECT n%26, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<56)
  INSERT INTO x44 SELECT n%28, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<60)
  INSERT INTO x45 SELECT n%30, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<64)
  INSERT INTO x46 SELECT n%32, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<72)
  INSERT INTO x47 SELECT n%36, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<80)
  INSERT INTO x48 SELECT n%40, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<8)
  INSERT INTO x49 SELECT n%4, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<12)
  INSERT INTO x50 SELECT n%6, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<16)
  INSERT INTO x51 SELECT n%8, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<20)
  INSERT INTO x52 SELECT n%10, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<24)
  INSERT INTO x53 SELECT n%12, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<32)
  INSERT INTO x54 SELECT n%16, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<36)
  INSERT INTO x55 SELECT n%18, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<40)
  INSERT INTO x56 SELECT n%20, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<44)
  INSERT INTO x57 SELECT n%22, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<48)
  INSERT INTO x58 SELECT n%24, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<52)
  INSERT INTO x59 SELECT n%26, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<56)
  INSERT INTO x60 SELECT n%28, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<60)
  INSERT INTO x61 SELECT n%30, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<64)
  INSERT INTO x62 SELECT n%32, format('%d-or-0x%04x',n,n) FROM c;
WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<72)
  INSERT INTO x63 SELECT n%36, format('%d-or-0x%04x',n,n) FROM c;
****************************************************************************/
CREATE INDEX t1a01 ON t1(a01);
CREATE INDEX t1a02 ON t1(a02);
CREATE INDEX t1a03 ON t1(a03);
CREATE INDEX t1a04 ON t1(a04);
CREATE INDEX t1a05 ON t1(a05);
CREATE INDEX t1a06 ON t1(a06);
CREATE INDEX t1a07 ON t1(a07);
CREATE INDEX t1a08 ON t1(a08);
CREATE INDEX t1a09 ON t1(a09);
CREATE INDEX t1a10 ON t1(a10);
CREATE INDEX t1a11 ON t1(a11);
CREATE INDEX t1a12 ON t1(a12);
CREATE INDEX t1a13 ON t1(a13);
CREATE INDEX t1a14 ON t1(a14);
CREATE INDEX t1a15 ON t1(a15);
CREATE INDEX t1a16 ON t1(a16);
CREATE INDEX t1a17 ON t1(a17);
CREATE INDEX t1a18 ON t1(a18);
CREATE INDEX t1a19 ON t1(a19);
CREATE INDEX t1a20 ON t1(a20);
CREATE INDEX t1a21 ON t1(a21);
CREATE INDEX t1a22 ON t1(a22);
CREATE INDEX t1a23 ON t1(a23);
CREATE INDEX t1a24 ON t1(a24);
CREATE INDEX t1a25 ON t1(a25);
CREATE INDEX t1a26 ON t1(a26);
CREATE INDEX t1a27 ON t1(a27);
CREATE INDEX t1a28 ON t1(a28);
CREATE INDEX t1a29 ON t1(a29);
CREATE INDEX t1a30 ON t1(a30);
CREATE INDEX t1a31 ON t1(a31);
CREATE INDEX t1a32 ON t1(a32);
CREATE INDEX t1a33 ON t1(a33);
CREATE INDEX t1a34 ON t1(a34);
CREATE INDEX t1a35 ON t1(a35);
CREATE INDEX t1a36 ON t1(a36);
CREATE INDEX t1a37 ON t1(a37);
CREATE INDEX t1a38 ON t1(a38);
CREATE INDEX t1a39 ON t1(a39);
CREATE INDEX t1a40 ON t1(a40);
CREATE INDEX t1a41 ON t1(a41);
CREATE INDEX t1a42 ON t1(a42);
CREATE INDEX t1a43 ON t1(a43);
CREATE INDEX t1a44 ON t1(a44);
CREATE INDEX t1a45 ON t1(a45);
CREATE INDEX t1a46 ON t1(a46);
CREATE INDEX t1a47 ON t1(a47);
CREATE INDEX t1a48 ON t1(a48);
CREATE INDEX t1a49 ON t1(a49);
CREATE INDEX t1a50 ON t1(a50);
CREATE INDEX t1a51 ON t1(a51);
CREATE INDEX t1a52 ON t1(a52);
CREATE INDEX t1a53 ON t1(a53);
CREATE INDEX t1a54 ON t1(a54);
CREATE INDEX t1a55 ON t1(a55);
CREATE INDEX t1a56 ON t1(a56);
CREATE INDEX t1a57 ON t1(a57);
CREATE INDEX t1a58 ON t1(a58);
CREATE INDEX t1a59 ON t1(a59);
CREATE INDEX t1a60 ON t1(a60);
CREATE INDEX t1a61 ON t1(a61);
CREATE INDEX t1a62 ON t1(a62);
CREATE INDEX t1a63 ON t1(a63);
CREATE INDEX x01x ON x01(b01);
CREATE INDEX x02x ON x02(b02);
CREATE INDEX x03x ON x03(b03);
CREATE INDEX x04x ON x04(b04);
CREATE INDEX x05x ON x05(b05);
CREATE INDEX x06x ON x06(b06);
CREATE INDEX x07x ON x07(b07);
CREATE INDEX x08x ON x08(b08);
CREATE INDEX x09x ON x09(b09);
CREATE INDEX x10x ON x10(b10);
CREATE INDEX x11x ON x11(b11);
CREATE INDEX x12x ON x12(b12);
CREATE INDEX x13x ON x13(b13);
CREATE INDEX x14x ON x14(b14);
CREATE INDEX x15x ON x15(b15);
CREATE INDEX x16x ON x16(b16);
CREATE INDEX x17x ON x17(b17);
CREATE INDEX x18x ON x18(b18);
CREATE INDEX x19x ON x19(b19);
CREATE INDEX x20x ON x20(b20);
CREATE INDEX x21x ON x21(b21);
CREATE INDEX x22x ON x22(b22);
CREATE INDEX x23x ON x23(b23);
CREATE INDEX x24x ON x24(b24);
CREATE INDEX x25x ON x25(b25);
CREATE INDEX x26x ON x26(b26);
CREATE INDEX x27x ON x27(b27);
CREATE INDEX x28x ON x28(b28);
CREATE INDEX x29x ON x29(b29);
CREATE INDEX x30x ON x30(b30);
CREATE INDEX x31x ON x31(b31);
CREATE INDEX x32x ON x32(b32);
CREATE INDEX x33x ON x33(b33);
CREATE INDEX x34x ON x34(b34);
CREATE INDEX x35x ON x35(b35);
CREATE INDEX x36x ON x36(b36);
CREATE INDEX x37x ON x37(b37);
CREATE INDEX x38x ON x38(b38);
CREATE INDEX x39x ON x39(b39);
CREATE INDEX x40x ON x40(b40);
CREATE INDEX x41x ON x41(b41);
CREATE INDEX x42x ON x42(b42);
CREATE INDEX x43x ON x43(b43);
CREATE INDEX x44x ON x44(b44);
CREATE INDEX x45x ON x45(b45);
CREATE INDEX x46x ON x46(b46);
CREATE INDEX x47x ON x47(b47);
CREATE INDEX x48x ON x48(b48);
CREATE INDEX x49x ON x49(b49);
CREATE INDEX x50x ON x50(b50);
CREATE INDEX x51x ON x51(b51);
CREATE INDEX x52x ON x52(b52);
CREATE INDEX x53x ON x53(b53);
CREATE INDEX x54x ON x54(b54);
CREATE INDEX x55x ON x55(b55);
CREATE INDEX x56x ON x56(b56);
CREATE INDEX x57x ON x57(b57);
CREATE INDEX x58x ON x58(b58);
CREATE INDEX x59x ON x59(b59);
CREATE INDEX x60x ON x60(b60);
CREATE INDEX x61x ON x61(b61);
CREATE INDEX x62x ON x62(b62);
CREATE INDEX x63x ON x63(b63);
ANALYZE sqlite_schema;
INSERT INTO sqlite_stat1(tbl,idx,stat) VALUES
  ('t1','t1a01','172800 14400'),
  ('t1','t1a02','172800 13293'),
  ('t1','t1a03','172800 12343'),
  ('t1','t1a04','172800 11520'),
  ('t1','t1a05','172800 10800'),
  ('t1','t1a06','172800 10165'),
  ('t1','t1a07','172800 9600'),
  ('t1','t1a08','172800 9095'),
  ('t1','t1a09','172800 8640'),
  ('t1','t1a10','172800 8229'),
  ('t1','t1a11','172800 7855'),
  ('t1','t1a12','172800 7514'),
  ('t1','t1a13','172800 7200'),
  ('t1','t1a14','172800 6912'),
  ('t1','t1a15','172800 6647'),
  ('t1','t1a16','172800 6400'),
  ('t1','t1a17','172800 6172'),
  ('t1','t1a18','172800 5959'),
  ('t1','t1a19','172800 5760'),
  ('t1','t1a20','172800 5575'),
  ('t1','t1a21','172800 5400'),
  ('t1','t1a22','172800 5237'),
  ('t1','t1a23','172800 5083'),
  ('t1','t1a24','172800 4938'),
  ('t1','t1a25','172800 4800'),
  ('t1','t1a26','172800 4671'),
  ('t1','t1a27','172800 4548'),
  ('t1','t1a28','172800 4431'),
  ('t1','t1a29','172800 4320'),
  ('t1','t1a30','172800 4215'),
  ('t1','t1a31','172800 4115'),
  ('t1','t1a32','172800 4019'),
  ('t1','t1a33','172800 6172'),
  ('t1','t1a34','172800 5959'),
  ('t1','t1a35','172800 5760'),
  ('t1','t1a36','172800 5575'),
  ('t1','t1a37','172800 5400'),
  ('t1','t1a38','172800 5237'),
  ('t1','t1a39','172800 5083'),
  ('t1','t1a40','172800 4938'),
  ('t1','t1a41','172800 4800'),
  ('t1','t1a42','172800 4671'),
  ('t1','t1a43','172800 4548'),
  ('t1','t1a44','172800 4431'),
  ('t1','t1a45','172800 4320'),
  ('t1','t1a46','172800 4215'),
  ('t1','t1a47','172800 4115'),
  ('t1','t1a48','172800 4019'),
  ('t1','t1a49','172800 6172'),
  ('t1','t1a50','172800 5959'),
  ('t1','t1a51','172800 5760'),
  ('t1','t1a52','172800 5575'),
  ('t1','t1a53','172800 5400'),
  ('t1','t1a54','172800 5237'),
  ('t1','t1a55','172800 5083'),
  ('t1','t1a56','172800 4938'),
  ('t1','t1a57','172800 4800'),
  ('t1','t1a58','172800 4671'),
  ('t1','t1a59','172800 4548'),
  ('t1','t1a60','172800 4431'),
  ('t1','t1a61','172800 4320'),
  ('t1','t1a62','172800 4215'),
  ('t1','t1a63','172800 4115'),
  ('x01','x01x','80 2'),
  ('x02','x02x','120 2'),
  ('x03','x03x','160 2'),
  ('x04','x04x','20 2'),
  ('x05','x05x','24 2'),
  ('x06','x06x','32 2'),
  ('x07','x07x','36 2'),
  ('x08','x08x','40 2'),
  ('x09','x09x','44 2'),
  ('x10','x10x','48 2'),
  ('x11','x11x','52 2'),
  ('x12','x12x','56 2'),
  ('x13','x13x','60 2'),
  ('x14','x14x','64 2'),
  ('x15','x15x','72 2'),
  ('x16','x16x','80 2'),
  ('x17','x17x','80 2'),
  ('x18','x18x','120 2'),
  ('x19','x19x','160 2'),
  ('x20','x20x','20 2'),
  ('x21','x21x','24 2'),
  ('x22','x22x','32 2'),
  ('x23','x23x','36 2'),
  ('x24','x24x','40 2'),
  ('x25','x25x','44 2'),
  ('x26','x26x','48 2'),
  ('x27','x27x','52 2'),
  ('x28','x28x','56 2'),
  ('x29','x29x','60 2'),
  ('x30','x30x','64 2'),
  ('x31','x31x','72 2'),
  ('x32','x32x','80 2'),
  ('x33','x33x','80 2'),
  ('x34','x34x','120 2'),
  ('x35','x35x','160 2'),
  ('x36','x36x','20 2'),
  ('x37','x37x','24 2'),
  ('x38','x38x','32 2'),
  ('x39','x39x','36 2'),
  ('x40','x40x','40 2'),
  ('x41','x41x','44 2'),
  ('x42','x42x','48 2'),
  ('x43','x43x','52 2'),
  ('x44','x44x','56 2'),
  ('x45','x45x','60 2'),
  ('x46','x46x','64 2'),
  ('x47','x47x','72 2'),
  ('x48','x48x','80 2'),
  ('x49','x49x','80 2'),
  ('x50','x50x','120 2'),
  ('x51','x51x','160 2'),
  ('x52','x52x','20 2'),
  ('x53','x53x','24 2'),
  ('x54','x54x','32 2'),
  ('x55','x55x','36 2'),
  ('x56','x56x','40 2'),
  ('x57','x57x','44 2'),
  ('x58','x58x','48 2'),
  ('x59','x59x','52 2'),
  ('x60','x60x','56 2'),
  ('x61','x61x','60 2'),
  ('x62','x62x','64 2'),
  ('x63','x63x','72 2');
ANALYZE sqlite_schema;
}
do_execsql_test 1.2 {
  EXPLAIN QUERY PLAN
  SELECT c01, c02, c03
    FROM t1, x01, x02, x03
   WHERE a01=b01 AND a02=b02 AND a03=b03;
} {/SCAN t1.*SEARCH.*SEARCH.*SEARCH/}  
do_execsql_test 1.3 {
  EXPLAIN QUERY PLAN
  SELECT c01, c02, c03, c04
    FROM t1, x01, x02, x03, x04
   WHERE a01=b01 AND a02=b02 AND a03=b03 AND a04=b04;
} {/SCAN .*SEARCH .*SEARCH .*SEARCH .*SEARCH /}  
do_execsql_test 1.4 {
  EXPLAIN QUERY PLAN
  SELECT c01, c02, c03, c04, c05
    FROM t1, x01, x02, x03, x04, x05
   WHERE a01=b01 AND a02=b02 AND a03=b03 AND a04=b04 AND a05=b05;
} {/SCAN .*SEARCH .*SEARCH .*SEARCH .*SEARCH .*SEARCH/}  
do_execsql_test 1.5 {
  EXPLAIN QUERY PLAN
  SELECT c01, c02, c03, c04, c05, c06
    FROM t1, x01, x02, x03, x04, x05, x06
   WHERE a01=b01 AND a02=b02 AND a03=b03 AND a04=b04 AND a05=b05
     AND a06=b06;
} {/SCAN .*SEARCH .*SEARCH .*SEARCH .*SEARCH .*SEARCH .*SEARCH/}  
do_execsql_test 1.6 {
  EXPLAIN QUERY PLAN
  SELECT c01, c02, c03, c04, c05, c06, c07
    FROM t1, x01, x02, x03, x04, x05, x06, x07
   WHERE a01=b01 AND a02=b02 AND a03=b03 AND a04=b04 AND a05=b05
     AND a06=b06 AND a07=b07;
} {/SCAN .*SEARCH .*SEARCH .*SEARCH .*SEARCH .*SEARCH .*SEARCH .*SEARCH/}  
do_execsql_test 1.7 {
  EXPLAIN QUERY PLAN
  SELECT c01, c02, c03, c04, c05, c06, c07, c08
    FROM t1, x01, x02, x03, x04, x05, x06, x07, x08
   WHERE a01=b01 AND a02=b02 AND a03=b03 AND a04=b04 AND a05=b05
     AND a06=b06 AND a07=b07 AND a08=b08;
} {~/SCAN.*SCAN/}
do_execsql_test 1.8 {
  EXPLAIN QUERY PLAN
  SELECT c01, c02, c03, c04, c05, c06, c07, c08,
         c09, c10, c11, c12, c13, c14, c15, c16,
         c17, c18, c19, c20, c21, c22, c23, c24,
         c25, c26, c27, c28, c29, c30, c31, c32
    FROM t1, x01, x02, x03, x04, x05, x06, x07, x08,
             x09, x10, x11, x12, x13, x14, x15, x16,
             x17, x18, x19, x20, x21, x22, x23, x24,
             x25, x26, x27, x28, x29, x30, x31, x32
   WHERE a01=b01 AND a02=b02 AND a03=b03 AND a04=b04 AND a05=b05 AND a06=b06
     AND a07=b07 AND a08=b08 AND a09=b09 AND a10=b10 AND a11=b11 AND a12=b12
     AND a13=b13 AND a14=b14 AND a15=b15 AND a16=b16 AND a17=b17 AND a18=b18
     AND a19=b19 AND a20=b20 AND a21=b21 AND a22=b22 AND a23=b23 AND a24=b24
     AND a25=b25 AND a26=b26 AND a27=b27 AND a28=b28 AND a29=b29 AND a30=b30
     AND a31=b31 AND a32=b32;
} {~/SCAN.*SCAN/}
do_execsql_test 1.9 {
  EXPLAIN QUERY PLAN
  SELECT c01, c02, c03, c04, c05, c06, c07, c08,
         c09, c10, c11, c12, c13, c14, c15, c16,
         c17, c18, c19, c20, c21, c22, c23, c24,
         c25, c26, c27, c28, c29, c30, c31, c32,
         c33, c34, c35, c36, c37, c38, c39, c40,
         c41, c42, c43, c44, c45, c46, c47, c48,
         c49, c50, c51, c52, c53, c54, c55, c56,
         c57, c58, c59, c60, c61, c62, c63
    FROM t1, x01, x02, x03, x04, x05, x06, x07, x08,
             x09, x10, x11, x12, x13, x14, x15, x16,
             x17, x18, x19, x20, x21, x22, x23, x24,
             x25, x26, x27, x28, x29, x30, x31, x32,
             x33, x34, x35, x36, x37, x38, x39, x40,
             x41, x42, x43, x44, x45, x46, x47, x48,
             x49, x50, x51, x52, x53, x54, x55, x56,
             x57, x58, x59, x60, x61, x62, x63
   WHERE a01=b01 AND a02=b02 AND a03=b03 AND a04=b04 AND a05=b05 AND a06=b06
     AND a07=b07 AND a08=b08 AND a09=b09 AND a10=b10 AND a11=b11 AND a12=b12
     AND a13=b13 AND a14=b14 AND a15=b15 AND a16=b16 AND a17=b17 AND a18=b18
     AND a19=b19 AND a20=b20 AND a21=b21 AND a22=b22 AND a23=b23 AND a24=b24
     AND a25=b25 AND a26=b26 AND a27=b27 AND a28=b28 AND a29=b29 AND a30=b30
     AND a31=b31 AND a32=b32 AND a33=b33 AND a34=b34 AND a35=b35 AND a36=b36
     AND a37=b37 AND a38=b38 AND a39=b39 AND a40=b40 AND a41=b41 AND a42=b42
     AND a43=b43 AND a44=b44 AND a45=b45 AND a46=b46 AND a47=b47 AND a48=b48
     AND a49=b49 AND a50=b50 AND a51=b51 AND a52=b52 AND a53=b53 AND a54=b54
     AND a55=b55 AND a56=b56 AND a57=b57 AND a58=b58 AND a59=b59 AND a60=b60
     AND a61=b61 AND a62=b62 AND a63=b63;
} {~/SCAN.*SCAN/}



finish_test
