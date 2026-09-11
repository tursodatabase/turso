# 2024-01-19
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
# This file implements tests for SQL literals


set testdir [file dirname $argv0]
source $testdir/tester.tcl
set ::testprefix literal

proc test_literal {tn lit type val} {
  do_execsql_test $tn.1 "SELECT typeof( $lit ), $lit" [list $type $val]

  ifcapable altertable {
    do_execsql_test $tn.2 "
      DROP TABLE IF EXISTS x1;
      CREATE TABLE x1(a);
      INSERT INTO x1 VALUES(123);
      ALTER TABLE x1 ADD COLUMN b DEFAULT $lit ;
      SELECT typeof(b), b FROM x1;
    " [list $type $val]
  }

  do_execsql_test $tn.3 "
    DROP TABLE IF EXISTS x1;
    CREATE TABLE x1(a DEFAULT $lit);
    INSERT INTO x1 DEFAULT VALUES;
    SELECT typeof(a), a FROM x1;
  " [list $type $val]
}

proc test_literal_error {tn lit unrec} {
  do_catchsql_test $tn "SELECT $lit" "1 {unrecognized token: \"$unrec\"}"
}


test_literal 1.0 45                  integer 45
test_literal 1.1 0xFF                integer 255
test_literal 1.2 0xFFFFFFFF          integer [expr 0xFFFFFFFF]
test_literal 1.3 0x123FFFFFFFF       integer [expr 0x123FFFFFFFF]
test_literal 1.4 -0x123FFFFFFFF      integer [expr -1 * 0x123FFFFFFFF]
test_literal 1.5 0xFFFFFFFFFFFFFFFF  integer -1
test_literal 1.7 0x7FFFFFFFFFFFFFFF  integer  [expr 0x7FFFFFFFFFFFFFFF]
test_literal 1.8 -0x7FFFFFFFFFFFFFFF integer [expr -0x7FFFFFFFFFFFFFFF]
test_literal 1.9 +0x7FFFFFFFFFFFFFFF integer [expr +0x7FFFFFFFFFFFFFFF]
test_literal 1.10 -45                integer -45
test_literal 1.11 '0xFF'             text 0xFF
test_literal 1.12 '-0xFF'            text -0xFF
test_literal 1.13 -'0xFF'            integer 0
test_literal 1.14 -9223372036854775808 integer -9223372036854775808

test_literal 2.1  1e12    real    1000000000000.0
test_literal 2.2  1.0     real    1.0
test_literal 2.3  1e1000  real    Inf
test_literal 2.4  -1e1000  real   -Inf

test_literal 3.1  1_000   integer 1000
test_literal 3.2  1.1_1   real    1.11
test_literal 3.3  1_0.1_1 real    10.11
test_literal 3.4  1e1_000 real    Inf
test_literal 3.5  12_3_456.7_8_9 real 123456.789
test_literal 3.6  9_223_372_036_854_775_807 integer 9223372036854775807
test_literal 3.7  9_223_372_036_854_775_808 real 9.22337203685478e+18
test_literal 3.8  -9_223_372_036_854_775_808 integer -9223372036854775808

foreach {tn lit unrec} {
  0    123a456       123a456
  1    1_            1_
  2    1_.4          1_.4
  3    1e_4          1e_4
  4    1_e4          1_e4
  5    1.4_e4        1.4_e4
  6    1.4e+_4       1.4e
  7    1.4e-_4       1.4e
  8    1.4e4_        1.4e4_
  9    1.4_e4        1.4_e4
  10   1.4e_4        1.4e_4
  11   12__34        12__34
  12   1234_         1234_
  13   12._34        12._34
  14   12_.34        12_.34
  15   12.34_        12.34_
  16   1.0e1_______2 1.0e1_______2 
} {
  test_literal_error 4.$tn $lit $unrec
}

# dbsqlfuzz e3186a9e7826e9cd7f4085aa4452f8696485f9e1
# See tag-20240224-a and -b
#
do_catchsql_test 5.1 {
  SELECT 1 ORDER BY 2_3;
} {1 {1st ORDER BY term out of range - should be between 1 and 1}}

finish_test
