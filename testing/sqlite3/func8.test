# 2023-03-17
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
# Test cases for SQL functions with names that are the same as join
# keywords:  CROSS FULL INNER LEFT NATURAL OUTER RIGHT
#
set testdir [file dirname $argv0]
source $testdir/tester.tcl

proc joinx {args} {return [join $args -]}
db func cross {joinx cross}
db func full {joinx full}
db func inner {joinx inner}
db func left {joinx left}
db func natural {joinx natural}
db func outer {joinx outer}
db func right {joinx right}
do_execsql_test func8-100 {
  CREATE TABLE cross(cross,full,inner,left,natural,outer,right);
  CREATE TABLE full(cross,full,inner,left,natural,outer,right);
  CREATE TABLE inner(cross,full,inner,left,natural,outer,right);
  CREATE TABLE left(cross,full,inner,left,natural,outer,right);
  CREATE TABLE natural(cross,full,inner,left,natural,outer,right);
  CREATE TABLE outer(cross,full,inner,left,natural,outer,right);
  CREATE TABLE right(cross,full,inner,left,natural,outer,right);
  INSERT INTO cross VALUES(1,2,3,4,5,6,7);
  INSERT INTO full VALUES(1,2,3,4,5,6,7);
  INSERT INTO inner VALUES(1,2,3,4,5,6,7);
  INSERT INTO left VALUES(1,2,3,4,5,6,7);
  INSERT INTO natural VALUES(1,2,3,4,5,6,7);
  INSERT INTO outer VALUES(1,2,3,4,5,6,7);
  INSERT INTO right VALUES(1,2,3,4,5,6,7);
}
do_execsql_test func8-110 {
  SELECT cross(cross,full,inner,left,natural,outer,right) FROM cross;
} cross-1-2-3-4-5-6-7
do_execsql_test func8-120 {
  SELECT full(cross,full,inner,left,natural,outer,right) FROM full;
} full-1-2-3-4-5-6-7
do_execsql_test func8-130 {
  SELECT inner(cross,full,inner,left,natural,outer,right) FROM inner;
} inner-1-2-3-4-5-6-7
do_execsql_test func8-140 {
  SELECT left(cross,full,inner,left,natural,outer,right) FROM left;
} left-1-2-3-4-5-6-7
do_execsql_test func8-150 {
  SELECT natural(cross,full,inner,left,natural,outer,right) FROM natural;
} natural-1-2-3-4-5-6-7
do_execsql_test func8-160 {
  SELECT outer(cross,full,inner,left,natural,outer,right) FROM outer;
} outer-1-2-3-4-5-6-7
do_execsql_test func8-170 {
  SELECT right(cross,full,inner,left,natural,outer,right) FROM right;
} right-1-2-3-4-5-6-7

finish_test
