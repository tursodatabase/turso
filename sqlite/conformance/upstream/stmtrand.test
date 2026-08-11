# 2024-05-24
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
# Verify that the stmtrand() extension function works.
#
# Stmtrand() is a pseudo-random number generator designed for testing.
# it has the property that it returns the same sequence of pseudo-random
# numbers for each SQL statement invocation.  This makes it suitable for
# testing because the results are reproducible.
#
# The optional argument is the seed for the PRNG.  The seed is only used
# on the first invocation.  If the argument is omitted, a seed of 0 is
# used.
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix stmtrand

load_static_extension db stmtrand

do_execsql_test 1.1 {
  WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<30)
  SELECT stmtrand()%10 FROM c
} {4 1 8 3 6 1 4 9 2 1 2 1 0 1 8 1 4 7 6 1 8 7 0 3 0 9 4 5 9 9}
do_execsql_test 1.2 {
  WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<30)
  SELECT stmtrand(0)%10 FROM c
} {4 1 8 3 6 1 4 9 2 1 2 1 0 1 8 1 4 7 6 1 8 7 0 3 0 9 4 5 9 9}
do_execsql_test 1.3 {
  WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<30)
  SELECT stmtrand(1)%10 FROM c
} {3 8 9 4 7 2 3 8 5 2 7 0 3 0 7 2 5 6 1 8 5 6 7 8 7 2 9 6 8 0}
do_execsql_test 1.4 {
  WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<30)
  SELECT stmtrand(x)%10 FROM c
} {3 8 9 4 7 2 3 8 5 2 7 0 3 0 7 2 5 6 1 8 5 6 7 8 7 2 9 6 8 0}


finish_test
