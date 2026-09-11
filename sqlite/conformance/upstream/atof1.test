# 2012 June 18
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
# Tests of the sqlite3AtoF() function.
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl

set mxpow 35
expr srand(1)
for {set i 1} {$i<20000} {incr i} {
  set pow [expr {int((rand()-0.5)*$mxpow)}]
  set x [expr {pow((rand()-0.5)*2*rand(),$pow)}]
  set xf [format %.32e $x]

  # Verify that text->real conversions get exactly same ieee754 floating-
  # point value in SQLite as they do in TCL.
  #
  do_test atof1-1.$i.1 {
    set y [db eval "SELECT $xf=\$x"]
    if {!$y} {
      puts -nonewline \173[db eval "SELECT real2hex($xf), real2hex(\$x)"]\175
      db eval "SELECT $xf+0.0 AS a, \$x AS b" {
        puts [format "\n%.60e\n%.60e\n%.60e" $x $a $b]
      }
    }
    set y
  } {1}

  # Verify that round-trip real->text->real conversions using the quote()
  # function preserve the bits of the numeric value exactly.
  #
  do_test atof1-1.$i.2 {
    set y [db eval {SELECT $x=CAST(quote($x) AS real)}]
    if {!$y} {
      db eval {SELECT real2hex($x) a, real2hex(CAST(quote($x) AS real)) b} {}
      puts ""
      if {$x<0} {
        puts "[format {!SCALE: %17s 1 23456789 123456789 123456789} {}]"
      } else {
        puts "[format {!SCALE: %16s 1 23456789 123456789 123456789} {}]"
      }
      puts "!IN:    $a $xf"
      puts [format {!QUOTE: %16s %s} {} [db eval {SELECT quote($x)}]]
      db eval {SELECT CAST(quote($x) AS real) c} {}
      puts "!OUT:   $b [format %.32e $c]"
    }
    set y
  } {1}
}

# 2020-01-08 ticket 9eda2697f5cc1aba
# When running sqlite3AtoF() on a blob with an odd number of bytes using
# UTF16, ignore the last byte so that the string has an integer number of
# UTF16 code points.
#
reset_db
do_execsql_test atof1-2.10 {
  PRAGMA encoding = 'UTF16be';
  CREATE TABLE t1(a, b);
  INSERT INTO t1(rowid,a) VALUES (1,x'00'),(2,3);
  SELECT substr(a,',') is true FROM t1 ORDER BY rowid;
} {0 1}
do_execsql_test atof1-2.20 {
  SELECT substr(a,',') is true FROM t1 ORDER BY rowid DESC;
} {1 0}
do_execsql_test atof1-2.30 {
  CREATE INDEX i1 ON t1(a);
  SELECT count(*) FROM t1 WHERE substr(a,',');
} {1}
# 2020-08-27 OSSFuzz find related to the above.
do_execsql_test atof1-2.40 {
  SELECT randomblob(0) - 1;
} {-1}

# 2024-12-07 https://sqlite.org/forum/forumpost/569a7209179a7f5e
# Incorrect conversion of floating point or integer literals that
# have significant digits that begin with 1844674407370955 followed
# by more digits in the range 0592 throgh 1609.
#
do_execsql_test atof-3.1 {
  WITH RECURSIVE bigval(i,vtxt) AS (
    SELECT 0, '18446744073709550000'
    UNION ALL
    SELECT i+1, format('1844674407370955%04d',i+1) FROM bigval
     WHERE i+1<=9999
  )
  SELECT vtxt, CAST(vtxt AS REAL) FROM bigval
   WHERE CAST(vtxt AS REAL) NOT GLOB '1.8446744073709[56]*';
} {}
do_execsql_test atof-3.2 {
  WITH RECURSIVE bigval(i,vtxt) AS (
    SELECT 0, '18.446744073709550000'
    UNION ALL
    SELECT i+1, format('18.44674407370955%04d',i+1) FROM bigval
     WHERE i+1<=9999
  )
  SELECT vtxt, CAST(vtxt AS REAL) FROM bigval
   WHERE CAST(vtxt AS REAL) NOT GLOB '18.446744073709*';
} {}
do_execsql_test atof-3.3 {
  WITH RECURSIVE exp(n,v1,v2) AS (
    SELECT -200, '1.8446744073709550592e-200', '1.8446744073709551609e-200'
    UNION ALL
    SELECT n+1, ('1.8446744073709550592e'||n),('1.8446744073709551609e'||n)
      FROM exp WHERE n<200
  )
  SELECT n, v1, v2
    FROM exp
   WHERE format('%.10e',CAST(v1 AS REAL)) NOT GLOB '1.8446*'
      OR format('%.10e',CAST(v2 AS REAL)) NOT GLOB '1.8446*';
} {}


finish_test
