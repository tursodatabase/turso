# 2021-09-29
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
# Test cases for varying separator handling by group_concat().
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix windowC

ifcapable !windowfunc {
  finish_test
  return
}

do_execsql_test 1.0 {
  CREATE TABLE x1(i INTEGER PRIMARY KEY, x);
}

foreach {tn bBlob seps} {
  1 0 {a b c def g}
  2 0 {abcdefg {} {} abcdefg}
  3 0 {a bc def ghij klmno pqrstu}
  4 1 {a bc def ghij klmno pqrstu}
  5 1 {, , , , , , , , , , , , ....... , ,}
} {
  foreach type {text blob} {
    do_test 1.$type.$tn.1 {
      execsql { DELETE FROM x1 }
      foreach s $seps {
        if {$type=="text"} {
          execsql {INSERT INTO x1 VALUES(NULL, $s)}
        } else {
          execsql {INSERT INTO x1 VALUES(NULL, CAST ($s AS blob))}
        }
      }
    } {}

    foreach {tn2 win} {
      1     "ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING"
      2     "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW"
      3     "ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING"
    } {
      do_test 1.$type.$tn.2.$tn2 {
        db eval "
          SELECT group_concat('val', x) OVER ( ORDER BY i $win ) AS val FROM x1
          " {
            if {[string range $val 0 2]!="val"
              || [string range $val end-2 end]!="val"
            } {
              error "unexpected return value: $val"
            }
          }
      } {} 
    }
  }
}

# 2021-10-12 dbsqlfuzz 6c31db077a14149a7b22a1069294bdb068be8a96
#
reset_db
do_execsql_test 2.0 {
  PRAGMA encoding=UTF16le;
  WITH separator(x) AS (VALUES(',a,'),(',bc,')),
       value(y) AS (VALUES(1),(x'5585d09013455178cd11ce4a'))
  SELECT group_concat(y,x) OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING)
  FROM separator, value;
} {{} 1 蕕郐䔓硑ᇍ䫎 1}
reset_db
do_execsql_test 2.1 {
  PRAGMA encoding=UTF16be;
  WITH separator(x) AS (VALUES(',a,'),(',bc,')),
       value(y) AS (VALUES(1),(x'5585d09013455178cd11ce4a'))
  SELECT group_concat(y,x) OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING)
  FROM separator, value;
} {{} 1 喅킐ፅ典촑칊 1}

finish_test
