# 2016 July 14
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
# This file implements regression tests for SQLite library.  The focus of
# this test file is the "sqlite3_trace_v2()" and "sqlite3_expanded_sql()"
# APIs.
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
ifcapable !trace { finish_test ; return }
set ::testprefix trace3

proc trace_v2_error { args } {
  lappend ::stmtlist(error) [string trim $args]
  error "trace error"; # this will be ignored.
}
proc trace_v2_record { args } {
  lappend ::stmtlist(record) [string trim $args]
}
proc trace_v2_nop { args } {}; # do nothing.

do_test trace3-1.0 {
  execsql {
    CREATE TABLE t1(a,b);
    INSERT INTO t1 VALUES(1,NULL);
    INSERT INTO t1 VALUES(2,-1);
    INSERT INTO t1 VALUES(3,0);
    INSERT INTO t1 VALUES(4,1);
    INSERT INTO t1 VALUES(5,-2147483648);
    INSERT INTO t1 VALUES(6,2147483647);
    INSERT INTO t1 VALUES(7,-9223372036854775808);
    INSERT INTO t1 VALUES(8,9223372036854775807);
    INSERT INTO t1 VALUES(9,-1.0);
    INSERT INTO t1 VALUES(10,0.0);
    INSERT INTO t1 VALUES(11,1.0);
    INSERT INTO t1 VALUES(12,'');
    INSERT INTO t1 VALUES(13,'1');
    INSERT INTO t1 VALUES(14,'one');
    INSERT INTO t1 VALUES(15,x'abcd0123');
    INSERT INTO t1 VALUES(16,x'4567cdef');
  }
} {}

do_test trace3-1.1 {
  set rc [catch {db trace_v2 1 2 3} msg]
  lappend rc $msg
} {1 {wrong # args: should be "db trace_v2 ?CALLBACK? ?MASK?"}}
do_test trace3-1.2 {
  set rc [catch {db trace_v2 1 bad} msg]
  lappend rc $msg
} {1 {bad trace type "bad": must be statement, profile, row, or close}}

do_test trace3-2.1 {
  db trace_v2 trace_v2_nop
  db trace_v2
} {trace_v2_nop}

do_test trace3-3.1 {
  unset -nocomplain ::stmtlist
  db trace_v2 trace_v2_nop
  execsql {
    SELECT a, b FROM t1 ORDER BY a;
  }
  array get ::stmtlist
} {}
do_test trace3-3.2 {
  set ::stmtlist(error) {}
  db trace_v2 trace_v2_error
  execsql {
    SELECT a, b FROM t1 ORDER BY a;
  }
  set ::stmtlist(error)
} {/^\{-?\d+ \{SELECT a, b FROM t1 ORDER BY a;\}\}$/}
do_test trace3-3.3 {
  set ::stmtlist(record) {}
  db trace_v2 trace_v2_record
  execsql {
    SELECT a, b FROM t1 ORDER BY a;
  }
  set ::stmtlist(record)
} {/^\{-?\d+ \{SELECT a, b FROM t1 ORDER BY a;\}\}$/}
do_test trace3-3.4 {
  set ::stmtlist(record) {}
  db trace_v2 trace_v2_record statement
  execsql {
    SELECT a, b FROM t1 ORDER BY a;
  }
  set ::stmtlist(record)
} {/^\{-?\d+ \{SELECT a, b FROM t1 ORDER BY a;\}\}$/}
do_test trace3-3.5 {
  set ::stmtlist(record) {}
  db trace_v2 trace_v2_record 1
  execsql {
    SELECT a, b FROM t1 ORDER BY a;
  }
  set ::stmtlist(record)
} {/^\{-?\d+ \{SELECT a, b FROM t1 ORDER BY a;\}\}$/}

do_test trace3-4.1 {
  set ::stmtlist(record) {}
  db trace_v2 trace_v2_record profile
  execsql {
    SELECT a, b FROM t1 ORDER BY a;
  }
  set ::stmtlist(record)
} {/^\{-?\d+ -?\d+\}$/}
do_test trace3-4.2 {
  set ::stmtlist(record) {}
  db trace_v2 trace_v2_record 2
  execsql {
    SELECT a, b FROM t1 ORDER BY a;
  }
  set ::stmtlist(record)
} {/^\{-?\d+ -?\d+\}$/}

do_test trace3-4.3 {
  set ::stmtlist(record) {}
  db trace_v2 trace_v2_record profile
  execsql {
    SELECT a, b FROM t1 ORDER BY a;
  }
  set stmt [lindex [lindex $::stmtlist(record) 0] 0]
  set ns [lindex [lindex $::stmtlist(record) 0] 1]
  list $stmt [expr {$ns >= 0 && $ns <= 9999999}]; # less than 0.010 seconds
} {/^-?\d+ 1$/}
do_test trace3-4.4 {
  set cnt 0
  while {1} {
    set ::stmtlist(record) {}
    db trace_v2 trace_v2_record 2
    execsql {
      SELECT a, b FROM t1 ORDER BY a;
    }
    set stmt [lindex [lindex $::stmtlist(record) 0] 0]
    set ns [lindex [lindex $::stmtlist(record) 0] 1]
    if {$ns<0 || $ns>9999999} {  #less than 0.010 seconds
      incr cnt
      if {$cnt>3} {
        set res "time out of bounds.  Expected less than 99999999.  Got $ns"
        break
      }
    } else {
      set res 1
      break
    }
  }
  list $stmt $res
} {/^-?\d+ 1$/}

do_test trace3-5.1 {
  set ::stmtlist(record) {}
  db trace_v2 trace_v2_record row
  execsql {
    SELECT a, b FROM t1 ORDER BY a;
  }
  set ::stmtlist(record)
} "/^[string trim [string repeat {-?\d+ } 16]]\$/"
do_test trace3-5.2 {
  set ::stmtlist(record) {}
  db trace_v2 trace_v2_record 4
  execsql {
    SELECT a, b FROM t1 ORDER BY a;
  }
  set ::stmtlist(record)
} "/^[string trim [string repeat {-?\d+ } 16]]\$/"

do_test trace3-6.1 {
  set ::stmtlist(record) {}
  db trace_v2 trace_v2_record {profile row}
  execsql {
    SELECT a, b FROM t1 ORDER BY a;
  }
  set ::stmtlist(record)
} "/^[string trim [string repeat {-?\d+ } 16]] \\\{-?\\d+ -?\\d+\\\}\$/"
do_test trace3-6.2 {
  set ::stmtlist(record) {}
  db trace_v2 trace_v2_record {statement profile row}
  execsql {
    SELECT a, b FROM t1 ORDER BY a;
  }
  set ::stmtlist(record)
} "/^\\\{-?\\d+ \\\{SELECT a, b FROM t1 ORDER BY a;\\\}\\\} [string trim \
[string repeat {-?\d+ } 16]] \\\{-?\\d+ -?\\d+\\\}\$/"

do_test trace3-7.1 {
  set DB [sqlite3_connection_pointer db]

  set STMT [sqlite3_prepare_v2 $DB \
      "SELECT a, b FROM t1 WHERE b = ? ORDER BY a;" -1 TAIL]
} {/^[0-9A-Fa-f]+$/}

do_test trace3-8.1 {
  list [sqlite3_bind_null $STMT 1] [sqlite3_expanded_sql $STMT]
} {{} {SELECT a, b FROM t1 WHERE b = NULL ORDER BY a;}}
do_test trace3-8.2 {
  list [sqlite3_bind_int $STMT 1 123] [sqlite3_expanded_sql $STMT]
} {{} {SELECT a, b FROM t1 WHERE b = 123 ORDER BY a;}}
do_test trace3-8.3 {
  list [sqlite3_bind_int64 $STMT 1 123] [sqlite3_expanded_sql $STMT]
} {{} {SELECT a, b FROM t1 WHERE b = 123 ORDER BY a;}}
do_test trace3-8.4 {
  list [sqlite3_bind_text $STMT 1 "some string" 11] \
      [sqlite3_expanded_sql $STMT]
} {{} {SELECT a, b FROM t1 WHERE b = 'some string' ORDER BY a;}}
do_test trace3-8.5 {
  list [sqlite3_bind_text $STMT 1 "some 'bad' string" 17] \
      [sqlite3_expanded_sql $STMT]
} {{} {SELECT a, b FROM t1 WHERE b = 'some ''bad'' string' ORDER BY a;}}
do_test trace3-8.6 {
  list [sqlite3_bind_double $STMT 1 123] [sqlite3_expanded_sql $STMT]
} {{} {SELECT a, b FROM t1 WHERE b = 123.0 ORDER BY a;}}
do_test trace3-8.7 {
  list [sqlite3_bind_text16 $STMT 1 \
      [encoding convertto unicode hi\000yall\000] 16] \
      [sqlite3_expanded_sql $STMT]
} {{} {SELECT a, b FROM t1 WHERE b = 'hi' ORDER BY a;}}
do_test trace3-8.8 {
  list [sqlite3_bind_blob $STMT 1 "\x12\x34\x56" 3] \
      [sqlite3_expanded_sql $STMT]
} {{} {SELECT a, b FROM t1 WHERE b = x'123456' ORDER BY a;}}
do_test trace3-8.9 {
  list [sqlite3_bind_blob $STMT 1 "\xAB\xCD\xEF" 3] \
      [sqlite3_expanded_sql $STMT]
} {{} {SELECT a, b FROM t1 WHERE b = x'abcdef' ORDER BY a;}}

do_test trace3-9.1 {
  sqlite3_finalize $STMT
} {SQLITE_OK}

do_test trace3-10.1 {
  db trace_v2 ""
  db trace_v2
} {}
do_test trace3-10.2 {
  unset -nocomplain ::stmtlist
  db trace_v2 "" {statement profile row}
  execsql {
    SELECT a, b FROM t1 ORDER BY a;
  }
  array get ::stmtlist
} {}

do_test trace3-11.1 {
  set ::stmtlist(record) {}
  db trace_v2 trace_v2_record close
  db close
  set ::stmtlist(record)
} {/^-?\d+$/}

reset_db

do_test trace3-11.2 {
  set ::stmtlist(record) {}
  db trace_v2 trace_v2_record 8
  db close
  set ::stmtlist(record)
} {/^-?\d+$/}

#-------------------------------------------------------------------------
reset_db
do_test 12.1.0 {
  set ::STMT [sqlite3_prepare_v2 $DB \
    "SELECT ?1 || ?1 || ?1 || ?2 || ?3 || ?4 || ? || ?1 || ?" -1 TAIL
  ]
  sqlite3_bind_parameter_count $::STMT
} {6}

do_test 12.1.1 {
  sqlite3_bind_text $STMT 1 "A" 1
  sqlite3_bind_text $STMT 2 "B" 1
  sqlite3_bind_text $STMT 3 "C" 1
  sqlite3_bind_text $STMT 4 "D" 1
  sqlite3_bind_text $STMT 5 "E" 1
  sqlite3_bind_text $STMT 6 "F" 1
  sqlite3_expanded_sql $STMT
} {SELECT 'A' || 'A' || 'A' || 'B' || 'C' || 'D' || 'E' || 'A' || 'F'}

do_test 12.1.2 {
  sqlite3_step $STMT
  sqlite3_column_text $STMT 0
} {AAABCDEAF}

do_test 12.1.3 {
  sqlite3_finalize $STMT
} {SQLITE_OK}

do_test 12.2.0 {
  execsql {
    CREATE TABLE nameFtsFuzzySearchTable(
      word, distance, langid, score, top, scope
    );
  }
  set ::STMT [sqlite3_prepare_v2 $DB {
    SELECT
      substr(word,1,length(?1)-1) AS term,
      distance,
      langid,
      score
    FROM
      nameFtsFuzzySearchTable
    WHERE
      word MATCH (?1) AND abs(?1) = abs(term)
      AND top = ?2 AND distance > ?3 AND scope = ?4 AND langid = ?
    GROUP BY term, langid 
    HAVING (1.0 - ((distance / 100.0) / CAST( length(?1) - 1 AS REAL ))) >= ?
  } -1 TAIL]
  sqlite3_bind_parameter_count $::STMT
} {6}

do_test 12.1.1 {
  sqlite3_bind_text $STMT 1 "A" 1
  sqlite3_bind_text $STMT 2 "B" 1
  sqlite3_bind_text $STMT 3 "C" 1
  sqlite3_bind_text $STMT 4 "D" 1
  sqlite3_bind_text $STMT 5 "E" 1
  sqlite3_bind_text $STMT 6 "F" 1
  sqlite3_expanded_sql $STMT
} {
    SELECT
      substr(word,1,length('A')-1) AS term,
      distance,
      langid,
      score
    FROM
      nameFtsFuzzySearchTable
    WHERE
      word MATCH ('A') AND abs('A') = abs(term)
      AND top = 'B' AND distance > 'C' AND scope = 'D' AND langid = 'E'
    GROUP BY term, langid 
    HAVING (1.0 - ((distance / 100.0) / CAST( length('A') - 1 AS REAL ))) >= 'F'
  }

do_test 12.1.2 {
  sqlite3_finalize $STMT
} {SQLITE_OK}

#-------------------------------------------------------------------------
reset_db
do_execsql_test 13.0 {
  CREATE TABLE T1(a, b);
  INSERT INTO t1 VALUES(1, 2), (3, 4);
}

proc trace_callback {args} {}
db trace_v2 trace_callback profile

do_test 13.1 {
  db eval { SELECT * FROM t1 } {
    db trace_v2 "" ""
  }
  set {} {}
} {}

finish_test
