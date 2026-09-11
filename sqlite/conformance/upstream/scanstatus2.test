# 2022 December 5
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

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix scanstatus2

ifcapable !scanstatus {
  finish_test
  return
}

sqlite3_db_config db STMT_SCANSTATUS 1

do_execsql_test 1.0 {
  CREATE TABLE t1(a, b);
  CREATE TABLE t2(x, y);
  INSERT INTO t1 VALUES(1, 2);
  INSERT INTO t1 VALUES(3, 4);
  INSERT INTO t2 VALUES('a', 'b');
  INSERT INTO t2 VALUES('c', 'd');
  INSERT INTO t2 VALUES('e', 'f');
}

proc do_zexplain_test {v2 tn sql res} {
  db eval $sql
  set stmt [db version -last-stmt-ptr]
  set idx 0
  set ret [list]

  set cmd sqlite3_stmt_scanstatus
  set f [list]
  if {$v2} { lappend f complex }

  while {1} {
    set r [sqlite3_stmt_scanstatus -flags $f $stmt $idx]
    if {[llength $r]==0} break
    lappend ret [dict get $r zExplain]
    incr idx
  }
  uplevel [list do_test $tn [list set {} $ret] [list {*}$res]]
}

proc get_cycles {stmt} {
  set r [sqlite3_stmt_scanstatus $stmt -1]
  dict get $r nCycle
}

proc foreach_scan {varname stmt body {debug 0}} {
  upvar $varname var
  for {set ii 0} {1} {incr ii} {
    set f "complex"
    if {$debug} { set f "complex debug" }
    set r [sqlite3_stmt_scanstatus -flags $f $stmt $ii]
    if {[llength $r]==0} break
    array set var $r
    uplevel $body
  }
}

proc get_eqp_graph {stmt iPar nIndent} {
  set res ""
  foreach_scan A $stmt {
    if {$A(iParentId)==$iPar} {
      set txt $A(zExplain)
      if {$A(nCycle)>=0} {
        append txt " (nCycle=$A(nCycle))"
      }
      append res "[string repeat - $nIndent]$txt\n"
      append res [get_eqp_graph $stmt $A(iSelectId) [expr $nIndent+2]]
    }
  }
  set res
}

proc get_graph {stmt} {
  set nCycle [get_cycles $stmt]
  set res "QUERY (nCycle=$nCycle)\n"
  append res [get_eqp_graph $stmt 0 2]
}

proc do_graph_test {tn sql res} {
  db eval $sql
  set stmt [db version -last-stmt-ptr]
  set graph [string trim [get_graph $stmt]]

  set graph [regsub -all {nCycle=[0-9]+} $graph nCycle=nnn]
  uplevel [list do_test $tn [list set {} $graph] [string trim $res]]
}

proc puts_graph {sql} {
  db eval $sql
  set stmt [db version -last-stmt-ptr]
  puts [string trim [get_graph $stmt]]
}

proc puts_debug_info {sql} {
  db eval $sql
  set stmt [db version -last-stmt-ptr]
  foreach_scan X $stmt {
    puts -nonewline "$X(debug_explain) $X(zExplain): "
    puts -nonewline "loop=$X(debug_loop) visit=$X(debug_visit) "
    puts            "csr=$X(debug_csr) range=$X(debug_range)"
  } 1
}

do_zexplain_test 0 1.1 {
  SELECT (SELECT a FROM t1 WHERE b=x) FROM t2 WHERE y=2
} {
  {SCAN t2}
  {SCAN t1}
}
do_zexplain_test 1 1.2 {
  SELECT (SELECT a FROM t1 WHERE b=x) FROM t2 WHERE y=2
} {
  {SCAN t2}
  {CORRELATED SCALAR SUBQUERY 1}
  {SCAN t1}
}

do_graph_test 1.3 {
  SELECT (SELECT a FROM t1 WHERE b=x) FROM t2 WHERE y=2
} {
QUERY (nCycle=nnn)
--SCAN t2 (nCycle=nnn)
--CORRELATED SCALAR SUBQUERY 1 (nCycle=nnn)
----SCAN t1 (nCycle=nnn)
}

do_graph_test 1.4 {
  WITH v2(x,y) AS MATERIALIZED (
    SELECT x,y FROM t2
  )
  SELECT * FROM t1, v2 ORDER BY y;
} {
QUERY (nCycle=nnn)
--MATERIALIZE v2 (nCycle=nnn)
----SCAN t2 (nCycle=nnn)
--SCAN t1 (nCycle=nnn)
--SCAN v2 (nCycle=nnn)
--USE TEMP B-TREE FOR ORDER BY (nCycle=nnn)
}

#-------------------------------------------------------------------------
ifcapable fts5 {
  reset_db
  sqlite3_db_config db STMT_SCANSTATUS 1
  do_execsql_test 2.0 {
    CREATE VIRTUAL TABLE ft USING fts5(a);
    INSERT INTO ft VALUES('abc');
    INSERT INTO ft VALUES('def');
    INSERT INTO ft VALUES('ghi');
  }
  
  do_graph_test 2.1 {
    SELECT * FROM ft('def')
  } {
QUERY (nCycle=nnn)
--SCAN ft VIRTUAL TABLE INDEX 0:M1 (nCycle=nnn)
  }
}

#-------------------------------------------------------------------------
reset_db
sqlite3_db_config db STMT_SCANSTATUS 1
do_execsql_test 3.0 {
  CREATE TABLE x1(a, b);
  CREATE TABLE x2(c, d);

  WITH s(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i<1000)
  INSERT INTO x1 SELECT i, i FROM s;
  INSERT INTO x2 SELECT a, b FROM x1;
}

do_graph_test 2.1 {
  SELECT * FROM x1, x2 WHERE c=+a;
} {
QUERY (nCycle=nnn)
--SCAN x1 (nCycle=nnn)
--CREATE AUTOMATIC INDEX ON x2(c, d) (nCycle=nnn)
--BLOOM FILTER ON x2 (c=?)
--SEARCH x2 USING AUTOMATIC COVERING INDEX (c=?) (nCycle=nnn)
}

#-------------------------------------------------------------------------
reset_db
sqlite3_db_config db STMT_SCANSTATUS 1
do_execsql_test 4.0 {
  CREATE TABLE rt1 (id INTEGER PRIMARY KEY, x1, x2);
  CREATE TABLE rt2 (id, x1, x2);
}

do_graph_test 4.1 {
  SELECT * FROM rt1, rt2 WHERE rt1.id%2 AND rt2.x1=rt1.x1;
} {
QUERY (nCycle=nnn)
--SCAN rt1 (nCycle=nnn)
--CREATE AUTOMATIC INDEX ON rt2(x1, id, x2) (nCycle=nnn)
--BLOOM FILTER ON rt2 (x1=?)
--SEARCH rt2 USING AUTOMATIC COVERING INDEX (x1=?) (nCycle=nnn)
}

do_graph_test 4.2 {
  SELECT rt2.id FROM rt1, rt2 WHERE rt1.id%2 AND rt2.x1=rt1.x1;
} {
QUERY (nCycle=nnn)
--SCAN rt1 (nCycle=nnn)
--CREATE AUTOMATIC INDEX ON rt2(x1, id) (nCycle=nnn)
--BLOOM FILTER ON rt2 (x1=?)
--SEARCH rt2 USING AUTOMATIC COVERING INDEX (x1=?) (nCycle=nnn)
}

do_graph_test 4.3 {
  SELECT rt2.id FROM rt1, rt2 WHERE rt1.id%2 AND (rt2.x1+1)=(rt1.x1+1);
} {
QUERY (nCycle=nnn)
--SCAN rt1 (nCycle=nnn)
--SCAN rt2 (nCycle=nnn)
}

do_graph_test 4.4 {
  SELECT rt2.id FROM rt1, rt2 WHERE rt1.id%2 AND rt2.x1=(rt1.x1+1) AND rt2.id>5;
} {
QUERY (nCycle=nnn)
--SCAN rt1 (nCycle=nnn)
--CREATE AUTOMATIC INDEX ON rt2(x1, id) WHERE <expr> (nCycle=nnn)
--BLOOM FILTER ON rt2 (x1=?)
--SEARCH rt2 USING AUTOMATIC PARTIAL COVERING INDEX (x1=?) (nCycle=nnn)
}

do_graph_test 4.5 {
  SELECT v1.cnt FROM rt1, (
    SELECT count(*) AS cnt, rt2.x1 AS x1 FROM rt2 GROUP BY x1
  ) AS v1 WHERE rt1.x1=v1.x1
} {
QUERY (nCycle=nnn)
--CO-ROUTINE v1
----SCAN rt2 (nCycle=nnn)
----USE TEMP B-TREE FOR GROUP BY (nCycle=nnn)
--SCAN rt1 (nCycle=nnn)
--CREATE AUTOMATIC INDEX ON v1(x1, cnt) (nCycle=nnn)
--BLOOM FILTER ON v1 (x1=?)
--SEARCH v1 USING AUTOMATIC COVERING INDEX (x1=?) (nCycle=nnn)
}

#-------------------------------------------------------------------------
reset_db

ifcapable trace { 
  do_execsql_test 5.0 {
    CREATE TABLE t1(x, y);
    CREATE TRIGGER tr1 AFTER DELETE ON t1 BEGIN
      SELECT 1;
    END;
    INSERT INTO t1 VALUES(1, 2);
  }
  
  proc trace {stmt sql} {
    array set A [sqlite3_stmt_scanstatus -flags complex [format %llx $stmt] 0]
    lappend ::trace_explain $A(zExplain)
  }
  db trace_v2 trace 
  
  set ::trace_explain [list]
  do_execsql_test 5.1 {
    DELETE FROM t1 WHERE x=1;
  }
  
  do_test 5.2 {
    set ::trace_explain
  } {{SCAN t1} {SCAN t1} {SCAN t1}}
}

#-------------------------------------------------------------------------
reset_db
sqlite3_db_config db STMT_SCANSTATUS 1

do_execsql_test 6.0 {
  CREATE TABLE t1(a, b);
  INSERT INTO t1 VALUES(1, 'one');
  INSERT INTO t1 VALUES(2, 'two');
  INSERT INTO t1 VALUES(3, 'three');
  INSERT INTO t1 VALUES(4, 'four');
  INSERT INTO t1 VALUES(5, 'five');
  INSERT INTO t1 VALUES(6, 'six');
  INSERT INTO t1 VALUES(7, 'seven');
  INSERT INTO t1 VALUES(8, 'eight');
}

do_graph_test 6.1 {
  SELECT (a % 2), group_concat(b) FROM t1 GROUP BY 1
} {
QUERY (nCycle=nnn)
--SCAN t1 (nCycle=nnn)
--USE TEMP B-TREE FOR GROUP BY (nCycle=nnn)
}

set sql {
  WITH xy(x, y) AS ( SELECT (a % 2), group_concat(b) FROM t1 GROUP BY 1)
  SELECT * FROM xy WHERE x=1
}
do_graph_test 6.2 $sql {
QUERY (nCycle=nnn)
--CO-ROUTINE xy
----SCAN t1 (nCycle=nnn)
----USE TEMP B-TREE FOR GROUP BY (nCycle=nnn)
--SCAN xy (nCycle=nnn)
}

do_graph_test 6.3 {
  WITH xy(x, y) AS ( SELECT (a % 2), group_concat(b) FROM t1 GROUP BY 1)
  SELECT * FROM xy, xy AS xy2
} {
QUERY (nCycle=nnn)
--MATERIALIZE xy (nCycle=nnn)
----SCAN t1 (nCycle=nnn)
----USE TEMP B-TREE FOR GROUP BY (nCycle=nnn)
--SCAN xy (nCycle=nnn)
--SCAN xy2 (nCycle=nnn)
}

#-------------------------------------------------------------------------
reset_db

# Check that an OOB parameter (45) does not cause asan or valgrind errors.
#
do_test 7.0 {
  db eval {SELECT * FROM sqlite_schema}
  set stmt [db version -last-stmt-ptr]
  sqlite3_stmt_scanstatus -flags complex $stmt 1000000
} {}

#explain_i { SELECT (a % 2), group_concat(b) FROM t1 GROUP BY 1 }
#puts_debug_info { SELECT (a % 2), group_concat(b) FROM t1 GROUP BY 1 }

finish_test
