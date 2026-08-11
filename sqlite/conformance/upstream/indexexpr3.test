# 2024-10-05
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
# This file implements regression tests for SQLite library.  The
# focus of this file is testing indexes on expressions.
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix indexexpr3


do_execsql_test 1.0 {
  CREATE TABLE t1(a, j);
  INSERT INTO t1 VALUES(1, '{x:"one"}');
  INSERT INTO t1 VALUES(2, '{x:"two"}');
  INSERT INTO t1 VALUES(3, '{x:"three"}');

  CREATE INDEX i1 ON t1( json_extract(j, '$.x') );
  CREATE INDEX i2 ON t1( a, json_extract(j, '$.x') );
}

proc do_hasfunction_test {tn sql res} {
  set nFunction 0
  db eval "EXPLAIN $sql" x {
    if {$x(opcode)=="Function"} {
      incr nFunction 
    }
  }

  do_execsql_test $tn "
    SELECT $nFunction;
    $sql
  " $res
}

do_hasfunction_test 1.1 {
  SELECT json_extract(j, '$.x') FROM t1 ORDER BY 1;
} {
  0 one three two
}

do_hasfunction_test 1.2 {
  SELECT json_extract(j, '$.x') FROM t1 WHERE a=2
} {
  0 two
}

do_hasfunction_test 1.3 {
  SELECT coalesce(json_extract(j, '$.x'), 'five') FROM t1 WHERE a=2
} {
  0 two
}

do_hasfunction_test 1.4 {
  SELECT json_extract(j, '$.x') || '.two' FROM t1 WHERE a=2
} {
  0 two.two
}

do_hasfunction_test 1.5 {
  SELECT json_insert( '{}', '$.y', json_extract(j, '$.x') ) FROM t1 WHERE a=2
} {
  2 {{"y":"two"}}
}

do_hasfunction_test 1.6 {
  SELECT json_insert( '{}', '$.y', coalesce( json_extract(j, '$.x'), 'five' ) )
  FROM t1 WHERE a=2
} {
  2 {{"y":"two"}}
}

#-------------------------------------------------------------------------
reset_db
do_execsql_test 2.0 {
  CREATE TABLE t1(a, b, j);
  CREATE INDEX i1 ON t1( a, json_extract(j, '$.x') );
}

do_eqp_test 2.1 {
  SELECT json_extract(j, '$.x') FROM t1 WHERE a=?
} {
  t1 USING COVERING INDEX i1
}

do_eqp_test 2.2 {
  SELECT b, json_extract(j, '$.x') FROM t1 WHERE a=?
} {
  t1 USING INDEX i1
}

do_eqp_test 2.3 {
  SELECT json_insert( '{}', json_extract(j, '$.x') ) FROM t1 WHERE a=?
} {
  t1 USING INDEX i1
}

do_eqp_test 2.4 {
  SELECT sum( json_extract(j, '$.x') ) FROM t1 WHERE a=?
} {
  t1 USING COVERING INDEX i1
}

do_eqp_test 2.5 {
  SELECT json_extract(j, '$.x'), sum( json_extract(j, '$.x') ) FROM t1 WHERE a=?
} {
  t1 USING INDEX i1
}



finish_test

