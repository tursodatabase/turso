# 2022 May 27
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

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix altertrig

# If SQLITE_OMIT_ALTERTABLE is defined, omit this file.
ifcapable !altertable {
  finish_test
  return
}

proc collapse_whitespace {in} {
  regsub -all {[ \t\n]+} [string trim $in] { }
}

proc do_whitespace_sql_test {tn sql res} {
  set got [execsql $sql]
  set wgot [list]
  set wres [list]
  foreach g $got { lappend wgot [collapse_whitespace $g] }
  foreach r $res { lappend wres [collapse_whitespace $r] }

  uplevel [list do_test $tn [list set {} $wgot] $wres]
}

do_execsql_test 1.0 {
  CREATE TABLE t1(x);
  CREATE TABLE t2(y);
  CREATE TABLE t3(z);
  CREATE TABLE t4(a);

  CREATE TRIGGER r1 INSERT ON t1 BEGIN 
    UPDATE t1 SET d='xyz' FROM t2, t3; 
  END;
}

do_whitespace_sql_test 1.1 {
  ALTER TABLE t3 RENAME TO t5;
  SELECT sql FROM sqlite_schema WHERE type='trigger';
} {{
  CREATE TRIGGER r1 INSERT ON t1 BEGIN 
    UPDATE t1 SET d='xyz' FROM t2, "t5"; 
  END
}}

do_execsql_test 1.2 {
  DROP TRIGGER r1;
  CREATE TRIGGER r1 INSERT ON t1 BEGIN 
    UPDATE t1 SET d='xyz' FROM t2, (SELECT * FROM t5); 
  END;
}

do_whitespace_sql_test 1.3 {
  ALTER TABLE t5 RENAME TO t3;
  SELECT sql FROM sqlite_schema WHERE type='trigger';
} {{
  CREATE TRIGGER r1 INSERT ON t1 BEGIN 
    UPDATE t1 SET d='xyz' FROM t2, (SELECT * FROM "t3"); 
  END
}}

foreach {tn alter update final} {
  1 {
    ALTER TABLE t3 RENAME TO t10
  } {
    UPDATE t1 SET d='xyz' FROM t2, (SELECT * FROM t3)
  } {
    UPDATE t1 SET d='xyz' FROM t2, (SELECT * FROM "t10")
  }

  2 {
    ALTER TABLE t3 RENAME TO t10
  } {
    UPDATE t1 SET a='xyz' FROM t3, (SELECT * FROM (SELECT e FROM t3))
  } {
    UPDATE t1 SET a='xyz' FROM "t10", (SELECT * FROM (SELECT e FROM "t10"))
  }

  3 {
    ALTER TABLE t3 RENAME e TO abc
  } {
    UPDATE t1 SET a='xyz' FROM t3, (SELECT * FROM (SELECT e FROM t3))
  } {
    UPDATE t1 SET a='xyz' FROM t3, (SELECT * FROM (SELECT abc FROM t3))
  }

  4 {
    ALTER TABLE t2 RENAME c TO abc
  } {
    UPDATE t1 SET a='xyz' FROM t3, (SELECT 1 FROM t2 WHERE c)
  } {
    UPDATE t1 SET a='xyz' FROM t3, (SELECT 1 FROM t2 WHERE abc)
  }

  5 {
    ALTER TABLE t2 RENAME c TO abc
  } {
    UPDATE t1 SET a=t2.c FROM t2
  } {
    UPDATE t1 SET a=t2.abc FROM t2
  }

  6 {
    ALTER TABLE t2 RENAME c TO abc
  } {
    UPDATE t1 SET a=t2.c FROM t2, t3
  } {
    UPDATE t1 SET a=t2.abc FROM t2, t3
  }

  7 {
    ALTER TABLE t4 RENAME e TO abc
  } {
    UPDATE t1 SET a=1 FROM t3 NATURAL JOIN t4 WHERE t4.e=a
  } {
    UPDATE t1 SET a=1 FROM t3 NATURAL JOIN t4 WHERE t4.abc=a
  }

  8 {
    ALTER TABLE t4 RENAME TO abc
  } {
    UPDATE t1 SET a=1 FROM t3 NATURAL JOIN t4 WHERE t4.e=a
  } {
    UPDATE t1 SET a=1 FROM t3 NATURAL JOIN "abc" WHERE "abc".e=a
  }
 
} {
  reset_db
  do_execsql_test 2.$tn.1 {
    CREATE TABLE t1(a,b);
    CREATE TABLE t2(c,d);
    CREATE TABLE t3(e,f);
    CREATE TABLE t4(e,f);
  }
  do_execsql_test 2.$tn.2 "
    CREATE TRIGGER r1 INSERT ON t1 BEGIN 
      $update;
    END
  "
  do_execsql_test 2.$tn.3 $alter

  do_whitespace_sql_test 2.$tn.4 {
    SELECT sqL FROM sqlite_schema WHERE type='trigger'
  } "{
    CREATE TRIGGER r1 INSERT ON t1 BEGIN 
      $final;
    END
  }"
}

finish_test
