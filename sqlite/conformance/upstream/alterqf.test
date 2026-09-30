# 2021 March 16
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
# This file implements regression tests for SQLite library. This
# script focuses on testing internal function sqlite_rename_quotefix().
#


set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix alterqf

# If SQLITE_OMIT_ALTERTABLE is defined, omit this file.
ifcapable !altertable {
  finish_test
  return
}


sqlite3_test_control SQLITE_TESTCTRL_INTERNAL_FUNCTIONS db
sqlite3_db_config db SQLITE_DBCONFIG_DQS_DDL 1
sqlite3_db_config db SQLITE_DBCONFIG_DQS_DML 1

do_execsql_test 1.0 {
  CREATE TABLE t1(a, b, c);
}

foreach {tn before after} {
  1 {CREATE VIEW v1 AS SELECT "a", "b", "notacolumn!", "c" FROM t1}
    {CREATE VIEW v1 AS SELECT "a", "b", 'notacolumn!', "c" FROM t1}

  2 {CREATE VIEW v1 AS SELECT "a", "b", "not'a'column!", "c" FROM t1}
    {CREATE VIEW v1 AS SELECT "a", "b", 'not''a''column!', "c" FROM t1}

  3 {CREATE VIEW v1 AS SELECT "a", "b", "not""a""column!", "c" FROM t1}
    {CREATE VIEW v1 AS SELECT "a", "b", 'not"a"column!', "c" FROM t1}

  4 {CREATE VIEW v1 AS SELECT "val", count("b") FROM t1 GROUP BY "abc"}
    {CREATE VIEW v1 AS SELECT 'val', count("b") FROM t1 GROUP BY 'abc'}

  5 {CREATE TABLE xyz(a CHECK (a!="str"), b AS (a||"str"))}
    {CREATE TABLE xyz(a CHECK (a!='str'), b AS (a||'str'))}

  6 {CREATE INDEX i1 ON t1(a || "str", "b", "val")}
    {CREATE INDEX i1 ON t1(a || 'str', "b", 'val')}

  7 {CREATE TRIGGER tr AFTER INSERT ON t1 BEGIN SELECT "abcd"; END}
    {CREATE TRIGGER tr AFTER INSERT ON t1 BEGIN SELECT 'abcd'; END}

  8 {CREATE VIEW v1 AS SELECT "string"'alias' FROM t1}
    {CREATE VIEW v1 AS SELECT 'string' 'alias' FROM t1}

  9 {CREATE INDEX i1 ON t1(a) WHERE "b"="bb"}
    {CREATE INDEX i1 ON t1(a) WHERE "b"='bb'}

 10 {CREATE TABLE t2(abc, xyz CHECK (xyz != "123"))} 
    {CREATE TABLE t2(abc, xyz CHECK (xyz != '123'))} 

 11 {CREATE TRIGGER ott AFTER UPDATE ON t1 BEGIN 
      SELECT max("str", new."a") FROM t1 
          WHERE string_agg("b", ",") OVER (ORDER BY c||"str");
      UPDATE t1 SET c= b + "str";
      DELETE FROM t1 WHERE EXISTS (
        SELECT 1 FROM t1 AS o WHERE o."a" = "o.a" AND t1.b IN("t1.b")
      );
    END;
 } {CREATE TRIGGER ott AFTER UPDATE ON t1 BEGIN 
      SELECT max('str', new."a") FROM t1 
          WHERE string_agg("b", ',') OVER (ORDER BY c||'str');
      UPDATE t1 SET c= b + 'str';
      DELETE FROM t1 WHERE EXISTS (
        SELECT 1 FROM t1 AS o WHERE o."a" = 'o.a' AND t1.b IN('t1.b')
      );
    END;
 }

} {
  do_execsql_test 1.$tn {
    SELECT sqlite_rename_quotefix('main', $before)
  } [list $after]
}

#-------------------------------------------------------------------------
reset_db
sqlite3_db_config db SQLITE_DBCONFIG_DQS_DDL 1
sqlite3_db_config db SQLITE_DBCONFIG_DQS_DML 1
do_execsql_test 2.0 {
  CREATE TABLE x1(
      one, two, three, PRIMARY KEY(one), 
      CHECK (three!="xyz"), CHECK (two!="one")
  ) WITHOUT ROWID;
  CREATE INDEX x1i ON x1(one+"two"+"four") WHERE "five";
  CREATE TEMP TRIGGER AFTER INSERT ON x1 BEGIN
    UPDATE x1 SET two=new.three || "new" WHERE one=new.one||"";
  END;
}

do_execsql_test 2.1 {
  ALTER TABLE x1 RENAME two TO 'four';
  SELECT sql FROM sqlite_schema;
  SELECT sql FROM sqlite_temp_schema;
} {{CREATE TABLE x1(
      one, "four", three, PRIMARY KEY(one), 
      CHECK (three!='xyz'), CHECK ("four"!="one")
  ) WITHOUT ROWID}
  {CREATE INDEX x1i ON x1(one+"four"+'four') WHERE 'five'}
  {CREATE TRIGGER AFTER INSERT ON x1 BEGIN
    UPDATE x1 SET "four"=new.three || 'new' WHERE one=new.one||'';
  END}
}


finish_test
