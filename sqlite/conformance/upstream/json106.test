# 2023-12-18
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
# Invariant tests for JSON built around the randomjson extension
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix json106

# These tests require virtual table "json_tree" to run.
ifcapable !vtab { finish_test ; return }

load_static_extension db randomjson
db eval {
  CREATE TEMP TABLE t1(j0,j5,p);
  CREATE TEMP TABLE kv(n,key,val);
}
unset -nocomplain ii
for {set ii 1} {$ii<=5000} {incr ii} {
  do_execsql_test $ii.1 {
    DELETE FROM t1;
    INSERT INTO t1(j0,j5) VALUES(random_json($ii),random_json5($ii));
    SELECT json_valid(j0), json_valid(j5,2) FROM t1;
  } {1 1}
  do_execsql_test $ii.2 {
    SELECT count(*)
      FROM t1, json_tree(j0) AS rt
     WHERE rt.type NOT IN ('object','array')
       AND rt.atom IS NOT (j0 ->> rt.fullkey);
  } 0
  do_execsql_test $ii.3 {
    SELECT count(*)
      FROM t1, json_tree(j5) AS rt
     WHERE rt.type NOT IN ('object','array')
       AND rt.atom IS NOT (j0 ->> rt.fullkey);
  } 0
  do_execsql_test $ii.4 {
    DELETE FROM kv;
    INSERT INTO kv
      SELECT rt.rowid, rt.fullkey, rt.atom
        FROM t1, json_tree(j0) AS rt
       WHERE rt.type NOT IN ('object','array');
  }
  do_execsql_test $ii.5 {
    SELECT count(*)
      FROM t1, kv
     WHERE key NOT LIKE '%]'
       AND json_remove(j5,key)->>key IS NOT NULL
  } 0
  do_execsql_test $ii.6 {
    SELECT count(*)
      FROM t1, kv
     WHERE key NOT LIKE '%]'
       AND json_insert(json_remove(j5,key),key,val)->>key IS NOT val
  } 0
  do_execsql_test $ii.7 {
    UPDATE t1 SET p=json_patch(j0,j5);
    SELECT count(*)
      FROM t1, kv
     WHERE p->>key IS NOT val
  } 0
  do_execsql_test $ii.8 {
    SELECT j0 FROM t1 WHERE json(j0)!=json(json_pretty(j0));
  } {}
  do_execsql_test $ii.9 {
    SELECT j5 FROM t1 WHERE json(j5)!=json(json_pretty(j5));
  } {}
}


finish_test
