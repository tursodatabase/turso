# 2023-04-28
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
# This file implements tests for the JSON5 enhancements to the
# JSON SQL functions extension to the SQLite library.
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix json502

ifcapable vtab {

do_execsql_test 1.1 {
  CREATE TABLE t1(x JSON);
  INSERT INTO t1(x) VALUES('{a:{b:{c:"hello",},},}');
  SELECT fullkey FROM t1, json_tree(x);
} {{$} {$.a} {$.a.b} {$.a.b.c}}

}

do_execsql_test 2.1 {
  SELECT json_error_position('{a:null,{"h":[1,[1,2,3]],"j":"abc"}:true}');
} 9
do_catchsql_test 2.2 {
  SELECT json('{a:null,{"h":[1,[1,2,3]],"j":"abc"}:true}');
} {1 {malformed JSON}}
do_catchsql_test 2.3 {
  SELECT '{a:null,{"h":[1,[1,2,3]],"j":"abc"}:true}'->'$h[#-1]';
} {1 {malformed JSON}}

# Verify that escaped label names are compared correctly.
# 
do_execsql_test 3.1 {
  SELECT '{"a\x62c":123}' ->> 'abc';
} 123
do_execsql_test 3.2 {
  SELECT '{"abc":123}' ->> 'a\x62c';
} 123

db null null
do_execsql_test 3.3 {
  DROP TABLE IF EXISTS t1;
  CREATE TABLE t1(x);
  INSERT INTO t1 VALUES(json_insert('{}','$.a\',111,'$."b\\"',222));
  INSERT INTO t1 VALUES(jsonb_insert('{}','$.a\',111,'$."b\\"',222));
  SELECT x->'$.a\', x->'$.a\\', x->'$."a\\"', x->'$."b\\"' FROM t1;
} {111 null 111 222 111 null 111 222}

do_execsql_test 3.4 {
  SELECT json_patch('{"a\x62c":123}','{"ab\x63":456}') ->> 'abc';
} 456

ifcapable vtab {
  do_execsql_test 4.1 {
    SELECT * FROM json_tree('{"\u0017":1}','$."\x17"');
  } {{\x17} 1 integer 1 1 null {$."\x17"} {$}}
}

# JSON PATH parsing bug involving backslash escapes, reported via
# private email from Florent De'Neve on 2024-09-04.
#
do_execsql_test 5.1 {
  SELECT json_extract('{"A\"Key":1}', '$.A"Key');
} 1
do_execsql_test 5.2 {
  SELECT json_extract('{"A\"Key":1}', '$."A\"Key"');
} 1
do_execsql_test 5.3 {
  SELECT JSON_SET('{}', '$."\"Key"', 1);
} {{{"\"Key":1}}}

finish_test
