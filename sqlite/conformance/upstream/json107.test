# 2024-01-23
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
# Legacy JSON bug:  If the input is a BLOB that when cast into TEXT looks
# like valid JSON, then treat it as valid JSON.
#
# The original intent of the JSON functions was to raise an error on any
# BLOB input.  That intent was clearly documented, but the code failed to
# to implement it.  Subsequently, many applications began to depend on the
# incorrect behavior, especially apps that used readfile() to read JSON
# content, since readfile() returns a BLOB.  So we need to support the
# bug moving forward.
#
# The tests in this fail verify that the original buggy behavior is
# preserved.
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix json107

if {[db one {PRAGMA encoding}]!="UTF-8"} {
  # These tests only work for a UTF-8 encoding.
  finish_test
  return
}

do_execsql_test 1.1 {
  SELECT json_valid( CAST('{"a":1}' AS BLOB) );
} 1
do_execsql_test 1.1.1 {
  SELECT json_valid( CAST('{"a":1}' AS BLOB), 1);
} 1
do_execsql_test 1.1.2 {
  SELECT json_valid( CAST('{"a":1}' AS BLOB), 2);
} 1
do_execsql_test 1.1.4 {
  SELECT json_valid( CAST('{"a":1}' AS BLOB), 4);
} 0
do_execsql_test 1.1.8 {
  SELECT json_valid( CAST('{"a":1}' AS BLOB), 8);
} 0

do_execsql_test 1.2.1 {
  SELECT CAST('{"a":123}' AS blob) -> 'a';
} 123
do_execsql_test 1.2.2 {
  SELECT CAST('{"a":123}' AS blob) ->> 'a';
} 123
do_execsql_test 1.2.3 {
  SELECT json_extract(CAST('{"a":123}' AS blob), '$.a');
} 123
do_execsql_test 1.3 {
  SELECT json_insert(CAST('{"a":123}' AS blob),'$.b',456);
} {{{"a":123,"b":456}}}
do_execsql_test 1.4 {
  SELECT json_remove(CAST('{"a":123,"b":456}' AS blob),'$.a');
} {{{"b":456}}}
do_execsql_test 1.5 {
  SELECT json_set(CAST('{"a":123,"b":456}' AS blob),'$.a',789);
} {{{"a":789,"b":456}}}
do_execsql_test 1.6 {
  SELECT json_replace(CAST('{"a":123,"b":456}' AS blob),'$.a',789);
} {{{"a":789,"b":456}}}
do_execsql_test 1.7 {
  SELECT json_type(CAST('{"a":123,"b":456}' AS blob));
} object
do_execsql_test 1.8 {
  SELECT json(CAST('{"a":123,"b":456}' AS blob));
} {{{"a":123,"b":456}}}

ifcapable vtab {
  do_execsql_test 2.1 {
    SELECT key, value FROM json_tree( CAST('{"a":123,"b":456}' AS blob) )
      WHERE atom;
  } {a 123 b 456}
} 
finish_test
