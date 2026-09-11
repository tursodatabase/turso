# 2023-04-27
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
set testprefix json501

# From https://spec.json5.org/#introduction
#
#-----------------------------------------------------------------------------
# Summary of Features
# 
# The following ECMAScript 5.1 features, which are not supported in JSON, have
# been extended to JSON5. 
#
# Objects
#
#    1) Object keys may be an ECMAScript 5.1 IdentifierName.
#    2) Objects may have a single trailing comma.
#
# Arrays
#
#    3) Arrays may have a single trailing comma.
#
# Strings
#
#    4) Strings may be single quoted.
#    5) Strings may span multiple lines by escaping new line characters.
#    6) Strings may include character escapes.
#
# Numbers
#
#    7) Numbers may be hexadecimal.
#    8) Numbers may have a leading or trailing decimal point.
#    9) Numbers may be IEEE 754 positive infinity, negative infinity, and NaN.
#   10) Numbers may begin with an explicit plus sign.
#
# Comments
#
#   11) Single and multi-line comments are allowed.
#
# White Space
#
#   12) Additional white space characters are allowed.
#-----------------------------------------------------------------------------
#
# Test number in this file are of the form X.Y where X is one of the item
# numbers in the feature list above and Y is the test sequence number.
#

###############################################################################
#    1) Object keys may be an ECMAScript 5.1 IdentifierName.
do_execsql_test 1.1 {
  WITH c(x) AS (VALUES('{a:5,b:6}'))
  SELECT x->>'a', json(x), json_valid(x), NOT json_error_position(x) FROM c;
} {5 {{"a":5,"b":6}} 0 1}
do_execsql_test 1.2 {
  SELECT '[7,null,{a:5,b:6},[8,9]]'->>'$[2].b';
} {6}
do_execsql_test 1.3 {
  SELECT '{ $123 : 789 }'->>'$."$123"';
} 789
do_execsql_test 1.4 {
  SELECT '{ _123$xyz : 789 }'->>'$."_123$xyz"';
} 789
do_execsql_test 1.5 {
  SELECT '{ MNO_123$xyz : 789 }'->>'$."MNO_123$xyz"';
} 789

do_execsql_test 1.6 {
  SELECT json('{ MNO_123$xyz : 789 }');
} [list {{"MNO_123$xyz":789}}]

do_catchsql_test 1.10 {
  SELECT json('{ MNO_123/xyz : 789 }');
} {1 {malformed JSON}}

do_execsql_test 1.11 {
  SELECT '{ MNO_123æxyz : 789 }'->>'MNO_123æxyz';
} {789}

###############################################################################
#    2) Objects may have a single trailing comma.

do_execsql_test 2.1 {
  WITH c(x) AS (VALUES('{"a":5, "b":6, }'))
  SELECT x->>'b', json(x), json_valid(x), NOT json_error_position(x) FROM c;
} {6 {{"a":5,"b":6}} 0 1}
do_execsql_test 2.2 {
  SELECT '{a:5, b:6 , }'->>'b';
} 6
do_catchsql_test 2.3 {
  SELECT '{a:5, b:6 ,, }'->>'b';
} {1 {malformed JSON}}
do_catchsql_test 2.4 {
  SELECT '{a:5, b:6, ,}'->>'b';
} {1 {malformed JSON}}

###############################################################################
#    3) Arrays may have a single trailing comma.

do_execsql_test 3.1 {
  WITH c(x) AS (VALUES('[5, 6,]'))
  SELECT x->>1, json(x), json_valid(x), NOT json_error_position(x) FROM c;
} {6 {[5,6]} 0 1}
do_execsql_test 3.2 {
  SELECT '[5, 6 , ]'->>1;
} 6
do_catchsql_test 3.3 {
  SELECT '[5, 6,,]'->>1;
} {1 {malformed JSON}}
do_catchsql_test 3.4 {
  SELECT '[5, 6 , , ]'->>1;
} {1 {malformed JSON}}

###############################################################################
#    4) Strings may be single quoted.

do_execsql_test 4.1 {
  WITH c(x) AS (VALUES('{"a": ''abcd''}'))
  SELECT x->>'a', json(x), json_valid(x), NOT json_error_position(x) FROM c;
} {abcd {{"a":"abcd"}} 0 1}
do_execsql_test 4.2 {
  SELECT '{b: 123, ''a'': ''ab\''cd''}'->>'a';
} {ab'cd}

###############################################################################
#    5) Strings may span multiple lines by escaping new line characters.

do_execsql_test 5.1 {
  WITH c(x) AS (VALUES('{a: "abc'||char(0x5c,0x0a)||'xyz"}'))
  SELECT x->>'a', json(x), json_valid(x), NOT json_error_position(x) FROM c;
} {abcxyz {{"a":"abcxyz"}} 0 1}
do_execsql_test 5.2 {
  SELECT ('{a: "abc'||char(0x5c,0x0d)||'xyz"}')->>'a';
} {abcxyz}
do_execsql_test 5.3 {
  SELECT ('{a: "abc'||char(0x5c,0x0d,0x0a)||'xyz"}')->>'a';
} {abcxyz}
do_execsql_test 5.4 {
  SELECT ('{a: "abc'||char(0x5c,0x2028)||'xyz"}')->>'a';
} {abcxyz}
do_execsql_test 5.5 {
  SELECT ('{a: "abc'||char(0x5c,0x2029)||'xyz"}')->>'a';
} {abcxyz}


###############################################################################
#    6) Strings may include character escapes.

do_execsql_test 6.1 {
  SELECT ('{a: "abc'||char(0x5c,0x27)||'xyz"}')->>'a';
} {abc'xyz}
do_execsql_test 6.2 {
  SELECT ('{a: "abc'||char(0x5c,0x22)||'xyz"}')->>'a';
} {abc"xyz}
do_execsql_test 6.3 {
  SELECT ('{a: "abc'||char(0x5c,0x5c)||'xyz"}')->>'a';
} {{abc\xyz}}
do_execsql_test 6.4 {
  SELECT hex(('{a: "abc\bxyz"}')->>'a');
} {6162630878797A}
do_execsql_test 6.5 {
  SELECT hex(('{a: "abc\f\n\r\t\vxyz"}')->>'a');
} {6162630C0A0D090B78797A}
do_execsql_test 6.6 {
  SELECT hex(('{a: "abc\0xyz"}')->>'a');
} {6162630078797A}
do_execsql_test 6.7 {
  SELECT '{a: "abc\x35\x4f\x6Exyz"}'->>'a';
} {abc5Onxyz}
do_execsql_test 6.8 {
  SELECT '{a: "\x6a\x6A\x6b\x6B\x6c\x6C\x6d\x6D\x6e\x6E\x6f\x6F"}'->>'a';
} {jjkkllmmnnoo}

###############################################################################
#    7) Numbers may be hexadecimal.

do_execsql_test 7.1 {
  SELECT '{a: 0x0}'->>'a';
} 0
do_execsql_test 7.2 {
  SELECT '{a: -0x0}'->>'a';
} 0
do_execsql_test 7.3 {
  SELECT '{a: +0x0}'->>'a';
} 0
do_execsql_test 7.4 {
  SELECT '{a: 0xabcdef}'->>'a';
} 11259375
do_execsql_test 7.5 {
  SELECT '{a: -0xaBcDeF}'->>'a';
} -11259375
do_execsql_test 7.6 {
  SELECT '{a: +0xABCDEF}'->>'a';
} 11259375

###############################################################################
#    8) Numbers may have a leading or trailing decimal point.

do_execsql_test 8.1 {
  WITH c(x) AS (VALUES('{x: 4.}')) SELECT x->>'x', json(x) FROM c;
} {4.0 {{"x":4.0}}}
do_execsql_test 8.2 {
  WITH c(x) AS (VALUES('{x: +4.}')) SELECT x->>'x', json(x) FROM c;
} {4.0 {{"x":4.0}}}
do_execsql_test 8.3 {
  WITH c(x) AS (VALUES('{x: -4.}')) SELECT x->>'x', json(x) FROM c;
} {-4.0 {{"x":-4.0}}}
do_execsql_test 8.3 {
  WITH c(x) AS (VALUES('{x: .5}')) SELECT x->>'x', json(x) FROM c;
} {0.5 {{"x":0.5}}}
do_execsql_test 8.4 {
  WITH c(x) AS (VALUES('{x: -.5}')) SELECT x->>'x', json(x) FROM c;
} {-0.5 {{"x":-0.5}}}
do_execsql_test 8.5 {
  WITH c(x) AS (VALUES('{x: +.5}')) SELECT x->>'x', json(x) FROM c;
} {0.5 {{"x":0.5}}}
do_execsql_test 8.6 {
  WITH c(x) AS (VALUES('{x: 4.e0}')) SELECT x->>'x', json(x) FROM c;
} {4.0 {{"x":4.0e0}}}
do_execsql_test 8.7 {
  WITH c(x) AS (VALUES('{x: +4.e1}')) SELECT x->>'x', json(x) FROM c;
} {40.0 {{"x":4.0e1}}}
do_execsql_test 8.8 {
  WITH c(x) AS (VALUES('{x: -4.e2}')) SELECT x->>'x', json(x) FROM c;
} {-400.0 {{"x":-4.0e2}}}
do_execsql_test 8.9 {
  WITH c(x) AS (VALUES('{x: .5e3}')) SELECT x->>'x', json(x) FROM c;
} {500.0 {{"x":0.5e3}}}
do_execsql_test 8.10 {
  WITH c(x) AS (VALUES('{x: -.5e-1}')) SELECT x->>'x', json(x) FROM c;
} {-0.05 {{"x":-0.5e-1}}}
do_execsql_test 8.11 {
  WITH c(x) AS (VALUES('{x: +.5e-2}')) SELECT x->>'x', json(x) FROM c;
} {0.005 {{"x":0.5e-2}}}


###############################################################################
#    9) Numbers may be IEEE 754 positive infinity, negative infinity, and NaN.

do_execsql_test 9.1 {
  WITH c(x) AS (VALUES('{x: +Infinity}')) SELECT x->>'x', json(x) FROM c;
} {Inf {{"x":9e999}}}
do_execsql_test 9.2 {
  WITH c(x) AS (VALUES('{x: -Infinity}')) SELECT x->>'x', json(x) FROM c;
} {-Inf {{"x":-9e999}}}
do_execsql_test 9.3 {
  WITH c(x) AS (VALUES('{x: Infinity}')) SELECT x->>'x', json(x) FROM c;
} {Inf {{"x":9e999}}}
do_execsql_test 9.4 {
  WITH c(x) AS (VALUES('{x: NaN}')) SELECT x->>'x', json(x) FROM c;
} {{} {{"x":null}}}

###############################################################################
#   10) Numbers may begin with an explicit plus sign.

do_execsql_test 10.1 {
  SELECT '{a: +123}'->'a';
} 123

###############################################################################
#   11) Single and multi-line comments are allowed.

do_execsql_test 11.1 {
  SELECT ' /* abc */ { /*def*/ aaa /* xyz */ : // to the end of line
          123 /* xyz */ , /* 123 */ }'->>'aaa';
} 123

###############################################################################
#   12) Additional white space characters are allowed.

do_execsql_test 12.1 {
  SELECT (char(0x09,0x0a,0x0b,0x0c,0x0d,0x20,0xa0,0x2028,0x2029)
          || '{a: "xyz"}')->>'a';
} xyz
do_execsql_test 12.2 {
  SELECT ('{a:' || char(0x09,0x0a,0x0b,0x0c,0x0d,0x20,0xa0,0x2028,0x2029)
          || '"xyz"}')->>'a';
} xyz
do_execsql_test 12.3 {
  SELECT (char(0x1680,0x2000,0x2001,0x2002,0x2003,0x2004,0x2005,
               0x2006,0x2007,0x2008,0x2009,0x200a,0x3000,0xfeff)
          || '{a: "xyz"}')->>'a';
} xyz
do_execsql_test 12.4 {
  SELECT ('{a: ' ||char(0x1680,0x2000,0x2001,0x2002,0x2003,0x2004,0x2005,
                        0x2006,0x2007,0x2008,0x2009,0x200a,0x3000,0xfeff)
          || ' "xyz"}')->>'a';
} xyz

# 2023-11-08 forum/forumpost/ddcad3e884
#
do_execsql_test 13.1 {
  SELECT json('{x:''a "b" c''}');
} {{{"x":"a \"b\" c"}}}

# 2024-01-31
# Allow control characters within JSON5 string literals.
#
for {set c 1} {$c<=0x1f} {incr c} {
  do_execsql_test 14.$c.1 {
    SELECT json_valid('"abc' || char($c) || 'xyz"');
  } {0}
  do_execsql_test 14.$c.2 {
    SELECT json_valid('"abc' || char($c) || 'xyz"', 2);
  } {1}
  switch $c {
    8   {set e "\\b"}
    9   {set e "\\t"}
    10  {set e "\\n"}
    12  {set e "\\f"}
    13  {set e "\\r"}
    default {set e [format "\\u00%02x" $c]}
  }
  do_execsql_test 14.$c.3 {
    SELECT json('{label:"abc' || char($c) || 'xyz"}');
  } "{{\"label\":\"abc${e}xyz\"}}"
  do_execsql_test 14.$c.4 {
    SELECT jsonb('{label:"abc' || char($c) || 'xyz"}') -> '$';
  } "{{\"label\":\"abc${e}xyz\"}}"
}


finish_test
