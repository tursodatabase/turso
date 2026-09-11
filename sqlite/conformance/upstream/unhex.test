# 2023 January 23
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
source [file join $testdir tester.tcl]

set testprefix unhex


foreach {tn hex} {
  1  0000
  2  FFFF
  3  0123456789ABCDEF
} {
  do_execsql_test 1.$tn.1 {
    SELECT hex( unhex( $hex ) );
  } $hex

  do_execsql_test 1.$tn.2 {
    SELECT hex( unhex( lower( $hex ) ) );
  } $hex
}

do_execsql_test 2.0 {
  SELECT typeof( unhex('') ), length( unhex('') );
} {blob 0}

foreach {tn hex} {
  1  ABC
  2  hello
  3  123456x7
  4  0xff
} {
  do_execsql_test 2.$tn {
    SELECT unhex( $hex ) IS NULL;
  } 1
}

do_catchsql_test 3.0 {
  SELECT unhex();
} {1 {wrong number of arguments to function unhex()}}
do_catchsql_test 3.1 {
  SELECT unhex('ABCD', '1234', '');
} {1 {wrong number of arguments to function unhex()}}

#--------------------------------------------------------------------------
# Test the 2-argument version.
#
# Zap global x array set in some previous test.
if {[array exists x]} {array unset x}
foreach {tn hex} {
  1 "FFFF  ABCD"
  2 "FFFF ABCD"
  3 "FFFFABCD "
  4 " FFFFABCD"
  5 "--FFFF AB- -CD- "
  6 "--"
  7 " --"
} {
  set out ""
  foreach x [split $hex ""] {
    if {[string is xdigit $x]} { append out $x }
  }

  do_execsql_test 5.$tn.1 {
    SELECT hex( unhex($hex, ' -') );
  } [list $out]
}

do_execsql_test 6.0 {
  SELECT typeof( unhex(' ', ' -') ), length( unhex('-', ' -') );
} {blob 0}


do_execsql_test 6.1 "
  SELECT hex( unhex('\u0E01ABCD\u0E02', '\uE01\uE02') )
" {ABCD}
do_execsql_test 6.2 "
  SELECT typeof( unhex('\u0E01ABCD\u0E02', '\uE03\uE02') )
" {null}
do_execsql_test 6.3 "
  SELECT hex( unhex('\u0E01AB CD\uE02\uE01', '\uE01 \uE02') )
" {ABCD}

#--------------------------------------------------------------------------
# Test that if either argument is NULL, the returned value is also NULL.
#
do_execsql_test 6.4.1 { SELECT typeof(unhex(NULL)) } {null}
do_execsql_test 6.4.2 { SELECT typeof(unhex(NULL, ' ')) } {null}
do_execsql_test 6.4.3 { SELECT typeof(unhex('1234', NULL)) } {null}


finish_test
