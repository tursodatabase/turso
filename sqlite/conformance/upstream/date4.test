# 2023-08-29
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
# Test cases for the strftime() SQL function.  Comparisons against the
# C-library strftime() function.

set testdir [file dirname $argv0]
source $testdir/tester.tcl

# Skip this whole file if date and time functions are omitted
# at compile-time
#
ifcapable {!datetime} {
  finish_test
  return
}

if {$tcl_platform(os)=="Linux"} {
  if {"" eq [strftime {%P} 1]} {
    # This is probably musl libc, which does not support
    # %k, %l, %P
    set FMT {%d,%e,%F,%H,%I,%j,%m,%M,%u,%w,%W,%Y,%%,%p,%U,%V,%G,%g}
  } else {
    set FMT {%d,%e,%F,%H,%k,%I,%l,%j,%m,%M,%u,%w,%W,%Y,%%,%P,%p,%U,%V,%G,%g}
  }
} else {
  set FMT {%d,%e,%F,%H,%I,%j,%p,%R,%u,%w,%W,%%}
}
for {set i 0} {$i<=24858} {incr i} {
  set TS [expr {$i*86390}]
  do_execsql_test date4-$i {
    SELECT strftime($::FMT,$::TS,'unixepoch');
  } [list [strftime $FMT $TS]]
}

finish_test
