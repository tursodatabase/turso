# 2024-08-19
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
# https://sqlite.org/forum/forumpost/eaa0a09786c6368b
#
# Apparently SQLite has been miscomputing leap-year dates before
# the year 0400.
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl

# Skip this whole file if date and time functions are omitted
# at compile-time
#
ifcapable {!datetime} {
  finish_test
  return
}

# Data sources:
#   1-10  https://ssd.jpl.nasa.gov/tools/jdc/#/cd
#   11    Jean Meeus, Astronomical Algorithms, ISBN 0-943396-61-1, p.59
#   12    https://en.wikipedia.org/wiki/Julian_day
#   
# ID YEAR MONTH DAY JD
set date5data {
   1 2024     2  29 2460369.5
   2 2024     3   1 2460370.5
   3 2023     2  28 2460003.5
   4 2023     3   1 2460004.5
   5 2000     2  29 2451603.5
   6 2000     3   1 2451604.5
   7 1900     2  28 2415078.5
   8 1900     3   1 2415079.5
   9 1712     2  29 2346413.5
  10 1712     3   1 2346414.5
  11 1977     4  26 2443259.5
  12 2013     1   1 2456293.5
}

foreach {id y m d jd} $date5data {
  set date [format %04d-%02d-%02d $y $m $d]
  do_execsql_test date5-jd$jd {
    SELECT date($::jd);
  } $date
  do_execsql_test date5-cal/$date {
    SELECT julianday($::date);
  } $jd
  for {set i 1} {$y+400*$i<=9999} {incr i} {
    set y2 [expr {$y+400*$i}]
    set date2 [format %04d-%02d-%02d $y2 $m $d]
    set jd2 [expr {$jd+146097*$i}]
    do_execsql_test date5-jd$jd2 {
      SELECT date($::jd2);
    } $date2
    do_execsql_test date5-cal/$date2 {
      SELECT julianday($::date2);
    } $jd2
  }
  for {set i 1} {$y-400*$i>=-4712} {incr i} {
    set y2 [expr {$y-400*$i}]
    if {$y2<0} {
      set date2 [format -%04d-%02d-%02d [expr {-$y2}] $m $d]
    } else {
      set date2 [format %04d-%02d-%02d $y2 $m $d]
    }
    set jd2 [expr {$jd-146097*$i}]
    do_execsql_test date5-jd$jd2 {
      SELECT date($::jd2);
    } $date2
    do_execsql_test date5-cal/$date2 {
      SELECT julianday($::date2);
    } $jd2
  }
}

finish_test
