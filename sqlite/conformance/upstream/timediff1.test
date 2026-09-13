# 2023-05-30
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
# focus of this file is testing date and time functions.
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

proc datetest {tnum expr result} {
  do_test timediff-$tnum [subst {
    execsql "SELECT coalesce($expr,'NULL')"
  }] [list $result]
}
set tcl_precision 15


# February overflow on a leap year
datetest 1.1 {datetime('2000-01-31','+1 month')} {2000-03-02 00:00:00}
datetest 1.2 {datetime('2004-01-29','+1 month')} {2004-02-29 00:00:00}
datetest 1.3 {datetime('2000-03-01','-1 day')}   {2000-02-29 00:00:00}
datetest 1.4 {datetime('2000-03-31','-1 month')} {2000-03-02 00:00:00}
datetest 1.5 {datetime('2000-03-30','-1 month')} {2000-03-01 00:00:00}
datetest 1.6 {datetime('2000-03-29','-1 month')} {2000-02-29 00:00:00}
datetest 1.7 {datetime('2000-03-28','-1 month')} {2000-02-28 00:00:00}
datetest 1.8 {datetime('2000-02-29','+1 year')}  {2001-03-01 00:00:00}
datetest 1.9 {datetime('2000-02-29','+4 years')} {2004-02-29 00:00:00}

datetest 1.10 {datetime('1998-11-10','+0001-03-19 12:34:56')} \
                                                 {2000-02-29 12:34:56}
datetest 1.11 {datetime('2000-01-31','+0004-01-00 12:34:56')} \
                                                 {2004-03-02 12:34:56}
datetest 1.12 {datetime('2000-01-29','+0008-01-00 12:34:56')} \
                                                 {2008-02-29 12:34:56}
datetest 1.13 {datetime('2001-03-31','-0001-01-00 06:10')} \
                                                 {2000-03-01 17:50:00}


# February overflow on a non-leap year
datetest 2.1 {datetime('2001-01-31','+1 month')} {2001-03-03 00:00:00}
datetest 2.2 {datetime('2005-01-29','+1 month')} {2005-03-01 00:00:00}
datetest 2.3 {datetime('2001-03-01','-1 day')}   {2001-02-28 00:00:00}
datetest 2.4 {datetime('2001-03-31','-1 month')} {2001-03-03 00:00:00}
datetest 2.5 {datetime('2001-03-30','-1 month')} {2001-03-02 00:00:00}
datetest 2.6 {datetime('2001-03-29','-1 month')} {2001-03-01 00:00:00}
datetest 2.7 {datetime('2001-03-28','-1 month')} {2001-02-28 00:00:00}

datetest 2.10 {datetime('1999-11-10','+0001-03-19 12:34:56')} \
                                                 {2001-03-01 12:34:56}
datetest 2.11 {datetime('2000-01-31','+0005-01-00 12:34:56')} \
                                                 {2005-03-03 12:34:56}
datetest 2.12 {datetime('2000-01-29','+0009-01-00 12:34:56')} \
                                                 {2009-03-01 12:34:56}
datetest 2.13 {datetime('2002-03-31','-0001-01-00 06:10')} \
                                                 {2001-03-02 17:50:00}

# timediff
datetest 3.1 {timediff('2000-03-02','2000-01-31')} {+0000-01-00 00:00:00.000}
datetest 3.2 {timediff('2000-01-31','2000-03-02')} {-0000-01-02 00:00:00.000}
datetest 3.3 {timediff('2000-03-02','1999-01-31')} {+0001-01-00 00:00:00.000}
datetest 3.4 {timediff('1999-01-31','2000-03-02')} {-0001-01-02 00:00:00.000}

unset -nocomplain p1
unset -nocomplain p2
set p1 {
  0   {-4713-11-24 12:00:00}
  1   {-2000-04-30 05:19:26}
  2   {0000-01-01 12:34:56}
  3   {1776-07-04 13:00:00}
  4   {1969-07-20 20:17}
  5   {2440587.5}
  6   {2000-05-29 14:26}
  7   {2023-05-29 18:11}
  8   {2050-05-29 14:26}
  9   {4796-02-29 11:23:55.46}
}
set p2 {
  A   {1066-10-14}
  B   {1900-02-28 11:00}
  C   {1900-03-01 12:00}
  D   {1904-02-29 11:25}
  E   {2000-02-29 13:00}
  E   {2000-03-01 14:00}
  F   {2001-03-31 15:15}
  G   {2002-04-01 16:59}
  H   {2003-04-30 17:00}
  I   {2004-05-01 23:59:59}
  J   {2005-06-01}
  K   {2006-06-30 01:23:45}
  L   {2007-12-31 02:00}
  M   {2008-01-01 01:59}
  N   {3152-07-04 12:00}
  P   {9999-12-31 23:59:59}
}

foreach {x1 d1} $p1 {
  foreach {x2 d2} $p2 {
    set r1 [db one {SELECT datetime($d1)}]
    do_execsql_test timediff-4-$x1$x2 {
      SELECT datetime($d2, timediff($d1,$d2));
    } [list $r1]
    set r2 [db one {SELECT datetime($d2)}]
    do_execsql_test timediff-4-$x2$x1 {
      SELECT datetime($d1, timediff($d2,$d1));
    } [list $r2]
  }
}

# Partial time-diffs as modifiers
#
datetest 5-1 {datetime('2000-01-01','+0001-02-03')} {2001-03-04 00:00:00}
datetest 5-2 {datetime('2000-01-01','+0001-02-03x')} {NULL}
datetest 5-3 {datetime('2000-01-01','+0001-11-03')} {2001-12-04 00:00:00}
datetest 5-4 {datetime('2000-01-01','+0001-12-03')} {NULL}
datetest 5-5 {datetime('2000-01-01','+0001-02-30')} {2001-03-31 00:00:00}
datetest 5-6 {datetime('2000-01-01','+0001-02-31')} {NULL}
datetest 5-7 {datetime('2000-01-01','+0001-02-03 0')} {NULL}
datetest 5-8 {datetime('2000-01-01','+0001-02-03 01')} {NULL}
datetest 5-9 {datetime('2000-01-01','+0001-02-03 01:')} {NULL}
datetest 5-10 {datetime('2000-01-01','+0001-02-03 01:0')} {NULL}
datetest 5-11 {datetime('2000-01-01','+0001-02-03 01:02')} {2001-03-04 01:02:00}
datetest 5-12 {datetime('2000-01-01','+0001-02-03 01:02:')} {NULL}
datetest 5-13 {datetime('2000-01-01','+0001-02-03 01:02:0')} {NULL}
datetest 5-14 {datetime('2000-01-01','+0001-02-03 01:02:03')} \
                                                   {2001-03-04 01:02:03}
datetest 5-15 {datetime('2000-01-01','+0001-02-03 01:02:03.')} NULL
datetest 5-16 {datetime('2000-01-01','+0001-02-03 01:02:03.5')} \
                                                   {2001-03-04 01:02:03}
datetest 5-17 {datetime('2000-01-01','+0001-02-03 01:02:03.50')} \
                                                   {2001-03-04 01:02:03}
datetest 5-18 {datetime('2000-01-01','+0001-02-03 01:02:03.500')} \
                                                   {2001-03-04 01:02:03}
datetest 5-19 {datetime('2000-01-01','+0001-02-03 01:02:03.500x')} {NULL}
datetest 5-20 {datetime('2000-01-01','+0001-02-03 01:02:03.500 x')} {NULL}

unset -nocomplain p1
unset -nocomplain p2
set p1 {
  a   {2000-01-01 00:00:00}
  b   {2000-01-31 23:59:59}
  c   {2000-02-01 00:00:00}
  d   {2000-02-29 23:59:59}
  e   {2000-03-01 00:00:00}
  f   {2000-03-31 23:59:59}
  g   {2000-04-01 00:00:00}
  h   {2000-04-30 23:59:59}
  i   {2000-05-01 00:00:00}
  j   {2000-05-31 23:59:59}
  k   {2000-06-01 00:00:00}
  l   {2000-06-30 23:59:59}
  m   {2000-07-01 00:00:00}
  n   {2000-07-31 23:59:59}
  o   {2000-08-01 00:00:00}
  p   {2000-08-31 23:59:59}
  q   {2000-09-01 00:00:00}
  r   {2000-09-30 23:59:59}
  s   {2000-10-01 00:00:00}
  t   {2000-10-31 23:59:59}
  u   {2000-11-01 00:00:00}
  v   {2000-11-30 23:59:59}
  w   {2000-12-01 00:00:00}
  x   {2000-12-31 23:59:59}
}
set p2 {
  A   {2001-01-01 00:00:00}
  B   {2001-01-31 23:59:59}
  C   {2001-02-01 00:00:00}
  D   {2001-02-28 23:59:59}
  E   {2001-03-01 00:00:00}
  F   {2001-03-31 23:59:59}
  G   {2001-04-01 00:00:00}
  H   {2001-04-30 23:59:59}
  I   {2001-05-01 00:00:00}
  J   {2001-05-31 23:59:59}
  K   {2001-06-01 00:00:00}
  L   {2001-06-30 23:59:59}
  M   {2001-07-01 00:00:00}
  N   {2001-07-31 23:59:59}
  O   {2001-08-01 00:00:00}
  P   {2001-08-31 23:59:59}
  Q   {2001-09-01 00:00:00}
  R   {2001-09-30 23:59:59}
  S   {2001-10-01 00:00:00}
  T   {2001-10-31 23:59:59}
  U   {2001-11-01 00:00:00}
  V   {2001-11-30 23:59:59}
  W   {2001-12-01 00:00:00}
  X   {2001-12-31 23:59:59}
}

foreach {x1 d1} $p1 {
  foreach {x2 d2} $p2 {
    set r1 [db one {SELECT datetime($d1)}]
    do_execsql_test timediff-6-$x1$x2 {
      SELECT datetime($d2, timediff($d1,$d2));
    } [list $r1]
    set r2 [db one {SELECT datetime($d2)}]
    do_execsql_test timediff-6-$x2$x1 {
      SELECT datetime($d1, timediff($d2,$d1));
    } [list $r2]
  }
}



finish_test
