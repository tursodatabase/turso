# 2022-01-27
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
  do_test date3-$tnum [subst {
    execsql "SELECT coalesce($expr,'NULL')"
  }] [list $result]
}
set tcl_precision 15

# EVIDENCE-OF: R-45708-63005 unixepoch(time-value, modifier, modifier,
# ...)
#
datetest 1.1 {unixepoch('1970-01-01')} {0}
datetest 1.2 {unixepoch('1969-12-31 23:59:59')} {-1}
datetest 1.3 {unixepoch('2106-02-07 06:28:15')} {4294967295}
datetest 1.4 {unixepoch('2106-02-07 06:28:16')} {4294967296}
datetest 1.5 {unixepoch('9999-12-31 23:59:59')} {253402300799}
datetest 1.6 {unixepoch('0000-01-01 00:00:00')} {-62167219200}

# EVIDENCE-OF: R-30877-63179 The unixepoch() function returns a unix
# timestamp - the number of seconds since 1970-01-01 00:00:00 UTC.
#
for {set i 1} {$i<=100} {incr i} {
  set x [expr {int(rand()*0xfffffffff)-0xffffffff}]
  datetest 1.7.$i "unixepoch($x,'unixepoch')==$x" {1}
}

# EVIDENCE-OF: R-62992-54137 The unixepoch() always returns an integer,
# even if the input time-value has millisecond precision.
#
datetest 1.8 {unixepoch('2022-01-27 12:59:28.052')} {1643288368}

# EVIDENCE-OF: R-05412-24332 If the time-value is numeric (the
# DDDDDDDDDD format) then the 'auto' modifier causes the time-value to
# interpreted as either a julian day number or a unix timestamp,
# depending on its magnitude.
#
# EVIDENCE-OF: R-56763-40111 If the value is between 0.0 and
# 5373484.499999, then it is interpreted as a julian day number
# (corresponding to dates between -4713-11-24 12:00:00 and 9999-12-31
# 23:59:59, inclusive).
#
# EVIDENCE-OF: R-07289-49223 For numeric values outside of the range of
# valid julian day numbers, but within the range of -210866760000 to
# 253402300799, the 'auto' modifier causes the value to be interpreted
# as a unix timestamp.
#
# EVIDENCE-OF: R-20795-34947 Other numeric values are out of range and
# cause a NULL return.
#
foreach {tn jd date} {
  2.1  0.0              {-4713-11-24 12:00:00}
  2.2  5373484.4999999  {9999-12-31 23:59:59}
  2.3  2440587.5        {1970-01-01 00:00:00}
  2.4  2440587.49998843 {1969-12-31 23:59:59}
  2.5  2440615.7475463  {1970-01-29 05:56:28}

  2.10 -1               {1969-12-31 23:59:59}
  2.11 5373485          {1970-03-04 04:38:05}
  2.12 -210866760000    {-4713-11-24 12:00:00}
  2.13 253402300799     {9999-12-31 23:59:59}

  2.20 -210866760001    {NULL}
  2.21 253402300800     {NULL}
} {
  datetest $tn "datetime($jd,'auto')" $date
}

# EVIDENCE-OF: R-38886-35357 The 'auto' modifier is a no-op for text
# time-values.
#
datetest 2.30 {date('2022-01-29','auto')==date('2022-01-29')} {1}

# EVIDENCE-OF: R-53132-26856 The 'auto' modifier can be used to work
# with date/time values even in cases where it is not known if the
# julian day number or unix timestamp formats are in use.
#
do_execsql_test date3-2.40 {
  WITH tx(timeval,datetime) AS (
     VALUES('2022-01-27 13:15:44','2022-01-27 13:15:44'),
           (2459607.05260275,'2022-01-27 13:15:44'),
           (1643289344,'2022-01-27 13:15:44')
  )
  SELECT datetime(timeval,'auto') == datetime FROM tx;
} {1 1 1}

# EVIDENCE-OF: R-49255-55373 The "unixepoch" modifier (11) only works if
# it immediately follows a time value in the DDDDDDDDDD format.
#
# EVIDENCE-OF: R-23075-39245 This modifier causes the DDDDDDDDDD to be
# interpreted not as a Julian day number as it normally would be, but as
# Unix Time - the number of seconds since 1970.
#
datetest 3.1 {datetime(2459607.05,'+1 hour','unixepoch')} {NULL}
datetest 3.2 {datetime(2459607.05,'unixepoch','+1 hour')} {1970-01-29 12:13:27}

# EVIDENCE-OF: R-21150-52363 The "julianday" modifier must immediately
# follow the initial time-value which must be of the form DDDDDDDDD.
#
# EVIDENCE-OF: R-31176-64601 Any other use of the 'julianday' modifier
# is an error and causes the function to return NULL.
#
# EVIDENCE-OF: R-32483-36353 The 'julianday' modifier forces the
# time-value number to be interpreted as a julian-day number.
#
# EVIDENCE-OF: R-25859-20124 The only difference is that adding
# 'julianday' forces the DDDDDDDDD time-value format, and causes a NULL
# to be returned if any other time-value format is used.
#
datetest 4.1 {datetime(2459607,'julianday')}           {2022-01-27 12:00:00}
datetest 4.2 {datetime(2459607,'+1 hour','julianday')} {NULL}
datetest 4.3 {datetime('2022-01-27','julianday')}      {NULL}



# EVIDENCE-OF: R-33431-18865 Unix timestamps for the first 63 days of
# 1970 will be interpreted as julian day numbers.
#
do_execsql_test date3-5.0 {
  WITH inc(x) AS (VALUES(-10) UNION ALL SELECT x+1 FROM inc WHERE x<100)
  SELECT count(*) FROM inc
  WHERE datetime('1970-01-01',format('%+d days',x))
     <> datetime(unixepoch('1970-01-01',format('%+d days',x)),'auto');
} {63}

finish_test
