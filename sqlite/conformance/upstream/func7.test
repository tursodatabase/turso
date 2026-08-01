# 2020-12-07
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#*************************************************************************
#
# Test cases for SQL functions based off the standard math library
#
set testdir [file dirname $argv0]
source $testdir/tester.tcl
ifcapable !mathlib {
  finish_test
  return
}

do_execsql_test func7-100 {
  SELECT ceil(99.9), ceiling(-99.01), floor(17), floor(-17.99);
} {100.0 -99.0 17 -18.0}
do_execsql_test func7-110 {
  SELECT quote(ceil(NULL)), ceil('-99.99');
} {NULL -99.0}
do_execsql_test func7-200 {
  SELECT round(ln(5),2), log(100.0), log(100), log(2,'256');
} {1.61 2.0 2.0 8.0}
do_execsql_test func7-210 {
  SELECT ln(-5), log(-5,100.0);
} {{} {}}

# Test cases derived from PostgreSQL documentation
#
do_execsql_test func7-pg-100 {
  SELECT abs(-17.4)
} {17.4}
do_execsql_test func7-pg-110 {
  SELECT ceil(42.2)
} {43.0}
do_execsql_test func7-pg-120 {
  SELECT ceil(-42.2)
} {-42.0}
do_execsql_test func7-pg-130 {
  SELECT round(exp(1.0),7)
} {2.7182818}
do_execsql_test func7-pg-140 {
  SELECT floor(42.8)
} {42.0}
do_execsql_test func7-pg-150 {
  SELECT floor(-42.8)
} {-43.0}
do_execsql_test func7-pg-160 {
  SELECT round(ln(2.0),7)
} {0.6931472}
do_execsql_test func7-pg-170 {
  SELECT log(100.0)
} {2.0}
do_execsql_test func7-pg-180 {
  SELECT log10(1000.0)
} {3.0}
do_execsql_test func7-pg-181 {
  SELECT format('%.30f', log10(100.0) );
} {2.000000000000000000000000000000}
do_execsql_test func7-pg-182 {
  SELECT format('%.30f', ln(exp(2.0)) );
} {2.000000000000000000000000000000}
do_execsql_test func7-pg-190 {
  SELECT log(2.0, 64.0)
} {6.0}
do_execsql_test func7-pg-200 {
   SELECT mod(9,4);
} {1.0}
do_execsql_test func7-pg-210 {
   SELECT round(pi(),7);
} {3.1415927}
do_execsql_test func7-pg-220 {
   SELECT power(9,3);
} {729.0}
do_execsql_test func7-pg-230 {
   SELECT round(radians(45.0),7);
} {0.7853982}
do_execsql_test func7-pg-240 {
   SELECT round(42.4);
} {42.0}
do_execsql_test func7-pg-250 {
   SELECT round(42.4382,2);
} {42.44}
do_execsql_test func7-pg-260 {
   SELECT sign(-8.4);
} {-1}
do_execsql_test func7-pg-270 {
   SELECT round( sqrt(2), 7);
} {1.4142136}
do_execsql_test func7-pg-280 {
   SELECT trunc(42.8), trunc(-42.8);
} {42.0 -42.0}
do_execsql_test func7-pg-300 {
   SELECT acos(1);
} {0.0}
do_execsql_test func7-pg-301 {
   SELECT format('%f',degrees(acos(0.5)));
} {60.0}
do_execsql_test func7-pg-310 {
   SELECT round( asin(1), 7);
} {1.5707963}
do_execsql_test func7-pg-311 {
   SELECT format('%f',degrees( asin(0.5) ));
} {30.0}
do_execsql_test func7-pg-320 {
   SELECT round( atan(1), 7);
} {0.7853982}
do_execsql_test func7-pg-321 {
   SELECT degrees( atan(1) );
} {45.0}
do_execsql_test func7-pg-330 {
   SELECT round( atan2(1,0), 7);
} {1.5707963}
do_execsql_test func7-pg-331 {
   SELECT degrees( atan2(1,0) );
} {90.0}
do_execsql_test func7-pg-400 {
   SELECT cos(0);
} {1.0}
do_execsql_test func7-pg-401 {
   SELECT cos( radians(60.0) );
} {0.5}
do_execsql_test func7-pg-400 {
   SELECT cos(0);
} {1.0}
do_execsql_test func7-pg-410 {
   SELECT round( sin(1), 7);
} {0.841471}
do_execsql_test func7-pg-411 {
   SELECT sin( radians(30) );
} {0.5}
do_execsql_test func7-pg-420 {
   SELECT round( tan(1), 7);
} {1.5574077}
do_execsql_test func7-pg-421 {
   SELECT round(tan( radians(45) ),10);
} {1.0}
do_execsql_test func7-pg-500 {
   SELECT round( sinh(1), 7);
} {1.1752012}
do_execsql_test func7-pg-510 {
   SELECT round( cosh(0), 7);
} {1.0}
do_execsql_test func7-pg-520 {
   SELECT round( tanh(1), 7);
} {0.7615942}
do_execsql_test func7-pg-530 {
   SELECT round( asinh(1), 7);
} {0.8813736}
do_execsql_test func7-pg-540 {
   SELECT round( acosh(1), 7);
} {0.0}
do_execsql_test func7-pg-550 {
   SELECT round( atanh(0.5), 7);
} {0.5493061}

# Test cases derived from MySQL documentation
#
do_execsql_test func7-mysql-100 {
   SELECT acos(1);
} {0.0}
do_execsql_test func7-mysql-110 {
   SELECT acos(1.0001);
} {{}}
do_execsql_test func7-mysql-120 {
   SELECT round( acos(0.0), 7);
} {1.5707963}
do_execsql_test func7-mysql-130 {
   SELECT round( asin(0.2), 7);
} {0.2013579}
do_execsql_test func7-mysql-140 {
   SELECT asin('foo');
} {{}}  ;# Note: MySQL returns 0 here, not NULL.
         # SQLite deliberately returns NULL.
         # SQLServer and Oracle throw an error.
do_execsql_test func7-mysql-150 {
   SELECT round( atan(2), 7), round( atan(-2), 7);
} {1.1071487 -1.1071487}
do_execsql_test func7-mysql-160 {
   SELECT round( atan2(-2,2), 7), round( atan2(pi(),0), 7);
} {-0.7853982 1.5707963}
do_execsql_test func7-mysql-170 {
   SELECT ceiling(1.23), ceiling(-1.23);
} {2.0 -1.0}
do_execsql_test func7-mysql-180 {
   SELECT cos(pi());
} {-1.0}
do_execsql_test func7-mysql-190 {
   SELECT degrees(pi()), degrees(pi()/2);
} {180.0 90.0}
do_execsql_test func7-mysql-190 {
   SELECT round( exp(2), 7), round( exp(-2), 7), exp(0);
} {7.3890561 0.1353353 1.0}
do_execsql_test func7-mysql-200 {
   SELECT floor(1.23), floor(-1.23);
} {1.0 -2.0}
do_execsql_test func7-mysql-210 {
   SELECT round(ln(2),7), quote(ln(-2));
} {0.6931472 NULL}
#do_execsql_test func7-mysql-220 {
#   SELECT round(log(2),7), log(-2);
#} {0.6931472 NULL}
# log() means natural logarithm in MySQL
do_execsql_test func7-mysql-230 {
   SELECT log(2,65536), log(10,100), quote(log(1,100)), quote(log(0,100));
} {16.0 2.0 NULL NULL}
do_execsql_test func7-mysql-240 {
   SELECT log2(65536), quote(log2(-100)), quote(log2(0));
} {16.0 NULL NULL}
do_execsql_test func7-mysql-250 {
   SELECT round(log10(2),7), log10(100), quote(log10(-100));
} {0.30103 2.0 NULL}
do_execsql_test func7-mysql-260 {
   SELECT mod(234,10), 253%7, mod(29,9), 29%9;
} {4.0 1 2.0 2}
do_execsql_test func7-mysql-270 {
   SELECT mod(34.5,3);
} {1.5}
do_execsql_test func7-mysql-280 {
   SELECT pow(2,2), pow(2,-2);
} {4.0 0.25}
do_execsql_test func7-mysql-281 {
   SELECT power(2,2), power(2,-2);
} {4.0 0.25}
do_execsql_test func7-mysql-290 {
   SELECT round(radians(90),7);
} {1.5707963}
do_execsql_test func7-mysql-300 {
   SELECT sign(-32), sign(0), sign(234);
} {-1 0 1}
do_execsql_test func7-mysql-310 {
   SELECT sin(pi()) BETWEEN -1.0e-15 AND 1.0e-15;
} {1}
do_execsql_test func7-mysql-320 {
   SELECT sqrt(4), round(sqrt(20),7), quote(sqrt(-16));
} {2.0 4.472136 NULL}
do_execsql_test func7-mysql-330 {
   SELECT tan(pi()) BETWEEN -1.0e-15 AND 1.0e-15;
} {1}
do_execsql_test func7-mysql-331 {
   SELECT round(tan(pi()+1),7);
} {1.5574077}


finish_test
