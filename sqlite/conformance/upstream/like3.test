# 2015-03-06
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
# This file implements regression tests for SQLite library.  The
# focus of this file is testing the LIKE and GLOB operators and
# in particular the optimizations that occur to help those operators
# run faster and that those optimizations work correctly when there
# are both strings and blobs being tested.
#
# Ticket 05f43be8fdda9fbd948d374319b99b054140bc36 shows that the following
# SQL was not working correctly:
#
#     CREATE TABLE t1(x TEXT UNIQUE COLLATE nocase);
#     INSERT INTO t1(x) VALUES(x'616263');
#     SELECT 'query-1', x FROM t1 WHERE x LIKE 'a%';
#     SELECT 'query-2', x FROM t1 WHERE +x LIKE 'a%';
#
# This script verifies that it works right now.
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl

ifcapable !like_match_blobs {
  finish_test
  return
}

do_execsql_test like3-1.1 {
  PRAGMA encoding=UTF8;
  CREATE TABLE t1(a,b TEXT COLLATE nocase);
  INSERT INTO t1(a,b)
     VALUES(1,'abc'),
           (2,'ABX'),
           (3,'BCD'),
           (4,x'616263'),
           (5,x'414258'),
           (6,x'424344');
  CREATE INDEX t1ba ON t1(b,a);

  SELECT a, b FROM t1 WHERE b LIKE 'aB%' ORDER BY +a;
} {1 abc 2 ABX 4 abc 5 ABX}
do_execsql_test like3-1.2 {
  SELECT a, b FROM t1 WHERE +b LIKE 'aB%' ORDER BY +a;
} {1 abc 2 ABX 4 abc 5 ABX}

do_execsql_test like3-2.0 {
  CREATE TABLE t2(a, b TEXT);
  INSERT INTO t2 SELECT a, b FROM t1;
  CREATE INDEX t2ba ON t2(b,a);
  SELECT a, b FROM t2 WHERE b GLOB 'ab*' ORDER BY +a;
} {1 abc 4 abc}
do_execsql_test like3-2.1 {
  SELECT a, b FROM t2 WHERE +b GLOB 'ab*' ORDER BY +a;
} {1 abc 4 abc}
do_execsql_test like3-2.2 {
  SELECT a, b FROM t2 WHERE b>=x'6162' AND b GLOB 'ab*'
} {4 abc}
do_execsql_test like3-2.3 {
  SELECT a, b FROM t2 WHERE +b>=x'6162' AND +b GLOB 'ab*'
} {4 abc}
do_execsql_test like3-2.4 {
  SELECT a, b FROM t2 WHERE b GLOB 'ab*' AND b>=x'6162'
} {4 abc}
do_execsql_test like3-2.5 {
  SELECT a, b FROM t2 WHERE +b GLOB 'ab*' AND +b>=x'6162'
} {4 abc}

do_execsql_test like3-3.0 {
  CREATE TABLE t3(x TEXT PRIMARY KEY COLLATE nocase);
  INSERT INTO t3(x) VALUES('aaa'),('abc'),('abd'),('abe'),('acz');
  INSERT INTO t3(x) SELECT CAST(x AS blob) FROM t3;
  SELECT quote(x) FROM t3 WHERE x LIKE 'ab%' ORDER BY x;
} {'abc' 'abd' 'abe' X'616263' X'616264' X'616265'}
do_execsql_test like3-3.1 {
  SELECT quote(x) FROM t3 WHERE x LIKE 'ab%' ORDER BY x DESC;
} {X'616265' X'616264' X'616263' 'abe' 'abd' 'abc'}
do_execsql_test like3-3.1ck {
  SELECT quote(x) FROM t3 WHERE x LIKE 'ab%' ORDER BY +x DESC;
} {X'616265' X'616264' X'616263' 'abe' 'abd' 'abc'}
do_execsql_test like3-3.2 {
  SELECT quote(x) FROM t3 WHERE x LIKE 'ab%' ORDER BY x ASC;
} {'abc' 'abd' 'abe' X'616263' X'616264' X'616265'}
do_execsql_test like3-3.2ck {
  SELECT quote(x) FROM t3 WHERE x LIKE 'ab%' ORDER BY +x ASC;
} {'abc' 'abd' 'abe' X'616263' X'616264' X'616265'}

do_execsql_test like3-4.0 {
  CREATE TABLE t4(x TEXT COLLATE nocase);
  CREATE INDEX t4x ON t4(x DESC);
  INSERT INTO t4(x) SELECT x FROM t3;
  SELECT quote(x) FROM t4 WHERE x LIKE 'ab%' ORDER BY x;
} {'abc' 'abd' 'abe' X'616263' X'616264' X'616265'}
do_execsql_test like3-4.1 {
  SELECT quote(x) FROM t4 WHERE x LIKE 'ab%' ORDER BY x DESC;
} {X'616265' X'616264' X'616263' 'abe' 'abd' 'abc'}
do_execsql_test like3-4.1ck {
  SELECT quote(x) FROM t4 WHERE x LIKE 'ab%' ORDER BY +x DESC;
} {X'616265' X'616264' X'616263' 'abe' 'abd' 'abc'}
do_execsql_test like3-4.2 {
  SELECT quote(x) FROM t4 WHERE x LIKE 'ab%' ORDER BY x ASC;
} {'abc' 'abd' 'abe' X'616263' X'616264' X'616265'}
do_execsql_test like3-4.2ck {
  SELECT quote(x) FROM t4 WHERE x LIKE 'ab%' ORDER BY +x ASC;
} {'abc' 'abd' 'abe' X'616263' X'616264' X'616265'}

# 2018-09-10 ticket https://sqlite.org/src/tktview/c94369cae9b561b1f996
# The like optimization fails for a column with numeric affinity if
# the pattern '/%' or begins with the escape character.
#
do_execsql_test like3-5.100 {
  CREATE TABLE t5a(x INT UNIQUE COLLATE nocase);
  INSERT INTO t5a(x) VALUES('/abc'),(123),(-234);
  SELECT x FROM t5a WHERE x LIKE '/%';
} {/abc}
do_eqp_test like3-5.101 {
  SELECT x FROM t5a WHERE x LIKE '/%';
} {
  QUERY PLAN
  `--SCAN t5a
}
do_execsql_test like3-5.110 {
  SELECT x FROM t5a WHERE x LIKE '/a%';
} {/abc}
ifcapable !icu {
do_eqp_test like3-5.111 {
  SELECT x FROM t5a WHERE x LIKE '/a%';
} {
  QUERY PLAN
  `--SEARCH t5a USING COVERING INDEX sqlite_autoindex_t5a_1 (x>? AND x<?)
}
}
do_execsql_test like3-5.120 {
  SELECT x FROM t5a WHERE x LIKE '^12%' ESCAPE '^';
} {123}
do_eqp_test like3-5.121 {
  SELECT x FROM t5a WHERE x LIKE '^12%' ESCAPE '^';
} {
  QUERY PLAN
  `--SCAN t5a
}
do_execsql_test like3-5.122 {
  SELECT x FROM t5a WHERE x LIKE '^-2%' ESCAPE '^';
} {-234}
do_eqp_test like3-5.123 {
  SELECT x FROM t5a WHERE x LIKE '^12%' ESCAPE '^';
} {
  QUERY PLAN
  `--SCAN t5a
}

do_execsql_test like3-5.200 {
  CREATE TABLE t5b(x INT UNIQUE COLLATE binary);
  INSERT INTO t5b(x) VALUES('/abc'),(123),(-234);
  SELECT x FROM t5b WHERE x GLOB '/*';
} {/abc}
do_eqp_test like3-5.201 {
  SELECT x FROM t5b WHERE x GLOB '/*';
} {
  QUERY PLAN
  `--SCAN t5b
}
do_execsql_test like3-5.210 {
  SELECT x FROM t5b WHERE x GLOB '/a*';
} {/abc}
do_eqp_test like3-5.211 {
  SELECT x FROM t5b WHERE x GLOB '/a*';
} {
  QUERY PLAN
  `--SEARCH t5b USING COVERING INDEX sqlite_autoindex_t5b_1 (x>? AND x<?)
}

# 2019-05-01
# another case of the above reported on the mailing list by Manuel Rigger.
#
do_execsql_test like3-5.300 {
  CREATE TABLE t5c (c0 REAL);
  CREATE INDEX t5c_0 ON t5c(c0 COLLATE NOCASE);
  INSERT INTO t5c(rowid, c0) VALUES (99,'+/');
  SELECT * FROM t5c WHERE (c0 LIKE '+/');
} {+/}

# 2019-05-08
# Yet another case for the above from Manuel Rigger.
#
do_execsql_test like3-5.400 {
  DROP TABLE IF EXISTS t0;
  CREATE TABLE t0(c0 INT UNIQUE COLLATE NOCASE);
  INSERT INTO t0(c0) VALUES ('./');
  SELECT * FROM t0 WHERE t0.c0 LIKE './';
} {./}

# 2019-06-14
# Ticket https://sqlite.org/src/info/ce8717f0885af975
do_execsql_test like3-5.410 {
  DROP TABLE IF EXISTS t0;
  CREATE TABLE t0(c0 INT UNIQUE COLLATE NOCASE);
  INSERT INTO t0(c0) VALUES ('.1%');
  SELECT * FROM t0 WHERE t0.c0 LIKE '.1%';
} {.1%}

# 2019-09-03
# Ticket https://sqlite.org/src/info/0f0428096f
do_execsql_test like3-5.420 {
  DROP TABLE IF EXISTS t0;
  CREATE TABLE t0(c0 UNIQUE);
  INSERT INTO t0(c0) VALUES(-1);
  SELECT * FROM t0 WHERE t0.c0 GLOB '-*';
} {-1}
do_execsql_test like3-5.421 {
  SELECT t0.c0 GLOB '-*' FROM t0;
} {1}



# 2019-02-27
# Verify that the LIKE optimization works with an ESCAPE clause when
# using PRAGMA case_sensitive_like=ON.
#
ifcapable !icu {
do_execsql_test like3-6.100 {
  DROP TABLE IF EXISTS t1;
  CREATE TABLE t1(path TEXT COLLATE nocase PRIMARY KEY,a,b,c) WITHOUT ROWID;
}
do_eqp_test like3-6.110 {
  SELECT * FROM t1 WHERE path LIKE 'a%';
} {
  QUERY PLAN
  `--SEARCH t1 USING PRIMARY KEY (path>? AND path<?)
}
do_eqp_test like3-6.120 {
  SELECT * FROM t1 WHERE path LIKE 'a%' ESCAPE 'x';
} {
  QUERY PLAN
  `--SEARCH t1 USING PRIMARY KEY (path>? AND path<?)
}
do_execsql_test like3-6.200 {
  DROP TABLE IF EXISTS t2;
  CREATE TABLE t2(path TEXT,x,y,z);
  CREATE INDEX t2path ON t2(path COLLATE nocase);
  CREATE INDEX t2path2 ON t2(path);
}
do_eqp_test like3-6.210 {
  SELECT * FROM t2 WHERE path LIKE 'a%';
} {
  QUERY PLAN
  `--SEARCH t2 USING INDEX t2path (path>? AND path<?)
}
do_eqp_test like3-6.220 {
  SELECT * FROM t2 WHERE path LIKE 'a%' ESCAPE '\';
} {
  QUERY PLAN
  `--SEARCH t2 USING INDEX t2path (path>? AND path<?)
}
db eval {PRAGMA case_sensitive_like=ON}
do_eqp_test like3-6.230 {
  SELECT * FROM t2 WHERE path LIKE 'a%';
} {
  QUERY PLAN
  `--SEARCH t2 USING INDEX t2path2 (path>? AND path<?)
}
do_eqp_test like3-6.240 {
  SELECT * FROM t2 WHERE path LIKE 'a%' ESCAPE '\';
} {
  QUERY PLAN
  `--SEARCH t2 USING INDEX t2path2 (path>? AND path<?)
}
}

#-------------------------------------------------------------------------

ifcapable utf16 {
  reset_db
  do_execsql_test like3-7.0 {
    PRAGMA encoding = 'UTF-16be';
  
    CREATE TABLE Example(word TEXT NOT NULL);
    CREATE INDEX Example_word on Example(word);
  
    INSERT INTO Example VALUES(char(0x307F));
  }
  
  do_execsql_test like3-7.1 {
    SELECT char(0x307F)=='み';
  } {1}
  
  do_execsql_test like3-7.1 {
    SELECT * FROM Example WHERE word GLOB 'み*'
  } {み}
  
  do_execsql_test like3-7.2 {
    SELECT * FROM Example WHERE word >= char(0x307F) AND word < char(0x3080);
  } {み}
}

#-------------------------------------------------------------------------
reset_db

# See forum thread https://sqlite.org/forum/info/d7b90d92ffbfc61f
foreach enc {
  UTF-8
  UTF-16le 
  UTF-16be
} {
  ifcapable icu {
    if {$enc=="UTF-8"} {
      # The invalid UTF8 used in these tests is incompatible with ICU
      # https://sqlite.org/forum/forumpost/2ca8a09a7e
      continue
    }
  }
  foreach {tn expr} {
    1 "CAST (X'FF' AS TEXT)"
    2 "CAST (X'FFBF' AS TEXT)"
    3 "CAST (X'FFBFBF' AS TEXT)"
    4 "CAST (X'FFBFBFBF' AS TEXT)"

    5 "'abc' || CAST (X'FF' AS TEXT)"
    6 "'def' || CAST (X'FFBF' AS TEXT)"
    7 "'ghi' || CAST (X'FFBFBF' AS TEXT)"
    8 "'jkl' || CAST (X'FFBFBFBF' AS TEXT)"
  } {
    reset_db
    execsql "PRAGMA encoding = '$enc'"
    set tn utf[string range $enc 4 end].$tn
    do_execsql_test like3-8.$tn.1 {
      CREATE TABLE t1(x);
    }
  
    do_execsql_test like3-8.$tn.2 {
      PRAGMA encoding
    } $enc
  
    do_execsql_test like3-8.$tn.3 "
      INSERT INTO t1 VALUES( $expr )
    "
  
    do_execsql_test like3-8.$tn.4 {
      SELECT typeof(x) FROM t1
    } {text}
  
    set x [db one {SELECT x || '%' FROM t1}]
  
    do_execsql_test like3-8.$tn.5 {
      SELECT rowid FROM t1 WHERE x LIKE $x
    } 1
  
    do_execsql_test like3-8.$tn.6 {
      CREATE INDEX i1 ON t1(x);
    }
  
    do_execsql_test like3-8.$tn.7 {
      SELECT rowid FROM t1 WHERE x LIKE $x
    } 1
  }
}

finish_test
