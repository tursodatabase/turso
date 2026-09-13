# 2021 February 19
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

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix alterdropcol2

# If SQLITE_OMIT_ALTERTABLE is defined, omit this file.
ifcapable !altertable {
  finish_test
  return
}

# EVIDENCE-OF: R-58318-35349 The DROP COLUMN syntax is used to remove an
# existing column from a table.
do_execsql_test 1.0 {
  CREATE TABLE t1(c, b, a, PRIMARY KEY(b, a)) WITHOUT ROWID;
  INSERT INTO t1 VALUES(1, 2, 3), (4, 5, 6);
}
do_execsql_test 1.1 {
  ALTER TABLE t1 DROP c;
}

# EVIDENCE-OF: The DROP COLUMN command removes the named column from the table,
# and also rewrites the entire table to purge the data associated with that
# column.  
do_execsql_test 1.2.1 {
  SELECT * FROM t1;
} {2 3   5 6}

do_execsql_test 1.2.2 {
  SELECT sql FROM sqlite_schema;
} {
  {CREATE TABLE t1(b, a, PRIMARY KEY(b, a)) WITHOUT ROWID}
}

proc do_atdc_error_test {tn schema atdc error} {
  reset_db
  execsql $schema
  uplevel [list do_catchsql_test $tn $atdc [list 1 [string trim $error]]]
}

#-------------------------------------------------------------------------
# Test cases 2.* attempt to verify the following:
#
# EVIDENCE-OF: R-24098-10282 The DROP COLUMN command only works if the column
# is not referenced by any other parts of the schema and is not a PRIMARY KEY
# and does not have a UNIQUE constraint.
#

# EVIDENCE-OF: R-52436-31752 The column is a PRIMARY KEY or part of one.
#
do_atdc_error_test 2.1.1 {
  CREATE TABLE x1(a PRIMARY KEY, b, c);
} { 
  ALTER TABLE x1 DROP COLUMN a 
} {
  cannot drop PRIMARY KEY column: "a"
}
do_atdc_error_test 2.1.2 {
  CREATE TABLE x1(a,b,c,d,e, PRIMARY KEY(b,c,d));
} { 
  ALTER TABLE x1 DROP COLUMN c
} {
  cannot drop PRIMARY KEY column: "c"
}

# EVIDENCE-OF: R-43412-16016 The column has a UNIQUE constraint.
#
do_atdc_error_test 2.2.1 {
  CREATE TABLE x1(a PRIMARY KEY, b, c UNIQUE);
} { 
  ALTER TABLE x1 DROP COLUMN c 
} {
  cannot drop UNIQUE column: "c"
}
do_atdc_error_test 2.2.2 {
  CREATE TABLE x1(a PRIMARY KEY, b, c, UNIQUE(b, c));
} { 
  ALTER TABLE x1 DROP COLUMN c 
} {
  error in table x1 after drop column: no such column: c
}

# EVIDENCE-OF: R-46731-08965 The column is indexed.
#
do_atdc_error_test 2.3.1 {
  CREATE TABLE 'one two'('x y', 'z 1', 'a b');
  CREATE INDEX idx ON 'one two'('z 1');
} { 
  ALTER TABLE 'one two' DROP COLUMN 'z 1' 
} {
  error in index idx after drop column: no such column: z 1
}
do_atdc_error_test 2.3.2 {
  CREATE TABLE x1(a, b, c);
  CREATE INDEX idx ON x1(a);
} { 
  ALTER TABLE x1 DROP COLUMN a;
} {
  error in index idx after drop column: no such column: a
}

# EVIDENCE-OF: R-46731-08965 The column is indexed.
#
do_atdc_error_test 2.4.1 {
  CREATE TABLE x1234(a, b, c PRIMARY KEY) WITHOUT ROWID;
  CREATE INDEX i1 ON x1234(b) WHERE ((a+5) % 10)==0;
} { 
  ALTER TABLE x1234 DROP a
} {
  error in index i1 after drop column: no such column: a
}

# EVIDENCE-OF: R-47838-03249 The column is named in a table or column
# CHECK constraint not associated with the column being dropped.
#
do_atdc_error_test 2.5.1 {
  CREATE TABLE x1234(a, b, c PRIMARY KEY, CHECK(((a+5)%10)!=0)) WITHOUT ROWID;
} { 
  ALTER TABLE x1234 DROP a
} {
  error in table x1234 after drop column: no such column: a
}

# EVIDENCE-OF: R-55640-01652 The column is used in a foreign key constraint.
#
do_atdc_error_test 2.6.1 {
  CREATE TABLE p1(x, y UNIQUE);
  CREATE TABLE c1(u, v, FOREIGN KEY (v) REFERENCES p1(y))
} { 
  ALTER TABLE c1 DROP v
} {
  error in table c1 after drop column: unknown column "v" in foreign key definition
}

# EVIDENCE-OF: R-20795-39479 The column is used in the expression of a 
# generated column.
do_atdc_error_test 2.7.1 {
  CREATE TABLE c1(u, v, w AS (u+v));
} { 
  ALTER TABLE c1 DROP v
} {
  error in table c1 after drop column: no such column: v
}
do_atdc_error_test 2.7.2 {
  CREATE TABLE c1(u, v, w AS (u+v) STORED);
} { 
  ALTER TABLE c1 DROP u
} {
  error in table c1 after drop column: no such column: u
}

# EVIDENCE-OF: R-01515-49025 The column appears in a trigger or view.
#
do_atdc_error_test 2.8.1 {
  CREATE TABLE log(l);
  CREATE TABLE c1(u, v, w);
  CREATE TRIGGER tr1 AFTER INSERT ON c1 BEGIN
    INSERT INTO log VALUES(new.w);
  END;
} { 
  ALTER TABLE c1 DROP w
} {
  error in trigger tr1 after drop column: no such column: new.w
}
do_atdc_error_test 2.8.2 {
  CREATE TABLE c1(u, v, w);
  CREATE VIEW v1 AS SELECT u, v, w FROM c1;
} { 
  ALTER TABLE c1 DROP w
} {
  error in view v1 after drop column: no such column: w
}
do_atdc_error_test 2.8.3 {
  CREATE TABLE c1(u, v, w);
  CREATE VIEW v1 AS SELECT * FROM c1 WHERE w IS NOT NULL;
} { 
  ALTER TABLE c1 DROP w
} {
  error in view v1 after drop column: no such column: w
}

#-------------------------------------------------------------------------
# Verify that a column that is part of a CHECK constraint may be dropped
# if the CHECK constraint was specified as part of the column definition.
#

# STALE-EVIDENCE: R-60924-11170 However, the column being deleted can be used in a
# column CHECK constraint because the column CHECK constraint is dropped
# together with the column itself.
do_execsql_test 3.0 {
  CREATE TABLE yyy(q, w, e CHECK (e > 0), r);
  INSERT INTO yyy VALUES(1,1,1,1), (2,2,2,2);

  CREATE TABLE zzz(q, w, e, r, CHECK (e > 0));
  INSERT INTO zzz VALUES(1,1,1,1), (2,2,2,2);
}
do_catchsql_test 3.1.1 {
  INSERT INTO yyy VALUES(0,0,0,0);
} {1 {CHECK constraint failed: e > 0}}
do_catchsql_test 3.1.2 {
  INSERT INTO yyy VALUES(0,0,0,0);
} {1 {CHECK constraint failed: e > 0}}

do_execsql_test 3.2.1 {
  ALTER TABLE yyy DROP e;
}
do_catchsql_test 3.2.2 {
  ALTER TABLE zzz DROP e;
} {1 {error in table zzz after drop column: no such column: e}}

finish_test
