--
-- CONSTRAINTS
-- Constraints can be specified with:
--  - DEFAULT clause
--  - CHECK clauses
--  - PRIMARY KEY clauses
--  - UNIQUE clauses
--  - EXCLUDE clauses
--  - NOT NULL clauses
--

-- directory paths are passed to us in environment variables
\getenv abs_srcdir PG_ABS_SRCDIR

--
-- DEFAULT syntax
--

CREATE TABLE DEFAULT_TBL (i int DEFAULT 100,
	x text DEFAULT 'vadim', f float8 DEFAULT 123.456);

INSERT INTO DEFAULT_TBL VALUES (1, 'thomas', 57.0613);
INSERT INTO DEFAULT_TBL VALUES (1, 'bruce');
INSERT INTO DEFAULT_TBL (i, f) VALUES (2, 987.654);
INSERT INTO DEFAULT_TBL (x) VALUES ('marc');
INSERT INTO DEFAULT_TBL VALUES (3, null, 1.0);

SELECT * FROM DEFAULT_TBL;

CREATE SEQUENCE DEFAULT_SEQ;

CREATE TABLE DEFAULTEXPR_TBL (i1 int DEFAULT 100 + (200-199) * 2,
	i2 int DEFAULT nextval('default_seq'));

INSERT INTO DEFAULTEXPR_TBL VALUES (-1, -2);
INSERT INTO DEFAULTEXPR_TBL (i1) VALUES (-3);
INSERT INTO DEFAULTEXPR_TBL (i2) VALUES (-4);
INSERT INTO DEFAULTEXPR_TBL (i2) VALUES (NULL);

SELECT * FROM DEFAULTEXPR_TBL;

-- syntax errors
--  test for extraneous comma
CREATE TABLE error_tbl (i int DEFAULT (100, ));
--  this will fail because gram.y uses b_expr not a_expr for defaults,
--  to avoid a shift/reduce conflict that arises from NOT NULL being
--  part of the column definition syntax:
CREATE TABLE error_tbl (b1 bool DEFAULT 1 IN (1, 2));
--  this should work, however:
CREATE TABLE error_tbl (b1 bool DEFAULT (1 IN (1, 2)));

DROP TABLE error_tbl;

--
-- CHECK syntax
--

CREATE TABLE CHECK_TBL (x int,
	CONSTRAINT CHECK_CON CHECK (x > 3));

INSERT INTO CHECK_TBL VALUES (5);
INSERT INTO CHECK_TBL VALUES (4);
INSERT INTO CHECK_TBL VALUES (3);
INSERT INTO CHECK_TBL VALUES (2);
INSERT INTO CHECK_TBL VALUES (6);
INSERT INTO CHECK_TBL VALUES (1);

SELECT * FROM CHECK_TBL;

CREATE TABLE NE_CHECK_TBL (x int,
	CONSTRAINT CHECK_CON CHECK (x > 3) NOT ENFORCED);

INSERT INTO NE_CHECK_TBL VALUES (5);
INSERT INTO NE_CHECK_TBL VALUES (4);
INSERT INTO NE_CHECK_TBL VALUES (3);
INSERT INTO NE_CHECK_TBL VALUES (2);
INSERT INTO NE_CHECK_TBL VALUES (6);
INSERT INTO NE_CHECK_TBL VALUES (1);

SELECT * FROM NE_CHECK_TBL;

CREATE SEQUENCE CHECK_SEQ;

CREATE TABLE CHECK2_TBL (x int, y text, z int,
	CONSTRAINT SEQUENCE_CON
	CHECK (x > 3 and y <> 'check failed' and z < 8));

INSERT INTO CHECK2_TBL VALUES (4, 'check ok', -2);
INSERT INTO CHECK2_TBL VALUES (1, 'x check failed', -2);
INSERT INTO CHECK2_TBL VALUES (5, 'z check failed', 10);
INSERT INTO CHECK2_TBL VALUES (0, 'check failed', -2);
INSERT INTO CHECK2_TBL VALUES (6, 'check failed', 11);
INSERT INTO CHECK2_TBL VALUES (7, 'check ok', 7);

SELECT * from CHECK2_TBL;

--
-- Check constraints on INSERT
--

CREATE SEQUENCE INSERT_SEQ;

CREATE TABLE INSERT_TBL (x INT DEFAULT nextval('insert_seq'),
	y TEXT DEFAULT '-NULL-',
	z INT DEFAULT -1 * currval('insert_seq'),
	CONSTRAINT INSERT_TBL_CON CHECK (x >= 3 AND y <> 'check failed' AND x < 8),
	CHECK (x + z = 0) ENFORCED, /* no change it is a default */
	CONSTRAINT NE_INSERT_TBL_CON CHECK (x + z = 1) NOT ENFORCED);

INSERT INTO INSERT_TBL(x,z) VALUES (2, -2);

SELECT * FROM INSERT_TBL;

SELECT 'one' AS one, nextval('insert_seq');

INSERT INTO INSERT_TBL(y) VALUES ('Y');
INSERT INTO INSERT_TBL(y) VALUES ('Y');
INSERT INTO INSERT_TBL(x,z) VALUES (1, -2);
INSERT INTO INSERT_TBL(z,x) VALUES (-7,  7);
INSERT INTO INSERT_TBL VALUES (5, 'check failed', -5);
INSERT INTO INSERT_TBL VALUES (7, '!check failed', -7);
INSERT INTO INSERT_TBL(y) VALUES ('-!NULL-');

SELECT * FROM INSERT_TBL;

INSERT INTO INSERT_TBL(y,z) VALUES ('check failed', 4);
INSERT INTO INSERT_TBL(x,y) VALUES (5, 'check failed');
INSERT INTO INSERT_TBL(x,y) VALUES (5, '!check failed');
INSERT INTO INSERT_TBL(y) VALUES ('-!NULL-');

SELECT * FROM INSERT_TBL;

SELECT 'seven' AS one, nextval('insert_seq');

INSERT INTO INSERT_TBL(y) VALUES ('Y');

SELECT 'eight' AS one, currval('insert_seq');

-- According to SQL, it is OK to insert a record that gives rise to NULL
-- constraint-condition results.  Postgres used to reject this, but it
-- was wrong:
INSERT INTO INSERT_TBL VALUES (null, null, null);

SELECT * FROM INSERT_TBL;

--
-- Check constraints on system columns
--

CREATE TABLE SYS_COL_CHECK_TBL (city text, state text, is_capital bool,
                  altitude int,
                  CHECK (NOT (is_capital AND tableoid::regclass::text = 'sys_col_check_tbl')));

INSERT INTO SYS_COL_CHECK_TBL VALUES ('Seattle', 'Washington', false, 100);
INSERT INTO SYS_COL_CHECK_TBL VALUES ('Olympia', 'Washington', true, 100);

SELECT *, tableoid::regclass::text FROM SYS_COL_CHECK_TBL;

DROP TABLE SYS_COL_CHECK_TBL;

--
-- Check constraints on system columns other then TableOid should return error
--
CREATE TABLE SYS_COL_CHECK_TBL (city text, state text, is_capital bool,
                  altitude int,
				  CHECK (NOT (is_capital AND ctid::text = 'sys_col_check_tbl')));

--
-- Check inheritance of defaults and constraints
--

CREATE TABLE INSERT_CHILD (cx INT default 42,
	cy INT CHECK (cy > x))
	INHERITS (INSERT_TBL);

INSERT INTO INSERT_CHILD(x,z,cy) VALUES (7,-7,11);
INSERT INTO INSERT_CHILD(x,z,cy) VALUES (7,-7,6);
INSERT INTO INSERT_CHILD(x,z,cy) VALUES (6,-7,7);
INSERT INTO INSERT_CHILD(x,y,z,cy) VALUES (6,'check failed',-6,7);

SELECT * FROM INSERT_CHILD;

DROP TABLE INSERT_CHILD;

--
-- Check NO INHERIT type of constraints and inheritance
--

CREATE TABLE ATACC1 (TEST INT
	CHECK (TEST > 0) NO INHERIT);

CREATE TABLE ATACC2 (TEST2 INT) INHERITS (ATACC1);
-- check constraint is not there on child
INSERT INTO ATACC2 (TEST) VALUES (-3);
-- check constraint is there on parent
INSERT INTO ATACC1 (TEST) VALUES (-3);
DROP TABLE ATACC1 CASCADE;

CREATE TABLE ATACC1 (TEST INT, TEST2 INT
	CHECK (TEST > 0), CHECK (TEST2 > 10) NO INHERIT);

CREATE TABLE ATACC2 () INHERITS (ATACC1);
-- check constraint is there on child
INSERT INTO ATACC2 (TEST) VALUES (-3);
-- check constraint is there on parent
INSERT INTO ATACC1 (TEST) VALUES (-3);
-- check constraint is not there on child
INSERT INTO ATACC2 (TEST2) VALUES (3);
-- check constraint is there on parent
INSERT INTO ATACC1 (TEST2) VALUES (3);
DROP TABLE ATACC1 CASCADE;

--
-- Check constraints on INSERT INTO
--

DELETE FROM INSERT_TBL;

ALTER SEQUENCE INSERT_SEQ RESTART WITH 4;

CREATE TEMP TABLE tmp (xd INT, yd TEXT, zd INT);

INSERT INTO tmp VALUES (null, 'Y', null);
INSERT INTO tmp VALUES (5, '!check failed', null);
INSERT INTO tmp VALUES (null, 'try again', null);
INSERT INTO INSERT_TBL(y) select yd from tmp;

SELECT * FROM INSERT_TBL;

INSERT INTO INSERT_TBL SELECT * FROM tmp WHERE yd = 'try again';
INSERT INTO INSERT_TBL(y,z) SELECT yd, -7 FROM tmp WHERE yd = 'try again';
INSERT INTO INSERT_TBL(y,z) SELECT yd, -8 FROM tmp WHERE yd = 'try again';

SELECT * FROM INSERT_TBL;

DROP TABLE tmp;

--
-- Check constraints on UPDATE
--

UPDATE INSERT_TBL SET x = NULL WHERE x = 5;
UPDATE INSERT_TBL SET x = 6 WHERE x = 6;
UPDATE INSERT_TBL SET x = -z, z = -x;
UPDATE INSERT_TBL SET x = z, z = x;

SELECT * FROM INSERT_TBL;

-- DROP TABLE INSERT_TBL;

--
-- Check constraints on COPY FROM
--

CREATE TABLE COPY_TBL (x INT, y TEXT, z INT,
	CONSTRAINT COPY_CON
	CHECK (x > 3 AND y <> 'check failed' AND x < 7 ));

\set filename :abs_srcdir '/data/constro.data'
COPY COPY_TBL FROM :'filename';

SELECT * FROM COPY_TBL;

\set filename :abs_srcdir '/data/constrf.data'
COPY COPY_TBL FROM :'filename';

SELECT * FROM COPY_TBL;

--
-- Primary keys
--

CREATE TABLE PRIMARY_TBL (i int PRIMARY KEY, t text);

INSERT INTO PRIMARY_TBL VALUES (1, 'one');
INSERT INTO PRIMARY_TBL VALUES (2, 'two');
INSERT INTO PRIMARY_TBL VALUES (1, 'three');
INSERT INTO PRIMARY_TBL VALUES (4, 'three');
INSERT INTO PRIMARY_TBL VALUES (5, 'one');
INSERT INTO PRIMARY_TBL (t) VALUES ('six');

SELECT * FROM PRIMARY_TBL;

DROP TABLE PRIMARY_TBL;

CREATE TABLE PRIMARY_TBL (i int, t text,
	PRIMARY KEY(i,t));

INSERT INTO PRIMARY_TBL VALUES (1, 'one');
INSERT INTO PRIMARY_TBL VALUES (2, 'two');
INSERT INTO PRIMARY_TBL VALUES (1, 'three');
INSERT INTO PRIMARY_TBL VALUES (4, 'three');
INSERT INTO PRIMARY_TBL VALUES (5, 'one');
INSERT INTO PRIMARY_TBL (t) VALUES ('six');

SELECT * FROM PRIMARY_TBL;

DROP TABLE PRIMARY_TBL;

--
-- Unique keys
--

CREATE TABLE UNIQUE_TBL (i int UNIQUE, t text);

INSERT INTO UNIQUE_TBL VALUES (1, 'one');
INSERT INTO UNIQUE_TBL VALUES (2, 'two');
INSERT INTO UNIQUE_TBL VALUES (1, 'three');
INSERT INTO UNIQUE_TBL VALUES (4, 'four');
INSERT INTO UNIQUE_TBL VALUES (5, 'one');
INSERT INTO UNIQUE_TBL (t) VALUES ('six');
INSERT INTO UNIQUE_TBL (t) VALUES ('seven');

INSERT INTO UNIQUE_TBL VALUES (5, 'five-upsert-insert') ON CONFLICT (i) DO UPDATE SET t = 'five-upsert-update';
INSERT INTO UNIQUE_TBL VALUES (6, 'six-upsert-insert') ON CONFLICT (i) DO UPDATE SET t = 'six-upsert-update';
-- should fail
INSERT INTO UNIQUE_TBL VALUES (1, 'a'), (2, 'b'), (2, 'b') ON CONFLICT (i) DO UPDATE SET t = 'fails';

SELECT * FROM UNIQUE_TBL;

DROP TABLE UNIQUE_TBL;

CREATE TABLE UNIQUE_TBL (i int UNIQUE NULLS NOT DISTINCT, t text);

INSERT INTO UNIQUE_TBL VALUES (1, 'one');
INSERT INTO UNIQUE_TBL VALUES (2, 'two');
INSERT INTO UNIQUE_TBL VALUES (1, 'three');  -- fail
INSERT INTO UNIQUE_TBL VALUES (4, 'four');
INSERT INTO UNIQUE_TBL VALUES (5, 'one');
INSERT INTO UNIQUE_TBL (t) VALUES ('six');
INSERT INTO UNIQUE_TBL (t) VALUES ('seven');  -- fail
INSERT INTO UNIQUE_TBL (t) VALUES ('eight') ON CONFLICT DO NOTHING;  -- no-op

SELECT * FROM UNIQUE_TBL;

DROP TABLE UNIQUE_TBL;

CREATE TABLE UNIQUE_TBL (i int, t text,
	UNIQUE(i,t));

INSERT INTO UNIQUE_TBL VALUES (1, 'one');
INSERT INTO UNIQUE_TBL VALUES (2, 'two');
INSERT INTO UNIQUE_TBL VALUES (1, 'three');
INSERT INTO UNIQUE_TBL VALUES (1, 'one');
INSERT INTO UNIQUE_TBL VALUES (5, 'one');
INSERT INTO UNIQUE_TBL (t) VALUES ('six');

SELECT * FROM UNIQUE_TBL;

DROP TABLE UNIQUE_TBL;

--
-- Deferrable unique constraints
--

CREATE TABLE unique_tbl (i int UNIQUE DEFERRABLE, t text);

INSERT INTO unique_tbl VALUES (0, 'one');
INSERT INTO unique_tbl VALUES (1, 'two');
INSERT INTO unique_tbl VALUES (2, 'tree');
INSERT INTO unique_tbl VALUES (3, 'four');
INSERT INTO unique_tbl VALUES (4, 'five');

BEGIN;

-- default is immediate so this should fail right away
UPDATE unique_tbl SET i = 1 WHERE i = 0;

ROLLBACK;

-- check is done at end of statement, so this should succeed
UPDATE unique_tbl SET i = i+1;

SELECT * FROM unique_tbl;

-- explicitly defer the constraint
BEGIN;

SET CONSTRAINTS unique_tbl_i_key DEFERRED;

INSERT INTO unique_tbl VALUES (3, 'three');
DELETE FROM unique_tbl WHERE t = 'tree'; -- makes constraint valid again

COMMIT; -- should succeed

SELECT * FROM unique_tbl;

-- try adding an initially deferred constraint
ALTER TABLE unique_tbl DROP CONSTRAINT unique_tbl_i_key;
ALTER TABLE unique_tbl ADD CONSTRAINT unique_tbl_i_key
	UNIQUE (i) DEFERRABLE INITIALLY DEFERRED;

BEGIN;

INSERT INTO unique_tbl VALUES (1, 'five');
INSERT INTO unique_tbl VALUES (5, 'one');
UPDATE unique_tbl SET i = 4 WHERE i = 2;
UPDATE unique_tbl SET i = 2 WHERE i = 4 AND t = 'four';
DELETE FROM unique_tbl WHERE i = 1 AND t = 'one';
DELETE FROM unique_tbl WHERE i = 5 AND t = 'five';

COMMIT;

SELECT * FROM unique_tbl;

-- should fail at commit-time
BEGIN;
INSERT INTO unique_tbl VALUES (3, 'Three'); -- should succeed for now
COMMIT; -- should fail

-- make constraint check immediate
BEGIN;

SET CONSTRAINTS ALL IMMEDIATE;

INSERT INTO unique_tbl VALUES (3, 'Three'); -- should fail

COMMIT;

-- forced check when SET CONSTRAINTS is called
BEGIN;

SET CONSTRAINTS ALL DEFERRED;

INSERT INTO unique_tbl VALUES (3, 'Three'); -- should succeed for now

SET CONSTRAINTS ALL IMMEDIATE; -- should fail

COMMIT;

-- test deferrable UNIQUE with a partitioned table
CREATE TABLE parted_uniq_tbl (i int UNIQUE DEFERRABLE) partition by range (i);
CREATE TABLE parted_uniq_tbl_1 PARTITION OF parted_uniq_tbl FOR VALUES FROM (0) TO (10);
CREATE TABLE parted_uniq_tbl_2 PARTITION OF parted_uniq_tbl FOR VALUES FROM (20) TO (30);
SELECT conname, conrelid::regclass FROM pg_constraint
  WHERE conname LIKE 'parted_uniq%' ORDER BY conname;
BEGIN;
INSERT INTO parted_uniq_tbl VALUES (1);
SAVEPOINT f;
INSERT INTO parted_uniq_tbl VALUES (1);	-- unique violation
ROLLBACK TO f;
SET CONSTRAINTS parted_uniq_tbl_i_key DEFERRED;
INSERT INTO parted_uniq_tbl VALUES (1);	-- OK now, fail at commit
COMMIT;
DROP TABLE parted_uniq_tbl;

-- test naming a constraint in a partition when a conflict exists
CREATE TABLE parted_fk_naming (
    id bigint NOT NULL default 1,
    id_abc bigint,
    CONSTRAINT dummy_constr FOREIGN KEY (id_abc)
        REFERENCES parted_fk_naming (id),
    PRIMARY KEY (id)
)
PARTITION BY LIST (id);
CREATE TABLE parted_fk_naming_1 (
    id bigint NOT NULL default 1,
    id_abc bigint,
    PRIMARY KEY (id),
    CONSTRAINT dummy_constr CHECK (true)
);
ALTER TABLE parted_fk_naming ATTACH PARTITION parted_fk_naming_1 FOR VALUES IN ('1');
SELECT conname FROM pg_constraint WHERE conrelid = 'parted_fk_naming_1'::regclass AND contype = 'f';
DROP TABLE parted_fk_naming;

--
-- Test various ways to create primary keys on partitions, linked to unique
-- indexes (without constraints) on the partitioned table.  Ideally these should
-- fail, but we don't dare change released behavior, so instead cope with it at
-- DETACH time.
CREATE TEMP TABLE t (a integer, b integer) PARTITION BY HASH (a, b);
CREATE TEMP TABLE tp (a integer, b integer, PRIMARY KEY (a, b), UNIQUE (b, a));
ALTER TABLE t ATTACH PARTITION tp FOR VALUES WITH (MODULUS 1, REMAINDER 0);
CREATE UNIQUE INDEX t_a_idx ON t (a, b);
CREATE UNIQUE INDEX t_b_idx ON t (b, a);
ALTER INDEX t_a_idx ATTACH PARTITION tp_pkey;
ALTER INDEX t_b_idx ATTACH PARTITION tp_b_a_key;
SELECT conname, conparentid, conislocal, coninhcount
  FROM pg_constraint WHERE conname IN ('tp_pkey', 'tp_b_a_key')
  ORDER BY conname DESC;
ALTER TABLE t DETACH PARTITION tp;
DROP TABLE t, tp;

CREATE TEMP TABLE t (a integer) PARTITION BY LIST (a);
CREATE TEMP TABLE tp (a integer PRIMARY KEY);
CREATE UNIQUE INDEX t_a_idx ON t (a);
ALTER TABLE t ATTACH PARTITION tp FOR VALUES IN (1);
ALTER TABLE t DETACH PARTITION tp;
DROP TABLE t, tp;

CREATE TEMP TABLE t (a integer) PARTITION BY LIST (a);
CREATE TEMP TABLE tp (a integer PRIMARY KEY);
CREATE UNIQUE INDEX t_a_idx ON ONLY t (a);
ALTER TABLE t ATTACH PARTITION tp FOR VALUES IN (1);
ALTER TABLE t DETACH PARTITION tp;
DROP TABLE t, tp;

CREATE TABLE regress_constr_partitioned (a integer) PARTITION BY LIST (a);
CREATE TABLE regress_constr_partition1 PARTITION OF regress_constr_partitioned FOR VALUES IN (1);
ALTER TABLE regress_constr_partition1 ADD PRIMARY KEY (a);
CREATE UNIQUE INDEX ON regress_constr_partitioned (a);
BEGIN;
ALTER TABLE regress_constr_partitioned DETACH PARTITION regress_constr_partition1;
ROLLBACK;
--  Leave this one in funny state for pg_upgrade testing

-- test a HOT update that invalidates the conflicting tuple.
-- the trigger should still fire and catch the violation

BEGIN;

INSERT INTO unique_tbl VALUES (3, 'Three'); -- should succeed for now
UPDATE unique_tbl SET t = 'THREE' WHERE i = 3 AND t = 'Three';

COMMIT; -- should fail

SELECT * FROM unique_tbl;

-- test a HOT update that modifies the newly inserted tuple,
-- but should succeed because we then remove the other conflicting tuple.

BEGIN;

INSERT INTO unique_tbl VALUES(3, 'tree'); -- should succeed for now
UPDATE unique_tbl SET t = 'threex' WHERE t = 'tree';
DELETE FROM unique_tbl WHERE t = 'three';

SELECT * FROM unique_tbl;

COMMIT;

SELECT * FROM unique_tbl;

-- enforceability cannot be specified or set for unique constraint
CREATE TABLE UNIQUE_EN_TBL(i int UNIQUE ENFORCED);
CREATE TABLE UNIQUE_NOTEN_TBL(i int UNIQUE NOT ENFORCED);
ALTER TABLE unique_tbl ALTER CONSTRAINT unique_tbl_i_key ENFORCED;
ALTER TABLE unique_tbl ALTER CONSTRAINT unique_tbl_i_key NOT ENFORCED;

-- can't make an existing constraint NOT VALID
ALTER TABLE unique_tbl ALTER CONSTRAINT unique_tbl_i_key NOT VALID;

DROP TABLE unique_tbl;

--
-- EXCLUDE constraints
--

CREATE TABLE circles (
  c1 CIRCLE,
  c2 TEXT,
  EXCLUDE USING gist
    (c1 WITH &&, (c2::circle) WITH &&)
    WHERE (circle_center(c1) <> '(0,0)')
);

-- these should succeed because they don't match the index predicate
INSERT INTO circles VALUES('<(0,0), 5>', '<(0,0), 5>');
INSERT INTO circles VALUES('<(0,0), 5>', '<(0,0), 4>');

-- succeed
INSERT INTO circles VALUES('<(10,10), 10>', '<(0,0), 5>');
-- fail, overlaps
INSERT INTO circles VALUES('<(20,20), 10>', '<(0,0), 4>');
-- succeed, because violation is ignored
INSERT INTO circles VALUES('<(20,20), 10>', '<(0,0), 4>')
  ON CONFLICT ON CONSTRAINT circles_c1_c2_excl DO NOTHING;
-- fail, because DO UPDATE variant requires unique index
INSERT INTO circles VALUES('<(20,20), 10>', '<(0,0), 4>')
  ON CONFLICT ON CONSTRAINT circles_c1_c2_excl DO UPDATE SET c2 = EXCLUDED.c2;
-- succeed because c1 doesn't overlap
INSERT INTO circles VALUES('<(20,20), 1>', '<(0,0), 5>');
-- succeed because c2 doesn't overlap
INSERT INTO circles VALUES('<(20,20), 10>', '<(10,10), 5>');

-- should fail on existing data without the WHERE clause
ALTER TABLE circles ADD EXCLUDE USING gist
  (c1 WITH &&, (c2::circle) WITH &&);

-- try reindexing an existing constraint
REINDEX INDEX circles_c1_c2_excl;

DROP TABLE circles;

-- Check deferred exclusion constraint

CREATE TABLE deferred_excl (
  f1 int,
  f2 int,
  CONSTRAINT deferred_excl_con EXCLUDE (f1 WITH =) INITIALLY DEFERRED
);

INSERT INTO deferred_excl VALUES(1);
INSERT INTO deferred_excl VALUES(2);
INSERT INTO deferred_excl VALUES(1); -- fail
INSERT INTO deferred_excl VALUES(1) ON CONFLICT ON CONSTRAINT deferred_excl_con DO NOTHING; -- fail
BEGIN;
INSERT INTO deferred_excl VALUES(2); -- no fail here
COMMIT; -- should fail here
BEGIN;
INSERT INTO deferred_excl VALUES(3);
INSERT INTO deferred_excl VALUES(3); -- no fail here
COMMIT; -- should fail here

-- bug #13148: deferred constraint versus HOT update
BEGIN;
INSERT INTO deferred_excl VALUES(2, 1); -- no fail here
DELETE FROM deferred_excl WHERE f1 = 2 AND f2 IS NULL; -- remove old row
UPDATE deferred_excl SET f2 = 2 WHERE f1 = 2;
COMMIT; -- should not fail

SELECT * FROM deferred_excl;

ALTER TABLE deferred_excl DROP CONSTRAINT deferred_excl_con;

-- This should fail, but worth testing because of HOT updates
UPDATE deferred_excl SET f1 = 3;

ALTER TABLE deferred_excl ADD EXCLUDE (f1 WITH =);

DROP TABLE deferred_excl;

-- verify constraints created for NOT NULL clauses
CREATE TABLE notnull_tbl1 (a INTEGER NOT NULL NOT NULL);
\d+ notnull_tbl1
-- specifying an existing constraint is a no-op
ALTER TABLE notnull_tbl1 ADD CONSTRAINT notnull_tbl1_a_not_null NOT NULL a;
-- but using a different constraint name is not allowed
ALTER TABLE notnull_tbl1 ADD CONSTRAINT nn NOT NULL a;
\d+ notnull_tbl1
-- duplicate name
ALTER TABLE notnull_tbl1 ADD COLUMN b INT CONSTRAINT notnull_tbl1_a_not_null NOT NULL;
-- DROP NOT NULL gets rid of both the attnotnull flag and the constraint itself
ALTER TABLE notnull_tbl1 ALTER a DROP NOT NULL;
\d+ notnull_tbl1
-- SET NOT NULL puts both back
ALTER TABLE notnull_tbl1 ALTER a SET NOT NULL;
\d+ notnull_tbl1
-- Doing it twice doesn't create a redundant constraint
ALTER TABLE notnull_tbl1 ALTER a SET NOT NULL;
select conname, contype, conkey from pg_constraint where conrelid = 'notnull_tbl1'::regclass;
-- Using the "table constraint" syntax also works
ALTER TABLE notnull_tbl1 ALTER a DROP NOT NULL;
ALTER TABLE notnull_tbl1 ADD CONSTRAINT foobar NOT NULL a;
\d+ notnull_tbl1
DROP TABLE notnull_tbl1;

-- Verify that constraint names and NO INHERIT are properly considered when
-- multiple constraint are specified, either explicitly or via SERIAL/PK/etc,
-- and that conflicting cases are rejected.  Mind that table constraints
-- handle this separately from column constraints.
create table notnull_tbl1 (a int primary key constraint foo not null);
\d+ notnull_tbl1
create table notnull_tbl2 (a serial, constraint foo not null a);
\d+ notnull_tbl2
create table notnull_tbl3 (constraint foo not null a, a int generated by default as identity);
\d+ notnull_tbl3
create table notnull_tbl4 (a int not null constraint foo not null);
\d+ notnull_tbl4
create table notnull_tbl5 (a int constraint foo not null constraint foo not null);
\d+ notnull_tbl5
create table notnull_tbl6 (like notnull_tbl1, constraint foo not null a);
\d+ notnull_tbl6
drop table notnull_tbl2, notnull_tbl3, notnull_tbl4, notnull_tbl5, notnull_tbl6;

-- error cases:
create table notnull_tbl_fail (a serial constraint foo not null constraint bar not null);
create table notnull_tbl_fail (a serial constraint foo not null no inherit constraint foo not null);
create table notnull_tbl_fail (a int constraint foo not null, constraint foo not null a no inherit);
create table notnull_tbl_fail (a serial constraint foo not null, constraint bar not null a);
create table notnull_tbl_fail (a serial, constraint foo not null a, constraint bar not null a);
create table notnull_tbl_fail (a serial, constraint foo not null a no inherit);
create table notnull_tbl_fail (a serial not null no inherit);
create table notnull_tbl_fail (like notnull_tbl1, constraint foo2 not null a);
create table notnull_tbl_fail (a int primary key constraint foo not null no inherit);
create table notnull_tbl_fail (a int not null no inherit primary key);
create table notnull_tbl_fail (a int primary key, not null a no inherit);
create table notnull_tbl_fail (a int, primary key(a), not null a no inherit);
create table notnull_tbl_fail (a int generated by default as identity, constraint foo not null a no inherit);
create table notnull_tbl_fail (a int generated by default as identity not null no inherit);

drop table notnull_tbl1;

-- NOT NULL NO INHERIT
CREATE TABLE ATACC1 (a int, NOT NULL a NO INHERIT);
CREATE TABLE ATACC2 () INHERITS (ATACC1);
\d+ ATACC2
DROP TABLE ATACC1, ATACC2;
CREATE TABLE ATACC1 (a int);
ALTER TABLE ATACC1 ADD NOT NULL a NO INHERIT;
CREATE TABLE ATACC2 () INHERITS (ATACC1);
\d+ ATACC2
DROP TABLE ATACC1, ATACC2;
CREATE TABLE ATACC1 (a int);
CREATE TABLE ATACC2 () INHERITS (ATACC1);
ALTER TABLE ATACC1 ADD NOT NULL a NO INHERIT;
\d+ ATACC2
CREATE TABLE ATACC3 (PRIMARY KEY (a)) INHERITS (ATACC1);
\d+ ATACC3
DROP TABLE ATACC1, ATACC2, ATACC3;

-- NOT NULL NO INHERIT is not possible on partitioned tables
CREATE TABLE ATACC1 (a int NOT NULL NO INHERIT) PARTITION BY LIST (a);
CREATE TABLE ATACC1 (a int, NOT NULL a NO INHERIT) PARTITION BY LIST (a);

-- it's not possible to override a no-inherit constraint with an inheritable one
CREATE TABLE ATACC2 (a int, CONSTRAINT a_is_not_null NOT NULL a NO INHERIT);
CREATE TABLE ATACC1 (a int);
CREATE TABLE ATACC3 (a int) INHERITS (ATACC2);
ALTER TABLE ATACC2 INHERIT ATACC1;
-- can't override
ALTER TABLE ATACC1 ADD CONSTRAINT ditto NOT NULL a;
-- dropping the NO INHERIT constraint allows this to work
ALTER TABLE ATACC2 DROP CONSTRAINT a_is_not_null;
ALTER TABLE ATACC1 ADD CONSTRAINT ditto NOT NULL a;
\d+ ATACC3
DROP TABLE ATACC1, ATACC2, ATACC3;

-- Can't have two constraints with the same name
CREATE TABLE notnull_tbl2 (a INTEGER CONSTRAINT blah NOT NULL, b INTEGER CONSTRAINT blah NOT NULL);

-- can't drop not-null in primary key
CREATE TABLE notnull_tbl2 (a INTEGER PRIMARY KEY);
ALTER TABLE notnull_tbl2 ALTER a DROP NOT NULL;
DROP TABLE notnull_tbl2;

CREATE TABLE notnull_tbl3 (a INTEGER NOT NULL, CHECK (a IS NOT NULL));
ALTER TABLE notnull_tbl3 ALTER A DROP NOT NULL;
ALTER TABLE notnull_tbl3 ADD b int, ADD CONSTRAINT pk PRIMARY KEY (a, b);
\d notnull_tbl3
ALTER TABLE notnull_tbl3 DROP CONSTRAINT pk;
\d notnull_tbl3

-- Primary keys cause not-null constraints to be created.
CREATE TABLE cnn_pk (a int, b int);
CREATE TABLE cnn_pk_child () INHERITS (cnn_pk);
ALTER TABLE cnn_pk ADD CONSTRAINT cnn_primarykey PRIMARY KEY (b);
\d+ cnn_pk*
ALTER TABLE cnn_pk DROP CONSTRAINT cnn_primarykey;
\d+ cnn_pk*
DROP TABLE cnn_pk, cnn_pk_child;

-- As above, but create the primary key ahead of time
CREATE TABLE cnn_pk (a int, b int, CONSTRAINT cnn_primarykey PRIMARY KEY (b));
CREATE TABLE cnn_pk_child () INHERITS (cnn_pk);
\d+ cnn_pk*
ALTER TABLE cnn_pk DROP CONSTRAINT cnn_primarykey;
\d+ cnn_pk*
DROP TABLE cnn_pk, cnn_pk_child;

-- As above, but create the primary key using a UNIQUE index
CREATE TABLE cnn_pk (a int, b int);
CREATE UNIQUE INDEX cnn_uq ON cnn_pk (b);
CREATE TABLE cnn_pk_child () INHERITS (cnn_pk);
ALTER TABLE cnn_pk ADD CONSTRAINT cnn_primarykey PRIMARY KEY USING INDEX cnn_uq;
\d+ cnn_pk*
DROP TABLE cnn_pk, cnn_pk_child;

-- Unique constraints don't give raise to not-null constraints, however.
create table cnn_uq (a int);
alter table cnn_uq add unique (a);
\d+ cnn_uq
drop table cnn_uq;
create table cnn_uq (a int);
create unique index cnn_uq_idx on cnn_uq (a);
alter table cnn_uq add unique using index cnn_uq_idx;
\d+ cnn_uq

-- can't create a primary key on a noinherit not-null
create table cnn_pk (a int not null no inherit);
alter table cnn_pk add primary key (a);
drop table cnn_pk;

-- Ensure partitions are scanned for null values when adding a PK
create table cnn2_parted(a int) partition by list (a);
create table cnn_part1 partition of cnn2_parted for values in (1, null);
insert into cnn_part1 values (null);
alter table cnn2_parted add primary key (a);
drop table cnn2_parted;

-- columns in regular and LIKE inheritance should be marked not-nullable
-- for primary keys, even if those are deferred
CREATE TABLE notnull_tbl4 (a INTEGER PRIMARY KEY INITIALLY DEFERRED);
CREATE TABLE notnull_tbl4_lk (LIKE notnull_tbl4);
CREATE TABLE notnull_tbl4_lk2 (LIKE notnull_tbl4 INCLUDING INDEXES);
CREATE TABLE notnull_tbl4_lk3 (LIKE notnull_tbl4 INCLUDING INDEXES, NOT NULL a);
ALTER TABLE notnull_tbl4_lk3 RENAME CONSTRAINT notnull_tbl4_a_not_null TO a_nn;
CREATE TABLE notnull_tbl4_cld () INHERITS (notnull_tbl4);
CREATE TABLE notnull_tbl4_cld2 (PRIMARY KEY (a) DEFERRABLE) INHERITS (notnull_tbl4);
CREATE TABLE notnull_tbl4_cld3 (PRIMARY KEY (a) DEFERRABLE, CONSTRAINT a_nn NOT NULL a) INHERITS (notnull_tbl4);
\d+ notnull_tbl4
\d+ notnull_tbl4_lk
\d+ notnull_tbl4_lk2
\d+ notnull_tbl4_lk3
\d+ notnull_tbl4_cld
\d+ notnull_tbl4_cld2
\d+ notnull_tbl4_cld3
-- leave these tables around for pg_upgrade testing

-- It's possible to remove a constraint from parents without affecting children
CREATE TABLE notnull_tbl5 (a int CONSTRAINT ann NOT NULL,
	b int CONSTRAINT bnn NOT NULL);
CREATE TABLE notnull_tbl5_child () INHERITS (notnull_tbl5);
ALTER TABLE ONLY notnull_tbl5 DROP CONSTRAINT ann;
ALTER TABLE ONLY notnull_tbl5 ALTER b DROP NOT NULL;
\d+ notnull_tbl5_child
CREATE TABLE notnull_tbl6 (a int CONSTRAINT ann NOT NULL,
	b int CONSTRAINT bnn NOT NULL, check (a > 0)) PARTITION BY LIST (a);
CREATE TABLE notnull_tbl6_1 PARTITION OF notnull_tbl6 FOR VALUES IN (1);
ALTER TABLE ONLY notnull_tbl6 DROP CONSTRAINT ann;
ALTER TABLE ONLY notnull_tbl6 ALTER b DROP NOT NULL;
\d+ notnull_tbl6_1


-- NOT NULL NOT VALID
PREPARE get_nnconstraint_info(regclass[]) AS
SELECT conrelid::regclass as tabname, conname, convalidated, conislocal, coninhcount
FROM  pg_constraint
WHERE conrelid = ANY($1)
ORDER BY conrelid::regclass::text COLLATE "C", conname;

CREATE TABLE notnull_tbl1 (a int, b int);
INSERT INTO notnull_tbl1 VALUES (NULL, 1), (300, 3);
ALTER TABLE notnull_tbl1 ADD CONSTRAINT nn NOT NULL a; -- error
ALTER TABLE notnull_tbl1 ADD CONSTRAINT nn NOT NULL a NOT VALID; -- ok
-- even an invalid not-null forbids new nulls
INSERT INTO notnull_tbl1 VALUES (NULL, 4);
\d+ notnull_tbl1

-- If we have an invalid constraint, we can't have another
ALTER TABLE notnull_tbl1 ADD CONSTRAINT nn1 NOT NULL a NOT VALID NO INHERIT;
ALTER TABLE notnull_tbl1 ADD CONSTRAINT nn NOT NULL a;

-- cannot add primary key on a column with an invalid not-null
ALTER TABLE notnull_tbl1 ADD PRIMARY KEY (a);

-- cannot set column as generated-as-identity if it has an invalid not-null
ALTER TABLE notnull_tbl1 ALTER COLUMN a ADD GENERATED ALWAYS AS IDENTITY;

-- ALTER column SET NOT NULL validates an invalid constraint (but this fails
-- because of rows with null values)
ALTER TABLE notnull_tbl1 ALTER a SET NOT NULL;
\d+ notnull_tbl1

-- Creating a derived table using LIKE gets the constraint, but it's valid
CREATE TABLE notnull_tbl1_copy (LIKE notnull_tbl1);
EXECUTE get_nnconstraint_info('{notnull_tbl1_copy}');

-- An inheritance child table gets the constraint, but it's valid
CREATE TABLE notnull_tbl1_child (a int, b int) INHERITS (notnull_tbl1);
EXECUTE get_nnconstraint_info('{notnull_tbl1_child, notnull_tbl1}');

-- Also try inheritance added after table creation
CREATE TABLE notnull_tbl1_child2 (c int, b int, a int);
ALTER TABLE notnull_tbl1_child2 INHERIT notnull_tbl1;	-- nope
ALTER TABLE notnull_tbl1_child2 ADD NOT NULL a NOT VALID;
ALTER TABLE notnull_tbl1_child2 INHERIT notnull_tbl1;
EXECUTE get_nnconstraint_info('{notnull_tbl1_child2}');

--table rewrite won't validate invalid constraint
ALTER TABLE notnull_tbl1 ADD column d float8 default random();

-- VALIDATE CONSTRAINT scans the table
ALTER TABLE notnull_tbl1 VALIDATE CONSTRAINT nn; -- error, nulls exist
UPDATE notnull_tbl1 SET a = 100 WHERE b = 1;
ALTER TABLE notnull_tbl1 VALIDATE CONSTRAINT nn; -- now ok
EXECUTE get_nnconstraint_info('{notnull_tbl1}');

--- now we can add primary key
ALTER TABLE notnull_tbl1 ADD PRIMARY KEY (a);
DROP TABLE notnull_tbl1, notnull_tbl1_child, notnull_tbl1_child2;

-- dropping an invalid constraint is possible
CREATE TABLE notnull_tbl1 (a int, b int);
ALTER TABLE notnull_tbl1 ADD NOT NULL a NOT VALID,
	ADD NOT NULL b NOT VALID;
ALTER TABLE notnull_tbl1 ALTER a DROP NOT NULL;
ALTER TABLE notnull_tbl1 DROP CONSTRAINT notnull_tbl1_b_not_null;
DROP TABLE notnull_tbl1;

-- ALTER .. NO INHERIT works for invalid constraints
CREATE TABLE notnull_tbl1 (a int);
CREATE TABLE notnull_tbl1_chld () INHERITS (notnull_tbl1);
ALTER TABLE notnull_tbl1 ADD CONSTRAINT nntbl1_a NOT NULL a NOT VALID;
ALTER TABLE notnull_tbl1 ALTER CONSTRAINT nntbl1_a NO INHERIT;

-- DROP CONSTRAINT recurses correctly on invalid constraints
ALTER TABLE notnull_tbl1 ALTER CONSTRAINT nntbl1_a INHERIT;
ALTER TABLE notnull_tbl1 DROP CONSTRAINT nntbl1_a;
DROP TABLE notnull_tbl1, notnull_tbl1_chld;

-- if a parent has a valid not null constraint then a child table cannot
-- have an invalid one
CREATE TABLE notnull_tbl1 (a int);
ALTER TABLE notnull_tbl1 ADD CONSTRAINT nn_parent NOT NULL a not valid;
CREATE TABLE notnull_chld0 (a int, CONSTRAINT nn_chld0 NOT NULL a);
ALTER TABLE notnull_tbl1 INHERIT notnull_chld0; --error

ALTER TABLE notnull_chld0 DROP CONSTRAINT nn_chld0;
ALTER TABLE notnull_chld0 ADD CONSTRAINT nn_chld0 NOT NULL a not valid;
ALTER TABLE notnull_tbl1 INHERIT notnull_chld0; --now ok

-- parents and child not-null will all be validated.
ALTER TABLE notnull_tbl1 VALIDATE CONSTRAINT nn_parent;
EXECUTE get_nnconstraint_info('{notnull_tbl1, notnull_chld0}');
DROP TABLE notnull_tbl1, notnull_chld0;

-- Test invalid not null on inheritance table.
CREATE TABLE notnull_inhparent (i int);
CREATE TABLE notnull_inhchild (i int) INHERITS (notnull_inhparent);
CREATE TABLE notnull_inhgrand () INHERITS (notnull_inhparent, notnull_inhchild);
ALTER TABLE notnull_inhparent ADD CONSTRAINT nn NOT NULL i NOT VALID;
ALTER TABLE notnull_inhchild ADD CONSTRAINT nn1 NOT NULL i; -- error
EXECUTE get_nnconstraint_info('{notnull_inhparent, notnull_inhchild, notnull_inhgrand}');
ALTER TABLE notnull_inhparent ALTER i SET NOT NULL; -- ok
EXECUTE get_nnconstraint_info('{notnull_inhparent, notnull_inhchild, notnull_inhgrand}');
DROP TABLE notnull_inhparent, notnull_inhchild, notnull_inhgrand;

-- Verify NOT NULL VALID/NOT VALID with partition table.
DROP TABLE notnull_tbl1;
CREATE TABLE notnull_tbl1 (a int, b int) PARTITION BY LIST (a);
ALTER TABLE notnull_tbl1 ADD CONSTRAINT notnull_con NOT NULL a NOT VALID; --ok
CREATE TABLE notnull_tbl1_1 PARTITION OF notnull_tbl1 FOR VALUES IN (1,2);
CREATE TABLE notnull_tbl1_2(a int, CONSTRAINT nn2 NOT NULL a, b int);
ALTER TABLE notnull_tbl1 ATTACH PARTITION notnull_tbl1_2 FOR VALUES IN (3,4);

CREATE TABLE notnull_tbl1_3(a int, b int);
INSERT INTO notnull_tbl1_3 values(NULL,1);
ALTER TABLE notnull_tbl1_3 add CONSTRAINT nn3 NOT NULL a NOT VALID;
ALTER TABLE notnull_tbl1 ATTACH PARTITION notnull_tbl1_3 FOR VALUES IN (NULL,5);

EXECUTE get_nnconstraint_info('{notnull_tbl1, notnull_tbl1_1, notnull_tbl1_2, notnull_tbl1_3}');
ALTER TABLE notnull_tbl1 ALTER COLUMN a SET NOT NULL; --error, notnull_tbl1_3 have null values
ALTER TABLE notnull_tbl1_3 VALIDATE CONSTRAINT nn3; --error

TRUNCATE notnull_tbl1;
ALTER TABLE notnull_tbl1 ALTER COLUMN a SET NOT NULL; --OK

EXECUTE get_nnconstraint_info('{notnull_tbl1, notnull_tbl1_1, notnull_tbl1_2, notnull_tbl1_3}');
DROP TABLE notnull_tbl1;

-- partitioned table have not-null, then the partitions can not be NOT NULL NOT VALID.
CREATE TABLE pp_nn (a int, b int, NOT NULL a) PARTITION BY LIST (a);
CREATE TABLE pp_nn_1(a int, b int);
ALTER TABLE pp_nn_1 ADD CONSTRAINT nn1 NOT NULL a NOT VALID;
ALTER TABLE pp_nn ATTACH PARTITION pp_nn_1 FOR VALUES IN (NULL,5); --error
ALTER TABLE pp_nn_1 VALIDATE CONSTRAINT nn1;
ALTER TABLE pp_nn ATTACH PARTITION pp_nn_1 FOR VALUES IN (NULL,5); --ok
DROP TABLE pp_nn;

-- Try a partition with an invalid constraint and create a PK on the parent.
CREATE TABLE pp_nn (a int) PARTITION BY HASH (a);
CREATE TABLE pp_nn_1 PARTITION OF pp_nn FOR VALUES WITH (MODULUS 2, REMAINDER 0);
ALTER TABLE pp_nn_1 ADD CONSTRAINT nn NOT NULL a NOT VALID;
ALTER TABLE ONLY pp_nn ADD PRIMARY KEY (a);
DROP TABLE pp_nn;

-- same as above, but the constraint is NO INHERIT
CREATE TABLE pp_nn (a int) PARTITION BY HASH (a);
CREATE TABLE pp_nn_1 PARTITION OF pp_nn FOR VALUES WITH (MODULUS 2, REMAINDER 0);
ALTER TABLE pp_nn_1 ADD CONSTRAINT nn NOT NULL a NO INHERIT;
ALTER TABLE ONLY pp_nn ADD PRIMARY KEY (a);
DROP TABLE pp_nn;

-- Create table with NOT NULL INVALID constraint, for pg_upgrade.
CREATE TABLE notnull_tbl1_upg (a int, b int);
INSERT INTO notnull_tbl1_upg VALUES (NULL, 1), (NULL, 2), (300, 3);
ALTER TABLE notnull_tbl1_upg ADD CONSTRAINT nn NOT NULL a NOT VALID;
-- Inherit test for pg_upgrade
CREATE TABLE notnull_parent_upg (a int);
CREATE TABLE notnull_child_upg () INHERITS (notnull_parent_upg);
ALTER TABLE notnull_child_upg ADD CONSTRAINT nn NOT NULL a;
ALTER TABLE notnull_parent_upg ADD CONSTRAINT nn NOT NULL a NOT VALID;
SELECT conrelid::regclass, contype, convalidated, conislocal
FROM pg_catalog.pg_constraint
WHERE conrelid in ('notnull_parent_upg'::regclass, 'notnull_child_upg'::regclass)
ORDER BY 1;

-- Partition table test, for pg_upgrade
CREATE TABLE notnull_part1_upg (a int, b int) PARTITION BY LIST (a);
ALTER TABLE notnull_part1_upg ADD CONSTRAINT notnull_con NOT NULL a NOT VALID; --ok
CREATE TABLE notnull_part1_1_upg PARTITION OF notnull_part1_upg FOR VALUES IN (1,2);
CREATE TABLE notnull_part1_2_upg (a int, CONSTRAINT nn2 NOT NULL a, b int);
ALTER TABLE notnull_part1_upg ATTACH PARTITION notnull_part1_2_upg FOR VALUES IN (3,4);
CREATE TABLE notnull_part1_3_upg (a int, b int);
INSERT INTO notnull_part1_3_upg values(NULL,1);
ALTER TABLE notnull_part1_3_upg add CONSTRAINT nn3 NOT NULL a NOT VALID;
ALTER TABLE notnull_part1_upg ATTACH PARTITION notnull_part1_3_upg FOR VALUES IN (NULL,5);
EXECUTE get_nnconstraint_info('{notnull_part1_upg, notnull_part1_1_upg, notnull_part1_2_upg, notnull_part1_3_upg}');

-- Inheritance test tables for pg_upgrade
create table constr_parent (a int);
create table constr_child (a int) inherits (constr_parent);
alter table constr_parent add not null a not valid;
alter table constr_child validate constraint constr_parent_a_not_null;
EXECUTE get_nnconstraint_info('{constr_parent, constr_child}');

create table constr_parent2 (a int);
create table constr_child2 () inherits (constr_parent2);
alter table constr_parent2 add not null a not valid;
alter table constr_child2 validate constraint constr_parent2_a_not_null;
EXECUTE get_nnconstraint_info('{constr_parent2, constr_child2}');

create table constr_parent3 (a int not null);
create table constr_child3 () inherits (constr_parent2, constr_parent3);
EXECUTE get_nnconstraint_info('{constr_parent3, constr_child3}');

COMMENT ON CONSTRAINT constr_parent2_a_not_null ON constr_parent2 IS 'this constraint is invalid';
COMMENT ON CONSTRAINT constr_parent2_a_not_null ON constr_child2 IS 'this constraint is valid';

DEALLOCATE get_nnconstraint_info;

-- end NOT NULL NOT VALID


-- Comments
-- Setup a low-level role to enforce non-superuser checks.
CREATE ROLE regress_constraint_comments;
SET SESSION AUTHORIZATION regress_constraint_comments;

CREATE TABLE constraint_comments_tbl (a int CONSTRAINT the_constraint CHECK (a > 0));
CREATE DOMAIN constraint_comments_dom AS int CONSTRAINT the_constraint CHECK (value > 0);

COMMENT ON CONSTRAINT the_constraint ON constraint_comments_tbl IS 'yes, the comment';
COMMENT ON CONSTRAINT the_constraint ON DOMAIN constraint_comments_dom IS 'yes, another comment';

-- no such constraint
COMMENT ON CONSTRAINT no_constraint ON constraint_comments_tbl IS 'yes, the comment';
COMMENT ON CONSTRAINT no_constraint ON DOMAIN constraint_comments_dom IS 'yes, another comment';

-- no such table/domain
COMMENT ON CONSTRAINT the_constraint ON no_comments_tbl IS 'bad comment';
COMMENT ON CONSTRAINT the_constraint ON DOMAIN no_comments_dom IS 'another bad comment';

COMMENT ON CONSTRAINT the_constraint ON constraint_comments_tbl IS NULL;
COMMENT ON CONSTRAINT the_constraint ON DOMAIN constraint_comments_dom IS NULL;

-- unauthorized user
RESET SESSION AUTHORIZATION;
CREATE ROLE regress_constraint_comments_noaccess;
SET SESSION AUTHORIZATION regress_constraint_comments_noaccess;
COMMENT ON CONSTRAINT the_constraint ON constraint_comments_tbl IS 'no, the comment';
COMMENT ON CONSTRAINT the_constraint ON DOMAIN constraint_comments_dom IS 'no, another comment';
RESET SESSION AUTHORIZATION;

DROP TABLE constraint_comments_tbl;
DROP DOMAIN constraint_comments_dom;

DROP ROLE regress_constraint_comments;
DROP ROLE regress_constraint_comments_noaccess;

-- Leave some constraints for the pg_upgrade test to pick up
CREATE DOMAIN constraint_comments_dom AS int;

ALTER DOMAIN constraint_comments_dom ADD CONSTRAINT inv_ck CHECK (value > 0) NOT VALID;
COMMENT ON CONSTRAINT inv_ck ON DOMAIN constraint_comments_dom IS 'comment on invalid constraint';

-- Create a table that exercises pg_upgrade
CREATE TABLE regress_notnull1 (a integer);
CREATE TABLE regress_notnull2 () INHERITS (regress_notnull1);
ALTER TABLE ONLY regress_notnull2 ALTER COLUMN a SET NOT NULL;
