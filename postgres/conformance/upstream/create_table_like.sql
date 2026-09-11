/* Test inheritance of structure (LIKE) */
CREATE TABLE inhx (xx text DEFAULT 'text');

/*
 * Test double inheritance
 *
 * Ensure that defaults are NOT included unless
 * INCLUDING DEFAULTS is specified
 */
CREATE TABLE ctla (aa TEXT);
CREATE TABLE ctlb (bb TEXT) INHERITS (ctla);

CREATE TABLE foo (LIKE nonexistent);

CREATE TABLE inhe (ee text, LIKE inhx) inherits (ctlb);
INSERT INTO inhe VALUES ('ee-col1', 'ee-col2', DEFAULT, 'ee-col4');
SELECT * FROM inhe; /* Columns aa, bb, xx value NULL, ee */
SELECT * FROM inhx; /* Empty set since LIKE inherits structure only */
SELECT * FROM ctlb; /* Has ee entry */
SELECT * FROM ctla; /* Has ee entry */

CREATE TABLE inhf (LIKE inhx, LIKE inhx); /* Throw error */

CREATE TABLE inhf (LIKE inhx INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
INSERT INTO inhf DEFAULT VALUES;
SELECT * FROM inhf; /* Single entry with value 'text' */

ALTER TABLE inhx add constraint foo CHECK (xx = 'text');
ALTER TABLE inhx ADD PRIMARY KEY (xx);
CREATE TABLE inhg (LIKE inhx); /* Doesn't copy constraint */
INSERT INTO inhg VALUES ('foo');
DROP TABLE inhg;
CREATE TABLE inhg (x text, LIKE inhx INCLUDING CONSTRAINTS, y text); /* Copies constraints */
INSERT INTO inhg VALUES ('x', 'text', 'y'); /* Succeeds */
INSERT INTO inhg VALUES ('x', 'text', 'y'); /* Succeeds -- Unique constraints not copied */
INSERT INTO inhg VALUES ('x', 'foo',  'y');  /* fails due to constraint */
SELECT * FROM inhg; /* Two records with three columns in order x=x, xx=text, y=y */
DROP TABLE inhg;

CREATE TABLE test_like_id_1 (a bigint GENERATED ALWAYS AS IDENTITY, b text);
\d test_like_id_1
INSERT INTO test_like_id_1 (b) VALUES ('b1');
SELECT * FROM test_like_id_1;
CREATE TABLE test_like_id_2 (LIKE test_like_id_1);
\d test_like_id_2
INSERT INTO test_like_id_2 (b) VALUES ('b2');
SELECT * FROM test_like_id_2;  -- identity was not copied
CREATE TABLE test_like_id_3 (LIKE test_like_id_1 INCLUDING IDENTITY);
\d test_like_id_3
INSERT INTO test_like_id_3 (b) VALUES ('b3');
SELECT * FROM test_like_id_3;  -- identity was copied and applied
DROP TABLE test_like_id_1, test_like_id_2, test_like_id_3;

CREATE TABLE test_like_gen_1 (a int, b int GENERATED ALWAYS AS (a * 2) STORED, c int GENERATED ALWAYS AS (a * 3) VIRTUAL);
\d test_like_gen_1
INSERT INTO test_like_gen_1 (a) VALUES (1);
SELECT * FROM test_like_gen_1;
CREATE TABLE test_like_gen_2 (LIKE test_like_gen_1);
\d test_like_gen_2
INSERT INTO test_like_gen_2 (a) VALUES (1);
SELECT * FROM test_like_gen_2;
CREATE TABLE test_like_gen_3 (LIKE test_like_gen_1 INCLUDING GENERATED);
\d test_like_gen_3
INSERT INTO test_like_gen_3 (a) VALUES (1);
SELECT * FROM test_like_gen_3;
DROP TABLE test_like_gen_1, test_like_gen_2, test_like_gen_3;

-- also test generated column with a "forward" reference (bug #16342)
CREATE TABLE test_like_4 (b int DEFAULT 42,
  c int GENERATED ALWAYS AS (a * 2) STORED,
  a int CHECK (a > 0));
\d test_like_4
CREATE TABLE test_like_4a (LIKE test_like_4);
CREATE TABLE test_like_4b (LIKE test_like_4 INCLUDING DEFAULTS);
CREATE TABLE test_like_4c (LIKE test_like_4 INCLUDING GENERATED);
CREATE TABLE test_like_4d (LIKE test_like_4 INCLUDING DEFAULTS INCLUDING GENERATED);
\d test_like_4a
INSERT INTO test_like_4a (a) VALUES(11);
SELECT a, b, c FROM test_like_4a;
\d test_like_4b
INSERT INTO test_like_4b (a) VALUES(11);
SELECT a, b, c FROM test_like_4b;
\d test_like_4c
INSERT INTO test_like_4c (a) VALUES(11);
SELECT a, b, c FROM test_like_4c;
\d test_like_4d
INSERT INTO test_like_4d (a) VALUES(11);
SELECT a, b, c FROM test_like_4d;

-- Test renumbering of Vars when combining LIKE with inheritance
CREATE TABLE test_like_5 (x point, y point, z point);
CREATE TABLE test_like_5x (p int CHECK (p > 0),
   q int GENERATED ALWAYS AS (p * 2) STORED);
CREATE TABLE test_like_5c (LIKE test_like_4 INCLUDING ALL)
  INHERITS (test_like_5, test_like_5x);
\d test_like_5c

-- Test updating of column numbers in statistics expressions (bug #18468)
CREATE TABLE test_like_6 (a int, c text, b text);
CREATE STATISTICS ext_stat ON (a || b) FROM test_like_6;
ALTER TABLE test_like_6 DROP COLUMN c;
CREATE TABLE test_like_6c (LIKE test_like_6 INCLUDING ALL);
\d+ test_like_6c

DROP TABLE test_like_4, test_like_4a, test_like_4b, test_like_4c, test_like_4d;
DROP TABLE test_like_5, test_like_5x, test_like_5c;
DROP TABLE test_like_6, test_like_6c;

CREATE TABLE inhg (x text, LIKE inhx INCLUDING INDEXES, y text); /* copies indexes */
INSERT INTO inhg VALUES (5, 10);
INSERT INTO inhg VALUES (20, 10); -- should fail
DROP TABLE inhg;
/* Multiple primary keys creation should fail */
CREATE TABLE inhg (x text, LIKE inhx INCLUDING INDEXES, PRIMARY KEY(x)); /* fails */
CREATE TABLE inhz (xx text DEFAULT 'text', yy int UNIQUE);
CREATE UNIQUE INDEX inhz_xx_idx on inhz (xx) WHERE xx <> 'test';
/* Ok to create multiple unique indexes */
CREATE TABLE inhg (x text UNIQUE, LIKE inhz INCLUDING INDEXES);
INSERT INTO inhg (xx, yy, x) VALUES ('test', 5, 10);
INSERT INTO inhg (xx, yy, x) VALUES ('test', 10, 15);
INSERT INTO inhg (xx, yy, x) VALUES ('foo', 10, 15); -- should fail
DROP TABLE inhg;
DROP TABLE inhz;

/* Use primary key imported by LIKE for self-referential FK constraint */
CREATE TABLE inhz (x text REFERENCES inhz, LIKE inhx INCLUDING INDEXES);
\d inhz
DROP TABLE inhz;

-- including storage and comments
CREATE TABLE ctlt1 (a text CHECK (length(a) > 2) ENFORCED PRIMARY KEY,
	b text CHECK (length(b) > 100) NOT ENFORCED);
ALTER TABLE ctlt1 ADD CONSTRAINT cc CHECK (length(b) > 100) NOT VALID;
CREATE INDEX ctlt1_b_key ON ctlt1 (b);
CREATE INDEX ctlt1_fnidx ON ctlt1 ((a || b));
CREATE STATISTICS ctlt1_a_b_stat ON a,b FROM ctlt1;
CREATE STATISTICS ctlt1_expr_stat ON (a || b) FROM ctlt1;
COMMENT ON STATISTICS ctlt1_a_b_stat IS 'ab stats';
COMMENT ON STATISTICS ctlt1_expr_stat IS 'ab expr stats';
COMMENT ON COLUMN ctlt1.a IS 'A';
COMMENT ON COLUMN ctlt1.b IS 'B';
COMMENT ON CONSTRAINT ctlt1_a_check ON ctlt1 IS 't1_a_check';
COMMENT ON INDEX ctlt1_pkey IS 'index pkey';
COMMENT ON INDEX ctlt1_b_key IS 'index b_key';
ALTER TABLE ctlt1 ALTER COLUMN a SET STORAGE MAIN;

CREATE TABLE ctlt2 (c text NOT NULL);
ALTER TABLE ctlt2 ALTER COLUMN c SET STORAGE EXTERNAL;
COMMENT ON COLUMN ctlt2.c IS 'C';
COMMENT ON CONSTRAINT ctlt2_c_not_null ON ctlt2 IS 't2_c_not_null';

CREATE TABLE ctlt3 (a text CHECK (length(a) < 5), c text CHECK (length(c) < 7));
ALTER TABLE ctlt3 ALTER COLUMN c SET STORAGE EXTERNAL;
ALTER TABLE ctlt3 ALTER COLUMN a SET STORAGE MAIN;
CREATE INDEX ctlt3_fnidx ON ctlt3 ((a || c));
COMMENT ON COLUMN ctlt3.a IS 'A3';
COMMENT ON COLUMN ctlt3.c IS 'C';
COMMENT ON CONSTRAINT ctlt3_a_check ON ctlt3 IS 't3_a_check';

CREATE TABLE ctlt4 (a text, c text);
ALTER TABLE ctlt4 ALTER COLUMN c SET STORAGE EXTERNAL;

CREATE TABLE ctlt12_storage (LIKE ctlt1 INCLUDING STORAGE, LIKE ctlt2 INCLUDING STORAGE);
\d+ ctlt12_storage
CREATE TABLE ctlt12_comments (LIKE ctlt1 INCLUDING COMMENTS, LIKE ctlt2 INCLUDING COMMENTS);
\d+ ctlt12_comments
SELECT conname, description FROM pg_description, pg_constraint c WHERE classoid = 'pg_constraint'::regclass AND objoid = c.oid AND c.conrelid = 'ctlt12_comments'::regclass;
CREATE TABLE ctlt1_inh (LIKE ctlt1 INCLUDING CONSTRAINTS INCLUDING COMMENTS) INHERITS (ctlt1);
\d+ ctlt1_inh
SELECT description FROM pg_description, pg_constraint c WHERE classoid = 'pg_constraint'::regclass AND objoid = c.oid AND c.conrelid = 'ctlt1_inh'::regclass;
CREATE TABLE ctlt13_inh () INHERITS (ctlt1, ctlt3);
\d+ ctlt13_inh
CREATE TABLE ctlt13_like (LIKE ctlt3 INCLUDING CONSTRAINTS INCLUDING INDEXES INCLUDING COMMENTS INCLUDING STORAGE) INHERITS (ctlt1);
\d+ ctlt13_like
SELECT description FROM pg_description, pg_constraint c WHERE classoid = 'pg_constraint'::regclass AND objoid = c.oid AND c.conrelid = 'ctlt13_like'::regclass;

CREATE TABLE ctlt_all (LIKE ctlt1 INCLUDING ALL);
\d+ ctlt_all
SELECT c.relname, objsubid, description FROM pg_description, pg_index i, pg_class c WHERE classoid = 'pg_class'::regclass AND objoid = i.indexrelid AND c.oid = i.indexrelid AND i.indrelid = 'ctlt_all'::regclass ORDER BY c.relname, objsubid;
SELECT s.stxname, objsubid, description FROM pg_description, pg_statistic_ext s WHERE classoid = 'pg_statistic_ext'::regclass AND objoid = s.oid AND s.stxrelid = 'ctlt_all'::regclass ORDER BY s.stxname, objsubid;

CREATE TABLE inh_error1 () INHERITS (ctlt1, ctlt4);
CREATE TABLE inh_error2 (LIKE ctlt4 INCLUDING STORAGE) INHERITS (ctlt1);

-- Check that LIKE isn't confused by a system catalog of the same name
CREATE TABLE pg_attrdef (LIKE ctlt1 INCLUDING ALL);
\d+ public.pg_attrdef
DROP TABLE public.pg_attrdef;

-- Check that LIKE isn't confused when new table masks the old, either
BEGIN;
CREATE SCHEMA ctl_schema;
SET LOCAL search_path = ctl_schema, public;
CREATE TABLE ctlt1 (LIKE ctlt1 INCLUDING ALL);
\d+ ctlt1
ROLLBACK;

DROP TABLE ctlt1, ctlt2, ctlt3, ctlt4, ctlt12_storage, ctlt12_comments, ctlt1_inh, ctlt13_inh, ctlt13_like, ctlt_all, ctla, ctlb CASCADE;

-- LIKE must respect NO INHERIT property of constraints
CREATE TABLE noinh_con_copy (a int CHECK (a > 0) NO INHERIT, b int not null,
	c int not null no inherit);

COMMENT ON CONSTRAINT noinh_con_copy_b_not_null ON noinh_con_copy IS 'not null b';
COMMENT ON CONSTRAINT noinh_con_copy_c_not_null ON noinh_con_copy IS 'not null c no inherit';

CREATE TABLE noinh_con_copy1 (LIKE noinh_con_copy INCLUDING CONSTRAINTS INCLUDING COMMENTS);
\d+ noinh_con_copy1

SELECT conname, description
FROM  pg_description, pg_constraint c
WHERE classoid = 'pg_constraint'::regclass
AND   objoid = c.oid AND c.conrelid = 'noinh_con_copy1'::regclass
ORDER BY conname COLLATE "C";

-- fail, as partitioned tables don't allow NO INHERIT constraints
CREATE TABLE noinh_con_copy1_parted (LIKE noinh_con_copy INCLUDING ALL)
  PARTITION BY LIST (a);

DROP TABLE noinh_con_copy, noinh_con_copy1;


/* LIKE with other relation kinds */

CREATE TABLE ctlt4 (a int, b text);

CREATE SEQUENCE ctlseq1;
CREATE TABLE ctlt10 (LIKE ctlseq1);  -- fail

CREATE VIEW ctlv1 AS SELECT * FROM ctlt4;
CREATE TABLE ctlt11 (LIKE ctlv1);
CREATE TABLE ctlt11a (LIKE ctlv1 INCLUDING ALL);

CREATE TYPE ctlty1 AS (a int, b text);
CREATE TABLE ctlt12 (LIKE ctlty1);

DROP SEQUENCE ctlseq1;
DROP TYPE ctlty1;
DROP VIEW ctlv1;
DROP TABLE IF EXISTS ctlt4, ctlt10, ctlt11, ctlt11a, ctlt12;

--
-- CREATE FOREIGN TABLE LIKE
--
CREATE FOREIGN DATA WRAPPER ctl_dummy;
CREATE SERVER ctl_s0 FOREIGN DATA WRAPPER ctl_dummy;

CREATE TABLE ctl_table(a int PRIMARY KEY,
  b varchar COMPRESSION pglz,
  c int GENERATED ALWAYS AS (a * 2) STORED,
  d bigint GENERATED ALWAYS AS IDENTITY,
  e int DEFAULT 1);

CREATE INDEX ctl_table_a_key ON ctl_table(a);
COMMENT ON COLUMN ctl_table.b IS 'Column b';
CREATE STATISTICS ctl_table_stat ON a,b FROM ctl_table;
ALTER TABLE ctl_table ADD CONSTRAINT foo CHECK (b = 'text');
ALTER TABLE ctl_table ALTER COLUMN b SET STORAGE MAIN;

\d+ ctl_table

-- Test EXCLUDING ALL
CREATE FOREIGN TABLE ctl_foreign_table1(LIKE ctl_table EXCLUDING ALL) SERVER ctl_s0;
\d+ ctl_foreign_table1
-- \d+ does not report the value of attcompression for a foreign table, so
-- check separately.
SELECT attname, attcompression FROM pg_attribute
  WHERE attrelid = 'ctl_foreign_table1'::regclass and attnum > 0 ORDER BY attnum;

-- Test INCLUDING ALL
-- INDEXES, IDENTITY, COMPRESSION, STORAGE are not copied.
CREATE FOREIGN TABLE ctl_foreign_table2(LIKE ctl_table INCLUDING ALL) SERVER ctl_s0;
\d+ ctl_foreign_table2
-- \d+ does not report the value of attcompression for a foreign table, so
-- check separately.
SELECT attname, attcompression FROM pg_attribute
  WHERE attrelid = 'ctl_foreign_table2'::regclass and attnum > 0 ORDER BY attnum;

-- LIKE ... INCLUDING STATISTICS with dropped columns in the parent,
-- so stxkeys attnums are not contiguous.
CREATE TABLE ctl_stats3_parent (a int, b int, c int);
ALTER TABLE ctl_stats3_parent DROP COLUMN b;
CREATE STATISTICS ctl_stats3_stat ON a, c FROM ctl_stats3_parent;
CREATE TABLE ctl_stats3_child (LIKE ctl_stats3_parent INCLUDING STATISTICS);
CREATE TABLE ctl_stats4_parent (a int, b int, c int, d int);
ALTER TABLE ctl_stats4_parent DROP COLUMN b;
CREATE STATISTICS ctl_stats4_stat ON a, c FROM ctl_stats4_parent;
CREATE TABLE ctl_stats4_child (LIKE ctl_stats4_parent INCLUDING STATISTICS);
SELECT s.stxrelid::regclass AS relation,
       array_agg(a.attname ORDER BY u.ord) AS stats_columns
FROM pg_statistic_ext s
CROSS JOIN LATERAL
  unnest(s.stxkeys::int2[]) WITH ORDINALITY AS u(attnum, ord)
JOIN pg_attribute a
  ON a.attrelid = s.stxrelid AND a.attnum = u.attnum
WHERE s.stxrelid IN ('ctl_stats3_child'::regclass,
                     'ctl_stats4_child'::regclass)
GROUP BY s.stxrelid
ORDER BY s.stxrelid::regclass::text;
DROP TABLE ctl_stats3_parent;
DROP TABLE ctl_stats3_child;
DROP TABLE ctl_stats4_parent;
DROP TABLE ctl_stats4_child;

DROP TABLE ctl_table;
DROP FOREIGN TABLE ctl_foreign_table1;
DROP FOREIGN TABLE ctl_foreign_table2;
DROP FOREIGN DATA WRAPPER ctl_dummy CASCADE;
