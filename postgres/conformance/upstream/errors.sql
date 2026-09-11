--
-- ERRORS
--

-- bad in postquel, but ok in PostgreSQL
select 1;


--
-- UNSUPPORTED STUFF

-- doesn't work
-- notify pg_class
--

--
-- SELECT

-- this used to be a syntax error, but now we allow an empty target list
select;

-- no such relation
select * from nonesuch;

-- bad name in target list
select nonesuch from pg_database;

-- empty distinct list isn't OK
select distinct from pg_database;

-- bad attribute name on lhs of operator
select * from pg_database where nonesuch = pg_database.datname;

-- bad attribute name on rhs of operator
select * from pg_database where pg_database.datname = nonesuch;

-- bad attribute name in select distinct on
select distinct on (foobar) * from pg_database;

-- grouping with FOR UPDATE
select null from pg_database group by datname for update;
select null from pg_database group by grouping sets (()) for update;


--
-- DELETE

-- missing relation name (this had better not wildcard!)
delete from;

-- no such relation
delete from nonesuch;


--
-- DROP

-- missing relation name (this had better not wildcard!)
drop table;

-- no such relation
drop table nonesuch;


--
-- ALTER TABLE

-- relation renaming

-- missing relation name
alter table rename;

-- no such relation
alter table nonesuch rename to newnonesuch;

-- no such relation
alter table nonesuch rename to stud_emp;

-- conflict
alter table stud_emp rename to student;

-- self-conflict
alter table stud_emp rename to stud_emp;


-- attribute renaming

-- no such relation
alter table nonesuchrel rename column nonesuchatt to newnonesuchatt;

-- no such attribute
alter table emp rename column nonesuchatt to newnonesuchatt;

-- conflict
alter table emp rename column salary to manager;

-- conflict
alter table emp rename column salary to ctid;


--
-- TRANSACTION STUFF

-- not in a xact
abort;

-- not in a xact
end;


--
-- CREATE AGGREGATE

-- sfunc/finalfunc type disagreement
create aggregate newavg2 (sfunc = int4pl,
			  basetype = int4,
			  stype = int4,
			  finalfunc = int2um,
			  initcond = '0');

-- left out basetype
create aggregate newcnt1 (sfunc = int4inc,
			  stype = int4,
			  initcond = '0');


--
-- DROP INDEX

-- missing index name
drop index;

-- bad index name
drop index 314159;

-- no such index
drop index nonesuch;


--
-- DROP AGGREGATE

-- missing aggregate name
drop aggregate;

-- missing aggregate type
drop aggregate newcnt1;

-- bad aggregate name
drop aggregate 314159 (int);

-- bad aggregate type
drop aggregate newcnt (nonesuch);

-- no such aggregate
drop aggregate nonesuch (int4);

-- no such aggregate for type
drop aggregate newcnt (float4);


--
-- DROP FUNCTION

-- missing function name
drop function ();

-- bad function name
drop function 314159();

-- no such function
drop function nonesuch();


--
-- DROP TYPE

-- missing type name
drop type;

-- bad type name
drop type 314159;

-- no such type
drop type nonesuch;


--
-- DROP OPERATOR

-- missing everything
drop operator;

-- bad operator name
drop operator equals;

-- missing type list
drop operator ===;

-- missing parentheses
drop operator int4, int4;

-- missing operator name
drop operator (int4, int4);

-- missing type list contents
drop operator === ();

-- no such operator
drop operator === (int4);

-- no such operator by that name
drop operator === (int4, int4);

-- no such type1
drop operator = (nonesuch);

-- no such type1
drop operator = ( , int4);

-- no such type1
drop operator = (nonesuch, int4);

-- no such type2
drop operator = (int4, nonesuch);

-- no such type2
drop operator = (int4, );


--
-- DROP RULE

-- missing rule name
drop rule;

-- bad rule name
drop rule 314159;

-- no such rule
drop rule nonesuch on noplace;

-- these postquel variants are no longer supported
drop tuple rule nonesuch;
drop instance rule nonesuch on noplace;
drop rewrite rule nonesuch;

--
-- Check that division-by-zero is properly caught.
--

select 1/0;

select 1::int8/0;

select 1/0::int8;

select 1::int2/0;

select 1/0::int2;

select 1::numeric/0;

select 1/0::numeric;

select 1::float8/0;

select 1/0::float8;

select 1::float4/0;

select 1/0::float4;


--
-- Test psql's reporting of syntax error location
--

xxx;

CREATE foo;

CREATE TABLE ;

CREATE TABLE
\g

INSERT INTO foo VALUES(123) foo;

INSERT INTO 123
VALUES(123);

INSERT INTO foo
VALUES(123) 123
;

-- with a tab
CREATE TABLE foo
  (id INT4 UNIQUE NOT NULL, id2 TEXT NOT NULL PRIMARY KEY,
	id3 INTEGER NOT NUL,
   id4 INT4 UNIQUE NOT NULL, id5 TEXT UNIQUE NOT NULL);

-- long line to be truncated on the left
CREATE TABLE foo(id INT4 UNIQUE NOT NULL, id2 TEXT NOT NULL PRIMARY KEY, id3 INTEGER NOT NUL,
id4 INT4 UNIQUE NOT NULL, id5 TEXT UNIQUE NOT NULL);

-- long line to be truncated on the right
CREATE TABLE foo(
id3 INTEGER NOT NUL, id4 INT4 UNIQUE NOT NULL, id5 TEXT UNIQUE NOT NULL, id INT4 UNIQUE NOT NULL, id2 TEXT NOT NULL PRIMARY KEY);

-- long line to be truncated both ways
CREATE TABLE foo(id INT4 UNIQUE NOT NULL, id2 TEXT NOT NULL PRIMARY KEY, id3 INTEGER NOT NUL, id4 INT4 UNIQUE NOT NULL, id5 TEXT UNIQUE NOT NULL);

-- long line to be truncated on the left, many lines
CREATE
TEMPORARY
TABLE
foo(id INT4 UNIQUE NOT NULL, id2 TEXT NOT NULL PRIMARY KEY, id3 INTEGER NOT NUL,
id4 INT4
UNIQUE
NOT
NULL,
id5 TEXT
UNIQUE
NOT
NULL)
;

-- long line to be truncated on the right, many lines
CREATE
TEMPORARY
TABLE
foo(
id3 INTEGER NOT NUL, id4 INT4 UNIQUE NOT NULL, id5 TEXT UNIQUE NOT NULL, id INT4 UNIQUE NOT NULL, id2 TEXT NOT NULL PRIMARY KEY)
;

-- long line to be truncated both ways, many lines
CREATE
TEMPORARY
TABLE
foo
(id
INT4
UNIQUE NOT NULL, idx INT4 UNIQUE NOT NULL, idy INT4 UNIQUE NOT NULL, id2 TEXT NOT NULL PRIMARY KEY, id3 INTEGER NOT NUL, id4 INT4 UNIQUE NOT NULL, id5 TEXT UNIQUE NOT NULL,
idz INT4 UNIQUE NOT NULL,
idv INT4 UNIQUE NOT NULL);

-- more than 10 lines...
CREATE
TEMPORARY
TABLE
foo
(id
INT4
UNIQUE
NOT
NULL
,
idm
INT4
UNIQUE
NOT
NULL,
idx INT4 UNIQUE NOT NULL, idy INT4 UNIQUE NOT NULL, id2 TEXT NOT NULL PRIMARY KEY, id3 INTEGER NOT NUL, id4 INT4 UNIQUE NOT NULL, id5 TEXT UNIQUE NOT NULL,
idz INT4 UNIQUE NOT NULL,
idv
INT4
UNIQUE
NOT
NULL);
