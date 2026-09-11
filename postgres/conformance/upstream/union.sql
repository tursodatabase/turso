--
-- UNION (also INTERSECT, EXCEPT)
--

-- Simple UNION constructs

SELECT 1 AS two UNION SELECT 2 ORDER BY 1;

SELECT 1 AS one UNION SELECT 1 ORDER BY 1;

SELECT 1 AS two UNION ALL SELECT 2;

SELECT 1 AS two UNION ALL SELECT 1;

SELECT 1 AS three UNION SELECT 2 UNION SELECT 3 ORDER BY 1;

SELECT 1 AS two UNION SELECT 2 UNION SELECT 2 ORDER BY 1;

SELECT 1 AS three UNION SELECT 2 UNION ALL SELECT 2 ORDER BY 1;

SELECT 1.1 AS two UNION SELECT 2.2 ORDER BY 1;

-- Mixed types

SELECT 1.1 AS two UNION SELECT 2 ORDER BY 1;

SELECT 1 AS two UNION SELECT 2.2 ORDER BY 1;

SELECT 1 AS one UNION SELECT 1.0::float8 ORDER BY 1;

SELECT 1.1 AS two UNION ALL SELECT 2 ORDER BY 1;

SELECT 1.0::float8 AS two UNION ALL SELECT 1 ORDER BY 1;

SELECT 1.1 AS three UNION SELECT 2 UNION SELECT 3 ORDER BY 1;

SELECT 1.1::float8 AS two UNION SELECT 2 UNION SELECT 2.0::float8 ORDER BY 1;

SELECT 1.1 AS three UNION SELECT 2 UNION ALL SELECT 2 ORDER BY 1;

SELECT 1.1 AS two UNION (SELECT 2 UNION ALL SELECT 2) ORDER BY 1;

--
-- Try testing from tables...
--

SELECT f1 AS five FROM FLOAT8_TBL
UNION
SELECT f1 FROM FLOAT8_TBL
ORDER BY 1;

SELECT f1 AS ten FROM FLOAT8_TBL
UNION ALL
SELECT f1 FROM FLOAT8_TBL;

SELECT f1 AS nine FROM FLOAT8_TBL
UNION
SELECT f1 FROM INT4_TBL
ORDER BY 1;

SELECT f1 AS ten FROM FLOAT8_TBL
UNION ALL
SELECT f1 FROM INT4_TBL;

SELECT f1 AS five FROM FLOAT8_TBL
  WHERE f1 BETWEEN -1e6 AND 1e6
UNION
SELECT f1 FROM INT4_TBL
  WHERE f1 BETWEEN 0 AND 1000000
ORDER BY 1;

SELECT CAST(f1 AS char(4)) AS three FROM VARCHAR_TBL
UNION
SELECT f1 FROM CHAR_TBL
ORDER BY 1;

SELECT f1 AS three FROM VARCHAR_TBL
UNION
SELECT CAST(f1 AS varchar) FROM CHAR_TBL
ORDER BY 1;

SELECT f1 AS eight FROM VARCHAR_TBL
UNION ALL
SELECT f1 FROM CHAR_TBL;

SELECT f1 AS five FROM TEXT_TBL
UNION
SELECT f1 FROM VARCHAR_TBL
UNION
SELECT TRIM(TRAILING FROM f1) FROM CHAR_TBL
ORDER BY 1;

--
-- INTERSECT and EXCEPT
--

SELECT q2 FROM int8_tbl INTERSECT SELECT q1 FROM int8_tbl ORDER BY 1;

SELECT q2 FROM int8_tbl INTERSECT ALL SELECT q1 FROM int8_tbl ORDER BY 1;

SELECT q2 FROM int8_tbl EXCEPT SELECT q1 FROM int8_tbl ORDER BY 1;

SELECT q2 FROM int8_tbl EXCEPT ALL SELECT q1 FROM int8_tbl ORDER BY 1;

SELECT q2 FROM int8_tbl EXCEPT ALL SELECT DISTINCT q1 FROM int8_tbl ORDER BY 1;

SELECT q1 FROM int8_tbl EXCEPT SELECT q2 FROM int8_tbl ORDER BY 1;

SELECT q1 FROM int8_tbl EXCEPT ALL SELECT q2 FROM int8_tbl ORDER BY 1;

SELECT q1 FROM int8_tbl EXCEPT ALL SELECT DISTINCT q2 FROM int8_tbl ORDER BY 1;

SELECT q1 FROM int8_tbl EXCEPT ALL SELECT q1 FROM int8_tbl FOR NO KEY UPDATE;

-- nested cases
(SELECT 1,2,3 UNION SELECT 4,5,6) INTERSECT SELECT 4,5,6;
(SELECT 1,2,3 UNION SELECT 4,5,6 ORDER BY 1,2) INTERSECT SELECT 4,5,6;
(SELECT 1,2,3 UNION SELECT 4,5,6) EXCEPT SELECT 4,5,6;
(SELECT 1,2,3 UNION SELECT 4,5,6 ORDER BY 1,2) EXCEPT SELECT 4,5,6;

-- exercise both hashed and sorted implementations of UNION/INTERSECT/EXCEPT

set enable_hashagg to on;

explain (costs off)
select count(*) from
  ( select unique1 from tenk1 union select fivethous from tenk1 ) ss;
select count(*) from
  ( select unique1 from tenk1 union select fivethous from tenk1 ) ss;

explain (costs off)
select count(*) from
  ( select unique1 from tenk1 intersect select fivethous from tenk1 ) ss;
select count(*) from
  ( select unique1 from tenk1 intersect select fivethous from tenk1 ) ss;

-- this query will prefer a sorted setop unless we force it.
set enable_indexscan to off;

explain (costs off)
select unique1 from tenk1 except select unique2 from tenk1 where unique2 != 10;
select unique1 from tenk1 except select unique2 from tenk1 where unique2 != 10;

reset enable_indexscan;

-- the hashed implementation is sensitive to child plans' tuple slot types
explain (costs off)
select * from int8_tbl intersect select q2, q1 from int8_tbl order by 1, 2;
select * from int8_tbl intersect select q2, q1 from int8_tbl order by 1, 2;
select q2, q1 from int8_tbl intersect select * from int8_tbl order by 1, 2;

set enable_hashagg to off;

explain (costs off)
select count(*) from
  ( select unique1 from tenk1 union select fivethous from tenk1 ) ss;
select count(*) from
  ( select unique1 from tenk1 union select fivethous from tenk1 ) ss;

explain (costs off)
select count(*) from
  ( select unique1 from tenk1 intersect select fivethous from tenk1 ) ss;
select count(*) from
  ( select unique1 from tenk1 intersect select fivethous from tenk1 ) ss;

explain (costs off)
select unique1 from tenk1 except select unique2 from tenk1 where unique2 != 10;
select unique1 from tenk1 except select unique2 from tenk1 where unique2 != 10;

explain (costs off)
select f1 from int4_tbl union all
  (select unique1 from tenk1 union select unique2 from tenk1);

reset enable_hashagg;

-- non-hashable type
set enable_hashagg to on;

explain (costs off)
select x from (values ('11'::varbit), ('10'::varbit)) _(x) union select x from (values ('11'::varbit), ('10'::varbit)) _(x);

set enable_hashagg to off;

explain (costs off)
select x from (values ('11'::varbit), ('10'::varbit)) _(x) union select x from (values ('11'::varbit), ('10'::varbit)) _(x);

reset enable_hashagg;

-- arrays
set enable_hashagg to on;

explain (costs off)
select x from (values (array[1, 2]), (array[1, 3])) _(x) union select x from (values (array[1, 2]), (array[1, 4])) _(x);
select x from (values (array[1, 2]), (array[1, 3])) _(x) union select x from (values (array[1, 2]), (array[1, 4])) _(x);
explain (costs off)
select x from (values (array[1, 2]), (array[1, 3])) _(x) intersect select x from (values (array[1, 2]), (array[1, 4])) _(x);
select x from (values (array[1, 2]), (array[1, 3])) _(x) intersect select x from (values (array[1, 2]), (array[1, 4])) _(x);
explain (costs off)
select x from (values (array[1, 2]), (array[1, 3])) _(x) except select x from (values (array[1, 2]), (array[1, 4])) _(x);
select x from (values (array[1, 2]), (array[1, 3])) _(x) except select x from (values (array[1, 2]), (array[1, 4])) _(x);

-- non-hashable type
explain (costs off)
select x from (values (array['10'::varbit]), (array['11'::varbit])) _(x) union select x from (values (array['10'::varbit]), (array['01'::varbit])) _(x);
select x from (values (array['10'::varbit]), (array['11'::varbit])) _(x) union select x from (values (array['10'::varbit]), (array['01'::varbit])) _(x);

set enable_hashagg to off;

explain (costs off)
select x from (values (array[1, 2]), (array[1, 3])) _(x) union select x from (values (array[1, 2]), (array[1, 4])) _(x);
select x from (values (array[1, 2]), (array[1, 3])) _(x) union select x from (values (array[1, 2]), (array[1, 4])) _(x);
explain (costs off)
select x from (values (array[1, 2]), (array[1, 3])) _(x) intersect select x from (values (array[1, 2]), (array[1, 4])) _(x);
select x from (values (array[1, 2]), (array[1, 3])) _(x) intersect select x from (values (array[1, 2]), (array[1, 4])) _(x);
explain (costs off)
select x from (values (array[1, 2]), (array[1, 3])) _(x) except select x from (values (array[1, 2]), (array[1, 4])) _(x);
select x from (values (array[1, 2]), (array[1, 3])) _(x) except select x from (values (array[1, 2]), (array[1, 4])) _(x);

reset enable_hashagg;

-- records
set enable_hashagg to on;

explain (costs off)
select x from (values (row(1, 2)), (row(1, 3))) _(x) union select x from (values (row(1, 2)), (row(1, 4))) _(x);
select x from (values (row(1, 2)), (row(1, 3))) _(x) union select x from (values (row(1, 2)), (row(1, 4))) _(x);
explain (costs off)
select x from (values (row(1, 2)), (row(1, 3))) _(x) intersect select x from (values (row(1, 2)), (row(1, 4))) _(x);
select x from (values (row(1, 2)), (row(1, 3))) _(x) intersect select x from (values (row(1, 2)), (row(1, 4))) _(x);
explain (costs off)
select x from (values (row(1, 2)), (row(1, 3))) _(x) except select x from (values (row(1, 2)), (row(1, 4))) _(x);
select x from (values (row(1, 2)), (row(1, 3))) _(x) except select x from (values (row(1, 2)), (row(1, 4))) _(x);

-- non-hashable type

-- With an anonymous row type, the typcache does not report that the
-- type is hashable.  (Otherwise, this would fail at execution time.)
explain (costs off)
select x from (values (row('10'::varbit)), (row('11'::varbit))) _(x) union select x from (values (row('10'::varbit)), (row('01'::varbit))) _(x);
select x from (values (row('10'::varbit)), (row('11'::varbit))) _(x) union select x from (values (row('10'::varbit)), (row('01'::varbit))) _(x);

-- With a defined row type, the typcache can inspect the type's fields
-- for hashability.
create type ct1 as (f1 varbit);
explain (costs off)
select x from (values (row('10'::varbit)::ct1), (row('11'::varbit)::ct1)) _(x) union select x from (values (row('10'::varbit)::ct1), (row('01'::varbit)::ct1)) _(x);
select x from (values (row('10'::varbit)::ct1), (row('11'::varbit)::ct1)) _(x) union select x from (values (row('10'::varbit)::ct1), (row('01'::varbit)::ct1)) _(x);
drop type ct1;

set enable_hashagg to off;

explain (costs off)
select x from (values (row(1, 2)), (row(1, 3))) _(x) union select x from (values (row(1, 2)), (row(1, 4))) _(x);
select x from (values (row(1, 2)), (row(1, 3))) _(x) union select x from (values (row(1, 2)), (row(1, 4))) _(x);
explain (costs off)
select x from (values (row(1, 2)), (row(1, 3))) _(x) intersect select x from (values (row(1, 2)), (row(1, 4))) _(x);
select x from (values (row(1, 2)), (row(1, 3))) _(x) intersect select x from (values (row(1, 2)), (row(1, 4))) _(x);
explain (costs off)
select x from (values (row(1, 2)), (row(1, 3))) _(x) except select x from (values (row(1, 2)), (row(1, 4))) _(x);
select x from (values (row(1, 2)), (row(1, 3))) _(x) except select x from (values (row(1, 2)), (row(1, 4))) _(x);

-- non-sortable type

-- Ensure we get a HashAggregate plan.  Keep enable_hashagg=off to ensure
-- there's no chance of a sort.
explain (costs off) select '123'::xid union select '123'::xid;

reset enable_hashagg;

--
-- Mixed types
--

SELECT f1 FROM float8_tbl INTERSECT SELECT f1 FROM int4_tbl ORDER BY 1;

SELECT f1 FROM float8_tbl EXCEPT SELECT f1 FROM int4_tbl ORDER BY 1;

--
-- Operator precedence and (((((extra))))) parentheses
--

SELECT q1 FROM int8_tbl INTERSECT SELECT q2 FROM int8_tbl UNION ALL SELECT q2 FROM int8_tbl  ORDER BY 1;

SELECT q1 FROM int8_tbl INTERSECT (((SELECT q2 FROM int8_tbl UNION ALL SELECT q2 FROM int8_tbl))) ORDER BY 1;

(((SELECT q1 FROM int8_tbl INTERSECT SELECT q2 FROM int8_tbl ORDER BY 1))) UNION ALL SELECT q2 FROM int8_tbl;

SELECT q1 FROM int8_tbl UNION ALL SELECT q2 FROM int8_tbl EXCEPT SELECT q1 FROM int8_tbl ORDER BY 1;

SELECT q1 FROM int8_tbl UNION ALL (((SELECT q2 FROM int8_tbl EXCEPT SELECT q1 FROM int8_tbl ORDER BY 1)));

(((SELECT q1 FROM int8_tbl UNION ALL SELECT q2 FROM int8_tbl))) EXCEPT SELECT q1 FROM int8_tbl ORDER BY 1;

--
-- Subqueries with ORDER BY & LIMIT clauses
--

-- In this syntax, ORDER BY/LIMIT apply to the result of the EXCEPT
SELECT q1,q2 FROM int8_tbl EXCEPT SELECT q2,q1 FROM int8_tbl
ORDER BY q2,q1;

-- This should fail, because q2 isn't a name of an EXCEPT output column
SELECT q1 FROM int8_tbl EXCEPT SELECT q2 FROM int8_tbl ORDER BY q2 LIMIT 1;

-- But this should work:
SELECT q1 FROM int8_tbl EXCEPT (((SELECT q2 FROM int8_tbl ORDER BY q2 LIMIT 1))) ORDER BY 1;

--
-- New syntaxes (7.1) permit new tests
--

(((((select * from int8_tbl)))));

--
-- Check behavior with empty select list (allowed since 9.4)
--

select union select;
select intersect select;
select except select;

-- check hashed implementation
set enable_hashagg = true;
set enable_sort = false;

-- We've no way to check hashed UNION as the empty pathkeys in the Append are
-- fine to make use of Unique, which is cheaper than HashAggregate and we've
-- no means to disable Unique.
explain (costs off)
select from generate_series(1,5) intersect select from generate_series(1,3);

select from generate_series(1,5) union all select from generate_series(1,3);
select from generate_series(1,5) intersect select from generate_series(1,3);
select from generate_series(1,5) intersect all select from generate_series(1,3);
select from generate_series(1,5) except select from generate_series(1,3);
select from generate_series(1,5) except all select from generate_series(1,3);

-- check sorted implementation
set enable_hashagg = false;
set enable_sort = true;

explain (costs off)
select from generate_series(1,5) union select from generate_series(1,3);
explain (costs off)
select from generate_series(1,5) intersect select from generate_series(1,3);

select from generate_series(1,5) union select from generate_series(1,3);
select from generate_series(1,5) union all select from generate_series(1,3);
select from generate_series(1,5) intersect select from generate_series(1,3);
select from generate_series(1,5) intersect all select from generate_series(1,3);
select from generate_series(1,5) except select from generate_series(1,3);
select from generate_series(1,5) except all select from generate_series(1,3);

-- Try a variation of the above but with a CTE which contains a column, again
-- with an empty final select list.

-- Ensure we get the expected 1 row with 0 columns
with cte as materialized (select s from generate_series(1,5) s)
select from cte union select from cte;

-- Ensure we get the same result as the above.
with cte as not materialized (select s from generate_series(1,5) s)
select from cte union select from cte;

reset enable_hashagg;
reset enable_sort;

--
-- Check handling of a case with unknown constants.  We don't guarantee
-- an undecorated constant will work in all cases, but historically this
-- usage has worked, so test we don't break it.
--

SELECT a.f1 FROM (SELECT 'test' AS f1 FROM varchar_tbl) a
UNION
SELECT b.f1 FROM (SELECT f1 FROM varchar_tbl) b
ORDER BY 1;

-- This should fail, but it should produce an error cursor
SELECT '3.4'::numeric UNION SELECT 'foo';

--
-- Test that expression-index constraints can be pushed down through
-- UNION or UNION ALL
--

CREATE TEMP TABLE t1 (a text, b text);
CREATE INDEX t1_ab_idx on t1 ((a || b));
CREATE TEMP TABLE t2 (ab text primary key);
INSERT INTO t1 VALUES ('a', 'b'), ('x', 'y');
INSERT INTO t2 VALUES ('ab'), ('xy');

set enable_seqscan = off;
set enable_indexscan = on;
set enable_bitmapscan = off;
set enable_sort = off;

explain (costs off)
 SELECT * FROM
 (SELECT a || b AS ab FROM t1
  UNION ALL
  SELECT * FROM t2) t
 WHERE ab = 'ab';

explain (costs off)
 SELECT * FROM
 (SELECT a || b AS ab FROM t1
  UNION
  SELECT * FROM t2) t
 WHERE ab = 'ab';

--
-- Test that ORDER BY for UNION ALL can be pushed down to inheritance
-- children.
--

CREATE TEMP TABLE t1c (b text, a text);
ALTER TABLE t1c INHERIT t1;
CREATE TEMP TABLE t2c (primary key (ab)) INHERITS (t2);
INSERT INTO t1c VALUES ('v', 'w'), ('c', 'd'), ('m', 'n'), ('e', 'f');
INSERT INTO t2c VALUES ('vw'), ('cd'), ('mn'), ('ef');
CREATE INDEX t1c_ab_idx on t1c ((a || b));

set enable_seqscan = on;
set enable_indexonlyscan = off;

explain (costs off)
  SELECT * FROM
  (SELECT a || b AS ab FROM t1
   UNION ALL
   SELECT ab FROM t2) t
  ORDER BY 1 LIMIT 8;

  SELECT * FROM
  (SELECT a || b AS ab FROM t1
   UNION ALL
   SELECT ab FROM t2) t
  ORDER BY 1 LIMIT 8;

reset enable_seqscan;
reset enable_indexscan;
reset enable_bitmapscan;
reset enable_sort;

-- This simpler variant of the above test has been observed to fail differently

create table events (event_id int primary key);
create table other_events (event_id int primary key);
create table events_child () inherits (events);

explain (costs off)
select event_id
 from (select event_id from events
       union all
       select event_id from other_events) ss
 order by event_id;

drop table events_child, events, other_events;

reset enable_indexonlyscan;

-- Test constraint exclusion of UNION ALL subqueries
explain (costs off)
 SELECT * FROM
  (SELECT 1 AS t, * FROM tenk1 a
   UNION ALL
   SELECT 2 AS t, * FROM tenk1 b) c
 WHERE t = 2;

-- Test that we push quals into UNION sub-selects only when it's safe
explain (costs off)
SELECT * FROM
  (SELECT 1 AS t, 2 AS x
   UNION
   SELECT 2 AS t, 4 AS x) ss
WHERE x < 4
ORDER BY x;

SELECT * FROM
  (SELECT 1 AS t, 2 AS x
   UNION
   SELECT 2 AS t, 4 AS x) ss
WHERE x < 4
ORDER BY x;

explain (costs off)
SELECT * FROM
  (SELECT 1 AS t, generate_series(1,10) AS x
   UNION
   SELECT 2 AS t, 4 AS x) ss
WHERE x < 4
ORDER BY x;

SELECT * FROM
  (SELECT 1 AS t, generate_series(1,10) AS x
   UNION
   SELECT 2 AS t, 4 AS x) ss
WHERE x < 4
ORDER BY x;

explain (costs off)
SELECT * FROM
  (SELECT 1 AS t, (random()*3)::int AS x
   UNION
   SELECT 2 AS t, 4 AS x) ss
WHERE x > 3
ORDER BY x;

SELECT * FROM
  (SELECT 1 AS t, (random()*3)::int AS x
   UNION
   SELECT 2 AS t, 4 AS x) ss
WHERE x > 3
ORDER BY x;

-- Test cases where the native ordering of a sub-select has more pathkeys
-- than the outer query cares about
explain (costs off)
select distinct q1 from
  (select distinct * from int8_tbl i81
   union all
   select distinct * from int8_tbl i82) ss
where q2 = q2;

select distinct q1 from
  (select distinct * from int8_tbl i81
   union all
   select distinct * from int8_tbl i82) ss
where q2 = q2;

explain (costs off)
select distinct q1 from
  (select distinct * from int8_tbl i81
   union all
   select distinct * from int8_tbl i82) ss
where -q1 = q2;

select distinct q1 from
  (select distinct * from int8_tbl i81
   union all
   select distinct * from int8_tbl i82) ss
where -q1 = q2;

-- Test proper handling of parameterized appendrel paths when the
-- potential join qual is expensive
create function expensivefunc(int) returns int
language plpgsql immutable strict cost 10000
as $$begin return $1; end$$;

create temp table t3 as select generate_series(-1000,1000) as x;
create index t3i on t3 (expensivefunc(x));
analyze t3;

explain (costs off)
select * from
  (select * from t3 a union all select * from t3 b) ss
  join int4_tbl on f1 = expensivefunc(x);
select * from
  (select * from t3 a union all select * from t3 b) ss
  join int4_tbl on f1 = expensivefunc(x);

drop table t3;
drop function expensivefunc(int);

-- Test handling of appendrel quals that const-simplify into an AND
explain (costs off)
select * from
  (select *, 0 as x from int8_tbl a
   union all
   select *, 1 as x from int8_tbl b) ss
where (x = 0) or (q1 >= q2 and q1 <= q2);
select * from
  (select *, 0 as x from int8_tbl a
   union all
   select *, 1 as x from int8_tbl b) ss
where (x = 0) or (q1 >= q2 and q1 <= q2);

--
-- Test the planner's ability to produce cheap startup plans with Append nodes
--

-- Ensure we get a Nested Loop join between tenk1 and tenk2
explain (costs off)
select t1.unique1 from tenk1 t1
inner join tenk2 t2 on t1.tenthous = t2.tenthous and t2.thousand = 0
   union all
(values(1)) limit 1;

-- Ensure there is no problem if cheapest_startup_path is NULL
explain (costs off)
select * from tenk1 t1
left join lateral
  (select t1.tenthous from tenk2 t2 union all (values(1)))
on true limit 1;

-- Test handling of Vars with varno 0 in estimate_array_length
explain (verbose, costs off)
select null::int[] union all select null::int[] union all select null::bigint[];
