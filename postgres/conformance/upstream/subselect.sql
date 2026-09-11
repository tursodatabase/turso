--
-- SUBSELECT
--

SELECT 1 AS one WHERE 1 IN (SELECT 1);

SELECT 1 AS zero WHERE 1 NOT IN (SELECT 1);

SELECT 1 AS zero WHERE 1 IN (SELECT 2);

-- Check grammar's handling of extra parens in assorted contexts

SELECT * FROM (SELECT 1 AS x) ss;
SELECT * FROM ((SELECT 1 AS x)) ss;

SELECT * FROM ((SELECT 1 AS x)), ((SELECT * FROM ((SELECT 2 AS y))));

(SELECT 2) UNION SELECT 2;
((SELECT 2)) UNION SELECT 2;

SELECT ((SELECT 2) UNION SELECT 2);
SELECT (((SELECT 2)) UNION SELECT 2);

SELECT (SELECT ARRAY[1,2,3])[1];
SELECT ((SELECT ARRAY[1,2,3]))[2];
SELECT (((SELECT ARRAY[1,2,3])))[3];

-- Set up some simple test tables

CREATE TABLE SUBSELECT_TBL (
  f1 integer,
  f2 integer,
  f3 float
);

INSERT INTO SUBSELECT_TBL VALUES (1, 2, 3);
INSERT INTO SUBSELECT_TBL VALUES (2, 3, 4);
INSERT INTO SUBSELECT_TBL VALUES (3, 4, 5);
INSERT INTO SUBSELECT_TBL VALUES (1, 1, 1);
INSERT INTO SUBSELECT_TBL VALUES (2, 2, 2);
INSERT INTO SUBSELECT_TBL VALUES (3, 3, 3);
INSERT INTO SUBSELECT_TBL VALUES (6, 7, 8);
INSERT INTO SUBSELECT_TBL VALUES (8, 9, NULL);

SELECT * FROM SUBSELECT_TBL;

-- Uncorrelated subselects

SELECT f1 AS "Constant Select" FROM SUBSELECT_TBL
  WHERE f1 IN (SELECT 1);

SELECT f1 AS "Uncorrelated Field" FROM SUBSELECT_TBL
  WHERE f1 IN (SELECT f2 FROM SUBSELECT_TBL);

SELECT f1 AS "Uncorrelated Field" FROM SUBSELECT_TBL
  WHERE f1 IN (SELECT f2 FROM SUBSELECT_TBL WHERE
    f2 IN (SELECT f1 FROM SUBSELECT_TBL));

SELECT f1, f2
  FROM SUBSELECT_TBL
  WHERE (f1, f2) NOT IN (SELECT f2, CAST(f3 AS int4) FROM SUBSELECT_TBL
                         WHERE f3 IS NOT NULL);

-- Correlated subselects

SELECT f1 AS "Correlated Field", f2 AS "Second Field"
  FROM SUBSELECT_TBL upper
  WHERE f1 IN (SELECT f2 FROM SUBSELECT_TBL WHERE f1 = upper.f1);

SELECT f1 AS "Correlated Field", f3 AS "Second Field"
  FROM SUBSELECT_TBL upper
  WHERE f1 IN
    (SELECT f2 FROM SUBSELECT_TBL WHERE CAST(upper.f2 AS float) = f3);

SELECT f1 AS "Correlated Field", f3 AS "Second Field"
  FROM SUBSELECT_TBL upper
  WHERE f3 IN (SELECT upper.f1 + f2 FROM SUBSELECT_TBL
               WHERE f2 = CAST(f3 AS integer));

SELECT f1 AS "Correlated Field"
  FROM SUBSELECT_TBL
  WHERE (f1, f2) IN (SELECT f2, CAST(f3 AS int4) FROM SUBSELECT_TBL
                     WHERE f3 IS NOT NULL);

-- Check ROWCOMPARE cases, both correlated and not

EXPLAIN (VERBOSE, COSTS OFF)
SELECT ROW(1, 2) = (SELECT f1, f2) AS eq FROM SUBSELECT_TBL;

SELECT ROW(1, 2) = (SELECT f1, f2) AS eq FROM SUBSELECT_TBL;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT ROW(1, 2) = (SELECT 3, 4) AS eq FROM SUBSELECT_TBL;

SELECT ROW(1, 2) = (SELECT 3, 4) AS eq FROM SUBSELECT_TBL;

SELECT ROW(1, 2) = (SELECT f1, f2 FROM SUBSELECT_TBL);  -- error

-- Subselects without aliases

SELECT count FROM (SELECT COUNT(DISTINCT name) FROM road);
SELECT COUNT(*) FROM (SELECT DISTINCT name FROM road);

SELECT * FROM (SELECT * FROM int4_tbl), (VALUES (123456)) WHERE f1 = column1;

CREATE VIEW view_unnamed_ss AS
SELECT * FROM (SELECT * FROM (SELECT abs(f1) AS a1 FROM int4_tbl)),
              (SELECT * FROM int8_tbl)
  WHERE a1 < 10 AND q1 > a1 ORDER BY q1, q2;

SELECT * FROM view_unnamed_ss;

\sv view_unnamed_ss

DROP VIEW view_unnamed_ss;

-- Test matching of locking clause to correct alias

CREATE VIEW view_unnamed_ss_locking AS
SELECT * FROM (SELECT * FROM int4_tbl), int8_tbl AS unnamed_subquery
  WHERE f1 = q1
  FOR UPDATE OF unnamed_subquery;

\sv view_unnamed_ss_locking

DROP VIEW view_unnamed_ss_locking;

--
-- Use some existing tables in the regression test
--

SELECT ss.f1 AS "Correlated Field", ss.f3 AS "Second Field"
  FROM SUBSELECT_TBL ss
  WHERE f1 NOT IN (SELECT f1+1 FROM INT4_TBL
                   WHERE f1 != ss.f1 AND f1 < 2147483647);

select q1, float8(count(*)) / (select count(*) from int8_tbl)
from int8_tbl group by q1 order by q1;

-- Unspecified-type literals in output columns should resolve as text

SELECT *, pg_typeof(f1) FROM
  (SELECT 'foo' AS f1 FROM generate_series(1,3)) ss ORDER BY 1;

-- ... unless there's context to suggest differently

explain (verbose, costs off) select '42' union all select '43';
explain (verbose, costs off) select '42' union all select 43;

-- check materialization of an initplan reference (bug #14524)
explain (verbose, costs off)
select 1 = all (select (select 1));
select 1 = all (select (select 1));

--
-- Check EXISTS simplification with LIMIT
--
explain (costs off)
select * from int4_tbl o where exists
  (select 1 from int4_tbl i where i.f1=o.f1 limit null);
explain (costs off)
select * from int4_tbl o where not exists
  (select 1 from int4_tbl i where i.f1=o.f1 limit 1);
explain (costs off)
select * from int4_tbl o where exists
  (select 1 from int4_tbl i where i.f1=o.f1 limit 0);

--
-- Test cases to catch unpleasant interactions between IN-join processing
-- and subquery pullup.
--

select count(*) from
  (select 1 from tenk1 a
   where unique1 IN (select hundred from tenk1 b)) ss;
select count(distinct ss.ten) from
  (select ten from tenk1 a
   where unique1 IN (select hundred from tenk1 b)) ss;
select count(*) from
  (select 1 from tenk1 a
   where unique1 IN (select distinct hundred from tenk1 b)) ss;
select count(distinct ss.ten) from
  (select ten from tenk1 a
   where unique1 IN (select distinct hundred from tenk1 b)) ss;

--
-- Test cases to check for overenthusiastic optimization of
-- "IN (SELECT DISTINCT ...)" and related cases.  Per example from
-- Luca Pireddu and Michael Fuhr.
--

CREATE TEMP TABLE foo (id integer);
CREATE TEMP TABLE bar (id1 integer, id2 integer);

INSERT INTO foo VALUES (1);

INSERT INTO bar VALUES (1, 1);
INSERT INTO bar VALUES (2, 2);
INSERT INTO bar VALUES (3, 1);

-- These cases require an extra level of distinct-ing above subquery s
SELECT * FROM foo WHERE id IN
    (SELECT id2 FROM (SELECT DISTINCT id1, id2 FROM bar) AS s);
SELECT * FROM foo WHERE id IN
    (SELECT id2 FROM (SELECT id1,id2 FROM bar GROUP BY id1,id2) AS s);
SELECT * FROM foo WHERE id IN
    (SELECT id2 FROM (SELECT id1, id2 FROM bar UNION
                      SELECT id1, id2 FROM bar) AS s);

-- These cases do not
SELECT * FROM foo WHERE id IN
    (SELECT id2 FROM (SELECT DISTINCT ON (id2) id1, id2 FROM bar) AS s);
SELECT * FROM foo WHERE id IN
    (SELECT id2 FROM (SELECT id2 FROM bar GROUP BY id2) AS s);
SELECT * FROM foo WHERE id IN
    (SELECT id2 FROM (SELECT id2 FROM bar UNION
                      SELECT id2 FROM bar) AS s);

--
-- Test case to catch problems with multiply nested sub-SELECTs not getting
-- recalculated properly.  Per bug report from Didier Moens.
--

CREATE TABLE orderstest (
    approver_ref integer,
    po_ref integer,
    ordercanceled boolean
);

INSERT INTO orderstest VALUES (1, 1, false);
INSERT INTO orderstest VALUES (66, 5, false);
INSERT INTO orderstest VALUES (66, 6, false);
INSERT INTO orderstest VALUES (66, 7, false);
INSERT INTO orderstest VALUES (66, 1, true);
INSERT INTO orderstest VALUES (66, 8, false);
INSERT INTO orderstest VALUES (66, 1, false);
INSERT INTO orderstest VALUES (77, 1, false);
INSERT INTO orderstest VALUES (1, 1, false);
INSERT INTO orderstest VALUES (66, 1, false);
INSERT INTO orderstest VALUES (1, 1, false);

CREATE VIEW orders_view AS
SELECT *,
(SELECT CASE
   WHEN ord.approver_ref=1 THEN '---' ELSE 'Approved'
 END) AS "Approved",
(SELECT CASE
 WHEN ord.ordercanceled
 THEN 'Canceled'
 ELSE
  (SELECT CASE
		WHEN ord.po_ref=1
		THEN
		 (SELECT CASE
				WHEN ord.approver_ref=1
				THEN '---'
				ELSE 'Approved'
			END)
		ELSE 'PO'
	END)
END) AS "Status",
(CASE
 WHEN ord.ordercanceled
 THEN 'Canceled'
 ELSE
  (CASE
		WHEN ord.po_ref=1
		THEN
		 (CASE
				WHEN ord.approver_ref=1
				THEN '---'
				ELSE 'Approved'
			END)
		ELSE 'PO'
	END)
END) AS "Status_OK"
FROM orderstest ord;

SELECT * FROM orders_view;

DROP TABLE orderstest cascade;

--
-- Test cases to catch situations where rule rewriter fails to propagate
-- hasSubLinks flag correctly.  Per example from Kyle Bateman.
--

create temp table parts (
    partnum     text,
    cost        float8
);

create temp table shipped (
    ttype       char(2),
    ordnum      int4,
    partnum     text,
    value       float8
);

create temp view shipped_view as
    select * from shipped where ttype = 'wt';

create rule shipped_view_insert as on insert to shipped_view do instead
    insert into shipped values('wt', new.ordnum, new.partnum, new.value);

insert into parts (partnum, cost) values (1, 1234.56);

insert into shipped_view (ordnum, partnum, value)
    values (0, 1, (select cost from parts where partnum = '1'));

select * from shipped_view;

create rule shipped_view_update as on update to shipped_view do instead
    update shipped set partnum = new.partnum, value = new.value
        where ttype = new.ttype and ordnum = new.ordnum;

update shipped_view set value = 11
    from int4_tbl a join int4_tbl b
      on (a.f1 = (select f1 from int4_tbl c where c.f1=b.f1))
    where ordnum = a.f1;

select * from shipped_view;

select f1, ss1 as relabel from
    (select *, (select sum(f1) from int4_tbl b where f1 >= a.f1) as ss1
     from int4_tbl a) ss;

--
-- Test cases involving PARAM_EXEC parameters and min/max index optimizations.
-- Per bug report from David Sanchez i Gregori.
--

select * from (
  select max(unique1) from tenk1 as a
  where exists (select 1 from tenk1 as b where b.thousand = a.unique2)
) ss;

select * from (
  select min(unique1) from tenk1 as a
  where not exists (select 1 from tenk1 as b where b.unique2 = 10000)
) ss;


--
-- Test cases for interactions between PARAM_EXEC, subplans and array
-- subscripts
--

-- check that array subscription doesn't conflict with PARAM_EXEC (see #19370)
SELECT (array[1,2])[(SELECT g.i)] FROM generate_series(1, 1) g(i);
SELECT (array[1,2])[(SELECT g.i):(SELECT g.i + 1)] FROM generate_series(1, 1) g(i);


--
-- Test that an IN implemented using a UniquePath does unique-ification
-- with the right semantics, as per bug #4113.  (Unfortunately we have
-- no simple way to ensure that this test case actually chooses that type
-- of plan, but it does in releases 7.4-8.3.  Note that an ordering difference
-- here might mean that some other plan type is being used, rendering the test
-- pointless.)
--

create temp table numeric_table (num_col numeric);
insert into numeric_table values (1), (1.000000000000000000001), (2), (3);

create temp table float_table (float_col float8);
insert into float_table values (1), (2), (3);

select * from float_table
  where float_col in (select num_col from numeric_table);

select * from numeric_table
  where num_col in (select float_col from float_table);

--
-- Test case for bug #4290: bogus calculation of subplan param sets
--

create temp table ta (id int primary key, val int);

insert into ta values(1,1);
insert into ta values(2,2);

create temp table tb (id int primary key, aval int);

insert into tb values(1,1);
insert into tb values(2,1);
insert into tb values(3,2);
insert into tb values(4,2);

create temp table tc (id int primary key, aid int);

insert into tc values(1,1);
insert into tc values(2,2);

select
  ( select min(tb.id) from tb
    where tb.aval = (select ta.val from ta where ta.id = tc.aid) ) as min_tb_id
from tc;

--
-- Test case for 8.3 "failed to locate grouping columns" bug
--

create temp table t1 (f1 numeric(14,0), f2 varchar(30));

select * from
  (select distinct f1, f2, (select f2 from t1 x where x.f1 = up.f1) as fs
   from t1 up) ss
group by f1,f2,fs;

--
-- Test case for bug #5514 (mishandling of whole-row Vars in subselects)
--

create temp table table_a(id integer);
insert into table_a values (42);

create temp view view_a as select * from table_a;

select view_a from view_a;
select (select view_a) from view_a;
select (select (select view_a)) from view_a;
select (select (a.*)::text) from view_a a;

--
-- Test case for bug #19037: no relation entry for relid N
--

explain (costs off)
select (1 = any(array_agg(f1))) = any (select false) from int4_tbl;

select (1 = any(array_agg(f1))) = any (select false) from int4_tbl;

--
-- Check that whole-row Vars reading the result of a subselect don't include
-- any junk columns therein
--

select q from (select max(f1) from int4_tbl group by f1 order by f1) q;
with q as (select max(f1) from int4_tbl group by f1 order by f1)
  select q from q;

--
-- Test case for sublinks pulled up into joinaliasvars lists in an
-- inherited update/delete query
--

begin;  --  this shouldn't delete anything, but be safe

delete from road
where exists (
  select 1
  from
    int4_tbl cross join
    ( select f1, array(select q1 from int8_tbl) as arr
      from text_tbl ) ss
  where road.name = ss.f1 );

rollback;

--
-- Test case for sublinks pushed down into subselects via join alias expansion
--

select
  (select sq1) as qq1
from
  (select exists(select 1 from int4_tbl where f1 = q2) as sq1, 42 as dummy
   from int8_tbl) sq0
  join
  int4_tbl i4 on dummy = i4.f1;

--
-- Test case for subselect within UPDATE of INSERT...ON CONFLICT DO UPDATE
--
create temp table upsert(key int4 primary key, val text);
insert into upsert values(1, 'val') on conflict (key) do update set val = 'not seen';
insert into upsert values(1, 'val') on conflict (key) do update set val = 'seen with subselect ' || (select f1 from int4_tbl where f1 != 0 limit 1)::text;

select * from upsert;

with aa as (select 'int4_tbl' u from int4_tbl limit 1)
insert into upsert values (1, 'x'), (999, 'y')
on conflict (key) do update set val = (select u from aa)
returning *;

--
-- Test case for cross-type partial matching in hashed subplan (bug #7597)
--

create temp table outer_7597 (f1 int4, f2 int4);
insert into outer_7597 values (0, 0);
insert into outer_7597 values (1, 0);
insert into outer_7597 values (0, null);
insert into outer_7597 values (1, null);

create temp table inner_7597(c1 int8, c2 int8);
insert into inner_7597 values(0, null);

select * from outer_7597 where (f1, f2) not in (select * from inner_7597);

--
-- Similar test case using text that verifies that collation
-- information is passed through by execTuplesEqual() in nodeSubplan.c
-- (otherwise it would error in texteq())
--

create temp table outer_text (f1 text, f2 text);
insert into outer_text values ('a', 'a');
insert into outer_text values ('b', 'a');
insert into outer_text values ('a', null);
insert into outer_text values ('b', null);

create temp table inner_text (c1 text, c2 text);
insert into inner_text values ('a', null);
insert into inner_text values ('123', '456');

select * from outer_text where (f1, f2) not in (select * from inner_text);

--
-- Another test case for cross-type hashed subplans: comparison of
-- inner-side values must be done with appropriate operator
--

explain (verbose, costs off)
select 'foo'::text in (select 'bar'::name union all select 'bar'::name);

select 'foo'::text in (select 'bar'::name union all select 'bar'::name);

--
-- Test that we don't try to hash nested records (bug #17363)
-- (Hashing could be supported, but for now we don't)
--

explain (verbose, costs off)
select row(row(row(1))) = any (select row(row(1)));

select row(row(row(1))) = any (select row(row(1)));

--
-- Test case for premature memory release during hashing of subplan output
--

select '1'::text in (select '1'::name union all select '1'::name);

--
-- Test that we don't try to use a hashed subplan if the simplified
-- testexpr isn't of the right shape
--

-- this fails by default, of course
select * from int8_tbl where q1 in (select c1 from inner_text);

begin;

-- make an operator to allow it to succeed
create function bogus_int8_text_eq(int8, text) returns boolean
language sql as 'select $1::text = $2';

create operator = (procedure=bogus_int8_text_eq, leftarg=int8, rightarg=text);

explain (costs off)
select * from int8_tbl where q1 in (select c1 from inner_text);
select * from int8_tbl where q1 in (select c1 from inner_text);

-- inlining of this function results in unusual number of hash clauses,
-- which we can still cope with
create or replace function bogus_int8_text_eq(int8, text) returns boolean
language sql as 'select $1::text = $2 and $1::text = $2';

explain (costs off)
select * from int8_tbl where q1 in (select c1 from inner_text);
select * from int8_tbl where q1 in (select c1 from inner_text);

-- inlining of this function causes LHS and RHS to be switched,
-- which we can't cope with, so hashing should be abandoned
create or replace function bogus_int8_text_eq(int8, text) returns boolean
language sql as 'select $2 = $1::text';

explain (costs off)
select * from int8_tbl where q1 in (select c1 from inner_text);
select * from int8_tbl where q1 in (select c1 from inner_text);

rollback;  -- to get rid of the bogus operator

--
-- Test resolution of hashed vs non-hashed implementation of EXISTS subplan
--
explain (costs off)
select count(*) from tenk1 t
where (exists(select 1 from tenk1 k where k.unique1 = t.unique2) or ten < 0);
select count(*) from tenk1 t
where (exists(select 1 from tenk1 k where k.unique1 = t.unique2) or ten < 0);

explain (costs off)
select count(*) from tenk1 t
where (exists(select 1 from tenk1 k where k.unique1 = t.unique2) or ten < 0)
  and thousand = 1;
select count(*) from tenk1 t
where (exists(select 1 from tenk1 k where k.unique1 = t.unique2) or ten < 0)
  and thousand = 1;

-- It's possible for the same EXISTS to get resolved both ways
create temp table exists_tbl (c1 int, c2 int, c3 int) partition by list (c1);
create temp table exists_tbl_null partition of exists_tbl for values in (null);
create temp table exists_tbl_def partition of exists_tbl default;
insert into exists_tbl select x, x/2, x+1 from generate_series(0,10) x;
analyze exists_tbl;
explain (costs off)
select * from exists_tbl t1
  where (exists(select 1 from exists_tbl t2 where t1.c1 = t2.c2) or c3 < 0);
select * from exists_tbl t1
  where (exists(select 1 from exists_tbl t2 where t1.c1 = t2.c2) or c3 < 0);

--
-- Test case for planner bug with nested EXISTS handling
--
select a.thousand from tenk1 a, tenk1 b
where a.thousand = b.thousand
  and exists ( select 1 from tenk1 c where b.hundred = c.hundred
                   and not exists ( select 1 from tenk1 d
                                    where a.thousand = d.thousand ) );

--
-- Check that nested sub-selects are not pulled up if they contain volatiles
--
explain (verbose, costs off)
  select x, x from
    (select (select now()) as x from (values(1),(2)) v(y)) ss;
explain (verbose, costs off)
  select x, x from
    (select (select random()) as x from (values(1),(2)) v(y)) ss;
explain (verbose, costs off)
  select x, x from
    (select (select now() where y=y) as x from (values(1),(2)) v(y)) ss;
explain (verbose, costs off)
  select x, x from
    (select (select random() where y=y) as x from (values(1),(2)) v(y)) ss;

--
-- Test rescan of a hashed subplan (the use of random() is to prevent the
-- sub-select from being pulled up, which would result in not hashing)
--
explain (verbose, costs off)
select sum(ss.tst::int) from
  onek o cross join lateral (
  select i.ten in (select f1 from int4_tbl where f1 <= o.hundred) as tst,
         random() as r
  from onek i where i.unique1 = o.unique1 ) ss
where o.ten = 0;

select sum(ss.tst::int) from
  onek o cross join lateral (
  select i.ten in (select f1 from int4_tbl where f1 <= o.hundred) as tst,
         random() as r
  from onek i where i.unique1 = o.unique1 ) ss
where o.ten = 0;

--
-- Test rescan of a hashed SetOp node
--
begin;
set local enable_sort = off;

explain (costs off)
select count(*) from
  onek o cross join lateral (
    select * from onek i1 where i1.unique1 = o.unique1
    except
    select * from onek i2 where i2.unique1 = o.unique2
  ) ss
where o.ten = 1;

select count(*) from
  onek o cross join lateral (
    select * from onek i1 where i1.unique1 = o.unique1
    except
    select * from onek i2 where i2.unique1 = o.unique2
  ) ss
where o.ten = 1;

rollback;

--
-- Test rescan of a sorted SetOp node
--
begin;
set local enable_hashagg = off;

explain (costs off)
select count(*) from
  onek o cross join lateral (
    select * from onek i1 where i1.unique1 = o.unique1
    except
    select * from onek i2 where i2.unique1 = o.unique2
  ) ss
where o.ten = 1;

select count(*) from
  onek o cross join lateral (
    select * from onek i1 where i1.unique1 = o.unique1
    except
    select * from onek i2 where i2.unique1 = o.unique2
  ) ss
where o.ten = 1;

rollback;

--
-- Test rescan of a RecursiveUnion node
--
explain (costs off)
select sum(o.four), sum(ss.a) from
  onek o cross join lateral (
    with recursive x(a) as
      (select o.four as a
       union
       select a + 1 from x
       where a < 10)
    select * from x
  ) ss
where o.ten = 1;

select sum(o.four), sum(ss.a) from
  onek o cross join lateral (
    with recursive x(a) as
      (select o.four as a
       union
       select a + 1 from x
       where a < 10)
    select * from x
  ) ss
where o.ten = 1;

--
-- Check we don't misoptimize a NOT IN where the subquery returns no rows.
--
create temp table notinouter (a int);
create temp table notininner (b int not null);
insert into notinouter values (null), (1);

select * from notinouter where a not in (select b from notininner);

--
-- Check we behave sanely in corner case of empty SELECT list (bug #8648)
--
create temp table nocolumns();
select exists(select * from nocolumns);

--
-- Check behavior with a SubPlan in VALUES (bug #14924)
--
select val.x
  from generate_series(1,10) as s(i),
  lateral (
    values ((select s.i + 1)), (s.i + 101)
  ) as val(x)
where s.i < 10 and (select val.x) < 110;

-- another variant of that (bug #16213)
explain (verbose, costs off)
select * from
(values
  (3 not in (select * from (values (1), (2)) ss1)),
  (false)
) ss;

select * from
(values
  (3 not in (select * from (values (1), (2)) ss1)),
  (false)
) ss;

--
-- Check sane behavior with nested IN SubLinks
--
explain (verbose, costs off)
select * from int4_tbl where
  (case when f1 in (select unique1 from tenk1 a) then f1 else null end) in
  (select ten from tenk1 b);
select * from int4_tbl where
  (case when f1 in (select unique1 from tenk1 a) then f1 else null end) in
  (select ten from tenk1 b);

--
-- Check for incorrect optimization when IN subquery contains a SRF
--
explain (verbose, costs off)
select * from int4_tbl o where (f1, f1) in
  (select f1, generate_series(1,50) / 10 g from int4_tbl i group by f1);
select * from int4_tbl o where (f1, f1) in
  (select f1, generate_series(1,50) / 10 g from int4_tbl i group by f1);

--
-- check for over-optimization of whole-row Var referencing an Append plan
--
select (select q from
         (select 1,2,3 where f1 > 0
          union all
          select 4,5,6.0 where f1 <= 0
         ) q )
from int4_tbl;

--
-- Check for sane handling of a lateral reference in a subquery's quals
-- (most of the complication here is to prevent the test case from being
-- flattened too much)
--
explain (verbose, costs off)
select * from
    int4_tbl i4,
    lateral (
        select i4.f1 > 1 as b, 1 as id
        from (select random() order by 1) as t1
      union all
        select true as b, 2 as id
    ) as t2
where b and f1 >= 0;

select * from
    int4_tbl i4,
    lateral (
        select i4.f1 > 1 as b, 1 as id
        from (select random() order by 1) as t1
      union all
        select true as b, 2 as id
    ) as t2
where b and f1 >= 0;

--
-- Check that volatile quals aren't pushed down past a DISTINCT:
-- nextval() should not be called more than the nominal number of times
--
create temp sequence ts1;

select * from
  (select distinct ten from tenk1) ss
  where ten < 10 + nextval('ts1')
  order by 1;

select nextval('ts1');

--
-- Check that volatile quals aren't pushed down past a set-returning function;
-- while a nonvolatile qual can be, if it doesn't reference the SRF.
--
create function tattle(x int, y int) returns bool
volatile language plpgsql as $$
begin
  raise notice 'x = %, y = %', x, y;
  return x > y;
end$$;

explain (verbose, costs off)
select * from
  (select 9 as x, unnest(array[1,2,3,11,12,13]) as u) ss
  where tattle(x, 8);

select * from
  (select 9 as x, unnest(array[1,2,3,11,12,13]) as u) ss
  where tattle(x, 8);

-- if we pretend it's stable, we get different results:
alter function tattle(x int, y int) stable;

explain (verbose, costs off)
select * from
  (select 9 as x, unnest(array[1,2,3,11,12,13]) as u) ss
  where tattle(x, 8);

select * from
  (select 9 as x, unnest(array[1,2,3,11,12,13]) as u) ss
  where tattle(x, 8);

-- although even a stable qual should not be pushed down if it references SRF
explain (verbose, costs off)
select * from
  (select 9 as x, unnest(array[1,2,3,11,12,13]) as u) ss
  where tattle(x, u);

select * from
  (select 9 as x, unnest(array[1,2,3,11,12,13]) as u) ss
  where tattle(x, u);

--
-- check that an upper-level qual is not pushed down if it references a grouped
-- Var whose underlying expression contains SRFs
--
explain (verbose, costs off)
select * from
  (select generate_series(1, ten) as g, count(*) from tenk1 group by 1) ss
  where ss.g = 1;

select * from
  (select generate_series(1, ten) as g, count(*) from tenk1 group by 1) ss
  where ss.g = 1;

--
-- check that an upper-level qual is not pushed down if it references a grouped
-- Var whose underlying expression contains volatile functions
--
alter function tattle(x int, y int) volatile;

explain (verbose, costs off)
select * from
  (select tattle(3, ten) as v, count(*) from tenk1 where unique1 < 3 group by 1) ss
  where ss.v;

select * from
  (select tattle(3, ten) as v, count(*) from tenk1 where unique1 < 3 group by 1) ss
  where ss.v;

-- if we pretend it's stable, we get different results:
alter function tattle(x int, y int) stable;

explain (verbose, costs off)
select * from
  (select tattle(3, ten) as v, count(*) from tenk1 where unique1 < 3 group by 1) ss
  where ss.v;

select * from
  (select tattle(3, ten) as v, count(*) from tenk1 where unique1 < 3 group by 1) ss
  where ss.v;

drop function tattle(x int, y int);

--
-- Test that LIMIT can be pushed to SORT through a subquery that just projects
-- columns.  We check for that having happened by looking to see if EXPLAIN
-- ANALYZE shows that a top-N sort was used.  We must suppress or filter away
-- all the non-invariant parts of the EXPLAIN ANALYZE output.
--
create table sq_limit (pk int primary key, c1 int, c2 int);
insert into sq_limit values
    (1, 1, 1),
    (2, 2, 2),
    (3, 3, 3),
    (4, 4, 4),
    (5, 1, 1),
    (6, 2, 2),
    (7, 3, 3),
    (8, 4, 4);

create function explain_sq_limit() returns setof text language plpgsql as
$$
declare ln text;
begin
    for ln in
        explain (analyze, summary off, timing off, costs off, buffers off)
        select * from (select pk,c2 from sq_limit order by c1,pk) as x limit 3
    loop
        ln := regexp_replace(ln, 'Memory: \S*',  'Memory: xxx');
        return next ln;
    end loop;
end;
$$;

select * from explain_sq_limit();

select * from (select pk,c2 from sq_limit order by c1,pk) as x limit 3;

drop function explain_sq_limit();

drop table sq_limit;

--
-- Ensure that backward scan direction isn't propagated into
-- expression subqueries (bug #15336)
--

begin;

declare c1 scroll cursor for
 select * from generate_series(1,4) i
  where i <> all (values (2),(3));

move forward all in c1;
fetch backward all in c1;

commit;

--
-- Check that JsonConstructorExpr is treated as non-strict, and thus can be
-- wrapped in a PlaceHolderVar
--

begin;

create temp table json_tab (a int);
insert into json_tab values (1);

explain (verbose, costs off)
select * from json_tab t1 left join (select json_array(1, a) from json_tab t2) s on false;

select * from json_tab t1 left join (select json_array(1, a) from json_tab t2) s on false;

rollback;

--
-- Verify that we correctly flatten cases involving a subquery output
-- expression that doesn't need to be wrapped in a PlaceHolderVar
--

explain (costs off)
select tname, attname from (
select relname::information_schema.sql_identifier as tname, * from
  (select * from pg_class c) ss1) ss2
  right join pg_attribute a on a.attrelid = ss2.oid
where tname = 'tenk1' and attnum = 1;

select tname, attname from (
select relname::information_schema.sql_identifier as tname, * from
  (select * from pg_class c) ss1) ss2
  right join pg_attribute a on a.attrelid = ss2.oid
where tname = 'tenk1' and attnum = 1;

-- Check behavior when there's a lateral reference in the output expression
explain (verbose, costs off)
select t1.ten, sum(x) from
  tenk1 t1 left join lateral (
    select t1.ten + t2.ten as x, t2.fivethous from tenk1 t2
  ) ss on t1.unique1 = ss.fivethous
group by t1.ten
order by t1.ten;

select t1.ten, sum(x) from
  tenk1 t1 left join lateral (
    select t1.ten + t2.ten as x, t2.fivethous from tenk1 t2
  ) ss on t1.unique1 = ss.fivethous
group by t1.ten
order by t1.ten;

explain (verbose, costs off)
select t1.q1, x from
  int8_tbl t1 left join
  (int8_tbl t2 left join
   lateral (select t2.q1+t3.q1 as x, * from int8_tbl t3) t3 on t2.q2 = t3.q2)
  on t1.q2 = t2.q2
order by 1, 2;

select t1.q1, x from
  int8_tbl t1 left join
  (int8_tbl t2 left join
   lateral (select t2.q1+t3.q1 as x, * from int8_tbl t3) t3 on t2.q2 = t3.q2)
  on t1.q2 = t2.q2
order by 1, 2;

-- strict expressions containing variables of rels under the same lowest
-- nulling outer join can escape being wrapped
explain (verbose, costs off)
select t1.q1, x from
  int8_tbl t1 left join
  (int8_tbl t2 inner join
   lateral (select t2.q1+1 as x, * from int8_tbl t3) t3 on t2.q2 = t3.q2)
  on t1.q2 = t2.q2
order by 1, 2;

select t1.q1, x from
  int8_tbl t1 left join
  (int8_tbl t2 inner join
   lateral (select t2.q1+1 as x, * from int8_tbl t3) t3 on t2.q2 = t3.q2)
  on t1.q2 = t2.q2
order by 1, 2;

-- otherwise we need to wrap the strict expressions
explain (verbose, costs off)
select t1.q1, x from
  int8_tbl t1 left join
  (int8_tbl t2 left join
   lateral (select t2.q1+1 as x, * from int8_tbl t3) t3 on t2.q2 = t3.q2)
  on t1.q2 = t2.q2
order by 1, 2;

select t1.q1, x from
  int8_tbl t1 left join
  (int8_tbl t2 left join
   lateral (select t2.q1+1 as x, * from int8_tbl t3) t3 on t2.q2 = t3.q2)
  on t1.q2 = t2.q2
order by 1, 2;

-- lateral references for simple Vars can escape being wrapped if the
-- referenced rel is under the same lowest nulling outer join
explain (verbose, costs off)
select t1.q1, x from
  int8_tbl t1 left join
  (int8_tbl t2 inner join
   lateral (select t2.q2 as x, * from int8_tbl t3) ss on t2.q2 = ss.q1)
  on t1.q1 = t2.q1
order by 1, 2;

select t1.q1, x from
  int8_tbl t1 left join
  (int8_tbl t2 inner join
   lateral (select t2.q2 as x, * from int8_tbl t3) ss on t2.q2 = ss.q1)
  on t1.q1 = t2.q1
order by 1, 2;

-- otherwise we need to wrap the Vars
explain (verbose, costs off)
select t1.q1, x from
  int8_tbl t1 left join
  (int8_tbl t2 left join
   lateral (select t2.q2 as x, * from int8_tbl t3) ss on t2.q2 = ss.q1)
  on t1.q1 = t2.q1
order by 1, 2;

select t1.q1, x from
  int8_tbl t1 left join
  (int8_tbl t2 left join
   lateral (select t2.q2 as x, * from int8_tbl t3) ss on t2.q2 = ss.q1)
  on t1.q1 = t2.q1
order by 1, 2;

-- lateral references for PHVs can also escape being wrapped if the
-- referenced rel is under the same lowest nulling outer join
explain (verbose, costs off)
select ss2.* from
  int8_tbl t1 left join
  (int8_tbl t2 left join
   (select coalesce(q1) as x, * from int8_tbl t3) ss1 on t2.q1 = ss1.q2 inner join
   lateral (select ss1.x as y, * from int8_tbl t4) ss2 on t2.q2 = ss2.q1)
  on t1.q2 = ss2.q1
order by 1, 2, 3;

select ss2.* from
  int8_tbl t1 left join
  (int8_tbl t2 left join
   (select coalesce(q1) as x, * from int8_tbl t3) ss1 on t2.q1 = ss1.q2 inner join
   lateral (select ss1.x as y, * from int8_tbl t4) ss2 on t2.q2 = ss2.q1)
  on t1.q2 = ss2.q1
order by 1, 2, 3;

-- otherwise we need to wrap the PHVs
explain (verbose, costs off)
select ss2.* from
  int8_tbl t1 left join
  (int8_tbl t2 left join
   (select coalesce(q1) as x, * from int8_tbl t3) ss1 on t2.q1 = ss1.q2 left join
   lateral (select ss1.x as y, * from int8_tbl t4) ss2 on t2.q2 = ss2.q1)
  on t1.q2 = ss2.q1
order by 1, 2, 3;

select ss2.* from
  int8_tbl t1 left join
  (int8_tbl t2 left join
   (select coalesce(q1) as x, * from int8_tbl t3) ss1 on t2.q1 = ss1.q2 left join
   lateral (select ss1.x as y, * from int8_tbl t4) ss2 on t2.q2 = ss2.q1)
  on t1.q2 = ss2.q1
order by 1, 2, 3;

--
-- Tests for CTE inlining behavior
--

-- Basic subquery that can be inlined
explain (verbose, costs off)
with x as (select * from (select f1 from subselect_tbl) ss)
select * from x where f1 = 1;

-- Explicitly request materialization
explain (verbose, costs off)
with x as materialized (select * from (select f1 from subselect_tbl) ss)
select * from x where f1 = 1;

-- Stable functions are safe to inline
explain (verbose, costs off)
with x as (select * from (select f1, now() from subselect_tbl) ss)
select * from x where f1 = 1;

-- Volatile functions prevent inlining
explain (verbose, costs off)
with x as (select * from (select f1, random() from subselect_tbl) ss)
select * from x where f1 = 1;

-- SELECT FOR UPDATE cannot be inlined
explain (verbose, costs off)
with x as (select * from (select f1 from subselect_tbl for update) ss)
select * from x where f1 = 1;

-- Multiply-referenced CTEs are inlined only when requested
explain (verbose, costs off)
with x as (select * from (select f1, now() as n from subselect_tbl) ss)
select * from x, x x2 where x.n = x2.n;

explain (verbose, costs off)
with x as not materialized (select * from (select f1, now() as n from subselect_tbl) ss)
select * from x, x x2 where x.n = x2.n;

-- Multiply-referenced CTEs can't be inlined if they contain outer self-refs
explain (verbose, costs off)
with recursive x(a) as
  ((values ('a'), ('b'))
   union all
   (with z as not materialized (select * from x)
    select z.a || z1.a as a from z cross join z as z1
    where length(z.a || z1.a) < 5))
select * from x;

with recursive x(a) as
  ((values ('a'), ('b'))
   union all
   (with z as not materialized (select * from x)
    select z.a || z1.a as a from z cross join z as z1
    where length(z.a || z1.a) < 5))
select * from x;

explain (verbose, costs off)
with recursive x(a) as
  ((values ('a'), ('b'))
   union all
   (with z as not materialized (select * from x)
    select z.a || z.a as a from z
    where length(z.a || z.a) < 5))
select * from x;

with recursive x(a) as
  ((values ('a'), ('b'))
   union all
   (with z as not materialized (select * from x)
    select z.a || z.a as a from z
    where length(z.a || z.a) < 5))
select * from x;

-- Check handling of outer references
explain (verbose, costs off)
with x as (select * from int4_tbl)
select * from (with y as (select * from x) select * from y) ss;

explain (verbose, costs off)
with x as materialized (select * from int4_tbl)
select * from (with y as (select * from x) select * from y) ss;

-- Ensure that we inline the correct CTE when there are
-- multiple CTEs with the same name
explain (verbose, costs off)
with x as (select 1 as y)
select * from (with x as (select 2 as y) select * from x) ss;

-- Row marks are not pushed into CTEs
explain (verbose, costs off)
with x as (select * from subselect_tbl)
select * from x for update;

-- Pull up direct-correlated ANY_SUBLINKs
explain (costs off)
select * from tenk1 A where hundred in (select hundred from tenk2 B where B.odd = A.odd);

explain (costs off)
select * from tenk1 A where exists
(select 1 from tenk2 B
where A.hundred in (select C.hundred FROM tenk2 C
WHERE c.odd = b.odd));

-- we should only try to pull up the sublink into RHS of a left join
-- but a.hundred is not available.
explain (costs off)
SELECT * FROM tenk1 A LEFT JOIN tenk2 B
ON A.hundred in (SELECT c.hundred FROM tenk2 C WHERE c.odd = b.odd);

-- we should only try to pull up the sublink into RHS of a left join
-- but a.odd is not available for this.
explain (costs off)
SELECT * FROM tenk1 A LEFT JOIN tenk2 B
ON B.hundred in (SELECT c.hundred FROM tenk2 C WHERE c.odd = a.odd);

-- should be able to pull up since all the references are available.
explain (costs off)
SELECT * FROM tenk1 A LEFT JOIN tenk2 B
ON B.hundred in (SELECT c.hundred FROM tenk2 C WHERE c.odd = b.odd);

-- we can pull up the sublink into the inner JoinExpr.
explain (costs off)
SELECT * FROM tenk1 A INNER JOIN tenk2 B
ON A.hundred in (SELECT c.hundred FROM tenk2 C WHERE c.odd = b.odd)
WHERE a.thousand < 750;

-- we can pull up the aggregate sublink into RHS of a left join.
explain (costs off)
SELECT * FROM tenk1 A LEFT JOIN tenk2 B
ON B.hundred in (SELECT min(c.hundred) FROM tenk2 C WHERE c.odd = b.odd);

--
-- Test VALUES to ARRAY (VtA) transformation
--

-- VtA transformation for joined VALUES is not supported
EXPLAIN (COSTS OFF)
SELECT * FROM onek, (VALUES('RFAAAA'), ('VJAAAA')) AS v (i)
  WHERE onek.stringu1 = v.i;

-- VtA transformation for a composite argument is not supported
EXPLAIN (COSTS OFF)
SELECT * FROM onek
  WHERE (unique1,ten) IN (VALUES (1,1), (20,0), (99,9), (17,99))
  ORDER BY unique1;

EXPLAIN (COSTS OFF)
SELECT * FROM onek
    WHERE unique1 IN (VALUES(10000), (2), (389), (1000), (2000), (10029))
    ORDER BY unique1;

EXPLAIN (COSTS OFF)
SELECT * FROM onek
    WHERE unique1 IN (VALUES(1200), (1));

-- Recursive evaluation of constant queries is not yet supported
EXPLAIN (COSTS OFF)
SELECT * FROM onek
  WHERE unique1 IN (SELECT x * x FROM (VALUES(1200), (1)) AS x(x));

EXPLAIN (COSTS OFF)
SELECT unique1, stringu1 FROM onek WHERE stringu1::name IN (VALUES('RFAAAA'), ('VJAAAA'));

EXPLAIN (COSTS OFF)
SELECT unique1, stringu1 FROM onek WHERE stringu1::text IN (VALUES('RFAAAA'), ('VJAAAA'));

EXPLAIN (COSTS OFF)
SELECT * from onek WHERE unique1 in (VALUES(1200::bigint), (1));

-- VtA shouldn't depend on the side of the join probing with the VALUES expression.
EXPLAIN (COSTS OFF)
SELECT c.unique1,c.ten FROM tenk1 c JOIN onek a USING (ten)
WHERE a.ten IN (VALUES (1), (2));
EXPLAIN (COSTS OFF)
SELECT c.unique1,c.ten FROM tenk1 c JOIN onek a USING (ten)
WHERE c.ten IN (VALUES (1), (2));

-- Constant expressions are simplified
EXPLAIN (COSTS OFF)
SELECT ten FROM onek WHERE sin(two ) +four IN (VALUES (sin(0.5)), (2));
EXPLAIN (COSTS OFF)

-- VtA allows NULLs in the list
SELECT ten FROM onek WHERE sin(two)+four IN (VALUES (sin(0.5)), (NULL), (2));

-- VtA is supported for custom plans where params are substituted with
-- constants.  VtA is not supported with generic plans where params prevent
-- us from building a constant array.
PREPARE test (int, numeric, text) AS
  SELECT ten FROM onek WHERE sin(two) * four / ($3::real) IN (VALUES (sin($2)), (2), ($1));
EXPLAIN (COSTS OFF) EXECUTE test(42, 3.14, '-1.5');
EXPLAIN (COSTS OFF) EXECUTE test(NULL, 3.14, NULL);
EXPLAIN (COSTS OFF) EXECUTE test(NULL, 3.14, '-1.5');
SET plan_cache_mode = 'force_generic_plan';
EXPLAIN (COSTS OFF) EXECUTE test(NULL, 3.14, '-1.5');
RESET plan_cache_mode;

--  VtA doesn't support LIMIT, OFFSET, and ORDER BY clauses
EXPLAIN (COSTS OFF)
SELECT ten FROM onek WHERE unique1 IN (VALUES (1), (2) OFFSET 1);
EXPLAIN (COSTS OFF)
SELECT ten FROM onek WHERE unique1 IN (VALUES (1), (2) ORDER BY 1);
EXPLAIN (COSTS OFF)
SELECT ten FROM onek WHERE unique1 IN (VALUES (1), (2) LIMIT 1);

EXPLAIN (COSTS OFF)
SELECT ten FROM onek t
WHERE unique1 IN (VALUES (0), ((2 IN (SELECT unique2 FROM onek c
  WHERE c.unique2 = t.unique1))::integer));

EXPLAIN (COSTS OFF)
SELECT ten FROM onek t
WHERE unique1 IN (VALUES (0), ((2 IN (SELECT unique2 FROM onek c
  WHERE c.unique2 IN (VALUES (sin(0.5)), (2))))::integer));

-- VtA is not allowed with subqueries
EXPLAIN (COSTS OFF)
SELECT ten FROM onek t WHERE unique1 IN (VALUES (0), ((2 IN
  (SELECT (3)))::integer)
);

-- VtA is not allowed with non-constant expressions
EXPLAIN (COSTS OFF)
SELECT ten FROM onek t WHERE unique1 IN (VALUES (0), (unique2));
EXPLAIN (COSTS OFF)
SELECT * FROM onek t1, lateral (SELECT * FROM onek t2 WHERE t2.ten IN (values (t1.ten), (1)));

-- VtA causes the whole expression to be evaluated as a constant
EXPLAIN (COSTS OFF)
SELECT ten FROM onek t WHERE 1.0::integer IN ((VALUES (1), (3)));
