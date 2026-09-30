--
-- JOIN
-- Test JOIN clauses
--

CREATE TABLE J1_TBL (
  i integer,
  j integer,
  t text
);

CREATE TABLE J2_TBL (
  i integer,
  k integer
);


INSERT INTO J1_TBL VALUES (1, 4, 'one');
INSERT INTO J1_TBL VALUES (2, 3, 'two');
INSERT INTO J1_TBL VALUES (3, 2, 'three');
INSERT INTO J1_TBL VALUES (4, 1, 'four');
INSERT INTO J1_TBL VALUES (5, 0, 'five');
INSERT INTO J1_TBL VALUES (6, 6, 'six');
INSERT INTO J1_TBL VALUES (7, 7, 'seven');
INSERT INTO J1_TBL VALUES (8, 8, 'eight');
INSERT INTO J1_TBL VALUES (0, NULL, 'zero');
INSERT INTO J1_TBL VALUES (NULL, NULL, 'null');
INSERT INTO J1_TBL VALUES (NULL, 0, 'zero');

INSERT INTO J2_TBL VALUES (1, -1);
INSERT INTO J2_TBL VALUES (2, 2);
INSERT INTO J2_TBL VALUES (3, -3);
INSERT INTO J2_TBL VALUES (2, 4);
INSERT INTO J2_TBL VALUES (5, -5);
INSERT INTO J2_TBL VALUES (5, -5);
INSERT INTO J2_TBL VALUES (0, NULL);
INSERT INTO J2_TBL VALUES (NULL, NULL);
INSERT INTO J2_TBL VALUES (NULL, 0);

-- useful in some tests below
create temp table onerow();
insert into onerow default values;
analyze onerow;


--
-- CORRELATION NAMES
-- Make sure that table/column aliases are supported
-- before diving into more complex join syntax.
--

SELECT *
  FROM J1_TBL AS tx;

SELECT *
  FROM J1_TBL tx;

SELECT *
  FROM J1_TBL AS t1 (a, b, c);

SELECT *
  FROM J1_TBL t1 (a, b, c);

SELECT *
  FROM J1_TBL t1 (a, b, c), J2_TBL t2 (d, e);

SELECT t1.a, t2.e
  FROM J1_TBL t1 (a, b, c), J2_TBL t2 (d, e)
  WHERE t1.a = t2.d;


--
-- CROSS JOIN
-- Qualifications are not allowed on cross joins,
-- which degenerate into a standard unqualified inner join.
--

SELECT *
  FROM J1_TBL CROSS JOIN J2_TBL;

-- ambiguous column
SELECT i, k, t
  FROM J1_TBL CROSS JOIN J2_TBL;

-- resolve previous ambiguity by specifying the table name
SELECT t1.i, k, t
  FROM J1_TBL t1 CROSS JOIN J2_TBL t2;

SELECT ii, tt, kk
  FROM (J1_TBL CROSS JOIN J2_TBL)
    AS tx (ii, jj, tt, ii2, kk);

SELECT tx.ii, tx.jj, tx.kk
  FROM (J1_TBL t1 (a, b, c) CROSS JOIN J2_TBL t2 (d, e))
    AS tx (ii, jj, tt, ii2, kk);

SELECT *
  FROM J1_TBL CROSS JOIN J2_TBL a CROSS JOIN J2_TBL b;


--
--
-- Inner joins (equi-joins)
--
--

--
-- Inner joins (equi-joins) with USING clause
-- The USING syntax changes the shape of the resulting table
-- by including a column in the USING clause only once in the result.
--

-- Inner equi-join on specified column
SELECT *
  FROM J1_TBL INNER JOIN J2_TBL USING (i);

-- Same as above, slightly different syntax
SELECT *
  FROM J1_TBL JOIN J2_TBL USING (i);

SELECT *
  FROM J1_TBL t1 (a, b, c) JOIN J2_TBL t2 (a, d) USING (a)
  ORDER BY a, d;

SELECT *
  FROM J1_TBL t1 (a, b, c) JOIN J2_TBL t2 (a, b) USING (b)
  ORDER BY b, t1.a;

-- test join using aliases
SELECT * FROM J1_TBL JOIN J2_TBL USING (i) WHERE J1_TBL.t = 'one';  -- ok
SELECT * FROM J1_TBL JOIN J2_TBL USING (i) AS x WHERE J1_TBL.t = 'one';  -- ok
SELECT * FROM (J1_TBL JOIN J2_TBL USING (i)) AS x WHERE J1_TBL.t = 'one';  -- error
SELECT * FROM J1_TBL JOIN J2_TBL USING (i) AS x WHERE x.i = 1;  -- ok
SELECT * FROM J1_TBL JOIN J2_TBL USING (i) AS x WHERE x.t = 'one';  -- error
SELECT * FROM (J1_TBL JOIN J2_TBL USING (i) AS x) AS xx WHERE x.i = 1;  -- error (XXX could use better hint)
SELECT * FROM J1_TBL a1 JOIN J2_TBL a2 USING (i) AS a1;  -- error
SELECT x.* FROM J1_TBL JOIN J2_TBL USING (i) AS x WHERE J1_TBL.t = 'one';
SELECT ROW(x.*) FROM J1_TBL JOIN J2_TBL USING (i) AS x WHERE J1_TBL.t = 'one';
SELECT row_to_json(x.*) FROM J1_TBL JOIN J2_TBL USING (i) AS x WHERE J1_TBL.t = 'one';

--
-- NATURAL JOIN
-- Inner equi-join on all columns with the same name
--

SELECT *
  FROM J1_TBL NATURAL JOIN J2_TBL;

SELECT *
  FROM J1_TBL t1 (a, b, c) NATURAL JOIN J2_TBL t2 (a, d);

SELECT *
  FROM J1_TBL t1 (a, b, c) NATURAL JOIN J2_TBL t2 (d, a);

-- mismatch number of columns
-- currently, Postgres will fill in with underlying names
SELECT *
  FROM J1_TBL t1 (a, b) NATURAL JOIN J2_TBL t2 (a);


--
-- Inner joins (equi-joins)
--

SELECT *
  FROM J1_TBL JOIN J2_TBL ON (J1_TBL.i = J2_TBL.i);

SELECT *
  FROM J1_TBL JOIN J2_TBL ON (J1_TBL.i = J2_TBL.k);


--
-- Non-equi-joins
--

SELECT *
  FROM J1_TBL JOIN J2_TBL ON (J1_TBL.i <= J2_TBL.k);


--
-- Outer joins
-- Note that OUTER is a noise word
--

SELECT *
  FROM J1_TBL LEFT OUTER JOIN J2_TBL USING (i)
  ORDER BY i, k, t;

SELECT *
  FROM J1_TBL LEFT JOIN J2_TBL USING (i)
  ORDER BY i, k, t;

SELECT *
  FROM J1_TBL RIGHT OUTER JOIN J2_TBL USING (i);

SELECT *
  FROM J1_TBL RIGHT JOIN J2_TBL USING (i);

SELECT *
  FROM J1_TBL FULL OUTER JOIN J2_TBL USING (i)
  ORDER BY i, k, t;

SELECT *
  FROM J1_TBL FULL JOIN J2_TBL USING (i)
  ORDER BY i, k, t;

SELECT *
  FROM J1_TBL LEFT JOIN J2_TBL USING (i) WHERE (k = 1);

SELECT *
  FROM J1_TBL LEFT JOIN J2_TBL USING (i) WHERE (i = 1);

--
-- semijoin selectivity for <>
--
explain (costs off)
select * from tenk1 a, tenk1 b
where exists(select * from tenk1 c
             where b.twothousand = c.twothousand and b.fivethous <> c.fivethous)
      and a.tenthous = b.tenthous and a.tenthous < 5000;


--
-- More complicated constructs
--

--
-- Multiway full join
--

CREATE TABLE t1 (name TEXT, n INTEGER);
CREATE TABLE t2 (name TEXT, n INTEGER);
CREATE TABLE t3 (name TEXT, n INTEGER);

INSERT INTO t1 VALUES ( 'bb', 11 );
INSERT INTO t2 VALUES ( 'bb', 12 );
INSERT INTO t2 VALUES ( 'cc', 22 );
INSERT INTO t2 VALUES ( 'ee', 42 );
INSERT INTO t3 VALUES ( 'bb', 13 );
INSERT INTO t3 VALUES ( 'cc', 23 );
INSERT INTO t3 VALUES ( 'dd', 33 );

SELECT * FROM t1 FULL JOIN t2 USING (name) FULL JOIN t3 USING (name);

--
-- Test interactions of join syntax and subqueries
--

-- Basic cases (we expect planner to pull up the subquery here)
SELECT * FROM
(SELECT * FROM t2) as s2
INNER JOIN
(SELECT * FROM t3) s3
USING (name);

SELECT * FROM
(SELECT * FROM t2) as s2
LEFT JOIN
(SELECT * FROM t3) s3
USING (name);

SELECT * FROM
(SELECT * FROM t2) as s2
FULL JOIN
(SELECT * FROM t3) s3
USING (name);

-- Cases with non-nullable expressions in subquery results;
-- make sure these go to null as expected
SELECT * FROM
(SELECT name, n as s2_n, 2 as s2_2 FROM t2) as s2
NATURAL INNER JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t3) s3;

SELECT * FROM
(SELECT name, n as s2_n, 2 as s2_2 FROM t2) as s2
NATURAL LEFT JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t3) s3;

SELECT * FROM
(SELECT name, n as s2_n, 2 as s2_2 FROM t2) as s2
NATURAL FULL JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t3) s3;

SELECT * FROM
(SELECT name, n as s1_n, 1 as s1_1 FROM t1) as s1
NATURAL INNER JOIN
(SELECT name, n as s2_n, 2 as s2_2 FROM t2) as s2
NATURAL INNER JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t3) s3;

SELECT * FROM
(SELECT name, n as s1_n, 1 as s1_1 FROM t1) as s1
NATURAL FULL JOIN
(SELECT name, n as s2_n, 2 as s2_2 FROM t2) as s2
NATURAL FULL JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t3) s3;

SELECT * FROM
(SELECT name, n as s1_n FROM t1) as s1
NATURAL FULL JOIN
  (SELECT * FROM
    (SELECT name, n as s2_n FROM t2) as s2
    NATURAL FULL JOIN
    (SELECT name, n as s3_n FROM t3) as s3
  ) ss2;

SELECT * FROM
(SELECT name, n as s1_n FROM t1) as s1
NATURAL FULL JOIN
  (SELECT * FROM
    (SELECT name, n as s2_n, 2 as s2_2 FROM t2) as s2
    NATURAL FULL JOIN
    (SELECT name, n as s3_n FROM t3) as s3
  ) ss2;

-- Constants as join keys can also be problematic
SELECT * FROM
  (SELECT name, n as s1_n FROM t1) as s1
FULL JOIN
  (SELECT name, 2 as s2_n FROM t2) as s2
ON (s1_n = s2_n);


-- Test for propagation of nullability constraints into sub-joins

create temp table x (x1 int, x2 int);
insert into x values (1,11);
insert into x values (2,22);
insert into x values (3,null);
insert into x values (4,44);
insert into x values (5,null);

create temp table y (y1 int, y2 int);
insert into y values (1,111);
insert into y values (2,222);
insert into y values (3,333);
insert into y values (4,null);

select * from x;
select * from y;

select * from x left join y on (x1 = y1 and x2 is not null);
select * from x left join y on (x1 = y1 and y2 is not null);

select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1);
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1 and x2 is not null);
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1 and y2 is not null);
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1 and xx2 is not null);
-- these should NOT give the same answers as above
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1) where (x2 is not null);
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1) where (y2 is not null);
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1) where (xx2 is not null);

--
-- regression test: check for bug with propagation of implied equality
-- to outside an IN
--
select count(*) from tenk1 a where unique1 in
  (select unique1 from tenk1 b join tenk1 c using (unique1)
   where b.unique2 = 42);

--
-- regression test: check for failure to generate a plan with multiple
-- degenerate IN clauses
--
select count(*) from tenk1 x where
  x.unique1 in (select a.f1 from int4_tbl a,float8_tbl b where a.f1=b.f1) and
  x.unique1 = 0 and
  x.unique1 in (select aa.f1 from int4_tbl aa,float8_tbl bb where aa.f1=bb.f1);

-- try that with GEQO too
begin;
set geqo = on;
set geqo_threshold = 2;
select count(*) from tenk1 x where
  x.unique1 in (select a.f1 from int4_tbl a,float8_tbl b where a.f1=b.f1) and
  x.unique1 = 0 and
  x.unique1 in (select aa.f1 from int4_tbl aa,float8_tbl bb where aa.f1=bb.f1);
rollback;

--
-- regression test: be sure we cope with proven-dummy append rels
--
explain (costs off)
select aa, bb, unique1, unique1
  from tenk1 right join b_star on aa = unique1
  where bb < bb and bb is null;

select aa, bb, unique1, unique1
  from tenk1 right join b_star on aa = unique1
  where bb < bb and bb is null;

--
-- regression test: check handling of empty-FROM subquery underneath outer join
--
explain (costs off)
select * from int8_tbl i1 left join (int8_tbl i2 join
  (select 123 as x) ss on i2.q1 = x) on i1.q2 = i2.q2
order by 1, 2;

select * from int8_tbl i1 left join (int8_tbl i2 join
  (select 123 as x) ss on i2.q1 = x) on i1.q2 = i2.q2
order by 1, 2;

--
-- regression test: check a case where join_clause_is_movable_into()
-- used to give an imprecise result, causing an assertion failure
--
select count(*)
from
  (select t3.tenthous as x1, coalesce(t1.stringu1, t2.stringu1) as x2
   from tenk1 t1
   left join tenk1 t2 on t1.unique1 = t2.unique1
   join tenk1 t3 on t1.unique2 = t3.unique2) ss,
  tenk1 t4,
  tenk1 t5
where t4.thousand = t5.unique1 and ss.x1 = t4.tenthous and ss.x2 = t5.stringu1;

--
-- regression test: check a case where we formerly missed including an EC
-- enforcement clause because it was expected to be handled at scan level
--
explain (costs off)
select a.f1, b.f1, t.thousand, t.tenthous from
  tenk1 t,
  (select sum(f1)+1 as f1 from int4_tbl i4a) a,
  (select sum(f1) as f1 from int4_tbl i4b) b
where b.f1 = t.thousand and a.f1 = b.f1 and (a.f1+b.f1+999) = t.tenthous;

select a.f1, b.f1, t.thousand, t.tenthous from
  tenk1 t,
  (select sum(f1)+1 as f1 from int4_tbl i4a) a,
  (select sum(f1) as f1 from int4_tbl i4b) b
where b.f1 = t.thousand and a.f1 = b.f1 and (a.f1+b.f1+999) = t.tenthous;

--
-- Test hash joins with multiple hash keys and subplans.
--

-- First ensure we get a hash join with multiple hash keys.
explain (costs off)
select t1.unique1,t2.unique1 from tenk1 t1
inner join tenk1 t2 on t1.two = t2.two
  and t1.unique1 = (select min(unique1) from tenk1
                    where t2.unique1=unique1)
where t1.unique1 < 10 and t2.unique1 < 10
order by t1.unique1;

-- Ensure we get the expected result
select t1.unique1,t2.unique1 from tenk1 t1
inner join tenk1 t2 on t1.two = t2.two
  and t1.unique1 = (select min(unique1) from tenk1
                    where t2.unique1=unique1)
where t1.unique1 < 10 and t2.unique1 < 10
order by t1.unique1;

--
-- checks for correct handling of quals in multiway outer joins
--
explain (costs off)
select t1.f1
from int4_tbl t1, int4_tbl t2
  left join int4_tbl t3 on t3.f1 > 0
  left join int4_tbl t4 on t3.f1 > 1
where t4.f1 is null;

select t1.f1
from int4_tbl t1, int4_tbl t2
  left join int4_tbl t3 on t3.f1 > 0
  left join int4_tbl t4 on t3.f1 > 1
where t4.f1 is null;

explain (costs off)
select *
from int4_tbl t1 left join int4_tbl t2 on true
  left join int4_tbl t3 on t2.f1 > 0
  left join int4_tbl t4 on t3.f1 > 0;

explain (costs off)
select * from onek t1
  left join onek t2 on t1.unique1 = t2.unique1
  left join onek t3 on t2.unique1 != t3.unique1
  left join onek t4 on t3.unique1 = t4.unique1;

explain (costs off)
select * from int4_tbl t1
  left join (select now() from int4_tbl t2
             left join int4_tbl t3 on t2.f1 = t3.f1
             left join int4_tbl t4 on t3.f1 = t4.f1) s on true
  inner join int4_tbl t5 on true;

explain (costs off)
select * from int4_tbl t1
  left join int4_tbl t2 on true
  left join int4_tbl t3 on true
  left join int4_tbl t4 on t2.f1 = t3.f1;

explain (costs off)
select * from int4_tbl t1
  left join int4_tbl t2 on true
  left join int4_tbl t3 on t2.f1 = t3.f1
  left join int4_tbl t4 on t3.f1 != t4.f1;

explain (costs off)
select * from int4_tbl t1
  left join (int4_tbl t2 left join int4_tbl t3 on t2.f1 > 0) on t2.f1 > 1
  left join int4_tbl t4 on t2.f1 > 2 and t3.f1 > 3
where t1.f1 = coalesce(t2.f1, 1);

explain (costs off)
select * from int4_tbl t1
  left join ((select t2.f1 from int4_tbl t2
                left join int4_tbl t3 on t2.f1 > 0
                where t3.f1 is null) s
             left join tenk1 t4 on s.f1 > 1)
    on s.f1 = t1.f1;

explain (costs off)
select * from int4_tbl t1
  left join ((select t2.f1 from int4_tbl t2
                left join int4_tbl t3 on t2.f1 > 0
                where t2.f1 <> coalesce(t3.f1, -1)) s
             left join tenk1 t4 on s.f1 > 1)
    on s.f1 = t1.f1;

explain (costs off)
select * from onek t1
    left join onek t2 on t1.unique1 = t2.unique1
    left join onek t3 on t2.unique1 = t3.unique1
    left join onek t4 on t3.unique1 = t4.unique1 and t2.unique2 = t4.unique2;

explain (costs off)
select * from int8_tbl t1 left join
    (int8_tbl t2 left join int8_tbl t3 full join int8_tbl t4 on false on false)
    left join int8_tbl t5 on t2.q1 = t5.q1
on t2.q2 = 123;

explain (costs off)
select * from int8_tbl t1
    left join int8_tbl t2 on true
    left join lateral
      (select * from int8_tbl t3 where t3.q1 = t2.q1 offset 0) s
      on t2.q1 = 1;

explain (costs off)
select * from int8_tbl t1
    left join int8_tbl t2 on true
    left join lateral
      (select * from generate_series(t2.q1, 100)) s
      on t2.q1 = 1;

explain (costs off)
select * from int8_tbl t1
    left join int8_tbl t2 on true
    left join lateral
      (select t2.q1 from int8_tbl t3) s
      on t2.q1 = 1;

explain (costs off)
select * from onek t1
    left join onek t2 on true
    left join lateral
      (select * from onek t3 where t3.two = t2.two offset 0) s
      on t2.unique1 = 1;

--
-- check a case where we formerly got confused by conflicting sort orders
-- in redundant merge join path keys
--
explain (costs off)
select * from
  j1_tbl full join
  (select * from j2_tbl order by j2_tbl.i desc, j2_tbl.k asc) j2_tbl
  on j1_tbl.i = j2_tbl.i and j1_tbl.i = j2_tbl.k;

select * from
  j1_tbl full join
  (select * from j2_tbl order by j2_tbl.i desc, j2_tbl.k asc) j2_tbl
  on j1_tbl.i = j2_tbl.i and j1_tbl.i = j2_tbl.k;

--
-- a different check for handling of redundant sort keys in merge joins
--
explain (costs off)
select count(*) from
  (select * from tenk1 x order by x.thousand, x.twothousand, x.fivethous) x
  left join
  (select * from tenk1 y order by y.unique2) y
  on x.thousand = y.unique2 and x.twothousand = y.hundred and x.fivethous = y.unique2;

select count(*) from
  (select * from tenk1 x order by x.thousand, x.twothousand, x.fivethous) x
  left join
  (select * from tenk1 y order by y.unique2) y
  on x.thousand = y.unique2 and x.twothousand = y.hundred and x.fivethous = y.unique2;

set enable_hashjoin = 0;
set enable_nestloop = 0;
set enable_hashagg = 0;

--
-- Check that we use the pathkeys from a prefix of the group by / order by
-- clause for the join pathkeys when that prefix covers all join quals.  We
-- expect this to lead to an incremental sort for the group by / order by.
--
explain (costs off)
select x.thousand, x.twothousand, count(*)
from tenk1 x inner join tenk1 y on x.thousand = y.thousand
group by x.thousand, x.twothousand
order by x.thousand desc, x.twothousand;

reset enable_hashagg;
reset enable_nestloop;
reset enable_hashjoin;

--
-- Clean up
--

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;

DROP TABLE J1_TBL;
DROP TABLE J2_TBL;

-- Both DELETE and UPDATE allow the specification of additional tables
-- to "join" against to determine which rows should be modified.

CREATE TEMP TABLE t1 (a int, b int);
CREATE TEMP TABLE t2 (a int, b int);
CREATE TEMP TABLE t3 (x int, y int);

INSERT INTO t1 VALUES (5, 10);
INSERT INTO t1 VALUES (15, 20);
INSERT INTO t1 VALUES (100, 100);
INSERT INTO t1 VALUES (200, 1000);
INSERT INTO t2 VALUES (200, 2000);
INSERT INTO t3 VALUES (5, 20);
INSERT INTO t3 VALUES (6, 7);
INSERT INTO t3 VALUES (7, 8);
INSERT INTO t3 VALUES (500, 100);

DELETE FROM t3 USING t1 table1 WHERE t3.x = table1.a;
SELECT * FROM t3;
DELETE FROM t3 USING t1 JOIN t2 USING (a) WHERE t3.x > t1.a;
SELECT * FROM t3;
DELETE FROM t3 USING t3 t3_other WHERE t3.x = t3_other.x AND t3.y = t3_other.y;
SELECT * FROM t3;

-- Test join against inheritance tree

create temp table t2a () inherits (t2);

insert into t2a values (200, 2001);

select * from t1 left join t2 on (t1.a = t2.a);

-- Test matching of column name with wrong alias

select t1.x from t1 join t3 on (t1.a = t3.x);

-- Test matching of locking clause with wrong alias

select t1.*, t2.*, unnamed_join.* from
  t1 join t2 on (t1.a = t2.a), t3 as unnamed_join
  for update of unnamed_join;

select foo.*, unnamed_join.* from
  t1 join t2 using (a) as foo, t3 as unnamed_join
  for update of unnamed_join;

select foo.*, unnamed_join.* from
  t1 join t2 using (a) as foo, t3 as unnamed_join
  for update of foo;

select bar.*, unnamed_join.* from
  (t1 join t2 using (a) as foo) as bar, t3 as unnamed_join
  for update of foo;

select bar.*, unnamed_join.* from
  (t1 join t2 using (a) as foo) as bar, t3 as unnamed_join
  for update of bar;

--
-- regression test for 8.1 merge right join bug
--

CREATE TEMP TABLE tt1 ( tt1_id int4, joincol int4 );
INSERT INTO tt1 VALUES (1, 11);
INSERT INTO tt1 VALUES (2, NULL);

CREATE TEMP TABLE tt2 ( tt2_id int4, joincol int4 );
INSERT INTO tt2 VALUES (21, 11);
INSERT INTO tt2 VALUES (22, 11);

set enable_hashjoin to off;
set enable_nestloop to off;

-- these should give the same results

select tt1.*, tt2.* from tt1 left join tt2 on tt1.joincol = tt2.joincol;

select tt1.*, tt2.* from tt2 right join tt1 on tt1.joincol = tt2.joincol;

reset enable_hashjoin;
reset enable_nestloop;

--
-- regression test for bug #18522 (merge-right-anti-join in inner_unique cases)
--

create temp table tbl_ra(a int unique, b int);
insert into tbl_ra select i, i%100 from generate_series(1,1000)i;
create index on tbl_ra (b);
analyze tbl_ra;

set enable_hashjoin to off;
set enable_nestloop to off;

-- ensure we get a merge right anti join
explain (costs off)
select * from tbl_ra t1
where not exists (select 1 from tbl_ra t2 where t2.b = t1.a) and t1.b < 2;

-- and check we get the expected results
select * from tbl_ra t1
where not exists (select 1 from tbl_ra t2 where t2.b = t1.a) and t1.b < 2;

reset enable_hashjoin;
reset enable_nestloop;

--
-- regression test for bug with hash-right-semi join
--

create temp table tbl_rs(a int, b int);
insert into tbl_rs select i, i from generate_series(1,10)i;
analyze tbl_rs;

-- ensure we get a hash right semi join
explain (costs off)
select * from tbl_rs t1 join
  lateral (select * from tbl_rs t2 where t2.a in
            (select t1.a+t3.a from tbl_rs t3) and t2.a < 5)
  on true;

-- and check we get the expected results
select * from tbl_rs t1 join
  lateral (select * from tbl_rs t2 where t2.a in
            (select t1.a+t3.a from tbl_rs t3) and t2.a < 5)
  on true;

--
-- regression test for bug with parallel-hash-right-semi join
--

begin;

-- encourage use of parallel plans
set local parallel_setup_cost=0;
set local parallel_tuple_cost=0;
set local min_parallel_table_scan_size=0;
set local max_parallel_workers_per_gather=4;

-- ensure we don't get parallel hash right semi join
explain (costs off)
select * from tenk1 t1
where exists (select 1 from tenk1 t2 where fivethous = t1.fivethous)
and t1.fivethous < 5;

rollback;

--
-- regression test for bug #13908 (hash join with skew tuples & nbatch increase)
--

set work_mem to '64kB';
set enable_mergejoin to off;
set enable_memoize to off;

explain (costs off)
select count(*) from tenk1 a, tenk1 b
  where a.hundred = b.thousand and (b.fivethous % 10) < 10;
select count(*) from tenk1 a, tenk1 b
  where a.hundred = b.thousand and (b.fivethous % 10) < 10;

reset work_mem;
reset enable_mergejoin;
reset enable_memoize;

--
-- regression test for 8.2 bug with improper re-ordering of left joins
--

create temp table tt3(f1 int, f2 text);
insert into tt3 select x, repeat('xyzzy', 100) from generate_series(1,10000) x;
analyze tt3;

create temp table tt4(f1 int);
insert into tt4 values (0),(1),(9999);
analyze tt4;

set enable_nestloop to off;

EXPLAIN (COSTS OFF)
SELECT a.f1
FROM tt4 a
LEFT JOIN (
        SELECT b.f1
        FROM tt3 b LEFT JOIN tt3 c ON (b.f1 = c.f1)
        WHERE COALESCE(c.f1, 0) = 0
) AS d ON (a.f1 = d.f1)
WHERE COALESCE(d.f1, 0) = 0
ORDER BY 1;

SELECT a.f1
FROM tt4 a
LEFT JOIN (
        SELECT b.f1
        FROM tt3 b LEFT JOIN tt3 c ON (b.f1 = c.f1)
        WHERE COALESCE(c.f1, 0) = 0
) AS d ON (a.f1 = d.f1)
WHERE COALESCE(d.f1, 0) = 0
ORDER BY 1;

reset enable_nestloop;

--
-- basic semijoin and antijoin recognition tests
--

explain (costs off)
select a.* from tenk1 a
where unique1 in (select unique2 from tenk1 b);

-- sadly, this is not an antijoin
explain (costs off)
select a.* from tenk1 a
where unique1 not in (select unique2 from tenk1 b);

explain (costs off)
select a.* from tenk1 a
where exists (select 1 from tenk1 b where a.unique1 = b.unique2);

explain (costs off)
select a.* from tenk1 a
where not exists (select 1 from tenk1 b where a.unique1 = b.unique2);

explain (costs off)
select a.* from tenk1 a left join tenk1 b on a.unique1 = b.unique2
where b.unique2 is null;

-- check that we avoid de-duplicating columns redundantly
set enable_memoize to off;
explain (costs off)
select 1 from tenk1
where (hundred, thousand) in (select twothousand, twothousand from onek);
reset enable_memoize;

--
-- regression test for bogus RTE_GROUP entries
--

explain (costs off)
select a.* from tenk1 a
where exists (select 1 from tenk1 b where a.unique1 = b.unique2 group by b.unique1);

--
-- regression test for proper handling of outer joins within antijoins
--

create temp table tt4x(c1 int, c2 int, c3 int);

explain (costs off)
select * from tt4x t1
where not exists (
  select 1 from tt4x t2
    left join tt4x t3 on t2.c3 = t3.c1
    left join ( select t5.c1 as c1
                from tt4x t4 left join tt4x t5 on t4.c2 = t5.c1
              ) a1 on t3.c2 = a1.c1
  where t1.c1 = t2.c2
);

--
-- regression test for problems of the sort depicted in bug #3494
--

create temp table tt5(f1 int, f2 int);
create temp table tt6(f1 int, f2 int);

insert into tt5 values(1, 10);
insert into tt5 values(1, 11);

insert into tt6 values(1, 9);
insert into tt6 values(1, 2);
insert into tt6 values(2, 9);

select * from tt5,tt6 where tt5.f1 = tt6.f1 and tt5.f1 = tt5.f2 - tt6.f2;

--
-- regression test for problems of the sort depicted in bug #3588
--

create temp table xx (pkxx int);
create temp table yy (pkyy int, pkxx int);

insert into xx values (1);
insert into xx values (2);
insert into xx values (3);

insert into yy values (101, 1);
insert into yy values (201, 2);
insert into yy values (301, NULL);

select yy.pkyy as yy_pkyy, yy.pkxx as yy_pkxx, yya.pkyy as yya_pkyy,
       xxa.pkxx as xxa_pkxx, xxb.pkxx as xxb_pkxx
from yy
     left join (SELECT * FROM yy where pkyy = 101) as yya ON yy.pkyy = yya.pkyy
     left join xx xxa on yya.pkxx = xxa.pkxx
     left join xx xxb on coalesce (xxa.pkxx, 1) = xxb.pkxx;

--
-- regression test for improper pushing of constants across outer-join clauses
-- (as seen in early 8.2.x releases)
--

create temp table zt1 (f1 int primary key);
create temp table zt2 (f2 int primary key);
create temp table zt3 (f3 int primary key);
insert into zt1 values(53);
insert into zt2 values(53);

select * from
  zt2 left join zt3 on (f2 = f3)
      left join zt1 on (f3 = f1)
where f2 = 53;

create temp view zv1 as select *,'dummy'::text AS junk from zt1;

select * from
  zt2 left join zt3 on (f2 = f3)
      left join zv1 on (f3 = f1)
where f2 = 53;

--
-- regression test for improper extraction of OR indexqual conditions
-- (as seen in early 8.3.x releases)
--

select a.unique2, a.ten, b.tenthous, b.unique2, b.hundred
from tenk1 a left join tenk1 b on a.unique2 = b.tenthous
where a.unique1 = 42 and
      ((b.unique2 is null and a.ten = 2) or b.hundred = 3);

--
-- test proper positioning of one-time quals in EXISTS (8.4devel bug)
--
prepare foo(bool) as
  select count(*) from tenk1 a left join tenk1 b
    on (a.unique2 = b.unique1 and exists
        (select 1 from tenk1 c where c.thousand = b.unique2 and $1));
execute foo(true);
execute foo(false);

--
-- test for sane behavior with noncanonical merge clauses, per bug #4926
--

begin;

set enable_mergejoin = 1;
set enable_hashjoin = 0;
set enable_nestloop = 0;

create temp table a (i integer);
create temp table b (x integer, y integer);

select * from a left join b on i = x and i = y and x = i;

rollback;

--
-- test handling of merge clauses using record_ops
--
begin;

create type mycomptype as (id int, v bigint);

create temp table tidv (idv mycomptype);
create index on tidv (idv);

explain (costs off)
select a.idv, b.idv from tidv a, tidv b where a.idv = b.idv;

set enable_mergejoin = 0;
set enable_hashjoin = 0;

explain (costs off)
select a.idv, b.idv from tidv a, tidv b where a.idv = b.idv;

rollback;

--
-- test NULL behavior of whole-row Vars, per bug #5025
--
select t1.q2, count(t2.*)
from int8_tbl t1 left join int8_tbl t2 on (t1.q2 = t2.q1)
group by t1.q2 order by 1;

select t1.q2, count(t2.*)
from int8_tbl t1 left join (select * from int8_tbl) t2 on (t1.q2 = t2.q1)
group by t1.q2 order by 1;

select t1.q2, count(t2.*)
from int8_tbl t1 left join (select * from int8_tbl offset 0) t2 on (t1.q2 = t2.q1)
group by t1.q2 order by 1;

select t1.q2, count(t2.*)
from int8_tbl t1 left join
  (select q1, case when q2=1 then 1 else q2 end as q2 from int8_tbl) t2
  on (t1.q2 = t2.q1)
group by t1.q2 order by 1;

--
-- test incorrect failure to NULL pulled-up subexpressions
--
begin;

create temp table a (
     code char not null,
     constraint a_pk primary key (code)
);
create temp table b (
     a char not null,
     num integer not null,
     constraint b_pk primary key (a, num)
);
create temp table c (
     name char not null,
     a char,
     constraint c_pk primary key (name)
);

insert into a (code) values ('p');
insert into a (code) values ('q');
insert into b (a, num) values ('p', 1);
insert into b (a, num) values ('p', 2);
insert into c (name, a) values ('A', 'p');
insert into c (name, a) values ('B', 'q');
insert into c (name, a) values ('C', null);

select c.name, ss.code, ss.b_cnt, ss.const
from c left join
  (select a.code, coalesce(b_grp.cnt, 0) as b_cnt, -1 as const
   from a left join
     (select count(1) as cnt, b.a from b group by b.a) as b_grp
     on a.code = b_grp.a
  ) as ss
  on (c.a = ss.code)
order by c.name;

rollback;

--
-- test incorrect handling of placeholders that only appear in targetlists,
-- per bug #6154
--
SELECT * FROM
( SELECT 1 as key1 ) sub1
LEFT JOIN
( SELECT sub3.key3, sub4.value2, COALESCE(sub4.value2, 66) as value3 FROM
    ( SELECT 1 as key3 ) sub3
    LEFT JOIN
    ( SELECT sub5.key5, COALESCE(sub6.value1, 1) as value2 FROM
        ( SELECT 1 as key5 ) sub5
        LEFT JOIN
        ( SELECT 2 as key6, 42 as value1 ) sub6
        ON sub5.key5 = sub6.key6
    ) sub4
    ON sub4.key5 = sub3.key3
) sub2
ON sub1.key1 = sub2.key3;

-- test the path using join aliases, too
SELECT * FROM
( SELECT 1 as key1 ) sub1
LEFT JOIN
( SELECT sub3.key3, value2, COALESCE(value2, 66) as value3 FROM
    ( SELECT 1 as key3 ) sub3
    LEFT JOIN
    ( SELECT sub5.key5, COALESCE(sub6.value1, 1) as value2 FROM
        ( SELECT 1 as key5 ) sub5
        LEFT JOIN
        ( SELECT 2 as key6, 42 as value1 ) sub6
        ON sub5.key5 = sub6.key6
    ) sub4
    ON sub4.key5 = sub3.key3
) sub2
ON sub1.key1 = sub2.key3;

--
-- test case where a PlaceHolderVar is used as a nestloop parameter
--

EXPLAIN (COSTS OFF)
SELECT qq, unique1
  FROM
  ( SELECT COALESCE(q1, 0) AS qq FROM int8_tbl a ) AS ss1
  FULL OUTER JOIN
  ( SELECT COALESCE(q2, -1) AS qq FROM int8_tbl b ) AS ss2
  USING (qq)
  INNER JOIN tenk1 c ON qq = unique2;

SELECT qq, unique1
  FROM
  ( SELECT COALESCE(q1, 0) AS qq FROM int8_tbl a ) AS ss1
  FULL OUTER JOIN
  ( SELECT COALESCE(q2, -1) AS qq FROM int8_tbl b ) AS ss2
  USING (qq)
  INNER JOIN tenk1 c ON qq = unique2;

--
-- nested nestloops can require nested PlaceHolderVars
--

create temp table nt1 (
  id int primary key,
  a1 boolean,
  a2 boolean
);
create temp table nt2 (
  id int primary key,
  nt1_id int,
  b1 boolean,
  b2 boolean,
  foreign key (nt1_id) references nt1(id)
);
create temp table nt3 (
  id int primary key,
  nt2_id int,
  c1 boolean,
  foreign key (nt2_id) references nt2(id)
);

insert into nt1 values (1,true,true);
insert into nt1 values (2,true,false);
insert into nt1 values (3,false,false);
insert into nt2 values (1,1,true,true);
insert into nt2 values (2,2,true,false);
insert into nt2 values (3,3,false,false);
insert into nt3 values (1,1,true);
insert into nt3 values (2,2,false);
insert into nt3 values (3,3,true);

explain (costs off)
select nt3.id
from nt3 as nt3
  left join
    (select nt2.*, (nt2.b1 and ss1.a3) AS b3
     from nt2 as nt2
       left join
         (select nt1.*, (nt1.id is not null) as a3 from nt1) as ss1
         on ss1.id = nt2.nt1_id
    ) as ss2
    on ss2.id = nt3.nt2_id
where nt3.id = 1 and ss2.b3;

select nt3.id
from nt3 as nt3
  left join
    (select nt2.*, (nt2.b1 and ss1.a3) AS b3
     from nt2 as nt2
       left join
         (select nt1.*, (nt1.id is not null) as a3 from nt1) as ss1
         on ss1.id = nt2.nt1_id
    ) as ss2
    on ss2.id = nt3.nt2_id
where nt3.id = 1 and ss2.b3;

--
-- test case where a PlaceHolderVar is propagated into a subquery
--

explain (costs off)
select * from
  int8_tbl t1 left join
  (select q1 as x, 42 as y from int8_tbl t2) ss
  on t1.q2 = ss.x
where
  1 = (select 1 from int8_tbl t3 where ss.y is not null limit 1)
order by 1,2;

select * from
  int8_tbl t1 left join
  (select q1 as x, 42 as y from int8_tbl t2) ss
  on t1.q2 = ss.x
where
  1 = (select 1 from int8_tbl t3 where ss.y is not null limit 1)
order by 1,2;

--
-- variant where a PlaceHolderVar is needed at a join, but not above the join
--

explain (costs off)
select * from
  int4_tbl as i41,
  lateral
    (select 1 as x from
      (select i41.f1 as lat,
              i42.f1 as loc from
         int8_tbl as i81, int4_tbl as i42) as ss1
      right join int4_tbl as i43 on (i43.f1 > 1)
      where ss1.loc = ss1.lat) as ss2
where i41.f1 > 0;

select * from
  int4_tbl as i41,
  lateral
    (select 1 as x from
      (select i41.f1 as lat,
              i42.f1 as loc from
         int8_tbl as i81, int4_tbl as i42) as ss1
      right join int4_tbl as i43 on (i43.f1 > 1)
      where ss1.loc = ss1.lat) as ss2
where i41.f1 > 0;

--
-- test the corner cases FULL JOIN ON TRUE and FULL JOIN ON FALSE
--
select * from int4_tbl a full join int4_tbl b on true;
select * from int4_tbl a full join int4_tbl b on false;

--
-- test for ability to use a cartesian join when necessary
--

create temp table q1 as select 1 as q1;
create temp table q2 as select 0 as q2;
analyze q1;
analyze q2;

explain (costs off)
select * from
  tenk1 join int4_tbl on f1 = twothousand,
  q1, q2
where q1 = thousand or q2 = thousand;

explain (costs off)
select * from
  tenk1 join int4_tbl on f1 = twothousand,
  q1, q2
where thousand = (q1 + q2);

--
-- test ability to generate a suitable plan for a star-schema query
--

explain (costs off)
select * from
  tenk1, int8_tbl a, int8_tbl b
where thousand = a.q1 and tenthous = b.q1 and a.q2 = 1 and b.q2 = 2;

--
-- test a corner case in which we shouldn't apply the star-schema optimization
--

explain (costs off)
select t1.unique2, t1.stringu1, t2.unique1, t2.stringu2 from
  tenk1 t1
  inner join int4_tbl i1
    left join (select v1.x2, v2.y1, 11 AS d1
               from (select 1,0 from onerow) v1(x1,x2)
               left join (select 3,1 from onerow) v2(y1,y2)
               on v1.x1 = v2.y2) subq1
    on (i1.f1 = subq1.x2)
  on (t1.unique2 = subq1.d1)
  left join tenk1 t2
  on (subq1.y1 = t2.unique1)
where t1.unique2 < 42 and t1.stringu1 > t2.stringu2;

select t1.unique2, t1.stringu1, t2.unique1, t2.stringu2 from
  tenk1 t1
  inner join int4_tbl i1
    left join (select v1.x2, v2.y1, 11 AS d1
               from (select 1,0 from onerow) v1(x1,x2)
               left join (select 3,1 from onerow) v2(y1,y2)
               on v1.x1 = v2.y2) subq1
    on (i1.f1 = subq1.x2)
  on (t1.unique2 = subq1.d1)
  left join tenk1 t2
  on (subq1.y1 = t2.unique1)
where t1.unique2 < 42 and t1.stringu1 > t2.stringu2;

-- variant that isn't quite a star-schema case

explain (verbose, costs off)
select ss1.d1 from
  tenk1 as t1
  inner join tenk1 as t2
  on t1.tenthous = t2.ten
  inner join
    int8_tbl as i8
    left join int4_tbl as i4
      inner join (select 64::information_schema.cardinal_number as d1
                  from tenk1 t3,
                       lateral (select abs(t3.unique1) + random()) ss0(x)
                  where t3.fivethous < 0) as ss1
      on i4.f1 = ss1.d1
    on i8.q1 = i4.f1
  on t1.tenthous = ss1.d1
where t1.unique1 < i4.f1;

select ss1.d1 from
  tenk1 as t1
  inner join tenk1 as t2
  on t1.tenthous = t2.ten
  inner join
    int8_tbl as i8
    left join int4_tbl as i4
      inner join (select 64::information_schema.cardinal_number as d1
                  from tenk1 t3,
                       lateral (select abs(t3.unique1) + random()) ss0(x)
                  where t3.fivethous < 0) as ss1
      on i4.f1 = ss1.d1
    on i8.q1 = i4.f1
  on t1.tenthous = ss1.d1
where t1.unique1 < i4.f1;

-- this variant is foldable by the remove-useless-RESULT-RTEs code

explain (costs off)
select t1.unique2, t1.stringu1, t2.unique1, t2.stringu2 from
  tenk1 t1
  inner join int4_tbl i1
    left join (select v1.x2, v2.y1, 11 AS d1
               from (values(1,0)) v1(x1,x2)
               left join (values(3,1)) v2(y1,y2)
               on v1.x1 = v2.y2) subq1
    on (i1.f1 = subq1.x2)
  on (t1.unique2 = subq1.d1)
  left join tenk1 t2
  on (subq1.y1 = t2.unique1)
where t1.unique2 < 42 and t1.stringu1 > t2.stringu2;

select t1.unique2, t1.stringu1, t2.unique1, t2.stringu2 from
  tenk1 t1
  inner join int4_tbl i1
    left join (select v1.x2, v2.y1, 11 AS d1
               from (values(1,0)) v1(x1,x2)
               left join (values(3,1)) v2(y1,y2)
               on v1.x1 = v2.y2) subq1
    on (i1.f1 = subq1.x2)
  on (t1.unique2 = subq1.d1)
  left join tenk1 t2
  on (subq1.y1 = t2.unique1)
where t1.unique2 < 42 and t1.stringu1 > t2.stringu2;

-- Here's a variant that we can't fold too aggressively, though,
-- or we end up with noplace to evaluate the lateral PHV
explain (verbose, costs off)
select * from
  (select 1 as x) ss1 left join (select 2 as y) ss2 on (true),
  lateral (select ss2.y as z limit 1) ss3;
select * from
  (select 1 as x) ss1 left join (select 2 as y) ss2 on (true),
  lateral (select ss2.y as z limit 1) ss3;

-- This example demonstrates the folly of our old "have_dangerous_phv" logic
begin;
set local from_collapse_limit to 2;
explain (verbose, costs off)
select * from int8_tbl t1
  left join
    (select coalesce(t2.q1 + x, 0) from int8_tbl t2,
       lateral (select t3.q1 as x from int8_tbl t3,
                  lateral (select t2.q1, t3.q1 offset 0) s))
  on true;
rollback;

-- ... not that the initial replacement didn't have some bugs too
begin;
create temp table t(i int primary key);

explain (verbose, costs off)
select * from t t1
    left join (select 1 as x, * from t t2(i2)) t2ss on t1.i = t2ss.i2
    left join t t3(i3) on false
    left join t t4(i4) on t4.i4 > t2ss.x;

explain (verbose, costs off)
select * from
     (select k from
         (select i, coalesce(i, j) as k from
             (select i from t union all select 0)
             join (select 1 as j limit 1) on i = j)
         right join (select 2 as x) on true
         join (select 3 as y) on i is not null
     ),
     lateral (select k as kl limit 1);

rollback;

-- PHVs containing SubLinks are quite tricky to get right
explain (verbose, costs off)
select *
from int8_tbl i8
  inner join
    (select (select true) as x
       from int4_tbl i4, lateral (select i4.f1 as y limit 1) ss1
       where i4.f1 = 0) ss2 on true
  right join (select false as z) ss3 on true,
  lateral (select i8.q2 as q2l where x limit 1) ss4
where i8.q2 = 123;

explain (verbose, costs off)
select *
from int8_tbl i8
  inner join
    (select (select true) as x
       from int4_tbl i4, lateral (select 1 as y limit 1) ss1
       where i4.f1 = 0) ss2 on true
  right join (select false as z) ss3 on true,
  lateral (select i8.q2 as q2l where x limit 1) ss4
where i8.q2 = 123;

-- Test proper handling of appendrel PHVs during useless-RTE removal
explain (costs off)
select * from
  (select 0 as z) as t1
  left join
  (select true as a) as t2
  on true,
  lateral (select true as b
           union all
           select a as b) as t3
where b;

select * from
  (select 0 as z) as t1
  left join
  (select true as a) as t2
  on true,
  lateral (select true as b
           union all
           select a as b) as t3
where b;

-- Test PHV in a semijoin qual, which confused useless-RTE removal (bug #17700)
explain (verbose, costs off)
with ctetable as not materialized ( select 1 as f1 )
select * from ctetable c1
where f1 in ( select c3.f1 from ctetable c2 full join ctetable c3 on true );

with ctetable as not materialized ( select 1 as f1 )
select * from ctetable c1
where f1 in ( select c3.f1 from ctetable c2 full join ctetable c3 on true );

-- Test PHV that winds up in a Result node, despite having nonempty nullingrels
explain (verbose, costs off)
select table_catalog, table_name
from int4_tbl t1
  inner join (int8_tbl t2
              left join information_schema.column_udt_usage on null)
  on null;

-- Test handling of qual pushdown to appendrel members with non-Var outputs
explain (verbose, costs off)
select * from int4_tbl left join (
  select text 'foo' union all select text 'bar'
) ss(x) on true
where ss.x is null;

-- Test computation of varnullingrels when translating appendrel Var
begin;

create temp table t_append (a int not null, b int);
insert into t_append values (1, 1);
insert into t_append values (2, 3);

explain (verbose, costs off)
select t1.a, s.a from t_append t1
  left join t_append t2 on t1.a = t2.b
  join lateral (
    select t1.a as a union all select t2.a as a
  ) s on true
where s.a is not null;

select t1.a, s.a from t_append t1
  left join t_append t2 on t1.a = t2.b
  join lateral (
    select t1.a as a union all select t2.a as a
  ) s on true
where s.a is not null;

rollback;

--
-- test inlining of immutable functions
--
create function f_immutable_int4(i integer) returns integer as
$$ begin return i; end; $$ language plpgsql immutable;

-- check optimization of function scan with join
explain (costs off)
select unique1 from tenk1, (select * from f_immutable_int4(1) x) x
where x = unique1;

explain (verbose, costs off)
select unique1, x.*
from tenk1, (select *, random() from f_immutable_int4(1) x) x
where x = unique1;

explain (costs off)
select unique1 from tenk1, f_immutable_int4(1) x where x = unique1;

explain (costs off)
select unique1 from tenk1, lateral f_immutable_int4(1) x where x = unique1;

explain (costs off)
select unique1 from tenk1, lateral f_immutable_int4(1) x where x in (select 17);

explain (costs off)
select unique1, x from tenk1 join f_immutable_int4(1) x on unique1 = x;

explain (costs off)
select unique1, x from tenk1 left join f_immutable_int4(1) x on unique1 = x;

explain (costs off)
select unique1, x from tenk1 right join f_immutable_int4(1) x on unique1 = x;

explain (costs off)
select unique1, x from tenk1 full join f_immutable_int4(1) x on unique1 = x;

-- check that pullup of a const function allows further const-folding
explain (costs off)
select unique1 from tenk1, f_immutable_int4(1) x where x = 42;

-- test inlining of immutable functions with PlaceHolderVars
explain (costs off)
select nt3.id
from nt3 as nt3
  left join
    (select nt2.*, (nt2.b1 or i4 = 42) AS b3
     from nt2 as nt2
       left join
         f_immutable_int4(0) i4
         on i4 = nt2.nt1_id
    ) as ss2
    on ss2.id = nt3.nt2_id
where nt3.id = 1 and ss2.b3;

drop function f_immutable_int4(int);

-- test inlining when function returns composite

create function mki8(bigint, bigint) returns int8_tbl as
$$select row($1,$2)::int8_tbl$$ language sql;

create function mki4(int) returns int4_tbl as
$$select row($1)::int4_tbl$$ language sql;

explain (verbose, costs off)
select * from mki8(1,2);
select * from mki8(1,2);

explain (verbose, costs off)
select * from mki4(42);
select * from mki4(42);

drop function mki8(bigint, bigint);
drop function mki4(int);

-- test const-folding of a whole-row Var into a per-field Var
-- (need to inline a function to reach this case, else parser does it)
create function f_field_select(t onek) returns int4 as
$$ select t.unique2; $$ language sql immutable;

explain (verbose, costs off)
select (t2.*).unique1, f_field_select(t2) from tenk1 t1
    left join onek t2 on t1.unique1 = t2.unique1
    left join int8_tbl t3 on true;

drop function f_field_select(t onek);

--
-- test extraction of restriction OR clauses from join OR clause
-- (we used to only do this for indexable clauses)
--

explain (costs off)
select * from tenk1 a join tenk1 b on
  (a.unique1 = 1 and b.unique1 = 2) or (a.unique2 = 3 and b.hundred = 4);
explain (costs off)
select * from tenk1 a join tenk1 b on
  (a.unique1 = 1 and b.unique1 = 2) or (a.unique2 = 3 and b.ten = 4);
explain (costs off)
select * from tenk1 a join tenk1 b on
  (a.unique1 = 1 and b.unique1 = 2) or
  ((a.unique2 = 3 or a.unique2 = 7) and b.hundred = 4);

explain (costs off)
select * from tenk1 a join tenk1 b on
  (a.unique1 = 1 and b.unique1 = 2) or
  ((a.unique2 = 3 or a.unique2 = 7) and b.hundred = 4);
explain (costs off)
select * from tenk1 a join tenk1 b on
  (a.unique1 < 20 or a.unique1 = 3 or a.unique1 = 1 and b.unique1 = 2) or
  ((a.unique2 = 3 or a.unique2 = 7) and b.hundred = 4);

--
-- test placement of movable quals in a parameterized join tree
--

explain (costs off)
select * from tenk1 t1 left join
  (tenk1 t2 join tenk1 t3 on t2.thousand = t3.unique2)
  on t1.hundred = t2.hundred and t1.ten = t3.ten
where t1.unique1 = 1;

explain (costs off)
select * from tenk1 t1 left join
  (tenk1 t2 join tenk1 t3 on t2.thousand = t3.unique2)
  on t1.hundred = t2.hundred and t1.ten + t2.ten = t3.ten
where t1.unique1 = 1;

explain (costs off)
select count(*) from
  tenk1 a join tenk1 b on a.unique1 = b.unique2
  left join tenk1 c on a.unique2 = b.unique1 and c.thousand = a.thousand
  join int4_tbl on b.thousand = f1;

select count(*) from
  tenk1 a join tenk1 b on a.unique1 = b.unique2
  left join tenk1 c on a.unique2 = b.unique1 and c.thousand = a.thousand
  join int4_tbl on b.thousand = f1;

explain (costs off)
select b.unique1 from
  tenk1 a join tenk1 b on a.unique1 = b.unique2
  left join tenk1 c on b.unique1 = 42 and c.thousand = a.thousand
  join int4_tbl i1 on b.thousand = f1
  right join int4_tbl i2 on i2.f1 = b.tenthous
  order by 1;

select b.unique1 from
  tenk1 a join tenk1 b on a.unique1 = b.unique2
  left join tenk1 c on b.unique1 = 42 and c.thousand = a.thousand
  join int4_tbl i1 on b.thousand = f1
  right join int4_tbl i2 on i2.f1 = b.tenthous
  order by 1;

explain (costs off)
select * from
(
  select unique1, q1, coalesce(unique1, -1) + q1 as fault
  from int8_tbl left join tenk1 on (q2 = unique2)
) ss
where fault = 122
order by fault;

select * from
(
  select unique1, q1, coalesce(unique1, -1) + q1 as fault
  from int8_tbl left join tenk1 on (q2 = unique2)
) ss
where fault = 122
order by fault;

explain (costs off)
select * from
(values (1, array[10,20]), (2, array[20,30])) as v1(v1x,v1ys)
left join (values (1, 10), (2, 20)) as v2(v2x,v2y) on v2x = v1x
left join unnest(v1ys) as u1(u1y) on u1y = v2y;

select * from
(values (1, array[10,20]), (2, array[20,30])) as v1(v1x,v1ys)
left join (values (1, 10), (2, 20)) as v2(v2x,v2y) on v2x = v1x
left join unnest(v1ys) as u1(u1y) on u1y = v2y;

--
-- test handling of potential equivalence clauses above outer joins
--

explain (costs off)
select q1, unique2, thousand, hundred
  from int8_tbl a left join tenk1 b on q1 = unique2
  where coalesce(thousand,123) = q1 and q1 = coalesce(hundred,123);

select q1, unique2, thousand, hundred
  from int8_tbl a left join tenk1 b on q1 = unique2
  where coalesce(thousand,123) = q1 and q1 = coalesce(hundred,123);

explain (costs off)
select f1, unique2, case when unique2 is null then f1 else 0 end
  from int4_tbl a left join tenk1 b on f1 = unique2
  where (case when unique2 is null then f1 else 0 end) = 0;

select f1, unique2, case when unique2 is null then f1 else 0 end
  from int4_tbl a left join tenk1 b on f1 = unique2
  where (case when unique2 is null then f1 else 0 end) = 0;

--
-- another case with equivalence clauses above outer joins (bug #8591)
--

explain (costs off)
select a.unique1, b.unique1, c.unique1, coalesce(b.twothousand, a.twothousand)
  from tenk1 a left join tenk1 b on b.thousand = a.unique1                        left join tenk1 c on c.unique2 = coalesce(b.twothousand, a.twothousand)
  where a.unique2 < 10 and coalesce(b.twothousand, a.twothousand) = 44;

select a.unique1, b.unique1, c.unique1, coalesce(b.twothousand, a.twothousand)
  from tenk1 a left join tenk1 b on b.thousand = a.unique1                        left join tenk1 c on c.unique2 = coalesce(b.twothousand, a.twothousand)
  where a.unique2 < 10 and coalesce(b.twothousand, a.twothousand) = 44;

-- related case

explain (costs off)
select * from int8_tbl t1 left join int8_tbl t2 on t1.q2 = t2.q1,
  lateral (select * from int8_tbl t3 where t2.q1 = t2.q2) ss;

select * from int8_tbl t1 left join int8_tbl t2 on t1.q2 = t2.q1,
  lateral (select * from int8_tbl t3 where t2.q1 = t2.q2) ss;

--
-- check handling of join aliases when flattening multiple levels of subquery
--

explain (verbose, costs off)
select foo1.join_key as foo1_id, foo3.join_key AS foo3_id, bug_field from
  (values (0),(1)) foo1(join_key)
left join
  (select join_key, bug_field from
    (select ss1.join_key, ss1.bug_field from
      (select f1 as join_key, 666 as bug_field from int4_tbl i1) ss1
    ) foo2
   left join
    (select unique2 as join_key from tenk1 i2) ss2
   using (join_key)
  ) foo3
using (join_key);

select foo1.join_key as foo1_id, foo3.join_key AS foo3_id, bug_field from
  (values (0),(1)) foo1(join_key)
left join
  (select join_key, bug_field from
    (select ss1.join_key, ss1.bug_field from
      (select f1 as join_key, 666 as bug_field from int4_tbl i1) ss1
    ) foo2
   left join
    (select unique2 as join_key from tenk1 i2) ss2
   using (join_key)
  ) foo3
using (join_key);

--
-- check handling of a variable-free join alias
--
explain (verbose, costs off)
select * from
int4_tbl i0 left join
( (select *, 123 as x from int4_tbl i1) ss1
  left join
  (select *, q2 as x from int8_tbl i2) ss2
  using (x)
) ss0
on (i0.f1 = ss0.f1)
order by i0.f1, x;

select * from
int4_tbl i0 left join
( (select *, 123 as x from int4_tbl i1) ss1
  left join
  (select *, q2 as x from int8_tbl i2) ss2
  using (x)
) ss0
on (i0.f1 = ss0.f1)
order by i0.f1, x;

--
-- test successful handling of nested outer joins with degenerate join quals
--

explain (verbose, costs off)
select t1.* from
  text_tbl t1
  left join (select *, '***'::text as d1 from int8_tbl i8b1) b1
    left join int8_tbl i8
      left join (select *, null::int as d2 from int8_tbl i8b2) b2
      on (i8.q1 = b2.q1)
    on (b2.d2 = b1.q2)
  on (t1.f1 = b1.d1)
  left join int4_tbl i4
  on (i8.q2 = i4.f1);

select t1.* from
  text_tbl t1
  left join (select *, '***'::text as d1 from int8_tbl i8b1) b1
    left join int8_tbl i8
      left join (select *, null::int as d2 from int8_tbl i8b2) b2
      on (i8.q1 = b2.q1)
    on (b2.d2 = b1.q2)
  on (t1.f1 = b1.d1)
  left join int4_tbl i4
  on (i8.q2 = i4.f1);

explain (verbose, costs off)
select t1.* from
  text_tbl t1
  left join (select *, '***'::text as d1 from int8_tbl i8b1) b1
    left join int8_tbl i8
      left join (select *, null::int as d2 from int8_tbl i8b2, int4_tbl i4b2) b2
      on (i8.q1 = b2.q1)
    on (b2.d2 = b1.q2)
  on (t1.f1 = b1.d1)
  left join int4_tbl i4
  on (i8.q2 = i4.f1);

select t1.* from
  text_tbl t1
  left join (select *, '***'::text as d1 from int8_tbl i8b1) b1
    left join int8_tbl i8
      left join (select *, null::int as d2 from int8_tbl i8b2, int4_tbl i4b2) b2
      on (i8.q1 = b2.q1)
    on (b2.d2 = b1.q2)
  on (t1.f1 = b1.d1)
  left join int4_tbl i4
  on (i8.q2 = i4.f1);

explain (verbose, costs off)
select t1.* from
  text_tbl t1
  left join (select *, '***'::text as d1 from int8_tbl i8b1) b1
    left join int8_tbl i8
      left join (select *, null::int as d2 from int8_tbl i8b2, int4_tbl i4b2
                 where q1 = f1) b2
      on (i8.q1 = b2.q1)
    on (b2.d2 = b1.q2)
  on (t1.f1 = b1.d1)
  left join int4_tbl i4
  on (i8.q2 = i4.f1);

select t1.* from
  text_tbl t1
  left join (select *, '***'::text as d1 from int8_tbl i8b1) b1
    left join int8_tbl i8
      left join (select *, null::int as d2 from int8_tbl i8b2, int4_tbl i4b2
                 where q1 = f1) b2
      on (i8.q1 = b2.q1)
    on (b2.d2 = b1.q2)
  on (t1.f1 = b1.d1)
  left join int4_tbl i4
  on (i8.q2 = i4.f1);

explain (verbose, costs off)
select * from
  text_tbl t1
  inner join int8_tbl i8
  on i8.q2 = 456
  right join text_tbl t2
  on t1.f1 = 'doh!'
  left join int4_tbl i4
  on i8.q1 = i4.f1;

select * from
  text_tbl t1
  inner join int8_tbl i8
  on i8.q2 = 456
  right join text_tbl t2
  on t1.f1 = 'doh!'
  left join int4_tbl i4
  on i8.q1 = i4.f1;

-- check handling of a variable-free qual for a non-commutable outer join
explain (costs off)
select nspname
from (select 1 as x) ss1
left join
( select n.nspname, c.relname
  from pg_class c left join pg_namespace n on n.oid = c.relnamespace
  where c.relkind = 'r'
) ss2 on false;

-- check handling of apparently-commutable outer joins with non-commutable
-- joins between them
explain (costs off)
select 1 from
  int4_tbl i4
  left join int8_tbl i8 on i4.f1 is not null
  left join (select 1 as a) ss1 on null
  join int4_tbl i42 on ss1.a is null or i8.q1 <> i8.q2
  right join (select 2 as b) ss2
  on ss2.b < i4.f1;

--
-- test for appropriate join order in the presence of lateral references
--

explain (verbose, costs off)
select * from
  text_tbl t1
  left join int8_tbl i8
  on i8.q2 = 123,
  lateral (select i8.q1, t2.f1 from text_tbl t2 limit 1) as ss
where t1.f1 = ss.f1;

select * from
  text_tbl t1
  left join int8_tbl i8
  on i8.q2 = 123,
  lateral (select i8.q1, t2.f1 from text_tbl t2 limit 1) as ss
where t1.f1 = ss.f1;

explain (verbose, costs off)
select * from
  text_tbl t1
  left join int8_tbl i8
  on i8.q2 = 123,
  lateral (select i8.q1, t2.f1 from text_tbl t2 limit 1) as ss1,
  lateral (select ss1.* from text_tbl t3 limit 1) as ss2
where t1.f1 = ss2.f1;

select * from
  text_tbl t1
  left join int8_tbl i8
  on i8.q2 = 123,
  lateral (select i8.q1, t2.f1 from text_tbl t2 limit 1) as ss1,
  lateral (select ss1.* from text_tbl t3 limit 1) as ss2
where t1.f1 = ss2.f1;

explain (verbose, costs off)
select 1 from
  text_tbl as tt1
  inner join text_tbl as tt2 on (tt1.f1 = 'foo')
  left join text_tbl as tt3 on (tt3.f1 = 'foo')
  left join text_tbl as tt4 on (tt3.f1 = tt4.f1),
  lateral (select tt4.f1 as c0 from text_tbl as tt5 limit 1) as ss1
where tt1.f1 = ss1.c0;

select 1 from
  text_tbl as tt1
  inner join text_tbl as tt2 on (tt1.f1 = 'foo')
  left join text_tbl as tt3 on (tt3.f1 = 'foo')
  left join text_tbl as tt4 on (tt3.f1 = tt4.f1),
  lateral (select tt4.f1 as c0 from text_tbl as tt5 limit 1) as ss1
where tt1.f1 = ss1.c0;

explain (verbose, costs off)
select 1 from
  int4_tbl as i4
  inner join
    ((select 42 as n from int4_tbl x1 left join int8_tbl x2 on f1 = q1) as ss1
     right join (select 1 as z) as ss2 on true)
  on false,
  lateral (select i4.f1, ss1.n from int8_tbl as i8 limit 1) as ss3;

select 1 from
  int4_tbl as i4
  inner join
    ((select 42 as n from int4_tbl x1 left join int8_tbl x2 on f1 = q1) as ss1
     right join (select 1 as z) as ss2 on true)
  on false,
  lateral (select i4.f1, ss1.n from int8_tbl as i8 limit 1) as ss3;

--
-- check a case where we formerly generated invalid parameterized paths
--

begin;

create temp table t (a int unique);

explain (costs off)
select 1 from t t1
  join lateral (select t1.a from (select 1) foo offset 0) as s1 on true
  join
    (select 1 from t t2
       inner join (t t3
                   left join (t t4 left join t t5 on t4.a = 1)
                   on t3.a = t4.a)
       on false
     where t3.a = coalesce(t5.a,1)) as s2
  on true;

rollback;

--
-- check a case in which a PlaceHolderVar forces join order
--

explain (verbose, costs off)
select ss2.* from
  int4_tbl i41
  left join int8_tbl i8
    join (select i42.f1 as c1, i43.f1 as c2, 42 as c3
          from int4_tbl i42, int4_tbl i43) ss1
    on i8.q1 = ss1.c2
  on i41.f1 = ss1.c1,
  lateral (select i41.*, i8.*, ss1.* from text_tbl limit 1) ss2
where ss1.c2 = 0;

select ss2.* from
  int4_tbl i41
  left join int8_tbl i8
    join (select i42.f1 as c1, i43.f1 as c2, 42 as c3
          from int4_tbl i42, int4_tbl i43) ss1
    on i8.q1 = ss1.c2
  on i41.f1 = ss1.c1,
  lateral (select i41.*, i8.*, ss1.* from text_tbl limit 1) ss2
where ss1.c2 = 0;

--
-- test successful handling of full join underneath left join (bug #14105)
--

explain (costs off)
select * from
  (select 1 as id) as xx
  left join
    (tenk1 as a1 full join (select 1 as id) as yy on (a1.unique1 = yy.id))
  on (xx.id = coalesce(yy.id));

select * from
  (select 1 as id) as xx
  left join
    (tenk1 as a1 full join (select 1 as id) as yy on (a1.unique1 = yy.id))
  on (xx.id = coalesce(yy.id));

--
-- test ability to push constants through outer join clauses
--

explain (costs off)
  select * from int4_tbl a left join tenk1 b on f1 = unique2 where f1 = 0;

explain (costs off)
  select * from tenk1 a full join tenk1 b using(unique2) where unique2 = 42;

--
-- test that quals attached to an outer join have correct semantics,
-- specifically that they don't re-use expressions computed below the join;
-- we force a mergejoin so that coalesce(b.q1, 1) appears as a join input
--

set enable_hashjoin to off;
set enable_nestloop to off;

explain (verbose, costs off)
  select a.q2, b.q1
    from int8_tbl a left join int8_tbl b on a.q2 = coalesce(b.q1, 1)
    where coalesce(b.q1, 1) > 0;
select a.q2, b.q1
  from int8_tbl a left join int8_tbl b on a.q2 = coalesce(b.q1, 1)
  where coalesce(b.q1, 1) > 0;

reset enable_hashjoin;
reset enable_nestloop;

--
-- test join strength reduction with a SubPlan providing the proof
--

explain (costs off)
select a.unique1, b.unique2
  from onek a left join onek b on a.unique1 = b.unique2
  where (b.unique2, random() > 0) = any (select q1, random() > 0 from int8_tbl c where c.q1 < b.unique1);

select a.unique1, b.unique2
  from onek a left join onek b on a.unique1 = b.unique2
  where (b.unique2, random() > 0) = any (select q1, random() > 0 from int8_tbl c where c.q1 < b.unique1);

--
-- test full-join strength reduction
--

explain (costs off)
select a.unique1, b.unique2
  from onek a full join onek b on a.unique1 = b.unique2
  where a.unique1 = 42;

select a.unique1, b.unique2
  from onek a full join onek b on a.unique1 = b.unique2
  where a.unique1 = 42;

explain (costs off)
select a.unique1, b.unique2
  from onek a full join onek b on a.unique1 = b.unique2
  where b.unique2 = 43;

select a.unique1, b.unique2
  from onek a full join onek b on a.unique1 = b.unique2
  where b.unique2 = 43;

explain (costs off)
select a.unique1, b.unique2
  from onek a full join onek b on a.unique1 = b.unique2
  where a.unique1 = 42 and b.unique2 = 42;

select a.unique1, b.unique2
  from onek a full join onek b on a.unique1 = b.unique2
  where a.unique1 = 42 and b.unique2 = 42;

--
-- test result-RTE removal underneath a full join
--

explain (costs off)
select * from
  (select * from int8_tbl i81 join (values(123,2)) v(v1,v2) on q2=v1) ss1
full join
  (select * from (values(456,2)) w(v1,v2) join int8_tbl i82 on q2=v1) ss2
on true;

select * from
  (select * from int8_tbl i81 join (values(123,2)) v(v1,v2) on q2=v1) ss1
full join
  (select * from (values(456,2)) w(v1,v2) join int8_tbl i82 on q2=v1) ss2
on true;

--
-- test join removal
--

begin;

CREATE TEMP TABLE a (id int PRIMARY KEY, b_id int);
CREATE TEMP TABLE b (id int PRIMARY KEY, c_id int);
CREATE TEMP TABLE c (id int PRIMARY KEY);
CREATE TEMP TABLE d (a int, b int);
CREATE TEMP TABLE e (id1 int, id2 int, PRIMARY KEY(id1, id2));
INSERT INTO a VALUES (0, 0), (1, NULL);
INSERT INTO b VALUES (0, 0), (1, NULL);
INSERT INTO c VALUES (0), (1);
INSERT INTO d VALUES (1,3), (2,2), (3,1);
INSERT INTO e VALUES (0,0), (2,2), (3,1);

-- all these cases should be optimizable into a simple seqscan
explain (costs off) SELECT a.* FROM a LEFT JOIN b ON a.b_id = b.id;
explain (costs off) SELECT b.* FROM b LEFT JOIN c ON b.c_id = c.id;
explain (costs off)
  SELECT a.* FROM a LEFT JOIN (b left join c on b.c_id = c.id)
  ON (a.b_id = b.id);
explain (costs off)
  SELECT a.* FROM a LEFT JOIN b ON a.id = b.id
  LEFT JOIN e ON e.id1 = a.b_id AND b.c_id = e.id2;

-- check optimization of outer join within another special join
explain (costs off)
select id from a where id in (
	select b.id from b left join c on b.id = c.id
);

-- check optimization with oddly-nested outer joins
explain (costs off)
select a1.id from
  (a a1 left join a a2 on true)
  left join
  (a a3 left join a a4 on a3.id = a4.id)
  on a2.id = a3.id;

explain (costs off)
select a1.id from
  (a a1 left join a a2 on a1.id = a2.id)
  left join
  (a a3 left join a a4 on a3.id = a4.id)
  on a2.id = a3.id;

explain (costs off)
select 1 from a t1
    left join a t2 on true
   inner join a t3 on true
    left join a t4 on t2.id = t4.id and t2.id = t3.id;

-- another example (bug #17781)
explain (costs off)
select ss1.f1
from int4_tbl as t1
  left join (int4_tbl as t2
             right join int4_tbl as t3 on null
             left join (int4_tbl as t4
                        right join int8_tbl as t5 on null)
               on t2.f1 = t4.f1
             left join ((select null as f1 from int4_tbl as t6) as ss1
                        inner join int8_tbl as t7 on null)
               on t5.q1 = t7.q2)
    on false;

-- variant with Var rather than PHV coming from t6
explain (costs off)
select ss1.f1
from int4_tbl as t1
  left join (int4_tbl as t2
             right join int4_tbl as t3 on null
             left join (int4_tbl as t4
                        right join int8_tbl as t5 on null)
               on t2.f1 = t4.f1
             left join ((select f1 from int4_tbl as t6) as ss1
                        inner join int8_tbl as t7 on null)
               on t5.q1 = t7.q2)
    on false;

-- per further discussion of bug #17781
explain (costs off)
select ss1.x
from (select f1/2 as x from int4_tbl i4 left join a on a.id = i4.f1) ss1
     right join int8_tbl i8 on true
where current_user is not null;  -- this is to add a Result node

-- and further discussion of bug #17781
explain (costs off)
select *
from int8_tbl t1
  left join (int8_tbl t2 left join onek t3 on t2.q1 > t3.unique1)
    on t1.q2 = t2.q2
  left join onek t4
    on t2.q2 < t3.unique2;

-- bug #19460: we need to clean up RestrictInfos more than we had been doing
explain (costs off)
select * from
  (select 1::int as id) as lhs
full join
  (select dummy_source.id
   from (select null::int as id) as dummy_source
   left join (select a.id from a where a.id = 42) as sub
   on sub.id = dummy_source.id
  ) as rhs
on lhs.id = rhs.id;

explain (costs off)
select * from
  (select 1::int as id) as lhs
full join
  (select dummy_source.id
   from (select 2::int as id) as dummy_source
   left join (select a.id from a) as sub
   on sub.id = dummy_source.id
  ) as rhs
on lhs.id = rhs.id;

-- More tests of correct placement of pseudoconstant quals

-- simple constant-false condition
explain (costs off)
select * from int8_tbl t1 left join
  (int8_tbl t2 inner join int8_tbl t3 on false
   left join int8_tbl t4 on t2.q2 = t4.q2)
on t1.q1 = t2.q1;

-- deduce constant-false from an EquivalenceClass
explain (costs off)
select * from int8_tbl t1 left join
  (int8_tbl t2 inner join int8_tbl t3 on (t2.q1-t3.q2) = 0 and (t2.q1-t3.q2) = 1
   left join int8_tbl t4 on t2.q2 = t4.q2)
on t1.q1 = t2.q1;

-- pseudoconstant based on an outer-level Param
explain (costs off)
select exists(
  select * from int8_tbl t1 left join
    (int8_tbl t2 inner join int8_tbl t3 on x0.f1 = 1
     left join int8_tbl t4 on t2.q2 = t4.q2)
  on t1.q1 = t2.q1
) from int4_tbl x0;

-- check that join removal works for a left join when joining a subquery
-- that is guaranteed to be unique by its GROUP BY clause
explain (costs off)
select d.* from d left join (select * from b group by b.id, b.c_id) s
  on d.a = s.id and d.b = s.c_id;

-- similarly, but keying off a DISTINCT clause
explain (costs off)
select d.* from d left join (select distinct * from b) s
  on d.a = s.id and d.b = s.c_id;

-- join removal is not possible when the GROUP BY contains a column that is
-- not in the join condition.  (Note: as of 9.6, we notice that b.id is a
-- primary key and so drop b.c_id from the GROUP BY of the resulting plan;
-- but this happens too late for join removal in the outer plan level.)
explain (costs off)
select d.* from d left join (select * from b group by b.id, b.c_id) s
  on d.a = s.id;

-- similarly, but keying off a DISTINCT clause
explain (costs off)
select d.* from d left join (select distinct * from b) s
  on d.a = s.id;

-- join removal is not possible here
explain (costs off)
select 1 from a t1
  left join (a t2 left join a t3 on t2.id = 1) on t2.id = 1;

-- check join removal works when uniqueness of the join condition is enforced
-- by a UNION
explain (costs off)
select d.* from d left join (select id from a union select id from b) s
  on d.a = s.id;

-- check join removal with a cross-type comparison operator
explain (costs off)
select i8.* from int8_tbl i8 left join (select f1 from int4_tbl group by f1) i4
  on i8.q1 = i4.f1;

-- check join removal with lateral references
explain (costs off)
select 1 from (select a.id FROM a left join b on a.b_id = b.id) q,
			  lateral generate_series(1, q.id) gs(i) where q.id = gs.i;

-- check join removal within RHS of an outer join
explain (costs off)
select c.id, ss.a from c
  left join (select d.a from onerow, d left join b on d.a = b.id) ss
  on c.id = ss.a;

-- check the case when the placeholder relates to an outer join and its
-- inner in the press field but actually uses only the outer side of the join
explain (costs off)
SELECT q.val FROM b LEFT JOIN (
  SELECT (q1.z IS NOT NULL) AS val
  FROM b LEFT JOIN (
    SELECT (t1.b_id IS NOT NULL) AS z FROM a t1 LEFT JOIN a t2 USING (id)
    ) AS q1
  ON true
) AS q ON true;

CREATE TEMP TABLE parted_b (id int PRIMARY KEY) partition by range(id);
CREATE TEMP TABLE parted_b1 partition of parted_b for values from (0) to (10);

-- test join removals on a partitioned table
explain (costs off)
select a.* from a left join parted_b pb on a.b_id = pb.id;

rollback;

create temp table parent (k int primary key, pd int);
create temp table child (k int unique, cd int);
insert into parent values (1, 10), (2, 20), (3, 30);
insert into child values (1, 100), (4, 400);

-- this case is optimizable
select p.* from parent p left join child c on (p.k = c.k);
explain (costs off)
  select p.* from parent p left join child c on (p.k = c.k);

-- this case is not
select p.*, linked from parent p
  left join (select c.*, true as linked from child c) as ss
  on (p.k = ss.k);
explain (costs off)
  select p.*, linked from parent p
    left join (select c.*, true as linked from child c) as ss
    on (p.k = ss.k);

-- check for a 9.0rc1 bug: join removal breaks pseudoconstant qual handling
select p.* from
  parent p left join child c on (p.k = c.k)
  where p.k = 1 and p.k = 2;
explain (costs off)
select p.* from
  parent p left join child c on (p.k = c.k)
  where p.k = 1 and p.k = 2;

select p.* from
  (parent p left join child c on (p.k = c.k)) join parent x on p.k = x.k
  where p.k = 1 and p.k = 2;
explain (costs off)
select p.* from
  (parent p left join child c on (p.k = c.k)) join parent x on p.k = x.k
  where p.k = 1 and p.k = 2;

-- bug 5255: this is not optimizable by join removal
begin;

CREATE TEMP TABLE a (id int PRIMARY KEY);
CREATE TEMP TABLE b (id int PRIMARY KEY, a_id int);
INSERT INTO a VALUES (0), (1);
INSERT INTO b VALUES (0, 0), (1, NULL);

SELECT * FROM b LEFT JOIN a ON (b.a_id = a.id) WHERE (a.id IS NULL OR a.id > 0);
SELECT b.* FROM b LEFT JOIN a ON (b.a_id = a.id) WHERE (a.id IS NULL OR a.id > 0);

rollback;

-- another join removal bug: this is not optimizable, either
begin;

create temp table innertab (id int8 primary key, dat1 int8);
insert into innertab values(123, 42);

SELECT * FROM
    (SELECT 1 AS x) ss1
  LEFT JOIN
    (SELECT q1, q2, COALESCE(dat1, q1) AS y
     FROM int8_tbl LEFT JOIN innertab ON q2 = id) ss2
  ON true;

-- join removal bug #17769: can't remove if there's a pushed-down reference
EXPLAIN (COSTS OFF)
SELECT q2 FROM
  (SELECT *
   FROM int8_tbl LEFT JOIN innertab ON q2 = id) ss
 WHERE COALESCE(dat1, 0) = q1;

-- join removal bug #17773: otherwise-removable PHV appears in a qual condition
EXPLAIN (VERBOSE, COSTS OFF)
SELECT q2 FROM
  (SELECT q2, 'constant'::text AS x
   FROM int8_tbl LEFT JOIN innertab ON q2 = id) ss
  RIGHT JOIN int4_tbl ON NULL
 WHERE x >= x;

-- join removal bug #17786: check that OR conditions are cleaned up
EXPLAIN (COSTS OFF)
SELECT f1, x
FROM int4_tbl
     JOIN ((SELECT 42 AS x FROM int8_tbl LEFT JOIN innertab ON q1 = id) AS ss1
           RIGHT JOIN tenk1 ON NULL)
        ON tenk1.unique1 = ss1.x OR tenk1.unique2 = ss1.x;

rollback;

-- another join removal bug: we must clean up correctly when removing a PHV
begin;

create temp table uniquetbl (f1 text unique);

explain (costs off)
select t1.* from
  uniquetbl as t1
  left join (select *, '***'::text as d1 from uniquetbl) t2
  on t1.f1 = t2.f1
  left join uniquetbl t3
  on t2.d1 = t3.f1;

explain (costs off)
select t0.*
from
 text_tbl t0
 left join
   (select case t1.ten when 0 then 'doh!'::text else null::text end as case1,
           t1.stringu2
     from tenk1 t1
     join int4_tbl i4 ON i4.f1 = t1.unique2
     left join uniquetbl u1 ON u1.f1 = t1.string4) ss
  on t0.f1 = ss.case1
where ss.stringu2 !~* ss.case1;

select t0.*
from
 text_tbl t0
 left join
   (select case t1.ten when 0 then 'doh!'::text else null::text end as case1,
           t1.stringu2
     from tenk1 t1
     join int4_tbl i4 ON i4.f1 = t1.unique2
     left join uniquetbl u1 ON u1.f1 = t1.string4) ss
  on t0.f1 = ss.case1
where ss.stringu2 !~* ss.case1;

rollback;

-- another join removal bug: we must clean up EquivalenceClasses too
begin;

create temp table t (a int unique);
insert into t values (1);

explain (costs off)
select 1
from t t1
  left join (select 2 as c
             from t t2 left join t t3 on t2.a = t3.a) s
    on true
where t1.a = s.c;

select 1
from t t1
  left join (select 2 as c
             from t t2 left join t t3 on t2.a = t3.a) s
    on true
where t1.a = s.c;

rollback;

-- check handling of semijoins after join removal: we must suppress
-- unique-ification of known-constant values
begin;

create temp table t (a int unique, b int);
insert into t values (1, 2);

explain (verbose, costs off)
select t1.a from t t1
  left join t t2 on t1.a = t2.a
       join t t3 on true
where exists (select 1 from t t4
                join t t5 on t4.b = t5.b
                join t t6 on t5.b = t6.b
              where t1.a = t4.a and t3.a = t5.a and t4.a = 1);

select t1.a from t t1
  left join t t2 on t1.a = t2.a
       join t t3 on true
where exists (select 1 from t t4
                join t t5 on t4.b = t5.b
                join t t6 on t5.b = t6.b
              where t1.a = t4.a and t3.a = t5.a and t4.a = 1);

rollback;

-- check handling of semijoins if all RHS columns are equated to constants: we
-- should suppress unique-ification in this case.
begin;

create temp table t (a int, b int);
insert into t values (1, 2);

explain (costs off)
select * from t t1, t t2 where exists
  (select 1 from t t3 where t1.a = t3.a and t2.b = t3.b and t3.a = 1 and t3.b = 2);

select * from t t1, t t2 where exists
  (select 1 from t t3 where t1.a = t3.a and t2.b = t3.b and t3.a = 1 and t3.b = 2);

rollback;

-- check handling of semijoin unique-ification for child relations if all RHS
-- columns are equated to constants.
begin;

create temp table p (a int, b int) partition by range (a);
create temp table p1 partition of p for values from (0) to (10);
create temp table p2 partition of p for values from (10) to (20);
insert into p values (1, 2);
insert into p values (10, 20);

set enable_partitionwise_join to on;

explain (costs off)
select * from p t1 where exists
  (select 1 from p t2 where t1.a = t2.a and t1.a = 1);

select * from p t1 where exists
  (select 1 from p t2 where t1.a = t2.a and t1.a = 1);

rollback;

-- test cases where we can remove a join, but not a PHV computed at it
begin;

create temp table t (a int unique, b int);
insert into t values (1,1), (2,2);

explain (costs off)
select 1
from t t1
  left join (select t2.a, 1 as c
             from t t2 left join t t3 on t2.a = t3.a) s
  on true
  left join t t4 on true
where s.a < s.c;

explain (costs off)
select t1.a, s.*
from t t1
  left join lateral (select t2.a, coalesce(t1.a, 1) as c
                     from t t2 left join t t3 on t2.a = t3.a) s
  on true
  left join t t4 on true
where s.a < s.c;

select t1.a, s.*
from t t1
  left join lateral (select t2.a, coalesce(t1.a, 1) as c
                     from t t2 left join t t3 on t2.a = t3.a) s
  on true
  left join t t4 on true
where s.a < s.c;

rollback;

-- test case to expose miscomputation of required relid set for a PHV
explain (verbose, costs off)
select i8.*, ss.v, t.unique2
  from int8_tbl i8
    left join int4_tbl i4 on i4.f1 = 1
    left join lateral (select i4.f1 + 1 as v) as ss on true
    left join tenk1 t on t.unique2 = ss.v
where q2 = 456;

select i8.*, ss.v, t.unique2
  from int8_tbl i8
    left join int4_tbl i4 on i4.f1 = 1
    left join lateral (select i4.f1 + 1 as v) as ss on true
    left join tenk1 t on t.unique2 = ss.v
where q2 = 456;

-- and check a related issue where we miscompute required relids for
-- a PHV that's been translated to a child rel
create temp table parttbl (a integer primary key) partition by range (a);
create temp table parttbl1 partition of parttbl for values from (1) to (100);
insert into parttbl values (11), (12);
explain (costs off)
select * from
  (select *, 12 as phv from parttbl) as ss
  right join int4_tbl on true
where ss.a = ss.phv and f1 = 0;

select * from
  (select *, 12 as phv from parttbl) as ss
  right join int4_tbl on true
where ss.a = ss.phv and f1 = 0;

-- bug #8444: we've historically allowed duplicate aliases within aliased JOINs

select * from
  int8_tbl x join (int4_tbl x cross join int4_tbl y) j on q1 = f1; -- error
select * from
  int8_tbl x join (int4_tbl x cross join int4_tbl y) j on q1 = y.f1; -- error
select * from
  int8_tbl x join (int4_tbl x cross join int4_tbl y(ff)) j on q1 = f1; -- ok

--
-- test that semi- or inner self-joins on a unique column are removed
--

-- enable only nestloop to get more predictable plans
set enable_hashjoin to off;
set enable_mergejoin to off;

create table sj (a int unique, b int, c int unique);
insert into sj values (1, null, 2), (null, 2, null), (2, 1, 1);
analyze sj;

-- Trivial self-join case.
explain (costs off)
select p.* from sj p, sj q where q.a = p.a and q.b = q.a - 1;
select p.* from sj p, sj q where q.a = p.a and q.b = q.a - 1;

-- Self-join removal performs after a subquery pull-up process and could remove
-- such kind of self-join too. Check this option.
explain (costs off)
select * from sj p
where exists (select * from sj q
              where q.a = p.a and q.b < 10);
select * from sj p
where exists (select * from sj q
              where q.a = p.a and q.b < 10);

-- Don't remove self-join for the case of equality of two different unique columns.
explain (costs off)
select * from sj t1, sj t2 where t1.a = t2.c and t1.b is not null;

-- Ensure that relations with TABLESAMPLE clauses are not considered as
-- candidates to be removed
explain (costs off)
select * from sj t1
    join lateral
      (select * from sj tablesample system(t1.b)) s
    on t1.a = s.a;

-- Ensure that SJE does not form a self-referential lateral dependency
explain (costs off)
select * from sj t1
    left join lateral
      (select t1.a as t1a, * from sj t2) s
    on true
where t1.a = s.a;

-- Degenerated case.
explain (costs off)
select * from
  (select a as x from sj where false) as q1,
  (select a as y from sj where false) as q2
where q1.x = q2.y;

-- We can't use a cross-EC generated self join qual because of current logic of
-- the generate_join_implied_equalities routine.
explain (costs off)
select * from sj t1, sj t2 where t1.a = t1.b and t1.b = t2.b and t2.b = t2.a;
explain (costs off)
select * from sj t1, sj t2, sj t3
where t1.a = t1.b and t1.b = t2.b and t2.b = t2.a and
      t1.b = t3.b and t3.b = t3.a;

-- Double self-join removal.
-- Use a condition on "b + 1", not on "b", for the second join, so that
-- the equivalence class is different from the first one, and we can
-- test the non-ec code path.
explain (costs off)
select *
from  sj t1
      join sj t2 on t1.a = t2.a and t1.b = t2.b
	  join sj t3 on t2.a = t3.a and t2.b + 1 = t3.b + 1;

-- subselect that references the removed relation
explain (costs off)
select t1.a, (select a from sj where a = t2.a and a = t1.a)
from sj t1, sj t2
where t1.a = t2.a;

-- self-join under outer join
explain (costs off)
select * from sj x join sj y on x.a = y.a
left join int8_tbl z on x.a = z.q1;

explain (costs off)
select * from sj x join sj y on x.a = y.a
left join int8_tbl z on y.a = z.q1;

explain (costs off)
select * from (
  select t1.*, t2.a as ax from sj t1 join sj t2
  on (t1.a = t2.a and t1.c * t1.c = t2.c + 2 and t2.b is null)
) as q1
left join
  (select t3.* from sj t3, sj t4 where t3.c = t4.c) as q2
on q1.ax = q2.a;

-- Test that placeholders are updated correctly after join removal
explain (costs off)
select * from (values (1)) x
left join (select coalesce(y.q1, 1) from int8_tbl y
	right join sj j1 inner join sj j2 on j1.a = j2.a
	on true) z
on true;

-- Test that references to the removed rel in lateral subqueries are replaced
-- correctly after join removal
explain (verbose, costs off)
select t3.a from sj t1
	join sj t2 on t1.a = t2.a
	join lateral (select t1.a offset 0) t3 on true;

explain (verbose, costs off)
select t3.a from sj t1
	join sj t2 on t1.a = t2.a
	join lateral (select * from (select t1.a offset 0) offset 0) t3 on true;

explain (verbose, costs off)
select t4.a from sj t1
	join sj t2 on t1.a = t2.a
	join lateral (select t3.a from sj t3, (select t1.a) offset 0) t4 on true;

-- Check updating of semi_rhs_exprs links from upper-level semi join to
-- the removing relation
explain (verbose, costs off)
select t1.a from sj t1 where t1.b in (
  select t2.b from sj t2 join sj t3 on t2.c=t3.c);

--
-- SJE corner case: uniqueness of an inner is [partially] derived from
-- baserestrictinfo clauses.
-- XXX: We really should allow SJE for these corner cases?
--

INSERT INTO sj VALUES (3, 1, 3);

-- Don't remove SJ
EXPLAIN (COSTS OFF)
SELECT * FROM sj j1, sj j2 WHERE j1.b = j2.b AND j1.a = 2 AND j2.a = 3;
-- Return one row
SELECT * FROM sj j1, sj j2 WHERE j1.b = j2.b AND j1.a = 2 AND j2.a = 3;

-- Remove SJ, define uniqueness by a constant
EXPLAIN (COSTS OFF)
SELECT * FROM sj j1, sj j2 WHERE j1.b = j2.b AND j1.a = 2 AND j2.a = 2;
-- Return one row
SELECT * FROM sj j1, sj j2 WHERE j1.b = j2.b AND j1.a = 2 AND j2.a = 2;

-- Remove SJ, define uniqueness by a constant expression
EXPLAIN (COSTS OFF)
SELECT * FROM sj j1, sj j2
WHERE j1.b = j2.b
  AND j1.a = (EXTRACT(DOW FROM current_timestamp(0))/15 + 3)::int
  AND (EXTRACT(DOW FROM current_timestamp(0))/15 + 3)::int = j2.a;
-- Return one row
SELECT * FROM sj j1, sj j2
WHERE j1.b = j2.b
  AND j1.a = (EXTRACT(DOW FROM current_timestamp(0))/15 + 3)::int
  AND (EXTRACT(DOW FROM current_timestamp(0))/15 + 3)::int = j2.a;

-- Remove SJ
EXPLAIN (COSTS OFF)
SELECT * FROM sj j1, sj j2 WHERE j1.b = j2.b AND j1.a = 1 AND j2.a = 1;
-- Return no rows
SELECT * FROM sj j1, sj j2 WHERE j1.b = j2.b AND j1.a = 1 AND j2.a = 1;

-- Shuffle a clause. Remove SJ
EXPLAIN (COSTS OFF)
SELECT * FROM sj j1, sj j2 WHERE j1.b = j2.b AND 1 = j1.a AND j2.a = 1;
-- Return no rows
SELECT * FROM sj j1, sj j2 WHERE j1.b = j2.b AND 1 = j1.a AND j2.a = 1;

-- SJE Corner case: a 'a.x=a.x' clause, have replaced with 'a.x IS NOT NULL'
-- after SJ elimination it shouldn't be a mergejoinable clause.
EXPLAIN (COSTS OFF)
SELECT t4.*
FROM (SELECT t1.*, t2.a AS a1 FROM sj t1, sj t2 WHERE t1.b = t2.b) AS t3
JOIN sj t4 ON (t4.a = t3.a) WHERE t3.a1 = 42;
SELECT t4.*
FROM (SELECT t1.*, t2.a AS a1 FROM sj t1, sj t2 WHERE t1.b = t2.b) AS t3
JOIN sj t4 ON (t4.a = t3.a) WHERE t3.a1 = 42;

-- Functional index
CREATE UNIQUE INDEX sj_fn_idx ON sj((a * a));

-- Remove SJ
EXPLAIN (COSTS OFF)
SELECT * FROM sj j1, sj j2
	WHERE j1.b = j2.b AND j1.a*j1.a = 1 AND j2.a*j2.a = 1;
-- Don't remove SJ
EXPLAIN (COSTS OFF)
SELECT * FROM sj j1, sj j2
	WHERE j1.b = j2.b AND j1.a*j1.a = 1 AND j2.a*j2.a = 2;

-- Restriction contains expressions in both sides, Remove SJ.
EXPLAIN (COSTS OFF)
SELECT * FROM sj j1, sj j2
WHERE j1.b = j2.b
  AND (j1.a*j1.a) = (EXTRACT(DOW FROM current_timestamp(0))/15 + 3)::int
  AND (EXTRACT(DOW FROM current_timestamp(0))/15 + 3)::int = (j2.a*j2.a);
-- Empty set of rows should be returned
SELECT * FROM sj j1, sj j2
WHERE j1.b = j2.b
  AND (j1.a*j1.a) = (EXTRACT(DOW FROM current_timestamp(0))/15 + 3)::int
  AND (EXTRACT(DOW FROM current_timestamp(0))/15 + 3)::int = (j2.a*j2.a);

-- Restriction contains volatile function - disable SJE feature.
EXPLAIN (COSTS OFF)
SELECT * FROM sj j1, sj j2
WHERE j1.b = j2.b
  AND (j1.a*j1.c/3) = (random()/3 + 3)::int
  AND (random()/3 + 3)::int = (j2.a*j2.c/3);
-- Return one row
SELECT * FROM sj j1, sj j2
WHERE j1.b = j2.b
  AND (j1.a*j1.c/3) = (random()/3 + 3)::int
  AND (random()/3 + 3)::int = (j2.a*j2.c/3);

-- Multiple filters
CREATE UNIQUE INDEX sj_temp_idx1 ON sj(a,b,c);

-- Remove SJ
EXPLAIN (COSTS OFF)
SELECT * FROM sj j1, sj j2
	WHERE j1.b = j2.b AND j1.a = 2 AND j1.c = 3 AND j2.a = 2 AND 3 = j2.c;

-- Don't remove SJ
EXPLAIN (COSTS OFF)
	SELECT * FROM sj j1, sj j2
	WHERE j1.b = j2.b AND 2 = j1.a AND j1.c = 3 AND j2.a = 1 AND 3 = j2.c;

CREATE UNIQUE INDEX sj_temp_idx ON sj(a,b);

-- Don't remove SJ
EXPLAIN (COSTS OFF)
SELECT * FROM sj j1, sj j2 WHERE j1.b = j2.b AND j1.a = 2;

-- Don't remove SJ
EXPLAIN (COSTS OFF)
SELECT * FROM sj j1, sj j2 WHERE j1.b = j2.b AND 2 = j2.a;

-- Don't remove SJ
EXPLAIN (COSTS OFF)
SELECT * FROM sj j1, sj j2 WHERE j1.b = j2.b AND (j1.a = 1 OR j2.a = 1);

DROP INDEX sj_fn_idx, sj_temp_idx1, sj_temp_idx;

-- Test that OR predicated are updated correctly after join removal
CREATE TABLE tab_with_flag ( id INT PRIMARY KEY, is_flag SMALLINT);
CREATE INDEX idx_test_is_flag ON tab_with_flag (is_flag);

EXPLAIN (COSTS OFF)
SELECT COUNT(*) FROM tab_with_flag
WHERE
	(is_flag IS NULL OR is_flag = 0)
	AND id IN (SELECT id FROM tab_with_flag WHERE id IN (2, 3));
DROP TABLE tab_with_flag;

-- HAVING clause
explain (costs off)
select p.b from sj p join sj q on p.a = q.a group by p.b having sum(p.a) = 1;

-- update lateral references and range table entry reference
explain (verbose, costs off)
select 1 from (select x.* from sj x, sj y where x.a = y.a) q,
  lateral generate_series(1, q.a) gs(i);

explain (verbose, costs off)
select 1 from (select y.* from sj x, sj y where x.a = y.a) q,
  lateral generate_series(1, q.a) gs(i);

-- Test that a non-EC-derived join clause is processed correctly. Use an
-- outer join so that we can't form an EC.
explain (costs off) select * from sj p join sj q on p.a = q.a
  left join sj r on p.a + q.a = r.a;

-- FIXME this constant false filter doesn't look good. Should we merge
-- equivalence classes?
explain (costs off)
select * from sj p, sj q where p.a = q.a and p.b = 1 and q.b = 2;

-- Check that attr_needed is updated correctly after self-join removal. In this
-- test, the join of j1 with j2 is removed. k1.b is required at either j1 or j2.
-- If this info is lost, join targetlist for (k1, k2) will not contain k1.b.
-- Use index scan for k1 so that we don't get 'b' from physical tlist used for
-- seqscan. Also disable reordering of joins because this test depends on a
-- particular join tree.
create table sk (a int, b int);
create index on sk(a);
set join_collapse_limit to 1;
set enable_seqscan to off;
explain (costs off) select 1 from
	(sk k1 join sk k2 on k1.a = k2.a)
	join (sj j1 join sj j2 on j1.a = j2.a) on j1.b = k1.b;
explain (costs off) select 1 from
	(sk k1 join sk k2 on k1.a = k2.a)
	join (sj j1 join sj j2 on j1.a = j2.a) on j2.b = k1.b;
reset join_collapse_limit;
reset enable_seqscan;

-- Check that clauses from the join filter list is not lost on the self-join removal
CREATE TABLE emp1 (id SERIAL PRIMARY KEY NOT NULL, code int);
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM emp1 e1, emp1 e2 WHERE e1.id = e2.id AND e2.code <> e1.code;

-- Shuffle self-joined relations. Only in the case of iterative deletion
-- attempts explains of these queries will be identical.
CREATE UNIQUE INDEX ON emp1((id*id));

EXPLAIN (COSTS OFF)
SELECT count(*) FROM emp1 c1, emp1 c2, emp1 c3
WHERE c1.id=c2.id AND c1.id*c2.id=c3.id*c3.id;

EXPLAIN (COSTS OFF)
SELECT count(*) FROM emp1 c1, emp1 c2, emp1 c3
WHERE c1.id=c3.id AND c1.id*c3.id=c2.id*c2.id;

EXPLAIN (COSTS OFF)
SELECT count(*) FROM emp1 c1, emp1 c2, emp1 c3
WHERE c3.id=c2.id AND c3.id*c2.id=c1.id*c1.id;

-- Check the usage of a parse tree by the set operations (bug #18170)
EXPLAIN (COSTS OFF)
SELECT c1.code FROM emp1 c1 LEFT JOIN emp1 c2 ON c1.id = c2.id
WHERE c2.id IS NOT NULL
EXCEPT ALL
SELECT c3.code FROM emp1 c3;

-- Check that SJE removes references from PHVs correctly
explain (costs off)
select * from emp1 t1 left join
    (select coalesce(t3.code, 1) from emp1 t2
        left join (emp1 t3 join emp1 t4 on t3.id = t4.id)
        on true)
on true;

-- Try PHV, which could potentially be removed completely by SJE, but that's
-- not implemented yet.
explain (verbose, costs off)
select 1 from emp1 t1 left join
    ((select 1 as x, * from emp1 t2) s1 inner join
        (select * from emp1 t3) s2 on s1.id = s2.id)
    on true
where s1.x = 1;

-- Check that PHVs do not impose any constraints on removing self joins
explain (verbose, costs off)
select * from emp1 t1 join emp1 t2 on t1.id = t2.id left join
    lateral (select t1.id as t1id, * from generate_series(1,1) t3) s on true;

explain (verbose, costs off)
select * from generate_series(1,10) t1(id) left join
    lateral (select t1.id as t1id, t2.id from emp1 t2 join emp1 t3 on t2.id = t3.id)
on true;

-- This is a degenerate case of PHV usage: it is evaluated and needed inside
-- a baserel scan operation that the SJE removes.  The PHV in this test should
-- be in the filter of parameterized Index Scan: the replace_nestloop_params()
-- code will detect if the placeholder list doesn't have a reference to this
-- parameter.
--
-- NOTE:  enable_hashjoin and enable_mergejoin must be disabled.
CREATE TABLE tbl_phv(x int, y int PRIMARY KEY);
CREATE INDEX tbl_phv_idx ON tbl_phv(x);
INSERT INTO tbl_phv (x, y)
  SELECT gs, gs FROM generate_series(1,100) AS gs;
VACUUM ANALYZE tbl_phv;
EXPLAIN (COSTS OFF, VERBOSE)
SELECT 1 FROM tbl_phv t1 LEFT JOIN
  (SELECT 1 extra, x, y FROM tbl_phv tl) t3 JOIN
    (SELECT y FROM tbl_phv tr) t4
  ON t4.y = t3.y
ON true WHERE t3.extra IS NOT NULL AND t3.x = t1.x % 2;
DROP TABLE IF EXISTS tbl_phv;

-- Check that SJE replaces join clauses involving the removed rel correctly
explain (costs off)
select * from emp1 t1
   inner join emp1 t2 on t1.id = t2.id
    left join emp1 t3 on t1.id > 1 and t1.id < 2;

-- Check that SJE doesn't replace the target relation
EXPLAIN (COSTS OFF)
WITH t1 AS (SELECT * FROM emp1)
UPDATE emp1 SET code = t1.code + 1 FROM t1
WHERE t1.id = emp1.id RETURNING emp1.id, emp1.code, t1.code;

-- Check that SJE correctly replaces relations in OR-clauses
EXPLAIN (COSTS OFF)
SELECT * FROM emp1 t1
   INNER JOIN emp1 t2 ON t1.id = t2.id
    LEFT JOIN emp1 t3 ON t1.code = 1 AND (t2.code = t3.code OR t2.code = 1);

    INSERT INTO emp1 VALUES (1, 1), (2, 1);

WITH t1 AS (SELECT * FROM emp1)
UPDATE emp1 SET code = t1.code + 1 FROM t1
WHERE t1.id = emp1.id RETURNING emp1.id, emp1.code, t1.code;

TRUNCATE emp1;

EXPLAIN (COSTS OFF)
UPDATE sj sq SET b = 1 FROM sj as sz WHERE sq.a = sz.a;

CREATE RULE sj_del_rule AS ON DELETE TO sj
  DO INSTEAD
    UPDATE sj SET a = 1 WHERE a = old.a;
EXPLAIN (COSTS OFF) DELETE FROM sj;
DROP RULE sj_del_rule ON sj CASCADE;

-- Check that SJE does not mistakenly omit qual clauses (bug #18187)
insert into emp1 values (1, 1);
explain (costs off)
select 1 from emp1 full join
    (select * from emp1 t1 join
        emp1 t2 join emp1 t3 on t2.id = t3.id
        on true
    where false) s on true
where false;
select 1 from emp1 full join
    (select * from emp1 t1 join
        emp1 t2 join emp1 t3 on t2.id = t3.id
        on true
    where false) s on true
where false;

-- Check that SJE does not mistakenly re-use knowledge of relation uniqueness
-- made with different set of quals
insert into emp1 values (2, 1);
explain (costs off)
select * from emp1 t1 where exists (select * from emp1 t2
                                    where t2.id = t1.code and t2.code > 0);
select * from emp1 t1 where exists (select * from emp1 t2
                                    where t2.id = t1.code and t2.code > 0);

-- We can remove the join even if we find the join can't duplicate rows and
-- the base quals of each side are different.  In the following case we end up
-- moving quals over to s1 to make it so it can't match any rows.
create table sl(a int, b int, c int);
create unique index on sl(a, b);
vacuum analyze sl;

-- Both sides are unique, but base quals are different
explain (costs off)
select * from sl t1, sl t2 where t1.a = t2.a and t1.b = 1 and t2.b = 2;

-- Check NullTest in baserestrictinfo list
explain (costs off)
select * from sl t1, sl t2
where t1.a = t2.a and t1.b = 1 and t2.b = 2
  and t1.c IS NOT NULL and t2.c IS NOT NULL
  and t2.b IS NOT NULL and t1.b IS NOT NULL
  and t1.a IS NOT NULL and t2.a IS NOT NULL;
explain (verbose, costs off)
select * from sl t1, sl t2
where t1.b = t2.b and t2.a = 3 and t1.a = 3
  and t1.c IS NOT NULL and t2.c IS NOT NULL
  and t2.b IS NOT NULL and t1.b IS NOT NULL
  and t1.a IS NOT NULL and t2.a IS NOT NULL;

-- Join qual isn't mergejoinable, but inner is unique.
EXPLAIN (COSTS OFF)
SELECT n2.a FROM sj n1, sj n2 WHERE n1.a <> n2.a AND n2.a = 1;

EXPLAIN (COSTS OFF)
SELECT * FROM
(SELECT n2.a FROM sj n1, sj n2 WHERE n1.a <> n2.a) q0, sl
WHERE q0.a = 1;

-- Do not forget to replace relid in bare Var join clause (bug #19435)
ALTER TABLE sl ADD COLUMN bool_col boolean;
EXPLAIN (COSTS OFF)
SELECT 1 AS c1 FROM sl sl1 LEFT JOIN (sl AS sl2 NATURAL JOIN sl AS sl3)
  ON sl2.bool_col LEFT JOIN sl AS sl4 ON sl2.bool_col;

-- Check optimization disabling if it will violate special join conditions.
-- Two identical joined relations satisfies self join removal conditions but
-- stay in different special join infos.
CREATE TABLE sj_t1 (id serial, a int);
CREATE TABLE sj_t2 (id serial, a int);
CREATE TABLE sj_t3 (id serial, a int);
CREATE TABLE sj_t4 (id serial, a int);

CREATE UNIQUE INDEX ON sj_t3 USING btree (a,id);
CREATE UNIQUE INDEX ON sj_t2 USING btree (id);

EXPLAIN (COSTS OFF)
SELECT * FROM sj_t1
JOIN (
	SELECT sj_t2.id AS id FROM sj_t2
	WHERE EXISTS
		(
		SELECT TRUE FROM sj_t3,sj_t4 WHERE sj_t3.a = 1 AND sj_t3.id = sj_t2.id
		)
	) t2t3t4
ON sj_t1.id = t2t3t4.id
JOIN (
	SELECT sj_t2.id AS id FROM sj_t2
	WHERE EXISTS
		(
		SELECT TRUE FROM sj_t3,sj_t4 WHERE sj_t3.a = 1 AND sj_t3.id = sj_t2.id
		)
	) _t2t3t4
ON sj_t1.id = _t2t3t4.id;

--
-- Test RowMarks-related code
--

-- Both sides have explicit LockRows marks
EXPLAIN (COSTS OFF)
SELECT a1.a FROM sj a1,sj a2 WHERE (a1.a=a2.a) FOR UPDATE;

reset enable_hashjoin;
reset enable_mergejoin;

--
-- Test hints given on incorrect column references are useful
--

select t1.uunique1 from
  tenk1 t1 join tenk2 t2 on t1.two = t2.two; -- error, prefer "t1" suggestion
select t2.uunique1 from
  tenk1 t1 join tenk2 t2 on t1.two = t2.two; -- error, prefer "t2" suggestion
select uunique1 from
  tenk1 t1 join tenk2 t2 on t1.two = t2.two; -- error, suggest both at once
select ctid from
  tenk1 t1 join tenk2 t2 on t1.two = t2.two; -- error, need qualification

--
-- Take care to reference the correct RTE
--

select atts.relid::regclass, s.* from pg_stats s join
    pg_attribute a on s.attname = a.attname and s.tablename =
    a.attrelid::regclass::text join (select unnest(indkey) attnum,
    indexrelid from pg_index i) atts on atts.attnum = a.attnum where
    schemaname != 'pg_catalog';

-- Test bug in rangetable flattening
explain (verbose, costs off)
select 1 from
  (select * from int8_tbl where q1 <> (select 42) offset 0) ss
where false;

--
-- Test LATERAL
--

select unique2, x.*
from tenk1 a, lateral (select * from int4_tbl b where f1 = a.unique1) x;
explain (costs off)
  select unique2, x.*
  from tenk1 a, lateral (select * from int4_tbl b where f1 = a.unique1) x;
select unique2, x.*
from int4_tbl x, lateral (select unique2 from tenk1 where f1 = unique1) ss;
explain (costs off)
  select unique2, x.*
  from int4_tbl x, lateral (select unique2 from tenk1 where f1 = unique1) ss;
explain (costs off)
  select unique2, x.*
  from int4_tbl x cross join lateral (select unique2 from tenk1 where f1 = unique1) ss;
select unique2, x.*
from int4_tbl x left join lateral (select unique1, unique2 from tenk1 where f1 = unique1) ss on true;
explain (costs off)
  select unique2, x.*
  from int4_tbl x left join lateral (select unique1, unique2 from tenk1 where f1 = unique1) ss on true;

-- check scoping of lateral versus parent references
-- the first of these should return int8_tbl.q2, the second int8_tbl.q1
select *, (select r from (select q1 as q2) x, (select q2 as r) y) from int8_tbl;
select *, (select r from (select q1 as q2) x, lateral (select q2 as r) y) from int8_tbl;

-- lateral with function in FROM
select count(*) from tenk1 a, lateral generate_series(1,two) g;
explain (costs off)
  select count(*) from tenk1 a, lateral generate_series(1,two) g;
explain (costs off)
  select count(*) from tenk1 a cross join lateral generate_series(1,two) g;
-- don't need the explicit LATERAL keyword for functions
explain (costs off)
  select count(*) from tenk1 a, generate_series(1,two) g;

-- lateral with UNION ALL subselect
explain (costs off)
  select * from generate_series(100,200) g,
    lateral (select * from int8_tbl a where g = q1 union all
             select * from int8_tbl b where g = q2) ss;
select * from generate_series(100,200) g,
  lateral (select * from int8_tbl a where g = q1 union all
           select * from int8_tbl b where g = q2) ss;

-- lateral with VALUES
explain (costs off)
  select count(*) from tenk1 a,
    tenk1 b join lateral (values(a.unique1)) ss(x) on b.unique2 = ss.x;
select count(*) from tenk1 a,
  tenk1 b join lateral (values(a.unique1)) ss(x) on b.unique2 = ss.x;

-- lateral with VALUES, no flattening possible
explain (costs off)
  select count(*) from tenk1 a,
    tenk1 b join lateral (values(a.unique1),(-1)) ss(x) on b.unique2 = ss.x;
select count(*) from tenk1 a,
  tenk1 b join lateral (values(a.unique1),(-1)) ss(x) on b.unique2 = ss.x;

-- lateral injecting a strange outer join condition
explain (costs off)
  select * from int8_tbl a,
    int8_tbl x left join lateral (select a.q1 from int4_tbl y) ss(z)
      on x.q2 = ss.z
  order by a.q1, a.q2, x.q1, x.q2, ss.z;
select * from int8_tbl a,
  int8_tbl x left join lateral (select a.q1 from int4_tbl y) ss(z)
    on x.q2 = ss.z
  order by a.q1, a.q2, x.q1, x.q2, ss.z;

-- lateral reference to a join alias variable
select * from (select f1/2 as x from int4_tbl) ss1 join int4_tbl i4 on x = f1,
  lateral (select x) ss2(y);
select * from (select f1 as x from int4_tbl) ss1 join int4_tbl i4 on x = f1,
  lateral (values(x)) ss2(y);
select * from ((select f1/2 as x from int4_tbl) ss1 join int4_tbl i4 on x = f1) j,
  lateral (select x) ss2(y);

-- lateral references requiring pullup
select * from (values(1)) x(lb),
  lateral generate_series(lb,4) x4;
select * from (select f1/1000000000 from int4_tbl) x(lb),
  lateral generate_series(lb,4) x4;
select * from (values(1)) x(lb),
  lateral (values(lb)) y(lbcopy);
select * from (values(1)) x(lb),
  lateral (select lb from int4_tbl) y(lbcopy);
select * from
  int8_tbl x left join (select q1,coalesce(q2,0) q2 from int8_tbl) y on x.q2 = y.q1,
  lateral (values(x.q1,y.q1,y.q2)) v(xq1,yq1,yq2);
select * from
  int8_tbl x left join (select q1,coalesce(q2,0) q2 from int8_tbl) y on x.q2 = y.q1,
  lateral (select x.q1,y.q1,y.q2) v(xq1,yq1,yq2);
select x.* from
  int8_tbl x left join (select q1,coalesce(q2,0) q2 from int8_tbl) y on x.q2 = y.q1,
  lateral (select x.q1,y.q1,y.q2) v(xq1,yq1,yq2);
select v.* from
  (int8_tbl x left join (select q1,coalesce(q2,0) q2 from int8_tbl) y on x.q2 = y.q1)
  left join int4_tbl z on z.f1 = x.q2,
  lateral (select x.q1,y.q1 union all select x.q2,y.q2) v(vx,vy);
select v.* from
  (int8_tbl x left join (select q1,(select coalesce(q2,0)) q2 from int8_tbl) y on x.q2 = y.q1)
  left join int4_tbl z on z.f1 = x.q2,
  lateral (select x.q1,y.q1 union all select x.q2,y.q2) v(vx,vy);
select v.* from
  (int8_tbl x left join (select q1,(select coalesce(q2,0)) q2 from int8_tbl) y on x.q2 = y.q1)
  left join int4_tbl z on z.f1 = x.q2,
  lateral (select x.q1,y.q1 from onerow union all select x.q2,y.q2 from onerow) v(vx,vy);

explain (verbose, costs off)
select * from
  int8_tbl a left join
  lateral (select *, a.q2 as x from int8_tbl b) ss on a.q2 = ss.q1;
select * from
  int8_tbl a left join
  lateral (select *, a.q2 as x from int8_tbl b) ss on a.q2 = ss.q1;
explain (verbose, costs off)
select * from
  int8_tbl a left join
  lateral (select *, coalesce(a.q2, 42) as x from int8_tbl b) ss on a.q2 = ss.q1;
select * from
  int8_tbl a left join
  lateral (select *, coalesce(a.q2, 42) as x from int8_tbl b) ss on a.q2 = ss.q1;

-- lateral can result in join conditions appearing below their
-- real semantic level
explain (verbose, costs off)
select * from int4_tbl i left join
  lateral (select * from int2_tbl j where i.f1 = j.f1) k on true;
select * from int4_tbl i left join
  lateral (select * from int2_tbl j where i.f1 = j.f1) k on true;
explain (verbose, costs off)
select * from int4_tbl i left join
  lateral (select coalesce(i) from int2_tbl j where i.f1 = j.f1) k on true;
select * from int4_tbl i left join
  lateral (select coalesce(i) from int2_tbl j where i.f1 = j.f1) k on true;
explain (verbose, costs off)
select * from int4_tbl a,
  lateral (
    select * from int4_tbl b left join int8_tbl c on (b.f1 = q1 and a.f1 = q2)
  ) ss;
select * from int4_tbl a,
  lateral (
    select * from int4_tbl b left join int8_tbl c on (b.f1 = q1 and a.f1 = q2)
  ) ss;

-- lateral reference in a PlaceHolderVar evaluated at join level
explain (verbose, costs off)
select * from
  int8_tbl a left join lateral
  (select b.q1 as bq1, c.q1 as cq1, least(a.q1,b.q1,c.q1) from
   int8_tbl b cross join int8_tbl c) ss
  on a.q2 = ss.bq1;
select * from
  int8_tbl a left join lateral
  (select b.q1 as bq1, c.q1 as cq1, least(a.q1,b.q1,c.q1) from
   int8_tbl b cross join int8_tbl c) ss
  on a.q2 = ss.bq1;

-- case requiring nested PlaceHolderVars
explain (verbose, costs off)
select * from
  int8_tbl c left join (
    int8_tbl a left join (select q1, coalesce(q2,42) as x from int8_tbl b) ss1
      on a.q2 = ss1.q1
    cross join
    lateral (select q1, coalesce(ss1.x,q2) as y from int8_tbl d) ss2
  ) on c.q2 = ss2.q1,
  lateral (select ss2.y offset 0) ss3;

-- another case requiring nested PlaceHolderVars
explain (verbose, costs off)
select * from
  (select 0 as val0) as ss0
  left join (select 1 as val) as ss1 on true
  left join lateral (select ss1.val as val_filtered where false) as ss2 on true;

select * from
  (select 0 as val0) as ss0
  left join (select 1 as val) as ss1 on true
  left join lateral (select ss1.val as val_filtered where false) as ss2 on true;

-- case that breaks the old ph_may_need optimization
explain (verbose, costs off)
select c.*,a.*,ss1.q1,ss2.q1,ss3.* from
  int8_tbl c left join (
    int8_tbl a left join
      (select q1, coalesce(q2,f1) as x from int8_tbl b, int4_tbl b2
       where q1 < f1) ss1
      on a.q2 = ss1.q1
    cross join
    lateral (select q1, coalesce(ss1.x,q2) as y from int8_tbl d) ss2
  ) on c.q2 = ss2.q1,
  lateral (select * from int4_tbl i where ss2.y > f1) ss3;

-- check processing of postponed quals (bug #9041)
explain (verbose, costs off)
select * from
  (select 1 as x offset 0) x cross join (select 2 as y offset 0) y
  left join lateral (
    select * from (select 3 as z offset 0) z where z.z = x.x
  ) zz on zz.z = y.y;

-- a new postponed-quals issue (bug #17768)
explain (costs off)
select * from int4_tbl t1,
  lateral (select * from int4_tbl t2 inner join int4_tbl t3 on t1.f1 = 1
           inner join (int4_tbl t4 left join int4_tbl t5 on true) on true) ss;

-- check dummy rels with lateral references (bug #15694)
explain (verbose, costs off)
select * from int8_tbl i8 left join lateral
  (select *, i8.q2 from int4_tbl where false) ss on true;
explain (verbose, costs off)
select * from int8_tbl i8 left join lateral
  (select *, i8.q2 from int4_tbl i1, int4_tbl i2 where false) ss on true;

-- check handling of nested appendrels inside LATERAL
select * from
  ((select 2 as v) union all (select 3 as v)) as q1
  cross join lateral
  ((select * from
      ((select 4 as v) union all (select 5 as v)) as q3)
   union all
   (select q1.v)
  ) as q2;

-- check the number of columns specified
SELECT * FROM (int8_tbl i cross join int4_tbl j) ss(a,b,c,d);

-- check we don't try to do a unique-ified semijoin with LATERAL
explain (verbose, costs off)
select * from
  (values (0,9998), (1,1000)) v(id,x),
  lateral (select f1 from int4_tbl
           where f1 = any (select unique1 from tenk1
                           where unique2 = v.x offset 0)) ss;
select * from
  (values (0,9998), (1,1000)) v(id,x),
  lateral (select f1 from int4_tbl
           where f1 = any (select unique1 from tenk1
                           where unique2 = v.x offset 0)) ss;

-- check proper extParam/allParam handling (this isn't exactly a LATERAL issue,
-- but we can make the test case much more compact with LATERAL)
explain (verbose, costs off)
select * from (values (0), (1)) v(id),
lateral (select * from int8_tbl t1,
         lateral (select * from
                    (select * from int8_tbl t2
                     where (q1, random() > 0) = any (select q2, random() > 0 from int8_tbl t3
                                     where q2 = (select greatest(t1.q1,t2.q2))
                                       and (select v.id=0)) offset 0) ss2) ss
         where t1.q1 = ss.q2) ss0;

select * from (values (0), (1)) v(id),
lateral (select * from int8_tbl t1,
         lateral (select * from
                    (select * from int8_tbl t2
                     where (q1, random() > 0) = any (select q2, random() > 0 from int8_tbl t3
                                     where q2 = (select greatest(t1.q1,t2.q2))
                                       and (select v.id=0)) offset 0) ss2) ss
         where t1.q1 = ss.q2) ss0;

-- test some error cases where LATERAL should have been used but wasn't
select f1,g from int4_tbl a, (select f1 as g) ss;
select f1,g from int4_tbl a, (select a.f1 as g) ss;
select f1,g from int4_tbl a cross join (select f1 as g) ss;
select f1,g from int4_tbl a cross join (select a.f1 as g) ss;
-- SQL:2008 says the left table is in scope but illegal to access here
select f1,g from int4_tbl a right join lateral generate_series(0, a.f1) g on true;
select f1,g from int4_tbl a full join lateral generate_series(0, a.f1) g on true;
-- check we complain about ambiguous table references
select * from
  int8_tbl x cross join (int4_tbl x cross join lateral (select x.f1) ss);
-- LATERAL can be used to put an aggregate into the FROM clause of its query
select 1 from tenk1 a, lateral (select max(a.unique1) from int4_tbl b) ss;

-- check behavior of LATERAL in UPDATE/DELETE

create temp table xx1 as select f1 as x1, -f1 as x2 from int4_tbl;

-- error, can't do this:
update xx1 set x2 = f1 from (select * from int4_tbl where f1 = x1) ss;
update xx1 set x2 = f1 from (select * from int4_tbl where f1 = xx1.x1) ss;
-- can't do it even with LATERAL:
update xx1 set x2 = f1 from lateral (select * from int4_tbl where f1 = x1) ss;
-- we might in future allow something like this, but for now it's an error:
update xx1 set x2 = f1 from xx1, lateral (select * from int4_tbl where f1 = x1) ss;

-- also errors:
delete from xx1 using (select * from int4_tbl where f1 = x1) ss;
delete from xx1 using (select * from int4_tbl where f1 = xx1.x1) ss;
delete from xx1 using lateral (select * from int4_tbl where f1 = x1) ss;

--
-- test LATERAL reference propagation down a multi-level inheritance hierarchy
-- produced for a multi-level partitioned table hierarchy.
--
create table join_pt1 (a int, b int, c varchar) partition by range(a);
create table join_pt1p1 partition of join_pt1 for values from (0) to (100) partition by range(b);
create table join_pt1p2 partition of join_pt1 for values from (100) to (200);
create table join_pt1p1p1 partition of join_pt1p1 for values from (0) to (100);
insert into join_pt1 values (1, 1, 'x'), (101, 101, 'y');
create table join_ut1 (a int, b int, c varchar);
insert into join_ut1 values (101, 101, 'y'), (2, 2, 'z');
explain (verbose, costs off)
select t1.b, ss.phv from join_ut1 t1 left join lateral
              (select t2.a as t2a, t3.a t3a, least(t1.a, t2.a, t3.a) phv
					  from join_pt1 t2 join join_ut1 t3 on t2.a = t3.b) ss
              on t1.a = ss.t2a order by t1.a;
select t1.b, ss.phv from join_ut1 t1 left join lateral
              (select t2.a as t2a, t3.a t3a, least(t1.a, t2.a, t3.a) phv
					  from join_pt1 t2 join join_ut1 t3 on t2.a = t3.b) ss
              on t1.a = ss.t2a order by t1.a;

drop table join_pt1;
drop table join_ut1;

--
-- test estimation behavior with multi-column foreign key and constant qual
--

begin;

create table fkest (x integer, x10 integer, x10b integer, x100 integer);
insert into fkest select x, x/10, x/10, x/100 from generate_series(1,1000) x;
create unique index on fkest(x, x10, x100);
analyze fkest;

explain (costs off)
select * from fkest f1
  join fkest f2 on (f1.x = f2.x and f1.x10 = f2.x10b and f1.x100 = f2.x100)
  join fkest f3 on f1.x = f3.x
  where f1.x100 = 2;

alter table fkest add constraint fk
  foreign key (x, x10b, x100) references fkest (x, x10, x100);

explain (costs off)
select * from fkest f1
  join fkest f2 on (f1.x = f2.x and f1.x10 = f2.x10b and f1.x100 = f2.x100)
  join fkest f3 on f1.x = f3.x
  where f1.x100 = 2;

rollback;

--
-- test that foreign key join estimation performs sanely for outer joins
--

begin;

create table fkest (a int, b int, c int unique, primary key(a,b));
create table fkest1 (a int, b int, primary key(a,b));

insert into fkest select x/10, x%10, x from generate_series(1,1000) x;
insert into fkest1 select x/10, x%10 from generate_series(1,1000) x;

alter table fkest1
  add constraint fkest1_a_b_fkey foreign key (a,b) references fkest;

analyze fkest;
analyze fkest1;

explain (costs off)
select *
from fkest f
  left join fkest1 f1 on f.a = f1.a and f.b = f1.b
  left join fkest1 f2 on f.a = f2.a and f.b = f2.b
  left join fkest1 f3 on f.a = f3.a and f.b = f3.b
where f.c = 1;

rollback;

--
-- test planner's ability to mark joins as unique
--

create table j1 (id int primary key);
create table j2 (id int primary key);
create table j3 (id int);

insert into j1 values(1),(2),(3);
insert into j2 values(1),(2),(3);
insert into j3 values(1),(1);

analyze j1;
analyze j2;
analyze j3;

-- ensure join is properly marked as unique
explain (verbose, costs off)
select * from j1 inner join j2 on j1.id = j2.id;

-- ensure join is not unique when not an equi-join
explain (verbose, costs off)
select * from j1 inner join j2 on j1.id > j2.id;

-- ensure non-unique rel is not chosen as inner
explain (verbose, costs off)
select * from j1 inner join j3 on j1.id = j3.id;

-- ensure left join is marked as unique
explain (verbose, costs off)
select * from j1 left join j2 on j1.id = j2.id;

-- ensure right join is marked as unique
explain (verbose, costs off)
select * from j1 right join j2 on j1.id = j2.id;

-- ensure full join is marked as unique
explain (verbose, costs off)
select * from j1 full join j2 on j1.id = j2.id;

-- a clauseless (cross) join can't be unique
explain (verbose, costs off)
select * from j1 cross join j2;

-- ensure a natural join is marked as unique
explain (verbose, costs off)
select * from j1 natural join j2;

-- ensure a distinct clause allows the inner to become unique
explain (verbose, costs off)
select * from j1
inner join (select distinct id from j3) j3 on j1.id = j3.id;

-- ensure group by clause allows the inner to become unique
explain (verbose, costs off)
select * from j1
inner join (select id from j3 group by id) j3 on j1.id = j3.id;

drop table j1;
drop table j2;
drop table j3;

-- test more complex permutations of unique joins

create table j1 (id1 int, id2 int, primary key(id1,id2));
create table j2 (id1 int, id2 int, primary key(id1,id2));
create table j3 (id1 int, id2 int, primary key(id1,id2));

insert into j1 values(1,1),(1,2);
insert into j2 values(1,1);
insert into j3 values(1,1);

analyze j1;
analyze j2;
analyze j3;

-- ensure there's no unique join when not all columns which are part of the
-- unique index are seen in the join clause
explain (verbose, costs off)
select * from j1
inner join j2 on j1.id1 = j2.id1;

-- ensure proper unique detection with multiple join quals
explain (verbose, costs off)
select * from j1
inner join j2 on j1.id1 = j2.id1 and j1.id2 = j2.id2;

-- ensure we don't detect the join to be unique when quals are not part of the
-- join condition
explain (verbose, costs off)
select * from j1
inner join j2 on j1.id1 = j2.id1 where j1.id2 = 1;

-- as above, but for left joins.
explain (verbose, costs off)
select * from j1
left join j2 on j1.id1 = j2.id1 where j1.id2 = 1;

create unique index j1_id2_idx on j1(id2) where id2 is not null;

-- ensure we don't use a partial unique index as unique proofs
explain (verbose, costs off)
select * from j1
inner join j2 on j1.id2 = j2.id2;

drop index j1_id2_idx;

-- validate logic in merge joins which skips mark and restore.
-- it should only do this if all quals which were used to detect the unique
-- are present as join quals, and not plain quals.
set enable_nestloop to 0;
set enable_hashjoin to 0;
set enable_sort to 0;

-- create indexes that will be preferred over the PKs to perform the join
create index j1_id1_idx on j1 (id1) where id1 % 1000 = 1;
create index j2_id1_idx on j2 (id1) where id1 % 1000 = 1;

-- need an additional row in j2, if we want j2_id1_idx to be preferred
insert into j2 values(1,2);
analyze j2;

explain (costs off) select * from j1
inner join j2 on j1.id1 = j2.id1 and j1.id2 = j2.id2
where j1.id1 % 1000 = 1 and j2.id1 % 1000 = 1;

select * from j1
inner join j2 on j1.id1 = j2.id1 and j1.id2 = j2.id2
where j1.id1 % 1000 = 1 and j2.id1 % 1000 = 1;

-- Exercise array keys mark/restore B-Tree code
explain (costs off) select * from j1
inner join j2 on j1.id1 = j2.id1 and j1.id2 = j2.id2
where j1.id1 % 1000 = 1 and j2.id1 % 1000 = 1 and j2.id1 = any (array[1]);

select * from j1
inner join j2 on j1.id1 = j2.id1 and j1.id2 = j2.id2
where j1.id1 % 1000 = 1 and j2.id1 % 1000 = 1 and j2.id1 = any (array[1]);

-- Exercise array keys "find extreme element" B-Tree code
explain (costs off) select * from j1
inner join j2 on j1.id1 = j2.id1 and j1.id2 = j2.id2
where j1.id1 % 1000 = 1 and j2.id1 % 1000 = 1 and j2.id1 >= any (array[1,5]);

select * from j1
inner join j2 on j1.id1 = j2.id1 and j1.id2 = j2.id2
where j1.id1 % 1000 = 1 and j2.id1 % 1000 = 1 and j2.id1 >= any (array[1,5]);

reset enable_nestloop;
reset enable_hashjoin;
reset enable_sort;

drop table j1;
drop table j2;
drop table j3;

-- check that semijoin inner is not seen as unique for a portion of the outerrel
explain (verbose, costs off)
select t1.unique1, t2.hundred
from onek t1, tenk1 t2
where exists (select 1 from tenk1 t3
              where t3.thousand = t1.unique1 and t3.tenthous = t2.hundred)
      and t1.unique1 < 1;

-- ... unless it actually is unique
create table j3 as select unique1, tenthous from onek;
vacuum analyze j3;
create unique index on j3(unique1, tenthous);

explain (verbose, costs off)
select t1.unique1, t2.hundred
from onek t1, tenk1 t2
where exists (select 1 from j3
              where j3.unique1 = t1.unique1 and j3.tenthous = t2.hundred)
      and t1.unique1 < 1;

drop table j3;

-- Exercise the "skip fetch" Bitmap Heap Scan optimization when candidate
-- tuples are discarded. This may occur when:
--   1. A join doesn't require all inner tuples to be scanned for each outer
--      tuple, and
--   2. The inner side is scanned using a bitmap heap scan, and
--   3. The bitmap heap scan is eligible for the "skip fetch" optimization.
--      This optimization is usable when no data from the underlying table is
--      needed. Use a temp table so it is only visible to this backend and
--      vacuum may reliably mark all blocks in the table all visible in the
--      visibility map.
CREATE TEMP TABLE skip_fetch (a INT, b INT) WITH (fillfactor=10);
INSERT INTO skip_fetch SELECT i % 3, i FROM generate_series(0,30) i;
CREATE INDEX ON skip_fetch(a);
VACUUM (ANALYZE) skip_fetch;

SET enable_indexonlyscan = off;
SET enable_seqscan = off;
EXPLAIN (COSTS OFF)
SELECT t1.a FROM skip_fetch t1 LEFT JOIN skip_fetch t2 ON t2.a = 1 WHERE t2.a IS NULL;
SELECT t1.a FROM skip_fetch t1 LEFT JOIN skip_fetch t2 ON t2.a = 1 WHERE t2.a IS NULL;

RESET enable_indexonlyscan;
RESET enable_seqscan;
-- Test BitmapHeapScan with a rescan releases resources correctly
SET enable_seqscan = off;
SET enable_indexscan = off;

CREATE TEMP TABLE rescan_bhs (a INT);
INSERT INTO rescan_bhs VALUES (1), (2);
CREATE INDEX ON rescan_bhs (a);

EXPLAIN (COSTS OFF)
SELECT * FROM rescan_bhs t1 LEFT JOIN rescan_bhs t2 ON t1.a IN
  (SELECT a FROM rescan_bhs t3 WHERE t2.a > 1);
SELECT * FROM rescan_bhs t1 LEFT JOIN rescan_bhs t2 ON t1.a IN
  (SELECT a FROM rescan_bhs t3 WHERE t2.a > 1);

RESET enable_seqscan;
RESET enable_indexscan;

-- Test that we do not account for nullingrels when looking up statistics
CREATE TABLE group_tbl (a INT, b INT);
INSERT INTO group_tbl SELECT 1, 1;
CREATE STATISTICS group_tbl_stat (ndistinct) ON a, b FROM group_tbl;
ANALYZE group_tbl;

EXPLAIN (COSTS OFF)
SELECT 1 FROM group_tbl t1
    LEFT JOIN (SELECT a c1, COALESCE(a) c2 FROM group_tbl t2) s ON TRUE
GROUP BY s.c1, s.c2;

DROP TABLE group_tbl;

-- Test that we ignore PlaceHolderVars when looking up statistics
EXPLAIN (COSTS OFF)
SELECT t1.unique1 FROM tenk1 t1 LEFT JOIN
  (SELECT *, 42 AS phv FROM tenk1 t2) ss ON t1.unique2 = ss.unique2
WHERE ss.unique1 = ss.phv AND t1.unique1 < 100;

SELECT t1.unique1 FROM tenk1 t1 LEFT JOIN
  (SELECT *, 42 AS phv FROM tenk1 t2) ss ON t1.unique2 = ss.unique2
WHERE ss.unique1 = ss.phv AND t1.unique1 < 100;

--
-- Test for a nested loop join involving index scan, transforming OR-clauses
-- to SAOP.
--

EXPLAIN (COSTS OFF)
SELECT COUNT(*) FROM tenk1 t1, tenk1 t2
WHERE t2.thousand = t1.tenthous OR t2.thousand = t1.unique1 OR t2.thousand = t1.unique2;
SELECT COUNT(*) FROM tenk1 t1, tenk1 t2
WHERE t2.thousand = t1.tenthous OR t2.thousand = t1.unique1 OR t2.thousand = t1.unique2;

EXPLAIN (COSTS OFF)
SELECT COUNT(*) FROM onek t1 LEFT JOIN tenk1 t2
    ON (t2.thousand = t1.tenthous OR t2.thousand = t1.thousand);
SELECT COUNT(*) FROM onek t1 LEFT JOIN tenk1 t2
    ON (t2.thousand = t1.tenthous OR t2.thousand = t1.thousand);
