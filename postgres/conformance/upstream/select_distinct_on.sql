--
-- SELECT_DISTINCT_ON
--

SELECT DISTINCT ON (string4) string4, two, ten
   FROM onek
   ORDER BY string4 using <, two using >, ten using <;

-- this will fail due to conflict of ordering requirements
SELECT DISTINCT ON (string4, ten) string4, two, ten
   FROM onek
   ORDER BY string4 using <, two using <, ten using <;

SELECT DISTINCT ON (string4, ten) string4, ten, two
   FROM onek
   ORDER BY string4 using <, ten using >, two using <;

-- bug #5049: early 8.4.x chokes on volatile DISTINCT ON clauses
select distinct on (1) floor(random()) as r, f1 from int4_tbl order by 1,2;

--
-- Test the planner's ability to use a LIMIT 1 instead of a Unique node when
-- all of the distinct_pathkeys have been marked as redundant
--

-- Ensure we also get a LIMIT plan with DISTINCT ON
EXPLAIN (COSTS OFF)
SELECT DISTINCT ON (four) four,two
   FROM tenk1 WHERE four = 0 ORDER BY 1;

-- and check the result of the above query is correct
SELECT DISTINCT ON (four) four,two
   FROM tenk1 WHERE four = 0 ORDER BY 1;

-- Ensure a Sort -> Limit is used when the ORDER BY contains additional cols
EXPLAIN (COSTS OFF)
SELECT DISTINCT ON (four) four,two
   FROM tenk1 WHERE four = 0 ORDER BY 1,2;

-- Same again but use a column that is indexed so that we get an index scan
-- then a limit
EXPLAIN (COSTS OFF)
SELECT DISTINCT ON (four) four,hundred
   FROM tenk1 WHERE four = 0 ORDER BY 1,2;

--
-- Test the planner's ability to reorder the distinctClause Pathkeys to match
-- the input path's ordering
--

CREATE TABLE distinct_on_tbl (x int, y int, z int);
INSERT INTO distinct_on_tbl SELECT i%10, i%10, i%10 FROM generate_series(1, 1000) AS i;
CREATE INDEX distinct_on_tbl_x_y_idx ON distinct_on_tbl (x, y);
ANALYZE distinct_on_tbl;

-- Produce results with sorting.
SET enable_hashagg TO OFF;

-- Ensure we avoid the need to re-sort by reordering the distinctClause
-- Pathkeys to match the ordering of the input path
EXPLAIN (COSTS OFF)
SELECT DISTINCT ON (y, x) x, y FROM distinct_on_tbl;
SELECT DISTINCT ON (y, x) x, y FROM distinct_on_tbl;

-- Ensure we leverage incremental-sort by reordering the distinctClause
-- Pathkeys to partially match the ordering of the input path
EXPLAIN (COSTS OFF)
SELECT DISTINCT ON (y, x) x, y FROM (SELECT * FROM distinct_on_tbl ORDER BY x) s;
SELECT DISTINCT ON (y, x) x, y FROM (SELECT * FROM distinct_on_tbl ORDER BY x) s;

-- Ensure we reorder the distinctClause Pathkeys to match the ordering of the
-- input path even if there is ORDER BY clause
EXPLAIN (COSTS OFF)
SELECT DISTINCT ON (y, x) x, y FROM distinct_on_tbl ORDER BY y;
SELECT DISTINCT ON (y, x) x, y FROM distinct_on_tbl ORDER BY y;

-- Ensure the resulting pathkey list matches the initial distinctClause Pathkeys
EXPLAIN (COSTS OFF)
SELECT DISTINCT ON (y, x) x, y FROM (select * from distinct_on_tbl order by x, z, y) s ORDER BY y, x, z;
SELECT DISTINCT ON (y, x) x, y FROM (select * from distinct_on_tbl order by x, z, y) s ORDER BY y, x, z;

RESET enable_hashagg;

DROP TABLE distinct_on_tbl;
