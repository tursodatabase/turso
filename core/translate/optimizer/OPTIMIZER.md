# Overview of the current state of the query optimizer in Limbo

Query optimization is obviously an important part of any SQL-based database engine. This document is an overview of what we currently do.

## Structure of the optimizer directory

1. `mod.rs`
   - Provides the high-level optimization interface through `optimize_plan()`

2. `access_method.rs`
   - Determines what is the best index to use when joining a table to a set of preceding tables

3. `constraints.rs` - Manages query constraints:
   - Extracts constraints from the WHERE clause
   - Determines which constraints are usable with indexes

4. `cost.rs`
   - Calculates the cost of doing a seek vs a scan, for example

5. `join.rs`
   - Implements the System R style dynamic programming join ordering algorithm

6. `order.rs`
   - Determines if sort operations can be eliminated based on the chosen access methods and join order

7. `unnest.rs`
   - Changes a correlated subquery into a join when both forms give the same rows
   - Leaves all other forms as subqueries

## Join reordering and optimal index selection

**The goals of query optimization are at least the following:**

1. Do as little page I/O as possible
2. Do as little CPU work as possible
3. Retain query correctness.

**The most important ways to achieve no. 1 and no. 2 are:**

1. Choose the optimal access method for each table (e.g. an index or a rowid-based seek, or a full table scan if all else fails).
2. Choose the best or near-best way to reorder the tables in the query so that those optimal access methods can be used.
3. Also factor in whether the chosen join order and indexes allow removal of any sort operations that are necessary for query correctness.

## Limbo's optimizer

Limbo's optimizer is an implementation of an extremely traditional [IBM System R](https://www.cs.cmu.edu/~15721-f24/slides/02-Selinger-SystemR-opt.pdf) style optimizer,
i.e. straight from the 70s! The DP algorithm is explained below.

### Current high level flow of the optimizer

1. **Choose how to run correlated subqueries**
   - Keep the query as written as one choice.
   - Make a second choice by changing supported `EXISTS`, `IN`, and subqueries that return one aggregate value into joins.
   - Plan both choices and keep the one with the lower cost.
   - On equal cost, keep the join because it avoids one subquery call per outer row.
2. **SQL rewriting**
  - Rewrite certain SQL expressions to another form (not a lot currently; e.g. rewrite BETWEEN as two comparisons)
  - Eliminate constant conditions: e.g. `WHERE 1` is removed, `WHERE 0` short-circuits the whole query because it is trivially false.
3. **Check whether there is an "interesting order"** that we should consider when evaluating indexes and join orders
    - Is there a GROUP BY? an ORDER BY? Both?
4. **Convert WHERE clause conjucts to Constraints**
    - E.g. in `WHERE t.x = 5`, the expression `5` _constrains_  table `t` to values of `x` that are exactly `5`.
    - E.g. in `Where t.x = u.x`, the expression `u.x` constrains `t`, AND `t.x` constrains `u`.
    - Per table, each constraint has an estimated _selectivity_ (how much it filters the result set); this affects join order calculations, see the paragraph on _Estimation_  below.
    - Per table, constraints are also analyzed for whether one or multiple of them can be used as an index seek key to avoid a full scan.
5. **Compute the best join order using a dynamic programming algorithm:**
  - `n` = number of tables considered
  - `n=1`: find the lowest _cost_ way to access each single table, given the constraints of the query. Memoize the result.
  - `n=2`: for each table found in the `n=1` step, find the best way to join that table with each other table. Memoize the result.
  - `n=3`: for each 2-table subset found, find the best way to join that result to each other table. Memoize the result.
  - `n=m`: for each `m-1` table subset found, find the best way to join that result to the `m'th` table
  - **Use pruning to reduce search space:**
    - Compute the literal query order first, and store its _cost_ as an upper threshold.
      In some cases it is not possible to compute this upper threshold from the literal order—for example, when 
      table-valued functions are involved and their arguments reference tables that appear to the right in the join order. 
      In such situations, the literal order cannot be executed directly, so no meaningful _cost_ can be assigned. 
      In these cases, the threshold is set to infinity, ensuring that valid plans are still considered.
    - If at any point a considered join order exceeds the upper threshold, discard that search path since it cannot be better than the current best.
      - For example, we have `SELECT * FROM a JOIN b JOIN c JOIN d`. Compute `JOIN(a,b,c,d)` first. If `JOIN (b,a)` is already worse than `JOIN(a,b,c,d)`, we don't have to even try `JOIN(b,a,c)`.
    - Also keep track of the best plan per _subset_:
      - If we find that `JOIN(b,a,c)` is better than any other permutation of the same tables, e.g. `JOIN(a,b,c)`, then we can discard _ALL_ of the other permutations for that subset. For example, we don't need to consider `JOIN(a,b,c,d)` because we know it's worse than `JOIN(b,a,c,d)`.
      - This is possible due to the associativity and commutativity of INNER JOINs.
  - Also keep track of the best _ordered plan_ , i.e. one that provides the "interesting order" mentioned above.
  - At the end, apply a cost penalty to the best overall plan
    - If it is now worse than the best sorted plan, then choose the sorted plan as the best plan for the query.
      - This allows us to eliminate a sorting operation.
    - If the best overall plan is still best even with the sorting penalty, then keep it. A sorting operation is later applied to sort the rows according to the desired order.
6. **Mutate the plan's `join_order` and `Operation`s to match the computed best plan.**

### Estimation of cost and cardinalities + a note on table statistics

Currently, in the absence of `ANALYZE`, `sqlite_stat1` etc. we assume the following:

1. Each table has `1,000,000` rows.
2. Each equality (`=`) filter will filter out some percentage of the result set.
3. Each nonequality (e.g. `>`) will filter out some smaller percentage of the result set.
4. Each `4096` byte database page holds `50` rows, i.e. roughly `80` bytes per row 
5. Sort operations have some CPU cost dependent on the number of input rows to the sort operation.

From the above, we derive the following formula for estimating the cost of joining `t1` with `t2`

```
JOIN_COST = PAGE_IO(t1.rows) + t1.rows * PAGE_IO(t2.rows)
```

For example, let's take the query `SELECT * FROM t1 JOIN t2 USING(foo) WHERE t2.foo > 10`. Let's assume the following:

- `t1` has `6400` rows and `t2` has `8000` rows
- there are no indexes at all
- let's ignore the CPU cost from the equation for simplicity.

The best access method for both is a full table scan. The output cardinality of `t1` is the full table, because nothing is filtering it. Hence, the cost of `t1 JOIN t2` becomes:

```
JOIN_COST = PAGE_IO(t1.input_rows) + t1.output_rows * PAGE_IO(t2.input_rows)

// plugging in the values:

JOIN_COST = PAGE_IO(6400) + 6400 * PAGE_IO(8000)
JOIN_COST = 80 + 6400 * 100 = 640080
```

Now let's consider `t2 JOIN t1`. The best access method for both is still a full scan, but since we can filter on `t2.foo > 10`, its output cardinality decreases. Let's assume only 1/4 of the rows of `t2` match the condition `t2.foo > 10`. Hence, the cost of `t2 join t1` becomes:

```
JOIN_COST = PAGE_IO(t2.input_rows) + t2.output_rows * PAGE_IO(t1.input_rows)

// plugging in the values:

JOIN_COST = PAGE_IO(8000) + 1/4 * 8000 * PAGE_IO(6400)
JOIN_COST = 100 + 2000 * 80 = 160100
```

Even though `t2` is a larger table, because we were able to reduce the input set to the join operation, it's dramatically cheaper.

#### Statistics

Since we don't support `ANALYZE`, nor can we assume that users will call `ANALYZE` anyway, we use simple magic constants to estimate the selectivity of join predicates, row count of tables, and so on. When we have support for `ANALYZE`, we should plug the statistics from `sqlite_stat1` and friends into the optimizer to make more informed decisions.

### Estimating the output cardinality of a join

The output cardinality (output row count) of an operation is as follows:

```
OUTPUT_CARDINALITY_JOIN = INPUT_CARDINALITY_RHS * OUTPUT_CARDINALITY_RHS

where

INPUT_CARDINALITY_RHS = OUTPUT_CARDINALITY_LHS
```

example:

```
SELECT * FROM products p JOIN order_lines o ON p.id = o.product_id
```
Assuming there are 100 products, i.e. just selecting all products would yield 100 rows:

```
OUTPUT_CARDINALITY_LHS = 100
INPUT_CARDINALITY_RHS = 100
```

Assuming p.id = o.product_id will return three orders per each product:

```
OUTPUT_CARDINALITY_RHS = 3

OUTPUT_CARDINALITY_JOIN = 100 * 3 = 300
```
i.e. the join is estimated to return 300 rows, 3 for each product.

Again, in the absence of statistics, we use magic constants to estimate these cardinalities.

Estimating them is important because in multi-way joins the output cardinality of the previous join becomes the input cardinality of the next one.
