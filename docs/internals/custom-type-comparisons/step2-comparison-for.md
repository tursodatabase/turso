# Step 2: comparison_for

Detailed design of step 2 in design.md section 8, after two hostile
reviews. File:line references are to main at 6a8ccc1d3c. Where this
file and design.md disagree about which step does what, this file is
newer.

## 1. What the step is

One entry point, `comparison_for`, decides how two or more expressions are compared. Every site that today calls comparison_collation, comparison_collation_across_queries, in_subquery_collation, in_list_collation, expr_collation, first_collation or compound_column_collation goes through it instead. Comparators that come from a column's declaration (`Column::collation()`, `key_collation`, direct `type_ordering` calls) do not move in this step; they move in step 3. Until then, `Constraint.collation == IndexColumn.collation` compares a value from `comparison_for` with one from the declaration path.

There is no behavior change. Results, error messages and bytecode stay identical. Insn, KeyInfo, IndexColumn, DistinctCtx, MinMaxDef and OrderTarget still carry `CollationSeq`.

Proof of no change (requirement R14: same results and same bytecode). EXPLAIN is not enough: it prints no collation for HashBuild (explain.rs:2692-2706), HashDistinct (2749) or OpenEphemeral (2370), and no index-column collations. Those are the hash join, compound, IN-subquery, recursive CTE and DISTINCT sites. So the proof has two parts:
1. **Program dump diff (merged, reused by steps 3-8).** A debug-only environment variable makes translation, not `prepare`, so cache hits are dumped too, write `format!("{:#?}", (&program.insns, &program.cursor_ref))` with addresses masked. `Insn` and `Index` derive Debug (builder.rs:291/305), so every CollationSeq the VM or an ephemeral index will use is in the text.
   - Each dump is keyed by a hash of all statement texts prepared so far on that connection. That keeps statements that depend on earlier ones apart, and doesn't depend on thread order or test names.
   - Every test file runs for real: sqlite/conformance (sqlite-sqltests, turso-sqltests, upstream), postgres/conformance and TCL. Each process writes one file, and the files are merged by key.
   - Baseline: main plus only the dump commit, run twice. Every key whose two baseline dumps differ (random data, stats, root pages) is masked.
   - A key present on one side only is a failure. The PR reports the statement, masked and unmatched counts.
2. **One-off agreement run (not merged).** One extra commit keeps the old functions and `assert_eq!`s old against new inside each builder. It runs over `cargo test`, both conformance suites, the differential fuzzer and the custom-types fuzzer. That catches per-call mismatches that happen to give the same program. The commit is then dropped, and the PR states the result.

## 2. Where design.md is wrong or underspecified for step 2

D1. design.md section 3 gives `comparison_for(sides: &[&ExprType], need, ctx)`. Types alone cannot reproduce the text collation rules. In `sqlite3BinaryCompareCollSeq`, a side with a COLLATE operator wins, left before right. Otherwise a column with a declared collation wins. So the result depends on whether a side has a COLLATE operator and on whether it is a column, and ExprType records neither. A Side must therefore carry its type plus a collation source: the expression and its ExprContext.

D2. There are three text-collation combining rules, and they give different answers, so `comparison_for` needs a rule input:
- Comparison: `comparison_collation` (collate.rs:491/502); SQLite's BinaryCompareCollSeq. SQLite also uses it for the subquery KeyInfo (expr.c:3656).
- LeftOnly: `in_list_collation` (collate.rs:558); SQLite expr.c:4014.
- FirstThatHasOne: `first_collation` / `compound_column_collation` (collate.rs:602/617); SQLite's multiSelectCollSeq and the functions flagged NEEDCOLL.

They differ on `min(col_nocase, x COLLATE binary)`: FirstThatHasOne gives NOCASE, while Comparison gives BINARY.

D3. `Comparison.conversions` / `Conversion`: nothing produces a conversion until step 7, so the field would be unused code. It is left out of step 2.

D4. Affinity stays out of step 2, because the sites don't share one rule today:
- comparison_affinity(lhs, rhs): binary.rs:410/462/755, translator.rs:674, hash.rs:100;
- get_expr_affinity(lhs): constraints.rs:1103;
- in_expr_affinity: condition.rs:66;
- term_comparison_affinity: constraints.rs:2245;
- comparison_affinity with two contexts: subquery.rs:1048.

The key, first and compound builders have no affinity at all. Step 7 adds `Comparison.affinity` with the "None for custom types" rule, and unifies these sites in that step.

D5. Only the capability checks (step 3) read `Need`, so it arrives in step 3: no field without a reader. Section 6 records the per-site classification for step 3. Step 3 adds one argument per builder call.

D6. `key_collation` (collate.rs:704) gives a column's comparator from its declaration (index.rs:1070/1091, schema.rs:5298/5302, alter.rs:1285/2023), not from a comparison of expressions. It moves in step 3, with the type traits and the COLLATE rule.

D7. "Today's rules" exist twice, and the two copies disagree:
- result types: expr_type.rs common_of_types (961/965), used by CASE, COALESCE and compound;
- comparisons: collate.rs common_ordering (665).

For example, numeric vs uuid gives Any as a result type but compares as Numeric. numeric vs int[] gives the array as a result type, but the comparison errors.

So step 2 adds no Dialect method. The pairwise fold is a private function in comparison.rs, `fold_comparison_type`, named so it can't be confused with expr_type.rs:961 `common_type`. It stays keyed on today's TypeOrdering. Step 3 rekeys it on the type traits. Step 8 writes the single rule for pairs and result types, and adds `Dialect::common_type`, together with its first override in PostgresDialect.

D8. in_subquery_collation (collate.rs:532) has two phases. Phase 1 folds `[lhs ordering, query column type ordering]`. If that gives no ordering, phase 2 falls back to comparison_collation_across_queries(lhs, rightmost arm expression). That is not SQLite's rule. Step 2 keeps it as is (no behavior change); step 8 replaces it.

D9. common_ordering is n-ary, and ArrayOfUnknownElements (AU) adapts only after it sees the other orderings. A plain left-to-right pairwise fold would change the error message for [AU, Numeric, TextArray]; today that message is "cannot compare type numeric with type text_array". The fold therefore moves AU sides to the end, keeping the order of the others.

D10. The hash join decides its comparison three times: access_method.rs:1286 (can it hash), constraints.rs (the term), and hash.rs:123 (again at emit time, from the expressions). Step 2 decides it once, in `tables_in_hashable_equal_test`, which is called per term from build_where_term_info (join.rs:2300). That function returns the `ComparatorHandle` alongside (left, right), and it is stored on `HashJoinKey` (plan.rs:2892). Both are Copy. hash.rs:123 reads it, and hash.rs:132 needs only `is_binary()`. Step 7, which adds conversions, decides what the key holds then.

D11. translator.rs:476 is the null-check `Ne` of a row-value NOT IN subquery. Today it uses `expr_collation(lhs)`, the left side alone. `SubqueryType::In` has no column type at that site (translator.rs:361-364). Step 2 uses `key_comparison(lhs)`, which is exactly today's result. That it ignores the subquery column, while the `Found`/`NotFound` above it use the ephemeral index's collations (subquery.rs:1063), is checked against SQLite in step 8.

D12. The IN-subquery phase 1 must keep the COLLATE check on the subquery side. Today collate.rs:547 sets has_collate from both lhs and the arm expression. So `n IN (SELECT i COLLATE nocase FROM t)` with n numeric(10,2) errors "collations are not supported by type numeric". The subquery side is therefore `Side::of_typed(query_column_type, arm_expr, ctx)`: its type comes from the query column, and its collation source is the arm expression. `CollationSource::None` is removed.

## 3. Types and signatures

New module core/translate/comparison.rs:

```rust
/// The runtime comparator. Every site still turns it into a CollationSeq
/// right away (Insn, KeyInfo, IndexColumn, ... keep CollationSeq until
/// step 7); it exists so that later steps change one type, not 50 sites.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ComparatorHandle(CollationSeq);
impl ComparatorHandle {
    pub(crate) fn collation_seq(self) -> CollationSeq;
    pub(crate) fn can_hash(self) -> bool;     // CollationSeq::can_hash (collate.rs:149)
    pub(crate) fn is_binary(self) -> bool;    // select.rs:1867, hash.rs:132
    pub(crate) fn is_custom(self) -> bool;    // aggregation.rs:867-873, plan.rs:672-676
    pub(crate) fn name(self) -> String;       // CollationSeq::name (collate.rs:153); error text
}

/// What comparison_for decided. In step 2 it holds only the comparator;
/// step 3 adds Need-dependent checks, step 7 conversions and affinity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Comparison {
    pub(crate) comparator: ComparatorHandle,
}

pub(crate) struct Side<'e, 'c, 's> { ty: ExprType, expr: &'e Expr, context: ExprContext<'c, 's> }
impl Side<'_, '_, '_> {
    pub(crate) fn of(expr, context) -> Self; // type_of is infallible (expr_type.rs:207)
    /// The query-column side of IN (subquery): the type of the query
    /// column, the COLLATE check of its arm expression (D12).
    pub(crate) fn of_typed(ty: ExprType, expr, context) -> Self;
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum CollationRule { Comparison, LeftOnly, FirstThatHasOne }

pub(crate) fn comparison_for(sides: &[Side], rule: CollationRule, schema: &Schema) -> Result<Comparison>;
```

The lifetimes follow first_collation's (collate.rs:602); the implementer checks that the compound and recursive-CTE sites compile.

comparison_for works in three steps:
1. It folds the side types with `fold_comparison_type`, with AU sides last (D9). The fold works on references and returns the index of the winning side, so it never clones an ExprType (`Custom.params` holds expressions, expr_type.rs:26-31).
2. If the folded type has an ordering: when any side has a COLLATE operator, it returns Err("collations are not supported by type X"), the same text as today. Otherwise it returns `ComparatorHandle(CollationSeq::Type(ordering))`.
3. Otherwise it resolves the text collation by `rule`, using expr_collation_if_any (collate.rs:761) and has_collate_operator (collate.rs:812). It resolves lazily and stops at the first side that decides the result, as first_text_collation (collate.rs:633) does today. Resolving every side up front would add "no such collation" errors. Type errors therefore come before collation errors.
   - Comparison: the first side with a COLLATE operator; else the first side with a collation; else BINARY.
   - LeftOnly: the collation of sides[0], else BINARY.
   - FirstThatHasOne: the first side with a collation, else BINARY.

`fold_comparison_type` is today's rule, one pair at a time. "Ordering" means type_ordering (expr_type.rs:594). `acc` is the result so far, and `next` is the next side. AU sides come last, so an AU side is only ever `next`, or `acc` when every side so far was AU. The rows are checked in order and the first match wins. The AU rows come first because `type_ordering(AU)` is `Array` (expr_type.rs:597-607).

| # | acc | next | result |
|---|---|---|---|
| 1 | Array or TextArray | AU | acc |
| 2 | another ordering X | AU | cannot_compare(X, Array) |
| 3 | no ordering | AU | AU |
| 4 | AU | AU | AU |
| 5 | ordering X | ordering X | acc |
| 6 | ordering X | ordering Y, Y ≠ X | cannot_compare(X, Y) (collate.rs:681/718) |
| 7 | one has an ordering | the other has none | the side with the ordering |
| 8 | no ordering | no ordering | no ordering; a text collation is resolved |

A result of AU means ordering Array. A COLLATE operator then errors with "collations are not supported by type array", as collate.rs:685-686 does today.

Rule detail to keep from collate.rs:512-514: under the Comparison rule, the COLLATE branch takes `expr_collation_if_any` of the first side that has a COLLATE operator. If that gives None, the result is BINARY; it does not fall through to the other side.

ExprType stays pub(crate); nothing outside core needs it in step 2.

## 4. Builders

All builders live in comparison.rs. The old functions are deleted.

| builder | replaces | rule | sides |
|---|---|---|---|
| `pair_comparison(lhs, rhs, ctx_l, ctx_r)` | comparison_collation / _across_queries (collate.rs:491/502) | Comparison | [lhs, rhs] |
| `in_list_comparison(lhs, items, ctx)` | in_list_collation (558) | LeftOnly | [lhs, items...] |
| `in_subquery_comparison(lhs, lhs_tables, plan, column, resolver)` | in_subquery_collation (532); takes `&Plan` as today, so the caller doesn't recompute the column type and arm (collate.rs:539-545) | phase 1: [lhs, of_typed(column_type, arm_expr)] (D12); phase 2 when there is no ordering: pair_comparison(lhs, arm_expr) (D8) | |
| `key_comparison(expr, ctx)`, plus `ExprContext::key_comparison` replacing expr_context.rs:48 | expr_collation (575) | FirstThatHasOne | [expr] |
| `first_comparison(sides: impl IntoIterator<Item = (&Expr, ExprContext)>)` | first_collation (602) | FirstThatHasOne | one context per side (recursive_cte.rs:291-294, vdbe_maintenance/plan.rs:650-654) |
| `compound_comparison(arms: impl IntoIterator<Item = (&Expr, ExprContext)>)` | compound_column_collation (617) | FirstThatHasOne | one context per arm (collate.rs:622) |

Sites that decide lazily keep doing so: binary.rs:465/758 call the builder only for comparison operators, inside the existing closure, never for `+`. constraints.rs:844 `decide_once` caches a `ComparatorHandle`.

constraints.rs:581-582 and unnest.rs:1149-1150 decide "do both sides compare the same" with two key calls and `==`. That is today's result; a pair call would differ when only one side has COLLATE. They stay as two `key_comparison` calls. In step 3 they get one named helper.

What happens to collate.rs:
- These are deleted, and their logic moves into comparison.rs: comparison_collation, comparison_collation_across_queries, in_subquery_collation, in_list_collation, expr_collation, first_collation, compound_column_collation, first_text_collation (633), common_type_ordering (648), common_ordering (665), and OperandOrder / expr_operand_order / operand_order (728-743).
- These stay: CollationSeq and its methods, expr_collation_if_any, column_collation (806), has_collate_operator, for_each_operand (827), cannot_compare, key_collation (D6), and expr_text_collation (586), which plan.rs:3214 uses.
- The collate.rs unit tests (from line 912) are rewritten against the builders, with the same inputs and the same expected results.

## 5. Call sites

- Pair (pair_comparison):
  - binary.rs:411, 465, 758;
  - translator.rs:676 (CASE x WHEN);
  - constraints.rs:845, 2238 and 2315 (both keep `.ok()?`);
  - constraints.rs:2515 (compare `.comparator.collation_seq()`);
  - the hash join (D10): decided once in tables_in_hashable_equal_test (`can_hash()`), stored on HashJoinKey, and read at main_loop/hash.rs:123 and 132 (`is_binary()`).
- IN subquery: subquery.rs:1063. constraints.rs:1400 stays, because it reads a stored IndexColumn.
- IN list: condition.rs:68; constraints.rs:1108.
- Key (key_comparison):
  - group_by.rs:175, 701;
  - aggregation.rs:619, 639, 867;
  - window.rs:649, 933, 2012, 2580 (matches `CollationSeq::Type(ordering)` from `comparator.collation_seq()` for the error text at 2581-2587, as today);
  - translator.rs:476 (D11);
  - order_by.rs:129, 627;
  - select.rs:1867;
  - main_loop/init.rs:15, 91;
  - optimizer/order.rs:102;
  - optimizer/mod.rs:934;
  - unnest.rs:1149-1150;
  - constraints.rs:581-582;
  - incremental/vdbe_maintenance/plan.rs:634, 672.
- First (first_comparison): translator.rs:945; recursive_cte.rs:297; incremental/vdbe_maintenance/plan.rs:650.
- Compound (compound_comparison): compound_select.rs:830.
- Not changed in step 2:
  - plan.rs:3214 and 3225 (metadata);
  - the key_collation callers;
  - schema.rs:8178, 8234 and 4209 (declarations);
  - `Column::collation()` readers (constraints.rs:243, order.rs:555/663/962, access_method.rs:2133/2151, optimizer/mod.rs:4334, index.rs:1071, incremental/view.rs:111);
  - direct `type_ordering` calls (index.rs:1154, schema.rs:7770);
  - custom_type_operators.rs.

  All of these come from declarations and move in step 3.

## 6. Need classification (for step 3; not a parameter in step 2)

- Equality:
  - `=`, `<>`, IS, IS NOT;
  - IN list and IN subquery;
  - hash joins;
  - GROUP BY, DISTINCT, PARTITION BY;
  - unnest.rs:1149;
  - equivalence classes;
  - NOT IN;
  - UNION, INTERSECT, EXCEPT.
- Order:
  - `<`, `<=`, `>`, `>=`, BETWEEN;
  - ORDER BY;
  - min and max;
  - RANGE offsets;
  - range terms in constraints.rs;
  - recursive CTE ORDER BY.
- Depends on the operator or function:
  - CASE x WHEN is Equality.
  - The NEEDCOLL functions are min, max and nullif (function.rs:1948-1953; SQLite func.c:2835, 2875). nullif is Equality; min and max with several arguments are Order.
  - A compound with ORDER BY is Order.

## 7. Tests

Step 2 is a pure refactor, so no sqltest fails on main. It adds these unit tests in comparison.rs:
- every row of the fold table, in order, including error messages. Cases: numeric vs uuid gives Numeric; numeric vs int[] errors; AU vs text_array gives text_array; AU vs numeric gives "cannot compare type numeric with type array"; a lone AU with COLLATE gives "collations are not supported by type array";
- the fold that moves AU sides last:
  - [AU, Numeric, TextArray] gives "cannot compare type numeric with type text_array";
  - [0, Numeric, AU] gives "cannot compare type numeric with type array";
  - (TextArray, AU), (Array, AU), (AU, AU), [AU, TextArray] and [TextArray, 0, AU] succeed, as `ts = ARRAY[u]` and `ARRAY[u] = ts` do today;
- the IN-subquery COLLATE check (D12): `n IN (SELECT i COLLATE nocase FROM t)` and `i IN (SELECT n COLLATE nocase FROM t)`, with n numeric(10,2) and i integer, both error "collations are not supported by type numeric";
- the three CollationRules on `min(col_nocase, x COLLATE binary)`-shaped input;
- error precedence: a type mismatch wins over an unknown collation, and an unknown collation on a side that doesn't decide the result gives no error;
- the collate.rs tests, rewritten.

The program dump diff (section 1) over the whole corpus must have no unmasked difference and no unmatched key. The one-off agreement run must report no mismatch. The PR states both results with their counts.

## 8. Order of work

1. Add the program dump (first commit). Run baseline against baseline twice to build the mask.
2. Add comparison.rs: the types, comparison_for and the builders.
3. Change the call sites one group at a time (pair, IN, key, first, compound, hash join), deleting each old function as its last caller goes.
4. Rewrite and add the unit tests.
5. Run the dump diff, cargo test and both conformance suites. On a throwaway commit, run the agreement check and record the result.

## 9. Decisions

1. `Need` arrives in step 3, with its first reader.
2. ExprType stays pub(crate). `Dialect::common_type` arrives in step 8, with its first override.
3. Constraint.collation stays a CollationSeq until step 7.
4. No oracle is merged. The program dump diff against main (merged, and reused by steps 3-8 for plain SQLite comparisons) plus the one-off agreement run prove no behavior change.
5. Affinity and the folded type are not stored on `Comparison` in step 2: neither has a single value or a reader yet (D4).
6. The two-phase in-subquery rule stays until step 8 (no behavior change in step 2).
7. Row-value IN lists use the first component's collation for every component. This needs checking against SQLite source (expr.c, sqlite3ExprCodeIN / exprINAffinity). If it is a bug, it is a separate SQLite-compatibility fix and not part of these steps.
8. key_collation and Column::collation move in step 3.
9. Type errors come before collation errors. That order is fixed now.
