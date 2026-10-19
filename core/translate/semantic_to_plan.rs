//! Adapts resolved semantic sources to planner structures.

use super::{
    optimizer::{
        apply_hir_selected_btree_access, apply_hir_selected_virtual_access, base_row_estimate,
        constraints::{hir_constraints_for_source, HirConstraintSource, HirTableConstraints},
        cost::{Cost, RowCountEstimate},
        join::compute_hir_join_order,
        order::{
            hir_plan_satisfies_order_target, ColumnOrder, ColumnTarget, EliminatesSortBy,
            HirOrderTarget, OrderTarget, OrderTargetPurpose,
        },
        CostModelParams, HirBtreeOperation, HirVirtualTableOperation,
    },
    plan::{
        ColumnUsedMask, HirCteMaterialization, HirFromGroupBoundary, HirFromLayout, HirJoinInfo,
        HirPlanSource, HirPlannedSource, HirWhereTerm, PredicateExpr,
    },
    semantic::hir::{self, ColumnUsage, HirDocument, QueryBlockId, QueryId, SourceId},
};
use crate::{
    incremental::view::IncrementalView,
    schema::{Index, Schema, Table},
    sync::{Arc, Mutex},
    LimboError, Result,
};
use rustc_hash::FxHashSet as HashSet;

/// The source an outer join may NULL-extend, when `source` is the right
/// side of one: its seek terms must stay checked on the added row.
fn hir_outer_join_source(
    source: SourceId,
    join_info: Option<&crate::translate::plan::HirJoinInfo>,
) -> Option<SourceId> {
    join_info.and_then(|join| {
        matches!(
            join.kind,
            hir::JoinKind::Left | hir::JoinKind::Right | hir::JoinKind::Full
        )
        .then_some(source)
    })
}

fn hir_block_grouping(block: &hir::QueryBlock) -> Option<&hir::Grouping> {
    match &block.body {
        hir::QueryBlockBody::Select {
            grouping: Some(grouping),
            ..
        } => Some(grouping),
        _ => None,
    }
}

/// One GROUP BY key, in the order the GROUP BY sorter compares keys in.
pub(crate) struct GroupKeyOrder {
    pub(crate) key: usize,
    pub(crate) order: turso_parser::ast::SortOrder,
    pub(crate) nulls: Option<turso_parser::ast::NullsOrder>,
}

/// The order the GROUP BY sorter compares its keys in, when every ORDER BY
/// term is a GROUP BY key with that key's collation: the keys the ORDER BY
/// names, in its order and with its directions, then the others ascending.
/// Groups then leave in ORDER BY order, so no second sort is needed. Like
/// legacy's `compute_order_target`, which reorders its GROUP BY keys the
/// same way. Which rows form a group does not depend on this order.
pub(crate) fn group_key_order(
    document: &HirDocument,
    order_by: &[hir::OrderTerm],
    grouping: &hir::Grouping,
) -> Option<Vec<GroupKeyOrder>> {
    use crate::translate::collate::CollationSeq;
    if order_by.is_empty() {
        return None;
    }
    let collation = |collation: Option<&hir::ResolvedCollation>| match collation
        .map(|collation| *collation.value())
    {
        None | Some(CollationSeq::Unset) => CollationSeq::Binary,
        Some(collation) => collation,
    };
    let mut order: Vec<GroupKeyOrder> = Vec::with_capacity(grouping.keys.len());
    for term in order_by {
        let mut expression = &term.expr;
        while let hir::Expr::Output(output) = expression {
            expression = &document.output(*output)?.expr;
        }
        let key = grouping
            .keys
            .iter()
            .zip(&grouping.key_collations)
            .position(|(key, key_collation)| {
                expression.equivalent(key)
                    && collation(term.collation.as_ref()) == collation(key_collation.as_ref())
            })?;
        if !order.iter().any(|known| known.key == key) {
            order.push(GroupKeyOrder {
                key,
                order: term.order,
                nulls: term.nulls,
            });
        }
    }
    for key in 0..grouping.keys.len() {
        if !order.iter().any(|known| known.key == key) {
            order.push(GroupKeyOrder {
                key,
                order: turso_parser::ast::SortOrder::Asc,
                nulls: None,
            });
        }
    }
    Some(order)
}

/// Whether a query's result is the same whatever order its rows are read
/// in: one ungrouped aggregate block whose aggregates are all count, sum,
/// total, avg, min or max.
fn query_ignores_row_order(query: &hir::Query) -> bool {
    use crate::function::{AggFunc, Func};
    let [block] = query.blocks.as_slice() else {
        return false;
    };
    if block.aggregate_count == 0
        || block.window_function_count != 0
        || !matches!(
            &block.body,
            hir::QueryBlockBody::Select { grouping: None, .. }
        )
    {
        return false;
    }
    let mut ignores_order = true;
    for output in &block.outputs {
        output.expr.for_each(&mut |expression| {
            let hir::Expr::Function(call) = expression else {
                return;
            };
            if !matches!(call.evaluation, hir::FunctionEvaluation::Aggregate { .. }) {
                return;
            }
            ignores_order &= matches!(
                call.function.value(),
                Func::Agg(
                    AggFunc::Count
                        | AggFunc::Count0
                        | AggFunc::Sum
                        | AggFunc::Total
                        | AggFunc::Avg
                        | AggFunc::Min
                        | AggFunc::Max
                )
            );
        });
    }
    ignores_order
}

/// Whether evaluating `expression` could return an error for some input,
/// as legacy's `expression_can_fail_on_input` decides. Function calls,
/// LIKE, RAISE, JSON and array operators, custom types, casts and nested
/// subqueries all count; plain columns, literals, parameters, arithmetic and
/// comparisons do not.
pub(crate) fn hir_expr_can_fail(document: &HirDocument, expression: &hir::Expr) -> bool {
    let mut can_fail = false;
    expression.for_each(&mut |expression| {
        can_fail |= match expression {
            hir::Expr::Function(_)
            | hir::Expr::Like { .. }
            | hir::Expr::Raise { .. }
            | hir::Expr::Cast { .. }
            | hir::Expr::Subquery(_)
            | hir::Expr::Output(_)
            | hir::Expr::Row(_)
            | hir::Expr::Array(_)
            | hir::Expr::Subscript { .. }
            | hir::Expr::FieldAccess(_)
            | hir::Expr::MergedColumn(_) => true,
            hir::Expr::Binary {
                operator,
                array_concat,
                custom,
                ..
            } => {
                *array_concat
                    || custom.is_some()
                    || matches!(
                        operator,
                        turso_parser::ast::Operator::ArrowRight
                            | turso_parser::ast::Operator::ArrowRightShift
                            | turso_parser::ast::Operator::ArrayContains
                            | turso_parser::ast::Operator::ArrayOverlap
                    )
            }
            hir::Expr::Column(reference) => {
                // Reading a column can run its generated expression or
                // decode a custom type.
                document.source(reference.source).is_none_or(|source| {
                    !matches!(
                        source.generated_expressions.get(reference.column),
                        None | Some(hir::ColumnReadExpression::Absent)
                            | Some(hir::ColumnReadExpression::NotRequired)
                    ) || source
                        .column_type_programs
                        .get(reference.column)
                        .is_none_or(Option::is_some)
                })
            }
            _ => false,
        };
    });
    can_fail
}

/// Whether a join constraint or table-function argument of `from` could
/// return an error, by the rules of [hir_expr_can_fail].
fn hir_from_can_fail(document: &HirDocument, from: &hir::From) -> bool {
    core::iter::once(from.first)
        .chain(from.joins.iter().map(|join| join.right))
        .any(|source| {
            document
                .source(source)
                .is_none_or(|source| !matches!(source.kind, hir::SourceKind::Table(_)))
        })
        || from.joins.iter().any(|join| match &join.constraint {
            hir::JoinConstraint::None => false,
            hir::JoinConstraint::On(expression) => hir_expr_can_fail(document, expression),
            hir::JoinConstraint::Using(_) | hir::JoinConstraint::Natural(_) => true,
        })
}

/// For a block that is `SELECT min(x) FROM t` or `SELECT max(x) FROM t`
/// with nothing else, where t may also be a FROM subquery or CTE, the order
/// that puts the answer first: x ascending for
/// MIN and descending for MAX. Like legacy and SQLite, a scan in that order
/// stops at the first non-NULL x.
fn simple_min_max_term(
    block: &hir::QueryBlock,
    input: &HirQueryBlockPlanInput,
) -> Option<hir::OrderTerm> {
    let [output] = block.outputs.as_slice() else {
        return None;
    };
    let hir::Expr::Function(call) = &output.expr else {
        return None;
    };
    let order = match call.function.value() {
        crate::function::Func::Agg(crate::function::AggFunc::Min) => {
            turso_parser::ast::SortOrder::Asc
        }
        crate::function::Func::Agg(crate::function::AggFunc::Max) => {
            turso_parser::ast::SortOrder::Desc
        }
        _ => return None,
    };
    let hir::FunctionArguments::Expressions {
        values,
        facts,
        distinctness: None,
        order_by,
    } = &call.arguments
    else {
        return None;
    };
    let ([argument], [fact]) = (values.as_slice(), facts.as_slice()) else {
        return None;
    };
    // Like legacy's `detect_simple_aggregate`: one table, or one FROM
    // subquery or CTE whose own order may put the answer first.
    let single_btree = matches!(
        input.sources.as_slice(),
        [HirPlanSource::BTree(_) | HirPlanSource::Derived { .. } | HirPlanSource::Cte { .. }]
    );
    (matches!(
        call.evaluation,
        hir::FunctionEvaluation::Aggregate { filter: None, .. }
    ) && order_by.is_empty()
        && block.aggregate_count == 1
        && matches!(
            &block.body,
            hir::QueryBlockBody::Select { grouping: None, .. }
        )
        && single_btree)
        .then(|| hir::OrderTerm {
            expr: argument.clone(),
            order,
            nulls: None,
            type_fact: fact.type_fact.clone(),
            collation: fact.collation.clone(),
        })
}

/// Whether a block is `SELECT count(*) FROM t` with nothing else, which
/// SQLite answers without a loop over the table.
fn is_simple_count(
    block: &hir::QueryBlock,
    input: &HirQueryBlockPlanInput,
    limit: Option<&hir::Limit>,
) -> bool {
    let [output] = block.outputs.as_slice() else {
        return false;
    };
    let hir::Expr::Function(call) = &output.expr else {
        return false;
    };
    matches!(
        call.function.value(),
        crate::function::Func::Agg(crate::function::AggFunc::Count0)
    ) && matches!(
        call.evaluation,
        hir::FunctionEvaluation::Aggregate { filter: None, .. }
    ) && block.aggregate_count == 1
        && matches!(
            &block.body,
            hir::QueryBlockBody::Select { grouping: None, .. }
        )
        && limit.is_none()
        && matches!(
            input.sources.as_slice(),
            [HirPlanSource::BTree(source)] if matches!(source.table, Table::BTree(_))
        )
        && input.predicates.is_empty()
}

enum HirFromItem<'a> {
    Source(SourceId),
    Group {
        source: SourceId,
        from: &'a hir::From,
    },
}

#[derive(Clone, Copy, Default)]
struct HirFromContext {
    parent_group: Option<SourceId>,
    outer_join: Option<SourceId>,
}

/// Planned form of one resolved HIR document. The semantic document stays
/// alive so source and output identities remain resolvable without copying
/// definitions.
pub(crate) struct HirPlan {
    pub(crate) document: Arc<HirDocument>,
    pub(crate) root: HirRootPlan,
    pub(crate) queries: Vec<HirPlannedQuery>,
    /// Table sources that read a materialized view. Their cursor merges the
    /// current transaction's changes into the stored rows.
    pub(crate) materialized_views: Vec<(SourceId, Arc<Mutex<IncrementalView>>)>,
    /// Parameters of the parts of the statement that turning subqueries
    /// into joins dropped, such as the SELECT list of an EXISTS. The
    /// statement still has them, so lowering registers them.
    pub(crate) dropped_parameters: Vec<hir::Parameter>,
}

/// Planner facts owned by the semantic root. Variants without a source scan
/// need no additional access data before lowering.
pub(crate) enum HirRootPlan {
    Query,
    Insert,
    Update(HirAccessPlan),
    Delete(HirAccessPlan),
    Trigger(HirTriggerPlan),
    SchemaExpressions,
}

/// Planner facts for one trigger body. Expressions remain owned by HIR; this
/// records only command access plans and their source order.
pub(crate) enum HirTriggerPlan {
    Predicate,
    Command(HirTriggerCommandPlan),
    Program(Vec<HirTriggerCommandPlan>),
}

/// Planner facts for one command inside a trigger body.
pub(crate) enum HirTriggerCommandPlan {
    Select(QueryId),
    Insert,
    Update(HirAccessPlan),
    Delete(HirAccessPlan),
}

/// Access plans and estimates for one query in the owned HIR document.
pub(crate) struct HirPlannedQuery {
    pub(crate) query: QueryId,
    /// How many times the plan expects the query to run: more than one for
    /// a correlated subquery, as legacy counts its calls.
    pub(crate) runs: f64,
    /// The order the query returns its rows in, as legacy's
    /// `subquery_intrinsic_order_consumed` reads it from a subquery's plan.
    pub(crate) output_order: Vec<crate::translate::optimizer::order::HirOutputOrder>,
    pub(crate) blocks: Vec<HirQueryBlockPlan>,
    /// The query is the subquery of a correlated `x IN (SELECT y ...)` that
    /// runs like `EXISTS (SELECT ... AND x = y)`: its block plan holds the
    /// extra `x = y` term, so it stops at the first match and can seek it.
    pub(crate) in_probe: bool,
    /// The outer values a correlated aggregate subquery reads. When not
    /// empty, each run stores its result under these values, and a later
    /// run with the same values reads the stored result instead of running
    /// again. This gives what legacy's grouped-table rewrite gives: the
    /// aggregate is computed once per key. Unlike that rewrite, it never
    /// computes keys no outer row asks for.
    pub(crate) result_cache_keys: Vec<hir::Expr>,
    pub(crate) output_cardinality: f64,
    pub(crate) cost: Cost,
}

impl HirPlan {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn build(
        document: Arc<HirDocument>,
        schema: &Schema,
        params: &CostModelParams,
    ) -> Result<Self> {
        let (document, dropped_parameters) =
            super::semantic_unnest::unnest_correlated_subqueries(document, schema, params)?;
        let mut plan = Self::build_without_unnesting(document, schema, params)?;
        plan.dropped_parameters = dropped_parameters;
        Ok(plan)
    }

    /// Plan `document` as it is, without turning its correlated subqueries
    /// into joins.
    pub(crate) fn build_without_unnesting(
        document: Arc<HirDocument>,
        schema: &Schema,
        params: &CostModelParams,
    ) -> Result<Self> {
        let mut queries = Vec::new();
        let mut active = HashSet::default();
        let context = HirPlanContext::new(&document);
        for query in &document.queries {
            context.plan_query_tree(query.id, schema, params, &mut queries, &mut active)?;
        }
        queries.sort_unstable_by_key(|plan| plan.query.index());
        let query_estimate = |query| {
            queries
                .iter()
                .find(|plan| plan.query == query)
                .map(|plan| HirQueryEstimate {
                    output_cardinality: plan.output_cardinality,
                    cost: plan.cost,
                    output_order: plan.output_order.clone(),
                })
        };
        let root = context.plan_root(schema, params, &query_estimate)?;
        let materialized_views = materialized_view_sources(&document, schema)?;
        Ok(Self {
            document,
            root,
            queries,
            materialized_views,
            dropped_parameters: Vec::new(),
        })
    }

    pub(crate) fn materialized_view(
        &self,
        source: SourceId,
    ) -> Option<&Arc<Mutex<IncrementalView>>> {
        self.materialized_views
            .iter()
            .find_map(|(view_source, view)| (*view_source == source).then_some(view))
    }

    pub(crate) fn planned_query(&self, query: QueryId) -> Option<&HirPlannedQuery> {
        self.queries.iter().find(|plan| plan.query == query)
    }
}

/// Resolved source and predicate input for one query block.
pub(crate) struct HirQueryBlockPlanInput {
    pub(crate) sources: Vec<HirPlanSource>,
    pub(crate) groups: Vec<HirFromGroupBoundary>,
    pub(crate) predicates: Vec<HirWhereTerm>,
    /// Index of the first predicate from the WHERE clause. Earlier predicates
    /// come from the FROM clause: join constraints and table-function
    /// arguments.
    pub(crate) where_start: usize,
    /// The block's correlated expression subqueries, whose number of runs
    /// planning counts from the chosen join order. Empty outside SELECT.
    pub(crate) subqueries: Vec<QueryId>,
}

/// Access choices for one resolved HIR query block.
pub(crate) struct HirQueryBlockPlan {
    pub(crate) block: QueryBlockId,
    pub(crate) groups: Vec<HirFromGroupBoundary>,
    pub(crate) loops: Vec<HirPlannedLoop>,
    pub(crate) predicates: Vec<HirWhereTerm>,
    /// Index of the first predicate from the WHERE clause. Earlier predicates
    /// come from the FROM clause. Planning only marks predicates consumed; it
    /// never reorders them.
    pub(crate) where_start: usize,
    pub(crate) group_sort_elided: bool,
    /// The loops return rows in ORDER BY order, so the query needs no sorter.
    pub(crate) order_sort_elided: bool,
    /// The loops return rows in the order the innermost window layer sorts
    /// them in, so that layer needs no sorter.
    pub(crate) window_sort_elided: bool,
    /// How many times each correlated expression subquery of the block runs
    /// for one run of the block, as legacy counts them.
    pub(crate) subquery_calls: Vec<(QueryId, f64)>,
    /// An index method applies LIMIT and OFFSET while it scans.
    pub(crate) limit_applied: bool,
    /// The block is `SELECT min(x)` or `SELECT max(x)` and the loop reads x
    /// in that aggregate's order, so the first non-NULL x is the answer.
    pub(crate) min_max_first_value: bool,
    /// The block is `SELECT count(*) FROM t`, which counts the entries of
    /// the table or its chosen index without a loop.
    pub(crate) simple_count: bool,
    /// LEFT JOINs run as inner joins, because a WHERE term rejects the NULL
    /// row they would add.
    pub(crate) inner_joins: Vec<SourceId>,
    pub(crate) output_cardinality: f64,
    pub(crate) cost: Cost,
}

/// Access choices and estimates shared by query blocks and DML scans.
pub(crate) struct HirAccessPlan {
    pub(crate) groups: Vec<HirFromGroupBoundary>,
    pub(crate) loops: Vec<HirPlannedLoop>,
    pub(crate) predicates: Vec<HirWhereTerm>,
    /// Index of the first predicate from the WHERE clause. Earlier predicates
    /// come from the FROM clause.
    pub(crate) where_start: usize,
    pub(crate) group_sort_elided: bool,
    /// The loops return rows in ORDER BY order.
    pub(crate) order_sort_elided: bool,
    /// The loops return rows in the order of a MIN or MAX target.
    pub(crate) min_max_first_value: bool,
    /// See [`HirQueryBlockPlan::subquery_calls`].
    pub(crate) subquery_calls: Vec<(QueryId, f64)>,
    pub(crate) output_cardinality: f64,
    pub(crate) cost: Cost,
}

/// One selected source loop in physical join order, still using HIR
/// expressions and document-local source identity.
pub(crate) struct HirPlannedLoop {
    pub(crate) source: SourceId,
    pub(crate) source_position: usize,
    pub(crate) access: HirSourceAccess,
}

impl HirPlannedLoop {
    /// The sources this loop supplies a row of: its own, and, for a hash
    /// lookup built from a materialized build input, the sources of the
    /// loops that filled it, which each hash row carries.
    pub(crate) fn sources(&self) -> Vec<SourceId> {
        match &self.access {
            HirSourceAccess::Hash(HirHashLookup {
                build_input: Some(input),
                ..
            }) => input.sources.clone(),
            _ => vec![self.source],
        }
    }

    pub(crate) fn supplies(&self, source: SourceId) -> bool {
        match &self.access {
            HirSourceAccess::Hash(HirHashLookup {
                build_input: Some(input),
                ..
            }) => input.sources.contains(&source),
            _ => self.source == source,
        }
    }
}

/// Whether an index scan can supply every referenced value itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BtreeTableLookup {
    ScanOnly,
    TableRequired,
}

/// Selected access for one resolved HIR source.
pub(crate) enum HirSourceAccess {
    BTree {
        operation: HirBtreeOperation,
        table_lookup: BtreeTableLookup,
    },
    Virtual(HirVirtualTableOperation),
    /// A scan through one query pattern of an index method, such as FTS.
    IndexMethod(crate::translate::hir_index_method::HirIndexMethodAccess),
    /// One seek per branch of an OR term, reading each row once.
    OrUnion(crate::translate::optimizer::HirOrUnion),
    /// Rows found through a hash table built once from the whole table,
    /// keyed by the columns that equal values from earlier loops. It replaces
    /// an automatic index, as legacy's hash join does.
    Hash(HirHashLookup),
    /// A FROM subquery. With `seek`, its rows are stored once and read
    /// through a temporary index over them, keyed by columns that earlier
    /// loops constrain, as legacy's materialized-subquery seek.
    Derived {
        query: QueryId,
        seek: Option<crate::translate::optimizer::HirBtreeOperation>,
    },
    /// A CTE reference; `seek` as for [`HirSourceAccess::Derived`]. A CTE
    /// stored once (`Shared` or `Explicit`) may be read `Backwards`, from
    /// its last row, to give an outer ORDER BY the opposite of its order.
    Cte {
        cte: hir::CteId,
        query: QueryId,
        materialization: HirCteMaterialization,
        seek: Option<crate::translate::optimizer::HirBtreeOperation>,
        direction: crate::translate::plan::IterationDirection,
    },
    RecursiveCte {
        cte: hir::CteId,
    },
    RecursiveInput {
        cte: hir::CteId,
    },
}

/// How a hash lookup finds a table's rows. See [HirSourceAccess::Hash].
pub(crate) struct HirHashLookup {
    pub(crate) keys: Vec<HirHashKey>,
    /// Columns of the table the query reads, kept with each row.
    pub(crate) columns: Vec<usize>,
    /// The access the build table was planned with, which EXPLAIN shows as
    /// legacy does. The hash table itself is built from every row, unless
    /// it has a build input.
    pub(crate) explain: Option<(HirBtreeOperation, BtreeTableLookup)>,
    /// Legacy's materialized build input: the hash table is built from the
    /// joined rows of the loops before the probe loop instead of from the
    /// whole table.
    pub(crate) build_input: Option<Box<HirHashBuildInput>>,
}

/// The loops a hash table is built from, legacy's "materialized hash build
/// input". They run once into a temporary table, each row keeping the hash
/// keys and the columns the query reads of every source they supply. The
/// block's loops then no longer include them: a lookup supplies a row of
/// each of their sources, read from the hash row. Legacy does this when
/// the build table has a term with an earlier table that the hash keys do
/// not cover, or was the probe of an earlier hash join, so that building
/// from the whole table would lose those joins.
pub(crate) struct HirHashBuildInput {
    pub(crate) loops: Vec<HirPlannedLoop>,
    /// The block's predicates as the loops check them: a term that reads a
    /// source outside these loops is consumed here and checked by the
    /// block.
    pub(crate) predicates: Vec<HirWhereTerm>,
    /// The sources the loops supply, in the order their columns are kept.
    pub(crate) sources: Vec<SourceId>,
}

/// One equality between a value of the hashed table's row and a value from
/// an earlier loop.
pub(crate) struct HirHashKey {
    /// An expression that reads only the hashed table.
    pub(crate) build: hir::Expr,
    pub(crate) value: hir::Expr,
    pub(crate) affinity: crate::vdbe::affinity::Affinity,
    pub(crate) collation: Option<crate::translate::collate::CollationSeq>,
}

#[derive(Clone)]
struct HirQueryEstimate {
    output_cardinality: f64,
    cost: Cost,
    output_order: Vec<crate::translate::optimizer::order::HirOutputOrder>,
}

/// The probe positions of the hash joins that keep the materialized build
/// input the join search gave them, like legacy's check after its join
/// search: only when the build table was the probe of an earlier hash join,
/// or has a term with an earlier table that the hash keys do not cover.
/// Otherwise a hash table built from the whole table joins the same rows.
fn materialized_hash_build_inputs(
    plan: &crate::translate::optimizer::join::JoinN,
    access_methods: &[super::optimizer::access_method::AccessMethod],
    constraints: &[HirTableConstraints],
) -> Result<Vec<usize>> {
    use super::optimizer::access_method::AccessMethodParams;
    let hash_join = |method: usize| match &access_methods[method].params {
        AccessMethodParams::HashJoin {
            build_table_idx,
            probe_table_idx,
            ..
        } => Some((*build_table_idx, *probe_table_idx)),
        _ => None,
    };
    let mut probes = Vec::new();
    for (step, (probe_position, method)) in plan.data.iter().copied().enumerate() {
        let AccessMethodParams::HashJoin {
            build_table_idx,
            join_keys,
            materialize_build_input: true,
            ..
        } = &access_methods[method].params
        else {
            continue;
        };
        let earlier = &plan.data[..step];
        let build_was_probe = earlier.iter().any(|(_, method)| {
            hash_join(*method).is_some_and(|(_, probe)| probe == *build_table_idx)
        });
        let mut prior = crate::translate::planner::TableMask::default();
        for (position, method) in earlier {
            prior.set(*position)?;
            if let Some((build, _)) = hash_join(*method) {
                prior.set(build)?;
            }
        }
        let has_prior_term = constraints[*build_table_idx]
            .constraints
            .iter()
            .any(|constraint| {
                constraint.lhs_mask.intersects(&prior)
                    && !join_keys
                        .iter()
                        .any(|key| key.where_clause_idx == constraint.where_clause_pos.0)
            });
        if build_was_probe || has_prior_term {
            probes.push(probe_position);
        }
    }
    Ok(probes)
}

/// Like legacy: turn each LEFT JOIN whose NULL row a WHERE term rejects into
/// an inner join, so its tables can be joined in any order, and return the
/// right sources of those joins. Its ON terms become ordinary terms.
fn left_joins_rejected_by_where(input: &mut HirQueryBlockPlanInput) -> Vec<SourceId> {
    let mut converted = Vec::new();
    // A parenthesized group's join is left alone.
    if !input.groups.is_empty() {
        return converted;
    }
    loop {
        let rejected = input.sources.iter().position(|source| {
            source
                .join_info()
                .is_some_and(|join| join.kind == hir::JoinKind::Left)
                && input.predicates.iter().any(|term| {
                    term.from_outer_join.is_none() && rejects_null_row(&term.expr, source.source())
                })
        });
        let Some(position) = rejected else {
            return converted;
        };
        let source = input.sources[position].source();
        if let Some(join) = input.sources[position].join_info_mut() {
            join.kind = hir::JoinKind::Inner;
        }
        for term in &mut input.predicates {
            if term.from_outer_join == Some(source) {
                term.from_outer_join = None;
            }
        }
        converted.push(source);
    }
}

/// Port of legacy's `where_term_is_null_rejecting_for_table` (SQLite's
/// `impliesNotNullRow`): whether `expr` can never be TRUE when every column
/// of `source` is NULL. When unsure, it says no.
fn rejects_null_row(expr: &hir::Expr, source: SourceId) -> bool {
    use turso_parser::ast::Operator::{And, Is, IsNot, Or};
    let rejects = |expr: &hir::Expr| rejects_null_row(expr, source);
    match expr {
        hir::Expr::Column(column) => column.source == source,
        hir::Expr::RowId(rowid) => *rowid == source,
        // These can be TRUE, or not NULL, when every input is NULL.
        hir::Expr::Binary {
            operator: Is | IsNot,
            ..
        }
        | hir::Expr::IsNull(_)
        | hir::Expr::NotNull(_)
        | hir::Expr::TruthTest { .. }
        | hir::Expr::Case { .. }
        | hir::Expr::Function(_)
        | hir::Expr::Like { .. } => false,
        // Either arm could be TRUE through the other one.
        hir::Expr::Binary {
            lhs,
            operator: And | Or,
            rhs,
            ..
        } => rejects(lhs) && rejects(rhs),
        // A comparison or arithmetic with a NULL input is NULL.
        hir::Expr::Binary { lhs, rhs, .. } => rejects(lhs) || rejects(rhs),
        // NULL is never in a non-empty list.
        hir::Expr::InList { lhs, values, .. } => !values.is_empty() && rejects(lhs),
        hir::Expr::Between {
            expr, start, end, ..
        } => rejects(expr) || (rejects(start) && rejects(end)),
        hir::Expr::Unary { expr, .. }
        | hir::Expr::Cast { expr, .. }
        | hir::Expr::Collate { expr, .. } => rejects(expr),
        _ => false,
    }
}

/// The most result columns a query block may have, SQLite's default
/// SQLITE_MAX_COLUMN.
const MAX_RESULT_COLUMNS: usize = 2000;

/// State shared while one HIR document is converted into plan nodes.
pub(crate) struct HirPlanContext<'a> {
    document: &'a HirDocument,
}

impl<'a> HirPlanContext<'a> {
    pub(crate) fn new(document: &'a HirDocument) -> Self {
        Self { document }
    }

    fn definition(&self, id: SourceId) -> &'a hir::Source {
        self.document
            .source(id)
            .expect("validated HIR contains referenced source")
    }

    fn plan_root(
        &self,
        schema: &Schema,
        params: &CostModelParams,
        query_estimate: &dyn Fn(QueryId) -> Option<HirQueryEstimate>,
    ) -> Result<HirRootPlan> {
        match &self.document.root {
            hir::HirRoot::Query(_) => Ok(HirRootPlan::Query),
            hir::HirRoot::Insert(_) => Ok(HirRootPlan::Insert),
            hir::HirRoot::Update(update) => self
                .plan_update(update, schema, params, query_estimate)
                .map(HirRootPlan::Update),
            hir::HirRoot::Delete(delete) => self
                .plan_delete(delete, schema, params, query_estimate)
                .map(HirRootPlan::Delete),
            hir::HirRoot::Trigger(trigger) => self
                .plan_trigger(trigger, schema, params, query_estimate)
                .map(HirRootPlan::Trigger),
            hir::HirRoot::SchemaExpressions(_) => Ok(HirRootPlan::SchemaExpressions),
        }
    }

    fn plan_trigger(
        &self,
        trigger: &hir::TriggerRoot,
        schema: &Schema,
        params: &CostModelParams,
        query_estimate: &dyn Fn(QueryId) -> Option<HirQueryEstimate>,
    ) -> Result<HirTriggerPlan> {
        match &trigger.body {
            hir::TriggerBody::Predicate(_) => Ok(HirTriggerPlan::Predicate),
            hir::TriggerBody::Command(command) => self
                .plan_trigger_command(command, schema, params, query_estimate)
                .map(HirTriggerPlan::Command),
            hir::TriggerBody::Program(program) => program
                .commands
                .iter()
                .map(|command| self.plan_trigger_command(command, schema, params, query_estimate))
                .collect::<Result<Vec<_>>>()
                .map(HirTriggerPlan::Program),
        }
    }

    fn plan_trigger_command(
        &self,
        command: &hir::TriggerCommand,
        schema: &Schema,
        params: &CostModelParams,
        query_estimate: &dyn Fn(QueryId) -> Option<HirQueryEstimate>,
    ) -> Result<HirTriggerCommandPlan> {
        match command {
            hir::TriggerCommand::Select(query) => Ok(HirTriggerCommandPlan::Select(*query)),
            hir::TriggerCommand::Insert(_) => Ok(HirTriggerCommandPlan::Insert),
            hir::TriggerCommand::Update(update) => self
                .plan_update(update, schema, params, query_estimate)
                .map(HirTriggerCommandPlan::Update),
            hir::TriggerCommand::Delete(delete) => self
                .plan_delete(delete, schema, params, query_estimate)
                .map(HirTriggerCommandPlan::Delete),
        }
    }

    fn plan_update(
        &self,
        update: &hir::Update,
        schema: &Schema,
        params: &CostModelParams,
        query_estimate: &dyn Fn(QueryId) -> Option<HirQueryEstimate>,
    ) -> Result<HirAccessPlan> {
        let usage = update.direct_column_usage(|source| self.document.source(source));
        let mut input = HirQueryBlockPlanInput {
            sources: Vec::new(),
            groups: Vec::new(),
            predicates: Vec::new(),
            where_start: 0,
            subqueries: Vec::new(),
        };
        // The target joins the result of the FROM clause, so a RIGHT or FULL
        // join inside FROM must never NULL-extend it: then it comes after
        // the FROM sources. Otherwise it comes first, which leads the
        // planner to the same join order as legacy, whose choice decides
        // which joined row updates a target row matched more than once.
        let right_join = update.from.as_ref().is_some_and(|from| {
            from.joins
                .iter()
                .any(|join| matches!(join.kind, hir::JoinKind::Right | hir::JoinKind::Full))
        });
        match &update.from {
            Some(from) if right_join => {
                self.append_from(from, HirFromContext::default(), &mut input, &usage)?;
                input.sources.push(self.plan_source(
                    update.target,
                    Some(HirJoinInfo {
                        kind: hir::JoinKind::Inner,
                    }),
                    &usage,
                )?);
            }
            from => {
                input
                    .sources
                    .push(self.plan_source(update.target, None, &usage)?);
                if let Some(from) = from {
                    self.append_from(from, HirFromContext::default(), &mut input, &usage)?;
                }
            }
        }
        input.where_start = input.predicates.len();
        if let Some(predicate) = &update.predicate {
            append_predicates(&mut input.predicates, predicate, None);
        }

        self.plan_source_access(
            input,
            None,
            false,
            &crate::translate::optimizer::join::HirHashJoins::NONE,
            false,
            1.0,
            schema,
            params,
            query_estimate,
        )?
        .ok_or_else(|| {
            LimboError::InternalError("UPDATE target produced no access plan".to_string())
        })
    }

    fn plan_delete(
        &self,
        delete: &hir::Delete,
        schema: &Schema,
        params: &CostModelParams,
        query_estimate: &dyn Fn(QueryId) -> Option<HirQueryEstimate>,
    ) -> Result<HirAccessPlan> {
        let usage = self
            .document
            .root_column_usage(delete.direct_column_usage());
        let mut input = HirQueryBlockPlanInput {
            sources: vec![self.plan_source(delete.target, None, &usage)?],
            groups: Vec::new(),
            predicates: Vec::new(),
            where_start: 0,
            subqueries: Vec::new(),
        };
        if let Some(predicate) = &delete.predicate {
            append_predicates(&mut input.predicates, predicate, None);
        }
        for term in &delete.order_by {
            self.register_expression_index_usage(&mut input.sources, &term.expr)?;
        }
        if let Some(returning) = &delete.returning {
            for output in &returning.outputs {
                self.register_expression_index_usage(&mut input.sources, &output.expr)?;
            }
        }

        let order_target = self.order_target(
            &delete.order_by,
            OrderTargetPurpose::EliminatesSort(EliminatesSortBy::Order),
        );
        self.plan_source_access(
            input,
            order_target.as_ref(),
            false,
            &crate::translate::optimizer::join::HirHashJoins::NONE,
            false,
            1.0,
            schema,
            params,
            query_estimate,
        )?
        .ok_or_else(|| {
            LimboError::InternalError("DELETE target produced no access plan".to_string())
        })
    }

    /// Plan query-backed dependencies before their owning query so source
    /// scans can use the child's output and work estimates. Then, like
    /// legacy's `plan_correlated_subqueries`, plan each correlated
    /// subquery again for the number of times the chosen plan runs it.
    fn plan_query_tree(
        &self,
        query_id: QueryId,
        schema: &Schema,
        params: &CostModelParams,
        planned: &mut Vec<HirPlannedQuery>,
        active: &mut HashSet<QueryId>,
    ) -> Result<HirQueryEstimate> {
        if let Some(plan) = planned.iter().find(|plan| plan.query == query_id) {
            return Ok(HirQueryEstimate {
                output_cardinality: plan.output_cardinality,
                cost: plan.cost,
                output_order: plan.output_order.clone(),
            });
        }
        if !active.insert(query_id) {
            return Err(LimboError::InternalError(format!(
                "HIR query dependency cycle at {query_id}"
            )));
        }
        for dependency in self.document.direct_query_dependencies(query_id) {
            self.plan_query_tree(dependency, schema, params, planned, active)?;
        }
        let estimate = self.plan_query(query_id, 1.0, schema, params, planned)?;
        self.plan_correlated_subqueries(query_id, 1.0, schema, params, planned)?;
        active.remove(&query_id);
        Ok(estimate)
    }

    /// Plan the correlated expression subqueries of `query_id` again for the
    /// number of times its plan runs them, as legacy does: a subquery that
    /// reads sources of a block runs once per row reaching the step that
    /// joins them, and any other once per run of the query. Like legacy, a
    /// statement that writes plans its subqueries for one run, and a
    /// subquery already planned for at least that many runs is kept.
    fn plan_correlated_subqueries(
        &self,
        query_id: QueryId,
        query_runs: f64,
        schema: &Schema,
        params: &CostModelParams,
        planned: &mut Vec<HirPlannedQuery>,
    ) -> Result<()> {
        if !matches!(self.document.root, hir::HirRoot::Query(_)) {
            return Ok(());
        }
        let Some(plan) = planned.iter().find(|plan| plan.query == query_id) else {
            return Err(LimboError::InternalError(format!(
                "HIR query {query_id} is not planned"
            )));
        };
        let counted = plan
            .blocks
            .iter()
            .flat_map(|block| block.subquery_calls.iter().copied())
            .collect::<Vec<_>>();
        for subquery in self.document.expression_subqueries(query_id) {
            if !self.document.reads_outer_sources(subquery) {
                continue;
            }
            let calls = counted
                .iter()
                .find_map(|(query, calls)| (*query == subquery).then_some(*calls))
                .unwrap_or(query_runs)
                .max(1.0);
            let planned_runs = planned
                .iter()
                .find(|plan| plan.query == subquery)
                .map_or(0.0, |plan| plan.runs);
            if planned_runs >= calls {
                continue;
            }
            self.plan_query(subquery, calls, schema, params, planned)?;
            self.plan_correlated_subqueries(subquery, calls, schema, params, planned)?;
        }
        Ok(())
    }

    /// Plan the blocks of one query whose dependencies are planned, for
    /// `runs` runs, and store the plan. Its estimates are for one run.
    fn plan_query(
        &self,
        query_id: QueryId,
        runs: f64,
        schema: &Schema,
        params: &CostModelParams,
        planned: &mut Vec<HirPlannedQuery>,
    ) -> Result<HirQueryEstimate> {
        let query = self
            .document
            .query(query_id)
            .expect("validated HIR query exists");
        let block_order_by = query
            .compounds
            .is_empty()
            .then_some(query.order_by.as_slice())
            .unwrap_or_default();
        let block_limit = query
            .compounds
            .is_empty()
            .then_some(query.limit.as_ref())
            .flatten();
        let query_estimate = |query| {
            planned
                .iter()
                .find(|plan| plan.query == query)
                .map(|plan| HirQueryEstimate {
                    output_cardinality: plan.output_cardinality,
                    cost: plan.cost,
                    output_order: plan.output_order.clone(),
                })
        };
        let in_probe = self.in_probe_equality(query);
        let mut blocks = query
            .blocks
            .iter()
            .map(|block| {
                let mut plan = self.plan_query_block(
                    block,
                    block_order_by,
                    block_limit,
                    in_probe.as_ref(),
                    runs,
                    schema,
                    params,
                    &query_estimate,
                )?;
                plan.output_cardinality /= runs;
                plan.cost = Cost(plan.cost.0 / runs);
                Ok(plan)
            })
            .collect::<Result<Vec<_>>>()?;
        // Like legacy's `estimated_output_rows`: the rows a block returns
        // after grouping or aggregates, and a literal LIMIT, not the rows its
        // loops read.
        let block_rows = query
            .blocks
            .iter()
            .zip(&blocks)
            .map(|(block, plan)| {
                self.hir_select_output_rows(
                    block,
                    plan.output_cardinality,
                    schema,
                    params,
                    &query_estimate,
                )
            })
            .collect::<Vec<_>>();
        let mut estimate = query_estimate_from_blocks(query, &blocks, &block_rows, params);
        if let Some(limit) = block_limit.and_then(|limit| hir_literal_limit(&limit.limit)) {
            let rows_before_limit = estimate.output_cardinality;
            estimate.output_cardinality = rows_before_limit.min(limit);
            // Like legacy: the calls cover the full result, and LIMIT only
            // needs the same share of them.
            if rows_before_limit > 0.0 {
                let share = (estimate.output_cardinality / rows_before_limit).min(1.0);
                for block in &mut blocks {
                    for (_, calls) in &mut block.subquery_calls {
                        *calls *= share;
                    }
                }
            }
        }
        let output_order = self.query_output_order(query, &blocks, planned);
        let plan = HirPlannedQuery {
            query: query_id,
            runs,
            output_order: output_order.clone(),
            blocks,
            in_probe: in_probe.is_some(),
            result_cache_keys: self.result_cache_keys(query),
            output_cardinality: estimate.output_cardinality,
            cost: estimate.cost,
        };
        match planned.iter_mut().find(|planned| planned.query == query_id) {
            Some(existing) => *existing = plan,
            None => planned.push(plan),
        }
        estimate.output_order = output_order;
        Ok(estimate)
    }

    /// The order a planned query returns its rows in, in terms of its output
    /// columns, as legacy's `subquery_intrinsic_order_consumed` reads it from
    /// a subquery's plan: its ORDER BY; else its GROUP BY keys, which leave
    /// the GROUP BY sorter ascending; else, for a block that only filters one
    /// source, the order its loop reads that source in: an index or rowid
    /// scan of a table, or the order of a FROM subquery or CTE it scans.
    fn query_output_order(
        &self,
        query: &hir::Query,
        blocks: &[HirQueryBlockPlan],
        planned: &[HirPlannedQuery],
    ) -> Vec<crate::translate::optimizer::order::HirOutputOrder> {
        use crate::translate::optimizer::order::HirOutputOrder;
        use turso_parser::ast::{NullsOrder, SortOrder};
        let (true, [block], [block_plan]) =
            (query.compounds.is_empty(), query.blocks.as_slice(), blocks)
        else {
            return Vec::new();
        };
        let collation = |collation: Option<&hir::ResolvedCollation>| {
            collation
                .map(|collation| *collation.value())
                .unwrap_or_default()
        };
        let nulls = |order: SortOrder, nulls: Option<NullsOrder>| {
            nulls.unwrap_or_else(|| NullsOrder::default_for(order))
        };
        let output_position = |expression: &hir::Expr| match expression {
            hir::Expr::Output(output) => query.output.iter().position(|id| id == output),
            expression => query.output.iter().position(|id| {
                self.document
                    .output(*id)
                    .is_some_and(|output| output.expr.equivalent(expression))
            }),
        };
        let mut columns = Vec::new();
        if !query.order_by.is_empty() {
            for term in &query.order_by {
                let Some(column) = output_position(&term.expr) else {
                    break;
                };
                columns.push(HirOutputOrder {
                    column,
                    order: term.order,
                    collation: collation(term.collation.as_ref()),
                    nulls: nulls(term.order, term.nulls),
                });
            }
            return columns;
        }
        // Windows sort rows again after grouping and after the loops.
        if !block.windows.is_empty() {
            return columns;
        }
        let hir::QueryBlockBody::Select {
            distinctness,
            grouping,
            ..
        } = &block.body
        else {
            return columns;
        };
        if let Some(grouping) = grouping {
            for (key, key_collation) in grouping.keys.iter().zip(&grouping.key_collations) {
                let Some(column) = output_position(key) else {
                    break;
                };
                columns.push(HirOutputOrder {
                    column,
                    order: SortOrder::Asc,
                    collation: collation(key_collation.as_ref()),
                    nulls: NullsOrder::default_for(SortOrder::Asc),
                });
            }
            return columns;
        }
        // Like legacy's `finalized_scan_subquery_order_consumed`: only a plain
        // scan of one source keeps the order rows are read in.
        let [planned_loop] = block_plan.loops.as_slice() else {
            return columns;
        };
        if block.aggregate_count != 0 || distinctness.is_some() || query.limit.is_some() {
            return columns;
        }
        let source = planned_loop.source;
        let source_column_position = |column: usize| {
            query.output.iter().position(|id| {
                self.document.output(*id).is_some_and(|output| {
                    matches!(&output.expr, hir::Expr::Column(reference)
                        if reference.source == source && reference.column == column)
                })
            })
        };
        let reverse = |order: SortOrder| match order {
            SortOrder::Asc => SortOrder::Desc,
            SortOrder::Desc => SortOrder::Asc,
        };
        match &planned_loop.access {
            HirSourceAccess::BTree {
                operation: HirBtreeOperation::Scan { iter_dir, index },
                ..
            } => {
                let backwards = *iter_dir == crate::translate::plan::IterationDirection::Backwards;
                let hir::SourceKind::Table(table) = &self.definition(source).kind else {
                    return columns;
                };
                let table_columns = table.value().columns();
                let keys: Vec<(
                    usize,
                    SortOrder,
                    Option<crate::translate::collate::CollationSeq>,
                    Option<NullsOrder>,
                )> = match index {
                    Some(index) => {
                        let mut keys = Vec::new();
                        for column in &index.columns {
                            if column.expr.is_some() {
                                break;
                            }
                            keys.push((
                                column.pos_in_table,
                                column.order,
                                column.collation,
                                column.nulls_order,
                            ));
                        }
                        keys
                    }
                    None => match table_columns
                        .iter()
                        .position(|column| column.is_rowid_alias())
                    {
                        Some(rowid) => vec![(rowid, SortOrder::Asc, None, None)],
                        None => Vec::new(),
                    },
                };
                for (table_column, order, key_collation, key_nulls) in keys {
                    let Some(column) = source_column_position(table_column) else {
                        break;
                    };
                    let forward_nulls = nulls(order, key_nulls);
                    columns.push(HirOutputOrder {
                        column,
                        order: if backwards { reverse(order) } else { order },
                        collation: key_collation
                            .or_else(|| table_columns[table_column].collation_opt())
                            .unwrap_or_default(),
                        nulls: if backwards {
                            forward_nulls.reverse()
                        } else {
                            forward_nulls
                        },
                    });
                }
            }
            HirSourceAccess::Derived {
                query: inner,
                seek: None,
            }
            | HirSourceAccess::Cte {
                query: inner,
                seek: None,
                ..
            } => {
                let backwards = matches!(
                    &planned_loop.access,
                    HirSourceAccess::Cte { direction, .. }
                        if *direction == crate::translate::plan::IterationDirection::Backwards
                );
                let Some(inner_order) = planned
                    .iter()
                    .find(|plan| plan.query == *inner)
                    .map(|plan| &plan.output_order)
                else {
                    return columns;
                };
                for inner_column in inner_order {
                    let Some(column) = source_column_position(inner_column.column) else {
                        break;
                    };
                    columns.push(HirOutputOrder {
                        column,
                        order: if backwards {
                            reverse(inner_column.order)
                        } else {
                            inner_column.order
                        },
                        collation: inner_column.collation,
                        nulls: if backwards {
                            inner_column.nulls.reverse()
                        } else {
                            inner_column.nulls
                        },
                    });
                }
            }
            _ => {}
        }
        columns
    }

    /// Legacy's `estimate_select_output_rows` for one run of a HIR block:
    /// an aggregate without GROUP BY returns one row; GROUP BY returns at most
    /// one row per group, counted from ANALYZE statistics of an index on the
    /// keys, or else bounded by the rows of the block's sources the keys
    /// read.
    fn hir_select_output_rows(
        &self,
        block: &hir::QueryBlock,
        input_rows: f64,
        schema: &Schema,
        params: &CostModelParams,
        query_estimate: &dyn Fn(QueryId) -> Option<HirQueryEstimate>,
    ) -> f64 {
        let hir::QueryBlockBody::Select { grouping, .. } = &block.body else {
            return input_rows;
        };
        let Some(grouping) = grouping else {
            return if block.aggregate_count == 0 {
                input_rows
            } else {
                1.0
            };
        };
        if grouping.keys.is_empty() {
            return 1.0;
        }
        if let Some(groups) = self.hir_group_count_from_analyze(block, grouping, schema) {
            return input_rows.min(groups.max(1.0));
        }
        let mut sources: Vec<SourceId> = Vec::new();
        for key in &grouping.keys {
            key.for_each(&mut |expression| {
                if let hir::Expr::Column(hir::ColumnRef { source, .. }) | hir::Expr::RowId(source) =
                    expression
                {
                    if !sources.contains(source) {
                        sources.push(*source);
                    }
                }
            });
        }
        if sources.is_empty() {
            return 1.0;
        }
        let rows_per_call = sources.iter().try_fold(1.0, |rows, source| {
            let definition = self.document.source(*source)?;
            if definition.owner != hir::SourceOwner::QueryBlock(block.id) {
                return None;
            }
            let source_rows = match &definition.kind {
                hir::SourceKind::Table(table) => *base_row_estimate(schema, table.value(), params),
                hir::SourceKind::Derived(query) => query_estimate(*query)
                    .map_or(*RowCountEstimate::hardcoded_fallback(params), |estimate| {
                        estimate.output_cardinality.max(1.0)
                    }),
                hir::SourceKind::Cte(cte) => match self.document.cte(*cte).map(|cte| &cte.body) {
                    Some(hir::CteBody::Query(query)) => query_estimate(*query)
                        .map_or(*RowCountEstimate::hardcoded_fallback(params), |estimate| {
                            estimate.output_cardinality.max(1.0)
                        }),
                    _ => *RowCountEstimate::hardcoded_fallback(params),
                },
                _ => return None,
            };
            Some(rows * source_rows)
        });
        rows_per_call.map_or(input_rows, |rows| input_rows.min(rows))
    }

    /// Legacy's `group_count_from_analyze`: when every GROUP BY key is a
    /// column of one table and an index starts with exactly those columns,
    /// ANALYZE says how many distinct keys it holds.
    fn hir_group_count_from_analyze(
        &self,
        block: &hir::QueryBlock,
        grouping: &hir::Grouping,
        schema: &Schema,
    ) -> Option<f64> {
        let mut source = None;
        let mut columns: Vec<usize> = Vec::new();
        for key in &grouping.keys {
            let hir::Expr::Column(column) = key else {
                return None;
            };
            if source.is_some_and(|source| source != column.source) {
                return None;
            }
            source = Some(column.source);
            columns.push(column.column);
        }
        let definition = self.document.source(source?)?;
        if definition.owner != hir::SourceOwner::QueryBlock(block.id) {
            return None;
        }
        let hir::SourceKind::Table(table) = &definition.kind else {
            return None;
        };
        let btree = table.value().btree()?;
        let table_stats = schema.analyze_stats.table_stats(&btree.name)?;
        table_stats
            .index_stats
            .iter()
            .filter_map(|(index_name, index_stats)| {
                let index = schema.get_index(&btree.name, index_name)?;
                if index.where_clause.is_some() || index.columns.len() < columns.len() {
                    return None;
                }
                let columns_match =
                    index
                        .columns
                        .iter()
                        .zip(&columns)
                        .all(|(index_column, group_column)| {
                            index_column.expr.is_none()
                                && index_column.pos_in_table == *group_column
                        });
                if !columns_match {
                    return None;
                }
                let total_rows = index_stats.total_rows? as f64;
                let rows_per_group = *index_stats
                    .avg_rows_per_distinct_prefix
                    .get(columns.len().checked_sub(1)?)?
                    as f64;
                (rows_per_group > 0.0).then_some(total_rows / rows_per_group)
            })
            .fold(None, |best: Option<f64>, groups| {
                Some(best.map_or(groups, |best| best.max(groups)))
            })
    }

    /// The outer values to cache a correlated subquery's result under, or
    /// none to run it every time.
    ///
    /// Only a SELECT statement caches: a statement that writes tables could
    /// change what the subquery reads between runs. Only an ungrouped
    /// aggregate caches, as legacy's rewrite does: such a subquery reads many
    /// rows to produce one, so a repeated key saves real work.
    fn result_cache_keys(&self, query: &hir::Query) -> Vec<hir::Expr> {
        let [block] = query.blocks.as_slice() else {
            return Vec::new();
        };
        if !matches!(self.document.root, hir::HirRoot::Query(_))
            || !query.compounds.is_empty()
            || block.aggregate_count == 0
            || block.window_function_count != 0
            || !matches!(
                &block.body,
                hir::QueryBlockBody::Select { grouping: None, .. }
            )
        {
            return Vec::new();
        }
        self.document
            .repeatable_outer_values(query.id)
            .unwrap_or_default()
    }

    /// The `x = y` term that lets a correlated `x IN (SELECT y ...)` run like
    /// `EXISTS (SELECT ... AND x = y)`, as legacy's semi-join rewrite does.
    /// The subquery then stops at its first match and can seek `y` through
    /// an index, instead of collecting every `y` into a list for each outer
    /// row.
    ///
    /// The two forms differ in three ways, so the probe is only used where
    /// none of them can change the result:
    ///
    /// - IN is NULL instead of false when nothing matches and `x` or some `y`
    ///   is NULL. The IN must be a whole term of a WHERE clause, where NULL
    ///   and false both reject the row.
    /// - IN reads every row of the subquery, and the probe stops at the first
    ///   match. An expression that fails on a later row would fail only in
    ///   the IN form, so no expression of the subquery may fail.
    /// - IN evaluates `x` once per outer row, and the probe once per inner
    ///   row it tests, so `x` may not fail either.
    fn in_probe_equality(&self, query: &hir::Query) -> Option<hir::Expr> {
        let [block] = query.blocks.as_slice() else {
            return None;
        };
        if !query.compounds.is_empty()
            || query.limit.is_some()
            || query.captures.is_empty()
            || block.from.is_none()
            || block.aggregate_count != 0
            || block.window_function_count != 0
            || block.outputs.len() != 1
        {
            return None;
        }
        let hir::QueryBlockBody::Select {
            filter,
            grouping: None,
            ..
        } = &block.body
        else {
            return None;
        };
        let parent = self.document.query(query.parent?)?;
        let mut probe = None;
        for parent_block in &parent.blocks {
            if let hir::QueryBlockBody::Select {
                filter: Some(parent_filter),
                ..
            } = &parent_block.body
            {
                for conjunct in parent_filter.conjuncts() {
                    if let hir::Expr::Subquery(hir::SubqueryExpr::In {
                        lhs,
                        query: in_query,
                        negated: false,
                        comparison,
                    }) = conjunct
                    {
                        if *in_query == query.id && comparison.components.len() == 1 {
                            probe = Some((lhs, comparison));
                        }
                    }
                }
            }
        }
        let (lhs, comparison) = probe?;
        if self.document.subquery_use_count(parent.id, query.id) != 1
            || hir_expr_can_fail(self.document, lhs)
        {
            return None;
        }
        let output = &block.outputs[0].expr;
        if hir_expr_can_fail(self.document, output)
            || filter
                .as_ref()
                .is_some_and(|filter| hir_expr_can_fail(self.document, filter))
            || block
                .from
                .as_ref()
                .is_some_and(|from| hir_from_can_fail(self.document, from))
        {
            return None;
        }
        Some(hir::Expr::Binary {
            lhs: lhs.clone(),
            operator: turso_parser::ast::Operator::Equals,
            rhs: Box::new(output.clone()),
            array_concat: false,
            custom: None,
            comparison: Some(comparison.clone()),
        })
    }

    /// Build planner source metadata without resolving names, allocating a
    /// second identity, or reading the catalog again.
    pub(crate) fn source(
        &self,
        source: SourceId,
        join_info: Option<HirJoinInfo>,
        usage: &[ColumnUsage],
    ) -> Result<HirPlannedSource> {
        planned_source_from_definition(self.definition(source), join_info, usage)
    }

    fn cte_materialization(
        &self,
        owner: QueryId,
        cte: hir::CteId,
        query: QueryId,
    ) -> HirCteMaterialization {
        let definition = self
            .document
            .cte(cte)
            .expect("validated HIR contains referenced CTE");
        if matches!(
            definition.materialized,
            turso_parser::ast::Materialized::Yes
        ) {
            return HirCteMaterialization::Explicit;
        }

        if self.cte_reference_count(owner, cte) > 1 && !self.query_tree_has_outer_dependency(query)
        {
            HirCteMaterialization::Shared
        } else {
            HirCteMaterialization::PerReference
        }
    }

    /// Count references in one query plan. A CTE body is a separate plan tree:
    /// when reached through a CTE source, its nested references are classified
    /// while that body is planned instead of being multiplied by its callers.
    fn cte_reference_count(&self, root: QueryId, target: hir::CteId) -> usize {
        self.document
            .queries
            .iter()
            .filter(|query| self.query_belongs_to_tree(query.id, root, true))
            .flat_map(|query| &query.blocks)
            .filter_map(|block| block.from.as_ref())
            .map(|from| self.count_cte_references_in_from(from, target))
            .sum()
    }

    fn count_cte_references_in_from(&self, from: &hir::From, target: hir::CteId) -> usize {
        core::iter::once(from.first)
            .chain(from.joins.iter().map(|join| join.right))
            .map(|source| self.count_cte_references_in_source(source, target))
            .sum()
    }

    fn count_cte_references_in_source(&self, source: SourceId, target: hir::CteId) -> usize {
        match &self.definition(source).kind {
            hir::SourceKind::Cte(cte) => usize::from(*cte == target),
            hir::SourceKind::FromGroup(group) => {
                self.count_cte_references_in_from(&group.from, target)
            }
            _ => 0,
        }
    }

    fn query_tree_has_outer_dependency(&self, root: QueryId) -> bool {
        let queries = self
            .document
            .queries
            .iter()
            .filter(|query| self.query_belongs_to_tree(query.id, root, false))
            .map(|query| query.id)
            .collect::<HashSet<_>>();

        self.document.queries.iter().any(|query| {
            queries.contains(&query.id)
                && query.captures.iter().any(|source| {
                    let source = self.definition(*source);
                    match source.owner {
                        hir::SourceOwner::QueryBlock(block) => !queries.contains(&block.query),
                        hir::SourceOwner::Cte(cte) => !self.cte_belongs_to_queries(cte, &queries),
                        hir::SourceOwner::Root => true,
                    }
                })
        })
    }

    fn query_belongs_to_tree(
        &self,
        mut query: QueryId,
        root: QueryId,
        stop_at_cte_body: bool,
    ) -> bool {
        loop {
            if query == root {
                return true;
            }
            if stop_at_cte_body && self.is_cte_body_query(query) {
                return false;
            }
            let Some(parent) = self
                .document
                .query(query)
                .expect("validated HIR query exists")
                .parent
            else {
                return false;
            };
            query = parent;
        }
    }

    fn is_cte_body_query(&self, query: QueryId) -> bool {
        self.document.ctes.iter().any(|cte| match &cte.body {
            hir::CteBody::Query(body) => *body == query,
            hir::CteBody::Recursive(recursive) => {
                recursive.seed == query || recursive.arms.iter().any(|arm| arm.query == query)
            }
        })
    }

    fn cte_belongs_to_queries(&self, cte: hir::CteId, queries: &HashSet<QueryId>) -> bool {
        let definition = self
            .document
            .cte(cte)
            .expect("validated HIR contains captured CTE source");
        match &definition.body {
            hir::CteBody::Query(query) => queries.contains(query),
            hir::CteBody::Recursive(recursive) => {
                queries.contains(&recursive.seed)
                    || recursive
                        .arms
                        .iter()
                        .any(|arm| queries.contains(&arm.query))
            }
        }
    }

    fn plan_source(
        &self,
        source: SourceId,
        join_info: Option<HirJoinInfo>,
        usage: &[ColumnUsage],
    ) -> Result<HirPlanSource> {
        match &self.definition(source).kind {
            hir::SourceKind::Table(_) | hir::SourceKind::TableFunction { .. } => self
                .source(source, join_info, usage)
                .map(HirPlanSource::BTree),
            hir::SourceKind::Derived(query) => Ok(HirPlanSource::Derived {
                source,
                query: *query,
                join_info,
                output_order: Vec::new(),
            }),
            hir::SourceKind::Cte(cte) => {
                let definition = self
                    .document
                    .cte(*cte)
                    .expect("validated HIR contains referenced CTE");
                match &definition.body {
                    hir::CteBody::Query(query) => {
                        let materialization = match self.definition(source).owner {
                            hir::SourceOwner::QueryBlock(block) => {
                                self.cte_materialization(block.query, *cte, *query)
                            }
                            // The FROM clause of UPDATE ... FROM reads the CTE
                            // once per reference, unless it is MATERIALIZED.
                            hir::SourceOwner::Root => {
                                if matches!(
                                    definition.materialized,
                                    turso_parser::ast::Materialized::Yes
                                ) {
                                    HirCteMaterialization::Explicit
                                } else {
                                    HirCteMaterialization::PerReference
                                }
                            }
                            owner => {
                                return Err(LimboError::InternalError(format!(
                                    "CTE HIR source {source} has non-query owner {owner:?}"
                                )));
                            }
                        };
                        Ok(HirPlanSource::Cte {
                            source,
                            cte: *cte,
                            query: *query,
                            materialization,
                            join_info,
                            output_order: Vec::new(),
                        })
                    }
                    hir::CteBody::Recursive(_) => Ok(HirPlanSource::RecursiveCte {
                        source,
                        cte: *cte,
                        join_info,
                    }),
                }
            }
            hir::SourceKind::RecursiveInput(cte) => Ok(HirPlanSource::RecursiveInput {
                source,
                cte: *cte,
                join_info,
            }),
            _ => Err(LimboError::InternalError(format!(
                "source {source} cannot be planned as a query source"
            ))),
        }
    }

    fn from_item(&self, source: SourceId) -> HirFromItem<'_> {
        match &self.definition(source).kind {
            hir::SourceKind::FromGroup(group) => HirFromItem::Group {
                source,
                from: &group.from,
            },
            _ => HirFromItem::Source(source),
        }
    }

    fn append_from(
        &self,
        from: &hir::From,
        context: HirFromContext,
        input: &mut HirQueryBlockPlanInput,
        usage: &[ColumnUsage],
    ) -> Result<()> {
        self.append_from_item(from.first, None, context, input, usage)?;

        for join in &from.joins {
            let local_outer_join = match join.kind {
                hir::JoinKind::Left | hir::JoinKind::Right | hir::JoinKind::Full => {
                    Some(join.right)
                }
                hir::JoinKind::Comma
                | hir::JoinKind::Inner
                | hir::JoinKind::Cross
                | hir::JoinKind::Semi
                | hir::JoinKind::Anti => None,
            };
            let predicate_owner = local_outer_join.or(context.outer_join);
            self.append_from_item(
                join.right,
                Some(HirJoinInfo { kind: join.kind }),
                HirFromContext {
                    outer_join: predicate_owner,
                    ..context
                },
                input,
                usage,
            )?;
            self.append_join_predicates(&mut input.predicates, join, predicate_owner);
        }
        Ok(())
    }

    fn append_from_item(
        &self,
        source: SourceId,
        join_info: Option<HirJoinInfo>,
        context: HirFromContext,
        input: &mut HirQueryBlockPlanInput,
        usage: &[ColumnUsage],
    ) -> Result<()> {
        match self.from_item(source) {
            HirFromItem::Source(source) => {
                input
                    .sources
                    .push(self.plan_source(source, join_info, usage)?);
                self.append_table_function_predicates(
                    &mut input.predicates,
                    source,
                    context.outer_join,
                );
            }
            HirFromItem::Group { source, from } => {
                let start = input.sources.len();
                self.append_from(
                    from,
                    HirFromContext {
                        parent_group: Some(source),
                        ..context
                    },
                    input,
                    usage,
                )?;
                input.groups.push(HirFromGroupBoundary {
                    source,
                    parent: context.parent_group,
                    source_range: start..input.sources.len(),
                    join_info,
                });
            }
        }
        Ok(())
    }

    fn append_join_predicates(
        &self,
        predicates: &mut Vec<HirWhereTerm>,
        join: &hir::Join,
        from_outer_join: Option<SourceId>,
    ) {
        match &join.constraint {
            hir::JoinConstraint::None => {}
            hir::JoinConstraint::On(expression) => {
                append_predicates(predicates, expression, from_outer_join)
            }
            hir::JoinConstraint::Using(columns) | hir::JoinConstraint::Natural(columns) => {
                for column in columns {
                    predicates.push(HirWhereTerm {
                        expr: using_equality(column),
                        from_outer_join,
                        consumed: false,
                    });
                }
            }
        }
    }

    /// Assemble planner input from resolved HIR without repeating binding.
    pub(crate) fn query_block_input(
        &self,
        block: &hir::QueryBlock,
        order_by: &[hir::OrderTerm],
        usage: &[ColumnUsage],
    ) -> Result<HirQueryBlockPlanInput> {
        let mut input = HirQueryBlockPlanInput {
            sources: Vec::new(),
            groups: Vec::new(),
            predicates: Vec::new(),
            where_start: 0,
            subqueries: Vec::new(),
        };

        if let Some(from) = &block.from {
            self.append_from(from, HirFromContext::default(), &mut input, usage)?;
        }
        input.where_start = input.predicates.len();

        if let hir::QueryBlockBody::Select {
            filter: Some(filter),
            ..
        } = &block.body
        {
            append_predicates(&mut input.predicates, filter, None);
        }

        // GROUP BY and window lowering copy every column the block reads into
        // a sorter or buffer, so an expression index cannot stand in for the
        // columns its expressions read.
        let copies_columns = !block.windows.is_empty()
            || matches!(
                &block.body,
                hir::QueryBlockBody::Select {
                    grouping: Some(grouping),
                    ..
                } if !grouping.keys.is_empty()
            );
        if copies_columns {
            return Ok(input);
        }
        for output in &block.outputs {
            self.register_expression_index_usage(&mut input.sources, &output.expr)?;
        }
        if let hir::QueryBlockBody::Select {
            grouping: Some(grouping),
            ..
        } = &block.body
        {
            for key in &grouping.keys {
                self.register_expression_index_usage(&mut input.sources, key)?;
            }
            if let Some(having) = &grouping.having {
                self.register_expression_index_usage(&mut input.sources, having)?;
            }
        }
        for term in order_by {
            self.register_expression_index_usage(&mut input.sources, &term.expr)?;
        }
        // A WHERE term equal to a partial index's predicate is not evaluated
        // when the index is read, so the columns only it reads need not be
        // in the index.
        let where_terms = input.predicates[input.where_start..]
            .iter()
            .map(|term| term.expr.clone())
            .collect::<Vec<_>>();
        for term in &where_terms {
            self.register_expression_index_usage(&mut input.sources, term)?;
        }

        Ok(input)
    }

    fn append_table_function_predicates(
        &self,
        predicates: &mut Vec<HirWhereTerm>,
        source: SourceId,
        from_outer_join: Option<SourceId>,
    ) {
        let hir::SourceKind::TableFunction {
            argument_predicates,
            ..
        } = &self.definition(source).kind
        else {
            return;
        };
        predicates.extend(
            argument_predicates
                .iter()
                .cloned()
                .map(|expr| HirWhereTerm {
                    expr,
                    from_outer_join,
                    consumed: false,
                }),
        );
    }

    /// Collect physical constraints for every source while keeping the source
    /// and predicate order established by `query_block_input`.
    pub(crate) fn constraints(
        &self,
        input: &HirQueryBlockPlanInput,
        base_rows: &[RowCountEstimate],
        schema: &Schema,
        params: &CostModelParams,
        query_rows: &dyn Fn(QueryId) -> Option<f64>,
    ) -> Result<Vec<HirTableConstraints>> {
        assert_eq!(
            input.sources.len(),
            base_rows.len(),
            "every HIR plan source must have a base-row estimate"
        );
        let layout = HirFromLayout::new(&input.sources, &input.groups);
        input
            .sources
            .iter()
            .zip(base_rows)
            .map(|(source, row_count)| {
                let source = match source {
                    HirPlanSource::BTree(source) => HirConstraintSource::BTree(source),
                    HirPlanSource::Derived { source, .. } => HirConstraintSource::Derived {
                        source: *source,
                        row_count: *row_count,
                    },
                    HirPlanSource::Cte { source, .. } => HirConstraintSource::Cte {
                        source: *source,
                        row_count: *row_count,
                    },
                    HirPlanSource::RecursiveCte { source, .. }
                    | HirPlanSource::RecursiveInput { source, .. } => HirConstraintSource::Cte {
                        source: *source,
                        row_count: *row_count,
                    },
                };
                hir_constraints_for_source(
                    self.document,
                    &layout,
                    &input.predicates,
                    source,
                    schema,
                    params,
                    query_rows,
                )
            })
            .collect()
    }

    /// Plan one resolved query block without exposing mutable planner input.
    #[allow(clippy::too_many_arguments)]
    fn plan_query_block(
        &self,
        block: &hir::QueryBlock,
        order_by: &[hir::OrderTerm],
        limit: Option<&hir::Limit>,
        extra_predicate: Option<&hir::Expr>,
        initial_cardinality: f64,
        schema: &Schema,
        params: &CostModelParams,
        query_estimate: &dyn Fn(QueryId) -> Option<HirQueryEstimate>,
    ) -> Result<HirQueryBlockPlan> {
        // Like legacy and SQLite's SQLITE_MAX_COLUMN.
        if block.outputs.len() > MAX_RESULT_COLUMNS {
            crate::bail_parse_error!("too many columns in result set");
        }
        let query = self
            .document
            .query(block.id.query)
            .expect("validated HIR query block has owning query");
        let usage = self.document.source_column_usage(query.id);
        if block.from.is_none() {
            let input = self.query_block_input(block, order_by, &usage)?;
            let output_cardinality = match &block.body {
                hir::QueryBlockBody::Values { rows } => initial_cardinality * rows.len() as f64,
                hir::QueryBlockBody::Select { .. } => initial_cardinality,
            };
            return Ok(HirQueryBlockPlan {
                block: block.id,
                groups: input.groups,
                loops: Vec::new(),
                predicates: input.predicates,
                where_start: input.where_start,
                group_sort_elided: false,
                order_sort_elided: false,
                window_sort_elided: false,
                subquery_calls: Vec::new(),
                limit_applied: false,
                min_max_first_value: false,
                simple_count: false,
                inner_joins: Vec::new(),
                output_cardinality,
                cost: Cost(0.0),
            });
        }

        let mut input = self.query_block_input(block, order_by, &usage)?;
        input.subqueries = self.document.expression_subqueries(block.id.query);
        if let Some(predicate) = extra_predicate {
            append_predicates(&mut input.predicates, predicate, None);
        }
        // Join planning tracks tables in a 64-bit mask.
        if input.sources.len() > crate::translate::plan::TableReferences::MAX_JOINED_TABLES {
            crate::bail_parse_error!(
                "Only up to {} tables can be joined",
                crate::translate::plan::TableReferences::MAX_JOINED_TABLES
            );
        }
        let where_start = input.where_start;
        let inner_joins = left_joins_rejected_by_where(&mut input);
        if let Some(plan) = self.plan_index_method(block, order_by, limit, &mut input)? {
            return Ok(plan);
        }
        if let Some(plan) = self.plan_index_method_join(
            block,
            &mut input,
            initial_cardinality,
            schema,
            params,
            query_estimate,
        )? {
            return Ok(plan);
        }
        let min_max_terms = simple_min_max_term(block, &input)
            .into_iter()
            .collect::<Vec<_>>();
        // Like legacy's `compute_order_target`: a grouped block aims for its
        // GROUP BY order even when it has an ORDER BY, so an index can still
        // replace the GROUP BY sorter.
        // Like the ORDER BY of legacy's window source subquery: the rows of a
        // block with windows are wanted in the innermost window's key order.
        let window_target = (min_max_terms.is_empty()
            && hir_block_grouping(block).is_none()
            && block.aggregate_count == 0)
            .then(|| self.window_target(block))
            .flatten();
        let order_target = if !min_max_terms.is_empty() {
            self.order_target(&min_max_terms, OrderTargetPurpose::Extremum)
        } else if window_target.is_some() {
            window_target.clone()
        } else if order_by.is_empty() || hir_block_grouping(block).is_some() {
            self.grouping_target(block, order_by)
        } else {
            self.order_target(
                order_by,
                OrderTargetPurpose::EliminatesSort(EliminatesSortBy::Order),
            )
        };
        let simple_count = is_simple_count(block, &input, limit);
        // Like legacy, a hash join never reads a source that a correlated
        // subquery of the block reads.
        let mut subquery_reads = Vec::new();
        for query in &input.subqueries {
            subquery_reads.extend(self.document.outer_sources(*query));
        }
        let hash = crate::translate::optimizer::join::HirHashJoins {
            allowed: self.hash_joins_allowed(&input),
            // A materialized build input is filled once per statement, so
            // the block must not read an outer row (legacy fills it once
            // for every outer row, which is wrong), and HIR fills it with
            // plain inner joins only (LEFT JOINs a WHERE term made inner
            // included); legacy also allows outer joins there. A semi-join
            // or anti-join comes after every other loop, so it never feeds
            // a build input.
            materialize: !self.document.reads_outer_sources(block.id.query)
                && input.sources.iter().all(|source| {
                    source.join_info().is_none_or(|join| {
                        matches!(
                            join.kind,
                            hir::JoinKind::Comma
                                | hir::JoinKind::Inner
                                | hir::JoinKind::Cross
                                | hir::JoinKind::Semi
                                | hir::JoinKind::Anti
                        )
                    })
                }),
            subquery_reads: &subquery_reads,
        };
        let access = self
            .plan_source_access(
                input,
                order_target.as_ref(),
                true,
                &hash,
                simple_count,
                initial_cardinality,
                schema,
                params,
                query_estimate,
            )?
            .ok_or_else(|| {
                LimboError::InternalError(
                    "query block with FROM produced no B-tree access plan".to_string(),
                )
            })?;
        let plan = HirQueryBlockPlan {
            block: block.id,
            groups: access.groups,
            loops: access.loops,
            predicates: access.predicates,
            where_start,
            group_sort_elided: access.group_sort_elided,
            order_sort_elided: access.order_sort_elided && window_target.is_none(),
            window_sort_elided: access.order_sort_elided && window_target.is_some(),
            subquery_calls: access.subquery_calls,
            limit_applied: false,
            min_max_first_value: access.min_max_first_value,
            simple_count,
            inner_joins,
            output_cardinality: access.output_cardinality,
            cost: access.cost,
        };
        Ok(plan)
    }

    /// Whether a block may use hash joins, which legacy chooses while
    /// ordering its joins (see [crate::translate::optimizer::join::HirHashJoins]).
    ///
    /// The hash table is built once per statement, from every row of its
    /// table, so a subquery that runs again probes the same table. Only a
    /// SELECT statement uses them, since a statement that writes could
    /// change the table. A block with a parenthesized FROM group, or with a
    /// RIGHT join, keeps its nested loops.
    fn hash_joins_allowed(&self, input: &HirQueryBlockPlanInput) -> bool {
        matches!(&self.document.root, hir::HirRoot::Query(_))
            && input.groups.is_empty()
            && !input.sources.iter().any(|source| {
                source
                    .join_info()
                    .is_some_and(|join| join.kind == hir::JoinKind::Right)
            })
    }

    /// The hash lookup of a hash join's build table: one key per join term,
    /// the build table's column against the probe table's value, compared
    /// with the term's affinity and collation.
    fn hash_join_lookup(
        &self,
        predicates: &[HirWhereTerm],
        join_keys: &[crate::translate::plan::HashJoinKey],
    ) -> Result<HirHashLookup> {
        use crate::translate::optimizer::constraints::BinaryExprSide;
        let keys = join_keys
            .iter()
            .map(|key| {
                let hir::Expr::Binary {
                    lhs,
                    rhs,
                    comparison,
                    ..
                } = &predicates[key.where_clause_idx].expr
                else {
                    return Err(LimboError::InternalError(
                        "HIR hash join key is not a binary term".to_string(),
                    ));
                };
                let (build, probe) = match key.build_side {
                    BinaryExprSide::Lhs => (lhs, rhs),
                    BinaryExprSide::Rhs => (rhs, lhs),
                };
                let component = comparison
                    .as_ref()
                    .and_then(|comparison| comparison.components.first());
                Ok(HirHashKey {
                    build: build.as_ref().clone(),
                    value: probe.as_ref().clone(),
                    affinity: component
                        .map_or(crate::vdbe::affinity::Affinity::Blob, |component| {
                            component.affinity
                        }),
                    collation: component
                        .and_then(|component| component.collation.as_ref())
                        .map(|collation| *collation.value()),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut columns = Vec::new();
        for key in &keys {
            key.build.for_each(&mut |expression| {
                if let hir::Expr::Column(column) = expression {
                    if !columns.contains(&column.column) {
                        columns.push(column.column);
                    }
                }
            });
        }
        Ok(HirHashLookup {
            columns,
            keys,
            explain: None,
            build_input: None,
        })
    }

    /// Plan a block that reads one table through a query pattern of one of
    /// its index methods, such as FTS, as legacy does. Like legacy, only the
    /// first table of a block is tried, and only when it is the only one.
    fn plan_index_method(
        &self,
        block: &hir::QueryBlock,
        order_by: &[hir::OrderTerm],
        limit: Option<&hir::Limit>,
        input: &mut HirQueryBlockPlanInput,
    ) -> Result<Option<HirQueryBlockPlan>> {
        let [HirPlanSource::BTree(planned)] = input.sources.as_slice() else {
            return Ok(None);
        };
        // A FROM-clause predicate, such as a table function's argument, is
        // not one the index applies.
        if input.predicates[..input.where_start]
            .iter()
            .any(|term| !term.consumed)
        {
            return Ok(None);
        }
        let source = self.definition(planned.internal_id);
        let hir::QueryBlockBody::Select {
            distinctness,
            grouping,
            ..
        } = &block.body
        else {
            return Ok(None);
        };
        // Like legacy, a grouped block keeps its usual scan.
        if grouping.is_some() {
            return Ok(None);
        }
        let rows_are_output = distinctness.is_none()
            && block.aggregate_count == 0
            && block.window_function_count == 0
            && block.windows.is_empty();
        let Some(matched) = crate::translate::hir_index_method::match_index_methods(
            &crate::translate::hir_index_method::IndexMethodQuery {
                document: self.document,
                source,
                outputs: &block.outputs,
                predicates: &input.predicates,
                where_start: input.where_start,
                order_by,
                limit,
                rows_are_output,
            },
        ) else {
            return Ok(None);
        };
        if let Some(position) = matched.predicate {
            input.predicates[position].consumed = true;
        }
        let source_id = source.id;
        let predicates = std::mem::take(&mut input.predicates);
        Ok(Some(HirQueryBlockPlan {
            block: block.id,
            groups: std::mem::take(&mut input.groups),
            loops: vec![HirPlannedLoop {
                source: source_id,
                source_position: 0,
                access: HirSourceAccess::IndexMethod(matched.access),
            }],
            predicates,
            where_start: input.where_start,
            group_sort_elided: false,
            order_sort_elided: matched.applies_order_by,
            window_sort_elided: false,
            subquery_calls: Vec::new(),
            limit_applied: matched.applies_limit,
            min_max_first_value: false,
            simple_count: false,
            inner_joins: Vec::new(),
            output_cardinality: 1.0,
            cost: Cost(1.0),
        }))
    }

    /// Plan an inner join whose tables include one read through an index
    /// method, such as FTS, for one of its WHERE terms. Like legacy when its
    /// join planner picks the index, that table becomes the outer loop; the
    /// other tables are planned as if its columns were known values, so the
    /// join terms can seek them.
    #[allow(clippy::too_many_arguments)]
    fn plan_index_method_join(
        &self,
        block: &hir::QueryBlock,
        input: &mut HirQueryBlockPlanInput,
        initial_cardinality: f64,
        schema: &Schema,
        params: &CostModelParams,
        query_estimate: &dyn Fn(QueryId) -> Option<HirQueryEstimate>,
    ) -> Result<Option<HirQueryBlockPlan>> {
        let inner = |source: &HirPlanSource| {
            source.join_info().is_none_or(|join| {
                matches!(
                    join.kind,
                    hir::JoinKind::Comma | hir::JoinKind::Inner | hir::JoinKind::Cross
                )
            })
        };
        if input.sources.len() < 2 || !input.groups.is_empty() || !input.sources.iter().all(inner) {
            return Ok(None);
        }
        let hir::QueryBlockBody::Select { grouping, .. } = &block.body else {
            return Ok(None);
        };
        if grouping.is_some() {
            return Ok(None);
        }
        let Some((position, matched)) =
            input
                .sources
                .iter()
                .enumerate()
                .find_map(|(position, source)| {
                    let source = self.definition(source.btree()?.internal_id);
                    crate::translate::hir_index_method::match_index_methods(
                        &crate::translate::hir_index_method::IndexMethodQuery {
                            document: self.document,
                            source,
                            outputs: &block.outputs,
                            predicates: &input.predicates,
                            where_start: input.where_start,
                            order_by: &[],
                            limit: None,
                            rows_are_output: false,
                        },
                    )
                    .filter(|matched| matched.predicate.is_some())
                    .map(|matched| (position, matched))
                })
        else {
            return Ok(None);
        };
        if let Some(predicate) = matched.predicate {
            input.predicates[predicate].consumed = true;
        }
        let indexed = input.sources.remove(position);
        let rest = HirQueryBlockPlanInput {
            sources: std::mem::take(&mut input.sources),
            groups: Vec::new(),
            predicates: std::mem::take(&mut input.predicates),
            where_start: input.where_start,
            subqueries: Vec::new(),
        };
        let access = self
            .plan_source_access(
                rest,
                None,
                true,
                &crate::translate::optimizer::join::HirHashJoins::NONE,
                false,
                initial_cardinality,
                schema,
                params,
                query_estimate,
            )?
            .ok_or_else(|| {
                LimboError::InternalError("HIR join produced no access plan".to_string())
            })?;
        let mut loops = Vec::with_capacity(access.loops.len() + 1);
        loops.push(HirPlannedLoop {
            source: indexed.source(),
            source_position: position,
            access: HirSourceAccess::IndexMethod(matched.access),
        });
        loops.extend(access.loops.into_iter().map(|mut planned_loop| {
            if planned_loop.source_position >= position {
                planned_loop.source_position += 1;
            }
            planned_loop
        }));
        Ok(Some(HirQueryBlockPlan {
            block: block.id,
            groups: Vec::new(),
            loops,
            predicates: access.predicates,
            where_start: input.where_start,
            group_sort_elided: false,
            order_sort_elided: false,
            window_sort_elided: false,
            subquery_calls: Vec::new(),
            limit_applied: false,
            min_max_first_value: false,
            simple_count: false,
            inner_joins: Vec::new(),
            output_cardinality: access.output_cardinality,
            cost: access.cost,
        }))
    }

    /// Choose source access methods and a complete join order directly from
    /// resolved HIR. This keeps the planning boundary free of parser-era table
    /// identities, synthetic subquery tables, and bound AST expressions.
    #[allow(clippy::too_many_arguments)]
    fn plan_source_access(
        &self,
        mut input: HirQueryBlockPlanInput,
        order_target: Option<&HirOrderTarget<'_>>,
        // Whether a table may be read through one seek per branch of an OR
        // term, which DELETE and UPDATE loops do not do yet.
        or_union: bool,
        // Whether and how the block may use hash joins.
        hash: &crate::translate::optimizer::join::HirHashJoins<'_>,
        // Whether the block is a plain `SELECT count(*) FROM t`.
        simple_count: bool,
        initial_cardinality: f64,
        schema: &Schema,
        params: &CostModelParams,
        query_estimate: &dyn Fn(QueryId) -> Option<HirQueryEstimate>,
    ) -> Result<Option<HirAccessPlan>> {
        let query_rows = |query| query_estimate(query).map(|estimate| estimate.output_cardinality);
        let mut base_rows = Vec::with_capacity(input.sources.len());
        let mut source_costs = Vec::with_capacity(input.sources.len());
        for source in &mut input.sources {
            match source {
                HirPlanSource::BTree(source) => {
                    base_rows.push(base_row_estimate(schema, &source.table, params));
                    source_costs.push(Cost(0.0));
                }
                HirPlanSource::Derived {
                    source,
                    query,
                    output_order,
                    ..
                } => {
                    let child = query_estimate(*query).ok_or_else(|| {
                        LimboError::InternalError(format!(
                            "derived HIR source {source} references unplanned query {query}"
                        ))
                    })?;
                    base_rows.push(RowCountEstimate::AnalyzeStats(
                        child.output_cardinality.max(1.0),
                    ));
                    source_costs.push(child.cost);
                    *output_order = child.output_order;
                }
                HirPlanSource::Cte {
                    source,
                    cte,
                    query,
                    output_order,
                    ..
                } => {
                    let child = query_estimate(*query).ok_or_else(|| {
                        LimboError::InternalError(format!(
                            "CTE HIR source {source} for {cte} references unplanned query {query}"
                        ))
                    })?;
                    base_rows.push(RowCountEstimate::AnalyzeStats(
                        child.output_cardinality.max(1.0),
                    ));
                    source_costs.push(child.cost);
                    *output_order = child.output_order;
                }
                HirPlanSource::RecursiveCte { .. } => {
                    base_rows.push(RowCountEstimate::hardcoded_fallback(params));
                    source_costs.push(Cost(0.0));
                }
                HirPlanSource::RecursiveInput { .. } => {
                    base_rows.push(RowCountEstimate::AnalyzeStats(1.0));
                    source_costs.push(Cost(0.0));
                }
            }
        }
        let mut constraints = self.constraints(&input, &base_rows, schema, params, &query_rows)?;
        for (source, table_constraints) in input.sources.iter().zip(&mut constraints) {
            let HirPlanSource::BTree(source) = source else {
                continue;
            };
            let hir::IndexHint::Indexed(index) = &source.indexed else {
                continue;
            };
            if index.value().where_clause.is_none() || !table_constraints.candidates.is_empty() {
                continue;
            }
            // INDEXED BY names a partial index whose WHERE the query does
            // not imply, so no plan reads the table through it.
            if !simple_count {
                crate::bail_parse_error!("no query solution");
            }
            // Like SQLite, `SELECT count(*) FROM t` counts the rows without
            // a loop, so the index does not matter.
            table_constraints.candidates.push(
                super::optimizer::constraints::ConstraintUseCandidate {
                    index: None,
                    refs: Vec::new(),
                    partial_index: None,
                },
            );
        }
        let mut access_methods = Vec::new();
        let result = compute_hir_join_order(
            self.document,
            &input.sources,
            &input.groups,
            &constraints,
            &input.predicates,
            &base_rows,
            &source_costs,
            order_target,
            initial_cardinality,
            &mut access_methods,
            schema,
            &schema.analyze_stats,
            params,
            or_union,
            hash,
        )?;

        let Some(mut result) = result else {
            return Ok(None);
        };
        // Like legacy, prefer a plan that returns rows in the order target's
        // order when the sort it saves costs more than the difference.
        if let Some(ordered) = result.best_ordered_plan.take() {
            const SORT_COST_PER_ROW_MULTIPLIER: f64 = 0.001;
            let sorting_penalty =
                Cost(result.best_plan.output_cardinality * SORT_COST_PER_ROW_MULTIPLIER);
            if result.best_plan.cost + sorting_penalty > ordered.cost {
                result.best_plan = ordered;
            }
        }
        let subquery_calls = self.subquery_calls(
            &input,
            &result.best_plan,
            &access_methods,
            &constraints,
            initial_cardinality,
            params,
        )?;
        // Whether the chosen loops already return rows in the order the
        // GROUP BY or ORDER BY target asks for, so its sort can be skipped.
        let sort_elided = |by: EliminatesSortBy| -> Result<bool> {
            match order_target {
                Some(target) if target.purpose == OrderTargetPurpose::EliminatesSort(by) => {
                    hir_plan_satisfies_order_target(
                        &result.best_plan,
                        &access_methods,
                        &input.sources,
                        self.document,
                        target,
                        schema,
                    )
                }
                _ => Ok(false),
            }
        };
        let group_and_order_sort_elided = sort_elided(EliminatesSortBy::GroupByAndOrder)?;
        let group_sort_elided =
            group_and_order_sort_elided || sort_elided(EliminatesSortBy::Group)?;
        let order_sort_elided =
            group_and_order_sort_elided || sort_elided(EliminatesSortBy::Order)?;
        let min_max_first_value = match order_target {
            Some(target) if target.is_extremum() => hir_plan_satisfies_order_target(
                &result.best_plan,
                &access_methods,
                &input.sources,
                self.document,
                target,
                schema,
            )?,
            _ => false,
        };
        let mut prior_sources = crate::translate::planner::TableMask::default();
        let mut loops: Vec<HirPlannedLoop> = Vec::with_capacity(input.sources.len());
        let mut read_out_of_order = false;
        let materialized_probes =
            materialized_hash_build_inputs(&result.best_plan, &access_methods, &constraints)?;
        // The build tables of hash joins. Like legacy, each is read in full
        // into its hash table, so its own access method is not applied and
        // its terms stay to be checked after each lookup.
        let hash_builds = result
            .best_plan
            .data
            .iter()
            .filter_map(|(_, method)| match &access_methods[*method].params {
                super::optimizer::access_method::AccessMethodParams::HashJoin {
                    build_table_idx,
                    ..
                } => Some(*build_table_idx),
                _ => None,
            })
            .collect::<Vec<_>>();
        for (source_position, access_method_position) in result.best_plan.data.iter().copied() {
            // A build table that itself probes an earlier hash table keeps
            // that lookup: its hash table is then built from a build input.
            let probes = matches!(
                access_methods[access_method_position].params,
                super::optimizer::access_method::AccessMethodParams::HashJoin { .. }
            );
            if hash_builds.contains(&source_position) && !probes {
                // EXPLAIN shows the access the build table was planned with,
                // as legacy does; it is applied to a copy of the terms, so
                // they all stay to be checked.
                let HirPlanSource::BTree(source) = &input.sources[source_position] else {
                    return Err(LimboError::InternalError(
                        "HIR hash join builds from a non-table source".to_string(),
                    ));
                };
                let mut scratch = input.predicates.clone();
                let operation = apply_hir_selected_btree_access(
                    source,
                    &constraints[source_position],
                    &mut scratch,
                    &mut access_methods[access_method_position].clone(),
                    &prior_sources,
                    source_position,
                )?;
                let definition = self.definition(source.internal_id);
                let table_lookup = selected_btree_index(&operation)
                    .filter(|index| !source.index_is_covering(definition, index))
                    .map_or(BtreeTableLookup::ScanOnly, |_| {
                        BtreeTableLookup::TableRequired
                    });
                loops.push(HirPlannedLoop {
                    source: source.internal_id,
                    source_position,
                    access: HirSourceAccess::BTree {
                        operation,
                        table_lookup,
                    },
                });
                prior_sources.set(source_position)?;
                continue;
            }
            // Like legacy's hash join, the previous loop's table is built into a
            // hash table and this table is scanned, probing it: this loop
            // comes first, and the build table's loop looks up its rows.
            if let super::optimizer::access_method::AccessMethodParams::HashJoin {
                build_table_idx,
                join_keys,
                ..
            } = &access_methods[access_method_position].params
            {
                let mut lookup = self.hash_join_lookup(&input.predicates, join_keys)?;
                let build_loop = if materialized_probes.contains(&source_position) {
                    // Like legacy's materialized build input: every loop so
                    // far runs once into a temporary table the hash table is
                    // built from, and the lookup supplies their rows.
                    let prefix = std::mem::take(&mut loops);
                    if !prefix
                        .iter()
                        .any(|planned_loop| planned_loop.source_position == *build_table_idx)
                    {
                        return Err(LimboError::InternalError(
                            "HIR hash join build is not among the loops before its probe"
                                .to_string(),
                        ));
                    }
                    let sources = prefix
                        .iter()
                        .flat_map(HirPlannedLoop::sources)
                        .collect::<Vec<_>>();
                    // The prefix checks the terms that read only its
                    // sources, and the block checks them no more.
                    let mut predicates = input.predicates.clone();
                    for (prefix_term, term) in predicates.iter_mut().zip(&mut input.predicates) {
                        if term.consumed {
                            continue;
                        }
                        let mut outside = false;
                        self.document.visit_expr_sources(&term.expr, &mut |source| {
                            outside |= !sources.contains(&source);
                        });
                        if outside {
                            prefix_term.consumed = true;
                        } else {
                            term.consumed = true;
                        }
                    }
                    lookup.build_input = Some(Box::new(HirHashBuildInput {
                        loops: prefix,
                        predicates,
                        sources,
                    }));
                    HirPlannedLoop {
                        source: input.sources[*build_table_idx].source(),
                        source_position: *build_table_idx,
                        access: HirSourceAccess::Hash(lookup),
                    }
                } else {
                    let build_loop: HirPlannedLoop = loops.pop().ok_or_else(|| {
                        LimboError::InternalError("HIR hash join has no build loop".to_string())
                    })?;
                    if build_loop.source_position != *build_table_idx {
                        return Err(LimboError::InternalError(
                            "HIR hash join build is not the previous loop".to_string(),
                        ));
                    }
                    if let HirSourceAccess::BTree {
                        operation,
                        table_lookup,
                    } = &build_loop.access
                    {
                        lookup.explain = Some((operation.clone(), *table_lookup));
                    }
                    HirPlannedLoop {
                        access: HirSourceAccess::Hash(lookup),
                        ..build_loop
                    }
                };
                for key in join_keys {
                    input.predicates[key.where_clause_idx].consumed = true;
                }
                loops.push(HirPlannedLoop {
                    source: input.sources[source_position].source(),
                    source_position,
                    access: HirSourceAccess::BTree {
                        operation: HirBtreeOperation::Scan {
                            iter_dir: crate::translate::plan::IterationDirection::Forwards,
                            index: None,
                        },
                        table_lookup: BtreeTableLookup::ScanOnly,
                    },
                });
                loops.push(build_loop);
                prior_sources.set(source_position)?;
                continue;
            }
            let access = match &input.sources[source_position] {
                HirPlanSource::BTree(source) => match &source.table {
                    Table::BTree(_) => {
                        let operation = apply_hir_selected_btree_access(
                            source,
                            &constraints[source_position],
                            &mut input.predicates,
                            &mut access_methods[access_method_position],
                            &prior_sources,
                            source_position,
                        )?;
                        // A scan may give way to one seek per branch of an
                        // OR term.
                        let union = match operation {
                            HirBtreeOperation::Scan { .. } if or_union => {
                                crate::translate::optimizer::plan_hir_or_union(
                                    self.document,
                                    &HirFromLayout::new(&input.sources, &input.groups),
                                    source,
                                    source_position,
                                    &input.predicates,
                                    &prior_sources,
                                    base_rows[source_position],
                                    schema,
                                    params,
                                )?
                            }
                            _ => None,
                        };
                        match union {
                            Some(union) => {
                                if union.covers_term {
                                    input.predicates[union.term].consumed = true;
                                }
                                // The union reads rows in rowid order, not
                                // the scan's, so an order the plan proved
                                // from this table's loop no longer holds.
                                read_out_of_order |= order_target.is_some_and(|target| {
                                    target
                                        .columns
                                        .iter()
                                        .any(|column| column.source == source.internal_id)
                                });
                                HirSourceAccess::OrUnion(union)
                            }
                            None => {
                                let definition = self.definition(source.internal_id);
                                let table_lookup = selected_btree_index(&operation)
                                    .filter(|index| !source.index_is_covering(definition, index))
                                    .map_or(BtreeTableLookup::ScanOnly, |_| {
                                        BtreeTableLookup::TableRequired
                                    });
                                HirSourceAccess::BTree {
                                    operation,
                                    table_lookup,
                                }
                            }
                        }
                    }
                    Table::Virtual(_) => {
                        HirSourceAccess::Virtual(apply_hir_selected_virtual_access(
                            &constraints[source_position],
                            &mut input.predicates,
                            &access_methods[access_method_position],
                        )?)
                    }
                    _ => {
                        return Err(LimboError::InternalError(format!(
                            "HIR catalog source {} is not a table",
                            source.internal_id
                        )))
                    }
                },
                HirPlanSource::Derived {
                    query, join_info, ..
                } => HirSourceAccess::Derived {
                    query: *query,
                    seek: crate::translate::optimizer::apply_hir_materialized_subquery_access(
                        &constraints[source_position],
                        &mut input.predicates,
                        &access_methods[access_method_position],
                        hir_outer_join_source(
                            input.sources[source_position].source(),
                            join_info.as_ref(),
                        ),
                    )?,
                },
                HirPlanSource::Cte {
                    cte,
                    query,
                    materialization,
                    join_info,
                    ..
                } => HirSourceAccess::Cte {
                    cte: *cte,
                    query: *query,
                    materialization: *materialization,
                    direction: match access_methods[access_method_position].params {
                        super::optimizer::access_method::AccessMethodParams::Subquery {
                            iter_dir,
                        } => iter_dir,
                        _ => crate::translate::plan::IterationDirection::Forwards,
                    },
                    seek: crate::translate::optimizer::apply_hir_materialized_subquery_access(
                        &constraints[source_position],
                        &mut input.predicates,
                        &access_methods[access_method_position],
                        hir_outer_join_source(
                            input.sources[source_position].source(),
                            join_info.as_ref(),
                        ),
                    )?,
                },
                HirPlanSource::RecursiveCte { cte, .. } => {
                    if !matches!(
                        access_methods[access_method_position].params,
                        super::optimizer::access_method::AccessMethodParams::Subquery { .. }
                    ) {
                        return Err(LimboError::InternalError(
                            "recursive CTE HIR source selected a non-subquery access method"
                                .to_string(),
                        ));
                    }
                    HirSourceAccess::RecursiveCte { cte: *cte }
                }
                HirPlanSource::RecursiveInput { cte, .. } => {
                    if !matches!(
                        access_methods[access_method_position].params,
                        super::optimizer::access_method::AccessMethodParams::Subquery { .. }
                    ) {
                        return Err(LimboError::InternalError(
                            "recursive CTE input selected a non-subquery access method".to_string(),
                        ));
                    }
                    HirSourceAccess::RecursiveInput { cte: *cte }
                }
            };
            loops.push(HirPlannedLoop {
                source: input.sources[source_position].source(),
                source_position,
                access,
            });
            prior_sources.set(source_position)?;
        }

        Ok(Some(HirAccessPlan {
            groups: input.groups,
            loops,
            where_start: input.where_start,
            predicates: input.predicates,
            group_sort_elided: group_sort_elided && !read_out_of_order,
            order_sort_elided: order_sort_elided && !read_out_of_order,
            min_max_first_value: min_max_first_value && !read_out_of_order,
            subquery_calls,
            output_cardinality: result.best_plan.output_cardinality,
            cost: result.best_plan.cost,
        }))
    }

    /// How many times each correlated expression subquery of the block runs,
    /// as legacy counts it for its chosen plan: for a subquery that reads
    /// sources of this block, the rows that reach the step where the last of
    /// them is joined. A subquery that reads none of them runs as often as
    /// the block, and is left out here.
    fn subquery_calls(
        &self,
        input: &HirQueryBlockPlanInput,
        plan: &crate::translate::optimizer::join::JoinN,
        access_methods: &[super::optimizer::access_method::AccessMethod],
        constraints: &[HirTableConstraints],
        initial_cardinality: f64,
        params: &CostModelParams,
    ) -> Result<Vec<(QueryId, f64)>> {
        let mut subqueries = Vec::with_capacity(input.subqueries.len());
        for query in &input.subqueries {
            let mut required = crate::translate::planner::TableMask::default();
            for source in self.document.outer_sources(*query) {
                if let Some(position) = input
                    .sources
                    .iter()
                    .position(|planned| planned.source() == source)
                {
                    required.set(position)?;
                }
            }
            let first_predicate = input.predicates.iter().position(|term| {
                let mut found = false;
                term.expr.for_each(&mut |expression| {
                    if let hir::Expr::Subquery(subquery) = expression {
                        found |= hir_subquery_query(subquery) == *query;
                    }
                });
                found
            });
            subqueries.push((*query, required, first_predicate));
        }
        crate::translate::optimizer::join::hir_subquery_calls_for_plan(
            plan,
            access_methods,
            constraints,
            &input.sources,
            &subqueries,
            input.predicates.len(),
            initial_cardinality,
            params,
        )
    }

    /// Build an ordering requirement from resolved HIR terms without reopening
    /// name resolution or copying expressions out of the document.
    pub(crate) fn order_target<'term>(
        &self,
        terms: &'term [hir::OrderTerm],
        purpose: OrderTargetPurpose,
    ) -> Option<HirOrderTarget<'term>>
    where
        'a: 'term,
    {
        let columns = terms
            .iter()
            .map(|term| self.order_column(term))
            .collect::<Option<Vec<_>>>()?;
        (!columns.is_empty()).then_some(OrderTarget { columns, purpose })
    }

    /// The order a grouped block wants its rows in, as legacy's
    /// `compute_order_target` picks it. When every ORDER BY term is a GROUP BY
    /// key, the keys in ORDER BY order and directions: rows in that order need
    /// neither the GROUP BY nor the ORDER BY sorter. Otherwise the keys in
    /// their own order, which only saves the GROUP BY sorter. Windows sort
    /// the grouped rows again, so then only the GROUP BY order helps.
    fn grouping_target<'term>(
        &self,
        block: &'term hir::QueryBlock,
        order_by: &'term [hir::OrderTerm],
    ) -> Option<HirOrderTarget<'term>>
    where
        'a: 'term,
    {
        let grouping = hir_block_grouping(block)?;
        let key_order = block
            .windows
            .is_empty()
            .then(|| group_key_order(self.document, order_by, grouping))
            .flatten();
        if let Some(key_order) = key_order {
            let columns = key_order
                .iter()
                .map(|key| {
                    self.ordering_column(
                        &grouping.keys[key.key],
                        key.order,
                        grouping.key_collations[key.key]
                            .as_ref()
                            .map(|collation| *collation.value())
                            .unwrap_or_default(),
                        key.nulls,
                    )
                })
                .collect::<Option<Vec<_>>>()?;
            return Some(OrderTarget {
                columns,
                purpose: OrderTargetPurpose::EliminatesSort(EliminatesSortBy::GroupByAndOrder),
            });
        }
        let columns = grouping
            .keys
            .iter()
            .zip(&grouping.key_collations)
            .map(|(expression, collation)| {
                self.ordering_column(
                    expression,
                    turso_parser::ast::SortOrder::Asc,
                    collation
                        .as_ref()
                        .map(|collation| *collation.value())
                        .unwrap_or_default(),
                    None,
                )
            })
            .collect::<Option<Vec<_>>>()?;
        (!columns.is_empty()).then_some(OrderTarget {
            columns,
            purpose: OrderTargetPurpose::EliminatesSort(EliminatesSortBy::Group),
        })
    }

    /// The order the innermost window layer, which reads the block's rows,
    /// sorts them in: PARTITION BY terms ascending, then ORDER BY terms, each
    /// key once, like the ORDER BY of legacy's window source subquery. Rows
    /// already in that order need no window sort. A key that a custom type
    /// sorts by its stored value is left to the sorter.
    fn window_target<'term>(&self, block: &'term hir::QueryBlock) -> Option<HirOrderTarget<'term>>
    where
        'a: 'term,
    {
        let window = block.windows.last()?;
        let collation = |collation: Option<&hir::ResolvedCollation>| {
            collation
                .map(|collation| *collation.value())
                .unwrap_or_default()
        };
        let partition = window.partition_by.iter().map(|term| {
            (
                &term.expr,
                &term.type_fact,
                turso_parser::ast::SortOrder::Asc,
                collation(term.collation.as_ref()),
                None,
            )
        });
        let order = window.order_by.iter().map(|term| {
            (
                &term.expr,
                &term.type_fact,
                term.order,
                collation(term.collation.as_ref()),
                term.nulls,
            )
        });
        let mut keys: Vec<&'term hir::Expr> = Vec::new();
        let mut columns = Vec::new();
        for (expression, type_fact, order, collation, nulls) in partition.chain(order) {
            if keys.iter().any(|key| key.equivalent(expression)) {
                continue;
            }
            let encoded = type_fact
                .declared
                .as_ref()
                .and_then(|declared| declared.custom())
                .is_some_and(|custom| custom.value().decode().is_some());
            if encoded {
                return None;
            }
            keys.push(expression);
            columns.push(self.ordering_column(expression, order, collation, nulls)?);
        }
        (!columns.is_empty()).then_some(OrderTarget {
            columns,
            purpose: OrderTargetPurpose::EliminatesSort(EliminatesSortBy::Order),
        })
    }

    fn order_column<'term>(
        &self,
        term: &'term hir::OrderTerm,
    ) -> Option<ColumnOrder<SourceId, &'term hir::Expr>>
    where
        'a: 'term,
    {
        self.ordering_column(
            &term.expr,
            term.order,
            term.collation
                .as_ref()
                .map(|collation| *collation.value())
                .unwrap_or_default(),
            term.nulls,
        )
    }

    fn ordering_column<'term>(
        &self,
        mut expression: &'term hir::Expr,
        order: turso_parser::ast::SortOrder,
        collation: crate::translate::collate::CollationSeq,
        nulls_order: Option<turso_parser::ast::NullsOrder>,
    ) -> Option<ColumnOrder<SourceId, &'term hir::Expr>>
    where
        'a: 'term,
    {
        while let hir::Expr::Output(output) = expression {
            expression = &self.document.output(*output)?.expr;
        }

        let (source, target) = match expression {
            hir::Expr::Column(column) => (column.source, ColumnTarget::Column(column.column)),
            hir::Expr::RowId(source) => (*source, ColumnTarget::RowId),
            expression => {
                let (source, _) = expression.single_source_column_usage()?;
                (source, ColumnTarget::Expr(expression))
            }
        };

        Some(ColumnOrder {
            source,
            target,
            order,
            collation,
            nulls_order,
        })
    }

    fn register_expression_index_usage(
        &self,
        sources: &mut [HirPlanSource],
        expression: &hir::Expr,
    ) -> Result<()> {
        let Some((source, columns)) = expression.single_source_column_usage() else {
            return Ok(());
        };
        let Some(planned) = sources
            .iter_mut()
            .find(|planned| planned.source() == source)
            .and_then(HirPlanSource::btree_mut)
        else {
            // Outer-query reads are planned by their owning query.
            return Ok(());
        };
        let has_expression_key = self
            .definition(source)
            .index_expressions
            .iter()
            .any(|index| index.columns.iter().any(Option::is_some) || index.predicate.is_some());
        if !has_expression_key {
            return Ok(());
        }

        let mut columns_mask = ColumnUsedMask::default();
        for column in columns {
            columns_mask.set(column)?;
        }
        planned.register_expression_index_usage(expression.clone(), columns_mask);
        Ok(())
    }
}

fn selected_btree_index(operation: &HirBtreeOperation) -> Option<&Index> {
    match operation {
        HirBtreeOperation::Scan { index, .. }
        | HirBtreeOperation::Seek { index, .. }
        | HirBtreeOperation::InSeek { index, .. } => index.as_deref(),
        HirBtreeOperation::RowidEq { .. } => None,
    }
}

/// Main-schema table sources that are materialized views, checked the way
/// legacy checks them before reading.
fn materialized_view_sources(
    document: &HirDocument,
    schema: &Schema,
) -> Result<Vec<(SourceId, Arc<Mutex<IncrementalView>>)>> {
    let mut views = Vec::new();
    for source in &document.sources {
        let hir::SourceKind::Table(table) = &source.kind else {
            continue;
        };
        if source.database.map(hir::DatabaseId::index) != Some(crate::MAIN_DB_ID) {
            continue;
        }
        let name = table.value().get_name();
        let Some(view) = schema.get_materialized_view(name) else {
            continue;
        };
        if !schema.has_compatible_dbsp_state_table(name) {
            use crate::incremental::compiler::DBSP_CIRCUIT_VERSION;
            return Err(LimboError::InternalError(format!(
                "Materialized view '{name}' has an incompatible version. \n\
                 The current version is {DBSP_CIRCUIT_VERSION}, but the view was created with a different version. \n\
                 Please DROP and recreate the view to use it."
            )));
        }
        views.push((source.id, view));
    }
    Ok(views)
}

fn hir_subquery_query(subquery: &hir::SubqueryExpr) -> QueryId {
    match subquery {
        hir::SubqueryExpr::Scalar { query, .. }
        | hir::SubqueryExpr::Row { query }
        | hir::SubqueryExpr::In { query, .. }
        | hir::SubqueryExpr::Exists(query) => *query,
    }
}

/// A non-negative literal LIMIT, as legacy reads one to cap its row
/// estimate. A negative LIMIT means no limit.
fn hir_literal_limit(limit: &hir::Expr) -> Option<f64> {
    let hir::Expr::Literal(turso_parser::ast::Literal::Numeric(text)) = limit else {
        return None;
    };
    match crate::util::parse_numeric_literal(text).ok()? {
        crate::types::Value::Numeric(crate::numeric::Numeric::Integer(value)) if value >= 0 => {
            Some(value as f64)
        }
        crate::types::Value::Numeric(crate::numeric::Numeric::Float(value)) => {
            let value: f64 = value.into();
            (value >= 0.0).then_some(value)
        }
        _ => None,
    }
}

/// `block_rows` holds the rows each block of `blocks` returns.
fn query_estimate_from_blocks(
    query: &hir::Query,
    blocks: &[HirQueryBlockPlan],
    block_rows: &[f64],
    params: &CostModelParams,
) -> HirQueryEstimate {
    let fallback = *RowCountEstimate::hardcoded_fallback(params);
    let block_rows = |block| {
        blocks
            .iter()
            .zip(block_rows)
            .find(|(plan, _)| plan.block == block)
            .map_or(fallback, |(_, rows)| *rows)
    };
    let mut output_cardinality = block_rows(query.first);
    for arm in &query.compounds {
        let rhs = block_rows(arm.block);
        output_cardinality = match arm.operator {
            turso_parser::ast::CompoundOperator::Union
            | turso_parser::ast::CompoundOperator::UnionAll => output_cardinality + rhs,
            turso_parser::ast::CompoundOperator::Except => output_cardinality,
            turso_parser::ast::CompoundOperator::Intersect => output_cardinality.min(rhs),
        };
    }

    HirQueryEstimate {
        output_cardinality,
        output_order: Vec::new(),
        cost: blocks
            .iter()
            .fold(Cost(0.0), |cost, block| cost + block.cost),
    }
}

fn append_predicates(
    predicates: &mut Vec<HirWhereTerm>,
    expression: &hir::Expr,
    from_outer_join: Option<SourceId>,
) {
    predicates.extend(
        crate::translate::plan::hir_planning_conjuncts(expression)
            .into_iter()
            .map(|expr| HirWhereTerm {
                expr,
                from_outer_join,
                consumed: false,
            }),
    );
}

fn using_equality(column: &hir::UsingColumn) -> hir::Expr {
    hir::Expr::Binary {
        lhs: column.left.clone(),
        operator: turso_parser::ast::Operator::Equals,
        rhs: Box::new(hir::Expr::Column(column.right)),
        array_concat: false,
        custom: None,
        comparison: Some(column.comparison.clone()),
    }
}

fn planned_source_from_definition(
    source: &hir::Source,
    join_info: Option<HirJoinInfo>,
    usage: &[ColumnUsage],
) -> Result<HirPlannedSource> {
    let table = match &source.kind {
        hir::SourceKind::Table(table) | hir::SourceKind::TableFunction { table, .. } => table,
        _ => {
            return Err(LimboError::InternalError(format!(
                "source {} is not a table source",
                source.id
            )))
        }
    };
    let database = source.database.ok_or_else(|| {
        LimboError::InternalError(format!("table source {} has no database", source.id))
    })?;
    let (col_used_mask, column_use_counts) = column_usage(source.id, usage)?;

    Ok(HirPlannedSource {
        op: (),
        table: table.value().clone(),
        identifier: (),
        internal_id: source.id,
        join_info,
        col_used_mask,
        column_use_counts,
        expression_index_usages: Vec::new(),
        database_id: database.index(),
        indexed: source.index_hint.clone(),
    })
}

fn column_usage(source: SourceId, usage: &[ColumnUsage]) -> Result<(ColumnUsedMask, Vec<usize>)> {
    let mut mask = ColumnUsedMask::default();
    let mut counts = Vec::new();
    for usage in usage
        .iter()
        .filter(|usage| usage.reference.source == source)
    {
        mask.set(usage.reference.column)?;
        if counts.len() <= usage.reference.column {
            counts.resize(usage.reference.column + 1, 0);
        }
        counts[usage.reference.column] = usage.count;
    }
    Ok((mask, counts))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        schema::{BTreeCharacteristics, BTreeTable, ColDef, Column, Index, Schema, Table, Type},
        sync::Arc,
        translate::collate::CollationSeq,
        translate::{
            optimizer::cost::RowCountEstimate,
            semantic::hir::{
                CatalogObject, CatalogObjectId, CatalogSnapshot, ColumnReadExpression,
                ComparisonComponent, ComparisonSemantics, CompoundArm, Cte, CteBody, CteColumn,
                CteId, DatabaseId, DatabaseSnapshot, IndexCoverage, Join, JoinConstraint, JoinKind,
                OrderTerm, Output, OutputId, Query, QueryBlock, QueryBlockBody, QueryBlockId,
                QueryId, QueryRoot, RecursiveArm, RecursiveCte, SourceColumn, SourceKind,
                SourceOwner, SubqueryExpr, TypeFact, UsingColumn,
            },
        },
        vdbe::affinity::Affinity,
    };
    use turso_parser::ast::{CompoundOperator, Materialized, NullsOrder, Operator, SortOrder};

    fn resolved_table(name: &str) -> hir::ResolvedTable {
        let columns = vec![
            Column::new(
                Some("first".to_string()),
                "INTEGER".to_string(),
                None,
                None,
                Type::Integer,
                None,
                ColDef::default(),
            ),
            Column::new(
                Some("second".to_string()),
                "TEXT".to_string(),
                None,
                None,
                Type::Text,
                None,
                ColDef::default(),
            ),
        ];
        let table = Table::BTree(Arc::new(BTreeTable::new(
            2,
            name.to_string(),
            vec![],
            columns,
            BTreeCharacteristics::HAS_ROWID,
            vec![],
            vec![],
            vec![],
            None,
        )));
        CatalogObject::new(
            CatalogObjectId::new(10),
            CatalogSnapshot::from_id(1),
            Some(DatabaseId::new(0)),
            Arc::new(table),
        )
    }

    fn source() -> hir::Source {
        hir::Source {
            id: SourceId::new(0),
            owner: SourceOwner::Root,
            database: Some(DatabaseId::new(0)),
            name: "items".to_string(),
            alias: Some("i".to_string()),
            kind: SourceKind::Table(resolved_table("items")),
            columns: vec![
                SourceColumn {
                    name: "first".to_string(),
                    type_fact: TypeFact::known(Type::Integer),
                    affinity: Affinity::Integer,
                    has_affinity: true,
                    collation: None,
                    hidden: false,
                    rowid_alias: false,
                },
                SourceColumn {
                    name: "second".to_string(),
                    type_fact: TypeFact::known(Type::Text),
                    affinity: Affinity::Text,
                    has_affinity: true,
                    collation: None,
                    hidden: false,
                    rowid_alias: false,
                },
            ],
            generated_expressions: vec![ColumnReadExpression::Absent; 2],
            default_expressions: vec![ColumnReadExpression::Absent; 2],
            column_type_programs: vec![None; 2],
            check_constraints: None,
            rowid_available: true,
            index_hint: hir::IndexHint::None,
            index_expressions: Vec::new(),
            index_coverage: IndexCoverage::Selective,
            index_method_patterns: Vec::new(),
        }
    }

    fn source_with_id(id: usize, name: &str) -> hir::Source {
        let mut source = source();
        source.id = SourceId::new(id);
        source.name = name.to_string();
        source.alias = None;
        source.kind = SourceKind::Table(resolved_table(name));
        source
    }

    fn group_source(id: usize, from: hir::From) -> hir::Source {
        hir::Source {
            id: SourceId::new(id),
            owner: SourceOwner::Root,
            database: None,
            name: format!("group-{id}"),
            alias: None,
            kind: SourceKind::FromGroup(hir::FromGroup {
                from: Box::new(from),
                columns: Vec::new(),
            }),
            columns: Vec::new(),
            generated_expressions: Vec::new(),
            default_expressions: Vec::new(),
            column_type_programs: Vec::new(),
            check_constraints: None,
            rowid_available: false,
            index_hint: hir::IndexHint::None,
            index_expressions: Vec::new(),
            index_coverage: IndexCoverage::Selective,
            index_method_patterns: Vec::new(),
        }
    }

    fn document(sources: Vec<hir::Source>) -> hir::HirDocument {
        hir::HirDocument {
            snapshot: CatalogSnapshot::from_id(1),
            databases: Vec::new(),
            root: hir::HirRoot::SchemaExpressions(hir::SchemaExpressionRoot {
                source: sources[0].id,
                expressions: Vec::new(),
            }),
            queries: Vec::new(),
            sources,
            ctes: Vec::new(),
            schema_programs: Vec::new(),
            cdc: None,
        }
    }

    fn add_query(document: &mut hir::HirDocument, block: &QueryBlock) {
        let query = block.id.query;
        document.root = hir::HirRoot::Query(QueryRoot { query });
        document.queries.push(Query {
            id: query,
            parent: None,
            captures: Vec::new(),
            reachable_ctes: Vec::new(),
            blocks: vec![block.clone()],
            first: block.id,
            compounds: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            output: block.outputs.iter().map(|output| output.id).collect(),
        });
    }

    fn binary(lhs: hir::Expr, operator: Operator, rhs: hir::Expr) -> hir::Expr {
        hir::Expr::Binary {
            lhs: Box::new(lhs),
            operator,
            rhs: Box::new(rhs),
            array_concat: false,
            custom: None,
            comparison: None,
        }
    }

    #[test]
    fn hir_order_target_resolves_outputs_and_borrows_expressions() {
        let source = SourceId::new(0);
        let query = QueryId::new(0);
        let block_id = QueryBlockId::new(query, 0);
        let output_id = OutputId::query(block_id, 0);
        let mut block = QueryBlock::new(
            block_id,
            QueryBlockBody::Select {
                distinctness: None,
                filter: None,
                grouping: None,
            },
        );
        block.outputs.push(Output {
            id: output_id,
            name: "first".to_string(),
            expr: hir::Expr::column(source, 0),
            type_fact: TypeFact::known(Type::Integer),
            affinity: Affinity::Integer,
            schema_affinity: Affinity::Integer,
            has_affinity: true,
            collation: None,
            collation_is_explicit: false,
            name_kind: hir::OutputNameKind::Inferred,
        });
        let mut document = document(vec![source_with_id(0, "items")]);
        document.root = hir::HirRoot::Query(QueryRoot { query });
        document.queries.push(Query {
            id: query,
            parent: None,
            captures: Vec::new(),
            reachable_ctes: Vec::new(),
            blocks: vec![block],
            first: block_id,
            compounds: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            output: vec![output_id],
        });

        let computed = binary(
            hir::Expr::column(source, 0),
            Operator::Add,
            hir::Expr::column(source, 1),
        );
        let collation = CatalogObject::new(
            CatalogObjectId::new(20),
            CatalogSnapshot::from_id(1),
            Some(DatabaseId::new(0)),
            Arc::new(CollationSeq::NoCase),
        );
        let terms = [
            OrderTerm {
                expr: hir::Expr::output(output_id),
                order: SortOrder::Desc,
                nulls: Some(NullsOrder::First),
                type_fact: TypeFact::known(Type::Integer),
                collation: None,
            },
            OrderTerm {
                expr: computed,
                order: SortOrder::Asc,
                nulls: None,
                type_fact: TypeFact::known(Type::Integer),
                collation: Some(collation),
            },
        ];
        let target = HirPlanContext::new(&document)
            .order_target(
                &terms,
                OrderTargetPurpose::EliminatesSort(
                    crate::translate::optimizer::order::EliminatesSortBy::Order,
                ),
            )
            .expect("resolved terms form one-source order target");

        assert!(matches!(target.columns[0].target, ColumnTarget::Column(0)));
        assert_eq!(target.columns[0].source, source);
        assert_eq!(target.columns[0].order, SortOrder::Desc);
        assert_eq!(target.columns[0].nulls_order, Some(NullsOrder::First));
        let ColumnTarget::Expr(expression) = target.columns[1].target else {
            panic!("computed HIR order term remains an expression");
        };
        assert!(std::ptr::eq(expression, &terms[1].expr));
        assert_eq!(target.columns[1].collation, CollationSeq::NoCase);
    }

    #[test]
    fn hir_source_reuses_identity_and_table_metadata() {
        let source = source();
        let SourceKind::Table(resolved) = &source.kind else {
            unreachable!();
        };
        let document = hir::HirDocument {
            snapshot: CatalogSnapshot::from_id(1),
            databases: Vec::new(),
            root: hir::HirRoot::SchemaExpressions(hir::SchemaExpressionRoot {
                source: source.id,
                expressions: Vec::new(),
            }),
            queries: Vec::new(),
            sources: vec![source.clone()],
            ctes: Vec::new(),
            schema_programs: Vec::new(),
            cdc: None,
        };
        let context = HirPlanContext::new(&document);
        let joined = context
            .source(
                source.id,
                Some(HirJoinInfo {
                    kind: hir::JoinKind::Inner,
                }),
                &[],
            )
            .expect("table source converts");

        assert_eq!(joined.internal_id, source.id);
        assert_eq!(
            joined.join_info.as_ref().unwrap().kind,
            hir::JoinKind::Inner
        );
        assert_eq!(joined.database_id, 0);
        let (Table::BTree(expected), Table::BTree(actual)) = (resolved.value(), &joined.table)
        else {
            panic!("resolved and planned tables are btree tables");
        };
        assert!(Arc::ptr_eq(expected, actual));
    }

    #[test]
    fn table_source_uses_hir_column_counts() {
        let source = source();
        let usage = [ColumnUsage {
            reference: hir::ColumnRef {
                source: source.id,
                column: 1,
            },
            count: 3,
        }];
        let joined =
            planned_source_from_definition(&source, None, &usage).expect("table source converts");

        assert_eq!(
            (&joined.col_used_mask).into_iter().collect::<Vec<_>>(),
            vec![1]
        );
        assert_eq!(joined.column_use_counts, vec![0, 3]);
    }

    #[test]
    fn table_source_preserves_index_hints_without_lookup() {
        let mut source = source();
        source.index_hint = hir::IndexHint::NotIndexed;
        let joined =
            planned_source_from_definition(&source, None, &[]).expect("table source converts");
        assert!(matches!(joined.indexed, hir::IndexHint::NotIndexed));

        let index = Index {
            name: "items_second".to_string(),
            table_name: "items".to_string(),
            root_page: 3,
            columns: Vec::new(),
            unique: false,
            ephemeral: false,
            has_rowid: true,
            where_clause: None,
            index_method: None,
            on_conflict: None,
        };
        source.index_hint = hir::IndexHint::Indexed(CatalogObject::new(
            CatalogObjectId::new(11),
            CatalogSnapshot::from_id(1),
            Some(DatabaseId::new(0)),
            Arc::new(index),
        ));
        let joined =
            planned_source_from_definition(&source, None, &[]).expect("table source converts");
        assert!(matches!(
            joined.indexed,
            hir::IndexHint::Indexed(index) if index.value().name == "items_second"
        ));
    }

    #[test]
    fn hir_covering_index_matches_frozen_expression_keys() {
        let mut source = source();
        let indexed_expression = binary(
            hir::Expr::column(source.id, 0),
            Operator::Add,
            hir::Expr::column(source.id, 1),
        );
        let index = CatalogObject::new(
            CatalogObjectId::new(11),
            CatalogSnapshot::from_id(1),
            Some(DatabaseId::new(0)),
            Arc::new(Index {
                name: "items_sum".to_string(),
                table_name: "items".to_string(),
                root_page: 3,
                columns: Vec::new(),
                unique: false,
                ephemeral: false,
                has_rowid: true,
                where_clause: None,
                index_method: None,
                on_conflict: None,
            }),
        );
        source.index_expressions.push(hir::IndexExpressions {
            index: index.clone(),
            columns: vec![Some(indexed_expression.clone())],
            predicate: None,
        });

        let usage = [
            ColumnUsage {
                reference: hir::ColumnRef {
                    source: source.id,
                    column: 0,
                },
                count: 1,
            },
            ColumnUsage {
                reference: hir::ColumnRef {
                    source: source.id,
                    column: 1,
                },
                count: 1,
            },
        ];
        let block_id = QueryBlockId::new(QueryId::new(0), 0);
        let mut block = QueryBlock::new(
            block_id,
            QueryBlockBody::Select {
                distinctness: None,
                filter: None,
                grouping: None,
            },
        );
        block.from = Some(hir::From {
            first: source.id,
            joins: Vec::new(),
        });
        block.outputs.push(hir::Output {
            id: hir::OutputId::query(block_id, 0),
            name: "sum".to_string(),
            expr: indexed_expression.clone(),
            type_fact: TypeFact::known(Type::Integer),
            affinity: Affinity::Integer,
            schema_affinity: Affinity::Integer,
            has_affinity: true,
            collation: None,
            collation_is_explicit: false,
            name_kind: hir::OutputNameKind::Inferred,
        });
        let order_by = [hir::OrderTerm {
            expr: indexed_expression,
            order: turso_parser::ast::SortOrder::Asc,
            nulls: None,
            type_fact: TypeFact::known(Type::Integer),
            collation: None,
        }];
        let document = document(vec![source.clone()]);
        let input = HirPlanContext::new(&document)
            .query_block_input(&block, &order_by, &usage)
            .expect("query block input converts");
        let mut planned = input.sources.into_iter().next().expect("source is planned");
        let HirPlanSource::BTree(planned) = &mut planned else {
            panic!("table source carries B-tree planning metadata");
        };

        // Output and ORDER BY use the same expression, so registration deduplicates them.
        assert_eq!(planned.expression_index_usages.len(), 1);

        assert!(planned.index_is_covering(&source, index.value()));

        planned.expression_index_usages[0].normalized_expr =
            Box::new(hir::Expr::column(source.id, 0));
        assert!(!planned.index_is_covering(&source, index.value()));
    }

    #[test]
    fn non_table_source_is_rejected() {
        let mut source = source();
        source.kind = SourceKind::SchemaExpression;
        let error = planned_source_from_definition(&source, None, &[])
            .expect_err("schema namespace has no planner table");
        assert_eq!(
            error.to_string(),
            "Internal error: source s0 is not a table source"
        );
    }

    #[test]
    fn query_block_input_preserves_source_order_and_right_join_predicates() {
        let left = SourceId::new(0);
        let right = SourceId::new(1);
        let document = document(vec![
            source_with_id(0, "left_items"),
            source_with_id(1, "right_items"),
        ]);
        let on = binary(
            binary(
                hir::Expr::column(left, 0),
                Operator::Equals,
                hir::Expr::column(right, 0),
            ),
            Operator::And,
            binary(
                hir::Expr::column(left, 1),
                Operator::Equals,
                hir::Expr::column(right, 1),
            ),
        );
        let filter = binary(
            hir::Expr::column(right, 0),
            Operator::NotEquals,
            hir::Expr::column(right, 1),
        );
        let mut block = QueryBlock::new(
            QueryBlockId::new(QueryId::new(0), 0),
            QueryBlockBody::Select {
                distinctness: None,
                filter: Some(filter),
                grouping: None,
            },
        );
        block.from = Some(hir::From {
            first: left,
            joins: vec![Join {
                right,
                kind: JoinKind::Right,
                constraint: JoinConstraint::On(on),
            }],
        });

        let input = HirPlanContext::new(&document)
            .query_block_input(&block, &[], &[])
            .expect("query block input converts");

        assert_eq!(
            input
                .sources
                .iter()
                .map(HirPlanSource::source)
                .collect::<Vec<_>>(),
            vec![left, right]
        );
        assert!(input.sources[0].join_info().is_none());
        let join = input.sources[1].join_info().unwrap();
        assert_eq!(join.kind, JoinKind::Right);
        assert_eq!(input.predicates.len(), 3);
        assert!(input.predicates[..2]
            .iter()
            .all(|predicate| predicate.from_outer_join == Some(right)));
        assert_eq!(input.predicates[2].from_outer_join, None);
        assert!(matches!(
            &input.predicates[0].expr,
            hir::Expr::Binary {
                operator: Operator::Equals,
                ..
            }
        ));
        assert!(matches!(
            &input.predicates[1].expr,
            hir::Expr::Binary {
                operator: Operator::Equals,
                ..
            }
        ));
        assert!(matches!(
            &input.predicates[2].expr,
            hir::Expr::Binary {
                operator: Operator::NotEquals,
                ..
            }
        ));
    }

    #[test]
    fn query_block_input_flattens_group_leaves_and_records_the_boundary() {
        let outer = SourceId::new(0);
        let inner_left = SourceId::new(1);
        let inner_right = SourceId::new(2);
        let group = SourceId::new(3);
        let inner_predicate = binary(
            hir::Expr::column(inner_left, 0),
            Operator::Equals,
            hir::Expr::column(inner_right, 0),
        );
        let outer_predicate = binary(
            hir::Expr::column(outer, 0),
            Operator::Equals,
            hir::Expr::column(inner_right, 0),
        );
        let group_from = hir::From {
            first: inner_left,
            joins: vec![Join {
                right: inner_right,
                kind: JoinKind::Inner,
                constraint: JoinConstraint::On(inner_predicate),
            }],
        };
        let document = document(vec![
            source_with_id(0, "outer_items"),
            source_with_id(1, "inner_left"),
            source_with_id(2, "inner_right"),
            group_source(3, group_from),
        ]);
        let mut block = QueryBlock::new(
            QueryBlockId::new(QueryId::new(0), 0),
            QueryBlockBody::Select {
                distinctness: None,
                filter: None,
                grouping: None,
            },
        );
        block.from = Some(hir::From {
            first: outer,
            joins: vec![Join {
                right: group,
                kind: JoinKind::Left,
                constraint: JoinConstraint::On(outer_predicate),
            }],
        });

        let input = HirPlanContext::new(&document)
            .query_block_input(&block, &[], &[])
            .expect("FROM group produces recursive planner input");

        assert_eq!(
            input
                .sources
                .iter()
                .map(HirPlanSource::source)
                .collect::<Vec<_>>(),
            vec![outer, inner_left, inner_right]
        );
        let [boundary] = input.groups.as_slice() else {
            panic!("one FROM-group boundary is recorded");
        };
        assert_eq!(boundary.source, group);
        assert_eq!(boundary.parent, None);
        assert_eq!(boundary.source_range, 1..3);
        assert_eq!(boundary.join_info.as_ref().unwrap().kind, JoinKind::Left);
        assert!(input.sources[1].join_info().is_none());
        assert_eq!(input.sources[2].join_info().unwrap().kind, JoinKind::Inner);
        assert_eq!(input.predicates.len(), 2);
        assert!(input
            .predicates
            .iter()
            .all(|predicate| predicate.from_outer_join == Some(group)));

        let layout = HirFromLayout::new(&input.sources, &input.groups);
        let mask = layout
            .table_mask(&document, &input.predicates[0].expr)
            .expect("group predicate has a physical source mask");
        assert!(!mask.get(0));
        assert!(mask.get(1));
        assert!(mask.get(2));
        assert!(!layout.outer_join_may_null_extend(outer));
        assert!(layout.outer_join_may_null_extend(inner_left));
        assert!(layout.outer_join_may_null_extend(inner_right));
        assert!(!layout.full_join_may_null_extend(inner_left));
    }

    #[test]
    fn owned_hir_plan_plans_parenthesized_from_group() {
        let outer = SourceId::new(0);
        let inner_left = SourceId::new(1);
        let inner_right = SourceId::new(2);
        let group = SourceId::new(3);
        let group_from = hir::From {
            first: inner_left,
            joins: vec![Join {
                right: inner_right,
                kind: JoinKind::Inner,
                constraint: JoinConstraint::None,
            }],
        };
        let mut document = document(vec![
            source_with_id(0, "outer_items"),
            source_with_id(1, "inner_left"),
            source_with_id(2, "inner_right"),
            group_source(3, group_from),
        ]);
        let mut block = QueryBlock::new(
            QueryBlockId::new(QueryId::new(0), 0),
            QueryBlockBody::Select {
                distinctness: None,
                filter: None,
                grouping: None,
            },
        );
        block.from = Some(hir::From {
            first: outer,
            joins: vec![Join {
                right: group,
                kind: JoinKind::Left,
                constraint: JoinConstraint::On(binary(
                    hir::Expr::column(outer, 0),
                    Operator::Equals,
                    hir::Expr::column(inner_right, 0),
                )),
            }],
        });
        add_query(&mut document, &block);

        let plan = HirPlan::build(
            Arc::new(document),
            &Schema::default(),
            &CostModelParams::default(),
        )
        .expect("parenthesized FROM group plans from HIR");
        let [block_plan] = plan
            .planned_query(block.id.query)
            .expect("query is planned")
            .blocks
            .as_slice()
        else {
            panic!("query has one block");
        };

        assert_eq!(block_plan.loops.len(), 3);
        assert_eq!(block_plan.groups.len(), 1);
        assert_eq!(block_plan.groups[0].source, group);
        assert_eq!(block_plan.groups[0].source_range, 1..3);
        assert_eq!(block_plan.loops[0].source, outer);
        assert!(block_plan.loops[1..]
            .iter()
            .all(|planned| planned.source == inner_left || planned.source == inner_right));
        assert_eq!(block_plan.predicates.len(), 1);
        assert_eq!(block_plan.predicates[0].from_outer_join, Some(group));
        assert!(!block_plan.predicates[0].consumed);
    }

    #[test]
    fn query_block_input_records_nested_group_ranges() {
        let last = SourceId::new(0);
        let first = SourceId::new(1);
        let second = SourceId::new(2);
        let inner_group = SourceId::new(3);
        let outer_group = SourceId::new(4);
        let inner_from = hir::From {
            first,
            joins: vec![Join {
                right: second,
                kind: JoinKind::Inner,
                constraint: JoinConstraint::None,
            }],
        };
        let outer_from = hir::From {
            first: inner_group,
            joins: vec![Join {
                right: last,
                kind: JoinKind::Cross,
                constraint: JoinConstraint::None,
            }],
        };
        let document = document(vec![
            source_with_id(0, "last"),
            source_with_id(1, "first"),
            source_with_id(2, "second"),
            group_source(3, inner_from),
            group_source(4, outer_from),
        ]);
        let mut block = QueryBlock::new(
            QueryBlockId::new(QueryId::new(0), 0),
            QueryBlockBody::Select {
                distinctness: None,
                filter: None,
                grouping: None,
            },
        );
        block.from = Some(hir::From {
            first: outer_group,
            joins: Vec::new(),
        });

        let input = HirPlanContext::new(&document)
            .query_block_input(&block, &[], &[])
            .expect("nested FROM groups produce recursive planner input");

        assert_eq!(
            input
                .sources
                .iter()
                .map(HirPlanSource::source)
                .collect::<Vec<_>>(),
            vec![first, second, last]
        );
        let inner = input
            .groups
            .iter()
            .find(|boundary| boundary.source == inner_group)
            .expect("inner boundary exists");
        assert_eq!(inner.parent, Some(outer_group));
        assert_eq!(inner.source_range, 0..2);
        let outer = input
            .groups
            .iter()
            .find(|boundary| boundary.source == outer_group)
            .expect("outer boundary exists");
        assert_eq!(outer.parent, None);
        assert_eq!(outer.source_range, 0..3);
    }

    #[test]
    fn hir_from_layout_keeps_nested_right_and_full_join_scope_local() {
        let sources = (0..4)
            .map(|id| {
                HirPlanSource::BTree(
                    planned_source_from_definition(
                        &source_with_id(id, &format!("source_{id}")),
                        None,
                        &[],
                    )
                    .expect("table source converts"),
                )
            })
            .collect::<Vec<_>>();
        let outer_group = SourceId::new(10);
        let nested_group = SourceId::new(11);
        let boundaries = |kind| {
            vec![
                HirFromGroupBoundary {
                    source: nested_group,
                    parent: Some(outer_group),
                    source_range: 2..4,
                    join_info: Some(HirJoinInfo { kind }),
                },
                HirFromGroupBoundary {
                    source: outer_group,
                    parent: None,
                    source_range: 1..4,
                    join_info: None,
                },
            ]
        };

        let right_groups = boundaries(JoinKind::Right);
        let right = HirFromLayout::new(&sources, &right_groups);
        assert!(!right.outer_join_may_null_extend(SourceId::new(0)));
        assert!(right.outer_join_may_null_extend(SourceId::new(1)));
        assert!(!right.outer_join_may_null_extend(SourceId::new(2)));
        assert!(!right.outer_join_may_null_extend(SourceId::new(3)));
        assert!(!(0..4).any(|id| right.full_join_may_null_extend(SourceId::new(id))));

        let full_groups = boundaries(JoinKind::Full);
        let full = HirFromLayout::new(&sources, &full_groups);
        assert!(!full.full_join_may_null_extend(SourceId::new(0)));
        for id in 1..4 {
            assert!(full.full_join_may_null_extend(SourceId::new(id)));
        }
    }

    #[test]
    fn query_block_input_adds_table_function_predicates_with_join_ownership() {
        let left = SourceId::new(0);
        let function = SourceId::new(1);
        let mut function_source = source_with_id(1, "table_function");
        let argument_predicate = binary(
            hir::Expr::column(function, 1),
            Operator::Equals,
            hir::Expr::column(left, 0),
        );
        let SourceKind::Table(table) = function_source.kind else {
            unreachable!();
        };
        function_source.kind = SourceKind::TableFunction {
            table,
            argument_predicates: vec![argument_predicate],
        };
        let document = document(vec![source_with_id(0, "items"), function_source]);
        let mut block = QueryBlock::new(
            QueryBlockId::new(QueryId::new(0), 0),
            QueryBlockBody::Select {
                distinctness: None,
                filter: None,
                grouping: None,
            },
        );
        block.from = Some(hir::From {
            first: left,
            joins: vec![Join {
                right: function,
                kind: JoinKind::Left,
                constraint: JoinConstraint::None,
            }],
        });

        let input = HirPlanContext::new(&document)
            .query_block_input(&block, &[], &[])
            .expect("table-function source converts");

        assert_eq!(input.sources.len(), 2);
        assert_eq!(input.sources[1].source(), function);
        assert_eq!(input.predicates.len(), 1);
        assert!(matches!(
            &input.predicates[0].expr,
            hir::Expr::Binary { lhs, operator: Operator::Equals, rhs, .. }
                if matches!(lhs.as_ref(), hir::Expr::Column(column) if *column == hir::ColumnRef { source: function, column: 1 })
                    && matches!(rhs.as_ref(), hir::Expr::Column(column) if *column == hir::ColumnRef { source: left, column: 0 })
        ));
        assert_eq!(input.predicates[0].from_outer_join, Some(function));
        assert!(!input.predicates[0].consumed);
    }

    #[test]
    fn query_block_input_builds_using_equality_from_resolved_columns() {
        let left = SourceId::new(0);
        let right = SourceId::new(1);
        let mut document = document(vec![
            source_with_id(0, "left_items"),
            source_with_id(1, "right_items"),
        ]);
        let comparison = ComparisonSemantics {
            components: vec![ComparisonComponent {
                affinity: Affinity::Integer,
                collation: None,
                array: false,
            }],
        };
        let mut block = QueryBlock::new(
            QueryBlockId::new(QueryId::new(0), 0),
            QueryBlockBody::Select {
                distinctness: None,
                filter: None,
                grouping: None,
            },
        );
        block.from = Some(hir::From {
            first: left,
            joins: vec![Join {
                right,
                kind: JoinKind::Left,
                constraint: JoinConstraint::Using(vec![UsingColumn {
                    name: "first".to_string(),
                    left: Box::new(hir::Expr::column(left, 0)),
                    right: hir::ColumnRef {
                        source: right,
                        column: 0,
                    },
                    value: hir::MergedColumnValue::Left,
                    type_fact: TypeFact::known(Type::Integer),
                    affinity: Affinity::Integer,
                    has_affinity: true,
                    collation: None,
                    comparison: comparison.clone(),
                }]),
            }],
        });

        let usage = [
            ColumnUsage {
                reference: hir::ColumnRef {
                    source: left,
                    column: 0,
                },
                count: 1,
            },
            ColumnUsage {
                reference: hir::ColumnRef {
                    source: right,
                    column: 0,
                },
                count: 1,
            },
        ];

        add_query(&mut document, &block);

        let context = HirPlanContext::new(&document);
        let input = context
            .query_block_input(&block, &[], &usage)
            .expect("query block input converts");

        assert_eq!(input.sources[1].join_info().unwrap().kind, JoinKind::Left);
        assert_eq!(input.predicates.len(), 1);
        assert_eq!(input.predicates[0].from_outer_join, Some(right));
        let hir::Expr::Binary {
            lhs,
            operator,
            rhs,
            comparison: actual_comparison,
            ..
        } = &input.predicates[0].expr
        else {
            panic!("USING becomes equality predicate");
        };
        assert_eq!(*operator, Operator::Equals);
        assert!(matches!(
            lhs.as_ref(),
            hir::Expr::Column(column) if *column == hir::ColumnRef { source: left, column: 0 }
        ));
        assert!(matches!(
            rhs.as_ref(),
            hir::Expr::Column(column) if *column == hir::ColumnRef { source: right, column: 0 }
        ));
        assert_eq!(actual_comparison.as_ref(), Some(&comparison));

        let params = CostModelParams::default();
        let mut schema = Schema::default();
        schema.analyze_stats.table_stats_mut("left_items").row_count = Some(1_000);
        schema
            .analyze_stats
            .table_stats_mut("right_items")
            .row_count = Some(1);
        let base_rows = input
            .sources
            .iter()
            .map(|source| {
                base_row_estimate(
                    &schema,
                    &source.btree().expect("test source is a table").table,
                    &params,
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            base_rows,
            [
                RowCountEstimate::AnalyzeStats(1_000.0),
                RowCountEstimate::AnalyzeStats(1.0),
            ]
        );
        let query_id = block.id.query;
        let document = Arc::new(document);
        let query_plan = HirPlan::build(document.clone(), &schema, &params)
            .expect("HIR query planning succeeds");
        assert!(Arc::ptr_eq(&query_plan.document, &document));
        let planned_query = query_plan
            .planned_query(query_id)
            .expect("root query is planned");
        let [plan] = planned_query.blocks.as_slice() else {
            panic!("ordinary SELECT has one planned block");
        };
        assert_eq!(plan.block, block.id);
        assert!(plan.output_cardinality > 0.0);
        assert!(plan.cost.0 >= 0.0);
        let [left_loop, right_loop] = plan.loops.as_slice() else {
            panic!("two sources produce two planned loops");
        };
        assert_eq!(left_loop.source, left);
        assert_eq!(left_loop.source_position, 0);
        assert_eq!(right_loop.source, right);
        assert_eq!(right_loop.source_position, 1);
        // Like legacy, a USING join never hashes: the right source seeks an
        // automatic index on the join equality.
        assert!(
            matches!(
                &right_loop.access,
                HirSourceAccess::BTree {
                    operation: crate::translate::optimizer::HirBtreeOperation::Seek {
                        index: Some(index),
                        ..
                    },
                    ..
                } if index.ephemeral
            ),
            "right source seeks an automatic index"
        );
        assert!(plan.predicates[0].consumed);
    }

    #[test]
    fn source_less_query_block_keeps_predicates_without_access_loops() {
        let mut document = document(vec![source()]);
        let block = QueryBlock::new(
            QueryBlockId::new(QueryId::new(0), 0),
            QueryBlockBody::Select {
                distinctness: None,
                filter: Some(hir::Expr::Literal(turso_parser::ast::Literal::Numeric(
                    "1".into(),
                ))),
                grouping: None,
            },
        );
        add_query(&mut document, &block);
        let plan = HirPlanContext::new(&document)
            .plan_query_block(
                &block,
                &[],
                None,
                None,
                7.0,
                &Schema::default(),
                &CostModelParams::default(),
                &|_| None,
            )
            .expect("source-less query block plans");

        assert!(plan.loops.is_empty());
        assert_eq!(plan.predicates.len(), 1);
        assert!(!plan.predicates[0].consumed);
        assert_eq!(plan.output_cardinality, 7.0);
        assert_eq!(plan.cost, Cost(0.0));
    }

    #[test]
    fn owned_hir_plan_plans_values_block() {
        let mut document = document(vec![source()]);
        let block = QueryBlock::new(
            QueryBlockId::new(QueryId::new(0), 0),
            QueryBlockBody::Values {
                rows: vec![vec![hir::Expr::Literal(
                    turso_parser::ast::Literal::Numeric("1".into()),
                )]],
            },
        );
        add_query(&mut document, &block);
        let document = Arc::new(document);

        let plan = HirPlan::build(
            document.clone(),
            &Schema::default(),
            &CostModelParams::default(),
        )
        .expect("VALUES query plans from HIR");

        assert!(Arc::ptr_eq(&plan.document, &document));
        let query = plan
            .planned_query(block.id.query)
            .expect("root query is planned");
        assert_eq!(query.blocks.len(), 1);
        assert_eq!(query.blocks[0].block, block.id);
        assert!(query.blocks[0].loops.is_empty());
        assert!(query.blocks[0].predicates.is_empty());
        assert_eq!(query.blocks[0].output_cardinality, 1.0);
        assert_eq!(query.blocks[0].cost, Cost(0.0));
    }

    #[test]
    fn owned_hir_plan_plans_compound_arms_without_arm_ordering() {
        let query_id = QueryId::new(0);
        let first_id = QueryBlockId::new(query_id, 0);
        let second_id = QueryBlockId::new(query_id, 1);
        let value = || QueryBlockBody::Values {
            rows: vec![vec![hir::Expr::Literal(
                turso_parser::ast::Literal::Numeric("1".into()),
            )]],
        };
        let first = QueryBlock::new(first_id, value());
        let second = QueryBlock::new(second_id, value());
        let mut document = document(vec![source()]);
        document.root = hir::HirRoot::Query(QueryRoot { query: query_id });
        document.queries.push(Query {
            id: query_id,
            parent: None,
            captures: Vec::new(),
            reachable_ctes: Vec::new(),
            blocks: vec![first, second],
            first: first_id,
            compounds: vec![CompoundArm {
                operator: CompoundOperator::UnionAll,
                block: second_id,
            }],
            order_by: vec![OrderTerm {
                expr: hir::Expr::Literal(turso_parser::ast::Literal::Numeric("1".into())),
                order: SortOrder::Desc,
                nulls: None,
                type_fact: TypeFact::known(Type::Integer),
                collation: None,
            }],
            limit: None,
            output: Vec::new(),
        });
        let document = Arc::new(document);

        let plan = HirPlan::build(
            document.clone(),
            &Schema::default(),
            &CostModelParams::default(),
        )
        .expect("compound query arms plan from HIR");

        let planned_query = plan.planned_query(query_id).expect("root query is planned");
        assert_eq!(
            planned_query
                .blocks
                .iter()
                .map(|block| block.block)
                .collect::<Vec<_>>(),
            [first_id, second_id]
        );
        assert_eq!(planned_query.output_cardinality, 2.0);
        let query = plan.document.query(query_id).expect("query remains owned");
        assert_eq!(query.compounds.len(), 1);
        assert_eq!(query.order_by.len(), 1);
    }

    #[test]
    fn owned_hir_plan_plans_query_dependencies_before_their_owner() {
        let root_query = QueryId::new(0);
        let child_query = QueryId::new(1);
        let root_block_id = QueryBlockId::new(root_query, 0);
        let child_block_id = QueryBlockId::new(child_query, 0);
        let source_id = SourceId::new(0);

        let mut root_block = QueryBlock::new(
            root_block_id,
            QueryBlockBody::Select {
                distinctness: None,
                filter: Some(hir::Expr::Binary {
                    lhs: Box::new(hir::Expr::column(source_id, 0)),
                    operator: Operator::Equals,
                    rhs: Box::new(hir::Expr::Literal(turso_parser::ast::Literal::Numeric(
                        "1".into(),
                    ))),
                    array_concat: false,
                    custom: None,
                    comparison: Some(ComparisonSemantics {
                        components: vec![ComparisonComponent {
                            affinity: Affinity::Integer,
                            collation: None,
                            array: false,
                        }],
                    }),
                }),
                grouping: None,
            },
        );
        root_block.from = Some(hir::From {
            first: source_id,
            joins: Vec::new(),
        });
        let mut child_block = QueryBlock::new(
            child_block_id,
            QueryBlockBody::Values {
                rows: vec![
                    vec![hir::Expr::Literal(turso_parser::ast::Literal::Numeric(
                        "1".into(),
                    ))],
                    vec![hir::Expr::Literal(turso_parser::ast::Literal::Numeric(
                        "2".into(),
                    ))],
                ],
            },
        );
        child_block.outputs.push(Output {
            id: OutputId::query(child_block_id, 0),
            name: "column1".to_string(),
            expr: hir::Expr::Literal(turso_parser::ast::Literal::Numeric("1".into())),
            type_fact: TypeFact::known(Type::Integer),
            affinity: Affinity::Integer,
            schema_affinity: Affinity::Integer,
            has_affinity: false,
            collation: None,
            collation_is_explicit: false,
            name_kind: hir::OutputNameKind::Inferred,
        });
        let query = |id, parent, block: QueryBlock| {
            let output = block.outputs.iter().map(|output| output.id).collect();
            Query {
                id,
                parent,
                captures: Vec::new(),
                reachable_ctes: Vec::new(),
                first: block.id,
                blocks: vec![block],
                compounds: Vec::new(),
                order_by: Vec::new(),
                limit: None,
                output,
            }
        };
        let mut derived_source = source();
        derived_source.id = source_id;
        derived_source.owner = SourceOwner::QueryBlock(root_block_id);
        derived_source.database = None;
        derived_source.kind = SourceKind::Derived(child_query);
        derived_source.columns.truncate(1);
        derived_source.generated_expressions.truncate(1);
        derived_source.default_expressions.truncate(1);
        derived_source.column_type_programs.truncate(1);
        let mut document = document(vec![derived_source]);
        document.root = hir::HirRoot::Query(QueryRoot { query: root_query });
        document.queries = vec![
            query(root_query, None, root_block),
            query(child_query, Some(root_query), child_block),
        ];
        document.validate().expect("derived query HIR is valid");
        let plan = HirPlan::build(
            Arc::new(document),
            &Schema::default(),
            &CostModelParams::default(),
        )
        .expect("derived query plans from HIR");

        let child = plan
            .planned_query(child_query)
            .expect("child query is planned");
        assert_eq!(child.output_cardinality, 2.0);
        let root = plan
            .planned_query(root_query)
            .expect("root query is planned");
        assert_eq!(root.output_cardinality, 0.2);
        let [source_loop] = root.blocks[0].loops.as_slice() else {
            panic!("derived source produces one scan loop");
        };
        assert_eq!(source_loop.source, source_id);
        assert_eq!(source_loop.source_position, 0);
        assert!(matches!(
            source_loop.access,
            HirSourceAccess::Derived { query, .. } if query == child_query
        ));
        assert!(root.cost.0 > child.cost.0);

        let mut subquery_document = plan.document.as_ref().clone();
        let subquery = QueryId::new(subquery_document.queries.len());
        let subquery_block = QueryBlockId::new(subquery, 0);
        let mut subquery_definition = subquery_document.queries[child_query.index()].clone();
        subquery_definition.id = subquery;
        subquery_definition.parent = Some(root_query);
        subquery_definition.first = subquery_block;
        subquery_definition.blocks[0].id = subquery_block;
        subquery_definition.blocks[0].outputs[0].id = OutputId::query(subquery_block, 0);
        subquery_definition.output = vec![OutputId::query(subquery_block, 0)];
        subquery_document.queries.push(subquery_definition);
        let QueryBlockBody::Select { filter, .. } =
            &mut subquery_document.queries[root_query.index()].blocks[0].body
        else {
            panic!("root query is SELECT");
        };
        *filter = Some(hir::Expr::Subquery(SubqueryExpr::Exists(subquery)));
        subquery_document
            .validate()
            .expect("expression subquery HIR is valid");
        let subquery_plan = HirPlan::build(
            Arc::new(subquery_document),
            &Schema::default(),
            &CostModelParams::default(),
        )
        .expect("expression subquery plans from HIR");
        assert!(subquery_plan.planned_query(subquery).is_some());
        assert!(subquery_plan.planned_query(root_query).is_some());

        let cte_id = CteId::new(0);
        let mut cte_document = plan.document.as_ref().clone();
        cte_document.sources[source_id.index()].kind = SourceKind::Cte(cte_id);
        cte_document.queries[root_query.index()].reachable_ctes = vec![cte_id];
        let source_column = &cte_document.sources[source_id.index()].columns[0];
        cte_document.ctes.push(Cte {
            id: cte_id,
            name: "numbers".to_string(),
            columns: vec![CteColumn {
                name: source_column.name.clone(),
                type_fact: source_column.type_fact.clone(),
                affinity: source_column.affinity,
                has_affinity: source_column.has_affinity,
                collation: source_column.collation.clone(),
            }],
            materialized: Materialized::Yes,
            body: CteBody::Query(child_query),
        });
        cte_document
            .validate()
            .expect("non-recursive CTE query HIR is valid");
        let materialization =
            HirPlanContext::new(&cte_document).cte_materialization(root_query, cte_id, child_query);
        assert_eq!(materialization, HirCteMaterialization::Explicit);
        let cte_plan = HirPlan::build(
            Arc::new(cte_document.clone()),
            &Schema::default(),
            &CostModelParams::default(),
        )
        .expect("non-recursive CTE source plans from HIR");
        let cte_root = cte_plan
            .planned_query(root_query)
            .expect("CTE root query is planned");
        assert_eq!(cte_root.output_cardinality, 0.2);
        let [cte_loop] = cte_root.blocks[0].loops.as_slice() else {
            panic!("CTE source produces one scan loop");
        };
        assert!(matches!(
            cte_loop.access,
            HirSourceAccess::Cte {
                cte,
                query,
                materialization: HirCteMaterialization::Explicit,
                ..
            } if cte == cte_id && query == child_query
        ));

        let mut recursive_document = cte_plan.document.as_ref().clone();
        let arm_query = QueryId::new(recursive_document.queries.len());
        let arm_block_id = QueryBlockId::new(arm_query, 0);
        let input_source = SourceId::new(recursive_document.sources.len());
        let mut input_definition = recursive_document.sources[source_id.index()].clone();
        input_definition.id = input_source;
        input_definition.owner = SourceOwner::QueryBlock(arm_block_id);
        input_definition.name = "numbers".to_string();
        input_definition.kind = SourceKind::RecursiveInput(cte_id);
        recursive_document.sources.push(input_definition);

        let mut arm_block = QueryBlock::new(
            arm_block_id,
            QueryBlockBody::Select {
                distinctness: None,
                filter: None,
                grouping: None,
            },
        );
        arm_block.from = Some(hir::From {
            first: input_source,
            joins: Vec::new(),
        });
        let mut arm_output =
            recursive_document.queries[child_query.index()].blocks[0].outputs[0].clone();
        arm_output.id = OutputId::query(arm_block_id, 0);
        arm_output.expr = hir::Expr::column(input_source, 0);
        arm_block.outputs.push(arm_output);
        recursive_document.queries.push(Query {
            id: arm_query,
            parent: Some(root_query),
            captures: Vec::new(),
            reachable_ctes: vec![cte_id],
            first: arm_block_id,
            blocks: vec![arm_block],
            compounds: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            output: vec![OutputId::query(arm_block_id, 0)],
        });
        recursive_document.ctes[cte_id.index()].body = CteBody::Recursive(RecursiveCte {
            seed: child_query,
            arms: vec![RecursiveArm {
                operator: CompoundOperator::UnionAll,
                query: arm_query,
            }],
            input_sources: vec![input_source],
            comparison_collations: vec![None],
            queue_order: Vec::new(),
            limit: None,
        });
        recursive_document
            .validate()
            .expect("recursive CTE source HIR is valid");
        let recursive_plan = HirPlan::build(
            Arc::new(recursive_document),
            &Schema::default(),
            &CostModelParams::default(),
        )
        .expect("recursive CTE source plans from HIR");
        assert!(recursive_plan.planned_query(child_query).is_some());
        let recursive_arm = recursive_plan
            .planned_query(arm_query)
            .expect("recursive arm query is planned");
        assert!(matches!(
            recursive_arm.blocks[0].loops[0].access,
            HirSourceAccess::RecursiveInput { cte } if cte == cte_id
        ));
        let recursive_root = recursive_plan
            .planned_query(root_query)
            .expect("recursive CTE owner query is planned");
        assert!(matches!(
            recursive_root.blocks[0].loops[0].access,
            HirSourceAccess::RecursiveCte { cte } if cte == cte_id
        ));

        cte_document.ctes[cte_id.index()].materialized = Materialized::Any;
        let materialization =
            HirPlanContext::new(&cte_document).cte_materialization(root_query, cte_id, child_query);
        assert_eq!(materialization, HirCteMaterialization::PerReference);

        let second_cte_source = SourceId::new(cte_document.sources.len());
        let mut second_reference = cte_document.sources[source_id.index()].clone();
        second_reference.id = second_cte_source;
        cte_document.sources.push(second_reference);
        cte_document.queries[root_query.index()].blocks[0]
            .from
            .as_mut()
            .expect("root query has a CTE source")
            .joins
            .push(Join {
                right: second_cte_source,
                kind: JoinKind::Inner,
                constraint: JoinConstraint::None,
            });
        cte_document.ctes[cte_id.index()].materialized = Materialized::No;
        cte_document
            .validate()
            .expect("two references to a NOT MATERIALIZED CTE are valid");

        let mut correlated_document = cte_document.clone();
        let outer_source = SourceId::new(correlated_document.sources.len());
        let mut outer_definition = source_with_id(outer_source.index(), "outer_items");
        outer_definition.owner = SourceOwner::QueryBlock(root_block_id);
        correlated_document.sources.push(outer_definition);
        correlated_document.databases.push(DatabaseSnapshot {
            database: DatabaseId::new(0),
            schema_version: 0,
        });
        correlated_document.queries[root_query.index()].blocks[0]
            .from
            .as_mut()
            .expect("root query has CTE references")
            .joins
            .push(Join {
                right: outer_source,
                kind: JoinKind::Inner,
                constraint: JoinConstraint::None,
            });
        correlated_document.queries[child_query.index()].blocks[0].outputs[0].expr =
            hir::Expr::column(outer_source, 0);
        correlated_document.queries[child_query.index()].captures = vec![outer_source];
        correlated_document
            .validate()
            .expect("correlated CTE body HIR is valid");
        assert_eq!(
            HirPlanContext::new(&correlated_document).cte_materialization(
                root_query,
                cte_id,
                child_query,
            ),
            HirCteMaterialization::PerReference,
        );

        let shared_plan = HirPlan::build(
            Arc::new(cte_document),
            &Schema::default(),
            &CostModelParams::default(),
        )
        .expect("repeated CTE references plan from HIR");
        assert_eq!(
            shared_plan
                .queries
                .iter()
                .filter(|planned| planned.query == child_query)
                .count(),
            1
        );
        assert!(shared_plan
            .planned_query(root_query)
            .expect("shared CTE root query is planned")
            .blocks[0]
            .loops
            .iter()
            .all(|source_loop| matches!(
                source_loop.access,
                HirSourceAccess::Cte {
                    materialization: HirCteMaterialization::Shared,
                    ..
                }
            )));

        let table_source_id = SourceId::new(1);
        let mut table_source = source_with_id(1, "items");
        table_source.owner = SourceOwner::QueryBlock(root_block_id);
        let mut mixed_document = plan.document.as_ref().clone();
        mixed_document.databases.push(DatabaseSnapshot {
            database: DatabaseId::new(0),
            schema_version: 0,
        });
        mixed_document.sources.push(table_source);
        let root_block = &mut mixed_document.queries[root_query.index()].blocks[0];
        root_block.from = Some(hir::From {
            first: table_source_id,
            joins: vec![Join {
                right: source_id,
                kind: JoinKind::Inner,
                constraint: JoinConstraint::On(hir::Expr::Binary {
                    lhs: Box::new(hir::Expr::column(table_source_id, 0)),
                    operator: Operator::Equals,
                    rhs: Box::new(hir::Expr::column(source_id, 0)),
                    array_concat: false,
                    custom: None,
                    comparison: Some(ComparisonSemantics {
                        components: vec![ComparisonComponent {
                            affinity: Affinity::Integer,
                            collation: None,
                            array: false,
                        }],
                    }),
                }),
            }],
        });
        mixed_document
            .validate()
            .expect("mixed table and derived query HIR is valid");
        let mixed_plan = HirPlan::build(
            Arc::new(mixed_document),
            &Schema::default(),
            &CostModelParams::default(),
        )
        .expect("table and derived source join plans from HIR");
        let mixed_root = mixed_plan
            .planned_query(root_query)
            .expect("mixed root query is planned");
        assert_eq!(mixed_root.blocks[0].loops.len(), 2);
        assert!(mixed_root.blocks[0].loops.iter().any(|source_loop| {
            source_loop.source == table_source_id
                && matches!(source_loop.access, HirSourceAccess::BTree { .. })
        }));
        assert!(mixed_root.blocks[0].loops.iter().any(|source_loop| {
            source_loop.source == source_id
                && matches!(
                    source_loop.access,
                    HirSourceAccess::Derived { query, .. } if query == child_query
                )
        }));
    }
}
