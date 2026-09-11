//! Remove dependent joins with the rules of Neumann and Kemper, "Unnesting
//! Arbitrary Queries" (BTW 2015), section 3.2.
//!
//! The raise step puts each correlated scalar subquery `S` of a block under a
//! dependent join with the join tree `T1` of the block. The rules below then
//! rewrite the tree:
//!
//! - `IntroduceDomain`: `T1 ⋈dep S` becomes `T1 ⟕ (D ⋈dep S)`, joined back on
//!   the domain columns with `IS`. `D` is the set of distinct values of the
//!   outer columns that `S` reads, computed from a copy of `T1` and of the
//!   filter terms of the block that are safe to copy. The columns of `S` that
//!   named `T1` now name `D`.
//! - `PushDependentJoinThroughProject`: `D ⋈dep Π(X)` is `Π ∪ A(D) (D ⋈dep X)`.
//! - `PushDependentJoinThroughAggregate`: `D ⋈dep Γ(X)` is `Γ ∪ A(D) (D ⋈dep X)`.
//! - `PushDependentJoinThroughFilter`: `D ⋈dep σ(X)` is `σ(D ⋈dep X)`.
//! - `PushDependentJoinThroughDistinct`: `D` is a set, so the distinct step
//!   moves above the join.
//! - `PushDependentJoinThroughJoin`: `D ⋈dep (X ⋈ Y)` is `(D ⋈dep X) ⋈ Y` when
//!   `Y` does not read `D`. The tables of a block form a left-deep chain of
//!   nested loops, so a join term of `Y` can read `D` from an outer loop. The
//!   paper replicates `D` on both sides for that case; this model does not
//!   need it.
//! - `DependentJoinToJoin`: `D ⋈dep X` is `D ⋈ X` when `X` does not read `D`.
//!
//! A scalar aggregate gives one row for an empty input, but a group-by gives
//! none. The join back is a left join, and a `count` value gets `coalesce`,
//! as CockroachDB does in `TryDecorrelateScalarGroupBy`.
//!
//! `IntroduceDomain` applies when:
//!
//! - the subquery is one aggregate value with no `GROUP BY`, `HAVING`,
//!   `DISTINCT`, `ORDER BY`, `LIMIT` other than 1, window, or nested subquery;
//! - its joins are inner or left joins, and no correlated term is an outer
//!   join term;
//! - it reads only columns of the block's own join tree, and that tree can be
//!   copied: base tables, virtual tables, and derived tables without
//!   subqueries;
//! - the block reads no table of a query outside it and has no `FULL JOIN`;
//! - the subquery value is not used inside an outer join term of the block;
//! - no inner WHERE term can fail on its input, such as `json_extract` on
//!   text that is not JSON. The join optimizer can run such a term on inner
//!   rows before the correlation term, and the original query never runs it on
//!   those rows;
//! - the value for an empty group is known.
//!
//! When an outer WHERE term cannot be copied into `D`, the domain can hold
//! values that no outer row reaches, and the aggregate runs for them too. The
//! rule then applies only when the aggregate cannot fail on extra input.
//!
//! A dependent join that no rule removes goes back to the prepared form when
//! the tree is lowered.

use rustc_hash::FxHashMap as HashMap;
use turso_parser::ast::{self, Expr, SortOrder, TableInternalId};

use crate::function::AggFunc;
use crate::numeric::Numeric;
use crate::schema::Table;
use crate::translate::expr::{
    expr_contains_nondeterministic_scalar_function, expr_references_any_subquery,
    expression_can_fail_on_input, walk_expr, walk_expr_mut, WalkControl,
};
use crate::translate::logical::walk::{
    collect_table_ids, filter_node, filter_slot, join_tree, leaf_ids,
};
use crate::translate::logical::{
    Aggregate, Block, DependentJoin, DependentJoinKind, DerivedTable, Distinct, Filter, Join,
    LogicalPlan, Project, Scan,
};
use crate::translate::optimizer::unnest::{
    coalesce_with_zero, is_null_on_empty_input, EmptyInputValue,
};
use crate::translate::plan::{
    self as plan, Distinctness, GroupBy, JoinInfo, JoinType, Operation, QueryDestination,
    ResultSetColumn, WhereTerm,
};
use crate::types::Value;
use crate::util::parse_signed_number;
use crate::Result;

use super::{Rule, RuleContext};

/// Apply `rewrite` to the first node, parents first, that it accepts. Nested
/// blocks are not visited.
fn rewrite_first(
    node: &mut LogicalPlan,
    rewrite: &mut impl FnMut(&mut LogicalPlan) -> Result<bool>,
) -> Result<bool> {
    if rewrite(node)? {
        return Ok(true);
    }
    match node {
        LogicalPlan::Join(join) => {
            Ok(rewrite_first(&mut join.left, rewrite)? || rewrite_first(&mut join.right, rewrite)?)
        }
        LogicalPlan::DependentJoin(join) => {
            Ok(rewrite_first(&mut join.left, rewrite)? || rewrite_first(&mut join.right, rewrite)?)
        }
        LogicalPlan::OneRow | LogicalPlan::Scan(_) | LogicalPlan::DerivedTable(_) => Ok(false),
        node => match node.input_mut() {
            Some(input) => rewrite_first(input, rewrite),
            None => Ok(false),
        },
    }
}

/// Apply `rewrite` until no node accepts it. Return whether any node did.
fn rewrite_all(
    root: &mut LogicalPlan,
    rewrite: &mut impl FnMut(&mut LogicalPlan) -> Result<bool>,
) -> Result<bool> {
    let mut changed = false;
    while rewrite_first(root, rewrite)? {
        changed = true;
    }
    Ok(changed)
}

fn domain_column(domain_id: TableInternalId, position: usize) -> Expr {
    Expr::Column {
        database: None,
        table: domain_id,
        column: position,
        is_rowid_alias: false,
    }
}

fn subtree_reads_table(node: &LogicalPlan, table_id: TableInternalId) -> bool {
    let mut found = false;
    node.for_each_expr(&mut |expr| {
        let mut tables = Vec::new();
        collect_table_ids(expr, &mut tables);
        found |= tables.contains(&table_id);
    });
    found
}

/// Take a domain join out of a node. The node becomes a placeholder.
fn take_domain_join(node: &mut LogicalPlan) -> Option<(DependentJoin, TableInternalId, usize)> {
    let LogicalPlan::DependentJoin(join) = node else {
        return None;
    };
    let DependentJoinKind::Domain {
        domain_id,
        column_count,
    } = join.kind
    else {
        return None;
    };
    let LogicalPlan::DependentJoin(join) = std::mem::replace(node, LogicalPlan::OneRow) else {
        unreachable!("checked: the node is a dependent join")
    };
    Some((join, domain_id, column_count))
}

pub(crate) struct PushDependentJoinThroughProject;

impl Rule for PushDependentJoinThroughProject {
    fn name(&self) -> &'static str {
        "PushDependentJoinThroughProject"
    }

    fn apply(&self, block: &mut Block, _: &mut RuleContext<'_, '_>) -> Result<bool> {
        rewrite_all(&mut block.root, &mut |node| {
            if !matches!(node, LogicalPlan::DependentJoin(join)
                if matches!(join.kind, DependentJoinKind::Domain { .. })
                    && matches!(*join.right, LogicalPlan::Project(_)))
            {
                return Ok(false);
            }
            let (join, domain_id, column_count) =
                take_domain_join(node).expect("checked: the node is a domain join");
            let LogicalPlan::Project(mut project) = *join.right else {
                unreachable!("checked: the right side is a projection")
            };
            project
                .columns
                .extend((0..column_count).map(|position| ResultSetColumn {
                    expr: domain_column(domain_id, position),
                    alias: Some(format!("k{position}")),
                    implicit_column_name: None,
                    contains_aggregates: false,
                }));
            let input = std::mem::replace(&mut project.input, Box::new(LogicalPlan::OneRow));
            project.input = Box::new(LogicalPlan::DependentJoin(DependentJoin {
                left: join.left,
                right: input,
                kind: join.kind,
            }));
            *node = LogicalPlan::Project(project);
            Ok(true)
        })
    }
}

pub(crate) struct PushDependentJoinThroughAggregate;

impl Rule for PushDependentJoinThroughAggregate {
    fn name(&self) -> &'static str {
        "PushDependentJoinThroughAggregate"
    }

    fn apply(&self, block: &mut Block, _: &mut RuleContext<'_, '_>) -> Result<bool> {
        rewrite_all(&mut block.root, &mut |node| {
            if !matches!(node, LogicalPlan::DependentJoin(join)
                if matches!(join.kind, DependentJoinKind::Domain { .. })
                    && matches!(*join.right, LogicalPlan::Aggregate(_)))
            {
                return Ok(false);
            }
            let (join, domain_id, column_count) =
                take_domain_join(node).expect("checked: the node is a domain join");
            let LogicalPlan::Aggregate(mut aggregate) = *join.right else {
                unreachable!("checked: the right side is an aggregate")
            };
            let group_by = aggregate.group_by.get_or_insert_with(|| GroupBy {
                exprs: Vec::new(),
                sort_order: Vec::new(),
                nulls_order: Vec::new(),
                sort_elided: false,
                having: None,
            });
            group_by
                .exprs
                .extend((0..column_count).map(|position| domain_column(domain_id, position)));
            group_by
                .sort_order
                .extend(std::iter::repeat_n(SortOrder::Asc, column_count));
            group_by
                .nulls_order
                .extend(std::iter::repeat_n(None, column_count));
            let input = std::mem::replace(&mut aggregate.input, Box::new(LogicalPlan::OneRow));
            aggregate.input = Box::new(LogicalPlan::DependentJoin(DependentJoin {
                left: join.left,
                right: input,
                kind: join.kind,
            }));
            *node = LogicalPlan::Aggregate(aggregate);
            Ok(true)
        })
    }
}

pub(crate) struct PushDependentJoinThroughFilter;

impl Rule for PushDependentJoinThroughFilter {
    fn name(&self) -> &'static str {
        "PushDependentJoinThroughFilter"
    }

    fn apply(&self, block: &mut Block, _: &mut RuleContext<'_, '_>) -> Result<bool> {
        rewrite_all(&mut block.root, &mut |node| {
            if !matches!(node, LogicalPlan::DependentJoin(join)
                if matches!(join.kind, DependentJoinKind::Domain { .. })
                    && matches!(*join.right, LogicalPlan::Filter(_)))
            {
                return Ok(false);
            }
            let (join, _, _) = take_domain_join(node).expect("checked: the node is a domain join");
            let LogicalPlan::Filter(mut filter) = *join.right else {
                unreachable!("checked: the right side is a filter")
            };
            let input = std::mem::replace(&mut filter.input, Box::new(LogicalPlan::OneRow));
            filter.input = Box::new(LogicalPlan::DependentJoin(DependentJoin {
                left: join.left,
                right: input,
                kind: join.kind,
            }));
            *node = LogicalPlan::Filter(filter);
            Ok(true)
        })
    }
}

pub(crate) struct PushDependentJoinThroughDistinct;

impl Rule for PushDependentJoinThroughDistinct {
    fn name(&self) -> &'static str {
        "PushDependentJoinThroughDistinct"
    }

    fn apply(&self, block: &mut Block, _: &mut RuleContext<'_, '_>) -> Result<bool> {
        rewrite_all(&mut block.root, &mut |node| {
            if !matches!(node, LogicalPlan::DependentJoin(join)
                if matches!(join.kind, DependentJoinKind::Domain { .. })
                    && matches!(*join.right, LogicalPlan::Distinct(_)))
            {
                return Ok(false);
            }
            let (join, _, _) = take_domain_join(node).expect("checked: the node is a domain join");
            let LogicalPlan::Distinct(mut distinct) = *join.right else {
                unreachable!("checked: the right side is a distinct step")
            };
            let input = std::mem::replace(&mut distinct.input, Box::new(LogicalPlan::OneRow));
            distinct.input = Box::new(LogicalPlan::DependentJoin(DependentJoin {
                left: join.left,
                right: input,
                kind: join.kind,
            }));
            *node = LogicalPlan::Distinct(distinct);
            Ok(true)
        })
    }
}

pub(crate) struct PushDependentJoinThroughJoin;

impl Rule for PushDependentJoinThroughJoin {
    fn name(&self) -> &'static str {
        "PushDependentJoinThroughJoin"
    }

    fn apply(&self, block: &mut Block, _: &mut RuleContext<'_, '_>) -> Result<bool> {
        rewrite_all(&mut block.root, &mut |node| {
            let accepts = match node {
                LogicalPlan::DependentJoin(join) => match (&join.kind, join.right.as_ref()) {
                    (DependentJoinKind::Domain { domain_id, .. }, LogicalPlan::Join(inner)) => {
                        !subtree_reads_table(&inner.right, *domain_id)
                    }
                    _ => false,
                },
                _ => false,
            };
            if !accepts {
                return Ok(false);
            }
            let (join, _, _) = take_domain_join(node).expect("checked: the node is a domain join");
            let LogicalPlan::Join(mut inner) = *join.right else {
                unreachable!("checked: the right side is a join")
            };
            let left = std::mem::replace(&mut inner.left, Box::new(LogicalPlan::OneRow));
            inner.left = Box::new(LogicalPlan::DependentJoin(DependentJoin {
                left: join.left,
                right: left,
                kind: join.kind,
            }));
            *node = LogicalPlan::Join(inner);
            Ok(true)
        })
    }
}

pub(crate) struct DependentJoinToJoin;

impl Rule for DependentJoinToJoin {
    fn name(&self) -> &'static str {
        "DependentJoinToJoin"
    }

    fn apply(&self, block: &mut Block, _: &mut RuleContext<'_, '_>) -> Result<bool> {
        rewrite_all(&mut block.root, &mut |node| {
            let accepts = match node {
                LogicalPlan::DependentJoin(join) => match (&join.kind, join.right.as_ref()) {
                    (
                        DependentJoinKind::Domain { domain_id, .. },
                        LogicalPlan::Scan(_) | LogicalPlan::DerivedTable(_),
                    ) => !subtree_reads_table(&join.right, *domain_id),
                    _ => false,
                },
                _ => false,
            };
            if !accepts {
                return Ok(false);
            }
            let (join, _, _) = take_domain_join(node).expect("checked: the node is a domain join");
            *node = LogicalPlan::Join(Join {
                left: join.left,
                right: join.right,
                info: JoinInfo {
                    join_type: JoinType::Inner,
                    using: vec![],
                    no_reorder: false,
                },
            });
            Ok(true)
        })
    }
}

pub(crate) struct IntroduceDomain;

impl Rule for IntroduceDomain {
    fn name(&self) -> &'static str {
        "IntroduceDomain"
    }

    fn apply(&self, block: &mut Block, context: &mut RuleContext<'_, '_>) -> Result<bool> {
        let mut changed = false;
        while let Some(analysis) = find_scalar_join_to_unnest(block, context)? {
            introduce_domain(block, analysis, context)?;
            changed = true;
        }
        Ok(changed)
    }
}

struct Analysis {
    subquery_id: TableInternalId,
    /// The outer columns the subquery reads, in first-use order.
    domain: Vec<Expr>,
    empty_value: EmptyInputValue,
}

/// Return the analysis of the first scalar dependent join that the rule can
/// rewrite.
fn find_scalar_join_to_unnest(
    block: &Block,
    context: &RuleContext<'_, '_>,
) -> Result<Option<Analysis>> {
    if block
        .outer_query_refs
        .iter()
        .any(|reference| reference.is_used())
    {
        return Ok(None);
    }
    let mut has_full_join = false;
    join_tree(&block.root).for_each_node(&mut |node| {
        if let LogicalPlan::Join(join) = node {
            has_full_join |= join.info.is_full_outer();
        }
    });
    if has_full_join {
        return Ok(None);
    }
    let mut joins = Vec::new();
    collect_scalar_joins(join_tree(&block.root), &mut joins);
    for join in joins {
        if let Some(analysis) = analyze_scalar_join(block, join, context)? {
            return Ok(Some(analysis));
        }
    }
    Ok(None)
}

fn collect_scalar_joins<'a>(node: &'a LogicalPlan, out: &mut Vec<&'a DependentJoin>) {
    if let LogicalPlan::DependentJoin(join) = node {
        if matches!(join.kind, DependentJoinKind::Scalar { .. }) {
            out.push(join);
        }
        collect_scalar_joins(&join.left, out);
    }
}

fn analyze_scalar_join(
    block: &Block,
    join: &DependentJoin,
    context: &RuleContext<'_, '_>,
) -> Result<Option<Analysis>> {
    let DependentJoinKind::Scalar {
        subquery,
        duplicates,
        ..
    } = &join.kind
    else {
        return Ok(None);
    };
    let LogicalPlan::DerivedTable(derived) = join.right.as_ref() else {
        return Ok(None);
    };
    if !subtree_can_be_copied(&join.left) {
        return Ok(None);
    }
    let outer_ids = leaf_ids(&join.left);
    let inner = &derived.block;
    if !inner.subqueries.is_empty() {
        return Ok(None);
    }
    let used_outer: Vec<TableInternalId> = inner
        .outer_query_refs
        .iter()
        .filter(|reference| reference.is_used())
        .map(|reference| reference.internal_id)
        .collect();
    if used_outer.is_empty() || used_outer.iter().any(|id| !outer_ids.contains(id)) {
        return Ok(None);
    }
    if inner
        .outer_query_refs
        .iter()
        .any(|reference| reference.is_used() && reference.scope_depth != 0)
    {
        return Ok(None);
    }
    let Some(shape) = scalar_aggregate_shape(&inner.root) else {
        return Ok(None);
    };
    if shape
        .terms
        .iter()
        .any(|term| expression_can_fail_on_input(&term.expr))
    {
        return Ok(None);
    }
    if shape
        .terms
        .iter()
        .any(|term| term.from_outer_join.is_some() && references_any(&term.expr, &outer_ids))
    {
        return Ok(None);
    }
    let Some(empty_value) = value_on_empty_input(&shape.value.expr, shape.aggregates) else {
        return Ok(None);
    };

    let mut value_ids = vec![subquery.internal_id];
    value_ids.extend(
        duplicates
            .iter()
            .map(|(_, duplicate)| duplicate.internal_id),
    );
    let mut value_used = false;
    let mut value_in_outer_join_term = false;
    if let Some(filter) = filter_node(&block.root) {
        for term in &filter.terms {
            if references_any(&term.expr, &value_ids) {
                value_used = true;
                value_in_outer_join_term |= term.from_outer_join.is_some();
            }
        }
    }
    block
        .root
        .for_each_expr(&mut |expr| value_used |= references_any(expr, &value_ids));
    if !value_used || value_in_outer_join_term {
        return Ok(None);
    }

    let mut domain = Vec::new();
    inner
        .root
        .for_each_expr(&mut |expr| collect_domain_columns(expr, &outer_ids, &mut domain));
    let domain_is_exact = filter_node(&block.root).is_none_or(|filter| {
        filter.terms.iter().all(|term| {
            references_any(&term.expr, &value_ids)
                || term_can_be_copied(&term.expr, &outer_ids, context).unwrap_or(false)
        })
    });
    if !domain_is_exact && !aggregates_can_run_for_unused_rows(&shape) {
        return Ok(None);
    }
    Ok(Some(Analysis {
        subquery_id: subquery.internal_id,
        domain,
        empty_value,
    }))
}

/// The parts of a subquery that returns one aggregate value.
struct ScalarAggregateShape<'a> {
    value: &'a ResultSetColumn,
    aggregates: &'a [plan::Aggregate],
    terms: &'a [WhereTerm],
}

fn scalar_aggregate_shape(root: &LogicalPlan) -> Option<ScalarAggregateShape<'_>> {
    let node = match root {
        LogicalPlan::Limit(limit) => {
            if limit.offset.is_some() || !limit_is_one(limit.limit.as_deref()) {
                return None;
            }
            limit.input.as_ref()
        }
        node => node,
    };
    let LogicalPlan::Project(project) = node else {
        return None;
    };
    if project.columns.len() != 1 {
        return None;
    }
    let LogicalPlan::Aggregate(aggregate) = project.input.as_ref() else {
        return None;
    };
    if aggregate.aggregates.is_empty()
        || aggregate
            .group_by
            .as_ref()
            .is_some_and(|group_by| !group_by.exprs.is_empty() || group_by.having.is_some())
    {
        return None;
    }
    let (joins, terms): (&LogicalPlan, &[WhereTerm]) = match aggregate.input.as_ref() {
        LogicalPlan::Filter(filter) => (&filter.input, &filter.terms),
        node => (node, &[]),
    };
    let mut joins_are_simple = true;
    let mut has_table = false;
    joins.for_each_node(&mut |node| match node {
        LogicalPlan::Join(join) => {
            joins_are_simple &=
                matches!(join.info.join_type, JoinType::Inner | JoinType::LeftOuter);
        }
        LogicalPlan::Scan(scan) => {
            has_table = true;
            joins_are_simple &= match &scan.table.table {
                Table::BTree(_) | Table::Virtual(_) => true,
                Table::FromClauseSubquery(subquery) => !plan::plan_is_correlated(&subquery.plan),
                Table::RecursiveCteInput(_) => false,
            };
        }
        LogicalPlan::DerivedTable(derived) => {
            has_table = true;
            joins_are_simple &= !derived
                .block
                .outer_query_refs
                .iter()
                .any(|reference| reference.is_used());
        }
        _ => joins_are_simple = false,
    });
    if !joins_are_simple || !has_table {
        return None;
    }
    Some(ScalarAggregateShape {
        value: &project.columns[0],
        aggregates: &aggregate.aggregates,
        terms,
    })
}

fn limit_is_one(limit: Option<&Expr>) -> bool {
    match limit {
        None => true,
        Some(limit) => matches!(
            parse_signed_number(limit),
            Ok(Value::Numeric(Numeric::Integer(1)))
        ),
    }
}

/// The value of the subquery for no input rows, if it is known.
fn value_on_empty_input(expr: &Expr, aggregates: &[plan::Aggregate]) -> Option<EmptyInputValue> {
    for aggregate in aggregates {
        if !crate::util::exprs_are_equivalent(expr, &aggregate.original_expr) {
            continue;
        }
        return match aggregate.func {
            AggFunc::Count | AggFunc::Count0 => Some(EmptyInputValue::IntegerZero),
            AggFunc::Total => Some(EmptyInputValue::RealZero),
            AggFunc::Avg
            | AggFunc::GroupConcat
            | AggFunc::Max
            | AggFunc::Min
            | AggFunc::StringAgg
            | AggFunc::Sum => Some(EmptyInputValue::Null),
            _ => None,
        };
    }
    is_null_on_empty_input(expr, aggregates).then_some(EmptyInputValue::Null)
}

/// Whether the aggregates can run on rows that no outer row asks for.
fn aggregates_can_run_for_unused_rows(shape: &ScalarAggregateShape<'_>) -> bool {
    shape.aggregates.iter().all(|aggregate| {
        matches!(
            aggregate.func,
            AggFunc::Avg
                | AggFunc::Count
                | AggFunc::Count0
                | AggFunc::Max
                | AggFunc::Min
                | AggFunc::Total
        )
    }) && !shape
        .aggregates
        .iter()
        .flat_map(|aggregate| aggregate.args.iter().chain(aggregate.filter_expr.iter()))
        .any(expression_can_fail_on_input)
}

/// Whether an outer WHERE term can run inside the domain table. A term that
/// can fail stays out, because the outer query can skip it for a row that an
/// earlier term rejects.
fn term_can_be_copied(
    expr: &Expr,
    outer_ids: &[TableInternalId],
    context: &RuleContext<'_, '_>,
) -> Result<bool> {
    let mut tables = Vec::new();
    collect_table_ids(expr, &mut tables);
    Ok(tables.iter().all(|id| outer_ids.contains(id))
        && !expr_references_any_subquery(expr)
        && !expression_can_fail_on_input(expr)
        && !expr_contains_nondeterministic_scalar_function(expr, context.resolver)?)
}

fn references_any(expr: &Expr, ids: &[TableInternalId]) -> bool {
    let mut tables = Vec::new();
    collect_table_ids(expr, &mut tables);
    tables.iter().any(|id| ids.contains(id))
}

fn collect_domain_columns(expr: &Expr, outer_ids: &[TableInternalId], domain: &mut Vec<Expr>) {
    walk_expr(expr, &mut |expr: &Expr| -> Result<WalkControl> {
        match expr {
            Expr::Column { table, .. } | Expr::RowId { table, .. } if outer_ids.contains(table) => {
                if !domain.iter().any(|known| known == expr) {
                    domain.push(expr.clone());
                }
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })
    .expect("walking an expression cannot fail");
}

/// Whether a subtree can be copied with fresh table ids.
fn subtree_can_be_copied(node: &LogicalPlan) -> bool {
    let mut can_copy = true;
    node.for_each_node(&mut |node| match node {
        LogicalPlan::Scan(scan) => {
            can_copy &= matches!(scan.table.table, Table::BTree(_) | Table::Virtual(_));
        }
        LogicalPlan::DerivedTable(derived) => {
            can_copy &= derived.shell.is_none() || derived.block.subqueries.is_empty();
            can_copy &= block_can_be_copied(&derived.block);
        }
        LogicalPlan::DependentJoin(_) => can_copy = false,
        _ => {}
    });
    can_copy
}

fn block_can_be_copied(block: &Block) -> bool {
    block.subqueries.is_empty() && subtree_can_be_copied(&block.root)
}

/// Rewrite one scalar dependent join into a left join with the grouped
/// subquery, and a domain join inside the subquery.
fn introduce_domain(
    block: &mut Block,
    analysis: Analysis,
    context: &mut RuleContext<'_, '_>,
) -> Result<()> {
    let Analysis {
        subquery_id,
        domain,
        empty_value,
    } = analysis;
    let domain_id = context.ids.next();

    let mut taken: Option<DependentJoin> = None;
    rewrite_first(&mut block.root, &mut |node| {
        let is_target = matches!(node, LogicalPlan::DependentJoin(join)
            if matches!(&join.kind, DependentJoinKind::Scalar { subquery, .. }
                if subquery.internal_id == subquery_id));
        if !is_target {
            return Ok(false);
        }
        let LogicalPlan::DependentJoin(join) = std::mem::replace(node, LogicalPlan::OneRow) else {
            unreachable!("checked: the node is the target join")
        };
        taken = Some(join);
        Ok(true)
    })?;
    let join = taken.expect("the analysis found the join");
    let DependentJoin { left, right, kind } = join;
    let DependentJoinKind::Scalar {
        subquery: _,
        duplicates,
        ..
    } = kind
    else {
        unreachable!("checked: the join is a scalar join")
    };
    let LogicalPlan::DerivedTable(mut derived) = *right else {
        unreachable!("checked: the right side is a subquery")
    };
    let outer_ids = leaf_ids(&left);

    let safe_terms: Vec<&WhereTerm> = match filter_node(&block.root) {
        Some(filter) => filter
            .terms
            .iter()
            .filter(|term| term_can_be_copied(&term.expr, &outer_ids, context).unwrap_or(false))
            .collect(),
        None => Vec::new(),
    };
    let domain_table =
        build_domain_table(&left, &safe_terms, &domain, domain_id, subquery_id, context)?;

    replace_outer_columns(&mut derived.block.root, &domain, domain_id);
    derived.block.outer_query_refs.clear();
    derived.block.query_destination = QueryDestination::placeholder_for_subquery();
    let inner_root = std::mem::replace(&mut derived.block.root, LogicalPlan::OneRow);
    let inner_root = match inner_root {
        LogicalPlan::Limit(limit) => *limit.input,
        node => node,
    };
    derived.block.root = LogicalPlan::DependentJoin(DependentJoin {
        left: Box::new(LogicalPlan::DerivedTable(domain_table)),
        right: Box::new(inner_root),
        kind: DependentJoinKind::Domain {
            domain_id,
            column_count: domain.len(),
        },
    });

    let join_terms: Vec<WhereTerm> = domain
        .iter()
        .enumerate()
        .map(|(position, outer_column)| WhereTerm {
            expr: Expr::Binary(
                Box::new(Expr::Column {
                    database: None,
                    table: subquery_id,
                    column: position + 1,
                    is_rowid_alias: false,
                }),
                ast::Operator::Is,
                Box::new(outer_column.clone()),
            ),
            from_outer_join: Some(subquery_id),
            consumed: false,
        })
        .collect();

    let mut placed = false;
    rewrite_first(&mut block.root, &mut |node| {
        if !matches!(node, LogicalPlan::OneRow) || placed {
            return Ok(false);
        }
        placed = true;
        Ok(true)
    })?;
    put_join(
        &mut block.root,
        LogicalPlan::Join(Join {
            left,
            right: Box::new(LogicalPlan::DerivedTable(derived)),
            info: JoinInfo {
                join_type: JoinType::LeftOuter,
                using: vec![],
                no_reorder: false,
            },
        }),
    );
    filter_slot(&mut block.root).terms.extend(join_terms);

    let mut value_ids = vec![subquery_id];
    value_ids.extend(
        duplicates
            .iter()
            .map(|(_, duplicate)| duplicate.internal_id),
    );
    name_bare_value_columns(block, &value_ids);
    let value = domain_column(subquery_id, 0);
    let replacement = match empty_value {
        EmptyInputValue::Null => value,
        EmptyInputValue::IntegerZero => coalesce_with_zero(value, "0"),
        EmptyInputValue::RealZero => coalesce_with_zero(value, "0.0"),
    };
    block.for_each_expr_mut(&mut |expr| {
        walk_expr_mut(expr, &mut |expr: &mut Expr| -> Result<WalkControl> {
            if let Expr::Column {
                table, column: 0, ..
            } = expr
            {
                if value_ids.contains(table) {
                    *expr = replacement.clone();
                    return Ok(WalkControl::SkipChildren);
                }
            }
            Ok(WalkControl::Continue)
        })
        .map(|_| ())
    })?;
    Ok(())
}

/// Put a join where `introduce_domain` left its placeholder.
fn put_join(root: &mut LogicalPlan, replacement: LogicalPlan) {
    let mut replacement = Some(replacement);
    rewrite_first(root, &mut |node| {
        if !matches!(node, LogicalPlan::OneRow) {
            return Ok(false);
        }
        *node = replacement.take().expect("one placeholder in the tree");
        Ok(true)
    })
    .expect("placing a join cannot fail");
    assert!(replacement.is_none(), "the placeholder must be in the tree");
}

/// A result column that is only the subquery value keeps the subquery text
/// as its name.
fn name_bare_value_columns(block: &mut Block, value_ids: &[TableInternalId]) {
    let mut node = &mut block.root;
    loop {
        match node {
            LogicalPlan::Project(project) => {
                for column in &mut project.columns {
                    if column.alias.is_some() {
                        continue;
                    }
                    let Expr::Column {
                        table, column: 0, ..
                    } = &column.expr
                    else {
                        continue;
                    };
                    if value_ids.contains(table) {
                        column.alias.clone_from(&column.implicit_column_name);
                    }
                }
                return;
            }
            LogicalPlan::Limit(next) => node = &mut next.input,
            LogicalPlan::Sort(next) => node = &mut next.input,
            LogicalPlan::Distinct(next) => node = &mut next.input,
            _ => return,
        }
    }
}

/// `SELECT DISTINCT <domain columns> FROM <copy of the outer join tree>
/// WHERE <copies of the safe outer terms>`.
fn build_domain_table(
    outer: &LogicalPlan,
    safe_terms: &[&WhereTerm],
    domain: &[Expr],
    domain_id: TableInternalId,
    subquery_id: TableInternalId,
    context: &mut RuleContext<'_, '_>,
) -> Result<DerivedTable> {
    let mut id_map: HashMap<TableInternalId, TableInternalId> = HashMap::default();
    let joins = copy_subtree(outer, context, &mut id_map);
    let terms: Vec<WhereTerm> = safe_terms
        .iter()
        .map(|term| {
            let mut expr = term.expr.clone();
            remap_tables(&mut expr, &id_map);
            WhereTerm {
                expr,
                from_outer_join: term.from_outer_join.map(|id| id_map[&id]),
                consumed: false,
            }
        })
        .collect();
    let input = if terms.is_empty() {
        joins
    } else {
        LogicalPlan::Filter(Filter {
            input: Box::new(joins),
            terms,
        })
    };
    let columns = domain
        .iter()
        .enumerate()
        .map(|(position, outer_column)| {
            let mut expr = outer_column.clone();
            remap_tables(&mut expr, &id_map);
            ResultSetColumn {
                expr,
                alias: Some(format!("k{position}")),
                implicit_column_name: None,
                contains_aggregates: false,
            }
        })
        .collect();
    let root = LogicalPlan::Distinct(Distinct {
        input: Box::new(LogicalPlan::Project(Project {
            input: Box::new(input),
            columns,
        })),
        distinctness: Distinctness::Distinct { ctx: None },
    });
    Ok(DerivedTable {
        identifier: format!("domain_{subquery_id}"),
        internal_id: domain_id,
        shell: None,
        value_without_collation: false,
        block: Box::new(new_block(root)),
    })
}

fn new_block(root: LogicalPlan) -> Block {
    Block {
        root,
        subqueries: Vec::new(),
        outer_query_refs: Vec::new(),
        right_join_swapped: false,
        query_destination: QueryDestination::placeholder_for_subquery(),
        input_cardinality_hint: None,
        phantom_params: Vec::new(),
    }
}

/// Replace the outer columns of a subquery with the columns of its domain.
fn replace_outer_columns(node: &mut LogicalPlan, domain: &[Expr], domain_id: TableInternalId) {
    node.for_each_expr_mut(&mut |expr| {
        walk_expr_mut(expr, &mut |expr: &mut Expr| -> Result<WalkControl> {
            if let Some(position) = domain.iter().position(|known| known == expr) {
                *expr = domain_column(domain_id, position);
                return Ok(WalkControl::SkipChildren);
            }
            Ok(WalkControl::Continue)
        })
        .map(|_| ())
    })
    .expect("replacing outer columns cannot fail");
}

/// Copy a subtree with fresh table ids. `subtree_can_be_copied` must hold.
fn copy_subtree(
    node: &LogicalPlan,
    context: &mut RuleContext<'_, '_>,
    id_map: &mut HashMap<TableInternalId, TableInternalId>,
) -> LogicalPlan {
    match node {
        LogicalPlan::OneRow => LogicalPlan::OneRow,
        LogicalPlan::Scan(scan) => {
            let mut table = scan.table.clone();
            let new_id = context.ids.next();
            id_map.insert(table.internal_id, new_id);
            table.internal_id = new_id;
            table.op = Operation::default_scan_for(&table.table);
            table.plan_estimate = None;
            table.join_info = None;
            LogicalPlan::Scan(Scan { table })
        }
        LogicalPlan::DerivedTable(derived) => {
            let mut nested: HashMap<TableInternalId, TableInternalId> = id_map.clone();
            let block = copy_block(&derived.block, context, &mut nested);
            let new_id = context.ids.next();
            id_map.insert(derived.internal_id, new_id);
            LogicalPlan::DerivedTable(DerivedTable {
                identifier: derived.identifier.clone(),
                internal_id: new_id,
                shell: None,
                value_without_collation: derived.value_without_collation,
                block: Box::new(block),
            })
        }
        LogicalPlan::Join(join) => LogicalPlan::Join(Join {
            left: Box::new(copy_subtree(&join.left, context, id_map)),
            right: Box::new(copy_subtree(&join.right, context, id_map)),
            info: join.info.clone(),
        }),
        LogicalPlan::DependentJoin(_) => {
            unreachable!("checked: a copied subtree holds no dependent join")
        }
        LogicalPlan::Filter(filter) => {
            let input = copy_subtree(&filter.input, context, id_map);
            let terms = filter
                .terms
                .iter()
                .map(|term| {
                    let mut expr = term.expr.clone();
                    remap_tables(&mut expr, id_map);
                    WhereTerm {
                        expr,
                        from_outer_join: term.from_outer_join.map(|id| id_map[&id]),
                        consumed: false,
                    }
                })
                .collect();
            LogicalPlan::Filter(Filter {
                input: Box::new(input),
                terms,
            })
        }
        LogicalPlan::Aggregate(aggregate) => {
            let input = copy_subtree(&aggregate.input, context, id_map);
            let mut group_by = aggregate.group_by.clone();
            if let Some(group_by) = &mut group_by {
                group_by
                    .exprs
                    .iter_mut()
                    .for_each(|expr| remap_tables(expr, id_map));
                if let Some(having) = &mut group_by.having {
                    having
                        .iter_mut()
                        .for_each(|expr| remap_tables(expr, id_map));
                }
            }
            let mut aggregates = aggregate.aggregates.clone();
            for aggregate in &mut aggregates {
                remap_tables(&mut aggregate.original_expr, id_map);
                aggregate
                    .args
                    .iter_mut()
                    .for_each(|expr| remap_tables(expr, id_map));
                if let Some(filter) = &mut aggregate.filter_expr {
                    remap_tables(filter, id_map);
                }
            }
            LogicalPlan::Aggregate(Aggregate {
                input: Box::new(input),
                group_by,
                aggregates,
            })
        }
        LogicalPlan::Project(project) => {
            let input = copy_subtree(&project.input, context, id_map);
            let columns = project
                .columns
                .iter()
                .map(|column| {
                    let mut column = column.clone();
                    remap_tables(&mut column.expr, id_map);
                    column
                })
                .collect();
            LogicalPlan::Project(Project {
                input: Box::new(input),
                columns,
            })
        }
        LogicalPlan::Distinct(distinct) => LogicalPlan::Distinct(Distinct {
            input: Box::new(copy_subtree(&distinct.input, context, id_map)),
            distinctness: Distinctness::Distinct { ctx: None },
        }),
        LogicalPlan::Sort(sort) => {
            let input = copy_subtree(&sort.input, context, id_map);
            let keys = sort
                .keys
                .iter()
                .map(|(expr, order, nulls)| {
                    let mut expr = expr.clone();
                    remap_tables(&mut expr, id_map);
                    (expr, *order, *nulls)
                })
                .collect();
            LogicalPlan::Sort(crate::translate::logical::Sort {
                input: Box::new(input),
                keys,
            })
        }
        LogicalPlan::Limit(limit) => {
            let input = copy_subtree(&limit.input, context, id_map);
            let copy = |expr: &Option<Box<Expr>>| {
                expr.as_ref().map(|expr| {
                    let mut expr = expr.clone();
                    remap_tables(&mut expr, id_map);
                    expr
                })
            };
            LogicalPlan::Limit(crate::translate::logical::Limit {
                limit: copy(&limit.limit),
                offset: copy(&limit.offset),
                input: Box::new(input),
            })
        }
    }
}

fn copy_block(
    block: &Block,
    context: &mut RuleContext<'_, '_>,
    id_map: &mut HashMap<TableInternalId, TableInternalId>,
) -> Block {
    Block {
        root: copy_subtree(&block.root, context, id_map),
        subqueries: Vec::new(),
        outer_query_refs: block.outer_query_refs.clone(),
        right_join_swapped: block.right_join_swapped,
        query_destination: QueryDestination::placeholder_for_subquery(),
        input_cardinality_hint: None,
        phantom_params: block.phantom_params.clone(),
    }
}

fn remap_tables(expr: &mut Expr, id_map: &HashMap<TableInternalId, TableInternalId>) {
    walk_expr_mut(expr, &mut |expr: &mut Expr| -> Result<WalkControl> {
        match expr {
            Expr::Column { table, .. } | Expr::RowId { table, .. } => {
                if let Some(new_id) = id_map.get(table) {
                    *table = *new_id;
                }
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })
    .expect("remapping tables cannot fail");
}
