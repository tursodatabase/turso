//! Turn a correlated scalar aggregate subquery into joins.
//!
//! This is the general unnesting of Neumann and Kemper, "Unnesting Arbitrary
//! Queries" (BTW 2015), for one subquery shape: a subquery that returns one
//! aggregate value and reads columns of the query around it.
//!
//! ```sql
//! -- Before
//! SELECT o.id, (SELECT count(*) FROM i WHERE i.k = o.k OR i.k < o.j) FROM o WHERE o.x > 1;
//!
//! -- After
//! SELECT o.id, coalesce(sq.value, 0)
//! FROM o
//! LEFT JOIN (
//!     SELECT count(*) AS value, d.k0, d.k1
//!     FROM (SELECT DISTINCT o.k AS k0, o.j AS k1 FROM o WHERE o.x > 1) d
//!     JOIN i
//!     WHERE i.k = d.k0 OR i.k < d.k1
//!     GROUP BY d.k0, d.k1
//! ) sq ON sq.k0 IS o.k AND sq.k1 IS o.j
//! WHERE o.x > 1;
//! ```
//!
//! The steps are the ones of the paper:
//!
//! 1. `D` is the set of distinct values of the outer columns that the subquery
//!    reads (its domain). It comes from a copy of the outer FROM clause and of
//!    the outer WHERE terms that are safe to copy.
//! 2. The dependent join between `D` and the subquery moves down through the
//!    aggregate (which then groups by the columns of `D`) and through the
//!    filter, until it reaches the base tables and becomes a plain join.
//! 3. The outer query joins the result back on the domain columns with `IS`,
//!    so NULL values match as the original correlation did. A left join keeps
//!    outer rows without a group. For those rows `count` must give 0, so its
//!    replacement wraps the joined value in `coalesce`.
//!
//! The rule applies when:
//!
//! - the subquery is one aggregate value with no `GROUP BY`, `HAVING`,
//!   `DISTINCT`, `LIMIT` other than 1, window, or non-FROM subquery;
//! - its joins are inner or left joins, and no correlated term is an outer
//!   join term;
//! - it reads only tables of the outer query, and every such table is a
//!   B-tree table;
//! - the outer query reads no table of a query outside it and has no
//!   `FULL JOIN`;
//! - the subquery value is not used inside an outer join term of the outer
//!   query, because that value must be ready before the join decides on a row;
//! - no inner WHERE term can fail on its input, such as `json_extract` on
//!   text that is not JSON. The join optimizer can run such a term on inner
//!   rows before the correlation term, and the original query never runs it on
//!   those rows;
//! - the value for an empty group is known.
//!
//! When an outer WHERE term cannot be copied into `D`, the domain can hold
//! values that no outer row reaches, and the aggregate runs for them too. The
//! rule then applies only when the aggregate cannot fail on extra input.

use rustc_hash::FxHashMap as HashMap;
use smallvec::SmallVec;
use turso_parser::ast::{self, Expr, SortOrder, SubqueryType, TableInternalId};

use crate::numeric::Numeric;
use crate::schema::Table;
use crate::translate::expr::{
    expr_contains_nondeterministic_scalar_function, expr_references_any_subquery,
    expression_can_fail_on_input, walk_expr_mut, WalkControl,
};
use crate::translate::logical::raise::raise_select_plan;
use crate::translate::logical::walk::{
    collect_table_ids, filter_node, filter_slot, join_tree, join_tree_mut, leaf_ids,
};
use crate::translate::logical::{
    Aggregate, Block, DerivedTable, Distinct, Filter, Join, LogicalPlan, Project,
};
use crate::translate::optimizer::unnest::{
    aggregate_can_run_for_unused_rows, coalesce_with_zero, result_on_empty_input, EmptyInputValue,
};
use crate::translate::plan::{
    Distinctness, GroupBy, JoinInfo, JoinType, Operation, Plan, QueryDestination, ResultSetColumn,
    SelectPlan, SubqueryState, WhereTerm,
};
use crate::types::Value;
use crate::util::parse_signed_number;
use crate::Result;

use super::{Rule, RuleContext};

pub(crate) struct DecorrelateScalarAggregates;

impl Rule for DecorrelateScalarAggregates {
    fn name(&self) -> &'static str {
        "DecorrelateScalarAggregates"
    }

    fn apply(&self, block: &mut Block, context: &mut RuleContext<'_, '_>) -> Result<bool> {
        if !outer_block_is_supported(block) {
            return Ok(false);
        }
        for index in 0..block.subqueries.len() {
            let Some(plan) = subquery_to_unnest(block, index, context)? else {
                continue;
            };
            unnest(block, index, plan, context)?;
            return Ok(true);
        }
        Ok(false)
    }
}

fn outer_block_is_supported(block: &Block) -> bool {
    if block
        .outer_query_refs
        .iter()
        .any(|reference| reference.is_used())
    {
        return false;
    }
    let mut supported = true;
    let mut has_table = false;
    join_tree(&block.root).for_each_node(&mut |node| match node {
        LogicalPlan::Join(join) => supported &= !join.info.is_full_outer(),
        LogicalPlan::Scan(scan) => {
            has_table = true;
            supported &= matches!(scan.table.table, Table::BTree(_));
        }
        LogicalPlan::DerivedTable(_) | LogicalPlan::OneRow => supported = false,
        _ => {}
    });
    supported && has_table
}

struct Analysis {
    /// The outer columns the subquery reads, in first-use order.
    domain: Vec<Expr>,
    empty_value: EmptyInputValue,
}

/// Check one subquery. Return its analysis when the rule applies to it.
fn subquery_to_unnest(
    block: &Block,
    index: usize,
    context: &RuleContext<'_, '_>,
) -> Result<Option<Analysis>> {
    let subquery = &block.subqueries[index];
    if !subquery.correlated
        || !matches!(
            subquery.query_type,
            SubqueryType::RowValue { num_regs: 1, .. }
        )
    {
        return Ok(None);
    }
    let SubqueryState::Unevaluated { plan: Some(plan) } = &subquery.state else {
        return Ok(None);
    };
    let Plan::Select(inner) = plan.as_ref() else {
        return Ok(None);
    };
    if !inner_plan_is_supported(inner) {
        return Ok(None);
    }
    if inner
        .where_clause
        .iter()
        .any(|term| expression_can_fail_on_input(&term.expr))
    {
        return Ok(None);
    }
    let Some(empty_value) = result_on_empty_input(inner) else {
        return Ok(None);
    };

    let outer_ids: Vec<TableInternalId> = leaf_ids(join_tree(&block.root));
    let used_outer_ids: Vec<TableInternalId> = inner
        .table_references
        .outer_query_refs()
        .iter()
        .filter(|reference| reference.is_used())
        .map(|reference| reference.internal_id)
        .collect();
    if used_outer_ids.is_empty()
        || used_outer_ids.iter().any(|id| !outer_ids.contains(id))
        || inner
            .table_references
            .outer_query_refs()
            .iter()
            .any(|reference| reference.is_used() && reference.scope_depth != 0)
    {
        return Ok(None);
    }
    if inner
        .where_clause
        .iter()
        .any(|term| term.from_outer_join.is_some() && references_any(&term.expr, &outer_ids))
    {
        return Ok(None);
    }

    let subquery_id = subquery.internal_id;
    let mut value_in_outer_join_term = false;
    let mut value_used = false;
    if let Some(filter) = filter_node(&block.root) {
        for term in &filter.terms {
            if references_subquery(&term.expr, subquery_id) {
                value_used = true;
                value_in_outer_join_term |= term.from_outer_join.is_some();
            }
        }
    }
    block
        .root
        .for_each_expr(&mut |expr| value_used |= references_subquery(expr, subquery_id));
    if !value_used || value_in_outer_join_term {
        return Ok(None);
    }

    let domain = domain_columns(inner, &outer_ids);
    let domain_is_exact = filter_node(&block.root).is_none_or(|filter| {
        filter.terms.iter().all(|term| {
            term_can_be_copied(&term.expr, context).unwrap_or(false)
                || references_subquery(&term.expr, subquery_id)
        })
    });
    if !domain_is_exact && !aggregate_can_run_for_unused_rows(inner) {
        return Ok(None);
    }
    Ok(Some(Analysis {
        domain,
        empty_value,
    }))
}

fn inner_plan_is_supported(inner: &SelectPlan) -> bool {
    if !inner.values.is_empty()
        || inner.window.is_some()
        || !inner.non_from_clause_subqueries.is_empty()
        || !inner.order_by.is_empty()
        || inner.offset.is_some()
        || inner.distinctness.is_distinct()
        || inner.aggregates.is_empty()
        || inner.result_columns.len() != 1
        || inner.table_references.joined_tables().is_empty()
        || inner.contains_constant_false_condition
    {
        return false;
    }
    if inner
        .group_by
        .as_ref()
        .is_some_and(|group_by| !group_by.exprs.is_empty() || group_by.having.is_some())
    {
        return false;
    }
    if let Some(limit) = &inner.limit {
        if !matches!(
            parse_signed_number(limit),
            Ok(Value::Numeric(Numeric::Integer(1)))
        ) {
            return false;
        }
    }
    inner.table_references.joined_tables().iter().all(|table| {
        let join_is_simple = table
            .join_info
            .as_ref()
            .is_none_or(|info| matches!(info.join_type, JoinType::Inner | JoinType::LeftOuter));
        let table_is_simple = match &table.table {
            Table::BTree(_) | Table::Virtual(_) => true,
            Table::FromClauseSubquery(subquery) => {
                !crate::translate::plan::plan_is_correlated(&subquery.plan)
            }
            Table::RecursiveCteInput(_) => false,
        };
        join_is_simple && table_is_simple
    })
}

/// Whether an outer WHERE term can run inside the domain table. A term that
/// can fail stays out, because the outer query can skip it for a row that an
/// earlier term rejects.
fn term_can_be_copied(expr: &Expr, context: &RuleContext<'_, '_>) -> Result<bool> {
    Ok(!expr_references_any_subquery(expr)
        && !expression_can_fail_on_input(expr)
        && !expr_contains_nondeterministic_scalar_function(expr, context.resolver)?)
}

fn references_any(expr: &Expr, ids: &[TableInternalId]) -> bool {
    let mut tables = Vec::new();
    collect_table_ids(expr, &mut tables);
    tables.iter().any(|id| ids.contains(id))
}

fn references_subquery(expr: &Expr, subquery_id: TableInternalId) -> bool {
    crate::translate::expr::expr_references_subquery_id(expr, subquery_id)
}

/// The outer column references of the subquery, each once.
fn domain_columns(inner: &SelectPlan, outer_ids: &[TableInternalId]) -> Vec<Expr> {
    let mut domain: Vec<Expr> = Vec::new();
    let mut collect = |expr: &Expr| {
        crate::translate::expr::walk_expr(expr, &mut |expr: &Expr| -> Result<WalkControl> {
            match expr {
                Expr::Column { table, .. } | Expr::RowId { table, .. }
                    if outer_ids.contains(table) =>
                {
                    if !domain.iter().any(|known| known == expr) {
                        domain.push(expr.clone());
                    }
                }
                _ => {}
            }
            Ok(WalkControl::Continue)
        })
        .expect("walking an expression cannot fail");
    };
    for term in &inner.where_clause {
        collect(&term.expr);
    }
    for column in &inner.result_columns {
        collect(&column.expr);
    }
    for aggregate in &inner.aggregates {
        collect(&aggregate.original_expr);
        for arg in &aggregate.args {
            collect(arg);
        }
        if let Some(filter) = &aggregate.filter_expr {
            collect(filter);
        }
    }
    domain
}

fn unnest(
    block: &mut Block,
    index: usize,
    analysis: Analysis,
    context: &mut RuleContext<'_, '_>,
) -> Result<()> {
    let subquery_id = block.subqueries[index].internal_id;
    let mut inner = take_inner_plan(block, index);
    inner.limit = None;
    let mut inner_block = raise_select_plan(&mut inner)
        .expect("checked: the inner plan has a shape the tree can hold");
    inner_block.outer_query_refs.clear();

    let domain_id = context.ids.next();
    let aggregate_id = context.ids.next();

    let domain_table =
        build_domain_table(block, &analysis.domain, domain_id, subquery_id, context)?;
    let aggregate_table = build_aggregate_table(
        inner_block,
        domain_table,
        &analysis.domain,
        domain_id,
        aggregate_id,
    );

    let value = Expr::Column {
        database: None,
        table: aggregate_id,
        column: 0,
        is_rowid_alias: false,
    };
    let replacement = match analysis.empty_value {
        EmptyInputValue::Null => value,
        EmptyInputValue::IntegerZero => coalesce_with_zero(value, "0"),
        EmptyInputValue::RealZero => coalesce_with_zero(value, "0.0"),
    };

    let join_terms: Vec<WhereTerm> = analysis
        .domain
        .iter()
        .enumerate()
        .map(|(position, outer_column)| WhereTerm {
            expr: Expr::Binary(
                Box::new(Expr::Column {
                    database: None,
                    table: aggregate_id,
                    column: position + 1,
                    is_rowid_alias: false,
                }),
                ast::Operator::Is,
                Box::new(outer_column.clone()),
            ),
            from_outer_join: Some(aggregate_id),
            consumed: false,
        })
        .collect();

    let joins = join_tree_mut(&mut block.root);
    let outer_joins = std::mem::replace(joins, LogicalPlan::OneRow);
    *joins = LogicalPlan::Join(Join {
        left: Box::new(outer_joins),
        right: Box::new(LogicalPlan::DerivedTable(aggregate_table)),
        info: JoinInfo {
            join_type: JoinType::LeftOuter,
            using: vec![],
            no_reorder: false,
        },
    });
    filter_slot(&mut block.root).terms.extend(join_terms);

    let mut replaced_ids: SmallVec<[TableInternalId; 2]> = SmallVec::new();
    replaced_ids.push(subquery_id);
    for other in &block.subqueries {
        if other.same_query == Some(subquery_id) {
            replaced_ids.push(other.internal_id);
        }
    }
    name_bare_subquery_columns(block, &replaced_ids);
    block.for_each_expr_mut(&mut |expr| {
        walk_expr_mut(expr, &mut |expr: &mut Expr| -> Result<WalkControl> {
            if let Expr::SubqueryResult { subquery_id, .. } = expr {
                if replaced_ids.contains(subquery_id) {
                    *expr = replacement.clone();
                    return Ok(WalkControl::SkipChildren);
                }
            }
            Ok(WalkControl::Continue)
        })
        .map(|_| ())
    })?;
    block
        .subqueries
        .retain(|subquery| !replaced_ids.contains(&subquery.internal_id));
    Ok(())
}

/// A result column that is only the subquery value keeps the subquery text
/// as its name.
fn name_bare_subquery_columns(block: &mut Block, subquery_ids: &[TableInternalId]) {
    let mut node = &mut block.root;
    loop {
        match node {
            LogicalPlan::Project(project) => {
                for column in &mut project.columns {
                    if column.alias.is_some() {
                        continue;
                    }
                    let Expr::SubqueryResult { subquery_id, .. } = &column.expr else {
                        continue;
                    };
                    if subquery_ids.contains(subquery_id) {
                        column.alias = column.implicit_column_name.clone();
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

fn take_inner_plan(block: &mut Block, index: usize) -> SelectPlan {
    let SubqueryState::Unevaluated { plan } = &mut block.subqueries[index].state else {
        unreachable!("checked: the subquery has not run")
    };
    let Plan::Select(inner) = *plan.take().expect("checked: the subquery has a plan") else {
        unreachable!("checked: the subquery is a SELECT")
    };
    *inner
}

/// `SELECT DISTINCT <domain columns> FROM <copy of the outer FROM clause>
/// WHERE <copies of the safe outer terms>`.
fn build_domain_table(
    block: &Block,
    domain: &[Expr],
    domain_id: TableInternalId,
    subquery_id: TableInternalId,
    context: &mut RuleContext<'_, '_>,
) -> Result<DerivedTable> {
    let mut id_map: HashMap<TableInternalId, TableInternalId> = HashMap::default();
    let joins = copy_join_tree(join_tree(&block.root), context, &mut id_map);

    let mut terms = Vec::new();
    if let Some(filter) = filter_node(&block.root) {
        for term in &filter.terms {
            if references_subquery(&term.expr, subquery_id)
                || !term_can_be_copied(&term.expr, context)?
            {
                continue;
            }
            let mut expr = term.expr.clone();
            remap_tables(&mut expr, &id_map);
            terms.push(WhereTerm {
                expr,
                from_outer_join: term.from_outer_join.map(|id| id_map[&id]),
                consumed: false,
            });
        }
    }
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

/// `SELECT <aggregate>, d.k0, ... FROM D JOIN <subquery tables> WHERE
/// <subquery terms> GROUP BY d.k0, ...`, with the outer columns of the
/// subquery replaced by the columns of `D`.
fn build_aggregate_table(
    inner: Block,
    domain_table: DerivedTable,
    domain: &[Expr],
    domain_id: TableInternalId,
    aggregate_id: TableInternalId,
) -> DerivedTable {
    let LogicalPlan::Project(project) = inner.root else {
        unreachable!("checked: the subquery starts with a projection")
    };
    let LogicalPlan::Aggregate(aggregate) = *project.input else {
        unreachable!("checked: the subquery aggregates")
    };
    let (inner_joins, inner_terms) = match *aggregate.input {
        LogicalPlan::Filter(filter) => (*filter.input, filter.terms),
        node => (node, Vec::new()),
    };

    let domain_column = |position: usize| Expr::Column {
        database: None,
        table: domain_id,
        column: position,
        is_rowid_alias: false,
    };
    let mut joins = LogicalPlan::Join(Join {
        left: Box::new(LogicalPlan::DerivedTable(domain_table)),
        right: Box::new(inner_joins),
        info: JoinInfo {
            join_type: JoinType::Inner,
            using: vec![],
            no_reorder: false,
        },
    });
    if !inner_terms.is_empty() {
        joins = LogicalPlan::Filter(Filter {
            input: Box::new(joins),
            terms: inner_terms,
        });
    }
    let group_count = domain.len();
    let mut root = LogicalPlan::Aggregate(Aggregate {
        input: Box::new(joins),
        group_by: Some(GroupBy {
            exprs: (0..group_count).map(domain_column).collect(),
            sort_order: vec![SortOrder::Asc; group_count],
            nulls_order: vec![None; group_count],
            sort_elided: false,
            having: None,
        }),
        aggregates: aggregate.aggregates,
    });
    let mut columns = project.columns;
    columns.extend((0..group_count).map(|position| ResultSetColumn {
        expr: domain_column(position),
        alias: Some(format!("k{position}")),
        implicit_column_name: None,
        contains_aggregates: false,
    }));
    root = LogicalPlan::Project(Project {
        input: Box::new(root),
        columns,
    });
    root.for_each_expr_mut(&mut |expr| {
        walk_expr_mut(expr, &mut |expr: &mut Expr| -> Result<WalkControl> {
            if let Some(position) = domain.iter().position(|known| known == expr) {
                *expr = domain_column(position);
                return Ok(WalkControl::SkipChildren);
            }
            Ok(WalkControl::Continue)
        })
        .map(|_| ())
    })
    .expect("replacing outer columns cannot fail");
    DerivedTable {
        identifier: format!("scalar_subquery_{aggregate_id}"),
        internal_id: aggregate_id,
        shell: None,
        value_without_collation: true,
        block: Box::new(new_block(root)),
    }
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

/// Copy a join tree of base tables with fresh table ids.
fn copy_join_tree(
    node: &LogicalPlan,
    context: &mut RuleContext<'_, '_>,
    id_map: &mut HashMap<TableInternalId, TableInternalId>,
) -> LogicalPlan {
    match node {
        LogicalPlan::Scan(scan) => {
            let mut table = scan.table.clone();
            let new_id = context.ids.next();
            id_map.insert(table.internal_id, new_id);
            table.internal_id = new_id;
            table.op = Operation::default_scan_for(&table.table);
            table.plan_estimate = None;
            table.join_info = None;
            LogicalPlan::Scan(crate::translate::logical::Scan { table })
        }
        LogicalPlan::Join(join) => LogicalPlan::Join(Join {
            left: Box::new(copy_join_tree(&join.left, context, id_map)),
            right: Box::new(copy_join_tree(&join.right, context, id_map)),
            info: join.info.clone(),
        }),
        _ => unreachable!("checked: the outer join tree holds only base tables"),
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
