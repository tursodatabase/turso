//! Merge a simple derived table into the block that reads it.
//!
//! ```sql
//! -- Before
//! SELECT s.a FROM (SELECT a, b FROM t1 WHERE b > 1) s WHERE s.a = 5;
//! -- After
//! SELECT t1.a FROM t1 WHERE t1.a = 5 AND t1.b > 1;
//! ```
//!
//! The parent then reads the base tables directly, so the join optimizer can
//! use their indexes. This is the flattener of SQLite (`select.c`), limited to
//! the cases below. A derived table moves into its parent when:
//!
//! - it is `SELECT <columns> FROM <joins> [WHERE ...]` with no aggregate,
//!   `DISTINCT`, `ORDER BY`, `LIMIT`, window, or non-FROM subquery;
//! - its joins are inner or left joins;
//! - its result columns call no nondeterministic function, because the parent
//!   can evaluate a substituted column more than once;
//! - its result columns call no function that returns a value with a subtype,
//!   such as the JSON functions. A subtype does not survive a subquery, and a
//!   substituted expression would keep it;
//! - it reads no column of a query outside the parent;
//! - it is not the right side of an outer, semi, or anti join in the parent;
//! - the parent has no `FULL JOIN`;
//! - no non-FROM subquery of the parent reads its columns;
//! - it is not a `MATERIALIZED` CTE or a CTE whose result other reads share.

use turso_parser::ast::{Expr, TableInternalId};

use crate::function::Func;
use crate::schema::Table;
use crate::translate::emitter::Resolver;
use crate::translate::expr::{
    expr_contains_nondeterministic_scalar_function, walk_expr, walk_expr_mut, WalkControl,
};
use crate::translate::plan::{JoinType, Plan, SelectPlan, SubqueryState, TableReferences};
use crate::Result;

use super::{Rule, RuleContext};
use crate::translate::logical::walk::{filter_slot, join_tree};
use crate::translate::logical::{Block, DerivedTable, LogicalPlan};

pub(crate) struct FlattenDerivedTables;

impl Rule for FlattenDerivedTables {
    fn name(&self) -> &'static str {
        "FlattenDerivedTables"
    }

    fn apply(&self, block: &mut Block, context: &mut RuleContext<'_, '_>) -> Result<bool> {
        let Some(target) = find_derived_table_to_flatten(block, context)? else {
            return Ok(false);
        };
        flatten(block, target);
        Ok(true)
    }
}

/// Return the first derived table of the block that can move into it.
fn find_derived_table_to_flatten(
    block: &Block,
    context: &RuleContext<'_, '_>,
) -> Result<Option<TableInternalId>> {
    let joins = join_tree(&block.root);
    let mut has_full_join = false;
    joins.for_each_node(&mut |node| {
        if let LogicalPlan::Join(join) = node {
            has_full_join |= join.info.is_full_outer();
        }
    });
    if has_full_join {
        return Ok(None);
    }
    let parent_table_count = count_leaves(joins);

    let mut candidates = Vec::new();
    collect_candidates(joins, false, &mut candidates);
    for derived in candidates {
        if !derived_table_is_simple(derived, context)? {
            continue;
        }
        if block
            .subqueries
            .iter()
            .any(|subquery| match &subquery.state {
                SubqueryState::Unevaluated { plan: Some(plan) } => {
                    plan_reads_table(plan, derived.internal_id)
                }
                SubqueryState::Unevaluated { plan: None } => false,
                SubqueryState::Evaluated { outer_ref_ids, .. } => {
                    outer_ref_ids.contains(&derived.internal_id)
                }
            })
        {
            continue;
        }
        let inner_table_count = count_leaves(join_tree(&derived.block.root));
        if parent_table_count - 1 + inner_table_count > TableReferences::MAX_JOINED_TABLES {
            continue;
        }
        if block_reads_rowid(block, derived.internal_id) {
            continue;
        }
        if identifiers_clash(joins, derived) {
            continue;
        }
        return Ok(Some(derived.internal_id));
    }
    Ok(None)
}

/// Collect the derived tables of a join tree that are not the right side of
/// an outer, semi, or anti join.
fn collect_candidates<'a>(
    node: &'a LogicalPlan,
    is_restricted_right_side: bool,
    out: &mut Vec<&'a DerivedTable>,
) {
    match node {
        LogicalPlan::DerivedTable(derived) if !is_restricted_right_side => out.push(derived),
        LogicalPlan::Join(join) => {
            collect_candidates(&join.left, false, out);
            let restricted = join.info.is_outer() || join.info.is_semi_or_anti();
            collect_candidates(&join.right, restricted, out);
        }
        _ => {}
    }
}

fn derived_table_is_simple(derived: &DerivedTable, context: &RuleContext<'_, '_>) -> Result<bool> {
    let block = &derived.block;
    if !block.subqueries.is_empty() {
        return Ok(false);
    }
    if block
        .outer_query_refs
        .iter()
        .any(|reference| reference.is_used())
    {
        return Ok(false);
    }
    if let Some(table) = &derived.shell {
        let Table::FromClauseSubquery(subquery) = &table.table else {
            unreachable!("a derived table shell holds a subquery")
        };
        if subquery.materialize_hint() || subquery.shared_materialization() {
            return Ok(false);
        }
    }
    let LogicalPlan::Project(project) = &block.root else {
        return Ok(false);
    };
    for column in &project.columns {
        if expr_contains_nondeterministic_scalar_function(&column.expr, context.resolver)?
            || expr_may_carry_subtype(&column.expr, context.resolver)?
        {
            return Ok(false);
        }
    }
    let joins = match project.input.as_ref() {
        LogicalPlan::Filter(filter) => filter.input.as_ref(),
        node => node,
    };
    let mut joins_are_simple = true;
    joins.for_each_node(&mut |node| match node {
        LogicalPlan::Join(join) => {
            joins_are_simple &=
                matches!(join.info.join_type, JoinType::Inner | JoinType::LeftOuter);
        }
        LogicalPlan::Scan(_) | LogicalPlan::DerivedTable(_) => {}
        _ => joins_are_simple = false,
    });
    Ok(joins_are_simple)
}

/// Whether a table of the derived table has the same name as a table of the
/// parent. Later steps look tables up by name, so names must stay unique.
fn identifiers_clash(parent_joins: &LogicalPlan, derived: &DerivedTable) -> bool {
    let mut parent_names = Vec::new();
    parent_joins.for_each_node(&mut |node| match node {
        LogicalPlan::Scan(scan) => parent_names.push(scan.table.identifier.as_str()),
        LogicalPlan::DerivedTable(other) if other.internal_id != derived.internal_id => {
            parent_names.push(other.identifier.as_str())
        }
        _ => {}
    });
    let mut clash = false;
    join_tree(&derived.block.root).for_each_node(&mut |node| {
        let name = match node {
            LogicalPlan::Scan(scan) => Some(scan.table.identifier.as_str()),
            LogicalPlan::DerivedTable(inner) => Some(inner.identifier.as_str()),
            _ => None,
        };
        if let Some(name) = name {
            clash |= parent_names
                .iter()
                .any(|parent| parent.eq_ignore_ascii_case(name));
        }
    });
    clash
}

/// Whether an expression calls a function whose result can carry a subtype.
fn expr_may_carry_subtype(expr: &Expr, resolver: &Resolver<'_>) -> Result<bool> {
    let mut found = false;
    walk_expr(expr, &mut |expr: &Expr| -> Result<WalkControl> {
        let (name, arg_count) = match expr {
            Expr::FunctionCall { name, args, .. } => (name, args.len()),
            Expr::FunctionCallStar { name, .. } => (name, 0),
            _ => return Ok(WalkControl::Continue),
        };
        let carries_subtype = match resolver.resolve_function(name.as_str(), arg_count)? {
            #[cfg(feature = "json")]
            Some(Func::Json(_)) => true,
            Some(Func::External(_)) => true,
            _ => false,
        };
        if carries_subtype {
            found = true;
            return Ok(WalkControl::SkipChildren);
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(found)
}

fn count_leaves(node: &LogicalPlan) -> usize {
    let mut count = 0;
    node.for_each_node(&mut |node| {
        if matches!(node, LogicalPlan::Scan(_) | LogicalPlan::DerivedTable(_)) {
            count += 1;
        }
    });
    count
}

fn block_reads_rowid(block: &Block, table_id: TableInternalId) -> bool {
    let mut found = false;
    block.root.for_each_expr(&mut |expr| {
        walk_expr(expr, &mut |expr: &Expr| -> Result<WalkControl> {
            if matches!(expr, Expr::RowId { table, .. } if *table == table_id) {
                found = true;
            }
            Ok(WalkControl::Continue)
        })
        .expect("walking an expression cannot fail");
    });
    found
}

/// Whether a plan or one of its nested plans reads a table of an enclosing query.
fn plan_reads_table(plan: &Plan, table_id: TableInternalId) -> bool {
    match plan {
        Plan::Select(select) => select_reads_table(select, table_id),
        Plan::CompoundSelect {
            left, right_most, ..
        } => {
            left.iter()
                .any(|(select, _)| select_reads_table(select, table_id))
                || select_reads_table(right_most, table_id)
        }
        Plan::RecursiveCte(cte) => {
            plan_reads_table(&cte.initial_query, table_id)
                || plan_reads_table(&cte.recursive_query, table_id)
        }
        Plan::Delete(_) | Plan::Update(_) => false,
    }
}

fn select_reads_table(select: &SelectPlan, table_id: TableInternalId) -> bool {
    select
        .table_references
        .outer_query_refs()
        .iter()
        .any(|reference| reference.internal_id == table_id && reference.is_used())
        || select.table_references.joined_tables().iter().any(|table| {
            matches!(&table.table, Table::FromClauseSubquery(subquery) if plan_reads_table(&subquery.plan, table_id))
        })
        || select
            .non_from_clause_subqueries
            .iter()
            .any(|subquery| match &subquery.state {
                SubqueryState::Unevaluated { plan: Some(plan) } => plan_reads_table(plan, table_id),
                SubqueryState::Unevaluated { plan: None } => false,
                SubqueryState::Evaluated { outer_ref_ids, .. } => outer_ref_ids.contains(&table_id),
            })
}

/// Move one derived table into the block.
fn flatten(block: &mut Block, target: TableInternalId) {
    let derived = take_leaf(&mut block.root, target).expect("the candidate is a leaf of the block");
    let DerivedTable {
        shell,
        block: inner,
        ..
    } = derived;
    let Block {
        root: inner_root,
        phantom_params,
        ..
    } = *inner;
    let LogicalPlan::Project(project) = inner_root else {
        unreachable!("checked: a simple derived table starts with a projection")
    };
    let substitutions: Vec<Expr> = project
        .columns
        .into_iter()
        .map(|column| column.expr)
        .collect();
    let (inner_joins, inner_terms) = match *project.input {
        LogicalPlan::Filter(filter) => (*filter.input, filter.terms),
        node => (node, Vec::new()),
    };

    name_bare_references(block, target, shell.as_ref());
    put_leaf(&mut block.root, inner_joins);
    if !inner_terms.is_empty() {
        let filter = filter_slot(&mut block.root);
        filter.terms.extend(inner_terms);
    }
    block
        .for_each_expr_mut(&mut |expr| {
            walk_expr_mut(expr, &mut |expr: &mut Expr| -> Result<WalkControl> {
                if let Expr::Column { table, column, .. } = expr {
                    if *table == target {
                        *expr = substitutions[*column].clone();
                        return Ok(WalkControl::SkipChildren);
                    }
                }
                Ok(WalkControl::Continue)
            })
            .map(|_| ())
        })
        .expect("substituting columns cannot fail");
    block.phantom_params.extend(phantom_params);
}

/// A result column that only names a derived column keeps that column's name.
fn name_bare_references(
    block: &mut Block,
    target: TableInternalId,
    shell: Option<&crate::translate::plan::JoinedTable>,
) {
    let Some(shell) = shell else {
        return;
    };
    let mut node = &mut block.root;
    loop {
        match node {
            LogicalPlan::Project(project) => {
                for column in &mut project.columns {
                    if column.alias.is_some() {
                        continue;
                    }
                    let Expr::Column {
                        table,
                        column: index,
                        ..
                    } = &column.expr
                    else {
                        continue;
                    };
                    if *table != target {
                        continue;
                    }
                    column.alias = shell.columns()[*index].name.clone();
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

/// Swap the derived table with a placeholder and return it.
fn take_leaf(node: &mut LogicalPlan, target: TableInternalId) -> Option<DerivedTable> {
    match node {
        LogicalPlan::DerivedTable(derived) if derived.internal_id == target => {
            let LogicalPlan::DerivedTable(derived) = std::mem::replace(node, LogicalPlan::OneRow)
            else {
                unreachable!("checked: the node is a derived table")
            };
            Some(derived)
        }
        LogicalPlan::Join(join) => {
            take_leaf(&mut join.left, target).or_else(|| take_leaf(&mut join.right, target))
        }
        LogicalPlan::OneRow | LogicalPlan::Scan(_) | LogicalPlan::DerivedTable(_) => None,
        node => node.input_mut().and_then(|input| take_leaf(input, target)),
    }
}

/// Put a join tree where `take_leaf` left its placeholder.
fn put_leaf(node: &mut LogicalPlan, replacement: LogicalPlan) {
    let mut replacement = Some(replacement);
    fn put(node: &mut LogicalPlan, replacement: &mut Option<LogicalPlan>) {
        match node {
            LogicalPlan::OneRow => {
                *node = replacement.take().expect("one placeholder in the tree");
            }
            LogicalPlan::Join(join) => {
                put(&mut join.left, replacement);
                if replacement.is_some() {
                    put(&mut join.right, replacement);
                }
            }
            LogicalPlan::Scan(_) | LogicalPlan::DerivedTable(_) => {}
            node => {
                if let Some(input) = node.input_mut() {
                    put(input, replacement);
                }
            }
        }
    }
    put(node, &mut replacement);
    assert!(replacement.is_none(), "the placeholder must be in the tree");
}
