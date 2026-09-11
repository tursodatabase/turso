//! Visit the expressions and the nodes of a tree.

use turso_parser::ast::{Expr, TableInternalId};

use crate::translate::expr::{walk_expr, WalkControl};
use crate::Result;

use super::{Block, Filter, LogicalPlan};

impl Block {
    /// Visit every expression of this block. Nested blocks and the plans of
    /// non-FROM subqueries are not visited.
    pub fn for_each_expr_mut(
        &mut self,
        visit: &mut impl FnMut(&mut Expr) -> Result<()>,
    ) -> Result<()> {
        self.root.for_each_expr_mut(visit)
    }
}

impl LogicalPlan {
    /// Visit every expression of this node and the nodes below it. Nested
    /// blocks and the plans of non-FROM subqueries are not visited.
    pub fn for_each_expr(&self, visit: &mut impl FnMut(&Expr)) {
        match self {
            LogicalPlan::OneRow | LogicalPlan::Scan(_) | LogicalPlan::DerivedTable(_) => {}
            LogicalPlan::Join(join) => {
                join.left.for_each_expr(visit);
                join.right.for_each_expr(visit);
            }
            LogicalPlan::Filter(filter) => {
                filter.input.for_each_expr(visit);
                filter.terms.iter().for_each(|term| visit(&term.expr));
            }
            LogicalPlan::Aggregate(aggregate) => {
                aggregate.input.for_each_expr(visit);
                if let Some(group_by) = &aggregate.group_by {
                    group_by.exprs.iter().for_each(&mut *visit);
                    if let Some(having) = &group_by.having {
                        having.iter().for_each(&mut *visit);
                    }
                }
                for aggregate in &aggregate.aggregates {
                    visit(&aggregate.original_expr);
                    aggregate.args.iter().for_each(&mut *visit);
                    if let Some(filter) = &aggregate.filter_expr {
                        visit(filter);
                    }
                }
            }
            LogicalPlan::Project(project) => {
                project.input.for_each_expr(visit);
                project
                    .columns
                    .iter()
                    .for_each(|column| visit(&column.expr));
            }
            LogicalPlan::Distinct(distinct) => distinct.input.for_each_expr(visit),
            LogicalPlan::Sort(sort) => {
                sort.input.for_each_expr(visit);
                sort.keys.iter().for_each(|(expr, _, _)| visit(expr));
            }
            LogicalPlan::Limit(limit) => {
                limit.input.for_each_expr(visit);
                if let Some(expr) = &limit.limit {
                    visit(expr);
                }
                if let Some(expr) = &limit.offset {
                    visit(expr);
                }
            }
        }
    }

    pub fn for_each_expr_mut(
        &mut self,
        visit: &mut impl FnMut(&mut Expr) -> Result<()>,
    ) -> Result<()> {
        match self {
            LogicalPlan::OneRow | LogicalPlan::Scan(_) | LogicalPlan::DerivedTable(_) => Ok(()),
            LogicalPlan::Join(join) => {
                join.left.for_each_expr_mut(visit)?;
                join.right.for_each_expr_mut(visit)
            }
            LogicalPlan::Filter(filter) => {
                filter.input.for_each_expr_mut(visit)?;
                filter
                    .terms
                    .iter_mut()
                    .try_for_each(|term| visit(&mut term.expr))
            }
            LogicalPlan::Aggregate(aggregate) => {
                aggregate.input.for_each_expr_mut(visit)?;
                if let Some(group_by) = &mut aggregate.group_by {
                    group_by.exprs.iter_mut().try_for_each(|expr| visit(expr))?;
                    if let Some(having) = &mut group_by.having {
                        having.iter_mut().try_for_each(|expr| visit(expr))?;
                    }
                }
                for aggregate in &mut aggregate.aggregates {
                    visit(&mut aggregate.original_expr)?;
                    aggregate.args.iter_mut().try_for_each(|expr| visit(expr))?;
                    if let Some(filter) = &mut aggregate.filter_expr {
                        visit(filter)?;
                    }
                }
                Ok(())
            }
            LogicalPlan::Project(project) => {
                project.input.for_each_expr_mut(visit)?;
                project
                    .columns
                    .iter_mut()
                    .try_for_each(|column| visit(&mut column.expr))
            }
            LogicalPlan::Distinct(distinct) => distinct.input.for_each_expr_mut(visit),
            LogicalPlan::Sort(sort) => {
                sort.input.for_each_expr_mut(visit)?;
                sort.keys
                    .iter_mut()
                    .try_for_each(|(expr, _, _)| visit(expr.as_mut()))
            }
            LogicalPlan::Limit(limit) => {
                limit.input.for_each_expr_mut(visit)?;
                if let Some(expr) = &mut limit.limit {
                    visit(expr.as_mut())?;
                }
                if let Some(expr) = &mut limit.offset {
                    visit(expr.as_mut())?;
                }
                Ok(())
            }
        }
    }

    /// Visit every node of the tree, parents before children. Nested blocks
    /// are not visited.
    pub fn for_each_node<'a>(&'a self, visit: &mut impl FnMut(&'a LogicalPlan)) {
        visit(self);
        match self {
            LogicalPlan::OneRow | LogicalPlan::Scan(_) | LogicalPlan::DerivedTable(_) => {}
            LogicalPlan::Join(join) => {
                join.left.for_each_node(visit);
                join.right.for_each_node(visit);
            }
            LogicalPlan::Filter(node) => node.input.for_each_node(visit),
            LogicalPlan::Aggregate(node) => node.input.for_each_node(visit),
            LogicalPlan::Project(node) => node.input.for_each_node(visit),
            LogicalPlan::Distinct(node) => node.input.for_each_node(visit),
            LogicalPlan::Sort(node) => node.input.for_each_node(visit),
            LogicalPlan::Limit(node) => node.input.for_each_node(visit),
        }
    }
}

/// Add the tables that an expression reads to `out`.
pub(crate) fn collect_table_ids(expr: &Expr, out: &mut Vec<TableInternalId>) {
    walk_expr(expr, &mut |expr: &Expr| -> Result<WalkControl> {
        match expr {
            Expr::Column { table, .. } | Expr::RowId { table, .. } => {
                if !out.contains(table) {
                    out.push(*table);
                }
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })
    .expect("collecting table ids cannot fail");
}

/// The join tree below the unary operators of a block.
pub(crate) fn join_tree(root: &LogicalPlan) -> &LogicalPlan {
    let mut node = root;
    loop {
        match node {
            LogicalPlan::Limit(next) => node = &next.input,
            LogicalPlan::Sort(next) => node = &next.input,
            LogicalPlan::Distinct(next) => node = &next.input,
            LogicalPlan::Project(next) => node = &next.input,
            LogicalPlan::Aggregate(next) => node = &next.input,
            LogicalPlan::Filter(next) => node = &next.input,
            LogicalPlan::OneRow
            | LogicalPlan::Scan(_)
            | LogicalPlan::DerivedTable(_)
            | LogicalPlan::Join(_) => return node,
        }
    }
}

pub(crate) fn join_tree_mut(root: &mut LogicalPlan) -> &mut LogicalPlan {
    let mut node = root;
    loop {
        match node {
            LogicalPlan::Limit(next) => node = &mut next.input,
            LogicalPlan::Sort(next) => node = &mut next.input,
            LogicalPlan::Distinct(next) => node = &mut next.input,
            LogicalPlan::Project(next) => node = &mut next.input,
            LogicalPlan::Aggregate(next) => node = &mut next.input,
            LogicalPlan::Filter(next) => node = &mut next.input,
            LogicalPlan::OneRow
            | LogicalPlan::Scan(_)
            | LogicalPlan::DerivedTable(_)
            | LogicalPlan::Join(_) => return node,
        }
    }
}

/// The filter directly above the join tree of a block, if there is one.
pub(crate) fn filter_node(root: &LogicalPlan) -> Option<&Filter> {
    let mut node = root;
    loop {
        match node {
            LogicalPlan::Limit(next) => node = &next.input,
            LogicalPlan::Sort(next) => node = &next.input,
            LogicalPlan::Distinct(next) => node = &next.input,
            LogicalPlan::Project(next) => node = &next.input,
            LogicalPlan::Aggregate(next) => node = &next.input,
            LogicalPlan::Filter(filter) => return Some(filter),
            LogicalPlan::OneRow
            | LogicalPlan::Scan(_)
            | LogicalPlan::DerivedTable(_)
            | LogicalPlan::Join(_) => return None,
        }
    }
}

/// The filter directly above the join tree. It is created when missing.
pub(crate) fn filter_slot(root: &mut LogicalPlan) -> &mut Filter {
    let mut node = root;
    loop {
        match node {
            LogicalPlan::Limit(next) => node = &mut next.input,
            LogicalPlan::Sort(next) => node = &mut next.input,
            LogicalPlan::Distinct(next) => node = &mut next.input,
            LogicalPlan::Project(next) => node = &mut next.input,
            LogicalPlan::Aggregate(next) => node = &mut next.input,
            LogicalPlan::Filter(_) => break,
            LogicalPlan::OneRow
            | LogicalPlan::Scan(_)
            | LogicalPlan::DerivedTable(_)
            | LogicalPlan::Join(_) => {
                let joins = std::mem::replace(node, LogicalPlan::OneRow);
                *node = LogicalPlan::Filter(Filter {
                    input: Box::new(joins),
                    terms: Vec::new(),
                });
                break;
            }
        }
    }
    let LogicalPlan::Filter(filter) = node else {
        unreachable!("the loop stops at a filter")
    };
    filter
}

/// The table ids of the leaves of a join tree, left to right.
pub(crate) fn leaf_ids(node: &LogicalPlan) -> Vec<TableInternalId> {
    let mut ids = Vec::new();
    node.for_each_node(&mut |node| {
        if let Some(id) = node.leaf_id() {
            ids.push(id);
        }
    });
    ids
}
