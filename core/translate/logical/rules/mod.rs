//! Rewrite rules over a `Block` tree.
//!
//! Rules come in two forms. The rules in the `.opt` files are written in
//! the rule language of `super::optgen` and run in the engine of `engine`;
//! they normalize expressions and filter terms. The rules in `decorrelate`
//! and `flatten` are structs with a `Rule` implementation, for rewrites that
//! need more than a pattern. The driver runs the rules on every nested block
//! first, then on the block itself, until no rule changes anything.

use turso_parser::ast::{Expr, TableInternalId};

use crate::schema::Table;
use crate::translate::emitter::Resolver;
use crate::translate::plan::{TableReferences, WhereTerm};
use crate::vdbe::builder::TableRefIdCounter;
use crate::{LimboError, Result};

use super::{Block, Filter, LogicalPlan};

pub(crate) mod decorrelate;
pub(crate) mod engine;
pub(crate) mod flatten;
pub(crate) mod funcs;
pub(crate) mod nodes;

pub(crate) use nodes::Context;

/// Normalize one expression with the rules of the `.opt` files. Return
/// whether it changed.
pub(crate) fn normalize_expr(
    expr: &mut Expr,
    context: Context,
    resolver: Option<&Resolver<'_>>,
) -> Result<bool> {
    let rules = engine::rule_set();
    let context_of_engine = engine::EngineContext {
        resolver,
        rules,
        virtual_tables: &[],
    };
    rules.normalize_expr(&context_of_engine, expr, context)
}

/// The virtual tables of a prepared plan, for `normalize_where_clause`.
pub(crate) fn virtual_table_ids(table_references: &TableReferences) -> Vec<TableInternalId> {
    table_references
        .joined_tables()
        .iter()
        .filter(|table| matches!(table.table, Table::Virtual(_)))
        .map(|table| table.internal_id)
        .collect()
}

pub(crate) enum WhereClauseOutcome {
    Continue,
    /// A term is always false, so the query returns no rows. Every term is
    /// marked consumed.
    AlwaysFalse,
}

/// Normalize the WHERE and ON terms of a prepared plan with the rules of
/// the `.opt` files: fold constants, split AND terms, drop true terms, take
/// shared conjuncts out of OR terms, and more.
pub(crate) fn normalize_where_clause(
    where_clause: &mut Vec<WhereTerm>,
    resolver: &Resolver<'_>,
    virtual_tables: &[TableInternalId],
) -> Result<WhereClauseOutcome> {
    if where_clause.is_empty() {
        return Ok(WhereClauseOutcome::Continue);
    }
    let terms = std::mem::take(where_clause);
    let mut node = LogicalPlan::Filter(Filter {
        input: Box::new(LogicalPlan::OneRow),
        terms,
    });
    let context = engine::EngineContext {
        resolver: Some(resolver),
        rules: engine::rule_set(),
        virtual_tables,
    };
    context.rules.normalize_plan(&context, &mut node)?;
    match node {
        LogicalPlan::Filter(filter) => *where_clause = filter.terms,
        LogicalPlan::OneRow => {}
        _ => {
            return Err(LimboError::InternalError(
                "logical plan rules changed a filter into another node".to_string(),
            ))
        }
    }
    let always_false = matches!(where_clause.as_slice(), [term]
        if term.from_outer_join.is_none()
            && !term.consumed
            && funcs::constant_truth(&term.expr) == Some(false));
    if always_false {
        for term in where_clause.iter_mut() {
            term.consumed = true;
        }
        return Ok(WhereClauseOutcome::AlwaysFalse);
    }
    Ok(WhereClauseOutcome::Continue)
}

/// The rules of the `.opt` files, as one rule of the driver.
pub(crate) struct Normalize;

impl Rule for Normalize {
    fn name(&self) -> &'static str {
        "Normalize"
    }

    fn apply(&self, block: &mut Block, context: &mut RuleContext<'_, '_>) -> Result<bool> {
        let mut virtual_tables = Vec::new();
        block.root.for_each_node(&mut |node| {
            if let LogicalPlan::Scan(scan) = node {
                if matches!(scan.table.table, Table::Virtual(_)) {
                    virtual_tables.push(scan.table.internal_id);
                }
            }
        });
        let engine_context = engine::EngineContext {
            resolver: Some(context.resolver),
            rules: engine::rule_set(),
            virtual_tables: &virtual_tables,
        };
        engine_context
            .rules
            .normalize_plan(&engine_context, &mut block.root)
    }
}

pub(crate) struct RuleContext<'a, 'r> {
    pub resolver: &'a Resolver<'r>,
    pub ids: &'a mut TableRefIdCounter,
}

pub(crate) trait Rule {
    fn name(&self) -> &'static str;

    /// Apply the rule to one block. Return `true` when the block changed.
    fn apply(&self, block: &mut Block, context: &mut RuleContext<'_, '_>) -> Result<bool>;
}

const RULES: &[&dyn Rule] = &[
    &Normalize,
    &decorrelate::PushDependentJoinThroughProject,
    &decorrelate::PushDependentJoinThroughAggregate,
    &decorrelate::PushDependentJoinThroughFilter,
    &decorrelate::PushDependentJoinThroughDistinct,
    &decorrelate::PushDependentJoinThroughJoin,
    &decorrelate::DependentJoinToJoin,
    &decorrelate::IntroduceDomain,
    &flatten::FlattenDerivedTables,
];

const MAX_ROUNDS: usize = 32;

/// Rewrite a block until no rule changes it. Return whether it changed.
pub(crate) fn rewrite_block(block: &mut Block, context: &mut RuleContext<'_, '_>) -> Result<bool> {
    let mut changed_once = false;
    for _ in 0..MAX_ROUNDS {
        let mut changed = rewrite_nested_blocks(&mut block.root, context)?;
        for rule in RULES {
            if rule.apply(block, context)? {
                tracing::trace!(rule = rule.name(), "logical plan rule changed the block");
                changed = true;
            }
        }
        if !changed {
            return Ok(changed_once);
        }
        changed_once = true;
    }
    Err(LimboError::InternalError(
        "logical plan rules did not stop changing the plan".to_string(),
    ))
}

fn rewrite_nested_blocks(
    node: &mut LogicalPlan,
    context: &mut RuleContext<'_, '_>,
) -> Result<bool> {
    match node {
        LogicalPlan::DerivedTable(derived) => rewrite_block(&mut derived.block, context),
        LogicalPlan::Join(join) => {
            let left = rewrite_nested_blocks(&mut join.left, context)?;
            let right = rewrite_nested_blocks(&mut join.right, context)?;
            Ok(left || right)
        }
        LogicalPlan::DependentJoin(join) => {
            let left = rewrite_nested_blocks(&mut join.left, context)?;
            let right = rewrite_nested_blocks(&mut join.right, context)?;
            Ok(left || right)
        }
        LogicalPlan::OneRow | LogicalPlan::Scan(_) => Ok(false),
        node => match node.input_mut() {
            Some(input) => rewrite_nested_blocks(input, context),
            None => Ok(false),
        },
    }
}
