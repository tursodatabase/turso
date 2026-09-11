//! Rewrite rules over a `Block` tree.
//!
//! A rule is one struct that matches a shape in the tree and replaces it,
//! in the style of the CockroachDB normalization rules. The driver runs the
//! rules on every nested block first, then on the block itself, until no rule
//! changes anything.

use crate::translate::emitter::Resolver;
use crate::vdbe::builder::TableRefIdCounter;
use crate::{LimboError, Result};

use super::{Block, LogicalPlan};

pub(crate) mod decorrelate;
pub(crate) mod flatten;

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
    &decorrelate::DecorrelateScalarAggregates,
    &flatten::FlattenDerivedTables,
];

const MAX_ROUNDS: usize = 8;

pub(crate) fn rewrite_block(block: &mut Block, context: &mut RuleContext<'_, '_>) -> Result<()> {
    rewrite_nested_blocks(&mut block.root, context)?;
    for _ in 0..MAX_ROUNDS {
        let mut changed = false;
        for rule in RULES {
            if rule.apply(block, context)? {
                tracing::trace!(rule = rule.name(), "logical plan rule changed the block");
                changed = true;
            }
        }
        if !changed {
            return Ok(());
        }
    }
    Err(LimboError::InternalError(
        "logical plan rules did not stop changing the plan".to_string(),
    ))
}

fn rewrite_nested_blocks(node: &mut LogicalPlan, context: &mut RuleContext<'_, '_>) -> Result<()> {
    match node {
        LogicalPlan::DerivedTable(derived) => rewrite_block(&mut derived.block, context),
        LogicalPlan::Join(join) => {
            rewrite_nested_blocks(&mut join.left, context)?;
            rewrite_nested_blocks(&mut join.right, context)
        }
        LogicalPlan::OneRow | LogicalPlan::Scan(_) => Ok(()),
        node => match node.input_mut() {
            Some(input) => rewrite_nested_blocks(input, context),
            None => Ok(()),
        },
    }
}
