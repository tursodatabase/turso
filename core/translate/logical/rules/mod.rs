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
