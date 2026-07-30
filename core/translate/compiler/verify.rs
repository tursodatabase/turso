//! Structural verification of a [`Function`] before emission.
//!
//! Emission assumes a well-formed CFG; the verifier is what makes that
//! assumption safe. It rejects malformed IR *before* bytecode exists, so a
//! compiler-construction bug surfaces as a diagnosable error instead of a
//! corrupt VDBE program. Checks:
//!
//! 1. The entry block has no parameters (there is no edge to bind them).
//! 2. Every reachable block has a terminator.
//! 3. Every edge's argument count matches the target's parameter count.
//! 4. Every use is dominated by its definition: same block earlier, or a
//!    strictly dominating block. Block parameters dominate their block.
//!
//! Unreachable blocks are ignored (emission skips them too).

use std::fmt;

use super::ir::{BlockId, DefSite, Function, Terminator, ValueId};

#[derive(Debug, PartialEq, Eq)]
pub enum VerifyError {
    EntryHasParams,
    JumpToEntry {
        from: BlockId,
    },
    MissingTerminator {
        block: BlockId,
    },
    ArityMismatch {
        from: BlockId,
        to: BlockId,
        args: usize,
        params: usize,
    },
    UseNotDominatedByDef {
        block: BlockId,
        value: ValueId,
    },
    CmpBranchNullTarget {
        block: BlockId,
    },
}

impl fmt::Display for VerifyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VerifyError::EntryHasParams => {
                write!(f, "entry block must not have block parameters")
            }
            VerifyError::JumpToEntry { from } => {
                write!(
                    f,
                    "block {from:?} jumps to the entry block; loop headers must be separate blocks"
                )
            }
            VerifyError::MissingTerminator { block } => {
                write!(f, "reachable block {block:?} has no terminator")
            }
            VerifyError::ArityMismatch {
                from,
                to,
                args,
                params,
            } => write!(
                f,
                "edge {from:?} -> {to:?} passes {args} argument(s) but the target has {params} parameter(s)"
            ),
            VerifyError::UseNotDominatedByDef { block, value } => write!(
                f,
                "value {value:?} is used in block {block:?} but its definition does not dominate the use"
            ),
            VerifyError::CmpBranchNullTarget { block } => write!(
                f,
                "CmpBranch in block {block:?} has a NULL target distinct from both the true and false targets; VDBE comparison jumps route NULL via a flag"
            ),
        }
    }
}

pub fn verify(func: &Function) -> Result<(), VerifyError> {
    if !func.block(BlockId::ENTRY).params.is_empty() {
        return Err(VerifyError::EntryHasParams);
    }

    let reachable = reachable_order(func)?;

    // Edge arity, checked for every edge leaving a reachable block.
    for &block_id in &reachable {
        let terminator = func
            .block(block_id)
            .terminator
            .as_ref()
            .expect("reachable blocks were checked for terminators");
        for target in terminator.targets() {
            // The entry block has no label at emission time (it is
            // emitted first), so edges back into it cannot be encoded;
            // loop headers must be blocks of their own.
            if target.block == BlockId::ENTRY {
                return Err(VerifyError::JumpToEntry { from: block_id });
            }
            let params = func.block(target.block).params.len();
            if target.args.len() != params {
                return Err(VerifyError::ArityMismatch {
                    from: block_id,
                    to: target.block,
                    args: target.args.len(),
                    params,
                });
            }
        }
    }

    let dom = Dominators::compute(func, &reachable);

    // Def-before-use. Instruction operands must be defined strictly
    // before the use in the same block, or in a strict dominator; jumps
    // only occur at terminators, so everything in a dominating block
    // executes before control reaches the user.
    for &block_id in &reachable {
        let block = func.block(block_id);
        let check = |value: ValueId, use_index: usize| -> Result<(), VerifyError> {
            let ok = match func.def_site(value) {
                DefSite::Param { block, .. } => {
                    block == block_id || dom.strictly_dominates(block, block_id)
                }
                DefSite::Inst { block, index } => {
                    if block == block_id {
                        index < use_index
                    } else {
                        dom.strictly_dominates(block, block_id)
                    }
                }
            };
            if ok {
                Ok(())
            } else {
                Err(VerifyError::UseNotDominatedByDef {
                    block: block_id,
                    value,
                })
            }
        };

        for (index, (_, inst)) in block.insts.iter().enumerate() {
            match inst {
                super::ir::Inst::Const(_)
                | super::ir::Inst::External { .. }
                | super::ir::Inst::Leaf(_) => {}
                super::ir::Inst::Unary { operand, .. }
                | super::ir::Inst::NullTest { operand, .. }
                | super::ir::Inst::Cast { operand, .. }
                | super::ir::Inst::Truth { operand, .. } => check(*operand, index)?,
                super::ir::Inst::Binary { lhs, rhs, .. }
                | super::ir::Inst::Compare { lhs, rhs, .. } => {
                    check(*lhs, index)?;
                    check(*rhs, index)?;
                }
                super::ir::Inst::Call { args, .. } => {
                    for &arg in args {
                        check(arg, index)?;
                    }
                }
                super::ir::Inst::EmitRow { values } => {
                    for &value in values {
                        check(value, index)?;
                    }
                }
            }
        }

        let end = block.insts.len();
        let terminator = block.terminator.as_ref().expect("checked above");
        match terminator {
            Terminator::Jump(_) | Terminator::Exit(_) => {}
            Terminator::Branch { cond, .. } => check(*cond, end)?,
            Terminator::CmpBranch {
                lhs,
                rhs,
                if_true,
                if_false,
                if_null,
                ..
            } => {
                check(*lhs, end)?;
                check(*rhs, end)?;
                if if_null != if_true && if_null != if_false {
                    return Err(VerifyError::CmpBranchNullTarget { block: block_id });
                }
            }
            Terminator::NullBranch { value, .. } => check(*value, end)?,
            Terminator::Ret { value } => check(*value, end)?,
            Terminator::Rewind { .. }
            | Terminator::Next { .. }
            | Terminator::DecrJumpZero { .. }
            | Terminator::IfPos { .. } => {}
        }
        for target in terminator.targets() {
            for &arg in &target.args {
                check(arg, end)?;
            }
        }
    }

    Ok(())
}

/// Reachable blocks in reverse postorder (entry first), verifying along
/// the way that each has a terminator.
fn reachable_order(func: &Function) -> Result<Vec<BlockId>, VerifyError> {
    let num_blocks = func.blocks.len();
    let mut visited = vec![false; num_blocks];
    let mut postorder = Vec::with_capacity(num_blocks);
    // Iterative DFS; (block, next successor index to visit).
    let mut stack: Vec<(BlockId, usize)> = vec![(BlockId::ENTRY, 0)];
    visited[BlockId::ENTRY.index()] = true;
    while let Some(&mut (block_id, ref mut next)) = stack.last_mut() {
        let terminator = func
            .block(block_id)
            .terminator
            .as_ref()
            .ok_or(VerifyError::MissingTerminator { block: block_id })?;
        let targets = terminator.targets();
        if *next < targets.len() {
            let successor = targets[*next].block;
            *next += 1;
            if !visited[successor.index()] {
                visited[successor.index()] = true;
                stack.push((successor, 0));
            }
        } else {
            postorder.push(block_id);
            stack.pop();
        }
    }
    postorder.reverse();
    Ok(postorder)
}

/// Immediate-dominator tree over the reachable subgraph, computed with the
/// standard Cooper–Harvey–Kennedy iterative algorithm on reverse
/// postorder. Function CFGs here are small; simplicity over asymptotics.
struct Dominators {
    /// Immediate dominator per block index; `None` for the entry and for
    /// unreachable blocks. Encoded as reverse-postorder positions.
    idom: Vec<Option<usize>>,
    /// Block index -> reverse-postorder position (usize::MAX if
    /// unreachable).
    rpo_position: Vec<usize>,
    /// Reverse-postorder position -> block id.
    order: Vec<BlockId>,
}

impl Dominators {
    fn compute(func: &Function, order: &[BlockId]) -> Self {
        let num_blocks = func.blocks.len();
        let mut rpo_position = vec![usize::MAX; num_blocks];
        for (position, &block) in order.iter().enumerate() {
            rpo_position[block.index()] = position;
        }

        // Predecessors, in reverse-postorder positions.
        let mut preds: Vec<Vec<usize>> = vec![Vec::new(); order.len()];
        for (position, &block) in order.iter().enumerate() {
            if let Some(terminator) = &func.block(block).terminator {
                for target in terminator.targets() {
                    let target_pos = rpo_position[target.block.index()];
                    if target_pos != usize::MAX {
                        preds[target_pos].push(position);
                    }
                }
            }
        }

        let mut idom: Vec<Option<usize>> = vec![None; order.len()];
        idom[0] = Some(0); // entry is its own idom during iteration
        let mut changed = true;
        while changed {
            changed = false;
            for position in 1..order.len() {
                let mut new_idom: Option<usize> = None;
                for &pred in &preds[position] {
                    if idom[pred].is_none() {
                        continue;
                    }
                    new_idom = Some(match new_idom {
                        None => pred,
                        Some(current) => Self::intersect(&idom, pred, current),
                    });
                }
                if new_idom.is_some() && idom[position] != new_idom {
                    idom[position] = new_idom;
                    changed = true;
                }
            }
        }
        // Entry's idom is conventionally itself during iteration; clear it
        // so `strictly_dominates` treats the entry as dominated only by
        // itself.
        idom[0] = None;

        Self {
            idom,
            rpo_position,
            order: order.to_vec(),
        }
    }

    fn intersect(idom: &[Option<usize>], mut a: usize, mut b: usize) -> usize {
        while a != b {
            while a > b {
                a = idom[a].unwrap_or(0);
            }
            while b > a {
                b = idom[b].unwrap_or(0);
            }
        }
        a
    }

    /// Does `a` strictly dominate `b`? Entry dominates everything
    /// reachable.
    fn strictly_dominates(&self, a: BlockId, b: BlockId) -> bool {
        let a_pos = self.rpo_position[a.index()];
        let b_pos = self.rpo_position[b.index()];
        if a_pos == usize::MAX || b_pos == usize::MAX || a_pos == b_pos {
            return false;
        }
        debug_assert_eq!(self.order[a_pos], a);
        let mut current = b_pos;
        while let Some(next) = self.idom[current] {
            if next == a_pos {
                return true;
            }
            current = next;
        }
        // Reached the entry without meeting `a`; only the entry itself
        // dominates here.
        a_pos == 0
    }
}
