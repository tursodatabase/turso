use crate::vdbe::{builder::ProgramBuilder, insn::Insn, BranchOffset};
use crate::Result;
use tracing::{instrument, Level};

#[derive(Debug, Clone, Copy)]
pub struct ConditionMetadata {
    pub jump_if_condition_is_true: bool,
    pub jump_target_when_true: BranchOffset,
    pub jump_target_when_false: BranchOffset,
}

#[instrument(skip_all, level = Level::DEBUG)]
pub(crate) fn emit_cond_jump(
    program: &mut ProgramBuilder,
    cond_meta: ConditionMetadata,
    reg: usize,
) {
    if cond_meta.jump_if_condition_is_true {
        program.emit_insn(Insn::If {
            reg,
            target_pc: cond_meta.jump_target_when_true,
            jump_if_null: false,
        });
    } else {
        program.emit_insn(Insn::IfNot {
            reg,
            target_pc: cond_meta.jump_target_when_false,
            jump_if_null: true,
        });
    }
}

pub(crate) fn assert_register_range_allocated(
    program: &mut ProgramBuilder,
    start_register: usize,
    count: usize,
) -> Result<()> {
    // Invariant: callers must have pre-allocated [start_register, start_register + count)
    // before asking expression translation to write a vector into that range.
    let required_next = start_register + count;
    let next_free = program.peek_next_register();
    if required_next <= next_free {
        Ok(())
    } else {
        crate::bail_parse_error!(
            "insufficient registers allocated for expression vector write (start={start_register}, count={count}, next_free={next_free})"
        )
    }
}
