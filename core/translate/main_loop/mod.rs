use turso_parser::ast::SortOrder;

use super::plan::{IterationDirection, SeekDef, SeekKey, SeekKeyComponent};
use crate::{
    schema::{Index, IndexColumn},
    translate::collate::CollationSeq,
    turso_assert,
    types::SeekOp,
    vdbe::{
        affinity::{self, Affinity},
        builder::{CursorType, ProgramBuilder},
        insn::{to_u32, CmpInsFlags, IdxInsertFlags, Insn},
        BranchOffset, CursorID,
    },
    Result,
};
use std::sync::Arc;

mod body;
mod close;
mod conditions;
mod hash;
mod in_seek;
mod init;
mod multi_index;
mod open;
mod seek;

pub(crate) use close::{emit_autoindex, AutoIndexBuild};
pub(crate) use in_seek::{
    emit_in_seek_advance, emit_in_seek_start, open_in_seek_values_cursor, InSeekLoop,
};
pub(crate) use seek::{SeekEmitter, SeekExpressionLowering};

#[derive(Clone, Copy, Debug)]
pub struct LeftJoinMetadata {
    pub reg_match_flag: usize,
    pub label_match_flag_set_true: BranchOffset,
    pub label_match_flag_check_value: BranchOffset,
}

impl LeftJoinMetadata {
    pub(crate) fn new(program: &mut ProgramBuilder) -> Self {
        Self {
            reg_match_flag: program.alloc_register(),
            label_match_flag_set_true: program.allocate_label(),
            label_match_flag_check_value: program.allocate_label(),
        }
    }

    pub(crate) fn reset(&self, program: &mut ProgramBuilder) {
        program.emit_insn(Insn::Integer {
            value: 0,
            dest: self.reg_match_flag,
        });
    }

    pub(crate) fn mark_matched(&self, program: &mut ProgramBuilder) {
        program.preassign_label_to_next_insn(self.label_match_flag_set_true);
        program.emit_insn(Insn::Integer {
            value: 1,
            dest: self.reg_match_flag,
        });
    }

    pub(crate) fn begin_unmatched_row(&self, program: &mut ProgramBuilder) -> BranchOffset {
        program.preassign_label_to_next_insn(self.label_match_flag_check_value);
        let finished = program.allocate_label();
        program.emit_insn(Insn::IfPos {
            reg: self.reg_match_flag,
            target_pc: finished,
            decrement_by: 0,
        });
        finished
    }

    pub(crate) fn finish_unmatched_row(
        &self,
        program: &mut ProgramBuilder,
        finished: BranchOffset,
    ) {
        program.emit_insn(Insn::Goto {
            target_pc: self.label_match_flag_set_true,
        });
        program.preassign_label_to_next_insn(finished);
    }
}
