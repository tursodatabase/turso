use crate::translate::{ProgramBuilder, ProgramBuilderOpts};
use crate::vdbe::insn::Insn;
use crate::{QueryMode, Result};
use limbo_sqlite3_parser::ast::{Name, TransactionType};

pub fn translate_tx_begin(
    tx_type: Option<TransactionType>,
    _tx_name: Option<Name>,
    mut program: ProgramBuilder,
) -> Result<ProgramBuilder> {
    program.extend(&ProgramBuilderOpts {
        query_mode: QueryMode::Normal,
        num_cursors: 0,
        approx_num_insns: 0,
        approx_num_labels: 0,
    });
    let tx_type = tx_type.unwrap_or(TransactionType::Deferred);
    match tx_type {
        TransactionType::Deferred => {
            program.emit_insn(Insn::AutoCommit {
                auto_commit: false,
                rollback: false,
            });
        }
        TransactionType::Immediate | TransactionType::Exclusive => {
            program.emit_insn(Insn::Transaction { write: true });
            // TODO: Emit transaction instruction on temporary tables when we support them.
            program.emit_insn(Insn::AutoCommit {
                auto_commit: false,
                rollback: false,
            });
        }
    }
    program.epilogue(super::emitter::TransactionMode::None);
    Ok(program)
}

pub fn translate_tx_commit(
    _tx_name: Option<Name>,
    mut program: ProgramBuilder,
) -> Result<ProgramBuilder> {
    program.extend(&ProgramBuilderOpts {
        query_mode: QueryMode::Normal,
        num_cursors: 0,
        approx_num_insns: 0,
        approx_num_labels: 0,
    });
    program.emit_insn(Insn::AutoCommit {
        auto_commit: true,
        rollback: false,
    });
    program.epilogue(super::emitter::TransactionMode::None);
    Ok(program)
}
