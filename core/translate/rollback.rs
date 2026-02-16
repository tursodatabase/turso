use turso_parser::ast::Name;

use crate::{
    bail_parse_error,
    vdbe::{builder::ProgramBuilder, insn::Insn},
    Result,
};

pub fn translate_rollback(
    mut program: ProgramBuilder,
    txn_name: Option<Name>,
    savepoint_name: Option<Name>,
) -> Result<ProgramBuilder> {
    if txn_name.is_some() || savepoint_name.is_some() {
        bail_parse_error!("txn_name and savepoint not supported yet");
    }

    program.emit_insn(Insn::AutoCommit {
        auto_commit: true,
        rollback: true,
    });
    program.rollback();
    Ok(program)
}
