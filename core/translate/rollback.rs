use turso_parser::ast::Name;

use crate::{
    vdbe::{builder::ProgramBuilder, insn::Insn},
    Result,
};

pub fn translate_rollback(
    mut program: ProgramBuilder,
    txn_name: Option<Name>,
    savepoint_name: Option<Name>,
) -> Result<ProgramBuilder> {
    assert!(
        txn_name.is_none() && savepoint_name.is_none(),
        "txn_name and savepoint not supported yet"
    );
    program.emit_insn(Insn::AutoCommit {
        auto_commit: true,
        rollback: true,
    });
    program.rollback();
    Ok(program)
}
