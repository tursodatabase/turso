use turso_parser::ast::Name;

#[allow(unused_imports)]
use crate::turso_assert;
use crate::{
    vdbe::{builder::ProgramBuilder, insn::Insn},
    Result,
};

pub fn translate_rollback(
    mut program: ProgramBuilder,
    txn_name: Option<Name>,
    savepoint_name: Option<Name>,
) -> Result<ProgramBuilder> {
    turso_assert!(
        txn_name.is_none() && savepoint_name.is_none(),
        "rollback: txn_name and savepoint not supported yet"
    );
    program.emit_insn(Insn::AutoCommit {
        auto_commit: true,
        rollback: true,
    });
    program.rollback();
    Ok(program)
}
