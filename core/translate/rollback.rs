use crate::{
    vdbe::{
        builder::ProgramBuilder,
        insn::{Insn, SavepointOp},
    },
    Result,
};
use turso_parser::ast::Name;

pub fn translate_savepoint(mut program: ProgramBuilder, name: Name) -> Result<ProgramBuilder> {
    program.emit_insn(Insn::Savepoint {
        op: SavepointOp::Begin,
        name: name.as_str().to_string(),
    });
    Ok(program)
}

pub fn translate_release(mut program: ProgramBuilder, name: Name) -> Result<ProgramBuilder> {
    program.emit_insn(Insn::Savepoint {
        op: SavepointOp::Release,
        name: name.as_str().to_string(),
    });
    Ok(program)
}

pub fn translate_rollback(
    mut program: ProgramBuilder,
    _txn_name: Option<Name>,
    savepoint_name: Option<Name>,
) -> Result<ProgramBuilder> {
    if let Some(savepoint_name) = savepoint_name {
        program.emit_insn(Insn::Savepoint {
            op: SavepointOp::RollbackTo,
            name: savepoint_name.as_str().to_string(),
        });
    } else {
        program.emit_insn(Insn::AutoCommit {
            auto_commit: true,
            rollback: true,
        });
        program.rollback();
    }
    Ok(program)
}
