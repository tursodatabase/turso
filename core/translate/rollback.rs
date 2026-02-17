use crate::{
    vdbe::{
        builder::ProgramBuilder,
        insn::{Insn, SavepointOp},
    },
    Result,
};
use turso_parser::ast::Name;

/// Emits bytecode for `SAVEPOINT <name>`.
pub fn translate_savepoint(program: &mut ProgramBuilder, name: Name) -> Result<()> {
    program.emit_insn(Insn::Savepoint {
        op: SavepointOp::Begin,
        name: name.as_str().to_ascii_lowercase(),
    });
    Ok(())
}

/// Emits bytecode for `RELEASE [SAVEPOINT] <name>`.
pub fn translate_release(program: &mut ProgramBuilder, name: Name) -> Result<()> {
    program.emit_insn(Insn::Savepoint {
        op: SavepointOp::Release,
        name: name.as_str().to_ascii_lowercase(),
    });
    Ok(())
}

/// Emits bytecode for either full transaction rollback or `ROLLBACK TO` named savepoint.
pub fn translate_rollback(
    program: &mut ProgramBuilder,
    _txn_name: Option<Name>,
    savepoint_name: Option<Name>,
) -> Result<()> {
    if let Some(savepoint_name) = savepoint_name {
        program.emit_insn(Insn::Savepoint {
            op: SavepointOp::RollbackTo,
            name: savepoint_name.as_str().to_ascii_lowercase(),
        });
    } else {
        program.emit_insn(Insn::AutoCommit {
            auto_commit: true,
            rollback: true,
        });
        program.rollback();
    }
    Ok(())
}
