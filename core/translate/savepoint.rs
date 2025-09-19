use crate::savepoint::SavepointOp;
use crate::schema::Schema;
use crate::translate::{ProgramBuilder, ProgramBuilderOpts};
use crate::vdbe::insn::Insn;
use crate::Result;
use turso_parser::ast::Name;

/// Translate SAVEPOINT statement into VDBE bytecode.
///
/// Creates a new savepoint with the given name. If this is the first savepoint
/// and we're not in a transaction, it behaves like BEGIN DEFERRED.
pub fn translate_savepoint(
    name: Name,
    _schema: &Schema,
    mut program: ProgramBuilder,
) -> Result<ProgramBuilder> {
    program.extend(&ProgramBuilderOpts {
        num_cursors: 0,
        approx_num_insns: 1,
        approx_num_labels: 0,
    });

    program.emit_insn(Insn::Savepoint {
        op: SavepointOp::Begin,
        name: name.as_str().to_string(),
    });

    Ok(program)
}

/// Translate RELEASE statement into VDBE bytecode.
///
/// Releases a savepoint and all savepoints created after it. If this releases
/// the outermost savepoint, it's equivalent to COMMIT.
pub fn translate_release(
    name: Name,
    _schema: &Schema,
    mut program: ProgramBuilder,
) -> Result<ProgramBuilder> {
    program.extend(&ProgramBuilderOpts {
        num_cursors: 0,
        approx_num_insns: 1,
        approx_num_labels: 0,
    });

    program.emit_insn(Insn::Savepoint {
        op: SavepointOp::Release,
        name: name.as_str().to_string(),
    });

    Ok(program)
}

/// Translate ROLLBACK TO savepoint statement into VDBE bytecode.
///
/// Rolls back the database to the state it was in when the savepoint was created.
/// All savepoints created after the named savepoint are removed, but the named
/// savepoint remains.
pub fn translate_rollback_to_savepoint(
    savepoint_name: Name,
    _schema: &Schema,
    mut program: ProgramBuilder,
) -> Result<ProgramBuilder> {
    program.extend(&ProgramBuilderOpts {
        num_cursors: 0,
        approx_num_insns: 1,
        approx_num_labels: 0,
    });

    program.emit_insn(Insn::Savepoint {
        op: SavepointOp::Rollback,
        name: savepoint_name.as_str().to_string(),
    });

    Ok(program)
}
