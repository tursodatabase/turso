use limbo_sqlite3_parser::ast::Name;

use crate::{
    schema::Schema,
    translate::emitter::TransactionMode,
    vdbe::{
        builder::{ProgramBuilder, QueryMode},
        insn::Insn,
    },
    Result, SymbolTable,
};

pub fn translate_rollback(
    _query_mode: QueryMode,
    _schema: &Schema,
    _syms: &SymbolTable,
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
    program.epilogue_maybe_rollback(TransactionMode::None, true);
    Ok(program)
}
