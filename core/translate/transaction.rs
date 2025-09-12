use crate::schema::Schema;
use crate::translate::{emitter::TransactionMode, ProgramBuilder, ProgramBuilderOpts};
use crate::vdbe::insn::Insn;
use crate::Result;
use turso_parser::ast::{Name, TransactionType};

pub fn translate_tx_begin(
    tx_type: Option<TransactionType>,
    _tx_name: Option<Name>,
    schema: &Schema,
    mut program: ProgramBuilder,
) -> Result<ProgramBuilder> {
    program.extend(&ProgramBuilderOpts {
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
            program.emit_insn(Insn::Transaction {
                db: 0,
                tx_mode: TransactionMode::Write,
                schema_cookie: schema.schema_version,
            });
            // TODO: Emit transaction instruction on temporary tables when we support them.
            program.emit_insn(Insn::AutoCommit {
                auto_commit: false,
                rollback: false,
            });
        }
        TransactionType::Concurrent => {
            program.emit_insn(Insn::Transaction {
                db: 0,
                tx_mode: TransactionMode::Concurrent,
                schema_cookie: schema.schema_version,
            });
            // TODO: Emit transaction instruction on temporary tables when we support them.
            program.emit_insn(Insn::AutoCommit {
                auto_commit: false,
                rollback: false,
            });
        }
    }
    Ok(program)
}

pub fn translate_tx_commit(
    _tx_name: Option<Name>,
    mut program: ProgramBuilder,
) -> Result<ProgramBuilder> {
    program.extend(&ProgramBuilderOpts {
        num_cursors: 0,
        approx_num_insns: 0,
        approx_num_labels: 0,
    });
    program.emit_insn(Insn::AutoCommit {
        auto_commit: true,
        rollback: false,
    });
    Ok(program)
}
