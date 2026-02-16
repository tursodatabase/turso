use crate::schema::Schema;
use crate::translate::emitter::{
    emit_cdc_commit_insns, prepare_cdc_if_necessary, Resolver, TransactionMode,
};
use crate::translate::{ProgramBuilder, ProgramBuilderOpts};
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
    schema: &Schema,
    resolver: &Resolver,
    mut program: ProgramBuilder,
) -> Result<ProgramBuilder> {
    program.extend(&ProgramBuilderOpts {
        num_cursors: 0,
        approx_num_insns: 0,
        approx_num_labels: 0,
    });

    // For v2 CDC, emit a COMMIT record before the AutoCommit instruction.
    let is_v2 = program
        .capture_data_changes_info()
        .as_ref()
        .is_some_and(|info| info.version() == "v2");
    if is_v2 {
        // Use a dummy table name for prepare_cdc_if_necessary — any name that isn't the
        // CDC table itself will work.
        if let Some((cdc_cursor_id, _)) =
            prepare_cdc_if_necessary(&mut program, schema, "__tx_commit__")?
        {
            emit_cdc_commit_insns(&mut program, resolver, cdc_cursor_id)?;
        }
    }

    program.emit_insn(Insn::AutoCommit {
        auto_commit: true,
        rollback: false,
    });
    Ok(program)
}
