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
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    program.extend(&ProgramBuilderOpts {
        num_cursors: 0,
        approx_num_insns: 0,
        approx_num_labels: 0,
    });
    let schema = resolver.schema();
    let tx_type = tx_type.unwrap_or(TransactionType::Deferred);
    // For temp we never eagerly open a transaction here: temp is a
    // per-connection database with no inter-connection locking, so
    // there is nothing to "claim" up front. The builder path
    // (`begin_read_on_database` / `begin_write_on_database`) emits
    // `Insn::Transaction` for `TEMP_DB_ID` lazily when an individual
    // statement inside the explicit transaction actually touches a
    // temp object. Eagerly emitting it here would hard-code
    // `TransactionMode::Write` regardless of the outer mode (which was
    // wrong for `BEGIN CONCURRENT`) and force a write on temp even
    // when the transaction only reads it.
    match tx_type {
        TransactionType::Deferred => {
            program.emit_insn(Insn::AutoCommit {
                auto_commit: false,
                rollback: false,
            });
        }
        TransactionType::Immediate | TransactionType::Exclusive => {
            program.emit_insn(Insn::Transaction {
                db: crate::MAIN_DB_ID,
                tx_mode: TransactionMode::Write,
                schema_cookie: schema.schema_version,
            });
            program.emit_insn(Insn::AutoCommit {
                auto_commit: false,
                rollback: false,
            });
        }
        TransactionType::Concurrent => {
            program.emit_insn(Insn::Transaction {
                db: crate::MAIN_DB_ID,
                tx_mode: TransactionMode::Concurrent,
                schema_cookie: schema.schema_version,
            });
            program.emit_insn(Insn::AutoCommit {
                auto_commit: false,
                rollback: false,
            });
        }
    }
    Ok(())
}

pub fn translate_tx_commit(
    _tx_name: Option<Name>,
    schema: &Schema,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    program.extend(&ProgramBuilderOpts {
        num_cursors: 0,
        approx_num_insns: 0,
        approx_num_labels: 0,
    });

    let cdc_info = program.capture_data_changes_info().as_ref();
    if cdc_info.is_some_and(|info| info.cdc_version().has_commit_record()) {
        // Use a dummy table name for prepare_cdc_if_necessary — any name that isn't the
        // CDC table itself will work.
        if let Some((cdc_cursor_id, _)) =
            prepare_cdc_if_necessary(program, schema, "__tx_commit__")?
        {
            emit_cdc_commit_insns(program, resolver, cdc_cursor_id)?;
        }
    }

    program.emit_insn(Insn::AutoCommit {
        auto_commit: true,
        rollback: false,
    });
    Ok(())
}
