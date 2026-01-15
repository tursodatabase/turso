use crate::schema::Schema;
use crate::translate::{
    emitter::{prepare_cdc_if_necessary, Resolver, TransactionMode},
    ProgramBuilder, ProgramBuilderOpts,
};
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

    // Emit CDC COMMIT record if CDC is enabled
    let cdc_table_name = program
        .capture_data_changes_mode()
        .table()
        .map(|s| s.to_string());
    if let Some(cdc_table_name) = cdc_table_name {
        if let Some(cdc_prepared) = prepare_cdc_if_necessary(&mut program, schema, "")? {
            let (cdc_cursor_id, _) = cdc_prepared;
            crate::translate::emitter::emit_cdc_commit_record(
                &mut program,
                resolver,
                cdc_cursor_id,
            )?;
        } else if let Some(turso_cdc_table) = schema.get_table(&cdc_table_name) {
            // CDC table exists but wasn't opened yet, open it now
            let Some(cdc_btree) = turso_cdc_table.btree().clone() else {
                crate::bail_parse_error!("no such table: {}", cdc_table_name);
            };
            let cdc_cursor_id = program.alloc_cursor_id(
                crate::vdbe::builder::CursorType::BTreeTable(cdc_btree.clone()),
            );
            program.emit_insn(Insn::OpenWrite {
                cursor_id: cdc_cursor_id,
                root_page: cdc_btree.root_page.into(),
                db: 0,
            });
            crate::translate::emitter::emit_cdc_commit_record(
                &mut program,
                resolver,
                cdc_cursor_id,
            )?;
        }
    }

    program.emit_insn(Insn::AutoCommit {
        auto_commit: true,
        rollback: false,
    });
    Ok(program)
}
