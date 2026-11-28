//! Bytecode that rewrites stored rows for `ALTER TABLE`: the rows of
//! `sqlite_schema`, and the rows of engine tables that name a user table.

use crate::ast;
use crate::dialect::{AddColumn, AlterColumn, CmdContext, DropColumn, RenameColumn, RenameTable};
use crate::function::{AlterTableFunc, Func, FuncCtx};
use crate::namespace::{NamespaceId, NamespaceRef};
use crate::schema::BTreeTable;
use crate::sync::Arc;
use crate::translate::alter::database_uses_mvcc;
use crate::translate::collate::CollationSeq;
use crate::translate::emitter::{
    emit_cdc_autocommit_commit, emit_cdc_insns, prepare_cdc_if_necessary, OperationMode, Resolver,
    UpdateRowSource,
};
use crate::translate::schema::sqlite_schema_rows::open_sqlite_schema_for_write;
use crate::translate::schema::SQLITE_TABLEID;
use crate::translate::update::translate_update_for_schema_change;
use crate::util::escape_sql_string_literal;
use crate::vdbe::builder::{CursorType, ProgramBuilder};
use crate::vdbe::insn::{to_u32, CmpInsFlags, InsertFlags, Insn, RegisterOrLiteral};
use crate::{CaptureDataChangesExt, LimboError, Result, TEMP_DB_ID};

/// Renames a table in every row of `sqlite_schema` of its database, in its
/// row of `sqlite_sequence`, in the row of its AUTOINCREMENT backing table,
/// and in the triggers of the temp database.
pub(super) fn emit_rename_table_rows(
    program: &mut ProgramBuilder,
    ctx: &CmdContext<'_>,
    table: &RenameTable<'_>,
) -> Result<()> {
    let resolver = ctx.resolver;
    let stored_old_name = table.stored_old_name;
    let stored_new_name = table.stored_new_name;

    // The rewrite function normalizes the old name it is given, and a
    // virtual table is in no namespace that changes its stored name.
    let old_name_argument = if table.is_virtual {
        table.written_old_name
    } else {
        stored_old_name
    };
    emit_rewrite_of_every_row(
        program,
        ctx,
        AlterTableFunc::RenameTable,
        &[old_name_argument.to_string(), stored_new_name.to_string()],
        &AlteredTable {
            stored_name: stored_old_name,
            written_name: table.written_old_name,
            statement: table.statement,
        },
    )?;
    if table.is_virtual {
        return Ok(());
    }

    emit_rename_sqlite_sequence_entry(
        program,
        resolver,
        ctx.connection,
        ctx.database_id,
        stored_old_name,
        stored_new_name,
    );
    let has_autoincrement_backing_table = resolver.with_schema(ctx.database_id, |s| {
        s.get_sequence(
            NamespaceId::ROOT,
            &crate::schema::autoincrement_sequence_name(stored_old_name),
        )
        .is_some()
    });
    if has_autoincrement_backing_table {
        emit_rename_autoincrement_backing_table_entry(
            program,
            resolver,
            ctx.connection,
            ctx.database_id,
            stored_old_name,
            stored_new_name,
        );
    }

    for trigger in table.temp_triggers {
        emit_update_of_stored_sql(
            program,
            ctx,
            &StoredSqlUpdate {
                database_id: TEMP_DB_ID,
                object_type: "trigger",
                name_for_errors: &trigger.name,
                name: &trigger.name,
                sql: &trigger.sql,
                new_tbl_name: trigger.is_on_the_table.then_some(table.written_new_name),
                statement: table.statement,
            },
        )?;
    }
    Ok(())
}

/// Renames a column in every row of `sqlite_schema` of the table's
/// database, and stores the new text of the triggers and views that name
/// the column.
pub(super) fn emit_rename_column_rows(
    program: &mut ProgramBuilder,
    ctx: &CmdContext<'_>,
    column: &RenameColumn<'_>,
) -> Result<()> {
    let resolver = ctx.resolver;
    let namespace = NamespaceRef {
        database_id: ctx.database_id,
        id: column.namespace,
    };
    let stored_table_name = resolver.mangle_name(namespace, column.table);
    let new_definition = ast::ColumnDefinition {
        col_name: column.new_name.clone(),
        col_type: None,
        constraints: vec![],
    };
    emit_rewrite_of_every_row(
        program,
        ctx,
        AlterTableFunc::RenameColumn,
        &[
            stored_table_name.clone(),
            column.written_old_name.to_string(),
            new_definition.to_string(),
        ],
        &AlteredTable {
            stored_name: &stored_table_name,
            written_name: column.written_table_name,
            statement: column.statement,
        },
    )?;

    // The table row's UPDATE records the statement, whose replay rewrites
    // these rows too.
    program.without_capture_data_changes(|program| {
        for trigger in column.triggers {
            let stored_name = resolver.mangle_name(trigger.namespace, &trigger.name);
            emit_update_of_stored_sql(
                program,
                ctx,
                &StoredSqlUpdate {
                    database_id: trigger.namespace.database_id,
                    object_type: "trigger",
                    name_for_errors: &trigger.name,
                    name: &stored_name,
                    sql: &trigger.sql,
                    new_tbl_name: None,
                    statement: column.statement,
                },
            )?;
        }
        for view in column.views {
            let stored_name = resolver.mangle_name(view.namespace, &view.name);
            emit_update_of_stored_sql(
                program,
                ctx,
                &StoredSqlUpdate {
                    database_id: view.namespace.database_id,
                    object_type: "view",
                    name_for_errors: &stored_name,
                    name: &stored_name,
                    sql: &view.sql,
                    new_tbl_name: None,
                    statement: column.statement,
                },
            )?;
        }
        Ok(())
    })
}

/// Replaces the definition of a column in every row of `sqlite_schema` of
/// the table's database.
pub(super) fn emit_alter_column_rows(
    program: &mut ProgramBuilder,
    ctx: &CmdContext<'_>,
    column: &AlterColumn<'_>,
) -> Result<()> {
    let namespace = NamespaceRef {
        database_id: ctx.database_id,
        id: column.namespace,
    };
    let stored_table_name = ctx.resolver.mangle_name(namespace, column.table);
    emit_rewrite_of_every_row(
        program,
        ctx,
        AlterTableFunc::AlterColumn,
        &[
            stored_table_name.clone(),
            column.written_old_name.to_string(),
            column.definition.to_string(),
        ],
        &AlteredTable {
            stored_name: &stored_table_name,
            written_name: column.written_table_name,
            statement: column.statement,
        },
    )
}

/// Stores the `CREATE TABLE` text of a table with its new column.
pub(super) fn emit_add_column_rows(
    program: &mut ProgramBuilder,
    ctx: &CmdContext<'_>,
    column: &AddColumn<'_>,
) -> Result<()> {
    emit_update_of_table_sql(
        program,
        ctx,
        column.namespace,
        column.name,
        column.table,
        column.statement,
    )?;
    Ok(())
}

/// Stores the `CREATE TABLE` text of a table without its dropped column.
pub(super) fn emit_drop_column_rows(
    program: &mut ProgramBuilder,
    ctx: &CmdContext<'_>,
    column: &DropColumn<'_>,
) -> Result<()> {
    emit_update_of_table_sql(
        program,
        ctx,
        column.namespace,
        column.name,
        column.table,
        column.statement,
    )?;
    Ok(())
}

/// Emits a loop over every row of `sqlite_schema` of the database the
/// statement changes. `func` gets the five columns of a row and then
/// `arguments`, and gives the five columns to store in place of the row.
fn emit_rewrite_of_every_row(
    program: &mut ProgramBuilder,
    ctx: &CmdContext<'_>,
    func: AlterTableFunc,
    arguments: &[String],
    altered_table: &AlteredTable<'_>,
) -> Result<()> {
    let cursor_id = ctx.sqlite_schema_cursor(program);
    let cdc_cursor_id =
        prepare_cdc_if_necessary(program, ctx.schema, ctx.database_id, Some(SQLITE_TABLEID))?
            .map(|(cdc_cursor_id, _)| cdc_cursor_id);
    program.cursor_loop(cursor_id, |program, rowid| {
        let first_column = program.alloc_registers(SQLITE_SCHEMA_COLUMN_COUNT);
        for i in 0..SQLITE_SCHEMA_COLUMN_COUNT {
            program.emit_column_or_rowid(cursor_id, i, first_column + i);
        }
        for argument in arguments {
            program.emit_string8_new_reg(argument.clone());
            program.mark_last_insn_constant();
        }

        let out = program.alloc_registers(SQLITE_SCHEMA_COLUMN_COUNT);
        program.emit_insn(Insn::Function {
            constant_mask: 0,
            start_reg: first_column,
            dest: out,
            func: FuncCtx {
                func: Func::AlterTable(func),
                arg_count: SQLITE_SCHEMA_COLUMN_COUNT + arguments.len(),
                collation: CollationSeq::BINARY,
            },
        });

        let record = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(out),
            count: to_u32(SQLITE_SCHEMA_COLUMN_COUNT),
            dest_reg: to_u32(record),
            index_name: None,
            affinity_str: None,
        });

        if let Some(cdc_cursor_id) = cdc_cursor_id {
            emit_cdc_for_altered_table_row(
                program,
                ctx.resolver,
                cdc_cursor_id,
                rowid,
                first_column,
                record,
                altered_table,
            )?;
        }

        // In MVCC mode, we need to delete before insert to properly
        // end the old version (Hekaton-style UPDATE = DELETE + INSERT)
        if database_uses_mvcc(ctx.connection, ctx.database_id) {
            program.emit_insn(Insn::Delete {
                cursor_id,
                table_name: SQLITE_TABLEID.to_string(),
                is_part_of_update: true,
            });
        }

        program.emit_insn(Insn::Insert {
            cursor: cursor_id,
            key_reg: rowid,
            record_reg: record,
            flag: InsertFlags(0),
            table_name: altered_table.written_name.to_string(),
        });
        Ok::<_, LimboError>(())
    })?;
    emit_cdc_autocommit_commit(program, ctx.resolver, cdc_cursor_id)
}

/// The table an `ALTER TABLE` statement changes.
struct AlteredTable<'a> {
    /// The `name` column of the table's row in `sqlite_schema`.
    stored_name: &'a str,
    /// The table's name as the statement spells it.
    written_name: &'a str,
    /// The text of the `ALTER TABLE` statement, for change data capture.
    statement: &'a str,
}

/// Records the rewrite of the altered table's own `sqlite_schema` row the way
/// ADD COLUMN records it: one UPDATE whose `updates` hold the ALTER statement
/// in place of the new `sql`, so that a CDC consumer replays the statement
/// once. The other rows rewritten by the ALTER (indexes, triggers, views) are
/// not recorded, because replaying the statement rewrites them too.
fn emit_cdc_for_altered_table_row(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    cdc_cursor_id: usize,
    rowid_reg: usize,
    old_row_start_reg: usize,
    new_row_record_reg: usize,
    altered_table: &AlteredTable<'_>,
) -> Result<()> {
    let skip_cdc_label = program.allocate_label();
    let table_type_reg = program.emit_string8_new_reg("table".to_string());
    program.mark_last_insn_constant();
    let table_name_reg = program.emit_string8_new_reg(altered_table.stored_name.to_string());
    program.mark_last_insn_constant();
    program.emit_insn(Insn::Ne {
        lhs: old_row_start_reg,
        rhs: table_type_reg,
        target_pc: skip_cdc_label,
        flags: CmpInsFlags::default(),
        collation: CollationSeq::for_internal_values(),
    });
    program.emit_insn(Insn::Ne {
        lhs: old_row_start_reg + 1,
        rhs: table_name_reg,
        target_pc: skip_cdc_label,
        flags: CmpInsFlags::default(),
        collation: CollationSeq::NOCASE,
    });

    let before_record_reg = if program.capture_data_changes_info().has_before() {
        let record_reg = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(old_row_start_reg),
            count: to_u32(SQLITE_SCHEMA_COLUMN_COUNT),
            dest_reg: to_u32(record_reg),
            index_name: None,
            affinity_str: None,
        });
        Some(record_reg)
    } else {
        None
    };
    let after_record_reg = program
        .capture_data_changes_info()
        .has_after()
        .then_some(new_row_record_reg);
    let updates_record_reg = if program.capture_data_changes_info().has_updates() {
        let sql_column = 4;
        let updates_start_reg = program.alloc_registers(2 * SQLITE_SCHEMA_COLUMN_COUNT);
        for column in 0..SQLITE_SCHEMA_COLUMN_COUNT {
            program.emit_bool(column == sql_column, updates_start_reg + column);
            program.emit_null(
                updates_start_reg + SQLITE_SCHEMA_COLUMN_COUNT + column,
                None,
            );
        }
        program.emit_string8(
            altered_table.statement.to_string(),
            updates_start_reg + SQLITE_SCHEMA_COLUMN_COUNT + sql_column,
        );
        let record_reg = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(updates_start_reg),
            count: to_u32(2 * SQLITE_SCHEMA_COLUMN_COUNT),
            dest_reg: to_u32(record_reg),
            index_name: None,
            affinity_str: None,
        });
        Some(record_reg)
    } else {
        None
    };
    emit_cdc_insns(
        program,
        resolver,
        OperationMode::UPDATE(UpdateRowSource::Normal),
        cdc_cursor_id,
        rowid_reg,
        before_record_reg,
        after_record_reg,
        updates_record_reg,
        SQLITE_TABLEID,
    )?;
    program.preassign_label_to_next_insn(skip_cdc_label);
    Ok(())
}

const SQLITE_SCHEMA_COLUMN_COUNT: usize = 5;

/// Rewrite the sqlite_schema row for the AUTOINCREMENT backing table when
/// the parent table is renamed. The backing table is keyed by name
/// `__turso_internal_seq___turso_internal_autoincrement_<table>`, so its
/// row is not touched by the general `Func::AlterTable::RenameTable` sweep
/// (which only matches rows whose `tbl_name` column equals the renamed
/// user table). Without this rewrite, the persistent sqlite_schema row
/// keeps the old backing-table name and SQL, so the next process to open
/// the DB rebuilds the in-memory schema with the stale backing table —
/// and an INSERT into the renamed table fails with "missing implicit
/// sequence for AUTOINCREMENT table".
///
/// Mirrors the existing `emit_rename_sqlite_sequence_entry` helper for
/// the `sqlite_sequence` row; the in-memory rename of `schema.sequences`
/// and `schema.tables` happens in `op_rename_table`.
fn emit_rename_autoincrement_backing_table_entry(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    connection: &Arc<crate::Connection>,
    database_id: usize,
    old_table_name_norm: &str,
    new_table_name_norm: &str,
) {
    use crate::schema::autoincrement_sequence_name;
    use crate::translate::sequence::{sequence_backing_table_name, sequence_backing_table_sql};

    let old_backing_name =
        sequence_backing_table_name(&autoincrement_sequence_name(old_table_name_norm));
    let new_seq_name = autoincrement_sequence_name(new_table_name_norm);
    let new_backing_name = sequence_backing_table_name(&new_seq_name);
    let new_backing_sql = sequence_backing_table_sql(&new_seq_name);

    let cursor_id = open_sqlite_schema_for_write(program, resolver, database_id);

    let old_backing_reg = program.emit_string8_new_reg(old_backing_name);
    program.mark_last_insn_constant();
    let new_backing_reg = program.emit_string8_new_reg(new_backing_name);
    program.mark_last_insn_constant();
    let new_sql_reg = program.emit_string8_new_reg(new_backing_sql);
    program.mark_last_insn_constant();

    program.cursor_loop(cursor_id, |program, rowid| {
        let name_reg = program.alloc_register();
        program.emit_column_or_rowid(cursor_id, 1, name_reg);

        let continue_label = program.allocate_label();
        program.emit_insn(Insn::Ne {
            lhs: name_reg,
            rhs: old_backing_reg,
            target_pc: continue_label,
            flags: CmpInsFlags::default(),
            collation: CollationSeq::for_internal_values(),
        });

        // Preserve type (col 0) and rootpage (col 3); overwrite name/tbl_name
        // (cols 1, 2) and sql (col 4). The backing table is INTERNAL — its
        // sql is regenerated from `sequence_backing_table_sql(new_seq_name)`
        // and never altered by user DDL, so we don't need to AST-rewrite it.
        let rec_start = program.alloc_registers(5);
        program.emit_column_or_rowid(cursor_id, 0, rec_start); // type
        program.emit_insn(Insn::Copy {
            src_reg: new_backing_reg,
            dst_reg: rec_start + 1, // name
            extra_amount: 0,
        });
        program.emit_insn(Insn::Copy {
            src_reg: new_backing_reg,
            dst_reg: rec_start + 2, // tbl_name
            extra_amount: 0,
        });
        program.emit_column_or_rowid(cursor_id, 3, rec_start + 3); // rootpage
        program.emit_insn(Insn::Copy {
            src_reg: new_sql_reg,
            dst_reg: rec_start + 4, // sql
            extra_amount: 0,
        });

        let record_reg = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(rec_start),
            count: to_u32(5),
            dest_reg: to_u32(record_reg),
            index_name: None,
            affinity_str: None,
        });

        // MVCC: end the old version before writing the new one
        // (Hekaton-style UPDATE = DELETE + INSERT), mirroring the
        // pattern used by `emit_rename_sqlite_sequence_entry` and the
        // main-table rewrite cursor_loop.
        if database_uses_mvcc(connection, database_id) {
            program.emit_insn(Insn::Delete {
                cursor_id,
                table_name: SQLITE_TABLEID.to_string(),
                is_part_of_update: true,
            });
        }

        program.emit_insn(Insn::Insert {
            cursor: cursor_id,
            key_reg: rowid,
            record_reg,
            flag: crate::vdbe::insn::InsertFlags(0),
            table_name: SQLITE_TABLEID.to_string(),
        });

        program.preassign_label_to_next_insn(continue_label);
    });
}

fn emit_rename_sqlite_sequence_entry(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    connection: &Arc<crate::Connection>,
    database_id: usize,
    old_table_name_norm: &str,
    new_table_name_norm: &str,
) {
    let Some(sqlite_sequence) = resolver.with_schema(database_id, |s| {
        s.get_btree_table(NamespaceId::ROOT, crate::schema::SQLITE_SEQUENCE_TABLE_NAME)
    }) else {
        return;
    };

    let seq_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(sqlite_sequence.clone()));
    let sequence_name_reg = program.alloc_register();
    let sequence_value_reg = program.alloc_register();
    let row_name_to_replace_reg = program.emit_string8_new_reg(old_table_name_norm.to_string());
    program.mark_last_insn_constant();
    let replacement_row_name_reg = program.emit_string8_new_reg(new_table_name_norm.to_string());
    program.mark_last_insn_constant();

    let affinity_str = sqlite_sequence
        .columns()
        .iter()
        .map(|col| col.affinity().aff_mask())
        .collect::<String>();

    program.emit_insn(Insn::OpenWrite {
        cursor_id: seq_cursor_id,
        root_page: RegisterOrLiteral::Literal(sqlite_sequence.root_page),
        db: database_id,
    });

    program.cursor_loop(seq_cursor_id, |program, rowid| {
        program.emit_column_or_rowid(seq_cursor_id, 0, sequence_name_reg);

        let continue_loop_label = program.allocate_label();
        program.emit_insn(Insn::Ne {
            lhs: sequence_name_reg,
            rhs: row_name_to_replace_reg,
            target_pc: continue_loop_label,
            flags: CmpInsFlags::default().jump_if_null(),
            collation: CollationSeq::for_internal_values(),
        });

        program.emit_column_or_rowid(seq_cursor_id, 1, sequence_value_reg);

        let record_start_reg = program.alloc_registers(2);
        let record_reg = program.alloc_register();

        program.emit_insn(Insn::Copy {
            src_reg: replacement_row_name_reg,
            dst_reg: record_start_reg,
            extra_amount: 0,
        });
        program.emit_insn(Insn::Copy {
            src_reg: sequence_value_reg,
            dst_reg: record_start_reg + 1,
            extra_amount: 0,
        });
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(record_start_reg),
            count: to_u32(2),
            dest_reg: to_u32(record_reg),
            index_name: None,
            affinity_str: Some(affinity_str.clone()),
        });

        // In MVCC mode, we need to delete before insert to properly
        // end the old version (Hekaton-style UPDATE = DELETE + INSERT)
        if database_uses_mvcc(connection, database_id) {
            program.emit_insn(Insn::Delete {
                cursor_id: seq_cursor_id,
                table_name: crate::schema::SQLITE_SEQUENCE_TABLE_NAME.to_string(),
                is_part_of_update: true,
            });
        }

        program.emit_insn(Insn::Insert {
            cursor: seq_cursor_id,
            key_reg: rowid,
            record_reg,
            flag: crate::translate::insert::autoincrement_bookkeeping_insert_flags(),
            table_name: crate::schema::SQLITE_SEQUENCE_TABLE_NAME.to_string(),
        });

        program.preassign_label_to_next_insn(continue_loop_label);
    });
}

/// Stores the `CREATE TABLE` text of `table`, which `ALTER TABLE` has
/// changed the columns of, in the row of the table `name`.
fn emit_update_of_table_sql(
    program: &mut ProgramBuilder,
    ctx: &CmdContext<'_>,
    namespace: NamespaceId,
    name: &str,
    table: &BTreeTable,
    statement: &str,
) -> Result<()> {
    let resolver = ctx.resolver;
    let namespace = NamespaceRef {
        database_id: ctx.database_id,
        id: namespace,
    };
    let stored_name = resolver.mangle_name(namespace, name);
    let sql = table.to_sql_as(&resolver.mangle_name(namespace, &table.declared_name));
    emit_update_of_stored_sql(
        program,
        ctx,
        &StoredSqlUpdate {
            database_id: ctx.database_id,
            object_type: "table",
            name_for_errors: name,
            name: &stored_name,
            sql: &sql,
            new_tbl_name: None,
            statement,
        },
    )
}

/// Emits `UPDATE sqlite_schema SET sql = ...` for the row of one object.
fn emit_update_of_stored_sql(
    program: &mut ProgramBuilder,
    ctx: &CmdContext<'_>,
    update: &StoredSqlUpdate<'_>,
) -> Result<()> {
    let StoredSqlUpdate {
        object_type,
        name_for_errors,
        ..
    } = update;
    let escaped_name = escape_sql_string_literal(update.name);
    let schema_table = schema_table_name_for_db(ctx.resolver, update.database_id);
    let escaped_sql = escape_sql_string_literal(update.sql);
    let tbl_name_update = match update.new_tbl_name {
        Some(tbl_name) => format!(", tbl_name = '{}'", escape_sql_string_literal(tbl_name)),
        None => String::new(),
    };
    let update_stmt = format!(
        r#"
            UPDATE {schema_table}
            SET sql = '{escaped_sql}'{tbl_name_update}
            WHERE name = '{escaped_name}' COLLATE NOCASE AND type = '{object_type}'
        "#,
    );

    let cmd = crate::dialect::parse_statement(ctx.connection.dialect().as_ref(), &update_stmt)
        .map_err(|e| {
            LimboError::ParseError(format!(
                "failed to parse {object_type} update SQL for {name_for_errors}: {e}"
            ))
        })?;
    let Some(ast::Cmd::Stmt(ast::Stmt::Update(update_ast))) = cmd else {
        return Err(LimboError::ParseError(format!(
            "failed to parse {object_type} update SQL for {name_for_errors}",
        )));
    };

    translate_update_for_schema_change(
        update_ast,
        ctx.resolver,
        program,
        ctx.connection,
        update.statement,
    )
}

/// The row of `sqlite_schema` that gets new SQL text.
struct StoredSqlUpdate<'a> {
    /// The database of the row.
    database_id: usize,
    /// The `type` column of the row.
    object_type: &'a str,
    name_for_errors: &'a str,
    /// The `name` column of the row.
    name: &'a str,
    /// The new `sql` column.
    sql: &'a str,
    /// The new `tbl_name` column, if it changes.
    new_tbl_name: Option<&'a str>,
    /// The text of the `ALTER TABLE` statement, for change data capture.
    statement: &'a str,
}

/// The name of `sqlite_schema` in SQL text, qualified with the database
/// name for the temp database and for an attached one.
fn schema_table_name_for_db(resolver: &Resolver, database_id: usize) -> String {
    if database_id == crate::MAIN_DB_ID {
        SQLITE_TABLEID.to_string()
    } else if database_id == crate::TEMP_DB_ID {
        format!("temp.{SQLITE_TABLEID}")
    } else {
        let db_name = resolver
            .namespace_name(database_id)
            .unwrap_or_else(|| "main".to_string());
        format!("{db_name}.{SQLITE_TABLEID}")
    }
}
