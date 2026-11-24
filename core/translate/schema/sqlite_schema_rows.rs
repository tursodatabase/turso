//! Bytecode that writes rows of `sqlite_schema`.

use crate::ast;
use crate::dialect::{
    Catalog, CatalogCmd, CmdContext, CreateIndex, CreateMaterializedView, CreateSequence,
    CreateTable, CreateTrigger, CreateType, CreateView, CreateVirtualTable, DropIndex,
    DropSequence, DropTable, DropTrigger, DropType, DropView, TableText,
};
use crate::namespace::{NamespaceId, NamespaceRef};
use crate::schema::{BTreeTable, TURSO_TYPES_TABLE_NAME};
use crate::storage::pager::CreateBTreeFlags;
use crate::sync::Arc;
use crate::translate::collate::CollationSeq;
use crate::translate::emitter::{
    emit_cdc_autocommit_commit, emit_cdc_full_record, emit_cdc_insns, prepare_cdc_if_necessary,
    OperationMode, Resolver,
};
use crate::translate::index::canonical_create_index_sql;
use crate::translate::schema::sqlite_schema_alter_rows::{
    emit_add_column_rows, emit_alter_column_rows, emit_drop_column_rows, emit_rename_column_rows,
    emit_rename_table_rows,
};
use crate::translate::schema::{emit_schema_cookie, SQLITE_TABLEID};
use crate::translate::sequence::{sequence_backing_table_name, sequence_backing_table_sql};
use crate::translate::ProgramBuilder;
use crate::vdbe::builder::CursorType;
use crate::vdbe::insn::{to_u32, CmpInsFlags, InsertFlags, Insn, RegisterOrLiteral};
use crate::{CaptureDataChangesExt, Result, MAIN_DB_ID};

/// The catalog of a SQLite database: a row of `sqlite_schema` per object,
/// with the SQL text that creates it.
#[derive(Debug)]
pub struct SqliteCatalog;

impl Catalog for SqliteCatalog {
    fn emit(
        &self,
        program: &mut ProgramBuilder,
        ctx: &CmdContext<'_>,
        cmd: &CatalogCmd<'_>,
    ) -> Result<()> {
        match cmd {
            CatalogCmd::CreateTable(table) => emit_create_table_rows(program, ctx, table),
            CatalogCmd::CreateIndex(index) => emit_create_index_row(program, ctx, index),
            CatalogCmd::DropIndex(index) => emit_delete_index_row(program, ctx, index),
            CatalogCmd::CreateSequence(sequence) => {
                emit_create_sequence_row(program, ctx, sequence)
            }
            CatalogCmd::DropSequence(sequence) => emit_delete_sequence_row(program, ctx, sequence),
            CatalogCmd::DropTable(table) => emit_drop_table_rows(program, ctx, table),
            CatalogCmd::CreateView(view) => emit_create_view_row(program, ctx, view),
            CatalogCmd::CreateMaterializedView(view) => {
                emit_create_materialized_view_row(program, ctx, view)
            }
            CatalogCmd::DropView(view) => emit_drop_view_row(program, ctx, view),
            CatalogCmd::RenameTable(table) => emit_rename_table_rows(program, ctx, table),
            CatalogCmd::RenameColumn(column) => emit_rename_column_rows(program, ctx, column),
            CatalogCmd::AlterColumn(column) => emit_alter_column_rows(program, ctx, column),
            CatalogCmd::AddColumn(column) => emit_add_column_rows(program, ctx, column),
            CatalogCmd::DropColumn(column) => emit_drop_column_rows(program, ctx, column),
            CatalogCmd::CreateTrigger(trigger) => emit_create_trigger_row(program, ctx, trigger),
            CatalogCmd::DropTrigger(trigger) => {
                emit_delete_trigger_row(program, ctx, trigger);
                Ok(())
            }
            CatalogCmd::CreateVirtualTable(table) => {
                emit_create_virtual_table_row(program, ctx, table)
            }
            CatalogCmd::CreateType(ty) => emit_create_type_row(program, ctx, ty),
            CatalogCmd::DropType(ty) => emit_delete_type_row(program, ctx, ty),
        }
    }
}

/// Writes the row of a new table and a row for each of its automatic
/// indexes.
fn emit_create_table_rows(
    program: &mut ProgramBuilder,
    ctx: &CmdContext<'_>,
    table: &CreateTable<'_>,
) -> Result<()> {
    let resolver = ctx.resolver;
    let namespace = NamespaceRef {
        database_id: ctx.database_id,
        id: table.namespace,
    };
    let stored_name = resolver.mangle_name(namespace, table.name);
    let sql = table_sql(resolver, namespace, table)?;

    let sqlite_schema_cursor_id = ctx.sqlite_schema_cursor(program);
    let cdc_table = prepare_cdc_if_necessary(
        program,
        resolver.schema(),
        ctx.database_id,
        Some(SQLITE_TABLEID),
    )?;
    emit_schema_entry(
        program,
        resolver,
        sqlite_schema_cursor_id,
        cdc_table.as_ref().map(|x| x.0),
        SchemaEntryType::Table,
        &stored_name,
        &stored_name,
        table.root_reg,
        Some(sql),
    )?;
    for index in table.automatic_indexes {
        emit_schema_entry(
            program,
            resolver,
            sqlite_schema_cursor_id,
            None,
            SchemaEntryType::Index,
            &index.name,
            &stored_name,
            index.root_reg,
            None,
        )?;
    }

    Ok(())
}

/// The SQL text to store for a new table. The resolver's dialect is the
/// one the statement was parsed with: the engine's own for an engine schema
/// statement, which stores plain SQLite text, the connection's otherwise.
/// The SQLite dialect renders canonical text from the AST; a frontend
/// dialect keeps its own input text.
fn table_sql(
    resolver: &Resolver,
    namespace: NamespaceRef,
    table: &CreateTable<'_>,
) -> Result<String> {
    let dialect = &resolver.catalog.dialect;
    let name_in_sql = ast::QualifiedName {
        namespace: None,
        name: resolver.name_in_sql(namespace, table.written_name),
        alias: None,
    };
    match table.text {
        TableText::FromSelect(sql) => Ok(sql.to_string()),
        TableText::Rewritten => dialect.format_rewritten_table_sql(&ast::Stmt::CreateTable {
            temporary: table.temporary,
            if_not_exists: table.if_not_exists,
            tbl_name: name_in_sql,
            body: table.body.clone(),
        }),
        TableText::Statement(input) => dialect.format_table_sql(input, &name_in_sql, table.body),
    }
}

/// Writes the row of a new index.
fn emit_create_index_row(
    program: &mut ProgramBuilder,
    ctx: &CmdContext<'_>,
    index: &CreateIndex<'_>,
) -> Result<()> {
    let resolver = ctx.resolver;
    let namespace = NamespaceRef {
        database_id: ctx.database_id,
        id: index.namespace,
    };
    let stored_name = resolver.mangle_name(namespace, index.name);
    let sql = canonical_create_index_sql(
        index.statement,
        resolver.name_in_sql(namespace, index.written_name),
        resolver.name_in_sql(namespace, index.written_table),
    );

    let sqlite_schema_cursor_id = ctx.sqlite_schema_cursor(program);
    let cdc_table = prepare_cdc_if_necessary(
        program,
        resolver.schema(),
        ctx.database_id,
        Some(SQLITE_TABLEID),
    )?;
    emit_schema_entry(
        program,
        resolver,
        sqlite_schema_cursor_id,
        cdc_table.map(|x| x.0),
        SchemaEntryType::Index,
        &stored_name,
        &resolver.mangle_name(namespace, index.table),
        index.root_reg,
        Some(sql),
    )?;

    Ok(())
}

/// Writes the row of a new sequence. The schema reads a sequence from the
/// row of its backing table, so that row is the one written.
fn emit_create_sequence_row(
    program: &mut ProgramBuilder,
    ctx: &CmdContext<'_>,
    sequence: &CreateSequence<'_>,
) -> Result<()> {
    let namespace = NamespaceRef {
        database_id: ctx.database_id,
        id: sequence.namespace,
    };
    let stored_name = ctx.resolver.mangle_name(namespace, sequence.name);
    let backing_table_name = sequence_backing_table_name(&stored_name);
    let sqlite_schema_cursor_id = ctx.sqlite_schema_cursor(program);
    emit_schema_entry(
        program,
        ctx.resolver,
        sqlite_schema_cursor_id,
        None,
        SchemaEntryType::Table,
        &backing_table_name,
        &backing_table_name,
        sequence.root_reg,
        Some(sequence_backing_table_sql(&stored_name)),
    )
}

/// Deletes the row of a sequence that is dropped, which is the row of its
/// backing table.
fn emit_delete_sequence_row(
    program: &mut ProgramBuilder,
    ctx: &CmdContext<'_>,
    sequence: &DropSequence<'_>,
) -> Result<()> {
    let namespace = NamespaceRef {
        database_id: ctx.database_id,
        id: sequence.namespace,
    };
    let stored_name = ctx.resolver.mangle_name(namespace, sequence.name);
    let sqlite_schema_cursor_id = ctx.sqlite_schema_cursor(program);
    emit_delete_sequence_backing_table_row(
        program,
        sqlite_schema_cursor_id,
        sequence_backing_table_name(&stored_name),
    );
    Ok(())
}

/// Deletes the `sqlite_schema` row of the table `backing_table_name` of a
/// sequence, with a scan of the open cursor `sqlite_schema_cursor_id`.
pub(crate) fn emit_delete_sequence_backing_table_row(
    program: &mut ProgramBuilder,
    sqlite_schema_cursor_id: usize,
    backing_table_name: String,
) {
    let seq_name_reg = program.alloc_register();
    let type_str_reg = program.alloc_register();
    program.emit_insn(Insn::String8 {
        dest: seq_name_reg,
        value: backing_table_name,
    });
    program.emit_insn(Insn::String8 {
        dest: type_str_reg,
        value: "table".to_string(),
    });

    // Scan sqlite_schema for (type='table' AND name=backing_table_name) and delete the row.
    let end_loop_label = program.allocate_label();
    let loop_start_label = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: sqlite_schema_cursor_id,
        pc_if_empty: end_loop_label,
    });
    program.preassign_label_to_next_insn(loop_start_label);
    let col0_reg = program.alloc_register();
    let col1_reg = program.alloc_register();
    program.emit_column_or_rowid(sqlite_schema_cursor_id, 0, col0_reg);
    program.emit_column_or_rowid(sqlite_schema_cursor_id, 1, col1_reg);
    let skip_delete_label = program.allocate_label();
    program.emit_insn(Insn::Ne {
        lhs: col0_reg,
        rhs: type_str_reg,
        target_pc: skip_delete_label,
        flags: CmpInsFlags::default(),
        collation: CollationSeq::for_internal_values(),
    });
    program.emit_insn(Insn::Ne {
        lhs: col1_reg,
        rhs: seq_name_reg,
        target_pc: skip_delete_label,
        flags: CmpInsFlags::default(),
        collation: CollationSeq::for_internal_values(),
    });
    program.emit_insn(Insn::Delete {
        cursor_id: sqlite_schema_cursor_id,
        table_name: "sqlite_schema".to_string(),
        is_part_of_update: false,
    });
    program.preassign_label_to_next_insn(skip_delete_label);
    program.emit_insn(Insn::Next {
        cursor_id: sqlite_schema_cursor_id,
        pc_if_next: loop_start_label,
        fullscan: false,
        is_index: false,
    });
    program.preassign_label_to_next_insn(end_loop_label);
}

/// Deletes the row of an index.
fn emit_delete_index_row(
    program: &mut ProgramBuilder,
    ctx: &CmdContext<'_>,
    index: &DropIndex<'_>,
) -> Result<()> {
    let resolver = ctx.resolver;
    let namespace = NamespaceRef {
        database_id: ctx.database_id,
        id: index.namespace,
    };

    let cdc_table = prepare_cdc_if_necessary(
        program,
        resolver.schema(),
        ctx.database_id,
        Some(SQLITE_TABLEID),
    )?;

    // According to sqlite should emit Null instruction
    // but why?
    let null_reg = program.alloc_register();
    program.emit_null(null_reg, None);

    // String8; r[3] = 'some idx name'
    let index_name_reg = program.emit_string8_new_reg(resolver.mangle_name(namespace, index.name));
    // String8; r[4] = 'index'
    let index_str_reg = program.emit_string8_new_reg("index".to_string());

    // for r[5]=rowid
    let row_id_reg = program.alloc_register();

    // We're going to use this cursor to search through sqlite_schema
    let sqlite_table = resolver
        .schema()
        .get_btree_table(NamespaceId::ROOT, SQLITE_TABLEID)
        .unwrap();
    let sqlite_schema_cursor_id = ctx.sqlite_schema_cursor(program);

    let loop_start_label = program.allocate_label();
    let loop_end_label = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: sqlite_schema_cursor_id,
        pc_if_empty: loop_end_label,
    });
    program.preassign_label_to_next_insn(loop_start_label);

    // Read sqlite_schema.name into dest_reg
    let dest_reg = program.alloc_register();
    program.emit_column_or_rowid(sqlite_schema_cursor_id, 1, dest_reg);

    // if current column is not index_name then jump to Next
    // skip if sqlite_schema.name != index_name_reg
    let next_label = program.allocate_label();
    program.emit_insn(Insn::Ne {
        lhs: index_name_reg,
        rhs: dest_reg,
        target_pc: next_label,
        flags: CmpInsFlags::default(),
        collation: CollationSeq::NOCASE,
    });

    // read type of table
    // skip if sqlite_schema.type != 'index' (index_str_reg)
    program.emit_column_or_rowid(sqlite_schema_cursor_id, 0, dest_reg);
    // if current column is not index then jump to Next
    program.emit_insn(Insn::Ne {
        lhs: index_str_reg,
        rhs: dest_reg,
        target_pc: next_label,
        flags: CmpInsFlags::default(),
        collation: CollationSeq::for_internal_values(),
    });

    program.emit_insn(Insn::RowId {
        cursor_id: sqlite_schema_cursor_id,
        dest: row_id_reg,
    });

    let label_once_end = program.allocate_label();
    program.emit_insn(Insn::Once {
        target_pc_when_reentered: label_once_end,
    });
    program.preassign_label_to_next_insn(label_once_end);

    if let Some((cdc_cursor_id, _)) = cdc_table {
        let before_record_reg = if program.capture_data_changes_info().has_before() {
            Some(emit_cdc_full_record(
                program,
                resolver,
                sqlite_table.columns(),
                sqlite_schema_cursor_id,
                row_id_reg,
            )?)
        } else {
            None
        };
        emit_cdc_insns(
            program,
            resolver,
            OperationMode::DELETE,
            cdc_cursor_id,
            row_id_reg,
            before_record_reg,
            None,
            None,
            SQLITE_TABLEID,
        )?;
    }

    program.emit_insn(Insn::Delete {
        cursor_id: sqlite_schema_cursor_id,
        table_name: "sqlite_schema".to_string(),
        is_part_of_update: false,
    });

    program.preassign_label_to_next_insn(next_label);
    program.emit_insn(Insn::Next {
        cursor_id: sqlite_schema_cursor_id,
        pc_if_next: loop_start_label,
        fullscan: false,
        is_index: false,
    });

    program.preassign_label_to_next_insn(loop_end_label);
    if let Some((cdc_cursor_id, _)) = cdc_table {
        emit_cdc_autocommit_commit(program, resolver, Some(cdc_cursor_id))?;
    }

    Ok(())
}

/// Deletes the rows of a dropped table and of its indexes and triggers.
fn emit_drop_table_rows(
    program: &mut ProgramBuilder,
    ctx: &CmdContext<'_>,
    table: &DropTable<'_>,
) -> Result<()> {
    emit_delete_rows_of_table(program, ctx, table)?;
    emit_delete_temp_triggers_on_table(program, ctx, table)?;
    Ok(())
}

/// Deletes every row whose `tbl_name` is the table: its own row and the
/// rows of its indexes and triggers.
fn emit_delete_rows_of_table(
    program: &mut ProgramBuilder,
    ctx: &CmdContext<'_>,
    table: &DropTable<'_>,
) -> Result<()> {
    let resolver = ctx.resolver;
    let database_id = ctx.database_id;
    let stored_name = resolver.mangle_name(
        NamespaceRef {
            database_id,
            id: table.namespace,
        },
        table.name,
    );
    let cdc_table = prepare_cdc_if_necessary(
        program,
        resolver.schema(),
        database_id,
        Some(SQLITE_TABLEID),
    )?;

    let [null_reg, tbl_name_reg] = table.scratch_regs;
    program.emit_null(null_reg, None);
    let table_reg = program.emit_string8_new_reg(stored_name);
    program.mark_last_insn_constant();
    let _table_type = program.emit_string8_new_reg("trigger".to_string());
    program.mark_last_insn_constant();
    let row_id_reg = program.alloc_register();

    let schema_table = resolver
        .schema()
        .get_btree_table(NamespaceId::ROOT, SQLITE_TABLEID)
        .unwrap();
    let sqlite_schema_cursor_id_0 = ctx.sqlite_schema_cursor(program);

    //  1. Remove all entries from the schema table related to the table we are dropping (including triggers)
    //  loop to beginning of schema table
    let end_metadata_label = program.allocate_label();
    let metadata_loop = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: sqlite_schema_cursor_id_0,
        pc_if_empty: end_metadata_label,
    });
    program.preassign_label_to_next_insn(metadata_loop);

    // start loop on schema table
    program.emit_column_or_rowid(sqlite_schema_cursor_id_0, 2, tbl_name_reg);
    let next_label = program.allocate_label();
    program.emit_insn(Insn::Ne {
        lhs: tbl_name_reg,
        rhs: table_reg,
        target_pc: next_label,
        flags: crate::vdbe::insn::CmpInsFlags::default(),
        collation: crate::translate::collate::CollationSeq::NOCASE,
    });
    program.emit_insn(Insn::RowId {
        cursor_id: sqlite_schema_cursor_id_0,
        dest: row_id_reg,
    });
    if let Some((cdc_cursor_id, _)) = cdc_table {
        let table_type = program.emit_string8_new_reg("table".to_string()); // r4
        program.mark_last_insn_constant();

        let skip_cdc_label = program.allocate_label();

        let entry_type_reg = program.alloc_register();
        program.emit_column_or_rowid(sqlite_schema_cursor_id_0, 0, entry_type_reg);
        program.emit_insn(Insn::Ne {
            lhs: entry_type_reg,
            rhs: table_type,
            target_pc: skip_cdc_label,
            flags: crate::vdbe::insn::CmpInsFlags::default(),
            collation: crate::translate::collate::CollationSeq::for_internal_values(),
        });
        let before_record_reg = if program.capture_data_changes_info().has_before() {
            Some(crate::translate::emitter::emit_cdc_full_record(
                program,
                resolver,
                schema_table.columns(),
                sqlite_schema_cursor_id_0,
                row_id_reg,
            )?)
        } else {
            None
        };
        emit_cdc_insns(
            program,
            resolver,
            OperationMode::DELETE,
            cdc_cursor_id,
            row_id_reg,
            before_record_reg,
            None,
            None,
            SQLITE_TABLEID,
        )?;
        program.preassign_label_to_next_insn(skip_cdc_label);
    }
    program.emit_insn(Insn::Delete {
        cursor_id: sqlite_schema_cursor_id_0,
        table_name: SQLITE_TABLEID.to_string(),
        is_part_of_update: false,
    });

    program.preassign_label_to_next_insn(next_label);
    program.emit_insn(Insn::Next {
        cursor_id: sqlite_schema_cursor_id_0,
        pc_if_next: metadata_loop,
        fullscan: false,
        is_index: false,
    });
    program.preassign_label_to_next_insn(end_metadata_label);
    // end of loop on schema table
    if let Some((cdc_cursor_id, _)) = cdc_table {
        emit_cdc_autocommit_commit(program, resolver, Some(cdc_cursor_id))?;
    }
    Ok(())
}

/// Deletes the rows of the triggers in the temp database that are on the
/// table, when the table is in another database.
fn emit_delete_temp_triggers_on_table(
    program: &mut ProgramBuilder,
    ctx: &CmdContext<'_>,
    table: &DropTable<'_>,
) -> Result<()> {
    // SQLite removes temp triggers targeting the dropped table.
    // Enumerate the temp schema triggers at translate time (which is
    // safe because a concurrent DDL would bump the schema cookie and
    // force a reprepare) and emit per-trigger bytecode to delete only
    // the rows that should be removed. Filtering in the bytecode by
    // `tbl_name` alone is not enough: two triggers with the same
    // unqualified table name can live in the temp schema but point to
    // different databases (e.g. one on `main.t`, one on `temp.t` when a
    // shadow table exists). We must key on trigger name.
    let resolver = ctx.resolver;
    let database_id = ctx.database_id;
    if database_id != crate::TEMP_DB_ID && resolver.has_temp_database() {
        // A temp schema trigger targets the dropped db iff:
        //   - it explicitly qualifies with the dropped db, or
        //   - it is unqualified AND dropping from main AND temp has no
        //     shadow table of the same name (in which case the
        //     unqualified reference resolves to main).
        let temp_has_shadow = resolver.with_schema(crate::TEMP_DB_ID, |s| {
            s.get_table(NamespaceId::ROOT, table.name).is_some()
        });
        let trigger_names_to_drop: Vec<String> = resolver.with_schema(crate::TEMP_DB_ID, |s| {
            s.get_triggers_for_table(NamespaceId::ROOT, table.name)
                .filter(|trigger| match trigger.target_database_id {
                    Some(db_id) => db_id == database_id,
                    None => !temp_has_shadow && database_id == crate::MAIN_DB_ID,
                })
                .map(|trigger| trigger.name.clone())
                .collect()
        });

        if !trigger_names_to_drop.is_empty() {
            let temp_schema_cookie = resolver.with_schema(crate::TEMP_DB_ID, |s| s.schema_version);
            program.begin_write_on_database(crate::TEMP_DB_ID, temp_schema_cookie)?;
            let temp_cursor = open_sqlite_schema_for_write(program, resolver, crate::TEMP_DB_ID);
            // Hoist the literal trigger names + `"trigger"` type
            // string into constant registers before the loop.
            let trigger_type_reg = program.emit_string8_new_reg("trigger".to_string());
            program.mark_last_insn_constant();
            let name_regs: Vec<usize> = trigger_names_to_drop
                .iter()
                .map(|name| {
                    let reg = program.emit_string8_new_reg(name.clone());
                    program.mark_last_insn_constant();
                    reg
                })
                .collect();

            let temp_end_label = program.allocate_label();
            let temp_loop_label = program.allocate_label();
            program.emit_insn(Insn::Rewind {
                cursor_id: temp_cursor,
                pc_if_empty: temp_end_label,
            });
            program.preassign_label_to_next_insn(temp_loop_label);
            let temp_next_label = program.allocate_label();
            let temp_delete_label = program.allocate_label();

            // Skip non-trigger rows (column 0 = type).
            let temp_type_reg = program.alloc_register();
            program.emit_column_or_rowid(temp_cursor, 0, temp_type_reg);
            program.emit_insn(Insn::Ne {
                lhs: temp_type_reg,
                rhs: trigger_type_reg,
                target_pc: temp_next_label,
                flags: crate::vdbe::insn::CmpInsFlags::default(),
                collation: crate::translate::collate::CollationSeq::for_internal_values(),
            });

            // Cascade-check name (column 1) against each trigger we
            // want to drop. First match jumps to the delete label.
            let temp_name_reg = program.alloc_register();
            program.emit_column_or_rowid(temp_cursor, 1, temp_name_reg);
            for name_reg in &name_regs {
                program.emit_insn(Insn::Eq {
                    lhs: temp_name_reg,
                    rhs: *name_reg,
                    target_pc: temp_delete_label,
                    flags: crate::vdbe::insn::CmpInsFlags::default(),
                    collation: crate::translate::collate::CollationSeq::for_internal_values(),
                });
            }
            // No name matched — skip the delete.
            program.emit_insn(Insn::Goto {
                target_pc: temp_next_label,
            });
            program.preassign_label_to_next_insn(temp_delete_label);
            program.emit_insn(Insn::Delete {
                cursor_id: temp_cursor,
                table_name: SQLITE_TABLEID.to_string(),
                is_part_of_update: false,
            });
            program.preassign_label_to_next_insn(temp_next_label);
            program.emit_insn(Insn::Next {
                cursor_id: temp_cursor,
                pc_if_next: temp_loop_label,
                fullscan: false,
                is_index: false,
            });
            program.preassign_label_to_next_insn(temp_end_label);
        }
    }
    Ok(())
}

/// Writes the row of a new view.
fn emit_create_view_row(
    program: &mut ProgramBuilder,
    ctx: &CmdContext<'_>,
    view: &CreateView<'_>,
) -> Result<()> {
    let resolver = ctx.resolver;
    let namespace = NamespaceRef {
        database_id: ctx.database_id,
        id: view.namespace,
    };
    let stored_name = resolver.mangle_name(namespace, view.name);
    let sql = create_view_sql(
        "VIEW",
        &resolver
            .name_in_sql(namespace, view.written_name)
            .as_ident(),
        view.columns,
        view.select,
    );

    let sqlite_schema_cursor_id = ctx.sqlite_schema_cursor(program);
    emit_schema_entry(
        program,
        resolver,
        sqlite_schema_cursor_id,
        None,
        SchemaEntryType::View,
        &stored_name,
        &stored_name,
        // A view has no B-tree.
        0,
        Some(sql),
    )?;

    Ok(())
}

/// Writes the row of a new materialized view.
fn emit_create_materialized_view_row(
    program: &mut ProgramBuilder,
    ctx: &CmdContext<'_>,
    view: &CreateMaterializedView<'_>,
) -> Result<()> {
    let resolver = ctx.resolver;
    let namespace = NamespaceRef {
        database_id: ctx.database_id,
        id: view.namespace,
    };
    let stored_name = resolver.mangle_name(namespace, view.name);
    let sql = create_view_sql(
        "MATERIALIZED VIEW",
        &view.written_name.as_ident(),
        view.columns,
        view.select,
    );

    let sqlite_schema_cursor_id = ctx.sqlite_schema_cursor(program);
    emit_schema_entry(
        program,
        resolver,
        sqlite_schema_cursor_id,
        None,
        SchemaEntryType::View,
        &stored_name,
        &stored_name,
        view.root_reg,
        Some(sql),
    )?;

    Ok(())
}

/// Deletes the row of a view or materialized view.
fn emit_drop_view_row(
    program: &mut ProgramBuilder,
    ctx: &CmdContext<'_>,
    view: &DropView<'_>,
) -> Result<()> {
    let namespace = NamespaceRef {
        database_id: ctx.database_id,
        id: view.namespace,
    };
    let sqlite_schema_cursor_id = ctx.sqlite_schema_cursor(program);

    let view_name_reg = program.alloc_register();
    let type_reg = program.alloc_register();
    let rowid_reg = ctx.sqlite_schema_rowid_reg(program);

    program.emit_insn(Insn::String8 {
        dest: view_name_reg,
        value: ctx.resolver.mangle_name(namespace, view.name),
    });
    program.emit_insn(Insn::String8 {
        dest: type_reg,
        value: "view".to_string(),
    });

    let end_loop_label = program.allocate_label();
    let loop_start_label = program.allocate_label();

    program.emit_insn(Insn::Rewind {
        cursor_id: sqlite_schema_cursor_id,
        pc_if_empty: end_loop_label,
    });
    program.preassign_label_to_next_insn(loop_start_label);

    // Column 0 is type, column 1 is name.
    let col0_reg = program.alloc_register();
    let col1_reg = program.alloc_register();

    program.emit_column_or_rowid(sqlite_schema_cursor_id, 0, col0_reg);
    program.emit_column_or_rowid(sqlite_schema_cursor_id, 1, col1_reg);

    let skip_delete_label = program.allocate_label();

    program.emit_insn(Insn::Ne {
        lhs: col0_reg,
        rhs: type_reg,
        target_pc: skip_delete_label,
        flags: CmpInsFlags::default(),
        collation: CollationSeq::for_internal_values(),
    });
    program.emit_insn(Insn::Ne {
        lhs: col1_reg,
        rhs: view_name_reg,
        target_pc: skip_delete_label,
        flags: CmpInsFlags::default(),
        collation: CollationSeq::for_internal_values(),
    });
    program.emit_insn(Insn::RowId {
        cursor_id: sqlite_schema_cursor_id,
        dest: rowid_reg,
    });
    program.emit_insn(Insn::Delete {
        cursor_id: sqlite_schema_cursor_id,
        table_name: "sqlite_schema".to_string(),
        is_part_of_update: false,
    });

    program.preassign_label_to_next_insn(skip_delete_label);

    program.emit_insn(Insn::Next {
        cursor_id: sqlite_schema_cursor_id,
        pc_if_next: loop_start_label,
        fullscan: false,
        is_index: false,
    });

    program.preassign_label_to_next_insn(end_loop_label);

    Ok(())
}

/// The SQL text to store for a new view or materialized view.
fn create_view_sql(
    view_kind: &str,
    view_name: &str,
    columns: &[ast::IndexedColumn],
    select_stmt: &ast::Select,
) -> String {
    let columns_str = columns
        .iter()
        .map(|col| col.col_name.as_ident())
        .collect::<Vec<String>>()
        .join(", ");
    if !columns_str.is_empty() {
        return format!("CREATE {view_kind} {view_name} ({columns_str}) AS {select_stmt}");
    }
    format!("CREATE {view_kind} {view_name} AS {select_stmt}")
}

/// Writes the row of a new trigger.
fn emit_create_trigger_row(
    program: &mut ProgramBuilder,
    ctx: &CmdContext<'_>,
    trigger: &CreateTrigger<'_>,
) -> Result<()> {
    let resolver = ctx.resolver;
    let namespace = NamespaceRef {
        database_id: ctx.database_id,
        id: trigger.namespace,
    };
    let sqlite_schema_cursor_id = ctx.sqlite_schema_cursor(program);
    emit_schema_entry(
        program,
        resolver,
        sqlite_schema_cursor_id,
        // Changes of triggers are not captured.
        None,
        SchemaEntryType::Trigger,
        &resolver.mangle_name(namespace, trigger.name),
        &resolver.mangle_name(trigger.table_namespace, trigger.table),
        // A trigger has no B-tree.
        0,
        Some(trigger.sql.to_string()),
    )
}

/// Deletes the row of a trigger.
fn emit_delete_trigger_row(
    program: &mut ProgramBuilder,
    ctx: &CmdContext<'_>,
    trigger: &DropTrigger<'_>,
) {
    let namespace = NamespaceRef {
        database_id: ctx.database_id,
        id: trigger.namespace,
    };
    let sqlite_schema_cursor_id = ctx.sqlite_schema_cursor(program);

    let search_loop_label = program.allocate_label();
    let skip_non_trigger_label = program.allocate_label();
    let done_label = program.allocate_label();
    let rewind_done_label = program.allocate_label();

    program.emit_insn(Insn::Rewind {
        cursor_id: sqlite_schema_cursor_id,
        pc_if_empty: rewind_done_label,
    });

    program.preassign_label_to_next_insn(search_loop_label);

    let type_reg = program.alloc_register();
    let name_reg = program.alloc_register();
    program.emit_insn(Insn::Column {
        cursor_id: sqlite_schema_cursor_id,
        column: 0,
        dest: type_reg,
        default: None,
    });
    program.emit_insn(Insn::Column {
        cursor_id: sqlite_schema_cursor_id,
        column: 1,
        dest: name_reg,
        default: None,
    });

    let type_str_reg = program.emit_string8_new_reg("trigger".to_string());
    program.emit_insn(Insn::Ne {
        lhs: type_reg,
        rhs: type_str_reg,
        target_pc: skip_non_trigger_label,
        flags: CmpInsFlags::default(),
        collation: CollationSeq::for_internal_values(),
    });

    let trigger_name_str_reg =
        program.emit_string8_new_reg(ctx.resolver.mangle_name(namespace, trigger.name));
    program.emit_insn(Insn::Ne {
        lhs: name_reg,
        rhs: trigger_name_str_reg,
        target_pc: skip_non_trigger_label,
        flags: CmpInsFlags::default(),
        collation: CollationSeq::for_internal_values(),
    });

    program.emit_insn(Insn::Delete {
        cursor_id: sqlite_schema_cursor_id,
        table_name: SQLITE_TABLEID.to_string(),
        is_part_of_update: false,
    });
    program.emit_insn(Insn::Goto {
        target_pc: done_label,
    });

    program.preassign_label_to_next_insn(skip_non_trigger_label);
    program.emit_insn(Insn::Next {
        cursor_id: sqlite_schema_cursor_id,
        pc_if_next: search_loop_label,
        fullscan: false,
        is_index: false,
    });

    program.preassign_label_to_next_insn(done_label);
    program.preassign_label_to_next_insn(rewind_done_label);
}

/// Writes the row of a new virtual table.
fn emit_create_virtual_table_row(
    program: &mut ProgramBuilder,
    ctx: &CmdContext<'_>,
    table: &CreateVirtualTable<'_>,
) -> Result<()> {
    let resolver = ctx.resolver;
    let sqlite_schema_cursor_id = ctx.sqlite_schema_cursor(program);
    let cdc_table = prepare_cdc_if_necessary(
        program,
        resolver.schema(),
        ctx.database_id,
        Some(SQLITE_TABLEID),
    )?;
    emit_schema_entry(
        program,
        resolver,
        sqlite_schema_cursor_id,
        cdc_table.map(|x| x.0),
        SchemaEntryType::Table,
        table.name,
        table.name,
        // A virtual table has no B-tree.
        0,
        Some(table.sql.to_string()),
    )
}

/// Writes the row of a new type or domain into the types table. It creates
/// the table first when the database has none.
fn emit_create_type_row(
    program: &mut ProgramBuilder,
    ctx: &CmdContext<'_>,
    ty: &CreateType<'_>,
) -> Result<()> {
    let resolver = ctx.resolver;
    let types_table: Arc<BTreeTable>;
    let types_root_page: RegisterOrLiteral<i64>;
    if let Some(existing) = resolver
        .schema()
        .get_btree_table(NamespaceId::ROOT, TURSO_TYPES_TABLE_NAME)
    {
        types_root_page = RegisterOrLiteral::Literal(existing.root_page);
        types_table = existing;
    } else {
        let created = emit_create_internal_table(
            program,
            resolver,
            TURSO_TYPES_TABLE_NAME,
            &format!("CREATE TABLE {TURSO_TYPES_TABLE_NAME}(name TEXT PRIMARY KEY, sql TEXT)"),
        )?;
        (types_table, types_root_page) = (created.table, created.root_page);
    }

    let types_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(types_table));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: types_cursor_id,
        root_page: types_root_page,
        db: MAIN_DB_ID,
    });

    let rowid_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: types_cursor_id,
        rowid_reg,
        prev_largest_reg: 0,
    });
    let name_reg = program.emit_string8_new_reg(ty.name.to_string());
    program.emit_string8_new_reg(ty.sql.to_string());
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(name_reg),
        count: to_u32(2),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::Insert {
        cursor: types_cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: InsertFlags::new(),
        table_name: TURSO_TYPES_TABLE_NAME.to_string(),
    });
    Ok(())
}

/// Deletes the row of a type or domain from the types table.
fn emit_delete_type_row(
    program: &mut ProgramBuilder,
    ctx: &CmdContext<'_>,
    ty: &DropType<'_>,
) -> Result<()> {
    let types_table = ctx
        .resolver
        .schema()
        .get_btree_table(NamespaceId::ROOT, TURSO_TYPES_TABLE_NAME)
        .ok_or_else(|| crate::LimboError::ParseError(format!("no such type: {}", ty.name)))?;
    let types_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(types_table.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: types_cursor_id,
        root_page: types_table.root_page.into(),
        db: MAIN_DB_ID,
    });

    let name_reg = program.alloc_register();
    program.emit_insn(Insn::String8 {
        dest: name_reg,
        value: ty.name.to_string(),
    });

    let end_loop_label = program.allocate_label();
    let loop_start_label = program.allocate_label();

    program.emit_insn(Insn::Rewind {
        cursor_id: types_cursor_id,
        pc_if_empty: end_loop_label,
    });
    program.preassign_label_to_next_insn(loop_start_label);

    let col0_reg = program.alloc_register();
    program.emit_column_or_rowid(types_cursor_id, 0, col0_reg);

    let skip_delete_label = program.allocate_label();

    program.emit_insn(Insn::Ne {
        lhs: col0_reg,
        rhs: name_reg,
        target_pc: skip_delete_label,
        flags: CmpInsFlags::default(),
        collation: CollationSeq::for_internal_values(),
    });

    program.emit_insn(Insn::Delete {
        cursor_id: types_cursor_id,
        table_name: TURSO_TYPES_TABLE_NAME.to_string(),
        is_part_of_update: false,
    });

    program.preassign_label_to_next_insn(skip_delete_label);

    program.emit_insn(Insn::Next {
        cursor_id: types_cursor_id,
        pc_if_next: loop_start_label,
        fullscan: false,
        is_index: false,
    });

    program.preassign_label_to_next_insn(end_loop_label);
    Ok(())
}

/// Creates one of the engine's own tables in the main database the first
/// time it is used: a new B-tree, its row in `sqlite_schema`, and the table
/// in the in-memory schema. Returns the table and the root page that the new
/// B-tree will have.
///
/// `SetCookie` is what marks the transaction as having changed the schema,
/// so that a rollback restores the connection schema (see
/// `Pager::rollback`). It has to precede `ParseSchema`: if a later
/// instruction fails, the schema would otherwise keep the table while the
/// rollback removed its page.
pub(crate) fn emit_create_internal_table(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    table_name: &str,
    create_sql: &str,
) -> Result<InternalTable> {
    let root_reg = program.alloc_register();
    program.emit_insn(Insn::CreateBtree {
        db: MAIN_DB_ID,
        root: root_reg,
        flags: CreateBTreeFlags::new_table(),
    });
    let schema_cursor_id = open_sqlite_schema_for_write(program, resolver, MAIN_DB_ID);
    emit_schema_entry(
        program,
        resolver,
        schema_cursor_id,
        None,
        SchemaEntryType::Table,
        table_name,
        table_name,
        root_reg,
        Some(create_sql.to_string()),
    )?;
    let table = Arc::new(BTreeTable::from_stored_sql(
        resolver.catalog.dialect.as_ref(),
        create_sql,
        0,
        false,
    )?);
    emit_schema_cookie(resolver, program);
    program.emit_insn(Insn::ParseSchema {
        db: MAIN_DB_ID,
        where_clause: Some(format!("tbl_name = '{table_name}' AND type != 'trigger'")),
        trigger_target_database_id: None,
    });
    Ok(InternalTable {
        table,
        root_page: RegisterOrLiteral::Register(root_reg),
    })
}

/// A table that [`emit_create_internal_table`] creates.
pub(crate) struct InternalTable {
    pub(crate) table: Arc<BTreeTable>,
    pub(crate) root_page: RegisterOrLiteral<i64>,
}

/// Opens `sqlite_schema` of the given database for writing and returns the
/// cursor.
pub(crate) fn open_sqlite_schema_for_write(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    database_id: usize,
) -> usize {
    let cursor_id = alloc_sqlite_schema_cursor(program, resolver, database_id);
    emit_open_sqlite_schema_cursor(program, cursor_id, database_id);
    cursor_id
}

/// Gives a cursor for writing `sqlite_schema` of the given database its
/// number, without opening it.
pub(crate) fn alloc_sqlite_schema_cursor(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    database_id: usize,
) -> usize {
    let sqlite_schema = resolver.with_schema(database_id, |s| {
        s.get_btree_table(NamespaceId::ROOT, SQLITE_TABLEID)
            .expect("every database has sqlite_schema")
    });
    program.alloc_cursor_id(CursorType::BTreeTable(sqlite_schema))
}

/// Opens a cursor that [`alloc_sqlite_schema_cursor`] gave a number.
pub(crate) fn emit_open_sqlite_schema_cursor(
    program: &mut ProgramBuilder,
    cursor_id: usize,
    database_id: usize,
) {
    program.emit_insn(Insn::OpenWrite {
        cursor_id,
        root_page: RegisterOrLiteral::Literal(SQLITE_SCHEMA_ROOT_PAGE),
        db: database_id,
    });
}

/// `sqlite_schema` is at the first page of every database.
const SQLITE_SCHEMA_ROOT_PAGE: i64 = 1;

#[allow(clippy::too_many_arguments)]
pub fn emit_schema_entry(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    sqlite_schema_cursor_id: usize,
    cdc_table_cursor_id: Option<usize>,
    entry_type: SchemaEntryType,
    name: &str,
    tbl_name: &str,
    root_page_reg: usize,
    sql: Option<String>,
) -> Result<()> {
    let rowid_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: sqlite_schema_cursor_id,
        rowid_reg,
        prev_largest_reg: 0,
    });

    let type_reg = program.emit_string8_new_reg(entry_type.as_str().to_string());
    program.emit_string8_new_reg(name.to_string());
    program.emit_string8_new_reg(tbl_name.to_string());

    let table_root_reg = program.alloc_register();
    if root_page_reg == 0 {
        program.emit_insn(Insn::Integer {
            dest: table_root_reg,
            value: 0, // virtual tables in sqlite always have rootpage=0
        });
    } else {
        program.emit_insn(Insn::Copy {
            src_reg: root_page_reg,
            dst_reg: table_root_reg,
            extra_amount: 0,
        });
    }

    let sql_reg = program.alloc_register();
    if let Some(sql) = sql {
        program.emit_string8(sql, sql_reg);
    } else {
        program.emit_null(sql_reg, None);
    }

    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(type_reg),
        count: to_u32(5),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });

    program.emit_insn(Insn::Insert {
        cursor: sqlite_schema_cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: InsertFlags::new(),
        table_name: tbl_name.to_string(),
    });

    if let Some(cdc_table_cursor_id) = cdc_table_cursor_id {
        let after_record_reg = if program.capture_data_changes_info().has_after() {
            Some(record_reg)
        } else {
            None
        };
        emit_cdc_insns(
            program,
            resolver,
            OperationMode::INSERT,
            cdc_table_cursor_id,
            rowid_reg,
            None,
            after_record_reg,
            None,
            SQLITE_TABLEID,
        )?;
        emit_cdc_autocommit_commit(program, resolver, Some(cdc_table_cursor_id))?;
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
pub enum SchemaEntryType {
    Table,
    Index,
    View,
    Trigger,
}

impl SchemaEntryType {
    fn as_str(&self) -> &'static str {
        match self {
            SchemaEntryType::Table => "table",
            SchemaEntryType::Index => "index",
            SchemaEntryType::View => "view",
            SchemaEntryType::Trigger => "trigger",
        }
    }
}
