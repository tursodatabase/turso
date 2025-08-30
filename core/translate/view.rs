use crate::schema::Schema;
use crate::translate::emitter::Resolver;
use crate::translate::schema::{emit_schema_entry, SchemaEntryType, SQLITE_TABLEID};
use crate::util::normalize_ident;
use crate::vdbe::builder::{CursorType, ProgramBuilder};
use crate::vdbe::insn::{CmpInsFlags, Cookie, Insn};
use crate::{Connection, Result, SymbolTable};
use std::sync::Arc;
use turso_parser::ast;

/// Common logic for creating views (both regular and materialized)
fn emit_create_view_program(
    schema: &Schema,
    view_name: &str,
    sql: String,
    syms: &SymbolTable,
    program: &mut ProgramBuilder,
    populate_materialized: bool,
) -> Result<()> {
    let normalized_view_name = normalize_ident(view_name);

    // Open cursor to sqlite_schema table
    let table = schema.get_btree_table(SQLITE_TABLEID).unwrap();
    let sqlite_schema_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: sqlite_schema_cursor_id,
        root_page: 1usize.into(),
        db: 0,
    });

    // Add the view entry to sqlite_schema
    let resolver = Resolver::new(schema, syms);
    emit_schema_entry(
        program,
        &resolver,
        sqlite_schema_cursor_id,
        None, // cdc_table_cursor_id, no cdc for views
        SchemaEntryType::View,
        &normalized_view_name,
        &normalized_view_name, // for views, tbl_name is same as name
        0,                     // views don't have a root page
        Some(sql),
    )?;

    // Parse schema to load the new view
    program.emit_insn(Insn::ParseSchema {
        db: sqlite_schema_cursor_id,
        where_clause: Some(format!("name = '{normalized_view_name}'")),
    });

    program.emit_insn(Insn::SetCookie {
        db: 0,
        cookie: Cookie::SchemaVersion,
        value: (schema.schema_version + 1) as i32,
        p5: 0,
    });

    // Populate materialized views if needed
    // Note: This must come after SetCookie since it may do I/O operations
    if populate_materialized {
        program.emit_insn(Insn::PopulateMaterializedViews);
    }

    Ok(())
}

pub fn translate_create_materialized_view(
    schema: &Schema,
    view_name: &str,
    select_stmt: &ast::Select,
    connection: Arc<Connection>,
    syms: &SymbolTable,
    mut program: ProgramBuilder,
) -> Result<ProgramBuilder> {
    // Check if experimental views are enabled
    if !connection.experimental_views_enabled() {
        return Err(crate::LimboError::ParseError(
            "CREATE MATERIALIZED VIEW is an experimental feature. Enable with --experimental-views flag"
                .to_string(),
        ));
    }

    let normalized_view_name = normalize_ident(view_name);

    // Check if view already exists
    if schema
        .get_materialized_view(&normalized_view_name)
        .is_some()
    {
        return Err(crate::LimboError::ParseError(format!(
            "View {normalized_view_name} already exists"
        )));
    }

    // Validate that this view can be created as an IncrementalView
    // This validation happens before updating sqlite_master to prevent
    // storing invalid view definitions
    use crate::incremental::view::IncrementalView;
    IncrementalView::can_create_view(select_stmt)?;

    // Reconstruct the SQL string
    let sql = create_materialized_view_to_str(view_name, select_stmt);

    // Use common logic to emit the view creation program
    emit_create_view_program(schema, view_name, sql, syms, &mut program, true)?;

    program.epilogue(schema);
    Ok(program)
}

fn create_materialized_view_to_str(view_name: &str, select_stmt: &ast::Select) -> String {
    format!("CREATE MATERIALIZED VIEW {view_name} AS {select_stmt}")
}

pub fn translate_create_view(
    schema: &Schema,
    view_name: &str,
    select_stmt: &ast::Select,
    _columns: &[ast::IndexedColumn],
    _connection: Arc<Connection>,
    syms: &SymbolTable,
    mut program: ProgramBuilder,
) -> Result<ProgramBuilder> {
    let normalized_view_name = normalize_ident(view_name);

    // Check if view already exists
    if schema.get_view(&normalized_view_name).is_some()
        || schema
            .get_materialized_view(&normalized_view_name)
            .is_some()
    {
        return Err(crate::LimboError::ParseError(format!(
            "View {normalized_view_name} already exists"
        )));
    }

    // Reconstruct the SQL string
    let sql = create_view_to_str(view_name, select_stmt);

    // Use common logic to emit the view creation program
    emit_create_view_program(schema, view_name, sql, syms, &mut program, false)?;

    Ok(program)
}

fn create_view_to_str(view_name: &str, select_stmt: &ast::Select) -> String {
    format!("CREATE VIEW {view_name} AS {select_stmt}")
}

pub fn translate_drop_view(
    schema: &Schema,
    view_name: &str,
    if_exists: bool,
    mut program: ProgramBuilder,
) -> Result<ProgramBuilder> {
    let normalized_view_name = normalize_ident(view_name);

    // Check if view exists (either regular or materialized)
    let view_exists = schema.get_view(&normalized_view_name).is_some()
        || schema
            .get_materialized_view(&normalized_view_name)
            .is_some();

    if !view_exists && !if_exists {
        return Err(crate::LimboError::ParseError(format!(
            "no such view: {normalized_view_name}"
        )));
    }

    if !view_exists && if_exists {
        // View doesn't exist but IF EXISTS was specified, nothing to do
        return Ok(program);
    }

    // Open cursor to sqlite_schema table
    let schema_table = schema.get_btree_table(SQLITE_TABLEID).unwrap();
    let sqlite_schema_cursor_id =
        program.alloc_cursor_id(CursorType::BTreeTable(schema_table.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: sqlite_schema_cursor_id,
        root_page: 1usize.into(),
        db: 0,
    });

    // Allocate registers for searching
    let view_name_reg = program.alloc_register();
    let type_reg = program.alloc_register();
    let rowid_reg = program.alloc_register();

    // Set the view name and type we're looking for
    program.emit_insn(Insn::String8 {
        dest: view_name_reg,
        value: normalized_view_name.clone(),
    });
    program.emit_insn(Insn::String8 {
        dest: type_reg,
        value: "view".to_string(),
    });

    // Start scanning from the beginning
    let end_loop_label = program.allocate_label();
    let loop_start_label = program.allocate_label();

    program.emit_insn(Insn::Rewind {
        cursor_id: sqlite_schema_cursor_id,
        pc_if_empty: end_loop_label,
    });
    program.preassign_label_to_next_insn(loop_start_label);

    // Check if this row is the view we're looking for
    // Column 0 is type, Column 1 is name, Column 2 is tbl_name
    let col0_reg = program.alloc_register();
    let col1_reg = program.alloc_register();

    program.emit_column_or_rowid(sqlite_schema_cursor_id, 0, col0_reg);
    program.emit_column_or_rowid(sqlite_schema_cursor_id, 1, col1_reg);

    // Check if type == 'view' and name == view_name
    let skip_delete_label = program.allocate_label();
    program.emit_insn(Insn::Ne {
        lhs: col0_reg,
        rhs: type_reg,
        target_pc: skip_delete_label,
        flags: CmpInsFlags::default(),
        collation: program.curr_collation(),
    });
    program.emit_insn(Insn::Ne {
        lhs: col1_reg,
        rhs: view_name_reg,
        target_pc: skip_delete_label,
        flags: CmpInsFlags::default(),
        collation: program.curr_collation(),
    });

    // Get the rowid and delete this row
    program.emit_insn(Insn::RowId {
        cursor_id: sqlite_schema_cursor_id,
        dest: rowid_reg,
    });
    program.emit_insn(Insn::Delete {
        cursor_id: sqlite_schema_cursor_id,
        table_name: "sqlite_schema".to_string(),
    });

    program.resolve_label(skip_delete_label, program.offset());

    // Move to next row
    program.emit_insn(Insn::Next {
        cursor_id: sqlite_schema_cursor_id,
        pc_if_next: loop_start_label,
    });

    program.preassign_label_to_next_insn(end_loop_label);

    // Remove the view from the in-memory schema
    program.emit_insn(Insn::DropView {
        db: 0,
        view_name: normalized_view_name.clone(),
    });

    // Update schema version (increment schema cookie)
    let schema_version_reg = program.alloc_register();
    program.emit_insn(Insn::Integer {
        dest: schema_version_reg,
        value: (schema.schema_version + 1) as i64,
    });
    program.emit_insn(Insn::SetCookie {
        db: 0,
        cookie: Cookie::SchemaVersion,
        value: (schema.schema_version + 1) as i32,
        p5: 1, // update version
    });

    program.epilogue(schema);
    Ok(program)
}
