use crate::schema::{Schema, DBSP_TABLE_PREFIX};
use crate::storage::pager::CreateBTreeFlags;
use crate::translate::emitter::Resolver;
use crate::translate::schema::{emit_schema_entry, SchemaEntryType, SQLITE_TABLEID};
use crate::util::{normalize_ident, PRIMARY_KEY_AUTOMATIC_INDEX_NAME_PREFIX};
use crate::vdbe::builder::{CursorType, ProgramBuilder};
use crate::vdbe::insn::{CmpInsFlags, Cookie, Insn, RegisterOrLiteral};
use crate::{Connection, Result};
use std::sync::Arc;
use turso_parser::ast;

pub fn translate_create_materialized_view(
    view_name: &ast::Name,
    resolver: &Resolver,
    select_stmt: &ast::Select,
    connection: Arc<Connection>,
    mut program: ProgramBuilder,
) -> Result<ProgramBuilder> {
    // Check if experimental views are enabled
    if !connection.experimental_views_enabled() {
        return Err(crate::LimboError::ParseError(
            "CREATE MATERIALIZED VIEW is an experimental feature. Enable with --experimental-views flag"
                .to_string(),
        ));
    }

    let normalized_view_name = normalize_ident(view_name.as_str());

    // Check if view already exists
    if resolver
        .schema
        .get_materialized_view(&normalized_view_name)
        .is_some()
    {
        return Err(crate::LimboError::ParseError(format!(
            "View {normalized_view_name} already exists"
        )));
    }

    // Validate the view can be created and extract its columns
    // This validation happens before updating sqlite_master to prevent
    // storing invalid view definitions
    use crate::incremental::view::IncrementalView;
    use crate::schema::BTreeTable;
    let view_column_schema =
        IncrementalView::validate_and_extract_columns(select_stmt, resolver.schema)?;
    let view_columns = view_column_schema.flat_columns();

    // Reconstruct the SQL string for storage
    let sql = create_materialized_view_to_str(&view_name.as_ident(), select_stmt);

    // Create a btree for storing the materialized view state
    // This btree will hold the materialized rows (row_id -> values)
    let view_root_reg = program.alloc_register();

    program.emit_insn(Insn::CreateBtree {
        db: 0,
        root: view_root_reg,
        flags: CreateBTreeFlags::new_table(),
    });

    // Create a second btree for DBSP operator state (e.g., aggregate state)
    // This is stored as a hidden table: __turso_internal_dbsp_state_<view_name>
    let dbsp_state_root_reg = program.alloc_register();

    program.emit_insn(Insn::CreateBtree {
        db: 0,
        root: dbsp_state_root_reg,
        flags: CreateBTreeFlags::new_table(),
    });

    // Create a proper BTreeTable for the cursor with the actual view columns
    let view_table = Arc::new(BTreeTable {
        root_page: 0, // Will be set to actual root page after creation
        name: normalized_view_name.clone(),
        columns: view_columns.clone(),
        primary_key_columns: vec![], // Materialized views use implicit rowid
        has_rowid: true,
        is_strict: false,
        has_autoincrement: false,

        unique_sets: vec![],
    });

    // Allocate a cursor for writing to the view's btree during population
    let view_cursor_id = program.alloc_cursor_id(crate::vdbe::builder::CursorType::BTreeTable(
        view_table.clone(),
    ));

    // Open the cursor to the view's btree
    program.emit_insn(Insn::OpenWrite {
        cursor_id: view_cursor_id,
        root_page: RegisterOrLiteral::Register(view_root_reg),
        db: 0,
    });

    // Clear any existing data in the btree
    // This is important because if we're reusing a page that previously held
    // a materialized view, there might be old data still there
    // We need to start with a clean slate
    let clear_loop_label = program.allocate_label();
    let clear_done_label = program.allocate_label();

    // Rewind to the beginning of the btree
    program.emit_insn(Insn::Rewind {
        cursor_id: view_cursor_id,
        pc_if_empty: clear_done_label,
    });

    // Loop to delete all rows
    program.preassign_label_to_next_insn(clear_loop_label);
    program.emit_insn(Insn::Delete {
        cursor_id: view_cursor_id,
        table_name: normalized_view_name.clone(),
    });
    program.emit_insn(Insn::Next {
        cursor_id: view_cursor_id,
        pc_if_next: clear_loop_label,
    });

    program.preassign_label_to_next_insn(clear_done_label);

    // Open cursor to sqlite_schema table
    let table = resolver.schema.get_btree_table(SQLITE_TABLEID).unwrap();
    let sqlite_schema_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: sqlite_schema_cursor_id,
        root_page: 1i64.into(),
        db: 0,
    });

    // Add the materialized view entry to sqlite_schema
    emit_schema_entry(
        &mut program,
        resolver,
        sqlite_schema_cursor_id,
        None, // cdc_table_cursor_id, no cdc for views
        SchemaEntryType::View,
        &normalized_view_name,
        &normalized_view_name,
        view_root_reg, // btree root for materialized view data
        Some(sql),
    )?;

    // Add the DBSP state table to sqlite_master (required for materialized views)
    // Include the version number in the table name
    use crate::incremental::compiler::DBSP_CIRCUIT_VERSION;
    let dbsp_table_name = ast::Name::exact(format!(
        "{DBSP_TABLE_PREFIX}{DBSP_CIRCUIT_VERSION}_{normalized_view_name}"
    ));
    let dbsp_table_ident = dbsp_table_name.as_ident();
    // The element_id column uses SQLite's dynamic typing system to store different value types:
    // - For hash-based operators (joins, filters): stores INTEGER hash values or rowids
    // - For future MIN/MAX operators: stores the actual values being compared (INTEGER, REAL, TEXT, BLOB)
    // SQLite's type affinity and sorting rules ensure correct ordering within each operator's data
    let dbsp_sql = format!(
        "CREATE TABLE {dbsp_table_ident} (\
         operator_id INTEGER NOT NULL, \
         zset_id BLOB NOT NULL, \
         element_id BLOB NOT NULL, \
         value BLOB, \
         weight INTEGER NOT NULL, \
         PRIMARY KEY (operator_id, zset_id, element_id)\
        )"
    );

    emit_schema_entry(
        &mut program,
        resolver,
        sqlite_schema_cursor_id,
        None, // cdc_table_cursor_id
        SchemaEntryType::Table,
        dbsp_table_name.as_str(),
        dbsp_table_name.as_str(),
        dbsp_state_root_reg, // Root for DBSP state table
        Some(dbsp_sql),
    )?;

    // Create automatic primary key index for the DBSP table
    // Since the table has PRIMARY KEY (operator_id, zset_id, element_id), we need an index
    let dbsp_index_root_reg = program.alloc_register();
    program.emit_insn(Insn::CreateBtree {
        db: 0,
        root: dbsp_index_root_reg,
        flags: CreateBTreeFlags::new_index(),
    });

    // Register the index in sqlite_schema
    let dbsp_index_name = format!(
        "{}{}_1",
        PRIMARY_KEY_AUTOMATIC_INDEX_NAME_PREFIX,
        &dbsp_table_name.as_str()
    );
    emit_schema_entry(
        &mut program,
        resolver,
        sqlite_schema_cursor_id,
        None, // cdc_table_cursor_id
        SchemaEntryType::Index,
        &dbsp_index_name,
        dbsp_table_name.as_str(),
        dbsp_index_root_reg,
        None, // Automatic indexes don't store SQL
    )?;

    // Parse schema to load the new view and DBSP state table
    program.emit_insn(Insn::ParseSchema {
        db: sqlite_schema_cursor_id,
        where_clause: Some(format!(
            "name = '{normalized_view_name}' OR name = '{dbsp_table_name}' OR name = '{dbsp_index_name}'"
        )),
    });

    program.emit_insn(Insn::SetCookie {
        db: 0,
        cookie: Cookie::SchemaVersion,
        value: (resolver.schema.schema_version + 1) as i32,
        p5: 0,
    });

    // Populate the materialized view
    let cursor_info = vec![(normalized_view_name.clone(), view_cursor_id)];
    program.emit_insn(Insn::PopulateMaterializedViews {
        cursors: cursor_info,
    });

    program.epilogue(resolver.schema);
    Ok(program)
}

fn create_materialized_view_to_str(view_name: &str, select_stmt: &ast::Select) -> String {
    format!("CREATE MATERIALIZED VIEW {view_name} AS {select_stmt}")
}

pub fn translate_create_view(
    view_name: &ast::Name,
    resolver: &Resolver,
    select_stmt: &ast::Select,
    _columns: &[ast::IndexedColumn],
    _connection: Arc<Connection>,
    mut program: ProgramBuilder,
) -> Result<ProgramBuilder> {
    let normalized_view_name = normalize_ident(view_name.as_str());

    // Check if view already exists
    if resolver.schema.get_view(&normalized_view_name).is_some()
        || resolver
            .schema
            .get_materialized_view(&normalized_view_name)
            .is_some()
    {
        return Err(crate::LimboError::ParseError(format!(
            "View {normalized_view_name} already exists"
        )));
    }

    // Reconstruct the SQL string
    let sql = create_view_to_str(&view_name.as_ident(), select_stmt);

    // Open cursor to sqlite_schema table
    let table = resolver.schema.get_btree_table(SQLITE_TABLEID).unwrap();
    let sqlite_schema_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: sqlite_schema_cursor_id,
        root_page: 1i64.into(),
        db: 0,
    });

    // Add the view entry to sqlite_schema
    emit_schema_entry(
        &mut program,
        resolver,
        sqlite_schema_cursor_id,
        None, // cdc_table_cursor_id, no cdc for views
        SchemaEntryType::View,
        &normalized_view_name,
        &normalized_view_name,
        0, // Regular views don't have a btree
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
        value: (resolver.schema.schema_version + 1) as i32,
        p5: 0,
    });

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
    let is_regular_view = schema.get_view(&normalized_view_name).is_some();
    let is_materialized_view = schema.is_materialized_view(&normalized_view_name);
    let view_exists = is_regular_view || is_materialized_view;

    if !view_exists && !if_exists {
        return Err(crate::LimboError::ParseError(format!(
            "no such view: {normalized_view_name}"
        )));
    }

    if !view_exists && if_exists {
        // View doesn't exist but IF EXISTS was specified, nothing to do
        return Ok(program);
    }

    // If this is a materialized view, we need to destroy its btree as well
    if is_materialized_view {
        if let Some(table) = schema.get_table(&normalized_view_name) {
            if let Some(btree_table) = table.btree() {
                // Destroy the btree for the materialized view
                program.emit_insn(Insn::Destroy {
                    root: btree_table.root_page,
                    former_root_reg: 0, // No autovacuum
                    is_temp: 0,
                });
            }
        }
    }

    // Open cursor to sqlite_schema table
    let schema_table = schema.get_btree_table(SQLITE_TABLEID).unwrap();
    let sqlite_schema_cursor_id =
        program.alloc_cursor_id(CursorType::BTreeTable(schema_table.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: sqlite_schema_cursor_id,
        root_page: 1i64.into(),
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

    // Both regular and materialized views are stored as type='view' in sqlite_schema
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
