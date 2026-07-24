use crate::schema::{BTreeCharacteristics, BTreeTable, SchemaObjectType, RESERVED_TABLE_PREFIXES};
use crate::storage::pager::CreateBTreeFlags;
use crate::sync::Arc;
use crate::translate::{
    emitter::Resolver,
    schema::{emit_schema_entry, SchemaEntryType, SQLITE_TABLEID},
};
use crate::util::{
    escape_sql_string_literal, extract_view_columns, normalize_ident,
    PRIMARY_KEY_AUTOMATIC_INDEX_NAME_PREFIX,
};
use crate::vdbe::builder::{CursorType, ProgramBuilder};
use crate::vdbe::insn::{CmpInsFlags, Cookie, Insn, RegisterOrLiteral};
use crate::{bail_parse_error, Connection, Result, MAIN_DB_ID};
use turso_parser::ast;

fn validate_materialized(
    connection: &Arc<crate::Connection>,
    database_id: usize,
    resolver: &Resolver,
    normalized_view_name: &str,
) -> Result<()> {
    // Check if experimental views are enabled
    if !connection.experimental_views_enabled() {
        return Err(crate::LimboError::ParseError(
            "CREATE MATERIALIZED VIEW is an experimental feature. Enable with --experimental-views flag"
                .to_string(),
        ));
    }
    // Compiled maintenance programs currently bind every persistent cursor to
    // the main pager/schema. Block attached databases until those handles
    // carry a database id.
    if database_id != crate::MAIN_DB_ID {
        crate::bail_parse_error!("materialized views are not supported on attached databases");
    }
    if RESERVED_TABLE_PREFIXES
        .iter()
        .any(|prefix| normalized_view_name.starts_with(prefix))
    {
        bail_parse_error!("Object name reserved for internal use: {normalized_view_name}",);
    }

    // Check if view already exists (including broken sqlite_schema rows,
    // which must be dropped before the name can be reused)
    if resolver.with_schema(database_id, |s| {
        s.materialized_view_exists(normalized_view_name)
            || s.broken_views.contains(normalized_view_name)
    }) {
        return Err(crate::LimboError::ParseError(format!(
            "View {normalized_view_name} already exists"
        )));
    }
    Ok(())
}

pub fn translate_create_materialized_view(
    view_name: &ast::QualifiedName,
    resolver: &Resolver,
    select_stmt: &ast::Select,
    if_not_exists: bool,
    connection: Arc<Connection>,
    program: &mut ProgramBuilder,
) -> Result<()> {
    let database_id = resolver.resolve_database_id(view_name)?;
    let schema_cookie = resolver.with_schema(database_id, |s| s.schema_version);
    program.begin_write_on_database(database_id, schema_cookie)?;
    let normalized_view_name = normalize_ident(view_name.name.as_str());

    if if_not_exists
        && resolver.with_schema(database_id, |s| {
            s.get_view(&normalized_view_name).is_some()
                || s.materialized_view_exists(&normalized_view_name)
                || s.broken_views.contains(&normalized_view_name)
        })
    {
        return Ok(());
    }

    // Validate the view can be created and extract its columns
    // This validation happens before updating sqlite_master to prevent
    // storing invalid view definitions
    validate_materialized(&connection, database_id, resolver, &normalized_view_name)?;

    // Check for cross-database table references first
    crate::util::validate_select_for_views(select_stmt, view_name.db_name.as_ref())?;

    let view_column_schema =
        resolver.with_schema(database_id, |s| extract_view_columns(select_stmt, s))?;
    let view_columns = view_column_schema.flat_columns();

    // One plan owns validation, the executable operator DAG, and the hidden
    // storage derived from that DAG. CREATE must not independently classify
    // the SQL and guess which tables codegen will later open.
    let maintenance_plan = resolver.with_schema(database_id, |_| {
        crate::incremental::vdbe_maintenance::plan_view(
            &normalized_view_name,
            select_stmt,
            resolver,
            &connection,
        )
    })?;
    if maintenance_plan.output_arity() != view_columns.len() {
        return Err(crate::LimboError::InternalError(format!(
            "materialized view result schema has {} columns but its maintenance DAG emits {}",
            view_columns.len(),
            maintenance_plan.output_arity(),
        )));
    }
    let hidden_tables = maintenance_plan
        .hidden_tables()
        .cloned()
        .collect::<Vec<_>>();

    // Reconstruct the SQL string for storage
    let sql = create_materialized_view_to_str(&view_name.name.as_ident(), select_stmt);

    // Create a btree for storing the materialized view state
    // This btree will hold the materialized rows (row_id -> values)
    let view_root_reg = program.alloc_register();

    program.emit_insn(Insn::CreateBtree {
        db: database_id,
        root: view_root_reg,
        flags: CreateBTreeFlags::new_table(),
    });

    // Create a proper BTreeTable for the cursor with the actual view columns
    let view_table = Arc::new(BTreeTable::new(
        0, // root_page, will be set to actual root page after creation
        normalized_view_name.clone(),
        crate::alloc::vec![], // primary_key_columns — materialized views use implicit rowid
        view_columns,
        BTreeCharacteristics::HAS_ROWID,
        crate::alloc::vec![],
        crate::alloc::vec![],
        crate::alloc::vec![],
        None,
    ));

    // Allocate a cursor for writing to the view's btree during population
    let view_cursor_id =
        program.alloc_cursor_id(crate::vdbe::builder::CursorType::BTreeTable(view_table));

    // Open the cursor to the view's btree
    program.emit_insn(Insn::OpenWrite {
        cursor_id: view_cursor_id,
        root_page: RegisterOrLiteral::Register(view_root_reg),
        db: database_id,
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
        is_part_of_update: false,
    });
    program.emit_insn(Insn::Next {
        cursor_id: view_cursor_id,
        pc_if_next: clear_loop_label,
    });

    program.preassign_label_to_next_insn(clear_done_label);

    // Open cursor to sqlite_schema table
    let table = resolver.with_schema(database_id, |s| s.get_btree_table(SQLITE_TABLEID).unwrap());
    let sqlite_schema_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: sqlite_schema_cursor_id,
        root_page: 1i64.into(),
        db: database_id,
    });

    // Add the materialized view entry to sqlite_schema
    emit_schema_entry(
        program,
        resolver,
        sqlite_schema_cursor_id,
        None, // cdc_table_cursor_id, no cdc for views
        SchemaEntryType::View,
        &normalized_view_name,
        &normalized_view_name,
        view_root_reg, // btree root for materialized view data
        Some(sql),
    )?;

    // Create the version marker and node-owned operator storage described by
    // the maintenance plan. Stateful tables expose a primary-key index used by
    // codegen; the marker only persists compatibility metadata.
    let mut parse_schema_names = vec![escape_sql_string_literal(&normalized_view_name)];
    for hidden in &hidden_tables {
        let table_root_reg = program.alloc_register();
        program.emit_insn(Insn::CreateBtree {
            db: database_id,
            root: table_root_reg,
            flags: CreateBTreeFlags::new_table(),
        });
        emit_schema_entry(
            program,
            resolver,
            sqlite_schema_cursor_id,
            None,
            SchemaEntryType::Table,
            &hidden.table_name,
            &hidden.table_name,
            table_root_reg,
            Some(hidden.create_sql.clone()),
        )?;

        parse_schema_names.push(escape_sql_string_literal(&hidden.table_name));
        if hidden.primary_key_index {
            let index_root_reg = program.alloc_register();
            program.emit_insn(Insn::CreateBtree {
                db: database_id,
                root: index_root_reg,
                flags: CreateBTreeFlags::new_index(),
            });
            let index_name = format!(
                "{}{}_1",
                PRIMARY_KEY_AUTOMATIC_INDEX_NAME_PREFIX, &hidden.table_name
            );
            emit_schema_entry(
                program,
                resolver,
                sqlite_schema_cursor_id,
                None,
                SchemaEntryType::Index,
                &index_name,
                &hidden.table_name,
                index_root_reg,
                None,
            )?;
            parse_schema_names.push(escape_sql_string_literal(&index_name));
        }
    }

    // Parse schema to load the new view (and its state table, if any)
    let where_clause = parse_schema_names
        .iter()
        .map(|name| format!("name = '{name}'"))
        .collect::<Vec<_>>()
        .join(" OR ");
    program.emit_insn(Insn::ParseSchema {
        db: database_id,
        where_clause: Some(where_clause),
    });

    let schema_version = resolver.with_schema(database_id, |s| s.schema_version);
    program.emit_insn(Insn::SetCookie {
        db: database_id,
        cookie: Cookie::SchemaVersion,
        value: (schema_version + 1) as i32,
        p5: 0,
    });

    // Populate the materialized view
    let cursor_info = vec![(normalized_view_name.clone(), view_cursor_id)];
    program.emit_insn(Insn::PopulateMaterializedViews {
        cursors: cursor_info,
    });

    program.epilogue(resolver.schema());
    Ok(())
}

fn create_materialized_view_to_str(view_name: &str, select_stmt: &ast::Select) -> String {
    format!("CREATE MATERIALIZED VIEW {view_name} AS {select_stmt}")
}

fn validate_create_view(
    resolver: &Resolver,
    database_id: usize,
    normalized_view_name: &str,
) -> Result<()> {
    // Check if view already exists. A broken view (unparseable sqlite_schema
    // row) also counts: creating over it would produce a duplicate row, so
    // the user must DROP VIEW it first.
    if resolver.with_schema(database_id, |s| {
        s.get_view(normalized_view_name).is_some()
            || s.materialized_view_exists(normalized_view_name)
            || s.broken_views.contains(normalized_view_name)
    }) {
        return Err(crate::LimboError::ParseError(format!(
            "View {normalized_view_name} already exists"
        )));
    }
    if RESERVED_TABLE_PREFIXES
        .iter()
        .any(|prefix| normalized_view_name.starts_with(prefix))
    {
        bail_parse_error!("Object name reserved for internal use: {normalized_view_name}",);
    }
    Ok(())
}

pub fn translate_create_view(
    view_name: &ast::QualifiedName,
    resolver: &Resolver,
    select_stmt: &ast::Select,
    columns: &[ast::IndexedColumn],
    if_not_exists: bool,
    program: &mut ProgramBuilder,
) -> Result<()> {
    let database_id = resolver.resolve_database_id(view_name)?;
    let schema_cookie = resolver.with_schema(database_id, |s| s.schema_version);
    program.begin_write_on_database(database_id, schema_cookie)?;
    let normalized_view_name = normalize_ident(view_name.name.as_str());

    if if_not_exists
        && resolver.with_schema(database_id, |s| {
            s.get_view(&normalized_view_name).is_some()
                || s.materialized_view_exists(&normalized_view_name)
                || s.broken_views.contains(&normalized_view_name)
        })
    {
        return Ok(());
    }

    validate_create_view(resolver, database_id, &normalized_view_name)?;

    // Check for name conflicts with existing schema objects
    if let Some(object_type) =
        resolver.with_schema(database_id, |s| s.get_object_type(&normalized_view_name))
    {
        // IF NOT EXISTS suppresses errors for table/view conflicts, matching
        // CREATE TABLE IF NOT EXISTS behavior
        if if_not_exists
            && matches!(
                object_type,
                SchemaObjectType::Table | SchemaObjectType::View
            )
        {
            return Ok(());
        }
        let type_str = match object_type {
            SchemaObjectType::Table => "table",
            SchemaObjectType::View => "view",
            SchemaObjectType::Index => "index",
        };
        return Err(crate::LimboError::ParseError(format!(
            "{type_str} {normalized_view_name} already exists"
        )));
    }

    crate::util::validate_select_for_views(select_stmt, view_name.db_name.as_ref())?;

    // Reconstruct the SQL string
    let sql = create_view_to_str(&view_name.name.as_ident(), columns, select_stmt);

    // Open cursor to sqlite_schema table
    let table = resolver.schema().get_btree_table(SQLITE_TABLEID).unwrap();
    let sqlite_schema_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: sqlite_schema_cursor_id,
        root_page: 1i64.into(),
        db: database_id,
    });

    // Add the view entry to sqlite_schema
    emit_schema_entry(
        program,
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
    let escaped_view_name = escape_sql_string_literal(&normalized_view_name);
    program.emit_insn(Insn::ParseSchema {
        db: database_id,
        where_clause: Some(format!("name = '{escaped_view_name}'")),
    });

    let schema_version = resolver.with_schema(database_id, |s| s.schema_version);
    program.emit_insn(Insn::SetCookie {
        db: database_id,
        cookie: Cookie::SchemaVersion,
        value: (schema_version + 1) as i32,
        p5: 0,
    });

    Ok(())
}

fn create_view_to_str(
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
        return format!("CREATE VIEW {view_name} ({columns_str}) AS {select_stmt}");
    }
    format!("CREATE VIEW {view_name} AS {select_stmt}")
}

pub fn translate_drop_view(
    resolver: &Resolver,
    view_name: &ast::QualifiedName,
    if_exists: bool,
    program: &mut ProgramBuilder,
) -> Result<()> {
    let database_id = resolver.resolve_database_id(view_name)?;
    let schema_cookie = resolver.with_schema(database_id, |s| s.schema_version);
    program.begin_write_on_database(database_id, schema_cookie)?;
    let normalized_view_name = normalize_ident(view_name.name.as_str());

    // Check if view exists: regular, materialized, or a broken sqlite_schema
    // row whose stored SQL failed to parse at load time. Broken views have no
    // in-memory representation, but DROP VIEW must still delete their row so
    // affected databases can be cleaned up.
    let (is_regular_view, is_materialized_view, is_broken_view) =
        resolver.with_schema(database_id, |s| {
            (
                s.get_view(&normalized_view_name).is_some(),
                s.materialized_view_exists(&normalized_view_name),
                s.broken_views.contains(&normalized_view_name),
            )
        });
    let view_exists = is_regular_view || is_materialized_view || is_broken_view;

    if !view_exists && !if_exists {
        return Err(crate::LimboError::ParseError(format!(
            "no such view: {normalized_view_name}"
        )));
    }

    if !view_exists && if_exists {
        // View doesn't exist but IF EXISTS was specified, nothing to do
        return Ok(());
    }

    if is_materialized_view {
        let dependent_views = resolver.with_schema(database_id, |s| {
            s.get_dependent_materialized_views(&normalized_view_name)
        });
        if !dependent_views.is_empty() {
            return Err(crate::LimboError::ParseError(format!(
                "cannot drop materialized view \"{normalized_view_name}\": it has dependent materialized view(s): {}",
                dependent_views.join(", ")
            )));
        }
    }

    // If this is a materialized view, we need to destroy its btree as well
    // and also clean up its internal tables (aggregate state, MIN/MAX
    // multiset) — those that exist for its shape.
    let internal_table_names: Vec<String> = if is_materialized_view {
        if let Some(table) =
            resolver.with_schema(database_id, |s| s.get_table(&normalized_view_name))
        {
            if let Some(btree_table) = table.btree() {
                // Destroy the btree for the materialized view
                program.emit_insn(Insn::Destroy {
                    db: database_id,
                    root: btree_table.root_page,
                    former_root_reg: 0, // No autovacuum
                    is_temp: 0,
                });
            }
        } else if let Some(root_page) = resolver.with_schema(database_id, |s| {
            s.incompatible_materialized_view_root(&normalized_view_name)
        }) {
            program.emit_insn(Insn::Destroy {
                db: database_id,
                root: root_page,
                former_root_reg: 0,
                is_temp: 0,
            });
        }

        // Every node-owned hidden state/multiset table of this view, including
        // incompatible older storage versions. Ownership parses the complete
        // node/branch suffix so similarly prefixed view names cannot steal
        // each other's state.
        resolver.with_schema(database_id, |s| {
            s.tables
                .keys()
                .filter(|table_name| {
                    crate::incremental::view::state_table_belongs_to_view(
                        table_name,
                        &normalized_view_name,
                    )
                })
                .cloned()
                .collect::<Vec<String>>()
        })
    } else {
        Vec::new()
    };

    // Destroy the internal tables' btrees and indexes (those that exist).
    for internal_table_name in &internal_table_names {
        let internal_indexes: Vec<_> = resolver.with_schema(database_id, |s| {
            s.get_indices(internal_table_name).cloned().collect()
        });
        for index in &internal_indexes {
            program.emit_insn(Insn::Destroy {
                db: database_id,
                root: index.root_page,
                former_root_reg: 0, // No autovacuum
                is_temp: 0,
            });
        }

        if let Some(internal_table) =
            resolver.with_schema(database_id, |s| s.get_table(internal_table_name))
        {
            if let Some(internal_btree) = internal_table.btree() {
                program.emit_insn(Insn::Destroy {
                    db: database_id,
                    root: internal_btree.root_page,
                    former_root_reg: 0, // No autovacuum
                    is_temp: 0,
                });
            }
        }
    }

    // Open cursor to sqlite_schema table (structure is the same for all databases)
    let schema_table =
        resolver.with_schema(MAIN_DB_ID, |s| s.get_btree_table(SQLITE_TABLEID).unwrap());
    let sqlite_schema_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(schema_table));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: sqlite_schema_cursor_id,
        root_page: 1i64.into(),
        db: database_id,
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

    // Check if this row should be deleted
    // Column 0 is type, Column 1 is name, Column 2 is tbl_name
    let col0_reg = program.alloc_register();
    let col1_reg = program.alloc_register();

    program.emit_column_or_rowid(sqlite_schema_cursor_id, 0, col0_reg);
    program.emit_column_or_rowid(sqlite_schema_cursor_id, 1, col1_reg);

    // Check if this row matches the view, DBSP table, or DBSP index
    let skip_delete_label = program.allocate_label();

    // Check if this is the view entry (type='view' and name=view_name)
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
    // Matches view - delete it
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

    // Move to next row
    program.emit_insn(Insn::Next {
        cursor_id: sqlite_schema_cursor_id,
        pc_if_next: loop_start_label,
    });

    program.preassign_label_to_next_insn(end_loop_label);

    // If this is a materialized view, delete internal table and index
    // entries in a second pass, one loop per internal table. We do this in
    // separate loops to ensure we catch all entries even if they come in
    // different orders in sqlite_schema.
    for dbsp_table_name in &internal_table_names {
        // Set up registers for the table name and types (outside the loop for efficiency)
        let dbsp_table_name_reg_2 = program.alloc_register();
        program.emit_insn(Insn::String8 {
            dest: dbsp_table_name_reg_2,
            value: dbsp_table_name.clone(),
        });
        let table_type_reg_2 = program.alloc_register();
        program.emit_insn(Insn::String8 {
            dest: table_type_reg_2,
            value: "table".to_string(),
        });
        let index_type_reg_2 = program.alloc_register();
        program.emit_insn(Insn::String8 {
            dest: index_type_reg_2,
            value: "index".to_string(),
        });
        let dbsp_index_name_reg_2 = program.alloc_register();
        let dbsp_index_name_2 =
            format!("{PRIMARY_KEY_AUTOMATIC_INDEX_NAME_PREFIX}{dbsp_table_name}_1");
        program.emit_insn(Insn::String8 {
            dest: dbsp_index_name_reg_2,
            value: dbsp_index_name_2,
        });

        // Allocate column registers once (outside the loop)
        let dbsp_col0_reg = program.alloc_register();
        let dbsp_col1_reg = program.alloc_register();

        // Second pass: delete DBSP table and index entries
        let dbsp_end_loop_label = program.allocate_label();
        let dbsp_loop_start_label = program.allocate_label();

        program.emit_insn(Insn::Rewind {
            cursor_id: sqlite_schema_cursor_id,
            pc_if_empty: dbsp_end_loop_label,
        });
        program.preassign_label_to_next_insn(dbsp_loop_start_label);

        // Read columns for this row (reusing the same registers)
        program.emit_column_or_rowid(sqlite_schema_cursor_id, 0, dbsp_col0_reg);
        program.emit_column_or_rowid(sqlite_schema_cursor_id, 1, dbsp_col1_reg);

        let dbsp_skip_delete_label = program.allocate_label();

        // Check if this is the DBSP table entry (type='table' and name=dbsp_table_name)
        let check_dbsp_index_label = program.allocate_label();
        program.emit_insn(Insn::Ne {
            lhs: dbsp_col0_reg,
            rhs: table_type_reg_2,
            target_pc: check_dbsp_index_label,
            flags: CmpInsFlags::default(),
            collation: program.curr_collation(),
        });
        program.emit_insn(Insn::Ne {
            lhs: dbsp_col1_reg,
            rhs: dbsp_table_name_reg_2,
            target_pc: check_dbsp_index_label,
            flags: CmpInsFlags::default(),
            collation: program.curr_collation(),
        });
        // Matches DBSP table - delete it
        program.emit_insn(Insn::RowId {
            cursor_id: sqlite_schema_cursor_id,
            dest: rowid_reg,
        });
        program.emit_insn(Insn::Delete {
            cursor_id: sqlite_schema_cursor_id,
            table_name: "sqlite_schema".to_string(),
            is_part_of_update: false,
        });
        program.emit_insn(Insn::Goto {
            target_pc: dbsp_skip_delete_label,
        });

        // Check if this is the DBSP index entry (type='index' and name=dbsp_index_name)
        program.preassign_label_to_next_insn(check_dbsp_index_label);
        program.emit_insn(Insn::Ne {
            lhs: dbsp_col0_reg,
            rhs: index_type_reg_2,
            target_pc: dbsp_skip_delete_label,
            flags: CmpInsFlags::default(),
            collation: program.curr_collation(),
        });
        program.emit_insn(Insn::Ne {
            lhs: dbsp_col1_reg,
            rhs: dbsp_index_name_reg_2,
            target_pc: dbsp_skip_delete_label,
            flags: CmpInsFlags::default(),
            collation: program.curr_collation(),
        });
        // Matches DBSP index - delete it
        program.emit_insn(Insn::RowId {
            cursor_id: sqlite_schema_cursor_id,
            dest: rowid_reg,
        });
        program.emit_insn(Insn::Delete {
            cursor_id: sqlite_schema_cursor_id,
            table_name: "sqlite_schema".to_string(),
            is_part_of_update: false,
        });

        program.preassign_label_to_next_insn(dbsp_skip_delete_label);

        // Move to next row
        program.emit_insn(Insn::Next {
            cursor_id: sqlite_schema_cursor_id,
            pc_if_next: dbsp_loop_start_label,
        });

        program.preassign_label_to_next_insn(dbsp_end_loop_label);
    }

    // Remove the view from the in-memory schema
    program.emit_insn(Insn::DropView {
        db: database_id,
        view_name: normalized_view_name,
    });

    // Update schema version (increment schema cookie)
    let schema_version = resolver.with_schema(database_id, |s| s.schema_version);
    let schema_version_reg = program.alloc_register();
    program.emit_insn(Insn::Integer {
        dest: schema_version_reg,
        value: (schema_version + 1) as i64,
    });
    program.emit_insn(Insn::SetCookie {
        db: database_id,
        cookie: Cookie::SchemaVersion,
        value: (schema_version + 1) as i32,
        p5: 1, // update version
    });

    program.epilogue(resolver.schema());
    Ok(())
}
