use std::sync::Arc;

use crate::vdbe::insn::{CmpInsFlags, Cookie};
use crate::{
    schema::{BTreeTable, Column, Index, IndexColumn, PseudoCursorType, Schema},
    storage::pager::CreateBTreeFlags,
    util::normalize_ident,
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::{IdxInsertFlags, Insn, RegisterOrLiteral},
    },
};
use turso_sqlite3_parser::ast::{self, Expr, Id, SortOrder, SortedColumn};

use super::schema::{emit_schema_entry, SchemaEntryType, SQLITE_TABLEID};

pub fn translate_create_index(
    unique_if_not_exists: (bool, bool),
    idx_name: &str,
    tbl_name: &str,
    columns: &[SortedColumn],
    schema: &Schema,
    mut program: ProgramBuilder,
) -> crate::Result<ProgramBuilder> {
    if !schema.indexes_enabled() {
        crate::bail_parse_error!(
            "CREATE INDEX is disabled by default. Run with `--experimental-indexes` to enable this feature."
        );
    }
    let idx_name = normalize_ident(idx_name);
    let tbl_name = normalize_ident(tbl_name);
    let opts = crate::vdbe::builder::ProgramBuilderOpts {
        num_cursors: 5,
        approx_num_insns: 40,
        approx_num_labels: 5,
    };
    program.extend(&opts);

    // Check if the index is being created on a valid btree table and
    // the name is globally unique in the schema.
    if !schema.is_unique_idx_name(&idx_name) {
        crate::bail_parse_error!("Error: index with name '{idx_name}' already exists.");
    }
    let Some(tbl) = schema.tables.get(&tbl_name) else {
        crate::bail_parse_error!("Error: table '{tbl_name}' does not exist.");
    };
    let Some(tbl) = tbl.btree() else {
        crate::bail_parse_error!("Error: table '{tbl_name}' is not a b-tree table.");
    };
    let columns = resolve_sorted_columns(&tbl, columns)?;

    let idx = Arc::new(Index {
        name: idx_name.clone(),
        table_name: tbl.name.clone(),
        root_page: 0, //  we dont have access till its created, after we parse the schema table
        columns: columns
            .iter()
            .map(|((pos_in_table, col), order)| IndexColumn {
                name: col.name.as_ref().unwrap().clone(),
                order: *order,
                pos_in_table: *pos_in_table,
                collation: col.collation,
                default: col.default.clone(),
            })
            .collect(),
        unique: unique_if_not_exists.0,
        ephemeral: false,
        has_rowid: tbl.has_rowid,
    });

    // Allocate the necessary cursors:
    //
    // 1. sqlite_schema_cursor_id - sqlite_schema table
    // 2. btree_cursor_id         - new index btree
    // 3. table_cursor_id         - table we are creating the index on
    // 4. sorter_cursor_id        - sorter
    // 5. pseudo_cursor_id        - pseudo table to store the sorted index values
    let sqlite_table = schema.get_btree_table(SQLITE_TABLEID).unwrap();
    let sqlite_schema_cursor_id =
        program.alloc_cursor_id(CursorType::BTreeTable(sqlite_table.clone()));
    let btree_cursor_id = program.alloc_cursor_id(CursorType::BTreeIndex(idx.clone()));
    let table_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(tbl.clone()));
    let sorter_cursor_id = program.alloc_cursor_id(CursorType::Sorter);
    let pseudo_cursor_id = program.alloc_cursor_id(CursorType::Pseudo(PseudoCursorType {
        column_count: tbl.columns.len(),
    }));

    // Create a new B-Tree and store the root page index in a register
    let root_page_reg = program.alloc_register();
    program.emit_insn(Insn::CreateBtree {
        db: 0,
        root: root_page_reg,
        flags: CreateBTreeFlags::new_index(),
    });

    // open the sqlite schema table for writing and create a new entry for the index
    program.emit_insn(Insn::OpenWrite {
        cursor_id: sqlite_schema_cursor_id,
        root_page: RegisterOrLiteral::Literal(sqlite_table.root_page),
        name: sqlite_table.name.clone(),
    });
    let sql = create_idx_stmt_to_sql(&tbl_name, &idx_name, unique_if_not_exists, &columns);
    emit_schema_entry(
        &mut program,
        sqlite_schema_cursor_id,
        SchemaEntryType::Index,
        &idx_name,
        &tbl_name,
        root_page_reg,
        Some(sql),
    );

    // determine the order of the columns in the index for the sorter
    let order = idx.columns.iter().map(|c| c.order).collect();
    // open the sorter and the pseudo table
    program.emit_insn(Insn::SorterOpen {
        cursor_id: sorter_cursor_id,
        columns: columns.len(),
        order,
        collations: tbl.column_collations(),
    });
    let content_reg = program.alloc_register();
    program.emit_insn(Insn::OpenPseudo {
        cursor_id: pseudo_cursor_id,
        content_reg,
        num_fields: columns.len() + 1,
    });

    // open the table we are creating the index on for reading
    program.emit_insn(Insn::OpenRead {
        cursor_id: table_cursor_id,
        root_page: tbl.root_page,
    });

    let loop_start_label = program.allocate_label();
    let loop_end_label = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: table_cursor_id,
        pc_if_empty: loop_end_label,
    });
    program.preassign_label_to_next_insn(loop_start_label);

    // Loop start:
    // Collect index values into start_reg..rowid_reg
    // emit MakeRecord (index key + rowid) into record_reg.
    //
    // Then insert the record into the sorter
    let start_reg = program.alloc_registers(columns.len() + 1);
    for (i, (col, _)) in columns.iter().enumerate() {
        program.emit_column(table_cursor_id, col.0, start_reg + i);
    }
    let rowid_reg = start_reg + columns.len();
    program.emit_insn(Insn::RowId {
        cursor_id: table_cursor_id,
        dest: rowid_reg,
    });
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg,
        count: columns.len() + 1,
        dest_reg: record_reg,
        index_name: Some(idx_name.clone()),
    });
    program.emit_insn(Insn::SorterInsert {
        cursor_id: sorter_cursor_id,
        record_reg,
    });

    program.emit_insn(Insn::Next {
        cursor_id: table_cursor_id,
        pc_if_next: loop_start_label,
    });
    program.preassign_label_to_next_insn(loop_end_label);

    // Open the index btree we created for writing to insert the
    // newly sorted index records.
    program.emit_insn(Insn::OpenWrite {
        cursor_id: btree_cursor_id,
        root_page: RegisterOrLiteral::Register(root_page_reg),
        name: idx_name.clone(),
    });

    let sorted_loop_start = program.allocate_label();
    let sorted_loop_end = program.allocate_label();

    // Sort the index records in the sorter
    program.emit_insn(Insn::SorterSort {
        cursor_id: sorter_cursor_id,
        pc_if_empty: sorted_loop_end,
    });
    program.preassign_label_to_next_insn(sorted_loop_start);
    let sorted_record_reg = program.alloc_register();
    program.emit_insn(Insn::SorterData {
        pseudo_cursor: pseudo_cursor_id,
        cursor_id: sorter_cursor_id,
        dest_reg: sorted_record_reg,
    });

    // seek to the end of the index btree to position the cursor for appending
    program.emit_insn(Insn::SeekEnd {
        cursor_id: btree_cursor_id,
    });
    // insert new index record
    program.emit_insn(Insn::IdxInsert {
        cursor_id: btree_cursor_id,
        record_reg: sorted_record_reg,
        unpacked_start: None, // TODO: optimize with these to avoid decoding record twice
        unpacked_count: None,
        flags: IdxInsertFlags::new().use_seek(false),
    });
    program.emit_insn(Insn::SorterNext {
        cursor_id: sorter_cursor_id,
        pc_if_next: sorted_loop_start,
    });
    program.preassign_label_to_next_insn(sorted_loop_end);

    // End of the outer loop
    //
    // Keep schema table open to emit ParseSchema, close the other cursors.
    program.close_cursors(&[sorter_cursor_id, table_cursor_id, btree_cursor_id]);

    program.emit_insn(Insn::SetCookie {
        db: 0,
        cookie: Cookie::SchemaVersion,
        value: schema.schema_version as i32 + 1,
        p5: 0,
    });
    // Parse the schema table to get the index root page and add new index to Schema
    let parse_schema_where_clause = format!("name = '{idx_name}' AND type = 'index'");
    program.emit_insn(Insn::ParseSchema {
        db: sqlite_schema_cursor_id,
        where_clause: Some(parse_schema_where_clause),
    });
    // Close the final sqlite_schema cursor
    program.emit_insn(Insn::Close {
        cursor_id: sqlite_schema_cursor_id,
    });

    // Epilogue:
    program.epilogue(super::emitter::TransactionMode::Write);

    Ok(program)
}

fn resolve_sorted_columns<'a>(
    table: &'a BTreeTable,
    cols: &[SortedColumn],
) -> crate::Result<Vec<((usize, &'a Column), SortOrder)>> {
    let mut resolved = Vec::with_capacity(cols.len());
    for sc in cols {
        let ident = normalize_ident(match &sc.expr {
            // SQLite supports indexes on arbitrary expressions, but we don't (yet).
            // See "How to use indexes on expressions" in https://www.sqlite.org/expridx.html
            Expr::Id(Id(col_name)) | Expr::Name(ast::Name(col_name)) => col_name,
            _ => crate::bail_parse_error!("Error: cannot use expressions in CREATE INDEX"),
        });
        let Some(col) = table.get_column(&ident) else {
            crate::bail_parse_error!(
                "Error: column '{ident}' does not exist in table '{}'",
                table.name
            );
        };
        resolved.push((col, sc.order.unwrap_or(SortOrder::Asc)));
    }
    Ok(resolved)
}

fn create_idx_stmt_to_sql(
    tbl_name: &str,
    idx_name: &str,
    unique_if_not_exists: (bool, bool),
    cols: &[((usize, &Column), SortOrder)],
) -> String {
    let mut sql = String::with_capacity(128);
    sql.push_str("CREATE ");
    if unique_if_not_exists.0 {
        sql.push_str("UNIQUE ");
    }
    sql.push_str("INDEX ");
    if unique_if_not_exists.1 {
        sql.push_str("IF NOT EXISTS ");
    }
    sql.push_str(idx_name);
    sql.push_str(" ON ");
    sql.push_str(tbl_name);
    sql.push_str(" (");
    for (i, (col, order)) in cols.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(col.1.name.as_ref().unwrap());
        if *order == SortOrder::Desc {
            sql.push_str(" DESC");
        }
    }
    sql.push(')');
    sql
}

pub fn translate_drop_index(
    idx_name: &str,
    if_exists: bool,
    schema: &Schema,
    mut program: ProgramBuilder,
) -> crate::Result<ProgramBuilder> {
    if !schema.indexes_enabled() {
        crate::bail_parse_error!(
            "DROP INDEX is disabled by default. Run with `--experimental-indexes` to enable this feature."
        );
    }
    let idx_name = normalize_ident(idx_name);
    let opts = crate::vdbe::builder::ProgramBuilderOpts {
        num_cursors: 5,
        approx_num_insns: 40,
        approx_num_labels: 5,
    };
    program.extend(&opts);

    // Find the index in Schema
    let mut maybe_index = None;
    for val in schema.indexes.values() {
        if maybe_index.is_some() {
            break;
        }
        for idx in val {
            if idx.name == idx_name {
                maybe_index = Some(idx);
                break;
            }
        }
    }

    // If there's no index if_exist is true,
    // then return normaly, otherwise show an error.
    if maybe_index.is_none() {
        if if_exists {
            program.epilogue(super::emitter::TransactionMode::Write);
            return Ok(program);
        } else {
            return Err(crate::error::LimboError::InvalidArgument(format!(
                "No such index: {}",
                &idx_name
            )));
        }
    }

    // According to sqlite should emit Null instruction
    // but why?
    let null_reg = program.alloc_register();
    program.emit_null(null_reg, None);

    // String8; r[3] = 'some idx name'
    let index_name_reg = program.emit_string8_new_reg(idx_name.to_string());
    // String8; r[4] = 'index'
    let index_str_reg = program.emit_string8_new_reg("index".to_string());

    // for r[5]=rowid
    let row_id_reg = program.alloc_register();

    // We're going to use this cursor to search through sqlite_schema
    let sqlite_table = schema.get_btree_table(SQLITE_TABLEID).unwrap();
    let sqlite_schema_cursor_id =
        program.alloc_cursor_id(CursorType::BTreeTable(sqlite_table.clone()));

    // Open root=1 iDb=0; sqlite_schema for writing
    program.emit_insn(Insn::OpenWrite {
        cursor_id: sqlite_schema_cursor_id,
        root_page: RegisterOrLiteral::Literal(sqlite_table.root_page),
        name: sqlite_table.name.clone(),
    });

    let loop_start_label = program.allocate_label();
    let loop_end_label = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: sqlite_schema_cursor_id,
        pc_if_empty: loop_end_label,
    });
    program.resolve_label(loop_start_label, program.offset());

    // Read sqlite_schema.name into dest_reg
    let dest_reg = program.alloc_register();
    program.emit_column(sqlite_schema_cursor_id, 1, dest_reg);

    // if current column is not index_name then jump to Next
    // skip if sqlite_schema.name != index_name_reg
    let next_label = program.allocate_label();
    program.emit_insn(Insn::Ne {
        lhs: index_name_reg,
        rhs: dest_reg,
        target_pc: next_label,
        flags: CmpInsFlags::default(),
        collation: program.curr_collation(),
    });

    // read type of table
    // skip if sqlite_schema.type != 'index' (index_str_reg)
    program.emit_column(sqlite_schema_cursor_id, 0, dest_reg);
    // if current column is not index then jump to Next
    program.emit_insn(Insn::Ne {
        lhs: index_str_reg,
        rhs: dest_reg,
        target_pc: next_label,
        flags: CmpInsFlags::default(),
        collation: program.curr_collation(),
    });

    program.emit_insn(Insn::RowId {
        cursor_id: sqlite_schema_cursor_id,
        dest: row_id_reg,
    });

    let label_once_end = program.allocate_label();
    program.emit_insn(Insn::Once {
        target_pc_when_reentered: label_once_end,
    });
    program.resolve_label(label_once_end, program.offset());

    program.emit_insn(Insn::Delete {
        cursor_id: sqlite_schema_cursor_id,
    });

    program.resolve_label(next_label, program.offset());
    program.emit_insn(Insn::Next {
        cursor_id: sqlite_schema_cursor_id,
        pc_if_next: loop_start_label,
    });

    program.resolve_label(loop_end_label, program.offset());

    program.emit_insn(Insn::SetCookie {
        db: 0,
        cookie: Cookie::SchemaVersion,
        value: schema.schema_version as i32 + 1,
        p5: 0,
    });

    // Destroy index btree
    program.emit_insn(Insn::Destroy {
        root: maybe_index.unwrap().root_page,
        former_root_reg: 0,
        is_temp: 0,
    });

    // Remove from the Schema any mention of the index
    if let Some(idx) = maybe_index {
        program.emit_insn(Insn::DropIndex {
            index: idx.clone(),
            db: 0,
        });
    }

    // Epilogue:
    program.epilogue(super::emitter::TransactionMode::Write);

    Ok(program)
}
