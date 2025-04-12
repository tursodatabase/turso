use std::sync::Arc;

use crate::{
    schema::{BTreeTable, Column, Index, IndexColumn, PseudoTable, Schema},
    types::Record,
    util::normalize_ident,
    vdbe::{
        builder::{CursorType, ProgramBuilder, QueryMode},
        insn::{IdxInsertFlags, Insn, RegisterOrLiteral},
    },
    OwnedValue,
};
use limbo_sqlite3_parser::ast::{self, Expr, Id, SortOrder, SortedColumn};

use super::schema::{emit_schema_entry, SchemaEntryType, SQLITE_TABLEID};

pub fn translate_create_index(
    mode: QueryMode,
    unique_if_not_exists: (bool, bool),
    idx_name: &str,
    tbl_name: &str,
    columns: &[SortedColumn],
    schema: &Schema,
) -> crate::Result<ProgramBuilder> {
    let idx_name = normalize_ident(idx_name);
    let tbl_name = normalize_ident(tbl_name);
    let mut program = ProgramBuilder::new(crate::vdbe::builder::ProgramBuilderOpts {
        query_mode: mode,
        num_cursors: 5,
        approx_num_insns: 40,
        approx_num_labels: 5,
    });

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

    // Prologue:
    let init_label = program.emit_init();
    let start_offset = program.offset();

    let idx = Arc::new(Index {
        name: idx_name.clone(),
        table_name: tbl.name.clone(),
        root_page: 0, //  we dont have access till its created, after we parse the schema table
        columns: columns
            .iter()
            .map(|c| IndexColumn {
                name: c.0 .1.name.as_ref().unwrap().clone(),
                order: c.1,
            })
            .collect(),
        unique: unique_if_not_exists.0,
    });

    // Allocate the necessary cursors:
    //
    // 1. sqlite_schema_cursor_id - sqlite_schema table
    // 2. btree_cursor_id         - new index btree
    // 3. table_cursor_id         - table we are creating the index on
    // 4. sorter_cursor_id        - sorter
    // 5. pseudo_cursor_id        - pseudo table to store the sorted index values
    let sqlite_table = schema.get_btree_table(SQLITE_TABLEID).unwrap();
    let sqlite_schema_cursor_id = program.alloc_cursor_id(
        Some(SQLITE_TABLEID.to_owned()),
        CursorType::BTreeTable(sqlite_table.clone()),
    );
    let btree_cursor_id = program.alloc_cursor_id(
        Some(idx_name.to_owned()),
        CursorType::BTreeIndex(idx.clone()),
    );
    let table_cursor_id = program.alloc_cursor_id(
        Some(tbl_name.to_owned()),
        CursorType::BTreeTable(tbl.clone()),
    );
    let sorter_cursor_id = program.alloc_cursor_id(None, CursorType::Sorter);
    let pseudo_table = PseudoTable::new_with_columns(tbl.columns.clone());
    let pseudo_cursor_id = program.alloc_cursor_id(None, CursorType::Pseudo(pseudo_table.into()));

    // Create a new B-Tree and store the root page index in a register
    let root_page_reg = program.alloc_register();
    program.emit_insn(Insn::CreateBtree {
        db: 0,
        root: root_page_reg,
        flags: 2, // index leaf
    });

    // open the sqlite schema table for writing and create a new entry for the index
    program.emit_insn(Insn::OpenWriteAsync {
        cursor_id: sqlite_schema_cursor_id,
        root_page: RegisterOrLiteral::Literal(sqlite_table.root_page),
    });
    program.emit_insn(Insn::OpenWriteAwait {});
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
    let order = idx
        .columns
        .iter()
        .map(|c| {
            OwnedValue::Integer(match c.order {
                SortOrder::Asc => 0,
                SortOrder::Desc => 1,
            })
        })
        .collect();
    // open the sorter and the pseudo table
    program.emit_insn(Insn::SorterOpen {
        cursor_id: sorter_cursor_id,
        columns: columns.len(),
        order: Record::new(order),
    });
    let content_reg = program.alloc_register();
    program.emit_insn(Insn::OpenPseudo {
        cursor_id: pseudo_cursor_id,
        content_reg,
        num_fields: columns.len() + 1,
    });

    // open the table we are creating the index on for reading
    program.emit_insn(Insn::OpenReadAsync {
        cursor_id: table_cursor_id,
        root_page: tbl.root_page,
    });
    program.emit_insn(Insn::OpenReadAwait {});

    program.emit_insn(Insn::RewindAsync {
        cursor_id: table_cursor_id,
    });
    let loop_start_label = program.allocate_label();
    let loop_end_label = program.allocate_label();
    program.emit_insn(Insn::RewindAwait {
        cursor_id: table_cursor_id,
        pc_if_empty: loop_end_label,
    });

    program.resolve_label(loop_start_label, program.offset());

    // Loop start:
    // Collect index values into start_reg..rowid_reg
    // emit MakeRecord (index key + rowid) into record_reg.
    //
    // Then insert the record into the sorter
    let start_reg = program.alloc_registers(columns.len() + 1);
    for (i, (col, _)) in columns.iter().enumerate() {
        program.emit_insn(Insn::Column {
            cursor_id: table_cursor_id,
            column: col.0,
            dest: start_reg + i,
        });
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
    });
    program.emit_insn(Insn::SorterInsert {
        cursor_id: sorter_cursor_id,
        record_reg,
    });

    program.emit_insn(Insn::NextAsync {
        cursor_id: table_cursor_id,
    });
    program.emit_insn(Insn::NextAwait {
        cursor_id: table_cursor_id,
        pc_if_next: loop_start_label,
    });
    program.resolve_label(loop_end_label, program.offset());

    // Open the index btree we created for writing to insert the
    // newly sorted index records.
    program.emit_insn(Insn::OpenWriteAsync {
        cursor_id: btree_cursor_id,
        root_page: RegisterOrLiteral::Register(root_page_reg),
    });
    program.emit_insn(Insn::OpenWriteAwait {});

    let sorted_loop_start = program.allocate_label();
    let sorted_loop_end = program.allocate_label();

    // Sort the index records in the sorter
    program.emit_insn(Insn::SorterSort {
        cursor_id: sorter_cursor_id,
        pc_if_empty: sorted_loop_end,
    });
    program.resolve_label(sorted_loop_start, program.offset());
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
    program.emit_insn(Insn::IdxInsertAsync {
        cursor_id: btree_cursor_id,
        record_reg: sorted_record_reg,
        unpacked_start: None, // TODO: optimize with these to avoid decoding record twice
        unpacked_count: None,
        flags: IdxInsertFlags::new().use_seek(false),
    });
    program.emit_insn(Insn::IdxInsertAwait {
        cursor_id: btree_cursor_id,
    });
    program.emit_insn(Insn::SorterNext {
        cursor_id: sorter_cursor_id,
        pc_if_next: sorted_loop_start,
    });
    program.resolve_label(sorted_loop_end, program.offset());

    // End of the outer loop
    //
    // Keep schema table open to emit ParseSchema, close the other cursors.
    program.close_cursors(&[sorter_cursor_id, table_cursor_id, btree_cursor_id]);

    // TODO: SetCookie for schema change
    //
    // Parse the schema table to get the index root page and add new index to Schema
    let parse_schema_where_clause = format!("name = '{}' AND type = 'index'", idx_name);
    program.emit_insn(Insn::ParseSchema {
        db: sqlite_schema_cursor_id,
        where_clause: parse_schema_where_clause,
    });
    // Close the final sqlite_schema cursor
    program.emit_insn(Insn::Close {
        cursor_id: sqlite_schema_cursor_id,
    });

    // Epilogue:
    program.emit_halt();
    program.resolve_label(init_label, program.offset());
    program.emit_transaction(true);
    program.emit_constant_insns();
    program.emit_goto(start_offset);

    Ok(program)
}

fn resolve_sorted_columns<'a>(
    table: &'a BTreeTable,
    cols: &[SortedColumn],
) -> crate::Result<Vec<((usize, &'a Column), SortOrder)>> {
    let mut resolved = Vec::with_capacity(cols.len());
    for sc in cols {
        let ident = normalize_ident(match &sc.expr {
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
