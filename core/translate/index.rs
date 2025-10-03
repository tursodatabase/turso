use std::sync::Arc;

use crate::bail_parse_error;
use crate::schema::{Table, RESERVED_TABLE_PREFIXES};
use crate::translate::emitter::{
    emit_cdc_full_record, emit_cdc_insns, prepare_cdc_if_necessary, OperationMode, Resolver,
};
use crate::translate::expr::{translate_condition_expr, ConditionMetadata};
use crate::translate::plan::{
    ColumnUsedMask, IterationDirection, JoinedTable, Operation, Scan, TableReferences,
};
use crate::vdbe::builder::CursorKey;
use crate::vdbe::insn::{CmpInsFlags, Cookie};
use crate::vdbe::BranchOffset;
use crate::{
    schema::{BTreeTable, Column, Index, IndexColumn, PseudoCursorType},
    storage::pager::CreateBTreeFlags,
    util::normalize_ident,
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::{IdxInsertFlags, Insn, RegisterOrLiteral},
    },
};
use turso_parser::ast::{Expr, Name, SortOrder, SortedColumn};

use super::schema::{emit_schema_entry, SchemaEntryType, SQLITE_TABLEID};

#[allow(clippy::too_many_arguments)]
pub fn translate_create_index(
    unique_if_not_exists: (bool, bool),
    resolver: &Resolver,
    idx_name: &Name,
    tbl_name: &Name,
    columns: &[SortedColumn],
    mut program: ProgramBuilder,
    connection: &Arc<crate::Connection>,
    where_clause: Option<Box<Expr>>,
) -> crate::Result<ProgramBuilder> {
    let original_idx_name = idx_name;
    let original_tbl_name = tbl_name;
    let idx_name = normalize_ident(idx_name.as_str());
    let tbl_name = normalize_ident(tbl_name.as_str());

    if tbl_name.eq_ignore_ascii_case("sqlite_sequence") {
        crate::bail_parse_error!("table sqlite_sequence may not be indexed");
    }
    if connection.mvcc_enabled() {
        crate::bail_parse_error!("CREATE INDEX is currently not supported when MVCC is enabled.");
    }
    if !resolver.schema.indexes_enabled() {
        crate::bail_parse_error!(
            "CREATE INDEX is disabled by default. Run with `--experimental-indexes` to enable this feature."
        );
    }
    if RESERVED_TABLE_PREFIXES
        .iter()
        .any(|prefix| idx_name.starts_with(prefix))
    {
        bail_parse_error!(
            "Object name reserved for internal use: {}",
            original_idx_name
        );
    }
    if RESERVED_TABLE_PREFIXES
        .iter()
        .any(|prefix| tbl_name.starts_with(prefix))
    {
        bail_parse_error!("Object name reserved for internal use: {}", tbl_name);
    }
    let opts = crate::vdbe::builder::ProgramBuilderOpts {
        num_cursors: 5,
        approx_num_insns: 40,
        approx_num_labels: 5,
    };
    program.extend(&opts);

    // Check if the index is being created on a valid btree table and
    // the name is globally unique in the schema.
    if !resolver.schema.is_unique_idx_name(&idx_name) {
        // If IF NOT EXISTS is specified, silently return without error
        if unique_if_not_exists.1 {
            return Ok(program);
        }
        crate::bail_parse_error!("Error: index with name '{idx_name}' already exists.");
    }
    let Some(table) = resolver.schema.tables.get(&tbl_name) else {
        crate::bail_parse_error!("Error: table '{tbl_name}' does not exist.");
    };
    let Some(tbl) = table.btree() else {
        crate::bail_parse_error!("Error: table '{tbl_name}' is not a b-tree table.");
    };
    let original_columns = columns;
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
        // store the *original* where clause, because we need to rewrite it
        // before translating, and it cannot reference a table alias
        where_clause: where_clause.clone(),
    });

    if !idx.validate_where_expr(table) {
        crate::bail_parse_error!(
            "Error: cannot use aggregate, window functions or reference other tables in WHERE clause of CREATE INDEX:\n {}",
            where_clause
                .expect("where expr has to exist in order to fail")
                .to_string()
        );
    }

    // Allocate the necessary cursors:
    //
    // 1. sqlite_schema_cursor_id - sqlite_schema table
    // 2. btree_cursor_id         - new index btree
    // 3. table_cursor_id         - table we are creating the index on
    // 4. sorter_cursor_id        - sorter
    // 5. pseudo_cursor_id        - pseudo table to store the sorted index values
    let sqlite_table = resolver.schema.get_btree_table(SQLITE_TABLEID).unwrap();
    let sqlite_schema_cursor_id =
        program.alloc_cursor_id(CursorType::BTreeTable(sqlite_table.clone()));
    let table_ref = program.table_reference_counter.next();
    let btree_cursor_id = program.alloc_cursor_id(CursorType::BTreeIndex(idx.clone()));
    let table_cursor_id = program.alloc_cursor_id_keyed(
        CursorKey::table(table_ref),
        CursorType::BTreeTable(tbl.clone()),
    );
    let sorter_cursor_id = program.alloc_cursor_id(CursorType::Sorter);
    let pseudo_cursor_id = program.alloc_cursor_id(CursorType::Pseudo(PseudoCursorType {
        column_count: tbl.columns.len(),
    }));

    let mut table_references = TableReferences::new(
        vec![JoinedTable {
            op: Operation::Scan(Scan::BTreeTable {
                iter_dir: IterationDirection::Forwards,
                index: None,
            }),
            table: Table::BTree(tbl.clone()),
            identifier: tbl_name.clone(),
            internal_id: table_ref,
            join_info: None,
            col_used_mask: ColumnUsedMask::default(),
            database_id: 0,
        }],
        vec![],
    );
    let where_clause = idx.bind_where_expr(Some(&mut table_references), connection);

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
        db: 0,
    });
    let sql = create_idx_stmt_to_sql(
        &original_tbl_name.as_ident(),
        &original_idx_name.as_ident(),
        unique_if_not_exists,
        original_columns,
        &idx.where_clause.clone(),
    );
    let cdc_table = prepare_cdc_if_necessary(&mut program, resolver.schema, SQLITE_TABLEID)?;
    emit_schema_entry(
        &mut program,
        resolver,
        sqlite_schema_cursor_id,
        cdc_table.map(|x| x.0),
        SchemaEntryType::Index,
        &idx_name,
        &tbl_name,
        root_page_reg,
        Some(sql),
    )?;

    // determine the order of the columns in the index for the sorter
    let order = idx.columns.iter().map(|c| c.order).collect();
    // open the sorter and the pseudo table
    program.emit_insn(Insn::SorterOpen {
        cursor_id: sorter_cursor_id,
        columns: columns.len(),
        order,
        collations: idx.columns.iter().map(|c| c.collation).collect(),
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
        db: 0,
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
    let mut skip_row_label = None;
    if let Some(where_clause) = where_clause {
        let label = program.allocate_label();
        translate_condition_expr(
            &mut program,
            &table_references,
            &where_clause,
            ConditionMetadata {
                jump_if_condition_is_true: false,
                jump_target_when_false: label,
                jump_target_when_true: BranchOffset::Placeholder,
            },
            resolver,
        )?;
        skip_row_label = Some(label);
    }

    let start_reg = program.alloc_registers(columns.len() + 1);
    for (i, (col, _)) in columns.iter().enumerate() {
        program.emit_column_or_rowid(table_cursor_id, col.0, start_reg + i);
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
        affinity_str: None,
    });
    program.emit_insn(Insn::SorterInsert {
        cursor_id: sorter_cursor_id,
        record_reg,
    });

    if let Some(skip_row_label) = skip_row_label {
        program.resolve_label(skip_row_label, program.offset());
    }
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
        db: 0,
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
        value: resolver.schema.schema_version as i32 + 1,
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

    Ok(program)
}

fn resolve_sorted_columns<'a>(
    table: &'a BTreeTable,
    cols: &[SortedColumn],
) -> crate::Result<Vec<((usize, &'a Column), SortOrder)>> {
    let mut resolved = Vec::with_capacity(cols.len());
    for sc in cols {
        let ident = match sc.expr.as_ref() {
            // SQLite supports indexes on arbitrary expressions, but we don't (yet).
            // See "How to use indexes on expressions" in https://www.sqlite.org/expridx.html
            Expr::Id(col_name) | Expr::Name(col_name) => col_name.as_str(),
            _ => crate::bail_parse_error!("Error: cannot use expressions in CREATE INDEX"),
        };
        let Some(col) = table.get_column(ident) else {
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
    cols: &[SortedColumn],
    where_clause: &Option<Box<Expr>>,
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
    for (i, col) in cols.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        let col_ident = match col.expr.as_ref() {
            Expr::Id(name) | Expr::Name(name) => name.as_ident(),
            _ => unreachable!("expressions in CREATE INDEX should have been rejected earlier"),
        };
        sql.push_str(&col_ident);
        if col.order.unwrap_or(SortOrder::Asc) == SortOrder::Desc {
            sql.push_str(" DESC");
        }
    }
    sql.push(')');
    if let Some(where_clause) = where_clause {
        sql.push_str(" WHERE ");
        sql.push_str(&where_clause.to_string());
    }
    sql
}

pub fn translate_drop_index(
    idx_name: &str,
    resolver: &Resolver,
    if_exists: bool,
    mut program: ProgramBuilder,
) -> crate::Result<ProgramBuilder> {
    if !resolver.schema.indexes_enabled() {
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
    for val in resolver.schema.indexes.values() {
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
            return Ok(program);
        } else {
            return Err(crate::error::LimboError::InvalidArgument(format!(
                "No such index: {}",
                &idx_name
            )));
        }
    }
    // Return an error if the index is associated with a unique or primary key constraint.
    if let Some(idx) = maybe_index {
        if idx.unique {
            return Err(crate::error::LimboError::InvalidArgument(
                "index associated with UNIQUE or PRIMARY KEY constraint cannot be dropped"
                    .to_string(),
            ));
        }
    }

    let cdc_table = prepare_cdc_if_necessary(&mut program, resolver.schema, SQLITE_TABLEID)?;

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
    let sqlite_table = resolver.schema.get_btree_table(SQLITE_TABLEID).unwrap();
    let sqlite_schema_cursor_id =
        program.alloc_cursor_id(CursorType::BTreeTable(sqlite_table.clone()));

    // Open root=1 iDb=0; sqlite_schema for writing
    program.emit_insn(Insn::OpenWrite {
        cursor_id: sqlite_schema_cursor_id,
        root_page: RegisterOrLiteral::Literal(sqlite_table.root_page),
        db: 0,
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
    program.emit_column_or_rowid(sqlite_schema_cursor_id, 1, dest_reg);

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
    program.emit_column_or_rowid(sqlite_schema_cursor_id, 0, dest_reg);
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

    if let Some((cdc_cursor_id, _)) = cdc_table {
        let before_record_reg = if program.capture_data_changes_mode().has_before() {
            Some(emit_cdc_full_record(
                &mut program,
                &sqlite_table.columns,
                sqlite_schema_cursor_id,
                row_id_reg,
            ))
        } else {
            None
        };
        emit_cdc_insns(
            &mut program,
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
        value: resolver.schema.schema_version as i32 + 1,
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

    Ok(program)
}
