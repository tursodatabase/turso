use crate::error::SQLITE_CONSTRAINT_UNIQUE;
use crate::function::{Deterministic, Func, ScalarFunc};
use crate::index_method::IndexMethodConfiguration;
use crate::numeric::Numeric;
use crate::schema::{Column, GeneratedType, Table, EXPR_INDEX_SENTINEL, RESERVED_TABLE_PREFIXES};
use crate::sync::Arc;
use crate::translate::{
    collate::CollationSeq,
    emitter::{
        emit_cdc_autocommit_commit, emit_cdc_full_record, emit_cdc_insns, prepare_cdc_if_necessary,
        OperationMode, Resolver,
    },
    expr::{
        bind_and_rewrite_expr, translate_condition_expr, translate_expr, unwrap_parens, walk_expr,
        BindingBehavior, ConditionMetadata, WalkControl,
    },
    insert::format_unique_violation_desc,
    plan::{ColumnUsedMask, IterationDirection, JoinedTable, Operation, Scan, TableReferences},
};
use crate::vdbe::builder::{CursorKey, ProgramBuilderOpts, SelfTableContext};
use crate::vdbe::insn::{to_u16, CmpInsFlags, Cookie};
use crate::{bail_parse_error, CaptureDataChangesExt, LimboError, MAIN_DB_ID};
use crate::{
    schema::{BTreeTable, Index, IndexColumn, PseudoCursorType},
    storage::pager::CreateBTreeFlags,
    util::{escape_sql_string_literal, normalize_ident, PRIMARY_KEY_AUTOMATIC_INDEX_NAME_PREFIX},
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::{IdxInsertFlags, Insn, RegisterOrLiteral},
    },
};
use rustc_hash::FxHashMap as HashMap;
use turso_parser::ast::{self, Expr, QualifiedName, SortOrder, SortedColumn};

use super::schema::{emit_schema_entry, SchemaEntryType, SQLITE_TABLEID};

fn canonical_create_index_sql(stmt: &ast::Stmt) -> String {
    let mut canonical_stmt = stmt.clone();
    let ast::Stmt::CreateIndex { idx_name, .. } = &mut canonical_stmt else {
        unreachable!("canonical_create_index_sql requires a CREATE INDEX statement");
    };
    // Attached db's index names may have a connection-local schema alias like `aux.idx1`.
    // Strip that qualifier before storing SQL in sqlite_schema so the schema text remains valid
    // when the same database is reopened or attached under a different alias.
    // e.g. CREATE INDEX aux.idx1 ON t1 (name) -> CREATE INDEX idx1 ON t1 (name)
    idx_name.db_name = None;
    canonical_stmt.to_string()
}

fn validate(
    tbl_name: &str,
    idx_name: &str,
    original_idx_name: &QualifiedName,
    using: &Option<ast::Name>,
    with_clause: &[(ast::Name, Box<Expr>)],
    connection: &Arc<crate::Connection>,
) -> crate::Result<()> {
    if !connection.experimental_index_method_enabled()
        && (using.is_some() || !with_clause.is_empty())
    {
        bail_parse_error!(
            "index method is an experimental feature. Enable with --experimental-index-method flag"
        )
    }
    if connection.mvcc_enabled() && using.is_some() {
        bail_parse_error!("Custom index modules are not supported in MVCC mode");
    }
    if tbl_name.eq_ignore_ascii_case("sqlite_sequence") {
        crate::bail_parse_error!("table sqlite_sequence may not be indexed");
    }
    if RESERVED_TABLE_PREFIXES
        .iter()
        .any(|prefix| idx_name.starts_with(prefix) || tbl_name.starts_with(prefix))
        && !connection.is_nested_stmt()
    {
        bail_parse_error!(
            "Object name reserved for internal use: {}",
            original_idx_name
        );
    }
    Ok(())
}

pub fn translate_create_index(
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
    resolver: &Resolver,
    stmt: ast::Stmt,
) -> crate::Result<()> {
    let sql = canonical_create_index_sql(&stmt);
    let ast::Stmt::CreateIndex {
        unique,
        if_not_exists,
        idx_name,
        tbl_name,
        columns,
        where_clause,
        with_clause,
        using,
    } = stmt
    else {
        panic!("translate_create_index must be called with CreateIndex AST node");
    };

    let original_idx_name = idx_name;
    let database_id = if original_idx_name.db_name.is_some() {
        resolver.resolve_database_id(&original_idx_name)?
    } else {
        resolver.resolve_existing_table_database_id(tbl_name.as_str())?
    };
    let idx_name = normalize_ident(original_idx_name.name.as_str());
    let tbl_name = normalize_ident(tbl_name.as_str());

    validate(
        &tbl_name,
        &idx_name,
        &original_idx_name,
        &using,
        &with_clause,
        connection,
    )?;
    let opts = ProgramBuilderOpts::new(5, 40, 5);
    program.extend(&opts);

    let schema_cookie = resolver.with_schema(database_id, |s| s.schema_version);
    program.begin_write_on_database(database_id, schema_cookie);

    // Check if the index is being created on a valid btree table and
    // the name is globally unique in the schema.
    let schema_unique = resolver.with_schema(database_id, |s| s.is_unique_idx_name(&idx_name));
    if !schema_unique {
        // If IF NOT EXISTS is specified, silently return without error
        if if_not_exists {
            return Ok(());
        }
        crate::bail_parse_error!("Error: index with name '{idx_name}' already exists.");
    }
    let table = resolver.with_schema(database_id, |s| s.get_table(&tbl_name));
    let Some(table) = table else {
        crate::bail_parse_error!("Error: table '{tbl_name}' does not exist.");
    };
    let Some(tbl) = table.btree() else {
        crate::bail_parse_error!("Error: table '{tbl_name}' is not a b-tree table.");
    };
    if !tbl.has_rowid {
        bail_parse_error!("CREATE INDEX on WITHOUT ROWID tables is not supported");
    }
    let columns = resolve_sorted_columns(&tbl, &columns)?;

    // Block CREATE INDEX on non-orderable custom type columns and STRUCT/UNION columns
    for col in &columns {
        if col.expr.is_none() {
            // Simple column reference (not expression index)
            if let Some(column) = tbl.columns().get(col.pos_in_table) {
                if let Some(type_def) = resolver
                    .schema()
                    .get_type_def(&column.ty_str, tbl.is_strict)
                {
                    if type_def.is_struct() || type_def.is_union() {
                        bail_parse_error!(
                            "cannot create index on STRUCT/UNION column '{}'",
                            col.name
                        );
                    }
                    if type_def.decode().is_some()
                        && !type_def.operators().iter().any(|op| op.op == "<")
                    {
                        bail_parse_error!(
                            "cannot create index on column '{}' of type '{}': type does not declare OPERATOR '<'",
                            col.name,
                            type_def.name
                        );
                    }
                }
            }
        }
    }

    if !with_clause.is_empty() && using.is_none() {
        crate::bail_parse_error!(
            "Error: additional parameters are allowed only for custom module indices: '{idx_name}' is not custom module index"
        );
    }

    let mut index_method = None;
    if let Some(using) = &using {
        let index_modules = &resolver.symbol_table.index_methods;
        let using = using.as_str();
        let index_module = index_modules.get(using);
        if index_module.is_none() {
            crate::bail_parse_error!("Error: unknown module name '{}'", using);
        }
        if let Some(index_module) = index_module {
            let parameters = resolve_index_method_parameters(with_clause)?;
            index_method = Some(index_module.attach(&IndexMethodConfiguration {
                table_name: tbl.name.clone(),
                index_name: idx_name.clone(),
                columns: columns.clone(),
                parameters,
            })?);
        }
    }
    let idx = Arc::new(Index {
        name: idx_name.clone(),
        table_name: tbl.name.clone(),
        root_page: 0, //  we dont have access till its created, after we parse the schema table
        columns: columns.clone(),
        unique,
        ephemeral: false,
        has_rowid: tbl.has_rowid,
        // store the *original* where clause, because we need to rewrite it
        // before translating, and it cannot reference a table alias
        where_clause: where_clause.clone(),
        index_method: index_method.clone(),
        on_conflict: None,
    });

    if !idx.validate_where_expr(&table, resolver) {
        crate::bail_parse_error!(
            "Error: cannot use aggregate, window functions or reference other tables in WHERE clause of CREATE INDEX:\n {}",
            where_clause
                .as_ref()
                .expect("where expr has to exist in order to fail")
                .to_string()
        );
    }

    // Allocate the necessary cursors:
    //
    // 1. sqlite_schema_cursor_id - sqlite_schema table
    // 2. index_cursor_id         - new index cursor
    // 3. table_cursor_id         - table we are creating the index on
    // 4. sorter_cursor_id        - sorter
    // 5. pseudo_cursor_id        - pseudo table to store the sorted index values
    let sqlite_table = resolver.schema().get_btree_table(SQLITE_TABLEID).unwrap();
    let sqlite_schema_cursor_id =
        program.alloc_cursor_id(CursorType::BTreeTable(sqlite_table.clone()));
    let table_ref = program.table_reference_counter.next();
    let index_cursor_id = program.alloc_cursor_index(None, &idx)?;
    let table_cursor_id = program.alloc_cursor_id_keyed(
        CursorKey::table(table_ref),
        CursorType::BTreeTable(tbl.clone()),
    );
    let sorter_cursor_id = program.alloc_cursor_id(CursorType::Sorter);
    let pseudo_cursor_id = program.alloc_cursor_id(CursorType::Pseudo(PseudoCursorType {
        column_count: tbl.columns().len(),
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
            column_use_counts: Vec::new(),
            expression_index_usages: Vec::new(),
            database_id: MAIN_DB_ID,
            indexed: None,
        }],
        vec![],
    );
    let where_clause = idx.bind_where_expr(Some(&mut table_references), resolver);

    // Create a new B-Tree and store the root page index in a register
    let root_page_reg = program.alloc_register();
    if idx.index_method.is_some() && !idx.is_backing_btree_index() {
        program.emit_insn(Insn::IndexMethodCreate {
            db: database_id,
            cursor_id: index_cursor_id,
        });
        // index method sqlite_schema row always has root_page equals to zero in the schema (same as virtual tables)
        program.emit_int(0, root_page_reg);
    } else {
        program.emit_insn(Insn::CreateBtree {
            db: database_id,
            root: root_page_reg,
            flags: CreateBTreeFlags::new_index(),
        });
    }

    // open the sqlite schema table for writing and create a new entry for the index
    program.emit_insn(Insn::OpenWrite {
        cursor_id: sqlite_schema_cursor_id,
        root_page: RegisterOrLiteral::Literal(sqlite_table.root_page),
        db: database_id,
    });
    let cdc_table = prepare_cdc_if_necessary(program, resolver.schema(), SQLITE_TABLEID)?;
    emit_schema_entry(
        program,
        resolver,
        sqlite_schema_cursor_id,
        cdc_table.map(|x| x.0),
        SchemaEntryType::Index,
        &idx_name,
        &tbl_name,
        root_page_reg,
        Some(sql),
    )?;

    if index_method
        .as_ref()
        .is_some_and(|m| !m.definition().backing_btree)
    {
        // open the table we are creating the index on for reading
        program.emit_insn(Insn::OpenRead {
            cursor_id: table_cursor_id,
            root_page: tbl.root_page,
            db: database_id,
        });

        // Open the index btree we created for writing to insert the
        // newly sorted index records.
        program.emit_insn(Insn::OpenWrite {
            cursor_id: index_cursor_id,
            root_page: RegisterOrLiteral::Register(root_page_reg),
            db: database_id,
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
            let condition_true_label = program.allocate_label();
            translate_condition_expr(
                program,
                &table_references,
                &where_clause,
                ConditionMetadata {
                    jump_if_condition_is_true: false,
                    jump_target_when_false: label,
                    jump_target_when_true: condition_true_label,
                    jump_target_when_null: label,
                },
                resolver,
            )?;
            program.preassign_label_to_next_insn(condition_true_label);
            skip_row_label = Some(label);
        }

        let start_reg = program.alloc_registers(columns.len() + 1);
        for (i, col) in columns.iter().enumerate() {
            emit_index_column_value_from_cursor(
                program,
                resolver,
                &mut table_references,
                table_cursor_id,
                col,
                start_reg + i,
            )?;
        }
        let rowid_reg = start_reg + columns.len();
        program.emit_insn(Insn::RowId {
            cursor_id: table_cursor_id,
            dest: rowid_reg,
        });
        let record_reg = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u16(start_reg),
            count: to_u16(columns.len() + 1),
            dest_reg: to_u16(record_reg),
            index_name: Some(idx_name.clone()),
            affinity_str: None,
        });

        // insert new index record
        program.emit_insn(Insn::IdxInsert {
            cursor_id: index_cursor_id,
            record_reg,
            unpacked_start: Some(start_reg),
            unpacked_count: Some((columns.len() + 1) as u16),
            flags: IdxInsertFlags::new().use_seek(false),
        });

        if let Some(skip_row_label) = skip_row_label {
            program.preassign_label_to_next_insn(skip_row_label);
        }
        program.emit_insn(Insn::Next {
            cursor_id: table_cursor_id,
            pc_if_next: loop_start_label,
        });
        program.preassign_label_to_next_insn(loop_end_label);
    } else if index_method.is_none() {
        // determine the order, collation, and nulls ordering of the columns in the index for the sorter
        let order_collations_nulls = idx
            .columns
            .iter()
            .map(|c| (c.order, c.collation, None))
            .collect();
        // open the sorter and the pseudo table
        program.emit_insn(Insn::SorterOpen {
            cursor_id: sorter_cursor_id,
            columns: columns.len(),
            order_collations_nulls,
            comparators: vec![],
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
            db: database_id,
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
            let condition_true_label = program.allocate_label();
            translate_condition_expr(
                program,
                &table_references,
                &where_clause,
                ConditionMetadata {
                    jump_if_condition_is_true: false,
                    jump_target_when_false: label,
                    jump_target_when_true: condition_true_label,
                    jump_target_when_null: label,
                },
                resolver,
            )?;
            program.preassign_label_to_next_insn(condition_true_label);
            skip_row_label = Some(label);
        }

        let start_reg = program.alloc_registers(columns.len() + 1);
        for (i, col) in columns.iter().enumerate() {
            emit_index_column_value_from_cursor(
                program,
                resolver,
                &mut table_references,
                table_cursor_id,
                col,
                start_reg + i,
            )?;
        }
        let rowid_reg = start_reg + columns.len();
        program.emit_insn(Insn::RowId {
            cursor_id: table_cursor_id,
            dest: rowid_reg,
        });
        let record_reg = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u16(start_reg),
            count: to_u16(columns.len() + 1),
            dest_reg: to_u16(record_reg),
            index_name: Some(idx_name.clone()),
            affinity_str: None,
        });
        program.emit_insn(Insn::SorterInsert {
            cursor_id: sorter_cursor_id,
            record_reg,
        });

        if let Some(skip_row_label) = skip_row_label {
            program.preassign_label_to_next_insn(skip_row_label);
        }
        program.emit_insn(Insn::Next {
            cursor_id: table_cursor_id,
            pc_if_next: loop_start_label,
        });
        program.preassign_label_to_next_insn(loop_end_label);

        // Open the index btree we created for writing to insert the
        // newly sorted index records.
        program.emit_insn(Insn::OpenWrite {
            cursor_id: index_cursor_id,
            root_page: RegisterOrLiteral::Register(root_page_reg),
            db: database_id,
        });

        let sorted_loop_start = program.allocate_label();
        let sorted_loop_end = program.allocate_label();

        // Sort the index records in the sorter
        program.emit_insn(Insn::SorterSort {
            cursor_id: sorter_cursor_id,
            pc_if_empty: sorted_loop_end,
        });

        let sorted_record_reg = program.alloc_register();

        if unique {
            // Since the records to be inserted are sorted, we can compare prev with current and if they are equal,
            // we fall through to Halt with a unique constraint violation error.
            let goto_label = program.allocate_label();
            let label_after_sorter_compare = program.allocate_label();
            program.preassign_label_to_next_insn(goto_label);
            program.emit_insn(Insn::Goto {
                target_pc: label_after_sorter_compare,
            });
            program.preassign_label_to_next_insn(sorted_loop_start);
            program.emit_insn(Insn::SorterCompare {
                cursor_id: sorter_cursor_id,
                sorted_record_reg,
                num_regs: columns.len(),
                pc_when_nonequal: goto_label,
            });
            program.emit_insn(Insn::Halt {
                err_code: SQLITE_CONSTRAINT_UNIQUE,
                description: format_unique_violation_desc(tbl_name.as_str(), &idx),
                on_error: None,
                description_reg: None,
            });
            program.preassign_label_to_next_insn(label_after_sorter_compare);
        } else {
            program.preassign_label_to_next_insn(sorted_loop_start);
        }

        program.emit_insn(Insn::SorterData {
            pseudo_cursor: pseudo_cursor_id,
            cursor_id: sorter_cursor_id,
            dest_reg: sorted_record_reg,
        });

        // seek to the end of the index btree to position the cursor for appending
        program.emit_insn(Insn::SeekEnd {
            cursor_id: index_cursor_id,
        });
        // insert new index record
        program.emit_insn(Insn::IdxInsert {
            cursor_id: index_cursor_id,
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
    }

    // End of the outer loop
    //
    // Keep schema table open to emit ParseSchema, close the other cursors.
    program.close_cursors(&[sorter_cursor_id, table_cursor_id, index_cursor_id]);

    let current_schema_version = resolver.with_schema(database_id, |s| s.schema_version);
    program.emit_insn(Insn::SetCookie {
        db: database_id,
        cookie: Cookie::SchemaVersion,
        value: current_schema_version as i32 + 1,
        p5: 0,
    });
    // Parse the schema table to get the index root page and add new index to Schema
    let escaped_idx_name = escape_sql_string_literal(&idx_name);
    let parse_schema_where_clause = format!("name = '{escaped_idx_name}' AND type = 'index'");
    program.emit_insn(Insn::ParseSchema {
        db: database_id,
        where_clause: Some(parse_schema_where_clause),
    });
    // Close the final sqlite_schema cursor
    program.emit_insn(Insn::Close {
        cursor_id: sqlite_schema_cursor_id,
    });

    Ok(())
}

pub fn resolve_sorted_columns(
    table: &BTreeTable,
    cols: &[SortedColumn],
) -> crate::Result<Vec<IndexColumn>> {
    let mut resolved = Vec::with_capacity(cols.len());
    for sc in cols {
        let order = sc.order.unwrap_or(SortOrder::Asc);
        let (explicit_collation, base_expr) = extract_collation(sc.expr.as_ref())?;
        // Unwrap parentheses for column resolution (SQLite treats (('col')) same as 'col')
        let unwrapped_expr = unwrap_parens(base_expr)?;
        if let Some((pos, column_name, column)) = resolve_index_column(unwrapped_expr, table) {
            let collation = explicit_collation.or_else(|| column.collation_opt());
            let expr = match column.generated_type() {
                GeneratedType::Virtual { expr, .. } => Some(expr.clone()),
                GeneratedType::NotGenerated => None,
            };
            resolved.push(IndexColumn {
                name: column_name,
                order,
                pos_in_table: pos,
                collation,
                default: column.default.clone(),
                expr,
            });
            continue;
        }
        if !validate_index_expression(unwrapped_expr, table) {
            crate::bail_parse_error!("Error: invalid expression in CREATE INDEX: {}", sc.expr);
        }
        resolved.push(IndexColumn {
            name: sc.expr.to_string(),
            order,
            pos_in_table: EXPR_INDEX_SENTINEL,
            collation: explicit_collation,
            default: None,
            expr: Some(sc.expr.clone()),
        });
    }
    Ok(resolved)
}

/// Extracts collation sequence from an expression if it is a Collate expression.
/// Given the example: `col1 COLLATE NOCASE` / Expr::Collation(Expr::Id(col1), CollationSeq)
/// returns (Some(CollationSeq), Expr::Id(col1))
fn extract_collation(expr: &Expr) -> crate::Result<(Option<CollationSeq>, &Expr)> {
    let mut current = expr;
    let mut coll = None;
    while let Expr::Collate(inner, seq) = current {
        coll = Some(CollationSeq::new(seq.as_str())?);
        current = inner.as_ref();
    }
    Ok((coll, current))
}

/// For a given Index Expression, attempts to resolve it to a column position in the table.
/// Returning (position_in_table, column_name, column_reference).
/// This is needed to support Collated indexes, where the ast node is not simply the column name,
/// but isn't treated like an arbitrary expression either.
fn resolve_index_column<'a>(
    expr: &'a Expr,
    table: &'a BTreeTable,
) -> Option<(usize, String, &'a Column)> {
    let (pos, column) = match expr {
        Expr::Id(col_name) | Expr::Name(col_name) => table.get_column(col_name.as_str())?,
        // SQLite interprets single-quoted strings as column names in index expressions
        // (backwards compatibility quirk). We do the same. The string includes quotes.
        Expr::Literal(ast::Literal::String(col_name)) => {
            let unquoted = col_name.trim_matches('\'');
            table.get_column(unquoted)?
        }
        Expr::Qualified(_, col) | Expr::DoublyQualified(_, _, col) => {
            table.get_column(col.as_str())?
        }
        Expr::RowId { .. } => table.get_rowid_alias_column()?,
        _ => return None,
    };
    let column_name = column
        .name
        .as_ref()
        .expect("column name must exist for indexed column")
        .clone();
    Some((pos, column_name, column))
}

/// Validates that an index expression only contains allowed constructs.
///
/// https://sqlite.org/expridx.html
/// There are certain reasonable restrictions on expressions that appear in CREATE INDEX statements:
/// Expressions in CREATE INDEX statements may only refer to columns of the table being indexed, not to columns in other tables.
/// Expressions in CREATE INDEX statements may contain function calls, but only to functions whose output is always determined completely by its input parameters
/// (a.k.a.: deterministic functions). Obviously, functions like random() will not work well in an index. But also functions like sqlite_version(), though they
/// are constant across any one database connection, are not constant across the life of the underlying database file, and hence may not be used in a CREATE INDEX statement.
/// Expressions in CREATE INDEX statements may not use subqueries.
/// Additionally, a standalone string literal is interpreted as a column name (for backwards
/// compatibility with SQLite), not as a string literal. It is rejected if no such column exists.
fn validate_index_expression(expr: &Expr, table: &BTreeTable) -> bool {
    // A top-level string literal would have been handled by resolve_index_column().
    // If we get here with a string literal, it means the column doesn't exist.
    // (SQLite interprets standalone string literals as column names for backwards compat.)
    // Note: extract_collation already unwraps parentheses, so we check the unwrapped expr.
    if matches!(expr, Expr::Literal(ast::Literal::String(_))) {
        return false;
    }

    let tbl_norm = normalize_ident(table.name.as_str());
    let has_col = |name: &str| {
        let n = normalize_ident(name);
        table
            .columns()
            .iter()
            .any(|c| c.name.as_ref().is_some_and(|cn| normalize_ident(cn) == n))
    };
    let is_tbl = |ns: &str| normalize_ident(ns).eq_ignore_ascii_case(&tbl_norm);
    let is_deterministic_fn = |name: &str, args: &[Box<Expr>]| {
        let n = normalize_ident(name);
        Func::resolve_function(&n, args.len())
            .is_ok_and(|f| f.is_some_and(|f| is_valid_index_function_call(&f, args)))
    };

    let mut ok = true;
    let _ = walk_expr(expr, &mut |e: &Expr| -> crate::Result<WalkControl> {
        if !ok {
            return Ok(WalkControl::SkipChildren);
        }
        match e {
            // String literals inside expressions are allowed (e.g., `c0 || 'suffix'`).
            // Standalone string literals are handled by resolve_index_column() which interprets
            // them as column names (SQLite backwards compat quirk).
            Expr::Literal(
                ast::Literal::CurrentDate
                | ast::Literal::CurrentTime
                | ast::Literal::CurrentTimestamp,
            ) => {
                ok = false;
            }
            Expr::Literal(_) | Expr::RowId { .. } => {}
            // must be a column of the target table
            Expr::Id(n) | Expr::Name(n) => {
                if !has_col(n.as_str()) {
                    ok = false;
                }
            }
            // Qualified: qualifier must match this index's table, column must exist
            Expr::Qualified(ns, col) | Expr::DoublyQualified(_, ns, col) => {
                if !is_tbl(ns.as_str()) || !has_col(col.as_str()) {
                    ok = false;
                }
            }
            Expr::FunctionCall {
                name, filter_over, ..
            }
            | Expr::FunctionCallStar {
                name, filter_over, ..
            } => {
                // reject windowed
                if filter_over.over_clause.is_some() {
                    ok = false;
                } else {
                    let argc = match e {
                        Expr::FunctionCall { args, .. } => args.as_slice(),
                        Expr::FunctionCallStar { .. } => &[] as &[Box<Expr>],
                        _ => unreachable!(),
                    };
                    if !is_deterministic_fn(name.as_str(), argc) {
                        ok = false;
                    }
                }
            }
            // Explicitly disallowed constructs
            Expr::Exists(_)
            | Expr::InSelect { .. }
            | Expr::Subquery(_)
            | Expr::Raise { .. }
            | Expr::Variable(_) => {
                ok = false;
            }
            _ => {}
        }
        Ok(if ok {
            WalkControl::Continue
        } else {
            WalkControl::SkipChildren
        })
    });
    ok
}

fn is_valid_index_function_call(func: &Func, args: &[Box<Expr>]) -> bool {
    match func {
        Func::Scalar(
            ScalarFunc::Date
            | ScalarFunc::Time
            | ScalarFunc::DateTime
            | ScalarFunc::UnixEpoch
            | ScalarFunc::JulianDay
            | ScalarFunc::StrfTime
            | ScalarFunc::TimeDiff,
        ) => is_deterministic_datetime_index_call(func, args),
        _ => func.is_deterministic(),
    }
}

// SQLite allows date/time functions in expression indexes only when they do not
// depend on the current clock or local timezone.
fn is_deterministic_datetime_index_call(func: &Func, args: &[Box<Expr>]) -> bool {
    match func {
        Func::Scalar(ScalarFunc::Date)
        | Func::Scalar(ScalarFunc::Time)
        | Func::Scalar(ScalarFunc::DateTime)
        | Func::Scalar(ScalarFunc::UnixEpoch)
        | Func::Scalar(ScalarFunc::JulianDay) => {
            !args.is_empty()
                && !is_current_time_expr(args[0].as_ref())
                && !args[1..]
                    .iter()
                    .any(|arg| is_unsafe_datetime_modifier(arg.as_ref()))
        }
        Func::Scalar(ScalarFunc::StrfTime) => {
            args.len() >= 2
                && !is_current_time_expr(args[1].as_ref())
                && !args[2..]
                    .iter()
                    .any(|arg| is_unsafe_datetime_modifier(arg.as_ref()))
        }
        Func::Scalar(ScalarFunc::TimeDiff) => {
            !args.iter().any(|arg| is_current_time_expr(arg.as_ref()))
        }
        _ => unreachable!("non-datetime function passed to datetime index validator"),
    }
}

fn is_current_time_expr(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Literal(ast::Literal::String(value)) if string_literal_eq(value, "now")
    ) || matches!(
        expr,
        Expr::Literal(
            ast::Literal::CurrentDate | ast::Literal::CurrentTime | ast::Literal::CurrentTimestamp
        )
    )
}

fn is_unsafe_datetime_modifier(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Literal(ast::Literal::String(value))
            if string_literal_eq(value, "localtime") || string_literal_eq(value, "utc")
    ) || is_current_time_expr(expr)
}

fn string_literal_eq(value: &str, expected: &str) -> bool {
    value.trim_matches('\'').eq_ignore_ascii_case(expected)
}

fn emit_index_column_value_from_cursor(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    table_references: &mut TableReferences,
    table_cursor_id: usize,
    idx_col: &IndexColumn,
    dest_reg: usize,
) -> crate::Result<()> {
    if let Some(expr) = &idx_col.expr {
        let mut expr = expr.as_ref().clone();
        bind_and_rewrite_expr(
            &mut expr,
            Some(table_references),
            None,
            resolver,
            BindingBehavior::ResultColumnsNotAllowed,
        )?;
        let self_table_context =
            table_references
                .joined_tables()
                .first()
                .map(|jt| SelfTableContext::ForSelect {
                    table_ref_id: jt.internal_id,
                    referenced_tables: table_references.clone(),
                });
        resolver.with_self_table_context(program, self_table_context.as_ref(), |program, _| {
            translate_expr(program, Some(table_references), &expr, dest_reg, resolver)?;
            Ok(())
        })?;
    } else {
        program.emit_column_or_rowid(table_cursor_id, idx_col.pos_in_table, dest_reg);
    }
    Ok(())
}

pub fn resolve_index_method_parameters(
    parameters: Vec<(turso_parser::ast::Name, Box<Expr>)>,
) -> crate::Result<HashMap<String, crate::Value>> {
    let mut resolved = HashMap::default();
    for (key, value) in parameters {
        let value = match *value {
            Expr::Literal(literal) => match literal {
                ast::Literal::Numeric(s) => match Numeric::from(s) {
                    Numeric::Integer(v) => crate::Value::Numeric(Numeric::Integer(v)),
                    Numeric::Float(v) => crate::Value::Numeric(Numeric::Float(v)),
                },
                ast::Literal::Null => crate::Value::Null,
                ast::Literal::String(s) => crate::Value::Text(s.into()),
                ast::Literal::Blob(b) => crate::Value::Blob(
                    b.as_bytes()
                        .chunks_exact(2)
                        .map(|pair| {
                            // We assume that sqlite3-parser has already validated that
                            // the input is valid hex string, thus unwrap is safe.
                            let hex_byte = std::str::from_utf8(pair).unwrap();
                            u8::from_str_radix(hex_byte, 16).unwrap()
                        })
                        .collect(),
                ),
                _ => bail_parse_error!("parameters must be constant literals"),
            },
            _ => bail_parse_error!("parameters must be constant literals"),
        };
        resolved.insert(key.as_str().to_string(), value);
    }
    Ok(resolved)
}

pub fn translate_drop_index(
    qualified_name: &QualifiedName,
    resolver: &Resolver,
    if_exists: bool,
    program: &mut ProgramBuilder,
) -> crate::Result<()> {
    let database_id = resolver.resolve_existing_index_database_id(qualified_name)?;
    let idx_name = normalize_ident(qualified_name.name.as_str());
    let opts = ProgramBuilderOpts::new(5, 40, 5);
    program.extend(&opts);

    let schema_cookie = resolver.with_schema(database_id, |s| s.schema_version);
    program.begin_write_on_database(database_id, schema_cookie);

    // Find the index in Schema
    let mut maybe_index = None;
    let indexes: Vec<_> = resolver.with_schema(database_id, |s| {
        s.indexes
            .values()
            .flat_map(|v| v.iter())
            .filter(|idx| idx.name == idx_name)
            .cloned()
            .collect()
    });
    if let Some(idx) = indexes.first() {
        maybe_index = Some(idx.clone());
    }

    // If there's no index if_exist is true,
    // then return normaly, otherwise show an error.
    if maybe_index.is_none() {
        if if_exists {
            return Ok(());
        } else {
            return Err(crate::error::LimboError::InvalidArgument(format!(
                "No such index: {}",
                &idx_name
            )));
        }
    }
    // Return an error if the index is an auto-generated constraint-backing index.
    // User-created UNIQUE indexes (via CREATE UNIQUE INDEX) are allowed to be dropped.
    if let Some(ref idx) = maybe_index {
        if idx
            .name
            .starts_with(PRIMARY_KEY_AUTOMATIC_INDEX_NAME_PREFIX)
        {
            return Err(crate::error::LimboError::InvalidArgument(
                "index associated with UNIQUE or PRIMARY KEY constraint cannot be dropped"
                    .to_string(),
            ));
        }
    }

    let cdc_table = prepare_cdc_if_necessary(program, resolver.schema(), SQLITE_TABLEID)?;

    // According to sqlite should emit Null instruction
    // but why?
    let null_reg = program.alloc_register();
    program.emit_null(null_reg, None);

    // String8; r[3] = 'some idx name'
    let index_name_reg = program.emit_string8_new_reg(idx_name);
    // String8; r[4] = 'index'
    let index_str_reg = program.emit_string8_new_reg("index".to_string());

    // for r[5]=rowid
    let row_id_reg = program.alloc_register();

    // We're going to use this cursor to search through sqlite_schema
    let sqlite_table = resolver.schema().get_btree_table(SQLITE_TABLEID).unwrap();
    let sqlite_schema_cursor_id =
        program.alloc_cursor_id(CursorType::BTreeTable(sqlite_table.clone()));

    // Open sqlite_schema for writing
    program.emit_insn(Insn::OpenWrite {
        cursor_id: sqlite_schema_cursor_id,
        root_page: RegisterOrLiteral::Literal(sqlite_table.root_page),
        db: database_id,
    });

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
    program.preassign_label_to_next_insn(label_once_end);

    if let Some((cdc_cursor_id, _)) = cdc_table {
        let before_record_reg = if program.capture_data_changes_info().has_before() {
            Some(emit_cdc_full_record(
                program,
                sqlite_table.columns(),
                sqlite_schema_cursor_id,
                row_id_reg,
                sqlite_table.is_strict,
            ))
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
    });

    program.preassign_label_to_next_insn(loop_end_label);
    if let Some((cdc_cursor_id, _)) = cdc_table {
        emit_cdc_autocommit_commit(program, resolver, cdc_cursor_id)?;
    }

    let current_schema_version = resolver.with_schema(database_id, |s| s.schema_version);
    program.emit_insn(Insn::SetCookie {
        db: database_id,
        cookie: Cookie::SchemaVersion,
        value: current_schema_version as i32 + 1,
        p5: 0,
    });

    let index = maybe_index.unwrap();
    if index.index_method.is_some() && !index.is_backing_btree_index() {
        let cursor_id = program.alloc_cursor_index(None, &index)?;
        program.emit_insn(Insn::IndexMethodDestroy {
            db: database_id,
            cursor_id,
        });
    } else {
        // Destroy index btree
        program.emit_insn(Insn::Destroy {
            db: database_id,
            root: index.root_page,
            former_root_reg: 0,
            is_temp: 0,
        });
    }

    // Remove from the Schema any mention of the index
    program.emit_insn(Insn::DropIndex {
        index: index.clone(),
        db: database_id,
    });

    Ok(())
}

/// Translate `OPTIMIZE INDEX [idx_name]` statement.
/// If idx_name is provided, optimize that specific index.
/// If idx_name is None, optimize all index method indexes.
pub fn translate_optimize(
    idx_name: Option<ast::QualifiedName>,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
) -> crate::Result<()> {
    if !connection.experimental_index_method_enabled() {
        crate::bail_parse_error!(
            "OPTIMIZE INDEX requires experimental index method feature. Enable with --experimental-index-method flag"
        )
    }

    let opts = ProgramBuilderOpts::new(5, 20, 2);
    program.extend(&opts);

    let mut indexes_to_optimize = Vec::new();

    if let Some(name) = idx_name {
        // Optimize a specific index
        let idx_name = normalize_ident(name.name.as_str());
        let database_id = resolver.resolve_existing_index_database_id(&name)?;
        let mut found = false;

        resolver.with_schema(database_id, |schema| {
            for val in schema.indexes.values() {
                for idx in val {
                    if idx.name == idx_name {
                        if idx.index_method.is_some() && !idx.is_backing_btree_index() {
                            indexes_to_optimize.push((database_id, idx.clone()));
                        } else {
                            // Not an index method index - nothing to optimize
                            tracing::debug!(
                                "OPTIMIZE INDEX: {} is not an index method index, nothing to optimize",
                                idx_name
                            );
                        }
                        found = true;
                        break;
                    }
                }
                if found {
                    break;
                }
            }
        });

        if !found {
            return Err(LimboError::InvalidArgument(format!(
                "No such index: {idx_name}"
            )));
        }
    } else {
        // Optimize all index method indexes across all visible databases.
        for (database_id, _, _) in connection.list_all_databases() {
            resolver.with_schema(database_id, |schema| {
                for val in schema.indexes.values() {
                    for idx in val {
                        if idx.index_method.is_some() && !idx.is_backing_btree_index() {
                            indexes_to_optimize.push((database_id, idx.clone()));
                        }
                    }
                }
            });
        }

        if indexes_to_optimize.is_empty() {
            tracing::debug!("OPTIMIZE INDEX: no index method indexes found to optimize");
            return Ok(());
        }
    }

    // Emit optimize instructions for each index method index
    for (database_id, idx) in &indexes_to_optimize {
        let cursor_id = program.alloc_cursor_index(None, idx)?;
        program.emit_insn(Insn::IndexMethodOptimize {
            db: *database_id,
            cursor_id,
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::canonical_create_index_sql;
    use turso_parser::{ast, parser::Parser};

    #[test]
    fn canonical_create_index_sql_drops_schema_qualifier_from_index_name_only() {
        let mut parser = Parser::new(b"CREATE INDEX aux.idx1 ON t1(name);");
        let stmt = match parser.next_cmd().unwrap().unwrap() {
            ast::Cmd::Stmt(stmt) => stmt,
            other => panic!("expected statement, got {other:?}"),
        };

        // let's confirm that the stmt does carry `aux`
        let ast::Stmt::CreateIndex { idx_name, .. } = &stmt else {
            panic!("expected CREATE INDEX statement");
        };
        assert_eq!(
            idx_name
                .db_name
                .as_ref()
                .map(ToString::to_string)
                .as_deref(),
            Some("aux")
        );

        let canonical_sql = canonical_create_index_sql(&stmt);
        assert_eq!(canonical_sql, "CREATE INDEX idx1 ON t1 (name)");
    }
}
