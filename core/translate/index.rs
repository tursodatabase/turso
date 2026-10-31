use crate::alloc::{TryClone, TursoIteratorExt, TursoVecExt};
use crate::error::SQLITE_CONSTRAINT_UNIQUE;
use crate::function::Func;
use crate::index_method::IndexMethodConfiguration;
use crate::namespace::{is_reserved_object_name, NamespaceId, NamespaceRef};
use crate::numeric::Numeric;
use crate::schema::{
    Column, GeneratedType, Schema, Table, EXPR_INDEX_SENTINEL, RESERVED_TABLE_PREFIXES,
};
use crate::sync::Arc;
use crate::translate::{
    catalog::CatalogContext,
    collate::{CollationSeq, TextCollation},
    emitter::{
        emit_cdc_autocommit_commit, emit_cdc_full_record, emit_cdc_insns, prepare_cdc_if_necessary,
        OperationMode, Resolver,
    },
    expr::expr_type::{type_of, type_ordering, TypeEnv},
    expr::{
        bind_and_rewrite_expr, emit_stored_column, translate_condition_expr, translate_expr,
        unwrap_parens, walk_expr, walk_expr_mut, BindingBehavior, ConditionMetadata, WalkControl,
    },
    insert::format_unique_violation_desc,
    plan::{ColumnUsedMask, IterationDirection, JoinedTable, Operation, Scan, TableReferences},
    planner::ROWID_STRS,
};
use crate::type_ordering::TypeOrdering;
use crate::vdbe::builder::{CursorKey, ProgramBuilderOpts, SelfTableContext};
use crate::vdbe::insn::{to_u32, ClearBtreeCount, CmpInsFlags, Cookie};
use crate::{
    bail_parse_error, CaptureDataChangesExt, LimboError, SymbolTable, MAIN_DB_ID, TEMP_DB_ID,
};
use crate::{
    schema::{
        is_deterministic_schema_function_call, BTreeTable, Index, IndexColumn, PseudoCursorType,
        SchemaObjectType,
    },
    storage::pager::CreateBTreeFlags,
    util::{escape_sql_string_literal, normalize_ident, PRIMARY_KEY_AUTOMATIC_INDEX_NAME_PREFIX},
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::{IdxInsertFlags, Insn, RegisterOrLiteral, SorterOpenData},
    },
};
use rustc_hash::FxHashMap as HashMap;
use turso_parser::ast::{self, Expr, QualifiedName, SortOrder, SortedColumn};

use super::schema::{emit_schema_entry, SchemaEntryType, SQLITE_TABLEID};

fn canonical_create_index_sql(
    stmt: &ast::Stmt,
    idx_name_in_sql: ast::Name,
    tbl_name_in_sql: ast::Name,
) -> String {
    let mut canonical_stmt = stmt.clone();
    let ast::Stmt::CreateIndex {
        idx_name, tbl_name, ..
    } = &mut canonical_stmt
    else {
        unreachable!("canonical_create_index_sql requires a CREATE INDEX statement");
    };
    // Attached db's index names may have a connection-local schema alias like `aux.idx1`.
    // Strip that qualifier before storing SQL in sqlite_schema so the schema text remains valid
    // when the same database is reopened or attached under a different alias.
    // e.g. CREATE INDEX aux.idx1 ON t1 (name) -> CREATE INDEX idx1 ON t1 (name)
    idx_name.namespace = None;
    idx_name.name = idx_name_in_sql;
    *tbl_name = tbl_name_in_sql;
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
    if tbl_name.eq_ignore_ascii_case("sqlite_sequence") {
        crate::bail_parse_error!("table sqlite_sequence may not be indexed");
    }
    if (is_reserved_object_name(connection.dialect().supports_namespaces(), idx_name)
        || RESERVED_TABLE_PREFIXES
            .iter()
            .any(|prefix| tbl_name.starts_with(prefix)))
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
    let written_stmt = stmt.clone();
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

    let mut original_idx_name = idx_name;
    let mut original_tbl_name = tbl_name;
    let namespace = match stored_index_of_namespace(
        connection,
        resolver,
        &original_idx_name,
        &original_tbl_name,
    ) {
        Some((namespace, bare_idx_name, bare_tbl_name)) => {
            original_idx_name.name = bare_idx_name;
            original_tbl_name = bare_tbl_name;
            namespace
        }
        None if original_idx_name.namespace.is_some() => {
            resolver.resolve_namespace(&original_idx_name)?
        }
        None => resolver.resolve_existing_table_named(original_tbl_name.as_str())?,
    };
    let database_id = namespace.database_id;
    let sql = canonical_create_index_sql(
        &written_stmt,
        resolver.name_in_sql(namespace, &original_idx_name.name),
        resolver.name_in_sql(namespace, &original_tbl_name),
    );
    let idx_name = normalize_ident(original_idx_name.name.as_str());
    let tbl_name = normalize_ident(original_tbl_name.as_str());

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
    program.begin_write_on_database(database_id, schema_cookie)?;

    // Check if the index is being created on a valid btree table and
    // the name is globally unique in the schema.
    let schema_unique = resolver.with_schema(database_id, |s| {
        s.is_unique_idx_name(namespace.id, &idx_name)
    });
    if !schema_unique {
        // If IF NOT EXISTS is specified, silently return without error
        if if_not_exists {
            return Ok(());
        }
        crate::bail_parse_error!("index {} already exists", original_idx_name.name.as_str());
    }
    // Materialized views store their rows in a real table btree (so plain
    // reads can use get_table() below like any other table), but writes to
    // that btree go through the DBSP maintenance path (sink.rs), which emits
    // raw Insert/Delete and has no notion of secondary indexes. An index
    // created here would never be kept up to date by later changes to the
    // view, silently going stale. Reject it the same way a plain view is
    // rejected, rather than letting the engine look up a corrupt stale
    // index on later reads.
    if resolver.with_schema(database_id, |s| {
        s.get_view(namespace.id, &tbl_name).is_some()
            || s.is_materialized_view(namespace.id, &tbl_name)
    }) {
        crate::bail_parse_error!("views may not be indexed");
    }
    let db_name = resolver
        .namespace_name(database_id)
        .unwrap_or_else(|| "main".to_string());
    let table = resolver.with_schema(database_id, |s| s.get_table(namespace.id, &tbl_name));
    let Some(table) = table else {
        // The index's target table always lives in the index's own database,
        // so SQLite qualifies the missing table with that database name.
        crate::bail_parse_error!("no such table: {}.{}", db_name, original_tbl_name.as_str());
    };
    let Some(tbl) = table.btree() else {
        crate::bail_parse_error!("virtual tables may not be indexed");
    };
    if !tbl.has_rowid {
        bail_parse_error!("CREATE INDEX on WITHOUT ROWID tables is not supported");
    }
    // Not checked when the schema loads or when a nested statement such as VACUUM
    // replays the stored schema, so databases that already have such an index keep
    // working.
    let reject_qualified_columns_in_db = (!resolver
        .catalog
        .dialect
        .allows_qualified_columns_in_index_terms()
        && !connection.is_nested_stmt())
    .then_some(db_name.as_str());
    let columns = resolver.with_schema(database_id, |schema| {
        resolve_sorted_columns_with_catalog(
            &tbl,
            &columns,
            Some(&resolver.catalog),
            reject_qualified_columns_in_db,
            schema,
            resolver.catalog.symbol_table,
        )
    })?;
    let mut where_clause = where_clause;
    if let Some(where_clause) = &mut where_clause {
        double_quoted_names_to_strings(where_clause, &tbl, true)?;
    }

    // Block CREATE INDEX on STRUCT/UNION columns and on types that can't be sorted
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
                    if column.type_ordering().is_none() && !type_def.is_sortable() {
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
        let index_modules = &resolver.catalog.symbol_table.index_methods;
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
                columns: columns.try_clone()?,
                parameters,
            })?);
        }
    }
    let idx = Arc::new(Index {
        name: idx_name.clone(),
        table_name: tbl.name.clone(),
        root_page: 0, //  we dont have access till its created, after we parse the schema table
        columns,
        unique,
        ephemeral: false,
        has_rowid: tbl.has_rowid,
        // store the *original* where clause, because we need to rewrite it
        // before translating, and it cannot reference a table alias
        where_clause: where_clause.clone(),
        index_method: index_method.clone(),
        on_conflict: None,
        constraint_name: None,
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

    let sqlite_table = resolver
        .schema()
        .get_btree_table(NamespaceId::ROOT, SQLITE_TABLEID)
        .unwrap();
    let sqlite_schema_cursor_id =
        program.alloc_cursor_id(CursorType::BTreeTable(sqlite_table.clone()));

    // Create a new B-Tree and store the root page index in a register
    let root_page_reg = program.alloc_register();
    let index_cursor_id = program.alloc_cursor_index(None, &idx)?;
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
    let cdc_table = prepare_cdc_if_necessary(
        program,
        resolver.schema(),
        database_id,
        Some(SQLITE_TABLEID),
    )?;
    emit_schema_entry(
        program,
        resolver,
        sqlite_schema_cursor_id,
        cdc_table.map(|x| x.0),
        SchemaEntryType::Index,
        &resolver.mangle_name(namespace, &idx_name),
        &resolver.mangle_name(namespace, &tbl_name),
        root_page_reg,
        Some(sql),
    )?;

    emit_refill_index(
        program,
        resolver,
        namespace,
        &tbl,
        &idx,
        index_cursor_id,
        RegisterOrLiteral::Register(root_page_reg),
        None,
    )?;

    let current_schema_version = resolver.with_schema(database_id, |s| s.schema_version);
    program.emit_insn(Insn::SetCookie {
        db: database_id,
        cookie: Cookie::SchemaVersion,
        value: current_schema_version as i32 + 1,
        p5: 0,
    });
    // Parse the schema table to get the index root page and add new index to Schema
    let escaped_idx_name = escape_sql_string_literal(&resolver.mangle_name(namespace, &idx_name));
    let parse_schema_where_clause = format!("name = '{escaped_idx_name}' AND type = 'index'");
    program.emit_insn(Insn::ParseSchema {
        db: database_id,
        where_clause: Some(parse_schema_where_clause),
        trigger_target_database_id: None,
    });
    // Close the final sqlite_schema cursor
    program.emit_insn(Insn::Close {
        cursor_id: sqlite_schema_cursor_id,
    });

    Ok(())
}

/// The namespace of the index that `idx_name` and `tbl_name` name by their
/// mangled names, with their bare names. The stored text of an index of a
/// table of a namespace names both so, and the engine runs that text again,
/// as VACUUM does when it copies a database.
fn stored_index_of_namespace(
    connection: &Arc<crate::Connection>,
    resolver: &Resolver,
    idx_name: &QualifiedName,
    tbl_name: &ast::Name,
) -> Option<(NamespaceRef, ast::Name, ast::Name)> {
    if !connection.is_nested_stmt() || idx_name.namespace.is_some() {
        return None;
    }
    resolver.with_schema(crate::MAIN_DB_ID, |schema| {
        let (id, bare_tbl_name) = schema.demangle_name(tbl_name.as_str());
        if id.is_root() {
            return None;
        }
        let (_, bare_idx_name) = schema.demangle_name(idx_name.name.as_str());
        Some((
            NamespaceRef {
                database_id: crate::MAIN_DB_ID,
                id,
            },
            ast::Name::exact(bare_idx_name.to_string()),
            ast::Name::exact(bare_tbl_name.to_string()),
        ))
    })
}

/// Emits the bytecode that rebuilds `idx` from a full scan of `tbl`.
///
/// `CREATE INDEX` calls this with a newly allocated root page. `REINDEX` calls it with
/// `clear_existing_root` so the existing b-tree is emptied only after all replacement
/// records have been collected and sorted. Statement journaling is therefore required
/// around REINDEX callers so any later refill error restores the original index b-tree.
#[allow(clippy::too_many_arguments)]
pub(crate) fn emit_refill_index(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    namespace: NamespaceRef,
    tbl: &Arc<BTreeTable>,
    idx: &Arc<Index>,
    index_cursor_id: usize,
    index_root_page: RegisterOrLiteral<i64>,
    clear_existing_root: Option<i64>,
) -> crate::Result<()> {
    let table_ref = program.table_reference_counter.next();
    let table_cursor_id = program.alloc_cursor_id_keyed(
        CursorKey::table(table_ref),
        CursorType::BTreeTable(tbl.clone()),
    );
    let sorter_cursor_id = program.alloc_cursor_id(CursorType::Sorter);
    let pseudo_cursor_id = program.alloc_cursor_id(CursorType::Pseudo(PseudoCursorType {
        column_count: tbl.columns().len(),
    }));
    let columns = &idx.columns;
    let tbl_name = normalize_ident(tbl.name.as_str());

    let mut table_references = TableReferences::new(
        vec![JoinedTable {
            op: Operation::Scan(Scan::BTreeTable {
                iter_dir: IterationDirection::Forwards,
                index: None,
            }),
            unmatched_right_rows_plan: None,
            table: Table::BTree(tbl.clone()),
            identifier: tbl_name.clone(),
            has_alias: false,
            internal_id: table_ref,
            join_info: None,
            col_used_mask: ColumnUsedMask::default(),
            column_use_counts: Vec::new(),
            expression_index_usages: Vec::new(),
            namespace,
            indexed: None,
            plan_estimate: None,
        }],
        vec![],
    );
    let where_clause = idx.bind_where_expr(Some(&mut table_references), resolver)?;

    if idx
        .index_method
        .as_ref()
        .is_some_and(|m| !m.definition().backing_btree)
    {
        program.emit_insn(Insn::OpenRead {
            cursor_id: table_cursor_id,
            root_page: tbl.root_page,
            db: namespace.database_id,
        });

        program.emit_insn(Insn::OpenWrite {
            cursor_id: index_cursor_id,
            root_page: index_root_page,
            db: namespace.database_id,
        });

        let loop_start_label = program.allocate_label();
        let loop_end_label = program.allocate_label();
        program.emit_insn(Insn::Rewind {
            cursor_id: table_cursor_id,
            pc_if_empty: loop_end_label,
        });
        program.preassign_label_to_next_insn(loop_start_label);

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
                tbl,
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
            start_reg: to_u32(start_reg),
            count: to_u32(columns.len() + 1),
            dest_reg: to_u32(record_reg),
            index_name: Some(idx.name.clone()),
            affinity_str: None,
        });

        program.emit_insn(Insn::IdxInsert {
            cursor_id: index_cursor_id,
            record_reg,
            unpacked_start: Some(start_reg),
            unpacked_count: Some((columns.len() + 1) as u32),
            flags: IdxInsertFlags::new().use_seek(false),
        });

        if let Some(skip_row_label) = skip_row_label {
            program.preassign_label_to_next_insn(skip_row_label);
        }
        program.emit_insn(Insn::Next {
            cursor_id: table_cursor_id,
            pc_if_next: loop_start_label,
            fullscan: false,
            is_index: false,
        });
        program.preassign_label_to_next_insn(loop_end_label);
    } else {
        let order_collations_nulls = idx
            .columns
            .iter()
            .map(|c| (c.order, c.collation, c.nulls_order))
            .try_collect()?;
        program.emit_insn(Insn::SorterOpen {
            data: Box::new(SorterOpenData {
                cursor_id: sorter_cursor_id,
                columns: columns.len(),
                order_collations_nulls,
            }),
        });
        // SorterData moves each sorted record into the pseudo cursor's
        // content register; the two must name the same register.
        let content_reg = program.alloc_register();
        program.emit_insn(Insn::OpenPseudo {
            cursor_id: pseudo_cursor_id,
            content_reg,
            num_fields: columns.len() + 1,
        });

        program.emit_insn(Insn::OpenRead {
            cursor_id: table_cursor_id,
            root_page: tbl.root_page,
            db: namespace.database_id,
        });

        let loop_start_label = program.allocate_label();
        let loop_end_label = program.allocate_label();
        program.emit_insn(Insn::Rewind {
            cursor_id: table_cursor_id,
            pc_if_empty: loop_end_label,
        });
        program.preassign_label_to_next_insn(loop_start_label);

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
                tbl,
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
            start_reg: to_u32(start_reg),
            count: to_u32(columns.len() + 1),
            dest_reg: to_u32(record_reg),
            index_name: Some(idx.name.clone()),
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
            fullscan: false,
            is_index: false,
        });
        program.preassign_label_to_next_insn(loop_end_label);

        if let Some(root) = clear_existing_root {
            program.emit_insn(Insn::ClearBtree {
                db: namespace.database_id,
                root,
                count: ClearBtreeCount::Nothing,
            });
        }
        program.emit_insn(Insn::OpenWrite {
            cursor_id: index_cursor_id,
            root_page: index_root_page,
            db: namespace.database_id,
        });

        let sorted_loop_start = program.allocate_label();
        let sorted_loop_end = program.allocate_label();

        program.emit_insn(Insn::SorterSort {
            cursor_id: sorter_cursor_id,
            pc_if_empty: sorted_loop_end,
        });

        if idx.unique {
            let goto_label = program.allocate_label();
            let label_after_sorter_compare = program.allocate_label();
            program.preassign_label_to_next_insn(goto_label);
            program.emit_insn(Insn::Goto {
                target_pc: label_after_sorter_compare,
            });
            program.preassign_label_to_next_insn(sorted_loop_start);
            program.emit_insn(Insn::SorterCompare {
                cursor_id: sorter_cursor_id,
                sorted_record_reg: content_reg,
                num_regs: columns.len(),
                pc_when_nonequal: goto_label,
            });
            program.emit_insn(Insn::Halt {
                constraint_info: None,
                err_code: SQLITE_CONSTRAINT_UNIQUE,
                description: format_unique_violation_desc(tbl_name.as_str(), idx),
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
            dest_reg: content_reg,
        });

        program.emit_insn(Insn::SeekEnd {
            cursor_id: index_cursor_id,
        });
        program.emit_insn(Insn::IdxInsert {
            cursor_id: index_cursor_id,
            record_reg: content_reg,
            unpacked_start: None,
            unpacked_count: None,
            flags: IdxInsertFlags::new().use_seek(false),
        });
        program.emit_insn(Insn::SorterNext {
            cursor_id: sorter_cursor_id,
            pc_if_next: sorted_loop_start,
        });
        program.preassign_label_to_next_insn(sorted_loop_end);
    }

    program.close_cursors(&[sorter_cursor_id, table_cursor_id, index_cursor_id]);
    Ok(())
}

type ReindexTarget = (NamespaceRef, Arc<BTreeTable>, Arc<Index>);

/// Translates a SQLite-compatible `REINDEX` statement into bytecode.
///
/// The supported scope is rowid b-tree indexes in non-MVCC mode. Target
/// resolution accepts SQLite's all-index, collation-name, table-name, and
/// index-name forms, then emits a destructive clear-plus-refill program for
/// each resolved index.
pub fn translate_reindex(
    name: Option<QualifiedName>,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
) -> crate::Result<()> {
    if connection.mvcc_enabled() {
        bail_parse_error!("REINDEX is not supported in MVCC mode");
    }

    let opts = ProgramBuilderOpts::new(5, 40, 5);
    program.extend(&opts);

    let targets = resolve_reindex_targets(name.as_ref(), &resolver.catalog, connection)?;
    if targets.is_empty() {
        return Ok(());
    }
    if connection.get_query_only() {
        bail_parse_error!("Cannot execute write statement in query_only mode");
    }

    let mut write_databases = Vec::new();
    for (namespace, _, _) in &targets {
        if !write_databases.contains(&namespace.database_id) {
            let schema_cookie = resolver.with_schema(namespace.database_id, |s| s.schema_version);
            program.begin_write_on_database(namespace.database_id, schema_cookie)?;
            write_databases.push(namespace.database_id);
        }
    }

    for (namespace, table, index) in targets {
        if index
            .index_method
            .as_ref()
            .is_some_and(|m| !m.definition().backing_btree)
        {
            bail_parse_error!(
                "REINDEX is not supported for custom index methods without a backing btree"
            );
        }
        if !table.has_rowid {
            bail_parse_error!("REINDEX on WITHOUT ROWID tables is not supported");
        }
        let index_cursor_id = program.alloc_cursor_index(None, &index)?;
        emit_refill_index(
            program,
            resolver,
            namespace,
            &table,
            &index,
            index_cursor_id,
            RegisterOrLiteral::Literal(index.root_page),
            Some(index.root_page),
        )?;
    }

    Ok(())
}

/// Resolves the optional `REINDEX` target name to concrete `(namespace, table, index)` triples.
///
/// SQLite resolves unqualified targets as collation, then table, then index. Qualified
/// names are schema-local and can only refer to a table or index in that database.
fn resolve_reindex_targets(
    name: Option<&QualifiedName>,
    resolver: &CatalogContext,
    connection: &Arc<crate::Connection>,
) -> crate::Result<Vec<ReindexTarget>> {
    let Some(name) = name else {
        return Ok(collect_all_reindex_targets(resolver, connection));
    };

    let normalized_name = normalize_ident(name.name.as_str());
    if name.namespace.is_none() {
        let collation = CollationSeq::new(&normalized_name).ok();
        if let Some(collation) = collation {
            let targets = collect_reindex_targets_by_collation(resolver, connection, collation);
            if !targets.is_empty()
                || !matches!(collation, CollationSeq::Text(TextCollation::Locale(_)))
            {
                return Ok(targets);
            }
        }
        if let Some(targets) = find_reindex_table(&normalized_name, resolver, connection) {
            return Ok(targets);
        }
        if let Some(target) = find_reindex_index(&normalized_name, resolver, connection) {
            return Ok(vec![target]);
        }
        if collation.is_some() {
            return Ok(Vec::new());
        }
        bail_parse_error!("unable to identify the object to be reindexed");
    }

    let namespace = resolver.resolve_namespace(name)?;
    if let Some(targets) = find_reindex_table_in_namespace(&normalized_name, namespace, resolver) {
        return Ok(targets);
    }
    if let Some(target) =
        find_reindex_index_in_db(&normalized_name, namespace.database_id, resolver)
    {
        return Ok(vec![target]);
    }
    bail_parse_error!("unable to identify the object to be reindexed");
}

/// Returns every reindexable index across temp, main, and attached databases.
fn collect_all_reindex_targets(
    resolver: &CatalogContext,
    connection: &Arc<crate::Connection>,
) -> Vec<ReindexTarget> {
    let mut targets = Vec::new();
    for (database_id, _, _) in connection.list_all_databases() {
        targets.extend(collect_all_reindex_targets_in_db(database_id, resolver));
    }
    targets
}

/// Returns every index across all schemas that contains at least one column using `collation`.
fn collect_reindex_targets_by_collation(
    resolver: &CatalogContext,
    connection: &Arc<crate::Connection>,
    collation: CollationSeq,
) -> Vec<ReindexTarget> {
    let mut targets = Vec::new();
    for (database_id, _, _) in connection.list_all_databases() {
        targets.extend(
            collect_all_reindex_targets_in_db(database_id, resolver)
                .into_iter()
                .filter(|(_, _, index)| index_matches_collation(index, collation)),
        );
    }
    targets
}

/// Returns every reindexable index in one schema database.
fn collect_all_reindex_targets_in_db(
    database_id: usize,
    resolver: &CatalogContext,
) -> Vec<ReindexTarget> {
    resolver.with_schema(database_id, |schema| {
        schema
            .namespace_ids()
            .flat_map(|namespace| {
                schema
                    .indexes(namespace)
                    .map(move |index| (namespace, index))
            })
            .filter_map(|(namespace, index)| {
                schema
                    .get_btree_table(namespace, &index.table_name)
                    .map(|table| {
                        (
                            NamespaceRef {
                                database_id,
                                id: namespace,
                            },
                            table,
                            index.clone(),
                        )
                    })
            })
            .collect()
    })
}

/// Finds the first table matching an unqualified `REINDEX table_name` target.
///
/// Search order follows SQLite name resolution for unqualified schema objects:
/// temp first, main second, then attached databases by numeric database id.
fn find_reindex_table(
    table_name: &str,
    resolver: &CatalogContext,
    connection: &Arc<crate::Connection>,
) -> Option<Vec<ReindexTarget>> {
    for database_id in reindex_unqualified_search_database_ids(connection) {
        if let Some(targets) = find_reindex_table_in_namespace(
            table_name,
            NamespaceRef::root_of(database_id),
            resolver,
        ) {
            return Some(targets);
        }
    }
    None
}

/// Returns all indexes belonging to `table_name` inside a single database.
///
/// A table or view without indexes is still a valid REINDEX target, so this
/// returns an empty vector wrapped in `Some` when the object exists.
fn find_reindex_table_in_namespace(
    table_name: &str,
    namespace: NamespaceRef,
    resolver: &CatalogContext,
) -> Option<Vec<ReindexTarget>> {
    resolver.with_schema(namespace.database_id, |schema| {
        let object_type = schema.get_object_type(namespace.id, table_name)?;
        if object_type == SchemaObjectType::View {
            return Some(Vec::new());
        }
        let SchemaObjectType::Table = object_type else {
            return None;
        };
        let Some(table) = schema.get_btree_table(namespace.id, table_name) else {
            return Some(Vec::new());
        };
        Some(
            schema
                .indices_including_backing(namespace.id, table.name.as_str())
                .cloned()
                .map(|index| (namespace, table.clone(), index))
                .collect(),
        )
    })
}

/// Finds the first index matching an unqualified `REINDEX index_name` target.
///
/// This is tried only after collation and table lookup fail.
fn find_reindex_index(
    index_name: &str,
    resolver: &CatalogContext,
    connection: &Arc<crate::Connection>,
) -> Option<ReindexTarget> {
    for database_id in reindex_unqualified_search_database_ids(connection) {
        if let Some(target) = find_reindex_index_in_db(index_name, database_id, resolver) {
            return Some(target);
        }
    }
    None
}

/// Finds one index by name inside a single database and returns its owning table.
fn find_reindex_index_in_db(
    index_name: &str,
    database_id: usize,
    resolver: &CatalogContext,
) -> Option<ReindexTarget> {
    resolver.with_schema(database_id, |schema| {
        for namespace in schema.namespace_ids() {
            if let Some(index) = schema
                .indexes(namespace)
                .find(|index| index.name.eq_ignore_ascii_case(index_name))
            {
                let table = schema.get_btree_table(namespace, &index.table_name)?;
                return Some((
                    NamespaceRef {
                        database_id,
                        id: namespace,
                    },
                    table,
                    index.clone(),
                ));
            }
        }
        None
    })
}

/// Returns database ids in SQLite's unqualified REINDEX search order.
fn reindex_unqualified_search_database_ids(connection: &Arc<crate::Connection>) -> Vec<usize> {
    let mut database_ids: Vec<_> = connection
        .list_all_databases()
        .into_iter()
        .map(|(database_id, _, _)| database_id)
        .collect();
    database_ids.sort_by_key(|database_id| match *database_id {
        TEMP_DB_ID => 0,
        MAIN_DB_ID => 1,
        _ => *database_id,
    });
    database_ids
}

/// Returns whether any indexed term explicitly uses `collation`.
fn index_matches_collation(index: &Index, collation: CollationSeq) -> bool {
    index
        .columns
        .iter()
        .any(|column| column.collation == collation)
}

pub fn resolve_sorted_columns(
    table: &BTreeTable,
    cols: &[SortedColumn],
    schema: &Schema,
    syms: &SymbolTable,
) -> crate::Result<crate::alloc::Vec<IndexColumn>> {
    resolve_sorted_columns_with_catalog(table, cols, None, None, schema, syms)
}

pub fn reject_explicit_nulls(cols: &[SortedColumn]) -> crate::Result<()> {
    for sc in cols {
        if let Some(nulls) = sc.nulls {
            crate::bail_parse_error!("unsupported use of {}", nulls);
        }
    }
    Ok(())
}

/// The index columns of `cols`. A column, or an expression, whose type has an
/// order is kept in that order, so the index is in the order comparisons on
/// it use; an explicit COLLATE on it is an error.
///
/// When `reject_qualified_columns_in_db` names the index's database, a qualified
/// column name in an indexed term is an error, as in SQLite. The WHERE clause of a
/// partial index is not checked: SQLite allows qualified names there.
fn resolve_sorted_columns_with_catalog(
    table: &BTreeTable,
    cols: &[SortedColumn],
    resolver: Option<&CatalogContext>,
    reject_qualified_columns_in_db: Option<&str>,
    schema: &Schema,
    syms: &SymbolTable,
) -> crate::Result<crate::alloc::Vec<IndexColumn>> {
    let mut resolved =
        <crate::alloc::Vec<_> as crate::alloc::TursoTryWithCapacityExt>::try_with_capacity_ext(
            cols.len(),
        )?;
    for sc in cols {
        let has_qualified_column = match reject_qualified_columns_in_db {
            Some(db_name) => has_qualified_columns(&sc.expr, table, db_name)?,
            None => false,
        };
        let order = sc.order.unwrap_or(SortOrder::Asc);
        let nulls = sc.nulls;
        let (explicit_collation, base_expr) = extract_collation(sc.expr.as_ref(), resolver)?;
        // Unwrap parentheses for column resolution (SQLite treats (('col')) same as 'col')
        let unwrapped_expr = unwrap_parens(base_expr)?;
        let index_column = if let Some((pos, column_name, column)) =
            resolve_index_column(unwrapped_expr, table)
        {
            let collation = index_collation(explicit_collation, column.type_ordering())?
                .unwrap_or_else(|| column.collation());
            let expr = match column.generated_type() {
                GeneratedType::Virtual { expr, .. } => Some(expr.clone()),
                GeneratedType::NotGenerated => None,
            };
            IndexColumn {
                name: column_name,
                order,
                nulls_order: nulls,
                pos_in_table: pos,
                collation,
                explicit_collation,
                default: column.default.clone(),
                expr,
            }
        } else {
            let mut expr = sc.expr.clone();
            double_quoted_names_to_strings(&mut expr, table, false)?;
            if matches!(unwrapped_expr, Expr::Literal(ast::Literal::String(_)))
                || !validate_index_expression(&expr, table)
            {
                crate::bail_parse_error!("Error: invalid expression in CREATE INDEX: {}", sc.expr);
            }
            let mut converted_expr = unwrapped_expr.clone();
            double_quoted_names_to_strings(&mut converted_expr, table, false)?;
            let ordering = expression_type_ordering(&converted_expr, table, schema, syms)?;
            let collation = index_collation(explicit_collation, ordering)?;
            IndexColumn {
                name: expr.to_string(),
                order,
                nulls_order: nulls,
                pos_in_table: EXPR_INDEX_SENTINEL,
                collation: collation.unwrap_or(CollationSeq::BINARY),
                explicit_collation,
                default: None,
                expr: Some(expr),
            }
        };
        // SQLite reports any other error in the same term instead of this one.
        if has_qualified_column {
            crate::bail_parse_error!("the \".\" operator prohibited in index expressions");
        }
        resolved
            .push_within_capacity(index_column)
            .expect("resolved index columns vector was preallocated to cols.len()");
    }
    Ok(resolved)
}

/// Returns whether `expr` contains a qualified column name. A qualified name that
/// does not name a column of `table` is reported as `no such column`, which SQLite
/// reports before any other error in the term.
fn has_qualified_columns(expr: &Expr, table: &BTreeTable, db_name: &str) -> crate::Result<bool> {
    let mut has_qualified_column = false;
    walk_expr(expr, &mut |e: &Expr| -> crate::Result<WalkControl> {
        let (db_qualifier, table_qualifier, column) = match e {
            Expr::Qualified(table_qualifier, column) => (None, table_qualifier, column),
            Expr::DoublyQualified(db_qualifier, table_qualifier, column) => {
                (Some(db_qualifier), table_qualifier, column)
            }
            _ => return Ok(WalkControl::Continue),
        };
        let resolves = db_qualifier.is_none_or(|db| db.as_str().eq_ignore_ascii_case(db_name))
            && table_qualifier.as_str().eq_ignore_ascii_case(&table.name)
            && table.get_column(column.as_str()).is_some();
        if !resolves {
            let table_column = format!("{}.{}", table_qualifier.as_str(), column.as_str());
            match db_qualifier {
                Some(db) => bail_parse_error!("no such column: {}.{table_column}", db.as_str()),
                None => bail_parse_error!("no such column: {table_column}"),
            }
        }
        has_qualified_column = true;
        Ok(WalkControl::Continue)
    })?;
    Ok(has_qualified_column)
}

/// The collation of an index column: the order of its type when it has one,
/// otherwise the explicit COLLATE, if any.
fn index_collation(
    explicit_collation: Option<TextCollation>,
    ordering: Option<TypeOrdering>,
) -> crate::Result<Option<CollationSeq>> {
    match (ordering, explicit_collation) {
        (Some(ordering), Some(_)) => {
            bail_parse_error!("collations are not supported by type {}", ordering.name())
        }
        (Some(ordering), None) => Ok(Some(CollationSeq::Type(ordering))),
        (None, explicit_collation) => Ok(explicit_collation.map(CollationSeq::Text)),
    }
}

/// The order of the type of an index expression over `table`'s columns, such
/// as numeric's order for `n * 2` when n is numeric.
pub(crate) fn expression_type_ordering(
    expr: &Expr,
    table: &BTreeTable,
    schema: &Schema,
    syms: &SymbolTable,
) -> crate::Result<Option<TypeOrdering>> {
    let mut expr = expr.clone();
    crate::schema::resolve_gencol_expr_columns(&mut expr, table.columns())?;
    let ty = type_of(&expr, &TypeEnv::over_table(table, schema, syms));
    Ok(type_ordering(&ty, schema))
}

/// Extracts collation sequence from an expression if it is a Collate expression.
/// Given the example: `col1 COLLATE NOCASE` / Expr::Collation(Expr::Id(col1), CollationSeq)
/// returns (Some(CollationSeq), Expr::Id(col1))
fn extract_collation<'a>(
    expr: &'a Expr,
    resolver: Option<&CatalogContext>,
) -> crate::Result<(Option<TextCollation>, &'a Expr)> {
    let mut current = expr;
    let mut coll = None;
    loop {
        current = unwrap_parens(current)?;
        match current {
            Expr::Collate(inner, seq) => {
                if coll.is_none() {
                    let collation = match resolver {
                        Some(resolver) => resolver.resolve_collation(seq.as_str())?,
                        None => TextCollation::new(seq.as_str())?,
                    };
                    if collation.is_custom() {
                        crate::bail_parse_error!("custom collations are not supported in indexes");
                    }
                    coll = Some(collation);
                }
                current = inner.as_ref();
            }
            _ => return Ok((coll, current)),
        }
    }
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
fn validate_index_expression(expr: &Expr, table: &BTreeTable) -> bool {
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
            .is_ok_and(|f| f.is_some_and(|f| is_deterministic_schema_function_call(&f, args)))
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

/// Like SQLite, reads a double-quoted name that is not a column of the indexed table as a
/// string literal. This is done once, when the index is created or loaded from the schema,
/// so the index does not start reading a column of that name added later.
pub fn double_quoted_names_to_strings(
    expr: &mut Expr,
    table: &BTreeTable,
    rowid_names_are_columns: bool,
) -> crate::Result<bool> {
    let mut changed = false;
    walk_expr_mut(expr, &mut |e: &mut Expr| -> crate::Result<WalkControl> {
        if let Expr::Id(name) = e {
            if is_double_quoted_string(name, table, rowid_names_are_columns) {
                *e = Expr::Literal(ast::Literal::String(name.as_literal()));
                changed = true;
            }
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(changed)
}

fn is_double_quoted_string(
    name: &ast::Name,
    table: &BTreeTable,
    rowid_names_are_columns: bool,
) -> bool {
    name.quoted_with('"')
        && table.get_column(name.as_str()).is_none()
        && !(rowid_names_are_columns
            && table.has_rowid
            && ROWID_STRS
                .iter()
                .any(|rowid| rowid.eq_ignore_ascii_case(name.as_str())))
}

fn emit_index_column_value_from_cursor(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    table_references: &mut TableReferences,
    table_cursor_id: usize,
    table: &BTreeTable,
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
        // For virtual generated column references, apply the column's
        // declared affinity to the computed expression result.
        if idx_col.pos_in_table != EXPR_INDEX_SENTINEL {
            let column = &table.columns()[idx_col.pos_in_table];
            if column.is_virtual_generated() {
                program.emit_column_affinity(dest_reg, column.affinity());
            }
        }
    } else {
        emit_stored_column(
            program,
            table_cursor_id,
            idx_col.pos_in_table,
            dest_reg,
            resolver,
        )?;
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
                ast::Literal::Blob(b) => {
                    crate::Value::Blob(ast::blob_literal_bytes(&b).try_collect()?)
                }
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
    let idx_name = normalize_ident(qualified_name.name.as_str());
    let opts = ProgramBuilderOpts::new(5, 40, 5);
    program.extend(&opts);

    let Some(namespace) = resolver.find_existing_index(qualified_name)? else {
        if if_exists {
            return Ok(());
        }
        return Err(crate::error::LimboError::InvalidArgument(format!(
            "No such index: {}",
            &idx_name
        )));
    };
    let database_id = namespace.database_id;
    let schema_cookie = resolver.with_schema(database_id, |s| s.schema_version);
    program.begin_write_on_database(database_id, schema_cookie)?;

    // Find the index in Schema
    let mut maybe_index = None;
    let indexes: Vec<_> = resolver.with_schema(database_id, |s| {
        s.indexes(namespace.id)
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

    let cdc_table = prepare_cdc_if_necessary(
        program,
        resolver.schema(),
        database_id,
        Some(SQLITE_TABLEID),
    )?;

    // According to sqlite should emit Null instruction
    // but why?
    let null_reg = program.alloc_register();
    program.emit_null(null_reg, None);

    // String8; r[3] = 'some idx name'
    let index_name_reg = program.emit_string8_new_reg(resolver.mangle_name(namespace, &idx_name));
    // String8; r[4] = 'index'
    let index_str_reg = program.emit_string8_new_reg("index".to_string());

    // for r[5]=rowid
    let row_id_reg = program.alloc_register();

    // We're going to use this cursor to search through sqlite_schema
    let sqlite_table = resolver
        .schema()
        .get_btree_table(NamespaceId::ROOT, SQLITE_TABLEID)
        .unwrap();
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
        namespace: namespace.id,
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
        let namespace = resolver.resolve_existing_index(&name)?;
        let database_id = namespace.database_id;
        let mut found = false;

        resolver.with_schema(database_id, |schema| {
            for idx in schema.indexes(namespace.id) {
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
                for namespace in schema.namespace_ids() {
                    for idx in schema.indexes(namespace) {
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
                .namespace
                .as_ref()
                .map(ToString::to_string)
                .as_deref(),
            Some("aux")
        );

        let ast::Stmt::CreateIndex { tbl_name, .. } = &stmt else {
            panic!("expected CREATE INDEX statement");
        };
        let canonical_sql =
            canonical_create_index_sql(&stmt, idx_name.name.clone(), tbl_name.clone());
        assert_eq!(canonical_sql, "CREATE INDEX idx1 ON t1 (name)");
        let stored_sql = canonical_create_index_sql(
            &stmt,
            ast::Name::exact("__turso_ns3_idx1".to_string()),
            ast::Name::exact("__turso_ns3_t1".to_string()),
        );
        assert_eq!(
            stored_sql,
            "CREATE INDEX __turso_ns3_idx1 ON __turso_ns3_t1 (name)"
        );
    }
}
