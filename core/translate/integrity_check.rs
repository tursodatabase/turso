use crate::alloc::Arc;
use crate::schema::Column;
use crate::translate::expr::emit_table_column;
use crate::vdbe::affinity::Affinity;
use crate::vdbe::builder::SelfTableContext;
use crate::{
    schema::{BTreeTable, GeneratedType, Index, Schema, Table, EXPR_INDEX_SENTINEL},
    translate::{
        emitter::Resolver,
        expr::{
            bind_and_rewrite_expr, translate_condition_expr, translate_expr_no_constant_opt,
            BindingBehavior, ConditionMetadata, NoConstantOptReason,
        },
        plan::{ColumnUsedMask, IterationDirection, JoinedTable, Operation, Scan, TableReferences},
    },
    vdbe::{
        builder::{CursorKey, CursorType, ProgramBuilder},
        insn::{CmpInsFlags, Insn, IntegrityCkData},
    },
    HashSet,
};
use turso_parser::ast;

/// Maximum number of errors to report with integrity check. If we exceed this number we will
/// short circuit the procedure and return early to not waste time. SQLite uses 100 as default.
pub const MAX_INTEGRITY_CHECK_ERRORS: usize = 100;

enum BoundIndexColumn {
    Column(usize),
    /// The affiniy is `Some` when the index column refers to a virtual generated column
    /// (the affinity is the column's declared type).
    Expr(Box<ast::Expr>, Option<Affinity>),
}

struct BoundIntegrityIndex {
    index: crate::sync::Arc<Index>,
    cursor_id: usize,
    expected_count_reg: usize,
    where_expr: Option<ast::Expr>,
    columns: Vec<BoundIndexColumn>,
    unique_nullable: Vec<bool>,
}

/// Translate PRAGMA integrity_check.
pub fn translate_integrity_check(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    database_id: usize,
    max_errors: usize,
    connection: &crate::Connection,
) -> crate::Result<()> {
    translate_integrity_check_impl(
        program,
        resolver,
        database_id,
        max_errors,
        false,
        connection,
    )
}

/// Translate PRAGMA quick_check.
pub fn translate_quick_check(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    database_id: usize,
    max_errors: usize,
    connection: &crate::Connection,
) -> crate::Result<()> {
    translate_integrity_check_impl(program, resolver, database_id, max_errors, true, connection)
}

fn translate_integrity_check_impl(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    database_id: usize,
    max_errors: usize,
    quick: bool,
    connection: &crate::Connection,
) -> crate::Result<()> {
    match connection.mv_store_for_db(database_id) {
        Some(mv_store) => {
            // Integrity checks read the target's physical file. Its root pages match the
            // shared MVCC schema, not a connection's potentially older transaction snapshot.
            let schema = connection.clone_shared_schema(database_id);
            translate_integrity_check_for_schema(
                &schema,
                program,
                resolver,
                database_id,
                max_errors,
                quick,
                Some(mv_store.as_ref()),
            )
        }
        None => resolver.with_schema(database_id, |schema| {
            translate_integrity_check_for_schema(
                schema,
                program,
                resolver,
                database_id,
                max_errors,
                quick,
                None,
            )
        }),
    }
}

fn emit_integrity_result_row(program: &mut ProgramBuilder, registers: &Registers) {
    program.emit_int(1, registers.had_error);
    program.emit_result_row(registers.message, 1);

    let continue_label = program.allocate_label();
    program.emit_insn(Insn::IfPos {
        reg: registers.remaining_errors,
        target_pc: continue_label,
        decrement_by: 1,
    });
    program.emit_insn(Insn::Halt {
        err_code: 0,
        on_error: None,
        description_reg: None,
        description: String::new(),
    });
    program.preassign_label_to_next_insn(continue_label);
}

fn emit_row_missing_from_index_error(
    program: &mut ProgramBuilder,
    registers: &Registers,
    row_number_reg: usize,
    index_name: &str,
) {
    program.emit_string8("row ".to_string(), registers.message);
    program.emit_insn(Insn::Concat {
        lhs: registers.message,
        rhs: row_number_reg,
        dest: registers.message,
    });
    program.emit_string8(" missing from index ".to_string(), registers.scratch);
    program.emit_insn(Insn::Concat {
        lhs: registers.message,
        rhs: registers.scratch,
        dest: registers.message,
    });
    program.emit_string8(index_name.to_string(), registers.scratch);
    program.emit_insn(Insn::Concat {
        lhs: registers.message,
        rhs: registers.scratch,
        dest: registers.message,
    });
    emit_integrity_result_row(program, registers);
}

fn bind_expr_for_table(
    expr: &ast::Expr,
    table_references: &mut TableReferences,
    resolver: &Resolver,
) -> crate::Result<ast::Expr> {
    let mut out = expr.clone();
    bind_and_rewrite_expr(
        &mut out,
        Some(table_references),
        None,
        resolver,
        BindingBehavior::ResultColumnsNotAllowed,
    )?;
    Ok(out)
}

fn translate_integrity_check_for_schema(
    schema: &Schema,
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    database_id: usize,
    max_errors: usize,
    quick: bool,
    mv_store: Option<&crate::MvStore>,
) -> crate::Result<()> {
    // 1) Run low-level btree/freelist/overflow verification first. This mirrors
    // SQLite's OP_IntegrityCk front-pass and can already emit corruption errors
    // before any row-by-row semantic checks run.
    let mut root_pages = Vec::with_capacity(schema.tables.len() + schema.indexes.len());
    let mut live_root_pages = HashSet::default();

    // integrity_check verifies the physical file, so a placeholder (negative) root for an
    // object a passive checkpoint has since materialized must be resolved to its real page.
    let resolve_root = |root_page: i64| -> i64 {
        match mv_store {
            Some(mv) => mv.resolve_root_page(root_page),
            None => root_page,
        }
    };

    for table in schema.tables.values() {
        if let Table::BTree(btree_table) = table.as_ref() {
            let table_root = resolve_root(btree_table.root_page);
            if table_root < 0 {
                continue;
            }
            root_pages.push(table_root);
            live_root_pages.insert(table_root);
            if let Some(indexes) = schema.indexes.get(btree_table.name.as_str()) {
                for index in indexes {
                    let index_root = resolve_root(index.root_page);
                    if index_root > 0 {
                        root_pages.push(index_root);
                        live_root_pages.insert(index_root);
                    }
                }
            }
        }
    }

    let passive = mv_store.is_some_and(|mv_store| mv_store.uses_passive_checkpoint());
    let mut dropped_roots = Vec::new();
    for &dropped_root in &schema.dropped_root_pages {
        if live_root_pages.contains(&dropped_root) {
            continue;
        }
        if passive {
            dropped_roots.push(dropped_root);
        } else {
            root_pages.push(dropped_root);
        }
    }

    let registers = Registers::init(program, max_errors);

    program.emit_insn(Insn::IntegrityCk {
        data: Box::new(IntegrityCkData {
            db: database_id,
            max_errors,
            roots: root_pages,
            dropped_roots,
            message_register: registers.message,
        }),
    });

    let no_structural_error_label = program.allocate_label();
    program.emit_insn(Insn::IsNull {
        reg: registers.message,
        target_pc: no_structural_error_label,
    });

    let database_name = resolver
        .get_database_name_by_index(database_id)
        .expect("resolved integrity-check database must still exist");
    program.emit_string8(
        format!("*** in database {database_name} ***\n"),
        registers.scratch,
    );
    program.emit_insn(Insn::Concat {
        lhs: registers.scratch,
        rhs: registers.message,
        dest: registers.message,
    });
    emit_integrity_result_row(program, &registers);
    program.preassign_label_to_next_insn(no_structural_error_label);

    // 2) For each ordinary btree table, scan every row and validate:
    //    - NOT NULL constraints
    //    - CHECK constraints
    //    - index membership/uniqueness (integrity_check only)
    //    - index cardinality cross-checks
    for table in schema.tables.values() {
        let Table::BTree(btree_table) = table.as_ref() else {
            continue;
        };

        if btree_table.root_page <= 0 {
            continue;
        }

        let table_ref_id = program.table_reference_counter.next();
        let table_cursor_id = program.alloc_cursor_id_keyed(
            CursorKey::table(table_ref_id),
            CursorType::BTreeTable(btree_table.clone()),
        );
        program.emit_insn(Insn::OpenRead {
            cursor_id: table_cursor_id,
            root_page: btree_table.root_page,
            db: database_id,
        });

        let mut table_references = TableReferences::new(
            vec![JoinedTable {
                op: Operation::Scan(Scan::BTreeTable {
                    iter_dir: IterationDirection::Forwards,
                    index: None,
                }),
                table: Table::BTree(btree_table.clone()),
                identifier: btree_table.name.clone(),
                internal_id: table_ref_id,
                join_info: None,
                col_used_mask: ColumnUsedMask::default(),
                rowid_referenced: false,
                column_use_counts: Vec::new(),
                expression_index_usages: Vec::new(),
                database_id,
                indexed: None,
                plan_estimate: None,
            }],
            vec![],
        );

        let mut bound_indexes = Vec::new();
        if let Some(indexes) = schema.indexes.get(btree_table.name.as_str()) {
            for index in indexes {
                if index.root_page <= 0 {
                    continue;
                }

                let cursor_id = program.alloc_cursor_index(None, index)?;
                program.emit_insn(Insn::OpenRead {
                    cursor_id,
                    root_page: index.root_page,
                    db: database_id,
                });

                let expected_count_reg = program.alloc_register();
                program.emit_int(0, expected_count_reg);

                let mut where_expr = None;
                if let Some(pred) = index.where_clause.as_deref() {
                    where_expr = Some(bind_expr_for_table(pred, &mut table_references, resolver)?);
                }

                let mut columns = Vec::with_capacity(index.columns.len());
                let mut unique_nullable = Vec::with_capacity(index.columns.len());
                for col in &index.columns {
                    if let Some(expr) = col.expr.as_deref() {
                        let affinity = if col.pos_in_table != EXPR_INDEX_SENTINEL {
                            Some(btree_table.columns()[col.pos_in_table].affinity())
                        } else {
                            // expression indexes don't apply affinity from the basae table
                            None
                        };
                        columns.push(BoundIndexColumn::Expr(
                            Box::new(bind_expr_for_table(expr, &mut table_references, resolver)?),
                            affinity,
                        ));
                        unique_nullable.push(true);
                    } else {
                        columns.push(BoundIndexColumn::Column(col.pos_in_table));
                        unique_nullable.push(!btree_table.columns()[col.pos_in_table].notnull());
                    }
                }

                bound_indexes.push(BoundIntegrityIndex {
                    index: index.clone(),
                    cursor_id,
                    expected_count_reg,
                    where_expr,
                    columns,
                    unique_nullable,
                });
            }
        }

        let mut bound_checks = Vec::with_capacity(btree_table.check_constraints.len());
        for check in &btree_table.check_constraints {
            bound_checks.push(bind_expr_for_table(
                &check.expr,
                &mut table_references,
                resolver,
            )?);
        }

        let row_number_reg = program.alloc_register();
        program.emit_int(0, row_number_reg);

        let table_empty_label = program.allocate_label();
        let loop_start_label = program.allocate_label();

        program.emit_insn(Insn::Rewind {
            cursor_id: table_cursor_id,
            pc_if_empty: table_empty_label,
        });
        program.preassign_label_to_next_insn(loop_start_label);

        program.emit_insn(Insn::AddImm {
            register: row_number_reg,
            value: 1,
        });

        let type_check_table = BTreeTable::type_check_table_ref(btree_table, schema);
        // check for NOT NULL columns, plus all non-IPK columns in strict tables
        let checked_columns = btree_table
            .columns()
            .iter()
            .enumerate()
            .filter(|(_, c)| !c.is_rowid_alias()) // nothing to check on IPK cols
            .filter(|(_, c)| c.notnull() || btree_table.is_strict)
            .map(|(idx, col)| {
                let col_ref = match col.generated_type() {
                    GeneratedType::Virtual { expr, .. } => BoundIndexColumn::Expr(
                        Box::new(bind_expr_for_table(expr, &mut table_references, resolver)?),
                        Some(col.affinity()),
                    ),
                    GeneratedType::NotGenerated => BoundIndexColumn::Column(idx),
                };

                Ok((col_ref, col, &type_check_table.columns()[idx]))
            })
            .collect::<crate::Result<Vec<_>>>()?;

        for (col_ref, col, type_check_col) in &checked_columns {
            let col_reg = emit_column(
                program,
                resolver,
                table_cursor_id,
                &table_references,
                col_ref,
            )?;

            let col_name = col.name.as_deref().unwrap_or("");
            if btree_table.is_strict {
                emit_strict_type_check(
                    program,
                    &registers,
                    btree_table,
                    type_check_col,
                    col_name,
                    col_reg,
                );
            }
            if col.notnull() {
                emit_notnull_check(program, &registers, btree_table, col_name, col_reg);
            }
        }

        for check_expr in &bound_checks {
            let check_ok = program.allocate_label();
            // Evaluate the CHECK expression into a register, then branch.
            // A CHECK constraint passes when the result is TRUE *or* NULL
            // (only explicit FALSE/0 is a violation), so we use
            // jump_if_null: true to treat NULL as passing.
            let check_reg = program.alloc_register();
            translate_expr_no_constant_opt(
                program,
                Some(&table_references),
                check_expr,
                check_reg,
                resolver,
                NoConstantOptReason::RegisterReuse,
            )?;
            program.emit_insn(Insn::If {
                reg: check_reg,
                target_pc: check_ok,
                jump_if_null: true,
            });
            program.emit_string8(
                format!("CHECK constraint failed in {}", btree_table.name),
                registers.message,
            );
            emit_integrity_result_row(program, &registers);
            program.preassign_label_to_next_insn(check_ok);
        }

        for bound_index in &bound_indexes {
            let skip_current_index = program.allocate_label();

            if let Some(where_expr) = bound_index.where_expr.as_ref() {
                let where_failed = skip_current_index;
                let where_true_fallthrough = program.allocate_label();
                translate_condition_expr(
                    program,
                    &table_references,
                    where_expr,
                    ConditionMetadata {
                        // For partial indexes, rows that evaluate predicate to FALSE/NULL
                        // are not part of the index and must be skipped.
                        jump_if_condition_is_true: false,
                        jump_target_when_true: where_true_fallthrough,
                        jump_target_when_false: where_failed,
                        jump_target_when_null: where_failed,
                    },
                    resolver,
                )?;
                program.preassign_label_to_next_insn(where_true_fallthrough);
            }

            // Count rows that are expected to appear in this index. For partial
            // indexes this is only rows where the predicate is true.
            program.emit_insn(Insn::AddImm {
                register: bound_index.expected_count_reg,
                value: 1,
            });

            let key_start_reg = program.alloc_registers(bound_index.columns.len() + 1);
            for (i, col) in bound_index.columns.iter().enumerate() {
                let target = key_start_reg + i;
                match col {
                    BoundIndexColumn::Column(pos) => {
                        emit_table_column(
                            program,
                            table_cursor_id,
                            table_ref_id,
                            &table_references,
                            &btree_table.columns()[*pos],
                            *pos,
                            target,
                            resolver,
                        )?;
                    }
                    BoundIndexColumn::Expr(expr, affinity) => {
                        let self_table_context =
                            table_references.joined_tables().first().map(|jt| {
                                SelfTableContext::ForSelect {
                                    table_ref_id: jt.internal_id,
                                    referenced_tables: table_references.clone(),
                                }
                            });

                        resolver.with_self_table_context(
                            program,
                            self_table_context.as_ref(),
                            |program, _| {
                                translate_expr_no_constant_opt(
                                    program,
                                    Some(&table_references),
                                    expr,
                                    target,
                                    resolver,
                                    NoConstantOptReason::RegisterReuse,
                                )?;
                                Ok(())
                            },
                        )?;
                        if let Some(aff) = affinity {
                            program.emit_column_affinity(target, *aff);
                        }
                    }
                }
            }

            let rowid_reg = key_start_reg + bound_index.columns.len();
            program.emit_insn(Insn::RowId {
                cursor_id: table_cursor_id,
                dest: rowid_reg,
            });

            if !quick {
                let found_label = program.allocate_label();
                // Verify the table row has a matching index entry (key columns + rowid).
                program.emit_insn(Insn::Found {
                    cursor_id: bound_index.cursor_id,
                    target_pc: found_label,
                    record_reg: key_start_reg,
                    num_regs: bound_index.columns.len() + 1,
                });
                //TODO these 3 registers are always used together, need to package them in a struct
                emit_row_missing_from_index_error(
                    program,
                    &registers,
                    row_number_reg,
                    &bound_index.index.name,
                );
                program.preassign_label_to_next_insn(found_label);

                if bound_index.index.unique {
                    // This intentionally runs even after a "missing from index"
                    // report above. SQLite does the same: a single corrupt row
                    // can violate multiple invariants and each should be
                    // independently reportable.
                    //
                    // Uniqueness rule matches SQLite:
                    //   unique key is valid if any key column is NULL, OR
                    //   the next index entry is strictly greater on key columns.
                    let unique_ok = program.allocate_label();
                    for (i, is_nullable) in bound_index.unique_nullable.iter().enumerate() {
                        if *is_nullable {
                            program.emit_insn(Insn::IsNull {
                                reg: key_start_reg + i,
                                target_pc: unique_ok,
                            });
                        }
                    }

                    let next_exists = program.allocate_label();
                    program.emit_insn(Insn::Next {
                        cursor_id: bound_index.cursor_id,
                        pc_if_next: next_exists,
                        fullscan: false,
                        is_index: false,
                    });
                    program.emit_insn(Insn::Goto {
                        target_pc: unique_ok,
                    });
                    program.preassign_label_to_next_insn(next_exists);

                    program.emit_insn(Insn::IdxGT {
                        cursor_id: bound_index.cursor_id,
                        start_reg: key_start_reg,
                        num_regs: bound_index.columns.len(),
                        target_pc: unique_ok,
                    });
                    program.emit_string8(
                        format!("non-unique entry in index {}", bound_index.index.name),
                        registers.message,
                    );
                    emit_integrity_result_row(program, &registers);
                    program.preassign_label_to_next_insn(unique_ok);
                }
            }
            program.preassign_label_to_next_insn(skip_current_index);
        }

        program.emit_insn(Insn::Next {
            cursor_id: table_cursor_id,
            pc_if_next: loop_start_label,
            fullscan: false,
            is_index: false,
        });
        program.preassign_label_to_next_insn(table_empty_label);

        for bound_index in &bound_indexes {
            // An index method's backing B-tree stores its data in the index
            // alone; the owning table has zero rows by construction, so the
            // entry-count comparison below would report every healthy FTS
            // database as corrupt. Its pages are still visited above.
            if bound_index.index.is_backing_btree_index() {
                program.emit_insn(Insn::Close {
                    cursor_id: bound_index.cursor_id,
                });
                continue;
            }
            if bound_index.where_expr.is_none() {
                let actual_count_reg = program.alloc_register();
                program.emit_insn(Insn::Count {
                    cursor_id: bound_index.cursor_id,
                    target_reg: actual_count_reg,
                    exact: true,
                });

                let counts_match = program.allocate_label();
                program.emit_insn(Insn::Eq {
                    lhs: actual_count_reg,
                    rhs: bound_index.expected_count_reg,
                    target_pc: counts_match,
                    flags: CmpInsFlags::default(),
                    collation: None,
                });
                program.emit_string8(
                    format!("wrong # of entries in index {}", bound_index.index.name),
                    registers.message,
                );
                emit_integrity_result_row(program, &registers);
                program.preassign_label_to_next_insn(counts_match);
            }

            program.emit_insn(Insn::Close {
                cursor_id: bound_index.cursor_id,
            });
        }

        program.emit_insn(Insn::Close {
            cursor_id: table_cursor_id,
        });
    }

    let has_errors_label = program.allocate_label();
    program.emit_insn(Insn::If {
        reg: registers.had_error,
        target_pc: has_errors_label,
        jump_if_null: false,
    });
    program.emit_string8("ok".to_string(), registers.message);
    program.emit_result_row(registers.message, 1);
    program.preassign_label_to_next_insn(has_errors_label);

    let column_name = if quick {
        "quick_check"
    } else {
        "integrity_check"
    };
    program.add_pragma_result_column(column_name.into());

    Ok(())
}

struct Registers {
    message: usize,
    scratch: usize,
    had_error: usize,
    remaining_errors: usize,
}

impl Registers {
    fn init(program: &mut ProgramBuilder, max_errors: usize) -> Self {
        let remaining_errors = program.alloc_register();
        let had_error = program.alloc_register();
        let message = program.alloc_register();
        let scratch = program.alloc_register();

        program.emit_int(max_errors.saturating_sub(1) as i64, remaining_errors);
        program.emit_int(0, had_error);

        Self {
            message,
            scratch,
            had_error,
            remaining_errors,
        }
    }
}

fn emit_strict_type_check(
    program: &mut ProgramBuilder,
    registers: &Registers,
    btree_table: &Arc<BTreeTable>,
    type_check_col: &Column,
    col_name: &str,
    col_reg: usize,
) {
    let Some(value_type) = type_check_col.strict_value_type() else {
        return;
    };

    let type_ok = program.allocate_label();
    program.emit_insn(Insn::IsType {
        reg: col_reg,
        target_pc: type_ok,
        value_type,
    });
    program.emit_string8(
        format!(
            "non-{} value in {}.{}",
            type_check_col.ty_str.to_ascii_uppercase(),
            btree_table.name,
            col_name
        ),
        registers.message,
    );
    emit_integrity_result_row(program, registers);
    program.preassign_label_to_next_insn(type_ok);
}

fn emit_notnull_check(
    program: &mut ProgramBuilder,
    registers: &Registers,
    btree_table: &Arc<BTreeTable>,
    col_name: &str,
    col_reg: usize,
) {
    let not_null_ok = program.allocate_label();
    program.emit_insn(Insn::NotNull {
        reg: col_reg,
        target_pc: not_null_ok,
    });
    program.emit_string8(
        format!("NULL value in {}.{}", btree_table.name, col_name),
        registers.message,
    );
    emit_integrity_result_row(program, registers);
    program.preassign_label_to_next_insn(not_null_ok);
}

/// Returns the register containing the column
fn emit_column(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    table_cursor_id: usize,
    table_references: &TableReferences,
    col_ref: &BoundIndexColumn,
) -> crate::Result<usize> {
    let col_value_reg = program.alloc_register();
    match col_ref {
        BoundIndexColumn::Column(idx) => {
            program.emit_column_or_rowid(table_cursor_id, *idx, col_value_reg);
        }
        BoundIndexColumn::Expr(expr, affinity) => {
            let self_table_context =
                table_references
                    .joined_tables()
                    .first()
                    .map(|jt| SelfTableContext::ForSelect {
                        table_ref_id: jt.internal_id,
                        referenced_tables: table_references.clone(),
                    });
            resolver.with_self_table_context(
                program,
                self_table_context.as_ref(),
                |program, _| {
                    translate_expr_no_constant_opt(
                        program,
                        Some(table_references),
                        expr,
                        col_value_reg,
                        resolver,
                        NoConstantOptReason::RegisterReuse,
                    )?;
                    if let Some(affinity) = affinity {
                        program.emit_column_affinity(col_value_reg, *affinity);
                    }
                    Ok(())
                },
            )?;
        }
    }
    Ok(col_value_reg)
}
