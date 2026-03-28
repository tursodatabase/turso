use crate::sync::Arc;
use crate::{schema::BTreeTable, turso_assert_eq, turso_assert_ne};
use turso_parser::{
    ast::{self, TableInternalId},
    parser::Parser,
};

use crate::{
    error::SQLITE_CONSTRAINT_CHECK,
    function::{AlterTableFunc, Func},
    schema::{CheckConstraint, Column, ForeignKey, Table, RESERVED_TABLE_PREFIXES},
    translate::{
        emitter::{emit_check_constraints, gencol::compute_virtual_columns, Resolver},
        expr::{translate_expr, walk_expr, walk_expr_mut, WalkControl},
        plan::{ColumnUsedMask, OuterQueryReference, TableReferences},
    },
    util::{
        check_expr_references_column, escape_sql_string_literal, normalize_ident,
        parse_numeric_literal, rewrite_view_sql_for_column_rename,
    },
    vdbe::{
        affinity::Affinity,
        builder::{CursorType, DmlColumnContext, ProgramBuilder},
        insn::{to_u16, CmpInsFlags, Cookie, Insn, RegisterOrLiteral},
    },
    vtab::VirtualTable,
    LimboError, Numeric, Result, Value,
};
use either::Either;

use super::{
    schema::{validate_check_expr, SQLITE_TABLEID},
    update::translate_update_for_schema_change,
};

fn validate(alter_table: &ast::AlterTableBody, table_name: &str) -> Result<()> {
    // Check if someone is trying to ALTER a system table
    if crate::schema::is_system_table(table_name) {
        crate::bail_parse_error!("table {} may not be modified", table_name);
    }
    if let ast::AlterTableBody::RenameTo(new_table_name) = alter_table {
        let normalized_new_name = normalize_ident(new_table_name.as_str());
        if RESERVED_TABLE_PREFIXES
            .iter()
            .any(|prefix| normalized_new_name.starts_with(prefix))
        {
            crate::bail_parse_error!("Object name reserved for internal use: {}", new_table_name);
        }
    }

    Ok(())
}

/// Check if an expression is a valid "constant" default for ALTER TABLE ADD COLUMN.
/// SQLite is very strict here - it only allows:
/// - Literals (numbers, strings, blobs, NULL, CURRENT_TIME/DATE/TIMESTAMP)
/// - Bare identifiers (treated as string literals, e.g., `DEFAULT hello` → "hello")
/// - Signed literals (+5, -5, +NULL, -NULL)
/// - Parenthesized versions of the above
///
/// It does NOT allow:
/// - Binary operations like (5 + 3)
/// - Function calls like COALESCE(NULL, 5)
/// - Comparisons, CASE expressions, CAST, etc.
///
/// Note: CURRENT_TIME/DATE/TIMESTAMP are allowed here but will be rejected at
/// runtime if the table has existing rows (see `default_requires_empty_table`).
fn is_strict_constant_default(expr: &ast::Expr) -> bool {
    match expr {
        ast::Expr::Literal(_) => true,
        // Bare identifiers are treated as string literals in DEFAULT clause
        ast::Expr::Id(_) => true,
        ast::Expr::Unary(ast::UnaryOperator::Positive | ast::UnaryOperator::Negative, inner) => {
            // Only allow unary +/- on literals
            matches!(inner.as_ref(), ast::Expr::Literal(_))
        }
        ast::Expr::Parenthesized(exprs) => {
            // Parenthesized expression with a single inner expression
            exprs.len() == 1 && is_strict_constant_default(&exprs[0])
        }
        _ => false,
    }
}

/// Check if a default expression requires the table to be empty (non-deterministic defaults).
/// CURRENT_TIME, CURRENT_DATE, CURRENT_TIMESTAMP cannot be used to backfill existing rows.
fn default_requires_empty_table(expr: &ast::Expr) -> bool {
    match expr {
        ast::Expr::Literal(lit) => matches!(
            lit,
            ast::Literal::CurrentDate | ast::Literal::CurrentTime | ast::Literal::CurrentTimestamp
        ),
        ast::Expr::Parenthesized(exprs) => {
            exprs.len() == 1 && default_requires_empty_table(&exprs[0])
        }
        _ => false,
    }
}

fn emit_rename_sqlite_sequence_entry(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    connection: &Arc<crate::Connection>,
    database_id: usize,
    old_table_name_norm: &str,
    new_table_name_norm: &str,
) {
    let Some(sqlite_sequence) = resolver.with_schema(database_id, |s| {
        s.get_btree_table(crate::schema::SQLITE_SEQUENCE_TABLE_NAME)
    }) else {
        return;
    };

    let seq_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(sqlite_sequence.clone()));
    let sequence_name_reg = program.alloc_register();
    let sequence_value_reg = program.alloc_register();
    let row_name_to_replace_reg = program.emit_string8_new_reg(old_table_name_norm.to_string());
    program.mark_last_insn_constant();
    let replacement_row_name_reg = program.emit_string8_new_reg(new_table_name_norm.to_string());
    program.mark_last_insn_constant();

    let affinity_str = sqlite_sequence
        .columns
        .iter()
        .map(|col| col.affinity().aff_mask())
        .collect::<String>();

    program.emit_insn(Insn::OpenWrite {
        cursor_id: seq_cursor_id,
        root_page: RegisterOrLiteral::Literal(sqlite_sequence.root_page),
        db: database_id,
    });

    program.cursor_loop(seq_cursor_id, |program, rowid| {
        program.emit_column_or_rowid(seq_cursor_id, 0, sequence_name_reg);

        let continue_loop_label = program.allocate_label();
        program.emit_insn(Insn::Ne {
            lhs: sequence_name_reg,
            rhs: row_name_to_replace_reg,
            target_pc: continue_loop_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });

        program.emit_column_or_rowid(seq_cursor_id, 1, sequence_value_reg);

        let record_start_reg = program.alloc_registers(2);
        let record_reg = program.alloc_register();

        program.emit_insn(Insn::Copy {
            src_reg: replacement_row_name_reg,
            dst_reg: record_start_reg,
            extra_amount: 0,
        });
        program.emit_insn(Insn::Copy {
            src_reg: sequence_value_reg,
            dst_reg: record_start_reg + 1,
            extra_amount: 0,
        });
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u16(record_start_reg),
            count: to_u16(2),
            dest_reg: to_u16(record_reg),
            index_name: None,
            affinity_str: Some(affinity_str.clone()),
        });

        // In MVCC mode, we need to delete before insert to properly
        // end the old version (Hekaton-style UPDATE = DELETE + INSERT)
        if connection.mvcc_enabled() {
            program.emit_insn(Insn::Delete {
                cursor_id: seq_cursor_id,
                table_name: crate::schema::SQLITE_SEQUENCE_TABLE_NAME.to_string(),
                is_part_of_update: true,
            });
        }

        program.emit_insn(Insn::Insert {
            cursor: seq_cursor_id,
            key_reg: rowid,
            record_reg,
            flag: crate::vdbe::insn::InsertFlags(0),
            table_name: crate::schema::SQLITE_SEQUENCE_TABLE_NAME.to_string(),
        });

        program.resolve_label(continue_loop_label, program.offset());
    });
}

fn literal_default_value(literal: &ast::Literal) -> Result<Value> {
    match literal {
        ast::Literal::Numeric(val) => parse_numeric_literal(val),
        ast::Literal::String(s) => Ok(Value::from_text(crate::translate::expr::sanitize_string(s))),
        ast::Literal::Blob(s) => Ok(Value::Blob(
            s.as_bytes()
                .chunks_exact(2)
                .map(|pair| {
                    let hex_byte = std::str::from_utf8(pair).expect("parser validated hex string");
                    u8::from_str_radix(hex_byte, 16).expect("parser validated hex digit")
                })
                .collect(),
        )),
        ast::Literal::Null => Ok(Value::Null),
        ast::Literal::True => Ok(Value::from_i64(1)),
        ast::Literal::False => Ok(Value::from_i64(0)),
        ast::Literal::CurrentDate => Ok(Value::from_text("CURRENT_DATE")),
        ast::Literal::CurrentTime => Ok(Value::from_text("CURRENT_TIME")),
        ast::Literal::CurrentTimestamp => Ok(Value::from_text("CURRENT_TIMESTAMP")),
        ast::Literal::Keyword(_) => Err(LimboError::ParseError(
            "Cannot add a column with non-constant default".to_string(),
        )),
    }
}

pub(crate) fn eval_constant_default_value(expr: &ast::Expr) -> Result<Value> {
    match expr {
        ast::Expr::Literal(literal) => literal_default_value(literal),
        ast::Expr::Id(name) => Ok(Value::from_text(name.as_str().to_string())),
        ast::Expr::Unary(op, inner) => {
            let value = eval_constant_default_value(inner)?;
            match (op, value) {
                (ast::UnaryOperator::Positive, value) => Ok(value),
                (ast::UnaryOperator::Negative, Value::Numeric(Numeric::Integer(i))) => {
                    Ok(Value::from_i64(-i))
                }
                (ast::UnaryOperator::Negative, Value::Numeric(Numeric::Float(f))) => {
                    Ok(Value::from_f64(-f64::from(f)))
                }
                (ast::UnaryOperator::Negative, Value::Null) => Ok(Value::Null),
                (_, value) => Ok(value),
            }
        }
        ast::Expr::Parenthesized(exprs) => {
            if exprs.len() == 1 {
                eval_constant_default_value(&exprs[0])
            } else {
                Err(LimboError::ParseError(
                    "Cannot add a column with non-constant default".to_string(),
                ))
            }
        }
        _ => Err(LimboError::ParseError(
            "Cannot add a column with non-constant default".to_string(),
        )),
    }
}

fn apply_affinity_to_value(value: &mut Value, affinity: Affinity) {
    if let Some(converted) = affinity.convert(value) {
        *value = match converted {
            Either::Left(val_ref) => val_ref.to_owned(),
            Either::Right(val) => val,
        };
    }
}

fn strict_default_type_mismatch(column: &Column) -> Result<bool> {
    let Some(default_expr) = column.default.as_ref() else {
        return Ok(false);
    };

    let mut value = eval_constant_default_value(default_expr)?;
    if matches!(value, Value::Null) {
        return Ok(false);
    }

    apply_affinity_to_value(&mut value, column.affinity());

    let ty = column.ty_str.as_str();
    if ty.eq_ignore_ascii_case("ANY") {
        return Ok(false);
    };

    let ok = if ty.eq_ignore_ascii_case("INT") || ty.eq_ignore_ascii_case("INTEGER") {
        match value {
            Value::Numeric(Numeric::Integer(_)) => true,
            Value::Numeric(Numeric::Float(f)) => {
                let f = f64::from(f);
                let i = f as i64;
                (i as f64) == f
            }
            _ => false,
        }
    } else if ty.eq_ignore_ascii_case("REAL") {
        matches!(value, Value::Numeric(Numeric::Float(_)))
    } else if ty.eq_ignore_ascii_case("TEXT") {
        matches!(value, Value::Text(_))
    } else if ty.eq_ignore_ascii_case("BLOB") {
        matches!(value, Value::Blob(_))
    } else {
        true
    };

    Ok(!ok)
}

fn emit_add_column_default_type_validation(
    program: &mut ProgramBuilder,
    original_btree: &Arc<BTreeTable>,
) -> Result<()> {
    let check_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(original_btree.clone()));
    program.emit_insn(Insn::OpenRead {
        cursor_id: check_cursor_id,
        root_page: original_btree.root_page,
        db: crate::MAIN_DB_ID,
    });

    let skip_check_label = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: check_cursor_id,
        pc_if_empty: skip_check_label,
    });

    program.emit_insn(Insn::Halt {
        err_code: 1,
        description: "type mismatch on DEFAULT".to_string(),
        on_error: None,
        description_reg: None,
    });

    program.resolve_label(skip_check_label, program.offset());
    Ok(())
}

/// Validate NOT NULL and CHECK constraints for a new virtual generated column
/// by scanning each row and computing the expression.
#[allow(clippy::too_many_arguments)]
fn emit_add_virtual_column_validation(
    program: &mut ProgramBuilder,
    table: &BTreeTable,
    column: &Column,
    constraints: &[ast::NamedColumnConstraint],
    resolver: &Resolver,
    connection: &Arc<crate::Connection>,
    database_id: usize,
) -> Result<()> {
    let has_notnull = column.notnull();
    let check_constraints: Vec<CheckConstraint> = constraints
        .iter()
        .filter_map(|c| {
            if let ast::ColumnConstraint::Check(expr) = &c.constraint {
                Some(CheckConstraint::new(
                    c.name.as_ref(),
                    expr,
                    column.name.as_deref(),
                ))
            } else {
                None
            }
        })
        .collect();

    if !has_notnull && check_constraints.is_empty() {
        return Ok(());
    }

    let mut resolved_table = table.clone();
    resolved_table.prepare_generated_columns()?;
    let new_column_name = column
        .name
        .as_deref()
        .ok_or_else(|| LimboError::ParseError("generated column name is missing".to_string()))?;
    let (new_column_idx, _) = resolved_table.get_column(new_column_name).ok_or_else(|| {
        LimboError::ParseError(format!(
            "new generated column unexpectedly missing from table {}",
            resolved_table.name
        ))
    })?;

    let mut original_table = table.clone();
    original_table.columns.retain(|c| !c.is_virtual_generated());
    original_table.has_virtual_columns = false;
    original_table.logical_to_physical_map =
        BTreeTable::build_logical_to_physical_map(&original_table.columns);
    let original_table = Arc::new(original_table);
    let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(original_table.clone()));
    program.emit_insn(Insn::OpenRead {
        cursor_id,
        root_page: original_table.root_page,
        db: database_id,
    });

    let skip_label = program.allocate_label();
    let loop_start = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id,
        pc_if_empty: skip_label,
    });
    program.preassign_label_to_next_insn(loop_start);

    let rowid_reg = program.alloc_register();
    program.emit_insn(Insn::RowId {
        cursor_id,
        dest: rowid_reg,
    });

    let layout = resolved_table.column_layout();
    let base_dest_reg = program.alloc_registers(layout.column_count());
    for (idx, table_column) in resolved_table.columns.iter().enumerate() {
        if table_column.is_virtual_generated() || table_column.is_rowid_alias() {
            continue;
        }

        program.emit_column_or_rowid(
            cursor_id,
            layout.to_reg_offset(idx),
            layout.to_register(base_dest_reg, idx),
        );
    }

    let dml_ctx =
        DmlColumnContext::layout(&resolved_table.columns, base_dest_reg, rowid_reg, layout);
    compute_virtual_columns(program, &resolved_table.columns, &dml_ctx, resolver)?;
    let result_reg = dml_ctx.to_column_reg(new_column_idx);

    if !check_constraints.is_empty() {
        let mut check_resolver = resolver.fork();
        let skip_row_label = program.allocate_label();
        emit_check_constraints(
            program,
            &check_constraints,
            &mut check_resolver,
            resolved_table.name.as_str(),
            rowid_reg,
            resolved_table
                .columns
                .iter()
                .enumerate()
                .filter_map(|(idx, col)| {
                    col.name
                        .as_deref()
                        .map(|name| (name, dml_ctx.to_column_reg(idx)))
                }),
            connection,
            ast::ResolveType::Abort,
            skip_row_label,
            None,
        )?;
    }

    if has_notnull {
        let notnull_passed = program.allocate_label();
        program.emit_insn(Insn::NotNull {
            reg: result_reg,
            target_pc: notnull_passed,
        });
        program.emit_insn(Insn::Halt {
            err_code: 1,
            description: "NOT NULL constraint failed".to_string(),
            on_error: None,
            description_reg: None,
        });
        program.resolve_label(notnull_passed, program.offset());
    }

    program.emit_insn(Insn::Next {
        cursor_id,
        pc_if_next: loop_start,
    });

    program.resolve_label(skip_label, program.offset());
    Ok(())
}

/// Validate CHECK constraints on a newly added column against the column's DEFAULT value.
///
/// When a table has existing rows, the new column gets the DEFAULT value (or NULL).
/// If that value would violate a CHECK constraint, the ALTER TABLE must be rejected.
/// This emits bytecode that:
/// 1. Checks if the table has any rows (Rewind)
/// 2. Evaluates the CHECK expression with the column reference substituted by the DEFAULT
/// 3. Halts with a CHECK constraint error if the result is false
#[allow(clippy::too_many_arguments)]
fn emit_add_column_check_validation(
    program: &mut ProgramBuilder,
    btree: &BTreeTable,
    original_btree: &Arc<BTreeTable>,
    new_column_name: &str,
    column: &Column,
    constraints: &[ast::NamedColumnConstraint],
    resolver: &Resolver,
    database_id: usize,
) -> Result<()> {
    // Determine the effective default value. If no DEFAULT, existing rows get NULL,
    // which always passes CHECK per SQL standard (NULL is not false).
    let default_expr = match &column.default {
        Some(expr) if !crate::util::expr_contains_null(expr) => *expr.clone(),
        _ => return Ok(()),
    };

    // Collect CHECK constraints from the column constraints being added.
    let check_exprs: Vec<(&Option<ast::Name>, &Box<ast::Expr>)> = constraints
        .iter()
        .filter_map(|c| {
            if let ast::ColumnConstraint::Check(expr) = &c.constraint {
                Some((&c.name, expr))
            } else {
                None
            }
        })
        .collect();

    if check_exprs.is_empty() {
        return Ok(());
    }

    let table_name = &btree.name;
    let col_name_lower = normalize_ident(new_column_name);

    // Open the table to check if it has rows.
    let check_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(original_btree.clone()));
    program.emit_insn(Insn::OpenRead {
        cursor_id: check_cursor_id,
        root_page: original_btree.root_page,
        db: database_id,
    });

    let skip_check_label = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: check_cursor_id,
        pc_if_empty: skip_check_label,
    });

    // Table has rows -- evaluate each CHECK constraint with the default value substituted.
    for (constraint_name, check_expr) in &check_exprs {
        let mut substituted = (*check_expr).clone();

        // Replace references to the new column with the default value expression.
        let _ = walk_expr_mut(
            &mut substituted,
            &mut |e: &mut ast::Expr| -> Result<WalkControl> {
                match e {
                    ast::Expr::Id(name) if normalize_ident(name.as_str()) == col_name_lower => {
                        *e = default_expr.clone();
                        Ok(WalkControl::SkipChildren)
                    }
                    ast::Expr::Qualified(tbl, col)
                        if normalize_ident(tbl.as_str()) == normalize_ident(table_name)
                            && normalize_ident(col.as_str()) == col_name_lower =>
                    {
                        *e = default_expr.clone();
                        Ok(WalkControl::SkipChildren)
                    }
                    _ => Ok(WalkControl::Continue),
                }
            },
        );

        let result_reg = program.alloc_register();
        translate_expr(program, None, &substituted, result_reg, resolver)?;

        // CHECK passes if the result is NULL or non-zero (truthy).
        let check_passed_label = program.allocate_label();

        program.emit_insn(Insn::IsNull {
            reg: result_reg,
            target_pc: check_passed_label,
        });

        program.emit_insn(Insn::If {
            reg: result_reg,
            target_pc: check_passed_label,
            jump_if_null: false,
        });

        // CHECK failed -- halt with constraint error.
        let name = match constraint_name {
            Some(name) => name.as_str().to_string(),
            None => format!("{check_expr}"),
        };
        program.emit_insn(Insn::Halt {
            err_code: SQLITE_CONSTRAINT_CHECK,
            description: name,
            on_error: None,
            description_reg: None,
        });

        program.preassign_label_to_next_insn(check_passed_label);
    }

    program.resolve_label(skip_check_label, program.offset());
    Ok(())
}

pub fn translate_alter_table(
    alter: ast::AlterTable,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
    input: &str,
) -> Result<()> {
    let ast::AlterTable {
        name: qualified_name,
        body: alter_table,
    } = alter;
    let database_id = resolver.resolve_database_id(&qualified_name)?;
    if crate::is_attached_db(database_id) {
        let schema_cookie = resolver.with_schema(database_id, |s| s.schema_version);
        program.begin_write_on_database(database_id, schema_cookie);
    }
    program.begin_write_operation();
    let table_name = qualified_name.name.as_str();
    // For attached databases, qualify sqlite_schema with the database name
    // so that the UPDATE targets the correct database's schema table.
    let qualified_schema_table = match &qualified_name.db_name {
        Some(db_name) => format!("{}.{}", db_name.as_str(), SQLITE_TABLEID),
        None => SQLITE_TABLEID.to_string(),
    };
    let schema_version = resolver.with_schema(database_id, |s| s.schema_version);
    validate(&alter_table, table_name)?;

    let table_indexes = resolver.with_schema(database_id, |s| {
        s.get_indices(table_name).cloned().collect::<Vec<_>>()
    });

    let Some(table) = resolver.with_schema(database_id, |s| s.get_table(table_name)) else {
        return Err(LimboError::ParseError(format!(
            "no such table: {table_name}"
        )));
    };
    if let Some(tbl) = table.virtual_table() {
        if let ast::AlterTableBody::RenameTo(new_name) = &alter_table {
            let new_name_norm = normalize_ident(new_name.as_str());
            return translate_rename_virtual_table(
                program,
                tbl,
                table_name,
                new_name_norm,
                resolver,
                connection,
                database_id,
            );
        }
    }
    let Some(original_btree) = table.btree() else {
        crate::bail_parse_error!("ALTER TABLE is only supported for BTree tables");
    };

    // Check if this table has dependent materialized views
    let dependent_views = resolver.with_schema(database_id, |s| {
        s.get_dependent_materialized_views(table_name)
    });
    if !dependent_views.is_empty() {
        return Err(LimboError::ParseError(format!(
            "cannot alter table \"{table_name}\": it has dependent materialized view(s): {}",
            dependent_views.join(", ")
        )));
    }

    let mut btree = (*original_btree).clone();

    match alter_table {
        ast::AlterTableBody::DropColumn(column_name) => {
            let column_name = column_name.as_str();

            // Tables always have at least one column.
            turso_assert_ne!(btree.columns.len(), 0);

            if btree.columns.len() == 1 {
                return Err(LimboError::ParseError(format!(
                    "cannot drop column \"{column_name}\": no other columns exist"
                )));
            }

            let (dropped_index, column) = btree.get_column(column_name).ok_or_else(|| {
                LimboError::ParseError(format!("no such column: \"{column_name}\""))
            })?;

            // Column cannot be dropped if:
            // The column is a PRIMARY KEY or part of one.
            // The column has a UNIQUE constraint.
            // The column is indexed.
            // The column is referenced in an expression index.
            // The column is named in the WHERE clause of a partial index.
            // The column is named in a table or column CHECK constraint not associated with the column being dropped.
            // The column is used in a foreign key constraint.
            // The column is used in the expression of a generated column.
            // The column appears in a trigger or view.

            if column.primary_key() {
                return Err(LimboError::ParseError(format!(
                    "cannot drop column \"{column_name}\": PRIMARY KEY"
                )));
            }

            if column.unique()
                || btree.unique_sets.iter().any(|set| {
                    set.columns
                        .iter()
                        .any(|(name, _)| name == &normalize_ident(column_name))
                })
            {
                return Err(LimboError::ParseError(format!(
                    "cannot drop column \"{column_name}\": UNIQUE"
                )));
            }

            let col_normalized = normalize_ident(column_name);
            for index in table_indexes.iter() {
                // Referenced in regular index
                let maybe_indexed_col = index
                    .columns
                    .iter()
                    .enumerate()
                    .find(|(_, col)| col.pos_in_table == dropped_index);
                if let Some((pos_in_index, indexed_col)) = maybe_indexed_col {
                    return Err(LimboError::ParseError(format!(
                        "cannot drop column \"{column_name}\": it is referenced in the index {}; position in index is {pos_in_index}, position in table is {}",
                        index.name, indexed_col.pos_in_table
                    )));
                }
                // Referenced in expression index
                for idx_col in &index.columns {
                    if let Some(expr) = &idx_col.expr {
                        if check_expr_references_column(expr, &col_normalized) {
                            return Err(LimboError::ParseError(format!(
                                "error in index {} after drop column: no such column: {column_name}",
                                index.name
                            )));
                        }
                    }
                }
                // Referenced in partial index
                if index.where_clause.is_some() {
                    let mut table_references = TableReferences::new(
                        vec![],
                        vec![OuterQueryReference {
                            identifier: table_name.to_string(),
                            internal_id: TableInternalId::from(0),
                            table: Table::BTree(Arc::new(btree.clone())),
                            col_used_mask: ColumnUsedMask::default(),
                            cte_select: None,
                            cte_explicit_columns: vec![],
                            cte_id: None,
                            cte_definition_only: false,
                            rowid_referenced: false,
                        }],
                    );
                    let where_copy = index
                        .bind_where_expr(Some(&mut table_references), resolver)
                        .ok_or_else(|| {
                            LimboError::ParseError(
                                "index where clause unexpectedly missing".to_string(),
                            )
                        })?;
                    let mut column_referenced = false;
                    walk_expr(
                        &where_copy,
                        &mut |e: &ast::Expr| -> crate::Result<WalkControl> {
                            if let ast::Expr::Column {
                                table,
                                column: column_index,
                                ..
                            } = e
                            {
                                if *table == TableInternalId::from(0)
                                    && *column_index == dropped_index
                                {
                                    column_referenced = true;
                                    return Ok(WalkControl::SkipChildren);
                                }
                            }
                            Ok(WalkControl::Continue)
                        },
                    )?;
                    if column_referenced {
                        return Err(LimboError::ParseError(format!(
                            "cannot drop column \"{column_name}\": indexed"
                        )));
                    }
                }
            }

            // Handle CHECK constraints:
            // - Column-level CHECK constraints for the dropped column are silently removed
            // - Table-level CHECK constraints referencing the dropped column cause an error
            for check in &btree.check_constraints {
                if check.column.is_some() {
                    // Column-level constraint: will be removed below
                    continue;
                }
                // Table-level constraint: check if it references the dropped column
                if check_expr_references_column(&check.expr, &col_normalized) {
                    return Err(LimboError::ParseError(format!(
                        "error in table {table_name} after drop column: no such column: {column_name}"
                    )));
                }
            }
            // Remove column-level CHECK constraints for the dropped column
            btree.check_constraints.retain(|c| {
                c.column
                    .as_ref()
                    .is_none_or(|col| normalize_ident(col) != normalize_ident(column_name))
            });

            // Check if column is used in a foreign key constraint (child side)
            // SQLite does not allow dropping a column that is part of a FK constraint
            let column_name_norm = normalize_ident(column_name);
            for fk in &btree.foreign_keys {
                if fk.child_columns.contains(&column_name_norm) {
                    return Err(LimboError::ParseError(format!(
                        "error in table {table_name} after drop column: unknown column \"{column_name}\" in foreign key definition"
                    )));
                }
            }

            // Must have at least one non-generated column after the drop
            {
                let remaining_non_generated = btree
                    .columns
                    .iter()
                    .enumerate()
                    .filter(|(idx, col)| *idx != dropped_index && !col.is_generated())
                    .count();
                if remaining_non_generated == 0 {
                    return Err(LimboError::ParseError(format!(
                        "error in table {table_name} after drop column: must have at least one non-generated column"
                    )));
                }
            }

            // Check if any virtual column depends on the dropped column
            {
                let mut dropped_set = rustc_hash::FxHashSet::default();
                dropped_set.insert(dropped_index);
                let affected =
                    crate::schema::columns_affected_by_update(&btree.columns, &dropped_set);
                for idx in &affected {
                    if *idx != dropped_index && btree.columns[*idx].is_virtual_generated() {
                        return Err(LimboError::ParseError(format!(
                            "error in table {table_name} after drop column: no such column: {column_name}"
                        )));
                    }
                }
            }

            // References in VIEWs are checked in the VDBE layer op_drop_column instruction.

            // Like SQLite, re-validate ALL triggers after the drop. Any trigger whose
            // body references a nonexistent column (in the altered table or any other
            // table) causes the DROP to fail. This catches cross-table references to
            // the dropped column as well as pre-existing errors that would surface at
            // trigger execution time.
            let post_drop_btree = {
                let mut t = btree.clone();
                t.columns.remove(dropped_index);
                t
            };
            let all_triggers: Vec<_> = resolver.with_schema(database_id, |s| {
                s.triggers.values().flatten().cloned().collect()
            });
            let table_name_norm = normalize_ident(table_name);
            for trigger in &all_triggers {
                if let Some(bad_col) = validate_trigger_columns_after_drop(
                    trigger,
                    &table_name_norm,
                    &post_drop_btree,
                    resolver,
                    database_id,
                )? {
                    return Err(LimboError::ParseError(format!(
                        "error in trigger {} after drop column: no such column: {}",
                        trigger.name, bad_col
                    )));
                }
            }

            btree.columns.remove(dropped_index);

            let sql = btree.to_sql().replace('\'', "''");

            let escaped_table_name = escape_sql_string_literal(table_name);
            let stmt = format!(
                r#"
                    UPDATE {qualified_schema_table}
                    SET sql = '{sql}'
                    WHERE name = '{escaped_table_name}' COLLATE NOCASE AND type = 'table'
                "#,
            );

            let mut parser = Parser::new(stmt.as_bytes());
            let cmd = parser.next_cmd().map_err(|e| {
                LimboError::ParseError(format!("failed to parse generated UPDATE statement: {e}"))
            })?;
            let Some(ast::Cmd::Stmt(ast::Stmt::Update(update))) = cmd else {
                return Err(LimboError::ParseError(
                    "generated UPDATE statement did not parse as expected".to_string(),
                ));
            };

            translate_update_for_schema_change(
                update,
                resolver,
                program,
                connection,
                input,
                |program| {
                    let table_name = btree.name.clone();
                    let source_column_by_schema_idx = btree
                        .columns
                        .iter()
                        .enumerate()
                        .map(|(new_idx, column)| {
                            if column.is_virtual_generated() {
                                None
                            } else if new_idx < dropped_index {
                                Some(new_idx)
                            } else {
                                Some(new_idx + 1)
                            }
                        })
                        .collect();
                    emit_rewrite_table_rows(
                        program,
                        original_btree.clone(),
                        &btree,
                        source_column_by_schema_idx,
                        connection,
                        database_id,
                    );

                    program.emit_insn(Insn::SetCookie {
                        db: database_id,
                        cookie: Cookie::SchemaVersion,
                        value: schema_version as i32 + 1,
                        p5: 0,
                    });

                    program.emit_insn(Insn::DropColumn {
                        db: database_id,
                        table: table_name,
                        column_index: dropped_index,
                    })
                },
            )?
        }
        ast::AlterTableBody::AddColumn(col_def) => {
            let is_generated = col_def
                .constraints
                .iter()
                .any(|c| matches!(c.constraint, ast::ColumnConstraint::Generated { .. }));
            let is_stored = col_def.constraints.iter().any(|c| {
                matches!(
                    c.constraint,
                    ast::ColumnConstraint::Generated {
                        typ: Some(ast::GeneratedColumnType::Stored),
                        ..
                    }
                )
            });
            if is_stored {
                return Err(LimboError::ParseError(
                    "cannot add a STORED column".to_string(),
                ));
            }
            if is_generated {
                for c in &col_def.constraints {
                    if let ast::ColumnConstraint::Generated { expr, .. } = &c.constraint {
                        crate::schema::validate_generated_expr(expr)?;
                    }
                }
            }
            let constraints = col_def.constraints.clone();
            let mut column = Column::try_from(&col_def)?;

            // SQLite is very strict about what constitutes a "constant" default for
            // ALTER TABLE ADD COLUMN. It only allows literals and signed literals,
            // not arbitrary constant expressions like (5 + 3) or COALESCE(NULL, 5).
            if !is_generated
                && column
                    .default
                    .as_ref()
                    .is_some_and(|default| !is_strict_constant_default(default))
            {
                return Err(LimboError::ParseError(
                    "Cannot add a column with non-constant default".to_string(),
                ));
            }

            let new_column_name = column.name.as_ref().ok_or_else(|| {
                LimboError::ParseError(
                    "column name is missing in ALTER TABLE ADD COLUMN".to_string(),
                )
            })?;
            if btree.get_column(new_column_name).is_some() {
                return Err(LimboError::ParseError(
                    "duplicate column name: ".to_string() + new_column_name,
                ));
            }

            let default_type_mismatch;
            {
                let ty = column.ty_str.as_str();
                if btree.is_strict && ty.is_empty() {
                    return Err(LimboError::ParseError(format!(
                        "missing datatype for {table_name}.{new_column_name}"
                    )));
                }
                let is_builtin = ty.is_empty()
                    || ty.eq_ignore_ascii_case("INT")
                    || ty.eq_ignore_ascii_case("INTEGER")
                    || ty.eq_ignore_ascii_case("REAL")
                    || ty.eq_ignore_ascii_case("TEXT")
                    || ty.eq_ignore_ascii_case("BLOB")
                    || ty.eq_ignore_ascii_case("ANY");
                if !is_builtin && btree.is_strict {
                    // On non-STRICT tables any type name is allowed and is
                    // treated as a plain affinity hint (no encode/decode).
                    // Custom type validation only applies to STRICT tables.
                    let type_def = resolver
                        .schema()
                        .get_type_def_unchecked(&normalize_ident(ty));
                    if type_def.is_none() {
                        return Err(LimboError::ParseError(format!(
                            "unknown datatype for {table_name}.{new_column_name}: \"{ty}\""
                        )));
                    }
                }

                default_type_mismatch = strict_default_type_mismatch(&column)?;
            }

            // If a column has no explicit DEFAULT but its custom type defines
            // one, propagate the type-level DEFAULT to the column so that
            // existing rows get the type default instead of NULL.
            if column.default.is_none() {
                if let Some(type_def) = resolver
                    .schema()
                    .get_type_def(&column.ty_str, btree.is_strict)
                {
                    if let Some(ref type_default) = type_def.default {
                        column.default = Some(type_default.clone());
                    }
                }
            }

            // TODO: All quoted ids will be quoted with `[]`, we should store some info from the parsed AST
            btree.columns.push(column.clone());

            // Add foreign key constraints and CHECK constraints to the btree table
            for constraint in &constraints {
                match &constraint.constraint {
                    ast::ColumnConstraint::ForeignKey {
                        clause,
                        defer_clause,
                    } => {
                        if clause.columns.len() > 1 {
                            return Err(LimboError::ParseError(format!(
                                "foreign key on {new_column_name} should reference only one column of table {}",
                                clause.tbl_name.as_str()
                            )));
                        }
                        let fk = ForeignKey {
                            parent_table: normalize_ident(clause.tbl_name.as_str()),
                            parent_columns: clause
                                .columns
                                .iter()
                                .map(|c| normalize_ident(c.col_name.as_str()))
                                .collect(),
                            on_delete: clause
                                .args
                                .iter()
                                .find_map(|arg| {
                                    if let ast::RefArg::OnDelete(act) = arg {
                                        Some(*act)
                                    } else {
                                        None
                                    }
                                })
                                .unwrap_or(ast::RefAct::NoAction),
                            on_update: clause
                                .args
                                .iter()
                                .find_map(|arg| {
                                    if let ast::RefArg::OnUpdate(act) = arg {
                                        Some(*act)
                                    } else {
                                        None
                                    }
                                })
                                .unwrap_or(ast::RefAct::NoAction),
                            child_columns: vec![new_column_name.to_string()],
                            deferred: match defer_clause {
                                Some(d) => {
                                    d.deferrable
                                        && matches!(
                                            d.init_deferred,
                                            Some(ast::InitDeferredPred::InitiallyDeferred)
                                        )
                                }
                                None => false,
                            },
                        };
                        btree.foreign_keys.push(Arc::new(fk));
                    }
                    ast::ColumnConstraint::Check(expr) => {
                        let column_names: Vec<&str> = btree
                            .columns
                            .iter()
                            .filter_map(|c| c.name.as_deref())
                            .collect();
                        validate_check_expr(expr, &btree.name, &column_names, resolver)?;
                        btree.check_constraints.push(CheckConstraint::new(
                            constraint.name.as_ref(),
                            expr,
                            Some(new_column_name),
                        ));
                    }
                    _ => {
                        // Other constraints (PRIMARY KEY, NOT NULL, etc.) are handled elsewhere
                    }
                }
            }

            let sql = btree.to_sql();
            let mut escaped = String::with_capacity(sql.len());

            for ch in sql.chars() {
                match ch {
                    '\'' => escaped.push_str("''"),
                    ch => escaped.push(ch),
                }
            }

            let escaped_table_name = escape_sql_string_literal(table_name);
            let stmt = format!(
                r#"
                    UPDATE {qualified_schema_table}
                    SET sql = '{escaped}'
                    WHERE name = '{escaped_table_name}' COLLATE NOCASE AND type = 'table'
                "#,
            );

            let mut parser = Parser::new(stmt.as_bytes());
            let cmd = parser.next_cmd().map_err(|e| {
                LimboError::ParseError(format!("failed to parse generated UPDATE statement: {e}"))
            })?;
            let Some(ast::Cmd::Stmt(ast::Stmt::Update(update))) = cmd else {
                return Err(LimboError::ParseError(
                    "generated UPDATE statement did not parse as expected".to_string(),
                ));
            };

            if is_generated {
                emit_add_virtual_column_validation(
                    program,
                    &btree,
                    &column,
                    &constraints,
                    resolver,
                    connection,
                    database_id,
                )?;
            } else {
                // Check if we need to verify the table is empty at runtime.
                let effective_default = column.default.as_ref().or_else(|| {
                    resolver
                        .schema()
                        .get_type_def(&column.ty_str, btree.is_strict)
                        .and_then(|td| td.default.as_ref())
                });
                let needs_notnull_check = column.notnull()
                    && effective_default
                        .is_none_or(|default| crate::util::expr_contains_null(default));

                let needs_nondeterministic_check = column
                    .default
                    .as_ref()
                    .is_some_and(|default| default_requires_empty_table(default));

                let (needs_empty_table_check, error_message) = if needs_notnull_check {
                    (true, "Cannot add a NOT NULL column with default value NULL")
                } else if needs_nondeterministic_check {
                    (true, "Cannot add a column with non-constant default")
                } else {
                    (false, "")
                };

                if needs_empty_table_check {
                    let check_cursor_id =
                        program.alloc_cursor_id(CursorType::BTreeTable(original_btree.clone()));
                    program.emit_insn(Insn::OpenRead {
                        cursor_id: check_cursor_id,
                        root_page: original_btree.root_page,
                        db: database_id,
                    });

                    let skip_error_label = program.allocate_label();
                    program.emit_insn(Insn::Rewind {
                        cursor_id: check_cursor_id,
                        pc_if_empty: skip_error_label,
                    });

                    program.emit_insn(Insn::Halt {
                        err_code: 1,
                        description: error_message.to_string(),
                        on_error: None,
                        description_reg: None,
                    });

                    program.resolve_label(skip_error_label, program.offset());
                }

                if default_type_mismatch {
                    emit_add_column_default_type_validation(program, &original_btree)?;
                }

                emit_add_column_check_validation(
                    program,
                    &btree,
                    &original_btree,
                    new_column_name,
                    &column,
                    &constraints,
                    resolver,
                    database_id,
                )?;
            }

            translate_update_for_schema_change(
                update,
                resolver,
                program,
                connection,
                input,
                |program| {
                    program.emit_insn(Insn::SetCookie {
                        db: database_id,
                        cookie: Cookie::SchemaVersion,
                        value: schema_version as i32 + 1,
                        p5: 0,
                    });
                    program.emit_insn(Insn::AddColumn {
                        db: database_id,
                        table: table_name.to_owned(),
                        column: Box::new(column),
                        check_constraints: btree.check_constraints.clone(),
                    });
                },
            )?
        }
        ast::AlterTableBody::RenameTo(new_name) => {
            let new_name = new_name.as_str();
            let normalized_old_name = normalize_ident(table_name);
            let normalized_new_name = normalize_ident(new_name);

            if resolver.with_schema(database_id, |s| {
                s.get_table(new_name).is_some()
                    || s.indexes
                        .values()
                        .flatten()
                        .any(|index| index.name == normalize_ident(new_name))
            }) {
                return Err(LimboError::ParseError(format!(
                    "there is already another table or index with this name: {new_name}"
                )));
            };

            let sqlite_schema = resolver
                .with_schema(database_id, |s| s.get_btree_table(SQLITE_TABLEID))
                .ok_or_else(|| {
                    LimboError::ParseError("sqlite_schema table not found in schema".to_string())
                })?;

            let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(sqlite_schema.clone()));

            program.emit_insn(Insn::OpenWrite {
                cursor_id,
                root_page: RegisterOrLiteral::Literal(sqlite_schema.root_page),
                db: database_id,
            });

            program.cursor_loop(cursor_id, |program, rowid| {
                let sqlite_schema_column_len = sqlite_schema.columns.len();
                turso_assert_eq!(sqlite_schema_column_len, 5);

                let first_column = program.alloc_registers(sqlite_schema_column_len);

                for i in 0..sqlite_schema_column_len {
                    program.emit_column_or_rowid(cursor_id, i, first_column + i);
                }

                program.emit_string8_new_reg(table_name.to_string());
                program.mark_last_insn_constant();

                program.emit_string8_new_reg(new_name.to_string());
                program.mark_last_insn_constant();

                let out = program.alloc_registers(5);

                program.emit_insn(Insn::Function {
                    constant_mask: 0,
                    start_reg: first_column,
                    dest: out,
                    func: crate::function::FuncCtx {
                        func: Func::AlterTable(AlterTableFunc::RenameTable),
                        arg_count: 7,
                    },
                });

                let record = program.alloc_register();

                program.emit_insn(Insn::MakeRecord {
                    start_reg: to_u16(out),
                    count: to_u16(sqlite_schema_column_len),
                    dest_reg: to_u16(record),
                    index_name: None,
                    affinity_str: None,
                });

                // In MVCC mode, we need to delete before insert to properly
                // end the old version (Hekaton-style UPDATE = DELETE + INSERT)
                if connection.mvcc_enabled() {
                    program.emit_insn(Insn::Delete {
                        cursor_id,
                        table_name: SQLITE_TABLEID.to_string(),
                        is_part_of_update: true,
                    });
                }

                program.emit_insn(Insn::Insert {
                    cursor: cursor_id,
                    key_reg: rowid,
                    record_reg: record,
                    flag: crate::vdbe::insn::InsertFlags(0),
                    table_name: table_name.to_string(),
                });
            });

            emit_rename_sqlite_sequence_entry(
                program,
                resolver,
                connection,
                database_id,
                &normalized_old_name,
                &normalized_new_name,
            );

            program.emit_insn(Insn::SetCookie {
                db: database_id,
                cookie: Cookie::SchemaVersion,
                value: schema_version as i32 + 1,
                p5: 0,
            });

            program.emit_insn(Insn::RenameTable {
                db: database_id,
                from: table_name.to_owned(),
                to: new_name.to_owned(),
            });
        }
        body @ (ast::AlterTableBody::AlterColumn { .. }
        | ast::AlterTableBody::RenameColumn { .. }) => {
            let from;
            let definition;
            let col_name;
            let rename;

            match body {
                ast::AlterTableBody::AlterColumn { old, new } => {
                    from = old;
                    definition = new;
                    col_name = definition.col_name.clone();
                    rename = false;
                }
                ast::AlterTableBody::RenameColumn { old, new } => {
                    from = old;
                    definition = ast::ColumnDefinition {
                        col_name: new.clone(),
                        col_type: None,
                        constraints: vec![],
                    };
                    col_name = new;
                    rename = true;
                }
                _ => unreachable!(),
            }

            let from = from.as_str();
            let col_name = col_name.as_str();

            let Some((column_index, _)) = btree.get_column(from) else {
                return Err(LimboError::ParseError(format!(
                    "no such column: \"{from}\""
                )));
            };

            if btree.get_column(col_name).is_some() {
                return Err(LimboError::ParseError(format!(
                    "duplicate column name: \"{col_name}\""
                )));
            };

            if definition
                .constraints
                .iter()
                .any(|c| matches!(c.constraint, ast::ColumnConstraint::PrimaryKey { .. }))
            {
                return Err(LimboError::ParseError(
                    "PRIMARY KEY constraint cannot be altered".to_string(),
                ));
            }

            if definition
                .constraints
                .iter()
                .any(|c| matches!(c.constraint, ast::ColumnConstraint::Unique { .. }))
            {
                return Err(LimboError::ParseError(
                    "UNIQUE constraint cannot be altered".to_string(),
                ));
            }

            let (rewrites_physical_layout, replacement_column) = match rename {
                true => (false, None),
                false => {
                    let replacement_column = Column::try_from(&definition)?;
                    let rewrites_physical_layout = !btree.columns[column_index].is_generated()
                        && replacement_column.is_generated();
                    (rewrites_physical_layout, Some(replacement_column))
                }
            };

            let is_making_column_generated = definition
                .constraints
                .iter()
                .any(|c| matches!(c.constraint, ast::ColumnConstraint::Generated { .. }));

            if is_making_column_generated {
                let is_stored = definition.constraints.iter().any(|c| {
                    matches!(
                        c.constraint,
                        ast::ColumnConstraint::Generated {
                            typ: Some(ast::GeneratedColumnType::Stored),
                            ..
                        }
                    )
                });
                if is_stored {
                    return Err(LimboError::ParseError(
                        "cannot add a STORED column".to_string(),
                    ));
                }

                for constraint in &definition.constraints {
                    if let ast::ColumnConstraint::Generated { expr, .. } = &constraint.constraint {
                        crate::schema::validate_generated_expr(expr)?;
                    }
                }

                let non_generated_count = btree
                    .columns
                    .iter()
                    .enumerate()
                    .filter(|(idx, col)| *idx != column_index && !col.is_generated())
                    .count();

                if non_generated_count == 0 {
                    return Err(LimboError::ParseError(
                        "must have at least one non-generated column".to_string(),
                    ));
                }
            }

            let rewritten_table = if rewrites_physical_layout {
                let mut table = btree.clone();
                table.columns[column_index] =
                    replacement_column.expect("replacement_column must exist for ALTER COLUMN");
                table.prepare_generated_columns()?;
                table.logical_to_physical_map =
                    BTreeTable::build_logical_to_physical_map(&table.columns);
                Some(table)
            } else {
                None
            };

            // If renaming, rewrite trigger SQL for all triggers that reference this column
            // We'll collect the triggers to rewrite and update them in sqlite_schema
            let mut triggers_to_rewrite: Vec<(String, String)> = Vec::new();
            let mut views_to_rewrite: Vec<(String, String)> = Vec::new();
            if rename {
                // Try to rewrite every trigger's SQL for the column rename.
                // If the rewritten SQL differs from the original, include it
                // in the update list. This matches SQLite's approach and avoids
                // incomplete detection heuristics that miss expression-level refs
                // (e.g., `SELECT b FROM src` in a trigger on a different table).
                let all_triggers: Vec<_> = resolver.with_schema(database_id, |s| {
                    s.triggers.values().flatten().cloned().collect()
                });
                for trigger in &all_triggers {
                    match trigger_col_rename::execute(
                        &trigger.sql,
                        table_name,
                        from,
                        col_name,
                        database_id,
                        resolver,
                    ) {
                        Ok(new_sql) => {
                            if new_sql != trigger.sql {
                                triggers_to_rewrite.push((trigger.name.clone(), new_sql));
                            }
                        }
                        Err(e) => {
                            // If we can't rewrite the trigger, fail the ALTER TABLE operation
                            return Err(LimboError::ParseError(format!(
                                "error in trigger {} after rename column: {}",
                                trigger.name, e
                            )));
                        }
                    }
                }
                let target_db_name = resolver
                    .get_database_name_by_index(database_id)
                    .ok_or_else(|| {
                        LimboError::InternalError(format!(
                            "unknown database id {database_id} during ALTER TABLE"
                        ))
                    })?;
                views_to_rewrite = resolver.with_schema(database_id, |s| -> Result<_> {
                    let mut rewrites = Vec::new();
                    for (view_name, view) in s.views.iter() {
                        if let Some(rewritten) = rewrite_view_sql_for_column_rename(
                            &view.sql,
                            s,
                            table_name,
                            &target_db_name,
                            from,
                            col_name,
                        )? {
                            rewrites.push((view_name.clone(), rewritten.sql));
                        }
                    }
                    Ok(rewrites)
                })?;
            }

            let sqlite_schema = resolver
                .with_schema(database_id, |s| s.get_btree_table(SQLITE_TABLEID))
                .ok_or_else(|| {
                    LimboError::ParseError("sqlite_schema table not found in schema".to_string())
                })?;

            let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(sqlite_schema.clone()));

            program.emit_insn(Insn::OpenWrite {
                cursor_id,
                root_page: RegisterOrLiteral::Literal(sqlite_schema.root_page),
                db: database_id,
            });

            program.cursor_loop(cursor_id, |program, rowid| {
                let sqlite_schema_column_len = sqlite_schema.columns.len();
                turso_assert_eq!(sqlite_schema_column_len, 5);

                let first_column = program.alloc_registers(sqlite_schema_column_len);

                for i in 0..sqlite_schema_column_len {
                    program.emit_column_or_rowid(cursor_id, i, first_column + i);
                }

                program.emit_string8_new_reg(table_name.to_string());
                program.mark_last_insn_constant();

                program.emit_string8_new_reg(from.to_string());
                program.mark_last_insn_constant();

                program.emit_string8_new_reg(definition.to_string());
                program.mark_last_insn_constant();

                let out = program.alloc_registers(sqlite_schema_column_len);

                program.emit_insn(Insn::Function {
                    constant_mask: 0,
                    start_reg: first_column,
                    dest: out,
                    func: crate::function::FuncCtx {
                        func: Func::AlterTable(if rename {
                            AlterTableFunc::RenameColumn
                        } else {
                            AlterTableFunc::AlterColumn
                        }),
                        arg_count: 8,
                    },
                });

                let record = program.alloc_register();

                program.emit_insn(Insn::MakeRecord {
                    start_reg: to_u16(out),
                    count: to_u16(sqlite_schema_column_len),
                    dest_reg: to_u16(record),
                    index_name: None,
                    affinity_str: None,
                });

                // In MVCC mode, we need to delete before insert to properly
                // end the old version (Hekaton-style UPDATE = DELETE + INSERT)
                if connection.mvcc_enabled() {
                    program.emit_insn(Insn::Delete {
                        cursor_id,
                        table_name: SQLITE_TABLEID.to_string(),
                        is_part_of_update: true,
                    });
                }

                program.emit_insn(Insn::Insert {
                    cursor: cursor_id,
                    key_reg: rowid,
                    record_reg: record,
                    flag: crate::vdbe::insn::InsertFlags(0),
                    table_name: table_name.to_string(),
                });
            });

            // Update trigger SQL for renamed columns
            for (trigger_name, new_sql) in triggers_to_rewrite {
                let escaped_sql = new_sql.replace('\'', "''");
                let escaped_trigger_name = escape_sql_string_literal(&trigger_name);
                let update_stmt = format!(
                    r#"
                        UPDATE {qualified_schema_table}
                        SET sql = '{escaped_sql}'
                        WHERE name = '{escaped_trigger_name}' COLLATE NOCASE AND type = 'trigger'
                    "#,
                );

                let mut parser = Parser::new(update_stmt.as_bytes());
                let cmd = parser.next_cmd().map_err(|e| {
                    LimboError::ParseError(format!(
                        "failed to parse trigger update SQL for {trigger_name}: {e}"
                    ))
                })?;
                let Some(ast::Cmd::Stmt(ast::Stmt::Update(update))) = cmd else {
                    return Err(LimboError::ParseError(format!(
                        "failed to parse trigger update SQL for {trigger_name}",
                    )));
                };

                translate_update_for_schema_change(
                    update,
                    resolver,
                    program,
                    connection,
                    input,
                    |_program| {},
                )?;
            }

            // Update view SQL for renamed columns
            for (view_name, new_sql) in views_to_rewrite {
                let escaped_sql = new_sql.replace('\'', "''");
                let update_stmt = format!(
                    r#"
                        UPDATE {qualified_schema_table}
                        SET sql = '{escaped_sql}'
                        WHERE name = '{view_name}' COLLATE NOCASE AND type = 'view'
                    "#,
                );

                let mut parser = Parser::new(update_stmt.as_bytes());
                let cmd = parser.next_cmd().map_err(|e| {
                    LimboError::ParseError(format!(
                        "failed to parse view update SQL for {view_name}: {e}"
                    ))
                })?;
                let Some(ast::Cmd::Stmt(ast::Stmt::Update(update))) = cmd else {
                    return Err(LimboError::ParseError(format!(
                        "failed to parse view update SQL for {view_name}",
                    )));
                };

                translate_update_for_schema_change(
                    update,
                    resolver,
                    program,
                    connection,
                    input,
                    |_program| {},
                )?;
            }

            if let Some(rewritten_table) = rewritten_table {
                emit_add_virtual_column_validation(
                    program,
                    &rewritten_table,
                    &rewritten_table.columns[column_index],
                    &definition.constraints,
                    resolver,
                    connection,
                    database_id,
                )?;

                let source_column_by_schema_idx = rewritten_table
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(idx, column)| {
                        if column.is_virtual_generated() {
                            None
                        } else {
                            Some(idx)
                        }
                    })
                    .collect();
                emit_rewrite_table_rows(
                    program,
                    original_btree.clone(),
                    &rewritten_table,
                    source_column_by_schema_idx,
                    connection,
                    database_id,
                );
            }

            program.emit_insn(Insn::SetCookie {
                db: database_id,
                cookie: Cookie::SchemaVersion,
                value: schema_version as i32 + 1,
                p5: 0,
            });
            program.emit_insn(Insn::AlterColumn {
                db: database_id,
                table: table_name.to_owned(),
                column_index,
                definition: Box::new(definition),
                rename,
            });
        }
    };

    Ok(())
}

/// Rewrite every row in `original_btree` into the physical layout of `rewritten_table`.
///
/// `source_column_by_schema_idx` is indexed by the rewritten table's schema order. Each
/// entry is either the physical column index to read from the old row image or `None`
/// for virtual generated columns, which are omitted from the stored record entirely.
fn emit_rewrite_table_rows(
    program: &mut ProgramBuilder,
    original_btree: Arc<BTreeTable>,
    rewritten_table: &BTreeTable,
    source_column_by_schema_idx: Vec<Option<usize>>,
    connection: &Arc<crate::Connection>,
    database_id: usize,
) {
    turso_assert_eq!(
        source_column_by_schema_idx.len(),
        rewritten_table.columns.len()
    );

    let layout = rewritten_table.column_layout();
    let non_virtual_column_count = layout.num_non_virtual_cols();
    let root_page = rewritten_table.root_page;
    let table_name = rewritten_table.name.clone();
    let affinity_str = non_virtual_affinity_str(rewritten_table);
    let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(original_btree));

    program.emit_insn(Insn::OpenWrite {
        cursor_id,
        root_page: RegisterOrLiteral::Literal(root_page),
        db: database_id,
    });

    program.cursor_loop(cursor_id, |program, rowid| {
        let base_dest_reg = program.alloc_registers(non_virtual_column_count);
        for (schema_idx, source_column_idx) in source_column_by_schema_idx.iter().enumerate() {
            let Some(source_column_idx) = source_column_idx else {
                continue;
            };

            program.emit_column_or_rowid(
                cursor_id,
                *source_column_idx,
                layout.to_register(base_dest_reg, schema_idx),
            );
        }

        let record = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u16(base_dest_reg),
            count: to_u16(non_virtual_column_count),
            dest_reg: to_u16(record),
            index_name: None,
            affinity_str: Some(affinity_str.clone()),
        });

        if connection.mvcc_enabled() {
            program.emit_insn(Insn::Delete {
                cursor_id,
                table_name: table_name.clone(),
                is_part_of_update: true,
            });
        }

        program.emit_insn(Insn::Insert {
            cursor: cursor_id,
            key_reg: rowid,
            record_reg: record,
            flag: crate::vdbe::insn::InsertFlags(0),
            table_name: table_name.clone(),
        });
    });
}

fn non_virtual_affinity_str(table: &BTreeTable) -> String {
    table
        .columns
        .iter()
        .filter(|col| !col.is_virtual_generated())
        .map(|col| col.affinity_with_strict(table.is_strict).aff_mask())
        .collect()
}

fn translate_rename_virtual_table(
    program: &mut ProgramBuilder,
    vtab: Arc<VirtualTable>,
    old_name: &str,
    new_name_norm: String,
    resolver: &Resolver,
    connection: &Arc<crate::Connection>,
    database_id: usize,
) -> Result<()> {
    let schema_version = resolver.with_schema(database_id, |s| s.schema_version);
    program.begin_write_operation();
    let vtab_cur = program.alloc_cursor_id(CursorType::VirtualTable(vtab));
    program.emit_insn(Insn::VOpen {
        cursor_id: vtab_cur,
    });

    let new_name_reg = program.emit_string8_new_reg(new_name_norm.clone());
    program.emit_insn(Insn::VRename {
        cursor_id: vtab_cur,
        new_name_reg,
    });
    // Rewrite sqlite_schema entry
    let sqlite_schema = resolver
        .schema()
        .get_btree_table(SQLITE_TABLEID)
        .ok_or_else(|| {
            LimboError::ParseError("sqlite_schema table not found in schema".to_string())
        })?;

    let schema_cur = program.alloc_cursor_id(CursorType::BTreeTable(sqlite_schema.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: schema_cur,
        root_page: RegisterOrLiteral::Literal(sqlite_schema.root_page),
        db: database_id,
    });

    program.cursor_loop(schema_cur, |program, rowid| {
        let ncols = sqlite_schema.columns.len();
        turso_assert_eq!(ncols, 5);

        let first_col = program.alloc_registers(ncols);
        for i in 0..ncols {
            program.emit_column_or_rowid(schema_cur, i, first_col + i);
        }

        program.emit_string8_new_reg(old_name.to_string());
        program.mark_last_insn_constant();

        program.emit_string8_new_reg(new_name_norm.clone());
        program.mark_last_insn_constant();

        let out = program.alloc_registers(ncols);

        program.emit_insn(Insn::Function {
            constant_mask: 0,
            start_reg: first_col,
            dest: out,
            func: crate::function::FuncCtx {
                func: Func::AlterTable(AlterTableFunc::RenameTable),
                arg_count: 7,
            },
        });

        let rec = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u16(out),
            count: to_u16(ncols),
            dest_reg: to_u16(rec),
            index_name: None,
            affinity_str: None,
        });

        // In MVCC mode, we need to delete before insert to properly
        // end the old version (Hekaton-style UPDATE = DELETE + INSERT)
        if connection.mvcc_enabled() {
            program.emit_insn(Insn::Delete {
                cursor_id: schema_cur,
                table_name: SQLITE_TABLEID.to_string(),
                is_part_of_update: true,
            });
        }

        program.emit_insn(Insn::Insert {
            cursor: schema_cur,
            key_reg: rowid,
            record_reg: rec,
            flag: crate::vdbe::insn::InsertFlags(0),
            table_name: old_name.to_string(),
        });
    });

    // Bump schema cookie
    program.emit_insn(Insn::SetCookie {
        db: database_id,
        cookie: Cookie::SchemaVersion,
        value: schema_version as i32 + 1,
        p5: 0,
    });

    program.emit_insn(Insn::RenameTable {
        db: database_id,
        from: old_name.to_owned(),
        to: new_name_norm,
    });

    program.emit_insn(Insn::Close {
        cursor_id: schema_cur,
    });
    program.emit_insn(Insn::Close {
        cursor_id: vtab_cur,
    });

    Ok(())
}

/* Triggers must be rewritten when a column is renamed, and DROP COLUMN on table T must be forbidden if any trigger on T references the column.
Here are some helpers related to that: */
mod trigger_col_rename {
    use crate::schema::BTreeTable;
    use crate::translate::emitter::Resolver;
    use crate::util::{
        normalize_ident, walk_expr_scoped, walk_from_clause_expressions, walk_select,
        ScopedExprVisitor,
    };
    use crate::Result;
    use crate::{walk_expr_mut, LimboError, WalkControl};
    use std::sync::Arc;
    use turso_parser::ast;

    /// Check if a trigger contains qualified references to a specific column in its owning table.
    /// Rewrite trigger SQL to replace old column name with new column name.
    /// This handles old.x, new.x, and unqualified x references.
    pub(super) fn execute(
        trigger_sql: &str,
        table_name: &str,
        old_column_name: &str,
        new_column_name: &str,
        database_id: usize,
        resolver: &Resolver,
    ) -> Result<String> {
        use turso_parser::parser::Parser;

        // Parse the trigger SQL
        let mut parser = Parser::new(trigger_sql.as_bytes());
        let cmd = parser
            .next_cmd()
            .map_err(|e| LimboError::ParseError(format!("failed to parse trigger SQL: {e}")))?;
        let Some(ast::Cmd::Stmt(ast::Stmt::CreateTrigger {
            temporary,
            if_not_exists,
            trigger_name,
            time,
            event,
            tbl_name,
            for_each_row,
            when_clause,
            mut commands,
        })) = cmd
        else {
            return Err(LimboError::ParseError(format!(
                "failed to parse trigger SQL: {trigger_sql}"
            )));
        };

    let old_col_norm = normalize_ident(old_column_name);
    let new_col_norm = normalize_ident(new_column_name);

        // Get the trigger's owning table to check unqualified column references
        let trigger_table_name_raw = tbl_name.name.as_str();
        let trigger_table_name = normalize_ident(trigger_table_name_raw);
        let trigger_table = resolver
            .with_schema(database_id, |schema| {
                schema.get_btree_table(&trigger_table_name)
            })
            .ok_or_else(|| {
                LimboError::ParseError(format!("trigger table not found: {trigger_table_name}"))
            })?;

    let target_table_name = normalize_ident(table_name);

    // Rewrite UPDATE OF column list if renaming a column in the trigger's owning table
    let is_renaming_trigger_table = trigger_table_name == target_table_name;
    let new_event = if is_renaming_trigger_table {
        match event {
            ast::TriggerEvent::UpdateOf(mut cols) => {
                // Rewrite column names in UPDATE OF list
                for col in &mut cols {
                    let col_norm = normalize_ident(col.as_str());
                    if col_norm == old_col_norm {
                        *col = ast::Name::from_string(new_col_norm.clone());
                    }
                }
                ast::TriggerEvent::UpdateOf(cols)
            }
            other => other,
        }
    } else {
        event
    };

    // Rewrite WHEN clause column references if present.
    // In WHEN clauses, only NEW.col / OLD.col qualified references are valid;
    // bare column names are not valid in trigger WHEN clauses per SQLite semantics,
    // so we only rewrite qualified NEW/OLD references here.
    let new_when_clause = when_clause
        .map(|e| {
            let mut expr = *e;
            walk_expr_mut(
                &mut expr,
                &mut |ex: &mut ast::Expr| -> Result<WalkControl> {
                    if let ast::Expr::Qualified(ns, col) | ast::Expr::DoublyQualified(_, ns, col) =
                        ex
                    {
                        let ns_norm = normalize_ident(ns.as_str());
                        let col_norm = normalize_ident(col.as_str());
                        if (ns_norm.eq_ignore_ascii_case("new")
                            || ns_norm.eq_ignore_ascii_case("old"))
                            && col_norm == *old_col_norm
                            && is_renaming_trigger_table
                            && trigger_table.get_column(&col_norm).is_some()
                        {
                            *col = ast::Name::from_string(&*new_col_norm);
                        }
                    }
                    Ok(WalkControl::Continue)
                },
            )?;
            Ok::<Box<ast::Expr>, LimboError>(Box::new(expr))
        })
        .transpose()?;

    // Validate: if the WHEN clause still contains a bare reference to the old column,
    // SQLite would error with "no such column". We must do the same.
    if let Some(ref when_expr) = new_when_clause {
        let mut has_bare_old_col = false;
        let _ = walk_expr_mut(
            &mut when_expr.clone(),
            &mut |ex: &mut ast::Expr| -> Result<WalkControl> {
                if let ast::Expr::Id(ref name) | ast::Expr::Name(ref name) = ex {
                    if normalize_ident(name.as_str()) == *old_col_norm {
                        has_bare_old_col = true;
                    }
                }
                Ok(WalkControl::Continue)
            },
        );
        if has_bare_old_col {
            return Err(LimboError::ParseError(format!(
                "error in trigger {}: no such column: {}",
                trigger_name.name.as_str(),
                old_col_norm
            )));
        }
    }

        let trigger_table_name_norm = normalize_ident(trigger_table_name_raw);
        let is_renaming_trigger_table = trigger_table_name_norm == target_table_name;
        let mut rename = RenameColumnOperation {
            trigger_table: &trigger_table,
            trigger_table_name: trigger_table_name_raw,
            target_table_name: &target_table_name,
            old_col_norm: &old_col_norm,
            new_col_norm: &new_col_norm,
            is_renaming_trigger_table,
        };

        for cmd in &mut commands {
            rename.execute(cmd, database_id, resolver)?;
        }

        // Reconstruct the SQL
        use crate::translate::trigger::create_trigger_to_sql;
        let new_sql = create_trigger_to_sql(
            temporary,
            if_not_exists,
            &trigger_name,
            time,
            &new_event,
            &tbl_name,
            for_each_row,
            new_when_clause.as_deref(),
            &commands,
        );

        Ok(new_sql)
    }

    struct RenameColumnOperation<'a> {
        trigger_table: &'a BTreeTable,
        trigger_table_name: &'a str,
        target_table_name: &'a str,
        old_col_norm: &'a str,
        new_col_norm: &'a str,
        is_renaming_trigger_table: bool,
    }

    impl<'a> RenameColumnOperation<'a> {
        /// Rewrite a trigger command to rename column references
        fn execute(
            &mut self,
            cmd: &mut ast::TriggerCmd,
            database_id: usize,
            resolver: &Resolver,
        ) -> crate::Result<()> {
            match cmd {
                ast::TriggerCmd::Update {
                    tbl_name,
                    sets,
                    from,
                    where_clause,
                    ..
                } => {
                    // Get the UPDATE target table to check if we're renaming a column in it
                    let update_table_name_norm = normalize_ident(tbl_name.as_str());
                    let is_renaming_update_table =
                        update_table_name_norm == *self.target_table_name;

                    // Rewrite SET column names if renaming a column in the UPDATE target table
                    if is_renaming_update_table {
                        for set in sets.iter_mut() {
                            for col_name in &mut set.col_names {
                                let col_norm = normalize_ident(col_name.as_str());
                                if col_norm == *self.old_col_norm {
                                    *col_name = ast::Name::from_string(self.new_col_norm);
                                }
                            }
                        }
                    }

                    // Build context for UPDATE SET/WHERE: unqualified refs refer to UPDATE target
                    let context = resolve_rename_context(
                        &update_table_name_norm,
                        self.target_table_name,
                        database_id,
                        resolver,
                    )?;
                    let from_target =
                        if from_clause_references_table(from.as_ref(), self.target_table_name) {
                            Some(self.target_table_name.to_string())
                        } else {
                            None
                        };

                    let mut scope = RenameScope {
                        from_target,
                        context,
                    };
                    for set in sets.iter_mut() {
                        walk_expr_scoped(&mut set.expr, &mut scope, self)?;
                    }
                    if let Some(ref mut where_expr) = where_clause {
                        walk_expr_scoped(where_expr, &mut scope, self)?;
                    }
                    if let Some(ref mut from_clause) = from {
                        scope.context = None;
                        scope.from_target = None;
                        walk_from_clause_expressions(from_clause, &mut scope, self)?;
                    }
                }
                ast::TriggerCmd::Insert {
                    tbl_name,
                    col_names,
                    select,
                    upsert,
                    ..
                } => {
                    let insert_table_name_norm = normalize_ident(tbl_name.as_str());
                    if insert_table_name_norm == *self.target_table_name {
                        for col_name in col_names.iter_mut() {
                            let col_norm = normalize_ident(col_name.as_str());
                            if col_norm == *self.old_col_norm {
                                *col_name = ast::Name::from_string(self.new_col_norm);
                            }
                        }
                    }
                    let mut scope = RenameScope {
                        from_target: None,
                        context: None,
                    };
                    walk_select(select, &mut scope, self)?;
                    if let Some(ref mut upsert_box) = upsert {
                        self.rewrite_cols_in_upsert(
                            upsert_box,
                            tbl_name.as_str(),
                            database_id,
                            resolver,
                        )?;
                    }
                }
                ast::TriggerCmd::Delete {
                    tbl_name,
                    where_clause,
                } => {
                    let delete_table_name_norm = normalize_ident(tbl_name.as_str());
                    if let Some(ref mut where_expr) = where_clause {
                        let context = resolve_rename_context(
                            &delete_table_name_norm,
                            self.target_table_name,
                            database_id,
                            resolver,
                        )?;
                        let mut scope = RenameScope {
                            from_target: None,
                            context,
                        };
                        walk_expr_scoped(where_expr, &mut scope, self)?;
                    }
                }
                ast::TriggerCmd::Select(select) => {
                    let mut scope = RenameScope {
                        from_target: None,
                        context: None,
                    };
                    walk_select(select, &mut scope, self)?;
                }
            }
            Ok(())
        }

        /// Rewrite a single expression's column reference
        ///
        /// Handles column references in trigger expressions:
        /// - NEW.column and OLD.column: Always refer to the trigger's owning table
        /// - Qualified references (e.g., u.x): Refer to the specified table
        /// - Unqualified references (e.g., x): Resolution order:
        ///   1. If `self.context_table` is provided (UPDATE/DELETE WHERE clauses), check the context table first
        ///   2. Otherwise, check the trigger's owning table
        ///   3. If `scope.from_target` is provided (FROM clause has target table), rename the column
        ///
        fn rename_cols_in_expr(&self, e: &mut ast::Expr, scope: &RenameScope) -> Result<()> {
            let context_table = scope
                .context
                .as_ref()
                .map(|ctx| (ctx.table.as_ref(), &ctx.name_norm, ctx.is_renaming));

            match e {
                ast::Expr::Qualified(ns, col) | ast::Expr::DoublyQualified(_, ns, col) => {
                    let ns_norm = normalize_ident(ns.as_str());
                    let col_norm = normalize_ident(col.as_str());

                    // Check if this is NEW.column or OLD.column
                    if (ns_norm.eq_ignore_ascii_case("new") || ns_norm.eq_ignore_ascii_case("old"))
                        && col_norm == *self.old_col_norm
                    {
                        // NEW.x and OLD.x always refer to the trigger's owning table
                        if self.is_renaming_trigger_table
                            && self.trigger_table.get_column(&col_norm).is_some()
                        {
                            *col = ast::Name::from_string(self.new_col_norm);
                        }
                    } else if col_norm == *self.old_col_norm {
                        // This is a qualified column reference like u.x or t.x
                        // Check if it refers to the context table (UPDATE/DELETE target) or trigger table
                        if let Some((_, ctx_name_norm, is_renaming_ctx)) = context_table {
                            if ns_norm == *ctx_name_norm && is_renaming_ctx {
                                // Qualified reference to context table (e.g., u.x where u is UPDATE target)
                                *col = ast::Name::from_string(self.new_col_norm);
                            }
                        }
                        // Also check if it's a qualified reference to the trigger's owning table
                        // (e.g., t.x in a trigger on table t).
                        // Only do this when there's no context table (i.e., in a SELECT), or when
                        // the trigger table IS the context table. In UPDATE/DELETE on a different
                        // table, qualified refs to the trigger table (t.x) are invalid in SQLite
                        // — only NEW.x/OLD.x are allowed — so we must not rename them.
                        if self.is_renaming_trigger_table {
                            let trigger_table_name_norm = normalize_ident(self.trigger_table_name);
                            if ns_norm == trigger_table_name_norm
                                && self.trigger_table.get_column(&col_norm).is_some()
                            {
                                // If we're inside an UPDATE/DELETE targeting a different table,
                                // a qualified ref to the trigger table (t.x) is invalid in SQLite.
                                let ctx_is_different_table =
                                    context_table.as_ref().is_some_and(|(_, ctx_name, _)| {
                                        **ctx_name != trigger_table_name_norm
                                    });
                                if ctx_is_different_table {
                                    return Err(LimboError::ParseError(format!(
                                        "no such column: {}.{col_norm}",
                                        self.trigger_table_name
                                    )));
                                }
                                *col = ast::Name::from_string(self.new_col_norm);
                            }
                        }
                        // Check if it's a qualified reference to a FROM clause table that is the
                        // rename target (e.g., src.b in SELECT src.b FROM src)
                        if let Some(target_name) = scope.from_target.as_deref() {
                            if ns_norm == *target_name {
                                *col = ast::Name::from_string(self.new_col_norm);
                            }
                        }
                    }
                }
                ast::Expr::Id(col) => {
                    // Unqualified column reference
                    let col_norm = normalize_ident(col.as_str());
                    if col_norm == *self.old_col_norm {
                        // Check context table first (for UPDATE/DELETE WHERE clauses)
                        if let Some((ctx_table, _, is_renaming_ctx)) = context_table {
                            if ctx_table.get_column(&col_norm).is_some() {
                                // This refers to the context table (UPDATE/DELETE target)
                                if is_renaming_ctx {
                                    *e = ast::Expr::Id(ast::Name::from_string(self.new_col_norm));
                                }
                                return Ok(());
                            }
                        }
                        // Check trigger's owning table or FROM clause target table
                        if (self.is_renaming_trigger_table
                            && self.trigger_table.get_column(&col_norm).is_some())
                            || scope.from_target.is_some()
                        {
                            *e = ast::Expr::Id(ast::Name::from_string(self.new_col_norm));
                        }
                    }
                }
                _ => {}
            }
            Ok(())
        }

        fn rewrite_cols_in_upsert(
            &mut self,
            upsert: &mut ast::Upsert,
            insert_table_name: &str,
            database_id: usize,
            resolver: &Resolver,
        ) -> Result<()> {
            let insert_table_name_norm = normalize_ident(insert_table_name);
            let insert_targets_renamed_table = insert_table_name_norm == *self.target_table_name;
            let old_col_norm = self.old_col_norm;
            let new_col_norm = self.new_col_norm;

            let context = resolve_rename_context(
                &insert_table_name_norm,
                self.target_table_name,
                database_id,
                resolver,
            )?;
            let mut scope = RenameScope {
                from_target: None,
                context,
            };

            if let Some(index) = &mut upsert.index {
                for target in &mut index.targets {
                    walk_expr_scoped(&mut target.expr, &mut scope, self)?;
                    if insert_targets_renamed_table {
                        rename_excluded_column_refs(&mut target.expr, old_col_norm, new_col_norm)?;
                    }
                }
                if let Some(where_clause) = &mut index.where_clause {
                    walk_expr_scoped(where_clause, &mut scope, self)?;
                    if insert_targets_renamed_table {
                        rename_excluded_column_refs(where_clause, old_col_norm, new_col_norm)?;
                    }
                }
            }

            if let ast::UpsertDo::Set { sets, where_clause } = &mut upsert.do_clause {
                for set in sets {
                    if insert_targets_renamed_table {
                        for col_name in &mut set.col_names {
                            if normalize_ident(col_name.as_str()) == *old_col_norm {
                                *col_name = ast::Name::from_string(new_col_norm);
                            }
                        }
                    }
                    walk_expr_scoped(&mut set.expr, &mut scope, self)?;
                    if insert_targets_renamed_table {
                        rename_excluded_column_refs(&mut set.expr, old_col_norm, new_col_norm)?;
                    }
                }
                if let Some(expr) = where_clause {
                    walk_expr_scoped(expr, &mut scope, self)?;
                    if insert_targets_renamed_table {
                        rename_excluded_column_refs(expr, old_col_norm, new_col_norm)?;
                    }
                }
            }

            if let Some(next) = &mut upsert.next {
                self.rewrite_cols_in_upsert(next, insert_table_name, database_id, resolver)?;
            }

            Ok(())
        }
    }

    struct RenameScope {
        from_target: Option<String>,
        context: Option<CommandTargetTable>,
    }

    /// The target table of an UPDATE/DELETE command inside a trigger body.
    /// Bare column references in SET/WHERE resolve against this table,
    /// so we need it to decide whether to rewrite them during rename.
    struct CommandTargetTable {
        table: Arc<BTreeTable>,
        name_norm: String,
        /// Whether this table is the one being renamed.
        is_renaming: bool,
    }

    impl ScopedExprVisitor for RenameColumnOperation<'_> {
        type Scope = RenameScope;
        type ScopeGuard = RenameScope;

        fn push_scope(
            &mut self,
            scope: &mut RenameScope,
            from: Option<&ast::FromClause>,
        ) -> RenameScope {
            let from_target = if from_clause_references_table(from, self.target_table_name) {
                Some(self.target_table_name.to_string())
            } else {
                None
            };
            std::mem::replace(
                scope,
                RenameScope {
                    from_target,
                    context: None,
                },
            )
        }

        fn pop_scope(&mut self, scope: &mut RenameScope, guard: RenameScope) {
            *scope = guard;
        }

        fn visit_expr(
            &mut self,
            expr: &mut ast::Expr,
            scope: &RenameScope,
        ) -> crate::Result<WalkControl> {
            self.rename_cols_in_expr(expr, scope)?;
            Ok(WalkControl::Continue)
        }
    }

    /// Check if a FROM clause references the target table.
    fn from_clause_references_table(from: Option<&ast::FromClause>, table_name: &str) -> bool {
        let Some(from_clause) = from else {
            return false;
        };
        if select_is_table(&from_clause.select, table_name) {
            return true;
        }
        from_clause
            .joins
            .iter()
            .any(|join| select_is_table(&join.table, table_name))
    }

    /// Resolve the context table for UPDATE/DELETE cmd dispatch during rename.
    fn resolve_rename_context(
        cmd_table_name_norm: &str,
        target_table_name: &str,
        database_id: usize,
        resolver: &Resolver,
    ) -> Result<Option<CommandTargetTable>> {
        let is_renaming = cmd_table_name_norm == target_table_name;
        let table = resolver
            .with_schema(database_id, |schema| {
                schema.get_btree_table(cmd_table_name_norm)
            })
            .ok_or_else(|| {
                LimboError::ParseError(format!("context table not found: {cmd_table_name_norm}"))
            })?;
        Ok(Some(CommandTargetTable {
            table,
            name_norm: cmd_table_name_norm.to_string(),
            is_renaming,
        }))
    }

    fn rename_excluded_column_refs(
        expr: &mut ast::Expr,
        old_col_norm: &str,
        new_col_norm: &str,
    ) -> Result<()> {
        walk_expr_mut(
            expr,
            &mut |e: &mut ast::Expr| -> crate::Result<WalkControl> {
                if let ast::Expr::Qualified(ns, col) | ast::Expr::DoublyQualified(_, ns, col) = e {
                    if normalize_ident(ns.as_str()) == "excluded"
                        && normalize_ident(col.as_str()) == *old_col_norm
                    {
                        *col = ast::Name::from_string(new_col_norm);
                    }
                }
                Ok(WalkControl::Continue)
            },
        )?;
        Ok(())
    }

    fn select_is_table(select_table: &ast::SelectTable, target_table_name: &str) -> bool {
        matches!(
            select_table,
            ast::SelectTable::Table(name, _, _)
                if normalize_ident(name.name.as_str()) == *target_table_name
        )
    }
} // mod trigger_col_rename

/// Validate all column references in a trigger after a DROP COLUMN operation.
/// Like SQLite, this re-validates the entire trigger — any unresolvable column
/// reference (whether related to the drop or pre-existing) causes the drop to fail.
///
/// Returns `Some(column_name)` if a bad column reference is found, `None` if all OK.
fn validate_trigger_columns_after_drop(
    trigger: &crate::schema::Trigger,
    altered_table_norm: &str,
    post_drop_table: &BTreeTable,
    resolver: &Resolver,
    database_id: usize,
) -> Result<Option<String>> {
    use crate::util::{walk_expr_scoped, walk_select, ScopedExprVisitor};

    let trigger_table_norm = normalize_ident(&trigger.table_name);

    // Determine the trigger's owning table columns (post-drop if it's the altered table)
    let owning_table_columns: Option<Vec<String>> = if trigger_table_norm == *altered_table_norm {
        Some(
            post_drop_table
                .columns
                .iter()
                .filter_map(|c| c.name.as_deref().map(normalize_ident))
                .collect(),
        )
    } else {
        resolver.with_schema(database_id, |s| {
            s.get_table(&trigger_table_norm).and_then(|t| {
                t.btree().map(|bt| {
                    bt.columns
                        .iter()
                        .filter_map(|c| c.name.as_deref().map(normalize_ident))
                        .collect()
                })
            })
        })
    };

    struct DropColumnVisitor<'a> {
        owning_table_columns: &'a Option<Vec<String>>,
        altered_table_norm: &'a str,
        post_drop_table: &'a BTreeTable,
        resolver: &'a Resolver<'a>,
        database_id: usize,
        bad_ref: Option<String>,
    }

    impl ScopedExprVisitor for DropColumnVisitor<'_> {
        type Scope = Vec<String>;
        type ScopeGuard = usize;

        fn push_scope(&mut self, scope: &mut Vec<String>, from: Option<&ast::FromClause>) -> usize {
            let saved_len = scope.len();
            if let Some(from) = from {
                self.collect_from_clause_columns(&from.select, scope);
                for join in &from.joins {
                    self.collect_from_clause_columns(&join.table, scope);
                }
            }
            saved_len
        }

        fn pop_scope(&mut self, scope: &mut Vec<String>, guard: usize) {
            scope.truncate(guard);
        }

        fn visit_expr(&mut self, expr: &mut ast::Expr, scope: &Vec<String>) -> Result<WalkControl> {
            if self.bad_ref.is_some() {
                return Ok(WalkControl::Stop);
            }
            self.bad_ref = check_column_ref_valid(
                expr,
                scope,
                self.owning_table_columns,
                self.altered_table_norm,
                self.post_drop_table,
                self.resolver,
                self.database_id,
            );
            if self.bad_ref.is_some() {
                Ok(WalkControl::Stop)
            } else {
                Ok(WalkControl::Continue)
            }
        }
    }

    impl DropColumnVisitor<'_> {
        fn collect_from_clause_columns(
            &self,
            select_table: &ast::SelectTable,
            cols: &mut Vec<String>,
        ) {
            match select_table {
                ast::SelectTable::Table(name, _, _) | ast::SelectTable::TableCall(name, _, _) => {
                    let table_name = normalize_ident(name.name.as_str());
                    if let Some(table_cols) = get_table_columns(
                        &table_name,
                        self.altered_table_norm,
                        self.post_drop_table,
                        self.resolver,
                        self.database_id,
                    ) {
                        for c in table_cols {
                            if !cols.contains(&c) {
                                cols.push(c);
                            }
                        }
                    }
                }
                ast::SelectTable::Select(_, _) | ast::SelectTable::Sub(_, _) => {}
            }
        }
    }

    let mut visitor = DropColumnVisitor {
        owning_table_columns: &owning_table_columns,
        altered_table_norm,
        post_drop_table,
        resolver,
        database_id,
        bad_ref: None,
    };

    // Clone trigger data so we can pass &mut to the walker
    let mut when_clause = trigger.when_clause.clone();
    let mut commands = trigger.commands.clone();

    // Validate WHEN clause — NEW/OLD refs resolve against the trigger's owning table
    if let Some(ref mut when_expr) = when_clause {
        if let Some(ref cols) = owning_table_columns {
            let mut scope = cols.clone();
            walk_expr_scoped(when_expr, &mut scope, &mut visitor)?;
            if let Some(bad) = visitor.bad_ref.take() {
                return Ok(Some(bad));
            }
        }
    }

    for cmd in &mut commands {
        match cmd {
            ast::TriggerCmd::Update {
                tbl_name,
                sets,
                where_clause,
                ..
            } => {
                let cmd_table_norm = normalize_ident(tbl_name.as_str());
                let cmd_table_cols = get_table_columns(
                    &cmd_table_norm,
                    altered_table_norm,
                    post_drop_table,
                    resolver,
                    database_id,
                );
                // Check expressions in SET values and WHERE — these can reference
                // both the command target table and the trigger's owning table (via NEW/OLD).
                // Note: SET target column names are NOT checked here — SQLite defers
                // that validation to trigger execution time.
                let mut scope = merge_cols(&cmd_table_cols, &owning_table_columns);
                for set in sets {
                    walk_expr_scoped(&mut set.expr, &mut scope, &mut visitor)?;
                    if let Some(bad) = visitor.bad_ref.take() {
                        return Ok(Some(bad));
                    }
                }
                if let Some(ref mut where_expr) = where_clause {
                    walk_expr_scoped(where_expr, &mut scope, &mut visitor)?;
                    if let Some(bad) = visitor.bad_ref.take() {
                        return Ok(Some(bad));
                    }
                }
            }
            // Note: INSERT column lists are NOT checked — SQLite defers that
            // validation to trigger execution time. But expressions in
            // INSERT ... VALUES and INSERT ... SELECT are checked.
            ast::TriggerCmd::Insert { select, .. } => {
                let mut scope = merge_cols(&owning_table_columns, &None);
                walk_select(select, &mut scope, &mut visitor)?;
                if let Some(bad) = visitor.bad_ref.take() {
                    return Ok(Some(bad));
                }
            }
            ast::TriggerCmd::Delete {
                tbl_name,
                where_clause,
                ..
            } => {
                let cmd_table_norm = normalize_ident(tbl_name.as_str());
                let cmd_table_cols = get_table_columns(
                    &cmd_table_norm,
                    altered_table_norm,
                    post_drop_table,
                    resolver,
                    database_id,
                );
                if let Some(ref mut where_expr) = where_clause {
                    let mut scope = merge_cols(&cmd_table_cols, &owning_table_columns);
                    walk_expr_scoped(where_expr, &mut scope, &mut visitor)?;
                    if let Some(bad) = visitor.bad_ref.take() {
                        return Ok(Some(bad));
                    }
                }
            }
            ast::TriggerCmd::Select(select) => {
                let mut scope = merge_cols(&owning_table_columns, &None);
                walk_select(select, &mut scope, &mut visitor)?;
                if let Some(bad) = visitor.bad_ref.take() {
                    return Ok(Some(bad));
                }
            }
        }
    }

    Ok(None)
}

/// Check a single expression node for invalid column references after a DROP COLUMN.
/// Returns `Some(bad_column_description)` if invalid, `None` if OK.
fn check_column_ref_valid(
    e: &ast::Expr,
    valid_columns: &[String],
    owning_table_columns: &Option<Vec<String>>,
    altered_table_norm: &str,
    post_drop_table: &BTreeTable,
    resolver: &Resolver,
    database_id: usize,
) -> Option<String> {
    match e {
        ast::Expr::Id(col) => {
            let col_norm = normalize_ident(col.as_str());
            if !valid_columns.contains(&col_norm) {
                return Some(col.to_string());
            }
        }
        ast::Expr::Qualified(ns, col) | ast::Expr::DoublyQualified(_, ns, col) => {
            let ns_norm = normalize_ident(ns.as_str());
            let col_norm = normalize_ident(col.as_str());
            if ns_norm.eq_ignore_ascii_case("new") || ns_norm.eq_ignore_ascii_case("old") {
                // NEW.col / OLD.col — validate against owning table columns
                if let Some(ref cols) = owning_table_columns {
                    if !cols.contains(&col_norm) {
                        return Some(format!("{ns}.{col}"));
                    }
                }
            } else {
                // table.col — validate against that table's columns
                let table_cols = get_table_columns(
                    &ns_norm,
                    altered_table_norm,
                    post_drop_table,
                    resolver,
                    database_id,
                );
                if let Some(cols) = table_cols {
                    if !cols.contains(&col_norm) {
                        return Some(format!("{ns}.{col}"));
                    }
                }
            }
        }
        _ => {}
    }
    None
}

/// Get the column names for a table, using the post-drop schema if it's the altered table.
fn get_table_columns(
    table_name_norm: &str,
    altered_table_norm: &str,
    post_drop_table: &BTreeTable,
    resolver: &Resolver,
    database_id: usize,
) -> Option<Vec<String>> {
    if table_name_norm == altered_table_norm {
        Some(
            post_drop_table
                .columns
                .iter()
                .filter_map(|c| c.name.as_deref().map(normalize_ident))
                .collect(),
        )
    } else {
        resolver.with_schema(database_id, |s| {
            s.get_table(table_name_norm).and_then(|t| {
                t.btree().map(|bt| {
                    bt.columns
                        .iter()
                        .filter_map(|c| c.name.as_deref().map(normalize_ident))
                        .collect()
                })
            })
        })
    }
}

/// Merge two optional column lists into one combined list for expression validation.
fn merge_cols(a: &Option<Vec<String>>, b: &Option<Vec<String>>) -> Vec<String> {
    let mut result = Vec::new();
    if let Some(cols) = a {
        result.extend(cols.iter().cloned());
    }
    if let Some(cols) = b {
        for c in cols {
            if !result.contains(c) {
                result.push(c.clone());
            }
        }
    }
    result
}
