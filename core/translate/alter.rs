use crate::sync::Arc;
use crate::{turso_assert_eq, turso_assert_ne};
use turso_parser::{
    ast::{self, TableInternalId},
    parser::Parser,
};

use crate::{
    error::SQLITE_CONSTRAINT_CHECK,
    function::{AlterTableFunc, Func},
    schema::{CheckConstraint, Column, ForeignKey, Table, RESERVED_TABLE_PREFIXES},
    translate::{
        emitter::Resolver,
        expr::{rewrite_between_expr, translate_expr, walk_expr, walk_expr_mut, WalkControl},
        plan::{ColumnUsedMask, OuterQueryReference, TableReferences},
    },
    util::{
        check_expr_references_column, normalize_ident, parse_numeric_literal,
        rewrite_view_sql_for_column_rename,
    },
    vdbe::{
        affinity::Affinity,
        builder::{CursorType, ProgramBuilder},
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
    original_btree: &Arc<crate::schema::BTreeTable>,
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
    });

    program.resolve_label(skip_check_label, program.offset());
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
    btree: &crate::schema::BTreeTable,
    original_btree: &Arc<crate::schema::BTreeTable>,
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
        rewrite_between_expr(&mut substituted);

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

            // TODO: check usage in generated column when implemented

            // References in VIEWs are checked in the VDBE layer op_drop_column instruction.

            // Check if any trigger owned by this table references the column being dropped
            let triggers_for_table: Vec<_> = resolver.with_schema(database_id, |s| {
                s.get_triggers_for_table(table_name).cloned().collect()
            });
            for trigger in &triggers_for_table {
                if trigger_references_column(trigger, &btree, column_name)? {
                    return Err(LimboError::ParseError(format!(
                        "cannot drop column \"{column_name}\": it is referenced in trigger {}",
                        trigger.name
                    )));
                }
            }

            btree.columns.remove(dropped_index);

            let sql = btree.to_sql().replace('\'', "''");

            let stmt = format!(
                r#"
                    UPDATE {qualified_schema_table}
                    SET sql = '{sql}'
                    WHERE name = '{table_name}' COLLATE NOCASE AND type = 'table'
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
                    let column_count = btree.columns.len();
                    let root_page = btree.root_page;
                    let table_name = btree.name.clone();

                    let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(original_btree));

                    program.emit_insn(Insn::OpenWrite {
                        cursor_id,
                        root_page: RegisterOrLiteral::Literal(root_page),
                        db: database_id,
                    });

                    program.cursor_loop(cursor_id, |program, rowid| {
                        let first_column = program.alloc_registers(column_count);

                        let mut iter = first_column;

                        for i in 0..(column_count + 1) {
                            if i == dropped_index {
                                continue;
                            }

                            program.emit_column_or_rowid(cursor_id, i, iter);

                            iter += 1;
                        }

                        let record = program.alloc_register();

                        let affinity_str = btree
                            .columns
                            .iter()
                            .map(|col| col.affinity_with_strict(btree.is_strict).aff_mask())
                            .collect::<String>();

                        program.emit_insn(Insn::MakeRecord {
                            start_reg: to_u16(first_column),
                            count: to_u16(column_count),
                            dest_reg: to_u16(record),
                            index_name: None,
                            affinity_str: Some(affinity_str),
                        });

                        // In MVCC mode, we need to delete before insert to properly
                        // end the old version (Hekaton-style UPDATE = DELETE + INSERT)
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
            if col_def
                .constraints
                .iter()
                .any(|c| matches!(c.constraint, ast::ColumnConstraint::Generated { .. }))
            {
                return Err(LimboError::ParseError(
                    "Alter table does not support adding generated columns".to_string(),
                ));
            }
            let constraints = col_def.constraints.clone();
            let mut column = Column::try_from(&col_def)?;

            // SQLite is very strict about what constitutes a "constant" default for
            // ALTER TABLE ADD COLUMN. It only allows literals and signed literals,
            // not arbitrary constant expressions like (5 + 3) or COALESCE(NULL, 5).
            if column
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

            let stmt = format!(
                r#"
                    UPDATE {qualified_schema_table}
                    SET sql = '{escaped}'
                    WHERE name = '{table_name}' COLLATE NOCASE AND type = 'table'
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

            // Check if we need to verify the table is empty at runtime.
            // This is required for:
            // 1. NOT NULL columns without a non-null default (existing rows would get NULL)
            // 2. Non-deterministic defaults like CURRENT_TIME (can't backfill existing rows)
            // 3. CHECK constraints on the new column. SQLite evaluates the CHECK against
            //    all existing rows via pragma_quick_check. We take a stricter approach and
            //    reject the ALTER if the table has any rows, since we don't yet support
            //    scanning existing data for constraint validation.
            // Check if the column has an effective default (column-level or type-level).
            let effective_default = column.default.as_ref().or_else(|| {
                resolver
                    .schema()
                    .get_type_def(&column.ty_str, btree.is_strict)
                    .and_then(|td| td.default.as_ref())
            });
            let needs_notnull_check = column.notnull()
                && effective_default.is_none_or(|default| crate::util::expr_contains_null(default));

            let needs_nondeterministic_check = column
                .default
                .as_ref()
                .is_some_and(|default| default_requires_empty_table(default));

            let (needs_empty_table_check, error_message) =
                if needs_notnull_check && needs_nondeterministic_check {
                    // Both conditions - use NOT NULL message (more specific)
                    (true, "Cannot add a NOT NULL column with default value NULL")
                } else if needs_notnull_check {
                    (true, "Cannot add a NOT NULL column with default value NULL")
                } else if needs_nondeterministic_check {
                    (true, "Cannot add a column with non-constant default")
                } else {
                    (false, "")
                };

            if needs_empty_table_check {
                // Emit bytecode to check if the table has any rows.
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

                // Table has rows - emit error
                program.emit_insn(Insn::Halt {
                    err_code: 1,
                    description: error_message.to_string(),
                    on_error: None,
                });

                program.resolve_label(skip_error_label, program.offset());
            }

            if default_type_mismatch {
                emit_add_column_default_type_validation(program, &original_btree)?;
            }

            // Validate CHECK constraints against the DEFAULT value for existing rows.
            // When a column with a CHECK constraint is added and the table has rows,
            // the default value (or NULL if no DEFAULT) must satisfy the CHECK.
            // We substitute column references in the CHECK expression with the default
            // value and evaluate it. If the result is false, we reject the ALTER when
            // the table has rows.
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

            let is_making_column_generated = definition
                .constraints
                .iter()
                .any(|c| matches!(c.constraint, ast::ColumnConstraint::Generated { .. }));

            if is_making_column_generated {
                let non_generated_count = btree
                    .columns
                    .iter()
                    .enumerate()
                    .filter(|(idx, col)| *idx != column_index && col.generated.is_none())
                    .count();

                if non_generated_count == 0 {
                    return Err(LimboError::ParseError(
                        "must have at least one non-generated column".to_string(),
                    ));
                }
            }

            // If renaming, rewrite trigger SQL for all triggers that reference this column
            // We'll collect the triggers to rewrite and update them in sqlite_schema
            let mut triggers_to_rewrite: Vec<(String, String)> = Vec::new();
            let mut views_to_rewrite: Vec<(String, String)> = Vec::new();
            if rename {
                // Find all triggers that might reference this column
                let target_table_name_norm = normalize_ident(table_name);
                let all_triggers: Vec<_> = resolver.with_schema(database_id, |s| {
                    s.triggers.values().flatten().cloned().collect()
                });
                for trigger in &all_triggers {
                    let trigger_table_name_norm = normalize_ident(&trigger.table_name);

                    // SQLite fails RENAME COLUMN if a trigger's WHEN clause references the column
                    // or if trigger commands contain qualified references to the trigger table (e.g., t.x)
                    if trigger_table_name_norm == target_table_name_norm {
                        let column_name_norm = normalize_ident(from);
                        let trigger_table = resolver
                            .with_schema(database_id, |s| {
                                s.get_btree_table(&trigger_table_name_norm)
                            })
                            .ok_or_else(|| {
                                LimboError::ParseError(format!(
                                    "trigger table not found: {trigger_table_name_norm}"
                                ))
                            })?;

                        // Check WHEN clause
                        if let Some(ref when_expr) = trigger.when_clause {
                            if expr_references_trigger_column(
                                when_expr,
                                &trigger_table,
                                &trigger_table_name_norm,
                                &column_name_norm,
                            )? {
                                return Err(LimboError::ParseError(format!(
                                    "error in trigger {}: no such column: {}",
                                    trigger.name, from
                                )));
                            }
                        }

                        // Check trigger commands for qualified references to trigger table (e.g., t.x)
                        // SQLite fails RENAME COLUMN if triggers contain qualified references like t.x
                        if check_trigger_has_qualified_ref_to_column(
                            trigger,
                            &trigger_table,
                            &trigger_table_name_norm,
                            &column_name_norm,
                        )? {
                            return Err(LimboError::ParseError(format!(
                                "error in trigger {}: no such column: {}",
                                trigger.name, from
                            )));
                        }
                    }

                    // Check if trigger references the column being renamed
                    // This includes:
                    // 1. References from the trigger's owning table (NEW.x, OLD.x, unqualified x)
                    // 2. References in INSERT column lists targeting the table being renamed
                    // 3. References in UPDATE SET column lists targeting the table being renamed
                    let mut needs_rewrite = false;

                    if trigger_table_name_norm == target_table_name_norm {
                        // Trigger is on the table being renamed - check for references
                        let trigger_table = resolver
                            .with_schema(database_id, |s| {
                                s.get_btree_table(&trigger_table_name_norm)
                            })
                            .ok_or_else(|| {
                                LimboError::ParseError(format!(
                                    "trigger table not found: {trigger_table_name_norm}"
                                ))
                            })?;

                        needs_rewrite = trigger_references_column(trigger, &trigger_table, from)?;
                    }

                    // Also check if trigger references the column in INSERT/UPDATE targeting other tables
                    // Parse the trigger to check INSERT column lists and UPDATE SET column lists
                    if !needs_rewrite {
                        needs_rewrite = trigger_references_column_in_other_tables(
                            trigger,
                            &target_table_name_norm,
                            from,
                        )?;
                    }

                    if needs_rewrite {
                        match rewrite_trigger_sql_for_column_rename(
                            &trigger.sql,
                            table_name,
                            from,
                            col_name,
                            resolver,
                        ) {
                            Ok(new_sql) => {
                                triggers_to_rewrite.push((trigger.name.clone(), new_sql));
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
                }
            }

            if rename {
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
                let update_stmt = format!(
                    r#"
                        UPDATE {qualified_schema_table}
                        SET sql = '{escaped_sql}'
                        WHERE name = '{trigger_name}' COLLATE NOCASE AND type = 'trigger'
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
                        UPDATE {SQLITE_TABLEID}
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

/// Check if a trigger contains qualified references to a specific column in its owning table.
/// This is used to detect cases like `t.x` in a trigger on table `t`, which SQLite fails on RENAME COLUMN.
/// Only checks for qualified table references (e.g., t.x), not NEW.x, OLD.x, or unqualified x.
fn check_trigger_has_qualified_ref_to_column(
    trigger: &crate::schema::Trigger,
    trigger_table: &crate::schema::BTreeTable,
    trigger_table_name_norm: &str,
    column_name_norm: &str,
) -> Result<bool> {
    use crate::translate::expr::walk_expr;
    use std::cell::Cell;

    let found = Cell::new(false);

    // Helper callback to check for qualified references to trigger table
    let mut check_qualified_ref = |e: &ast::Expr| -> Result<crate::translate::expr::WalkControl> {
        if let ast::Expr::Qualified(ns, col) | ast::Expr::DoublyQualified(_, ns, col) = e {
            let ns_norm = normalize_ident(ns.as_str());
            let col_norm = normalize_ident(col.as_str());
            // Only check for qualified refs to trigger table (not NEW/OLD)
            if !ns_norm.eq_ignore_ascii_case("new")
                && !ns_norm.eq_ignore_ascii_case("old")
                && ns_norm == trigger_table_name_norm
                && col_norm == column_name_norm
                && trigger_table.get_column(&col_norm).is_some()
            {
                found.set(true);
                return Ok(crate::translate::expr::WalkControl::SkipChildren);
            }
        }
        Ok(crate::translate::expr::WalkControl::Continue)
    };

    for cmd in &trigger.commands {
        match cmd {
            ast::TriggerCmd::Update {
                sets, where_clause, ..
            } => {
                for set in sets {
                    walk_expr(&set.expr, &mut check_qualified_ref)?;
                    if found.get() {
                        return Ok(true);
                    }
                }
                if let Some(ref where_expr) = where_clause {
                    walk_expr(where_expr, &mut check_qualified_ref)?;
                    if found.get() {
                        return Ok(true);
                    }
                }
            }
            ast::TriggerCmd::Insert { select, .. } | ast::TriggerCmd::Select(select) => {
                // Walk through all expressions in SELECT checking for qualified refs
                if let Some(ref with_clause) = select.with {
                    for cte in &with_clause.ctes {
                        if walk_one_select_expressions_for_qualified_ref(
                            &cte.select.body.select,
                            &mut check_qualified_ref,
                        )? {
                            return Ok(true);
                        }
                    }
                }
                if walk_one_select_expressions_for_qualified_ref(
                    &select.body.select,
                    &mut check_qualified_ref,
                )? {
                    return Ok(true);
                }
                for compound in &select.body.compounds {
                    if walk_one_select_expressions_for_qualified_ref(
                        &compound.select,
                        &mut check_qualified_ref,
                    )? {
                        return Ok(true);
                    }
                }
                for sorted_col in &select.order_by {
                    walk_expr(&sorted_col.expr, &mut check_qualified_ref)?;
                    if found.get() {
                        return Ok(true);
                    }
                }
                if let Some(ref limit) = select.limit {
                    walk_expr(&limit.expr, &mut check_qualified_ref)?;
                    if found.get() {
                        return Ok(true);
                    }
                    if let Some(ref offset) = limit.offset {
                        walk_expr(offset, &mut check_qualified_ref)?;
                        if found.get() {
                            return Ok(true);
                        }
                    }
                }
            }
            ast::TriggerCmd::Delete { where_clause, .. } => {
                if let Some(ref where_expr) = where_clause {
                    walk_expr(where_expr, &mut check_qualified_ref)?;
                    if found.get() {
                        return Ok(true);
                    }
                }
            }
        }
    }

    Ok(false)
}

/// Helper to walk OneSelect expressions checking for qualified references
/// Uses a callback that can mutate the found flag through a closure
fn walk_one_select_expressions_for_qualified_ref<F>(
    one_select: &ast::OneSelect,
    check_fn: &mut F,
) -> Result<bool>
where
    F: FnMut(&ast::Expr) -> Result<crate::translate::expr::WalkControl>,
{
    use crate::translate::expr::walk_expr;

    match one_select {
        ast::OneSelect::Select {
            columns,
            from,
            where_clause,
            group_by,
            window_clause,
            ..
        } => {
            for col in columns {
                if let ast::ResultColumn::Expr(expr, _) = col {
                    if matches!(
                        walk_expr(expr, check_fn)?,
                        crate::translate::expr::WalkControl::SkipChildren
                    ) {
                        return Ok(true);
                    }
                }
            }
            if let Some(ref from_clause) = from {
                // Walk FROM clause expressions manually using walk_expr
                match from_clause.select.as_ref() {
                    ast::SelectTable::Select(select, _) => {
                        // Recursively check the subquery
                        if walk_one_select_expressions_for_qualified_ref(
                            &select.body.select,
                            check_fn,
                        )? {
                            return Ok(true);
                        }
                    }
                    ast::SelectTable::TableCall(_, args, _) => {
                        for arg in args {
                            if matches!(
                                walk_expr(arg, check_fn)?,
                                crate::translate::expr::WalkControl::SkipChildren
                            ) {
                                return Ok(true);
                            }
                        }
                    }
                    _ => {}
                }
                for join in &from_clause.joins {
                    if let Some(ast::JoinConstraint::On(ref expr)) = join.constraint {
                        let skip_children = matches!(
                            walk_expr(expr, check_fn)?,
                            crate::translate::expr::WalkControl::SkipChildren
                        );
                        if skip_children {
                            return Ok(true);
                        }
                    }
                }
            }
            if let Some(ref where_expr) = where_clause {
                if matches!(
                    walk_expr(where_expr, check_fn)?,
                    crate::translate::expr::WalkControl::SkipChildren
                ) {
                    return Ok(true);
                }
            }
            if let Some(ref group_by) = group_by {
                for expr in &group_by.exprs {
                    if matches!(
                        walk_expr(expr, check_fn)?,
                        crate::translate::expr::WalkControl::SkipChildren
                    ) {
                        return Ok(true);
                    }
                }
                if let Some(ref having_expr) = group_by.having {
                    if matches!(
                        walk_expr(having_expr, check_fn)?,
                        crate::translate::expr::WalkControl::SkipChildren
                    ) {
                        return Ok(true);
                    }
                }
            }
            for window_def in window_clause {
                for expr in &window_def.window.partition_by {
                    if matches!(
                        walk_expr(expr, check_fn)?,
                        crate::translate::expr::WalkControl::SkipChildren
                    ) {
                        return Ok(true);
                    }
                }
                for sorted_col in &window_def.window.order_by {
                    if matches!(
                        walk_expr(&sorted_col.expr, check_fn)?,
                        crate::translate::expr::WalkControl::SkipChildren
                    ) {
                        return Ok(true);
                    }
                }
            }
        }
        ast::OneSelect::Values(values) => {
            for row in values {
                for expr in row {
                    if matches!(
                        walk_expr(expr, check_fn)?,
                        crate::translate::expr::WalkControl::SkipChildren
                    ) {
                        return Ok(true);
                    }
                }
            }
        }
    }
    Ok(false)
}

/// Check if an expression references a specific column from a trigger's owning table.
/// Checks for NEW.column, OLD.column, unqualified column references, and qualified
/// references to the trigger table (e.g., t.x in a trigger on table t).
/// Returns true if the column is found, false otherwise.
fn expr_references_trigger_column(
    expr: &ast::Expr,
    table: &crate::schema::BTreeTable,
    trigger_table_name: &str,
    column_name_norm: &str,
) -> Result<bool> {
    use crate::translate::expr::walk_expr;
    let trigger_table_name_norm = normalize_ident(trigger_table_name);
    let mut found = false;
    walk_expr(expr, &mut |e: &ast::Expr| -> Result<
        crate::translate::expr::WalkControl,
    > {
        match e {
            ast::Expr::Qualified(ns, col) | ast::Expr::DoublyQualified(_, ns, col) => {
                let ns_norm = normalize_ident(ns.as_str());
                let col_norm = normalize_ident(col.as_str());

                // Check NEW.column or OLD.column
                if (ns_norm.eq_ignore_ascii_case("new") || ns_norm.eq_ignore_ascii_case("old"))
                    && col_norm == *column_name_norm
                {
                    found = true;
                    return Ok(crate::translate::expr::WalkControl::SkipChildren);
                }
                // Check qualified reference to trigger table (e.g., t.x)
                if ns_norm == trigger_table_name_norm
                    && col_norm == *column_name_norm
                    && table.get_column(&col_norm).is_some()
                {
                    found = true;
                    return Ok(crate::translate::expr::WalkControl::SkipChildren);
                }
            }
            ast::Expr::Id(col) => {
                // Unqualified column reference - check if it matches
                let col_norm = normalize_ident(col.as_str());
                if col_norm == *column_name_norm {
                    // Verify this column exists in the trigger's owning table
                    if table.get_column(&col_norm).is_some() {
                        found = true;
                        return Ok(crate::translate::expr::WalkControl::SkipChildren);
                    }
                }
            }
            _ => {}
        }
        Ok(crate::translate::expr::WalkControl::Continue)
    })?;
    Ok(found)
}

/// Check if a trigger references a specific column from its owning table.
/// Returns true if the column is referenced as old.x, new.x, or unqualified x.
fn trigger_references_column(
    trigger: &crate::schema::Trigger,
    table: &crate::schema::BTreeTable,
    column_name: &str,
) -> Result<bool> {
    let column_name_norm = normalize_ident(column_name);
    let mut found = false;

    // Check when_clause
    if let Some(ref when_expr) = trigger.when_clause {
        // Get trigger table name for checking qualified references
        let trigger_table_name = normalize_ident(&trigger.table_name);
        found = expr_references_trigger_column(
            when_expr,
            table,
            &trigger_table_name,
            &column_name_norm,
        )?;
    }

    if found {
        return Ok(true);
    }

    // Check all trigger commands
    // Note: We only check NEW.x, OLD.x, and unqualified x references in expressions.
    // INSERT column lists and UPDATE SET column lists are NOT checked here because
    // SQLite allows DROP COLUMN even when triggers reference columns in INSERT/UPDATE
    // column lists targeting the owning table. The error only occurs when the trigger
    // is actually executed.
    for cmd in &trigger.commands {
        match cmd {
            ast::TriggerCmd::Update {
                sets, where_clause, ..
            } => {
                // Check SET expressions (not column names in SET clause)
                let trigger_table_name = normalize_ident(&trigger.table_name);
                for set in sets {
                    if expr_references_trigger_column(
                        &set.expr,
                        table,
                        &trigger_table_name,
                        &column_name_norm,
                    )? {
                        found = true;
                        break;
                    }
                }
                // Check WHERE clause
                if !found {
                    if let Some(ref where_expr) = where_clause {
                        found = expr_references_trigger_column(
                            where_expr,
                            table,
                            &trigger_table_name,
                            &column_name_norm,
                        )?;
                    }
                }
            }
            ast::TriggerCmd::Insert { select, .. } => {
                // Check SELECT/VALUES expressions (not column names in INSERT clause)
                let trigger_table_name = normalize_ident(&trigger.table_name);
                walk_select_expressions(
                    select,
                    table,
                    &trigger_table_name,
                    &column_name_norm,
                    &mut found,
                )?;
            }
            ast::TriggerCmd::Delete { where_clause, .. } => {
                if let Some(ref where_expr) = where_clause {
                    let trigger_table_name = normalize_ident(&trigger.table_name);
                    found = expr_references_trigger_column(
                        where_expr,
                        table,
                        &trigger_table_name,
                        &column_name_norm,
                    )?;
                }
            }
            ast::TriggerCmd::Select(select) => {
                let trigger_table_name = normalize_ident(&trigger.table_name);
                walk_select_expressions(
                    select,
                    table,
                    &trigger_table_name,
                    &column_name_norm,
                    &mut found,
                )?;
            }
        }
        if found {
            break;
        }
    }

    Ok(found)
}

/// Helper to check if an expression references a column.
/// Used as a callback within walk_expr, so it mutates a found flag.
/// Also checks for qualified references to the trigger table (e.g., t.x).
fn check_column_ref(
    e: &ast::Expr,
    table: &crate::schema::BTreeTable,
    trigger_table_name: &str,
    column_name_norm: &str,
    found: &mut bool,
) -> Result<()> {
    let trigger_table_name_norm = normalize_ident(trigger_table_name);
    match e {
        ast::Expr::Qualified(ns, col) | ast::Expr::DoublyQualified(_, ns, col) => {
            let ns_norm = normalize_ident(ns.as_str());
            let col_norm = normalize_ident(col.as_str());

            let new_or_old = (ns_norm.eq_ignore_ascii_case("new")
                || ns_norm.eq_ignore_ascii_case("old"))
                && col_norm == *column_name_norm;
            let qualified_ref = ns_norm == trigger_table_name_norm
                && col_norm == *column_name_norm
                && table.get_column(&col_norm).is_some();
            if new_or_old || qualified_ref {
                *found = true;
            }
        }
        ast::Expr::Id(col) => {
            // Unqualified column reference - check if it matches
            let col_norm = normalize_ident(col.as_str());
            if col_norm == *column_name_norm {
                // Verify this column exists in the trigger's owning table
                if table.get_column(&col_norm).is_some() {
                    *found = true;
                }
            }
        }
        _ => {}
    }
    Ok(())
}

/// Check if a trigger references a column from a table other than its owning table.
/// This checks INSERT column lists and UPDATE SET column lists that target the table being renamed.
fn trigger_references_column_in_other_tables(
    trigger: &crate::schema::Trigger,
    target_table_name: &str,
    column_name: &str,
) -> Result<bool> {
    let column_name_norm = normalize_ident(column_name);
    let target_table_name_norm = normalize_ident(target_table_name);

    // Check all trigger commands for INSERT/UPDATE targeting the table being renamed
    for cmd in &trigger.commands {
        match cmd {
            ast::TriggerCmd::Insert {
                tbl_name,
                col_names,
                ..
            } => {
                // Check if INSERT targets the table being renamed
                let insert_table_name_norm = normalize_ident(tbl_name.as_str());
                if insert_table_name_norm == target_table_name_norm {
                    // Check if column name appears in INSERT column list
                    for col_name in col_names {
                        let col_norm = normalize_ident(col_name.as_str());
                        if col_norm == column_name_norm {
                            return Ok(true);
                        }
                    }
                }
            }
            ast::TriggerCmd::Update { tbl_name, sets, .. } => {
                // Check if UPDATE targets the table being renamed
                let update_table_name_norm = normalize_ident(tbl_name.as_str());
                if update_table_name_norm == target_table_name_norm {
                    // Check if column name appears in UPDATE SET column list
                    for set in sets {
                        for col_name in &set.col_names {
                            let col_norm = normalize_ident(col_name.as_str());
                            if col_norm == column_name_norm {
                                return Ok(true);
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }

    Ok(false)
}

/// Walk through all expressions in a SELECT statement
fn walk_select_expressions(
    select: &ast::Select,
    table: &crate::schema::BTreeTable,
    trigger_table_name: &str,
    column_name_norm: &str,
    found: &mut bool,
) -> Result<()> {
    // Check WITH clause (CTEs)
    if let Some(ref with_clause) = select.with {
        for cte in &with_clause.ctes {
            walk_select_expressions(
                &cte.select,
                table,
                trigger_table_name,
                column_name_norm,
                found,
            )?;
            if *found {
                return Ok(());
            }
        }
    }

    // Check main SELECT body
    walk_one_select_expressions(
        &select.body.select,
        table,
        trigger_table_name,
        column_name_norm,
        found,
    )?;
    if *found {
        return Ok(());
    }

    // Check compound SELECTs (UNION, EXCEPT, INTERSECT)
    for compound in &select.body.compounds {
        walk_one_select_expressions(
            &compound.select,
            table,
            trigger_table_name,
            column_name_norm,
            found,
        )?;
        if *found {
            return Ok(());
        }
    }

    // Check ORDER BY
    for sorted_col in &select.order_by {
        walk_expr(
            &sorted_col.expr,
            &mut |e: &ast::Expr| -> Result<WalkControl> {
                check_column_ref(e, table, trigger_table_name, column_name_norm, found)?;
                Ok(WalkControl::Continue)
            },
        )?;
        if *found {
            return Ok(());
        }
    }

    // Check LIMIT
    if let Some(ref limit) = select.limit {
        walk_expr(&limit.expr, &mut |e: &ast::Expr| -> Result<WalkControl> {
            check_column_ref(e, table, trigger_table_name, column_name_norm, found)?;
            Ok(WalkControl::Continue)
        })?;
        if *found {
            return Ok(());
        }
        if let Some(ref offset) = limit.offset {
            walk_expr(offset, &mut |e: &ast::Expr| -> Result<WalkControl> {
                check_column_ref(e, table, trigger_table_name, column_name_norm, found)?;
                Ok(WalkControl::Continue)
            })?;
            if *found {
                return Ok(());
            }
        }
    }

    Ok(())
}

/// Walk through all expressions in a OneSelect
fn walk_one_select_expressions(
    one_select: &ast::OneSelect,
    table: &crate::schema::BTreeTable,
    trigger_table_name: &str,
    column_name_norm: &str,
    found: &mut bool,
) -> Result<()> {
    match one_select {
        ast::OneSelect::Select {
            columns,
            from,
            where_clause,
            group_by,
            window_clause,
            ..
        } => {
            // Check columns
            for col in columns {
                if let ast::ResultColumn::Expr(expr, _) = col {
                    walk_expr(expr, &mut |e: &ast::Expr| -> Result<WalkControl> {
                        check_column_ref(e, table, trigger_table_name, column_name_norm, found)?;
                        Ok(WalkControl::Continue)
                    })?;
                    if *found {
                        return Ok(());
                    }
                }
            }

            // Check FROM clause and JOIN conditions
            if let Some(ref from_clause) = from {
                walk_from_clause_expressions(
                    from_clause,
                    table,
                    trigger_table_name,
                    column_name_norm,
                    found,
                )?;
                if *found {
                    return Ok(());
                }
            }

            // Check WHERE clause
            if let Some(ref where_expr) = where_clause {
                walk_expr(where_expr, &mut |e: &ast::Expr| -> Result<WalkControl> {
                    check_column_ref(e, table, trigger_table_name, column_name_norm, found)?;
                    Ok(WalkControl::Continue)
                })?;
                if *found {
                    return Ok(());
                }
            }

            // Check GROUP BY and HAVING
            if let Some(ref group_by) = group_by {
                for expr in &group_by.exprs {
                    walk_expr(expr, &mut |e: &ast::Expr| -> Result<WalkControl> {
                        check_column_ref(e, table, trigger_table_name, column_name_norm, found)?;
                        Ok(WalkControl::Continue)
                    })?;
                    if *found {
                        return Ok(());
                    }
                }
                if let Some(ref having_expr) = group_by.having {
                    walk_expr(having_expr, &mut |e: &ast::Expr| -> Result<WalkControl> {
                        check_column_ref(e, table, trigger_table_name, column_name_norm, found)?;
                        Ok(WalkControl::Continue)
                    })?;
                    if *found {
                        return Ok(());
                    }
                }
            }

            // Check WINDOW clause
            for window_def in window_clause {
                walk_window_expressions(
                    &window_def.window,
                    table,
                    trigger_table_name,
                    column_name_norm,
                    found,
                )?;
                if *found {
                    return Ok(());
                }
            }
        }
        ast::OneSelect::Values(values) => {
            for row in values {
                for expr in row {
                    walk_expr(expr, &mut |e: &ast::Expr| -> Result<WalkControl> {
                        check_column_ref(e, table, trigger_table_name, column_name_norm, found)?;
                        Ok(WalkControl::Continue)
                    })?;
                    if *found {
                        return Ok(());
                    }
                }
            }
        }
    }
    Ok(())
}

/// Walk through expressions in a FROM clause (including JOIN conditions)
fn walk_from_clause_expressions(
    from_clause: &ast::FromClause,
    table: &crate::schema::BTreeTable,
    trigger_table_name: &str,
    column_name_norm: &str,
    found: &mut bool,
) -> Result<()> {
    // Check main table (could be a subquery)
    walk_select_table_expressions(
        &from_clause.select,
        table,
        trigger_table_name,
        column_name_norm,
        found,
    )?;
    if *found {
        return Ok(());
    }

    // Check JOIN conditions
    for join in &from_clause.joins {
        walk_select_table_expressions(
            &join.table,
            table,
            trigger_table_name,
            column_name_norm,
            found,
        )?;
        if *found {
            return Ok(());
        }
        if let Some(ref constraint) = join.constraint {
            match constraint {
                ast::JoinConstraint::On(expr) => {
                    walk_expr(expr, &mut |e: &ast::Expr| -> Result<WalkControl> {
                        check_column_ref(e, table, trigger_table_name, column_name_norm, found)?;
                        Ok(WalkControl::Continue)
                    })?;
                    if *found {
                        return Ok(());
                    }
                }
                ast::JoinConstraint::Using(_) => {
                    // USING clause contains column names, not expressions
                }
            }
        }
    }
    Ok(())
}

/// Walk through expressions in a SelectTable (table, subquery, or table function)
fn walk_select_table_expressions(
    select_table: &ast::SelectTable,
    table: &crate::schema::BTreeTable,
    trigger_table_name: &str,
    column_name_norm: &str,
    found: &mut bool,
) -> Result<()> {
    match select_table {
        ast::SelectTable::Select(select, _) => {
            walk_select_expressions(select, table, trigger_table_name, column_name_norm, found)?;
        }
        ast::SelectTable::Sub(from_clause, _) => {
            walk_from_clause_expressions(
                from_clause,
                table,
                trigger_table_name,
                column_name_norm,
                found,
            )?;
        }
        ast::SelectTable::TableCall(_, args, _) => {
            for arg in args {
                walk_expr(arg, &mut |e: &ast::Expr| -> Result<WalkControl> {
                    check_column_ref(e, table, trigger_table_name, column_name_norm, found)?;
                    Ok(WalkControl::Continue)
                })?;
                if *found {
                    return Ok(());
                }
            }
        }
        ast::SelectTable::Table(_, _, _) => {
            // Table reference, no expressions
        }
    }
    Ok(())
}

/// Walk through expressions in a Window definition
fn walk_window_expressions(
    window: &ast::Window,
    table: &crate::schema::BTreeTable,
    trigger_table_name: &str,
    column_name_norm: &str,
    found: &mut bool,
) -> Result<()> {
    // Check PARTITION BY expressions
    for expr in &window.partition_by {
        walk_expr(expr, &mut |e: &ast::Expr| -> Result<WalkControl> {
            check_column_ref(e, table, trigger_table_name, column_name_norm, found)?;
            Ok(WalkControl::Continue)
        })?;
        if *found {
            return Ok(());
        }
    }

    // Check ORDER BY expressions
    for sorted_col in &window.order_by {
        walk_expr(
            &sorted_col.expr,
            &mut |e: &ast::Expr| -> Result<WalkControl> {
                check_column_ref(e, table, trigger_table_name, column_name_norm, found)?;
                Ok(WalkControl::Continue)
            },
        )?;
        if *found {
            return Ok(());
        }
    }

    // TODO: FrameClause can also contain expressions, but they're more complex
    // For now, we'll skip them as they're less common in triggers
    Ok(())
}

/// Rewrite trigger SQL to replace old column name with new column name.
/// This handles old.x, new.x, and unqualified x references.
fn rewrite_trigger_sql_for_column_rename(
    trigger_sql: &str,
    table_name: &str,
    old_column_name: &str,
    new_column_name: &str,
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
        commands,
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
        .schema()
        .get_btree_table(&trigger_table_name)
        .ok_or_else(|| {
            LimboError::ParseError(format!("trigger table not found: {trigger_table_name}"))
        })?;

    // Check if this trigger references the column being renamed
    // We need to check if the column exists in the table being renamed
    let target_table_name = normalize_ident(table_name);
    let target_table = resolver
        .schema()
        .get_btree_table(&target_table_name)
        .ok_or_else(|| {
            LimboError::ParseError(format!("target table not found: {target_table_name}"))
        })?;

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

    // Note: SQLite fails RENAME COLUMN if a trigger's WHEN clause references the column.
    // We check for this earlier and fail the operation immediately, matching SQLite.
    // If we reach here, the WHEN clause doesn't reference the column, so we keep it unchanged.

    let mut new_commands = Vec::new();
    for cmd in commands {
        let new_cmd = rewrite_trigger_cmd_for_column_rename(
            cmd,
            &trigger_table,
            &target_table,
            trigger_table_name_raw,
            &target_table_name,
            &old_col_norm,
            &new_col_norm,
            resolver,
        )?;
        new_commands.push(new_cmd);
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
        when_clause.as_deref(),
        &new_commands,
    );

    Ok(new_sql)
}

/// Rewrite an expression to replace column references
///
/// `context_table_name` is used for UPDATE/DELETE WHERE clauses where unqualified column
/// references refer to the UPDATE/DELETE target table, not the trigger's owning table.
/// If `None`, unqualified references refer to the trigger's owning table.
#[allow(clippy::too_many_arguments)]
fn rewrite_expr_for_column_rename(
    expr: &ast::Expr,
    trigger_table: &crate::schema::BTreeTable,
    trigger_table_name: &str,
    target_table_name: &str,
    old_col_norm: &str,
    new_col_norm: &str,
    context_table_name: Option<&str>,
    resolver: &Resolver,
) -> Result<ast::Expr> {
    use crate::translate::expr::walk_expr_mut;

    let trigger_table_name_norm = normalize_ident(trigger_table_name);
    let target_table_name_norm = normalize_ident(target_table_name);
    let is_renaming_trigger_table = trigger_table_name_norm == target_table_name_norm;

    // Get context table if provided (for UPDATE/DELETE WHERE clauses)
    let context_table_info: Option<(crate::sync::Arc<crate::schema::BTreeTable>, String, bool)> =
        if let Some(ctx_name) = context_table_name {
            let ctx_name_norm = normalize_ident(ctx_name);
            let is_renaming = ctx_name_norm == target_table_name_norm;
            let table = resolver
                .schema()
                .get_btree_table(&ctx_name_norm)
                .ok_or_else(|| {
                    LimboError::ParseError(format!("context table not found: {ctx_name_norm}"))
                })?;
            Some((table, ctx_name_norm, is_renaming))
        } else {
            None
        };

    let mut expr = expr.clone();
    walk_expr_mut(&mut expr, &mut |e: &mut ast::Expr| -> Result<WalkControl> {
        rewrite_expr_column_ref_with_context(
            e,
            trigger_table,
            trigger_table_name,
            old_col_norm,
            new_col_norm,
            is_renaming_trigger_table,
            context_table_info
                .as_ref()
                .map(|(t, n, r)| (t.as_ref(), n, *r)),
        )?;
        Ok(WalkControl::Continue)
    })?;

    Ok(expr)
}

/// Rewrite a trigger command to replace column references
#[allow(clippy::too_many_arguments)]
fn rewrite_trigger_cmd_for_column_rename(
    cmd: ast::TriggerCmd,
    trigger_table: &crate::schema::BTreeTable,
    target_table: &crate::schema::BTreeTable,
    trigger_table_name: &str,
    target_table_name: &str,
    old_col_norm: &str,
    new_col_norm: &str,
    resolver: &Resolver,
) -> Result<ast::TriggerCmd> {
    use crate::translate::expr::walk_expr_mut;

    match cmd {
        ast::TriggerCmd::Update {
            or_conflict,
            tbl_name,
            mut sets,
            from,
            where_clause,
        } => {
            // Get the UPDATE target table to check if we're renaming a column in it
            let update_table_name_norm = normalize_ident(tbl_name.as_str());
            let is_renaming_update_table = update_table_name_norm == *target_table_name;

            // Rewrite SET column names if renaming a column in the UPDATE target table
            if is_renaming_update_table {
                for set in &mut sets {
                    for col_name in &mut set.col_names {
                        let col_norm = normalize_ident(col_name.as_str());
                        if col_norm == *old_col_norm {
                            *col_name = ast::Name::from_string(new_col_norm);
                        }
                    }
                }
            }

            // Rewrite SET expressions
            for set in &mut sets {
                walk_expr_mut(
                    &mut set.expr,
                    &mut |e: &mut ast::Expr| -> Result<WalkControl> {
                        rewrite_expr_column_ref(
                            e,
                            trigger_table,
                            trigger_table_name,
                            target_table_name,
                            old_col_norm,
                            new_col_norm,
                        )?;
                        Ok(WalkControl::Continue)
                    },
                )?;
            }

            // Rewrite WHERE clause - unqualified column references refer to UPDATE target table
            let new_where = where_clause
                .map(|e| {
                    rewrite_expr_for_column_rename(
                        &e,
                        trigger_table,
                        trigger_table_name,
                        target_table_name,
                        old_col_norm,
                        new_col_norm,
                        Some(&update_table_name_norm), // UPDATE WHERE: unqualified refs refer to UPDATE target
                        resolver,
                    )
                    .map(Box::new)
                })
                .transpose()?;
            Ok(ast::TriggerCmd::Update {
                or_conflict,
                tbl_name,
                sets,
                from,
                where_clause: new_where,
            })
        }
        ast::TriggerCmd::Insert {
            or_conflict,
            tbl_name,
            mut col_names,
            select,
            upsert,
            returning,
        } => {
            // Rewrite column names in INSERT column list
            // Check if the INSERT is targeting the table being renamed
            let insert_table_name_norm = normalize_ident(tbl_name.as_str());
            if insert_table_name_norm == *target_table_name {
                // This INSERT targets the table being renamed, so rewrite column names
                for col_name in &mut col_names {
                    let col_norm = normalize_ident(col_name.as_str());
                    if col_norm == *old_col_norm {
                        *col_name = ast::Name::from_string(new_col_norm);
                    }
                }
            }
            // Rewrite SELECT expressions
            let new_select = rewrite_select_for_column_rename(
                select,
                trigger_table,
                target_table,
                trigger_table_name,
                target_table_name,
                old_col_norm,
                new_col_norm,
            )?;
            Ok(ast::TriggerCmd::Insert {
                or_conflict,
                tbl_name,
                col_names,
                select: new_select,
                upsert,
                returning,
            })
        }
        ast::TriggerCmd::Delete {
            tbl_name,
            where_clause,
        } => {
            // Get the DELETE target table to check if we're renaming a column in it
            let delete_table_name_norm = normalize_ident(tbl_name.as_str());

            // Rewrite WHERE clause - unqualified column references refer to DELETE target table
            let new_where = where_clause
                .map(|e| {
                    rewrite_expr_for_column_rename(
                        &e,
                        trigger_table,
                        trigger_table_name,
                        target_table_name,
                        old_col_norm,
                        new_col_norm,
                        Some(&delete_table_name_norm), // DELETE WHERE: unqualified refs refer to DELETE target
                        resolver,
                    )
                    .map(Box::new)
                })
                .transpose()?;
            Ok(ast::TriggerCmd::Delete {
                tbl_name,
                where_clause: new_where,
            })
        }
        ast::TriggerCmd::Select(select) => {
            let new_select = rewrite_select_for_column_rename(
                select,
                trigger_table,
                target_table,
                trigger_table_name,
                target_table_name,
                old_col_norm,
                new_col_norm,
            )?;
            Ok(ast::TriggerCmd::Select(new_select))
        }
    }
}

/// Rewrite a SELECT statement to replace column references
fn rewrite_select_for_column_rename(
    select: ast::Select,
    trigger_table: &crate::schema::BTreeTable,
    _target_table: &crate::schema::BTreeTable,
    trigger_table_name: &str,
    target_table_name: &str,
    old_col_norm: &str,
    new_col_norm: &str,
) -> Result<ast::Select> {
    use crate::translate::expr::walk_expr_mut;

    let mut rewrite_cb = |e: &mut ast::Expr| -> Result<WalkControl> {
        rewrite_expr_column_ref(
            e,
            trigger_table,
            trigger_table_name,
            target_table_name,
            old_col_norm,
            new_col_norm,
        )?;
        Ok(WalkControl::Continue)
    };

    let mut select = select;

    // Rewrite WITH clause (CTEs)
    // Note: We clone here because Select doesn't implement Default, so we can't use mem::take.
    // The clone is necessary to move ownership to rewrite_select_for_column_rename.
    if let Some(ref mut with_clause) = select.with {
        for cte in &mut with_clause.ctes {
            cte.select = rewrite_select_for_column_rename(
                cte.select.clone(),
                trigger_table,
                _target_table,
                trigger_table_name,
                target_table_name,
                old_col_norm,
                new_col_norm,
            )?;
        }
    }

    // Rewrite main SELECT body
    rewrite_one_select_for_column_rename(
        &mut select.body.select,
        trigger_table,
        trigger_table_name,
        target_table_name,
        old_col_norm,
        new_col_norm,
    )?;

    // Rewrite compound SELECTs (UNION, EXCEPT, INTERSECT)
    for compound in &mut select.body.compounds {
        rewrite_one_select_for_column_rename(
            &mut compound.select,
            trigger_table,
            trigger_table_name,
            target_table_name,
            old_col_norm,
            new_col_norm,
        )?;
    }

    // Rewrite ORDER BY
    for sorted_col in &mut select.order_by {
        walk_expr_mut(&mut sorted_col.expr, &mut rewrite_cb)?;
    }

    // Rewrite LIMIT
    if let Some(ref mut limit) = select.limit {
        walk_expr_mut(&mut limit.expr, &mut rewrite_cb)?;
        if let Some(ref mut offset) = limit.offset {
            walk_expr_mut(offset, &mut rewrite_cb)?;
        }
    }

    Ok(select)
}

/// Rewrite a OneSelect to replace column references
fn rewrite_one_select_for_column_rename(
    one_select: &mut ast::OneSelect,
    trigger_table: &crate::schema::BTreeTable,
    trigger_table_name: &str,
    target_table_name: &str,
    old_col_norm: &str,
    new_col_norm: &str,
) -> Result<()> {
    use crate::translate::expr::walk_expr_mut;

    let mut rewrite_cb = |e: &mut ast::Expr| -> Result<WalkControl> {
        rewrite_expr_column_ref(
            e,
            trigger_table,
            trigger_table_name,
            target_table_name,
            old_col_norm,
            new_col_norm,
        )?;
        Ok(WalkControl::Continue)
    };

    match one_select {
        ast::OneSelect::Select {
            columns,
            from,
            where_clause,
            group_by,
            window_clause,
            ..
        } => {
            // Rewrite columns
            for col in columns {
                if let ast::ResultColumn::Expr(expr, _) = col {
                    walk_expr_mut(expr, &mut rewrite_cb)?;
                }
            }

            // Rewrite FROM clause and JOIN conditions
            if let Some(ref mut from_clause) = from {
                rewrite_from_clause_for_column_rename(
                    from_clause,
                    trigger_table,
                    trigger_table_name,
                    target_table_name,
                    old_col_norm,
                    new_col_norm,
                )?;
            }

            // Rewrite WHERE clause
            if let Some(ref mut where_expr) = where_clause {
                walk_expr_mut(where_expr, &mut rewrite_cb)?;
            }

            // Rewrite GROUP BY and HAVING
            if let Some(ref mut group_by) = group_by {
                for expr in &mut group_by.exprs {
                    walk_expr_mut(expr, &mut rewrite_cb)?;
                }
                if let Some(ref mut having_expr) = group_by.having {
                    walk_expr_mut(having_expr, &mut rewrite_cb)?;
                }
            }

            // Rewrite WINDOW clause
            for window_def in window_clause {
                rewrite_window_for_column_rename(
                    &mut window_def.window,
                    trigger_table,
                    trigger_table_name,
                    target_table_name,
                    old_col_norm,
                    new_col_norm,
                )?;
            }
        }
        ast::OneSelect::Values(values) => {
            for row in values {
                for expr in row {
                    walk_expr_mut(expr, &mut rewrite_cb)?;
                }
            }
        }
    }
    Ok(())
}

/// Rewrite expressions in a FROM clause (including JOIN conditions)
fn rewrite_from_clause_for_column_rename(
    from_clause: &mut ast::FromClause,
    trigger_table: &crate::schema::BTreeTable,
    trigger_table_name: &str,
    target_table_name: &str,
    old_col_norm: &str,
    new_col_norm: &str,
) -> Result<()> {
    use crate::translate::expr::walk_expr_mut;

    let mut rewrite_cb = |e: &mut ast::Expr| -> Result<WalkControl> {
        rewrite_expr_column_ref(
            e,
            trigger_table,
            trigger_table_name,
            target_table_name,
            old_col_norm,
            new_col_norm,
        )?;
        Ok(WalkControl::Continue)
    };

    // Rewrite main table (could be a subquery)
    rewrite_select_table_for_column_rename(
        &mut from_clause.select,
        trigger_table,
        trigger_table_name,
        target_table_name,
        old_col_norm,
        new_col_norm,
    )?;

    // Rewrite JOIN conditions
    for join in &mut from_clause.joins {
        rewrite_select_table_for_column_rename(
            &mut join.table,
            trigger_table,
            trigger_table_name,
            target_table_name,
            old_col_norm,
            new_col_norm,
        )?;
        if let Some(ref mut constraint) = join.constraint {
            match constraint {
                ast::JoinConstraint::On(expr) => {
                    walk_expr_mut(expr, &mut rewrite_cb)?;
                }
                ast::JoinConstraint::Using(_) => {
                    // USING clause contains column names, not expressions
                }
            }
        }
    }
    Ok(())
}

/// Rewrite expressions in a SelectTable (table, subquery, or table function)
fn rewrite_select_table_for_column_rename(
    select_table: &mut ast::SelectTable,
    trigger_table: &crate::schema::BTreeTable,
    trigger_table_name: &str,
    target_table_name: &str,
    old_col_norm: &str,
    new_col_norm: &str,
) -> Result<()> {
    use crate::translate::expr::walk_expr_mut;

    let mut rewrite_cb = |e: &mut ast::Expr| -> Result<WalkControl> {
        rewrite_expr_column_ref(
            e,
            trigger_table,
            trigger_table_name,
            target_table_name,
            old_col_norm,
            new_col_norm,
        )?;
        Ok(WalkControl::Continue)
    };

    match select_table {
        ast::SelectTable::Select(select, _) => {
            // Note: We clone here because Select doesn't implement Default, so we can't use mem::take.
            // The clone is necessary to move ownership to rewrite_select_for_column_rename.
            *select = rewrite_select_for_column_rename(
                select.clone(),
                trigger_table,
                trigger_table, // target_table not needed for subqueries
                trigger_table_name,
                target_table_name,
                old_col_norm,
                new_col_norm,
            )?;
        }
        ast::SelectTable::Sub(from_clause, _) => {
            rewrite_from_clause_for_column_rename(
                from_clause,
                trigger_table,
                trigger_table_name,
                target_table_name,
                old_col_norm,
                new_col_norm,
            )?;
        }
        ast::SelectTable::TableCall(_, args, _) => {
            for arg in args {
                walk_expr_mut(arg, &mut rewrite_cb)?;
            }
        }
        ast::SelectTable::Table(_, _, _) => {
            // Table reference, no expressions
        }
    }
    Ok(())
}

/// Rewrite expressions in a Window definition
fn rewrite_window_for_column_rename(
    window: &mut ast::Window,
    trigger_table: &crate::schema::BTreeTable,
    trigger_table_name: &str,
    target_table_name: &str,
    old_col_norm: &str,
    new_col_norm: &str,
) -> Result<()> {
    use crate::translate::expr::walk_expr_mut;

    let mut rewrite_cb = |e: &mut ast::Expr| -> Result<WalkControl> {
        rewrite_expr_column_ref(
            e,
            trigger_table,
            trigger_table_name,
            target_table_name,
            old_col_norm,
            new_col_norm,
        )?;
        Ok(WalkControl::Continue)
    };

    // Rewrite PARTITION BY expressions
    for expr in &mut window.partition_by {
        walk_expr_mut(expr, &mut rewrite_cb)?;
    }

    // Rewrite ORDER BY expressions
    for sorted_col in &mut window.order_by {
        walk_expr_mut(&mut sorted_col.expr, &mut rewrite_cb)?;
    }

    // TODO: FrameClause can also contain expressions, but they're more complex
    // For now, we'll skip them as they're less common in triggers
    Ok(())
}

/// Rewrite a single expression's column reference
///
/// Handles column references in trigger expressions:
/// - NEW.column and OLD.column: Always refer to the trigger's owning table
/// - Qualified references (e.g., u.x): Refer to the specified table
/// - Unqualified references (e.g., x): Resolution order:
///   1. If `context_table` is provided (UPDATE/DELETE WHERE clauses), check the context table first
///   2. Otherwise, check the trigger's owning table
///
/// This matches SQLite's column resolution order where unqualified columns in UPDATE/DELETE
/// WHERE clauses refer to the target table, not the trigger's owning table.
///
/// `context_table`: Optional tuple of (table, normalized_name, is_renaming) for UPDATE/DELETE
///                  target tables. If `None`, unqualified references refer to the trigger's owning table.
fn rewrite_expr_column_ref_with_context(
    e: &mut ast::Expr,
    trigger_table: &crate::schema::BTreeTable,
    trigger_table_name: &str,
    old_col_norm: &str,
    new_col_norm: &str,
    is_renaming_trigger_table: bool,
    context_table: Option<(&crate::schema::BTreeTable, &String, bool)>,
) -> Result<()> {
    match e {
        ast::Expr::Qualified(ns, col) | ast::Expr::DoublyQualified(_, ns, col) => {
            let ns_norm = normalize_ident(ns.as_str());
            let col_norm = normalize_ident(col.as_str());

            // Check if this is NEW.column or OLD.column
            if (ns_norm.eq_ignore_ascii_case("new") || ns_norm.eq_ignore_ascii_case("old"))
                && col_norm == *old_col_norm
            {
                // NEW.x and OLD.x always refer to the trigger's owning table
                if is_renaming_trigger_table && trigger_table.get_column(&col_norm).is_some() {
                    *col = ast::Name::from_string(new_col_norm);
                }
            } else if col_norm == *old_col_norm {
                // This is a qualified column reference like u.x or t.x
                // Check if it refers to the context table (UPDATE/DELETE target) or trigger table
                if let Some((_, ctx_name_norm, is_renaming_ctx)) = context_table {
                    if ns_norm == *ctx_name_norm && is_renaming_ctx {
                        // Qualified reference to context table (e.g., u.x where u is UPDATE target)
                        *col = ast::Name::from_string(new_col_norm);
                    }
                }
                // Also check if it's a qualified reference to the trigger's owning table
                // (e.g., t.x in a trigger on table t)
                if is_renaming_trigger_table {
                    let trigger_table_name_norm = normalize_ident(trigger_table_name);
                    if ns_norm == trigger_table_name_norm
                        && trigger_table.get_column(&col_norm).is_some()
                    {
                        *col = ast::Name::from_string(new_col_norm);
                    }
                }
            }
        }
        ast::Expr::Id(col) => {
            // Unqualified column reference
            let col_norm = normalize_ident(col.as_str());
            if col_norm == *old_col_norm {
                // Check context table first (for UPDATE/DELETE WHERE clauses)
                if let Some((ctx_table, _, is_renaming_ctx)) = context_table {
                    if ctx_table.get_column(&col_norm).is_some() {
                        // This refers to the context table (UPDATE/DELETE target)
                        if is_renaming_ctx {
                            *e = ast::Expr::Id(ast::Name::from_string(new_col_norm));
                        }
                        return Ok(());
                    }
                }
                // Otherwise, check trigger's owning table
                if is_renaming_trigger_table && trigger_table.get_column(&col_norm).is_some() {
                    *e = ast::Expr::Id(ast::Name::from_string(new_col_norm));
                }
            }
        }
        _ => {}
    }
    Ok(())
}

/// Rewrite a single expression's column reference (convenience wrapper for non-context cases)
fn rewrite_expr_column_ref(
    e: &mut ast::Expr,
    trigger_table: &crate::schema::BTreeTable,
    trigger_table_name: &str,
    target_table_name: &str,
    old_col_norm: &str,
    new_col_norm: &str,
) -> Result<()> {
    let trigger_table_name_norm = normalize_ident(trigger_table_name);
    let target_table_name_norm = normalize_ident(target_table_name);
    let is_renaming_trigger_table = trigger_table_name_norm == target_table_name_norm;

    rewrite_expr_column_ref_with_context(
        e,
        trigger_table,
        trigger_table_name,
        old_col_norm,
        new_col_norm,
        is_renaming_trigger_table,
        None,
    )
}
