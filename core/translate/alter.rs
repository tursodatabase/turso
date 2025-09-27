use std::sync::Arc;
use turso_parser::{
    ast::{self, TableInternalId},
    parser::Parser,
};

use crate::{
    function::{AlterTableFunc, Func},
    schema::{Column, Table},
    translate::{
        emitter::Resolver,
        expr::{walk_expr, WalkControl},
        plan::{ColumnUsedMask, OuterQueryReference, TableReferences},
    },
    util::normalize_ident,
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::{Cookie, Insn, RegisterOrLiteral},
    },
    LimboError, Result,
};

use super::{schema::SQLITE_TABLEID, update::translate_update_for_schema_change};

pub fn translate_alter_table(
    alter: ast::AlterTable,
    resolver: &Resolver,
    mut program: ProgramBuilder,
    connection: &Arc<crate::Connection>,
    input: &str,
) -> Result<ProgramBuilder> {
    program.begin_write_operation();
    let ast::AlterTable {
        name: table_name,
        body: alter_table,
    } = alter;
    let table_name = table_name.name.as_str();

    // Check if someone is trying to ALTER a system table
    if crate::schema::is_system_table(table_name) {
        crate::bail_parse_error!("table {} may not be modified", table_name);
    }

    let table_indexes = resolver.schema.get_indices(table_name).collect::<Vec<_>>();

    if !table_indexes.is_empty() && !resolver.schema.indexes_enabled() {
        // Let's disable altering a table with indices altogether instead of checking column by
        // column to be extra safe.
        crate::bail_parse_error!(
            "ALTER TABLE for table with indexes is disabled. Omit the `--experimental-indexes=false` flag to enable this feature."
        );
    }

    let Some(original_btree) = resolver
        .schema
        .get_table(table_name)
        .and_then(|table| table.btree())
    else {
        return Err(LimboError::ParseError(format!(
            "no such table: {table_name}"
        )));
    };

    // Check if this table has dependent materialized views
    let dependent_views = resolver.schema.get_dependent_materialized_views(table_name);
    if !dependent_views.is_empty() {
        return Err(LimboError::ParseError(format!(
            "cannot alter table \"{table_name}\": it has dependent materialized view(s): {}",
            dependent_views.join(", ")
        )));
    }

    let mut btree = (*original_btree).clone();

    Ok(match alter_table {
        ast::AlterTableBody::DropColumn(column_name) => {
            let column_name = column_name.as_str();

            // Tables always have at least one column.
            assert_ne!(btree.columns.len(), 0);

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
            // The column is named in the WHERE clause of a partial index.
            // The column is named in a table or column CHECK constraint not associated with the column being dropped.
            // The column is used in a foreign key constraint.
            // The column is used in the expression of a generated column.
            // The column appears in a trigger or view.

            if column.primary_key {
                return Err(LimboError::ParseError(format!(
                    "cannot drop column \"{column_name}\": PRIMARY KEY"
                )));
            }

            if column.unique
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

            for index in table_indexes.iter() {
                // Referenced in regular index
                if index
                    .columns
                    .iter()
                    .any(|col| col.pos_in_table == dropped_index)
                {
                    return Err(LimboError::ParseError(format!(
                        "cannot drop column \"{column_name}\": indexed"
                    )));
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
                        }],
                    );
                    let where_copy = index
                        .bind_where_expr(Some(&mut table_references), connection)
                        .expect("where clause to exist");
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

            // TODO: check usage in CHECK constraint when implemented
            // TODO: check usage in foreign key constraint when implemented
            // TODO: check usage in generated column when implemented

            // References in VIEWs are checked in the VDBE layer op_drop_column instruction.

            btree.columns.remove(dropped_index);

            let sql = btree.to_sql().replace('\'', "''");

            let stmt = format!(
                r#"
                    UPDATE {SQLITE_TABLEID}
                    SET sql = '{sql}'
                    WHERE name = '{table_name}' COLLATE NOCASE AND type = 'table'
                "#,
            );

            let mut parser = Parser::new(stmt.as_bytes());
            let Some(ast::Cmd::Stmt(ast::Stmt::Update(mut update))) = parser.next_cmd().unwrap()
            else {
                unreachable!();
            };

            translate_update_for_schema_change(
                &mut update,
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
                        db: 0,
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
                            .map(|col| col.affinity().aff_mask())
                            .collect::<String>();

                        program.emit_insn(Insn::MakeRecord {
                            start_reg: first_column,
                            count: column_count,
                            dest_reg: record,
                            index_name: None,
                            affinity_str: Some(affinity_str),
                        });

                        program.emit_insn(Insn::Insert {
                            cursor: cursor_id,
                            key_reg: rowid,
                            record_reg: record,
                            flag: crate::vdbe::insn::InsertFlags(0),
                            table_name: table_name.clone(),
                        });
                    });

                    program.emit_insn(Insn::SetCookie {
                        db: 0,
                        cookie: Cookie::SchemaVersion,
                        value: resolver.schema.schema_version as i32 + 1,
                        p5: 0,
                    });

                    program.emit_insn(Insn::DropColumn {
                        table: table_name,
                        column_index: dropped_index,
                    })
                },
            )?
        }
        ast::AlterTableBody::AddColumn(col_def) => {
            let column = Column::from(&col_def);

            if let Some(default) = &column.default {
                if !matches!(
                    default.as_ref(),
                    ast::Expr::Literal(
                        ast::Literal::Null
                            | ast::Literal::Blob(_)
                            | ast::Literal::Numeric(_)
                            | ast::Literal::String(_)
                    )
                ) {
                    // TODO: This is slightly inaccurate since sqlite returns a `Runtime
                    // error`.
                    return Err(LimboError::ParseError(
                        "Cannot add a column with non-constant default".to_string(),
                    ));
                }
            }

            let new_column_name = column.name.as_ref().unwrap();
            if btree.get_column(new_column_name).is_some() {
                return Err(LimboError::ParseError(
                    "duplicate column name: ".to_string() + new_column_name,
                ));
            }

            // TODO: All quoted ids will be quoted with `[]`, we should store some info from the parsed AST
            btree.columns.push(column.clone());

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
                    UPDATE {SQLITE_TABLEID}
                    SET sql = '{escaped}'
                    WHERE name = '{table_name}' COLLATE NOCASE AND type = 'table'
                "#,
            );

            let mut parser = Parser::new(stmt.as_bytes());
            let Some(ast::Cmd::Stmt(ast::Stmt::Update(mut update))) = parser.next_cmd().unwrap()
            else {
                unreachable!();
            };

            translate_update_for_schema_change(
                &mut update,
                resolver,
                program,
                connection,
                input,
                |program| {
                    program.emit_insn(Insn::SetCookie {
                        db: 0,
                        cookie: Cookie::SchemaVersion,
                        value: resolver.schema.schema_version as i32 + 1,
                        p5: 0,
                    });
                    program.emit_insn(Insn::AddColumn {
                        table: table_name.to_owned(),
                        column,
                    });
                },
            )?
        }
        ast::AlterTableBody::RenameTo(new_name) => {
            let new_name = new_name.as_str();

            if resolver.schema.get_table(new_name).is_some()
                || resolver
                    .schema
                    .indexes
                    .values()
                    .flatten()
                    .any(|index| index.name == normalize_ident(new_name))
            {
                return Err(LimboError::ParseError(format!(
                    "there is already another table or index with this name: {new_name}"
                )));
            };

            let sqlite_schema = resolver
                .schema
                .get_btree_table(SQLITE_TABLEID)
                .expect("sqlite_schema should be on schema");

            let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(sqlite_schema.clone()));

            program.emit_insn(Insn::OpenWrite {
                cursor_id,
                root_page: RegisterOrLiteral::Literal(sqlite_schema.root_page),
                db: 0,
            });

            program.cursor_loop(cursor_id, |program, rowid| {
                let sqlite_schema_column_len = sqlite_schema.columns.len();
                assert_eq!(sqlite_schema_column_len, 5);

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
                    start_reg: out,
                    count: sqlite_schema_column_len,
                    dest_reg: record,
                    index_name: None,
                    affinity_str: None,
                });

                program.emit_insn(Insn::Insert {
                    cursor: cursor_id,
                    key_reg: rowid,
                    record_reg: record,
                    flag: crate::vdbe::insn::InsertFlags(0),
                    table_name: table_name.to_string(),
                });
            });

            program.emit_insn(Insn::SetCookie {
                db: 0,
                cookie: Cookie::SchemaVersion,
                value: resolver.schema.schema_version as i32 + 1,
                p5: 0,
            });

            program.emit_insn(Insn::RenameTable {
                from: table_name.to_owned(),
                to: new_name.to_owned(),
            });

            program
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

            let sqlite_schema = resolver
                .schema
                .get_btree_table(SQLITE_TABLEID)
                .expect("sqlite_schema should be on schema");

            let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(sqlite_schema.clone()));

            program.emit_insn(Insn::OpenWrite {
                cursor_id,
                root_page: RegisterOrLiteral::Literal(sqlite_schema.root_page),
                db: 0,
            });

            program.cursor_loop(cursor_id, |program, rowid| {
                let sqlite_schema_column_len = sqlite_schema.columns.len();
                assert_eq!(sqlite_schema_column_len, 5);

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
                    start_reg: out,
                    count: sqlite_schema_column_len,
                    dest_reg: record,
                    index_name: None,
                    affinity_str: None,
                });

                program.emit_insn(Insn::Insert {
                    cursor: cursor_id,
                    key_reg: rowid,
                    record_reg: record,
                    flag: crate::vdbe::insn::InsertFlags(0),
                    table_name: table_name.to_string(),
                });
            });

            program.emit_insn(Insn::SetCookie {
                db: 0,
                cookie: Cookie::SchemaVersion,
                value: resolver.schema.schema_version as i32 + 1,
                p5: 0,
            });
            program.emit_insn(Insn::AlterColumn {
                table: table_name.to_owned(),
                column_index,
                definition,
                rename,
            });

            program
        }
    })
}
