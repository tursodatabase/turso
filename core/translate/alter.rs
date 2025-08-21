use fallible_iterator::FallibleIterator as _;
use std::sync::Arc;
use turso_sqlite3_parser::{ast, lexer::sql::Parser};

use crate::{
    function::{AlterTableFunc, Func},
    schema::{Column, Schema},
    util::normalize_ident,
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::{Cookie, Insn, RegisterOrLiteral},
    },
    LimboError, Result, SymbolTable,
};

use super::{schema::SQLITE_TABLEID, update::translate_update_for_schema_change};

pub fn translate_alter_table(
    alter: (ast::QualifiedName, ast::AlterTableBody),
    syms: &SymbolTable,
    schema: &Schema,
    mut program: ProgramBuilder,
    connection: &Arc<crate::Connection>,
    input: &str,
) -> Result<ProgramBuilder> {
    program.begin_write_operation();
    let (table_name, alter_table) = alter;
    let table_name = table_name.name.as_str();
    if schema.table_has_indexes(table_name) && !schema.indexes_enabled() {
        // Let's disable altering a table with indices altogether instead of checking column by
        // column to be extra safe.
        crate::bail_parse_error!(
            "ALTER TABLE for table with indexes is disabled. Omit the `--experimental-indexes=false` flag to enable this feature."
        );
    }

    let Some(original_btree) = schema.get_table(table_name).and_then(|table| table.btree()) else {
        return Err(LimboError::ParseError(format!(
            "no such table: {table_name}"
        )));
    };

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

            if column.primary_key {
                return Err(LimboError::ParseError(format!(
                    "cannot drop column \"{column_name}\": PRIMARY KEY"
                )));
            }

            if column.unique
                || btree.unique_sets.as_ref().is_some_and(|set| {
                    set.iter().any(|set| {
                        set.iter()
                            .any(|(name, _)| name == &normalize_ident(column_name))
                    })
                })
            {
                return Err(LimboError::ParseError(format!(
                    "cannot drop column \"{column_name}\": UNIQUE"
                )));
            }

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
            let Some(ast::Cmd::Stmt(ast::Stmt::Update(mut update))) = parser.next().unwrap() else {
                unreachable!();
            };

            translate_update_for_schema_change(
                schema,
                &mut update,
                syms,
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

                        program.emit_insn(Insn::MakeRecord {
                            start_reg: first_column,
                            count: column_count,
                            dest_reg: record,
                            index_name: None,
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
                        value: schema.schema_version as i32 + 1,
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
            let column = Column::from(col_def);

            if let Some(default) = &column.default {
                if !matches!(
                    default,
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
            let Some(ast::Cmd::Stmt(ast::Stmt::Update(mut update))) = parser.next().unwrap() else {
                unreachable!();
            };

            translate_update_for_schema_change(
                schema,
                &mut update,
                syms,
                program,
                connection,
                input,
                |program| {
                    program.emit_insn(Insn::SetCookie {
                        db: 0,
                        cookie: Cookie::SchemaVersion,
                        value: schema.schema_version as i32 + 1,
                        p5: 0,
                    });
                    program.emit_insn(Insn::AddColumn {
                        table: table_name.to_owned(),
                        column,
                    });
                },
            )?
        }
        ast::AlterTableBody::RenameColumn { old, new } => {
            let rename_from = old.as_str();
            let rename_to = new.as_str();

            let Some((column_index, _)) = btree.get_column(rename_from) else {
                return Err(LimboError::ParseError(format!(
                    "no such column: \"{rename_from}\""
                )));
            };

            if btree.get_column(rename_to).is_some() {
                return Err(LimboError::ParseError(format!(
                    "duplicate column name: \"{rename_from}\""
                )));
            };

            let sqlite_schema = schema
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

                program.emit_string8_new_reg(rename_from.to_string());
                program.mark_last_insn_constant();

                program.emit_string8_new_reg(rename_to.to_string());
                program.mark_last_insn_constant();

                let out = program.alloc_registers(sqlite_schema_column_len);

                program.emit_insn(Insn::Function {
                    constant_mask: 0,
                    start_reg: first_column,
                    dest: out,
                    func: crate::function::FuncCtx {
                        func: Func::AlterTable(AlterTableFunc::RenameColumn),
                        arg_count: 8,
                    },
                });

                let record = program.alloc_register();

                program.emit_insn(Insn::MakeRecord {
                    start_reg: out,
                    count: sqlite_schema_column_len,
                    dest_reg: record,
                    index_name: None,
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
                value: schema.schema_version as i32 + 1,
                p5: 0,
            });
            program.emit_insn(Insn::RenameColumn {
                table: table_name.to_owned(),
                column_index,
                name: rename_to.to_owned(),
            });

            program
        }
        ast::AlterTableBody::RenameTo(new_name) => {
            let new_name = new_name.as_str();

            if schema.get_table(new_name).is_some()
                || schema
                    .indexes
                    .values()
                    .flatten()
                    .any(|index| index.name == normalize_ident(new_name))
            {
                return Err(LimboError::ParseError(format!(
                    "there is already another table or index with this name: {new_name}"
                )));
            };

            let sqlite_schema = schema
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
                value: schema.schema_version as i32 + 1,
                p5: 0,
            });

            program.emit_insn(Insn::RenameTable {
                from: table_name.to_owned(),
                to: new_name.to_owned(),
            });

            program
        }
    })
}
