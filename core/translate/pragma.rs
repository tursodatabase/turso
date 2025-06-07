//! VDBE bytecode generation for pragma statements.
//! More info: https://www.sqlite.org/pragma.html.

use limbo_sqlite3_parser::ast;
use limbo_sqlite3_parser::ast::PragmaName;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use crate::fast_lock::SpinLock;
use crate::schema::Schema;
use crate::storage::sqlite3_ondisk::{DatabaseHeader, MIN_PAGE_CACHE_SIZE};
use crate::storage::wal::CheckpointMode;
use crate::util::{normalize_ident, parse_signed_number};
use crate::vdbe::builder::{ProgramBuilder, ProgramBuilderOpts, QueryMode};
use crate::vdbe::insn::{Cookie, Insn};
use crate::{bail_parse_error, Pager, Value};
use std::str::FromStr;
use strum::IntoEnumIterator;

fn list_pragmas(program: &mut ProgramBuilder) {
    for x in PragmaName::iter() {
        let register = program.emit_string8_new_reg(x.to_string());
        program.emit_result_row(register, 1);
    }

    program.epilogue(crate::translate::emitter::TransactionMode::None);
}

pub fn translate_pragma(
    query_mode: QueryMode,
    schema: &Schema,
    name: &ast::QualifiedName,
    body: Option<ast::PragmaBody>,
    database_header: Arc<SpinLock<DatabaseHeader>>,
    pager: Rc<Pager>,
    connection: Weak<crate::Connection>,
    mut program: ProgramBuilder,
) -> crate::Result<ProgramBuilder> {
    let opts = ProgramBuilderOpts {
        query_mode,
        num_cursors: 0,
        approx_num_insns: 20,
        approx_num_labels: 0,
    };
    program.extend(&opts);
    let mut write = false;

    if name.name.0.to_lowercase() == "pragma_list" {
        list_pragmas(&mut program);
        return Ok(program);
    }

    let pragma = match PragmaName::from_str(&name.name.0) {
        Ok(pragma) => pragma,
        Err(_) => bail_parse_error!("Not a valid pragma name"),
    };

    match body {
        None => {
            query_pragma(
                pragma,
                schema,
                None,
                database_header.clone(),
                connection,
                &mut program,
            )?;
        }
        Some(ast::PragmaBody::Equals(value)) => match pragma {
            PragmaName::TableInfo => {
                query_pragma(
                    pragma,
                    schema,
                    Some(value),
                    database_header.clone(),
                    connection,
                    &mut program,
                )?;
            }
            _ => {
                write = true;
                update_pragma(
                    pragma,
                    schema,
                    value,
                    database_header.clone(),
                    pager,
                    connection,
                    &mut program,
                )?;
            }
        },
        Some(ast::PragmaBody::Call(value)) => match pragma {
            PragmaName::TableInfo => {
                query_pragma(
                    pragma,
                    schema,
                    Some(value),
                    database_header.clone(),
                    connection,
                    &mut program,
                )?;
            }
            _ => {
                todo!()
            }
        },
    };
    program.epilogue(match write {
        false => super::emitter::TransactionMode::Read,
        true => super::emitter::TransactionMode::Write,
    });

    Ok(program)
}

fn update_pragma(
    pragma: PragmaName,
    schema: &Schema,
    value: ast::Expr,
    header: Arc<SpinLock<DatabaseHeader>>,
    pager: Rc<Pager>,
    connection: Weak<crate::Connection>,
    program: &mut ProgramBuilder,
) -> crate::Result<()> {
    match pragma {
        PragmaName::CacheSize => {
            let cache_size = match parse_signed_number(&value)? {
                Value::Integer(size) => size,
                Value::Float(size) => size as i64,
                _ => bail_parse_error!("Invalid value for cache size pragma"),
            };
            update_cache_size(cache_size, header, pager, connection)?;
            Ok(())
        }
        PragmaName::JournalMode => {
            query_pragma(
                PragmaName::JournalMode,
                schema,
                None,
                header,
                connection,
                program,
            )?;
            Ok(())
        }
        PragmaName::LegacyFileFormat => Ok(()),
        PragmaName::WalCheckpoint => {
            query_pragma(
                PragmaName::WalCheckpoint,
                schema,
                None,
                header,
                connection,
                program,
            )?;
            Ok(())
        }
        PragmaName::PageCount => {
            query_pragma(
                PragmaName::PageCount,
                schema,
                None,
                header,
                connection,
                program,
            )?;
            Ok(())
        }
        PragmaName::UserVersion => {
            let data = parse_signed_number(&value)?;
            let version_value = match data {
                Value::Integer(i) => i as i32,
                Value::Float(f) => f as i32,
                _ => unreachable!(),
            };

            program.emit_insn(Insn::SetCookie {
                db: 0,
                cookie: Cookie::UserVersion,
                value: version_value,
                p5: 1,
            });
            Ok(())
        }
        PragmaName::SchemaVersion => {
            // TODO: Implement updating schema_version
            todo!("updating schema_version not yet implemented")
        }
        PragmaName::TableInfo => {
            // because we need control over the write parameter for the transaction,
            // this should be unreachable. We have to force-call query_pragma before
            // getting here
            unreachable!();
        }
        PragmaName::PageSize => {
            todo!("updating page_size is not yet implemented")
        }
    }
}

fn query_pragma(
    pragma: PragmaName,
    schema: &Schema,
    value: Option<ast::Expr>,
    database_header: Arc<SpinLock<DatabaseHeader>>,
    connection: Weak<crate::Connection>,
    program: &mut ProgramBuilder,
) -> crate::Result<()> {
    let register = program.alloc_register();
    match pragma {
        PragmaName::CacheSize => {
            program.emit_int(
                connection.upgrade().unwrap().get_cache_size() as i64,
                register,
            );
            program.emit_result_row(register, 1);
        }
        PragmaName::JournalMode => {
            program.emit_string8("wal".into(), register);
            program.emit_result_row(register, 1);
        }
        PragmaName::LegacyFileFormat => {}
        PragmaName::WalCheckpoint => {
            // Checkpoint uses 3 registers: P1, P2, P3. Ref Insn::Checkpoint for more info.
            // Allocate two more here as one was allocated at the top.
            program.alloc_register();
            program.alloc_register();
            program.emit_insn(Insn::Checkpoint {
                database: 0,
                checkpoint_mode: CheckpointMode::Passive,
                dest: register,
            });
            program.emit_result_row(register, 3);
        }
        PragmaName::PageCount => {
            program.emit_insn(Insn::PageCount {
                db: 0,
                dest: register,
            });
            program.emit_result_row(register, 1);
        }
        PragmaName::TableInfo => {
            let table = match value {
                Some(ast::Expr::Name(name)) => {
                    let tbl = normalize_ident(&name.0);
                    schema.get_table(&tbl)
                }
                _ => None,
            };

            let base_reg = register;
            program.alloc_register();
            program.alloc_register();
            program.alloc_register();
            program.alloc_register();
            program.alloc_register();
            if let Some(table) = table {
                for (i, column) in table.columns().iter().enumerate() {
                    // cid
                    program.emit_int(i as i64, base_reg);
                    // name
                    program.emit_string8(column.name.clone().unwrap_or_default(), base_reg + 1);

                    // type
                    program.emit_string8(column.ty_str.clone(), base_reg + 2);

                    // notnull
                    program.emit_bool(column.notnull, base_reg + 3);

                    // dflt_value
                    match &column.default {
                        None => {
                            program.emit_null(base_reg + 4, None);
                        }
                        Some(expr) => {
                            program.emit_string8(expr.to_string(), base_reg + 4);
                        }
                    }

                    // pk
                    program.emit_bool(column.primary_key, base_reg + 5);

                    program.emit_result_row(base_reg, 6);
                }
            }
        }
        PragmaName::UserVersion => {
            program.emit_insn(Insn::ReadCookie {
                db: 0,
                dest: register,
                cookie: Cookie::UserVersion,
            });
            program.emit_result_row(register, 1);
        }
        PragmaName::SchemaVersion => {
            program.emit_insn(Insn::ReadCookie {
                db: 0,
                dest: register,
                cookie: Cookie::SchemaVersion,
            });
            program.emit_result_row(register, 1);
        }
        PragmaName::PageSize => {
            program.emit_int(database_header.lock().get_page_size().into(), register);
            program.emit_result_row(register, 1);
        }
    }

    Ok(())
}

fn update_cache_size(
    value: i64,
    header: Arc<SpinLock<DatabaseHeader>>,
    pager: Rc<Pager>,
    connection: Weak<crate::Connection>,
) -> crate::Result<()> {
    let mut cache_size_unformatted: i64 = value;
    let mut cache_size = if cache_size_unformatted < 0 {
        let kb = cache_size_unformatted.abs() * 1024;
        let page_size = header.lock().get_page_size();
        kb / page_size as i64
    } else {
        value
    } as usize;

    if cache_size < MIN_PAGE_CACHE_SIZE {
        cache_size = MIN_PAGE_CACHE_SIZE;
        cache_size_unformatted = MIN_PAGE_CACHE_SIZE as i64;
    }
    connection
        .upgrade()
        .unwrap()
        .set_cache_size(cache_size_unformatted as i32);

    // update cache size
    pager
        .change_page_cache_size(cache_size)
        .expect("couldn't update page cache size");

    Ok(())
}
