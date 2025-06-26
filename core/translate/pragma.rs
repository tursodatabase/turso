//! VDBE bytecode generation for pragma statements.
//! More info: https://www.sqlite.org/pragma.html.

use limbo_sqlite3_parser::ast::PragmaName;
use limbo_sqlite3_parser::ast::{self, Expr};
use std::rc::Rc;
use std::sync::Arc;

use crate::schema::Schema;
use crate::storage::pager::AutoVacuumMode;
use crate::storage::sqlite3_ondisk::MIN_PAGE_CACHE_SIZE;
use crate::storage::wal::CheckpointMode;
use crate::util::{normalize_ident, parse_signed_number};
use crate::vdbe::builder::{ProgramBuilder, ProgramBuilderOpts, QueryMode};
use crate::vdbe::insn::{Cookie, Insn};
use crate::{bail_parse_error, storage, LimboError, Value};
use std::str::FromStr;
use strum::IntoEnumIterator;

use super::integrity_check::translate_integrity_check;
use crate::storage::header_accessor;
use crate::storage::pager::Pager;

fn list_pragmas(program: &mut ProgramBuilder) {
    for x in PragmaName::iter() {
        let register = program.emit_string8_new_reg(x.to_string());
        program.emit_result_row(register, 1);
    }
    program.add_pragma_result_column("pragma_list".into());
    program.epilogue(crate::translate::emitter::TransactionMode::None);
}

#[allow(clippy::too_many_arguments)]
pub fn translate_pragma(
    query_mode: QueryMode,
    schema: &Schema,
    name: &ast::QualifiedName,
    body: Option<ast::PragmaBody>,
    pager: Rc<Pager>,
    connection: Arc<crate::Connection>,
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

    if name.name.0.eq_ignore_ascii_case("pragma_list") {
        list_pragmas(&mut program);
        return Ok(program);
    }

    let pragma = match PragmaName::from_str(&name.name.0) {
        Ok(pragma) => pragma,
        Err(_) => bail_parse_error!("Not a valid pragma name"),
    };

    match body {
        None => {
            query_pragma(pragma, schema, None, pager, connection, &mut program)?;
        }
        Some(ast::PragmaBody::Equals(value) | ast::PragmaBody::Call(value)) => match pragma {
            PragmaName::TableInfo => {
                query_pragma(pragma, schema, Some(value), pager, connection, &mut program)?;
            }
            _ => {
                write = true;
                update_pragma(pragma, schema, value, pager, connection, &mut program)?;
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
    pager: Rc<Pager>,
    connection: Arc<crate::Connection>,
    program: &mut ProgramBuilder,
) -> crate::Result<()> {
    match pragma {
        PragmaName::CacheSize => {
            let cache_size = match parse_signed_number(&value)? {
                Value::Integer(size) => size,
                Value::Float(size) => size as i64,
                _ => bail_parse_error!("Invalid value for cache size pragma"),
            };
            update_cache_size(cache_size, pager, connection)?;
            Ok(())
        }
        PragmaName::JournalMode => {
            query_pragma(
                PragmaName::JournalMode,
                schema,
                None,
                pager,
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
                Some(value),
                pager,
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
                pager,
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
        PragmaName::AutoVacuum => {
            let auto_vacuum_mode = match value {
                Expr::Name(name) => {
                    let name = name.0.to_lowercase();
                    match name.as_str() {
                        "none" => 0,
                        "full" => 1,
                        "incremental" => 2,
                        _ => {
                            return Err(LimboError::InvalidArgument(
                                "invalid auto vacuum mode".to_string(),
                            ));
                        }
                    }
                }
                _ => {
                    return Err(LimboError::InvalidArgument(
                        "invalid auto vacuum mode".to_string(),
                    ))
                }
            };
            match auto_vacuum_mode {
                0 => update_auto_vacuum_mode(AutoVacuumMode::None, 0, pager)?,
                1 => update_auto_vacuum_mode(AutoVacuumMode::Full, 1, pager)?,
                2 => update_auto_vacuum_mode(AutoVacuumMode::Incremental, 1, pager)?,
                _ => {
                    return Err(LimboError::InvalidArgument(
                        "invalid auto vacuum mode".to_string(),
                    ))
                }
            }
            let largest_root_page_number_reg = program.alloc_register();
            program.emit_insn(Insn::ReadCookie {
                db: 0,
                dest: largest_root_page_number_reg,
                cookie: Cookie::LargestRootPageNumber,
            });
            let set_cookie_label = program.allocate_label();
            program.emit_insn(Insn::If {
                reg: largest_root_page_number_reg,
                target_pc: set_cookie_label,
                jump_if_null: false,
            });
            program.emit_insn(Insn::Halt {
                err_code: 0,
                description: "Early halt because auto vacuum mode is not enabled".to_string(),
            });
            program.resolve_label(set_cookie_label, program.offset());
            program.emit_insn(Insn::SetCookie {
                db: 0,
                cookie: Cookie::IncrementalVacuum,
                value: auto_vacuum_mode - 1,
                p5: 0,
            });
            Ok(())
        }
        PragmaName::IntegrityCheck => unreachable!("integrity_check cannot be set"),
    }
}

fn query_pragma(
    pragma: PragmaName,
    schema: &Schema,
    value: Option<ast::Expr>,
    pager: Rc<Pager>,
    connection: Arc<crate::Connection>,
    program: &mut ProgramBuilder,
) -> crate::Result<()> {
    let register = program.alloc_register();
    match pragma {
        PragmaName::CacheSize => {
            program.emit_int(connection.get_cache_size() as i64, register);
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
        }
        PragmaName::JournalMode => {
            program.emit_string8("wal".into(), register);
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
        }
        PragmaName::LegacyFileFormat => {}
        PragmaName::WalCheckpoint => {
            // Checkpoint uses 3 registers: P1, P2, P3. Ref Insn::Checkpoint for more info.
            // Allocate two more here as one was allocated at the top.
            let mode = match value {
                Some(ast::Expr::Name(name)) => {
                    let mode_name = normalize_ident(&name.0);
                    CheckpointMode::from_str(&mode_name).map_err(|e| {
                        LimboError::ParseError(format!("Unknown Checkpoint Mode: {}", e))
                    })?
                }
                _ => CheckpointMode::Passive,
            };

            if !matches!(mode, CheckpointMode::Passive) {
                return Err(LimboError::ParseError(
                    "only Passive mode supported".to_string(),
                ));
            }

            program.alloc_registers(2);
            program.emit_insn(Insn::Checkpoint {
                database: 0,
                checkpoint_mode: mode,
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
            program.add_pragma_result_column(pragma.to_string());
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
            program.alloc_registers(5);
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
            let col_names = ["cid", "name", "type", "notnull", "dflt_value", "pk"];
            for name in col_names {
                program.add_pragma_result_column(name.into());
            }
        }
        PragmaName::UserVersion => {
            program.emit_insn(Insn::ReadCookie {
                db: 0,
                dest: register,
                cookie: Cookie::UserVersion,
            });
            program.add_pragma_result_column(pragma.to_string());
            program.emit_result_row(register, 1);
        }
        PragmaName::SchemaVersion => {
            program.emit_insn(Insn::ReadCookie {
                db: 0,
                dest: register,
                cookie: Cookie::SchemaVersion,
            });
            program.add_pragma_result_column(pragma.to_string());
            program.emit_result_row(register, 1);
        }
        PragmaName::PageSize => {
            let pager_inner = pager.inner.lock().unwrap();
            program.emit_int(
                header_accessor::get_page_size(&pager_inner)
                    .unwrap_or(storage::sqlite3_ondisk::DEFAULT_PAGE_SIZE) as i64,
                register,
            );
            drop(pager_inner);
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
        }
        PragmaName::AutoVacuum => {
            let auto_vacuum_mode = pager.get_auto_vacuum_mode();
            let auto_vacuum_mode_i64: i64 = match auto_vacuum_mode {
                AutoVacuumMode::None => 0,
                AutoVacuumMode::Full => 1,
                AutoVacuumMode::Incremental => 2,
            };
            let register = program.alloc_register();
            program.emit_insn(Insn::Int64 {
                _p1: 0,
                out_reg: register,
                _p3: 0,
                value: auto_vacuum_mode_i64,
            });
            program.emit_result_row(register, 1);
        }
        PragmaName::IntegrityCheck => {
            translate_integrity_check(schema, program)?;
        }
    }

    Ok(())
}

fn update_auto_vacuum_mode(
    auto_vacuum_mode: AutoVacuumMode,
    largest_root_page_number: u32,
    pager: Rc<Pager>,
) -> crate::Result<()> {
    let pager_inner = pager.inner.lock().unwrap();
    header_accessor::set_vacuum_mode_largest_root_page(&pager_inner, largest_root_page_number)?;
    drop(pager_inner);
    pager.set_auto_vacuum_mode(auto_vacuum_mode);
    Ok(())
}

fn update_cache_size(
    value: i64,
    pager: Rc<Pager>,
    connection: Arc<crate::Connection>,
) -> crate::Result<()> {
    let mut cache_size_unformatted: i64 = value;
    let mut cache_size = if cache_size_unformatted < 0 {
        let kb = cache_size_unformatted.abs() * 1024;
        let pager_inner = pager.inner.lock().unwrap();
        let page_size = header_accessor::get_page_size(&pager_inner)
            .unwrap_or(storage::sqlite3_ondisk::DEFAULT_PAGE_SIZE) as i64;
        kb / page_size as i64
    } else {
        value
    } as usize;

    if cache_size < MIN_PAGE_CACHE_SIZE {
        cache_size = MIN_PAGE_CACHE_SIZE;
        cache_size_unformatted = MIN_PAGE_CACHE_SIZE as i64;
    }
    connection.set_cache_size(cache_size_unformatted as i32);

    // update cache size
    pager
        .change_page_cache_size(cache_size)
        .expect("couldn't update page cache size");

    Ok(())
}
