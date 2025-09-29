//! VDBE bytecode generation for pragma statements.
//! More info: https://www.sqlite.org/pragma.html.

use chrono::Datelike;
use std::sync::Arc;
use turso_macros::match_ignore_ascii_case;
use turso_parser::ast::{self, ColumnDefinition, Expr, Literal};
use turso_parser::ast::{PragmaName, QualifiedName};

use super::integrity_check::translate_integrity_check;
use crate::pragma::pragma_for;
use crate::schema::Schema;
use crate::storage::encryption::{CipherMode, EncryptionKey};
use crate::storage::pager::AutoVacuumMode;
use crate::storage::pager::Pager;
use crate::storage::sqlite3_ondisk::CacheSize;
use crate::storage::wal::CheckpointMode;
use crate::translate::emitter::{Resolver, TransactionMode};
use crate::translate::schema::translate_create_table;
use crate::util::{normalize_ident, parse_signed_number, parse_string, IOExt as _};
use crate::vdbe::builder::{ProgramBuilder, ProgramBuilderOpts};
use crate::vdbe::insn::{Cookie, Insn};
use crate::{bail_parse_error, CaptureDataChangesMode, LimboError, Value};
use std::str::FromStr;
use strum::IntoEnumIterator;

fn list_pragmas(program: &mut ProgramBuilder) {
    for x in PragmaName::iter() {
        let register = program.emit_string8_new_reg(x.to_string());
        program.emit_result_row(register, 1);
    }
    program.add_pragma_result_column("pragma_list".into());
}

pub fn translate_pragma(
    resolver: &Resolver,
    name: &ast::QualifiedName,
    body: Option<ast::PragmaBody>,
    pager: Arc<Pager>,
    connection: Arc<crate::Connection>,
    mut program: ProgramBuilder,
) -> crate::Result<ProgramBuilder> {
    let opts = ProgramBuilderOpts {
        num_cursors: 0,
        approx_num_insns: 20,
        approx_num_labels: 0,
    };
    program.extend(&opts);

    if name.name.as_str().eq_ignore_ascii_case("pragma_list") {
        list_pragmas(&mut program);
        return Ok(program);
    }

    let pragma = match PragmaName::from_str(name.name.as_str()) {
        Ok(pragma) => pragma,
        Err(_) => bail_parse_error!("Not a valid pragma name"),
    };

    let (mut program, mode) = match body {
        None => query_pragma(pragma, resolver.schema, None, pager, connection, program)?,
        Some(ast::PragmaBody::Equals(value) | ast::PragmaBody::Call(value)) => match pragma {
            PragmaName::TableInfo => query_pragma(
                pragma,
                resolver.schema,
                Some(*value),
                pager,
                connection,
                program,
            )?,
            _ => update_pragma(pragma, resolver, *value, pager, connection, program)?,
        },
    };
    match mode {
        TransactionMode::None => {}
        TransactionMode::Read => {
            program.begin_read_operation();
        }
        TransactionMode::Write => {
            program.begin_write_operation();
        }
        TransactionMode::Concurrent => {
            program.begin_concurrent_operation();
        }
    }

    Ok(program)
}

fn update_pragma(
    pragma: PragmaName,
    resolver: &Resolver,
    value: ast::Expr,
    pager: Arc<Pager>,
    connection: Arc<crate::Connection>,
    mut program: ProgramBuilder,
) -> crate::Result<(ProgramBuilder, TransactionMode)> {
    match pragma {
        PragmaName::ApplicationId => {
            let data = parse_signed_number(&value)?;
            let app_id_value = match data {
                Value::Integer(i) => i as i32,
                Value::Float(f) => f as i32,
                _ => bail_parse_error!("expected integer, got {:?}", data),
            };

            program.emit_insn(Insn::SetCookie {
                db: 0,
                cookie: Cookie::ApplicationId,
                value: app_id_value,
                p5: 1,
            });
            Ok((program, TransactionMode::Write))
        }
        PragmaName::BusyTimeout => {
            let data = parse_signed_number(&value)?;
            let busy_timeout_ms = match data {
                Value::Integer(i) => i as i32,
                Value::Float(f) => f as i32,
                _ => bail_parse_error!("expected integer, got {:?}", data),
            };
            let busy_timeout_ms = busy_timeout_ms.max(0);
            connection.set_busy_timeout(std::time::Duration::from_millis(busy_timeout_ms as u64));
            Ok((program, TransactionMode::Write))
        }
        PragmaName::CacheSize => {
            let cache_size = match parse_signed_number(&value)? {
                Value::Integer(size) => size,
                Value::Float(size) => size as i64,
                _ => bail_parse_error!("Invalid value for cache size pragma"),
            };
            update_cache_size(cache_size, pager, connection)?;
            Ok((program, TransactionMode::None))
        }
        PragmaName::Encoding => {
            let year = chrono::Local::now().year();
            bail_parse_error!("It's {year}. UTF-8 won.");
        }
        PragmaName::JournalMode => {
            // For JournalMode, when setting a value, we use the opcode
            let mode_str = match value {
                Expr::Name(name) => name.as_str().to_string(),
                _ => parse_string(&value)?,
            };

            let result_reg = program.alloc_register();
            program.emit_insn(Insn::JournalMode {
                db: 0,
                dest: result_reg,
                new_mode: Some(mode_str),
            });
            program.emit_result_row(result_reg, 1);
            program.add_pragma_result_column("journal_mode".into());
            Ok((program, TransactionMode::None))
        }
        PragmaName::LegacyFileFormat => Ok((program, TransactionMode::None)),
        PragmaName::WalCheckpoint => query_pragma(
            PragmaName::WalCheckpoint,
            resolver.schema,
            Some(value),
            pager,
            connection,
            program,
        ),
        PragmaName::ModuleList => Ok((program, TransactionMode::None)),
        PragmaName::PageCount => query_pragma(
            PragmaName::PageCount,
            resolver.schema,
            None,
            pager,
            connection,
            program,
        ),
        PragmaName::MaxPageCount => {
            let data = parse_signed_number(&value)?;
            let max_page_count_value = match data {
                Value::Integer(i) => i as usize,
                Value::Float(f) => f as usize,
                _ => unreachable!(),
            };

            let result_reg = program.alloc_register();
            program.emit_insn(Insn::MaxPgcnt {
                db: 0,
                dest: result_reg,
                new_max: max_page_count_value,
            });
            program.emit_result_row(result_reg, 1);
            program.add_pragma_result_column("max_page_count".into());
            Ok((program, TransactionMode::Write))
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
            Ok((program, TransactionMode::Write))
        }
        PragmaName::SchemaVersion => {
            // SQLite allowing this to be set is an incredibly stupid idea in my view.
            // In "defensive mode", this is a silent nop. So let's emulate that always.
            program.emit_insn(Insn::Noop {});
            Ok((program, TransactionMode::None))
        }
        PragmaName::TableInfo => {
            // because we need control over the write parameter for the transaction,
            // this should be unreachable. We have to force-call query_pragma before
            // getting here
            unreachable!();
        }
        PragmaName::PageSize => {
            let page_size = match parse_signed_number(&value)? {
                Value::Integer(size) => size,
                Value::Float(size) => size as i64,
                _ => bail_parse_error!("Invalid value for page size pragma"),
            };
            update_page_size(connection, page_size as u32)?;
            Ok((program, TransactionMode::None))
        }
        PragmaName::AutoVacuum => {
            let auto_vacuum_mode = match value {
                Expr::Name(name) => {
                    let name = name.as_str().as_bytes();
                    match_ignore_ascii_case!(match name {
                        b"none" => 0,
                        b"full" => 1,
                        b"incremental" => 2,
                        _ => {
                            return Err(LimboError::InvalidArgument(
                                "invalid auto vacuum mode".to_string(),
                            ));
                        }
                    })
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
            Ok((program, TransactionMode::None))
        }
        PragmaName::IntegrityCheck => unreachable!("integrity_check cannot be set"),
        PragmaName::UnstableCaptureDataChangesConn => {
            let value = parse_string(&value)?;
            // todo(sivukhin): ideally, we should consistently update capture_data_changes connection flag only after successfull execution of schema change statement
            // but for now, let's keep it as is...
            let opts = CaptureDataChangesMode::parse(&value)?;
            if let Some(table) = &opts.table() {
                if resolver.schema.get_table(table).is_none() {
                    program = translate_create_table(
                        QualifiedName {
                            db_name: None,
                            name: ast::Name::exact(table.to_string()),
                            alias: None,
                        },
                        resolver,
                        false,
                        true, // if_not_exists
                        ast::CreateTableBody::ColumnsAndConstraints {
                            columns: turso_cdc_table_columns(),
                            constraints: vec![],
                            options: ast::TableOptions::NONE,
                        },
                        program,
                        &connection,
                    )?;
                }
            }
            connection.set_capture_data_changes(opts);
            Ok((program, TransactionMode::Write))
        }
        PragmaName::DatabaseList => unreachable!("database_list cannot be set"),
        PragmaName::QueryOnly => query_pragma(
            PragmaName::QueryOnly,
            resolver.schema,
            Some(value),
            pager,
            connection,
            program,
        ),
        PragmaName::FreelistCount => query_pragma(
            PragmaName::FreelistCount,
            resolver.schema,
            Some(value),
            pager,
            connection,
            program,
        ),
        PragmaName::EncryptionKey => {
            let value = parse_string(&value)?;
            let key = EncryptionKey::from_hex_string(&value)?;
            connection.set_encryption_key(key)?;
            Ok((program, TransactionMode::None))
        }
        PragmaName::EncryptionCipher => {
            let value = parse_string(&value)?;
            let cipher = CipherMode::try_from(value.as_str())?;
            connection.set_encryption_cipher(cipher)?;
            Ok((program, TransactionMode::None))
        }
        PragmaName::Synchronous => {
            use crate::SyncMode;

            let mode = match value {
                Expr::Name(name) => {
                    let name_bytes = name.as_str().as_bytes();
                    match_ignore_ascii_case!(match name_bytes {
                        b"OFF" | b"FALSE" | b"NO" | b"0" => SyncMode::Off,
                        _ => SyncMode::Full,
                    })
                }
                Expr::Literal(Literal::Numeric(n)) => match n.as_str() {
                    "0" => SyncMode::Off,
                    _ => SyncMode::Full,
                },
                _ => SyncMode::Full,
            };

            connection.set_sync_mode(mode);
            Ok((program, TransactionMode::None))
        }
        PragmaName::DataSyncRetry => {
            let retry_enabled = match value {
                Expr::Name(name) => {
                    let name_bytes = name.as_str().as_bytes();
                    match_ignore_ascii_case!(match name_bytes {
                        b"ON" | b"TRUE" | b"YES" | b"1" => true,
                        _ => false,
                    })
                }
                Expr::Literal(Literal::Numeric(n)) => !matches!(n.as_str(), "0"),
                _ => false,
            };

            connection.set_data_sync_retry(retry_enabled);
            Ok((program, TransactionMode::None))
        }
    }
}

fn query_pragma(
    pragma: PragmaName,
    schema: &Schema,
    value: Option<ast::Expr>,
    pager: Arc<Pager>,
    connection: Arc<crate::Connection>,
    mut program: ProgramBuilder,
) -> crate::Result<(ProgramBuilder, TransactionMode)> {
    let register = program.alloc_register();
    match pragma {
        PragmaName::ApplicationId => {
            program.emit_insn(Insn::ReadCookie {
                db: 0,
                dest: register,
                cookie: Cookie::ApplicationId,
            });
            program.add_pragma_result_column(pragma.to_string());
            program.emit_result_row(register, 1);
            Ok((program, TransactionMode::Read))
        }
        PragmaName::BusyTimeout => {
            program.emit_int(connection.get_busy_timeout().as_millis() as i64, register);
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
            Ok((program, TransactionMode::None))
        }
        PragmaName::CacheSize => {
            program.emit_int(connection.get_cache_size() as i64, register);
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
            Ok((program, TransactionMode::None))
        }
        PragmaName::DatabaseList => {
            let base_reg = register;
            program.alloc_registers(2);

            // Get all databases (main + attached) and emit a row for each
            let all_databases = connection.list_all_databases();
            for (seq_number, name, file_path) in all_databases {
                // seq (sequence number)
                program.emit_int(seq_number as i64, base_reg);

                // name (alias)
                program.emit_string8(name, base_reg + 1);

                // file path
                program.emit_string8(file_path, base_reg + 2);

                program.emit_result_row(base_reg, 3);
            }

            let pragma = pragma_for(&pragma);
            for col_name in pragma.columns.iter() {
                program.add_pragma_result_column(col_name.to_string());
            }
            Ok((program, TransactionMode::None))
        }
        PragmaName::Encoding => {
            let encoding = pager
                .io
                .block(|| pager.with_header(|header| header.text_encoding))
                .unwrap_or_default()
                .to_string();
            program.emit_string8(encoding, register);
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
            Ok((program, TransactionMode::None))
        }
        PragmaName::JournalMode => {
            // Use the JournalMode opcode to get the current journal mode
            program.emit_insn(Insn::JournalMode {
                db: 0,
                dest: register,
                new_mode: None,
            });
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
            Ok((program, TransactionMode::None))
        }
        PragmaName::LegacyFileFormat => Ok((program, TransactionMode::None)),
        PragmaName::WalCheckpoint => {
            // Checkpoint uses 3 registers: P1, P2, P3. Ref Insn::Checkpoint for more info.
            // Allocate two more here as one was allocated at the top.
            let mode = match value {
                Some(ast::Expr::Name(name)) => {
                    let mode_name = normalize_ident(name.as_str());
                    CheckpointMode::from_str(&mode_name).map_err(|e| {
                        LimboError::ParseError(format!("Unknown Checkpoint Mode: {e}"))
                    })?
                }
                _ => CheckpointMode::Passive {
                    upper_bound_inclusive: None,
                },
            };

            program.alloc_registers(2);
            program.emit_insn(Insn::Checkpoint {
                database: 0,
                checkpoint_mode: mode,
                dest: register,
            });
            program.emit_result_row(register, 3);
            Ok((program, TransactionMode::None))
        }
        PragmaName::ModuleList => {
            let modules = connection.get_syms_vtab_mods();
            for module in modules {
                program.emit_string8(module.to_string(), register);
                program.emit_result_row(register, 1);
            }

            program.add_pragma_result_column(pragma.to_string());
            Ok((program, TransactionMode::None))
        }
        PragmaName::PageCount => {
            program.emit_insn(Insn::PageCount {
                db: 0,
                dest: register,
            });
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
            Ok((program, TransactionMode::Read))
        }
        PragmaName::MaxPageCount => {
            program.emit_insn(Insn::MaxPgcnt {
                db: 0,
                dest: register,
                new_max: 0, // 0 means just return current max
            });
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
            Ok((program, TransactionMode::Read))
        }
        PragmaName::TableInfo => {
            let name = match value {
                Some(ast::Expr::Name(name)) => Some(normalize_ident(name.as_str())),
                _ => None,
            };

            let base_reg = register;
            program.alloc_registers(5);
            if let Some(name) = name {
                if let Some(table) = schema.get_table(&name) {
                    emit_columns_for_table_info(&mut program, table.columns(), base_reg);
                } else if let Some(view_mutex) = schema.get_materialized_view(&name) {
                    let view = view_mutex.lock().unwrap();
                    let flat_columns = view.column_schema.flat_columns();
                    emit_columns_for_table_info(&mut program, &flat_columns, base_reg);
                } else if let Some(view) = schema.get_view(&name) {
                    emit_columns_for_table_info(&mut program, &view.columns, base_reg);
                }
            }
            let col_names = ["cid", "name", "type", "notnull", "dflt_value", "pk"];
            for name in col_names {
                program.add_pragma_result_column(name.into());
            }
            Ok((program, TransactionMode::None))
        }
        PragmaName::UserVersion => {
            program.emit_insn(Insn::ReadCookie {
                db: 0,
                dest: register,
                cookie: Cookie::UserVersion,
            });
            program.add_pragma_result_column(pragma.to_string());
            program.emit_result_row(register, 1);
            Ok((program, TransactionMode::Read))
        }
        PragmaName::SchemaVersion => {
            program.emit_insn(Insn::ReadCookie {
                db: 0,
                dest: register,
                cookie: Cookie::SchemaVersion,
            });
            program.add_pragma_result_column(pragma.to_string());
            program.emit_result_row(register, 1);
            Ok((program, TransactionMode::Read))
        }
        PragmaName::PageSize => {
            program.emit_int(
                pager
                    .io
                    .block(|| pager.with_header(|header| header.page_size.get()))
                    .unwrap_or(connection.get_page_size().get()) as i64,
                register,
            );
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
            Ok((program, TransactionMode::None))
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
            Ok((program, TransactionMode::None))
        }
        PragmaName::IntegrityCheck => {
            translate_integrity_check(schema, &mut program)?;
            Ok((program, TransactionMode::Read))
        }
        PragmaName::UnstableCaptureDataChangesConn => {
            let pragma = pragma_for(&pragma);
            let second_column = program.alloc_register();
            let opts = connection.get_capture_data_changes();
            program.emit_string8(opts.mode_name().to_string(), register);
            if let Some(table) = &opts.table() {
                program.emit_string8(table.to_string(), second_column);
            } else {
                program.emit_null(second_column, None);
            }
            program.emit_result_row(register, 2);
            program.add_pragma_result_column(pragma.columns[0].to_string());
            program.add_pragma_result_column(pragma.columns[1].to_string());
            Ok((program, TransactionMode::Read))
        }
        PragmaName::QueryOnly => {
            if let Some(value_expr) = value {
                let is_query_only = match value_expr {
                    ast::Expr::Literal(Literal::Numeric(i)) => i.parse::<i64>().unwrap() != 0,
                    ast::Expr::Literal(Literal::String(..)) | ast::Expr::Name(..) => {
                        let s = match &value_expr {
                            ast::Expr::Literal(Literal::String(s)) => s.as_bytes(),
                            ast::Expr::Name(n) => n.as_str().as_bytes(),
                            _ => unreachable!(),
                        };
                        match_ignore_ascii_case!(match s {
                            b"1" | b"on" | b"true" => true,
                            _ => false,
                        })
                    }
                    _ => {
                        return Err(LimboError::ParseError(format!(
                            "Invalid value for PRAGMA query_only: {value_expr:?}"
                        )));
                    }
                };
                connection.set_query_only(is_query_only);
                return Ok((program, TransactionMode::None));
            };

            let register = program.alloc_register();
            let is_query_only = connection.get_query_only();
            program.emit_int(is_query_only as i64, register);
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());

            Ok((program, TransactionMode::None))
        }
        PragmaName::FreelistCount => {
            let value = pager.freepage_list();
            let register = program.alloc_register();
            program.emit_int(value as i64, register);
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
            Ok((program, TransactionMode::None))
        }
        PragmaName::EncryptionKey => {
            let msg = {
                if connection.encryption_key.read().is_some() {
                    "encryption key is set for this session"
                } else {
                    "encryption key is not set for this session"
                }
            };
            let register = program.alloc_register();
            program.emit_string8(msg.to_string(), register);
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
            Ok((program, TransactionMode::None))
        }
        PragmaName::EncryptionCipher => {
            if let Some(cipher) = connection.get_encryption_cipher_mode() {
                let register = program.alloc_register();
                program.emit_string8(cipher.to_string(), register);
                program.emit_result_row(register, 1);
                program.add_pragma_result_column(pragma.to_string());
            }
            Ok((program, TransactionMode::None))
        }
        PragmaName::Synchronous => {
            let mode = connection.get_sync_mode();
            let register = program.alloc_register();
            program.emit_int(mode as i64, register);
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
            Ok((program, TransactionMode::None))
        }
        PragmaName::DataSyncRetry => {
            let retry_enabled = connection.get_data_sync_retry();
            let register = program.alloc_register();
            program.emit_int(retry_enabled as i64, register);
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
            Ok((program, TransactionMode::None))
        }
    }
}

/// Helper function to emit column information for PRAGMA table_info
/// Used by both tables and views since they now have the same column emission logic
fn emit_columns_for_table_info(
    program: &mut ProgramBuilder,
    columns: &[crate::schema::Column],
    base_reg: usize,
) {
    // According to the SQLite documentation: "The 'cid' column should not be taken to
    // mean more than 'rank within the current result set'."
    // Therefore, we enumerate only after filtering out hidden columns.
    for (i, column) in columns.iter().filter(|col| !col.hidden).enumerate() {
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

fn update_auto_vacuum_mode(
    auto_vacuum_mode: AutoVacuumMode,
    largest_root_page_number: u32,
    pager: Arc<Pager>,
) -> crate::Result<()> {
    pager.io.block(|| {
        pager.with_header_mut(|header| {
            header.vacuum_mode_largest_root_page = largest_root_page_number.into()
        })
    })?;
    pager.set_auto_vacuum_mode(auto_vacuum_mode);
    Ok(())
}

fn update_cache_size(
    value: i64,
    pager: Arc<Pager>,
    connection: Arc<crate::Connection>,
) -> crate::Result<()> {
    let mut cache_size_unformatted: i64 = value;

    let mut cache_size = if cache_size_unformatted < 0 {
        let kb = cache_size_unformatted.abs().saturating_mul(1024);
        let page_size = pager
            .io
            .block(|| pager.with_header(|header| header.page_size))
            .unwrap_or_default()
            .get() as i64;
        if page_size == 0 {
            return Err(LimboError::InternalError(
                "Page size cannot be zero".to_string(),
            ));
        }
        kb / page_size
    } else {
        value
    };

    if cache_size > CacheSize::MAX_SAFE {
        cache_size = 0;
        cache_size_unformatted = 0;
    }

    if cache_size < 0 {
        cache_size = 0;
        cache_size_unformatted = 0;
    }

    let final_cache_size = if cache_size < CacheSize::MIN {
        cache_size_unformatted = CacheSize::MIN;
        CacheSize::MIN
    } else {
        cache_size
    };

    connection.set_cache_size(cache_size_unformatted as i32);

    pager
        .change_page_cache_size(final_cache_size as usize)
        .map_err(|e| LimboError::InternalError(format!("Failed to update page cache size: {e}")))?;

    Ok(())
}

pub const TURSO_CDC_DEFAULT_TABLE_NAME: &str = "turso_cdc";
fn turso_cdc_table_columns() -> Vec<ColumnDefinition> {
    vec![
        ast::ColumnDefinition {
            col_name: ast::Name::exact("change_id".to_string()),
            col_type: Some(ast::Type {
                name: "INTEGER".to_string(),
                size: None,
            }),
            constraints: vec![ast::NamedColumnConstraint {
                name: None,
                constraint: ast::ColumnConstraint::PrimaryKey {
                    order: None,
                    conflict_clause: None,
                    auto_increment: true,
                },
            }],
        },
        ast::ColumnDefinition {
            col_name: ast::Name::exact("change_time".to_string()),
            col_type: Some(ast::Type {
                name: "INTEGER".to_string(),
                size: None,
            }),
            constraints: vec![],
        },
        ast::ColumnDefinition {
            col_name: ast::Name::exact("change_type".to_string()),
            col_type: Some(ast::Type {
                name: "INTEGER".to_string(),
                size: None,
            }),
            constraints: vec![],
        },
        ast::ColumnDefinition {
            col_name: ast::Name::exact("table_name".to_string()),
            col_type: Some(ast::Type {
                name: "TEXT".to_string(),
                size: None,
            }),
            constraints: vec![],
        },
        ast::ColumnDefinition {
            col_name: ast::Name::exact("id".to_string()),
            col_type: None,
            constraints: vec![],
        },
        ast::ColumnDefinition {
            col_name: ast::Name::exact("before".to_string()),
            col_type: Some(ast::Type {
                name: "BLOB".to_string(),
                size: None,
            }),
            constraints: vec![],
        },
        ast::ColumnDefinition {
            col_name: ast::Name::exact("after".to_string()),
            col_type: Some(ast::Type {
                name: "BLOB".to_string(),
                size: None,
            }),
            constraints: vec![],
        },
        ast::ColumnDefinition {
            col_name: ast::Name::exact("updates".to_string()),
            col_type: Some(ast::Type {
                name: "BLOB".to_string(),
                size: None,
            }),
            constraints: vec![],
        },
    ]
}

fn update_page_size(connection: Arc<crate::Connection>, page_size: u32) -> crate::Result<()> {
    connection.reset_page_size(page_size)?;
    Ok(())
}
