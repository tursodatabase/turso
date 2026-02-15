//! VDBE bytecode generation for pragma statements.
//! More info: https://www.sqlite.org/pragma.html.

use crate::sync::Arc;
use chrono::Datelike;
use turso_macros::match_ignore_ascii_case;
use turso_parser::ast::{self, ColumnDefinition, Expr, Literal};
use turso_parser::ast::{PragmaName, QualifiedName};

use super::integrity_check::{
    translate_integrity_check, translate_quick_check, MAX_INTEGRITY_CHECK_ERRORS,
};
use crate::function::Func;
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
use crate::{bail_parse_error, CaptureDataChangesInfo, LimboError, Numeric, Value};
use std::str::FromStr;
use strum::IntoEnumIterator;

fn list_pragmas(program: &mut ProgramBuilder) {
    for x in PragmaName::iter() {
        let register = program.emit_string8_new_reg(x.to_string());
        program.emit_result_row(register, 1);
    }
    program.add_pragma_result_column("pragma_list".into());
}

/// Parse max_errors from an optional value expression.
/// Returns the parsed integer if value is a numeric literal, otherwise returns the default.
fn parse_max_errors_from_value(value: &Option<Expr>) -> usize {
    match value {
        Some(Expr::Literal(Literal::Numeric(n))) => {
            n.parse::<usize>().unwrap_or(MAX_INTEGRITY_CHECK_ERRORS)
        }
        _ => MAX_INTEGRITY_CHECK_ERRORS,
    }
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
        None => query_pragma(pragma, resolver, None, pager, connection, program)?,
        Some(ast::PragmaBody::Equals(value) | ast::PragmaBody::Call(value)) => match pragma {
            // These pragmas take a parameter but are queries, not setters
            PragmaName::IndexInfo
            | PragmaName::IndexXinfo
            | PragmaName::IndexList
            | PragmaName::TableList
            | PragmaName::TableInfo
            | PragmaName::TableXinfo
            | PragmaName::IntegrityCheck
            | PragmaName::QuickCheck => {
                query_pragma(pragma, resolver, Some(*value), pager, connection, program)?
            }
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
    let parse_pragma_enabled = |expr: &ast::Expr| -> bool {
        if let Expr::Literal(Literal::Numeric(n)) = expr {
            return !matches!(n.as_str(), "0");
        };
        let name_bytes = match expr {
            Expr::Literal(Literal::Keyword(name)) => name.as_bytes(),
            Expr::Name(name) | Expr::Id(name) => name.as_str().as_bytes(),
            _ => "".as_bytes(),
        };
        match_ignore_ascii_case!(match name_bytes {
            b"ON" | b"TRUE" | b"YES" | b"1" => true,
            _ => false,
        })
    };
    match pragma {
        PragmaName::ApplicationId => {
            let data = parse_signed_number(&value)?;
            let app_id_value = match data {
                Value::Numeric(Numeric::Integer(i)) => i as i32,
                Value::Numeric(Numeric::Float(f)) => f64::from(f) as i32,
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
                Value::Numeric(Numeric::Integer(i)) => i as i32,
                Value::Numeric(Numeric::Float(f)) => f64::from(f) as i32,
                _ => bail_parse_error!("expected integer, got {:?}", data),
            };
            let busy_timeout_ms = busy_timeout_ms.max(0);
            connection.set_busy_timeout(std::time::Duration::from_millis(busy_timeout_ms as u64));
            Ok((program, TransactionMode::Write))
        }
        PragmaName::CacheSize => {
            let cache_size = match parse_signed_number(&value)? {
                Value::Numeric(Numeric::Integer(size)) => size,
                Value::Numeric(Numeric::Float(size)) => f64::from(size) as i64,
                _ => bail_parse_error!("Invalid value for cache size pragma"),
            };
            update_cache_size(cache_size, pager, connection)?;
            Ok((program, TransactionMode::None))
        }
        PragmaName::CacheSpill => {
            let enabled = parse_pragma_enabled(&value);
            connection.get_pager().set_spill_enabled(enabled);
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
            resolver,
            Some(value),
            pager,
            connection,
            program,
        ),
        PragmaName::ModuleList => Ok((program, TransactionMode::None)),
        PragmaName::PageCount => query_pragma(
            PragmaName::PageCount,
            resolver,
            None,
            pager,
            connection,
            program,
        ),
        PragmaName::MaxPageCount => {
            let data = parse_signed_number(&value)?;
            let max_page_count_value = match data {
                Value::Numeric(Numeric::Integer(i)) => i as usize,
                Value::Numeric(Numeric::Float(f)) => f64::from(f) as usize,
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
                Value::Numeric(Numeric::Integer(i)) => i as i32,
                Value::Numeric(Numeric::Float(f)) => f64::from(f) as i32,
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
        PragmaName::TableXinfo => {
            // because we need control over the write parameter for the transaction,
            // this should be unreachable. We have to force-call query_pragma before
            // getting here
            unreachable!();
        }
        PragmaName::PageSize => {
            let page_size = match parse_signed_number(&value)? {
                Value::Numeric(Numeric::Integer(size)) => size,
                Value::Numeric(Numeric::Float(size)) => f64::from(size) as i64,
                _ => bail_parse_error!("Invalid value for page size pragma"),
            };
            update_page_size(connection, page_size as u32)?;
            Ok((program, TransactionMode::None))
        }
        PragmaName::AutoVacuum => {
            // Check if autovacuum is enabled in database opts
            if !connection.db.opts.enable_autovacuum {
                return Err(LimboError::InvalidArgument(
                    "Autovacuum is not enabled. Use --experimental-autovacuum flag to enable it."
                        .to_string(),
                ));
            }

            let is_empty = is_database_empty(resolver.schema, &pager)?;
            tracing::debug!(
                "Checking if database is empty for auto_vacuum pragma: {}",
                is_empty
            );

            if !is_empty {
                // SQLite's behavior is to silently ignore this pragma if the database is not empty.
                tracing::debug!("Attempted to set auto_vacuum, database is not empty so we are ignoring pragma.");
                return Ok((program, TransactionMode::None));
            }

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
        PragmaName::QuickCheck => unreachable!("quick_check cannot be set"),
        PragmaName::UnstableCaptureDataChangesConn => {
            let value = parse_string(&value)?;
            let opts = CaptureDataChangesInfo::parse(&value, Some(TURSO_CDC_CURRENT_VERSION.to_string()))?;
            if let Some(info) = &opts {
                if resolver.schema.get_table(&info.table).is_none() {
                    program = translate_create_table(
                        QualifiedName {
                            db_name: None,
                            name: ast::Name::exact(info.table.to_string()),
                            alias: None,
                        },
                        resolver,
                        false,
                        true, // if_not_exists
                        ast::CreateTableBody::ColumnsAndConstraints {
                            columns: turso_cdc_table_columns(),
                            constraints: vec![],
                            options: ast::TableOptions::empty(),
                        },
                        program,
                        &connection,
                    )?;

                    // InitCdcVersion creates the version table, records the version,
                    // and enables CDC — done at execution time so version table
                    // operations are not captured by CDC.
                    program.emit_insn(Insn::InitCdcVersion {
                        cdc_table_name: info.table.to_string(),
                        version: TURSO_CDC_CURRENT_VERSION.to_string(),
                        cdc_mode: value,
                    });
                } else {
                    connection.set_capture_data_changes_info(opts);
                }
            } else {
                connection.set_capture_data_changes_info(opts);
            }
            Ok((program, TransactionMode::Write))
        }
        PragmaName::DatabaseList => unreachable!("database_list cannot be set"),
        PragmaName::IndexInfo => unreachable!("index_info cannot be set"),
        PragmaName::IndexXinfo => unreachable!("index_xinfo cannot be set"),
        PragmaName::IndexList => unreachable!("index_list cannot be set"),
        PragmaName::TableList => unreachable!("table_list cannot be set"),
        PragmaName::QueryOnly => query_pragma(
            PragmaName::QueryOnly,
            resolver,
            Some(value),
            pager,
            connection,
            program,
        ),
        PragmaName::FreelistCount => query_pragma(
            PragmaName::FreelistCount,
            resolver,
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
            let mode = if let Expr::Literal(Literal::Numeric(n)) = &value {
                match n.as_str() {
                    "0" => SyncMode::Off,
                    "1" => SyncMode::Normal,
                    _ => SyncMode::Full, // SQLite defaults to NORMAL for invalid values, but we want to default to a higher durability level so deviating here.
                }
            } else {
                let name_bytes = match &value {
                    Expr::Literal(Literal::Keyword(name)) => name.as_bytes(),
                    Expr::Name(name) | Expr::Id(name) => name.as_str().as_bytes(),
                    _ => b"",
                };
                match_ignore_ascii_case!(match name_bytes {
                    b"OFF" | b"0" => SyncMode::Off,
                    b"NORMAL" | b"1" => SyncMode::Normal,
                    _ => SyncMode::Full,
                })
            };
            connection.set_sync_mode(mode);
            Ok((program, TransactionMode::None))
        }
        PragmaName::DataSyncRetry => {
            let retry_enabled = parse_pragma_enabled(&value);
            connection.set_data_sync_retry(retry_enabled);
            Ok((program, TransactionMode::None))
        }
        PragmaName::MvccCheckpointThreshold => {
            let threshold = match parse_signed_number(&value)? {
                Value::Numeric(Numeric::Integer(size)) if size >= -1 => size,
                _ => bail_parse_error!(
                    "mvcc_checkpoint_threshold must be -1, 0, or a positive integer"
                ),
            };

            connection.set_mvcc_checkpoint_threshold(threshold)?;
            Ok((program, TransactionMode::None))
        }
        PragmaName::ForeignKeys => {
            let enabled = parse_pragma_enabled(&value);
            connection.set_foreign_keys_enabled(enabled);
            Ok((program, TransactionMode::None))
        }
        PragmaName::IgnoreCheckConstraints => {
            let enabled = parse_pragma_enabled(&value);
            connection.set_check_constraints_ignored(enabled);
            Ok((program, TransactionMode::None))
        }
        #[cfg(target_vendor = "apple")]
        PragmaName::Fullfsync => {
            let enabled = parse_pragma_enabled(&value);
            let sync_type = if enabled {
                crate::io::FileSyncType::FullFsync
            } else {
                crate::io::FileSyncType::Fsync
            };
            connection.set_sync_type(sync_type);
            Ok((program, TransactionMode::None))
        }
        PragmaName::TempStore => {
            use crate::TempStore;
            // Try to parse as a string first (default, file, memory)
            let temp_store = if let Expr::Literal(Literal::Numeric(n)) = &value {
                // Numeric value: 0, 1, or 2
                match n.as_str() {
                    "0" => TempStore::Default,
                    "1" => TempStore::File,
                    "2" => TempStore::Memory,
                    _ => bail_parse_error!("temp_store must be 0, 1, 2, DEFAULT, FILE, or MEMORY"),
                }
            } else {
                // Try as keyword/identifier: DEFAULT, FILE, MEMORY
                let name_bytes = match &value {
                    Expr::Literal(Literal::Keyword(name)) => name.as_bytes(),
                    Expr::Name(name) | Expr::Id(name) => name.as_str().as_bytes(),
                    Expr::Literal(Literal::String(s)) => s.as_bytes(),
                    _ => bail_parse_error!("temp_store must be 0, 1, 2, DEFAULT, FILE, or MEMORY"),
                };
                match_ignore_ascii_case!(match name_bytes {
                    b"DEFAULT" | b"0" => TempStore::Default,
                    b"FILE" | b"1" => TempStore::File,
                    b"MEMORY" | b"2" => TempStore::Memory,
                    _ => bail_parse_error!("temp_store must be 0, 1, 2, DEFAULT, FILE, or MEMORY"),
                })
            };
            connection.set_temp_store(temp_store);
            Ok((program, TransactionMode::None))
        }
        PragmaName::FunctionList => query_pragma(
            PragmaName::FunctionList,
            resolver,
            Some(value),
            pager,
            connection,
            program,
        ),
    }
}

fn query_pragma(
    pragma: PragmaName,
    resolver: &Resolver,
    value: Option<ast::Expr>,
    pager: Arc<Pager>,
    connection: Arc<crate::Connection>,
    mut program: ProgramBuilder,
) -> crate::Result<(ProgramBuilder, TransactionMode)> {
    let schema = resolver.schema;
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
        PragmaName::CacheSpill => {
            let spill_enabled = connection.get_pager().get_spill_enabled();
            program.emit_int(spill_enabled as i64, register);
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
            program.add_pragma_result_column("busy".to_string());
            program.add_pragma_result_column("log".to_string());
            program.add_pragma_result_column("checkpointed".to_string());
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
        PragmaName::FunctionList => {
            // 6 columns: name, builtin, type, enc, narg, flags
            let base_reg = register;
            program.alloc_registers(5);

            const SQLITE_DETERMINISTIC: i64 = 0x800;
            const SQLITE_INNOCUOUS: i64 = 0x200000;

            // Built-in functions
            for entry in Func::builtin_function_list() {
                let mut flags: i64 = 0;
                if entry.deterministic {
                    flags |= SQLITE_DETERMINISTIC;
                }
                flags |= SQLITE_INNOCUOUS;

                program.emit_string8(entry.name, base_reg);
                program.emit_int(1, base_reg + 1); // builtin = 1
                program.emit_string8(entry.func_type.to_string(), base_reg + 2);
                program.emit_string8("utf8".to_string(), base_reg + 3);
                program.emit_int(entry.narg as i64, base_reg + 4);
                program.emit_int(flags, base_reg + 5);
                program.emit_result_row(base_reg, 6);
            }

            // External (extension) functions
            for (name, is_agg, argc) in connection.get_syms_functions() {
                let func_type = if is_agg { "a" } else { "s" };
                program.emit_string8(name, base_reg);
                program.emit_int(0, base_reg + 1); // builtin = 0
                program.emit_string8(func_type.to_string(), base_reg + 2);
                program.emit_string8("utf8".to_string(), base_reg + 3);
                program.emit_int(argc as i64, base_reg + 4);
                program.emit_int(0, base_reg + 5); // flags = 0 for extensions
                program.emit_result_row(base_reg, 6);
            }

            let pragma_meta = pragma_for(&pragma);
            for col_name in pragma_meta.columns.iter() {
                program.add_pragma_result_column(col_name.to_string());
            }
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
        PragmaName::IndexInfo => {
            let index_name = match value {
                Some(ast::Expr::Name(name)) => Some(normalize_ident(name.as_str())),
                _ => None,
            };

            let base_reg = register;
            // 3 columns: seqno, cid, name
            program.alloc_registers(2);

            if let Some(index_name) = index_name {
                let index = schema
                    .indexes
                    .values()
                    .flatten()
                    .find(|idx| idx.name.eq_ignore_ascii_case(&index_name));

                if let Some(index) = index {
                    for (seqno, col) in index.columns.iter().enumerate() {
                        program.emit_int(seqno as i64, base_reg);
                        program.emit_int(col.pos_in_table as i64, base_reg + 1);
                        program.emit_string8(col.name.clone(), base_reg + 2);
                        program.emit_result_row(base_reg, 3);
                    }
                }
            }

            let pragma_meta = pragma_for(&pragma);
            for col_name in pragma_meta.columns.iter() {
                program.add_pragma_result_column(col_name.to_string());
            }
            Ok((program, TransactionMode::None))
        }
        PragmaName::IndexXinfo => {
            let index_name = match value {
                Some(ast::Expr::Name(name)) => Some(normalize_ident(name.as_str())),
                _ => None,
            };

            let base_reg = register;
            // 6 columns: seqno, cid, name, desc, coll, key
            program.alloc_registers(5);

            if let Some(index_name) = index_name {
                let index = schema
                    .indexes
                    .values()
                    .flatten()
                    .find(|idx| idx.name.eq_ignore_ascii_case(&index_name));

                if let Some(index) = index {
                    for (seqno, col) in index.columns.iter().enumerate() {
                        let desc = matches!(col.order, ast::SortOrder::Desc);
                        let coll = col
                            .collation
                            .map(|c| c.to_string().to_uppercase())
                            .unwrap_or_else(|| "BINARY".to_string());

                        program.emit_int(seqno as i64, base_reg);
                        program.emit_int(col.pos_in_table as i64, base_reg + 1);
                        program.emit_string8(col.name.clone(), base_reg + 2);
                        program.emit_int(desc as i64, base_reg + 3);
                        program.emit_string8(coll, base_reg + 4);
                        program.emit_int(1, base_reg + 5); // key column
                        program.emit_result_row(base_reg, 6);
                    }

                    // Emit trailing rowid row if the index has one
                    if index.has_rowid {
                        let seqno = index.columns.len();
                        program.emit_int(seqno as i64, base_reg);
                        program.emit_int(-1, base_reg + 1);
                        program.emit_string8(String::new(), base_reg + 2);
                        program.emit_int(0, base_reg + 3);
                        program.emit_string8("BINARY".to_string(), base_reg + 4);
                        program.emit_int(0, base_reg + 5); // not a key column
                        program.emit_result_row(base_reg, 6);
                    }
                }
            }

            let pragma_meta = pragma_for(&pragma);
            for col_name in pragma_meta.columns.iter() {
                program.add_pragma_result_column(col_name.to_string());
            }
            Ok((program, TransactionMode::None))
        }
        PragmaName::IndexList => {
            let table_name = match value {
                Some(ast::Expr::Name(name)) => Some(normalize_ident(name.as_str())),
                _ => None,
            };

            let base_reg = register;
            // 5 columns: seq, name, unique, origin, partial
            program.alloc_registers(4);

            if let Some(table_name) = table_name {
                if let Some(table) = schema.get_table(&table_name) {
                    let pk_cols: Vec<String> = table
                        .btree()
                        .map(|bt| {
                            bt.primary_key_columns
                                .iter()
                                .map(|(name, _)| name.clone())
                                .collect()
                        })
                        .unwrap_or_default();

                    for (seq, index) in schema.get_indices(&table_name).enumerate() {
                        let origin = if index.name.starts_with("sqlite_autoindex_") {
                            let idx_cols: Vec<&str> =
                                index.columns.iter().map(|c| c.name.as_str()).collect();
                            if idx_cols.len() == pk_cols.len()
                                && idx_cols
                                    .iter()
                                    .zip(pk_cols.iter())
                                    .all(|(a, b)| a.eq_ignore_ascii_case(b))
                            {
                                "pk"
                            } else {
                                "u"
                            }
                        } else {
                            "c"
                        };

                        program.emit_int(seq as i64, base_reg);
                        program.emit_string8(index.name.clone(), base_reg + 1);
                        program.emit_int(index.unique as i64, base_reg + 2);
                        program.emit_string8(origin.to_string(), base_reg + 3);
                        program.emit_int(index.where_clause.is_some() as i64, base_reg + 4);
                        program.emit_result_row(base_reg, 5);
                    }
                }
            }

            let pragma_meta = pragma_for(&pragma);
            for col_name in pragma_meta.columns.iter() {
                program.add_pragma_result_column(col_name.to_string());
            }
            Ok((program, TransactionMode::None))
        }
        PragmaName::TableList => {
            let name = match value {
                Some(ast::Expr::Name(name)) => Some(normalize_ident(name.as_str())),
                _ => None,
            };

            let base_reg = register;
            // 6 columns: schema, name, type, ncol, wr, strict
            program.alloc_registers(5);

            let emit_table_row = |program: &mut ProgramBuilder,
                                  name: &str,
                                  obj_type: &str,
                                  ncol: usize,
                                  wr: bool,
                                  strict: bool| {
                program.emit_string8("main".to_string(), base_reg);
                program.emit_string8(name.to_string(), base_reg + 1);
                program.emit_string8(obj_type.to_string(), base_reg + 2);
                program.emit_int(ncol as i64, base_reg + 3);
                program.emit_int(wr as i64, base_reg + 4);
                program.emit_int(strict as i64, base_reg + 5);
                program.emit_result_row(base_reg, 6);
            };

            if let Some(name) = name {
                // Specific table/view lookup
                if let Some(table) = schema.get_table(&name) {
                    let (wr, strict) = match table.btree() {
                        Some(bt) => (!bt.has_rowid, bt.is_strict),
                        None => (false, false),
                    };
                    emit_table_row(
                        &mut program,
                        table.get_name(),
                        "table",
                        table.columns().len(),
                        wr,
                        strict,
                    );
                } else if let Some(view) = schema.get_view(&name) {
                    emit_table_row(
                        &mut program,
                        &view.name,
                        "view",
                        view.columns.len(),
                        false,
                        false,
                    );
                }
            } else {
                // List all tables and views (only BTree tables, not built-in virtual tables)
                for table in schema.tables.values() {
                    let Some(bt) = table.btree() else {
                        continue;
                    };
                    emit_table_row(
                        &mut program,
                        &bt.name,
                        "table",
                        bt.columns.len(),
                        !bt.has_rowid,
                        bt.is_strict,
                    );
                }
                for view in schema.views.values() {
                    emit_table_row(
                        &mut program,
                        &view.name,
                        "view",
                        view.columns.len(),
                        false,
                        false,
                    );
                }
            }

            let pragma_meta = pragma_for(&pragma);
            for col_name in pragma_meta.columns.iter() {
                program.add_pragma_result_column(col_name.to_string());
            }
            Ok((program, TransactionMode::None))
        }
        PragmaName::TableInfo => {
            let name = match value {
                Some(ast::Expr::Name(name)) => Some(normalize_ident(name.as_str())),
                _ => None,
            };

            let base_reg = register;
            // we need 6 registers, but first register was allocated at the beginning  of the "query_pragma" function
            program.alloc_registers(5);
            if let Some(name) = name {
                if let Some(table) = schema.get_table(&name) {
                    emit_columns_for_table_info(&mut program, table.columns(), base_reg, false);
                } else if let Some(view_mutex) = schema.get_materialized_view(&name) {
                    let view = view_mutex.lock();
                    let flat_columns = view.column_schema.flat_columns();
                    emit_columns_for_table_info(&mut program, &flat_columns, base_reg, false);
                } else if let Some(view) = schema.get_view(&name) {
                    emit_columns_for_table_info(&mut program, &view.columns, base_reg, false);
                }
            }
            let col_names = ["cid", "name", "type", "notnull", "dflt_value", "pk"];
            for name in col_names {
                program.add_pragma_result_column(name.into());
            }
            Ok((program, TransactionMode::None))
        }
        PragmaName::TableXinfo => {
            let name = match value {
                Some(ast::Expr::Name(name)) => Some(normalize_ident(name.as_str())),
                _ => None,
            };

            let base_reg = register;
            // we need 7 registers, but first register was allocated at the beginning  of the "query_pragma" function
            program.alloc_registers(6);
            if let Some(name) = name {
                if let Some(table) = schema.get_table(&name) {
                    emit_columns_for_table_info(&mut program, table.columns(), base_reg, true);
                } else if let Some(view_mutex) = schema.get_materialized_view(&name) {
                    let view = view_mutex.lock();
                    let flat_columns = view.column_schema.flat_columns();
                    emit_columns_for_table_info(&mut program, &flat_columns, base_reg, true);
                } else if let Some(view) = schema.get_view(&name) {
                    emit_columns_for_table_info(&mut program, &view.columns, base_reg, true);
                }
            }
            let col_names = [
                "cid",
                "name",
                "type",
                "notnull",
                "dflt_value",
                "pk",
                "hidden",
            ];
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
                    .unwrap_or_else(|_| connection.get_page_size().get()) as i64,
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
            let max_errors = parse_max_errors_from_value(&value);
            translate_integrity_check(schema, &mut program, resolver, max_errors)?;
            Ok((program, TransactionMode::Read))
        }
        PragmaName::QuickCheck => {
            let max_errors = parse_max_errors_from_value(&value);
            translate_quick_check(schema, &mut program, resolver, max_errors)?;
            Ok((program, TransactionMode::Read))
        }
        PragmaName::UnstableCaptureDataChangesConn => {
            let pragma = pragma_for(&pragma);
            let second_column = program.alloc_register();
            let third_column = program.alloc_register();
            let opts = connection.get_capture_data_changes_info();
            match opts.as_ref() {
                Some(info) => {
                    program.emit_string8(info.mode_name().to_string(), register);
                    program.emit_string8(info.table.clone(), second_column);
                    match &info.version {
                        Some(v) => program.emit_string8(v.clone(), third_column),
                        None => program.emit_null(third_column, None),
                    }
                }
                None => {
                    program.emit_string8("off".to_string(), register);
                    program.emit_null(second_column, None);
                    program.emit_null(third_column, None);
                }
            }
            program.emit_result_row(register, 3);
            program.add_pragma_result_column(pragma.columns[0].to_string());
            program.add_pragma_result_column(pragma.columns[1].to_string());
            program.add_pragma_result_column(pragma.columns[2].to_string());
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
        PragmaName::MvccCheckpointThreshold => {
            let threshold = connection.mvcc_checkpoint_threshold()?;
            let register = program.alloc_register();
            program.emit_int(threshold, register);
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
            Ok((program, TransactionMode::None))
        }
        PragmaName::ForeignKeys => {
            let enabled = connection.foreign_keys_enabled();
            let register = program.alloc_register();
            program.emit_int(enabled as i64, register);
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
            Ok((program, TransactionMode::None))
        }
        PragmaName::IgnoreCheckConstraints => {
            let ignored = connection.check_constraints_ignored();
            let register = program.alloc_register();
            program.emit_int(ignored as i64, register);
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
            Ok((program, TransactionMode::None))
        }
        #[cfg(target_vendor = "apple")]
        PragmaName::Fullfsync => {
            let enabled = connection.get_sync_type() == crate::io::FileSyncType::FullFsync;
            let register = program.alloc_register();
            program.emit_int(enabled as i64, register);
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
            Ok((program, TransactionMode::None))
        }
        PragmaName::TempStore => {
            let temp_store = connection.get_temp_store();
            let register = program.alloc_register();
            program.emit_int(temp_store as i64, register);
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
    extended: bool,
) {
    // According to the SQLite documentation: "The 'cid' column should not be taken to
    // mean more than 'rank within the current result set'."
    // Therefore, we enumerate only after filtering out hidden columns (if extended set to false).
    let mut cid = 0;
    for column in columns.iter() {
        // Determine column type which will be used for filtering in table_info pragma or as "hidden" column for table_xinfo pragma.
        //
        // SQLite docs about table_xinfo:
        // > The output has the same columns as for PRAGMA table_info plus a column, "hidden",
        // > whose value signifies a normal column (0), a dynamic or stored generated column (2 or 3),
        // > or a hidden column in a virtual table (1). The rows for which this field is non-zero are those omitted for PRAGMA table_info.
        //
        // (see https://sqlite.org/pragma.html#pragma_table_xinfo)
        let column_type = if column.hidden() {
            // hidden column
            1
        } else {
            // normal column
            0
        };

        if !extended && column_type != 0 {
            // This pragma (table_info) does not show information about generated columns or hidden columns.
            continue;
        }

        // cid
        program.emit_int(cid as i64, base_reg);
        cid += 1;

        // name
        program.emit_string8(column.name.clone().unwrap_or_default(), base_reg + 1);

        // type
        program.emit_string8(column.ty_str.clone(), base_reg + 2);

        // notnull
        program.emit_bool(column.notnull(), base_reg + 3);

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
        program.emit_bool(column.primary_key(), base_reg + 5);

        if extended {
            program.emit_int(column_type, base_reg + 6);
        }

        program.emit_result_row(base_reg, 6 + if extended { 1 } else { 0 });
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
#[allow(dead_code)]
pub const TURSO_CDC_VERSION_TABLE_NAME: &str = "turso_cdc_version";
pub const TURSO_CDC_CURRENT_VERSION: &str = "v1";
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

fn is_database_empty(schema: &Schema, pager: &Arc<Pager>) -> crate::Result<bool> {
    if schema.tables.len() > 1 {
        return Ok(false);
    }
    if let Some(table_arc) = schema.tables.values().next() {
        let table_name = match table_arc.as_ref() {
            crate::schema::Table::BTree(tbl) => &tbl.name,
            crate::schema::Table::Virtual(tbl) => &tbl.name,
            crate::schema::Table::FromClauseSubquery(tbl) => &tbl.name,
        };

        if table_name != "sqlite_schema" {
            return Ok(false);
        }
    }

    let db_size_result = pager
        .io
        .block(|| pager.with_header(|header| header.database_size.get()));

    match db_size_result {
        Err(_) => Ok(true),
        Ok(0 | 1) => Ok(true),
        Ok(_) => Ok(false),
    }
}
