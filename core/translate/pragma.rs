//! VDBE bytecode generation for pragma statements.
//! More info: https://www.sqlite.org/pragma.html.

use chrono::Datelike;
use std::rc::Rc;
use std::sync::Arc;
use turso_sqlite3_parser::ast::{self, ColumnDefinition, Expr, Literal, Name};
use turso_sqlite3_parser::ast::{PragmaName, QualifiedName};

use crate::pragma::pragma_for;
use crate::schema::Schema;
use crate::schema::Table as SchemaTable;
use crate::storage::pager::AutoVacuumMode;
use crate::storage::sqlite3_ondisk::CacheSize;
use crate::storage::wal::CheckpointMode;
use crate::translate::schema::translate_create_table;
use crate::util::{normalize_ident, parse_signed_number, parse_string, IOExt as _};
use crate::vdbe::builder::{ProgramBuilder, ProgramBuilderOpts};
use crate::vdbe::insn::{Cookie, Insn};
use crate::{bail_parse_error, CaptureDataChangesMode, LimboError, SymbolTable, Value};
use std::str::FromStr;
use strum::IntoEnumIterator;

use super::integrity_check::translate_integrity_check;
use crate::storage::pager::Pager;
use crate::translate::emitter::TransactionMode;

fn list_pragmas(program: &mut ProgramBuilder) {
    for x in PragmaName::iter() {
        let register = program.emit_string8_new_reg(x.to_string());
        program.emit_result_row(register, 1);
    }
    program.add_pragma_result_column("pragma_list".into());
}

#[allow(clippy::too_many_arguments)]
pub fn translate_pragma(
    schema: &Schema,
    syms: &SymbolTable,
    name: &ast::QualifiedName,
    body: Option<ast::PragmaBody>,
    pager: Rc<Pager>,
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

    // Special handling for schema-qualified PRAGMAs that need the name
    if let PragmaName::TableList = pragma {
        let (mut program, mode) = translate_table_list(name, body, connection, program)?;
        match mode {
            TransactionMode::Read => program.begin_read_operation(),
            TransactionMode::Write => program.begin_write_operation(),
            TransactionMode::None => {}
        }
        return Ok(program);
    }

    let (mut program, mode) = match body {
        None => query_pragma(pragma, schema, None, pager, connection, program)?,
        Some(ast::PragmaBody::Equals(value) | ast::PragmaBody::Call(value)) => match pragma {
            PragmaName::TableInfo => {
                query_pragma(pragma, schema, Some(value), pager, connection, program)?
            }
            _ => update_pragma(pragma, schema, syms, value, pager, connection, program)?,
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
    }

    Ok(program)
}

fn update_pragma(
    pragma: PragmaName,
    schema: &Schema,
    syms: &SymbolTable,
    value: ast::Expr,
    pager: Rc<Pager>,
    connection: Arc<crate::Connection>,
    mut program: ProgramBuilder,
) -> crate::Result<(ProgramBuilder, TransactionMode)> {
    match pragma {
        PragmaName::ApplicationId => {
            let data = parse_signed_number(&value)?;
            let app_id_value = match data {
                Value::Integer(i) => i as i32,
                Value::Float(f) => f as i32,
                _ => unreachable!(),
            };

            program.emit_insn(Insn::SetCookie {
                db: 0,
                cookie: Cookie::ApplicationId,
                value: app_id_value,
                p5: 1,
            });
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
            schema,
            Some(value),
            pager,
            connection,
            program,
        ),
        PragmaName::PageCount => query_pragma(
            PragmaName::PageCount,
            schema,
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
                    let name = name.as_str().to_lowercase();
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
            Ok((program, TransactionMode::None))
        }
        PragmaName::IntegrityCheck => unreachable!("integrity_check cannot be set"),
        PragmaName::UnstableCaptureDataChangesConn => {
            let value = parse_string(&value)?;
            // todo(sivukhin): ideally, we should consistently update capture_data_changes connection flag only after successfull execution of schema change statement
            // but for now, let's keep it as is...
            let opts = CaptureDataChangesMode::parse(&value)?;
            if let Some(table) = &opts.table() {
                // make sure that we have table created
                program = translate_create_table(
                    QualifiedName::single(ast::Name::from_str(table)),
                    false,
                    ast::CreateTableBody::columns_and_constraints_from_definition(
                        turso_cdc_table_columns(),
                        None,
                        ast::TableOptions::NONE,
                    )
                    .unwrap(),
                    true,
                    schema,
                    syms,
                    program,
                )?;
            }
            connection.set_capture_data_changes(opts);
            Ok((program, TransactionMode::Write))
        }
        PragmaName::DatabaseList => unreachable!("database_list cannot be set"),
        PragmaName::TableList => unreachable!("table_list cannot be set"),
        PragmaName::QueryOnly => query_pragma(
            PragmaName::QueryOnly,
            schema,
            Some(value),
            pager,
            connection,
            program,
        ),
    }
}

fn query_pragma(
    pragma: PragmaName,
    schema: &Schema,
    value: Option<ast::Expr>,
    pager: Rc<Pager>,
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
        // TableList handled in translate_pragma to support schema/table filters
        PragmaName::TableList => unreachable!("table_list handled in translate_pragma"),
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
                _ => CheckpointMode::Passive,
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
            let table = match value {
                Some(ast::Expr::Name(name)) => {
                    let tbl = normalize_ident(name.as_str());
                    schema.get_table(&tbl)
                }
                _ => None,
            };

            let base_reg = register;
            program.alloc_registers(5);
            if let Some(table) = table {
                // According to the SQLite documentation: "The 'cid' column should not be taken to
                // mean more than 'rank within the current result set'."
                // Therefore, we enumerate only after filtering out hidden columns.
                for (i, column) in table.columns().iter().filter(|col| !col.hidden).enumerate() {
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
                    .unwrap_or(connection.get_page_size()) as i64,
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
                    ast::Expr::Literal(Literal::String(ref s))
                    | ast::Expr::Name(Name::Ident(ref s)) => {
                        let s = s.to_lowercase();
                        s == "1" || s == "on" || s == "true"
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
    }
}

fn translate_table_list(
    name: &ast::QualifiedName,
    body: Option<ast::PragmaBody>,
    connection: Arc<crate::Connection>,
    mut program: ProgramBuilder,
) -> crate::Result<(ProgramBuilder, TransactionMode)> {
    // Add columns first
    let pragma = pragma_for(&PragmaName::TableList);
    for col_name in pragma.columns.iter() {
        program.add_pragma_result_column(col_name.to_string());
    }

    // Determine schema filter
    // If name.db_name is present and not "main", use attached schema by that name, else main
    let filter_schema: Option<String> = name
        .db_name
        .as_ref()
        .map(|n| crate::util::normalize_ident(n.as_str()));

    // Determine table-name filter from value Expr (handles name, 'string', or parentheses)
    fn extract_ident(e: &ast::Expr) -> Option<String> {
        match e {
            ast::Expr::Name(n) => {
                let s = n.as_str();
                if s.len() >= 2 && s.starts_with('\'') && s.ends_with('\'') {
                    Some(crate::util::normalize_ident(&s[1..s.len()-1]))
                } else {
                    Some(crate::util::normalize_ident(s))
                }
            }
            ast::Expr::Literal(ast::Literal::String(s)) => Some(crate::util::normalize_ident(s)),
            ast::Expr::Parenthesized(v) if !v.is_empty() => extract_ident(&v[0]),
            _ => None,
        }
    }
    let filter_table: Option<String> = match body {
        Some(ast::PragmaBody::Equals(ref v)) | Some(ast::PragmaBody::Call(ref v)) => extract_ident(v),
        _ => None,
    };

    let mut emit_for = |db_id: usize, db_alias: &str| {
        connection.with_schema(db_id, |schema| {
            for table in schema.tables.values() {
                let tname = table.get_name();
                if let Some(ref ft) = filter_table {
                    if !tname.eq_ignore_ascii_case(ft) {
                        continue;
                    }
                }
                let base = program.alloc_register();
                // Reserve registers for remaining columns
                program.alloc_registers(5);
                // schema
                program.emit_string8(db_alias.to_string(), base);
                // name
                program.emit_string8(tname.to_string(), base + 1);
                // type
                let typ = if matches!(&**table, SchemaTable::Virtual(_)) {
                    "virtual"
                } else {
                    // Turso does not support views or shadow tables yet
                    "table"
                };
                program.emit_string8(typ.to_string(), base + 2);
                // ncol: include all columns currently tracked
                program.emit_int(table.columns().len() as i64, base + 3);
                // wr: 1 if WITHOUT ROWID, 0 otherwise (per SQLite spec for table_list)
                let wr = match table.btree() {
                    Some(btree) => if btree.has_rowid { 0 } else { 1 },
                    None => 0,
                };
                program.emit_int(wr, base + 4);
                // strict
                let strict = match table.btree() {
                    Some(btree) => if btree.is_strict { 1 } else { 0 },
                    None => 0,
                };
                program.emit_int(strict, base + 5);
                program.emit_result_row(base, 6);
            }
        });
    };

    match filter_schema.as_deref() {
        Some("main") | None => {
            if filter_schema.is_none() || filter_schema.as_deref() == Some("main") {
                emit_for(0, "main");
            }
            // If no schema filter, include attached schemas too
            if filter_schema.is_none() {
                for db_name in connection.list_attached_databases() {
                    if let Some((idx, _db)) = connection.get_attached_database(&db_name) {
                        emit_for(idx, &db_name);
                    }
                }
            }
        }
        Some("temp") => {
            // temp maps to database_id 1 per our implementation
            emit_for(1, "temp");
        }
        Some(other) => {
            if let Some((idx, _db)) = connection.get_attached_database(other) {
                emit_for(idx, other);
            }
        }
    }

    Ok((program, TransactionMode::None))
}

fn update_auto_vacuum_mode(
    auto_vacuum_mode: AutoVacuumMode,
    largest_root_page_number: u32,
    pager: Rc<Pager>,
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
    pager: Rc<Pager>,
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
            col_name: ast::Name::from_str("change_id"),
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
            col_name: ast::Name::from_str("change_time"),
            col_type: Some(ast::Type {
                name: "INTEGER".to_string(),
                size: None,
            }),
            constraints: vec![],
        },
        ast::ColumnDefinition {
            col_name: ast::Name::from_str("change_type"),
            col_type: Some(ast::Type {
                name: "INTEGER".to_string(),
                size: None,
            }),
            constraints: vec![],
        },
        ast::ColumnDefinition {
            col_name: ast::Name::from_str("table_name"),
            col_type: Some(ast::Type {
                name: "TEXT".to_string(),
                size: None,
            }),
            constraints: vec![],
        },
        ast::ColumnDefinition {
            col_name: ast::Name::from_str("id"),
            col_type: None,
            constraints: vec![],
        },
        ast::ColumnDefinition {
            col_name: ast::Name::from_str("before"),
            col_type: Some(ast::Type {
                name: "BLOB".to_string(),
                size: None,
            }),
            constraints: vec![],
        },
        ast::ColumnDefinition {
            col_name: ast::Name::from_str("after"),
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
