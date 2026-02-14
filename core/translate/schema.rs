use crate::sync::Arc;

use crate::ast;
use crate::ext::VTabImpl;
use crate::function::Func;
use crate::schema::{
    create_table, BTreeTable, ColDef, Column, SchemaObjectType, Table, Type,
    RESERVED_TABLE_PREFIXES, SQLITE_SEQUENCE_TABLE_NAME, TURSO_TYPES_TABLE_NAME,
};
use crate::stats::STATS_TABLE;
use crate::storage::pager::CreateBTreeFlags;
use crate::translate::emitter::{
    emit_cdc_autocommit_commit, emit_cdc_full_record, emit_cdc_insns, prepare_cdc_if_necessary,
    OperationMode, Resolver,
};
use crate::translate::expr::{walk_expr, WalkControl};
use crate::translate::fkeys::emit_fk_drop_table_check;
use crate::translate::planner::ROWID_STRS;
use crate::translate::{ProgramBuilder, ProgramBuilderOpts};
use crate::util::normalize_ident;
use crate::util::PRIMARY_KEY_AUTOMATIC_INDEX_NAME_PREFIX;
use crate::vdbe::builder::CursorType;
use crate::vdbe::insn::{
    to_u16, {CmpInsFlags, Cookie, InsertFlags, Insn, RegisterOrLiteral},
};
use crate::Connection;
use crate::{bail_parse_error, CaptureDataChangesExt, Result};

use turso_ext::VTabKind;

/// Validate a CHECK constraint expression at CREATE TABLE / ALTER TABLE ADD COLUMN time.
/// Rejects non-existent columns, non-existent functions, aggregates, window functions,
/// bind parameters, and subqueries.
pub(crate) fn validate_check_expr(
    expr: &ast::Expr,
    table_name: &str,
    column_names: &[&str],
    resolver: &Resolver,
) -> Result<()> {
    let normalized_table = normalize_ident(table_name);
    walk_expr(expr, &mut |e: &ast::Expr| -> Result<WalkControl> {
        match e {
            ast::Expr::Id(name) | ast::Expr::Name(name) => {
                let n = normalize_ident(name.as_str());
                if !column_names.iter().any(|c| normalize_ident(c) == n)
                    && !ROWID_STRS.iter().any(|r| r.eq_ignore_ascii_case(&n))
                {
                    bail_parse_error!("no such column: {}", name.as_str());
                }
            }
            ast::Expr::Qualified(tbl, col) => {
                if normalize_ident(tbl.as_str()) != normalized_table {
                    bail_parse_error!("no such column: {}.{}", tbl.as_str(), col.as_str());
                }
                let cn = normalize_ident(col.as_str());
                if !column_names.iter().any(|c| normalize_ident(c) == cn)
                    && !ROWID_STRS.iter().any(|r| r.eq_ignore_ascii_case(&cn))
                {
                    bail_parse_error!("no such column: {}", col.as_str());
                }
            }
            ast::Expr::DoublyQualified(db, tbl, col) => {
                bail_parse_error!(
                    "no such column: {}.{}.{}",
                    db.as_str(),
                    tbl.as_str(),
                    col.as_str()
                );
            }
            ast::Expr::FunctionCall {
                name,
                args,
                filter_over,
                ..
            } => {
                if filter_over.over_clause.is_some() {
                    bail_parse_error!("misuse of window function {}()", name.as_str());
                }
                if let Some(func) = resolver.resolve_function(name.as_str(), args.len()) {
                    if matches!(func, Func::Agg(..)) {
                        bail_parse_error!("misuse of aggregate function {}()", name.as_str());
                    }
                } else {
                    bail_parse_error!("no such function: {}", name.as_str());
                }
            }
            ast::Expr::FunctionCallStar { name, filter_over } => {
                if filter_over.over_clause.is_some() {
                    bail_parse_error!("misuse of window function {}()", name.as_str());
                }
                if let Some(func) = resolver.resolve_function(name.as_str(), 0) {
                    if matches!(func, Func::Agg(..)) {
                        bail_parse_error!("misuse of aggregate function {}()", name.as_str());
                    }
                } else {
                    bail_parse_error!("no such function: {}", name.as_str());
                }
            }
            ast::Expr::Variable(_) => {
                bail_parse_error!("parameters prohibited in CHECK constraints");
            }
            ast::Expr::Subquery(_) | ast::Expr::Exists(_) | ast::Expr::InSelect { .. } => {
                bail_parse_error!("subqueries prohibited in CHECK constraints");
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(())
}

fn validate(
    body: &ast::CreateTableBody,
    table_name: &str,
    connection: &Connection,
    resolver: &Resolver,
) -> Result<()> {
    if let ast::CreateTableBody::ColumnsAndConstraints {
        options,
        columns,
        constraints,
    } = &body
    {
        if options.contains_without_rowid() {
            bail_parse_error!("WITHOUT ROWID tables are not supported");
        }
        if options.contains_strict() && !connection.experimental_strict_enabled() {
            bail_parse_error!(
                "STRICT tables are an experimental feature. Enable them with --experimental-strict flag"
            );
        }
        let column_names: Vec<&str> = columns.iter().map(|c| c.col_name.as_str()).collect();
        for i in 0..columns.len() {
            let col_i = &columns[i];
            for constraint in &col_i.constraints {
                match &constraint.constraint {
                    ast::ColumnConstraint::Check(expr) => {
                        validate_check_expr(expr, table_name, &column_names, resolver)?;
                    }
                    ast::ColumnConstraint::Generated { .. } => {
                        bail_parse_error!("GENERATED columns are not supported yet");
                    }
                    ast::ColumnConstraint::NotNull {
                        conflict_clause, ..
                    }
                    | ast::ColumnConstraint::PrimaryKey {
                        conflict_clause, ..
                    } if conflict_clause.is_some() => {
                        bail_parse_error!(
                            "ON CONFLICT clauses are not supported yet in column definitions"
                        );
                    }
                    _ => {}
                }
            }
            for j in &columns[(i + 1)..] {
                if col_i
                    .col_name
                    .as_str()
                    .eq_ignore_ascii_case(j.col_name.as_str())
                {
                    bail_parse_error!("duplicate column name: {}", j.col_name.as_str());
                }
            }
        }
        for constraint in constraints {
            if let ast::TableConstraint::Check(ref expr) = constraint.constraint {
                validate_check_expr(expr, table_name, &column_names, resolver)?;
            }
        }

        let is_strict = options.contains_strict();

        for c in columns {
            if let Some(ref col_type) = c.col_type {
                let type_name = &col_type.name;
                let name_bytes = type_name.as_bytes();
                let is_builtin = turso_macros::match_ignore_ascii_case!(match name_bytes {
                    b"INT" | b"INTEGER" | b"REAL" | b"TEXT" | b"BLOB" | b"ANY" => true,
                    _ => false,
                });

                if !is_builtin && is_strict {
                    // On non-STRICT tables any type name is allowed and is
                    // treated as a plain affinity hint (no encode/decode).
                    // Custom type validation only applies to STRICT tables.
                    let type_def = resolver.schema.get_type_def_unchecked(type_name);
                    {
                        match type_def {
                            None => {
                                bail_parse_error!(
                                    "unknown datatype for {}.{}: \"{}\"",
                                    table_name,
                                    c.col_name,
                                    type_name
                                );
                            }
                            Some(td) if !td.params.is_empty() => {
                                // Parametric type: verify the column provides the right
                                // number of parameters (e.g. numeric(10,2)).
                                let provided = match &col_type.size {
                                    Some(ast::TypeSize::TypeSize(_, _)) => 2,
                                    Some(ast::TypeSize::MaxSize(_)) => 1,
                                    None => 0,
                                };
                                if provided != td.params.len() {
                                    bail_parse_error!(
                                        "type \"{}\" requires {} parameter(s), got {}",
                                        type_name,
                                        td.params.len(),
                                        provided
                                    );
                                }
                            }
                            Some(_) => {}
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

pub fn translate_create_table(
    tbl_name: ast::QualifiedName,
    resolver: &Resolver,
    temporary: bool,
    if_not_exists: bool,
    body: ast::CreateTableBody,
    program: &mut ProgramBuilder,
    connection: &Connection,
) -> Result<()> {
    let database_id = resolver.resolve_database_id(&tbl_name)?;
    if crate::is_attached_db(database_id) {
        let schema_cookie = resolver.with_schema(database_id, |s| s.schema_version);
        program.begin_write_on_database(database_id, schema_cookie);
    }
    let normalized_tbl_name = normalize_ident(tbl_name.name.as_str());
    if temporary {
        bail_parse_error!("TEMPORARY table not supported yet");
    }
    validate(&body, &normalized_tbl_name, connection, resolver)?;

    let opts = ProgramBuilderOpts {
        num_cursors: 1,
        approx_num_insns: 30,
        approx_num_labels: 1,
    };
    program.extend(&opts);

    if !connection.is_mvcc_bootstrap_connection()
        && RESERVED_TABLE_PREFIXES
            .iter()
            .any(|prefix| normalized_tbl_name.starts_with(prefix))
        && !connection.is_nested_stmt()
    {
        bail_parse_error!(
            "Object name reserved for internal use: {}",
            tbl_name.name.as_str()
        );
    }

    // Check for name conflicts with existing schema objects
    if let Some(object_type) =
        resolver.with_schema(database_id, |s| s.get_object_type(&normalized_tbl_name))
    {
        match object_type {
            // IF NOT EXISTS suppresses errors for table/view conflicts
            SchemaObjectType::Table | SchemaObjectType::View if if_not_exists => {
                return Ok(());
            }
            _ => {
                let type_str = match object_type {
                    SchemaObjectType::Table => "table",
                    SchemaObjectType::View => "view",
                    SchemaObjectType::Index => "index",
                };
                bail_parse_error!("{} {} already exists", type_str, normalized_tbl_name);
            }
        }
    }

    let mut has_autoincrement = false;
    if let ast::CreateTableBody::ColumnsAndConstraints {
        columns,
        constraints,
        ..
    } = &body
    {
        for col in columns {
            for constraint in &col.constraints {
                if let ast::ColumnConstraint::PrimaryKey { auto_increment, .. } =
                    constraint.constraint
                {
                    if auto_increment {
                        has_autoincrement = true;
                        break;
                    }
                }
            }
            if has_autoincrement {
                break;
            }
        }
        if !has_autoincrement {
            for constraint in constraints {
                if let ast::TableConstraint::PrimaryKey { auto_increment, .. } =
                    constraint.constraint
                {
                    if auto_increment {
                        has_autoincrement = true;
                        break;
                    }
                }
            }
        }
    }

    let schema_master_table = resolver.schema().get_btree_table(SQLITE_TABLEID).unwrap();
    let sqlite_schema_cursor_id =
        program.alloc_cursor_id(CursorType::BTreeTable(schema_master_table));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: sqlite_schema_cursor_id,
        root_page: 1i64.into(),
        db: database_id,
    });
    let cdc_table = prepare_cdc_if_necessary(program, resolver.schema(), SQLITE_TABLEID)?;

    let created_sequence_table = if has_autoincrement
        && resolver.with_schema(database_id, |s| {
            s.get_table(SQLITE_SEQUENCE_TABLE_NAME).is_none()
        }) {
        let seq_table_root_reg = program.alloc_register();
        program.emit_insn(Insn::CreateBtree {
            db: database_id,
            root: seq_table_root_reg,
            flags: CreateBTreeFlags::new_table(),
        });

        let seq_sql = "CREATE TABLE sqlite_sequence(name,seq)";
        emit_schema_entry(
            program,
            resolver,
            sqlite_schema_cursor_id,
            cdc_table.as_ref().map(|x| x.0),
            SchemaEntryType::Table,
            SQLITE_SEQUENCE_TABLE_NAME,
            SQLITE_SEQUENCE_TABLE_NAME,
            seq_table_root_reg,
            Some(seq_sql.to_string()),
        )?;
        true
    } else {
        false
    };

    let sql = create_table_body_to_str(&tbl_name, &body)?;

    let parse_schema_label = program.allocate_label();
    // TODO: ReadCookie
    // TODO: If
    // TODO: SetCookie
    // TODO: SetCookie

    let table_root_reg = program.alloc_register();
    program.emit_insn(Insn::CreateBtree {
        db: database_id,
        root: table_root_reg,
        flags: CreateBTreeFlags::new_table(),
    });

    // Create an automatic index B-tree if needed
    //
    // NOTE: we are deviating from SQLite bytecode here. For some reason, SQLite first creates a placeholder entry
    // for the table in sqlite_schema, then writes the index to sqlite_schema, then UPDATEs the table placeholder entry
    // in sqlite_schema with actual data.
    //
    // What we do instead is:
    // 1. Create the table B-tree
    // 2. Create the index B-tree
    // 3. Add the table entry to sqlite_schema
    // 4. Add the index entry to sqlite_schema
    //
    // I.e. we skip the weird song and dance with the placeholder entry. Unclear why sqlite does this.
    // The sqlite code has this comment:
    //
    // "This just creates a place-holder record in the sqlite_schema table.
    // The record created does not contain anything yet.  It will be replaced
    // by the real entry in code generated at sqlite3EndTable()."
    //
    // References:
    // https://github.com/sqlite/sqlite/blob/95f6df5b8d55e67d1e34d2bff217305a2f21b1fb/src/build.c#L1355
    // https://github.com/sqlite/sqlite/blob/95f6df5b8d55e67d1e34d2bff217305a2f21b1fb/src/build.c#L2856-L2871
    // https://github.com/sqlite/sqlite/blob/95f6df5b8d55e67d1e34d2bff217305a2f21b1fb/src/build.c#L1334C5-L1336C65

    let index_regs = collect_autoindexes(&body, program, &normalized_tbl_name)?;
    if let Some(index_regs) = index_regs.as_ref() {
        for index_reg in index_regs.iter() {
            program.emit_insn(Insn::CreateBtree {
                db: database_id,
                root: *index_reg,
                flags: CreateBTreeFlags::new_index(),
            });
        }
    }

    let table = resolver.schema().get_btree_table(SQLITE_TABLEID).unwrap();
    let sqlite_schema_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: sqlite_schema_cursor_id,
        root_page: 1i64.into(),
        db: database_id,
    });

    let cdc_table = prepare_cdc_if_necessary(program, resolver.schema(), SQLITE_TABLEID)?;

    emit_schema_entry(
        program,
        resolver,
        sqlite_schema_cursor_id,
        cdc_table.as_ref().map(|x| x.0),
        SchemaEntryType::Table,
        &normalized_tbl_name,
        &normalized_tbl_name,
        table_root_reg,
        Some(sql),
    )?;

    if let Some(index_regs) = index_regs {
        for (idx, index_reg) in index_regs.into_iter().enumerate() {
            let index_name = format!(
                "{PRIMARY_KEY_AUTOMATIC_INDEX_NAME_PREFIX}{}_{}",
                normalized_tbl_name,
                idx + 1
            );
            emit_schema_entry(
                program,
                resolver,
                sqlite_schema_cursor_id,
                None,
                SchemaEntryType::Index,
                &index_name,
                &normalized_tbl_name,
                index_reg,
                None,
            )?;
        }
    }

    program.resolve_label(parse_schema_label, program.offset());
    let schema_version = resolver.with_schema(database_id, |s| s.schema_version);
    program.emit_insn(Insn::SetCookie {
        db: database_id,
        cookie: Cookie::SchemaVersion,
        value: schema_version as i32 + 1,
        p5: 0,
    });

    // TODO: remove format, it sucks for performance but is convenient
    let mut parse_schema_where_clause =
        format!("tbl_name = '{normalized_tbl_name}' AND type != 'trigger'");
    if created_sequence_table {
        parse_schema_where_clause.push_str(" OR tbl_name = 'sqlite_sequence'");
    }

    program.emit_insn(Insn::ParseSchema {
        db: database_id,
        where_clause: Some(parse_schema_where_clause),
    });

    // TODO: SqlExec

    Ok(())
}

#[derive(Debug, Clone, Copy)]
pub enum SchemaEntryType {
    Table,
    Index,
    View,
    Trigger,
}

impl SchemaEntryType {
    fn as_str(&self) -> &'static str {
        match self {
            SchemaEntryType::Table => "table",
            SchemaEntryType::Index => "index",
            SchemaEntryType::View => "view",
            SchemaEntryType::Trigger => "trigger",
        }
    }
}
pub const SQLITE_TABLEID: &str = "sqlite_schema";

#[allow(clippy::too_many_arguments)]
pub fn emit_schema_entry(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    sqlite_schema_cursor_id: usize,
    cdc_table_cursor_id: Option<usize>,
    entry_type: SchemaEntryType,
    name: &str,
    tbl_name: &str,
    root_page_reg: usize,
    sql: Option<String>,
) -> Result<()> {
    let rowid_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: sqlite_schema_cursor_id,
        rowid_reg,
        prev_largest_reg: 0,
    });

    let type_reg = program.emit_string8_new_reg(entry_type.as_str().to_string());
    program.emit_string8_new_reg(name.to_string());
    program.emit_string8_new_reg(tbl_name.to_string());

    let table_root_reg = program.alloc_register();
    if root_page_reg == 0 {
        program.emit_insn(Insn::Integer {
            dest: table_root_reg,
            value: 0, // virtual tables in sqlite always have rootpage=0
        });
    } else {
        program.emit_insn(Insn::Copy {
            src_reg: root_page_reg,
            dst_reg: table_root_reg,
            extra_amount: 0,
        });
    }

    let sql_reg = program.alloc_register();
    if let Some(sql) = sql {
        program.emit_string8(sql, sql_reg);
    } else {
        program.emit_null(sql_reg, None);
    }

    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u16(type_reg),
        count: to_u16(5),
        dest_reg: to_u16(record_reg),
        index_name: None,
        affinity_str: None,
    });

    program.emit_insn(Insn::Insert {
        cursor: sqlite_schema_cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: InsertFlags::new(),
        table_name: tbl_name.to_string(),
    });

    if let Some(cdc_table_cursor_id) = cdc_table_cursor_id {
        let after_record_reg = if program.capture_data_changes_info().has_after() {
            Some(record_reg)
        } else {
            None
        };
        emit_cdc_insns(
            program,
            resolver,
            OperationMode::INSERT,
            cdc_table_cursor_id,
            rowid_reg,
            None,
            after_record_reg,
            None,
            SQLITE_TABLEID,
        )?;
        emit_cdc_autocommit_commit(program, resolver, cdc_table_cursor_id)?;
    }
    Ok(())
}

/// Check if an automatic PRIMARY KEY index is required for the table.
/// If so, create a register for the index root page and return it.
///
/// An automatic PRIMARY KEY index is not required if:
/// - The table has no PRIMARY KEY
/// - The table has a single-column PRIMARY KEY whose typename is _exactly_ "INTEGER" e.g. not "INT".
///   In this case, the PRIMARY KEY column becomes an alias for the rowid.
///
/// Otherwise, an automatic PRIMARY KEY index is required.
fn collect_autoindexes(
    body: &ast::CreateTableBody,
    program: &mut ProgramBuilder,
    tbl_name: &str,
) -> Result<Option<Vec<usize>>> {
    let table = create_table(tbl_name, body, 0)?;

    let mut regs: Vec<usize> = Vec::new();

    // include UNIQUE singles, include PK single only if not rowid alias
    for us in table.unique_sets.iter().filter(|us| us.columns.len() == 1) {
        let (col_name, _sort) = us.columns.first().unwrap();
        let Some((_pos, col)) = table.get_column(col_name) else {
            bail_parse_error!("Column {col_name} not found in table {}", table.name);
        };

        let needs_index = if us.is_primary_key {
            !(col.primary_key() && col.is_rowid_alias())
        } else {
            // UNIQUE single needs an index
            true
        };

        if needs_index {
            regs.push(program.alloc_register());
        }
    }

    for _us in table.unique_sets.iter().filter(|us| us.columns.len() > 1) {
        regs.push(program.alloc_register());
    }
    if regs.is_empty() {
        Ok(None)
    } else {
        Ok(Some(regs))
    }
}

fn create_table_body_to_str(
    tbl_name: &ast::QualifiedName,
    body: &ast::CreateTableBody,
) -> crate::Result<String> {
    let mut sql = String::new();
    sql.push_str(format!("CREATE TABLE {} {}", tbl_name.name.as_ident(), body).as_str());
    match body {
        ast::CreateTableBody::ColumnsAndConstraints {
            columns: _,
            constraints: _,
            options: _,
        } => {}
        ast::CreateTableBody::AsSelect(_select) => {
            crate::bail_parse_error!("CREATE TABLE AS SELECT is not supported")
        }
    }
    Ok(sql)
}

fn create_vtable_body_to_str(vtab: &ast::CreateVirtualTable, module: Arc<VTabImpl>) -> String {
    let args = vtab
        .args
        .iter()
        .map(|arg| arg.to_string())
        .collect::<Vec<String>>()
        .join(", ");
    let if_not_exists = if vtab.if_not_exists {
        "IF NOT EXISTS "
    } else {
        ""
    };
    let ext_args = vtab
        .args
        .iter()
        .map(|a| turso_ext::Value::from_text(a.to_string()))
        .collect::<Vec<_>>();
    let schema = module
        .implementation
        .create_schema(ext_args)
        .unwrap_or_default();
    let vtab_args = if let Some(first_paren) = schema.find('(') {
        let closing_paren = schema.rfind(')').unwrap_or_default();
        &schema[first_paren..=closing_paren]
    } else {
        "()"
    };
    format!(
        "CREATE VIRTUAL TABLE {} {} USING {}{}\n /*{}{}*/",
        vtab.tbl_name.name.as_ident(),
        if_not_exists,
        vtab.module_name.as_ident(),
        if args.is_empty() {
            String::new()
        } else {
            format!("({args})")
        },
        vtab.tbl_name.name.as_ident(),
        vtab_args
    )
}

pub fn translate_create_virtual_table(
    vtab: ast::CreateVirtualTable,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
) -> Result<()> {
    if connection.mvcc_enabled() {
        bail_parse_error!("Virtual tables are not supported in MVCC mode");
    }
    let ast::CreateVirtualTable {
        if_not_exists,
        tbl_name,
        module_name,
        args,
    } = &vtab;

    let table_name = tbl_name.name.as_str().to_string();
    let module_name_str = module_name.as_str().to_string();
    let args_vec = args.clone();
    let Some(vtab_module) = resolver.symbol_table.vtab_modules.get(&module_name_str) else {
        bail_parse_error!("no such module: {}", module_name_str);
    };
    if !vtab_module.module_kind.eq(&VTabKind::VirtualTable) {
        bail_parse_error!("module {} is not a virtual table", module_name_str);
    };
    if resolver.schema().get_table(&table_name).is_some() {
        if *if_not_exists {
            return Ok(());
        }
        bail_parse_error!("Table {} already exists", tbl_name);
    }

    let opts = ProgramBuilderOpts {
        num_cursors: 2,
        approx_num_insns: 40,
        approx_num_labels: 2,
    };
    program.extend(&opts);
    let module_name_reg = program.emit_string8_new_reg(module_name_str.clone());
    let table_name_reg = program.emit_string8_new_reg(table_name.clone());
    let args_reg = if !args_vec.is_empty() {
        let args_start = program.alloc_register();

        // Emit string8 instructions for each arg
        for (i, arg) in args_vec.iter().enumerate() {
            program.emit_string8(arg.clone(), args_start + i);
        }
        let args_record_reg = program.alloc_register();

        // VCreate expects an array of args as a record
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u16(args_start),
            count: to_u16(args_vec.len()),
            dest_reg: to_u16(args_record_reg),
            index_name: None,
            affinity_str: None,
        });
        Some(args_record_reg)
    } else {
        None
    };

    program.emit_insn(Insn::VCreate {
        module_name: module_name_reg,
        table_name: table_name_reg,
        args_reg,
    });
    let table = resolver.schema().get_btree_table(SQLITE_TABLEID).unwrap();
    let sqlite_schema_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: sqlite_schema_cursor_id,
        root_page: 1i64.into(),
        db: crate::MAIN_DB_ID,
    });

    let cdc_table = prepare_cdc_if_necessary(program, resolver.schema(), SQLITE_TABLEID)?;
    let sql = create_vtable_body_to_str(&vtab, vtab_module.clone());
    emit_schema_entry(
        program,
        resolver,
        sqlite_schema_cursor_id,
        cdc_table.map(|x| x.0),
        SchemaEntryType::Table,
        tbl_name.name.as_str(),
        tbl_name.name.as_str(),
        0, // virtual tables dont have a root page
        Some(sql),
    )?;

    program.emit_insn(Insn::SetCookie {
        db: crate::MAIN_DB_ID,
        cookie: Cookie::SchemaVersion,
        value: resolver.schema().schema_version as i32 + 1,
        p5: 0,
    });
    let parse_schema_where_clause = format!("tbl_name = '{table_name}' AND type != 'trigger'");
    program.emit_insn(Insn::ParseSchema {
        db: sqlite_schema_cursor_id,
        where_clause: Some(parse_schema_where_clause),
    });

    Ok(())
}

/// Validates whether a DROP TABLE operation is allowed on the given table name.
fn validate_drop_table(
    resolver: &Resolver,
    tbl_name: &str,
    connection: &Arc<Connection>,
) -> Result<()> {
    if !connection.is_nested_stmt()
        && crate::schema::is_system_table(tbl_name)
        // special case, allow dropping `sqlite_stat1`
        && !tbl_name.eq_ignore_ascii_case(STATS_TABLE)
    {
        bail_parse_error!("Cannot drop system table {}", tbl_name);
    }
    // Check if this is a materialized view - if so, refuse to drop it with DROP TABLE
    if resolver.schema().is_materialized_view(tbl_name) {
        bail_parse_error!(
            "Cannot DROP TABLE on materialized view {tbl_name}. Use DROP VIEW instead.",
        );
    }
    Ok(())
}

pub fn translate_drop_table(
    tbl_name: ast::QualifiedName,
    resolver: &mut Resolver,
    if_exists: bool,
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
) -> Result<()> {
    let database_id = resolver.resolve_database_id(&tbl_name)?;
    let name = tbl_name.name.as_str();
    let opts = ProgramBuilderOpts {
        num_cursors: 4,
        approx_num_insns: 40,
        approx_num_labels: 4,
    };
    program.extend(&opts);

    if crate::is_attached_db(database_id) {
        let schema_cookie = resolver.with_schema(database_id, |s| s.schema_version);
        program.begin_write_on_database(database_id, schema_cookie);
    }

    let Some(table) = resolver.with_schema(database_id, |s| s.get_table(name)) else {
        if if_exists {
            return Ok(());
        }
        bail_parse_error!("No such table: {name}");
    };
    validate_drop_table(resolver, name, connection)?;
    // Check if foreign keys are enabled and if this table is referenced by foreign keys
    // Fire FK actions (CASCADE, SET NULL, SET DEFAULT) or check for violations (RESTRICT, NO ACTION)
    if connection.foreign_keys_enabled()
        && resolver.with_schema(database_id, |s| s.any_resolved_fks_referencing(name))
    {
        emit_fk_drop_table_check(program, resolver, name, connection, database_id)?;
    }
    let cdc_table = prepare_cdc_if_necessary(program, resolver.schema(), SQLITE_TABLEID)?;

    let null_reg = program.alloc_register(); //  r1
    program.emit_null(null_reg, None);
    let table_name_and_root_page_register = program.alloc_register(); //  r2, this register is special because it's first used to track table name and then moved root page
    let table_reg = program.emit_string8_new_reg(normalize_ident(tbl_name.name.as_str())); //  r3
    program.mark_last_insn_constant();
    let _table_type = program.emit_string8_new_reg("trigger".to_string()); //  r4
    program.mark_last_insn_constant();
    let row_id_reg = program.alloc_register(); //  r5

    let schema_table = resolver.schema().get_btree_table(SQLITE_TABLEID).unwrap();
    let sqlite_schema_cursor_id_0 = program.alloc_cursor_id(
        //  cursor 0
        CursorType::BTreeTable(schema_table.clone()),
    );
    program.emit_insn(Insn::OpenWrite {
        cursor_id: sqlite_schema_cursor_id_0,
        root_page: 1i64.into(),
        db: database_id,
    });

    //  1. Remove all entries from the schema table related to the table we are dropping (including triggers)
    //  loop to beginning of schema table
    let end_metadata_label = program.allocate_label();
    let metadata_loop = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: sqlite_schema_cursor_id_0,
        pc_if_empty: end_metadata_label,
    });
    program.preassign_label_to_next_insn(metadata_loop);

    // start loop on schema table
    program.emit_column_or_rowid(
        sqlite_schema_cursor_id_0,
        2,
        table_name_and_root_page_register,
    );
    let next_label = program.allocate_label();
    program.emit_insn(Insn::Ne {
        lhs: table_name_and_root_page_register,
        rhs: table_reg,
        target_pc: next_label,
        flags: CmpInsFlags::default(),
        collation: program.curr_collation(),
    });
    program.emit_insn(Insn::RowId {
        cursor_id: sqlite_schema_cursor_id_0,
        dest: row_id_reg,
    });
    if let Some((cdc_cursor_id, _)) = cdc_table {
        let table_type = program.emit_string8_new_reg("table".to_string()); // r4
        program.mark_last_insn_constant();

        let skip_cdc_label = program.allocate_label();

        let entry_type_reg = program.alloc_register();
        program.emit_column_or_rowid(sqlite_schema_cursor_id_0, 0, entry_type_reg);
        program.emit_insn(Insn::Ne {
            lhs: entry_type_reg,
            rhs: table_type,
            target_pc: skip_cdc_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });
        let before_record_reg = if program.capture_data_changes_info().has_before() {
            Some(emit_cdc_full_record(
                program,
                &schema_table.columns,
                sqlite_schema_cursor_id_0,
                row_id_reg,
                schema_table.is_strict,
            ))
        } else {
            None
        };
        emit_cdc_insns(
            program,
            resolver,
            OperationMode::DELETE,
            cdc_cursor_id,
            row_id_reg,
            before_record_reg,
            None,
            None,
            SQLITE_TABLEID,
        )?;
        program.resolve_label(skip_cdc_label, program.offset());
    }
    program.emit_insn(Insn::Delete {
        cursor_id: sqlite_schema_cursor_id_0,
        table_name: SQLITE_TABLEID.to_string(),
        is_part_of_update: false,
    });

    program.resolve_label(next_label, program.offset());
    program.emit_insn(Insn::Next {
        cursor_id: sqlite_schema_cursor_id_0,
        pc_if_next: metadata_loop,
    });
    program.preassign_label_to_next_insn(end_metadata_label);
    // end of loop on schema table
    if let Some((cdc_cursor_id, _)) = cdc_table {
        emit_cdc_autocommit_commit(program, resolver, cdc_cursor_id)?;
    }

    //  2. Destroy the indices within a loop
    let indices = resolver.schema().get_indices(tbl_name.name.as_str());
    for index in indices {
        if index.index_method.is_some() && !index.is_backing_btree_index() {
            // Index methods without backing btree need special destroy handling
            let cursor_id = program.alloc_cursor_index(None, index)?;
            program.emit_insn(Insn::IndexMethodDestroy {
                db: database_id,
                cursor_id,
            });
        } else {
            program.emit_insn(Insn::Destroy {
                db: database_id,
                root: index.root_page,
                former_root_reg: 0, //  no autovacuum (https://www.sqlite.org/opcode.html#Destroy)
                is_temp: 0,
            });
        }

        //  3. TODO: Open an ephemeral table, and read over triggers from schema table into ephemeral table
        //  Requires support via https://github.com/tursodatabase/turso/pull/768

        //  4. TODO: Open a write cursor to the schema table and re-insert all triggers into the sqlite schema table from the ephemeral table and delete old trigger
        //  Requires support via https://github.com/tursodatabase/turso/pull/768
    }

    //  3. Destroy the table structure
    match table.as_ref() {
        Table::BTree(table) => {
            program.emit_insn(Insn::Destroy {
                db: database_id,
                root: table.root_page,
                former_root_reg: table_name_and_root_page_register,
                is_temp: 0,
            });
        }
        Table::Virtual(vtab) => {
            // From what I see, TableValuedFunction is not stored in the schema as a table.
            // But this line here below is a safeguard in case this behavior changes in the future
            // And mirrors what SQLite does.
            if matches!(vtab.kind, turso_ext::VTabKind::TableValuedFunction) {
                return Err(crate::LimboError::ParseError(format!(
                    "table {} may not be dropped",
                    vtab.name
                )));
            }
            program.emit_insn(Insn::VDestroy {
                table_name: vtab.name.clone(),
                db: database_id,
            });
        }
        Table::FromClauseSubquery(..) => panic!("FromClauseSubquery can't be dropped"),
    };

    let schema_data_register = program.alloc_register();
    let schema_row_id_register = program.alloc_register();
    program.emit_null(schema_data_register, Some(schema_row_id_register));

    // All of the following processing needs to be done only if the table is not a virtual table
    if table.btree().is_some() {
        // 4. Open an ephemeral table, and read over the entry from the schema table whose root page was moved in the destroy operation

        // cursor id 1
        let sqlite_schema_cursor_id_1 =
            program.alloc_cursor_id(CursorType::BTreeTable(schema_table.clone()));
        let simple_table_rc = Arc::new(BTreeTable {
            root_page: 0, // Not relevant for ephemeral table definition
            name: "ephemeral_scratch".to_string(),
            has_rowid: true,
            has_autoincrement: false,
            primary_key_columns: vec![],
            columns: vec![Column::new(
                Some("rowid".to_string()),
                "INTEGER".to_string(),
                None,
                None,
                Type::Integer,
                None,
                ColDef::default(),
            )],
            is_strict: false,
            unique_sets: vec![],
            foreign_keys: vec![],
            check_constraints: vec![],
        });
        // cursor id 2
        let ephemeral_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(simple_table_rc));
        program.emit_insn(Insn::OpenEphemeral {
            cursor_id: ephemeral_cursor_id,
            is_table: true,
        });
        let if_not_label = program.allocate_label();
        program.emit_insn(Insn::IfNot {
            reg: table_name_and_root_page_register,
            target_pc: if_not_label,
            jump_if_null: true, //  jump anyway
        });
        program.emit_insn(Insn::OpenRead {
            cursor_id: sqlite_schema_cursor_id_1,
            root_page: 1i64,
            db: database_id,
        });

        let schema_column_0_register = program.alloc_register();
        let schema_column_1_register = program.alloc_register();
        let schema_column_2_register = program.alloc_register();
        let moved_to_root_page_register = program.alloc_register(); //  the register that will contain the root page number the last root page is moved to
        let schema_column_4_register = program.alloc_register();
        let prev_root_page_register = program.alloc_register(); //  the register that will contain the root page number that the last root page was on before VACUUM
        let _r14 = program.alloc_register(); //  Unsure why this register is allocated but putting it in here to make comparison with SQLite easier
        let new_record_register = program.alloc_register();

        //  Loop to copy over row id's from the schema table for rows that have the same root page as the one that was moved
        let copy_schema_to_temp_table_loop_end_label = program.allocate_label();
        let copy_schema_to_temp_table_loop = program.allocate_label();
        program.emit_insn(Insn::Rewind {
            cursor_id: sqlite_schema_cursor_id_1,
            pc_if_empty: copy_schema_to_temp_table_loop_end_label,
        });
        program.preassign_label_to_next_insn(copy_schema_to_temp_table_loop);
        // start loop on schema table
        program.emit_column_or_rowid(sqlite_schema_cursor_id_1, 3, prev_root_page_register);
        // The label and Insn::Ne are used to skip over any rows in the schema table that don't have the root page that was moved
        let next_label = program.allocate_label();
        program.emit_insn(Insn::Ne {
            lhs: prev_root_page_register,
            rhs: table_name_and_root_page_register,
            target_pc: next_label,
            flags: CmpInsFlags::default(),
            collation: program.curr_collation(),
        });
        program.emit_insn(Insn::RowId {
            cursor_id: sqlite_schema_cursor_id_1,
            dest: schema_row_id_register,
        });
        program.emit_insn(Insn::Insert {
            cursor: ephemeral_cursor_id,
            key_reg: schema_row_id_register,
            record_reg: schema_data_register,
            flag: InsertFlags::new(),
            table_name: "scratch_table".to_string(),
        });

        program.resolve_label(next_label, program.offset());
        program.emit_insn(Insn::Next {
            cursor_id: sqlite_schema_cursor_id_1,
            pc_if_next: copy_schema_to_temp_table_loop,
        });
        program.preassign_label_to_next_insn(copy_schema_to_temp_table_loop_end_label);
        // End loop to copy over row id's from the schema table for rows that have the same root page as the one that was moved

        program.resolve_label(if_not_label, program.offset());

        // 5. Open a write cursor to the schema table and re-insert the records placed in the ephemeral table but insert the correct root page now
        program.emit_insn(Insn::OpenWrite {
            cursor_id: sqlite_schema_cursor_id_1,
            root_page: 1i64.into(),
            db: database_id,
        });

        // Loop to copy over row id's from the ephemeral table and then re-insert into the schema table with the correct root page
        let copy_temp_table_to_schema_loop_end_label = program.allocate_label();
        let copy_temp_table_to_schema_loop = program.allocate_label();
        program.emit_insn(Insn::Rewind {
            cursor_id: ephemeral_cursor_id,
            pc_if_empty: copy_temp_table_to_schema_loop_end_label,
        });
        program.preassign_label_to_next_insn(copy_temp_table_to_schema_loop);
        //  start loop on schema table
        program.emit_insn(Insn::RowId {
            cursor_id: ephemeral_cursor_id,
            dest: schema_row_id_register,
        });
        //  the next_label and Insn::NotExists are used to skip patching any rows in the schema table that don't have the row id that was written to the ephemeral table
        let next_label = program.allocate_label();
        program.emit_insn(Insn::NotExists {
            cursor: sqlite_schema_cursor_id_1,
            rowid_reg: schema_row_id_register,
            target_pc: next_label,
        });
        program.emit_column_or_rowid(sqlite_schema_cursor_id_1, 0, schema_column_0_register);
        program.emit_column_or_rowid(sqlite_schema_cursor_id_1, 1, schema_column_1_register);
        program.emit_column_or_rowid(sqlite_schema_cursor_id_1, 2, schema_column_2_register);
        let root_page = table.get_root_page()?;
        program.emit_insn(Insn::Integer {
            value: root_page,
            dest: moved_to_root_page_register,
        });
        program.emit_column_or_rowid(sqlite_schema_cursor_id_1, 4, schema_column_4_register);
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u16(schema_column_0_register),
            count: to_u16(5),
            dest_reg: to_u16(new_record_register),
            index_name: None,
            affinity_str: None,
        });
        program.emit_insn(Insn::Delete {
            cursor_id: sqlite_schema_cursor_id_1,
            table_name: SQLITE_TABLEID.to_string(),
            is_part_of_update: false,
        });
        program.emit_insn(Insn::Insert {
            cursor: sqlite_schema_cursor_id_1,
            key_reg: schema_row_id_register,
            record_reg: new_record_register,
            flag: InsertFlags::new(),
            table_name: SQLITE_TABLEID.to_string(),
        });

        program.resolve_label(next_label, program.offset());
        program.emit_insn(Insn::Next {
            cursor_id: ephemeral_cursor_id,
            pc_if_next: copy_temp_table_to_schema_loop,
        });
        program.preassign_label_to_next_insn(copy_temp_table_to_schema_loop_end_label);
        // End loop to copy over row id's from the ephemeral table and then re-insert into the schema table with the correct root page
    }

    // if drops table, sequence table should reset.
    if let Some(seq_table) = resolver
        .schema()
        .get_table(SQLITE_SEQUENCE_TABLE_NAME)
        .and_then(|t| t.btree())
    {
        let seq_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(seq_table.clone()));
        let seq_table_name_reg = program.alloc_register();
        let dropped_table_name_reg =
            program.emit_string8_new_reg(tbl_name.name.as_str().to_string());
        program.mark_last_insn_constant();

        program.emit_insn(Insn::OpenWrite {
            cursor_id: seq_cursor_id,
            root_page: seq_table.root_page.into(),
            db: database_id,
        });

        let end_loop_label = program.allocate_label();
        let loop_start_label = program.allocate_label();

        program.emit_insn(Insn::Rewind {
            cursor_id: seq_cursor_id,
            pc_if_empty: end_loop_label,
        });

        program.preassign_label_to_next_insn(loop_start_label);

        program.emit_column_or_rowid(seq_cursor_id, 0, seq_table_name_reg);

        let continue_loop_label = program.allocate_label();
        program.emit_insn(Insn::Ne {
            lhs: seq_table_name_reg,
            rhs: dropped_table_name_reg,
            target_pc: continue_loop_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });

        program.emit_insn(Insn::Delete {
            cursor_id: seq_cursor_id,
            table_name: SQLITE_SEQUENCE_TABLE_NAME.to_string(),
            is_part_of_update: false,
        });

        program.resolve_label(continue_loop_label, program.offset());
        program.emit_insn(Insn::Next {
            cursor_id: seq_cursor_id,
            pc_if_next: loop_start_label,
        });

        program.preassign_label_to_next_insn(end_loop_label);
    }

    // Clean up turso_cdc_version entry for the dropped table (if version table exists)
    if let Some(version_table) = resolver
        .schema()
        .get_table(crate::translate::pragma::TURSO_CDC_VERSION_TABLE_NAME)
        .and_then(|t| t.btree())
    {
        let ver_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(version_table.clone()));
        let ver_table_name_reg = program.alloc_register();
        let dropped_name_reg = program.emit_string8_new_reg(tbl_name.name.as_str().to_string());
        program.mark_last_insn_constant();

        program.emit_insn(Insn::OpenWrite {
            cursor_id: ver_cursor_id,
            root_page: version_table.root_page.into(),
            db: crate::MAIN_DB_ID,
        });

        let end_ver_loop_label = program.allocate_label();
        let ver_loop_start_label = program.allocate_label();

        program.emit_insn(Insn::Rewind {
            cursor_id: ver_cursor_id,
            pc_if_empty: end_ver_loop_label,
        });

        program.preassign_label_to_next_insn(ver_loop_start_label);

        program.emit_column_or_rowid(ver_cursor_id, 0, ver_table_name_reg);

        let continue_ver_label = program.allocate_label();
        program.emit_insn(Insn::Ne {
            lhs: ver_table_name_reg,
            rhs: dropped_name_reg,
            target_pc: continue_ver_label,
            flags: CmpInsFlags::default(),
            collation: None,
        });

        program.emit_insn(Insn::Delete {
            cursor_id: ver_cursor_id,
            table_name: crate::translate::pragma::TURSO_CDC_VERSION_TABLE_NAME.to_string(),
            is_part_of_update: false,
        });

        program.resolve_label(continue_ver_label, program.offset());
        program.emit_insn(Insn::Next {
            cursor_id: ver_cursor_id,
            pc_if_next: ver_loop_start_label,
        });

        program.preassign_label_to_next_insn(end_ver_loop_label);
    }

    // Drop the in-memory structures for the table
    program.emit_insn(Insn::DropTable {
        db: database_id,
        _p2: 0,
        _p3: 0,
        table_name: tbl_name.name.as_str().to_string(),
    });

    let current_schema_version = resolver.with_schema(database_id, |s| s.schema_version);
    program.emit_insn(Insn::SetCookie {
        db: database_id,
        cookie: Cookie::SchemaVersion,
        value: current_schema_version as i32 + 1,
        p5: 0,
    });

    Ok(())
}

/// Validate an encode or decode expression for safety.
/// Rejects subqueries, aggregates, and window functions.
fn validate_type_expr(expr: &ast::Expr, kind: &str, resolver: &Resolver) -> Result<()> {
    walk_expr(expr, &mut |e: &ast::Expr| -> Result<WalkControl> {
        match e {
            ast::Expr::Subquery(_) | ast::Expr::Exists(_) | ast::Expr::InSelect { .. } => {
                bail_parse_error!("subqueries prohibited in {kind} expressions");
            }
            ast::Expr::FunctionCall {
                name,
                args,
                filter_over,
                ..
            } => {
                if filter_over.over_clause.is_some() {
                    bail_parse_error!("window functions prohibited in {kind} expressions");
                }
                if let Some(func) = resolver.resolve_function(name.as_str(), args.len()) {
                    if matches!(func, Func::Agg(..)) {
                        bail_parse_error!(
                            "aggregate functions prohibited in {kind} expressions: {}",
                            name.as_str()
                        );
                    }
                }
            }
            ast::Expr::FunctionCallStar { name, .. } => {
                bail_parse_error!(
                    "aggregate functions prohibited in {kind} expressions: {}",
                    name.as_str()
                );
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(())
}

pub fn translate_create_type(
    type_name: &str,
    body: &ast::CreateTypeBody,
    if_not_exists: bool,
    resolver: &Resolver,
    mut program: ProgramBuilder,
) -> Result<ProgramBuilder> {
    let normalized_name = normalize_ident(type_name);

    // Check if type already exists
    if resolver
        .schema
        .get_type_def_unchecked(&normalized_name)
        .is_some()
    {
        if if_not_exists {
            return Ok(program);
        }
        bail_parse_error!("type {normalized_name} already exists");
    }

    // Validate encode/decode expressions for safety
    if let Some(ref encode) = body.encode {
        validate_type_expr(encode, "ENCODE", resolver)?;
    }
    if let Some(ref decode) = body.decode {
        validate_type_expr(decode, "DECODE", resolver)?;
    }

    // Reconstruct the SQL string (without IF NOT EXISTS) using TypeDef::to_sql()
    let type_def = crate::schema::TypeDef::from_create_type(&normalized_name, body, false);
    let sql = type_def.to_sql();

    // Ensure sqlite_turso_types table exists (lazy creation)
    let types_table: Arc<BTreeTable>;
    let types_root_page: RegisterOrLiteral<i64>;

    if let Some(existing) = resolver.schema.get_btree_table(TURSO_TYPES_TABLE_NAME) {
        types_table = existing.clone();
        types_root_page = RegisterOrLiteral::Literal(existing.root_page);
    } else {
        // Create the sqlite_turso_types btree
        let table_root_reg = program.alloc_register();
        program.emit_insn(Insn::CreateBtree {
            db: 0,
            root: table_root_reg,
            flags: CreateBTreeFlags::new_table(),
        });
        let create_sql =
            format!("CREATE TABLE {TURSO_TYPES_TABLE_NAME}(name TEXT PRIMARY KEY, sql TEXT)");
        types_table = Arc::new(BTreeTable::from_sql(&create_sql, 0)?);
        types_root_page = RegisterOrLiteral::Register(table_root_reg);

        // Register it in sqlite_schema so it persists
        let schema_table = resolver.schema.get_btree_table(SQLITE_TABLEID).unwrap();
        let schema_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(schema_table));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: schema_cursor_id,
            root_page: 1i64.into(),
            db: 0,
        });
        emit_schema_entry(
            &mut program,
            resolver,
            schema_cursor_id,
            None,
            SchemaEntryType::Table,
            TURSO_TYPES_TABLE_NAME,
            TURSO_TYPES_TABLE_NAME,
            table_root_reg,
            Some(create_sql),
        )?;

        // Parse schema to register the new table in-memory
        program.emit_insn(Insn::ParseSchema {
            db: schema_cursor_id,
            where_clause: Some(format!(
                "tbl_name = '{TURSO_TYPES_TABLE_NAME}' AND type != 'trigger'"
            )),
        });
    }

    // Open sqlite_turso_types for writing
    let types_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(types_table));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: types_cursor_id,
        root_page: types_root_page,
        db: 0,
    });

    // Insert (name, sql) record
    let rowid_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: types_cursor_id,
        rowid_reg,
        prev_largest_reg: 0,
    });
    let name_reg = program.emit_string8_new_reg(normalized_name);
    program.emit_string8_new_reg(sql.clone());
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u16(name_reg),
        count: to_u16(2),
        dest_reg: to_u16(record_reg),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::Insert {
        cursor: types_cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: InsertFlags::new(),
        table_name: TURSO_TYPES_TABLE_NAME.to_string(),
    });

    // Add the type to the in-memory registry
    program.emit_insn(Insn::AddType { db: 0, sql });

    program.emit_insn(Insn::SetCookie {
        db: 0,
        cookie: Cookie::SchemaVersion,
        value: (resolver.schema.schema_version + 1) as i32,
        p5: 0,
    });

    Ok(program)
}

pub fn translate_drop_type(
    type_name: &str,
    if_exists: bool,
    resolver: &Resolver,
    mut program: ProgramBuilder,
) -> Result<ProgramBuilder> {
    let normalized_name = normalize_ident(type_name);

    // Check if type exists
    let type_def = resolver.schema.get_type_def_unchecked(&normalized_name);
    if type_def.is_none() {
        if if_exists {
            return Ok(program);
        }
        bail_parse_error!("no such type: {normalized_name}");
    }

    // Check if built-in type
    if type_def.unwrap().is_builtin {
        bail_parse_error!("cannot drop built-in type: {normalized_name}");
    }

    // Check if any table uses this type
    for (_, table) in resolver.schema.tables.iter() {
        for col in table.columns() {
            if normalize_ident(&col.ty_str) == normalized_name {
                bail_parse_error!(
                    "cannot drop type {normalized_name}: used by column {} in table {}",
                    col.name.as_deref().unwrap_or("?"),
                    table.get_name()
                );
            }
        }
    }

    // Open cursor to sqlite_turso_types table
    let types_table = resolver
        .schema
        .get_btree_table(TURSO_TYPES_TABLE_NAME)
        .ok_or_else(|| crate::LimboError::ParseError(format!("no such type: {normalized_name}")))?;
    let types_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(types_table.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: types_cursor_id,
        root_page: types_table.root_page.into(),
        db: 0,
    });

    // Search for matching row: name=type_name (col 0)
    let name_reg = program.alloc_register();
    program.emit_insn(Insn::String8 {
        dest: name_reg,
        value: normalized_name.clone(),
    });

    let end_loop_label = program.allocate_label();
    let loop_start_label = program.allocate_label();

    program.emit_insn(Insn::Rewind {
        cursor_id: types_cursor_id,
        pc_if_empty: end_loop_label,
    });
    program.preassign_label_to_next_insn(loop_start_label);

    // Read name (col 0)
    let col0_reg = program.alloc_register();
    program.emit_column_or_rowid(types_cursor_id, 0, col0_reg);

    let skip_delete_label = program.allocate_label();

    // Check name=type_name
    program.emit_insn(Insn::Ne {
        lhs: col0_reg,
        rhs: name_reg,
        target_pc: skip_delete_label,
        flags: CmpInsFlags::default(),
        collation: program.curr_collation(),
    });

    // Delete matching row
    program.emit_insn(Insn::Delete {
        cursor_id: types_cursor_id,
        table_name: TURSO_TYPES_TABLE_NAME.to_string(),
        is_part_of_update: false,
    });

    program.resolve_label(skip_delete_label, program.offset());

    program.emit_insn(Insn::Next {
        cursor_id: types_cursor_id,
        pc_if_next: loop_start_label,
    });

    program.preassign_label_to_next_insn(end_loop_label);

    // Remove from in-memory schema
    program.emit_insn(Insn::DropType {
        db: 0,
        type_name: normalized_name,
    });

    program.emit_insn(Insn::SetCookie {
        db: 0,
        cookie: Cookie::SchemaVersion,
        value: (resolver.schema.schema_version + 1) as i32,
        p5: 0,
    });

    Ok(program)
}
