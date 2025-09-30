use std::sync::Arc;

use crate::ast;
use crate::ext::VTabImpl;
use crate::schema::create_table;
use crate::schema::BTreeTable;
use crate::schema::Column;
use crate::schema::Table;
use crate::schema::Type;
use crate::schema::RESERVED_TABLE_PREFIXES;
use crate::storage::pager::CreateBTreeFlags;
use crate::translate::emitter::emit_cdc_full_record;
use crate::translate::emitter::emit_cdc_insns;
use crate::translate::emitter::prepare_cdc_if_necessary;
use crate::translate::emitter::OperationMode;
use crate::translate::emitter::Resolver;
use crate::translate::ProgramBuilder;
use crate::translate::ProgramBuilderOpts;
use crate::util::normalize_ident;
use crate::util::PRIMARY_KEY_AUTOMATIC_INDEX_NAME_PREFIX;
use crate::vdbe::builder::CursorType;
use crate::vdbe::insn::Cookie;
use crate::vdbe::insn::{CmpInsFlags, InsertFlags, Insn};
use crate::Connection;
use crate::{bail_parse_error, Result};

use turso_ext::VTabKind;

pub fn translate_create_table(
    tbl_name: ast::QualifiedName,
    resolver: &Resolver,
    temporary: bool,
    if_not_exists: bool,
    body: ast::CreateTableBody,
    mut program: ProgramBuilder,
    connection: &Connection,
) -> Result<ProgramBuilder> {
    let normalized_tbl_name = normalize_ident(tbl_name.name.as_str());
    if temporary {
        bail_parse_error!("TEMPORARY table not supported yet");
    }

    // Check for STRICT mode without experimental flag
    if let ast::CreateTableBody::ColumnsAndConstraints { options, .. } = &body {
        if options.contains(ast::TableOptions::STRICT) && !connection.experimental_strict_enabled()
        {
            bail_parse_error!(
                "STRICT tables are an experimental feature. Enable them with --experimental-strict flag"
            );
        }
    }
    let opts = ProgramBuilderOpts {
        num_cursors: 1,
        approx_num_insns: 30,
        approx_num_labels: 1,
    };
    program.extend(&opts);

    if RESERVED_TABLE_PREFIXES
        .iter()
        .any(|prefix| normalized_tbl_name.starts_with(prefix))
    {
        bail_parse_error!(
            "Object name reserved for internal use: {}",
            tbl_name.name.as_str()
        );
    }

    if resolver.schema.get_table(&normalized_tbl_name).is_some() {
        if if_not_exists {
            return Ok(program);
        }
        bail_parse_error!("Table {} already exists", normalized_tbl_name);
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

    let schema_master_table = resolver.schema.get_btree_table(SQLITE_TABLEID).unwrap();
    let sqlite_schema_cursor_id =
        program.alloc_cursor_id(CursorType::BTreeTable(schema_master_table.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: sqlite_schema_cursor_id,
        root_page: 1i64.into(),
        db: 0,
    });
    let cdc_table = prepare_cdc_if_necessary(&mut program, resolver.schema, SQLITE_TABLEID)?;

    let created_sequence_table =
        if has_autoincrement && resolver.schema.get_table("sqlite_sequence").is_none() {
            let seq_table_root_reg = program.alloc_register();
            program.emit_insn(Insn::CreateBtree {
                db: 0,
                root: seq_table_root_reg,
                flags: CreateBTreeFlags::new_table(),
            });

            let seq_sql = "CREATE TABLE sqlite_sequence(name,seq)";
            emit_schema_entry(
                &mut program,
                resolver,
                sqlite_schema_cursor_id,
                cdc_table.as_ref().map(|x| x.0),
                SchemaEntryType::Table,
                "sqlite_sequence",
                "sqlite_sequence",
                seq_table_root_reg,
                Some(seq_sql.to_string()),
            )?;
            true
        } else {
            false
        };

    let sql = create_table_body_to_str(&tbl_name, &body);

    let parse_schema_label = program.allocate_label();
    // TODO: ReadCookie
    // TODO: If
    // TODO: SetCookie
    // TODO: SetCookie

    let table_root_reg = program.alloc_register();
    program.emit_insn(Insn::CreateBtree {
        db: 0,
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

    let index_regs = collect_autoindexes(&body, &mut program, &normalized_tbl_name)?;
    if let Some(index_regs) = index_regs.as_ref() {
        if !resolver.schema.indexes_enabled() {
            bail_parse_error!("Constraints UNIQUE and PRIMARY KEY (unless INTEGER PRIMARY KEY) on table are not supported without indexes");
        }
        for index_reg in index_regs.iter() {
            program.emit_insn(Insn::CreateBtree {
                db: 0,
                root: *index_reg,
                flags: CreateBTreeFlags::new_index(),
            });
        }
    }

    let table = resolver.schema.get_btree_table(SQLITE_TABLEID).unwrap();
    let sqlite_schema_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: sqlite_schema_cursor_id,
        root_page: 1i64.into(),
        db: 0,
    });

    let cdc_table = prepare_cdc_if_necessary(&mut program, resolver.schema, SQLITE_TABLEID)?;

    emit_schema_entry(
        &mut program,
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
                &mut program,
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
    // TODO: SetCookie
    program.emit_insn(Insn::SetCookie {
        db: 0,
        cookie: Cookie::SchemaVersion,
        value: resolver.schema.schema_version as i32 + 1,
        p5: 0,
    });

    // TODO: remove format, it sucks for performance but is convenient
    let mut parse_schema_where_clause =
        format!("tbl_name = '{normalized_tbl_name}' AND type != 'trigger'");
    if created_sequence_table {
        parse_schema_where_clause.push_str(" OR tbl_name = 'sqlite_sequence'");
    }

    program.emit_insn(Insn::ParseSchema {
        db: sqlite_schema_cursor_id,
        where_clause: Some(parse_schema_where_clause),
    });

    // TODO: SqlExec

    Ok(program)
}

#[derive(Debug, Clone, Copy)]
pub enum SchemaEntryType {
    Table,
    Index,
    View,
}

impl SchemaEntryType {
    fn as_str(&self) -> &'static str {
        match self {
            SchemaEntryType::Table => "table",
            SchemaEntryType::Index => "index",
            SchemaEntryType::View => "view",
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
        start_reg: type_reg,
        count: 5,
        dest_reg: record_reg,
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
        let after_record_reg = if program.capture_data_changes_mode().has_after() {
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
            !(col.primary_key && col.is_rowid_alias)
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

fn create_table_body_to_str(tbl_name: &ast::QualifiedName, body: &ast::CreateTableBody) -> String {
    let mut sql = String::new();
    sql.push_str(format!("CREATE TABLE {} {}", tbl_name.name.as_ident(), body).as_str());
    match body {
        ast::CreateTableBody::ColumnsAndConstraints {
            columns: _,
            constraints: _,
            options: _,
        } => {}
        ast::CreateTableBody::AsSelect(_select) => todo!("as select not yet supported"),
    }
    sql
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
    mut program: ProgramBuilder,
) -> Result<ProgramBuilder> {
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
    if resolver.schema.get_table(&table_name).is_some() {
        if *if_not_exists {
            return Ok(program);
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
            start_reg: args_start,
            count: args_vec.len(),
            dest_reg: args_record_reg,
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
    let table = resolver.schema.get_btree_table(SQLITE_TABLEID).unwrap();
    let sqlite_schema_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: sqlite_schema_cursor_id,
        root_page: 1i64.into(),
        db: 0,
    });

    let cdc_table = prepare_cdc_if_necessary(&mut program, resolver.schema, SQLITE_TABLEID)?;
    let sql = create_vtable_body_to_str(&vtab, vtab_module.clone());
    emit_schema_entry(
        &mut program,
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
        db: 0,
        cookie: Cookie::SchemaVersion,
        value: resolver.schema.schema_version as i32 + 1,
        p5: 0,
    });
    let parse_schema_where_clause = format!("tbl_name = '{table_name}' AND type != 'trigger'");
    program.emit_insn(Insn::ParseSchema {
        db: sqlite_schema_cursor_id,
        where_clause: Some(parse_schema_where_clause),
    });

    Ok(program)
}

pub fn translate_drop_table(
    tbl_name: ast::QualifiedName,
    resolver: &Resolver,
    if_exists: bool,
    mut program: ProgramBuilder,
) -> Result<ProgramBuilder> {
    if tbl_name
        .name
        .as_str()
        .eq_ignore_ascii_case("sqlite_sequence")
    {
        bail_parse_error!("table sqlite_sequence may not be dropped");
    }

    if !resolver.schema.indexes_enabled()
        && resolver
            .schema
            .table_has_indexes(&tbl_name.name.to_string())
    {
        bail_parse_error!(
            "DROP TABLE with indexes on the table is disabled by default. Omit the `--experimental-indexes=false` flag to enable this feature."
        );
    }

    let opts = ProgramBuilderOpts {
        num_cursors: 4,
        approx_num_insns: 40,
        approx_num_labels: 4,
    };
    program.extend(&opts);
    let table = resolver.schema.get_table(tbl_name.name.as_str());
    if table.is_none() {
        if if_exists {
            return Ok(program);
        }
        bail_parse_error!("No such table: {}", tbl_name.name.as_str());
    }

    if RESERVED_TABLE_PREFIXES
        .iter()
        .any(|prefix| tbl_name.name.as_str().starts_with(prefix))
    {
        bail_parse_error!("table {} may not be dropped", tbl_name.name.as_str());
    }

    let table = table.unwrap(); // safe since we just checked for None

    // Check if this is a materialized view - if so, refuse to drop it with DROP TABLE
    if resolver.schema.is_materialized_view(tbl_name.name.as_str()) {
        bail_parse_error!(
            "Cannot DROP TABLE on materialized view {}. Use DROP VIEW instead.",
            tbl_name.name.as_str()
        );
    }
    let cdc_table = prepare_cdc_if_necessary(&mut program, resolver.schema, SQLITE_TABLEID)?;

    let null_reg = program.alloc_register(); //  r1
    program.emit_null(null_reg, None);
    let table_name_and_root_page_register = program.alloc_register(); //  r2, this register is special because it's first used to track table name and then moved root page
    let table_reg = program.emit_string8_new_reg(tbl_name.name.as_str().to_string()); //  r3
    program.mark_last_insn_constant();
    let table_type = program.emit_string8_new_reg("trigger".to_string()); //  r4
    program.mark_last_insn_constant();
    let row_id_reg = program.alloc_register(); //  r5

    let schema_table = resolver.schema.get_btree_table(SQLITE_TABLEID).unwrap();
    let sqlite_schema_cursor_id_0 = program.alloc_cursor_id(
        //  cursor 0
        CursorType::BTreeTable(schema_table.clone()),
    );
    program.emit_insn(Insn::OpenWrite {
        cursor_id: sqlite_schema_cursor_id_0,
        root_page: 1i64.into(),
        db: 0,
    });

    //  1. Remove all entries from the schema table related to the table we are dropping, except for triggers
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
    program.emit_column_or_rowid(
        sqlite_schema_cursor_id_0,
        0,
        table_name_and_root_page_register,
    );
    program.emit_insn(Insn::Eq {
        lhs: table_name_and_root_page_register,
        rhs: table_type,
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
        let before_record_reg = if program.capture_data_changes_mode().has_before() {
            Some(emit_cdc_full_record(
                &mut program,
                &schema_table.columns,
                sqlite_schema_cursor_id_0,
                row_id_reg,
            ))
        } else {
            None
        };
        emit_cdc_insns(
            &mut program,
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
    });

    program.resolve_label(next_label, program.offset());
    program.emit_insn(Insn::Next {
        cursor_id: sqlite_schema_cursor_id_0,
        pc_if_next: metadata_loop,
    });
    program.preassign_label_to_next_insn(end_metadata_label);
    // end of loop on schema table

    //  2. Destroy the indices within a loop
    let indices = resolver.schema.get_indices(tbl_name.name.as_str());
    for index in indices {
        program.emit_insn(Insn::Destroy {
            root: index.root_page,
            former_root_reg: 0, //  no autovacuum (https://www.sqlite.org/opcode.html#Destroy)
            is_temp: 0,
        });

        //  3. TODO: Open an ephemeral table, and read over triggers from schema table into ephemeral table
        //  Requires support via https://github.com/tursodatabase/turso/pull/768

        //  4. TODO: Open a write cursor to the schema table and re-insert all triggers into the sqlite schema table from the ephemeral table and delete old trigger
        //  Requires support via https://github.com/tursodatabase/turso/pull/768
    }

    //  3. Destroy the table structure
    match table.as_ref() {
        Table::BTree(table) => {
            program.emit_insn(Insn::Destroy {
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
                db: 0, // TODO change this for multiple databases
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
            columns: vec![Column {
                name: Some("rowid".to_string()),
                ty: Type::Integer,
                ty_str: "INTEGER".to_string(),
                primary_key: false,
                is_rowid_alias: false,
                notnull: false,
                default: None,
                unique: false,
                collation: None,
                hidden: false,
            }],
            is_strict: false,
            unique_sets: vec![],
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
            db: 0,
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
            db: 0,
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
        let root_page = table.get_root_page();
        program.emit_insn(Insn::Integer {
            value: root_page,
            dest: moved_to_root_page_register,
        });
        program.emit_column_or_rowid(sqlite_schema_cursor_id_1, 4, schema_column_4_register);
        program.emit_insn(Insn::MakeRecord {
            start_reg: schema_column_0_register,
            count: 5,
            dest_reg: new_record_register,
            index_name: None,
            affinity_str: None,
        });
        program.emit_insn(Insn::Delete {
            cursor_id: sqlite_schema_cursor_id_1,
            table_name: SQLITE_TABLEID.to_string(),
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
        .schema
        .get_table("sqlite_sequence")
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
            db: 0,
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
            table_name: "sqlite_sequence".to_string(),
        });

        program.resolve_label(continue_loop_label, program.offset());
        program.emit_insn(Insn::Next {
            cursor_id: seq_cursor_id,
            pc_if_next: loop_start_label,
        });

        program.preassign_label_to_next_insn(end_loop_label);
    }

    // Drop the in-memory structures for the table
    program.emit_insn(Insn::DropTable {
        db: 0,
        _p2: 0,
        _p3: 0,
        table_name: tbl_name.name.as_str().to_string(),
    });

    program.emit_insn(Insn::SetCookie {
        db: 0,
        cookie: Cookie::SchemaVersion,
        value: resolver.schema.schema_version as i32 + 1,
        p5: 0,
    });

    Ok(program)
}
