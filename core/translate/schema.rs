use std::collections::HashSet;
use std::fmt::Display;
use std::ops::Range;
use std::rc::Rc;

use crate::ast;
use crate::ext::VTabImpl;
use crate::schema::Schema;
use crate::schema::Table;
use crate::storage::pager::CreateBTreeFlags;
use crate::translate::ProgramBuilder;
use crate::translate::ProgramBuilderOpts;
use crate::translate::QueryMode;
use crate::util::PRIMARY_KEY_AUTOMATIC_INDEX_NAME_PREFIX;
use crate::vdbe::builder::CursorType;
use crate::vdbe::insn::{CmpInsFlags, Insn};
use crate::LimboError;
use crate::SymbolTable;
use crate::{bail_parse_error, Result};

use limbo_ext::VTabKind;
use limbo_sqlite3_parser::ast::{fmt::ToTokens, CreateVirtualTable};

#[derive(Debug, Clone, Copy)]
pub enum ParseSchema {
    None,
    Reload,
}

pub fn translate_create_table(
    query_mode: QueryMode,
    tbl_name: ast::QualifiedName,
    temporary: bool,
    body: ast::CreateTableBody,
    if_not_exists: bool,
    schema: &Schema,
) -> Result<ProgramBuilder> {
    if temporary {
        bail_parse_error!("TEMPORARY table not supported yet");
    }
    let mut program = ProgramBuilder::new(ProgramBuilderOpts {
        query_mode,
        num_cursors: 1,
        approx_num_insns: 30,
        approx_num_labels: 1,
    });
    if schema.get_table(tbl_name.name.0.as_str()).is_some() {
        if if_not_exists {
            let init_label = program.emit_init();
            let start_offset = program.offset();
            program.emit_halt();
            program.preassign_label_to_next_insn(init_label);
            program.emit_transaction(true);
            program.emit_constant_insns();
            program.emit_goto(start_offset);

            return Ok(program);
        }
        bail_parse_error!("Table {} already exists", tbl_name);
    }

    let sql = create_table_body_to_str(&tbl_name, &body);

    let parse_schema_label = program.allocate_label();
    let init_label = program.emit_init();
    let start_offset = program.offset();
    // TODO: ReadCookie
    // TODO: If
    // TODO: SetCookie
    // TODO: SetCookie

    // Create the table B-tree
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

    let index_regs = check_automatic_pk_index_required(&body, &mut program, &tbl_name.name.0)?;
    if let Some(index_regs) = index_regs.as_ref() {
        for index_reg in index_regs.clone() {
            program.emit_insn(Insn::CreateBtree {
                db: 0,
                root: index_reg,
                flags: CreateBTreeFlags::new_index(),
            });
        }
    }

    let table = schema.get_btree_table(SQLITE_TABLEID).unwrap();
    let sqlite_schema_cursor_id = program.alloc_cursor_id(
        Some(SQLITE_TABLEID.to_owned()),
        CursorType::BTreeTable(table.clone()),
    );
    program.emit_insn(Insn::OpenWrite {
        cursor_id: sqlite_schema_cursor_id,
        root_page: 1usize.into(),
        name: tbl_name.name.0.clone(),
    });

    // Add the table entry to sqlite_schema
    emit_schema_entry(
        &mut program,
        sqlite_schema_cursor_id,
        SchemaEntryType::Table,
        &tbl_name.name.0,
        &tbl_name.name.0,
        table_root_reg,
        Some(sql),
    );

    // If we need an automatic index, add its entry to sqlite_schema
    if let Some(index_regs) = index_regs {
        for (idx, index_reg) in index_regs.into_iter().enumerate() {
            let index_name = format!(
                "{}{}_{}",
                PRIMARY_KEY_AUTOMATIC_INDEX_NAME_PREFIX,
                tbl_name.name.0,
                idx + 1
            );
            emit_schema_entry(
                &mut program,
                sqlite_schema_cursor_id,
                SchemaEntryType::Index,
                &index_name,
                &tbl_name.name.0,
                index_reg,
                None,
            );
        }
    }

    program.resolve_label(parse_schema_label, program.offset());
    // TODO: SetCookie
    //
    // TODO: remove format, it sucks for performance but is convenient
    let parse_schema_where_clause = format!("tbl_name = '{}' AND type != 'trigger'", tbl_name);
    program.emit_insn(Insn::ParseSchema {
        db: sqlite_schema_cursor_id,
        where_clause: Some(parse_schema_where_clause),
    });

    // TODO: SqlExec
    program.emit_halt();
    program.preassign_label_to_next_insn(init_label);
    program.emit_transaction(true);
    program.emit_constant_insns();
    program.emit_goto(start_offset);

    Ok(program)
}

#[derive(Debug, Clone, Copy)]
pub enum SchemaEntryType {
    Table,
    Index,
}

impl SchemaEntryType {
    fn as_str(&self) -> &'static str {
        match self {
            SchemaEntryType::Table => "table",
            SchemaEntryType::Index => "index",
        }
    }
}
pub const SQLITE_TABLEID: &str = "sqlite_schema";

pub fn emit_schema_entry(
    program: &mut ProgramBuilder,
    sqlite_schema_cursor_id: usize,
    entry_type: SchemaEntryType,
    name: &str,
    tbl_name: &str,
    root_page_reg: usize,
    sql: Option<String>,
) {
    let rowid_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: sqlite_schema_cursor_id,
        rowid_reg,
        prev_largest_reg: 0,
    });

    let type_reg = program.emit_string8_new_reg(entry_type.as_str().to_string());
    program.emit_string8_new_reg(name.to_string());
    program.emit_string8_new_reg(tbl_name.to_string());

    let rootpage_reg = program.alloc_register();
    if root_page_reg == 0 {
        program.emit_insn(Insn::Integer {
            dest: rootpage_reg,
            value: 0, // virtual tables in sqlite always have rootpage=0
        });
    } else {
        program.emit_insn(Insn::Copy {
            src_reg: root_page_reg,
            dst_reg: rootpage_reg,
            amount: 1,
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
    });

    program.emit_insn(Insn::Insert {
        cursor: sqlite_schema_cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: 0,
        table_name: tbl_name.to_string(),
    });
}

#[derive(Debug)]
struct PrimaryKeyColumnInfo<'a> {
    name: &'a String,
    is_descending: bool,
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
fn check_automatic_pk_index_required(
    body: &ast::CreateTableBody,
    program: &mut ProgramBuilder,
    tbl_name: &str,
) -> Result<Option<Range<usize>>> {
    match body {
        ast::CreateTableBody::ColumnsAndConstraints {
            columns,
            constraints,
            options,
        } => {
            let mut primary_key_definition = None;
            // Used to dedup named unique constraints
            let mut unique_sets = vec![];

            // Check table constraints for PRIMARY KEY
            if let Some(constraints) = constraints {
                for constraint in constraints {
                    if let ast::TableConstraint::PrimaryKey {
                        columns: pk_cols, ..
                    } = &constraint.constraint
                    {
                        if primary_key_definition.is_some() {
                            bail_parse_error!("table {} has more than one primary key", tbl_name);
                        }
                        let primary_key_column_results = pk_cols
                            .iter()
                            .map(|col| match &col.expr {
                                ast::Expr::Id(name) => {
                                    if !columns.iter().any(|(k, _)| k.0 == name.0) {
                                        bail_parse_error!("No such column: {}", name.0);
                                    }
                                    Ok(PrimaryKeyColumnInfo {
                                        name: &name.0,
                                        is_descending: matches!(
                                            col.order,
                                            Some(ast::SortOrder::Desc)
                                        ),
                                    })
                                }
                                _ => Err(LimboError::ParseError(
                                    "expressions prohibited in PRIMARY KEY and UNIQUE constraints"
                                        .to_string(),
                                )),
                            })
                            .collect::<Result<Vec<_>>>()?;

                        for pk_info in primary_key_column_results {
                            let column_name = pk_info.name;
                            let (_, column_def) = columns
                                .iter()
                                .find(|(k, _)| k.0 == *column_name)
                                .expect("primary key column should be in Create Body columns");

                            match &mut primary_key_definition {
                                Some(PrimaryKeyDefinitionType::Simple { column, .. }) => {
                                    let mut columns = HashSet::new();
                                    columns.insert(std::mem::take(column));
                                    // Have to also insert the current column_name we are iterating over in primary_key_column_results
                                    columns.insert(column_name.clone());
                                    primary_key_definition =
                                        Some(PrimaryKeyDefinitionType::Composite { columns });
                                }
                                Some(PrimaryKeyDefinitionType::Composite { columns }) => {
                                    columns.insert(column_name.clone());
                                }
                                None => {
                                    let typename =
                                        column_def.col_type.as_ref().map(|t| t.name.as_str());
                                    let is_descending = pk_info.is_descending;
                                    primary_key_definition =
                                        Some(PrimaryKeyDefinitionType::Simple {
                                            typename,
                                            is_descending,
                                            column: column_name.clone(),
                                        });
                                }
                            }
                        }
                    } else if let ast::TableConstraint::Unique {
                        columns: unique_columns,
                        conflict_clause,
                    } = &constraint.constraint
                    {
                        if conflict_clause.is_some() {
                            unimplemented!("ON CONFLICT not implemented");
                        }

                        let col_names = unique_columns
                            .iter()
                            .map(|column| match &column.expr {
                                limbo_sqlite3_parser::ast::Expr::Id(id) => {
                                    if !columns.iter().any(|(k, _)| k.0 == id.0) {
                                        bail_parse_error!("No such column: {}", id.0);
                                    }
                                    Ok(crate::util::normalize_ident(&id.0))
                                }
                                _ => {
                                    todo!("Unsupported unique expression");
                                }
                            })
                            .collect::<Result<HashSet<String>>>()?;
                        unique_sets.push(col_names);
                    }
                }
            }

            // Check column constraints for PRIMARY KEY and UNIQUE
            for (_, col_def) in columns.iter() {
                for constraint in &col_def.constraints {
                    if matches!(
                        constraint.constraint,
                        ast::ColumnConstraint::PrimaryKey { .. }
                    ) {
                        if primary_key_definition.is_some() {
                            bail_parse_error!("table {} has more than one primary key", tbl_name);
                        }
                        let typename = col_def.col_type.as_ref().map(|t| t.name.as_str());
                        primary_key_definition = Some(PrimaryKeyDefinitionType::Simple {
                            typename,
                            is_descending: false,
                            column: col_def.col_name.0.clone(),
                        });
                    } else if matches!(constraint.constraint, ast::ColumnConstraint::Unique(..)) {
                        let mut single_set = HashSet::new();
                        single_set.insert(col_def.col_name.0.clone());
                        unique_sets.push(single_set);
                    }
                }
            }

            // Check if table has rowid
            if options.contains(ast::TableOptions::WITHOUT_ROWID) {
                bail_parse_error!("WITHOUT ROWID tables are not supported yet");
            }

            unique_sets.dedup();

            // Check if we need an automatic index
            let mut pk_is_unique = false;
            let auto_index_pk = if let Some(primary_key_definition) = &primary_key_definition {
                match primary_key_definition {
                    PrimaryKeyDefinitionType::Simple {
                        typename,
                        is_descending,
                        column,
                    } => {
                        pk_is_unique = unique_sets
                            .iter()
                            .any(|set| set.len() == 1 && set.contains(column));
                        let is_integer =
                            typename.is_some() && typename.unwrap().eq_ignore_ascii_case("INTEGER"); // Should match on any case of INTEGER
                        !is_integer || *is_descending
                    }
                    PrimaryKeyDefinitionType::Composite { columns } => {
                        pk_is_unique = unique_sets.iter().any(|set| set == columns);
                        true
                    }
                }
            } else {
                false
            };
            let mut total_indices = unique_sets.len();
            // if pk needs and index, but we already found out we primary key is unique, we only need a single index since constraint pk == unique
            if auto_index_pk && !pk_is_unique {
                total_indices += 1;
            }

            if total_indices > 0 {
                let index_start_reg = program.alloc_registers(total_indices);
                Ok(Some(index_start_reg..index_start_reg + total_indices))
            } else {
                Ok(None)
            }
        }
        ast::CreateTableBody::AsSelect(_) => {
            bail_parse_error!("CREATE TABLE AS SELECT not supported yet")
        }
    }
}

#[derive(Debug)]
enum PrimaryKeyDefinitionType<'a> {
    Simple {
        column: String,
        typename: Option<&'a str>,
        is_descending: bool,
    },
    Composite {
        columns: HashSet<String>,
    },
}

struct TableFormatter<'a> {
    body: &'a ast::CreateTableBody,
}

impl Display for TableFormatter<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.body.to_fmt(f)
    }
}

fn create_table_body_to_str(tbl_name: &ast::QualifiedName, body: &ast::CreateTableBody) -> String {
    let mut sql = String::new();
    let formatter = TableFormatter { body };
    sql.push_str(format!("CREATE TABLE {} {}", tbl_name.name.0, formatter).as_str());
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

fn create_vtable_body_to_str(vtab: &CreateVirtualTable, module: Rc<VTabImpl>) -> String {
    let args = if let Some(args) = &vtab.args {
        args.iter()
            .map(|arg| arg.to_string())
            .collect::<Vec<String>>()
            .join(", ")
    } else {
        "".to_string()
    };
    let if_not_exists = if vtab.if_not_exists {
        "IF NOT EXISTS "
    } else {
        ""
    };
    let ext_args = vtab
        .args
        .as_ref()
        .unwrap_or(&vec![])
        .iter()
        .map(|a| limbo_ext::Value::from_text(a.to_string()))
        .collect::<Vec<_>>();
    let schema = module
        .implementation
        .init_schema(ext_args)
        .unwrap_or_default();
    let vtab_args = if let Some(first_paren) = schema.find('(') {
        let closing_paren = schema.rfind(')').unwrap_or_default();
        &schema[first_paren..=closing_paren]
    } else {
        "()"
    };
    format!(
        "CREATE VIRTUAL TABLE {} {} USING {}{}\n /*{}{}*/",
        vtab.tbl_name.name.0,
        if_not_exists,
        vtab.module_name.0,
        if args.is_empty() {
            String::new()
        } else {
            format!("({})", args)
        },
        vtab.tbl_name.name.0,
        vtab_args
    )
}

pub fn translate_create_virtual_table(
    vtab: CreateVirtualTable,
    schema: &Schema,
    query_mode: QueryMode,
    syms: &SymbolTable,
) -> Result<ProgramBuilder> {
    let ast::CreateVirtualTable {
        if_not_exists,
        tbl_name,
        module_name,
        args,
    } = &vtab;

    let table_name = tbl_name.name.0.clone();
    let module_name_str = module_name.0.clone();
    let args_vec = args.clone().unwrap_or_default();
    let Some(vtab_module) = syms.vtab_modules.get(&module_name_str) else {
        bail_parse_error!("no such module: {}", module_name_str);
    };
    if !vtab_module.module_kind.eq(&VTabKind::VirtualTable) {
        bail_parse_error!("module {} is not a virtual table", module_name_str);
    };
    if schema.get_table(&table_name).is_some() {
        if *if_not_exists {
            let mut program = ProgramBuilder::new(ProgramBuilderOpts {
                query_mode,
                num_cursors: 1,
                approx_num_insns: 5,
                approx_num_labels: 1,
            });
            let init_label = program.emit_init();
            let start_offset = program.offset();
            program.emit_halt();
            program.preassign_label_to_next_insn(init_label);
            program.emit_transaction(true);
            program.emit_constant_insns();
            program.emit_goto(start_offset);
            return Ok(program);
        }
        bail_parse_error!("Table {} already exists", tbl_name);
    }

    let mut program = ProgramBuilder::new(ProgramBuilderOpts {
        query_mode,
        num_cursors: 2,
        approx_num_insns: 40,
        approx_num_labels: 2,
    });
    let init_label = program.emit_init();
    let start_offset = program.offset();
    let module_name_reg = program.emit_string8_new_reg(module_name_str.clone());
    let table_name_reg = program.emit_string8_new_reg(table_name.clone());
    let args_reg = if !args_vec.is_empty() {
        let args_start = program.alloc_register();

        // Emit string8 instructions for each arg
        for (i, arg) in args_vec.iter().enumerate() {
            program.emit_string8(arg.to_string().clone(), args_start + i);
        }
        let args_record_reg = program.alloc_register();

        // VCreate expects an array of args as a record
        program.emit_insn(Insn::MakeRecord {
            start_reg: args_start,
            count: args_vec.len(),
            dest_reg: args_record_reg,
            index_name: None,
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
    let table = schema.get_btree_table(SQLITE_TABLEID).unwrap();
    let sqlite_schema_cursor_id = program.alloc_cursor_id(
        Some(SQLITE_TABLEID.to_owned()),
        CursorType::BTreeTable(table.clone()),
    );
    program.emit_insn(Insn::OpenWrite {
        cursor_id: sqlite_schema_cursor_id,
        root_page: 1usize.into(),
        name: table_name.clone(),
    });

    let sql = create_vtable_body_to_str(&vtab, vtab_module.clone());
    emit_schema_entry(
        &mut program,
        sqlite_schema_cursor_id,
        SchemaEntryType::Table,
        &tbl_name.name.0,
        &tbl_name.name.0,
        0, // virtual tables dont have a root page
        Some(sql),
    );

    let parse_schema_where_clause = format!("tbl_name = '{}' AND type != 'trigger'", table_name);
    program.emit_insn(Insn::ParseSchema {
        db: sqlite_schema_cursor_id,
        where_clause: Some(parse_schema_where_clause),
    });

    program.emit_halt();
    program.preassign_label_to_next_insn(init_label);
    program.emit_transaction(true);
    program.emit_constant_insns();
    program.emit_goto(start_offset);

    Ok(program)
}

pub fn translate_drop_table(
    query_mode: QueryMode,
    tbl_name: ast::QualifiedName,
    if_exists: bool,
    schema: &Schema,
) -> Result<ProgramBuilder> {
    let mut program = ProgramBuilder::new(ProgramBuilderOpts {
        query_mode,
        num_cursors: 1,
        approx_num_insns: 30,
        approx_num_labels: 1,
    });
    let table = schema.get_table(tbl_name.name.0.as_str());
    if table.is_none() {
        if if_exists {
            let init_label = program.emit_init();
            let start_offset = program.offset();
            program.emit_halt();
            program.preassign_label_to_next_insn(init_label);
            program.emit_transaction(true);
            program.emit_constant_insns();
            program.emit_goto(start_offset);

            return Ok(program);
        }
        bail_parse_error!("No such table: {}", tbl_name.name.0.as_str());
    }

    let table = table.unwrap(); // safe since we just checked for None

    let init_label = program.emit_init();
    let start_offset = program.offset();

    let null_reg = program.alloc_register(); //  r1
    program.emit_null(null_reg, None);
    let tbl_name_reg = program.alloc_register(); //  r2
    let table_reg = program.emit_string8_new_reg(tbl_name.name.0.clone()); //  r3
    program.mark_last_insn_constant();
    let table_type = program.emit_string8_new_reg("trigger".to_string()); //  r4
    program.mark_last_insn_constant();
    let row_id_reg = program.alloc_register(); //  r5

    let table_name = "sqlite_schema";
    let schema_table = schema.get_btree_table(table_name).unwrap();
    let sqlite_schema_cursor_id = program.alloc_cursor_id(
        Some(table_name.to_string()),
        CursorType::BTreeTable(schema_table.clone()),
    );
    program.emit_insn(Insn::OpenWrite {
        cursor_id: sqlite_schema_cursor_id,
        root_page: 1usize.into(),
        name: tbl_name.name.0.clone(),
    });

    //  1. Remove all entries from the schema table related to the table we are dropping, except for triggers
    //  loop to beginning of schema table
    let end_metadata_label = program.allocate_label();
    let metadata_loop = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: sqlite_schema_cursor_id,
        pc_if_empty: end_metadata_label,
    });
    program.preassign_label_to_next_insn(metadata_loop);

    //  start loop on schema table
    program.emit_insn(Insn::Column {
        cursor_id: sqlite_schema_cursor_id,
        column: 2,
        dest: tbl_name_reg,
    });
    let next_label = program.allocate_label();
    program.emit_insn(Insn::Ne {
        lhs: tbl_name_reg,
        rhs: table_reg,
        target_pc: next_label,
        flags: CmpInsFlags::default(),
    });
    program.emit_insn(Insn::Column {
        cursor_id: sqlite_schema_cursor_id,
        column: 0,
        dest: tbl_name_reg,
    });
    program.emit_insn(Insn::Eq {
        lhs: tbl_name_reg,
        rhs: table_type,
        target_pc: next_label,
        flags: CmpInsFlags::default(),
    });
    program.emit_insn(Insn::RowId {
        cursor_id: sqlite_schema_cursor_id,
        dest: row_id_reg,
    });
    program.emit_insn(Insn::Delete {
        cursor_id: sqlite_schema_cursor_id,
    });

    program.resolve_label(next_label, program.offset());
    program.emit_insn(Insn::Next {
        cursor_id: sqlite_schema_cursor_id,
        pc_if_next: metadata_loop,
    });
    program.preassign_label_to_next_insn(end_metadata_label);
    //  end of loop on schema table

    //  2. Destroy the indices within a loop
    let indices = schema.get_indices(&tbl_name.name.0);
    for index in indices {
        program.emit_insn(Insn::Destroy {
            root: index.root_page,
            former_root_reg: 0, //  no autovacuum (https://www.sqlite.org/opcode.html#Destroy)
            is_temp: 0,
        });
        let null_reg_1 = program.alloc_register();
        let null_reg_2 = program.alloc_register();
        program.emit_null(null_reg_1, Some(null_reg_2));

        //  3. TODO: Open an ephemeral table, and read over triggers from schema table into ephemeral table
        //  Requires support via https://github.com/tursodatabase/limbo/pull/768

        //  4. TODO: Open a write cursor to the schema table and re-insert all triggers into the sqlite schema table from the ephemeral table and delete old trigger
        //  Requires support via https://github.com/tursodatabase/limbo/pull/768
    }

    //  3. Destroy the table structure
    match table.as_ref() {
        Table::BTree(table) => {
            program.emit_insn(Insn::Destroy {
                root: table.root_page,
                former_root_reg: 0, //  no autovacuum (https://www.sqlite.org/opcode.html#Destroy)
                is_temp: 0,
            });
        }
        Table::Virtual(vtab) => {
            // From what I see, TableValuedFunction is not stored in the schema as a table.
            // But this line here below is a safeguard in case this behavior changes in the future
            // And mirrors what SQLite does.
            if matches!(vtab.kind, limbo_ext::VTabKind::TableValuedFunction) {
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
        Table::Pseudo(..) => unimplemented!(),
    };

    let r6 = program.alloc_register();
    let r7 = program.alloc_register();
    program.emit_null(r6, Some(r7));

    //  3. TODO: Open an ephemeral table, and read over triggers from schema table into ephemeral table
    //  Requires support via https://github.com/tursodatabase/limbo/pull/768

    //  4. TODO: Open a write cursor to the schema table and re-insert all triggers into the sqlite schema table from the ephemeral table and delete old trigger
    //  Requires support via https://github.com/tursodatabase/limbo/pull/768

    //  Drop the in-memory structures for the table
    program.emit_insn(Insn::DropTable {
        db: 0,
        _p2: 0,
        _p3: 0,
        table_name: tbl_name.name.0,
    });

    //  end of the program
    program.emit_halt();
    program.preassign_label_to_next_insn(init_label);
    program.emit_transaction(true);
    program.emit_constant_insns();

    program.emit_goto(start_offset);

    Ok(program)
}
