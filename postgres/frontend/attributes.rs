//! Columns, PostgreSQL's `pg_attribute` rows, in the `pg_attribute` table of
//! the main database: one row per column of every user table.
//!
//! `sqlite_schema` stays the source of a table's definition. The rows here
//! say the same thing in PostgreSQL's form, for clients that read
//! `pg_attribute`. Every statement that creates a table or changes its
//! columns writes the table's rows in the same transaction as its
//! `sqlite_schema` row: the engine tells the frontend the new columns with
//! [`CatalogChange::SetColumns`](turso_core::CatalogChange::SetColumns).
//!
//! Nothing in memory holds these rows. A query reads the table.

use std::sync::Arc;

use turso_core::namespace::NamespaceId;
use turso_core::schema::{BTreeTable, Schema};
use turso_core::{
    to_u32, CmpInsFlags, CollationSeq, CursorType, InsertFlags, Insn, ProgramBuilder,
    RegisterOrLiteral, MAIN_DB_ID,
};
use turso_parser::ast;

use crate::catalog::{sqlite_type_to_pg_oid, type_storage, user_type_oid};

pub(crate) const ATTRIBUTE_TABLE_NAME: &str = "pg_attribute";

/// PostgreSQL's oid of `pg_attribute`, which every row shows as `tableoid`.
/// pg_dump reads it.
const ATTRIBUTE_TABLE_OID: i64 = 1249;

/// The columns of [`ATTRIBUTE_TABLE_NAME`] and their types: PostgreSQL's
/// columns, and `tableoid`, which is a system column in PostgreSQL.
/// [`attribute_row`] gives the values of a row in this order.
const ATTRIBUTE_COLUMNS: [(&str, &str); 27] = [
    ("attrelid", "INTEGER"),
    ("attname", "TEXT"),
    ("atttypid", "INTEGER"),
    ("attstattarget", "INTEGER"),
    ("attlen", "INTEGER"),
    ("attnum", "INTEGER"),
    ("attndims", "INTEGER"),
    ("attcacheoff", "INTEGER"),
    ("atttypmod", "INTEGER"),
    ("attbyval", "BOOLEAN"),
    ("attstorage", "TEXT"),
    ("attalign", "TEXT"),
    ("attcompression", "TEXT"),
    ("attnotnull", "BOOLEAN"),
    ("atthasdef", "BOOLEAN"),
    ("atthasmissing", "BOOLEAN"),
    ("attidentity", "TEXT"),
    ("attgenerated", "TEXT"),
    ("attisdropped", "BOOLEAN"),
    ("attislocal", "BOOLEAN"),
    ("attinhcount", "INTEGER"),
    ("attcollation", "INTEGER"),
    ("attacl", "TEXT"),
    ("attoptions", "TEXT"),
    ("attfdwoptions", "TEXT"),
    ("attmissingval", "TEXT"),
    (TABLEOID_COLUMN_NAME, "INTEGER"),
];

const TABLEOID_COLUMN_NAME: &str = "tableoid";

/// The position of `attrelid` in a row.
const ATTRELID_COLUMN: usize = 0;

/// Where the oid of the table whose columns are written comes from.
pub(crate) enum TableOid {
    /// The table has a class row with this oid.
    Stored(i64),
    /// The running statement creates the table and has its oid in this
    /// register.
    Register(usize),
}

/// A value of one column of a `pg_attribute` row.
enum AttributeValue {
    Integer(i64),
    Text(String),
    Null,
    /// The oid of the table the row belongs to.
    TableOid,
}

/// Whether `name` in the root namespace is a catalog table that this
/// frontend stores. Every role reads such a table and no user statement
/// writes it.
pub(crate) fn is_stored_catalog_table(namespace: NamespaceId, name: &str) -> bool {
    namespace.is_root() && name.eq_ignore_ascii_case(ATTRIBUTE_TABLE_NAME)
}

/// Hides the `tableoid` column of `table` when it is a stored catalog
/// table. `tableoid` is a system column in PostgreSQL: a query that names it
/// gets it, and `SELECT *` does not.
pub(crate) fn hide_tableoid_column(table: &mut BTreeTable) {
    if !table.name.eq_ignore_ascii_case(ATTRIBUTE_TABLE_NAME) {
        return;
    }
    for column in table.columns_mut().iter_mut() {
        if column.name.as_deref() == Some(TABLEOID_COLUMN_NAME) {
            column.set_hidden(true);
        }
    }
}

/// The statement that creates the table. None when `schema` has the table.
pub(crate) fn initialize_statements(schema: &Schema) -> Vec<String> {
    if schema
        .get_btree_table(NamespaceId::ROOT, ATTRIBUTE_TABLE_NAME)
        .is_some()
    {
        return Vec::new();
    }
    // The text is valid in SQLite and in PostgreSQL, because VACUUM stores
    // it again as PostgreSQL text.
    let column_definitions: Vec<String> = ATTRIBUTE_COLUMNS
        .iter()
        .map(|(name, ty)| format!("{name} {ty}"))
        .collect();
    vec![format!(
        "CREATE TABLE {ATTRIBUTE_TABLE_NAME} ({})",
        column_definitions.join(", ")
    )]
}

/// Writes the rows of the columns of `table`, whose oid is `oid`. The rows
/// that a table with a class row had before are deleted first.
pub(crate) fn emit_set_table_columns(
    program: &mut ProgramBuilder,
    schema: &Schema,
    oid: TableOid,
    table: &BTreeTable,
) {
    let Some(cursor_id) = open_attribute_table_for_writing(program, schema) else {
        return;
    };
    let oid_reg = match oid {
        TableOid::Stored(oid) => {
            let oid_reg = emit_integer(program, oid);
            emit_delete_rows_of_table(program, cursor_id, oid_reg);
            oid_reg
        }
        TableOid::Register(oid_reg) => oid_reg,
    };
    for position in 0..table.columns().len() {
        let row = attribute_row(schema, table, position);
        emit_row_insert(program, cursor_id, oid_reg, &row);
    }
}

/// Deletes the rows of the columns of the table with oid `oid`, which the
/// running statement drops.
pub(crate) fn emit_delete_table_columns(program: &mut ProgramBuilder, schema: &Schema, oid: i64) {
    let Some(cursor_id) = open_attribute_table_for_writing(program, schema) else {
        return;
    };
    let oid_reg = emit_integer(program, oid);
    emit_delete_rows_of_table(program, cursor_id, oid_reg);
}

/// Opens the table for writing and returns its cursor, or None when the
/// database has no catalog, which only happens outside the frontend's own
/// open path, such as a core-level open for tests.
fn open_attribute_table_for_writing(
    program: &mut ProgramBuilder,
    schema: &Schema,
) -> Option<usize> {
    let table = schema.get_btree_table(NamespaceId::ROOT, ATTRIBUTE_TABLE_NAME)?;
    let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(Arc::clone(&table)));
    program.emit_insn(Insn::OpenWrite {
        cursor_id,
        root_page: RegisterOrLiteral::Literal(table.root_page),
        db: MAIN_DB_ID,
    });
    Some(cursor_id)
}

fn emit_integer(program: &mut ProgramBuilder, value: i64) -> usize {
    let dest = program.alloc_register();
    program.emit_insn(Insn::Integer { value, dest });
    dest
}

/// Deletes every row whose `attrelid` equals the oid in `oid_reg`.
fn emit_delete_rows_of_table(program: &mut ProgramBuilder, cursor_id: usize, oid_reg: usize) {
    let loop_end = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id,
        pc_if_empty: loop_end,
    });
    let loop_start = program.allocate_label();
    program.preassign_label_to_next_insn(loop_start);

    let next = program.allocate_label();
    let attrelid_reg = program.alloc_register();
    program.emit_column_or_rowid(cursor_id, ATTRELID_COLUMN, attrelid_reg);
    program.emit_insn(Insn::Ne {
        lhs: attrelid_reg,
        rhs: oid_reg,
        target_pc: next,
        flags: CmpInsFlags::default().jump_if_null(),
        collation: CollationSeq::for_internal_values(),
    });
    // Marked as part of an update so that changes() does not count it.
    program.emit_insn(Insn::Delete {
        cursor_id,
        table_name: ATTRIBUTE_TABLE_NAME.to_string(),
        is_part_of_update: true,
    });

    program.preassign_label_to_next_insn(next);
    program.emit_insn(Insn::Next {
        cursor_id,
        pc_if_next: loop_start,
        fullscan: false,
        is_index: false,
    });
    program.preassign_label_to_next_insn(loop_end);
}

/// Inserts `row` under a new rowid, with the oid in `oid_reg` as the oid of
/// the table the row belongs to.
fn emit_row_insert(
    program: &mut ProgramBuilder,
    cursor_id: usize,
    oid_reg: usize,
    row: &[AttributeValue],
) {
    let first_column_reg = program.alloc_registers(row.len());
    for (position, value) in row.iter().enumerate() {
        let dest = first_column_reg + position;
        match value {
            AttributeValue::Integer(value) => {
                program.emit_insn(Insn::Integer {
                    value: *value,
                    dest,
                });
            }
            AttributeValue::Text(value) => {
                program.emit_insn(Insn::String8 {
                    value: value.clone(),
                    dest,
                });
            }
            AttributeValue::Null => {
                program.emit_insn(Insn::Null {
                    dest,
                    dest_end: None,
                });
            }
            AttributeValue::TableOid => {
                program.emit_insn(Insn::Copy {
                    src_reg: oid_reg,
                    dst_reg: dest,
                    extra_amount: 0,
                });
            }
        }
    }
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(first_column_reg),
        count: to_u32(row.len()),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });
    let rowid_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: cursor_id,
        rowid_reg,
        prev_largest_reg: 0,
    });
    program.emit_insn(Insn::Insert {
        cursor: cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: InsertFlags::new()
            .skip_last_rowid()
            .skip_all_change_counts(),
        table_name: ATTRIBUTE_TABLE_NAME.to_string(),
    });
}

/// The row of the column at `position` of `table`.
fn attribute_row(
    schema: &Schema,
    table: &BTreeTable,
    position: usize,
) -> [AttributeValue; ATTRIBUTE_COLUMNS.len()] {
    use AttributeValue::{Integer, Null, TableOid, Text};

    let column = &table.columns()[position];
    let type_oid = match schema.get_type_def(&column.ty_str, table.is_strict) {
        Some(type_def) if !type_def.is_builtin => user_type_oid(&type_def.name),
        _ => sqlite_type_to_pg_oid(&column.ty_str),
    };
    let storage = if column.is_array() {
        "x"
    } else {
        type_storage(schema, &column.ty_str)
    };
    let has_default =
        (column.default.is_some() && column.identity.is_none()) || column.is_stored_generated();
    let generated = if column.is_stored_generated() {
        "s"
    } else {
        ""
    };
    let identity = match column.identity {
        Some(ast::IdentityKind::Always) => "a",
        Some(ast::IdentityKind::ByDefault) => "d",
        None => "",
    };

    [
        TableOid,                                      // attrelid
        Text(column.name.clone().unwrap_or_default()), // attname
        Integer(type_oid),                             // atttypid
        Integer(-1),                                   // attstattarget
        Integer(-1),                                   // attlen
        Integer(position as i64 + 1),                  // attnum
        Integer(0),                                    // attndims
        Integer(-1),                                   // attcacheoff
        Integer(-1),                                   // atttypmod
        Integer(1),                                    // attbyval
        Text(storage.to_string()),                     // attstorage
        Text("i".to_string()),                         // attalign
        Text(String::new()),                           // attcompression
        Integer(i64::from(column.notnull())),          // attnotnull
        Integer(i64::from(has_default)),               // atthasdef
        Integer(0),                                    // atthasmissing
        Text(identity.to_string()),                    // attidentity
        Text(generated.to_string()),                   // attgenerated
        Integer(0),                                    // attisdropped
        Integer(1),                                    // attislocal
        Integer(0),                                    // attinhcount
        Integer(0),                                    // attcollation
        Null,                                          // attacl
        Null,                                          // attoptions
        Null,                                          // attfdwoptions
        Null,                                          // attmissingval
        Integer(ATTRIBUTE_TABLE_OID),                  // tableoid
    ]
}
