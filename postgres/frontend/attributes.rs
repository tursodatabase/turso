//! Columns, PostgreSQL's `pg_attribute` rows, in the `pg_attribute` table of
//! the main database: one row per column of each user table.
//!
//! `sqlite_schema` stays the source of truth for the engine. These rows are
//! a copy for PostgreSQL clients, which read the table with plain SQL.
//! Nothing in memory holds them. The statement that creates, alters or
//! drops a table writes its rows in its own bytecode, next to its
//! `sqlite_schema` row, see [`crate::classes::PostgresCatalog`].
//!
//! The unique index on `(attrelid, attnum)` has PostgreSQL's name for it.
//! The bytecode here keeps it in step with the table, as an INSERT or a
//! DELETE would.
//!
//! A table gets rows when it gets a class row, so the tables of the temp
//! database and of an attached database have none.

use turso_core::ast;
use turso_core::schema::{BTreeTable, Column, Schema};
use turso_core::{to_u32, InsertFlags, Insn, ProgramBuilder, Result};

use crate::catalog::{sqlite_type_to_pg_oid, type_storage, user_type_oid};
use crate::schema_init::TableInit;
use crate::unique_index::{emit_unique_index_insert, open_table_and_index, Cursors};

pub(crate) const ATTRIBUTE_TABLE_NAME: &str = "pg_attribute";

pub(crate) const ATTRIBUTE_INDEX_NAME: &str = "pg_attribute_relid_attnum_index";

/// PostgreSQL's oid of `pg_attribute`, which every row has as its
/// `tableoid`.
const PG_ATTRIBUTE_OID: i64 = 1249;

// `tableoid` is a system column in PostgreSQL. It is stored here, and
// `schema_init::hide_system_columns` keeps it out of `SELECT *`.
const CREATE_ATTRIBUTE_TABLE_SQL: &str = "CREATE TABLE pg_attribute (\
    attrelid INTEGER, \
    attname TEXT, \
    atttypid INTEGER, \
    attstattarget INTEGER, \
    attlen INTEGER, \
    attnum INTEGER, \
    attndims INTEGER, \
    attcacheoff INTEGER, \
    atttypmod INTEGER, \
    attbyval BOOLEAN, \
    attstorage TEXT, \
    attalign TEXT, \
    attcompression TEXT, \
    attnotnull BOOLEAN, \
    atthasdef BOOLEAN, \
    atthasmissing BOOLEAN, \
    attidentity TEXT, \
    attgenerated TEXT, \
    attisdropped BOOLEAN, \
    attislocal BOOLEAN, \
    attinhcount INTEGER, \
    attcollation INTEGER, \
    attacl TEXT, \
    attoptions TEXT, \
    attfdwoptions TEXT, \
    attmissingval TEXT, \
    tableoid INTEGER)";

const CREATE_ATTRIBUTE_INDEX_SQL: &str =
    "CREATE UNIQUE INDEX pg_attribute_relid_attnum_index ON pg_attribute (attrelid, attnum)";

const ATTRELID_COLUMN: usize = 0;
const ATTNUM_COLUMN: usize = 5;
const ATTRIBUTE_COLUMN_COUNT: usize = 27;

// An index entry is `(attrelid, attnum, rowid)`.
const INDEX_ATTNUM_COLUMN: usize = 1;
const INDEX_KEY_LEN: usize = 3;

pub(crate) const TABLE_INIT: TableInit = TableInit {
    table: ATTRIBUTE_TABLE_NAME,
    statements: &[CREATE_ATTRIBUTE_TABLE_SQL, CREATE_ATTRIBUTE_INDEX_SQL],
};

/// Inserts the rows of the columns of `table`, a new table whose oid is in
/// `attrelid_reg`.
pub(crate) fn emit_insert_attributes(
    program: &mut ProgramBuilder,
    schema: &Schema,
    attrelid_reg: usize,
    table: &BTreeTable,
) -> Result<()> {
    if let Some(cursors) = open_attribute_table(program, schema)? {
        emit_insert_rows(program, schema, &cursors, attrelid_reg, table);
    }
    Ok(())
}

/// Replaces the rows of the table with oid `attrelid` by the rows of the
/// columns of `table`, which is that table after the running statement.
pub(crate) fn emit_replace_attributes(
    program: &mut ProgramBuilder,
    schema: &Schema,
    attrelid: i64,
    table: &BTreeTable,
) -> Result<()> {
    if let Some(cursors) = open_attribute_table(program, schema)? {
        let attrelid_reg = program.alloc_register();
        program.emit_int(attrelid, attrelid_reg);
        emit_delete_rows(program, &cursors, attrelid_reg);
        emit_insert_rows(program, schema, &cursors, attrelid_reg, table);
    }
    Ok(())
}

/// Deletes the rows of the table with oid `attrelid`, which the running
/// statement drops.
pub(crate) fn emit_delete_attributes(
    program: &mut ProgramBuilder,
    schema: &Schema,
    attrelid: i64,
) -> Result<()> {
    if let Some(cursors) = open_attribute_table(program, schema)? {
        let attrelid_reg = program.alloc_register();
        program.emit_int(attrelid, attrelid_reg);
        emit_delete_rows(program, &cursors, attrelid_reg);
    }
    Ok(())
}

/// Opens the table and its index for writing. None when the database has
/// no such table, which only happens outside the frontend's own open path,
/// such as a core-level open for tests.
fn open_attribute_table(program: &mut ProgramBuilder, schema: &Schema) -> Result<Option<Cursors>> {
    open_table_and_index(program, schema, ATTRIBUTE_TABLE_NAME, ATTRIBUTE_INDEX_NAME)
}

fn emit_insert_rows(
    program: &mut ProgramBuilder,
    schema: &Schema,
    cursors: &Cursors,
    attrelid_reg: usize,
    table: &BTreeTable,
) {
    let row_reg = program.alloc_registers(ATTRIBUTE_COLUMN_COUNT);
    let record_reg = program.alloc_register();
    let key_reg = program.alloc_registers(INDEX_KEY_LEN);
    let rowid_reg = key_reg + INDEX_KEY_LEN - 1;
    for (position, column) in table.columns().iter().enumerate() {
        program.emit_insn(Insn::Copy {
            src_reg: attrelid_reg,
            dst_reg: row_reg + ATTRELID_COLUMN,
            extra_amount: 0,
        });
        let values = attribute_values(schema, table, position, column);
        for (value, reg) in values.into_iter().zip(row_reg + ATTRELID_COLUMN + 1..) {
            match value {
                Constant::Integer(value) => program.emit_int(value, reg),
                Constant::Text(value) => program.emit_string8(value.to_string(), reg),
                Constant::Null => program.emit_null(reg, None),
            }
        }
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(row_reg),
            count: to_u32(ATTRIBUTE_COLUMN_COUNT),
            dest_reg: to_u32(record_reg),
            index_name: None,
            affinity_str: None,
        });
        program.emit_insn(Insn::NewRowid {
            cursor: cursors.table,
            rowid_reg,
            prev_largest_reg: 0,
        });
        program.emit_insn(Insn::Copy {
            src_reg: row_reg + ATTRELID_COLUMN,
            dst_reg: key_reg,
            extra_amount: 0,
        });
        program.emit_insn(Insn::Copy {
            src_reg: row_reg + ATTNUM_COLUMN,
            dst_reg: key_reg + INDEX_ATTNUM_COLUMN,
            extra_amount: 0,
        });
        // The catalog never writes a column number of a relation twice, so
        // this only fails on a table that something else wrote.
        emit_unique_index_insert(
            program,
            cursors.index,
            ATTRIBUTE_INDEX_NAME,
            key_reg,
            INDEX_KEY_LEN,
            format!("{ATTRIBUTE_TABLE_NAME} already has a row for this column"),
        );
        program.emit_insn(Insn::Insert {
            cursor: cursors.table,
            key_reg: rowid_reg,
            record_reg,
            flag: InsertFlags::new()
                .skip_last_rowid()
                .skip_all_change_counts(),
            table_name: ATTRIBUTE_TABLE_NAME.to_string(),
        });
    }
}

/// A value that is known when the statement is compiled.
enum Constant<'a> {
    Integer(i64),
    Text(&'a str),
    Null,
}

/// The values of the row of `column`, the column at `position` of `table`,
/// in the order of the table's columns after `attrelid`.
///
/// FIXME(pg-catalog): `attlen`, `attbyval`, `attalign` and `atttypmod` are
/// the same for every column. PostgreSQL takes them from the column's type
/// and its modifier: smallint has `attlen` 2 and `attalign` 's', and
/// varchar(10) has `atttypmod` 14. An array column gets the oid of its
/// element type and `attndims` 0, where PostgreSQL gives int[] the oid 1007
/// and `attndims` 1. A lookup of the column's type in a pg_type table
/// replaces these values.
fn attribute_values<'a>(
    schema: &Schema,
    table: &BTreeTable,
    position: usize,
    column: &'a Column,
) -> [Constant<'a>; ATTRIBUTE_COLUMN_COUNT - 1] {
    use Constant::{Integer, Null, Text};

    let atttypid = match schema.get_type_def(&column.ty_str, table.is_strict) {
        Some(type_def) if !type_def.is_builtin => user_type_oid(&type_def.name),
        _ => sqlite_type_to_pg_oid(&column.ty_str),
    };
    let attstorage = if column.is_array() {
        "x"
    } else {
        type_storage(schema, &column.ty_str)
    };
    // The default of an identity column is its sequence, which PostgreSQL
    // does not count as a default.
    let atthasdef =
        (column.default.is_some() && column.identity.is_none()) || column.is_stored_generated();
    let attidentity = match column.identity {
        Some(ast::IdentityKind::Always) => "a",
        Some(ast::IdentityKind::ByDefault) => "d",
        None => "",
    };
    let attgenerated = if column.is_stored_generated() {
        "s"
    } else {
        ""
    };
    [
        Text(column.name.as_deref().unwrap_or_default()), // attname
        Integer(atttypid),
        Integer(-1),                  // attstattarget
        Integer(-1),                  // attlen
        Integer(position as i64 + 1), // attnum
        Integer(0),                   // attndims
        Integer(-1),                  // attcacheoff
        Integer(-1),                  // atttypmod
        Integer(1),                   // attbyval
        Text(attstorage),
        Text("i"),                            // attalign
        Text(""),                             // attcompression
        Integer(i64::from(column.notnull())), // attnotnull
        Integer(i64::from(atthasdef)),
        Integer(0), // atthasmissing
        Text(attidentity),
        Text(attgenerated),
        Integer(0),                // attisdropped
        Integer(1),                // attislocal
        Integer(0),                // attinhcount
        Integer(0),                // attcollation
        Null,                      // attacl
        Null,                      // attoptions
        Null,                      // attfdwoptions
        Null,                      // attmissingval
        Integer(PG_ATTRIBUTE_OID), // tableoid
    ]
}

/// Deletes the rows whose `attrelid` is the value of `attrelid_reg`, and
/// their index entries.
fn emit_delete_rows(program: &mut ProgramBuilder, cursors: &Cursors, attrelid_reg: usize) {
    let key_reg = program.alloc_registers(INDEX_KEY_LEN);
    let rowid_reg = key_reg + INDEX_KEY_LEN - 1;
    program.emit_insn(Insn::Copy {
        src_reg: attrelid_reg,
        dst_reg: key_reg,
        extra_amount: 0,
    });

    // Each pass seeks the first entry that is left and deletes it, so the
    // loop does not depend on where a delete leaves the index cursor.
    let loop_start = program.allocate_label();
    let loop_end = program.allocate_label();
    program.preassign_label_to_next_insn(loop_start);
    program.emit_insn(Insn::SeekGE {
        is_index: true,
        cursor_id: cursors.index,
        start_reg: key_reg,
        num_regs: 1,
        target_pc: loop_end,
        eq_only: false,
        null_matching_mask: Default::default(),
    });
    program.emit_insn(Insn::IdxGT {
        cursor_id: cursors.index,
        start_reg: key_reg,
        num_regs: 1,
        target_pc: loop_end,
    });
    program.emit_insn(Insn::Column {
        cursor_id: cursors.index,
        column: INDEX_ATTNUM_COLUMN,
        dest: key_reg + INDEX_ATTNUM_COLUMN,
        default: None,
    });
    program.emit_insn(Insn::IdxRowId {
        cursor_id: cursors.index,
        dest: rowid_reg,
    });
    let row_deleted = program.allocate_label();
    program.emit_insn(Insn::NotExists {
        cursor: cursors.table,
        rowid_reg,
        target_pc: row_deleted,
    });
    // Marked as part of an update so that changes() does not count it.
    program.emit_insn(Insn::Delete {
        cursor_id: cursors.table,
        table_name: ATTRIBUTE_TABLE_NAME.to_string(),
        is_part_of_update: true,
    });
    program.preassign_label_to_next_insn(row_deleted);
    program.emit_insn(Insn::IdxDelete {
        start_reg: key_reg,
        num_regs: INDEX_KEY_LEN,
        cursor_id: cursors.index,
        raise_error_if_no_matching_entry: true,
    });
    program.emit_insn(Insn::Goto {
        target_pc: loop_start,
    });
    program.preassign_label_to_next_insn(loop_end);
}
