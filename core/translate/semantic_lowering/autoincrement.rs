//! AUTOINCREMENT for HIR INSERT: like SQLite, the largest rowid a
//! table has ever used is kept in its `sqlite_sequence` row, and a new rowid
//! is always larger than it.
//!
//! Under MVCC, the table's hidden sequence hands out new rowids
//! instead and keeps `sqlite_sequence` itself; a rowid from the statement, or
//! one an UPDATE or UPSERT moves a row to, advances it.

use crate::{
    error::SQLITE_FULL,
    schema::{BTreeTable, SQLITE_SEQUENCE_TABLE_NAME},
    sync::Arc,
    translate::semantic::hir,
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::{to_u32, InsertFlags, Insn},
    },
    LimboError, Result,
};

/// Where an AUTOINCREMENT table's sequence is kept.
pub(super) enum Sequence {
    Table(TableSequence),
    /// Under MVCC: the hidden sequence.
    Mvcc(hir::SequenceOperation),
}

impl Sequence {
    /// The sequence of an INSERT or UPDATE target. `mvcc` says whether its
    /// database runs MVCC.
    pub(super) fn new(
        autoincrement: &hir::ResolvedAutoincrement,
        database: usize,
        table: &BTreeTable,
        mvcc: bool,
    ) -> Result<Self> {
        if mvcc {
            let operation = autoincrement.mvcc_sequence.clone().ok_or_else(|| {
                LimboError::InternalError(format!(
                    "missing implicit sequence for AUTOINCREMENT table \"{}\"",
                    table.name
                ))
            })?;
            return Ok(Self::Mvcc(operation));
        }
        let sqlite_sequence = autoincrement.sqlite_sequence.value().require_btree()?;
        check_sqlite_sequence(&sqlite_sequence)?;
        Ok(Self::Table(TableSequence {
            table: sqlite_sequence,
            database,
            name: table.name.clone(),
        }))
    }

    /// Before an INSERT of several rows: add the target's row to
    /// `sqlite_sequence` with 0 when it has none.
    pub(super) fn ensure_row(&self, program: &mut ProgramBuilder) {
        if let Self::Table(sequence) = self {
            sequence.ensure_row(program);
        }
    }

    /// At the start of a row.
    pub(super) fn open(&self, program: &mut ProgramBuilder) -> SequenceCursor<'_> {
        match self {
            Self::Table(sequence) => SequenceCursor::Table(sequence.open(program)),
            Self::Mvcc(operation) => SequenceCursor::Mvcc(operation),
        }
    }

    /// After a row was moved to the rowid in `rowid`: under MVCC, advance the
    /// hidden sequence past it. `sqlite_sequence` is left
    /// alone, as in SQLite: the next new rowid is larger than every rowid
    /// anyway.
    pub(super) fn emit_moved_row(&self, program: &mut ProgramBuilder, rowid: usize) -> Result<()> {
        match self {
            Self::Table(_) => Ok(()),
            Self::Mvcc(operation) => emit_advance_past(program, operation, rowid),
        }
    }
}

/// The sequence of a row being inserted.
pub(super) enum SequenceCursor<'a> {
    Table(TableSequenceCursor<'a>),
    Mvcc(&'a hir::SequenceOperation),
}

impl SequenceCursor<'_> {
    /// Generate the rowid of a new row into `rowid`.
    pub(super) fn emit_new_rowid(
        &self,
        program: &mut ProgramBuilder,
        table_cursor: usize,
        rowid: usize,
    ) -> Result<()> {
        match self {
            Self::Table(cursor) => {
                cursor.emit_new_rowid(program, table_cursor, rowid);
                Ok(())
            }
            Self::Mvcc(operation) => {
                let (database, backing_table, sqlite_sequence) = mvcc_tables(operation)?;
                crate::translate::sequence::emit_disk_read_nextval(
                    program,
                    database,
                    backing_table,
                    sqlite_sequence,
                    &operation.normalized_name,
                    operation.sequence.value(),
                    rowid,
                    None,
                )
            }
        }
    }

    /// Once the rowid is known, before any constraint is checked: a rowid
    /// from the statement raises the sequence. Like SQLite, this happens
    /// even when the row is then skipped.
    pub(super) fn emit_given_rowid(
        &self,
        program: &mut ProgramBuilder,
        rowid: usize,
    ) -> Result<()> {
        match self {
            Self::Table(cursor) => {
                cursor.emit_given_rowid(program, rowid);
                Ok(())
            }
            Self::Mvcc(operation) => emit_advance_past(program, operation, rowid),
        }
    }

    /// After the row and its triggers.
    pub(super) fn emit_close(&self, program: &mut ProgramBuilder, rowid: usize) {
        if let Self::Table(cursor) = self {
            cursor.emit_close(program, rowid);
        }
    }
}

/// The database, backing table and `sqlite_sequence` of a hidden sequence.
fn mvcc_tables(
    operation: &hir::SequenceOperation,
) -> Result<(usize, Arc<BTreeTable>, Option<Arc<BTreeTable>>)> {
    let database = operation
        .sequence
        .database()
        .ok_or_else(|| LimboError::InternalError("HIR sequence has no owning database".into()))?;
    let backing_table = operation.backing_table.value().require_btree()?;
    let sqlite_sequence = operation
        .sqlite_sequence
        .as_ref()
        .map(|table| table.value().require_btree())
        .transpose()?;
    Ok((database.index(), backing_table, sqlite_sequence))
}

/// Under MVCC: raise the hidden sequence past the rowid in `value`.
fn emit_advance_past(
    program: &mut ProgramBuilder,
    operation: &hir::SequenceOperation,
    value: usize,
) -> Result<()> {
    let (database, backing_table, sqlite_sequence) = mvcc_tables(operation)?;
    crate::translate::sequence::emit_disk_advance_past(
        program,
        database,
        backing_table,
        sqlite_sequence,
        &operation.normalized_name,
        operation.sequence.value(),
        value,
    )
}

/// A `sqlite_sequence` table that does not have the shape
/// SQLite creates, (name, seq) with a rowid, is corruption, not a table to
/// read through.
fn check_sqlite_sequence(table: &BTreeTable) -> Result<()> {
    if !table.has_rowid {
        crate::bail_corrupt_error!("malformed sqlite_sequence: table must have rowid");
    }
    let columns = table.columns();
    if columns.len() != 2 {
        crate::bail_corrupt_error!(
            "malformed sqlite_sequence: expected 2 columns, got {}",
            columns.len()
        );
    }
    let named = |column: &crate::schema::Column, name: &str| {
        column
            .name
            .as_deref()
            .is_some_and(|column| column.eq_ignore_ascii_case(name))
    };
    if !named(&columns[0], "name") || !named(&columns[1], "seq") {
        crate::bail_corrupt_error!("malformed sqlite_sequence: expected columns (name, seq)");
    }
    Ok(())
}

/// The `sqlite_sequence` table of the target's database and the name of the
/// target's row in it.
pub(super) struct TableSequence {
    pub(super) table: Arc<BTreeTable>,
    pub(super) database: usize,
    pub(super) name: String,
}

impl TableSequence {
    /// Before an INSERT of several rows: add the target's row with 0 when
    /// it has none.
    pub(super) fn ensure_row(&self, program: &mut ProgramBuilder) {
        let cursor = self.open_write(program);
        let name = program.emit_string8_new_reg(self.name.clone());
        let exists = program.allocate_label();
        let missing = program.allocate_label();
        let next = program.allocate_label();
        program.emit_insn(Insn::Rewind {
            cursor_id: cursor,
            pc_if_empty: missing,
        });
        program.preassign_label_to_next_insn(next);
        let row_name = program.alloc_register();
        program.emit_column_or_rowid(cursor, 0, row_name);
        program.emit_insn(Insn::Eq {
            lhs: name,
            rhs: row_name,
            target_pc: exists,
            flags: Default::default(),
            collation: None,
        });
        program.emit_insn(Insn::Next {
            cursor_id: cursor,
            pc_if_next: next,
            fullscan: false,
        });
        program.preassign_label_to_next_insn(missing);
        let zero = program.alloc_register();
        program.emit_insn(Insn::Integer {
            dest: zero,
            value: 0,
        });
        let record = self.emit_record(program, name, zero);
        let rowid = program.alloc_register();
        program.emit_insn(Insn::NewRowid {
            cursor,
            rowid_reg: rowid,
            prev_largest_reg: 0,
        });
        program.emit_insn(Insn::Insert {
            cursor,
            key_reg: rowid,
            record_reg: record,
            flag: InsertFlags::new(),
            table_name: SQLITE_SEQUENCE_TABLE_NAME.to_string(),
        });
        program.preassign_label_to_next_insn(exists);
        program.emit_insn(Insn::Close { cursor_id: cursor });
    }

    /// At the start of a row: open `sqlite_sequence` and read the target's
    /// row.
    pub(super) fn open(&self, program: &mut ProgramBuilder) -> TableSequenceCursor<'_> {
        let cursor = self.open_write(program);
        let name = program.emit_string8_new_reg(self.name.clone());
        let sequence = TableSequenceCursor {
            sequence: self,
            cursor,
            name,
            value: program.alloc_register(),
            rowid: program.alloc_register(),
        };
        sequence.emit_clear(program);
        sequence.reload(program);
        sequence
    }

    fn open_write(&self, program: &mut ProgramBuilder) -> usize {
        let cursor = program.alloc_cursor_id(CursorType::BTreeTable(self.table.clone()));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: cursor,
            root_page: self.table.root_page.into(),
            db: self.database,
        });
        cursor
    }

    /// A `sqlite_sequence` record of `name` and `value`.
    fn emit_record(&self, program: &mut ProgramBuilder, name: usize, value: usize) -> usize {
        let record = program.alloc_register();
        let start = program.alloc_registers(2);
        program.emit_insn(Insn::Copy {
            src_reg: name,
            dst_reg: start,
            extra_amount: 0,
        });
        program.emit_insn(Insn::Copy {
            src_reg: value,
            dst_reg: start + 1,
            extra_amount: 0,
        });
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(start),
            count: to_u32(2),
            dest_reg: to_u32(record),
            index_name: None,
            affinity_str: Some(
                self.table
                    .columns()
                    .iter()
                    .map(|column| column.affinity().aff_mask())
                    .collect(),
            ),
        });
        record
    }
}

/// `sqlite_sequence` open for one row: the target's sequence value and the
/// rowid of its row, NULL while there is none.
pub(super) struct TableSequenceCursor<'a> {
    sequence: &'a TableSequence,
    cursor: usize,
    name: usize,
    value: usize,
    rowid: usize,
}

impl TableSequenceCursor<'_> {
    /// Generate the rowid of a new row into `rowid`: one more than the
    /// largest of the sequence value and the table's largest rowid, then
    /// store it in the sequence.
    pub(super) fn emit_new_rowid(
        &self,
        program: &mut ProgramBuilder,
        table_cursor: usize,
        rowid: usize,
    ) {
        self.reload(program);
        let largest = program.alloc_register();
        let unused = program.alloc_register();
        program.emit_insn(Insn::NewRowid {
            cursor: table_cursor,
            rowid_reg: unused,
            prev_largest_reg: largest,
        });
        program.emit_insn(Insn::Copy {
            src_reg: self.value,
            dst_reg: rowid,
            extra_amount: 0,
        });
        program.emit_insn(Insn::MemMax {
            dest_reg: rowid,
            src_reg: largest,
        });
        // Like SQLite, the statement fails once the largest rowid is used.
        let room = program.allocate_label();
        let max = program.alloc_register();
        program.emit_insn(Insn::Integer {
            dest: max,
            value: i64::MAX,
        });
        program.emit_insn(Insn::Ne {
            lhs: rowid,
            rhs: max,
            target_pc: room,
            flags: Default::default(),
            collation: None,
        });
        program.emit_insn(Insn::Halt {
            err_code: SQLITE_FULL,
            description: "database or disk is full".to_string(),
            on_error: None,
            description_reg: None,
        });
        program.preassign_label_to_next_insn(room);
        program.emit_insn(Insn::AddImm {
            register: rowid,
            value: 1,
        });
        self.emit_write(program, rowid);
    }

    /// Once the rowid is known, before any constraint is checked: a rowid
    /// from the statement raises the sequence value. Like SQLite, this
    /// happens even when the row is then skipped.
    pub(super) fn emit_given_rowid(&self, program: &mut ProgramBuilder, rowid: usize) {
        self.reload(program);
        let missing = program.allocate_label();
        let done = program.allocate_label();
        program.emit_insn(Insn::IsNull {
            reg: self.rowid,
            target_pc: missing,
        });
        program.emit_insn(Insn::Le {
            lhs: rowid,
            rhs: self.value,
            target_pc: done,
            flags: Default::default(),
            collation: None,
        });
        self.emit_write(program, rowid);
        program.emit_insn(Insn::Goto { target_pc: done });
        // Without a row yet, store max(0, rowid).
        program.preassign_label_to_next_insn(missing);
        let value = program.alloc_register();
        program.emit_insn(Insn::Copy {
            src_reg: self.value,
            dst_reg: value,
            extra_amount: 0,
        });
        program.emit_insn(Insn::MemMax {
            dest_reg: value,
            src_reg: rowid,
        });
        self.emit_write(program, value);
        program.preassign_label_to_next_insn(done);
    }

    /// After the row and its triggers: raise the sequence value to the
    /// row's rowid when it is larger, then close `sqlite_sequence`.
    pub(super) fn emit_close(&self, program: &mut ProgramBuilder, rowid: usize) {
        self.reload(program);
        let done = program.allocate_label();
        program.emit_insn(Insn::Le {
            lhs: rowid,
            rhs: self.value,
            target_pc: done,
            flags: Default::default(),
            collation: None,
        });
        self.emit_write(program, rowid);
        program.preassign_label_to_next_insn(done);
        program.emit_insn(Insn::Close {
            cursor_id: self.cursor,
        });
    }

    fn emit_clear(&self, program: &mut ProgramBuilder) {
        program.emit_insn(Insn::Integer {
            dest: self.value,
            value: 0,
        });
        program.emit_insn(Insn::Null {
            dest: self.rowid,
            dest_end: None,
        });
    }

    /// Read the target's row again, since a trigger may have changed it.
    fn reload(&self, program: &mut ProgramBuilder) {
        self.emit_clear(program);
        let next = program.allocate_label();
        let other = program.allocate_label();
        let done = program.allocate_label();
        program.emit_insn(Insn::Rewind {
            cursor_id: self.cursor,
            pc_if_empty: done,
        });
        program.preassign_label_to_next_insn(next);
        let name = program.alloc_register();
        program.emit_column_or_rowid(self.cursor, 0, name);
        program.emit_insn(Insn::Ne {
            lhs: self.name,
            rhs: name,
            target_pc: other,
            flags: Default::default(),
            collation: None,
        });
        program.emit_column_or_rowid(self.cursor, 1, self.value);
        // Like SQLite, the stored value is used as an integer.
        program.emit_insn(Insn::AddImm {
            register: self.value,
            value: 0,
        });
        program.emit_insn(Insn::RowId {
            cursor_id: self.cursor,
            dest: self.rowid,
        });
        program.emit_insn(Insn::Goto { target_pc: done });
        program.preassign_label_to_next_insn(other);
        program.emit_insn(Insn::Next {
            cursor_id: self.cursor,
            pc_if_next: next,
            fullscan: false,
        });
        program.preassign_label_to_next_insn(done);
    }

    /// Store `value` in the target's row, adding the row when it has none.
    fn emit_write(&self, program: &mut ProgramBuilder, value: usize) {
        let record = self.sequence.emit_record(program, self.name, value);
        let existing = program.allocate_label();
        let done = program.allocate_label();
        program.emit_insn(Insn::NotNull {
            reg: self.rowid,
            target_pc: existing,
        });
        program.emit_insn(Insn::NewRowid {
            cursor: self.cursor,
            rowid_reg: self.rowid,
            prev_largest_reg: 0,
        });
        program.emit_insn(Insn::Insert {
            cursor: self.cursor,
            key_reg: self.rowid,
            record_reg: record,
            flag: InsertFlags::new(),
            table_name: SQLITE_SEQUENCE_TABLE_NAME.to_string(),
        });
        program.emit_insn(Insn::Goto { target_pc: done });
        program.preassign_label_to_next_insn(existing);
        program.emit_insn(Insn::Insert {
            cursor: self.cursor,
            key_reg: self.rowid,
            record_reg: record,
            flag: InsertFlags(turso_parser::ast::ResolveType::Replace.bit_value() as u8),
            table_name: SQLITE_SEQUENCE_TABLE_NAME.to_string(),
        });
        program.preassign_label_to_next_insn(done);
    }
}
