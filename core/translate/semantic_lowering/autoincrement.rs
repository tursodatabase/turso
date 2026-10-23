//! AUTOINCREMENT for HIR INSERT: like SQLite, the largest rowid a
//! table has ever used is kept in its `sqlite_sequence` row, and a new rowid
//! is always larger than it. The row is written when the statement ends,
//! so a statement that stops with FAIL leaves it alone.
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
            triggers_insert_into_table: autoincrement.triggers_insert_into_table,
        }))
    }

    /// At the start of an INSERT: the sequence as the statement sees it.
    pub(super) fn begin(&self, program: &mut ProgramBuilder) -> StatementSequence<'_> {
        match self {
            Self::Table(sequence) => StatementSequence::Table(sequence.begin(program)),
            Self::Mvcc(operation) => StatementSequence::Mvcc(operation),
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

/// The sequence of an INSERT statement. Like SQLite, `sqlite_sequence` is
/// read at the start and whenever a rowid is generated, as a trigger may
/// have written it, and the largest rowid the statement used is written
/// back once, when the statement ends: a statement that fails with FAIL
/// keeps its rows but leaves `sqlite_sequence` alone.
pub(super) enum StatementSequence<'a> {
    Table(TableSequenceState<'a>),
    Mvcc(&'a hir::SequenceOperation),
}

impl StatementSequence<'_> {
    /// Generate the rowid of a new row into `rowid`.
    pub(super) fn emit_new_rowid(
        &self,
        program: &mut ProgramBuilder,
        table_cursor: usize,
        rowid: usize,
    ) -> Result<()> {
        match self {
            Self::Table(state) => {
                state.emit_new_rowid(program, table_cursor, rowid);
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
            Self::Table(state) => {
                state.emit_given_rowid(program, rowid);
                Ok(())
            }
            Self::Mvcc(operation) => emit_advance_past(program, operation, rowid),
        }
    }

    /// Once the row's rowid is known: an INSERT a trigger runs reads
    /// `sqlite_sequence` from disk, so when a trigger inserts into the
    /// table, the rowids this statement used are written first.
    pub(super) fn emit_visible_to_triggers(&self, program: &mut ProgramBuilder) {
        if let Self::Table(state) = self {
            if state.sequence.triggers_insert_into_table {
                state.emit_write_if_advanced(program);
            }
        }
    }

    /// After a row was moved to the rowid in `rowid` by an UPSERT: under
    /// MVCC, advance the hidden sequence past it.
    pub(super) fn emit_moved_row(&self, program: &mut ProgramBuilder, rowid: usize) -> Result<()> {
        match self {
            Self::Table(_) => Ok(()),
            Self::Mvcc(operation) => emit_advance_past(program, operation, rowid),
        }
    }

    /// When the statement ends: store the largest rowid it used.
    pub(super) fn emit_end(&self, program: &mut ProgramBuilder) {
        if let Self::Table(state) = self {
            state.emit_end(program);
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
    /// Whether a trigger in the schema inserts into the target.
    pub(super) triggers_insert_into_table: bool,
}

impl TableSequence {
    /// At the start of an INSERT: open `sqlite_sequence`, read the
    /// target's row, and start the largest rowid from its value.
    pub(super) fn begin(&self, program: &mut ProgramBuilder) -> TableSequenceState<'_> {
        let cursor = self.open_write(program);
        let name = program.emit_string8_new_reg(self.name.clone());
        let state = TableSequenceState {
            sequence: self,
            cursor,
            name,
            value: program.alloc_register(),
            rowid: program.alloc_register(),
            max_rowid: program.alloc_register(),
        };
        state.reload(program);
        program.emit_insn(Insn::Copy {
            src_reg: state.value,
            dst_reg: state.max_rowid,
            extra_amount: 0,
        });
        state
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

/// `sqlite_sequence` open for an INSERT: the target's sequence value and
/// the rowid of its row, NULL while there is none, and the largest rowid
/// the statement has used.
pub(super) struct TableSequenceState<'a> {
    sequence: &'a TableSequence,
    cursor: usize,
    name: usize,
    value: usize,
    rowid: usize,
    max_rowid: usize,
}

impl TableSequenceState<'_> {
    /// Generate the rowid of a new row into `rowid`: one more than the
    /// largest of the sequence value, the rowids the statement used and
    /// the table's largest rowid.
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
            src_reg: self.max_rowid,
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
        program.emit_insn(Insn::Copy {
            src_reg: rowid,
            dst_reg: self.max_rowid,
            extra_amount: 0,
        });
    }

    /// Once the rowid is known, before any constraint is checked: a rowid
    /// from the statement counts as used. Like SQLite, this happens even
    /// when the row is then skipped.
    pub(super) fn emit_given_rowid(&self, program: &mut ProgramBuilder, rowid: usize) {
        program.emit_insn(Insn::MemMax {
            dest_reg: self.max_rowid,
            src_reg: rowid,
        });
    }

    /// When the statement ends: store the largest rowid it used, then
    /// close `sqlite_sequence`.
    pub(super) fn emit_end(&self, program: &mut ProgramBuilder) {
        self.emit_write_if_advanced(program);
        program.emit_insn(Insn::Close {
            cursor_id: self.cursor,
        });
    }

    /// Store the largest rowid the statement used when it is larger than
    /// the stored value, or when the target has no row yet.
    fn emit_write_if_advanced(&self, program: &mut ProgramBuilder) {
        self.reload(program);
        let write = program.allocate_label();
        let done = program.allocate_label();
        program.emit_insn(Insn::IsNull {
            reg: self.rowid,
            target_pc: write,
        });
        program.emit_insn(Insn::Le {
            lhs: self.max_rowid,
            rhs: self.value,
            target_pc: done,
            flags: Default::default(),
            collation: None,
        });
        program.preassign_label_to_next_insn(write);
        self.emit_write(program, self.max_rowid);
        program.preassign_label_to_next_insn(done);
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
        // SQLite writes the row with the table name as declared, so the
        // name is matched without case, and the stored spelling is kept
        // for the write, so SQLite finds the row afterwards.
        let name = program.alloc_register();
        program.emit_column_or_rowid(self.cursor, 0, name);
        program.emit_insn(Insn::Ne {
            lhs: self.name,
            rhs: name,
            target_pc: other,
            flags: Default::default(),
            collation: Some(crate::translate::collate::CollationSeq::NoCase),
        });
        program.emit_insn(Insn::Copy {
            src_reg: name,
            dst_reg: self.name,
            extra_amount: 0,
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
            is_index: program.cursor_is_index(self.cursor),
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
            // Bookkeeping: not a change of the statement's, and not the
            // last inserted rowid.
            flag: InsertFlags::new()
                .skip_last_rowid()
                .skip_all_change_counts(),
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
