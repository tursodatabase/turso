//! Incremental blob I/O — Turso's implementation of SQLite's `sqlite3_blob_open`
//! family (`sqlite3_blob_read`/`write`/`bytes`/`close`).
//!
//! ## A principled departure from SQLite
//!
//! In SQLite the byte-level blob reads and writes are implemented in C directly against
//! the b-tree (`sqlite3BtreePayload`/`sqlite3BtreePutData`); the VDBE is used only to
//! position a cursor. Turso instead treats its bytecode VM as a general execution
//! engine, so the byte I/O runs **through the VDBE**: a `Blob` handle owns a paused
//! bytecode program that holds the row's cursor open, and each read/write rebinds a few
//! parameters and steps a `BlobRead`/`BlobWrite` opcode. Those opcodes are backed by
//! faithful, overflow-aware, in-place payload access in `storage::btree`, so large
//! values are streamed page-by-page exactly as SQLite's API promises — the value is
//! never materialized in full.
//!
//! ## Expiry
//!
//! Like SQLite, a handle is *expired* when the row it points to is written (updated,
//! deleted, or replaced) after the handle is opened; writes to *other* rows of the
//! same table merely disturb the cursor, which re-seeks its pinned rowid on the next
//! access. The enforcement lives in the b-tree cursor: a peer write to the same root
//! first saves every cursor's position and notifies each one of the rowid being
//! written (`note_external_row_write`, SQLite's invalidateIncrblobCursors), and every
//! blob entry point either restores its position or fails with
//! [`LimboError::BlobHandleExpired`]. Expiry does not abort the paused program —
//! writes made before it remain part of the handle's transaction and commit at
//! close, exactly as `sqlite3_blob_write` documents.

use std::num::NonZero;
use std::sync::Arc;

use crate::alloc::TursoSliceExt;
use crate::schema::{BTreeTable, Column, Schema};
use crate::storage::pager::Pager;
use crate::vdbe::builder::{CursorType, ProgramBuilder, ProgramBuilderOpts, QueryMode};
use crate::vdbe::insn::{CmpInsFlags, Insn, RegisterOrLiteral};
use crate::{Connection, LimboError, Numeric, Result, Statement, StepResult, Value};

/// Outcome of driving a blob handle's paused program to its next stop.
#[derive(PartialEq, Eq)]
enum StepOutcome {
    /// The program yielded a result row.
    Row,
    /// The program halted.
    Done,
}

/// Advance `stmt` to its next stop, pumping IO and mapping Interrupt/Busy to
/// [`LimboError::Busy`]. The single step loop behind read/write/close/open.
fn pump_step(stmt: &mut Statement, _pager: &Pager) -> Result<StepOutcome> {
    loop {
        match stmt.step()? {
            StepResult::Row => return Ok(StepOutcome::Row),
            StepResult::Done => return Ok(StepOutcome::Done),
            StepResult::IO | StepResult::Yield => stmt.wait_for_io()?,
            StepResult::Interrupt | StepResult::Busy => return Err(LimboError::Busy),
        }
    }
}

/// An open incremental-blob handle to a single column value of a single row.
pub struct Blob {
    stmt: Statement,
    pager: Arc<crate::storage::pager::Pager>,
    /// Byte length of the value; fixed for the handle's lifetime (the API cannot
    /// resize). Captured by the handle's own program, so it can't race other statements.
    len: usize,
    read_only: bool,
    /// Latched when an op reports [`LimboError::BlobHandleExpired`] (the handle's row
    /// was written after open). The paused program stays alive, but every later op
    /// keeps failing — SQLite's SQLITE_ABORT contract for expired handles.
    expired: bool,
}

impl std::fmt::Debug for Blob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Blob")
            .field("len", &self.len)
            .field("read_only", &self.read_only)
            .field("expired", &self.expired)
            .finish_non_exhaustive()
    }
}

impl Blob {
    /// Number of bytes in the value (`sqlite3_blob_bytes`).
    pub fn bytes(&self) -> usize {
        self.len
    }

    /// Read `buf.len()` bytes starting at `offset` (`sqlite3_blob_read`).
    pub fn read(&mut self, offset: usize, buf: &mut [u8]) -> Result<()> {
        if self.expired {
            return Err(LimboError::BlobHandleExpired);
        }
        self.check_in_bounds(offset, buf.len())?;
        if buf.is_empty() {
            return Ok(());
        }
        // op=1 (read), offset, amount.
        self.bind(1, 1)?;
        self.bind(2, offset as i64)?;
        self.bind(3, buf.len() as i64)?;
        match self.step_op()? {
            Value::Blob(b) if b.len() == buf.len() => {
                buf.copy_from_slice(&b);
                Ok(())
            }
            other => Err(LimboError::InternalError(format!(
                "blob read returned unexpected value {other:?}"
            ))),
        }
    }

    /// Write `data` starting at `offset` (`sqlite3_blob_write`). Cannot resize the
    /// value — the range must lie within it.
    pub fn write(&mut self, offset: usize, data: &[u8]) -> Result<()> {
        if self.expired {
            return Err(LimboError::BlobHandleExpired);
        }
        if self.read_only {
            return Err(LimboError::ReadOnly);
        }
        self.check_in_bounds(offset, data.len())?;
        if data.is_empty() {
            return Ok(());
        }
        // op=2 (write), offset, src blob.
        self.bind(1, 2)?;
        self.bind(2, offset as i64)?;
        self.bind_blob(4, data.try_to_vec()?)?;
        match self.step_op()? {
            Value::Numeric(Numeric::Integer(1)) => Ok(()),
            other => Err(LimboError::InternalError(format!(
                "blob write returned unexpected ack {other:?}"
            ))),
        }
    }

    /// Close the handle, halting its program and committing its transaction. Consumed
    /// unconditionally (like `sqlite3_blob_close`): even on error the statement drops
    /// and its cleanup releases the transaction. Expiry doesn't abort the program, so
    /// pre-expiry writes still commit here.
    pub fn close(mut self) -> Result<()> {
        self.bind(1, 0)?; // op=0: leave the loop and halt.
        while pump_step(&mut self.stmt, &self.pager)? == StepOutcome::Row {}
        Ok(())
    }

    /// Reject a `[offset, offset+len)` access that overflows or reaches past the
    /// value. The value's length is fixed for the handle's lifetime, so this is the
    /// same check for reads and writes.
    fn check_in_bounds(&self, offset: usize, len: usize) -> Result<()> {
        if offset.checked_add(len).is_none_or(|end| end > self.len) {
            return Err(LimboError::InternalError(
                "blob access out of range".to_string(),
            ));
        }
        Ok(())
    }

    fn bind(&mut self, index: usize, value: i64) -> Result<()> {
        self.stmt.bind_at(
            NonZero::new(index).unwrap(),
            Value::Numeric(Numeric::Integer(value)),
        )
    }

    fn bind_blob(&mut self, index: usize, value: crate::alloc::Vec<u8>) -> Result<()> {
        self.stmt
            .bind_at(NonZero::new(index).unwrap(), Value::Blob(value))
    }

    /// Step the bound op to its result row and return the single acknowledging value.
    /// A NULL ack is the program's expiry signal (the row was written after the
    /// handle pinned it): latch `expired` and report it, without disturbing the
    /// still-paused program, so `close()` can commit writes made before the expiry.
    fn step_op(&mut self) -> Result<Value> {
        let value = match pump_step(&mut self.stmt, &self.pager)? {
            StepOutcome::Row => self
                .stmt
                .row()
                .expect("step returned Row so a row must be present")
                .get_values()
                .next()
                .cloned(),
            StepOutcome::Done => {
                return Err(LimboError::InternalError(
                    "blob statement ended unexpectedly".to_string(),
                ))
            }
        };
        match value {
            Some(Value::Null) => {
                self.expired = true;
                Err(LimboError::BlobHandleExpired)
            }
            Some(value) => Ok(value),
            None => Err(LimboError::InternalError(
                "blob op produced an empty result row".to_string(),
            )),
        }
    }
}

/// Refuse a read-write handle on any column whose bytes other structures depend on.
/// An in-place byte write bypasses index maintenance, foreign-key enforcement, and
/// materialized-view upkeep, so allowing it would silently desynchronize them —
/// this is SQLite's "cannot open indexed column for writing" rule, applied strictly.
/// Expression indexes and partial indexes are rejected without inspecting their
/// ASTs: a wrong "column not referenced" answer corrupts the index, so any such
/// index on the table blocks the write. Foreign-key child columns are rejected only
/// while foreign-key enforcement is on, matching sqlite3_blob_open.
fn check_column_writable(
    schema: &Schema,
    table: &BTreeTable,
    col_idx: usize,
    col: &Column,
    fk_enforced: bool,
) -> Result<()> {
    if table.name.starts_with("sqlite_") {
        return Err(LimboError::InternalError(format!(
            "table {} may not be modified",
            table.name
        )));
    }
    let col_name = col.name.as_deref().unwrap_or("");
    let indexed = schema.get_indices(&table.name).any(|index| {
        index
            .columns
            .iter()
            .any(|ic| ic.pos_in_table == col_idx || ic.expr.is_some())
            || index.where_clause.is_some()
    }) || table
        .primary_key_columns
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case(col_name))
        || table.unique_sets.iter().any(|set| {
            set.columns
                .iter()
                .any(|(name, _)| name.eq_ignore_ascii_case(col_name))
        });
    if indexed {
        return Err(LimboError::InternalError(
            "cannot open indexed column for writing".to_string(),
        ));
    }
    if fk_enforced
        && table.foreign_keys.iter().any(|fk| {
            fk.child_columns
                .iter()
                .any(|name| name.eq_ignore_ascii_case(col_name))
        })
    {
        return Err(LimboError::InternalError(
            "cannot open foreign key column for writing".to_string(),
        ));
    }
    if schema
        .table_to_materialized_views
        .get(&table.name)
        .is_some_and(|views| !views.is_empty())
    {
        return Err(LimboError::InternalError(format!(
            "cannot open column of table {} for writing: materialized views depend on it",
            table.name
        )));
    }
    Ok(())
}

impl Connection {
    /// Open an incremental-blob handle to `column` of the row with `rowid` in `table`
    /// (`sqlite3_blob_open`). `read_write` requests write access.
    pub fn blob_open(
        self: &Arc<Self>,
        table: &str,
        column: &str,
        rowid: i64,
        read_write: bool,
    ) -> Result<Blob> {
        let fk_enforced = self.foreign_keys_enabled();
        let (table_ref, record_column) = self.with_schema_mut(|schema| {
            if schema.is_materialized_view(table) || schema.get_view(table).is_some() {
                return Err(LimboError::InternalError(format!(
                    "cannot open view: {table}"
                )));
            }
            if matches!(
                schema.get_table(table).as_deref(),
                Some(crate::schema::Table::Virtual(_))
            ) {
                return Err(LimboError::InternalError(format!(
                    "cannot open virtual table: {table}"
                )));
            }
            let tbl = schema
                .get_btree_table(table)
                .ok_or_else(|| LimboError::InternalError(format!("no such table: {table}")))?;
            if !tbl.has_rowid {
                return Err(LimboError::InternalError(format!(
                    "cannot open table without rowid: {table}"
                )));
            }
            let (idx, col) = tbl.get_column(column).ok_or_else(|| {
                LimboError::InternalError(format!("no such column: {table}.{column}"))
            })?;
            if col.is_rowid_alias() {
                // The value of a rowid alias IS the integer rowid; the record slot
                // holds NULL and there are no bytes to address.
                return Err(LimboError::InternalError(
                    "cannot open value of type integer".to_string(),
                ));
            }
            if col.is_virtual_generated() {
                return Err(LimboError::InternalError(format!(
                    "cannot open generated column: {column}"
                )));
            }
            if read_write {
                check_column_writable(schema, &tbl, idx, col, fk_enforced)?;
            }
            // Virtual generated columns are not stored, so the schema position and
            // the record position diverge on tables that have them; the map gives
            // the on-disk position the b-tree layer must address.
            let record_column = tbl
                .logical_to_physical_map
                .get(idx)
                .copied()
                .filter(|&pos| pos != usize::MAX)
                .ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "column {table}.{column} has no stored record position"
                    ))
                })?;
            Ok((tbl, record_column))
        })??;

        let root = table_ref.root_page;
        let program = build_blob_program(self, table_ref, root, record_column, rowid, read_write)?;
        let pager = self.get_pager();
        let mut stmt = Statement::new(program, pager.clone(), QueryMode::Normal, 0);
        // Step to the "ready" row: the cursor is open and positioned on the row, and
        // the row carries the value's byte length (BlobLen), which also validated
        // that the value is byte-addressable (TEXT or BLOB, not null/integer/real).
        let len = match pump_step(&mut stmt, &pager)? {
            StepOutcome::Row => {
                let row = stmt
                    .row()
                    .expect("step returned Row so a row must be present");
                match row.get_values().next() {
                    Some(Value::Numeric(Numeric::Integer(n))) if *n >= 0 => *n as usize,
                    other => {
                        return Err(LimboError::InternalError(format!(
                            "blob open returned an invalid length: {other:?}"
                        )))
                    }
                }
            }
            StepOutcome::Done => {
                return Err(LimboError::InternalError(format!("no such rowid: {rowid}")))
            }
        };
        Ok(Blob {
            stmt,
            pager,
            len,
            read_only: !read_write,
            expired: false,
        })
    }
}

/// Build the paused loop that holds the row's cursor open and services read/write ops
/// via bound parameters: p1 = op (1 read, 2 write, 0 close), p2 = offset, p3 = amount
/// (read), p4 = source blob (write). Each op yields one result row. The "ready" row
/// stepped to by `blob_open` carries the column value's byte length via `BlobLen`,
/// which also enforces that the value is TEXT or BLOB — both checked by the program
/// that owns the row's cursor, so neither can go stale between open and use.
fn build_blob_program(
    conn: &Arc<Connection>,
    table_ref: Arc<BTreeTable>,
    root: i64,
    record_column: usize,
    rowid: i64,
    read_write: bool,
) -> Result<crate::vdbe::Program> {
    let mut b = ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(1, 64, 16));
    b.prologue();
    if read_write {
        b.begin_write_operation()
            .map_err(|_| LimboError::InternalError("blob txn setup".to_string()))?;
    } else {
        b.begin_read_operation()
            .map_err(|_| LimboError::InternalError("blob txn setup".to_string()))?;
    }
    let cursor = b.alloc_cursor_id(CursorType::BTreeTable(table_ref));
    if read_write {
        b.emit_insn(Insn::OpenWrite {
            cursor_id: cursor,
            root_page: RegisterOrLiteral::Literal(root),
            db: 0,
        });
    } else {
        b.emit_insn(Insn::OpenRead {
            cursor_id: cursor,
            root_page: root,
            db: 0,
        });
    }
    let r_rowid = b.alloc_register();
    b.emit_insn(Insn::Integer {
        value: rowid,
        dest: r_rowid,
    });
    let l_notfound = b.allocate_label();
    b.emit_insn(Insn::SeekRowid {
        cursor_id: cursor,
        src_reg: r_rowid,
        target_pc: l_notfound,
    });

    // Ready signal: cursor open + positioned; the row is the value's byte length.
    let r_ready = b.alloc_register();
    b.emit_insn(Insn::BlobLen {
        cursor,
        column: record_column,
        dest: r_ready,
    });
    b.emit_result_row(r_ready, 1);

    let l_loop = b.allocate_label();
    let l_write = b.allocate_label();
    let l_done = b.allocate_label();
    let l_end = b.allocate_label();

    b.preassign_label_to_next_insn(l_loop);
    let r_op = b.alloc_register();
    b.emit_insn(Insn::Variable {
        index: NonZero::new(1).unwrap(),
        dest: r_op,
    });
    let r_zero = b.alloc_register();
    b.emit_insn(Insn::Integer {
        value: 0,
        dest: r_zero,
    });
    b.emit_insn(Insn::Eq {
        lhs: r_op,
        rhs: r_zero,
        target_pc: l_done,
        flags: CmpInsFlags::default(),
        collation: None,
    });
    let r_off = b.alloc_register();
    b.emit_insn(Insn::Variable {
        index: NonZero::new(2).unwrap(),
        dest: r_off,
    });
    let r_two = b.alloc_register();
    b.emit_insn(Insn::Integer {
        value: 2,
        dest: r_two,
    });
    b.emit_insn(Insn::Eq {
        lhs: r_op,
        rhs: r_two,
        target_pc: l_write,
        flags: CmpInsFlags::default(),
        collation: None,
    });

    // op == 1: read.
    let r_amt = b.alloc_register();
    b.emit_insn(Insn::Variable {
        index: NonZero::new(3).unwrap(),
        dest: r_amt,
    });
    let r_data = b.alloc_register();
    b.emit_insn(Insn::BlobRead {
        cursor,
        column: record_column,
        offset: r_off,
        amount: r_amt,
        dest: r_data,
    });
    b.emit_result_row(r_data, 1);
    b.emit_insn(Insn::Goto { target_pc: l_loop });

    // op == 2: write. The ack row is 1 on success, NULL if the handle expired.
    b.preassign_label_to_next_insn(l_write);
    let r_src = b.alloc_register();
    b.emit_insn(Insn::Variable {
        index: NonZero::new(4).unwrap(),
        dest: r_src,
    });
    let r_ack = b.alloc_register();
    b.emit_insn(Insn::BlobWrite {
        cursor,
        column: record_column,
        offset: r_off,
        src: r_src,
        dest: r_ack,
    });
    b.emit_result_row(r_ack, 1);
    b.emit_insn(Insn::Goto { target_pc: l_loop });

    b.preassign_label_to_next_insn(l_done);
    b.emit_insn(Insn::Goto { target_pc: l_end });
    b.preassign_label_to_next_insn(l_notfound);
    b.emit_halt_err(1, format!("no such rowid: {rowid}"));
    b.preassign_label_to_next_insn(l_end);

    conn.with_schema_mut(|schema| b.epilogue(schema))?;
    b.build(conn.clone(), false, "incremental blob io")
        .map_err(|e| LimboError::InternalError(format!("blob program build failed: {e}")))
}
