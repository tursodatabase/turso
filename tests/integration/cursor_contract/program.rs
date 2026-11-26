//! Compiles a list of cursor ops into one straight-line VDBE program.
//!
//! Every op produces exactly one result row `(jumped, rowid, col)`, so the caller can
//! compare the observations of two engines step by step.

use std::sync::Arc;
use turso_core::namespace::NamespaceId;
use turso_core::vdbe::builder::{CursorType, ProgramBuilder, ProgramBuilderOpts, QueryMode};
use turso_core::vdbe::insn::{InsertFlags, Insn, RegisterOrLiteral};
use turso_core::vdbe::Program;
use turso_core::{Connection, Value};

/// One cursor opcode on a table cursor. `cur` is the index of the cursor, all cursors
/// are open on the same table `t(id INTEGER PRIMARY KEY, v INTEGER)`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Op {
    Rewind(usize),
    Last(usize),
    Next(usize),
    Prev(usize),
    SeekRowid(usize, i64),
    SeekGE(usize, i64),
    SeekGT(usize, i64),
    SeekLE(usize, i64),
    SeekLT(usize, i64),
    NotExists(usize, i64),
    /// Allocates a rowid. The rowid is observed in the `rowid` slot of the result row
    /// and remembered for `InsertNew`.
    NewRowid(usize),
    Insert {
        cur: usize,
        rowid: i64,
        value: i64,
        require_seek: bool,
    },
    /// Inserts at the rowid that the last `NewRowid` on this cursor returned. This is the
    /// only way to write the "no seek after NewRowid" pattern, because the rowid is not
    /// known when the test is written.
    InsertNew {
        cur: usize,
        value: i64,
        require_seek: bool,
    },
    Delete(usize),
    /// Reads column `col` (0 is `id`, 1 is `v`) into the `col` slot of the result row.
    Column(usize, usize),
    /// Reads the rowid into the `rowid` slot of the result row.
    RowId(usize),
    NullRow(usize),
    /// The count is observed in the `rowid` slot of the result row.
    Count(usize),
}

/// What one op returned. `jumped` is true when the jump target of the op was taken
/// (for `Next` and `Prev` this means "a row was found").
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Obs {
    pub jumped: bool,
    pub rowid: Option<i64>,
    pub col: Option<i64>,
}

/// How a column read is emitted. SPEC 2b says that all modes read the same row, so a test
/// may run the same program with each mode and compare the observations.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ReadMode {
    /// `Column`.
    Column,
    /// `ColumnRange` with one column.
    ColumnRange,
    /// `ColumnHasField` guards a `Column`: the read happens only if the record has the field.
    ColumnHasField,
    /// `RowData` reads the whole record. The harness decodes the blob. `RowData` returns an
    /// error on a cursor without a record, where `Column` gives NULL, so a `ColumnHasField`
    /// guard skips the read there. `RowDataRaw` has no guard and is not in `ALL`.
    RowData,
    /// `RowData` without the guard.
    RowDataRaw,
}

impl ReadMode {
    pub const ALL: [ReadMode; 4] = [
        ReadMode::Column,
        ReadMode::ColumnRange,
        ReadMode::ColumnHasField,
        ReadMode::RowData,
    ];
}

/// Where the cursors of a program live.
pub enum Target<'a> {
    /// The cursors are open on the b-tree of the table in the database schema.
    Persistent,
    /// The cursors are open on an ephemeral b-tree: no MVCC and no schema. The program
    /// writes the setup rows itself, before the first op.
    Ephemeral(&'a [(i64, i64)]),
}

/// Emits a read of column `column` of the current row of `cursor` into `dest` (or into the
/// `raw` register, for `RowData`: the caller decodes the blob with `decode_record`).
pub fn emit_column_read(
    b: &mut ProgramBuilder,
    cursor: usize,
    column: usize,
    dest: usize,
    raw: usize,
    mode: ReadMode,
) {
    match mode {
        ReadMode::Column => {
            b.emit_insn(Insn::Column {
                cursor_id: cursor,
                column,
                dest,
                default: None,
            });
        }
        ReadMode::ColumnRange => {
            b.emit_insn(Insn::ColumnRange {
                cursor_id: cursor,
                start_column: column,
                dest,
                defaults: vec![None],
            });
        }
        ReadMode::ColumnHasField => {
            let has_field = b.allocate_label();
            let done = b.allocate_label();
            b.emit_insn(Insn::ColumnHasField {
                cursor_id: cursor,
                column,
                target_pc: has_field,
            });
            // No field: the slot keeps the NULL that the op started with.
            b.emit_insn(Insn::Goto { target_pc: done });
            b.preassign_label_to_next_insn(has_field);
            b.emit_insn(Insn::Column {
                cursor_id: cursor,
                column,
                dest,
                default: None,
            });
            b.preassign_label_to_next_insn(done);
        }
        ReadMode::RowData => {
            let has_field = b.allocate_label();
            let done = b.allocate_label();
            b.emit_insn(Insn::ColumnHasField {
                cursor_id: cursor,
                column,
                target_pc: has_field,
            });
            b.emit_insn(Insn::Goto { target_pc: done });
            b.preassign_label_to_next_insn(has_field);
            b.emit_insn(Insn::RowData {
                cursor_id: cursor,
                dest: raw,
            });
            b.preassign_label_to_next_insn(done);
        }
        ReadMode::RowDataRaw => {
            b.emit_insn(Insn::RowData {
                cursor_id: cursor,
                dest: raw,
            });
        }
    }
}

/// Decodes a record that holds only NULL and integers. `RowData` gives such a blob.
pub fn decode_record(raw: &Value) -> Result<Vec<Option<i64>>, String> {
    let bytes = match raw {
        // `RowData` on a cursor without a row gives NULL, like `Column`.
        Value::Null => return Ok(Vec::new()),
        Value::Blob(bytes) => bytes.to_vec(),
        other => return Err(format!("RowData gave {other:?}")),
    };
    fn varint(bytes: &[u8], pos: &mut usize) -> u64 {
        let mut value = 0u64;
        for i in 0..9 {
            let byte = bytes[*pos];
            *pos += 1;
            if i == 8 {
                return (value << 8) | byte as u64;
            }
            value = (value << 7) | (byte & 0x7f) as u64;
            if byte & 0x80 == 0 {
                break;
            }
        }
        value
    }
    let mut pos = 0;
    let header_len = varint(&bytes, &mut pos) as usize;
    let mut types = Vec::new();
    while pos < header_len {
        types.push(varint(&bytes, &mut pos));
    }
    let mut fields = Vec::new();
    for ty in types {
        let size = match ty {
            0 | 8 | 9 => 0,
            1 => 1,
            2 => 2,
            3 => 3,
            4 => 4,
            5 => 6,
            6 => 8,
            other => return Err(format!("serial type {other} is not an integer")),
        };
        fields.push(match ty {
            0 => None,
            8 => Some(0),
            9 => Some(1),
            _ => {
                // Big endian, two's complement.
                let slice = &bytes[pos..pos + size];
                let mut value = if slice[0] & 0x80 != 0 { -1i64 } else { 0 };
                for byte in slice {
                    value = (value << 8) | *byte as i64;
                }
                Some(value)
            }
        });
        pos += size;
    }
    Ok(fields)
}

/// Registers that are shared by all ops. `flag`, `rowid`, `col` and `raw` are next to each
/// other, because one `ResultRow` returns them.
struct Regs {
    flag: usize,
    rowid: usize,
    col: usize,
    /// The record that `RowData` read. NULL for every other read.
    raw: usize,
    /// Argument of the op: the key of a seek, or the rowid of an insert.
    arg: usize,
    /// Two registers `(id, v)` that `MakeRecord` packs into the row.
    record_fields: usize,
    record: usize,
    /// The rowid of the last `NewRowid`, one register per cursor.
    new_rowid: Vec<usize>,
}

pub fn compile(
    conn: &Arc<Connection>,
    root_page: i64,
    target: &Target,
    ops: &[Op],
    ncursors: usize,
    mode: ReadMode,
) -> Result<Program, String> {
    let table = conn
        .with_schema_mut(|s| s.get_btree_table(NamespaceId::ROOT, "t").unwrap())
        .map_err(|e| e.to_string())?;
    let mut b = ProgramBuilder::new(
        QueryMode::Normal,
        None,
        ProgramBuilderOpts::new(ncursors, 16 * ops.len() + 16, 2 * ops.len() + 2),
    );
    b.prologue();
    b.begin_write_operation().map_err(|e| e.to_string())?;
    let cursors: Vec<usize> = (0..ncursors)
        .map(|i| {
            let cursor = b.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
            match target {
                Target::Persistent => {
                    b.emit_insn(Insn::OpenWrite {
                        cursor_id: cursor,
                        root_page: RegisterOrLiteral::Literal(root_page),
                        db: 0,
                    });
                }
                // The first cursor makes the b-tree. The others are peers on the same tree.
                Target::Ephemeral(_) if i == 0 => {
                    b.emit_insn(Insn::OpenEphemeral {
                        cursor_id: cursor,
                        is_table: true,
                    });
                }
                Target::Ephemeral(_) => {
                    b.emit_insn(Insn::OpenDup {
                        new_cursor_id: cursor,
                        original_cursor_id: 0,
                    });
                }
            }
            cursor
        })
        .collect();
    let flag = b.alloc_registers(4);
    let regs = Regs {
        flag,
        rowid: flag + 1,
        col: flag + 2,
        raw: flag + 3,
        arg: b.alloc_register(),
        record_fields: b.alloc_registers(2),
        record: b.alloc_register(),
        new_rowid: (0..ncursors).map(|_| b.alloc_register()).collect(),
    };
    if let Target::Ephemeral(rows) = target {
        // An ephemeral table has no SQL, so the program writes the setup rows.
        for (id, value) in *rows {
            load_int(&mut b, regs.arg, *id);
            emit_insert(&mut b, cursors[0], &regs, regs.arg, *value, true);
        }
    }
    for op in ops {
        emit_op(&mut b, &cursors, &regs, mode, *op);
        // Every op gives one row, so that the observation list has the same length as `ops`.
        b.emit_result_row(regs.flag, 4);
    }
    conn.with_schema_mut(|schema| b.epilogue(schema))
        .map_err(|e| e.to_string())?;
    b.build(conn, false, "cursor contract test")
        .map_err(|e| e.to_string())
}

fn emit_op(b: &mut ProgramBuilder, cursors: &[usize], regs: &Regs, mode: ReadMode, op: Op) {
    // Start every op with empty observation slots. An op that reads nothing gives NULL.
    b.emit_insn(Insn::Null {
        dest: regs.rowid,
        dest_end: Some(regs.raw),
    });
    match op {
        Op::Rewind(c) => emit_jump(b, regs, |target_pc| Insn::Rewind {
            cursor_id: cursors[c],
            pc_if_empty: target_pc,
        }),
        Op::Last(c) => emit_jump(b, regs, |target_pc| Insn::Last {
            cursor_id: cursors[c],
            pc_if_empty: target_pc,
        }),
        Op::Next(c) => emit_jump(b, regs, |target_pc| Insn::Next {
            cursor_id: cursors[c],
            pc_if_next: target_pc,
            fullscan: false,
            is_index: false,
        }),
        Op::Prev(c) => emit_jump(b, regs, |target_pc| Insn::Prev {
            cursor_id: cursors[c],
            pc_if_prev: target_pc,
            fullscan: false,
            is_index: false,
        }),
        Op::SeekRowid(c, key) => {
            load_int(b, regs.arg, key);
            emit_jump(b, regs, |target_pc| Insn::SeekRowid {
                cursor_id: cursors[c],
                src_reg: regs.arg,
                target_pc,
            })
        }
        Op::NotExists(c, key) => {
            load_int(b, regs.arg, key);
            emit_jump(b, regs, |target_pc| Insn::NotExists {
                cursor: cursors[c],
                rowid_reg: regs.arg,
                target_pc,
            })
        }
        Op::SeekGE(c, key) => {
            load_int(b, regs.arg, key);
            emit_jump(b, regs, |target_pc| Insn::SeekGE {
                is_index: false,
                cursor_id: cursors[c],
                start_reg: regs.arg,
                num_regs: 1,
                target_pc,
                eq_only: false,
                null_matching_mask: Default::default(),
            })
        }
        Op::SeekGT(c, key) => {
            load_int(b, regs.arg, key);
            emit_jump(b, regs, |target_pc| Insn::SeekGT {
                is_index: false,
                cursor_id: cursors[c],
                start_reg: regs.arg,
                num_regs: 1,
                target_pc,
            })
        }
        Op::SeekLE(c, key) => {
            load_int(b, regs.arg, key);
            emit_jump(b, regs, |target_pc| Insn::SeekLE {
                is_index: false,
                cursor_id: cursors[c],
                start_reg: regs.arg,
                num_regs: 1,
                target_pc,
                eq_only: false,
                null_matching_mask: Default::default(),
            })
        }
        Op::SeekLT(c, key) => {
            load_int(b, regs.arg, key);
            emit_jump(b, regs, |target_pc| Insn::SeekLT {
                is_index: false,
                cursor_id: cursors[c],
                start_reg: regs.arg,
                num_regs: 1,
                target_pc,
            })
        }
        Op::NewRowid(c) => {
            load_int(b, regs.flag, 0);
            b.emit_insn(Insn::NewRowid {
                cursor: cursors[c],
                rowid_reg: regs.new_rowid[c],
                prev_largest_reg: 0,
            });
            b.emit_insn(Insn::Copy {
                src_reg: regs.new_rowid[c],
                dst_reg: regs.rowid,
                extra_amount: 0,
            });
        }
        Op::Insert {
            cur,
            rowid,
            value,
            require_seek,
        } => {
            load_int(b, regs.flag, 0);
            load_int(b, regs.arg, rowid);
            emit_insert(b, cursors[cur], regs, regs.arg, value, require_seek);
        }
        Op::InsertNew {
            cur,
            value,
            require_seek,
        } => {
            load_int(b, regs.flag, 0);
            emit_insert(
                b,
                cursors[cur],
                regs,
                regs.new_rowid[cur],
                value,
                require_seek,
            );
        }
        Op::Delete(c) => {
            load_int(b, regs.flag, 0);
            b.emit_insn(Insn::Delete {
                cursor_id: cursors[c],
                table_name: "t".to_string(),
                is_part_of_update: false,
            });
        }
        // Reading is not guarded, so that a read of an invalid cursor shows up as NULL.
        Op::Column(c, col) => {
            load_int(b, regs.flag, 0);
            // The `id` column of a rowid alias is stored as NULL in the record, so the
            // record modes cannot read it. `Column` reads the rowid for it.
            let mode = if col == 0 { ReadMode::Column } else { mode };
            emit_column_read(b, cursors[c], col, regs.col, regs.raw, mode);
        }
        Op::RowId(c) => {
            load_int(b, regs.flag, 0);
            b.emit_insn(Insn::RowId {
                cursor_id: cursors[c],
                dest: regs.rowid,
            });
        }
        Op::NullRow(c) => {
            load_int(b, regs.flag, 0);
            b.emit_insn(Insn::NullRow {
                cursor_id: cursors[c],
            });
        }
        Op::Count(c) => {
            load_int(b, regs.flag, 0);
            b.emit_insn(Insn::Count {
                cursor_id: cursors[c],
                target_reg: regs.rowid,
                exact: true,
            });
        }
    }
}

/// Emits an op that can jump. The jump goes to the instruction after the op, so both
/// paths continue in the same place. `flag` is 1 before the op and the fall-through path
/// sets it to 0, so after the op the flag tells which path ran.
fn emit_jump(
    b: &mut ProgramBuilder,
    regs: &Regs,
    op: impl FnOnce(turso_core::vdbe::BranchOffset) -> Insn,
) {
    let after = b.allocate_label();
    load_int(b, regs.flag, 1);
    b.emit_insn(op(after));
    load_int(b, regs.flag, 0);
    b.preassign_label_to_next_insn(after);
}

fn emit_insert(
    b: &mut ProgramBuilder,
    cursor: usize,
    regs: &Regs,
    key_reg: usize,
    value: i64,
    require_seek: bool,
) {
    // The record has two fields. The `id` field of a rowid alias column is stored as NULL.
    b.emit_insn(Insn::Null {
        dest: regs.record_fields,
        dest_end: None,
    });
    load_int(b, regs.record_fields + 1, value);
    b.emit_insn(Insn::MakeRecord {
        start_reg: regs.record_fields as u32,
        count: 2,
        dest_reg: regs.record as u32,
        index_name: None,
        affinity_str: None,
    });
    let flag = if require_seek {
        InsertFlags::new().require_seek()
    } else {
        InsertFlags::new()
    };
    b.emit_insn(Insn::Insert {
        cursor,
        key_reg,
        record_reg: regs.record,
        flag,
        table_name: "t".to_string(),
    });
}

fn load_int(b: &mut ProgramBuilder, dest: usize, value: i64) {
    b.emit_insn(Insn::Integer { value, dest });
}
