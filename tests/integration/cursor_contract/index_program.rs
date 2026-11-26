//! Index cursor programs (SPEC 2b). Like `program.rs`, an op list is compiled into one
//! straight-line VDBE program and every op gives one result row.
//!
//! The index is `i_ab ON ti(a, b)` of the table `ti(id INTEGER PRIMARY KEY, a, b)`. The columns
//! have no type, so a value keeps its type. The index has the sort order and the collation of
//! an `IdxDef` (SPEC 2c). An index key is `(a, b, rowid)`. The ops write the index b-tree directly, so
//! the table may be out of step with the index after a program. Do not run `integrity_check`.

use super::index_model::{ColDef, Coll, IdxDef, Order, Val};
use super::program::{emit_column_read, ReadMode};
use super::{catch_panic, open_database, step_rows, DbKind, Layout};
use std::sync::Arc;
use turso_core::ast::SortOrder;
use turso_core::namespace::NamespaceId;
use turso_core::schema::{Index, IndexColumn};
use turso_core::vdbe::builder::{CursorType, ProgramBuilder, ProgramBuilderOpts, QueryMode};
use turso_core::vdbe::insn::{
    ClearBtreeCount, IdxInsertFlags, InsertFlags, Insn, RegisterOrLiteral,
};
use turso_core::vdbe::Program;
use turso_core::CollationSeq;
use turso_core::{Connection, Value};

thread_local! {
    static SEEK_GE_EQ_ONLY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Runs `f` with every `IOp::SeekGE` emitted as `SeekGE { eq_only: true }`. This is the form
/// that the translator emits in front of an `IdxGT` loop. It is a flag on the thread and not
/// an `IOp`, so that the programs of the other tests do not change.
pub fn with_eq_only_seek_ge<T>(f: impl FnOnce() -> T) -> T {
    SEEK_GE_EQ_ONLY.with(|flag| flag.set(true));
    let out = f();
    SEEK_GE_EQ_ONLY.with(|flag| flag.set(false));
    out
}

/// A prefix of an index key: one or two values, NULL allowed.
pub type Prefix = Vec<Val>;

/// A setup row `(id, a, b)` of the table `ti`. SQL puts it into the table and the index.
pub type Row = (i64, Val, Val);

/// One op. `cur` is the number of an index cursor, `tbl` the number of a table cursor.
/// All index cursors are open on `i_ab`, all table cursors on `ti`.
#[derive(Debug, Clone, PartialEq)]
pub enum IOp {
    Rewind(usize),
    Last(usize),
    Next(usize),
    Prev(usize),
    SeekGE(usize, Prefix),
    SeekGT(usize, Prefix),
    SeekLE(usize, Prefix),
    SeekLT(usize, Prefix),
    Found(usize, Prefix),
    NotFound(usize, Prefix),
    NoConflict(usize, Prefix),
    IdxGE(usize, Prefix),
    IdxGT(usize, Prefix),
    IdxLE(usize, Prefix),
    IdxLT(usize, Prefix),
    /// `use_seek` tells the engine to trust the position of the last seek.
    IdxInsert {
        cur: usize,
        a: Val,
        b: Val,
        rowid: i64,
        use_seek: bool,
    },
    /// The key must be in the index.
    IdxDelete {
        cur: usize,
        a: Val,
        b: Val,
        rowid: i64,
    },
    Delete(usize),
    /// Reads column `i` (0 is `a`, 1 is `b`) into the slot `c0` or `c1` of the result row.
    Column(usize, usize),
    /// Reads the rowid of the entry into the `rowid` slot.
    IdxRowId(usize),
    SeekEnd(usize),
    NullRow(usize),
    /// The count is observed in the `rowid` slot.
    Count(usize),
    /// `ClearBtree` on the root page of the index. It is a WAL feature: MVCC returns an error.
    Clear(usize),
    /// Reads `IdxRowId`, `Column 0` and `Column 1` of the cursor in one op.
    Observe(usize),
    /// Table cursor `tbl` seeks (later, on the first read) to the row of the current entry
    /// of index cursor `idx`.
    DeferredSeek {
        idx: usize,
        tbl: usize,
    },
    /// Reads column `col` of the table row: 1 is `a` and goes to `c0`, 2 is `b` and goes to
    /// `c1`, 0 is the rowid and goes to the `rowid` slot.
    TblColumn(usize, usize),
    /// Completes a pending `DeferredSeek` of the table cursor.
    FinishSeek(usize),
}

/// What one op returned. `jumped` is true when the jump target of the op was taken (for
/// `Next` and `Prev` this means "a row was found").
#[derive(Debug, Clone, PartialEq)]
pub struct IObs {
    pub jumped: bool,
    pub rowid: Option<i64>,
    pub c0: Val,
    pub c1: Val,
}

/// The result of a run. If the engine fails in the middle of the program (as MVCC does on
/// `Clear`), `error` is set and `obs` holds the observations before the failing op.
#[derive(Debug, Clone, PartialEq)]
pub struct Outcome {
    pub obs: Vec<IObs>,
    pub error: Option<String>,
}

/// Everything that one run needs.
#[derive(Debug, Clone)]
pub struct IndexRun<'a> {
    pub kind: DbKind,
    pub layout: Layout,
    pub rows: &'a [Row],
    /// Sort order and collation of the index columns.
    pub idef: IdxDef,
    /// The number of index cursors.
    pub nidx: usize,
    /// The number of table cursors (only `DeferredSeek` needs them).
    pub ntbl: usize,
    pub mode: ReadMode,
}

/// Runs `ops` in one write transaction. A panic is returned as an `Outcome` with an error
/// that starts with `panic:`.
pub fn run_index(cfg: &IndexRun, ops: &[IOp]) -> Outcome {
    match catch_panic(|| run_index_inner(cfg, ops)) {
        Ok(outcome) => outcome,
        Err(error) => Outcome {
            obs: Vec::new(),
            error: Some(error),
        },
    }
}

fn run_index_inner(cfg: &IndexRun, ops: &[IOp]) -> Result<Outcome, String> {
    let fill: Vec<String> = cfg
        .rows
        .iter()
        .map(|(id, a, b)| format!("INSERT INTO ti VALUES ({id}, {}, {})", sql(a), sql(b)))
        .collect();
    let (db, conn) = open_database(
        cfg.kind,
        cfg.layout,
        &[
            "CREATE TABLE ti(id INTEGER PRIMARY KEY, a, b)",
            &create_index_sql(cfg.idef),
        ],
        &fill,
    )?;
    let program = compile_index(&conn, cfg, ops)?;
    let (rows, error) = step_rows(program, &db, &conn)?;
    let obs = rows
        .iter()
        .zip(ops)
        .map(|(row, op)| to_iobs(row, op))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Outcome { obs, error })
}

/// `CREATE INDEX i_ab ON ti(a [COLLATE X] [DESC], b [COLLATE X] [DESC])`.
fn create_index_sql(idef: IdxDef) -> String {
    let column = |name: &str, def: ColDef| {
        let coll = match def.coll {
            Coll::Binary => "",
            Coll::NoCase => " COLLATE NOCASE",
            Coll::RTrim => " COLLATE RTRIM",
        };
        let order = match def.order {
            Order::Asc => "",
            Order::Desc => " DESC",
        };
        format!("{name}{coll}{order}")
    };
    format!(
        "CREATE INDEX i_ab ON ti({}, {})",
        column("a", idef.cols[0]),
        column("b", idef.cols[1])
    )
}

fn sql(value: &Val) -> String {
    match value {
        Val::Null => "NULL".to_string(),
        Val::Int(v) => v.to_string(),
        Val::Text(t) => format!("'{}'", t.replace('\'', "''")),
    }
}

/// Registers that are shared by all ops. `flag`, `rowid`, `c0`, `c1` and `raw` are next to
/// each other, because one `ResultRow` returns them.
struct Regs {
    flag: usize,
    rowid: usize,
    c0: usize,
    c1: usize,
    /// The record that `RowData` read. NULL for every other read.
    raw: usize,
    /// Registers for a prefix (at most 2 values).
    prefix: usize,
    /// Registers for a full key `(a, b, rowid)`.
    key: usize,
    record: usize,
}

const RESULT_REGS: usize = 5;

fn compile_index(conn: &Arc<Connection>, cfg: &IndexRun, ops: &[IOp]) -> Result<Program, String> {
    let (table, index) = conn
        .with_schema_mut(|s| {
            (
                s.get_btree_table(NamespaceId::ROOT, "ti").unwrap(),
                s.get_index(NamespaceId::ROOT, "ti", "i_ab")
                    .unwrap()
                    .clone(),
            )
        })
        .map_err(|e| e.to_string())?;
    let ephemeral = cfg.kind == DbKind::Ephemeral;
    let index = if ephemeral {
        Arc::new(ephemeral_index(cfg.idef))
    } else {
        index
    };
    let mut b = ProgramBuilder::new(
        QueryMode::Normal,
        None,
        ProgramBuilderOpts::new(cfg.nidx + cfg.ntbl, 16 * ops.len() + 64, 2 * ops.len() + 2),
    );
    b.prologue();
    b.begin_write_operation().map_err(|e| e.to_string())?;
    let idx_root = index.root_page;
    let mut idx_cursors: Vec<usize> = Vec::new();
    for _ in 0..cfg.nidx {
        let cursor = b.alloc_cursor_id(CursorType::BTreeIndex(index.clone()));
        if !ephemeral {
            b.emit_insn(Insn::OpenWrite {
                cursor_id: cursor,
                root_page: RegisterOrLiteral::Literal(idx_root),
                db: 0,
            });
        } else if idx_cursors.is_empty() {
            // The first cursor makes the b-tree. The others are peers on the same tree.
            b.emit_insn(Insn::OpenAutoindex { cursor_id: cursor });
        } else {
            b.emit_insn(Insn::OpenDup {
                new_cursor_id: cursor,
                original_cursor_id: idx_cursors[0],
            });
        }
        idx_cursors.push(cursor);
    }
    let tbl_root = table.root_page;
    let mut tbl_cursors: Vec<usize> = Vec::new();
    for _ in 0..cfg.ntbl {
        let cursor = b.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
        if !ephemeral {
            b.emit_insn(Insn::OpenWrite {
                cursor_id: cursor,
                root_page: RegisterOrLiteral::Literal(tbl_root),
                db: 0,
            });
        } else if tbl_cursors.is_empty() {
            b.emit_insn(Insn::OpenEphemeral {
                cursor_id: cursor,
                is_table: true,
            });
        } else {
            b.emit_insn(Insn::OpenDup {
                new_cursor_id: cursor,
                original_cursor_id: tbl_cursors[0],
            });
        }
        tbl_cursors.push(cursor);
    }
    let flag = b.alloc_registers(RESULT_REGS);
    let regs = Regs {
        flag,
        rowid: flag + 1,
        c0: flag + 2,
        c1: flag + 3,
        raw: flag + 4,
        prefix: b.alloc_registers(2),
        key: b.alloc_registers(3),
        record: b.alloc_register(),
    };
    let cx = Cx {
        idx: &idx_cursors,
        tbl: &tbl_cursors,
        mode: cfg.mode,
        idx_root,
    };
    if ephemeral {
        // An ephemeral b-tree has no SQL, so the program writes the setup rows.
        for (id, a, v) in cfg.rows {
            if !tbl_cursors.is_empty() {
                emit_table_row(&mut b, &cx, &regs, *id, a, v);
            }
            if !idx_cursors.is_empty() {
                emit_idx_insert(&mut b, idx_cursors[0], &regs, a, v, *id, false);
            }
        }
    }
    for op in ops {
        emit_op(&mut b, &cx, &regs, op)?;
        // Every op gives one row, so that the observation list has the same length as `ops`.
        b.emit_result_row(regs.flag, RESULT_REGS);
    }
    conn.with_schema_mut(|schema| b.epilogue(schema))
        .map_err(|e| e.to_string())?;
    b.build(conn, false, "index cursor contract test")
        .map_err(|e| e.to_string())
}

/// The index of an ephemeral run: `(a, b, rowid)` and no schema.
fn ephemeral_index(idef: IdxDef) -> Index {
    let column = |name: &str, pos: usize, def: ColDef| {
        let collation = match def.coll {
            Coll::Binary => CollationSeq::BINARY,
            Coll::NoCase => CollationSeq::NOCASE,
            Coll::RTrim => CollationSeq::RTRIM,
        };
        let mut col = IndexColumn::new(name, pos, collation);
        col.order = match def.order {
            Order::Asc => SortOrder::Asc,
            Order::Desc => SortOrder::Desc,
        };
        col
    };
    Index {
        name: "ephemeral_ab".to_string(),
        table_name: String::new(),
        root_page: 0,
        columns: vec![column("a", 0, idef.cols[0]), column("b", 1, idef.cols[1])],
        unique: false,
        ephemeral: true,
        has_rowid: true,
        where_clause: None,
        index_method: None,
        on_conflict: None,
        constraint_name: None,
    }
}

/// Cursors of one program.
struct Cx<'a> {
    idx: &'a [usize],
    tbl: &'a [usize],
    mode: ReadMode,
    idx_root: i64,
}

fn emit_op(b: &mut ProgramBuilder, cx: &Cx, regs: &Regs, op: &IOp) -> Result<(), String> {
    // Start every op with empty observation slots. An op that reads nothing gives NULL.
    b.emit_insn(Insn::Null {
        dest: regs.rowid,
        dest_end: Some(regs.raw),
    });
    let idx = |c: &usize| cx.idx[*c];
    match op {
        IOp::Rewind(c) => emit_jump(b, regs, |target_pc| Insn::Rewind {
            cursor_id: idx(c),
            pc_if_empty: target_pc,
        }),
        IOp::Last(c) => emit_jump(b, regs, |target_pc| Insn::Last {
            cursor_id: idx(c),
            pc_if_empty: target_pc,
        }),
        IOp::Next(c) => emit_jump(b, regs, |target_pc| Insn::Next {
            cursor_id: idx(c),
            pc_if_next: target_pc,
            fullscan: false,
            is_index: true,
        }),
        IOp::Prev(c) => emit_jump(b, regs, |target_pc| Insn::Prev {
            cursor_id: idx(c),
            pc_if_prev: target_pc,
            fullscan: false,
            is_index: true,
        }),
        IOp::SeekGE(c, p) => {
            load_prefix(b, regs, p);
            emit_jump(b, regs, |target_pc| Insn::SeekGE {
                is_index: true,
                cursor_id: idx(c),
                start_reg: regs.prefix,
                num_regs: p.len(),
                target_pc,
                eq_only: SEEK_GE_EQ_ONLY.with(|f| f.get()),
                null_matching_mask: Default::default(),
            })
        }
        IOp::SeekGT(c, p) => {
            load_prefix(b, regs, p);
            emit_jump(b, regs, |target_pc| Insn::SeekGT {
                is_index: true,
                cursor_id: idx(c),
                start_reg: regs.prefix,
                num_regs: p.len(),
                target_pc,
            })
        }
        IOp::SeekLE(c, p) => {
            load_prefix(b, regs, p);
            emit_jump(b, regs, |target_pc| Insn::SeekLE {
                is_index: true,
                cursor_id: idx(c),
                start_reg: regs.prefix,
                num_regs: p.len(),
                target_pc,
                eq_only: false,
                null_matching_mask: Default::default(),
            })
        }
        IOp::SeekLT(c, p) => {
            load_prefix(b, regs, p);
            emit_jump(b, regs, |target_pc| Insn::SeekLT {
                is_index: true,
                cursor_id: idx(c),
                start_reg: regs.prefix,
                num_regs: p.len(),
                target_pc,
            })
        }
        IOp::Found(c, p) => {
            load_prefix(b, regs, p);
            emit_jump(b, regs, |target_pc| Insn::Found {
                cursor_id: idx(c),
                target_pc,
                record_reg: regs.prefix,
                num_regs: p.len(),
            })
        }
        IOp::NotFound(c, p) => {
            load_prefix(b, regs, p);
            emit_jump(b, regs, |target_pc| Insn::NotFound {
                cursor_id: idx(c),
                target_pc,
                record_reg: regs.prefix,
                num_regs: p.len(),
            })
        }
        IOp::NoConflict(c, p) => {
            load_prefix(b, regs, p);
            emit_jump(b, regs, |target_pc| Insn::NoConflict {
                cursor_id: idx(c),
                target_pc,
                record_reg: regs.prefix,
                num_regs: p.len(),
            })
        }
        IOp::IdxGE(c, p) => {
            load_prefix(b, regs, p);
            emit_jump(b, regs, |target_pc| Insn::IdxGE {
                cursor_id: idx(c),
                start_reg: regs.prefix,
                num_regs: p.len(),
                target_pc,
            })
        }
        IOp::IdxGT(c, p) => {
            load_prefix(b, regs, p);
            emit_jump(b, regs, |target_pc| Insn::IdxGT {
                cursor_id: idx(c),
                start_reg: regs.prefix,
                num_regs: p.len(),
                target_pc,
            })
        }
        IOp::IdxLE(c, p) => {
            load_prefix(b, regs, p);
            emit_jump(b, regs, |target_pc| Insn::IdxLE {
                cursor_id: idx(c),
                start_reg: regs.prefix,
                num_regs: p.len(),
                target_pc,
            })
        }
        IOp::IdxLT(c, p) => {
            load_prefix(b, regs, p);
            emit_jump(b, regs, |target_pc| Insn::IdxLT {
                cursor_id: idx(c),
                start_reg: regs.prefix,
                num_regs: p.len(),
                target_pc,
            })
        }
        IOp::IdxInsert {
            cur,
            a,
            b: bb,
            rowid,
            use_seek,
        } => {
            load_int(b, regs.flag, 0);
            emit_idx_insert(b, idx(cur), regs, a, bb, *rowid, *use_seek);
        }
        IOp::IdxDelete {
            cur,
            a,
            b: bb,
            rowid,
        } => {
            load_int(b, regs.flag, 0);
            load_key(b, regs, a, bb, *rowid);
            b.emit_insn(Insn::IdxDelete {
                start_reg: regs.key,
                num_regs: 3,
                cursor_id: idx(cur),
                raise_error_if_no_matching_entry: true,
            });
        }
        IOp::Delete(c) => {
            load_int(b, regs.flag, 0);
            b.emit_insn(Insn::Delete {
                cursor_id: idx(c),
                table_name: "ti".to_string(),
                is_part_of_update: false,
            });
        }
        // Reading is not guarded, so that a read of an invalid cursor shows up as NULL.
        IOp::Column(c, i) => {
            load_int(b, regs.flag, 0);
            let dest = if *i == 0 { regs.c0 } else { regs.c1 };
            emit_column_read(b, idx(c), *i, dest, regs.raw, cx.mode);
        }
        IOp::IdxRowId(c) => {
            load_int(b, regs.flag, 0);
            b.emit_insn(Insn::IdxRowId {
                cursor_id: idx(c),
                dest: regs.rowid,
            });
        }
        IOp::Observe(c) => {
            load_int(b, regs.flag, 0);
            if matches!(cx.mode, ReadMode::RowData | ReadMode::RowDataRaw) {
                // One read gives the whole entry: both columns and the rowid.
                emit_column_read(b, idx(c), 0, regs.c0, regs.raw, cx.mode);
            } else {
                b.emit_insn(Insn::IdxRowId {
                    cursor_id: idx(c),
                    dest: regs.rowid,
                });
                emit_column_read(b, idx(c), 0, regs.c0, regs.raw, cx.mode);
                emit_column_read(b, idx(c), 1, regs.c1, regs.raw, cx.mode);
            }
        }
        IOp::SeekEnd(c) => {
            load_int(b, regs.flag, 0);
            b.emit_insn(Insn::SeekEnd { cursor_id: idx(c) });
        }
        IOp::NullRow(c) => {
            load_int(b, regs.flag, 0);
            b.emit_insn(Insn::NullRow { cursor_id: idx(c) });
        }
        IOp::Count(c) => {
            load_int(b, regs.flag, 0);
            b.emit_insn(Insn::Count {
                cursor_id: idx(c),
                target_reg: regs.rowid,
                exact: true,
            });
        }
        IOp::Clear(_) => {
            if cx.idx_root == 0 {
                return Err("Clear needs a persistent index".to_string());
            }
            load_int(b, regs.flag, 0);
            b.emit_insn(Insn::ClearBtree {
                db: 0,
                root: cx.idx_root,
                count: ClearBtreeCount::Nothing,
            });
        }
        IOp::DeferredSeek { idx: i, tbl } => {
            load_int(b, regs.flag, 0);
            b.emit_insn(Insn::DeferredSeek {
                index_cursor_id: cx.idx[*i],
                table_cursor_id: cx.tbl[*tbl],
            });
        }
        IOp::TblColumn(t, col) => {
            load_int(b, regs.flag, 0);
            match col {
                0 => {
                    b.emit_insn(Insn::RowId {
                        cursor_id: cx.tbl[*t],
                        dest: regs.rowid,
                    });
                }
                1 => emit_column_read(b, cx.tbl[*t], 1, regs.c0, regs.raw, cx.mode),
                2 => emit_column_read(b, cx.tbl[*t], 2, regs.c1, regs.raw, cx.mode),
                other => return Err(format!("table column {other} does not exist")),
            }
        }
        IOp::FinishSeek(t) => {
            load_int(b, regs.flag, 0);
            b.emit_insn(Insn::FinishSeek {
                cursor_id: cx.tbl[*t],
            });
        }
    }
    Ok(())
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

fn load_int(b: &mut ProgramBuilder, dest: usize, value: i64) {
    load_value(b, dest, &Val::Int(value));
}

fn load_value(b: &mut ProgramBuilder, dest: usize, value: &Val) {
    match value {
        Val::Int(value) => b.emit_insn(Insn::Integer {
            value: *value,
            dest,
        }),
        Val::Text(value) => b.emit_insn(Insn::String8 {
            value: value.clone(),
            dest,
        }),
        Val::Null => b.emit_insn(Insn::Null {
            dest,
            dest_end: None,
        }),
    };
}

fn load_prefix(b: &mut ProgramBuilder, regs: &Regs, prefix: &Prefix) {
    assert!(
        (1..=2).contains(&prefix.len()),
        "a prefix has 1 or 2 values"
    );
    for (i, value) in prefix.iter().enumerate() {
        load_value(b, regs.prefix + i, value);
    }
}

fn load_key(b: &mut ProgramBuilder, regs: &Regs, a: &Val, bb: &Val, rowid: i64) {
    load_value(b, regs.key, a);
    load_value(b, regs.key + 1, bb);
    load_int(b, regs.key + 2, rowid);
}

fn emit_idx_insert(
    b: &mut ProgramBuilder,
    cursor: usize,
    regs: &Regs,
    a: &Val,
    bb: &Val,
    rowid: i64,
    use_seek: bool,
) {
    load_key(b, regs, a, bb, rowid);
    b.emit_insn(Insn::MakeRecord {
        start_reg: regs.key as u32,
        count: 3,
        dest_reg: regs.record as u32,
        index_name: None,
        affinity_str: None,
    });
    b.emit_insn(Insn::IdxInsert {
        cursor_id: cursor,
        record_reg: regs.record,
        unpacked_start: Some(regs.key),
        unpacked_count: Some(3),
        flags: IdxInsertFlags::new().use_seek(use_seek),
    });
}

/// Writes the row `(NULL, a, b)` at `rowid` into the first table cursor.
fn emit_table_row(b: &mut ProgramBuilder, cx: &Cx, regs: &Regs, rowid: i64, a: &Val, bb: &Val) {
    // The `id` field of a rowid alias column is stored as NULL.
    load_value(b, regs.key, &Val::Null);
    load_value(b, regs.key + 1, a);
    load_value(b, regs.key + 2, bb);
    b.emit_insn(Insn::MakeRecord {
        start_reg: regs.key as u32,
        count: 3,
        dest_reg: regs.record as u32,
        index_name: None,
        affinity_str: None,
    });
    load_int(b, regs.prefix, rowid);
    b.emit_insn(Insn::Insert {
        cursor: cx.tbl[0],
        key_reg: regs.prefix,
        record_reg: regs.record,
        flag: InsertFlags::new().require_seek(),
        table_name: "ti".to_string(),
    });
}

/// Turns a result row into an observation. In `RowData` mode the column values come from
/// the record that the op read.
fn to_iobs(row: &[Value], op: &IOp) -> Result<IObs, String> {
    let int = |v: &Value| match v {
        Value::Null => Ok(None),
        Value::Numeric(_) => Ok(v.as_int()),
        other => Err(format!("unexpected value {other:?}")),
    };
    let val = |v: &Value| match v {
        Value::Null => Ok(Val::Null),
        Value::Numeric(_) => Ok(v.as_int().map_or(Val::Null, Val::Int)),
        Value::Text(t) => Ok(Val::text(t.as_str())),
        other => Err(format!("unexpected value {other:?}")),
    };
    let mut obs = IObs {
        jumped: int(&row[0])? == Some(1),
        rowid: int(&row[1])?,
        c0: val(&row[2])?,
        c1: val(&row[3])?,
    };
    if matches!(row[4], Value::Blob(_)) {
        let fields = decode_index_record(&row[4])?;
        let field = |i: usize| fields.get(i).cloned().unwrap_or(Val::Null);
        let rowid_field = |i: usize| match fields.get(i) {
            Some(Val::Int(v)) => Some(*v),
            _ => None,
        };
        match op {
            IOp::Column(_, 0) => obs.c0 = field(0),
            IOp::Column(_, i) => obs.c1 = field(*i),
            // The table record is `(NULL, a, b)`.
            IOp::TblColumn(_, 1) => obs.c0 = field(1),
            IOp::TblColumn(_, i) => obs.c1 = field(*i),
            // The index record is `(a, b, rowid)`.
            IOp::Observe(_) => {
                obs.c0 = field(0);
                obs.c1 = field(1);
                obs.rowid = rowid_field(2);
            }
            other => return Err(format!("{other:?} gave a record")),
        }
    }
    Ok(obs)
}

/// Decodes a record that holds NULL, integers and text. Like `decode_record` in `program.rs`,
/// but a text field is decoded too.
fn decode_index_record(raw: &Value) -> Result<Vec<Val>, String> {
    let bytes = match raw {
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
            // An odd serial type from 13 is text.
            t if t >= 13 && t % 2 == 1 => ((t - 13) / 2) as usize,
            other => return Err(format!("serial type {other} is not an integer or text")),
        };
        fields.push(match ty {
            0 => Val::Null,
            8 => Val::Int(0),
            9 => Val::Int(1),
            t if t >= 13 => {
                let text = std::str::from_utf8(&bytes[pos..pos + size])
                    .map_err(|e| format!("text field is not UTF-8: {e}"))?;
                Val::text(text)
            }
            _ => {
                // Big endian, two's complement.
                let slice = &bytes[pos..pos + size];
                let mut value = if slice[0] & 0x80 != 0 { -1i64 } else { 0 };
                for byte in slice {
                    value = (value << 8) | *byte as i64;
                }
                Val::Int(value)
            }
        });
        pos += size;
    }
    Ok(fields)
}
