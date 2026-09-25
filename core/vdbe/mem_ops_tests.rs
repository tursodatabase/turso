//! Tests for the linear-memory opcodes (MemLoad/MemStore/MemCopy), which treat a
//! blob register as byte-addressable memory. Programs are hand-assembled with
//! ProgramBuilder rather than compiled from SQL, since no SQL construct emits them.

use std::sync::Arc;

use crate::numeric::Numeric;
use crate::vdbe::builder::{ProgramBuilder, ProgramBuilderOpts, QueryMode};
use crate::vdbe::insn::{Insn, MemWidth};
use crate::{Database, MemoryIO, Statement, Value, IO, MAIN_DB_ID};

/// Hand-assemble a program (prologue + body + epilogue), run it on a fresh in-memory
/// database, and collect the result rows.
fn run_program(body: impl FnOnce(&mut ProgramBuilder)) -> crate::Result<Vec<Vec<Value>>> {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:")?;
    let conn = db.connect()?;
    let mut builder =
        ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 64, 8));
    builder.prologue();
    builder.begin_read_operation().unwrap();
    body(&mut builder);
    conn.with_schema(MAIN_DB_ID, |schema| builder.epilogue(schema));
    let program = builder.build(conn.clone(), false, "hand-assembled test program")?;
    let mut stmt = Statement::new(program, conn.get_pager(), QueryMode::Normal, 0);
    stmt.run_collect_rows()
}

fn int_rows(rows: &[Vec<Value>]) -> Vec<Vec<i64>> {
    rows.iter()
        .map(|row| {
            row.iter()
                .map(|v| match v {
                    Value::Numeric(Numeric::Integer(n)) => *n,
                    other => panic!("expected integer, got {other:?}"),
                })
                .collect()
        })
        .collect()
}

#[test]
fn mem_store_load_round_trips_all_widths() {
    let rows = run_program(|b| {
        let mem = b.alloc_register();
        let addr = b.alloc_register();
        let val = b.alloc_register();
        let out_start = b.alloc_registers(4);
        b.emit_insn(Insn::Blob {
            value: vec![0u8; 64],
            dest: mem,
        });
        // Store 0xAABBCCDD11223344 at offset 8, then read it back at each width.
        b.emit_insn(Insn::Integer {
            value: 8,
            dest: addr,
        });
        b.emit_insn(Insn::Integer {
            value: 0xAABBCCDD11223344u64 as i64,
            dest: val,
        });
        b.emit_insn(Insn::MemStore {
            mem,
            addr,
            width: MemWidth::W8,
            src: val,
            card_table: None,
            page_shift: 0,
        });
        for (i, width) in [MemWidth::W1, MemWidth::W2, MemWidth::W4, MemWidth::W8]
            .into_iter()
            .enumerate()
        {
            b.emit_insn(Insn::MemLoad {
                mem,
                addr,
                width,
                signed: false,
                dest: out_start + i,
            });
        }
        b.emit_result_row(out_start, 4);
    })
    .unwrap();
    assert_eq!(
        int_rows(&rows),
        vec![vec![0x44, 0x3344, 0x11223344, 0xAABBCCDD11223344u64 as i64]]
    );
}

#[test]
fn mem_load_sign_extends() {
    let rows = run_program(|b| {
        let mem = b.alloc_register();
        let addr = b.alloc_register();
        let val = b.alloc_register();
        let out_start = b.alloc_registers(2);
        b.emit_insn(Insn::Blob {
            value: vec![0u8; 16],
            dest: mem,
        });
        b.emit_insn(Insn::Integer {
            value: 0,
            dest: addr,
        });
        b.emit_insn(Insn::Integer {
            value: -2,
            dest: val,
        });
        b.emit_insn(Insn::MemStore {
            mem,
            addr,
            width: MemWidth::W2,
            src: val,
            card_table: None,
            page_shift: 0,
        });
        b.emit_insn(Insn::MemLoad {
            mem,
            addr,
            width: MemWidth::W2,
            signed: true,
            dest: out_start,
        });
        b.emit_insn(Insn::MemLoad {
            mem,
            addr,
            width: MemWidth::W2,
            signed: false,
            dest: out_start + 1,
        });
        b.emit_result_row(out_start, 2);
    })
    .unwrap();
    assert_eq!(int_rows(&rows), vec![vec![-2, 0xFFFE]]);
}

#[test]
fn mem_store_marks_card_table() {
    // 4-byte pages (page_shift 2) over a 32-byte memory: 8 pages, one card byte.
    let rows = run_program(|b| {
        let mem = b.alloc_register();
        let cards = b.alloc_register();
        let addr = b.alloc_register();
        let val = b.alloc_register();
        let zero = b.alloc_register();
        let out = b.alloc_register();
        b.emit_insn(Insn::Blob {
            value: vec![0u8; 32],
            dest: mem,
        });
        b.emit_insn(Insn::Blob {
            value: vec![0u8; 1],
            dest: cards,
        });
        b.emit_insn(Insn::Integer {
            value: 7,
            dest: val,
        });
        // Touch page 1 (offset 5) and pages 6..=7 (a 4-byte store spanning offset 22..26).
        b.emit_insn(Insn::Integer {
            value: 5,
            dest: addr,
        });
        b.emit_insn(Insn::MemStore {
            mem,
            addr,
            width: MemWidth::W1,
            src: val,
            card_table: Some(cards),
            page_shift: 2,
        });
        b.emit_insn(Insn::Integer {
            value: 22,
            dest: addr,
        });
        b.emit_insn(Insn::MemStore {
            mem,
            addr,
            width: MemWidth::W4,
            src: val,
            card_table: Some(cards),
            page_shift: 2,
        });
        // Read the card byte back out through MemLoad on the card blob itself.
        b.emit_insn(Insn::Integer {
            value: 0,
            dest: zero,
        });
        b.emit_insn(Insn::MemLoad {
            mem: cards,
            addr: zero,
            width: MemWidth::W1,
            signed: false,
            dest: out,
        });
        b.emit_result_row(out, 1);
    })
    .unwrap();
    // page 1 (bit 1), page 5 (bit 5, offset 22..24), page 6 (bit 6, offset 24..26).
    assert_eq!(int_rows(&rows), vec![vec![0b0110_0010]]);
}

#[test]
fn mem_copy_between_and_within_blobs() {
    let rows = run_program(|b| {
        let src = b.alloc_register();
        let dst = b.alloc_register();
        let src_addr = b.alloc_register();
        let dst_addr = b.alloc_register();
        let len = b.alloc_register();
        let out_start = b.alloc_registers(2);
        b.emit_insn(Insn::Blob {
            value: (0u8..16).collect(),
            dest: src,
        });
        b.emit_insn(Insn::Blob {
            value: vec![0u8; 16],
            dest: dst,
        });
        // dst[4..12] = src[0..8]
        b.emit_insn(Insn::Integer {
            value: 0,
            dest: src_addr,
        });
        b.emit_insn(Insn::Integer {
            value: 4,
            dest: dst_addr,
        });
        b.emit_insn(Insn::Integer {
            value: 8,
            dest: len,
        });
        b.emit_insn(Insn::MemCopy {
            dest_mem: dst,
            dest_addr: dst_addr,
            src_mem: src,
            src_addr,
            len,
            card_table: None,
            page_shift: 0,
        });
        // Overlapping move within dst: dst[6..14] = dst[4..12]
        b.emit_insn(Insn::Integer {
            value: 4,
            dest: src_addr,
        });
        b.emit_insn(Insn::Integer {
            value: 6,
            dest: dst_addr,
        });
        b.emit_insn(Insn::MemCopy {
            dest_mem: dst,
            dest_addr: dst_addr,
            src_mem: dst,
            src_addr,
            len,
            card_table: None,
            page_shift: 0,
        });
        // Read dst[6..14] as a little-endian u64: expect bytes 0,1,2,...,7.
        b.emit_insn(Insn::Integer {
            value: 6,
            dest: src_addr,
        });
        b.emit_insn(Insn::MemLoad {
            mem: dst,
            addr: src_addr,
            width: MemWidth::W8,
            signed: false,
            dest: out_start,
        });
        // And dst[4..6] survived the overlap start: bytes 0, 1.
        b.emit_insn(Insn::Integer {
            value: 4,
            dest: src_addr,
        });
        b.emit_insn(Insn::MemLoad {
            mem: dst,
            addr: src_addr,
            width: MemWidth::W2,
            signed: false,
            dest: out_start + 1,
        });
        b.emit_result_row(out_start, 2);
    })
    .unwrap();
    assert_eq!(int_rows(&rows), vec![vec![0x0706050403020100, 0x0100]]);
}

#[test]
fn mem_load_out_of_bounds_fails() {
    let result = run_program(|b| {
        let mem = b.alloc_register();
        let addr = b.alloc_register();
        let out = b.alloc_register();
        b.emit_insn(Insn::Blob {
            value: vec![0u8; 8],
            dest: mem,
        });
        b.emit_insn(Insn::Integer {
            value: 5,
            dest: addr,
        });
        b.emit_insn(Insn::MemLoad {
            mem,
            addr,
            width: MemWidth::W4,
            signed: false,
            dest: out,
        });
        b.emit_result_row(out, 1);
    });
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("out of bounds"),
        "unexpected error: {err}"
    );
}
