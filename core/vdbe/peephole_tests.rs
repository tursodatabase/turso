//! End-to-end tests for the peephole pass: prepare real SQL, compare the
//! EXPLAIN bytecode with the pass on and off, and check that results stay
//! identical. The on/off switch is the same escape hatch `TURSO_PEEPHOLE=0`
//! uses, applied through a thread-local override so both variants can be
//! prepared in one process.

use std::sync::Arc;

use crate::vdbe::builder::{ProgramBuilder, ProgramBuilderOpts, QueryMode};
use crate::vdbe::insn::{CmpInsFlags, Insn};
use crate::vdbe::peephole;
use crate::{Connection, Database, MemoryIO, SqliteDialect, Statement, Value, IO};

fn fresh_db() -> Arc<Connection> {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
    db.connect().unwrap()
}

fn exec(conn: &Arc<Connection>, sql: &str) {
    conn.execute(sql).unwrap();
}

fn rows(conn: &Arc<Connection>, sql: &str) -> Vec<Vec<Value>> {
    let mut stmt = conn.prepare(sql).unwrap();
    stmt.run_collect_rows().unwrap()
}

/// Run `f` with the peephole pass forced on or off for this thread.
fn with_pass<T>(enabled: bool, f: impl FnOnce() -> T) -> T {
    struct Restore;
    impl Drop for Restore {
        fn drop(&mut self) {
            peephole::set_enabled_for_current_thread(None);
        }
    }
    let _restore = Restore;
    peephole::set_enabled_for_current_thread(Some(enabled));
    f()
}

#[derive(Debug, Clone)]
struct ExplainRow {
    addr: i64,
    opcode: String,
    p2: i64,
    p3: i64,
}

fn explain(conn: &Arc<Connection>, sql: &str) -> Vec<ExplainRow> {
    rows(conn, &format!("EXPLAIN {sql}"))
        .into_iter()
        .map(|row| {
            let int = |v: &Value| -> i64 {
                match v {
                    Value::Numeric(crate::Numeric::Integer(i)) => *i,
                    other => panic!("expected integer, got {other:?}"),
                }
            };
            ExplainRow {
                addr: int(&row[0]),
                opcode: row[1].to_text().unwrap().to_string(),
                p2: int(&row[3]),
                p3: int(&row[4]),
            }
        })
        .collect()
}

fn opcode_count(program: &[ExplainRow], opcode: &str) -> usize {
    program.iter().filter(|r| r.opcode == opcode).count()
}

/// `Goto` whose target is the very next instruction.
fn has_goto_to_next(program: &[ExplainRow]) -> bool {
    program
        .iter()
        .any(|r| r.opcode == "Goto" && r.p2 == r.addr + 1)
}

#[test]
fn single_row_insert_epilogue_collapses_in_real_bytecode() {
    let conn = fresh_db();
    exec(
        &conn,
        "CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT, y INT)",
    );
    let sql = "INSERT INTO t(id, x, y) VALUES (1, 'a', 2)";
    let off = with_pass(false, || explain(&conn, sql));
    let on = with_pass(true, || explain(&conn, sql));

    // Unoptimized: the statement body ends `Insert; Goto +1; Goto +1; Halt`.
    let insert_off = off.iter().position(|r| r.opcode == "Insert").unwrap();
    assert_eq!(off[insert_off + 1].opcode, "Goto");
    assert_eq!(off[insert_off + 1].p2, off[insert_off + 1].addr + 1);
    assert_eq!(off[insert_off + 2].opcode, "Goto");
    assert_eq!(off[insert_off + 2].p2, off[insert_off + 2].addr + 1);
    assert_eq!(off[insert_off + 3].opcode, "Halt");

    // Optimized: the Insert is followed directly by the Halt.
    let insert_on = on.iter().position(|r| r.opcode == "Insert").unwrap();
    assert_eq!(on[insert_on + 1].opcode, "Halt");
    assert!(!has_goto_to_next(&on));
    assert!(on.len() < off.len());
}

#[test]
fn insert_with_explicit_rowid_inverts_not_null_over_goto() {
    let conn = fresh_db();
    exec(&conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT)");
    let sql = "INSERT INTO t(id, x) VALUES (5, 'a')";
    let off = with_pass(false, || explain(&conn, sql));
    let on = with_pass(true, || explain(&conn, sql));

    // Unoptimized: `NotNull -> +2; Goto` guards the explicit-rowid path.
    let not_null_off = off.iter().position(|r| r.opcode == "NotNull").unwrap();
    assert_eq!(off[not_null_off].p2, off[not_null_off].addr + 2);
    assert_eq!(off[not_null_off + 1].opcode, "Goto");

    // Optimized: the pair became one IsNull that jumps where the Goto went,
    // which is the NewRowid path for the id-is-NULL case.
    assert_eq!(opcode_count(&on, "NotNull"), 0);
    let is_null_on = on.iter().position(|r| r.opcode == "IsNull").unwrap();
    let is_null_target = on[is_null_on].p2;
    let target_row = on.iter().find(|r| r.addr == is_null_target).unwrap();
    assert_eq!(target_row.opcode, "NewRowid");

    // Both variants insert their row: one with the inverted bytecode, one
    // with the original.
    with_pass(true, || {
        exec(&conn, sql);
    });
    with_pass(false, || {
        exec(&conn, "INSERT INTO t(id, x) VALUES (6, 'b')");
    });
    let count = rows(&conn, "SELECT count(*) FROM t");
    assert!(
        matches!(count[0][0], Value::Numeric(crate::Numeric::Integer(2))),
        "expected both inserts to land, got {count:?}"
    );
}

#[test]
fn group_by_loses_dead_abort_subroutine_and_merges_copies() {
    let conn = fresh_db();
    exec(&conn, "CREATE TABLE g(a INT, b TEXT)");
    let sql = "SELECT b, count(*) FROM g GROUP BY b";
    let off = with_pass(false, || explain(&conn, sql));
    let on = with_pass(true, || explain(&conn, sql));

    // The abort subroutine (`Integer 1 -> abort flag; Return`) is emitted but
    // nothing Gosubs it: one Return disappears with it.
    assert_eq!(
        opcode_count(&on, "Return"),
        opcode_count(&off, "Return") - 1
    );

    // The two adjacent result Copies merge into one ranged Copy.
    assert_eq!(opcode_count(&on, "Copy"), opcode_count(&off, "Copy") - 1);
    let merged_copy = on.iter().find(|r| r.opcode == "Copy").unwrap();
    assert_eq!(merged_copy.p3, 1, "merged Copy must span two registers");

    assert!(on.len() < off.len());
}

#[test]
fn results_stay_identical_for_null_heavy_comparisons() {
    let queries = [
        "SELECT a = b FROM t ORDER BY 1",
        "SELECT a, b FROM t WHERE a = b",
        "SELECT a, b FROM t WHERE a <> b",
        "SELECT a, b FROM t WHERE a < b OR s = 'z'",
        "SELECT CASE WHEN a = b THEN 'eq' WHEN a < b THEN 'lt' ELSE 'other' END FROM t",
        "SELECT count(*) FROM t WHERE NOT (a = b)",
        "SELECT s, count(*) FROM t GROUP BY s HAVING count(*) >= 1 ORDER BY s",
        "SELECT a IS b FROM t ORDER BY 1",
    ];
    let run_all = |enabled: bool| -> Vec<Vec<Vec<Value>>> {
        with_pass(enabled, || {
            let conn = fresh_db();
            exec(&conn, "CREATE TABLE t(a INT, b INT, s TEXT)");
            exec(
                &conn,
                "INSERT INTO t VALUES (1, 1, 'x'), (2, 3, 'y'), (NULL, 1, 'z'), \
                 (4, NULL, 'w'), (NULL, NULL, 'v')",
            );
            queries.iter().map(|q| rows(&conn, q)).collect()
        })
    };
    let off = run_all(false);
    let on = run_all(true);
    for ((off_rows, on_rows), query) in off.iter().zip(on.iter()).zip(queries.iter()) {
        assert_eq!(
            format!("{off_rows:?}"),
            format!("{on_rows:?}"),
            "results changed with the peephole pass for: {query}"
        );
    }
}

/// Build `Eq(jump_if_null) -> +2; Goto L` by hand, run it on NULL input with
/// the pass on and off, and also run the *wrong* inversion (Ne with the flag
/// unchanged) to show the flag flip is what keeps NULL semantics intact.
#[test]
fn eq_inversion_preserves_jump_if_null_semantics_at_runtime() {
    // r_out = 111 when the Eq branch is taken, 222 when not.
    fn build_and_run(conn: &Arc<Connection>, wrong_inversion: bool) -> i64 {
        let mut b = ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 16, 8));
        b.prologue();
        let r_lhs = b.alloc_register();
        let r_rhs = b.alloc_register();
        let r_out = b.alloc_register();
        b.emit_insn(Insn::Null {
            dest: r_lhs,
            dest_end: None,
        });
        b.emit_insn(Insn::Integer {
            value: 7,
            dest: r_rhs,
        });
        let taken = b.allocate_label();
        let not_taken = b.allocate_label();
        let done = b.allocate_label();
        if wrong_inversion {
            // What rule 2 would produce if it forgot to flip JUMP_IF_NULL:
            // jumps to `not_taken` on NULL, unlike the original program.
            b.emit_insn(Insn::Ne {
                lhs: r_lhs,
                rhs: r_rhs,
                target_pc: not_taken,
                flags: CmpInsFlags::default().jump_if_null(),
                collation: None,
            });
        } else {
            // Jumps on NULL because of jump_if_null.
            b.emit_insn(Insn::Eq {
                lhs: r_lhs,
                rhs: r_rhs,
                target_pc: taken,
                flags: CmpInsFlags::default().jump_if_null(),
                collation: None,
            });
            b.emit_insn(Insn::Goto {
                target_pc: not_taken,
            });
        }
        b.preassign_label_to_next_insn(taken);
        b.emit_insn(Insn::Integer {
            value: 111,
            dest: r_out,
        });
        b.emit_insn(Insn::Goto { target_pc: done });
        b.preassign_label_to_next_insn(not_taken);
        b.emit_insn(Insn::Integer {
            value: 222,
            dest: r_out,
        });
        b.preassign_label_to_next_insn(done);
        b.emit_result_row(r_out, 1);
        conn.with_schema_mut(|schema| b.epilogue(schema)).unwrap();
        let program = b.build(conn.clone(), false, "peephole nulltest").unwrap();
        let mut stmt = Statement::new(program, conn.get_pager(), QueryMode::Normal, 0);
        let result = stmt.run_collect_rows().unwrap();
        match &result[0][0] {
            Value::Numeric(crate::Numeric::Integer(i)) => *i,
            other => panic!("expected integer, got {other:?}"),
        }
    }

    let conn = fresh_db();
    // NULL = 7 with jump_if_null jumps: the branch is taken.
    let off = with_pass(false, || build_and_run(&conn, false));
    assert_eq!(off, 111);
    // The pass inverts the Eq into `Ne(!jump_if_null) -> not_taken`; NULL
    // must still take the original branch.
    let on = with_pass(true, || build_and_run(&conn, false));
    assert_eq!(on, 111);
    // Without the flag flip the NULL case flips to the wrong branch: this is
    // the bug the flip prevents.
    let wrong = with_pass(false, || build_and_run(&conn, true));
    assert_eq!(wrong, 222);
}

/// The escape hatch really changes what gets prepared: with the pass off the
/// epilogue keeps its `Goto +1`s, with it on they are gone.
#[test]
fn escape_hatch_toggles_the_pass() {
    let conn = fresh_db();
    exec(&conn, "CREATE TABLE t(a INT)");
    let sql = "INSERT INTO t VALUES (1)";
    let off = with_pass(false, || explain(&conn, sql));
    let on = with_pass(true, || explain(&conn, sql));
    assert!(has_goto_to_next(&off));
    assert!(!has_goto_to_next(&on));
    assert!(on.len() < off.len());
}

/// Not a correctness test: measures real prepare cost with the pass on and
/// off in alternating blocks within one process, so machine speed drift
/// cancels out. Run with:
/// `cargo test --release -p turso_core --lib prepare_overhead -- --ignored --nocapture`
#[test]
#[ignore = "manual timing harness"]
fn prepare_overhead_manually() {
    use std::time::Instant;
    let conn = fresh_db();
    exec(
        &conn,
        "CREATE TABLE users(id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT, \
         state TEXT, city TEXT, age INT, email TEXT, phone_number TEXT, zipcode TEXT)",
    );
    let queries = [
        ("SELECT 1", 20_000u32),
        ("SELECT * FROM users LIMIT 1", 10_000),
        (
            "SELECT first_name, count(1) FROM users GROUP BY first_name \
             HAVING count(1) > 1 ORDER BY count(1) LIMIT 1",
            5_000,
        ),
        ("INSERT INTO users(id, first_name) VALUES (1, 'a')", 10_000),
    ];
    for (sql, iters) in queries {
        let measure = |enabled: bool| {
            with_pass(enabled, || {
                let t0 = Instant::now();
                for _ in 0..iters {
                    let stmt = conn.prepare(sql).unwrap();
                    std::hint::black_box(&stmt);
                }
                t0.elapsed()
            })
        };
        // Warm up, then alternate off/on blocks and use the sums.
        measure(false);
        measure(true);
        let (mut off_total, mut on_total) = (Vec::new(), Vec::new());
        for _ in 0..6 {
            off_total.push(measure(false));
            on_total.push(measure(true));
        }
        off_total.sort();
        on_total.sort();
        // Median block per side.
        let off = off_total[off_total.len() / 2] / iters;
        let on = on_total[on_total.len() / 2] / iters;
        let delta = on.as_nanos() as i128 - off.as_nanos() as i128;
        eprintln!(
            "prepare {:60} off {:>8.3?} on {:>8.3?} delta {:>6}ns ({:+.1}%)",
            &sql[..sql.len().min(60)],
            off,
            on,
            delta,
            delta as f64 * 100.0 / off.as_nanos() as f64,
        );
    }
}
