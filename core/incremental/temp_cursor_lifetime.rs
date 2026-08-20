//! Regression coverage for the DBSP circuit's cursor *lifetime*: a non-root
//! operator's state write must survive re-poll after a mid-balance yield (see
//! the `ProcessingInputs` cursor-ownership note in compiler.rs). Root nodes
//! are covered separately by `CommitState::CommitOperators`.

use crate::incremental::compiler::{DbspCircuit, DbspCompiler};
use crate::incremental::dbsp::Delta;
use crate::mvcc::yield_hooks::YieldPointMarker;
use crate::mvcc::yield_points::{YieldInjector, YieldPoint};
use crate::storage::btree::{
    BTreeCursor, BTreeWriteYieldPoint, CursorTrait, BTREE_WRITE_YIELD_FAMILY,
};
use crate::storage::pager::CreateBTreeFlags;
use crate::sync::Arc;
use crate::translate::logical::LogicalPlanBuilder;
use crate::util::IOExt;
use crate::{Connection, Database, MemoryIO, Pager, SqliteDialect, Value, IO};
use rustc_hash::FxHashMap as HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use turso_parser::ast;
use turso_parser::parser::Parser;

/// Fires the requested yield point exactly once, for the btree whose write
/// selection key matches.
#[derive(Debug)]
struct OneShotYieldInjector {
    point: YieldPoint,
    selection_key: u64,
    fired: AtomicBool,
}

impl OneShotYieldInjector {
    fn new(point: YieldPoint, selection_key: u64) -> Arc<Self> {
        Arc::new(Self {
            point,
            selection_key,
            fired: AtomicBool::new(false),
        })
    }

    fn fired(&self) -> bool {
        self.fired.load(Ordering::Acquire)
    }
}

impl YieldInjector for OneShotYieldInjector {
    fn should_yield(&self, _instance_id: u64, selection_key: u64, point: YieldPoint) -> bool {
        point == self.point
            && selection_key == self.selection_key
            && !self.fired.swap(true, Ordering::AcqRel)
    }
}

struct Fixture {
    conn: Arc<Connection>,
    pager: Arc<Pager>,
    circuit: DbspCircuit,
    main_data_root: i64,
    state_root: i64,
}

/// Compiles a circuit whose state-persisting operator (the aggregate) is a
/// *non-root* node, so it gets its cursor pair from `ExecuteState::ProcessingInputs`.
fn fixture() -> Fixture {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, k INTEGER, v INTEGER)")
        .unwrap();
    let pager = conn.pager.load().clone();

    let main_data_root = pager
        .io
        .block(|| pager.btree_create(&CreateBTreeFlags::new_table()))
        .unwrap() as i64;
    let state_root = pager
        .io
        .block(|| pager.btree_create(&CreateBTreeFlags::new_table()))
        .unwrap() as i64;
    let state_index_root = pager
        .io
        .block(|| pager.btree_create(&CreateBTreeFlags::new_index()))
        .unwrap() as i64;

    let schema = conn.schema.read().clone();
    // `count(*) + 0` forces a Projection above the aggregate (a bare `count(*)`
    // would be elided, leaving the aggregate at the root).
    let mut parser = Parser::new(b"SELECT k, count(*) + 0 AS c FROM t GROUP BY k");
    let cmd = parser.next().unwrap().unwrap();
    let ast::Cmd::Stmt(stmt) = cmd else {
        panic!("expected a statement");
    };
    let logical_plan = LogicalPlanBuilder::new(&schema)
        .build_statement(&stmt)
        .unwrap();
    let circuit = DbspCompiler::new(main_data_root, state_root, state_index_root)
        .compile(&logical_plan)
        .unwrap();

    Fixture {
        conn,
        pager,
        circuit,
        main_data_root,
        state_root,
    }
}

fn count_btree_rows(pager: &Arc<Pager>, root: i64, num_columns: usize) -> usize {
    let mut cursor = BTreeCursor::new_table(pager.clone(), root, num_columns);
    pager.io.block(|| cursor.rewind()).unwrap();
    let mut n = 0;
    while pager.io.block(|| cursor.rowid()).unwrap().is_some() {
        n += 1;
        pager.io.block(|| cursor.next()).unwrap();
    }
    n
}

/// Enough distinct groups that persisting the aggregate's state fills a leaf of
/// the DBSP state btree and the next insert balances it.
const GROUPS: i64 = 400;

#[test]
fn non_root_operator_survives_mid_balance_yield_in_state_write() {
    let Fixture {
        conn,
        pager,
        mut circuit,
        main_data_root,
        state_root,
    } = fixture();

    let injector = OneShotYieldInjector::new(
        BTreeWriteYieldPoint::AfterInsertOverflowCellBeforeBalance.point(),
        BTREE_WRITE_YIELD_FAMILY ^ state_root as u64,
    );
    // IVM-owned cursors must carry a yield context (`install_dbsp_yield_context`)
    // or they are invisible to injection and no fuzzer seed can reach this write.
    conn.set_yield_injector(Some(injector.clone()));

    let mut delta = Delta::new();
    for k in 0..GROUPS {
        delta.insert(
            k + 1,
            vec![
                Value::from_i64(k + 1),
                Value::from_i64(k),
                Value::from_i64(k * 2),
            ],
        );
    }
    let mut input = HashMap::default();
    input.insert("t".to_string(), delta);

    pager
        .io
        .block(|| circuit.commit(input.clone(), pager.clone()))
        .unwrap();

    conn.set_yield_injector(None);
    assert!(
        injector.fired(),
        "no mid-balance yield was injected into the DBSP state btree; the test \
         does not exercise the bug (are IVM cursors missing their yield context?)"
    );

    // One materialized row per group. A child node that resumed its yielded
    // state write on a rebuilt cursor either faults or drops the row.
    assert_eq!(
        count_btree_rows(&pager, main_data_root, 3),
        GROUPS as usize,
        "materialized rows lost: a non-root node's state write did not survive a \
         mid-balance yield (compiler.rs ProcessingInputs cursor lifetime)"
    );
}
