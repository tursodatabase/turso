//! Allocation-failure behavior of the record buffer-recycling paths.
//!
//! Installs a process-wide allocator backend that fails tagged allocations
//! while armed on the current thread, then drives the fallible record and
//! value-copy APIs through failure and recovery. Runs as its own test binary
//! because the Turso allocator backend is process-global and set-once.
//!
//! Requires `--cfg nightly` (only the allocator-aware `Vec<T, TursoAllocator>`
//! routes through the backend) and the `allocation_metric` feature (the
//! allocation-site guards compile to nothing without it). Run with:
//! `RUSTFLAGS="--cfg nightly" cargo +nightly test -p turso_core \
//!     --features allocation_metric --test record_alloc_fault`
//! The concurrent simulator soaks the same sites probabilistically in CI via
//! `--allocation-fault-probability`.
//!
//! The Text arm of `Value::try_clone_from` is fallible through
//! `String::try_reserve` but not injectable here: `String` is not
//! allocator-parameterized, so its growth never reaches the Turso backend.
#![cfg(all(nightly, feature = "allocation_metric"))]
#![feature(allocator_api)]

use std::cell::Cell;
use std::ptr::NonNull;
use std::sync::Once;

use turso_core::alloc::{
    set_allocator, AllocError, AllocationSite, ApiAllocator, Global, Layout, TursoAllocBackend,
    ValueBlobAllocationSite,
};
use turso_core::types::{ImmutableRecord, RecordBuf, Value};

#[derive(Clone, Copy, PartialEq)]
enum FailMode {
    Disarmed,
    /// Fail every tagged allocation site.
    AllTagged,
    /// Fail only the record buffer-recycling sites.
    RecyclingSites,
    /// Fail the aggregate-accumulation site and the value-copy site
    /// aggregates stage their arguments through.
    AggSites,
}

thread_local! {
    static ARMED: Cell<FailMode> = const { Cell::new(FailMode::Disarmed) };
}

fn is_recycling_site(site: AllocationSite) -> bool {
    matches!(
        site,
        AllocationSite::ValueBlob(
            ValueBlobAllocationSite::CloneFrom
                | ValueBlobAllocationSite::RecordBuild
                | ValueBlobAllocationSite::RecordCopy
        )
    )
}

struct FailTaggedWhileArmed;

unsafe impl TursoAllocBackend for FailTaggedWhileArmed {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let fail = match (
            ARMED.with(Cell::get),
            turso_core::alloc::current_allocation_site(),
        ) {
            (FailMode::Disarmed, _) | (_, None) => false,
            (_, Some(AllocationSite::NoFaultInjection)) => false,
            (FailMode::AllTagged, Some(_)) => true,
            (FailMode::RecyclingSites, Some(site)) => is_recycling_site(site),
            (FailMode::AggSites, Some(site)) => matches!(
                site,
                AllocationSite::ValueBlob(
                    ValueBlobAllocationSite::AggAccumulate | ValueBlobAllocationSite::CloneFrom
                )
            ),
        };
        if fail {
            return Err(AllocError);
        }
        Global.allocate(layout)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        unsafe { Global.deallocate(ptr, layout) }
    }
}

static BACKEND: FailTaggedWhileArmed = FailTaggedWhileArmed;
static INSTALL: Once = Once::new();

fn with_failing_mode<T>(mode: FailMode, f: impl FnOnce() -> T) -> T {
    INSTALL.call_once(|| {
        unsafe { set_allocator(&BACKEND) }.expect("no other backend installed in this process");
    });
    ARMED.with(|armed| armed.set(mode));
    let result = f();
    ARMED.with(|armed| armed.set(FailMode::Disarmed));
    result
}

fn with_failing_allocations<T>(f: impl FnOnce() -> T) -> T {
    with_failing_mode(FailMode::AllTagged, f)
}

fn sample_values() -> Vec<Value> {
    vec![
        Value::build_text(String::from(
            "a text value long enough to require a real heap allocation",
        )),
        Value::Blob({
            let mut blob = turso_core::alloc::vec![];
            blob.extend(0u8..96);
            blob
        }),
        Value::from_i64(1234567890),
        Value::Null,
    ]
}

#[test]
fn record_build_fails_without_panicking_and_recovers() {
    let values = sample_values();
    let expected = ImmutableRecord::from_values(&values, values.len()).unwrap();

    // Growth from an empty buffer must allocate; while armed it errors.
    let err = with_failing_allocations(|| ImmutableRecord::build(&values, RecordBuf::alloc()));
    assert!(err.is_err(), "build under allocation failure must error");

    // The same call succeeds once allocations recover, byte-identical.
    let record = ImmutableRecord::build(&values, RecordBuf::alloc()).unwrap();
    assert_eq!(record.get_payload(), expected.get_payload());
}

#[test]
fn record_build_into_recycled_buffer_needs_no_allocation() {
    let values = sample_values();
    let record = ImmutableRecord::build(&values, RecordBuf::alloc()).unwrap();
    let expected = record.get_payload().to_vec();

    // A recycled buffer with sufficient capacity allocates nothing, so the
    // rebuild succeeds even while every tagged allocation fails.
    let rebuilt = with_failing_allocations(|| ImmutableRecord::build(&values, record.retire()))
        .expect("recycled-buffer build must not allocate");
    assert_eq!(rebuilt.get_payload(), expected);
}

#[test]
fn record_copy_payload_fails_without_panicking_and_recovers() {
    let values = sample_values();
    let source = ImmutableRecord::from_values(&values, values.len()).unwrap();

    let err = with_failing_allocations(|| {
        ImmutableRecord::copy_payload(source.get_payload(), RecordBuf::alloc())
    });
    assert!(
        err.is_err(),
        "copy_payload under allocation failure must error"
    );

    let copy = ImmutableRecord::copy_payload(source.get_payload(), RecordBuf::alloc()).unwrap();
    assert_eq!(copy.get_payload(), source.get_payload());

    // With a recycled buffer of sufficient capacity the copy allocates nothing.
    let spare = ImmutableRecord::copy_payload(source.get_payload(), RecordBuf::alloc()).unwrap();
    let recopied = with_failing_allocations(|| {
        ImmutableRecord::copy_payload(source.get_payload(), spare.retire())
    })
    .expect("recycled-buffer copy must not allocate");
    assert_eq!(recopied.get_payload(), source.get_payload());
}

#[test]
fn value_try_clone_from_fails_without_panicking_and_stays_valid() {
    // Blob growth past the destination's capacity errors while armed and
    // leaves the destination valid (empty but usable).
    let mut big = turso_core::alloc::vec![];
    big.extend(0u8..128);
    let src = Value::Blob(big);
    let mut dst = Value::Blob(turso_core::alloc::vec![1, 2, 3]);
    let err = with_failing_allocations(|| dst.try_clone_from(&src));
    assert!(
        err.is_err(),
        "blob growth under allocation failure must error"
    );
    match &dst {
        Value::Blob(b) => assert!(b.is_empty()),
        other => panic!("destination changed variant: {other:?}"),
    }
    dst.try_clone_from(&src).unwrap();
    assert_eq!(dst, src);

    // A destination whose buffer already fits the source copies without
    // allocating, even while armed.
    let small = Value::Blob(turso_core::alloc::vec![9, 9]);
    with_failing_allocations(|| dst.try_clone_from(&small))
        .expect("in-capacity blob copy must not allocate");
    assert_eq!(dst, small);

    // Variant mismatch allocates a fresh value; that path is fallible too and
    // leaves the destination untouched on failure.
    let mut dst = Value::from_i64(7);
    let err = with_failing_allocations(|| dst.try_clone_from(&src));
    assert!(
        err.is_err(),
        "mismatch copy under allocation failure must error"
    );
    assert_eq!(dst, Value::from_i64(7));
    dst.try_clone_from(&src).unwrap();
    assert_eq!(dst, src);
}

mod end_to_end {
    use super::{with_failing_mode, FailMode};
    use std::sync::Arc;
    use turso_core::{Database, SqliteDialect, StepResult, Value};

    fn open_mem_db() -> (Arc<Database>, Arc<turso_core::Connection>) {
        #[allow(clippy::arc_with_non_send_sync)]
        let io = Arc::new(turso_core::MemoryIO::new());
        let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let conn = db.connect().unwrap();
        (db, conn)
    }

    fn run(
        db: &Database,
        conn: &Arc<turso_core::Connection>,
        sql: &str,
    ) -> turso_core::Result<Vec<Value>> {
        let mut stmt = conn.prepare(sql)?;
        let mut first_col = Vec::new();
        loop {
            match stmt.step()? {
                StepResult::Row => {
                    let row = stmt.row().expect("row available after StepResult::Row");
                    first_col.push(row.get_value(0).clone());
                }
                StepResult::IO | StepResult::Yield => {
                    db.io.step()?;
                }
                StepResult::Done => break,
                StepResult::Interrupt | StepResult::Busy => {
                    panic!("unexpected step result")
                }
            }
        }
        Ok(first_col)
    }

    /// A sorted query and an INSERT surface allocation failures in the record
    /// buffer-recycling opcodes (MakeRecord, RowData, SorterData) as statement
    /// errors, without panicking, and the connection stays usable.
    #[test]
    fn statements_error_cleanly_when_recycling_sites_fail() {
        let (db, conn) = open_mem_db();
        run(&db, &conn, "CREATE TABLE t (x INTEGER, y TEXT)").unwrap();
        for i in 0..100 {
            run(
                &db,
                &conn,
                &format!("INSERT INTO t VALUES ({i}, 'row payload number {i} with some heft')"),
            )
            .unwrap();
        }

        let select = "SELECT y FROM t ORDER BY y DESC";
        let insert = "INSERT INTO t VALUES (100, 'inserted under allocation failure')";

        let select_err = with_failing_mode(FailMode::RecyclingSites, || run(&db, &conn, select));
        assert!(
            select_err.is_err(),
            "ORDER BY under failing recycling sites must error, got {select_err:?}"
        );
        let insert_err = with_failing_mode(FailMode::RecyclingSites, || run(&db, &conn, insert));
        assert!(
            insert_err.is_err(),
            "INSERT under failing recycling sites must error, got {insert_err:?}"
        );

        // The connection recovers: the same statements succeed afterwards and
        // the sorted result reflects only the successful inserts.
        run(&db, &conn, insert).unwrap();
        let rows = run(&db, &conn, select).unwrap();
        assert_eq!(rows.len(), 101);
        let mut sorted = rows.clone();
        sorted.sort_by(|a, b| b.cmp(a));
        assert_eq!(rows, sorted, "results are ordered DESC");
    }

    /// Aggregate and window-function statements surface allocation failures
    /// in the accumulator paths as statement errors, without panicking, and
    /// the connection recovers.
    ///
    /// Only blob-backed accumulators are injectable: group_concat accumulates
    /// into a String, which is not allocator-parameterized, so its (fallible)
    /// growth never reaches the Turso backend — the same limitation as the
    /// Text arm of `Value::try_clone_from`. json_group_array grows a ValueBlob
    /// (AggAccumulate) and last_value over growing blobs recaptures through
    /// the Blob arm of try_clone_from (CloneFrom).
    #[test]
    fn aggregates_error_cleanly_when_agg_sites_fail() {
        let (db, conn) = open_mem_db();
        run(&db, &conn, "CREATE TABLE t (x INTEGER, y TEXT, b BLOB)").unwrap();
        for i in 0..50 {
            run(
                &db,
                &conn,
                &format!(
                    "INSERT INTO t VALUES ({i}, 'value {i} with enough text to allocate', zeroblob(64 + {i}))"
                ),
            )
            .unwrap();
        }

        let json_group = "SELECT json_group_array(y) FROM t";
        let last_value = "SELECT last_value(b) OVER (ORDER BY x) FROM t";

        for sql in [json_group, last_value] {
            let err = with_failing_mode(FailMode::AggSites, || run(&db, &conn, sql));
            assert!(
                err.is_err(),
                "{sql} under failing aggregate sites must error, got {err:?}"
            );
        }

        // Recovery: the aggregates produce full, correct results.
        let rows = run(&db, &conn, json_group).unwrap();
        let expected = format!(
            "[{}]",
            (0..50)
                .map(|i| format!("\"value {i} with enough text to allocate\""))
                .collect::<Vec<_>>()
                .join(",")
        );
        assert_eq!(rows, vec![Value::build_text(expected)]);
        let rows = run(&db, &conn, last_value).unwrap();
        assert_eq!(rows.len(), 50);

        // group_concat is not injectable, but its rewritten accumulator path
        // must still produce the exact concatenation.
        let rows = run(&db, &conn, "SELECT group_concat(y, '|') FROM t").unwrap();
        let expected = (0..50)
            .map(|i| format!("value {i} with enough text to allocate"))
            .collect::<Vec<_>>()
            .join("|");
        assert_eq!(rows, vec![Value::build_text(expected)]);
    }
}
