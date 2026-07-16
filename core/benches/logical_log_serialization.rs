use divan::{black_box, AllocProfiler, Bencher};
use mimalloc::MiMalloc;
use turso_core::mvcc::database::{MVTableId, Row, RowID, RowKey, RowVersion, TxTimestampOrID};
use turso_core::mvcc::persistent_storage::logical_log::benchmark_serialize_op_entry;

#[global_allocator]
static ALLOC: AllocProfiler<MiMalloc> = AllocProfiler::new(MiMalloc);

fn main() {
    divan::main();
}

fn table_upsert(payload_len: usize) -> RowVersion {
    let payload = vec![0x5a; payload_len];
    let row =
        Row::new_table_row(RowID::new(MVTableId::new(-1), RowKey::Int(42)), &payload, 1).unwrap();
    RowVersion::new(1, Some(TxTimestampOrID::Timestamp(1)), None, row, false)
}

#[turso_macros::divan_bench(args = [16, 1_024, 65_536])]
fn cold_buffer(bencher: Bencher, payload_len: usize) {
    let row_version = table_upsert(payload_len);

    bencher
        .with_inputs(Vec::new)
        .bench_local_values(|mut buffer| {
            benchmark_serialize_op_entry(black_box(&mut buffer), black_box(&row_version), None)
                .unwrap();
            black_box(buffer)
        });
}

#[turso_macros::divan_bench(args = [1, 16, 256])]
fn transaction_batch(bencher: Bencher, op_count: usize) {
    let row_version = table_upsert(128);

    bencher
        .with_inputs(Vec::new)
        .bench_local_values(|mut buffer| {
            for _ in 0..op_count {
                benchmark_serialize_op_entry(black_box(&mut buffer), black_box(&row_version), None)
                    .unwrap();
            }
            black_box(buffer)
        });
}

#[turso_macros::divan_bench(args = [16, 1_024, 65_536])]
fn portable_extension(bencher: Bencher, extension_len: usize) {
    let row_version = table_upsert(128);
    let extension = vec![0xa5; extension_len];

    bencher
        .with_inputs(Vec::new)
        .bench_local_values(|mut buffer| {
            benchmark_serialize_op_entry(
                black_box(&mut buffer),
                black_box(&row_version),
                Some(black_box(&extension)),
            )
            .unwrap();
            black_box(buffer)
        });
}
