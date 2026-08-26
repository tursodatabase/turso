//! Library-first MVCC write-throughput bench.
//!
//! Callers construct [`RunSpec`] and call [`run`]. Isolation and plots wrap the
//! same binary; they do not own the loop.

mod coop;
mod latency;
mod observe;
mod run;
mod spec;
mod sqlite;
mod turso;
mod worker;

pub use run::{run, run_with_csv_sink};
pub use spec::{
    BenchError, CheckpointLabel, CheckpointPolicy, Engine, EngineLabel, LogThreshold, RepeatReport,
    ResultRow, RunSpec, Spread, StopWhen, Topology, TopologyLabel, Turso, TxnLatency, CSV_HEADER,
    SCHEMA_VERSION,
};
