//! Recorder lifecycle soak for transparent daily partitions.
//!
//! The workload continuously inserts events, deletes partitions outside a
//! retention window, periodically reopens the database, and verifies the
//! logical row count after every simulated day.
//!
//! Run with:
//! `cargo run -p turso_core --example partitioning_soak`
//!
//! The workload can be scaled with these positive integer environment variables:
//! `TURSO_PARTITION_SOAK_DAYS`, `TURSO_PARTITION_SOAK_EVENTS_PER_DAY`,
//! `TURSO_PARTITION_SOAK_RETENTION_DAYS`, and
//! `TURSO_PARTITION_SOAK_REOPEN_EVERY_DAYS`.

use std::sync::Arc;
use std::time::Instant;
use std::{num::NonZeroUsize, path::Path};

use turso_core::partition::{DefaultPathResolver, PartitionConfig, PartitionPathResolver};
use turso_core::{Connection, Database, LimboError, Numeric, PlatformIO, Result, SqliteDialect};

const DAY_MICROS: i64 = 86_400_000_000;
const FIRST_DAY: i64 = 1_735_689_600_000_000;
const EVENTS_SCHEMA: &str = "CREATE TABLE events(\
    id INTEGER PRIMARY KEY,\
    ts INTEGER NOT NULL,\
    camera TEXT NOT NULL,\
    event_type TEXT NOT NULL,\
    confidence REAL NOT NULL\
)";
const EVENTS_PARTITION_SCHEMA: &str = "CREATE TABLE events(\
    id INTEGER PRIMARY KEY,\
    ts INTEGER NOT NULL,\
    camera TEXT NOT NULL,\
    event_type TEXT NOT NULL,\
    confidence REAL NOT NULL\
);\
CREATE INDEX idx_events_ts ON events(ts);\
CREATE INDEX idx_events_type_ts ON events(event_type, ts)";

struct Workload {
    days: i64,
    events_per_day: i64,
    retention_days: i64,
    reopen_every_days: i64,
}

fn main() -> Result<()> {
    let workload = Workload {
        days: positive_env("TURSO_PARTITION_SOAK_DAYS", 365)?,
        events_per_day: positive_env("TURSO_PARTITION_SOAK_EVENTS_PER_DAY", 120)?,
        retention_days: positive_env("TURSO_PARTITION_SOAK_RETENTION_DAYS", 30)?,
        reopen_every_days: positive_env("TURSO_PARTITION_SOAK_REOPEN_EVERY_DAYS", 7)?,
    };
    let directory = tempfile::tempdir().map_err(|error| {
        LimboError::InternalError(format!("failed to create soak directory: {error}"))
    })?;
    let database_path = directory.path().join("recorder.db");
    let partitions_directory = directory.path().join("events");
    let started = Instant::now();

    println!(
        "partition soak: {} days, {} events/day, {} retained days, reopen every {} days",
        workload.days, workload.events_per_day, workload.retention_days, workload.reopen_every_days
    );

    let (mut database, mut connection) =
        open_recorder(&database_path, &partitions_directory, true)?;
    let mut next_id = 1_i64;

    for day in 0..workload.days {
        let day_start = checked_add(
            FIRST_DAY,
            checked_mul(day, DAY_MICROS, "computing the simulated day")?,
            "computing the simulated timestamp",
        )?;
        {
            let mut insert = connection.prepare(
                "INSERT INTO events(id, ts, camera, event_type, confidence) \
                 VALUES (?, ?, 'camera-0', 'motion', 0.9)",
            )?;
            for event in 0..workload.events_per_day {
                let offset = checked_mul(event, DAY_MICROS, "spacing daily events")?
                    / workload.events_per_day;
                let timestamp = checked_add(day_start, offset, "computing an event timestamp")?;
                insert.bind_at(parameter_index(1)?, turso_core::Value::from_i64(next_id))?;
                insert.bind_at(parameter_index(2)?, turso_core::Value::from_i64(timestamp))?;
                insert.run_ignore_rows()?;
                insert.reset()?;
                next_id = checked_add(next_id, 1, "advancing the event identifier")?;
            }
        }

        if day >= workload.retention_days {
            let expired_day = day - workload.retention_days;
            let expired_timestamp = checked_add(
                FIRST_DAY,
                checked_mul(expired_day, DAY_MICROS, "computing the expired day")?,
                "computing the expired timestamp",
            )?;
            let expired_path = DefaultPathResolver::daily(partitions_directory.clone())
                .resolve_path("events", expired_timestamp);
            connection.delete_partition("events", &expired_path)?;
        }

        let retained_days = (day + 1).min(workload.retention_days);
        verify_state(&connection, retained_days, workload.events_per_day)?;

        let completed_days = day + 1;
        if completed_days % workload.reopen_every_days == 0 && completed_days < workload.days {
            connection.close()?;
            drop(connection);
            drop(database);
            (database, connection) = open_recorder(&database_path, &partitions_directory, false)?;
            verify_state(&connection, retained_days, workload.events_per_day)?;
        }

        if completed_days % 30 == 0 || completed_days == workload.days {
            println!(
                "  completed {completed_days}/{} days in {:.2?}",
                workload.days,
                started.elapsed()
            );
        }
    }

    let expected_total = checked_mul(workload.days, workload.events_per_day, "counting inserts")?;
    let elapsed = started.elapsed();
    println!(
        "SUCCESS: {expected_total} inserts, {} retained partitions, {} reopen cycles, {:.2?}",
        workload.days.min(workload.retention_days),
        (workload.days - 1) / workload.reopen_every_days,
        elapsed
    );
    println!(
        "  sustained rate: {:.0} inserts/sec",
        expected_total as f64 / elapsed.as_secs_f64()
    );

    connection.close()?;
    drop(database);
    Ok(())
}

fn open_recorder(
    database_path: &Path,
    partitions_directory: &Path,
    initialize: bool,
) -> Result<(Arc<Database>, Arc<Connection>)> {
    let io = Arc::new(PlatformIO::new()?);
    let database = Database::open_file(
        io,
        database_path.to_string_lossy().as_ref(),
        Arc::new(SqliteDialect),
    )?;
    let connection = database.connect()?;
    if initialize {
        connection.execute(format!("{EVENTS_SCHEMA} PARTITION BY (ts)"))?;
    }
    connection.register_partitioned_table(
        "events",
        PartitionConfig::new(
            Box::new(DefaultPathResolver::daily(
                partitions_directory.to_path_buf(),
            )),
            EVENTS_PARTITION_SCHEMA.to_string(),
            "ts".to_string(),
        ),
    )?;
    Ok((database, connection))
}

fn verify_state(
    connection: &Arc<Connection>,
    expected_days: i64,
    events_per_day: i64,
) -> Result<()> {
    let expected_rows = checked_mul(expected_days, events_per_day, "counting retained rows")?;
    let mut statement = connection.prepare("SELECT count(*) FROM events")?;
    let rows = statement.run_collect_rows()?;
    let actual_rows = rows
        .first()
        .and_then(|row| row.first())
        .and_then(|value| match value {
            turso_core::Value::Numeric(Numeric::Integer(value)) => Some(*value),
            _ => None,
        })
        .ok_or_else(|| {
            LimboError::ConversionError("expected one integer count result".to_string())
        })?;
    if actual_rows != expected_rows {
        return Err(LimboError::InternalError(format!(
            "soak row-count mismatch: expected {expected_rows}, found {actual_rows}"
        )));
    }

    let actual_partitions = connection.list_partitions("events")?.len();
    let expected_partitions = usize::try_from(expected_days).map_err(|error| {
        LimboError::ConversionError(format!("invalid expected partition count: {error}"))
    })?;
    if actual_partitions != expected_partitions {
        return Err(LimboError::InternalError(format!(
            "soak partition-count mismatch: expected {expected_partitions}, found {actual_partitions}"
        )));
    }
    Ok(())
}

fn positive_env(name: &str, default: i64) -> Result<i64> {
    match std::env::var(name) {
        Ok(value) => {
            let parsed = value.parse::<i64>().map_err(|error| {
                LimboError::InvalidArgument(format!("{name} must be a positive integer: {error}"))
            })?;
            if parsed <= 0 {
                return Err(LimboError::InvalidArgument(format!(
                    "{name} must be greater than zero"
                )));
            }
            Ok(parsed)
        }
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(LimboError::InvalidArgument(format!(
            "failed to read {name}: {error}"
        ))),
    }
}

fn parameter_index(index: usize) -> Result<NonZeroUsize> {
    NonZeroUsize::new(index).ok_or_else(|| {
        LimboError::InvalidArgument("statement parameter indices start at one".to_string())
    })
}

fn checked_add(left: i64, right: i64, context: &str) -> Result<i64> {
    left.checked_add(right)
        .ok_or_else(|| LimboError::InternalError(format!("integer overflow while {context}")))
}

fn checked_mul(left: i64, right: i64, context: &str) -> Result<i64> {
    left.checked_mul(right)
        .ok_or_else(|| LimboError::InternalError(format!("integer overflow while {context}")))
}
