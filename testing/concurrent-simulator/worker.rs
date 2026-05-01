//! Worker process for multiprocess mode.
//!
//! Each worker opens the database with the host's real multiprocess-capable
//! filesystem I/O backend and executes SQL commands received from the
//! coordinator over stdin, returning results over stdout using the JSON-line
//! protocol.

use std::io::{BufRead, BufReader, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};

use turso_core::{
    CheckpointMode, Connection, Database, DatabaseOpts, LimboError, OpenFlags,
    SharedWalCoordinationOpenTelemetryMode, SharedWalTestingSnapshot, StepResult, Value,
};

use crate::multiprocess_platform_io;
use crate::protocol::{
    self, WorkerCommand, WorkerCoordinationOpenMode, WorkerResponse, WorkerSharedWalSnapshot,
    WorkerStartupTelemetry,
};

fn worker_trace_enabled() -> bool {
    std::env::var_os("WHOPPER_TRACE_WORKER").is_some()
}

/// Run the worker process main loop.
pub fn run_worker(
    db_path: &str,
    enable_mvcc: bool,
    connections_per_process: usize,
) -> anyhow::Result<()> {
    // Install a panic hook that writes to stderr so panics don't pollute stdout JSON protocol.
    std::panic::set_hook(Box::new(|info| {
        eprintln!("WORKER PANIC: {info}");
    }));

    // Re-initialize the tracing subscriber to explicitly write to stderr.
    // The parent process's init_logger() may have configured a stdout writer
    // which would corrupt the JSON-line protocol on stdout.
    use tracing_subscriber::{EnvFilter, fmt, prelude::*};
    let _ = tracing_subscriber::registry()
        .with(
            fmt::layer()
                .with_writer(std::io::stderr)
                .with_ansi(false)
                .with_line_number(true)
                .without_time()
                .with_thread_ids(false),
        )
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")))
        .try_init();

    let io = multiprocess_platform_io()?;
    let db_opts = DatabaseOpts::new().with_multiprocess_wal(true);
    let db = Database::open_file_with_flags(
        io,
        db_path,
        OpenFlags::default(),
        db_opts,
        None, // encryption_opts
    )?;

    if connections_per_process == 0 {
        return Err(anyhow::anyhow!(
            "connections_per_process must be greater than zero"
        ));
    }

    let connections = (0..connections_per_process)
        .map(|_| db.connect())
        .collect::<turso_core::Result<Vec<_>>>()?;

    if enable_mvcc {
        connections[0].execute("PRAGMA journal_mode = 'mvcc'")?;
    }

    let telemetry = worker_startup_telemetry(&db)?;

    // Signal ready
    send_response(&WorkerResponse::Ready { telemetry })?;

    let stdin = BufReader::new(std::io::stdin());
    for line in stdin.lines() {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        let cmd: WorkerCommand = serde_json::from_str(&line)?;
        match cmd {
            WorkerCommand::Execute {
                connection_idx,
                sql,
            } => {
                let conn = match connection_at(&connections, connection_idx) {
                    Ok(conn) => conn,
                    Err(err) => {
                        send_response(&WorkerResponse::Error {
                            error_kind: "InvalidArgument".to_string(),
                            message: err.to_string(),
                        })?;
                        continue;
                    }
                };
                let response = execute_sql(&conn, &sql);
                if let WorkerResponse::Error {
                    ref error_kind,
                    ref message,
                } = response
                {
                    if error_kind == "Panic" || error_kind == "Internal" {
                        eprintln!("WORKER SQL ERROR [{error_kind}]: {message} (sql: {sql})");
                    }
                }
                send_response(&response)?;
            }
            WorkerCommand::DisableAutoCheckpoint { connection_idx } => {
                let conn = match connection_at(&connections, connection_idx) {
                    Ok(conn) => conn,
                    Err(err) => {
                        send_response(&WorkerResponse::Error {
                            error_kind: "InvalidArgument".to_string(),
                            message: err.to_string(),
                        })?;
                        continue;
                    }
                };
                conn.wal_auto_checkpoint_disable();
                send_response(&WorkerResponse::Ack)?;
            }
            WorkerCommand::PassiveCheckpoint {
                connection_idx,
                upper_bound_inclusive,
            } => {
                let conn = match connection_at(&connections, connection_idx) {
                    Ok(conn) => conn,
                    Err(err) => {
                        send_response(&WorkerResponse::Error {
                            error_kind: "InvalidArgument".to_string(),
                            message: err.to_string(),
                        })?;
                        continue;
                    }
                };
                conn.checkpoint_for_testing(CheckpointMode::Passive {
                    upper_bound_inclusive,
                })?;
                send_response(&WorkerResponse::Ack)?;
            }
            WorkerCommand::ClearBackfillProof => {
                db.clear_backfill_proof_for_testing()?;
                send_response(&WorkerResponse::Ack)?;
            }
            WorkerCommand::InstallUnpublishedBackfillProof {
                connection_idx,
                upper_bound_inclusive,
            } => {
                let conn = match connection_at(&connections, connection_idx) {
                    Ok(conn) => conn,
                    Err(err) => {
                        send_response(&WorkerResponse::Error {
                            error_kind: "InvalidArgument".to_string(),
                            message: err.to_string(),
                        })?;
                        continue;
                    }
                };
                conn.install_unpublished_backfill_proof_for_testing(upper_bound_inclusive)?;
                send_response(&WorkerResponse::Ack)?;
            }
            WorkerCommand::ReadSharedWalSnapshot => {
                let snapshot = db
                    .shared_wal_snapshot_for_testing()?
                    .map(worker_shared_wal_snapshot);
                send_response(&WorkerResponse::SharedWalSnapshot { snapshot })?;
            }
            WorkerCommand::FindFrameForPage { page_id } => {
                let frame_id = db.shared_wal_find_frame_for_testing(page_id)?;
                send_response(&WorkerResponse::FrameLookup { frame_id })?;
            }
            WorkerCommand::Shutdown => {
                for conn in &connections {
                    conn.close()?;
                }
                break;
            }
        }
    }

    Ok(())
}

fn connection_at(
    connections: &[Arc<Connection>],
    connection_idx: usize,
) -> turso_core::Result<Arc<Connection>> {
    connections.get(connection_idx).cloned().ok_or_else(|| {
        LimboError::InvalidArgument(format!("invalid connection index {connection_idx}"))
    })
}

fn worker_startup_telemetry(db: &Arc<Database>) -> anyhow::Result<WorkerStartupTelemetry> {
    let telemetry = db.shared_wal_open_telemetry()?;
    let coordination_open_mode = telemetry.coordination_open_mode.map(|mode| match mode {
        SharedWalCoordinationOpenTelemetryMode::Exclusive => WorkerCoordinationOpenMode::Exclusive,
        SharedWalCoordinationOpenTelemetryMode::MultiProcess => {
            WorkerCoordinationOpenMode::MultiProcess
        }
    });
    Ok(WorkerStartupTelemetry {
        loaded_from_disk_scan: telemetry.loaded_from_disk_scan,
        reopened_max_frame: telemetry.reopened_max_frame,
        reopened_nbackfills: telemetry.reopened_nbackfills,
        reopened_checkpoint_seq: telemetry.reopened_checkpoint_seq,
        coordination_open_mode,
        sanitized_backfill_proof_on_open: telemetry.sanitized_backfill_proof_on_open,
    })
}

fn worker_shared_wal_snapshot(snapshot: SharedWalTestingSnapshot) -> WorkerSharedWalSnapshot {
    WorkerSharedWalSnapshot {
        max_frame: snapshot.max_frame,
        nbackfills: snapshot.nbackfills,
        checkpoint_seq: snapshot.checkpoint_seq,
        frame_index_overflowed: snapshot.frame_index_overflowed,
    }
}

/// Execute a SQL statement to completion and return a WorkerResponse.
fn execute_sql(conn: &Arc<Connection>, sql: &str) -> WorkerResponse {
    // Catch panics so the coordinator gets an error response instead of a crash.
    let sql_owned = sql.to_string();
    let conn = conn.clone();
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        execute_sql_inner(&conn, &sql_owned)
    })) {
        Ok(response) => response,
        Err(panic_info) => {
            let msg = if let Some(s) = panic_info.downcast_ref::<String>() {
                s.clone()
            } else if let Some(s) = panic_info.downcast_ref::<&str>() {
                s.to_string()
            } else {
                "unknown panic".to_string()
            };
            eprintln!("worker panic during SQL execution: {msg}");
            WorkerResponse::Error {
                error_kind: "Panic".to_string(),
                message: format!("worker panicked: {msg}"),
            }
        }
    }
}

fn execute_sql_inner(conn: &Arc<Connection>, sql: &str) -> WorkerResponse {
    fn rollback_autocommit_if_needed(conn: &Arc<Connection>) {
        if !conn.get_auto_commit() {
            return;
        }
        let _ = conn.execute("ROLLBACK");
    }

    let trace_worker = worker_trace_enabled();
    if trace_worker {
        eprintln!(
            "WORKER TRACE pid={} start sql={:?} auto_commit={}",
            std::process::id(),
            sql,
            conn.get_auto_commit()
        );
    }

    // Retry loop for SchemaUpdated: when another process changes the schema,
    // we need to reload it from disk and re-prepare the statement.
    for _attempt in 0..3 {
        if trace_worker {
            eprintln!(
                "WORKER TRACE pid={} prepare-start sql={:?} auto_commit={}",
                std::process::id(),
                sql,
                conn.get_auto_commit()
            );
        }
        let mut stmt = match conn.prepare(sql) {
            Ok(stmt) => {
                if trace_worker {
                    eprintln!(
                        "WORKER TRACE pid={} prepare-finish sql={:?} auto_commit={}",
                        std::process::id(),
                        sql,
                        conn.get_auto_commit()
                    );
                }
                stmt
            }
            Err(turso_core::LimboError::SchemaUpdated) => {
                if let Err(e) = conn.maybe_reparse_schema() {
                    eprintln!("worker: failed to reparse schema: {e}");
                }
                continue;
            }
            Err(e) => {
                return WorkerResponse::Error {
                    error_kind: protocol::limbo_error_to_kind(&e).to_string(),
                    message: e.to_string(),
                };
            }
        };

        let mut rows: Vec<Vec<Value>> = Vec::new();
        let mut schema_updated = false;
        let mut rollback_autocommit = false;
        let mut step_count: u64 = 0;
        let mut io_count: u64 = 0;
        let started_at = Instant::now();
        let mut last_trace_at = started_at;

        let result = loop {
            step_count += 1;
            if trace_worker && last_trace_at.elapsed() >= Duration::from_secs(1) {
                eprintln!(
                    "WORKER TRACE pid={} stepping sql={:?} steps={} ios={} elapsed_ms={} auto_commit={}",
                    std::process::id(),
                    sql,
                    step_count,
                    io_count,
                    started_at.elapsed().as_millis(),
                    conn.get_auto_commit()
                );
                last_trace_at = Instant::now();
            }

            if trace_worker && step_count == 1 {
                eprintln!(
                    "WORKER TRACE pid={} step-start sql={:?} auto_commit={}",
                    std::process::id(),
                    sql,
                    conn.get_auto_commit()
                );
            }

            match stmt.step() {
                Ok(StepResult::Row) => {
                    if let Some(row) = stmt.row() {
                        let values: Vec<Value> = row.get_values().cloned().collect();
                        rows.push(values);
                    }
                }
                Ok(StepResult::Done) => {
                    break WorkerResponse::Ok { rows };
                }
                Ok(StepResult::Busy) => {
                    rollback_autocommit = true;
                    break WorkerResponse::Error {
                        error_kind: "Busy".to_string(),
                        message: "Database is busy".to_string(),
                    };
                }
                Ok(StepResult::Interrupt) => {
                    break WorkerResponse::Error {
                        error_kind: "Interrupt".to_string(),
                        message: "Interrupted".to_string(),
                    };
                }
                Ok(StepResult::IO) => {
                    io_count += 1;
                    stmt.get_pager()
                        .io
                        .step()
                        .expect("worker should advance statement IO");
                    continue;
                }
                Err(turso_core::LimboError::SchemaUpdated) => {
                    schema_updated = true;
                    break WorkerResponse::Error {
                        error_kind: "SchemaUpdated".to_string(),
                        message: "Database schema changed".to_string(),
                    };
                }
                Err(e) => {
                    if matches!(
                        e,
                        turso_core::LimboError::Busy
                            | turso_core::LimboError::BusySnapshot
                            | turso_core::LimboError::WriteWriteConflict
                            | turso_core::LimboError::CommitDependencyAborted
                    ) {
                        rollback_autocommit = true;
                    }
                    break WorkerResponse::Error {
                        error_kind: protocol::limbo_error_to_kind(&e).to_string(),
                        message: e.to_string(),
                    };
                }
            }
        };

        if rollback_autocommit {
            drop(stmt);
            rollback_autocommit_if_needed(conn);
            if trace_worker {
                eprintln!(
                    "WORKER TRACE pid={} finish sql={:?} result=rollback steps={} ios={} elapsed_ms={} auto_commit={}",
                    std::process::id(),
                    sql,
                    step_count,
                    io_count,
                    started_at.elapsed().as_millis(),
                    conn.get_auto_commit()
                );
            }
            return result;
        }

        if schema_updated {
            // Reload schema from disk and retry (only works outside explicit transactions)
            drop(stmt);
            if let Err(e) = conn.maybe_reparse_schema() {
                eprintln!("worker: failed to reparse schema after SchemaUpdated: {e}");
                return result;
            }
            continue;
        }

        if trace_worker {
            eprintln!(
                "WORKER TRACE pid={} finish sql={:?} result={:?} steps={} ios={} elapsed_ms={} auto_commit={}",
                std::process::id(),
                sql,
                &result,
                step_count,
                io_count,
                started_at.elapsed().as_millis(),
                conn.get_auto_commit()
            );
        }

        return result;
    }

    // Exhausted retries
    WorkerResponse::Error {
        error_kind: "SchemaUpdated".to_string(),
        message: "Database schema changed (exhausted retries)".to_string(),
    }
}

/// Write a JSON-line response to stdout.
fn send_response(response: &WorkerResponse) -> anyhow::Result<()> {
    let mut stdout = std::io::stdout().lock();
    serde_json::to_writer(&mut stdout, response)?;
    stdout.write_all(b"\n")?;
    stdout.flush()?;
    Ok(())
}
