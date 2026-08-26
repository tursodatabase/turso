use std::io::Write;
use std::path::Path;

use crate::observe::{shm_path_for, wal_path_for};
use crate::spec::{BenchError, Engine, RepeatReport, RunSpec};
use crate::sqlite::run_sqlite_once;
use crate::turso::run_turso_once;

pub fn run(spec: &RunSpec) -> Result<RepeatReport, BenchError> {
    run_with_csv_sink(spec, None)
}

pub fn run_with_csv_sink(
    spec: &RunSpec,
    mut sink: Option<&mut dyn Write>,
) -> Result<RepeatReport, BenchError> {
    spec.check_invariants()?;
    let mut rows = Vec::with_capacity(spec.repeats.get());
    for repeat in 0..spec.repeats.get() {
        unlink_db_tree(&spec.path)?;
        let mut row = match &spec.engine {
            Engine::Turso(t) => run_turso_once(spec, t)?,
            Engine::Sqlite => run_sqlite_once(spec)?,
        };
        row.repeat = repeat as u32;
        if let Some(w) = sink.as_mut() {
            row.write_csv_line(w)?;
            w.flush()?;
        }
        rows.push(row);
    }
    Ok(RepeatReport { rows })
}

pub(crate) fn unlink_db_tree(path: &Path) -> Result<(), BenchError> {
    let log_path = path.with_extension("db-log");
    let extras = [wal_path_for(path), shm_path_for(path), log_path];
    remove_if_exists(path)?;
    for extra in extras {
        remove_if_exists(&extra)?;
    }
    Ok(())
}

fn remove_if_exists(path: &Path) -> Result<(), BenchError> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}
