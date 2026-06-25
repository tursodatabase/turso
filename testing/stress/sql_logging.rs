use std::cell::RefCell;
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;
use turso_stress::sync::{Arc, StdMutex};
use turso_stress::ThreadId;

/// SqlLog uses [StdMutex] instead of [AsyncMutex] (shuttle-instrumented) because
/// we don't care about Shuttle intercepting invocations on the logger.
type SqlLog = Arc<StdMutex<RefCell<BufWriter<File>>>>;

#[derive(Clone)]
pub struct SqlLogger {
    log: SqlLog,
}

impl SqlLogger {
    pub(crate) fn new(sql_log_path: &str) -> Result<Self, std::io::Error> {
        let file = File::create(sql_log_path)?;
        let log: SqlLog = Arc::new(StdMutex::new(RefCell::new(BufWriter::new(file))));
        Ok(Self { log })
    }

    pub fn log(&self, thread: &ThreadId, sql: &str, result: &str) {
        let sql = sql.trim().trim_end_matches(';');
        let w = self.log.lock().unwrap();
        let mut w = w.borrow_mut();
        writeln!(w, "{sql}; -- [thread:{thread}] {result}").unwrap();
        if result.starts_with("ERROR") {
            w.flush().unwrap();
        }
    }

    pub fn log_result<T>(&self, thread: &ThreadId, sql: &str, result: &Result<T, turso::Error>) {
        match result {
            Ok(_) => {
                self.log(thread, sql, "OK");
            }
            Err(turso::Error::Busy(e)) => {
                self.log(thread, sql, &format!("ERROR(busy): {e}"));
                println!("Error (busy): {e}");
            }
            Err(turso::Error::DatabaseFull(e)) => {
                self.log(thread, sql, &format!("ERROR(database_full): {e}"));
                eprintln!("Database full: {e}");
            }
            Err(turso::Error::IoError(std::io::ErrorKind::StorageFull, _)) => {
                self.log(thread, sql, "ERROR(io): StorageFull");
                eprintln!("No storage space, stopping");
            }
            Err(turso::Error::BusySnapshot(e)) => {
                self.log(thread, sql, &format!("ERROR(busy_snapshot): {e}"));
                println!("Error (busy snapshot): {e}");
            }
            Err(turso::Error::IoError(kind, op)) => {
                self.log(thread, sql, &format!("ERROR(io): {op}: {kind:?}"));
                eprintln!("I/O error ({op}: {kind:?})");
            }
            Err(turso::Error::Corrupt(e)) => {
                self.log(thread, sql, &format!("ERROR(corrupt): {e}"));
            }
            Err(turso::Error::Constraint(e)) => {
                self.log(thread, sql, &format!("ERROR(constraint): {e}"));
            }
            Err(e) => {
                self.log(thread, sql, &format!("ERROR: {e}"));
            }
        }
    }
}

impl Drop for SqlLogger {
    fn drop(&mut self) {
        let log = self.log.lock().unwrap();
        log.borrow_mut().flush().unwrap();
    }
}
