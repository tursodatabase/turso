use std::fs::File;
use std::io::BufWriter;
use std::io::Write;
use std::sync::{Arc, Mutex};

type SqlLog = Arc<Mutex<BufWriter<File>>>;

#[derive(Clone)]
pub struct SqlLogger {
    log: SqlLog,
}

impl SqlLogger {
    pub(crate) fn new(sql_log_path: &str) -> Result<Self, std::io::Error> {
        let file = File::create(sql_log_path)?;
        let log: SqlLog = Arc::new(Mutex::new(BufWriter::new(file)));
        Ok(Self { log })
    }

    pub fn log(&self, thread: usize, sql: &str, result: &str) {
        let sql = sql.trim().trim_end_matches(';');
        let mut w = self.log.lock().unwrap();
        writeln!(w, "{sql}; -- [thread:{thread}] {result}").unwrap();
        if result.starts_with("ERROR") {
            w.flush().unwrap();
        }
    }
}

impl Drop for SqlLogger {
    fn drop(&mut self) {
        let mut w = self.log.lock().unwrap();
        w.flush().unwrap();
    }
}
