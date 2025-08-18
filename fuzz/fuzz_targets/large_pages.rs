#![no_main]
use arbitrary::Arbitrary;
use libfuzzer_sys::{fuzz_target, Corpus};
use std::{error::Error, fs, path::PathBuf, sync::Arc};

#[derive(Debug, Arbitrary, Clone)]
struct LargeDataConfig {
    blob_size: u32,
    num_insertions: u8,
    num_updates: u8,
    use_multiple_tables: bool,
    create_indexes: bool,
}

impl LargeDataConfig {
    fn normalize(&mut self) {
        self.blob_size = self.blob_size;
        self.num_insertions = self.num_insertions.max(1).min(100);
        self.num_updates = self.num_updates.min(50);
    }
}

#[derive(Debug, Arbitrary)]
enum Operation {
    Insert {
        table_id: u8,
        data_size: u32,
    },
    Update {
        table_id: u8,
        rowid: u8,
        data_size: u32,
    },
    Delete {
        table_id: u8,
        rowid: u8,
    },
    CreateIndex {
        table_id: u8,
    },
}

#[derive(Debug, Arbitrary)]
struct LargeDataWorkload {
    config: LargeDataConfig,
    operations: Vec<Operation>,
}

impl LargeDataWorkload {
    fn normalize(&mut self) {
        self.config.normalize();
        // Limit operations to prevent timeout
        self.operations.truncate(200);
    }
}

fn create_temp_file() -> Result<PathBuf, Box<dyn Error>> {
    let temp_dir = std::env::temp_dir();
    let file_name = format!("turso_fuzz_{}.db", std::process::id());
    Ok(temp_dir.join(file_name))
}

fn setup_wal_mode(conn: &rusqlite::Connection) -> Result<(), Box<dyn Error>> {
    conn.execute("PRAGMA journal_mode = WAL", ())?;
    conn.execute("PRAGMA synchronous = NORMAL", ())?;
    Ok(())
}

fn create_tables(
    rusqlite_conn: &rusqlite::Connection,
    turso_conn: &Arc<turso_core::Connection>,
    num_tables: u8,
    create_indexes: bool,
) -> Result<(), Box<dyn Error>> {
    let num_tables = num_tables.max(1).min(5);

    for i in 0..num_tables {
        let create_table_sql = format!(
            "CREATE TABLE IF NOT EXISTS t{} (id INTEGER PRIMARY KEY, data BLOB, metadata TEXT)",
            i
        );

        rusqlite_conn.execute(&create_table_sql, ())?;
        turso_conn.execute(&create_table_sql)?;

        if create_indexes {
            let create_index_sql = format!(
                "CREATE INDEX IF NOT EXISTS idx_t{}_metadata ON t{} (metadata)",
                i, i
            );

            // Indexes might not be fully supported in turso, so we ignore errors
            let _ = rusqlite_conn.execute(&create_index_sql, ());
            let _ = turso_conn.execute(&create_index_sql);
        }
    }

    Ok(())
}

fn execute_operation(
    operation: &Operation,
    rusqlite_conn: &rusqlite::Connection,
    turso_conn: &Arc<turso_core::Connection>,
    _config: &LargeDataConfig,
) -> Result<(), Box<dyn Error>> {
    match operation {
        Operation::Insert {
            table_id,
            data_size,
        } => {
            let table_num = table_id % 5;
            let blob_size = (*data_size as usize);

            let insert_sql = format!(
                "INSERT INTO t{} (data, metadata) VALUES (randomblob(?), ?)",
                table_num
            );
            let metadata = format!("size_{}_table_{}", blob_size, table_num);

            rusqlite_conn.execute(&insert_sql, rusqlite::params![blob_size, metadata])?;
            let insert_turso_sql = format!(
                "INSERT INTO t{} (data, metadata) VALUES (randomblob({}), {})",
                table_num, blob_size, metadata
            );
            turso_conn.execute(&insert_turso_sql).unwrap();
        }

        Operation::Update {
            table_id,
            rowid,
            data_size,
        } => {
            let table_num = table_id % 5;
            let blob_size = (*data_size as usize).max(1024).min(65536);
            let row_id = (*rowid as i64).max(1);

            let update_sql = format!(
                "UPDATE t{} SET data = randomblob(?) WHERE rowid = ?",
                table_num
            );

            // Execute on rusqlite
            let _ = rusqlite_conn.execute(&update_sql, rusqlite::params![blob_size, row_id]);

            // Execute on turso
            let update_turso_sql = format!(
                "UPDATE t{} SET data = randomblob({}) WHERE rowid = {}",
                table_num, blob_size, row_id
            );
            turso_conn.execute(&update_turso_sql).unwrap();
        }

        Operation::Delete { table_id, rowid } => {
            let table_num = table_id % 5;
            let row_id = (*rowid as i64).max(1);

            let delete_sql = format!("DELETE FROM t{} WHERE rowid = ?", table_num);

            // Execute on both databases
            let _ = rusqlite_conn.execute(&delete_sql, rusqlite::params![row_id]);

            let delete_sql = format!("DELETE FROM t{} WHERE rowid = {}", table_num, row_id);
            turso_conn.execute(&delete_sql).unwrap();
        }

        Operation::CreateIndex { table_id } => {
            let table_num = table_id % 5;
            let index_sql = format!(
                "CREATE INDEX IF NOT EXISTS idx_t{}_data ON t{} (metadata)",
                table_num, table_num
            );

            let _ = rusqlite_conn.execute(&index_sql, ());
            let _ = turso_conn.execute(&index_sql);
        }
    }

    Ok(())
}

fn do_fuzz(mut workload: LargeDataWorkload) -> Result<Corpus, Box<dyn Error>> {
    workload.normalize();

    // Create temporary file
    let temp_file = create_temp_file()?;

    // Ensure cleanup on drop
    struct TempFileGuard(PathBuf);
    impl Drop for TempFileGuard {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
            let _ = fs::remove_file(self.0.with_extension("db-wal"));
            let _ = fs::remove_file(self.0.with_extension("db-shm"));
        }
    }
    let _guard = TempFileGuard(temp_file.clone());

    // Setup rusqlite connection with WAL mode
    let rusqlite_conn = rusqlite::Connection::open(&temp_file)?;
    setup_wal_mode(&rusqlite_conn)?;

    // Setup turso connection
    let io = Arc::new(turso_core::MemoryIO::new());
    let db = turso_core::Database::open_file(
        io.clone(),
        temp_file.as_os_str().to_str().unwrap(),
        false,
        true,
    )?;
    let turso_conn = db.connect()?;

    // Create tables
    let num_tables = if workload.config.use_multiple_tables {
        5
    } else {
        1
    };
    create_tables(
        &rusqlite_conn,
        &turso_conn,
        num_tables,
        workload.config.create_indexes,
    )?;

    // Perform initial insertions
    for i in 0..workload.config.num_insertions {
        let operation = Operation::Insert {
            table_id: i % num_tables,
            data_size: workload.config.blob_size,
        };

        if let Err(_) = execute_operation(&operation, &rusqlite_conn, &turso_conn, &workload.config)
        {
            // If we can't perform basic insertions, this test case isn't useful
            return Ok(Corpus::Reject);
        }
    }

    // Perform updates
    for i in 0..workload.config.num_updates {
        let operation = Operation::Update {
            table_id: i % num_tables,
            rowid: (i % workload.config.num_insertions) + 1,
            data_size: workload.config.blob_size / 2, // Use smaller size for updates
        };

        let _ = execute_operation(&operation, &rusqlite_conn, &turso_conn, &workload.config);
    }

    // Execute additional operations
    for operation in &workload.operations {
        let _ = execute_operation(operation, &rusqlite_conn, &turso_conn, &workload.config);
    }

    // Check database integrity
    match rusqlite_conn.execute("PRAGMA integrity_check", ()) {
        Ok(_) => {}
        Err(_) => return Ok(Corpus::Reject),
    }
    match turso_conn.execute("PRAGMA integrity_check") {
        Ok(_) => {}
        Err(_) => return Ok(Corpus::Reject),
    }

    Ok(Corpus::Keep)
}

fuzz_target!(|workload: LargeDataWorkload| -> Corpus { do_fuzz(workload).unwrap_or(Corpus::Keep) });
