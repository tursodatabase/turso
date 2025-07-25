use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use crate::{
    errors::Error,
    types::{
        DatabaseChange, DatabaseReplayOperation, DatabaseReplayRowChange,
        DatabaseReplayRowChangeType,
    },
    Result,
};

pub struct DatabaseLocalSync {
    inner: turso::Database,
    cdc_table: Arc<String>,
    pragma_query: String,
}

const DEFAULT_CDC_TABLE_NAME: &str = "turso_cdc";
const DEFAULT_CDC_MODE: &str = "full";
const DEFAULT_CHANGES_BATCH_SIZE: usize = 100;
const CDC_PRAGMA_NAME: &str = "unstable_capture_data_changes_conn";

#[derive(Debug)]
pub struct DatabaseLocalSyncOpts {
    pub cdc_table: Option<String>,
    pub cdc_mode: Option<String>,
}

impl DatabaseLocalSync {
    pub fn new(database: turso::Database) -> Self {
        let opts = DatabaseLocalSyncOpts {
            cdc_table: None,
            cdc_mode: None,
        };
        Self::new_with_opts(database, opts)
    }
    pub fn new_with_opts(database: turso::Database, opts: DatabaseLocalSyncOpts) -> Self {
        tracing::debug!("create local sync database with options {:?}", opts);
        let cdc_table = opts.cdc_table.unwrap_or(DEFAULT_CDC_TABLE_NAME.to_string());
        let cdc_mode = opts.cdc_mode.unwrap_or(DEFAULT_CDC_MODE.to_string());
        let pragma_query = format!("PRAGMA {}('{},{}')", CDC_PRAGMA_NAME, cdc_mode, cdc_table);
        Self {
            inner: database,
            cdc_table: Arc::new(cdc_table.to_string()),
            pragma_query,
        }
    }
    pub async fn connect(&self) -> Result<turso::Connection> {
        let connection = self.inner.connect().map_err(Error::TursoError)?;
        tracing::debug!("set '{}' for new connection", CDC_PRAGMA_NAME);
        connection
            .execute(&self.pragma_query, ())
            .await
            .map_err(Error::TursoError)?;
        Ok(connection)
    }
    /// Builds an iterator which emits [DatabaseReplayOperation] by extracting data from CDC table
    pub async fn iterate_changes(
        &self,
        opts: DatabaseChangesIteratorOpts,
    ) -> Result<DatabaseChangesIterator> {
        tracing::debug!("opening changes iterator with options {:?}", opts);
        let conn = self.connect().await?;
        let query = opts.mode.query(&self.cdc_table, opts.batch_size);
        let query_stmt = conn.prepare(&query).await.map_err(Error::TursoError)?;
        Ok(DatabaseChangesIterator {
            first_change_id: opts.first_change_id,
            batch: VecDeque::with_capacity(opts.batch_size),
            query_stmt,
            txn_boundary_returned: false,
            mode: opts.mode,
        })
    }
    /// Start replay session which can apply [DatabaseReplayOperation] from [Self::iterate_change_operations]
    ///
    /// As replay session can open transaction under the hood, this method consumes Connection in order to avoid potential misuse of the API
    /// (for example, if [DatabaseReplaySession] will be dropped in the middle of the replay, there will be a problem
    /// as it needs to execute async ROLLBACK command in order to properly cleanup connection state, but async in Drop
    /// is not directly possible)
    pub async fn start_replay_session(&self) -> Result<DatabaseReplaySession> {
        tracing::debug!("opening replay session");
        Ok(DatabaseReplaySession {
            conn: self.connect().await?,
            cached_delete_stmt: HashMap::new(),
            cached_insert_stmt: HashMap::new(),
            in_txn: false,
        })
    }
}

#[derive(Debug)]
pub enum DatabaseChangesIteratorMode {
    Apply,
    Revert,
}

impl DatabaseChangesIteratorMode {
    pub fn query(&self, table_name: &str, limit: usize) -> String {
        let (operation, order) = match self {
            DatabaseChangesIteratorMode::Apply => (">=", "ASC"),
            DatabaseChangesIteratorMode::Revert => ("<=", "DESC"),
        };
        format!(
            "SELECT * FROM {} WHERE change_id {} ? ORDER BY change_id {} LIMIT {}",
            table_name, operation, order, limit
        )
    }
    pub fn first_id(&self) -> i64 {
        match self {
            DatabaseChangesIteratorMode::Apply => -1,
            DatabaseChangesIteratorMode::Revert => i64::MAX,
        }
    }
    pub fn next_id(&self, id: i64) -> i64 {
        match self {
            DatabaseChangesIteratorMode::Apply => id + 1,
            DatabaseChangesIteratorMode::Revert => id - 1,
        }
    }
}

#[derive(Debug)]
pub struct DatabaseChangesIteratorOpts {
    pub first_change_id: Option<i64>,
    pub batch_size: usize,
    pub mode: DatabaseChangesIteratorMode,
}

impl Default for DatabaseChangesIteratorOpts {
    fn default() -> Self {
        Self {
            first_change_id: None,
            batch_size: DEFAULT_CHANGES_BATCH_SIZE,
            mode: DatabaseChangesIteratorMode::Apply,
        }
    }
}

pub struct DatabaseChangesIterator {
    query_stmt: turso::Statement,
    first_change_id: Option<i64>,
    batch: VecDeque<DatabaseReplayRowChange>,
    txn_boundary_returned: bool,
    mode: DatabaseChangesIteratorMode,
}

impl DatabaseChangesIterator {
    pub async fn next(&mut self) -> Result<Option<DatabaseReplayOperation>> {
        if self.batch.is_empty() {
            self.refill().await?;
        }
        // todo(sivukhin): iterator must be more clever about transaction boundaries - but for that we need to extend CDC table
        // for now, if iterator reach the end of CDC table - we are sure that this is a transaction boundary
        if let Some(change) = self.batch.pop_front() {
            self.txn_boundary_returned = false;
            Ok(Some(DatabaseReplayOperation::RowChange(change)))
        } else if !self.txn_boundary_returned {
            self.txn_boundary_returned = true;
            Ok(Some(DatabaseReplayOperation::Commit))
        } else {
            Ok(None)
        }
    }
    async fn refill(&mut self) -> Result<()> {
        let change_id_filter = self.first_change_id.unwrap_or(self.mode.first_id());
        self.query_stmt.reset();

        let rows = self.query_stmt.query((change_id_filter,)).await;
        let mut rows = rows.map_err(Error::TursoError)?;

        while let Some(row) = rows.next().await.map_err(Error::TursoError)? {
            let database_change: DatabaseChange = row.try_into()?;
            let replay_change = match self.mode {
                DatabaseChangesIteratorMode::Apply => database_change.into_apply()?,
                DatabaseChangesIteratorMode::Revert => database_change.into_revert()?,
            };
            self.batch.push_back(replay_change);
        }
        let batch_len = self.batch.len();
        if batch_len > 0 {
            self.first_change_id = Some(self.mode.next_id(self.batch[batch_len - 1].change_id));
        }
        Ok(())
    }
}

pub struct DatabaseReplaySession {
    conn: turso::Connection,
    cached_delete_stmt: HashMap<String, turso::Statement>,
    cached_insert_stmt: HashMap<(String, usize), turso::Statement>,
    in_txn: bool,
}

impl DatabaseReplaySession {
    pub async fn replay_operation(&mut self, replay_event: DatabaseReplayOperation) -> Result<()> {
        match replay_event {
            DatabaseReplayOperation::Commit => {
                tracing::trace!("replay: commit replayed changes after transaction boundary");
                self.conn
                    .execute("COMMIT", ())
                    .await
                    .map_err(Error::TursoError)?;
                self.in_txn = false;
            }
            DatabaseReplayOperation::RowChange(change) => {
                if !self.in_txn {
                    tracing::trace!("replay: start txn for replaying changes");
                    self.conn
                        .execute("BEGIN", ())
                        .await
                        .map_err(Error::TursoError)?;
                    self.in_txn = true;
                }
                tracing::trace!("replay: change={:?}", change);
                let table_name = &change.table_name;
                match change.change {
                    DatabaseReplayRowChangeType::Delete => {
                        self.replay_delete(table_name, change.id).await?
                    }
                    DatabaseReplayRowChangeType::Update { bin_record } => {
                        self.replay_delete(table_name, change.id).await?;
                        let values = parse_bin_record(bin_record)?;
                        self.replay_insert(table_name, change.id, values).await?;
                    }
                    DatabaseReplayRowChangeType::Insert { bin_record } => {
                        let values = parse_bin_record(bin_record)?;
                        self.replay_insert(table_name, change.id, values).await?;
                    }
                }
            }
        }
        Ok(())
    }
    async fn replay_delete(&mut self, table_name: &str, id: i64) -> Result<()> {
        let stmt = self.cached_delete_stmt(table_name).await?;
        stmt.execute((id,)).await.map_err(Error::TursoError)?;
        Ok(())
    }
    async fn replay_insert(
        &mut self,
        table_name: &str,
        id: i64,
        mut values: Vec<turso::Value>,
    ) -> Result<()> {
        let columns = values.len();
        let stmt = self.cached_insert_stmt(table_name, columns).await?;

        values.push(turso::Value::Integer(id));
        let params = turso::params::Params::Positional(values);

        stmt.execute(params).await.map_err(Error::TursoError)?;
        Ok(())
    }
    async fn cached_delete_stmt(&mut self, table_name: &str) -> Result<&mut turso::Statement> {
        if !self.cached_delete_stmt.contains_key(table_name) {
            tracing::trace!("prepare delete statement for replay: table={}", table_name);
            let query = format!("DELETE FROM {} WHERE rowid = ?", table_name);
            let stmt = self.conn.prepare(&query).await.map_err(Error::TursoError)?;
            self.cached_delete_stmt.insert(table_name.to_string(), stmt);
        }
        tracing::trace!(
            "ready to use prepared delete statement for replay: table={}",
            table_name
        );
        Ok(self.cached_delete_stmt.get_mut(table_name).unwrap())
    }
    async fn cached_insert_stmt(
        &mut self,
        table_name: &str,
        columns: usize,
    ) -> Result<&mut turso::Statement> {
        let key = (table_name.to_string(), columns);
        if !self.cached_insert_stmt.contains_key(&key) {
            tracing::trace!(
                "prepare insert statement for replay: table={}, columns={}",
                table_name,
                columns
            );
            let mut table_info = self
                .conn
                .query(
                    &format!("SELECT name FROM pragma_table_info('{}')", table_name),
                    (),
                )
                .await
                .map_err(Error::TursoError)?;

            let mut column_names = Vec::with_capacity(columns + 1);
            while let Some(table_info_row) = table_info.next().await.map_err(Error::TursoError)? {
                let value = table_info_row.get_value(0).map_err(Error::TursoError)?;
                column_names.push(value.as_text().expect("must be text").to_string());
            }
            column_names.push("rowid".to_string());

            let placeholders = ["?"].repeat(columns + 1).join(",");
            let query = format!(
                "INSERT INTO {}({}) VALUES ({})",
                table_name,
                column_names.join(", "),
                placeholders
            );
            let stmt = self.conn.prepare(&query).await.map_err(Error::TursoError)?;
            self.cached_insert_stmt.insert(key.clone(), stmt);
        }
        tracing::trace!(
            "ready to use prepared insert statement for replay: table={}, columns={}",
            table_name,
            columns
        );
        Ok(self.cached_insert_stmt.get_mut(&key).unwrap())
    }
}

fn parse_bin_record(bin_record: Vec<u8>) -> Result<Vec<turso::Value>> {
    let record = turso_core::types::ImmutableRecord::from_bin_record(bin_record);
    let mut cursor = turso_core::types::RecordCursor::new();
    let columns = cursor.count(&record);
    let mut values = Vec::with_capacity(columns);
    for i in 0..columns {
        let value = cursor
            .get_value(&record, i)
            .map_err(|e| Error::TursoError(e.into()))?;
        values.push(value.to_owned().into());
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use tempfile::NamedTempFile;

    use crate::database_local_sync::DatabaseLocalSync;

    async fn fetch_rows(conn: &turso::Connection, query: &str) -> Vec<Vec<turso::Value>> {
        let mut rows = vec![];
        let mut iterator = conn.query(query, ()).await.unwrap();
        while let Some(row) = iterator.next().await.unwrap() {
            let mut row_values = vec![];
            for i in 0..row.column_count() {
                row_values.push(row.get_value(i).unwrap());
            }
            rows.push(row_values);
        }
        rows
    }

    #[tokio::test]
    async fn test_database_cdc() {
        let temp_file1 = NamedTempFile::new().unwrap();
        let db_path1 = temp_file1.path().to_str().unwrap();

        let temp_file2 = NamedTempFile::new().unwrap();
        let db_path2 = temp_file2.path().to_str().unwrap();

        let db1 = turso::Builder::new_local(db_path1).build().await.unwrap();
        let db1 = DatabaseLocalSync::new(db1);
        let conn1 = db1.connect().await.unwrap();

        let db2 = turso::Builder::new_local(db_path2).build().await.unwrap();
        let db2 = DatabaseLocalSync::new(db2);
        let conn2 = db2.connect().await.unwrap();

        conn1
            .execute("CREATE TABLE a(x INTEGER PRIMARY KEY, y);", ())
            .await
            .unwrap();
        conn1
            .execute("CREATE TABLE b(x INTEGER PRIMARY KEY, y, z);", ())
            .await
            .unwrap();
        conn2
            .execute("CREATE TABLE a(x INTEGER PRIMARY KEY, y);", ())
            .await
            .unwrap();
        conn2
            .execute("CREATE TABLE b(x INTEGER PRIMARY KEY, y, z);", ())
            .await
            .unwrap();

        conn1
            .execute("INSERT INTO a VALUES (1, 'hello'), (2, 'turso')", ())
            .await
            .unwrap();

        conn1
            .execute(
                "INSERT INTO b VALUES (3, 'bye', 0.1), (4, 'limbo', 0.2)",
                (),
            )
            .await
            .unwrap();

        let mut iterator = db1.iterate_changes(Default::default()).await.unwrap();
        {
            let mut replay = db2.start_replay_session().await.unwrap();
            while let Some(change) = iterator.next().await.unwrap() {
                replay.replay_operation(change).await.unwrap();
            }
        }
        assert_eq!(
            fetch_rows(&conn2, "SELECT * FROM a").await,
            vec![
                vec![
                    turso::Value::Integer(1),
                    turso::Value::Text("hello".to_string())
                ],
                vec![
                    turso::Value::Integer(2),
                    turso::Value::Text("turso".to_string())
                ],
            ]
        );
        assert_eq!(
            fetch_rows(&conn2, "SELECT * FROM b").await,
            vec![
                vec![
                    turso::Value::Integer(3),
                    turso::Value::Text("bye".to_string()),
                    turso::Value::Real(0.1)
                ],
                vec![
                    turso::Value::Integer(4),
                    turso::Value::Text("limbo".to_string()),
                    turso::Value::Real(0.2)
                ],
            ]
        );

        conn1
            .execute("DELETE FROM b WHERE y = 'limbo'", ())
            .await
            .unwrap();

        {
            let mut replay = db2.start_replay_session().await.unwrap();
            while let Some(change) = iterator.next().await.unwrap() {
                replay.replay_operation(change).await.unwrap();
            }
        }

        assert_eq!(
            fetch_rows(&conn2, "SELECT * FROM a").await,
            vec![
                vec![
                    turso::Value::Integer(1),
                    turso::Value::Text("hello".to_string())
                ],
                vec![
                    turso::Value::Integer(2),
                    turso::Value::Text("turso".to_string())
                ],
            ]
        );
        assert_eq!(
            fetch_rows(&conn2, "SELECT * FROM b").await,
            vec![vec![
                turso::Value::Integer(3),
                turso::Value::Text("bye".to_string()),
                turso::Value::Real(0.1)
            ],]
        );

        conn1
            .execute("UPDATE b SET y = x'deadbeef' WHERE x = 3", ())
            .await
            .unwrap();

        {
            let mut replay = db2.start_replay_session().await.unwrap();
            while let Some(change) = iterator.next().await.unwrap() {
                replay.replay_operation(change).await.unwrap();
            }
        }

        assert_eq!(
            fetch_rows(&conn2, "SELECT * FROM a").await,
            vec![
                vec![
                    turso::Value::Integer(1),
                    turso::Value::Text("hello".to_string())
                ],
                vec![
                    turso::Value::Integer(2),
                    turso::Value::Text("turso".to_string())
                ],
            ]
        );
        assert_eq!(
            fetch_rows(&conn2, "SELECT * FROM b").await,
            vec![vec![
                turso::Value::Integer(3),
                turso::Value::Blob(vec![0xde, 0xad, 0xbe, 0xef]),
                turso::Value::Real(0.1)
            ]]
        );
    }
}
