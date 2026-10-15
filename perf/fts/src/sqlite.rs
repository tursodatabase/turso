use anyhow::{Result, ensure};
use memory_benchmark::fts::{Execution, FtsConfig, QueryCase, QueryState, RunResult, document};
use rusqlite::Connection;
use tempfile::TempDir;

pub struct SqliteWorkload {
    sessions: Vec<Connection>,
    _directory: TempDir,
    config: FtsConfig,
}

impl SqliteWorkload {
    pub async fn prepare(config: FtsConfig) -> Result<Self> {
        config.validate()?;
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("fts.db");
        let mut conn = Connection::open(&path)?;
        conn.pragma_update(None, "journal_mode", "wal")?;
        conn.execute_batch("CREATE TABLE docs(id INTEGER PRIMARY KEY, title TEXT, body TEXT)")?;
        for start in (0..config.documents).step_by(500) {
            let transaction = conn.transaction()?;
            {
                let mut insert = transaction.prepare("INSERT INTO docs VALUES (?1, ?2, ?3)")?;
                for id in start..config.documents.min(start + 500) {
                    let (title, body) = document(id);
                    insert.execute(rusqlite::params![id as i64, title, body])?;
                }
            }
            transaction.commit()?;
        }
        conn.execute_batch(
            "CREATE VIRTUAL TABLE docs_fts USING fts5(title, body, content='docs', content_rowid='id', tokenize='unicode61');
             INSERT INTO docs_fts(docs_fts) VALUES ('rebuild');",
        )?;
        drop(conn);
        let sessions = (0..config.connections)
            .map(|_| Connection::open(&path))
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut workload = Self {
            sessions,
            _directory: directory,
            config,
        };
        if matches!(config.state, QueryState::Warm) {
            workload.batch(1).await?;
        }
        Ok(workload)
    }

    pub fn sessions(&mut self) -> &mut [Connection] {
        &mut self.sessions
    }

    pub async fn run(&mut self) -> Result<RunResult> {
        let queries = match self.config.execution {
            Execution::Queries(_) => 1,
            Execution::Transactions {
                queries_per_transaction,
                ..
            } => queries_per_transaction,
        };
        let mut result = RunResult::default();
        for _ in 0..self.config.execution.batches() {
            let batch = self.batch(queries).await?;
            result.queries += batch.queries;
            result.transactions += batch.transactions;
            result.rows += batch.rows;
            result.id_sum += batch.id_sum;
            result.max_active_transactions = batch.max_active_transactions;
        }
        Ok(result)
    }

    async fn batch(&mut self, queries: usize) -> Result<RunResult> {
        let transactions = matches!(self.config.execution, Execution::Transactions { .. });
        if transactions {
            for conn in &self.sessions {
                conn.execute_batch("BEGIN")?;
                ensure!(!conn.is_autocommit(), "BEGIN must start a transaction");
            }
        }
        let mut result = RunResult::default();
        if self.sessions.len() == 1 {
            result = query_batch(&self.sessions[0], self.config.query, queries)?;
        } else {
            let mut workers = tokio::task::JoinSet::new();
            for conn in self.sessions.drain(..) {
                let case = self.config.query;
                workers.spawn_blocking(move || {
                    let result = query_batch(&conn, case, queries);
                    (conn, result)
                });
            }
            let mut outcomes = Vec::new();
            while let Some(outcome) = workers.join_next().await {
                outcomes.push(outcome);
            }
            for outcome in outcomes {
                let (conn, batch) = outcome?;
                self.sessions.push(conn);
                let batch = batch?;
                result.queries += batch.queries;
                result.rows += batch.rows;
                result.id_sum += batch.id_sum;
            }
        }
        if transactions {
            for conn in &self.sessions {
                conn.execute_batch("COMMIT")?;
                ensure!(conn.is_autocommit(), "COMMIT must end the transaction");
            }
            result.transactions = self.sessions.len();
            result.max_active_transactions = self.sessions.len();
        }
        Ok(result)
    }
}

pub(super) fn query_batch(conn: &Connection, case: QueryCase, queries: usize) -> Result<RunResult> {
    let mut result = RunResult::default();
    for _ in 0..queries {
        let mut statement = conn.prepare(sql(case))?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            result.rows += 1;
            result.id_sum += row.get::<_, i64>(0)?;
        }
        result.queries += 1;
    }
    Ok(result)
}

fn sql(case: QueryCase) -> &'static str {
    match case {
        QueryCase::Rare => "SELECT rowid FROM docs_fts WHERE docs_fts MATCH 'rare'",
        QueryCase::Common => "SELECT rowid FROM docs_fts WHERE docs_fts MATCH 'common'",
        QueryCase::And => "SELECT rowid FROM docs_fts WHERE docs_fts MATCH 'alpha AND beta'",
        QueryCase::Or => "SELECT rowid FROM docs_fts WHERE docs_fts MATCH 'alpha OR beta'",
        QueryCase::Phrase => "SELECT rowid FROM docs_fts WHERE docs_fts MATCH '\"common rare\"'",
        QueryCase::Ranked => {
            "SELECT rowid, bm25(docs_fts) AS score FROM docs_fts WHERE docs_fts MATCH 'alpha OR beta' ORDER BY score ASC LIMIT 10"
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use memory_benchmark::fts::CorpusConfig;
    use memory_benchmark::workload::JournalMode;

    #[tokio::test]
    async fn ranked_scores_are_sorted_and_limit_handles_small_corpora() -> Result<()> {
        for (documents, expected_rows) in [(5, 4), (203, 10)] {
            let workload = SqliteWorkload::prepare(FtsConfig {
                query: QueryCase::Ranked,
                state: QueryState::First,
                documents,
                corpus: CorpusConfig::default(),
                mode: JournalMode::Wal,
                connections: 1,
                execution: Execution::Queries(1),
            })
            .await?;
            let conn = &workload.sessions[0];
            assert_eq!(
                conn.pragma_query_value(None, "journal_mode", |row| row.get::<_, String>(0))?,
                "wal"
            );
            let mut statement = conn.prepare(sql(QueryCase::Ranked))?;
            let mut rows = statement.query([])?;
            let mut ids = Vec::new();
            let mut previous = f64::NEG_INFINITY;
            while let Some(row) = rows.next()? {
                let id: i64 = row.get(0)?;
                let score: f64 = row.get(1)?;
                assert!((0..documents as i64).contains(&id));
                assert!(id % 2 == 0 || id % 3 == 0);
                assert!(!ids.contains(&id));
                assert!(score < 0.0 && score >= previous);
                if documents == 203 {
                    assert_eq!(id % 6, 0);
                }
                previous = score;
                ids.push(id);
            }
            assert_eq!(ids.len(), expected_rows);
        }
        Ok(())
    }
}
