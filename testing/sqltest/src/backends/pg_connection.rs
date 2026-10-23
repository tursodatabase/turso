use super::{BackendError, QueryResult};
use std::time::Duration;
use turso_pg_client::{BackendEvent, ConnParams, PgConn, error_message};

/// One connection to a server that speaks the PostgreSQL wire protocol.
pub struct PgConnection {
    /// Taken while a query runs on the blocking pool and put back afterwards;
    /// left empty after a timeout
    conn: Option<PgConn>,
    timeout: Duration,
}

impl PgConnection {
    pub async fn connect(
        params: &ConnParams,
        timeout: Duration,
    ) -> Result<Self, turso_pg_client::Error> {
        let params = params.clone();
        let conn = tokio::task::spawn_blocking(move || {
            PgConn::connect(&params, &[("application_name", "sqltest")])
        })
        .await
        .expect("connect task panicked")?;
        conn.set_read_timeout(Some(timeout))?;
        Ok(Self {
            conn: Some(conn),
            timeout,
        })
    }

    pub async fn execute(&mut self, sql: &str) -> Result<QueryResult, BackendError> {
        // If we don't split statements, Postgres runs all statements in one big implicit transaction.
        //
        //     When a simple Query message contains more than one SQL statement
        //     (separated by semicolons), those statements are executed as a single transaction,
        //     unless explicit transaction control commands are included to force a different behavior.
        //     Source: https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-MULTI-STATEMENT
        let mut rows = Vec::new();
        for statement in split_statements(sql) {
            let result = self.execute_statement(statement).await?;
            if result.is_error() {
                return Ok(result);
            }
            rows.extend(result.rows);
        }
        Ok(QueryResult::success(rows))
    }

    async fn execute_statement(&mut self, sql: String) -> Result<QueryResult, BackendError> {
        let mut conn = self
            .conn
            .take()
            .ok_or_else(|| BackendError::Execute("connection lost by earlier timeout".into()))?;
        let (conn, events) = tokio::task::spawn_blocking(move || {
            let events = conn.simple_query(&sql);
            (conn, events)
        })
        .await
        .expect("query task panicked");

        let events = match events {
            Ok(events) => {
                self.conn = Some(conn);
                events
            }
            Err(turso_pg_client::Error::Io(e))
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                return Err(BackendError::Timeout(self.timeout));
            }
            Err(e) => return Err(BackendError::Execute(e.to_string())),
        };

        // The simple query protocol runs statements in order and aborts the
        // rest of the string after an error, so the first ErrorResponse is
        // the query's outcome. NULL renders as an empty string, matching the
        // list-mode convention the comparison layer expects.
        let mut rows = Vec::new();
        for event in events {
            match event {
                BackendEvent::DataRow(row) => rows.push(
                    row.into_iter()
                        .map(|v| v.unwrap_or_default())
                        .collect::<Vec<String>>(),
                ),
                BackendEvent::ErrorResponse(fields) => {
                    return Ok(QueryResult::error(error_message(&fields).to_string()));
                }
                _ => {}
            }
        }
        Ok(QueryResult::success(rows))
    }
}

fn split_statements(sql: &str) -> Vec<String> {
    match pg_query::split_with_parser(sql) {
        // splitting statements removes their trailing semicolon, so we add it back
        Ok(statements) if !statements.is_empty() => statements
            .into_iter()
            .map(|statement| format!("{};", statement.trim()))
            .collect(),
        _ => vec![sql.to_string()],
    }
}

#[cfg(test)]
mod tests {
    use super::split_statements;

    #[test]
    fn begin_atomic_body_stays_in_one_statement() {
        assert_eq!(
            split_statements(
                "CREATE FUNCTION f() RETURNS int LANGUAGE sql BEGIN ATOMIC SELECT 1; SELECT 2; END; SELECT f();"
            ),
            [
                "CREATE FUNCTION f() RETURNS int LANGUAGE sql BEGIN ATOMIC SELECT 1; SELECT 2; END;",
                "SELECT f();"
            ]
        );
    }

    #[test]
    fn block_with_syntax_error_is_sent_whole() {
        assert_eq!(
            split_statements("SELECT 1; selecgt 2;"),
            ["SELECT 1; selecgt 2;"]
        );
    }

    #[test]
    fn transaction_begin_is_split() {
        assert_eq!(
            split_statements("BEGIN; SELECT atomic FROM t; COMMIT;"),
            ["BEGIN;", "SELECT atomic FROM t;", "COMMIT;"]
        );
    }
}
