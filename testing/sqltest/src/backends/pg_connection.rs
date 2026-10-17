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
        let mut conn = self
            .conn
            .take()
            .ok_or_else(|| BackendError::Execute("connection lost by earlier timeout".into()))?;
        let sql = sql.to_string();
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
                BackendEvent::CopyData(data) => {
                    let text = String::from_utf8(data)
                        .map_err(|e| BackendError::Execute(e.to_string()))?;
                    let row = text.strip_suffix('\n').ok_or_else(|| {
                        BackendError::Execute("COPY text row has no final newline".into())
                    })?;
                    rows.push(row.split('\t').map(str::to_owned).collect());
                }
                BackendEvent::ErrorResponse(fields) => {
                    return Ok(QueryResult::error(error_message(&fields).to_string()));
                }
                _ => {}
            }
        }
        Ok(QueryResult::success(rows))
    }
}
