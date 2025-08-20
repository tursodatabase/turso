mod protocol;

use anyhow::{anyhow, Result};
use base64::{engine::general_purpose, Engine as _};
use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    num::NonZero,
    sync::atomic::{AtomicUsize, Ordering},
    sync::Arc,
    thread,
    time::Duration,
};
use turso_core::types::Text;
use turso_core::{Connection, StepResult, Value};

use protocol::{
    Batch, BatchResult, Column, CursorEntry, CursorRequest, CursorResponse, DescribeParam,
    DescribeResult, ExecuteResult, PipelineRequest, PipelineResponse, SqlError, SqlValue,
    Statement, StreamRequest, StreamResponse, StreamResult,
};

pub struct SqlServer {
    connection: Arc<Connection>,
    interrupt_count: Arc<AtomicUsize>,
    baton_counter: Arc<AtomicUsize>,
}

impl SqlServer {
    pub fn new(connection: Arc<Connection>, interrupt_count: Arc<AtomicUsize>) -> Self {
        Self {
            connection,
            interrupt_count,
            baton_counter: Arc::new(AtomicUsize::new(1)),
        }
    }

    pub fn run(&self, address: &str, db_file: &str) -> Result<()> {
        let listener = TcpListener::bind(address)?;
        listener.set_nonblocking(true)?;
        eprintln!("Turso v{}", env!("CARGO_PKG_VERSION"));
        eprintln!("Database: {}", db_file);
        eprintln!("SQL over HTTP server listening on {}", address);

        loop {
            // Check if we've been interrupted
            if self.interrupt_count.load(Ordering::SeqCst) > 0 {
                eprintln!("SQL server interrupted, shutting down...");
                break;
            }

            match listener.accept() {
                Ok((stream, _)) => {
                    if let Err(e) = self.handle_connection(stream) {
                        eprintln!("Error handling connection: {}", e);
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // No incoming connection, sleep briefly and continue
                    thread::sleep(Duration::from_millis(100));
                    continue;
                }
                Err(e) => {
                    eprintln!("Error accepting connection: {}", e);
                }
            }
        }

        Ok(())
    }

    fn handle_connection(&self, mut stream: TcpStream) -> Result<()> {
        let mut request_line = String::new();
        {
            let mut reader = BufReader::new(&stream);
            reader.read_line(&mut request_line)?;

            let parts: Vec<&str> = request_line.trim().split_whitespace().collect();
            if parts.len() < 3 {
                self.send_error_response(&mut stream, 400, "Bad Request")?;
                return Ok(());
            }

            let method = parts[0];
            let path = parts[1];

            // Read headers
            let mut _headers = HashMap::new();
            let mut content_length = 0;
            loop {
                let mut line = String::new();
                reader.read_line(&mut line)?;
                let line = line.trim();
                if line.is_empty() {
                    break;
                }
                if let Some((key, value)) = line.split_once(": ") {
                    _headers.insert(key.to_lowercase(), value.to_string());
                    if key.to_lowercase() == "content-length" {
                        content_length = value.parse().unwrap_or(0);
                    }
                }
            }

            // Read body if needed
            let body = if content_length > 0 {
                let mut buffer = vec![0; content_length];
                reader.read_exact(&mut buffer)?;
                String::from_utf8(buffer)?
            } else {
                String::new()
            };

            match (method, path) {
                ("GET", "/v3") => self.handle_version_check(&mut stream),
                ("POST", "/v2/pipeline") => self.handle_pipeline(&mut stream, &body),
                ("POST", "/v3/pipeline") => self.handle_pipeline(&mut stream, &body),
                ("POST", "/v3/cursor") => self.handle_cursor(&mut stream, &body),
                _ => {
                    println!("{} {} -> 404 Not Found", method, path);
                    self.send_error_response(&mut stream, 404, "Not Found")?;
                    return Ok(());
                }
            }
        }
    }

    /// Handle a version check request.
    fn handle_version_check(&self, stream: &mut TcpStream) -> Result<()> {
        let resp = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
        stream.write_all(resp.as_bytes())?;
        stream.flush()?;
        Ok(())
    }

    /// Handle a pipeline request.
    fn handle_pipeline(&self, stream: &mut TcpStream, body: &str) -> Result<()> {
        let req: PipelineRequest = serde_json::from_str(body)?;
        let resp = self.process_pipeline(req)?;
        let resp_body = serde_json::to_string(&resp)?;
        self.send_response(stream, &resp_body)?;
        Ok(())
    }

    /// Handle a cursor request.
    fn handle_cursor(&self, stream: &mut TcpStream, body: &str) -> Result<()> {
        let req: CursorRequest = serde_json::from_str(body)?;
        let (resp, entries) = self.process_cursor(req)?;
        let resp_body = serde_json::to_string(&resp)?;
        let header = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n"
        );
        stream.write_all(header.as_bytes())?;
        self.write_chunk(stream, &format!("{}\n", resp_body))?;
        for entry in entries {
            let entry_json = serde_json::to_string(&entry)?;
            self.write_chunk(stream, &format!("{}\n", entry_json))?;
        }
        stream.write_all(b"0\r\n\r\n")?;
        stream.flush()?;
        Ok(())
    }

    fn send_response(&self, stream: &mut TcpStream, body: &str) -> Result<()> {
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        stream.write_all(response.as_bytes())?;
        stream.flush()?;
        Ok(())
    }

    fn send_error_response(
        &self,
        stream: &mut TcpStream,
        status: u16,
        message: &str,
    ) -> Result<()> {
        let response = format!(
            "HTTP/1.1 {} {}\r\nContent-Length: 0\r\n\r\n",
            status, message
        );
        stream.write_all(response.as_bytes())?;
        stream.flush()?;
        Ok(())
    }

    fn write_chunk(&self, stream: &mut TcpStream, data: &str) -> Result<()> {
        let chunk = format!("{:x}\r\n{}\r\n", data.len(), data);
        stream.write_all(chunk.as_bytes())?;
        Ok(())
    }

    fn process_pipeline(&self, request: PipelineRequest) -> Result<PipelineResponse> {
        let connection = &self.connection;
        let new_baton = Some(format!(
            "baton_{}",
            self.baton_counter.fetch_add(1, Ordering::SeqCst)
        ));
        let mut results = Vec::new();

        for req in request.requests {
            let result = match req {
                StreamRequest::Execute { stmt } => self.execute_statement(&connection, stmt),
                StreamRequest::Batch { batch } => self.execute_batch(&connection, batch),
                StreamRequest::Sequence { sql } => self.execute_sequence(&connection, sql),
                StreamRequest::Describe { sql } => self.describe_statement(&connection, sql),
                StreamRequest::Close => Ok(StreamResult::Ok {
                    response: StreamResponse::Close,
                }),
                StreamRequest::GetAutocommit => Ok(StreamResult::Ok {
                    response: StreamResponse::GetAutocommit {
                        is_autocommit: true, // Simplified for now
                    },
                }),
            };

            results.push(result.unwrap_or_else(|e| StreamResult::Error {
                error: SqlError {
                    message: e.to_string(),
                    code: Some("SQLITE_ERROR".to_string()),
                },
            }));
        }

        Ok(PipelineResponse {
            baton: new_baton,
            base_url: None,
            results,
        })
    }

    fn process_cursor(&self, request: CursorRequest) -> Result<(CursorResponse, Vec<CursorEntry>)> {
        let connection = &self.connection;
        let new_baton = Some(format!(
            "baton_{}",
            self.baton_counter.fetch_add(1, Ordering::SeqCst)
        ));
        let mut entries = Vec::new();

        for (step_idx, step) in request.batch.steps.iter().enumerate() {
            // Check condition if present
            if let Some(_condition) = &step.condition {
                // Simplified condition evaluation for now
                // if !self.evaluate_condition(condition, &entries) {
                //     continue;
                // }
            }

            match self.execute_statement_cursor(&connection, &step.stmt, step_idx) {
                Ok(mut step_entries) => entries.append(&mut step_entries),
                Err(e) => {
                    entries.push(CursorEntry::StepError {
                        step: step_idx,
                        error: SqlError {
                            message: e.to_string(),
                            code: Some("SQLITE_ERROR".to_string()),
                        },
                    });
                    break;
                }
            }
        }

        Ok((
            CursorResponse {
                baton: new_baton,
                base_url: None,
            },
            entries,
        ))
    }

    fn execute_statement(
        &self,
        connection: &Arc<Connection>,
        stmt: Statement,
    ) -> Result<StreamResult> {
        let mut prepared_stmt = connection.prepare(&stmt.sql)?;

        // Bind positional parameters
        for (i, arg) in stmt.args.iter().enumerate() {
            let idx = NonZero::new(i + 1).unwrap();
            prepared_stmt.bind_at(idx, self.sql_value_to_value(arg));
        }

        // Bind named parameters
        for named_arg in &stmt.named_args {
            // Try to find parameter with various name formats
            let param_idx = prepared_stmt
                .parameters()
                .index(&named_arg.name)
                .or_else(|| {
                    prepared_stmt
                        .parameters()
                        .index(&format!(":{}", named_arg.name))
                })
                .or_else(|| {
                    prepared_stmt
                        .parameters()
                        .index(&format!("@{}", named_arg.name))
                })
                .or_else(|| {
                    prepared_stmt
                        .parameters()
                        .index(&format!("${}", named_arg.name))
                });

            if let Some(idx) = param_idx {
                prepared_stmt.bind_at(idx, self.sql_value_to_value(&named_arg.value));
            }
        }

        // Execute the statement first to get the results
        let mut result_rows = Vec::new();
        let mut cols = Vec::new();

        // Get column info
        for i in 0..prepared_stmt.num_columns() {
            cols.push(Column {
                name: prepared_stmt.get_column_name(i).to_string(),
                decltype: prepared_stmt.get_column_type(i),
            });
        }

        // Execute statement and collect rows if needed
        loop {
            match prepared_stmt.step() {
                Ok(StepResult::Row) => {
                    if stmt.want_rows {
                        let row = prepared_stmt.row().unwrap();
                        let mut result_row = Vec::new();
                        for value in row.get_values() {
                            result_row.push(self.to_sql_value(value));
                        }
                        result_rows.push(result_row);
                    }
                }
                Ok(StepResult::IO) => {
                    if prepared_stmt.run_once().is_err() {
                        break;
                    }
                }
                Ok(StepResult::Done) => break,
                Ok(StepResult::Interrupt) => break,
                Ok(StepResult::Busy) => break,
                Err(e) => return Err(anyhow!("Error executing statement: {}", e)),
            }
        }

        // Get the actual affected row count and last insert rowid
        let stmt_changes = prepared_stmt.n_change();
        let conn_changes = connection.changes();
        let affected_row_count = if stmt_changes > 0 {
            stmt_changes
        } else {
            conn_changes
        };
        let last_insert_rowid = if affected_row_count > 0 {
            Some(connection.last_insert_rowid().to_string())
        } else {
            None
        };

        Ok(StreamResult::Ok {
            response: StreamResponse::Execute {
                result: ExecuteResult {
                    cols,
                    rows: result_rows,
                    affected_row_count: affected_row_count as u64,
                    last_insert_rowid,
                },
            },
        })
    }

    fn execute_statement_cursor(
        &self,
        connection: &Arc<Connection>,
        stmt: &Statement,
        step_idx: usize,
    ) -> Result<Vec<CursorEntry>> {
        let mut entries = Vec::new();

        let mut prepared_stmt = connection.prepare(&stmt.sql)?;

        // Bind positional parameters
        for (i, arg) in stmt.args.iter().enumerate() {
            let idx = NonZero::new(i + 1).unwrap();
            prepared_stmt.bind_at(idx, self.sql_value_to_value(arg));
        }

        // Bind named parameters
        for named_arg in &stmt.named_args {
            // Try to find parameter with various name formats
            let param_idx = prepared_stmt
                .parameters()
                .index(&named_arg.name)
                .or_else(|| {
                    prepared_stmt
                        .parameters()
                        .index(&format!(":{}", named_arg.name))
                })
                .or_else(|| {
                    prepared_stmt
                        .parameters()
                        .index(&format!("@{}", named_arg.name))
                })
                .or_else(|| {
                    prepared_stmt
                        .parameters()
                        .index(&format!("${}", named_arg.name))
                });

            if let Some(idx) = param_idx {
                prepared_stmt.bind_at(idx, self.sql_value_to_value(&named_arg.value));
            }
        }

        let mut cols = Vec::new();

        // Get column info
        for i in 0..prepared_stmt.num_columns() {
            cols.push(Column {
                name: prepared_stmt.get_column_name(i).to_string(),
                decltype: prepared_stmt.get_column_type(i),
            });
        }

        entries.push(CursorEntry::StepBegin {
            step: step_idx,
            cols,
        });

        // Execute and get rows if wanted
        if stmt.want_rows {
            loop {
                match prepared_stmt.step() {
                    Ok(StepResult::Row) => {
                        let row = prepared_stmt.row().unwrap();
                        let mut result_row = Vec::new();

                        for value in row.get_values() {
                            result_row.push(self.to_sql_value(value));
                        }

                        entries.push(CursorEntry::Row { row: result_row });
                    }
                    Ok(StepResult::IO) => {
                        if prepared_stmt.run_once().is_err() {
                            break;
                        }
                    }
                    Ok(StepResult::Done) => break,
                    Ok(StepResult::Interrupt) => break,
                    Ok(StepResult::Busy) => break,
                    Err(e) => return Err(anyhow!("Error executing statement: {}", e)),
                }
            }
        } else {
            // Just run the statement to completion without collecting rows
            loop {
                match prepared_stmt.step() {
                    Ok(StepResult::Row) => continue, // Skip rows since want_rows is false
                    Ok(StepResult::IO) => {
                        if prepared_stmt.run_once().is_err() {
                            break;
                        }
                    }
                    Ok(StepResult::Done) => break,
                    Ok(StepResult::Interrupt) => break,
                    Ok(StepResult::Busy) => break,
                    Err(e) => return Err(anyhow!("Error executing statement: {}", e)),
                }
            }
        }

        // Get the actual affected row count and last insert rowid
        let stmt_changes = prepared_stmt.n_change();
        let conn_changes = connection.changes();
        let affected_row_count = if stmt_changes > 0 {
            stmt_changes
        } else {
            conn_changes
        };
        let last_insert_rowid = if affected_row_count > 0 {
            Some(connection.last_insert_rowid().to_string())
        } else {
            None
        };

        entries.push(CursorEntry::StepEnd {
            affected_row_count: affected_row_count as u64,
            last_insert_rowid,
        });

        Ok(entries)
    }

    fn execute_batch(&self, connection: &Arc<Connection>, batch: Batch) -> Result<StreamResult> {
        let mut step_results = Vec::new();
        let mut step_errors = Vec::new();

        for (_step_idx, step) in batch.steps.iter().enumerate() {
            match self.execute_statement(connection, step.stmt.clone()) {
                Ok(StreamResult::Ok { response }) => {
                    if let StreamResponse::Execute { result } = response {
                        step_results.push(Some(result));
                        step_errors.push(None);
                    }
                }
                Ok(StreamResult::Error { error }) => {
                    step_results.push(None);
                    step_errors.push(Some(error));
                }
                Err(e) => {
                    step_results.push(None);
                    step_errors.push(Some(SqlError {
                        message: e.to_string(),
                        code: Some("SQLITE_ERROR".to_string()),
                    }));
                }
            }
        }

        Ok(StreamResult::Ok {
            response: StreamResponse::Batch {
                result: BatchResult {
                    step_results,
                    step_errors,
                },
            },
        })
    }

    fn execute_sequence(&self, connection: &Arc<Connection>, sql: String) -> Result<StreamResult> {
        match connection.execute(&sql) {
            Ok(()) => Ok(StreamResult::Ok {
                response: StreamResponse::Sequence,
            }),
            Err(e) => Err(anyhow!("Error executing sequence: {}", e)),
        }
    }

    fn describe_statement(
        &self,
        connection: &Arc<Connection>,
        sql: String,
    ) -> Result<StreamResult> {
        let prepared_stmt = connection.prepare(&sql)?;

        let mut cols = Vec::new();
        let mut params = Vec::new();

        // Get column information
        for i in 0..prepared_stmt.num_columns() {
            cols.push(Column {
                name: prepared_stmt.get_column_name(i).to_string(),
                decltype: prepared_stmt.get_column_type(i),
            });
        }

        // Get parameter information
        for i in 1..=prepared_stmt.parameters_count() {
            params.push(DescribeParam {
                name: None, // TODO: Get parameter names if available
            });
        }

        Ok(StreamResult::Ok {
            response: StreamResponse::Describe {
                result: DescribeResult {
                    params,
                    cols,
                    is_explain: false,
                    is_readonly: true, // TODO: Determine if statement is read-only
                },
            },
        })
    }

    fn to_sql_value(&self, value: &Value) -> SqlValue {
        match value {
            Value::Null => SqlValue::Null,
            Value::Integer(i) => SqlValue::Integer {
                value: i.to_string(),
            },
            Value::Float(f) => SqlValue::Float { value: *f },
            Value::Text(s) => SqlValue::Text {
                value: s.to_string(),
            },
            Value::Blob(b) => SqlValue::Blob {
                base64: general_purpose::STANDARD.encode(b),
            },
        }
    }

    fn sql_value_to_value(&self, sql_value: &SqlValue) -> Value {
        match sql_value {
            SqlValue::Null => Value::Null,
            SqlValue::Integer { value } => Value::Integer(value.parse().unwrap_or(0)),
            SqlValue::Float { value } => Value::Float(*value),
            SqlValue::Text { value } => Value::Text(Text::new(value)),
            SqlValue::Blob { base64 } => match general_purpose::STANDARD.decode(base64) {
                Ok(bytes) => Value::Blob(bytes),
                Err(_) => Value::Blob(vec![]),
            },
        }
    }
}
