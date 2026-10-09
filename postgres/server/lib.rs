use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::num::NonZero;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

use async_trait::async_trait;
use bytes::{Buf, Bytes};
use futures::{stream, Sink, SinkExt, TryStreamExt};
use tokio::net::TcpListener;
use tracing::{error, info};
use turso_core::Value;
use turso_pg::{split_statements, Connection, PgConnection};

use pgwire::api::auth::StartupHandler;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{
    send_execution_response, send_query_response, send_ready_for_query, ExtendedQueryHandler,
    SimpleQueryHandler,
};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo,
    QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::{
    ClientInfo, ClientPortalStore, NoopHandler, PgWireConnectionState, PgWireServerHandlers, Type,
};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::copy::{CopyData, CopyDone, CopyOutResponse};
use pgwire::messages::data::DataRow;
use pgwire::messages::response::{EmptyQueryResponse, TransactionStatus};
use pgwire::messages::simplequery::Query;
use pgwire::messages::PgWireBackendMessage;
use pgwire::tokio::process_socket;
use pgwire::types::format::FormatOptions;

pub struct TursoPgServer {
    address: String,
    db_file: String,
    conn: Arc<Mutex<PgConnection>>,
    interrupt_count: Arc<AtomicUsize>,
}

impl TursoPgServer {
    pub fn new(
        address: String,
        db_file: String,
        conn: Connection,
        interrupt_count: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            address,
            db_file,
            conn: Arc::new(Mutex::new(conn)),
            interrupt_count,
        }
    }

    pub fn run(&self) -> anyhow::Result<()> {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(self.run_async())
    }

    async fn run_async(&self) -> anyhow::Result<()> {
        let listener = TcpListener::bind(&self.address).await?;
        println!(
            "PostgreSQL server listening on {} (database: {})",
            listener.local_addr()?,
            self.db_file
        );

        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((socket, addr)) => {
                            info!("PostgreSQL client connected from {}", addr);
                            let factory_ref = Arc::new(TursoPgFactory {
                                handler: Arc::new(TursoPgHandler {
                                    conn: self.conn.clone(),
                                    db_file: self.db_file.clone(),
                                    query_parser: Arc::new(NoopQueryParser::new()),
                                    cursors: Mutex::new(HashMap::new()),
                                }),
                            });
                            tokio::spawn(async move {
                                if let Err(e) = process_socket(socket, None, factory_ref).await {
                                    error!("Error processing connection from {}: {}", addr, e);
                                }
                            });
                        }
                        Err(e) => {
                            error!("Error accepting connection: {}", e);
                        }
                    }
                }
                _ = tokio::signal::ctrl_c() => {
                    println!("\nShutting down PostgreSQL server...");
                    break;
                }
            }

            if self.interrupt_count.load(Ordering::SeqCst) > 0 {
                println!("Shutting down PostgreSQL server...");
                break;
            }
        }

        Ok(())
    }
}

struct TursoPgHandler {
    conn: Arc<Mutex<PgConnection>>,
    db_file: String,
    query_parser: Arc<NoopQueryParser>,
    cursors: Mutex<HashMap<String, PgCursor>>,
}

impl TursoPgHandler {
    /// After a DROP SCHEMA query succeeds, delete the schema's database file.
    /// Uses simple string matching to detect DROP SCHEMA statements.
    fn cleanup_dropped_schema_file(&self, query: &str) {
        if self.db_file == ":memory:" {
            return;
        }
        // Simple detection: look for DROP SCHEMA pattern
        let trimmed = query.trim().to_lowercase();
        if !trimmed.starts_with("drop schema") {
            return;
        }
        // Extract schema name: "drop schema [if exists] <name> [cascade|restrict]"
        let rest = trimmed.strip_prefix("drop schema").unwrap().trim();
        let rest = rest
            .strip_prefix("if exists")
            .map(|s| s.trim())
            .unwrap_or(rest);
        // Take the first word as the schema name
        let name = rest
            .split_whitespace()
            .next()
            .unwrap_or("")
            .trim_matches('"');
        if name.is_empty() || name == "public" {
            return;
        }
        let parent = std::path::Path::new(&self.db_file)
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."));
        let schema_file = parent.join(format!("turso-postgres-schema-{name}.db"));
        if schema_file.exists() {
            if let Err(e) = std::fs::remove_file(&schema_file) {
                tracing::warn!("Failed to delete schema file {:?}: {}", schema_file, e);
            } else {
                tracing::info!("Deleted schema file {:?}", schema_file);
            }
            // Also clean up WAL and SHM files
            let wal = schema_file.with_extension("db-wal");
            let shm = schema_file.with_extension("db-shm");
            let _ = std::fs::remove_file(wal);
            let _ = std::fs::remove_file(shm);
        }
    }
}

struct TursoPgFactory {
    handler: Arc<TursoPgHandler>,
}

impl PgWireServerHandlers for TursoPgFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.handler.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        Arc::new(NoopHandler)
    }
}

#[async_trait]
impl SimpleQueryHandler for TursoPgHandler {
    async fn on_query<C>(&self, client: &mut C, query: Query) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if !matches!(client.state(), PgWireConnectionState::ReadyForQuery) {
            return Err(PgWireError::NotReadyForQuery);
        }
        client.set_state(PgWireConnectionState::QueryInProgress);
        let statements = split_statements(&query.query)
            .map_err(|e| PgWireError::UserError(Box::new(error_info(&e.to_string()))))?;
        if statements.is_empty() {
            client
                .feed(PgWireBackendMessage::EmptyQueryResponse(
                    EmptyQueryResponse::new(),
                ))
                .await?;
        }
        for sql in statements {
            let parsed = turso_pg_parser::parse(&sql)
                .map_err(|e| PgWireError::UserError(Box::new(error_info(&e.to_string()))))?;
            match turso_pg_parser::translator::try_extract_copy_to(&parsed)
                .map_err(|e| PgWireError::UserError(Box::new(error_info(&e))))?
            {
                Some(copy) => {
                    let output = self.copy_to_stdout(&copy)?;
                    client
                        .feed(PgWireBackendMessage::CopyOutResponse(CopyOutResponse::new(
                            0,
                            output.columns as i16,
                            vec![0; output.columns],
                        )))
                        .await?;
                    for row in output.rows {
                        client
                            .feed(PgWireBackendMessage::CopyData(CopyData::new(Bytes::from(
                                row,
                            ))))
                            .await?;
                    }
                    client
                        .feed(PgWireBackendMessage::CopyDone(CopyDone::new()))
                        .await?;
                    send_execution_response(client, Tag::new("COPY").with_rows(output.row_count))
                        .await?;
                    client.set_transaction_status(self.transaction_status());
                }
                None => {
                    let responses = SimpleQueryHandler::do_query(self, client, &sql).await?;
                    client.set_transaction_status(self.transaction_status());
                    for response in responses {
                        match response {
                            Response::Query(mut result) => {
                                send_query_response(client, &mut result, true).await?
                            }
                            Response::Execution(tag) => {
                                send_execution_response(client, tag).await?;
                            }
                            _ => {
                                return Err(PgWireError::ApiError(
                                    "unexpected simple query response".into(),
                                ));
                            }
                        }
                    }
                }
            }
        }
        let transaction_status = self.transaction_status();
        client.set_state(PgWireConnectionState::ReadyForQuery);
        client.set_transaction_status(transaction_status);
        send_ready_for_query(client, transaction_status).await
    }

    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let conn = self.conn.lock().unwrap().clone();

        // Per the PostgreSQL simple query protocol, a query string may contain
        // multiple semicolon-separated statements. Split and execute each one.
        let statements = split_statements(query)
            .map_err(|e| PgWireError::UserError(Box::new(error_info(&e.to_string()))))?;

        let mut responses = Vec::new();
        for sql in &statements {
            let parsed = turso_pg_parser::parse(sql)
                .map_err(|e| PgWireError::UserError(Box::new(error_info(&e.to_string()))))?;
            if let Some(cursor) = turso_pg_parser::translator::try_extract_cursor(&parsed)
                .map_err(|e| PgWireError::UserError(Box::new(error_info(&e))))?
            {
                if client.transaction_status() == TransactionStatus::Error {
                    return Err(PgWireError::UserError(Box::new(error_info(
                        "current transaction is aborted",
                    ))));
                }
                responses.push(self.cursor_response(cursor).await?);
                continue;
            }
            let mut stmt = conn
                .prepare(sql)
                .map_err(|e| PgWireError::UserError(Box::new(error_info(&e.to_string()))))?;

            self.cleanup_dropped_schema_file(sql);

            if stmt.num_columns() == 0 || is_pg_non_query(sql) {
                responses.push(execute_non_query(&mut stmt, sql)?);
            } else {
                let header = Arc::new(build_field_info(&stmt, &Format::UnifiedText));
                responses.push(execute_query(&mut stmt, header)?);
            }
            if conn.inner().get_auto_commit() {
                self.cursors.lock().unwrap().clear();
            }
        }

        Ok(responses)
    }
}

struct PgCursor {
    header: Arc<Vec<FieldInfo>>,
    rows: VecDeque<DataRow>,
}

impl TursoPgHandler {
    async fn cursor_response(
        &self,
        command: turso_pg_parser::translator::PgCursorStmt,
    ) -> PgWireResult<Response> {
        use turso_pg_parser::translator::PgCursorStmt;

        match command {
            PgCursorStmt::Declare { name, query } => {
                if self.transaction_status() != TransactionStatus::Transaction {
                    return Err(PgWireError::UserError(Box::new(error_info(
                        "DECLARE CURSOR can only be used in transaction blocks",
                    ))));
                }
                if self.cursors.lock().unwrap().contains_key(&name) {
                    return Err(PgWireError::UserError(Box::new(error_info(&format!(
                        "cursor \"{name}\" already exists"
                    )))));
                }
                let conn = self.conn.lock().unwrap().clone();
                let mut stmt = conn
                    .prepare(&query)
                    .map_err(|e| PgWireError::UserError(Box::new(error_info(&e.to_string()))))?;
                let header = Arc::new(build_field_info(&stmt, &Format::UnifiedText));
                let Response::Query(mut result) = execute_query(&mut stmt, header.clone())? else {
                    unreachable!();
                };
                let rows: Vec<DataRow> = result.data_rows().try_collect().await?;
                self.cursors.lock().unwrap().insert(
                    name,
                    PgCursor {
                        header,
                        rows: rows.into(),
                    },
                );
                Ok(Response::Execution(Tag::new("DECLARE CURSOR")))
            }
            PgCursorStmt::Fetch { name, count } => {
                let mut cursors = self.cursors.lock().unwrap();
                let cursor = cursors.get_mut(&name).ok_or_else(|| {
                    PgWireError::UserError(Box::new(error_info(&format!(
                        "cursor \"{name}\" does not exist"
                    ))))
                })?;
                let count = cursor
                    .rows
                    .len()
                    .min(usize::try_from(count).unwrap_or(usize::MAX));
                let rows: Vec<_> = cursor.rows.drain(..count).map(Ok).collect();
                let mut response = QueryResponse::new(cursor.header.clone(), stream::iter(rows));
                response.set_command_tag("FETCH");
                Ok(Response::Query(response))
            }
            PgCursorStmt::Close { name } => {
                let mut cursors = self.cursors.lock().unwrap();
                match name {
                    Some(name) => {
                        cursors.remove(&name).ok_or_else(|| {
                            PgWireError::UserError(Box::new(error_info(&format!(
                                "cursor \"{name}\" does not exist"
                            ))))
                        })?;
                    }
                    None => cursors.clear(),
                }
                Ok(Response::Execution(Tag::new("CLOSE CURSOR")))
            }
        }
    }
}

struct CopyOutput {
    columns: usize,
    rows: Vec<Vec<u8>>,
    row_count: usize,
}

impl TursoPgHandler {
    fn transaction_status(&self) -> TransactionStatus {
        if self.conn.lock().unwrap().inner().get_auto_commit() {
            TransactionStatus::Idle
        } else {
            TransactionStatus::Transaction
        }
    }

    fn copy_to_stdout(
        &self,
        copy: &turso_pg_parser::translator::PgCopyToStmt,
    ) -> PgWireResult<CopyOutput> {
        let quote = |name: &str| format!("\"{}\"", name.replace('"', "\"\""));
        let columns = copy
            .columns
            .as_ref()
            .map(|columns| {
                columns
                    .iter()
                    .map(|name| quote(name))
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_else(|| "*".to_owned());
        let table = match &copy.schema_name {
            Some(schema) => format!("{}.{}", quote(schema), quote(&copy.table_name)),
            None => quote(&copy.table_name),
        };
        let conn = self.conn.lock().unwrap().clone();
        let mut stmt = conn
            .prepare(&format!("SELECT {columns} FROM {table}"))
            .map_err(|e| PgWireError::UserError(Box::new(error_info(&e.to_string()))))?;
        let header = Arc::new(build_field_info(&stmt, &Format::UnifiedText));
        let column_count = header.len();
        let mut rows = Vec::new();
        stmt.run_with_row_callback(|row| {
            let mut output = Vec::new();
            for (index, value) in row.get_values().enumerate() {
                if index > 0 {
                    output.push(b'\t');
                }
                if matches!(value, Value::Null) {
                    output.extend_from_slice(b"\\N");
                    continue;
                }
                let mut encoder = DataRowEncoder::new(Arc::new(vec![header[index].clone()]));
                encode_value(&mut encoder, value, header[index].datatype())?;
                let data_row = encoder
                    .finish()
                    .map_err(|e| turso_core::LimboError::InternalError(e.to_string()))?;
                let mut data = data_row.data.freeze();
                let length = data.get_i32();
                assert!(length >= 0);
                for byte in &data[..length as usize] {
                    match byte {
                        b'\\' => output.extend_from_slice(b"\\\\"),
                        b'\t' => output.extend_from_slice(b"\\t"),
                        b'\n' => output.extend_from_slice(b"\\n"),
                        b'\r' => output.extend_from_slice(b"\\r"),
                        byte => output.push(*byte),
                    }
                }
            }
            output.push(b'\n');
            rows.push(output);
            Ok(())
        })
        .map_err(|e| PgWireError::UserError(Box::new(error_info(&e.to_string()))))?;
        Ok(CopyOutput {
            columns: column_count,
            row_count: rows.len(),
            rows,
        })
    }
}

#[async_trait]
impl ExtendedQueryHandler for TursoPgHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_query<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let conn = self.conn.lock().unwrap().clone();
        let query = &portal.statement.statement;

        let mut stmt = conn
            .prepare(query)
            .map_err(|e| PgWireError::UserError(Box::new(error_info(&e.to_string()))))?;

        // Clean up schema file after successful DROP SCHEMA
        self.cleanup_dropped_schema_file(query);

        // Bind parameters from the portal
        bind_portal_parameters(&mut stmt, portal)?;

        if stmt.num_columns() == 0 || is_pg_non_query(query) {
            let response = execute_non_query(&mut stmt, query)?;
            if conn.inner().get_auto_commit() {
                self.cursors.lock().unwrap().clear();
            }
            return Ok(response);
        }

        let header = Arc::new(build_field_info(&stmt, &portal.result_column_format));
        execute_query(&mut stmt, header)
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let conn = self.conn.lock().unwrap().clone();
        let stmt = conn
            .prepare(&target.statement)
            .map_err(|e| PgWireError::UserError(Box::new(error_info(&e.to_string()))))?;

        let param_types: Vec<Type> = target
            .parameter_types
            .iter()
            .map(|t| t.clone().unwrap_or(Type::TEXT))
            .collect();

        let fields = build_field_info(&stmt, &Format::UnifiedText);
        Ok(DescribeStatementResponse::new(param_types, fields))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let conn = self.conn.lock().unwrap().clone();
        let stmt = conn
            .prepare(&portal.statement.statement)
            .map_err(|e| PgWireError::UserError(Box::new(error_info(&e.to_string()))))?;

        let fields = build_field_info(&stmt, &portal.result_column_format);
        Ok(DescribePortalResponse::new(fields))
    }
}

/// Build FieldInfo metadata from a prepared statement's column information.
fn build_field_info(stmt: &turso_core::Statement, format: &Format) -> Vec<FieldInfo> {
    if is_pg_non_query(stmt.get_sql()) {
        return vec![];
    }
    (0..stmt.num_columns())
        .map(|i| {
            let name = stmt.get_column_name(i).into_owned();
            let pg_type = resolve_pg_type_for_column(stmt, i);
            FieldInfo::new(name, None, None, pg_type, format.format_for(i))
        })
        .collect()
}

/// Decide the PG wire type for a result column.
///
/// `get_column_type_info` is the single source of truth: it handles direct
/// table-column references (declared name, array depth, custom-type kind,
/// resolved primitive), bare literals (`SELECT 42` -> INTEGER), and typed
/// expressions like CAST. When it returns `Ok(None)` (no determined primitive)
/// or `Err` (custom types not enabled — won't happen in PG mode, but the wire
/// layer shouldn't panic if it does), the safe default is TEXT;
/// `encode_value` already handles per-value type mismatches.
fn resolve_pg_type_for_column(stmt: &turso_core::Statement, idx: usize) -> Type {
    use turso_core::ColumnTypeKind;

    let Some(info) = stmt.get_column_type_info(idx).ok().flatten() else {
        return Type::TEXT;
    };
    // STRUCT and UNION columns live as BLOBs on disk, but exposing them as
    // BYTEA would force clients to deal with raw bytes. Map them to JSONB so
    // libpq/psql/JDBC see structured data they can introspect.
    let mut base = match info.kind {
        ColumnTypeKind::Struct | ColumnTypeKind::Union => Type::JSONB,
        _ => {
            // Prefer the declared name (the user-visible type), then fall
            // back to the resolved base for custom/domain types whose
            // declared name isn't in the lookup table.
            let mapped = sqlite_type_to_pg_type(&info.declared_name);
            if mapped == Type::TEXT {
                info.base_type
                    .as_deref()
                    .map(sqlite_type_to_pg_type)
                    .unwrap_or(Type::TEXT)
            } else {
                mapped
            }
        }
    };
    if info.array_dimensions > 0 {
        base = scalar_pg_type_to_array_type(&base);
    }
    base
}

/// Map a scalar PG type to its array counterpart.
fn scalar_pg_type_to_array_type(scalar: &Type) -> Type {
    if *scalar == Type::INT4 {
        Type::INT4_ARRAY
    } else if *scalar == Type::INT8 {
        Type::INT8_ARRAY
    } else if *scalar == Type::FLOAT8 {
        Type::FLOAT8_ARRAY
    } else if *scalar == Type::BOOL {
        Type::BOOL_ARRAY
    } else if *scalar == Type::TEXT || *scalar == Type::VARCHAR {
        Type::TEXT_ARRAY
    } else if *scalar == Type::UUID {
        Type::UUID_ARRAY
    } else if *scalar == Type::JSON {
        Type::JSON_ARRAY
    } else if *scalar == Type::JSONB {
        Type::JSONB_ARRAY
    } else if *scalar == Type::DATE {
        Type::DATE_ARRAY
    } else if *scalar == Type::TIME {
        Type::TIME_ARRAY
    } else if *scalar == Type::TIMESTAMP {
        Type::TIMESTAMP_ARRAY
    } else if *scalar == Type::TIMESTAMPTZ {
        Type::TIMESTAMPTZ_ARRAY
    } else if *scalar == Type::INET {
        Type::INET_ARRAY
    } else if *scalar == Type::CIDR {
        Type::CIDR_ARRAY
    } else if *scalar == Type::MACADDR {
        Type::MACADDR_ARRAY
    } else if *scalar == Type::MACADDR8 {
        Type::MACADDR8_ARRAY
    } else if *scalar == Type::NUMERIC {
        Type::NUMERIC_ARRAY
    } else if *scalar == Type::BYTEA {
        Type::BYTEA_ARRAY
    } else if *scalar == Type::FLOAT4 {
        Type::FLOAT4_ARRAY
    } else {
        Type::TEXT_ARRAY
    }
}

/// Execute a query that returns rows and build a Query response.
fn execute_query(
    stmt: &mut turso_core::Statement,
    header: Arc<Vec<FieldInfo>>,
) -> PgWireResult<Response> {
    let mut rows: Vec<PgWireResult<DataRow>> = Vec::new();
    let header_clone = header.clone();

    stmt.run_with_row_callback(|row| {
        let mut encoder = DataRowEncoder::new(header_clone.clone());
        for (i, val) in row.get_values().enumerate() {
            let pg_type = header_clone
                .get(i)
                .map(|fi| fi.datatype().clone())
                .unwrap_or(Type::TEXT);
            encode_value(&mut encoder, val, &pg_type)?;
        }
        rows.push(encoder.finish());
        Ok(())
    })
    .map_err(|e| PgWireError::UserError(Box::new(error_info(&e.to_string()))))?;

    let data_stream = stream::iter(rows);
    Ok(Response::Query(QueryResponse::new(header, data_stream)))
}

/// Execute a non-SELECT statement and build an Execution response.
fn execute_non_query(stmt: &mut turso_core::Statement, query: &str) -> PgWireResult<Response> {
    stmt.run_ignore_rows()
        .map_err(|e| PgWireError::UserError(Box::new(error_info(&e.to_string()))))?;

    let affected = stmt.n_change();
    let tag = command_tag(query, affected as usize);
    Ok(Response::Execution(tag))
}

/// Extract parameters from a Portal and bind them to a prepared statement.
///
/// PostgreSQL parameters ($1, $2, ...) map to portal parameters 0, 1, ...
/// The bytecode compiler may allocate internal parameter indices in a different
/// order than the $N numbering (e.g. if $2 appears before $1 in the SQL), so we
/// look up each parameter's internal index by name.
fn bind_portal_parameters(
    stmt: &mut turso_core::Statement,
    portal: &Portal<String>,
) -> PgWireResult<()> {
    for i in 0..portal.parameter_len() {
        let value = match &portal.parameters[i] {
            None => Value::Null,
            Some(bytes) => {
                let pg_type = portal
                    .statement
                    .parameter_types
                    .get(i)
                    .and_then(|t| t.as_ref())
                    .unwrap_or(&Type::UNKNOWN);
                pg_bytes_to_value(bytes, pg_type)?
            }
        };
        // Portal parameter i corresponds to PostgreSQL $N where N = i + 1.
        // Look up the internal index that the bytecode compiler assigned to $N.
        let pg_param_name = format!("${}", i + 1);
        let idx = stmt
            .parameter_index(&pg_param_name)
            .unwrap_or_else(|| NonZero::new(i + 1).expect("parameter index must be non-zero"));
        // Ignore bind errors: parameter index mismatches or value coercion
        // failures surface as wire-protocol errors during the subsequent
        // execute, with a more useful message than a generic Bind failure.
        let _ = stmt.bind_at(idx, value);
    }
    Ok(())
}

/// Convert raw parameter bytes to a turso Value based on the PostgreSQL type.
/// Assumes text format encoding (UTF-8 string representations).
fn pg_bytes_to_value(bytes: &[u8], pg_type: &Type) -> PgWireResult<Value> {
    let text = std::str::from_utf8(bytes).map_err(|e| {
        PgWireError::UserError(Box::new(error_info(&format!(
            "invalid UTF-8 in parameter: {e}"
        ))))
    })?;

    match *pg_type {
        Type::INT2 | Type::INT4 | Type::INT8 => {
            let i: i64 = text.parse().map_err(|e| {
                PgWireError::UserError(Box::new(error_info(&format!(
                    "invalid integer parameter: {e}"
                ))))
            })?;
            Ok(Value::from_i64(i))
        }
        Type::FLOAT4 | Type::FLOAT8 | Type::NUMERIC => {
            let f: f64 = text.parse().map_err(|e| {
                PgWireError::UserError(Box::new(error_info(&format!(
                    "invalid float parameter: {e}"
                ))))
            })?;
            Ok(Value::from_f64(f))
        }
        Type::BOOL => match text {
            "t" | "true" | "TRUE" | "1" | "yes" | "on" => Ok(Value::from_i64(1)),
            "f" | "false" | "FALSE" | "0" | "no" | "off" => Ok(Value::from_i64(0)),
            _ => Err(PgWireError::UserError(Box::new(error_info(&format!(
                "invalid boolean parameter: {text}"
            ))))),
        },
        Type::BYTEA => {
            // PostgreSQL text format for bytea uses \x hex encoding
            if let Some(hex_str) = text.strip_prefix("\\x") {
                let data = decode_hex(hex_str).map_err(|e| {
                    PgWireError::UserError(Box::new(error_info(&format!(
                        "invalid bytea hex parameter: {e}"
                    ))))
                })?;
                Ok(Value::from_blob(data))
            } else {
                // Raw bytes as-is
                Ok(Value::from_blob(bytes.to_vec()))
            }
        }
        // UNKNOWN: try to infer type from text content (numeric-looking values
        // should be bound as numbers so comparisons with COUNT/SUM etc. work)
        Type::UNKNOWN => {
            if let Ok(i) = text.parse::<i64>() {
                Ok(Value::from_i64(i))
            } else if let Ok(f) = text.parse::<f64>() {
                Ok(Value::from_f64(f))
            } else if text.eq_ignore_ascii_case("true") || text.eq_ignore_ascii_case("t") {
                Ok(Value::from_i64(1))
            } else if text.eq_ignore_ascii_case("false") || text.eq_ignore_ascii_case("f") {
                Ok(Value::from_i64(0))
            } else {
                Ok(Value::from_text(text.to_owned()))
            }
        }
        // TEXT, VARCHAR, and all other types → text
        _ => Ok(Value::from_text(text.to_owned())),
    }
}

/// Decode a hex string into bytes.
fn decode_hex(hex: &str) -> Result<Vec<u8>, String> {
    if hex.len() % 2 != 0 {
        return Err("odd-length hex string".to_owned());
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&hex[i..i + 2], 16)
                .map_err(|e| format!("invalid hex at position {i}: {e}"))
        })
        .collect()
}

fn encode_value(
    encoder: &mut DataRowEncoder,
    val: &Value,
    pg_type: &Type,
) -> turso_core::Result<()> {
    match val {
        Value::Null => encoder
            .encode_field(&None::<i8>)
            .map_err(|e| turso_core::LimboError::InternalError(e.to_string())),
        Value::Numeric(turso_core::Numeric::Integer(i)) => {
            // Boolean columns: encode as true/false instead of 0/1
            if *pg_type == Type::BOOL {
                encoder
                    .encode_field(&(*i != 0))
                    .map_err(|e| turso_core::LimboError::InternalError(e.to_string()))
            } else {
                encoder
                    .encode_field(i)
                    .map_err(|e| turso_core::LimboError::InternalError(e.to_string()))
            }
        }
        Value::Numeric(turso_core::Numeric::Float(f)) => encoder
            .encode_field(&f64::from(*f))
            .map_err(|e| turso_core::LimboError::InternalError(e.to_string())),
        Value::Text(t) => {
            let text = t.value.as_ref();
            // For TIMESTAMPTZ columns, ensure timezone info is present so clients
            // parse the value correctly (as UTC, not local time).
            // TIMESTAMP (without TZ) should NOT have timezone suffix.
            if *pg_type == Type::TIMESTAMPTZ
                && !text.contains('+')
                && !text.contains('Z')
                && !text.ends_with("-00")
            {
                let with_tz = format!("{text}+00");
                encoder
                    .encode_field(&with_tz.as_str())
                    .map_err(|e| turso_core::LimboError::InternalError(e.to_string()))
            } else if pg_type.name().starts_with('_') {
                // Array types: pgwire's to_sql_text quotes strings containing
                // {, }, or commas when the type is Kind::Array. Since we store
                // array values as pre-formatted PG array literals (e.g.
                // "{1,2,3}"), encode with Type::TEXT to bypass the quoting.
                encoder
                    .encode_field_with_type_and_format(
                        &text,
                        &Type::TEXT,
                        FieldFormat::Text,
                        &FormatOptions::default(),
                    )
                    .map_err(|e| turso_core::LimboError::InternalError(e.to_string()))
            } else {
                encoder
                    .encode_field(&text)
                    .map_err(|e| turso_core::LimboError::InternalError(e.to_string()))
            }
        }
        Value::Blob(b) => encoder
            .encode_field(&b.as_slice())
            .map_err(|e| turso_core::LimboError::InternalError(e.to_string())),
    }
}

fn sqlite_type_to_pg_type(type_str: &str) -> Type {
    let upper = type_str.to_uppercase();
    match upper.as_str() {
        "INTEGER" | "INT" | "INT4" | "SMALLINT" | "INT2" | "SERIAL" | "SMALLSERIAL" => Type::INT4,
        "BIGINT" | "INT8" | "BIGSERIAL" => Type::INT8,
        "REAL" | "FLOAT" | "FLOAT4" | "FLOAT8" | "DOUBLE" | "DOUBLE PRECISION" | "NUMERIC"
        | "DECIMAL" => Type::FLOAT8,
        "TEXT" | "VARCHAR" | "CHAR" | "CHARACTER VARYING" | "CHARACTER" | "NAME" => Type::TEXT,
        "BLOB" | "BYTEA" => Type::BYTEA,
        "BOOLEAN" | "BOOL" => Type::BOOL,
        "UUID" => Type::UUID,
        "JSON" => Type::JSON,
        "JSONB" => Type::JSONB,
        "DATE" => Type::DATE,
        "TIME" | "TIMETZ" => Type::TIME,
        "TIMESTAMP" => Type::TIMESTAMP,
        "TIMESTAMPTZ" => Type::TIMESTAMPTZ,
        "INET" => Type::INET,
        "CIDR" => Type::CIDR,
        "MACADDR" => Type::MACADDR,
        "MACADDR8" => Type::MACADDR8,
        _ => {
            // Handle parameterized types like varchar(50), numeric(10,2)
            if upper.starts_with("VARCHAR") || upper.starts_with("CHAR") {
                Type::VARCHAR
            } else if upper.starts_with("NUMERIC") || upper.starts_with("DECIMAL") {
                Type::NUMERIC
            } else {
                Type::TEXT
            }
        }
    }
}

/// PG statements handled by `try_prepare_pg()` that return a dummy SELECT
/// but should produce a command-tag response, not a result set.
fn is_pg_non_query(sql: &str) -> bool {
    let upper = sql.trim().to_uppercase();
    upper.starts_with("COPY")
        || upper.starts_with("SET")
        || upper.starts_with("LOCK")
        || upper.starts_with("CREATE SCHEMA")
        || upper.starts_with("DROP SCHEMA")
        || upper.starts_with("REFRESH MATERIALIZED VIEW")
        || upper.starts_with("COMMENT")
}

fn command_tag(query: &str, affected_rows: usize) -> Tag {
    let upper = query.trim().to_uppercase();
    if upper.starts_with("INSERT") {
        Tag::new("INSERT").with_oid(0).with_rows(affected_rows)
    } else if upper.starts_with("UPDATE") {
        Tag::new("UPDATE").with_rows(affected_rows)
    } else if upper.starts_with("DELETE") || upper.starts_with("TRUNCATE") {
        Tag::new("DELETE").with_rows(affected_rows)
    } else if upper.starts_with("CREATE VIEW") {
        Tag::new("CREATE VIEW")
    } else if upper.starts_with("CREATE INDEX") {
        Tag::new("CREATE INDEX")
    } else if upper.starts_with("CREATE SCHEMA") {
        Tag::new("CREATE SCHEMA")
    } else if is_create_table_as(&upper) {
        // PostgreSQL reports CREATE TABLE AS completion as `SELECT n` (the
        // rows inserted), except WITH NO DATA which skips the insert and
        // keeps the plain tag.
        if ends_with_with_no_data(&upper) {
            Tag::new("CREATE TABLE AS")
        } else {
            Tag::new("SELECT").with_rows(affected_rows)
        }
    } else if upper.starts_with("CREATE") {
        Tag::new("CREATE TABLE")
    } else if upper.starts_with("DROP VIEW") {
        Tag::new("DROP VIEW")
    } else if upper.starts_with("DROP INDEX") {
        Tag::new("DROP INDEX")
    } else if upper.starts_with("DROP SCHEMA") {
        Tag::new("DROP SCHEMA")
    } else if upper.starts_with("DROP") {
        Tag::new("DROP TABLE")
    } else if upper.starts_with("ALTER") {
        Tag::new("ALTER TABLE")
    } else if upper.starts_with("BEGIN") || upper.starts_with("START") {
        Tag::new("BEGIN")
    } else if upper.starts_with("COMMIT") {
        Tag::new("COMMIT")
    } else if upper.starts_with("ROLLBACK") {
        Tag::new("ROLLBACK")
    } else if upper.starts_with("SAVEPOINT") {
        Tag::new("SAVEPOINT")
    } else if upper.starts_with("RELEASE") {
        Tag::new("RELEASE")
    } else if upper.starts_with("SET") {
        Tag::new("SET")
    } else if upper.starts_with("LOCK") {
        Tag::new("LOCK TABLE")
    } else if upper.starts_with("COPY") {
        Tag::new("COPY").with_rows(affected_rows)
    } else if upper.starts_with("COMMENT") {
        Tag::new("COMMENT")
    } else if upper.starts_with("SELECT") || upper.starts_with("WITH") {
        // Row-returning SELECTs never reach command_tag (they take the
        // query-response path), so a zero-column SELECT- or WITH-prefixed
        // statement is SELECT ... INTO (writable CTEs are unsupported),
        // which PostgreSQL reports as `SELECT n` like CREATE TABLE AS.
        Tag::new("SELECT").with_rows(affected_rows)
    } else {
        Tag::new("OK")
    }
}

/// Whether the statement ends with `WITH NO DATA`, token-wise (ignoring
/// trailing whitespace and statement terminators).
fn ends_with_with_no_data(upper: &str) -> bool {
    let mut tokens = upper
        .trim_end()
        .trim_end_matches(';')
        .split_whitespace()
        .rev();
    tokens.next() == Some("DATA") && tokens.next() == Some("NO") && tokens.next() == Some("WITH")
}

/// Best-effort detection of `CREATE [TEMP|UNLOGGED] TABLE [IF NOT EXISTS]
/// <name> AS ...` from the statement text, in the same spirit as the prefix
/// matching in `command_tag`. Quoted table names containing whitespace are
/// not recognized and fall back to the plain CREATE TABLE tag.
fn is_create_table_as(upper: &str) -> bool {
    let mut tokens = upper.split_whitespace();
    if tokens.next() != Some("CREATE") {
        return false;
    }
    let mut tok = tokens.next();
    while matches!(
        tok,
        Some("TEMP" | "TEMPORARY" | "UNLOGGED" | "GLOBAL" | "LOCAL")
    ) {
        tok = tokens.next();
    }
    if tok != Some("TABLE") {
        return false;
    }
    tok = tokens.next();
    if tok == Some("IF") {
        if tokens.next() != Some("NOT") || tokens.next() != Some("EXISTS") {
            return false;
        }
        tok = tokens.next();
    }
    // `tok` is the table name; AS must follow it (possibly fused with an
    // opening parenthesis, as in `AS(SELECT 1)`).
    let Some(name) = tok else {
        return false;
    };
    // A quoted name that opens without closing in the same token spans
    // whitespace, so the next token is part of the name, not AS.
    if name.starts_with('"') && (name.len() == 1 || !name.ends_with('"')) {
        return false;
    }
    matches!(tokens.next(), Some(t) if t == "AS" || t.starts_with("AS("))
}

fn error_info(message: &str) -> ErrorInfo {
    ErrorInfo::new("ERROR".to_owned(), "XX000".to_owned(), message.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use turso_pg_client::{BackendEvent, ConnParams, PgConn};

    fn test_handler() -> TursoPgHandler {
        let (_io, db) = turso_pg::open_database(
            ":memory:",
            None,
            turso_pg::OpenFlags::default(),
            turso_pg::DatabaseOpts::new(),
        )
        .unwrap();
        let conn = turso_pg::PgConnection::connect(&db).unwrap();
        TursoPgHandler {
            conn: Arc::new(Mutex::new(conn)),
            db_file: ":memory:".to_owned(),
            query_parser: Arc::new(NoopQueryParser::new()),
            cursors: Mutex::new(HashMap::new()),
        }
    }

    fn protocol_client() -> PgConn {
        let (address_sender, address_receiver) = mpsc::channel();
        std::thread::spawn(move || {
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(async move {
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    address_sender.send(listener.local_addr().unwrap()).unwrap();
                    let (socket, _) = listener.accept().await.unwrap();
                    let factory = Arc::new(TursoPgFactory {
                        handler: Arc::new(test_handler()),
                    });
                    process_socket(socket, None, factory).await.unwrap();
                });
        });
        let address = address_receiver.recv().unwrap();
        PgConn::connect(
            &ConnParams {
                host: address.ip().to_string(),
                port: address.port(),
                user: "postgres".to_owned(),
                password: None,
                database: "postgres".to_owned(),
            },
            &[],
        )
        .unwrap()
    }

    fn command_complete(body: &[u8]) -> String {
        String::from_utf8(body[..body.len() - 1].to_vec()).unwrap()
    }

    #[test]
    fn test_sequence_dump_protocol_uses_persisted_state_and_boolean_text() {
        let mut client = protocol_client();
        let setup = client.simple_query("CREATE SEQUENCE dump_counter START 41 INCREMENT 3 MAXVALUE 999; SELECT set_config('search_path','',false)").unwrap();
        assert!(
            !setup
                .iter()
                .any(|event| matches!(event, BackendEvent::ErrorResponse(_))),
            "{setup:?}"
        );
        for expected in [["41", "f"], ["44", "t"]] {
            let events = client
                .simple_query("SELECT last_value,is_called FROM public.dump_counter")
                .unwrap();
            assert_eq!(
                events.iter().find_map(|event| match event {
                    BackendEvent::RowDescription(columns) => Some(
                        columns
                            .iter()
                            .map(|column| column.type_oid)
                            .collect::<Vec<_>>()
                    ),
                    _ => None,
                }),
                Some(vec![20, 16])
            );
            assert_eq!(
                events
                    .iter()
                    .filter_map(|event| match event {
                        BackendEvent::DataRow(row) => Some(row.clone()),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
                vec![expected.map(|value| Some(value.to_string())).to_vec()]
            );
            if expected[1] == "f" {
                client
                    .simple_query("SELECT nextval('dump_counter'); SELECT nextval('dump_counter')")
                    .unwrap();
            }
        }
    }

    #[test]
    fn test_copy_protocol_orders_statements_and_encodes_rows() {
        let mut client = protocol_client();
        client
            .simple_query(
                "CREATE TABLE items (value TEXT); INSERT INTO items VALUES ('one\ttwo'), (NULL)",
            )
            .unwrap();
        client
            .send_query("SELECT 1; COPY items TO STDOUT; SELECT 2")
            .unwrap();

        let mut messages = Vec::new();
        loop {
            let message = client.read_message().unwrap();
            let done = message.0 == b'Z';
            messages.push(message);
            if done {
                break;
            }
        }
        let tags: Vec<u8> = messages.iter().map(|message| message.0).collect();
        assert_eq!(
            tags,
            [b'T', b'D', b'C', b'H', b'd', b'd', b'c', b'C', b'T', b'D', b'C', b'Z']
        );
        assert_eq!(messages[4].1, b"one\\ttwo\n");
        assert_eq!(messages[5].1, b"\\N\n");
        assert_eq!(command_complete(&messages[7].1), "COPY 2");
    }

    #[test]
    fn test_copy_protocol_empty_table_reports_zero_rows() {
        let mut client = protocol_client();
        client
            .simple_query("CREATE TABLE items (value TEXT)")
            .unwrap();
        client.send_query("COPY items TO STDOUT").unwrap();

        let mut messages = Vec::new();
        loop {
            let message = client.read_message().unwrap();
            let done = message.0 == b'Z';
            messages.push(message);
            if done {
                break;
            }
        }
        assert_eq!(
            messages.iter().map(|message| message.0).collect::<Vec<_>>(),
            [b'H', b'c', b'C', b'Z']
        );
        assert_eq!(command_complete(&messages[2].1), "COPY 0");
    }

    #[test]
    fn test_cursor_protocol_batches_exhausts_and_reuses_name() {
        let mut client = protocol_client();
        let values = (1..=205)
            .map(|id| format!("({id}, 'payload-{}')", (id * 17) % 43))
            .collect::<Vec<_>>()
            .join(",");
        client.simple_query(&format!("CREATE TABLE dump_probe (id INTEGER, name TEXT);
            INSERT INTO dump_probe VALUES {values}; CREATE TABLE dump_empty (id INTEGER, name TEXT)")).unwrap();
        let events = client.simple_query("BEGIN; DECLARE _pg_dump_cursor CURSOR FOR SELECT id, name FROM ONLY public.dump_probe ORDER BY id").unwrap();
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, BackendEvent::ErrorResponse(_))),
            "{events:?}"
        );
        assert!(matches!(
            events.last(),
            Some(BackendEvent::ReadyForQuery(b'T'))
        ));
        for (start, count) in [(1, 100), (101, 100), (201, 5), (206, 0), (206, 0)] {
            let events = client
                .simple_query("FETCH 100 FROM _pg_dump_cursor")
                .unwrap();
            let rows: Vec<_> = events
                .iter()
                .filter_map(|event| match event {
                    BackendEvent::DataRow(row) => Some(row.clone()),
                    _ => None,
                })
                .collect();
            let expected: Vec<_> = (start..start + count)
                .map(|id| {
                    vec![
                        Some(id.to_string()),
                        Some(format!("payload-{}", (id * 17) % 43)),
                    ]
                })
                .collect();
            assert_eq!(rows, expected);
            assert!(
                matches!(&events[0], BackendEvent::RowDescription(columns) if columns.len() == 2 && columns[0].name == "id" && columns[1].name == "name")
            );
            assert!(events.iter().any(|event| matches!(event, BackendEvent::CommandComplete(tag) if tag == &format!("FETCH {count}"))), "{events:?}");
            assert!(matches!(
                events.last(),
                Some(BackendEvent::ReadyForQuery(b'T'))
            ));
        }
        let events = client.simple_query("CLOSE _pg_dump_cursor; DECLARE _pg_dump_cursor NO SCROLL CURSOR FOR SELECT id, name FROM ONLY public.dump_empty; FETCH 100 FROM _pg_dump_cursor; CLOSE _pg_dump_cursor; COMMIT; SELECT 47").unwrap();
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, BackendEvent::ErrorResponse(_))),
            "{events:?}"
        );
        assert_eq!(
            events
                .iter()
                .filter_map(|event| match event {
                    BackendEvent::CommandComplete(tag) => Some(tag.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            [
                "CLOSE CURSOR",
                "DECLARE CURSOR",
                "FETCH 0",
                "CLOSE CURSOR",
                "COMMIT",
                "SELECT 1"
            ]
        );
        assert!(matches!(
            events.last(),
            Some(BackendEvent::ReadyForQuery(b'I'))
        ));
    }

    #[test]
    fn test_cursor_requires_transaction_and_closes_at_transaction_end() {
        let mut client = protocol_client();
        let events = client
            .simple_query("DECLARE c CURSOR FOR SELECT 17")
            .unwrap();
        assert!(events
            .iter()
            .any(|event| matches!(event, BackendEvent::ErrorResponse(_))));
        assert!(matches!(
            events.last(),
            Some(BackendEvent::ReadyForQuery(b'I'))
        ));
        let events = client
            .simple_query("BEGIN; DECLARE c CURSOR FOR SELECT 17; COMMIT; BEGIN; FETCH 1 FROM c")
            .unwrap();
        assert!(events.iter().any(|event| matches!(event, BackendEvent::CommandComplete(tag) if tag == "DECLARE CURSOR")), "{events:?}");
        assert!(events
            .iter()
            .any(|event| matches!(event, BackendEvent::ErrorResponse(_))));
        assert!(matches!(
            events.last(),
            Some(BackendEvent::ReadyForQuery(b'E'))
        ));
        let events = client.simple_query("ROLLBACK; BEGIN; DECLARE c CURSOR FOR SELECT 29; FETCH ALL FROM c; CLOSE ALL; COMMIT").unwrap();
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, BackendEvent::ErrorResponse(_))),
            "{events:?}"
        );
        assert!(events.iter().any(|event| matches!(event, BackendEvent::DataRow(row) if row == &vec![Some("29".to_owned())])));
        assert!(matches!(
            events.last(),
            Some(BackendEvent::ReadyForQuery(b'I'))
        ));
    }

    #[test]
    fn test_current_schemas_wire_text_is_a_postgres_array() {
        let mut client = protocol_client();
        client
            .simple_query("SELECT set_config('search_path', '', false)")
            .unwrap();
        let events = client.simple_query("SELECT pg_catalog.current_schemas(false), pg_catalog.current_schemas(true), pg_catalog.current_schemas(NULL)").unwrap();
        assert!(
            events
                .iter()
                .any(|event| matches!(event, BackendEvent::DataRow(row)
            if row == &vec![Some("{}".to_owned()), Some("{pg_catalog}".to_owned()), None])),
            "{events:?}"
        );
        client
            .simple_query("SELECT set_config('search_path', 'missing, public, public', false)")
            .unwrap();
        let events = client
            .simple_query(
                "SELECT pg_catalog.current_schemas(false), pg_catalog.current_schemas(true)",
            )
            .unwrap();
        assert!(
            events
                .iter()
                .any(|event| matches!(event, BackendEvent::DataRow(row)
            if row == &vec![Some("{public}".to_owned()), Some("{pg_catalog,public}".to_owned())])),
            "{events:?}"
        );
    }

    #[test]
    fn test_cursor_errors_preserve_failed_transaction_and_release_names() {
        let mut client = protocol_client();
        let events = client
            .simple_query("BEGIN; DECLARE c CURSOR FOR SELECT * FROM missing_relation")
            .unwrap();
        assert!(events
            .iter()
            .any(|event| matches!(event, BackendEvent::ErrorResponse(_))));
        assert!(matches!(
            events.last(),
            Some(BackendEvent::ReadyForQuery(b'E'))
        ));
        let events = client
            .simple_query(
                "ROLLBACK; BEGIN; DECLARE c CURSOR FOR SELECT 17; DECLARE c CURSOR FOR SELECT 29",
            )
            .unwrap();
        assert!(
            events
                .iter()
                .any(|event| matches!(event, BackendEvent::ErrorResponse(fields)
            if turso_pg_client::error_message(fields).contains("already exists"))),
            "{events:?}"
        );
        let events = client.simple_query("FETCH 1 FROM c").unwrap();
        assert!(events
            .iter()
            .any(|event| matches!(event, BackendEvent::ErrorResponse(fields)
            if turso_pg_client::error_message(fields).contains("aborted"))));
        assert!(matches!(
            events.last(),
            Some(BackendEvent::ReadyForQuery(b'E'))
        ));
        let events = client
            .simple_query(
                "ROLLBACK; BEGIN; DECLARE c CURSOR FOR SELECT 43; FETCH ALL FROM c; COMMIT",
            )
            .unwrap();
        assert!(
            events
                .iter()
                .any(|event| matches!(event, BackendEvent::DataRow(row)
            if row == &vec![Some("43".to_owned())])),
            "{events:?}"
        );
        assert!(matches!(
            events.last(),
            Some(BackendEvent::ReadyForQuery(b'I'))
        ));
    }

    #[test]
    fn test_rollback_to_keeps_transaction_status() {
        let mut client = protocol_client();
        let events = client
            .simple_query("BEGIN; SAVEPOINT before_insert; ROLLBACK TO before_insert")
            .unwrap();
        assert!(matches!(
            events.last(),
            Some(BackendEvent::ReadyForQuery(b'T'))
        ));
        assert!(matches!(
            client.simple_query("ROLLBACK").unwrap().last(),
            Some(BackendEvent::ReadyForQuery(b'I'))
        ));
    }

    #[test]
    fn test_error_after_begin_reports_failed_transaction_and_stops_batch() {
        let mut client = protocol_client();
        client
            .simple_query("CREATE TABLE items (value TEXT)")
            .unwrap();
        let events = client
            .simple_query("BEGIN; SELECT * FROM missing_table; COPY items TO STDOUT")
            .unwrap();
        assert!(
            matches!(events.first(), Some(BackendEvent::CommandComplete(tag)) if tag == "BEGIN")
        );
        assert!(events
            .iter()
            .any(|event| matches!(event, BackendEvent::ErrorResponse(_))));
        assert!(matches!(
            events.last(),
            Some(BackendEvent::ReadyForQuery(b'E'))
        ));
        assert!(!events.iter().any(
            |event| matches!(event, BackendEvent::CommandComplete(tag) if tag.starts_with("COPY"))
        ));
        assert!(!events
            .iter()
            .any(|event| matches!(event, BackendEvent::Other(b'H' | b'd' | b'c'))));
    }

    #[test]
    fn test_copy_to_stdout_text_rows() {
        let handler = test_handler();
        let conn = handler.conn.lock().unwrap().clone();
        conn.execute(
            "CREATE TABLE items (a TEXT, b TEXT, n BIGINT, ok BOOLEAN, f REAL, raw BYTEA)",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO items VALUES ('', NULL, 9223372036854775807, 1, 1.25, unhex('00ff'))",
        )
        .unwrap();
        conn.execute("INSERT INTO items VALUES ('\\N', 'one\ttwo\nthree\rfour\\five', -2, 0, -0.5, unhex('5c2e'))")
            .unwrap();
        let parsed = turso_pg_parser::parse("COPY items (b, a, n, ok, f, raw) TO STDOUT").unwrap();
        let copy = turso_pg_parser::translator::try_extract_copy_to(&parsed)
            .unwrap()
            .unwrap();
        let output = handler.copy_to_stdout(&copy).unwrap();
        assert_eq!(output.columns, 6);
        assert_eq!(output.row_count, 2);
        assert_eq!(
            output.rows,
            vec![
                b"\\N\t\t9223372036854775807\tt\t1.25\t\\\\x00ff\n".to_vec(),
                b"one\\ttwo\\nthree\\rfour\\\\five\t\\\\N\t-2\tf\t-0.5\t\\\\x5c2e\n".to_vec(),
            ]
        );
    }

    #[test]
    fn test_copy_to_stdout_empty_table() {
        let handler = test_handler();
        handler
            .conn
            .lock()
            .unwrap()
            .execute("CREATE TABLE empty_items (value TEXT)")
            .unwrap();
        let parsed = turso_pg_parser::parse("COPY empty_items TO STDOUT").unwrap();
        let copy = turso_pg_parser::translator::try_extract_copy_to(&parsed)
            .unwrap()
            .unwrap();
        let output = handler.copy_to_stdout(&copy).unwrap();
        assert_eq!(output.columns, 1);
        assert_eq!(output.row_count, 0);
        assert!(output.rows.is_empty());
    }

    #[test]
    fn test_pg_is_in_recovery_returns_true_text_on_wire() {
        let (_io, db) = turso_pg::open_database(
            ":memory:",
            None,
            turso_pg::OpenFlags::default(),
            turso_pg::DatabaseOpts::new(),
        )
        .unwrap();
        let conn = Connection::connect(&db).unwrap();
        for sql in [
            "SELECT pg_catalog.pg_is_in_recovery()",
            "SELECT pg_is_in_recovery() AS recovery",
            "SELECT (PG_IS_IN_RECOVERY())",
        ] {
            for format in [Format::UnifiedText, Format::UnifiedBinary] {
                let mut stmt = conn.prepare(sql).unwrap();
                let fields = Arc::new(build_field_info(&stmt, &format));
                assert_eq!(*fields[0].datatype(), Type::TEXT, "{sql}");
                let rows = stmt.run_collect_rows().unwrap();
                assert_eq!(rows.len(), 1);
                let mut encoder = DataRowEncoder::new(fields.clone());
                encode_value(&mut encoder, &rows[0][0], fields[0].datatype()).unwrap();
                let row = encoder.finish().unwrap();
                assert_eq!(row.field_count, 1);
                assert_eq!(&row.data[..], b"\0\0\0\x01t");
            }
        }
        let stmt = conn.prepare("SELECT 7 AS pg_is_in_recovery").unwrap();
        let fields = build_field_info(&stmt, &Format::UnifiedText);
        assert_eq!(*fields[0].datatype(), Type::INT4);
    }

    #[test]
    fn test_set_transaction_is_non_query() {
        assert!(is_pg_non_query(
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
        ));
        assert!(is_pg_non_query("  set\ntransaction read only"));
        assert!(!is_pg_non_query(
            "SELECT set_config('search_path', '', false)"
        ));
    }

    #[test]
    fn test_access_share_lock_is_non_query() {
        assert!(is_pg_non_query(
            "LOCK TABLE public.items IN ACCESS SHARE MODE"
        ));
        assert!(is_pg_non_query("  lock\ntable items in access share mode"));
        assert!(!is_pg_non_query("SELECT 'LOCK TABLE items'"));
    }

    #[test]
    fn test_access_share_lock_command_tag() {
        assert_eq!(
            command_tag("LOCK TABLE items IN ACCESS SHARE MODE", 0),
            Tag::new("LOCK TABLE")
        );
    }

    #[test]
    fn test_access_share_lock_has_no_result_fields() {
        let (_io, db) = turso_pg::open_database(
            ":memory:",
            None,
            turso_pg::OpenFlags::default(),
            turso_pg::DatabaseOpts::new(),
        )
        .unwrap();
        let conn = turso_pg::PgConnection::connect(&db).unwrap();
        conn.execute("CREATE TABLE items (v INTEGER)").unwrap();
        let stmt = conn
            .prepare("LOCK TABLE items IN ACCESS SHARE MODE")
            .unwrap();
        for format in [Format::UnifiedText, Format::UnifiedBinary] {
            assert!(build_field_info(&stmt, &format).is_empty());
        }
        let query = conn.prepare("SELECT 'LOCK TABLE items'").unwrap();
        assert_eq!(build_field_info(&query, &Format::UnifiedText).len(), 1);
    }

    #[test]
    fn test_pg_bytes_to_value_integer() {
        let val = pg_bytes_to_value(b"42", &Type::INT4).unwrap();
        assert_eq!(val, Value::from_i64(42));

        let val = pg_bytes_to_value(b"-100", &Type::INT8).unwrap();
        assert_eq!(val, Value::from_i64(-100));

        let val = pg_bytes_to_value(b"0", &Type::INT2).unwrap();
        assert_eq!(val, Value::from_i64(0));
    }

    #[test]
    fn test_pg_bytes_to_value_float() {
        let val = pg_bytes_to_value(b"3.25", &Type::FLOAT8).unwrap();
        assert_eq!(val, Value::from_f64(3.25));

        let val = pg_bytes_to_value(b"-0.5", &Type::FLOAT4).unwrap();
        assert_eq!(val, Value::from_f64(-0.5));

        let val = pg_bytes_to_value(b"1.23", &Type::NUMERIC).unwrap();
        assert_eq!(val, Value::from_f64(1.23));
    }

    #[test]
    fn test_pg_bytes_to_value_bool() {
        let val = pg_bytes_to_value(b"t", &Type::BOOL).unwrap();
        assert_eq!(val, Value::from_i64(1));

        let val = pg_bytes_to_value(b"f", &Type::BOOL).unwrap();
        assert_eq!(val, Value::from_i64(0));

        let val = pg_bytes_to_value(b"true", &Type::BOOL).unwrap();
        assert_eq!(val, Value::from_i64(1));

        let val = pg_bytes_to_value(b"false", &Type::BOOL).unwrap();
        assert_eq!(val, Value::from_i64(0));
    }

    #[test]
    fn test_pg_bytes_to_value_text() {
        let val = pg_bytes_to_value(b"hello world", &Type::TEXT).unwrap();
        assert_eq!(val, Value::from_text("hello world".to_owned()));

        let val = pg_bytes_to_value(b"Alice", &Type::VARCHAR).unwrap();
        assert_eq!(val, Value::from_text("Alice".to_owned()));
    }

    #[test]
    fn test_pg_bytes_to_value_bytea() {
        let val = pg_bytes_to_value(b"\\xDEADBEEF", &Type::BYTEA).unwrap();
        assert_eq!(val, Value::from_blob(vec![0xDE, 0xAD, 0xBE, 0xEF]));
    }

    #[test]
    fn test_pg_bytes_to_value_unknown_type_as_text() {
        // Unknown types should be treated as text
        let val = pg_bytes_to_value(b"some-uuid-value", &Type::UUID).unwrap();
        assert_eq!(val, Value::from_text("some-uuid-value".to_owned()));
    }

    #[test]
    fn test_pg_bytes_to_value_integer_parse_error() {
        let result = pg_bytes_to_value(b"not_a_number", &Type::INT4);
        assert!(result.is_err());
    }

    #[test]
    fn test_pg_bytes_to_value_float_parse_error() {
        let result = pg_bytes_to_value(b"not_a_float", &Type::FLOAT8);
        assert!(result.is_err());
    }

    #[test]
    fn test_pg_bytes_to_value_bool_invalid() {
        let result = pg_bytes_to_value(b"maybe", &Type::BOOL);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_hex() {
        assert_eq!(
            decode_hex("DEADBEEF").unwrap(),
            vec![0xDE, 0xAD, 0xBE, 0xEF]
        );
        assert_eq!(decode_hex("00ff").unwrap(), vec![0x00, 0xFF]);
        assert_eq!(decode_hex("").unwrap(), Vec::<u8>::new());
        assert!(decode_hex("0").is_err()); // odd length
        assert!(decode_hex("GG").is_err()); // invalid hex
    }

    #[test]
    fn test_sqlite_type_to_pg_type() {
        assert_eq!(sqlite_type_to_pg_type("INTEGER"), Type::INT4);
        assert_eq!(sqlite_type_to_pg_type("INT"), Type::INT4);
        assert_eq!(sqlite_type_to_pg_type("INT4"), Type::INT4);
        assert_eq!(sqlite_type_to_pg_type("SMALLINT"), Type::INT4);
        assert_eq!(sqlite_type_to_pg_type("BIGINT"), Type::INT8);
        assert_eq!(sqlite_type_to_pg_type("INT8"), Type::INT8);
        assert_eq!(sqlite_type_to_pg_type("REAL"), Type::FLOAT8);
        assert_eq!(sqlite_type_to_pg_type("TEXT"), Type::TEXT);
        assert_eq!(sqlite_type_to_pg_type("BLOB"), Type::BYTEA);
        assert_eq!(sqlite_type_to_pg_type("BOOLEAN"), Type::BOOL);
        assert_eq!(sqlite_type_to_pg_type("TIMESTAMP"), Type::TIMESTAMP);
        assert_eq!(sqlite_type_to_pg_type("TIMESTAMPTZ"), Type::TIMESTAMPTZ);
        assert_eq!(sqlite_type_to_pg_type("DATE"), Type::DATE);
        assert_eq!(sqlite_type_to_pg_type("JSON"), Type::JSON);
        assert_eq!(sqlite_type_to_pg_type("JSONB"), Type::JSONB);
        assert_eq!(sqlite_type_to_pg_type("UUID"), Type::UUID);
        // Unknown types map to TEXT
        assert_eq!(sqlite_type_to_pg_type("UNKNOWN"), Type::TEXT);
    }

    #[test]
    fn test_unknown_type_inference() {
        // UNKNOWN type should infer integers from numeric-looking strings
        let val = pg_bytes_to_value(b"42", &Type::UNKNOWN).unwrap();
        assert!(matches!(
            val,
            Value::Numeric(turso_core::Numeric::Integer(42))
        ));

        // UNKNOWN type should infer floats
        let val = pg_bytes_to_value(b"3.14", &Type::UNKNOWN).unwrap();
        if let Value::Numeric(turso_core::Numeric::Float(f)) = val {
            #[allow(clippy::approx_constant)]
            let expected = 3.14;
            assert!((f64::from(f) - expected).abs() < 0.001);
        } else {
            panic!("Expected Float");
        }

        // UNKNOWN type should keep text for non-numeric strings
        let val = pg_bytes_to_value(b"hello", &Type::UNKNOWN).unwrap();
        assert!(matches!(val, Value::Text(_)));
    }

    #[test]
    fn test_is_create_table_as() {
        assert!(is_create_table_as("CREATE TABLE T AS SELECT 1"));
        assert!(is_create_table_as("CREATE TEMP TABLE T AS SELECT 1"));
        assert!(is_create_table_as("CREATE UNLOGGED TABLE T AS SELECT 1"));
        assert!(is_create_table_as(
            "CREATE TABLE IF NOT EXISTS T AS SELECT 1"
        ));
        assert!(is_create_table_as("CREATE TABLE S.T AS SELECT 1"));
        assert!(is_create_table_as("CREATE TABLE T AS(SELECT 1)"));
        assert!(is_create_table_as("CREATE TABLE \"T\" AS SELECT 1"));

        assert!(!is_create_table_as("CREATE TABLE T (X INT)"));
        assert!(!is_create_table_as("CREATE INDEX I ON T (X)"));
        assert!(!is_create_table_as("CREATE VIEW V AS SELECT 1"));
        // Quoted name containing whitespace: `AS` is part of the name.
        assert!(!is_create_table_as("CREATE TABLE \"A AS B\" (X INT)"));
    }

    #[test]
    fn test_ends_with_with_no_data() {
        assert!(ends_with_with_no_data(
            "CREATE TABLE T AS SELECT 1 WITH NO DATA"
        ));
        assert!(ends_with_with_no_data(
            "CREATE TABLE T AS SELECT 1 WITH  NO\nDATA ; "
        ));

        assert!(!ends_with_with_no_data("CREATE TABLE T AS SELECT 1"));
        assert!(!ends_with_with_no_data("SELECT 'WITH NO DATA'"));
    }
}
