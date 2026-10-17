use std::fmt::Debug;
use std::num::NonZero;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use async_trait::async_trait;
use bytes::{Buf, Bytes};
use futures::{stream, Sink, SinkExt};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tracing::{error, info};
use turso_core::Value;
use turso_pg::{Connection, PgConnection};
use turso_pg_parser::copy::{parse_copy_to_stdout, CopyToStdout};

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

        let factory = Arc::new(TursoPgFactory {
            handler: Arc::new(TursoPgHandler {
                conn: self.conn.clone(),
                db_file: self.db_file.clone(),
                query_parser: Arc::new(NoopQueryParser::new()),
            }),
        });
        let io = self.conn.lock().await.inner().get_pager().io.clone();
        let mut io_task = tokio::spawn(poll_io(io));

        loop {
            tokio::select! {
                result = &mut io_task => return Ok(result??),
                result = listener.accept() => {
                    match result {
                        Ok((socket, addr)) => {
                            info!("PostgreSQL client connected from {}", addr);
                            let factory_ref = factory.clone();
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

        io_task.abort();
        let _ = io_task.await;
        Ok(())
    }
}

async fn poll_io(io: Arc<dyn turso_core::IO>) -> turso_core::Result<()> {
    let mut timer = tokio::time::interval(Duration::from_millis(1));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        timer.tick().await;
        io.step()?;
    }
}

struct TursoPgHandler {
    conn: Arc<Mutex<PgConnection>>,
    db_file: String,
    query_parser: Arc<NoopQueryParser>,
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
        SimpleQueryHandler::do_query(self, client, &query.query).await?;
        client.set_state(PgWireConnectionState::ReadyForQuery);
        send_ready_for_query(client, client.transaction_status()).await
    }

    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        turso_pg_parser::parse(query).map_err(query_error)?;
        let statements = turso_pg_parser::split_statements(query).map_err(query_error)?;
        if statements.is_empty() {
            client
                .feed(PgWireBackendMessage::EmptyQueryResponse(
                    EmptyQueryResponse::new(),
                ))
                .await?;
        }
        let conn = self.conn.lock().await;
        for sql in statements {
            match parse_copy_to_stdout(&sql).map_err(query_error)? {
                Some(copy) => {
                    copy_to_stdout(client, &conn, copy).await?;
                }
                None => {
                    let mut stmt = conn.prepare(&sql).map_err(query_error)?;
                    let response = if stmt.num_columns() == 0 || is_pg_non_query(&sql) {
                        execute_non_query(&mut stmt, &sql).await?
                    } else {
                        let header = Arc::new(build_field_info(&stmt, &Format::UnifiedText));
                        execute_query(&mut stmt, header).await?
                    };
                    self.cleanup_dropped_schema_file(&sql);
                    match response {
                        Response::Query(mut result) => {
                            send_query_response(client, &mut result, true).await?;
                        }
                        Response::Execution(tag) => send_execution_response(client, tag).await?,
                        _ => unreachable!("simple queries return query or execution responses"),
                    }
                }
            }
            let status = if conn.inner().get_auto_commit() {
                TransactionStatus::Idle
            } else {
                client.transaction_status().to_in_transaction_state()
            };
            client.set_transaction_status(status);
        }
        Ok(Vec::new())
    }
}

async fn copy_to_stdout<C>(
    client: &mut C,
    conn: &PgConnection,
    copy: CopyToStdout,
) -> PgWireResult<()>
where
    C: Sink<PgWireBackendMessage> + Unpin,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    let mut stmt = conn.prepare_copy_to_stdout(&copy).map_err(query_error)?;
    let mut has_row = next_copy_row(&mut stmt).await?;
    let fields = Arc::new(build_field_info(&stmt, &Format::UnifiedText));
    let column_count = i16::try_from(fields.len()).map_err(query_error)?;
    client
        .send(PgWireBackendMessage::CopyOutResponse(CopyOutResponse::new(
            0,
            column_count,
            vec![0; fields.len()],
        )))
        .await?;
    let mut row_count = 0;
    while has_row {
        let data = encode_copy_row(stmt.row().expect("COPY row is present"), fields.clone())?;
        client
            .send(PgWireBackendMessage::CopyData(CopyData::new(data)))
            .await?;
        row_count += 1;
        has_row = next_copy_row(&mut stmt).await?;
    }
    client
        .send(PgWireBackendMessage::CopyDone(CopyDone::new()))
        .await?;
    send_execution_response(client, Tag::new("COPY").with_rows(row_count)).await
}

async fn next_copy_row(stmt: &mut turso_pg::Statement) -> PgWireResult<bool> {
    loop {
        match stmt.step().map_err(query_error)? {
            turso_core::StepResult::Row => return Ok(true),
            turso_core::StepResult::Done => return Ok(false),
            turso_core::StepResult::IO | turso_core::StepResult::Yield => {
                if let Some(completion) = stmt.take_io_completions() {
                    wait_for_io(completion).await?;
                } else {
                    tokio::task::yield_now().await;
                }
            }
            turso_core::StepResult::Sleep { duration } => tokio::time::sleep(duration).await,
            turso_core::StepResult::Busy => return Err(query_error(turso_core::LimboError::Busy)),
            turso_core::StepResult::Interrupt => {
                return Err(query_error(turso_core::LimboError::Interrupt));
            }
        }
    }
}

fn encode_copy_row(row: &turso_core::Row, fields: Arc<Vec<FieldInfo>>) -> PgWireResult<Bytes> {
    assert_eq!(row.len(), fields.len());
    let mut encoder = DataRowEncoder::new(fields.clone());
    for (value, field) in row.get_values().zip(fields.iter()) {
        encode_value(&mut encoder, value, field.datatype()).map_err(query_error)?;
    }
    let mut values = encoder.finish()?.data.freeze();
    let mut output = Vec::new();
    for index in 0..fields.len() {
        if index > 0 {
            output.push(b'\t');
        }
        let length = values.get_i32();
        if length == -1 {
            output.extend_from_slice(b"\\N");
            continue;
        }
        assert!(length >= 0);
        for byte in &values[..length as usize] {
            match byte {
                b'\\' => output.extend_from_slice(b"\\\\"),
                b'\t' => output.extend_from_slice(b"\\t"),
                b'\n' => output.extend_from_slice(b"\\n"),
                b'\r' => output.extend_from_slice(b"\\r"),
                8 => output.extend_from_slice(b"\\b"),
                12 => output.extend_from_slice(b"\\f"),
                11 => output.extend_from_slice(b"\\v"),
                byte => output.push(*byte),
            }
        }
        values.advance(length as usize);
    }
    output.push(b'\n');
    Ok(Bytes::from(output))
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
        let conn = self.conn.lock().await;
        let query = &portal.statement.statement;

        let mut stmt = conn
            .prepare(query)
            .map_err(|e| PgWireError::UserError(Box::new(error_info(&e.to_string()))))?;

        // Bind parameters from the portal
        bind_portal_parameters(&mut stmt, portal)?;

        if stmt.num_columns() == 0 || is_pg_non_query(query) {
            let response = execute_non_query(&mut stmt, query).await?;
            self.cleanup_dropped_schema_file(query);
            return Ok(response);
        }

        let header = Arc::new(build_field_info(&stmt, &portal.result_column_format));
        execute_query(&mut stmt, header).await
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let conn = self.conn.lock().await;
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
        let conn = self.conn.lock().await;
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
async fn execute_query(
    stmt: &mut turso_pg::Statement,
    header: Arc<Vec<FieldInfo>>,
) -> PgWireResult<Response> {
    let mut rows: Vec<PgWireResult<DataRow>> = Vec::new();
    let header_clone = header.clone();

    loop {
        let result = stmt
            .run_with_row_callback_nonblock(|row| {
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
        match result {
            turso_core::IOResult::Done(()) => break,
            turso_core::IOResult::IO(io) => wait_for_io(io).await?,
        }
    }

    let data_stream = stream::iter(rows);
    Ok(Response::Query(QueryResponse::new(header, data_stream)))
}

/// Execute a non-SELECT statement and build an Execution response.
async fn execute_non_query(stmt: &mut turso_pg::Statement, query: &str) -> PgWireResult<Response> {
    loop {
        match stmt
            .run_ignore_rows_nonblock()
            .map_err(|e| PgWireError::UserError(Box::new(error_info(&e.to_string()))))?
        {
            turso_core::IOResult::Done(()) => break,
            turso_core::IOResult::IO(io) => wait_for_io(io).await?,
        }
    }

    let affected = stmt.n_change();
    let tag = command_tag(query, affected as usize);
    Ok(Response::Execution(tag))
}

async fn wait_for_io(completion: turso_core::types::IOCompletions) -> PgWireResult<()> {
    if completion.is_explicit_yield() {
        tokio::task::yield_now().await;
    }
    completion
        .0
        .await
        .map_err(|e| PgWireError::UserError(Box::new(error_info(&e.to_string()))))
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

fn query_error(error: impl std::fmt::Display) -> PgWireError {
    PgWireError::UserError(Box::new(error_info(&error.to_string())))
}

fn error_info(message: &str) -> ErrorInfo {
    ErrorInfo::new("ERROR".to_owned(), "XX000".to_owned(), message.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::poll;
    use std::task::Poll;
    use turso_core::{Clock, Completion, IO};

    #[tokio::test]
    async fn timer_task_drives_io_and_stops_when_aborted() {
        let io = Arc::new(PollingIO {
            expected_task: std::sync::Mutex::new(None),
            steps: AtomicUsize::new(0),
            completion: Completion::new_wait(),
            fail: false,
            inner: None,
        });
        let wait = wait_for_io(turso_core::types::IOCompletions(io.completion.clone()));
        tokio::pin!(wait);
        assert!(matches!(poll!(&mut wait), Poll::Pending));
        assert_eq!(io.steps.load(Ordering::SeqCst), 0);

        let task = tokio::spawn(poll_io(io.clone()));
        *io.expected_task.lock().unwrap() = Some(task.id());
        tokio::time::timeout(Duration::from_secs(1), wait)
            .await
            .unwrap()
            .unwrap();
        assert!(io.steps.load(Ordering::SeqCst) >= 3);
        assert!(io.completion.succeeded());

        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let steps = io.steps.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(io.steps.load(Ordering::SeqCst), steps);
    }

    #[tokio::test]
    async fn timer_task_returns_io_errors() {
        let io = Arc::new(PollingIO {
            expected_task: std::sync::Mutex::new(None),
            steps: AtomicUsize::new(0),
            completion: Completion::new_wait(),
            fail: true,
            inner: None,
        });
        let task = tokio::spawn(poll_io(io.clone()));
        *io.expected_task.lock().unwrap() = Some(task.id());
        let result = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(result, Err(turso_core::LimboError::InternalError(message)) if message == "poll failed")
        );
        assert_eq!(io.steps.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn wait_returns_completion_errors() {
        let completion = Completion::new_wait();
        completion.abort();
        assert!(wait_for_io(turso_core::types::IOCompletions(completion))
            .await
            .unwrap_err()
            .to_string()
            .contains("aborted"));
    }

    #[tokio::test]
    async fn copy_to_stdout_waits_for_the_io_task() {
        let io = Arc::new(PollingIO {
            expected_task: std::sync::Mutex::new(None),
            steps: AtomicUsize::new(0),
            completion: Completion::new_wait(),
            fail: false,
            inner: Some(Arc::new(turso_core::MemoryYieldIO::new())),
        });
        let db = turso_pg::open_database_with_io(
            io.clone(),
            "copy.db",
            turso_pg::OpenFlags::default(),
            turso_pg::DatabaseOpts::new(),
        )
        .unwrap();
        let conn = PgConnection::connect(&db).unwrap();
        conn.execute("CREATE TABLE items (id INTEGER); INSERT INTO items VALUES (7), (19)")
            .unwrap();
        conn.inner().get_pager().clear_page_cache(false);
        let steps = io.steps.load(Ordering::SeqCst);
        let task = tokio::spawn(poll_io(io.clone()));
        *io.expected_task.lock().unwrap() = Some(task.id());
        let mut messages = Vec::new();
        let mut sink = futures::sink::unfold(&mut messages, |messages, message| {
            messages.push(message);
            std::future::ready(Ok::<_, PgWireError>(messages))
        });
        let copy = parse_copy_to_stdout("COPY items TO STDOUT")
            .unwrap()
            .unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            copy_to_stdout(&mut sink, &conn, copy),
        )
        .await
        .unwrap()
        .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(io.steps.load(Ordering::SeqCst) > steps);
        drop(sink);
        assert_eq!(messages.len(), 5);
        assert!(
            matches!(&messages[1], PgWireBackendMessage::CopyData(row) if row.data == b"7\n"[..])
        );
        assert!(
            matches!(&messages[2], PgWireBackendMessage::CopyData(row) if row.data == b"19\n"[..])
        );
    }

    struct PollingIO {
        expected_task: std::sync::Mutex<Option<tokio::task::Id>>,
        steps: AtomicUsize,
        completion: Completion,
        fail: bool,
        inner: Option<Arc<dyn IO>>,
    }

    impl Clock for PollingIO {
        fn current_time_monotonic(&self) -> turso_core::MonotonicInstant {
            turso_core::io::clock::DefaultClock.current_time_monotonic()
        }

        fn current_time_wall_clock(&self) -> turso_core::WallClockInstant {
            turso_core::io::clock::DefaultClock.current_time_wall_clock()
        }
    }

    impl IO for PollingIO {
        fn open_file(
            &self,
            path: &str,
            flags: turso_core::OpenFlags,
            direct: bool,
        ) -> turso_core::Result<Arc<dyn turso_core::File>> {
            self.inner.as_ref().unwrap().open_file(path, flags, direct)
        }

        fn remove_file(&self, path: &str) -> turso_core::Result<()> {
            self.inner.as_ref().unwrap().remove_file(path)
        }

        fn file_id(&self, path: &str) -> turso_core::Result<turso_core::io::FileId> {
            self.inner.as_ref().unwrap().file_id(path)
        }

        fn supports_shared_wal_coordination(&self) -> bool {
            false
        }

        fn step(&self) -> turso_core::Result<()> {
            assert_eq!(tokio::task::try_id(), *self.expected_task.lock().unwrap());
            let steps = self.steps.fetch_add(1, Ordering::SeqCst) + 1;
            if let Some(inner) = &self.inner {
                return inner.step();
            }
            if self.fail {
                return Err(turso_core::LimboError::InternalError(
                    "poll failed".to_string(),
                ));
            }
            if steps == 3 {
                self.completion.complete(0);
            }
            Ok(())
        }
    }

    use turso_pg_client::{BackendEvent, ConnParams, PgConn};

    #[test]
    fn copy_to_stdout_sends_text_in_statement_order() {
        let (mut client, conn) = copy_test_client();
        let events = client
            .simple_query(
                r#"CREATE TABLE "A""B;C" (id INT, value TEXT, active BOOLEAN, data BYTEA);
                   INSERT INTO "A""B;C" VALUES
                     (7, E'one\ttwo\nthree\rfour\b\f\013\\N', true, NULL),
                     (9, '', false, NULL), (11, NULL, NULL, NULL);"#,
            )
            .unwrap();
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, BackendEvent::ErrorResponse(_))),
            "{events:?}"
        );
        for (id, bytes) in [(7, vec![0, 255]), (11, Vec::new())] {
            let mut stmt = conn
                .prepare(r#"UPDATE "A""B;C" SET data = $1 WHERE id = $2"#)
                .unwrap();
            stmt.bind_at(NonZero::new(1).unwrap(), Value::from_blob(bytes))
                .unwrap();
            stmt.bind_at(NonZero::new(2).unwrap(), Value::from_i64(id))
                .unwrap();
            stmt.run_ignore_rows().unwrap();
        }
        let messages = query_messages(
            &mut client,
            r#"SELECT 41; /* export */ COPY public."A""B;C" (value, id, active, data) TO stdout (FORMAT text); SELECT 43"#,
        );
        let tags: Vec<_> = messages.iter().map(|message| message.0).collect();
        let errors: Vec<_> = messages
            .iter()
            .filter(|m| m.0 == b'E')
            .map(|m| String::from_utf8_lossy(&m.1))
            .collect();
        assert_eq!(tags, b"TDCHdddcCTDCZ", "{errors:?}");
        assert_eq!(messages[3].1, [0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(
            messages[4].1,
            b"one\\ttwo\\nthree\\rfour\\b\\f\\v\\\\N\t7\tt\t\\\\x00ff\n"
        );
        assert_eq!(messages[5].1, b"\t9\tf\t\\N\n");
        assert_eq!(messages[6].1, b"\\N\t11\t\\N\t\\\\x\n");
        assert_eq!(messages[8].1, b"COPY 3\0");
        assert_eq!(messages.last().unwrap().1, b"I");
    }

    #[test]
    fn copy_to_stdout_handles_empty_tables_and_transactions() {
        let (mut client, _) = copy_test_client();
        client
            .simple_query("CREATE TABLE empty_table (id INT)")
            .unwrap();
        let messages = query_messages(&mut client, "BEGIN; COPY empty_table TO STDOUT");
        let tags: Vec<_> = messages.iter().map(|message| message.0).collect();
        assert_eq!(tags, b"CHcCZ");
        assert_eq!(messages[3].1, b"COPY 0\0");
        assert_eq!(messages[4].1, b"T");
        let messages = query_messages(&mut client, "COMMIT");
        assert_eq!(messages.last().unwrap().1, b"I");
        let messages = query_messages(&mut client, "BEGIN; COPY missing TO STDOUT; COMMIT");
        assert_eq!(messages.iter().map(|m| m.0).collect::<Vec<_>>(), b"CEZ");
        assert_eq!(messages.last().unwrap().1, b"E");
        let messages = query_messages(&mut client, "ROLLBACK");
        assert_eq!(messages.last().unwrap().1, b"I");
        for sql in ["", "; ;", "-- no statement"] {
            let messages = query_messages(&mut client, sql);
            assert_eq!(messages.iter().map(|m| m.0).collect::<Vec<_>>(), b"IZ");
        }
    }

    #[test]
    fn copy_to_stdout_reports_errors_without_running_later_statements() {
        let (mut client, _) = copy_test_client();
        client
            .simple_query("CREATE TABLE items (id INT); INSERT INTO items VALUES (7); CREATE VIEW item_view AS SELECT id FROM items")
            .unwrap();
        for sql in [
            "COPY items TO STDOUT (FORMAT csv); DELETE FROM items",
            "COPY items TO ''; DELETE FROM items",
            "COPY items (missing) TO STDOUT; DELETE FROM items",
            "COPY missing TO STDOUT; DELETE FROM items",
            "COPY items (id, id) TO STDOUT; DELETE FROM items",
            "COPY items (rowid) TO STDOUT; DELETE FROM items",
            "COPY item_view TO STDOUT; DELETE FROM items",
            "INSERT INTO items VALUES (99); COPY items TO STDOUT WHERE id > 0",
        ] {
            let messages = query_messages(&mut client, sql);
            assert_eq!(
                messages.iter().map(|m| m.0).collect::<Vec<_>>(),
                b"EZ",
                "{sql}: {messages:?}"
            );
            assert_eq!(messages.last().unwrap().1, b"I");
        }
        let messages = query_messages(&mut client, "COPY items TO STDOUT");
        assert_eq!(messages[1].1, b"7\n");
        assert_eq!(messages[3].1, b"COPY 1\0");
    }

    #[test]
    fn copy_to_stdout_uses_the_schema_and_search_path() {
        let (mut client, conn) = copy_test_client();
        conn.inner().execute("ATTACH ':memory:' AS other").unwrap();
        conn.execute(
            r#"CREATE TABLE public.items (id INT);
               INSERT INTO public.items VALUES (7);
               CREATE TABLE other.items (id INT, "Text Value" TEXT);
               INSERT INTO other.items VALUES (19, 'São Paulo');"#,
        )
        .unwrap();
        conn.inner()
            .execute("CREATE TRIGGER items AFTER INSERT ON items BEGIN SELECT 1; END")
            .unwrap();
        client
            .simple_query("SET search_path TO other, public")
            .unwrap();
        let messages = query_messages(&mut client, r#"COPY items ("Text Value", id) TO STDOUT"#);
        assert_eq!(messages[1].1, "São Paulo\t19\n".as_bytes());
        let messages = query_messages(&mut client, "COPY public.items TO STDOUT");
        assert_eq!(messages[1].1, b"7\n");
        conn.execute("DROP TABLE other.items; CREATE VIEW other.items AS SELECT 23 AS id")
            .unwrap();
        let messages = query_messages(&mut client, "COPY items TO STDOUT");
        assert_eq!(messages.iter().map(|m| m.0).collect::<Vec<_>>(), b"EZ");
        conn.inner()
            .execute(
                "CREATE TEMP TABLE temp.items (id INTEGER); INSERT INTO temp.items VALUES (31)",
            )
            .unwrap();
        let messages = query_messages(&mut client, "COPY items TO STDOUT");
        assert_eq!(messages[1].1, b"31\n");
        let messages = query_messages(&mut client, "COPY public.items TO STDOUT");
        assert_eq!(messages[1].1, b"7\n");
        conn.inner().execute("DROP TABLE temp.items").unwrap();
        client
            .simple_query("SELECT set_config('search_path', '', false)")
            .unwrap();
        let messages = query_messages(&mut client, "COPY items TO STDOUT");
        assert_eq!(messages.iter().map(|m| m.0).collect::<Vec<_>>(), b"EZ");
    }

    #[test]
    fn copy_to_stdout_excludes_generated_columns() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("source.db");
        let path = path.to_str().unwrap();
        {
            let (_io, db) = turso_core::Database::open_new(
                path,
                None::<&str>,
                turso_core::OpenFlags::default(),
                turso_core::DatabaseOpts::new().with_generated_columns(true),
                None,
                Arc::new(turso_core::dialect::SqliteDialect),
            )
            .unwrap();
            let conn = db.connect(turso_core::ConnectOpts::new()).unwrap();
            conn.execute("CREATE TABLE items (id INTEGER, doubled INTEGER GENERATED ALWAYS AS (id * 2) VIRTUAL)").unwrap();
            conn.execute("INSERT INTO items (id) VALUES (7)").unwrap();
            conn.close().unwrap();
        }
        let (mut client, _) = copy_test_client_at(path);
        let messages = query_messages(&mut client, "COPY items TO STDOUT");
        assert_eq!(messages[0].1, [0, 0, 1, 0, 0]);
        assert_eq!(messages[1].1, b"7\n");
        let messages = query_messages(&mut client, "COPY items (doubled) TO STDOUT");
        assert_eq!(messages.iter().map(|m| m.0).collect::<Vec<_>>(), b"EZ");
    }

    #[test]
    fn copy_to_stdout_sends_rows_before_a_later_read_error() {
        let (mut client, conn) = copy_test_client();
        conn.execute(
            "CREATE TABLE items (id INT);
             WITH RECURSIVE numbers(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM numbers WHERE n < 10000)
             INSERT INTO items SELECT n FROM numbers",
        )
        .unwrap();
        let steps = AtomicUsize::new(0);
        conn.inner().set_progress_handler(
            1,
            Some(Box::new(move || {
                steps.fetch_add(1, Ordering::SeqCst) > 1000
            })),
        );
        let messages = query_messages(&mut client, "COPY items TO STDOUT; DELETE FROM items");
        conn.inner().set_progress_handler(0, None);
        assert_eq!(messages[0].0, b'H', "{messages:?}");
        let rows: Vec<_> = messages.iter().filter(|m| m.0 == b'd').collect();
        assert!(!rows.is_empty());
        assert!(rows.len() < 10000);
        assert_eq!(rows[0].1, b"1\n");
        assert_eq!(messages[messages.len() - 2].0, b'E');
        assert_eq!(messages.last().unwrap().1, b"I");
        assert!(!messages.iter().any(|m| matches!(m.0, b'c' | b'C')));
        let messages = query_messages(&mut client, "SELECT count(*) FROM items");
        assert_eq!(
            turso_pg_client::parse_data_row(&messages[1].1).unwrap(),
            vec![Some("10000".to_owned())]
        );
    }

    fn copy_test_client() -> (PgConn, PgConnection) {
        copy_test_client_at(":memory:")
    }

    fn copy_test_client_at(path: &str) -> (PgConn, PgConnection) {
        let (io, db) = turso_pg::open_database(
            path,
            None,
            turso_pg::OpenFlags::default(),
            turso_pg::DatabaseOpts::new()
                .with_generated_columns(true)
                .with_attach(true),
        )
        .unwrap();
        let conn = PgConnection::connect(&db).unwrap();
        let handler = Arc::new(TursoPgHandler {
            conn: Arc::new(Mutex::new(conn.clone())),
            db_file: path.to_owned(),
            query_parser: Arc::new(NoopQueryParser::new()),
        });
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    let listener = TcpListener::from_std(listener).unwrap();
                    let (socket, _) = listener.accept().await.unwrap();
                    let io_task = tokio::spawn(poll_io(io));
                    process_socket(socket, None, Arc::new(TursoPgFactory { handler }))
                        .await
                        .unwrap();
                    io_task.abort();
                    let _ = io_task.await;
                });
        });
        let client = PgConn::connect(
            &ConnParams {
                host: address.ip().to_string(),
                port: address.port(),
                user: "turso".to_owned(),
                password: None,
                database: "main".to_owned(),
            },
            &[],
        )
        .unwrap();
        client
            .set_read_timeout(Some(std::time::Duration::from_secs(10)))
            .unwrap();
        (client, conn)
    }

    fn query_messages(client: &mut PgConn, sql: &str) -> Vec<(u8, Vec<u8>)> {
        client.send_query(sql).unwrap();
        let mut messages = Vec::new();
        loop {
            let message = client.read_message().unwrap();
            let done = message.0 == b'Z';
            messages.push(message);
            if done {
                return messages;
            }
        }
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
