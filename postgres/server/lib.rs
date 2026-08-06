use std::num::NonZero;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

use async_trait::async_trait;
use futures::stream;
use tokio::net::TcpListener;
use tracing::{error, info};
use turso_core::Value;
use turso_pg::{
    auto_attach_schemas, pg_error, split_statements, LimboError, PgConnection, PgCopyFromStmt,
};

use futures::sink::{Sink, SinkExt};
use pgwire::api::copy::CopyHandler;
use pgwire::api::results::CopyResponse;
use pgwire::messages::copy::{CopyData, CopyDone};
use pgwire::messages::PgWireBackendMessage;
use std::fmt::Debug;

use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::auth::StartupHandler;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo,
    QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::{ClientInfo, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use pgwire::messages::PgWireFrontendMessage;
use pgwire::tokio::process_socket;
use pgwire::types::format::FormatOptions;

pub struct TursoPgServer {
    address: String,
    db_file: String,
    db: Arc<turso_core::Database>,
    interrupt_count: Arc<AtomicUsize>,
}

impl TursoPgServer {
    pub fn new(
        address: String,
        db_file: String,
        db: Arc<turso_core::Database>,
        interrupt_count: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            address,
            db_file,
            db,
            interrupt_count,
        }
    }

    pub fn run(&self) -> anyhow::Result<()> {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(self.run_async())
    }

    /// Opens a fresh session for one client connection: its own database
    /// connection (PostgreSQL gives every client its own backend, so
    /// transactions and session state must not be shared) with the schema
    /// databases attached, since ATTACH is per-connection state.
    fn open_session(&self) -> anyhow::Result<TursoPgFactory> {
        let conn = PgConnection::new(self.db.connect()?);
        auto_attach_schemas(&conn, &self.db_file);
        Ok(TursoPgFactory {
            handler: Arc::new(TursoPgHandler {
                conn: Arc::new(Mutex::new(conn)),
                db_file: self.db_file.clone(),
                query_parser: Arc::new(NoopQueryParser::new()),
                copy_in: Mutex::new(None),
            }),
        })
    }

    async fn run_async(&self) -> anyhow::Result<()> {
        let listener = TcpListener::bind(&self.address).await?;
        println!(
            "PostgreSQL server listening on {} (database: {})",
            self.address, self.db_file
        );

        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((socket, addr)) => {
                            info!("PostgreSQL client connected from {}", addr);
                            let factory = match self.open_session() {
                                Ok(factory) => Arc::new(factory),
                                Err(e) => {
                                    error!("Error opening session for {}: {}", addr, e);
                                    continue;
                                }
                            };
                            tokio::spawn(async move {
                                if let Err(e) = process_socket(socket, None, factory).await {
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
    /// An in-progress `COPY ... FROM STDIN`: the parsed statement and the
    /// wire data accumulated so far. One per session; PostgreSQL allows
    /// only one COPY at a time on a connection.
    copy_in: Mutex<Option<(PgCopyFromStmt, Vec<u8>)>>,
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

/// Seeds the session's configuration from the run-time parameters the
/// client sent in the StartupMessage (psql sends application_name, JDBC
/// sends extra_float_digits, poolers pass `options`), then proceeds with
/// the no-auth handshake.
struct TursoPgStartupHandler {
    conn: Arc<Mutex<PgConnection>>,
}

#[async_trait]
impl NoopStartupHandler for TursoPgStartupHandler {
    async fn post_startup<C>(
        &self,
        _client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if let PgWireFrontendMessage::Startup(startup) = &message {
            let conn = self.conn.lock().unwrap();
            conn.init_startup_parameters(
                startup
                    .parameters
                    .iter()
                    .map(|(name, value)| (name.as_str(), value.as_str())),
            );
        }
        Ok(())
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
        Arc::new(TursoPgStartupHandler {
            conn: self.handler.conn.clone(),
        })
    }

    fn copy_handler(&self) -> Arc<impl CopyHandler> {
        self.handler.clone()
    }
}

#[async_trait]
impl CopyHandler for TursoPgHandler {
    async fn on_copy_data<C>(&self, _client: &mut C, copy_data: CopyData) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let mut copy_in = self.copy_in.lock().unwrap();
        match copy_in.as_mut() {
            Some((_, buffer)) => {
                buffer.extend_from_slice(&copy_data.data);
                Ok(())
            }
            None => Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "08P01".to_owned(),
                "COPY data received without an active COPY".to_owned(),
            )))),
        }
    }

    async fn on_copy_done<C>(&self, client: &mut C, _done: CopyDone) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let Some((stmt, buffer)) = self.copy_in.lock().unwrap().take() else {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "08P01".to_owned(),
                "CopyDone received without an active COPY".to_owned(),
            ))));
        };
        let data = String::from_utf8_lossy(&buffer);
        let conn = self.conn.lock().unwrap().clone();
        let rows = conn
            .copy_stdin_finish(&stmt, &data)
            .map_err(|e| PgWireError::UserError(Box::new(error_info(&e, ""))))?;
        client
            .send(PgWireBackendMessage::CommandComplete(
                Tag::new("COPY").with_rows(rows).into(),
            ))
            .await?;
        Ok(())
    }
}

#[async_trait]
impl SimpleQueryHandler for TursoPgHandler {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo
            + pgwire::api::ClientPortalStore
            + Sink<PgWireBackendMessage>
            + Unpin
            + Send
            + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let conn = self.conn.lock().unwrap().clone();

        // Per the PostgreSQL simple query protocol, a query string may contain
        // multiple semicolon-separated statements. Split and execute each one.
        let statements = split_statements(query)
            .map_err(|e| PgWireError::UserError(Box::new(error_info(&e, query))))?;

        let mut responses = Vec::new();
        for sql in &statements {
            // COPY ... FROM STDIN: stash the descriptor and hand the
            // sub-protocol to the CopyHandler. The framework sends
            // CopyInResponse and routes the data messages there.
            if let Some(copy_stmt) = conn.parse_copy_stdin(sql) {
                *self.copy_in.lock().unwrap() = Some((copy_stmt, Vec::new()));
                responses.push(Response::CopyIn(CopyResponse::new(0, 0, vec![])));
                break;
            }

            // COPY ... TO STDOUT: stream the rows here — data frames must
            // precede the CommandComplete the returned response produces.
            match conn.copy_to_stdout(sql) {
                Ok(None) => {}
                Ok(Some(lines)) => {
                    let rows = lines.len();
                    client
                        .send(PgWireBackendMessage::CopyOutResponse(
                            pgwire::messages::copy::CopyOutResponse::new(0, 0, vec![]),
                        ))
                        .await?;
                    for line in lines {
                        let mut data = line.into_bytes();
                        data.push(b'\n');
                        client
                            .send(PgWireBackendMessage::CopyData(CopyData::new(data.into())))
                            .await?;
                    }
                    client
                        .send(PgWireBackendMessage::CopyDone(CopyDone::new()))
                        .await?;
                    responses.push(Response::Execution(Tag::new("COPY").with_rows(rows)));
                    continue;
                }
                Err(e) => {
                    return Err(PgWireError::UserError(Box::new(error_info(&e, sql))));
                }
            }

            let mut stmt = conn
                .prepare(sql)
                .map_err(|e| PgWireError::UserError(Box::new(error_info(&e, sql))))?;

            self.cleanup_dropped_schema_file(sql);

            if stmt.num_columns() == 0 || is_pg_non_query(sql) {
                responses.push(execute_non_query(&mut stmt, sql)?);
            } else {
                let header = Arc::new(build_field_info(&stmt, &Format::UnifiedText));
                let extra_float_digits = conn.extra_float_digits();
                responses.push(execute_query(&mut stmt, header, extra_float_digits)?);
            }
        }

        Ok(responses)
    }
}

#[async_trait]
impl ExtendedQueryHandler for TursoPgHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    /// Execute row limits (portal suspension) are handled by pgwire's
    /// on_execute around this method: it slices the returned row stream,
    /// sends PortalSuspended when a limit is reached, and resumes a
    /// suspended portal without calling here again — which is why
    /// max_rows goes unused. Two knowing gaps: pgwire drops the *unnamed*
    /// portal after every Execute, so suspend-then-resume works on named
    /// portals only (what cursor drivers use; fixing the unnamed case
    /// means forking pgwire's whole on_execute), and the row stream is
    /// buffered here rather than pulled from the engine on demand.
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
            .map_err(|e| PgWireError::UserError(Box::new(error_info(&e, query))))?;

        // Clean up schema file after successful DROP SCHEMA
        self.cleanup_dropped_schema_file(query);

        // Bind parameters from the portal
        bind_portal_parameters(&mut stmt, portal)?;

        if stmt.num_columns() == 0 || is_pg_non_query(query) {
            return execute_non_query(&mut stmt, query);
        }

        let header = Arc::new(build_field_info(&stmt, &portal.result_column_format));
        let extra_float_digits = conn.extra_float_digits();
        execute_query(&mut stmt, header, extra_float_digits)
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
            .map_err(|e| PgWireError::UserError(Box::new(error_info(&e, &target.statement))))?;

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
        let stmt = conn.prepare(&portal.statement.statement).map_err(|e| {
            PgWireError::UserError(Box::new(error_info(&e, &portal.statement.statement)))
        })?;

        let fields = build_field_info(&stmt, &portal.result_column_format);
        Ok(DescribePortalResponse::new(fields))
    }
}

/// Build FieldInfo metadata from a prepared statement's column information.
fn build_field_info(stmt: &turso_core::Statement, format: &Format) -> Vec<FieldInfo> {
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
    extra_float_digits: i32,
) -> PgWireResult<Response> {
    let mut rows: Vec<PgWireResult<DataRow>> = Vec::new();
    let header_clone = header.clone();

    stmt.run_with_row_callback(|row| {
        let mut encoder = DataRowEncoder::new(header_clone.clone());
        for (i, val) in row.get_values().enumerate() {
            let (pg_type, field_format) = header_clone
                .get(i)
                .map(|fi| (fi.datatype().clone(), fi.format()))
                .unwrap_or((Type::TEXT, FieldFormat::Text));
            encode_value(
                &mut encoder,
                val,
                &pg_type,
                field_format,
                extra_float_digits,
            )?;
        }
        rows.push(encoder.finish());
        Ok(())
    })
    .map_err(|e| PgWireError::UserError(Box::new(error_info(&e, ""))))?;

    let data_stream = stream::iter(rows);
    Ok(Response::Query(QueryResponse::new(header, data_stream)))
}

/// Execute a non-SELECT statement and build an Execution response.
fn execute_non_query(stmt: &mut turso_core::Statement, query: &str) -> PgWireResult<Response> {
    stmt.run_ignore_rows()
        .map_err(|e| PgWireError::UserError(Box::new(error_info(&e, query))))?;

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
                if portal.parameter_format.is_binary(i) {
                    pg_binary_to_value(bytes, pg_type)?
                } else {
                    pg_bytes_to_value(bytes, pg_type)?
                }
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

/// Convert a binary-format parameter to a turso Value. Drivers switch to
/// binary Bind for prepared statements (JDBC, asyncpg), so the network
/// byte-order encodings of the wire-common types must decode; anything
/// unhandled fails clearly rather than being misread as text.
fn pg_binary_to_value(bytes: &[u8], pg_type: &Type) -> PgWireResult<Value> {
    let exact = |n: usize| -> PgWireResult<&[u8]> {
        if bytes.len() == n {
            Ok(bytes)
        } else {
            Err(PgWireError::UserError(Box::new(param_error_info(
                &format!(
                    "invalid length {} for binary {} parameter",
                    bytes.len(),
                    pg_type.name()
                ),
            ))))
        }
    };
    match *pg_type {
        Type::BOOL => Ok(Value::from_i64((exact(1)?[0] != 0) as i64)),
        Type::INT2 => Ok(Value::from_i64(
            i16::from_be_bytes(exact(2)?.try_into().unwrap()) as i64,
        )),
        Type::INT4 => Ok(Value::from_i64(
            i32::from_be_bytes(exact(4)?.try_into().unwrap()) as i64,
        )),
        Type::INT8 => Ok(Value::from_i64(i64::from_be_bytes(
            exact(8)?.try_into().unwrap(),
        ))),
        Type::FLOAT4 => Ok(Value::from_f64(
            f32::from_be_bytes(exact(4)?.try_into().unwrap()) as f64,
        )),
        Type::FLOAT8 => Ok(Value::from_f64(f64::from_be_bytes(
            exact(8)?.try_into().unwrap(),
        ))),
        Type::BYTEA => Ok(Value::from_blob(bytes.to_vec())),
        // The binary encoding of text types is the text itself.
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::UNKNOWN => {
            let text = std::str::from_utf8(bytes).map_err(|e| {
                PgWireError::UserError(Box::new(param_error_info(&format!(
                    "invalid UTF-8 in parameter: {e}"
                ))))
            })?;
            Ok(Value::from_text(text.to_owned()))
        }
        _ => Err(PgWireError::UserError(Box::new(param_error_info(
            &format!("binary format for type {} is not supported", pg_type.name()),
        )))),
    }
}

/// Convert raw parameter bytes to a turso Value based on the PostgreSQL type.
/// Assumes text format encoding (UTF-8 string representations).
fn pg_bytes_to_value(bytes: &[u8], pg_type: &Type) -> PgWireResult<Value> {
    let text = std::str::from_utf8(bytes).map_err(|e| {
        PgWireError::UserError(Box::new(param_error_info(&format!(
            "invalid UTF-8 in parameter: {e}"
        ))))
    })?;

    match *pg_type {
        Type::INT2 | Type::INT4 | Type::INT8 => {
            let i: i64 = text.parse().map_err(|e| {
                PgWireError::UserError(Box::new(param_error_info(&format!(
                    "invalid integer parameter: {e}"
                ))))
            })?;
            Ok(Value::from_i64(i))
        }
        Type::FLOAT4 | Type::FLOAT8 | Type::NUMERIC => {
            let f: f64 = text.parse().map_err(|e| {
                PgWireError::UserError(Box::new(param_error_info(&format!(
                    "invalid float parameter: {e}"
                ))))
            })?;
            Ok(Value::from_f64(f))
        }
        Type::BOOL => match text {
            "t" | "true" | "TRUE" | "1" | "yes" | "on" => Ok(Value::from_i64(1)),
            "f" | "false" | "FALSE" | "0" | "no" | "off" => Ok(Value::from_i64(0)),
            _ => Err(PgWireError::UserError(Box::new(param_error_info(
                &format!("invalid boolean parameter: {text}"),
            )))),
        },
        Type::BYTEA => {
            // PostgreSQL text format for bytea uses \x hex encoding
            if let Some(hex_str) = text.strip_prefix("\\x") {
                let data = decode_hex(hex_str).map_err(|e| {
                    PgWireError::UserError(Box::new(param_error_info(&format!(
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

/// Render a float8 the way PostgreSQL's float8out does.
///
/// extra_float_digits >= 1 (the default) selects shortest-roundtrip
/// digits; 0 and below selects the legacy fixed-precision form with
/// 15 + extra_float_digits significant digits. Scientific notation kicks
/// in below 1e-4 or at 1e15 (legacy: at 10^precision), the exponent is
/// signed and zero-padded to two digits, and the specials use
/// PostgreSQL's spellings.
fn format_float8(value: f64, extra_float_digits: i32) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value.is_infinite() {
        return if value < 0.0 { "-Infinity" } else { "Infinity" }.to_string();
    }
    if value == 0.0 {
        return if value.is_sign_negative() { "-0" } else { "0" }.to_string();
    }

    let (digits, exp10, sci_at) = if extra_float_digits >= 1 {
        // "{:e}" renders the shortest-roundtrip mantissa, e.g. "1.5e-7".
        let sci = format!("{:e}", value.abs());
        let (mantissa, exp) = sci.split_once('e').expect("{:e} always has an exponent");
        (
            mantissa.replace('.', ""),
            exp.parse::<i32>().expect("float exponent is an integer"),
            15,
        )
    } else {
        let precision = (15 + extra_float_digits).max(1) as usize;
        let sci = format!("{:.*e}", precision - 1, value.abs());
        let (mantissa, exp) = sci.split_once('e').expect("{:e} always has an exponent");
        let mut digits = mantissa.replace('.', "");
        // %g strips trailing zeros after rounding.
        while digits.len() > 1 && digits.ends_with('0') {
            digits.pop();
        }
        let exp = exp.parse::<i32>().expect("float exponent is an integer");
        (digits, exp, precision as i32)
    };

    let sign = if value < 0.0 { "-" } else { "" };
    let mut out = String::with_capacity(digits.len() + 8);
    out.push_str(sign);
    if exp10 < -4 || exp10 >= sci_at {
        // Scientific: d[.ddd]e±NN with the exponent padded to two digits.
        out.push_str(&digits[..1]);
        if digits.len() > 1 {
            out.push('.');
            out.push_str(&digits[1..]);
        }
        out.push('e');
        out.push(if exp10 < 0 { '-' } else { '+' });
        out.push_str(&format!("{:02}", exp10.abs()));
    } else if exp10 < 0 {
        out.push_str("0.");
        for _ in 0..(-exp10 - 1) {
            out.push('0');
        }
        out.push_str(&digits);
    } else {
        let int_len = exp10 as usize + 1;
        if digits.len() <= int_len {
            out.push_str(&digits);
            for _ in 0..(int_len - digits.len()) {
                out.push('0');
            }
        } else {
            out.push_str(&digits[..int_len]);
            out.push('.');
            out.push_str(&digits[int_len..]);
        }
    }
    out
}

/// PostgreSQL's binary wire representation of one value for the declared
/// column type: big-endian fixed-width numbers, days/microseconds since
/// 2000-01-01 for date/time types, raw bytes for the string family, a
/// version byte plus text for jsonb, and base-10000 digit groups for
/// numeric. Types without an implemented representation error rather than
/// sending bytes the client would misdecode.
fn binary_wire_bytes(val: &Value, pg_type: &Type) -> turso_core::Result<Vec<u8>> {
    use turso_core::Numeric;

    let invalid = |what: &str| {
        turso_core::LimboError::InternalError(format!(
            "cannot encode value as binary {what}: {val:?}"
        ))
    };
    let text_of = |v: &Value| -> String {
        match v {
            Value::Text(t) => t.as_str().to_string(),
            Value::Numeric(Numeric::Float(f)) => {
                // Rust's Display is shortest-roundtrip and never scientific,
                // which decimal parsers below rely on.
                format!("{}", f64::from(*f))
            }
            other => other.to_string(),
        }
    };
    let as_i64 = |v: &Value| -> Option<i64> {
        match v {
            Value::Numeric(Numeric::Integer(i)) => Some(*i),
            Value::Numeric(Numeric::Float(f)) => {
                let f = f64::from(*f);
                (f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64)
                    .then_some(f as i64)
            }
            Value::Text(t) => t.as_str().trim().parse().ok(),
            _ => None,
        }
    };
    let as_f64 = |v: &Value| -> Option<f64> {
        match v {
            Value::Numeric(Numeric::Integer(i)) => Some(*i as f64),
            Value::Numeric(Numeric::Float(f)) => Some(f64::from(*f)),
            Value::Text(t) => t.as_str().trim().parse().ok(),
            _ => None,
        }
    };

    if *pg_type == Type::INT2 {
        let i = as_i64(val).ok_or_else(|| invalid("int2"))?;
        let i = i16::try_from(i)
            .map_err(|_| turso_core::LimboError::ParseError("smallint out of range".to_string()))?;
        Ok(i.to_be_bytes().to_vec())
    } else if *pg_type == Type::INT4 {
        let i = as_i64(val).ok_or_else(|| invalid("int4"))?;
        let i = i32::try_from(i)
            .map_err(|_| turso_core::LimboError::ParseError("integer out of range".to_string()))?;
        Ok(i.to_be_bytes().to_vec())
    } else if *pg_type == Type::INT8 {
        let i = as_i64(val).ok_or_else(|| invalid("int8"))?;
        Ok(i.to_be_bytes().to_vec())
    } else if *pg_type == Type::OID {
        let i = as_i64(val).ok_or_else(|| invalid("oid"))?;
        let i = u32::try_from(i)
            .map_err(|_| turso_core::LimboError::ParseError("OID out of range".to_string()))?;
        Ok(i.to_be_bytes().to_vec())
    } else if *pg_type == Type::FLOAT4 {
        let f = as_f64(val).ok_or_else(|| invalid("float4"))?;
        Ok((f as f32).to_be_bytes().to_vec())
    } else if *pg_type == Type::FLOAT8 {
        let f = as_f64(val).ok_or_else(|| invalid("float8"))?;
        Ok(f.to_be_bytes().to_vec())
    } else if *pg_type == Type::BOOL {
        let b = match val {
            Value::Numeric(Numeric::Integer(i)) => *i != 0,
            Value::Numeric(Numeric::Float(f)) => f64::from(*f) != 0.0,
            Value::Text(t) => matches!(
                t.as_str().to_ascii_lowercase().as_str(),
                "t" | "true" | "yes" | "on" | "1"
            ),
            _ => return Err(invalid("bool")),
        };
        Ok(vec![b as u8])
    } else if *pg_type == Type::BYTEA {
        Ok(match val {
            Value::Blob(b) => b.to_vec(),
            other => text_of(other).into_bytes(),
        })
    } else if *pg_type == Type::JSONB {
        // Binary jsonb is a format version byte followed by the json text.
        let mut out = vec![1u8];
        out.extend_from_slice(text_of(val).as_bytes());
        Ok(out)
    } else if *pg_type == Type::UUID {
        let text = text_of(val);
        let hex: String = text.chars().filter(|c| *c != '-').collect();
        if hex.len() != 32 {
            return Err(invalid("uuid"));
        }
        decode_hex(&hex).map_err(|_| invalid("uuid"))
    } else if *pg_type == Type::DATE {
        let text = text_of(val);
        let days = parse_date_to_pg_days(&text).ok_or_else(|| invalid("date"))?;
        Ok((days as i32).to_be_bytes().to_vec())
    } else if *pg_type == Type::TIME {
        let text = text_of(val);
        let micros = parse_time_to_micros(&text).ok_or_else(|| invalid("time"))?;
        Ok(micros.to_be_bytes().to_vec())
    } else if *pg_type == Type::TIMESTAMP || *pg_type == Type::TIMESTAMPTZ {
        let text = text_of(val);
        let micros = parse_timestamp_to_pg_micros(&text).ok_or_else(|| invalid("timestamp"))?;
        Ok(micros.to_be_bytes().to_vec())
    } else if *pg_type == Type::NUMERIC {
        let text = text_of(val);
        numeric_wire_bytes(&text).ok_or_else(|| invalid("numeric"))
    } else if *pg_type == Type::TEXT
        || *pg_type == Type::VARCHAR
        || *pg_type == Type::BPCHAR
        || *pg_type == Type::NAME
        || *pg_type == Type::CHAR
        || *pg_type == Type::UNKNOWN
        || *pg_type == Type::JSON
    {
        // The string family's binary format is the text itself; plain
        // json has no version byte either.
        Ok(text_of(val).into_bytes())
    } else {
        Err(turso_core::LimboError::ParseError(format!(
            "binary format for type \"{}\" is not supported",
            pg_type.name()
        )))
    }
}

/// Days between 1970-01-01 and y-m-d in the proleptic Gregorian calendar
/// (Howard Hinnant's days_from_civil).
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m as i64 + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719_468
}

/// Days between PostgreSQL's 2000-01-01 epoch and the Unix epoch.
const PG_EPOCH_DAYS_FROM_UNIX: i64 = 10_957;

/// "YYYY-MM-DD" to days since 2000-01-01, PostgreSQL's binary date unit.
fn parse_date_to_pg_days(s: &str) -> Option<i64> {
    let mut parts = s.trim().split('-');
    let y: i64 = parts.next()?.parse().ok()?;
    let m: u32 = parts.next()?.parse().ok()?;
    let d: u32 = parts.next()?.parse().ok()?;
    if parts.next().is_some() || !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    Some(days_from_civil(y, m, d) - PG_EPOCH_DAYS_FROM_UNIX)
}

/// "HH:MM[:SS[.fraction]]" to microseconds since midnight.
fn parse_time_to_micros(s: &str) -> Option<i64> {
    let s = s.trim();
    let (hms, frac) = match s.split_once('.') {
        Some((hms, frac)) => (hms, Some(frac)),
        None => (s, None),
    };
    let mut parts = hms.split(':');
    let h: i64 = parts.next()?.parse().ok()?;
    let m: i64 = parts.next()?.parse().ok()?;
    let sec: i64 = match parts.next() {
        Some(sec) => sec.parse().ok()?,
        None => 0,
    };
    if parts.next().is_some()
        || !(0..=24).contains(&h)
        || !(0..60).contains(&m)
        || !(0..61).contains(&sec)
    {
        return None;
    }
    let micros_frac: i64 = match frac {
        Some(frac) => {
            if frac.is_empty() || frac.len() > 6 || !frac.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            format!("{frac:0<6}").parse().ok()?
        }
        None => 0,
    };
    Some(((h * 60 + m) * 60 + sec) * 1_000_000 + micros_frac)
}

/// "YYYY-MM-DD HH:MM:SS[.fraction][±HH[:MM]|Z]" to microseconds since
/// 2000-01-01 00:00:00 UTC, PostgreSQL's binary timestamp unit.
fn parse_timestamp_to_pg_micros(s: &str) -> Option<i64> {
    let s = s.trim();
    let (date_part, time_part) = s.split_once([' ', 'T'])?;
    // A timezone suffix begins with +, -, or Z somewhere after the HH:MM
    // digits start; a leading '-' cannot occur inside the time itself.
    let (time_part, offset_secs) = if let Some(rest) = time_part.strip_suffix('Z') {
        (rest, 0)
    } else if let Some(pos) = time_part.rfind(['+', '-']).filter(|&p| p > 0) {
        let (time, tz) = time_part.split_at(pos);
        let sign: i64 = if tz.starts_with('-') { -1 } else { 1 };
        let tz = &tz[1..];
        let (th, tm) = match tz.split_once(':') {
            Some((th, tm)) => (th.parse::<i64>().ok()?, tm.parse::<i64>().ok()?),
            None => (tz.parse::<i64>().ok()?, 0),
        };
        (time, sign * (th * 3600 + tm * 60))
    } else {
        (time_part, 0)
    };
    let days = parse_date_to_pg_days(date_part)?;
    let micros = parse_time_to_micros(time_part)?;
    Some(days * 86_400_000_000 + micros - offset_secs * 1_000_000)
}

/// A plain decimal string ("[-]digits[.digits]" or "NaN") to PostgreSQL's
/// binary numeric format: i16 count of base-10000 digit groups, i16 weight
/// of the first group (in units of 10000^weight), u16 sign, u16 display
/// scale, then the groups.
fn numeric_wire_bytes(s: &str) -> Option<Vec<u8>> {
    let s = s.trim();
    if s.eq_ignore_ascii_case("nan") {
        return Some(
            [0u16, 0, 0xC000, 0]
                .iter()
                .flat_map(|v| v.to_be_bytes())
                .collect(),
        );
    }
    let (negative, s) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let (int_part, frac_part) = match s.split_once('.') {
        Some((i, f)) => (i, f),
        None => (s, ""),
    };
    if (int_part.is_empty() && frac_part.is_empty())
        || !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let dscale = frac_part.len() as u16;

    // Group the digits into base-10000 chunks aligned on the decimal
    // point: pad the integer side to a multiple of 4 on the left and the
    // fraction side on the right.
    let mut padded = String::new();
    for _ in 0..(4 - int_part.len() % 4) % 4 {
        padded.push('0');
    }
    padded.push_str(int_part);
    let int_groups = padded.len() / 4;
    padded.push_str(frac_part);
    while padded.len() % 4 != 0 {
        padded.push('0');
    }
    let mut groups: Vec<i16> = padded
        .as_bytes()
        .chunks(4)
        .map(|c| {
            std::str::from_utf8(c)
                .expect("ascii digits")
                .parse()
                .expect("four digits")
        })
        .collect();

    // Strip zero groups off both ends; weight counts from the first kept
    // group, so leading strips lower it.
    let mut weight = int_groups as i64 - 1;
    let leading_zeros = groups.iter().take_while(|g| **g == 0).count();
    groups.drain(..leading_zeros);
    weight -= leading_zeros as i64;
    while groups.last() == Some(&0) {
        groups.pop();
    }
    if groups.is_empty() {
        weight = 0;
    }

    let mut out = Vec::with_capacity(8 + groups.len() * 2);
    out.extend_from_slice(&(groups.len() as i16).to_be_bytes());
    out.extend_from_slice(&(weight as i16).to_be_bytes());
    out.extend_from_slice(&if negative { 0x4000u16 } else { 0 }.to_be_bytes());
    out.extend_from_slice(&dscale.to_be_bytes());
    for group in groups {
        out.extend_from_slice(&group.to_be_bytes());
    }
    Some(out)
}

fn encode_value(
    encoder: &mut DataRowEncoder,
    val: &Value,
    pg_type: &Type,
    field_format: FieldFormat,
    extra_float_digits: i32,
) -> turso_core::Result<()> {
    // Binary result columns carry the type's PostgreSQL binary wire
    // representation, computed here; NULL uses the shared -1-length path.
    if field_format == FieldFormat::Binary && !matches!(val, Value::Null) {
        let bytes = binary_wire_bytes(val, pg_type)?;
        return encoder
            .encode_field_with_type_and_format(
                &bytes.as_slice(),
                &Type::BYTEA,
                FieldFormat::Binary,
                &FormatOptions::default(),
            )
            .map_err(|e| turso_core::LimboError::InternalError(e.to_string()));
    }
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
            .encode_field_with_type_and_format(
                &format_float8(f64::from(*f), extra_float_digits).as_str(),
                &Type::TEXT,
                FieldFormat::Text,
                &FormatOptions::default(),
            )
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

/// Maps an engine error to a PostgreSQL error response with its SQLSTATE.
fn error_info(e: &LimboError, sql: &str) -> ErrorInfo {
    let info = pg_error(e, sql);
    let mut error = ErrorInfo::new("ERROR".to_owned(), info.code.to_owned(), info.message);
    error.position = info.position.map(|p| p.to_string());
    error
}

/// A malformed extended-protocol parameter value: invalid_text_representation.
fn param_error_info(message: &str) -> ErrorInfo {
    ErrorInfo::new("ERROR".to_owned(), "22P02".to_owned(), message.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn numeric_wire_bytes_matches_postgres_binary_format() {
        // 123.45: groups [123, 4500], weight 0, positive, dscale 2.
        assert_eq!(
            numeric_wire_bytes("123.45").unwrap(),
            vec![0, 2, 0, 0, 0, 0, 0, 2, 0, 123, 0x11, 0x94]
        );
        // 20001.5 spans three groups with weight 1.
        assert_eq!(
            numeric_wire_bytes("20001.5").unwrap(),
            vec![0, 3, 0, 1, 0, 0, 0, 1, 0, 2, 0, 1, 0x13, 0x88]
        );
        // -0.0042: one group in the 10000^-1 position, negative, dscale 4.
        assert_eq!(
            numeric_wire_bytes("-0.0042").unwrap(),
            vec![0, 1, 0xff, 0xff, 0x40, 0, 0, 4, 0, 42]
        );
        // Zero keeps only the display scale.
        assert_eq!(
            numeric_wire_bytes("0.00").unwrap(),
            vec![0, 0, 0, 0, 0, 0, 0, 2]
        );
        // NaN is the reserved sign value.
        assert_eq!(
            numeric_wire_bytes("NaN").unwrap(),
            vec![0, 0, 0, 0, 0xc0, 0, 0, 0]
        );
        assert_eq!(numeric_wire_bytes("12e4"), None);
    }

    #[test]
    fn date_and_time_parsers_use_the_postgres_epoch() {
        assert_eq!(parse_date_to_pg_days("2000-01-01"), Some(0));
        assert_eq!(parse_date_to_pg_days("1999-12-31"), Some(-1));
        assert_eq!(parse_date_to_pg_days("2024-01-15"), Some(8780));
        assert_eq!(parse_date_to_pg_days("2024-13-01"), None);

        assert_eq!(parse_time_to_micros("00:00"), Some(0));
        assert_eq!(parse_time_to_micros("12:00:00.25"), Some(43_200_250_000));
        assert_eq!(parse_time_to_micros("25:00:00"), None);

        assert_eq!(parse_timestamp_to_pg_micros("2000-01-01 00:00:00"), Some(0));
        assert_eq!(
            parse_timestamp_to_pg_micros("2024-01-15T12:00:00"),
            Some(758_635_200_000_000)
        );
        // A -05 offset means five hours later in UTC.
        assert_eq!(
            parse_timestamp_to_pg_micros("2000-01-01 00:00:00-05"),
            Some(5 * 3600 * 1_000_000)
        );
        assert_eq!(
            parse_timestamp_to_pg_micros("2000-01-01 01:30:00+01:30"),
            Some(0)
        );
        assert_eq!(
            parse_timestamp_to_pg_micros("2000-01-01 00:00:00Z"),
            Some(0)
        );
    }
}
