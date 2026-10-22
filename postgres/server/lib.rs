use std::fmt::Debug;
use std::num::NonZero;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

use async_trait::async_trait;
use futures::{stream, Sink, SinkExt};
use tokio::net::TcpListener;
use tracing::{error, info};
use turso_core::Value;
use turso_pg::{classify_tx_stmt, TxStmt};
use turso_pg::{split_statements, Connection, PgConnection};

use pgwire::api::auth::StartupHandler;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo,
    QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::store::PortalStore;
use pgwire::api::{
    ClientInfo, ClientPortalStore, NoopHandler, PgWireConnectionState, PgWireServerHandlers, Type,
    DEFAULT_NAME,
};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use pgwire::messages::extendedquery::{Describe, Execute};
use pgwire::messages::response::{ErrorResponse, ReadyForQuery, TransactionStatus};
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

        loop {
            tokio::select! {
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

        Ok(())
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
        // pgwire initializes its local transaction-status tracking from
        // client.transaction_status() at the top of _on_query, so we must
        // set the status BEFORE calling _on_query.  For ROLLBACK TO SAVEPOINT
        // in the Error state, we pre-execute the savepoint rollback and then
        // advance the status to Transaction so _on_query starts from the right
        // state.  The statement will run again inside do_query, but that second
        // execution is a no-op (no work has been done since the pre-execution).
        if client.transaction_status() == TransactionStatus::Error {
            let stmts = split_statements(&query.query).unwrap_or_default();
            if let Some(first) = stmts.first() {
                if matches!(classify_tx_stmt(first), TxStmt::SavepointRollback) {
                    let conn = self.conn.lock().unwrap().clone();
                    if conn
                        .prepare(first)
                        .and_then(|mut s| s.run_ignore_rows())
                        .is_ok()
                    {
                        client.set_transaction_status(TransactionStatus::Transaction);
                    }
                }
            }
        }
        let result = self._on_query(client, query).await;
        finish_query(client, result, false).await
    }

    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let conn = self.conn.lock().unwrap().clone();

        // Per the PostgreSQL simple query protocol, a query string may contain
        // multiple semicolon-separated statements. Split and execute each one.
        let statements = split_statements(query).map_err(pg_error)?;
        let mut responses = Vec::new();
        let mut start_idx = 0;

        if client.transaction_status() == TransactionStatus::Error {
            let first = statements.first().map(|s| s.as_str()).unwrap_or("");
            match classify_tx_stmt(first) {
                TxStmt::SavepointRollback => {
                    let mut stmt = conn.prepare(first).map_err(pg_error)?;
                    stmt.run_ignore_rows().map_err(pg_error)?;
                    responses.push(Response::TransactionStart(Tag::new("ROLLBACK")));
                    start_idx = 1;
                }
                TxStmt::FullRollback | TxStmt::Commit => {
                    // Validate the statement through the translator first so unsupported
                    // variants like AND CHAIN are rejected rather than silently replaced.
                    if let Err(e) = conn.prepare(first) {
                        return batch_error(responses, pg_error(e));
                    }
                    // Exit the aborted transaction. COMMIT/END become ROLLBACK (PostgreSQL behavior).
                    // Ignore errors: the core may have already auto-rolled back the transaction.
                    let _ = conn
                        .prepare("ROLLBACK")
                        .and_then(|mut s| s.run_ignore_rows());
                    responses.push(Response::TransactionEnd(Tag::new("ROLLBACK")));
                    start_idx = 1;
                }
                _ => {
                    return Err(pg_error(turso_pg::Error::aborted_transaction()));
                }
            }
        }

        for sql in &statements[start_idx..] {
            let mut stmt = match conn.prepare(sql) {
                Ok(s) => s,
                Err(e) => {
                    return batch_error(responses, pg_error(e));
                }
            };

            let resp = if stmt.num_columns() == 0 || is_pg_non_query(sql) {
                match execute_non_query(&mut stmt, sql) {
                    Ok(r) => {
                        self.cleanup_dropped_schema_file(sql);
                        r
                    }
                    Err(e) if matches!(classify_tx_stmt(sql), TxStmt::Commit) => {
                        let err = rollback_after_failed_commit(client, &conn, e);
                        return batch_error(responses, err);
                    }
                    Err(e) => return batch_error(responses, e),
                }
            } else {
                let header = Arc::new(build_field_info(&stmt, &Format::UnifiedText));
                match execute_query(&mut stmt, header) {
                    Ok(r) => r,
                    Err(e) => return batch_error(responses, e),
                }
            };

            responses.push(resp);
        }

        Ok(responses)
    }
}

/// A statement in a simple-query batch failed. With no responses produced yet,
/// return the error itself so finish_query reports the full error fields
/// (table, column, constraint). With earlier responses (e.g. TransactionStart
/// from a prior BEGIN), keep them and append the error inline so pgwire
/// applies the transaction-status transition of each response in sequence;
/// pgwire's ErrorInfo cannot carry the table/column/constraint fields.
fn batch_error(mut responses: Vec<Response>, err: PgWireError) -> PgWireResult<Vec<Response>> {
    if responses.is_empty() {
        return Err(err);
    }
    responses.push(pg_error_to_response(err)?);
    Ok(responses)
}

/// COMMIT itself failed (e.g. a deferred constraint fired at commit time).
/// PostgreSQL then rolls the transaction back and returns to Idle instead of
/// entering the aborted state.
fn rollback_after_failed_commit<C>(
    client: &mut C,
    conn: &turso_pg::Connection,
    err: PgWireError,
) -> PgWireError
where
    C: ClientInfo,
{
    let _ = conn
        .prepare("ROLLBACK")
        .and_then(|mut s| s.run_ignore_rows());
    client.set_transaction_status(TransactionStatus::Idle);
    err
}

fn pg_error_to_response(err: PgWireError) -> PgWireResult<Response> {
    let PgWireError::ApiError(e) = err else {
        return Err(err);
    };
    match e.downcast::<turso_pg::Error>() {
        Ok(pg_err) => {
            let mut info = ErrorInfo::new(
                "ERROR".to_owned(),
                pg_err.sqlstate.to_owned(),
                pg_err.message.clone(),
            );
            info.detail.clone_from(&pg_err.detail);
            Ok(Response::Error(Box::new(info)))
        }
        Err(e) => Err(PgWireError::ApiError(e)),
    }
}

#[async_trait]
impl ExtendedQueryHandler for TursoPgHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    async fn on_execute<C>(&self, client: &mut C, message: Execute) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        // Same pre-execution trick as on_query: advance wire status to Transaction
        // before _on_execute reads it so pgwire uses the right initial value.
        if client.transaction_status() == TransactionStatus::Error {
            let portal_name = message.name.as_deref().unwrap_or(DEFAULT_NAME);
            if let Some(portal) = client.portal_store().get_portal(portal_name) {
                let query = portal.statement.statement.as_str();
                if matches!(classify_tx_stmt(query), TxStmt::SavepointRollback) {
                    let conn = self.conn.lock().unwrap().clone();
                    if conn
                        .prepare(query)
                        .and_then(|mut s| s.run_ignore_rows())
                        .is_ok()
                    {
                        client.set_transaction_status(TransactionStatus::Transaction);
                    }
                }
            }
        }
        let result = self._on_execute(client, message).await;
        finish_query(client, result, true).await
    }

    async fn on_describe<C>(&self, client: &mut C, message: Describe) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let result = self._on_describe(client, message).await;
        finish_query(client, result, true).await
    }

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let conn = self.conn.lock().unwrap().clone();
        let query = &portal.statement.statement;

        if client.transaction_status() == TransactionStatus::Error {
            match classify_tx_stmt(query) {
                TxStmt::SavepointRollback => {
                    let mut stmt = conn.prepare(query).map_err(pg_error)?;
                    stmt.run_ignore_rows().map_err(pg_error)?;
                    return Ok(Response::TransactionStart(Tag::new("ROLLBACK")));
                }
                TxStmt::FullRollback | TxStmt::Commit => {
                    // Validate the statement through the translator first so unsupported
                    // variants like AND CHAIN are rejected rather than silently replaced.
                    if let Err(e) = conn.prepare(query) {
                        return Err(pg_error(e));
                    }
                    // Exit the aborted transaction. COMMIT/END become ROLLBACK (PostgreSQL behavior).
                    // Ignore errors: the core may have already auto-rolled back the transaction.
                    let _ = conn
                        .prepare("ROLLBACK")
                        .and_then(|mut s| s.run_ignore_rows());
                    return Ok(Response::TransactionEnd(Tag::new("ROLLBACK")));
                }
                _ => {
                    return Err(pg_error(turso_pg::Error::aborted_transaction()));
                }
            }
        }

        let mut stmt = conn.prepare(query).map_err(pg_error)?;

        // Bind parameters from the portal
        bind_portal_parameters(&mut stmt, portal)?;

        if stmt.num_columns() == 0 || is_pg_non_query(query) {
            return match execute_non_query(&mut stmt, query) {
                Ok(response) => {
                    // Clean up schema file after successful DROP SCHEMA execution
                    self.cleanup_dropped_schema_file(query);
                    Ok(response)
                }
                Err(e) if matches!(classify_tx_stmt(query), TxStmt::Commit) => {
                    Err(rollback_after_failed_commit(client, &conn, e))
                }
                Err(e) => Err(e),
            };
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
        let param_types: Vec<Type> = target
            .parameter_types
            .iter()
            .map(|t| t.clone().unwrap_or(Type::TEXT))
            .collect();

        let stmt_sql = &target.statement;
        if !matches!(classify_tx_stmt(stmt_sql), TxStmt::Other) || is_pg_non_query(stmt_sql) {
            return Ok(DescribeStatementResponse::new(param_types, vec![]));
        }

        let conn = self.conn.lock().unwrap().clone();
        let stmt = conn.prepare_for_describe(stmt_sql).map_err(pg_error)?;

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
        let stmt_sql = &portal.statement.statement;
        if !matches!(classify_tx_stmt(stmt_sql), TxStmt::Other) || is_pg_non_query(stmt_sql) {
            return Ok(DescribePortalResponse::new(vec![]));
        }

        let conn = self.conn.lock().unwrap().clone();
        let stmt = conn.prepare_for_describe(stmt_sql).map_err(pg_error)?;

        let fields = build_field_info(&stmt, &portal.result_column_format);
        Ok(DescribePortalResponse::new(fields))
    }
}

/// Build FieldInfo metadata from a prepared statement's column information.
fn build_field_info(stmt: &turso_pg::Statement, format: &Format) -> Vec<FieldInfo> {
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
fn resolve_pg_type_for_column(stmt: &turso_pg::Statement, idx: usize) -> Type {
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
    stmt: &mut turso_pg::Statement,
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
    .map_err(pg_error)?;

    let data_stream = stream::iter(rows);
    Ok(Response::Query(QueryResponse::new(header, data_stream)))
}

fn execute_non_query(stmt: &mut turso_pg::Statement, query: &str) -> PgWireResult<Response> {
    stmt.run_ignore_rows().map_err(pg_error)?;

    let affected = stmt.n_change();
    let tag = command_tag(query, affected as usize);
    match classify_tx_stmt(query) {
        TxStmt::Begin => Ok(Response::TransactionStart(tag)),
        TxStmt::Commit | TxStmt::FullRollback => Ok(Response::TransactionEnd(tag)),
        _ => Ok(Response::Execution(tag)),
    }
}

/// Extract parameters from a Portal and bind them to a prepared statement.
///
/// PostgreSQL parameters ($1, $2, ...) map to portal parameters 0, 1, ...
/// The bytecode compiler may allocate internal parameter indices in a different
/// order than the $N numbering (e.g. if $2 appears before $1 in the SQL), so we
/// look up each parameter's internal index by name.
fn bind_portal_parameters(
    stmt: &mut turso_pg::Statement,
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
    // Transaction-control statements can prepare as no-op SELECTs (e.g. COMMIT
    // outside a transaction block), so route them by classification rather
    // than by the prepared statement's column count.
    if !matches!(classify_tx_stmt(sql), TxStmt::Other) {
        return true;
    }
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
    } else if upper.starts_with("COMMIT") || upper.starts_with("END") {
        Tag::new("COMMIT")
    } else if upper.starts_with("ROLLBACK") || upper.starts_with("ABORT") {
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

fn pg_error(error: turso_pg::Error) -> PgWireError {
    PgWireError::ApiError(Box::new(error))
}

async fn finish_query<C>(
    client: &mut C,
    result: PgWireResult<()>,
    wait_for_sync: bool,
) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    let Err(PgWireError::ApiError(error)) = result else {
        return result;
    };
    let error = match error.downcast::<turso_pg::Error>() {
        Ok(error) => error,
        Err(error) => return Err(PgWireError::ApiError(error)),
    };
    let mut fields = vec![
        (b'S', "ERROR".to_string()),
        (b'C', error.sqlstate.to_string()),
        (b'M', error.message.clone()),
    ];
    for (tag, value) in [
        (b'D', &error.detail),
        (b't', &error.table_name),
        (b'c', &error.column_name),
        (b'n', &error.constraint_name),
    ] {
        if let Some(value) = value {
            fields.push((tag, value.clone()));
        }
    }
    client
        .send(PgWireBackendMessage::ErrorResponse(ErrorResponse::new(
            fields,
        )))
        .await?;
    let status = client.transaction_status().to_error_state();
    client.set_transaction_status(status);
    if wait_for_sync {
        client.set_state(PgWireConnectionState::AwaitingSync);
    } else {
        client.set_state(PgWireConnectionState::ReadyForQuery);
        client
            .feed(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                status,
            )))
            .await?;
    }
    client.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_integer_case_and_function_results_are_int4_on_wire() {
        let (_io, db) = turso_pg::open_database(
            ":memory:",
            None,
            turso_pg::OpenFlags::default(),
            turso_pg::DatabaseOpts::new(),
        )
        .unwrap();
        let conn = Connection::connect(&db).unwrap();
        conn.execute("CREATE TABLE t(id int PRIMARY KEY, x int)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 1)").unwrap();
        for sql in [
            "SELECT x * 2 FROM t",
            "SELECT coalesce(x, 2) * coalesce(x, 3) FROM t",
            "SELECT abs(x) * 2 FROM t",
            "SELECT CASE WHEN x = 1 THEN x ELSE 0 END - 1 FROM t",
            "SELECT coalesce(x, 2) FROM t",
        ] {
            let stmt = conn.prepare(sql).unwrap();
            let fields = build_field_info(&stmt, &Format::UnifiedText);
            assert_eq!(*fields[0].datatype(), Type::INT4, "{sql}");
        }
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

    #[test]
    fn command_tag_maps_transaction_keyword_aliases() {
        assert_eq!(command_tag("END", 0), Tag::new("COMMIT"));
        assert_eq!(command_tag("ABORT", 0), Tag::new("ROLLBACK"));
        assert_eq!(command_tag("COMMIT", 0), Tag::new("COMMIT"));
        assert_eq!(command_tag("ROLLBACK", 0), Tag::new("ROLLBACK"));
    }

    #[test]
    fn test_begin_returns_transaction_start_response() {
        let (_io, db) = turso_pg::open_database(
            ":memory:",
            None,
            turso_pg::OpenFlags::default(),
            turso_pg::DatabaseOpts::new(),
        )
        .unwrap();
        let conn = Connection::connect(&db).unwrap();
        for sql in ["BEGIN", "BEGIN READ ONLY", "START TRANSACTION"] {
            let mut stmt = conn.prepare(sql).unwrap();
            let resp = execute_non_query(&mut stmt, sql).unwrap();
            assert!(
                matches!(resp, Response::TransactionStart(_)),
                "{sql} should produce TransactionStart"
            );
            conn.execute("ROLLBACK").unwrap();
        }
    }

    #[test]
    fn test_commit_rollback_return_transaction_end_response() {
        let (_io, db) = turso_pg::open_database(
            ":memory:",
            None,
            turso_pg::OpenFlags::default(),
            turso_pg::DatabaseOpts::new(),
        )
        .unwrap();
        let conn = Connection::connect(&db).unwrap();
        for sql in ["COMMIT", "ROLLBACK"] {
            conn.execute("BEGIN").unwrap();
            let mut stmt = conn.prepare(sql).unwrap();
            let resp = execute_non_query(&mut stmt, sql).unwrap();
            assert!(
                matches!(resp, Response::TransactionEnd(_)),
                "{sql} should produce TransactionEnd"
            );
        }
        // ROLLBACK TO SAVEPOINT stays in the transaction — must not produce TransactionEnd.
        conn.execute("BEGIN").unwrap();
        conn.execute("SAVEPOINT sp").unwrap();
        let mut stmt = conn.prepare("ROLLBACK TO sp").unwrap();
        let resp = execute_non_query(&mut stmt, "ROLLBACK TO sp").unwrap();
        assert!(
            matches!(resp, Response::Execution(_)),
            "ROLLBACK TO sp should produce Execution, not TransactionEnd"
        );
        conn.execute("ROLLBACK").unwrap();
    }

    #[test]
    fn classify_tx_stmt_correctly_identifies_transaction_statements() {
        assert!(matches!(classify_tx_stmt("ROLLBACK"), TxStmt::FullRollback));
        assert!(matches!(
            classify_tx_stmt("  rollback  "),
            TxStmt::FullRollback
        ));
        assert!(matches!(
            classify_tx_stmt("ROLLBACK WORK"),
            TxStmt::FullRollback
        ));
        assert!(matches!(classify_tx_stmt("ABORT"), TxStmt::FullRollback));
        assert!(matches!(classify_tx_stmt("COMMIT"), TxStmt::Commit));
        assert!(matches!(classify_tx_stmt("END"), TxStmt::Commit));
        assert!(matches!(
            classify_tx_stmt("ROLLBACK /*comment*/ TO SAVEPOINT sp"),
            TxStmt::SavepointRollback
        ));
        assert!(matches!(
            classify_tx_stmt("ROLLBACK TO sp"),
            TxStmt::SavepointRollback
        ));
        assert!(matches!(
            classify_tx_stmt("ROLLBACK WORK TO SAVEPOINT sp"),
            TxStmt::SavepointRollback
        ));
        assert!(matches!(classify_tx_stmt("BEGIN"), TxStmt::Begin));
        assert!(matches!(
            classify_tx_stmt("START TRANSACTION"),
            TxStmt::Begin
        ));
        assert!(matches!(classify_tx_stmt("SELECT 1"), TxStmt::Other));
    }

    /// No statement the PostgreSQL frontend accepts today can make COMMIT
    /// itself fail (deferred constraints are rejected at CREATE TABLE), so the
    /// commit failure is synthesized and the test checks what the server must
    /// do with it: roll the transaction back and report the wire connection as
    /// Idle, like PostgreSQL, instead of leaving an open transaction in the
    /// Error state.
    #[test]
    fn failed_commit_rolls_back_and_reports_idle() {
        let (_io, db) = turso_pg::open_database(
            ":memory:",
            None,
            turso_pg::OpenFlags::default(),
            turso_pg::DatabaseOpts::new(),
        )
        .unwrap();
        let conn = Connection::connect(&db).unwrap();
        conn.execute("CREATE TABLE t (a int)").unwrap();
        conn.execute("BEGIN").unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        assert!(!conn.inner().get_auto_commit());

        let commit_err = pg_error(turso_pg::Error::from(turso_pg::LimboError::InternalError(
            "commit failed".to_string(),
        )));
        let mut client =
            pgwire::api::DefaultClient::<String>::new("127.0.0.1:1".parse().unwrap(), false);
        client.set_transaction_status(TransactionStatus::Transaction);
        let err = rollback_after_failed_commit(&mut client, &conn, commit_err);
        assert!(matches!(err, PgWireError::ApiError(_)));
        assert_eq!(client.transaction_status(), TransactionStatus::Idle);
        assert!(
            conn.inner().get_auto_commit(),
            "a failed COMMIT must end with the transaction rolled back"
        );
        let mut count = conn.prepare("SELECT count(*) FROM t").unwrap();
        let rows = count.run_collect_rows().unwrap();
        assert_eq!(rows, vec![vec![Value::from_i64(0)]]);
    }

    #[test]
    fn prepare_for_describe_does_not_clear_read_only() {
        let (_io, db) = turso_pg::open_database(
            ":memory:",
            None,
            turso_pg::OpenFlags::default(),
            turso_pg::DatabaseOpts::new(),
        )
        .unwrap();
        let conn = Connection::connect(&db).unwrap();
        conn.execute("BEGIN READ ONLY").unwrap();
        assert!(
            conn.inner().is_tx_read_only(),
            "BEGIN READ ONLY must mark the transaction read-only"
        );
        let _stmt = conn.prepare_for_describe("COMMIT").unwrap();
        assert!(
            conn.inner().is_tx_read_only(),
            "prepare_for_describe(COMMIT) must not clear the read-only transaction mode"
        );
        conn.execute("ROLLBACK").unwrap();
    }

    #[test]
    fn prepare_begin_read_only_does_not_set_read_only_before_execute() {
        let (_io, db) = turso_pg::open_database(
            ":memory:",
            None,
            turso_pg::OpenFlags::default(),
            turso_pg::DatabaseOpts::new(),
        )
        .unwrap();
        let conn = Connection::connect(&db).unwrap();
        let _stmt = conn.prepare("BEGIN READ ONLY").unwrap();
        assert!(
            !conn.inner().is_tx_read_only(),
            "prepare(BEGIN READ ONLY) must not set read-only mode before execute"
        );
    }

    #[test]
    fn prepare_commit_does_not_clear_read_only_before_execute() {
        let (_io, db) = turso_pg::open_database(
            ":memory:",
            None,
            turso_pg::OpenFlags::default(),
            turso_pg::DatabaseOpts::new(),
        )
        .unwrap();
        let conn = Connection::connect(&db).unwrap();
        conn.execute("BEGIN READ ONLY").unwrap();
        assert!(
            conn.inner().is_tx_read_only(),
            "BEGIN READ ONLY must mark the transaction read-only"
        );
        let _stmt = conn.prepare("COMMIT").unwrap();
        assert!(
            conn.inner().is_tx_read_only(),
            "prepare(COMMIT) must not clear read-only mode before execute"
        );
        conn.execute("ROLLBACK").unwrap();
    }
}
