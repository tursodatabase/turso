use std::fmt::Debug;
use std::num::NonZero;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use async_trait::async_trait;
use futures::{stream, Sink, SinkExt};
use tokio::net::TcpListener;
use tracing::{error, info};
use turso_core::{ConnectOpts, Database, Value};
use turso_pg::{split_statements, PgConnection, TransactionState};
use turso_pg_parser::cmdtag::CommandTag;
use turso_pg_parser::copy::{parse_copy_to_stdout, CopyToStdout};
use turso_pg_parser::{returns_rows, ParseResult};

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
use pgwire::api::store::PortalStore;
use pgwire::api::{
    ClientInfo, ClientPortalStore, NoopHandler, PgWireConnectionState, PgWireServerHandlers, Type,
};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::copy::{CopyData, CopyDone, CopyOutResponse};
use pgwire::messages::data::DataRow;
use pgwire::messages::extendedquery::{Describe, Execute};
use pgwire::messages::response::{
    EmptyQueryResponse, ErrorResponse, ReadyForQuery, TransactionStatus,
};
use pgwire::messages::simplequery::Query;
use pgwire::messages::PgWireBackendMessage;
use pgwire::tokio::process_socket;
use pgwire::types::format::FormatOptions;

pub struct TursoPgServer {
    address: String,
    db_file: String,
    db: Arc<Database>,
    /// The settings that each session connects with, such as the encryption
    /// key.
    connect_opts: ConnectOpts,
    interrupt_count: Arc<AtomicUsize>,
}

impl TursoPgServer {
    pub fn new(
        address: String,
        db_file: String,
        db: Arc<Database>,
        connect_opts: ConnectOpts,
        interrupt_count: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            address,
            db_file,
            db,
            connect_opts,
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
        self.serve(listener).await
    }

    /// Accepts clients on `listener` until the server is interrupted. Each
    /// client gets its own session, with its own transaction and settings.
    pub async fn serve(&self, listener: TcpListener) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((socket, addr)) => {
                            info!("PostgreSQL client connected from {}", addr);
                            let session = match PgConnection::connect_with(
                                &self.db,
                                self.connect_opts.clone(),
                            ) {
                                Ok(session) => session,
                                Err(e) => {
                                    error!("Error opening a session for {}: {}", addr, e);
                                    continue;
                                }
                            };
                            let factory = Arc::new(TursoPgFactory {
                                handler: Arc::new(TursoPgHandler {
                                    conn: session.clone(),
                                    query_parser: Arc::new(NoopQueryParser::new()),
                                }),
                            });
                            tokio::spawn(async move {
                                if let Err(e) = process_socket(socket, None, factory).await {
                                    error!("Error processing connection from {}: {}", addr, e);
                                }
                                if let Err(e) = session.close() {
                                    error!("Error closing the session of {}: {}", addr, e);
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
    conn: PgConnection,
    query_parser: Arc<NoopQueryParser>,
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
        let result = self.run_simple_query(client, query).await;
        let status = wire_transaction_status(&self.conn);
        finish_query(client, result, false, status).await
    }

    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let conn = &self.conn;

        // Per the PostgreSQL simple query protocol, a query string may contain
        // multiple semicolon-separated statements. Split and execute each one.
        let statements = split_statements(query).map_err(pg_error)?;

        let mut responses = Vec::new();
        for sql in &statements {
            let mut stmt = conn.prepare(sql).map_err(pg_error)?;
            let parsed = parse_statement(sql)?;

            if stmt.num_columns() == 0 || !returns_rows(&parsed) {
                responses.push(execute_non_query(&mut stmt, &parsed)?);
            } else {
                let header = Arc::new(build_field_info(&stmt, &Format::UnifiedText));
                responses.push(execute_query(&mut stmt, header)?);
            }
        }

        Ok(responses)
    }
}

impl TursoPgHandler {
    async fn run_simple_query<C>(&self, client: &mut C, query: Query) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if !matches!(client.state(), PgWireConnectionState::ReadyForQuery) {
            return Err(PgWireError::NotReadyForQuery);
        }
        client.set_state(PgWireConnectionState::QueryInProgress);
        let conn = &self.conn;
        // Errors raised here never reach PgConnection::prepare, so report
        // them to the session: any error inside a transaction block aborts
        // the block.
        let parse_error = |error: turso_pg_parser::ParseError| {
            conn.abort_transaction_block_on_error();
            pg_error(turso_core::LimboError::ParseError(error.to_string()).into())
        };
        turso_pg_parser::parse(&query.query).map_err(parse_error)?;
        let statements = turso_pg_parser::split_statements(&query.query).map_err(parse_error)?;
        if statements.is_empty() {
            client
                .feed(PgWireBackendMessage::EmptyQueryResponse(
                    EmptyQueryResponse::new(),
                ))
                .await?;
        }
        for sql in statements {
            match parse_copy_to_stdout(&sql).map_err(parse_error)? {
                Some(copy) => copy_to_stdout(client, conn, &copy).await?,
                None => {
                    for response in SimpleQueryHandler::do_query(self, client, &sql).await? {
                        match response {
                            Response::Query(mut result) => {
                                send_query_response(client, &mut result, true).await?
                            }
                            Response::Execution(tag) => {
                                send_execution_response(client, tag).await?
                            }
                            _ => unreachable!("simple queries return query or execution responses"),
                        }
                    }
                }
            }
            client.set_transaction_status(wire_transaction_status(conn));
        }
        client.set_state(PgWireConnectionState::ReadyForQuery);
        send_ready_for_query(client, client.transaction_status()).await
    }
}

async fn copy_to_stdout<C>(
    client: &mut C,
    conn: &PgConnection,
    copy: &CopyToStdout,
) -> PgWireResult<()>
where
    C: Sink<PgWireBackendMessage> + Unpin,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    let mut stmt = conn.prepare_copy_to_stdout(copy).map_err(pg_error)?;
    let mut has_row = stmt
        .run_one_step_blocking(|| Ok(()), || Ok(()))
        .map_err(pg_error)?
        .is_some();
    let encoder = turso_pg::CopyTextEncoder::new(&stmt);
    let column_count = i16::try_from(stmt.num_columns())
        .map_err(|error| PgWireError::UserError(Box::new(error_info(&error.to_string()))))?;
    client
        .send(PgWireBackendMessage::CopyOutResponse(CopyOutResponse::new(
            0,
            column_count,
            vec![0; stmt.num_columns()],
        )))
        .await?;
    let mut row_count = 0;
    while has_row {
        let mut data = Vec::new();
        encoder.encode_row(stmt.row().expect("COPY row is present"), &mut data);
        client
            .send(PgWireBackendMessage::CopyData(CopyData::new(data.into())))
            .await?;
        row_count += 1;
        has_row = stmt
            .run_one_step_blocking(|| Ok(()), || Ok(()))
            .map_err(pg_error)?
            .is_some();
    }
    client
        .send(PgWireBackendMessage::CopyDone(CopyDone::new()))
        .await?;
    send_execution_response(client, Tag::new("COPY").with_rows(row_count)).await
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
        let result = self._on_execute(client, message).await;
        let status = wire_transaction_status(&self.conn);
        finish_query(client, result, true, status).await
    }

    async fn on_describe<C>(&self, client: &mut C, message: Describe) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let result = self._on_describe(client, message).await;
        let status = wire_transaction_status(&self.conn);
        finish_query(client, result, true, status).await
    }

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
        let conn = &self.conn;
        let query = &portal.statement.statement;

        let mut stmt = conn.prepare(query).map_err(pg_error)?;

        // Bind parameters from the portal
        bind_portal_parameters(&mut stmt, portal)?;

        let parsed = parse_statement(query)?;
        if stmt.num_columns() == 0 || !returns_rows(&parsed) {
            return execute_non_query(&mut stmt, &parsed)
                .map(|response| with_transaction_state(conn, response));
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
        let conn = &self.conn;
        let stmt = conn.prepare(&target.statement).map_err(pg_error)?;

        let param_types: Vec<Type> = target
            .parameter_types
            .iter()
            .map(|t| t.clone().unwrap_or(Type::TEXT))
            .collect();

        let fields = describe_fields(&stmt, &target.statement, &Format::UnifiedText)?;
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
        let conn = &self.conn;
        let stmt = conn
            .prepare(&portal.statement.statement)
            .map_err(pg_error)?;

        let fields = describe_fields(
            &stmt,
            &portal.statement.statement,
            &portal.result_column_format,
        )?;
        Ok(DescribePortalResponse::new(fields))
    }
}

/// The fields of the rows that `sql` returns, or none if it returns no rows.
fn describe_fields(
    stmt: &turso_pg::Statement,
    sql: &str,
    format: &Format,
) -> PgWireResult<Vec<FieldInfo>> {
    if !returns_rows(&parse_statement(sql)?) {
        return Ok(vec![]);
    }
    Ok(build_field_info(stmt, format))
}

fn parse_statement(sql: &str) -> PgWireResult<ParseResult> {
    turso_pg_parser::parse(sql)
        .map_err(|error| pg_error(turso_core::LimboError::ParseError(error.to_string()).into()))
}

/// Build FieldInfo metadata from a prepared statement's column information.
fn build_field_info(stmt: &turso_pg::Statement, format: &Format) -> Vec<FieldInfo> {
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

/// Execute a non-SELECT statement and build an Execution response.
fn execute_non_query(
    stmt: &mut turso_pg::Statement,
    parsed: &ParseResult,
) -> PgWireResult<Response> {
    stmt.run_ignore_rows().map_err(pg_error)?;

    let affected = stmt.n_change() as usize;
    // A COMMIT that ended an aborted block performed a rollback; its tag
    // cannot come from the statement text.
    let tag = match stmt.command_tag_override() {
        Some(tag) => Tag::new(tag),
        None => wire_tag(CommandTag::of(parsed), affected),
    };
    Ok(Response::Execution(tag))
}

fn wire_tag(tag: CommandTag, affected_rows: usize) -> Tag {
    match tag {
        // INSERT also reports the OID of the inserted row, which is always 0
        // because tables have no OIDs.
        CommandTag::Insert => Tag::new(tag.name()).with_oid(0).with_rows(affected_rows),
        _ if tag.has_row_count() => Tag::new(tag.name()).with_rows(affected_rows),
        _ => Tag::new(tag.name()),
    }
}

/// Reflect the session's transaction state onto the wire protocol: pgwire
/// derives the ReadyForQuery status byte from TransactionStart and
/// TransactionEnd responses, so report which side of a transaction block the
/// statement left the session on.
fn with_transaction_state(conn: &PgConnection, response: Response) -> Response {
    let Response::Execution(tag) = response else {
        return response;
    };
    match conn.transaction_state() {
        TransactionState::Idle => Response::TransactionEnd(tag),
        TransactionState::Active | TransactionState::Aborted => Response::TransactionStart(tag),
    }
}

/// The ReadyForQuery status byte for the session's current transaction
/// state: 'I' idle, 'T' in a transaction block, 'E' in a failed one.
fn wire_transaction_status(conn: &PgConnection) -> TransactionStatus {
    match conn.transaction_state() {
        TransactionState::Idle => TransactionStatus::Idle,
        TransactionState::Active => TransactionStatus::Transaction,
        TransactionState::Aborted => TransactionStatus::Error,
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
    hex.as_bytes()
        .chunks_exact(2)
        .enumerate()
        .map(|(pair_index, pair)| {
            let position = 2 * pair_index;
            let high = decode_hex_digit(pair[0], position)?;
            let low = decode_hex_digit(pair[1], position + 1)?;
            Ok((high << 4) | low)
        })
        .collect()
}

fn decode_hex_digit(byte: u8, position: usize) -> Result<u8, String> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(format!("invalid hex at position {position}")),
    }
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

fn error_info(message: &str) -> ErrorInfo {
    ErrorInfo::new(
        "ERROR".to_owned(),
        "XX000".to_owned(),
        error_field_value(message),
    )
}

/// ErrorResponse fields are NUL-terminated strings, so a NUL inside a value
/// would end the field early and the client would read the rest as more
/// fields. PostgreSQL fills these fields from C strings, which also end at
/// the first NUL, so cut the value there.
fn error_field_value(value: &str) -> String {
    match value.find('\0') {
        Some(end) => value[..end].to_owned(),
        None => value.to_owned(),
    }
}

fn pg_error(error: turso_pg::Error) -> PgWireError {
    PgWireError::ApiError(Box::new(error))
}

async fn finish_query<C>(
    client: &mut C,
    result: PgWireResult<()>,
    wait_for_sync: bool,
    status: TransactionStatus,
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
        (b'M', error_field_value(&error.message)),
    ];
    for (tag, value) in [
        (b'D', &error.detail),
        (b't', &error.table_name),
        (b'c', &error.column_name),
        (b'n', &error.constraint_name),
    ] {
        if let Some(value) = value {
            fields.push((tag, error_field_value(value)));
        }
    }
    client
        .send(PgWireBackendMessage::ErrorResponse(ErrorResponse::new(
            fields,
        )))
        .await?;
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

    use turso_pg_client::{BackendEvent, ConnParams, PgConn};

    #[test]
    fn error_response_fields_end_at_nul() {
        let (mut client, _conn) = copy_test_client();
        let events = client
            .simple_query(
                "SELECT set_config('nodot' || \"char\"(0) || 'Dinjected detail', 'x', false)",
            )
            .unwrap();
        let fields = events
            .iter()
            .find_map(|event| match event {
                BackendEvent::ErrorResponse(fields) => Some(fields),
                _ => None,
            })
            .unwrap_or_else(|| panic!("{events:?}"));
        assert_eq!(
            fields.get(&b'M').map(String::as_str),
            Some("unrecognized configuration parameter \"nodot"),
            "{fields:?}"
        );
        assert!(!fields.contains_key(&b'D'), "{fields:?}");
        assert!(
            matches!(events.last(), Some(BackendEvent::ReadyForQuery(_))),
            "{events:?}"
        );
    }

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
        client
            .simple_query("SELECT set_config('search_path', '', false)")
            .unwrap();
        let messages = query_messages(&mut client, "COPY items TO STDOUT");
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
        let (_io, db) = turso_pg::open_database(
            ":memory:",
            None,
            turso_pg::OpenFlags::default(),
            turso_pg::DatabaseOpts::new().with_attach(true),
        )
        .unwrap();
        let conn = PgConnection::connect(&db).unwrap();
        let handler = Arc::new(TursoPgHandler {
            conn: conn.clone(),
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
                    process_socket(socket, None, Arc::new(TursoPgFactory { handler }))
                        .await
                        .unwrap();
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

    #[test]
    fn server_side_parse_errors_abort_the_block_on_the_wire() {
        // A syntax error fails the server's own parse of the query string and
        // never reaches PgConnection::prepare, but it is still an error
        // inside the transaction block: later statements must get 25P02 and
        // COMMIT must roll the block back.
        let (mut client, _) = copy_test_client();
        query_messages(&mut client, "CREATE TABLE d1 (x INT)");
        query_messages(&mut client, "BEGIN");
        query_messages(&mut client, "INSERT INTO d1 VALUES (1)");
        let messages = query_messages(&mut client, "SELCT 1");
        assert_eq!(messages.iter().map(|m| m.0).collect::<Vec<_>>(), b"EZ");
        assert_eq!(messages.last().unwrap().1, b"E");
        let messages = query_messages(&mut client, "SELECT 'after'");
        assert_eq!(messages.iter().map(|m| m.0).collect::<Vec<_>>(), b"EZ");
        assert!(
            String::from_utf8_lossy(&messages[0].1).contains("25P02"),
            "{messages:?}"
        );
        let messages = query_messages(&mut client, "COMMIT");
        assert_eq!(messages[0].1, b"ROLLBACK\0");
        assert_eq!(messages.last().unwrap().1, b"I");
        let messages = query_messages(&mut client, "SELECT count(*) FROM d1");
        assert_eq!(messages[1].1, b"\0\x01\0\0\0\x010");
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
        let conn = PgConnection::connect(&db).unwrap();
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
    fn test_aborted_transaction_wire_status_and_commit_tag() {
        let (_io, db) = turso_pg::open_database(
            ":memory:",
            None,
            turso_pg::OpenFlags::default(),
            turso_pg::DatabaseOpts::new(),
        )
        .unwrap();
        let conn = PgConnection::connect(&db).unwrap();
        conn.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();

        let mut begin = conn.prepare("BEGIN").unwrap();
        let response =
            execute_non_query(&mut begin, &turso_pg_parser::parse("BEGIN").unwrap()).unwrap();
        assert!(matches!(
            with_transaction_state(&conn, response),
            Response::TransactionStart(_)
        ));
        assert_eq!(
            wire_transaction_status(&conn),
            TransactionStatus::Transaction
        );

        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        assert!(conn.execute("SELECT 1/0").is_err());
        assert_eq!(wire_transaction_status(&conn), TransactionStatus::Error);

        // COMMIT ends the aborted block as a rollback and reports it so.
        let mut commit = conn.prepare("COMMIT").unwrap();
        let response =
            execute_non_query(&mut commit, &turso_pg_parser::parse("COMMIT").unwrap()).unwrap();
        let wrapped = with_transaction_state(&conn, response);
        let Response::TransactionEnd(tag) = wrapped else {
            panic!("COMMIT must report the end of the transaction block");
        };
        assert_eq!(tag, Tag::new("ROLLBACK"));
        assert_eq!(wire_transaction_status(&conn), TransactionStatus::Idle);
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
        let conn = PgConnection::connect(&db).unwrap();
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
    fn test_functions_that_return_a_custom_type_are_described_as_that_type() {
        let (_io, db) = turso_pg::open_database(
            ":memory:",
            None,
            turso_pg::OpenFlags::default(),
            turso_pg::DatabaseOpts::new(),
        )
        .unwrap();
        let conn = PgConnection::connect(&db).unwrap();
        conn.execute("SELECT set_config('request.jwt.claim.sub', '11111111-2222-3333-4444-555555555555', false)")
            .unwrap();
        for (sql, expected_type) in [
            ("SELECT booleq(true, true)", Type::BOOL),
            ("SELECT now()", Type::TIMESTAMPTZ),
            ("SELECT auth.uid()", Type::UUID),
            ("SELECT CAST(1 AS timestamptz)", Type::TIMESTAMPTZ),
            ("SELECT random()", Type::FLOAT8),
            ("SELECT random() * 10", Type::FLOAT8),
            ("SELECT sqrt(4.0)", Type::FLOAT8),
            ("SELECT exp(1.0)", Type::FLOAT8),
            ("SELECT power(2.0, 3.0)", Type::FLOAT8),
        ] {
            let mut stmt = conn.prepare(sql).unwrap();
            let fields = Arc::new(build_field_info(&stmt, &Format::UnifiedText));
            assert_eq!(*fields[0].datatype(), expected_type, "{sql}");
            let _ = stmt.run_collect_rows().unwrap();
        }

        let mut stmt = conn.prepare("SELECT booleq(true, true)").unwrap();
        let fields = Arc::new(build_field_info(&stmt, &Format::UnifiedText));
        let rows = stmt.run_collect_rows().unwrap();
        let mut encoder = DataRowEncoder::new(fields.clone());
        encode_value(&mut encoder, &rows[0][0], fields[0].datatype()).unwrap();
        assert_eq!(&encoder.finish().unwrap().data[..], b"\0\0\0\x01t");
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
            assert!(
                describe_fields(&stmt, "LOCK TABLE items IN ACCESS SHARE MODE", &format)
                    .unwrap()
                    .is_empty()
            );
        }
        let sql = "SELECT 'LOCK TABLE items'";
        let query = conn.prepare(sql).unwrap();
        assert_eq!(
            describe_fields(&query, sql, &Format::UnifiedText)
                .unwrap()
                .len(),
            1
        );
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
    fn test_pg_bytes_to_value_bytea_rejects_multi_byte_character() {
        let result = pg_bytes_to_value("\\x\u{20ac}a".as_bytes(), &Type::BYTEA);
        assert!(matches!(
            result,
            Err(PgWireError::UserError(info))
                if info.message == "invalid bytea hex parameter: invalid hex at position 0"
        ));
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
        assert_eq!(decode_hex("0aff").unwrap(), vec![0x0A, 0xFF]);
        assert_eq!(
            decode_hex("0123456789abcdefABCDEF").unwrap(),
            vec![0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0xAB, 0xCD, 0xEF]
        );
        assert_eq!(decode_hex("").unwrap(), Vec::<u8>::new());
        assert!(decode_hex("0").is_err()); // odd length
        assert!(decode_hex("GG").is_err()); // invalid hex
        assert!(decode_hex("zz").is_err());
        assert!(decode_hex("+f").is_err());
    }

    #[test]
    fn test_decode_hex_rejects_multi_byte_character() {
        assert_eq!(
            decode_hex("\u{20ac}a"),
            Err("invalid hex at position 0".to_owned())
        );
        assert_eq!(
            decode_hex("a\u{20ac}"),
            Err("invalid hex at position 1".to_owned())
        );
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
}
