use std::fmt::Debug;
use std::num::NonZero;
use std::ops::RangeInclusive;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use async_trait::async_trait;
use chrono::{NaiveDate, NaiveDateTime, TimeDelta};
use futures::{stream, Sink, SinkExt};
use tokio::net::TcpListener;
use tracing::{error, info};
use turso_core::{
    read_temporal, ConnectOpts, Database, LimboError, NonNan, Numeric, StepResult, Temporal,
    TemporalType, Value,
};
use turso_pg::{
    split_statements, sql_parse_error, sqlstate_error, PgConnection, StatementResultKind,
    TransactionState,
};
use turso_pg_parser::cmdtag::CommandTag;
use turso_pg_parser::copy::{parse_copy_to_stdout, CopyToStdout};
use turso_pg_parser::sqlstate::{
    SQLSTATE_DATETIME_FIELD_OVERFLOW, SQLSTATE_FEATURE_NOT_SUPPORTED,
    SQLSTATE_INVALID_BINARY_REPRESENTATION, SQLSTATE_INVALID_PARAMETER_VALUE,
    SQLSTATE_PROTOCOL_VIOLATION,
};
use turso_pg_parser::ParseResult;

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
    /// Every client gets its own connection from `connect`, so session
    /// state such as the current role is not shared between clients.
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
        finish_query(client, result, false, &self.conn).await
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

            if stmt.result_kind() != StatementResultKind::Rows {
                responses.push(execute_non_query(&mut stmt, &parsed)?);
            } else {
                let header = Arc::new(build_field_info(&stmt, &Format::UnifiedText)?);
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
        let parse_error =
            |error: turso_pg_parser::ParseError| pg_error(sql_parse_error(error).into());
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
        finish_query(client, result, true, &self.conn).await
    }

    async fn on_describe<C>(&self, client: &mut C, message: Describe) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let result = self._on_describe(client, message).await;
        finish_query(client, result, true, &self.conn).await
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
        if stmt.result_kind() != StatementResultKind::Rows {
            return execute_non_query(&mut stmt, &parsed)
                .map(|response| with_transaction_state(conn, response));
        }

        let header = Arc::new(build_field_info(&stmt, &portal.result_column_format)?);
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

        let fields = describe_fields(&stmt, &Format::UnifiedText)?;
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

        let fields = describe_fields(&stmt, &portal.result_column_format)?;
        Ok(DescribePortalResponse::new(fields))
    }
}

/// The fields of an ordinary result set, or none for commands and COPY.
fn describe_fields(stmt: &turso_pg::Statement, format: &Format) -> PgWireResult<Vec<FieldInfo>> {
    if stmt.result_kind() != StatementResultKind::Rows {
        return Ok(vec![]);
    }
    build_field_info(stmt, format)
}

fn parse_statement(sql: &str) -> PgWireResult<ParseResult> {
    turso_pg_parser::parse(sql).map_err(|error| pg_error(sql_parse_error(error).into()))
}

/// Build FieldInfo metadata from a prepared statement's column information.
/// A column the client asks for in binary format is refused, before any row
/// is produced, when tursopg cannot send its type in binary format.
fn build_field_info(stmt: &turso_pg::Statement, format: &Format) -> PgWireResult<Vec<FieldInfo>> {
    let binary = binary_formats(format, stmt.num_columns(), "result column")?;
    (0..stmt.num_columns())
        .map(|i| {
            let name = stmt.get_column_name(i).into_owned();
            let pg_type = resolve_pg_type_for_column(stmt, i);
            if !binary[i] {
                return Ok(FieldInfo::new(name, None, None, pg_type, FieldFormat::Text));
            }
            if BinaryEncoding::of_result(&pg_type).is_none() {
                return Err(binary_not_supported(
                    &format!("result column {name}"),
                    &pg_type,
                ));
            }
            Ok(FieldInfo::new(
                name,
                None,
                None,
                pg_type,
                FieldFormat::Binary,
            ))
        })
        .collect()
}

/// For each of `count` parameters or result columns, whether the client
/// asked for binary format (code 1) rather than text (code 0).
fn binary_formats(format: &Format, count: usize, kind: &str) -> PgWireResult<Vec<bool>> {
    let codes = match format {
        Format::UnifiedText => return Ok(vec![false; count]),
        Format::UnifiedBinary => return Ok(vec![true; count]),
        Format::Individual(codes) => codes,
    };
    if codes.len() != count {
        let message = format!(
            "bind message has {} {kind} formats but {count} {kind}s",
            codes.len()
        );
        return Err(wire_error(SQLSTATE_PROTOCOL_VIOLATION, message));
    }
    codes
        .iter()
        .map(|code| match code {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(wire_error(
                SQLSTATE_INVALID_PARAMETER_VALUE,
                format!("unsupported format code: {code}"),
            )),
        })
        .collect()
}

fn binary_not_supported(what: &str, pg_type: &Type) -> PgWireError {
    let message = format!(
        "binary format is not supported for {what} of type {}; use text format",
        pg_type.name()
    );
    wire_error(SQLSTATE_FEATURE_NOT_SUPPORTED, message)
}

fn wire_error(sqlstate: &'static str, message: String) -> PgWireError {
    pg_error(sqlstate_error(sqlstate, message).into())
}

/// How tursopg reads or writes a value of a type in binary format.
#[derive(Clone, Copy)]
enum BinaryEncoding {
    /// The binary format of these types is the text's UTF-8 bytes, the same
    /// bytes as their text format.
    Text,
    Bool,
    /// Read in parameters only: tursopg describes no result column as int2.
    Int2,
    Int4,
    Int8,
    Float8,
    /// date, time, timestamp and timestamptz.
    Temporal(TemporalType),
    Bytea,
}

impl BinaryEncoding {
    /// None for a type whose binary format tursopg neither reads nor writes.
    fn of(pg_type: &Type) -> Option<Self> {
        match *pg_type {
            Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => Some(Self::Text),
            Type::BOOL => Some(Self::Bool),
            Type::INT2 => Some(Self::Int2),
            Type::INT4 => Some(Self::Int4),
            Type::INT8 => Some(Self::Int8),
            Type::FLOAT8 => Some(Self::Float8),
            Type::DATE => Some(Self::Temporal(TemporalType::Date)),
            Type::TIME => Some(Self::Temporal(TemporalType::Time)),
            Type::TIMESTAMP => Some(Self::Temporal(TemporalType::Timestamp)),
            Type::TIMESTAMPTZ => Some(Self::Temporal(TemporalType::Timestamptz)),
            Type::BYTEA => Some(Self::Bytea),
            _ => None,
        }
    }

    /// None for a result column type that tursopg cannot send in binary
    /// format. A time column is one: tursopg stores a timetz as a time
    /// without its offset, so the column may be a timetz, which PostgreSQL
    /// sends as type timetz with the offset.
    fn of_result(pg_type: &Type) -> Option<Self> {
        match *pg_type {
            Type::TIME => None,
            _ => Self::of(pg_type),
        }
    }
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
    let mut encoding_failed = false;

    let result = stmt.run_with_row_callback(|row| {
        let mut encoder = DataRowEncoder::new(header_clone.clone());
        for (i, val) in row.get_values().enumerate() {
            let (pg_type, format) = header_clone
                .get(i)
                .map(|fi| (fi.datatype().clone(), fi.format()))
                .unwrap_or((Type::TEXT, FieldFormat::Text));
            encode_value(&mut encoder, val, &pg_type, format)
                .inspect_err(|_| encoding_failed = true)?;
        }
        rows.push(encoder.finish());
        Ok(())
    });
    if encoding_failed {
        // The statement stopped at a row it could not send. Interrupt it so
        // that it fails like any other statement: its writes are rolled
        // back and a transaction block is aborted. Resetting it instead
        // would commit the writes of an INSERT ... RETURNING.
        stmt.interrupt();
        let interrupted = stmt.step().map_err(pg_error)?;
        assert!(
            matches!(interrupted, StepResult::Interrupt),
            "an interrupted statement stops, got {interrupted:?}"
        );
    }
    result.map_err(pg_error)?;

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
    let binary = binary_formats(
        &portal.parameter_format,
        portal.parameter_len(),
        "parameter",
    )?;
    for (i, (bytes, binary)) in portal.parameters.iter().zip(binary).enumerate() {
        let declared = portal.statement.parameter_types.get(i);
        let value = match bytes {
            None => Value::Null,
            Some(bytes) => {
                let pg_type = declared.and_then(|t| t.as_ref()).unwrap_or(&Type::UNKNOWN);
                if binary {
                    binary_parameter_value(bytes, pg_type, i + 1)?
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

/// Reads parameter `$n` sent in binary format as `pg_type`, the type Parse
/// declared for it. As in PostgreSQL, the type's receive function reads the
/// bytes it needs, and bytes left over are an error. A type whose binary
/// format is its text is read as in text format. A parameter whose type Parse
/// left out or left unspecified (OID 0) has type `unknown` and is refused:
/// PostgreSQL works out its type from the query, and tursopg cannot.
fn binary_parameter_value(bytes: &[u8], pg_type: &Type, n: usize) -> PgWireResult<Value> {
    let Some(encoding) = BinaryEncoding::of(pg_type) else {
        return Err(binary_not_supported(&format!("parameter ${n}"), pg_type));
    };
    let mut rest = bytes;
    let value = match encoding {
        BinaryEncoding::Text => return pg_bytes_to_value(bytes, pg_type),
        BinaryEncoding::Bytea => return Ok(Value::from_blob(bytes.to_vec())),
        BinaryEncoding::Bool => {
            let message = "no data left in message";
            let (byte, tail) = rest
                .split_first()
                .ok_or_else(|| wire_error(SQLSTATE_PROTOCOL_VIOLATION, message.to_owned()))?;
            rest = tail;
            // As in PostgreSQL, every byte but 0 is true.
            Value::from_i64(i64::from(*byte != 0))
        }
        BinaryEncoding::Int2 => Value::from_i64(i16::from_be_bytes(take_bytes(&mut rest)?).into()),
        BinaryEncoding::Int4 => Value::from_i64(i32::from_be_bytes(take_bytes(&mut rest)?).into()),
        BinaryEncoding::Int8 => Value::from_i64(i64::from_be_bytes(take_bytes(&mut rest)?)),
        // tursopg stores no NaN.
        BinaryEncoding::Float8 => match NonNan::new(f64::from_be_bytes(take_bytes(&mut rest)?)) {
            Some(float) => Value::Numeric(Numeric::Float(float)),
            None => {
                let message = format!("NaN is not supported for parameter ${n} of type float8");
                return Err(wire_error(SQLSTATE_FEATURE_NOT_SUPPORTED, message));
            }
        },
        // Bound as PostgreSQL's text for the value in time zone UTC, so that
        // `SELECT $1` returns what PostgreSQL returns. ENCODE reads the `+00`.
        BinaryEncoding::Temporal(ty) => {
            let zone = match ty {
                TemporalType::Timestamptz => "+00",
                TemporalType::Date | TemporalType::Time | TemporalType::Timestamp => "",
            };
            let text = receive_temporal(&mut rest, ty, pg_type)?.text();
            Value::build_text(format!("{text}{zone}"))
        }
    };
    if !rest.is_empty() {
        let message = format!("incorrect binary data format in bind parameter {n}");
        return Err(wire_error(SQLSTATE_INVALID_BINARY_REPRESENTATION, message));
    }
    Ok(value)
}

/// Removes the first `N` bytes of `rest` and returns them. As in PostgreSQL,
/// fewer bytes is a protocol violation.
fn take_bytes<const N: usize>(rest: &mut &[u8]) -> PgWireResult<[u8; N]> {
    let message = "insufficient data left in message";
    let (bytes, tail) = rest
        .split_first_chunk::<N>()
        .ok_or_else(|| wire_error(SQLSTATE_PROTOCOL_VIOLATION, message.to_owned()))?;
    *rest = tail;
    Ok(*bytes)
}

/// Reads a binary date (days since 2000-01-01), time (microseconds since
/// midnight), or timestamp or timestamptz (microseconds since 2000-01-01
/// 00:00:00 UTC). As in PostgreSQL, a value outside its type's range is
/// refused. Infinity and the years outside 1 to 9999 are in PostgreSQL's
/// range, but tursopg does not store them.
fn receive_temporal(rest: &mut &[u8], ty: TemporalType, pg_type: &Type) -> PgWireResult<Temporal> {
    let unsupported = || {
        let message = format!(
            "only {} values in the years 0001 to 9999 are supported",
            pg_type.name()
        );
        wire_error(SQLSTATE_FEATURE_NOT_SUPPORTED, message)
    };
    let out_of_range = |name: &str| {
        wire_error(
            SQLSTATE_DATETIME_FIELD_OVERFLOW,
            format!("{name} out of range"),
        )
    };
    match ty {
        TemporalType::Date => {
            // PostgreSQL sends -infinity and infinity as the smallest and
            // largest values.
            let days = match i32::from_be_bytes(take_bytes(rest)?) {
                i32::MIN | i32::MAX => return Err(unsupported()),
                days if PG_DAYS.contains(&days) => days,
                _ => return Err(out_of_range("date")),
            };
            pg_epoch()
                .checked_add_signed(TimeDelta::days(days.into()))
                .and_then(Temporal::date)
                .ok_or_else(unsupported)
        }
        TemporalType::Time => Temporal::time(i64::from_be_bytes(take_bytes(rest)?))
            .ok_or_else(|| out_of_range("time")),
        TemporalType::Timestamp | TemporalType::Timestamptz => {
            let micros = match i64::from_be_bytes(take_bytes(rest)?) {
                i64::MIN | i64::MAX => return Err(unsupported()),
                micros if PG_MICROS.contains(&micros) => micros,
                _ => return Err(out_of_range("timestamp")),
            };
            NaiveDateTime::from(pg_epoch())
                .checked_add_signed(TimeDelta::microseconds(micros))
                .and_then(Temporal::timestamp)
                .ok_or_else(unsupported)
        }
    }
}

/// PostgreSQL's dates, 4714-11-24 BC to 5874897-12-31, as days since
/// 2000-01-01 (`IS_VALID_DATE`).
const PG_DAYS: RangeInclusive<i32> = -2_451_545..=2_145_031_948;

/// PostgreSQL's timestamps, 4714-11-24 00:00:00 BC to 294276-12-31
/// 23:59:59.999999, as microseconds since 2000-01-01 00:00:00
/// (`IS_VALID_TIMESTAMP`).
const PG_MICROS: RangeInclusive<i64> = -211_813_488_000_000_000..=9_223_371_331_199_999_999;

/// Day 0 of PostgreSQL's binary dates and timestamps.
fn pg_epoch() -> NaiveDate {
    NaiveDate::from_ymd_opt(2000, 1, 1).expect("2000-01-01 is a date")
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
    format: FieldFormat,
) -> turso_core::Result<()> {
    match format {
        FieldFormat::Text => encode_text(encoder, val, pg_type),
        FieldFormat::Binary => encode_binary(encoder, val, pg_type),
    }
}

/// Encodes a value in text format, also in a column sent in binary format.
fn encode_text(
    encoder: &mut DataRowEncoder,
    val: &Value,
    pg_type: &Type,
) -> turso_core::Result<()> {
    let options = FormatOptions::default();
    let text = FieldFormat::Text;
    let encoded = match val {
        Value::Null => {
            encoder.encode_field_with_type_and_format(&None::<i8>, pg_type, text, &options)
        }
        Value::Numeric(turso_core::Numeric::Integer(i)) => {
            // Boolean columns: encode as true/false instead of 0/1
            if *pg_type == Type::BOOL {
                encoder.encode_field_with_type_and_format(&(*i != 0), pg_type, text, &options)
            } else {
                encoder.encode_field_with_type_and_format(i, pg_type, text, &options)
            }
        }
        Value::Numeric(turso_core::Numeric::Float(f)) => {
            encoder.encode_field_with_type_and_format(&f64::from(*f), pg_type, text, &options)
        }
        Value::Text(t) => {
            let value = t.value.as_ref();
            // For TIMESTAMPTZ columns, ensure timezone info is present so clients
            // parse the value correctly (as UTC, not local time).
            // TIMESTAMP (without TZ) should NOT have timezone suffix.
            if *pg_type == Type::TIMESTAMPTZ
                && !value.contains('+')
                && !value.contains('Z')
                && !value.ends_with("-00")
            {
                let with_tz = format!("{value}+00");
                encoder.encode_field_with_type_and_format(
                    &with_tz.as_str(),
                    pg_type,
                    text,
                    &options,
                )
            } else if pg_type.name().starts_with('_') {
                // Array types: pgwire's to_sql_text quotes strings containing
                // {, }, or commas when the type is Kind::Array. Since we store
                // array values as pre-formatted PG array literals (e.g.
                // "{1,2,3}"), encode with Type::TEXT to bypass the quoting.
                encoder.encode_field_with_type_and_format(&value, &Type::TEXT, text, &options)
            } else {
                encoder.encode_field_with_type_and_format(&value, pg_type, text, &options)
            }
        }
        Value::Blob(b) => {
            encoder.encode_field_with_type_and_format(&b.as_slice(), pg_type, text, &options)
        }
    };
    encoded.map_err(|e| LimboError::InternalError(e.to_string()))
}

/// Encodes a value in binary format. The encoding follows the column's type,
/// not the stored value: `count(*)` is typed text, and
/// `coalesce(NULL::float8, 1)` holds an integer. A value that the type's
/// binary format cannot hold is refused, because the client would misread
/// any bytes sent for it.
fn encode_binary(
    encoder: &mut DataRowEncoder,
    val: &Value,
    pg_type: &Type,
) -> turso_core::Result<()> {
    use turso_core::Numeric::{Float, Integer};
    let encoding = BinaryEncoding::of_result(pg_type)
        .expect("build_field_info refuses binary format for a type without a binary encoding");
    let options = FormatOptions::default();
    let binary = FieldFormat::Binary;
    let encoded = match (encoding, val) {
        (BinaryEncoding::Text, _) => return encode_text(encoder, val, pg_type),
        (_, Value::Null) => {
            encoder.encode_field_with_type_and_format(&None::<i8>, pg_type, binary, &options)
        }
        (BinaryEncoding::Bool, Value::Numeric(Integer(i))) => {
            encoder.encode_field_with_type_and_format(&(*i != 0), pg_type, binary, &options)
        }
        (BinaryEncoding::Int4, Value::Numeric(Integer(i))) if i32::try_from(*i).is_ok() => {
            encoder.encode_field_with_type_and_format(&(*i as i32), pg_type, binary, &options)
        }
        (BinaryEncoding::Int8, Value::Numeric(Integer(i))) => {
            encoder.encode_field_with_type_and_format(i, pg_type, binary, &options)
        }
        (BinaryEncoding::Float8, Value::Numeric(Integer(i))) => {
            encoder.encode_field_with_type_and_format(&(*i as f64), pg_type, binary, &options)
        }
        (BinaryEncoding::Float8, Value::Numeric(Float(f))) => {
            encoder.encode_field_with_type_and_format(&f64::from(*f), pg_type, binary, &options)
        }
        (BinaryEncoding::Bytea, Value::Blob(b)) => {
            encoder.encode_field_with_type_and_format(&b.as_slice(), pg_type, binary, &options)
        }
        (BinaryEncoding::Temporal(ty), Value::Text(text)) => {
            match stored_temporal(text.as_str(), ty, pg_type)? {
                Temporal::Date(date) => {
                    let days = i32::try_from((date - pg_epoch()).num_days())
                        .expect("the days between the years 1 and 9999 fit in i32");
                    encoder.encode_field_with_type_and_format(&days, pg_type, binary, &options)
                }
                Temporal::Time(_) => {
                    unreachable!("BinaryEncoding::of_result gives no encoding for time")
                }
                Temporal::Timestamp(timestamp) => {
                    let micros = (timestamp - NaiveDateTime::from(pg_epoch()))
                        .num_microseconds()
                        .expect("the microseconds between the years 1 and 9999 fit in i64");
                    encoder.encode_field_with_type_and_format(&micros, pg_type, binary, &options)
                }
            }
        }
        (
            BinaryEncoding::Bool
            | BinaryEncoding::Int2
            | BinaryEncoding::Int4
            | BinaryEncoding::Int8
            | BinaryEncoding::Float8
            | BinaryEncoding::Temporal(_)
            | BinaryEncoding::Bytea,
            _,
        ) => {
            return Err(sqlstate_error(
                SQLSTATE_FEATURE_NOT_SUPPORTED,
                format!(
                    "cannot send the {} value {val} in binary format as type {}; use text format",
                    val.value_type(),
                    pg_type.name()
                ),
            ))
        }
    };
    encoded.map_err(|e| LimboError::InternalError(e.to_string()))
}

/// The value of `text`, which a column of type `ty` holds. Text that is not
/// exactly what the type's ENCODE stores is refused, such as `'2024-1-5'` in
/// the date column of `SELECT CAST('2024-01-05' AS date) UNION ALL SELECT
/// '2024-1-5'`.
fn stored_temporal(text: &str, ty: TemporalType, pg_type: &Type) -> turso_core::Result<Temporal> {
    match read_temporal(text, ty) {
        Ok(value) if value.text() == text => Ok(value),
        _ => Err(sqlstate_error(
            SQLSTATE_FEATURE_NOT_SUPPORTED,
            format!(
                "cannot send \"{text}\" in binary format as type {0}: it is not in the form a {0} column stores",
                pg_type.name()
            ),
        )),
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
    conn: &PgConnection,
) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    if result.is_err() {
        // Any error inside a transaction block aborts the block, as in PostgreSQL.
        conn.abort_transaction_block_on_error();
    }
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
    let status = wire_transaction_status(conn);
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
            .simple_query("SELECT set_config('nodot' || chr(0) || 'Dinjected detail', 'x', false)")
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
    fn command_results_send_no_rows_and_still_apply_transaction_state() {
        let (mut client, _) = copy_test_client();
        client
            .simple_query("CREATE TABLE command_results (v INT)")
            .unwrap();
        let messages = query_messages(&mut client,
            "BEGIN; SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY; LOCK TABLE command_results IN ACCESS SHARE MODE");
        assert_eq!(messages.iter().map(|m| m.0).collect::<Vec<_>>(), b"CCCZ");
        assert_eq!(messages.last().unwrap().1, b"T");
        let messages = query_messages(&mut client, "INSERT INTO command_results VALUES (1)");
        assert_eq!(messages.iter().map(|m| m.0).collect::<Vec<_>>(), b"EZ");
        assert_eq!(messages.last().unwrap().1, b"E");
        let messages = query_messages(&mut client, "COMMIT");
        assert_eq!(messages.iter().map(|m| m.0).collect::<Vec<_>>(), b"CZ");
        assert_eq!(messages[0].1, b"ROLLBACK\0");
        assert_eq!(messages.last().unwrap().1, b"I");
        let messages = query_messages(&mut client, "SELECT v FROM command_results WHERE false");
        assert_eq!(messages.iter().map(|m| m.0).collect::<Vec<_>>(), b"TCZ");
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
        conn.execute(
            r#"CREATE SCHEMA other;
               CREATE TABLE public.items (id INT);
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
    fn binary_results_follow_the_column_type() {
        let (_io, db) = turso_pg::open_database(
            ":memory:",
            None,
            turso_pg::OpenFlags::default(),
            turso_pg::DatabaseOpts::new(),
        )
        .unwrap();
        let conn = PgConnection::connect(&db).unwrap();
        conn.execute("CREATE TABLE t (i int, b bigint, x bytea)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (42, 42, 'abc'::bytea)")
            .unwrap();
        // Each field is a 4-byte length followed by the value.
        for (sql, expected_type, binary, text) in [
            (
                "SELECT 42::int4",
                Type::INT4,
                &b"\0\0\0\x04\0\0\0\x2a"[..],
                &b"\0\0\0\x0242"[..],
            ),
            (
                "SELECT -1::int4",
                Type::INT4,
                b"\0\0\0\x04\xff\xff\xff\xff",
                b"\0\0\0\x02-1",
            ),
            (
                "SELECT i FROM t",
                Type::INT4,
                b"\0\0\0\x04\0\0\0\x2a",
                b"\0\0\0\x0242",
            ),
            (
                "SELECT b FROM t",
                Type::INT8,
                b"\0\0\0\x08\0\0\0\0\0\0\0\x2a",
                b"\0\0\0\x0242",
            ),
            (
                "SELECT 9000000000::int8",
                Type::INT8,
                b"\0\0\0\x08\0\0\0\x02\x18\x71\x1a\0",
                b"\0\0\0\x0a9000000000",
            ),
            (
                "SELECT int8(9000000000)",
                Type::INT8,
                b"\0\0\0\x08\0\0\0\x02\x18\x71\x1a\0",
                b"\0\0\0\x0a9000000000",
            ),
            (
                "SELECT NULL::int4",
                Type::INT4,
                b"\xff\xff\xff\xff",
                b"\xff\xff\xff\xff",
            ),
            (
                "SELECT x FROM t",
                Type::BYTEA,
                b"\0\0\0\x03abc",
                b"\0\0\0\x08\\x616263",
            ),
            (
                "SELECT 1.5::float8",
                Type::FLOAT8,
                b"\0\0\0\x08\x3f\xf8\0\0\0\0\0\0",
                b"\0\0\0\x031.5",
            ),
            ("SELECT true", Type::BOOL, b"\0\0\0\x01\x01", b"\0\0\0\x01t"),
            // The bytes PostgreSQL 14 sends, with time zone UTC.
            (
                "SELECT CAST('2024-01-05' AS date)",
                Type::DATE,
                b"\0\0\0\x04\0\0\x22\x42",
                b"\0\0\0\x0a2024-01-05",
            ),
            (
                "SELECT CAST('2024-01-05 10:00:00.5' AS timestamp)",
                Type::TIMESTAMP,
                b"\0\0\0\x08\0\x02\xb1\x2e\xae\x12\xc9\x20",
                b"\0\0\0\x152024-01-05 10:00:00.5",
            ),
            (
                "SELECT CAST('2024-01-05 10:00:00.5+02' AS timestamptz)",
                Type::TIMESTAMPTZ,
                b"\0\0\0\x08\0\x02\xb1\x2d\0\xeb\x81\x20",
                b"\0\0\0\x182024-01-05 08:00:00.5+00",
            ),
            // The column's type, not the stored value, picks the encoding.
            (
                "SELECT count(*) FROM t",
                Type::TEXT,
                b"\0\0\0\x011",
                b"\0\0\0\x011",
            ),
            (
                "SELECT coalesce(NULL::float8, 1)",
                Type::FLOAT8,
                b"\0\0\0\x08\x3f\xf0\0\0\0\0\0\0",
                b"\0\0\0\x011",
            ),
        ] {
            for (format, expected) in [(Format::UnifiedBinary, binary), (Format::UnifiedText, text)]
            {
                let mut stmt = conn.prepare(sql).unwrap();
                let fields = Arc::new(build_field_info(&stmt, &format).unwrap());
                assert_eq!(*fields[0].datatype(), expected_type, "{sql}");
                let rows = stmt.run_collect_rows().unwrap();
                let mut encoder = DataRowEncoder::new(fields.clone());
                encode_value(
                    &mut encoder,
                    &rows[0][0],
                    fields[0].datatype(),
                    fields[0].format(),
                )
                .unwrap();
                assert_eq!(&encoder.finish().unwrap().data[..], expected, "{sql}");
            }
        }
    }

    #[test]
    fn binary_formats_are_refused_where_the_bytes_would_be_wrong() {
        let conn = memory_connection();
        conn.execute("CREATE TABLE u (u uuid)").unwrap();
        // PostgreSQL sends the timetz as 23:30:00 with offset -03.
        for sql in ["SELECT u FROM u", "SELECT CAST('23:30:00-03' AS timetz)"] {
            let stmt = conn.prepare(sql).unwrap();
            let error = build_field_info(&stmt, &Format::UnifiedBinary).unwrap_err();
            assert_eq!(sqlstate(error), SQLSTATE_FEATURE_NOT_SUPPORTED, "{sql}");
        }

        // UNION ALL types the column by its first SELECT, so a date column
        // can hold text that a date column never stores.
        let sql = "SELECT CAST('2024-01-05' AS date) UNION ALL SELECT '2024-1-5'";
        let mut union = conn.prepare(sql).unwrap();
        let fields = Arc::new(build_field_info(&union, &Format::UnifiedBinary).unwrap());
        let Err(error) = execute_query(&mut union, fields) else {
            panic!("{sql} must fail");
        };
        assert_eq!(sqlstate(error), SQLSTATE_FEATURE_NOT_SUPPORTED);

        let codes = |codes: Vec<i16>, count| binary_formats(&Format::Individual(codes), count, "x");
        assert_eq!(codes(vec![0, 1], 2).unwrap(), [false, true]);
        assert_eq!(
            sqlstate(codes(vec![1, 1], 1).unwrap_err()),
            SQLSTATE_PROTOCOL_VIOLATION
        );
        let error = codes(vec![0, 2], 2).unwrap_err();
        assert_eq!(sqlstate(error), SQLSTATE_INVALID_PARAMETER_VALUE);

        // Parameter types declared in Parse, the binary bytes, and the value
        // bound or the SQLSTATE of PostgreSQL 14's error.
        let one = |pg_type: Type| vec![Some(pg_type)];
        let (be4, be8) = (
            |i: i32| i.to_be_bytes().to_vec(),
            |i: i64| i.to_be_bytes().to_vec(),
        );
        let text = |text: &'static str| Ok(Value::from_text(text));
        let unsupported = SQLSTATE_FEATURE_NOT_SUPPORTED;
        let range = SQLSTATE_DATETIME_FIELD_OVERFLOW;
        for (declared, bytes, expected) in [
            (one(Type::TEXT), b"42".to_vec(), text("42")),
            // Type OID 0: PostgreSQL reads `SELECT $1::int4` with these bytes
            // as 42.
            (vec![None], be4(42), Err(unsupported)),
            (
                one(Type::BYTEA),
                b"\\x41".to_vec(),
                Ok(Value::from_blob(b"\\x41".to_vec())),
            ),
            (one(Type::UUID), vec![0; 16], Err(unsupported)),
            (vec![], b"42".to_vec(), Err(unsupported)),
            (one(Type::BOOL), vec![], Err(SQLSTATE_PROTOCOL_VIOLATION)),
            (one(Type::BOOL), vec![2], Ok(Value::from_i64(1))),
            (one(Type::INT2), vec![0x80, 0], Ok(Value::from_i64(-32768))),
            (one(Type::INT4), be4(-1), Ok(Value::from_i64(-1))),
            (one(Type::INT8), be8(42), Ok(Value::from_i64(42))),
            (
                one(Type::INT4),
                vec![0; 3],
                Err(SQLSTATE_PROTOCOL_VIOLATION),
            ),
            (
                one(Type::INT4),
                vec![0; 5],
                Err(SQLSTATE_INVALID_BINARY_REPRESENTATION),
            ),
            (
                one(Type::FLOAT8),
                1.5f64.to_be_bytes().to_vec(),
                Ok(Value::from_f64(1.5)),
            ),
            (
                one(Type::FLOAT8),
                f64::NAN.to_be_bytes().to_vec(),
                Err(unsupported),
            ),
            (one(Type::DATE), be4(8770), text("2024-01-05")),
            // infinity
            (one(Type::DATE), be4(i32::MAX), Err(unsupported)),
            (one(Type::DATE), be4(i32::MAX - 1), Err(range)),
            // 0000-12-31, the day before 0001-01-01
            (one(Type::DATE), be4(-730_120), Err(unsupported)),
            (one(Type::TIME), be8(86_400_000_000), text("24:00:00")),
            (one(Type::TIME), be8(86_400_000_001), Err(range)),
            (
                one(Type::TIMESTAMP),
                be8(757_764_000_500_000),
                text("2024-01-05 10:00:00.5"),
            ),
            // -infinity
            (one(Type::TIMESTAMP), be8(i64::MIN), Err(unsupported)),
            (one(Type::TIMESTAMP), be8(i64::MAX - 1), Err(range)),
            // 10000-01-01 00:00:00
            (
                one(Type::TIMESTAMP),
                be8(252_455_616_000_000_000),
                Err(unsupported),
            ),
            (
                one(Type::TIMESTAMPTZ),
                be8(757_756_800_500_000),
                text("2024-01-05 08:00:00.5+00"),
            ),
        ] {
            let mut portal = Portal::<String>::default();
            portal.statement =
                Arc::new(StoredStatement::new(String::new(), String::new(), declared));
            portal.parameter_format = Format::UnifiedBinary;
            portal.parameters = vec![Some(bytes.clone().into())];
            let mut stmt = conn.prepare("SELECT $1").unwrap();
            match (bind_portal_parameters(&mut stmt, &portal), expected) {
                (Ok(()), Ok(value)) => assert_eq!(stmt.run_collect_rows().unwrap(), [[value]]),
                (Err(error), Err(expected)) => assert_eq!(sqlstate(error), expected, "{bytes:?}"),
                (result, expected) => panic!("{bytes:?}: {result:?}, expected {expected:?}"),
            }
        }
    }

    #[test]
    fn a_row_refused_in_binary_format_leaves_no_write() {
        let conn = memory_connection();
        conn.execute("CREATE TABLE o (i int)").unwrap();
        // 3000000000 is outside the range of int4's 4-byte binary format.
        let insert = "INSERT INTO o VALUES (3000000000) RETURNING i";
        for in_block in [false, true] {
            if in_block {
                conn.execute("BEGIN").unwrap();
            }
            let mut stmt = conn.prepare(insert).unwrap();
            let fields = Arc::new(build_field_info(&stmt, &Format::UnifiedBinary).unwrap());
            let Err(error) = execute_query(&mut stmt, fields) else {
                panic!("{insert} must fail");
            };
            assert_eq!(sqlstate(error), SQLSTATE_FEATURE_NOT_SUPPORTED);
            drop(stmt);
            if in_block {
                assert_eq!(wire_transaction_status(&conn), TransactionStatus::Error);
                conn.execute("COMMIT").unwrap();
            }
            let mut count = conn.prepare("SELECT count(*) FROM o").unwrap();
            assert_eq!(count.run_collect_rows().unwrap(), [[Value::from_i64(0)]]);
        }
    }

    #[test]
    fn a_refused_binary_format_aborts_the_transaction_block() {
        let (mut client, _conn) = copy_test_client();
        client.simple_query("CREATE TABLE u (u uuid)").unwrap();
        client.simple_query("BEGIN").unwrap();
        // Parse, Bind asking for the result in binary format, Execute, Sync.
        client
            .write_message(b'P', b"\0SELECT u FROM u\0\0\0")
            .unwrap();
        client
            .write_message(b'B', b"\0\0\0\0\0\0\0\x01\0\x01")
            .unwrap();
        client.write_message(b'E', b"\0\0\0\0\0").unwrap();
        client.write_message(b'S', b"").unwrap();
        let mut events = Vec::new();
        while !matches!(events.last(), Some(BackendEvent::ReadyForQuery(_))) {
            events.push(client.read_event().unwrap());
        }
        let [_, _, BackendEvent::ErrorResponse(fields), BackendEvent::ReadyForQuery(b'E')] =
            &events[..]
        else {
            panic!("{events:?}");
        };
        assert_eq!(fields[&b'C'], SQLSTATE_FEATURE_NOT_SUPPORTED);
        let events = client.simple_query("COMMIT").unwrap();
        assert!(matches!(&events[0], BackendEvent::CommandComplete(tag) if tag == "ROLLBACK"));
    }

    fn memory_connection() -> PgConnection {
        let (_io, db) = turso_pg::open_database(
            ":memory:",
            None,
            turso_pg::OpenFlags::default(),
            turso_pg::DatabaseOpts::new(),
        )
        .unwrap();
        PgConnection::connect(&db).unwrap()
    }

    fn sqlstate(error: PgWireError) -> &'static str {
        let PgWireError::ApiError(error) = error else {
            panic!("not an SQL error: {error:?}");
        };
        error.downcast::<turso_pg::Error>().unwrap().sqlstate
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
                let fields = Arc::new(build_field_info(&stmt, &format).unwrap());
                assert_eq!(*fields[0].datatype(), Type::TEXT, "{sql}");
                let rows = stmt.run_collect_rows().unwrap();
                assert_eq!(rows.len(), 1);
                let mut encoder = DataRowEncoder::new(fields.clone());
                encode_value(
                    &mut encoder,
                    &rows[0][0],
                    fields[0].datatype(),
                    fields[0].format(),
                )
                .unwrap();
                let row = encoder.finish().unwrap();
                assert_eq!(row.field_count, 1);
                assert_eq!(&row.data[..], b"\0\0\0\x01t");
            }
        }
        let stmt = conn.prepare("SELECT 7 AS pg_is_in_recovery").unwrap();
        let fields = build_field_info(&stmt, &Format::UnifiedText).unwrap();
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
            let fields = build_field_info(&stmt, &Format::UnifiedText).unwrap();
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
            (
                "SELECT CAST('2024-01-05 10:00:00+00' AS timestamptz)",
                Type::TIMESTAMPTZ,
            ),
            ("SELECT random()", Type::FLOAT8),
            ("SELECT random() * 10", Type::FLOAT8),
            ("SELECT array_to_json(ARRAY[1])", Type::JSON),
        ] {
            let mut stmt = conn.prepare(sql).unwrap();
            let fields = Arc::new(build_field_info(&stmt, &Format::UnifiedText).unwrap());
            assert_eq!(*fields[0].datatype(), expected_type, "{sql}");
            let _ = stmt.run_collect_rows().unwrap();
        }

        let mut stmt = conn.prepare("SELECT booleq(true, true)").unwrap();
        let fields = Arc::new(build_field_info(&stmt, &Format::UnifiedText).unwrap());
        let rows = stmt.run_collect_rows().unwrap();
        let mut encoder = DataRowEncoder::new(fields.clone());
        encode_value(
            &mut encoder,
            &rows[0][0],
            fields[0].datatype(),
            fields[0].format(),
        )
        .unwrap();
        assert_eq!(&encoder.finish().unwrap().data[..], b"\0\0\0\x01t");
    }

    #[test]
    fn commands_and_copy_have_no_ordinary_result_fields() {
        let (_io, db) = turso_pg::open_database(
            ":memory:",
            None,
            turso_pg::OpenFlags::default(),
            turso_pg::DatabaseOpts::new(),
        )
        .unwrap();
        let conn = turso_pg::PgConnection::connect(&db).unwrap();
        conn.execute("CREATE TABLE items (v INTEGER)").unwrap();
        for sql in [
            "LOCK TABLE items IN ACCESS SHARE MODE",
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY",
            "RESET ALL",
            "COPY items TO STDOUT",
        ] {
            let stmt = conn.prepare(sql).unwrap();
            for format in [Format::UnifiedText, Format::UnifiedBinary] {
                assert!(describe_fields(&stmt, &format).unwrap().is_empty(), "{sql}");
            }
        }
        let sql = "SELECT 'LOCK TABLE items'";
        let query = conn.prepare(sql).unwrap();
        assert_eq!(
            describe_fields(&query, &Format::UnifiedText).unwrap().len(),
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
