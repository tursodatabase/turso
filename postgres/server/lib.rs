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
use turso_core::{ConnectOpts, Database, StatementTimes, Value, WallClockInstant};
use turso_pg::{
    parse_pg_bool, split_statements, sql_parse_error, sqlstate_error, type_input, PgConnection,
    StatementResultKind, TransactionState,
};
use turso_pg_parser::cmdtag::CommandTag;
use turso_pg_parser::copy::{parse_copy_to_stdout, CopyToStdout};
use turso_pg_parser::sqlstate::{
    SQLSTATE_FEATURE_NOT_SUPPORTED, SQLSTATE_INVALID_PARAMETER_VALUE,
    SQLSTATE_INVALID_TEXT_REPRESENTATION, SQLSTATE_PROTOCOL_VIOLATION,
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
use pgwire::api::stmt::{QueryParser, StoredStatement};
use pgwire::api::store::PortalStore;
use pgwire::api::{
    ClientInfo, ClientPortalStore, NoopHandler, PgWireConnectionState, PgWireServerHandlers, Type,
    DEFAULT_NAME,
};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::copy::{CopyData, CopyDone, CopyOutResponse};
use pgwire::messages::data::DataRow;
use pgwire::messages::extendedquery::{Bind, BindComplete, Describe, Execute, Sync as PgSync};
use pgwire::messages::response::{
    EmptyQueryResponse, ErrorResponse, ReadyForQuery, TransactionStatus,
};
use pgwire::messages::simplequery::Query;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
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
                                handler: Arc::new(TursoPgHandler::new(session.clone())),
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
    query_parser: Arc<TimedQueryParser>,
    message_times: Arc<MessageTimes>,
}

impl TursoPgHandler {
    fn new(conn: PgConnection) -> Self {
        let message_times = Arc::new(MessageTimes {
            conn: conn.clone(),
            implicit_transaction_start: Mutex::new(None),
        });
        Self {
            conn,
            query_parser: Arc::new(TimedQueryParser {
                message_times: message_times.clone(),
            }),
            message_times,
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
        Arc::new(UtcOnlyStartupHandler)
    }
}

/// Accepts a connection like `NoopHandler`, except when the startup packet
/// asks for a session time zone other than UTC.
struct UtcOnlyStartupHandler;

#[async_trait]
impl StartupHandler for UtcOnlyStartupHandler {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if let PgWireFrontendMessage::Startup(startup) = &message {
            // PostgreSQL gives the TimeZone parameter priority over `options`.
            // Refusing any non-UTC zone needs no precedence rule.
            let non_utc = startup
                .parameters
                .iter()
                .flat_map(|(name, value)| -> Vec<&str> {
                    if name.eq_ignore_ascii_case("timezone") {
                        vec![value.as_str()]
                    } else if name == "options" {
                        time_zones_in_options(value)
                    } else {
                        Vec::new()
                    }
                })
                .find(|zone| !is_utc(zone));
            if let Some(zone) = non_utc {
                let mut info = ErrorInfo::new(
                    "FATAL".to_owned(),
                    SQLSTATE_FEATURE_NOT_SUPPORTED.to_owned(),
                    format!(
                        "time zone \"{}\" is not supported; tursopg supports only UTC",
                        error_field_value(zone)
                    ),
                );
                info.hint = Some(
                    "Set the TimeZone connection parameter to UTC (in psql, PGTZ=UTC). Times with time zone are then shown in UTC."
                        .to_owned(),
                );
                return Err(PgWireError::UserError(Box::new(info)));
            }
        }
        NoopHandler.on_startup(client, message).await
    }
}

/// Every time zone that the `options` startup parameter sets, with
/// `-c TimeZone=x`, `-cTimeZone=x` or `--TimeZone=x`.
fn time_zones_in_options(options: &str) -> Vec<&str> {
    let mut zones = Vec::new();
    let mut words = options.split_whitespace();
    while let Some(word) = words.next() {
        let setting = if word == "-c" {
            words.next()
        } else {
            word.strip_prefix("-c").or_else(|| word.strip_prefix("--"))
        };
        if let Some((name, value)) = setting.and_then(|setting| setting.split_once('=')) {
            if name.eq_ignore_ascii_case("timezone") {
                zones.push(value);
            }
        }
    }
    zones
}

/// Whether `zone` names UTC exactly.
fn is_utc(zone: &str) -> bool {
    const UTC_NAMES: [&str; 11] = [
        "utc",
        "etc/utc",
        "gmt",
        "etc/gmt",
        "uct",
        "universal",
        "zulu",
        "+00",
        "+00:00",
        "00:00",
        "0",
    ];
    let zone = zone.trim();
    UTC_NAMES.iter().any(|name| zone.eq_ignore_ascii_case(name))
}

#[async_trait]
impl SimpleQueryHandler for TursoPgHandler {
    async fn on_query<C>(&self, client: &mut C, query: Query) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let _times = self.message_times.simple_query_received();
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
    type QueryParser = TimedQueryParser;

    async fn on_execute<C>(&self, client: &mut C, message: Execute) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let _times = self.message_times.extended_message_received();
        let in_transaction_block = !self.conn.inner().get_auto_commit();
        let result = self._on_execute(client, message).await;
        if in_transaction_block && self.conn.inner().get_auto_commit() {
            self.message_times.transaction_ended();
        }
        finish_query(client, result, true, &self.conn).await
    }

    async fn on_bind<C>(&self, client: &mut C, message: Bind) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        self.message_times.extended_message_received();
        let statement_name = message.statement_name.as_deref().unwrap_or(DEFAULT_NAME);
        let Some(statement) = client.portal_store().get_statement(statement_name) else {
            return Err(PgWireError::StatementNotFound(statement_name.to_owned()));
        };
        let portal = Portal::try_new(&message, statement)?;
        client.portal_store().put_portal(Arc::new(portal));
        client
            .send(PgWireBackendMessage::BindComplete(BindComplete::new()))
            .await?;
        Ok(())
    }

    async fn on_sync<C>(&self, client: &mut C, _message: PgSync) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        self.message_times.sync_received();
        client
            .send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                client.transaction_status(),
            )))
            .await?;
        client.flush().await?;
        Ok(())
    }

    async fn on_describe<C>(&self, client: &mut C, message: Describe) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let _times = self.message_times.extended_message_received();
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
        if has_no_statements(query)? {
            return Ok(Response::EmptyQuery);
        }

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
        if has_no_statements(&target.statement)? {
            return Ok(DescribeStatementResponse::new(vec![], vec![]));
        }
        let stmt = conn.prepare(&target.statement).map_err(pg_error)?;

        // FIXME(pg-catalog): This describes a parameter with no declared type as text, where PostgreSQL gives the type its context requires. A typed expression tree replaces this.
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
        if has_no_statements(&portal.statement.statement)? {
            return Ok(DescribePortalResponse::new(vec![]));
        }
        let stmt = conn
            .prepare(&portal.statement.statement)
            .map_err(pg_error)?;

        let fields = describe_fields(&stmt, &portal.result_column_format)?;
        Ok(DescribePortalResponse::new(fields))
    }
}

/// The clock of the messages of one session, for the times that
/// `now()` and `statement_timestamp()` report. PostgreSQL takes the time of
/// a statement from the time it received the client message, and an
/// implicit transaction starts with the first message after the previous
/// Sync.
struct MessageTimes {
    conn: PgConnection,
    /// The first extended-protocol message since the last Sync or the end
    /// of a transaction block.
    implicit_transaction_start: Mutex<Option<WallClockInstant>>,
}

impl MessageTimes {
    /// A Query message is a whole implicit transaction.
    fn simple_query_received(&self) -> CommandTimesGuard<'_> {
        let now = self.conn.inner().current_time_wall_clock();
        self.set(StatementTimes {
            statement: now,
            transaction: now,
        })
    }

    fn extended_message_received(&self) -> CommandTimesGuard<'_> {
        let now = self.conn.inner().current_time_wall_clock();
        let transaction = *self
            .implicit_transaction_start
            .lock()
            .expect("the lock is not held across a panic")
            .get_or_insert(now);
        self.set(StatementTimes {
            statement: now,
            transaction,
        })
    }

    fn sync_received(&self) {
        self.transaction_ended();
    }

    /// The next message starts a new transaction, as after COMMIT or
    /// ROLLBACK of a transaction block in PostgreSQL.
    fn transaction_ended(&self) {
        *self
            .implicit_transaction_start
            .lock()
            .expect("the lock is not held across a panic") = None;
    }

    fn set(&self, times: StatementTimes) -> CommandTimesGuard<'_> {
        self.conn.inner().set_command_times(Some(times));
        CommandTimesGuard(&self.conn)
    }
}

/// Clears the times of the message when its handling ends.
struct CommandTimesGuard<'a>(&'a PgConnection);

impl Drop for CommandTimesGuard<'_> {
    fn drop(&mut self) {
        self.0.inner().set_command_times(None);
    }
}

/// Keeps the text of a statement as it is, and starts the implicit
/// transaction at the Parse message when it is the first message since Sync.
struct TimedQueryParser {
    message_times: Arc<MessageTimes>,
}

#[async_trait]
impl QueryParser for TimedQueryParser {
    type Statement = String;

    async fn parse_sql<C>(
        &self,
        _client: &C,
        sql: &str,
        _types: &[Option<Type>],
    ) -> PgWireResult<Self::Statement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        self.message_times.extended_message_received();
        Ok(sql.to_owned())
    }
}

/// Whether `sql` holds only whitespace, semicolons and comments. PostgreSQL
/// answers Describe of such a query with NoData and Execute with
/// EmptyQueryResponse, not an error.
fn has_no_statements(sql: &str) -> PgWireResult<bool> {
    let statements = turso_pg_parser::split_statements(sql)
        .map_err(|error| pg_error(sql_parse_error(error).into()))?;
    Ok(statements.is_empty())
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
            if BinaryEncoding::of(&pg_type).is_none() {
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
    pg_error(sqlstate_error(sqlstate, message, None).into())
}

/// How tursopg reads or writes a value of a type in binary format.
#[derive(Clone, Copy)]
enum BinaryEncoding {
    /// The binary format of these types is the text's UTF-8 bytes, the same
    /// bytes as their text format.
    Text,
    Bool,
    Int2,
    Int4,
    Int8,
    Float8,
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
            Type::BYTEA => Some(Self::Bytea),
            _ => None,
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

/// FIXME(pg-catalog): This hand-codes typarray of pg_type, and sends an array of any type it does not list as text[]. A lookup of typarray in a pg_type table replaces this.
/// Map a scalar PG type to its array counterpart.
fn scalar_pg_type_to_array_type(scalar: &Type) -> Type {
    if *scalar == Type::INT2 {
        Type::INT2_ARRAY
    } else if *scalar == Type::INT4 {
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
    let text_types: Vec<turso_pg::TextType> = (0..stmt.num_columns())
        .map(|i| turso_pg::TextType::of_column(stmt, i))
        .collect();
    let mut encode_failed = false;

    let result = stmt.run_with_row_callback(|row| {
        let mut encoder = DataRowEncoder::new(header_clone.clone());
        for (i, val) in row.get_values().enumerate() {
            let (pg_type, format) = header_clone
                .get(i)
                .map(|fi| (fi.datatype().clone(), fi.format()))
                .unwrap_or((Type::TEXT, FieldFormat::Text));
            encode_value(&mut encoder, val, &pg_type, format, text_types[i]).inspect_err(|_| {
                encode_failed = true;
            })?;
        }
        rows.push(encoder.finish());
        Ok(())
    });
    if encode_failed {
        stop_statement(stmt)?;
    }
    result.map_err(pg_error)?;

    let data_stream = stream::iter(rows);
    Ok(Response::Query(QueryResponse::new(header, data_stream)))
}

/// Stops `stmt` after a row it returned could not be sent, and rolls back the
/// statement's changes. Resetting the statement instead would commit the rows
/// that an autocommit `INSERT ... RETURNING` has already written.
fn stop_statement(stmt: &mut turso_pg::Statement) -> PgWireResult<()> {
    stmt.interrupt();
    match stmt.run_ignore_rows() {
        Err(error) if matches!(error.core_error(), turso_core::LimboError::Interrupt) => Ok(()),
        Err(error) => Err(pg_error(error)),
        Ok(()) => unreachable!("a statement interrupted after returning a row ran to its end"),
    }
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

/// FIXME(pg-catalog): This refuses a binary parameter of unknown type because Turso cannot work out its type from the query. A typed expression tree replaces this refusal.
/// Reads parameter `$n` sent in binary format as `pg_type`, the type Parse
/// declared for it. A type whose binary format is its text is read as in text
/// format. A parameter whose type Parse left out or left unspecified (OID 0)
/// has type `unknown` and is refused: PostgreSQL works out its type from the
/// query, and tursopg cannot.
fn binary_parameter_value(bytes: &[u8], pg_type: &Type, n: usize) -> PgWireResult<Value> {
    match BinaryEncoding::of(pg_type) {
        Some(BinaryEncoding::Text) => pg_bytes_to_value(bytes, pg_type),
        Some(BinaryEncoding::Bytea) => Ok(Value::from_blob(bytes.to_vec())),
        Some(
            BinaryEncoding::Bool
            | BinaryEncoding::Int2
            | BinaryEncoding::Int4
            | BinaryEncoding::Int8
            | BinaryEncoding::Float8,
        )
        | None => Err(binary_not_supported(&format!("parameter ${n}"), pg_type)),
    }
}

/// FIXME(pg-catalog): This hand-codes the input function (typinput in pg_type) of each declared parameter type, and differs from it: it reads numeric as a float. A call to the typinput function of the declared type replaces this.
/// Convert raw parameter bytes to a turso Value based on the PostgreSQL type.
/// Assumes text format encoding (UTF-8 string representations).
fn pg_bytes_to_value(bytes: &[u8], pg_type: &Type) -> PgWireResult<Value> {
    let text = std::str::from_utf8(bytes).map_err(|e| {
        PgWireError::UserError(Box::new(error_info(&format!(
            "invalid UTF-8 in parameter: {e}"
        ))))
    })?;

    if let Some(read) = type_input(i64::from(pg_type.oid())) {
        return read(text).map_err(|error| pg_error(error.into()));
    }

    match *pg_type {
        Type::FLOAT4 | Type::FLOAT8 | Type::NUMERIC => {
            let f: f64 = text.parse().map_err(|e| {
                PgWireError::UserError(Box::new(error_info(&format!(
                    "invalid float parameter: {e}"
                ))))
            })?;
            Ok(Value::from_f64(f))
        }
        Type::BOOL => match parse_pg_bool(text) {
            Some(truth) => Ok(Value::from_i64(i64::from(truth))),
            None => Err(wire_error(
                SQLSTATE_INVALID_TEXT_REPRESENTATION,
                format!("invalid input syntax for type boolean: \"{text}\""),
            )),
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
        // FIXME(pg-catalog): PostgreSQL resolves an unknown-type parameter from its context, as an operand of an operator or a function or as a target column. A typed expression tree replaces this guess.
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
    text_type: turso_pg::TextType,
) -> turso_core::Result<()> {
    match format {
        FieldFormat::Binary => encode_binary(encoder, val, pg_type, text_type),
        FieldFormat::Text => encode_text(encoder, val, text_type)
            .map_err(|e| turso_core::LimboError::InternalError(e.to_string())),
    }
}

/// Writes `val` in text format, whatever format the column's header names.
fn encode_text(
    encoder: &mut DataRowEncoder,
    val: &Value,
    text_type: turso_pg::TextType,
) -> PgWireResult<()> {
    let text = turso_pg::value_text(val, text_type);
    // pgwire writes a string as it is for Type::TEXT. For an array type it
    // would quote the string, which already is an array literal like {1,2,3}.
    encoder.encode_field_with_type_and_format(
        &text.as_deref(),
        &Type::TEXT,
        FieldFormat::Text,
        &FormatOptions::default(),
    )
}

/// Binary format writes a value in the encoding of its column's type, not of
/// the stored value: `count(*)` is typed text, and `coalesce(NULL::float8, 1)`
/// holds an integer. A value that the type cannot hold, such as 12.5 in the
/// int4 column of `SELECT 1 UNION ALL SELECT 12.5`, is refused with 0A000.
fn encode_binary(
    encoder: &mut DataRowEncoder,
    val: &Value,
    pg_type: &Type,
    text_type: turso_pg::TextType,
) -> turso_core::Result<()> {
    use turso_core::Numeric::{Float, Integer};
    use BinaryEncoding::{Bool, Bytea, Float8, Int2, Int4, Int8, Text};
    let encoding = BinaryEncoding::of(pg_type)
        .expect("build_field_info refuses binary format for a type without a binary encoding");
    let written = match (encoding, val) {
        (Text, _) => encode_text(encoder, val, text_type),
        (_, Value::Null) => encoder.encode_field(&None::<i8>),
        (Bool, Value::Numeric(Integer(i))) => encoder.encode_field(&(*i != 0)),
        (Int2, Value::Numeric(Integer(i))) => match i16::try_from(*i) {
            Ok(i) => encoder.encode_field(&i),
            Err(_) => return Err(binary_value_not_supported(pg_type)),
        },
        (Int4, Value::Numeric(Integer(i))) => match i32::try_from(*i) {
            Ok(i) => encoder.encode_field(&i),
            Err(_) => return Err(binary_value_not_supported(pg_type)),
        },
        (Int8, Value::Numeric(Integer(i))) => encoder.encode_field(i),
        (Float8, Value::Numeric(Integer(i))) => encoder.encode_field(&(*i as f64)),
        (Float8, Value::Numeric(Float(f))) => encoder.encode_field(&f64::from(*f)),
        (Bytea, Value::Blob(b)) => encoder.encode_field(&b.as_slice()),
        (Bool | Int2 | Int4 | Int8, Value::Numeric(Float(_)))
        | (Bytea, Value::Numeric(_))
        | (Bool | Int2 | Int4 | Int8 | Float8 | Bytea, Value::Text(_))
        | (Bool | Int2 | Int4 | Int8 | Float8, Value::Blob(_)) => {
            return Err(binary_value_not_supported(pg_type))
        }
    };
    written.map_err(|e| turso_core::LimboError::InternalError(e.to_string()))
}

fn binary_value_not_supported(pg_type: &Type) -> turso_core::LimboError {
    let message = format!(
        "cannot send this value in binary format as type {}; use text format",
        pg_type.name()
    );
    sqlstate_error(SQLSTATE_FEATURE_NOT_SUPPORTED, message, None)
}

/// FIXME(pg-catalog): This is a second hand-written map from type names to pg_type entries, besides `pg_type_oid` in postgres/frontend/catalog.rs, and the two disagree: this one sends numeric without a precision as float8 and timetz as time, where pg_type has numeric (1700) and timetz (1266). A lookup in a pg_type table replaces this.
fn sqlite_type_to_pg_type(type_str: &str) -> Type {
    let upper = type_str.to_uppercase();
    match upper.as_str() {
        "SMALLINT" | "INT2" => Type::INT2,
        "INTEGER" | "INT" | "INT4" | "SERIAL" | "SMALLSERIAL" => Type::INT4,
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
        (b'H', &error.hint),
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
        let handler = Arc::new(TursoPgHandler::new(conn.clone()));
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
    fn binary_numbers_follow_the_column_type() {
        let (_io, db) = turso_pg::open_database(
            ":memory:",
            None,
            turso_pg::OpenFlags::default(),
            turso_pg::DatabaseOpts::new(),
        )
        .unwrap();
        let conn = PgConnection::connect(&db).unwrap();
        conn.execute("CREATE TABLE t (i int, b bigint, s smallint)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (42, 42, 42)").unwrap();
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
                "SELECT 42::int2",
                Type::INT2,
                b"\0\0\0\x02\0\x2a",
                b"\0\0\0\x0242",
            ),
            (
                "SELECT s FROM t",
                Type::INT2,
                b"\0\0\0\x02\0\x2a",
                b"\0\0\0\x0242",
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
                "SELECT 1.5::float8",
                Type::FLOAT8,
                b"\0\0\0\x08\x3f\xf8\0\0\0\0\0\0",
                b"\0\0\0\x031.5",
            ),
            ("SELECT true", Type::BOOL, b"\0\0\0\x01\x01", b"\0\0\0\x01t"),
            // The column's type, not the stored value, picks the encoding.
            (
                "SELECT count(*) FROM t",
                Type::TEXT,
                b"\0\0\0\x011",
                b"\0\0\0\x011",
            ),
            // json_each() returns the text of a member, which is not a number.
            (
                "SELECT value FROM json_each('{\"a\":\"s\"}')",
                Type::TEXT,
                b"\0\0\0\x03\"s\"",
                b"\0\0\0\x03\"s\"",
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
                    turso_pg::TextType::of_column(&stmt, 0),
                )
                .unwrap();
                assert_eq!(&encoder.finish().unwrap().data[..], expected, "{sql}");
            }
        }
        // A value that the column's binary type cannot hold is refused.
        for sql in [
            "SELECT abs(i) * 100000000 FROM t",
            "SELECT 1 UNION ALL SELECT 12.5",
        ] {
            let mut stmt = conn.prepare(sql).unwrap();
            let fields = Arc::new(build_field_info(&stmt, &Format::UnifiedBinary).unwrap());
            let Err(error) = execute_query(&mut stmt, fields) else {
                panic!("{sql}: no error");
            };
            assert_eq!(sqlstate(error), SQLSTATE_FEATURE_NOT_SUPPORTED, "{sql}");
        }
    }

    #[test]
    fn unsupported_binary_formats_are_refused() {
        let conn = memory_connection();
        let timestamptz = conn
            .prepare("SELECT CAST('2024-01-05 10:00+00' AS timestamptz)")
            .unwrap();
        let error = build_field_info(&timestamptz, &Format::UnifiedBinary).unwrap_err();
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
        // bound, or None for a refused parameter.
        for (declared, bytes, expected) in [
            (
                vec![Some(Type::TEXT)],
                &b"42"[..],
                Some(Value::from_text("42")),
            ),
            // Type OID 0: PostgreSQL reads `SELECT $1::int4` with these bytes
            // as 42.
            (vec![None], b"\0\0\0\x2a", None),
            (
                vec![Some(Type::BYTEA)],
                b"\\x41",
                Some(Value::from_blob(b"\\x41".to_vec())),
            ),
            (vec![Some(Type::INT8)], b"\0\0\0\0\0\0\0\x2a", None),
            (vec![Some(Type::TIMESTAMPTZ)], b"\0\0\0\0\0\0\0\0", None),
            (vec![], b"42", None),
        ] {
            let mut portal = Portal::<String>::default();
            portal.statement =
                Arc::new(StoredStatement::new(String::new(), String::new(), declared));
            portal.parameter_format = Format::UnifiedBinary;
            portal.parameters = vec![Some(bytes.to_vec().into())];
            let mut stmt = conn.prepare("SELECT $1").unwrap();
            match (bind_portal_parameters(&mut stmt, &portal), expected) {
                (Ok(()), Some(value)) => assert_eq!(stmt.run_collect_rows().unwrap(), [[value]]),
                (Err(error), None) => assert_eq!(sqlstate(error), SQLSTATE_FEATURE_NOT_SUPPORTED),
                (result, expected) => panic!("{bytes:?}: {result:?}, expected {expected:?}"),
            }
        }
    }

    /// A parameter sent without a type has type unknown, which PostgreSQL
    /// reads as text input, whatever the text looks like.
    #[test]
    fn untyped_parameters_are_text_input_for_time_and_json_types() {
        let conn = memory_connection();
        conn.execute("CREATE TABLE w (j json, ts timestamp)")
            .unwrap();
        let bind = |sql: &str, text: &str| {
            let mut portal = Portal::<String>::default();
            portal.statement = Arc::new(StoredStatement::new(
                String::new(),
                String::new(),
                vec![None],
            ));
            portal.parameters = vec![Some(text.as_bytes().to_vec().into())];
            let mut stmt = conn.prepare(sql).unwrap();
            bind_portal_parameters(&mut stmt, &portal).unwrap();
            stmt.run_collect_rows().unwrap()
        };
        bind("INSERT INTO w (j) VALUES ($1)", "123");
        bind("INSERT INTO w (ts) VALUES ($1)", "20200101");
        let rows = bind("SELECT $1::timestamp::text", "20200103");
        assert_eq!(rows, [[Value::from_text("2020-01-03 00:00:00")]]);
        let rows = bind(
            "SELECT (SELECT j::text FROM w WHERE j IS NOT NULL), (SELECT ts::text FROM w WHERE ts IS NOT NULL), $1::text",
            "x",
        );
        assert_eq!(
            rows,
            [[
                Value::from_text("123"),
                Value::from_text("2020-01-01 00:00:00"),
                Value::from_text("x")
            ]]
        );
    }

    /// Known gap: PostgreSQL stores the text 'true' of an untyped parameter in
    /// a jsonb column as the JSON value true. tursopg binds it as a boolean,
    /// which the column stores as the number 1.
    #[test]
    fn untyped_parameter_true_in_jsonb_is_a_known_gap() {
        let conn = memory_connection();
        conn.execute("CREATE TABLE w (j jsonb)").unwrap();
        let mut portal = Portal::<String>::default();
        portal.statement = Arc::new(StoredStatement::new(
            String::new(),
            String::new(),
            vec![None],
        ));
        portal.parameters = vec![Some(b"true".to_vec().into())];
        let mut stmt = conn.prepare("INSERT INTO w VALUES ($1)").unwrap();
        bind_portal_parameters(&mut stmt, &portal).unwrap();
        stmt.run_collect_rows().unwrap();
        let mut stmt = conn.prepare("SELECT j::text FROM w").unwrap();
        let rows = stmt.run_collect_rows().unwrap();
        assert_eq!(rows, [[Value::from_text("1")]]);
    }

    /// Known gap: PostgreSQL reads the text '20240101' of an untyped
    /// parameter compared with a date column as the date 2024-01-01.
    /// tursopg binds the number-like text as a number, and the comparison
    /// refuses it with 22007 "only text can be converted to date".
    #[test]
    fn untyped_number_like_parameter_compared_with_date_is_a_known_gap() {
        let conn = memory_connection();
        conn.execute("CREATE TABLE w (d date)").unwrap();
        conn.execute("INSERT INTO w VALUES ('2024-01-01')").unwrap();
        let mut portal = Portal::<String>::default();
        portal.statement = Arc::new(StoredStatement::new(
            String::new(),
            String::new(),
            vec![None],
        ));
        portal.parameters = vec![Some(b"20240101".to_vec().into())];
        let mut stmt = conn.prepare("SELECT count(*) FROM w WHERE d = $1").unwrap();
        bind_portal_parameters(&mut stmt, &portal).unwrap();
        // PostgreSQL 18 returns one row with count 1.
        let error = stmt.run_collect_rows().unwrap_err();
        assert_eq!(error.sqlstate, "22007");
    }

    #[test]
    fn a_refused_binary_format_aborts_the_transaction_block() {
        let (mut client, _conn) = copy_test_client();
        client.simple_query("CREATE TABLE u (u uuid)").unwrap();
        client.simple_query("BEGIN").unwrap();
        let events = execute_with_binary_result(&mut client, "SELECT u FROM u");
        assert_refused(&events, b'E');
        let events = client.simple_query("COMMIT").unwrap();
        assert!(matches!(&events[0], BackendEvent::CommandComplete(tag) if tag == "ROLLBACK"));
    }

    #[test]
    fn an_empty_extended_query_keeps_the_transaction_block() {
        let (mut client, _conn) = copy_test_client();
        client.simple_query("CREATE TABLE q (i int)").unwrap();
        client.simple_query("BEGIN").unwrap();
        client.simple_query("INSERT INTO q VALUES (1)").unwrap();
        for sql in ["", ";", "-- c", "/* c */"] {
            // Parse, Describe statement, Bind, Describe portal, Execute, Sync.
            let mut parse = vec![0];
            parse.extend_from_slice(sql.as_bytes());
            parse.extend_from_slice(b"\0\0\0");
            client.write_message(b'P', &parse).unwrap();
            client.write_message(b'D', b"S\0").unwrap();
            client.write_message(b'B', b"\0\0\0\0\0\0\0\0").unwrap();
            client.write_message(b'D', b"P\0").unwrap();
            client.write_message(b'E', b"\0\0\0\0\0").unwrap();
            client.write_message(b'S', b"").unwrap();
            let mut events = Vec::new();
            while !matches!(events.last(), Some(BackendEvent::ReadyForQuery(_))) {
                events.push(client.read_event().unwrap());
            }
            // ParseComplete, ParameterDescription, NoData, BindComplete, NoData.
            let [BackendEvent::Other(b'1'), BackendEvent::Other(b't'), BackendEvent::Other(b'n'), BackendEvent::Other(b'2'), BackendEvent::Other(b'n'), BackendEvent::EmptyQueryResponse, BackendEvent::ReadyForQuery(b'T')] =
                &events[..]
            else {
                panic!("{sql:?}: {events:?}");
            };
        }
        let events = client.simple_query("COMMIT").unwrap();
        assert!(matches!(&events[0], BackendEvent::CommandComplete(tag) if tag == "COMMIT"));
        assert_row_count(&mut client, "q", "1");
    }

    const THREE_TIMES: &str = "SELECT now(), statement_timestamp(), clock_timestamp()";

    fn data_rows(events: &[BackendEvent]) -> Vec<Vec<Option<String>>> {
        events
            .iter()
            .filter_map(|event| match event {
                BackendEvent::DataRow(row) => Some(row.clone()),
                _ => None,
            })
            .collect()
    }

    /// Sends Parse, Bind and Execute of `sql`, without Sync.
    fn send_extended(client: &mut PgConn, sql: &str) {
        let mut parse = vec![0];
        parse.extend_from_slice(sql.as_bytes());
        parse.extend_from_slice(b"\0\0\0");
        client.write_message(b'P', &parse).unwrap();
        client.write_message(b'B', b"\0\0\0\0\0\0\0\0").unwrap();
        client.write_message(b'E', b"\0\0\0\0\0").unwrap();
    }

    fn sync_and_read(client: &mut PgConn) -> Vec<BackendEvent> {
        client.write_message(b'S', b"").unwrap();
        let mut events = Vec::new();
        while !matches!(events.last(), Some(BackendEvent::ReadyForQuery(_))) {
            events.push(client.read_event().unwrap());
        }
        events
    }

    #[test]
    fn the_statements_of_one_query_message_share_now_and_statement_timestamp() {
        let (mut client, _conn) = copy_test_client();
        let events = client
            .simple_query(&format!(
                "{THREE_TIMES}; SELECT count(*) FROM generate_series(1, 100000); {THREE_TIMES}"
            ))
            .unwrap();
        let rows = data_rows(&events);
        let [first, _, second] = &rows[..] else {
            panic!("{events:?}");
        };
        assert_eq!(first[0], second[0], "now()");
        assert_eq!(first[1], second[1], "statement_timestamp()");
        // Windows reads the clock in steps of about 15 ms, so two
        // statements of one message can read the same time.
        assert!(first[2] <= second[2], "clock_timestamp()");
        // A later message is a later statement and transaction.
        std::thread::sleep(std::time::Duration::from_millis(20));
        let later = data_rows(&client.simple_query(THREE_TIMES).unwrap());
        assert_ne!(first[0], later[0][0]);
        assert_ne!(first[1], later[0][1]);
    }

    #[test]
    fn an_extended_batch_shares_now_until_sync() {
        let (mut client, _conn) = copy_test_client();
        send_extended(&mut client, THREE_TIMES);
        std::thread::sleep(std::time::Duration::from_millis(20));
        send_extended(&mut client, THREE_TIMES);
        let rows = data_rows(&sync_and_read(&mut client));
        let [first, second] = &rows[..] else {
            panic!("{rows:?}");
        };
        assert_eq!(first[0], second[0], "now() lasts until Sync");
        assert_ne!(first[1], second[1], "each Execute has its own time");
        std::thread::sleep(std::time::Duration::from_millis(20));
        send_extended(&mut client, THREE_TIMES);
        let after_sync = data_rows(&sync_and_read(&mut client));
        assert_ne!(first[0], after_sync[0][0], "Sync ends the transaction");
    }

    #[test]
    fn a_transaction_after_commit_in_an_extended_batch_has_its_own_now() {
        let (mut client, _conn) = copy_test_client();
        send_extended(&mut client, "BEGIN");
        send_extended(&mut client, THREE_TIMES);
        send_extended(&mut client, "COMMIT");
        std::thread::sleep(std::time::Duration::from_millis(20));
        send_extended(&mut client, THREE_TIMES);
        let rows = data_rows(&sync_and_read(&mut client));
        let [first, second] = &rows[..] else {
            panic!("{rows:?}");
        };
        assert_ne!(first[0], second[0], "COMMIT ends the transaction");
    }

    // The result of abs() has no registered type, so the product is not
    // checked against the range of int4 but is described as int4.
    // 4000000000 doesn't fit it, so the second row is refused after the first
    // row is inserted.
    const INSERT_RETURNING_MISFIT: &str =
        "INSERT INTO r VALUES (1), (2) RETURNING abs(i) * 2000000000";

    #[test]
    fn a_refused_returning_row_rolls_back_the_insert() {
        let (mut client, _conn) = copy_test_client();
        client.simple_query("CREATE TABLE r (i int)").unwrap();
        let events = execute_with_binary_result(&mut client, INSERT_RETURNING_MISFIT);
        assert_refused(&events, b'I');
        assert_row_count(&mut client, "r", "0");
    }

    #[test]
    fn a_refused_returning_row_in_a_block_is_not_committed() {
        let (mut client, _conn) = copy_test_client();
        client.simple_query("CREATE TABLE r (i int)").unwrap();
        client.simple_query("BEGIN").unwrap();
        let events = execute_with_binary_result(&mut client, INSERT_RETURNING_MISFIT);
        assert_refused(&events, b'E');
        let events = client.simple_query("COMMIT").unwrap();
        assert!(matches!(&events[0], BackendEvent::CommandComplete(tag) if tag == "ROLLBACK"));
        assert_row_count(&mut client, "r", "0");
    }

    /// Sends Parse, Bind asking for the one result column in binary format,
    /// Execute and Sync, and returns the events up to ReadyForQuery.
    fn execute_with_binary_result(client: &mut PgConn, sql: &str) -> Vec<BackendEvent> {
        let mut parse = vec![0];
        parse.extend_from_slice(sql.as_bytes());
        parse.extend_from_slice(b"\0\0\0");
        client.write_message(b'P', &parse).unwrap();
        client
            .write_message(b'B', b"\0\0\0\0\0\0\0\x01\0\x01")
            .unwrap();
        client.write_message(b'E', b"\0\0\0\0\0").unwrap();
        client.write_message(b'S', b"").unwrap();
        let mut events = Vec::new();
        while !matches!(events.last(), Some(BackendEvent::ReadyForQuery(_))) {
            events.push(client.read_event().unwrap());
        }
        events
    }

    #[test]
    fn a_text_parameter_with_a_fraction_cast_to_an_integer_fails() {
        let (mut client, _conn) = copy_test_client();
        let events = execute_with_text_parameter(&mut client, "SELECT $1::int", "2.5");
        assert_eq!(
            error_sqlstate(&events),
            Some(SQLSTATE_INVALID_TEXT_REPRESENTATION),
            "{events:?}"
        );
    }

    #[test]
    fn a_text_parameter_with_a_fraction_written_to_an_integer_column_fails() {
        let (mut client, _conn) = copy_test_client();
        client
            .simple_query("CREATE TABLE fraction (n int)")
            .unwrap();
        let events =
            execute_with_text_parameter(&mut client, "INSERT INTO fraction VALUES ($1)", "2.5");
        assert_eq!(
            error_sqlstate(&events),
            Some(SQLSTATE_INVALID_TEXT_REPRESENTATION),
            "{events:?}"
        );
        assert_row_count(&mut client, "fraction", "0");
    }

    /// Sends Parse, Bind with the one parameter `parameter` in text format,
    /// Execute and Sync, and returns the events up to ReadyForQuery.
    fn execute_with_text_parameter(
        client: &mut PgConn,
        sql: &str,
        parameter: &str,
    ) -> Vec<BackendEvent> {
        let mut parse = vec![0];
        parse.extend_from_slice(sql.as_bytes());
        parse.extend_from_slice(b"\0\0\0");
        client.write_message(b'P', &parse).unwrap();
        let mut bind = b"\0\0\0\0\0\x01".to_vec();
        bind.extend_from_slice(&(parameter.len() as u32).to_be_bytes());
        bind.extend_from_slice(parameter.as_bytes());
        bind.extend_from_slice(b"\0\0");
        client.write_message(b'B', &bind).unwrap();
        client.write_message(b'E', b"\0\0\0\0\0").unwrap();
        sync_and_read(client)
    }

    fn error_sqlstate(events: &[BackendEvent]) -> Option<&str> {
        events.iter().find_map(|event| match event {
            BackendEvent::ErrorResponse(fields) => fields.get(&b'C').map(String::as_str),
            _ => None,
        })
    }

    /// Checks that Execute got 0A000 and no rows, followed by ReadyForQuery
    /// with the transaction status `status`.
    fn assert_refused(events: &[BackendEvent], status: u8) {
        let [_, _, BackendEvent::ErrorResponse(fields), BackendEvent::ReadyForQuery(got)] = events
        else {
            panic!("{events:?}");
        };
        assert_eq!(fields[&b'C'], SQLSTATE_FEATURE_NOT_SUPPORTED);
        assert_eq!(*got, status, "{events:?}");
    }

    fn assert_row_count(client: &mut PgConn, table: &str, count: &str) {
        let events = client
            .simple_query(&format!("SELECT count(*) FROM {table}"))
            .unwrap();
        let Some(BackendEvent::DataRow(row)) = events.get(1) else {
            panic!("{events:?}");
        };
        assert_eq!(row[0].as_deref(), Some(count));
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
    fn test_pg_is_in_recovery_returns_true_bool_on_wire() {
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
                assert_eq!(*fields[0].datatype(), Type::BOOL, "{sql}");
                let rows = stmt.run_collect_rows().unwrap();
                assert_eq!(rows.len(), 1);
                let mut encoder = DataRowEncoder::new(fields.clone());
                encode_value(
                    &mut encoder,
                    &rows[0][0],
                    fields[0].datatype(),
                    fields[0].format(),
                    turso_pg::TextType::of_column(&stmt, 0),
                )
                .unwrap();
                let row = encoder.finish().unwrap();
                assert_eq!(row.field_count, 1);
                let expected: &[u8] = if matches!(format, Format::UnifiedText) {
                    b"\0\0\0\x01t"
                } else {
                    b"\0\0\0\x01\x01"
                };
                assert_eq!(&row.data[..], expected);
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
            turso_pg::TextType::of_column(&stmt, 0),
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
    fn test_pg_bytes_to_value_integer_reads_text_as_postgres_does() {
        for (text, expected) in [(" 42 ", 42), ("+7", 7), ("0x1F", 31), ("1_000", 1000)] {
            for pg_type in [Type::INT2, Type::INT4, Type::INT8] {
                let val = pg_bytes_to_value(text.as_bytes(), &pg_type).unwrap();
                assert_eq!(val, Value::from_i64(expected), "{text:?} as {pg_type}");
            }
        }
    }

    #[test]
    fn test_pg_bytes_to_value_integer_outside_the_range_of_its_type() {
        for (text, pg_type) in [
            ("32768", Type::INT2),
            ("-2147483649", Type::INT4),
            ("9223372036854775808", Type::INT8),
        ] {
            let Err(error) = pg_bytes_to_value(text.as_bytes(), &pg_type) else {
                panic!("{text:?} as {pg_type}: no error");
            };
            assert_eq!(sqlstate(error), "22003", "{text:?} as {pg_type}");
        }
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

    /// A boolean parameter is read as boolean text in SQL is: in any case,
    /// from the first letters of a word, and with whitespace around it.
    #[test]
    fn test_pg_bytes_to_value_bool_reads_text_as_postgres_does() {
        for text in ["y", "True", " t", "tr", "YES", "on", "1"] {
            let val = pg_bytes_to_value(text.as_bytes(), &Type::BOOL).unwrap();
            assert_eq!(val, Value::from_i64(1), "{text:?}");
        }
        for text in ["n", "False", "f ", "fals", "NO", "of", "off", "0"] {
            let val = pg_bytes_to_value(text.as_bytes(), &Type::BOOL).unwrap();
            assert_eq!(val, Value::from_i64(0), "{text:?}");
        }
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
        for text in ["not_a_number", "1.5", "1e2", ""] {
            let Err(error) = pg_bytes_to_value(text.as_bytes(), &Type::INT4) else {
                panic!("{text:?}: no error");
            };
            assert_eq!(
                sqlstate(error),
                SQLSTATE_INVALID_TEXT_REPRESENTATION,
                "{text:?}"
            );
        }
    }

    #[test]
    fn test_pg_bytes_to_value_float_parse_error() {
        let result = pg_bytes_to_value(b"not_a_float", &Type::FLOAT8);
        assert!(result.is_err());
    }

    #[test]
    fn test_pg_bytes_to_value_bool_invalid() {
        for text in ["maybe", "o", "truex", ""] {
            let Err(error) = pg_bytes_to_value(text.as_bytes(), &Type::BOOL) else {
                panic!("{text:?}: no error");
            };
            assert_eq!(
                sqlstate(error),
                SQLSTATE_INVALID_TEXT_REPRESENTATION,
                "{text:?}"
            );
        }
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
        assert_eq!(sqlite_type_to_pg_type("SMALLINT"), Type::INT2);
        assert_eq!(sqlite_type_to_pg_type("INT2"), Type::INT2);
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

    /// Known gap: PostgreSQL 18 describes `1.5::numeric` as numeric (oid
    /// 1700). tursopg describes it as float8, so a client decodes the value
    /// with the wrong type.
    #[test]
    fn numeric_result_column_oid_is_a_known_gap() {
        let (mut client, _conn) = copy_test_client();
        let events = client.simple_query("SELECT 5::int2, 1.5::numeric").unwrap();
        let oids: Vec<u32> = events
            .iter()
            .find_map(|event| match event {
                BackendEvent::RowDescription(columns) => {
                    Some(columns.iter().map(|c| c.type_oid).collect())
                }
                _ => None,
            })
            .unwrap_or_else(|| panic!("{events:?}"));
        // PostgreSQL 18 sends [21, 1700].
        assert_eq!(oids, [Type::INT2.oid(), Type::FLOAT8.oid()]);
    }
}
