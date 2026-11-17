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
use turso_core::{ConnectOpts, Database, LimboError, Value};
use turso_pg::{
    split_statements, sql_parse_error, sqlstate_error, PgConnection, StatementResultKind,
    TransactionState,
};
use turso_pg_parser::cmdtag::CommandTag;
use turso_pg_parser::copy::{parse_copy_to_stdout, CopyToStdout};
use turso_pg_parser::sqlstate::{
    SQLSTATE_CHARACTER_NOT_IN_REPERTOIRE, SQLSTATE_FEATURE_NOT_SUPPORTED,
    SQLSTATE_INVALID_PARAMETER_VALUE, SQLSTATE_NAME_TOO_LONG, SQLSTATE_PROTOCOL_VIOLATION,
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
use pgwire::api::stmt::{NoopQueryParser, QueryParser, StoredStatement};
use pgwire::api::store::PortalStore;
use pgwire::api::{
    ClientInfo, ClientPortalStore, NoopHandler, PgWireConnectionState, PgWireServerHandlers, Type,
    DEFAULT_NAME,
};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::copy::{CopyData, CopyDone, CopyOutResponse};
use pgwire::messages::data::{DataRow, FORMAT_CODE_BINARY, FORMAT_CODE_TEXT};
use pgwire::messages::extendedquery::{
    Bind, BindComplete, Describe, Execute, Parse, ParseComplete, Sync as PgSync,
};
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
        finish_query(client, &self.conn, result, false).await
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
                let header = Arc::new(text_fields(&stmt));
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
        refuse_replaced_bytes("query text", &query.query)?;
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
        }
        client.set_state(PgWireConnectionState::ReadyForQuery);
        send_ready_for_query(client, wire_transaction_status(conn)).await
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

    async fn on_parse<C>(&self, client: &mut C, message: Parse) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let result = parse(client, &message, self.query_parser()).await;
        finish_query(client, &self.conn, result, true).await
    }

    async fn on_bind<C>(&self, client: &mut C, message: Bind) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let result = bind(client, &message).await;
        finish_query(client, &self.conn, result, true).await
    }

    async fn on_execute<C>(&self, client: &mut C, message: Execute) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let result = self._on_execute(client, message).await;
        finish_query(client, &self.conn, result, true).await
    }

    async fn on_describe<C>(&self, client: &mut C, message: Describe) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let result = self._on_describe(client, message).await;
        finish_query(client, &self.conn, result, true).await
    }

    /// Reports the session's transaction state. pgwire's own copy of the
    /// status can still say a block is open after an error aborted it.
    async fn on_sync<C>(&self, client: &mut C, _message: PgSync) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        send_ready_for_query(client, wire_transaction_status(&self.conn)).await
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
        let parsed = parse_statement(query)?;
        if turso_pg_parser::is_empty_query(&parsed) {
            return Ok(Response::EmptyQuery);
        }

        let mut stmt = conn.prepare(query).map_err(pg_error)?;

        // Bind parameters from the portal
        bind_portal_parameters(&mut stmt, portal)?;

        if stmt.result_kind() != StatementResultKind::Rows {
            return execute_non_query(&mut stmt, &parsed);
        }

        let header = apply_result_formats(text_fields(&stmt), &portal.result_column_format)
            .map_err(|e| pg_error(e.into()))?;
        execute_query(&mut stmt, Arc::new(header))
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
        let param_types: Vec<Type> = target
            .parameter_types
            .iter()
            .map(|t| t.clone().unwrap_or(Type::TEXT))
            .collect();
        if turso_pg_parser::is_empty_query(&parse_statement(&target.statement)?) {
            return Ok(DescribeStatementResponse::new(param_types, vec![]));
        }
        let stmt = conn.prepare(&target.statement).map_err(pg_error)?;

        // A statement has no result formats until it is bound to a portal.
        Ok(DescribeStatementResponse::new(
            param_types,
            describe_fields(&stmt),
        ))
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
        if turso_pg_parser::is_empty_query(&parse_statement(&portal.statement.statement)?) {
            return Ok(DescribePortalResponse::new(vec![]));
        }
        let stmt = conn
            .prepare(&portal.statement.statement)
            .map_err(pg_error)?;

        if stmt.result_kind() != StatementResultKind::Rows {
            return Ok(DescribePortalResponse::new(vec![]));
        }
        let fields = apply_result_formats(text_fields(&stmt), &portal.result_column_format)
            .map_err(|e| pg_error(e.into()))?;
        Ok(DescribePortalResponse::new(fields))
    }
}

/// Stores the statement of a Parse message, as pgwire's own Parse handler
/// does, but returns its errors so that [`finish_query`] reports them.
async fn parse<C>(client: &mut C, message: &Parse, parser: Arc<NoopQueryParser>) -> PgWireResult<()>
where
    C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    C::PortalStore: PortalStore<Statement = String>,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    // pgwire reads an empty statement name as None.
    let name = message.name.as_deref().unwrap_or(DEFAULT_NAME);
    refuse_replaced_bytes("a statement name", name)?;
    refuse_replaced_bytes("query text", &message.query)?;
    let parameter_types: Vec<_> = message
        .type_oids
        .iter()
        .copied()
        .map(Type::from_oid)
        .collect();
    let statement = parser
        .parse_sql(client, &message.query, &parameter_types)
        .await?;
    let statement = StoredStatement::new(name.to_owned(), statement, parameter_types);
    client.portal_store().put_statement(Arc::new(statement));
    client
        .send(PgWireBackendMessage::ParseComplete(ParseComplete::new()))
        .await?;
    Ok(())
}

/// Creates the portal of a Bind message, as pgwire's own Bind handler does,
/// but returns its errors so that [`finish_query`] reports them.
async fn bind<C>(client: &mut C, message: &Bind) -> PgWireResult<()>
where
    C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    C::PortalStore: PortalStore<Statement = String>,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    // Names are refused where statements and portals are created, so a name
    // with U+FFFD in Bind, Execute, Describe or Close finds nothing.
    if let Some(portal) = &message.portal_name {
        refuse_replaced_bytes("a portal name", portal)?;
    }
    // pgwire reads an empty statement name as None.
    let name = message.statement_name.as_deref().unwrap_or(DEFAULT_NAME);
    let Some(statement) = client.portal_store().get_statement(name) else {
        return Err(PgWireError::StatementNotFound(name.to_owned()));
    };
    check_bind_formats(message, &statement.parameter_types).map_err(|e| pg_error(e.into()))?;
    let portal = Portal::try_new(message, statement)?;
    client.portal_store().put_portal(Arc::new(portal));
    client
        .send(PgWireBackendMessage::BindComplete(BindComplete::new()))
        .await?;
    Ok(())
}

/// pgwire reads the strings of protocol messages with `String::from_utf8_lossy`
/// (`get_cstring` in pgwire's messages/codec.rs), which turns bytes that are
/// not valid UTF-8 into U+FFFD. A U+FFFD in such a string is the only sign of
/// that, so tursopg refuses every U+FFFD rather than run or store text other
/// than what the client sent. PostgreSQL refuses only the invalid bytes.
fn refuse_replaced_bytes(what: &str, text: &str) -> PgWireResult<()> {
    if !text.contains(char::REPLACEMENT_CHARACTER) {
        return Ok(());
    }
    Err(pg_error(
        sqlstate_error(
            SQLSTATE_FEATURE_NOT_SUPPORTED,
            format!("{what} with invalid UTF-8 or U+FFFD is not supported"),
        )
        .into(),
    ))
}

/// Refuses the format codes of a Bind message that tursopg cannot follow:
/// codes other than text and binary, a parameter format count that matches
/// neither all parameters nor each one, and a parameter value in binary
/// format of a type tursopg cannot read in binary. A NULL has no bytes to
/// read. pgwire reads every code other than binary as text, so this must look
/// at the message itself.
fn check_bind_formats(bind: &Bind, parameter_types: &[Option<Type>]) -> Result<(), LimboError> {
    let param_formats = &bind.parameter_format_codes;
    let params = bind.parameters.len();
    if param_formats.len() > 1 && param_formats.len() != params {
        return Err(sqlstate_error(
            SQLSTATE_PROTOCOL_VIOLATION,
            format!(
                "bind message has {} parameter formats but {params} parameters",
                param_formats.len()
            ),
        ));
    }
    for (i, value) in bind.parameters.iter().enumerate() {
        let code = match param_formats.as_slice() {
            [] => FORMAT_CODE_TEXT,
            [code] => *code,
            codes => codes[i],
        };
        check_format_code(code)?;
        if code == FORMAT_CODE_BINARY && value.is_some() {
            let pg_type = parameter_type(parameter_types, i);
            if BinaryType::of(pg_type).is_none_or(|binary_type| !binary_type.can_read()) {
                return Err(sqlstate_error(
                    SQLSTATE_FEATURE_NOT_SUPPORTED,
                    format!(
                        "binary format is not supported for parameter ${} of type {}; send it in text format",
                        i + 1,
                        pg_type.name()
                    ),
                ));
            }
        }
    }
    // PostgreSQL checks a result format code only when it sends a row in
    // that format; checking every code here refuses more, never less.
    for &code in &bind.result_column_format_codes {
        check_format_code(code)?;
    }
    Ok(())
}

fn check_format_code(code: i16) -> Result<(), LimboError> {
    if code != FORMAT_CODE_TEXT && code != FORMAT_CODE_BINARY {
        return Err(sqlstate_error(
            SQLSTATE_INVALID_PARAMETER_VALUE,
            format!("unsupported format code: {code}"),
        ));
    }
    Ok(())
}

/// The type of parameter `i`, as Parse gave it. A parameter Parse gave no
/// type for is of type unknown, as in PostgreSQL before it infers one.
fn parameter_type(parameter_types: &[Option<Type>], i: usize) -> &Type {
    match parameter_types.get(i) {
        Some(Some(pg_type)) => pg_type,
        Some(None) | None => &Type::UNKNOWN,
    }
}

/// The text-format fields of an ordinary result set, or none for commands
/// and COPY.
fn describe_fields(stmt: &turso_pg::Statement) -> Vec<FieldInfo> {
    if stmt.result_kind() != StatementResultKind::Rows {
        return vec![];
    }
    text_fields(stmt)
}

fn parse_statement(sql: &str) -> PgWireResult<ParseResult> {
    turso_pg_parser::parse(sql).map_err(|error| pg_error(sql_parse_error(error).into()))
}

/// RowDescription fields in text format, from a prepared statement's column
/// information.
fn text_fields(stmt: &turso_pg::Statement) -> Vec<FieldInfo> {
    (0..stmt.num_columns())
        .map(|i| {
            let name = stmt.get_column_name(i).into_owned();
            let pg_type = resolve_pg_type_for_column(stmt, i);
            FieldInfo::new(name, None, None, pg_type, FieldFormat::Text)
        })
        .collect()
}

/// Gives each field the result format a Bind asked for. Refuses a format
/// count that matches neither all columns nor each one, as PostgreSQL does,
/// and binary format for a type tursopg cannot send in binary.
fn apply_result_formats(
    fields: Vec<FieldInfo>,
    format: &Format,
) -> Result<Vec<FieldInfo>, LimboError> {
    if let Format::Individual(codes) = format {
        if codes.len() != fields.len() {
            return Err(sqlstate_error(
                SQLSTATE_PROTOCOL_VIOLATION,
                format!(
                    "bind message has {} result formats but query has {} columns",
                    codes.len(),
                    fields.len()
                ),
            ));
        }
    }
    fields
        .into_iter()
        .enumerate()
        .map(|(i, field)| {
            let format = format.format_for(i);
            let pg_type = field.datatype();
            if format == FieldFormat::Binary && BinaryType::of(pg_type).is_none() {
                return Err(sqlstate_error(
                    SQLSTATE_FEATURE_NOT_SUPPORTED,
                    format!(
                        "binary format is not supported for result column \"{}\" of type {}",
                        field.name(),
                        pg_type.name()
                    ),
                ));
            }
            Ok(FieldInfo::new(
                field.name().to_owned(),
                None,
                None,
                pg_type.clone(),
                format,
            ))
        })
        .collect()
}

/// The types that tursopg can send in binary format, and of those the ones
/// it can read in binary format.
#[derive(Clone, Copy)]
enum BinaryType {
    Bool,
    Int2,
    Int4,
    Int8,
    Float8,
    /// text, varchar and bpchar, whose binary format is the text's UTF-8
    /// bytes. tursopg refuses bpchar with a length, so it never stores the
    /// padding PostgreSQL adds.
    Text,
    /// Binary format like `Text`, at most 63 bytes.
    Name,
    Bytea,
}

impl BinaryType {
    fn of(pg_type: &Type) -> Option<Self> {
        match *pg_type {
            Type::BOOL => Some(Self::Bool),
            Type::INT2 => Some(Self::Int2),
            Type::INT4 => Some(Self::Int4),
            Type::INT8 => Some(Self::Int8),
            Type::FLOAT8 => Some(Self::Float8),
            Type::TEXT | Type::VARCHAR | Type::BPCHAR => Some(Self::Text),
            Type::NAME => Some(Self::Name),
            Type::BYTEA => Some(Self::Bytea),
            _ => None,
        }
    }

    /// Whether tursopg can read a parameter of this type in binary format.
    fn can_read(self) -> bool {
        match self {
            Self::Text | Self::Name | Self::Bytea => true,
            Self::Bool | Self::Int2 | Self::Int4 | Self::Int8 | Self::Float8 => false,
        }
    }
}

/// Decide the PG wire type for a result column.
///
/// `get_column_type_info` is the single source of truth: it handles direct
/// table-column references (declared name, array depth, custom-type kind,
/// resolved primitive), bare literals (`SELECT 42` -> INTEGER), and typed
/// expressions like CAST. A column it has no type for is described as TEXT,
/// and its values are sent as text in both formats.
fn resolve_pg_type_for_column(stmt: &turso_pg::Statement, idx: usize) -> Type {
    use turso_core::ColumnTypeKind;

    let info = stmt
        .get_column_type_info(idx)
        .expect("the PostgreSQL dialect always enables custom types");
    let Some(info) = info else {
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
            let mapped = declared_type_to_pg_type(&info.declared_name);
            if mapped == Type::TEXT {
                info.base_type
                    .as_deref()
                    .map(declared_type_to_pg_type)
                    .unwrap_or(Type::TEXT)
            } else {
                mapped
            }
        }
    };
    if info.array_dimensions > 0 {
        base = array_type_of(&base);
    }
    base
}

/// The PostgreSQL type of a declared type name, from the catalog's type
/// table, so RowDescription and `pg_attribute` agree.
fn declared_type_to_pg_type(declared: &str) -> Type {
    pg_type_of_oid(turso_pg::sqlite_type_to_pg_oid(declared))
}

/// The array type whose elements have type `element`, from the catalog's
/// `pg_type` rows.
fn array_type_of(element: &Type) -> Type {
    let oid = turso_pg::pg_array_type_oid(i64::from(element.oid()))
        .unwrap_or_else(|| panic!("type {element} has no row in pg_type"));
    pg_type_of_oid(oid)
}

fn pg_type_of_oid(oid: i64) -> Type {
    u32::try_from(oid)
        .ok()
        .and_then(Type::from_oid)
        .unwrap_or_else(|| panic!("type OID {oid} is not a PostgreSQL type"))
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
        assert_eq!(
            row.len(),
            header_clone.len(),
            "a row has one value per result column"
        );
        for (val, field) in row.get_values().zip(header_clone.iter()) {
            encode_value(&mut encoder, val, field)?;
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
/// PostgreSQL parameters ($1, $2, ...) are portal parameters 0, 1, ...
fn bind_portal_parameters(
    stmt: &mut turso_pg::Statement,
    portal: &Portal<String>,
) -> PgWireResult<()> {
    // Like PostgreSQL, a statement takes as many parameters as the larger of
    // its highest `$N` and the number of types Parse gave.
    let required = stmt
        .parameters_count()
        .max(portal.statement.parameter_types.len());
    let supplied = portal.parameter_len();
    if supplied != required {
        let name = match portal.statement.id.as_str() {
            DEFAULT_NAME => "",
            name => name,
        };
        return Err(pg_error(
            turso_pg::sqlstate_error(
                SQLSTATE_PROTOCOL_VIOLATION,
                format!(
                    "bind message supplies {supplied} parameters, but prepared statement \"{name}\" requires {required}"
                ),
            )
            .into(),
        ));
    }
    for (i, bytes) in portal.parameters.iter().enumerate() {
        let value = match bytes {
            None => Value::Null,
            Some(bytes) => {
                let pg_type = parameter_type(&portal.statement.parameter_types, i);
                match portal.parameter_format.format_for(i) {
                    FieldFormat::Text => pg_bytes_to_value(bytes, pg_type)?,
                    FieldFormat::Binary => {
                        decode_binary_parameter(bytes, pg_type).map_err(|e| pg_error(e.into()))?
                    }
                }
            }
        };
        // tursopg compiles `$N` as `?N`, so parameter i binds at index i + 1.
        let index = NonZero::new(i + 1).expect("i + 1 is never zero");
        stmt.bind_at(index, value).map_err(pg_error)?;
    }
    Ok(())
}

/// Reads a parameter in binary format. `check_bind_formats` has refused the
/// types that tursopg cannot read in binary format.
fn decode_binary_parameter(bytes: &[u8], pg_type: &Type) -> Result<Value, LimboError> {
    let binary_type = BinaryType::of(pg_type)
        .filter(|binary_type| binary_type.can_read())
        .expect("check_bind_formats refuses binary parameters of this type");
    match binary_type {
        BinaryType::Text => Ok(Value::from_text(parameter_text(bytes)?.to_owned())),
        BinaryType::Name => {
            let name = parameter_text(bytes)?;
            if name.len() > turso_pg_parser::translator::MAX_IDENTIFIER_BYTES {
                return Err(sqlstate_error(
                    SQLSTATE_NAME_TOO_LONG,
                    "identifier too long",
                ));
            }
            Ok(Value::from_text(name.to_owned()))
        }
        BinaryType::Bytea => Ok(Value::from_blob(bytes.to_vec())),
        BinaryType::Bool
        | BinaryType::Int2
        | BinaryType::Int4
        | BinaryType::Int8
        | BinaryType::Float8 => unreachable!("tursopg cannot read {pg_type} in binary format"),
    }
}

/// The text of a parameter. Like PostgreSQL, it must be valid UTF-8 without
/// NUL, and the error shows the bytes of the first character that is not.
fn parameter_text(bytes: &[u8]) -> Result<&str, LimboError> {
    let valid_up_to = match std::str::from_utf8(bytes) {
        Ok(_) => bytes.len(),
        Err(error) => error.valid_up_to(),
    };
    let bad_at = match bytes[..valid_up_to].iter().position(|&byte| byte == 0) {
        Some(nul_at) => nul_at,
        None if valid_up_to == bytes.len() => {
            let text = std::str::from_utf8(bytes).expect("all bytes are valid UTF-8");
            return Ok(text);
        }
        None => valid_up_to,
    };
    // PostgreSQL shows as many bytes as the first byte says the character
    // has, or the bytes that are left if there are fewer (pg_utf_mblen).
    let character_len = match bytes[bad_at] {
        0x00..=0x7f => 1,
        byte if byte & 0xe0 == 0xc0 => 2,
        byte if byte & 0xf0 == 0xe0 => 3,
        byte if byte & 0xf8 == 0xf0 => 4,
        _ => 1,
    };
    let shown = &bytes[bad_at..(bad_at + character_len).min(bytes.len())];
    let shown: Vec<String> = shown.iter().map(|byte| format!("0x{byte:02x}")).collect();
    Err(sqlstate_error(
        SQLSTATE_CHARACTER_NOT_IN_REPERTOIRE,
        format!(
            "invalid byte sequence for encoding \"UTF8\": {}",
            shown.join(" ")
        ),
    ))
}

/// Convert raw parameter bytes to a turso Value based on the PostgreSQL type.
/// Assumes text format encoding (UTF-8 string representations).
fn pg_bytes_to_value(bytes: &[u8], pg_type: &Type) -> PgWireResult<Value> {
    let text = parameter_text(bytes).map_err(|e| pg_error(e.into()))?;

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

/// Encodes one value of a row in the format of its result column.
fn encode_value(
    encoder: &mut DataRowEncoder,
    val: &Value,
    field: &FieldInfo,
) -> turso_core::Result<()> {
    let pg_type = field.datatype();
    match field.format() {
        FieldFormat::Text => encode_text(encoder, val, pg_type),
        FieldFormat::Binary => {
            let binary_type = BinaryType::of(pg_type)
                .expect("apply_result_formats refuses binary format for this type");
            encode_binary(encoder, val, binary_type, pg_type)
        }
    }
}

/// Encodes a value in text format. Binary text, varchar and name columns
/// use it too: their binary format is the same bytes.
fn encode_text(
    encoder: &mut DataRowEncoder,
    val: &Value,
    pg_type: &Type,
) -> turso_core::Result<()> {
    let options = FormatOptions::default();
    match val {
        Value::Null => encoder
            .encode_field_with_type_and_format(&None::<i8>, pg_type, FieldFormat::Text, &options)
            .map_err(encode_error),
        Value::Numeric(turso_core::Numeric::Integer(i)) => {
            // Boolean columns: encode as true/false instead of 0/1
            if *pg_type == Type::BOOL {
                encoder
                    .encode_field_with_type_and_format(
                        &(*i != 0),
                        pg_type,
                        FieldFormat::Text,
                        &options,
                    )
                    .map_err(encode_error)
            } else {
                encoder
                    .encode_field_with_type_and_format(i, pg_type, FieldFormat::Text, &options)
                    .map_err(encode_error)
            }
        }
        Value::Numeric(turso_core::Numeric::Float(f)) => encoder
            .encode_field_with_type_and_format(&f64::from(*f), pg_type, FieldFormat::Text, &options)
            .map_err(encode_error),
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
                    .encode_field_with_type_and_format(
                        &with_tz.as_str(),
                        pg_type,
                        FieldFormat::Text,
                        &options,
                    )
                    .map_err(encode_error)
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
                        &options,
                    )
                    .map_err(encode_error)
            } else {
                encoder
                    .encode_field_with_type_and_format(&text, pg_type, FieldFormat::Text, &options)
                    .map_err(encode_error)
            }
        }
        Value::Blob(b) => encoder
            .encode_field_with_type_and_format(&b.as_slice(), pg_type, FieldFormat::Text, &options)
            .map_err(encode_error),
    }
}

/// Encodes a value in binary format. The encoding follows the column's type,
/// not the stored value: `coalesce(NULL::float8, 1)` holds an integer, and
/// `count(*)` is typed text. A value that the type's binary format cannot
/// hold is refused, because the client would misread any bytes sent for it.
fn encode_binary(
    encoder: &mut DataRowEncoder,
    val: &Value,
    binary_type: BinaryType,
    pg_type: &Type,
) -> turso_core::Result<()> {
    use turso_core::Numeric::{Float, Integer};
    let options = FormatOptions::default();
    let binary = FieldFormat::Binary;
    if let Value::Null = val {
        return encoder
            .encode_field_with_type_and_format(&None::<i8>, pg_type, binary, &options)
            .map_err(encode_error);
    }
    let encoded = match (binary_type, val) {
        (BinaryType::Bool, Value::Numeric(Integer(0))) => {
            encoder.encode_field_with_type_and_format(&false, pg_type, binary, &options)
        }
        (BinaryType::Bool, Value::Numeric(Integer(1))) => {
            encoder.encode_field_with_type_and_format(&true, pg_type, binary, &options)
        }
        (BinaryType::Bool, Value::Numeric(Integer(i))) => {
            return Err(sqlstate_error(
                SQLSTATE_FEATURE_NOT_SUPPORTED,
                format!("cannot send {i} in binary format as type bool: only 0 and 1 are booleans"),
            ))
        }
        (BinaryType::Int2, Value::Numeric(Integer(i))) => {
            let i = i16::try_from(*i).map_err(|_| out_of_range(*i, pg_type))?;
            encoder.encode_field_with_type_and_format(&i, pg_type, binary, &options)
        }
        (BinaryType::Int4, Value::Numeric(Integer(i))) => {
            let i = i32::try_from(*i).map_err(|_| out_of_range(*i, pg_type))?;
            encoder.encode_field_with_type_and_format(&i, pg_type, binary, &options)
        }
        (BinaryType::Int8, Value::Numeric(Integer(i))) => {
            encoder.encode_field_with_type_and_format(i, pg_type, binary, &options)
        }
        (BinaryType::Float8, Value::Numeric(Integer(i))) => {
            encoder.encode_field_with_type_and_format(&(*i as f64), pg_type, binary, &options)
        }
        (BinaryType::Float8, Value::Numeric(Float(f))) => {
            encoder.encode_field_with_type_and_format(&f64::from(*f), pg_type, binary, &options)
        }
        // The binary format of text is its UTF-8 bytes, the same bytes as
        // its text format.
        (BinaryType::Text | BinaryType::Name, _) => return encode_text(encoder, val, pg_type),
        (BinaryType::Bytea, Value::Blob(b)) => {
            encoder.encode_field_with_type_and_format(&b.as_slice(), pg_type, binary, &options)
        }
        (
            BinaryType::Bool
            | BinaryType::Int2
            | BinaryType::Int4
            | BinaryType::Int8
            | BinaryType::Float8
            | BinaryType::Bytea,
            _,
        ) => {
            return Err(sqlstate_error(
                SQLSTATE_FEATURE_NOT_SUPPORTED,
                format!(
                    "cannot send a value of storage class {} in binary format as type {}",
                    val.value_type(),
                    pg_type.name()
                ),
            ))
        }
    };
    encoded.map_err(encode_error)
}

fn out_of_range(i: i64, pg_type: &Type) -> LimboError {
    sqlstate_error(
        SQLSTATE_FEATURE_NOT_SUPPORTED,
        format!(
            "cannot send {i} in binary format as type {}: it is out of range",
            pg_type.name()
        ),
    )
}

fn encode_error(error: PgWireError) -> LimboError {
    LimboError::InternalError(error.to_string())
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

/// Report the error of a query, Bind, Execute or Describe message, after
/// applying PostgreSQL's transaction block rules to it unless the session
/// already did. A simple query ends with ReadyForQuery; an extended query
/// message waits for Sync, which `on_sync` answers. A broken connection or
/// a fatal error goes back to pgwire, which closes the connection.
async fn finish_query<C>(
    client: &mut C,
    conn: &PgConnection,
    result: PgWireResult<()>,
    wait_for_sync: bool,
) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    let error = match result {
        Ok(()) => return Ok(()),
        Err(error) => error,
    };
    let response = match error {
        PgWireError::IoError(_) => return Err(error),
        PgWireError::ApiError(error) => match error.downcast::<turso_pg::Error>() {
            Ok(error) => turso_error_response(&error),
            Err(error) => wire_error_response(PgWireError::ApiError(error))?,
        },
        error => wire_error_response(error)?,
    };
    conn.abort_transaction_block_on_error();
    client
        .send(PgWireBackendMessage::ErrorResponse(response))
        .await?;
    if wait_for_sync {
        client.set_state(PgWireConnectionState::AwaitingSync);
    } else {
        client.set_state(PgWireConnectionState::ReadyForQuery);
        client
            .feed(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                wire_transaction_status(conn),
            )))
            .await?;
    }
    client.flush().await?;
    Ok(())
}

fn turso_error_response(error: &turso_pg::Error) -> ErrorResponse {
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
    ErrorResponse::new(fields)
}

/// The ErrorResponse for an error that pgwire or the wire server raised, or
/// the error itself when it is fatal, so that pgwire closes the connection.
fn wire_error_response(error: PgWireError) -> PgWireResult<ErrorResponse> {
    let info = ErrorInfo::from(error);
    if info.is_fatal() {
        return Err(PgWireError::UserError(Box::new(info)));
    }
    Ok(info.into())
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

    #[test]
    fn bind_must_supply_as_many_parameters_as_the_statement_takes() {
        let (mut client, _) = copy_test_client();
        // A statement takes as many parameters as the larger of its highest
        // `$N` and the number of types Parse gave. Messages are PostgreSQL 17's.
        let one: &[u8] = b"1";
        for (statement, types, params, message) in [
            (
                "",
                &[][..],
                &[][..],
                "bind message supplies 0 parameters, but prepared statement \"\" requires 1",
            ),
            (
                "",
                &[],
                &[Some(one), Some(b"2")],
                "bind message supplies 2 parameters, but prepared statement \"\" requires 1",
            ),
            (
                "s1",
                &[23, 23],
                &[Some(one)],
                "bind message supplies 1 parameters, but prepared statement \"s1\" requires 2",
            ),
        ] {
            client
                .send_parse(statement, "SELECT $1::int", types)
                .unwrap();
            client.send_bind("", statement, &[], params, &[]).unwrap();
            client.send_execute("", 0).unwrap();
            client.send_sync().unwrap();
            let messages = read_until_ready(&mut client);
            assert_eq!(
                error_of(&messages),
                (SQLSTATE_PROTOCOL_VIOLATION.to_owned(), message.to_owned()),
                "{messages:?}"
            );
        }

        client.send_parse("", "SELECT $1::int", &[23, 23]).unwrap();
        client
            .send_bind("", "", &[], &[Some(one), Some(b"2")], &[])
            .unwrap();
        client.send_execute("", 0).unwrap();
        client.send_sync().unwrap();
        let messages = read_until_ready(&mut client);
        let rows: Vec<_> = messages.iter().filter(|m| m.0 == b'D').collect();
        assert_eq!(rows.len(), 1, "{messages:?}");
        assert_eq!(
            turso_pg_client::parse_data_row(&rows[0].1).unwrap(),
            vec![Some("1".to_owned())]
        );

        // The i-th Bind value is `$i`, wherever `$i` appears in the SQL.
        client
            .send_parse("", "SELECT $2::text || '-' || $1::text", &[])
            .unwrap();
        client
            .send_bind("", "", &[], &[Some(b"a"), Some(b"b")], &[])
            .unwrap();
        client.send_execute("", 0).unwrap();
        client.send_sync().unwrap();
        let messages = read_until_ready(&mut client);
        let rows: Vec<_> = messages.iter().filter(|m| m.0 == b'D').collect();
        assert_eq!(rows.len(), 1, "{messages:?}");
        assert_eq!(
            turso_pg_client::parse_data_row(&rows[0].1).unwrap(),
            vec![Some("b-a".to_owned())]
        );
    }

    #[test]
    fn errors_raised_outside_the_session_abort_the_transaction_block() {
        let (mut client, conn) = copy_test_client();
        conn.execute("CREATE TABLE t (a int)").unwrap();
        type Run = fn(&mut PgConn) -> Vec<(u8, Vec<u8>)>;
        let cases: [(&str, Run); 3] = [
            ("parameter that is not UTF-8", |client| {
                extended_messages(client, "SELECT $1::text", &[Some(&b"\xff"[..])])
            }),
            ("wrong number of parameters", |client| {
                extended_messages(client, "SELECT $1::int", &[])
            }),
            ("Bind to an unknown statement", |client| {
                client.send_bind("", "unknown", &[], &[], &[]).unwrap();
                client.send_execute("", 0).unwrap();
                client.send_sync().unwrap();
                read_until_ready(client)
            }),
        ];
        for (context, run) in cases {
            query_messages(&mut client, "BEGIN");
            query_messages(&mut client, "INSERT INTO t VALUES (1)");
            let messages = run(&mut client);
            error_of(&messages);
            assert_eq!(messages.last().unwrap().1, b"E", "{context}: {messages:?}");
            assert_block_aborted(&mut client, context);
        }
        let messages = query_messages(&mut client, "SELECT count(*) FROM t");
        assert_eq!(
            turso_pg_client::parse_data_row(&messages[1].1).unwrap(),
            vec![Some("0".to_owned())]
        );
    }

    #[test]
    fn an_empty_extended_query_gets_empty_query_response() {
        // The messages PostgreSQL 17 sends. An empty query is no error, so
        // the transaction block stays open.
        let (mut client, conn) = copy_test_client();
        conn.execute("CREATE TABLE t (a int)").unwrap();
        query_messages(&mut client, "BEGIN");
        query_messages(&mut client, "INSERT INTO t VALUES (1)");
        for sql in ["", " ", ";", "-- c"] {
            client.send_parse("", sql, &[]).unwrap();
            client.send_bind("", "", &[], &[], &[]).unwrap();
            client.send_describe_portal("").unwrap();
            client.send_execute("", 0).unwrap();
            client.send_sync().unwrap();
            let messages = read_until_ready(&mut client);
            let tags: Vec<u8> = messages.iter().map(|m| m.0).collect();
            assert_eq!(tags, b"12nIZ", "{sql:?}: {messages:?}");
            assert_eq!(messages.last().unwrap().1, b"T", "{sql:?}: {messages:?}");
        }
        client.send_parse("", "", &[]).unwrap();
        client.send_describe_statement("").unwrap();
        client.send_sync().unwrap();
        let messages = read_until_ready(&mut client);
        assert_eq!(
            messages,
            vec![
                (b'1', vec![]),
                (b't', vec![0, 0]),
                (b'n', vec![]),
                (b'Z', b"T".to_vec())
            ]
        );
        let messages = query_messages(&mut client, "COMMIT");
        assert_eq!(messages[0], (b'C', b"COMMIT\0".to_vec()));
        let messages = query_messages(&mut client, "SELECT count(*) FROM t");
        assert_eq!(
            turso_pg_client::parse_data_row(&messages[1].1).unwrap(),
            vec![Some("1".to_owned())]
        );
    }

    #[test]
    fn ready_for_query_reports_the_session_transaction_state() {
        let (mut client, _) = copy_test_client();
        for (sql, status) in [("BEGIN", b"T"), ("SELECT 1/0", b"E"), ("COMMIT", b"I")] {
            let messages = extended_messages(&mut client, sql, &[]);
            assert_eq!(messages.last().unwrap().1, status, "{sql}: {messages:?}");
        }
        let messages = query_messages(&mut client, "BEGIN");
        assert_eq!(messages.last().unwrap().1, b"T", "{messages:?}");
    }

    #[test]
    fn binary_parameters_of_other_types_are_refused() {
        let (mut client, _) = copy_test_client();
        let int8 = 42i64.to_be_bytes();
        for (sql, types, formats, params, message) in [
            (
                "SELECT $1::int8",
                &[20][..],
                &[1][..],
                &[Some(&int8[..])][..],
                "binary format is not supported for parameter $1 of type int8; send it in text format",
            ),
            (
                "SELECT $1::int8, $2::int8",
                &[20, 20],
                &[0, 1],
                &[Some(b"1"), Some(&int8)],
                "binary format is not supported for parameter $2 of type int8; send it in text format",
            ),
            // PostgreSQL reads a parameter that Parse gave no type in the
            // binary format of the type it infers; tursopg infers none.
            (
                "SELECT $1 AS v",
                &[],
                &[1],
                &[Some(b"x")],
                "binary format is not supported for parameter $1 of type unknown; send it in text format",
            ),
        ] {
            let messages = formatted_messages(&mut client, sql, types, formats, params, &[]);
            assert_eq!(tags(&messages), "1EZ", "{message}");
            assert_eq!(
                error_of(&messages),
                (SQLSTATE_FEATURE_NOT_SUPPORTED.to_owned(), message.to_owned())
            );
        }
    }

    #[test]
    fn text_parameters_and_binary_nulls_are_accepted() {
        let (mut client, _) = copy_test_client();
        for formats in [&[][..], &[0]] {
            let messages = formatted_messages(
                &mut client,
                "SELECT $1::int8 AS n",
                &[],
                formats,
                &[Some(b"7")],
                &[],
            );
            assert_eq!(tags(&messages), "12TDCZ", "{formats:?}");
            assert_eq!(data_row(&messages), vec![Some("7".to_owned())]);
        }
        let messages = formatted_messages(
            &mut client,
            "SELECT $1::int8 AS a, $2::int8 AS b",
            &[],
            &[0, 0],
            &[Some(b"7"), Some(b"8")],
            &[],
        );
        assert_eq!(
            data_row(&messages),
            vec![Some("7".to_owned()), Some("8".to_owned())]
        );
        // A NULL has no bytes, so its format does not matter.
        let messages = formatted_messages(
            &mut client,
            "SELECT $1::int8 AS n",
            &[20],
            &[1],
            &[None],
            &[],
        );
        assert_eq!(tags(&messages), "12TDCZ");
        assert_eq!(data_row(&messages), vec![None]);
        // One format code with no parameters applies to nothing.
        let messages = formatted_messages(&mut client, "SELECT 1 AS n", &[], &[1], &[], &[]);
        assert_eq!(tags(&messages), "12TDCZ");
    }

    #[test]
    fn binary_text_and_bytea_parameters_are_their_bytes() {
        let (mut client, conn) = copy_test_client();
        // text, varchar, bpchar and name.
        for oid in [25, 1043, 1042, 19] {
            let messages = formatted_messages(
                &mut client,
                "SELECT $1 AS v",
                &[oid],
                &[1],
                &[Some("héllo ".as_bytes())],
                &[],
            );
            assert_eq!(tags(&messages), "12TDCZ", "{oid}: {messages:?}");
            assert_eq!(
                data_row(&messages),
                vec![Some("héllo ".to_owned())],
                "{oid}"
            );
        }

        // A name holds at most 63 bytes.
        let name = "a".repeat(63);
        let messages = formatted_messages(
            &mut client,
            "SELECT $1 AS v",
            &[19],
            &[1],
            &[Some(name.as_bytes())],
            &[],
        );
        assert_eq!(data_row(&messages), vec![Some(name)]);
        let name = "a".repeat(64);
        let messages = formatted_messages(
            &mut client,
            "SELECT $1 AS v",
            &[19],
            &[1],
            &[Some(name.as_bytes())],
            &[],
        );
        assert_eq!(
            error_of(&messages),
            (
                SQLSTATE_NAME_TOO_LONG.to_owned(),
                "identifier too long".to_owned()
            )
        );

        // A bytea column stores only blobs, and bytea bytes need not be UTF-8.
        conn.execute("CREATE TABLE b (v bytea)").unwrap();
        let messages = formatted_messages(
            &mut client,
            "INSERT INTO b VALUES ($1)",
            &[17],
            &[1],
            &[Some(b"\0\xff")],
            &[],
        );
        assert_eq!(tags(&messages), "12nCZ", "{messages:?}");
        let messages = formatted_messages(&mut client, "SELECT v FROM b", &[], &[], &[], &[1]);
        assert_eq!(tags(&messages), "12TDCZ");
        assert_eq!(messages[3].1, b"\0\x01\0\0\0\x02\0\xff");
    }

    #[test]
    fn parameters_must_be_utf8_without_nul() {
        let (mut client, _) = copy_test_client();
        // PostgreSQL shows the bytes of the first character that is not
        // valid, as many as its first byte says it has.
        for (bytes, shown) in [
            (&b"\xe2\x82"[..], "0xe2 0x82"),
            (b"\xe2\x82\x41", "0xe2 0x82 0x41"),
            (b"x\xc3", "0xc3"),
            (b"\xf0\x9f\x98", "0xf0 0x9f 0x98"),
            (b"\xed\xa0\x80", "0xed 0xa0 0x80"),
            (b"\xc0\x80", "0xc0 0x80"),
            (b"a\xffb", "0xff"),
            (b"\xff\x00", "0xff"),
            (b"a\0b", "0x00"),
            (b"a\0\xff", "0x00"),
        ] {
            for format in [0, 1] {
                let messages = formatted_messages(
                    &mut client,
                    "SELECT $1 AS v",
                    &[25],
                    &[format],
                    &[Some(bytes)],
                    &[],
                );
                assert_eq!(
                    error_of(&messages),
                    (
                        SQLSTATE_CHARACTER_NOT_IN_REPERTOIRE.to_owned(),
                        format!("invalid byte sequence for encoding \"UTF8\": {shown}")
                    ),
                    "{bytes:?} in format {format}"
                );
            }
        }
    }

    #[test]
    fn binary_results_are_sent_as_postgresql_sends_them() {
        let (mut client, conn) = copy_test_client();
        conn.execute(
            "CREATE TABLE r (s smallint, v varchar(10), b bytea);
             INSERT INTO r VALUES (-2, 'héllo', 'abc'::bytea)",
        )
        .unwrap();
        let sql = "SELECT true AS t, false AS f, s, 42::int4 AS i, 42::int8 AS l, \
                   1.5::float8 AS d, 'x'::text AS x, v, b FROM r";
        let messages = formatted_messages(&mut client, sql, &[], &[], &[], &[1]);
        assert_eq!(tags(&messages), "12TDCZ", "{messages:?}");
        // Each value is a 4-byte length followed by the value.
        let mut expected = 9i16.to_be_bytes().to_vec();
        for value in [
            &[1][..],
            &[0],
            &[0xff, 0xfe],
            &[0, 0, 0, 42],
            &[0, 0, 0, 0, 0, 0, 0, 42],
            &[63, 248, 0, 0, 0, 0, 0, 0],
            b"x",
            "héllo".as_bytes(),
            b"abc",
        ] {
            expected.extend((value.len() as i32).to_be_bytes());
            expected.extend(value);
        }
        assert_eq!(messages[3].1, expected);

        let sql = "SELECT 42::int8 AS a, 'x'::text AS b";
        let messages = formatted_messages(&mut client, sql, &[], &[], &[], &[1, 0]);
        assert_eq!(tags(&messages), "12TDCZ");
        assert_eq!(
            messages[3].1,
            [0, 2, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 42, 0, 0, 0, 1, b'x']
        );
        let messages =
            formatted_messages(&mut client, "SELECT NULL::int8 AS n", &[], &[], &[], &[1]);
        assert_eq!(tags(&messages), "12TDCZ");
        assert_eq!(messages[3].1, [0, 1, 255, 255, 255, 255]);
    }

    #[test]
    fn binary_results_of_other_types_are_refused_before_any_row() {
        let (mut client, conn) = copy_test_client();
        conn.execute(
            "CREATE TABLE typed (d date, u uuid, n numeric(10,2));
             INSERT INTO typed VALUES
               ('2024-01-02', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 1.5)",
        )
        .unwrap();
        for (column, pg_type) in [("d", "date"), ("u", "uuid"), ("n", "numeric")] {
            let sql = format!("SELECT {column} FROM typed");
            let expected = (
                SQLSTATE_FEATURE_NOT_SUPPORTED.to_owned(),
                format!(
                    "binary format is not supported for result column \"{column}\" of type {pg_type}"
                ),
            );
            let messages = formatted_messages(&mut client, &sql, &[], &[], &[], &[1]);
            assert_eq!(tags(&messages), "12EZ", "{sql}");
            assert_eq!(error_of(&messages), expected, "{sql}");

            // Without a Describe, Execute refuses before its first row.
            client.send_parse("", &sql, &[]).unwrap();
            client.send_bind("", "", &[], &[], &[1]).unwrap();
            client.send_execute("", 0).unwrap();
            client.send_sync().unwrap();
            let messages = read_until_ready(&mut client);
            assert_eq!(tags(&messages), "12EZ", "{sql}");
            assert_eq!(error_of(&messages), expected, "{sql}");
        }
        let messages = formatted_messages(
            &mut client,
            "SELECT 42::int8 AS a, d FROM typed",
            &[],
            &[],
            &[],
            &[0, 1],
        );
        assert_eq!(tags(&messages), "12EZ");
        assert_eq!(
            error_of(&messages).1,
            "binary format is not supported for result column \"d\" of type date"
        );
    }

    #[test]
    fn format_codes_and_counts_are_checked_as_in_postgresql() {
        let (mut client, _) = copy_test_client();
        let int8 = "SELECT $1::int8";
        for (sql, param_formats, params, result_formats, expected_tags, sqlstate, message) in [
            (
                int8,
                vec![0, 0],
                vec![Some(&b"1"[..])],
                vec![],
                "1EZ",
                SQLSTATE_PROTOCOL_VIOLATION,
                "bind message has 2 parameter formats but 1 parameters",
            ),
            (
                int8,
                vec![2, 0],
                vec![Some(b"1")],
                vec![],
                "1EZ",
                SQLSTATE_PROTOCOL_VIOLATION,
                "bind message has 2 parameter formats but 1 parameters",
            ),
            (
                int8,
                vec![2],
                vec![Some(b"1")],
                vec![],
                "1EZ",
                SQLSTATE_INVALID_PARAMETER_VALUE,
                "unsupported format code: 2",
            ),
            (
                int8,
                vec![-1],
                vec![None],
                vec![],
                "1EZ",
                SQLSTATE_INVALID_PARAMETER_VALUE,
                "unsupported format code: -1",
            ),
            // PostgreSQL refuses a result format code when it sends the
            // first row in it; tursopg refuses it at Bind.
            (
                "SELECT 1",
                vec![],
                vec![],
                vec![2],
                "1EZ",
                SQLSTATE_INVALID_PARAMETER_VALUE,
                "unsupported format code: 2",
            ),
            // PostgreSQL refuses a wrong result format count at Bind; tursopg
            // knows the columns only at Describe or Execute, after
            // BindComplete.
            (
                "SELECT 1",
                vec![],
                vec![],
                vec![0, 0],
                "12EZ",
                SQLSTATE_PROTOCOL_VIOLATION,
                "bind message has 2 result formats but query has 1 columns",
            ),
        ] {
            let messages = formatted_messages(
                &mut client,
                sql,
                &[],
                &param_formats,
                &params,
                &result_formats,
            );
            assert_eq!(tags(&messages), expected_tags, "{message}");
            assert_eq!(
                error_of(&messages),
                (sqlstate.to_owned(), message.to_owned())
            );
        }
    }

    #[test]
    fn refused_formats_abort_the_transaction_block() {
        let (mut client, conn) = copy_test_client();
        conn.execute("CREATE TABLE o (i int)").unwrap();
        conn.execute("INSERT INTO o VALUES (3000000000)").unwrap();
        let int8 = 42i64.to_be_bytes();
        for (sql, param_types, param_formats, params, result_formats) in [
            (
                "SELECT $1::int8",
                &[20][..],
                &[1][..],
                &[Some(&int8[..])][..],
                &[][..],
            ),
            (
                "SELECT CAST('2024-01-02' AS date) AS d",
                &[],
                &[],
                &[],
                &[1],
            ),
            // Refused while the row is encoded.
            ("SELECT i FROM o", &[], &[], &[], &[1]),
        ] {
            query_messages(&mut client, "BEGIN");
            let messages = formatted_messages(
                &mut client,
                sql,
                param_types,
                param_formats,
                params,
                result_formats,
            );
            assert_eq!(
                error_of(&messages).0,
                SQLSTATE_FEATURE_NOT_SUPPORTED,
                "{sql}"
            );
            assert_eq!(messages.last().unwrap().1, b"E", "{sql}");
            assert_block_aborted(&mut client, sql);
        }
    }

    #[test]
    fn text_that_is_not_valid_utf8_is_refused() {
        /// A message's type byte and body.
        type Message = (u8, &'static [u8]);
        let (mut client, _conn) = copy_test_client();
        let parse_select_1: Message = (b'P', b"\0SELECT 1\0\0\0");
        let sync: Message = (b'S', b"");
        let cases: [(&str, &[Message]); 4] = [
            ("Query", &[(b'Q', b"SELECT 'a\xffb'\0")]),
            ("Parse query", &[(b'P', b"\0SELECT 'a\xffb'\0\0\0"), sync]),
            ("Parse name", &[(b'P', b"s\xff\0SELECT 1\0\0\0"), sync]),
            (
                "Bind portal name",
                &[parse_select_1, (b'B', b"p\xff\0\0\0\0\0\0\0\0"), sync],
            ),
        ];
        for (context, messages) in cases {
            query_messages(&mut client, "BEGIN");
            for &(tag, body) in messages {
                client.write_message(tag, body).unwrap();
            }
            let replies = read_until_ready(&mut client);
            assert_eq!(
                error_of(&replies).0,
                SQLSTATE_FEATURE_NOT_SUPPORTED,
                "{context}"
            );
            assert_block_aborted(&mut client, context);
        }
        // A U+FFFD the client sent is refused too, since it looks the same.
        let messages = query_messages(&mut client, "SELECT '\u{FFFD}'");
        assert_eq!(error_of(&messages).0, SQLSTATE_FEATURE_NOT_SUPPORTED);
        let messages = query_messages(&mut client, "SELECT 'é'");
        assert_eq!(data_row(&messages), vec![Some("é".to_string())]);
    }

    fn query_messages(client: &mut PgConn, sql: &str) -> Vec<(u8, Vec<u8>)> {
        client.send_query(sql).unwrap();
        read_until_ready(client)
    }

    fn read_until_ready(client: &mut PgConn) -> Vec<(u8, Vec<u8>)> {
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

    /// The SQLSTATE and message of the only ErrorResponse among the messages.
    fn error_of(messages: &[(u8, Vec<u8>)]) -> (String, String) {
        let errors: Vec<_> = messages.iter().filter(|m| m.0 == b'E').collect();
        assert_eq!(errors.len(), 1, "{messages:?}");
        let fields = turso_pg_client::parse_error_fields(&errors[0].1).unwrap();
        (fields[&b'C'].clone(), fields[&b'M'].clone())
    }

    /// Runs `sql` with the extended query protocol and text parameters.
    fn extended_messages(
        client: &mut PgConn,
        sql: &str,
        params: &[Option<&[u8]>],
    ) -> Vec<(u8, Vec<u8>)> {
        client.send_parse("", sql, &[]).unwrap();
        client.send_bind("", "", &[], params, &[]).unwrap();
        client.send_execute("", 0).unwrap();
        client.send_sync().unwrap();
        read_until_ready(client)
    }

    /// Runs `sql` with the extended query protocol, the given parameter types
    /// and format codes, and a Describe of the portal before Execute.
    fn formatted_messages(
        client: &mut PgConn,
        sql: &str,
        param_types: &[u32],
        param_formats: &[i16],
        params: &[Option<&[u8]>],
        result_formats: &[i16],
    ) -> Vec<(u8, Vec<u8>)> {
        client.send_parse("", sql, param_types).unwrap();
        client
            .send_bind("", "", param_formats, params, result_formats)
            .unwrap();
        client.send_describe_portal("").unwrap();
        client.send_execute("", 0).unwrap();
        client.send_sync().unwrap();
        read_until_ready(client)
    }

    /// The message types, such as "12TDCZ" for ParseComplete, BindComplete,
    /// RowDescription, DataRow, CommandComplete and ReadyForQuery.
    fn tags(messages: &[(u8, Vec<u8>)]) -> String {
        messages.iter().map(|message| message.0 as char).collect()
    }

    fn data_row(messages: &[(u8, Vec<u8>)]) -> Vec<Option<String>> {
        let rows: Vec<_> = messages.iter().filter(|m| m.0 == b'D').collect();
        assert_eq!(rows.len(), 1, "{messages:?}");
        turso_pg_client::parse_data_row(&rows[0].1).unwrap()
    }

    /// Checks that the open transaction block is aborted: the next statement
    /// gets 25P02 and COMMIT rolls back.
    fn assert_block_aborted(client: &mut PgConn, context: &str) {
        let messages = query_messages(client, "SELECT 1");
        assert_eq!(
            error_of(&messages).0,
            turso_pg_parser::sqlstate::SQLSTATE_IN_FAILED_SQL_TRANSACTION,
            "{context}"
        );
        let messages = query_messages(client, "COMMIT");
        assert_eq!(messages[0], (b'C', b"ROLLBACK\0".to_vec()), "{context}");
        assert_eq!(messages.last().unwrap().1, b"I", "{context}");
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
        conn.execute("CREATE TABLE t (i int, b bigint)").unwrap();
        conn.execute("INSERT INTO t VALUES (42, 42)").unwrap();
        conn.execute("CREATE TABLE o (i int)").unwrap();
        conn.execute("INSERT INTO o VALUES (3000000000)").unwrap();
        conn.execute("CREATE TABLE s (v smallint)").unwrap();
        conn.execute("INSERT INTO s VALUES (-2)").unwrap();
        // Each field is a 4-byte length followed by the value.
        for (sql, expected_type, binary, text) in [
            (
                "SELECT v FROM s",
                Type::INT2,
                &b"\0\0\0\x02\xff\xfe"[..],
                &b"\0\0\0\x02-2"[..],
            ),
            (
                "SELECT 42::int4",
                Type::INT4,
                b"\0\0\0\x04\0\0\0\x2a",
                b"\0\0\0\x0242",
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
            (
                "SELECT coalesce(NULL::float8, 1)",
                Type::FLOAT8,
                b"\0\0\0\x08\x3f\xf0\0\0\0\0\0\0",
                b"\0\0\0\x011",
            ),
        ] {
            for (format, expected) in [(Format::UnifiedBinary, binary), (Format::UnifiedText, text)]
            {
                let (pg_type, data) = encode_first_value(&conn, sql, &format).unwrap();
                assert_eq!(pg_type, expected_type, "{sql}");
                assert_eq!(data, expected, "{sql}");
            }
        }

        // An int4 value outside the i32 range has no 4-byte binary encoding.
        let error = encode_first_value(&conn, "SELECT i FROM o", &Format::UnifiedBinary)
            .map(|_| ())
            .unwrap_err();
        let error = turso_pg::Error::from(error);
        assert_eq!(error.sqlstate, SQLSTATE_FEATURE_NOT_SUPPORTED);
        assert_eq!(
            error.message,
            "cannot send 3000000000 in binary format as type int4: it is out of range"
        );
        let (_, text) = encode_first_value(&conn, "SELECT i FROM o", &Format::UnifiedText).unwrap();
        assert_eq!(text, b"\0\0\0\x0a3000000000");

        for (value, pg_type, message) in [
            (
                Value::from_text("abc".to_owned()),
                Type::INT8,
                "cannot send a value of storage class TEXT in binary format as type int8",
            ),
            (
                Value::from_i64(2),
                Type::BOOL,
                "cannot send 2 in binary format as type bool: only 0 and 1 are booleans",
            ),
            (
                Value::from_text("abc".to_owned()),
                Type::BYTEA,
                "cannot send a value of storage class TEXT in binary format as type bytea",
            ),
            (
                Value::from_i64(32768),
                Type::INT2,
                "cannot send 32768 in binary format as type int2: it is out of range",
            ),
            (
                Value::from_i64(-2147483649),
                Type::INT4,
                "cannot send -2147483649 in binary format as type int4: it is out of range",
            ),
        ] {
            let error = encode_binary_value(&value, &pg_type).unwrap_err();
            let error = turso_pg::Error::from(error);
            assert_eq!(error.sqlstate, SQLSTATE_FEATURE_NOT_SUPPORTED, "{message}");
            assert_eq!(error.message, message);
        }
        for (value, pg_type, expected) in [
            (Value::from_i64(0), Type::BOOL, &b"\0\0\0\x01\0"[..]),
            (Value::from_i64(1), Type::BOOL, b"\0\0\0\x01\x01"),
            (Value::Null, Type::BOOL, b"\xff\xff\xff\xff"),
            (Value::from_i64(-32768), Type::INT2, b"\0\0\0\x02\x80\0"),
            (Value::from_i64(32767), Type::INT2, b"\0\0\0\x02\x7f\xff"),
            (
                Value::from_i64(-2147483648),
                Type::INT4,
                b"\0\0\0\x04\x80\0\0\0",
            ),
            (
                Value::from_i64(2147483647),
                Type::INT4,
                b"\0\0\0\x04\x7f\xff\xff\xff",
            ),
            (
                Value::from_blob(vec![0, 0xff]),
                Type::BYTEA,
                b"\0\0\0\x02\0\xff",
            ),
        ] {
            assert_eq!(
                encode_binary_value(&value, &pg_type).unwrap(),
                expected,
                "{value:?} as {pg_type}"
            );
        }
    }

    /// The type of a query's first column and the DataRow bytes of the first
    /// value in the given result format.
    fn encode_first_value(
        conn: &PgConnection,
        sql: &str,
        format: &Format,
    ) -> Result<(Type, Vec<u8>), LimboError> {
        let mut stmt = conn.prepare(sql).unwrap();
        let fields = Arc::new(apply_result_formats(text_fields(&stmt), format)?);
        let rows = stmt.run_collect_rows().unwrap();
        let mut encoder = DataRowEncoder::new(fields.clone());
        encode_value(&mut encoder, &rows[0][0], &fields[0])?;
        let data = encoder.finish().unwrap().data.to_vec();
        Ok((fields[0].datatype().clone(), data))
    }

    /// The DataRow bytes of `value` in binary format as type `pg_type`.
    fn encode_binary_value(value: &Value, pg_type: &Type) -> Result<Vec<u8>, LimboError> {
        let field = FieldInfo::new(
            "v".to_owned(),
            None,
            None,
            pg_type.clone(),
            FieldFormat::Binary,
        );
        let mut encoder = DataRowEncoder::new(Arc::new(vec![field.clone()]));
        encode_value(&mut encoder, value, &field)?;
        Ok(encoder.finish().unwrap().data.to_vec())
    }

    #[test]
    fn row_description_types_match_pg_attribute() {
        let (_io, db) = turso_pg::open_database(
            ":memory:",
            None,
            turso_pg::OpenFlags::default(),
            turso_pg::DatabaseOpts::new(),
        )
        .unwrap();
        let conn = PgConnection::connect(&db).unwrap();
        conn.execute(
            "CREATE TABLE typed (a smallint, b numeric(10,2), c varchar(10), d macaddr8, \
             e integer, f text, g double precision, h smallint[], i varchar(10)[])",
        )
        .unwrap();
        let stmt = conn.prepare("SELECT * FROM typed").unwrap();
        let types: Vec<Type> = text_fields(&stmt)
            .iter()
            .map(|field| field.datatype().clone())
            .collect();
        assert_eq!(
            types,
            [
                Type::INT2,
                Type::NUMERIC,
                Type::VARCHAR,
                Type::MACADDR8,
                Type::INT4,
                Type::TEXT,
                Type::FLOAT8,
                Type::INT2_ARRAY,
                Type::VARCHAR_ARRAY,
            ]
        );
        // pg_attribute gives the element type for the array columns h and i
        // (#11937), so only the scalar columns a to g are compared.
        let mut stmt = conn
            .prepare(
                "SELECT a.atttypid FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid \
                 WHERE c.relname = 'typed' AND a.attname IN ('a', 'b', 'c', 'd', 'e', 'f', 'g') \
                 ORDER BY a.attnum",
            )
            .unwrap();
        let oids: Vec<Value> = stmt
            .run_collect_rows()
            .unwrap()
            .into_iter()
            .map(|row| row[0].clone())
            .collect();
        let scalar_column_types = &types[..7];
        let expected: Vec<Value> = scalar_column_types
            .iter()
            .map(|t| Value::from_i64(i64::from(t.oid())))
            .collect();
        assert_eq!(oids, expected);
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
            // The column is typed text, whose binary format is the same text.
            for format in [Format::UnifiedText, Format::UnifiedBinary] {
                let (pg_type, data) = encode_first_value(&conn, sql, &format).unwrap();
                assert_eq!(pg_type, Type::TEXT, "{sql}");
                assert_eq!(data, b"\0\0\0\x01t", "{sql}");
            }
        }
        let stmt = conn.prepare("SELECT 7 AS pg_is_in_recovery").unwrap();
        let fields = text_fields(&stmt);
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
        execute_non_query(&mut begin, &turso_pg_parser::parse("BEGIN").unwrap()).unwrap();
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
        let Response::Execution(tag) = response else {
            panic!("COMMIT must report a command tag");
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
            let fields = text_fields(&stmt);
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
            let (pg_type, _) = encode_first_value(&conn, sql, &Format::UnifiedText).unwrap();
            assert_eq!(pg_type, expected_type, "{sql}");
        }

        let (_, data) =
            encode_first_value(&conn, "SELECT booleq(true, true)", &Format::UnifiedText).unwrap();
        assert_eq!(data, b"\0\0\0\x01t");
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
            assert!(describe_fields(&stmt).is_empty(), "{sql}");
        }
        let sql = "SELECT 'LOCK TABLE items'";
        let query = conn.prepare(sql).unwrap();
        assert_eq!(describe_fields(&query).len(), 1);
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
    fn declared_type_names_map_to_pg_types() {
        for (declared, expected) in [
            ("INTEGER", Type::INT4),
            ("smallint", Type::INT2),
            ("bigint", Type::INT8),
            ("REAL", Type::FLOAT8),
            ("TEXT", Type::TEXT),
            ("varchar(10)", Type::VARCHAR),
            ("numeric(10,2)", Type::NUMERIC),
            ("BLOB", Type::BYTEA),
            ("bytea", Type::BYTEA),
            ("boolean", Type::BOOL),
            ("uuid", Type::UUID),
            ("json", Type::JSON),
            ("jsonb", Type::JSONB),
            ("date", Type::DATE),
            ("time", Type::TIME),
            ("timestamp", Type::TIMESTAMP),
            ("timestamptz", Type::TIMESTAMPTZ),
            ("inet", Type::INET),
            ("cidr", Type::CIDR),
            ("macaddr", Type::MACADDR),
            ("macaddr8", Type::MACADDR8),
            ("mood", Type::TEXT),
        ] {
            assert_eq!(declared_type_to_pg_type(declared), expected, "{declared}");
        }
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
