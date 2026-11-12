use std::collections::VecDeque;
use std::sync::Arc;

use turso_core::{
    ConnectAsyncState, ConnectOpts, Connection, Database, IOResult, LimboError, OpenDbAsyncState,
    OpenOptions, Statement, Value, IO,
};

/// Opens a PostgreSQL database and initializes its catalog before returning it.
/// `options` must use `PostgresDialect`; `connect_opts` supplies the bootstrap
/// connection's encryption key and superuser role. Drop the state after an
/// error before retrying the open.
pub fn open_database_async(
    state: &mut OpenDatabaseState,
    io: Arc<dyn IO>,
    path: &str,
    options: &OpenOptions,
    connect_opts: ConnectOpts,
) -> turso_core::types::IOResultOr<Arc<Database>> {
    loop {
        match state.phase {
            OpenPhase::Opening => {
                let db = match Database::open_async(&mut state.core, io.clone(), path, options)? {
                    IOResult::Done(value) => value,
                    IOResult::IO(completions) => return Ok(IOResult::IO(completions)),
                };
                if db.dialect().name() != "postgres" {
                    return Err(LimboError::InvalidArgument(
                        "PostgreSQL opening requires PostgresDialect".into(),
                    )
                    .into());
                }
                state.db = Some(db);
                state.phase = OpenPhase::Connecting;
            }
            OpenPhase::Connecting => {
                let db = state.db.as_ref().unwrap();
                let conn = match db.connect_async(connect_opts.clone(), &mut state.connect)? {
                    IOResult::Done(value) => value,
                    IOResult::IO(completions) => return Ok(IOResult::IO(completions)),
                };
                crate::session::refuse_tables_in_no_schema(&conn)?;
                if crate::classes::initialize_schema(&conn.current_schema(), db.is_readonly())?
                    .is_empty()
                {
                    state.phase = OpenPhase::Done;
                    return Ok(IOResult::Done(state.db.take().unwrap()));
                }
                state.statement = Some(conn.prepare_engine_schema_statement("BEGIN IMMEDIATE")?);
                state.conn = Some(conn);
                state.phase = OpenPhase::Beginning;
            }
            OpenPhase::Beginning => {
                match state
                    .statement
                    .as_mut()
                    .unwrap()
                    .run_ignore_rows_nonblock()?
                {
                    IOResult::Done(value) => value,
                    IOResult::IO(completions) => return Ok(IOResult::IO(completions)),
                };
                state.statement = None;
                let conn = state.conn.as_ref().unwrap();
                state.queries = crate::classes::initialize_schema(
                    &conn.current_schema(),
                    state.db.as_ref().unwrap().is_readonly(),
                )?
                .into();
                state.phase = OpenPhase::Initializing;
            }
            OpenPhase::Initializing => {
                let conn = state.conn.as_ref().unwrap();
                if state.statement.is_none() {
                    let Some(sql) = state.queries.pop_front() else {
                        state.queries =
                            crate::schema_state::schema_state_queries(&conn.current_schema())
                                .into();
                        state.phase = OpenPhase::Reading;
                        continue;
                    };
                    state.statement = Some(conn.prepare_engine_schema_statement(&sql)?);
                }
                match state
                    .statement
                    .as_mut()
                    .unwrap()
                    .run_ignore_rows_nonblock()?
                {
                    IOResult::Done(value) => value,
                    IOResult::IO(completions) => return Ok(IOResult::IO(completions)),
                };
                state.statement = None;
            }
            OpenPhase::Reading => {
                let conn = state.conn.as_ref().unwrap();
                if state.statement.is_none() {
                    let Some(sql) = state.queries.pop_front() else {
                        let catalog = crate::schema_state::build_schema_state(
                            &conn.current_schema(),
                            &state.results,
                        )?;
                        conn.with_schema_mut(|schema| schema.dialect_schema = Some(catalog))?;
                        state.statement = Some(conn.prepare_engine_schema_statement("COMMIT")?);
                        state.phase = OpenPhase::Committing;
                        continue;
                    };
                    state.statement = Some(conn.prepare_engine_schema_statement(&sql)?);
                }
                let rows = &mut state.rows;
                match state
                    .statement
                    .as_mut()
                    .unwrap()
                    .run_with_row_callback_nonblock(|row| {
                        rows.push(row.get_values().cloned().collect());
                        Ok(())
                    })? {
                    IOResult::Done(value) => value,
                    IOResult::IO(completions) => return Ok(IOResult::IO(completions)),
                };
                state.statement = None;
                state.results.push(std::mem::take(&mut state.rows));
            }
            OpenPhase::Committing => {
                match state
                    .statement
                    .as_mut()
                    .unwrap()
                    .run_ignore_rows_nonblock()?
                {
                    IOResult::Done(value) => value,
                    IOResult::IO(completions) => return Ok(IOResult::IO(completions)),
                };
                state.statement = None;
                state.phase = OpenPhase::Done;
                return Ok(IOResult::Done(state.db.take().unwrap()));
            }
            OpenPhase::Done => panic!("PostgreSQL open called after completion"),
        }
    }
}

/// Resumable PostgreSQL database opening, including catalog initialization.
#[derive(Default)]
pub struct OpenDatabaseState {
    core: OpenDbAsyncState,
    connect: ConnectAsyncState,
    phase: OpenPhase,
    db: Option<Arc<Database>>,
    conn: Option<Arc<Connection>>,
    statement: Option<Statement>,
    queries: VecDeque<String>,
    rows: Vec<Vec<Value>>,
    results: Vec<Vec<Vec<Value>>>,
}

#[derive(Default)]
enum OpenPhase {
    #[default]
    Opening,
    Connecting,
    Beginning,
    Initializing,
    Reading,
    Committing,
    Done,
}
