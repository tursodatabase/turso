use std::sync::{Arc, Mutex};

use sql_generation::model::table::SimValue;
use tracing::instrument;
use turso_core::{Connection, LimboError, Result, StepResult, Value};

use crate::{
    generation::{
        Shadow as _,
        plan::{
            ConnectionState, Interaction, InteractionPlanIterator, InteractionPlanState,
            InteractionType, ResultSet,
        },
    },
    model::Query,
};

use super::env::{SimConnection, SimulatorEnv};

#[derive(Debug, Clone, Copy)]
pub struct Execution {
    pub connection_index: usize,
    pub interaction_index: usize,
}

impl Execution {
    pub fn new(connection_index: usize, interaction_index: usize) -> Self {
        Self {
            connection_index,
            interaction_index,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionHistory {
    pub history: Vec<Execution>,
}

impl ExecutionHistory {
    pub fn new() -> Self {
        Self {
            history: Vec::new(),
        }
    }
}

pub struct ExecutionResult {
    pub history: ExecutionHistory,
    pub error: Option<LimboError>,
}

impl ExecutionResult {
    pub fn new(history: ExecutionHistory, error: Option<LimboError>) -> Self {
        Self { history, error }
    }
}

pub(crate) fn execute_interactions(
    env: Arc<Mutex<SimulatorEnv>>,
    mut plan: impl InteractionPlanIterator,
    state: &mut InteractionPlanState,
    conn_states: &mut [ConnectionState],
    last_execution: Arc<Mutex<Execution>>,
) -> ExecutionResult {
    let mut history = ExecutionHistory::new();
    let now = std::time::Instant::now();
    env.clear_poison();
    let mut env = env.lock().unwrap();

    env.clear_tables();

    let mut interaction = plan
        .next(&mut env)
        .expect("we should always have at least 1 interaction to start");

    for _tick in 0..env.opts.ticks {
        tracing::trace!("Executing tick {}", _tick);

        let connection_index = interaction.connection_index;
        let conn_state = &mut conn_states[connection_index];

        history
            .history
            .push(Execution::new(connection_index, state.interaction_pointer));
        let mut last_execution = last_execution.lock().unwrap();
        last_execution.connection_index = connection_index;
        last_execution.interaction_index = state.interaction_pointer;
        // Execute the interaction for the selected connection
        match execute_plan(&mut env, &interaction, conn_state) {
            Ok(ExecutionContinuation::NextInteraction) => {
                state.interaction_pointer += 1;
                let Some(new_interaction) = plan.next(&mut env) else {
                    break;
                };
                interaction = new_interaction;
            }
            Err(err) => {
                return ExecutionResult::new(history, Some(err));
            }
            _ => {}
        }
        // Check if the maximum time for the simulation has been reached
        if now.elapsed().as_secs() >= env.opts.max_time_simulation as u64 {
            return ExecutionResult::new(
                history,
                Some(LimboError::InternalError(
                    "maximum time for simulation reached".into(),
                )),
            );
        }
    }

    ExecutionResult::new(history, None)
}

pub fn execute_plan(
    env: &mut SimulatorEnv,
    interaction: &Interaction,
    conn_state: &mut ConnectionState,
) -> Result<ExecutionContinuation> {
    let connection_index = interaction.connection_index;
    let connection = &mut env.connections[connection_index];
    if let SimConnection::Disconnected = connection {
        tracing::debug!("connecting {}", connection_index);
        env.connect(connection_index);
        Ok(ExecutionContinuation::Stay)
    } else {
        tracing::debug!("connection {} already connected", connection_index);
        execute_interaction(env, interaction, &mut conn_state.stack)
    }
}

/// The next point of control flow after executing an interaction.
/// `execute_interaction` uses this type in conjunction with a result, where
/// the `Err` case indicates a full-stop due to a bug, and the `Ok` case
/// indicates the next step in the plan.
#[derive(PartialEq, Debug)]
pub(crate) enum ExecutionContinuation {
    /// Stay in the current interaction
    Stay,
    /// Default continuation, execute the next interaction.
    NextInteraction,
    //  /// Typically used in the case of preconditions failures, skip to the next property.
    // NextProperty,
}

pub fn execute_interaction(
    env: &mut SimulatorEnv,
    interaction: &Interaction,
    stack: &mut Vec<ResultSet>,
) -> Result<ExecutionContinuation> {
    let connection = &mut env.connections[interaction.connection_index];
    match connection {
        SimConnection::LimboConnection(..) => execute_interaction_turso(env, interaction, stack),
        SimConnection::SQLiteConnection(..) => {
            execute_interaction_rusqlite(env, interaction, stack)
        }
        SimConnection::Disconnected => unreachable!(),
    }
}

#[instrument(skip(env, interaction, stack), fields(seed = %env.opts.seed, interaction = %interaction))]
pub fn execute_interaction_turso(
    env: &mut SimulatorEnv,
    interaction: &Interaction,
    stack: &mut Vec<ResultSet>,
) -> Result<ExecutionContinuation> {
    let SimConnection::LimboConnection(conn) = &mut env.connections[interaction.connection_index]
    else {
        unreachable!()
    };
    // Leave this empty info! here to print the span of the execution
    tracing::info!("");
    match &interaction.interaction {
        InteractionType::Query(_) => {
            tracing::debug!(?interaction);
            let results = interaction.execute_query(conn);
            if results.is_err() {
                tracing::error!(?results);
            }
            stack.push(results);
            // TODO: skip integrity check with mvcc
            if !env.profile.experimental_mvcc {
                limbo_integrity_check(conn)?;
            }
        }
        InteractionType::FsyncQuery(query) => {
            let results = interaction.execute_fsync_query(conn.clone(), env);
            if results.is_err() {
                tracing::error!(?results);
            }
            stack.push(results);

            let query_interaction = Interaction::new(
                interaction.connection_index,
                InteractionType::Query(query.clone()),
            );

            execute_interaction(env, &query_interaction, stack)?;
        }
        InteractionType::Assertion(_) => {
            interaction.execute_assertion(stack, env)?;
            stack.clear();
        }
        InteractionType::Assumption(_) => {
            let assumption_result = interaction.execute_assumption(stack, env);
            stack.clear();

            if let Err(err) = assumption_result {
                tracing::warn!("assumption failed: {:?}", err);
                return Err(err);
            }
        }
        InteractionType::Fault(_) => {
            interaction.execute_fault(env, interaction.connection_index)?;
        }
        InteractionType::FaultyQuery(_) => {
            let conn = conn.clone();
            let results = interaction.execute_faulty_query(&conn, env);
            if results.is_err() {
                tracing::error!(?results);
            }
            stack.push(results);
            // Reset fault injection
            env.io.inject_fault(false);
            // TODO: skip integrity check with mvcc
            if !env.profile.experimental_mvcc {
                limbo_integrity_check(&conn)?;
            }
        }
    }
    let _ = interaction.shadow(&mut env.get_conn_tables_mut(interaction.connection_index));
    Ok(ExecutionContinuation::NextInteraction)
}

fn limbo_integrity_check(conn: &Arc<Connection>) -> Result<()> {
    let mut rows = conn.query("PRAGMA integrity_check;")?.unwrap();
    let mut result = Vec::new();

    while let Ok(row) = rows.step() {
        match row {
            StepResult::Row => {
                let row = rows.row().unwrap();

                let val = match row.get_value(0) {
                    turso_core::Value::Text(text) => text.as_str().to_string(),
                    _ => unreachable!(),
                };
                result.push(val);
            }
            StepResult::IO => {
                rows.run_once()?;
            }
            StepResult::Interrupt => {}
            StepResult::Done => {
                break;
            }
            StepResult::Busy => {
                return Err(LimboError::Busy);
            }
        }
    }

    if result.is_empty() {
        return Err(LimboError::InternalError(
            "PRAGMA integrity_check did not return a value".to_string(),
        ));
    }
    let message = result.join("\n");
    if message != "ok" {
        return Err(LimboError::InternalError(format!(
            "Integrity Check Failed: {message}"
        )));
    }
    Ok(())
}

fn execute_interaction_rusqlite(
    env: &mut SimulatorEnv,
    interaction: &Interaction,
    stack: &mut Vec<ResultSet>,
) -> turso_core::Result<ExecutionContinuation> {
    tracing::trace!(
        "execute_interaction_rusqlite(connection_index={}, interaction={})",
        interaction.connection_index,
        interaction
    );
    let SimConnection::SQLiteConnection(conn) = &mut env.connections[interaction.connection_index]
    else {
        unreachable!()
    };
    match &interaction.interaction {
        InteractionType::Query(query) => {
            tracing::debug!("{}", interaction);
            let results = execute_query_rusqlite(conn, query).map_err(|e| {
                turso_core::LimboError::InternalError(format!("error executing query: {e}"))
            });
            tracing::debug!("{:?}", results);
            stack.push(results);
        }
        InteractionType::FsyncQuery(..) => {
            unimplemented!("cannot implement fsync query in rusqlite, as we do not control IO");
        }
        InteractionType::Assertion(_) => {
            interaction.execute_assertion(stack, env)?;
            stack.clear();
        }
        InteractionType::Assumption(_) => {
            let assumption_result = interaction.execute_assumption(stack, env);
            stack.clear();

            if let Err(err) = assumption_result {
                tracing::warn!("assumption failed: {:?}", err);
                return Err(err);
            }
        }
        InteractionType::Fault(_) => {
            interaction.execute_fault(env, interaction.connection_index)?;
        }
        InteractionType::FaultyQuery(_) => {
            unimplemented!("cannot implement faulty query in rusqlite, as we do not control IO");
        }
    }

    let _ = interaction.shadow(&mut env.get_conn_tables_mut(interaction.connection_index));
    Ok(ExecutionContinuation::NextInteraction)
}

fn execute_query_rusqlite(
    connection: &rusqlite::Connection,
    query: &Query,
) -> rusqlite::Result<Vec<Vec<SimValue>>> {
    match query {
        Query::Create(create) => {
            connection.execute(create.to_string().as_str(), ())?;
            Ok(vec![])
        }
        Query::Select(select) => {
            let mut stmt = connection.prepare(select.to_string().as_str())?;
            let columns = stmt.column_count();
            let rows = stmt.query_map([], |row| {
                let mut values = vec![];
                for i in 0..columns {
                    let value = row.get_unwrap(i);
                    let value = match value {
                        rusqlite::types::Value::Null => Value::Null,
                        rusqlite::types::Value::Integer(i) => Value::Integer(i),
                        rusqlite::types::Value::Real(f) => Value::Float(f),
                        rusqlite::types::Value::Text(s) => Value::build_text(s),
                        rusqlite::types::Value::Blob(b) => Value::Blob(b),
                    };
                    values.push(SimValue(value));
                }
                Ok(values)
            })?;
            let mut result = vec![];
            for row in rows {
                result.push(row?);
            }
            Ok(result)
        }
        Query::Insert(insert) => {
            connection.execute(insert.to_string().as_str(), ())?;
            Ok(vec![])
        }
        Query::Delete(delete) => {
            connection.execute(delete.to_string().as_str(), ())?;
            Ok(vec![])
        }
        Query::Drop(drop) => {
            connection.execute(drop.to_string().as_str(), ())?;
            Ok(vec![])
        }
        Query::Update(update) => {
            connection.execute(update.to_string().as_str(), ())?;
            Ok(vec![])
        }
        Query::CreateIndex(create_index) => {
            connection.execute(create_index.to_string().as_str(), ())?;
            Ok(vec![])
        }
        Query::Begin(begin) => {
            connection.execute(begin.to_string().as_str(), ())?;
            Ok(vec![])
        }
        Query::Commit(commit) => {
            connection.execute(commit.to_string().as_str(), ())?;
            Ok(vec![])
        }
        Query::Rollback(rollback) => {
            connection.execute(rollback.to_string().as_str(), ())?;
            Ok(vec![])
        }
    }
}
