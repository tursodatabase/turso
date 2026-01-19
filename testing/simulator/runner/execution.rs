use std::sync::{Arc, Mutex};

use rand::Rng;
use sql_generation::model::table::SimValue;
use tracing::instrument;
use turso_core::{Connection, LimboError, Result, Value};

/// Checks if an error is a recoverable error that should not fail the simulation.
/// These errors indicate expected behavior, not bugs.
///
/// - WriteWriteConflict: MVCC conflict, the DB rolled back the transaction
/// - TxError: Transaction state error (e.g., BEGIN twice, COMMIT with no transaction)
/// - Constraint: Constraint violation (e.g., UNIQUE, NOT NULL). The simulator may generate
///   queries that violate constraints (e.g., UPDATE setting a unique column to the same value
///   across multiple rows). The DB correctly rejects these with statement-level rollback.
fn is_recoverable_tx_error(err: &LimboError) -> bool {
    matches!(
        err,
        LimboError::WriteWriteConflict | LimboError::TxError(_) | LimboError::Constraint(_)
    )
}

/// Returns true if the error indicates the transaction was rolled back by the database.
fn error_causes_rollback(err: &LimboError) -> bool {
    matches!(err, LimboError::WriteWriteConflict)
}

use crate::{
    generation::Shadow as _,
    model::{
        Query, ResultSet,
        interactions::{
            ConnectionState, Interaction, InteractionBuilder, InteractionPlanIterator,
            InteractionPlanState, InteractionType,
        },
    },
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
    #[expect(dead_code)]
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
            Ok(ExecutionContinuation::NextInteractionOutsideThisProperty) => {
                // Skip remaining interactions in this property by advancing until we
                // find an interaction with a different id (i.e., a different property)
                let current_property_id = interaction.id();
                loop {
                    state.interaction_pointer += 1;
                    let Some(new_interaction) = plan.next(&mut env) else {
                        // No more interactions, we're done
                        return ExecutionResult::new(history, None);
                    };
                    if new_interaction.id() != current_property_id {
                        interaction = new_interaction;
                        break;
                    }
                }
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
    NextInteractionOutsideThisProperty,
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

#[instrument(skip(env, interaction, stack), fields(conn_index = interaction.connection_index, interaction = %interaction))]
pub fn execute_interaction_turso(
    env: &mut SimulatorEnv,
    interaction: &Interaction,
    stack: &mut Vec<ResultSet>,
) -> Result<ExecutionContinuation> {
    let connection_index = interaction.connection_index;
    // Leave this empty info! here to print the span of the execution
    tracing::info!("");
    match &interaction.interaction {
        InteractionType::Query(query) => {
            tracing::debug!(?interaction);

            let results = {
                let SimConnection::LimboConnection(conn) = &mut env.connections[connection_index]
                else {
                    unreachable!()
                };
                interaction
                    .execute_query(conn)
                    .inspect_err(|err| tracing::error!(?err))
            };

            // Handle errors: skip shadow on any failed query
            // - Without ignore_error: recoverable errors skip property, others fail simulation
            // - With ignore_error: skip shadow but continue executing the property
            let (skip_shadow, skip_property) = if let Err(err) = &results {
                if interaction.ignore_error {
                    (true, false) // query failed but ignore_error is set: skip shadow, continue property
                } else if is_recoverable_tx_error(err) {
                    // Only rollback shadow state if the DB actually rolled back
                    if error_causes_rollback(err) && env.conn_in_transaction(connection_index) {
                        env.rollback_conn(connection_index);
                    }
                    (true, true) // skip shadow and skip rest of property
                } else {
                    return Err(err.clone());
                }
            } else {
                (false, false) // success shadow normally, continue property
            };

            stack.push(results);
            // TODO: skip integrity check with mvcc
            if !env.profile.experimental_mvcc && env.rng.random_ratio(1, 10) {
                let SimConnection::LimboConnection(conn) = &mut env.connections[connection_index]
                else {
                    unreachable!()
                };
                limbo_integrity_check(conn)?;
            }
            env.update_conn_last_interaction(connection_index, Some(query));

            if skip_shadow {
                if skip_property {
                    return Ok(ExecutionContinuation::NextInteractionOutsideThisProperty);
                } else {
                    // Skip shadow but continue to next interaction in this property
                    return Ok(ExecutionContinuation::NextInteraction);
                }
            }
        }
        InteractionType::FsyncQuery(query) => {
            let conn = {
                let SimConnection::LimboConnection(conn) = &mut env.connections[connection_index]
                else {
                    unreachable!()
                };
                conn.clone()
            };
            let results = interaction
                .execute_fsync_query(conn, env)
                .inspect_err(|err| tracing::error!(?err));

            stack.push(results);

            let query_interaction = InteractionBuilder::from_interaction(interaction)
                .interaction(InteractionType::Query(query.clone()))
                .build()
                .unwrap();

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
            interaction.execute_fault(env, connection_index)?;
        }
        InteractionType::FaultyQuery(_) => {
            let conn = {
                let SimConnection::LimboConnection(conn) = &mut env.connections[connection_index]
                else {
                    unreachable!()
                };
                conn.clone()
            };
            let results = interaction
                .execute_faulty_query(&conn, env)
                .inspect_err(|err| tracing::error!(?err));

            stack.push(results);
            // Reset fault injection
            env.io.inject_fault(false);
            // TODO: skip integrity check with mvcc
            if !env.profile.experimental_mvcc && env.rng.random_ratio(1, 10) {
                limbo_integrity_check(&conn)?;
            }
        }
    }
    let _ = interaction.shadow(&mut env.get_conn_tables_mut(connection_index));
    Ok(ExecutionContinuation::NextInteraction)
}

fn limbo_integrity_check(conn: &Arc<Connection>) -> Result<()> {
    let mut rows = conn.query("PRAGMA integrity_check;")?.unwrap();
    let mut result = Vec::new();

    rows.run_with_row_callback(|row| {
        let val = match row.get_value(0) {
            turso_core::Value::Text(text) => text.as_str().to_string(),
            _ => unreachable!(),
        };
        result.push(val);
        Ok(())
    })?;

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

#[instrument(skip(env, interaction, stack), fields(conn_index = interaction.connection_index, interaction = %interaction))]
fn execute_interaction_rusqlite(
    env: &mut SimulatorEnv,
    interaction: &Interaction,
    stack: &mut Vec<ResultSet>,
) -> turso_core::Result<ExecutionContinuation> {
    tracing::info!("");
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

            // Skip shadow on any failed query, if ignore_error is set, continue property
            if let Err(err) = &results {
                let err_clone = err.clone();
                stack.push(results);
                if interaction.ignore_error {
                    // Skip shadow but continue property
                    return Ok(ExecutionContinuation::NextInteraction);
                } else {
                    return Err(err_clone);
                }
            }

            tracing::debug!("{:?}", results);
            stack.push(results);
            env.update_conn_last_interaction(interaction.connection_index, Some(query));
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
    // https://sqlite.org/forum/forumpost/9fe5d047f0
    // Due to a bug in sqlite, we need to execute this query to clear the internal stmt cache so that schema changes become visible always to other connections
    connection.query_one("SELECT * FROM pragma_user_version()", (), |_| Ok(()))?;
    match query {
        Query::Select(select) => {
            let mut stmt = connection.prepare(select.to_string().as_str())?;
            let rows = stmt.query_map([], |row| {
                let mut values = vec![];
                for i in 0.. {
                    let value = match row.get(i) {
                        Ok(value) => value,
                        Err(err) => match err {
                            rusqlite::Error::InvalidColumnIndex(_) => break,
                            _ => {
                                tracing::error!(?err);
                                panic!("{err}")
                            }
                        },
                    };
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
        Query::Placeholder => {
            unreachable!("simulation cannot have a placeholder Query for execution")
        }
        _ => {
            connection.execute(query.to_string().as_str(), ())?;
            Ok(vec![])
        }
    }
}
