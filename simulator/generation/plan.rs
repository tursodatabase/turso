use std::{
    collections::HashSet,
    fmt::{Debug, Display},
    path::Path,
    sync::Arc,
    vec,
};

use serde::{Deserialize, Serialize};
use turso_core::{Connection, Result, StepResult};

use crate::{
    model::{
        query::{update::Update, Create, CreateIndex, Delete, Drop, Insert, Query, Select},
        table::SimValue,
    },
    runner::env::SimConnection,
    SimulatorEnv,
};

use crate::generation::{frequency, Arbitrary, ArbitraryFrom};

use super::property::{remaining, Property};

pub(crate) type ResultSet = Result<Vec<Vec<SimValue>>>;

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct InteractionPlan {
    pub(crate) plan: Vec<Interactions>,
}

impl InteractionPlan {
    /// Compute via diff computes a a plan from a given `.plan` file without the need to parse
    /// sql. This is possible because there are two versions of the plan file, one that is human
    /// readable and one that is serialized as JSON. Under watch mode, the users will be able to
    /// delete interactions from the human readable file, and this function uses the JSON file as
    /// a baseline to detect with interactions were deleted and constructs the plan from the
    /// remaining interactions.
    pub(crate) fn compute_via_diff(plan_path: &Path) -> Vec<Vec<Interaction>> {
        let interactions = std::fs::read_to_string(plan_path).unwrap();
        let interactions = interactions.lines().collect::<Vec<_>>();

        let plan: InteractionPlan = serde_json::from_str(
            std::fs::read_to_string(plan_path.with_extension("json"))
                .unwrap()
                .as_str(),
        )
        .unwrap();

        let mut plan = plan
            .plan
            .into_iter()
            .map(|i| i.interactions())
            .collect::<Vec<_>>();

        let (mut i, mut j) = (0, 0);

        while i < interactions.len() && j < plan.len() {
            if interactions[i].starts_with("-- begin")
                || interactions[i].starts_with("-- end")
                || interactions[i].is_empty()
            {
                i += 1;
                continue;
            }

            // interactions[i] is the i'th line in the human readable plan
            // plan[j][k] is the k'th interaction in the j'th property
            let mut k = 0;

            while k < plan[j].len() {
                if i >= interactions.len() {
                    let _ = plan.split_off(j + 1);
                    let _ = plan[j].split_off(k);
                    break;
                }
                if interactions[i].contains(plan[j][k].to_string().as_str()) {
                    i += 1;
                    k += 1;
                } else {
                    plan[j].remove(k);
                }
            }

            if plan[j].is_empty() {
                plan.remove(j);
            } else {
                j += 1;
            }
        }
        let _ = plan.split_off(j);
        plan
    }
}

pub(crate) struct InteractionPlanState {
    pub(crate) stack: Vec<ResultSet>,
    pub(crate) interaction_pointer: usize,
    pub(crate) secondary_pointer: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum Interactions {
    Property(Property),
    Query(Query),
    Fault(Fault),
}

impl Interactions {
    pub(crate) fn name(&self) -> Option<&str> {
        match self {
            Interactions::Property(property) => Some(property.name()),
            Interactions::Query(_) => None,
            Interactions::Fault(_) => None,
        }
    }

    pub(crate) fn interactions(&self) -> Vec<Interaction> {
        match self {
            Interactions::Property(property) => property.interactions(),
            Interactions::Query(query) => vec![Interaction::Query(query.clone())],
            Interactions::Fault(fault) => vec![Interaction::Fault(fault.clone())],
        }
    }
}

impl Interactions {
    pub(crate) fn dependencies(&self) -> HashSet<String> {
        match self {
            Interactions::Property(property) => {
                property
                    .interactions()
                    .iter()
                    .fold(HashSet::new(), |mut acc, i| match i {
                        Interaction::Query(q) => {
                            acc.extend(q.dependencies());
                            acc
                        }
                        _ => acc,
                    })
            }
            Interactions::Query(query) => query.dependencies(),
            Interactions::Fault(_) => HashSet::new(),
        }
    }

    pub(crate) fn uses(&self) -> Vec<String> {
        match self {
            Interactions::Property(property) => {
                property
                    .interactions()
                    .iter()
                    .fold(vec![], |mut acc, i| match i {
                        Interaction::Query(q) => {
                            acc.extend(q.uses());
                            acc
                        }
                        _ => acc,
                    })
            }
            Interactions::Query(query) => query.uses(),
            Interactions::Fault(_) => vec![],
        }
    }
}

impl Display for InteractionPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for interactions in &self.plan {
            match interactions {
                Interactions::Property(property) => {
                    let name = property.name();
                    writeln!(f, "-- begin testing '{}'", name)?;
                    for interaction in property.interactions() {
                        write!(f, "\t")?;

                        match interaction {
                            Interaction::Query(query) => writeln!(f, "{};", query)?,
                            Interaction::Assumption(assumption) => {
                                writeln!(f, "-- ASSUME {};", assumption.message)?
                            }
                            Interaction::Assertion(assertion) => {
                                writeln!(f, "-- ASSERT {};", assertion.message)?
                            }
                            Interaction::Fault(fault) => writeln!(f, "-- FAULT '{}';", fault)?,
                            Interaction::FsyncQuery(query) => {
                                writeln!(f, "-- FSYNC QUERY;")?;
                                writeln!(f, "{};", query)?;
                                writeln!(f, "{};", query)?
                            }
                            Interaction::FaultyQuery(query) => {
                                writeln!(f, "{}; --FAULTY QUERY", query)?
                            }
                        }
                    }
                    writeln!(f, "-- end testing '{}'", name)?;
                }
                Interactions::Fault(fault) => {
                    writeln!(f, "-- FAULT '{}'", fault)?;
                }
                Interactions::Query(query) => {
                    writeln!(f, "{};", query)?;
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct InteractionStats {
    pub(crate) read_count: usize,
    pub(crate) write_count: usize,
    pub(crate) delete_count: usize,
    pub(crate) update_count: usize,
    pub(crate) create_count: usize,
    pub(crate) create_index_count: usize,
    pub(crate) drop_count: usize,
    pub(crate) begin_count: usize,
    pub(crate) commit_count: usize,
    pub(crate) rollback_count: usize,
}

impl Display for InteractionStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Read: {}, Write: {}, Delete: {}, Update: {}, Create: {}, CreateIndex: {}, Drop: {}, Begin: {}, Commit: {}, Rollback: {}",
            self.read_count,
            self.write_count,
            self.delete_count,
            self.update_count,
            self.create_count,
            self.create_index_count,
            self.drop_count,
            self.begin_count,
            self.commit_count,
            self.rollback_count,
        )
    }
}

#[derive(Debug)]
pub(crate) enum Interaction {
    Query(Query),
    Assumption(Assertion),
    Assertion(Assertion),
    Fault(Fault),
    /// Will attempt to run any random query. However, when the connection tries to sync it will
    /// close all connections and reopen the database and assert that no data was lost
    FsyncQuery(Query),
    FaultyQuery(Query),
}

impl Display for Interaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Query(query) => write!(f, "{}", query),
            Self::Assumption(assumption) => write!(f, "ASSUME {}", assumption.message),
            Self::Assertion(assertion) => write!(f, "ASSERT {}", assertion.message),
            Self::Fault(fault) => write!(f, "FAULT '{}'", fault),
            Self::FsyncQuery(query) => write!(f, "{}", query),
            Self::FaultyQuery(query) => write!(f, "{} -- FAULTY QUERY", query),
        }
    }
}

type AssertionFunc = dyn Fn(&Vec<ResultSet>, &mut SimulatorEnv) -> Result<bool>;

enum AssertionAST {
    Pick(),
}

pub(crate) struct Assertion {
    pub(crate) func: Box<AssertionFunc>,
    pub(crate) message: String,
}

impl Debug for Assertion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Assertion")
            .field("message", &self.message)
            .finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum Fault {
    Disconnect,
    ReopenDatabase,
}

impl Display for Fault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Fault::Disconnect => write!(f, "DISCONNECT"),
            Fault::ReopenDatabase => write!(f, "REOPEN_DATABASE"),
        }
    }
}

impl InteractionPlan {
    pub(crate) fn new() -> Self {
        Self { plan: Vec::new() }
    }

    pub(crate) fn stats(&self) -> InteractionStats {
        let mut read = 0;
        let mut write = 0;
        let mut delete = 0;
        let mut create = 0;
        let mut drop = 0;
        let mut update = 0;
        let mut create_index = 0;
        let mut begin = 0;
        let mut commit = 0;
        let mut rollback = 0;

        for interactions in &self.plan {
            match interactions {
                Interactions::Property(property) => {
                    for interaction in &property.interactions() {
                        if let Interaction::Query(query) = interaction {
                            match query {
                                Query::Select(_) => read += 1,
                                Query::Insert(_) => write += 1,
                                Query::Delete(_) => delete += 1,
                                Query::Create(_) => create += 1,
                                Query::Drop(_) => drop += 1,
                                Query::Update(_) => update += 1,
                                Query::CreateIndex(_) => create_index += 1,
                                Query::Begin(_) => begin += 1,
                                Query::Commit(_) => commit += 1,
                                Query::Rollback(_) => rollback += 1,
                            }
                        }
                    }
                }
                Interactions::Query(query) => match query {
                    Query::Select(_) => read += 1,
                    Query::Insert(_) => write += 1,
                    Query::Delete(_) => delete += 1,
                    Query::Create(_) => create += 1,
                    Query::Drop(_) => drop += 1,
                    Query::Update(_) => update += 1,
                    Query::CreateIndex(_) => create_index += 1,
                    Query::Begin(_) => begin += 1,
                    Query::Commit(_) => commit += 1,
                    Query::Rollback(_) => rollback += 1,
                },
                Interactions::Fault(_) => {}
            }
        }

        InteractionStats {
            read_count: read,
            write_count: write,
            delete_count: delete,
            update_count: update,
            create_count: create,
            create_index_count: create_index,
            drop_count: drop,
            begin_count: begin,
            commit_count: commit,
            rollback_count: rollback,
        }
    }
}

impl ArbitraryFrom<&mut SimulatorEnv> for InteractionPlan {
    fn arbitrary_from<R: rand::Rng>(rng: &mut R, env: &mut SimulatorEnv) -> Self {
        let mut plan = InteractionPlan::new();

        let num_interactions = env.opts.max_interactions;

        // First create at least one table
        let create_query = Create::arbitrary(rng);
        env.tables.push(create_query.table.clone());

        plan.plan
            .push(Interactions::Query(Query::Create(create_query)));

        while plan.plan.len() < num_interactions {
            tracing::debug!(
                "Generating interaction {}/{}",
                plan.plan.len(),
                num_interactions
            );
            let interactions = Interactions::arbitrary_from(rng, (env, plan.stats()));

            plan.plan.push(interactions);
        }

        tracing::info!("Generated plan with {} interactions", plan.plan.len());
        plan
    }
}

impl Interaction {
    pub(crate) fn shadow(&self, env: &mut SimulatorEnv) -> Vec<Vec<SimValue>> {
        match self {
            Self::Query(query) => query.shadow(env),
            Self::FsyncQuery(query) => {
                let mut first = query.shadow(env);
                first.extend(query.shadow(env));
                first
            }
            Self::Assumption(_) | Self::Assertion(_) | Self::Fault(_) | Self::FaultyQuery(_) => {
                vec![]
            }
        }
    }
    pub(crate) fn execute_query(&self, conn: &mut Arc<Connection>) -> ResultSet {
        if let Self::Query(query) = self {
            let query_str = query.to_string();
            let rows = conn.query(&query_str);
            if rows.is_err() {
                let err = rows.err();
                tracing::debug!(
                    "Error running query '{}': {:?}",
                    &query_str[0..query_str.len().min(4096)],
                    err
                );
                return Err(err.unwrap());
            }
            let rows = rows?;
            assert!(rows.is_some());
            let mut rows = rows.unwrap();
            let mut out = Vec::new();
            while let Ok(row) = rows.step() {
                match row {
                    StepResult::Row => {
                        let row = rows.row().unwrap();
                        let mut r = Vec::new();
                        for v in row.get_values() {
                            let v = v.into();
                            r.push(v);
                        }
                        out.push(r);
                    }
                    StepResult::IO => {
                        rows.run_once().unwrap();
                    }
                    StepResult::Interrupt => {}
                    StepResult::Done => {
                        break;
                    }
                    StepResult::Busy => {
                        return Err(turso_core::LimboError::Busy);
                    }
                }
            }

            Ok(out)
        } else {
            unreachable!("unexpected: this function should only be called on queries")
        }
    }

    pub(crate) fn execute_assertion(
        &self,
        stack: &Vec<ResultSet>,
        env: &mut SimulatorEnv,
    ) -> Result<()> {
        match self {
            Self::Assertion(assertion) => {
                let result = assertion.func.as_ref()(stack, env);
                match result {
                    Ok(true) => Ok(()),
                    Ok(false) => Err(turso_core::LimboError::InternalError(
                        assertion.message.clone(),
                    )),
                    Err(err) => Err(turso_core::LimboError::InternalError(format!(
                        "{}. Inner error: {}",
                        assertion.message, err
                    ))),
                }
            }
            _ => {
                unreachable!("unexpected: this function should only be called on assertions")
            }
        }
    }

    pub(crate) fn execute_assumption(
        &self,
        stack: &Vec<ResultSet>,
        env: &mut SimulatorEnv,
    ) -> Result<()> {
        match self {
            Self::Assumption(assumption) => {
                let result = assumption.func.as_ref()(stack, env);
                match result {
                    Ok(true) => Ok(()),
                    Ok(false) => Err(turso_core::LimboError::InternalError(
                        assumption.message.clone(),
                    )),
                    Err(err) => Err(turso_core::LimboError::InternalError(format!(
                        "{}. Inner error: {}",
                        assumption.message, err
                    ))),
                }
            }
            _ => {
                unreachable!("unexpected: this function should only be called on assumptions")
            }
        }
    }

    pub(crate) fn execute_fault(&self, env: &mut SimulatorEnv, conn_index: usize) -> Result<()> {
        match self {
            Self::Fault(fault) => {
                match fault {
                    Fault::Disconnect => {
                        if env.connections[conn_index].is_connected() {
                            env.connections[conn_index].disconnect();
                        } else {
                            return Err(turso_core::LimboError::InternalError(
                                "connection already disconnected".into(),
                            ));
                        }
                        env.connections[conn_index] = SimConnection::Disconnected;
                    }
                    Fault::ReopenDatabase => {
                        reopen_database(env);
                    }
                }
                Ok(())
            }
            _ => {
                unreachable!("unexpected: this function should only be called on faults")
            }
        }
    }

    pub(crate) fn execute_fsync_query(
        &self,
        conn: Arc<Connection>,
        env: &mut SimulatorEnv,
    ) -> ResultSet {
        if let Self::FsyncQuery(query) = self {
            let query_str = query.to_string();
            let rows = conn.query(&query_str);
            if rows.is_err() {
                let err = rows.err();
                tracing::debug!(
                    "Error running query '{}': {:?}",
                    &query_str[0..query_str.len().min(4096)],
                    err
                );
                return Err(err.unwrap());
            }
            let mut rows = rows.unwrap().unwrap();
            let mut out = Vec::new();
            while let Ok(row) = rows.step() {
                match row {
                    StepResult::Row => {
                        let row = rows.row().unwrap();
                        let mut r = Vec::new();
                        for v in row.get_values() {
                            let v = v.into();
                            r.push(v);
                        }
                        out.push(r);
                    }
                    StepResult::IO => {
                        let syncing = {
                            let files = env.io.files.borrow();
                            // TODO: currently assuming we only have 1 file that is syncing
                            files
                                .iter()
                                .any(|file| file.sync_completion.borrow().is_some())
                        };
                        if syncing {
                            reopen_database(env);
                        } else {
                            rows.run_once().unwrap();
                        }
                    }
                    StepResult::Done => {
                        break;
                    }
                    StepResult::Busy => {
                        return Err(turso_core::LimboError::Busy);
                    }
                    StepResult::Interrupt => {}
                }
            }

            Ok(out)
        } else {
            unreachable!("unexpected: this function should only be called on queries")
        }
    }

    pub(crate) fn execute_faulty_query(
        &self,
        conn: &Arc<Connection>,
        env: &mut SimulatorEnv,
    ) -> ResultSet {
        use rand::Rng;
        if let Self::FaultyQuery(query) = self {
            let query_str = query.to_string();
            let rows = conn.query(&query_str);
            if rows.is_err() {
                let err = rows.err();
                tracing::debug!(
                    "Error running query '{}': {:?}",
                    &query_str[0..query_str.len().min(4096)],
                    err
                );
                return Err(err.unwrap());
            }
            let mut rows = rows.unwrap().unwrap();
            let mut out = Vec::new();
            let mut current_prob = 0.05;
            let mut incr = 0.001;
            loop {
                let syncing = {
                    let files = env.io.files.borrow();
                    // TODO: currently assuming we only have 1 file that is syncing
                    files
                        .iter()
                        .any(|file| file.sync_completion.borrow().is_some())
                };
                let inject_fault = env.rng.gen_bool(current_prob);
                if inject_fault || syncing {
                    env.io.inject_fault(true);
                }

                match rows.step()? {
                    StepResult::Row => {
                        let row = rows.row().unwrap();
                        let mut r = Vec::new();
                        for v in row.get_values() {
                            let v = v.into();
                            r.push(v);
                        }
                        out.push(r);
                    }
                    StepResult::IO => {
                        rows.run_once()?;
                        current_prob += incr;
                        if current_prob > 1.0 {
                            current_prob = 1.0;
                        } else {
                            incr *= 1.01;
                        }
                    }
                    StepResult::Done => {
                        break;
                    }
                    StepResult::Busy => {
                        return Err(turso_core::LimboError::Busy);
                    }
                    StepResult::Interrupt => {}
                }
            }

            Ok(out)
        } else {
            unreachable!("unexpected: this function should only be called on queries")
        }
    }
}

fn reopen_database(env: &mut SimulatorEnv) {
    // 1. Close all connections without default checkpoint-on-close behavior
    // to expose bugs related to how we handle WAL
    let num_conns = env.connections.len();
    env.connections.clear();

    // Clear all open files
    // TODO: for correct reporting of faults we should get all the recorded numbers and transfer to the new file
    env.io.files.borrow_mut().clear();

    // 2. Re-open database
    let db_path = env.db_path.clone();
    let db = match turso_core::Database::open_file(
        env.io.clone(),
        &db_path,
        env.opts.experimental_mvcc,
        env.opts.experimental_indexes,
    ) {
        Ok(db) => db,
        Err(e) => {
            panic!("error opening simulator test file {:?}: {:?}", db_path, e);
        }
    };
    env.db = db;

    for _ in 0..num_conns {
        env.connections
            .push(SimConnection::LimboConnection(env.db.connect().unwrap()));
    }
}

fn random_create<R: rand::Rng>(rng: &mut R, _env: &SimulatorEnv) -> Interactions {
    Interactions::Query(Query::Create(Create::arbitrary(rng)))
}

fn random_read<R: rand::Rng>(rng: &mut R, env: &SimulatorEnv) -> Interactions {
    Interactions::Query(Query::Select(Select::arbitrary_from(rng, env)))
}

fn random_write<R: rand::Rng>(rng: &mut R, env: &SimulatorEnv) -> Interactions {
    Interactions::Query(Query::Insert(Insert::arbitrary_from(rng, env)))
}

fn random_delete<R: rand::Rng>(rng: &mut R, env: &SimulatorEnv) -> Interactions {
    Interactions::Query(Query::Delete(Delete::arbitrary_from(rng, env)))
}

fn random_update<R: rand::Rng>(rng: &mut R, env: &SimulatorEnv) -> Interactions {
    Interactions::Query(Query::Update(Update::arbitrary_from(rng, env)))
}

fn random_drop<R: rand::Rng>(rng: &mut R, env: &SimulatorEnv) -> Interactions {
    Interactions::Query(Query::Drop(Drop::arbitrary_from(rng, env)))
}

fn random_create_index<R: rand::Rng>(rng: &mut R, env: &SimulatorEnv) -> Option<Interactions> {
    if env.tables.is_empty() {
        return None;
    }
    Some(Interactions::Query(Query::CreateIndex(
        CreateIndex::arbitrary_from(rng, env),
    )))
}

fn random_fault<R: rand::Rng>(rng: &mut R, env: &SimulatorEnv) -> Interactions {
    let faults = if env.opts.disable_reopen_database {
        vec![Fault::Disconnect]
    } else {
        vec![Fault::Disconnect, Fault::ReopenDatabase]
    };
    let fault = faults[rng.gen_range(0..faults.len())].clone();
    Interactions::Fault(fault)
}

impl ArbitraryFrom<(&SimulatorEnv, InteractionStats)> for Interactions {
    fn arbitrary_from<R: rand::Rng>(
        rng: &mut R,
        (env, stats): (&SimulatorEnv, InteractionStats),
    ) -> Self {
        let remaining_ = remaining(env, &stats);
        frequency(
            vec![
                (
                    f64::min(remaining_.read, remaining_.write) + remaining_.create,
                    Box::new(|rng: &mut R| {
                        Interactions::Property(Property::arbitrary_from(rng, (env, &stats)))
                    }),
                ),
                (
                    remaining_.read,
                    Box::new(|rng: &mut R| random_read(rng, env)),
                ),
                (
                    remaining_.write,
                    Box::new(|rng: &mut R| random_write(rng, env)),
                ),
                (
                    remaining_.create,
                    Box::new(|rng: &mut R| random_create(rng, env)),
                ),
                (
                    remaining_.create_index,
                    Box::new(|rng: &mut R| {
                        if let Some(interaction) = random_create_index(rng, env) {
                            interaction
                        } else {
                            // if no tables exist, we can't create an index, so fallback to creating a table
                            random_create(rng, env)
                        }
                    }),
                ),
                (
                    remaining_.delete,
                    Box::new(|rng: &mut R| random_delete(rng, env)),
                ),
                (
                    remaining_.update,
                    Box::new(|rng: &mut R| random_update(rng, env)),
                ),
                (
                    // remaining_.drop,
                    0.0,
                    Box::new(|rng: &mut R| random_drop(rng, env)),
                ),
                (
                    remaining_
                        .read
                        .min(remaining_.write)
                        .min(remaining_.create)
                        .max(1.0),
                    Box::new(|rng: &mut R| random_fault(rng, env)),
                ),
            ],
            rng,
        )
    }
}
