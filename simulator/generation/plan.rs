use std::{
    fmt::{Debug, Display},
    ops::{Deref, DerefMut},
    path::Path,
    rc::Rc,
    sync::Arc,
    vec,
};

use indexmap::IndexSet;
use serde::{Deserialize, Serialize};

use sql_generation::{
    generation::{Arbitrary, ArbitraryFrom, GenerationContext, frequency, query::SelectFree},
    model::{
        query::{
            Create, CreateIndex, Delete, Drop, Insert, Select,
            transaction::{Begin, Commit},
            update::Update,
        },
        table::SimValue,
    },
};
use turso_core::{Connection, Result, StepResult};

use crate::{
    SimulatorEnv,
    generation::Shadow,
    model::Query,
    runner::env::{ShadowTablesMut, SimConnection, SimulationType},
};

use super::property::{Property, remaining};

pub(crate) type ResultSet = Result<Vec<Vec<SimValue>>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct InteractionPlan {
    pub plan: Vec<Interactions>,
    pub mvcc: bool,
}

impl InteractionPlan {
    pub(crate) fn new(mvcc: bool) -> Self {
        Self {
            plan: Vec::new(),
            mvcc,
        }
    }

    pub fn new_with(plan: Vec<Interactions>, mvcc: bool) -> Self {
        Self { plan, mvcc }
    }

    #[inline]
    pub fn plan(&self) -> &[Interactions] {
        &self.plan
    }

    // TODO: this is just simplified logic so we can get something rolling with begin concurrent
    // transactions in the simulator. Ideally when we generate the plan we will have begin and commits statements across interactions
    pub fn push(&mut self, interactions: Interactions) {
        if self.mvcc {
            let conn_index = interactions.connection_index;
            let begin = Interactions::new(
                conn_index,
                InteractionsType::Query(Query::Begin(Begin::Concurrent)),
            );
            let commit =
                Interactions::new(conn_index, InteractionsType::Query(Query::Commit(Commit)));
            self.plan.push(begin);
            self.plan.push(interactions);
            self.plan.push(commit);
        } else {
            self.plan.push(interactions);
        }
    }

    /// Compute via diff computes a a plan from a given `.plan` file without the need to parse
    /// sql. This is possible because there are two versions of the plan file, one that is human
    /// readable and one that is serialized as JSON. Under watch mode, the users will be able to
    /// delete interactions from the human readable file, and this function uses the JSON file as
    /// a baseline to detect with interactions were deleted and constructs the plan from the
    /// remaining interactions.
    pub(crate) fn compute_via_diff(plan_path: &Path) -> impl InteractionPlanIterator {
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
                log::error!("Comparing '{}' with '{}'", interactions[i], plan[j][k]);
                if interactions[i].contains(plan[j][k].to_string().as_str()) {
                    i += 1;
                    k += 1;
                } else {
                    plan[j].remove(k);
                    panic!("Comparing '{}' with '{}'", interactions[i], plan[j][k]);
                }
            }

            if plan[j].is_empty() {
                plan.remove(j);
            } else {
                j += 1;
            }
        }
        let _ = plan.split_off(j);
        PlanIterator {
            iter: plan.into_iter().flatten(),
        }
    }

    pub fn interactions_list(&self) -> Vec<Interaction> {
        self.plan
            .clone()
            .into_iter()
            .flat_map(|interactions| interactions.interactions().into_iter())
            .collect()
    }

    pub fn interactions_list_with_secondary_index(&self) -> Vec<(usize, Interaction)> {
        self.plan
            .clone()
            .into_iter()
            .enumerate()
            .flat_map(|(idx, interactions)| {
                interactions
                    .interactions()
                    .into_iter()
                    .map(move |interaction| (idx, interaction))
            })
            .collect()
    }

    pub(crate) fn stats(&self) -> InteractionStats {
        let mut stats = InteractionStats {
            select_count: 0,
            insert_count: 0,
            delete_count: 0,
            update_count: 0,
            create_count: 0,
            create_index_count: 0,
            drop_count: 0,
            begin_count: 0,
            commit_count: 0,
            rollback_count: 0,
        };

        fn query_stat(q: &Query, stats: &mut InteractionStats) {
            match q {
                Query::Select(_) => stats.select_count += 1,
                Query::Insert(_) => stats.insert_count += 1,
                Query::Delete(_) => stats.delete_count += 1,
                Query::Create(_) => stats.create_count += 1,
                Query::Drop(_) => stats.drop_count += 1,
                Query::Update(_) => stats.update_count += 1,
                Query::CreateIndex(_) => stats.create_index_count += 1,
                Query::Begin(_) => stats.begin_count += 1,
                Query::Commit(_) => stats.commit_count += 1,
                Query::Rollback(_) => stats.rollback_count += 1,
            }
        }
        for interactions in &self.plan {
            match &interactions.interactions {
                InteractionsType::Property(property) => {
                    for interaction in &property.interactions(interactions.connection_index) {
                        if let InteractionType::Query(query) = &interaction.interaction {
                            query_stat(query, &mut stats);
                        }
                    }
                }
                InteractionsType::Query(query) => {
                    query_stat(query, &mut stats);
                }
                InteractionsType::Fault(_) => {}
            }
        }

        stats
    }

    pub fn init_plan(env: &mut SimulatorEnv) -> Self {
        let mut plan = InteractionPlan::new(env.profile.experimental_mvcc);

        // First create at least one table
        let create_query = Create::arbitrary(&mut env.rng.clone(), &env.connection_context(0));

        // initial query starts at 0th connection
        plan.plan.push(Interactions::new(
            0,
            InteractionsType::Query(Query::Create(create_query)),
        ));

        plan
    }

    /// Appends a new [Interactions] and outputs the next set of [Interaction] to take
    pub fn generate_next_interaction(
        &mut self,
        rng: &mut impl rand::Rng,
        env: &mut SimulatorEnv,
    ) -> Option<Vec<Interaction>> {
        let num_interactions = env.opts.max_interactions as usize;
        if self.len() < num_interactions {
            tracing::debug!("Generating interaction {}/{}", self.len(), num_interactions);
            let interactions = {
                let conn_index = env.choose_conn(rng);
                let conn_ctx = &env.connection_context(conn_index);
                Interactions::arbitrary_from(rng, conn_ctx, (env, self.stats(), conn_index))
            };

            let out_interactions = interactions.interactions();
            assert!(!out_interactions.is_empty());
            self.push(interactions);
            Some(out_interactions)
        } else {
            None
        }
    }

    pub fn generator<'a>(
        &'a mut self,
        rng: &'a mut impl rand::Rng,
    ) -> impl InteractionPlanIterator {
        let interactions = self.interactions_list();
        let iter = interactions.into_iter();
        PlanGenerator {
            plan: self,
            iter,
            rng,
        }
    }

    pub fn static_iterator(&self) -> impl InteractionPlanIterator {
        PlanIterator {
            iter: self.interactions_list().into_iter(),
        }
    }
}

impl Deref for InteractionPlan {
    type Target = [Interactions];

    fn deref(&self) -> &Self::Target {
        &self.plan
    }
}

impl DerefMut for InteractionPlan {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.plan
    }
}

impl IntoIterator for InteractionPlan {
    type Item = Interactions;

    type IntoIter = <Vec<Interactions> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.plan.into_iter()
    }
}

pub trait InteractionPlanIterator {
    fn next(&mut self, env: &mut SimulatorEnv) -> Option<Interaction>;
}

impl<T: InteractionPlanIterator> InteractionPlanIterator for &mut T {
    fn next(&mut self, env: &mut SimulatorEnv) -> Option<Interaction> {
        T::next(self, env)
    }
}

pub struct PlanGenerator<'a, R: rand::Rng> {
    plan: &'a mut InteractionPlan,
    iter: <Vec<Interaction> as IntoIterator>::IntoIter,
    rng: &'a mut R,
}

impl<'a, R: rand::Rng> InteractionPlanIterator for PlanGenerator<'a, R> {
    /// try to generate the next [Interactions] and store it
    fn next(&mut self, env: &mut SimulatorEnv) -> Option<Interaction> {
        self.iter.next().or_else(|| {
            // Iterator ended, try to create a new iterator
            // This will not be an infinte sequence because generate_next_interaction will eventually
            // stop generating
            let mut iter = self
                .plan
                .generate_next_interaction(self.rng, env)
                .map_or(Vec::new().into_iter(), |interactions| {
                    interactions.into_iter()
                });
            let next = iter.next();
            self.iter = iter;

            next
        })
    }
}

pub struct PlanIterator<I: Iterator<Item = Interaction>> {
    iter: I,
}

impl<I> InteractionPlanIterator for PlanIterator<I>
where
    I: Iterator<Item = Interaction>,
{
    fn next(&mut self, _env: &mut SimulatorEnv) -> Option<Interaction> {
        self.iter.next()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct InteractionPlanState {
    pub interaction_pointer: usize,
}

#[derive(Debug, Default, Clone)]
pub struct ConnectionState {
    pub stack: Vec<ResultSet>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Interactions {
    pub connection_index: usize,
    pub interactions: InteractionsType,
}

impl Interactions {
    pub fn new(connection_index: usize, interactions: InteractionsType) -> Self {
        Self {
            connection_index,
            interactions,
        }
    }
}

impl Deref for Interactions {
    type Target = InteractionsType;

    fn deref(&self) -> &Self::Target {
        &self.interactions
    }
}

impl DerefMut for Interactions {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.interactions
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InteractionsType {
    Property(Property),
    Query(Query),
    Fault(Fault),
}

impl Shadow for Interactions {
    type Result = ();

    fn shadow(&self, tables: &mut ShadowTablesMut) {
        match &self.interactions {
            InteractionsType::Property(property) => {
                let initial_tables = tables.clone();
                for interaction in property.interactions(self.connection_index) {
                    let res = interaction.shadow(tables);
                    if res.is_err() {
                        // If any interaction fails, we reset the tables to the initial state
                        **tables = initial_tables.clone();
                        break;
                    }
                }
            }
            InteractionsType::Query(query) => {
                let _ = query.shadow(tables);
            }
            InteractionsType::Fault(_) => {}
        }
    }
}

impl Interactions {
    pub(crate) fn name(&self) -> Option<&str> {
        match &self.interactions {
            InteractionsType::Property(property) => Some(property.name()),
            InteractionsType::Query(_) => None,
            InteractionsType::Fault(_) => None,
        }
    }

    pub(crate) fn interactions(&self) -> Vec<Interaction> {
        match &self.interactions {
            InteractionsType::Property(property) => property.interactions(self.connection_index),
            InteractionsType::Query(query) => vec![Interaction::new(
                self.connection_index,
                InteractionType::Query(query.clone()),
            )],
            InteractionsType::Fault(fault) => vec![Interaction::new(
                self.connection_index,
                InteractionType::Fault(*fault),
            )],
        }
    }

    pub(crate) fn dependencies(&self) -> IndexSet<String> {
        match &self.interactions {
            InteractionsType::Property(property) => property
                .interactions(self.connection_index)
                .iter()
                .fold(IndexSet::new(), |mut acc, i| match &i.interaction {
                    InteractionType::Query(q) => {
                        acc.extend(q.dependencies());
                        acc
                    }
                    _ => acc,
                }),
            InteractionsType::Query(query) => query.dependencies(),
            InteractionsType::Fault(_) => IndexSet::new(),
        }
    }

    pub(crate) fn uses(&self) -> Vec<String> {
        match &self.interactions {
            InteractionsType::Property(property) => property
                .interactions(self.connection_index)
                .iter()
                .fold(vec![], |mut acc, i| match &i.interaction {
                    InteractionType::Query(q) => {
                        acc.extend(q.uses());
                        acc
                    }
                    _ => acc,
                }),
            InteractionsType::Query(query) => query.uses(),
            InteractionsType::Fault(_) => vec![],
        }
    }
}

// FIXME: for the sql display come back and add connection index as a comment
impl Display for InteractionPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for interactions in &self.plan {
            match &interactions.interactions {
                InteractionsType::Property(property) => {
                    let name = property.name();
                    writeln!(f, "-- begin testing '{name}'")?;
                    for interaction in property.interactions(interactions.connection_index) {
                        writeln!(f, "\t{interaction}")?;
                    }
                    writeln!(f, "-- end testing '{name}'")?;
                }
                InteractionsType::Fault(fault) => {
                    writeln!(f, "-- FAULT '{fault}'")?;
                }
                InteractionsType::Query(query) => {
                    writeln!(f, "{query};")?;
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct InteractionStats {
    pub(crate) select_count: u32,
    pub(crate) insert_count: u32,
    pub(crate) delete_count: u32,
    pub(crate) update_count: u32,
    pub(crate) create_count: u32,
    pub(crate) create_index_count: u32,
    pub(crate) drop_count: u32,
    pub(crate) begin_count: u32,
    pub(crate) commit_count: u32,
    pub(crate) rollback_count: u32,
}

impl InteractionStats {
    pub fn total_writes(&self) -> u32 {
        self.insert_count
            + self.delete_count
            + self.update_count
            + self.create_count
            + self.create_index_count
            + self.drop_count
    }
}

impl Display for InteractionStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Read: {}, Write: {}, Delete: {}, Update: {}, Create: {}, CreateIndex: {}, Drop: {}, Begin: {}, Commit: {}, Rollback: {}",
            self.select_count,
            self.insert_count,
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

type AssertionFunc = dyn Fn(&Vec<ResultSet>, &mut SimulatorEnv) -> Result<Result<(), String>>;

enum AssertionAST {
    Pick(),
}

#[derive(Clone)]
pub struct Assertion {
    pub func: Rc<AssertionFunc>,
    pub name: String, // For display purposes in the plan
}

impl Debug for Assertion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Assertion")
            .field("name", &self.name)
            .finish()
    }
}

impl Assertion {
    pub fn new<F>(name: String, func: F) -> Self
    where
        F: Fn(&Vec<ResultSet>, &mut SimulatorEnv) -> Result<Result<(), String>> + 'static,
    {
        Self {
            func: Rc::new(func),
            name,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
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

#[derive(Debug, Clone)]
pub struct Interaction {
    pub connection_index: usize,
    pub interaction: InteractionType,
}

impl Deref for Interaction {
    type Target = InteractionType;

    fn deref(&self) -> &Self::Target {
        &self.interaction
    }
}

impl DerefMut for Interaction {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.interaction
    }
}

impl Interaction {
    pub fn new(connection_index: usize, interaction: InteractionType) -> Self {
        Self {
            connection_index,
            interaction,
        }
    }
}

#[derive(Debug, Clone)]
pub enum InteractionType {
    Query(Query),
    Assumption(Assertion),
    Assertion(Assertion),
    Fault(Fault),
    /// Will attempt to run any random query. However, when the connection tries to sync it will
    /// close all connections and reopen the database and assert that no data was lost
    FsyncQuery(Query),
    FaultyQuery(Query),
}

// FIXME: add the connection index here later
impl Display for Interaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.interaction)
    }
}

impl Display for InteractionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Query(query) => write!(f, "{query}"),
            Self::Assumption(assumption) => write!(f, "-- ASSUME {};", assumption.name),
            Self::Assertion(assertion) => {
                write!(f, "-- ASSERT {};", assertion.name)
            }
            Self::Fault(fault) => write!(f, "-- FAULT '{fault}';"),
            Self::FsyncQuery(query) => {
                writeln!(f, "-- FSYNC QUERY;")?;
                writeln!(f, "{query};")?;
                write!(f, "{query};")
            }
            Self::FaultyQuery(query) => write!(f, "{query}; -- FAULTY QUERY"),
        }
    }
}

impl Shadow for InteractionType {
    type Result = anyhow::Result<Vec<Vec<SimValue>>>;
    fn shadow(&self, env: &mut ShadowTablesMut) -> Self::Result {
        match self {
            Self::Query(query) => query.shadow(env),
            Self::FsyncQuery(query) => {
                let mut first = query.shadow(env)?;
                first.extend(query.shadow(env)?);
                Ok(first)
            }
            Self::Assumption(_) | Self::Assertion(_) | Self::Fault(_) | Self::FaultyQuery(_) => {
                Ok(vec![])
            }
        }
    }
}
impl InteractionType {
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
                // Do not panic on parse error, because DoubleCreateFailure relies on it
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
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(message)) => Err(turso_core::LimboError::InternalError(format!(
                        "Assertion '{}' failed: {}",
                        assertion.name, message
                    ))),
                    Err(err) => Err(turso_core::LimboError::InternalError(format!(
                        "Assertion '{}' execution error: {}",
                        assertion.name, err
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
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(message)) => Err(turso_core::LimboError::InternalError(format!(
                        "Assumption '{}' failed: {}",
                        assumption.name, message
                    ))),
                    Err(err) => Err(turso_core::LimboError::InternalError(format!(
                        "Assumption '{}' execution error: {}",
                        assumption.name, err
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
                        let syncing = env.io.syncing();
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
                if let Some(turso_core::LimboError::ParseError(e)) = err {
                    panic!("Unexpected parse error: {e}");
                }
                return Err(err.unwrap());
            }
            let mut rows = rows.unwrap().unwrap();
            let mut out = Vec::new();
            let mut current_prob = 0.05;
            let mut incr = 0.001;
            loop {
                let syncing = env.io.syncing();
                let inject_fault = env.rng.random_bool(current_prob);
                // TODO: avoid for now injecting faults when syncing
                if inject_fault && !syncing {
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
    env.io.close_files();

    // 2. Re-open database
    match env.type_ {
        SimulationType::Differential => {
            for _ in 0..num_conns {
                env.connections.push(SimConnection::SQLiteConnection(
                    rusqlite::Connection::open(env.get_db_path())
                        .expect("Failed to open SQLite connection"),
                ));
            }
        }
        SimulationType::Default | SimulationType::Doublecheck => {
            env.db = None;
            let db = match turso_core::Database::open_file(
                env.io.clone(),
                env.get_db_path().to_str().expect("path should be 'to_str'"),
                false,
                true,
            ) {
                Ok(db) => db,
                Err(e) => {
                    tracing::error!(
                        "Failed to open database at {}: {}",
                        env.get_db_path().display(),
                        e
                    );
                    panic!("Failed to open database: {e}");
                }
            };

            env.db = Some(db);

            for _ in 0..num_conns {
                env.connections.push(SimConnection::LimboConnection(
                    env.db.as_ref().expect("db to be Some").connect().unwrap(),
                ));
            }
        }
    };
}

fn random_create<R: rand::Rng>(rng: &mut R, env: &SimulatorEnv, conn_index: usize) -> Interactions {
    let conn_ctx = env.connection_context(conn_index);
    let mut create = Create::arbitrary(rng, &conn_ctx);
    while conn_ctx
        .tables()
        .iter()
        .any(|t| t.name == create.table.name)
    {
        create = Create::arbitrary(rng, &conn_ctx);
    }
    Interactions::new(conn_index, InteractionsType::Query(Query::Create(create)))
}

fn random_read<R: rand::Rng>(rng: &mut R, env: &SimulatorEnv, conn_index: usize) -> Interactions {
    Interactions::new(
        conn_index,
        InteractionsType::Query(Query::Select(Select::arbitrary(
            rng,
            &env.connection_context(conn_index),
        ))),
    )
}

fn random_expr<R: rand::Rng>(rng: &mut R, env: &SimulatorEnv, conn_index: usize) -> Interactions {
    Interactions::new(
        conn_index,
        InteractionsType::Query(Query::Select(
            SelectFree::arbitrary(rng, &env.connection_context(conn_index)).0,
        )),
    )
}

fn random_write<R: rand::Rng>(rng: &mut R, env: &SimulatorEnv, conn_index: usize) -> Interactions {
    Interactions::new(
        conn_index,
        InteractionsType::Query(Query::Insert(Insert::arbitrary(
            rng,
            &env.connection_context(conn_index),
        ))),
    )
}

fn random_delete<R: rand::Rng>(rng: &mut R, env: &SimulatorEnv, conn_index: usize) -> Interactions {
    Interactions::new(
        conn_index,
        InteractionsType::Query(Query::Delete(Delete::arbitrary(
            rng,
            &env.connection_context(conn_index),
        ))),
    )
}

fn random_update<R: rand::Rng>(rng: &mut R, env: &SimulatorEnv, conn_index: usize) -> Interactions {
    Interactions::new(
        conn_index,
        InteractionsType::Query(Query::Update(Update::arbitrary(
            rng,
            &env.connection_context(conn_index),
        ))),
    )
}

fn random_drop<R: rand::Rng>(rng: &mut R, env: &SimulatorEnv, conn_index: usize) -> Interactions {
    Interactions::new(
        conn_index,
        InteractionsType::Query(Query::Drop(Drop::arbitrary(
            rng,
            &env.connection_context(conn_index),
        ))),
    )
}

fn random_create_index<R: rand::Rng>(
    rng: &mut R,
    env: &SimulatorEnv,
    conn_index: usize,
) -> Option<Interactions> {
    let conn_ctx = env.connection_context(conn_index);
    if conn_ctx.tables().is_empty() {
        return None;
    }
    let mut create_index = CreateIndex::arbitrary(rng, &conn_ctx);
    while conn_ctx
        .tables()
        .iter()
        .find(|t| t.name == create_index.table_name)
        .expect("table should exist")
        .indexes
        .iter()
        .any(|i| i == &create_index.index_name)
    {
        create_index = CreateIndex::arbitrary(rng, &conn_ctx);
    }

    Some(Interactions::new(
        conn_index,
        InteractionsType::Query(Query::CreateIndex(create_index)),
    ))
}

fn random_fault<R: rand::Rng>(rng: &mut R, env: &SimulatorEnv) -> Interactions {
    let faults = if env.opts.disable_reopen_database {
        vec![Fault::Disconnect]
    } else {
        vec![Fault::Disconnect, Fault::ReopenDatabase]
    };
    let fault = faults[rng.random_range(0..faults.len())];
    Interactions::new(env.choose_conn(rng), InteractionsType::Fault(fault))
}

impl ArbitraryFrom<(&SimulatorEnv, InteractionStats, usize)> for Interactions {
    fn arbitrary_from<R: rand::Rng, C: GenerationContext>(
        rng: &mut R,
        conn_ctx: &C,
        (env, stats, conn_index): (&SimulatorEnv, InteractionStats, usize),
    ) -> Self {
        let remaining_ = remaining(
            env.opts.max_interactions,
            &env.profile.query,
            &stats,
            env.profile.experimental_mvcc,
        );
        frequency(
            vec![
                (
                    u32::min(remaining_.select, remaining_.insert) + remaining_.create,
                    Box::new(|rng: &mut R| {
                        Interactions::new(
                            conn_index,
                            InteractionsType::Property(Property::arbitrary_from(
                                rng,
                                conn_ctx,
                                (env, &stats),
                            )),
                        )
                    }),
                ),
                (
                    remaining_.select,
                    Box::new(|rng: &mut R| random_read(rng, env, conn_index)),
                ),
                (
                    remaining_.select / 3,
                    Box::new(|rng: &mut R| random_expr(rng, env, conn_index)),
                ),
                (
                    remaining_.insert,
                    Box::new(|rng: &mut R| random_write(rng, env, conn_index)),
                ),
                (
                    remaining_.create,
                    Box::new(|rng: &mut R| random_create(rng, env, conn_index)),
                ),
                (
                    remaining_.create_index,
                    Box::new(|rng: &mut R| {
                        if let Some(interaction) = random_create_index(rng, env, conn_index) {
                            interaction
                        } else {
                            // if no tables exist, we can't create an index, so fallback to creating a table
                            random_create(rng, env, conn_index)
                        }
                    }),
                ),
                (
                    remaining_.delete,
                    Box::new(|rng: &mut R| random_delete(rng, env, conn_index)),
                ),
                (
                    remaining_.update,
                    Box::new(|rng: &mut R| random_update(rng, env, conn_index)),
                ),
                (
                    // remaining_.drop,
                    0,
                    Box::new(|rng: &mut R| random_drop(rng, env, conn_index)),
                ),
                (
                    remaining_
                        .select
                        .min(remaining_.insert)
                        .min(remaining_.create)
                        .max(1),
                    Box::new(|rng: &mut R| random_fault(rng, env)),
                ),
            ],
            rng,
        )
    }
}
