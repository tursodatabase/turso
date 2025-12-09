use std::{
    fmt::{Debug, Display},
    ops::{Deref, DerefMut},
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
use turso_core::{Connection, LimboError, Result, StepResult};

use crate::{
    SimulatorEnv,
    generation::{
        Shadow,
        assertion::{Assertion, Bindings, Relation, eval_assertion},
    },
    model::Query,
    runner::env::{ShadowTablesMut, SimConnection, SimulationType},
};

use super::property::{Property, remaining};

pub(crate) type ResultSet = Result<Relation>;

#[derive(Debug, Clone)]
pub(crate) struct InteractionPlan {
    plan: Vec<InteractionGroup>,
    pub mvcc: bool,
    // Len should not count transactions statements, just so we can generate more meaningful interactions per run
    len: usize,
}

impl InteractionPlan {
    pub(crate) fn new(mvcc: bool) -> Self {
        Self {
            plan: Vec::new(),
            mvcc,
            len: 0,
        }
    }

    #[inline]
    pub fn plan(&self) -> &[InteractionGroup] {
        &self.plan
    }

    /// Length of interactions that are not transaction statements
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn push(&mut self, interactions: InteractionGroup) {
        self.plan.push(interactions);
    }

    pub fn interactions_list(&self) -> Vec<Interaction> {
        self.plan
            .clone()
            .into_iter()
            .flat_map(|ig| ig.interactions.into_iter())
            .collect()
    }

    pub fn interactions_list_with_secondary_index(&self) -> Vec<(usize, Interaction)> {
        self.plan
            .clone()
            .into_iter()
            .enumerate()
            .flat_map(|(idx, ig)| {
                ig.interactions
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
        for ig in &self.plan {
            for interaction in &ig.interactions {
                if let InteractionType::Query(query) = &interaction.interaction {
                    query_stat(query, &mut stats);
                }
            }
        }

        stats
    }

    pub fn init_plan(env: &mut SimulatorEnv) -> Self {
        let mut plan = InteractionPlan::new(env.profile.experimental_mvcc);

        // First create at least one table
        let create_query = Create::arbitrary(&mut env.rng.clone(), &env.connection_context(0));

        // initial query starts at 0th connection
        plan.push(InteractionGroup::single(Interaction::new(
            0,
            InteractionType::Query(Query::Create(create_query)),
        )));

        plan
    }

    /// Appends a new [Interactions] and outputs the next set of [Interaction] to take
    pub fn generate_next_interaction(
        &mut self,
        rng: &mut impl rand::Rng,
        env: &mut SimulatorEnv,
    ) -> Option<InteractionGroup> {
        let num_interactions = env.opts.max_interactions as usize;
        if self.len() < num_interactions {
            let conn_index = env.choose_conn(rng);
            let mut ig = if self.mvcc && !env.conn_in_transaction(conn_index) {
                let query = Query::Begin(Begin::Concurrent);
                InteractionGroup::single(Interaction::new(
                    conn_index,
                    InteractionType::Query(query),
                ))
            } else if self.mvcc
                && env.conn_in_transaction(conn_index)
                && env.has_conn_executed_query_after_transaction(conn_index)
                && rng.random_bool(0.4)
            {
                let query = Query::Commit(Commit);
                InteractionGroup::single(Interaction::new(
                    conn_index,
                    InteractionType::Query(query),
                ))
            } else {
                let conn_ctx = &env.connection_context(conn_index);
                InteractionGroup::arbitrary_from(rng, conn_ctx, (env, self.stats(), conn_index))
            };

            tracing::debug!("Generating interaction {}/{}", self.len(), num_interactions);

            assert!(!ig.interactions.is_empty());

            let ig = if self.mvcc
                && ig
                    .interactions
                    .iter()
                    .any(|interaction| interaction.is_ddl())
            {
                // DDL statements must be serial, so commit all connections and then execute the DDL
                let mut commit_interactions = (0..env.connections.len())
                    .filter(|&idx| env.conn_in_transaction(idx))
                    .map(|idx| {
                        let query = Query::Commit(Commit);
                        let interaction = Interaction::new(idx, InteractionType::Query(query));
                        self.push(InteractionGroup::single(interaction.clone()));
                        InteractionGroup::single(interaction)
                    })
                    .fold(
                        Vec::with_capacity(env.connections.len()),
                        |mut accum, mut curr| {
                            accum.append(&mut curr.interactions);
                            accum
                        },
                    );
                commit_interactions.append(&mut ig.interactions);
                InteractionGroup::new(InteractionGroupType::Transaction, None, commit_interactions)
            } else {
                ig
            };

            self.push(ig.clone());
            Some(ig)
        } else {
            // after we generated all interactions if some connection is still in a transaction, commit
            (0..env.connections.len())
                .find(|idx| env.conn_in_transaction(*idx))
                .map(|conn_index| {
                    let query = Query::Commit(Commit);
                    let interaction = Interaction::new(conn_index, InteractionType::Query(query));
                    self.push(InteractionGroup::single(interaction.clone()));
                    InteractionGroup::single(interaction)
                })
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
    type Target = Vec<InteractionGroup>;

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
    type Item = InteractionGroup;

    type IntoIter = <Vec<InteractionGroup> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.plan.into_iter()
    }
}

impl<'a> IntoIterator for &'a InteractionPlan {
    type Item = &'a InteractionGroup;

    type IntoIter = <&'a Vec<InteractionGroup> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.plan.iter()
    }
}

impl<'a> IntoIterator for &'a mut InteractionPlan {
    type Item = &'a mut InteractionGroup;

    type IntoIter = <&'a mut Vec<InteractionGroup> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.plan.iter_mut()
    }
}

pub trait InteractionPlanIterator {
    fn next(&mut self, env: &mut SimulatorEnv) -> Option<Interaction>;
}

impl<T: InteractionPlanIterator> InteractionPlanIterator for &mut T {
    #[inline]
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
                .map_or(Vec::new().into_iter(), |ig| ig.interactions.into_iter());
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
    #[inline]
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
    pub bindings: Bindings,
}

impl ConnectionState {
    pub fn last_result(&self) -> Option<Result<Vec<Vec<SimValue>>>> {
        self.bindings.get("__last").cloned().map(|r| match r {
            Ok(rel) => Ok(rel.rows),
            Err(e) => Err(e),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InteractionGroupType {
    Single,
    Property,
    Transaction,
}

impl Display for InteractionGroupType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InteractionGroupType::Single => write!(f, "single"),
            InteractionGroupType::Property => write!(f, "property"),
            InteractionGroupType::Transaction => write!(f, "transaction"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct InteractionGroup {
    typ_: InteractionGroupType,
    name: Option<String>,
    interactions: Vec<Interaction>,
}

impl InteractionGroup {
    pub(crate) fn new(
        typ_: InteractionGroupType,
        name: Option<String>,
        interactions: Vec<Interaction>,
    ) -> Self {
        Self {
            typ_,
            name,
            interactions,
        }
    }

    pub(crate) fn single(interaction: Interaction) -> Self {
        Self {
            typ_: InteractionGroupType::Single,
            name: None,
            interactions: vec![interaction],
        }
    }
}

impl Iterator for InteractionGroup {
    type Item = Interaction;

    fn next(&mut self) -> Option<Self::Item> {
        if self.interactions.is_empty() {
            None
        } else {
            Some(self.interactions.remove(0))
        }
    }
}

impl InteractionGroup {
    pub(crate) fn dependencies(&self) -> IndexSet<String> {
        self.interactions
            .iter()
            .fold(IndexSet::new(), |mut acc, i| {
                acc.extend(i.dependencies());
                acc
            })
    }

    pub(crate) fn uses(&self) -> Vec<String> {
        self.interactions.iter().fold(vec![], |mut acc, i| {
            acc.extend(i.uses());
            acc
        })
    }
}

// FIXME: for the sql display come back and add connection index as a comment
impl Display for InteractionPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for ig in &self.plan {
            match ig.typ_ {
                InteractionGroupType::Property | InteractionGroupType::Transaction => {
                    if let Some(name) = &ig.name {
                        writeln!(f, "-- begin {} '{}'", ig.typ_, name)?;
                    } else {
                        writeln!(f, "-- begin {}", ig.typ_)?;
                    }

                    for interaction in &ig.interactions {
                        writeln!(f, "{interaction};")?;
                    }

                    if ig.name.is_some() {
                        writeln!(f, "-- end {} '{}'", ig.typ_, ig.name.as_ref().unwrap())?;
                    } else {
                        writeln!(f, "-- end {}", ig.typ_)?;
                    }
                }
                InteractionGroupType::Single => {
                    writeln!(
                        f,
                        "{};",
                        ig.interactions.get(0).expect(
                            "InteractionGroupType::Single must have exactly one interaction"
                        )
                    )?;
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Fault {
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
    pub binding: Option<String>,
    pub connection_index: usize,
    pub interaction: InteractionType,
    pub ignore_error: bool,
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
            ignore_error: false,
            binding: None,
        }
    }

    pub fn new_ignore_error(connection_index: usize, interaction: InteractionType) -> Self {
        Self {
            connection_index,
            interaction,
            ignore_error: true,
            binding: None,
        }
    }

    pub fn dependencies(&self) -> IndexSet<String> {
        match &self.interaction {
            InteractionType::Query(q) => q.dependencies(),
            _ => IndexSet::new(),
        }
    }
    pub fn uses(&self) -> Vec<String> {
        match &self.interaction {
            InteractionType::Query(q) => q.uses(),
            _ => vec![],
        }
    }
}

#[derive(Debug, Clone)]
pub enum Control {
    ShadowIfOk {
        from: String,
        apply: Query,
        conn_index: usize,
    },
    RollbackIfErr {
        from: String,
        conn_index: usize,
    },
}

#[derive(Debug, Clone)]
pub enum InteractionType {
    Query(Query),
    Assumption(Assertion),
    Assertion(Assertion),
    Fault(Fault),
    Control(Control),
    /// Will attempt to run any random query. However, when the connection tries to sync it will
    /// close all connections and reopen the database and assert that no data was lost
    FsyncQuery(Query),
    FaultyQuery(Query),
}

impl Interaction {
    pub fn bind(mut self, binding: String) -> Self {
        self.binding = Some(binding);
        self
    }
}

// FIXME: add the connection index here later
impl Display for Interaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(binding) = &self.binding {
            writeln!(f, "-- @bind {}", binding)?;
        }
        if self.ignore_error {
            writeln!(f, "-- @ignore_error")?;
        }
        write!(f, "{} -- {}", self.interaction, self.connection_index)
    }
}

impl Display for InteractionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Query(query) => write!(f, "{query};"),
            Self::Assumption(assumption) => write!(f, "-- ASSUME {};", assumption),
            Self::Assertion(assertion) => {
                write!(f, "-- ASSERT {};", assertion)
            }
            Self::Control(control) => match control {
                Control::ShadowIfOk {
                    from,
                    apply,
                    conn_index,
                } => {
                    write!(
                        f,
                        "-- CONTROL SHADOW_IF_OK from '{}' apply '{}' on conn {}",
                        from, apply, conn_index
                    )
                }
                Control::RollbackIfErr { from, conn_index } => {
                    write!(
                        f,
                        "-- CONTROL ROLLBACK_IF_ERR from '{}' on conn {}",
                        from, conn_index
                    )
                }
            },
            Self::Fault(fault) => write!(f, "-- FAULT '{fault}'"),
            Self::FsyncQuery(query) => {
                writeln!(f, "-- FSYNC QUERY")?;
                writeln!(f, "{query};")?;
                write!(f, "{query};")
            }
            Self::FaultyQuery(query) => write!(f, "{query}; -- FAULTY QUERY;"),
        }
    }
}

impl Shadow for InteractionType {
    type Result = anyhow::Result<Vec<Vec<SimValue>>>;
    fn shadow(&self, env: &mut ShadowTablesMut) -> Self::Result {
        match self {
            Self::Query(query) => {
                if !query.is_transaction() {
                    env.add_query(query);
                }
                query.shadow(env)
            }
            Self::Control(cnode) => match cnode {
                Control::ShadowIfOk {
                    from: _,
                    apply,
                    conn_index: _,
                } => apply.shadow(env),
                Control::RollbackIfErr {
                    from: _,
                    conn_index: _,
                } => Ok(vec![]),
            },
            Self::Assumption(_)
            | Self::Assertion(_)
            | Self::Fault(_)
            | Self::FaultyQuery(_)
            | Self::FsyncQuery(_) => Ok(vec![]),
        }
    }
}

impl InteractionType {
    pub fn is_ddl(&self) -> bool {
        match self {
            InteractionType::Query(query)
            | InteractionType::FsyncQuery(query)
            | InteractionType::FaultyQuery(query) => query.is_ddl(),
            _ => false,
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
                // Do not panic on parse error, because DoubleCreateFailure relies on it
                return Err(err.unwrap());
            }
            let rows = rows?;
            assert!(rows.is_some());
            let mut rows = rows.unwrap();
            let mut out =
                Relation::new(rows.column_names().iter().map(|s| s.to_string()).collect());
            while let Ok(row) = rows.step() {
                match row {
                    StepResult::Row => {
                        let row = rows.row().unwrap();
                        let mut r = Vec::new();
                        for v in row.get_values() {
                            let v = v.into();
                            r.push(v);
                        }
                        out.rows.push(r);
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
        bindings: &Bindings,
        env: &mut SimulatorEnv,
    ) -> Result<()> {
        match self {
            Self::Assertion(assertion) => match eval_assertion(assertion, bindings, env) {
                anyhow::Result::Ok(_) => Ok(()),
                anyhow::Result::Err(e) => Err(LimboError::InternalError(format!(
                    "Assertion '{}' evaluation error: {e}",
                    assertion
                )))
                .into(),
            },
            _ => unreachable!("execute_assertion only valid for Assertion"),
        }
    }

    pub(crate) fn execute_assumption(
        &self,
        bindings: &Bindings,
        env: &mut SimulatorEnv,
    ) -> Result<()> {
        match self {
            Self::Assumption(assert) => match eval_assertion(assert, bindings, env) {
                anyhow::Result::Ok(_) => Ok(()),
                anyhow::Result::Err(e) => Err(LimboError::InternalError(format!(
                    "Assumption '{}' evaluation error: {e}",
                    assert
                ))
                .into()),
            },
            _ => unreachable!("execute_assumption only valid for Assumption"),
        }
    }

    pub(crate) fn execute_fault(&self, env: &mut SimulatorEnv, conn_index: usize) -> Result<()> {
        match self {
            Self::Fault(fault) => {
                match fault {
                    Fault::Disconnect => {
                        if env.connections[conn_index].is_connected() {
                            if env.conn_in_transaction(conn_index) {
                                env.rollback_conn(conn_index);
                            }
                            env.connections[conn_index].disconnect();
                        } else {
                            return Err(turso_core::LimboError::InternalError(
                                "connection already disconnected".into(),
                            ));
                        }
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
            let mut out =
                Relation::new(rows.column_names().iter().map(|s| s.to_string()).collect());
            while let Ok(row) = rows.step() {
                match row {
                    StepResult::Row => {
                        let row = rows.row().unwrap();
                        let mut r = Vec::new();
                        for v in row.get_values() {
                            let v = v.into();
                            r.push(v);
                        }
                        out.rows.push(r);
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
            let mut out =
                Relation::new(rows.column_names().iter().map(|s| s.to_string()).collect());
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
                        out.rows.push(r);
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

    pub(crate) fn execute_control(
        &self,
        env: &mut SimulatorEnv,
        bindings: &Bindings,
    ) -> Result<()> {
        match self {
            Self::Control(control) => match control {
                Control::ShadowIfOk {
                    from,
                    apply,
                    conn_index,
                } => {
                    let from_result = bindings.get(from).ok_or_else(|| {
                        LimboError::InternalError(format!("binding '{}' not found", from))
                    })?;
                    if from_result.is_ok() {
                        let _ = apply.shadow(&mut env.get_conn_tables_mut(*conn_index));
                    }
                    Ok(())
                }
                Control::RollbackIfErr { from, conn_index } => {
                    let from_result = bindings.get(from).ok_or_else(|| {
                        LimboError::InternalError(format!("binding '{}' not found", from))
                    })?;
                    if from_result.is_err() {
                        env.rollback_conn(*conn_index);
                    }
                    Ok(())
                }
            },
            _ => unreachable!("execute_control only valid for Control"),
        }
    }
}

fn reopen_database(env: &mut SimulatorEnv) {
    // 1. Close all connections without default checkpoint-on-close behavior
    // to expose bugs related to how we handle WAL
    let mvcc = env.profile.experimental_mvcc;
    let indexes = env.profile.query.gen_opts.indexes;
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
                mvcc,
                indexes,
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

fn random_create<R: rand::Rng>(
    rng: &mut R,
    env: &SimulatorEnv,
    conn_index: usize,
) -> InteractionGroup {
    let conn_ctx = env.connection_context(conn_index);
    let mut create = Create::arbitrary(rng, &conn_ctx);
    while conn_ctx
        .tables()
        .iter()
        .any(|t| t.name == create.table.name)
    {
        create = Create::arbitrary(rng, &conn_ctx);
    }
    InteractionGroup::single(Interaction::new(
        conn_index,
        InteractionType::Query(Query::Create(create)),
    ))
}

fn random_read<R: rand::Rng>(
    rng: &mut R,
    env: &SimulatorEnv,
    conn_index: usize,
) -> InteractionGroup {
    InteractionGroup::single(Interaction::new(
        conn_index,
        InteractionType::Query(Query::Select(Select::arbitrary(
            rng,
            &env.connection_context(conn_index),
        ))),
    ))
}

fn random_expr<R: rand::Rng>(
    rng: &mut R,
    env: &SimulatorEnv,
    conn_index: usize,
) -> InteractionGroup {
    InteractionGroup::single(Interaction::new(
        conn_index,
        InteractionType::Query(Query::Select(
            SelectFree::arbitrary(rng, &env.connection_context(conn_index)).0,
        )),
    ))
}

fn random_write<R: rand::Rng>(
    rng: &mut R,
    env: &SimulatorEnv,
    conn_index: usize,
) -> InteractionGroup {
    InteractionGroup::single(Interaction::new(
        conn_index,
        InteractionType::Query(Query::Insert(Insert::arbitrary(
            rng,
            &env.connection_context(conn_index),
        ))),
    ))
}

fn random_delete<R: rand::Rng>(
    rng: &mut R,
    env: &SimulatorEnv,
    conn_index: usize,
) -> InteractionGroup {
    InteractionGroup::single(Interaction::new(
        conn_index,
        InteractionType::Query(Query::Delete(Delete::arbitrary(
            rng,
            &env.connection_context(conn_index),
        ))),
    ))
}

fn random_update<R: rand::Rng>(
    rng: &mut R,
    env: &SimulatorEnv,
    conn_index: usize,
) -> InteractionGroup {
    tracing::trace!(
        "Generating Update interaction for connection {} with env {:?}",
        conn_index,
        env
    );
    InteractionGroup::single(Interaction::new(
        conn_index,
        InteractionType::Query(Query::Update(Update::arbitrary(
            rng,
            &env.connection_context(conn_index),
        ))),
    ))
}

fn random_drop<R: rand::Rng>(
    rng: &mut R,
    env: &SimulatorEnv,
    conn_index: usize,
) -> InteractionGroup {
    InteractionGroup::single(Interaction::new(
        conn_index,
        InteractionType::Query(Query::Drop(Drop::arbitrary(
            rng,
            &env.connection_context(conn_index),
        ))),
    ))
}

fn random_create_index<R: rand::Rng>(
    rng: &mut R,
    env: &SimulatorEnv,
    conn_index: usize,
) -> Option<InteractionGroup> {
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

    Some(InteractionGroup::single(Interaction::new(
        conn_index,
        InteractionType::Query(Query::CreateIndex(create_index)),
    )))
}

fn random_fault<R: rand::Rng>(rng: &mut R, env: &SimulatorEnv) -> InteractionGroup {
    let faults = if env.opts.disable_reopen_database {
        vec![Fault::Disconnect]
    } else {
        vec![Fault::Disconnect, Fault::ReopenDatabase]
    };
    let fault = faults[rng.random_range(0..faults.len())];
    InteractionGroup::single(Interaction::new(
        env.choose_conn(rng),
        InteractionType::Fault(fault),
    ))
}

impl ArbitraryFrom<(&SimulatorEnv, InteractionStats, usize)> for InteractionGroup {
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
        tracing::trace!(
            "Generating random interaction for conn {} with remaining {:?} and stats {}",
            conn_index,
            remaining_,
            stats
        );
        frequency(
            vec![
                (
                    u32::min(remaining_.select, remaining_.insert) + remaining_.create,
                    Box::new(|rng: &mut R| {
                        let property = Property::arbitrary_from(rng, conn_ctx, (env, &stats));
                        InteractionGroup::new(
                            InteractionGroupType::Property,
                            Some(property.name().to_string()),
                            property.interactions(conn_index),
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
