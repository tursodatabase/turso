use std::{
    fmt::{Debug, Display},
    ops::{Deref, DerefMut},
    path::Path,
    rc::Rc,
    sync::Arc,
};

use indexmap::IndexSet;
use serde::{Deserialize, Serialize};
use sql_generation::model::table::SimValue;
use turso_core::{Connection, Result, StepResult};

use crate::{
    generation::Shadow,
    model::{Query, ResultSet, property::Property},
    runner::env::{ShadowTablesMut, SimConnection, SimulationType, SimulatorEnv},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct InteractionPlan {
    plan: Vec<Interactions>,
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

    pub fn new_with(plan: Vec<Interactions>, mvcc: bool) -> Self {
        let len = plan
            .iter()
            .filter(|interaction| !interaction.ignore())
            .count();
        Self { plan, mvcc, len }
    }

    #[inline]
    fn new_len(&self) -> usize {
        self.plan
            .iter()
            .filter(|interaction| !interaction.ignore())
            .count()
    }

    /// Length of interactions that are not transaction statements
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn plan(&self) -> &[Interactions] {
        &self.plan
    }

    pub fn push(&mut self, interactions: Interactions) {
        if !interactions.ignore() {
            self.len += 1;
        }
        self.plan.push(interactions);
    }

    pub fn remove(&mut self, index: usize) -> Interactions {
        let interactions = self.plan.remove(index);
        if !interactions.ignore() {
            self.len -= 1;
        }
        interactions
    }

    pub fn truncate(&mut self, len: usize) {
        self.plan.truncate(len);
        self.len = self.new_len();
    }

    pub fn retain_mut<F>(&mut self, mut f: F)
    where
        F: FnMut(&mut Interactions) -> bool,
    {
        let f = |t: &mut Interactions| {
            let ignore = t.ignore();
            let retain = f(t);
            // removed an interaction that was not previously ignored
            if !retain && !ignore {
                self.len -= 1;
            }
            retain
        };
        self.plan.retain_mut(f);
    }

    #[expect(dead_code)]
    pub fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&Interactions) -> bool,
    {
        let f = |t: &Interactions| {
            let ignore = t.ignore();
            let retain = f(t);
            // removed an interaction that was not previously ignored
            if !retain && !ignore {
                self.len -= 1;
            }
            retain
        };
        self.plan.retain(f);
        self.len = self.new_len();
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
                tracing::error!("Comparing '{}' with '{}'", interactions[i], plan[j][k]);
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
        let mut stats = InteractionStats::default();

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
                Query::AlterTable(_) => stats.alter_table_count += 1,
                Query::DropIndex(_) => stats.drop_index_count += 1,
                Query::Placeholder => {}
                Query::Pragma(_) => stats.pragma_count += 1,
            }
        }
        for interactions in &self.plan {
            match &interactions.interactions {
                InteractionsType::Property(property) => {
                    if matches!(property, Property::AllTableHaveExpectedContent { .. }) {
                        // Skip Property::AllTableHaveExpectedContent when counting stats
                        // this allows us to generate more relevant interactions as we count less Select's to the Stats
                        continue;
                    }
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

    pub fn static_iterator(&self) -> impl InteractionPlanIterator {
        PlanIterator {
            iter: self.interactions_list().into_iter(),
        }
    }
}

impl Deref for InteractionPlan {
    type Target = Vec<Interactions>;

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

impl<'a> IntoIterator for &'a InteractionPlan {
    type Item = &'a Interactions;

    type IntoIter = <&'a Vec<Interactions> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.plan.iter()
    }
}

impl<'a> IntoIterator for &'a mut InteractionPlan {
    type Item = &'a mut Interactions;

    type IntoIter = <&'a mut Vec<Interactions> as IntoIterator>::IntoIter;

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

    pub fn get_extensional_queries(&mut self) -> Option<&mut Vec<Query>> {
        match &mut self.interactions {
            InteractionsType::Property(property) => property.get_extensional_queries(),
            InteractionsType::Query(..) | InteractionsType::Fault(..) => None,
        }
    }

    /// Whether the interaction needs to check the database tables
    pub fn check_tables(&self) -> bool {
        match &self.interactions {
            InteractionsType::Property(property) => property.check_tables(),
            InteractionsType::Query(..) | InteractionsType::Fault(..) => false,
        }
    }

    /// Interactions that are not counted/ignored in the InteractionPlan.
    /// Used in InteractionPlan to not count certain interactions to its length, as they are just auxiliary. This allows more
    /// meaningful interactions to be generation
    fn ignore(&self) -> bool {
        self.is_transaction()
            || matches!(
                self.interactions,
                InteractionsType::Property(Property::AllTableHaveExpectedContent { .. })
            )
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

impl InteractionsType {
    pub fn is_transaction(&self) -> bool {
        match self {
            InteractionsType::Query(query) => query.is_transaction(),
            _ => false,
        }
    }
}

impl Interactions {
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
                    writeln!(f, "{query}; -- {}", interactions.connection_index)?;
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct InteractionStats {
    pub select_count: u32,
    pub insert_count: u32,
    pub delete_count: u32,
    pub update_count: u32,
    pub create_count: u32,
    pub create_index_count: u32,
    pub drop_count: u32,
    pub begin_count: u32,
    pub commit_count: u32,
    pub rollback_count: u32,
    pub alter_table_count: u32,
    pub drop_index_count: u32,
    pub pragma_count: u32,
}

impl Display for InteractionStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Read: {}, Insert: {}, Delete: {}, Update: {}, Create: {}, CreateIndex: {}, Drop: {}, Begin: {}, Commit: {}, Rollback: {}, Alter Table: {}, Drop Index: {}",
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
            self.alter_table_count,
            self.drop_index_count,
        )
    }
}

type AssertionFunc = dyn Fn(&Vec<ResultSet>, &mut SimulatorEnv) -> Result<Result<(), String>>;

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
        }
    }

    pub fn new_ignore_error(connection_index: usize, interaction: InteractionType) -> Self {
        Self {
            connection_index,
            interaction,
            ignore_error: true,
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
        write!(f, "{}; -- {}", self.interaction, self.connection_index)
    }
}

impl Display for InteractionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Query(query) => write!(f, "{query}"),
            Self::Assumption(assumption) => write!(f, "-- ASSUME {}", assumption.name),
            Self::Assertion(assertion) => {
                write!(f, "-- ASSERT {};", assertion.name)
            }
            Self::Fault(fault) => write!(f, "-- FAULT '{fault}'"),
            Self::FsyncQuery(query) => {
                writeln!(f, "-- FSYNC QUERY")?;
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
            Self::Query(query) => {
                if !query.is_transaction() {
                    env.add_query(query);
                }
                query.shadow(env)
            }
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
            assert!(
                !matches!(query, Query::Placeholder),
                "simulation cannot have a placeholder Query for execution"
            );

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
            let db = match turso_core::Database::open_file_with_flags(
                env.io.clone(),
                env.get_db_path().to_str().expect("path should be 'to_str'"),
                turso_core::OpenFlags::default(),
                turso_core::DatabaseOpts::new()
                    .with_mvcc(mvcc)
                    .with_indexes(indexes)
                    .with_autovacuum(true),
                None,
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
