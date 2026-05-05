use std::{num::NonZeroUsize, vec};

use sql_generation::{
    generation::{Arbitrary, ArbitraryFrom, GenerationContext, frequency},
    model::query::{
        Create,
        pragma::{Pragma, WalCheckpointMode},
        transaction::{Begin, Commit},
    },
};

use crate::{
    SimulatorEnv,
    generation::{
        WeightedDistribution,
        property::PropertyDistribution,
        query::{
            PragmaGeneration, QueryDistribution, QueryGeneration, STORAGE_STRESS_PAGE_SIZES,
            possible_queries,
        },
    },
    model::{
        Query,
        interactions::{
            Fault, Interaction, InteractionBuilder, InteractionPlan, InteractionPlanIterator,
            InteractionType, Interactions, InteractionsType, StorageStress,
        },
        metrics::{InteractionStats, Remaining},
        property::Property,
    },
};

const STORAGE_STRESS_RATE: f64 = 0.05;
const STORAGE_STRESS_INITIAL_PAGE_SIZE_RATE: f64 = 0.75;
const STORAGE_STRESS_CHECKPOINT_CONN_RATE: f64 = 0.45;
// Storage stress deliberately checkpoints early: once the alternate page-size
// connection has produced a small WAL, checkpointing is the behavior under test.
const STORAGE_STRESS_MIN_MAIN_TABLE_ROWS_FOR_CHECKPOINT: usize = 4;

fn is_main_table_name(name: &str) -> bool {
    !name.contains('.')
}

fn main_schema_empty(env: &SimulatorEnv) -> bool {
    (0..env.connections.len()).all(|conn_index| {
        env.get_conn_tables(conn_index)
            .iter()
            .all(|table| !is_main_table_name(&table.name))
    })
}

fn random_storage_stress_page_size<R: rand::Rng + ?Sized>(rng: &mut R) -> Query {
    let page_size = STORAGE_STRESS_PAGE_SIZES[rng.random_range(0..STORAGE_STRESS_PAGE_SIZES.len())];
    Query::Pragma(Pragma::PageSizeSet(page_size))
}

fn storage_pragmas_allowed(env: &SimulatorEnv) -> bool {
    !matches!(env.type_, crate::runner::env::SimulationType::Differential) && !env.profile.mvcc
}

fn main_table_rows(env: &SimulatorEnv, conn_index: usize) -> usize {
    env.get_conn_tables(conn_index)
        .iter()
        .filter(|table| is_main_table_name(&table.name))
        .map(|table| table.rows.len())
        .sum()
}

fn query_generation_for(
    env: &SimulatorEnv,
    conn_index: usize,
    storage_stress: bool,
) -> QueryGeneration {
    let in_transaction = env.conn_in_transaction(conn_index);
    let main_empty = main_schema_empty(env);
    let storage_pragmas = storage_pragmas_allowed(env);

    QueryGeneration {
        pragma: PragmaGeneration {
            page_size_get_weight: if storage_pragmas { 2 } else { 0 },
            page_size_set_weight: if storage_pragmas
                && env.main_database_allows_initial_page_size()
                && !in_transaction
            {
                if storage_stress { 16 } else { 2 }
            } else {
                0
            },
            integrity_check_weight: if storage_pragmas && !main_empty { 2 } else { 0 },
        },
        storage_stress,
    }
}

impl InteractionPlan {
    fn decide_storage_stress(&mut self, rng: &mut impl rand::Rng, env: &SimulatorEnv) {
        if matches!(self.storage_stress, StorageStress::Undecided) {
            self.storage_stress =
                if storage_pragmas_allowed(env) && rng.random_bool(STORAGE_STRESS_RATE) {
                    StorageStress::Active {
                        page_size_conn: None,
                        checkpoint_attempted: false,
                    }
                } else {
                    StorageStress::Off
                };
        }
    }

    fn storage_stress_page_size_conn(&self) -> Option<usize> {
        match self.storage_stress {
            StorageStress::Active { page_size_conn, .. } => page_size_conn,
            StorageStress::Undecided | StorageStress::Off => None,
        }
    }

    fn storage_stress_enabled_for(&self, conn_index: usize) -> bool {
        self.storage_stress_page_size_conn() == Some(conn_index)
    }

    fn storage_stress_initial_page_size(
        &mut self,
        rng: &mut impl rand::Rng,
        env: &SimulatorEnv,
    ) -> Option<Interactions> {
        if !matches!(self.storage_stress, StorageStress::Active { .. }) {
            return None;
        }

        if !main_schema_empty(env) || !rng.random_bool(STORAGE_STRESS_INITIAL_PAGE_SIZE_RATE) {
            self.storage_stress = StorageStress::Off;
            return None;
        }

        let conn_index = rng.random_range(0..env.connections.len());
        let StorageStress::Active {
            ref mut page_size_conn,
            ref mut checkpoint_attempted,
        } = self.storage_stress
        else {
            unreachable!("storage stress should still be active after initial page size decision");
        };
        *page_size_conn = Some(conn_index);
        *checkpoint_attempted = false;

        Some(Interactions::new(
            conn_index,
            InteractionsType::Query(random_storage_stress_page_size(rng)),
        ))
    }

    fn choose_storage_stress_conn(
        &self,
        rng: &mut impl rand::Rng,
        env: &SimulatorEnv,
    ) -> Option<usize> {
        let conn_index = self.storage_stress_page_size_conn()?;
        if env.conn_in_transaction(conn_index)
            || main_table_rows(env, conn_index) < STORAGE_STRESS_MIN_MAIN_TABLE_ROWS_FOR_CHECKPOINT
            || !rng.random_bool(STORAGE_STRESS_CHECKPOINT_CONN_RATE)
        {
            return None;
        }
        Some(conn_index)
    }

    fn storage_stress_checkpoint(&mut self, env: &SimulatorEnv) -> Option<Interactions> {
        let StorageStress::Active {
            page_size_conn: Some(conn_index),
            ref mut checkpoint_attempted,
        } = self.storage_stress
        else {
            return None;
        };

        if *checkpoint_attempted
            || env.conn_in_transaction(conn_index)
            || main_table_rows(env, conn_index) < STORAGE_STRESS_MIN_MAIN_TABLE_ROWS_FOR_CHECKPOINT
        {
            return None;
        }

        *checkpoint_attempted = true;
        Some(Interactions::new(
            conn_index,
            InteractionsType::Property(Property::WalCheckpointPreservesDatabase {
                mode: WalCheckpointMode::Truncate,
            }),
        ))
    }

    fn observe_storage_stress_interaction(&mut self, interaction: &Interaction) {
        let StorageStress::Active {
            ref mut page_size_conn,
            ref mut checkpoint_attempted,
        } = self.storage_stress
        else {
            return;
        };

        match &interaction.interaction {
            InteractionType::Fault(Fault::Disconnect)
                if Some(interaction.connection_index) == *page_size_conn =>
            {
                *page_size_conn = None;
                *checkpoint_attempted = false;
            }
            InteractionType::Fault(Fault::ReopenDatabase) => {
                *page_size_conn = None;
                *checkpoint_attempted = false;
            }
            _ => {}
        }
    }

    pub fn generator<'a>(
        &'a mut self,
        rng: &'a mut impl rand::Rng,
    ) -> impl InteractionPlanIterator {
        let interactions = self.interactions_list().to_vec();
        let iter = interactions.into_iter();
        PlanGenerator {
            plan: self,
            peek: None,
            iter,
            rng,
        }
    }

    /// Appends a new [Interactions] and outputs the next set of [Interaction] to take
    pub fn generate_next_interaction(
        &mut self,
        rng: &mut impl rand::Rng,
        env: &mut SimulatorEnv,
    ) -> Option<Interactions> {
        let num_interactions = env.opts.max_interactions as usize;
        self.decide_storage_stress(rng, env);

        // First interaction
        if self.len_properties() == 0 {
            if let Some(interactions) = self.storage_stress_initial_page_size(rng, env) {
                return Some(interactions);
            }

            // First create at least one table
            let create_query = Create::arbitrary(&mut env.rng.clone(), &env.connection_context(0));

            // initial query starts at 0th connection
            let interactions =
                Interactions::new(0, InteractionsType::Query(Query::Create(create_query)));
            return Some(interactions);
        }

        // If last interaction needs to check all db tables, generate the Property to do so.
        // But only generate the interaction if we actually have any tables to check
        // This can happen if we created a table, and then deleted the table, and there are no more tables to check
        if let Some(i) = self.last_interactions()
            && i.check_tables()
            && !env
                .connection_context(i.connection_index)
                .tables()
                .is_empty()
        {
            let interactions = if let InteractionsType::Query(query) = &i.interactions {
                assert!(query.is_dml());
                let mut table = query.uses();
                assert!(table.len() == 1);
                let table = table.pop().unwrap();

                Interactions::new(
                    i.connection_index,
                    InteractionsType::Property(Property::TableHasExpectedContent { table }),
                )
            } else {
                Interactions::new(
                    i.connection_index,
                    InteractionsType::Property(Property::AllTableHaveExpectedContent {
                        tables: env
                            .connection_context(i.connection_index)
                            .tables()
                            .iter()
                            .map(|t| t.name.clone())
                            .collect(),
                    }),
                )
            };

            return Some(interactions);
        }

        if let Some(conn_index) = self.storage_stress_page_size_conn()
            && main_schema_empty(env)
        {
            let create_query = Create::arbitrary(rng, &env.connection_context(conn_index));
            return Some(Interactions::new(
                conn_index,
                InteractionsType::Query(Query::Create(create_query)),
            ));
        }

        if self.len_properties() < num_interactions {
            if let Some(interactions) = self.storage_stress_checkpoint(env) {
                return Some(interactions);
            }

            let conn_index = self
                .choose_storage_stress_conn(rng, env)
                .unwrap_or_else(|| env.choose_conn(rng));
            let interactions = if self.mvcc && !env.conn_in_transaction(conn_index) {
                let query = Query::Begin(Begin::Concurrent);
                Interactions::new(conn_index, InteractionsType::Query(query))
            } else if self.mvcc
                && env.conn_in_transaction(conn_index)
                && env.has_conn_executed_query_after_transaction(conn_index)
                && rng.random_bool(0.4)
            {
                let query = Query::Commit(Commit);
                Interactions::new(conn_index, InteractionsType::Query(query))
            } else {
                let conn_ctx = &env.connection_context(conn_index);
                let mut interactions = Interactions::arbitrary_from(
                    rng,
                    conn_ctx,
                    (
                        env,
                        self.stats(),
                        conn_index,
                        query_generation_for(
                            env,
                            conn_index,
                            self.storage_stress_enabled_for(conn_index),
                        ),
                    ),
                );

                // With 30% probability, redirect CREATE TABLE to an attached database
                if let InteractionsType::Query(Query::Create(ref mut create)) =
                    interactions.interactions
                {
                    if !env.attached_dbs.is_empty() && rng.random_bool(0.3) {
                        let db = &env.attached_dbs[rng.random_range(0..env.attached_dbs.len())];
                        create.table.name = format!("{}.{}", db, create.table.name);
                    }
                }

                interactions
            };

            tracing::debug!(
                "Generating interaction {}/{}",
                self.len_properties(),
                num_interactions
            );

            Some(interactions)
        } else {
            // after we generated all interactions if some connection is still in a transaction, commit
            (0..env.connections.len())
                .find(|idx| env.conn_in_transaction(*idx))
                .map(|conn_index| {
                    Interactions::new(conn_index, InteractionsType::Query(Query::Commit(Commit)))
                })
        }
    }
}

pub struct PlanGenerator<'a, R: rand::Rng> {
    plan: &'a mut InteractionPlan,
    peek: Option<Interaction>,
    iter: <Vec<Interaction> as IntoIterator>::IntoIter,
    rng: &'a mut R,
}

impl<'a, R: rand::Rng> PlanGenerator<'a, R> {
    fn next_interaction(&mut self, env: &mut SimulatorEnv) -> Option<Interaction> {
        self.iter
            .next()
            .or_else(|| {
                // Iterator ended, try to create a new iterator
                // This will not be an infinte sequence because generate_next_interaction will eventually
                // stop generating
                let interactions = self.plan.generate_next_interaction(self.rng, env)?;

                let id = self.plan.next_property_id();

                let iter = interactions.interactions(id);

                assert!(!iter.is_empty());

                let mut iter = iter.into_iter();

                self.plan.push_interactions(interactions);

                let next = iter.next();
                self.iter = iter;

                next
            })
            .map(|interaction| {
                // Certain properties can generate intermediate queries
                // we need to generate them here and substitute
                if let InteractionType::Query(Query::Placeholder) = &interaction.interaction {
                    let stats = self.plan.stats();

                    let conn_ctx = env.connection_context(interaction.connection_index);

                    let remaining_ = Remaining::new(
                        env.opts.max_interactions,
                        &env.profile.query,
                        stats,
                        env.profile.mvcc,
                        &conn_ctx,
                    );

                    let Some(InteractionsType::Property(property)) = self
                        .plan
                        .last_interactions()
                        .as_ref()
                        .map(|interactions| &interactions.interactions)
                    else {
                        unreachable!("only properties have extensional queries");
                    };

                    let queries = possible_queries(conn_ctx.tables());
                    let query_distr = QueryDistribution::new(
                        queries,
                        &remaining_,
                        query_generation_for(
                            env,
                            interaction.connection_index,
                            self.plan
                                .storage_stress_enabled_for(interaction.connection_index),
                        ),
                    );

                    let query_gen = property.get_extensional_query_gen_function();

                    let mut count = 0;
                    let new_query = loop {
                        if count > 1_000_000 {
                            panic!("possible infinite loop in query generation");
                        }
                        if let Some(new_query) =
                            (query_gen)(self.rng, &conn_ctx, &query_distr, property)
                        {
                            break new_query;
                        }
                        count += 1;
                    };

                    InteractionBuilder::from_interaction(&interaction)
                        .interaction(InteractionType::Query(new_query))
                        .build()
                        .unwrap()
                } else {
                    interaction
                }
            })
    }

    fn peek(&mut self, env: &mut SimulatorEnv) -> Option<&Interaction> {
        if self.peek.is_none() {
            self.peek = self.next_interaction(env);
        }
        self.peek.as_ref()
    }
}

impl<'a, R: rand::Rng> InteractionPlanIterator for PlanGenerator<'a, R> {
    /// try to generate the next [Interactions] and store it
    fn next(&mut self, env: &mut SimulatorEnv) -> Option<Interaction> {
        let mvcc = self.plan.mvcc;
        let mut next_interaction = || match self.peek(env) {
            Some(peek_interaction) => {
                if mvcc && peek_interaction.is_ddl() {
                    // if any connection is in a transaction,
                    // try to commit the transaction as we cannot execute DDL statements in concurrent mode

                    if let Some(conn_index) =
                        (0..env.connections.len()).find(|idx| env.conn_in_transaction(*idx))
                    {
                        return Some(
                            InteractionBuilder::from_interaction(peek_interaction)
                                .interaction(InteractionType::Query(Query::Commit(Commit)))
                                .connection_index(conn_index)
                                .build()
                                .unwrap(),
                        );
                    }
                }

                self.peek.take()
            }
            None => {
                // after we generated all interactions if some connection is still in a transaction, commit
                let commit = (0..env.connections.len())
                    .find(|idx| env.conn_in_transaction(*idx))
                    .map(|conn_index| {
                        let query = Query::Commit(Commit);
                        let interactions =
                            Interactions::new(conn_index, InteractionsType::Query(query));

                        let interaction = InteractionBuilder::with_interaction(
                            InteractionType::Query(Query::Commit(Commit)),
                        )
                        .connection_index(conn_index)
                        .id(self.plan.next_property_id())
                        .build()
                        .unwrap();

                        self.plan.push_interactions(interactions);

                        interaction
                    });

                #[cfg(debug_assertions)]
                if commit.is_none() {
                    // Do a final sanity check to make sure that all interactions are sorted by ids
                    assert!(self.plan.interactions_list().is_sorted_by_key(|a| a.id()));
                }
                commit
            }
        };
        let next_interaction = next_interaction();
        // intercept interaction to update metrics
        if let Some(next_interaction) = next_interaction.as_ref() {
            self.plan
                .observe_storage_stress_interaction(next_interaction);
            // Skip counting queries that come from Properties that only exist to check tables
            if let Some(property_meta) = next_interaction.property_meta
                && !property_meta.property.check_tables()
            {
                self.plan.stats_mut().update(next_interaction);
            }
            self.plan.push(next_interaction.clone());
        }

        next_interaction
    }
}

impl Interactions {
    pub(crate) fn interactions(&self, id: NonZeroUsize) -> Vec<Interaction> {
        let ret = match &self.interactions {
            InteractionsType::Property(property) => {
                property.interactions(self.connection_index, id)
            }
            InteractionsType::Query(query) => {
                let mut builder =
                    InteractionBuilder::with_interaction(InteractionType::Query(query.clone()));
                builder.connection_index(self.connection_index).id(id);
                let interaction = builder.build().unwrap();
                vec![interaction]
            }
            InteractionsType::Fault(fault) => {
                let mut builder =
                    InteractionBuilder::with_interaction(InteractionType::Fault(*fault));
                builder.connection_index(self.connection_index).id(id);
                let interaction = builder.build().unwrap();
                vec![interaction]
            }
        };

        assert!(!ret.is_empty());
        ret
    }
}

fn random_fault<R: rand::Rng + ?Sized>(
    rng: &mut R,
    env: &SimulatorEnv,
    conn_index: usize,
) -> Interactions {
    let faults = if env.opts.disable_reopen_database {
        vec![Fault::Disconnect]
    } else {
        vec![Fault::Disconnect, Fault::ReopenDatabase]
    };
    let fault = faults[rng.random_range(0..faults.len())];
    Interactions::new(conn_index, InteractionsType::Fault(fault))
}

impl ArbitraryFrom<(&SimulatorEnv, &InteractionStats, usize, QueryGeneration)> for Interactions {
    fn arbitrary_from<R: rand::Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        conn_ctx: &C,
        (env, stats, conn_index, query_generation): (
            &SimulatorEnv,
            &InteractionStats,
            usize,
            QueryGeneration,
        ),
    ) -> Self {
        let remaining_ = Remaining::new(
            env.opts.max_interactions,
            &env.profile.query,
            stats,
            env.profile.mvcc,
            conn_ctx,
        );

        let queries = possible_queries(conn_ctx.tables());
        let query_distr = QueryDistribution::new(queries, &remaining_, query_generation);

        #[expect(clippy::type_complexity)]
        let mut choices: Vec<(u32, Box<dyn Fn(&mut R) -> Interactions>)> = vec![
            (
                query_distr.weights().total_weight(),
                Box::new(|rng: &mut R| {
                    Interactions::new(
                        conn_index,
                        InteractionsType::Query(Query::arbitrary_from(rng, conn_ctx, &query_distr)),
                    )
                }),
            ),
            (
                remaining_
                    .select
                    .min(remaining_.insert)
                    .min(remaining_.create)
                    .max(1),
                Box::new(|rng: &mut R| random_fault(rng, env, conn_index)),
            ),
        ];

        if let Ok(property_distr) =
            PropertyDistribution::new(env, &remaining_, &query_distr, conn_ctx, conn_index)
        {
            choices.push((
                property_distr.weights().total_weight(),
                Box::new(move |rng: &mut R| {
                    Interactions::new(
                        conn_index,
                        InteractionsType::Property(Property::arbitrary_from(
                            rng,
                            conn_ctx,
                            &property_distr,
                        )),
                    )
                }),
            ));
        };

        frequency(choices, rng)
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;
    use crate::{
        profiles::Profile,
        runner::{
            cli::SimulatorCLI,
            env::{Paths, SimulationType},
        },
    };

    #[test]
    fn page_size_set_uses_database_initialization_not_empty_schema() {
        let temp_dir = tempfile::tempdir().unwrap();
        let cli_opts = SimulatorCLI::parse_from([
            "limbo-simulator",
            "--minimum-tests",
            "1",
            "--maximum-tests",
            "1",
            "--io-backend",
            "memory",
        ]);
        let profile = Profile {
            max_connections: 1,
            ..Default::default()
        };
        let mut env = SimulatorEnv::new(
            0,
            &cli_opts,
            Paths::new(temp_dir.path()),
            SimulationType::Default,
            &profile,
        );

        assert!(main_schema_empty(&env));
        assert!(
            query_generation_for(&env, 0, false)
                .pragma
                .page_size_set_weight
                > 0
        );

        env.mark_main_database_initialized_for_page_size();

        assert!(main_schema_empty(&env));
        assert_eq!(
            query_generation_for(&env, 0, false)
                .pragma
                .page_size_set_weight,
            0
        );
    }
}
