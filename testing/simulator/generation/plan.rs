use std::{num::NonZeroUsize, vec};

use crate::{
    SimulatorEnv,
    generation::{
        WeightedDistribution,
        property::PropertyDistribution,
        query::{QueryDistribution, possible_queries},
    },
    model::{
        Query,
        interactions::{
            Fault, Interaction, InteractionBuilder, InteractionPlan, InteractionPlanIterator,
            InteractionType, Interactions, InteractionsType,
        },
        metrics::{InteractionStats, Remaining},
        property::Property,
    },
};
use sql_generation::model::query::Select;
use sql_generation::model::query::predicate::Predicate;
use sql_generation::{
    generation::{Arbitrary, ArbitraryFrom, GenerationContext, frequency},
    model::query::{
        Create,
        transaction::{Begin, Commit},
    },
};

impl InteractionPlan {
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
        // First interaction
        if self.len_properties() == 0 {
            // First create at least one table
            let create_query = Create::arbitrary(&mut env.rng.clone(), &env.connection_context(0));

            // initial query starts at 0th connection
            let interactions =
                Interactions::new(0, InteractionsType::Query(Query::Create(create_query)));
            return Some(interactions);
        }

        let num_interactions = env.opts.max_interactions as usize;
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

        if self.len_properties() < num_interactions {
            let conn_index = env.choose_conn(rng);
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
                let mut interactions =
                    Interactions::arbitrary_from(rng, conn_ctx, (env, self.stats(), conn_index));

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
                    let query_distr = QueryDistribution::new(queries, &remaining_);

                    let query_gen = property.get_extensional_query_gen_function();

                    let new_query = 'retries: {
                        for _ in 0..if env.profile.mvcc {1_000} else {1_000_000} {
                            if let Some(query) =
                                query_gen(self.rng, &conn_ctx, &query_distr, property)
                            {
                                break 'retries query;
                            }
                        }
                        break 'retries if env.profile.mvcc {
                            // Under MVCC the property's required table can be absent from this
                            // connection's snapshot in the shadow model, due to difficulties in
                            // keeping track of concurrent connections.
                            // Fall back to an innocuous SELECT TRUE.
                            let property_name: &'static str = property.into();
                            tracing::info!(
                                property = property_name,
                                connection_index = interaction.connection_index,
                                "extensional query generation exhausted max attempts; falling back to SELECT TRUE"
                            );
                            Query::Select(Select::expr(Predicate::true_()))
                        } else {
                            panic!("possible infinite loop in query generation");
                        };
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

        // In MVCC mode, if the next peeked interaction is DDL but some connection is
        // still in a transaction, we must drain those transactions first via synthetic
        // COMMITs (DDL cannot run in concurrent mode).
        //
        // The synthetic COMMIT is its own logical step, not part of the DDL property.
        // We give it its own fresh property id and renumber the still-peeked DDL to a
        // higher fresh id, so the plan stays sorted by id. This matters because:
        //   - find_interactions_range relies on binary search by id.
        //   - if a synthetic COMMIT fails recoverably (e.g. WriteWriteConflict) and
        //     execution returns NextInteractionOutsideThisProperty, the skip loop in
        //     `execute_interactions` advances `plan.next` until it sees a different
        //     id. Reusing the DDL's id would trap that loop forever, because plan.next
        //     keeps minting same-id synthetic COMMITs for the remaining in-txn
        //     connections (they never execute in the skip loop, so progress stalls).
        // Only synthesize when the peeked DDL is the *first* interaction we will
        // emit for its property — i.e. nothing with this id is already in the plan.
        // If an interaction with the same id is already there, we're mid-way
        // through emitting that property (e.g. an InsertValuesSelect whose
        // placeholder middle query just substituted to a DDL). Synthesizing here
        // would slip COMMITs into the property's emission and leave the rest of
        // the property (its verifying SELECT and ASSERT) to execute against
        // post-DDL state — wrong, and not what the property is testing.
        let in_flight_property = self
            .peek(env)
            .map(|p| p.id())
            .is_some_and(|peek_id| {
                self.plan
                    .interactions_list()
                    .last()
                    .is_some_and(|last| last.id() == peek_id)
            });
        let synthetic_pre_ddl_commit = if mvcc
            && !in_flight_property
            && self.peek(env).is_some_and(|p| p.is_ddl())
        {
            (0..env.connections.len())
                .find(|idx| env.conn_in_transaction(*idx))
                .map(|conn_index| {
                    let commit_id = self.plan.next_property_id();
                    let renumbered_id = self.plan.next_property_id();
                    // Renumber only the peeked DDL — NOT the rest of `self.iter`.
                    //
                    // Earlier versions also renumbered the iter to "preserve plan order
                    // ↔ id order" so binary search by id stayed valid. That was wrong:
                    // when the iter belongs to a different in-flight property whose
                    // placeholder happened to substitute to a DDL (e.g.
                    // InsertValuesSelect with a DDL middle query), the iter holds that
                    // property's remaining structural interactions (its SELECT and
                    // verification ASSERT). Renumbering them moves them out of their
                    // home property, so they survive `NextInteractionOutsideThisProperty`
                    // skipping and execute later against wrong state, e.g. failing the
                    // "row should be found" assertion because intervening synthetic
                    // commits and DDL changed the database between the INSERT and the
                    // verifying SELECT. The plan-sort invariant has been relaxed
                    // (find_interactions_range now linear-scans).
                    let old_ddl = self
                        .peek
                        .take()
                        .expect("peek was Some on the line above");
                    let renumbered_ddl = InteractionBuilder::from_interaction(&old_ddl)
                        .id(renumbered_id)
                        .build()
                        .unwrap();
                    self.peek = Some(renumbered_ddl);

                    InteractionBuilder::with_interaction(InteractionType::Query(Query::Commit(
                        Commit,
                    )))
                    .connection_index(conn_index)
                    .id(commit_id)
                    .build()
                    .unwrap()
                })
        } else {
            None
        };

        let next_interaction = if synthetic_pre_ddl_commit.is_some() {
            synthetic_pre_ddl_commit
        } else {
            match self.peek(env) {
                Some(_) => self.peek.take(),
                None => {
                    // after we generated all interactions if some connection is still in a
                    // transaction, commit
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

                    commit
                }
            }
        };
        // intercept interaction to update metrics
        if let Some(next_interaction) = next_interaction.as_ref() {
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

impl ArbitraryFrom<(&SimulatorEnv, &InteractionStats, usize)> for Interactions {
    fn arbitrary_from<R: rand::Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        conn_ctx: &C,
        (env, stats, conn_index): (&SimulatorEnv, &InteractionStats, usize),
    ) -> Self {
        let remaining_ = Remaining::new(
            env.opts.max_interactions,
            &env.profile.query,
            stats,
            env.profile.mvcc,
            conn_ctx,
        );

        let queries = possible_queries(conn_ctx.tables());
        let query_distr = QueryDistribution::new(queries, &remaining_);

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
            PropertyDistribution::new(env, &remaining_, &query_distr, conn_ctx)
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
