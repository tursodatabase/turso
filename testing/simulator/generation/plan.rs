use std::{num::NonZeroUsize, vec};

use sql_generation::{
    generation::{Arbitrary, ArbitraryFrom, GenerationContext, frequency},
    model::query::{
        transaction::{Begin, Commit},
    },
};

use crate::{
    generation::{
        property::PropertyDistribution,
        query::{QueryDistribution, possible_queries},
    },
    model::{
        interactions::{
            Fault, Interaction, InteractionBuilder, InteractionPlan, InteractionPlanIterator,
            InteractionType, Interactions, InteractionsType,
        },
        metrics::{InteractionStats, Remaining},
        property::Property,
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
        // First interaction or continue scenario initialization
        if self.len_properties() == 0 {
            // Check if coordinated matview scenario is enabled
            if env.profile.query.enable_coordinated_matview_scenario {
                // Generate coordinated matview scenario
                let scenario = CoordinatedMatviewScenario::generate();

                // Build all setup queries: CREATE TABLEs, initial INSERTs, CREATE MATERIALIZED VIEWs
                let mut setup_queries = Vec::new();
                setup_queries.extend(scenario.create_table_queries());
                setup_queries.extend(scenario.initial_insert_queries());
                setup_queries.extend(scenario.create_matview_queries());

                // Store remaining queries for subsequent interactions
                self.coordinated_matview_scenario_queries = Some(setup_queries);
            }
        }

        // If we have pending scenario queries, generate them first
        if let Some(ref mut queries) = self.coordinated_matview_scenario_queries {
            if !queries.is_empty() {
                let query = queries.remove(0);
                let interactions = Interactions::new(0, InteractionsType::Query(query));
                return Some(interactions);
            } else {
                // All scenario queries generated, clear the flag
                self.coordinated_matview_scenario_queries = None;
            }
        }

        // First interaction (if scenario not enabled or completed)
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
            // For non-MVCC mode, if any connection has an exclusive lock (BEGIN IMMEDIATE),
            // we must use that connection to avoid "Database is busy" errors.
            // Check both executed transactions (env) and pending generated transactions (plan).
            let conn_index = if !self.mvcc {
                if let Some(txn_conn) = self.pending_txn_conn() {
                    // Use the connection with pending generated transaction
                    txn_conn
                } else if let Some(txn_conn) =
                    (0..env.connections.len()).find(|idx| env.conn_in_transaction(*idx))
                {
                    // Use the connection that has an executed transaction
                    txn_conn
                } else {
                    env.choose_conn(rng)
                }
            } else {
                env.choose_conn(rng)
            };

            // Check both executed and pending transaction state
            let in_txn =
                env.conn_in_transaction(conn_index) || self.pending_txn_conn() == Some(conn_index);
            let batch_enabled = env.profile.query.enable_transaction_batching;

            // For non-MVCC mode, check if ANY connection is in a transaction (executed or pending)
            // (BEGIN IMMEDIATE takes exclusive write lock, so only one txn at a time)
            let any_conn_in_txn = !self.mvcc
                && (self.pending_txn_conn().is_some()
                    || (0..env.connections.len()).any(|idx| env.conn_in_transaction(idx)));

            // Plan-level transaction batching (both MVCC and non-MVCC modes)
            let interactions = if !in_txn
                && batch_enabled
                && !any_conn_in_txn  // Don't start new txn if another conn has exclusive lock
                && rng.random_bool(env.profile.query.transaction_batch_start_probability)
            {
                // Start a batch transaction (CONCURRENT for MVCC, IMMEDIATE for non-MVCC)
                let begin = if self.mvcc {
                    Begin::Concurrent
                } else {
                    Begin::Immediate
                };
                // Track the pending transaction
                self.set_pending_txn_conn(Some(conn_index));
                Interactions::new(conn_index, InteractionsType::Query(Query::Begin(begin)))
            } else if in_txn
                && env.has_conn_executed_query_after_transaction(conn_index)
                && rng.random_bool(env.profile.query.transaction_batch_commit_probability)
            {
                // Randomly commit the batch - clear pending transaction state
                self.set_pending_txn_conn(None);
                Interactions::new(conn_index, InteractionsType::Query(Query::Commit(Commit)))
            } else {
                // Generate property/query
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
            // For non-MVCC mode, also check pending_txn_conn to handle the case where
            // a DISCONNECT was generated (clearing pending state) but not yet executed
            (0..env.connections.len())
                .find(|idx| {
                    let executed_txn = env.conn_in_transaction(*idx);
                    if self.mvcc {
                        executed_txn
                    } else {
                        // For non-MVCC, also require pending_txn_conn to be set
                        // This prevents generating COMMIT after a DISCONNECT was generated
                        executed_txn && self.pending_txn_conn() == Some(*idx)
                    }
                })
                .map(|conn_index| {
                    self.set_pending_txn_conn(None);
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
                        env.profile.experimental_mvcc,
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
        // Extract pending_txn_conn before the closure to avoid borrow conflicts
        let pending_txn_conn = self.plan.pending_txn_conn();

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

                // For non-MVCC mode: if any connection has an exclusive lock (BEGIN IMMEDIATE),
                // and the peeked interaction is for a different connection, commit first
                // to avoid "Database is busy" errors
                if !mvcc {
                    // Check both pending and executed transactions
                    let txn_conn = pending_txn_conn.or_else(|| {
                        (0..env.connections.len()).find(|idx| env.conn_in_transaction(*idx))
                    });
                    if let Some(txn_conn) = txn_conn {
                        if peek_interaction.connection_index != txn_conn {
                            // Interaction is for a different connection - commit the transaction first
                            return Some(
                                InteractionBuilder::from_interaction(peek_interaction)
                                    .interaction(InteractionType::Query(Query::Commit(Commit)))
                                    .connection_index(txn_conn)
                                    .build()
                                    .unwrap(),
                            );
                        }
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

        // Clear pending transaction state when we commit or disconnect
        if let Some(ref interaction) = next_interaction {
            if !mvcc {
                match &interaction.interaction {
                    InteractionType::Query(Query::Commit(_)) => {
                        // COMMIT ends the transaction
                        self.plan.set_pending_txn_conn(None);
                    }
                    InteractionType::Fault(Fault::Disconnect | Fault::ReopenDatabase) => {
                        // Disconnect/reopen causes implicit rollback
                        self.plan.set_pending_txn_conn(None);
                    }
                    _ => {}
                }
            }
        }

        // intercept interaction to update metrics
        if let Some(next_interaction) = next_interaction.as_ref() {
            // Skip counting queries that come from Properties that only exist to check tables
            // But always count direct query interactions (property_meta is None)
            let should_update_stats = match next_interaction.property_meta {
                Some(property_meta) => !property_meta.property.check_tables(),
                None => true,
            };
            if should_update_stats {
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
            env.profile.experimental_mvcc,
            conn_ctx,
        );

        let queries = possible_queries(conn_ctx.tables());
        let query_distr = QueryDistribution::new(queries, &remaining_);

        #[expect(clippy::type_complexity)]
        let mut choices: Vec<(u32, Box<dyn Fn(&mut R) -> Interactions>)> = vec![(
            query_distr.weights().total_weight(),
            Box::new(|rng: &mut R| {
                Interactions::new(
                    conn_index,
                    InteractionsType::Query(Query::arbitrary_from(rng, conn_ctx, &query_distr)),
                )
            }),
        )];

        // Only include faults if enabled in profile
        if env.profile.io.fault.enable {
            choices.push((
                remaining_
                    .select
                    .min(remaining_.insert)
                    .min(remaining_.create)
                    .max(1),
                Box::new(|rng: &mut R| random_fault(rng, env, conn_index)),
            ));
        }

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

/// Coordinated materialized view scenario for IVM testing.
/// Creates base tables, populates them with initial data, and creates materialized views.
/// This provides a consistent setup for testing incremental view maintenance.
pub struct CoordinatedMatviewScenario {
    pub base_tables: Vec<CoordinatedTable>,
    pub matviews: Vec<CoordinatedMatview>,
}

pub struct CoordinatedTable {
    pub name: String,
    pub columns: Vec<(String, String)>, // (name, type)
    pub initial_rows: Vec<Vec<String>>,
}

#[allow(dead_code)]
pub struct CoordinatedMatview {
    pub name: String,
    pub select_sql: String,
}

impl CoordinatedMatviewScenario {
    /// Generate a scenario for testing materialized views.
    /// Creates navigation_history, navigation_cursor, and blocks tables
    /// with materialized views that join them.
    pub fn generate() -> Self {
        let navigation_history = CoordinatedTable {
            name: "navigation_history".to_string(),
            columns: vec![
                ("id".to_string(), "INTEGER PRIMARY KEY".to_string()),
                ("block_id".to_string(), "INTEGER".to_string()),
                ("created_at".to_string(), "TEXT".to_string()),
            ],
            initial_rows: vec![
                vec![
                    "1".to_string(),
                    "100".to_string(),
                    "'2024-01-01'".to_string(),
                ],
                vec![
                    "2".to_string(),
                    "101".to_string(),
                    "'2024-01-02'".to_string(),
                ],
            ],
        };

        let navigation_cursor = CoordinatedTable {
            name: "navigation_cursor".to_string(),
            columns: vec![
                ("id".to_string(), "INTEGER PRIMARY KEY".to_string()),
                ("history_id".to_string(), "INTEGER".to_string()),
                ("cursor_pos".to_string(), "INTEGER".to_string()),
            ],
            initial_rows: vec![vec!["1".to_string(), "1".to_string(), "0".to_string()]],
        };

        let blocks = CoordinatedTable {
            name: "blocks".to_string(),
            columns: vec![
                ("id".to_string(), "INTEGER PRIMARY KEY".to_string()),
                ("parent_id".to_string(), "INTEGER".to_string()),
                ("content".to_string(), "TEXT".to_string()),
            ],
            initial_rows: vec![
                vec![
                    "100".to_string(),
                    "NULL".to_string(),
                    "'Root block'".to_string(),
                ],
                vec![
                    "101".to_string(),
                    "100".to_string(),
                    "'Child block'".to_string(),
                ],
            ],
        };

        let current_focus = CoordinatedMatview {
            name: "current_focus".to_string(),
            select_sql: "SELECT nc.*, nh.block_id, nh.created_at FROM navigation_cursor nc INNER JOIN navigation_history nh ON nc.history_id = nh.id".to_string(),
        };

        let blocks_with_paths = CoordinatedMatview {
            name: "blocks_with_paths".to_string(),
            select_sql: "SELECT * FROM blocks".to_string(),
        };

        Self {
            base_tables: vec![navigation_history, navigation_cursor, blocks],
            matviews: vec![current_focus, blocks_with_paths],
        }
    }

    /// Generate CREATE TABLE queries for all base tables
    pub fn create_table_queries(&self) -> Vec<Query> {
        use sql_generation::model::query::Create;
        use sql_generation::model::table::{Column, ColumnType, Table};

        self.base_tables
            .iter()
            .map(|t| {
                let columns = t
                    .columns
                    .iter()
                    .map(|(name, type_str)| {
                        let col_type = if type_str.contains("INTEGER") {
                            ColumnType::Integer
                        } else {
                            ColumnType::Text
                        };
                        Column {
                            name: name.clone(),
                            column_type: col_type,
                            constraints: vec![],
                        }
                    })
                    .collect();

                Query::Create(Create {
                    table: Table {
                        name: t.name.clone(),
                        columns,
                        indexes: vec![],
                        rows: vec![],
                    },
                })
            })
            .collect()
    }

    /// Generate INSERT queries for initial data
    pub fn initial_insert_queries(&self) -> Vec<Query> {
        use sql_generation::model::query::Insert;
        use sql_generation::model::table::SimValue;
        use turso_core::Value;

        self.base_tables
            .iter()
            .flat_map(|t| {
                t.initial_rows.iter().map(|row| {
                    let values: Vec<SimValue> = row
                        .iter()
                        .map(|v| {
                            if v == "NULL" {
                                SimValue(Value::Null)
                            } else if let Ok(i) = v.parse::<i64>() {
                                SimValue(Value::from_i64(i))
                            } else {
                                // Strip quotes from string literals
                                let s = v.trim_matches('\'');
                                SimValue(Value::Text(s.to_string().into()))
                            }
                        })
                        .collect();

                    Query::Insert(Insert::Values {
                        table: t.name.clone(),
                        values: vec![values],
                        conflict: None,
                        on_conflict: None,
                    })
                })
            })
            .collect()
    }

    /// Generate CREATE MATERIALIZED VIEW queries
    pub fn create_matview_queries(&self) -> Vec<Query> {
        use sql_generation::model::query::predicate::Predicate;
        use sql_generation::model::query::select::{
            FromClause, ResultColumn, SelectBody, SelectInner, SelectTable,
        };
        use sql_generation::model::query::{CreateMaterializedView, Select};
        use sql_generation::model::table::JoinedTable;
        use turso_parser::ast::Distinctness;

        self.matviews
            .iter()
            .map(|mv| {
                // For current_focus, generate a proper join query
                let select = if mv.name == "current_focus" {
                    Select {
                        with: None,
                        body: SelectBody {
                            select: Box::new(SelectInner {
                                distinctness: Distinctness::All,
                                columns: vec![ResultColumn::Star],
                                from: Some(FromClause {
                                    table: SelectTable::Table(
                                        "navigation_cursor".to_string(),
                                        Some("nc".to_string()),
                                    ),
                                    joins: vec![JoinedTable {
                                        table: "navigation_history".to_string(),
                                        alias: Some("nh".to_string()),
                                        join_type: sql_generation::model::table::JoinType::Inner,
                                        on: Predicate::eq_bare(
                                            Predicate::qualified_column("nc", "history_id"),
                                            Predicate::qualified_column("nh", "id"),
                                        ),
                                    }],
                                }),
                                where_clause: Predicate::true_(),
                                order_by: None,
                            }),
                            compounds: vec![],
                        },
                        limit: None,
                    }
                } else {
                    // Simple SELECT * FROM blocks for blocks_with_paths
                    Select::simple("blocks".to_string(), Predicate::true_())
                };

                Query::CreateMaterializedView(CreateMaterializedView {
                    name: mv.name.clone(),
                    select,
                })
            })
            .collect()
    }
}
