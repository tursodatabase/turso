use crate::{
    SandboxedResult, SimulatorEnv,
    generation::{
        plan::{InteractionPlan, InteractionType, Interactions, InteractionsType},
        property::Property,
    },
    model::Query,
    run_simulation,
    runner::execution::Execution,
};
use std::sync::{Arc, Mutex};

impl InteractionPlan {
    /// Create a smaller interaction plan by deleting a property
    pub(crate) fn shrink_interaction_plan(&self, failing_execution: &Execution) -> InteractionPlan {
        // todo: this is a very naive implementation, next steps are;
        // - Shrink to multiple values by removing random interactions
        // - Shrink properties by removing their extensions, or shrinking their values
        let mut plan = self.clone();

        let all_interactions = self.interactions_list_with_secondary_index();
        let secondary_interactions_index = all_interactions[failing_execution.interaction_index].0;

        // Index of the parent property where the interaction originated from
        let failing_property = &self.plan[secondary_interactions_index];
        let mut depending_tables = failing_property.dependencies();

        {
            let mut idx = failing_execution.interaction_index;
            loop {
                if all_interactions[idx].0 != secondary_interactions_index {
                    // Stop when we reach a different property
                    break;
                }
                match &all_interactions[idx].1.interaction {
                    InteractionType::Query(query) | InteractionType::FaultyQuery(query) => {
                        depending_tables = query.dependencies();
                        break;
                    }
                    // Fault does not depend on
                    InteractionType::Fault(..) => break,
                    _ => {
                        // In principle we should never fail this checked_sub.
                        // But if there is a bug in how we count the secondary index
                        // we may panic if we do not use a checked_sub.
                        if let Some(new_idx) = idx.checked_sub(1) {
                            idx = new_idx;
                        } else {
                            tracing::warn!("failed to find error query");
                            break;
                        }
                    }
                }
            }
        }

        let before = self.len();

        // Remove all properties after the failing one
        plan.plan.truncate(secondary_interactions_index + 1);

        // means we errored in some fault on transaction statement so just maintain the statements from before the failing one
        if !depending_tables.is_empty() {
            let mut idx = 0;
            // Remove all properties that do not use the failing tables
            plan.plan.retain_mut(|interactions| {
                let retain = if idx == secondary_interactions_index {
                    if let InteractionsType::Property(
                        Property::FsyncNoWait { tables, .. } | Property::FaultyQuery { tables, .. },
                    ) = &mut interactions.interactions
                    {
                        tables.retain(|table| depending_tables.contains(table));
                    }
                    true
                } else if matches!(
                    interactions.interactions,
                    InteractionsType::Query(Query::Begin(..))
                        | InteractionsType::Query(Query::Commit(..))
                        | InteractionsType::Query(Query::Rollback(..))
                ) {
                    true
                } else {
                    let mut has_table = interactions
                        .uses()
                        .iter()
                        .any(|t| depending_tables.contains(t));

                    if has_table {
                        // Remove the extensional parts of the properties
                        if let InteractionsType::Property(p) = &mut interactions.interactions {
                            match p {
                                Property::InsertValuesSelect { queries, .. }
                                | Property::DoubleCreateFailure { queries, .. }
                                | Property::DeleteSelect { queries, .. }
                                | Property::DropSelect { queries, .. } => {
                                    queries.clear();
                                }
                                Property::FsyncNoWait { tables, query }
                                | Property::FaultyQuery { tables, query } => {
                                    if !query.uses().iter().any(|t| depending_tables.contains(t)) {
                                        tables.clear();
                                    } else {
                                        tables.retain(|table| depending_tables.contains(table));
                                    }
                                }
                                Property::SelectLimit { .. }
                                | Property::SelectSelectOptimizer { .. }
                                | Property::WhereTrueFalseNull { .. }
                                | Property::UNIONAllPreservesCardinality { .. }
                                | Property::ReadYourUpdatesBack { .. }
                                | Property::TableHasExpectedContent { .. } => {}
                            }
                        }
                        // Check again after query clear if the interactions still uses the failing table
                        has_table = interactions
                            .uses()
                            .iter()
                            .any(|t| depending_tables.contains(t));
                    }
                    let is_fault = matches!(interactions.interactions, InteractionsType::Fault(..));
                    is_fault
                        || (has_table
                            && !matches!(
                                interactions.interactions,
                                InteractionsType::Query(Query::Select(_))
                                    | InteractionsType::Property(Property::SelectLimit { .. })
                                    | InteractionsType::Property(
                                        Property::SelectSelectOptimizer { .. }
                                    )
                            ))
                };
                idx += 1;
                retain
            });

            // Comprise of idxs of Begin interactions
            let mut begin_idx = Vec::new();
            // Comprise of idxs of the intereactions Commit and Rollback
            let mut end_tx_idx = Vec::new();

            for (idx, interactions) in plan.plan.iter().enumerate() {
                match &interactions.interactions {
                    InteractionsType::Query(Query::Begin(..)) => {
                        begin_idx.push(idx);
                    }
                    InteractionsType::Query(Query::Commit(..))
                    | InteractionsType::Query(Query::Rollback(..)) => {
                        let last_begin = begin_idx.last().unwrap() + 1;
                        if last_begin == idx {
                            end_tx_idx.push(idx);
                        }
                    }
                    _ => {}
                }
            }

            // remove interactions if its just a Begin Commit/Rollback with no queries in the middle
            let mut range_transactions = end_tx_idx.into_iter().peekable();
            let mut idx = 0;
            plan.plan.retain_mut(|_| {
                let mut retain = true;

                if let Some(txn_interaction_idx) = range_transactions.peek().copied() {
                    if txn_interaction_idx == idx {
                        range_transactions.next();
                    }
                    if txn_interaction_idx == idx || txn_interaction_idx.saturating_sub(1) == idx {
                        retain = false;
                    }
                }
                idx += 1;
                retain
            });
        }

        let after = plan.len();

        tracing::info!(
            "Shrinking interaction plan from {} to {} properties",
            before,
            after
        );

        plan
    }
    /// Create a smaller interaction plan by deleting a property
    pub(crate) fn brute_shrink_interaction_plan(
        &self,
        result: &SandboxedResult,
        env: Arc<Mutex<SimulatorEnv>>,
    ) -> InteractionPlan {
        let failing_execution = match result {
            SandboxedResult::Panicked {
                error: _,
                last_execution: e,
            } => e,
            SandboxedResult::FoundBug {
                error: _,
                history: _,
                last_execution: e,
            } => e,
            SandboxedResult::Correct => {
                unreachable!("shrink is never called on correct result")
            }
        };

        let mut plan = self.clone();
        let all_interactions = self.interactions_list_with_secondary_index();
        let secondary_interactions_index = all_interactions[failing_execution.interaction_index].0;

        {
            let mut idx = failing_execution.interaction_index;
            loop {
                if all_interactions[idx].0 != secondary_interactions_index {
                    // Stop when we reach a different property
                    break;
                }
                match &all_interactions[idx].1.interaction {
                    // Fault does not depend on
                    InteractionType::Fault(..) => break,
                    _ => {
                        // In principle we should never fail this checked_sub.
                        // But if there is a bug in how we count the secondary index
                        // we may panic if we do not use a checked_sub.
                        if let Some(new_idx) = idx.checked_sub(1) {
                            idx = new_idx;
                        } else {
                            tracing::warn!("failed to find error query");
                            break;
                        }
                    }
                }
            }
        }

        let before = self.len();

        plan.plan.truncate(secondary_interactions_index + 1);

        // phase 1: shrink extensions
        for interaction in &mut plan.plan {
            if let InteractionsType::Property(property) = &mut interaction.interactions {
                match property {
                    Property::InsertValuesSelect { queries, .. }
                    | Property::DoubleCreateFailure { queries, .. }
                    | Property::DeleteSelect { queries, .. }
                    | Property::DropSelect { queries, .. } => {
                        let mut temp_plan = InteractionPlan::new_with(
                            queries
                                .iter()
                                .map(|q| {
                                    Interactions::new(
                                        interaction.connection_index,
                                        InteractionsType::Query(q.clone()),
                                    )
                                })
                                .collect(),
                            self.mvcc,
                        );

                        temp_plan = InteractionPlan::iterative_shrink(
                            temp_plan,
                            failing_execution,
                            result,
                            env.clone(),
                            secondary_interactions_index,
                        );
                        //temp_plan = Self::shrink_queries(temp_plan, failing_execution, result, env);

                        *queries = temp_plan
                            .into_iter()
                            .filter_map(|i| match i.interactions {
                                InteractionsType::Query(q) => Some(q),
                                _ => None,
                            })
                            .collect();
                    }
                    Property::WhereTrueFalseNull { .. }
                    | Property::UNIONAllPreservesCardinality { .. }
                    | Property::SelectLimit { .. }
                    | Property::SelectSelectOptimizer { .. }
                    | Property::FaultyQuery { .. }
                    | Property::FsyncNoWait { .. }
                    | Property::ReadYourUpdatesBack { .. }
                    | Property::TableHasExpectedContent { .. } => {}
                }
            }
        }

        // phase 2: shrink the entire plan
        plan = Self::iterative_shrink(
            plan,
            failing_execution,
            result,
            env,
            secondary_interactions_index,
        );

        let after = plan.len();

        tracing::info!(
            "Shrinking interaction plan from {} to {} properties",
            before,
            after
        );

        plan
    }

    /// shrink a plan by removing one interaction at a time (and its deps) while preserving the error
    fn iterative_shrink(
        mut plan: InteractionPlan,
        failing_execution: &Execution,
        old_result: &SandboxedResult,
        env: Arc<Mutex<SimulatorEnv>>,
        secondary_interaction_index: usize,
    ) -> InteractionPlan {
        for i in (0..plan.len()).rev() {
            if i == secondary_interaction_index {
                continue;
            }
            let mut test_plan = plan.clone();

            test_plan.plan.remove(i);

            if Self::test_shrunk_plan(&test_plan, failing_execution, old_result, env.clone()) {
                plan = test_plan;
            }
        }
        plan
    }

    fn test_shrunk_plan(
        test_plan: &InteractionPlan,
        failing_execution: &Execution,
        old_result: &SandboxedResult,
        env: Arc<Mutex<SimulatorEnv>>,
    ) -> bool {
        let last_execution = Arc::new(Mutex::new(*failing_execution));
        let result = SandboxedResult::from(
            std::panic::catch_unwind(|| {
                let plan = test_plan.static_iterator();

                run_simulation(env.clone(), plan, last_execution.clone())
            }),
            last_execution,
        );
        match (old_result, &result) {
            (
                SandboxedResult::Panicked { error: e1, .. },
                SandboxedResult::Panicked { error: e2, .. },
            )
            | (
                SandboxedResult::FoundBug { error: e1, .. },
                SandboxedResult::FoundBug { error: e2, .. },
            ) => e1 == e2,
            _ => false,
        }
    }
}
