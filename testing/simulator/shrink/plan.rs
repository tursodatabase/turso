use indexmap::IndexSet;
use sql_generation::model::query::alter_table::{AlterTable, AlterTableType};
use turso_core::turso_assert_eq;

use crate::{
    SandboxedResult, SimulatorEnv,
    model::{
        Query,
        interactions::{InteractionPlan, InteractionType},
        property::PropertyDiscriminants,
    },
    run_simulation,
    runner::execution::Execution,
};
use std::{
    collections::HashMap,
    num::NonZeroUsize,
    ops::Range,
    sync::{Arc, Mutex},
};

impl InteractionPlan {
    /// Create a smaller interaction plan by deleting a property
    pub(crate) fn shrink_interaction_plan(&self, failing_execution: &Execution) -> InteractionPlan {
        // todo: this is a very naive implementation, next steps are;
        // - Shrink to multiple values by removing random interactions
        // - Shrink properties by removing their extensions, or shrinking their values
        let mut plan = self.clone();

        let all_interactions = self.interactions_list();
        let failing_interaction = &all_interactions[failing_execution.interaction_index];

        let range = self.find_interactions_range(failing_interaction.id());

        // Interactions that are part of the failing overall property
        let mut failing_property = all_interactions
            [range.start..=failing_execution.interaction_index]
            .iter()
            .rev();

        let mut depending_tables = failing_property
            .find_map(|interaction| {
                match &interaction.interaction {
                    InteractionType::Query(query) | InteractionType::FaultyQuery(query) => {
                        Some(query.dependencies())
                    }
                    // Fault does not depend on tables
                    InteractionType::Fault(..) => None,
                    InteractionType::Assertion(assert) | InteractionType::Assumption(assert) => {
                        (!assert.tables.is_empty()).then(|| assert.dependencies())
                    }
                    _ => None,
                }
            })
            .unwrap_or_else(IndexSet::new);

        // Iterate over the rest of the interactions to identify if the depending tables ever changed names
        all_interactions[..range.start]
            .iter()
            .rev()
            .for_each(|interaction| match &interaction.interaction {
                InteractionType::Query(query)
                | InteractionType::FsyncQuery(query)
                | InteractionType::FaultyQuery(query) => {
                    if let Query::AlterTable(AlterTable {
                        table_name,
                        alter_table_type: AlterTableType::RenameTo { new_name },
                    }) = query
                    {
                        if depending_tables.contains(new_name)
                            || depending_tables.contains(table_name)
                        {
                            depending_tables.insert(new_name.clone());
                            depending_tables.insert(table_name.clone());
                        }
                    }
                }
                _ => {}
            });

        let before = self.len();

        // Remove all properties after the failing one
        plan.truncate(failing_execution.interaction_index + 1);

        // means we errored in some fault on transaction statement so just maintain the statements from before the failing one
        if !depending_tables.is_empty() {
            plan.remove_properties(&depending_tables, range);
        }

        let after = plan.len();

        tracing::info!(
            "Shrinking interaction plan from {} to {} interactions",
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
        let all_interactions = self.interactions_list();
        let property_id = all_interactions[failing_execution.interaction_index].id();

        let before = self.len_properties();

        plan.truncate(failing_execution.interaction_index + 1);

        // phase 2: shrink the entire plan
        plan = Self::iterative_shrink(&plan, failing_execution, result, env, property_id);

        let after = plan.len_properties();

        tracing::info!(
            "Shrinking interaction plan from {} to {} properties",
            before,
            after
        );

        plan
    }

    /// shrink a plan by removing one interaction at a time (and its deps) while preserving the error
    fn iterative_shrink(
        plan: &InteractionPlan,
        failing_execution: &Execution,
        old_result: &SandboxedResult,
        env: Arc<Mutex<SimulatorEnv>>,
        failing_property_id: NonZeroUsize,
    ) -> InteractionPlan {
        let mut iter_properties = plan.rev_iter_properties();

        let mut ret_plan = plan.clone();

        while let Some(property_interactions) = iter_properties.next_property() {
            // get the overall property id and try to remove it
            // need to consume the iterator, to advance outer iterator
            if let Some((_, interaction)) = property_interactions.last()
                && interaction.id() != failing_property_id
            {
                // try to remove the property
                let mut test_plan = ret_plan.clone();
                test_plan.remove_property(interaction.id());
                if Self::test_shrunk_plan(&test_plan, failing_execution, old_result, env.clone()) {
                    ret_plan = test_plan;
                }
            }
        }

        ret_plan
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

    /// Remove all properties that do not use the failing tables
    fn remove_properties(
        &mut self,
        depending_tables: &IndexSet<String>,
        failing_interaction_range: Range<usize>,
    ) {
        // First pass - mark indexes that should be retained
        let mut retain_map = Vec::with_capacity(self.len());
        let mut iter_properties = self.iter_properties();
        while let Some(property_interactions) = iter_properties.next_property() {
            for (idx, interaction) in property_interactions {
                let retain = if failing_interaction_range.end == idx {
                    true
                } else {
                    let is_part_of_property = failing_interaction_range.contains(&idx);

                    let has_table = interaction
                        .uses()
                        .iter()
                        .any(|t| depending_tables.contains(t));

                    let is_fault = matches!(&interaction.interaction, InteractionType::Fault(..));
                    let is_transaction = matches!(
                        &interaction.interaction,
                        InteractionType::Query(Query::Begin(..))
                            | InteractionType::Query(Query::Commit(..))
                            | InteractionType::Query(Query::Rollback(..))
                    );
                    let is_pragma = matches!(
                        &interaction.interaction,
                        InteractionType::Query(Query::Pragma(..))
                    );

                    let skip_interaction = if let Some(property_meta) = interaction.property_meta {
                        if matches!(
                            property_meta.property,
                            PropertyDiscriminants::AllTableHaveExpectedContent
                                | PropertyDiscriminants::SelectLimit
                                | PropertyDiscriminants::SelectSelectOptimizer
                                | PropertyDiscriminants::TableHasExpectedContent
                                | PropertyDiscriminants::UnionAllPreservesCardinality
                                | PropertyDiscriminants::WhereTrueFalseNull
                        ) {
                            // Theses properties only emit select queries, so they can be discarded entirely
                            true
                        } else {
                            property_meta.extension
                                && matches!(
                                    &interaction.interaction,
                                    InteractionType::Query(Query::Select(..))
                                )
                        }
                    } else {
                        matches!(
                            &interaction.interaction,
                            InteractionType::Query(Query::Select(..))
                        )
                    };

                    (is_part_of_property || !skip_interaction)
                        && (is_fault || is_transaction || is_pragma || has_table)
                };
                retain_map.push(retain);
            }
        }

        #[cfg(debug_assertions)]
        turso_assert_eq!(self.len(), retain_map.len());

        let mut idx = 0;
        // Remove all properties that do not use the failing tables
        self.retain_mut(|_| {
            let retain = retain_map[idx];
            idx += 1;
            retain
        });

        // Comprises of idxs of Begin interactions
        let mut begin_idx: HashMap<usize, Vec<usize>> = HashMap::new();
        // Comprises of idxs of Commit and Rollback intereactions
        let mut end_tx_idx: HashMap<usize, Vec<usize>> = HashMap::new();

        for (idx, interaction) in self.interactions_list().iter().enumerate() {
            match &interaction.interaction {
                InteractionType::Query(Query::Begin(..)) => {
                    begin_idx
                        .entry(interaction.connection_index)
                        .or_insert_with(|| vec![idx]);
                }
                InteractionType::Query(Query::Commit(..))
                | InteractionType::Query(Query::Rollback(..)) => {
                    let last_begin = begin_idx
                        .get(&interaction.connection_index)
                        .and_then(|list| list.last())
                        .unwrap()
                        + 1;
                    if last_begin == idx {
                        end_tx_idx
                            .entry(interaction.connection_index)
                            .or_insert_with(|| vec![idx]);
                    }
                }
                _ => {}
            }
        }

        // remove interactions if its just a Begin Commit/Rollback with no queries in the middle
        let mut range_transactions = end_tx_idx
            .into_iter()
            .map(|(conn_index, list)| (conn_index, list.into_iter().peekable()))
            .collect::<HashMap<_, _>>();
        let mut idx = 0;
        self.retain_mut(|interactions| {
            let mut retain = true;

            let iter = range_transactions.get_mut(&interactions.connection_index);

            if let Some(iter) = iter {
                if let Some(txn_interaction_idx) = iter.peek().copied() {
                    if txn_interaction_idx == idx {
                        iter.next();
                    }
                    if txn_interaction_idx == idx || txn_interaction_idx.saturating_sub(1) == idx {
                        retain = false;
                    }
                }
            }

            idx += 1;
            retain
        });
    }
}
