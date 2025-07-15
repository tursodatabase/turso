use crate::model::query::Query;
use crate::{
    generation::{
        plan::{Interaction, InteractionPlan, Interactions},
        property::Property,
    },
    run_simulation,
    runner::execution::Execution,
    SandboxedResult, SimulatorEnv,
};
use std::sync::{Arc, Mutex};

impl InteractionPlan {
    /// Create a smaller interaction plan by deleting a property
    pub(crate) fn shrink_interaction_plan(&self, failing_execution: &Execution) -> InteractionPlan {
        // todo: this is a very naive implementation, next steps are;
        // - Shrink to multiple values by removing random interactions
        // - Shrink properties by removing their extensions, or shrinking their values
        let mut plan = self.clone();
        let failing_property = &self.plan[failing_execution.interaction_index];
        let mut depending_tables = failing_property.dependencies();

        let interactions = failing_property.interactions();

        {
            let mut idx = failing_execution.secondary_index;
            loop {
                match &interactions[idx] {
                    Interaction::Query(query) => {
                        depending_tables = query.dependencies();
                        break;
                    }
                    // Fault does not depend on
                    Interaction::Fault(..) => break,
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

        let before = self.plan.len();

        // Remove all properties after the failing one
        plan.plan.truncate(failing_execution.interaction_index + 1);

        let mut idx = 0;
        // Remove all properties that do not use the failing tables
        plan.plan.retain_mut(|interactions| {
            let retain = if idx == failing_execution.interaction_index {
                true
            } else {
                let mut has_table = interactions
                    .uses()
                    .iter()
                    .any(|t| depending_tables.contains(t));

                if has_table {
                    // Remove the extensional parts of the properties
                    if let Interactions::Property(p) = interactions {
                        match p {
                            Property::InsertValuesSelect { queries, .. }
                            | Property::DoubleCreateFailure { queries, .. }
                            | Property::DeleteSelect { queries, .. }
                            | Property::DropSelect { queries, .. } => {
                                queries.clear();
                            }
                            Property::SelectLimit { .. }
                            | Property::SelectSelectOptimizer { .. }
                            | Property::WhereTrueFalseNull { .. }
                            | Property::UNIONAllPreservesCardinality { .. }
                            | Property::FsyncNoWait { .. }
                            | Property::FaultyQuery { .. } => {}
                        }
                    }
                    // Check again after query clear if the interactions still uses the failing table
                    has_table = interactions
                        .uses()
                        .iter()
                        .any(|t| depending_tables.contains(t));
                }
                let is_fault = matches!(interactions, Interactions::Fault(..));
                is_fault
                    || (has_table
                        && !matches!(
                            interactions,
                            Interactions::Query(Query::Select(_))
                                | Interactions::Property(Property::SelectLimit { .. })
                                | Interactions::Property(Property::SelectSelectOptimizer { .. })
                        ))
            };
            idx += 1;
            retain
        });

        let after = plan.plan.len();

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
        let failing_property = &self.plan[failing_execution.interaction_index];

        let interactions = failing_property.interactions();

        {
            let mut idx = failing_execution.secondary_index;
            loop {
                match &interactions[idx] {
                    // Fault does not depend on
                    Interaction::Fault(..) => break,
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

        let before = self.plan.len();

        plan.plan.truncate(failing_execution.interaction_index + 1);

        // phase 1: shrink extensions
        for interaction in &mut plan.plan {
            if let Interactions::Property(property) = interaction {
                match property {
                    Property::InsertValuesSelect { queries, .. }
                    | Property::DoubleCreateFailure { queries, .. }
                    | Property::DeleteSelect { queries, .. }
                    | Property::DropSelect { queries, .. } => {
                        let mut temp_plan = InteractionPlan {
                            plan: queries
                                .iter()
                                .map(|q| Interactions::Query(q.clone()))
                                .collect(),
                        };

                        temp_plan = InteractionPlan::iterative_shrink(
                            temp_plan,
                            failing_execution,
                            result,
                            env.clone(),
                        );
                        //temp_plan = Self::shrink_queries(temp_plan, failing_execution, result, env);

                        *queries = temp_plan
                            .plan
                            .into_iter()
                            .filter_map(|i| match i {
                                Interactions::Query(q) => Some(q),
                                _ => None,
                            })
                            .collect();
                    }
                    Property::WhereTrueFalseNull { .. }
                    | Property::UNIONAllPreservesCardinality { .. }
                    | Property::SelectLimit { .. }
                    | Property::SelectSelectOptimizer { .. }
                    | Property::FaultyQuery { .. }
                    | Property::FsyncNoWait { .. } => {}
                }
            }
        }

        // phase 2: shrink the entire plan
        plan = Self::iterative_shrink(plan, failing_execution, result, env);

        let after = plan.plan.len();

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
    ) -> InteractionPlan {
        for i in (0..plan.plan.len()).rev() {
            if i == failing_execution.interaction_index {
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
                run_simulation(
                    env.clone(),
                    vec![Arc::new(test_plan.clone())],
                    last_execution.clone(),
                )
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
