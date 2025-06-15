use crate::{
    generation::{
        plan::{Interaction, InteractionPlan, Interactions},
        property::Property,
    },
    run_simulation,
    runner::cli::SimulatorCLI,
    runner::execution::Execution,
    SimulatorEnv,
};
use clap::Parser;
use std::path::Path;
use std::sync::{Arc, Mutex};

impl InteractionPlan {
    /// Create a smaller interaction plan by deleting a property
    pub(crate) fn shrink_interaction_plan(&self, failing_execution: &Execution) -> InteractionPlan {
        // todo: this is a very naive implementation, next steps are;
        // - Shrink interaction plans more efficiently (i.e. tracking dependencies between queries)
        // - Shrink values
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

                        temp_plan = InteractionPlan::iterative_shrink(temp_plan, failing_execution);

                        *queries = temp_plan
                            .plan
                            .into_iter()
                            .filter_map(|i| match i {
                                Interactions::Query(q) => Some(q),
                                _ => None,
                            })
                            .collect();
                    }
                    Property::SelectLimit { .. } | Property::SelectSelectOptimizer { .. } => {}
                }
            }
        }

        // phase 2: shrink the entire plan
        plan = Self::iterative_shrink(plan, failing_execution);

        // Remove all properties that do not use the failing tables
        plan.plan
            .retain(|p| p.uses().iter().any(|t| depending_tables.contains(t)));

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
    ) -> InteractionPlan {
        for i in (0..plan.plan.len()).rev() {
            if i == failing_execution.interaction_index {
                continue;
            }
            let mut test_plan = plan.clone();

            test_plan.plan.remove(i);

            // run the test plan to see if it reproduces the error
            let cli_opts = SimulatorCLI {
                seed: Some(0),
                ..SimulatorCLI::parse()
            };
            let env = Arc::new(Mutex::new(SimulatorEnv::new(
                0,
                &cli_opts,
                Path::new("test.db"),
            )));
            let last_execution = Arc::new(Mutex::new(*failing_execution));

            let result = std::panic::catch_unwind(|| {
                run_simulation(
                    env.clone(),
                    &mut [test_plan.clone()],
                    last_execution.clone(),
                )
            });

            if let Ok(execution_result) = result {
                if let Some(_) = execution_result.error {
                    // if we get the same error, shrink is valid
                    plan = test_plan;
                    continue;
                }
            }
        }
        plan
    }
}
