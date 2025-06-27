use crate::model::query::{select::Select, Query};
use crate::{
    generation::{
        plan::{Interaction, InteractionPlan, Interactions},
        property::Property,
    },
    run_simulation,
    runner::{cli::SimulatorCLI, execution::Execution},
    SandboxedResult, SimulatorEnv,
};
use clap::Parser;
use std::path::Path;
use std::sync::{Arc, Mutex};

impl InteractionPlan {
    /// Create a smaller interaction plan by deleting a property
    pub(crate) fn shrink_interaction_plan(
        &self,
        failing_execution: &Execution,
        env: &SimulatorEnv,
        result: &SandboxedResult,
    ) -> InteractionPlan {
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

        // Remove all properties that do not use the failing tables
        plan.plan
            .retain(|p| p.uses().iter().any(|t| depending_tables.contains(t)));

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

                        temp_plan =
                            InteractionPlan::iterative_shrink(temp_plan, failing_execution, result);
                        temp_plan = Self::shrink_queries(temp_plan, failing_execution, result, env);

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
        plan = Self::iterative_shrink(plan, failing_execution, result);
        // phase 3: shrink individual queries
        plan = Self::shrink_queries(plan, failing_execution, result, env);

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
    ) -> InteractionPlan {
        for i in (0..plan.plan.len()).rev() {
            if i == failing_execution.interaction_index {
                continue;
            }
            let mut test_plan = plan.clone();

            test_plan.plan.remove(i);

            if Self::test_shrunk_plan(&test_plan, failing_execution, old_result) {
                plan = test_plan;
            }
        }
        plan
    }

    fn shrink_queries(
        mut plan: InteractionPlan,
        failing_execution: &Execution,
        old_result: &SandboxedResult,
        env: &SimulatorEnv,
    ) -> InteractionPlan {
        for i in 0..plan.plan.len() {
            if let Interactions::Query(query) = &plan.plan[i] {
                match query {
                    Query::Select(Select {
                        table,
                        result_columns,
                        ..
                    }) => {
                        let mut current_cols =
                            if let crate::model::query::select::ResultColumn::Star =
                                &result_columns[0]
                            {
                                let column_names = Self::get_table_column_names(table, env);
                                column_names
                                    .into_iter()
                                    .map(|name| {
                                        crate::model::query::select::ResultColumn::Column(name)
                                    })
                                    .collect::<Vec<_>>()
                            } else {
                                result_columns.clone()
                            };

                        // try removing columns one by one while preserving the bug
                        while current_cols.len() > 1 {
                            let mut found_shrink = false;
                            for remove_idx in 0..current_cols.len() {
                                let mut candidate = current_cols.clone();
                                candidate.remove(remove_idx);

                                let mut test_plan = plan.clone();
                                if let Interactions::Query(Query::Select(Select {
                                    result_columns: test_columns,
                                    ..
                                })) = &mut test_plan.plan[i]
                                {
                                    *test_columns = candidate.clone();
                                }
                                if Self::test_shrunk_plan(&test_plan, failing_execution, old_result)
                                {
                                    current_cols = candidate;
                                    found_shrink = true;
                                    break;
                                }
                            }
                            if !found_shrink {
                                break;
                            }
                        }

                        // update the plan with the shrunk columns
                        if let Interactions::Query(Query::Select(Select {
                            result_columns, ..
                        })) = &mut plan.plan[i]
                        {
                            tracing::info!("Shrunk query {} to {:?}", i, current_cols);
                            *result_columns = current_cols;
                        }
                    }
                    Query::Insert { .. } => {
                        // todo: shrink insert queries
                    }
                    Query::Update { .. } => {
                        // todo: shrink update queries
                    }
                    _ => {}
                }
            }
        }

        plan
    }

    fn test_shrunk_plan(
        test_plan: &InteractionPlan,
        failing_execution: &Execution,
        old_result: &SandboxedResult,
    ) -> bool {
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

        let result = SandboxedResult::from(
            std::panic::catch_unwind(|| {
                run_simulation(
                    env.clone(),
                    &mut [test_plan.clone()],
                    last_execution.clone(),
                )
            }),
            last_execution.clone(),
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
    fn get_table_column_names(table_name: &str, env: &SimulatorEnv) -> Vec<String> {
        env.tables
            .iter()
            .find(|t| t.name == table_name)
            .map(|table| table.columns.iter().map(|col| col.name.clone()).collect())
            .unwrap_or_default()
    }
}
