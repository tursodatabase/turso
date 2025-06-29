use turso_sim::{
    generation::{
        plan::{Interaction, InteractionPlan, Interactions},
        property::Property,
    },
    model::query::Query,
};

use crate::runner::execution::Execution;

/// Create a smaller interaction plan by deleting a property
pub(crate) fn shrink_interaction_plan(
    curr_plan: &InteractionPlan,
    failing_execution: &Execution,
) -> InteractionPlan {
    // todo: this is a very naive implementation, next steps are;
    // - Shrink to multiple values by removing random interactions
    // - Shrink properties by removing their extensions, or shrinking their values
    let mut plan = curr_plan.clone();
    let failing_property = &curr_plan.plan[failing_execution.interaction_index];
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

    let before = curr_plan.plan.len();

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
                        Property::SelectLimit { .. } | Property::SelectSelectOptimizer { .. }
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
            has_table
                && !matches!(
                    interactions,
                    Interactions::Query(Query::Select(_))
                        | Interactions::Property(Property::SelectLimit { .. })
                        | Interactions::Property(Property::SelectSelectOptimizer { .. })
                )
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
