use crate::{
    generation::{gen_random_text, pick, pick_n_unique, ArbitraryFrom},
    runner::env::SimulatorEnv,
};
use rand::Rng;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum SortOrder {
    Asc,
    Desc,
}

impl std::fmt::Display for SortOrder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SortOrder::Asc => write!(f, "ASC"),
            SortOrder::Desc => write!(f, "DESC"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(crate) struct CreateIndex {
    pub(crate) index_name: String,
    pub(crate) table_name: String,
    pub(crate) columns: Vec<(String, SortOrder)>,
}

impl CreateIndex {
    pub(crate) fn shadow(
        &self,
        _env: &mut crate::runner::env::SimulatorEnv,
    ) -> Vec<Vec<crate::model::table::Value>> {
        // CREATE INDEX doesn't require any shadowing; we don't need to keep track
        // in the simulator what indexes exist.
        vec![]
    }
}

impl std::fmt::Display for CreateIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CREATE INDEX {} ON {} ({})",
            self.index_name,
            self.table_name,
            self.columns
                .iter()
                .map(|(name, order)| format!("{} {}", name, order))
                .collect::<Vec<String>>()
                .join(", ")
        )
    }
}

impl ArbitraryFrom<&SimulatorEnv> for CreateIndex {
    fn arbitrary_from<R: Rng>(rng: &mut R, env: &SimulatorEnv) -> Self {
        assert!(
            !env.tables.is_empty(),
            "Cannot create an index when no tables exist in the environment."
        );

        let table = pick(&env.tables, rng);

        if table.columns.is_empty() {
            panic!(
                "Cannot create an index on table '{}' as it has no columns.",
                table.name
            );
        }

        let num_columns_to_pick = rng.gen_range(1..=table.columns.len());
        let picked_column_indices = pick_n_unique(0..table.columns.len(), num_columns_to_pick, rng);

        let columns = picked_column_indices
            .into_iter()
            .map(|i| {
                let column = &table.columns[i];
                (
                    column.name.clone(),
                    if rng.gen_bool(0.5) {
                        SortOrder::Asc
                    } else {
                        SortOrder::Desc
                    },
                )
            })
            .collect::<Vec<(String, SortOrder)>>();

        let index_name = format!(
            "idx_{}_{}",
            table.name,
            gen_random_text(rng).chars().take(8).collect::<String>()
        );

        CreateIndex {
            index_name,
            table_name: table.name.clone(),
            columns,
        }
    }
}
