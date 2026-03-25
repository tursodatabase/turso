use garde::Validate;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::num::NonZeroU32;

use sql_generation::generation::Opts;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Validate)]
#[serde(deny_unknown_fields, default)]
pub struct QueryProfile {
    #[garde(dive)]
    pub gen_opts: Opts,
    /// Produces a new `TableHasExpectedContent` after each freestanding DML statement
    #[garde(skip)]
    pub check_after_dml: bool,
    #[garde(skip)]
    pub select_weight: u32,
    #[garde(skip)]
    pub create_table_weight: u32,
    #[garde(skip)]
    pub create_index_weight: u32,
    #[garde(skip)]
    pub insert_weight: u32,
    #[garde(skip)]
    pub update_weight: u32,
    #[garde(skip)]
    pub delete_weight: u32,
    #[garde(skip)]
    pub drop_table_weight: u32,
    #[garde(skip)]
    pub alter_table_weight: u32,
    #[garde(skip)]
    pub drop_index: u32,
    #[garde(skip)]
    pub pragma_weight: u32,
}

impl Default for QueryProfile {
    fn default() -> Self {
        Self {
            gen_opts: Opts::default(),
            check_after_dml: true,
            select_weight: 60,
            create_table_weight: 15,
            create_index_weight: 5,
            insert_weight: 30,
            update_weight: 20,
            delete_weight: 20,
            drop_table_weight: 2,
            alter_table_weight: 2,
            drop_index: 2,
            pragma_weight: 2,
        }
    }
}

impl QueryProfile {
    pub fn expr_index_stress() -> Self {
        let mut profile = Self::default();
        profile.gen_opts.table.large_table.enable = false;
        profile.gen_opts.table.rowid_alias_prob = 0.5;
        profile.gen_opts.query.insert.min_rows = NonZeroU32::new(10).unwrap();
        profile.gen_opts.query.insert.max_rows = NonZeroU32::new(60).unwrap();
        profile.gen_opts.query.update.expr_index_update_prob = 0.95;
        profile.gen_opts.query.create_index.expr_term_prob = 0.8;
        profile.create_index_weight = 15;
        profile.update_weight = 40;
        profile
    }

    /// Attention: edit this function when another weight is added
    pub fn total_weight(&self) -> u32 {
        self.select_weight
            + self.create_table_weight
            + self.create_index_weight
            + self.insert_weight
            + self.update_weight
            + self.delete_weight
            + self.drop_table_weight
            + self.alter_table_weight
            + self.pragma_weight
    }
}

#[derive(Debug, Clone, strum::VariantArray)]
pub enum QueryTypes {
    CreateTable,
    CreateIndex,
    Insert,
    Update,
    Delete,
    DropTable,
}
