use garde::Validate;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sql_generation::generation::Opts;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Validate)]
#[serde(deny_unknown_fields, default)]
pub struct QueryProfile {
    #[garde(dive)]
    pub gen_opts: Opts,
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
}

impl Default for QueryProfile {
    fn default() -> Self {
        Self {
            gen_opts: Opts::default(),
            select_weight: 60,
            create_table_weight: 15,
            create_index_weight: 5,
            insert_weight: 30,
            update_weight: 20,
            delete_weight: 20,
            drop_table_weight: 2,
        }
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
