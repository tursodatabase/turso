use std::{
    fmt::Display,
    num::{NonZero, NonZeroU32},
    ops::Range,
};

use garde::Validate;
use rand::distr::weighted::WeightedIndex;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::model::table::Table;

/// Trait used to provide context to generation functions
pub trait GenerationContext {
    fn tables(&self) -> &Vec<Table>;
    fn opts(&self) -> &Opts;
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Validate, Default)]
#[serde(deny_unknown_fields, default)]
pub struct Opts {
    #[garde(dive)]
    pub table: TableOpts,
    #[garde(dive)]
    pub query: QueryOpts,
    #[garde(skip)]
    /// Generate arbitrary INSERT INTO ... SELECT queries. This is disabled by default, as it makes
    /// the simulator very slow and generates huge databases.
    pub arbitrary_insert_into_select: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Validate)]
#[serde(deny_unknown_fields, default)]
pub struct TableOpts {
    #[garde(dive)]
    pub large_table: LargeTableOpts,
    #[garde(range(min = 0.0, max = 1.0))]
    #[serde(alias = "rowid_style_prob")]
    pub rowid_alias_prob: f64,
    /// Range of numbers of columns to generate
    #[garde(custom(range_struct_min(1)))]
    pub column_range: Range<u32>,
}

impl Default for TableOpts {
    fn default() -> Self {
        Self {
            large_table: Default::default(),
            rowid_alias_prob: 0.05,
            // Up to 10 columns
            column_range: 1..11,
        }
    }
}

/// Options for generating large tables
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Validate)]
#[serde(deny_unknown_fields, default)]
pub struct LargeTableOpts {
    #[garde(skip)]
    pub enable: bool,
    #[garde(range(min = 0.0, max = 1.0))]
    pub large_table_prob: f64,

    /// Range of numbers of columns to generate
    #[garde(custom(range_struct_min(1)))]
    pub column_range: Range<u32>,
}

impl Default for LargeTableOpts {
    fn default() -> Self {
        Self {
            enable: true,
            large_table_prob: 0.1,
            // todo: make this higher (128+)
            column_range: 64..125,
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, JsonSchema, Validate)]
#[serde(deny_unknown_fields, default)]
pub struct QueryOpts {
    #[garde(dive)]
    pub select: SelectOpts,
    #[garde(dive)]
    pub from_clause: FromClauseOpts,
    #[garde(dive)]
    pub insert: InsertOpts,
    #[garde(dive)]
    pub update: UpdateOpts,
    #[garde(dive)]
    pub alter_table: AlterTableOpts,
    #[garde(dive)]
    pub create_index: CreateIndexOpts,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Validate)]
#[serde(deny_unknown_fields, default)]
pub struct SelectOpts {
    #[garde(range(min = 0.0, max = 1.0))]
    pub order_by_prob: f64,
    #[garde(length(min = 1))]
    pub compound_selects: Vec<CompoundSelectWeight>,
}

impl Default for SelectOpts {
    fn default() -> Self {
        Self {
            order_by_prob: 0.3,
            compound_selects: vec![
                CompoundSelectWeight {
                    num_compound_selects: 0,
                    weight: 95,
                },
                CompoundSelectWeight {
                    num_compound_selects: 1,
                    weight: 4,
                },
                CompoundSelectWeight {
                    num_compound_selects: 2,
                    weight: 1,
                },
            ],
        }
    }
}

impl SelectOpts {
    pub fn compound_select_weighted_index(&self) -> WeightedIndex<u32> {
        WeightedIndex::new(self.compound_selects.iter().map(|weight| weight.weight)).unwrap()
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CompoundSelectWeight {
    pub num_compound_selects: u32,
    pub weight: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Validate)]
#[serde(deny_unknown_fields)]
pub struct FromClauseOpts {
    #[garde(length(min = 1))]
    pub joins: Vec<JoinWeight>,
}

impl Default for FromClauseOpts {
    fn default() -> Self {
        Self {
            joins: vec![
                JoinWeight {
                    num_joins: 0,
                    weight: 90,
                },
                JoinWeight {
                    num_joins: 1,
                    weight: 7,
                },
                JoinWeight {
                    num_joins: 2,
                    weight: 3,
                },
            ],
        }
    }
}

impl FromClauseOpts {
    pub fn as_weighted_index(&self) -> WeightedIndex<u32> {
        WeightedIndex::new(self.joins.iter().map(|weight| weight.weight)).unwrap()
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct JoinWeight {
    pub num_joins: u32,
    pub weight: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Validate)]
#[serde(deny_unknown_fields, default)]
pub struct InsertOpts {
    #[garde(skip)]
    pub min_rows: NonZeroU32,
    #[garde(skip)]
    pub max_rows: NonZeroU32,
    #[garde(range(min = 0.0, max = 1.0))]
    pub upsert_prob: f64,
}

impl Default for InsertOpts {
    fn default() -> Self {
        Self {
            min_rows: NonZero::new(1).unwrap(),
            max_rows: NonZero::new(10).unwrap(),
            upsert_prob: 0.15,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, Validate)]
#[serde(deny_unknown_fields, default)]
pub struct UpdateOpts {
    #[garde(skip)]
    pub padding_size: Option<usize>,
    #[garde(skip)]
    pub force_late_failure: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Validate)]
#[serde(deny_unknown_fields)]
pub struct AlterTableOpts {
    #[garde(skip)]
    pub alter_column: bool,
}

#[expect(clippy::derivable_impls)]
impl Default for AlterTableOpts {
    fn default() -> Self {
        Self {
            alter_column: Default::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Validate)]
#[serde(deny_unknown_fields, default)]
pub struct CreateIndexOpts {
    #[garde(range(min = 0.0, max = 1.0))]
    pub expr_term_prob: f64,
    #[garde(range(min = 0, max = 10))]
    pub max_expr_terms: usize,
}

impl Default for CreateIndexOpts {
    fn default() -> Self {
        Self {
            expr_term_prob: 0.15,
            max_expr_terms: 2,
        }
    }
}

fn range_struct_min<T: PartialOrd + Display>(
    min: T,
) -> impl FnOnce(&Range<T>, &()) -> garde::Result {
    move |value, _| {
        if value.start < min {
            return Err(garde::Error::new(format!(
                "range start `{}` is smaller than {min}",
                value.start
            )));
        } else if value.end < min {
            return Err(garde::Error::new(format!(
                "range end `{}` is smaller than {min}",
                value.end
            )));
        }
        Ok(())
    }
}

#[expect(dead_code)]
fn range_struct_max<T: PartialOrd + Display>(
    max: T,
) -> impl FnOnce(&Range<T>, &()) -> garde::Result {
    move |value, _| {
        if value.start > max {
            return Err(garde::Error::new(format!(
                "range start `{}` is smaller than {max}",
                value.start
            )));
        } else if value.end > max {
            return Err(garde::Error::new(format!(
                "range end `{}` is smaller than {max}",
                value.end
            )));
        }
        Ok(())
    }
}
