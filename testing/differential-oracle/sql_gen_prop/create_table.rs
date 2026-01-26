//! CREATE TABLE statement type and generation strategy.

use anarchist_readable_name_generator_lib::readable_name_custom;
use proptest::prelude::*;
use std::collections::HashSet;
use std::fmt;
use std::ops::RangeInclusive;

use crate::profile::StatementProfile;
use crate::schema::{Collation, ColumnDef, DataType, GeneratedColumn, GeneratedStorage, Schema};

// =============================================================================
// DATA TYPE WEIGHTS
// =============================================================================

/// Weights for controlling data type generation distribution.
#[derive(Debug, Clone)]
pub struct DataTypeWeights {
    /// Weight for INTEGER type.
    pub integer: u32,
    /// Weight for REAL type.
    pub real: u32,
    /// Weight for TEXT type.
    pub text: u32,
    /// Weight for BLOB type.
    pub blob: u32,
}

impl Default for DataTypeWeights {
    fn default() -> Self {
        Self {
            integer: 30,
            real: 20,
            text: 35,
            blob: 15,
        }
    }
}

impl DataTypeWeights {
    /// Builder method to create weights for only integer types.
    pub fn integer_only(self) -> Self {
        Self {
            integer: 100,
            real: 0,
            text: 0,
            blob: 0,
        }
    }

    /// Builder method to set integer weight.
    pub fn with_integer(mut self, weight: u32) -> Self {
        self.integer = weight;
        self
    }

    /// Builder method to set real weight.
    pub fn with_real(mut self, weight: u32) -> Self {
        self.real = weight;
        self
    }

    /// Builder method to set text weight.
    pub fn with_text(mut self, weight: u32) -> Self {
        self.text = weight;
        self
    }

    /// Builder method to set blob weight.
    pub fn with_blob(mut self, weight: u32) -> Self {
        self.blob = weight;
        self
    }

    /// Returns an iterator over enabled data types with their weights.
    pub fn enabled_types(&self) -> impl Iterator<Item = (DataType, u32)> {
        [
            (DataType::Integer, self.integer),
            (DataType::Real, self.real),
            (DataType::Text, self.text),
            (DataType::Blob, self.blob),
        ]
        .into_iter()
        .filter(|(_, w)| *w > 0)
    }
}

// =============================================================================
// COLUMN PROFILE
// =============================================================================

/// Profile for controlling column definition generation.
#[derive(Debug, Clone)]
pub struct ColumnProfile {
    /// Probability (0-100) that a non-PK column is NOT NULL.
    pub not_null_probability: u8,
    /// Probability (0-100) that a non-PK column has UNIQUE constraint.
    pub unique_probability: u8,
    /// Probability (0-100) that a column has a DEFAULT value.
    pub default_probability: u8,
    /// Weights for data type generation.
    pub data_type_weights: DataTypeWeights,
    /// Probability (0-100) that a non-PK column is a generated (computed) column.
    pub generated_probability: u8,
    /// Probability (0-100) that a generated column is STORED (vs VIRTUAL).
    pub generated_stored_probability: u8,
    /// Probability (0-100) that a generated column has COLLATE on TEXT types.
    pub generated_collation_probability: u8,
    /// Probability (0-100) of generating an intentionally invalid expression
    /// for error testing. Default 0 (all expressions valid).
    pub invalid_generated_expr_probability: u8,
}

impl Default for ColumnProfile {
    fn default() -> Self {
        Self {
            not_null_probability: 30,
            unique_probability: 10,
            default_probability: 15,
            data_type_weights: DataTypeWeights::default(),
            generated_probability: 25,
            generated_stored_probability: 50,
            generated_collation_probability: 10,
            invalid_generated_expr_probability: 0,
        }
    }
}

impl ColumnProfile {
    /// Builder method to create a profile where all columns are nullable with no constraints.
    pub fn minimal(self) -> Self {
        Self {
            not_null_probability: 0,
            unique_probability: 0,
            default_probability: 0,
            generated_probability: 0,
            ..self
        }
    }

    /// Builder method to create a profile with strict constraints (many NOT NULL and UNIQUE).
    pub fn strict(self) -> Self {
        Self {
            not_null_probability: 70,
            unique_probability: 30,
            default_probability: 20,
            ..self
        }
    }

    /// Builder method to create a profile with all constraints enabled at maximum probability.
    pub fn full_constraints(self) -> Self {
        Self {
            not_null_probability: 100,
            unique_probability: 50,
            default_probability: 40,
            ..self
        }
    }

    /// Builder method to create a profile with many generated columns for targeted testing.
    pub fn generated_heavy(self) -> Self {
        Self {
            generated_probability: 60,
            generated_stored_probability: 50,
            invalid_generated_expr_probability: 0,
            ..self
        }
    }

    /// Builder method for heavy generated column testing with some invalid expressions.
    pub fn generated_heavy_with_errors(self) -> Self {
        Self {
            generated_probability: 60,
            generated_stored_probability: 50,
            invalid_generated_expr_probability: 30,
            ..self
        }
    }

    /// Builder method for testing error handling with all invalid generated expressions.
    pub fn generated_all_invalid(self) -> Self {
        Self {
            generated_probability: 60,
            invalid_generated_expr_probability: 100,
            ..self
        }
    }

    /// Builder method to set NOT NULL probability.
    pub fn with_not_null_probability(mut self, probability: u8) -> Self {
        self.not_null_probability = probability.min(100);
        self
    }

    /// Builder method to set UNIQUE probability.
    pub fn with_unique_probability(mut self, probability: u8) -> Self {
        self.unique_probability = probability.min(100);
        self
    }

    /// Builder method to set DEFAULT probability.
    pub fn with_default_probability(mut self, probability: u8) -> Self {
        self.default_probability = probability.min(100);
        self
    }

    /// Builder method to set data type weights.
    pub fn with_data_type_weights(mut self, weights: DataTypeWeights) -> Self {
        self.data_type_weights = weights;
        self
    }

    /// Builder method to set generated column probability.
    pub fn with_generated_probability(mut self, probability: u8) -> Self {
        self.generated_probability = probability.min(100);
        self
    }

    /// Builder method to set generated STORED probability (vs VIRTUAL).
    pub fn with_generated_stored_probability(mut self, probability: u8) -> Self {
        self.generated_stored_probability = probability.min(100);
        self
    }

    /// Builder method to set invalid generated expression probability for error testing.
    pub fn with_invalid_generated_expr_probability(mut self, probability: u8) -> Self {
        self.invalid_generated_expr_probability = probability.min(100);
        self
    }
}

// =============================================================================
// PRIMARY KEY PROFILE
// =============================================================================

/// Profile for controlling primary key generation.
#[derive(Debug, Clone)]
pub struct PrimaryKeyProfile {
    /// Whether to always include a primary key column.
    pub always_include: bool,
    /// Probability (0-100) of including a primary key when not always_include.
    pub include_probability: u8,
    /// Whether primary key should be INTEGER (enables ROWID alias optimization).
    pub prefer_integer: bool,
    /// Weights for data types allowed for primary keys.
    pub data_type_weights: DataTypeWeights,
}

impl Default for PrimaryKeyProfile {
    fn default() -> Self {
        Self {
            always_include: true,
            include_probability: 90,
            prefer_integer: true,
            data_type_weights: DataTypeWeights::default()
                .with_integer(60)
                .with_text(30)
                .with_real(5)
                .with_blob(5),
        }
    }
}

impl PrimaryKeyProfile {
    /// Builder method to create a profile that always uses INTEGER PRIMARY KEY.
    pub fn integer_only(self) -> Self {
        Self {
            always_include: true,
            include_probability: 100,
            prefer_integer: true,
            data_type_weights: self.data_type_weights.integer_only(),
        }
    }

    /// Builder method to create a profile that may omit primary keys.
    pub fn optional(self) -> Self {
        Self {
            always_include: false,
            include_probability: 70,
            prefer_integer: true,
            data_type_weights: self.data_type_weights,
        }
    }

    /// Builder method to create a profile with no primary keys.
    pub fn none(self) -> Self {
        Self {
            always_include: false,
            include_probability: 0,
            prefer_integer: false,
            data_type_weights: self.data_type_weights,
        }
    }

    /// Builder method to set always_include.
    pub fn with_always_include(mut self, always: bool) -> Self {
        self.always_include = always;
        self
    }

    /// Builder method to set include probability.
    pub fn with_include_probability(mut self, probability: u8) -> Self {
        self.include_probability = probability.min(100);
        self
    }

    /// Builder method to set prefer_integer.
    pub fn with_prefer_integer(mut self, prefer: bool) -> Self {
        self.prefer_integer = prefer;
        self
    }

    /// Builder method to set data type weights.
    pub fn with_data_type_weights(mut self, weights: DataTypeWeights) -> Self {
        self.data_type_weights = weights;
        self
    }
}

// =============================================================================
// CREATE TABLE PROFILE
// =============================================================================

/// Profile for controlling CREATE TABLE statement generation.
#[derive(Debug, Clone)]
pub struct CreateTableProfile {
    /// Pattern for table name generation (regex).
    pub identifier_pattern: String,
    /// Range for number of non-PK columns.
    pub column_count_range: RangeInclusive<usize>,
    /// Probability (0-100) of generating IF NOT EXISTS clause.
    pub if_not_exists_probability: u8,
    /// Profile for primary key generation.
    pub primary_key: PrimaryKeyProfile,
    /// Profile for non-PK column generation.
    pub column: ColumnProfile,
}

impl Default for CreateTableProfile {
    fn default() -> Self {
        Self {
            identifier_pattern: "[a-z][a-z0-9_]{0,30}".to_string(),
            column_count_range: 0..=10,
            if_not_exists_probability: 50,
            primary_key: PrimaryKeyProfile::default(),
            column: ColumnProfile::default(),
        }
    }
}

impl CreateTableProfile {
    /// Builder method to create a profile for small tables with minimal constraints.
    pub fn small(self) -> Self {
        Self {
            identifier_pattern: "[a-z][a-z0-9_]{0,15}".to_string(),
            column_count_range: 1..=3,
            if_not_exists_probability: 50,
            primary_key: self.primary_key.integer_only(),
            column: self.column.minimal(),
        }
    }

    /// Builder method to create a profile for large tables with varied constraints.
    pub fn large(self) -> Self {
        Self {
            identifier_pattern: "[a-z][a-z0-9_]{0,30}".to_string(),
            column_count_range: 5..=20,
            if_not_exists_probability: 30,
            primary_key: self.primary_key,
            column: self.column.strict(),
        }
    }

    /// Builder method to create a profile for strict schema with many constraints.
    pub fn strict(self) -> Self {
        Self {
            identifier_pattern: "[a-z][a-z0-9_]{0,30}".to_string(),
            column_count_range: 2..=8,
            if_not_exists_probability: 20,
            primary_key: self.primary_key.integer_only(),
            column: self.column.full_constraints(),
        }
    }

    /// Builder method to create a profile for tables without primary keys.
    pub fn no_primary_key(self) -> Self {
        Self {
            identifier_pattern: "[a-z][a-z0-9_]{0,30}".to_string(),
            column_count_range: 1..=10,
            if_not_exists_probability: 50,
            primary_key: self.primary_key.none(),
            column: self.column,
        }
    }

    /// Builder method to set identifier pattern.
    pub fn with_identifier_pattern(mut self, pattern: impl Into<String>) -> Self {
        self.identifier_pattern = pattern.into();
        self
    }

    /// Builder method to set column count range.
    pub fn with_column_count_range(mut self, range: RangeInclusive<usize>) -> Self {
        self.column_count_range = range;
        self
    }

    /// Builder method to set IF NOT EXISTS probability.
    pub fn with_if_not_exists_probability(mut self, probability: u8) -> Self {
        self.if_not_exists_probability = probability.min(100);
        self
    }

    /// Builder method to set primary key profile.
    pub fn with_primary_key(mut self, profile: PrimaryKeyProfile) -> Self {
        self.primary_key = profile;
        self
    }

    /// Builder method to set column profile.
    pub fn with_column(mut self, profile: ColumnProfile) -> Self {
        self.column = profile;
        self
    }
}

/// A CREATE TABLE statement.
#[derive(Debug, Clone)]
pub struct CreateTableStatement {
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
    pub if_not_exists: bool,
}

impl fmt::Display for CreateTableStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CREATE TABLE ")?;

        if self.if_not_exists {
            write!(f, "IF NOT EXISTS ")?;
        }

        write!(f, "{} (", self.table_name)?;

        let col_defs: Vec<String> = self.columns.iter().map(|c| c.to_string()).collect();
        write!(f, "{})", col_defs.join(", "))
    }
}

/// ReadableName strategy. Used to generate more readable identifiers
#[derive(Debug, Clone, Copy)]
struct ReadableName;

#[derive(Debug, Clone)]
struct ReadableNameTree(String);

impl prop::strategy::ValueTree for ReadableNameTree {
    type Value = String;

    fn current(&self) -> Self::Value {
        self.0.clone()
    }

    fn simplify(&mut self) -> bool {
        false
    }

    fn complicate(&mut self) -> bool {
        false
    }
}

impl Strategy for ReadableName {
    type Tree = ReadableNameTree;

    type Value = String;

    fn new_tree(
        &self,
        runner: &mut prop::test_runner::TestRunner,
    ) -> prop::strategy::NewTree<Self> {
        let name = readable_name_custom("_", runner.rng()).replace("-", "_");
        Ok(ReadableNameTree(name))
    }
}

/// Generate a valid SQL identifier.
pub fn identifier() -> impl Strategy<Value = String> {
    prop_oneof![
        3 => "[a-z][a-z0-9_]{0,30}".prop_filter("must not be empty", |s| !s.is_empty()),
        7 =>  ReadableName
    ]
}

/// Generate a valid SQL identifier that is not in the excluded set.
pub fn identifier_excluding(excluded: HashSet<String>) -> impl Strategy<Value = String> {
    identifier().prop_filter("must not be in excluded set", move |s| {
        !excluded.contains(s.as_str())
    })
}

/// Generate a data type with default uniform weights.
pub fn data_type() -> impl Strategy<Value = DataType> {
    prop_oneof![
        Just(DataType::Integer),
        Just(DataType::Real),
        Just(DataType::Text),
        Just(DataType::Blob),
    ]
}

/// Generate a data type with custom weights.
pub fn data_type_weighted(weights: &DataTypeWeights) -> BoxedStrategy<DataType> {
    let weighted_types: Vec<(u32, BoxedStrategy<DataType>)> = weights
        .enabled_types()
        .map(|(dt, w)| (w, Just(dt).boxed()))
        .collect();

    if weighted_types.is_empty() {
        // Fallback to INTEGER if all weights are zero
        Just(DataType::Integer).boxed()
    } else {
        proptest::strategy::Union::new_weighted(weighted_types).boxed()
    }
}

/// Generate a default value for a given data type.
fn default_value_for_type(data_type: DataType) -> BoxedStrategy<Option<String>> {
    match data_type {
        DataType::Integer => prop_oneof![
            Just(Some("0".to_string())),
            Just(Some("1".to_string())),
            Just(Some("-1".to_string())),
            (0i64..1000).prop_map(|n| Some(n.to_string())),
        ]
        .boxed(),
        DataType::Real => prop_oneof![
            Just(Some("0.0".to_string())),
            Just(Some("1.0".to_string())),
            (0.0f64..100.0).prop_map(|n| Some(format!("{n:.2}"))),
        ]
        .boxed(),
        DataType::Text => prop_oneof![
            Just(Some("''".to_string())),
            Just(Some("'default'".to_string())),
            "[a-z]{1,10}".prop_map(|s| Some(format!("'{s}'"))),
        ]
        .boxed(),
        DataType::Blob => prop_oneof![
            Just(Some("X''".to_string())),
            Just(Some("X'00'".to_string())),
        ]
        .boxed(),
        DataType::Null => Just(Some("NULL".to_string())).boxed(),
    }
}

// =============================================================================
// GENERATED COLUMN EXPRESSION GENERATION
// =============================================================================

/// Generate a valid expression for a generated column.
///
/// The expression can only reference columns that appear before this column
/// (provided in `prior_columns`). The expression must be deterministic.
fn generated_expr_for_type(
    data_type: DataType,
    prior_columns: &[ColumnDef],
) -> BoxedStrategy<String> {
    // Filter prior columns to those we can reference
    let referenceable: Vec<_> = prior_columns
        .iter()
        .filter(|c| !c.is_generated() || c.is_stored_generated())
        .collect();

    if referenceable.is_empty() {
        // No prior columns to reference, use a literal expression
        return literal_expr_for_type(data_type);
    }

    // Generate expressions based on target type
    match data_type {
        DataType::Integer => integer_generated_expr(&referenceable),
        DataType::Real => real_generated_expr(&referenceable),
        DataType::Text => text_generated_expr(&referenceable),
        DataType::Blob => blob_generated_expr(&referenceable),
        DataType::Null => Just("NULL".to_string()).boxed(),
    }
}

/// Generate a literal expression for a type (when no prior columns available).
fn literal_expr_for_type(data_type: DataType) -> BoxedStrategy<String> {
    match data_type {
        DataType::Integer => prop_oneof![
            Just("0".to_string()),
            Just("1".to_string()),
            Just("42".to_string()),
            (0i64..1000).prop_map(|n| n.to_string()),
        ]
        .boxed(),
        DataType::Real => prop_oneof![
            Just("0.0".to_string()),
            Just("1.0".to_string()),
            (0.0f64..100.0).prop_map(|n| format!("{n:.2}")),
        ]
        .boxed(),
        DataType::Text => prop_oneof![
            Just("''".to_string()),
            Just("'computed'".to_string()),
            "[a-z]{1,8}".prop_map(|s| format!("'{s}'")),
        ]
        .boxed(),
        DataType::Blob => prop_oneof![Just("X''".to_string()), Just("X'00'".to_string()),].boxed(),
        DataType::Null => Just("NULL".to_string()).boxed(),
    }
}

/// Generate an integer expression referencing prior columns.
fn integer_generated_expr(prior_columns: &[&ColumnDef]) -> BoxedStrategy<String> {
    let int_cols: Vec<_> = prior_columns
        .iter()
        .filter(|c| c.data_type == DataType::Integer)
        .map(|c| c.name.clone())
        .collect();

    if int_cols.is_empty() {
        // No integer columns, use CAST or LENGTH
        let text_cols: Vec<_> = prior_columns
            .iter()
            .filter(|c| c.data_type == DataType::Text)
            .map(|c| c.name.clone())
            .collect();

        if text_cols.is_empty() {
            return literal_expr_for_type(DataType::Integer);
        }

        return proptest::sample::select(text_cols)
            .prop_flat_map(|col| {
                prop_oneof![
                    Just(format!("LENGTH({col})")),
                    Just(format!("INSTR({col}, 'a')")),
                    Just(format!("CAST({col} AS INTEGER)")),
                ]
            })
            .boxed();
    }

    proptest::sample::select(int_cols.clone())
        .prop_flat_map(move |col| {
            prop_oneof![
                // Simple reference
                Just(col.clone()),
                // Arithmetic
                Just(format!("{col} + 1")),
                Just(format!("{col} * 2")),
                Just(format!("ABS({col})")),
                Just(format!("{col} % 10")),
                // Functions
                Just(format!("COALESCE({col}, 0)")),
                Just(format!("IFNULL({col}, -1)")),
                Just(format!("MAX({col}, 0)")),
            ]
        })
        .boxed()
}

/// Generate a real expression referencing prior columns.
fn real_generated_expr(prior_columns: &[&ColumnDef]) -> BoxedStrategy<String> {
    let real_cols: Vec<_> = prior_columns
        .iter()
        .filter(|c| c.data_type == DataType::Real)
        .map(|c| c.name.clone())
        .collect();

    let int_cols: Vec<_> = prior_columns
        .iter()
        .filter(|c| c.data_type == DataType::Integer)
        .map(|c| c.name.clone())
        .collect();

    if real_cols.is_empty() && int_cols.is_empty() {
        return literal_expr_for_type(DataType::Real);
    }

    let all_numeric: Vec<_> = real_cols.iter().chain(int_cols.iter()).cloned().collect();

    proptest::sample::select(all_numeric)
        .prop_flat_map(|col| {
            prop_oneof![
                Just(format!("CAST({col} AS REAL)")),
                Just(format!("{col} * 1.0")),
                Just(format!("{col} / 2.0")),
                Just(format!("ABS({col})")),
                Just(format!("ROUND({col}, 2)")),
                Just(format!("COALESCE({col}, 0.0)")),
            ]
        })
        .boxed()
}

/// Generate a text expression referencing prior columns.
fn text_generated_expr(prior_columns: &[&ColumnDef]) -> BoxedStrategy<String> {
    let text_cols: Vec<_> = prior_columns
        .iter()
        .filter(|c| c.data_type == DataType::Text)
        .map(|c| c.name.clone())
        .collect();

    let int_cols: Vec<_> = prior_columns
        .iter()
        .filter(|c| c.data_type == DataType::Integer)
        .map(|c| c.name.clone())
        .collect();

    if text_cols.is_empty() && int_cols.is_empty() {
        return literal_expr_for_type(DataType::Text);
    }

    if !text_cols.is_empty() {
        proptest::sample::select(text_cols.clone())
            .prop_flat_map(move |col| {
                prop_oneof![
                    // Simple reference
                    Just(col.clone()),
                    // String functions
                    Just(format!("UPPER({col})")),
                    Just(format!("LOWER({col})")),
                    Just(format!("TRIM({col})")),
                    Just(format!("'{col}: ' || {col}")),
                    Just(format!("COALESCE({col}, '')")),
                    Just(format!("SUBSTR({col}, 1, 10)")),
                    Just(format!("REPLACE({col}, 'a', 'A')")),
                ]
            })
            .boxed()
    } else {
        // Only integer columns available, convert to text
        proptest::sample::select(int_cols)
            .prop_flat_map(|col| {
                prop_oneof![
                    Just(format!("CAST({col} AS TEXT)")),
                    Just(format!("'#' || {col}")),
                    Just(format!("PRINTF('%d', {col})")),
                ]
            })
            .boxed()
    }
}

/// Generate a blob expression referencing prior columns.
fn blob_generated_expr(prior_columns: &[&ColumnDef]) -> BoxedStrategy<String> {
    let blob_cols: Vec<_> = prior_columns
        .iter()
        .filter(|c| c.data_type == DataType::Blob)
        .map(|c| c.name.clone())
        .collect();

    let text_cols: Vec<_> = prior_columns
        .iter()
        .filter(|c| c.data_type == DataType::Text)
        .map(|c| c.name.clone())
        .collect();

    if !blob_cols.is_empty() {
        proptest::sample::select(blob_cols)
            .prop_map(|col| format!("COALESCE({col}, X'')"))
            .boxed()
    } else if !text_cols.is_empty() {
        // Convert text to blob
        proptest::sample::select(text_cols)
            .prop_map(|col| format!("CAST({col} AS BLOB)"))
            .boxed()
    } else {
        literal_expr_for_type(DataType::Blob)
    }
}

/// Generate an intentionally invalid expression for error testing.
fn invalid_generated_expr() -> BoxedStrategy<String> {
    prop_oneof![
        // Subquery (not allowed in generated columns)
        Just("(SELECT 1)".to_string()),
        // Non-deterministic function
        Just("RANDOM()".to_string()),
        Just("datetime('now')".to_string()),
        // Aggregate function
        Just("SUM(1)".to_string()),
        Just("COUNT(*)".to_string()),
        // Window function
        Just("ROW_NUMBER() OVER ()".to_string()),
    ]
    .boxed()
}

/// Generate storage type for a generated column.
fn generated_storage(stored_probability: u8) -> BoxedStrategy<GeneratedStorage> {
    (0u8..100)
        .prop_map(move |roll| {
            if roll < stored_probability {
                GeneratedStorage::Stored
            } else {
                GeneratedStorage::Virtual
            }
        })
        .boxed()
}

/// Generate optional collation for a TEXT generated column.
fn generated_collation(collation_probability: u8) -> BoxedStrategy<Option<Collation>> {
    (0u8..100)
        .prop_flat_map(move |roll| {
            if roll < collation_probability {
                prop_oneof![
                    Just(Some(Collation::NoCase)),
                    Just(Some(Collation::RTrim)),
                    Just(Some(Collation::Binary)),
                ]
                .boxed()
            } else {
                Just(None).boxed()
            }
        })
        .boxed()
}

// =============================================================================
// COLUMN GENERATION WITH GENERATED COLUMN SUPPORT
// =============================================================================

/// Generate a column definition that may be a generated column.
/// Takes prior columns so generated expressions can reference them.
pub fn column_def_with_profile_and_context(
    profile: &ColumnProfile,
    prior_columns: Vec<ColumnDef>,
) -> BoxedStrategy<ColumnDef> {
    let not_null_prob = profile.not_null_probability;
    let unique_prob = profile.unique_probability;
    let default_prob = profile.default_probability;
    let generated_prob = profile.generated_probability;
    let stored_prob = profile.generated_stored_probability;
    let collation_prob = profile.generated_collation_probability;
    let invalid_prob = profile.invalid_generated_expr_probability;
    let data_type_weights = profile.data_type_weights.clone();

    (
        identifier(),
        data_type_weighted(&data_type_weights),
        0u8..100, // for NOT NULL decision
        0u8..100, // for UNIQUE decision
        0u8..100, // for DEFAULT decision
        0u8..100, // for GENERATED decision
        0u8..100, // for INVALID expr decision
    )
        .prop_flat_map(
            move |(name, dt, not_null_roll, unique_roll, default_roll, gen_roll, invalid_roll)| {
                let nullable = not_null_roll >= not_null_prob;
                let unique = unique_roll < unique_prob;
                let is_generated = gen_roll < generated_prob;
                let prior_cols = prior_columns.clone();

                if is_generated {
                    // Generate a generated column
                    let use_invalid = invalid_roll < invalid_prob;
                    let expr_strategy = if use_invalid {
                        invalid_generated_expr()
                    } else {
                        generated_expr_for_type(dt, &prior_cols)
                    };

                    let collation_strategy = if dt == DataType::Text {
                        generated_collation(collation_prob)
                    } else {
                        Just(None).boxed()
                    };

                    (
                        expr_strategy,
                        generated_storage(stored_prob),
                        collation_strategy,
                    )
                        .prop_map(move |(expr, storage, collation)| ColumnDef {
                            name: name.clone(),
                            data_type: dt,
                            nullable,
                            primary_key: false,
                            unique,
                            default: None, // Generated columns cannot have DEFAULT
                            generated: Some(GeneratedColumn {
                                expr,
                                storage,
                                collation,
                            }),
                        })
                        .boxed()
                } else {
                    // Generate a regular column
                    let has_default = default_roll < default_prob;
                    let default_strategy = if has_default {
                        default_value_for_type(dt)
                    } else {
                        Just(None).boxed()
                    };

                    default_strategy
                        .prop_map(move |default| ColumnDef {
                            name: name.clone(),
                            data_type: dt,
                            nullable,
                            primary_key: false,
                            unique,
                            default,
                            generated: None,
                        })
                        .boxed()
                }
            },
        )
        .boxed()
}

/// Generate a column definition with profile-controlled constraints.
pub fn column_def_with_profile(profile: &ColumnProfile) -> BoxedStrategy<ColumnDef> {
    let not_null_prob = profile.not_null_probability;
    let unique_prob = profile.unique_probability;
    let default_prob = profile.default_probability;
    let data_type_weights = profile.data_type_weights.clone();

    (
        identifier(),
        data_type_weighted(&data_type_weights),
        0u8..100, // for NOT NULL decision
        0u8..100, // for UNIQUE decision
        0u8..100, // for DEFAULT decision
    )
        .prop_flat_map(
            move |(name, dt, not_null_roll, unique_roll, default_roll)| {
                let nullable = not_null_roll >= not_null_prob;
                let unique = unique_roll < unique_prob;
                let has_default = default_roll < default_prob;

                let default_strategy = if has_default {
                    default_value_for_type(dt)
                } else {
                    Just(None).boxed()
                };

                default_strategy.prop_map(move |default| ColumnDef {
                    name: name.clone(),
                    data_type: dt,
                    nullable,
                    primary_key: false,
                    unique,
                    default,
                    generated: None,
                })
            },
        )
        .boxed()
}

/// Generate a column definition with default settings.
pub fn column_def() -> BoxedStrategy<ColumnDef> {
    column_def_with_profile(&ColumnProfile::default())
}

/// Generate a primary key column definition with profile.
pub fn primary_key_column_def_with_profile(
    profile: &PrimaryKeyProfile,
) -> BoxedStrategy<ColumnDef> {
    let data_type_weights = profile.data_type_weights.clone();

    (identifier(), data_type_weighted(&data_type_weights))
        .prop_map(|(name, data_type)| ColumnDef {
            name,
            data_type,
            nullable: false,
            primary_key: true,
            unique: false,
            default: None,
            generated: None,
        })
        .boxed()
}

/// Generate a primary key column definition with default settings.
pub fn primary_key_column_def() -> BoxedStrategy<ColumnDef> {
    primary_key_column_def_with_profile(&PrimaryKeyProfile::default())
}

/// Generate an optional primary key column based on profile settings.
fn optional_primary_key(profile: &PrimaryKeyProfile) -> BoxedStrategy<Option<ColumnDef>> {
    if profile.always_include {
        primary_key_column_def_with_profile(profile)
            .prop_map(Some)
            .boxed()
    } else {
        let include_prob = profile.include_probability;
        let profile = profile.clone();
        (0u8..100)
            .prop_flat_map(move |roll| {
                if roll < include_prob {
                    primary_key_column_def_with_profile(&profile)
                        .prop_map(Some)
                        .boxed()
                } else {
                    Just(None).boxed()
                }
            })
            .boxed()
    }
}

/// Generate IF NOT EXISTS based on probability.
fn if_not_exists_with_probability(probability: u8) -> BoxedStrategy<bool> {
    (0u8..100).prop_map(move |roll| roll < probability).boxed()
}

/// Generate columns incrementally, allowing generated columns to reference prior columns.
fn columns_with_generated_support(
    pk_col: Option<ColumnDef>,
    column_count: usize,
    profile: &ColumnProfile,
) -> BoxedStrategy<Vec<ColumnDef>> {
    let generated_prob = profile.generated_probability;
    let stored_prob = profile.generated_stored_probability;
    let collation_prob = profile.generated_collation_probability;
    let invalid_prob = profile.invalid_generated_expr_probability;
    let profile = profile.clone();

    // First, generate all column base info (names, types, constraints)
    let base_columns_strategy =
        proptest::collection::vec(column_def_with_profile(&profile), column_count);

    base_columns_strategy
        .prop_flat_map(move |base_cols| {
            // Now decide which columns become generated and generate their expressions
            let mut strategies: Vec<BoxedStrategy<ColumnDef>> = Vec::new();

            // Add PK column first if present (PKs are never generated for now)
            let prior_cols: Vec<ColumnDef> = if let Some(ref pk) = pk_col {
                vec![pk.clone()]
            } else {
                vec![]
            };

            if let Some(pk) = pk_col.clone() {
                strategies.push(Just(pk).boxed());
            }

            // Track columns as we build up the list
            let mut all_prior: Vec<ColumnDef> = prior_cols;

            for col in base_cols {
                let prior_for_this = all_prior.clone();
                let col_for_closure = col.clone();

                // Generate random values for this column's generated status
                let strategy = (0u8..100, 0u8..100, 0u8..100, 0u8..100)
                    .prop_flat_map(
                        move |(gen_roll, stored_roll, collation_roll, invalid_roll)| {
                            let is_generated = gen_roll < generated_prob;

                            if is_generated {
                                let use_invalid = invalid_roll < invalid_prob;
                                let expr_strategy = if use_invalid {
                                    invalid_generated_expr()
                                } else {
                                    generated_expr_for_type(
                                        col_for_closure.data_type,
                                        &prior_for_this,
                                    )
                                };

                                let storage = if stored_roll < stored_prob {
                                    GeneratedStorage::Stored
                                } else {
                                    GeneratedStorage::Virtual
                                };

                                let collation = if col_for_closure.data_type == DataType::Text
                                    && collation_roll < collation_prob
                                {
                                    Some(match collation_roll % 3 {
                                        0 => Collation::NoCase,
                                        1 => Collation::RTrim,
                                        _ => Collation::Binary,
                                    })
                                } else {
                                    None
                                };

                                let col_name = col_for_closure.name.clone();
                                let col_type = col_for_closure.data_type;
                                let col_nullable = col_for_closure.nullable;
                                let col_unique = col_for_closure.unique;

                                expr_strategy
                                    .prop_map(move |expr| ColumnDef {
                                        name: col_name.clone(),
                                        data_type: col_type,
                                        nullable: col_nullable,
                                        primary_key: false,
                                        unique: col_unique,
                                        default: None,
                                        generated: Some(GeneratedColumn {
                                            expr,
                                            storage,
                                            collation,
                                        }),
                                    })
                                    .boxed()
                            } else {
                                // Keep as regular column
                                Just(col_for_closure.clone()).boxed()
                            }
                        },
                    )
                    .boxed();

                strategies.push(strategy);
                all_prior.push(col);
            }

            // Combine all column strategies
            strategies.into_iter().collect::<Vec<_>>()
        })
        .boxed()
}

/// Generate a CREATE TABLE statement with profile.
pub fn create_table(
    schema: &Schema,
    profile: &StatementProfile,
) -> BoxedStrategy<CreateTableStatement> {
    let existing_names = schema.table_names();

    // Extract profile values from the CreateTableProfile
    let create_table_profile = profile.create_table_profile();
    let column_count_range = create_table_profile.column_count_range.clone();
    let column_profile = create_table_profile.column.clone();
    let pk_profile = create_table_profile.primary_key.clone();
    let if_not_exists_prob = create_table_profile.if_not_exists_probability;

    (
        identifier_excluding(existing_names),
        if_not_exists_with_probability(if_not_exists_prob),
        optional_primary_key(&pk_profile),
        column_count_range.clone(),
    )
        .prop_flat_map(move |(table_name, if_not_exists, pk_col, col_count)| {
            let column_profile = column_profile.clone();

            columns_with_generated_support(pk_col, col_count, &column_profile).prop_map(
                move |columns| {
                    let mut final_columns = columns;

                    // Ensure at least one column exists
                    if final_columns.is_empty() {
                        final_columns.push(ColumnDef {
                            name: "id".to_string(),
                            data_type: DataType::Integer,
                            nullable: false,
                            primary_key: true,
                            unique: false,
                            default: None,
                            generated: None,
                        });
                    }

                    CreateTableStatement {
                        table_name: table_name.clone(),
                        columns: final_columns,
                        if_not_exists,
                    }
                },
            )
        })
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::profile::{StatementProfile, WeightedProfile};
    use crate::schema::SchemaBuilder;
    use proptest::test_runner::TestRunner;

    fn empty_schema() -> Schema {
        SchemaBuilder::new().build()
    }

    #[test]
    fn test_create_table_display() {
        let stmt = CreateTableStatement {
            table_name: "users".to_string(),
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    primary_key: true,
                    unique: false,
                    default: None,
                    generated: None,
                },
                ColumnDef {
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    nullable: false,
                    primary_key: false,
                    unique: false,
                    default: None,
                    generated: None,
                },
                ColumnDef {
                    name: "email".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                    primary_key: false,
                    unique: true,
                    default: None,
                    generated: None,
                },
            ],
            if_not_exists: false,
        };

        assert_eq!(
            stmt.to_string(),
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)"
        );
    }

    #[test]
    fn test_create_table_if_not_exists() {
        let stmt = CreateTableStatement {
            table_name: "test".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                primary_key: true,
                unique: false,
                default: None,
                generated: None,
            }],
            if_not_exists: true,
        };

        assert_eq!(
            stmt.to_string(),
            "CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY)"
        );
    }

    #[test]
    fn test_create_table_with_generated_column() {
        use crate::schema::{Collation, GeneratedColumn, GeneratedStorage};

        let stmt = CreateTableStatement {
            table_name: "products".to_string(),
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    primary_key: true,
                    unique: false,
                    default: None,
                    generated: None,
                },
                ColumnDef {
                    name: "price".to_string(),
                    data_type: DataType::Real,
                    nullable: false,
                    primary_key: false,
                    unique: false,
                    default: None,
                    generated: None,
                },
                ColumnDef {
                    name: "quantity".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    primary_key: false,
                    unique: false,
                    default: None,
                    generated: None,
                },
                // VIRTUAL generated column
                ColumnDef {
                    name: "total".to_string(),
                    data_type: DataType::Real,
                    nullable: true,
                    primary_key: false,
                    unique: false,
                    default: None,
                    generated: Some(GeneratedColumn {
                        expr: "price * quantity".to_string(),
                        storage: GeneratedStorage::Virtual,
                        collation: None,
                    }),
                },
                // STORED generated column
                ColumnDef {
                    name: "description".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                    primary_key: false,
                    unique: false,
                    default: None,
                    generated: Some(GeneratedColumn {
                        expr: "'Item #' || id".to_string(),
                        storage: GeneratedStorage::Stored,
                        collation: Some(Collation::NoCase),
                    }),
                },
            ],
            if_not_exists: false,
        };

        let sql = stmt.to_string();
        assert!(sql.contains("total REAL AS (price * quantity) VIRTUAL"));
        assert!(sql.contains("description TEXT AS ('Item #' || id) STORED COLLATE NOCASE"));
    }

    #[test]
    fn test_data_type_weights_default() {
        let weights = DataTypeWeights::default();
        assert_eq!(weights.integer, 30);
        assert_eq!(weights.real, 20);
        assert_eq!(weights.text, 35);
        assert_eq!(weights.blob, 15);
    }

    #[test]
    fn test_data_type_weights_builder() {
        let weights = DataTypeWeights::default()
            .with_integer(50)
            .with_real(0)
            .with_text(50)
            .with_blob(0);

        assert_eq!(weights.integer, 50);
        assert_eq!(weights.real, 0);
        assert_eq!(weights.text, 50);
        assert_eq!(weights.blob, 0);

        let enabled: Vec<_> = weights.enabled_types().collect();
        assert_eq!(enabled.len(), 2);
    }

    #[test]
    fn test_column_profile_default() {
        let profile = ColumnProfile::default();
        assert_eq!(profile.not_null_probability, 30);
        assert_eq!(profile.unique_probability, 10);
        assert_eq!(profile.default_probability, 15);
    }

    #[test]
    fn test_column_profile_builder() {
        let profile = ColumnProfile::default()
            .with_not_null_probability(100)
            .with_unique_probability(50)
            .with_default_probability(25);

        assert_eq!(profile.not_null_probability, 100);
        assert_eq!(profile.unique_probability, 50);
        assert_eq!(profile.default_probability, 25);
    }

    #[test]
    fn test_primary_key_profile_default() {
        let profile = PrimaryKeyProfile::default();
        assert!(profile.always_include);
        assert!(profile.prefer_integer);
    }

    #[test]
    fn test_primary_key_profile_none() {
        let profile = PrimaryKeyProfile::default().none();
        assert!(!profile.always_include);
        assert_eq!(profile.include_probability, 0);
    }

    #[test]
    fn test_create_table_profile_default() {
        let profile = CreateTableProfile::default();
        assert_eq!(profile.if_not_exists_probability, 50);
        assert!(profile.primary_key.always_include);
    }

    #[test]
    fn test_create_table_profile_strict() {
        let profile = CreateTableProfile::default().strict();
        assert_eq!(profile.column.not_null_probability, 100);
        assert_eq!(profile.column.unique_probability, 50);
    }

    #[test]
    fn test_create_table_profile_no_primary_key() {
        let profile = CreateTableProfile::default().no_primary_key();
        assert!(!profile.primary_key.always_include);
        assert_eq!(profile.primary_key.include_probability, 0);
    }

    #[test]
    fn test_integer_only_generates_integers() {
        let weights = DataTypeWeights::default().integer_only();
        let strategy = data_type_weighted(&weights);

        let mut runner = TestRunner::default();
        for _ in 0..20 {
            let dt = strategy.new_tree(&mut runner).unwrap().current();
            assert_eq!(dt, DataType::Integer);
        }
    }

    proptest! {
        #[test]
        fn generated_create_table_has_columns(
            stmt in create_table(&empty_schema(), &StatementProfile::default())
        ) {
            prop_assert!(!stmt.columns.is_empty());
        }

        #[test]
        fn generated_create_table_with_strict_profile(
            stmt in create_table(
                &empty_schema(),
                &StatementProfile::default()
                    .with_create_table_profile(WeightedProfile::with_extra(1, CreateTableProfile::default().strict()))
            )
        ) {
            let sql = stmt.to_string();
            prop_assert!(sql.starts_with("CREATE TABLE"));
            // Strict profile should have primary key
            prop_assert!(stmt.columns.iter().any(|c| c.primary_key));
        }

        #[test]
        fn generated_create_table_with_small_profile(
            stmt in create_table(
                &empty_schema(),
                &StatementProfile::default()
                    .with_create_table_profile(WeightedProfile::with_extra(1, CreateTableProfile::default().small()))
            )
        ) {
            // Small profile: 1-3 columns plus optional PK
            prop_assert!(stmt.columns.len() <= 5);
        }

        #[test]
        fn generated_column_with_strict_profile(
            col in column_def_with_profile(&ColumnProfile::default().full_constraints())
        ) {
            // Full constraints profile has 100% NOT NULL probability
            prop_assert!(!col.nullable);
        }

        #[test]
        fn generated_column_with_minimal_profile(
            col in column_def_with_profile(&ColumnProfile::default().minimal())
        ) {
            // Minimal profile has 0% UNIQUE and DEFAULT probability
            prop_assert!(!col.unique);
            prop_assert!(col.default.is_none());
        }
    }
}
