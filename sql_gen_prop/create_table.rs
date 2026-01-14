//! CREATE TABLE statement type and generation strategy.

use proptest::prelude::*;
use std::collections::HashSet;
use std::fmt;
use std::ops::RangeInclusive;

use crate::schema::{ColumnDef, DataType, Schema};

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
    /// Create weights with all types equally weighted.
    pub fn uniform() -> Self {
        Self {
            integer: 25,
            real: 25,
            text: 25,
            blob: 25,
        }
    }

    /// Create weights favoring numeric types.
    pub fn numeric_heavy() -> Self {
        Self {
            integer: 45,
            real: 35,
            text: 15,
            blob: 5,
        }
    }

    /// Create weights favoring text types.
    pub fn text_heavy() -> Self {
        Self {
            integer: 20,
            real: 10,
            text: 60,
            blob: 10,
        }
    }

    /// Create weights for only integer types.
    pub fn integer_only() -> Self {
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
}

impl Default for ColumnProfile {
    fn default() -> Self {
        Self {
            not_null_probability: 30,
            unique_probability: 10,
            default_probability: 15,
            data_type_weights: DataTypeWeights::default(),
        }
    }
}

impl ColumnProfile {
    /// Create a profile where all columns are nullable with no constraints.
    pub fn minimal() -> Self {
        Self {
            not_null_probability: 0,
            unique_probability: 0,
            default_probability: 0,
            data_type_weights: DataTypeWeights::default(),
        }
    }

    /// Create a profile with strict constraints (many NOT NULL and UNIQUE).
    pub fn strict() -> Self {
        Self {
            not_null_probability: 70,
            unique_probability: 30,
            default_probability: 20,
            data_type_weights: DataTypeWeights::default(),
        }
    }

    /// Create a profile with all constraints enabled at maximum probability.
    pub fn full_constraints() -> Self {
        Self {
            not_null_probability: 100,
            unique_probability: 50,
            default_probability: 40,
            data_type_weights: DataTypeWeights::default(),
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
    /// Create a profile that always uses INTEGER PRIMARY KEY.
    pub fn integer_only() -> Self {
        Self {
            always_include: true,
            include_probability: 100,
            prefer_integer: true,
            data_type_weights: DataTypeWeights::integer_only(),
        }
    }

    /// Create a profile that may omit primary keys.
    pub fn optional() -> Self {
        Self {
            always_include: false,
            include_probability: 70,
            prefer_integer: true,
            data_type_weights: DataTypeWeights::default(),
        }
    }

    /// Create a profile with no primary keys.
    pub fn none() -> Self {
        Self {
            always_include: false,
            include_probability: 0,
            prefer_integer: false,
            data_type_weights: DataTypeWeights::default(),
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
    /// Create a profile for small tables with minimal constraints.
    pub fn small() -> Self {
        Self {
            identifier_pattern: "[a-z][a-z0-9_]{0,15}".to_string(),
            column_count_range: 1..=3,
            if_not_exists_probability: 50,
            primary_key: PrimaryKeyProfile::integer_only(),
            column: ColumnProfile::minimal(),
        }
    }

    /// Create a profile for large tables with varied constraints.
    pub fn large() -> Self {
        Self {
            identifier_pattern: "[a-z][a-z0-9_]{0,30}".to_string(),
            column_count_range: 5..=20,
            if_not_exists_probability: 30,
            primary_key: PrimaryKeyProfile::default(),
            column: ColumnProfile::strict(),
        }
    }

    /// Create a profile for strict schema with many constraints.
    pub fn strict() -> Self {
        Self {
            identifier_pattern: "[a-z][a-z0-9_]{0,30}".to_string(),
            column_count_range: 2..=8,
            if_not_exists_probability: 20,
            primary_key: PrimaryKeyProfile::integer_only(),
            column: ColumnProfile::full_constraints(),
        }
    }

    /// Create a profile for tables without primary keys.
    pub fn no_primary_key() -> Self {
        Self {
            identifier_pattern: "[a-z][a-z0-9_]{0,30}".to_string(),
            column_count_range: 1..=10,
            if_not_exists_probability: 50,
            primary_key: PrimaryKeyProfile::none(),
            column: ColumnProfile::default(),
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

        write!(f, "\"{}\" (", self.table_name)?;

        let col_defs: Vec<String> = self.columns.iter().map(|c| c.to_string()).collect();
        write!(f, "{})", col_defs.join(", "))
    }
}

/// Generate a valid SQL identifier.
pub fn identifier() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9_]{0,30}".prop_filter("must not be empty", |s| !s.is_empty())
}

/// Generate a valid SQL identifier that is not in the excluded set.
pub fn identifier_excluding(excluded: HashSet<String>) -> impl Strategy<Value = String> {
    identifier().prop_filter_map("must not be in excluded set", move |s| {
        if excluded.contains(&s) { None } else { Some(s) }
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
        .prop_flat_map(move |(name, dt, not_null_roll, unique_roll, default_roll)| {
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
            })
        })
        .boxed()
}

/// Generate a column definition with default settings.
pub fn column_def() -> BoxedStrategy<ColumnDef> {
    column_def_with_profile(&ColumnProfile::default())
}

/// Generate a primary key column definition with profile.
pub fn primary_key_column_def_with_profile(profile: &PrimaryKeyProfile) -> BoxedStrategy<ColumnDef> {
    let data_type_weights = profile.data_type_weights.clone();

    (identifier(), data_type_weighted(&data_type_weights)).prop_map(|(name, data_type)| ColumnDef {
        name,
        data_type,
        nullable: false,
        primary_key: true,
        unique: false,
        default: None,
    }).boxed()
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
        (0u8..100).prop_flat_map(move |roll| {
            if roll < include_prob {
                primary_key_column_def_with_profile(&profile)
                    .prop_map(Some)
                    .boxed()
            } else {
                Just(None).boxed()
            }
        }).boxed()
    }
}

/// Generate IF NOT EXISTS based on probability.
fn if_not_exists_with_probability(probability: u8) -> BoxedStrategy<bool> {
    (0u8..100).prop_map(move |roll| roll < probability).boxed()
}

/// Generate a CREATE TABLE statement with profile.
pub fn create_table(
    schema: &Schema,
    profile: &CreateTableProfile,
) -> BoxedStrategy<CreateTableStatement> {
    let existing_names = schema.table_names();

    let column_count_range = profile.column_count_range.clone();
    let column_profile = profile.column.clone();
    let pk_profile = profile.primary_key.clone();
    let if_not_exists_prob = profile.if_not_exists_probability;

    (
        identifier_excluding(existing_names),
        if_not_exists_with_probability(if_not_exists_prob),
        optional_primary_key(&pk_profile),
        proptest::collection::vec(column_def_with_profile(&column_profile), column_count_range),
    )
        .prop_map(|(table_name, if_not_exists, pk_col, other_cols)| {
            let mut columns = Vec::with_capacity(other_cols.len() + 1);
            if let Some(pk) = pk_col {
                columns.push(pk);
            }
            columns.extend(other_cols);

            // Ensure at least one column exists
            if columns.is_empty() {
                columns.push(ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    primary_key: true,
                    unique: false,
                    default: None,
                });
            }

            CreateTableStatement {
                table_name,
                columns,
                if_not_exists,
            }
        })
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
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
                },
                ColumnDef {
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    nullable: false,
                    primary_key: false,
                    unique: false,
                    default: None,
                },
                ColumnDef {
                    name: "email".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                    primary_key: false,
                    unique: true,
                    default: None,
                },
            ],
            if_not_exists: false,
        };

        assert_eq!(
            stmt.to_string(),
            "CREATE TABLE \"users\" (\"id\" INTEGER PRIMARY KEY, \"name\" TEXT NOT NULL, \"email\" TEXT UNIQUE)"
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
            }],
            if_not_exists: true,
        };

        assert_eq!(
            stmt.to_string(),
            "CREATE TABLE IF NOT EXISTS \"test\" (\"id\" INTEGER PRIMARY KEY)"
        );
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
        let profile = PrimaryKeyProfile::none();
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
        let profile = CreateTableProfile::strict();
        assert_eq!(profile.column.not_null_probability, 100);
        assert_eq!(profile.column.unique_probability, 50);
    }

    #[test]
    fn test_create_table_profile_no_primary_key() {
        let profile = CreateTableProfile::no_primary_key();
        assert!(!profile.primary_key.always_include);
        assert_eq!(profile.primary_key.include_probability, 0);
    }

    #[test]
    fn test_integer_only_generates_integers() {
        let weights = DataTypeWeights::integer_only();
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
            stmt in create_table(&empty_schema(), &CreateTableProfile::default())
        ) {
            prop_assert!(!stmt.columns.is_empty());
        }

        #[test]
        fn generated_create_table_with_strict_profile(
            stmt in create_table(&empty_schema(), &CreateTableProfile::strict())
        ) {
            let sql = stmt.to_string();
            prop_assert!(sql.starts_with("CREATE TABLE"));
            // Strict profile should have primary key
            prop_assert!(stmt.columns.iter().any(|c| c.primary_key));
        }

        #[test]
        fn generated_create_table_with_small_profile(
            stmt in create_table(&empty_schema(), &CreateTableProfile::small())
        ) {
            // Small profile: 1-3 columns plus optional PK
            prop_assert!(stmt.columns.len() <= 5);
        }

        #[test]
        fn generated_column_with_strict_profile(
            col in column_def_with_profile(&ColumnProfile::full_constraints())
        ) {
            // Full constraints profile has 100% NOT NULL probability
            prop_assert!(!col.nullable);
        }

        #[test]
        fn generated_column_with_minimal_profile(
            col in column_def_with_profile(&ColumnProfile::minimal())
        ) {
            // Minimal profile has 0% UNIQUE and DEFAULT probability
            prop_assert!(!col.unique);
            prop_assert!(col.default.is_none());
        }
    }
}
