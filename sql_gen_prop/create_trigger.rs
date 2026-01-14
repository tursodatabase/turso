//! CREATE TRIGGER statement generation.
//!
//! SQLite triggers support:
//! - Timing: BEFORE, AFTER, INSTEAD OF
//! - Events: INSERT, UPDATE, DELETE
//! - Body: One or more SQL statements

use std::fmt::{self, Display};

use proptest::prelude::*;
use strum::IntoEnumIterator;

use crate::create_table::identifier_excluding;
use crate::delete::delete_for_table;
use crate::generator::SqlGeneratorKind;
use crate::insert::insert_for_table;
use crate::schema::{Schema, TableRef};
use crate::select::select_for_table;
use crate::update::update_for_table;
use crate::{DeleteStatement, InsertStatement, SelectStatement, StatementProfile, UpdateStatement};

/// Context needed for CREATE TRIGGER generation.
#[derive(Debug, Clone)]
pub struct CreateTriggerContext<'a> {
    /// The table to create triggers on.
    pub table: &'a TableRef,
    /// The schema containing the table.
    pub schema: &'a Schema,
}

/// Trigger timing (when the trigger fires relative to the event).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::EnumIter)]
pub enum TriggerTiming {
    Before,
    After,
}

impl fmt::Display for TriggerTiming {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TriggerTiming::Before => write!(f, "BEFORE"),
            TriggerTiming::After => write!(f, "AFTER"),
        }
    }
}

/// Trigger event (what operation fires the trigger).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::EnumIter)]
pub enum TriggerEvent {
    Insert,
    Update,
    Delete,
}

impl fmt::Display for TriggerEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TriggerEvent::Insert => write!(f, "INSERT"),
            TriggerEvent::Update => write!(f, "UPDATE"),
            TriggerEvent::Delete => write!(f, "DELETE"),
        }
    }
}

/// Enum representing the kinds of CREATE TRIGGER variations.
///
/// Each kind represents a combination of timing and event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::EnumIter)]
pub enum CreateTriggerKind {
    BeforeInsert,
    BeforeUpdate,
    BeforeDelete,
    AfterInsert,
    AfterUpdate,
    AfterDelete,
}

impl CreateTriggerKind {
    /// Returns the timing for this trigger kind.
    pub fn timing(&self) -> TriggerTiming {
        match self {
            CreateTriggerKind::BeforeInsert
            | CreateTriggerKind::BeforeUpdate
            | CreateTriggerKind::BeforeDelete => TriggerTiming::Before,
            CreateTriggerKind::AfterInsert
            | CreateTriggerKind::AfterUpdate
            | CreateTriggerKind::AfterDelete => TriggerTiming::After,
        }
    }

    /// Returns the event for this trigger kind.
    pub fn event(&self) -> TriggerEvent {
        match self {
            CreateTriggerKind::BeforeInsert | CreateTriggerKind::AfterInsert => {
                TriggerEvent::Insert
            }
            CreateTriggerKind::BeforeUpdate | CreateTriggerKind::AfterUpdate => {
                TriggerEvent::Update
            }
            CreateTriggerKind::BeforeDelete | CreateTriggerKind::AfterDelete => {
                TriggerEvent::Delete
            }
        }
    }
}

impl SqlGeneratorKind for CreateTriggerKind {
    type Context<'a> = CreateTriggerContext<'a>;
    type Output = CreateTriggerStatement;
    type Profile = StatementProfile;

    fn available(&self, _ctx: &Self::Context<'_>) -> bool {
        // All trigger kinds are available if we have a table
        true
    }

    fn supported(&self) -> bool {
        match self {
            CreateTriggerKind::BeforeInsert => true,
            CreateTriggerKind::BeforeUpdate => true,
            CreateTriggerKind::BeforeDelete => true,
            CreateTriggerKind::AfterInsert => true,
            CreateTriggerKind::AfterUpdate => true,
            CreateTriggerKind::AfterDelete => true,
        }
    }

    fn strategy<'a>(
        &self,
        ctx: &Self::Context<'_>,
        profile: &Self::Profile,
    ) -> BoxedStrategy<Self::Output> {
        create_trigger_with_timing_event(
            ctx.table,
            ctx.schema,
            profile,
            self.timing(),
            self.event(),
        )
    }
}

/// Weights for CREATE TRIGGER operation types.
///
/// Each weight determines the relative probability of generating that
/// trigger type. A weight of 0 disables that trigger type entirely.
#[derive(Debug, Clone)]
pub struct CreateTriggerOpWeights {
    pub before_insert: u32,
    pub before_update: u32,
    pub before_delete: u32,
    pub after_insert: u32,
    pub after_update: u32,
    pub after_delete: u32,
}

impl Default for CreateTriggerOpWeights {
    fn default() -> Self {
        Self {
            before_insert: 15,
            before_update: 15,
            before_delete: 15,
            after_insert: 20,
            after_update: 20,
            after_delete: 15,
        }
    }
}

impl CreateTriggerOpWeights {
    /// Create weights with all values set to zero.
    pub fn none() -> Self {
        Self {
            before_insert: 0,
            before_update: 0,
            before_delete: 0,
            after_insert: 0,
            after_update: 0,
            after_delete: 0,
        }
    }

    /// Builder method to set BEFORE INSERT weight.
    pub fn with_before_insert(mut self, weight: u32) -> Self {
        self.before_insert = weight;
        self
    }

    /// Builder method to set BEFORE UPDATE weight.
    pub fn with_before_update(mut self, weight: u32) -> Self {
        self.before_update = weight;
        self
    }

    /// Builder method to set BEFORE DELETE weight.
    pub fn with_before_delete(mut self, weight: u32) -> Self {
        self.before_delete = weight;
        self
    }

    /// Builder method to set AFTER INSERT weight.
    pub fn with_after_insert(mut self, weight: u32) -> Self {
        self.after_insert = weight;
        self
    }

    /// Builder method to set AFTER UPDATE weight.
    pub fn with_after_update(mut self, weight: u32) -> Self {
        self.after_update = weight;
        self
    }

    /// Builder method to set AFTER DELETE weight.
    pub fn with_after_delete(mut self, weight: u32) -> Self {
        self.after_delete = weight;
        self
    }

    /// Returns the total weight (sum of all weights).
    pub fn total_weight(&self) -> u32 {
        self.before_insert
            + self.before_update
            + self.before_delete
            + self.after_insert
            + self.after_update
            + self.after_delete
    }

    /// Returns true if at least one trigger type is enabled.
    pub fn has_enabled_operations(&self) -> bool {
        self.total_weight() > 0
    }

    /// Returns the weight for a given trigger kind.
    pub fn weight_for(&self, kind: CreateTriggerKind) -> u32 {
        match kind {
            CreateTriggerKind::BeforeInsert => self.before_insert,
            CreateTriggerKind::BeforeUpdate => self.before_update,
            CreateTriggerKind::BeforeDelete => self.before_delete,
            CreateTriggerKind::AfterInsert => self.after_insert,
            CreateTriggerKind::AfterUpdate => self.after_update,
            CreateTriggerKind::AfterDelete => self.after_delete,
        }
    }

    /// Returns an iterator over all trigger kinds with weight > 0.
    pub fn enabled_operations(&self) -> impl Iterator<Item = (CreateTriggerKind, u32)> + '_ {
        CreateTriggerKind::iter()
            .map(|kind| (kind, self.weight_for(kind)))
            .filter(|(_, w)| *w > 0)
    }
}

// =============================================================================
// CREATE TRIGGER PROFILE
// =============================================================================

/// Profile for controlling CREATE TRIGGER statement generation.
#[derive(Debug, Clone)]
pub struct CreateTriggerProfile {
    /// Operation weights for trigger types.
    pub op_weights: CreateTriggerOpWeights,
    /// Range for number of statements in trigger body.
    pub body_statement_count_range: std::ops::RangeInclusive<usize>,
}

impl Default for CreateTriggerProfile {
    fn default() -> Self {
        Self {
            op_weights: CreateTriggerOpWeights::default(),
            body_statement_count_range: 1..=3,
        }
    }
}

impl CreateTriggerProfile {
    /// Create a profile with minimal trigger bodies.
    pub fn minimal() -> Self {
        Self {
            op_weights: CreateTriggerOpWeights::default(),
            body_statement_count_range: 1..=1,
        }
    }

    /// Create a profile with complex trigger bodies.
    pub fn complex() -> Self {
        Self {
            op_weights: CreateTriggerOpWeights::default(),
            body_statement_count_range: 2..=5,
        }
    }

    /// Builder method to set operation weights.
    pub fn with_op_weights(mut self, weights: CreateTriggerOpWeights) -> Self {
        self.op_weights = weights;
        self
    }

    /// Builder method to set body statement count range.
    pub fn with_body_statement_count_range(
        mut self,
        range: std::ops::RangeInclusive<usize>,
    ) -> Self {
        self.body_statement_count_range = range;
        self
    }
}

#[derive(Debug, Clone)]
pub enum TriggerSqlStatement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
}

impl Display for TriggerSqlStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TriggerSqlStatement::Select(stmt) => write!(f, "{stmt}"),
            TriggerSqlStatement::Insert(stmt) => write!(f, "{stmt}"),
            TriggerSqlStatement::Update(stmt) => write!(f, "{stmt}"),
            TriggerSqlStatement::Delete(stmt) => write!(f, "{stmt}"),
        }
    }
}

/// CREATE TRIGGER statement.
#[derive(Debug, Clone)]
pub struct CreateTriggerStatement {
    /// Trigger name.
    pub name: String,
    /// Whether to use IF NOT EXISTS.
    pub if_not_exists: bool,
    /// When the trigger fires (BEFORE, AFTER).
    pub timing: TriggerTiming,
    /// What event fires the trigger (INSERT, UPDATE, DELETE).
    pub event: TriggerEvent,
    /// Table name the trigger is on.
    pub table_name: String,
    /// The trigger body (SQL statements).
    pub body: Vec<TriggerSqlStatement>,
}

impl fmt::Display for CreateTriggerStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CREATE TRIGGER ")?;
        if self.if_not_exists {
            write!(f, "IF NOT EXISTS ")?;
        }
        write!(
            f,
            "\"{}\" {} {} ON \"{}\" BEGIN ",
            self.name, self.timing, self.event, self.table_name
        )?;
        for stmt in &self.body {
            write!(f, "{stmt}; ")?;
        }
        write!(f, "END")
    }
}

/// Generate a trigger timing strategy.
pub fn trigger_timing() -> impl Strategy<Value = TriggerTiming> {
    prop_oneof![Just(TriggerTiming::Before), Just(TriggerTiming::After),]
}

/// Generate a trigger event strategy.
pub fn trigger_event() -> impl Strategy<Value = TriggerEvent> {
    prop_oneof![
        Just(TriggerEvent::Insert),
        Just(TriggerEvent::Update),
        Just(TriggerEvent::Delete),
    ]
}

/// Generate a trigger body containing valid DML statements.
fn trigger_body(
    table: &TableRef,
    schema: &Schema,
    profile: &StatementProfile,
) -> BoxedStrategy<Vec<TriggerSqlStatement>> {
    // Generate 1-3 DML statements for the trigger body
    proptest::collection::vec(
        prop_oneof![
            select_for_table(table, schema, profile.select_profile())
                .prop_map(TriggerSqlStatement::Select),
            insert_for_table(table, profile.insert_profile()).prop_map(TriggerSqlStatement::Insert),
            update_for_table(table, schema, profile.update_profile())
                .prop_map(TriggerSqlStatement::Update),
            delete_for_table(table, schema, profile.delete_profile())
                .prop_map(TriggerSqlStatement::Delete),
        ],
        1..=3,
    )
    .boxed()
}

/// Generate a CREATE TRIGGER statement with specific timing and event.
pub fn create_trigger_with_timing_event(
    table: &TableRef,
    schema: &Schema,
    profile: &StatementProfile,
    timing: TriggerTiming,
    event: TriggerEvent,
) -> BoxedStrategy<CreateTriggerStatement> {
    let table_name = table.name.clone();
    let existing_triggers = schema.trigger_names();

    (
        identifier_excluding(existing_triggers),
        any::<bool>(),
        trigger_body(table, schema, profile),
    )
        .prop_map(move |(name, if_not_exists, body)| CreateTriggerStatement {
            name,
            if_not_exists,
            timing,
            event,
            table_name: table_name.clone(),
            body,
        })
        .boxed()
}

/// Generate a CREATE TRIGGER statement for a table with optional operation weights.
pub fn create_trigger_for_table(
    table: &TableRef,
    schema: &Schema,
    profile: &StatementProfile,
) -> BoxedStrategy<CreateTriggerStatement> {
    let w = &profile.create_trigger_profile().op_weights;
    let ctx = CreateTriggerContext { table, schema };

    let strategies: Vec<(u32, BoxedStrategy<CreateTriggerStatement>)> = w
        .enabled_operations()
        .filter(|(kind, _)| kind.supported() && kind.available(&ctx))
        .map(|(kind, weight)| (weight, kind.strategy(&ctx, profile)))
        .collect();

    assert!(
        !strategies.is_empty(),
        "No valid CREATE TRIGGER operations can be generated for the given table and profile"
    );

    proptest::strategy::Union::new_weighted(strategies).boxed()
}

/// Generate a CREATE TRIGGER statement for any table in the schema.
pub fn create_trigger_for_schema(
    schema: &Schema,
    profile: &StatementProfile,
) -> BoxedStrategy<CreateTriggerStatement> {
    assert!(
        !schema.tables.is_empty(),
        "Schema must have at least one table"
    );

    let tables = schema.tables.clone();
    let schema_clone = schema.clone();
    let profile = profile.clone();
    proptest::sample::select((*tables).clone())
        .prop_flat_map(move |table| create_trigger_for_table(&table, &schema_clone, &profile))
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Table,
        schema::{ColumnDef, DataType, SchemaBuilder},
    };

    fn test_table() -> Table {
        Table::new(
            "users",
            vec![
                ColumnDef::new("id", DataType::Integer).primary_key(),
                ColumnDef::new("name", DataType::Text).not_null(),
                ColumnDef::new("email", DataType::Text),
            ],
        )
    }

    fn test_schema() -> Schema {
        SchemaBuilder::new().add_table(test_table()).build()
    }

    proptest! {
        #[test]
        fn create_trigger_generates_valid_sql(stmt in create_trigger_for_table(&test_table().into(), &test_schema(), &Default::default())) {
            let sql = stmt.to_string();
            prop_assert!(sql.starts_with("CREATE TRIGGER"));
            prop_assert!(sql.contains("ON \"users\""));
            prop_assert!(sql.contains("BEGIN") && sql.contains("END"));
        }

        #[test]
        fn create_trigger_for_schema_generates_valid_sql(stmt in create_trigger_for_schema(&test_schema(), &Default::default())) {
            let sql = stmt.to_string();
            prop_assert!(sql.starts_with("CREATE TRIGGER"));
            prop_assert!(sql.contains("BEGIN") && sql.contains("END"));
        }

        #[test]
        fn create_trigger_before_only(stmt in {
            let mut profile = StatementProfile::default();
            profile.create_trigger.extra.op_weights = CreateTriggerOpWeights::none()
                .with_before_insert(50)
                .with_before_update(50);
            create_trigger_for_table(
                &test_table().into(),
                &test_schema(),
                &profile,
            )
        }
        ) {
            let sql = stmt.to_string();
            prop_assert!(sql.contains("BEFORE"));
        }
    }

    #[test]
    fn test_create_trigger_op_weights_default() {
        let weights = CreateTriggerOpWeights::default();
        assert!(weights.has_enabled_operations());
        assert_eq!(weights.total_weight(), 100);
    }

    #[test]
    fn test_create_trigger_op_weights_builder() {
        let weights = CreateTriggerOpWeights::none().with_after_insert(100);
        assert_eq!(weights.after_insert, 100);
        assert_eq!(weights.before_insert, 0);
        assert_eq!(weights.total_weight(), 100);
    }
}
