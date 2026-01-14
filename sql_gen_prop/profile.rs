//! Profiles for controlling SQL statement generation.
//!
//! Profiles allow fine-grained control over which types of statements are
//! generated and with what relative frequency.

use strum::IntoEnumIterator;

use crate::alter_table::AlterTableOpWeights;
use crate::statement::StatementKind;

// Re-export profiles from their respective modules
pub use crate::condition::ConditionProfile;
pub use crate::create_index::CreateIndexProfile;
pub use crate::create_table::CreateTableProfile;
pub use crate::create_trigger::{CreateTriggerOpWeights, CreateTriggerProfile};
pub use crate::delete::DeleteProfile;
pub use crate::expression::{ExpressionProfile, ExtendedExpressionProfile};
pub use crate::function::{ExtendedFunctionProfile, FunctionProfile};
pub use crate::insert::InsertProfile;
pub use crate::select::SelectProfile;
pub use crate::update::UpdateProfile;
pub use crate::value::ValueProfile;

/// A weighted profile that holds an overall weight and optional detailed settings.
///
/// This generic struct allows statement types to have both:
/// - A weight determining how often this statement type is generated relative to others
/// - Optional detailed settings for fine-grained control within that statement type
#[derive(Debug, Clone)]
pub struct WeightedProfile<T: Default> {
    /// The overall weight for this statement type.
    pub weight: u32,
    /// Optional detailed settings for fine-grained control.
    pub extra: T,
}

impl<T: Default> WeightedProfile<T> {
    /// Create a new weighted profile with the given weight and no detail.
    pub fn new(weight: u32) -> Self {
        Self {
            weight,
            extra: Default::default(),
        }
    }

    /// Create a new weighted profile with weight and detail.
    pub const fn with_extra(weight: u32, extra: T) -> Self {
        Self { weight, extra }
    }

    /// Builder method to set the weight.
    pub fn weight(mut self, weight: u32) -> Self {
        self.weight = weight;
        self
    }

    /// Builder method to set the detail.
    pub fn extra(mut self, extra: T) -> Self {
        self.extra = extra;
        self
    }

    /// Returns true if the weight is greater than zero.
    pub fn is_enabled(&self) -> bool {
        self.weight > 0
    }
}

impl<T: Default> Default for WeightedProfile<T> {
    fn default() -> Self {
        Self {
            weight: 0,
            extra: Default::default(),
        }
    }
}

// =============================================================================
// GLOBAL GENERATION PROFILE
// =============================================================================

/// Global profile containing all generation settings.
///
/// This profile aggregates all sub-profiles and provides a single point
/// of configuration for the entire SQL generation system.
#[derive(Debug, Clone, Default)]
pub struct GenerationProfile {
    /// Profile for value generation.
    pub value: ValueProfile,
    /// Extended expression profile.
    pub expression: ExtendedExpressionProfile,
    /// Extended function profile.
    pub function: ExtendedFunctionProfile,
    /// Condition profile.
    pub condition: ConditionProfile,
}

impl GenerationProfile {
    /// Create a minimal profile for fast testing.
    pub fn minimal() -> Self {
        Self {
            value: ValueProfile::minimal(),
            expression: ExtendedExpressionProfile::simple(),
            function: ExtendedFunctionProfile::minimal(),
            condition: ConditionProfile::simple(),
        }
    }

    /// Create a complex profile for thorough testing.
    pub fn complex() -> Self {
        Self {
            value: ValueProfile::large(),
            expression: ExtendedExpressionProfile::complex(),
            function: ExtendedFunctionProfile::default(),
            condition: ConditionProfile::complex(),
        }
    }

    /// Builder method to set value profile.
    pub fn with_value(mut self, profile: ValueProfile) -> Self {
        self.value = profile;
        self
    }

    /// Builder method to set expression profile.
    pub fn with_expression(mut self, profile: ExtendedExpressionProfile) -> Self {
        self.expression = profile;
        self
    }

    /// Builder method to set function profile.
    pub fn with_function(mut self, profile: ExtendedFunctionProfile) -> Self {
        self.function = profile;
        self
    }

    /// Builder method to set condition profile.
    pub fn with_condition(mut self, profile: ConditionProfile) -> Self {
        self.condition = profile;
        self
    }
}

// =============================================================================
// STATEMENT PROFILE
// =============================================================================

/// Profile controlling SQL statement generation weights.
///
/// Each weight determines the relative probability of generating that
/// statement type. A weight of 0 disables that statement type entirely.
///
/// Statement types are divided into:
/// - **DML (Data Manipulation)**: SELECT, INSERT, UPDATE, DELETE
/// - **DDL (Data Definition)**: CREATE TABLE, CREATE INDEX, DROP TABLE, DROP INDEX,
///   ALTER TABLE, CREATE VIEW, DROP VIEW, CREATE TRIGGER, DROP TRIGGER
/// - **Transaction control**: BEGIN, COMMIT, ROLLBACK, SAVEPOINT, RELEASE
/// - **Utility**: VACUUM, ANALYZE, REINDEX
#[derive(Debug, Clone)]
pub struct StatementProfile {
    // DML weights with optional profiles
    /// SELECT weight and optional generation profile.
    pub select: WeightedProfile<SelectProfile>,
    /// INSERT weight and optional generation profile.
    pub insert: WeightedProfile<InsertProfile>,
    /// UPDATE weight and optional generation profile.
    pub update: WeightedProfile<UpdateProfile>,
    /// DELETE weight and optional generation profile.
    pub delete: WeightedProfile<DeleteProfile>,

    // DDL weights - Tables
    /// CREATE TABLE weight and optional generation profile.
    pub create_table: WeightedProfile<CreateTableProfile>,
    /// DROP TABLE weight (no extra profile needed).
    pub drop_table_weight: u32,
    /// ALTER TABLE weight and optional operation-level weights.
    pub alter_table: WeightedProfile<AlterTableOpWeights>,

    // DDL weights - Indexes
    /// CREATE INDEX weight and optional generation profile.
    pub create_index: WeightedProfile<CreateIndexProfile>,
    /// DROP INDEX weight (no extra profile needed).
    pub drop_index_weight: u32,

    // DDL weights - Views
    /// CREATE VIEW weight (no extra profile needed).
    pub create_view_weight: u32,
    /// DROP VIEW weight (no extra profile needed).
    pub drop_view_weight: u32,

    // DDL weights - Triggers
    /// CREATE TRIGGER weight and optional operation-level weights.
    pub create_trigger: WeightedProfile<CreateTriggerProfile>,
    /// DROP TRIGGER weight.
    pub drop_trigger_weight: u32,

    // Transaction control weights
    pub begin_weight: u32,
    pub commit_weight: u32,
    pub rollback_weight: u32,
    pub savepoint_weight: u32,
    pub release_weight: u32,

    // Utility weights
    pub vacuum_weight: u32,
    pub analyze_weight: u32,
    pub reindex_weight: u32,

    // Global generation profile
    /// Global profile for value, expression, function, and condition generation.
    pub generation: GenerationProfile,
}

impl Default for StatementProfile {
    fn default() -> Self {
        Self {
            // DML - most common operations
            select: WeightedProfile::new(40),
            insert: WeightedProfile::new(25),
            update: WeightedProfile::new(15),
            delete: WeightedProfile::new(10),

            // DDL - less frequent
            create_table: WeightedProfile::new(2),
            drop_table_weight: 1,
            alter_table: WeightedProfile::new(1),
            create_index: WeightedProfile::new(2),
            drop_index_weight: 1,
            create_view_weight: 1,
            drop_view_weight: 1,
            create_trigger: WeightedProfile::new(1),
            drop_trigger_weight: 1,

            // Transaction control - disabled by default (can cause issues with oracle)
            begin_weight: 0,
            commit_weight: 0,
            rollback_weight: 0,
            savepoint_weight: 0,
            release_weight: 0,

            // Utility - rare
            vacuum_weight: 0,
            analyze_weight: 0,
            reindex_weight: 0,

            // Global generation profile
            generation: GenerationProfile::default(),
        }
    }
}

impl StatementProfile {
    /// Create a profile with all weights set to zero.
    pub fn none() -> Self {
        Self {
            select: WeightedProfile::new(0),
            insert: WeightedProfile::new(0),
            update: WeightedProfile::new(0),
            delete: WeightedProfile::new(0),
            create_table: WeightedProfile::new(0),
            drop_table_weight: 0,
            alter_table: WeightedProfile::new(0),
            create_index: WeightedProfile::new(0),
            drop_index_weight: 0,
            create_view_weight: 0,
            drop_view_weight: 0,
            create_trigger: WeightedProfile::new(0),
            drop_trigger_weight: 0,
            begin_weight: 0,
            commit_weight: 0,
            rollback_weight: 0,
            savepoint_weight: 0,
            release_weight: 0,
            vacuum_weight: 0,
            analyze_weight: 0,
            reindex_weight: 0,
            generation: GenerationProfile::default(),
        }
    }

    /// Create a DML-only profile (no DDL, no transactions, no utility).
    pub fn dml_only() -> Self {
        Self {
            select: WeightedProfile::new(40),
            insert: WeightedProfile::new(30),
            update: WeightedProfile::new(20),
            delete: WeightedProfile::new(10),
            ..Self::none()
        }
    }

    /// Create a read-only profile (SELECT only).
    pub fn read_only() -> Self {
        Self::none().with_select(100)
    }

    /// Create a write-heavy profile (mostly INSERT/UPDATE, no DDL).
    pub fn write_heavy() -> Self {
        Self {
            select: WeightedProfile::new(10),
            insert: WeightedProfile::new(50),
            update: WeightedProfile::new(30),
            delete: WeightedProfile::new(10),
            ..Self::none()
        }
    }

    /// Create a profile without DELETE statements.
    pub fn no_delete() -> Self {
        Self {
            delete: WeightedProfile::new(0),
            ..Self::default()
        }
    }

    /// Create a DDL-only profile (schema changes only).
    pub fn ddl_only() -> Self {
        Self {
            create_table: WeightedProfile::new(20),
            drop_table_weight: 15,
            alter_table: WeightedProfile::new(15),
            create_index: WeightedProfile::new(20),
            drop_index_weight: 10,
            create_view_weight: 10,
            drop_view_weight: 10,
            ..Self::none()
        }
    }

    /// Create a transaction-heavy profile for testing transaction handling.
    pub fn transaction_heavy() -> Self {
        Self {
            select: WeightedProfile::new(20),
            insert: WeightedProfile::new(20),
            update: WeightedProfile::new(10),
            delete: WeightedProfile::new(5),
            begin_weight: 10,
            commit_weight: 10,
            rollback_weight: 10,
            savepoint_weight: 8,
            release_weight: 7,
            ..Self::none()
        }
    }

    // Builder methods for DML

    /// Builder method to set SELECT weight.
    pub fn with_select(mut self, weight: u32) -> Self {
        self.select.weight = weight;
        self
    }

    /// Builder method to set SELECT profile with weight and generation settings.
    pub fn with_select_profile(mut self, profile: WeightedProfile<SelectProfile>) -> Self {
        self.select = profile;
        self
    }

    /// Builder method to set INSERT weight.
    pub fn with_insert(mut self, weight: u32) -> Self {
        self.insert.weight = weight;
        self
    }

    /// Builder method to set INSERT profile with weight and generation settings.
    pub fn with_insert_profile(mut self, profile: WeightedProfile<InsertProfile>) -> Self {
        self.insert = profile;
        self
    }

    /// Builder method to set UPDATE weight.
    pub fn with_update(mut self, weight: u32) -> Self {
        self.update.weight = weight;
        self
    }

    /// Builder method to set UPDATE profile with weight and generation settings.
    pub fn with_update_profile(mut self, profile: WeightedProfile<UpdateProfile>) -> Self {
        self.update = profile;
        self
    }

    /// Builder method to set DELETE weight.
    pub fn with_delete(mut self, weight: u32) -> Self {
        self.delete.weight = weight;
        self
    }

    /// Builder method to set DELETE profile with weight and generation settings.
    pub fn with_delete_profile(mut self, profile: WeightedProfile<DeleteProfile>) -> Self {
        self.delete = profile;
        self
    }

    // Builder methods for DDL - Tables

    /// Builder method to set CREATE TABLE weight.
    pub fn with_create_table(mut self, weight: u32) -> Self {
        self.create_table.weight = weight;
        self
    }

    /// Builder method to set CREATE TABLE profile with weight and generation settings.
    pub fn with_create_table_profile(
        mut self,
        profile: WeightedProfile<CreateTableProfile>,
    ) -> Self {
        self.create_table = profile;
        self
    }

    /// Builder method to set DROP TABLE weight.
    pub fn with_drop_table(mut self, weight: u32) -> Self {
        self.drop_table_weight = weight;
        self
    }

    /// Builder method to set ALTER TABLE weight.
    pub fn with_alter_table(mut self, weight: u32) -> Self {
        self.alter_table.weight = weight;
        self
    }

    /// Builder method to set ALTER TABLE profile with weight and operation weights.
    pub fn with_alter_table_profile(
        mut self,
        profile: WeightedProfile<AlterTableOpWeights>,
    ) -> Self {
        self.alter_table = profile;
        self
    }

    // Builder methods for DDL - Indexes

    /// Builder method to set CREATE INDEX weight.
    pub fn with_create_index(mut self, weight: u32) -> Self {
        self.create_index.weight = weight;
        self
    }

    /// Builder method to set CREATE INDEX profile with weight and generation settings.
    pub fn with_create_index_profile(
        mut self,
        profile: WeightedProfile<CreateIndexProfile>,
    ) -> Self {
        self.create_index = profile;
        self
    }

    /// Builder method to set DROP INDEX weight.
    pub fn with_drop_index(mut self, weight: u32) -> Self {
        self.drop_index_weight = weight;
        self
    }

    // Builder methods for DDL - Views

    /// Builder method to set CREATE VIEW weight.
    pub fn with_create_view(mut self, weight: u32) -> Self {
        self.create_view_weight = weight;
        self
    }

    /// Builder method to set DROP VIEW weight.
    pub fn with_drop_view(mut self, weight: u32) -> Self {
        self.drop_view_weight = weight;
        self
    }

    // Builder methods for DDL - Triggers

    /// Builder method to set CREATE TRIGGER weight.
    pub fn with_create_trigger(mut self, weight: u32) -> Self {
        self.create_trigger.weight = weight;
        self
    }

    /// Builder method to set CREATE TRIGGER profile with weight and generation settings.
    pub fn with_create_trigger_profile(
        mut self,
        profile: WeightedProfile<CreateTriggerProfile>,
    ) -> Self {
        self.create_trigger = profile;
        self
    }

    /// Builder method to set DROP TRIGGER weight.
    pub fn with_drop_trigger(mut self, weight: u32) -> Self {
        self.drop_trigger_weight = weight;
        self
    }

    // Builder methods for transaction control

    /// Builder method to set BEGIN weight.
    pub fn with_begin(mut self, weight: u32) -> Self {
        self.begin_weight = weight;
        self
    }

    /// Builder method to set COMMIT weight.
    pub fn with_commit(mut self, weight: u32) -> Self {
        self.commit_weight = weight;
        self
    }

    /// Builder method to set ROLLBACK weight.
    pub fn with_rollback(mut self, weight: u32) -> Self {
        self.rollback_weight = weight;
        self
    }

    /// Builder method to set SAVEPOINT weight.
    pub fn with_savepoint(mut self, weight: u32) -> Self {
        self.savepoint_weight = weight;
        self
    }

    /// Builder method to set RELEASE weight.
    pub fn with_release(mut self, weight: u32) -> Self {
        self.release_weight = weight;
        self
    }

    // Builder methods for utility

    /// Builder method to set VACUUM weight.
    pub fn with_vacuum(mut self, weight: u32) -> Self {
        self.vacuum_weight = weight;
        self
    }

    /// Builder method to set ANALYZE weight.
    pub fn with_analyze(mut self, weight: u32) -> Self {
        self.analyze_weight = weight;
        self
    }

    /// Builder method to set REINDEX weight.
    pub fn with_reindex(mut self, weight: u32) -> Self {
        self.reindex_weight = weight;
        self
    }

    // Builder method for global generation profile

    /// Builder method to set global generation profile.
    pub fn with_generation(mut self, profile: GenerationProfile) -> Self {
        self.generation = profile;
        self
    }

    /// Returns the total DML weight.
    pub fn dml_weight(&self) -> u32 {
        self.select.weight + self.insert.weight + self.update.weight + self.delete.weight
    }

    /// Returns the total DDL weight.
    pub fn ddl_weight(&self) -> u32 {
        self.create_table.weight
            + self.drop_table_weight
            + self.alter_table.weight
            + self.create_index.weight
            + self.drop_index_weight
            + self.create_view_weight
            + self.drop_view_weight
            + self.create_trigger.weight
            + self.drop_trigger_weight
    }

    /// Returns the total transaction control weight.
    pub fn transaction_weight(&self) -> u32 {
        self.begin_weight
            + self.commit_weight
            + self.rollback_weight
            + self.savepoint_weight
            + self.release_weight
    }

    /// Returns the total utility weight.
    pub fn utility_weight(&self) -> u32 {
        self.vacuum_weight + self.analyze_weight + self.reindex_weight
    }

    /// Returns the total weight (sum of all weights).
    pub fn total_weight(&self) -> u32 {
        self.dml_weight() + self.ddl_weight() + self.transaction_weight() + self.utility_weight()
    }

    /// Returns true if at least one statement type is enabled.
    pub fn has_enabled_statements(&self) -> bool {
        self.total_weight() > 0
    }

    /// Returns true if any DML statement is enabled.
    pub fn has_dml(&self) -> bool {
        self.dml_weight() > 0
    }

    /// Returns true if any DDL statement is enabled.
    pub fn has_ddl(&self) -> bool {
        self.ddl_weight() > 0
    }

    /// Returns true if any transaction statement is enabled.
    pub fn has_transaction(&self) -> bool {
        self.transaction_weight() > 0
    }

    /// Returns the weight for a given statement kind.
    pub fn weight_for(&self, kind: StatementKind) -> u32 {
        match kind {
            StatementKind::Select => self.select.weight,
            StatementKind::Insert => self.insert.weight,
            StatementKind::Update => self.update.weight,
            StatementKind::Delete => self.delete.weight,
            StatementKind::CreateTable => self.create_table.weight,
            StatementKind::DropTable => self.drop_table_weight,
            StatementKind::AlterTable => self.alter_table.weight,
            StatementKind::CreateIndex => self.create_index.weight,
            StatementKind::DropIndex => self.drop_index_weight,
            StatementKind::CreateView => self.create_view_weight,
            StatementKind::DropView => self.drop_view_weight,
            StatementKind::CreateTrigger => self.create_trigger.weight,
            StatementKind::DropTrigger => self.drop_trigger_weight,
            StatementKind::Begin => self.begin_weight,
            StatementKind::Commit => self.commit_weight,
            StatementKind::Rollback => self.rollback_weight,
            StatementKind::Savepoint => self.savepoint_weight,
            StatementKind::Release => self.release_weight,
            StatementKind::Vacuum => self.vacuum_weight,
            StatementKind::Analyze => self.analyze_weight,
            StatementKind::Reindex => self.reindex_weight,
        }
    }

    /// Returns an iterator over all statement kinds with weight > 0.
    pub fn enabled_statements(&self) -> impl Iterator<Item = (StatementKind, u32)> + '_ {
        StatementKind::iter()
            .map(|kind| (kind, self.weight_for(kind)))
            .filter(|(_, w)| *w > 0)
    }

    // Convenience accessor methods

    /// Returns the SELECT profile or default.
    pub fn select_profile(&self) -> &SelectProfile {
        &self.select.extra
    }

    /// Returns the INSERT profile or default.
    pub fn insert_profile(&self) -> &InsertProfile {
        &self.insert.extra
    }

    /// Returns the UPDATE profile or default.
    pub fn update_profile(&self) -> &UpdateProfile {
        &self.update.extra
    }

    /// Returns the DELETE profile or default.
    pub fn delete_profile(&self) -> &DeleteProfile {
        &self.delete.extra
    }

    /// Returns the CREATE TABLE profile or default.
    pub fn create_table_profile(&self) -> &CreateTableProfile {
        &self.create_table.extra
    }

    /// Returns the CREATE INDEX profile or default.
    pub fn create_index_profile(&self) -> &CreateIndexProfile {
        &self.create_index.extra
    }

    /// Returns the CREATE TRIGGER profile or default.
    pub fn create_trigger_profile(&self) -> &CreateTriggerProfile {
        &self.create_trigger.extra
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_profile() {
        let profile = StatementProfile::default();
        assert_eq!(profile.select.weight, 40);
        assert_eq!(profile.insert.weight, 25);
        assert_eq!(profile.update.weight, 15);
        assert_eq!(profile.delete.weight, 10);
        assert!(profile.has_dml());
        assert!(profile.has_ddl());
        assert!(!profile.has_transaction()); // Disabled by default
    }

    #[test]
    fn test_dml_only_profile() {
        let profile = StatementProfile::dml_only();
        assert!(profile.has_dml());
        assert!(!profile.has_ddl());
        assert!(!profile.has_transaction());
        assert_eq!(profile.ddl_weight(), 0);
    }

    #[test]
    fn test_ddl_only_profile() {
        let profile = StatementProfile::ddl_only();
        assert!(!profile.has_dml());
        assert!(profile.has_ddl());
        assert!(!profile.has_transaction());
        assert_eq!(profile.dml_weight(), 0);
    }

    #[test]
    fn test_transaction_heavy_profile() {
        let profile = StatementProfile::transaction_heavy();
        assert!(profile.has_dml());
        assert!(!profile.has_ddl());
        assert!(profile.has_transaction());
    }

    #[test]
    fn test_read_only_profile() {
        let profile = StatementProfile::read_only();
        assert_eq!(profile.select.weight, 100);
        assert_eq!(profile.insert.weight, 0);
        assert_eq!(profile.update.weight, 0);
        assert_eq!(profile.delete.weight, 0);
        assert_eq!(profile.ddl_weight(), 0);
    }

    #[test]
    fn test_no_delete_profile() {
        let profile = StatementProfile::no_delete();
        assert_eq!(profile.delete.weight, 0);
        assert!(profile.select.weight > 0);
        assert!(profile.insert.weight > 0);
        assert!(profile.update.weight > 0);
    }

    #[test]
    fn test_builder_pattern() {
        let profile = StatementProfile::none()
            .with_select(50)
            .with_insert(30)
            .with_create_table(20);
        assert_eq!(profile.select.weight, 50);
        assert_eq!(profile.insert.weight, 30);
        assert_eq!(profile.create_table.weight, 20);
        assert_eq!(profile.total_weight(), 100);
    }

    #[test]
    fn test_value_profile() {
        let profile = ValueProfile::default();
        assert_eq!(profile.text_max_length, 100);
        assert_eq!(profile.blob_max_size, 100);

        let minimal = ValueProfile::minimal();
        assert_eq!(minimal.text_max_length, 10);
    }

    #[test]
    fn test_condition_profile() {
        let profile = ConditionProfile::default();
        assert_eq!(profile.max_depth, 2);
        assert_eq!(profile.max_order_by_items, 3);

        let simple = ConditionProfile::simple();
        assert_eq!(simple.max_depth, 0);
    }

    #[test]
    fn test_select_profile() {
        let profile = SelectProfile::default();
        assert_eq!(profile.expression_max_depth, 2);
        assert!(profile.allow_aggregates);
        assert_eq!(*profile.limit_range.start(), 1);
        assert_eq!(*profile.limit_range.end(), 1000);

        let simple = SelectProfile::simple();
        assert!(!simple.allow_aggregates);
    }

    #[test]
    fn test_generation_profile() {
        let profile = GenerationProfile::default();
        assert_eq!(profile.value.text_max_length, 100);
        assert_eq!(profile.condition.max_depth, 2);

        let minimal = GenerationProfile::minimal();
        assert_eq!(minimal.value.text_max_length, 10);
    }

    #[test]
    fn test_statement_profile_with_profiles() {
        let select_profile = SelectProfile::complex();
        let profile = StatementProfile::default()
            .with_select_profile(WeightedProfile::with_extra(50, select_profile.clone()))
            .with_generation(GenerationProfile::minimal());

        assert_eq!(profile.select.weight, 50);
        assert_eq!(profile.generation.value.text_max_length, 10);
    }
}
