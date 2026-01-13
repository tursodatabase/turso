//! Profiles for controlling SQL statement generation.
//!
//! Profiles allow fine-grained control over which types of statements are
//! generated and with what relative frequency.

use strum::IntoEnumIterator;

use crate::alter_table::AlterTableOpWeights;
use crate::statement::StatementKind;

/// A weighted profile that holds an overall weight and optional detailed settings.
///
/// This generic struct allows statement types to have both:
/// - A weight determining how often this statement type is generated relative to others
/// - Optional detailed settings for fine-grained control within that statement type
#[derive(Debug, Clone)]
pub struct WeightedProfile<T> {
    /// The overall weight for this statement type.
    pub weight: u32,
    /// Optional detailed settings for fine-grained control.
    pub extra: Option<T>,
}

impl<T> WeightedProfile<T> {
    /// Create a new weighted profile with the given weight and no detail.
    pub const fn new(weight: u32) -> Self {
        Self {
            weight,
            extra: None,
        }
    }

    /// Create a new weighted profile with weight and detail.
    pub const fn with_detail(weight: u32, detail: T) -> Self {
        Self {
            weight,
            extra: Some(detail),
        }
    }

    /// Builder method to set the weight.
    pub fn weight(mut self, weight: u32) -> Self {
        self.weight = weight;
        self
    }

    /// Builder method to set the detail.
    pub fn extra(mut self, detail: T) -> Self {
        self.extra = Some(detail);
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
            extra: None,
        }
    }
}

/// Profile controlling SQL statement generation weights.
///
/// Each weight determines the relative probability of generating that
/// statement type. A weight of 0 disables that statement type entirely.
///
/// Statement types are divided into:
/// - **DML (Data Manipulation)**: SELECT, INSERT, UPDATE, DELETE
/// - **DDL (Data Definition)**: CREATE TABLE, CREATE INDEX, DROP TABLE, DROP INDEX,
///   ALTER TABLE, CREATE VIEW, DROP VIEW
/// - **Transaction control**: BEGIN, COMMIT, ROLLBACK, SAVEPOINT, RELEASE
/// - **Utility**: VACUUM, ANALYZE, REINDEX
#[derive(Debug, Clone)]
pub struct StatementProfile {
    // DML weights
    pub select_weight: u32,
    pub insert_weight: u32,
    pub update_weight: u32,
    pub delete_weight: u32,

    // DDL weights - Tables
    pub create_table_weight: u32,
    pub drop_table_weight: u32,
    pub alter_table_weight: u32,

    // DDL weights - Indexes
    pub create_index_weight: u32,
    pub drop_index_weight: u32,

    // DDL weights - Views
    pub create_view_weight: u32,
    pub drop_view_weight: u32,

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

    // Sub-profiles for fine-grained control
    /// Operation weights for ALTER TABLE statements.
    /// When `None`, uses default weights for ALTER TABLE operations.
    pub alter_table_op_weights: Option<AlterTableOpWeights>,
}

impl Default for StatementProfile {
    fn default() -> Self {
        Self {
            // DML - most common operations
            select_weight: 40,
            insert_weight: 25,
            update_weight: 15,
            delete_weight: 10,

            // DDL - less frequent
            create_table_weight: 2,
            drop_table_weight: 1,
            alter_table_weight: 1,
            create_index_weight: 2,
            drop_index_weight: 1,
            create_view_weight: 1,
            drop_view_weight: 1,

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

            // Sub-profiles
            alter_table_op_weights: None,
        }
    }
}

impl StatementProfile {
    /// Create a profile with all weights set to zero.
    pub fn none() -> Self {
        Self {
            select_weight: 0,
            insert_weight: 0,
            update_weight: 0,
            delete_weight: 0,
            create_table_weight: 0,
            drop_table_weight: 0,
            alter_table_weight: 0,
            create_index_weight: 0,
            drop_index_weight: 0,
            create_view_weight: 0,
            drop_view_weight: 0,
            begin_weight: 0,
            commit_weight: 0,
            rollback_weight: 0,
            savepoint_weight: 0,
            release_weight: 0,
            vacuum_weight: 0,
            analyze_weight: 0,
            reindex_weight: 0,
            alter_table_op_weights: None,
        }
    }

    /// Create a DML-only profile (no DDL, no transactions, no utility).
    pub fn dml_only() -> Self {
        Self {
            select_weight: 40,
            insert_weight: 30,
            update_weight: 20,
            delete_weight: 10,
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
            select_weight: 10,
            insert_weight: 50,
            update_weight: 30,
            delete_weight: 10,
            ..Self::none()
        }
    }

    /// Create a profile without DELETE statements.
    pub fn no_delete() -> Self {
        Self {
            delete_weight: 0,
            ..Self::default()
        }
    }

    /// Create a DDL-only profile (schema changes only).
    pub fn ddl_only() -> Self {
        Self {
            create_table_weight: 20,
            drop_table_weight: 15,
            alter_table_weight: 15,
            create_index_weight: 20,
            drop_index_weight: 10,
            create_view_weight: 10,
            drop_view_weight: 10,
            ..Self::none()
        }
    }

    /// Create a transaction-heavy profile for testing transaction handling.
    pub fn transaction_heavy() -> Self {
        Self {
            select_weight: 20,
            insert_weight: 20,
            update_weight: 10,
            delete_weight: 5,
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
        self.select_weight = weight;
        self
    }

    /// Builder method to set INSERT weight.
    pub fn with_insert(mut self, weight: u32) -> Self {
        self.insert_weight = weight;
        self
    }

    /// Builder method to set UPDATE weight.
    pub fn with_update(mut self, weight: u32) -> Self {
        self.update_weight = weight;
        self
    }

    /// Builder method to set DELETE weight.
    pub fn with_delete(mut self, weight: u32) -> Self {
        self.delete_weight = weight;
        self
    }

    // Builder methods for DDL - Tables

    /// Builder method to set CREATE TABLE weight.
    pub fn with_create_table(mut self, weight: u32) -> Self {
        self.create_table_weight = weight;
        self
    }

    /// Builder method to set DROP TABLE weight.
    pub fn with_drop_table(mut self, weight: u32) -> Self {
        self.drop_table_weight = weight;
        self
    }

    /// Builder method to set ALTER TABLE weight.
    pub fn with_alter_table(mut self, weight: u32) -> Self {
        self.alter_table_weight = weight;
        self
    }

    // Builder methods for DDL - Indexes

    /// Builder method to set CREATE INDEX weight.
    pub fn with_create_index(mut self, weight: u32) -> Self {
        self.create_index_weight = weight;
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

    // Builder methods for sub-profiles

    /// Builder method to set ALTER TABLE operation weights.
    pub fn with_alter_table_op_weights(mut self, op_weights: AlterTableOpWeights) -> Self {
        self.alter_table_op_weights = Some(op_weights);
        self
    }

    /// Returns the total DML weight.
    pub fn dml_weight(&self) -> u32 {
        self.select_weight + self.insert_weight + self.update_weight + self.delete_weight
    }

    /// Returns the total DDL weight.
    pub fn ddl_weight(&self) -> u32 {
        self.create_table_weight
            + self.drop_table_weight
            + self.alter_table_weight
            + self.create_index_weight
            + self.drop_index_weight
            + self.create_view_weight
            + self.drop_view_weight
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
            StatementKind::Select => self.select_weight,
            StatementKind::Insert => self.insert_weight,
            StatementKind::Update => self.update_weight,
            StatementKind::Delete => self.delete_weight,
            StatementKind::CreateTable => self.create_table_weight,
            StatementKind::DropTable => self.drop_table_weight,
            StatementKind::AlterTable => self.alter_table_weight,
            StatementKind::CreateIndex => self.create_index_weight,
            StatementKind::DropIndex => self.drop_index_weight,
            StatementKind::CreateView => self.create_view_weight,
            StatementKind::DropView => self.drop_view_weight,
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_profile() {
        let profile = StatementProfile::default();
        assert_eq!(profile.select_weight, 40);
        assert_eq!(profile.insert_weight, 25);
        assert_eq!(profile.update_weight, 15);
        assert_eq!(profile.delete_weight, 10);
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
        assert_eq!(profile.select_weight, 100);
        assert_eq!(profile.insert_weight, 0);
        assert_eq!(profile.update_weight, 0);
        assert_eq!(profile.delete_weight, 0);
        assert_eq!(profile.ddl_weight(), 0);
    }

    #[test]
    fn test_no_delete_profile() {
        let profile = StatementProfile::no_delete();
        assert_eq!(profile.delete_weight, 0);
        assert!(profile.select_weight > 0);
        assert!(profile.insert_weight > 0);
        assert!(profile.update_weight > 0);
    }

    #[test]
    fn test_builder_pattern() {
        let profile = StatementProfile::none()
            .with_select(50)
            .with_insert(30)
            .with_create_table(20);
        assert_eq!(profile.select_weight, 50);
        assert_eq!(profile.insert_weight, 30);
        assert_eq!(profile.create_table_weight, 20);
        assert_eq!(profile.total_weight(), 100);
    }
}
