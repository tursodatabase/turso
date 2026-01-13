//! Profiles for controlling SQL statement generation.
//!
//! Profiles allow fine-grained control over which types of statements are
//! generated and with what relative frequency.

use strum::IntoEnumIterator;

use crate::statement::StatementKind;

/// Profile controlling SQL statement generation weights.
///
/// Each weight determines the relative probability of generating that
/// statement type. A weight of 0 disables that statement type entirely.
///
/// Statement types are divided into:
/// - **DML (Data Manipulation)**: SELECT, INSERT, UPDATE, DELETE
/// - **DDL (Data Definition)**: CREATE TABLE, CREATE INDEX, DROP TABLE, DROP INDEX
#[derive(Debug, Clone)]
pub struct StatementProfile {
    // DML weights
    pub select_weight: u32,
    pub insert_weight: u32,
    pub update_weight: u32,
    pub delete_weight: u32,

    // DDL weights
    pub create_table_weight: u32,
    pub create_index_weight: u32,
    pub drop_table_weight: u32,
    pub drop_index_weight: u32,
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
            create_table_weight: 3,
            create_index_weight: 3,
            drop_table_weight: 2,
            drop_index_weight: 2,
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
            create_index_weight: 0,
            drop_table_weight: 0,
            drop_index_weight: 0,
        }
    }

    /// Create a DML-only profile (no DDL).
    pub fn dml_only() -> Self {
        Self {
            select_weight: 40,
            insert_weight: 30,
            update_weight: 20,
            delete_weight: 10,
            create_table_weight: 0,
            create_index_weight: 0,
            drop_table_weight: 0,
            drop_index_weight: 0,
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
            create_table_weight: 0,
            create_index_weight: 0,
            drop_table_weight: 0,
            drop_index_weight: 0,
        }
    }

    /// Create a profile without DELETE statements.
    pub fn no_delete() -> Self {
        Self {
            select_weight: 45,
            insert_weight: 30,
            update_weight: 20,
            delete_weight: 0,
            create_table_weight: 2,
            create_index_weight: 2,
            drop_table_weight: 1,
            drop_index_weight: 0,
        }
    }

    /// Create a DDL-only profile (schema changes only).
    pub fn ddl_only() -> Self {
        Self {
            select_weight: 0,
            insert_weight: 0,
            update_weight: 0,
            delete_weight: 0,
            create_table_weight: 30,
            create_index_weight: 30,
            drop_table_weight: 20,
            drop_index_weight: 20,
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

    // Builder methods for DDL

    /// Builder method to set CREATE TABLE weight.
    pub fn with_create_table(mut self, weight: u32) -> Self {
        self.create_table_weight = weight;
        self
    }

    /// Builder method to set CREATE INDEX weight.
    pub fn with_create_index(mut self, weight: u32) -> Self {
        self.create_index_weight = weight;
        self
    }

    /// Builder method to set DROP TABLE weight.
    pub fn with_drop_table(mut self, weight: u32) -> Self {
        self.drop_table_weight = weight;
        self
    }

    /// Builder method to set DROP INDEX weight.
    pub fn with_drop_index(mut self, weight: u32) -> Self {
        self.drop_index_weight = weight;
        self
    }

    /// Returns the total DML weight.
    pub fn dml_weight(&self) -> u32 {
        self.select_weight + self.insert_weight + self.update_weight + self.delete_weight
    }

    /// Returns the total DDL weight.
    pub fn ddl_weight(&self) -> u32 {
        self.create_table_weight
            + self.create_index_weight
            + self.drop_table_weight
            + self.drop_index_weight
    }

    /// Returns the total weight (sum of all weights).
    pub fn total_weight(&self) -> u32 {
        self.dml_weight() + self.ddl_weight()
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

    /// Returns the weight for a given statement kind.
    pub fn weight_for(&self, kind: StatementKind) -> u32 {
        match kind {
            StatementKind::Select => self.select_weight,
            StatementKind::Insert => self.insert_weight,
            StatementKind::Update => self.update_weight,
            StatementKind::Delete => self.delete_weight,
            StatementKind::CreateTable => self.create_table_weight,
            StatementKind::CreateIndex => self.create_index_weight,
            StatementKind::DropTable => self.drop_table_weight,
            StatementKind::DropIndex => self.drop_index_weight,
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
        assert_eq!(profile.create_table_weight, 3);
        assert_eq!(profile.create_index_weight, 3);
        assert_eq!(profile.drop_table_weight, 2);
        assert_eq!(profile.drop_index_weight, 2);
        assert_eq!(profile.total_weight(), 100);
    }

    #[test]
    fn test_dml_only_profile() {
        let profile = StatementProfile::dml_only();
        assert!(profile.has_dml());
        assert!(!profile.has_ddl());
        assert_eq!(profile.ddl_weight(), 0);
    }

    #[test]
    fn test_ddl_only_profile() {
        let profile = StatementProfile::ddl_only();
        assert!(!profile.has_dml());
        assert!(profile.has_ddl());
        assert_eq!(profile.dml_weight(), 0);
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
