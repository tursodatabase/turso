//! Partition manager for coordinating partition operations.

use std::collections::HashMap;
use std::path::Path;

use super::error::PartitionError;
use super::file::{create_partition_file, open_partition_file, PartitionFile, PartitionInfo};
use super::path_resolver::PartitionPathResolver;

/// Configuration for a partitioned table.
pub struct PartitionConfig {
    /// Path resolver for generating partition file paths
    pub path_resolver: Box<dyn PartitionPathResolver>,
    /// SQL schema for creating new partitions
    pub schema_sql: String,
    /// Name of the partition column
    pub partition_column: String,
}

impl PartitionConfig {
    /// Create a new partition configuration.
    pub fn new(
        path_resolver: Box<dyn PartitionPathResolver>,
        schema_sql: String,
        partition_column: String,
    ) -> Self {
        Self {
            path_resolver,
            schema_sql,
            partition_column,
        }
    }
}

/// Manager for partitioned tables.
///
/// The PartitionManager tracks all partitioned tables and their associated
/// partition files. It handles routing inserts to the correct partition
/// and managing partition attachment/detachment.
pub struct PartitionManager {
    /// Table name -> partition configuration
    configs: HashMap<String, PartitionConfig>,
    /// Table name -> list of partition files
    partitions: HashMap<String, Vec<PartitionFile>>,
}

impl Default for PartitionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl PartitionManager {
    /// Create a new partition manager.
    pub fn new() -> Self {
        Self {
            configs: HashMap::new(),
            partitions: HashMap::new(),
        }
    }

    /// Register a table for partitioning.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to partition
    /// * `config` - Partition configuration
    ///
    /// # Returns
    /// Error if the table is already registered
    pub fn register_table(
        &mut self,
        table_name: &str,
        config: PartitionConfig,
    ) -> Result<(), PartitionError> {
        if self.configs.contains_key(table_name) {
            return Err(PartitionError::TableAlreadyRegistered(
                table_name.to_string(),
            ));
        }

        self.configs.insert(table_name.to_string(), config);
        self.partitions.insert(table_name.to_string(), Vec::new());
        Ok(())
    }

    /// Unregister a table from partitioning.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to unregister
    ///
    /// # Returns
    /// Error if the table is not registered
    pub fn unregister_table(&mut self, table_name: &str) -> Result<(), PartitionError> {
        if !self.configs.contains_key(table_name) {
            return Err(PartitionError::TableNotPartitioned(table_name.to_string()));
        }

        self.configs.remove(table_name);
        self.partitions.remove(table_name);
        Ok(())
    }

    /// Check if a table is registered for partitioning.
    pub fn is_partitioned(&self, table_name: &str) -> bool {
        self.configs.contains_key(table_name)
    }

    /// Get partition configuration for a table.
    pub fn get_config(&self, table_name: &str) -> Option<&PartitionConfig> {
        self.configs.get(table_name)
    }

    /// List all partitions for a table.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table
    ///
    /// # Returns
    /// List of partition info, or error if table is not partitioned
    pub fn list_partitions(&self, table_name: &str) -> Result<Vec<PartitionInfo>, PartitionError> {
        let partitions = self
            .partitions
            .get(table_name)
            .ok_or_else(|| PartitionError::TableNotPartitioned(table_name.to_string()))?;

        Ok(partitions.iter().map(|p| p.to_info()).collect())
    }

    /// Get all attached partitions for a table.
    pub fn get_attached_partitions(&self, table_name: &str) -> Vec<&PartitionFile> {
        self.partitions
            .get(table_name)
            .map(|parts| parts.iter().filter(|p| p.attached).collect())
            .unwrap_or_default()
    }

    /// Route an insert to the correct partition based on timestamp.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table
    /// * `timestamp_micros` - Timestamp value in microseconds
    ///
    /// # Returns
    /// Reference to the partition file that should receive the insert
    pub fn route_insert(
        &self,
        table_name: &str,
        timestamp_micros: i64,
    ) -> Result<&PartitionFile, PartitionError> {
        let partitions = self
            .partitions
            .get(table_name)
            .ok_or_else(|| PartitionError::TableNotPartitioned(table_name.to_string()))?;

        for partition in partitions {
            if partition.contains(timestamp_micros) && partition.attached {
                return Ok(partition);
            }
        }

        // No attached partition found for this timestamp
        Err(PartitionError::NotAttached {
            table: table_name.to_string(),
            partition: format!("timestamp {}", timestamp_micros),
        })
    }

    /// Ensure a partition exists for the given timestamp.
    ///
    /// If no partition exists for the timestamp, creates a new one.
    /// If a partition exists but is not attached, attaches it.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table
    /// * `timestamp_micros` - Timestamp value in microseconds
    ///
    /// # Returns
    /// The partition file for the given timestamp
    pub fn ensure_partition(
        &mut self,
        table_name: &str,
        timestamp_micros: i64,
    ) -> Result<&PartitionFile, PartitionError> {
        let config = self
            .configs
            .get(table_name)
            .ok_or_else(|| PartitionError::TableNotPartitioned(table_name.to_string()))?;

        // Calculate partition range
        let interval = config.path_resolver.interval_micros();
        let range_start = (timestamp_micros / interval) * interval;
        let range_end = range_start + interval;

        // Generate path and alias
        let path = config
            .path_resolver
            .resolve_path(table_name, timestamp_micros);
        let alias = config
            .path_resolver
            .generate_alias(table_name, timestamp_micros);

        // Check if partition already exists in our list
        let partitions = self
            .partitions
            .get_mut(table_name)
            .ok_or_else(|| PartitionError::TableNotPartitioned(table_name.to_string()))?;

        // Check if we already have this partition
        if let Some(idx) = partitions.iter().position(|p| p.range_start == range_start) {
            return Ok(&partitions[idx]);
        }

        // Create or open the partition file
        let partition = if path.exists() {
            open_partition_file(&path, alias, range_start, range_end)?
        } else {
            let schema_sql = config.schema_sql.clone();
            create_partition_file(&path, alias, range_start, range_end, &schema_sql)?
        };

        partitions.push(partition);

        // Sort by range_start for efficient lookups
        partitions.sort_by_key(|p| p.range_start);

        // Return reference to the newly added partition
        let idx = partitions
            .iter()
            .position(|p| p.range_start == range_start)
            .unwrap();
        Ok(&partitions[idx])
    }

    /// Attach a partition file.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table
    /// * `path` - Path to the partition file
    ///
    /// # Returns
    /// Info about the attached partition
    pub fn attach_partition(
        &mut self,
        table_name: &str,
        path: &Path,
    ) -> Result<PartitionInfo, PartitionError> {
        let config = self
            .configs
            .get(table_name)
            .ok_or_else(|| PartitionError::TableNotPartitioned(table_name.to_string()))?;

        // Parse the path to get the time range
        let (range_start, range_end) = config
            .path_resolver
            .parse_path(path)
            .ok_or_else(|| PartitionError::FileNotFound(path.to_path_buf()))?;

        // Generate alias
        let alias = config.path_resolver.generate_alias(table_name, range_start);

        // Open the partition file
        let mut partition = open_partition_file(path, alias, range_start, range_end)?;

        // TODO: Actually ATTACH the database
        // For now, just mark as attached
        partition.mark_attached(0);

        let info = partition.to_info();

        // Add to our list
        let partitions = self
            .partitions
            .get_mut(table_name)
            .ok_or_else(|| PartitionError::TableNotPartitioned(table_name.to_string()))?;

        // Check if already in list
        if let Some(existing) = partitions.iter_mut().find(|p| p.path == path.to_path_buf()) {
            existing.mark_attached(0);
            return Ok(existing.to_info());
        }

        partitions.push(partition);
        partitions.sort_by_key(|p| p.range_start);

        Ok(info)
    }

    /// Detach a partition file.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table
    /// * `path` - Path to the partition file
    pub fn detach_partition(
        &mut self,
        table_name: &str,
        path: &Path,
    ) -> Result<(), PartitionError> {
        let partitions = self
            .partitions
            .get_mut(table_name)
            .ok_or_else(|| PartitionError::TableNotPartitioned(table_name.to_string()))?;

        let partition = partitions
            .iter_mut()
            .find(|p| p.path == path.to_path_buf())
            .ok_or_else(|| PartitionError::FileNotFound(path.to_path_buf()))?;

        partition.mark_detached();

        Ok(())
    }

    /// Update the database_id for a partition after ATTACH.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table
    /// * `path` - Path to the partition file
    /// * `database_id` - Database ID from the connection
    pub fn update_partition_database_id(
        &mut self,
        table_name: &str,
        path: &Path,
        database_id: usize,
    ) {
        if let Some(partitions) = self.partitions.get_mut(table_name) {
            if let Some(partition) = partitions.iter_mut().find(|p| p.path == path.to_path_buf()) {
                partition.mark_attached(database_id);
            }
        }
    }

    /// Get the alias for a partition by path.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table
    /// * `path` - Path to the partition file
    ///
    /// # Returns
    /// The database alias if found
    pub fn get_partition_alias(&self, table_name: &str, path: &Path) -> Option<String> {
        self.partitions.get(table_name).and_then(|partitions| {
            partitions
                .iter()
                .find(|p| p.path == path.to_path_buf())
                .map(|p| p.db_alias.clone())
        })
    }

    /// Get partition by database alias.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table
    /// * `alias` - Database alias
    ///
    /// # Returns
    /// Reference to the partition file if found
    pub fn get_partition_by_alias(&self, table_name: &str, alias: &str) -> Option<&PartitionFile> {
        self.partitions
            .get(table_name)
            .and_then(|partitions| partitions.iter().find(|p| p.db_alias == alias))
    }

    /// Get partition by database ID.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table
    /// * `database_id` - Database ID
    ///
    /// # Returns
    /// Reference to the partition file if found
    pub fn get_partition_by_database_id(
        &self,
        table_name: &str,
        database_id: usize,
    ) -> Option<&PartitionFile> {
        self.partitions.get(table_name).and_then(|partitions| {
            partitions
                .iter()
                .find(|p| p.database_id == Some(database_id))
        })
    }

    /// Filter partitions by time range.
    ///
    /// Used for partition pruning in SELECT queries.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table
    /// * `start` - Optional start of range (inclusive)
    /// * `end` - Optional end of range (exclusive)
    ///
    /// # Returns
    /// List of partitions that overlap with the given range
    pub fn filter_by_range(
        &self,
        table_name: &str,
        start: Option<i64>,
        end: Option<i64>,
    ) -> Vec<&PartitionFile> {
        let partitions = match self.partitions.get(table_name) {
            Some(p) => p,
            None => return Vec::new(),
        };

        let start = start.unwrap_or(i64::MIN);
        let end = end.unwrap_or(i64::MAX);

        partitions
            .iter()
            .filter(|p| p.attached && p.overlaps(start, end))
            .collect()
    }

    /// Discover existing partition files on disk.
    ///
    /// Scans the filesystem for partition files matching the configured pattern.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table
    ///
    /// # Returns
    /// List of discovered partition infos
    pub fn discover_partitions(
        &mut self,
        table_name: &str,
    ) -> Result<Vec<PartitionInfo>, PartitionError> {
        let config = self
            .configs
            .get(table_name)
            .ok_or_else(|| PartitionError::TableNotPartitioned(table_name.to_string()))?;

        let pattern = config.path_resolver.glob_pattern(table_name);
        let mut discovered = Vec::new();

        // Use glob to find matching files
        if let Ok(paths) = glob::glob(&pattern) {
            for entry in paths.flatten() {
                if let Some((range_start, range_end)) = config.path_resolver.parse_path(&entry) {
                    let alias = config.path_resolver.generate_alias(table_name, range_start);

                    if let Ok(partition) =
                        open_partition_file(&entry, alias, range_start, range_end)
                    {
                        discovered.push(partition.to_info());
                    }
                }
            }
        }

        discovered.sort_by_key(|p| p.range_start);
        Ok(discovered)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition::path_resolver::DefaultPathResolver;
    use std::path::PathBuf;

    fn create_test_config() -> PartitionConfig {
        let resolver = Box::new(DefaultPathResolver::daily(PathBuf::from(
            "/tmp/test_partitions",
        )));
        PartitionConfig::new(
            resolver,
            "CREATE TABLE events (id INTEGER, ts INTEGER, data TEXT)".to_string(),
            "ts".to_string(),
        )
    }

    #[test]
    fn test_register_table() {
        let mut manager = PartitionManager::new();

        assert!(!manager.is_partitioned("events"));

        manager
            .register_table("events", create_test_config())
            .unwrap();

        assert!(manager.is_partitioned("events"));
    }

    #[test]
    fn test_register_table_duplicate() {
        let mut manager = PartitionManager::new();

        manager
            .register_table("events", create_test_config())
            .unwrap();

        let result = manager.register_table("events", create_test_config());
        assert!(matches!(
            result,
            Err(PartitionError::TableAlreadyRegistered(_))
        ));
    }

    #[test]
    fn test_unregister_table() {
        let mut manager = PartitionManager::new();

        manager
            .register_table("events", create_test_config())
            .unwrap();
        assert!(manager.is_partitioned("events"));

        manager.unregister_table("events").unwrap();
        assert!(!manager.is_partitioned("events"));
    }

    #[test]
    fn test_list_partitions_empty() {
        let mut manager = PartitionManager::new();
        manager
            .register_table("events", create_test_config())
            .unwrap();

        let partitions = manager.list_partitions("events").unwrap();
        assert!(partitions.is_empty());
    }

    #[test]
    fn test_route_insert_no_partitions() {
        let mut manager = PartitionManager::new();
        manager
            .register_table("events", create_test_config())
            .unwrap();

        let result = manager.route_insert("events", 1737547200_000_000);
        assert!(matches!(result, Err(PartitionError::NotAttached { .. })));
    }

    #[test]
    fn test_filter_by_range() {
        let mut manager = PartitionManager::new();
        manager
            .register_table("events", create_test_config())
            .unwrap();

        // Add some test partitions
        let partitions = manager.partitions.get_mut("events").unwrap();
        let mut p1 = PartitionFile::new(
            PathBuf::from("/tmp/p1.db"),
            "events_20250121".to_string(),
            1737417600_000_000, // 2025-01-21
            1737504000_000_000, // 2025-01-22
        );
        p1.mark_attached(1);

        let mut p2 = PartitionFile::new(
            PathBuf::from("/tmp/p2.db"),
            "events_20250122".to_string(),
            1737504000_000_000, // 2025-01-22
            1737590400_000_000, // 2025-01-23
        );
        p2.mark_attached(2);

        partitions.push(p1);
        partitions.push(p2);

        // Test filtering
        let filtered = manager.filter_by_range(
            "events",
            Some(1737460000_000_000), // Jan 21 midday
            Some(1737547200_000_000), // Jan 22 midday
        );

        assert_eq!(filtered.len(), 2);
    }

    #[test]
    fn test_route_insert_with_attached_partition() {
        let mut manager = PartitionManager::new();
        manager
            .register_table("events", create_test_config())
            .unwrap();

        // Add an attached partition
        let partitions = manager.partitions.get_mut("events").unwrap();
        let mut p1 = PartitionFile::new(
            PathBuf::from("/tmp/p1.db"),
            "events_20250122".to_string(),
            1737504000_000_000, // 2025-01-22 00:00:00 UTC
            1737590400_000_000, // 2025-01-23 00:00:00 UTC
        );
        p1.mark_attached(1);
        partitions.push(p1);

        // Route should succeed for timestamp in range
        let result = manager.route_insert("events", 1737547200_000_000); // Jan 22 midday
        assert!(result.is_ok());
        let partition = result.unwrap();
        assert_eq!(partition.db_alias, "events_20250122");

        // Route should fail for timestamp outside range
        let result = manager.route_insert("events", 1737417600_000_000); // Jan 21
        assert!(matches!(result, Err(PartitionError::NotAttached { .. })));
    }

    #[test]
    fn test_get_attached_partitions() {
        let mut manager = PartitionManager::new();
        manager
            .register_table("events", create_test_config())
            .unwrap();

        // Initially no attached partitions
        let attached = manager.get_attached_partitions("events");
        assert!(attached.is_empty());

        // Add partitions - one attached, one not
        let partitions = manager.partitions.get_mut("events").unwrap();
        let mut p1 = PartitionFile::new(
            PathBuf::from("/tmp/p1.db"),
            "events_20250121".to_string(),
            1737417600_000_000,
            1737504000_000_000,
        );
        p1.mark_attached(1);

        let p2 = PartitionFile::new(
            PathBuf::from("/tmp/p2.db"),
            "events_20250122".to_string(),
            1737504000_000_000,
            1737590400_000_000,
        );
        // p2 is not attached

        partitions.push(p1);
        partitions.push(p2);

        // Should only return attached partitions
        let attached = manager.get_attached_partitions("events");
        assert_eq!(attached.len(), 1);
        assert_eq!(attached.first().unwrap().db_alias, "events_20250121");
    }

    #[test]
    fn test_filter_by_range_unbounded() {
        let mut manager = PartitionManager::new();
        manager
            .register_table("events", create_test_config())
            .unwrap();

        // Add attached partitions
        let partitions = manager.partitions.get_mut("events").unwrap();
        for i in 21usize..=25 {
            let day_offset = (i - 20) as i64;
            let start = 1737331200_000_000 + day_offset * 86400_000_000;
            let mut p = PartitionFile::new(
                PathBuf::from(format!("/tmp/p{}.db", i)),
                format!("events_202501{}", i),
                start,
                start + 86400_000_000,
            );
            p.mark_attached(i);
            partitions.push(p);
        }

        // Filter with no bounds - should get all attached
        let all = manager.filter_by_range("events", None, None);
        assert_eq!(all.len(), 5);

        // Filter with only start bound
        let from_22 = manager.filter_by_range("events", Some(1737504000_000_000), None);
        assert_eq!(from_22.len(), 4); // Jan 22, 23, 24, 25

        // Filter with only end bound
        let before_24 = manager.filter_by_range("events", None, Some(1737676800_000_000));
        assert_eq!(before_24.len(), 3); // Jan 21, 22, 23
    }

    #[test]
    fn test_unregister_table_not_found() {
        let mut manager = PartitionManager::new();

        // Unregistering non-existent table should fail
        let result = manager.unregister_table("nonexistent");
        assert!(matches!(
            result,
            Err(PartitionError::TableNotPartitioned(_))
        ));
    }
}
