//! Partition path resolution traits and implementations.
//!
//! The `PartitionPathResolver` trait allows users to customize how partition
//! file paths are generated based on timestamps. This enables flexible
//! directory structures for different use cases.

use std::path::{Path, PathBuf};

use chrono::{DateTime, NaiveDate, NaiveTime, TimeZone, Utc};

/// Trait for generating paths to partition files.
///
/// Implement this trait to customize how partition files are named and organized.
/// The default implementation uses a simple date-based naming scheme.
///
/// # Example
///
/// ```ignore
/// use turso_core::partition::path_resolver::{PartitionPathResolver, DefaultPathResolver};
/// use std::path::PathBuf;
///
/// let resolver = DefaultPathResolver::new(PathBuf::from("/data"), 86400);
/// let path = resolver.resolve_path("events", 1737500000);
/// // Returns: /data/events_2025-01-22.db
/// ```
pub trait PartitionPathResolver: Send + Sync {
    /// Generate the path to a partition file for a given timestamp.
    ///
    /// # Arguments
    /// * `table` - Name of the table being partitioned
    /// * `timestamp_micros` - Unix timestamp in microseconds
    ///
    /// # Returns
    /// Full path to the partition file
    fn resolve_path(&self, table: &str, timestamp_micros: i64) -> PathBuf;

    /// Parse a path back into a timestamp range.
    ///
    /// Used during discovery of existing partition files.
    ///
    /// # Arguments
    /// * `path` - Path to a potential partition file
    ///
    /// # Returns
    /// `Some((start_micros, end_micros))` if the path matches the expected format
    /// `None` if the path doesn't match
    fn parse_path(&self, path: &Path) -> Option<(i64, i64)>;

    /// Get a glob pattern for discovering existing partition files.
    ///
    /// # Arguments
    /// * `table` - Name of the table
    ///
    /// # Returns
    /// A glob pattern string (e.g., "/data/events_*.db")
    fn glob_pattern(&self, table: &str) -> String;

    /// Get the partition interval in microseconds.
    ///
    /// Default is 86,400,000,000 (24 hours in microseconds).
    fn interval_micros(&self) -> i64 {
        86_400_000_000 // 24 hours in microseconds
    }

    /// Generate a database alias for the partition.
    ///
    /// This alias is used when ATTACHing the partition database.
    ///
    /// # Arguments
    /// * `table` - Name of the table
    /// * `timestamp_micros` - Unix timestamp in microseconds
    ///
    /// # Returns
    /// Database alias string (e.g., "events_20250122")
    fn generate_alias(&self, table: &str, timestamp_micros: i64) -> String {
        let date = micros_to_date(timestamp_micros);
        match date {
            Some(d) => format!("{}_{}", table, d.format("%Y%m%d")),
            None => format!("{}_{}", table, timestamp_micros),
        }
    }
}

/// Default path resolver with simple date-based naming.
///
/// Generates paths in the format: `{directory}/{table}_{YYYY-MM-DD}.db`
#[derive(Clone, Debug)]
pub struct DefaultPathResolver {
    /// Base directory for partition files
    pub directory: PathBuf,
    /// Partition interval in microseconds
    pub interval_micros: i64,
}

impl DefaultPathResolver {
    /// Create a new default path resolver.
    ///
    /// # Arguments
    /// * `directory` - Base directory for partition files
    /// * `interval_seconds` - Partition interval in seconds (typically 86400 for daily)
    pub fn new(directory: PathBuf, interval_seconds: u64) -> Self {
        Self {
            directory,
            interval_micros: (interval_seconds as i64) * 1_000_000,
        }
    }

    /// Create a daily partition resolver.
    pub fn daily(directory: PathBuf) -> Self {
        Self::new(directory, 86400)
    }
}

impl PartitionPathResolver for DefaultPathResolver {
    fn resolve_path(&self, table: &str, timestamp_micros: i64) -> PathBuf {
        let date_str = match micros_to_date(timestamp_micros) {
            Some(date) => date.format("%Y-%m-%d").to_string(),
            None => "invalid".to_string(),
        };
        self.directory.join(format!("{}_{}.db", table, date_str))
    }

    fn parse_path(&self, path: &Path) -> Option<(i64, i64)> {
        let filename = path.file_stem()?.to_str()?;
        // Parse "events_2025-01-22" -> extract date
        let date_part = filename.rsplit('_').next()?;
        let date = NaiveDate::parse_from_str(date_part, "%Y-%m-%d").ok()?;

        let start = date_to_micros(date)?;
        let end = start + self.interval_micros;
        Some((start, end))
    }

    fn glob_pattern(&self, table: &str) -> String {
        format!("{}/{}_*.db", self.directory.display(), table)
    }

    fn interval_micros(&self) -> i64 {
        self.interval_micros
    }
}

/// Video analytics path resolver.
///
/// Generates paths in the format: `{base_dir}/{YYYY-MM-DD}/{plugin_id}.bin`
/// Suitable for organizing data by date with plugin-specific files.
#[derive(Clone, Debug)]
pub struct VideoAnalyticsPathResolver {
    /// Base directory for partition files
    pub base_dir: PathBuf,
    /// Plugin identifier (used as filename)
    pub plugin_id: String,
    /// Partition interval in microseconds
    pub interval_micros: i64,
}

impl VideoAnalyticsPathResolver {
    /// Create a new video analytics path resolver.
    ///
    /// # Arguments
    /// * `base_dir` - Base directory for partition files
    /// * `plugin_id` - Plugin identifier (used as filename without extension)
    /// * `interval_seconds` - Partition interval in seconds
    pub fn new(base_dir: PathBuf, plugin_id: String, interval_seconds: u64) -> Self {
        Self {
            base_dir,
            plugin_id,
            interval_micros: (interval_seconds as i64) * 1_000_000,
        }
    }

    /// Create a daily partition resolver for video analytics.
    pub fn daily(base_dir: PathBuf, plugin_id: String) -> Self {
        Self::new(base_dir, plugin_id, 86400)
    }
}

impl PartitionPathResolver for VideoAnalyticsPathResolver {
    fn resolve_path(&self, _table: &str, timestamp_micros: i64) -> PathBuf {
        let date_str = match micros_to_date(timestamp_micros) {
            Some(date) => date.format("%Y-%m-%d").to_string(),
            None => "invalid".to_string(),
        };

        // /data/2025-01-05/plugin_lpr.bin
        self.base_dir
            .join(&date_str)
            .join(format!("{}.bin", self.plugin_id))
    }

    fn parse_path(&self, path: &Path) -> Option<(i64, i64)> {
        // Verify filename matches plugin_id
        let filename = path.file_stem()?.to_str()?;
        if filename != self.plugin_id {
            return None;
        }

        // Parse date from parent directory
        let parent = path.parent()?;
        let date_str = parent.file_name()?.to_str()?;
        let date = NaiveDate::parse_from_str(date_str, "%Y-%m-%d").ok()?;

        let start = date_to_micros(date)?;
        let end = start + self.interval_micros;
        Some((start, end))
    }

    fn glob_pattern(&self, _table: &str) -> String {
        format!("{}/*/{}.bin", self.base_dir.display(), self.plugin_id)
    }

    fn interval_micros(&self) -> i64 {
        self.interval_micros
    }

    fn generate_alias(&self, _table: &str, timestamp_micros: i64) -> String {
        let date = micros_to_date(timestamp_micros);
        match date {
            Some(d) => format!("{}_{}", self.plugin_id, d.format("%Y%m%d")),
            None => format!("{}_{}", self.plugin_id, timestamp_micros),
        }
    }
}

/// Convert microseconds to NaiveDate (UTC).
fn micros_to_date(timestamp_micros: i64) -> Option<NaiveDate> {
    let secs = timestamp_micros / 1_000_000;
    let nsecs = ((timestamp_micros % 1_000_000) * 1_000) as u32;
    let dt = DateTime::from_timestamp(secs, nsecs)?;
    Some(dt.date_naive())
}

/// Convert NaiveDate to start-of-day microseconds (UTC).
fn date_to_micros(date: NaiveDate) -> Option<i64> {
    let midnight = NaiveTime::from_hms_opt(0, 0, 0)?;
    let datetime = date.and_time(midnight);
    let utc_datetime = Utc.from_utc_datetime(&datetime);
    Some(utc_datetime.timestamp_micros())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;

    #[test]
    fn test_default_resolver_resolve_path() {
        let resolver = DefaultPathResolver::daily(PathBuf::from("/data"));

        // 2025-01-22 12:00:00 UTC in microseconds
        let timestamp = 1737547200_000_000i64;
        let path = resolver.resolve_path("events", timestamp);

        assert!(path.to_str().unwrap().contains("events_2025-01-22.db"));
    }

    #[test]
    fn test_default_resolver_parse_path() {
        let resolver = DefaultPathResolver::daily(PathBuf::from("/data"));

        let path = PathBuf::from("/data/events_2025-01-22.db");
        let result = resolver.parse_path(&path);

        assert!(result.is_some());
        let (start, end) = result.unwrap();
        // Check that end - start is 24 hours in microseconds
        assert_eq!(end - start, 86_400_000_000);
    }

    #[test]
    fn test_default_resolver_glob_pattern() {
        let resolver = DefaultPathResolver::daily(PathBuf::from("/data"));
        let pattern = resolver.glob_pattern("events");
        assert_eq!(pattern, "/data/events_*.db");
    }

    #[test]
    fn test_video_analytics_resolver_resolve_path() {
        let resolver =
            VideoAnalyticsPathResolver::daily(PathBuf::from("/data"), "lpr_plugin".to_string());

        let timestamp = 1737547200_000_000i64; // 2025-01-22
        let path = resolver.resolve_path("events", timestamp);

        let path_str = path.to_str().unwrap();
        assert!(path_str.contains("2025-01-22"));
        assert!(path_str.contains("lpr_plugin.bin"));
    }

    #[test]
    fn test_video_analytics_resolver_parse_path() {
        let resolver =
            VideoAnalyticsPathResolver::daily(PathBuf::from("/data"), "lpr_plugin".to_string());

        let path = PathBuf::from("/data/2025-01-22/lpr_plugin.bin");
        let result = resolver.parse_path(&path);

        assert!(result.is_some());
    }

    #[test]
    fn test_video_analytics_resolver_wrong_plugin() {
        let resolver =
            VideoAnalyticsPathResolver::daily(PathBuf::from("/data"), "lpr_plugin".to_string());

        let path = PathBuf::from("/data/2025-01-22/other_plugin.bin");
        let result = resolver.parse_path(&path);

        assert!(result.is_none());
    }

    #[test]
    fn test_generate_alias() {
        let resolver = DefaultPathResolver::daily(PathBuf::from("/data"));

        let timestamp = 1737547200_000_000i64; // 2025-01-22
        let alias = resolver.generate_alias("events", timestamp);

        assert_eq!(alias, "events_20250122");
    }

    #[test]
    fn test_micros_to_date() {
        let timestamp = 1737547200_000_000i64; // 2025-01-22 12:00:00 UTC
        let date = micros_to_date(timestamp);

        assert!(date.is_some());
        let date = date.unwrap();
        assert_eq!(date.year(), 2025);
        assert_eq!(date.month(), 1);
        assert_eq!(date.day(), 22);
    }
}
