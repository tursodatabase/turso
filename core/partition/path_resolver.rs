//! Partition path resolution traits and implementations.
//!
//! The `PartitionPathResolver` trait allows users to customize how partition
//! file paths are generated based on timestamps. This enables flexible
//! directory structures for different use cases.

use std::path::{Path, PathBuf};

use chrono::{DateTime, NaiveDate, NaiveTime, TimeZone, Utc};

use super::error::PartitionError;

const DAY_MICROS: i64 = 86_400_000_000;

fn portable_path_component(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";

    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-') {
            encoded.push(char::from(byte));
        } else {
            encoded.push('~');
            encoded.push(char::from(HEX[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
    }
    encoded
}

fn interval_range_start(timestamp_micros: i64, interval_micros: i64) -> Option<i64> {
    if interval_micros <= 0 {
        return None;
    }
    timestamp_micros
        .div_euclid(interval_micros)
        .checked_mul(interval_micros)
}

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
            None => format!("{table}_{timestamp_micros}"),
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
            interval_micros: i64::try_from(interval_seconds)
                .ok()
                .and_then(|seconds| seconds.checked_mul(1_000_000))
                .unwrap_or(0),
        }
    }

    /// Create a daily partition resolver.
    pub fn daily(directory: PathBuf) -> Self {
        Self::new(directory, 86400)
    }
}

impl PartitionPathResolver for DefaultPathResolver {
    fn resolve_path(&self, table: &str, timestamp_micros: i64) -> PathBuf {
        let table = portable_path_component(table);
        let suffix = if self.interval_micros == DAY_MICROS {
            match micros_to_date(timestamp_micros) {
                Some(date) => date.format("%Y-%m-%d").to_string(),
                None => "invalid".to_string(),
            }
        } else {
            interval_range_start(timestamp_micros, self.interval_micros)
                .map_or_else(|| "invalid".to_string(), |start| start.to_string())
        };
        self.directory.join(format!("{table}_{suffix}.db"))
    }

    fn parse_path(&self, path: &Path) -> Option<(i64, i64)> {
        let filename = path.file_stem()?.to_str()?;
        let suffix = filename.rsplit('_').next()?;
        let start = if self.interval_micros == DAY_MICROS {
            let date = NaiveDate::parse_from_str(suffix, "%Y-%m-%d").ok()?;
            date_to_micros(date)?
        } else {
            suffix.parse::<i64>().ok()?
        };
        let end = start.checked_add(self.interval_micros)?;
        Some((start, end))
    }

    fn glob_pattern(&self, table: &str) -> String {
        let table = portable_path_component(table);
        format!(
            "{}/{}_*.db",
            glob::Pattern::escape(self.directory.to_string_lossy().as_ref()),
            glob::Pattern::escape(&table)
        )
    }

    fn interval_micros(&self) -> i64 {
        self.interval_micros
    }

    fn generate_alias(&self, table: &str, timestamp_micros: i64) -> String {
        if self.interval_micros == DAY_MICROS {
            return match micros_to_date(timestamp_micros) {
                Some(date) => format!("{}_{}", table, date.format("%Y%m%d")),
                None => format!("{table}_{timestamp_micros}"),
            };
        }
        match interval_range_start(timestamp_micros, self.interval_micros) {
            Some(start) => format!("{table}_range_{start}"),
            None => format!("{table}_invalid"),
        }
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
    pub fn new(
        base_dir: PathBuf,
        plugin_id: String,
        interval_seconds: u64,
    ) -> Result<Self, PartitionError> {
        validate_plugin_id(&plugin_id)?;
        Ok(Self {
            base_dir,
            plugin_id,
            interval_micros: i64::try_from(interval_seconds)
                .ok()
                .and_then(|seconds| seconds.checked_mul(1_000_000))
                .unwrap_or(0),
        })
    }

    /// Create a daily partition resolver for video analytics.
    pub fn daily(base_dir: PathBuf, plugin_id: String) -> Result<Self, PartitionError> {
        Self::new(base_dir, plugin_id, 86400)
    }
}

fn validate_plugin_id(plugin_id: &str) -> Result<(), PartitionError> {
    const MAX_PLUGIN_ID_LEN: usize = 128;

    if plugin_id.is_empty() {
        return Err(PartitionError::InvalidPluginId {
            plugin_id: plugin_id.to_string(),
            reason: "the id must not be empty".to_string(),
        });
    }
    if plugin_id.len() > MAX_PLUGIN_ID_LEN {
        return Err(PartitionError::InvalidPluginId {
            plugin_id: plugin_id.to_string(),
            reason: format!("the id must not exceed {MAX_PLUGIN_ID_LEN} bytes"),
        });
    }
    if !plugin_id
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err(PartitionError::InvalidPluginId {
            plugin_id: plugin_id.to_string(),
            reason: "use only ASCII letters, digits, '-' and '_'".to_string(),
        });
    }

    let uppercase = plugin_id.to_ascii_uppercase();
    let reserved = matches!(uppercase.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || uppercase.strip_prefix("COM").is_some_and(|suffix| {
            matches!(suffix, "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9")
        })
        || uppercase.strip_prefix("LPT").is_some_and(|suffix| {
            matches!(suffix, "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9")
        });
    if reserved {
        return Err(PartitionError::InvalidPluginId {
            plugin_id: plugin_id.to_string(),
            reason: "the id is a reserved Windows device name".to_string(),
        });
    }

    Ok(())
}

impl PartitionPathResolver for VideoAnalyticsPathResolver {
    fn resolve_path(&self, _table: &str, timestamp_micros: i64) -> PathBuf {
        let range_directory = if self.interval_micros == DAY_MICROS {
            match micros_to_date(timestamp_micros) {
                Some(date) => date.format("%Y-%m-%d").to_string(),
                None => "invalid".to_string(),
            }
        } else {
            interval_range_start(timestamp_micros, self.interval_micros)
                .map_or_else(|| "invalid".to_string(), |start| start.to_string())
        };

        self.base_dir
            .join(&range_directory)
            .join(format!("{}.bin", self.plugin_id))
    }

    fn parse_path(&self, path: &Path) -> Option<(i64, i64)> {
        // Verify filename matches plugin_id
        let filename = path.file_stem()?.to_str()?;
        if filename != self.plugin_id {
            return None;
        }

        let parent = path.parent()?;
        let range = parent.file_name()?.to_str()?;
        let start = if self.interval_micros == DAY_MICROS {
            let date = NaiveDate::parse_from_str(range, "%Y-%m-%d").ok()?;
            date_to_micros(date)?
        } else {
            range.parse::<i64>().ok()?
        };
        let end = start.checked_add(self.interval_micros)?;
        Some((start, end))
    }

    fn glob_pattern(&self, _table: &str) -> String {
        format!(
            "{}/*/{}.bin",
            glob::Pattern::escape(self.base_dir.to_string_lossy().as_ref()),
            glob::Pattern::escape(&self.plugin_id)
        )
    }

    fn interval_micros(&self) -> i64 {
        self.interval_micros
    }

    fn generate_alias(&self, _table: &str, timestamp_micros: i64) -> String {
        if self.interval_micros == DAY_MICROS {
            return match micros_to_date(timestamp_micros) {
                Some(date) => format!("{}_{}", self.plugin_id, date.format("%Y%m%d")),
                None => format!("{}_{timestamp_micros}", self.plugin_id),
            };
        }
        match interval_range_start(timestamp_micros, self.interval_micros) {
            Some(start) => format!("{}_range_{start}", self.plugin_id),
            None => format!("{}_invalid", self.plugin_id),
        }
    }
}

/// Convert microseconds to NaiveDate (UTC).
fn micros_to_date(timestamp_micros: i64) -> Option<NaiveDate> {
    let secs = timestamp_micros.div_euclid(1_000_000);
    let nsecs = timestamp_micros.rem_euclid(1_000_000).checked_mul(1_000)? as u32;
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
        let timestamp = 1_737_547_200_000_000_i64;
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
            VideoAnalyticsPathResolver::daily(PathBuf::from("/data"), "lpr_plugin".to_string())
                .unwrap();

        let timestamp = 1_737_547_200_000_000_i64; // 2025-01-22
        let path = resolver.resolve_path("events", timestamp);

        let path_str = path.to_str().unwrap();
        assert!(path_str.contains("2025-01-22"));
        assert!(path_str.contains("lpr_plugin.bin"));
    }

    #[test]
    fn test_video_analytics_resolver_parse_path() {
        let resolver =
            VideoAnalyticsPathResolver::daily(PathBuf::from("/data"), "lpr_plugin".to_string())
                .unwrap();

        let path = PathBuf::from("/data/2025-01-22/lpr_plugin.bin");
        let result = resolver.parse_path(&path);

        assert!(result.is_some());
    }

    #[test]
    fn test_video_analytics_resolver_wrong_plugin() {
        let resolver =
            VideoAnalyticsPathResolver::daily(PathBuf::from("/data"), "lpr_plugin".to_string())
                .unwrap();

        let path = PathBuf::from("/data/2025-01-22/other_plugin.bin");
        let result = resolver.parse_path(&path);

        assert!(result.is_none());
    }

    #[test]
    fn test_generate_alias() {
        let resolver = DefaultPathResolver::daily(PathBuf::from("/data"));

        let timestamp = 1_737_547_200_000_000_i64; // 2025-01-22
        let alias = resolver.generate_alias("events", timestamp);

        assert_eq!(alias, "events_20250122");
    }

    #[test]
    fn test_micros_to_date() {
        let timestamp = 1_737_547_200_000_000_i64; // 2025-01-22 12:00:00 UTC
        let date = micros_to_date(timestamp);

        assert!(date.is_some());
        let date = date.unwrap();
        assert_eq!(date.year(), 2025);
        assert_eq!(date.month(), 1);
        assert_eq!(date.day(), 22);
    }

    #[test]
    fn test_pre_epoch_timestamp_uses_previous_utc_day() {
        let resolver = DefaultPathResolver::daily(PathBuf::from("/data"));
        let path = resolver.resolve_path("events", -1);
        assert_eq!(path, PathBuf::from("/data/events_1969-12-31.db"));
        assert_eq!(resolver.parse_path(&path), Some((-86_400_000_000, 0)));
        assert_eq!(resolver.generate_alias("events", -1), "events_19691231");
    }

    #[test]
    fn test_discovery_patterns_escape_user_supplied_names() {
        let default = DefaultPathResolver::daily(PathBuf::from("/data/[camera]*"));
        assert_eq!(
            default.glob_pattern("events[0]*"),
            "/data/[[]camera[]][*]/events~5B0~5D~2A_*.db"
        );

        let video = VideoAnalyticsPathResolver::daily(
            PathBuf::from("/data/[camera]*"),
            "plugin[0]*".to_string(),
        );
        assert!(matches!(video, Err(PartitionError::InvalidPluginId { .. })));
    }

    #[test]
    fn test_default_resolver_encodes_table_names_as_one_portable_component() {
        let resolver = DefaultPathResolver::daily(PathBuf::from("/archive"));
        let path = resolver.resolve_path("../camera\\stream:0", 1_737_547_200_000_000);
        assert_eq!(
            path,
            PathBuf::from("/archive/~2E~2E~2Fcamera~5Cstream~3A0_2025-01-22.db")
        );
        assert!(path.starts_with("/archive"));
        assert_eq!(
            resolver.glob_pattern("../camera\\stream:0"),
            "/archive/~2E~2E~2Fcamera~5Cstream~3A0_*.db"
        );
        assert_eq!(
            resolver.parse_path(&path),
            Some((1_737_504_000_000_000, 1_737_590_400_000_000))
        );
    }

    #[test]
    fn test_subdaily_resolvers_generate_unique_roundtrippable_ranges() {
        let default = DefaultPathResolver::new(PathBuf::from("/data"), 3_600);
        let first = default.resolve_path("events", 1_000);
        let second = default.resolve_path("events", 3_600_000_000 + 1_000);
        assert_ne!(first, second);
        assert_eq!(default.parse_path(&first), Some((0, 3_600_000_000)));
        assert_eq!(
            default.parse_path(&second),
            Some((3_600_000_000, 7_200_000_000))
        );
        assert_ne!(
            default.generate_alias("events", 1_000),
            default.generate_alias("events", 3_600_000_000 + 1_000)
        );

        let video =
            VideoAnalyticsPathResolver::new(PathBuf::from("/data"), "plugin".to_string(), 3_600)
                .unwrap();
        let first = video.resolve_path("events", 1_000);
        let second = video.resolve_path("events", 3_600_000_000 + 1_000);
        assert_ne!(first, second);
        assert_eq!(video.parse_path(&first), Some((0, 3_600_000_000)));
        assert_eq!(
            video.parse_path(&second),
            Some((3_600_000_000, 7_200_000_000))
        );
    }

    #[test]
    fn test_video_analytics_resolver_rejects_non_portable_plugin_ids() {
        for plugin_id in [
            "",
            ".",
            "..",
            "../escape",
            "subdir/plugin",
            r"subdir\plugin",
            "plugin.bin",
            "plugin name",
            "NUL",
            "com1",
            "LPT9",
        ] {
            let result =
                VideoAnalyticsPathResolver::daily(PathBuf::from("/data"), plugin_id.to_string());
            assert!(
                matches!(result, Err(PartitionError::InvalidPluginId { .. })),
                "plugin id should be rejected: {plugin_id:?}"
            );
        }
    }

    #[test]
    fn test_video_analytics_resolver_accepts_portable_plugin_ids() {
        for plugin_id in ["line_crossing", "lpr-2", "Plugin42"] {
            let resolver =
                VideoAnalyticsPathResolver::daily(PathBuf::from("/data"), plugin_id.to_string())
                    .unwrap();
            assert_eq!(
                resolver.resolve_path("events", 0),
                PathBuf::from(format!("/data/1970-01-01/{plugin_id}.bin"))
            );
        }
    }
}
