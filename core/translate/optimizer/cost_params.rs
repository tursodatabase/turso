/// Cost model parameters for query optimization.
///
/// These parameters control the heuristics used by the query optimizer for
/// cost estimation. They can be tuned to improve plan selection for specific
/// workloads (e.g., TPC-H).
///
/// # JSON Loading (requires `optimizer_params` feature)
///
/// When the `optimizer_params` feature is enabled, parameters can be loaded
/// from a JSON file via the `TURSO_OPTIMIZER_PARAMS` environment variable.
/// The JSON file does not need to specify all fields, and unspecified fields will use the default values.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(default))]
pub struct CostModelParams {
    // === Cardinality Fallbacks (when no ANALYZE stats) ===
    /// Assumed rows per table when statistics unavailable.
    pub rows_per_table_fallback: f64,

    /// Estimated rows per B-tree page (affects IO cost calculations).
    pub rows_per_page: f64,

    // === Selectivity Fallbacks ===
    /// Selectivity for equality predicate on unindexed column (e.g., `x = 5`).
    pub sel_eq_unindexed: f64,

    /// Selectivity for equality predicate on indexed column.
    /// Should be <= sel_eq_unindexed since indexes imply higher selectivity.
    pub sel_eq_indexed: f64,

    /// Selectivity for range predicates (>, >=, <, <=).
    pub sel_range: f64,

    /// Selectivity for IS NULL predicate.
    pub sel_is_null: f64,

    /// Selectivity for IS NOT NULL predicate.
    pub sel_is_not_null: f64,

    /// Selectivity for LIKE predicate.
    pub sel_like: f64,

    /// Selectivity for NOT LIKE predicate.
    pub sel_not_like: f64,

    /// Selectivity for other/unknown predicates.
    pub sel_other: f64,

    /// Estimated rows from IN subquery when actual count unknown.
    /// Matches SQLite's estimate (where.c line 3230).
    pub in_subquery_rows: f64,

    // === Scan/Seek Cost Weights ===
    /// Discount factor for repeated scans (cache benefit).
    /// Range: [0, 1). Higher = more cache benefit assumed.
    pub cache_reuse_factor: f64,

    /// CPU cost per row processed (relative to page IO = 1.0).
    pub cpu_cost_per_row: f64,

    /// CPU cost per index seek (key comparisons).
    pub cpu_cost_per_seek: f64,

    /// Bonus subtracted from cost when using an index (encourages index usage).
    pub index_bonus: f64,

    /// Density multiplier for covering indexes (more rows fit per page
    /// because only indexed columns are stored).
    pub covering_index_density: f64,

    // === Sort Cost ===
    /// CPU cost per row for sorting (used in O(n log n) estimate).
    /// This is used when estimating the cost saved by using an ordered index.
    pub sort_cpu_per_row: f64,

    // === Hash Join Cost ===
    /// CPU cost to compute hash of a row.
    pub hash_cpu_cost: f64,

    /// CPU cost to insert row into hash table.
    pub hash_insert_cost: f64,

    /// CPU cost to probe hash table.
    pub hash_lookup_cost: f64,

    /// Estimated bytes per row for hash table spill estimation.
    pub hash_bytes_per_row: f64,

    /// Selectivity threshold for hash join build-side materialization.
    /// Below this threshold, materialization may be beneficial.
    pub hash_materialize_selectivity_threshold: f64,

    /// Stricter selectivity threshold for nested hash probe operations.
    pub hash_nested_probe_selectivity_threshold: f64,

    // === Join Optimization ===
    /// Selectivity heuristic factor for closed ranges (e.g., `x > 5 AND x < 10`).
    /// Applied when both lower and upper bounds exist on an index column.
    pub closed_range_selectivity_factor: f64,
    /// Fanout (how many rows are produced per lookup) for index seeks with unique index.
    pub fanout_index_seek_unique: f64,
    /// Fanout for index seeks with non-unique index.
    pub fanout_index_seek_non_unique: f64,
    /// Fanout for index seeks with unmatched columns.
    pub fanout_index_seek_per_unmatched_column: f64,
}

impl CostModelParams {
    /// Create default parameters as a const fn (for compile-time static).
    pub const fn new() -> Self {
        Self {
            // Cardinality fallbacks
            rows_per_table_fallback: 1_000_000.0,
            rows_per_page: 50.0,

            // Selectivity fallbacks
            sel_eq_unindexed: 0.1,
            sel_eq_indexed: 0.01,
            sel_range: 0.4,
            sel_is_null: 0.1,
            sel_is_not_null: 0.9,
            sel_like: 0.2,
            sel_not_like: 0.2,
            sel_other: 0.9,
            in_subquery_rows: 25.0,

            // Scan/Seek costs
            cache_reuse_factor: 0.2,
            cpu_cost_per_row: 0.001,
            cpu_cost_per_seek: 0.01,
            index_bonus: 0.5,
            covering_index_density: 2.0,

            // Sort costs
            sort_cpu_per_row: 0.002,

            // Hash join specific costs and thresholds
            hash_cpu_cost: 0.001,
            hash_insert_cost: 0.002,
            hash_lookup_cost: 0.003,
            hash_bytes_per_row: 100.0,
            hash_materialize_selectivity_threshold: 0.5,
            hash_nested_probe_selectivity_threshold: 0.15,

            // Join optimization
            closed_range_selectivity_factor: 0.2,
            fanout_index_seek_unique: 1.0,
            fanout_index_seek_non_unique: 2.0,
            fanout_index_seek_per_unmatched_column: 4.0,
        }
    }
}

/// Compile-time static default parameters (zero runtime overhead).
#[cfg(any(
    not(feature = "optimizer_params"),
    all(test, feature = "optimizer_params")
))]
pub static DEFAULT_PARAMS: CostModelParams = CostModelParams::new();

impl Default for CostModelParams {
    fn default() -> Self {
        Self::new()
    }
}

impl CostModelParams {
    /// Load parameters from a JSON file.
    ///
    /// Returns default parameters if the file cannot be read, parsed, or validated.
    #[cfg(feature = "optimizer_params")]
    pub fn load_from_file(path: &std::path::Path) -> Self {
        match std::fs::read_to_string(path) {
            Ok(contents) => match serde_json::from_str::<Self>(&contents) {
                Ok(params) => {
                    if let Err(e) = params.validate() {
                        tracing::warn!(?path, error = %e, "Invalid cost params, using defaults");
                        return Self::default();
                    }
                    tracing::info!(?path, "Loaded optimizer cost parameters from file");
                    params
                }
                Err(e) => {
                    tracing::warn!(?path, error = %e, "Failed to parse cost params JSON, using defaults");
                    Self::default()
                }
            },
            Err(e) => {
                tracing::warn!(?path, error = %e, "Failed to read cost params file, using defaults");
                Self::default()
            }
        }
    }

    /// Load parameters from the `TURSO_OPTIMIZER_PARAMS` environment variable path,
    /// or return defaults if not set or loading fails.
    #[cfg(feature = "optimizer_params")]
    fn from_env_or_default() -> Self {
        match std::env::var("TURSO_OPTIMIZER_PARAMS") {
            Ok(path) => Self::load_from_file(std::path::Path::new(&path)),
            Err(_) => Self::default(),
        }
    }
}

/// Lazily-loaded parameters from `TURSO_OPTIMIZER_PARAMS` env var (cached process-wide).
/// Falls back to defaults if env var not set or loading fails.
#[cfg(feature = "optimizer_params")]
pub static LOADED_PARAMS: std::sync::LazyLock<CostModelParams> =
    std::sync::LazyLock::new(CostModelParams::from_env_or_default);

#[cfg(feature = "optimizer_params")]
impl CostModelParams {
    /// Validate that parameters are within sensible bounds.
    ///
    /// Returns an error message if any parameter is invalid.
    #[cfg(feature = "optimizer_params")]
    pub fn validate(&self) -> Result<(), String> {
        // Selectivity must be in (0, 1]
        let selectivity_params = [
            ("sel_eq_unindexed", self.sel_eq_unindexed),
            ("sel_eq_indexed", self.sel_eq_indexed),
            ("sel_range", self.sel_range),
            ("sel_is_null", self.sel_is_null),
            ("sel_is_not_null", self.sel_is_not_null),
            ("sel_like", self.sel_like),
            ("sel_not_like", self.sel_not_like),
            ("sel_other", self.sel_other),
        ];

        for (name, val) in selectivity_params {
            if val <= 0.0 || val > 1.0 {
                return Err(format!("{name} must be in (0, 1], got {val}"));
            }
        }

        // Indexed selectivity should be <= unindexed (indexes are more selective)
        if self.sel_eq_indexed > self.sel_eq_unindexed {
            return Err(format!(
                "sel_eq_indexed ({}) should be <= sel_eq_unindexed ({})",
                self.sel_eq_indexed, self.sel_eq_unindexed
            ));
        }

        // Positive value checks
        if self.rows_per_table_fallback <= 0.0 {
            return Err("rows_per_table_fallback must be positive".into());
        }
        if self.rows_per_page <= 0.0 {
            return Err("rows_per_page must be positive".into());
        }
        if self.in_subquery_rows <= 0.0 {
            return Err("in_subquery_rows must be positive".into());
        }

        // Cache reuse factor must be in [0, 1)
        if self.cache_reuse_factor < 0.0 || self.cache_reuse_factor >= 1.0 {
            return Err(format!(
                "cache_reuse_factor must be in [0, 1), got {}",
                self.cache_reuse_factor
            ));
        }

        // Cost multipliers must be non-negative
        let cost_params = [
            ("cpu_cost_per_row", self.cpu_cost_per_row),
            ("cpu_cost_per_seek", self.cpu_cost_per_seek),
            ("sort_cpu_per_row", self.sort_cpu_per_row),
            ("hash_cpu_cost", self.hash_cpu_cost),
            ("hash_insert_cost", self.hash_insert_cost),
            ("hash_lookup_cost", self.hash_lookup_cost),
        ];

        for (name, val) in cost_params {
            if val < 0.0 {
                return Err(format!("{name} must be non-negative, got {val}"));
            }
        }

        // Covering index density must be >= 1.0
        if self.covering_index_density < 1.0 {
            return Err(format!(
                "covering_index_density must be >= 1.0, got {}",
                self.covering_index_density
            ));
        }

        Ok(())
    }
}

#[cfg(all(test, feature = "optimizer_params"))]
mod tests {
    use super::*;

    #[test]
    fn test_default_params_are_valid() {
        let params = CostModelParams::default();
        assert!(params.validate().is_ok());
    }

    #[test]
    fn test_invalid_selectivity_rejected() {
        let mut params = CostModelParams {
            sel_eq_unindexed: 1.5,
            ..Default::default()
        };
        assert!(params.validate().is_err());

        params = CostModelParams {
            sel_range: 0.0,
            ..Default::default()
        };
        assert!(params.validate().is_err());

        params = CostModelParams {
            sel_is_null: -0.1,
            ..Default::default()
        };
        assert!(params.validate().is_err());
    }

    #[test]
    fn test_indexed_selectivity_constraint() {
        let params = CostModelParams {
            sel_eq_indexed: 0.5,
            sel_eq_unindexed: 0.1,
            ..Default::default()
        };
        assert!(params.validate().is_err());
    }

    #[test]
    fn test_cache_reuse_bounds() {
        let mut params = CostModelParams {
            cache_reuse_factor: 1.0,
            ..Default::default()
        };
        assert!(params.validate().is_err());

        params.cache_reuse_factor = -0.1;
        assert!(params.validate().is_err());

        params.cache_reuse_factor = 0.99;
        assert!(params.validate().is_ok());
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_serde_roundtrip() {
        let params = CostModelParams::default();
        let json = serde_json::to_string(&params).unwrap();
        let parsed: CostModelParams = serde_json::from_str(&json).unwrap();
        assert!((params.sel_eq_unindexed - parsed.sel_eq_unindexed).abs() < f64::EPSILON);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_partial_json_uses_defaults() {
        let defaults = CostModelParams::new();
        let json = r#"{"sel_eq_unindexed": 0.05}"#;
        let params: CostModelParams = serde_json::from_str(json).unwrap();
        assert!((params.sel_eq_unindexed - 0.05).abs() < f64::EPSILON);
        // Other fields should be defaults
        assert!((params.sel_range - defaults.sel_range).abs() < f64::EPSILON);
    }

    #[test]
    fn test_load_from_file() {
        let dir = std::env::temp_dir();
        let path = dir.join("test_cost_params.json");
        let defaults = CostModelParams::new();

        // Write a partial JSON file - unspecified fields should use defaults
        let json = r#"{
            "sel_eq_unindexed": 0.15,
            "sel_eq_indexed": 0.005,
            "rows_per_table_fallback": 500000.0
        }"#;
        std::fs::write(&path, json).unwrap();

        let params = CostModelParams::load_from_file(&path);

        // Specified values should be loaded
        assert!((params.sel_eq_unindexed - 0.15).abs() < f64::EPSILON);
        assert!((params.sel_eq_indexed - 0.005).abs() < f64::EPSILON);
        assert!((params.rows_per_table_fallback - 500000.0).abs() < f64::EPSILON);

        // Unspecified values should be defaults
        assert!((params.sel_range - defaults.sel_range).abs() < f64::EPSILON);
        assert!((params.rows_per_page - defaults.rows_per_page).abs() < f64::EPSILON);

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_load_from_file_invalid_json_returns_defaults() {
        let dir = std::env::temp_dir();
        let path = dir.join("test_invalid_cost_params.json");
        let defaults = CostModelParams::new();

        std::fs::write(&path, "not valid json {{{").unwrap();

        let params = CostModelParams::load_from_file(&path);

        // Should return defaults on parse error
        assert!((params.sel_eq_unindexed - defaults.sel_eq_unindexed).abs() < f64::EPSILON);
        assert!(
            (params.rows_per_table_fallback - defaults.rows_per_table_fallback).abs()
                < f64::EPSILON
        );

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_load_from_file_missing_returns_defaults() {
        let path = std::path::Path::new("/nonexistent/path/to/params.json");
        let defaults = CostModelParams::new();

        let params = CostModelParams::load_from_file(path);

        // Should return defaults when file doesn't exist
        assert!((params.sel_eq_unindexed - defaults.sel_eq_unindexed).abs() < f64::EPSILON);
        assert!(
            (params.rows_per_table_fallback - defaults.rows_per_table_fallback).abs()
                < f64::EPSILON
        );
    }
}
