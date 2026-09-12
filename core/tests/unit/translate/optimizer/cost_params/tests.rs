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
    assert!((params.rows_per_table_page - defaults.rows_per_table_page).abs() < f64::EPSILON);

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
        (params.rows_per_table_fallback - defaults.rows_per_table_fallback).abs() < f64::EPSILON
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
        (params.rows_per_table_fallback - defaults.rows_per_table_fallback).abs() < f64::EPSILON
    );
}
