use super::apply_experimental_features;

#[test]
fn apply_experimental_features_maps_feature_list() {
    // No features -> defaults.
    assert_eq!(
        apply_experimental_features(turso_core::DatabaseOpts::new(), &[]),
        turso_core::DatabaseOpts::new()
    );

    let features: Vec<String> = [
        "views",
        "index_method",
        "custom_types",
        "autovacuum",
        "vacuum",
        "encryption",
        "attach",
        "generated_columns",
        "multiprocess_wal",
        "without_rowid",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    let opts = apply_experimental_features(turso_core::DatabaseOpts::new(), &features);
    assert!(opts.enable_views);
    assert!(opts.enable_index_method);
    assert!(opts.enable_custom_types);
    assert!(opts.enable_autovacuum);
    assert!(opts.enable_vacuum);
    assert!(opts.enable_encryption);
    assert!(opts.enable_attach);
    assert!(opts.enable_generated_columns);
    assert!(opts.enable_multiprocess_wal);
    assert!(opts.enable_without_rowid);

    // `strict` and unknown names are no-ops.
    assert_eq!(
        apply_experimental_features(
            turso_core::DatabaseOpts::new(),
            &["strict".to_string(), "unknown".to_string()]
        ),
        turso_core::DatabaseOpts::new()
    );
}
