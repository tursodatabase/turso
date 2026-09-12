use super::{DatabaseMetadata, DatabaseSavedConfiguration, RemotePullProtocol};

#[test]
fn update_configuration_reports_change_only_when_values_differ() {
    let mut meta = DatabaseMetadata::load(
        br#"{
                "version": "v1",
                "client_unique_id": "client-a",
                "synced_revision": null,
                "revert_since_wal_salt": null,
                "revert_since_wal_watermark": 0,
                "last_pull_unix_time": null,
                "last_push_unix_time": null,
                "last_pushed_pull_gen_hint": 0,
                "last_pushed_change_id_hint": 0,
                "partial_bootstrap_server_revision": null,
                "saved_configuration": null
            }"#,
    )
    .unwrap();
    let configuration = DatabaseSavedConfiguration {
        remote_url: Some("http://remote".to_string()),
        partial_sync_prefetch: Some(true),
        partial_sync_segment_size: Some(4096),
    };

    assert!(meta.update_configuration(configuration.clone()));
    assert_eq!(meta.saved_configuration, Some(configuration.clone()));

    // Re-applying the identical configuration (what every open of an
    // existing replica does) must not report a change, otherwise every
    // open rewrites the metadata file.
    assert!(!meta.update_configuration(configuration.clone()));

    let updated = DatabaseSavedConfiguration {
        remote_url: Some("http://other-remote".to_string()),
        ..configuration
    };
    assert!(meta.update_configuration(updated.clone()));
    assert_eq!(meta.saved_configuration, Some(updated));
}

#[test]
fn metadata_load_defaults_missing_logical_table_map() {
    let meta = DatabaseMetadata::load(
        br#"{
                "version": "v1",
                "client_unique_id": "client-a",
                "synced_revision": null,
                "revert_since_wal_salt": null,
                "revert_since_wal_watermark": 0,
                "last_pull_unix_time": null,
                "last_push_unix_time": null,
                "last_pushed_pull_gen_hint": 0,
                "last_pushed_change_id_hint": 0,
                "partial_bootstrap_server_revision": null,
                "saved_configuration": null
            }"#,
    )
    .unwrap();

    assert_eq!(meta.last_pushed_replay_floor_change_id_hint, 0);
    assert!(!meta.fresh_bootstrap_pending_cdc_ack);
    assert!(!meta.logical_mvcc_pull_active());
    assert!(meta.logical_table_names_by_stable_id.is_empty());
}

#[test]
fn metadata_load_pins_pre_protocol_replicas_with_revisions_to_pages() {
    // A replica whose metadata predates remote_pull_protocol is a
    // page-protocol replica by definition (MVCC logical sync never
    // shipped without the field). Pinning it to Pages keeps existing
    // WAL replicas on the exact same code path after upgrading.
    let pages_meta = DatabaseMetadata::load(
        br#"{
                "version": "v1",
                "client_unique_id": "client-a",
                "synced_revision": {"type": "v1", "revision": "{\"generation\":3,\"wal_fragment_no\":7}"},
                "revert_since_wal_salt": null,
                "revert_since_wal_watermark": 0,
                "last_pull_unix_time": null,
                "last_push_unix_time": null,
                "last_pushed_pull_gen_hint": 0,
                "last_pushed_change_id_hint": 0,
                "partial_bootstrap_server_revision": null,
                "saved_configuration": null
            }"#,
    )
    .unwrap();
    assert_eq!(pages_meta.remote_pull_protocol, RemotePullProtocol::Pages);
    assert!(!pages_meta.logical_mvcc_pull_active());
}

#[test]
fn metadata_load_keeps_unknown_protocol_for_replicas_without_revision() {
    // Deferred-bootstrap replicas have never contacted the server; the
    // first pull must detect the protocol instead of assuming one.
    let meta = DatabaseMetadata::load(
        br#"{
                "version": "v1",
                "client_unique_id": "client-a",
                "synced_revision": null,
                "revert_since_wal_salt": null,
                "revert_since_wal_watermark": 0,
                "last_pull_unix_time": null,
                "last_push_unix_time": null,
                "last_pushed_pull_gen_hint": 0,
                "last_pushed_change_id_hint": 0,
                "partial_bootstrap_server_revision": null,
                "saved_configuration": null
            }"#,
    )
    .unwrap();
    assert_eq!(meta.remote_pull_protocol, RemotePullProtocol::Unknown);
}

#[test]
fn metadata_round_trips_remote_pull_protocol() {
    let mut meta = DatabaseMetadata::load(
        br#"{
                "version": "v1",
                "client_unique_id": "client-a",
                "synced_revision": null,
                "revert_since_wal_salt": null,
                "revert_since_wal_watermark": 0,
                "last_pull_unix_time": null,
                "last_push_unix_time": null,
                "last_pushed_pull_gen_hint": 0,
                "last_pushed_change_id_hint": 0,
                "partial_bootstrap_server_revision": null,
                "saved_configuration": null
            }"#,
    )
    .unwrap();
    meta.remote_pull_protocol = RemotePullProtocol::MvccLogical;
    let reloaded = DatabaseMetadata::load(&meta.dump().unwrap()).unwrap();
    assert_eq!(
        reloaded.remote_pull_protocol,
        RemotePullProtocol::MvccLogical
    );
    assert!(reloaded.logical_mvcc_pull_active());
}
