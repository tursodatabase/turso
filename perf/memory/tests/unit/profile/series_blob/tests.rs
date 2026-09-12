use super::*;

#[test]
fn series_blob_profile_uses_batch_size_as_series_stop() {
    let mut profile = SeriesBlob::new(2, 7);

    let (phase, batches) = profile.next_batch(3);
    assert_eq!(phase, Phase::Setup);
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].len(), 1);
    assert_eq!(
        batches[0][0].sql,
        "CREATE TABLE IF NOT EXISTS bench (id INTEGER PRIMARY KEY, data BLOB NOT NULL)"
    );
    assert!(batches[0][0].params.is_empty());

    let (phase, batches) = profile.next_batch(3);
    assert_eq!(phase, Phase::Run);
    assert_eq!(batches.len(), 3);

    for batch in batches {
        assert_eq!(batch.len(), 1);
        let item = &batch[0];
        assert_eq!(
            item.sql,
            "INSERT INTO bench (data) SELECT zeroblob(?) FROM generate_series(1, ?)"
        );
        assert_eq!(item.params.len(), 2);
        assert_eq!(item.params[0], turso::Value::Integer(BLOB_SIZE_BYTES));
        assert_eq!(item.params[1], turso::Value::Integer(7));
    }
}

#[test]
fn series_blob_profile_stops_after_iterations() {
    let mut profile = SeriesBlob::new(1, 4);

    let (phase, _) = profile.next_batch(1);
    assert_eq!(phase, Phase::Setup);

    let (phase, _) = profile.next_batch(1);
    assert_eq!(phase, Phase::Run);

    let (phase, batches) = profile.next_batch(1);
    assert_eq!(phase, Phase::Done);
    assert!(batches.is_empty());
}
