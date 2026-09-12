use super::*;

#[test]
fn control_record_round_trips_and_detects_corruption() {
    let control = FtsControlV2::new(0xdead_beef);
    let bytes = control.encode();
    assert_eq!(FtsControlV2::decode(&bytes).unwrap(), control);

    assert!(FtsControlV2::decode(&bytes[..bytes.len() - 1]).is_err());
    let mut corrupted = bytes;
    corrupted[9] ^= 0xff;
    assert!(FtsControlV2::decode(&corrupted).is_err());
}

#[test]
fn segment_descriptor_round_trips() {
    let segment_id = SegmentId::generate_random();
    let descriptor = SegmentDescriptor {
        segment_id,
        max_doc: 42,
        files: vec![
            SegmentFileEntry {
                name: format!("{}.term", segment_id.uuid_string()),
                size: 1234,
                num_chunks: 1,
            },
            SegmentFileEntry {
                name: format!("{}.store", segment_id.uuid_string()),
                size: 5 * 1024 * 1024,
                num_chunks: 10,
            },
        ],
    };
    let bytes = descriptor.encode().unwrap();
    assert_eq!(
        SegmentDescriptor::decode(segment_id, &bytes).unwrap(),
        descriptor
    );

    let mut corrupted = bytes;
    *corrupted.last_mut().unwrap() ^= 0x01;
    assert!(SegmentDescriptor::decode(segment_id, &corrupted).is_err());
}

#[test]
fn alive_bitset_marks_exactly_the_tombstoned_docs() {
    let deleted = BTreeSet::from([0u32, 3, 64, 129]);
    let bitset = alive_bitset(130, &deleted);
    assert_eq!(bitset.num_alive_docs(), 130 - deleted.len());
    for doc in 0..130 {
        assert_eq!(
            bitset.is_deleted(doc),
            deleted.contains(&doc),
            "doc {doc} has the wrong liveness"
        );
    }
}

#[test]
fn alive_bitset_with_no_tombstones_keeps_every_doc() {
    let bitset = alive_bitset(65, &BTreeSet::new());
    assert_eq!(bitset.num_alive_docs(), 65);
    assert!(!bitset.is_deleted(64));
}
