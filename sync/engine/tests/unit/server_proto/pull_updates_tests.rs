use super::{
    MvccLogicalLogMetadataProto, MvccLogicalLogRangeProto, PageSetRawEncodingProto,
    PageUpdatesEncodingReq, PullUpdatesApplyMode, PullUpdatesProtocol, PullUpdatesReqProtoBody,
    PullUpdatesRespProtoBody, PullUpdatesStreamKind,
};
use prost::Message;

#[test]
fn pull_updates_request_stream_kind_round_trips_proto() {
    let req = PullUpdatesReqProtoBody {
        encoding: PageUpdatesEncodingReq::Raw as i32,
        stream_kind: PullUpdatesStreamKind::MvccLogicalLog as i32,
        server_revision: "server-rev".to_string(),
        client_revision: "client-rev".to_string(),
        long_poll_timeout_ms: 123,
        server_pages_selector: Vec::new().into(),
        server_query_selector: String::new(),
        client_pages: Vec::new().into(),
    };

    let decoded = PullUpdatesReqProtoBody::decode(req.encode_to_vec().as_slice()).unwrap();
    assert_eq!(
        PullUpdatesStreamKind::try_from(decoded.stream_kind).unwrap(),
        PullUpdatesStreamKind::MvccLogicalLog
    );
}

#[test]
fn pull_updates_mvcc_log_header_round_trips_metadata() {
    let header = PullUpdatesRespProtoBody {
        server_revision: "rev-42".to_string(),
        protocol: PullUpdatesProtocol::MvccLogical as i32,
        db_size: 3,
        raw_encoding: Some(PageSetRawEncodingProto {}),
        zstd_encoding: None,
        stream_kind: PullUpdatesStreamKind::MvccLogicalLog as i32,
        apply_mode: PullUpdatesApplyMode::Incremental as i32,
        mvcc_log: Some(MvccLogicalLogMetadataProto {
            format: "lml3".to_string(),
            checkpoint_transition: true,
            ranges: vec![MvccLogicalLogRangeProto {
                generation: 7,
                start_offset: 11,
                end_offset: 99,
                starts_with_header: false,
                crc_seed: Some(vec![1, 2, 3, 4]),
            }],
        }),
    };

    let decoded = PullUpdatesRespProtoBody::decode_length_delimited(
        header.encode_length_delimited_to_vec().as_slice(),
    )
    .unwrap();
    assert_eq!(
        PullUpdatesStreamKind::try_from(decoded.stream_kind).unwrap(),
        PullUpdatesStreamKind::MvccLogicalLog
    );
    assert_eq!(
        PullUpdatesApplyMode::try_from(decoded.apply_mode).unwrap(),
        PullUpdatesApplyMode::Incremental
    );
    let mvcc_log = decoded.mvcc_log.unwrap();
    assert_eq!(mvcc_log.format, "lml3");
    assert!(mvcc_log.checkpoint_transition);
    assert_eq!(mvcc_log.ranges[0].end_offset, 99);
    assert_eq!(
        mvcc_log.ranges[0].crc_seed.as_deref(),
        Some(&[1, 2, 3, 4][..])
    );
}
