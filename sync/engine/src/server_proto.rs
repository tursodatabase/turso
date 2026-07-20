
use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
#[derive(prost::Enumeration)]
#[repr(i32)]
pub enum PageUpdatesEncodingReq {
    Raw = 0,
    Zstd = 1,
}

#[derive(prost::Message)]
pub struct PullUpdatesReqProtoBody {
    /// requested encoding of the pages
    #[prost(enumeration = "PageUpdatesEncodingReq", tag = "1")]
    pub encoding: i32,
    /// requested update stream kind
    ///
    /// Kept at tag 8 so older boolean clients remain wire-compatible:
    /// false/absent decodes as Pages(0), true decodes as MvccLogicalLog(1).
    #[prost(enumeration = "PullUpdatesStreamKind", tag = "8")]
    pub stream_kind: i32,
    /// revision of the requested pages on server side; can be None - in which case server will pick latest revision
    #[prost(string, tag = "2")]
    pub server_revision: String,
    /// client revision
    #[prost(string, tag = "3")]
    pub client_revision: String,
    /// timeout to wait for new changes before returning empty response; used only if client_revision is set and server_revision is not
    #[prost(uint32, tag = "4")]
    pub long_poll_timeout_ms: u32,
    /// server pages to select for sending; empty set will be interpreted as request for all pages
    /// if not empty - then server_pages_selector holds bytes for RoaringBitmap with bits set for pages to return
    #[prost(bytes, tag = "5")]
    pub server_pages_selector: Bytes,
    /// server query which select pages for sending
    #[prost(string, tag = "7")]
    pub server_query_selector: String,
    /// client pages
    #[prost(bytes, tag = "6")]
    pub client_pages: Bytes,
}

#[derive(prost::Message, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct PageData {
    #[prost(uint64, tag = "1")]
    pub page_id: u64,

    #[serde(with = "bytes_as_base64_pad")]
    #[prost(bytes, tag = "2")]
    pub encoded_page: Bytes,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Copy)]
#[serde(rename_all = "snake_case")]
#[derive(prost::Enumeration)]
#[repr(i32)]
pub enum PullUpdatesStreamKind {
    Pages = 0,
    MvccLogicalLog = 1,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Copy)]
#[serde(rename_all = "snake_case")]
#[derive(prost::Enumeration)]
#[repr(i32)]
pub enum PullUpdatesApplyMode {
    Incremental = 0,
    ReplaceBase = 1,
}

#[derive(prost::Message)]
pub struct PageSetRawEncodingProto {}

#[derive(prost::Message)]
pub struct PageSetZstdEncodingProto {
    #[prost(int32, tag = "1")]
    pub level: i32,
    #[prost(uint32, repeated, tag = "2")]
    pub pages_dict: Vec<u32>,
}

#[derive(prost::Message, Clone, PartialEq, Eq)]
pub struct MvccLogicalLogRangeProto {
    #[prost(uint64, tag = "1")]
    pub generation: u64,
    #[prost(uint64, tag = "2")]
    pub start_offset: u64,
    #[prost(uint64, tag = "3")]
    pub end_offset: u64,
    #[prost(bool, tag = "4")]
    pub starts_with_header: bool,
    #[prost(bytes, optional, tag = "5")]
    pub crc_seed: Option<Vec<u8>>,
}

#[derive(prost::Message, Clone, PartialEq, Eq)]
pub struct MvccLogicalLogMetadataProto {
    #[prost(string, tag = "1")]
    pub format: String,
    #[prost(bool, tag = "2")]
    pub checkpoint_transition: bool,
    #[prost(message, repeated, tag = "3")]
    pub ranges: Vec<MvccLogicalLogRangeProto>,
}

#[derive(prost::Message)]
pub struct PullUpdatesRespProtoBody {
    #[prost(string, tag = "1")]
    pub server_revision: String,
    // db size in pages (e.g. for 4kb db file db_size equals to 1)
    #[prost(uint64, tag = "2")]
    pub db_size: u64,
    #[prost(optional, message, tag = "3")]
    pub raw_encoding: Option<PageSetRawEncodingProto>,
    #[prost(optional, message, tag = "4")]
    pub zstd_encoding: Option<PageSetZstdEncodingProto>,
    #[prost(enumeration = "PullUpdatesStreamKind", tag = "5")]
    pub stream_kind: i32,
    #[prost(enumeration = "PullUpdatesApplyMode", tag = "6")]
    pub apply_mode: i32,
    #[prost(optional, message, tag = "7")]
    pub mvcc_log: Option<MvccLogicalLogMetadataProto>,
}


// The hrana v3 wire-protocol types are shared with the serverless drivers via
// the turso_remote_protocol crate. Re-export them so the sync engine and wire
// server keep referring to them as `server_proto::{...}`; the page-sync (prost)
// types above stay local to the sync engine.
pub use turso_remote_protocol::{
    Batch, BatchCond, BatchCondList, BatchResult, BatchStep, BatchStreamReq, BatchStreamResp,
    Col, Error, ExecuteStreamReq, ExecuteStreamResp, NamedArg, Row, Stmt, StmtResult,
    StreamRequest, StreamResponse, StreamResult, Value,
};

/// Legacy aliases for the pre-consolidation body type names used across the
/// sync engine and wire server.
pub type PipelineReqBody = turso_remote_protocol::PipelineRequest;
pub type PipelineRespBody = turso_remote_protocol::PipelineResponse;

pub(crate) mod bytes_as_base64_pad {
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    use bytes::Bytes;
    use serde::{de, ser};
    use serde::{de::Error as _, Serialize as _};

    pub fn serialize<S: ser::Serializer>(value: &Bytes, ser: S) -> Result<S::Ok, S::Error> {
        STANDARD.encode(value).serialize(ser)
    }

    pub fn deserialize<'de, D: de::Deserializer<'de>>(de: D) -> Result<Bytes, D::Error> {
        let text = <&'de str as de::Deserialize>::deserialize(de)?;
        let bytes = STANDARD.decode(text).map_err(|_| {
            D::Error::invalid_value(de::Unexpected::Str(text), &"binary data encoded as base64")
        })?;
        Ok(Bytes::from(bytes))
    }
}

#[cfg(test)]
mod pull_updates_tests {
    use super::{
        MvccLogicalLogMetadataProto, MvccLogicalLogRangeProto, PageSetRawEncodingProto,
        PageUpdatesEncodingReq, PullUpdatesApplyMode, PullUpdatesReqProtoBody,
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
}
