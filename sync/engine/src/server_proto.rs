pub use turso_remote_protocol::*;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Page sync types (prost-based, unrelated to hrana pipeline protocol)
// ---------------------------------------------------------------------------

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

#[derive(prost::Message)]
pub struct PageSetRawEncodingProto {}

#[derive(prost::Message)]
pub struct PageSetZstdEncodingProto {
    #[prost(int32, tag = "1")]
    pub level: i32,
    #[prost(uint32, repeated, tag = "2")]
    pub pages_dict: Vec<u32>,
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
}

/// Legacy alias for code that still references the old name.
pub type PipelineReqBody = PipelineRequest;

/// Legacy alias for code that still references the old name.
pub type PipelineRespBody = PipelineResponse;
