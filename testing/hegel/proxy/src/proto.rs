//! Protobuf message definitions for hrana v3-protobuf pipeline protocol.
//!
//! Field numbers are derived from the @libsql/hrana-client TypeScript encoder.
//! We only decode the fields needed for protocol validation (baton, request
//! types, SQL strings) — everything else is forwarded as raw bytes.

use prost::Message;

// ---------------------------------------------------------------------------
// Request messages
// ---------------------------------------------------------------------------

#[derive(Clone, Message)]
pub struct PipelineReqBody {
    #[prost(string, optional, tag = "1")]
    pub baton: Option<String>,
    #[prost(message, repeated, tag = "2")]
    pub requests: Vec<StreamRequestMsg>,
}

/// Wrapper for the oneof StreamRequest.
#[derive(Clone, Message)]
pub struct StreamRequestMsg {
    #[prost(oneof = "StreamRequestType", tags = "1, 2, 3, 4, 5, 6, 7, 8")]
    pub request: Option<StreamRequestType>,
}

#[derive(Clone, ::prost::Oneof)]
pub enum StreamRequestType {
    #[prost(message, tag = "1")]
    Close(CloseStreamReq),
    #[prost(message, tag = "2")]
    Execute(ExecuteStreamReq),
    #[prost(message, tag = "3")]
    Batch(BatchStreamReq),
    #[prost(message, tag = "4")]
    Sequence(SequenceStreamReq),
    #[prost(message, tag = "5")]
    Describe(DescribeStreamReq),
    #[prost(message, tag = "6")]
    StoreSql(StoreSqlStreamReq),
    #[prost(message, tag = "7")]
    CloseSql(CloseSqlStreamReq),
    #[prost(message, tag = "8")]
    GetAutocommit(GetAutocommitStreamReq),
}

#[derive(Clone, Message)]
pub struct CloseStreamReq {}

#[derive(Clone, Message)]
pub struct ExecuteStreamReq {
    #[prost(message, optional, tag = "1")]
    pub stmt: Option<Stmt>,
}

#[derive(Clone, Message)]
pub struct BatchStreamReq {
    #[prost(message, optional, tag = "1")]
    pub batch: Option<Batch>,
}

#[derive(Clone, Message)]
pub struct SequenceStreamReq {
    #[prost(string, optional, tag = "1")]
    pub sql: Option<String>,
    #[prost(int32, optional, tag = "2")]
    pub sql_id: Option<i32>,
}

#[derive(Clone, Message)]
pub struct DescribeStreamReq {
    #[prost(string, optional, tag = "1")]
    pub sql: Option<String>,
    #[prost(int32, optional, tag = "2")]
    pub sql_id: Option<i32>,
}

#[derive(Clone, Message)]
pub struct StoreSqlStreamReq {
    #[prost(int32, tag = "1")]
    pub sql_id: i32,
    #[prost(string, tag = "2")]
    pub sql: String,
}

#[derive(Clone, Message)]
pub struct CloseSqlStreamReq {
    #[prost(int32, tag = "1")]
    pub sql_id: i32,
}

#[derive(Clone, Message)]
pub struct GetAutocommitStreamReq {}

#[derive(Clone, Message)]
pub struct Stmt {
    #[prost(string, optional, tag = "1")]
    pub sql: Option<String>,
    #[prost(int32, optional, tag = "2")]
    pub sql_id: Option<i32>,
    // args (tag 3), named_args (tag 4), want_rows (tag 5) — not needed for validation
}

#[derive(Clone, Message)]
pub struct Batch {
    #[prost(message, repeated, tag = "1")]
    pub steps: Vec<BatchStep>,
}

#[derive(Clone, Message)]
pub struct BatchStep {
    // condition (tag 1) — not needed for validation
    #[prost(message, optional, tag = "2")]
    pub stmt: Option<Stmt>,
}

// ---------------------------------------------------------------------------
// Response messages (only need baton + result types)
// ---------------------------------------------------------------------------

#[derive(Clone, Message)]
pub struct PipelineRespBody {
    #[prost(string, optional, tag = "1")]
    pub baton: Option<String>,
    #[prost(string, optional, tag = "2")]
    pub base_url: Option<String>,
    #[prost(message, repeated, tag = "3")]
    pub results: Vec<StreamResultMsg>,
}

/// Wrapper for the oneof StreamResult.
#[derive(Clone, Message)]
pub struct StreamResultMsg {
    #[prost(oneof = "StreamResultType", tags = "1, 2")]
    pub result: Option<StreamResultType>,
}

#[derive(Clone, ::prost::Oneof)]
pub enum StreamResultType {
    #[prost(message, tag = "1")]
    Ok(StreamResponseMsg),
    #[prost(message, tag = "2")]
    Error(ProtoError),
}

/// We don't need to decode the full response, just know it was OK.
#[derive(Clone, Message)]
pub struct StreamResponseMsg {
    // Fields 1-8 correspond to response types but we don't need their contents.
    // Prost will silently skip unknown fields.
}

#[derive(Clone, Message)]
pub struct ProtoError {
    #[prost(string, tag = "1")]
    pub message: String,
    #[prost(string, tag = "2")]
    pub code: String,
}

// ---------------------------------------------------------------------------
// Conversion to shared JSON protocol types for validation
// ---------------------------------------------------------------------------

use crate::protocol;

impl PipelineReqBody {
    /// Convert to the JSON protocol PipelineRequest for validation.
    pub fn to_json_request(&self) -> protocol::PipelineRequest {
        protocol::PipelineRequest {
            baton: self.baton.clone(),
            requests: self.requests.iter().map(|r| r.to_json()).collect(),
        }
    }
}

impl StreamRequestMsg {
    fn to_json(&self) -> protocol::StreamRequest {
        match &self.request {
            Some(StreamRequestType::Close(_)) => protocol::StreamRequest::Close,
            Some(StreamRequestType::Execute(e)) => {
                let stmt = e.stmt.as_ref();
                protocol::StreamRequest::Execute(protocol::ExecuteStreamReq {
                    stmt: protocol::Stmt {
                        sql: stmt.and_then(|s| s.sql.clone()),
                        sql_id: stmt.and_then(|s| s.sql_id),
                        args: vec![],
                        named_args: vec![],
                        want_rows: Some(true),
                        replication_index: None,
                    },
                })
            }
            Some(StreamRequestType::Batch(b)) => {
                let steps = b
                    .batch
                    .as_ref()
                    .map(|batch| {
                        batch
                            .steps
                            .iter()
                            .map(|step| {
                                let stmt = step.stmt.as_ref();
                                protocol::BatchStep {
                                    stmt: protocol::Stmt {
                                        sql: stmt.and_then(|s| s.sql.clone()),
                                        sql_id: stmt.and_then(|s| s.sql_id),
                                        args: vec![],
                                        named_args: vec![],
                                        want_rows: Some(true),
                                        replication_index: None,
                                    },
                                    condition: None,
                                }
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                protocol::StreamRequest::Batch(protocol::BatchStreamReq {
                    batch: protocol::Batch {
                        steps,
                        replication_index: None,
                    },
                })
            }
            Some(StreamRequestType::Sequence(s)) => {
                protocol::StreamRequest::Sequence(protocol::SequenceStreamReq {
                    sql: s.sql.clone(),
                    sql_id: s.sql_id,
                })
            }
            Some(StreamRequestType::Describe(d)) => {
                protocol::StreamRequest::Describe(protocol::DescribeStreamReq {
                    sql: d.sql.clone(),
                    sql_id: d.sql_id,
                })
            }
            Some(StreamRequestType::StoreSql(s)) => {
                protocol::StreamRequest::StoreSql(protocol::StoreSqlStreamReq {
                    sql_id: s.sql_id,
                    sql: s.sql.clone(),
                })
            }
            Some(StreamRequestType::CloseSql(c)) => {
                protocol::StreamRequest::CloseSql(protocol::CloseSqlStreamReq { sql_id: c.sql_id })
            }
            Some(StreamRequestType::GetAutocommit(_)) => protocol::StreamRequest::GetAutocommit,
            None => protocol::StreamRequest::Close, // shouldn't happen
        }
    }
}

impl PipelineRespBody {
    /// Convert to the JSON protocol PipelineResponse for state tracking.
    pub fn to_json_response(&self) -> protocol::PipelineResponse {
        protocol::PipelineResponse {
            baton: self.baton.clone(),
            base_url: self.base_url.clone(),
            results: self
                .results
                .iter()
                .map(|r| match &r.result {
                    Some(StreamResultType::Ok(_)) => protocol::StreamResult::Ok {
                        response: protocol::StreamResponse::Close {},
                    },
                    Some(StreamResultType::Error(e)) => protocol::StreamResult::Error {
                        error: protocol::Error {
                            message: e.message.clone(),
                            code: e.code.clone(),
                        },
                    },
                    None => protocol::StreamResult::None,
                })
                .collect(),
        }
    }
}
