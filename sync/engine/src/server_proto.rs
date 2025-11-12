use std::collections::VecDeque;

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
    #[prost(enumeration = "PageUpdatesEncodingReq", tag = "1")]
    pub encoding: i32,
    #[prost(string, tag = "2")]
    pub server_revision: String,
    #[prost(string, tag = "3")]
    pub client_revision: String,
    #[prost(uint32, tag = "4")]
    pub long_poll_timeout_ms: u32,
    #[prost(bytes, tag = "5")]
    pub server_pages_selector: Bytes,
    #[prost(string, tag = "7")]
    pub server_query_selector: String,
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
    #[prost(uint64, tag = "2")]
    pub db_size: u64,
    #[prost(optional, message, tag = "3")]
    pub raw_encoding: Option<PageSetRawEncodingProto>,
    #[prost(optional, message, tag = "4")]
    pub zstd_encoding: Option<PageSetZstdEncodingProto>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PipelineReqBody {
    pub baton: Option<String>,
    pub requests: VecDeque<StreamRequest>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PipelineRespBody {
    pub baton: Option<String>,
    pub base_url: Option<String>,
    pub results: Vec<StreamResult>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamRequest {
    #[serde(skip_deserializing)]
    #[default]
    None,
    /// See [`ExecuteStreamReq`]
    Execute(ExecuteStreamReq),
    /// See [`BatchStreamReq`]
    Batch(BatchStreamReq),
}

#[derive(Serialize, Deserialize, Default, Debug, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamResult {
    #[default]
    None,
    Ok {
        response: StreamResponse,
    },
    Error {
        error: Error,
    },
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamResponse {
    Execute(ExecuteStreamResp),
    Batch(BatchStreamResp),
}

#[derive(Serialize, Deserialize, Debug)]
/// A request to execute a batch of SQL statements that may each have a [`BatchCond`] that must be satisfied for the statement to be executed.
pub struct BatchStreamReq {
    pub batch: Batch,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
/// A response to a [`BatchStreamReq`].
pub struct BatchStreamResp {
    pub result: BatchResult,
}

#[derive(Clone, Deserialize, Serialize, Debug, Default, PartialEq)]
pub struct BatchResult {
    pub step_results: Vec<Option<StmtResult>>,
    pub step_errors: Vec<Option<Error>>,
    #[serde(default, with = "option_u64_as_str")]
    pub replication_index: Option<u64>,
}

#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct Batch {
    pub steps: VecDeque<BatchStep>,
    #[serde(default, with = "option_u64_as_str")]
    pub replication_index: Option<u64>,
}

#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct BatchStep {
    #[serde(default)]
    pub condition: Option<BatchCond>,
    pub stmt: Stmt,
}

#[derive(Clone, Deserialize, Serialize, Debug, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BatchCond {
    #[serde(skip_deserializing)]
    #[default]
    None,
    Ok {
        step: u32,
    },
    Error {
        step: u32,
    },
    Not {
        cond: Box<BatchCond>,
    },
    And(BatchCondList),
    Or(BatchCondList),
    IsAutocommit {},
}

#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct BatchCondList {
    pub conds: Vec<BatchCond>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
/// A response to a [`ExecuteStreamReq`].
pub struct ExecuteStreamResp {
    pub result: StmtResult,
}
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Default)]
pub struct StmtResult {
    pub cols: Vec<Col>,
    pub rows: Vec<Row>,
    pub affected_row_count: u64,
    #[serde(with = "option_i64_as_str")]
    pub last_insert_rowid: Option<i64>,
    #[serde(default, with = "option_u64_as_str")]
    pub replication_index: Option<u64>,
    #[serde(default)]
    pub rows_read: u64,
    #[serde(default)]
    pub rows_written: u64,
    #[serde(default)]
    pub query_duration_ms: f64,
}

#[derive(Clone, Deserialize, Serialize, Debug, PartialEq)]
pub struct Col {
    pub name: Option<String>,
    pub decltype: Option<String>,
}

#[derive(Clone, Deserialize, Serialize, Debug, PartialEq)]
#[serde(transparent)]
pub struct Row {
    pub values: Vec<Value>,
}

#[derive(Serialize, Deserialize, Debug)]
/// A request to execute a single SQL statement.
pub struct ExecuteStreamReq {
    pub stmt: Stmt,
}

#[derive(Clone, Deserialize, Serialize, Debug, PartialEq)]
pub struct Error {
    pub message: String,
    pub code: String,
}

#[derive(Clone, Deserialize, Serialize, Debug)]
/// A SQL statement to execute.
pub struct Stmt {
    #[serde(default)]
    /// The SQL statement to execute.
    pub sql: Option<String>,
    #[serde(default)]
    /// The ID of the SQL statement (if it is a stored statement; see [`crate::connections_manager::StreamResource`]).
    pub sql_id: Option<i32>,
    #[serde(default)]
    /// The positional arguments to the SQL statement.
    pub args: Vec<Value>,
    #[serde(default)]
    /// The named arguments to the SQL statement.
    pub named_args: Vec<NamedArg>,
    #[serde(default)]
    /// Whether the SQL statement should return rows.
    pub want_rows: Option<bool>,
    #[serde(default, with = "option_u64_as_str")]
    /// The replication index of the SQL statement (a LibSQL concept, currently not used).
    pub replication_index: Option<u64>,
}

#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct NamedArg {
    pub name: String,
    pub value: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Value {
    #[serde(skip_deserializing)]
    #[default]
    None,
    Null,
    Integer {
        #[serde(with = "i64_as_str")]
        value: i64,
    },
    Float {
        value: f64,
    },
    Text {
        value: String,
    },
    Blob {
        #[serde(with = "bytes_as_base64", rename = "base64")]
        value: Bytes,
    },
}

pub mod option_u64_as_str {
    use serde::de::Error;
    use serde::{de::Visitor, ser, Deserializer, Serialize as _};

    pub fn serialize<S: ser::Serializer>(value: &Option<u64>, ser: S) -> Result<S::Ok, S::Error> {
        value.map(|v| v.to_string()).serialize(ser)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u64>, D::Error> {
        struct V;

        impl<'de> Visitor<'de> for V {
            type Value = Option<u64>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(formatter, "a string representing an integer, or null")
            }

            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                deserializer.deserialize_any(V)
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(None)
            }

            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(None)
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(Some(v))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                v.parse().map_err(E::custom).map(Some)
            }
        }

        d.deserialize_option(V)
    }

    #[cfg(test)]
    mod test {
        use serde::Deserialize;

        #[test]
        fn deserialize_ok() {
            #[derive(Deserialize)]
            struct Test {
                #[serde(with = "super")]
                value: Option<u64>,
            }

            let json = r#"{"value": null }"#;
            let val: Test = serde_json::from_str(json).unwrap();
            assert!(val.value.is_none());

            let json = r#"{"value": "124" }"#;
            let val: Test = serde_json::from_str(json).unwrap();
            assert_eq!(val.value.unwrap(), 124);

            let json = r#"{"value": 124 }"#;
            let val: Test = serde_json::from_str(json).unwrap();
            assert_eq!(val.value.unwrap(), 124);
        }
    }
}

mod i64_as_str {
    use serde::{de, ser};
    use serde::{de::Error as _, Serialize as _};

    pub fn serialize<S: ser::Serializer>(value: &i64, ser: S) -> Result<S::Ok, S::Error> {
        value.to_string().serialize(ser)
    }

    pub fn deserialize<'de, D: de::Deserializer<'de>>(de: D) -> Result<i64, D::Error> {
        let str_value = <&'de str as de::Deserialize>::deserialize(de)?;
        str_value.parse().map_err(|_| {
            D::Error::invalid_value(
                de::Unexpected::Str(str_value),
                &"decimal integer as a string",
            )
        })
    }
}

pub(crate) mod bytes_as_base64 {
    use base64::{engine::general_purpose::STANDARD_NO_PAD, Engine as _};
    use bytes::Bytes;
    use serde::{de, ser};
    use serde::{de::Error as _, Serialize as _};

    pub fn serialize<S: ser::Serializer>(value: &Bytes, ser: S) -> Result<S::Ok, S::Error> {
        STANDARD_NO_PAD.encode(value).serialize(ser)
    }

    pub fn deserialize<'de, D: de::Deserializer<'de>>(de: D) -> Result<Bytes, D::Error> {
        let text = <&'de str as de::Deserialize>::deserialize(de)?;
        let text = text.trim_end_matches('=');
        let bytes = STANDARD_NO_PAD.decode(text).map_err(|_| {
            D::Error::invalid_value(de::Unexpected::Str(text), &"binary data encoded as base64")
        })?;
        Ok(Bytes::from(bytes))
    }
}

mod option_i64_as_str {
    use serde::de::{Error, Visitor};
    use serde::{ser, Deserializer, Serialize as _};

    pub fn serialize<S: ser::Serializer>(value: &Option<i64>, ser: S) -> Result<S::Ok, S::Error> {
        value.map(|v| v.to_string()).serialize(ser)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<i64>, D::Error> {
        struct V;

        impl<'de> Visitor<'de> for V {
            type Value = Option<i64>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(formatter, "a string representing a signed integer, or null")
            }

            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                deserializer.deserialize_any(V)
            }

            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(None)
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(None)
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(Some(v))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                v.parse().map_err(E::custom).map(Some)
            }
        }

        d.deserialize_option(V)
    }
}

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
