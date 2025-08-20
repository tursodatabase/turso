use serde::{Deserialize, Serialize};

// SQL over HTTP Protocol Types
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SqlValue {
    #[serde(rename = "null")]
    Null,
    #[serde(rename = "integer")]
    Integer { value: String },
    #[serde(rename = "float")]
    Float { value: f64 },
    #[serde(rename = "text")]
    Text { value: String },
    #[serde(rename = "blob")]
    Blob { base64: String },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub decltype: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamedArg {
    pub name: String,
    pub value: SqlValue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Statement {
    pub sql: String,
    #[serde(default)]
    pub args: Vec<SqlValue>,
    #[serde(default)]
    pub named_args: Vec<NamedArg>,
    #[serde(default = "default_want_rows")]
    pub want_rows: bool,
}

fn default_want_rows() -> bool {
    true
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BatchStep {
    pub stmt: Statement,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub condition: Option<BatchCondition>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum BatchCondition {
    #[serde(rename = "ok")]
    Ok { step: usize },
    #[serde(rename = "error")]
    Error { step: usize },
    #[serde(rename = "not")]
    Not { cond: Box<BatchCondition> },
    #[serde(rename = "and")]
    And { conds: Vec<BatchCondition> },
    #[serde(rename = "or")]
    Or { conds: Vec<BatchCondition> },
    #[serde(rename = "is_autocommit")]
    IsAutocommit,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Batch {
    pub steps: Vec<BatchStep>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum StreamRequest {
    #[serde(rename = "execute")]
    Execute { stmt: Statement },
    #[serde(rename = "batch")]
    Batch { batch: Batch },
    #[serde(rename = "sequence")]
    Sequence { sql: String },
    #[serde(rename = "describe")]
    Describe { sql: String },
    #[serde(rename = "close")]
    Close,
    #[serde(rename = "get_autocommit")]
    GetAutocommit,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PipelineRequest {
    pub baton: Option<String>,
    pub requests: Vec<StreamRequest>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CursorRequest {
    pub baton: Option<String>,
    pub batch: Batch,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExecuteResult {
    pub cols: Vec<Column>,
    pub rows: Vec<Vec<SqlValue>>,
    pub affected_row_count: u64,
    pub last_insert_rowid: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BatchResult {
    pub step_results: Vec<Option<ExecuteResult>>,
    pub step_errors: Vec<Option<SqlError>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DescribeResult {
    pub params: Vec<DescribeParam>,
    pub cols: Vec<Column>,
    pub is_explain: bool,
    pub is_readonly: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DescribeParam {
    pub name: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SqlError {
    pub message: String,
    pub code: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum StreamResponse {
    #[serde(rename = "execute")]
    Execute { result: ExecuteResult },
    #[serde(rename = "batch")]
    Batch { result: BatchResult },
    #[serde(rename = "sequence")]
    Sequence,
    #[serde(rename = "describe")]
    Describe { result: DescribeResult },
    #[serde(rename = "close")]
    Close,
    #[serde(rename = "get_autocommit")]
    GetAutocommit { is_autocommit: bool },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum StreamResult {
    #[serde(rename = "ok")]
    Ok { response: StreamResponse },
    #[serde(rename = "error")]
    Error { error: SqlError },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PipelineResponse {
    pub baton: Option<String>,
    pub base_url: Option<String>,
    pub results: Vec<StreamResult>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CursorResponse {
    pub baton: Option<String>,
    pub base_url: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum CursorEntry {
    #[serde(rename = "step_begin")]
    StepBegin { step: usize, cols: Vec<Column> },
    #[serde(rename = "step_end")]
    StepEnd {
        affected_row_count: u64,
        last_insert_rowid: Option<String>,
    },
    #[serde(rename = "step_error")]
    StepError { step: usize, error: SqlError },
    #[serde(rename = "row")]
    Row { row: Vec<SqlValue> },
    #[serde(rename = "error")]
    Error { error: SqlError },
}
