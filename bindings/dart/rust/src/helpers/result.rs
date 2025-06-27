use std::collections::HashMap;

use crate::helpers::return_value::ReturnValue;

pub struct QueryResult {
    pub rows: Vec<HashMap<String, ReturnValue>>,
    pub columns: Vec<String>,
    pub rows_affected: u64,
    pub last_insert_rowid: i64,
}

pub struct ExecuteResult {
    pub rows_affected: u64,
}
