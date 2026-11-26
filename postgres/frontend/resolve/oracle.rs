use super::catalog::UNKNOWNOID;
use serde_json::Value;
use std::sync::LazyLock;

/// One call of `testdata/oracle.json` and what PostgreSQL 18 answered.
pub(crate) struct OracleCase {
    pub kind: String,
    pub name: String,
    pub args: Vec<i64>,
    pub chosen: Option<i64>,
    pub result: Option<i64>,
    pub coercions: Vec<String>,
    pub node: Option<String>,
    pub sqlstate: Option<String>,
    pub message: Option<String>,
    pub detail: Option<String>,
    pub hint: Option<String>,
}

pub(crate) fn oracle_cases() -> &'static [OracleCase] {
    &CASES
}

static CASES: LazyLock<Vec<OracleCase>> = LazyLock::new(|| {
    let oracle: Value =
        serde_json::from_str(include_str!("testdata/oracle.json")).expect("oracle.json is JSON");
    let cases = oracle["cases"].as_array().expect("the oracle has cases");
    cases
        .iter()
        .map(|case| OracleCase {
            kind: string(&case["kind"]),
            name: string(&case["name"]),
            args: case["args"]
                .as_array()
                .expect("a case has arguments")
                .iter()
                .map(|arg| arg.as_i64().unwrap_or(UNKNOWNOID))
                .collect(),
            chosen: case["chosen"].as_i64(),
            result: case["result"].as_i64(),
            coercions: case["coercions"]
                .as_array()
                .map(|coercions| coercions.iter().map(string).collect())
                .unwrap_or_default(),
            node: case["node"].as_str().map(str::to_string),
            sqlstate: case["error"]["sqlstate"].as_str().map(str::to_string),
            message: case["error"]["message"].as_str().map(str::to_string),
            detail: case["error"]["detail"].as_str().map(str::to_string),
            hint: case["error"]["hint"].as_str().map(str::to_string),
        })
        .collect()
});

fn string(value: &Value) -> String {
    value.as_str().expect("a string").to_string()
}
