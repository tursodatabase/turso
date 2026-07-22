//! Wire-protocol conformance tests: exercise the SQL over HTTP protocol
//! (`serverless/PROTOCOL.md`) directly with raw HTTP requests against a
//! live Turso Cloud database.
//!
//! Requires `TURSO_DATABASE_URL` and `TURSO_AUTH_TOKEN`; every test skips
//! when they are not set.

use serde_json::{json, Value};
use turso_serverless_conformance::{config_or_skip, unique_name, TestConfig};

async fn post(config: &TestConfig, path: &str, body: &Value) -> (u16, String) {
    let response = reqwest::Client::new()
        .post(format!("{}{path}", config.url))
        .header("Authorization", format!("Bearer {}", config.auth_token))
        .header("Content-Type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .expect("http request failed");
    let status = response.status().as_u16();
    let body = response.text().await.expect("failed to read response body");
    (status, body)
}

/// POST to `/v3/pipeline` and return the response status and JSON body.
async fn pipeline(config: &TestConfig, body: Value) -> (u16, Value) {
    let (status, text) = post(config, "/v3/pipeline", &body).await;
    let json = serde_json::from_str(&text).unwrap_or(Value::Null);
    (status, json)
}

/// POST to `/v3/cursor` and return the status and the parsed body lines.
async fn cursor(config: &TestConfig, body: Value) -> (u16, Vec<Value>) {
    let (status, text) = post(config, "/v3/cursor", &body).await;
    let lines = text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("cursor body line is not valid JSON"))
        .collect();
    (status, lines)
}

/// Execute SQL statements on a fresh stream, closing it afterwards, and
/// assert every request succeeded. Returns the per-request results.
async fn exec(config: &TestConfig, statements: &[&str]) -> Vec<Value> {
    let mut requests: Vec<Value> = statements
        .iter()
        .map(|sql| json!({"type": "execute", "stmt": {"sql": sql}}))
        .collect();
    requests.push(json!({"type": "close"}));
    let (status, response) = pipeline(config, json!({"baton": null, "requests": requests})).await;
    assert_eq!(status, 200, "pipeline failed: {response}");
    let results = response["results"].as_array().expect("missing results");
    for result in results {
        assert_eq!(result["type"], "ok", "request failed: {result}");
    }
    results.clone()
}

#[tokio::test]
async fn pipeline_execute_returns_result_shape() {
    let config = config_or_skip!();
    let (status, response) = pipeline(
        &config,
        json!({
            "baton": null,
            "requests": [
                {
                    "type": "execute",
                    "stmt": {
                        "sql": "SELECT ? AS n, ? AS t",
                        "args": [
                            {"type": "integer", "value": "9223372036854775807"},
                            {"type": "text", "value": "hello"},
                        ],
                        "want_rows": true,
                    },
                },
                {"type": "close"},
            ],
        }),
    )
    .await;

    assert_eq!(status, 200);
    // A `null` baton in a response always means the stream is closed.
    assert_eq!(response["baton"], Value::Null);
    let results = response["results"].as_array().unwrap();
    assert_eq!(results.len(), 2);

    let result = &results[0];
    assert_eq!(result["type"], "ok");
    assert_eq!(result["response"]["type"], "execute");
    let exec_result = &result["response"]["result"];
    let cols = exec_result["cols"].as_array().unwrap();
    assert_eq!(cols.len(), 2);
    assert_eq!(cols[0]["name"], "n");
    assert_eq!(cols[1]["name"], "t");
    let rows = exec_result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    // Integers are transported as decimal strings (section 8.2).
    assert_eq!(
        rows[0][0],
        json!({"type": "integer", "value": "9223372036854775807"})
    );
    assert_eq!(rows[0][1], json!({"type": "text", "value": "hello"}));
    assert_eq!(exec_result["affected_row_count"], 0);
    assert_eq!(exec_result["last_insert_rowid"], Value::Null);

    assert_eq!(results[1]["type"], "ok");
    assert_eq!(results[1]["response"]["type"], "close");
}

#[tokio::test]
async fn sql_errors_are_reported_in_band_with_status_200() {
    let config = config_or_skip!();
    let (status, response) = pipeline(
        &config,
        json!({
            "baton": null,
            "requests": [
                {"type": "execute", "stmt": {"sql": "SELECT FROM WHERE"}},
                {"type": "close"},
            ],
        }),
    )
    .await;

    // SQL errors are in-band: the pipeline itself was processed.
    assert_eq!(status, 200);
    let result = &response["results"][0];
    assert_eq!(result["type"], "error");
    assert!(result["error"]["message"].is_string());
    assert!(result["error"]["code"].is_string());
}

#[tokio::test]
async fn failed_request_does_not_abort_pipeline() {
    let config = config_or_skip!();
    let (status, response) = pipeline(
        &config,
        json!({
            "baton": null,
            "requests": [
                {"type": "execute", "stmt": {"sql": "SELECT FROM WHERE"}},
                {"type": "execute", "stmt": {"sql": "SELECT 1"}},
                {"type": "close"},
            ],
        }),
    )
    .await;

    assert_eq!(status, 200);
    let results = response["results"].as_array().unwrap();
    assert_eq!(results[0]["type"], "error");
    // The server still executed the remaining requests (section 5.3).
    assert_eq!(results[1]["type"], "ok");
    assert_eq!(
        results[1]["response"]["result"]["rows"][0][0],
        json!({"type": "integer", "value": "1"})
    );
}

#[tokio::test]
async fn interactive_transaction_spans_requests_via_baton() {
    let config = config_or_skip!();
    let table = unique_name("p_tx");
    exec(&config, &[&format!("CREATE TABLE {table} (x INTEGER)")]).await;

    // First request: BEGIN on a fresh stream. The transaction keeps the
    // stream open, so the server must return a baton.
    let (status, response) = pipeline(
        &config,
        json!({
            "baton": null,
            "requests": [
                {"type": "execute", "stmt": {"sql": "BEGIN"}},
                {"type": "execute", "stmt": {"sql": format!("INSERT INTO {table} VALUES (1)")}},
            ],
        }),
    )
    .await;
    assert_eq!(status, 200);
    for result in response["results"].as_array().unwrap() {
        assert_eq!(result["type"], "ok", "request failed: {result}");
    }
    let baton = response["baton"]
        .as_str()
        .expect("server must return a baton while a transaction is open")
        .to_string();

    // Second request continues the stream and commits.
    let (status, response) = pipeline(
        &config,
        json!({
            "baton": baton,
            "requests": [
                {"type": "execute", "stmt": {"sql": "COMMIT"}},
                {"type": "close"},
            ],
        }),
    )
    .await;
    assert_eq!(status, 200);
    for result in response["results"].as_array().unwrap() {
        assert_eq!(result["type"], "ok", "request failed: {result}");
    }
    assert_eq!(response["baton"], Value::Null);

    // The commit is visible on a fresh stream.
    let results = exec(&config, &[&format!("SELECT count(*) FROM {table}")]).await;
    assert_eq!(
        results[0]["response"]["result"]["rows"][0][0],
        json!({"type": "integer", "value": "1"})
    );

    exec(&config, &[&format!("DROP TABLE {table}")]).await;
}

#[tokio::test]
async fn get_autocommit_reflects_transaction_state() {
    let config = config_or_skip!();
    let (status, response) = pipeline(
        &config,
        json!({
            "baton": null,
            "requests": [
                {"type": "get_autocommit"},
                {"type": "execute", "stmt": {"sql": "BEGIN"}},
                {"type": "get_autocommit"},
                {"type": "execute", "stmt": {"sql": "ROLLBACK"}},
                {"type": "get_autocommit"},
                {"type": "close"},
            ],
        }),
    )
    .await;

    assert_eq!(status, 200);
    let results = response["results"].as_array().unwrap();
    assert_eq!(results[0]["response"]["is_autocommit"], true);
    assert_eq!(results[2]["response"]["is_autocommit"], false);
    assert_eq!(results[4]["response"]["is_autocommit"], true);
}

#[tokio::test]
async fn batch_conditions_control_step_execution() {
    let config = config_or_skip!();
    let table = unique_name("p_cond");
    exec(
        &config,
        &[
            &format!("CREATE TABLE {table} (x INTEGER UNIQUE)"),
            &format!("INSERT INTO {table} VALUES (1)"),
        ],
    )
    .await;

    let (status, response) = pipeline(
        &config,
        json!({
            "baton": null,
            "requests": [
                {
                    "type": "batch",
                    "batch": {
                        "steps": [
                            // Step 0: succeeds.
                            {"stmt": {"sql": "SELECT 1"}},
                            // Step 1: fails on the unique constraint.
                            {"stmt": {"sql": format!("INSERT INTO {table} VALUES (1)")}},
                            // Step 2: runs because step 0 succeeded.
                            {
                                "condition": {"type": "ok", "step": 0},
                                "stmt": {"sql": "SELECT 2"},
                            },
                            // Step 3: skipped because step 1 did not succeed.
                            {
                                "condition": {"type": "ok", "step": 1},
                                "stmt": {"sql": "SELECT 3"},
                            },
                            // Step 4: runs; `error` is the negation of `ok`,
                            // so it is true for the failed step 1.
                            {
                                "condition": {"type": "error", "step": 1},
                                "stmt": {"sql": "SELECT 4"},
                            },
                            // Step 5: skipped; `error` on a skipped step is
                            // true, so `not` of it is false.
                            {
                                "condition": {"type": "not", "cond": {"type": "error", "step": 3}},
                                "stmt": {"sql": "SELECT 5"},
                            },
                        ],
                    },
                },
                {"type": "close"},
            ],
        }),
    )
    .await;

    assert_eq!(status, 200);
    let result = &response["results"][0];
    assert_eq!(result["type"], "ok", "batch failed: {result}");
    let batch_result = &result["response"]["result"];
    let step_results = batch_result["step_results"].as_array().unwrap();
    let step_errors = batch_result["step_errors"].as_array().unwrap();
    assert_eq!(step_results.len(), 6);
    assert_eq!(step_errors.len(), 6);

    // Executed successfully: result set, no error.
    for executed in [0usize, 2, 4] {
        assert!(
            step_results[executed].is_object(),
            "step {executed} should have executed: {batch_result}"
        );
        assert!(step_errors[executed].is_null());
    }
    // Failed: no result, an error.
    assert!(step_results[1].is_null());
    assert!(step_errors[1].is_object());
    // Skipped: both entries null.
    for skipped in [3usize, 5] {
        assert!(
            step_results[skipped].is_null(),
            "step {skipped} should be skipped"
        );
        assert!(
            step_errors[skipped].is_null(),
            "step {skipped} should be skipped"
        );
    }

    exec(&config, &[&format!("DROP TABLE {table}")]).await;
}

#[tokio::test]
async fn atomic_batch_rolls_back_on_failure() {
    let config = config_or_skip!();
    let table = unique_name("p_atomic");
    exec(
        &config,
        &[&format!("CREATE TABLE {table} (x INTEGER UNIQUE)")],
    )
    .await;

    // The BEGIN/COMMIT/ROLLBACK condition chain from section 6.2: the
    // second insert violates the unique constraint, so COMMIT is skipped
    // and ROLLBACK fires, undoing the first insert.
    let (status, response) = pipeline(
        &config,
        json!({
            "baton": null,
            "requests": [
                {
                    "type": "batch",
                    "batch": {
                        "steps": [
                            {"stmt": {"sql": "BEGIN"}},
                            {
                                "condition": {"type": "ok", "step": 0},
                                "stmt": {"sql": format!("INSERT INTO {table} VALUES (1)")},
                            },
                            {
                                "condition": {"type": "ok", "step": 1},
                                "stmt": {"sql": format!("INSERT INTO {table} VALUES (1)")},
                            },
                            {
                                "condition": {"type": "ok", "step": 2},
                                "stmt": {"sql": "COMMIT"},
                            },
                            {
                                "condition": {"type": "not", "cond": {"type": "ok", "step": 3}},
                                "stmt": {"sql": "ROLLBACK"},
                            },
                        ],
                    },
                },
                {"type": "close"},
            ],
        }),
    )
    .await;

    assert_eq!(status, 200);
    let batch_result = &response["results"][0]["response"]["result"];
    let step_results = batch_result["step_results"].as_array().unwrap();
    let step_errors = batch_result["step_errors"].as_array().unwrap();
    assert!(
        step_errors[2].is_object(),
        "step 2 should fail: {batch_result}"
    );
    assert!(step_results[3].is_null(), "COMMIT should be skipped");
    assert!(step_results[4].is_object(), "ROLLBACK should run");

    let results = exec(&config, &[&format!("SELECT count(*) FROM {table}")]).await;
    assert_eq!(
        results[0]["response"]["result"]["rows"][0][0],
        json!({"type": "integer", "value": "0"})
    );

    exec(&config, &[&format!("DROP TABLE {table}")]).await;
}

#[tokio::test]
async fn sequence_stops_at_first_failing_statement() {
    let config = config_or_skip!();
    let table = unique_name("p_seq");
    exec(&config, &[&format!("CREATE TABLE {table} (x INTEGER)")]).await;

    let (status, response) = pipeline(
        &config,
        json!({
            "baton": null,
            "requests": [
                {
                    "type": "sequence",
                    "sql": format!(
                        "INSERT INTO {table} VALUES (1); \
                         INSERT INTO missing_{table} VALUES (2); \
                         INSERT INTO {table} VALUES (3)"
                    ),
                },
                {"type": "close"},
            ],
        }),
    )
    .await;

    assert_eq!(status, 200);
    assert_eq!(response["results"][0]["type"], "error");

    let results = exec(&config, &[&format!("SELECT count(*) FROM {table}")]).await;
    assert_eq!(
        results[0]["response"]["result"]["rows"][0][0],
        json!({"type": "integer", "value": "1"})
    );

    exec(&config, &[&format!("DROP TABLE {table}")]).await;
}

#[tokio::test]
async fn describe_reports_params_and_readonly() {
    let config = config_or_skip!();
    let table = unique_name("p_desc");
    exec(
        &config,
        &[&format!(
            "CREATE TABLE {table} (id INTEGER PRIMARY KEY, name TEXT)"
        )],
    )
    .await;

    let (status, response) = pipeline(
        &config,
        json!({
            "baton": null,
            "requests": [
                {"type": "describe", "sql": format!("SELECT id, name FROM {table} WHERE id = :id")},
                {"type": "describe", "sql": format!("DELETE FROM {table}")},
                {"type": "close"},
            ],
        }),
    )
    .await;

    assert_eq!(status, 200);
    let result = &response["results"][0];
    assert_eq!(result["type"], "ok", "describe failed: {result}");
    let describe = &result["response"]["result"];
    let params = describe["params"].as_array().unwrap();
    assert_eq!(params.len(), 1);
    let cols = describe["cols"].as_array().unwrap();
    assert_eq!(cols.len(), 2);
    assert_eq!(cols[0]["name"], "id");
    assert_eq!(cols[1]["name"], "name");
    assert_eq!(describe["is_explain"], false);
    assert_eq!(describe["is_readonly"], true);

    let delete_describe = &response["results"][1]["response"]["result"];
    assert_eq!(delete_describe["is_readonly"], false);

    exec(&config, &[&format!("DROP TABLE {table}")]).await;
}

#[tokio::test]
async fn stored_sql_is_scoped_to_the_stream() {
    let config = config_or_skip!();
    let (status, response) = pipeline(
        &config,
        json!({
            "baton": null,
            "requests": [
                {"type": "store_sql", "sql_id": 1, "sql": "SELECT ? + 1"},
                {
                    "type": "execute",
                    "stmt": {"sql_id": 1, "args": [{"type": "integer", "value": "41"}]},
                },
                {"type": "close_sql", "sql_id": 1},
                // Closing an unknown sql_id is not an error.
                {"type": "close_sql", "sql_id": 42},
                {"type": "close"},
            ],
        }),
    )
    .await;

    assert_eq!(status, 200);
    let results = response["results"].as_array().unwrap();
    assert_eq!(results[0]["type"], "ok", "store_sql failed: {}", results[0]);
    assert_eq!(
        results[1]["type"], "ok",
        "execute by sql_id failed: {}",
        results[1]
    );
    assert_eq!(
        results[1]["response"]["result"]["rows"][0][0],
        json!({"type": "integer", "value": "42"})
    );
    assert_eq!(results[2]["type"], "ok");
    assert_eq!(
        results[3]["type"], "ok",
        "close_sql of unknown id failed: {}",
        results[3]
    );

    // Stored SQL is scoped to the stream: a fresh stream does not see the
    // id. The server may reject the reference in-band or fail the whole
    // request.
    let (status, response) = pipeline(
        &config,
        json!({
            "baton": null,
            "requests": [
                {"type": "execute", "stmt": {"sql_id": 1}},
                {"type": "close"},
            ],
        }),
    )
    .await;
    if status == 200 {
        assert_eq!(
            response["results"][0]["type"], "error",
            "an unknown sql_id must not execute: {response}"
        );
    }
}

#[tokio::test]
async fn close_must_be_the_last_request() {
    let config = config_or_skip!();
    let (status, _response) = pipeline(
        &config,
        json!({
            "baton": null,
            "requests": [
                {"type": "close"},
                {"type": "execute", "stmt": {"sql": "SELECT 1"}},
            ],
        }),
    )
    .await;

    // A close request that is not the last request of the pipeline fails
    // the whole HTTP request (section 5.3).
    assert_ne!(status, 200);
}

#[tokio::test]
async fn condition_referencing_later_step_is_a_protocol_error() {
    let config = config_or_skip!();
    let (status, _response) = pipeline(
        &config,
        json!({
            "baton": null,
            "requests": [
                {
                    "type": "batch",
                    "batch": {
                        "steps": [
                            {
                                "condition": {"type": "ok", "step": 1},
                                "stmt": {"sql": "SELECT 1"},
                            },
                            {"stmt": {"sql": "SELECT 2"}},
                        ],
                    },
                },
                {"type": "close"},
            ],
        }),
    )
    .await;

    // A condition may only refer to an earlier step (section 6.2.1).
    assert_ne!(status, 200);
}

#[tokio::test]
async fn mixing_positional_and_named_args_is_rejected() {
    let config = config_or_skip!();
    let (status, response) = pipeline(
        &config,
        json!({
            "baton": null,
            "requests": [
                {
                    "type": "execute",
                    "stmt": {
                        "sql": "SELECT :a, :b",
                        "args": [{"type": "integer", "value": "1"}],
                        "named_args": [{"name": "b", "value": {"type": "integer", "value": "2"}}],
                    },
                },
                {"type": "close"},
            ],
        }),
    )
    .await;

    // Setting both args and named_args to non-empty arrays is an error
    // (section 8.1); the server may reject it in-band or as a
    // protocol-level failure.
    if status == 200 {
        assert_eq!(response["results"][0]["type"], "error");
    }
}

#[tokio::test]
async fn named_args_match_with_or_without_prefix() {
    let config = config_or_skip!();
    for name in ["a", ":a"] {
        let (status, response) = pipeline(
            &config,
            json!({
                "baton": null,
                "requests": [
                    {
                        "type": "execute",
                        "stmt": {
                            "sql": "SELECT :a",
                            "named_args": [
                                {"name": name, "value": {"type": "integer", "value": "7"}},
                            ],
                        },
                    },
                    {"type": "close"},
                ],
            }),
        )
        .await;
        assert_eq!(status, 200);
        let result = &response["results"][0];
        assert_eq!(result["type"], "ok", "binding {name:?} failed: {result}");
        assert_eq!(
            result["response"]["result"]["rows"][0][0],
            json!({"type": "integer", "value": "7"})
        );
    }
}

#[tokio::test]
async fn want_rows_false_omits_rows() {
    let config = config_or_skip!();
    let (status, response) = pipeline(
        &config,
        json!({
            "baton": null,
            "requests": [
                {"type": "execute", "stmt": {"sql": "SELECT 1, 2, 3", "want_rows": false}},
                {"type": "close"},
            ],
        }),
    )
    .await;

    assert_eq!(status, 200);
    let result = &response["results"][0];
    assert_eq!(result["type"], "ok");
    assert_eq!(result["response"]["result"]["rows"], json!([]));
}

/// Send a `SELECT 1` pipeline with the given `Authorization` bearer token
/// (or no header at all) and return the response status.
async fn auth_status(config: &TestConfig, token: Option<&str>) -> u16 {
    let mut request = reqwest::Client::new()
        .post(format!("{}/v3/pipeline", config.url))
        .header("Content-Type", "application/json");
    if let Some(token) = token {
        request = request.header("Authorization", format!("Bearer {token}"));
    }
    request
        .body(
            json!({
                "baton": null,
                "requests": [{"type": "execute", "stmt": {"sql": "SELECT 1"}}],
            })
            .to_string(),
        )
        .send()
        .await
        .expect("http request failed")
        .status()
        .as_u16()
}

#[tokio::test]
async fn invalid_auth_token_is_unauthorized() {
    let config = config_or_skip!();

    // A structurally valid JWT whose signature cannot verify.
    let fake_jwt = "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.e30.aW52YWxpZC1zaWduYXR1cmU";
    let status = auth_status(&config, Some(fake_jwt)).await;
    if status == 200 {
        // The server does not enforce authentication (e.g. a local
        // development server); nothing to test.
        eprintln!("skipping: server accepted an invalid token");
        return;
    }
    assert_eq!(status, 401);

    // A missing token is also unauthorized (section 3).
    assert_eq!(auth_status(&config, None).await, 401);

    // A malformed token (not a JWT at all) is rejected as well. The spec
    // asks for 401, but Turso Cloud answers 400 for tokens it cannot
    // parse; accept both.
    let status = auth_status(&config, Some("invalid-token")).await;
    assert!(
        status == 401 || status == 400,
        "malformed token got status {status}"
    );
}

#[tokio::test]
async fn stale_baton_is_rejected() {
    let config = config_or_skip!();
    // Open a stream with a transaction so it stays alive.
    let (status, response) = pipeline(
        &config,
        json!({
            "baton": null,
            "requests": [{"type": "execute", "stmt": {"sql": "BEGIN"}}],
        }),
    )
    .await;
    assert_eq!(status, 200);
    let baton1 = response["baton"]
        .as_str()
        .expect("expected a baton")
        .to_string();

    // Consume baton1.
    let (status, response) = pipeline(
        &config,
        json!({
            "baton": baton1,
            "requests": [{"type": "execute", "stmt": {"sql": "SELECT 1"}}],
        }),
    )
    .await;
    assert_eq!(status, 200);
    let baton2 = response["baton"]
        .as_str()
        .expect("expected a baton")
        .to_string();

    if baton1 == baton2 {
        // The server reuses baton values; staleness is unobservable.
        let _ = pipeline(
            &config,
            json!({"baton": baton2, "requests": [{"type": "close"}]}),
        )
        .await;
        return;
    }

    // A baton that is not from the most recent response is invalid
    // (section 4.2) and fails with a non-200 status (section 4.5).
    let (status, _response) = pipeline(
        &config,
        json!({
            "baton": baton1,
            "requests": [{"type": "execute", "stmt": {"sql": "SELECT 1"}}],
        }),
    )
    .await;
    assert_ne!(status, 200);
}

#[tokio::test]
async fn cursor_streams_batch_results_line_by_line() {
    let config = config_or_skip!();
    let table = unique_name("p_cursor");
    exec(&config, &[&format!("CREATE TABLE {table} (x INTEGER)")]).await;
    exec(
        &config,
        &[&format!("INSERT INTO {table} VALUES (1), (2), (3)")],
    )
    .await;

    let (status, lines) = cursor(
        &config,
        json!({
            "baton": null,
            "batch": {
                "steps": [
                    {"stmt": {"sql": format!("SELECT x FROM {table} ORDER BY x")}},
                ],
            },
        }),
    )
    .await;

    assert_eq!(status, 200);
    // The first line is the cursor response; the baton is issued before
    // the batch executes (section 7.2), so it is always present.
    assert!(lines[0]["baton"].is_string(), "first line: {}", lines[0]);

    let entries = &lines[1..];
    assert_eq!(entries[0]["type"], "step_begin");
    assert_eq!(entries[0]["step"], 0);
    assert_eq!(entries[0]["cols"][0]["name"], "x");
    for (i, expected) in ["1", "2", "3"].iter().enumerate() {
        assert_eq!(entries[1 + i]["type"], "row");
        assert_eq!(
            entries[1 + i]["row"][0],
            json!({"type": "integer", "value": expected})
        );
    }
    assert_eq!(entries[4]["type"], "step_end");
    // The reserved replication_index entry is not emitted by every
    // server, but when present it is the final entry of the body.
    if let Some(pos) = entries
        .iter()
        .position(|e| e["type"] == "replication_index")
    {
        assert_eq!(pos, entries.len() - 1);
    }

    // Close the stream the cursor left open.
    let baton = lines[0]["baton"].as_str().unwrap();
    let _ = pipeline(
        &config,
        json!({"baton": baton, "requests": [{"type": "close"}]}),
    )
    .await;

    exec(&config, &[&format!("DROP TABLE {table}")]).await;
}

#[tokio::test]
async fn cursor_step_error_does_not_abort_the_batch() {
    let config = config_or_skip!();
    let table = unique_name("p_curerr");
    exec(
        &config,
        &[
            &format!("CREATE TABLE {table} (x INTEGER UNIQUE)"),
            &format!("INSERT INTO {table} VALUES (1)"),
        ],
    )
    .await;

    let (status, lines) = cursor(
        &config,
        json!({
            "baton": null,
            "batch": {
                "steps": [
                    // Fails on the unique constraint.
                    {"stmt": {"sql": format!("INSERT INTO {table} VALUES (1)"), "want_rows": false}},
                    {"stmt": {"sql": "SELECT 1"}},
                ],
            },
        }),
    )
    .await;

    assert_eq!(status, 200);
    let entries = &lines[1..];
    let step_error = entries
        .iter()
        .find(|e| e["type"] == "step_error")
        .expect("expected a step_error entry");
    assert_eq!(step_error["step"], 0);
    assert!(step_error["error"]["message"].is_string());

    // The second step still ran. (The failing step may emit its own
    // step_begin before the step_error, so look for step 1 explicitly.)
    entries
        .iter()
        .find(|e| e["type"] == "step_begin" && e["step"] == 1)
        .expect("expected a step_begin entry for the second step");

    if let Some(baton) = lines[0]["baton"].as_str() {
        let _ = pipeline(
            &config,
            json!({"baton": baton, "requests": [{"type": "close"}]}),
        )
        .await;
    }
    exec(&config, &[&format!("DROP TABLE {table}")]).await;
}

#[tokio::test]
async fn cursor_reports_last_insert_rowid_on_step_end() {
    let config = config_or_skip!();
    let table = unique_name("p_rowid");
    exec(
        &config,
        &[&format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY)")],
    )
    .await;

    let (status, lines) = cursor(
        &config,
        json!({
            "baton": null,
            "batch": {
                "steps": [
                    {"stmt": {"sql": format!("INSERT INTO {table} VALUES (123)"), "want_rows": false}},
                ],
            },
        }),
    )
    .await;

    assert_eq!(status, 200);
    let step_end = lines[1..]
        .iter()
        .find(|e| e["type"] == "step_end")
        .expect("expected a step_end entry");
    assert_eq!(step_end["affected_row_count"], 1);
    // The cursor endpoint emits last_insert_rowid as a JSON number, but
    // clients must accept both a number and a decimal string.
    let rowid = &step_end["last_insert_rowid"];
    let rowid = rowid
        .as_i64()
        .or_else(|| rowid.as_str().and_then(|s| s.parse().ok()))
        .expect("last_insert_rowid must be a number or decimal string");
    assert_eq!(rowid, 123);

    if let Some(baton) = lines[0]["baton"].as_str() {
        let _ = pipeline(
            &config,
            json!({"baton": baton, "requests": [{"type": "close"}]}),
        )
        .await;
    }
    exec(&config, &[&format!("DROP TABLE {table}")]).await;
}

#[tokio::test]
async fn cursor_is_autocommit_condition_skips_step_inside_transaction() {
    let config = config_or_skip!();
    let (status, lines) = cursor(
        &config,
        json!({
            "baton": null,
            "batch": {
                "steps": [
                    {"stmt": {"sql": "BEGIN", "want_rows": false}},
                    // Skipped: the connection is inside a transaction now.
                    {
                        "condition": {"type": "is_autocommit"},
                        "stmt": {"sql": "SELECT 1"},
                    },
                    {"stmt": {"sql": "ROLLBACK", "want_rows": false}},
                    // Runs: back in autocommit after ROLLBACK.
                    {
                        "condition": {"type": "is_autocommit"},
                        "stmt": {"sql": "SELECT 2"},
                    },
                ],
            },
        }),
    )
    .await;

    assert_eq!(status, 200);
    let entries = &lines[1..];
    let executed_steps: Vec<i64> = entries
        .iter()
        .filter(|e| e["type"] == "step_begin")
        .map(|e| e["step"].as_i64().unwrap())
        .collect();
    assert!(
        !executed_steps.contains(&1),
        "step 1 should be skipped inside the transaction: {executed_steps:?}"
    );
    assert!(
        executed_steps.contains(&3),
        "step 3 should run in autocommit: {executed_steps:?}"
    );

    if let Some(baton) = lines[0]["baton"].as_str() {
        let _ = pipeline(
            &config,
            json!({"baton": baton, "requests": [{"type": "close"}]}),
        )
        .await;
    }
}

#[tokio::test]
async fn blob_values_roundtrip_in_base64() {
    let config = config_or_skip!();
    // The server accepts both padded and unpadded base64 (section 8.2):
    // "3q2+7w" is 0xdeadbeef unpadded.
    for arg in ["3q2+7w", "3q2+7w=="] {
        let (status, response) = pipeline(
            &config,
            json!({
                "baton": null,
                "requests": [
                    {
                        "type": "execute",
                        "stmt": {"sql": "SELECT ?", "args": [{"type": "blob", "base64": arg}]},
                    },
                    {"type": "close"},
                ],
            }),
        )
        .await;
        assert_eq!(status, 200);
        let result = &response["results"][0];
        assert_eq!(result["type"], "ok", "blob arg {arg:?} failed: {result}");
        let value = &result["response"]["result"]["rows"][0][0];
        assert_eq!(value["type"], "blob");
        let b64 = value["base64"].as_str().unwrap();
        use base64::Engine;
        let decoded = base64::engine::general_purpose::STANDARD_NO_PAD
            .decode(b64.trim_end_matches('='))
            .expect("response blob is not valid base64");
        assert_eq!(decoded, vec![0xde, 0xad, 0xbe, 0xef]);
    }
}

#[tokio::test]
async fn v2_pipeline_endpoint_accepts_execute() {
    let config = config_or_skip!();
    let (status, text) = post(
        &config,
        "/v2/pipeline",
        &json!({
            "baton": null,
            "requests": [
                {"type": "execute", "stmt": {"sql": "SELECT 1"}},
                {"type": "close"},
            ],
        }),
    )
    .await;
    let response: Value = serde_json::from_str(&text).unwrap_or(Value::Null);

    // The server also accepts pipeline requests at /v2/pipeline (section 2).
    assert_eq!(status, 200, "v2 pipeline failed: {response}");
    assert_eq!(response["results"][0]["type"], "ok");
}
