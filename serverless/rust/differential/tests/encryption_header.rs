// Property test for the remote encryption key header (PROTOCOL.md
// section 3.1): a driver configured with a key attaches
// `x-turso-encryption-key: <key>` to every HTTP request it sends — pipeline
// and cursor endpoints alike — and a driver with no key never sends the
// header.
//
// Runs against a local stub HTTP server that records request headers and
// speaks just enough of the protocol for the driver to complete a
// statement. It needs no Turso Cloud database and runs unconditionally.

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use hegel::{generators as gs, TestCase};
use serde_json::json;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use turso_serverless_differential::{spec_encryption_header, EncryptionHeaderSpec};

fn settings() -> hegel::Settings {
    hegel::Settings::new()
        .test_cases(spec_encryption_header().num_examples)
        .suppress_health_check([hegel::HealthCheck::TooSlow])
}

// ---------------------------------------------------------------------------
// Stub HTTP server: records every request's headers and answers with canned
// protocol responses.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct RecordedRequest {
    path: String,
    /// Header names lowercased.
    headers: HashMap<String, String>,
}

struct StubServer {
    addr: SocketAddr,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
}

impl StubServer {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = requests.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let recorded = recorded.clone();
                tokio::spawn(async move {
                    let _ = serve_connection(stream, recorded).await;
                });
            }
        });
        Self { addr, requests }
    }

    fn take_requests(&self) -> Vec<RecordedRequest> {
        std::mem::take(&mut self.requests.lock().unwrap())
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

async fn serve_connection(
    mut stream: TcpStream,
    recorded: Arc<Mutex<Vec<RecordedRequest>>>,
) -> std::io::Result<()> {
    let mut buf: Vec<u8> = Vec::new();
    loop {
        // Read the request head.
        let head_end = loop {
            if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
                break pos;
            }
            let mut chunk = [0u8; 4096];
            let n = stream.read(&mut chunk).await?;
            if n == 0 {
                return Ok(());
            }
            buf.extend_from_slice(&chunk[..n]);
        };
        let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
        let mut lines = head.split("\r\n");
        let request_line = lines.next().unwrap_or_default();
        let path = request_line
            .split_whitespace()
            .nth(1)
            .unwrap_or_default()
            .to_string();
        let mut headers = HashMap::new();
        for line in lines {
            if let Some((name, value)) = line.split_once(':') {
                headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
            }
        }

        // Read the request body.
        let content_length: usize = headers
            .get("content-length")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        let body_start = head_end + 4;
        while buf.len() < body_start + content_length {
            let mut chunk = [0u8; 4096];
            let n = stream.read(&mut chunk).await?;
            if n == 0 {
                return Ok(());
            }
            buf.extend_from_slice(&chunk[..n]);
        }
        let body: Vec<u8> = buf[body_start..body_start + content_length].to_vec();
        buf.drain(..body_start + content_length);

        recorded.lock().unwrap().push(RecordedRequest {
            path: path.clone(),
            headers,
        });

        let response_body = match path.as_str() {
            "/v3/pipeline" => pipeline_response(&body),
            "/v3/cursor" => cursor_response(),
            other => panic!("stub server: unexpected path {other}"),
        };
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            response_body.len(),
            response_body
        );
        stream.write_all(response.as_bytes()).await?;
    }
}

/// One ok result per request in the pipeline body (section 5.2).
fn pipeline_response(body: &[u8]) -> String {
    let request: serde_json::Value =
        serde_json::from_slice(body).expect("stub server: invalid pipeline request body");
    let results: Vec<serde_json::Value> = request["requests"]
        .as_array()
        .expect("stub server: pipeline request has no requests array")
        .iter()
        .map(|r| {
            let response = match r["type"].as_str().unwrap() {
                "execute" => json!({
                    "type": "execute",
                    "result": {
                        "cols": [],
                        "rows": [],
                        "affected_row_count": 0,
                        "last_insert_rowid": null,
                    },
                }),
                "get_autocommit" => json!({"type": "get_autocommit", "is_autocommit": true}),
                "sequence" => json!({"type": "sequence"}),
                "close" => json!({"type": "close"}),
                other => panic!("stub server: unexpected pipeline request type {other}"),
            };
            json!({"type": "ok", "response": response})
        })
        .collect();
    json!({"baton": null, "base_url": null, "results": results}).to_string()
}

/// A cursor response body (section 7.2): the cursor response line, then the
/// entries for the statement step and the driver's trailing autocommit
/// probe step.
fn cursor_response() -> String {
    [
        json!({"baton": null, "base_url": null}),
        json!({"type": "step_begin", "step": 0, "cols": [{"name": "1", "decltype": null}]}),
        json!({"type": "row", "row": [{"type": "integer", "value": "1"}]}),
        json!({"type": "step_end", "affected_row_count": 0, "last_insert_rowid": null}),
        json!({"type": "step_begin", "step": 1, "cols": []}),
        json!({"type": "step_end", "affected_row_count": 0, "last_insert_rowid": null}),
    ]
    .map(|line| line.to_string())
    .join("\n")
        + "\n"
}

// ---------------------------------------------------------------------------
// The property test
// ---------------------------------------------------------------------------

/// Draw a key from the spec's base64 alphabet and length bounds, with
/// optional trailing `=` padding.
fn gen_key(tc: &TestCase, spec: &EncryptionHeaderSpec) -> String {
    let mut key: String = tc.draw(
        gs::text()
            .alphabet(&spec.key_alphabet)
            .min_size(spec.key_min_len)
            .max_size(spec.key_max_len),
    );
    let padding: u8 = tc.draw(gs::integers::<u8>());
    for _ in 0..(padding % 3) {
        key.push('=');
    }
    key
}

/// Run one execute() (pipeline endpoint) and one query() (cursor endpoint)
/// against the stub server, then return the recorded requests.
async fn run_statements(server: &StubServer, key: Option<&str>) -> Vec<RecordedRequest> {
    let mut builder =
        turso_serverless::Builder::new_remote(format!("http://127.0.0.1:{}", server.addr.port()))
            .with_auth_token("test-token");
    if let Some(key) = key {
        builder = builder.with_remote_encryption_key(key);
    }
    let conn = builder.build().await.unwrap().connect().unwrap();

    conn.execute("SELECT 1", ()).await.unwrap();
    let mut rows = conn.query("SELECT 1", ()).await.unwrap();
    while rows.next().await.unwrap().is_some() {}

    let recorded = server.take_requests();
    assert!(
        recorded.iter().any(|r| r.path == "/v3/pipeline"),
        "no pipeline request recorded: {recorded:?}"
    );
    assert!(
        recorded.iter().any(|r| r.path == "/v3/cursor"),
        "no cursor request recorded: {recorded:?}"
    );
    recorded
}

#[hegel::test(settings())]
fn encryption_header(tc: TestCase) {
    let spec = spec_encryption_header();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let key = gen_key(&tc, &spec);
        let server = StubServer::start().await;

        // With a key: every request carries the header with exactly the key.
        for request in run_statements(&server, Some(&key)).await {
            assert_eq!(
                request.headers.get(&spec.header).map(String::as_str),
                Some(key.as_str()),
                "request to {} did not carry {}: {key:?}",
                request.path,
                spec.header,
            );
        }

        // Without a key: the header is absent from every request.
        for request in run_statements(&server, None).await {
            assert!(
                !request.headers.contains_key(&spec.header),
                "request to {} carried {} without a configured key",
                request.path,
                spec.header,
            );
        }
    });
}
