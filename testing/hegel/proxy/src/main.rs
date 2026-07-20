mod proto;
mod protocol;
mod state;
mod validator;

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Router;
use clap::Parser;
use protocol::{PipelineRequest, PipelineResponse};
use state::StreamState;
use tracing::{error, info, warn};
use validator::{mark_http_error, update_state, validate_request};

#[derive(Parser, Debug)]
#[command(name = "hrana-proxy", about = "Validating Hrana protocol proxy")]
struct Args {
    /// Upstream libsql-server URL
    #[arg(long, default_value = "http://localhost:8080")]
    upstream: String,

    /// Address to listen on
    #[arg(long, default_value = "0.0.0.0:9090")]
    listen: String,
}

/// Shared proxy state.
struct ProxyState {
    upstream: String,
    client: reqwest::Client,
    /// Per-connection stream state, keyed by a synthetic connection ID
    /// derived from the request (we use the baton or a counter for new streams).
    streams: Mutex<HashMap<String, StreamState>>,
    /// Batons that have been used in previous requests. The server increments
    /// baton_seq after each use, so re-sending an old baton is always invalid.
    seen_batons: Mutex<HashSet<String>>,
    conn_counter: Mutex<u64>,
}

/// Derive a connection key from the request baton.
/// New streams (baton=null) get a fresh counter-based key that we return
/// so the caller can associate it with the response baton.
fn connection_key(state: &ProxyState, baton: &Option<String>) -> String {
    match baton {
        Some(b) => b.clone(),
        None => {
            let mut counter = state.conn_counter.lock().unwrap();
            *counter += 1;
            format!("__new_{}", *counter)
        }
    }
}

/// Validate a parsed request, returning a violation response or None.
fn check_violations(
    proxy: &ProxyState,
    pipeline_req: &PipelineRequest,
) -> Option<(StatusCode, String)> {
    let conn_key = connection_key(proxy, &pipeline_req.baton);
    let (current_state, seen_batons_snapshot) = {
        let streams = proxy.streams.lock().unwrap();
        let seen = proxy.seen_batons.lock().unwrap();
        (
            streams.get(&conn_key).cloned().unwrap_or_default(),
            seen.clone(),
        )
    };

    let violations = validate_request(&current_state, pipeline_req, &seen_batons_snapshot);
    if violations.is_empty() {
        return None;
    }
    for v in &violations {
        warn!("Protocol violation: {}", v);
    }
    let messages: Vec<String> = violations.iter().map(|v| v.to_string()).collect();
    Some((
        StatusCode::INTERNAL_SERVER_ERROR,
        serde_json::json!({
            "proxy_error": true,
            "violations": messages
        })
        .to_string(),
    ))
}

/// Forward raw bytes to upstream, then update stream state from the response.
async fn forward_and_update(
    proxy: &ProxyState,
    headers: &HeaderMap,
    raw_body: Vec<u8>,
    pipeline_req: &PipelineRequest,
    upstream_path: &str,
) -> axum::response::Response {
    let conn_key = connection_key(proxy, &pipeline_req.baton);
    let upstream_url = format!("{}/{upstream_path}", proxy.upstream);
    let mut req_builder = proxy.client.post(&upstream_url);

    for (name, value) in headers.iter() {
        let name_str = name.as_str();
        if name_str == "content-type"
            || name_str == "authorization"
            || name_str.starts_with("x-turso-")
        {
            req_builder = req_builder.header(name.clone(), value.clone());
        }
    }

    let upstream_resp = match req_builder.body(raw_body).send().await {
        Ok(r) => r,
        Err(e) => {
            error!("Upstream request failed: {}", e);
            return (
                StatusCode::BAD_GATEWAY,
                format!("Upstream request failed: {e}"),
            )
                .into_response();
        }
    };

    let status = upstream_resp.status();
    let resp_headers = upstream_resp.headers().clone();
    let resp_bytes = match upstream_resp.bytes().await {
        Ok(b) => b,
        Err(e) => {
            error!("Failed to read upstream response: {}", e);
            return (
                StatusCode::BAD_GATEWAY,
                format!("Failed to read upstream response: {e}"),
            )
                .into_response();
        }
    };

    // Record that this baton has been used
    if let Some(ref baton) = pipeline_req.baton {
        proxy.seen_batons.lock().unwrap().insert(baton.clone());
    }

    if !status.is_success() {
        let mut streams = proxy.streams.lock().unwrap();
        if conn_key.starts_with("__new_") {
            streams.remove(&conn_key);
        }
        if let Some(ref old_baton) = pipeline_req.baton {
            if let Some(s) = streams.get_mut(old_baton) {
                mark_http_error(s);
            }
        }
        let mut response = axum::http::Response::builder().status(status.as_u16());
        if let Some(ct) = resp_headers.get("content-type") {
            response = response.header("content-type", ct);
        }
        return response
            .body(Body::from(resp_bytes))
            .unwrap()
            .into_response();
    }

    // Parse the response for state tracking — try JSON first, then protobuf.
    let pipeline_resp: Option<PipelineResponse> =
        serde_json::from_slice(&resp_bytes).ok().or_else(|| {
            use prost::Message;
            proto::PipelineRespBody::decode(&*resp_bytes)
                .ok()
                .map(|pb| pb.to_json_response())
        });

    if let Some(pipeline_resp) = pipeline_resp {
        let mut streams = proxy.streams.lock().unwrap();
        if conn_key.starts_with("__new_") {
            streams.remove(&conn_key);
        }

        let has_close = pipeline_req.requests.iter().any(|r| r.is_close());

        if has_close {
            if let Some(ref old_baton) = pipeline_req.baton {
                streams.remove(old_baton);
            }
            if let Some(ref new_baton) = pipeline_resp.baton {
                let mut s = StreamState::default();
                update_state(&mut s, pipeline_req, &pipeline_resp);
                streams.insert(new_baton.clone(), s);
            }
        } else if let Some(ref new_baton) = pipeline_resp.baton {
            let mut s = if let Some(ref old_baton) = pipeline_req.baton {
                streams.remove(old_baton).unwrap_or_default()
            } else {
                StreamState::default()
            };
            update_state(&mut s, pipeline_req, &pipeline_resp);
            streams.insert(new_baton.clone(), s);
        }
    }

    let mut response = axum::http::Response::builder().status(status.as_u16());
    if let Some(ct) = resp_headers.get("content-type") {
        response = response.header("content-type", ct);
    }
    response
        .body(Body::from(resp_bytes))
        .unwrap()
        .into_response()
}

/// Handle JSON pipeline requests (v2 and v3 share the same format).
async fn handle_pipeline_json(
    State(proxy): State<Arc<ProxyState>>,
    headers: HeaderMap,
    body: String,
    upstream_path: &str,
) -> axum::response::Response {
    let pipeline_req: PipelineRequest = match serde_json::from_str(&body) {
        Ok(r) => r,
        Err(e) => {
            error!("Failed to parse pipeline request: {e}");
            return (
                StatusCode::BAD_REQUEST,
                format!("Failed to parse pipeline request: {e}"),
            )
                .into_response();
        }
    };

    if let Some((status, msg)) = check_violations(&proxy, &pipeline_req) {
        return (status, msg).into_response();
    }

    forward_and_update(
        &proxy,
        &headers,
        body.into_bytes(),
        &pipeline_req,
        upstream_path,
    )
    .await
}

/// Handle v3 JSON pipeline requests.
async fn handle_pipeline_v3(
    state: State<Arc<ProxyState>>,
    headers: HeaderMap,
    body: String,
) -> impl IntoResponse {
    handle_pipeline_json(state, headers, body, "v3/pipeline").await
}

/// Handle v2 JSON pipeline requests.
async fn handle_pipeline_v2(
    state: State<Arc<ProxyState>>,
    headers: HeaderMap,
    body: String,
) -> impl IntoResponse {
    handle_pipeline_json(state, headers, body, "v2/pipeline").await
}

/// Handle v3-protobuf pipeline requests.
async fn handle_pipeline_protobuf(
    State(proxy): State<Arc<ProxyState>>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    use prost::Message;

    let pb_req = match proto::PipelineReqBody::decode(&*body) {
        Ok(r) => r,
        Err(e) => {
            error!("Failed to parse protobuf pipeline request: {e}");
            return (
                StatusCode::BAD_REQUEST,
                format!("Failed to parse protobuf pipeline request: {e}"),
            )
                .into_response();
        }
    };

    let pipeline_req = pb_req.to_json_request();

    if let Some((status, msg)) = check_violations(&proxy, &pipeline_req) {
        return (status, msg).into_response();
    }

    forward_and_update(
        &proxy,
        &headers,
        body.to_vec(),
        &pipeline_req,
        "v3-protobuf/pipeline",
    )
    .await
}

/// Respond OK to v3-protobuf version check so clients use protobuf.
async fn handle_version_check() -> impl IntoResponse {
    "ok"
}

/// Pass-through handler for cursor requests (no validation, just forward).
async fn handle_cursor(
    State(proxy): State<Arc<ProxyState>>,
    headers: HeaderMap,
    body: String,
) -> impl IntoResponse {
    let upstream_url = format!("{}/v3/cursor", proxy.upstream);
    let mut req_builder = proxy.client.post(&upstream_url);

    for (name, value) in headers.iter() {
        let name_str = name.as_str();
        if name_str == "content-type"
            || name_str == "authorization"
            || name_str.starts_with("x-turso-")
        {
            req_builder = req_builder.header(name.clone(), value.clone());
        }
    }

    let upstream_resp = match req_builder.body(body).send().await {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                format!("Upstream request failed: {e}"),
            )
                .into_response();
        }
    };

    let status = upstream_resp.status();
    let resp_headers = upstream_resp.headers().clone();

    let resp_bytes = match upstream_resp.bytes().await {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                format!("Failed to read upstream response: {e}"),
            )
                .into_response();
        }
    };

    let mut response = axum::http::Response::builder().status(status.as_u16());
    if let Some(ct) = resp_headers.get("content-type") {
        response = response.header("content-type", ct);
    }
    response
        .body(Body::from(resp_bytes))
        .unwrap()
        .into_response()
}

async fn handle_health() -> impl IntoResponse {
    "OK"
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "hrana_proxy=info".parse().unwrap()),
        )
        .init();

    let args = Args::parse();

    let proxy_state = Arc::new(ProxyState {
        upstream: args.upstream.clone(),
        client: reqwest::Client::new(),
        streams: Mutex::new(HashMap::new()),
        seen_batons: Mutex::new(HashSet::new()),
        conn_counter: Mutex::new(0),
    });

    let app = Router::new()
        .route("/v2/pipeline", post(handle_pipeline_v2))
        .route("/v3/pipeline", post(handle_pipeline_v3))
        .route("/v3-protobuf/pipeline", post(handle_pipeline_protobuf))
        .route("/v3-protobuf", get(handle_version_check))
        .route("/v3/cursor", post(handle_cursor))
        .route("/health", get(handle_health))
        .with_state(proxy_state);

    let addr: SocketAddr = args.listen.parse().expect("Invalid listen address");
    info!("Hrana validating proxy listening on {}", addr);
    info!("Forwarding to upstream: {}", args.upstream);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
