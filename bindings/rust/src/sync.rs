use std::{
    future::Future,
    io::ErrorKind,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
    time::Duration,
};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{header::AUTHORIZATION, Request};
use hyper_tls::HttpsConnector;
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client},
    rt::TokioExecutor,
};
use tokio::sync::mpsc;

use crate::{connection::Connection, Error, Result};

// Public re-exports of sync types for users of this crate.
pub use turso_sync_sdk_kit::rsapi::DatabaseSyncStats;
pub use turso_sync_sdk_kit::rsapi::PartialBootstrapStrategy;
pub use turso_sync_sdk_kit::rsapi::PartialSyncOpts;

// Constants used across the sync module
const DEFAULT_CLIENT_NAME: &str = "turso-sync-rust";

// Builder for a synced database.
pub struct Builder {
    // Absolute or relative path to local database file (":memory:" is supported).
    path: String,
    // Remote URL base. Supports https://, http:// and libsql:// (translated to https://).
    remote_url: Option<String>,
    // Optional authorization token (e.g., Bearer token).
    auth_token: Option<String>,
    // Optional custom client identifier used by the sync engine for telemetry/tracing.
    client_name: Option<String>,
    // Optional long-poll timeout when waiting for server changes.
    long_poll_timeout: Option<Duration>,
    // Whether to bootstrap a database if it's empty (download schema and initial data).
    bootstrap_if_empty: bool,
    // Partial sync configuration (EXPERIMENTAL).
    partial_sync_config_experimental: Option<PartialSyncOpts>,
}

impl Builder {
    // Create a new Builder for a synced database.
    pub fn new_remote(path: &str) -> Self {
        Self {
            path: path.to_string(),
            remote_url: None,
            auth_token: None,
            client_name: None,
            long_poll_timeout: None,
            bootstrap_if_empty: true,
            partial_sync_config_experimental: None,
        }
    }

    // Set remote_url for HTTP requests.
    // If remote_url omitted in configuration - tursodb will try to load it from the metadata file
    pub fn with_remote_url(mut self, remote_url: impl Into<String>) -> Self {
        self.remote_url = Some(remote_url.into());
        self
    }

    // Set optional authorization token for HTTP requests.
    pub fn with_auth_token(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }

    // Set custom client name (defaults to 'turso-sync-rust').
    pub fn with_client_name(mut self, name: impl Into<String>) -> Self {
        self.client_name = Some(name.into());
        self
    }

    // Set long poll timeout for waiting remote changes.
    pub fn with_long_poll_timeout(mut self, timeout: Duration) -> Self {
        self.long_poll_timeout = Some(timeout);
        self
    }

    // Configure bootstrap behavior for empty databases.
    pub fn bootstrap_if_empty(mut self, enable: bool) -> Self {
        self.bootstrap_if_empty = enable;
        self
    }

    // Set experimental partial sync configuration.
    pub fn with_partial_sync_opts_experimental(mut self, opts: PartialSyncOpts) -> Self {
        self.partial_sync_config_experimental = Some(opts);
        self
    }

    // Build the synced database object, initialize and open it.
    pub async fn build(self) -> Result<Database> {
        // Build core database config for the embedded engine.
        let db_config = turso_sdk_kit::rsapi::TursoDatabaseConfig {
            path: self.path.clone(),
            experimental_features: None,
            // IMPORTANT: async IO must be turned on to delegate IO to this layer.
            async_io: true,
            encryption: None,
            vfs: None,
            io: None,
            db_file: None,
        };

        // Build sync engine config.
        let sync_config = turso_sync_sdk_kit::rsapi::TursoDatabaseSyncConfig {
            path: self.path.clone(),
            remote_url: self.remote_url.clone(),
            client_name: self
                .client_name
                .clone()
                .unwrap_or_else(|| DEFAULT_CLIENT_NAME.to_string()),
            long_poll_timeout_ms: self
                .long_poll_timeout
                .map(|d| d.as_millis().min(u32::MAX as u128) as u32),
            bootstrap_if_empty: self.bootstrap_if_empty,
            reserved_bytes: None,
            partial_sync_opts: self.partial_sync_config_experimental.clone(),
        };

        // Create sync wrapper.
        let sync =
            turso_sync_sdk_kit::rsapi::TursoDatabaseSync::<Bytes>::new(db_config, sync_config)
                .map_err(Error::from)?;

        // IO worker will process SyncEngine IO queue on a dedicated tokio thread.
        let url = if let Some(remote_url) = &self.remote_url {
            Some(normalize_base_url(&remote_url).map_err(Error::Error)?)
        } else {
            None
        };
        let io_worker = IoWorker::spawn(sync.clone(), url, self.auth_token.clone());

        // Create (bootstrap + open) database in one go.
        let op = sync.create();
        drive_operation(op, io_worker.clone()).await?;

        Ok(Database {
            sync,
            io: io_worker,
        })
    }
}

// Synced Database handle.
#[derive(Clone)]
pub struct Database {
    sync: Arc<turso_sync_sdk_kit::rsapi::TursoDatabaseSync<Bytes>>,
    io: Arc<IoWorker>,
}

impl Database {
    // Push local changes to the remote.
    pub async fn push(&self) -> Result<()> {
        let op = self.sync.push_changes();
        drive_operation(op, self.io.clone()).await?;
        Ok(())
    }

    // Pull remote changes; returns true if any changes were applied.
    pub async fn pull(&self) -> Result<bool> {
        // First, wait for changes...
        let op = self.sync.wait_changes();
        let result = drive_operation_result(op, self.io.clone()).await?;
        let mut has_changes = false;

        if let Some(
            turso_sync_sdk_kit::turso_async_operation::TursoAsyncOperationResult::Changes {
                changes,
            },
        ) = result
        {
            if !changes.empty() {
                has_changes = true;
                // Then, apply them.
                let op_apply = self.sync.apply_changes(changes);
                drive_operation(op_apply, self.io.clone()).await?;
            }
        }

        Ok(has_changes)
    }

    // Force WAL checkpoint for the main database.
    pub async fn checkpoint(&self) -> Result<()> {
        let op = self.sync.checkpoint();
        drive_operation(op, self.io.clone()).await?;
        Ok(())
    }

    // Retrieve sync statistics for the database.
    pub async fn stats(&self) -> Result<DatabaseSyncStats> {
        let op = self.sync.stats();
        let result = drive_operation_result(op, self.io.clone()).await?;
        match result {
            Some(turso_sync_sdk_kit::turso_async_operation::TursoAsyncOperationResult::Stats {
                stats,
            }) => Ok(stats),
            _ => Err(Error::Misuse(
                "unexpected result type from stats operation".to_string(),
            )),
        }
    }

    // Create a SQL connection to the synced database.
    pub async fn connect(&self) -> Result<Connection> {
        let op = self.sync.connect();
        let result = drive_operation_result(op, self.io.clone()).await?;
        match result {
            Some(
                turso_sync_sdk_kit::turso_async_operation::TursoAsyncOperationResult::Connection {
                    connection,
                },
            ) => {
                // Provide extra_io callback to kick IO worker when driver needs to make progress.
                let io = self.io.clone();
                let extra_io = Arc::new(move |waker| {
                    io.register(waker);
                    io.kick();
                    Ok(())
                });
                Ok(Connection::create(connection, Some(extra_io)))
            }
            _ => Err(Error::Misuse(
                "unexpected result type from connect operation".to_string(),
            )),
        }
    }
}

// Drive an operation that has no result (returns None when done).
async fn drive_operation(
    op: Box<turso_sync_sdk_kit::turso_async_operation::TursoDatabaseAsyncOperation>,
    io: Arc<IoWorker>,
) -> Result<()> {
    let fut = AsyncOpFuture::new(op, io);
    fut.await.map(|_| ())
}

// Drive an operation and retrieve its result (if any).
async fn drive_operation_result(
    op: Box<turso_sync_sdk_kit::turso_async_operation::TursoDatabaseAsyncOperation>,
    io: Arc<IoWorker>,
) -> Result<Option<turso_sync_sdk_kit::turso_async_operation::TursoAsyncOperationResult>> {
    let fut = AsyncOpFuture::new(op, io);
    fut.await
}

// Custom Future that integrates with TursoDatabaseAsyncOperation and our IO worker.
struct AsyncOpFuture {
    op: Option<Box<turso_sync_sdk_kit::turso_async_operation::TursoDatabaseAsyncOperation>>,
    io: Arc<IoWorker>,
}

impl AsyncOpFuture {
    fn new(
        op: Box<turso_sync_sdk_kit::turso_async_operation::TursoDatabaseAsyncOperation>,
        io: Arc<IoWorker>,
    ) -> Self {
        Self { op: Some(op), io }
    }
}

impl Future for AsyncOpFuture {
    type Output =
        Result<Option<turso_sync_sdk_kit::turso_async_operation::TursoAsyncOperationResult>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        let Some(op) = &this.op else {
            return Poll::Ready(Err(Error::Misuse(
                "operation future has been already completed".to_string(),
            )));
        };

        this.io.register(cx.waker().clone());

        // Try to resume the operation.
        match op.resume() {
            Ok(turso_sdk_kit::rsapi::TursoStatusCode::Done) => {
                // Try to take the result (may be None).
                let result = op.take_result().map(Some).or_else(|err| match err {
                    turso_sdk_kit::rsapi::TursoError::Misuse(msg)
                        if msg.contains("operation has no result") =>
                    {
                        Ok(None)
                    }
                    other => Err(Error::from(other)),
                })?;
                // Drop the op and complete.
                this.op.take();
                Poll::Ready(Ok(result))
            }
            Ok(turso_sdk_kit::rsapi::TursoStatusCode::Io) => {
                // Kick IO worker to process queued IO.
                this.io.kick();
                // Wait until IO worker makes progress and wakes us.
                Poll::Pending
            }
            Ok(turso_sdk_kit::rsapi::TursoStatusCode::Row) => {
                // Not expected from top-level sync operations.
                Poll::Ready(Err(Error::Misuse(
                    "unexpected row status in sync operation".to_string(),
                )))
            }
            Err(e) => Poll::Ready(Err(Error::from(e))),
        }
    }
}

// Normalize remote base URL, mapping libsql:// to https:// and validating allowed schemes.
fn normalize_base_url(input: &str) -> std::result::Result<String, String> {
    let s = input.trim();
    let s = if let Some(rest) = s.strip_prefix("libsql://") {
        format!("https://{rest}")
    } else {
        s.to_string()
    };
    // Accept http or https only
    if !(s.starts_with("https://") || s.starts_with("http://")) {
        return Err(format!("unsupported remote URL scheme: {input}"));
    }
    // Ensure no trailing slash to make join predictable.
    let base = s.trim_end_matches('/').to_string();
    Ok(base)
}

// The IO worker owns a dedicated Tokio runtime on a separate thread, and processes
// the SyncEngine IO queue (HTTP and atomic file operations).
struct IoWorker {
    // Reference to the sync database to pull IO items from its queue.
    sync: Arc<turso_sync_sdk_kit::rsapi::TursoDatabaseSync<Bytes>>,
    // Normalized base URL (http/https).
    base_url: Option<String>,
    // Optional auth token.
    auth_token: Option<String>,
    // Channel to wake the worker to process IO.
    tx: mpsc::UnboundedSender<()>,
    // Wakers to notify pending futures when IO makes progress.
    wakers: Arc<Mutex<Vec<Waker>>>,
}

impl IoWorker {
    fn spawn(
        sync: Arc<turso_sync_sdk_kit::rsapi::TursoDatabaseSync<Bytes>>,
        base_url: Option<String>,
        auth_token: Option<String>,
    ) -> Arc<Self> {
        let (tx, rx) = mpsc::unbounded_channel::<()>();
        let wakers = Arc::new(Mutex::new(Vec::new()));

        let worker = Arc::new(Self {
            sync,
            base_url,
            auth_token,
            tx,
            wakers: wakers.clone(),
        });

        // Spin a separate Tokio runtime on its own thread to process IO queue.
        let worker_clone = worker.clone();
        std::thread::Builder::new()
            .name("turso-sync-io".to_string())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to build IO runtime");

                rt.block_on(async move {
                    IoWorker::run_loop(worker_clone, rx, wakers).await;
                });
            })
            .expect("failed to spawn IO worker thread");

        worker
    }

    // Register a waker to be awakened upon IO progress.
    fn register(&self, waker: Waker) {
        let mut wakers = self.wakers.lock().unwrap();
        wakers.push(waker);
    }

    // Kick the IO worker to process IO queue.
    fn kick(&self) {
        let _ = self.tx.send(());
    }

    // Called from the IO thread once progress has been made to notify all pending futures.
    fn notify_progress(wakers: &Arc<Mutex<Vec<Waker>>>) {
        let wakers = {
            let mut guard = wakers.lock().unwrap();
            std::mem::take(&mut *guard)
        };
        for w in wakers {
            w.wake();
        }
    }

    async fn run_loop(
        this: Arc<IoWorker>,
        mut rx: mpsc::UnboundedReceiver<()>,
        wakers: Arc<Mutex<Vec<Waker>>>,
    ) {
        // Create HTTPS-capable Hyper client.
        let mut http_connector = HttpConnector::new();
        http_connector.enforce_http(false);
        let https: HttpsConnector<HttpConnector> = HttpsConnector::new();
        let client: Client<HttpsConnector<HttpConnector>, Full<Bytes>> =
            Client::builder(TokioExecutor::new()).build::<_, Full<Bytes>>(https);

        while rx.recv().await.is_some() {
            // Process all pending items in the sync IO queue.
            let mut made_progress = false;
            loop {
                let item = this.sync.take_io_item();
                let Some(item) = item else {
                    this.sync.step_io_callbacks();
                    IoWorker::notify_progress(&wakers);
                    break;
                };

                made_progress = true;

                match item.get_request() {
                    turso_sync_sdk_kit::sync_engine_io::SyncEngineIoRequest::Http {
                        url,
                        method,
                        path,
                        body,
                        headers,
                    } => {
                        IoWorker::process_http(
                            &this,
                            &client,
                            url.as_deref(),
                            method,
                            path,
                            body.as_ref().map(|v| Bytes::from(v.clone())),
                            headers,
                            item.get_completion().clone(),
                        )
                        .await;
                    }
                    turso_sync_sdk_kit::sync_engine_io::SyncEngineIoRequest::FullRead { path } => {
                        IoWorker::process_full_read(
                            path,
                            item.get_completion().clone(),
                            &this.sync,
                        )
                        .await;
                    }
                    turso_sync_sdk_kit::sync_engine_io::SyncEngineIoRequest::FullWrite {
                        path,
                        content,
                    } => {
                        IoWorker::process_full_write(
                            path,
                            content,
                            item.get_completion().clone(),
                            &this.sync,
                        )
                        .await;
                    }
                }
            }

            // Run queued IO callbacks and wake all pending ops, yielding control
            // to allow them to make progress before we loop again.
            if made_progress {
                this.sync.step_io_callbacks();
                IoWorker::notify_progress(&wakers);
                // Let waiting tasks run on their executors.
                tokio::task::yield_now().await;
            }
        }
    }

    async fn process_http(
        this: &Arc<IoWorker>,
        client: &Client<HttpsConnector<HttpConnector>, Full<Bytes>>,
        url: Option<&str>,
        method: &str,
        path: &str,
        body: Option<Bytes>,
        headers: &[(String, String)],
        completion: turso_sync_sdk_kit::sync_engine_io::SyncEngineIoCompletion<Bytes>,
    ) {
        // Build full URL.
        let full_url = if path.starts_with("http://") || path.starts_with("https://") {
            path.to_string()
        } else {
            // Ensure the path begins with '/'
            let p = if path.starts_with('/') {
                path.to_string()
            } else {
                format!("/{path}")
            };
            let Some(url) = this.base_url.as_deref().or_else(|| url.map(|x| x)) else {
                completion.poison("remote_url is not available".to_string());
                return;
            };
            format!("{}{}", url, p)
        };

        let mut builder = Request::builder().method(method).uri(&full_url);

        // Set headers from request
        if let Some(headers_map) = builder.headers_mut() {
            for (k, v) in headers {
                if let Ok(name) = hyper::header::HeaderName::try_from(k.as_str()) {
                    if let Ok(value) = hyper::header::HeaderValue::try_from(v.as_str()) {
                        headers_map.insert(name, value);
                    }
                }
            }
            // Add Authorization header if not already set
            if let Some(token) = &this.auth_token {
                if !headers_map.contains_key(AUTHORIZATION) {
                    let value = format!("Bearer {token}");
                    if let Ok(hv) = hyper::header::HeaderValue::try_from(value.as_str()) {
                        headers_map.insert(AUTHORIZATION, hv);
                    }
                }
            }
        }

        // Body must be Full<Bytes> to match the client type.
        let req_body = Full::new(body.unwrap_or_default());

        let request = match builder.body(req_body) {
            Ok(r) => r,
            Err(err) => {
                completion.poison(format!("failed to build request: {err}"));
                this.sync.step_io_callbacks();
                return;
            }
        };

        let mut response = match client.request(request).await {
            Ok(r) => r,
            Err(err) => {
                completion.poison(format!("http request failed: {err}"));
                this.sync.step_io_callbacks();
                return;
            }
        };

        // Propagate status
        let status = response.status().as_u16();
        completion.status(status as u32);
        this.sync.step_io_callbacks();
        IoWorker::notify_progress(&this.wakers);

        // Stream response body in chunks
        while let Some(frame_res) = response.body_mut().frame().await {
            match frame_res {
                Ok(frame) => {
                    if let Some(chunk) = frame.data_ref() {
                        completion.push_buffer(chunk.clone());
                        this.sync.step_io_callbacks();
                        IoWorker::notify_progress(&this.wakers);
                    }
                }
                Err(err) => {
                    completion.poison(format!("error reading response body: {err}"));
                    this.sync.step_io_callbacks();
                    IoWorker::notify_progress(&this.wakers);
                    return;
                }
            }
        }

        // Done streaming
        completion.done();
        this.sync.step_io_callbacks();
        IoWorker::notify_progress(&this.wakers);
    }

    async fn process_full_read(
        path: &str,
        completion: turso_sync_sdk_kit::sync_engine_io::SyncEngineIoCompletion<Bytes>,
        sync: &Arc<turso_sync_sdk_kit::rsapi::TursoDatabaseSync<Bytes>>,
    ) {
        match tokio::fs::read(path).await {
            Ok(content) => {
                completion.push_buffer(Bytes::from(content));
                completion.done();
            }
            Err(err) if err.kind() == ErrorKind::NotFound => completion.done(),
            Err(err) => {
                completion.poison(format!("full read failed for {path}: {err}"));
            }
        }
        // Step callbacks after progress.
        sync.step_io_callbacks();
    }

    async fn process_full_write(
        path: &str,
        content: &Vec<u8>,
        completion: turso_sync_sdk_kit::sync_engine_io::SyncEngineIoCompletion<Bytes>,
        sync: &Arc<turso_sync_sdk_kit::rsapi::TursoDatabaseSync<Bytes>>,
    ) {
        // Write the whole content in one go (non-chunked)
        match tokio::fs::write(path, content).await {
            Ok(_) => {
                // For full write there is no data to stream back; just finish.
                completion.done();
            }
            Err(err) => {
                completion.poison(format!("full write failed for {path}: {err}"));
            }
        }
        // Step callbacks after progress.
        sync.step_io_callbacks();
    }
}

#[cfg(test)]
mod tests {
    use anyhow::{anyhow, Context, Result};
    use rand::{distr::Alphanumeric, Rng};
    use reqwest::Client;
    use serde_json::json;
    use std::{
        env,
        process::{Child, Command, Stdio},
        thread::sleep,
        time::Duration,
    };
    use tempfile::TempDir;
    use turso_sync_sdk_kit::rsapi::PartialBootstrapStrategy;

    use crate::sync::PartialSyncOpts;
    use crate::{Rows, Value};

    const ADMIN_URL: &str = "http://localhost:8081";
    const USER_URL: &str = "http://localhost:8080";

    fn random_str() -> String {
        rand::rng()
            .sample_iter(&Alphanumeric)
            .take(8)
            .map(char::from)
            .collect()
    }

    async fn handle_response(resp: reqwest::Response) -> Result<()> {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();

        if status == 400 && text.contains("already exists") {
            return Ok(());
        }

        if !status.is_success() {
            return Err(anyhow!("request failed: {status} {text}"));
        }

        Ok(())
    }

    pub struct TursoServer {
        user_url: String,
        db_url: String,
        host: String,
        server: Option<Child>,
        client: Client,
    }

    impl TursoServer {
        pub async fn new() -> Result<Self> {
            let client = Client::new();

            if env::var("LOCAL_SYNC_SERVER").is_err() {
                let name = random_str();
                let tokens: Vec<&str> = USER_URL.split("://").collect();

                handle_response(
                    client
                        .post(format!("{ADMIN_URL}/v1/tenants/{name}"))
                        .send()
                        .await?,
                )
                .await?;
                handle_response(
                    client
                        .post(format!("{ADMIN_URL}/v1/tenants/{name}/groups/{name}"))
                        .send()
                        .await?,
                )
                .await?;
                handle_response(
                    client
                        .post(format!(
                            "{ADMIN_URL}/v1/tenants/{name}/groups/{name}/databases/{name}"
                        ))
                        .send()
                        .await?,
                )
                .await?;

                Ok(Self {
                    user_url: USER_URL.to_string(),
                    db_url: format!("{}://{}--{}--{}.{}", tokens[0], name, name, name, tokens[1]),
                    host: format!("{name}--{name}--{name}.localhost"),
                    server: None,
                    client,
                })
            } else {
                let port: u16 = rand::rng().random_range(10_000..=65_535);
                let server_bin = env::var("LOCAL_SYNC_SERVER").unwrap();

                let child = Command::new(server_bin)
                    .args(["--sync-server", &format!("0.0.0.0:{port}")])
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()
                    .context("failed to spawn local sync server")?;

                let user_url = format!("http://localhost:{port}");

                // wait for server readiness
                loop {
                    if client.get(&user_url).send().await.is_ok() {
                        break;
                    }
                    sleep(Duration::from_millis(100));
                }

                Ok(Self {
                    user_url: user_url.clone(),
                    db_url: user_url,
                    host: String::new(),
                    server: Some(child),
                    client,
                })
            }
        }

        pub fn db_url(&self) -> &str {
            &self.db_url
        }

        pub async fn db_sql(&self, sql: &str) -> Result<Vec<Vec<Value>>> {
            let resp = self
                .client
                .post(format!("{}/v2/pipeline", self.user_url))
                .header("Host", &self.host)
                .json(&json!({
                    "requests": [{
                        "type": "execute",
                        "stmt": { "sql": sql }
                    }]
                }))
                .send()
                .await?
                .error_for_status()?;

            let value: serde_json::Value = resp.json().await?;

            let result = &value["results"][0];
            if result["type"] != "ok" {
                return Err(anyhow!("remote sql execution failed: {value}"));
            }

            let rows = result["response"]["result"]["rows"]
                .as_array()
                .ok_or_else(|| anyhow!("invalid response shape"))?;

            Ok(rows
                .iter()
                .map(|row| {
                    row.as_array()
                        .unwrap()
                        .iter()
                        .map(|cell| match cell["value"].clone() {
                            serde_json::Value::Null => Value::Null,
                            serde_json::Value::Number(number) => {
                                if number.is_i64() {
                                    Value::Integer(number.as_i64().unwrap())
                                } else {
                                    Value::Real(number.as_f64().unwrap())
                                }
                            }
                            serde_json::Value::String(s) => Value::Text(s),
                            _ => panic!("unexpected json output"),
                        })
                        .collect()
                })
                .collect())
        }
    }

    impl Drop for TursoServer {
        fn drop(&mut self) {
            if let Some(child) = &mut self.server {
                let _ = child.kill();
            }
        }
    }

    async fn all_rows(mut rows: Rows) -> Result<Vec<Vec<Value>>> {
        let mut result = Vec::new();
        while let Some(row) = rows.next().await? {
            result.push(row.values.into_iter().map(|x| x.into()).collect());
        }
        Ok(result)
    }

    #[tokio::test]
    pub async fn test_sync_bootstrap() {
        let _ = tracing_subscriber::fmt::try_init();
        let server = TursoServer::new().await.unwrap();
        server.db_sql("CREATE TABLE t(x)").await.unwrap();
        server
            .db_sql("INSERT INTO t VALUES ('hello'), ('turso'), ('sync')")
            .await
            .unwrap();
        server.db_sql("SELECT * FROM t").await.unwrap();
        let db = crate::sync::Builder::new_remote(":memory:")
            .with_remote_url(server.db_url())
            .build()
            .await
            .unwrap();
        let conn = db.connect().await.unwrap();
        let rows = conn.query("SELECT * FROM t", ()).await.unwrap();
        let all = all_rows(rows).await.unwrap();
        assert_eq!(
            all,
            vec![
                vec![Value::Text("hello".to_string())],
                vec![Value::Text("turso".to_string())],
                vec![Value::Text("sync".to_string())],
            ]
        );
    }

    #[tokio::test]
    pub async fn test_sync_bootstrap_persistence() {
        let _ = tracing_subscriber::fmt::try_init();
        let dir = TempDir::new().unwrap();
        let server = TursoServer::new().await.unwrap();
        server.db_sql("CREATE TABLE t(x)").await.unwrap();
        server
            .db_sql("INSERT INTO t VALUES ('hello'), ('turso'), ('sync')")
            .await
            .unwrap();
        server.db_sql("SELECT * FROM t").await.unwrap();
        let db = crate::sync::Builder::new_remote(dir.path().join("local.db").to_str().unwrap())
            .with_remote_url(server.db_url())
            .build()
            .await
            .unwrap();
        let conn = db.connect().await.unwrap();
        let rows = conn.query("SELECT * FROM t", ()).await.unwrap();
        let all = all_rows(rows).await.unwrap();
        assert_eq!(
            all,
            vec![
                vec![Value::Text("hello".to_string())],
                vec![Value::Text("turso".to_string())],
                vec![Value::Text("sync".to_string())],
            ]
        );
    }

    #[tokio::test]
    pub async fn test_sync_config_persistence() {
        let _ = tracing_subscriber::fmt::try_init();
        let dir = TempDir::new().unwrap();
        let server = TursoServer::new().await.unwrap();
        server.db_sql("CREATE TABLE t(x)").await.unwrap();
        server.db_sql("INSERT INTO t VALUES (42)").await.unwrap();
        {
            let db1 =
                crate::sync::Builder::new_remote(dir.path().join("local.db").to_str().unwrap())
                    .with_remote_url(server.db_url())
                    .build()
                    .await
                    .unwrap();
            let conn = db1.connect().await.unwrap();
            let rows = conn.query("SELECT * FROM t", ()).await.unwrap();
            let all = all_rows(rows).await.unwrap();
            assert_eq!(all, vec![vec![Value::Integer(42)],]);
        }
        server.db_sql("INSERT INTO t VALUES (41)").await.unwrap();
        {
            let db2 =
                crate::sync::Builder::new_remote(dir.path().join("local.db").to_str().unwrap())
                    .build()
                    .await
                    .unwrap();
            db2.pull().await.unwrap();
            let conn = db2.connect().await.unwrap();
            let rows = conn.query("SELECT * FROM t", ()).await.unwrap();
            let all = all_rows(rows).await.unwrap();
            assert_eq!(
                all,
                vec![vec![Value::Integer(42)], vec![Value::Integer(41)],]
            );
        }
    }

    #[tokio::test]
    pub async fn test_sync_pull() {
        let _ = tracing_subscriber::fmt::try_init();
        let server = TursoServer::new().await.unwrap();
        server.db_sql("CREATE TABLE t(x)").await.unwrap();
        server
            .db_sql("INSERT INTO t VALUES ('hello'), ('turso'), ('sync')")
            .await
            .unwrap();
        server.db_sql("SELECT * FROM t").await.unwrap();
        let db = crate::sync::Builder::new_remote(":memory:")
            .with_remote_url(server.db_url())
            .build()
            .await
            .unwrap();
        let conn = db.connect().await.unwrap();
        let rows = conn.query("SELECT * FROM t", ()).await.unwrap();
        let all = all_rows(rows).await.unwrap();
        assert_eq!(
            all,
            vec![
                vec![Value::Text("hello".to_string())],
                vec![Value::Text("turso".to_string())],
                vec![Value::Text("sync".to_string())],
            ]
        );

        server
            .db_sql("INSERT INTO t VALUES ('pull works')")
            .await
            .unwrap();

        let rows = conn.query("SELECT * FROM t", ()).await.unwrap();
        let all = all_rows(rows).await.unwrap();
        assert_eq!(
            all,
            vec![
                vec![Value::Text("hello".to_string())],
                vec![Value::Text("turso".to_string())],
                vec![Value::Text("sync".to_string())],
            ]
        );

        db.pull().await.unwrap();

        let rows = conn.query("SELECT * FROM t", ()).await.unwrap();
        let all = all_rows(rows).await.unwrap();
        assert_eq!(
            all,
            vec![
                vec![Value::Text("hello".to_string())],
                vec![Value::Text("turso".to_string())],
                vec![Value::Text("sync".to_string())],
                vec![Value::Text("pull works".to_string())],
            ]
        );
    }

    #[tokio::test]
    pub async fn test_sync_push() {
        let _ = tracing_subscriber::fmt::try_init();
        let server = TursoServer::new().await.unwrap();
        server.db_sql("CREATE TABLE t(x)").await.unwrap();
        server
            .db_sql("INSERT INTO t VALUES ('hello'), ('turso'), ('sync')")
            .await
            .unwrap();
        server.db_sql("SELECT * FROM t").await.unwrap();
        let db = crate::sync::Builder::new_remote(":memory:")
            .with_remote_url(server.db_url())
            .build()
            .await
            .unwrap();
        let conn = db.connect().await.unwrap();
        let rows = conn.query("SELECT * FROM t", ()).await.unwrap();
        let all = all_rows(rows).await.unwrap();
        assert_eq!(
            all,
            vec![
                vec![Value::Text("hello".to_string())],
                vec![Value::Text("turso".to_string())],
                vec![Value::Text("sync".to_string())],
            ]
        );

        conn.execute("INSERT INTO t VALUES ('push works')", ())
            .await
            .unwrap();

        let all = server.db_sql("SELECT * FROM t").await.unwrap();
        assert_eq!(
            all,
            vec![
                vec![Value::Text("hello".to_string())],
                vec![Value::Text("turso".to_string())],
                vec![Value::Text("sync".to_string())],
            ]
        );

        db.push().await.unwrap();

        let rows = conn.query("SELECT * FROM t", ()).await.unwrap();
        let all = all_rows(rows).await.unwrap();
        assert_eq!(
            all,
            vec![
                vec![Value::Text("hello".to_string())],
                vec![Value::Text("turso".to_string())],
                vec![Value::Text("sync".to_string())],
                vec![Value::Text("push works".to_string())],
            ]
        );
    }

    #[tokio::test]
    pub async fn test_sync_checkpoint() {
        let _ = tracing_subscriber::fmt::try_init();
        let server = TursoServer::new().await.unwrap();
        let db = crate::sync::Builder::new_remote(":memory:")
            .with_remote_url(server.db_url())
            .build()
            .await
            .unwrap();
        let conn = db.connect().await.unwrap();
        conn.execute("CREATE TABLE t(x)", ()).await.unwrap();
        for i in 0..1024 {
            conn.execute("INSERT INTO t VALUES (?)", (i,))
                .await
                .unwrap();
        }

        let stats1 = db.stats().await.unwrap();
        assert!(stats1.main_wal_size > 1024 * 1024);
        db.checkpoint().await.unwrap();
        let stats2 = db.stats().await.unwrap();
        assert!(stats2.main_wal_size < 8 * 1024);
    }

    #[tokio::test]
    pub async fn test_sync_partial() {
        let _ = tracing_subscriber::fmt::try_init();
        let server = TursoServer::new().await.unwrap();
        server.db_sql("CREATE TABLE t(x)").await.unwrap();
        server
            .db_sql("INSERT INTO t SELECT randomblob(1024) FROM generate_series(1, 2000)")
            .await
            .unwrap();
        {
            let full_db = crate::sync::Builder::new_remote(":memory:")
                .with_remote_url(server.db_url())
                .build()
                .await
                .unwrap();
            let conn = full_db.connect().await.unwrap();
            let _ = all_rows(
                conn.query("SELECT LENGTH(x) FROM t LIMIT 1", ())
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
            assert!(full_db.stats().await.unwrap().network_received_bytes > 2000 * 1024);
        }
        {
            let partial_db = crate::sync::Builder::new_remote(":memory:")
                .with_remote_url(server.db_url())
                .with_partial_sync_opts_experimental(PartialSyncOpts {
                    bootstrap_strategy: Some(PartialBootstrapStrategy::Prefix {
                        length: 128 * 1024,
                    }),
                    segment_size: 128 * 1024,
                    prefetch: false,
                })
                .build()
                .await
                .unwrap();
            let conn = partial_db.connect().await.unwrap();
            let _ = all_rows(
                conn.query("SELECT LENGTH(x) FROM t LIMIT 1", ())
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
            assert!(partial_db.stats().await.unwrap().network_received_bytes < 256 * (1024 + 10));
            let before = tokio::time::Instant::now();
            let all = all_rows(
                conn.query("SELECT SUM(LENGTH(x)) FROM t", ())
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
            println!(
                "duration: {:?}",
                tokio::time::Instant::now().duration_since(before)
            );
            assert_eq!(all, vec![vec![Value::Integer(2000 * 1024)]]);
            assert!(partial_db.stats().await.unwrap().network_received_bytes > 2000 * 1024);
        }
    }

    #[tokio::test]
    pub async fn test_sync_partial_segment_size() {
        let _ = tracing_subscriber::fmt::try_init();
        let server = TursoServer::new().await.unwrap();
        server.db_sql("CREATE TABLE t(x)").await.unwrap();
        server
            .db_sql("INSERT INTO t SELECT randomblob(1024) FROM generate_series(1, 256)")
            .await
            .unwrap();
        {
            let full_db = crate::sync::Builder::new_remote(":memory:")
                .with_remote_url(server.db_url())
                .build()
                .await
                .unwrap();
            let conn = full_db.connect().await.unwrap();
            let _ = all_rows(
                conn.query("SELECT LENGTH(x) FROM t LIMIT 1", ())
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
            assert!(full_db.stats().await.unwrap().network_received_bytes > 256 * 1024);
        }
        {
            let partial_db = crate::sync::Builder::new_remote(":memory:")
                .with_remote_url(server.db_url())
                .with_partial_sync_opts_experimental(PartialSyncOpts {
                    bootstrap_strategy: Some(PartialBootstrapStrategy::Prefix {
                        length: 128 * 1024,
                    }),
                    segment_size: 4 * 1024,
                    prefetch: false,
                })
                .build()
                .await
                .unwrap();
            let conn = partial_db.connect().await.unwrap();
            let _ = all_rows(
                conn.query("SELECT LENGTH(x) FROM t LIMIT 1", ())
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
            assert!(partial_db.stats().await.unwrap().network_received_bytes < 128 * 1024 * 3 / 2);
            let before = tokio::time::Instant::now();
            let all = all_rows(
                conn.query("SELECT SUM(LENGTH(x)) FROM t", ())
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
            println!(
                "duration segment size: {:?}",
                tokio::time::Instant::now().duration_since(before)
            );
            assert_eq!(all, vec![vec![Value::Integer(256 * 1024)]]);
            assert!(partial_db.stats().await.unwrap().network_received_bytes > 256 * 1024);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    pub async fn test_sync_partial_prefetch() {
        let _ = tracing_subscriber::fmt::try_init();
        let server = TursoServer::new().await.unwrap();
        server.db_sql("CREATE TABLE t(x)").await.unwrap();
        server
            .db_sql("INSERT INTO t SELECT randomblob(1024) FROM generate_series(1, 2000)")
            .await
            .unwrap();
        {
            let full_db = crate::sync::Builder::new_remote(":memory:")
                .with_remote_url(server.db_url())
                .build()
                .await
                .unwrap();
            let conn = full_db.connect().await.unwrap();
            let _ = all_rows(
                conn.query("SELECT LENGTH(x) FROM t LIMIT 1", ())
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
            assert!(full_db.stats().await.unwrap().network_received_bytes > 2000 * 1024);
        }
        {
            let partial_db = crate::sync::Builder::new_remote(":memory:")
                .with_remote_url(server.db_url())
                .with_partial_sync_opts_experimental(PartialSyncOpts {
                    bootstrap_strategy: Some(PartialBootstrapStrategy::Prefix {
                        length: 128 * 1024,
                    }),
                    segment_size: 128 * 1024,
                    prefetch: true,
                })
                .build()
                .await
                .unwrap();
            let conn = partial_db.connect().await.unwrap();
            let _ = all_rows(
                conn.query("SELECT LENGTH(x) FROM t LIMIT 1", ())
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
            assert!(partial_db.stats().await.unwrap().network_received_bytes < 1300 * (1024 + 10));
            let before = tokio::time::Instant::now();
            let all = all_rows(
                conn.query("SELECT SUM(LENGTH(x)) FROM t", ())
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
            println!(
                "duration prefetch: {:?}",
                tokio::time::Instant::now().duration_since(before)
            );
            assert_eq!(all, vec![vec![Value::Integer(2000 * 1024)]]);
            assert!(partial_db.stats().await.unwrap().network_received_bytes > 2000 * 1024);
        }
    }
}
