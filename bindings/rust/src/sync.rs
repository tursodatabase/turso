use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
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
pub use turso_sync_sdk_kit::rsapi::PartialSyncOpts;

// Constants used across the sync module
const DEFAULT_CLIENT_NAME: &str = "turso-sync-rust";

// Builder for a synced database.
pub struct Builder {
    // Absolute or relative path to local database file (":memory:" is supported).
    path: String,
    // Remote URL base. Supports https://, http:// and libsql:// (translated to https://).
    remote_url: String,
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
    pub fn new_remote(path: &str, remote_url: &str) -> Self {
        Self {
            path: path.to_string(),
            remote_url: remote_url.to_string(),
            auth_token: None,
            client_name: None,
            long_poll_timeout: None,
            bootstrap_if_empty: true,
            partial_sync_config_experimental: None,
        }
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
        let io_worker = IoWorker::spawn(
            sync.clone(),
            normalize_base_url(&self.remote_url).map_err(|e| Error::Error(e))?,
            self.auth_token.clone(),
        );

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
                let extra_io = Arc::new(move || {
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
    // To avoid re-registering the waker too frequently.
    registered: bool,
}

impl AsyncOpFuture {
    fn new(
        op: Box<turso_sync_sdk_kit::turso_async_operation::TursoDatabaseAsyncOperation>,
        io: Arc<IoWorker>,
    ) -> Self {
        Self {
            op: Some(op),
            io,
            registered: false,
        }
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

        // Ensure we're registered to be woken when IO progress happens.
        if !this.registered {
            this.io.register(cx.waker().clone());
            this.registered = true;
        }

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
        return Err(format!("unsupported remote URL scheme: {}", input));
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
    base_url: String,
    // Optional auth token.
    auth_token: Option<String>,
    // Channel to wake the worker to process IO.
    tx: mpsc::UnboundedSender<()>,
    // Wakers to notify pending futures when IO makes progress.
    wakers: Arc<Mutex<Vec<Waker>>>,
    // Whether a kick is scheduled and not yet processed (dedup kiks).
    scheduled: Arc<AtomicBool>,
}

impl IoWorker {
    fn spawn(
        sync: Arc<turso_sync_sdk_kit::rsapi::TursoDatabaseSync<Bytes>>,
        base_url: String,
        auth_token: Option<String>,
    ) -> Arc<Self> {
        let (tx, rx) = mpsc::unbounded_channel::<()>();
        let wakers = Arc::new(Mutex::new(Vec::new()));
        let scheduled = Arc::new(AtomicBool::new(false));

        let worker = Arc::new(Self {
            sync,
            base_url,
            auth_token,
            tx,
            wakers: wakers.clone(),
            scheduled: scheduled.clone(),
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
                    IoWorker::run_loop(worker_clone, rx, wakers, scheduled).await;
                });
            })
            .expect("failed to spawn IO worker thread");

        worker
    }

    // Register a waker to be awakened upon IO progress.
    fn register(&self, waker: Waker) {
        let mut wakers = self.wakers.lock().unwrap();
        // Replace existing equal waker if present to avoid growth.
        // If not, push new.
        // Note: Waker doesn't implement PartialEq; just push, we'll drain later.
        wakers.push(waker);
    }

    // Kick the IO worker to process IO queue.
    fn kick(&self) {
        if !self.scheduled.swap(true, Ordering::SeqCst) {
            let _ = self.tx.send(());
        }
    }

    // Called from the IO thread once progress has been made to notify all pending futures.
    fn notify_progress(wakers: &Arc<Mutex<Vec<Waker>>>) {
        let mut pending = Vec::new();
        {
            let mut guard = wakers.lock().unwrap();
            pending.append(&mut *guard);
        }
        for w in pending {
            w.wake();
        }
    }

    async fn run_loop(
        this: Arc<IoWorker>,
        mut rx: mpsc::UnboundedReceiver<()>,
        wakers: Arc<Mutex<Vec<Waker>>>,
        scheduled: Arc<AtomicBool>,
    ) {
        // Create HTTPS-capable Hyper client.
        let mut http_connector = HttpConnector::new();
        http_connector.enforce_http(false);
        let https: HttpsConnector<HttpConnector> = HttpsConnector::new();
        let client: Client<HttpsConnector<HttpConnector>, Full<Bytes>> =
            Client::builder(TokioExecutor::new()).build::<_, Full<Bytes>>(https);

        while let Some(_) = rx.recv().await {
            // Reset scheduled flag so next kick can be sent.
            scheduled.store(false, Ordering::SeqCst);

            // Process all pending items in the sync IO queue.
            let mut made_progress = false;
            loop {
                let item = this.sync.take_io_item();
                let Some(item) = item else {
                    break;
                };

                made_progress = true;

                match item.get_request() {
                    turso_sync_sdk_kit::sync_engine_io::SyncEngineIoRequest::Http {
                        method,
                        path,
                        body,
                        headers,
                    } => {
                        IoWorker::process_http(
                            &this,
                            &client,
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
                format!("/{}", path)
            };
            format!("{}{}", this.base_url, p)
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
                    let value = format!("Bearer {}", token);
                    if let Ok(hv) = hyper::header::HeaderValue::try_from(value.as_str()) {
                        headers_map.insert(AUTHORIZATION, hv);
                    }
                }
            }
        }

        // Body must be Full<Bytes> to match the client type.
        let req_body = Full::new(body.unwrap_or_else(Bytes::new));

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
            Err(err) => {
                completion.poison(format!("full read failed for {}: {}", path, err));
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
                completion.poison(format!("full write failed for {}: {}", path, err));
            }
        }
        // Step callbacks after progress.
        sync.step_io_callbacks();
    }
}
