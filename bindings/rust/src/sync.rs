use std::{
    future::Future,
    io::ErrorKind,
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    task::{Context, Poll, Waker},
    time::Duration,
};

use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::{header::AUTHORIZATION, Request};
use hyper_rustls::HttpsConnector;
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client},
    rt::TokioExecutor,
};
use tokio::sync::mpsc;
use turso_sdk_kit::IoBackend;

use crate::{connection::Connection, Error, Result};

// Public re-exports of sync types for users of this crate.
pub use turso_sync_sdk_kit::rsapi::DatabaseSyncStats;
pub use turso_sync_sdk_kit::rsapi::PartialBootstrapStrategy;
pub use turso_sync_sdk_kit::rsapi::PartialSyncOpts;

// Constants used across the sync module
const DEFAULT_CLIENT_NAME: &str = "turso-sync-rust";
const CHECKPOINT_BUSY_RETRY_DELAY: Duration = Duration::from_millis(10);
const CHECKPOINT_BUSY_MAX_ATTEMPTS: usize = 100;

/// Future returned by an auth token provider. Resolves to a bearer token string
/// (without the `Bearer ` prefix — that prefix is added when building the header).
pub type AuthTokenFut = Pin<Box<dyn Future<Output = Result<String>> + Send + 'static>>;

/// Async callback that produces an auth token on demand. Invoked before every
/// HTTP request issued by the sync engine, so it can return a freshly-rotated
/// token (e.g. fetched from a secrets manager or refreshed via OAuth).
pub type AuthTokenFn = Arc<dyn Fn() -> AuthTokenFut + Send + Sync + 'static>;

/// Encryption cipher for Turso Cloud remote encryption.
/// These match the server-side encryption settings.
#[derive(Debug, Clone, Copy)]
pub enum RemoteEncryptionCipher {
    Aes256Gcm,
    Aes128Gcm,
    ChaCha20Poly1305,
    Aegis128L,
    Aegis128X2,
    Aegis128X4,
    Aegis256,
    Aegis256X2,
    Aegis256X4,
}

impl RemoteEncryptionCipher {
    /// Returns the total reserved bytes as required by the server
    pub fn reserved_bytes(&self) -> usize {
        match self {
            Self::Aes256Gcm | Self::Aes128Gcm | Self::ChaCha20Poly1305 => 28,
            Self::Aegis128L | Self::Aegis128X2 | Self::Aegis128X4 => 32,
            Self::Aegis256 | Self::Aegis256X2 | Self::Aegis256X4 => 48,
        }
    }
}

impl std::str::FromStr for RemoteEncryptionCipher {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "aes256gcm" | "aes-256-gcm" => Ok(Self::Aes256Gcm),
            "aes128gcm" | "aes-128-gcm" => Ok(Self::Aes128Gcm),
            "chacha20poly1305" | "chacha20-poly1305" => Ok(Self::ChaCha20Poly1305),
            "aegis128l" | "aegis-128l" => Ok(Self::Aegis128L),
            "aegis128x2" | "aegis-128x2" => Ok(Self::Aegis128X2),
            "aegis128x4" | "aegis-128x4" => Ok(Self::Aegis128X4),
            "aegis256" | "aegis-256" => Ok(Self::Aegis256),
            "aegis256x2" | "aegis-256x2" => Ok(Self::Aegis256X2),
            "aegis256x4" | "aegis-256x4" => Ok(Self::Aegis256X4),
            _ => Err(format!(
                "unknown cipher: '{s}'. Supported: aes256gcm, aes128gcm, chacha20poly1305, \
                 aegis128l, aegis128x2, aegis128x4, aegis256, aegis256x2, aegis256x4"
            )),
        }
    }
}

// Builder for a synced database.
pub struct Builder {
    // Absolute or relative path to local database file (":memory:" is supported).
    path: String,
    // Remote URL base. Supports https://, http://, libsql:// and turso:// (the latter two are
    // translated to https://).
    remote_url: Option<String>,
    // Optional authorization token provider (static string or async callback).
    auth_token: Option<AuthTokenFn>,
    // Optional custom client identifier used by the sync engine for telemetry/tracing.
    client_name: Option<String>,
    // Optional long-poll timeout when waiting for server changes.
    long_poll_timeout: Option<Duration>,
    // Whether to bootstrap a database if it's empty (download schema and initial data).
    bootstrap_if_empty: bool,
    // Partial sync configuration (EXPERIMENTAL).
    partial_sync_config_experimental: Option<PartialSyncOpts>,
    // Encryption key (base64-encoded) for the Turso Cloud database
    remote_encryption_key: Option<String>,
    // Encryption cipher for the Turso Cloud database
    remote_encryption_cipher: Option<RemoteEncryptionCipher>,
    // Sync-protocol override: None (default) auto-detects the remote protocol
    // from the first pull-updates response; Some(true) forces MVCC logical-log
    // pulls; Some(false) forces page-stream pulls.
    logical_mvcc_pull: Option<bool>,
    // Experimental engine features to enable on the local synced database.
    // These mirror the local [`crate::Builder`] flags so synced databases
    // expose the same SQL surface as their local-only counterparts. Local
    // at-rest `encryption` is intentionally omitted because the sync engine
    // does not support local encryption (cloud encryption is configured
    // separately via `with_remote_encryption`).
    enable_attach: bool,
    enable_custom_types: bool,
    enable_index_method: bool,
    enable_materialized_views: bool,
    enable_vacuum: bool,
    enable_generated_columns: bool,
    enable_multiprocess_wal: bool,
    enable_without_rowid: bool,
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
            remote_encryption_key: None,
            remote_encryption_cipher: None,
            logical_mvcc_pull: None,
            enable_attach: false,
            enable_custom_types: false,
            enable_index_method: false,
            enable_materialized_views: false,
            enable_vacuum: false,
            enable_generated_columns: false,
            enable_multiprocess_wal: false,
            enable_without_rowid: false,
        }
    }

    /// Enable the experimental `attach` engine feature for the synced database.
    /// Mirrors the local [`crate::Builder::experimental_attach`] method.
    pub fn experimental_attach(mut self, enable: bool) -> Self {
        self.enable_attach = enable;
        self
    }

    /// Enable the experimental `custom_types` engine feature for the synced
    /// database. Mirrors the local [`crate::Builder::experimental_custom_types`].
    pub fn experimental_custom_types(mut self, enable: bool) -> Self {
        self.enable_custom_types = enable;
        self
    }

    /// Enable the experimental `index_method` engine feature for the synced
    /// database. When enabled, SQL statements like
    /// `CREATE INDEX idx ON t USING fts (...)` are accepted by the local
    /// engine. Mirrors the local [`crate::Builder::experimental_index_method`]
    /// method so callers can use the same SQL surface in synced mode.
    pub fn experimental_index_method(mut self, enable: bool) -> Self {
        self.enable_index_method = enable;
        self
    }

    /// Enable the experimental materialized `views` engine feature for the
    /// synced database. Mirrors the local
    /// [`crate::Builder::experimental_materialized_views`].
    pub fn experimental_materialized_views(mut self, enable: bool) -> Self {
        self.enable_materialized_views = enable;
        self
    }

    /// Enable the experimental `vacuum` engine feature for the synced database.
    /// Mirrors the local [`crate::Builder::experimental_vacuum`].
    pub fn experimental_vacuum(mut self, enable: bool) -> Self {
        self.enable_vacuum = enable;
        self
    }

    /// Enable the experimental `generated_columns` engine feature for the
    /// synced database. Mirrors the local
    /// [`crate::Builder::experimental_generated_columns`].
    pub fn experimental_generated_columns(mut self, enable: bool) -> Self {
        self.enable_generated_columns = enable;
        self
    }

    /// Enable the experimental `multiprocess_wal` engine feature for the synced
    /// database. Mirrors the local
    /// [`crate::Builder::experimental_multiprocess_wal`].
    pub fn experimental_multiprocess_wal(mut self, enable: bool) -> Self {
        self.enable_multiprocess_wal = enable;
        self
    }

    /// Enable the experimental `without_rowid` engine feature for the synced
    /// database. Mirrors the local
    /// [`crate::Builder::experimental_without_rowid`].
    pub fn experimental_without_rowid(mut self, enable: bool) -> Self {
        self.enable_without_rowid = enable;
        self
    }

    // Set remote_url for HTTP requests.
    // If remote_url omitted in configuration - tursodb will try to load it from the metadata file
    pub fn with_remote_url(mut self, remote_url: impl Into<String>) -> Self {
        self.remote_url = Some(remote_url.into());
        self
    }

    // Set optional authorization token for HTTP requests.
    pub fn with_auth_token(mut self, token: impl Into<String>) -> Self {
        let token = token.into();
        self.auth_token = Some(Arc::new(move || {
            let token = token.clone();
            Box::pin(async move { Ok(token) })
        }));
        self
    }

    /// Set an async callback that produces an auth token on demand.
    ///
    /// The callback is invoked before every HTTP request, so it can return a
    /// freshly rotated token (e.g. fetched from a secrets manager or refreshed
    /// via OAuth). If the callback returns an error, the in-flight sync
    /// operation fails with that error.
    ///
    /// Calling this overrides any previously configured static token.
    pub fn with_auth_token_fn<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<String>> + Send + 'static,
    {
        self.auth_token = Some(Arc::new(move || Box::pin(f())));
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

    /// Set encryption key (base64-encoded) and cipher for the Turso Cloud database.
    /// The cipher is used to calculate the correct reserved_bytes for the database.
    pub fn with_remote_encryption(
        mut self,
        base64_key: impl Into<String>,
        cipher: RemoteEncryptionCipher,
    ) -> Self {
        self.remote_encryption_key = Some(base64_key.into());
        self.remote_encryption_cipher = Some(cipher);
        self
    }

    /// Set encryption key (base64-encoded) for the Turso Cloud database.
    /// The key will be sent as x-turso-encryption-key header with sync HTTP requests.
    /// Note: For deferred sync (no initial bootstrap), use with_remote_encryption() instead
    /// to also specify the cipher for correct reserved_bytes calculation.
    pub fn with_remote_encryption_key(mut self, base64_key: impl Into<String>) -> Self {
        self.remote_encryption_key = Some(base64_key.into());
        self
    }

    /// Override the sync protocol used for incremental pulls.
    ///
    /// By default the protocol is auto-detected from the first pull-updates
    /// response and persisted in the sync metadata, so calling this is only
    /// needed for tests or as an escape hatch: `true` forces MVCC logical-log
    /// pulls, `false` forces page-stream pulls.
    pub fn with_logical_mvcc_pull(mut self, enable: bool) -> Self {
        self.logical_mvcc_pull = Some(enable);
        self
    }

    /// Compose the `experimental_features` comma-separated string consumed by
    /// [`turso_sdk_kit::rsapi::TursoDatabaseConfig`] (and ultimately
    /// `turso_core::DatabaseOpts::with_experimental_feature`) from the boolean
    /// flags on this Builder. Returns `None` when no feature is enabled. The
    /// feature tokens must match the names parsed by the core.
    fn experimental_features_string(&self) -> Option<String> {
        let mut features: Vec<&str> = Vec::new();
        if self.enable_attach {
            features.push("attach");
        }
        if self.enable_custom_types {
            features.push("custom_types");
        }
        if self.enable_index_method {
            features.push("index_method");
        }
        if self.enable_materialized_views {
            features.push("views");
        }
        if self.enable_vacuum {
            features.push("vacuum");
        }
        if self.enable_generated_columns {
            features.push("generated_columns");
        }
        if self.enable_multiprocess_wal {
            features.push("multiprocess_wal");
        }
        if self.enable_without_rowid {
            features.push("without_rowid");
        }
        if features.is_empty() {
            None
        } else {
            Some(features.join(","))
        }
    }

    // Build the synced database object, initialize and open it.
    pub async fn build(self) -> Result<Database> {
        // Compose the experimental_features string from the boolean flags
        // exposed on this Builder.
        let experimental_features = self.experimental_features_string();

        // Build core database config for the embedded engine.
        let db_config = turso_sdk_kit::rsapi::TursoDatabaseConfig {
            path: self.path.clone(),
            experimental_features,
            // IMPORTANT: async IO must be turned on to delegate IO to this layer.
            async_io: true,
            encryption: None,
            vfs: IoBackend::Default,
            io: None,
            db_file: None,
            page_codec: None,
            open_flags: Default::default(),
        };

        let url = if let Some(remote_url) = &self.remote_url {
            Some(normalize_base_url(remote_url).map_err(Error::Error)?)
        } else {
            None
        };

        // Calculate reserved_bytes from cipher if provided.
        let reserved_bytes = self
            .remote_encryption_cipher
            .map(|cipher| cipher.reserved_bytes());

        // Build sync engine config.
        let sync_config = turso_sync_sdk_kit::rsapi::TursoDatabaseSyncConfig {
            path: self.path.clone(),
            remote_url: url.clone(),
            client_name: self
                .client_name
                .clone()
                .unwrap_or_else(|| DEFAULT_CLIENT_NAME.to_string()),
            long_poll_timeout_ms: self
                .long_poll_timeout
                .map(|d| d.as_millis().min(u32::MAX as u128) as u32),
            bootstrap_if_empty: self.bootstrap_if_empty,
            reserved_bytes,
            partial_sync_opts: self.partial_sync_config_experimental.clone(),
            remote_encryption_key: self.remote_encryption_key.clone(),
            push_operations_threshold: None,
            pull_bytes_threshold: None,
            logical_mvcc_pull: self.logical_mvcc_pull,
        };

        // Create sync wrapper.
        let sync =
            turso_sync_sdk_kit::rsapi::TursoDatabaseSync::<Bytes>::new(db_config, sync_config)
                .map_err(Error::from)?;

        // IO worker will process SyncEngine IO queue on a dedicated tokio thread.
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
        for attempt in 0..CHECKPOINT_BUSY_MAX_ATTEMPTS {
            let op = self.sync.checkpoint();
            let result = drive_operation(op, self.io.clone()).await;
            match result {
                Ok(()) => return Ok(()),
                Err(error)
                    if is_sync_busy_error(&error) && attempt + 1 < CHECKPOINT_BUSY_MAX_ATTEMPTS =>
                {
                    tokio::time::sleep(CHECKPOINT_BUSY_RETRY_DELAY).await;
                }
                Err(error) => return Err(error),
            }
        }
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

fn is_sync_busy_error(error: &Error) -> bool {
    match error {
        Error::Busy(_) => true,
        Error::Error(message) => message.contains("Database is busy"),
        _ => false,
    }
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

// Normalize remote base URL, mapping libsql:// and turso:// to https:// and validating allowed
// schemes.
fn normalize_base_url(input: &str) -> std::result::Result<String, String> {
    let s = input.trim();
    let s = if let Some(rest) = s
        .strip_prefix("libsql://")
        .or_else(|| s.strip_prefix("turso://"))
    {
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

// Largest body frame we hand to hyper in one piece. Hyper turns each frame
// into a single IoSlice for vectored socket writes, and on Windows
// IoSlice::new panics for buffers larger than u32::MAX because WSABUF stores
// the length as a 32-bit integer. Keep frames far below that limit.
const MAX_BODY_FRAME_SIZE: usize = 4 * 1024 * 1024;

// Request body that yields its payload in frames of at most
// MAX_BODY_FRAME_SIZE bytes. Frames are zero-copy slices of the original
// buffer, so this adds no extra memory over sending the body whole.
struct ChunkedBody {
    rest: Bytes,
}

impl ChunkedBody {
    fn new(data: Bytes) -> Self {
        Self { rest: data }
    }
}

impl hyper::body::Body for ChunkedBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<std::result::Result<hyper::body::Frame<Bytes>, Self::Error>>> {
        let this = self.get_mut();
        if this.rest.is_empty() {
            return Poll::Ready(None);
        }
        let len = this.rest.len().min(MAX_BODY_FRAME_SIZE);
        let chunk = this.rest.split_to(len);
        Poll::Ready(Some(Ok(hyper::body::Frame::data(chunk))))
    }

    fn is_end_stream(&self) -> bool {
        self.rest.is_empty()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        hyper::body::SizeHint::with_exact(self.rest.len() as u64)
    }
}

// The IO worker owns a dedicated Tokio runtime on a separate thread, and processes
// the SyncEngine IO queue (HTTP and atomic file operations).
struct IoWorker {
    // Channel to wake the worker to process IO.
    tx: mpsc::UnboundedSender<()>,
    // Wakers to notify pending futures when IO makes progress.
    wakers: Arc<Mutex<Vec<Waker>>>,
}

impl IoWorker {
    fn spawn(
        sync: Arc<turso_sync_sdk_kit::rsapi::TursoDatabaseSync<Bytes>>,
        base_url: Option<String>,
        auth_token: Option<AuthTokenFn>,
    ) -> Arc<Self> {
        let (tx, rx) = mpsc::unbounded_channel::<()>();
        let wakers = Arc::new(Mutex::new(Vec::new()));
        let weak_sync = Arc::downgrade(&sync);

        let worker = Arc::new(Self {
            tx,
            wakers: wakers.clone(),
        });

        // Keep the worker thread independent from the handle so dropping the
        // last Database releases the sync engine immediately on Windows.
        std::thread::Builder::new()
            .name("turso-sync-io".to_string())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to build IO runtime");

                rt.block_on(async move {
                    IoWorker::run_loop(weak_sync, base_url, auth_token, rx, wakers).await
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
    fn notify_progress(wakers: &Mutex<Vec<Waker>>) {
        let wakers = {
            let mut guard = wakers.lock().unwrap();
            std::mem::take(&mut *guard)
        };
        for w in wakers {
            w.wake();
        }
    }

    async fn run_loop(
        sync: Weak<turso_sync_sdk_kit::rsapi::TursoDatabaseSync<Bytes>>,
        base_url: Option<String>,
        auth_token: Option<AuthTokenFn>,
        mut rx: mpsc::UnboundedReceiver<()>,
        wakers: Arc<Mutex<Vec<Waker>>>,
    ) {
        // Create HTTPS-capable Hyper client.
        let mut http_connector = HttpConnector::new();
        http_connector.enforce_http(false);
        let https: HttpsConnector<HttpConnector> = HttpsConnector::<HttpConnector>::builder()
            .with_native_roots()
            .expect("failed to load native root CA certificates")
            .https_or_http()
            .enable_http1()
            .build();
        let client: Client<HttpsConnector<HttpConnector>, ChunkedBody> =
            Client::builder(TokioExecutor::new()).build::<_, ChunkedBody>(https);

        while rx.recv().await.is_some() {
            let Some(sync) = sync.upgrade() else {
                break;
            };
            // Process all pending items in the sync IO queue.
            let mut made_progress = false;
            loop {
                let item = sync.take_io_item();
                let Some(item) = item else {
                    sync.step_io_callbacks();
                    IoWorker::notify_progress(&wakers);
                    break;
                };

                made_progress = true;

                // Take the request by value so large HTTP bodies move into
                // the outgoing request instead of being copied.
                let (request, completion) = item.into_parts();
                match request {
                    turso_sync_sdk_kit::sync_engine_io::SyncEngineIoRequest::Http {
                        url,
                        method,
                        path,
                        body,
                        headers,
                    } => {
                        IoWorker::process_http(
                            &sync,
                            base_url.as_deref(),
                            auth_token.as_ref(),
                            &wakers,
                            &client,
                            url.as_deref(),
                            &method,
                            &path,
                            body.map(Bytes::from),
                            &headers,
                            completion,
                        )
                        .await;
                    }
                    turso_sync_sdk_kit::sync_engine_io::SyncEngineIoRequest::FullRead { path } => {
                        IoWorker::process_full_read(&path, completion, &sync).await;
                    }
                    turso_sync_sdk_kit::sync_engine_io::SyncEngineIoRequest::FullWrite {
                        path,
                        content,
                    } => {
                        IoWorker::process_full_write(&path, &content, completion, &sync).await;
                    }
                }
            }

            // Run queued IO callbacks and wake all pending ops, yielding control
            // to allow them to make progress before we loop again.
            if made_progress {
                sync.step_io_callbacks();
                IoWorker::notify_progress(&wakers);
                // Let waiting tasks run on their executors.
                tokio::task::yield_now().await;
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn process_http(
        sync: &turso_sync_sdk_kit::rsapi::TursoDatabaseSync<Bytes>,
        base_url: Option<&str>,
        auth_token: Option<&AuthTokenFn>,
        wakers: &Mutex<Vec<Waker>>,
        client: &Client<HttpsConnector<HttpConnector>, ChunkedBody>,
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
            let Some(url) = base_url.or(url) else {
                completion.poison("remote_url is not available".to_string());
                return;
            };
            format!("{url}{p}")
        };

        // Resolve auth token (may fail if a dynamic provider returns an error).
        // Resolved here rather than once at spawn so dynamic providers can rotate
        // the token between requests.
        let auth_token = match auth_token {
            Some(provider) => match provider().await {
                Ok(token) => Some(token),
                Err(err) => {
                    completion.poison(format!("failed to resolve auth token: {err}"));
                    sync.step_io_callbacks();
                    return;
                }
            },
            None => None,
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
            if let Some(token) = &auth_token {
                if !headers_map.contains_key(AUTHORIZATION) {
                    let value = format!("Bearer {token}");
                    if let Ok(hv) = hyper::header::HeaderValue::try_from(value.as_str()) {
                        headers_map.insert(AUTHORIZATION, hv);
                    }
                }
            }
        }

        let req_body = ChunkedBody::new(body.unwrap_or_default());

        let request = match builder.body(req_body) {
            Ok(r) => r,
            Err(err) => {
                completion.poison(format!("failed to build request: {err}"));
                sync.step_io_callbacks();
                return;
            }
        };

        let mut response = match client.request(request).await {
            Ok(r) => r,
            Err(err) => {
                completion.poison(format!("http request failed: {err}"));
                sync.step_io_callbacks();
                return;
            }
        };

        // Propagate status
        let status = response.status().as_u16();
        completion.status(status as u32);
        sync.step_io_callbacks();
        IoWorker::notify_progress(wakers);

        // Stream response body in chunks
        while let Some(frame_res) = response.body_mut().frame().await {
            match frame_res {
                Ok(frame) => {
                    if let Some(chunk) = frame.data_ref() {
                        completion.push_buffer(chunk.clone());
                        sync.step_io_callbacks();
                        IoWorker::notify_progress(wakers);
                    }
                }
                Err(err) => {
                    completion.poison(format!("error reading response body: {err}"));
                    sync.step_io_callbacks();
                    IoWorker::notify_progress(wakers);
                    return;
                }
            }
        }

        // Done streaming
        completion.done();
        sync.step_io_callbacks();
        IoWorker::notify_progress(wakers);
    }

    async fn process_full_read(
        path: &str,
        completion: turso_sync_sdk_kit::sync_engine_io::SyncEngineIoCompletion<Bytes>,
        sync: &turso_sync_sdk_kit::rsapi::TursoDatabaseSync<Bytes>,
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
        sync: &turso_sync_sdk_kit::rsapi::TursoDatabaseSync<Bytes>,
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
#[path = "../tests/unit/sync/tests.rs"]
mod tests;
