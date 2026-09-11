//! Turso serverless driver: SQL over HTTP.
//!
//! This crate implements the client side of the [SQL over HTTP
//! protocol](https://github.com/tursodatabase/turso/blob/main/serverless/PROTOCOL.md),
//! for talking to a remote Turso database
//! from environments where the only networking primitive is an HTTP
//! request. The API mirrors the embedded `turso` driver, so the same code
//! can run against a local database or Turso Cloud.
//!
//! # Example
//!
//! ```rust,no_run
//! # async fn run() -> turso_serverless::Result<()> {
//! let db = turso_serverless::Builder::new_remote("libsql://my-db.turso.io")
//!     .with_auth_token("<token>")
//!     .build()
//!     .await?;
//! let conn = db.connect()?;
//! let mut rows = conn.query("SELECT ?", ("hello",)).await?;
//! while let Some(row) = rows.next().await? {
//!     println!("{:?}", row.get_value(0)?);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Batches
//!
//! Multiple parameterized statements can be sent in a single HTTP request
//! with [`Connection::batch`], or atomically with
//! [`Connection::transactional_batch`]:
//!
//! ```rust,no_run
//! # async fn run(conn: turso_serverless::Connection) -> turso_serverless::Result<()> {
//! let results = conn
//!     .batch([
//!         ("INSERT INTO users (name) VALUES (?1)", ("Alice",)),
//!         ("INSERT INTO users (name) VALUES (?1)", ("Bob",)),
//!     ])
//!     .await?;
//! assert_eq!(results.len(), 2);
//! # Ok(())
//! # }
//! ```

use std::{future::Future, pin::Pin, sync::Arc};

pub mod batch;
mod column;
pub mod connection;
mod error;
pub mod params;
pub mod protocol;
mod rows;
mod session;
mod statement;
pub mod transaction;
pub mod value;

pub use batch::{BatchResult, BatchStatement, IntoBatchStatement};
pub use column::Column;
pub use connection::Connection;
pub use error::{BoxError, Error, Result};
pub use params::{params_from_iter, IntoParams, IntoValue, Params};
pub use protocol::ENCRYPTION_KEY_HEADER;
pub use rows::{Row, Rows};
pub use statement::Statement;
pub use transaction::{Transaction, TransactionBehavior};
pub use value::{FromValue, Value};

/// Future returned by an auth token provider. Resolves to a bearer token
/// string (without the `Bearer ` prefix — that prefix is added when building
/// the header).
pub type AuthTokenFut = Pin<Box<dyn Future<Output = Result<String>> + Send + 'static>>;

/// Async callback that produces an auth token on demand. Invoked before every
/// HTTP request, so it can return a freshly-rotated token (e.g. fetched from
/// a secrets manager or refreshed via OAuth).
pub type AuthTokenFn = Arc<dyn Fn() -> AuthTokenFut + Send + Sync + 'static>;

/// A builder for [`Database`].
pub struct Builder {
    url: String,
    auth_token: Option<AuthTokenFn>,
    remote_encryption_key: Option<String>,
}

impl Builder {
    /// Create a builder for a remote database.
    ///
    /// Accepts `libsql://`, `turso://`, `https://`, and `http://` URLs.
    pub fn new_remote(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            auth_token: None,
            remote_encryption_key: None,
        }
    }

    /// Set the authentication token, sent as a bearer token with every
    /// request.
    ///
    /// Calling this overrides any previously configured token callback.
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
    /// freshly rotated token (e.g. fetched from a secrets manager or
    /// refreshed via OAuth). If the callback returns an error, the in-flight
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

    /// Set the customer-managed encryption key (base64-encoded) for an
    /// encrypted database.
    pub fn with_remote_encryption_key(mut self, base64_key: impl Into<String>) -> Self {
        self.remote_encryption_key = Some(base64_key.into());
        self
    }

    /// Build the database handle.
    pub async fn build(self) -> Result<Database> {
        Ok(Database {
            url: self.url,
            auth_token: self.auth_token,
            remote_encryption_key: self.remote_encryption_key,
        })
    }
}

/// A remote database handle.
///
/// Holds the URL and authentication token needed to create connections;
/// no network traffic happens until a connection executes a statement.
#[derive(Clone)]
pub struct Database {
    url: String,
    auth_token: Option<AuthTokenFn>,
    remote_encryption_key: Option<String>,
}

impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database").field("url", &self.url).finish()
    }
}

impl Database {
    /// Create a new connection to the database.
    pub fn connect(&self) -> Result<Connection> {
        Ok(Connection::new(
            &self.url,
            self.auth_token.clone(),
            self.remote_encryption_key.clone(),
        ))
    }
}
