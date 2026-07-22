//! Turso serverless driver: SQL over HTTP.
//!
//! This crate implements the client side of the SQL over HTTP protocol
//! (see `serverless/PROTOCOL.md`), for talking to a remote Turso database
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

mod column;
mod connection;
mod error;
pub mod params;
pub mod protocol;
mod rows;
mod session;
mod statement;
mod transaction;
mod value;

pub use column::Column;
pub use connection::Connection;
pub use error::{BoxError, Error, Result};
pub use params::{params_from_iter, IntoParams, IntoValue};
pub use rows::{Row, Rows};
pub use statement::Statement;
pub use transaction::{Transaction, TransactionBehavior};
pub use value::{FromValue, Value};

/// A builder for [`Database`].
pub struct Builder {
    url: String,
    auth_token: Option<String>,
}

impl Builder {
    /// Create a builder for a remote database.
    ///
    /// Accepts `libsql://`, `turso://`, `https://`, and `http://` URLs.
    pub fn new_remote(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            auth_token: None,
        }
    }

    /// Set the authentication token, sent as a bearer token with every
    /// request.
    pub fn with_auth_token(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }

    /// Build the database handle.
    pub async fn build(self) -> Result<Database> {
        Ok(Database {
            url: self.url,
            auth_token: self.auth_token,
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
    auth_token: Option<String>,
}

impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database").field("url", &self.url).finish()
    }
}

impl Database {
    /// Create a new connection to the database.
    pub fn connect(&self) -> Result<Connection> {
        Ok(Connection::new(&self.url, self.auth_token.clone()))
    }
}
