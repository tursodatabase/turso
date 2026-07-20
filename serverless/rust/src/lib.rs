mod column;
mod connection;
mod error;
pub mod params;
mod protocol;
mod rows;
mod session;
mod statement;
mod transaction;
mod value;

pub use column::Column;
pub use connection::{BatchResult, BatchStatement, Connection};
pub use error::{BoxError, Error, Result};
pub use params::{params_from_iter, IntoParams, IntoValue, Params};
pub use rows::{Row, Rows};
pub use session::{SessionConfig, ENCRYPTION_KEY_HEADER};
pub use statement::Statement;
pub use transaction::{DropBehavior, Transaction, TransactionBehavior};
pub use value::{Value, ValueRef, ValueType};

/// Configuration for connecting to a remote Turso database.
pub struct Builder {
    url: String,
    config: SessionConfig,
}

impl Builder {
    /// Create a new remote database builder.
    ///
    /// Accepts `turso://`, `https://`, `http://`, or `libsql://` URLs.
    pub fn new_remote(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            config: SessionConfig::default(),
        }
    }

    /// Set the authentication token.
    pub fn with_auth_token(mut self, token: impl Into<String>) -> Self {
        self.config.auth_token = Some(token.into());
        self
    }

    /// Set the base64 encryption key sent as `x-turso-encryption-key` for
    /// encrypted Turso Cloud databases.
    pub fn with_encryption_key(mut self, key: impl Into<String>) -> Self {
        self.config.encryption_key = Some(key.into());
        self
    }

    /// Attach an extra HTTP header to every request. Passing a `Host` header
    /// (case-insensitive) makes `build()` return an error.
    pub fn with_request_header(
        mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.config
            .request_headers
            .push((name.into(), value.into()));
        self
    }

    /// Set the default per-request timeout in milliseconds (0 disables).
    pub fn with_query_timeout(mut self, milliseconds: u64) -> Self {
        self.config.query_timeout_ms = Some(milliseconds);
        self
    }

    /// Build the remote database handle.
    pub async fn build(self) -> Result<Database> {
        for (name, _) in &self.config.request_headers {
            if name.eq_ignore_ascii_case("host") {
                return Err(Error::Misuse(
                    "overwriting the 'Host' header is not supported".to_string(),
                ));
            }
        }
        Ok(Database {
            url: self.url,
            config: self.config,
        })
    }
}

/// A remote database handle.
///
/// Unlike the local `turso::Database`, this does not contain an embedded
/// database engine. It only holds the URL and config needed to create
/// connections that talk to a remote Turso server over HTTP.
#[derive(Clone)]
pub struct Database {
    url: String,
    config: SessionConfig,
}

impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database").finish()
    }
}

impl Database {
    /// Create a new connection to the remote database.
    pub fn connect(&self) -> Result<Connection> {
        Ok(Connection::new(self.url.clone(), self.config.clone()))
    }
}
