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
pub use connection::Connection;
pub use error::{BoxError, Error, Result};
pub use params::Params;
pub use rows::{Row, Rows};
pub use statement::Statement;
pub use transaction::{Transaction, TransactionBehavior};
pub use value::{Value, ValueRef, ValueType};

/// Configuration for connecting to a remote Turso database.
pub struct Builder {
    url: String,
    auth_token: Option<String>,
}

impl Builder {
    /// Create a new remote database builder.
    ///
    /// Accepts `turso://`, `https://`, `http://`, or `libsql://` URLs.
    pub fn new_remote(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            auth_token: None,
        }
    }

    /// Set the authentication token.
    pub fn with_auth_token(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }

    /// Build the remote database handle.
    pub async fn build(self) -> Result<Database> {
        Ok(Database {
            url: self.url,
            auth_token: self.auth_token,
        })
    }
}

/// A remote database handle.
///
/// Unlike the local `turso::Database`, this does not contain an embedded
/// database engine. It only holds the URL and auth token needed to create
/// connections that talk to a remote Turso server over HTTP.
#[derive(Clone)]
pub struct Database {
    url: String,
    auth_token: Option<String>,
}

impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database").finish()
    }
}

impl Database {
    /// Create a new connection to the remote database.
    pub fn connect(&self) -> Result<Connection> {
        Ok(Connection::new(self.url.clone(), self.auth_token.clone()))
    }
}
