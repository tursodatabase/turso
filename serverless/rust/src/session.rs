use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use reqwest::Client;

use crate::protocol::{
    PipelineRequest, PipelineResponse, StreamRequest, StreamResponse, StreamResult,
};
use crate::{Error, Result};

/// Header carrying the base64 encryption key for encrypted Turso Cloud DBs.
pub const ENCRYPTION_KEY_HEADER: &str = "x-turso-encryption-key";

/// Normalize a database URL: rewrite `turso://` and `libsql://` to `https://`.
pub fn normalize_url(url: &str) -> String {
    if let Some(rest) = url.strip_prefix("libsql://") {
        format!("https://{rest}")
    } else if let Some(rest) = url.strip_prefix("turso://") {
        format!("https://{rest}")
    } else {
        url.to_string()
    }
}

/// Configuration for a [`Session`].
#[derive(Clone, Default)]
pub struct SessionConfig {
    pub auth_token: Option<String>,
    pub request_headers: Vec<(String, String)>,
    pub encryption_key: Option<String>,
    /// Per-request timeout in milliseconds (0/None disables).
    pub query_timeout_ms: Option<u64>,
}

/// A session manages connection state (baton, autocommit) with a remote server.
///
/// Transaction state is server-authoritative: every pipeline request carries a
/// `get_autocommit` request and the cached autocommit flag is refreshed from
/// the answer. The baton is always persisted across requests — a non-null baton
/// does NOT imply an open transaction.
pub struct Session {
    client: Client,
    config: SessionConfig,
    baton: Option<String>,
    base_url: String,
    /// Shared with the owning `Connection` so `is_autocommit()` can read it
    /// without locking the session.
    autocommit: Arc<AtomicBool>,
}

impl Session {
    pub fn new(url: String, config: SessionConfig, autocommit: Arc<AtomicBool>) -> Self {
        autocommit.store(true, Ordering::SeqCst);
        Self {
            client: Client::new(),
            config,
            baton: None,
            base_url: normalize_url(&url),
            autocommit,
        }
    }

    fn set_autocommit(&self, value: bool) {
        self.autocommit.store(value, Ordering::SeqCst);
    }

    fn apply_headers(&self, mut req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        req = req.header("Content-Type", "application/json");
        if let Some(token) = &self.config.auth_token {
            req = req.header("Authorization", format!("Bearer {token}"));
        }
        if let Some(key) = &self.config.encryption_key {
            req = req.header(ENCRYPTION_KEY_HEADER, key);
        }
        for (name, value) in &self.config.request_headers {
            req = req.header(name.as_str(), value.as_str());
        }
        if let Some(ms) = self.config.query_timeout_ms {
            if ms > 0 {
                req = req.timeout(Duration::from_millis(ms));
            }
        }
        req
    }

    /// Refresh the cached autocommit flag from a pipeline response's answer to
    /// the trailing `get_autocommit` request.
    fn update_autocommit(&self, response: &PipelineResponse) {
        for result in &response.results {
            if let StreamResult::Ok {
                response: StreamResponse::GetAutocommit(ga),
            } = result
            {
                self.set_autocommit(ga.is_autocommit);
                return;
            }
        }
    }

    /// Execute a pipeline request. Appends a `get_autocommit` request (unless
    /// disabled), always persists the returned baton, and refreshes the cached
    /// transaction state.
    pub async fn execute_pipeline(
        &mut self,
        requests: Vec<StreamRequest>,
    ) -> Result<PipelineResponse> {
        self.execute_pipeline_inner(requests, true).await
    }

    async fn execute_pipeline_inner(
        &mut self,
        mut requests: Vec<StreamRequest>,
        append_autocommit: bool,
    ) -> Result<PipelineResponse> {
        if append_autocommit {
            requests.push(StreamRequest::GetAutocommit);
        }
        let request = PipelineRequest {
            baton: self.baton.clone(),
            requests,
        };

        let url = format!("{}/v3/pipeline", self.base_url);
        let body = serde_json::to_string(&request)
            .map_err(|e| Error::Error(format!("JSON serialization error: {e}")))?;

        let response = self
            .apply_headers(self.client.post(&url))
            .body(body)
            .send()
            .await;

        let response = match response {
            Ok(r) => r,
            Err(e) => {
                // A dead stream means the server rolled back; reset to autocommit.
                self.baton = None;
                self.set_autocommit(true);
                return Err(Error::Error(format!("HTTP error: {e}")));
            }
        };

        if !response.status().is_success() {
            let status = response.status();
            self.baton = None;
            self.set_autocommit(true);
            return Err(Error::Error(format!("HTTP error! status: {status}")));
        }

        let pipeline_response: PipelineResponse = response
            .json()
            .await
            .map_err(|e| Error::Error(format!("Failed to parse pipeline response: {e}")))?;

        // Always persist the baton; it does not imply an open transaction.
        self.baton.clone_from(&pipeline_response.baton);
        if let Some(base_url) = &pipeline_response.base_url {
            self.base_url.clone_from(base_url);
        }
        if append_autocommit {
            self.update_autocommit(&pipeline_response);
        }

        Ok(pipeline_response)
    }

    /// Close the session by sending a close request to the server.
    pub async fn close(&mut self) -> Result<()> {
        if self.baton.is_some() {
            let _ = self
                .execute_pipeline_inner(vec![StreamRequest::Close], false)
                .await;
        }
        self.baton = None;
        self.base_url = String::new();
        self.set_autocommit(true);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_url_libsql() {
        assert_eq!(
            normalize_url("libsql://my-db.turso.io"),
            "https://my-db.turso.io"
        );
    }

    #[test]
    fn test_normalize_url_turso() {
        assert_eq!(
            normalize_url("turso://my-db.turso.io"),
            "https://my-db.turso.io"
        );
    }

    #[test]
    fn test_normalize_url_https_passthrough() {
        assert_eq!(
            normalize_url("https://my-db.turso.io"),
            "https://my-db.turso.io"
        );
    }

    #[test]
    fn test_normalize_url_http_passthrough() {
        assert_eq!(
            normalize_url("http://localhost:8080"),
            "http://localhost:8080"
        );
    }

    #[test]
    fn test_normalize_url_turso_with_port() {
        assert_eq!(
            normalize_url("turso://my-db.turso.io:443"),
            "https://my-db.turso.io:443"
        );
    }

    #[test]
    fn test_normalize_url_with_path_and_query() {
        assert_eq!(
            normalize_url("libsql://my-db.turso.io/db?timeout=30"),
            "https://my-db.turso.io/db?timeout=30"
        );
    }

    #[test]
    fn test_normalize_url_empty() {
        assert_eq!(normalize_url(""), "");
    }
}
