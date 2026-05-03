use crate::{Error, Result};
use futures::StreamExt;
use reqwest::Client;

use crate::protocol::{
    CursorBatch, CursorEntry, CursorRequest, CursorResponse, PipelineRequest, PipelineResponse,
};

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

/// A session manages connection state (baton) with a remote Turso server.
pub struct Session {
    client: Client,
    auth_token: Option<String>,
    baton: Option<String>,
    base_url: String,
    keep_alive: bool,
}

impl Session {
    pub fn new(url: String, auth_token: Option<String>) -> Self {
        let client = Client::new();
        Self {
            client,
            auth_token,
            baton: None,
            base_url: normalize_url(&url),
            keep_alive: false,
        }
    }

    pub fn set_keep_alive(&mut self, val: bool) {
        self.keep_alive = val;
    }

    fn auth_header(&self) -> Option<String> {
        self.auth_token.as_ref().map(|t| format!("Bearer {t}"))
    }

    /// Execute a cursor request (streaming NDJSON for queries with rows).
    #[allow(dead_code)]
    pub async fn execute_cursor(&mut self, batch: CursorBatch) -> Result<Vec<CursorEntry>> {
        let baton = if self.keep_alive {
            self.baton.take()
        } else {
            None
        };
        let request = CursorRequest { baton, batch };

        let url = format!("{}/v3/cursor", self.base_url);
        let mut req = self
            .client
            .post(&url)
            .header("Content-Type", "application/json");

        if let Some(auth) = self.auth_header() {
            req = req.header("Authorization", auth);
        }

        let body = serde_json::to_string(&request)
            .map_err(|e| Error::Error(format!("JSON serialization error: {e}")))?;

        let response = req
            .body(body)
            .send()
            .await
            .map_err(|e| Error::Error(format!("HTTP error: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            // Try to parse error message from body
            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&body) {
                if let Some(msg) = parsed.get("message").and_then(|m| m.as_str()) {
                    return Err(Error::Error(msg.to_string()));
                }
            }
            return Err(Error::Error(format!("HTTP error! status: {status}")));
        }

        // Parse NDJSON stream: first line is CursorResponse, rest are CursorEntry
        let bytes = response
            .bytes_stream()
            .fold(Vec::new(), |mut acc, chunk| async move {
                if let Ok(chunk) = chunk {
                    acc.extend_from_slice(&chunk);
                }
                acc
            })
            .await;

        let text = String::from_utf8_lossy(&bytes);
        let mut lines = text.lines().filter(|l| !l.trim().is_empty());

        // First line: CursorResponse
        let first_line = lines
            .next()
            .ok_or_else(|| Error::Error("No cursor response received".to_string()))?;

        let cursor_response: CursorResponse = serde_json::from_str(first_line)
            .map_err(|e| Error::Error(format!("Failed to parse cursor response: {e}")))?;

        if self.keep_alive {
            self.baton = cursor_response.baton;
        }
        if let Some(base_url) = cursor_response.base_url {
            self.base_url = base_url;
        }

        // Remaining lines: CursorEntry
        let mut entries = Vec::new();
        for line in lines {
            let entry: CursorEntry = serde_json::from_str(line)
                .map_err(|e| Error::Error(format!("Failed to parse cursor entry: {e}")))?;
            entries.push(entry);
        }

        Ok(entries)
    }

    /// Execute a pipeline request (JSON request/response for describe, sequence, close).
    pub async fn execute_pipeline(
        &mut self,
        mut requests: Vec<crate::protocol::StreamRequest>,
    ) -> Result<PipelineResponse> {
        let baton = if self.keep_alive {
            self.baton.take()
        } else {
            // When not keeping alive, append Close so the server releases the stream.
            requests.push(crate::protocol::StreamRequest::Close);
            None
        };
        let request = PipelineRequest { baton, requests };

        let url = format!("{}/v3/pipeline", self.base_url);
        let mut req = self
            .client
            .post(&url)
            .header("Content-Type", "application/json");

        if let Some(auth) = self.auth_header() {
            req = req.header("Authorization", auth);
        }

        let body = serde_json::to_string(&request)
            .map_err(|e| Error::Error(format!("JSON serialization error: {e}")))?;

        let response = req
            .body(body)
            .send()
            .await
            .map_err(|e| Error::Error(format!("HTTP error: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            return Err(Error::Error(format!("HTTP error! status: {status}")));
        }

        let pipeline_response: PipelineResponse = response
            .json()
            .await
            .map_err(|e| Error::Error(format!("Failed to parse pipeline response: {e}")))?;

        if self.keep_alive {
            self.baton.clone_from(&pipeline_response.baton);
        }
        if let Some(base_url) = &pipeline_response.base_url {
            self.base_url.clone_from(base_url);
        }

        // On error outside a transaction, reset baton so we don't leak
        // a stale stream.  Inside a transaction (keep_alive=true) the
        // server keeps the stream alive for SQL-level errors so the
        // client can still ROLLBACK — trust the baton in the response.
        if !self.keep_alive {
            let has_error = pipeline_response
                .results
                .iter()
                .any(|r| matches!(r, crate::protocol::StreamResult::Error { .. }));
            if has_error {
                self.baton = None;
            }
        }

        Ok(pipeline_response)
    }

    /// Close the session by sending a close request to the server.
    pub async fn close(&mut self) -> Result<()> {
        if self.baton.is_some() {
            let _ = self
                .execute_pipeline(vec![crate::protocol::StreamRequest::Close])
                .await;
        }
        self.baton = None;
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
    fn test_normalize_url_https() {
        assert_eq!(
            normalize_url("https://my-db.turso.io"),
            "https://my-db.turso.io"
        );
    }

    #[test]
    fn test_normalize_url_http() {
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
    fn test_normalize_url_libsql_with_port() {
        assert_eq!(
            normalize_url("libsql://my-db.turso.io:8080"),
            "https://my-db.turso.io:8080"
        );
    }

    #[test]
    fn test_normalize_url_with_path() {
        assert_eq!(
            normalize_url("turso://my-db.turso.io/v1/db"),
            "https://my-db.turso.io/v1/db"
        );
    }

    #[test]
    fn test_normalize_url_with_query_params() {
        assert_eq!(
            normalize_url("libsql://my-db.turso.io?foo=bar"),
            "https://my-db.turso.io?foo=bar"
        );
    }

    #[test]
    fn test_normalize_url_ws_passthrough() {
        assert_eq!(normalize_url("ws://localhost:8080"), "ws://localhost:8080");
    }

    #[test]
    fn test_normalize_url_wss_passthrough() {
        assert_eq!(
            normalize_url("wss://my-db.turso.io"),
            "wss://my-db.turso.io"
        );
    }

    #[test]
    fn test_normalize_url_empty() {
        assert_eq!(normalize_url(""), "");
    }

    #[test]
    fn test_normalize_url_libsql_with_path_and_query() {
        assert_eq!(
            normalize_url("libsql://my-db.turso.io/db?timeout=30"),
            "https://my-db.turso.io/db?timeout=30"
        );
    }
}
