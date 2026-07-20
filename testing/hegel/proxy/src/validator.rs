use std::collections::HashSet;

use crate::protocol::{PipelineRequest, PipelineResponse, StreamRequest};
use crate::state::{detect_transaction_boundaries, StreamState};

/// A violation of a protocol property.
#[derive(Debug)]
pub struct Violation {
    pub property: &'static str,
    pub message: String,
}

impl std::fmt::Display for Violation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.property, self.message)
    }
}

/// Validate a pipeline request against the current stream state.
/// Returns a list of violations (empty = valid).
pub fn validate_request(
    state: &StreamState,
    req: &PipelineRequest,
    seen_batons: &HashSet<String>,
) -> Vec<Violation> {
    let mut violations = Vec::new();

    // Collect all SQL from the request
    let all_sql: Vec<&str> = req.requests.iter().flat_map(|r| r.sql_strings()).collect();

    let (any_begins, _any_ends) = detect_transaction_boundaries(&all_sql);
    let has_close = req.requests.iter().any(StreamRequest::is_close);

    // Property: baton_freshness
    // After a close or HTTP-level error, the next request must use baton: null
    if (state.last_had_close || state.last_had_http_error) && req.baton.is_some() {
        violations.push(Violation {
            property: "baton_freshness",
            message: format!(
                "Previous pipeline {} but this request has baton {:?} instead of null",
                if state.last_had_close {
                    "included close"
                } else {
                    "got HTTP error"
                },
                req.baton
            ),
        });
    }

    // Property: no_baton_reuse
    // The server increments baton_seq after each request. Sending a previously-used
    // baton causes BatonReused (HTTP 400).
    if let Some(ref baton) = req.baton {
        if seen_batons.contains(baton) {
            violations.push(Violation {
                property: "no_baton_reuse",
                message: format!("Baton {baton:?} was already used in a previous request"),
            });
        }
    }

    // Property: baton_continuity
    // During a transaction, the baton must match the previous response's baton
    if state.in_transaction {
        if let Some(ref last) = state.last_baton {
            match &req.baton {
                Some(b) if b != last => {
                    violations.push(Violation {
                        property: "baton_continuity",
                        message: format!("In transaction: expected baton {last:?}, got {b:?}",),
                    });
                }
                None => {
                    violations.push(Violation {
                        property: "baton_continuity",
                        message: format!("In transaction: expected baton {last:?}, got null",),
                    });
                }
                _ => {} // matches, all good
            }
        }
    }

    // Property: stream_lifecycle
    // Non-transactional pipelines must include a close request.
    // "Non-transactional" means: not currently in a transaction AND not starting one.
    if !state.in_transaction && !any_begins && !has_close {
        violations.push(Violation {
            property: "stream_lifecycle",
            message: "Non-transactional pipeline does not include close request".to_string(),
        });
    }

    // Property: close_is_terminal
    // If a pipeline includes close, it must be the last request.
    // The server executes all requests in order — any request after close
    // will fail with BatonStreamClosed.
    if has_close {
        if let Some(pos) = req.requests.iter().position(StreamRequest::is_close) {
            if pos + 1 < req.requests.len() {
                violations.push(Violation {
                    property: "close_is_terminal",
                    message: format!(
                        "Close is at position {} but pipeline has {} requests — close must be last",
                        pos,
                        req.requests.len()
                    ),
                });
            }
        }
    }

    violations
}

/// Update stream state after receiving a successful (HTTP 200) response.
///
/// Per-request errors (SQL errors, constraint violations) inside a 200 response
/// do NOT invalidate the stream — the server still returns a valid baton.
/// Only HTTP-level errors (non-200) invalidate the stream, and those are
/// handled separately in the proxy's main handler.
pub fn update_state(state: &mut StreamState, req: &PipelineRequest, resp: &PipelineResponse) {
    let all_sql: Vec<&str> = req.requests.iter().flat_map(|r| r.sql_strings()).collect();

    let (any_begins, any_ends) = detect_transaction_boundaries(&all_sql);
    let has_close = req.requests.iter().any(StreamRequest::is_close);

    // Update transaction state
    if any_begins {
        state.in_transaction = true;
    }
    if any_ends {
        state.in_transaction = false;
    }

    // Update baton from the server response
    state.last_baton = resp.baton.clone();

    // Track whether this pipeline had a close
    state.last_had_close = has_close;
    state.last_had_http_error = false;
}

/// Mark the stream state as having received an HTTP-level error.
/// The client should use baton: null on the next request.
pub fn mark_http_error(state: &mut StreamState) {
    state.last_baton = None;
    state.in_transaction = false;
    state.last_had_close = false;
    state.last_had_http_error = true;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::*;

    fn make_execute(sql: &str) -> StreamRequest {
        StreamRequest::Execute(ExecuteStreamReq {
            stmt: Stmt {
                sql: Some(sql.to_string()),
                sql_id: None,
                args: vec![],
                named_args: vec![],
                want_rows: Some(true),
                replication_index: None,
            },
        })
    }

    fn empty_seen() -> HashSet<String> {
        HashSet::new()
    }

    #[test]
    fn test_non_transactional_without_close_fails() {
        let state = StreamState::default();
        let req = PipelineRequest {
            baton: None,
            requests: vec![make_execute("SELECT 1")],
        };
        let violations = validate_request(&state, &req, &empty_seen());
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].property, "stream_lifecycle");
    }

    #[test]
    fn test_non_transactional_with_close_passes() {
        let state = StreamState::default();
        let req = PipelineRequest {
            baton: None,
            requests: vec![make_execute("SELECT 1"), StreamRequest::Close],
        };
        let violations = validate_request(&state, &req, &empty_seen());
        assert!(violations.is_empty());
    }

    #[test]
    fn test_begin_without_close_passes() {
        let state = StreamState::default();
        let req = PipelineRequest {
            baton: None,
            requests: vec![make_execute("BEGIN")],
        };
        let violations = validate_request(&state, &req, &empty_seen());
        assert!(violations.is_empty());
    }

    #[test]
    fn test_baton_freshness_after_close() {
        let state = StreamState {
            last_had_close: true,
            ..Default::default()
        };
        let req = PipelineRequest {
            baton: Some("stale-baton".to_string()),
            requests: vec![make_execute("SELECT 1"), StreamRequest::Close],
        };
        let violations = validate_request(&state, &req, &empty_seen());
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].property, "baton_freshness");
    }

    #[test]
    fn test_baton_freshness_after_http_error() {
        let state = StreamState {
            last_had_http_error: true,
            ..Default::default()
        };
        let req = PipelineRequest {
            baton: Some("stale-baton".to_string()),
            requests: vec![make_execute("SELECT 1"), StreamRequest::Close],
        };
        let violations = validate_request(&state, &req, &empty_seen());
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].property, "baton_freshness");
    }

    #[test]
    fn test_baton_continuity_in_transaction() {
        let state = StreamState {
            last_baton: Some("baton-1".to_string()),
            in_transaction: true,
            ..Default::default()
        };
        let req = PipelineRequest {
            baton: Some("wrong-baton".to_string()),
            requests: vec![make_execute("INSERT INTO t VALUES (1)")],
        };
        let violations = validate_request(&state, &req, &empty_seen());
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].property, "baton_continuity");
    }

    #[test]
    fn test_in_transaction_no_close_needed() {
        let state = StreamState {
            last_baton: Some("baton-1".to_string()),
            in_transaction: true,
            ..Default::default()
        };
        let req = PipelineRequest {
            baton: Some("baton-1".to_string()),
            requests: vec![make_execute("INSERT INTO t VALUES (1)")],
        };
        let violations = validate_request(&state, &req, &empty_seen());
        assert!(violations.is_empty());
    }

    #[test]
    fn test_close_is_terminal_passes_when_last() {
        let state = StreamState::default();
        let req = PipelineRequest {
            baton: None,
            requests: vec![make_execute("SELECT 1"), StreamRequest::Close],
        };
        let violations = validate_request(&state, &req, &empty_seen());
        assert!(violations.is_empty());
    }

    #[test]
    fn test_close_is_terminal_fails_when_not_last() {
        let state = StreamState::default();
        let req = PipelineRequest {
            baton: None,
            requests: vec![
                make_execute("SELECT 1"),
                StreamRequest::Close,
                make_execute("SELECT 2"),
            ],
        };
        let violations = validate_request(&state, &req, &empty_seen());
        assert!(violations.iter().any(|v| v.property == "close_is_terminal"));
    }

    #[test]
    fn test_no_baton_reuse() {
        let state = StreamState::default();
        let mut seen = HashSet::new();
        seen.insert("old-baton".to_string());
        let req = PipelineRequest {
            baton: Some("old-baton".to_string()),
            requests: vec![make_execute("SELECT 1"), StreamRequest::Close],
        };
        let violations = validate_request(&state, &req, &seen);
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].property, "no_baton_reuse");
    }

    #[test]
    fn test_sql_error_does_not_invalidate_stream() {
        let mut state = StreamState {
            last_baton: Some("baton-1".to_string()),
            in_transaction: true,
            ..Default::default()
        };
        let req = PipelineRequest {
            baton: Some("baton-1".to_string()),
            requests: vec![make_execute("INSERT INTO nonexistent VALUES (1)")],
        };
        // Simulate a 200 response with a per-request error but valid baton
        let resp = PipelineResponse {
            baton: Some("baton-2".to_string()),
            base_url: None,
            results: vec![StreamResult::Error {
                error: Error {
                    message: "no such table: nonexistent".to_string(),
                    code: "SQLITE_ERROR".to_string(),
                },
            }],
        };
        update_state(&mut state, &req, &resp);
        // Stream should still be alive — baton updated, still in transaction
        assert_eq!(state.last_baton, Some("baton-2".to_string()));
        assert!(state.in_transaction);
        assert!(!state.last_had_http_error);
    }

    #[test]
    fn test_http_error_invalidates_stream() {
        let mut state = StreamState {
            last_baton: Some("baton-1".to_string()),
            in_transaction: true,
            ..Default::default()
        };
        mark_http_error(&mut state);
        assert!(state.last_baton.is_none());
        assert!(!state.in_transaction);
        assert!(state.last_had_http_error);
    }
}
