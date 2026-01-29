//! Elle integration for transactional consistency checking.
//!
//! Elle is a black-box transactional consistency checker from Jepsen that detects
//! anomalies (G0, G1, G2, G-Single from Adya's formalism) by analyzing transaction histories.
//!
//! This module provides types and utilities to record operations in Elle's expected format
//! and export them to EDN for analysis with elle-cli.

use std::fmt;
use std::fs::File;
use std::io::{self, Write};
use std::path::Path;

/// Elle operation types for the list-append model.
/// In this model, objects are lists of integers and transactions either append values or read lists.
#[derive(Debug, Clone)]
pub enum ElleOp {
    /// Append a value to a list identified by key
    Append { key: String, value: i64 },
    /// Read a list by key, result is None before execution and Some after
    Read {
        key: String,
        result: Option<Vec<i64>>,
    },
}

impl ElleOp {
    /// Convert to EDN format.
    /// Append: [:append "key" value]
    /// Read: [:r "key" nil] or [:r "key" [1 2 3]]
    pub fn to_edn(&self) -> String {
        match self {
            ElleOp::Append { key, value } => {
                format!("[:append \"{}\" {}]", escape_edn_string(key), value)
            }
            ElleOp::Read { key, result } => {
                let result_str = match result {
                    None => "nil".to_string(),
                    Some(vals) => {
                        if vals.is_empty() {
                            "[]".to_string()
                        } else {
                            let vals_str: Vec<String> =
                                vals.iter().map(|v| v.to_string()).collect();
                            format!("[{}]", vals_str.join(" "))
                        }
                    }
                };
                format!("[:r \"{}\" {}]", escape_edn_string(key), result_str)
            }
        }
    }
}

/// Event types matching Jepsen's expected values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElleEventType {
    /// Operation invoked but not yet completed
    Invoke,
    /// Operation completed successfully
    Ok,
    /// Operation failed (e.g., transaction aborted)
    Fail,
    /// Informational event (crash, etc.)
    Info,
}

impl fmt::Display for ElleEventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ElleEventType::Invoke => write!(f, ":invoke"),
            ElleEventType::Ok => write!(f, ":ok"),
            ElleEventType::Fail => write!(f, ":fail"),
            ElleEventType::Info => write!(f, ":info"),
        }
    }
}

/// A single history event in Elle's format.
#[derive(Debug, Clone)]
pub struct ElleEvent {
    /// Event type (invoke, ok, fail, info)
    pub event_type: ElleEventType,
    /// Operations in this transaction
    pub value: Vec<ElleOp>,
    /// Process ID (fiber_id in our case)
    pub process: usize,
    /// Monotonically increasing event index
    pub index: u64,
}

impl ElleEvent {
    /// Convert to EDN format.
    /// Example: {:type :ok, :f :txn, :value [[:append "x" 1] [:r "y" [1 2]]], :process 0, :index 5}
    pub fn to_edn(&self) -> String {
        let ops_str: Vec<String> = self.value.iter().map(|op| op.to_edn()).collect();
        format!(
            "{{:type {}, :f :txn, :value [{}], :process {}, :index {}}}",
            self.event_type,
            ops_str.join(" "),
            self.process,
            self.index
        )
    }
}

/// History recorder that accumulates Elle events during simulation.
#[derive(Debug, Default)]
pub struct ElleHistory {
    events: Vec<ElleEvent>,
    index_counter: u64,
}

impl ElleHistory {
    pub fn new() -> Self {
        Self {
            events: Vec::new(),
            index_counter: 0,
        }
    }

    /// Record an invoke event (operation started).
    pub fn record_invoke(&mut self, process: usize, ops: Vec<ElleOp>) {
        let event = ElleEvent {
            event_type: ElleEventType::Invoke,
            value: ops,
            process,
            index: self.index_counter,
        };
        self.index_counter += 1;
        self.events.push(event);
    }

    /// Record an ok event (operation completed successfully).
    pub fn record_ok(&mut self, process: usize, ops: Vec<ElleOp>) {
        let event = ElleEvent {
            event_type: ElleEventType::Ok,
            value: ops,
            process,
            index: self.index_counter,
        };
        self.index_counter += 1;
        self.events.push(event);
    }

    /// Record a fail event (operation failed/aborted).
    pub fn record_fail(&mut self, process: usize, ops: Vec<ElleOp>) {
        let event = ElleEvent {
            event_type: ElleEventType::Fail,
            value: ops,
            process,
            index: self.index_counter,
        };
        self.index_counter += 1;
        self.events.push(event);
    }

    /// Record an info event (crash, etc.).
    #[allow(dead_code)]
    pub fn record_info(&mut self, process: usize, ops: Vec<ElleOp>) {
        let event = ElleEvent {
            event_type: ElleEventType::Info,
            value: ops,
            process,
            index: self.index_counter,
        };
        self.index_counter += 1;
        self.events.push(event);
    }

    /// Get the number of recorded events.
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Check if history is empty.
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Export history to an EDN file.
    pub fn export_edn(&self, path: &Path) -> io::Result<()> {
        let mut file = File::create(path)?;
        for event in &self.events {
            writeln!(file, "{}", event.to_edn())?;
        }
        Ok(())
    }
}

/// Escape special characters in EDN strings.
fn escape_edn_string(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

/// Number of Elle keys to use (small key space ensures conflicts).
pub const ELLE_KEY_COUNT: usize = 10;

/// Generate an Elle key name from an index.
pub fn elle_key_name(index: usize) -> String {
    format!("k{index}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_elle_op_to_edn() {
        let append = ElleOp::Append {
            key: "x".to_string(),
            value: 42,
        };
        assert_eq!(append.to_edn(), "[:append \"x\" 42]");

        let read_nil = ElleOp::Read {
            key: "y".to_string(),
            result: None,
        };
        assert_eq!(read_nil.to_edn(), "[:r \"y\" nil]");

        let read_empty = ElleOp::Read {
            key: "z".to_string(),
            result: Some(vec![]),
        };
        assert_eq!(read_empty.to_edn(), "[:r \"z\" []]");

        let read_values = ElleOp::Read {
            key: "w".to_string(),
            result: Some(vec![1, 2, 3]),
        };
        assert_eq!(read_values.to_edn(), "[:r \"w\" [1 2 3]]");
    }

    #[test]
    fn test_elle_event_to_edn() {
        let event = ElleEvent {
            event_type: ElleEventType::Ok,
            value: vec![
                ElleOp::Append {
                    key: "x".to_string(),
                    value: 1,
                },
                ElleOp::Read {
                    key: "y".to_string(),
                    result: Some(vec![1, 2]),
                },
            ],
            process: 0,
            index: 5,
        };
        assert_eq!(
            event.to_edn(),
            "{:type :ok, :f :txn, :value [[:append \"x\" 1] [:r \"y\" [1 2]]], :process 0, :index 5}"
        );
    }

    #[test]
    fn test_elle_history_recording() {
        let mut history = ElleHistory::new();

        history.record_invoke(
            0,
            vec![ElleOp::Append {
                key: "x".to_string(),
                value: 1,
            }],
        );
        history.record_ok(
            0,
            vec![ElleOp::Append {
                key: "x".to_string(),
                value: 1,
            }],
        );

        assert_eq!(history.len(), 2);
        assert_eq!(history.events[0].index, 0);
        assert_eq!(history.events[1].index, 1);
    }
}
