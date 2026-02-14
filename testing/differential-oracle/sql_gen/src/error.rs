//! Error types for SQL generation.

use std::fmt;

/// Error that can occur during SQL generation.
#[derive(Debug, Clone)]
pub struct GenError {
    pub kind: GenErrorKind,
    pub scope: String,
    pub context: Vec<String>,
}

/// Kind of generation error.
#[derive(Debug, Clone)]
pub enum GenErrorKind {
    /// No valid candidates available (all filtered out by policy/capabilities).
    Exhausted { reason: String },

    /// Schema doesn't have required objects.
    SchemaEmpty { needed: String },

    /// Recursion limit reached.
    DepthExceeded { limit: usize },

    /// Invalid configuration.
    InvalidConfig { message: String },
}

impl GenError {
    /// Create an exhausted error.
    pub fn exhausted(scope: &str, reason: impl Into<String>) -> Self {
        Self {
            kind: GenErrorKind::Exhausted {
                reason: reason.into(),
            },
            scope: scope.to_string(),
            context: vec![],
        }
    }

    /// Create a schema empty error.
    pub fn schema_empty(needed: impl Into<String>) -> Self {
        Self {
            kind: GenErrorKind::SchemaEmpty {
                needed: needed.into(),
            },
            scope: String::new(),
            context: vec![],
        }
    }

    /// Create a depth exceeded error.
    pub fn depth_exceeded(limit: usize) -> Self {
        Self {
            kind: GenErrorKind::DepthExceeded { limit },
            scope: String::new(),
            context: vec![],
        }
    }

    /// Create an invalid config error.
    pub fn invalid_config(message: impl Into<String>) -> Self {
        Self {
            kind: GenErrorKind::InvalidConfig {
                message: message.into(),
            },
            scope: String::new(),
            context: vec![],
        }
    }

    /// Add context to the error.
    pub fn with_context(mut self, ctx: impl Into<String>) -> Self {
        self.context.push(ctx.into());
        self
    }

    /// Set the scope.
    pub fn in_scope(mut self, scope: impl Into<String>) -> Self {
        self.scope = scope.into();
        self
    }
}

impl fmt::Display for GenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            GenErrorKind::Exhausted { reason } => {
                if self.scope.is_empty() {
                    write!(f, "no valid candidates: {reason}")?;
                } else {
                    write!(f, "no valid candidates in scope '{}': {reason}", self.scope)?;
                }
            }
            GenErrorKind::SchemaEmpty { needed } => {
                write!(f, "schema missing required {needed}")?;
            }
            GenErrorKind::DepthExceeded { limit } => {
                write!(f, "recursion depth exceeded limit of {limit}")?;
            }
            GenErrorKind::InvalidConfig { message } => {
                write!(f, "invalid configuration: {message}")?;
            }
        }

        if !self.context.is_empty() {
            write!(f, "\n  context:")?;
            for c in &self.context {
                write!(f, "\n    - {c}")?;
            }
        }

        Ok(())
    }
}

impl std::error::Error for GenError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exhausted_error() {
        let err = GenError::exhausted("where clause", "no columns available");
        assert!(matches!(err.kind, GenErrorKind::Exhausted { .. }));
        assert_eq!(err.scope, "where clause");
    }

    #[test]
    fn test_error_with_context() {
        let err = GenError::exhausted("select", "no tables")
            .with_context("tried to generate SELECT")
            .with_context("schema was empty");

        assert_eq!(err.context.len(), 2);
        let msg = err.to_string();
        assert!(msg.contains("context:"));
        assert!(msg.contains("tried to generate SELECT"));
    }

    #[test]
    fn test_schema_empty_error() {
        let err = GenError::schema_empty("tables");
        assert!(matches!(err.kind, GenErrorKind::SchemaEmpty { .. }));
        assert!(err.to_string().contains("tables"));
    }

    #[test]
    fn test_depth_exceeded_error() {
        let err = GenError::depth_exceeded(5);
        assert!(matches!(err.kind, GenErrorKind::DepthExceeded { limit: 5 }));
    }
}
