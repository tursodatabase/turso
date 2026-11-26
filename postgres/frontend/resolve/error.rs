// Translated from PostgreSQL 18.6, src/backend/parser/parse_func.c. See licenses/postgres-license.md.

/// The error PostgreSQL raises when it cannot choose a function or an operator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ResolveError {
    NoMatch { message: String, hint: &'static str },
    Ambiguous { message: String, hint: &'static str },
}

impl ResolveError {
    pub(crate) fn sqlstate(&self) -> &'static str {
        match self {
            ResolveError::NoMatch { .. } => "42883",
            ResolveError::Ambiguous { .. } => "42725",
        }
    }

    pub(crate) fn message(&self) -> &str {
        match self {
            ResolveError::NoMatch { message, .. } | ResolveError::Ambiguous { message, .. } => {
                message
            }
        }
    }

    pub(crate) fn hint(&self) -> &'static str {
        match self {
            ResolveError::NoMatch { hint, .. } | ResolveError::Ambiguous { hint, .. } => hint,
        }
    }
}

pub(crate) fn function_does_not_exist(signature: &str) -> ResolveError {
    ResolveError::NoMatch {
        message: format!("function {signature} does not exist"),
        hint: "No function matches the given name and argument types. You might need to add explicit type casts.",
    }
}

pub(crate) fn function_is_not_unique(signature: &str) -> ResolveError {
    ResolveError::Ambiguous {
        message: format!("function {signature} is not unique"),
        hint:
            "Could not choose a best candidate function. You might need to add explicit type casts.",
    }
}
