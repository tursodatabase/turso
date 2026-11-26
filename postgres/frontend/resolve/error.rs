// Translated from PostgreSQL 18.6, src/backend/parser/parse_func.c and src/backend/parser/parse_coerce.c. See licenses/postgres-license.md.

/// The error PostgreSQL raises when it cannot choose a function or an operator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ResolveError {
    NoMatch {
        message: String,
        hint: &'static str,
    },
    Ambiguous {
        message: String,
        hint: &'static str,
    },
    DatatypeMismatch {
        message: String,
        detail: Option<String>,
    },
    UndefinedObject {
        message: String,
    },
}

impl ResolveError {
    pub(crate) fn sqlstate(&self) -> &'static str {
        match self {
            ResolveError::NoMatch { .. } => "42883",
            ResolveError::Ambiguous { .. } => "42725",
            ResolveError::DatatypeMismatch { .. } => "42804",
            ResolveError::UndefinedObject { .. } => "42704",
        }
    }

    pub(crate) fn message(&self) -> &str {
        match self {
            ResolveError::NoMatch { message, .. }
            | ResolveError::Ambiguous { message, .. }
            | ResolveError::DatatypeMismatch { message, .. }
            | ResolveError::UndefinedObject { message } => message,
        }
    }

    pub(crate) fn hint(&self) -> Option<&'static str> {
        match self {
            ResolveError::NoMatch { hint, .. } | ResolveError::Ambiguous { hint, .. } => Some(hint),
            ResolveError::DatatypeMismatch { .. } | ResolveError::UndefinedObject { .. } => None,
        }
    }

    pub(crate) fn detail(&self) -> Option<&str> {
        match self {
            ResolveError::DatatypeMismatch { detail, .. } => detail.as_deref(),
            ResolveError::NoMatch { .. }
            | ResolveError::Ambiguous { .. }
            | ResolveError::UndefinedObject { .. } => None,
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

pub(crate) fn datatype_mismatch(message: String) -> ResolveError {
    ResolveError::DatatypeMismatch {
        message,
        detail: None,
    }
}

pub(crate) fn datatype_mismatch_with_detail(message: String, detail: String) -> ResolveError {
    ResolveError::DatatypeMismatch {
        message,
        detail: Some(detail),
    }
}

pub(crate) fn undefined_object(message: String) -> ResolveError {
    ResolveError::UndefinedObject { message }
}
