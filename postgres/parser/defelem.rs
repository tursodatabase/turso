//! Reads the arguments of statement options (DefElem nodes) the way
//! PostgreSQL's commands/define.c does.

use pg_query::protobuf::{node::Node, DefElem};

use crate::ParseError;

/// Reads a Boolean option the way PostgreSQL's defGetBoolean does: an option
/// with no value is true, and true, false, on, off (in any case), 0 and 1 are
/// accepted. Any other value is a syntax error.
pub(crate) fn boolean_option(option: &DefElem) -> Result<bool, ParseError> {
    let value = match option.arg.as_ref().and_then(|arg| arg.node.as_ref()) {
        None => Some(true),
        Some(Node::Boolean(value)) => Some(value.boolval),
        Some(Node::Integer(value)) => match value.ival {
            0 => Some(false),
            1 => Some(true),
            _ => None,
        },
        Some(Node::String(value)) => match value.sval.to_ascii_lowercase().as_str() {
            "true" | "on" => Some(true),
            "false" | "off" => Some(false),
            _ => None,
        },
        Some(_) => None,
    };
    value.ok_or_else(|| {
        ParseError::syntax_error(format!("{} requires a Boolean value", option.defname))
    })
}
