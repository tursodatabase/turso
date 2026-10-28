//! What a role was not allowed to do.
//!
//! Core reports a denied action as data, not as a message. Each frontend turns
//! it into the message its users expect, so core never needs to know how, for
//! example, PostgreSQL words a permission error.

use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermissionDenied {
    pub action: Action,
    pub object: Object,
}

impl PermissionDenied {
    /// The role lacks the privilege for `action` on `object`.
    pub fn privilege(action: Action, object: Object) -> Self {
        Self { action, object }
    }
}

impl fmt::Display for PermissionDenied {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "permission denied: {} on {}", self.action, self.object)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    SetRole,
}

impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Action::SetRole => "set role",
        };
        f.write_str(name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Object {
    Role(String),
}

impl fmt::Display for Object {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Object::Role(name) => write!(f, "role {name}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn denials_name_the_action_and_the_object() {
        let set_role = PermissionDenied::privilege(Action::SetRole, Object::Role("r".to_string()));

        assert_eq!(
            set_role.to_string(),
            "permission denied: set role on role r"
        );
    }
}
