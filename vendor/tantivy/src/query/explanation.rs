use std::borrow::Cow;
use std::fmt;

use serde::Serialize;

use crate::{DocId, Score, TantivyError};

pub(crate) fn does_not_match(doc: DocId) -> TantivyError {
    TantivyError::InvalidArgument(format!("Document #({doc}) does not match"))
}

/// Object describing the score of a given document.
/// It is organized in trees.
///
/// `.to_pretty_json()` can be useful to print out a human readable
/// representation of this tree when debugging a given score.
#[derive(Clone, Serialize)]
pub struct Explanation {
    value: Score,
    description: Cow<'static, str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    details: Option<Vec<Explanation>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    context: Option<Vec<String>>,
}
impl fmt::Debug for Explanation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Explanation({})", self.to_pretty_json())
    }
}

impl Explanation {
    /// Creates a new explanation object.
    pub fn new_with_string(description: String, value: Score) -> Explanation {
        Explanation {
            value,
            description: Cow::Owned(description),
            details: None,
            context: None,
        }
    }
    /// Creates a new explanation object.
    pub fn new(description: &'static str, value: Score) -> Explanation {
        Explanation {
            value,
            description: Cow::Borrowed(description),
            details: None,
            context: None,
        }
    }

    /// Returns the value associated with the current node.
    pub fn value(&self) -> Score {
        self.value
    }

    /// Add some detail, explaining some part of the current node formula.
    ///
    /// Details are treated as child of the current node.
    pub fn add_detail(&mut self, child_explanation: Explanation) {
        self.details
            .get_or_insert_with(Vec::new)
            .push(child_explanation);
    }

    /// Adds some extra context to the explanation.
    pub fn add_context(&mut self, context: String) {
        self.context.get_or_insert_with(Vec::new).push(context);
    }

    /// Shortcut for `self.details.push(Explanation::new(name, value));`
    pub fn add_const(&mut self, name: &'static str, value: Score) {
        self.details
            .get_or_insert_with(Vec::new)
            .push(Explanation::new(name, value));
    }

    /// Returns an indented json representation of the explanation tree for debug usage.
    pub fn to_pretty_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap()
    }
}
