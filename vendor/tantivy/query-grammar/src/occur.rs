use std::fmt;
use std::fmt::Write;

use serde::Serialize;

/// Defines whether a term in a query must be present,
/// should be present or must not be present.
#[derive(Debug, Clone, Hash, Copy, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Occur {
    /// For a given document to be considered for scoring,
    /// at least one of the queries with the Should or the Must
    /// Occur constraint must be within the document.
    Should,
    /// Document without the queries are excluded from the search.
    Must,
    /// Document that contain the query are excluded from the
    /// search.
    MustNot,
}

impl Occur {
    /// Returns the one-char prefix symbol for this `Occur`.
    /// - `Should` => '?',
    /// - `Must` => '+'
    /// - `Not` => '-'
    fn to_char(self) -> char {
        match self {
            Occur::Should => '?',
            Occur::Must => '+',
            Occur::MustNot => '-',
        }
    }

    /// Compose two occur values.
    pub fn compose(left: Occur, right: Occur) -> Occur {
        match (left, right) {
            (Occur::Should, _) => right,
            (Occur::Must, Occur::MustNot) => Occur::MustNot,
            (Occur::Must, _) => Occur::Must,
            (Occur::MustNot, Occur::MustNot) => Occur::Must,
            (Occur::MustNot, _) => Occur::MustNot,
        }
    }
}

impl fmt::Display for Occur {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_char(self.to_char())
    }
}

#[cfg(test)]
mod test {
    use crate::Occur;

    #[test]
    fn test_occur_compose() {
        assert_eq!(Occur::compose(Occur::Should, Occur::Should), Occur::Should);
        assert_eq!(Occur::compose(Occur::Should, Occur::Must), Occur::Must);
        assert_eq!(
            Occur::compose(Occur::Should, Occur::MustNot),
            Occur::MustNot
        );
        assert_eq!(Occur::compose(Occur::Must, Occur::Should), Occur::Must);
        assert_eq!(Occur::compose(Occur::Must, Occur::Must), Occur::Must);
        assert_eq!(Occur::compose(Occur::Must, Occur::MustNot), Occur::MustNot);
        assert_eq!(
            Occur::compose(Occur::MustNot, Occur::Should),
            Occur::MustNot
        );
        assert_eq!(Occur::compose(Occur::MustNot, Occur::Must), Occur::MustNot);
        assert_eq!(Occur::compose(Occur::MustNot, Occur::MustNot), Occur::Must);
    }
}
