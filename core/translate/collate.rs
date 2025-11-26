use std::{cmp::Ordering, str::FromStr as _};

use tracing::Level;

// TODO: in the future allow user to define collation sequences
// Will have to meddle with ffi for this
#[derive(
    Debug, Clone, Copy, Eq, PartialEq, strum_macros::Display, strum_macros::EnumString, Default,
)]
#[strum(ascii_case_insensitive)]
/// **Pre defined collation sequences**\
/// Collating functions only matter when comparing string values.
/// Numeric values are always compared numerically, and BLOBs are always compared byte-by-byte using memcmp().
pub enum CollationSeq {
    /// Standard String compare
    #[default]
    Binary,
    /// Ascii case insensitive
    NoCase,
    /// Same as Binary but with trimmed whitespace
    Rtrim,
}

impl CollationSeq {
    pub fn new(collation: &str) -> crate::Result<Self> {
        CollationSeq::from_str(collation).map_err(|_| {
            crate::LimboError::ParseError(format!("no such collation sequence: {collation}"))
        })
    }

    pub fn compare_strings(&self, lhs: &str, rhs: &str) -> Ordering {
        tracing::event!(Level::DEBUG, collate = %self, lhs, rhs);
        match self {
            CollationSeq::Binary => Self::binary_cmp(lhs, rhs),
            CollationSeq::NoCase => Self::nocase_cmp(lhs, rhs),
            CollationSeq::Rtrim => Self::rtrim_cmp(lhs, rhs),
        }
    }

    fn binary_cmp(lhs: &str, rhs: &str) -> Ordering {
        lhs.cmp(rhs)
    }

    fn nocase_cmp(lhs: &str, rhs: &str) -> Ordering {
        let nocase_lhs = uncased::UncasedStr::new(lhs);
        let nocase_rhs = uncased::UncasedStr::new(rhs);
        nocase_lhs.cmp(nocase_rhs)
    }

    fn rtrim_cmp(lhs: &str, rhs: &str) -> Ordering {
        lhs.trim_end().cmp(rhs.trim_end())
    }
}
