use std::ops::BitOr;

use serde::{Deserialize, Serialize};

use crate::schema::flags::{IndexedFlag, SchemaFlagList, StoredFlag};

/// Define how a facet field should be handled by tantivy.
///
/// Note that a Facet is always indexed and stored as a fastfield.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct FacetOptions {
    stored: bool,
}

impl FacetOptions {
    /// Returns true if the value is stored.
    #[inline]
    pub fn is_stored(&self) -> bool {
        self.stored
    }

    /// Set the field as stored.
    ///
    /// Only the fields that are set as *stored* are
    /// persisted into the Tantivy's store.
    #[must_use]
    pub fn set_stored(mut self) -> FacetOptions {
        self.stored = true;
        self
    }
}

impl From<()> for FacetOptions {
    fn from(_: ()) -> FacetOptions {
        FacetOptions::default()
    }
}

impl From<StoredFlag> for FacetOptions {
    fn from(_: StoredFlag) -> Self {
        FacetOptions { stored: true }
    }
}

impl<T: Into<FacetOptions>> BitOr<T> for FacetOptions {
    type Output = FacetOptions;

    fn bitor(self, other: T) -> FacetOptions {
        let other = other.into();
        FacetOptions {
            stored: self.stored | other.stored,
        }
    }
}

impl<Head, Tail> From<SchemaFlagList<Head, Tail>> for FacetOptions
where
    Head: Clone,
    Tail: Clone,
    Self: BitOr<Output = Self> + From<Head> + From<Tail>,
{
    fn from(head_tail: SchemaFlagList<Head, Tail>) -> Self {
        Self::from(head_tail.head) | Self::from(head_tail.tail)
    }
}

impl From<IndexedFlag> for FacetOptions {
    fn from(_: IndexedFlag) -> Self {
        FacetOptions { stored: false }
    }
}

#[cfg(test)]
mod tests {
    use crate::schema::{FacetOptions, INDEXED};

    #[test]
    fn test_from_index_flag() {
        let facet_option = FacetOptions::from(INDEXED);
        assert_eq!(facet_option, FacetOptions::default());
    }
}
