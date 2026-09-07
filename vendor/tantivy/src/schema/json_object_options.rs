use std::ops::BitOr;

use serde::{Deserialize, Serialize};

use super::text_options::{FastFieldTextOptions, TokenizerName};
use crate::schema::flags::{FastFlag, SchemaFlagList, StoredFlag};
use crate::schema::{TextFieldIndexing, TextOptions};

/// The `JsonObjectOptions` make it possible to
/// configure how a json object field should be indexed and stored.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct JsonObjectOptions {
    stored: bool,
    // If set to some, int, date, f64 and text will be indexed.
    // Text will use the TextFieldIndexing setting for indexing.
    indexing: Option<TextFieldIndexing>,
    // Store all field as fast fields with an optional tokenizer for text.
    fast: FastFieldTextOptions,
    /// tantivy will generate paths to the different nodes of the json object
    /// both in:
    /// - the inverted index (for the terms)
    /// - fast fields (for the column names).
    ///
    /// These json path are encoded by concatenating the list of object keys that
    /// are visited from the root to the leaf.
    ///
    /// By default, if an object key contains a `.`, we keep it as a `.` it as is.
    /// On the search side, users will then have to escape this `.` in the query parser
    /// or when referring to a column name.
    ///
    /// For instance:
    /// `{"root": {"child.with.dot": "hello"}}`
    ///
    /// Can be searched using the following query
    /// `root.child\.with\.dot:hello`
    ///
    /// If `expand_dots_enabled` is set to true, we will treat this `.` in object keys
    /// as json separators. In other words, if set to true, our object will be
    /// processed as if it was
    /// `{"root": {"child": {"with": {"dot": "hello"}}}}`
    /// and it can be search using the following query:
    /// `root.child.with.dot:hello`
    #[serde(default)]
    expand_dots_enabled: bool,
}

impl JsonObjectOptions {
    /// Returns `true` if the json object should be stored.
    #[inline]
    pub fn is_stored(&self) -> bool {
        self.stored
    }

    /// Returns `true` iff the json object should be indexed.
    #[inline]
    pub fn is_indexed(&self) -> bool {
        self.indexing.is_some()
    }

    /// Returns true if and only if the json object fields are
    /// to be treated as fast fields.
    #[inline]
    pub fn is_fast(&self) -> bool {
        matches!(self.fast, FastFieldTextOptions::IsEnabled(true))
            || matches!(
                &self.fast,
                FastFieldTextOptions::EnabledWithTokenizer { with_tokenizer: _ }
            )
    }

    /// Returns true if and only if the value is a fast field.
    #[inline]
    pub fn get_fast_field_tokenizer_name(&self) -> Option<&str> {
        match &self.fast {
            FastFieldTextOptions::IsEnabled(true) | FastFieldTextOptions::IsEnabled(false) => None,
            FastFieldTextOptions::EnabledWithTokenizer {
                with_tokenizer: tokenizer,
            } => Some(tokenizer.name()),
        }
    }

    /// Returns `true` iff dots in json keys should be expanded.
    ///
    /// When expand_dots is enabled, json object like
    /// `{"k8s.node.id": 5}` is processed as if it was
    /// `{"k8s": {"node": {"id": 5}}}`.
    /// This option has the merit of allowing users to
    /// write queries  like `k8s.node.id:5`.
    /// On the other, enabling that feature can lead to
    /// ambiguity.
    ///
    /// If disabled, the "." needs to be escaped:
    /// `k8s\.node\.id:5`.
    #[inline]
    pub fn is_expand_dots_enabled(&self) -> bool {
        self.expand_dots_enabled
    }

    /// Sets `expands_dots` to true.
    /// See `is_expand_dots_enabled` for more information.
    pub fn set_expand_dots_enabled(mut self) -> Self {
        self.expand_dots_enabled = true;
        self
    }

    /// Returns the text indexing options.
    ///
    /// If set to `Some` then both int and str values will be indexed.
    /// The inner `TextFieldIndexing` will however, only apply to the str values
    /// in the json object.
    #[inline]
    pub fn get_text_indexing_options(&self) -> Option<&TextFieldIndexing> {
        self.indexing.as_ref()
    }

    /// Sets the field as stored
    #[must_use]
    pub fn set_stored(mut self) -> Self {
        self.stored = true;
        self
    }

    /// Set the field as a fast field.
    ///
    /// Fast fields are designed for random access.
    /// Access time are similar to a random lookup in an array.
    /// Text fast fields will have the term ids stored in the fast field.
    ///
    /// The effective cardinality depends on the tokenizer. Without a tokenizer, the text will be
    /// stored as is, which equals to the "raw" tokenizer. The tokenizer can be used to apply
    /// normalization like lower case.
    /// The passed tokenizer_name must be available on the fast field tokenizer manager.
    /// `Index::fast_field_tokenizer`.
    ///
    /// The original text can be retrieved via
    /// [`TermDictionary::ord_to_term()`](crate::termdict::TermDictionary::ord_to_term)
    /// from the dictionary.
    #[must_use]
    pub fn set_fast(mut self, tokenizer_name: Option<&str>) -> Self {
        if let Some(tokenizer) = tokenizer_name {
            let tokenizer = TokenizerName::from_name(tokenizer);
            self.fast = FastFieldTextOptions::EnabledWithTokenizer {
                with_tokenizer: tokenizer,
            }
        } else {
            self.fast = FastFieldTextOptions::IsEnabled(true);
        }
        self
    }

    /// Sets the field as indexed, with the specific indexing options.
    #[must_use]
    pub fn set_indexing_options(mut self, indexing: TextFieldIndexing) -> Self {
        self.indexing = Some(indexing);
        self
    }
}

impl From<StoredFlag> for JsonObjectOptions {
    fn from(_stored_flag: StoredFlag) -> Self {
        JsonObjectOptions {
            stored: true,
            indexing: None,
            fast: FastFieldTextOptions::default(),
            expand_dots_enabled: false,
        }
    }
}

impl From<FastFlag> for JsonObjectOptions {
    fn from(_fast_flag: FastFlag) -> Self {
        JsonObjectOptions {
            stored: false,
            indexing: None,
            fast: FastFieldTextOptions::IsEnabled(true),
            expand_dots_enabled: false,
        }
    }
}

impl From<()> for JsonObjectOptions {
    fn from(_: ()) -> Self {
        Self::default()
    }
}

impl<T: Into<JsonObjectOptions>> BitOr<T> for JsonObjectOptions {
    type Output = JsonObjectOptions;

    fn bitor(self, other: T) -> Self {
        let other: JsonObjectOptions = other.into();
        JsonObjectOptions {
            indexing: self.indexing.or(other.indexing),
            stored: self.stored | other.stored,
            fast: self.fast | other.fast,
            expand_dots_enabled: self.expand_dots_enabled | other.expand_dots_enabled,
        }
    }
}

impl<Head, Tail> From<SchemaFlagList<Head, Tail>> for JsonObjectOptions
where
    Head: Clone,
    Tail: Clone,
    Self: BitOr<Output = Self> + From<Head> + From<Tail>,
{
    fn from(head_tail: SchemaFlagList<Head, Tail>) -> Self {
        Self::from(head_tail.head) | Self::from(head_tail.tail)
    }
}

impl From<TextOptions> for JsonObjectOptions {
    fn from(text_options: TextOptions) -> Self {
        JsonObjectOptions {
            stored: text_options.is_stored(),
            indexing: text_options.get_indexing_options().cloned(),
            fast: text_options.fast,
            expand_dots_enabled: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{FAST, STORED, TEXT};

    #[test]
    fn test_json_options() {
        {
            let json_options: JsonObjectOptions = (STORED | TEXT).into();
            assert!(json_options.is_stored());
            assert!(json_options.is_indexed());
            assert!(!json_options.is_fast());
        }
        {
            let json_options: JsonObjectOptions = TEXT.into();
            assert!(!json_options.is_stored());
            assert!(json_options.is_indexed());
            assert!(!json_options.is_fast());
        }
        {
            let json_options: JsonObjectOptions = STORED.into();
            assert!(json_options.is_stored());
            assert!(!json_options.is_indexed());
            assert!(!json_options.is_fast());
        }
        {
            let json_options: JsonObjectOptions = FAST.into();
            assert!(!json_options.is_stored());
            assert!(!json_options.is_indexed());
            assert!(json_options.is_fast());
        }
        {
            let json_options: JsonObjectOptions = (FAST | STORED).into();
            assert!(json_options.is_stored());
            assert!(!json_options.is_indexed());
            assert!(json_options.is_fast());
        }
    }
}
