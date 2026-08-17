//! SQL identifier strings.

use std::borrow::{Borrow, Cow};
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

use identstr::{policy, ArcStorage, IdentStr};

/// Input types accepted by [`Identifier`] constructors (`&str`, `String`,
/// `Cow<str>`, `Arc<str>`).
pub trait IdentifierInput: identstr::Input<ArcStorage> {}
impl<T: identstr::Input<ArcStorage>> IdentifierInput for T {}

pub use identstr::{KeyStr as IdentifierStr, Quote};

type Inner = IdentStr<Quote, policy::Ascii, ArcStorage>;

/// A SQL identifier: the dequoted text plus the quote style it was written
/// with, in 16 bytes.
///
/// Equality, ordering, and hashing fold ASCII letters the way SQLite does,
/// so `Identifier::new("FOO") == Identifier::new("foo")`. Non-ASCII bytes
/// compare exactly. The quote style never affects comparisons.
///
/// `Display` and [`as_str`](Identifier::as_str) return the text exactly as
/// written (original case, no quotes).
///
/// Text up to 16 bytes (15 when a quote style is stored) lives inline;
/// longer text is stored in a shared `Arc<str>`, so cloning never copies
/// the text.
///
/// Maps keyed by `Identifier` can be queried by plain string without
/// allocating: `map.get(IdentifierStr::new("users"))`.
#[derive(Clone, Default)]
pub struct Identifier(Inner);

impl Identifier {
    /// Creates an identifier from already-dequoted text.
    ///
    /// The input is stored as-is: quote characters in it are part of the
    /// name. Use [`parse`](Identifier::parse) for SQL source text that may
    /// carry quote delimiters.
    pub fn new(text: impl IdentifierInput) -> Self {
        Self(Inner::from_unquoted(text))
    }

    /// Creates an identifier from SQL source text.
    ///
    /// When the input is wrapped in `"…"`, `'…'`, `` `…` `` or `[…]`, the
    /// delimiters are stripped, doubled closing delimiters are unescaped,
    /// and the quote style is remembered. Anything else (including
    /// malformed quoting) is stored as plain text.
    pub fn parse(source: impl IdentifierInput) -> Self {
        Self(Inner::new(source))
    }

    /// Creates an identifier from dequoted text with a known quote style.
    pub fn with_quote(text: impl IdentifierInput, quote: Quote) -> Self {
        Self(Inner::with_quote(text, quote))
    }

    pub const fn empty() -> Self {
        Self(Inner::empty())
    }

    /// The identifier text as written, without quote delimiters.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// The quote style the identifier was written with, if any.
    pub fn quote(&self) -> Option<Quote> {
        self.0.quote()
    }

    pub fn is_empty(&self) -> bool {
        self.as_str().is_empty()
    }

    pub fn len(&self) -> usize {
        self.as_str().len()
    }

    /// Like `str::starts_with`, but folds ASCII case the way identifier
    /// comparison does.
    pub fn starts_with_ignore_ascii_case(&self, prefix: &str) -> bool {
        self.as_str()
            .get(..prefix.len())
            .is_some_and(|head| head.eq_ignore_ascii_case(prefix))
    }

    /// Like `str::strip_prefix`, but folds ASCII case the way identifier
    /// comparison does.
    pub fn strip_prefix_ignore_ascii_case(&self, prefix: &str) -> Option<&str> {
        if self.starts_with_ignore_ascii_case(prefix) {
            self.as_str().get(prefix.len()..)
        } else {
            None
        }
    }

    /// Renders the identifier with its preserved quote style, restoring
    /// doubled closing delimiters (`"a""b"` round-trips).
    pub fn to_quoted_string(&self) -> String {
        self.0.to_quoted_string()
    }

    /// Writes the identifier with its preserved quote style.
    pub fn write_quoted(&self, output: &mut (impl fmt::Write + ?Sized)) -> fmt::Result {
        self.0.write_quoted(output)
    }

    /// Returns a `Display` adapter that renders with the preserved quote
    /// style, for use in `format!`/`write!` without allocating.
    pub fn display_quoted(&self) -> impl fmt::Display + '_ {
        self.0.display_quoted()
    }
}

impl fmt::Debug for Identifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("Identifier");
        debug.field("value", &self.as_str());
        debug.field("quote", &self.quote());
        debug.finish()
    }
}

impl fmt::Display for Identifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl PartialEq for Identifier {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for Identifier {}

impl PartialOrd for Identifier {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Identifier {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl Hash for Identifier {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

/// Lets maps keyed by `Identifier` be queried by plain string text without
/// allocating: `map.get(IdentifierStr::new(name))`. Sound because
/// [`IdentifierStr`] folds case during comparison and hashing exactly like
/// `Identifier` itself.
impl Borrow<IdentifierStr> for Identifier {
    fn borrow(&self) -> &IdentifierStr {
        IdentifierStr::new(self.as_str())
    }
}

/// `Deref` gives `Identifier` the full `&str` read API (`len`, `contains`,
/// `chars`, …). Those methods are byte-exact: use the identifier's own
/// comparison traits or the `*_ignore_ascii_case` helpers when a check must
/// fold case.
impl std::ops::Deref for Identifier {
    type Target = str;

    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<str> for Identifier {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl PartialEq<str> for Identifier {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<&str> for Identifier {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

impl PartialEq<String> for Identifier {
    fn eq(&self, other: &String) -> bool {
        self.0 == other.as_str()
    }
}

impl PartialEq<Identifier> for str {
    fn eq(&self, other: &Identifier) -> bool {
        other.0 == self
    }
}

impl PartialEq<Identifier> for &str {
    fn eq(&self, other: &Identifier) -> bool {
        other.0 == *self
    }
}

impl PartialEq<Identifier> for String {
    fn eq(&self, other: &Identifier) -> bool {
        other.0 == self.as_str()
    }
}

impl From<&str> for Identifier {
    fn from(text: &str) -> Self {
        Self::new(text)
    }
}

impl From<String> for Identifier {
    fn from(text: String) -> Self {
        Self::new(text)
    }
}

impl From<Cow<'_, str>> for Identifier {
    fn from(text: Cow<'_, str>) -> Self {
        Self::new(text)
    }
}

impl From<Identifier> for String {
    fn from(identifier: Identifier) -> Self {
        identifier.as_str().to_owned()
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for Identifier {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for Identifier {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        String::deserialize(deserializer).map(Identifier::new)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eq_folds_ascii_case_only() {
        assert_eq!(Identifier::new("Foo"), Identifier::new("foo"));
        assert_eq!(Identifier::new("FOO"), Identifier::new("foo"));
        assert_ne!(Identifier::new("foo"), Identifier::new("bar"));
        // SQLite folds only A-Z/a-z; non-ASCII compares byte-exact.
        assert_ne!(Identifier::new("straße"), Identifier::new("STRASSE"));
        assert_eq!(Identifier::new("café"), Identifier::new("café"));
        assert_ne!(Identifier::new("café"), Identifier::new("CAFÉ"));
    }

    #[test]
    fn new_keeps_quote_characters_in_text() {
        let identifier = Identifier::new("\"Users\"");
        assert_eq!(identifier.as_str(), "\"Users\"");
        assert_eq!(identifier.quote(), None);
    }

    #[test]
    fn parse_strips_quotes_and_remembers_style() {
        let identifier = Identifier::parse("\"User\"\"Table\"");
        assert_eq!(identifier.as_str(), "User\"Table");
        assert_eq!(identifier.quote(), Some(Quote::Double));
        assert_eq!(identifier.to_quoted_string(), "\"User\"\"Table\"");

        let plain = Identifier::parse("Users");
        assert_eq!(plain.quote(), None);

        let bracket = Identifier::parse("[Orders]");
        assert_eq!(bracket.as_str(), "Orders");
        assert_eq!(bracket.quote(), Some(Quote::Bracket));
    }

    #[test]
    fn quote_style_does_not_affect_equality() {
        assert_eq!(Identifier::parse("\"Foo\""), Identifier::new("FOO"));
    }

    #[test]
    fn hash_and_ord_agree_with_eq() {
        use std::collections::hash_map::DefaultHasher;

        let hash = |identifier: &Identifier| {
            let mut hasher = DefaultHasher::new();
            identifier.hash(&mut hasher);
            hasher.finish()
        };

        let a = Identifier::new("MyTable");
        let b = Identifier::new("mytable");
        assert_eq!(a, b);
        assert_eq!(hash(&a), hash(&b));
        assert_eq!(a.cmp(&b), Ordering::Equal);
        assert!(Identifier::new("A") < Identifier::new("b"));
    }

    #[test]
    fn str_comparisons_fold_case() {
        let identifier = Identifier::new("MyTable");
        assert!(identifier == "mytable");
        assert!("MYTABLE" == identifier);
        let owned = String::from("mytable");
        assert!(identifier == owned);
    }

    #[test]
    fn display_preserves_original_case() {
        assert_eq!(Identifier::new("MyTable").to_string(), "MyTable");
    }

    #[test]
    fn map_lookup_by_str_needs_no_allocation() {
        use std::collections::HashMap;

        let mut map = HashMap::new();
        map.insert(Identifier::parse("\"Users\""), 7);
        assert_eq!(map.get(IdentifierStr::new("USERS")), Some(&7));
        assert_eq!(map.get(&Identifier::new("users")), Some(&7));
    }

    #[test]
    fn compact_size() {
        assert_eq!(std::mem::size_of::<Identifier>(), 16);
    }
}
