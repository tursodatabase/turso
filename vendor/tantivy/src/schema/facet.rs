use std::borrow::{Borrow, Cow};
use std::fmt::{self, Debug, Display, Formatter};
use std::io::{self, Read, Write};
use std::str;
use std::string::FromUtf8Error;

use common::*;
use once_cell::sync::Lazy;
use regex::Regex;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

const SLASH_BYTE: u8 = b'/';
const ESCAPE_BYTE: u8 = b'\\';

/// BYTE used as a level separation in the binary
/// representation of facets.
pub const FACET_SEP_BYTE: u8 = 0u8;

/// `char` used as a level separation in the binary
/// representation of facets. (It is the null codepoint.)
pub const FACET_SEP_CHAR: char = '\u{0}';

/// An error enum for facet parser.
#[derive(Debug, PartialEq, Eq, Error)]
pub enum FacetParseError {
    /// The facet text representation is unparsable.
    #[error("Failed to parse the facet string: '{0}'")]
    FacetParseError(String),
}

/// A Facet represent a point in a given hierarchy.
///
/// They are typically represented similarly to a filepath.
/// For instance, an e-commerce website could
/// have a `Facet` for `/electronics/tv_and_video/led_tv`.
///
/// A document can be associated with any number of facets.
/// The hierarchy implicitly imply that a document
/// belonging to a facet also belongs to the ancestor of
/// its facet. In the example above, `/electronics/tv_and_video/`
/// and `/electronics`.
#[derive(Clone, Default, Eq, Hash, PartialEq, Ord, PartialOrd)]
pub struct Facet(pub(crate) String);

impl Facet {
    /// Returns a new instance of the "root facet"
    /// Equivalent to `/`.
    pub fn root() -> Facet {
        Facet("".to_string())
    }

    /// Returns true if the facet is the root facet `/`.
    pub fn is_root(&self) -> bool {
        self.encoded_str().is_empty()
    }

    /// Returns a binary representation of the facet.
    ///
    /// In this representation, `0u8` is used as a separator
    /// and the string parts of the facet are unescaped.
    /// (The first `/` is not encoded at all).
    ///
    /// This representation has the benefit of making it possible to
    /// express "being a child of a given facet" as a range over
    /// the term ordinals.
    pub fn encoded_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn from_encoded_string(facet_string: String) -> Facet {
        Facet(facet_string)
    }

    /// Creates a `Facet` from its binary representation.
    pub fn from_encoded(encoded_bytes: Vec<u8>) -> Result<Facet, FromUtf8Error> {
        // facet bytes validation. `0u8` is used a separator but that is still legal utf-8
        String::from_utf8(encoded_bytes).map(Facet)
    }

    /// Parse a text representation of a facet.
    ///
    /// If one of the segments of this path
    /// contains a `/`, it should be escaped
    /// using an anti-slash `\`.
    pub fn from_text<T>(path: &T) -> Result<Facet, FacetParseError>
    where T: ?Sized + AsRef<str> {
        #[derive(Copy, Clone)]
        enum State {
            Escaped,
            Idle,
        }
        let path_ref = path.as_ref();
        if path_ref.is_empty() {
            return Err(FacetParseError::FacetParseError(path_ref.to_string()));
        }
        if !path_ref.starts_with('/') {
            return Err(FacetParseError::FacetParseError(path_ref.to_string()));
        }
        let mut facet_encoded = String::new();
        let mut state = State::Idle;
        let path_bytes = path_ref.as_bytes();
        let mut last_offset = 1;
        for i in 1..path_bytes.len() {
            let c = path_bytes[i];
            match (state, c) {
                (State::Idle, ESCAPE_BYTE) => {
                    facet_encoded.push_str(&path_ref[last_offset..i]);
                    last_offset = i + 1;
                    state = State::Escaped
                }
                (State::Idle, SLASH_BYTE) => {
                    facet_encoded.push_str(&path_ref[last_offset..i]);
                    facet_encoded.push(FACET_SEP_CHAR);
                    last_offset = i + 1;
                }
                (State::Escaped, _escaped_char) => {
                    state = State::Idle;
                }
                (State::Idle, _any_char) => {}
            }
        }
        facet_encoded.push_str(&path_ref[last_offset..]);
        Ok(Facet(facet_encoded))
    }

    /// Returns a `Facet` from an iterator over the different
    /// steps of the facet path.
    ///
    /// The steps are expected to be unescaped.
    pub fn from_path<Path>(path: Path) -> Facet
    where
        Path: IntoIterator,
        Path::Item: AsRef<str>,
    {
        let mut facet_string: String = String::with_capacity(100);
        let mut step_it = path.into_iter();
        if let Some(step) = step_it.next() {
            facet_string.push_str(step.as_ref());
        }
        for step in step_it {
            facet_string.push(FACET_SEP_CHAR);
            facet_string.push_str(step.as_ref());
        }
        Facet(facet_string)
    }

    /// Returns `true` if other is a `strict` subfacet of `self`.
    ///
    /// Disclaimer: By strict we mean that the relation is not reflexive.
    /// `/happy` is not a prefix of `/happy`.
    pub fn is_prefix_of(&self, other: &Facet) -> bool {
        let self_str = self.encoded_str();
        let other_str = other.encoded_str();

        // Fast path, but also required to ensure that / is not a prefix of /.
        if other_str.len() <= self_str.len() {
            return false;
        }

        // Root is a prefix of every other path.
        // This is not just an optimisation. It is necessary for correctness.
        if self.is_root() {
            return true;
        }

        other_str.starts_with(self_str) && other_str.as_bytes()[self_str.len()] == FACET_SEP_BYTE
    }

    /// Extract path from the `Facet`.
    pub fn to_path(&self) -> Vec<&str> {
        self.encoded_str().split(FACET_SEP_CHAR).collect()
    }

    /// This function is the inverse of Facet::from(&str).
    pub fn to_path_string(&self) -> String {
        format!("{self}")
    }
}

impl Borrow<str> for Facet {
    fn borrow(&self) -> &str {
        self.encoded_str()
    }
}

impl<'a, T: ?Sized + AsRef<str>> From<&'a T> for Facet {
    fn from(path_asref: &'a T) -> Facet {
        Facet::from_text(path_asref).unwrap()
    }
}

impl BinarySerializable for Facet {
    fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
        <String as BinarySerializable>::serialize(&self.0, writer)
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        Ok(Facet(<String as BinarySerializable>::deserialize(reader)?))
    }
}

impl Display for Facet {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        for step in self.0.split(FACET_SEP_CHAR) {
            write!(f, "/")?;
            write!(f, "{}", escape_slashes(step))?;
        }
        Ok(())
    }
}

fn escape_slashes(s: &str) -> Cow<'_, str> {
    static SLASH_PTN: Lazy<Regex> = Lazy::new(|| Regex::new(r"[\\/]").unwrap());
    SLASH_PTN.replace_all(s, "\\/")
}

impl Serialize for Facet {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Facet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        <Cow<'de, str> as Deserialize<'de>>::deserialize(deserializer).and_then(|path| {
            Facet::from_text(&*path).map_err(|err| D::Error::custom(err.to_string()))
        })
    }
}

impl Debug for Facet {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Facet({self})")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::{Facet, FacetParseError};

    #[test]
    fn test_root() {
        assert_eq!(Facet::root(), Facet::from("/"));
        assert_eq!(format!("{}", Facet::root()), "/");
        assert!(Facet::root().is_root());
        assert_eq!(Facet::root().encoded_str(), "");
    }

    #[test]
    fn test_from_path() {
        assert_eq!(
            Facet::from_path(vec!["top", "a", "firstdoc"]),
            Facet::from("/top/a/firstdoc")
        );
    }

    #[test]
    fn test_facet_display() {
        {
            let v = ["first", "second", "third"];
            let facet = Facet::from_path(v.iter());
            assert_eq!(format!("{facet}"), "/first/second/third");
        }
        {
            let v = ["first", "sec/ond", "third"];
            let facet = Facet::from_path(v.iter());
            assert_eq!(format!("{facet}"), "/first/sec\\/ond/third");
        }
    }

    #[test]
    fn test_facet_debug() {
        let v = ["first", "second", "third"];
        let facet = Facet::from_path(v.iter());
        assert_eq!(format!("{facet:?}"), "Facet(/first/second/third)");
    }

    #[test]
    fn test_to_path() {
        let v = ["first", "second", "third\\/not_fourth"];
        let facet = Facet::from_path(v.iter());
        assert_eq!(facet.to_path(), v);
    }

    #[test]
    fn test_to_path_string() {
        let v = ["first", "second", "third/not_fourth"];
        let facet = Facet::from_path(v.iter());
        assert_eq!(
            facet.to_path_string(),
            String::from("/first/second/third\\/not_fourth")
        );
    }

    #[test]
    fn test_to_path_string_empty() {
        let v: Vec<&str> = vec![];
        let facet = Facet::from_path(v.iter());
        assert_eq!(facet.to_path_string(), "/");
    }

    #[test]
    fn test_from_text() {
        assert_eq!(
            Err(FacetParseError::FacetParseError("INVALID".to_string())),
            Facet::from_text("INVALID")
        );
    }

    #[test]
    fn only_proper_prefixes() {
        assert!(Facet::from("/foo").is_prefix_of(&Facet::from("/foo/bar")));

        assert!(!Facet::from("/foo/bar").is_prefix_of(&Facet::from("/foo/bar")));
    }

    #[test]
    fn root_is_a_prefix() {
        assert!(Facet::from("/").is_prefix_of(&Facet::from("/foobar")));
        assert!(!Facet::from("/").is_prefix_of(&Facet::from("/")));
    }

    #[test]
    fn deserialize_from_borrowed_string() {
        let facet = serde_json::from_str::<Facet>(r#""/foo/bar""#).unwrap();
        assert_eq!(facet, Facet::from_path(["foo", "bar"]));
    }

    #[test]
    fn deserialize_from_owned_string() {
        let facet = serde_json::from_str::<Facet>(r#""/foo/\u263A""#).unwrap();
        assert_eq!(facet, Facet::from_path(["foo", "☺"]));
    }

    #[test]
    fn deserialize_from_invalid_string() {
        let error = serde_json::from_str::<Facet>(r#""foo/bar""#).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Failed to parse the facet string: 'foo/bar'"
        );
    }
}
