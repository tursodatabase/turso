/// A SQL identifier with ASCII-only case-insensitive equality, ordering, and hashing.
/// Stores the original-case string; comparisons fold only A-Z to a-z.
#[derive(Clone, Debug)]
pub struct Identifier(String);

impl Identifier {
    pub fn new(s: String) -> Self {
        Self(s)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_inner(self) -> String {
        self.0
    }
}

impl PartialEq for Identifier {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq_ignore_ascii_case(&other.0)
    }
}

impl Eq for Identifier {}

impl std::hash::Hash for Identifier {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        for byte in self.0.bytes() {
            state.write_u8(byte.to_ascii_lowercase());
        }
        state.write_usize(self.0.len());
    }
}

impl PartialOrd for Identifier {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Identifier {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let a = self.0.as_bytes();
        let b = other.0.as_bytes();
        for (x, y) in a.iter().zip(b.iter()) {
            match x.to_ascii_lowercase().cmp(&y.to_ascii_lowercase()) {
                std::cmp::Ordering::Equal => continue,
                ord => return ord,
            }
        }
        a.len().cmp(&b.len())
    }
}

impl std::fmt::Display for Identifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for Identifier {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for Identifier {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

impl AsRef<str> for Identifier {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl PartialEq<str> for Identifier {
    fn eq(&self, other: &str) -> bool {
        self.0.eq_ignore_ascii_case(other)
    }
}

impl PartialEq<Identifier> for str {
    fn eq(&self, other: &Identifier) -> bool {
        self.eq_ignore_ascii_case(&other.0)
    }
}

impl PartialEq<&str> for Identifier {
    fn eq(&self, other: &&str) -> bool {
        self.0.eq_ignore_ascii_case(other)
    }
}

impl PartialEq<Identifier> for &str {
    fn eq(&self, other: &Identifier) -> bool {
        self.eq_ignore_ascii_case(&other.0)
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for Identifier {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for Identifier {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        String::deserialize(deserializer).map(Identifier)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ascii_case_insensitive_eq() {
        assert_eq!(Identifier::from("Foo"), Identifier::from("foo"));
        assert_eq!(Identifier::from("FOO"), Identifier::from("foo"));
        assert_ne!(Identifier::from("foo"), Identifier::from("bar"));
    }

    #[test]
    fn non_ascii_case_sensitive() {
        // SQLite only folds A-Z/a-z. Non-ASCII must be byte-exact.
        assert_ne!(Identifier::from("straße"), Identifier::from("STRASSE"));
        assert_eq!(Identifier::from("café"), Identifier::from("café"));
        assert_ne!(Identifier::from("café"), Identifier::from("CAFÉ"));
    }

    #[test]
    fn hash_consistent_with_eq() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let hash = |id: &Identifier| {
            let mut h = DefaultHasher::new();
            id.hash(&mut h);
            h.finish()
        };

        let a = Identifier::from("MyTable");
        let b = Identifier::from("mytable");
        assert_eq!(a, b);
        assert_eq!(hash(&a), hash(&b));
    }

    #[test]
    fn ord_consistent_with_eq() {
        use std::cmp::Ordering;
        let a = Identifier::from("Abc");
        let b = Identifier::from("abc");
        assert_eq!(a.cmp(&b), Ordering::Equal);

        let id_a = Identifier::from("a");
        let id_a_upper = Identifier::from("A");
        let id_b = Identifier::from("b");
        assert!(id_a < id_b);
        assert!(id_a_upper < id_b);
    }

    #[test]
    fn partial_eq_str() {
        let id = Identifier::from("MyTable");
        assert!(id == "mytable");
        assert!("MYTABLE" == id);
    }

    #[test]
    fn display_preserves_original_case() {
        let id = Identifier::from("MyTable");
        assert_eq!(id.to_string(), "MyTable");
    }
}
