use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::mem::offset_of;
use std::sync::Arc;

/// A SQL identifier that compares, hashes and orders with ASCII-only case
/// folding, the same way SQLite compares names. The original text is kept
/// for display.
///
/// Layout (16 bytes, Umbra style): the length, the first four bytes, and then
/// either the next eight bytes (identifiers up to 12 bytes live fully on the
/// stack) or a pointer to a shared heap string. Equality of two short
/// identifiers is two folded word compares and never touches the heap.
#[repr(C)]
pub struct Identifier {
    len: u32,
    prefix: [u8; PREFIX_LEN],
    tail: Tail,
}

#[repr(C)]
union Tail {
    inline: [u8; SUFFIX_LEN],
    heap: *const u8,
}

const PREFIX_LEN: usize = 4;
const SUFFIX_LEN: usize = 8;
const INLINE_LEN: usize = PREFIX_LEN + SUFFIX_LEN;

const HIGH_BITS: u64 = 0x8080_8080_8080_8080;
const LOW_SEVEN: u64 = 0x7f7f_7f7f_7f7f_7f7f;
const CASE_BITS: u64 = 0x2020_2020_2020_2020;
const BELOW_UPPER_A: u64 = 0x3f3f_3f3f_3f3f_3f3f;
const BELOW_UPPER_Z_PLUS_ONE: u64 = 0x2525_2525_2525_2525;
const BELOW_LOWER_A: u64 = 0x1f1f_1f1f_1f1f_1f1f;
const BELOW_LOWER_Z_PLUS_ONE: u64 = 0x0505_0505_0505_0505;

/// Lowercase every ASCII byte `A..=Z` in the eight bytes of `word` at once.
/// Bytes outside that range, including non-ASCII bytes, do not change.
#[inline(always)]
const fn fold_word(word: u64) -> u64 {
    let seven_bit = word & LOW_SEVEN;
    let at_least_upper_a = seven_bit.wrapping_add(BELOW_UPPER_A);
    let past_upper_z = seven_bit.wrapping_add(BELOW_UPPER_Z_PLUS_ONE);
    let is_upper = at_least_upper_a & !past_upper_z & !word & HIGH_BITS;
    word | (is_upper >> 2)
}

#[inline(always)]
const fn fold_prefix(prefix: [u8; PREFIX_LEN]) -> u32 {
    fold_word(u32::from_ne_bytes(prefix) as u64) as u32
}

/// Set bit 5 (0x20) in every byte of the result whose byte in `word` is an
/// ASCII letter of either case; all other bits are zero.
#[inline(always)]
const fn letter_case_bits(word: u64) -> u64 {
    let lower = word | CASE_BITS;
    let seven_bit = lower & LOW_SEVEN;
    let at_least_lower_a = seven_bit.wrapping_add(BELOW_LOWER_A);
    let past_lower_z = seven_bit.wrapping_add(BELOW_LOWER_Z_PLUS_ONE);
    (at_least_lower_a & !past_lower_z & !lower & HIGH_BITS) >> 2
}

/// Equality of two words of bytes with ASCII case folding. Two bytes are
/// equal when they are identical, or when they differ only in bit 5 and the
/// byte is a letter. Most mismatches stop at the first test.
#[inline(always)]
const fn words_eq_ignore_ascii_case(lhs: u64, rhs: u64) -> bool {
    let diff = lhs ^ rhs;
    diff & !CASE_BITS == 0 && diff & !letter_case_bits(lhs) == 0
}

#[inline(always)]
fn load_word(bytes: &[u8], at: usize) -> u64 {
    u64::from_ne_bytes(bytes[at..at + SUFFIX_LEN].try_into().expect("8 bytes"))
}

/// Case-insensitive equality of two byte slices of the same length, eight
/// bytes at a time. The last word overlaps the previous one when the length
/// is not a multiple of eight, which is cheaper than a byte loop.
#[inline]
fn eq_ignore_ascii_case_words(lhs: &[u8], rhs: &[u8]) -> bool {
    debug_assert_eq!(lhs.len(), rhs.len());
    let len = lhs.len();
    if len < SUFFIX_LEN {
        return lhs.eq_ignore_ascii_case(rhs);
    }
    let mut at = 0;
    while at + SUFFIX_LEN <= len {
        if !words_eq_ignore_ascii_case(load_word(lhs, at), load_word(rhs, at)) {
            return false;
        }
        at += SUFFIX_LEN;
    }
    let last = len - SUFFIX_LEN;
    words_eq_ignore_ascii_case(load_word(lhs, last), load_word(rhs, last))
}

// SAFETY: the heap variant points into an `Arc<str>`, which is `Send` and
// `Sync`; the inline variant is plain bytes.
unsafe impl Send for Identifier {}
unsafe impl Sync for Identifier {}

impl Identifier {
    pub fn new(s: String) -> Self {
        Self::from(s.as_str())
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn as_str(&self) -> &str {
        let len = self.len();
        let ptr = if len <= INLINE_LEN {
            // SAFETY: `prefix` and `tail.inline` are adjacent in the repr(C)
            // layout, so the first 12 bytes after `len` are one contiguous
            // buffer. The pointer is derived from the whole struct so it is
            // allowed to cover both fields.
            unsafe {
                (self as *const Self)
                    .cast::<u8>()
                    .add(offset_of!(Self, prefix))
            }
        } else {
            // SAFETY: `len > INLINE_LEN` means `tail` holds the heap pointer.
            unsafe { self.tail.heap }
        };
        // SAFETY: both buffers hold `len` bytes copied from a `str`.
        unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(ptr, len)) }
    }

    pub fn into_inner(self) -> String {
        self.as_str().to_string()
    }

    #[inline]
    fn is_inline(&self) -> bool {
        self.len() <= INLINE_LEN
    }

    #[inline]
    fn inline_word(&self) -> u64 {
        debug_assert!(self.is_inline());
        // SAFETY: the inline variant is active, and it is plain bytes anyway.
        u64::from_ne_bytes(unsafe { self.tail.inline })
    }

    #[inline]
    fn inline_word_be(&self) -> u64 {
        debug_assert!(self.is_inline());
        // SAFETY: the inline variant is active, and it is plain bytes anyway.
        u64::from_be_bytes(unsafe { self.tail.inline })
    }

    #[inline]
    fn folded_head(&self) -> u64 {
        u64::from(self.len) | (u64::from(fold_prefix(self.prefix)) << 32)
    }

    /// Bytes after the prefix. Only the heap variant needs this.
    #[inline]
    fn rest(&self) -> &[u8] {
        &self.as_str().as_bytes()[PREFIX_LEN.min(self.len())..]
    }

    fn heap_ptr(&self) -> *const str {
        debug_assert!(!self.is_inline());
        // SAFETY: `len > INLINE_LEN` means `tail` holds the heap pointer.
        let data = unsafe { self.tail.heap };
        std::ptr::slice_from_raw_parts(data, self.len()) as *const str
    }

    fn inline(bytes: &[u8]) -> Self {
        debug_assert!(bytes.len() <= INLINE_LEN);
        let mut buf = [0u8; INLINE_LEN];
        buf[..bytes.len()].copy_from_slice(bytes);
        let mut prefix = [0u8; PREFIX_LEN];
        prefix.copy_from_slice(&buf[..PREFIX_LEN]);
        let mut inline = [0u8; SUFFIX_LEN];
        inline.copy_from_slice(&buf[PREFIX_LEN..]);
        Self {
            len: bytes.len() as u32,
            prefix,
            tail: Tail { inline },
        }
    }
}

impl From<&str> for Identifier {
    fn from(s: &str) -> Self {
        let bytes = s.as_bytes();
        if bytes.len() <= INLINE_LEN {
            return Self::inline(bytes);
        }
        let len = u32::try_from(bytes.len()).expect("identifier longer than u32::MAX bytes");
        let mut prefix = [0u8; PREFIX_LEN];
        prefix.copy_from_slice(&bytes[..PREFIX_LEN]);
        let heap = Arc::into_raw(Arc::<str>::from(s)).cast::<u8>();
        Self {
            len,
            prefix,
            tail: Tail { heap },
        }
    }
}

impl From<String> for Identifier {
    fn from(s: String) -> Self {
        Self::from(s.as_str())
    }
}

impl From<&String> for Identifier {
    fn from(s: &String) -> Self {
        Self::from(s.as_str())
    }
}

impl Clone for Identifier {
    #[inline]
    fn clone(&self) -> Self {
        if !self.is_inline() {
            // SAFETY: the pointer came from `Arc::into_raw` in `From<&str>`
            // and the count is decremented exactly once per clone in `drop`.
            unsafe { Arc::increment_strong_count(self.heap_ptr()) };
        }
        Self {
            len: self.len,
            prefix: self.prefix,
            // SAFETY: `Tail` is `Copy`-like plain data; the heap variant's
            // reference count was incremented above.
            tail: unsafe { std::ptr::read(&self.tail) },
        }
    }
}

impl Drop for Identifier {
    #[inline]
    fn drop(&mut self) {
        if !self.is_inline() {
            // SAFETY: the pointer came from `Arc::into_raw` and every clone
            // holds its own strong count.
            unsafe { drop(Arc::from_raw(self.heap_ptr())) };
        }
    }
}

impl Default for Identifier {
    fn default() -> Self {
        Self::inline(&[])
    }
}

impl PartialEq for Identifier {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        if self.len != other.len {
            return false;
        }
        let lhs = u64::from(u32::from_ne_bytes(self.prefix));
        let rhs = u64::from(u32::from_ne_bytes(other.prefix));
        if !words_eq_ignore_ascii_case(lhs, rhs) {
            return false;
        }
        if self.is_inline() {
            words_eq_ignore_ascii_case(self.inline_word(), other.inline_word())
        } else {
            eq_ignore_ascii_case_words(self.rest(), other.rest())
        }
    }
}

impl Eq for Identifier {}

impl Hash for Identifier {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.folded_head());
        if self.is_inline() {
            state.write_u64(fold_word(self.inline_word()));
            return;
        }
        let mut chunks = self.rest().chunks_exact(SUFFIX_LEN);
        for chunk in &mut chunks {
            let word = u64::from_ne_bytes(chunk.try_into().expect("chunk is 8 bytes"));
            state.write_u64(fold_word(word));
        }
        let remainder = chunks.remainder();
        if !remainder.is_empty() {
            let mut buf = [0u8; SUFFIX_LEN];
            buf[..remainder.len()].copy_from_slice(remainder);
            state.write_u64(fold_word(u64::from_ne_bytes(buf)));
        }
    }
}

impl PartialOrd for Identifier {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Identifier {
    fn cmp(&self, other: &Self) -> Ordering {
        let lhs = fold_word(u64::from(u32::from_be_bytes(self.prefix)));
        let rhs = fold_word(u64::from(u32::from_be_bytes(other.prefix)));
        if lhs != rhs {
            return lhs.cmp(&rhs);
        }
        if self.is_inline() && other.is_inline() {
            let lhs = fold_word(self.inline_word_be());
            let rhs = fold_word(other.inline_word_be());
            return lhs.cmp(&rhs).then(self.len.cmp(&other.len));
        }
        let lhs = self.rest();
        let rhs = other.rest();
        for (a, b) in lhs.iter().zip(rhs) {
            let ordering = a.to_ascii_lowercase().cmp(&b.to_ascii_lowercase());
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        lhs.len().cmp(&rhs.len())
    }
}

impl PartialEq<str> for Identifier {
    #[inline]
    fn eq(&self, other: &str) -> bool {
        if self.len() != other.len() {
            return false;
        }
        if self.is_inline() {
            *self == Identifier::inline(other.as_bytes())
        } else {
            eq_ignore_ascii_case_words(self.as_str().as_bytes(), other.as_bytes())
        }
    }
}

impl PartialEq<&str> for Identifier {
    #[inline]
    fn eq(&self, other: &&str) -> bool {
        *self == **other
    }
}

impl PartialEq<String> for Identifier {
    #[inline]
    fn eq(&self, other: &String) -> bool {
        *self == **other
    }
}

impl PartialEq<Identifier> for str {
    #[inline]
    fn eq(&self, other: &Identifier) -> bool {
        *other == *self
    }
}

impl PartialEq<Identifier> for &str {
    #[inline]
    fn eq(&self, other: &Identifier) -> bool {
        *other == **self
    }
}

impl PartialEq<Identifier> for String {
    #[inline]
    fn eq(&self, other: &Identifier) -> bool {
        *other == **self
    }
}

impl std::fmt::Display for Identifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::fmt::Debug for Identifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self.as_str(), f)
    }
}

impl AsRef<str> for Identifier {
    #[inline]
    fn as_ref(&self) -> &str {
        self.as_str()
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
        struct IdentifierVisitor;
        impl serde::de::Visitor<'_> for IdentifierVisitor {
            type Value = Identifier;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a string")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Identifier::from(v))
            }
        }
        deserializer.deserialize_str(IdentifierVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;

    fn hash_of(id: &Identifier) -> u64 {
        let mut h = DefaultHasher::new();
        id.hash(&mut h);
        h.finish()
    }

    const SAMPLES: &[&str] = &[
        "",
        "a",
        "A",
        "id",
        "Id",
        "rowid",
        "ROWID",
        "created_at",
        "twelve_bytes",
        "TWELVE_BYTES",
        "thirteen_byte",
        "THIRTEEN_BYTE",
        "products_category",
        "sqlite_autoindex_users_1",
        "SQLITE_AUTOINDEX_USERS_1",
        "sqlite_autoindex_users_2",
        "sqlite_autoindex_Users_1",
        "sqlite_autoindex_users_10",
        "sqlite_autoindex_users_1_",
        "xqlite_autoindex_users_1",
        "sqlite_autoinde",
        "sqlite_autoindeX",
        "sqlite_autoindex_",
        "café",
        "CAFÉ",
        "straße",
        "a\0b",
        "ab",
        "abc",
        "[weird]",
        "{brace}",
        "@at",
        "_under",
        "`tick`",
    ];

    fn reference_eq(a: &str, b: &str) -> bool {
        a.eq_ignore_ascii_case(b)
    }

    fn reference_cmp(a: &str, b: &str) -> Ordering {
        let a: Vec<u8> = a.bytes().map(|b| b.to_ascii_lowercase()).collect();
        let b: Vec<u8> = b.bytes().map(|b| b.to_ascii_lowercase()).collect();
        a.cmp(&b)
    }

    #[test]
    fn fold_word_lowercases_only_ascii_upper() {
        for b in 0..=255u8 {
            let word = u64::from_ne_bytes([b; 8]);
            let expected = u64::from_ne_bytes([b.to_ascii_lowercase(); 8]);
            assert_eq!(fold_word(word), expected, "byte {b:#x}");
        }
    }

    #[test]
    fn words_eq_ignore_ascii_case_matches_the_byte_reference() {
        for a in 0..=255u8 {
            for b in 0..=255u8 {
                let lhs = u64::from_ne_bytes([a; 8]);
                let rhs = u64::from_ne_bytes([b; 8]);
                let expected = a.eq_ignore_ascii_case(&b);
                assert_eq!(
                    words_eq_ignore_ascii_case(lhs, rhs),
                    expected,
                    "{a:#x} vs {b:#x}"
                );
                assert_eq!(
                    fold_word(lhs) == fold_word(rhs),
                    expected,
                    "fold {a:#x} vs {b:#x}"
                );
            }
        }
    }

    #[test]
    fn round_trips_text_and_length() {
        for &s in SAMPLES {
            let id = Identifier::from(s);
            assert_eq!(id.as_str(), s);
            assert_eq!(id.len(), s.len());
            assert_eq!(id.to_string(), s);
            assert_eq!(format!("{id:?}"), format!("{s:?}"));
            assert_eq!(Identifier::new(s.to_string()).as_str(), s);
            assert_eq!(id.clone().into_inner(), s);
        }
    }

    #[test]
    fn eq_hash_and_ord_match_the_byte_wise_reference() {
        for &a in SAMPLES {
            for &b in SAMPLES {
                let ia = Identifier::from(a);
                let ib = Identifier::from(b);
                assert_eq!(ia == ib, reference_eq(a, b), "{a:?} == {b:?}");
                assert_eq!(ia == b, reference_eq(a, b), "{a:?} == str {b:?}");
                assert_eq!(b == ia, reference_eq(a, b), "str {b:?} == {a:?}");
                assert_eq!(ia == b.to_string(), reference_eq(a, b));
                assert_eq!(ia.cmp(&ib), reference_cmp(a, b), "{a:?} cmp {b:?}");
                if ia == ib {
                    assert_eq!(hash_of(&ia), hash_of(&ib), "{a:?} hash {b:?}");
                }
            }
        }
    }

    #[test]
    fn clone_shares_long_strings_and_drops_cleanly() {
        let id = Identifier::from("a_rather_long_identifier_name");
        let copies: Vec<Identifier> = (0..8).map(|_| id.clone()).collect();
        let arc = unsafe {
            Arc::increment_strong_count(id.heap_ptr());
            Arc::from_raw(id.heap_ptr())
        };
        assert_eq!(Arc::strong_count(&arc), 1 + 8 + 1);
        drop(copies);
        assert_eq!(Arc::strong_count(&arc), 2);
        assert_eq!(id.as_str(), "a_rather_long_identifier_name");
        drop(id);
        assert_eq!(Arc::strong_count(&arc), 1);
        assert_eq!(&*arc, "a_rather_long_identifier_name");
    }

    #[test]
    fn short_identifiers_are_inline_and_sixteen_bytes() {
        assert_eq!(std::mem::size_of::<Identifier>(), 16);
        assert!(Identifier::from("twelve_bytes").is_inline());
        assert!(!Identifier::from("thirteen_byte").is_inline());
    }

    #[test]
    fn works_as_a_hash_map_key() {
        let mut map = std::collections::HashMap::new();
        map.insert(Identifier::from("MyTable"), 1);
        map.insert(Identifier::from("a_much_longer_table_name"), 2);
        assert_eq!(map.get(&Identifier::from("mytable")), Some(&1));
        assert_eq!(
            map.get(&Identifier::from("A_MUCH_LONGER_TABLE_NAME")),
            Some(&2)
        );
        assert_eq!(map.get(&Identifier::from("other")), None);
    }

    #[test]
    fn ord_is_consistent_with_eq_in_a_btree_set() {
        let mut set = std::collections::BTreeSet::new();
        for &s in SAMPLES {
            set.insert(Identifier::from(s));
        }
        let distinct: std::collections::HashSet<String> =
            SAMPLES.iter().map(|s| s.to_ascii_lowercase()).collect();
        assert_eq!(set.len(), distinct.len());
    }
}
