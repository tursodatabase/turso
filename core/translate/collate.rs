use std::{
    cmp::Ordering,
    collections::HashMap,
    hash::{Hash, Hasher},
    str::FromStr as _,
};

use icu_collator::{options::CollatorOptions, Collator, CollatorBorrowed};
use icu_locale::Locale;

use crate::{
    sync::{LazyLock, Mutex, RwLock},
    Result,
};

/// **Pre defined collation sequences**\
/// Collating functions only matter when comparing string values.
/// Numeric values are always compared numerically, and BLOBs are always compared byte-by-byte using memcmp().
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub enum CollationSeq {
    Unset,
    Binary,
    NoCase,
    Rtrim,
    Locale(LocaleCollationId),
    /// Name/id token for a connection-owned callback. The comparison itself
    /// must be resolved through `Connection` at runtime.
    Custom(u32),
}

#[derive(Default)]
struct CustomCollationNames {
    // Custom collation callbacks are connection-local. This process-wide table
    // only interns names so bytecode can carry compact `CollationSeq::Custom`
    // tokens and resolve the callback through the active connection at runtime.
    by_name: HashMap<String, u32>,
    by_id: HashMap<u32, String>,
}

static CUSTOM_COLLATION_NAMES: LazyLock<Mutex<CustomCollationNames>> =
    LazyLock::new(|| Mutex::new(CustomCollationNames::default()));

impl CollationSeq {
    pub fn new(collation: &str) -> crate::Result<Self> {
        match crate::util::normalize_ident(collation).as_str() {
            "binary" => return Ok(Self::Binary),
            "nocase" => return Ok(Self::NoCase),
            "rtrim" => return Ok(Self::Rtrim),
            _ => {}
        }

        LocaleCollationRegistry::global()
            .get_or_register(collation)
            .map(Self::Locale)
    }

    #[inline]
    /// Returns the collation, defaulting to BINARY if unset
    pub const fn from_bits(bits: u8) -> Self {
        match bits {
            2 => Self::NoCase,
            3 => Self::Rtrim,
            _ => Self::Binary,
        }
    }

    #[inline]
    pub const fn to_bits(self) -> u16 {
        match self {
            Self::Unset => 0,
            Self::Binary => 1,
            Self::NoCase => 2,
            Self::Rtrim => 3,
            Self::Locale(id) => id.to_bits(),
            Self::Custom(_) => 0,
        }
    }

    #[inline]
    pub const fn from_storage_bits(bits: u16) -> Self {
        match bits {
            0 => Self::Unset,
            1 => Self::Binary,
            2 => Self::NoCase,
            3 => Self::Rtrim,
            bits => Self::Locale(LocaleCollationId::from_bits(bits)),
        }
    }

    #[inline]
    pub const fn id(self) -> u32 {
        match self {
            Self::Custom(id) => id,
            _ => self.to_bits() as u32,
        }
    }

    #[inline]
    pub const fn is_custom(self) -> bool {
        matches!(self, Self::Custom(_))
    }

    pub fn custom(collation: &str) -> Self {
        let normalized = crate::util::normalize_ident(collation);
        let mut registry = CUSTOM_COLLATION_NAMES.lock();
        if let Some(id) = registry.by_name.get(&normalized) {
            return Self::Custom(*id);
        }

        let mut id = custom_collation_id(&normalized);
        while id <= 3 || registry.by_id.contains_key(&id) {
            id = id.wrapping_add(1).max(4);
        }

        registry.by_name.insert(normalized, id);
        registry.by_id.insert(id, collation.to_string());
        Self::Custom(id)
    }

    pub(crate) fn known_custom(collation: &str) -> Option<Self> {
        let normalized = crate::util::normalize_ident(collation);
        CUSTOM_COLLATION_NAMES
            .lock()
            .by_name
            .get(&normalized)
            .copied()
            .map(Self::Custom)
    }

    pub fn name(self) -> String {
        match self {
            Self::Unset => "Unset".to_string(),
            Self::Binary => "Binary".to_string(),
            Self::NoCase => "NoCase".to_string(),
            Self::Rtrim => "RTrim".to_string(),
            Self::Locale(id) => LocaleCollationRegistry::global().name(id),
            Self::Custom(id) => CUSTOM_COLLATION_NAMES
                .lock()
                .by_id
                .get(&id)
                .cloned()
                .unwrap_or_else(|| format!("collation_{id}")),
        }
    }

    #[inline(always)]
    pub fn compare_strings(&self, lhs: &str, rhs: &str) -> Ordering {
        match *self {
            Self::Unset | Self::Binary => Self::binary_cmp(lhs, rhs),
            Self::NoCase => Self::nocase_cmp(lhs, rhs),
            Self::Rtrim => Self::rtrim_cmp(lhs, rhs),
            Self::Locale(id) => LocaleCollationRegistry::global().compare(id, lhs, rhs),
            // Immutable comparison paths have no connection to fetch the external
            // callback from. Runtime VDBE paths dispatch custom collations via
            // `Connection`; schema/index paths reject them before storage.
            Self::Custom(_) => Self::binary_cmp(lhs, rhs),
        }
    }

    #[inline(always)]
    fn binary_cmp(lhs: &str, rhs: &str) -> Ordering {
        lhs.cmp(rhs)
    }

    #[inline(always)]
    fn nocase_cmp(lhs: &str, rhs: &str) -> Ordering {
        for (left, right) in lhs.bytes().zip(rhs.bytes()) {
            let left = left.to_ascii_lowercase();
            let right = right.to_ascii_lowercase();
            if left != right {
                return left.cmp(&right);
            }
            if left == 0 {
                return lhs.len().cmp(&rhs.len());
            }
        }
        lhs.len().cmp(&rhs.len())
    }

    #[inline(always)]
    fn rtrim_cmp(lhs: &str, rhs: &str) -> Ordering {
        lhs.trim_end_matches(' ').cmp(rhs.trim_end_matches(' '))
    }

    pub fn hash_key(&self, text: &str) -> Vec<u8> {
        match self {
            Self::Unset | Self::Binary => text.as_bytes().to_vec(),
            Self::NoCase => text.bytes().map(|b| b.to_ascii_lowercase()).collect(),
            Self::Rtrim => text.trim_end_matches(' ').as_bytes().to_vec(),
            Self::Locale(id) => LocaleCollationRegistry::global().sort_key(*id, text),
            // Hash joins using custom collations are disabled during planning
            // because the callback is connection-owned and may define arbitrary equality.
            Self::Custom(_) => text.as_bytes().to_vec(),
        }
    }
}

impl Default for CollationSeq {
    fn default() -> Self {
        Self::Binary
    }
}

impl std::fmt::Display for CollationSeq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.name())
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub struct LocaleCollationId(u16);

impl LocaleCollationId {
    const FIRST_STORAGE_BIT: u16 = 4;

    fn from_index(index: usize) -> Result<Self> {
        if index > (u16::MAX - Self::FIRST_STORAGE_BIT) as usize {
            return Err(crate::LimboError::ParseError(
                "too many locale collation sequences".to_string(),
            ));
        }
        Ok(Self(index as u16))
    }

    const fn from_bits(bits: u16) -> Self {
        Self(bits - Self::FIRST_STORAGE_BIT)
    }

    const fn to_bits(self) -> u16 {
        self.0 + Self::FIRST_STORAGE_BIT
    }
}

struct LocaleCollation {
    name: String,
    collator: CollatorBorrowed<'static>,
}

struct LocaleCollationRegistry {
    collations: RwLock<Vec<LocaleCollation>>,
}

impl LocaleCollationRegistry {
    fn global() -> &'static Self {
        static REGISTRY: LazyLock<LocaleCollationRegistry> =
            LazyLock::new(|| LocaleCollationRegistry {
                collations: RwLock::new(Vec::new()),
            });
        &REGISTRY
    }

    fn get_or_register(&self, name: &str) -> Result<LocaleCollationId> {
        if let Some(id) = self.find(name) {
            return Ok(id);
        }

        let locale = Locale::from_str(name).map_err(|_| {
            crate::LimboError::ParseError(format!("no such collation sequence: {name}"))
        })?;
        let collator =
            Collator::try_new(locale.into(), CollatorOptions::default()).map_err(|_| {
                crate::LimboError::ParseError(format!("no such collation sequence: {name}"))
            })?;

        let mut collations = self.collations.write();
        if let Some((idx, _)) = collations
            .iter()
            .enumerate()
            .find(|(_, collation)| collation.name.eq_ignore_ascii_case(name))
        {
            return LocaleCollationId::from_index(idx);
        }
        let id = LocaleCollationId::from_index(collations.len())?;
        collations.push(LocaleCollation {
            name: name.to_string(),
            collator,
        });
        Ok(id)
    }

    fn find(&self, name: &str) -> Option<LocaleCollationId> {
        self.collations
            .read()
            .iter()
            .enumerate()
            .find(|(_, collation)| collation.name.eq_ignore_ascii_case(name))
            .and_then(|(idx, _)| LocaleCollationId::from_index(idx).ok())
    }

    fn compare(&self, id: LocaleCollationId, lhs: &str, rhs: &str) -> Ordering {
        self.with_collation(id, |collation| collation.collator.compare(lhs, rhs))
    }

    fn sort_key(&self, id: LocaleCollationId, text: &str) -> Vec<u8> {
        self.with_collation(id, |collation| {
            let mut key = Vec::new();
            collation
                .collator
                .write_sort_key_to(text, &mut key)
                .expect("Vec collation key sink should be infallible");
            key
        })
    }

    fn name(&self, id: LocaleCollationId) -> String {
        self.with_collation(id, |collation| collation.name.clone())
    }

    fn with_collation<T>(&self, id: LocaleCollationId, f: impl FnOnce(&LocaleCollation) -> T) -> T {
        let collations = self.collations.read();
        let collation = collations
            .get(id.0 as usize)
            .expect("locale collation id should be registered");
        f(collation)
    }
}

fn custom_collation_id(name: &str) -> u32 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    name.hash(&mut hasher);
    ((hasher.finish() as u32) & 0x7fff_fffc).max(4)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_locale_collation_names() {
        assert!(matches!(
            CollationSeq::new("fr-FR").unwrap(),
            CollationSeq::Locale(_)
        ));
        assert!(matches!(
            CollationSeq::new("es-u-co-trad").unwrap(),
            CollationSeq::Locale(_)
        ));
        assert!(matches!(
            CollationSeq::new("en-u-kf-upper").unwrap(),
            CollationSeq::Locale(_)
        ));
        assert!(matches!(
            CollationSeq::new("en-US-u-kf-upper").unwrap(),
            CollationSeq::Locale(_)
        ));
        assert!(CollationSeq::new("compile_options").is_err());
    }

    #[test]
    fn test_locale_collation_compare() {
        let traditional_spanish = CollationSeq::new("es-u-co-trad").unwrap();
        assert_eq!(
            traditional_spanish.compare_strings("pollo", "polvo"),
            Ordering::Greater
        );

        let upper_first = CollationSeq::new("en-u-kf-upper").unwrap();
        assert_eq!(upper_first.compare_strings("A", "a"), Ordering::Less);

        let upper_first_us = CollationSeq::new("en-US-u-kf-upper").unwrap();
        assert_eq!(upper_first_us.compare_strings("A", "a"), Ordering::Less);
    }
}
