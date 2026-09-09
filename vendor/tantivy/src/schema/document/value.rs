use std::fmt::Debug;
use std::net::Ipv6Addr;

use common::DateTime;

use crate::tokenizer::PreTokenizedString;

/// A single field value.
pub trait Value<'a>: Send + Sync + Debug {
    /// The child value type returned by this doc value.
    /// The iterator for walking through the elements within the array.
    type ArrayIter: Iterator<Item = Self>;
    /// The visitor walking through the key-value pairs within
    /// the object.
    type ObjectIter: Iterator<Item = (&'a str, Self)>;

    /// Returns the field value represented by an enum which borrows it's data.
    fn as_value(&self) -> ReferenceValue<'a, Self>;

    #[inline]
    /// If the Value is a leaf, returns the associated leaf. Returns None otherwise.
    fn as_leaf(&self) -> Option<ReferenceValueLeaf<'a>> {
        if let ReferenceValue::Leaf(val) = self.as_value() {
            Some(val)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a String, returns the associated str. Returns None otherwise.
    fn as_str(&self) -> Option<&'a str> {
        self.as_leaf().and_then(|leaf| leaf.as_str())
    }

    #[inline]
    /// If the Value is a u64, returns the associated u64. Returns None otherwise.
    fn as_u64(&self) -> Option<u64> {
        self.as_leaf().and_then(|leaf| leaf.as_u64())
    }

    #[inline]
    /// If the Value is a i64, returns the associated i64. Returns None otherwise.
    fn as_i64(&self) -> Option<i64> {
        self.as_leaf().and_then(|leaf| leaf.as_i64())
    }

    #[inline]
    /// If the Value is a f64, returns the associated f64. Returns None otherwise.
    fn as_f64(&self) -> Option<f64> {
        self.as_leaf().and_then(|leaf| leaf.as_f64())
    }

    #[inline]
    /// If the Value is a datetime, returns the associated datetime. Returns None otherwise.
    fn as_datetime(&self) -> Option<DateTime> {
        self.as_leaf().and_then(|leaf| leaf.as_datetime())
    }

    #[inline]
    /// If the Value is a IP address, returns the associated IP. Returns None otherwise.
    fn as_ip_addr(&self) -> Option<Ipv6Addr> {
        self.as_leaf().and_then(|leaf| leaf.as_ip_addr())
    }

    #[inline]
    /// If the Value is a bool, returns the associated bool. Returns None otherwise.
    fn as_bool(&self) -> Option<bool> {
        self.as_leaf().and_then(|leaf| leaf.as_bool())
    }

    #[inline]
    /// If the Value is a pre-tokenized string, returns the associated string. Returns None
    /// otherwise.
    fn as_pre_tokenized_text(&self) -> Option<Box<PreTokenizedString>> {
        self.as_leaf()
            .and_then(|leaf| leaf.into_pre_tokenized_text())
    }

    #[inline]
    /// If the Value is a bytes value, returns the associated set of bytes. Returns None otherwise.
    fn as_bytes(&self) -> Option<&'a [u8]> {
        self.as_leaf().and_then(|leaf| leaf.as_bytes())
    }

    #[inline]
    /// If the Value is a facet, returns the associated facet. Returns None otherwise.
    fn as_facet(&self) -> Option<&'a str> {
        self.as_leaf().and_then(|leaf| leaf.as_facet())
    }

    #[inline]
    /// Returns the iterator over the array if the Value is an array.
    fn as_array(&self) -> Option<Self::ArrayIter> {
        if let ReferenceValue::Array(val) = self.as_value() {
            Some(val)
        } else {
            None
        }
    }

    #[inline]
    /// Returns the iterator over the object if the Value is an object.
    fn as_object(&self) -> Option<Self::ObjectIter> {
        if let ReferenceValue::Object(val) = self.as_value() {
            Some(val)
        } else {
            None
        }
    }
}

/// A enum representing a leaf value for tantivy to index.
#[derive(Clone, Debug, PartialEq)]
pub enum ReferenceValueLeaf<'a> {
    /// A null value.
    Null,
    /// The str type is used for any text information.
    Str(&'a str),
    /// Unsigned 64-bits Integer `u64`
    U64(u64),
    /// Signed 64-bits Integer `i64`
    I64(i64),
    /// 64-bits Float `f64`
    F64(f64),
    /// Date/time with nanoseconds precision
    Date(DateTime),
    /// Facet string needs to match the format of
    /// [Facet::encoded_str](crate::schema::Facet::encoded_str).
    Facet(&'a str),
    /// Arbitrarily sized byte array
    Bytes(&'a [u8]),
    /// IpV6 Address. Internally there is no IpV4, it needs to be converted to `Ipv6Addr`.
    IpAddr(Ipv6Addr),
    /// Bool value
    Bool(bool),
    /// Pre-tokenized str type,
    PreTokStr(Box<PreTokenizedString>),
}

impl From<u64> for ReferenceValueLeaf<'_> {
    #[inline]
    fn from(value: u64) -> Self {
        ReferenceValueLeaf::U64(value)
    }
}

impl From<i64> for ReferenceValueLeaf<'_> {
    #[inline]
    fn from(value: i64) -> Self {
        ReferenceValueLeaf::I64(value)
    }
}

impl From<f64> for ReferenceValueLeaf<'_> {
    #[inline]
    fn from(value: f64) -> Self {
        ReferenceValueLeaf::F64(value)
    }
}

impl From<bool> for ReferenceValueLeaf<'_> {
    #[inline]
    fn from(value: bool) -> Self {
        ReferenceValueLeaf::Bool(value)
    }
}

impl<'a> From<&'a str> for ReferenceValueLeaf<'a> {
    #[inline]
    fn from(value: &'a str) -> Self {
        ReferenceValueLeaf::Str(value)
    }
}

impl<'a> From<&'a [u8]> for ReferenceValueLeaf<'a> {
    #[inline]
    fn from(value: &'a [u8]) -> Self {
        ReferenceValueLeaf::Bytes(value)
    }
}

impl From<DateTime> for ReferenceValueLeaf<'_> {
    #[inline]
    fn from(value: DateTime) -> Self {
        ReferenceValueLeaf::Date(value)
    }
}

impl From<Ipv6Addr> for ReferenceValueLeaf<'_> {
    #[inline]
    fn from(value: Ipv6Addr) -> Self {
        ReferenceValueLeaf::IpAddr(value)
    }
}

impl From<PreTokenizedString> for ReferenceValueLeaf<'_> {
    #[inline]
    fn from(val: PreTokenizedString) -> Self {
        ReferenceValueLeaf::PreTokStr(Box::new(val))
    }
}

impl<'a, T: Value<'a> + ?Sized> From<ReferenceValueLeaf<'a>> for ReferenceValue<'a, T> {
    #[inline]
    fn from(value: ReferenceValueLeaf<'a>) -> Self {
        match value {
            ReferenceValueLeaf::Null => ReferenceValue::Leaf(ReferenceValueLeaf::Null),
            ReferenceValueLeaf::Str(val) => ReferenceValue::Leaf(ReferenceValueLeaf::Str(val)),
            ReferenceValueLeaf::U64(val) => ReferenceValue::Leaf(ReferenceValueLeaf::U64(val)),
            ReferenceValueLeaf::I64(val) => ReferenceValue::Leaf(ReferenceValueLeaf::I64(val)),
            ReferenceValueLeaf::F64(val) => ReferenceValue::Leaf(ReferenceValueLeaf::F64(val)),
            ReferenceValueLeaf::Date(val) => ReferenceValue::Leaf(ReferenceValueLeaf::Date(val)),
            ReferenceValueLeaf::Facet(val) => ReferenceValue::Leaf(ReferenceValueLeaf::Facet(val)),
            ReferenceValueLeaf::Bytes(val) => ReferenceValue::Leaf(ReferenceValueLeaf::Bytes(val)),
            ReferenceValueLeaf::IpAddr(val) => {
                ReferenceValue::Leaf(ReferenceValueLeaf::IpAddr(val))
            }
            ReferenceValueLeaf::Bool(val) => ReferenceValue::Leaf(ReferenceValueLeaf::Bool(val)),
            ReferenceValueLeaf::PreTokStr(val) => {
                ReferenceValue::Leaf(ReferenceValueLeaf::PreTokStr(val))
            }
        }
    }
}

impl<'a> ReferenceValueLeaf<'a> {
    #[inline]
    /// Returns if the value is `null` or not.
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    #[inline]
    /// If the Value is a String, returns the associated str. Returns None otherwise.
    pub fn as_str(&self) -> Option<&'a str> {
        if let Self::Str(val) = self {
            Some(val)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a u64, returns the associated u64. Returns None otherwise.
    pub fn as_u64(&self) -> Option<u64> {
        if let Self::U64(val) = self {
            Some(*val)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a i64, returns the associated i64. Returns None otherwise.
    pub fn as_i64(&self) -> Option<i64> {
        if let Self::I64(val) = self {
            Some(*val)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a f64, returns the associated f64. Returns None otherwise.
    pub fn as_f64(&self) -> Option<f64> {
        if let Self::F64(val) = self {
            Some(*val)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a datetime, returns the associated datetime. Returns None otherwise.
    pub fn as_datetime(&self) -> Option<DateTime> {
        if let Self::Date(val) = self {
            Some(*val)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a IP address, returns the associated IP. Returns None otherwise.
    pub fn as_ip_addr(&self) -> Option<Ipv6Addr> {
        if let Self::IpAddr(val) = self {
            Some(*val)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a bool, returns the associated bool. Returns None otherwise.
    pub fn as_bool(&self) -> Option<bool> {
        if let Self::Bool(val) = self {
            Some(*val)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a pre-tokenized string, consumes it and returns the string.
    /// Returns None otherwise.
    pub fn into_pre_tokenized_text(self) -> Option<Box<PreTokenizedString>> {
        if let Self::PreTokStr(val) = self {
            Some(val)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a bytes value, returns the associated set of bytes. Returns None otherwise.
    pub fn as_bytes(&self) -> Option<&'a [u8]> {
        if let Self::Bytes(val) = self {
            Some(val)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a facet, returns the associated facet. Returns None otherwise.
    pub fn as_facet(&self) -> Option<&'a str> {
        if let Self::Facet(val) = self {
            Some(val)
        } else {
            None
        }
    }
}

/// A enum representing a value for tantivy to index.
#[derive(Clone, Debug, PartialEq)]
pub enum ReferenceValue<'a, V>
where V: Value<'a> + ?Sized
{
    /// A null value.
    Leaf(ReferenceValueLeaf<'a>),
    /// A an array containing multiple values.
    Array(V::ArrayIter),
    /// A nested / dynamic object.
    Object(V::ObjectIter),
}

impl<'a, V> ReferenceValue<'a, V>
where V: Value<'a>
{
    #[inline]
    /// Returns if the value is `null` or not.
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Leaf(ReferenceValueLeaf::Null))
    }

    #[inline]
    /// If the Value is a leaf, returns the associated leaf. Returns None otherwise.
    pub fn as_leaf(&self) -> Option<&ReferenceValueLeaf<'a>> {
        if let Self::Leaf(val) = self {
            Some(val)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a leaf, consume it and return the leaf. Returns None otherwise.
    pub fn into_leaf(self) -> Option<ReferenceValueLeaf<'a>> {
        if let Self::Leaf(val) = self {
            Some(val)
        } else {
            None
        }
    }

    #[inline]
    /// If the Value is a String, returns the associated str. Returns None otherwise.
    pub fn as_str(&self) -> Option<&'a str> {
        self.as_leaf().and_then(|leaf| leaf.as_str())
    }

    #[inline]
    /// If the Value is a u64, returns the associated u64. Returns None otherwise.
    pub fn as_u64(&self) -> Option<u64> {
        self.as_leaf().and_then(|leaf| leaf.as_u64())
    }

    #[inline]
    /// If the Value is a i64, returns the associated i64. Returns None otherwise.
    pub fn as_i64(&self) -> Option<i64> {
        self.as_leaf().and_then(|leaf| leaf.as_i64())
    }

    #[inline]
    /// If the Value is a f64, returns the associated f64. Returns None otherwise.
    pub fn as_f64(&self) -> Option<f64> {
        self.as_leaf().and_then(|leaf| leaf.as_f64())
    }

    #[inline]
    /// If the Value is a datetime, returns the associated datetime. Returns None otherwise.
    pub fn as_datetime(&self) -> Option<DateTime> {
        self.as_leaf().and_then(|leaf| leaf.as_datetime())
    }

    #[inline]
    /// If the Value is a IP address, returns the associated IP. Returns None otherwise.
    pub fn as_ip_addr(&self) -> Option<Ipv6Addr> {
        self.as_leaf().and_then(|leaf| leaf.as_ip_addr())
    }

    #[inline]
    /// If the Value is a bool, returns the associated bool. Returns None otherwise.
    pub fn as_bool(&self) -> Option<bool> {
        self.as_leaf().and_then(|leaf| leaf.as_bool())
    }

    #[inline]
    /// If the Value is a pre-tokenized string, consumes it and returns the string.
    /// Returns None otherwise.
    pub fn into_pre_tokenized_text(self) -> Option<Box<PreTokenizedString>> {
        self.into_leaf()
            .and_then(|leaf| leaf.into_pre_tokenized_text())
    }

    #[inline]
    /// If the Value is a bytes value, returns the associated set of bytes. Returns None otherwise.
    pub fn as_bytes(&self) -> Option<&'a [u8]> {
        self.as_leaf().and_then(|leaf| leaf.as_bytes())
    }

    #[inline]
    /// If the Value is a facet, returns the associated facet. Returns None otherwise.
    pub fn as_facet(&self) -> Option<&'a str> {
        self.as_leaf().and_then(|leaf| leaf.as_facet())
    }

    #[inline]
    /// Returns true if the Value is an array.
    pub fn is_array(&self) -> bool {
        matches!(self, Self::Array(_))
    }

    #[inline]
    /// Returns true if the Value is an object.
    pub fn is_object(&self) -> bool {
        matches!(self, Self::Object(_))
    }
}
