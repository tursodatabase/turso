use std::fmt;
use std::fmt::Debug;
use std::net::Ipv6Addr;

use serde::{Deserialize, Serialize};

use crate::InvalidData;
use crate::value::NumericalType;

/// The column type represents the column type.
/// Any changes need to be propagated to `COLUMN_TYPES`.
#[derive(Hash, Eq, PartialEq, Debug, Clone, Copy, Ord, PartialOrd, Serialize, Deserialize)]
#[repr(u8)]
pub enum ColumnType {
    I64 = 0u8,
    U64 = 1u8,
    F64 = 2u8,
    Bytes = 3u8,
    Str = 4u8,
    Bool = 5u8,
    IpAddr = 6u8,
    DateTime = 7u8,
}

impl fmt::Display for ColumnType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let short_str = match self {
            ColumnType::I64 => "i64",
            ColumnType::U64 => "u64",
            ColumnType::F64 => "f64",
            ColumnType::Bytes => "bytes",
            ColumnType::Str => "str",
            ColumnType::Bool => "bool",
            ColumnType::IpAddr => "ip",
            ColumnType::DateTime => "datetime",
        };
        write!(f, "{short_str}")
    }
}

// The order needs to match _exactly_ the order in the enum
const COLUMN_TYPES: [ColumnType; 8] = [
    ColumnType::I64,
    ColumnType::U64,
    ColumnType::F64,
    ColumnType::Bytes,
    ColumnType::Str,
    ColumnType::Bool,
    ColumnType::IpAddr,
    ColumnType::DateTime,
];

impl ColumnType {
    pub fn to_code(self) -> u8 {
        self as u8
    }
    pub fn is_date_time(&self) -> bool {
        self == &ColumnType::DateTime
    }

    pub(crate) fn try_from_code(code: u8) -> Result<ColumnType, InvalidData> {
        COLUMN_TYPES.get(code as usize).copied().ok_or(InvalidData)
    }
}

impl From<NumericalType> for ColumnType {
    fn from(numerical_type: NumericalType) -> Self {
        match numerical_type {
            NumericalType::I64 => ColumnType::I64,
            NumericalType::U64 => ColumnType::U64,
            NumericalType::F64 => ColumnType::F64,
        }
    }
}

impl ColumnType {
    pub fn numerical_type(&self) -> Option<NumericalType> {
        match self {
            ColumnType::I64 => Some(NumericalType::I64),
            ColumnType::U64 => Some(NumericalType::U64),
            ColumnType::F64 => Some(NumericalType::F64),
            ColumnType::Bytes
            | ColumnType::Str
            | ColumnType::Bool
            | ColumnType::IpAddr
            | ColumnType::DateTime => None,
        }
    }
}

// TODO remove if possible
pub trait HasAssociatedColumnType: 'static + Debug + Send + Sync + Copy + PartialOrd {
    fn column_type() -> ColumnType;
    fn default_value() -> Self;
}

impl HasAssociatedColumnType for u64 {
    fn column_type() -> ColumnType {
        ColumnType::U64
    }

    fn default_value() -> Self {
        0u64
    }
}

impl HasAssociatedColumnType for i64 {
    fn column_type() -> ColumnType {
        ColumnType::I64
    }

    fn default_value() -> Self {
        0i64
    }
}

impl HasAssociatedColumnType for f64 {
    fn column_type() -> ColumnType {
        ColumnType::F64
    }

    fn default_value() -> Self {
        Default::default()
    }
}

impl HasAssociatedColumnType for bool {
    fn column_type() -> ColumnType {
        ColumnType::Bool
    }
    fn default_value() -> Self {
        Default::default()
    }
}

impl HasAssociatedColumnType for common::DateTime {
    fn column_type() -> ColumnType {
        ColumnType::DateTime
    }
    fn default_value() -> Self {
        Default::default()
    }
}

impl HasAssociatedColumnType for Ipv6Addr {
    fn column_type() -> ColumnType {
        ColumnType::IpAddr
    }

    fn default_value() -> Self {
        Ipv6Addr::from([0u8; 16])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Cardinality;

    #[test]
    fn test_column_type_to_code() {
        for (code, expected_column_type) in super::COLUMN_TYPES.iter().copied().enumerate() {
            if let Ok(column_type) = ColumnType::try_from_code(code as u8) {
                assert_eq!(column_type, expected_column_type);
            }
        }
        for code in COLUMN_TYPES.len() as u8..=u8::MAX {
            assert!(ColumnType::try_from_code(code).is_err());
        }
    }

    #[test]
    fn test_cardinality_to_code() {
        let mut num_cardinality = 0;
        for code in u8::MIN..=u8::MAX {
            if let Ok(cardinality) = Cardinality::try_from_code(code) {
                assert_eq!(cardinality.to_code(), code);
                num_cardinality += 1;
            }
        }
        assert_eq!(num_cardinality, 3);
    }
}
