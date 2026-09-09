use std::net::Ipv6Addr;
use std::sync::Arc;
use std::{fmt, io};

use common::file_slice::FileSlice;
use common::{ByteCount, DateTime, OwnedBytes};
use serde::{Deserialize, Serialize};

use crate::column::{BytesColumn, Column, StrColumn};
use crate::column_values::{StrictlyMonotonicFn, monotonic_map_column};
use crate::columnar::ColumnType;
use crate::{Cardinality, ColumnIndex, ColumnValues, NumericalType, Version};

#[derive(Clone)]
pub enum DynamicColumn {
    Bool(Column<bool>),
    I64(Column<i64>),
    U64(Column<u64>),
    F64(Column<f64>),
    IpAddr(Column<Ipv6Addr>),
    DateTime(Column<DateTime>),
    Bytes(BytesColumn),
    Str(StrColumn),
}

impl fmt::Debug for DynamicColumn {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[{} {} |", self.get_cardinality(), self.column_type())?;
        match self {
            DynamicColumn::Bool(col) => write!(f, " {col:?}")?,
            DynamicColumn::I64(col) => write!(f, " {col:?}")?,
            DynamicColumn::U64(col) => write!(f, " {col:?}")?,
            DynamicColumn::F64(col) => write!(f, "{col:?}")?,
            DynamicColumn::IpAddr(col) => write!(f, "{col:?}")?,
            DynamicColumn::DateTime(col) => write!(f, "{col:?}")?,
            DynamicColumn::Bytes(col) => write!(f, "{col:?}")?,
            DynamicColumn::Str(col) => write!(f, "{col:?}")?,
        }
        write!(f, "]")
    }
}

impl DynamicColumn {
    pub fn column_index(&self) -> &ColumnIndex {
        match self {
            DynamicColumn::Bool(c) => &c.index,
            DynamicColumn::I64(c) => &c.index,
            DynamicColumn::U64(c) => &c.index,
            DynamicColumn::F64(c) => &c.index,
            DynamicColumn::IpAddr(c) => &c.index,
            DynamicColumn::DateTime(c) => &c.index,
            DynamicColumn::Bytes(c) => &c.ords().index,
            DynamicColumn::Str(c) => &c.ords().index,
        }
    }

    pub fn get_cardinality(&self) -> Cardinality {
        self.column_index().get_cardinality()
    }

    pub fn num_values(&self) -> u32 {
        match self {
            DynamicColumn::Bool(c) => c.values.num_vals(),
            DynamicColumn::I64(c) => c.values.num_vals(),
            DynamicColumn::U64(c) => c.values.num_vals(),
            DynamicColumn::F64(c) => c.values.num_vals(),
            DynamicColumn::IpAddr(c) => c.values.num_vals(),
            DynamicColumn::DateTime(c) => c.values.num_vals(),
            DynamicColumn::Bytes(c) => c.ords().values.num_vals(),
            DynamicColumn::Str(c) => c.ords().values.num_vals(),
        }
    }

    pub fn column_type(&self) -> ColumnType {
        match self {
            DynamicColumn::Bool(_) => ColumnType::Bool,
            DynamicColumn::I64(_) => ColumnType::I64,
            DynamicColumn::U64(_) => ColumnType::U64,
            DynamicColumn::F64(_) => ColumnType::F64,
            DynamicColumn::IpAddr(_) => ColumnType::IpAddr,
            DynamicColumn::DateTime(_) => ColumnType::DateTime,
            DynamicColumn::Bytes(_) => ColumnType::Bytes,
            DynamicColumn::Str(_) => ColumnType::Str,
        }
    }

    pub fn coerce_numerical(self, target_numerical_type: NumericalType) -> Option<Self> {
        match target_numerical_type {
            NumericalType::I64 => self.coerce_to_i64(),
            NumericalType::U64 => self.coerce_to_u64(),
            NumericalType::F64 => self.coerce_to_f64(),
        }
    }

    pub fn is_numerical(&self) -> bool {
        self.column_type().numerical_type().is_some()
    }

    pub fn is_f64(&self) -> bool {
        self.column_type().numerical_type() == Some(NumericalType::F64)
    }
    pub fn is_i64(&self) -> bool {
        self.column_type().numerical_type() == Some(NumericalType::I64)
    }
    pub fn is_u64(&self) -> bool {
        self.column_type().numerical_type() == Some(NumericalType::U64)
    }

    fn coerce_to_f64(self) -> Option<DynamicColumn> {
        match self {
            DynamicColumn::I64(column) => Some(DynamicColumn::F64(Column {
                index: column.index,
                values: Arc::new(monotonic_map_column(column.values, MapI64ToF64)),
            })),
            DynamicColumn::U64(column) => Some(DynamicColumn::F64(Column {
                index: column.index,
                values: Arc::new(monotonic_map_column(column.values, MapU64ToF64)),
            })),
            DynamicColumn::F64(_) => Some(self),
            _ => None,
        }
    }
    fn coerce_to_i64(self) -> Option<DynamicColumn> {
        match self {
            DynamicColumn::U64(column) => {
                if column.max_value() > i64::MAX as u64 {
                    return None;
                }
                Some(DynamicColumn::I64(Column {
                    index: column.index,
                    values: Arc::new(monotonic_map_column(column.values, MapU64ToI64)),
                }))
            }
            DynamicColumn::I64(_) => Some(self),
            _ => None,
        }
    }
    fn coerce_to_u64(self) -> Option<DynamicColumn> {
        match self {
            DynamicColumn::I64(column) => {
                if column.min_value() < 0 {
                    return None;
                }
                Some(DynamicColumn::U64(Column {
                    index: column.index,
                    values: Arc::new(monotonic_map_column(column.values, MapI64ToU64)),
                }))
            }
            DynamicColumn::U64(_) => Some(self),
            _ => None,
        }
    }
}

struct MapI64ToF64;
impl StrictlyMonotonicFn<i64, f64> for MapI64ToF64 {
    #[inline(always)]
    fn mapping(&self, inp: i64) -> f64 {
        inp as f64
    }
    #[inline(always)]
    fn inverse(&self, out: f64) -> i64 {
        out as i64
    }
}

struct MapU64ToF64;
impl StrictlyMonotonicFn<u64, f64> for MapU64ToF64 {
    #[inline(always)]
    fn mapping(&self, inp: u64) -> f64 {
        inp as f64
    }
    #[inline(always)]
    fn inverse(&self, out: f64) -> u64 {
        out as u64
    }
}

struct MapU64ToI64;
impl StrictlyMonotonicFn<u64, i64> for MapU64ToI64 {
    #[inline(always)]
    fn mapping(&self, inp: u64) -> i64 {
        inp as i64
    }
    #[inline(always)]
    fn inverse(&self, out: i64) -> u64 {
        out as u64
    }
}

struct MapI64ToU64;
impl StrictlyMonotonicFn<i64, u64> for MapI64ToU64 {
    #[inline(always)]
    fn mapping(&self, inp: i64) -> u64 {
        inp as u64
    }
    #[inline(always)]
    fn inverse(&self, out: u64) -> i64 {
        out as i64
    }
}

macro_rules! static_dynamic_conversions {
    ($typ:ty, $enum_name:ident) => {
        impl From<DynamicColumn> for Option<$typ> {
            fn from(dynamic_column: DynamicColumn) -> Option<$typ> {
                if let DynamicColumn::$enum_name(col) = dynamic_column {
                    Some(col)
                } else {
                    None
                }
            }
        }

        impl From<$typ> for DynamicColumn {
            fn from(typed_column: $typ) -> Self {
                DynamicColumn::$enum_name(typed_column)
            }
        }
    };
}

static_dynamic_conversions!(Column<bool>, Bool);
static_dynamic_conversions!(Column<u64>, U64);
static_dynamic_conversions!(Column<i64>, I64);
static_dynamic_conversions!(Column<f64>, F64);
static_dynamic_conversions!(Column<DateTime>, DateTime);
static_dynamic_conversions!(StrColumn, Str);
static_dynamic_conversions!(BytesColumn, Bytes);
static_dynamic_conversions!(Column<Ipv6Addr>, IpAddr);

#[derive(Clone, Debug)]
pub struct DynamicColumnHandle {
    pub(crate) file_slice: FileSlice,
    pub(crate) column_type: ColumnType,
    pub(crate) format_version: Version,
}

impl DynamicColumnHandle {
    // TODO rename load
    pub fn open(&self) -> io::Result<DynamicColumn> {
        let column_bytes: OwnedBytes = self.file_slice.read_bytes()?;
        self.open_internal(column_bytes)
    }

    #[doc(hidden)]
    pub fn file_slice(&self) -> &FileSlice {
        &self.file_slice
    }

    /// Returns the `u64` fast field reader reader associated with `fields` of types
    /// Str, u64, i64, f64, bool, ip, or datetime.
    ///
    /// Notice that for IpAddr, the fastfield reader will return the u64 representation of the
    /// IpAddr.
    /// In order to convert to u128 back cast to `CompactSpaceU64Accessor` and call
    /// `compact_to_u128`.
    ///
    /// If not, the fastfield reader will returns the u64-value associated with the original
    /// FastValue.
    pub fn open_u64_lenient(&self) -> io::Result<Option<Column<u64>>> {
        let column_bytes = self.file_slice.read_bytes()?;
        match self.column_type {
            ColumnType::Str | ColumnType::Bytes => {
                let column: BytesColumn =
                    crate::column::open_column_bytes(column_bytes, self.format_version)?;
                Ok(Some(column.term_ord_column))
            }
            ColumnType::IpAddr => {
                let column = crate::column::open_column_u128_as_compact_u64(
                    column_bytes,
                    self.format_version,
                )?;
                Ok(Some(column))
            }
            ColumnType::Bool
            | ColumnType::I64
            | ColumnType::U64
            | ColumnType::F64
            | ColumnType::DateTime => {
                let column =
                    crate::column::open_column_u64::<u64>(column_bytes, self.format_version)?;
                Ok(Some(column))
            }
        }
    }

    fn open_internal(&self, column_bytes: OwnedBytes) -> io::Result<DynamicColumn> {
        let dynamic_column: DynamicColumn = match self.column_type {
            ColumnType::Bytes => {
                crate::column::open_column_bytes(column_bytes, self.format_version)?.into()
            }
            ColumnType::Str => {
                crate::column::open_column_str(column_bytes, self.format_version)?.into()
            }
            ColumnType::I64 => {
                crate::column::open_column_u64::<i64>(column_bytes, self.format_version)?.into()
            }
            ColumnType::U64 => {
                crate::column::open_column_u64::<u64>(column_bytes, self.format_version)?.into()
            }
            ColumnType::F64 => {
                crate::column::open_column_u64::<f64>(column_bytes, self.format_version)?.into()
            }
            ColumnType::Bool => {
                crate::column::open_column_u64::<bool>(column_bytes, self.format_version)?.into()
            }
            ColumnType::IpAddr => {
                crate::column::open_column_u128::<Ipv6Addr>(column_bytes, self.format_version)?
                    .into()
            }
            ColumnType::DateTime => {
                crate::column::open_column_u64::<DateTime>(column_bytes, self.format_version)?
                    .into()
            }
        };
        Ok(dynamic_column)
    }

    pub fn num_bytes(&self) -> ByteCount {
        self.file_slice.num_bytes()
    }

    /// Legacy helper returning the column space usage.
    pub fn column_and_dictionary_num_bytes(&self) -> io::Result<ColumnSpaceUsage> {
        self.space_usage()
    }

    /// Return the space usage of the column, optionally broken down by dictionary and column
    /// values.
    ///
    /// For dictionary encoded columns (strings and bytes), this splits the total footprint into
    /// the dictionary and the remaining column data (including index and values).
    /// For all other column types, the dictionary size is `None` and the column size
    /// equals the total bytes.
    pub fn space_usage(&self) -> io::Result<ColumnSpaceUsage> {
        let total_num_bytes = self.num_bytes();
        let dynamic_column = self.open()?;
        let dictionary_num_bytes = match &dynamic_column {
            DynamicColumn::Bytes(bytes_column) => bytes_column.dictionary().num_bytes(),
            DynamicColumn::Str(str_column) => str_column.dictionary().num_bytes(),
            _ => {
                return Ok(ColumnSpaceUsage::new(self.num_bytes(), None));
            }
        };
        assert!(dictionary_num_bytes <= total_num_bytes);
        let column_num_bytes =
            ByteCount::from(total_num_bytes.get_bytes() - dictionary_num_bytes.get_bytes());
        Ok(ColumnSpaceUsage::new(
            column_num_bytes,
            Some(dictionary_num_bytes),
        ))
    }

    pub fn column_type(&self) -> ColumnType {
        self.column_type
    }
}

/// Represents space usage of a column.
///
/// `column_num_bytes` tracks the column payload (index, values and footer).
/// For dictionary encoded columns, `dictionary_num_bytes` captures the dictionary footprint.
/// [`ColumnSpaceUsage::total_num_bytes`] returns the sum of both parts.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColumnSpaceUsage {
    column_num_bytes: ByteCount,
    dictionary_num_bytes: Option<ByteCount>,
}

impl ColumnSpaceUsage {
    pub(crate) fn new(
        column_num_bytes: ByteCount,
        dictionary_num_bytes: Option<ByteCount>,
    ) -> Self {
        ColumnSpaceUsage {
            column_num_bytes,
            dictionary_num_bytes,
        }
    }

    pub fn column_num_bytes(&self) -> ByteCount {
        self.column_num_bytes
    }

    pub fn dictionary_num_bytes(&self) -> Option<ByteCount> {
        self.dictionary_num_bytes
    }

    pub fn total_num_bytes(&self) -> ByteCount {
        self.column_num_bytes + self.dictionary_num_bytes.unwrap_or_default()
    }

    /// Merge two space usage values by summing their components.
    pub fn merge(&self, other: &ColumnSpaceUsage) -> ColumnSpaceUsage {
        let dictionary_num_bytes = match (self.dictionary_num_bytes, other.dictionary_num_bytes) {
            (Some(lhs), Some(rhs)) => Some(lhs + rhs),
            (Some(val), None) | (None, Some(val)) => Some(val),
            (None, None) => None,
        };
        ColumnSpaceUsage {
            column_num_bytes: self.column_num_bytes + other.column_num_bytes,
            dictionary_num_bytes,
        }
    }
}
