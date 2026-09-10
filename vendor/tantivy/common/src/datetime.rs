use std::fmt;
use std::io::{Read, Write};

use serde::{Deserialize, Serialize};
use time::format_description::well_known::Rfc3339;
use time::{OffsetDateTime, PrimitiveDateTime, UtcOffset};

use crate::BinarySerializable;

/// Precision with which datetimes are truncated when stored in fast fields. This setting is only
/// relevant for fast fields. In the docstore, datetimes are always saved with nanosecond precision.
#[derive(
    Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Default,
)]
#[serde(rename_all = "lowercase")]
pub enum DateTimePrecision {
    /// Second precision.
    #[default]
    Seconds,
    /// Millisecond precision.
    Milliseconds,
    /// Microsecond precision.
    Microseconds,
    /// Nanosecond precision.
    Nanoseconds,
}

/// A date/time value with nanoseconds precision.
///
/// This timestamp does not carry any explicit time zone information.
/// Users are responsible for applying the provided conversion
/// functions consistently. Internally the time zone is assumed
/// to be UTC, which is also used implicitly for JSON serialization.
///
/// All constructors and conversions are provided as explicit
/// functions and not by implementing any `From`/`Into` traits
/// to prevent unintended usage.
#[derive(Clone, Default, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DateTime {
    // Timestamp in nanoseconds.
    pub(crate) timestamp_nanos: i64,
}

impl DateTime {
    /// Minimum possible `DateTime` value.
    pub const MIN: DateTime = DateTime {
        timestamp_nanos: i64::MIN,
    };

    /// Maximum possible `DateTime` value.
    pub const MAX: DateTime = DateTime {
        timestamp_nanos: i64::MAX,
    };

    /// Create new from UNIX timestamp in seconds
    pub const fn from_timestamp_secs(seconds: i64) -> Self {
        Self {
            timestamp_nanos: seconds * 1_000_000_000,
        }
    }

    /// Create new from UNIX timestamp in milliseconds
    pub const fn from_timestamp_millis(milliseconds: i64) -> Self {
        Self {
            timestamp_nanos: milliseconds * 1_000_000,
        }
    }

    /// Create new from UNIX timestamp in microseconds.
    pub const fn from_timestamp_micros(microseconds: i64) -> Self {
        Self {
            timestamp_nanos: microseconds * 1_000,
        }
    }

    /// Create new from UNIX timestamp in nanoseconds.
    pub const fn from_timestamp_nanos(nanoseconds: i64) -> Self {
        Self {
            timestamp_nanos: nanoseconds,
        }
    }

    /// Create new from `OffsetDateTime`
    ///
    /// The given date/time is converted to UTC and the actual
    /// time zone is discarded.
    pub fn from_utc(dt: OffsetDateTime) -> Self {
        let timestamp_nanos = dt.unix_timestamp_nanos() as i64;
        Self { timestamp_nanos }
    }

    /// Create new from `PrimitiveDateTime`
    ///
    /// Implicitly assumes that the given date/time is in UTC!
    /// Otherwise the original value must only be reobtained with
    /// [`Self::into_primitive()`].
    pub fn from_primitive(dt: PrimitiveDateTime) -> Self {
        Self::from_utc(dt.assume_utc())
    }

    /// Convert to UNIX timestamp in seconds.
    pub const fn into_timestamp_secs(self) -> i64 {
        self.timestamp_nanos / 1_000_000_000
    }

    /// Convert to UNIX timestamp in milliseconds.
    pub const fn into_timestamp_millis(self) -> i64 {
        self.timestamp_nanos / 1_000_000
    }

    /// Convert to UNIX timestamp in microseconds.
    pub const fn into_timestamp_micros(self) -> i64 {
        self.timestamp_nanos / 1_000
    }

    /// Convert to UNIX timestamp in nanoseconds.
    pub const fn into_timestamp_nanos(self) -> i64 {
        self.timestamp_nanos
    }

    /// Convert to UTC `OffsetDateTime`
    pub fn into_utc(self) -> OffsetDateTime {
        let utc_datetime = OffsetDateTime::from_unix_timestamp_nanos(self.timestamp_nanos as i128)
            .expect("valid UNIX timestamp");
        debug_assert_eq!(UtcOffset::UTC, utc_datetime.offset());
        utc_datetime
    }

    /// Convert to `OffsetDateTime` with the given time zone
    pub fn into_offset(self, offset: UtcOffset) -> OffsetDateTime {
        self.into_utc().to_offset(offset)
    }

    /// Convert to `PrimitiveDateTime` without any time zone
    ///
    /// The value should have been constructed with [`Self::from_primitive()`].
    /// Otherwise the time zone is implicitly assumed to be UTC.
    pub fn into_primitive(self) -> PrimitiveDateTime {
        let utc_datetime = self.into_utc();
        // Discard the UTC time zone offset
        debug_assert_eq!(UtcOffset::UTC, utc_datetime.offset());
        PrimitiveDateTime::new(utc_datetime.date(), utc_datetime.time())
    }

    /// Truncates the microseconds value to the corresponding precision.
    pub fn truncate(self, precision: DateTimePrecision) -> Self {
        let truncated_timestamp_micros = match precision {
            DateTimePrecision::Seconds => (self.timestamp_nanos / 1_000_000_000) * 1_000_000_000,
            DateTimePrecision::Milliseconds => (self.timestamp_nanos / 1_000_000) * 1_000_000,
            DateTimePrecision::Microseconds => (self.timestamp_nanos / 1_000) * 1_000,
            DateTimePrecision::Nanoseconds => self.timestamp_nanos,
        };
        Self {
            timestamp_nanos: truncated_timestamp_micros,
        }
    }
}

impl fmt::Debug for DateTime {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let utc_rfc3339 = self.into_utc().format(&Rfc3339).map_err(|_| fmt::Error)?;
        f.write_str(&utc_rfc3339)
    }
}

impl BinarySerializable for DateTime {
    fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> std::io::Result<()> {
        let timestamp_micros = self.into_timestamp_micros();
        <i64 as BinarySerializable>::serialize(&timestamp_micros, writer)
    }

    fn deserialize<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let timestamp_micros = <i64 as BinarySerializable>::deserialize(reader)?;
        Ok(Self::from_timestamp_micros(timestamp_micros))
    }
}
