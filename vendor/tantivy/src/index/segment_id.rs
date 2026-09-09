use std::cmp::Ordering;
use std::error::Error;
use std::fmt;
use std::str::FromStr;
#[cfg(test)]
use std::sync::atomic;

#[cfg(test)]
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Uuid identifying a segment.
///
/// Tantivy's segment are identified
/// by a UUID which is used to prefix the filenames
/// of all of the file associated with the segment.
///
/// In unit test, for reproducibility, the `SegmentId` are
/// simply generated in an autoincrement fashion.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SegmentId(Uuid);

#[cfg(test)]
static AUTO_INC_COUNTER: Lazy<atomic::AtomicUsize> = Lazy::new(atomic::AtomicUsize::default);

#[cfg(test)]
const ZERO_ARRAY: [u8; 8] = [0u8; 8];

// During tests, we generate the segment id in a autoincrement manner
// for consistency of segment id between run.
//
// The order of the test execution is not guaranteed, but the order
// of segments within a single test is guaranteed.
#[cfg(test)]
fn create_uuid() -> Uuid {
    let new_auto_inc_id = (*AUTO_INC_COUNTER).fetch_add(1, atomic::Ordering::SeqCst);
    Uuid::from_fields(new_auto_inc_id as u32, 0, 0, &ZERO_ARRAY)
}

#[cfg(not(test))]
fn create_uuid() -> Uuid {
    Uuid::new_v4()
}

impl SegmentId {
    #[doc(hidden)]
    pub fn generate_random() -> SegmentId {
        SegmentId(create_uuid())
    }

    /// Returns a shorter identifier of the segment.
    ///
    /// We are using UUID4, so only 6 bits are fixed,
    /// and the rest is random.
    ///
    /// Picking the first 8 chars is ok to identify
    /// segments in a display message (e.g. a5c4dfcb).
    pub fn short_uuid_string(&self) -> String {
        self.0.as_simple().to_string()[..8].to_string()
    }

    /// Returns a segment uuid string.
    ///
    /// It consists in 32 lowercase hexadecimal chars
    /// (e.g. a5c4dfcbdfe645089129e308e26d5523)
    pub fn uuid_string(&self) -> String {
        self.0.as_simple().to_string()
    }

    /// Build a `SegmentId` string from the full uuid string.
    ///
    /// E.g. "a5c4dfcbdfe645089129e308e26d5523"
    pub fn from_uuid_string(uuid_string: &str) -> Result<SegmentId, SegmentIdParseError> {
        FromStr::from_str(uuid_string)
    }
}

/// Error type used when parsing a `SegmentId` from a string fails.
pub struct SegmentIdParseError(uuid::Error);

impl Error for SegmentIdParseError {}

impl fmt::Debug for SegmentIdParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl fmt::Display for SegmentIdParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for SegmentId {
    type Err = SegmentIdParseError;

    fn from_str(uuid_string: &str) -> Result<Self, SegmentIdParseError> {
        let uuid = Uuid::parse_str(uuid_string).map_err(SegmentIdParseError)?;
        Ok(SegmentId(uuid))
    }
}

impl fmt::Debug for SegmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Seg({:?})", self.short_uuid_string())
    }
}

impl fmt::Display for SegmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Seg({:?})", self.short_uuid_string())
    }
}

impl PartialOrd for SegmentId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SegmentId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.as_bytes().cmp(other.0.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::SegmentId;

    #[test]
    fn test_to_uuid_string() {
        let full_uuid = "a5c4dfcbdfe645089129e308e26d5523";
        let segment_id = SegmentId::from_uuid_string(full_uuid).unwrap();
        assert_eq!(segment_id.uuid_string(), full_uuid);
        assert_eq!(segment_id.short_uuid_string(), "a5c4dfcb");
        // one extra char
        assert!(SegmentId::from_uuid_string("a5c4dfcbdfe645089129e308e26d5523b").is_err());
    }
}
