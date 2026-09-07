use std::ops::RangeInclusive;

/// The range of a blank in value space.
///
/// A blank is an unoccupied space in the data.
/// Use try_into() to construct.
/// A range has to have at least length of 3. Invalid ranges will be rejected.
///
/// Ordered by range length.
#[derive(Debug, Eq, PartialEq, Clone)]
pub(crate) struct BlankRange {
    blank_range: RangeInclusive<u128>,
}
impl TryFrom<RangeInclusive<u128>> for BlankRange {
    type Error = &'static str;
    fn try_from(range: RangeInclusive<u128>) -> Result<Self, Self::Error> {
        let blank_size = range.end().saturating_sub(*range.start());
        if blank_size < 2 {
            Err("invalid range")
        } else {
            Ok(BlankRange { blank_range: range })
        }
    }
}
impl BlankRange {
    pub(crate) fn blank_size(&self) -> u128 {
        self.blank_range.end() - self.blank_range.start() + 1
    }
    pub(crate) fn blank_range(&self) -> RangeInclusive<u128> {
        self.blank_range.clone()
    }
}

impl Ord for BlankRange {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.blank_size().cmp(&other.blank_size())
    }
}
impl PartialOrd for BlankRange {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
