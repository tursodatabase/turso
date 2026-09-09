use std::iter::Sum;
use std::ops::{Add, AddAssign};

use serde::{Deserialize, Serialize};

/// Indicates space usage in bytes
#[derive(Copy, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ByteCount(u64);

impl std::fmt::Debug for ByteCount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.human_readable())
    }
}

impl std::fmt::Display for ByteCount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.human_readable())
    }
}

const SUFFIX_AND_THRESHOLD: [(&str, u64); 5] = [
    ("KB", 1_000),
    ("MB", 1_000_000),
    ("GB", 1_000_000_000),
    ("TB", 1_000_000_000_000),
    ("PB", 1_000_000_000_000_000),
];

impl ByteCount {
    #[inline]
    pub fn get_bytes(&self) -> u64 {
        self.0
    }

    pub fn human_readable(&self) -> String {
        for (suffix, threshold) in SUFFIX_AND_THRESHOLD.iter().rev() {
            if self.get_bytes() >= *threshold {
                let unit_num = self.get_bytes() as f64 / *threshold as f64;
                return format!("{unit_num:.2} {suffix}");
            }
        }
        format!("{:.2} B", self.get_bytes())
    }
}

impl From<u64> for ByteCount {
    fn from(value: u64) -> Self {
        ByteCount(value)
    }
}
impl From<usize> for ByteCount {
    fn from(value: usize) -> Self {
        ByteCount(value as u64)
    }
}

impl Sum for ByteCount {
    #[inline]
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(ByteCount::default(), |acc, x| acc + x)
    }
}

impl PartialEq<u64> for ByteCount {
    #[inline]
    fn eq(&self, other: &u64) -> bool {
        self.get_bytes() == *other
    }
}

impl PartialOrd<u64> for ByteCount {
    #[inline]
    fn partial_cmp(&self, other: &u64) -> Option<std::cmp::Ordering> {
        self.get_bytes().partial_cmp(other)
    }
}

impl Add for ByteCount {
    type Output = Self;

    #[inline]
    fn add(self, other: Self) -> Self {
        Self(self.get_bytes() + other.get_bytes())
    }
}

impl AddAssign for ByteCount {
    #[inline]
    fn add_assign(&mut self, other: Self) {
        *self = Self(self.get_bytes() + other.get_bytes());
    }
}

#[cfg(test)]
mod test {
    use crate::ByteCount;

    #[test]
    fn test_bytes() {
        assert_eq!(ByteCount::from(0u64).human_readable(), "0 B");
        assert_eq!(ByteCount::from(300u64).human_readable(), "300 B");
        assert_eq!(ByteCount::from(1_000_000u64).human_readable(), "1.00 MB");
        assert_eq!(ByteCount::from(1_500_000u64).human_readable(), "1.50 MB");
        assert_eq!(
            ByteCount::from(1_500_000_000u64).human_readable(),
            "1.50 GB"
        );
        assert_eq!(
            ByteCount::from(3_213_000_000_000u64).human_readable(),
            "3.21 TB"
        );
    }
}
