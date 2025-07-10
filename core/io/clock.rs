#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Instant {
    pub secs: i64,
    pub micros: u32,
}

impl<T: chrono::TimeZone> From<chrono::DateTime<T>> for Instant {
    fn from(value: chrono::DateTime<T>) -> Self {
        Instant {
            secs: value.timestamp(),
            micros: value.timestamp_subsec_micros(),
        }
    }
}

pub trait Clock {
    fn now(&self) -> Instant;
}
