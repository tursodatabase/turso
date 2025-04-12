#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Instant {
    pub secs: i64,
    pub micros: u32,
}

pub trait Clock {
    fn now(&self) -> Instant;
}
