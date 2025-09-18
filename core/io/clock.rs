use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Instant {
    pub secs: i64,
    pub micros: u32,
}

const NSEC_PER_SEC: u64 = 1_000_000_000;
const NANOS_PER_MICRO: u32 = 1_000;
const MICROS_PER_SEC: u32 = NSEC_PER_SEC as u32 / NANOS_PER_MICRO;

impl Instant {
    pub fn to_system_time(self) -> SystemTime {
        if self.secs >= 0 {
            UNIX_EPOCH + Duration::new(self.secs as u64, self.micros * 1000)
        } else {
            let positive_secs = (-self.secs) as u64;

            if self.micros > 0 {
                // We have partial seconds that reduce the negative offset
                // Need to borrow 1 second and subtract the remainder
                let nanos_to_subtract = (1_000_000 - self.micros) * 1000;
                UNIX_EPOCH - Duration::new(positive_secs - 1, nanos_to_subtract)
            } else {
                // Exactly N seconds before epoch
                UNIX_EPOCH - Duration::new(positive_secs, 0)
            }
        }
    }

    pub fn checked_add_duration(&self, other: &Duration) -> Option<Instant> {
        let mut secs = self.secs.checked_add_unsigned(other.as_secs())?;

        // Micros calculations can't overflow because micros are <1B which fit
        // in a u32.
        let mut micros = other.subsec_micros() + self.micros;
        if micros >= MICROS_PER_SEC {
            micros -= MICROS_PER_SEC;
            secs = secs.checked_add(1)?;
        }

        Some(Self { secs, micros })
    }

    pub fn checked_sub_duration(&self, other: &Duration) -> Option<Instant> {
        let mut secs = self.secs.checked_sub_unsigned(other.as_secs())?;

        // Similar to above, micros can't overflow.
        let mut micros = self.micros as i32 - other.subsec_micros() as i32;
        if micros < 0 {
            micros += MICROS_PER_SEC as i32;
            secs = secs.checked_sub(1)?;
        }
        Some(Self {
            secs,
            micros: micros as u32,
        })
    }
}

impl<T: chrono::TimeZone> From<chrono::DateTime<T>> for Instant {
    fn from(value: chrono::DateTime<T>) -> Self {
        Instant {
            secs: value.timestamp(),
            micros: value.timestamp_subsec_micros(),
        }
    }
}

impl std::ops::Add<Duration> for Instant {
    type Output = Instant;

    fn add(self, rhs: Duration) -> Self::Output {
        self.checked_add_duration(&rhs).unwrap()
    }
}

impl std::ops::Sub<Duration> for Instant {
    type Output = Instant;

    fn sub(self, rhs: Duration) -> Self::Output {
        self.checked_sub_duration(&rhs).unwrap()
    }
}

pub trait Clock {
    fn now(&self) -> Instant;
}
