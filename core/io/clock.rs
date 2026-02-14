use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// A monotonic instant in time, backed by `std::time::Instant`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct MonotonicInstant(std::time::Instant);

impl MonotonicInstant {
    pub fn now() -> Self {
        MonotonicInstant(std::time::Instant::now())
    }

    pub fn elapsed(&self) -> Duration {
        self.0.elapsed()
    }

    pub fn duration_since(&self, earlier: MonotonicInstant) -> Duration {
        self.0.duration_since(earlier.0)
    }

    pub fn checked_add(&self, duration: Duration) -> Option<MonotonicInstant> {
        self.0.checked_add(duration).map(MonotonicInstant)
    }

    pub fn checked_sub(&self, duration: Duration) -> Option<MonotonicInstant> {
        self.0.checked_sub(duration).map(MonotonicInstant)
    }
}

impl std::ops::Add<Duration> for MonotonicInstant {
    type Output = MonotonicInstant;

    fn add(self, rhs: Duration) -> Self::Output {
        MonotonicInstant(self.0 + rhs)
    }
}

impl std::ops::Sub<Duration> for MonotonicInstant {
    type Output = MonotonicInstant;

    fn sub(self, rhs: Duration) -> Self::Output {
        MonotonicInstant(self.0 - rhs)
    }
}

/// Wall-clock time as seconds and microseconds since Unix epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct WallClockInstant {
    pub secs: i64,
    pub micros: u32,
}

const MICROS_PER_SEC: u32 = 1_000_000;

impl WallClockInstant {
    pub fn now() -> Self {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before Unix epoch");
        WallClockInstant {
            secs: duration.as_secs() as i64,
            micros: duration.subsec_micros(),
        }
    }

    pub fn to_system_time(self) -> SystemTime {
        if self.secs >= 0 {
            UNIX_EPOCH + Duration::new(self.secs as u64, self.micros * 1000)
        } else {
            let positive_secs = (-self.secs) as u64;
            if self.micros > 0 {
                let nanos_to_subtract = (1_000_000 - self.micros) * 1000;
                UNIX_EPOCH - Duration::new(positive_secs - 1, nanos_to_subtract)
            } else {
                UNIX_EPOCH - Duration::new(positive_secs, 0)
            }
        }
    }

    pub fn checked_add_duration(&self, other: &Duration) -> Option<WallClockInstant> {
        let mut secs = self.secs.checked_add_unsigned(other.as_secs())?;
        let mut micros = other.subsec_micros() + self.micros;
        if micros >= MICROS_PER_SEC {
            micros -= MICROS_PER_SEC;
            secs = secs.checked_add(1)?;
        }
        Some(Self { secs, micros })
    }

    pub fn checked_sub_duration(&self, other: &Duration) -> Option<WallClockInstant> {
        let mut secs = self.secs.checked_sub_unsigned(other.as_secs())?;
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

impl std::ops::Add<Duration> for WallClockInstant {
    type Output = WallClockInstant;

    fn add(self, rhs: Duration) -> Self::Output {
        self.checked_add_duration(&rhs)
            .expect("duration addition overflow")
    }
}

impl std::ops::Sub<Duration> for WallClockInstant {
    type Output = WallClockInstant;

    fn sub(self, rhs: Duration) -> Self::Output {
        self.checked_sub_duration(&rhs)
            .expect("duration subtraction underflow")
    }
}

impl<T: chrono::TimeZone> From<chrono::DateTime<T>> for WallClockInstant {
    fn from(value: chrono::DateTime<T>) -> Self {
        WallClockInstant {
            secs: value.timestamp(),
            micros: value.timestamp_subsec_micros(),
        }
    }
}

pub trait Clock {
    /// Monotonic time for timeout checking and elapsed time measurement.
    /// Cheap on real systems (reads TSC), controllable in simulation.
    fn current_time_monotonic(&self) -> MonotonicInstant;

    /// Wall-clock time for timestamps (WAL, datetime functions).
    /// Controllable in simulation for deterministic behavior.
    fn current_time_wall_clock(&self) -> WallClockInstant;
}

pub struct DefaultClock;

impl Clock for DefaultClock {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        MonotonicInstant::now()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        WallClockInstant::now()
    }
}
