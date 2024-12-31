use chrono::{DateTime, Utc};
use std::time::UNIX_EPOCH;

#[derive(Debug)]
pub enum Clock {
    Sim(DateTime<Utc>),
    Real,
}

impl Clock {
    pub fn sim_clock() -> Self {
        Clock::Sim(UNIX_EPOCH.into())
    }

    pub fn now(&self) -> DateTime<Utc> {
        match self {
            Clock::Sim(now) => *now,
            Clock::Real => chrono::Local::now().to_utc(),
        }
    }
}
