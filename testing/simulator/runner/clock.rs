use std::cell::RefCell;

use chrono::{DateTime, Utc};
use rand::Rng;
use rand_chacha::ChaCha8Rng;

#[derive(Debug)]
pub struct SimulatorClock {
    curr_time: RefCell<DateTime<Utc>>,
    rng: RefCell<ChaCha8Rng>,
    min_tick: u64,
    max_tick: u64,
}

impl SimulatorClock {
    pub fn new(rng: ChaCha8Rng, min_tick: u64, max_tick: u64) -> Self {
        Self {
            curr_time: RefCell::new(Utc::now()),
            rng: RefCell::new(rng),
            min_tick,
            max_tick,
        }
    }

    pub fn now(&self) -> DateTime<Utc> {
        let mut time = self.curr_time.borrow_mut();
        let nanos = self
            .rng
            .borrow_mut()
            .random_range(self.min_tick..self.max_tick);
        let nanos = std::time::Duration::from_micros(nanos);
        *time += nanos;
        *time
    }
}
