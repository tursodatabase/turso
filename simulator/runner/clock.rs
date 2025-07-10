use std::cell::RefCell;

use chrono::{DateTime, Utc};
use rand::Rng;
use rand_chacha::ChaCha8Rng;

#[derive(Debug)]
pub struct SimulatorClock {
    curr_time: RefCell<DateTime<Utc>>,
    rng: RefCell<ChaCha8Rng>,
}

impl SimulatorClock {
    const MIN_TICK: u64 = 1;
    const MAX_TICK: u64 = 30;

    pub fn new(rng: ChaCha8Rng) -> Self {
        Self {
            curr_time: RefCell::new(Utc::now()),
            rng: RefCell::new(rng),
        }
    }

    pub fn now(&self) -> DateTime<Utc> {
        let mut time = self.curr_time.borrow_mut();
        let nanos = self
            .rng
            .borrow_mut()
            .gen_range(Self::MIN_TICK..Self::MAX_TICK);
        let nanos = std::time::Duration::from_micros(nanos);
        *time += nanos;
        *time
    }
}
