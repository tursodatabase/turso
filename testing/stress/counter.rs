use turso_stress::sync::{Arc, Mutex};

/// StressCounter wraps an Arc, so even after being cloned it still refers to the same state.
#[derive(Clone)]
pub struct StressCounter {
    inner: Arc<Mutex<Inner>>,
}

struct Inner {
    iteration_counts: Vec<usize>,
    completed_count: usize,
    iteration_limit: usize,
}

impl StressCounter {
    pub fn new(num_threads: usize, iteration_limit: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                iteration_counts: vec![0; num_threads],
                completed_count: 0,
                iteration_limit,
            })),
        }
    }

    pub fn register_iterations(&mut self, thread_idx: usize, iterations: usize) {
        let mut inner = self.inner.lock().unwrap();

        inner.iteration_counts[thread_idx] += iterations;
        if inner.iteration_counts[thread_idx] >= inner.iteration_limit {
            inner.completed_count += 1;
        }
    }

    pub fn iteration_idx(&self, thread_idx: usize) -> usize {
        let inner = self.inner.lock().unwrap();

        inner.iteration_counts[thread_idx]
    }

    pub fn all_done(&self) -> bool {
        let inner = self.inner.lock().unwrap();

        inner.completed_count == inner.iteration_counts.len()
    }

    pub fn done(&self, thread_idx: usize) -> bool {
        let inner = self.inner.lock().unwrap();

        inner.iteration_counts[thread_idx] >= inner.iteration_limit
    }

    pub fn incomplete_threads(&self) -> usize {
        let inner = self.inner.lock().unwrap();

        inner.iteration_counts.len() - inner.completed_count
    }
}
