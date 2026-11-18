use std::hint::black_box;

const SMALL_STACK: usize = 256 * 1024;
const DEPTH: u64 = 100_000;

#[turso_macros::grow_stack_when_low]
fn count_levels(remaining: u64) -> u64 {
    if remaining == 0 {
        return 0;
    }
    1 + black_box(count_levels(remaining - 1))
}

struct Counter {
    calls: u64,
}

impl Counter {
    #[turso_macros::grow_stack_when_low]
    fn count_calls(&mut self, remaining: u64) {
        self.calls += 1;
        if remaining > 0 {
            self.count_calls(black_box(remaining - 1));
        }
    }
}

#[test]
fn function_recurses_deeper_than_the_thread_stack() {
    assert_eq!(on_small_stack(|| count_levels(DEPTH)), DEPTH);
}

#[test]
fn method_recurses_deeper_than_the_thread_stack() {
    let calls = on_small_stack(|| {
        let mut counter = Counter { calls: 0 };
        counter.count_calls(DEPTH);
        counter.calls
    });
    assert_eq!(calls, DEPTH + 1);
}

fn on_small_stack<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    std::thread::Builder::new()
        .stack_size(SMALL_STACK)
        .spawn(work)
        .expect("failed to spawn the test thread")
        .join()
        .expect("the test thread panicked")
}
