mod datetime;
mod likeop;
mod numeric;
mod value;

use divan::AllocProfiler;
use mimalloc::MiMalloc;

#[global_allocator]
static ALLOC: AllocProfiler<MiMalloc> = AllocProfiler::new(MiMalloc);

fn main() {
    divan::main();
}
