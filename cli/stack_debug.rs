//! Stack-usage watermark harness for `.stack-debug on` mode.
//!
//! Primes a region of stack memory below the current sp with a sentinel byte,
//! runs the caller-supplied closure, then scans the primed region for the
//! deepest byte the closure touched. The number reported is a lower bound on
//! how much stack the closure actually consumed (we detect writes, not raw
//! reservation), so the value is a "signal, not a measurement" — useful for
//! relative comparison between queries.

const SENTINEL: u8 = 0xAA;
const SCAN_REGION: usize = 512 * 1024;
const PRIME_FRAME_BYTES: usize = 4 * 1024;

#[inline(never)]
fn sp_addr() -> usize {
    let probe: u32 = 0;
    let addr = &probe as *const u32 as usize;
    std::hint::black_box(addr)
}

#[inline(never)]
fn prime_to(target_low: usize) -> u8 {
    let mut buf = [0u8; PRIME_FRAME_BYTES];
    let buf_start = buf.as_ptr() as usize;
    for i in 0..buf.len() {
        unsafe {
            std::ptr::write_volatile(buf.as_mut_ptr().add(i), SENTINEL);
        }
    }
    let mut acc: u8 = 0;
    if buf_start > target_low {
        acc = prime_to(target_low);
    }
    // Read buf after the recursion so the compiler cannot tail-call.
    acc.wrapping_add(unsafe { std::ptr::read_volatile(buf.as_ptr()) })
}

#[inline(never)]
fn scan_deepest(target_low: usize, sp_top: usize) -> usize {
    // Walk down from sp_top, looking for a run of WINDOW consecutive sentinel
    // bytes. That run marks the transition from the closure's touched region
    // into primed territory; the deepest touched byte is just above it.
    const WINDOW: usize = 1024;
    let mut run = 0usize;
    let mut addr = sp_top;
    while addr > target_low {
        addr -= 1;
        let byte = unsafe { std::ptr::read_volatile(addr as *const u8) };
        if byte == SENTINEL {
            run += 1;
            if run >= WINDOW {
                return sp_top.saturating_sub(addr + WINDOW);
            }
        } else {
            run = 0;
        }
    }
    sp_top.saturating_sub(target_low)
}

#[inline(never)]
pub fn measure<F: FnOnce()>(f: F) -> usize {
    let sp_top = sp_addr();
    let target_low = sp_top.saturating_sub(SCAN_REGION);
    std::hint::black_box(prime_to(target_low));
    f();
    scan_deepest(target_low, sp_top)
}
