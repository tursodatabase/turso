/// Commit-txn wall latency. Samples are nanoseconds from BEGIN start to COMMIT Done.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct TxnLatency {
    pub n: u64,
    pub mean_us: f64,
    pub p50_us: f64,
    pub p95_us: f64,
    pub p99_us: f64,
}

impl TxnLatency {
    pub fn from_nanos(samples_ns: &[u64]) -> Self {
        if samples_ns.is_empty() {
            return Self::default();
        }
        let mut xs = samples_ns.to_vec();
        xs.sort_unstable();
        let n = xs.len() as u64;
        let sum: u128 = xs.iter().map(|&v| u128::from(v)).sum();
        let mean_ns = sum as f64 / n as f64;
        Self {
            n,
            mean_us: mean_ns / 1_000.0,
            p50_us: percentile_ns(&xs, 0.50) / 1_000.0,
            p95_us: percentile_ns(&xs, 0.95) / 1_000.0,
            p99_us: percentile_ns(&xs, 0.99) / 1_000.0,
        }
    }
}

fn percentile_ns(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    if sorted.len() == 1 {
        return sorted[0] as f64;
    }
    let idx = p * (sorted.len() - 1) as f64;
    let lo = idx.floor() as usize;
    let hi = idx.ceil() as usize;
    if lo == hi {
        sorted[lo] as f64
    } else {
        let w = idx - lo as f64;
        sorted[lo] as f64 * (1.0 - w) + sorted[hi] as f64 * w
    }
}

#[cfg(test)]
mod tests {
    use super::TxnLatency;

    #[test]
    fn p50_of_even_samples() {
        let lat = TxnLatency::from_nanos(&[1_000, 2_000, 3_000, 4_000]);
        assert_eq!(lat.n, 4);
        assert!((lat.p50_us - 2.5).abs() < 1e-9);
        assert!((lat.mean_us - 2.5).abs() < 1e-9);
    }
}
