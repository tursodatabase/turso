//! Functions that the swarm testing of whopper and of limbo_sim share. Swarm
//! testing picks the workloads, their weights and the faults of a run from
//! its seed.

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use super::pick;

/// Weight multipliers in quarters: 1/4, 1/2, 1, 2 and 4.
const WEIGHT_QUARTERS: [u32; 5] = [1, 2, 4, 8, 16];

/// The random number generator of stream `stream` of `seed`. A swarm draws its
/// choices from a stream that the simulation does not use, so that the choices
/// do not change the random values of the simulation.
pub fn seeded_stream(seed: u64, stream: u64) -> ChaCha8Rng {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    rng.set_stream(stream);
    rng
}

/// `weight` multiplied by 1/4, 1/2, 1, 2 or 4, rounded up. A weight of 0
/// stays 0, and a weight of 1 or more stays 1 or more.
pub fn scale_weight<R: Rng + ?Sized>(weight: u32, rng: &mut R) -> u32 {
    (weight * pick(&WEIGHT_QUARTERS, rng)).div_ceil(4)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use rand::RngCore;

    use super::*;

    #[test]
    fn scale_weight_multiplies_by_a_quarter_to_four() {
        let mut rng = seeded_stream(1, 1);
        let scaled: BTreeSet<u32> = (0..200).map(|_| scale_weight(8, &mut rng)).collect();
        assert_eq!(scaled, BTreeSet::from([2, 4, 8, 16, 32]));
        for _ in 0..200 {
            assert_eq!(scale_weight(0, &mut rng), 0);
            assert!(scale_weight(1, &mut rng) >= 1);
        }
    }

    #[test]
    fn each_stream_of_a_seed_repeats_and_differs_from_the_others() {
        let values = |seed, stream| -> Vec<u64> {
            let mut rng = seeded_stream(seed, stream);
            (0..4).map(|_| rng.next_u64()).collect()
        };
        assert_eq!(values(7, 1), values(7, 1));
        assert_ne!(values(7, 0), values(7, 1));
        assert_ne!(values(7, 1), values(7, 2));
        assert_eq!(values(7, 0), {
            let mut rng = ChaCha8Rng::seed_from_u64(7);
            (0..4).map(|_| rng.next_u64()).collect::<Vec<_>>()
        });
    }
}
