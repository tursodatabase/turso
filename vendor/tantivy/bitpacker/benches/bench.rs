#![feature(test)]

extern crate test;

#[cfg(test)]
mod tests {
    use rand::rng;
    use rand::seq::IteratorRandom;
    use tantivy_bitpacker::{BitPacker, BitUnpacker, BlockedBitpacker};
    use test::Bencher;

    #[inline(never)]
    fn create_bitpacked_data(bit_width: u8, num_els: u32) -> Vec<u8> {
        let mut bitpacker = BitPacker::new();
        let mut buffer = Vec::new();
        for _ in 0..num_els {
            // the values do not matter.
            bitpacker.write(0u64, bit_width, &mut buffer).unwrap();
            bitpacker.flush(&mut buffer).unwrap();
        }
        buffer
    }

    #[bench]
    fn bench_bitpacking_read(b: &mut Bencher) {
        let bit_width = 3;
        let num_els = 1_000_000u32;
        let bit_unpacker = BitUnpacker::new(bit_width);
        let data = create_bitpacked_data(bit_width, num_els);
        let idxs: Vec<u32> = (0..num_els).choose_multiple(&mut rng(), 100_000);
        b.iter(|| {
            let mut out = 0u64;
            for &idx in &idxs {
                out = out.wrapping_add(bit_unpacker.get(idx, &data[..]));
            }
            out
        });
    }

    #[bench]
    fn bench_blockedbitp_read(b: &mut Bencher) {
        let mut blocked_bitpacker = BlockedBitpacker::new();
        for val in 0..=21500 {
            blocked_bitpacker.add(val * val);
        }
        b.iter(|| {
            let mut out = 0u64;
            for val in 0..=21500 {
                out = out.wrapping_add(blocked_bitpacker.get(val));
            }
            out
        });
    }

    #[bench]
    fn bench_blockedbitp_create(b: &mut Bencher) {
        b.iter(|| {
            let mut blocked_bitpacker = BlockedBitpacker::new();
            for val in 0..=21500 {
                blocked_bitpacker.add(val * val);
            }
            blocked_bitpacker
        });
    }
}
