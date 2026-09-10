use ahash::AHashMap;
use rand::{rngs::StdRng, Rng, SeedableRng};
use rand_distr::Exp;
use tantivy_stacker::ArenaHashMap;

fn main() {
    for _ in 0..1_000_000 {
        let seed: u64 = rand::random();
        test_with_seed(seed);
    }
}

fn test_with_seed(seed: u64) {
    let mut hash_map = AHashMap::new();
    let mut arena_hashmap = ArenaHashMap::default();
    let mut rng = StdRng::seed_from_u64(seed);
    let key_count = rng.random_range(1_000..=1_000_000);
    let exp = Exp::new(0.05).unwrap();

    for _ in 0..key_count {
        let key_length = rng.sample::<f32, _>(exp).min(u16::MAX as f32).max(1.0) as usize;

        let key: Vec<u8> = (0..key_length).map(|_| rng.gen()).collect();

        arena_hashmap.mutate_or_create(&key, |current_count| {
            let count: u64 = current_count.unwrap_or(0);
            count + 1
        });
        hash_map.entry(key).and_modify(|e| *e += 1).or_insert(1);
    }

    println!(
        "Seed: {} \t {:.2}MB",
        seed,
        arena_hashmap.memory_arena.len() as f32 / 1024.0 / 1024.0
    );
    // Check the contents of the ArenaHashMap
    for (key, addr) in arena_hashmap.iter() {
        let count: u64 = arena_hashmap.read(addr);
        let count_expected = hash_map
            .get(key)
            .unwrap_or_else(|| panic!("NOT FOUND: Key: {:?}, Count: {}", key, count));
        assert_eq!(count, *count_expected);
    }
}
