use tantivy_stacker::ArenaHashMap;

const ALICE: &str = include_str!("../../benches/alice.txt");

fn main() {
    create_hash_map((0..100_000_000).map(|el| el.to_string()));

    for _ in 0..1000 {
        create_hash_map(ALICE.split_whitespace());
    }
}

fn create_hash_map<T: AsRef<str>>(terms: impl Iterator<Item = T>) -> ArenaHashMap {
    let mut map = ArenaHashMap::with_capacity(4);
    for term in terms {
        map.mutate_or_create(term.as_ref().as_bytes(), |val| {
            if let Some(mut val) = val {
                val += 1;
                val
            } else {
                1u64
            }
        });
    }

    map
}
