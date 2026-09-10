use binggan::plugins::PeakMemAllocPlugin;
use binggan::{BenchRunner, INSTRUMENTED_SYSTEM, PeakMemAlloc, black_box};
use rand::SeedableRng;
use rustc_hash::FxHashMap;
use tantivy_stacker::{ArenaHashMap, ExpUnrolledLinkedList, MemoryArena};

const ALICE: &str = include_str!("../../benches/alice.txt");

#[global_allocator]
pub static GLOBAL: &PeakMemAlloc<std::alloc::System> = &INSTRUMENTED_SYSTEM;

fn bench_vint() {
    let mut runner = BenchRunner::new();
    // Set the peak mem allocator. This will enable peak memory reporting.
    runner.add_plugin(PeakMemAllocPlugin::new(GLOBAL));

    {
        let input_bytes = ALICE.len();

        let alice_terms_as_bytes: Vec<&[u8]> = ALICE
            .split_ascii_whitespace()
            .map(|el| el.as_bytes())
            .collect();

        let alice_terms_as_bytes_with_docid: Vec<(u32, &[u8])> = ALICE
            .split_ascii_whitespace()
            .map(|el| el.as_bytes())
            .enumerate()
            .map(|(docid, el)| (docid as u32, el))
            .collect();

        // Alice benchmark
        let mut group = runner.new_group();
        group.set_name(format!("alice (num terms: {})", ALICE.len()));
        group.set_input_size(input_bytes);
        group.register_with_input("hashmap", &alice_terms_as_bytes, move |data| {
            black_box(create_hash_map(data.iter()));
        });
        group.register_with_input(
            "hasmap with postings",
            &alice_terms_as_bytes_with_docid,
            move |data| {
                black_box(create_hash_map_with_expull(data.iter().cloned()));
            },
        );
        group.register_with_input(
            "fxhashmap ref postings",
            &alice_terms_as_bytes,
            move |data| {
                black_box(create_fx_hash_ref_map_with_expull(data.iter().cloned()));
            },
        );
        group.register_with_input(
            "fxhasmap owned postings",
            &alice_terms_as_bytes,
            move |data| {
                black_box(create_fx_hash_owned_map_with_expull(data.iter().cloned()));
            },
        );
        group.run();
    }

    {
        for (num_numbers, num_numbers_label) in [
            (100_000u64, "100k"),
            (1_000_000, "1mio"),
            (2_000_000, "2mio"),
            (5_000_000, "5mio"),
        ] {
            // benchmark unique numbers
            {
                let numbers: Vec<[u8; 8]> = (0..num_numbers).map(|el| el.to_le_bytes()).collect();
                let numbers_with_doc: Vec<_> = numbers
                    .iter()
                    .enumerate()
                    .map(|(docid, el)| (docid as u32, el))
                    .collect();

                let input_bytes = numbers.len() * 8;
                let mut group = runner.new_group();
                group.set_name(format!("numbers unique {}", num_numbers_label));
                group.set_input_size(input_bytes);
                group.register_with_input("only hashmap", &numbers, move |data| {
                    black_box(create_hash_map(data.iter()));
                });
                group.register_with_input("hasmap with postings", &numbers_with_doc, move |data| {
                    black_box(create_hash_map_with_expull(data.iter().cloned()));
                });
                group.run();
            }
            // benchmark zipfs distribution numbers
            {
                use rand::distr::Distribution;
                use rand::rngs::StdRng;
                let mut rng = StdRng::from_seed([3u8; 32]);
                let zipf = rand_distr::Zipf::new(10_000.0f64, 1.03).unwrap();
                let numbers: Vec<[u8; 8]> = (0..num_numbers)
                    .map(|_| zipf.sample(&mut rng).to_le_bytes())
                    .collect();
                let numbers_with_doc: Vec<_> = numbers
                    .iter()
                    .enumerate()
                    .map(|(docid, el)| (docid as u32, el))
                    .collect();

                let input_bytes = numbers.len() * 8;
                let mut group = runner.new_group();
                group.set_name(format!("zipfs numbers {}", num_numbers_label));
                group.set_input_size(input_bytes);
                group.register_with_input("hashmap", &numbers, move |data| {
                    black_box(create_hash_map(data.iter()));
                });
                group.register_with_input("hasmap with postings", &numbers_with_doc, move |data| {
                    black_box(create_hash_map_with_expull(data.iter().cloned()));
                });
                group.run();
            }
        }
    }
}

fn main() {
    bench_vint();
}

const HASHMAP_CAPACITY: usize = 1 << 15;

/// Only records the doc ids
#[derive(Clone, Default, Copy)]
pub struct DocIdRecorder {
    stack: ExpUnrolledLinkedList,
}
impl DocIdRecorder {
    fn new_doc(&mut self, doc: u32, arena: &mut MemoryArena) {
        self.stack.writer(arena).write_u32_vint(doc);
    }
}

fn create_hash_map<T: AsRef<[u8]>>(terms: impl Iterator<Item = T>) -> ArenaHashMap {
    let mut map = ArenaHashMap::with_capacity(HASHMAP_CAPACITY);
    for term in terms {
        map.mutate_or_create(term.as_ref(), |val| {
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

fn create_hash_map_with_expull<T: AsRef<[u8]>>(
    terms: impl Iterator<Item = (u32, T)>,
) -> ArenaHashMap {
    let mut memory_arena = MemoryArena::default();
    let mut map = ArenaHashMap::with_capacity(HASHMAP_CAPACITY);
    for (i, term) in terms {
        map.mutate_or_create(term.as_ref(), |val: Option<DocIdRecorder>| {
            if let Some(mut rec) = val {
                rec.new_doc(i, &mut memory_arena);
                rec
            } else {
                DocIdRecorder::default()
            }
        });
    }

    map
}

fn create_fx_hash_ref_map_with_expull(
    terms: impl Iterator<Item = &'static [u8]>,
) -> FxHashMap<&'static [u8], Vec<u32>> {
    let terms = terms.enumerate();
    let mut map = FxHashMap::with_capacity_and_hasher(HASHMAP_CAPACITY, Default::default());
    for (i, term) in terms {
        map.entry(term.as_ref())
            .or_insert_with(Vec::new)
            .push(i as u32);
    }
    map
}

fn create_fx_hash_owned_map_with_expull(
    terms: impl Iterator<Item = &'static [u8]>,
) -> FxHashMap<Vec<u8>, Vec<u32>> {
    let terms = terms.enumerate();
    let mut map = FxHashMap::with_capacity_and_hasher(HASHMAP_CAPACITY, Default::default());
    for (i, term) in terms {
        map.entry(term.as_ref().to_vec())
            .or_insert_with(Vec::new)
            .push(i as u32);
    }
    map
}
