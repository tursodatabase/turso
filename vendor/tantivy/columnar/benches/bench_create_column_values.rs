use binggan::{InputGroup, black_box};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tantivy_columnar::column_values::{CodecType, serialize_u64_based_column_values};

fn get_data() -> Vec<u64> {
    let mut rng = StdRng::seed_from_u64(2u64);
    let mut data: Vec<_> = (100..55_000_u64)
        .map(|num| num + rng.random::<u8>() as u64)
        .collect();
    data.push(99_000);
    data.insert(1000, 2000);
    data.insert(2000, 100);
    data.insert(3000, 4100);
    data.insert(4000, 100);
    data.insert(5000, 800);
    data
}

fn main() {
    let data = get_data();
    let mut group: InputGroup<(CodecType, Vec<u64>)> = InputGroup::new_with_inputs(vec![
        (
            "bitpacked codec".to_string(),
            (CodecType::Bitpacked, data.clone()),
        ),
        (
            "linear codec".to_string(),
            (CodecType::Linear, data.clone()),
        ),
        (
            "blockwise linear codec".to_string(),
            (CodecType::BlockwiseLinear, data.clone()),
        ),
    ]);

    group.register("serialize column_values", |data| {
        let mut buffer = Vec::new();
        serialize_u64_based_column_values(&data.1.as_slice(), &[data.0], &mut buffer).unwrap();
        black_box(buffer.len());
    });

    group.run();
}
