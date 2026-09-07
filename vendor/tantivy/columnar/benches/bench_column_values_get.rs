use std::sync::Arc;

use binggan::{InputGroup, black_box};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tantivy_columnar::ColumnValues;
use tantivy_columnar::column_values::{CodecType, serialize_and_load_u64_based_column_values};

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

#[inline(never)]
fn value_iter() -> impl Iterator<Item = u64> {
    0..20_000
}

type Col = Arc<dyn ColumnValues<u64>>;

fn main() {
    let data = get_data();
    let inputs: Vec<(String, Col)> = vec![
        (
            "bitpacked".to_string(),
            serialize_and_load_u64_based_column_values(&data.as_slice(), &[CodecType::Bitpacked]),
        ),
        (
            "linear".to_string(),
            serialize_and_load_u64_based_column_values(&data.as_slice(), &[CodecType::Linear]),
        ),
        (
            "blockwise_linear".to_string(),
            serialize_and_load_u64_based_column_values(
                &data.as_slice(),
                &[CodecType::BlockwiseLinear],
            ),
        ),
    ];

    let mut group: InputGroup<Col> = InputGroup::new_with_inputs(inputs);

    group.register("fastfield_get", |col: &Col| {
        let mut sum = 0u64;
        for pos in value_iter() {
            sum = sum.wrapping_add(col.get_val(pos as u32));
        }
        black_box(sum);
    });

    group.run();
}
