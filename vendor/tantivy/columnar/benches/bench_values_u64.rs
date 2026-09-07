use std::ops::RangeInclusive;
use std::sync::Arc;

use binggan::{InputGroup, black_box};
use rand::prelude::*;
use tantivy_columnar::column_values::{CodecType, serialize_and_load_u64_based_column_values};
use tantivy_columnar::*;

// Warning: this generates the same permutation at each call
fn generate_permutation() -> Vec<u64> {
    let mut permutation: Vec<u64> = (0u64..100_000u64).collect();
    permutation.shuffle(&mut StdRng::from_seed([1u8; 32]));
    permutation
}

// Warning: this generates the same permutation at each call
fn generate_permutation_gcd() -> Vec<u64> {
    let mut permutation: Vec<u64> = (1u64..100_000u64).map(|el| el * 1000).collect();
    permutation.shuffle(&mut StdRng::from_seed([1u8; 32]));
    permutation
}

pub fn serialize_and_load(column: &[u64], codec_type: CodecType) -> Arc<dyn ColumnValues<u64>> {
    serialize_and_load_u64_based_column_values(&column, &[codec_type])
}

const FIFTY_PERCENT_RANGE: RangeInclusive<u64> = 1..=50;
const SINGLE_ITEM: u64 = 90;
const SINGLE_ITEM_RANGE: RangeInclusive<u64> = 90..=90;
const ONE_PERCENT_ITEM_RANGE: RangeInclusive<u64> = 49..=49;

fn get_data_50percent_item() -> Vec<u128> {
    let mut rng = StdRng::from_seed([1u8; 32]);

    let mut data = vec![];
    for _ in 0..300_000 {
        let val = rng.random_range(1..=100);
        data.push(val);
    }
    data.push(SINGLE_ITEM);

    data.shuffle(&mut rng);
    data.iter().map(|el| *el as u128).collect::<Vec<_>>()
}

type VecCol = (Vec<u64>, Arc<dyn ColumnValues<u64>>);

fn bench_access() {
    let permutation = generate_permutation();
    let column_perm: Arc<dyn ColumnValues<u64>> =
        serialize_and_load(&permutation, CodecType::Bitpacked);

    let permutation_gcd = generate_permutation_gcd();
    let column_perm_gcd: Arc<dyn ColumnValues<u64>> =
        serialize_and_load(&permutation_gcd, CodecType::Bitpacked);

    let mut group: InputGroup<VecCol> = InputGroup::new_with_inputs(vec![
        (
            "access".to_string(),
            (permutation.clone(), column_perm.clone()),
        ),
        (
            "access_gcd".to_string(),
            (permutation_gcd.clone(), column_perm_gcd.clone()),
        ),
    ]);

    group.register("stride7_vec", |inp: &VecCol| {
        let n = inp.0.len();
        let mut a = 0u64;
        for i in (0..n / 7).map(|val| val * 7) {
            a += inp.0[i];
        }
        black_box(a);
    });

    group.register("fullscan_vec", |inp: &VecCol| {
        let mut a = 0u64;
        for i in 0..inp.0.len() {
            a += inp.0[i];
        }
        black_box(a);
    });

    group.register("stride7_column_values", |inp: &VecCol| {
        let n = inp.1.num_vals() as usize;
        let mut a = 0u64;
        for i in (0..n / 7).map(|val| val * 7) {
            a += inp.1.get_val(i as u32);
        }
        black_box(a);
    });

    group.register("fullscan_column_values", |inp: &VecCol| {
        let mut a = 0u64;
        let n = inp.1.num_vals() as usize;
        for i in 0..n {
            a += inp.1.get_val(i as u32);
        }
        black_box(a);
    });

    group.run();
}

fn bench_range() {
    let data_50 = get_data_50percent_item();
    let data_u64 = data_50.iter().map(|el| *el as u64).collect::<Vec<_>>();
    let column_data: Arc<dyn ColumnValues<u64>> =
        serialize_and_load(&data_u64, CodecType::Bitpacked);

    let mut group: InputGroup<Arc<dyn ColumnValues<u64>>> =
        InputGroup::new_with_inputs(vec![("dist_50pct_item".to_string(), column_data.clone())]);

    group.register(
        "fastfield_getrange_u64_50percent_hit",
        |col: &Arc<dyn ColumnValues<u64>>| {
            let mut positions = Vec::new();
            col.get_row_ids_for_value_range(FIFTY_PERCENT_RANGE, 0..col.num_vals(), &mut positions);
            black_box(positions.len());
        },
    );

    group.register(
        "fastfield_getrange_u64_1percent_hit",
        |col: &Arc<dyn ColumnValues<u64>>| {
            let mut positions = Vec::new();
            col.get_row_ids_for_value_range(
                ONE_PERCENT_ITEM_RANGE,
                0..col.num_vals(),
                &mut positions,
            );
            black_box(positions.len());
        },
    );

    group.register(
        "fastfield_getrange_u64_single_hit",
        |col: &Arc<dyn ColumnValues<u64>>| {
            let mut positions = Vec::new();
            col.get_row_ids_for_value_range(SINGLE_ITEM_RANGE, 0..col.num_vals(), &mut positions);
            black_box(positions.len());
        },
    );

    group.register(
        "fastfield_getrange_u64_hit_all",
        |col: &Arc<dyn ColumnValues<u64>>| {
            let mut positions = Vec::new();
            col.get_row_ids_for_value_range(0..=u64::MAX, 0..col.num_vals(), &mut positions);
            black_box(positions.len());
        },
    );

    group.run();
}

fn main() {
    bench_access();
    bench_range();
}
