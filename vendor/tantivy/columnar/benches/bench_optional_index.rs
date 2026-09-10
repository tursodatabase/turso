use binggan::{InputGroup, black_box};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tantivy_columnar::column_index::{OptionalIndex, Set};

const TOTAL_NUM_VALUES: u32 = 1_000_000;

fn gen_optional_index(fill_ratio: f64) -> OptionalIndex {
    let mut rng: StdRng = StdRng::from_seed([1u8; 32]);
    let vals: Vec<u32> = (0..TOTAL_NUM_VALUES)
        .map(|_| rng.random_bool(fill_ratio))
        .enumerate()
        .filter(|(_pos, val)| *val)
        .map(|(pos, _)| pos as u32)
        .collect();
    OptionalIndex::for_test(TOTAL_NUM_VALUES, &vals)
}

fn random_range_iterator(
    start: u32,
    end: u32,
    avg_step_size: u32,
    avg_deviation: u32,
) -> impl Iterator<Item = u32> {
    let mut rng: StdRng = StdRng::from_seed([1u8; 32]);
    let mut current = start;
    std::iter::from_fn(move || {
        current += rng.random_range(avg_step_size - avg_deviation..=avg_step_size + avg_deviation);
        if current >= end { None } else { Some(current) }
    })
}

fn n_percent_step_iterator(percent: f32, num_values: u32) -> impl Iterator<Item = u32> {
    let ratio = percent / 100.0;
    let step_size = (1f32 / ratio) as u32;
    let deviation = step_size - 1;
    random_range_iterator(0, num_values, step_size, deviation)
}

fn walk_over_data(codec: &OptionalIndex, avg_step_size: u32) -> Option<u32> {
    walk_over_data_from_positions(
        codec,
        random_range_iterator(0, TOTAL_NUM_VALUES, avg_step_size, 0),
    )
}

fn walk_over_data_from_positions(
    codec: &OptionalIndex,
    positions: impl Iterator<Item = u32>,
) -> Option<u32> {
    let mut dense_idx: Option<u32> = None;
    for idx in positions {
        dense_idx = dense_idx.or(codec.rank_if_exists(idx));
    }
    dense_idx
}

fn main() {
    // Build separate inputs for each fill ratio.
    let inputs: Vec<(String, OptionalIndex)> = vec![
        ("fill=1%".to_string(), gen_optional_index(0.01)),
        ("fill=5%".to_string(), gen_optional_index(0.05)),
        ("fill=10%".to_string(), gen_optional_index(0.10)),
        ("fill=50%".to_string(), gen_optional_index(0.50)),
        ("fill=90%".to_string(), gen_optional_index(0.90)),
    ];

    let mut group: InputGroup<OptionalIndex> = InputGroup::new_with_inputs(inputs);

    // Translate orig->codec (rank_if_exists) with sampling
    group.register("orig_to_codec_10pct_hit", |codec: &OptionalIndex| {
        black_box(walk_over_data(codec, 100));
    });
    group.register("orig_to_codec_1pct_hit", |codec: &OptionalIndex| {
        black_box(walk_over_data(codec, 1000));
    });
    group.register("orig_to_codec_full_scan", |codec: &OptionalIndex| {
        black_box(walk_over_data_from_positions(codec, 0..TOTAL_NUM_VALUES));
    });

    // Translate codec->orig (select/select_batch) on sampled ranks
    fn bench_translate_codec_to_orig_util(codec: &OptionalIndex, percent_hit: f32) {
        let num_non_nulls = codec.num_non_nulls();
        let idxs: Vec<u32> = if percent_hit == 100.0f32 {
            (0..num_non_nulls).collect()
        } else {
            n_percent_step_iterator(percent_hit, num_non_nulls).collect()
        };
        let mut output = vec![0u32; idxs.len()];
        output.copy_from_slice(&idxs[..]);
        codec.select_batch(&mut output);
        black_box(output);
    }

    group.register("codec_to_orig_0.005pct_hit", |codec: &OptionalIndex| {
        bench_translate_codec_to_orig_util(codec, 0.005);
    });
    group.register("codec_to_orig_10pct_hit", |codec: &OptionalIndex| {
        bench_translate_codec_to_orig_util(codec, 10.0);
    });
    group.register("codec_to_orig_full_scan", |codec: &OptionalIndex| {
        bench_translate_codec_to_orig_util(codec, 100.0);
    });

    group.run();
}
