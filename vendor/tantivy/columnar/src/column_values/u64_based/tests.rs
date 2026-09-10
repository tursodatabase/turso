use proptest::prelude::*;
use proptest::{prop_oneof, proptest};
use rand::Rng;

#[test]
fn test_serialize_and_load_simple() {
    let mut buffer = Vec::new();
    let vals = &[1u64, 2u64, 5u64];
    serialize_u64_based_column_values(
        &&vals[..],
        &[CodecType::Bitpacked, CodecType::BlockwiseLinear],
        &mut buffer,
    )
    .unwrap();
    assert_eq!(buffer.len(), 7);
    let col = load_u64_based_column_values::<u64>(OwnedBytes::new(buffer)).unwrap();
    assert_eq!(col.num_vals(), 3);
    assert_eq!(col.get_val(0), 1);
    assert_eq!(col.get_val(1), 2);
    assert_eq!(col.get_val(2), 5);
}

#[test]
fn test_empty_column_i64() {
    let vals: [i64; 0] = [];
    let mut num_acceptable_codecs = 0;
    for codec in ALL_U64_CODEC_TYPES {
        let mut buffer = Vec::new();
        if serialize_u64_based_column_values(&&vals[..], &[codec], &mut buffer).is_err() {
            continue;
        }
        num_acceptable_codecs += 1;
        let col = load_u64_based_column_values::<i64>(OwnedBytes::new(buffer)).unwrap();
        assert_eq!(col.num_vals(), 0);
        assert_eq!(col.min_value(), i64::MIN);
        assert_eq!(col.max_value(), i64::MIN);
    }
    assert!(num_acceptable_codecs > 0);
}

#[test]
fn test_empty_column_u64() {
    let vals: [u64; 0] = [];
    let mut num_acceptable_codecs = 0;
    for codec in ALL_U64_CODEC_TYPES {
        let mut buffer = Vec::new();
        if serialize_u64_based_column_values(&&vals[..], &[codec], &mut buffer).is_err() {
            continue;
        }
        num_acceptable_codecs += 1;
        let col = load_u64_based_column_values::<u64>(OwnedBytes::new(buffer)).unwrap();
        assert_eq!(col.num_vals(), 0);
        assert_eq!(col.min_value(), u64::MIN);
        assert_eq!(col.max_value(), u64::MIN);
    }
    assert!(num_acceptable_codecs > 0);
}

#[test]
fn test_empty_column_f64() {
    let vals: [f64; 0] = [];
    let mut num_acceptable_codecs = 0;
    for codec in ALL_U64_CODEC_TYPES {
        let mut buffer = Vec::new();
        if serialize_u64_based_column_values(&&vals[..], &[codec], &mut buffer).is_err() {
            continue;
        }
        num_acceptable_codecs += 1;
        let col = load_u64_based_column_values::<f64>(OwnedBytes::new(buffer)).unwrap();
        assert_eq!(col.num_vals(), 0);
        // FIXME. f64::MIN would be better!
        assert!(col.min_value().is_nan());
        assert!(col.max_value().is_nan());
    }
    assert!(num_acceptable_codecs > 0);
}

pub(crate) fn create_and_validate<TColumnCodec: ColumnCodec>(
    vals: &[u64],
    name: &str,
) -> Option<(f32, f32)> {
    let mut stats_collector = StatsCollector::default();
    let mut codec_estimator: TColumnCodec::Estimator = Default::default();

    for val in vals.boxed_iter() {
        stats_collector.collect(val);
        codec_estimator.collect(val);
    }
    codec_estimator.finalize();
    let stats = stats_collector.stats();
    let estimation = codec_estimator.estimate(&stats)?;

    let mut buffer = Vec::new();
    codec_estimator
        .serialize(&stats, vals.boxed_iter().as_mut(), &mut buffer)
        .unwrap();

    let actual_compression = buffer.len() as u64;

    let reader = TColumnCodec::load(OwnedBytes::new(buffer)).unwrap();
    assert_eq!(reader.num_vals(), vals.len() as u32);
    let mut buffer = Vec::new();
    for (doc, orig_val) in vals.iter().copied().enumerate() {
        let val = reader.get_val(doc as u32);
        assert_eq!(
            val, orig_val,
            "val `{val}` does not match orig_val {orig_val:?}, in data set {name}, data `{vals:?}`",
        );

        buffer.resize(1, 0);
        reader.get_vals(&[doc as u32], &mut buffer);
        let val = buffer[0];
        assert_eq!(
            val, orig_val,
            "val `{val}` does not match orig_val {orig_val:?}, in data set {name}, data `{vals:?}`",
        );
    }

    let all_docs: Vec<u32> = (0..vals.len() as u32).collect();
    buffer.resize(all_docs.len(), 0);
    reader.get_vals(&all_docs, &mut buffer);
    assert_eq!(vals, buffer);

    if !vals.is_empty() {
        let test_rand_idx = rand::rng().random_range(0..=vals.len() - 1);
        let expected_positions: Vec<u32> = vals
            .iter()
            .enumerate()
            .filter(|(_, el)| **el == vals[test_rand_idx])
            .map(|(pos, _)| pos as u32)
            .collect();
        let mut positions = Vec::new();
        reader.get_row_ids_for_value_range(
            vals[test_rand_idx]..=vals[test_rand_idx],
            0..vals.len() as u32,
            &mut positions,
        );
        assert_eq!(expected_positions, positions);
    }
    if actual_compression > 1000 {
        assert!(relative_difference(estimation, actual_compression) < 0.10f32);
    }
    Some((
        compression_rate(estimation, stats.num_rows),
        compression_rate(actual_compression, stats.num_rows),
    ))
}

fn compression_rate(num_bytes: u64, num_values: u32) -> f32 {
    num_bytes as f32 / (num_values as f32 * 8.0)
}

fn relative_difference(left: u64, right: u64) -> f32 {
    let left = left as f32;
    let right = right as f32;
    2.0f32 * (left - right).abs() / (left + right)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn test_proptest_small_bitpacked(data in proptest::collection::vec(num_strategy(), 1..10)) {
        create_and_validate::<BitpackedCodec>(&data, "proptest bitpacked");
    }

    #[test]
    fn test_proptest_small_linear(data in proptest::collection::vec(num_strategy(), 1..10)) {
        create_and_validate::<LinearCodec>(&data, "proptest linearinterpol");
    }


    #[test]
    fn test_proptest_small_blockwise_linear(data in proptest::collection::vec(num_strategy(), 1..10)) {
        create_and_validate::<BlockwiseLinearCodec>(&data, "proptest multilinearinterpol");
    }
}

#[test]
fn test_small_blockwise_linear_example() {
    create_and_validate::<BlockwiseLinearCodec>(
        &[9223372036854775808, 9223370937344622593],
        "proptest multilinearinterpol",
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]

    #[test]
    fn test_proptest_large_bitpacked(data in proptest::collection::vec(num_strategy(), 1..6000)) {
        create_and_validate::<BitpackedCodec>(&data, "proptest bitpacked");
    }

    #[test]
    fn test_proptest_large_linear(data in proptest::collection::vec(num_strategy(), 1..6000)) {
        create_and_validate::<LinearCodec>(&data, "proptest linearinterpol");
    }

    #[test]
    fn test_proptest_large_blockwise_linear(data in proptest::collection::vec(num_strategy(), 1..6000)) {
        create_and_validate::<BlockwiseLinearCodec>(&data, "proptest multilinearinterpol");
    }
}

fn num_strategy() -> impl Strategy<Value = u64> {
    prop_oneof![
        1 => prop::num::u64::ANY.prop_map(|num| u64::MAX - (num % 10) ),
        1 => prop::num::u64::ANY.prop_map(|num| num % 10 ),
        20 => prop::num::u64::ANY,
    ]
}

pub fn get_codec_test_datasets() -> Vec<(Vec<u64>, &'static str)> {
    let mut data_and_names = vec![];

    let data = (10..=10_000_u64).collect::<Vec<_>>();
    data_and_names.push((data, "simple monotonically increasing"));

    data_and_names.push((
        vec![5, 6, 7, 8, 9, 10, 99, 100],
        "offset in linear interpol",
    ));
    data_and_names.push((vec![5, 50, 3, 13, 1, 1000, 35], "rand small"));
    data_and_names.push((vec![10], "single value"));

    data_and_names.push((
        vec![1572656989877777, 1170935903116329, 720575940379279, 0],
        "overflow error",
    ));

    data_and_names
}

fn test_codec<C: ColumnCodec>() {
    let codec_name = std::any::type_name::<C>();
    for (data, dataset_name) in get_codec_test_datasets() {
        let estimate_actual_opt: Option<(f32, f32)> =
            tests::create_and_validate::<C>(&data, dataset_name);
        let result = if let Some((estimate, actual)) = estimate_actual_opt {
            format!("Estimate `{estimate}` Actual `{actual}`")
        } else {
            "Disabled".to_string()
        };
        println!("Codec {codec_name}, DataSet {dataset_name}, {result}");
    }
}
#[test]
fn test_codec_bitpacking() {
    test_codec::<BitpackedCodec>();
}
#[test]
fn test_codec_interpolation() {
    test_codec::<LinearCodec>();
}
#[test]
fn test_codec_multi_interpolation() {
    test_codec::<BlockwiseLinearCodec>();
}

use super::*;

fn estimate<C: ColumnCodec>(vals: &[u64]) -> Option<f32> {
    let mut stats_collector = StatsCollector::default();
    let mut estimator = C::Estimator::default();
    for &val in vals {
        stats_collector.collect(val);
        estimator.collect(val);
    }
    estimator.finalize();
    let stats = stats_collector.stats();
    let num_bytes = estimator.estimate(&stats)?;
    if stats.num_rows == 0 {
        return None;
    }
    Some(num_bytes as f32 / (8.0 * stats.num_rows as f32))
}

#[test]
fn estimation_good_interpolation_case() {
    let data = (10..=20000_u64).collect::<Vec<_>>();

    let linear_interpol_estimation = estimate::<LinearCodec>(&data).unwrap();
    assert_le!(linear_interpol_estimation, 0.01);

    let multi_linear_interpol_estimation = estimate::<BlockwiseLinearCodec>(&data).unwrap();
    assert_le!(multi_linear_interpol_estimation, 0.2);
    assert_lt!(linear_interpol_estimation, multi_linear_interpol_estimation);

    let bitpacked_estimation = estimate::<BitpackedCodec>(&data).unwrap();
    assert_lt!(linear_interpol_estimation, bitpacked_estimation);
}

#[test]
fn estimation_test_bad_interpolation_case_monotonically_increasing() {
    let mut data: Vec<u64> = (201..=20000_u64).collect();
    data.push(1_000_000);

    // in this case the linear interpolation can't in fact not be worse than bitpacking,
    // but the estimator adds some threshold, which leads to estimated worse behavior
    let linear_interpol_estimation = estimate::<LinearCodec>(&data[..]).unwrap();
    assert_le!(linear_interpol_estimation, 0.35);

    let bitpacked_estimation = estimate::<BitpackedCodec>(&data).unwrap();
    assert_le!(bitpacked_estimation, 0.32);
    assert_le!(bitpacked_estimation, linear_interpol_estimation);
}

#[test]
fn test_fast_field_codec_type_to_code() {
    let mut count_codec = 0;
    for code in 0..=255 {
        if let Some(codec_type) = CodecType::try_from_code(code) {
            assert_eq!(codec_type.to_code(), code);
            count_codec += 1;
        }
    }
    assert_eq!(count_codec, 3);
}

fn test_fastfield_gcd_i64_with_codec(codec_type: CodecType, num_vals: usize) -> io::Result<()> {
    let mut vals: Vec<i64> = (-4..=(num_vals as i64) - 5).map(|val| val * 1000).collect();
    let mut buffer: Vec<u8> = Vec::new();
    crate::column_values::serialize_u64_based_column_values(
        &&vals[..],
        &[codec_type],
        &mut buffer,
    )?;
    let buffer = OwnedBytes::new(buffer);
    let column = crate::column_values::load_u64_based_column_values::<i64>(buffer.clone())?;
    assert_eq!(column.get_val(0), -4000i64);
    assert_eq!(column.get_val(1), -3000i64);
    assert_eq!(column.get_val(2), -2000i64);
    assert_eq!(column.max_value(), (num_vals as i64 - 5) * 1000);
    assert_eq!(column.min_value(), -4000i64);

    // Can't apply gcd
    let mut buffer_without_gcd = Vec::new();
    vals.pop();
    vals.push(1001i64);
    crate::column_values::serialize_u64_based_column_values(
        &&vals[..],
        &[codec_type],
        &mut buffer_without_gcd,
    )?;
    let buffer_without_gcd = OwnedBytes::new(buffer_without_gcd);
    assert!(buffer_without_gcd.len() > buffer.len());

    Ok(())
}

#[test]
fn test_fastfield_gcd_i64() -> io::Result<()> {
    for &codec_type in &[
        CodecType::Bitpacked,
        CodecType::BlockwiseLinear,
        CodecType::Linear,
    ] {
        test_fastfield_gcd_i64_with_codec(codec_type, 5500)?;
    }
    Ok(())
}

fn test_fastfield_gcd_u64_with_codec(codec_type: CodecType, num_vals: usize) -> io::Result<()> {
    let mut vals: Vec<u64> = (1..=num_vals).map(|i| i as u64 * 1000u64).collect();
    let mut buffer: Vec<u8> = Vec::new();
    crate::column_values::serialize_u64_based_column_values(
        &&vals[..],
        &[codec_type],
        &mut buffer,
    )?;
    let buffer = OwnedBytes::new(buffer);
    let column = crate::column_values::load_u64_based_column_values::<u64>(buffer.clone())?;
    assert_eq!(column.get_val(0), 1000u64);
    assert_eq!(column.get_val(1), 2000u64);
    assert_eq!(column.get_val(2), 3000u64);
    assert_eq!(column.max_value(), num_vals as u64 * 1000);
    assert_eq!(column.min_value(), 1000u64);

    // Can't apply gcd
    let mut buffer_without_gcd = Vec::new();
    vals.pop();
    vals.push(1001u64);
    crate::column_values::serialize_u64_based_column_values(
        &&vals[..],
        &[codec_type],
        &mut buffer_without_gcd,
    )?;
    let buffer_without_gcd = OwnedBytes::new(buffer_without_gcd);
    assert!(buffer_without_gcd.len() > buffer.len());
    Ok(())
}

#[test]
fn test_fastfield_gcd_u64() -> io::Result<()> {
    for &codec_type in &[
        CodecType::Bitpacked,
        CodecType::BlockwiseLinear,
        CodecType::Linear,
    ] {
        test_fastfield_gcd_u64_with_codec(codec_type, 5500)?;
    }
    Ok(())
}

#[test]
pub fn test_fastfield2() {
    let test_fastfield = crate::column_values::serialize_and_load_u64_based_column_values::<u64>(
        &&[100u64, 200u64, 300u64][..],
        &ALL_U64_CODEC_TYPES,
    );
    assert_eq!(test_fastfield.get_val(0), 100);
    assert_eq!(test_fastfield.get_val(1), 200);
    assert_eq!(test_fastfield.get_val(2), 300);
}
