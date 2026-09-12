use super::*;
use crate::translate::collate::CollationSeq;
use crate::types::{ImmutableRecord, Value, ValueRef, ValueType};
use crate::util::IOExt;
use crate::PlatformIO;
use rand_chacha::{
    rand_core::{RngCore, SeedableRng},
    ChaCha8Rng,
};

fn get_seed() -> u64 {
    std::env::var("SEED").map_or(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
        |v| {
            v.parse()
                .expect("Failed to parse SEED environment variable as u64")
        },
    ) as u64
}

#[test]
fn fuzz_normalized_key_invariant() {
    use crate::types::AsValueRef;
    use turso_parser::ast::NullsOrder;
    let seed = get_seed();
    let mut rng = ChaCha8Rng::seed_from_u64(seed);

    // Values chosen to stress every branch: type boundaries, f64 exactness
    // limits, shared prefixes, embedded NULs, lengths straddling 7 bytes.
    let gen_value = |rng: &mut ChaCha8Rng| -> Value {
        match rng.next_u64() % 10 {
            0 => Value::Null,
            1 => Value::from_i64(rng.next_u64() as i64),
            2 => Value::from_i64((rng.next_u64() % 100) as i64 - 50),
            3 => {
                // Integers around the 2^49..2^53 exactness boundaries.
                let base = 1i64 << (48 + rng.next_u64() % 8);
                Value::from_i64(base + (rng.next_u64() % 5) as i64 - 2)
            }
            4 => {
                let numerator = rng.next_u64() as f64;
                let denominator = (rng.next_u64() as f64).abs().max(1.0);
                Value::from_f64(numerator / denominator)
            }
            5 => Value::from_f64(if rng.next_u64() % 2 == 0 { 0.0 } else { -0.0 }),
            6..=8 => {
                let alphabet = [b'a', b'b', b'\0'];
                let len = (rng.next_u64() % 10) as usize;
                let s: String = (0..len)
                    .map(|_| alphabet[(rng.next_u64() % 3) as usize] as char)
                    .collect();
                Value::build_text(s)
            }
            _ => {
                let len = (rng.next_u64() % 10) as usize;
                let mut blob = try_vec![0u8; len].unwrap();
                rng.fill_bytes(&mut blob);
                Value::Blob(blob)
            }
        }
    };

    let gen_key = |rng: &mut ChaCha8Rng| KeyInfo {
        sort_order: if rng.next_u64() % 2 == 0 {
            SortOrder::Asc
        } else {
            SortOrder::Desc
        },
        collation: CollationSeq::Binary,
        nulls_order: match rng.next_u64() % 3 {
            0 => None,
            1 => Some(NullsOrder::First),
            _ => Some(NullsOrder::Last),
        },
    };

    for _ in 0..200_000 {
        // Half the iterations use a two-column key so the multi-column path
        // is covered: the normalized key still encodes only the first
        // column, and `decisive` must never be set (else equal first
        // columns would wrongly short-circuit past the second column).
        let ncols = 1 + (rng.next_u64() % 2) as usize;
        // Fixed-size arrays sliced to `ncols`: the crate's `Vec` alias is
        // allocator-parameterized under the nightly cfg and has no
        // `FromIterator`, so `collect()` into it would not compile.
        let va = [gen_value(&mut rng), gen_value(&mut rng)];
        let vb = [gen_value(&mut rng), gen_value(&mut rng)];
        let keys = [gen_key(&mut rng), gen_key(&mut rng)];
        let comparators: [Option<SortComparator>; 2] = [None, None];
        let ra = [va[0].as_value_ref(), va[1].as_value_ref()];
        let rb = [vb[0].as_value_ref(), vb[1].as_value_ref()];

        let (norm_a, dec_a) =
            normalized_first_key(&ra[..ncols], &keys[..ncols], &comparators[..ncols]);
        let (norm_b, dec_b) =
            normalized_first_key(&rb[..ncols], &keys[..ncols], &comparators[..ncols]);
        // The normalized key only ever reflects the first column.
        let a = &ra[0];
        let b = &rb[0];
        let key = &keys[0];
        let reference = cmp_in_column(a, b, key);

        if ncols > 1 {
            assert!(
                !dec_a && !dec_b,
                "multi-column keys must never be decisive: {va:?} vs {vb:?} keys {keys:?}"
            );
        }
        match norm_a.cmp(&norm_b) {
            Ordering::Equal => {
                if dec_a && dec_b {
                    assert_eq!(
                        reference,
                        Ordering::Equal,
                        "decisive equal norms must mean equal keys: {va:?} vs {vb:?} keys {keys:?}"
                    );
                }
            }
            ord => {
                // A strict normalized order must match the first-column
                // reference exactly: it may never contradict the reference,
                // and it may never separate keys the reference deems equal
                // (that would split GROUP BY groups or skip a later column).
                assert_eq!(
                    ord, reference,
                    "strict norm order must match reference: {va:?} vs {vb:?} keys {keys:?}"
                );
            }
        }
    }
}

#[test]
fn fuzz_external_sort() {
    let seed = get_seed();
    let mut rng = ChaCha8Rng::seed_from_u64(seed);

    let io = Arc::new(PlatformIO::new().unwrap());

    let attempts = 8;
    for _ in 0..attempts {
        let mut sorter = Sorter::new(
            &[SortOrder::Asc],
            try_vec![CollationSeq::Binary].unwrap(),
            try_vec![None].unwrap(),
            try_vec![None].unwrap(),
            256,
            64,
            io.clone(),
            crate::TempStore::Default,
        )
        .unwrap();

        let num_records = 1000 + rng.next_u64() % 2000;
        let num_records = num_records as i64;

        let num_values = 1 + rng.next_u64() % 4;
        let value_types = generate_value_types(&mut rng, num_values as usize);

        let mut initial_records = Vec::with_capacity(num_records as usize);
        for i in (0..num_records).rev() {
            let mut values = try_vec![Value::from_i64(i)].unwrap();
            values.append(&mut generate_values(&mut rng, &value_types));
            let record = ImmutableRecord::from_values(&values, values.len()).unwrap();

            io.block(|| sorter.insert(&record))
                .expect("Failed to insert the record");
            initial_records.push(record);
        }

        io.block(|| sorter.sort())
            .expect("Failed to sort the records");

        assert!(!sorter.is_empty());
        assert!(!sorter.chunks.is_empty());

        for i in 0..num_records {
            assert!(sorter.has_more());
            let record = sorter.record().unwrap();
            assert_eq!(record.get_values().unwrap()[0], ValueRef::from_i64(i));
            // Check that the record remained unchanged after sorting.
            assert_eq!(record, &initial_records[(num_records - i - 1) as usize]);

            io.block(|| sorter.next())
                .expect("Failed to get the next record");
        }
        assert!(!sorter.has_more());
    }
}

fn generate_value_types<R: RngCore>(rng: &mut R, num_values: usize) -> Vec<ValueType> {
    let mut value_types = <Vec<ValueType> as TursoVecExt<ValueType>>::with_capacity(num_values);

    for _ in 0..num_values {
        let value_type: ValueType = match rng.next_u64() % 4 {
            0 => ValueType::Integer,
            1 => ValueType::Float,
            2 => ValueType::Blob,
            3 => ValueType::Null,
            _ => unreachable!(),
        };
        value_types.push(value_type);
    }

    value_types
}

fn generate_values<R: RngCore>(rng: &mut R, value_types: &[ValueType]) -> Vec<Value> {
    let mut values = <Vec<Value> as TursoVecExt<Value>>::with_capacity(value_types.len());
    for value_type in value_types {
        let value = match value_type {
            ValueType::Integer => Value::from_i64(rng.next_u64() as i64),
            ValueType::Float => {
                let numerator = rng.next_u64() as f64;
                let denominator = rng.next_u64() as f64;
                Value::from_f64(numerator / denominator)
            }
            ValueType::Blob => {
                let mut blob = <Vec<u8> as TursoVecExt<u8>>::with_capacity(
                    (rng.next_u64() % 2047 + 1) as usize,
                );
                rng.fill_bytes(&mut blob);
                Value::Blob(blob)
            }
            ValueType::Null => Value::Null,
            _ => unreachable!(),
        };
        values.push(value);
    }
    values
}

fn assert_secondary_key_sort(
    second_order: SortOrder,
    second_nulls: Option<turso_parser::ast::NullsOrder>,
    seconds: &[Value],
    expected: &[ValueRef],
) {
    let io = Arc::new(PlatformIO::new().unwrap());
    let mut sorter = Sorter::new(
        &[SortOrder::Asc, second_order],
        try_vec![CollationSeq::Binary, CollationSeq::Binary].unwrap(),
        try_vec![None, second_nulls].unwrap(),
        try_vec![None, None].unwrap(),
        1 << 20,
        64,
        io.clone(),
        crate::TempStore::Default,
    )
    .unwrap();

    for second in seconds {
        let values = try_vec![Value::from_i64(1), second.clone()].unwrap();
        let record = ImmutableRecord::from_values(&values, values.len()).unwrap();
        io.block(|| sorter.insert(&record))
            .expect("Failed to insert the record");
    }

    io.block(|| sorter.sort())
        .expect("Failed to sort the records");
    assert!(sorter.chunks.is_empty());

    let mut idx = 0;
    while sorter.has_more() {
        {
            let record = sorter.record().unwrap();
            let vals = record.get_values().unwrap();
            assert_eq!(vals[0], ValueRef::from_i64(1));
            assert_eq!(vals[1], expected[idx]);
        }
        idx += 1;
        io.block(|| sorter.next())
            .expect("Failed to get the next record");
    }
    assert_eq!(idx, expected.len());
}

#[test]
fn spilled_sort_orders_secondary_key_across_chunks() {
    let io = Arc::new(PlatformIO::new().unwrap());
    let mut sorter = Sorter::new(
        &[SortOrder::Asc, SortOrder::Asc],
        try_vec![CollationSeq::Binary, CollationSeq::Binary].unwrap(),
        try_vec![None, None].unwrap(),
        try_vec![None, None].unwrap(),
        // Tiny buffer so the sorter spills to multiple chunk files.
        256,
        64,
        io.clone(),
        crate::TempStore::Default,
    )
    .unwrap();

    let n = 200;
    // Equal first key, ascending second key on insert.
    for x in 0..n {
        let values = try_vec![Value::from_i64(1), Value::from_i64(x)].unwrap();
        let record = ImmutableRecord::from_values(&values, values.len()).unwrap();
        io.block(|| sorter.insert(&record))
            .expect("Failed to insert the record");
    }

    io.block(|| sorter.sort())
        .expect("Failed to sort the records");
    assert!(
        !sorter.chunks.is_empty(),
        "test requires the sorter to have spilled to chunks"
    );

    let mut idx = 0;
    while sorter.has_more() {
        {
            let record = sorter.record().unwrap();
            let vals = record.get_values().unwrap();
            assert_eq!(vals[0], ValueRef::from_i64(1));
            assert_eq!(
                vals[1],
                ValueRef::from_i64(idx),
                "secondary key out of order at position {idx}"
            );
        }
        idx += 1;
        io.block(|| sorter.next())
            .expect("Failed to get the next record");
    }
    assert_eq!(idx, n);
}

#[test]
fn spilled_sort_places_nulls_last_across_chunks() {
    let io = Arc::new(PlatformIO::new().unwrap());
    let mut sorter = Sorter::new(
        &[SortOrder::Asc, SortOrder::Asc],
        try_vec![CollationSeq::Binary, CollationSeq::Binary].unwrap(),
        try_vec![None, Some(turso_parser::ast::NullsOrder::Last)].unwrap(),
        try_vec![None, None].unwrap(),
        256,
        64,
        io.clone(),
        crate::TempStore::Default,
    )
    .unwrap();

    let n = 200;
    for x in 0..n {
        let second = if x % 2 == 0 {
            Value::Null
        } else {
            Value::from_i64(x)
        };
        let values = try_vec![Value::from_i64(1), second].unwrap();
        let record = ImmutableRecord::from_values(&values, values.len()).unwrap();
        io.block(|| sorter.insert(&record)).unwrap();
    }

    io.block(|| sorter.sort()).unwrap();
    assert!(
        !sorter.chunks.is_empty(),
        "test requires the sorter to have spilled to chunks"
    );

    let mut idx = 0;
    let mut prev = None;
    while sorter.has_more() {
        {
            let record = sorter.record().unwrap();
            let vals = record.get_values().unwrap();
            assert_eq!(vals[0], ValueRef::from_i64(1));
            match vals[1] {
                ValueRef::Null => {
                    assert!(idx >= n / 2, "NULL emitted before all non-NULL values");
                }
                v => {
                    assert!(idx < n / 2, "non-NULL value {v:?} emitted after NULL block");
                    let current = match v {
                        ValueRef::Numeric(crate::numeric::Numeric::Integer(i)) => i,
                        other => panic!("unexpected value {other:?}"),
                    };
                    if let Some(prev) = prev {
                        assert!(current > prev);
                    }
                    prev = Some(current);
                }
            }
        }
        idx += 1;
        io.block(|| sorter.next()).unwrap();
    }
    assert_eq!(idx, n);
}

#[test]
fn in_memory_sort_applies_desc_on_secondary_key() {
    let seconds = try_vec![
        Value::from_i64(10),
        Value::from_i64(40),
        Value::from_i64(20),
        Value::from_i64(30)
    ]
    .unwrap();
    assert_secondary_key_sort(
        SortOrder::Desc,
        None,
        &seconds,
        &[
            ValueRef::from_i64(40),
            ValueRef::from_i64(30),
            ValueRef::from_i64(20),
            ValueRef::from_i64(10),
        ],
    );
}

#[test]
fn in_memory_sort_places_nulls_last_on_desc_secondary_key() {
    let seconds = try_vec![
        Value::Null,
        Value::from_i64(10),
        Value::Null,
        Value::from_i64(20)
    ]
    .unwrap();
    assert_secondary_key_sort(
        SortOrder::Desc,
        Some(turso_parser::ast::NullsOrder::Last),
        &seconds,
        &[
            ValueRef::from_i64(20),
            ValueRef::from_i64(10),
            ValueRef::Null,
            ValueRef::Null,
        ],
    );
}
