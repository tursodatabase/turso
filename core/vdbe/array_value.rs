use crate::storage::sqlite3_ondisk::{read_varint, write_varint_to_vec};
use crate::types::{AsValueRef, ImmutableRecord, RecordBuf, Value, ValueIterator};
use crate::{LimboError, Result};

pub const DEFAULT_LOWER_BOUND: i32 = 1;

const FRAME_MARK: u64 = 1 << 63;
const FRAME_LEN: usize = 9;

/// An array: its elements and the subscript of its first element.
#[derive(Debug, Clone, PartialEq)]
pub struct ArrayValue {
    pub lower_bound: i32,
    pub elements: Vec<Value>,
}

impl ArrayValue {
    pub fn new(elements: Vec<Value>) -> Self {
        Self {
            lower_bound: DEFAULT_LOWER_BOUND,
            elements,
        }
    }

    pub fn decode(blob: &[u8]) -> Result<Self> {
        let view = ArrayView::parse(blob)?;
        let mut elements = Vec::with_capacity(view.elements().size_hint().0);
        for element in view.elements() {
            elements.push(element?.to_owned()?);
        }
        Ok(Self {
            lower_bound: view.lower_bound(),
            elements,
        })
    }

    pub fn encode(&self) -> Result<Value> {
        encode_array(self.lower_bound, &self.elements)
    }
}

/// Stores `elements` as an array whose first element has subscript `lower_bound`.
pub fn encode_array<'a>(
    lower_bound: i32,
    elements: impl IntoIterator<Item = impl AsValueRef + 'a> + Clone,
) -> Result<Value> {
    let is_empty = elements.clone().into_iter().next().is_none();
    if lower_bound > DEFAULT_LOWER_BOUND && !is_empty {
        let length = elements.clone().into_iter().count() as i64;
        if i64::from(lower_bound) + length > i64::from(i32::MAX) {
            return Err(LimboError::ArrayBoundTooLarge {
                which: "lower",
                bound: i64::from(lower_bound),
            });
        }
    }
    let payload = ImmutableRecord::build(elements, RecordBuf::alloc())?.into_payload();
    if lower_bound == DEFAULT_LOWER_BOUND || is_empty {
        return Ok(Value::Blob(payload));
    }
    let mut framed = Vec::with_capacity(FRAME_LEN + payload.len());
    write_varint_to_vec(FRAME_MARK | u64::from(lower_bound as u32), &mut framed);
    framed.extend_from_slice(&payload);
    Ok(Value::from_slice(&framed)?)
}

/// A stored array, read in place.
#[derive(Clone)]
pub struct ArrayView<'a> {
    lower_bound: i32,
    elements: ValueIterator<'a>,
}

impl<'a> ArrayView<'a> {
    pub fn parse(blob: &'a [u8]) -> Result<Self> {
        let (first, first_len) = read_varint(blob)?;
        if first < FRAME_MARK {
            return Ok(Self {
                lower_bound: DEFAULT_LOWER_BOUND,
                elements: ValueIterator::new(blob)?,
            });
        }
        if first_len != FRAME_LEN || first & !(FRAME_MARK | u64::from(u32::MAX)) != 0 {
            return Err(LimboError::Corrupt("array has a bad lower bound".into()));
        }
        let elements = ValueIterator::new(&blob[FRAME_LEN..])?;
        let lower_bound = if elements.clone().next().is_none() {
            DEFAULT_LOWER_BOUND
        } else {
            first as u32 as i32
        };
        Ok(Self {
            lower_bound,
            elements,
        })
    }

    pub fn lower_bound(&self) -> i32 {
        self.lower_bound
    }

    pub fn elements(&self) -> ValueIterator<'a> {
        self.elements.clone()
    }

    pub fn position_of(&self, subscript: i64) -> Option<usize> {
        position_of(self.lower_bound, subscript)
    }
}

/// The index of the element with `subscript`, or None before the first element.
pub fn position_of(lower_bound: i32, subscript: i64) -> Option<usize> {
    usize::try_from(subscript.checked_sub(i64::from(lower_bound))?).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn int_array(values: &[i64]) -> ArrayValue {
        ArrayValue::new(values.iter().map(|v| Value::from_i64(*v)).collect())
    }

    fn encoded(array: &ArrayValue) -> Vec<u8> {
        match array.encode().unwrap() {
            Value::Blob(blob) => blob.to_vec(),
            other => panic!("expected a blob, got {other:?}"),
        }
    }

    fn bounded(lower_bound: i32, elements: Vec<Value>) -> ArrayValue {
        ArrayValue {
            lower_bound,
            elements,
        }
    }

    #[test]
    fn array_round_trips() {
        let array = int_array(&[1, 2, 3]);
        assert_eq!(ArrayValue::decode(&encoded(&array)).unwrap(), array);
    }

    #[test]
    fn empty_array_round_trips() {
        let array = int_array(&[]);
        assert_eq!(ArrayValue::decode(&encoded(&array)).unwrap(), array);
    }

    #[test]
    fn array_keeps_nulls_texts_and_nested_arrays() {
        let nested = bounded(0, vec![Value::from_i64(7)]).encode().unwrap();
        let array = ArrayValue::new(vec![
            Value::Null,
            Value::build_text("a b".to_string()),
            nested,
        ]);
        assert_eq!(ArrayValue::decode(&encoded(&array)).unwrap(), array);
    }

    #[test]
    fn array_with_the_default_lower_bound_is_a_plain_record() {
        let record =
            ImmutableRecord::from_values(&[Value::from_i64(1), Value::from_i64(2)], 2).unwrap();
        assert_eq!(encoded(&int_array(&[1, 2])), record.into_payload().to_vec());
    }

    #[test]
    fn plain_record_has_the_default_lower_bound() {
        let blob = encoded(&int_array(&[9]));
        assert_eq!(
            ArrayView::parse(&blob).unwrap().lower_bound(),
            DEFAULT_LOWER_BOUND
        );
    }

    #[test]
    fn array_round_trips_with_another_lower_bound() {
        for lower_bound in [0, -3, 2, i32::MIN, i32::MAX - 3] {
            let array = bounded(
                lower_bound,
                vec![Value::from_i64(10), Value::Null, Value::from_i64(30)],
            );
            assert_eq!(ArrayValue::decode(&encoded(&array)).unwrap(), array);
        }
    }

    #[test]
    fn long_array_round_trips_with_another_lower_bound() {
        let array = bounded(0, (0..20_000).map(Value::from_i64).collect());
        assert_eq!(ArrayValue::decode(&encoded(&array)).unwrap(), array);
    }

    #[test]
    fn array_with_another_lower_bound_has_a_frame_of_nine_bytes() {
        let plain = encoded(&int_array(&[1, 2]));
        let framed = encoded(&bounded(0, vec![Value::from_i64(1), Value::from_i64(2)]));
        assert_eq!(framed.len(), plain.len() + FRAME_LEN);
        assert_eq!(framed[FRAME_LEN..], plain[..]);
    }

    #[test]
    fn empty_array_has_the_default_lower_bound() {
        let decoded = ArrayValue::decode(&encoded(&bounded(5, vec![]))).unwrap();
        assert_eq!(decoded, int_array(&[]));
    }

    #[test]
    fn a_frame_around_no_elements_has_the_default_lower_bound() {
        let mut blob = Vec::new();
        write_varint_to_vec(FRAME_MARK | 5, &mut blob);
        blob.extend_from_slice(&encoded(&int_array(&[])));
        assert_eq!(
            ArrayView::parse(&blob).unwrap().lower_bound(),
            DEFAULT_LOWER_BOUND
        );
    }

    #[test]
    fn array_whose_upper_bound_does_not_fit_is_refused() {
        let ok = bounded(i32::MAX - 2, vec![Value::from_i64(1), Value::from_i64(2)]);
        assert!(ok.encode().is_ok());
        let too_long = bounded(i32::MAX - 2, vec![Value::from_i64(1); 3]);
        assert!(matches!(
            too_long.encode(),
            Err(LimboError::ArrayBoundTooLarge { .. })
        ));
    }

    #[test]
    fn plain_reader_fails_on_an_array_with_another_lower_bound() {
        let blob = encoded(&bounded(0, vec![Value::from_i64(1)]));
        assert!(matches!(
            ValueIterator::new(&blob),
            Err(LimboError::Corrupt(_))
        ));
    }

    #[test]
    fn frame_with_stray_bits_is_corrupt() {
        let mut blob = Vec::new();
        write_varint_to_vec(FRAME_MARK | (1 << 40), &mut blob);
        blob.extend_from_slice(&encoded(&int_array(&[1])));
        assert!(matches!(
            ArrayView::parse(&blob),
            Err(LimboError::Corrupt(_))
        ));
    }

    #[test]
    fn view_reads_elements_in_place() {
        let blob = encoded(&bounded(0, vec![Value::from_i64(4), Value::from_i64(5)]));
        let view = ArrayView::parse(&blob).unwrap();
        assert_eq!(view.lower_bound(), 0);
        assert_eq!(view.elements().count(), 2);
        assert_eq!(view.elements().count(), 2);
    }

    #[test]
    fn decoding_a_blob_that_is_not_an_array_fails() {
        assert!(ArrayValue::decode(&[]).is_err());
    }
}
