use std::ops::{Deref, DerefMut};
use std::slice::SliceIndex;

use super::{decode_serial_type_into_register, Register, ValueIteratorExt};
use crate::types::{NanPolicy, ValueIterator};
use crate::Value;

/// Registers visible to SQL instructions. Setters and decoders apply the
/// dialect rule; general mutation uses a guard before another instruction reads it.
#[derive(Debug)]
pub(crate) struct Registers {
    values: Box<[Register]>,
    nan_policy: NanPolicy,
}

impl Registers {
    pub(crate) fn new(count: usize, nan_policy: NanPolicy) -> Self {
        Self {
            values: (0..count).map(|_| Register::Value(Value::Null)).collect(),
            nan_policy,
        }
    }

    pub(crate) fn nan_policy(&self) -> NanPolicy {
        self.nan_policy
    }

    pub(crate) fn resize(&mut self, count: usize) {
        let mut values = std::mem::take(&mut self.values).into_vec();
        values.resize_with(count, || Register::Value(Value::Null));
        self.values = values.into_boxed_slice();
    }

    pub(crate) fn swap(&mut self, left: usize, right: usize) {
        self.values.swap(left, right);
    }

    #[inline]
    pub(crate) fn set_value(&mut self, index: usize, value: Value) {
        self.write(index).set_value(value)
    }

    #[inline]
    pub(crate) fn set_int(&mut self, index: usize, value: i64) {
        self.values[index].set_int(value)
    }

    #[inline]
    pub(crate) fn set_float(&mut self, index: usize, value: crate::Double) {
        self.write(index).set_float(value)
    }

    #[inline]
    pub(crate) fn set_numeric(&mut self, index: usize, value: crate::Numeric) {
        match value {
            crate::Numeric::Integer(value) => self.set_int(index, value),
            crate::Numeric::Float(value) => self.set_float(index, value),
        }
    }

    #[inline]
    pub(crate) fn set_text(
        &mut self,
        index: usize,
        value: crate::types::Text,
    ) -> crate::Result<()> {
        self.values[index].set_text(value)
    }

    #[inline]
    pub(crate) fn set_blob(
        &mut self,
        index: usize,
        value: crate::types::ValueBlob,
    ) -> crate::Result<()> {
        self.values[index].set_blob(value)
    }

    #[inline]
    pub(crate) fn set_null(&mut self, index: usize) {
        self.values[index].set_null();
    }

    pub(crate) fn clear<I>(&mut self, range: I)
    where
        I: SliceIndex<[Register], Output = [Register]>,
    {
        for register in &mut self.values[range] {
            register.set_null();
        }
    }

    /// Decode one column without checking its value again after decoding.
    #[inline]
    pub(crate) fn decode_column(
        &mut self,
        payload: &[u8],
        column: usize,
        dest: usize,
    ) -> crate::Result<bool> {
        Ok(ValueIterator::new(payload)?
            .nth_into_register(column, &mut self.values[dest])
            .transpose()?
            .is_some())
    }

    /// Decode consecutive columns. Registers beyond the returned count are unchanged.
    #[inline]
    pub(crate) fn decode_columns(
        &mut self,
        payload: &[u8],
        column: usize,
        dests: std::ops::Range<usize>,
    ) -> crate::Result<usize> {
        ValueIterator::new(payload)?.decode_into_registers_after(column, &mut self.values[dests])
    }

    /// Decode a value whose serial type was read separately from its data.
    #[inline]
    pub(crate) fn decode_serial(
        &mut self,
        serial_type: u64,
        data: &mut &[u8],
        dest: usize,
    ) -> crate::Result<()> {
        decode_serial_type_into_register(serial_type, data, &mut self.values[dest])
    }

    /// Decode a record held in a register into a distinct range of registers.
    pub(crate) fn decode_record_columns(
        &mut self,
        source: usize,
        column: usize,
        dests: std::ops::Range<usize>,
    ) -> crate::Result<usize> {
        let (source, dests) = self.read_and_write(source, dests);
        let Register::Record(record) = source else {
            crate::bail_parse_error!("non-register register in pseudo-record ({source:?})");
        };
        ValueIterator::new(record.get_payload())?.decode_into_registers_after(column, dests)
    }

    /// Decode a field from a blob register, or write NULL for a missing field.
    pub(crate) fn decode_blob_column(
        &mut self,
        source: usize,
        offset: usize,
        column: usize,
        dest: usize,
    ) -> crate::Result<()> {
        let (source, dests) = self.read_and_write(source, dest..dest + 1);
        let dest = &mut dests[0];
        if let Value::Blob(blob) = source.get_value() {
            if let Some(result) =
                ValueIterator::new(&blob[offset..])?.nth_into_register(column, dest)
            {
                return result;
            }
        }
        dest.set_null();
        Ok(())
    }

    // Only the decoders above can borrow unguarded outputs. Each uses this
    // register file's rule, including when decoding stops with an error.
    fn read_and_write(
        &mut self,
        source: usize,
        range: std::ops::Range<usize>,
    ) -> (&Register, &mut [Register]) {
        assert!(
            !range.contains(&source),
            "input and output registers overlap"
        );
        if source < range.start {
            let (left, right) = self.values.split_at_mut(range.start);
            (&left[source], &mut right[..range.len()])
        } else {
            let (left, right) = self.values.split_at_mut(range.end);
            (&right[source - range.end], &mut left[range])
        }
    }

    pub(crate) fn write_disjoint<const N: usize>(
        &mut self,
        indices: [usize; N],
    ) -> Result<DisjointWrites<'_, N>, std::slice::GetDisjointMutError> {
        let values = self.values.get_disjoint_mut(indices)?;
        Ok(DisjointWrites {
            values,
            nan_policy: self.nan_policy,
        })
    }

    #[inline]
    pub(crate) fn write<I>(&mut self, index: I) -> RegisterWrite<'_, I::Output>
    where
        I: SliceIndex<[Register]>,
        I::Output: Normalize,
    {
        RegisterWrite {
            value: &mut self.values[index],
            nan_policy: self.nan_policy,
        }
    }
}

impl Deref for Registers {
    type Target = [Register];

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.values
    }
}

/// The guard also covers direct numeric mutation and extension callbacks.
/// Aggregate accumulators are scratch state; only their published result is
/// normalized. Dropping the guard applies the rule on I/O yields and errors too.
#[derive(Debug)]
pub(crate) struct RegisterWrite<'a, T: Normalize + ?Sized> {
    value: &'a mut T,
    nan_policy: NanPolicy,
}

impl<T: Normalize + ?Sized> Deref for RegisterWrite<'_, T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        self.value
    }
}

impl<T: Normalize + ?Sized> DerefMut for RegisterWrite<'_, T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut T {
        self.value
    }
}

impl<T: Normalize + ?Sized> Drop for RegisterWrite<'_, T> {
    #[inline]
    fn drop(&mut self) {
        self.value.normalize(self.nan_policy);
    }
}

pub(crate) trait Normalize {
    fn normalize(&mut self, policy: NanPolicy);
}

impl Normalize for Register {
    #[inline]
    fn normalize(&mut self, policy: NanPolicy) {
        if let Register::Value(value) = self {
            policy.normalize(value);
        }
    }
}

impl Normalize for [Register] {
    #[inline]
    fn normalize(&mut self, policy: NanPolicy) {
        if policy == NanPolicy::Null {
            for register in self {
                register.normalize(policy);
            }
        }
    }
}

pub(crate) struct DisjointWrites<'a, const N: usize> {
    values: [&'a mut Register; N],
    nan_policy: NanPolicy,
}

impl<'a, const N: usize> Deref for DisjointWrites<'a, N> {
    type Target = [&'a mut Register; N];
    fn deref(&self) -> &Self::Target {
        &self.values
    }
}

impl<const N: usize> DerefMut for DisjointWrites<'_, N> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.values
    }
}

impl<const N: usize> Drop for DisjointWrites<'_, N> {
    fn drop(&mut self) {
        for value in &mut self.values {
            value.normalize(self.nan_policy);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Double, Numeric};

    #[test]
    fn writes_apply_the_policy_even_on_an_early_return() {
        for policy in [NanPolicy::Null, NanPolicy::Preserve] {
            let mut registers = Registers::new(3, policy);
            let result = write_nan_then_fail(&mut registers);
            assert!(result.is_err());
            for index in [0, 2] {
                assert_eq!(
                    registers[index].get_value().is_nan(),
                    policy == NanPolicy::Preserve
                );
                assert_eq!(registers[index].is_null(), policy == NanPolicy::Null);
            }
        }
    }

    fn write_nan_then_fail(registers: &mut Registers) -> Result<(), ()> {
        let mut writes = registers.write_disjoint([0, 2]).unwrap();
        *writes[0] = Register::Value(Value::from_f64(f64::NAN));
        writes[1].set_numeric(Numeric::Float(Double::new(f64::NAN)));
        Err(())
    }

    #[test]
    fn aggregate_scratch_keeps_nan_until_the_result_is_published() {
        let mut registers = Registers::new(2, NanPolicy::Null);
        *registers.write(0) = Register::Aggregate(crate::types::AggContext::Builtin(
            crate::alloc::try_vec![Value::from_f64(f64::NAN)].unwrap(),
        ));
        let Register::Aggregate(crate::types::AggContext::Builtin(payload)) = &registers[0] else {
            unreachable!()
        };
        assert!(payload[0].is_nan());
        registers.set_value(1, payload[0].clone());
        assert!(registers[1].is_null());
    }
}
