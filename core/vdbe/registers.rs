use std::ops::{Deref, DerefMut};
use std::slice::SliceIndex;

use super::Register;
use crate::types::NanPolicy;
use crate::Value;

/// Registers visible to SQL instructions. All mutation goes through a write
/// guard, so a result obeys the dialect before the next instruction reads it.
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
        self.write(index).set_int(value)
    }

    #[inline]
    pub(crate) fn set_float(&mut self, index: usize, value: crate::Double) {
        self.write(index).set_float(value)
    }

    #[inline]
    pub(crate) fn set_numeric(&mut self, index: usize, value: crate::Numeric) {
        self.write(index).set_numeric(value)
    }

    #[inline]
    pub(crate) fn set_text(
        &mut self,
        index: usize,
        value: crate::types::Text,
    ) -> crate::Result<()> {
        self.write(index).set_text(value)
    }

    #[inline]
    pub(crate) fn set_blob(
        &mut self,
        index: usize,
        value: crate::types::ValueBlob,
    ) -> crate::Result<()> {
        self.write(index).set_blob(value)
    }

    /// Borrow one input and a distinct range of outputs.
    pub(crate) fn read_and_write(
        &mut self,
        source: usize,
        range: std::ops::Range<usize>,
    ) -> (&Register, RegisterWrite<'_, [Register]>) {
        assert!(
            !range.contains(&source),
            "input and output registers overlap"
        );
        let (source, outputs) = if source < range.start {
            let (left, right) = self.values.split_at_mut(range.start);
            (&left[source], &mut right[..range.len()])
        } else {
            let (left, right) = self.values.split_at_mut(range.end);
            (&right[source - range.end], &mut left[range])
        };
        (
            source,
            RegisterWrite {
                value: outputs,
                nan_policy: self.nan_policy,
            },
        )
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
            let result: Result<(), ()> = (|| {
                let mut writes = registers.write_disjoint([0, 2]).unwrap();
                *writes[0] = Register::Value(Value::from_f64(f64::NAN));
                writes[1].set_numeric(Numeric::Float(Double::new(f64::NAN)));
                Err(())
            })();
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
