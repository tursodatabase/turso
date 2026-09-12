//! Statement parameters, mirroring the embedded `turso::params` module.

use std::borrow::Cow;

use crate::{Error, Result, Value};

mod sealed {
    pub trait Sealed {}
}

use sealed::Sealed;

/// Converts some type into parameters that can be passed to a statement.
///
/// The same forms as the embedded `turso` driver are supported:
///
/// # Positional parameters
///
/// - Tuples of up to 16 heterogeneous values: `(1, "foo")`.
/// - The [`crate::params!`] macro: `params![1, "foo"]`.
/// - Const arrays of a homogeneous type: `[1, 2, 3]`.
///
/// # Named parameters
///
/// Named parameter keys must include the SQL prefix used in the statement,
/// for example `:name`, `@name`, or `$name`.
///
/// - Tuples of key-value pairs: `((":a", 1), (":b", "foo"))`.
/// - The [`crate::named_params!`] macro: `named_params![":a": 1, ":b": "foo"]`.
/// - Const arrays: `[(":a", 1), (":b", 2)]`.
pub trait IntoParams: Sealed {
    #[doc(hidden)]
    fn into_params(self) -> Result<Params>;
}

#[derive(Debug, Clone)]
#[doc(hidden)]
pub enum Params {
    None,
    Positional(Vec<Value>),
    Named(Vec<(Cow<'static, str>, Value)>),
}

/// Convert an owned iterator into positional params.
pub fn params_from_iter<I>(iter: I) -> impl IntoParams
where
    I: IntoIterator,
    I::Item: IntoValue,
{
    iter.into_iter().collect::<Vec<_>>()
}

impl Sealed for () {}
impl IntoParams for () {
    fn into_params(self) -> Result<Params> {
        Ok(Params::None)
    }
}

impl Sealed for Params {}
impl IntoParams for Params {
    fn into_params(self) -> Result<Params> {
        Ok(self)
    }
}

impl<T: IntoValue> Sealed for Vec<T> {}
impl<T: IntoValue> IntoParams for Vec<T> {
    fn into_params(self) -> Result<Params> {
        let values = self
            .into_iter()
            .map(|i| i.into_value())
            .collect::<Result<Vec<_>>>()?;

        Ok(Params::Positional(values))
    }
}

impl<T: IntoValue> Sealed for Vec<(String, T)> {}
impl<T: IntoValue> IntoParams for Vec<(String, T)> {
    fn into_params(self) -> Result<Params> {
        let values = self
            .into_iter()
            .map(|(k, v)| Ok((Cow::Owned(k), v.into_value()?)))
            .collect::<Result<Vec<_>>>()?;

        Ok(Params::Named(values))
    }
}

impl<T: IntoValue, const N: usize> Sealed for [T; N] {}
impl<T: IntoValue, const N: usize> IntoParams for [T; N] {
    fn into_params(self) -> Result<Params> {
        self.into_iter().collect::<Vec<_>>().into_params()
    }
}

// Named parameters with static string keys to avoid String allocations.
impl<T: IntoValue, const N: usize> Sealed for [(&'static str, T); N] {}
impl<T: IntoValue, const N: usize> IntoParams for [(&'static str, T); N] {
    fn into_params(self) -> Result<Params> {
        let values = self
            .into_iter()
            .map(|(k, v)| Ok((Cow::Borrowed(k), v.into_value()?)))
            .collect::<Result<Vec<_>>>()?;

        Ok(Params::Named(values))
    }
}

impl<T: IntoValue + Clone, const N: usize> Sealed for &[T; N] {}
impl<T: IntoValue + Clone, const N: usize> IntoParams for &[T; N] {
    fn into_params(self) -> Result<Params> {
        self.iter().cloned().collect::<Vec<_>>().into_params()
    }
}

macro_rules! tuple_into_params {
    ($count:literal : $(($field:tt $ftype:ident)),* $(,)?) => {
        impl<$($ftype,)*> Sealed for ($($ftype,)*) where $($ftype: IntoValue,)* {}
        impl<$($ftype,)*> IntoParams for ($($ftype,)*) where $($ftype: IntoValue,)* {
            fn into_params(self) -> Result<Params> {
                let params = Params::Positional(vec![$(self.$field.into_value()?),*]);
                Ok(params)
            }
        }
    }
}

macro_rules! named_tuple_into_params {
    ($count:literal : $(($field:tt $ftype:ident)),* $(,)?) => {
        impl<$($ftype,)*> Sealed for ($((&'static str, $ftype),)*) where $($ftype: IntoValue,)* {}
        impl<$($ftype,)*> IntoParams for ($((&'static str, $ftype),)*) where $($ftype: IntoValue,)* {
            fn into_params(self) -> Result<Params> {
                let params = Params::Named(vec![$((Cow::Borrowed(self.$field.0), self.$field.1.into_value()?)),*]);
                Ok(params)
            }
        }
    }
}

named_tuple_into_params!(1: (0 A));
named_tuple_into_params!(2: (0 A), (1 B));
named_tuple_into_params!(3: (0 A), (1 B), (2 C));
named_tuple_into_params!(4: (0 A), (1 B), (2 C), (3 D));
named_tuple_into_params!(5: (0 A), (1 B), (2 C), (3 D), (4 E));
named_tuple_into_params!(6: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F));
named_tuple_into_params!(7: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G));
named_tuple_into_params!(8: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H));
named_tuple_into_params!(9: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I));
named_tuple_into_params!(10: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J));
named_tuple_into_params!(11: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J), (10 K));
named_tuple_into_params!(12: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J), (10 K), (11 L));
named_tuple_into_params!(13: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J), (10 K), (11 L), (12 M));
named_tuple_into_params!(14: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J), (10 K), (11 L), (12 M), (13 N));
named_tuple_into_params!(15: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J), (10 K), (11 L), (12 M), (13 N), (14 O));
named_tuple_into_params!(16: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J), (10 K), (11 L), (12 M), (13 N), (14 O), (15 P));

tuple_into_params!(1: (0 A));
tuple_into_params!(2: (0 A), (1 B));
tuple_into_params!(3: (0 A), (1 B), (2 C));
tuple_into_params!(4: (0 A), (1 B), (2 C), (3 D));
tuple_into_params!(5: (0 A), (1 B), (2 C), (3 D), (4 E));
tuple_into_params!(6: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F));
tuple_into_params!(7: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G));
tuple_into_params!(8: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H));
tuple_into_params!(9: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I));
tuple_into_params!(10: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J));
tuple_into_params!(11: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J), (10 K));
tuple_into_params!(12: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J), (10 K), (11 L));
tuple_into_params!(13: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J), (10 K), (11 L), (12 M));
tuple_into_params!(14: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J), (10 K), (11 L), (12 M), (13 N));
tuple_into_params!(15: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J), (10 K), (11 L), (12 M), (13 N), (14 O));
tuple_into_params!(16: (0 A), (1 B), (2 C), (3 D), (4 E), (5 F), (6 G), (7 H), (8 I), (9 J), (10 K), (11 L), (12 M), (13 N), (14 O), (15 P));

/// Converts a Rust type into a SQL [`Value`].
pub trait IntoValue {
    fn into_value(self) -> Result<Value>;
}

impl<T> IntoValue for T
where
    T: TryInto<Value>,
    T::Error: Into<crate::BoxError>,
{
    fn into_value(self) -> Result<Value> {
        self.try_into()
            .map_err(|e| Error::ToSqlConversionFailure(e.into()))
    }
}

impl IntoValue for Result<Value> {
    fn into_value(self) -> Result<Value> {
        self
    }
}

/// Construct positional params from a heterogeneous set of params types.
#[macro_export]
macro_rules! params {
    () => {
       ()
    };
    ($($value:expr),* $(,)?) => {{
        use $crate::params::IntoValue;
        [$($value.into_value()),*]
    }};
}

/// Construct named params from a heterogeneous set of params types.
#[macro_export]
macro_rules! named_params {
    () => {
        ()
    };
    ($($param_name:literal: $value:expr),* $(,)?) => {{
        use $crate::params::IntoValue;
        [$(($param_name, $value.into_value())),*]
    }};
}

#[cfg(test)]
#[path = "tests/unit/params/tests.rs"]
mod tests;
