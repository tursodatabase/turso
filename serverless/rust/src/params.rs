//! Parameter binding utilities, mirroring the embedded `turso` crate's
//! `IntoParams`/`IntoValue` traits so code written for the embedded driver
//! compiles unchanged against the serverless driver.
//!
//! # Positional parameters
//!
//! - Tuples of up to 16 heterogeneous items: `(1, "foo")`.
//! - The [`params!`](crate::params) macro for larger/heterogeneous lists.
//! - Const arrays for homogeneous types: `[1, 2, 3]`.
//!
//! # Named parameters
//!
//! Keys must include the SQL prefix (`:name`, `@name`, `$name`, `?1`):
//! `((":a", 1), (":b", "foo"))`, [`named_params!`](crate::named_params), or
//! `[(":a", 1), (":b", 2)]`.

use std::borrow::Cow;

use crate::{Error, Result, Value};

mod sealed {
    pub trait Sealed {}
}

use sealed::Sealed;

/// Converts some type into parameters that can be passed to a statement.
///
/// Sealed — not intended to be implemented by hand.
pub trait IntoParams: Sealed {
    #[doc(hidden)]
    fn into_params(self) -> Result<Params>;
}

/// Bound parameters for a SQL statement.
#[derive(Debug, Clone)]
pub enum Params {
    None,
    Positional(Vec<Value>),
    Named(Vec<(Cow<'static, str>, Value)>),
}

/// Convert an owned iterator into positional [`Params`].
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

// Rust has no variadic generics, so `IntoParams` for tuples must be implemented
// per arity. `impl_tuple_params!` emits both the positional `(A, B, …)` and the
// named `((&str, A), (&str, B), …)` impls for one arity by *destructuring* self
// (so no positional `.0/.1` bookkeeping), and `impl_tuple_params_upto!` recurses
// over an ident list to cover every arity from the list's length down to 1 —
// so the whole family is generated from the single `A B C … P` line below.
//
// Tuples are only ergonomic sugar for a handful of heterogeneous values (16
// matches the embedded `turso` driver; extend the ident list to raise it). For
// an arbitrary number of parameters use a slice/`Vec`, `params!`, or
// `params_from_iter` — those are unbounded.
macro_rules! impl_tuple_params {
    ($($ftype:ident)+) => {
        impl<$($ftype: IntoValue),+> Sealed for ($($ftype,)+) {}
        impl<$($ftype: IntoValue),+> IntoParams for ($($ftype,)+) {
            #[allow(non_snake_case)]
            fn into_params(self) -> Result<Params> {
                let ($($ftype,)+) = self;
                Ok(Params::Positional(vec![$($ftype.into_value()?),+]))
            }
        }

        impl<$($ftype: IntoValue),+> Sealed for ($((&'static str, $ftype),)+) {}
        impl<$($ftype: IntoValue),+> IntoParams for ($((&'static str, $ftype),)+) {
            #[allow(non_snake_case)]
            fn into_params(self) -> Result<Params> {
                let ($($ftype,)+) = self;
                Ok(Params::Named(vec![$((Cow::Borrowed($ftype.0), $ftype.1.into_value()?)),+]))
            }
        }
    };
}

macro_rules! impl_tuple_params_upto {
    () => {};
    ($first:ident $($rest:ident)*) => {
        impl_tuple_params!($first $($rest)*);
        impl_tuple_params_upto!($($rest)*);
    };
}

impl_tuple_params_upto!(A B C D E F G H I J K L M N O P);

/// Converts a single value into a [`Value`], falling back to `TryInto<Value>`.
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

/// Construct positional params from a heterogeneous set of value types.
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

/// Construct named params from a heterogeneous set of value types.
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
mod tests {
    use super::IntoParams;
    use crate::Value;

    #[test]
    fn tuple_positional() {
        let p = (1i32, "foo").into_params().unwrap();
        match p {
            super::Params::Positional(v) => {
                assert_eq!(v, vec![Value::Integer(1), Value::Text("foo".into())]);
            }
            _ => panic!("expected positional"),
        }
    }

    #[test]
    fn array_positional() {
        let p = [1i32, 2, 3].into_params().unwrap();
        match p {
            super::Params::Positional(v) => assert_eq!(v.len(), 3),
            _ => panic!("expected positional"),
        }
    }

    #[test]
    fn params_macro() {
        let p = params![1, "hello", 3.15].into_params().unwrap();
        match p {
            super::Params::Positional(v) => {
                assert_eq!(v[0], Value::Integer(1));
                assert_eq!(v[1], Value::Text("hello".into()));
                assert_eq!(v[2], Value::Real(3.15));
            }
            _ => panic!("expected positional"),
        }
    }

    #[test]
    fn named_tuple() {
        let p = ((":a", 1i32), (":b", "x")).into_params().unwrap();
        match p {
            super::Params::Named(v) => {
                assert_eq!(v[0].0.as_ref(), ":a");
                assert_eq!(v[1].1, Value::Text("x".into()));
            }
            _ => panic!("expected named"),
        }
    }

    #[test]
    fn named_params_macro() {
        let p = named_params![":a": 1, ":b": "world"].into_params().unwrap();
        match p {
            super::Params::Named(v) => {
                assert_eq!(v[0].0.as_ref(), ":a");
                assert_eq!(v[1].1, Value::Text("world".into()));
            }
            _ => panic!("expected named"),
        }
    }

    #[test]
    fn params_from_iter_works() {
        let p = super::params_from_iter(vec![1i32, 2, 3])
            .into_params()
            .unwrap();
        match p {
            super::Params::Positional(v) => assert_eq!(v.len(), 3),
            _ => panic!("expected positional"),
        }
    }

    #[test]
    fn bulk_params_are_unbounded() {
        // Tuple sugar caps at arity 16, but the bulk paths (Vec /
        // params_from_iter) are unbounded — 50k parameters bind fine here; the
        // real ceiling is the engine's SQLITE_MAX_VARIABLE_NUMBER, not the API.
        let n = 50_000usize;
        let via_iter = super::params_from_iter((0..n as i64).map(Value::from))
            .into_params()
            .unwrap();
        let via_vec: super::Params = (0..n as i64)
            .map(Value::from)
            .collect::<Vec<_>>()
            .into_params()
            .unwrap();
        for p in [via_iter, via_vec] {
            match p {
                super::Params::Positional(v) => assert_eq!(v.len(), n),
                _ => panic!("expected positional"),
            }
        }
    }
}
