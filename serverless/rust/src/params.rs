use std::borrow::Cow;

use crate::Value;

/// Parameters for a SQL statement.
///
/// # Positional parameters
///
/// ```ignore
/// use turso_serverless::params;
/// conn.execute("SELECT ?, ?", params![1, "foo"]).await?;
/// conn.execute("SELECT ?", vec![Value::Integer(42)]).await?;
/// ```
///
/// # Named parameters
///
/// Named parameter keys must include the SQL prefix used in the statement,
/// for example `:name`, `@name`, `$name`, or `?1`.
///
/// ```ignore
/// use turso_serverless::named_params;
/// conn.execute("SELECT :a, :b", named_params![":a": 1, ":b": "foo"]).await?;
/// ```
#[derive(Debug, Clone)]
pub enum Params {
    None,
    Positional(Vec<Value>),
    Named(Vec<(Cow<'static, str>, Value)>),
}

impl From<()> for Params {
    fn from(_: ()) -> Self {
        Params::None
    }
}

impl From<Vec<Value>> for Params {
    fn from(values: Vec<Value>) -> Self {
        Params::Positional(values)
    }
}

impl From<Vec<(String, Value)>> for Params {
    fn from(values: Vec<(String, Value)>) -> Self {
        Params::Named(
            values
                .into_iter()
                .map(|(k, v)| (Cow::Owned(k), v))
                .collect(),
        )
    }
}

impl<const N: usize> From<[(&'static str, Value); N]> for Params {
    fn from(values: [(&'static str, Value); N]) -> Self {
        Params::Named(
            values
                .into_iter()
                .map(|(k, v)| (Cow::Borrowed(k), v))
                .collect(),
        )
    }
}

/// Construct positional params from a heterogeneous set of value types.
///
/// Each argument is converted via `Into<Value>`.
///
/// ```ignore
/// use turso_serverless::params;
/// conn.execute("SELECT ?, ?, ?", params![1, "hello", 3.15]).await?;
/// ```
#[macro_export]
macro_rules! params {
    () => {
        $crate::Params::None
    };
    ($($value:expr),* $(,)?) => {
        $crate::Params::Positional(vec![$($crate::Value::from($value)),*])
    };
}

/// Construct named params from a heterogeneous set of value types.
///
/// Each value is converted via `Into<Value>`.
///
/// ```ignore
/// use turso_serverless::named_params;
/// conn.execute("SELECT :a, :b", named_params![":a": 1, ":b": "hello"]).await?;
/// ```
#[macro_export]
macro_rules! named_params {
    () => {
        $crate::Params::None
    };
    ($($param_name:literal: $value:expr),* $(,)?) => {
        $crate::Params::Named(vec![$(
            (::std::borrow::Cow::Borrowed($param_name), $crate::Value::from($value))
        ),*])
    };
}

#[cfg(test)]
mod tests {
    use crate::Value;

    #[test]
    fn test_params_macro_positional() {
        let p = params![1, "hello", 3.15];
        match p {
            crate::Params::Positional(v) => {
                assert_eq!(v.len(), 3);
                assert_eq!(v[0], Value::Integer(1));
                assert_eq!(v[1], Value::Text("hello".to_string()));
                assert_eq!(v[2], Value::Real(3.15));
            }
            _ => panic!("expected Positional"),
        }
    }

    #[test]
    fn test_params_macro_empty() {
        let p = params![];
        assert!(matches!(p, crate::Params::None));
    }

    #[test]
    fn test_named_params_macro() {
        let p = named_params![":a": 1, ":b": "world"];
        match p {
            crate::Params::Named(v) => {
                assert_eq!(v.len(), 2);
                assert_eq!(v[0].0.as_ref(), ":a");
                assert_eq!(v[0].1, Value::Integer(1));
                assert_eq!(v[1].0.as_ref(), ":b");
                assert_eq!(v[1].1, Value::Text("world".to_string()));
            }
            _ => panic!("expected Named"),
        }
    }

    #[test]
    fn test_vec_value_into_params() {
        let p: crate::Params = vec![Value::Integer(42), Value::Text("hi".into())].into();
        match p {
            crate::Params::Positional(v) => assert_eq!(v.len(), 2),
            _ => panic!("expected Positional"),
        }
    }

    #[test]
    fn test_unit_into_params() {
        let p: crate::Params = ().into();
        assert!(matches!(p, crate::Params::None));
    }
}
