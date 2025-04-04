//! This module contains all `Param` related utilities and traits.

use crate::Value;
use magnus::{RArray, RHash, RString, Ruby, TryConvert, Value as RValue};

impl TryConvert for Params {
    fn try_convert(val: RValue) -> std::result::Result<Self, magnus::Error> {
        if let Ok(array) = RArray::try_convert(val) {
            let values: std::result::Result<Vec<Value>, magnus::Error> = array
                .into_iter()
                .map(|v| {
                    RString::try_convert(v)
                        .and_then(|s| s.to_string())
                        .map(Value::Text)
                })
                .collect();

            return Ok(Params::Positional(values?));
        }
        if let Ok(hash) = RHash::try_convert(val) {
            let mut values: Vec<(String, Value)> = Vec::new();

            hash.foreach(|k: RValue, v: RValue| {
                let key = RString::try_convert(k)?.to_string()?;
                let value = Value::try_from(v).map_err(|_| {
                    magnus::Error::new(
                        Ruby::get_with(v).exception_arg_error(),
                        "value conversion failed",
                    )
                })?;
                values.push((key, value));
                Ok(magnus::r_hash::ForEach::Continue)
            })?;
            return Ok(Params::Named(values));
        }
        Ok(Params::None)
    }
}

#[derive(Debug, Clone)]
#[doc(hidden)]
pub enum Params {
    None,
    Positional(Vec<Value>),
    Named(Vec<(String, Value)>),
}
