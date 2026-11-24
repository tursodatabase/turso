use regex::Regex;
use turso_ext::{register_extension, scalar, Value, ValueType};

register_extension! {
    scalars: { regexp, regexp_like, regexp_substr, regexp_replace, regexp_capture }
}

#[scalar(name = "regexp")]
fn regexp(args: &[Value]) -> Value {
    if args.len() != 2 {
        return wrong_arity("regexp");
    }
    regex(&args[0], &args[1])
}

fn regex(pattern: &Value, haystack: &Value) -> Value {
    match (pattern.value_type(), haystack.value_type()) {
        (ValueType::Text, ValueType::Text) => {
            let Some(pattern) = pattern.to_text() else {
                return Value::null();
            };
            let Some(haystack) = haystack.to_text() else {
                return Value::null();
            };
            let re = match Regex::new(pattern) {
                Ok(re) => re,
                Err(_) => return Value::null(),
            };
            Value::from_integer(re.is_match(haystack) as i64)
        }
        _ => Value::null(),
    }
}

#[scalar(name = "regexp_like")]
fn regexp_like(args: &[Value]) -> Value {
    if args.len() != 2 {
        return wrong_arity("regexp_like");
    }
    regex(&args[1], &args[0])
}

#[scalar(name = "regexp_substr")]
fn regexp_substr(&self, args: &[Value]) -> Value {
    if args.len() != 2 {
        return wrong_arity("regexp_substr");
    }
    match (args[0].value_type(), args[1].value_type()) {
        (ValueType::Text, ValueType::Text) => {
            let Some(haystack) = &args[0].to_text() else {
                return Value::null();
            };
            let Some(pattern) = &args[1].to_text() else {
                return Value::null();
            };
            let re = match Regex::new(pattern) {
                Ok(re) => re,
                Err(_) => return Value::null(),
            };
            match re.find(haystack) {
                Some(mat) => Value::from_text(mat.as_str().to_string()),
                None => Value::null(),
            }
        }
        _ => Value::null(),
    }
}

/// Registered with `argc = -1`, so the engine does not check the argument
/// count. Indexing `args` from the `extern "C"` shim aborts the process.
fn wrong_arity(name: &str) -> Value {
    Value::error_with_message(format!("wrong number of arguments to function {name}()"))
}

#[scalar(name = "regexp_replace")]
fn regexp_replace(&self, args: &[Value]) -> Value {
    if args.len() < 2 {
        return Value::from_text("".to_string());
    }

    let Some(source_text) = args[0].to_text() else {
        return Value::from_text("".to_string());
    };

    let Some(pattern_text) = args[1].to_text() else {
        return Value::from_text("".to_string());
    };

    let replacement = args.get(2).and_then(|v| v.to_text()).unwrap_or("");

    let re = match Regex::new(pattern_text) {
        Ok(re) => re,
        Err(_) => return Value::from_text("".to_string()),
    };

    Value::from_text(re.replace(source_text, replacement).to_string())
}

#[scalar(name = "regexp_capture")]
fn regexp_capture(args: &[Value]) -> Value {
    if args.len() < 2 {
        return Value::from_text("".to_string());
    }
    let Some(source_text) = args[0].to_text() else {
        return Value::null();
    };
    let Some(pattern_text) = args[1].to_text() else {
        return Value::null();
    };

    let group_index: usize = args
        .get(2)
        .and_then(|v| v.to_integer())
        .map(|n| n as usize)
        .unwrap_or(1);

    let re = match Regex::new(pattern_text) {
        Ok(re) => re,
        Err(_) => return Value::null(),
    };

    if let Some(caps) = re.captures(source_text) {
        if let Some(m) = caps.get(group_index) {
            return Value::from_text(m.as_str().to_string());
        }
    }

    Value::null()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Call the C shim `SELECT regexp(...)` uses.
    fn call(
        func: unsafe extern "C" fn(
            usize,
            i32,
            *const Value,
            Option<turso_ext::ContextDestructor>,
            Option<turso_ext::ValueDestructor>,
        ) -> Value,
        args: &[Value],
    ) -> Value {
        unsafe { func(0, args.len() as i32, args.as_ptr(), None, None) }
    }

    fn text(s: &str) -> Value {
        Value::from_text(s.to_string())
    }

    fn assert_wrong_arity(
        name: &str,
        func: unsafe extern "C" fn(
            usize,
            i32,
            *const Value,
            Option<turso_ext::ContextDestructor>,
            Option<turso_ext::ValueDestructor>,
        ) -> Value,
    ) {
        for argc in [0usize, 1, 3] {
            let args: Vec<Value> = (0..argc).map(|_| text("a")).collect();
            let result = call(func, &args);
            let details = result
                .to_error_details()
                .unwrap_or_else(|| panic!("{name}/{argc} should error, got {result:?}"));
            let expected = format!("wrong number of arguments to function {name}()");
            assert_eq!(
                details.1.as_deref(),
                Some(expected.as_str()),
                "unexpected error for {name}/{argc}"
            );
        }
    }

    #[test]
    fn regexp_rejects_wrong_arity() {
        assert_wrong_arity("regexp", regexp);
    }

    #[test]
    fn regexp_like_rejects_wrong_arity() {
        assert_wrong_arity("regexp_like", regexp_like);
    }

    #[test]
    fn regexp_substr_rejects_wrong_arity() {
        assert_wrong_arity("regexp_substr", regexp_substr);
    }

    #[test]
    fn regexp_matches_with_two_arguments() {
        assert_eq!(
            call(regexp, &[text("^a.c$"), text("abc")]).to_integer(),
            Some(1)
        );
        assert_eq!(
            call(regexp, &[text("^a.c$"), text("abd")]).to_integer(),
            Some(0)
        );
    }
}
