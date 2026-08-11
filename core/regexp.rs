use crate::ext::register_scalar_function;
use turso_ext::{scalar, ExtensionApi, Value};

pub fn register_extension(ext_api: &mut ExtensionApi) {
    unsafe {
        register_scalar_function(ext_api.ctx, c"regexp".as_ptr(), regexp);
    }
}

#[scalar(name = "regexp")]
fn regexp(args: &[Value]) -> Value {
    // Registered as varargs, so this is the only arity check. Indexing out of
    // bounds below would panic through the `extern "C"` shim and abort.
    if args.len() != 2 {
        return Value::error_with_message("wrong number of arguments to function regexp()".into());
    }
    let Some(pattern) = args[0].to_text_coerced() else {
        return Value::null();
    };
    let Some(haystack) = args[1].to_text_coerced() else {
        return Value::null();
    };

    let re = match regex::Regex::new(&pattern) {
        Ok(re) => re,
        Err(_) => return Value::null(),
    };

    Value::from_integer(re.is_match(&haystack) as i64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use turso_ext::ValueType;

    /// Go through the generated C-ABI shim, the way `SELECT regexp(..)` does.
    fn call(args: &[Value]) -> Value {
        unsafe { regexp(0, args.len() as i32, args.as_ptr(), None, None) }
    }

    fn text(s: &str) -> Value {
        Value::from_text(s.to_string())
    }

    #[test]
    fn regexp_rejects_wrong_arity() {
        // argc == 1 used to be admitted and then index args[1], aborting.
        for argc in [0usize, 1, 3] {
            let args: Vec<Value> = (0..argc).map(|_| text("a")).collect();
            let result = call(&args);
            let details = result
                .to_error_details()
                .unwrap_or_else(|| panic!("regexp/{argc} should error, got {result:?}"));
            assert_eq!(
                details.1.as_deref(),
                Some("wrong number of arguments to function regexp()"),
                "unexpected error for regexp/{argc}"
            );
        }
    }

    #[test]
    fn regexp_matches_with_two_arguments() {
        assert_eq!(call(&[text("^a.c$"), text("abc")]).to_integer(), Some(1));
        assert_eq!(call(&[text("^a.c$"), text("abd")]).to_integer(), Some(0));
        // An invalid pattern is NULL, not an error.
        assert_eq!(
            call(&[text("("), text("abc")]).value_type(),
            ValueType::Null
        );
    }
}
