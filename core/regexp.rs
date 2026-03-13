use crate::ext::register_scalar_function;
use turso_ext::{scalar, ExtensionApi, Value};

pub fn register_extension(ext_api: &mut ExtensionApi) {
    unsafe {
        register_scalar_function(ext_api.ctx, c"regexp".as_ptr(), regexp);
    }
}

#[scalar(name = "regexp")]
fn regexp(args: &[Value]) -> Value {
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
