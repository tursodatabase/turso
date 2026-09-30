use regex::Regex;
use turso_ext::{register_extension, scalar, Value, ValueType};

register_extension! {
    scalars: { regexp, regexp_like, regexp_substr, regexp_replace, regexp_capture }
}

#[scalar(name = "regexp")]
fn regexp(args: &[Value]) -> Value {
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
    regex(&args[1], &args[0])
}

#[scalar(name = "regexp_substr")]
fn regexp_substr(&self, args: &[Value]) -> Value {
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
