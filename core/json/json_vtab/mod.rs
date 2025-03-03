use std::{fmt::Display, iter::successors};

use limbo_ext::{register_extension, ResultCode, Value};

use crate::OwnedValue;

use super::{
    get_json_value, json_path,
    json_path::{JsonPath, PathElement},
    Val,
};

pub mod json_each;
pub mod json_tree;

use json_each::JsonEachVTab;
use json_tree::JsonTreeVTab;

register_extension! {
    vtabs: { JsonEachVTab, JsonTreeVTab }
}

macro_rules! try_option {
    ($expr:expr, $err:expr) => {
        match $expr {
            Some(val) => val,
            None => return $err,
        }
    };
}

fn filter(args: &[Value]) -> Result<(Val, InPlaceJsonPath), ResultCode> {
    if args.len() != 1 && args.len() != 2 {
        return Err(ResultCode::InvalidArgs);
    }
    // TODO: For now we are not dealing with JSONB

    // TODO include json subtype when it is merged to main
    let json_val = match args[0].value_type() {
        limbo_ext::ValueType::Null => OwnedValue::Null,
        limbo_ext::ValueType::Text => OwnedValue::from_text(args[0].to_text().unwrap()),
        limbo_ext::ValueType::Integer => OwnedValue::Integer(args[0].to_integer().unwrap()),
        limbo_ext::ValueType::Float => OwnedValue::Float(args[0].to_float().unwrap()),
        limbo_ext::ValueType::Blob => OwnedValue::from_blob(args[0].to_blob().unwrap()),
        limbo_ext::ValueType::Error => return Err(ResultCode::InvalidArgs),
    };

    let json_val = try_option!(
        get_json_value(&json_val).ok(),
        Err(ResultCode::InvalidArgs) // Invalid Json
                                     // TODO: see how to return a meaningful error message via ResultCode
    );
    let path = args
        .get(1)
        .map(|v| v.to_text().unwrap_or("$"))
        .unwrap_or("$");

    let j_path = try_option!(json_path(path).ok(), Err(ResultCode::InvalidArgs));

    let json_val = try_option!(advance_path(json_val, &j_path), Err(ResultCode::EOF));

    Ok((json_val, InPlaceJsonPath::from_json_path(path, &j_path)))
}

// Modified json_extract_single code
fn advance_path(val: Val, path: &JsonPath) -> Option<Val> {
    let mut current_element = val;

    for element in path.elements.iter() {
        match element {
            PathElement::Root() => continue,
            PathElement::Key(key, _) => match current_element {
                Val::Object(map) => {
                    if let Some((_, value)) = map.into_iter().find(|(k, _)| k == key) {
                        current_element = value;
                    } else {
                        return None;
                    }
                }
                _ => return None,
            },
            PathElement::ArrayLocator(idx) => match current_element {
                Val::Array(ref mut array) => {
                    if let Some(mut idx) = *idx {
                        if idx < 0 {
                            idx += array.len() as i32;
                        }

                        if idx < array.len() as i32 {
                            current_element = array.remove(idx as usize);
                        } else {
                            return None;
                        }
                    }
                }
                _ => return None,
            },
        }
    }
    Some(current_element)
}

trait JsonPathExt {
    fn count_elements(&self) -> Vec<usize>;
}

impl JsonPathExt for JsonPath<'_> {
    fn count_elements(&self) -> Vec<usize> {
        let mut count = Vec::new();
        for element in &self.elements {
            match element {
                PathElement::Root() => {
                    count.push(1);
                }
                PathElement::Key(key, _) => {
                    count.push(key.len() + 1); // +1 here for the dot
                }
                PathElement::ArrayLocator(idx) => {
                    // Counts the length of idx number and +2 for the []
                    count.push(successors(idx.clone(), |&n| (n >= 10).then(|| n / 10)).count() + 2);
                }
            }
        }
        count
    }
}

trait ValExt {
    fn type_name(&self) -> String;
    fn to_value(&self) -> Value;
    fn atom_value(&self) -> Value;
    fn key_value_count(&self) -> usize;
}

impl ValExt for Val {
    fn type_name(&self) -> String {
        let val = match self {
            Val::Null => "null",
            Val::Bool(v) => {
                if *v {
                    "true"
                } else {
                    "false"
                }
            }
            Val::Integer(_) => "integer",
            Val::Float(_) => "real",
            Val::String(_) => "text",
            Val::Array(_) => "array",
            Val::Object(_) => "object",
            Val::Removed => unreachable!(),
        };
        val.to_string()
    }

    fn to_value(&self) -> Value {
        match self {
            Val::Null => Value::null(),
            Val::Bool(v) => {
                if *v {
                    Value::from_integer(1)
                } else {
                    Value::from_integer(0)
                }
            }
            Val::Integer(v) => Value::from_integer(*v),
            Val::Float(v) => Value::from_float(*v),
            Val::String(v) => Value::from_text(v.clone()),
            Val::Removed => unreachable!(),
            // TODO: as we cannot declare a subtype for JSON I have to return text here
            v => Value::from_text(v.to_string()),
        }
    }

    fn atom_value(&self) -> Value {
        match self {
            Val::Null => Value::null(),
            Val::Bool(v) => {
                if *v {
                    Value::from_integer(1)
                } else {
                    Value::from_integer(0)
                }
            }
            Val::Integer(v) => Value::from_integer(*v),
            Val::Float(v) => Value::from_float(*v),
            Val::String(v) => Value::from_text(v.clone()),
            Val::Removed => unreachable!(),
            _ => Value::null(),
        }
    }

    fn key_value_count(&self) -> usize {
        match self {
            Val::Array(v) => v.iter().map(|val| val.key_value_count()).sum::<usize>() + 1,
            Val::Object(v) => v.iter().map(|(_, val)| val.key_value_count() + 1).sum(),
            Val::Removed => unreachable!(),
            _ => 1,
        }
    }
}

impl Display for Val {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Val::Null => write!(f, "{}", ""),
            Val::Bool(v) => {
                if *v {
                    write!(f, "{}", "1")
                } else {
                    write!(f, "{}", "0")
                }
            }
            Val::Integer(v) => write!(f, "{}", v),
            Val::Float(v) => write!(f, "{}", v),
            Val::String(v) => write!(f, "{}", v),
            Val::Array(vals) => {
                let mut vals_iter = vals.iter();
                write!(f, "[")?;
                let mut comma = false;
                while let Some(val) = vals_iter.next() {
                    if comma {
                        write!(f, ",")?;
                    }
                    write!(f, "{}", val.to_string())?; // Call format recursively
                    comma = true;
                }
                write!(f, "]")
            }
            Val::Object(vals) => {
                write!(f, "{{")?;
                let mut comma = false;
                for (key, val) in vals {
                    if comma {
                        write!(f, ",")?;
                    }
                    write!(f, "\"{}\": {}", key, val.to_string())?; // Call format recursively
                    comma = true;
                }
                write!(f, "}}")
            }
            Val::Removed => unreachable!(),
        }
    }
}

trait VecExt<T> {
    fn remove_first(&mut self) -> Option<T>;
}

impl<T> VecExt<T> for Vec<T> {
    fn remove_first(&mut self) -> Option<T> {
        if self.is_empty() {
            return None;
        }
        Some(self.remove(0))
    }
}

#[derive(Debug, Default, Clone)]
struct InPlaceJsonPath {
    path: String,
    items_len: Vec<usize>, // Each element stores the length of chars pushed into path for that path element
}

impl InPlaceJsonPath {
    fn pop(&mut self) -> Option<()> {
        if let Some(mut len) = self.items_len.pop() {
            if len == 0 {
                return Some(());
            }
            // Remove the last element string
            while let Some(_) = self.path.pop() {
                len -= 1;
                if len == 0 {
                    break;
                }
            }
            return Some(());
        }
        None
    }

    fn push(&mut self, element: String) {
        self.items_len.push(element.len());
        self.path.push_str(&element);
    }

    fn push_array_locator(&mut self, key: &str) {
        self.push(format!("[{}]", key));
    }

    fn push_key(&mut self, key: &str) {
        self.push(format!(".{}", key));
    }

    fn last(&self) -> Option<&str> {
        if let Some(len) = self.items_len.last() {
            let path_len = self.path.len();
            Some(&self.path[path_len - len..path_len])
        } else {
            None
        }
    }

    fn from_json_path(path: &str, j_path: &JsonPath<'_>) -> Self {
        Self {
            path: path.to_owned(),
            items_len: j_path.count_elements(),
        }
    }
}
