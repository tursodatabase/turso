use turso_core::Value;

use crate::{
    generation::{ArbitraryFrom, GenerationContext},
    model::table::{ColumnType, SimValue},
};

pub struct LTValue(pub SimValue);

impl ArbitraryFrom<(&SimValue, ColumnType)> for LTValue {
    fn arbitrary_from<R: rand::Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        _context: &C,
        (value, _col_type): (&SimValue, ColumnType),
    ) -> Self {
        let new_value = match &value.0 {
            Value::Integer(i) => {
                // Handle edge case: if i is at or near minimum, return minimum
                if *i <= i64::MIN + 1 {
                    Value::Integer(i64::MIN)
                } else {
                    Value::Integer(rng.random_range(i64::MIN..*i - 1))
                }
            }
            Value::Float(f) => Value::Float(f - rng.random_range(0.0..1e10)),
            value @ Value::Text(..) => {
                // Either shorten the string, or make at least one character smaller and mutate the rest
                let t = value.to_string();
                if t.is_empty() {
                    // Empty string - nothing is less, return empty
                    Value::build_text(String::new())
                } else if rng.random_bool(0.01) {
                    let mut t = t;
                    t.pop();
                    Value::build_text(t)
                } else {
                    Value::build_text(mutate_string(&t, rng, MutationType::Decrement))
                }
            }
            Value::Blob(b) => {
                // Either shorten the blob, or make at least one byte smaller and mutate the rest
                let mut b = b.clone();
                if b.is_empty() {
                    // Empty blob - nothing is less, return empty
                    Value::Blob(b)
                } else if rng.random_bool(0.01) {
                    b.pop();
                    Value::Blob(b)
                } else {
                    let index = rng.random_range(0..b.len());
                    b[index] = b[index].saturating_sub(1);
                    // Mutate the rest of the blob
                    for val in b.iter_mut().skip(index + 1) {
                        *val = rng.random_range(0..=255);
                    }
                    Value::Blob(b)
                }
            }
            // A value with storage class NULL is considered less than any other value (including another value with storage class NULL)
            Value::Null => Value::Null,
        };
        Self(SimValue(new_value))
    }
}

pub struct GTValue(pub SimValue);

impl ArbitraryFrom<(&SimValue, ColumnType)> for GTValue {
    fn arbitrary_from<R: rand::Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        context: &C,
        (value, col_type): (&SimValue, ColumnType),
    ) -> Self {
        let new_value = match &value.0 {
            Value::Integer(i) => {
                // Handle edge case: if i is at or near maximum, return maximum
                if *i >= i64::MAX - 1 {
                    Value::Integer(i64::MAX)
                } else {
                    Value::Integer(rng.random_range(*i + 1..=i64::MAX))
                }
            }
            Value::Float(f) => {
                // Handle edge case: if f is at or near upper bound, return upper bound
                if *f >= 1e10 {
                    Value::Float(1e10)
                } else {
                    Value::Float(rng.random_range(*f..1e10))
                }
            }
            value @ Value::Text(..) => {
                // Either lengthen the string, or make at least one character larger and mutate the rest
                let t = value.to_string();
                if t.is_empty() {
                    // Empty string - append a character to make it greater
                    let c = if rng.random_bool(0.5) {
                        rng.random_range(UPPERCASE_A..=UPPERCASE_Z) as u8 as char
                    } else {
                        rng.random_range(LOWERCASE_A..=LOWERCASE_Z) as u8 as char
                    };
                    Value::build_text(c.to_string())
                } else if rng.random_bool(0.01) {
                    let mut t = t;
                    if rng.random_bool(0.5) {
                        t.push(rng.random_range(UPPERCASE_A..=UPPERCASE_Z) as u8 as char);
                    } else {
                        t.push(rng.random_range(LOWERCASE_A..=LOWERCASE_Z) as u8 as char);
                    }
                    Value::build_text(t)
                } else {
                    Value::build_text(mutate_string(&t, rng, MutationType::Increment))
                }
            }
            Value::Blob(b) => {
                // Either lengthen the blob, or make at least one byte larger and mutate the rest
                let mut b = b.clone();
                if b.is_empty() {
                    // Empty blob - append a byte to make it greater
                    b.push(rng.random_range(0..=255));
                    Value::Blob(b)
                } else if rng.random_bool(0.01) {
                    b.push(rng.random_range(0..=255));
                    Value::Blob(b)
                } else {
                    let index = rng.random_range(0..b.len());
                    b[index] = b[index].saturating_add(1);
                    // Mutate the rest of the blob
                    for val in b.iter_mut().skip(index + 1) {
                        *val = rng.random_range(0..=255);
                    }
                    Value::Blob(b)
                }
            }
            Value::Null => {
                // Any value is greater than NULL, except NULL
                SimValue::arbitrary_from(rng, context, col_type).0
            }
        };
        Self(SimValue(new_value))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum MutationType {
    Decrement,
    Increment,
}

const UPPERCASE_A: u32 = 'A' as u32;
const UPPERCASE_Z: u32 = 'Z' as u32;
const LOWERCASE_A: u32 = 'a' as u32;
const LOWERCASE_Z: u32 = 'z' as u32;

fn mutate_string<R: rand::Rng + ?Sized>(
    t: &str,
    rng: &mut R,
    mutation_type: MutationType,
) -> String {
    if t.is_empty() {
        // Handle empty string: for increment, return a random char; for decrement, stay empty
        return if mutation_type == MutationType::Increment {
            let c = if rng.random_bool(0.5) {
                rng.random_range(UPPERCASE_A..=UPPERCASE_Z) as u8 as char
            } else {
                rng.random_range(LOWERCASE_A..=LOWERCASE_Z) as u8 as char
            };
            c.to_string()
        } else {
            String::new()
        };
    }

    let mut chars = t.chars().map(|c| c as u32).collect::<Vec<_>>();

    // Try to find a character that can be safely incremented/decremented
    let mut index = None;
    for _ in 0..100 {
        let candidate = rng.random_range(0..chars.len());
        let c = chars[candidate];
        // Check if character is in the "safe" range (not at boundary)
        let can_decrement = ((UPPERCASE_A + 1)..=UPPERCASE_Z).contains(&c)
            || ((LOWERCASE_A + 1)..=LOWERCASE_Z).contains(&c);
        let can_increment =
            (UPPERCASE_A..UPPERCASE_Z).contains(&c) || (LOWERCASE_A..LOWERCASE_Z).contains(&c);

        let can_mutate = match mutation_type {
            MutationType::Decrement => can_decrement,
            MutationType::Increment => can_increment,
        };
        if can_mutate {
            index = Some(candidate);
            break;
        }
    }

    // Fallback: if no suitable character found, use length-based mutation
    if index.is_none() {
        return if mutation_type == MutationType::Decrement {
            // For decrement: remove last char (shorter string < original)
            let mut s = t.to_string();
            s.pop();
            s
        } else {
            // For increment: append a char (longer string > original)
            format!("{t}A")
        };
    }

    let index = index.unwrap();
    if mutation_type == MutationType::Decrement {
        chars[index] -= 1;
    } else {
        chars[index] += 1;
    }

    // Mutate the rest of the string with printable ASCII characters
    for val in chars.iter_mut().skip(index + 1) {
        if rng.random_bool(0.5) {
            *val = rng.random_range(UPPERCASE_A..=UPPERCASE_Z);
        } else {
            *val = rng.random_range(LOWERCASE_A..=LOWERCASE_Z);
        }
    }

    chars
        .into_iter()
        .map(|c| char::from_u32(c).unwrap())
        .collect::<String>()
}

#[cfg(test)]
mod tests {
    use anarchist_readable_name_generator_lib::readable_name;

    use super::*;

    #[test]
    fn test_mutate_string_fuzz() {
        let mut rng = rand::rng();
        for _ in 0..1000 {
            let mut t = readable_name();
            while !t.is_ascii() {
                t = readable_name();
            }
            let t2 = mutate_string(&t, &mut rng, MutationType::Decrement);
            assert!(t2.is_ascii(), "{}", t);
            assert!(t2 < t);
        }
        for _ in 0..1000 {
            let mut t = readable_name();
            while !t.is_ascii() {
                t = readable_name();
            }
            let t2 = mutate_string(&t, &mut rng, MutationType::Increment);
            assert!(t2.is_ascii(), "{}", t);
            assert!(t2 > t);
        }
    }
}
