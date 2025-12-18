use std::collections::HashMap;

use regex::{Regex, RegexBuilder};

use crate::{
    function::MathFunc,
    numeric::{NullableInteger, Numeric},
    translate::collate::CollationSeq,
    types::{compare_immutable_single, AsValueRef, SeekOp},
    vdbe::affinity::Affinity,
    LimboError, Result, Value, ValueRef,
};

// we use math functions from Rust stdlib in order to be as portable as possible for the production version of the tursodb
#[cfg(not(test))]
mod cmath {
    pub fn exp(x: f64) -> f64 {
        x.exp()
    }
    pub fn log(x: f64) -> f64 {
        x.ln()
    }
    pub fn log10(x: f64) -> f64 {
        x.log(10.)
    }
    pub fn log2(x: f64) -> f64 {
        x.log(2.)
    }
    pub fn pow(x: f64, y: f64) -> f64 {
        x.powf(y)
    }
    pub fn sin(x: f64) -> f64 {
        x.sin()
    }
    pub fn sinh(x: f64) -> f64 {
        x.sinh()
    }
    pub fn asin(x: f64) -> f64 {
        x.asin()
    }
    pub fn asinh(x: f64) -> f64 {
        x.asinh()
    }
    pub fn cos(x: f64) -> f64 {
        x.cos()
    }
    pub fn cosh(x: f64) -> f64 {
        x.cosh()
    }
    pub fn acos(x: f64) -> f64 {
        x.acos()
    }
    pub fn acosh(x: f64) -> f64 {
        x.acosh()
    }
    pub fn tan(x: f64) -> f64 {
        x.tan()
    }
    pub fn tanh(x: f64) -> f64 {
        x.tanh()
    }
    pub fn atan(x: f64) -> f64 {
        x.atan()
    }
    pub fn atanh(x: f64) -> f64 {
        x.atanh()
    }
    pub fn atan2(x: f64, y: f64) -> f64 {
        x.atan2(y)
    }
}

// we use exactly same math function as SQLite in tests in order to avoid mismatch in the differential tests due to floating-point precision issues
#[cfg(test)]
mod cmath {
    extern "C" {
        pub fn exp(x: f64) -> f64;
        pub fn log(x: f64) -> f64;
        pub fn log10(x: f64) -> f64;
        pub fn log2(x: f64) -> f64;
        pub fn pow(x: f64, y: f64) -> f64;

        pub fn sin(x: f64) -> f64;
        pub fn sinh(x: f64) -> f64;
        pub fn asin(x: f64) -> f64;
        pub fn asinh(x: f64) -> f64;

        pub fn cos(x: f64) -> f64;
        pub fn cosh(x: f64) -> f64;
        pub fn acos(x: f64) -> f64;
        pub fn acosh(x: f64) -> f64;

        pub fn tan(x: f64) -> f64;
        pub fn tanh(x: f64) -> f64;
        pub fn atan(x: f64) -> f64;
        pub fn atanh(x: f64) -> f64;
        pub fn atan2(x: f64, y: f64) -> f64;
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum ComparisonOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl ComparisonOp {
    pub(super) fn compare<V1: AsValueRef, V2: AsValueRef>(
        &self,
        lhs: V1,
        rhs: V2,
        collation: CollationSeq,
    ) -> bool {
        let order = compare_immutable_single(lhs, rhs, collation);
        match self {
            ComparisonOp::Eq => order.is_eq(),
            ComparisonOp::Ne => order.is_ne(),
            ComparisonOp::Lt => order.is_lt(),
            ComparisonOp::Le => order.is_le(),
            ComparisonOp::Gt => order.is_gt(),
            ComparisonOp::Ge => order.is_ge(),
        }
    }

    pub(super) fn compare_nulls<V1: AsValueRef, V2: AsValueRef>(
        &self,
        lhs: V1,
        rhs: V2,
        null_eq: bool,
    ) -> bool {
        let (lhs, rhs) = (lhs.as_value_ref(), rhs.as_value_ref());
        assert!(matches!(lhs, ValueRef::Null) || matches!(rhs, ValueRef::Null));

        match self {
            ComparisonOp::Eq => {
                let both_null = lhs == rhs;
                null_eq && both_null
            }
            ComparisonOp::Ne => {
                let at_least_one_null = lhs != rhs;
                null_eq && at_least_one_null
            }
            ComparisonOp::Lt | ComparisonOp::Le | ComparisonOp::Gt | ComparisonOp::Ge => false,
        }
    }
}

impl From<SeekOp> for ComparisonOp {
    fn from(value: SeekOp) -> Self {
        match value {
            SeekOp::GE { eq_only: true } | SeekOp::LE { eq_only: true } => ComparisonOp::Eq,
            SeekOp::GE { eq_only: false } => ComparisonOp::Ge,
            SeekOp::GT => ComparisonOp::Gt,
            SeekOp::LE { eq_only: false } => ComparisonOp::Le,
            SeekOp::LT => ComparisonOp::Lt,
        }
    }
}

enum TrimType {
    All,
    Left,
    Right,
}

impl TrimType {
    fn trim<'a>(&self, text: &'a str, pattern: &[char]) -> &'a str {
        match self {
            TrimType::All => text.trim_matches(pattern),
            TrimType::Right => text.trim_end_matches(pattern),
            TrimType::Left => text.trim_start_matches(pattern),
        }
    }
}

impl Value {
    pub fn exec_lower(&self) -> Option<Self> {
        self.cast_text()
            .map(|s| Value::build_text(s.to_ascii_lowercase()))
    }

    pub fn exec_length(&self) -> Self {
        match self {
            Value::Text(t) => {
                let s = t.as_str();
                let len_before_null = s.find('\0').map_or_else(
                    || s.chars().count(),
                    |null_pos| s[..null_pos].chars().count(),
                );
                Value::Integer(len_before_null as i64)
            }
            Value::Integer(_) | Value::Float(_) => {
                // For numbers, SQLite returns the length of the string representation
                Value::Integer(self.to_string().chars().count() as i64)
            }
            Value::Blob(blob) => Value::Integer(blob.len() as i64),
            _ => self.to_owned(),
        }
    }

    pub fn exec_octet_length(&self) -> Self {
        match self {
            Value::Text(_) | Value::Integer(_) | Value::Float(_) => {
                Value::Integer(self.to_string().into_bytes().len() as i64)
            }
            Value::Blob(blob) => Value::Integer(blob.len() as i64),
            _ => self.to_owned(),
        }
    }

    pub fn exec_upper(&self) -> Option<Self> {
        self.cast_text()
            .map(|s| Value::build_text(s.to_ascii_uppercase()))
    }

    pub fn exec_sign(&self) -> Option<Value> {
        let v = Numeric::from_value_strict(self).try_into_f64()?;

        Some(Value::Integer(if v > 0.0 {
            1
        } else if v < 0.0 {
            -1
        } else {
            0
        }))
    }

    /// Generates the Soundex code for a given word
    pub fn exec_soundex(&self) -> Value {
        let s = match self {
            Value::Null => return Value::build_text("?000"),
            Value::Text(s) => {
                // return ?000 if non ASCII alphabet character is found
                if !s.as_str().chars().all(|c| c.is_ascii_alphabetic()) {
                    return Value::build_text("?000");
                }
                s.clone()
            }
            _ => return Value::build_text("?000"), // For unsupported types, return NULL
        };

        // Remove numbers and spaces
        let word: String = s
            .as_str()
            .chars()
            .filter(|c| !c.is_ascii_digit())
            .collect::<String>()
            .replace(" ", "");
        if word.is_empty() {
            return Value::build_text("0000");
        }

        let soundex_code = |c| match c {
            'b' | 'f' | 'p' | 'v' => Some('1'),
            'c' | 'g' | 'j' | 'k' | 'q' | 's' | 'x' | 'z' => Some('2'),
            'd' | 't' => Some('3'),
            'l' => Some('4'),
            'm' | 'n' => Some('5'),
            'r' => Some('6'),
            _ => None,
        };

        // Convert the word to lowercase for consistent lookups
        let word = word.to_lowercase();
        let first_letter = word
            .chars()
            .next()
            .expect("word should not be empty after validation");

        // Remove all occurrences of 'h' and 'w' except the first letter
        let code: String = word
            .chars()
            .skip(1)
            .filter(|&ch| ch != 'h' && ch != 'w')
            .fold(first_letter.to_string(), |mut acc, ch| {
                acc.push(ch);
                acc
            });

        // Replace consonants with digits based on Soundex mapping
        let tmp: String = code
            .chars()
            .map(|ch| match soundex_code(ch) {
                Some(code) => code.to_string(),
                None => ch.to_string(),
            })
            .collect();

        // Remove adjacent same digits
        let tmp = tmp.chars().fold(String::new(), |mut acc, ch| {
            if !acc.ends_with(ch) {
                acc.push(ch);
            }
            acc
        });

        // Remove all occurrences of a, e, i, o, u, y except the first letter
        let mut result = tmp
            .chars()
            .enumerate()
            .filter(|(i, ch)| *i == 0 || !matches!(ch, 'a' | 'e' | 'i' | 'o' | 'u' | 'y'))
            .map(|(_, ch)| ch)
            .collect::<String>();

        // If the first symbol is a digit, replace it with the saved first letter
        if let Some(first_digit) = result.chars().next() {
            if first_digit.is_ascii_digit() {
                result.replace_range(0..1, &first_letter.to_string());
            }
        }

        // Append zeros if the result contains less than 4 characters
        while result.len() < 4 {
            result.push('0');
        }

        // Retain the first 4 characters and convert to uppercase
        result.truncate(4);
        Value::build_text(result.to_uppercase())
    }

    pub fn exec_abs(&self) -> Result<Self> {
        Ok(match self {
            Value::Null => Value::Null,
            Value::Integer(v) => {
                Value::Integer(v.checked_abs().ok_or(LimboError::IntegerOverflow)?)
            }
            Value::Float(non_nan) => Value::Float(non_nan.abs()),
            _ => {
                let s = match self {
                    Value::Text(text) => text.to_string(),
                    Value::Blob(blob) => String::from_utf8_lossy(blob).to_string(),
                    _ => unreachable!(),
                };

                crate::numeric::str_to_f64(s)
                    .map(|v| Value::Float(f64::from(v).abs()))
                    .unwrap_or(Value::Float(0.0))
            }
        })
    }

    pub fn exec_random<F>(generate_random_number: F) -> Self
    where
        F: Fn() -> i64,
    {
        Value::Integer(generate_random_number())
    }

    pub fn exec_randomblob<F>(&self, fill_bytes: F) -> Value
    where
        F: Fn(&mut [u8]),
    {
        let length = match self {
            Value::Integer(i) => *i,
            Value::Float(f) => *f as i64,
            Value::Text(t) => t.as_str().parse().unwrap_or(1),
            _ => 1,
        }
        .max(1) as usize;

        let mut blob: Vec<u8> = vec![0; length];
        fill_bytes(&mut blob);
        Value::Blob(blob)
    }

    pub fn exec_quote(&self) -> Self {
        use std::fmt::Write;
        match self {
            Value::Null => Value::build_text("NULL"),
            Value::Integer(_) | Value::Float(_) => self.to_owned(),
            Value::Blob(b) => {
                // SQLite returns X'hexdigits' for blobs
                let mut quoted = String::with_capacity(3 + b.len() * 2);
                quoted.push_str("X'");
                for byte in b.iter() {
                    write!(&mut quoted, "{byte:02X}").expect("unable to write hex bytes");
                }
                quoted.push('\'');
                Value::build_text(quoted)
            }
            Value::Text(s) => {
                let mut quoted = String::with_capacity(s.as_str().len() + 2);
                quoted.push('\'');
                for c in s.as_str().chars() {
                    if c == '\0' {
                        break;
                    } else if c == '\'' {
                        quoted.push('\'');
                        quoted.push(c);
                    } else {
                        quoted.push(c);
                    }
                }
                quoted.push('\'');
                Value::build_text(quoted)
            }
        }
    }

    pub fn exec_nullif(&self, second_value: &Self) -> Self {
        if self != second_value {
            self.clone()
        } else {
            Value::Null
        }
    }

    pub fn exec_substring(
        value: &Value,
        start_value: &Value,
        length_value: Option<&Value>,
    ) -> Value {
        /// Function is stabilized but not released for version 1.88 \
        /// https://doc.rust-lang.org/src/core/str/mod.rs.html#453
        const fn ceil_char_boundary(s: &str, index: usize) -> usize {
            const fn is_utf8_char_boundary(c: u8) -> bool {
                // This is bit magic equivalent to: b < 128 || b >= 192
                (c as i8) >= -0x40
            }

            if index >= s.len() {
                s.len()
            } else {
                let mut i = index;
                while i < s.len() {
                    if is_utf8_char_boundary(s.as_bytes()[i]) {
                        break;
                    }
                    i += 1;
                }

                //  The character boundary will be within four bytes of the index
                debug_assert!(i <= index + 3);

                i
            }
        }

        fn calculate_postions(
            start: i64,
            bytes_len: usize,
            length_value: Option<&Value>,
        ) -> (usize, usize) {
            let bytes_len = bytes_len as i64;

            // The left-most character of X is number 1.
            // If Y is negative then the first character of the substring is found by counting from the right rather than the left.
            let first_position = if start < 0 {
                bytes_len.saturating_sub((start).abs())
            } else {
                start - 1
            };
            // If Z is negative then the abs(Z) characters preceding the Y-th character are returned.
            let last_position = match length_value {
                Some(Value::Integer(length)) => first_position + *length,
                _ => bytes_len,
            };

            let (start, end) = if first_position <= last_position {
                (first_position, last_position)
            } else {
                (last_position, first_position)
            };

            (
                start.clamp(-0, bytes_len) as usize,
                end.clamp(0, bytes_len) as usize,
            )
        }

        let start_value = start_value.exec_cast("INT");
        let length_value = length_value.map(|value| value.exec_cast("INT"));

        match (value, start_value) {
            (Value::Blob(b), Value::Integer(start)) => {
                let (start, end) = calculate_postions(start, b.len(), length_value.as_ref());
                Value::from_blob(b[start..end].to_vec())
            }
            (value, Value::Integer(start)) => {
                if let Some(text) = value.cast_text() {
                    let (mut start, mut end) =
                        calculate_postions(start, text.len(), length_value.as_ref());

                    // https://github.com/sqlite/sqlite/blob/a248d84f/src/func.c#L417
                    let s = text.as_str();
                    let mut start_byte_idx = 0;
                    end -= start;
                    while start > 0 {
                        start_byte_idx = ceil_char_boundary(s, start_byte_idx + 1);
                        start -= 1;
                    }
                    let mut end_byte_idx = start_byte_idx;
                    while end > 0 {
                        end_byte_idx = ceil_char_boundary(s, end_byte_idx + 1);
                        end -= 1;
                    }
                    Value::build_text(s[start_byte_idx..end_byte_idx].to_string())
                } else {
                    Value::Null
                }
            }
            _ => Value::Null,
        }
    }

    pub fn exec_instr(&self, pattern: &Value) -> Value {
        if self == &Value::Null || pattern == &Value::Null {
            return Value::Null;
        }

        if let (Value::Blob(reg), Value::Blob(pattern)) = (self, pattern) {
            // SQLite returns 1 for empty pattern (found at position 1)
            if pattern.is_empty() {
                return Value::Integer(1);
            }
            let result = reg
                .windows(pattern.len())
                .position(|window| window == *pattern)
                .map_or(0, |i| i + 1);
            return Value::Integer(result as i64);
        }

        let reg_str;
        let reg = match self {
            Value::Text(s) => s.as_str(),
            _ => {
                reg_str = self.to_string();
                reg_str.as_str()
            }
        };

        let pattern_str;
        let pattern = match pattern {
            Value::Text(s) => s.as_str(),
            _ => {
                pattern_str = pattern.to_string();
                pattern_str.as_str()
            }
        };

        match reg.find(pattern) {
            Some(position) => Value::Integer(position as i64 + 1),
            None => Value::Integer(0),
        }
    }

    pub fn exec_typeof(&self) -> Value {
        match self {
            Value::Null => Value::build_text("null"),
            Value::Integer(_) => Value::build_text("integer"),
            Value::Float(_) => Value::build_text("real"),
            Value::Text(_) => Value::build_text("text"),
            Value::Blob(_) => Value::build_text("blob"),
        }
    }

    pub fn exec_hex(&self) -> Value {
        match self {
            Value::Text(_) | Value::Integer(_) | Value::Float(_) => {
                let text = self.to_string();
                Value::build_text(hex::encode_upper(text))
            }
            Value::Blob(blob_bytes) => Value::build_text(hex::encode_upper(blob_bytes)),
            Value::Null => Value::build_text(""),
        }
    }

    pub fn exec_unhex(&self, ignored_chars: Option<&Value>) -> Value {
        match self {
            Value::Null => Value::Null,
            _ => match ignored_chars {
                None => match self
                    .cast_text()
                    .map(|s| hex::decode(&s[0..s.find('\0').unwrap_or(s.len())]))
                {
                    Some(Ok(bytes)) => Value::Blob(bytes),
                    _ => Value::Null,
                },
                Some(ignore) => match ignore {
                    Value::Text(_) => {
                        let pat = ignore.to_string();
                        let trimmed = self
                            .to_string()
                            .trim_start_matches(|x| pat.contains(x))
                            .trim_end_matches(|x| pat.contains(x))
                            .to_string();
                        match hex::decode(trimmed) {
                            Ok(bytes) => Value::Blob(bytes),
                            _ => Value::Null,
                        }
                    }
                    _ => Value::Null,
                },
            },
        }
    }

    pub fn exec_unicode(&self) -> Value {
        match self {
            Value::Text(_) | Value::Integer(_) | Value::Float(_) | Value::Blob(_) => {
                let text = self.to_string();
                if let Some(first_char) = text.chars().next() {
                    Value::Integer(first_char as u32 as i64)
                } else {
                    Value::Null
                }
            }
            _ => Value::Null,
        }
    }

    pub fn exec_round(&self, precision: Option<&Value>) -> Value {
        let Some(f) = Numeric::from(self).try_into_f64() else {
            return Value::Null;
        };

        let precision = match precision.map(|v| Numeric::from(v).try_into_f64()) {
            None => 0.0,
            Some(Some(v)) => v,
            Some(None) => return Value::Null,
        };

        if !(-4503599627370496.0..=4503599627370496.0).contains(&f) {
            return Value::Float(f);
        }

        let precision = if precision < 1.0 { 0.0 } else { precision };
        let precision = precision.clamp(0.0, 30.0) as usize;

        if precision == 0 {
            return Value::Float(((f + if f < 0.0 { -0.5 } else { 0.5 }) as i64) as f64);
        }

        let f: f64 = crate::numeric::str_to_f64(format!("{f:.precision$}"))
            .expect("formatted float should always parse successfully")
            .into();

        Value::Float(f)
    }

    fn _exec_trim(&self, pattern: Option<&Value>, trim_type: TrimType) -> Value {
        match (self, pattern) {
            (Value::Text(_) | Value::Integer(_) | Value::Float(_), Some(pattern)) => {
                let pattern_chars: Vec<char> = pattern.to_string().chars().collect();
                let text = self.to_string();
                Value::build_text(trim_type.trim(&text, &pattern_chars).to_string())
            }
            (Value::Text(t), None) => {
                Value::build_text(trim_type.trim(t.as_str(), &[' ']).to_string())
            }
            (reg, _) => reg.to_owned(),
        }
    }

    // Implements TRIM pattern matching.
    pub fn exec_trim(&self, pattern: Option<&Value>) -> Value {
        self._exec_trim(pattern, TrimType::All)
    }
    // Implements RTRIM pattern matching.
    pub fn exec_rtrim(&self, pattern: Option<&Value>) -> Value {
        self._exec_trim(pattern, TrimType::Right)
    }

    // Implements LTRIM pattern matching.
    pub fn exec_ltrim(&self, pattern: Option<&Value>) -> Value {
        self._exec_trim(pattern, TrimType::Left)
    }

    pub fn exec_zeroblob(&self) -> Value {
        let length: i64 = match self {
            Value::Integer(i) => *i,
            Value::Float(f) => *f as i64,
            Value::Text(s) => s.as_str().parse().unwrap_or(0),
            _ => 0,
        };
        Value::Blob(vec![0; length.max(0) as usize])
    }

    // exec_if returns whether you should jump
    pub fn exec_if(&self, jump_if_null: bool, not: bool) -> bool {
        Numeric::from(self)
            .try_into_bool()
            .map(|jump| if not { !jump } else { jump })
            .unwrap_or(jump_if_null)
    }

    pub fn exec_cast(&self, datatype: &str) -> Value {
        if matches!(self, Value::Null) {
            return Value::Null;
        }
        match Affinity::affinity(datatype) {
            // NONE	Casting a value to a type-name with no affinity causes the value to be converted into a BLOB. Casting to a BLOB consists of first casting the value to TEXT in the encoding of the database connection, then interpreting the resulting byte sequence as a BLOB instead of as TEXT.
            // Historically called NONE, but it's the same as BLOB
            Affinity::Blob => {
                // Convert to TEXT first, then interpret as BLOB
                // TODO: handle encoding
                let text = self.to_string();
                Value::Blob(text.into_bytes())
            }
            // TEXT To cast a BLOB value to TEXT, the sequence of bytes that make up the BLOB is interpreted as text encoded using the database encoding.
            // Casting an INTEGER or REAL value into TEXT renders the value as if via sqlite3_snprintf() except that the resulting TEXT uses the encoding of the database connection.
            Affinity::Text => {
                // Convert everything to text representation
                // TODO: handle encoding and whatever sqlite3_snprintf does
                Value::build_text(self.to_string())
            }
            Affinity::Real => match self {
                Value::Blob(b) => {
                    let text = String::from_utf8_lossy(b);
                    Value::Float(
                        crate::numeric::str_to_f64(&text)
                            .map(f64::from)
                            .unwrap_or(0.0),
                    )
                }
                Value::Text(t) => {
                    Value::Float(crate::numeric::str_to_f64(t).map(f64::from).unwrap_or(0.0))
                }
                Value::Integer(i) => Value::Float(*i as f64),
                Value::Float(f) => Value::Float(*f),
                _ => Value::Float(0.0),
            },
            Affinity::Integer => match self {
                Value::Blob(b) => {
                    // Convert BLOB to TEXT first
                    let text = String::from_utf8_lossy(b);
                    Value::Integer(crate::numeric::str_to_i64(&text).unwrap_or(0))
                }
                Value::Text(t) => Value::Integer(crate::numeric::str_to_i64(t).unwrap_or(0)),
                Value::Integer(i) => Value::Integer(*i),
                // A cast of a REAL value into an INTEGER results in the integer between the REAL value and zero
                // that is closest to the REAL value. If a REAL is greater than the greatest possible signed integer (+9223372036854775807)
                // then the result is the greatest possible signed integer and if the REAL is less than the least possible signed integer (-9223372036854775808)
                // then the result is the least possible signed integer.
                Value::Float(f) => {
                    let i = f.trunc() as i128;
                    if i > i64::MAX as i128 {
                        Value::Integer(i64::MAX)
                    } else if i < i64::MIN as i128 {
                        Value::Integer(i64::MIN)
                    } else {
                        Value::Integer(i as i64)
                    }
                }
                _ => Value::Integer(0),
            },
            Affinity::Numeric => match self {
                Value::Null => Value::Null,
                Value::Integer(v) => Value::Integer(*v),
                Value::Float(v) => Self::Float(*v),
                _ => {
                    let s = match self {
                        Value::Text(text) => text.to_string(),
                        Value::Blob(blob) => String::from_utf8_lossy(blob.as_slice()).to_string(),
                        _ => unreachable!(),
                    };

                    match crate::numeric::str_to_f64(&s) {
                        Some(parsed) => {
                            let Some(int) = crate::numeric::str_to_i64(&s) else {
                                return Value::Integer(0);
                            };

                            if f64::from(parsed) == int as f64 {
                                return Value::Integer(int);
                            }

                            Value::Float(parsed.into())
                        }
                        None => Value::Integer(0),
                    }
                }
            },
        }
    }

    pub fn exec_replace(source: &Value, pattern: &Value, replacement: &Value) -> Value {
        // The replace(X,Y,Z) function returns a string formed by substituting string Z for every occurrence of
        // string Y in string X. The BINARY collating sequence is used for comparisons. If Y is an empty string
        // then return X unchanged. If Z is not initially a string, it is cast to a UTF-8 string prior to processing.

        // If any of the arguments is NULL, the result is NULL.
        if matches!(source, Value::Null)
            || matches!(pattern, Value::Null)
            || matches!(replacement, Value::Null)
        {
            return Value::Null;
        }

        let source = source.exec_cast("TEXT");
        let pattern = pattern.exec_cast("TEXT");
        let replacement = replacement.exec_cast("TEXT");

        // If any of the casts failed, panic as text casting is not expected to fail.
        match (&source, &pattern, &replacement) {
            (Value::Text(source), Value::Text(pattern), Value::Text(replacement)) => {
                if pattern.as_str().is_empty() {
                    return Value::Text(source.clone());
                }

                let result = source
                    .as_str()
                    .replace(pattern.as_str(), replacement.as_str());
                Value::build_text(result)
            }
            _ => unreachable!("text cast should never fail"),
        }
    }

    pub fn exec_math_unary(&self, function: &MathFunc) -> Value {
        let v = Numeric::from_value_strict(self);

        // In case of some functions and integer input, return the input as is
        if let Numeric::Integer(i) = v {
            if matches! { function, MathFunc::Ceil | MathFunc::Ceiling | MathFunc::Floor | MathFunc::Trunc }
            {
                return Value::Integer(i);
            }
        }

        let Some(f) = v.try_into_f64() else {
            return Value::Null;
        };

        if matches! { function, MathFunc::Ln | MathFunc::Log10 | MathFunc::Log2 } && f <= 0.0 {
            return Value::Null;
        }

        #[allow(unused_unsafe)]
        let result = match function {
            MathFunc::Acos => unsafe { cmath::acos(f) },
            MathFunc::Acosh => unsafe { cmath::acosh(f) },
            MathFunc::Asin => unsafe { cmath::asin(f) },
            MathFunc::Asinh => unsafe { cmath::asinh(f) },
            MathFunc::Atan => unsafe { cmath::atan(f) },
            MathFunc::Atanh => unsafe { cmath::atanh(f) },
            MathFunc::Ceil | MathFunc::Ceiling => libm::ceil(f),
            MathFunc::Cos => unsafe { cmath::cos(f) },
            MathFunc::Cosh => unsafe { cmath::cosh(f) },
            MathFunc::Degrees => f.to_degrees(),
            MathFunc::Exp => unsafe { cmath::exp(f) },
            MathFunc::Floor => libm::floor(f),
            MathFunc::Ln => unsafe { cmath::log(f) },
            MathFunc::Log10 => unsafe { cmath::log10(f) },
            MathFunc::Log2 => unsafe { cmath::log2(f) },
            MathFunc::Radians => f.to_radians(),
            MathFunc::Sin => unsafe { cmath::sin(f) },
            MathFunc::Sinh => unsafe { cmath::sinh(f) },
            MathFunc::Sqrt => libm::sqrt(f),
            MathFunc::Tan => unsafe { cmath::tan(f) },
            MathFunc::Tanh => unsafe { cmath::tanh(f) },
            MathFunc::Trunc => libm::trunc(f),
            _ => unreachable!("Unexpected mathematical unary function {:?}", function),
        };

        if result.is_nan() {
            Value::Null
        } else {
            Value::Float(result)
        }
    }

    pub fn exec_math_binary(&self, rhs: &Value, function: &MathFunc) -> Value {
        let Some(lhs) = Numeric::from_value_strict(self).try_into_f64() else {
            return Value::Null;
        };

        let Some(rhs) = Numeric::from_value_strict(rhs).try_into_f64() else {
            return Value::Null;
        };

        #[allow(unused_unsafe)]
        let result = match function {
            MathFunc::Atan2 => unsafe { cmath::atan2(lhs, rhs) },
            MathFunc::Mod => libm::fmod(lhs, rhs),
            MathFunc::Pow | MathFunc::Power => unsafe { cmath::pow(lhs, rhs) },
            _ => unreachable!("Unexpected mathematical binary function {:?}", function),
        };

        if result.is_nan() {
            Value::Null
        } else {
            Value::Float(result)
        }
    }

    pub fn exec_math_log(&self, base: Option<&Value>) -> Value {
        let Some(f) = Numeric::from_value_strict(self).try_into_f64() else {
            return Value::Null;
        };

        let base = match base.map(|value| Numeric::from_value_strict(value).try_into_f64()) {
            Some(Some(f)) => f,
            Some(None) => return Value::Null,
            None => 10.0,
        };

        if f <= 0.0 || base <= 0.0 || base == 1.0 {
            return Value::Null;
        }

        if base == 2.0 {
            return Value::Float(libm::log2(f));
        } else if base == 10.0 {
            return Value::Float(libm::log10(f));
        };

        let log_x = libm::log(f);
        let log_base = libm::log(base);

        if log_base <= 0.0 {
            return Value::Null;
        }

        let result = log_x / log_base;
        Value::Float(result)
    }

    pub fn exec_add(&self, rhs: &Value) -> Value {
        (Numeric::from(self) + Numeric::from(rhs)).into()
    }

    pub fn exec_subtract(&self, rhs: &Value) -> Value {
        (Numeric::from(self) - Numeric::from(rhs)).into()
    }

    pub fn exec_multiply(&self, rhs: &Value) -> Value {
        (Numeric::from(self) * Numeric::from(rhs)).into()
    }

    pub fn exec_divide(&self, rhs: &Value) -> Value {
        (Numeric::from(self) / Numeric::from(rhs)).into()
    }

    pub fn exec_bit_and(&self, rhs: &Value) -> Value {
        (NullableInteger::from(self) & NullableInteger::from(rhs)).into()
    }

    pub fn exec_bit_or(&self, rhs: &Value) -> Value {
        (NullableInteger::from(self) | NullableInteger::from(rhs)).into()
    }

    pub fn exec_remainder(&self, rhs: &Value) -> Value {
        let convert_to_float = matches!(Numeric::from(self), Numeric::Float(_))
            || matches!(Numeric::from(rhs), Numeric::Float(_));

        match NullableInteger::from(self) % NullableInteger::from(rhs) {
            NullableInteger::Null => Value::Null,
            NullableInteger::Integer(v) => {
                if convert_to_float {
                    Value::Float(v as f64)
                } else {
                    Value::Integer(v)
                }
            }
        }
    }

    pub fn exec_bit_not(&self) -> Value {
        (!NullableInteger::from(self)).into()
    }

    pub fn exec_shift_left(&self, rhs: &Value) -> Value {
        (NullableInteger::from(self) << NullableInteger::from(rhs)).into()
    }

    pub fn exec_shift_right(&self, rhs: &Value) -> Value {
        (NullableInteger::from(self) >> NullableInteger::from(rhs)).into()
    }

    pub fn exec_boolean_not(&self) -> Value {
        match Numeric::from(self).try_into_bool() {
            None => Value::Null,
            Some(v) => Value::Integer(!v as i64),
        }
    }

    pub fn exec_concat(&self, rhs: &Value) -> Value {
        if let (Value::Blob(lhs), Value::Blob(rhs)) = (self, rhs) {
            return Value::build_text(
                String::from_utf8_lossy(&[lhs.as_slice(), rhs.as_slice()].concat()).into_owned(),
            );
        }

        let Some(lhs) = self.cast_text() else {
            return Value::Null;
        };

        let Some(rhs) = rhs.cast_text() else {
            return Value::Null;
        };

        Value::build_text(lhs + &rhs)
    }

    pub fn exec_and(&self, rhs: &Value) -> Value {
        match (
            Numeric::from(self).try_into_bool(),
            Numeric::from(rhs).try_into_bool(),
        ) {
            (Some(false), _) | (_, Some(false)) => Value::Integer(0),
            (None, _) | (_, None) => Value::Null,
            _ => Value::Integer(1),
        }
    }

    pub fn exec_or(&self, rhs: &Value) -> Value {
        match (
            Numeric::from(self).try_into_bool(),
            Numeric::from(rhs).try_into_bool(),
        ) {
            (Some(true), _) | (_, Some(true)) => Value::Integer(1),
            (None, _) | (_, None) => Value::Null,
            _ => Value::Integer(0),
        }
    }

    // Implements LIKE pattern matching. Caches the constructed regex if a cache is provided
    pub fn exec_like(
        regex_cache: Option<&mut HashMap<String, Regex>>,
        pattern: &str,
        text: &str,
    ) -> bool {
        if let Some(cache) = regex_cache {
            match cache.get(pattern) {
                Some(re) => re.is_match(text),
                None => {
                    let re = construct_like_regex(pattern);
                    let res = re.is_match(text);
                    cache.insert(pattern.to_string(), re);
                    res
                }
            }
        } else {
            let re = construct_like_regex(pattern);
            re.is_match(text)
        }
    }

    pub fn exec_min<'a, T: Iterator<Item = &'a Value>>(regs: T) -> Value {
        regs.min().map(|v| v.to_owned()).unwrap_or(Value::Null)
    }

    pub fn exec_max<'a, T: Iterator<Item = &'a Value>>(regs: T) -> Value {
        regs.max().map(|v| v.to_owned()).unwrap_or(Value::Null)
    }

    pub fn exec_concat_strings<'a, T: Iterator<Item = &'a Self>>(registers: T) -> Self {
        let mut result = String::new();
        for val in registers {
            match val {
                Value::Null => continue,
                Value::Blob(_) => todo!("TODO concat blob"),
                v => result.push_str(&format!("{v}")),
            }
        }
        Value::build_text(result)
    }

    pub fn exec_concat_ws<'a, T: ExactSizeIterator<Item = &'a Self>>(mut registers: T) -> Self {
        if registers.len() == 0 {
            return Value::Null;
        }

        let separator = match registers
            .next()
            .expect("registers should have at least one element after length check")
        {
            Value::Null | Value::Blob(_) => return Value::Null,
            v => format!("{v}"),
        };

        let parts = registers.filter_map(|val| match val {
            Value::Text(_) | Value::Integer(_) | Value::Float(_) => Some(format!("{val}")),
            _ => None,
        });

        let result = parts.collect::<Vec<_>>().join(&separator);
        Value::build_text(result)
    }

    pub fn exec_char<'a, T: Iterator<Item = &'a Self>>(values: T) -> Self {
        let result: String = values
            .filter_map(|x| {
                if let Value::Integer(i) = x {
                    Some(*i as u8 as char)
                } else {
                    None
                }
            })
            .collect();
        Value::build_text(result)
    }
}

pub fn construct_like_regex(pattern: &str) -> Regex {
    let mut regex_pattern = String::with_capacity(pattern.len() * 2);

    regex_pattern.push('^');

    for c in pattern.chars() {
        match c {
            '\\' => regex_pattern.push_str("\\\\"),
            '%' => regex_pattern.push_str(".*"),
            '_' => regex_pattern.push('.'),
            ch => {
                if ch.is_ascii_alphabetic() {
                    regex_pattern.push('[');
                    regex_pattern.push(ch.to_ascii_lowercase());
                    regex_pattern.push(ch.to_ascii_uppercase());
                    regex_pattern.push(']');
                } else {
                    if regex_syntax::is_meta_character(c) {
                        regex_pattern.push('\\');
                    }
                    regex_pattern.push(ch);
                }
            }
        }
    }

    regex_pattern.push('$');

    RegexBuilder::new(&regex_pattern)
        .dot_matches_new_line(true)
        .build()
        .expect("constructed LIKE regex pattern should be valid")
}

#[cfg(test)]
mod tests {
    use crate::types::Value;
    use crate::vdbe::Register;

    use rand::{Rng, RngCore};
    use std::collections::HashMap;

    #[test]
    fn test_exec_add() {
        let inputs = vec![
            (Value::Integer(3), Value::Integer(1)),
            (Value::Float(3.0), Value::Float(1.0)),
            (Value::Float(3.0), Value::Integer(1)),
            (Value::Integer(3), Value::Float(1.0)),
            (Value::Null, Value::Null),
            (Value::Null, Value::Integer(1)),
            (Value::Null, Value::Float(1.0)),
            (Value::Null, Value::Text("2".into())),
            (Value::Integer(1), Value::Null),
            (Value::Float(1.0), Value::Null),
            (Value::Text("1".into()), Value::Null),
            (Value::Text("1".into()), Value::Text("3".into())),
            (Value::Text("1.0".into()), Value::Text("3.0".into())),
            (Value::Text("1.0".into()), Value::Float(3.0)),
            (Value::Text("1.0".into()), Value::Integer(3)),
            (Value::Float(1.0), Value::Text("3.0".into())),
            (Value::Integer(1), Value::Text("3".into())),
        ];

        let outputs = [
            Value::Integer(4),
            Value::Float(4.0),
            Value::Float(4.0),
            Value::Float(4.0),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Integer(4),
            Value::Float(4.0),
            Value::Float(4.0),
            Value::Float(4.0),
            Value::Float(4.0),
            Value::Float(4.0),
        ];

        assert_eq!(
            inputs.len(),
            outputs.len(),
            "Inputs and Outputs should have same size"
        );
        for (i, (lhs, rhs)) in inputs.iter().enumerate() {
            assert_eq!(
                lhs.exec_add(rhs),
                outputs[i],
                "Wrong ADD for lhs: {lhs}, rhs: {rhs}"
            );
        }
    }

    #[test]
    fn test_exec_subtract() {
        let inputs = vec![
            (Value::Integer(3), Value::Integer(1)),
            (Value::Float(3.0), Value::Float(1.0)),
            (Value::Float(3.0), Value::Integer(1)),
            (Value::Integer(3), Value::Float(1.0)),
            (Value::Null, Value::Null),
            (Value::Null, Value::Integer(1)),
            (Value::Null, Value::Float(1.0)),
            (Value::Null, Value::Text("1".into())),
            (Value::Integer(1), Value::Null),
            (Value::Float(1.0), Value::Null),
            (Value::Text("4".into()), Value::Null),
            (Value::Text("1".into()), Value::Text("3".into())),
            (Value::Text("1.0".into()), Value::Text("3.0".into())),
            (Value::Text("1.0".into()), Value::Float(3.0)),
            (Value::Text("1.0".into()), Value::Integer(3)),
            (Value::Float(1.0), Value::Text("3.0".into())),
            (Value::Integer(1), Value::Text("3".into())),
        ];

        let outputs = [
            Value::Integer(2),
            Value::Float(2.0),
            Value::Float(2.0),
            Value::Float(2.0),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Integer(-2),
            Value::Float(-2.0),
            Value::Float(-2.0),
            Value::Float(-2.0),
            Value::Float(-2.0),
            Value::Float(-2.0),
        ];

        assert_eq!(
            inputs.len(),
            outputs.len(),
            "Inputs and Outputs should have same size"
        );
        for (i, (lhs, rhs)) in inputs.iter().enumerate() {
            assert_eq!(
                lhs.exec_subtract(rhs),
                outputs[i],
                "Wrong subtract for lhs: {lhs}, rhs: {rhs}"
            );
        }
    }

    #[test]
    fn test_exec_multiply() {
        let inputs = vec![
            (Value::Integer(3), Value::Integer(2)),
            (Value::Float(3.0), Value::Float(2.0)),
            (Value::Float(3.0), Value::Integer(2)),
            (Value::Integer(3), Value::Float(2.0)),
            (Value::Null, Value::Null),
            (Value::Null, Value::Integer(1)),
            (Value::Null, Value::Float(1.0)),
            (Value::Null, Value::Text("1".into())),
            (Value::Integer(1), Value::Null),
            (Value::Float(1.0), Value::Null),
            (Value::Text("4".into()), Value::Null),
            (Value::Text("2".into()), Value::Text("3".into())),
            (Value::Text("2.0".into()), Value::Text("3.0".into())),
            (Value::Text("2.0".into()), Value::Float(3.0)),
            (Value::Text("2.0".into()), Value::Integer(3)),
            (Value::Float(2.0), Value::Text("3.0".into())),
            (Value::Integer(2), Value::Text("3.0".into())),
        ];

        let outputs = [
            Value::Integer(6),
            Value::Float(6.0),
            Value::Float(6.0),
            Value::Float(6.0),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Integer(6),
            Value::Float(6.0),
            Value::Float(6.0),
            Value::Float(6.0),
            Value::Float(6.0),
            Value::Float(6.0),
        ];

        assert_eq!(
            inputs.len(),
            outputs.len(),
            "Inputs and Outputs should have same size"
        );
        for (i, (lhs, rhs)) in inputs.iter().enumerate() {
            assert_eq!(
                lhs.exec_multiply(rhs),
                outputs[i],
                "Wrong multiply for lhs: {lhs}, rhs: {rhs}"
            );
        }
    }

    #[test]
    fn test_exec_divide() {
        let inputs = vec![
            (Value::Integer(1), Value::Integer(0)),
            (Value::Float(1.0), Value::Float(0.0)),
            (Value::Integer(i64::MIN), Value::Integer(-1)),
            (Value::Float(6.0), Value::Float(2.0)),
            (Value::Float(6.0), Value::Integer(2)),
            (Value::Integer(6), Value::Integer(2)),
            (Value::Null, Value::Integer(2)),
            (Value::Integer(2), Value::Null),
            (Value::Null, Value::Null),
            (Value::Text("6".into()), Value::Text("2".into())),
            (Value::Text("6".into()), Value::Integer(2)),
        ];

        let outputs = [
            Value::Null,
            Value::Null,
            Value::Float(9.223372036854776e18),
            Value::Float(3.0),
            Value::Float(3.0),
            Value::Float(3.0),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Float(3.0),
            Value::Float(3.0),
        ];

        assert_eq!(
            inputs.len(),
            outputs.len(),
            "Inputs and Outputs should have same size"
        );
        for (i, (lhs, rhs)) in inputs.iter().enumerate() {
            assert_eq!(
                lhs.exec_divide(rhs),
                outputs[i],
                "Wrong divide for lhs: {lhs}, rhs: {rhs}"
            );
        }
    }

    #[test]
    fn test_exec_remainder() {
        let inputs = vec![
            (Value::Null, Value::Null),
            (Value::Null, Value::Float(1.0)),
            (Value::Null, Value::Integer(1)),
            (Value::Null, Value::Text("1".into())),
            (Value::Float(1.0), Value::Null),
            (Value::Integer(1), Value::Null),
            (Value::Integer(12), Value::Integer(0)),
            (Value::Float(12.0), Value::Float(0.0)),
            (Value::Float(12.0), Value::Integer(0)),
            (Value::Integer(12), Value::Float(0.0)),
            (Value::Integer(i64::MIN), Value::Integer(-1)),
            (Value::Integer(12), Value::Integer(3)),
            (Value::Float(12.0), Value::Float(3.0)),
            (Value::Float(12.0), Value::Integer(3)),
            (Value::Integer(12), Value::Float(3.0)),
            (Value::Integer(12), Value::Integer(-3)),
            (Value::Float(12.0), Value::Float(-3.0)),
            (Value::Float(12.0), Value::Integer(-3)),
            (Value::Integer(12), Value::Float(-3.0)),
            (Value::Text("12.0".into()), Value::Text("3.0".into())),
            (Value::Text("12.0".into()), Value::Float(3.0)),
            (Value::Float(12.0), Value::Text("3.0".into())),
        ];
        let outputs = vec![
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Float(0.0),
            Value::Integer(0),
            Value::Float(0.0),
            Value::Float(0.0),
            Value::Float(0.0),
            Value::Integer(0),
            Value::Float(0.0),
            Value::Float(0.0),
            Value::Float(0.0),
            Value::Float(0.0),
            Value::Float(0.0),
            Value::Float(0.0),
        ];

        assert_eq!(
            inputs.len(),
            outputs.len(),
            "Inputs and Outputs should have same size"
        );

        for (i, (lhs, rhs)) in inputs.iter().enumerate() {
            assert_eq!(
                lhs.exec_remainder(rhs),
                outputs[i],
                "Wrong remainder for lhs: {lhs}, rhs: {rhs}"
            );
        }
    }

    #[test]
    fn test_exec_and() {
        let inputs = vec![
            (Value::Integer(0), Value::Null),
            (Value::Null, Value::Integer(1)),
            (Value::Null, Value::Null),
            (Value::Float(0.0), Value::Null),
            (Value::Integer(1), Value::Float(2.2)),
            (Value::Integer(0), Value::Text("string".into())),
            (Value::Integer(0), Value::Text("1".into())),
            (Value::Integer(1), Value::Text("1".into())),
        ];
        let outputs = [
            Value::Integer(0),
            Value::Null,
            Value::Null,
            Value::Integer(0),
            Value::Integer(1),
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(1),
        ];

        assert_eq!(
            inputs.len(),
            outputs.len(),
            "Inputs and Outputs should have same size"
        );
        for (i, (lhs, rhs)) in inputs.iter().enumerate() {
            assert_eq!(
                lhs.exec_and(rhs),
                outputs[i],
                "Wrong AND for lhs: {lhs}, rhs: {rhs}"
            );
        }
    }

    #[test]
    fn test_exec_or() {
        let inputs = vec![
            (Value::Integer(0), Value::Null),
            (Value::Null, Value::Integer(1)),
            (Value::Null, Value::Null),
            (Value::Float(0.0), Value::Null),
            (Value::Integer(1), Value::Float(2.2)),
            (Value::Float(0.0), Value::Integer(0)),
            (Value::Integer(0), Value::Text("string".into())),
            (Value::Integer(0), Value::Text("1".into())),
            (Value::Integer(0), Value::Text("".into())),
        ];
        let outputs = [
            Value::Null,
            Value::Integer(1),
            Value::Null,
            Value::Null,
            Value::Integer(1),
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(1),
            Value::Integer(0),
        ];

        assert_eq!(
            inputs.len(),
            outputs.len(),
            "Inputs and Outputs should have same size"
        );
        for (i, (lhs, rhs)) in inputs.iter().enumerate() {
            assert_eq!(
                lhs.exec_or(rhs),
                outputs[i],
                "Wrong OR for lhs: {lhs}, rhs: {rhs}"
            );
        }
    }

    #[test]
    fn test_length() {
        let input_str = Value::build_text("bob");
        let expected_len = Value::Integer(3);
        assert_eq!(input_str.exec_length(), expected_len);

        let input_integer = Value::Integer(123);
        let expected_len = Value::Integer(3);
        assert_eq!(input_integer.exec_length(), expected_len);

        let input_float = Value::Float(123.456);
        let expected_len = Value::Integer(7);
        assert_eq!(input_float.exec_length(), expected_len);

        let expected_blob = Value::Blob("example".as_bytes().to_vec());
        let expected_len = Value::Integer(7);
        assert_eq!(expected_blob.exec_length(), expected_len);
    }

    #[test]
    fn test_quote() {
        let input = Value::build_text("abc\0edf");
        let expected = Value::build_text("'abc'");
        assert_eq!(input.exec_quote(), expected);

        let input = Value::Integer(123);
        let expected = Value::Integer(123);
        assert_eq!(input.exec_quote(), expected);

        let input = Value::build_text("hello''world");
        let expected = Value::build_text("'hello''''world'");
        assert_eq!(input.exec_quote(), expected);
    }

    #[test]
    fn test_typeof() {
        let input = Value::Null;
        let expected: Value = Value::build_text("null");
        assert_eq!(input.exec_typeof(), expected);

        let input = Value::Integer(123);
        let expected: Value = Value::build_text("integer");
        assert_eq!(input.exec_typeof(), expected);

        let input = Value::Float(123.456);
        let expected: Value = Value::build_text("real");
        assert_eq!(input.exec_typeof(), expected);

        let input = Value::build_text("hello");
        let expected: Value = Value::build_text("text");
        assert_eq!(input.exec_typeof(), expected);

        let input = Value::Blob("limbo".as_bytes().to_vec());
        let expected: Value = Value::build_text("blob");
        assert_eq!(input.exec_typeof(), expected);
    }

    #[test]
    fn test_unicode() {
        assert_eq!(Value::build_text("a").exec_unicode(), Value::Integer(97));
        assert_eq!(
            Value::build_text("😊").exec_unicode(),
            Value::Integer(128522)
        );
        assert_eq!(Value::build_text("").exec_unicode(), Value::Null);
        assert_eq!(Value::Integer(23).exec_unicode(), Value::Integer(50));
        assert_eq!(Value::Integer(0).exec_unicode(), Value::Integer(48));
        assert_eq!(Value::Float(0.0).exec_unicode(), Value::Integer(48));
        assert_eq!(Value::Float(23.45).exec_unicode(), Value::Integer(50));
        assert_eq!(Value::Null.exec_unicode(), Value::Null);
        assert_eq!(
            Value::Blob("example".as_bytes().to_vec()).exec_unicode(),
            Value::Integer(101)
        );
    }

    #[test]
    fn test_min_max() {
        let input_int_vec = [
            Register::Value(Value::Integer(-1)),
            Register::Value(Value::Integer(10)),
        ];
        assert_eq!(
            Value::exec_min(input_int_vec.iter().map(|v| v.get_value())),
            Value::Integer(-1)
        );
        assert_eq!(
            Value::exec_max(input_int_vec.iter().map(|v| v.get_value())),
            Value::Integer(10)
        );

        let str1 = Register::Value(Value::build_text("A"));
        let str2 = Register::Value(Value::build_text("z"));
        let input_str_vec = [str2, str1.clone()];
        assert_eq!(
            Value::exec_min(input_str_vec.iter().map(|v| v.get_value())),
            Value::build_text("A")
        );
        assert_eq!(
            Value::exec_max(input_str_vec.iter().map(|v| v.get_value())),
            Value::build_text("z")
        );

        let input_null_vec = [Register::Value(Value::Null), Register::Value(Value::Null)];
        assert_eq!(
            Value::exec_min(input_null_vec.iter().map(|v| v.get_value())),
            Value::Null
        );
        assert_eq!(
            Value::exec_max(input_null_vec.iter().map(|v| v.get_value())),
            Value::Null
        );

        let input_mixed_vec = [Register::Value(Value::Integer(10)), str1];
        assert_eq!(
            Value::exec_min(input_mixed_vec.iter().map(|v| v.get_value())),
            Value::Integer(10)
        );
        assert_eq!(
            Value::exec_max(input_mixed_vec.iter().map(|v| v.get_value())),
            Value::build_text("A")
        );
    }

    #[test]
    fn test_trim() {
        let input_str = Value::build_text("     Bob and Alice     ");
        let expected_str = Value::build_text("Bob and Alice");
        assert_eq!(input_str.exec_trim(None), expected_str);

        let input_str = Value::build_text("     Bob and Alice     ");
        let pattern_str = Value::build_text("Bob and");
        let expected_str = Value::build_text("Alice");
        assert_eq!(input_str.exec_trim(Some(&pattern_str)), expected_str);

        let input_str = Value::build_text("\ta");
        let expected_str = Value::build_text("\ta");
        assert_eq!(input_str.exec_trim(None), expected_str);

        let input_str = Value::build_text("\na");
        let expected_str = Value::build_text("\na");
        assert_eq!(input_str.exec_trim(None), expected_str);
    }

    #[test]
    fn test_ltrim() {
        let input_str = Value::build_text("     Bob and Alice     ");
        let expected_str = Value::build_text("Bob and Alice     ");
        assert_eq!(input_str.exec_ltrim(None), expected_str);

        let input_str = Value::build_text("     Bob and Alice     ");
        let pattern_str = Value::build_text("Bob and");
        let expected_str = Value::build_text("Alice     ");
        assert_eq!(input_str.exec_ltrim(Some(&pattern_str)), expected_str);
    }

    #[test]
    fn test_rtrim() {
        let input_str = Value::build_text("     Bob and Alice     ");
        let expected_str = Value::build_text("     Bob and Alice");
        assert_eq!(input_str.exec_rtrim(None), expected_str);

        let input_str = Value::build_text("     Bob and Alice     ");
        let pattern_str = Value::build_text("Bob and");
        let expected_str = Value::build_text("     Bob and Alice");
        assert_eq!(input_str.exec_rtrim(Some(&pattern_str)), expected_str);

        let input_str = Value::build_text("     Bob and Alice     ");
        let pattern_str = Value::build_text("and Alice");
        let expected_str = Value::build_text("     Bob");
        assert_eq!(input_str.exec_rtrim(Some(&pattern_str)), expected_str);
    }

    #[test]
    fn test_soundex() {
        let input_str = Value::build_text("Pfister");
        let expected_str = Value::build_text("P236");
        assert_eq!(input_str.exec_soundex(), expected_str);

        let input_str = Value::build_text("husobee");
        let expected_str = Value::build_text("H210");
        assert_eq!(input_str.exec_soundex(), expected_str);

        let input_str = Value::build_text("Tymczak");
        let expected_str = Value::build_text("T522");
        assert_eq!(input_str.exec_soundex(), expected_str);

        let input_str = Value::build_text("Ashcraft");
        let expected_str = Value::build_text("A261");
        assert_eq!(input_str.exec_soundex(), expected_str);

        let input_str = Value::build_text("Robert");
        let expected_str = Value::build_text("R163");
        assert_eq!(input_str.exec_soundex(), expected_str);

        let input_str = Value::build_text("Rupert");
        let expected_str = Value::build_text("R163");
        assert_eq!(input_str.exec_soundex(), expected_str);

        let input_str = Value::build_text("Rubin");
        let expected_str = Value::build_text("R150");
        assert_eq!(input_str.exec_soundex(), expected_str);

        let input_str = Value::build_text("Kant");
        let expected_str = Value::build_text("K530");
        assert_eq!(input_str.exec_soundex(), expected_str);

        let input_str = Value::build_text("Knuth");
        let expected_str = Value::build_text("K530");
        assert_eq!(input_str.exec_soundex(), expected_str);

        let input_str = Value::build_text("x");
        let expected_str = Value::build_text("X000");
        assert_eq!(input_str.exec_soundex(), expected_str);

        let input_str = Value::build_text("闪电五连鞭");
        let expected_str = Value::build_text("?000");
        assert_eq!(input_str.exec_soundex(), expected_str);
    }

    #[test]
    fn test_upper_case() {
        let input_str = Value::build_text("Limbo");
        let expected_str = Value::build_text("LIMBO");
        assert_eq!(input_str.exec_upper().unwrap(), expected_str);

        let input_int = Value::Integer(10);
        assert_eq!(input_int.exec_upper().unwrap(), Value::build_text("10"));
        assert_eq!(Value::Null.exec_upper(), None)
    }

    #[test]
    fn test_lower_case() {
        let input_str = Value::build_text("Limbo");
        let expected_str = Value::build_text("limbo");
        assert_eq!(input_str.exec_lower().unwrap(), expected_str);

        let input_int = Value::Integer(10);
        assert_eq!(input_int.exec_lower().unwrap(), Value::build_text("10"));
        assert_eq!(Value::Null.exec_lower(), None)
    }

    #[test]
    fn test_hex() {
        let input_str = Value::build_text("limbo");
        let expected_val = Value::build_text("6C696D626F");
        assert_eq!(input_str.exec_hex(), expected_val);

        let input_int = Value::Integer(100);
        let expected_val = Value::build_text("313030");
        assert_eq!(input_int.exec_hex(), expected_val);

        let input_float = Value::Float(12.34);
        let expected_val = Value::build_text("31322E3334");
        assert_eq!(input_float.exec_hex(), expected_val);

        let input_blob = Value::Blob(vec![0xff]);
        let expected_val = Value::build_text("FF");
        assert_eq!(input_blob.exec_hex(), expected_val);
    }

    #[test]
    fn test_unhex() {
        let input = Value::build_text("6f");
        let expected = Value::Blob(vec![0x6f]);
        assert_eq!(input.exec_unhex(None), expected);

        let input = Value::build_text("6f");
        let expected = Value::Blob(vec![0x6f]);
        assert_eq!(input.exec_unhex(None), expected);

        let input = Value::build_text("611");
        let expected = Value::Null;
        assert_eq!(input.exec_unhex(None), expected);

        let input = Value::build_text("");
        let expected = Value::Blob(vec![]);
        assert_eq!(input.exec_unhex(None), expected);

        let input = Value::build_text("61x");
        let expected = Value::Null;
        assert_eq!(input.exec_unhex(None), expected);

        let input = Value::Null;
        let expected = Value::Null;
        assert_eq!(input.exec_unhex(None), expected);
    }

    #[test]
    fn test_abs() {
        let int_positive_reg = Value::Integer(10);
        let int_negative_reg = Value::Integer(-10);
        assert_eq!(int_positive_reg.exec_abs().unwrap(), int_positive_reg);
        assert_eq!(int_negative_reg.exec_abs().unwrap(), int_positive_reg);

        let float_positive_reg = Value::Integer(10);
        let float_negative_reg = Value::Integer(-10);
        assert_eq!(float_positive_reg.exec_abs().unwrap(), float_positive_reg);
        assert_eq!(float_negative_reg.exec_abs().unwrap(), float_positive_reg);

        assert_eq!(
            Value::build_text("a").exec_abs().unwrap(),
            Value::Float(0.0)
        );
        assert_eq!(Value::Null.exec_abs().unwrap(), Value::Null);

        // ABS(i64::MIN) should return RuntimeError
        assert!(Value::Integer(i64::MIN).exec_abs().is_err());
    }

    #[test]
    fn test_char() {
        assert_eq!(
            Value::exec_char(
                [
                    Register::Value(Value::Integer(108)),
                    Register::Value(Value::Integer(105))
                ]
                .iter()
                .map(|reg| reg.get_value())
            ),
            Value::build_text("li")
        );
        assert_eq!(Value::exec_char(std::iter::empty()), Value::build_text(""));
        assert_eq!(
            Value::exec_char(
                [Register::Value(Value::Null)]
                    .iter()
                    .map(|reg| reg.get_value())
            ),
            Value::build_text("")
        );
        assert_eq!(
            Value::exec_char(
                [Register::Value(Value::build_text("a"))]
                    .iter()
                    .map(|reg| reg.get_value())
            ),
            Value::build_text("")
        );
    }

    #[test]
    fn test_like_with_escape_or_regexmeta_chars() {
        assert!(Value::exec_like(None, r#"\%A"#, r#"\A"#));
        assert!(Value::exec_like(None, "%a%a", "aaaa"));
    }

    #[test]
    fn test_like_no_cache() {
        assert!(Value::exec_like(None, "a%", "aaaa"));
        assert!(Value::exec_like(None, "%a%a", "aaaa"));
        assert!(!Value::exec_like(None, "%a.a", "aaaa"));
        assert!(!Value::exec_like(None, "a.a%", "aaaa"));
        assert!(!Value::exec_like(None, "%a.ab", "aaaa"));
    }

    #[test]
    fn test_like_with_cache() {
        let mut cache = HashMap::new();
        assert!(Value::exec_like(Some(&mut cache), "a%", "aaaa"));
        assert!(Value::exec_like(Some(&mut cache), "%a%a", "aaaa"));
        assert!(!Value::exec_like(Some(&mut cache), "%a.a", "aaaa"));
        assert!(!Value::exec_like(Some(&mut cache), "a.a%", "aaaa"));
        assert!(!Value::exec_like(Some(&mut cache), "%a.ab", "aaaa"));

        // again after values have been cached
        assert!(Value::exec_like(Some(&mut cache), "a%", "aaaa"));
        assert!(Value::exec_like(Some(&mut cache), "%a%a", "aaaa"));
        assert!(!Value::exec_like(Some(&mut cache), "%a.a", "aaaa"));
        assert!(!Value::exec_like(Some(&mut cache), "a.a%", "aaaa"));
        assert!(!Value::exec_like(Some(&mut cache), "%a.ab", "aaaa"));
    }

    #[test]
    fn test_random() {
        match Value::exec_random(|| rand::rng().random()) {
            Value::Integer(value) => {
                // Check that the value is within the range of i64
                assert!(
                    (i64::MIN..=i64::MAX).contains(&value),
                    "Random number out of range"
                );
            }
            _ => panic!("exec_random did not return an Integer variant"),
        }
    }

    #[test]
    fn test_exec_randomblob() {
        struct TestCase {
            input: Value,
            expected_len: usize,
        }

        let test_cases = vec![
            TestCase {
                input: Value::Integer(5),
                expected_len: 5,
            },
            TestCase {
                input: Value::Integer(0),
                expected_len: 1,
            },
            TestCase {
                input: Value::Integer(-1),
                expected_len: 1,
            },
            TestCase {
                input: Value::build_text(""),
                expected_len: 1,
            },
            TestCase {
                input: Value::build_text("5"),
                expected_len: 5,
            },
            TestCase {
                input: Value::build_text("0"),
                expected_len: 1,
            },
            TestCase {
                input: Value::build_text("-1"),
                expected_len: 1,
            },
            TestCase {
                input: Value::Float(2.9),
                expected_len: 2,
            },
            TestCase {
                input: Value::Float(-3.15),
                expected_len: 1,
            },
            TestCase {
                input: Value::Null,
                expected_len: 1,
            },
        ];

        for test_case in &test_cases {
            let result = test_case.input.exec_randomblob(|dest| {
                rand::rng().fill_bytes(dest);
            });
            match result {
                Value::Blob(blob) => {
                    assert_eq!(blob.len(), test_case.expected_len);
                }
                _ => panic!("exec_randomblob did not return a Blob variant"),
            }
        }
    }

    #[test]
    fn test_exec_round() {
        let input_val = Value::Float(123.456);
        let expected_val = Value::Float(123.0);
        assert_eq!(input_val.exec_round(None), expected_val);

        let input_val = Value::Float(123.456);
        let precision_val = Value::Integer(2);
        let expected_val = Value::Float(123.46);
        assert_eq!(input_val.exec_round(Some(&precision_val)), expected_val);

        let input_val = Value::Float(123.456);
        let precision_val = Value::build_text("1");
        let expected_val = Value::Float(123.5);
        assert_eq!(input_val.exec_round(Some(&precision_val)), expected_val);

        let input_val = Value::build_text("123.456");
        let precision_val = Value::Integer(2);
        let expected_val = Value::Float(123.46);
        assert_eq!(input_val.exec_round(Some(&precision_val)), expected_val);

        let input_val = Value::Integer(123);
        let precision_val = Value::Integer(1);
        let expected_val = Value::Float(123.0);
        assert_eq!(input_val.exec_round(Some(&precision_val)), expected_val);

        let input_val = Value::Float(100.123);
        let expected_val = Value::Float(100.0);
        assert_eq!(input_val.exec_round(None), expected_val);

        let input_val = Value::Float(100.123);
        let expected_val = Value::Null;
        assert_eq!(input_val.exec_round(Some(&Value::Null)), expected_val);
    }

    #[test]
    fn test_exec_if() {
        let reg = Value::Integer(0);
        assert!(!reg.exec_if(false, false));
        assert!(reg.exec_if(false, true));

        let reg = Value::Integer(1);
        assert!(reg.exec_if(false, false));
        assert!(!reg.exec_if(false, true));

        let reg = Value::Null;
        assert!(!reg.exec_if(false, false));
        assert!(!reg.exec_if(false, true));

        let reg = Value::Null;
        assert!(reg.exec_if(true, false));
        assert!(reg.exec_if(true, true));

        let reg = Value::Null;
        assert!(!reg.exec_if(false, false));
        assert!(!reg.exec_if(false, true));
    }

    #[test]
    fn test_nullif() {
        assert_eq!(
            Value::Integer(1).exec_nullif(&Value::Integer(1)),
            Value::Null
        );
        assert_eq!(
            Value::Float(1.1).exec_nullif(&Value::Float(1.1)),
            Value::Null
        );
        assert_eq!(
            Value::build_text("limbo").exec_nullif(&Value::build_text("limbo")),
            Value::Null
        );

        assert_eq!(
            Value::Integer(1).exec_nullif(&Value::Integer(2)),
            Value::Integer(1)
        );
        assert_eq!(
            Value::Float(1.1).exec_nullif(&Value::Float(1.2)),
            Value::Float(1.1)
        );
        assert_eq!(
            Value::build_text("limbo").exec_nullif(&Value::build_text("limb")),
            Value::build_text("limbo")
        );
    }

    #[test]
    fn test_substring() {
        let str_value = Value::build_text("limbo");
        let start_value = Value::Integer(1);
        let length_value = Value::Integer(3);
        let expected_val = Value::build_text("lim");
        assert_eq!(
            Value::exec_substring(&str_value, &start_value, Some(&length_value)),
            expected_val
        );

        let str_value = Value::build_text("limbo");
        let start_value = Value::Integer(1);
        let length_value = Value::Integer(10);
        let expected_val = Value::build_text("limbo");
        assert_eq!(
            Value::exec_substring(&str_value, &start_value, Some(&length_value)),
            expected_val
        );

        let str_value = Value::build_text("limbo");
        let start_value = Value::Integer(10);
        let length_value = Value::Integer(3);
        let expected_val = Value::build_text("");
        assert_eq!(
            Value::exec_substring(&str_value, &start_value, Some(&length_value)),
            expected_val
        );

        let str_value = Value::build_text("limbo");
        let start_value = Value::Integer(3);
        let length_value = Value::Null;
        let expected_val = Value::build_text("mbo");
        assert_eq!(
            Value::exec_substring(&str_value, &start_value, Some(&length_value)),
            expected_val
        );

        let str_value = Value::build_text("limbo");
        let start_value = Value::Integer(10);
        let length_value = Value::Null;
        let expected_val = Value::build_text("");
        assert_eq!(
            Value::exec_substring(&str_value, &start_value, Some(&length_value)),
            expected_val
        );
    }

    #[test]
    fn test_exec_instr() {
        let input = Value::build_text("limbo");
        let pattern = Value::build_text("im");
        let expected = Value::Integer(2);
        assert_eq!(input.exec_instr(&pattern), expected);

        let input = Value::build_text("limbo");
        let pattern = Value::build_text("limbo");
        let expected = Value::Integer(1);
        assert_eq!(input.exec_instr(&pattern), expected);

        let input = Value::build_text("limbo");
        let pattern = Value::build_text("o");
        let expected = Value::Integer(5);
        assert_eq!(input.exec_instr(&pattern), expected);

        let input = Value::build_text("liiiiimbo");
        let pattern = Value::build_text("ii");
        let expected = Value::Integer(2);
        assert_eq!(input.exec_instr(&pattern), expected);

        let input = Value::build_text("limbo");
        let pattern = Value::build_text("limboX");
        let expected = Value::Integer(0);
        assert_eq!(input.exec_instr(&pattern), expected);

        let input = Value::build_text("limbo");
        let pattern = Value::build_text("");
        let expected = Value::Integer(1);
        assert_eq!(input.exec_instr(&pattern), expected);

        let input = Value::build_text("");
        let pattern = Value::build_text("limbo");
        let expected = Value::Integer(0);
        assert_eq!(input.exec_instr(&pattern), expected);

        let input = Value::build_text("");
        let pattern = Value::build_text("");
        let expected = Value::Integer(1);
        assert_eq!(input.exec_instr(&pattern), expected);

        let input = Value::Null;
        let pattern = Value::Null;
        let expected = Value::Null;
        assert_eq!(input.exec_instr(&pattern), expected);

        let input = Value::build_text("limbo");
        let pattern = Value::Null;
        let expected = Value::Null;
        assert_eq!(input.exec_instr(&pattern), expected);

        let input = Value::Null;
        let pattern = Value::build_text("limbo");
        let expected = Value::Null;
        assert_eq!(input.exec_instr(&pattern), expected);

        let input = Value::Integer(123);
        let pattern = Value::Integer(2);
        let expected = Value::Integer(2);
        assert_eq!(input.exec_instr(&pattern), expected);

        let input = Value::Integer(123);
        let pattern = Value::Integer(5);
        let expected = Value::Integer(0);
        assert_eq!(input.exec_instr(&pattern), expected);

        let input = Value::Float(12.34);
        let pattern = Value::Float(2.3);
        let expected = Value::Integer(2);
        assert_eq!(input.exec_instr(&pattern), expected);

        let input = Value::Float(12.34);
        let pattern = Value::Float(5.6);
        let expected = Value::Integer(0);
        assert_eq!(input.exec_instr(&pattern), expected);

        let input = Value::Float(12.34);
        let pattern = Value::build_text(".");
        let expected = Value::Integer(3);
        assert_eq!(input.exec_instr(&pattern), expected);

        let input = Value::Blob(vec![1, 2, 3, 4, 5]);
        let pattern = Value::Blob(vec![3, 4]);
        let expected = Value::Integer(3);
        assert_eq!(input.exec_instr(&pattern), expected);

        let input = Value::Blob(vec![1, 2, 3, 4, 5]);
        let pattern = Value::Blob(vec![3, 2]);
        let expected = Value::Integer(0);
        assert_eq!(input.exec_instr(&pattern), expected);

        let input = Value::Blob(vec![0x61, 0x62, 0x63, 0x64, 0x65]);
        let pattern = Value::build_text("cd");
        let expected = Value::Integer(3);
        assert_eq!(input.exec_instr(&pattern), expected);

        let input = Value::build_text("abcde");
        let pattern = Value::Blob(vec![0x63, 0x64]);
        let expected = Value::Integer(3);
        assert_eq!(input.exec_instr(&pattern), expected);

        let input = Value::build_text("abcde");
        let pattern = Value::build_text("");
        let expected = Value::Integer(1);
        assert_eq!(input.exec_instr(&pattern), expected);
    }

    #[test]
    fn test_exec_sign() {
        let input = Value::Integer(42);
        let expected = Some(Value::Integer(1));
        assert_eq!(input.exec_sign(), expected);

        let input = Value::Integer(-42);
        let expected = Some(Value::Integer(-1));
        assert_eq!(input.exec_sign(), expected);

        let input = Value::Integer(0);
        let expected = Some(Value::Integer(0));
        assert_eq!(input.exec_sign(), expected);

        let input = Value::Float(0.0);
        let expected = Some(Value::Integer(0));
        assert_eq!(input.exec_sign(), expected);

        let input = Value::Float(0.1);
        let expected = Some(Value::Integer(1));
        assert_eq!(input.exec_sign(), expected);

        let input = Value::Float(42.0);
        let expected = Some(Value::Integer(1));
        assert_eq!(input.exec_sign(), expected);

        let input = Value::Float(-42.0);
        let expected = Some(Value::Integer(-1));
        assert_eq!(input.exec_sign(), expected);

        let input = Value::build_text("abc");
        let expected = None;
        assert_eq!(input.exec_sign(), expected);

        let input = Value::build_text("42");
        let expected = Some(Value::Integer(1));
        assert_eq!(input.exec_sign(), expected);

        let input = Value::build_text("-42");
        let expected = Some(Value::Integer(-1));
        assert_eq!(input.exec_sign(), expected);

        let input = Value::build_text("0");
        let expected = Some(Value::Integer(0));
        assert_eq!(input.exec_sign(), expected);

        let input = Value::Blob(b"abc".to_vec());
        let expected = None;
        assert_eq!(input.exec_sign(), expected);

        let input = Value::Blob(b"42".to_vec());
        let expected = None;
        assert_eq!(input.exec_sign(), expected);

        let input = Value::Blob(b"-42".to_vec());
        let expected = None;
        assert_eq!(input.exec_sign(), expected);

        let input = Value::Blob(b"0".to_vec());
        let expected = None;
        assert_eq!(input.exec_sign(), expected);

        let input = Value::Null;
        let expected = None;
        assert_eq!(input.exec_sign(), expected);
    }

    #[test]
    fn test_exec_zeroblob() {
        let input = Value::Integer(0);
        let expected = Value::Blob(vec![]);
        assert_eq!(input.exec_zeroblob(), expected);

        let input = Value::Null;
        let expected = Value::Blob(vec![]);
        assert_eq!(input.exec_zeroblob(), expected);

        let input = Value::Integer(4);
        let expected = Value::Blob(vec![0; 4]);
        assert_eq!(input.exec_zeroblob(), expected);

        let input = Value::Integer(-1);
        let expected = Value::Blob(vec![]);
        assert_eq!(input.exec_zeroblob(), expected);

        let input = Value::build_text("5");
        let expected = Value::Blob(vec![0; 5]);
        assert_eq!(input.exec_zeroblob(), expected);

        let input = Value::build_text("-5");
        let expected = Value::Blob(vec![]);
        assert_eq!(input.exec_zeroblob(), expected);

        let input = Value::build_text("text");
        let expected = Value::Blob(vec![]);
        assert_eq!(input.exec_zeroblob(), expected);

        let input = Value::Float(2.6);
        let expected = Value::Blob(vec![0; 2]);
        assert_eq!(input.exec_zeroblob(), expected);

        let input = Value::Blob(vec![1]);
        let expected = Value::Blob(vec![]);
        assert_eq!(input.exec_zeroblob(), expected);
    }

    #[test]
    fn test_replace() {
        let input_str = Value::build_text("bob");
        let pattern_str = Value::build_text("b");
        let replace_str = Value::build_text("a");
        let expected_str = Value::build_text("aoa");
        assert_eq!(
            Value::exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = Value::build_text("bob");
        let pattern_str = Value::build_text("b");
        let replace_str = Value::build_text("");
        let expected_str = Value::build_text("o");
        assert_eq!(
            Value::exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = Value::build_text("bob");
        let pattern_str = Value::build_text("b");
        let replace_str = Value::build_text("abc");
        let expected_str = Value::build_text("abcoabc");
        assert_eq!(
            Value::exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = Value::build_text("bob");
        let pattern_str = Value::build_text("a");
        let replace_str = Value::build_text("b");
        let expected_str = Value::build_text("bob");
        assert_eq!(
            Value::exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = Value::build_text("bob");
        let pattern_str = Value::build_text("");
        let replace_str = Value::build_text("a");
        let expected_str = Value::build_text("bob");
        assert_eq!(
            Value::exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = Value::build_text("bob");
        let pattern_str = Value::Null;
        let replace_str = Value::build_text("a");
        let expected_str = Value::Null;
        assert_eq!(
            Value::exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = Value::build_text("bo5");
        let pattern_str = Value::Integer(5);
        let replace_str = Value::build_text("a");
        let expected_str = Value::build_text("boa");
        assert_eq!(
            Value::exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = Value::build_text("bo5.0");
        let pattern_str = Value::Float(5.0);
        let replace_str = Value::build_text("a");
        let expected_str = Value::build_text("boa");
        assert_eq!(
            Value::exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = Value::build_text("bo5");
        let pattern_str = Value::Float(5.0);
        let replace_str = Value::build_text("a");
        let expected_str = Value::build_text("bo5");
        assert_eq!(
            Value::exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = Value::build_text("bo5.0");
        let pattern_str = Value::Float(5.0);
        let replace_str = Value::Float(6.0);
        let expected_str = Value::build_text("bo6.0");
        assert_eq!(
            Value::exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        // todo: change this test to use (0.1 + 0.2) instead of 0.3 when decimals are implemented.
        let input_str = Value::build_text("tes3");
        let pattern_str = Value::Integer(3);
        let replace_str = Value::Float(0.3);
        let expected_str = Value::build_text("tes0.3");
        assert_eq!(
            Value::exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );
    }
}
