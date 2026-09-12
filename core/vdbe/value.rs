#[cfg(feature = "json")]
use crate::types::TextSubtype;
use crate::{
    function::MathFunc,
    numeric::{format_float, format_float_for_quote, NullableInteger, Numeric},
    translate::collate::CollationSeq,
    types::{compare_immutable_single, AsValueRef, SeekOp},
    vdbe::affinity::{real_to_i64, Affinity},
    LimboError, Result, Value,
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
    // Use log10/log2 directly rather than log(x, base): the latter computes
    // ln(x)/ln(base), which can be 1 ulp off from the dedicated functions
    // SQLite calls, and e.g. mod() amplifies that into visible divergence.
    pub fn log10(x: f64) -> f64 {
        x.log10()
    }
    pub fn log2(x: f64) -> f64 {
        x.log2()
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
    pub fn degrees(x: f64) -> f64 {
        x.to_degrees()
    }
    pub fn radians(x: f64) -> f64 {
        x.to_radians()
    }
}

// we use exactly same math function as SQLite in tests in order to avoid mismatch in the differential tests due to floating-point precision issues
#[cfg(test)]
#[path = "../tests/unit/vdbe/value/cmath.rs"]
mod cmath;

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

#[inline]
fn sqlite_text_prefix(s: &str) -> &str {
    // A short value is scanned byte by byte: the character searcher of
    // `find` costs more to set up than the scan.
    let nul = if s.len() <= 32 {
        s.bytes().position(|b| b == 0)
    } else {
        s.find('\0')
    };
    match nul {
        Some(idx) => &s[..idx],
        None => s,
    }
}

enum TrimType {
    All,
    Left,
    Right,
}

impl Value {
    pub fn exec_lower(&self) -> Option<Self> {
        self.cast_text()
            .map(|s| Value::build_text(s.to_ascii_lowercase()))
    }

    pub fn exec_length(&self) -> Self {
        match self {
            Value::Text(t) => {
                Value::from_i64(sqlite_text_prefix(t.as_str()).chars().count() as i64)
            }
            Value::Numeric(_) => {
                // For numbers, SQLite returns the length of the string representation
                Value::from_i64(self.to_string().chars().count() as i64)
            }
            Value::Blob(blob) => Value::from_i64(blob.len() as i64),
            _ => self.to_owned(),
        }
    }

    pub fn exec_octet_length(&self) -> Self {
        match self {
            Value::Text(s) => Value::from_i64(s.as_str().len() as i64),
            Value::Blob(blob) => Value::from_i64(blob.len() as i64),
            Value::Numeric(_) => Value::from_i64(self.to_string().len() as i64),
            _ => self.to_owned(),
        }
    }

    pub fn exec_upper(&self) -> Option<Self> {
        self.cast_text()
            .map(|s| Value::build_text(s.to_ascii_uppercase()))
    }

    pub fn exec_sign(&self) -> Option<Value> {
        let v = Numeric::from_value_strict(self).map(|value| value.to_f64())?;

        Some(Value::from_i64(if v > 0.0 {
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
            Value::Text(s) => s.as_str(),
            Value::Null => return Value::build_text("?000"),
            _ => return Value::build_text("?000"),
        };

        if s.bytes().any(|b| !b.is_ascii_alphabetic()) {
            return Value::build_text("?000");
        }

        let mut bytes = s.bytes();
        let Some(first_char) = bytes.next() else {
            return Value::build_text("?000");
        };

        let first_upper = first_char.to_ascii_uppercase();
        let mut result = String::with_capacity(4);
        result.push(first_upper as char);
        let get_code = |b: u8| -> Option<char> {
            match b.to_ascii_lowercase() {
                b'b' | b'f' | b'p' | b'v' => Some('1'),
                b'c' | b'g' | b'j' | b'k' | b'q' | b's' | b'x' | b'z' => Some('2'),
                b'd' | b't' => Some('3'),
                b'l' => Some('4'),
                b'm' | b'n' => Some('5'),
                b'r' => Some('6'),
                _ => None, // a, e, i, o, u, y, h, w
            }
        };

        let mut prev_code = get_code(first_char);

        for b in bytes {
            if result.len() >= 4 {
                break;
            }

            // H and W are ignored completely in this step for continuity checks
            let lower = b.to_ascii_lowercase();
            if lower == b'h' || lower == b'w' {
                continue;
            }

            let code = get_code(b);
            if code.is_some() && code != prev_code {
                result.push(code.unwrap());
                prev_code = code;
            } else if code.is_none() {
                // Reset previous code for vowels/separators (a,e,i,o,u,y)
                prev_code = None;
            }
        }

        while result.len() < 4 {
            result.push('0');
        }

        Value::build_text(result)
    }

    #[expect(
        clippy::unnecessary_lazy_evaluations,
        reason = "ok_or skips the drop glue that otherwise bloats the happy path"
    )]
    pub fn exec_abs(&self) -> Result<Self> {
        Ok(match self {
            Value::Null => Value::Null,
            Value::Numeric(Numeric::Integer(v)) => {
                Value::from_i64(v.checked_abs().ok_or_else(|| LimboError::IntegerOverflow)?)
            }
            Value::Numeric(Numeric::Float(non_nan)) => Value::from_f64(f64::from(*non_nan).abs()),
            _ => {
                let s = match self {
                    Value::Text(text) => std::borrow::Cow::Borrowed(text.as_str()),
                    Value::Blob(blob) => String::from_utf8_lossy(blob),
                    _ => unreachable!(),
                };

                crate::numeric::str_to_f64(s)
                    .map(|v| Value::from_f64(f64::from(v).abs()))
                    .unwrap_or_else(|| Value::from_f64(0.0))
            }
        })
    }

    pub fn exec_random<F>(generate_random_number: F) -> Self
    where
        F: Fn() -> i64,
    {
        Value::from_i64(generate_random_number())
    }

    /// SQLite default max blob/string size (1GB)
    pub const MAX_BLOB_LENGTH: i64 = 1_000_000_000;

    pub fn exec_randomblob<F>(&self, fill_bytes: F) -> Result<Value>
    where
        F: Fn(&mut [u8]),
    {
        let length = match self {
            Value::Numeric(Numeric::Integer(i)) => *i,
            Value::Numeric(Numeric::Float(f)) => f64::from(*f) as i64,
            Value::Text(t) => t.as_str().parse().unwrap_or(1),
            _ => 1,
        }
        .max(1);

        if length > Self::MAX_BLOB_LENGTH {
            return Err(LimboError::TooBig);
        }

        let mut blob = crate::alloc::try_vec![0; length as usize]?;
        fill_bytes(&mut blob);
        Ok(Value::Blob(blob))
    }

    pub fn exec_quote(&self) -> Self {
        use std::fmt::Write;
        match self {
            Value::Null => Value::build_text("NULL"),
            Value::Numeric(Numeric::Integer(i)) => Value::build_text(i.to_string()),
            Value::Numeric(Numeric::Float(f)) => {
                Value::build_text(format_float_for_quote(f64::from(*f)))
            }
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

    pub fn exec_unistr_quote(&self) -> Self {
        const HEX: &[u8; 16] = b"0123456789abcdef";

        match self {
            Value::Text(s) => {
                let s = s.as_str();
                let mut end = s.len();
                let mut has_ctrl = false;

                for (i, &b) in s.as_bytes().iter().enumerate() {
                    match b {
                        0 => {
                            end = i;
                            break;
                        }
                        1..=0x1f => has_ctrl = true,
                        _ => {}
                    }
                }

                if !has_ctrl {
                    return self.exec_quote();
                }

                let prefix = &s[..end];
                let mut extra = 0;
                for &b in prefix.as_bytes() {
                    extra += match b {
                        1..=0x1f => 5, // \u00xx is 6 output bytes, replacing 1 input byte.
                        b'\\' | b'\'' => 1,
                        _ => 0,
                    };
                }

                let mut out = String::with_capacity(prefix.len() + extra + "unistr('')".len());
                out.push_str("unistr('");
                for c in prefix.chars() {
                    match c {
                        '\x01'..='\x1f' => {
                            let b = c as u8;
                            out.push('\\');
                            out.push('u');
                            out.push('0');
                            out.push('0');
                            out.push(HEX[(b >> 4) as usize] as char);
                            out.push(HEX[(b & 0x0f) as usize] as char);
                        }
                        '\\' => out.push_str("\\\\"),
                        '\'' => out.push_str("''"),
                        _ => out.push(c),
                    }
                }
                out.push_str("')");
                Value::build_text(out)
            }
            _ => self.exec_quote(),
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
    ) -> std::result::Result<Value, crate::alloc::TryReserveError> {
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

        // Match SQLite's substr algorithm exactly (func.c substrFunc)
        // Uses wrapping arithmetic to match C overflow behavior
        fn calculate_postions(
            mut p1: i64,
            len: usize,
            length_value: Option<&Value>,
        ) -> (usize, usize) {
            let len = len as i64;
            let mut p2 = match length_value {
                Some(Value::Numeric(Numeric::Integer(length))) => *length,
                // SQLite uses SQLITE_LIMIT_LENGTH (default 1 billion) when no explicit length.
                // Using len causes wrong results when p1 is large negative number.
                _ => Value::MAX_BLOB_LENGTH,
            };

            // Track if length was explicitly provided
            let explicit_length = length_value.is_some();

            // Handle negative start position (count from end)
            if p1 < 0 {
                p1 = p1.wrapping_add(len);
                if p1 < 0 {
                    if p2 < 0 {
                        p2 = 0;
                    } else {
                        p2 += p1;
                    }
                    p1 = 0;
                }
            } else if p1 > 0 {
                p1 -= 1; // Convert 1-indexed to 0-indexed
            } else if p2 > 0 && explicit_length {
                // SQLite quirk: when p1==0, p2>0, and explicit length, decrement p2
                // This means substr('x', 0, 3) returns 2 chars, not 3
                // But substr('x', 0) with no length returns whole string
                p2 -= 1;
            }

            // Handle negative length (characters preceding position)
            if p2 < 0 {
                if p2 < -p1 {
                    p2 = p1;
                } else {
                    p2 = -p2;
                }
                p1 -= p2;
            }

            // Clamp to valid range
            let start = p1.max(0).min(len) as usize;
            let end = p1.saturating_add(p2).max(0).min(len) as usize;
            (start, end)
        }

        let start_value = start_value.exec_cast("INT")?;
        let length_value = length_value
            .map(|value| value.exec_cast("INT"))
            .transpose()?;

        // If length is explicitly NULL, return NULL (SQLite behavior)
        if matches!(length_value, Some(Value::Null)) {
            return Ok(Value::Null);
        }

        Ok(match (value, start_value) {
            (Value::Blob(b), Value::Numeric(Numeric::Integer(start))) => {
                let (start, end) = calculate_postions(start, b.len(), length_value.as_ref());
                return Value::from_slice(&b[start..end]);
            }
            (value, Value::Numeric(Numeric::Integer(start))) => {
                if let Some(text) = value.cast_text() {
                    let s = sqlite_text_prefix(text.as_str());
                    // Use character count to accurately resolve negative offsets in UTF-8 strings
                    let char_count = s.chars().count();
                    let (mut start, mut end) =
                        calculate_postions(start, char_count, length_value.as_ref());

                    // https://github.com/sqlite/sqlite/blob/a248d84f/src/func.c#L417
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
        })
    }

    pub fn exec_instr(&self, pattern: &Value) -> Value {
        if matches!(self, Value::Null) || matches!(pattern, Value::Null) {
            return Value::Null;
        }

        if let (Value::Blob(reg), Value::Blob(pattern)) = (self, pattern) {
            // SQLite returns 1 for empty pattern (found at position 1)
            if pattern.is_empty() {
                return Value::from_i64(1);
            }
            let result = reg
                .windows(pattern.len())
                .position(|window| window == *pattern)
                .map_or(0, |i| i + 1);
            return Value::from_i64(result as i64);
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
            Some(byte_pos) => {
                // Convert byte position to character position (1-indexed)
                let char_pos = reg[..byte_pos].chars().count() + 1;
                Value::from_i64(char_pos as i64)
            }
            None => Value::from_i64(0),
        }
    }

    pub fn exec_subtype(&self) -> Value {
        match self {
            #[cfg(feature = "json")]
            Value::Text(t) if t.subtype == TextSubtype::Json => Value::from_i64(74),
            _ => Value::from_i64(0),
        }
    }

    pub fn exec_typeof(&self) -> Value {
        match self {
            Value::Null => Value::build_text("null"),
            Value::Numeric(Numeric::Integer(_)) => Value::build_text("integer"),
            Value::Numeric(Numeric::Float(_)) => Value::build_text("real"),
            Value::Text(_) => Value::build_text("text"),
            Value::Blob(_) => Value::build_text("blob"),
        }
    }

    pub fn exec_hex(&self) -> Value {
        match self {
            Value::Text(_) | Value::Numeric(_) => {
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
                None => match self.cast_text() {
                    Some(text) => {
                        let input = &text[0..text.find('\0').unwrap_or(text.len())];
                        let mut bytes = crate::alloc::vec![0; input.len() / 2];
                        match hex::decode_to_slice(input, &mut bytes) {
                            Ok(()) => Value::from_blob(bytes),
                            Err(_) => Value::Null,
                        }
                    }
                    None => Value::Null,
                },
                Some(ignore) => match ignore {
                    Value::Text(_) => {
                        let input = self.to_string();
                        let ignore = ignore.to_string();
                        let mut chars = input.chars().peekable();
                        let mut out =
                            <crate::ValueBlob as crate::alloc::TursoVecExt<u8>>::with_capacity(
                                input.len() / 2,
                            );

                        let is_sep = |c: char| ignore.contains(c) && !c.is_ascii_hexdigit();

                        loop {
                            while let Some(&c) = chars.peek() {
                                if is_sep(c) {
                                    chars.next();
                                } else {
                                    break;
                                }
                            }

                            let Some(c1) = chars.next() else {
                                return Value::Blob(out);
                            };
                            let Some(hi) = c1.to_digit(16) else {
                                return Value::Null;
                            };

                            let Some(c2) = chars.next() else {
                                return Value::Null;
                            };
                            let Some(lo) = c2.to_digit(16) else {
                                return Value::Null;
                            };

                            out.push(((hi << 4) | lo) as u8);
                        }
                    }
                    _ => Value::Null,
                },
            },
        }
    }

    /// Returns the raw bytes backing this value for the byte-manipulation
    /// functions (`get_byte`/`set_byte`). Blobs are used verbatim; text is
    /// interpreted as its UTF-8 bytes; numbers use their textual form (matching
    /// how `hex()` coerces). `NULL` has no byte view.
    fn byte_view(&self) -> Option<std::borrow::Cow<'_, [u8]>> {
        match self {
            Value::Null => None,
            Value::Blob(b) => Some(std::borrow::Cow::Borrowed(b)),
            Value::Text(t) => Some(std::borrow::Cow::Borrowed(t.as_str().as_bytes())),
            Value::Numeric(_) => Some(std::borrow::Cow::Owned(self.to_string().into_bytes())),
        }
    }

    /// PostgreSQL `get_byte(bytea, offset)`: returns the byte at the 0-based
    /// `offset` as an integer in the range 0..=255. Raises an error when the
    /// offset falls outside `0..length-1`, matching PostgreSQL exactly. A `NULL`
    /// input or offset yields `NULL`.
    pub fn exec_get_byte(&self, offset: &Value) -> Result<Value> {
        let Value::Numeric(Numeric::Integer(offset)) = offset.exec_cast("INT")? else {
            return Ok(Value::Null);
        };
        let Some(bytes) = self.byte_view() else {
            return Ok(Value::Null);
        };
        let len = bytes.len() as i64;
        if offset < 0 || offset >= len {
            return Err(LimboError::InvalidArgument(format!(
                "index {offset} out of valid range, 0..{}",
                len - 1
            )));
        }
        Ok(Value::from_i64(i64::from(bytes[offset as usize])))
    }

    /// PostgreSQL `set_byte(bytea, offset, newvalue)`: returns a blob with the
    /// byte at the 0-based `offset` replaced by the low 8 bits of `newvalue`.
    /// Raises an error when the offset falls outside `0..length-1`, matching
    /// PostgreSQL exactly. A `NULL` input, offset, or value yields `NULL`.
    pub fn exec_set_byte(&self, offset: &Value, new_value: &Value) -> Result<Value> {
        let Value::Numeric(Numeric::Integer(offset)) = offset.exec_cast("INT")? else {
            return Ok(Value::Null);
        };
        let Value::Numeric(Numeric::Integer(new_value)) = new_value.exec_cast("INT")? else {
            return Ok(Value::Null);
        };
        let Some(bytes) = self.byte_view() else {
            return Ok(Value::Null);
        };
        let len = bytes.len() as i64;
        if offset < 0 || offset >= len {
            return Err(LimboError::InvalidArgument(format!(
                "index {offset} out of valid range, 0..{}",
                len - 1
            )));
        }
        // Copy into a Turso-allocated blob, then overwrite the target byte in place.
        let mut result = Value::from_slice(&bytes)?;
        if let Value::Blob(out) = &mut result {
            // PostgreSQL truncates the new value to its low 8 bits (int32 stored
            // into an unsigned char), so e.g. 6555 becomes 155 and -1 becomes 255.
            out[offset as usize] = new_value as u8;
        }
        Ok(result)
    }

    pub fn exec_unicode(&self) -> Value {
        match self {
            Value::Text(_) | Value::Numeric(_) | Value::Blob(_) => {
                let text = self.to_string();
                if let Some(first_char) = text.chars().next() {
                    if first_char == '\0' {
                        return Value::Null;
                    }
                    Value::from_i64(first_char as u32 as i64)
                } else {
                    Value::Null
                }
            }
            _ => Value::Null,
        }
    }

    pub fn exec_unistr(&self) -> Result<Value> {
        let text = match self {
            Value::Text(t) => std::borrow::Cow::Borrowed(t.as_str()),
            Value::Numeric(_) | Value::Blob(_) => std::borrow::Cow::Owned(self.to_string()),
            _ => return Ok(Value::Null),
        };
        let bytes = text.as_bytes();
        let len = bytes.len();
        let mut out = String::with_capacity(len);
        let mut i = 0;

        while i < len {
            if bytes[i] != b'\\' {
                let start = i;
                while i < len && bytes[i] != b'\\' {
                    i += 1;
                }
                out.push_str(&text[start..i]);
                continue;
            }

            let v = match bytes.get(i + 1) {
                Some(b'\\') => {
                    out.push('\\');
                    i += 2;
                    continue;
                }
                Some(b) if b.is_ascii_hexdigit() => {
                    let v = parse_n_hex(&bytes[i + 1..], 4)?;
                    i += 5;
                    v
                }
                Some(b'+') => {
                    let v = parse_n_hex(&bytes[i + 2..], 6)?;
                    i += 8;
                    v
                }
                Some(b'u') => {
                    let v = parse_n_hex(&bytes[i + 2..], 4)?;
                    i += 6;
                    v
                }
                Some(b'U') => {
                    let v = parse_n_hex(&bytes[i + 2..], 8)?;
                    i += 10;
                    v
                }
                _ => return Err(LimboError::ParseError("invalid Unicode escape".to_string())),
            };

            // Reject surrogates and values above U+10FFFF. SQLite encodes
            // these as raw bytes, but Value::Text requires valid UTF-8.
            let ch = char::from_u32(v)
                .ok_or_else(|| LimboError::ParseError("invalid Unicode escape".to_string()))?;
            out.push(ch);
        }

        Ok(Value::build_text(out))
    }

    pub fn exec_round(&self, precision: Option<&Value>) -> Value {
        let Some(f) = Numeric::from_value(self).map(|v| v.to_f64()) else {
            return Value::Null;
        };

        let precision = match precision.map(|v| Numeric::from_value(v).map(|v| v.to_f64())) {
            None => 0.0,
            Some(Some(v)) => v,
            Some(None) => return Value::Null,
        };

        if !(-4503599627370496.0..=4503599627370496.0).contains(&f) {
            return Value::from_f64(f);
        }

        let precision = if precision < 1.0 { 0.0 } else { precision };
        let precision = precision.clamp(0.0, 30.0) as usize;

        if precision == 0 {
            return Value::from_f64(((f + if f < 0.0 { -0.5 } else { 0.5 }) as i64) as f64);
        }

        let f: f64 =
            crate::numeric::round_half_away_from_zero_tie(f, precision).unwrap_or_else(|| {
                crate::numeric::str_to_f64(format!("{f:.precision$}"))
                    .expect("formatted float should always parse successfully")
                    .into()
            });

        Value::from_f64(f)
    }

    fn _exec_trim(&self, pattern: Option<&Value>, trim_type: TrimType) -> Value {
        let text_cow = match self {
            Value::Text(s) => std::borrow::Cow::Borrowed(s.as_str()),
            Value::Null => return Value::Null,
            _ => std::borrow::Cow::Owned(self.to_string()),
        };
        let trimmed = match pattern {
            Some(p) => {
                if matches!(p, Value::Null) {
                    return Value::Null;
                }
                let pat_cow = match p {
                    Value::Text(s) => std::borrow::Cow::Borrowed(s.as_str()),
                    _ => std::borrow::Cow::Owned(p.to_string()),
                };
                let p_str = pat_cow.as_ref();
                match trim_type {
                    TrimType::All => text_cow.trim_matches(|c| p_str.contains(c)),
                    TrimType::Left => text_cow.trim_start_matches(|c| p_str.contains(c)),
                    TrimType::Right => text_cow.trim_end_matches(|c| p_str.contains(c)),
                }
            }
            None => match trim_type {
                TrimType::All => text_cow.trim_matches(' '),
                TrimType::Left => text_cow.trim_start_matches(' '),
                TrimType::Right => text_cow.trim_end_matches(' '),
            },
        };
        Value::build_text(trimmed.to_string())
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

    pub fn exec_zeroblob(&self) -> Result<Value> {
        let length: i64 = match self {
            Value::Numeric(Numeric::Integer(i)) => *i,
            Value::Numeric(Numeric::Float(f)) => f64::from(*f) as i64,
            Value::Text(s) => s.as_str().parse().unwrap_or(0),
            _ => 0,
        }
        .max(0);

        if length > Self::MAX_BLOB_LENGTH {
            return Err(LimboError::TooBig);
        }

        Ok(Value::Blob(crate::alloc::try_vec![0; length as usize]?))
    }

    // exec_if returns whether you should jump
    #[inline(always)]
    pub fn exec_if(&self, jump_if_null: bool, not: bool) -> bool {
        return if let Value::Numeric(Numeric::Integer(i)) = self {
            (*i != 0) != not
        } else {
            exec_if_converted(self, jump_if_null, not)
        };

        // Less common cases kept out of line to keep stack frames small
        #[inline(never)]
        fn exec_if_converted(value: &Value, jump_if_null: bool, not: bool) -> bool {
            match Numeric::from_value(value) {
                Some(v) => v.to_bool() != not,
                None => jump_if_null,
            }
        }
    }

    pub fn exec_cast(
        &self,
        datatype: &str,
    ) -> std::result::Result<Value, crate::alloc::TryReserveError> {
        if matches!(self, Value::Null) {
            return Ok(Value::Null);
        }
        Ok(match Affinity::affinity(datatype) {
            // NONE	Casting a value to a type-name with no affinity causes the value to be converted into a BLOB. Casting to a BLOB consists of first casting the value to TEXT in the encoding of the database connection, then interpreting the resulting byte sequence as a BLOB instead of as TEXT.
            Affinity::Blob | Affinity::None => {
                if let Value::Blob(blob) = self {
                    return Value::from_slice(blob);
                }
                // Convert to TEXT first, then interpret as BLOB
                // TODO: handle encoding
                let text = self.to_string();
                return Value::from_slice(text.as_bytes());
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
                    Value::from_f64(
                        crate::numeric::str_to_f64(&text)
                            .map(f64::from)
                            .unwrap_or(0.0),
                    )
                }
                Value::Text(t) => {
                    Value::from_f64(crate::numeric::str_to_f64(t).map(f64::from).unwrap_or(0.0))
                }
                Value::Numeric(Numeric::Integer(i)) => Value::from_f64(*i as f64),
                Value::Numeric(Numeric::Float(f)) => Value::Numeric(Numeric::Float(*f)),
                _ => Value::from_f64(0.0),
            },
            Affinity::Integer => match self {
                Value::Blob(b) => {
                    // Convert BLOB to TEXT first
                    let text = String::from_utf8_lossy(b);
                    Value::from_i64(crate::numeric::str_to_i64(&text).unwrap_or(0))
                }
                Value::Text(t) => Value::from_i64(crate::numeric::str_to_i64(t).unwrap_or(0)),
                Value::Numeric(Numeric::Integer(i)) => Value::from_i64(*i),
                // A cast of a REAL value into an INTEGER follows SQLite's sqlite3RealToI64:
                // truncate toward zero and clamp to i64::MIN/MAX if outside the safe range.
                Value::Numeric(Numeric::Float(f)) => Value::from_i64(real_to_i64(f64::from(*f))),
                _ => Value::from_i64(0),
            },
            Affinity::Numeric => match self {
                Value::Null => Value::Null,
                Value::Numeric(Numeric::Integer(v)) => Value::from_i64(*v),
                Value::Numeric(Numeric::Float(v)) => Value::Numeric(Numeric::Float(*v)),
                _ => {
                    let s = match self {
                        Value::Text(text) => text.as_str().into(),
                        Value::Blob(blob) => String::from_utf8_lossy(blob.as_slice()),
                        _ => unreachable!(),
                    };
                    crate::util::checked_cast_text_to_numeric(&s, false)
                        .ok()
                        .unwrap_or_else(|| Value::from_i64(0))
                }
            },
        })
    }

    pub fn exec_replace(
        source: &Value,
        pattern: &Value,
        replacement: &Value,
    ) -> std::result::Result<Value, crate::alloc::TryReserveError> {
        // The replace(X,Y,Z) function returns a string formed by substituting string Z for every occurrence of
        // string Y in string X. The BINARY collating sequence is used for comparisons. If Y is an empty string
        // then return X unchanged. If Z is not initially a string, it is cast to a UTF-8 string prior to processing.

        // If any of the arguments is NULL, the result is NULL.
        if matches!(source, Value::Null)
            || matches!(pattern, Value::Null)
            || matches!(replacement, Value::Null)
        {
            return Ok(Value::Null);
        }

        let source = source.exec_cast("TEXT")?;
        let pattern = pattern.exec_cast("TEXT")?;
        let replacement = replacement.exec_cast("TEXT")?;

        // If any of the casts failed, panic as text casting is not expected to fail.
        match (&source, &pattern, &replacement) {
            (Value::Text(source), Value::Text(pattern), Value::Text(replacement)) => {
                if pattern.as_str().is_empty() || pattern.as_str().starts_with('\0') {
                    return Ok(Value::Text(source.clone()));
                }

                let result = source
                    .as_str()
                    .replace(pattern.as_str(), replacement.as_str());
                Ok(Value::build_text(result))
            }
            _ => unreachable!("text cast should never fail"),
        }
    }

    pub fn exec_math_unary(&self, function: &MathFunc) -> Value {
        let v = Numeric::from_value_strict(self);

        // In case of some functions and integer input, return the input as is
        if let Some(Numeric::Integer(i)) = v {
            if matches! { function, MathFunc::Ceil | MathFunc::Ceiling | MathFunc::Floor | MathFunc::Trunc }
            {
                return Value::from_i64(i);
            }
        }

        let Some(f) = v.map(|v| v.to_f64()) else {
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
            MathFunc::Degrees => cmath::degrees(f),
            MathFunc::Exp => unsafe { cmath::exp(f) },
            MathFunc::Floor => libm::floor(f),
            MathFunc::Ln => unsafe { cmath::log(f) },
            MathFunc::Log10 => unsafe { cmath::log10(f) },
            MathFunc::Log2 => unsafe { cmath::log2(f) },
            MathFunc::Radians => cmath::radians(f),
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
            Value::from_f64(result)
        }
    }

    pub fn exec_math_binary(&self, rhs: &Value, function: &MathFunc) -> Value {
        let Some(lhs) = Numeric::from_value_strict(self).map(|v| v.to_f64()) else {
            return Value::Null;
        };

        let Some(rhs) = Numeric::from_value_strict(rhs).map(|v| v.to_f64()) else {
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
            Value::from_f64(result)
        }
    }

    /// Mirrors SQLite's logFunc: log(X) calls log10 directly, and log(B,X)
    /// always computes log(X)/log(B). Special-casing base 2 or 10 to call
    /// their dedicated logarithm functions looks more accurate but shifts
    /// the result by 1 ulp relative to SQLite's ratio.
    #[allow(unused_unsafe)]
    pub fn exec_math_log(&self, base: Option<&Value>) -> Value {
        let Some(f) = Numeric::from_value_strict(self).map(|v| v.to_f64()) else {
            return Value::Null;
        };

        if f <= 0.0 {
            return Value::Null;
        }

        let base = match base.map(|value| Numeric::from_value_strict(value).map(|v| v.to_f64())) {
            Some(Some(base)) => base,
            Some(None) => return Value::Null,
            None => return Value::from_f64(unsafe { cmath::log10(f) }),
        };

        if base <= 0.0 {
            return Value::Null;
        }

        let log_base = unsafe { cmath::log(base) };
        if log_base <= 0.0 {
            return Value::Null;
        }

        Value::from_f64(unsafe { cmath::log(f) } / log_base)
    }

    pub fn exec_add(&self, rhs: &Value) -> Value {
        (|| Numeric::from_value(self)?.checked_add(Numeric::from_value(rhs)?))().into()
    }

    pub fn exec_subtract(&self, rhs: &Value) -> Value {
        (|| Numeric::from_value(self)?.checked_sub(Numeric::from_value(rhs)?))().into()
    }

    pub fn exec_multiply(&self, rhs: &Value) -> Value {
        (|| Numeric::from_value(self)?.checked_mul(Numeric::from_value(rhs)?))().into()
    }

    pub fn exec_divide(&self, rhs: &Value) -> Value {
        (|| Numeric::from_value(self)?.checked_div(Numeric::from_value(rhs)?))().into()
    }

    pub fn exec_bit_and(&self, rhs: &Value) -> Value {
        (NullableInteger::from(self) & NullableInteger::from(rhs)).into()
    }

    pub fn exec_bit_or(&self, rhs: &Value) -> Value {
        (NullableInteger::from(self) | NullableInteger::from(rhs)).into()
    }

    pub fn exec_remainder(&self, rhs: &Value) -> Value {
        let convert_to_float = matches!(Numeric::from_value(self), Some(Numeric::Float(_)))
            || matches!(Numeric::from_value(rhs), Some(Numeric::Float(_)));

        match NullableInteger::from(self) % NullableInteger::from(rhs) {
            NullableInteger::Null => Value::Null,
            NullableInteger::Integer(v) => {
                if convert_to_float {
                    Value::from_f64(v as f64)
                } else {
                    Value::from_i64(v)
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
        match Numeric::from_value(self).map(|v| v.to_bool()) {
            None => Value::Null,
            Some(v) => Value::from_i64(!v as i64),
        }
    }

    #[turso_macros::allocation_site(crate::alloc::ValueBlobAllocationSite::Concat)]
    pub fn exec_concat(
        &self,
        rhs: &Value,
    ) -> std::result::Result<Value, crate::alloc::TryReserveError> {
        if let (Value::Blob(lhs), Value::Blob(rhs)) = (self, rhs) {
            let mut blob =
                <crate::ValueBlob as crate::alloc::TursoTryWithCapacityExt>::try_with_capacity_ext(
                    lhs.len() + rhs.len(),
                )?;
            blob.extend_from_slice(lhs);
            blob.extend_from_slice(rhs);
            return Ok(Value::Blob(blob));
        }

        let Some(lhs) = self.cast_text() else {
            return Ok(Value::Null);
        };

        let Some(rhs) = rhs.cast_text() else {
            return Ok(Value::Null);
        };

        Ok(Value::build_text(lhs + &rhs))
    }

    pub fn exec_and(&self, rhs: &Value) -> Value {
        match (
            Numeric::from_value(self).map(|v| v.to_bool()),
            Numeric::from_value(rhs).map(|v| v.to_bool()),
        ) {
            (Some(false), _) | (_, Some(false)) => Value::from_i64(0),
            (None, _) | (_, None) => Value::Null,
            _ => Value::from_i64(1),
        }
    }

    pub fn exec_or(&self, rhs: &Value) -> Value {
        match (
            Numeric::from_value(self).map(|v| v.to_bool()),
            Numeric::from_value(rhs).map(|v| v.to_bool()),
        ) {
            (Some(true), _) | (_, Some(true)) => Value::from_i64(1),
            (None, _) | (_, None) => Value::Null,
            _ => Value::from_i64(0),
        }
    }

    pub fn exec_like(pattern: &str, text: &str, escape: Option<char>) -> Result<bool, LimboError> {
        const MAX_LIKE_PATTERN_LENGTH: usize = 50000;
        if pattern.len() > MAX_LIKE_PATTERN_LENGTH {
            return Err(LimboError::Constraint(
                "LIKE or GLOB pattern too complex".to_string(),
            ));
        }
        let pattern = sqlite_text_prefix(pattern);
        let text = sqlite_text_prefix(text);

        // ASCII pattern and text without an escape character, the usual
        // case: match the bytes directly. This comes before the wildcard
        // scans below, which cost more per row than the match itself.
        if escape.is_none()
            && crate::types::is_ascii(pattern.as_bytes())
            && crate::types::is_ascii(text.as_bytes())
        {
            return Ok(like_ascii(pattern.as_bytes(), text.as_bytes()));
        }

        let has_escape = escape.is_some_and(|e| pattern.contains(e));

        // 1. Exact match (no wildcards)
        if !has_escape && !pattern.contains(['%', '_']) {
            return Ok(pattern.eq_ignore_ascii_case(text));
        }

        // 2. Fast Path: 'abc%' (Prefix)
        if !has_escape
            && pattern.ends_with('%')
            && !pattern[..pattern.len() - 1].contains(['%', '_'])
        {
            let prefix = &pattern[..pattern.len() - 1];
            if text.len() >= prefix.len() && text.is_char_boundary(prefix.len()) {
                return Ok(text[..prefix.len()].eq_ignore_ascii_case(prefix));
            }
            // Fall through to pattern_compare if boundary check fails (multi-byte UTF-8)
        }

        // 3. Fast Path: '%abc' (Suffix)
        if !has_escape && pattern.starts_with('%') && !pattern[1..].contains(['%', '_']) {
            let suffix = &pattern[1..];
            let start = text.len().wrapping_sub(suffix.len());
            if text.len() >= suffix.len() && text.is_char_boundary(start) {
                return Ok(text[start..].eq_ignore_ascii_case(suffix));
            }
            // Fall through to pattern_compare if boundary check fails (multi-byte UTF-8)
        }

        Ok(pattern_compare(pattern, text, &LIKE_INFO, escape) == CompareResult::Match)
    }

    pub fn exec_glob(pattern: &str, text: &str) -> Result<bool, LimboError> {
        const MAX_GLOB_PATTERN_LENGTH: usize = 50000;
        const GLOB_CHARS: [char; 3] = ['*', '?', '['];

        if pattern.len() > MAX_GLOB_PATTERN_LENGTH {
            return Err(LimboError::Constraint(
                "GLOB pattern too complex".to_string(),
            ));
        }
        let pattern = sqlite_text_prefix(pattern);
        let text = sqlite_text_prefix(text);

        // 1. Exact match (no wildcards)
        if !pattern.contains(GLOB_CHARS) {
            return Ok(pattern == text);
        }

        // 2. Fast Path: 'abc*' (Prefix)
        if pattern.ends_with('*') && !pattern[..pattern.len() - 1].contains(GLOB_CHARS) {
            let prefix = &pattern[..pattern.len() - 1];
            if text.len() >= prefix.len() && text.is_char_boundary(prefix.len()) {
                return Ok(&text[..prefix.len()] == prefix);
            }
            // Fall through to pattern_compare if boundary check fails (multi-byte UTF-8)
        }

        // 3. Fast Path: '*abc' (Suffix)
        if pattern.starts_with('*') && !pattern[1..].contains(GLOB_CHARS) {
            let suffix = &pattern[1..];
            let start = text.len().wrapping_sub(suffix.len());
            if text.len() >= suffix.len() && text.is_char_boundary(start) {
                return Ok(&text[start..] == suffix);
            }
            // Fall through to pattern_compare if boundary check fails (multi-byte UTF-8)
        }

        Ok(pattern_compare(pattern, text, &GLOB_INFO, None) == CompareResult::Match)
    }

    pub fn exec_min<'a, T: Iterator<Item = &'a Value>>(regs: T) -> Value {
        // SQLite: multi-arg min() returns NULL if ANY argument is NULL
        let mut result: Option<&Value> = None;
        for v in regs {
            if matches!(v, Value::Null) {
                return Value::Null;
            }
            result = Some(match result {
                None => v,
                Some(cur) if v <= cur => v,
                Some(cur) => cur,
            });
        }
        result.map(|v| v.to_owned()).unwrap_or(Value::Null)
    }

    pub fn exec_max<'a, T: Iterator<Item = &'a Value>>(regs: T) -> Value {
        // SQLite: multi-arg max() returns NULL if ANY argument is NULL
        let mut result: Option<&Value> = None;
        for v in regs {
            if matches!(v, Value::Null) {
                return Value::Null;
            }
            result = Some(match result {
                None => v,
                Some(cur) if v > cur => v,
                Some(cur) => cur,
            });
        }
        result.map(|v| v.to_owned()).unwrap_or(Value::Null)
    }

    /// Fallibly concatenate another value onto this Text value, converting it to a string.
    /// Panics if self is not a Text value.
    pub fn exec_group_concat(
        &mut self,
        other: &Value,
    ) -> std::result::Result<(), crate::alloc::TryReserveError> {
        let Value::Text(text) = self else {
            panic!("group_concat accumulator must be a Text value");
        };
        let acc = match &mut text.value {
            std::borrow::Cow::Owned(s) => s,
            borrowed => {
                let mut s = String::new();
                s.try_reserve(borrowed.len())?;
                s.push_str(borrowed);
                *borrowed = std::borrow::Cow::Owned(s);
                let std::borrow::Cow::Owned(s) = borrowed else {
                    unreachable!("accumulator was just converted to Owned");
                };
                s
            }
        };
        match other {
            Value::Text(text) => {
                acc.try_reserve(text.as_str().len())?;
                acc.push_str(text.as_str());
            }
            other => {
                let rendered = other.to_string();
                acc.try_reserve(rendered.len())?;
                acc.push_str(&rendered);
            }
        }
        Ok(())
    }

    pub fn exec_concat_strings<'a, T: Iterator<Item = &'a Self>>(registers: T) -> Self {
        let mut result = String::new();
        for val in registers {
            match val {
                Value::Null => continue,
                Value::Text(s) => result.push_str(s.as_str()),
                Value::Blob(b) => result.push_str(&String::from_utf8_lossy(b)),
                Value::Numeric(Numeric::Integer(i)) => result.push_str(&i.to_string()),
                Value::Numeric(Numeric::Float(f)) => result.push_str(&format_float(f64::from(*f))),
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
            Value::Text(_) | Value::Numeric(_) => Some(format!("{val}")),
            _ => None,
        });

        let result = parts.collect::<Vec<_>>().join(&separator);
        Value::build_text(result)
    }

    pub fn exec_char<'a, T: Iterator<Item = &'a Self>>(values: T) -> Self {
        let result: String = values
            .map(|x| {
                // char() coerces every argument to an integer codepoint, the
                // same way sqlite3_value_int64 / CAST(... AS INTEGER) does:
                // text and blobs by their numeric prefix (0 if none), floats by
                // truncation, NULL as 0. Turso previously accepted only integer
                // arguments and dropped the rest.
                let codepoint = match x {
                    Value::Numeric(Numeric::Integer(i)) => *i,
                    Value::Numeric(Numeric::Float(f)) => real_to_i64(f64::from(*f)),
                    Value::Text(t) => crate::numeric::str_to_i64(t.as_str()).unwrap_or(0),
                    Value::Blob(b) => {
                        crate::numeric::str_to_i64(String::from_utf8_lossy(b).as_ref()).unwrap_or(0)
                    }
                    Value::Null => 0,
                };
                // Invalid codepoints (negative, surrogates, or > U+10FFFF)
                // become U+FFFD, matching SQLite.
                if codepoint >= 0 {
                    char::from_u32(codepoint as u32).unwrap_or('\u{FFFD}')
                } else {
                    '\u{FFFD}'
                }
            })
            .collect();
        Value::build_text(result)
    }
}

/// Parse exactly `n` hex digits into a u32. Mirrors SQLite's isNHex().
fn parse_n_hex(bytes: &[u8], n: usize) -> Result<u32> {
    if bytes.len() < n {
        return Err(LimboError::ParseError("invalid Unicode escape".to_string()));
    }
    let mut v: u32 = 0;
    for &b in &bytes[..n] {
        let digit = match b {
            b'0'..=b'9' => b - b'0',
            b'a'..=b'f' => b - b'a' + 10,
            b'A'..=b'F' => b - b'A' + 10,
            _ => return Err(LimboError::ParseError("invalid Unicode escape".to_string())),
        };
        v = (v << 4) | digit as u32;
    }
    Ok(v)
}

/// Result of LIKE pattern comparison.
/// `NoWildcardMatch` signals an early abort when a literal after `%` cannot be found,
/// allowing the algorithm to skip unnecessary backtracking.
#[derive(PartialEq)]
enum CompareResult {
    Match,
    NoMatch,
    NoWildcardMatch,
}

struct PatternInfo {
    match_all: char,
    match_one: char,
    match_set: Option<char>,
    no_case: bool,
}

const LIKE_INFO: PatternInfo = PatternInfo {
    match_all: '%',
    match_one: '_',
    match_set: None,
    no_case: true,
};

/// LIKE without an escape character over ASCII bytes: `_` matches one byte,
/// `%` any run of bytes, letters compare without case. The last `%` seen is
/// the only backtrack point, as in the classic wildcard match: when the
/// bytes after it stop matching, the run it covers grows by one and the
/// match resumes after it. Same answers as `pattern_compare` with
/// `LIKE_INFO` for every ASCII input (see the tests).
fn like_ascii(pattern: &[u8], text: &[u8]) -> bool {
    let (mut p, mut t) = (0, 0);
    let mut backtrack: Option<(usize, usize)> = None;
    while t < text.len() {
        match pattern.get(p) {
            Some(b'%') => {
                backtrack = Some((p, t));
                p += 1;
            }
            Some(&c) if c == b'_' || c.eq_ignore_ascii_case(&text[t]) => {
                p += 1;
                t += 1;
            }
            _ => match backtrack {
                Some((star_p, star_t)) => {
                    p = star_p + 1;
                    t = star_t + 1;
                    backtrack = Some((star_p, t));
                }
                None => return false,
            },
        }
    }
    pattern[p..].iter().all(|&c| c == b'%')
}

const GLOB_INFO: PatternInfo = PatternInfo {
    match_all: '*',
    match_one: '?',
    match_set: Some('['),
    no_case: false,
};

/// LIKE and GLOB pattern matching based on SQLite's patternCompare algorithm (src/func.c).
/// Uses recursive descent with early termination via `NoWildcardMatch` to avoid
/// exponential backtracking on patterns like `%a%a%a%...%b`.
/// Ref: https://github.com/sqlite/sqlite/blob/master/src/func.c#L728
fn pattern_compare(
    pattern: &str,
    text: &str,
    info: &PatternInfo,
    escape: Option<char>,
) -> CompareResult {
    let mut p_indices = pattern.char_indices();
    let mut t_indices = text.char_indices();

    let mut p_curr = p_indices.next();
    let mut t_curr = t_indices.next();

    // Checkpoints for backtracking
    let mut wildcard_p_iter: Option<std::str::CharIndices> = None;
    let mut wildcard_t_iter: Option<std::str::CharIndices> = None;

    loop {
        match (p_curr, t_curr) {
            (Some((_, p_char)), Some((_, t_char))) => {
                if p_char == info.match_all && Some(p_char) != escape {
                    // Consume consecutive match_alls
                    let mut next_p = p_indices.clone();
                    while let Some((_, c)) = next_p.clone().next() {
                        if c == info.match_all && Some(c) != escape {
                            next_p.next();
                        } else {
                            break;
                        }
                    }

                    let mut lookahead_p = next_p.clone();
                    if let Some((_, next_char)) = lookahead_p.next() {
                        let is_wildcard = (next_char == info.match_all
                            && Some(next_char) != escape)
                            || (next_char == info.match_one && Some(next_char) != escape)
                            || (info.match_set == Some(next_char));

                        let is_escaped_next = Some(next_char) == escape;

                        if !is_wildcard && !is_escaped_next {
                            let mut found = false;

                            // Check current text char
                            if compare_chars(next_char, t_char, info.no_case) {
                                found = true;
                            } else {
                                // Scan remaining text
                                let lookahead_t = t_indices.clone();
                                for (_, t_c) in lookahead_t {
                                    if compare_chars(next_char, t_c, info.no_case) {
                                        found = true;
                                        break;
                                    }
                                }
                            }

                            if !found {
                                return CompareResult::NoWildcardMatch;
                            }
                        }
                    }

                    p_indices = next_p;
                    wildcard_p_iter = Some(p_indices.clone());
                    p_curr = p_indices.next();

                    if p_curr.is_none() {
                        return CompareResult::Match;
                    }

                    wildcard_t_iter = Some(t_indices.clone());
                    continue;
                }

                if p_char == info.match_one && Some(p_char) != escape {
                    p_curr = p_indices.next();
                    t_curr = t_indices.next();
                    continue;
                }

                // Handle Set (GLOB only)
                if info.match_set == Some(p_char) {
                    let mut seen = false;
                    let mut invert = false;
                    let c = t_char;

                    let mut next_c_opt = p_indices.next();

                    if let Some((_, c2)) = next_c_opt {
                        if c2 == '^' {
                            invert = true;
                            next_c_opt = p_indices.next();
                        }
                    }

                    let mut c2_opt = next_c_opt;
                    if let Some((_, c2)) = c2_opt {
                        if c2 == ']' {
                            if c == ']' {
                                seen = true;
                            }
                            c2_opt = p_indices.next();
                        }
                    }

                    let mut prior_c: Option<char> = None;

                    while let Some((_, c2)) = c2_opt {
                        if c2 == ']' {
                            break;
                        }

                        let mut is_range = false;
                        if c2 == '-' && prior_c.is_some() {
                            let lookahead = p_indices.clone().next();
                            if let Some((_, c3)) = lookahead {
                                if c3 != ']' {
                                    is_range = true;
                                    let start = prior_c.unwrap();
                                    let end = c3;
                                    if c >= start && c <= end {
                                        seen = true;
                                    }
                                    p_indices.next();
                                    prior_c = None;
                                }
                            }
                        }

                        if !is_range {
                            if c == c2 {
                                seen = true;
                            }
                            prior_c = Some(c2);
                        }

                        c2_opt = p_indices.next();
                    }

                    if c2_opt.is_none() || !(seen ^ invert) {
                        // Fallthrough to backtracking
                    } else {
                        p_curr = p_indices.next();
                        t_curr = t_indices.next();
                        continue;
                    }
                } else {
                    let (expected_char, next_p_iter) = if Some(p_char) == escape {
                        if let Some((_, literal)) = p_indices.next() {
                            (literal, p_indices.clone())
                        } else {
                            return CompareResult::NoMatch;
                        }
                    } else {
                        (p_char, p_indices.clone())
                    };

                    if compare_chars(expected_char, t_char, info.no_case) {
                        p_indices = next_p_iter;
                        p_curr = p_indices.next();
                        t_curr = t_indices.next();
                        continue;
                    }
                }
            }
            (None, None) => return CompareResult::Match,
            (Some((_, p_char)), None) if p_char == info.match_all && Some(p_char) != escape => {
                let mut temp = p_indices.clone();
                loop {
                    match temp.next() {
                        Some((_, c)) if c == info.match_all && Some(c) != escape => continue,
                        None => return CompareResult::Match,
                        _ => break,
                    }
                }
            }
            _ => {}
        }

        // Backtracking
        if let (Some(wp), Some(wt)) = (wildcard_p_iter.clone(), wildcard_t_iter.clone()) {
            p_indices = wp;
            p_curr = p_indices.next();
            t_indices = wt.clone();
            t_curr = t_indices.next();

            if t_curr.is_some() {
                wildcard_t_iter = Some(t_indices.clone());
                continue;
            }
        }

        return CompareResult::NoMatch;
    }
}

fn compare_chars(p: char, t: char, no_case: bool) -> bool {
    if no_case {
        p.eq_ignore_ascii_case(&t)
    } else {
        p == t
    }
}

#[cfg(test)]
#[path = "../tests/unit/vdbe/value/tests.rs"]
mod tests;
