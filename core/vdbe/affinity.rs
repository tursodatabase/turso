use either::Either;
use turso_parser::ast::{Expr, Literal};

use crate::{types::AsValueRef, Value, ValueRef};

/// # SQLite Column Type Affinities
///
/// Each column in an SQLite 3 database is assigned one of the following type affinities:
///
/// - **TEXT**
/// - **NUMERIC**
/// - **INTEGER**
/// - **REAL**
/// - **BLOB**
///
/// > **Note:** Historically, the "BLOB" type affinity was called "NONE". However, this term was renamed to avoid confusion with "no affinity".
///
/// ## Affinity Descriptions
///
/// ### **TEXT**
/// - Stores data using the NULL, TEXT, or BLOB storage classes.
/// - Numerical data inserted into a column with TEXT affinity is converted into text form before being stored.
/// - **Example:**
///   ```sql
///   CREATE TABLE example (col TEXT);
///   INSERT INTO example (col) VALUES (123); -- Stored as '123' (text)
///   SELECT typeof(col) FROM example; -- Returns 'text'
///   ```
///
/// ### **NUMERIC**
/// - Can store values using all five storage classes.
/// - Text data is converted to INTEGER or REAL (in that order of preference) if it is a well-formed integer or real literal.
/// - If the text represents an integer too large for a 64-bit signed integer, it is converted to REAL.
/// - If the text is not a well-formed literal, it is stored as TEXT.
/// - Hexadecimal integer literals are stored as TEXT for historical compatibility.
/// - Floating-point values that can be exactly represented as integers are converted to integers.
/// - **Example:**
///   ```sql
///   CREATE TABLE example (col NUMERIC);
///   INSERT INTO example (col) VALUES ('3.0e+5'); -- Stored as 300000 (integer)
///   SELECT typeof(col) FROM example; -- Returns 'integer'
///   ```
///
/// ### **INTEGER**
/// - Behaves like NUMERIC affinity but differs in `CAST` expressions.
/// - **Example:**
///   ```sql
///   CREATE TABLE example (col INTEGER);
///   INSERT INTO example (col) VALUES (4.0); -- Stored as 4 (integer)
///   SELECT typeof(col) FROM example; -- Returns 'integer'
///   ```
///
/// ### **REAL**
/// - Similar to NUMERIC affinity but forces integer values into floating-point representation.
/// - **Optimization:** Small floating-point values with no fractional component may be stored as integers on disk to save space. This is invisible at the SQL level.
/// - **Example:**
///   ```sql
///   CREATE TABLE example (col REAL);
///   INSERT INTO example (col) VALUES (4); -- Stored as 4.0 (real)
///   SELECT typeof(col) FROM example; -- Returns 'real'
///   ```
///
/// ### **BLOB**
/// - Does not prefer any storage class.
/// - No coercion is performed between storage classes.
/// - **Example:**
///   ```sql
///   CREATE TABLE example (col BLOB);
///   INSERT INTO example (col) VALUES (x'1234'); -- Stored as a binary blob
///   SELECT typeof(col) FROM example; -- Returns 'blob'
///   ```
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Affinity {
    Blob = 0,
    Text = 1,
    Numeric = 2,
    Integer = 3,
    Real = 4,
}

pub const SQLITE_AFF_NONE: char = 'A'; // Historically called NONE, but it's the same as BLOB
pub const SQLITE_AFF_TEXT: char = 'B';
pub const SQLITE_AFF_NUMERIC: char = 'C';
pub const SQLITE_AFF_INTEGER: char = 'D';
pub const SQLITE_AFF_REAL: char = 'E';

impl Affinity {
    /// This is meant to be used in opcodes like Eq, which state:
    ///
    /// "The SQLITE_AFF_MASK portion of P5 must be an affinity character - SQLITE_AFF_TEXT, SQLITE_AFF_INTEGER, and so forth.
    /// An attempt is made to coerce both inputs according to this affinity before the comparison is made.
    /// If the SQLITE_AFF_MASK is 0x00, then numeric affinity is used.
    /// Note that the affinity conversions are stored back into the input registers P1 and P3.
    /// So this opcode can cause persistent changes to registers P1 and P3.""
    pub fn aff_mask(&self) -> char {
        match self {
            Affinity::Integer => SQLITE_AFF_INTEGER,
            Affinity::Text => SQLITE_AFF_TEXT,
            Affinity::Blob => SQLITE_AFF_NONE,
            Affinity::Real => SQLITE_AFF_REAL,
            Affinity::Numeric => SQLITE_AFF_NUMERIC,
        }
    }

    pub fn from_char(char: char) -> Self {
        match char {
            SQLITE_AFF_INTEGER => Affinity::Integer,
            SQLITE_AFF_TEXT => Affinity::Text,
            SQLITE_AFF_NONE => Affinity::Blob,
            SQLITE_AFF_REAL => Affinity::Real,
            SQLITE_AFF_NUMERIC => Affinity::Numeric,
            _ => Affinity::Blob,
        }
    }

    pub fn as_char_code(&self) -> u8 {
        self.aff_mask() as u8
    }

    pub fn from_char_code(code: u8) -> Self {
        Self::from_char(code as char)
    }

    pub fn is_numeric(&self) -> bool {
        matches!(self, Affinity::Integer | Affinity::Real | Affinity::Numeric)
    }

    pub fn has_affinity(&self) -> bool {
        !matches!(self, Affinity::Blob)
    }

    /// 3.1. Determination Of Column Affinity
    /// For tables not declared as STRICT, the affinity of a column is determined by the declared type of the column, according to the following rules in the order shown:
    ///
    /// If the declared type contains the string "INT" then it is assigned INTEGER affinity.
    ///
    /// If the declared type of the column contains any of the strings "CHAR", "CLOB", or "TEXT" then that column has TEXT affinity. Notice that the type VARCHAR contains the string "CHAR" and is thus assigned TEXT affinity.
    ///
    /// If the declared type for a column contains the string "BLOB" or if no type is specified then the column has affinity BLOB.
    ///
    /// If the declared type for a column contains any of the strings "REAL", "FLOA", or "DOUB" then the column has REAL affinity.
    ///
    /// Otherwise, the affinity is NUMERIC.
    ///
    /// Note that the order of the rules for determining column affinity is important. A column whose declared type is "CHARINT" will match both rules 1 and 2 but the first rule takes precedence and so the column affinity will be INTEGER.
    #[expect(clippy::self_named_constructors)]
    pub fn affinity(datatype: &str) -> Self {
        let datatype = datatype.to_ascii_uppercase();

        // Rule 1: INT -> INTEGER affinity
        if datatype.contains("INT") {
            return Affinity::Integer;
        }

        // Rule 2: CHAR/CLOB/TEXT -> TEXT affinity
        if datatype.contains("CHAR") || datatype.contains("CLOB") || datatype.contains("TEXT") {
            return Affinity::Text;
        }

        // Rule 3: BLOB or empty -> BLOB affinity (historically called NONE)
        if datatype.contains("BLOB") || datatype.is_empty() || datatype.contains("ANY") {
            return Affinity::Blob;
        }

        // Rule 4: REAL/FLOA/DOUB -> REAL affinity
        if datatype.contains("REAL") || datatype.contains("FLOA") || datatype.contains("DOUB") {
            return Affinity::Real;
        }

        // Rule 5: Otherwise -> NUMERIC affinity
        Affinity::Numeric
    }

    pub fn convert<'a>(&self, val: &'a impl AsValueRef) -> Option<Either<ValueRef<'a>, Value>> {
        let val = val.as_value_ref();
        let is_text = matches!(val, ValueRef::Text(_));
        // Apply affinity conversions
        match self {
            Affinity::Numeric | Affinity::Integer => is_text
                .then(|| apply_numeric_affinity(val, false))
                .flatten()
                .map(Either::Left),

            Affinity::Text => {
                if is_text {
                    is_numeric_value(val)
                        .then(|| stringify_register(val))
                        .flatten()
                        .map(Either::Right)
                } else {
                    None
                }
            }

            Affinity::Real => {
                let mut left = is_text
                    .then(|| apply_numeric_affinity(val, false))
                    .flatten();

                if let ValueRef::Integer(i) = left.unwrap_or(val) {
                    left = Some(ValueRef::Float(i as f64));
                }

                left.map(Either::Left)
            }

            Affinity::Blob => None, // Do nothing for blob affinity.
        }
    }

    /// Return TRUE if the given expression is a constant which would be
    /// unchanged by OP_Affinity with the affinity given in the second
    /// argument.
    ///
    /// This routine is used to determine if the OP_Affinity operation
    /// can be omitted.  When in doubt return FALSE.  A false negative
    /// is harmless.  A false positive, however, can result in the wrong
    /// answer.
    ///
    /// reference https://github.com/sqlite/sqlite/blob/master/src/expr.c#L3000
    pub fn expr_needs_no_affinity_change(&self, expr: &Expr) -> bool {
        if !self.has_affinity() {
            return true;
        }
        // TODO: check for unary minus in the expr, as it may be an additional optimization.
        // This involves mostly likely walking the expression
        match expr {
            Expr::Literal(literal) => match literal {
                Literal::Numeric(_) => self.is_numeric(),
                Literal::String(_) => matches!(self, Affinity::Text),
                Literal::Blob(_) => true,
                _ => false,
            },
            Expr::Column {
                is_rowid_alias: true,
                ..
            } => self.is_numeric(),
            _ => false,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum NumericParseResult {
    NotNumeric,      // not a valid number
    PureInteger,     // pure integer (entire string)
    HasDecimalOrExp, // has decimal point or exponent (entire string)
    ValidPrefixOnly, // valid prefix but not entire string
}

#[derive(Debug)]
pub enum ParsedNumber {
    None,
    Integer(i64),
    Float(f64),
}

impl ParsedNumber {
    fn as_integer(&self) -> Option<i64> {
        match self {
            ParsedNumber::Integer(i) => Some(*i),
            _ => None,
        }
    }

    fn as_float(&self) -> Option<f64> {
        match self {
            ParsedNumber::Float(f) => Some(*f),
            _ => None,
        }
    }
}

pub fn try_for_float(text: &str) -> (NumericParseResult, ParsedNumber) {
    let bytes = text.as_bytes();
    if bytes.is_empty() {
        return (NumericParseResult::NotNumeric, ParsedNumber::None);
    }

    let mut pos = 0;
    let len = bytes.len();

    while pos < len && is_space(bytes[pos]) {
        pos += 1;
    }

    if pos >= len {
        return (NumericParseResult::NotNumeric, ParsedNumber::None);
    }

    let mut sign = 1i64;

    if bytes[pos] == b'-' {
        sign = -1;
        pos += 1;
    } else if bytes[pos] == b'+' {
        pos += 1;
    }

    if pos >= len {
        return (NumericParseResult::NotNumeric, ParsedNumber::None);
    }

    let mut significand = 0u64;
    let mut decimal_adjust = 0i32;
    let mut has_digits = false;

    // Parse digits before decimal point
    while pos < len && bytes[pos].is_ascii_digit() {
        has_digits = true;
        let digit = (bytes[pos] - b'0') as u64;

        if significand <= (u64::MAX - 9) / 10 {
            significand = significand * 10 + digit;
        } else {
            // Skip overflow digits but adjust exponent
            decimal_adjust += 1;
        }
        pos += 1;
    }

    let mut has_decimal = false;
    let mut has_exponent = false;

    // Check for decimal point
    if pos < len && bytes[pos] == b'.' {
        has_decimal = true;
        pos += 1;

        // Parse fractional digits
        while pos < len && bytes[pos].is_ascii_digit() {
            has_digits = true;
            let digit = (bytes[pos] - b'0') as u64;

            if significand <= (u64::MAX - 9) / 10 {
                significand = significand * 10 + digit;
                decimal_adjust -= 1;
            }
            pos += 1;
        }
    }

    if !has_digits {
        return (NumericParseResult::NotNumeric, ParsedNumber::None);
    }

    // Check for exponent
    let mut exponent = 0i32;
    if pos < len && (bytes[pos] == b'e' || bytes[pos] == b'E') {
        has_exponent = true;
        pos += 1;

        if pos >= len {
            // Incomplete exponent, but we have valid digits before
            return create_result_from_significand(
                significand,
                sign,
                decimal_adjust,
                has_decimal,
                has_exponent,
                NumericParseResult::ValidPrefixOnly,
            );
        }

        let mut exp_sign = 1i32;
        if bytes[pos] == b'-' {
            exp_sign = -1;
            pos += 1;
        } else if bytes[pos] == b'+' {
            pos += 1;
        }

        if pos >= len || !bytes[pos].is_ascii_digit() {
            // Incomplete exponent
            return create_result_from_significand(
                significand,
                sign,
                decimal_adjust,
                has_decimal,
                false,
                NumericParseResult::ValidPrefixOnly,
            );
        }

        // Parse exponent digits
        while pos < len && bytes[pos].is_ascii_digit() {
            let digit = (bytes[pos] - b'0') as i32;
            if exponent < 10000 {
                exponent = exponent * 10 + digit;
            } else {
                exponent = 10000; // Cap at large value
            }
            pos += 1;
        }
        exponent *= exp_sign;
    }

    // Skip trailing whitespace
    while pos < len && is_space(bytes[pos]) {
        pos += 1;
    }

    // Determine if we consumed the entire string
    let consumed_all = pos >= len;
    let final_exponent = decimal_adjust + exponent;

    let parse_result = if !consumed_all {
        NumericParseResult::ValidPrefixOnly
    } else if has_decimal || has_exponent {
        NumericParseResult::HasDecimalOrExp
    } else {
        NumericParseResult::PureInteger
    };

    create_result_from_significand(
        significand,
        sign,
        final_exponent,
        has_decimal,
        has_exponent,
        parse_result,
    )
}

fn create_result_from_significand(
    significand: u64,
    sign: i64,
    exponent: i32,
    has_decimal: bool,
    has_exponent: bool,
    parse_result: NumericParseResult,
) -> (NumericParseResult, ParsedNumber) {
    if significand == 0 {
        match parse_result {
            NumericParseResult::PureInteger => {
                return (parse_result, ParsedNumber::Integer(0));
            }
            _ => {
                return (parse_result, ParsedNumber::Float(0.0));
            }
        }
    }

    // For pure integers without exponent, try to return as integer
    if !has_decimal && !has_exponent && exponent == 0 && significand <= i64::MAX as u64 {
        let signed_val = (significand as i64).wrapping_mul(sign);
        return (parse_result, ParsedNumber::Integer(signed_val));
    }

    // Convert to float
    let mut result = significand as f64;

    let mut exp = exponent;
    match exp.cmp(&0) {
        std::cmp::Ordering::Greater => {
            while exp >= 100 {
                result *= 1e100;
                exp -= 100;
            }
            while exp >= 10 {
                result *= 1e10;
                exp -= 10;
            }
            while exp >= 1 {
                result *= 10.0;
                exp -= 1;
            }
        }
        std::cmp::Ordering::Less => {
            while exp <= -100 {
                result *= 1e-100;
                exp += 100;
            }
            while exp <= -10 {
                result *= 1e-10;
                exp += 10;
            }
            while exp <= -1 {
                result *= 0.1;
                exp += 1;
            }
        }
        std::cmp::Ordering::Equal => {}
    }

    if sign < 0 {
        result = -result;
    }

    (parse_result, ParsedNumber::Float(result))
}

pub fn is_space(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | b'\r' | b'\x0c')
}

fn real_to_i64(r: f64) -> i64 {
    if r < -9223372036854774784.0 {
        i64::MIN
    } else if r > 9223372036854774784.0 {
        i64::MAX
    } else {
        r as i64
    }
}

fn apply_integer_affinity(val: ValueRef) -> Option<ValueRef> {
    let ValueRef::Float(f) = val else {
        return None;
    };

    let ix = real_to_i64(f);

    // Only convert if round-trip is exact and not at extreme values
    if f == (ix as f64) && ix > i64::MIN && ix < i64::MAX {
        Some(ValueRef::Integer(ix))
    } else {
        None
    }
}

/// Try to convert a value into a numeric representation if we can
/// do so without loss of information. In other words, if the string
/// looks like a number, convert it into a number. If it does not
/// look like a number, leave it alone.
pub fn apply_numeric_affinity(val: ValueRef, try_for_int: bool) -> Option<ValueRef> {
    let ValueRef::Text(text) = val else {
        return None; // Only apply to text values
    };

    let text_str = text.as_str();
    let (parse_result, parsed_value) = try_for_float(text_str);

    // Only convert if we have a complete valid number (not just a prefix)
    match parse_result {
        NumericParseResult::NotNumeric | NumericParseResult::ValidPrefixOnly => {
            None // Leave as text
        }
        NumericParseResult::PureInteger => {
            if let Some(int_val) = parsed_value.as_integer() {
                Some(ValueRef::Integer(int_val))
            } else if let Some(float_val) = parsed_value.as_float() {
                let res = ValueRef::Float(float_val);
                if try_for_int {
                    apply_integer_affinity(res)
                } else {
                    Some(res)
                }
            } else {
                None
            }
        }
        NumericParseResult::HasDecimalOrExp => {
            if let Some(float_val) = parsed_value.as_float() {
                let res = ValueRef::Float(float_val);
                // If try_for_int is true, try to convert float to int if exact
                if try_for_int {
                    apply_integer_affinity(res)
                } else {
                    Some(res)
                }
            } else {
                None
            }
        }
    }
}

fn is_numeric_value(val: ValueRef) -> bool {
    matches!(val, ValueRef::Integer(_) | ValueRef::Float(_))
}

fn stringify_register(val: ValueRef) -> Option<Value> {
    match val {
        ValueRef::Integer(i) => Some(Value::build_text(i.to_string())),
        ValueRef::Float(f) => Some(Value::build_text(f.to_string())),
        ValueRef::Text(_) | ValueRef::Null | ValueRef::Blob(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_apply_numeric_affinity_partial_numbers() {
        let val = Value::Text("123abc".into());
        let res = apply_numeric_affinity(val.as_value_ref(), false);
        assert!(res.is_none());

        let val = Value::Text("-53093015420544-15062897".into());
        let res = apply_numeric_affinity(val.as_value_ref(), false);
        assert!(res.is_none());

        let val = Value::Text("123.45xyz".into());
        let res = apply_numeric_affinity(val.as_value_ref(), false);
        assert!(res.is_none());
    }

    #[test]
    fn test_apply_numeric_affinity_complete_numbers() {
        let val = Value::Text("123".into());
        let res = apply_numeric_affinity(val.as_value_ref(), false);
        assert_eq!(res, Some(ValueRef::Integer(123)));

        let val = Value::Text("123.45".into());
        let res = apply_numeric_affinity(val.as_value_ref(), false);
        assert_eq!(res, Some(ValueRef::Float(123.45)));

        let val = Value::Text("  -456  ".into());
        let res = apply_numeric_affinity(val.as_value_ref(), false);
        assert_eq!(res, Some(ValueRef::Integer(-456)));

        let val = Value::Text("0".into());
        let res = apply_numeric_affinity(val.as_value_ref(), false);
        assert_eq!(res, Some(ValueRef::Integer(0)));
    }
}
