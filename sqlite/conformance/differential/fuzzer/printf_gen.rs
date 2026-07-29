//! Adversarial printf SQL statement generator.
//!
//! Generates random `SELECT printf(...)` statements designed to find divergences
//! between Turso and SQLite. Focuses on corner cases: type mismatches, missing
//! arguments, extreme values, conflicting flags, and boundary precisions.

use rand::Rng;
use rand_chacha::ChaCha8Rng;

/// Specifier types that printf supports.
const SPECIFIER_TYPES: &[char] = &[
    'd', 'i', 'u', 'f', 'e', 'E', 'g', 'G', 'x', 'X', 'o', 'p', 's', 'c', 'q', 'Q', 'w', 'n', 'r',
];

/// Flag characters.
const FLAGS: &[char] = &['-', '+', ' ', '0', '#', ',', '!'];

/// Hardcoded edge case SQL statements that are run every time.
/// These are the known tricky cases that have caught real bugs.
pub const EDGE_CASE_BATTERY: &[&str] = &[
    // Missing arguments
    "SELECT printf('%p')",
    "SELECT printf('%d %d %d', 1)",
    "SELECT printf('%f %s')",
    // Type mismatches
    "SELECT printf('%d', 'hello')",
    "SELECT printf('%d', '')",
    "SELECT printf('%d', '3.14abc')",
    "SELECT printf('%f', 'not_a_number')",
    "SELECT printf('%f', '42')",
    "SELECT printf('%x', 'ff')",
    "SELECT printf('%s', 42)",
    "SELECT printf('%s', 3.14)",
    // NULL handling
    "SELECT printf('%s', NULL)",
    "SELECT printf('%d', NULL)",
    "SELECT printf('%f', NULL)",
    "SELECT printf('%q', NULL)",
    "SELECT printf('%Q', NULL)",
    "SELECT printf('%w', NULL)",
    "SELECT printf('%c', NULL)",
    // Rounding half-away-from-zero
    "SELECT printf('%.0f', 0.5)",
    "SELECT printf('%.0f', 1.5)",
    "SELECT printf('%.0f', 2.5)",
    "SELECT printf('%.0f', 3.5)",
    "SELECT printf('%.0f', 4.5)",
    "SELECT printf('%.0f', -0.5)",
    "SELECT printf('%.0f', -1.5)",
    "SELECT printf('%.1f', 0.25)",
    "SELECT printf('%.2f', 0.125)",
    "SELECT printf('%.0e', 2.5)",
    "SELECT printf('%.0e', 4.5)",
    // %g threshold rounding
    "SELECT printf('%g', 999999.5)",
    "SELECT printf('%.1g', 9.5)",
    "SELECT printf('%.2g', 99.5)",
    "SELECT printf('%.3g', 999.5)",
    // Infinity and NaN
    "SELECT printf('%f', 1e308*10)",
    "SELECT printf('%e', 1e308*10)",
    "SELECT printf('%g', 1e308*10)",
    "SELECT printf('%020f', 1e308*10)",
    "SELECT printf('%020e', 1e308*10)",
    "SELECT printf('%020g', 1e308*10)",
    "SELECT printf('%+f', 1e308*10)",
    "SELECT printf('%f', -(1e308*10))",
    "SELECT printf('%020f', -(1e308*10))",
    "SELECT printf('%f', 0.0/0.0)",
    "SELECT printf('%e', 0.0/0.0)",
    "SELECT printf('%g', 0.0/0.0)",
    "SELECT printf('%d', 0.0/0.0)",
    // Negative zero
    "SELECT printf('%f', -0.0)",
    "SELECT printf('%e', -0.0)",
    "SELECT printf('%g', -0.0)",
    "SELECT printf('%d', -0.0)",
    // Alternate flag (#)
    "SELECT printf('%#x', 255)",
    "SELECT printf('%#X', 255)",
    "SELECT printf('%#o', 8)",
    "SELECT printf('%#08x', 255)",
    "SELECT printf('%#04x', 255)",
    "SELECT printf('%#08o', 8)",
    "SELECT printf('%#.0f', 3.0)",
    "SELECT printf('%#.0e', 1.0)",
    "SELECT printf('%#.0g', 1.0)",
    "SELECT printf('%#g', 100000.0)",
    "SELECT printf('%#.1g', 1.0)",
    "SELECT printf('%#x', 0)",
    // Alt-form-2 (!)
    "SELECT printf('%!f', 1.0)",
    "SELECT printf('%!f', 3.14)",
    "SELECT printf('%!f', 0.0)",
    "SELECT printf('%!.0f', 3.0)",
    "SELECT printf('%!e', 23000000.0)",
    "SELECT printf('%!.0e', 23000000.0)",
    "SELECT printf('%!g', 100.0)",
    "SELECT printf('%!g', 0.0)",
    "SELECT printf('%!g', 0.000001)",
    // Zero-pad ignored for strings/chars
    "SELECT printf('%05s', 'hi')",
    "SELECT printf('%05c', 'A')",
    "SELECT printf('%05q', 'hi')",
    "SELECT printf('%05Q', 'hi')",
    // Flag conflicts: 0 + -
    "SELECT printf('%-010.2f', 3.14)",
    "SELECT printf('%-010d', 42)",
    // Flag conflicts: + and space
    "SELECT printf('%+ d', 42)",
    "SELECT printf('% +d', 42)",
    // Significant digit limiting
    "SELECT printf('%.20f', 1.0/3.0)",
    "SELECT printf('%.20e', 1.0/3.0)",
    "SELECT printf('%.20g', 1.0/3.0)",
    "SELECT printf('%.15f', 1234567890.123456789)",
    // Comma separator
    "SELECT printf('%,d', 1234567)",
    "SELECT printf('%,d', -1234567)",
    "SELECT printf('%,.2f', 1234567.89)",
    "SELECT printf('%,d', 999)",
    // Width and precision
    "SELECT printf('%10d', 42)",
    "SELECT printf('%010d', 42)",
    "SELECT printf('%.5d', 42)",
    "SELECT printf('%10.5d', 42)",
    "SELECT printf('%10s', 'hi')",
    "SELECT printf('%-10s', 'hi')",
    "SELECT printf('%.3s', 'hello')",
    "SELECT printf('%10.3s', 'hello')",
    // %q/%Q/%w with width/precision
    "SELECT printf('%10q', 'hi')",
    "SELECT printf('%-10q', 'hi')",
    "SELECT printf('%.2q', 'hello')",
    "SELECT printf('%10Q', 'hi')",
    "SELECT printf('%.2Q', 'hello')",
    "SELECT printf('%10w', 'hi')",
    "SELECT printf('%.2w', 'hello')",
    // Extreme integers
    "SELECT printf('%d', 9223372036854775807)",
    "SELECT printf('%d', -9223372036854775808)",
    "SELECT printf('%u', -1)",
    "SELECT printf('%x', -1)",
    "SELECT printf('%o', -1)",
    // Blob handling
    "SELECT printf('%s', X'48656C6C6F')",
    "SELECT printf('%s', X'48004C')",
    "SELECT printf('%d', X'3432')",
    // Trailing percent
    "SELECT printf('test%')",
    // Percent escape
    "SELECT printf('100%% done')",
    "SELECT printf('%%%%')",
    // Multiple specifiers
    "SELECT printf('%d %s %f', 42, 'hello', 3.14)",
    "SELECT printf('%d|%d|%d', 1, 2, 3)",
    // Dynamic width/precision
    "SELECT printf('%*d', 10, 42)",
    "SELECT printf('%*d', -10, 42)",
    "SELECT printf('%.*f', 2, 3.14159)",
    "SELECT printf('%*.*f', 10, 2, 3.14159)",
    // %n (silently ignored)
    "SELECT printf('%n')",
    "SELECT printf('before%nafter')",
    // Extra arguments (should be ignored)
    "SELECT printf('%d', 1, 2, 3)",
    "SELECT printf('hello', 1, 2)",
    // Non-string format argument
    "SELECT printf(123)",
    "SELECT printf(3.14)",
    "SELECT printf(NULL) IS NULL",
    // Large precision on various types
    "SELECT printf('%.50f', 1.0)",
    "SELECT printf('%.0f', 1e20)",
    "SELECT printf('%.50d', 42)",
    // Precision 0 special cases
    "SELECT printf('%.0f', 0.0)",
    "SELECT printf('%.0g', 0.0)",
    "SELECT printf('%.0e', 0.0)",
    "SELECT printf('%.0d', 0)",
    "SELECT printf('%.0x', 0)",
    "SELECT printf('%.0o', 0)",
    "SELECT printf('%.0s', 'hello')",
];

/// Adversarial printf SQL generator.
pub struct PrintfGenerator {
    rng: ChaCha8Rng,
    /// Probability (0.0-1.0) that an argument will be a corner case.
    corner_case_prob: f64,
}

impl PrintfGenerator {
    pub fn new(rng: ChaCha8Rng) -> Self {
        Self {
            rng,
            corner_case_prob: 0.5,
        }
    }

    /// Generate a random printf SQL statement.
    pub fn gen_printf_sql(&mut self) -> String {
        let (format_str, specifiers) = self.gen_format_string();
        let args = self.gen_args(&specifiers);

        if args.is_empty() {
            format!("SELECT printf('{format_str}')")
        } else {
            format!("SELECT printf('{}', {})", format_str, args.join(", "))
        }
    }

    /// Generate a format string and return it along with the specifier chars
    /// (needed so we can generate appropriately typed or mistyped args).
    fn gen_format_string(&mut self) -> (String, Vec<SpecInfo>) {
        let num_specifiers = self.rng.random_range(1..=4);
        let mut format_str = String::new();
        let mut specifiers = Vec::new();

        for i in 0..num_specifiers {
            // Sometimes add literal text between specifiers
            if i > 0 && self.rng.random_bool(0.5) {
                let literals = [" ", "|", ", ", " - ", ": ", "  "];
                let lit = literals[self.rng.random_range(0..literals.len())];
                format_str.push_str(lit);
            }

            // Occasionally insert %%
            if self.rng.random_bool(0.1) {
                format_str.push_str("%%");
            }

            let spec = self.gen_specifier();
            format_str.push('%');

            // Flags
            for &flag in &spec.flags {
                format_str.push(flag);
            }

            // Width
            match spec.width {
                Width::None => {}
                Width::Fixed(n) => {
                    format_str.push_str(&n.to_string());
                }
                Width::Dynamic => {
                    format_str.push('*');
                    specifiers.push(SpecInfo {
                        spec_type: '*',
                        flags: Vec::new(),
                    });
                }
            }

            // Precision
            match spec.precision {
                Precision::None => {}
                Precision::Fixed(n) => {
                    format_str.push('.');
                    format_str.push_str(&n.to_string());
                }
                Precision::Dynamic => {
                    format_str.push_str(".*");
                    specifiers.push(SpecInfo {
                        spec_type: '*',
                        flags: Vec::new(),
                    });
                }
            }

            format_str.push(spec.spec_type);
            specifiers.push(SpecInfo {
                spec_type: spec.spec_type,
                flags: spec.flags.clone(),
            });
        }

        // Occasionally add trailing %
        if self.rng.random_bool(0.05) {
            format_str.push('%');
        }

        (format_str, specifiers)
    }

    /// Generate a single format specifier with random flags, width, precision, type.
    fn gen_specifier(&mut self) -> Specifier {
        // Type selection
        let spec_type = if self.rng.random_bool(0.05) {
            // 5% chance of unknown/invalid specifier
            let invalid = ['y', 'j', 'b', 'a'];
            invalid[self.rng.random_range(0..invalid.len())]
        } else {
            SPECIFIER_TYPES[self.rng.random_range(0..SPECIFIER_TYPES.len())]
        };

        // Flags - biased toward conflicting combinations
        let mut flags = Vec::new();
        if self.rng.random_bool(0.3) {
            // Conflicting: 0 and -
            flags.push('0');
            flags.push('-');
        } else {
            for &flag in FLAGS {
                if self.rng.random_bool(0.25) {
                    flags.push(flag);
                }
            }
        }
        // Sometimes add + and space together (conflicting)
        if self.rng.random_bool(0.15) && !flags.contains(&'+') && !flags.contains(&' ') {
            flags.push('+');
            flags.push(' ');
        }

        // Width
        let width_roll: f64 = self.rng.random();
        let width = if width_roll < 0.40 {
            Width::None
        } else if width_roll < 0.60 {
            Width::Fixed(self.rng.random_range(1..=10))
        } else if width_roll < 0.80 {
            Width::Fixed(self.rng.random_range(10..=50))
        } else if width_roll < 0.90 {
            Width::Dynamic
        } else {
            Width::Fixed(self.rng.random_range(100..=1000))
        };

        // Precision - biased toward boundary values
        let prec_roll: f64 = self.rng.random();
        let precision = if prec_roll < 0.30 {
            Precision::None
        } else if prec_roll < 0.50 {
            Precision::Fixed(0)
        } else if prec_roll < 0.70 {
            Precision::Fixed(self.rng.random_range(1..=6))
        } else if prec_roll < 0.85 {
            // Boundary: 15-16 is where IEEE precision breaks down
            let boundary = [15, 16, 17, 20];
            Precision::Fixed(boundary[self.rng.random_range(0..boundary.len())])
        } else if prec_roll < 0.95 {
            Precision::Fixed(self.rng.random_range(20..=50))
        } else {
            Precision::Dynamic
        };

        Specifier {
            spec_type,
            flags,
            width,
            precision,
        }
    }

    /// Generate arguments for the given specifiers.
    /// Deliberately introduces argument count mismatches.
    fn gen_args(&mut self, specifiers: &[SpecInfo]) -> Vec<String> {
        let arg_consuming: Vec<&SpecInfo> =
            specifiers.iter().filter(|s| s.spec_type != 'n').collect();

        if arg_consuming.is_empty() {
            // No args needed, but sometimes generate extra
            if self.rng.random_bool(0.2) {
                return vec![self.gen_random_arg()];
            }
            return Vec::new();
        }

        let roll: f64 = self.rng.random();
        if roll < 0.50 {
            // Exact match
            arg_consuming.iter().map(|s| self.gen_arg(s)).collect()
        } else if roll < 0.70 {
            // Too few args
            let count = self.rng.random_range(0..arg_consuming.len());
            arg_consuming[..count]
                .iter()
                .map(|s| self.gen_arg(s))
                .collect()
        } else if roll < 0.80 {
            // Zero args despite having specifiers
            Vec::new()
        } else if roll < 0.90 {
            // Too many args
            let mut args: Vec<String> = arg_consuming.iter().map(|s| self.gen_arg(s)).collect();
            let extra = self.rng.random_range(1..=3);
            for _ in 0..extra {
                args.push(self.gen_random_arg());
            }
            args
        } else {
            // All NULL args
            vec!["NULL".to_string(); arg_consuming.len()]
        }
    }

    /// Generate an argument for a specifier. Uses corner_case_prob to decide
    /// whether to generate a matched or mismatched arg.
    fn gen_arg(&mut self, spec: &SpecInfo) -> String {
        if self.rng.random_bool(self.corner_case_prob) {
            self.gen_corner_case_arg(spec)
        } else {
            self.gen_matched_arg(spec)
        }
    }

    /// Generate an argument that matches the specifier's expected type.
    fn gen_matched_arg(&mut self, spec: &SpecInfo) -> String {
        match spec.spec_type {
            '*' => {
                // Width/precision: use small values to avoid GB-sized output
                self.rng.random_range(-200..=500).to_string()
            }
            'd' | 'i' | 'u' | 'x' | 'X' | 'o' | 'p' | 'r' => self.gen_integer_arg(),
            'f' | 'e' | 'E' | 'g' | 'G' => self.gen_float_arg(),
            's' | 'c' | 'q' | 'Q' | 'w' => self.gen_string_arg(),
            _ => self.gen_random_arg(),
        }
    }

    /// Generate a deliberately wrong or extreme argument for a specifier.
    fn gen_corner_case_arg(&mut self, spec: &SpecInfo) -> String {
        let roll: f64 = self.rng.random();
        if roll < 0.25 {
            // NULL
            "NULL".to_string()
        } else if roll < 0.50 {
            // Wrong type entirely
            match spec.spec_type {
                'd' | 'i' | 'u' | 'x' | 'X' | 'o' | 'p' | 'r' => {
                    // Int specifier gets string or float
                    if self.rng.random_bool(0.5) {
                        self.gen_string_arg()
                    } else {
                        self.gen_float_arg()
                    }
                }
                'f' | 'e' | 'E' | 'g' | 'G' => {
                    // Float specifier gets string or integer
                    if self.rng.random_bool(0.5) {
                        self.gen_string_arg()
                    } else {
                        self.gen_integer_arg()
                    }
                }
                's' | 'c' => {
                    // String specifier gets int, float, or blob
                    let r: f64 = self.rng.random();
                    if r < 0.33 {
                        self.gen_integer_arg()
                    } else if r < 0.66 {
                        self.gen_float_arg()
                    } else {
                        self.gen_blob_arg()
                    }
                }
                'q' | 'Q' | 'w' => {
                    // SQL quoting gets int, float, blob, or NULL
                    let r: f64 = self.rng.random();
                    if r < 0.25 {
                        self.gen_integer_arg()
                    } else if r < 0.50 {
                        self.gen_float_arg()
                    } else if r < 0.75 {
                        self.gen_blob_arg()
                    } else {
                        "NULL".to_string()
                    }
                }
                '*' => {
                    // Dynamic width/precision gets string or negative
                    if self.rng.random_bool(0.5) {
                        self.gen_string_arg()
                    } else {
                        format!("{}", self.rng.random_range(-100..=0i64))
                    }
                }
                _ => self.gen_random_arg(),
            }
        } else {
            // Extreme value for the correct type
            self.gen_extreme_arg(spec)
        }
    }

    /// Generate an extreme but type-appropriate argument.
    fn gen_extreme_arg(&mut self, spec: &SpecInfo) -> String {
        match spec.spec_type {
            '*' => {
                // Width/precision args should be small to avoid huge output
                let extremes: &[&str] = &[
                    "0", "-1", "1", "5", "10", "20", "50", "100", "200", "-20", "-69", "500",
                    "-500",
                ];
                extremes[self.rng.random_range(0..extremes.len())].to_string()
            }
            'd' | 'i' | 'u' | 'x' | 'X' | 'o' | 'p' | 'r' => {
                let extremes: &[&str] = &[
                    "0",
                    "-1",
                    "1",
                    "9223372036854775807",  // i64::MAX
                    "-9223372036854775808", // i64::MIN
                    "256",
                    "65536",
                    "4294967296",
                    "-2147483648", // i32::MIN
                    "2147483647",  // i32::MAX
                ];
                extremes[self.rng.random_range(0..extremes.len())].to_string()
            }
            'f' | 'e' | 'E' | 'g' | 'G' => {
                let extremes: &[&str] = &[
                    "0.0",
                    "-0.0",
                    "0.5",
                    "1.5",
                    "2.5",
                    "3.5",
                    "4.5",
                    "-0.5",
                    "-1.5",
                    "-2.5",
                    "0.0/0.0",     // NaN
                    "1e308*10",    // +Inf
                    "-(1e308*10)", // -Inf
                    "1.0/3.0",     // repeating decimal
                    "1e20",
                    "1e-300",
                    "999999.5",             // %g threshold
                    "9.5",                  // %.1g threshold
                    "99.5",                 // %.2g threshold
                    "0.25",                 // half-away boundary
                    "0.125",                // half-away boundary
                    "1234567890.123456789", // sig digit test
                ];
                extremes[self.rng.random_range(0..extremes.len())].to_string()
            }
            's' | 'c' | 'q' | 'Q' | 'w' => {
                let extremes: &[&str] = &[
                    "''",
                    "'hello'",
                    "'it''s a test'",
                    "'42'",
                    "'3.14'",
                    "'-0'",
                    "'1e5'",
                    "'  spaces  '",
                    "X'48004C'", // blob with NUL
                    "X'FF80'",   // invalid UTF-8 blob
                    "X'00'",     // just NUL
                    "X''",       // empty blob
                    "NULL",
                ];
                extremes[self.rng.random_range(0..extremes.len())].to_string()
            }
            _ => self.gen_random_arg(),
        }
    }

    /// Generate a random integer SQL literal.
    fn gen_integer_arg(&mut self) -> String {
        let roll: f64 = self.rng.random();
        if roll < 0.3 {
            // Small integers
            format!("{}", self.rng.random_range(-100..=100i64))
        } else if roll < 0.6 {
            // Common values
            let common = [0, 1, -1, 42, -42, 255, 256, 1000];
            format!("{}", common[self.rng.random_range(0..common.len())])
        } else {
            // Large/extreme values
            let extreme: &[i64] = &[
                i64::MAX,
                i64::MIN,
                i32::MAX as i64,
                i32::MIN as i64,
                u32::MAX as i64,
            ];
            format!("{}", extreme[self.rng.random_range(0..extreme.len())])
        }
    }

    /// Generate a random float SQL literal (possibly as expression for NaN/Inf).
    fn gen_float_arg(&mut self) -> String {
        let roll: f64 = self.rng.random();
        if roll < 0.3 {
            // Small floats
            format!("{:.6}", self.rng.random_range(-100.0..100.0f64))
        } else if roll < 0.6 {
            // Common values
            let common = ["0.0", "1.0", "-1.0", "3.14", "-3.14", "0.5", "1.5", "2.5"];
            common[self.rng.random_range(0..common.len())].to_string()
        } else {
            // Extreme values
            let extreme = [
                "0.0/0.0",     // NaN
                "1e308*10",    // Inf
                "-(1e308*10)", // -Inf
                "-0.0",
                "1e20",
                "1e-10",
                "1e308",
                "1.0/3.0",
                "999999.5",
            ];
            extreme[self.rng.random_range(0..extreme.len())].to_string()
        }
    }

    /// Generate a random string SQL literal (properly escaped).
    fn gen_string_arg(&mut self) -> String {
        let roll: f64 = self.rng.random();
        if roll < 0.3 {
            // Simple strings
            let simple = ["'hello'", "'world'", "'test'", "'abc'", "'A'", "'hi'"];
            simple[self.rng.random_range(0..simple.len())].to_string()
        } else if roll < 0.6 {
            // Numeric strings (coercion tests)
            let numeric = [
                "'42'", "'3.14'", "'-0'", "'1e5'", "'0xff'", "'123abc'", "''",
            ];
            numeric[self.rng.random_range(0..numeric.len())].to_string()
        } else {
            // Strings with special chars
            let special = [
                "'it''s'",
                "'he said \"hi\"'",
                "'  spaces  '",
                "'line1\nline2'",
                "''",
            ];
            special[self.rng.random_range(0..special.len())].to_string()
        }
    }

    /// Generate a random blob SQL literal.
    fn gen_blob_arg(&mut self) -> String {
        let blobs = [
            "X'48656C6C6F'", // "Hello"
            "X'48004C'",     // H\0L (NUL inside)
            "X'FF80'",       // invalid UTF-8
            "X'00'",         // just NUL
            "X''",           // empty
            "X'3432'",       // "42" as bytes
        ];
        blobs[self.rng.random_range(0..blobs.len())].to_string()
    }

    /// Generate a random argument of any type.
    fn gen_random_arg(&mut self) -> String {
        let roll: f64 = self.rng.random();
        if roll < 0.25 {
            self.gen_integer_arg()
        } else if roll < 0.50 {
            self.gen_float_arg()
        } else if roll < 0.75 {
            self.gen_string_arg()
        } else if roll < 0.90 {
            "NULL".to_string()
        } else {
            self.gen_blob_arg()
        }
    }
}

/// Internal representation of a format specifier during generation.
struct Specifier {
    spec_type: char,
    flags: Vec<char>,
    width: Width,
    precision: Precision,
}

/// Info about a generated specifier, kept for argument generation.
pub struct SpecInfo {
    pub spec_type: char,
    pub flags: Vec<char>,
}

#[derive(Clone)]
enum Width {
    None,
    Fixed(usize),
    Dynamic,
}

#[derive(Clone)]
enum Precision {
    None,
    Fixed(usize),
    Dynamic,
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng;

    use super::*;

    #[test]
    fn test_generator_deterministic() {
        let rng1 = ChaCha8Rng::seed_from_u64(42);
        let rng2 = ChaCha8Rng::seed_from_u64(42);
        let mut generator1 = PrintfGenerator::new(rng1);
        let mut generator2 = PrintfGenerator::new(rng2);

        for _ in 0..100 {
            assert_eq!(generator1.gen_printf_sql(), generator2.gen_printf_sql());
        }
    }

    #[test]
    fn test_generator_produces_valid_sql() {
        let rng = ChaCha8Rng::seed_from_u64(12345);
        let mut generator = PrintfGenerator::new(rng);

        for _ in 0..100 {
            let sql = generator.gen_printf_sql();
            assert!(sql.starts_with("SELECT printf("));
            assert!(sql.ends_with(')'));
        }
    }

    #[test]
    fn test_edge_cases_are_valid_sql() {
        for sql in EDGE_CASE_BATTERY {
            assert!(
                sql.starts_with("SELECT printf("),
                "Invalid edge case: {sql}"
            );
        }
    }
}
