//! Literal value generation.

use crate::ast::Literal;
use crate::context::Context;
use crate::policy::{LiteralConfig, Policy, StringCharset};
use crate::schema::DataType;

/// Generate a literal value for the given data type.
pub fn generate_literal(ctx: &mut Context, data_type: DataType, policy: &Policy) -> Literal {
    generate_literal_with_config(ctx, data_type, &policy.literal_config)
}

/// Generate a literal value for the given data type using the provided config.
pub fn generate_literal_with_config(
    ctx: &mut Context,
    data_type: DataType,
    config: &LiteralConfig,
) -> Literal {
    // Check for NULL generation
    if ctx.gen_bool_with_prob(config.null_probability) {
        return Literal::Null;
    }

    match data_type {
        DataType::Integer => generate_integer(ctx, config),
        DataType::Real => generate_real(ctx, config),
        DataType::Text => generate_text(ctx, config),
        DataType::Blob => generate_blob(ctx, config),
        DataType::Null => Literal::Null,
    }
}

/// Generate an integer literal.
pub fn generate_integer(ctx: &mut Context, config: &LiteralConfig) -> Literal {
    let value = ctx.gen_i64_range(config.int_min, config.int_max);
    Literal::Integer(value)
}

/// Generate a real literal.
pub fn generate_real(ctx: &mut Context, config: &LiteralConfig) -> Literal {
    let value = ctx.gen_f64_range(config.real_min, config.real_max);
    Literal::Real(value)
}

/// Generate a text literal.
pub fn generate_text(ctx: &mut Context, config: &LiteralConfig) -> Literal {
    let len = ctx.gen_range_inclusive(config.string_min_len, config.string_max_len);
    let text = generate_string_with_charset(ctx, len, config.string_charset);
    Literal::Text(text)
}

/// Generate a string using the specified charset.
fn generate_string_with_charset(ctx: &mut Context, len: usize, charset: StringCharset) -> String {
    let chars: &[u8] = match charset {
        StringCharset::Alphanumeric => {
            b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        }
        StringCharset::Alpha => b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
        StringCharset::Numeric => b"0123456789",
        StringCharset::AsciiPrintable => {
            b" !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"
        }
        StringCharset::Unicode => {
            // For simplicity, use alphanumeric for now
            // TODO: Add actual unicode support
            b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        }
    };

    (0..len)
        .map(|_| {
            let idx = ctx.gen_range(chars.len());
            chars[idx] as char
        })
        .collect()
}

/// Generate a blob literal.
pub fn generate_blob(ctx: &mut Context, config: &LiteralConfig) -> Literal {
    let len = ctx.gen_range_inclusive(config.blob_min_size, config.blob_max_size);
    let bytes = ctx.gen_bytes(len);
    Literal::Blob(bytes)
}

/// Generate a literal suitable for comparison with a column.
pub fn generate_comparable_literal(
    ctx: &mut Context,
    data_type: DataType,
    config: &LiteralConfig,
) -> Literal {
    match data_type {
        DataType::Integer => generate_integer(ctx, config),
        DataType::Real => generate_real(ctx, config),
        DataType::Text => generate_text(ctx, config),
        DataType::Blob => generate_blob(ctx, config),
        DataType::Null => Literal::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_config() -> LiteralConfig {
        LiteralConfig::default()
    }

    #[test]
    fn test_generate_integer() {
        let mut ctx = Context::new_with_seed(42);
        let config = default_config();
        let lit = generate_integer(&mut ctx, &config);
        assert!(matches!(lit, Literal::Integer(_)));
    }

    #[test]
    fn test_generate_integer_range() {
        let mut ctx = Context::new_with_seed(42);
        let config = LiteralConfig {
            int_min: 0,
            int_max: 10,
            ..Default::default()
        };

        for _ in 0..100 {
            if let Literal::Integer(v) = generate_integer(&mut ctx, &config) {
                assert!((0..=10).contains(&v), "value {v} out of range");
            }
        }
    }

    #[test]
    fn test_generate_real() {
        let mut ctx = Context::new_with_seed(42);
        let config = default_config();
        let lit = generate_real(&mut ctx, &config);
        assert!(matches!(lit, Literal::Real(_)));
    }

    #[test]
    fn test_generate_real_range() {
        let mut ctx = Context::new_with_seed(42);
        let config = LiteralConfig {
            real_min: 0.0,
            real_max: 1.0,
            ..Default::default()
        };

        for _ in 0..100 {
            if let Literal::Real(v) = generate_real(&mut ctx, &config) {
                assert!((0.0..=1.0).contains(&v), "value {v} out of range");
            }
        }
    }

    #[test]
    fn test_generate_text() {
        let mut ctx = Context::new_with_seed(42);
        let config = default_config();
        let lit = generate_text(&mut ctx, &config);
        if let Literal::Text(s) = lit {
            assert!(!s.is_empty());
        } else {
            panic!("Expected Text literal");
        }
    }

    #[test]
    fn test_generate_text_length() {
        let mut ctx = Context::new_with_seed(42);
        let config = LiteralConfig {
            string_min_len: 5,
            string_max_len: 10,
            ..Default::default()
        };

        for _ in 0..100 {
            if let Literal::Text(s) = generate_text(&mut ctx, &config) {
                assert!(
                    s.len() >= 5 && s.len() <= 10,
                    "length {} out of range",
                    s.len()
                );
            }
        }
    }

    #[test]
    fn test_generate_blob() {
        let mut ctx = Context::new_with_seed(42);
        let config = default_config();
        let lit = generate_blob(&mut ctx, &config);
        if let Literal::Blob(b) = lit {
            assert!(!b.is_empty());
        } else {
            panic!("Expected Blob literal");
        }
    }

    #[test]
    fn test_generate_blob_size() {
        let mut ctx = Context::new_with_seed(42);
        let config = LiteralConfig {
            blob_min_size: 8,
            blob_max_size: 16,
            ..Default::default()
        };

        for _ in 0..100 {
            if let Literal::Blob(b) = generate_blob(&mut ctx, &config) {
                assert!(
                    b.len() >= 8 && b.len() <= 16,
                    "length {} out of range",
                    b.len()
                );
            }
        }
    }

    #[test]
    fn test_null_probability() {
        let mut ctx = Context::new_with_seed(42);
        let policy = Policy::default().with_null_probability(1.0);

        for _ in 0..10 {
            let lit = generate_literal(&mut ctx, DataType::Integer, &policy);
            assert!(matches!(lit, Literal::Null));
        }
    }

    #[test]
    fn test_string_charset_alpha() {
        let mut ctx = Context::new_with_seed(42);
        let config = LiteralConfig {
            string_charset: StringCharset::Alpha,
            string_min_len: 10,
            string_max_len: 10,
            ..Default::default()
        };

        if let Literal::Text(s) = generate_text(&mut ctx, &config) {
            assert!(s.chars().all(|c| c.is_ascii_alphabetic()));
        }
    }

    #[test]
    fn test_string_charset_numeric() {
        let mut ctx = Context::new_with_seed(42);
        let config = LiteralConfig {
            string_charset: StringCharset::Numeric,
            string_min_len: 10,
            string_max_len: 10,
            ..Default::default()
        };

        if let Literal::Text(s) = generate_text(&mut ctx, &config) {
            assert!(s.chars().all(|c| c.is_ascii_digit()));
        }
    }
}
