//! Literal value generation.

use crate::ast::Literal;
use crate::context::Context;
use crate::policy::Policy;
use crate::schema::DataType;

/// Generate a literal value for the given data type.
pub fn generate_literal(ctx: &mut Context, data_type: DataType, policy: &Policy) -> Literal {
    // Check for NULL generation
    if ctx.gen_bool_with_prob(policy.null_probability) {
        return Literal::Null;
    }

    match data_type {
        DataType::Integer => generate_integer(ctx),
        DataType::Real => generate_real(ctx),
        DataType::Text => generate_text(ctx),
        DataType::Blob => generate_blob(ctx),
        DataType::Null => Literal::Null,
    }
}

/// Generate an integer literal.
pub fn generate_integer(ctx: &mut Context) -> Literal {
    // Use a reasonable range that avoids edge cases
    let value = ctx.gen_i64_range(-1_000_000, 1_000_000);
    Literal::Integer(value)
}

/// Generate a real literal.
pub fn generate_real(ctx: &mut Context) -> Literal {
    let value = ctx.gen_f64_range(-1000.0, 1000.0);
    Literal::Real(value)
}

/// Generate a text literal.
pub fn generate_text(ctx: &mut Context) -> Literal {
    let len = ctx.gen_range_inclusive(1, 20);
    let text = ctx.gen_string(len);
    Literal::Text(text)
}

/// Generate a blob literal.
pub fn generate_blob(ctx: &mut Context) -> Literal {
    let len = ctx.gen_range_inclusive(1, 16);
    let bytes = ctx.gen_bytes(len);
    Literal::Blob(bytes)
}

/// Generate a literal suitable for comparison with a column.
pub fn generate_comparable_literal(
    ctx: &mut Context,
    data_type: DataType,
    _policy: &Policy,
) -> Literal {
    match data_type {
        DataType::Integer => generate_integer(ctx),
        DataType::Real => generate_real(ctx),
        DataType::Text => generate_text(ctx),
        DataType::Blob => generate_blob(ctx),
        DataType::Null => Literal::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_integer() {
        let mut ctx = Context::new_with_seed(42);
        let lit = generate_integer(&mut ctx);
        assert!(matches!(lit, Literal::Integer(_)));
    }

    #[test]
    fn test_generate_real() {
        let mut ctx = Context::new_with_seed(42);
        let lit = generate_real(&mut ctx);
        assert!(matches!(lit, Literal::Real(_)));
    }

    #[test]
    fn test_generate_text() {
        let mut ctx = Context::new_with_seed(42);
        let lit = generate_text(&mut ctx);
        if let Literal::Text(s) = lit {
            assert!(!s.is_empty());
            assert!(s.len() <= 20);
        } else {
            panic!("Expected Text literal");
        }
    }

    #[test]
    fn test_generate_blob() {
        let mut ctx = Context::new_with_seed(42);
        let lit = generate_blob(&mut ctx);
        if let Literal::Blob(b) = lit {
            assert!(!b.is_empty());
            assert!(b.len() <= 16);
        } else {
            panic!("Expected Blob literal");
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
}
