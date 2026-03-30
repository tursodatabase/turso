//! Translation of VACUUM statements to VDBE bytecode.

use crate::vdbe::builder::ProgramBuilder;
use crate::vdbe::insn::Insn;
use crate::{bail_parse_error, Result};
use turso_parser::ast::{Expr, Literal, Name};

/// Translate a VACUUM statement into VDBE bytecode.
///
/// Currently only VACUUM INTO is supported. Plain VACUUM (which compacts
/// the database in place) is not yet implemented.
///
/// # Arguments
/// * `program` - The program builder to emit instructions to
/// * `schema_name` - Optional schema/database name (not yet supported)
/// * `into` - Optional destination path for VACUUM INTO
///
/// # Returns
/// The modified program builder on success
pub fn translate_vacuum(
    program: &mut ProgramBuilder,
    schema_name: Option<&Name>,
    into: Option<&Expr>,
) -> Result<()> {
    // Schema name support (for attached databases) is not yet implemented
    if schema_name.is_some() {
        bail_parse_error!("VACUUM with schema name is not supported yet");
    }

    match into {
        Some(dest_expr) => {
            // VACUUM INTO 'path' - create compacted copy at destination
            let dest_path = extract_path_from_expr(dest_expr)?;
            program.emit_insn(Insn::VacuumInto { dest_path });
            Ok(())
        }
        None => {
            // Plain VACUUM - not yet supported
            bail_parse_error!(
                "VACUUM is not supported yet. Use VACUUM INTO 'filename' to create a compacted copy."
            );
        }
    }
}

/// Extract a file path string from an expression.
///
/// The expression can be either:
/// - A string literal: `VACUUM INTO 'path/to/file.db'`
/// - An identifier (variable name, though not commonly used)
fn extract_path_from_expr(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Literal(Literal::String(s)) => {
            // Remove surrounding quotes if present
            let path = s.trim_matches('\'').trim_matches('"');
            if path.is_empty() {
                bail_parse_error!("VACUUM INTO path cannot be empty");
            }
            Ok(path.to_string())
        }
        Expr::Id(name) => {
            // Allow identifier as path (unusual but valid)
            Ok(name.as_str().to_string())
        }
        _ => {
            bail_parse_error!("VACUUM INTO requires a string literal path");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_path_from_string_literal() {
        let expr = Expr::Literal(Literal::String("'test.db'".to_string()));
        let path = extract_path_from_expr(&expr).unwrap();
        assert_eq!(path, "test.db");
    }

    #[test]
    fn test_extract_path_from_string_literal_double_quotes() {
        let expr = Expr::Literal(Literal::String("\"test.db\"".to_string()));
        let path = extract_path_from_expr(&expr).unwrap();
        assert_eq!(path, "test.db");
    }

    #[test]
    fn test_extract_path_from_identifier() {
        let expr = Expr::Id(Name::exact("myfile".to_string()));
        let path = extract_path_from_expr(&expr).unwrap();
        assert_eq!(path, "myfile");
    }

    #[test]
    fn test_extract_path_empty_fails() {
        let expr = Expr::Literal(Literal::String("''".to_string()));
        assert!(extract_path_from_expr(&expr).is_err());
    }
}
