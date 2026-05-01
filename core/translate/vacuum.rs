//! Translation of VACUUM statements to VDBE bytecode.

use crate::vdbe::builder::ProgramBuilder;
use crate::vdbe::insn::Insn;
use crate::{bail_parse_error, Connection, Result};
use crate::{sync::Arc, LimboError};
use turso_parser::ast::{Expr, Literal, Name};

/// Translate a VACUUM statement into VDBE bytecode.
///
/// We have `VACUUM INTO`. The in-place `VACUUM` is experimental and gated
/// behind the experimental vacuum feature flag.
///
/// # Arguments
/// * `program` - The program builder to emit instructions to
/// * `schema_name` - Optional schema/database name to vacuum (defaults to "main" if None)
/// * `into` - Optional destination path for VACUUM INTO
///
/// # Returns
/// The modified program builder on success
pub fn translate_vacuum(
    program: &mut ProgramBuilder,
    schema_name: Option<&Name>,
    into: Option<&Expr>,
    connection: Arc<Connection>,
) -> Result<()> {
    let schema_name = schema_name.map_or_else(|| "main".to_string(), |n| n.as_str().to_string());
    match into {
        Some(dest_expr) => {
            // VACUUM INTO 'path' - create compacted copy at destination
            let dest_path = extract_path_from_expr(dest_expr)?;
            program.emit_insn(Insn::VacuumInto {
                schema_name,
                dest_path,
            });
            Ok(())
        }
        None => {
            if !connection.experimental_vacuum_enabled() {
                return Err(LimboError::ParseError(
                    "VACUUM is an experimental feature. Enable with --experimental-vacuum flag"
                        .to_string(),
                ));
            }
            if connection.experimental_multiprocess_wal_enabled() {
                return Err(LimboError::ParseError(
                    "VACUUM is incompatible with experimental multiprocess WAL".to_string(),
                ));
            }

            // Schema-qualified VACUUM is not supported yet.
            if schema_name != "main" {
                bail_parse_error!(
                    "VACUUM is only supported for the main database; schema '{}' is not supported yet",
                    schema_name
                );
            }
            program.emit_insn(Insn::Vacuum {
                db: crate::MAIN_DB_ID,
            });
            Ok(())
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
