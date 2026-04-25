//! Translation of VACUUM statements to VDBE bytecode.

use crate::util::OpenOptions;
use crate::vdbe::builder::ProgramBuilder;
use crate::vdbe::insn::Insn;
use crate::{bail_parse_error, EncryptionOpts, Result};
use turso_parser::ast::{Expr, Literal, Name};

/// Translate a VACUUM statement into VDBE bytecode.
///
/// Currently only VACUUM INTO is supported. Plain VACUUM (which compacts
/// the database in place) is not yet implemented.
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
) -> Result<()> {
    let schema_name = schema_name.map_or_else(|| "main".to_string(), |n| n.as_str().to_string());
    match into {
        Some(dest_expr) => {
            // VACUUM INTO 'path' - create compacted copy at destination
            let dest = extract_path_from_expr(dest_expr)?;
            let opts = OpenOptions::parse(dest.as_str())?;
            // Do we need to throw error if nonsense options like mode=ro are passed?
            match (opts.cipher, opts.hexkey) {
                (Some(cipher), Some(hexkey)) => program.emit_insn(Insn::VacuumInto {
                    dest_path: opts.path,
                    encryption_opts: Some(EncryptionOpts { cipher, hexkey }),
                }),
                (Some(_), None) => bail_parse_error!("hexkey is required when cipher is provided"),
                (None, Some(_)) => bail_parse_error!("cipher is required when hexkey is provided"),
                (None, None) => program.emit_insn(Insn::VacuumInto {
                    schema_name,
                    dest_path: opts.path,
                    encryption_opts: None,
                }),
            }

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
