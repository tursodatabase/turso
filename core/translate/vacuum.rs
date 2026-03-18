//! Translation of VACUUM statements to VDBE bytecode.

use crate::vdbe::builder::ProgramBuilder;
use crate::vdbe::insn::Insn;
use crate::{bail_parse_error, Result};
use turso_parser::ast::{Expr, Literal, Name};

/// Translate a VACUUM statement into VDBE bytecode.
///
/// Supports both plain VACUUM (in-place compaction) and VACUUM INTO (copy to destination).
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
            let dest_reg = program.alloc_register();
            match dest_expr {
                // VACUUM INTO ?1 / @name
                Expr::Variable(variable) => {
                    let index = usize::try_from(variable.index.get())
                        .expect("u32 variable index must fit into usize")
                        .try_into()
                        .expect("variable index must be non-zero");
                    if let Some(name) = variable.name.as_deref() {
                        program.parameters.push_named_at(name, index);
                    } else {
                        program.parameters.push_index(index);
                    }
                    program.emit_insn(Insn::Variable {
                        index,
                        dest: dest_reg,
                    });
                }
                // VACUUM INTO 'path' / VACUUM INTO foo
                _ => {
                    let dest_path = extract_path_from_expr(dest_expr)?;
                    program.emit_string8(dest_path, dest_reg);
                }
            }
            program.emit_insn(Insn::Vacuum {
                dest_path_reg: Some(dest_reg),
            });
            Ok(())
        }
        None => {
            // Plain VACUUM - compact database in-place
            program.emit_insn(Insn::Vacuum {
                dest_path_reg: None,
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
            bail_parse_error!(
                "VACUUM INTO requires a string literal path, identifier, or bound parameter"
            );
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
