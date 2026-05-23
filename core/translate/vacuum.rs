//! Translation of VACUUM statements to VDBE bytecode.

use crate::util::OpenOptions;
use crate::vdbe::builder::ProgramBuilder;
use crate::vdbe::insn::Insn;
use crate::{bail_parse_error, Connection, EncryptionOpts, Result};
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
            let dest = extract_path_from_expr(dest_expr)?;
            let opts = OpenOptions::parse(dest.as_str())?;
            // TODO: Handle all URI parameters, including throwing errors for nonsense ones like
            // mode=ro
            match (opts.cipher, opts.hexkey) {
                (Some(cipher), Some(hexkey)) => program.emit_insn(Insn::VacuumInto {
                    schema_name,
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
    use crate::vdbe::builder::ProgramBuilderOpts;
    use crate::{DatabaseOpts, QueryMode};

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

    fn make_builder() -> ProgramBuilder {
        ProgramBuilder::new(
            QueryMode::Normal,
            None,
            ProgramBuilderOpts {
                num_cursors: 0,
                approx_num_insns: 4,
                approx_num_labels: 0,
            },
        )
    }

    fn quoted_string_expr(s: &str) -> Expr {
        Expr::Literal(Literal::String(format!("'{s}'")))
    }

    #[test]
    fn test_translate_vacuum_into_uri_with_cipher_and_hexkey() {
        let mut program = make_builder();
        let dest = quoted_string_expr("file:test.db?cipher=aes256&hexkey=00112233");
        let (_, connection) = Connection::from_uri(":memory:", DatabaseOpts::new())
            .expect("in-memory connection should succeed");
        translate_vacuum(&mut program, None, Some(&dest), connection).unwrap();

        let (dest_path, encryption_opts) = program
            .insns
            .iter()
            .find_map(|(insn, _)| match insn {
                Insn::VacuumInto {
                    schema_name: _,
                    dest_path,
                    encryption_opts,
                } => Some((dest_path.clone(), encryption_opts.clone())),
                _ => None,
            })
            .expect("VacuumInto instruction was not emitted");

        // The URI scheme and query string are stripped from the destination path.
        assert_eq!(dest_path, "test.db");
        let opts = encryption_opts.expect("encryption_opts should be set");
        assert_eq!(opts.cipher, "aes256");
        assert_eq!(opts.hexkey, "00112233");
    }

    #[test]
    fn test_translate_vacuum_into_uri_only_cipher_errors() {
        let mut program = make_builder();
        let dest = quoted_string_expr("file:test.db?cipher=aes256");
        let (_, connection) = Connection::from_uri(":memory:", DatabaseOpts::new())
            .expect("in-memory connection should succeed");
        let err = translate_vacuum(&mut program, None, Some(&dest), connection).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Parse error: hexkey is required when cipher is provided"
        );
    }

    #[test]
    fn test_translate_vacuum_into_uri_only_hexkey_errors() {
        let mut program = make_builder();
        let dest = quoted_string_expr("file:test.db?hexkey=00112233");
        let (_, connection) = Connection::from_uri(":memory:", DatabaseOpts::new())
            .expect("in-memory connection should succeed");
        let err = translate_vacuum(&mut program, None, Some(&dest), connection).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Parse error: cipher is required when hexkey is provided"
        );
    }

    #[test]
    fn test_translate_vacuum_into_uri_without_encryption_params() {
        let mut program = make_builder();
        let dest = quoted_string_expr("file:test.db");
        let (_, connection) = Connection::from_uri(":memory:", DatabaseOpts::new())
            .expect("in-memory connection should succeed");
        translate_vacuum(&mut program, None, Some(&dest), connection).unwrap();

        let (dest_path, encryption_opts) = program
            .insns
            .iter()
            .find_map(|(insn, _)| match insn {
                Insn::VacuumInto {
                    schema_name: _,
                    dest_path,
                    encryption_opts,
                } => Some((dest_path.clone(), encryption_opts.clone())),
                _ => None,
            })
            .expect("VacuumInto instruction was not emitted");

        assert_eq!(dest_path, "test.db");
        assert!(encryption_opts.is_none());
    }
}
