//! Translation of VACUUM statements to VDBE bytecode.

use crate::translate::semantic::context::SemanticContext;
use crate::translate::semantic_lowering::PlannedSelect;
use crate::vdbe::builder::ProgramBuilder;
use crate::vdbe::insn::Insn;
use crate::{bail_parse_error, Connection, Result, TEMP_DB_ID};
use crate::{sync::Arc, LimboError};
use turso_parser::ast::{Expr, Name};

/// Translate a VACUUM statement into VDBE bytecode.
///
/// We have `VACUUM INTO`. The in-place `VACUUM` is experimental and gated
/// behind the experimental vacuum feature flag.
///
/// # Arguments
/// * `program` - The program builder to emit instructions to
/// * `catalog` - Resolves names in the VACUUM INTO path expression
/// * `schema_name` - Optional schema/database name to vacuum (defaults to "main" if None)
/// * `into` - Optional destination path for VACUUM INTO
///
/// # Returns
/// The modified program builder on success
pub fn translate_vacuum(
    program: &mut ProgramBuilder,
    catalog: &SemanticContext<'_>,
    schema_name: Option<&Name>,
    into: Option<&Expr>,
    connection: Arc<Connection>,
) -> Result<()> {
    let schema_name = schema_name.map_or_else(|| "main".to_string(), |n| n.as_str().to_string());
    match into {
        Some(dest_expr) => {
            if connection.get_database_id_by_name(&schema_name)? == TEMP_DB_ID {
                return Ok(());
            }
            let path = PlannedSelect::values(vec![dest_expr.clone()], catalog, &connection)?;
            let dest_reg = path.emit_values(program)?;
            program.emit_insn(Insn::VacuumInto {
                schema_name,
                dest_reg,
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
