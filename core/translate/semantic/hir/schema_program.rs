//! Bound schema expressions owned by one HIR document.

use super::{Expr, SchemaProgramId, SourceId};
use crate::vdbe::affinity::Affinity;

/// One schema expression bound against a synthetic document-local source.
#[derive(Clone, Debug)]
pub struct BoundSchemaProgram {
    pub input_source: SourceId,
    pub body: Expr,
}

/// One invocation of a bound schema program.
///
/// The runtime value is supplied separately from the explicit arguments.
#[derive(Clone, Debug)]
pub struct BoundSchemaCall {
    pub program: SchemaProgramId,
    pub arguments: Vec<Expr>,
}

/// Static metadata needed to encode an array into its stored representation.
#[derive(Clone, Debug)]
pub struct BoundArrayStorage {
    pub element_affinity: Affinity,
    pub element_type: String,
    pub table_name: String,
    pub column_name: String,
    pub dimensions: u32,
}

/// Bound transformations for one source column's declared type.
#[derive(Clone, Debug)]
pub struct BoundColumnTypePrograms {
    pub encode: Vec<BoundSchemaCall>,
    pub decode: Vec<BoundSchemaCall>,
    pub array: Option<BoundArrayStorage>,
    /// Scalar NOT NULL custom types still run their encoder for NULL. Arrays
    /// always leave NULL untouched.
    pub encode_nulls: bool,
}

/// One domain check and the message emitted when it rejects a value.
#[derive(Clone, Debug)]
pub struct BoundDomainCheck {
    pub call: BoundSchemaCall,
    pub failure_description: String,
}

/// Constraints inherited from a resolved domain cast target.
#[derive(Clone, Debug)]
pub struct BoundDomainConstraints {
    pub not_null_description: Option<String>,
    pub checks: Vec<BoundDomainCheck>,
}

/// Custom-type work resolved for one cast target.
#[derive(Clone, Debug)]
pub struct BoundCastPrograms {
    pub encode: Vec<BoundSchemaCall>,
    pub domain: Option<BoundDomainConstraints>,
    /// Whether code generation must apply SQLite's ordinary CAST affinity.
    pub apply_builtin_affinity: bool,
}
