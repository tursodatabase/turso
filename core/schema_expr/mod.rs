//! Stored expressions owned by schema objects.
//!
//! A valid expression records table columns by position. Parser syntax is kept
//! only when schema loading deliberately preserves an expression that could not
//! be resolved.

mod render;
mod resolve;
mod rewrite;

pub(crate) use rewrite::{rename_schema_expr_identifiers, validate_column_rename_using_clause};

use crate::function::Func;
use crate::schema::TypeDef;
use crate::sync::Arc;
use crate::translate::collate::CollationSeq;
use crate::util::normalize_ident;
use crate::{LimboError, Result};
use turso_parser::ast;

/// Rules used while resolving an expression that will be stored in the schema.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SchemaExprProfile {
    /// A column or domain default. References to table columns are forbidden.
    Default,
    /// A table CHECK constraint.
    Check { strict_types: bool },
    /// A generated-column expression.
    GeneratedColumn,
    /// One expression-valued CREATE INDEX key.
    IndexKey,
    /// A partial-index WHERE predicate.
    PartialIndexPredicate,
    /// A domain CHECK constraint whose `value` name is a semantic input.
    DomainCheck,
    /// An ENCODE or DECODE template whose named inputs are positional type
    /// parameters rather than table columns.
    TypeTransform,
}

impl SchemaExprProfile {
    pub(crate) const fn description(self) -> &'static str {
        match self {
            Self::Default => "default values",
            Self::Check { .. } => "CHECK constraints",
            Self::GeneratedColumn => "generated columns",
            Self::IndexKey => "index expressions",
            Self::PartialIndexPredicate => "partial index predicates",
            Self::DomainCheck => "domain CHECK expressions",
            Self::TypeTransform => "type transform expressions",
        }
    }

    pub(crate) const fn allows_table_columns(self) -> bool {
        matches!(
            self,
            Self::Check { .. }
                | Self::GeneratedColumn
                | Self::IndexKey
                | Self::PartialIndexPredicate
        )
    }

    pub(crate) const fn allows_rowid(self) -> bool {
        matches!(self, Self::Check { .. } | Self::PartialIndexPredicate)
    }

    pub(crate) const fn rejects_current_time_literals(self) -> bool {
        matches!(
            self,
            Self::GeneratedColumn
                | Self::IndexKey
                | Self::PartialIndexPredicate
                | Self::TypeTransform
        )
    }

    pub(crate) const fn requires_deterministic_function_calls(self) -> bool {
        matches!(
            self,
            Self::GeneratedColumn
                | Self::IndexKey
                | Self::PartialIndexPredicate
                | Self::DomainCheck
                | Self::TypeTransform
        )
    }
}

/// Whether a semantic failure should be returned or preserved for schema repair.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ResolutionMode {
    Strict,
    PreserveUnresolved,
}

/// A table column visible to a stored expression.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SchemaColumn {
    name: String,
    is_rowid_alias: bool,
    declared_type: Option<String>,
}

impl SchemaColumn {
    pub(crate) fn new(
        name: impl Into<String>,
        is_rowid_alias: bool,
        declared_type: Option<String>,
    ) -> Self {
        Self {
            name: name.into(),
            is_rowid_alias,
            declared_type,
        }
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) const fn is_rowid_alias(&self) -> bool {
        self.is_rowid_alias
    }

    pub(crate) fn declared_type(&self) -> Option<&str> {
        self.declared_type.as_deref()
    }
}

/// Column facts needed to resolve a stored expression.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SchemaTable {
    name: String,
    columns: Vec<SchemaColumn>,
    has_rowid: bool,
}

/// One named input visible inside an ENCODE or DECODE template.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SchemaTypeParameter {
    name: String,
    declared_type: Option<String>,
}

impl SchemaTypeParameter {
    pub(crate) fn new(name: impl Into<String>, declared_type: Option<String>) -> Self {
        Self {
            name: name.into(),
            declared_type,
        }
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn declared_type(&self) -> Option<&str> {
        self.declared_type.as_deref()
    }
}

/// Semantic inputs available while resolving one stored expression.
///
/// `expected_type` is the declared destination type. Most expressions do not
/// need it, but constructors such as `union_value()` cannot determine their
/// union identity from their arguments alone.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct SchemaExprContext<'a> {
    table: Option<&'a SchemaTable>,
    expected_type: Option<&'a str>,
    type_parameters: Option<&'a [SchemaTypeParameter]>,
    default_column_name: Option<&'a str>,
}

impl<'a> SchemaExprContext<'a> {
    pub(crate) const fn new(table: Option<&'a SchemaTable>) -> Self {
        Self {
            table,
            expected_type: None,
            type_parameters: None,
            default_column_name: None,
        }
    }

    pub(crate) const fn for_table(table: &'a SchemaTable) -> Self {
        Self::new(Some(table))
    }

    pub(crate) const fn without_table() -> Self {
        Self::new(None)
    }

    pub(crate) const fn with_expected_type(mut self, expected_type: Option<&'a str>) -> Self {
        self.expected_type = expected_type;
        self
    }

    pub(crate) const fn with_type_parameters(
        mut self,
        type_parameters: &'a [SchemaTypeParameter],
    ) -> Self {
        self.type_parameters = Some(type_parameters);
        self
    }

    pub(crate) const fn with_default_column_name(
        mut self,
        default_column_name: Option<&'a str>,
    ) -> Self {
        self.default_column_name = default_column_name;
        self
    }

    pub(crate) const fn table(self) -> Option<&'a SchemaTable> {
        self.table
    }

    pub(crate) const fn expected_type(self) -> Option<&'a str> {
        self.expected_type
    }

    pub(crate) const fn type_parameters(self) -> Option<&'a [SchemaTypeParameter]> {
        self.type_parameters
    }

    pub(crate) const fn default_column_name(self) -> Option<&'a str> {
        self.default_column_name
    }
}

impl SchemaTable {
    pub(crate) fn new(
        name: impl Into<String>,
        columns: Vec<SchemaColumn>,
        has_rowid: bool,
    ) -> Self {
        Self {
            name: name.into(),
            columns,
            has_rowid,
        }
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn columns(&self) -> &[SchemaColumn] {
        &self.columns
    }

    pub(crate) const fn has_rowid(&self) -> bool {
        self.has_rowid
    }

    pub(crate) fn column(&self, position: usize) -> Option<&SchemaColumn> {
        self.columns.get(position)
    }

    pub(crate) fn find_column(&self, name: &str) -> Option<(usize, &SchemaColumn)> {
        let name = normalize_ident(name);
        self.columns
            .iter()
            .enumerate()
            .find(|(_, column)| normalize_ident(column.name()) == name)
    }

    pub(crate) fn is_own_name(&self, name: &str) -> bool {
        normalize_ident(name) == normalize_ident(&self.name)
    }
}

/// Type information used only for STRICT CHECK validation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SchemaValueType {
    Integer,
    Real,
    Text,
    Blob,
    Any,
    Custom(String),
}

/// Catalog lookups needed to resolve a stored expression.
///
/// The schema-expression module owns the rules. Callers only adapt their
/// catalog snapshot to these lookups.
pub(crate) trait SchemaExprResolver {
    fn resolve_function(&self, name: &str, argument_count: usize) -> Result<Option<Func>> {
        Func::resolve_function(name, argument_count)
    }

    fn resolve_collation(&self, name: &str) -> Result<CollationSeq> {
        CollationSeq::new(name)
    }

    fn resolve_type(&self, name: &str) -> Result<Option<SchemaValueType>> {
        Ok(builtin_type(name))
    }

    /// Return the shape of a custom type while resolving field access and
    /// custom-type functions. The definition is consulted during analysis but
    /// is never retained by the stored expression.
    fn resolve_custom_type(&self, _name: &str) -> Result<Option<Arc<TypeDef>>> {
        Ok(None)
    }
}

/// Resolver for schema reloads that have no connection-owned extensions.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct BuiltinSchemaExprResolver;

impl SchemaExprResolver for BuiltinSchemaExprResolver {}

fn builtin_type(name: &str) -> Option<SchemaValueType> {
    let name = normalize_ident(name);
    turso_macros::match_ignore_ascii_case!(match name.as_bytes() {
        b"INT" | b"INTEGER" => Some(SchemaValueType::Integer),
        b"REAL" | b"FLOAT" | b"DOUBLE" => Some(SchemaValueType::Real),
        b"TEXT" => Some(SchemaValueType::Text),
        b"BLOB" => Some(SchemaValueType::Blob),
        b"ANY" => Some(SchemaValueType::Any),
        _ => None,
    })
}

/// A reference to one column of the table that owns an expression.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct SelfColumn {
    position: usize,
    is_rowid_alias: bool,
}

impl SelfColumn {
    pub(crate) const fn new(position: usize, is_rowid_alias: bool) -> Self {
        Self {
            position,
            is_rowid_alias,
        }
    }

    pub(crate) const fn position(self) -> usize {
        self.position
    }

    pub(crate) const fn is_rowid_alias(self) -> bool {
        self.is_rowid_alias
    }
}

/// Resolved meaning of a struct or union field access.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SchemaFieldAccess {
    StructField { field_index: usize },
    UnionVariant { tag_index: u8 },
}

/// Schema identities retained by a resolved custom-type function call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SchemaCustomTypeFunction {
    UnionValue {
        tag_index: u8,
        tag_name: String,
        result_type: String,
    },
    UnionTag {
        tag_names: Vec<String>,
    },
    UnionExtract {
        tag_index: u8,
        tag_name: String,
        result_type: String,
        result_array_dimensions: u32,
    },
    StructExtract {
        field_index: usize,
        field_name: String,
        result_type: String,
        result_array_dimensions: u32,
    },
}

/// A cast target owned by a valid stored expression.
///
/// Parser type sizes are expressions themselves, so keeping `ast::Type` here
/// would let unresolved parser syntax leak into the valid representation.
#[derive(Clone, Debug)]
pub(crate) struct SchemaTypeName {
    name: String,
    size: Option<SchemaTypeSize>,
    array_dimensions: u32,
}

impl SchemaTypeName {
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn size(&self) -> Option<&SchemaTypeSize> {
        self.size.as_ref()
    }

    pub(crate) const fn array_dimensions(&self) -> u32 {
        self.array_dimensions
    }
}

/// Size expressions in a cast target, resolved with the same rules as the
/// expression that contains the cast.
#[derive(Clone, Debug)]
pub(crate) enum SchemaTypeSize {
    MaxSize(Box<SchemaExprNode>),
    TypeSize(Box<SchemaExprNode>, Box<SchemaExprNode>),
}

/// A valid stored expression. Every name with column meaning is represented by
/// `SelfColumn`, `SelfRowId`, or `DomainValue`.
#[derive(Clone, Debug)]
pub(crate) enum SchemaExprNode {
    Between {
        lhs: Box<Self>,
        not: bool,
        start: Box<Self>,
        end: Box<Self>,
    },
    Binary(Box<Self>, ast::Operator, Box<Self>),
    Case {
        base: Option<Box<Self>>,
        when_then_pairs: Vec<(Box<Self>, Box<Self>)>,
        else_expr: Option<Box<Self>>,
    },
    Cast {
        expr: Box<Self>,
        type_name: SchemaTypeName,
        resolved_type: Option<SchemaValueType>,
    },
    Collate {
        expr: Box<Self>,
        name: ast::Name,
        collation: CollationSeq,
    },
    FieldAccess {
        base: Box<Self>,
        field: ast::Name,
        resolution: SchemaFieldAccess,
    },
    CustomTypeFunction {
        call: Box<Self>,
        resolution: SchemaCustomTypeFunction,
    },
    Function {
        name: ast::Name,
        function: Func,
        distinctness: Option<ast::Distinctness>,
        args: Vec<Self>,
        star: bool,
    },
    SelfColumn(SelfColumn),
    SelfRowId,
    DomainValue,
    TypeParameter {
        position: usize,
        name: String,
    },
    InList {
        lhs: Box<Self>,
        not: bool,
        rhs: Vec<Self>,
    },
    IsNull(Box<Self>),
    Like {
        lhs: Box<Self>,
        not: bool,
        op: ast::LikeOperator,
        rhs: Box<Self>,
        escape: Option<Box<Self>>,
    },
    Literal(ast::Literal),
    NotNull(Box<Self>),
    Parenthesized(Vec<Self>),
    Unary(ast::UnaryOperator, Box<Self>),
    Array(Vec<Self>),
    Subscript {
        base: Box<Self>,
        index: Box<Self>,
    },
    Raise {
        action: ast::ResolveType,
        message: Option<Box<Self>>,
    },
}

/// A completely resolved expression safe to compile or maintain positionally.
#[derive(Clone, Debug)]
pub(crate) struct ValidSchemaExpr {
    profile: SchemaExprProfile,
    root: SchemaExprNode,
}

impl ValidSchemaExpr {
    pub(crate) const fn profile(&self) -> SchemaExprProfile {
        self.profile
    }

    pub(crate) fn root(&self) -> &SchemaExprNode {
        &self.root
    }
}

/// Syntax retained because lenient schema loading could not resolve it.
#[derive(Clone, Debug)]
pub(crate) struct UnresolvedSchemaExpr {
    profile: SchemaExprProfile,
    syntax: Box<ast::Expr>,
    error: Option<LimboError>,
}

impl UnresolvedSchemaExpr {
    pub(crate) const fn profile(&self) -> SchemaExprProfile {
        self.profile
    }

    pub(crate) fn syntax(&self) -> &ast::Expr {
        &self.syntax
    }

    pub(crate) fn error(&self) -> Option<&LimboError> {
        self.error.as_ref()
    }
}

/// A stored schema expression in either compilable or repairable form.
#[derive(Clone, Debug)]
pub(crate) enum SchemaExpr {
    Valid(ValidSchemaExpr),
    Unresolved(UnresolvedSchemaExpr),
}

impl SchemaExpr {
    pub(crate) fn resolve(
        syntax: &ast::Expr,
        profile: SchemaExprProfile,
        context: SchemaExprContext<'_>,
        resolver: &dyn SchemaExprResolver,
        mode: ResolutionMode,
    ) -> Result<Self> {
        resolve::resolve(syntax, profile, context, resolver, mode)
    }

    pub(crate) fn preserve_unresolved(syntax: ast::Expr, profile: SchemaExprProfile) -> Self {
        Self::Unresolved(UnresolvedSchemaExpr {
            profile,
            syntax: Box::new(syntax),
            error: None,
        })
    }

    /// Retry resolution of syntax deliberately retained by a lenient schema
    /// load. A valid expression is already positional and needs no name lookup.
    pub(crate) fn resolve_unresolved(
        &mut self,
        context: SchemaExprContext<'_>,
        resolver: &dyn SchemaExprResolver,
        mode: ResolutionMode,
    ) -> Result<()> {
        let Self::Unresolved(unresolved) = self else {
            return Ok(());
        };
        let resolved = Self::resolve(
            unresolved.syntax(),
            unresolved.profile(),
            context,
            resolver,
            mode,
        )?;
        *self = resolved;
        Ok(())
    }

    pub(crate) const fn profile(&self) -> SchemaExprProfile {
        match self {
            Self::Valid(expr) => expr.profile,
            Self::Unresolved(expr) => expr.profile,
        }
    }

    pub(crate) fn as_valid(&self) -> Result<&ValidSchemaExpr> {
        match self {
            Self::Valid(expr) => Ok(expr),
            Self::Unresolved(expr) => Err(expr.error.clone().unwrap_or_else(|| {
                LimboError::ParseError(format!(
                    "unresolved stored {} cannot be compiled",
                    expr.profile.description()
                ))
            })),
        }
    }

    pub(crate) fn as_unresolved(&self) -> Option<&UnresolvedSchemaExpr> {
        match self {
            Self::Valid(_) => None,
            Self::Unresolved(expr) => Some(expr),
        }
    }
}

/// Column and pseudo-column dependencies of a valid stored expression.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct SchemaDependencies {
    columns: Vec<usize>,
    uses_rowid: bool,
    uses_domain_value: bool,
}

impl SchemaDependencies {
    pub(crate) fn columns(&self) -> &[usize] {
        &self.columns
    }

    pub(crate) const fn uses_rowid(&self) -> bool {
        self.uses_rowid
    }

    pub(crate) const fn uses_domain_value(&self) -> bool {
        self.uses_domain_value
    }
}

/// Result of remapping positions after a column is dropped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DropRemap {
    Remapped,
    UnresolvedSyntaxPreserved,
}
