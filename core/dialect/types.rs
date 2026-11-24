//! What core asks a dialect about a type.
//!
//! Core does not know what a type name means. The dialect that parses a
//! statement gives each type it reads a [`DialectType`], which the AST
//! carries as an opaque handle, and core asks that type what to do.

use crate::ast::{self, TableInternalId};
use crate::schema::Schema;
use crate::vdbe::affinity::Affinity;
use crate::{Result, Value};
use std::fmt::{Debug, Display};
use std::sync::Arc;

/// A type as its dialect knows it.
pub trait DialectType: Debug + Display + Send + Sync {
    /// Gives `value` as a value of this type, as a CAST to the type does.
    /// `from` is the type of `value`, when [`Dialect::type_of`] knows it. A
    /// value does not say its type: a boolean may be stored as an integer.
    ///
    /// [`Dialect::type_of`]: crate::Dialect::type_of
    fn cast(
        &self,
        value: &Value,
        from: Option<&dyn DialectType>,
        context: CastContext,
    ) -> Result<Value>;

    /// The affinity that the comparison instructions of the VDBE give values
    /// of the type. Those instructions take an affinity, as SQLite's do, so
    /// every dialect has to give one until comparisons ask the type too.
    fn comparison_affinity(&self) -> Affinity;

    /// The type itself, so that the dialect that made it can tell which of
    /// its types it is.
    fn as_any(&self) -> &dyn std::any::Any;
}

/// Whether the statement wrote the CAST.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CastContext {
    /// The statement wrote `CAST(value AS type)`.
    Explicit,
    /// The statement wrote no CAST: the engine needs the value as the type.
    Implicit,
}

/// What only core knows about the statement an expression is in.
pub trait TypeEnv {
    /// The type of column `column` of the table `table` of the statement,
    /// as the dialect that read the table's definition gave it. None for a
    /// column declared without a type, for an array column, and for a column
    /// of a subquery or view.
    fn column_type(&self, table: TableInternalId, column: usize) -> Option<Arc<dyn DialectType>>;

    /// The schema, through which a dialect reaches its catalog.
    fn schema(&self) -> &Schema;
}

/// The handle a [`ast::TypeResolver`] gives for `ty`.
pub fn type_handle(ty: Arc<dyn DialectType>) -> ast::TypeHandle {
    ast::TypeHandle(Arc::new(ty))
}

/// The type behind a handle.
pub fn handle_type(handle: &ast::TypeHandle) -> &Arc<dyn DialectType> {
    let any: &(dyn std::any::Any + Send + Sync) = &*handle.0;
    any.downcast_ref::<Arc<dyn DialectType>>()
        .expect("the handle of a type was not made by type_handle")
}

/// The type the dialect gave `type_name` when it parsed it.
pub(crate) fn dialect_type(type_name: &ast::Type) -> &Arc<dyn DialectType> {
    let Some(handle) = &type_name.resolved else {
        panic!("no dialect has said what the type {} is", type_name.name);
    };
    handle_type(handle)
}

/// The handle of a type that core writes itself for a value it knows the
/// affinity of, such as the integer that subtracting two dates gives.
pub(crate) fn type_of_affinity(affinity: Affinity) -> ast::TypeHandle {
    type_handle(core_type(affinity))
}

/// The type core casts to where no dialect named one.
pub(crate) fn core_type(affinity: Affinity) -> Arc<dyn DialectType> {
    Arc::new(TypeOfAffinity(affinity))
}

#[derive(Debug)]
struct TypeOfAffinity(Affinity);

impl DialectType for TypeOfAffinity {
    fn cast(
        &self,
        value: &Value,
        _from: Option<&dyn DialectType>,
        _context: CastContext,
    ) -> Result<Value> {
        Ok(value.exec_cast(self.0)?)
    }

    fn comparison_affinity(&self) -> Affinity {
        self.0
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl Display for TypeOfAffinity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}
