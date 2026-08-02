//! Owned, resolved SQL produced by semantic analysis.
//!
//! HIR contains SQL meaning, not parser names or execution resources. All IDs
//! are local to one [`HirDocument`], and all catalog objects are tied to the
//! snapshot against which the document was analyzed.

mod dependencies;
mod expr;
mod query;
mod root;
mod schema_program;
pub(crate) mod validate;

#[cfg(test)]
mod validation_properties;

use std::fmt;

use crate::{
    function::Func,
    schema::{Index, Table, Trigger, Type, TypeDef},
    sync::Arc,
    translate::collate::CollationSeq,
};

pub use expr::*;
pub use query::*;
pub use root::*;
pub use schema_program::*;

macro_rules! document_id {
    ($name:ident, $prefix:literal) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        #[repr(transparent)]
        pub struct $name(usize);

        impl $name {
            pub const fn new(index: usize) -> Self {
                Self(index)
            }

            pub const fn index(self) -> usize {
                self.0
            }
        }

        impl std::convert::From<$name> for usize {
            fn from(value: $name) -> Self {
                value.index()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!($prefix, "{}"), self.0)
            }
        }
    };
}

document_id!(QueryId, "q");
document_id!(SourceId, "s");
document_id!(CteId, "c");
document_id!(SchemaProgramId, "schema_program");

/// Identifies a block within a query.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QueryBlockId {
    pub query: QueryId,
    pub index: usize,
}

impl QueryBlockId {
    pub const fn new(query: QueryId, index: usize) -> Self {
        Self { query, index }
    }
}

/// The object that owns an output expression.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum OutputOwner {
    QueryBlock(QueryBlockId),
    IndexMethodPattern(IndexMethodPatternId),
    Root,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IndexMethodPatternId {
    pub source: SourceId,
    pub index: CatalogObjectId,
    pub pattern: usize,
}

/// Identifies an output without copying its expression into every reference.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OutputId {
    pub owner: OutputOwner,
    pub index: usize,
}

impl OutputId {
    pub const fn query(block: QueryBlockId, index: usize) -> Self {
        Self {
            owner: OutputOwner::QueryBlock(block),
            index,
        }
    }

    pub const fn root(index: usize) -> Self {
        Self {
            owner: OutputOwner::Root,
            index,
        }
    }

    pub const fn index_method_pattern(owner: IndexMethodPatternId, index: usize) -> Self {
        Self {
            owner: OutputOwner::IndexMethodPattern(owner),
            index,
        }
    }
}

/// Identity of the catalog snapshot used for semantic analysis.
///
/// The semantic context owns the corresponding catalog data. The token lets
/// later physical planning reject metadata from a different snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct CatalogSnapshot(u64);

impl CatalogSnapshot {
    pub(crate) const fn from_id(id: u64) -> Self {
        Self(id)
    }

    pub const fn id(self) -> u64 {
        self.0
    }
}

/// Stable identity of one object within a catalog snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct CatalogObjectId(u64);

impl CatalogObjectId {
    pub(crate) const fn new(id: u64) -> Self {
        Self(id)
    }

    pub const fn id(self) -> u64 {
        self.0
    }
}

/// Stable identity of main, temp, or an attached database.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct DatabaseId(usize);

impl DatabaseId {
    pub(crate) const fn new(index: usize) -> Self {
        Self(index)
    }

    pub const fn index(self) -> usize {
        self.0
    }
}

/// An owned catalog value together with the identity that resolved it.
#[derive(Clone, Debug)]
pub struct CatalogObject<T> {
    id: CatalogObjectId,
    snapshot: CatalogSnapshot,
    database: Option<DatabaseId>,
    value: Arc<T>,
}

impl<T> PartialEq for CatalogObject<T> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.snapshot == other.snapshot && self.database == other.database
    }
}

impl<T> Eq for CatalogObject<T> {}

impl<T> CatalogObject<T> {
    pub(crate) fn new(
        id: CatalogObjectId,
        snapshot: CatalogSnapshot,
        database: Option<DatabaseId>,
        value: Arc<T>,
    ) -> Self {
        Self {
            id,
            snapshot,
            database,
            value,
        }
    }

    pub const fn id(&self) -> CatalogObjectId {
        self.id
    }

    pub const fn snapshot(&self) -> CatalogSnapshot {
        self.snapshot
    }

    pub const fn database(&self) -> Option<DatabaseId> {
        self.database
    }

    pub fn value(&self) -> &T {
        &self.value
    }

    pub fn handle(&self) -> Arc<T> {
        self.value.clone()
    }
}

pub type ResolvedTable = CatalogObject<Table>;
pub type ResolvedIndex = CatalogObject<Index>;
pub type ResolvedFunction = CatalogObject<Func>;
pub type ResolvedCollation = CatalogObject<CollationSeq>;
pub type ResolvedType = CatalogObject<TypeDef>;
pub type ResolvedTrigger = CatalogObject<Trigger>;

/// A declared SQL type whose meaning was resolved during analysis.
#[derive(Clone, Debug, PartialEq)]
pub struct DeclaredType {
    pub name: String,
    pub storage: Type,
    /// Resolved custom/domain definitions from the declared leaf through its
    /// ancestors. Empty means a built-in declared type.
    pub custom_chain: Vec<ResolvedType>,
    pub array_dimensions: u32,
}

impl DeclaredType {
    pub fn custom(&self) -> Option<&ResolvedType> {
        self.custom_chain.first()
    }
}

/// Type information semantic analysis can state without guessing.
///
/// `storage` may be known even when there was no declared type, for example
/// for a literal. Array rank is kept separately from `declared` because array
/// literals and computed arrays have no SQL declaration to carry it. A
/// positive rank means the expression may return an array and records the
/// deepest bounded result seen during analysis. `array_rank_unbounded` keeps
/// that fact sound when a dynamic value or recursive cycle can add an unknown
/// number of dimensions. These are representation facts, not a promise that
/// every runtime path returns an array.
#[derive(Clone, Debug, PartialEq)]
pub struct TypeFact {
    pub storage: Option<Type>,
    pub declared: Option<DeclaredType>,
    pub array_dimensions: u32,
    pub array_rank_unbounded: bool,
}

impl TypeFact {
    pub const fn dynamic() -> Self {
        Self {
            storage: None,
            declared: None,
            array_dimensions: 0,
            array_rank_unbounded: false,
        }
    }

    pub const fn known(storage: Type) -> Self {
        Self {
            storage: Some(storage),
            declared: None,
            array_dimensions: 0,
            array_rank_unbounded: false,
        }
    }

    pub const fn known_array(array_dimensions: u32) -> Self {
        assert!(
            array_dimensions > 0,
            "an array type must have a positive rank"
        );
        Self {
            storage: Some(Type::Blob),
            declared: None,
            array_dimensions,
            array_rank_unbounded: false,
        }
    }

    /// An ARRAY constructor adds one outer dimension to its deepest known
    /// element. Empty constructors are therefore rank one. The runtime accepts
    /// ragged constructors, so mixed element ranks keep the maximum nesting
    /// instead of losing the fact that every result is still an array.
    pub fn array_literal_result(element_facts: impl IntoIterator<Item = Self>) -> Self {
        let mut nested_dimensions = 0;
        let mut array_rank_unbounded = false;
        for fact in element_facts {
            nested_dimensions = nested_dimensions.max(fact.array_dimensions);
            array_rank_unbounded |= fact.array_rank_unbounded || fact.storage.is_none();
        }
        Self {
            storage: Some(Type::Blob),
            declared: None,
            array_dimensions: nested_dimensions
                .checked_add(1)
                .expect("array rank overflow during semantic expression analysis"),
            array_rank_unbounded,
        }
    }

    pub fn declared(declared: DeclaredType) -> Self {
        Self {
            storage: Some(declared.storage),
            array_dimensions: declared.array_dimensions,
            array_rank_unbounded: false,
            declared: Some(declared),
        }
    }

    pub const fn is_array(&self) -> bool {
        self.array_dimensions > 0 || self.array_rank_unbounded
    }

    /// Merge expressions whose runtime value is selected from one argument,
    /// such as CASE, COALESCE, and scalar MIN/MAX.
    pub fn selected_value_result<'a>(facts: impl IntoIterator<Item = &'a Self>) -> Self {
        let mut scalar: Option<Self> = None;
        let mut scalar_conflict = false;
        let mut array: Option<Self> = None;
        let mut array_declaration_conflict = false;
        let mut saw_dynamic = false;
        let mut saw_argument = false;
        for fact in facts {
            saw_argument = true;
            if fact.storage == Some(Type::Null) {
                continue;
            }
            let Some(storage) = fact.storage else {
                saw_dynamic = true;
                continue;
            };

            if fact.is_array() {
                array = Some(match array {
                    None => fact.clone(),
                    Some(current) => {
                        let array_dimensions = current.array_dimensions.max(fact.array_dimensions);
                        let declared = if array_declaration_conflict {
                            None
                        } else {
                            let declared = Self::compatible_array_declaration(
                                &current,
                                fact,
                                array_dimensions,
                            );
                            if current.declared.is_some()
                                && fact.declared.is_some()
                                && declared.is_none()
                            {
                                array_declaration_conflict = true;
                            }
                            declared
                        };
                        Self {
                            storage: Some(Type::Blob),
                            declared,
                            array_dimensions,
                            array_rank_unbounded: current.array_rank_unbounded
                                || fact.array_rank_unbounded,
                        }
                    }
                });
                continue;
            }

            scalar = Some(match scalar {
                None => fact.clone(),
                Some(current) if current == *fact => current,
                Some(current) if current.storage == Some(storage) => Self {
                    storage: Some(storage),
                    declared: None,
                    array_dimensions: 0,
                    array_rank_unbounded: false,
                },
                Some(current)
                    if matches!(
                        current.storage,
                        Some(Type::Integer) | Some(Type::Real) | Some(Type::Numeric)
                    ) && matches!(storage, Type::Integer | Type::Real | Type::Numeric) =>
                {
                    Self::known(Type::Numeric)
                }
                Some(current) => {
                    scalar_conflict = true;
                    current
                }
            });
        }

        if let Some(mut array) = array {
            if saw_dynamic || scalar.is_some() || scalar_conflict || array_declaration_conflict {
                array.declared = None;
            }
            array.array_rank_unbounded |= saw_dynamic;
            return array;
        }
        if saw_dynamic || scalar_conflict {
            return Self::dynamic();
        }
        scalar.unwrap_or_else(|| {
            if saw_argument {
                Self::known(Type::Null)
            } else {
                Self::dynamic()
            }
        })
    }

    /// Infer `||` and ARRAY_CAT results. Array concatenation keeps the highest
    /// input rank and retains a compatible declared array element type.
    pub fn concat_result(lhs: &Self, rhs: &Self) -> Self {
        let array_dimensions = lhs.array_dimensions.max(rhs.array_dimensions);
        if !lhs.is_array() && !rhs.is_array() {
            return Self::known(Type::Text);
        }
        let array_rank_unbounded = lhs.array_rank_unbounded
            || rhs.array_rank_unbounded
            || (lhs.storage.is_none() || rhs.storage.is_none());
        Self::array_concat_result_with_rank(lhs, rhs, array_dimensions.max(1), array_rank_unbounded)
    }

    /// Infer ARRAY_CAT, whose result is an array even when its arguments have
    /// dynamic type facts.
    pub fn array_concat_result(lhs: &Self, rhs: &Self) -> Self {
        let array_dimensions = lhs.array_dimensions.max(rhs.array_dimensions).max(1);
        let array_rank_unbounded = lhs.array_rank_unbounded
            || rhs.array_rank_unbounded
            || lhs.storage.is_none()
            || rhs.storage.is_none();
        Self::array_concat_result_with_rank(lhs, rhs, array_dimensions, array_rank_unbounded)
    }

    /// Infer an array mutator that inserts one value into a container. An
    /// inserted rank-N array becomes an element at rank N + 1, so append,
    /// prepend, and replacement can deepen a ragged runtime array.
    pub fn array_with_element_result(container: &Self, element: &Self) -> Self {
        let element_dimensions = element
            .array_dimensions
            .checked_add(1)
            .expect("array rank overflow during semantic function analysis");
        let array_dimensions = container.array_dimensions.max(element_dimensions).max(1);
        let array_rank_unbounded = container.array_rank_unbounded
            || element.array_rank_unbounded
            || container.storage.is_none()
            || element.storage.is_none();
        let mut result = Self::array_concat_result_with_rank(
            container,
            element,
            array_dimensions,
            array_rank_unbounded,
        );
        if array_rank_unbounded
            || (container.array_dimensions > 0 && array_dimensions > container.array_dimensions)
        {
            result.declared = None;
        }
        result
    }

    fn array_concat_result_with_rank(
        lhs: &Self,
        rhs: &Self,
        array_dimensions: u32,
        array_rank_unbounded: bool,
    ) -> Self {
        Self {
            storage: Some(Type::Blob),
            declared: if array_rank_unbounded {
                None
            } else {
                Self::compatible_array_declaration(lhs, rhs, array_dimensions)
            },
            array_dimensions,
            array_rank_unbounded,
        }
    }

    fn compatible_array_declaration(
        lhs: &Self,
        rhs: &Self,
        array_dimensions: u32,
    ) -> Option<DeclaredType> {
        match (
            lhs.declared.as_ref().filter(|_| lhs.is_array()),
            rhs.declared.as_ref().filter(|_| rhs.is_array()),
        ) {
            (Some(lhs), Some(rhs))
                if lhs.name.eq_ignore_ascii_case(&rhs.name)
                    && lhs.custom_chain == rhs.custom_chain =>
            {
                let mut declared = lhs.clone();
                declared.storage = Type::Blob;
                declared.array_dimensions = array_dimensions;
                Some(declared)
            }
            (Some(declared), None) if !rhs.is_array() => {
                let mut declared = declared.clone();
                declared.storage = Type::Blob;
                declared.array_dimensions = array_dimensions;
                Some(declared)
            }
            (None, Some(declared)) if !lhs.is_array() => {
                let mut declared = declared.clone();
                declared.storage = Type::Blob;
                declared.array_dimensions = array_dimensions;
                Some(declared)
            }
            _ => None,
        }
    }

    pub fn arithmetic_result(lhs: &Self, rhs: &Self) -> Self {
        match (lhs.storage, rhs.storage) {
            (Some(Type::Integer), Some(Type::Integer)) => Self::known(Type::Integer),
            (Some(Type::Real), _) | (_, Some(Type::Real)) => Self::known(Type::Real),
            _ => Self::known(Type::Numeric),
        }
    }
}

impl Default for TypeFact {
    fn default() -> Self {
        Self::dynamic()
    }
}

/// One complete semantic-analysis result.
#[derive(Clone, Debug)]
pub struct HirDocument {
    pub snapshot: CatalogSnapshot,
    pub databases: Vec<DatabaseSnapshot>,
    pub root: HirRoot,
    pub queries: Vec<Query>,
    pub sources: Vec<Source>,
    pub ctes: Vec<Cte>,
    pub schema_programs: Vec<BoundSchemaProgram>,
    pub cdc: Option<CdcPlan>,
}

/// One database schema frozen into the semantic-analysis snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DatabaseSnapshot {
    pub database: DatabaseId,
    pub schema_version: u32,
}

impl HirDocument {
    pub fn query(&self, id: QueryId) -> Option<&Query> {
        self.queries.get(id.index()).filter(|query| query.id == id)
    }

    pub fn query_block(&self, id: QueryBlockId) -> Option<&QueryBlock> {
        self.query(id.query)?
            .blocks
            .get(id.index)
            .filter(|block| block.id == id)
    }

    pub fn source(&self, id: SourceId) -> Option<&Source> {
        self.sources
            .get(id.index())
            .filter(|source| source.id == id)
    }

    pub fn output(&self, id: OutputId) -> Option<&Output> {
        match id.owner {
            OutputOwner::QueryBlock(block) => self
                .query_block(block)?
                .outputs
                .get(id.index)
                .filter(|output| output.id == id),
            OutputOwner::IndexMethodPattern(pattern) => self
                .source(pattern.source)?
                .index_method_patterns
                .get(pattern.pattern)?
                .outputs
                .get(id.index)
                .filter(|output| output.id == id),
            OutputOwner::Root => match &self.root {
                HirRoot::Insert(insert) => insert
                    .returning
                    .as_ref()?
                    .outputs
                    .get(id.index)
                    .filter(|output| output.id == id),
                HirRoot::Update(update) => update
                    .returning
                    .as_ref()?
                    .outputs
                    .get(id.index)
                    .filter(|output| output.id == id),
                HirRoot::Delete(delete) => delete
                    .returning
                    .as_ref()?
                    .outputs
                    .get(id.index)
                    .filter(|output| output.id == id),
                HirRoot::Query(_)
                | HirRoot::TriggerPredicate(_)
                | HirRoot::SchemaExpressions(_) => None,
            },
        }
    }

    pub fn cte(&self, id: CteId) -> Option<&Cte> {
        self.ctes.get(id.index()).filter(|cte| cte.id == id)
    }

    pub fn schema_program(&self, id: SchemaProgramId) -> Option<&BoundSchemaProgram> {
        self.schema_programs.get(id.index())
    }
}
