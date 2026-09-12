//! Bound relations. Execution resources belong to the binding adapter's lowering context.

mod binding;
mod columns;
mod inspect;
mod lower;
mod rewrite;
mod scalar;

use std::collections::BTreeSet;

use turso_parser::ast::{self, TableInternalId};

use crate::schema::BTreeTable;
use crate::sync::Arc;
use crate::translate::collate::CollationSeq;
use crate::vdbe::affinity::Affinity;
use crate::{LimboError, Result};

pub(crate) use binding::{bind, BindError};
use columns::ColumnSet;
pub(crate) use inspect::inspect_plan;
pub(crate) use lower::rewrite_select;
use scalar::Scalar;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ColumnId {
    pub relation: TableInternalId,
    pub position: Option<usize>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Scope {
    Local,
    Outer(usize),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ColumnReference {
    pub column: ColumnId,
    pub scope: Scope,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Column {
    pub id: ColumnId,
    pub name: String,
    pub nullable: bool,
    pub affinity: Affinity,
    pub collation: CollationSeq,
}

#[derive(Clone, Debug)]
pub(crate) struct Binding {
    pub id: TableInternalId,
    pub name: String,
    pub columns: BindingColumns,
}

#[derive(Clone, Debug)]
pub(crate) enum BindingColumns {
    Catalog(Arc<BTreeTable>),
    Derived(Vec<Column>),
}

#[derive(Clone, Debug)]
pub(crate) struct SharedInput {
    pub id: usize,
    pub input: Relation,
    pub columns: Vec<ColumnId>,
}

#[derive(Clone, Debug)]
pub(crate) struct Output {
    pub column: Column,
    pub expr: Scalar,
    pub alias: Option<String>,
    pub implicit_name: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum JoinKind {
    Inner,
    Semi,
    Anti,
}

#[derive(Clone, Debug)]
pub(crate) enum Relation {
    OneRow,
    Scan(TableInternalId),
    SharedRef {
        binding: TableInternalId,
        input: usize,
    },
    Subquery {
        binding: TableInternalId,
        input: Box<Relation>,
        columns: Vec<ColumnId>,
    },
    Filter {
        input: Box<Relation>,
        predicates: Vec<Scalar>,
    },
    Project {
        input: Box<Relation>,
        outputs: Vec<Output>,
    },
    Join {
        left: Box<Relation>,
        right: Box<Relation>,
        kind: JoinKind,
        predicates: Vec<Scalar>,
    },
    DependentJoin {
        left: Box<Relation>,
        right: Box<Relation>,
        kind: JoinKind,
        subquery: TableInternalId,
    },
    Sort {
        input: Box<Relation>,
        keys: Vec<(Scalar, ast::SortOrder, Option<ast::NullsOrder>)>,
    },
    Limit {
        input: Box<Relation>,
        limit: Option<Box<Scalar>>,
        offset: Option<Box<Scalar>>,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct LogicalPlan {
    pub root: Relation,
    pub bindings: Vec<Binding>,
    pub shared_inputs: Vec<SharedInput>,
    pub outer_columns: Vec<ColumnId>,
    pub parameters: Vec<ast::Variable>,
}

#[derive(Default)]
pub(crate) struct Properties {
    pub outputs: ColumnSet,
    pub outer: ColumnSet,
}

impl LogicalPlan {
    pub(crate) fn validate(&self) -> Result<()> {
        let mut relations = BTreeSet::new();
        for binding in &self.bindings {
            require(relations.insert(binding.id), "duplicate relation binding")?;
            if let BindingColumns::Derived(columns) = &binding.columns {
                for (position, column) in columns.iter().enumerate() {
                    require(
                        column.id
                            == ColumnId {
                                relation: binding.id,
                                position: Some(position),
                            },
                        "derived column has the wrong identity",
                    )?;
                }
            }
        }
        let mut shared_ids = BTreeSet::new();
        for input in &self.shared_inputs {
            validate_shared_references(&input.input, &shared_ids)?;
            require(shared_ids.insert(input.id), "duplicate shared input")?;
            let properties = self.properties(&input.input)?;
            require(
                properties.outer.is_empty(),
                "shared input depends on an outer row",
            )?;
            require(
                properties.outputs == input.columns.iter().copied().collect(),
                "shared output mapping differs from its producer",
            )?;
        }
        let properties = self.properties(&self.root)?;
        require(
            properties
                .outer
                .iter()
                .all(|id| self.outer_columns.contains(id)),
            "root has an unbound outer reference",
        )
    }

    pub(crate) fn properties(&self, relation: &Relation) -> Result<Properties> {
        let properties = match relation {
            Relation::OneRow => Properties::default(),
            Relation::Scan(id) | Relation::SharedRef { binding: id, .. } => {
                let binding = self
                    .bindings
                    .iter()
                    .find(|binding| binding.id == *id)
                    .ok_or_else(|| invalid("scan references an unknown relation"))?;
                if let Relation::SharedRef { input, .. } = relation {
                    let source = self
                        .shared_inputs
                        .iter()
                        .find(|source| source.id == *input)
                        .ok_or_else(|| invalid("reference has no shared producer"))?;
                    require(
                        source.columns.len() == binding.column_count(),
                        "shared reference column count differs",
                    )?;
                }
                Properties {
                    outputs: binding.column_ids().collect(),
                    outer: ColumnSet::default(),
                }
            }
            Relation::Subquery {
                binding,
                input,
                columns,
            } => {
                let mut properties = self.properties(input)?;
                let binding = self
                    .bindings
                    .iter()
                    .find(|candidate| candidate.id == *binding)
                    .ok_or_else(|| invalid("subquery references an unknown relation"))?;
                require(
                    columns.len() == binding.column_count()
                        && columns.len() == properties.outputs.len()
                        && *columns == self.output_columns(input)?,
                    "subquery output mapping differs from its input",
                )?;
                properties.outputs = binding.column_ids().collect();
                require(
                    properties.outputs.is_disjoint(&properties.outer),
                    "subquery depends on its own output",
                )?;
                properties
            }
            Relation::Filter { input, predicates } => {
                let mut properties = self.properties(input)?;
                for expr in predicates {
                    validate_scalar(expr, &mut properties)?;
                }
                properties
            }
            Relation::Project { input, outputs } => {
                let mut properties = self.properties(input)?;
                for output in outputs {
                    validate_scalar(&output.expr, &mut properties)?;
                }
                let output_ids: ColumnSet = outputs.iter().map(|output| output.column.id).collect();
                require(
                    output_ids.len() == outputs.len(),
                    "projection repeats an output identity",
                )?;
                properties.outputs = output_ids;
                properties
            }
            Relation::Join {
                left,
                right,
                kind,
                predicates,
            } => {
                let mut left = self.properties(left)?;
                let right = self.properties(right)?;
                require(
                    left.outputs.is_disjoint(&right.outputs),
                    "join inputs share column identities",
                )?;
                require(
                    right.outer.is_disjoint(&left.outputs),
                    "ordinary join still has a dependency",
                )?;
                require(
                    left.outer.is_disjoint(&right.outputs),
                    "ordinary join has a reverse dependency",
                )?;
                let outputs = left.outputs.clone();
                left.outputs.extend(right.outputs);
                left.outer.extend(right.outer);
                for predicate in predicates {
                    validate_scalar(predicate, &mut left)?;
                }
                if *kind != JoinKind::Inner {
                    left.outputs = outputs;
                }
                left
            }
            Relation::DependentJoin {
                left, right, kind, ..
            } => {
                let mut left = self.properties(left)?;
                let right = self.properties(right)?;
                require(
                    left.outputs.is_disjoint(&right.outputs),
                    "dependent inputs share column identities",
                )?;
                require(
                    *kind != JoinKind::Inner,
                    "dependent inner join has no lowering",
                )?;
                left.outer.extend(right.outer.difference(&left.outputs));
                left
            }
            Relation::Sort { input, keys } => {
                let mut properties = self.properties(input)?;
                for (expr, _, _) in keys {
                    validate_scalar(expr, &mut properties)?;
                }
                properties
            }
            Relation::Limit {
                input,
                limit,
                offset,
            } => {
                let mut properties = self.properties(input)?;
                for expr in limit.iter().chain(offset.iter()) {
                    validate_scalar(expr, &mut properties)?;
                }
                properties
            }
        };
        Ok(properties)
    }

    fn output_columns(&self, relation: &Relation) -> Result<Vec<ColumnId>> {
        match relation {
            Relation::OneRow => Ok(Vec::new()),
            Relation::Scan(id)
            | Relation::SharedRef { binding: id, .. }
            | Relation::Subquery { binding: id, .. } => self
                .bindings
                .iter()
                .find(|binding| binding.id == *id)
                .map(|binding| binding.column_ids().collect())
                .ok_or_else(|| invalid("output references an unknown relation")),
            Relation::Project { outputs, .. } => {
                Ok(outputs.iter().map(|output| output.column.id).collect())
            }
            Relation::Filter { input, .. }
            | Relation::Sort { input, .. }
            | Relation::Limit { input, .. } => self.output_columns(input),
            Relation::Join {
                left, right, kind, ..
            } => {
                let mut outputs = self.output_columns(left)?;
                if *kind == JoinKind::Inner {
                    outputs.extend(self.output_columns(right)?);
                }
                Ok(outputs)
            }
            Relation::DependentJoin { left, .. } => self.output_columns(left),
        }
    }
}

impl Binding {
    pub(crate) fn column_ids(&self) -> impl Iterator<Item = ColumnId> + '_ {
        let (count, rowid) = match &self.columns {
            BindingColumns::Catalog(table) => (table.columns().len(), table.has_rowid),
            BindingColumns::Derived(columns) => (columns.len(), false),
        };
        (0..count)
            .map(Some)
            .chain(rowid.then_some(None))
            .map(|position| ColumnId {
                relation: self.id,
                position,
            })
    }

    pub(crate) fn column(&self, id: ColumnId) -> Column {
        match &self.columns {
            BindingColumns::Catalog(table) => match id.position {
                Some(position) => {
                    let column = &table.columns()[position];
                    Column {
                        id,
                        name: column.name.clone().unwrap_or_default(),
                        nullable: !column.notnull() && !column.is_rowid_alias(),
                        affinity: column.affinity_with_strict(table.is_strict),
                        collation: column.collation(),
                    }
                }
                None => Column {
                    id,
                    name: "rowid".to_owned(),
                    nullable: false,
                    affinity: Affinity::Integer,
                    collation: CollationSeq::Binary,
                },
            },
            BindingColumns::Derived(columns) => {
                columns[id.position.expect("derived columns have an ordinal")].clone()
            }
        }
    }

    pub(crate) fn unique_keys(&self) -> Vec<Vec<ColumnId>> {
        let BindingColumns::Catalog(table) = &self.columns else {
            return Vec::new();
        };
        let mut keys: Vec<_> = table
            .columns()
            .iter()
            .enumerate()
            .filter(|(_, column)| column.is_rowid_alias())
            .map(|(position, _)| {
                vec![ColumnId {
                    relation: self.id,
                    position: Some(position),
                }]
            })
            .collect();
        if table.has_rowid {
            keys.push(vec![ColumnId {
                relation: self.id,
                position: None,
            }]);
        }
        keys
    }

    fn column_count(&self) -> usize {
        match &self.columns {
            BindingColumns::Catalog(table) => table.columns().len() + usize::from(table.has_rowid),
            BindingColumns::Derived(columns) => columns.len(),
        }
    }
}

fn validate_shared_references(relation: &Relation, available: &BTreeSet<usize>) -> Result<()> {
    match relation {
        Relation::OneRow | Relation::Scan(_) => Ok(()),
        Relation::SharedRef { input, .. } => require(
            available.contains(input),
            "shared input has a forward or recursive reference",
        ),
        Relation::Filter { input, .. }
        | Relation::Subquery { input, .. }
        | Relation::Project { input, .. }
        | Relation::Sort { input, .. }
        | Relation::Limit { input, .. } => validate_shared_references(input, available),
        Relation::Join { left, right, .. } | Relation::DependentJoin { left, right, .. } => {
            validate_shared_references(left, available)?;
            validate_shared_references(right, available)
        }
    }
}

fn validate_scalar(expr: &Scalar, properties: &mut Properties) -> Result<()> {
    for reference in &expr.references {
        match reference.scope {
            Scope::Local => require(
                properties.outputs.contains(&reference.column),
                "scalar references a column outside its input",
            )?,
            Scope::Outer(_) => {
                require(
                    !properties.outputs.contains(&reference.column),
                    "outer scalar references a local input",
                )?;
                properties.outer.insert(reference.column);
            }
        }
    }
    Ok(())
}

fn require(condition: bool, message: &str) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(invalid(message))
    }
}

fn invalid(message: &str) -> LimboError {
    LimboError::InternalError(format!("invalid logical plan: {message}"))
}
