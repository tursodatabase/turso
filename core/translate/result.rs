//! Frozen metadata for columns returned by a prepared statement.

use super::semantic::hir::{self, HirDocument};
use crate::{schema::Type, vdbe::affinity::Affinity};

#[derive(Debug, Clone)]
pub struct ResultSetColumn {
    pub name: String,
    pub origin: Option<ResultColumnOrigin>,
    pub type_fact: hir::TypeFact,
    pub affinity: Affinity,
}

#[derive(Debug, Clone)]
pub struct ResultColumnOrigin {
    pub table_name: String,
    pub column_name: String,
}

impl ResultSetColumn {
    pub fn from_hir(output: &hir::Output, document: &HirDocument) -> Self {
        Self {
            name: output.name.clone(),
            origin: expression_origin(&output.expr, document),
            type_fact: output.type_fact.clone(),
            affinity: output.affinity,
        }
    }

    pub fn pragma(name: String) -> Self {
        Self {
            name,
            origin: None,
            type_fact: hir::TypeFact::dynamic(),
            affinity: Affinity::Blob,
        }
    }

    pub fn storage_type_name(&self) -> Option<&'static str> {
        type_name(self.type_fact.storage?)
    }

    pub fn affinity_name(&self) -> Option<&'static str> {
        match self.affinity {
            Affinity::Integer => Some("INTEGER"),
            Affinity::Real => Some("REAL"),
            Affinity::Text => Some("TEXT"),
            Affinity::Numeric => Some("NUMERIC"),
            Affinity::Blob => None,
        }
    }
}

fn expression_origin(expression: &hir::Expr, document: &HirDocument) -> Option<ResultColumnOrigin> {
    let column = match expression {
        hir::Expr::Column(column) => *column,
        hir::Expr::MergedColumn(column) => match column.value {
            hir::MergedColumnValue::Left => return expression_origin(&column.left, document),
            hir::MergedColumnValue::Right => column.right,
            hir::MergedColumnValue::Coalesce => return None,
        },
        _ => return None,
    };
    let source = document.source(column.source)?;
    let table = match &source.kind {
        hir::SourceKind::Table(table)
        | hir::SourceKind::TableFunction { table, .. }
        | hir::SourceKind::Pseudo { table, .. } => table,
        hir::SourceKind::SchemaExpression
        | hir::SourceKind::Cte(_)
        | hir::SourceKind::Derived(_)
        | hir::SourceKind::RecursiveInput(_) => return None,
    };
    Some(ResultColumnOrigin {
        table_name: table.value().get_name().to_string(),
        column_name: source.columns.get(column.column)?.name.clone(),
    })
}

fn type_name(storage: Type) -> Option<&'static str> {
    match storage {
        Type::Integer => Some("INTEGER"),
        Type::Real => Some("REAL"),
        Type::Text => Some("TEXT"),
        Type::Blob => Some("BLOB"),
        Type::Numeric => Some("NUMERIC"),
        Type::Null => None,
    }
}
