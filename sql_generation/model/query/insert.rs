use std::{fmt::Display, panic};

use serde::{Deserialize, Serialize};

use turso_parser::ast::{
    InsertBody, Name, QualifiedName, ResolveType, ResultColumn, With,
};

use crate::model::table::SimValue;

use super::select::Select;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Insert {
    Values {
        table: String,
        values: Vec<Vec<SimValue>>,
    },
    Select {
        table: String,
        select: Box<Select>,
    },
}

impl
    From<(
        Option<With>,
        Option<ResolveType>,
        QualifiedName,
        Vec<Name>,
        InsertBody,
        Vec<ResultColumn>,
    )> for Insert
{
    fn from(
        (with, or_conflict, tbl_name, columns, body, returning): (
            Option<With>,
            Option<ResolveType>,
            QualifiedName,
            Vec<Name>,
            InsertBody,
            Vec<ResultColumn>,
        ),
    ) -> Self {
        if with.is_some() {
            panic!("WITH clause in INSERT not supported");
        }
        if or_conflict.is_some() {
            panic!("OR CONFLICT clause in INSERT not supported");
        }

        if !returning.is_empty() {
            panic!("RETURNING clause in INSERT not supported");
        }

        if columns.len() > 0 {
            panic!("Specifying column names in INSERT not supported");
        }

        match body {
            InsertBody::Select(select, upsert) => {
                if upsert.is_some() {
                    panic!("UPSERT in INSERT SELECT not supported");
                }
                match &select.body.select {
                    turso_parser::ast::OneSelect::Select { .. } => Insert::Select {
                        table: tbl_name.to_string(),
                        select: Box::new(select.into()),
                    },
                    turso_parser::ast::OneSelect::Values(items) => {
                        let mut values = Vec::new();
                        for item in items {
                            let row = item
                                .into_iter()
                                .map(|expr| SimValue::from(*expr.clone()))
                                .collect();
                            values.push(row);
                        }
                        Insert::Values {
                            table: tbl_name.to_string(),
                            values,
                        }
                    }
                }
            }
            InsertBody::DefaultValues => panic!("DEFAULT VALUES in INSERT not supported"),
        }
    }
}

impl Insert {
    pub fn table(&self) -> &str {
        match self {
            Insert::Values { table, .. } | Insert::Select { table, .. } => table,
        }
    }
}

impl Display for Insert {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Insert::Values { table, values } => {
                write!(f, "INSERT INTO {table} VALUES ")?;
                for (i, row) in values.iter().enumerate() {
                    if i != 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "(")?;
                    for (j, value) in row.iter().enumerate() {
                        if j != 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{value}")?;
                    }
                    write!(f, ")")?;
                }
                Ok(())
            }
            Insert::Select { table, select } => {
                write!(f, "INSERT INTO {table} ")?;
                write!(f, "{select}")
            }
        }
    }
}
