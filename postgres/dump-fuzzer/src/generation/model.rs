use std::fmt::{self, Display};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Identifier(String);

impl Identifier {
    pub fn new(name: impl Into<String>) -> Self {
        let name = name.into();
        assert!(
            !name.is_empty() && name.len() <= 63 && !name.contains('\0'),
            "SQL identifiers must contain 1..=63 UTF-8 bytes and no NUL"
        );
        Self(name)
    }

    pub fn numbered(prefix: &str, index: usize) -> Self {
        Self::new(format!("{prefix} {index}"))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug)]
pub struct Table {
    pub name: Identifier,
    pub index: usize,
    pub columns: Vec<Column>,
}

#[derive(Clone, Debug)]
pub struct Column {
    pub name: Identifier,
    pub data_type: DataType,
    pub constraints: Vec<Constraint>,
}

#[derive(Clone, Copy, Debug)]
pub enum DataType {
    Integer,
    BigInt,
    Text,
    Boolean,
}

#[derive(Clone, Copy, Debug)]
pub enum Constraint {
    PrimaryKey,
    NotNull,
    Default(i32),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    Null,
    Integer(i64),
    Text(String),
    Boolean(bool),
}

#[derive(Clone, Debug)]
pub struct Row(pub Vec<Value>);

#[derive(Clone, Debug)]
pub struct IndexKey {
    pub column: Identifier,
    pub direction: SortDirection,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SortDirection {
    Ascending,
    Descending,
}

#[derive(Clone, Debug)]
pub enum Predicate {
    IsNull {
        column: Identifier,
        negated: bool,
    },
    Compare {
        column: Identifier,
        comparison: Comparison,
        value: i32,
    },
}

#[derive(Clone, Copy, Debug)]
pub enum Comparison {
    GreaterOrEqual,
    NotEqual,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Command {
    All,
    Select,
    Insert,
    Update,
    Delete,
}

#[derive(Clone, Copy, Debug)]
pub enum PolicyMode {
    Permissive,
    Restrictive,
}

#[derive(Clone, Copy, Debug)]
pub enum RlsAction {
    Enable,
    Force,
    Disable,
}

#[derive(Clone, Copy, Debug)]
pub enum PrivilegeAction {
    Grant,
    Revoke,
}

#[derive(Clone, Debug)]
pub struct Policy {
    pub name: Identifier,
    pub table: Identifier,
    pub mode: PolicyMode,
    pub command: Command,
    pub using: Option<Predicate>,
    pub check: Option<Predicate>,
}

#[derive(Clone, Debug)]
pub enum Statement {
    StandardConformingStrings,
    CreateTable(Table),
    Insert {
        table: Identifier,
        row: Row,
    },
    CreateIndex {
        name: Identifier,
        table: Identifier,
        keys: Vec<IndexKey>,
        unique: bool,
    },
    RowSecurity {
        table: Identifier,
        action: RlsAction,
    },
    CreatePolicy(Policy),
    AlterPolicy {
        name: Identifier,
        table: Identifier,
        using: Predicate,
    },
    DropPolicy {
        name: Identifier,
        table: Identifier,
    },
    Privilege {
        action: PrivilegeAction,
        command: Command,
        table: Identifier,
    },
    DefaultPrivilege {
        schema: Identifier,
        command: Command,
    },
}

#[derive(Clone, Debug)]
pub struct Workload(pub Vec<Statement>);

impl Table {
    pub fn has_primary_key(&self) -> bool {
        self.columns.iter().any(Column::is_primary_key)
    }
}

impl Column {
    pub fn is_primary_key(&self) -> bool {
        self.constraints
            .iter()
            .any(|c| matches!(c, Constraint::PrimaryKey))
    }

    pub fn is_nullable(&self) -> bool {
        !self
            .constraints
            .iter()
            .any(|c| matches!(c, Constraint::PrimaryKey | Constraint::NotNull))
    }
}

impl Display for Workload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for statement in &self.0 {
            writeln!(f, "{statement};")?;
        }
        Ok(())
    }
}

impl Display for Statement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StandardConformingStrings => f.write_str("SET standard_conforming_strings = on"),
            Self::CreateTable(table) => {
                write!(f, "CREATE TABLE {} (", table.name)?;
                separated(f, &table.columns)?;
                f.write_str(")")
            }
            Self::Insert { table, row } => {
                write!(f, "INSERT INTO {table} VALUES (")?;
                separated(f, &row.0)?;
                f.write_str(")")
            }
            Self::CreateIndex {
                name,
                table,
                keys,
                unique,
            } => {
                f.write_str(if *unique {
                    "CREATE UNIQUE INDEX "
                } else {
                    "CREATE INDEX "
                })?;
                write!(f, "{name} ON {table} (")?;
                separated(f, keys)?;
                f.write_str(")")
            }
            Self::RowSecurity { table, action } => {
                let action = match action {
                    RlsAction::Enable => "ENABLE",
                    RlsAction::Force => "FORCE",
                    RlsAction::Disable => "DISABLE",
                };
                write!(f, "ALTER TABLE {table} {action} ROW LEVEL SECURITY")
            }
            Self::CreatePolicy(policy) => {
                let mode = match policy.mode {
                    PolicyMode::Permissive => "PERMISSIVE",
                    PolicyMode::Restrictive => "RESTRICTIVE",
                };
                write!(
                    f,
                    "CREATE POLICY {} ON {} AS {mode} FOR {} TO PUBLIC",
                    policy.name, policy.table, policy.command
                )?;
                if let Some(using) = &policy.using {
                    write!(f, " USING ({using})")?;
                }
                if let Some(check) = &policy.check {
                    write!(f, " WITH CHECK ({check})")?;
                }
                Ok(())
            }
            Self::AlterPolicy { name, table, using } => {
                write!(f, "ALTER POLICY {name} ON {table} USING ({using})")
            }
            Self::DropPolicy { name, table } => write!(f, "DROP POLICY {name} ON {table}"),
            Self::Privilege {
                action,
                command,
                table,
            } => match action {
                PrivilegeAction::Grant => write!(f, "GRANT {command} ON TABLE {table} TO PUBLIC"),
                PrivilegeAction::Revoke => {
                    write!(f, "REVOKE {command} ON TABLE {table} FROM PUBLIC")
                }
            },
            Self::DefaultPrivilege { schema, command } => write!(
                f,
                "ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT {command} ON TABLES TO PUBLIC"
            ),
        }
    }
}

impl Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.name, self.data_type)?;
        for constraint in &self.constraints {
            write!(f, " {constraint}")?;
        }
        Ok(())
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Integer => "INTEGER",
            Self::BigInt => "BIGINT",
            Self::Text => "TEXT",
            Self::Boolean => "BOOLEAN",
        })
    }
}

impl Display for Constraint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PrimaryKey => f.write_str("PRIMARY KEY"),
            Self::NotNull => f.write_str("NOT NULL"),
            Self::Default(value) => write!(f, "DEFAULT {value}"),
        }
    }
}

impl Display for Identifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "\"{}\"", self.0.replace('"', "\"\""))
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => f.write_str("NULL"),
            Self::Integer(value) => write!(f, "{value}"),
            Self::Boolean(value) => write!(f, "{value}"),
            Self::Text(value) => {
                assert!(!value.contains('\0'), "PostgreSQL text cannot contain NUL");
                write!(f, "'{}'", value.replace('\'', "''"))
            }
        }
    }
}

impl Display for IndexKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {}",
            self.column,
            match self.direction {
                SortDirection::Ascending => "ASC",
                SortDirection::Descending => "DESC",
            }
        )
    }
}

impl Display for Predicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IsNull { column, negated } => {
                write!(f, "{column} IS {}NULL", if *negated { "NOT " } else { "" })
            }
            Self::Compare {
                column,
                comparison,
                value,
            } => {
                let op = match comparison {
                    Comparison::GreaterOrEqual => ">=",
                    Comparison::NotEqual => "<>",
                };
                write!(f, "{column} {op} {value}")
            }
        }
    }
}

impl Display for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::All => "ALL",
            Self::Select => "SELECT",
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
        })
    }
}

fn separated<T: Display>(f: &mut fmt::Formatter<'_>, items: &[T]) -> fmt::Result {
    for (index, item) in items.iter().enumerate() {
        if index != 0 {
            f.write_str(", ")?;
        }
        write!(f, "{item}")?;
    }
    Ok(())
}
