use std::collections::HashSet;
use std::path::Path;

use anyhow::{bail, Context, Result};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Workload {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub schema: Option<Schema>,
    #[serde(default)]
    pub data: Vec<DataSpec>,
    #[serde(default)]
    pub operations: Vec<OperationSpec>,
}

#[derive(Debug, Deserialize)]
pub struct Schema {
    #[serde(default)]
    pub tables: Vec<TableDef>,
}

#[derive(Debug, Deserialize)]
pub struct TableDef {
    pub name: String,
    #[serde(default)]
    pub columns: Vec<ColumnDef>,
    #[serde(default)]
    pub primary_key: Vec<String>,
    #[serde(default)]
    pub indexes: Vec<IndexDef>,
    #[serde(default)]
    pub triggers: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    #[serde(rename = "type", default)]
    pub col_type: Option<String>,
    #[serde(default)]
    pub sql_type: Option<String>,
    #[serde(default)]
    pub primary_key: bool,
    #[serde(default)]
    pub nullable: Option<bool>,
    #[serde(default)]
    pub default: Option<serde_yaml::Value>,
    #[serde(default)]
    pub unique: bool,
    #[serde(default)]
    pub references: Option<String>,
    #[serde(default)]
    pub generated: Option<String>,
    #[serde(rename = "virtual", default)]
    pub virtual_: bool,
}

#[derive(Debug, Deserialize)]
pub struct IndexDef {
    pub name: String,
    pub columns: Vec<String>,
    #[serde(default)]
    pub unique: bool,
    #[serde(rename = "where", default)]
    pub where_: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct DataSpec {
    pub table: String,
    pub rows: RowsSpec,
    /// Per-column generator specs; insertion order is preserved.
    #[serde(default)]
    pub columns: serde_yaml::Mapping,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum RowsSpec {
    Count(u64),
    Literal(Vec<serde_yaml::Mapping>),
}

#[derive(Debug, Deserialize)]
pub struct StatementSpec {
    pub sql: String,
    #[serde(default)]
    pub bind: serde_yaml::Mapping,
}

#[derive(Debug, Deserialize)]
pub struct RequiresSpec {
    pub pool: String,
    #[serde(default = "default_min")]
    pub min: usize,
}

fn default_min() -> usize {
    1
}

#[derive(Debug, Deserialize)]
pub struct ExpectSpec {
    #[serde(default)]
    pub rows: Option<RowsExpect>,
    #[serde(default)]
    pub errors: Vec<serde_yaml::Value>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum RowsExpect {
    Exact(i64),
    Range(String),
}

#[derive(Debug, Deserialize)]
pub struct OperationSpec {
    pub name: String,
    pub weight: f64,
    #[serde(default)]
    pub sql: Option<String>,
    #[serde(default)]
    pub transaction: Option<Vec<StatementSpec>>,
    #[serde(default)]
    pub batch: Option<Vec<StatementSpec>>,
    #[serde(default)]
    pub concurrent: bool,
    #[serde(default)]
    pub bind: serde_yaml::Mapping,
    #[serde(default)]
    pub produces: Option<String>,
    #[serde(default)]
    pub requires: Option<RequiresSpec>,
    #[serde(default)]
    pub expect: Option<ExpectSpec>,
}

pub fn load_workload(path: &Path) -> Result<Workload> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("cannot read workload file {}", path.display()))?;
    let doc: Workload = serde_yaml::from_str(&text)
        .with_context(|| format!("{}: invalid workload", path.display()))?;
    if doc.name.is_empty() {
        bail!("{}: missing top-level \"name\"", path.display());
    }
    if let Some(schema) = &doc.schema {
        for table in &schema.tables {
            if table.columns.is_empty() {
                bail!("{}: table {} has no columns", path.display(), table.name);
            }
        }
    }
    for op in &doc.operations {
        if !(op.weight > 0.0) {
            bail!(
                "{}: operation {} needs a positive weight",
                path.display(),
                op.name
            );
        }
        let bodies = [
            op.sql.is_some(),
            op.transaction.is_some(),
            op.batch.is_some(),
        ]
        .iter()
        .filter(|b| **b)
        .count();
        if bodies != 1 {
            bail!(
                "{}: operation {} must have exactly one of sql | transaction | batch",
                path.display(),
                op.name
            );
        }
    }
    Ok(doc)
}

fn map_type(t: &str) -> Option<&'static str> {
    Some(match t {
        "text" => "TEXT",
        "integer" => "INTEGER",
        "real" => "REAL",
        "blob" => "BLOB",
        "boolean" => "INTEGER",
        "timestamp" => "INTEGER",
        "json" => "TEXT",
        _ => return None,
    })
}

pub fn yaml_sql_literal(v: &serde_yaml::Value) -> String {
    match v {
        serde_yaml::Value::Null => "NULL".to_string(),
        serde_yaml::Value::Bool(b) => if *b { "1" } else { "0" }.to_string(),
        serde_yaml::Value::Number(n) => n.to_string(),
        serde_yaml::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        other => format!(
            "'{}'",
            serde_yaml::to_string(other).unwrap_or_default().trim()
        ),
    }
}

fn column_sql(col: &ColumnDef, table_name: &str) -> Result<String> {
    let col_type = match (&col.sql_type, &col.col_type) {
        (Some(sql_type), _) => sql_type.clone(),
        (None, Some(t)) => map_type(t)
            .with_context(|| {
                format!(
                    "table {table_name}, column {}: unknown type \"{t}\"",
                    col.name
                )
            })?
            .to_string(),
        (None, None) => "TEXT".to_string(),
    };
    let mut parts = vec![col.name.clone(), col_type];
    if col.primary_key {
        parts.push("PRIMARY KEY".to_string());
    }
    if col.nullable == Some(false) {
        parts.push("NOT NULL".to_string());
    }
    if col.unique {
        parts.push("UNIQUE".to_string());
    }
    if let Some(default) = &col.default {
        parts.push(format!("DEFAULT {}", yaml_sql_literal(default)));
    }
    if let Some(references) = &col.references {
        match references.split_once('.') {
            Some((ref_table, ref_col)) => parts.push(format!("REFERENCES {ref_table}({ref_col})")),
            None => parts.push(format!("REFERENCES {references}")),
        }
    }
    if let Some(generated) = &col.generated {
        let kind = if col.virtual_ { "VIRTUAL" } else { "STORED" };
        parts.push(format!("GENERATED ALWAYS AS ({generated}) {kind}"));
    }
    Ok(parts.join(" "))
}

/// Generate the CREATE TABLE statements for a workload's schema.
pub fn table_statements(workload: &Workload) -> Result<Vec<String>> {
    let mut stmts = Vec::new();
    let Some(schema) = &workload.schema else {
        return Ok(stmts);
    };
    for table in &schema.tables {
        let mut defs = Vec::new();
        for col in &table.columns {
            defs.push(format!("  {}", column_sql(col, &table.name)?));
        }
        if !table.primary_key.is_empty() {
            defs.push(format!("  PRIMARY KEY ({})", table.primary_key.join(", ")));
        }
        stmts.push(format!(
            "CREATE TABLE IF NOT EXISTS {} (\n{}\n)",
            table.name,
            defs.join(",\n")
        ));
    }
    Ok(stmts)
}

/// Generate the CREATE INDEX / TRIGGER statements for a workload's schema.
pub fn post_seed_statements(workload: &Workload) -> Vec<String> {
    let mut stmts = Vec::new();
    let Some(schema) = &workload.schema else {
        return stmts;
    };
    for table in &schema.tables {
        for idx in &table.indexes {
            let unique = if idx.unique { "UNIQUE " } else { "" };
            let where_ = idx
                .where_
                .as_ref()
                .map(|w| format!(" WHERE {w}"))
                .unwrap_or_default();
            stmts.push(format!(
                "CREATE {unique}INDEX IF NOT EXISTS {} ON {} ({}){where_}",
                idx.name,
                table.name,
                idx.columns.join(", ")
            ));
        }
        for trigger in &table.triggers {
            stmts.push(trigger.clone());
        }
    }
    stmts
}

/// Generate the CREATE TABLE / INDEX / TRIGGER statements for a workload's schema.
#[cfg(test)]
pub fn ddl_statements(workload: &Workload) -> Result<Vec<String>> {
    let mut stmts = table_statements(workload)?;
    stmts.extend(post_seed_statements(workload));
    Ok(stmts)
}

fn scan_binds(binds: &serde_yaml::Mapping, pools: &mut HashSet<String>) {
    for spec in binds.values() {
        if let Some(serde_yaml::Value::String(target)) = spec.get("ref") {
            pools.insert(target.clone());
        }
    }
}

/// The set of `table.column` keys that need an entity pool: every `ref` target
/// and every `produces` target across the data and operations sections.
pub fn needed_pools(workload: &Workload) -> HashSet<String> {
    let mut pools = HashSet::new();
    for data in &workload.data {
        scan_binds(&data.columns, &mut pools);
    }
    for op in &workload.operations {
        if let Some(produces) = &op.produces {
            pools.insert(produces.clone());
        }
        scan_binds(&op.bind, &mut pools);
        for stmt in op
            .transaction
            .iter()
            .flatten()
            .chain(op.batch.iter().flatten())
        {
            scan_binds(&stmt.bind, &mut pools);
        }
        if let Some(requires) = &op.requires {
            if requires.pool.contains('.') {
                pools.insert(requires.pool.clone());
            }
        }
    }
    pools
}

/// Total seed rows a workload inserts (for progress reporting).
pub fn total_seed_rows(workload: &Workload) -> u64 {
    workload
        .data
        .iter()
        .map(|spec| match &spec.rows {
            RowsSpec::Count(n) => *n,
            RowsSpec::Literal(rows) => rows.len() as u64,
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn workload(yaml: &str) -> Workload {
        serde_yaml::from_str(yaml).unwrap()
    }

    #[test]
    fn ddl_generation() {
        let w = workload(
            r#"
name: t
schema:
  tables:
    - name: users
      columns:
        - { name: id, type: text, primary_key: true }
        - { name: age, type: integer, nullable: false, default: 0 }
        - { name: email, type: text, unique: true }
      indexes:
        - { name: users_age, columns: [age], where: "age > 0" }
    - name: items
      columns:
        - { name: a, type: text, nullable: false }
        - { name: b, type: integer, nullable: false }
        - { name: owner, type: text, references: users.id }
      primary_key: [a, b]
"#,
        );
        let stmts = ddl_statements(&w).unwrap();
        assert_eq!(stmts.len(), 3);
        assert!(stmts[0].contains("id TEXT PRIMARY KEY"));
        assert!(stmts[0].contains("age INTEGER NOT NULL DEFAULT 0"));
        assert!(stmts[0].contains("email TEXT UNIQUE"));
        assert!(stmts[1].contains("PRIMARY KEY (a, b)"));
        assert!(stmts[1].contains("owner TEXT REFERENCES users(id)"));
        assert!(stmts.iter().any(|stmt| stmt
            .contains("CREATE INDEX IF NOT EXISTS users_age ON users (age) WHERE age > 0")));
    }

    #[test]
    fn pools_from_refs_and_produces() {
        let w = workload(
            r#"
name: t
data:
  - table: child
    rows: 10
    columns:
      parent_id: { ref: parent.id }
operations:
  - name: ins
    weight: 1
    produces: child.id
    bind:
      id: { pattern: "c-{seq}" }
    sql: "INSERT INTO child (id) VALUES (:id)"
  - name: del
    weight: 1
    requires: { pool: other.key }
    bind:
      id: { ref: child.id, consume: true }
    sql: "DELETE FROM child WHERE id = :id"
"#,
        );
        let pools = needed_pools(&w);
        assert!(pools.contains("parent.id"));
        assert!(pools.contains("child.id"));
        assert!(pools.contains("other.key"));
        assert_eq!(pools.len(), 3);
    }
}
