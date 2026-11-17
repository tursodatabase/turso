use crate::dump_ast;
use anyhow::Result;
use pg_query::{protobuf::ConstrType, NodeEnum as N};
use serde::Serialize;
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

#[derive(Default, Serialize)]
pub struct Summary {
    pub attempts: u64,
    pub generated: Generated,
    pub unparsed_workloads: u64,
    pub engines: BTreeMap<String, Engine>,
    pub passed: u64,
    pub failed: u64,
}

impl Summary {
    pub fn finish(
        &self,
        output: &Path,
        seed: Option<u64>,
        disabled_features: &[String],
    ) -> Result<PathBuf> {
        fs::create_dir_all(output)?;
        let file = tempfile::Builder::new()
            .prefix("summary-")
            .suffix(".json")
            .tempfile_in(output)?;
        serde_json::to_writer_pretty(
            file.as_file(),
            &serde_json::json!({
                "seed": seed, "disabled_features": disabled_features,
                "counting": "Executed workloads, including shrinking/replay; feature execution counts require a complete successful load; affected-row counts include partial loads.",
                "summary": self,
            }),
        )?;
        let (_, path) = file.keep()?;
        print!(
            "{}",
            crate::report::render(self, seed, disabled_features, &path)
        );
        Ok(path)
    }

    pub fn loaded(&mut self, engine: &str, generated: &Generated, success: bool, log: &str) {
        let engine = self.engines.entry(engine.to_owned()).or_default();
        if success {
            engine.completed.add(generated);
        } else {
            engine.failed_loads += 1;
        }
        for line in log.lines() {
            let words: Vec<_> = line.split_whitespace().collect();
            match words.as_slice() {
                ["INSERT", "0", rows] => {
                    if let Ok(rows) = rows.parse::<u64>() {
                        engine.inserted_rows += rows;
                    }
                }
                ["UPDATE", rows] => {
                    if let Ok(rows) = rows.parse::<u64>() {
                        engine.updated_rows += rows;
                    }
                }
                ["DELETE", rows] => {
                    if let Ok(rows) = rows.parse::<u64>() {
                        engine.deleted_rows += rows;
                    }
                }
                _ => {}
            }
        }
    }

    pub fn dumped(&mut self, engine: &str, sql: &str) -> Result<()> {
        let dump = Dump::parse(sql)?;
        let engine = self.engines.entry(engine.to_owned()).or_default();
        engine.dumps += 1;
        engine.dump.rows += dump.rows;
        engine.dump.empty_tables += dump.empty_tables;
        engine.dump.populated_tables += dump.populated_tables;
        engine.dump.null_cells += dump.null_cells;
        engine.dump.duplicate_rows += dump.duplicate_rows;
        Ok(())
    }
}

#[derive(Default, Serialize)]
pub struct Generated {
    pub workloads: u64,
    pub tables: u64,
    pub empty_schemas: u64,
    pub empty_tables: u64,
    pub populated_tables: u64,
    pub values_rows: u64,
    pub null_cells: u64,
    pub features: BTreeMap<String, u64>,
}

impl Generated {
    pub fn parse(sql: &str) -> Result<Self> {
        let mut result = Self {
            workloads: 1,
            features: schema_features()
                .into_iter()
                .map(|name| (name, 0))
                .collect(),
            ..Self::default()
        };
        let mut tables = BTreeMap::new();
        for statement in pg_query::parse(sql)?.protobuf.stmts {
            let Some(node) = statement.stmt.and_then(|s| s.node) else {
                continue;
            };
            let feature = match node {
                N::CreateStmt(create) => {
                    let columns = create
                        .table_elts
                        .iter()
                        .filter(|node| matches!(node.node, Some(N::ColumnDef(_))))
                        .count();
                    result.feature(&format!("table.columns.{columns}"));
                    if let Some(table) = create.relation {
                        tables
                            .entry((table.schemaname, table.relname))
                            .or_insert(0u64);
                    }
                    for column in create.table_elts {
                        if let Some(N::ColumnDef(column)) = column.node {
                            result.column(&column);
                            for constraint in column.constraints {
                                if let Some(N::Constraint(constraint)) = constraint.node {
                                    let name = match ConstrType::try_from(constraint.contype)? {
                                        ConstrType::ConstrDefault => Some("column_defaults"),
                                        ConstrType::ConstrNotnull => Some("not_null"),
                                        ConstrType::ConstrPrimary => Some("primary_keys"),
                                        _ => None,
                                    };
                                    if let Some(name) = name {
                                        result.feature(name);
                                    }
                                }
                            }
                        }
                    }
                    "create_table"
                }
                N::InsertStmt(insert) => {
                    if let Some(N::SelectStmt(select)) = insert.select_stmt.and_then(|s| s.node) {
                        if select.values_lists.is_empty() {
                            "insert_select"
                        } else {
                            let count = select.values_lists.len() as u64;
                            result.values_rows += count;
                            if let Some(table) = insert.relation {
                                if let Some(rows) =
                                    tables.get_mut(&(table.schemaname, table.relname))
                                {
                                    *rows += count;
                                }
                            }
                            for row in select.values_lists {
                                if let Some(N::List(row)) = row.node {
                                    result.null_cells += row.items.iter().filter(|cell| matches!(&cell.node, Some(N::AConst(value)) if value.isnull)).count() as u64;
                                }
                            }
                            "insert_values"
                        }
                    } else {
                        "insert_other"
                    }
                }
                N::UpdateStmt(_) => "update",
                N::DeleteStmt(_) => "delete",
                N::IndexStmt(index) => {
                    result.feature(if index.unique {
                        "index.unique"
                    } else {
                        "index.non_unique"
                    });
                    result.feature(&format!("index.method.{}", index.access_method));
                    result.feature(if index.index_params.len() == 1 {
                        "index.single_column"
                    } else {
                        "index.multicolumn"
                    });
                    for node in &index.index_params {
                        if let Some(N::IndexElem(key)) = &node.node {
                            result.feature(
                                if key.ordering == pg_query::protobuf::SortByDir::SortbyDesc as i32
                                {
                                    "index.key.desc"
                                } else {
                                    "index.key.asc"
                                },
                            );
                        }
                    }
                    "create_index"
                }
                N::CreatePolicyStmt(policy) => {
                    result.feature(if policy.permissive {
                        "policy.permissive"
                    } else {
                        "policy.restrictive"
                    });
                    result.feature(&format!(
                        "policy.command.{}",
                        policy.cmd_name.to_lowercase()
                    ));
                    if policy.qual.is_some() {
                        result.feature("policy.using");
                    }
                    if policy.with_check.is_some() {
                        result.feature("policy.with_check");
                    }
                    "create_policy"
                }
                N::AlterPolicyStmt(_) => "alter_policy",
                N::DropStmt(drop)
                    if drop.remove_type == pg_query::protobuf::ObjectType::ObjectPolicy as i32 =>
                {
                    "drop_policy"
                }
                N::GrantStmt(grant) => {
                    result.privileges(if grant.is_grant { "grant" } else { "revoke" }, &grant);
                    if grant.is_grant {
                        "grant"
                    } else {
                        "revoke"
                    }
                }
                N::AlterDefaultPrivilegesStmt(defaults) => {
                    if let Some(grant) = defaults.action {
                        result.privileges("default_privileges", &grant);
                    }
                    "default_privileges"
                }
                N::AlterTableStmt(alter) => {
                    for command in alter.cmds {
                        if let Some(N::AlterTableCmd(command)) = command.node {
                            use pg_query::protobuf::AlterTableType as A;
                            let name = match A::try_from(command.subtype)? {
                                A::AtEnableRowSecurity => "enable_rls",
                                A::AtDisableRowSecurity => "disable_rls",
                                A::AtForceRowSecurity => "force_rls",
                                _ => "alter_table_other",
                            };
                            result.feature(name);
                        }
                    }
                    continue;
                }
                N::VariableSetStmt(_) => "set",
                _ => "other",
            };
            result.feature(feature);
        }
        result.tables = tables.len() as u64;
        result.empty_tables = tables.values().filter(|rows| **rows == 0).count() as u64;
        result.populated_tables = result.tables - result.empty_tables;
        result.empty_schemas = u64::from(tables.is_empty());
        Ok(result)
    }

    pub fn add(&mut self, other: &Self) {
        self.workloads += other.workloads;
        self.tables += other.tables;
        self.empty_schemas += other.empty_schemas;
        self.empty_tables += other.empty_tables;
        self.populated_tables += other.populated_tables;
        self.values_rows += other.values_rows;
        self.null_cells += other.null_cells;
        for (feature, count) in &other.features {
            *self.features.entry(feature.clone()).or_default() += count;
        }
    }

    fn column(&mut self, column: &pg_query::protobuf::ColumnDef) {
        self.feature("column.total");
        if let Some(typ) = &column.type_name {
            let names: Vec<_> = typ
                .names
                .iter()
                .filter_map(|n| match &n.node {
                    Some(N::String(s)) => Some(s.sval.as_str()),
                    _ => None,
                })
                .collect();
            let name = match names.as_slice() {
                ["pg_catalog", "int4"] => "integer",
                ["pg_catalog", "int8"] => "bigint",
                ["pg_catalog", "bool"] => "boolean",
                ["text"] | ["pg_catalog", "text"] => "text",
                _ => "other",
            };
            self.feature(&format!(
                "column.type.{}",
                if typ.array_bounds.is_empty() {
                    name
                } else {
                    "other"
                }
            ));
        }
        let mut required = false;
        let mut default = false;
        for node in &column.constraints {
            if let Some(N::Constraint(constraint)) = &node.node {
                if matches!(
                    ConstrType::try_from(constraint.contype),
                    Ok(ConstrType::ConstrNotnull | ConstrType::ConstrPrimary)
                ) {
                    required = true;
                }
                if constraint.contype == ConstrType::ConstrDefault as i32 {
                    default = true;
                    if let Some(N::AConst(value)) =
                        constraint.raw_expr.as_ref().and_then(|n| n.node.as_ref())
                    {
                        use pg_query::protobuf::a_const::Val;
                        let integer = match &value.val {
                            Some(Val::Ival(value)) => Some(i64::from(value.ival)),
                            Some(Val::Fval(value)) => value.fval.parse::<i64>().ok(),
                            _ => None,
                        };
                        if let Some(integer) = integer {
                            self.feature(match integer.cmp(&0) {
                                std::cmp::Ordering::Less => "default.integer.negative",
                                std::cmp::Ordering::Equal => "default.integer.zero",
                                std::cmp::Ordering::Greater => "default.integer.positive",
                            });
                        }
                    }
                }
            }
        }
        self.feature(if required {
            "column.required"
        } else {
            "column.nullable"
        });
        self.feature(if default {
            "column.with_default"
        } else {
            "column.without_default"
        });
    }

    fn privileges(&mut self, kind: &str, grant: &pg_query::protobuf::GrantStmt) {
        if grant.privileges.is_empty() {
            self.feature(&format!("{kind}.privilege.all"));
        }
        for node in &grant.privileges {
            if let Some(N::AccessPriv(privilege)) = &node.node {
                self.feature(&format!(
                    "{kind}.privilege.{}",
                    privilege.priv_name.to_lowercase()
                ));
            }
        }
    }

    fn feature(&mut self, feature: &str) {
        *self.features.entry(feature.to_owned()).or_default() += 1;
    }
}

#[derive(Default, Serialize)]
pub struct Engine {
    pub completed: Generated,
    pub failed_loads: u64,
    pub inserted_rows: u64,
    pub updated_rows: u64,
    pub deleted_rows: u64,
    pub dumps: u64,
    pub dump: Dump,
}

#[derive(Default, Serialize)]
pub struct Dump {
    pub rows: u64,
    pub empty_tables: u64,
    pub populated_tables: u64,
    pub null_cells: u64,
    pub duplicate_rows: u64,
}

impl Dump {
    pub fn parse(sql: &str) -> Result<Self> {
        let ast = dump_ast(sql)?;
        let mut result = Self::default();
        for statement in ast {
            if let Some(rows) = statement.get("CopyData").and_then(|rows| rows.as_array()) {
                result.rows += rows.len() as u64;
                result.empty_tables += u64::from(rows.is_empty());
                result.populated_tables += u64::from(!rows.is_empty());
                result.null_cells += rows
                    .iter()
                    .filter_map(|row| row.as_array())
                    .flatten()
                    .filter(|cell| cell.is_null())
                    .count() as u64;
                result.duplicate_rows +=
                    rows.windows(2).filter(|pair| pair[0] == pair[1]).count() as u64;
            }
        }
        Ok(result)
    }
}

fn schema_features() -> Vec<String> {
    let mut names: Vec<String> = [
        "column.total",
        "column.nullable",
        "column.required",
        "column.with_default",
        "column.without_default",
        "column.type.integer",
        "column.type.bigint",
        "column.type.text",
        "column.type.boolean",
        "default.integer.negative",
        "default.integer.zero",
        "default.integer.positive",
        "index.non_unique",
        "index.unique",
        "index.multicolumn",
        "index.key.asc",
        "index.key.desc",
        "index.method.btree",
        "index.single_column",
        "policy.permissive",
        "policy.restrictive",
        "policy.using",
        "policy.with_check",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    for columns in 1..=8 {
        names.push(format!("table.columns.{columns}"));
    }
    for command in ["all", "select", "insert", "update", "delete"] {
        names.push(format!("policy.command.{command}"));
        for kind in ["grant", "revoke", "default_privileges"] {
            names.push(format!("{kind}.privilege.{command}"));
        }
    }
    names
}
