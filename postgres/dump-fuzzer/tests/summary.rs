use hegel::{generators as gs, TestCase};
use std::collections::BTreeSet;
use turso_pg_dump_fuzzer::summary::{Dump, Generated, Summary};

#[hegel::test(test_cases = 2000)]
fn generated_counts_come_from_sql_structure(tc: TestCase) {
    let rows = tc.draw(gs::integers::<usize>().max_value(20));
    let mut sql = "CREATE TABLE empty (x INTEGER); CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER DEFAULT 7, label TEXT NOT NULL);".to_owned();
    for n in 0..rows {
        sql.push_str(&format!(
            "INSERT INTO t VALUES ({n}, NULL, 'CREATE TABLE fake (x TEXT); GRANT SELECT; NULL');"
        ));
    }
    sql.push_str("INSERT INTO t SELECT * FROM t; UPDATE t SET n = 1; DELETE FROM t; CREATE INDEX idx ON t(n);");
    let stats = Generated::parse(&sql).unwrap();
    assert_eq!(stats.tables, 2);
    assert_eq!(stats.values_rows, rows as u64);
    assert_eq!(stats.null_cells, rows as u64);
    assert_eq!(stats.populated_tables, u64::from(rows != 0));
    assert_eq!(stats.empty_tables, 2 - u64::from(rows != 0));
    assert_eq!(
        stats
            .features
            .get("insert_values")
            .copied()
            .unwrap_or_default(),
        rows as u64
    );
    for feature in [
        "insert_select",
        "update",
        "delete",
        "create_index",
        "primary_keys",
        "not_null",
        "column_defaults",
    ] {
        assert_eq!(stats.features[feature], 1);
    }
    assert!(!stats.features.contains_key("grant"));
}

#[hegel::test(test_cases = 2000)]
fn aggregation_preserves_totals(tc: TestCase) {
    let n = tc.draw(gs::integers::<i32>());
    let stats = Generated::parse(&format!(
        "CREATE TABLE t(x INTEGER); INSERT INTO t VALUES ({n});"
    ))
    .unwrap();
    let repetitions = tc.draw(gs::integers::<usize>().max_value(30));
    let mut combined = Generated::default();
    for _ in 0..repetitions {
        combined.add(&stats);
    }
    assert_eq!(combined.workloads, repetitions as u64);
    assert_eq!(combined.tables, repetitions as u64);
    assert_eq!(combined.values_rows, repetitions as u64);
    assert_eq!(
        combined
            .features
            .get("insert_values")
            .copied()
            .unwrap_or_default(),
        repetitions as u64
    );
}

#[hegel::test(test_cases = 2000)]
fn dump_counts_keep_duplicates_and_null_distinctions(tc: TestCase) {
    let values = tc.draw(gs::vecs(gs::integers::<i16>()).max_size(30));
    let mut sql = "COPY empty (x) FROM stdin;\n\\.\nCOPY t (a,b,c,d) FROM stdin;\n".to_owned();
    for value in &values {
        sql.push_str(&format!("{value}\t\\N\t\\\\N\t\n"));
    }
    sql.push_str("\\.\n");
    let stats = Dump::parse(&sql).unwrap();
    assert_eq!(stats.rows, values.len() as u64);
    assert_eq!(stats.null_cells, values.len() as u64);
    assert_eq!(
        stats.duplicate_rows,
        (values.len() - values.iter().collect::<BTreeSet<_>>().len()) as u64
    );
    assert_eq!(stats.empty_tables, 1 + u64::from(values.is_empty()));
    assert_eq!(stats.populated_tables, u64::from(!values.is_empty()));
}

#[hegel::test(test_cases = 2000)]
fn partial_load_counts_acknowledged_rows_but_not_completed_features(tc: TestCase) {
    let rows = tc.draw(gs::integers::<u32>());
    let generated =
        Generated::parse("CREATE TABLE t(x INTEGER); INSERT INTO t VALUES (NULL);").unwrap();
    let mut summary = Summary::default();
    summary.loaded(
        "postgres",
        &generated,
        false,
        &format!("CREATE TABLE\nINSERT 0 {rows}\nUPDATE 3\nDELETE 2\nERROR: failure\n"),
    );
    let engine = &summary.engines["postgres"];
    assert_eq!(engine.completed.workloads, 0);
    assert!(engine.completed.features.is_empty());
    assert_eq!(engine.failed_loads, 1);
    assert_eq!(engine.inserted_rows, rows as u64);
    assert_eq!(engine.updated_rows, 3);
    assert_eq!(engine.deleted_rows, 2);
}

#[hegel::test(test_cases = 2000)]
fn complete_loads_have_features_without_claiming_dumps(tc: TestCase) {
    let n = tc.draw(gs::integers::<i32>());
    let generated = Generated::parse(&format!(
        "CREATE TABLE t(x INTEGER); INSERT INTO t VALUES ({n});"
    ))
    .unwrap();
    let mut summary = Summary::default();
    summary.loaded("turso", &generated, true, "CREATE TABLE\nINSERT 0 1\n");
    let engine = &summary.engines["turso"];
    assert_eq!(engine.completed.workloads, 1);
    assert_eq!(engine.completed.values_rows, 1);
    assert_eq!(engine.dumps, 0);
    assert_eq!(engine.dump.rows, 0);
}

#[hegel::test(test_cases = 2000)]
fn malformed_dump_does_not_contribute_counts(tc: TestCase) {
    let n = tc.draw(gs::integers::<i32>());
    let mut summary = Summary::default();
    assert!(summary
        .dumped("postgres", &format!("COPY t FROM stdin;\n{n}\n"))
        .is_err());
    assert!(summary.engines.is_empty());
}

#[test]
fn accounting_keeps_generated_workload_when_artifact_setup_fails() {
    let directory = tempfile::tempdir().unwrap();
    let output = directory.path().join("file");
    std::fs::write(&output, "not a directory").unwrap();
    let runner = turso_pg_dump_fuzzer::runner::Runner {
        reference: "postgresql://test@localhost/postgres".to_owned(),
        binary: directory.path().join("unused"),
        output,
        psql: "unused".into(),
        pg_dump: "unused".into(),
        privileges: false,
        disabled_features: vec![],
    };
    let mut summary = Summary::default();
    assert!(runner
        .check_with_summary("SELECT 1;", &|_| {}, &mut summary)
        .is_err());
    assert_eq!(summary.generated.workloads, 1);
    assert_eq!(summary.failed, 1);
}

#[hegel::test(test_cases = 2000)]
fn accounting_does_not_invent_tables_for_insert_targets(tc: TestCase) {
    let n = tc.draw(gs::integers::<i32>());
    let stats = Generated::parse(&format!("INSERT INTO missing VALUES ({n});")).unwrap();
    assert_eq!(stats.tables, 0);
    assert_eq!(stats.values_rows, 1);
}

#[hegel::test(test_cases = 2000)]
fn identical_rows_in_different_tables_are_not_duplicates(tc: TestCase) {
    let n = tc.draw(gs::integers::<i32>());
    let stats = Dump::parse(&format!(
        "COPY a FROM stdin;\n{n}\n\\.\nCOPY b FROM stdin;\n{n}\n\\.\n"
    ))
    .unwrap();
    assert_eq!(stats.rows, 2);
    assert_eq!(stats.duplicate_rows, 0);
}

#[hegel::test(test_cases = 2000)]
fn dump_summary_is_independent_of_row_order(tc: TestCase) {
    let rows = tc.draw(gs::vecs(gs::integers::<i16>()).max_size(30));
    let dump = |rows: Vec<i16>| {
        format!(
            "COPY t FROM stdin;\n{}\\.\n",
            rows.iter()
                .map(|n| format!("{n}\t\\N\n"))
                .collect::<String>()
        )
    };
    let forward = Dump::parse(&dump(rows.clone())).unwrap();
    let reverse = Dump::parse(&dump(rows.into_iter().rev().collect())).unwrap();
    assert_eq!(
        serde_json::to_value(forward).unwrap(),
        serde_json::to_value(reverse).unwrap()
    );
}

#[hegel::test(test_cases = 2000)]
fn schema_columns_count_types_and_constraints(tc: TestCase) {
    let primary = tc.draw(gs::booleans());
    let not_null = tc.draw(gs::booleans());
    let default = tc.draw(gs::integers::<i32>());
    let sql = format!(
        "CREATE TABLE t(id INTEGER{}, value BIGINT DEFAULT {default}, label TEXT{}, flag BOOLEAN);",
        if primary { " PRIMARY KEY" } else { "" },
        if not_null { " NOT NULL" } else { "" }
    );
    let stats = Generated::parse(&sql).unwrap();
    for name in ["integer", "bigint", "text", "boolean"] {
        assert_eq!(stats.features[&format!("column.type.{name}")], 1);
    }
    assert_eq!(stats.features["column.total"], 4);
    assert_eq!(
        stats.features["column.nullable"],
        4 - u64::from(primary) - u64::from(not_null)
    );
    assert_eq!(
        stats.features["column.required"],
        u64::from(primary) + u64::from(not_null)
    );
    assert_eq!(stats.features["column.without_default"], 3);
    let sign = if default < 0 {
        "negative"
    } else if default == 0 {
        "zero"
    } else {
        "positive"
    };
    assert_eq!(stats.features[&format!("default.integer.{sign}")], 1);
}

#[hegel::test(test_cases = 2000)]
fn policy_and_privilege_details_match_generated_choices(tc: TestCase) {
    let command = tc.draw(gs::sampled_from(&[
        "ALL", "SELECT", "INSERT", "UPDATE", "DELETE",
    ]));
    let permissive = tc.draw(gs::booleans());
    let threshold = tc.draw(gs::integers::<i32>());
    let using = command != "INSERT";
    let check = matches!(command, "ALL" | "INSERT" | "UPDATE");
    let sql = format!(
        "CREATE POLICY p ON t AS {} FOR {command} TO PUBLIC {} {};
        GRANT {command} ON t TO PUBLIC; REVOKE {command} ON t FROM PUBLIC;
        ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT {command} ON TABLES TO PUBLIC;",
        if permissive {
            "PERMISSIVE"
        } else {
            "RESTRICTIVE"
        },
        if using {
            format!("USING (id >= {threshold})")
        } else {
            String::new()
        },
        if check {
            format!("WITH CHECK (id >= {threshold})")
        } else {
            String::new()
        }
    );
    let stats = Generated::parse(&sql).unwrap();
    assert_eq!(
        stats.features[&format!("policy.command.{}", command.to_lowercase())],
        1
    );
    assert_eq!(stats.features["policy.permissive"], u64::from(permissive));
    assert_eq!(stats.features["policy.restrictive"], u64::from(!permissive));
    assert_eq!(stats.features["policy.using"], u64::from(using));
    assert_eq!(stats.features["policy.with_check"], u64::from(check));
    for kind in ["grant", "revoke", "default_privileges"] {
        assert_eq!(
            stats.features[&format!("{kind}.privilege.{}", command.to_lowercase())],
            1
        );
    }
}

#[hegel::test(test_cases = 2000)]
fn detailed_features_add_only_for_complete_loads(tc: TestCase) {
    let count = tc.draw(gs::integers::<usize>().max_value(30));
    let success = tc.draw(gs::booleans());
    let sql = (0..count)
        .map(|i| format!("CREATE TABLE t{i}(x INTEGER); CREATE INDEX i{i} ON t{i}(x);"))
        .collect::<String>();
    let generated = Generated::parse(&sql).unwrap();
    let mut summary = Summary::default();
    summary.generated.add(&generated);
    summary.loaded("turso", &generated, success, "");
    for key in [
        "column.total",
        "column.type.integer",
        "index.non_unique",
        "index.method.btree",
        "index.single_column",
    ] {
        assert_eq!(generated.features[key], count as u64);
        assert_eq!(
            summary.engines["turso"]
                .completed
                .features
                .get(key)
                .copied()
                .unwrap_or_default(),
            if success { count as u64 } else { 0 }
        );
    }
    let output = tempfile::tempdir().unwrap();
    let path = summary.finish(output.path(), Some(42), &[]).unwrap();
    let report: serde_json::Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert_eq!(
        report["summary"]["generated"]["features"],
        serde_json::to_value(&generated.features).unwrap()
    );
}

#[test]
fn integer_default_signs_include_boundaries() {
    for (value, sign) in [
        (i32::MIN, "negative"),
        (-1, "negative"),
        (0, "zero"),
        (1, "positive"),
        (i32::MAX, "positive"),
    ] {
        let stats =
            Generated::parse(&format!("CREATE TABLE t(x INTEGER DEFAULT {value});")).unwrap();
        assert_eq!(stats.features[&format!("default.integer.{sign}")], 1);
        assert_eq!(
            ["negative", "zero", "positive"]
                .into_iter()
                .map(|s| stats.features[&format!("default.integer.{s}")])
                .sum::<u64>(),
            1
        );
    }
}

#[hegel::test(test_cases = 2000)]
fn schema_summary_counts_width_and_index_variants(tc: TestCase) {
    let columns = tc.draw(gs::integers::<usize>().min_value(1).max_value(8));
    let keys = tc.draw(
        gs::integers::<usize>()
            .min_value(1)
            .max_value(columns.min(3)),
    );
    let descending = tc.draw(gs::integers::<usize>().max_value(keys));
    let unique = tc.draw(gs::booleans());
    let definition = (0..columns)
        .map(|i| format!("c{i} INTEGER"))
        .collect::<Vec<_>>()
        .join(", ");
    let index = (0..keys)
        .map(|i| format!("c{i} {}", if i < descending { "DESC" } else { "ASC" }))
        .collect::<Vec<_>>()
        .join(", ");
    let stats = Generated::parse(&format!(
        "CREATE TABLE t({definition}); CREATE {}INDEX idx ON t({index});",
        if unique { "UNIQUE " } else { "" }
    ))
    .unwrap();
    assert_eq!(stats.features[&format!("table.columns.{columns}")], 1);
    assert_eq!(stats.features["index.key.desc"], descending as u64);
    assert_eq!(stats.features["index.key.asc"], (keys - descending) as u64);
    assert_eq!(stats.features["index.unique"], u64::from(unique));
    assert_eq!(stats.features["index.non_unique"], u64::from(!unique));
    assert_eq!(stats.features["index.multicolumn"], u64::from(keys > 1));
    assert_eq!(stats.features["index.single_column"], u64::from(keys == 1));
}
