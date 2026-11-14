use crate::summary::{Engine, Summary};
use comfy_table::{
    presets::{ASCII_FULL_CONDENSED, UTF8_FULL_CONDENSED},
    Attribute, Cell, CellAlignment, Color, ContentArrangement, Table,
};
use std::{collections::BTreeSet, io::IsTerminal, path::Path};

pub(crate) fn render(
    summary: &Summary,
    seed: Option<u64>,
    disabled: &[String],
    path: &Path,
) -> String {
    let terminal = std::io::stdout().is_terminal();
    let color = terminal
        && std::env::var_os("NO_COLOR").is_none_or(|value| value.is_empty())
        && std::env::var("TERM").is_ok_and(|value| value != "dumb");
    let mut output = String::from("\nPG DUMP FUZZER - Generation summary\n");
    let mut totals = table(&["Workloads", "Passed", "Failed", "Seed"], terminal, color);
    totals.add_row([
        number(summary.attempts),
        styled(number(summary.passed), Color::Green, color),
        styled(
            number(summary.failed),
            if summary.failed == 0 {
                Color::DarkGrey
            } else {
                Color::Red
            },
            color,
        ),
        Cell::new(
            seed.map(|s| s.to_string())
                .unwrap_or_else(|| "automatic".to_owned()),
        ),
    ]);
    output.push_str(&format!("{totals}\n"));
    output.push_str(&format!(
        "Disabled generators: {}\n",
        if disabled.is_empty() {
            "none".to_owned()
        } else {
            disabled.join(", ")
        }
    ));
    if summary.unparsed_workloads > 0 {
        output.push_str(&format!(
            "Unparsed workloads: {} (excluded from feature counts)\n",
            summary.unparsed_workloads
        ));
    }

    let generated = &summary.generated;
    let mut inputs = table(&["Generated input", "Count"], terminal, color);
    for (name, count) in [
        ("Tables", generated.tables),
        ("Initially empty tables", generated.empty_tables),
        ("Initially populated tables", generated.populated_tables),
        ("Empty schemas", generated.empty_schemas),
        ("VALUES rows", generated.values_rows),
        ("NULL cells in VALUES", generated.null_cells),
    ] {
        inputs.add_row([Cell::new(name), number(count)]);
    }
    output.push_str(&format!("\n{inputs}\n"));

    let empty = Engine::default();
    let pg = summary.engines.get("postgres").unwrap_or(&empty);
    let turso = summary.engines.get("turso").unwrap_or(&empty);
    let mut engines = table(&["Loads and data", "PostgreSQL", "Turso"], terminal, color);
    for (name, pg, turso) in [
        (
            "Complete loads",
            pg.completed.workloads,
            turso.completed.workloads,
        ),
        ("Failed loads", pg.failed_loads, turso.failed_loads),
        (
            "Acknowledged inserts",
            pg.inserted_rows,
            turso.inserted_rows,
        ),
        ("Parsed dumps", pg.dumps, turso.dumps),
        ("Dumped rows", pg.dump.rows, turso.dump.rows),
        (
            "Empty COPY tables",
            pg.dump.empty_tables,
            turso.dump.empty_tables,
        ),
        (
            "Populated COPY tables",
            pg.dump.populated_tables,
            turso.dump.populated_tables,
        ),
        (
            "Dumped NULL cells",
            pg.dump.null_cells,
            turso.dump.null_cells,
        ),
        (
            "Duplicate rows",
            pg.dump.duplicate_rows,
            turso.dump.duplicate_rows,
        ),
    ] {
        engines.add_row([Cell::new(name), number(pg), number(turso)]);
    }
    output.push_str(&format!("\n{engines}\n"));
    output.push_str(
        "\nFeature counts: PG/Turso count only complete loads. Zero means not exercised.\n",
    );
    output.push_str("Affected rows above include successful statements before a load failure.\n");

    let mut features: BTreeSet<&str> = [
        "create_table",
        "primary_keys",
        "not_null",
        "column_defaults",
        "insert_values",
        "create_index",
        "enable_rls",
        "force_rls",
        "disable_rls",
        "create_policy",
        "alter_policy",
        "drop_policy",
        "grant",
        "revoke",
        "default_privileges",
        "set",
        "other",
    ]
    .into_iter()
    .collect();
    features.extend(generated.features.keys().map(String::as_str));
    features.extend(pg.completed.features.keys().map(String::as_str));
    features.extend(turso.completed.features.keys().map(String::as_str));
    for (title, prefix) in [
        ("SQL statements and constraints", ""),
        ("Table column counts", "table.columns."),
        ("Columns", "column."),
        ("Integer defaults", "default.integer."),
        ("Indexes", "index."),
        ("Policies", "policy."),
        ("Grants", "grant.privilege."),
        ("Revokes", "revoke.privilege."),
        ("Default privileges", "default_privileges.privilege."),
    ] {
        let mut detail = table(&["Feature", "Generated", "PG", "Turso"], terminal, color);
        let mut rows = 0;
        for feature in &features {
            if if prefix.is_empty() {
                feature.contains('.')
            } else {
                !feature.starts_with(prefix)
            } {
                continue;
            }
            let count = |features: &std::collections::BTreeMap<String, u64>| {
                features.get(*feature).copied().unwrap_or_default()
            };
            let generated_count = count(&generated.features);
            let pg_count = count(&pg.completed.features);
            let turso_count = count(&turso.completed.features);
            let label = feature
                .strip_prefix(prefix)
                .unwrap_or(feature)
                .replace(['_', '.'], " ");
            detail.add_row([
                styled(
                    Cell::new(label),
                    if generated_count == 0 {
                        Color::DarkGrey
                    } else {
                        Color::Reset
                    },
                    color,
                ),
                styled(
                    number(generated_count),
                    if generated_count == 0 {
                        Color::DarkGrey
                    } else {
                        Color::Reset
                    },
                    color,
                ),
                styled(
                    number(pg_count),
                    if pg_count < generated_count {
                        Color::Yellow
                    } else {
                        Color::Reset
                    },
                    color,
                ),
                styled(
                    number(turso_count),
                    if turso_count < generated_count {
                        Color::Yellow
                    } else {
                        Color::Reset
                    },
                    color,
                ),
            ]);
            rows += 1;
        }
        if rows > 0 {
            output.push_str(&format!("\n{title}\n{detail}\n"));
        }
    }
    output.push_str(&format!("\nJSON report: {}\n", path.display()));
    output
}

fn table(headers: &[&str], terminal: bool, color: bool) -> Table {
    let mut table = Table::new();
    table
        .load_preset(if terminal {
            UTF8_FULL_CONDENSED
        } else {
            ASCII_FULL_CONDENSED
        })
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(headers.iter().map(|header| {
            let cell = Cell::new(header);
            if color {
                cell.add_attribute(Attribute::Bold).fg(Color::Cyan)
            } else {
                cell
            }
        }));
    if !terminal {
        table.force_no_tty();
    }
    table
}

fn number(value: u64) -> Cell {
    Cell::new(value).set_alignment(CellAlignment::Right)
}

fn styled(cell: Cell, color: Color, enabled: bool) -> Cell {
    if enabled {
        cell.fg(color)
    } else {
        cell
    }
}
