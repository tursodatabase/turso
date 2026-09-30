// Copyright 2023-2026 the Turso authors. All rights reserved. MIT license.

//! psql's describe commands, reimplemented over catalog queries.
//!
//! Covers the forms the regress corpus uses: `\d`, `\d pattern`,
//! `\d+ pattern`, `\dD`, `\dT+`, `\sv`, `\sf`. Output mirrors psql's
//! layout: a centered title, an aligned column table without a row count
//! (list commands keep the count), footer lines, and a trailing blank
//! line. The catalog queries go to the server like any other statement,
//! so missing emulation surfaces as a rendered error in the transcript —
//! a server gap to burn down, not a harness failure.

use anyhow::Result;

use crate::Session;

pub(crate) fn run(
    session: &mut Session,
    name: &str,
    args: &[String],
    out: &mut String,
) -> Result<()> {
    let pattern = args.first().map(String::as_str);
    match name {
        "d" | "d+" => match pattern {
            Some(p) => describe_relation(session, p, name == "d+", out),
            None => list_relations(session, out),
        },
        "dD" | "dD+" => list_domains(session, pattern, out),
        "dT" | "dT+" => list_types(session, pattern, out),
        "sv" => show_view(session, pattern, out),
        "sf" => show_function(session, pattern, out),
        _ => {
            out.push_str(&format!("invalid command \\{name}\n"));
            Ok(())
        }
    }
}

/// Runs a catalog query, flattening rows to strings (NULL becomes "").
/// Returns None when the server errored; the error is already rendered.
fn q(session: &mut Session, sql: &str, out: &mut String) -> Result<Option<Vec<Vec<String>>>> {
    let table = session.collect(sql, out)?;
    Ok(table.map(|t| {
        t.rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|v| v.unwrap_or_default())
                    .collect::<Vec<String>>()
            })
            .collect()
    }))
}

/// Server booleans may arrive as t/f or as SQLite-style 0/1.
fn is_true(value: &str) -> bool {
    matches!(value, "t" | "true" | "1" | "yes" | "on")
}

/// Converts a psql name pattern into anchored regexes for schema and name:
/// unquoted text lowercases, `*` becomes `.*`, `?` becomes `.`, double
/// quotes preserve case, a dot splits schema from name.
fn pattern_to_regex(pattern: &str) -> (Option<String>, String) {
    let mut parts: Vec<String> = vec![String::new()];
    let mut in_quotes = false;
    for c in pattern.chars() {
        let current = parts.last_mut().expect("parts is never empty");
        match c {
            '"' => in_quotes = !in_quotes,
            '.' if !in_quotes => parts.push(String::new()),
            '*' if !in_quotes => current.push_str(".*"),
            '?' if !in_quotes => current.push('.'),
            _ => {
                let c = if in_quotes { c } else { c.to_ascii_lowercase() };
                if "$()[]{}|^+\\".contains(c) {
                    current.push('\\');
                }
                current.push(c);
            }
        }
    }
    let name = format!("^({})$", parts.pop().expect("parts is never empty"));
    let schema = parts.pop().map(|s| format!("^({s})$"));
    (schema, name)
}

/// Escapes a string for inclusion in a single-quoted SQL literal.
fn lit(s: &str) -> String {
    s.replace('\'', "''")
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

/// Renders a describe table: centered title, centered headers, left-aligned
/// values, optional `(n rows)` count, footers, and a trailing blank line.
fn render(
    title: Option<&str>,
    headers: &[&str],
    rows: &[Vec<String>],
    footers: &[String],
    row_count: bool,
    out: &mut String,
) {
    let ncols = headers.len();
    let widths: Vec<usize> = (0..ncols)
        .map(|i| {
            rows.iter()
                .map(|r| r.get(i).map_or(0, |v| v.chars().count()))
                .max()
                .unwrap_or(0)
                .max(headers[i].chars().count())
        })
        .collect();
    let total: usize = widths.iter().map(|w| w + 2).sum::<usize>() + ncols.saturating_sub(1);

    if let Some(title) = title {
        let indent = total.saturating_sub(title.chars().count()) / 2;
        out.push_str(&format!("{}{title}\n", " ".repeat(indent)));
    }

    let header: Vec<String> = headers
        .iter()
        .zip(&widths)
        .map(|(h, &w)| {
            let pad = w - h.chars().count();
            format!(" {}{h}{} ", " ".repeat(pad / 2), " ".repeat(pad - pad / 2))
        })
        .collect();
    out.push_str(&header.join("|"));
    out.push('\n');
    let sep: Vec<String> = widths.iter().map(|&w| "-".repeat(w + 2)).collect();
    out.push_str(&sep.join("+"));
    out.push('\n');

    for row in rows {
        let cells: Vec<String> = (0..ncols)
            .map(|i| {
                let v = row.get(i).map(String::as_str).unwrap_or("");
                format!(" {v}{} ", " ".repeat(widths[i] - v.chars().count()))
            })
            .collect();
        let line = cells.join("|");
        out.push_str(line.trim_end());
        out.push('\n');
    }

    if row_count {
        let n = rows.len();
        out.push_str(&format!("({n} row{})\n", if n == 1 { "" } else { "s" }));
    }
    for footer in footers {
        out.push_str(footer);
        out.push('\n');
    }
    out.push('\n');
}

// ---------------------------------------------------------------------------
// \d — relations
// ---------------------------------------------------------------------------

fn list_relations(session: &mut Session, out: &mut String) -> Result<()> {
    let sql = "SELECT n.nspname, c.relname, c.relkind, pg_catalog.pg_get_userbyid(c.relowner) \
               FROM pg_catalog.pg_class c \
               LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
               WHERE c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f') \
                 AND n.nspname <> 'pg_catalog' AND n.nspname <> 'information_schema' \
                 AND pg_catalog.pg_table_is_visible(c.oid) \
               ORDER BY 1, 2";
    let Some(rows) = q(session, sql, out)? else {
        return Ok(());
    };
    let rows: Vec<Vec<String>> = rows
        .into_iter()
        .map(|r| {
            vec![
                r[0].clone(),
                r[1].clone(),
                relkind_name(&r[2]).to_string(),
                r[3].clone(),
            ]
        })
        .collect();
    render(
        Some("List of relations"),
        &["Schema", "Name", "Type", "Owner"],
        &rows,
        &[],
        true,
        out,
    );
    Ok(())
}

fn relkind_name(kind: &str) -> &'static str {
    match kind {
        "r" => "table",
        "p" => "partitioned table",
        "v" => "view",
        "m" => "materialized view",
        "S" => "sequence",
        "i" | "I" => "index",
        "f" => "foreign table",
        _ => "relation",
    }
}

fn describe_relation(
    session: &mut Session,
    pattern: &str,
    verbose: bool,
    out: &mut String,
) -> Result<()> {
    let (schema_re, name_re) = pattern_to_regex(pattern);
    let schema_cond = match &schema_re {
        Some(re) => format!("n.nspname OPERATOR(pg_catalog.~) '{}'", lit(re)),
        None => "pg_catalog.pg_table_is_visible(c.oid)".to_string(),
    };
    let sql = format!(
        "SELECT c.oid, n.nspname, c.relname, c.relkind \
         FROM pg_catalog.pg_class c \
         LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.relname OPERATOR(pg_catalog.~) '{}' AND {schema_cond} \
         ORDER BY 2, 3",
        lit(&name_re)
    );
    let Some(rels) = q(session, &sql, out)? else {
        return Ok(());
    };
    if rels.is_empty() {
        out.push_str(&format!("Did not find any relation named \"{pattern}\".\n"));
        return Ok(());
    }
    for rel in rels {
        let (oid, schema, name, kind) = (&rel[0], &rel[1], &rel[2], rel[3].as_str());
        match kind {
            "S" => describe_sequence(session, schema, name, out)?,
            "i" | "I" => describe_index(session, oid, schema, name, verbose, out)?,
            _ => describe_table(session, oid, schema, name, kind, verbose, out)?,
        }
    }
    Ok(())
}

fn describe_table(
    session: &mut Session,
    oid: &str,
    schema: &str,
    name: &str,
    kind: &str,
    verbose: bool,
    out: &mut String,
) -> Result<()> {
    let sql = format!(
        "SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), \
                a.attnotnull, \
                pg_catalog.pg_get_expr(d.adbin, d.adrelid), \
                a.attstorage \
         FROM pg_catalog.pg_attribute a \
         LEFT JOIN pg_catalog.pg_attrdef d \
                ON a.attrelid = d.adrelid AND a.attnum = d.adnum \
         WHERE a.attrelid = {oid} AND a.attnum > 0 AND NOT a.attisdropped \
         ORDER BY a.attnum"
    );
    let Some(cols) = q(session, &sql, out)? else {
        return Ok(());
    };
    let mut rows = Vec::new();
    for c in &cols {
        let mut row = vec![
            c[0].clone(),
            c[1].clone(),
            String::new(), // collation: emulation has no per-column collations yet
            if is_true(&c[2]) { "not null" } else { "" }.to_string(),
            c[3].clone(),
        ];
        if verbose {
            row.push(storage_name(&c[4]).to_string());
            row.push(String::new()); // stats target
            row.push(String::new()); // description
        }
        rows.push(row);
    }

    let title = match kind {
        "v" => format!("View \"{schema}.{name}\""),
        "m" => format!("Materialized view \"{schema}.{name}\""),
        "p" => format!("Partitioned table \"{schema}.{name}\""),
        _ => format!("Table \"{schema}.{name}\""),
    };
    let mut headers = vec!["Column", "Type", "Collation", "Nullable", "Default"];
    if verbose {
        headers.extend(["Storage", "Stats target", "Description"]);
    }

    let mut footers = Vec::new();
    if kind == "v" || kind == "m" {
        if verbose {
            if let Some(def) = q(
                session,
                &format!("SELECT pg_catalog.pg_get_viewdef({oid}, true)"),
                out,
            )? {
                if let Some(text) = def.first().and_then(|r| r.first()) {
                    footers.push("View definition:".to_string());
                    footers.push(text.trim_end().to_string());
                }
            }
        }
    } else {
        table_footers(session, oid, &mut footers, out)?;
    }
    render(Some(&title), &headers, &rows, &footers, false, out);
    Ok(())
}

fn storage_name(code: &str) -> &'static str {
    match code {
        "x" => "extended",
        "m" => "main",
        "e" => "external",
        _ => "plain",
    }
}

/// Appends the Indexes / Check constraints / Foreign-key constraints /
/// Referenced by footers of a table.
fn table_footers(
    session: &mut Session,
    oid: &str,
    footers: &mut Vec<String>,
    out: &mut String,
) -> Result<()> {
    let sql = format!(
        "SELECT c2.relname, i.indisprimary, i.indisunique, \
                pg_catalog.pg_get_indexdef(i.indexrelid) \
         FROM pg_catalog.pg_index i \
         JOIN pg_catalog.pg_class c2 ON c2.oid = i.indexrelid \
         WHERE i.indrelid = {oid} \
         ORDER BY i.indisprimary DESC, i.indisunique DESC, c2.relname"
    );
    if let Some(indexes) = q(session, &sql, out)? {
        if !indexes.is_empty() {
            footers.push("Indexes:".to_string());
            for idx in &indexes {
                // psql shows the part after USING: "btree (a, b)".
                let def = match idx[3].split_once(" USING ") {
                    Some((_, tail)) => tail,
                    None => idx[3].as_str(),
                };
                let flag = if is_true(&idx[1]) {
                    "PRIMARY KEY, "
                } else if is_true(&idx[2]) {
                    "UNIQUE CONSTRAINT, "
                } else {
                    ""
                };
                footers.push(format!("    \"{}\" {flag}{def}", idx[0]));
            }
        }
    }

    let sql = format!(
        "SELECT conname, pg_catalog.pg_get_constraintdef(oid) \
         FROM pg_catalog.pg_constraint \
         WHERE conrelid = {oid} AND contype = 'c' ORDER BY conname"
    );
    if let Some(checks) = q(session, &sql, out)? {
        if !checks.is_empty() {
            footers.push("Check constraints:".to_string());
            for c in &checks {
                footers.push(format!("    \"{}\" {}", c[0], c[1]));
            }
        }
    }

    let sql = format!(
        "SELECT conname, pg_catalog.pg_get_constraintdef(oid) \
         FROM pg_catalog.pg_constraint \
         WHERE conrelid = {oid} AND contype = 'f' ORDER BY conname"
    );
    if let Some(fks) = q(session, &sql, out)? {
        if !fks.is_empty() {
            footers.push("Foreign-key constraints:".to_string());
            for f in &fks {
                footers.push(format!("    \"{}\" {}", f[0], f[1]));
            }
        }
    }

    let sql = format!(
        "SELECT c.relname, con.conname, pg_catalog.pg_get_constraintdef(con.oid) \
         FROM pg_catalog.pg_constraint con \
         JOIN pg_catalog.pg_class c ON c.oid = con.conrelid \
         WHERE con.confrelid = {oid} AND con.contype = 'f' ORDER BY c.relname, con.conname"
    );
    if let Some(refs) = q(session, &sql, out)? {
        if !refs.is_empty() {
            footers.push("Referenced by:".to_string());
            for r in &refs {
                footers.push(format!(
                    "    TABLE \"{}\" CONSTRAINT \"{}\" {}",
                    r[0], r[1], r[2]
                ));
            }
        }
    }
    Ok(())
}

fn describe_sequence(
    session: &mut Session,
    schema: &str,
    name: &str,
    out: &mut String,
) -> Result<()> {
    let sql = format!(
        "SELECT data_type, start_value, min_value, max_value, increment_by, cycle, cache_size \
         FROM pg_catalog.pg_sequences \
         WHERE schemaname = '{}' AND sequencename = '{}'",
        lit(schema),
        lit(name)
    );
    let Some(rows) = q(session, &sql, out)? else {
        return Ok(());
    };
    let Some(r) = rows.first() else {
        return Ok(());
    };
    let row = vec![
        r[0].clone(),
        r[1].clone(),
        r[2].clone(),
        r[3].clone(),
        r[4].clone(),
        if is_true(&r[5]) { "yes" } else { "no" }.to_string(),
        r[6].clone(),
    ];
    render(
        Some(&format!("Sequence \"{schema}.{name}\"")),
        &[
            "Type",
            "Start",
            "Minimum",
            "Maximum",
            "Increment",
            "Cycles?",
            "Cache",
        ],
        &[row],
        &[],
        false,
        out,
    );
    Ok(())
}

fn describe_index(
    session: &mut Session,
    oid: &str,
    schema: &str,
    name: &str,
    verbose: bool,
    out: &mut String,
) -> Result<()> {
    let sql = format!(
        "SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), a.attstorage \
         FROM pg_catalog.pg_attribute a \
         WHERE a.attrelid = {oid} AND a.attnum > 0 AND NOT a.attisdropped \
         ORDER BY a.attnum"
    );
    let Some(cols) = q(session, &sql, out)? else {
        return Ok(());
    };
    let mut rows = Vec::new();
    for c in &cols {
        let mut row = vec![c[0].clone(), c[1].clone(), "yes".to_string(), c[0].clone()];
        if verbose {
            row.push(storage_name(&c[2]).to_string());
            row.push(String::new()); // stats target
        }
        rows.push(row);
    }
    let mut headers = vec!["Column", "Type", "Key?", "Definition"];
    if verbose {
        headers.extend(["Storage", "Stats target"]);
    }

    let sql = format!(
        "SELECT am.amname, c2.relname, n2.nspname \
         FROM pg_catalog.pg_index i \
         JOIN pg_catalog.pg_class c2 ON c2.oid = i.indrelid \
         LEFT JOIN pg_catalog.pg_namespace n2 ON n2.oid = c2.relnamespace \
         JOIN pg_catalog.pg_class ci ON ci.oid = i.indexrelid \
         LEFT JOIN pg_catalog.pg_am am ON am.oid = ci.relam \
         WHERE i.indexrelid = {oid}"
    );
    let mut footers = Vec::new();
    if let Some(rows) = q(session, &sql, out)? {
        if let Some(r) = rows.first() {
            let am = if r[0].is_empty() { "btree" } else { &r[0] };
            footers.push(format!("{am}, for table \"{}.{}\"", r[2], r[1]));
        }
    }
    render(
        Some(&format!("Index \"{schema}.{name}\"")),
        &headers,
        &rows,
        &footers,
        false,
        out,
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// \dD, \dT+ — type lists
// ---------------------------------------------------------------------------

fn list_domains(session: &mut Session, pattern: Option<&str>, out: &mut String) -> Result<()> {
    let name_cond = match pattern {
        Some(p) => {
            let (_, name_re) = pattern_to_regex(p);
            format!("t.typname OPERATOR(pg_catalog.~) '{}'", lit(&name_re))
        }
        None => "pg_catalog.pg_type_is_visible(t.oid)".to_string(),
    };
    let sql = format!(
        "SELECT n.nspname, t.typname, \
                pg_catalog.format_type(t.typbasetype, t.typtypmod), \
                t.typnotnull, t.typdefault, t.oid \
         FROM pg_catalog.pg_type t \
         LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace \
         WHERE t.typtype = 'd' AND {name_cond} \
           AND n.nspname <> 'pg_catalog' AND n.nspname <> 'information_schema' \
         ORDER BY 1, 2"
    );
    let Some(domains) = q(session, &sql, out)? else {
        return Ok(());
    };
    let mut rows = Vec::new();
    for d in &domains {
        let checks = q(
            session,
            &format!(
                "SELECT pg_catalog.pg_get_constraintdef(oid) \
                 FROM pg_catalog.pg_constraint WHERE contypid = {} ORDER BY conname",
                d[5]
            ),
            out,
        )?
        .map(|rows| {
            rows.iter()
                .map(|r| r[0].clone())
                .collect::<Vec<_>>()
                .join(" ")
        })
        .unwrap_or_default();
        rows.push(vec![
            d[0].clone(),
            d[1].clone(),
            d[2].clone(),
            String::new(), // collation
            if is_true(&d[3]) { "not null" } else { "" }.to_string(),
            d[4].clone(),
            checks,
        ]);
    }
    render(
        Some("List of domains"),
        &[
            "Schema",
            "Name",
            "Type",
            "Collation",
            "Nullable",
            "Default",
            "Check",
        ],
        &rows,
        &[],
        true,
        out,
    );
    Ok(())
}

fn list_types(session: &mut Session, pattern: Option<&str>, out: &mut String) -> Result<()> {
    let name_cond = match pattern {
        Some(p) => {
            let (_, name_re) = pattern_to_regex(p);
            format!(
                "(t.typname OPERATOR(pg_catalog.~) '{re}' \
                  OR pg_catalog.format_type(t.oid, NULL) OPERATOR(pg_catalog.~) '{re}')",
                re = lit(&name_re)
            )
        }
        None => "pg_catalog.pg_type_is_visible(t.oid)".to_string(),
    };
    let sql = format!(
        "SELECT n.nspname, pg_catalog.format_type(t.oid, NULL), t.typname, t.typlen, \
                pg_catalog.pg_get_userbyid(t.typowner) \
         FROM pg_catalog.pg_type t \
         LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace \
         WHERE {name_cond} AND t.typtype <> 'd' \
         ORDER BY 1, 2"
    );
    let Some(types) = q(session, &sql, out)? else {
        return Ok(());
    };
    let rows: Vec<Vec<String>> = types
        .iter()
        .map(|t| {
            let size = match t[3].parse::<i64>() {
                Ok(n) if n > 0 => t[3].clone(),
                _ => "var".to_string(),
            };
            vec![
                t[0].clone(),
                t[1].clone(),
                t[2].clone(),
                size,
                String::new(), // elements
                t[4].clone(),
                String::new(), // access privileges
                String::new(), // description
            ]
        })
        .collect();
    render(
        Some("List of data types"),
        &[
            "Schema",
            "Name",
            "Internal name",
            "Size",
            "Elements",
            "Owner",
            "Access privileges",
            "Description",
        ],
        &rows,
        &[],
        true,
        out,
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// \sv, \sf — object definitions
// ---------------------------------------------------------------------------

fn show_view(session: &mut Session, pattern: Option<&str>, out: &mut String) -> Result<()> {
    let Some(pattern) = pattern else {
        out.push_str("\\sv: missing required argument\n");
        return Ok(());
    };
    let (_, name_re) = pattern_to_regex(pattern);
    let sql = format!(
        "SELECT n.nspname, c.relname, pg_catalog.pg_get_viewdef(c.oid, true) \
         FROM pg_catalog.pg_class c \
         LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.relkind IN ('v', 'm') AND c.relname OPERATOR(pg_catalog.~) '{}' \
           AND pg_catalog.pg_table_is_visible(c.oid) \
         ORDER BY 1, 2",
        lit(&name_re)
    );
    let Some(views) = q(session, &sql, out)? else {
        return Ok(());
    };
    if views.is_empty() {
        out.push_str(&format!("Did not find any view named \"{pattern}\".\n"));
        return Ok(());
    }
    for v in &views {
        out.push_str(&format!("CREATE OR REPLACE VIEW {}.{} AS\n", v[0], v[1]));
        let def = v[2].trim_end().trim_end_matches(';');
        out.push_str(def);
        out.push('\n');
    }
    Ok(())
}

fn show_function(session: &mut Session, pattern: Option<&str>, out: &mut String) -> Result<()> {
    let Some(pattern) = pattern else {
        out.push_str("\\sf: missing required argument\n");
        return Ok(());
    };
    let (_, name_re) = pattern_to_regex(pattern);
    let sql = format!(
        "SELECT pg_catalog.pg_get_functiondef(p.oid) \
         FROM pg_catalog.pg_proc p \
         WHERE p.proname OPERATOR(pg_catalog.~) '{}'",
        lit(&name_re)
    );
    let Some(funcs) = q(session, &sql, out)? else {
        return Ok(());
    };
    if funcs.is_empty() {
        out.push_str(&format!("Did not find any function named \"{pattern}\".\n"));
        return Ok(());
    }
    for f in &funcs {
        out.push_str(f[0].trim_end());
        out.push('\n');
    }
    Ok(())
}
