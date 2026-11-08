pub mod cmdtag;
pub mod copy;
use pg_query::NodeEnum;
pub use pg_query::ParseResult;
use thiserror::Error;

pub mod sqlstate;
pub mod translator;

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("{0}")]
    ParseError(String),
    /// An error that PostgreSQL reports with this SQLSTATE.
    #[error("{message}")]
    Sqlstate {
        sqlstate: &'static str,
        message: String,
    },
    /// A schema-qualified name of an object that the translator accepts only
    /// without a schema. If the schema does not exist, PostgreSQL reports the
    /// missing schema. Only a caller with a connection can find that out.
    #[error("a schema-qualified {kind} name is not supported")]
    SchemaQualifiedName { kind: String, schema: String },
}

/// Parse a PostgreSQL SQL statement using pg_query
pub fn parse(sql: &str) -> Result<ParseResult, ParseError> {
    pg_query::parse(sql).map_err(|e| ParseError::ParseError(e.to_string()))
}

/// Whether the first statement in `parsed` sends rows to the client.
pub fn returns_rows(parsed: &ParseResult) -> bool {
    match first_statement(parsed) {
        Some(NodeEnum::SelectStmt(select)) => select.into_clause.is_none(),
        Some(NodeEnum::InsertStmt(insert)) => !insert.returning_list.is_empty(),
        Some(NodeEnum::UpdateStmt(update)) => !update.returning_list.is_empty(),
        Some(NodeEnum::DeleteStmt(delete)) => !delete.returning_list.is_empty(),
        Some(NodeEnum::MergeStmt(merge)) => !merge.returning_list.is_empty(),
        Some(NodeEnum::ExplainStmt(_) | NodeEnum::VariableShowStmt(_)) => true,
        _ => false,
    }
}

fn first_statement(parsed: &ParseResult) -> Option<&NodeEnum> {
    parsed.protobuf.stmts.first()?.stmt.as_ref()?.node.as_ref()
}

/// Split a multi-statement SQL string into individual statements.
/// Uses pg_query's scanner which correctly handles semicolons inside
/// string literals, comments, and dollar-quoted strings.
/// Returns the individual statement strings (without trailing semicolons).
pub fn split_statements(sql: &str) -> Result<Vec<String>, ParseError> {
    let parts =
        pg_query::split_with_scanner(sql).map_err(|e| ParseError::ParseError(e.to_string()))?;
    Ok(parts
        .into_iter()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect())
}

/// Get tables referenced in a query
pub fn get_tables(sql: &str) -> Result<Vec<String>, ParseError> {
    let result = parse(sql)?;
    Ok(result.tables())
}

/// Normalize a query (replace constants with $1, $2, etc.)
pub fn normalize(sql: &str) -> Result<String, ParseError> {
    pg_query::normalize(sql).map_err(|e| ParseError::ParseError(e.to_string()))
}

/// Get a fingerprint for a query (for caching/deduplication)
pub fn fingerprint(sql: &str) -> Result<String, ParseError> {
    pg_query::fingerprint(sql)
        .map(|fp| fp.hex)
        .map_err(|e| ParseError::ParseError(e.to_string()))
}

/// Quote an identifier following PostgreSQL's server-side quote_identifier()
/// rules: return it bare only when it is all lower-case ASCII letters,
/// digits, and underscores, does not start with a digit, and is not a
/// keyword outside the unreserved category; otherwise wrap it in double
/// quotes with embedded quotes doubled.
pub fn quote_identifier(ident: &str) -> String {
    let safe_chars = !ident.is_empty()
        && !ident.starts_with(|c: char| c.is_ascii_digit())
        && ident
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if safe_chars && !keyword_requires_quoting(ident) {
        return ident.to_string();
    }
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// `sql` with `schema` in front of the table name that starts at byte
/// offset `location`.
pub fn qualify_create_table_name(sql: &str, location: usize, schema: &str) -> String {
    let mut sql = sql.to_string();
    sql.insert_str(location, &format!("{}.", quote_identifier(schema)));
    sql
}

/// `sql` with the index name `name` before the `ON` keyword of a CREATE
/// INDEX statement, which starts at byte offset `location`.
pub fn name_create_index(sql: &str, location: usize, name: &str) -> String {
    let mut sql = sql.to_string();
    sql.insert_str(location, &format!("{} ", quote_identifier(name)));
    sql
}

/// `sql`, a CREATE TABLE statement, with its serial column `column` written
/// out as what `serial` stands for: the integer type, NOT NULL and a
/// DEFAULT of `nextval` of `sequence`, the string the stored text passes to
/// `nextval`. A table is loaded from its stored text, so the text has to
/// name the sequence when the sequence is not named `<table>_<column>_seq`.
pub fn write_out_serial_column(
    sql: &str,
    column: &str,
    sequence: &str,
) -> Result<String, ParseError> {
    use pg_query::protobuf::node::Node;
    use pg_query::NodeRef;

    let parse_result = parse(sql)?;
    let nodes = parse_result.protobuf.nodes();
    let Some((NodeRef::CreateStmt(create), ..)) = nodes.first() else {
        return Err(ParseError::ParseError(
            "not a CREATE TABLE statement".to_string(),
        ));
    };
    let type_name = create
        .table_elts
        .iter()
        .find_map(|element| match &element.node {
            Some(Node::ColumnDef(col_def)) if col_def.colname == column => {
                col_def.type_name.as_ref()
            }
            _ => None,
        })
        .ok_or_else(|| ParseError::ParseError(format!("no column \"{column}\"")))?;
    let type_word = type_name.names.last().and_then(|name| match &name.node {
        Some(Node::String(word)) => Some(word.sval.to_lowercase()),
        _ => None,
    });
    let integer_type = match type_word.as_deref() {
        Some("serial" | "serial4") => "integer",
        Some("bigserial" | "serial8") => "bigint",
        Some("smallserial" | "serial2") => "smallint",
        _ => {
            return Err(ParseError::ParseError(format!(
                "column \"{column}\" is not a serial column"
            )))
        }
    };
    let location = type_name.location as usize;
    let end = qualified_name_end(sql, location)?;
    let sequence = sequence.replace('\'', "''");
    Ok(format!(
        "{}{integer_type} NOT NULL DEFAULT nextval('{sequence}'){}",
        &sql[..location],
        &sql[end..]
    ))
}

/// `sql`, a CREATE TABLE statement, with its table named `name` and no
/// schema. `sql` is returned as it is when the table is already called
/// `name` or when `sql` is not a CREATE TABLE statement.
pub fn name_create_table(sql: &str, name: &str) -> Result<String, ParseError> {
    use pg_query::NodeRef;

    let parse_result = parse(sql)?;
    let nodes = parse_result.protobuf.nodes();
    let Some((NodeRef::CreateStmt(create), ..)) = nodes.first() else {
        return Ok(sql.to_string());
    };
    let Some(relation) = create.relation.as_ref() else {
        return Ok(sql.to_string());
    };
    name_at(sql, relation.location as usize, name)
}

/// The schema, if the statement names one, and the name of the table that
/// a CREATE TABLE statement creates.
pub fn create_table_name(parse_result: &ParseResult) -> Option<(Option<String>, String)> {
    use pg_query::NodeRef;

    let nodes = parse_result.protobuf.nodes();
    let Some((NodeRef::CreateStmt(create), ..)) = nodes.first() else {
        return None;
    };
    let relation = create.relation.as_ref()?;
    Some((
        (!relation.schemaname.is_empty()).then(|| relation.schemaname.clone()),
        relation.relname.clone(),
    ))
}

/// A table that a foreign key of a CREATE TABLE or ALTER TABLE statement
/// refers to: the byte offset of its name in the statement, the schema the
/// name has, if any, and the name itself, as PostgreSQL reads them.
#[derive(Debug, PartialEq, Eq)]
pub struct ForeignKeyTable {
    pub location: usize,
    pub schema: Option<String>,
    pub name: String,
}

/// The tables that the foreign keys of a CREATE TABLE or ALTER TABLE
/// statement refer to, in the order they are written.
pub fn foreign_key_tables(parse_result: &ParseResult) -> Vec<ForeignKeyTable> {
    let mut tables = Vec::new();
    for stmt in &parse_result.protobuf.stmts {
        if let Some(node) = &stmt.stmt {
            collect_foreign_key_tables(node, &mut tables);
        }
    }
    tables.sort_by_key(|table| table.location);
    tables
}

fn collect_foreign_key_tables(node: &pg_query::protobuf::Node, tables: &mut Vec<ForeignKeyTable>) {
    let children: &[pg_query::protobuf::Node] = match &node.node {
        Some(NodeEnum::CreateStmt(create)) => &create.table_elts,
        Some(NodeEnum::AlterTableStmt(alter)) => &alter.cmds,
        Some(NodeEnum::AlterTableCmd(cmd)) => {
            cmd.def.as_deref().map(std::slice::from_ref).unwrap_or(&[])
        }
        Some(NodeEnum::ColumnDef(column)) => &column.constraints,
        Some(NodeEnum::Constraint(constraint)) => {
            if let Some(pktable) = &constraint.pktable {
                tables.push(ForeignKeyTable {
                    location: pktable.location as usize,
                    schema: (!pktable.schemaname.is_empty()).then(|| pktable.schemaname.clone()),
                    name: pktable.relname.clone(),
                });
            }
            &[]
        }
        _ => &[],
    };
    for child in children {
        collect_foreign_key_tables(child, tables);
    }
}

/// `sql` with the name that starts at byte offset `location`, which may
/// have a schema in front of it, replaced by `name` alone.
pub fn name_at(sql: &str, location: usize, name: &str) -> Result<String, ParseError> {
    let end = qualified_name_end(sql, location)?;
    Ok(format!(
        "{}{}{}",
        &sql[..location],
        quote_identifier(name),
        &sql[end..]
    ))
}

/// The byte offset just past the name that starts at byte offset
/// `location` of `sql`, with the schema that may be in front of it.
fn qualified_name_end(sql: &str, location: usize) -> Result<usize, ParseError> {
    use pg_query::protobuf::Token;

    let scan = pg_query::scan(sql).map_err(|e| ParseError::ParseError(e.to_string()))?;
    let mut tokens = scan
        .tokens
        .iter()
        .skip_while(|token| (token.start as usize) < location);
    let first = tokens
        .next()
        .ok_or_else(|| ParseError::ParseError("no name at the location".to_string()))?;
    let mut end = first.end as usize;
    let mut rest = tokens.peekable();
    if rest
        .peek()
        .is_some_and(|token| token.token == Token::Ascii46 as i32)
    {
        rest.next();
        if let Some(ident) = rest.next() {
            end = ident.end as usize;
        }
    }
    Ok(end)
}

/// pg_query's scanner is the same lexer the PostgreSQL server uses, so its
/// keyword classification matches server-side quote_identifier().
fn keyword_requires_quoting(ident: &str) -> bool {
    use pg_query::protobuf::KeywordKind;
    let scan =
        pg_query::scan(ident).expect("scanning a bare lower-case ASCII identifier cannot fail");
    match scan.tokens.as_slice() {
        [token] => !matches!(
            token.keyword_kind(),
            KeywordKind::NoKeyword | KeywordKind::UnreservedKeyword
        ),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returns_rows_is_true_only_for_statements_that_send_rows() {
        let returns = |sql: &str| returns_rows(&parse(sql).unwrap());
        for sql in [
            "/* a comment */ SELECT 1",
            "VALUES (1)",
            "SHOW search_path",
            "EXPLAIN SELECT 1",
            "INSERT INTO t VALUES (1) RETURNING x",
            "DELETE FROM t RETURNING *",
            "SELECT 'LOCK TABLE t'",
        ] {
            assert!(returns(sql), "{sql}");
        }
        for sql in [
            "-- a comment\nSET search_path TO s",
            "SELECT 1 INTO t",
            "INSERT INTO t VALUES (1)",
            "LOCK TABLE t IN ACCESS SHARE MODE",
        ] {
            assert!(!returns(sql), "{sql}");
        }
    }

    #[test]
    fn name_create_table_writes_the_mangled_name_over_the_written_one() {
        assert_eq!(
            name_create_table("CREATE TABLE t (x int)", "app.t").unwrap(),
            "CREATE TABLE \"app.t\" (x int)"
        );
        assert_eq!(
            name_create_table("CREATE TABLE \"app\".t(x int)", "app.t").unwrap(),
            "CREATE TABLE \"app.t\"(x int)"
        );
        assert_eq!(
            name_create_table("create table if not exists app . \"T\" (x int)", "app.T").unwrap(),
            "create table if not exists \"app.T\" (x int)"
        );
        assert_eq!(
            name_create_table("CREATE TABLE t (x int)", "t").unwrap(),
            "CREATE TABLE t (x int)"
        );
        assert_eq!(name_create_table("SELECT 1", "t").unwrap(), "SELECT 1");
    }

    #[test]
    fn name_create_table_drops_the_database_of_a_name_that_is_already_the_mangled_name() {
        assert_eq!(
            name_create_table(
                "CREATE TABLE main.\"__turso_ns1_t\" (x int)",
                "__turso_ns1_t"
            )
            .unwrap(),
            "CREATE TABLE __turso_ns1_t (x int)"
        );
        assert_eq!(
            name_create_table("CREATE TABLE public.t2(a INT)", "t2").unwrap(),
            "CREATE TABLE t2(a INT)"
        );
    }

    #[test]
    fn foreign_key_tables_are_found_with_their_schema_and_location() {
        let sql = "CREATE TABLE t (a int REFERENCES app.p (id), b int, FOREIGN KEY (b) REFERENCES \"Q\"(id), c int REFERENCES other . r)";
        let parse_result = parse(sql).unwrap();
        let tables = foreign_key_tables(&parse_result);
        assert_eq!(
            tables,
            [
                ForeignKeyTable {
                    location: sql.find("app.p").unwrap(),
                    schema: Some("app".to_string()),
                    name: "p".to_string(),
                },
                ForeignKeyTable {
                    location: sql.find("\"Q\"").unwrap(),
                    schema: None,
                    name: "Q".to_string(),
                },
                ForeignKeyTable {
                    location: sql.find("other . r").unwrap(),
                    schema: Some("other".to_string()),
                    name: "r".to_string(),
                },
            ]
        );
        let mut sql = sql.to_string();
        for table in tables.iter().rev() {
            sql = name_at(&sql, table.location, &format!("__turso_ns1_{}", table.name)).unwrap();
        }
        assert_eq!(
            sql,
            "CREATE TABLE t (a int REFERENCES __turso_ns1_p (id), b int, FOREIGN KEY (b) REFERENCES \"__turso_ns1_Q\"(id), c int REFERENCES __turso_ns1_r)"
        );
        assert!(
            foreign_key_tables(
                &parse("ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (a) REFERENCES app.p (id)")
                    .unwrap()
            )
            .len()
                == 1
        );
        assert!(foreign_key_tables(&parse("CREATE TABLE t (a int)").unwrap()).is_empty());
    }

    #[test]
    fn test_parse_simple_select() {
        let sql = "SELECT * FROM users WHERE id = 1";
        let result = parse(sql);
        assert!(result.is_ok());

        let tables = get_tables(sql).unwrap();
        assert_eq!(tables, vec!["users"]);
    }

    #[test]
    fn test_parse_complex_query() {
        let sql = "WITH regional_sales AS (
            SELECT region, SUM(amount) AS total_sales
            FROM orders
            GROUP BY region
        )
        SELECT * FROM regional_sales ORDER BY total_sales DESC";

        assert!(parse(sql).is_ok());
    }

    #[test]
    fn test_normalize() {
        let sql = "SELECT * FROM users WHERE age > 25 AND name = 'John'";
        let normalized = normalize(sql).unwrap();
        assert!(normalized.contains("$1"));
        assert!(normalized.contains("$2"));
    }

    #[test]
    fn test_parse_postgresql_specific() {
        // Test PostgreSQL-specific syntax that we struggled with before
        let queries = vec![
            "SELECT * FROM users ORDER BY name USING >",
            "SELECT * FROM person* p",
            "VALUES (1,2), (3,4)",
            "SELECT foo FROM (SELECT 1) AS foo",
            "INSERT INTO users (name, data) VALUES ('John', '{\"key\": \"value\"}'::jsonb)",
            "SELECT * FROM users WHERE data @> '{\"active\": true}'",
            "UPDATE users SET (name, age) = ('John', 30) WHERE id = 1",
            "CREATE TABLE posts PARTITION OF main_posts FOR VALUES IN (1, 2, 3)",
            "SELECT COUNT(*) FILTER (WHERE active) FROM users",
            "SELECT DISTINCT ON (region) * FROM sales ORDER BY region, amount DESC",
        ];

        for sql in queries {
            let result = parse(sql);
            assert!(result.is_ok(), "Failed to parse: {sql}");
        }
    }
}
