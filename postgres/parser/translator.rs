// PostgreSQL AST to Turso AST translator
//
// This module translates pg_query's PostgreSQL AST into Turso's SQL AST
// representation, handling the semantic differences between PostgreSQL and SQLite.

use crate::ParseError;
use pg_query::protobuf::JoinType as PgJoinType;
use pg_query::{NodeRef, ParseResult};
use turso_parser::ast;
use turso_parser::ast::GroupBy;

/// Result of translating a PostgreSQL command, which may include
/// prerequisite statements (e.g., implicit CREATE SEQUENCE for serial columns).
#[derive(Debug)]
pub struct TranslateResult {
    /// Prerequisite statements that must be executed before the main command.
    /// For example, serial columns generate implicit CREATE SEQUENCE statements.
    pub prereqs: Vec<ast::Stmt>,
    /// The main translated command.
    pub cmd: ast::Cmd,
}

/// Translates a PostgreSQL query into Turso's AST
#[derive(Default)]
pub struct PostgreSQLTranslator {
    translating_index: std::cell::Cell<bool>,
}

impl PostgreSQLTranslator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a `QualifiedName` from a PG `RangeVar`, preserving schema qualifier.
    fn qualified_name_from_range_var(
        &self,
        range_var: &pg_query::protobuf::RangeVar,
    ) -> ast::QualifiedName {
        let is_schemata = range_var
            .schemaname
            .eq_ignore_ascii_case("information_schema")
            && range_var.relname.eq_ignore_ascii_case("schemata");
        let mapped_name = if is_schemata {
            "pg_information_schema_schemata".to_string()
        } else {
            self.map_table_name(&range_var.relname)
        };
        let name = ast::Name::from_string(mapped_name);
        let alias = range_var
            .alias
            .as_ref()
            .filter(|a| !a.aliasname.is_empty())
            .map(|a| ast::Name::from_string(&a.aliasname));
        let schema = if range_var.schemaname.is_empty() {
            is_catalog_table_name(name.as_str()).then_some("main")
        } else {
            Some(map_schema_name(&range_var.schemaname))
        };
        let mut qn = match schema {
            Some(schema) => ast::QualifiedName::fullname(ast::Name::from_string(schema), name),
            None => ast::QualifiedName::single(name),
        };
        qn.alias = alias;
        qn
    }

    /// Maps PostgreSQL system table names to their Turso equivalents.
    /// pg_class, pg_namespace, and pg_attribute have virtual table implementations
    /// and are passed through as-is. Other information_schema names are mapped
    /// to SQLite equivalents.
    fn map_table_name(&self, table_name: &str) -> String {
        match table_name.to_lowercase().as_str() {
            // These have virtual table implementations in pg_catalog.rs - pass through
            "pg_class"
            | "pg_namespace"
            | "pg_attribute"
            | "pg_roles"
            | "pg_am"
            | "pg_database"
            | "pg_proc"
            | "pg_policy"
            | "pg_trigger"
            | "pg_index"
            | "pg_constraint"
            | "pg_statistic_ext"
            | "pg_inherits"
            | "pg_rewrite"
            | "pg_foreign_table"
            | "pg_partitioned_table"
            | "pg_type"
            | "pg_collation"
            | "pg_attrdef"
            | "pg_description"
            | "pg_publication"
            | "pg_publication_namespace"
            | "pg_publication_rel"
            | "pg_get_tabledef"
            | "pg_settings"
            | "pg_extension"
            | "pg_tables"
            | "pg_indexes" => table_name.to_string(),
            "information_schema.tables" => "sqlite_master".to_string(),
            "information_schema.columns" => "pragma_table_info".to_string(),
            // Default: keep original name
            _ => table_name.to_string(),
        }
    }

    /// Translate a PostgreSQL parse result into Turso's format.
    /// For statements that may generate prerequisites (e.g., serial columns),
    /// use `translate_with_prereqs` instead.
    pub fn translate(&self, parse_result: &ParseResult) -> Result<ast::Stmt, ParseError> {
        let translated = self.translate_with_prereqs(parse_result)?;
        let ast::Cmd::Stmt(stmt) = translated.cmd else {
            return Err(ParseError::ParseError(
                "expected a PostgreSQL statement".to_string(),
            ));
        };
        Ok(stmt)
    }

    /// Translate a PostgreSQL parse result, returning both the main command
    /// and any prerequisite statements (e.g., implicit CREATE SEQUENCE for serial columns).
    pub fn translate_with_prereqs(
        &self,
        parse_result: &ParseResult,
    ) -> Result<TranslateResult, ParseError> {
        if parse_result.protobuf.nodes().is_empty() {
            return Err(ParseError::ParseError("No statements found".to_string()));
        }

        let node = &parse_result.protobuf.nodes()[0];

        // CREATE TABLE is special: serial columns generate prerequisite CREATE SEQUENCE stmts
        if let NodeRef::CreateStmt(create) = &node.0 {
            let translated = self.translate_create_table_with_prereqs(create)?;
            return Ok(TranslateResult {
                prereqs: translated.prereqs,
                cmd: translated.cmd,
            });
        }

        let cmd = match node.0 {
            NodeRef::ExplainStmt(explain) => self.translate_explain(explain)?,
            node => ast::Cmd::Stmt(self.translate_node(node)?),
        };

        Ok(TranslateResult {
            prereqs: vec![],
            cmd,
        })
    }

    fn translate_node(&self, node: NodeRef<'_>) -> Result<ast::Stmt, ParseError> {
        Ok(match node {
            NodeRef::SelectStmt(select) => {
                // Top-level SELECT ... INTO is PG's legacy spelling of
                // CREATE TABLE AS.
                if select.into_clause.is_some() {
                    self.translate_select_into(select)?
                } else {
                    let select_ast = self.translate_select(select)?;
                    ast::Stmt::Select(select_ast)
                }
            }
            NodeRef::InsertStmt(insert) => self.translate_insert(insert)?,
            NodeRef::UpdateStmt(update) => self.translate_update(update)?,
            NodeRef::DeleteStmt(delete) => self.translate_delete(delete)?,
            NodeRef::TransactionStmt(txn) => self.translate_transaction(txn)?,
            NodeRef::LockStmt(lock) => self.translate_lock_stub(lock)?,
            NodeRef::DropStmt(drop) => self.translate_drop(drop)?,
            NodeRef::AlterTableStmt(alter) => self.translate_alter_table(alter)?,
            NodeRef::RenameStmt(rename) => self.translate_rename_stmt(rename)?,
            NodeRef::IndexStmt(idx) => self.translate_create_index(idx)?,
            NodeRef::TruncateStmt(truncate) => self.translate_truncate(truncate)?,
            NodeRef::ViewStmt(view) => self.translate_create_view(view)?,
            NodeRef::CreateTableAsStmt(ctas) => self.translate_create_table_as(ctas)?,
            NodeRef::CreateEnumStmt(enum_stmt) => translate_create_enum(enum_stmt)?,
            NodeRef::CreateDomainStmt(domain) => self.translate_create_domain(domain)?,
            NodeRef::CopyStmt(_) => {
                return Err(ParseError::ParseError(
                    "COPY is handled at the postgres frontend layer".to_string(),
                ))
            }
            NodeRef::CreateSeqStmt(seq) => self.translate_create_sequence(seq)?,
            _ => {
                return Err(ParseError::ParseError(format!(
                    "{} is not supported",
                    node_ref_name(&node)
                )))
            }
        })
    }

    fn translate_explain(
        &self,
        explain: &pg_query::protobuf::ExplainStmt,
    ) -> Result<ast::Cmd, ParseError> {
        if !explain.options.is_empty() {
            return Err(ParseError::ParseError(
                "EXPLAIN options are not supported".to_string(),
            ));
        }
        let query = explain
            .query
            .as_ref()
            .and_then(|query| query.node.as_ref())
            .ok_or_else(|| ParseError::ParseError("EXPLAIN missing statement".to_string()))?;
        Ok(ast::Cmd::ExplainQueryPlan {
            stmt: self.translate_node(query.to_ref())?,
            format: ast::EqpFormat::Text,
        })
    }

    fn translate_lock_stub(
        &self,
        lock: &pg_query::protobuf::LockStmt,
    ) -> Result<ast::Stmt, ParseError> {
        if lock.mode != 1 || lock.nowait {
            return Err(ParseError::ParseError(
                "only LOCK IN ACCESS SHARE MODE without NOWAIT is supported".to_string(),
            ));
        }
        Ok(ast::Stmt::Select(ast::Select {
            with: None,
            body: ast::SelectBody {
                select: ast::OneSelect::Select {
                    distinctness: None,
                    columns: vec![ast::ResultColumn::Expr(
                        Box::new(ast::Expr::Literal(ast::Literal::Numeric("0".into()))),
                        None,
                    )],
                    from: None,
                    where_clause: None,
                    group_by: None,
                    window_clause: vec![],
                },
                compounds: vec![],
            },
            order_by: vec![],
            limit: Some(ast::Limit {
                expr: Box::new(ast::Expr::FunctionCall {
                    name: ast::Name::from_string("pg_lock_access_share"),
                    distinctness: None,
                    args: vec![],
                    order_by: vec![],
                    within_group: vec![],
                    filter_over: ast::FunctionTail {
                        filter_clause: None,
                        over_clause: None,
                    },
                }),
                offset: None,
            }),
        }))
    }

    /// Translate a PostgreSQL CREATE TABLE statement into Turso AST.
    /// PG-created tables are always STRICT to enable Turso custom types.
    fn translate_create_table(
        &self,
        create: &pg_query::protobuf::CreateStmt,
    ) -> Result<(ast::Stmt, Vec<String>), ParseError> {
        use pg_query::protobuf::node::Node;
        use pg_query::protobuf::ConstrType;

        let relation = create
            .relation
            .as_ref()
            .ok_or_else(|| ParseError::ParseError("CREATE TABLE missing table name".into()))?;

        let tbl_name = self.qualified_name_from_range_var(relation);

        reject_temporary(relation, "CREATE TEMPORARY TABLE")?;
        // PARTITION OF also records the parent in inh_relations, so check
        // partitioning first for the more accurate message.
        if create.partbound.is_some() || create.partspec.is_some() {
            return Err(not_supported("table partitioning"));
        }
        if !create.inh_relations.is_empty() {
            return Err(not_supported("CREATE TABLE ... INHERITS"));
        }
        if create.of_typename.is_some() {
            return Err(not_supported("CREATE TABLE ... OF type"));
        }
        reject_table_storage_clauses(
            &create.options,
            &create.tablespacename,
            &create.access_method,
        )?;
        // ON COMMIT is only valid on temporary tables, which are rejected
        // above, but check it directly rather than rely on that.
        if matches!(
            pg_query::protobuf::OnCommitAction::try_from(create.oncommit),
            Ok(pg_query::protobuf::OnCommitAction::OncommitDeleteRows
                | pg_query::protobuf::OnCommitAction::OncommitDrop)
        ) {
            return Err(not_supported("ON COMMIT DELETE ROWS / DROP"));
        }

        if create.table_elts.is_empty() {
            return Err(ParseError::ParseError(
                "CREATE TABLE with no columns is not yet supported".into(),
            ));
        }

        let mut columns = Vec::new();
        let mut table_constraints = Vec::new();
        // Collect implicit sequence names for serial columns
        let mut serial_sequences: Vec<String> = Vec::new();

        // Extract table name for serial sequence naming
        let tbl_name_str = tbl_name.name.as_str().to_string();

        // Check for table-level PK (to suppress column-level PK emission)
        let mut has_table_pk = false;
        for elt in &create.table_elts {
            let Some(ref inner) = elt.node else { continue };
            if let Node::Constraint(constraint) = inner {
                let contype =
                    ConstrType::try_from(constraint.contype).unwrap_or(ConstrType::Undefined);
                if contype == ConstrType::ConstrPrimary {
                    has_table_pk = true;
                    break;
                }
            }
        }

        for elt in &create.table_elts {
            let Some(ref inner) = elt.node else { continue };
            match inner {
                Node::ColumnDef(col_def) => {
                    let col = self.translate_create_table_column(
                        col_def,
                        has_table_pk,
                        &tbl_name_str,
                        &relation.schemaname,
                        &mut serial_sequences,
                    )?;
                    columns.push(col);
                }
                Node::Constraint(constraint) => {
                    reject_constraint_attributes(constraint, &relation.schemaname)?;
                    let contype =
                        ConstrType::try_from(constraint.contype).unwrap_or(ConstrType::Undefined);
                    match contype {
                        ConstrType::ConstrPrimary => {
                            let pk_cols = extract_key_columns(&constraint.keys)?;
                            table_constraints.push(ast::NamedTableConstraint {
                                name: None,
                                constraint: ast::TableConstraint::PrimaryKey {
                                    columns: pk_cols
                                        .into_iter()
                                        .map(|c| ast::SortedColumn {
                                            expr: Box::new(ast::Expr::Id(ast::Name::from_string(
                                                c,
                                            ))),
                                            order: None,
                                            nulls: None,
                                        })
                                        .collect(),
                                    auto_increment: false,
                                    conflict_clause: None,
                                },
                            });
                        }
                        ConstrType::ConstrUnique => {
                            let unique_cols = extract_key_columns(&constraint.keys)?;
                            table_constraints.push(ast::NamedTableConstraint {
                                name: None,
                                constraint: ast::TableConstraint::Unique {
                                    columns: unique_cols
                                        .into_iter()
                                        .map(|c| ast::SortedColumn {
                                            expr: Box::new(ast::Expr::Id(ast::Name::from_string(
                                                c,
                                            ))),
                                            order: None,
                                            nulls: None,
                                        })
                                        .collect(),
                                    conflict_clause: None,
                                },
                            });
                        }
                        ConstrType::ConstrForeign => {
                            if let Some(fk) = extract_foreign_key(constraint) {
                                table_constraints.push(
                                    self.pg_fk_to_table_constraint(&fk, &constraint.fk_attrs),
                                );
                            }
                        }
                        ConstrType::ConstrCheck => {
                            if let Some(ref raw_expr) = constraint.raw_expr {
                                let expr = self.translate_expr(raw_expr)?;
                                table_constraints.push(ast::NamedTableConstraint {
                                    name: None,
                                    constraint: ast::TableConstraint::Check {
                                        expr: Box::new(expr),
                                        source: None,
                                    },
                                });
                            }
                        }
                        other => {
                            return Err(not_supported(format!(
                                "table constraint {}",
                                constr_type_name(other)
                            )))
                        }
                    }
                }
                Node::TableLikeClause(_) => {
                    return Err(not_supported("CREATE TABLE ... (LIKE ...)"))
                }
                other => {
                    return Err(not_supported(format!(
                        "CREATE TABLE element {}",
                        node_ref_name(&other.to_ref())
                    )))
                }
            }
        }

        let stmt = ast::Stmt::CreateTable {
            temporary: false,
            if_not_exists: create.if_not_exists,
            tbl_name,
            body: ast::CreateTableBody::ColumnsAndConstraints {
                columns,
                constraints: table_constraints,
                options: ast::TableOptions {
                    without_rowid_text: None,
                    strict_text: Some("STRICT".to_string()),
                },
            },
        };
        Ok((stmt, serial_sequences))
    }

    /// Translate CREATE TABLE, generating prerequisite CREATE SEQUENCE statements
    /// for any serial columns.
    fn translate_create_table_with_prereqs(
        &self,
        create: &pg_query::protobuf::CreateStmt,
    ) -> Result<TranslateResult, ParseError> {
        let (stmt, serial_sequences) = self.translate_create_table(create)?;

        let prereqs = serial_sequences
            .into_iter()
            .map(|seq_name| ast::Stmt::CreateSequence {
                if_not_exists: true,
                seq_name: ast::QualifiedName::single(ast::Name::from_string(seq_name)),
                start: None,
                increment: None,
                min_value: None,
                max_value: None,
                cycle: false,
            })
            .collect();

        Ok(TranslateResult {
            prereqs,
            cmd: ast::Cmd::Stmt(stmt),
        })
    }

    /// Translate a single PG column definition for CREATE TABLE to a Turso AST ColumnDefinition.
    /// Handles SERIAL/BIGSERIAL as NOT NULL + DEFAULT nextval(), column-level PK/FK, and type mapping.
    fn translate_create_table_column(
        &self,
        col_def: &pg_query::protobuf::ColumnDef,
        has_table_pk: bool,
        table_name: &str,
        table_schema: &str,
        serial_sequences: &mut Vec<String>,
    ) -> Result<ast::ColumnDefinition, ParseError> {
        use pg_query::protobuf::node::Node;
        use pg_query::protobuf::ConstrType;

        reject_column_def_clauses(col_def)?;
        let name = col_def.colname.clone();
        let pg_type = extract_type_name(col_def)?;
        let typmods = extract_integer_typmods(col_def)?;

        let is_serial = is_serial_type(&pg_type);

        let mapping = map_column_type(&pg_type, &typmods)?;

        // Serial does NOT imply PRIMARY KEY in PostgreSQL
        let mut is_primary_key = false;
        let mut is_not_null = col_def.is_not_null || is_serial;
        let mut is_unique = false;
        let mut default_expr: Option<ast::Expr> = None;
        let mut foreign_key: Option<PgForeignKey> = None;
        let mut check_constraints = Vec::new();

        for constraint_node in &col_def.constraints {
            let Some(Node::Constraint(constraint)) = &constraint_node.node else {
                continue;
            };
            reject_constraint_attributes(constraint, table_schema)?;
            let contype = ConstrType::try_from(constraint.contype).unwrap_or(ConstrType::Undefined);
            match contype {
                ConstrType::ConstrPrimary => is_primary_key = true,
                ConstrType::ConstrNotnull => is_not_null = true,
                ConstrType::ConstrUnique => is_unique = true,
                ConstrType::ConstrDefault => {
                    if let Some(ref raw_expr) = constraint.raw_expr {
                        default_expr = Some(self.translate_expr(raw_expr)?);
                    }
                }
                ConstrType::ConstrCheck => {
                    if let Some(ref raw_expr) = constraint.raw_expr {
                        check_constraints.push(ast::NamedColumnConstraint {
                            name: if constraint.conname.is_empty() {
                                None
                            } else {
                                Some(ast::Name::from_string(constraint.conname.clone()))
                            },
                            constraint: ast::ColumnConstraint::Check {
                                expr: Box::new(self.translate_expr(raw_expr)?),
                                source: None,
                            },
                        });
                    }
                }
                ConstrType::ConstrForeign => {
                    if foreign_key.is_some() {
                        return Err(not_supported("multiple REFERENCES on one column"));
                    }
                    foreign_key = extract_foreign_key(constraint);
                }
                // NULL, NOT DEFERRABLE and INITIALLY IMMEDIATE restate the defaults.
                ConstrType::ConstrNull
                | ConstrType::ConstrAttrNotDeferrable
                | ConstrType::ConstrAttrImmediate => {}
                other => {
                    return Err(not_supported(format!(
                        "column constraint {}",
                        constr_type_name(other)
                    )))
                }
            }
        }

        // Build the type with size parameters (e.g. varchar(4), numeric(10,2))
        let col_type = if mapping.type_name.is_empty() {
            None
        } else {
            let size = match mapping.type_params.as_slice() {
                [p, s] => Some(ast::TypeSize::TypeSize(
                    Box::new(ast::Expr::Literal(ast::Literal::Numeric(p.to_string()))),
                    Box::new(ast::Expr::Literal(ast::Literal::Numeric(s.to_string()))),
                )),
                [n] => Some(ast::TypeSize::MaxSize(Box::new(ast::Expr::Literal(
                    ast::Literal::Numeric(n.to_string()),
                )))),
                _ => None,
            };
            Some(ast::Type {
                name: mapping.type_name.clone(),
                size,
                array_dimensions: mapping.array_dimensions,
            })
        };

        // Build constraints list
        let mut constraints = Vec::new();

        // For serial columns, add DEFAULT nextval('tablename_colname_seq')
        if is_serial && default_expr.is_none() {
            let seq_name = format!("{}_{}_seq", table_name.to_lowercase(), name.to_lowercase());
            serial_sequences.push(seq_name.clone());
            default_expr = Some(ast::Expr::FunctionCall {
                name: ast::Name::from_string("nextval"),
                distinctness: None,
                args: vec![Box::new(ast::Expr::Literal(ast::Literal::String(format!(
                    "'{seq_name}'"
                ))))],
                order_by: vec![],
                within_group: vec![],
                filter_over: ast::FunctionTail {
                    filter_clause: None,
                    over_clause: None,
                },
            });
        }

        // PRIMARY KEY (only on column level if there's no table-level PK)
        if is_primary_key && !has_table_pk {
            constraints.push(ast::NamedColumnConstraint {
                name: None,
                constraint: ast::ColumnConstraint::PrimaryKey {
                    order: None,
                    conflict_clause: None,
                    auto_increment: false,
                },
            });
        }

        // NOT NULL (don't duplicate if already PK)
        if is_not_null && !is_primary_key {
            constraints.push(ast::NamedColumnConstraint {
                name: None,
                constraint: ast::ColumnConstraint::NotNull {
                    nullable: false,
                    conflict_clause: None,
                },
            });
        }

        // UNIQUE
        if is_unique {
            constraints.push(ast::NamedColumnConstraint {
                name: None,
                constraint: ast::ColumnConstraint::Unique(None),
            });
        }

        // DEFAULT
        if let Some(expr) = default_expr {
            constraints.push(ast::NamedColumnConstraint {
                name: None,
                constraint: ast::ColumnConstraint::Default(Box::new(expr)),
            });
        }

        // REFERENCES (column-level FK)
        if let Some(ref fk) = foreign_key {
            let clause = self.pg_fk_to_fk_clause(fk);
            constraints.push(ast::NamedColumnConstraint {
                name: None,
                constraint: ast::ColumnConstraint::ForeignKey {
                    clause,
                    defer_clause: None,
                },
            });
        }

        constraints.extend(check_constraints);

        Ok(ast::ColumnDefinition {
            col_name: ast::Name::from_string(name),
            col_type,
            constraints,
        })
    }

    /// Convert a PgForeignKey to an AST ForeignKeyClause.
    fn pg_fk_to_fk_clause(&self, fk: &PgForeignKey) -> ast::ForeignKeyClause {
        let mut args = Vec::new();
        if let Some(ref action) = fk.on_delete {
            if let Some(act) = parse_ref_act(action) {
                args.push(ast::RefArg::OnDelete(act));
            }
        }
        if let Some(ref action) = fk.on_update {
            if let Some(act) = parse_ref_act(action) {
                args.push(ast::RefArg::OnUpdate(act));
            }
        }
        ast::ForeignKeyClause {
            tbl_name: ast::Name::from_string(fk.ref_table.clone()),
            columns: fk
                .ref_columns
                .iter()
                .map(|c| ast::IndexedColumn {
                    col_name: ast::Name::from_string(c.clone()),
                    collation_name: None,
                    order: None,
                })
                .collect(),
            args,
        }
    }

    /// Convert a PgForeignKey to a table-level FOREIGN KEY constraint.
    fn pg_fk_to_table_constraint(
        &self,
        fk: &PgForeignKey,
        fk_attr_nodes: &[pg_query::protobuf::Node],
    ) -> ast::NamedTableConstraint {
        use pg_query::protobuf::node::Node;

        let columns: Vec<ast::IndexedColumn> = fk_attr_nodes
            .iter()
            .filter_map(|n| match &n.node {
                Some(Node::String(s)) => Some(ast::IndexedColumn {
                    col_name: ast::Name::from_string(s.sval.clone()),
                    collation_name: None,
                    order: None,
                }),
                _ => None,
            })
            .collect();

        ast::NamedTableConstraint {
            name: None,
            constraint: ast::TableConstraint::ForeignKey {
                columns,
                clause: self.pg_fk_to_fk_clause(fk),
                defer_clause: None,
            },
        }
    }

    fn translate_transaction(
        &self,
        txn: &pg_query::protobuf::TransactionStmt,
    ) -> Result<ast::Stmt, ParseError> {
        use pg_query::protobuf::TransactionStmtKind;

        if txn.chain {
            return Err(not_supported("COMMIT/ROLLBACK AND CHAIN"));
        }
        // Isolation levels and DEFERRABLE are accepted: Turso transactions are
        // serializable, which satisfies every level a client can request.
        // READ ONLY is a restriction that is not enforced, so it must not be
        // silently dropped.
        for opt in &txn.options {
            let Some(pg_query::protobuf::node::Node::DefElem(def)) = &opt.node else {
                return Err(not_supported("transaction option"));
            };
            match def.defname.as_str() {
                "transaction_isolation" | "transaction_deferrable" => {}
                "transaction_read_only" => {
                    // pg_query encodes READ WRITE as integer 0 and READ ONLY as 1.
                    let read_write = matches!(
                        def.arg.as_ref().and_then(|n| n.node.as_ref()),
                        Some(pg_query::protobuf::node::Node::AConst(pg_query::protobuf::AConst {
                            val: Some(pg_query::protobuf::a_const::Val::Ival(i)),
                            ..
                        })) if i.ival == 0
                    );
                    if !read_write {
                        return Err(not_supported("READ ONLY transactions"));
                    }
                }
                other => return Err(not_supported(format!("transaction option {other}"))),
            }
        }

        match TransactionStmtKind::try_from(txn.kind) {
            Ok(TransactionStmtKind::TransStmtBegin | TransactionStmtKind::TransStmtStart) => {
                Ok(ast::Stmt::Begin {
                    typ: None,
                    name: None,
                })
            }
            Ok(TransactionStmtKind::TransStmtCommit) => Ok(ast::Stmt::Commit { name: None }),
            Ok(TransactionStmtKind::TransStmtRollback) => Ok(ast::Stmt::Rollback {
                tx_name: None,
                savepoint_name: None,
            }),
            Ok(TransactionStmtKind::TransStmtSavepoint) => Ok(ast::Stmt::Savepoint {
                name: ast::Name::from_string(&txn.savepoint_name),
            }),
            Ok(TransactionStmtKind::TransStmtRelease) => Ok(ast::Stmt::Release {
                name: ast::Name::from_string(&txn.savepoint_name),
            }),
            Ok(TransactionStmtKind::TransStmtRollbackTo) => Ok(ast::Stmt::Rollback {
                tx_name: None,
                savepoint_name: Some(ast::Name::from_string(&txn.savepoint_name)),
            }),
            _ => Err(ParseError::ParseError(format!(
                "Unsupported transaction statement kind: {}",
                txn.kind
            ))),
        }
    }

    fn translate_alter_table(
        &self,
        alter: &pg_query::protobuf::AlterTableStmt,
    ) -> Result<ast::Stmt, ParseError> {
        use pg_query::protobuf::node::Node;
        use pg_query::protobuf::AlterTableType;

        let relation = alter
            .relation
            .as_ref()
            .ok_or_else(|| ParseError::ParseError("ALTER TABLE missing table name".into()))?;
        let name = self.qualified_name_from_range_var(relation);

        // ALTER INDEX/VIEW/SEQUENCE/... share AlterTableStmt with ALTER TABLE.
        if alter.objtype != pg_query::protobuf::ObjectType::ObjectTable as i32 {
            return Err(not_supported(format!(
                "ALTER {}",
                drop_object_type_name(
                    pg_query::protobuf::ObjectType::try_from(alter.objtype)
                        .unwrap_or(pg_query::protobuf::ObjectType::Undefined)
                )
            )));
        }
        if alter.missing_ok {
            return Err(not_supported("ALTER TABLE IF EXISTS"));
        }
        if alter.cmds.len() > 1 {
            return Err(not_supported("ALTER TABLE with multiple subcommands"));
        }
        let cmd_node = alter
            .cmds
            .first()
            .ok_or_else(|| ParseError::ParseError("ALTER TABLE missing command".into()))?;
        let cmd = match &cmd_node.node {
            Some(Node::AlterTableCmd(cmd)) => cmd,
            _ => {
                return Err(ParseError::ParseError(
                    "ALTER TABLE: unexpected command node".into(),
                ));
            }
        };

        let subtype = AlterTableType::try_from(cmd.subtype)
            .map_err(|_| ParseError::ParseError("ALTER TABLE: invalid subtype".into()))?;
        if cmd.missing_ok {
            return Err(not_supported(format!(
                "ALTER TABLE {} IF [NOT] EXISTS",
                alter_subtype_name(subtype)
            )));
        }
        if cmd.behavior == pg_query::protobuf::DropBehavior::DropCascade as i32 {
            return Err(not_supported(format!(
                "ALTER TABLE {} CASCADE",
                alter_subtype_name(subtype)
            )));
        }

        let body = match subtype {
            AlterTableType::AtAddColumn => {
                let col_def = match &cmd.def {
                    Some(def_node) => match &def_node.node {
                        Some(Node::ColumnDef(cd)) => cd,
                        _ => {
                            return Err(ParseError::ParseError(
                                "ADD COLUMN: expected ColumnDef".into(),
                            ));
                        }
                    },
                    None => {
                        return Err(ParseError::ParseError(
                            "ADD COLUMN: missing column definition".into(),
                        ));
                    }
                };
                let col = self.translate_column_def(col_def, &relation.schemaname)?;
                ast::AlterTableBody::AddColumn(col)
            }
            AlterTableType::AtDropColumn => {
                ast::AlterTableBody::DropColumn(ast::Name::from_string(&cmd.name))
            }
            AlterTableType::AtAlterColumnType => {
                // ALTER TABLE t ALTER COLUMN c TYPE new_type
                // Map to AlterColumn with the new column definition
                let col_def = match &cmd.def {
                    Some(def_node) => match &def_node.node {
                        Some(Node::ColumnDef(cd)) => cd,
                        _ => {
                            return Err(ParseError::ParseError(
                                "ALTER COLUMN TYPE: expected ColumnDef".into(),
                            ));
                        }
                    },
                    None => {
                        return Err(ParseError::ParseError(
                            "ALTER COLUMN TYPE: missing column definition".into(),
                        ));
                    }
                };
                // The USING conversion expression is stored as the raw default.
                if col_def.raw_default.is_some() {
                    return Err(not_supported("ALTER COLUMN TYPE ... USING"));
                }
                let col = self.translate_column_def(col_def, &relation.schemaname)?;
                ast::AlterTableBody::AlterColumn {
                    old: ast::Name::from_string(&cmd.name),
                    new: col,
                }
            }
            AlterTableType::AtColumnDefault => {
                return Err(ParseError::ParseError(
                    "ALTER COLUMN SET/DROP DEFAULT is not supported".into(),
                ));
            }
            AlterTableType::AtDropNotNull => {
                return Err(ParseError::ParseError(
                    "ALTER COLUMN DROP NOT NULL is not supported".into(),
                ));
            }
            AlterTableType::AtSetNotNull => {
                return Err(ParseError::ParseError(
                    "ALTER COLUMN SET NOT NULL is not supported".into(),
                ));
            }
            AlterTableType::AtAddConstraint => {
                return Err(ParseError::ParseError(
                    "ALTER TABLE ADD CONSTRAINT is not supported".into(),
                ));
            }
            _ => {
                return Err(ParseError::ParseError(format!(
                    "ALTER TABLE {} is not supported",
                    alter_subtype_name(subtype)
                )));
            }
        };

        Ok(ast::Stmt::AlterTable(ast::AlterTable { name, body }))
    }

    fn translate_rename_stmt(
        &self,
        rename: &pg_query::protobuf::RenameStmt,
    ) -> Result<ast::Stmt, ParseError> {
        use pg_query::protobuf::ObjectType;

        let rename_type = ObjectType::try_from(rename.rename_type)
            .map_err(|_| ParseError::ParseError("RENAME: invalid type".into()))?;

        let relation = rename
            .relation
            .as_ref()
            .ok_or_else(|| ParseError::ParseError("RENAME missing relation".into()))?;
        let name = self.qualified_name_from_range_var(relation);

        if rename.missing_ok {
            return Err(not_supported("ALTER ... IF EXISTS ... RENAME"));
        }
        // RENAME COLUMN records the kind of relation separately; only tables
        // are handled here.
        if rename_type == ObjectType::ObjectColumn
            && rename.relation_type != ObjectType::ObjectTable as i32
        {
            return Err(not_supported(format!(
                "ALTER {} ... RENAME COLUMN",
                drop_object_type_name(
                    ObjectType::try_from(rename.relation_type).unwrap_or(ObjectType::Undefined)
                )
            )));
        }

        match rename_type {
            ObjectType::ObjectTable => {
                let new_name = ast::Name::from_string(&rename.newname);
                Ok(ast::Stmt::AlterTable(ast::AlterTable {
                    name,
                    body: ast::AlterTableBody::RenameTo(new_name),
                }))
            }
            ObjectType::ObjectColumn => {
                let old = ast::Name::from_string(&rename.subname);
                let new = ast::Name::from_string(&rename.newname);
                Ok(ast::Stmt::AlterTable(ast::AlterTable {
                    name,
                    body: ast::AlterTableBody::RenameColumn { old, new },
                }))
            }
            _ => Err(ParseError::ParseError(format!(
                "Unsupported RENAME type: {rename_type:?}"
            ))),
        }
    }

    /// Translate a PG ColumnDef to a Turso ColumnDefinition AST node.
    fn translate_column_def(
        &self,
        col_def: &pg_query::protobuf::ColumnDef,
        table_schema: &str,
    ) -> Result<ast::ColumnDefinition, ParseError> {
        use pg_query::protobuf::node::Node;
        use pg_query::protobuf::ConstrType;

        let col_name = ast::Name::from_string(&col_def.colname);
        reject_column_def_clauses(col_def)?;

        let pg_type = extract_type_name(col_def)?;
        // Serial columns need the implicit sequence that only CREATE TABLE
        // generates.
        if is_serial_type(&pg_type) {
            return Err(not_supported(format!("{pg_type} in ALTER TABLE")));
        }
        let typmods = extract_integer_typmods(col_def)?;
        let mapping = map_column_type(&pg_type, &typmods)?;
        let size = match mapping.type_params.as_slice() {
            [p, s] => Some(ast::TypeSize::TypeSize(
                Box::new(ast::Expr::Literal(ast::Literal::Numeric(p.to_string()))),
                Box::new(ast::Expr::Literal(ast::Literal::Numeric(s.to_string()))),
            )),
            [n] => Some(ast::TypeSize::MaxSize(Box::new(ast::Expr::Literal(
                ast::Literal::Numeric(n.to_string()),
            )))),
            _ => None,
        };
        let col_type = Some(ast::Type {
            name: mapping.type_name,
            size,
            array_dimensions: mapping.array_dimensions,
        });

        let mut constraints = Vec::new();
        for constraint_node in &col_def.constraints {
            let Some(Node::Constraint(constraint)) = &constraint_node.node else {
                continue;
            };
            reject_constraint_attributes(constraint, table_schema)?;
            let contype = ConstrType::try_from(constraint.contype).unwrap_or(ConstrType::Undefined);
            let constraint_ast = match contype {
                ConstrType::ConstrPrimary => Some(ast::ColumnConstraint::PrimaryKey {
                    order: None,
                    conflict_clause: None,
                    auto_increment: false,
                }),
                ConstrType::ConstrNotnull => Some(ast::ColumnConstraint::NotNull {
                    nullable: false,
                    conflict_clause: None,
                }),
                ConstrType::ConstrUnique => Some(ast::ColumnConstraint::Unique(None)),
                ConstrType::ConstrDefault => match constraint.raw_expr.as_ref() {
                    Some(raw_expr) => Some(ast::ColumnConstraint::Default(Box::new(
                        self.translate_expr(raw_expr)?,
                    ))),
                    None => None,
                },
                ConstrType::ConstrNull
                | ConstrType::ConstrAttrNotDeferrable
                | ConstrType::ConstrAttrImmediate => None,
                other => {
                    return Err(not_supported(format!(
                        "column constraint {} in ALTER TABLE",
                        constr_type_name(other)
                    )))
                }
            };
            if let Some(c) = constraint_ast {
                constraints.push(ast::NamedColumnConstraint {
                    name: None,
                    constraint: c,
                });
            }
        }

        Ok(ast::ColumnDefinition {
            col_name,
            col_type,
            constraints,
        })
    }

    fn translate_create_index(
        &self,
        idx: &pg_query::protobuf::IndexStmt,
    ) -> Result<ast::Stmt, ParseError> {
        let relation = idx
            .relation
            .as_ref()
            .ok_or_else(|| ParseError::ParseError("CREATE INDEX missing table name".into()))?;
        let tbl_name = ast::Name::from_string(self.map_table_name(&relation.relname));

        // PostgreSQL derives a name such as t_a_idx; until that is implemented,
        // an unnamed index would be created with an empty name.
        if idx.idxname.is_empty() {
            return Err(not_supported("CREATE INDEX without an index name"));
        }
        if !idx.access_method.is_empty() && !idx.access_method.eq_ignore_ascii_case("btree") {
            return Err(not_supported(format!(
                "CREATE INDEX ... USING {}",
                idx.access_method
            )));
        }
        if !idx.index_including_params.is_empty() {
            return Err(not_supported("CREATE INDEX ... INCLUDE"));
        }
        if idx.nulls_not_distinct {
            return Err(not_supported("NULLS NOT DISTINCT"));
        }
        reject_table_storage_clauses(&idx.options, &idx.table_space, "")?;
        let idx_name = ast::QualifiedName::single(ast::Name::from_string(&idx.idxname));

        self.translating_index.set(true);
        let index_parts = self.translate_index_parts(idx);
        self.translating_index.set(false);
        let (columns, where_clause) = index_parts?;

        Ok(ast::Stmt::CreateIndex {
            unique: idx.unique,
            if_not_exists: idx.if_not_exists,
            idx_name,
            tbl_name,
            using: None,
            columns,
            with_clause: vec![],
            where_clause,
        })
    }

    fn translate_index_parts(
        &self,
        idx: &pg_query::protobuf::IndexStmt,
    ) -> Result<(Vec<ast::SortedColumn>, Option<Box<ast::Expr>>), ParseError> {
        use pg_query::protobuf::node::Node;

        let mut columns = Vec::new();
        for param_node in &idx.index_params {
            let Some(Node::IndexElem(elem)) = &param_node.node else {
                return Err(ParseError::ParseError(
                    "CREATE INDEX: unexpected index element".into(),
                ));
            };
            reject_index_elem_clauses(elem, "CREATE INDEX")?;
            let expr = if !elem.name.is_empty() {
                Box::new(ast::Expr::Id(ast::Name::from_string(&elem.name)))
            } else if let Some(ref expr_node) = elem.expr {
                Box::new(self.translate_expr(expr_node)?)
            } else {
                return Err(ParseError::ParseError(
                    "CREATE INDEX: index element has no name or expression".into(),
                ));
            };

            let order = match pg_query::protobuf::SortByDir::try_from(elem.ordering) {
                Ok(pg_query::protobuf::SortByDir::SortbyAsc) => Some(ast::SortOrder::Asc),
                Ok(pg_query::protobuf::SortByDir::SortbyDesc) => Some(ast::SortOrder::Desc),
                _ => None,
            };

            let nulls = match pg_query::protobuf::SortByNulls::try_from(elem.nulls_ordering) {
                Ok(pg_query::protobuf::SortByNulls::SortbyNullsFirst) => {
                    Some(ast::NullsOrder::First)
                }
                Ok(pg_query::protobuf::SortByNulls::SortbyNullsLast) => Some(ast::NullsOrder::Last),
                _ => None,
            };

            columns.push(ast::SortedColumn { expr, order, nulls });
        }

        let where_clause = if let Some(ref wc) = idx.where_clause {
            Some(Box::new(self.translate_expr(wc)?))
        } else {
            None
        };

        Ok((columns, where_clause))
    }

    fn translate_drop(&self, drop: &pg_query::protobuf::DropStmt) -> Result<ast::Stmt, ParseError> {
        use pg_query::protobuf::node::Node;
        use pg_query::protobuf::ObjectType;

        let remove_type = ObjectType::try_from(drop.remove_type)
            .map_err(|_| ParseError::ParseError("Invalid object type in DROP".into()))?;

        // Extract the first qualified name from drop.objects
        let obj_node = drop
            .objects
            .first()
            .ok_or_else(|| ParseError::ParseError("DROP missing object name".into()))?;

        let qualified_name = match &obj_node.node {
            Some(Node::List(list)) => {
                let names: Vec<String> = list
                    .items
                    .iter()
                    .filter_map(|item| match &item.node {
                        Some(Node::String(s)) => Some(s.sval.clone()),
                        _ => None,
                    })
                    .collect();
                match names.len() {
                    0 => return Err(ParseError::ParseError("DROP: empty name list".into())),
                    1 => ast::QualifiedName::single(ast::Name::from_string(names[0].clone())),
                    _ => ast::QualifiedName::fullname(
                        ast::Name::from_string(names[0].clone()),
                        ast::Name::from_string(names[1].clone()),
                    ),
                }
            }
            Some(Node::String(s)) => {
                ast::QualifiedName::single(ast::Name::from_string(s.sval.clone()))
            }
            Some(Node::TypeName(tn)) => {
                // DROP TYPE uses TypeName nodes; extract the last name component
                let type_name = tn
                    .names
                    .iter()
                    .filter_map(|n| match &n.node {
                        Some(Node::String(s)) => Some(s.sval.clone()),
                        _ => None,
                    })
                    .next_back()
                    .ok_or_else(|| ParseError::ParseError("DROP TYPE: empty type name".into()))?;
                ast::QualifiedName::single(ast::Name::from_string(type_name))
            }
            _ => {
                return Err(ParseError::ParseError(
                    "DROP: unexpected object format".into(),
                ));
            }
        };

        match remove_type {
            ObjectType::ObjectTable => Ok(ast::Stmt::DropTable {
                if_exists: drop.missing_ok,
                tbl_name: qualified_name,
            }),
            ObjectType::ObjectIndex => Ok(ast::Stmt::DropIndex {
                if_exists: drop.missing_ok,
                idx_name: qualified_name,
            }),
            ObjectType::ObjectView | ObjectType::ObjectMatview => Ok(ast::Stmt::DropView {
                if_exists: drop.missing_ok,
                view_name: qualified_name,
            }),
            ObjectType::ObjectType => Ok(ast::Stmt::DropType {
                if_exists: drop.missing_ok,
                type_name: qualified_name.name.as_str().to_string(),
            }),
            ObjectType::ObjectDomain => Ok(ast::Stmt::DropDomain {
                if_exists: drop.missing_ok,
                domain_name: qualified_name.name.as_str().to_string(),
            }),
            ObjectType::ObjectSequence => Ok(ast::Stmt::DropSequence {
                if_exists: drop.missing_ok,
                seq_name: qualified_name,
            }),
            _ => Err(ParseError::ParseError(format!(
                "DROP {} is not supported",
                drop_object_type_name(remove_type)
            ))),
        }
    }

    fn translate_truncate(
        &self,
        truncate: &pg_query::protobuf::TruncateStmt,
    ) -> Result<ast::Stmt, ParseError> {
        // TRUNCATE TABLE t → DELETE FROM t (first relation only)
        let relation = truncate
            .relations
            .first()
            .and_then(|n| match &n.node {
                Some(pg_query::protobuf::node::Node::RangeVar(rv)) => Some(rv),
                _ => None,
            })
            .ok_or_else(|| ParseError::ParseError("TRUNCATE: missing table name".into()))?;
        let tbl_name = self.qualified_name_from_range_var(relation);
        Ok(ast::Stmt::Delete {
            with: None,
            tbl_name,
            indexed: None,
            where_clause: None,
            returning: vec![],
        })
    }

    fn translate_create_view(
        &self,
        view: &pg_query::protobuf::ViewStmt,
    ) -> Result<ast::Stmt, ParseError> {
        let relation = view
            .view
            .as_ref()
            .ok_or_else(|| ParseError::ParseError("CREATE VIEW: missing view name".into()))?;
        let view_name = self.qualified_name_from_range_var(relation);

        // Translate column aliases
        let columns: Vec<ast::IndexedColumn> = view
            .aliases
            .iter()
            .filter_map(|alias| match &alias.node {
                Some(pg_query::protobuf::node::Node::String(s)) => Some(ast::IndexedColumn {
                    col_name: ast::Name::from_string(&s.sval),
                    collation_name: None,
                    order: None,
                }),
                _ => None,
            })
            .collect();

        // Translate the query
        let query_node = view
            .query
            .as_ref()
            .ok_or_else(|| ParseError::ParseError("CREATE VIEW: missing query".into()))?;
        let select_stmt = match &query_node.node {
            Some(pg_query::protobuf::node::Node::SelectStmt(s)) => s,
            _ => {
                return Err(ParseError::ParseError(
                    "CREATE VIEW: expected SELECT statement".into(),
                ));
            }
        };
        let select = self.translate_select(select_stmt)?;

        Ok(ast::Stmt::CreateView {
            temporary: false,
            if_not_exists: false,
            view_name,
            columns,
            select,
        })
    }

    /// Translate CREATE MATERIALIZED VIEW and CREATE TABLE AS (both parsed
    /// by PG as CreateTableAsStmt, distinguished by objtype).
    fn translate_create_table_as(
        &self,
        ctas: &pg_query::protobuf::CreateTableAsStmt,
    ) -> Result<ast::Stmt, ParseError> {
        use pg_query::protobuf::ObjectType;

        let objtype = ObjectType::try_from(ctas.objtype)
            .map_err(|_| ParseError::ParseError("CREATE TABLE AS: invalid object type".into()))?;

        let label = match objtype {
            ObjectType::ObjectMatview => "CREATE MATERIALIZED VIEW",
            ObjectType::ObjectTable => "CREATE TABLE AS",
            _ => {
                return Err(ParseError::ParseError(
                    "CREATE TABLE AS: unsupported object type".into(),
                ));
            }
        };

        let into_clause = ctas
            .into
            .as_ref()
            .ok_or_else(|| ParseError::ParseError(format!("{label}: missing INTO clause")))?;

        let query_node = ctas
            .query
            .as_ref()
            .ok_or_else(|| ParseError::ParseError(format!("{label}: missing query")))?;
        let select_stmt = match &query_node.node {
            Some(pg_query::protobuf::node::Node::SelectStmt(s)) => s,
            _ => {
                return Err(ParseError::ParseError(format!(
                    "{label}: expected SELECT statement"
                )));
            }
        };

        if objtype == ObjectType::ObjectTable {
            return self.translate_ctas_table(into_clause, select_stmt, ctas.if_not_exists);
        }

        let relation = into_clause.rel.as_ref().ok_or_else(|| {
            ParseError::ParseError("CREATE MATERIALIZED VIEW: missing view name".into())
        })?;
        let view_name = self.qualified_name_from_range_var(relation);

        // Column aliases from the INTO clause
        let columns: Vec<ast::IndexedColumn> = into_clause
            .col_names
            .iter()
            .filter_map(|node| match &node.node {
                Some(pg_query::protobuf::node::Node::String(s)) => Some(ast::IndexedColumn {
                    col_name: ast::Name::from_string(&s.sval),
                    collation_name: None,
                    order: None,
                }),
                _ => None,
            })
            .collect();

        let select = self.translate_select(select_stmt)?;

        Ok(ast::Stmt::CreateMaterializedView {
            if_not_exists: ctas.if_not_exists,
            view_name,
            columns,
            select,
        })
    }

    /// Translate CREATE TABLE AS / SELECT INTO into Turso's
    /// `CREATE TABLE ... AS SELECT`. The new table's schema is derived from
    /// the SELECT by the engine. TEMP is silently ignored (the table is
    /// persistent), like in CREATE TABLE.
    fn translate_ctas_table(
        &self,
        into_clause: &pg_query::protobuf::IntoClause,
        select_stmt: &pg_query::protobuf::SelectStmt,
        if_not_exists: bool,
    ) -> Result<ast::Stmt, ParseError> {
        let relation = into_clause
            .rel
            .as_ref()
            .ok_or_else(|| ParseError::ParseError("CREATE TABLE AS: missing table name".into()))?;
        let tbl_name = self.qualified_name_from_range_var(relation);

        // The engine names the new table's columns after the SELECT's output
        // columns and has no way to override them, so reject an explicit
        // column list rather than silently misname columns.
        if !into_clause.col_names.is_empty() {
            return Err(ParseError::ParseError(
                "CREATE TABLE AS: explicit column list is not supported".into(),
            ));
        }

        let mut select = self.translate_select(select_stmt)?;

        // WITH NO DATA: create the table from the SELECT's shape without
        // copying rows. LIMIT 0 keeps schema derivation intact while
        // guaranteeing an empty result.
        if into_clause.skip_data {
            select.limit = Some(ast::Limit {
                expr: Box::new(ast::Expr::Literal(ast::Literal::Numeric("0".into()))),
                offset: None,
            });
        }

        Ok(ast::Stmt::CreateTable {
            temporary: false,
            if_not_exists,
            tbl_name,
            body: ast::CreateTableBody::AsSelect(select),
        })
    }

    /// Translate top-level `SELECT ... INTO table FROM ...`, which PG parses
    /// as a SelectStmt carrying an IntoClause and treats as CREATE TABLE AS.
    fn translate_select_into(
        &self,
        select: &pg_query::protobuf::SelectStmt,
    ) -> Result<ast::Stmt, ParseError> {
        let into_clause = select
            .into_clause
            .as_ref()
            .expect("caller checked into_clause");
        let mut inner = select.clone();
        inner.into_clause = None;
        self.translate_ctas_table(into_clause, &inner, false)
    }

    fn translate_insert(
        &self,
        insert: &pg_query::protobuf::InsertStmt,
    ) -> Result<ast::Stmt, ParseError> {
        use pg_query::protobuf::node::Node;

        // Extract table name
        let relation = insert
            .relation
            .as_ref()
            .ok_or_else(|| ParseError::ParseError("INSERT missing target table".into()))?;
        let tbl_name = self.qualified_name_from_range_var(relation);

        // Extract column names (empty means "all columns" in positional INSERT)
        let columns: Vec<ast::Name> = insert
            .cols
            .iter()
            .filter_map(|col_node| match &col_node.node {
                Some(Node::ResTarget(res_target)) => Some(ast::Name::from_string(&res_target.name)),
                _ => None,
            })
            .collect();
        let has_explicit_columns = !columns.is_empty();

        // Translate body (VALUES or SELECT)
        let select_stmt_node = insert
            .select_stmt
            .as_ref()
            .ok_or_else(|| ParseError::ParseError("INSERT missing VALUES or SELECT".into()))?;

        let select_stmt = match &select_stmt_node.node {
            Some(Node::SelectStmt(select)) => select,
            _ => {
                return Err(ParseError::ParseError(
                    "INSERT body is not a SelectStmt".into(),
                ));
            }
        };

        // Translate ON CONFLICT clause (upsert)
        let upsert = self.translate_on_conflict(&insert.on_conflict_clause)?;

        let (body, columns) = if !select_stmt.values_lists.is_empty() {
            // VALUES clause — compute which columns are DEFAULT in ALL rows.
            let (rows, default_mask) = self.translate_values(&select_stmt.values_lists)?;
            // Strip columns that use DEFAULT in every row
            let filtered_columns: Vec<ast::Name> = columns
                .into_iter()
                .enumerate()
                .filter(|(i, _)| !default_mask.get(*i).copied().unwrap_or(false))
                .map(|(_, col)| col)
                .collect();
            // If ALL named columns are DEFAULT, use DEFAULT VALUES.
            // But only when the INSERT had explicit column names — an empty
            // column list means "all columns" (positional VALUES), not defaults.
            if filtered_columns.is_empty() && has_explicit_columns {
                (ast::InsertBody::DefaultValues, filtered_columns)
            } else {
                let select = ast::Select {
                    with: None,
                    body: ast::SelectBody {
                        select: ast::OneSelect::Values(rows),
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                };
                (ast::InsertBody::Select(select, upsert), filtered_columns)
            }
        } else {
            // INSERT ... SELECT
            let select = self.translate_select(select_stmt)?;
            (ast::InsertBody::Select(select, upsert), columns)
        };

        let returning = self.translate_returning(&insert.returning_list, relation)?;
        let with = self.translate_with_clause(&insert.with_clause)?;

        Ok(ast::Stmt::Insert {
            with,
            or_conflict: None,
            tbl_name,
            columns,
            body,
            returning,
        })
    }

    /// Translate VALUES lists, returning both the translated rows and a bitmask of
    /// which column positions contain DEFAULT in every row. These positions should
    /// be stripped from the INSERT column list since SQLite doesn't support DEFAULT
    /// in VALUES — omitting the column achieves the same effect.
    /// Columns with DEFAULT in only SOME rows keep the DEFAULT as NULL.
    // The Box<Expr> is required by ast::OneSelect::Values(Vec<Vec<Box<Expr>>>)
    #[allow(clippy::vec_box, clippy::type_complexity)]
    fn translate_values(
        &self,
        values_lists: &[pg_query::protobuf::Node],
    ) -> Result<(Vec<Vec<Box<ast::Expr>>>, Vec<bool>), ParseError> {
        use pg_query::protobuf::node::Node;

        // First pass: compute the default mask across ALL rows.
        // A column is "all defaults" only if every row has DEFAULT for that position.
        let mut default_mask: Vec<bool> = Vec::new();
        for (row_idx, row_node) in values_lists.iter().enumerate() {
            if let Some(Node::List(list)) = &row_node.node {
                if row_idx == 0 {
                    default_mask = list
                        .items
                        .iter()
                        .map(|item| matches!(&item.node, Some(Node::SetToDefault(_))))
                        .collect();
                } else {
                    for (i, item) in list.items.iter().enumerate() {
                        if let Some(mask) = default_mask.get_mut(i) {
                            if !matches!(&item.node, Some(Node::SetToDefault(_))) {
                                *mask = false;
                            }
                        }
                    }
                }
            }
        }

        // Second pass: translate values, filtering out all-default columns.
        // Columns with DEFAULT in only some rows emit NULL (via translate_expr).
        let mut rows = Vec::new();
        for row_node in values_lists.iter() {
            match &row_node.node {
                Some(Node::List(list)) => {
                    let row: Vec<Box<ast::Expr>> = list
                        .items
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| !default_mask.get(*i).copied().unwrap_or(false))
                        .map(|(_, item)| Ok(Box::new(self.translate_expr(item)?)))
                        .collect::<Result<Vec<_>, ParseError>>()?;
                    rows.push(row);
                }
                _ => {
                    return Err(ParseError::ParseError(
                        "Expected list in VALUES clause".into(),
                    ));
                }
            }
        }
        Ok((rows, default_mask))
    }

    fn translate_update(
        &self,
        update: &pg_query::protobuf::UpdateStmt,
    ) -> Result<ast::Stmt, ParseError> {
        use pg_query::protobuf::node::Node;

        // Extract table name
        let relation = update
            .relation
            .as_ref()
            .ok_or_else(|| ParseError::ParseError("UPDATE missing target table".into()))?;
        let tbl_name = self.qualified_name_from_range_var(relation);

        // Extract SET assignments from target_list
        // Each ResTarget has name (column) and val (expression)
        let mut sets = Vec::new();
        for target_node in &update.target_list {
            match &target_node.node {
                Some(Node::ResTarget(res_target)) => {
                    let col_name = ast::Name::from_string(&res_target.name);
                    let expr = if let Some(val) = &res_target.val {
                        Box::new(self.translate_expr(val)?)
                    } else {
                        return Err(ParseError::ParseError(
                            "SET assignment missing value".into(),
                        ));
                    };
                    sets.push(ast::Set {
                        col_names: vec![col_name],
                        expr,
                    });
                }
                _ => {
                    return Err(ParseError::ParseError(
                        "Unexpected node in UPDATE SET clause".into(),
                    ));
                }
            }
        }

        // Translate WHERE clause
        let where_clause = if let Some(where_node) = &update.where_clause {
            Some(Box::new(self.translate_expr(where_node)?))
        } else {
            None
        };

        // Translate FROM clause (PG extension: UPDATE ... FROM ...)
        let from = if !update.from_clause.is_empty() {
            Some(self.translate_from_items(&update.from_clause)?)
        } else {
            None
        };

        let returning = self.translate_returning(&update.returning_list, relation)?;
        let with = self.translate_with_clause(&update.with_clause)?;

        Ok(ast::Stmt::Update(ast::Update {
            with,
            or_conflict: None,
            tbl_name,
            indexed: None,
            sets,
            from,
            where_clause,
            returning,
        }))
    }

    fn translate_delete(
        &self,
        delete: &pg_query::protobuf::DeleteStmt,
    ) -> Result<ast::Stmt, ParseError> {
        // Extract table name
        let relation = delete
            .relation
            .as_ref()
            .ok_or_else(|| ParseError::ParseError("DELETE missing target table".into()))?;
        let tbl_name = self.qualified_name_from_range_var(relation);

        // Translate WHERE clause
        let where_clause = if let Some(where_node) = &delete.where_clause {
            Some(Box::new(self.translate_expr(where_node)?))
        } else {
            None
        };

        let returning = self.translate_returning(&delete.returning_list, relation)?;
        let with = self.translate_with_clause(&delete.with_clause)?;

        Ok(ast::Stmt::Delete {
            with,
            tbl_name,
            indexed: None,
            where_clause,
            returning,
        })
    }

    fn translate_select(
        &self,
        select: &pg_query::protobuf::SelectStmt,
    ) -> Result<ast::Select, ParseError> {
        use pg_query::protobuf::SetOperation;

        // Top-level SELECT INTO is intercepted (and its IntoClause stripped)
        // before translation, so an INTO seen here is in a nested position,
        // which PG rejects during parse analysis.
        if select.into_clause.is_some() {
            return Err(ParseError::ParseError(
                "SELECT ... INTO is not allowed here".into(),
            ));
        }

        // Check if this is a UNION/INTERSECT/EXCEPT (set operation)
        let set_op = select.op();
        if set_op != SetOperation::SetopNone && set_op != SetOperation::Undefined {
            return self.translate_set_operation(select);
        }

        // VALUES clause: standalone VALUES (1,'a'), (2,'b') ...
        if !select.values_lists.is_empty() && select.target_list.is_empty() {
            let mut rows = Vec::new();
            for row_node in &select.values_lists {
                let Some(pg_query::protobuf::node::Node::List(list)) = &row_node.node else {
                    return Err(ParseError::ParseError(
                        "VALUES: expected list node".to_string(),
                    ));
                };
                let mut exprs = Vec::new();
                for item in &list.items {
                    exprs.push(Box::new(self.translate_expr(item)?));
                }
                rows.push(exprs);
            }
            let body = ast::SelectBody {
                select: ast::OneSelect::Values(rows),
                compounds: vec![],
            };
            let order_by = self.translate_order_by(&select.sort_clause)?;
            let limit = self.translate_limit(&select.limit_count, &select.limit_offset)?;
            return Ok(ast::Select {
                with: None,
                body,
                order_by,
                limit,
            });
        }

        // Regular SELECT — translate FROM, columns, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT
        let from_clause = if !select.from_clause.is_empty() {
            Some(self.translate_from_items(&select.from_clause)?)
        } else {
            None
        };

        let target_list = &select.target_list;
        if target_list.is_empty() {
            return Err(ParseError::ParseError(
                "SELECT requires at least one column or expression".to_string(),
            ));
        }

        let result_columns = self.translate_target_list(target_list)?;

        let where_clause = if let Some(where_clause) = &select.where_clause {
            Some(self.translate_expr(where_clause)?)
        } else {
            None
        };

        let order_by = self.translate_order_by(&select.sort_clause)?;

        let distinctness = if !select.distinct_clause.is_empty() {
            Some(ast::Distinctness::Distinct)
        } else {
            None
        };

        let group_by = self.translate_group_by(&select.group_clause, &select.having_clause)?;

        let window_clause = self.translate_window_clause(&select.window_clause)?;

        let limit = self.translate_limit(&select.limit_count, &select.limit_offset)?;

        let one_select = ast::OneSelect::Select {
            distinctness,
            columns: result_columns,
            from: from_clause,
            where_clause: where_clause.map(Box::new),
            group_by,
            window_clause,
        };

        let select_body = ast::SelectBody {
            select: one_select,
            compounds: vec![],
        };

        let with = self.translate_with_clause(&select.with_clause)?;

        Ok(ast::Select {
            with,
            body: select_body,
            order_by,
            limit,
        })
    }

    fn translate_set_operation(
        &self,
        select: &pg_query::protobuf::SelectStmt,
    ) -> Result<ast::Select, ParseError> {
        // Flatten the left-deep tree of set operations into a list.
        // pg_query represents A UNION B UNION C as:
        //   SetOp(SetOp(A, B), C)
        let mut parts: Vec<(
            Option<ast::CompoundOperator>,
            &pg_query::protobuf::SelectStmt,
        )> = Vec::new();
        Self::flatten_set_operation(select, &mut parts);

        if parts.is_empty() {
            return Err(ParseError::ParseError("Empty set operation".to_string()));
        }

        // First part becomes the primary select
        let (_, first_stmt) = &parts[0];
        let first_select = self.translate_one_select(first_stmt)?;

        // Remaining parts become compounds
        let mut compounds = Vec::new();
        for (op, stmt) in parts.iter().skip(1) {
            let operator =
                op.ok_or_else(|| ParseError::ParseError("Missing compound operator".to_string()))?;
            compounds.push(ast::CompoundSelect {
                operator,
                select: self.translate_one_select(stmt)?,
            });
        }

        let order_by = self.translate_order_by(&select.sort_clause)?;
        let limit = self.translate_limit(&select.limit_count, &select.limit_offset)?;

        Ok(ast::Select {
            with: None,
            body: ast::SelectBody {
                select: first_select,
                compounds,
            },
            order_by,
            limit,
        })
    }

    fn flatten_set_operation<'a>(
        stmt: &'a pg_query::protobuf::SelectStmt,
        parts: &mut Vec<(
            Option<ast::CompoundOperator>,
            &'a pg_query::protobuf::SelectStmt,
        )>,
    ) {
        use pg_query::protobuf::SetOperation;

        let set_op = stmt.op();
        if set_op == SetOperation::SetopNone || set_op == SetOperation::Undefined {
            // Leaf select
            parts.push((None, stmt));
            return;
        }

        let operator = match (set_op, stmt.all) {
            (SetOperation::SetopUnion, true) => ast::CompoundOperator::UnionAll,
            (SetOperation::SetopUnion, false) => ast::CompoundOperator::Union,
            (SetOperation::SetopIntersect, _) => ast::CompoundOperator::Intersect,
            (SetOperation::SetopExcept, _) => ast::CompoundOperator::Except,
            _ => return,
        };

        if let Some(larg) = &stmt.larg {
            Self::flatten_set_operation(larg, parts);
        }
        if let Some(rarg) = &stmt.rarg {
            // The first element pushed from rarg gets the operator
            let prev_len = parts.len();
            Self::flatten_set_operation(rarg, parts);
            if parts.len() > prev_len {
                parts[prev_len].0 = Some(operator);
            }
        }
    }

    /// Translate a single leaf SELECT (no set operations) into a OneSelect.
    fn translate_one_select(
        &self,
        select: &pg_query::protobuf::SelectStmt,
    ) -> Result<ast::OneSelect, ParseError> {
        // Compound-select leaves carry their own IntoClause. PG allows INTO
        // only on the first leaf of a top-level compound; reject all of them
        // rather than implement that form.
        if select.into_clause.is_some() {
            return Err(ParseError::ParseError(
                "SELECT ... INTO is not allowed here".into(),
            ));
        }

        let from_clause = if !select.from_clause.is_empty() {
            Some(self.translate_from_items(&select.from_clause)?)
        } else {
            None
        };

        let target_list = &select.target_list;
        if target_list.is_empty() {
            return Err(ParseError::ParseError(
                "SELECT requires at least one column or expression".to_string(),
            ));
        }

        let result_columns = self.translate_target_list(target_list)?;

        let where_clause = if let Some(where_clause) = &select.where_clause {
            Some(self.translate_expr(where_clause)?)
        } else {
            None
        };

        let distinctness = if !select.distinct_clause.is_empty() {
            Some(ast::Distinctness::Distinct)
        } else {
            None
        };

        let group_by = self.translate_group_by(&select.group_clause, &select.having_clause)?;

        let window_clause = self.translate_window_clause(&select.window_clause)?;

        Ok(ast::OneSelect::Select {
            distinctness,
            columns: result_columns,
            from: from_clause,
            where_clause: where_clause.map(Box::new),
            group_by,
            window_clause,
        })
    }

    /// Translate multiple FROM items. The first becomes the primary table,
    /// subsequent items become comma-joins (implicit cross join).
    fn translate_from_items(
        &self,
        from_items: &[pg_query::protobuf::Node],
    ) -> Result<ast::FromClause, ParseError> {
        let mut from_clause = self.translate_from_clause(&from_items[0])?;
        // Additional FROM items are comma-joins (implicit cross join)
        for item in &from_items[1..] {
            let table = match &item.node {
                Some(pg_query::protobuf::node::Node::RangeVar(range_var)) => {
                    self.translate_range_var(range_var)?
                }
                Some(pg_query::protobuf::node::Node::JoinExpr(join_expr)) => {
                    // A JoinExpr as a comma-separated item — flatten its joins
                    let nested = self.translate_join_expr(join_expr)?;
                    if nested.joins.iter().any(join_keeps_unmatched_right_rows) {
                        return Err(ParseError::ParseError(
                            "RIGHT and FULL joins after a comma in FROM are not supported"
                                .to_string(),
                        ));
                    }
                    from_clause.joins.push(ast::JoinedSelectTable {
                        operator: ast::JoinOperator::Comma,
                        table: nested.select,
                        constraint: None,
                    });
                    from_clause.joins.extend(nested.joins);
                    continue;
                }
                Some(pg_query::protobuf::node::Node::RangeFunction(range_func)) => {
                    self.translate_range_function(range_func)?
                }
                Some(pg_query::protobuf::node::Node::RangeSubselect(range_sub)) => {
                    self.translate_range_subselect(range_sub)?
                }
                other => {
                    return Err(ParseError::ParseError(format!(
                        "Unsupported FROM item type: {other:?}"
                    )));
                }
            };
            from_clause.joins.push(ast::JoinedSelectTable {
                operator: ast::JoinOperator::Comma,
                table: Box::new(table),
                constraint: None,
            });
        }
        Ok(from_clause)
    }

    fn translate_from_clause(
        &self,
        from_item: &pg_query::protobuf::Node,
    ) -> Result<ast::FromClause, ParseError> {
        match &from_item.node {
            Some(pg_query::protobuf::node::Node::RangeVar(range_var)) => {
                let select_table = self.translate_range_var(range_var)?;

                Ok(ast::FromClause {
                    select: Box::new(select_table),
                    joins: vec![],
                })
            }
            Some(pg_query::protobuf::node::Node::JoinExpr(join_expr)) => {
                self.translate_join_expr(join_expr)
            }
            Some(pg_query::protobuf::node::Node::RangeSubselect(range_sub)) => {
                let select_table = self.translate_range_subselect(range_sub)?;
                Ok(ast::FromClause {
                    select: Box::new(select_table),
                    joins: vec![],
                })
            }
            Some(pg_query::protobuf::node::Node::RangeFunction(range_func)) => {
                let select_table = self.translate_range_function(range_func)?;
                Ok(ast::FromClause {
                    select: Box::new(select_table),
                    joins: vec![],
                })
            }
            _ => Err(ParseError::ParseError(format!(
                "Unsupported FROM clause type: {:?}",
                from_item.node
            ))),
        }
    }

    fn translate_range_var(
        &self,
        range_var: &pg_query::protobuf::RangeVar,
    ) -> Result<ast::SelectTable, ParseError> {
        let qualified_name = self.qualified_name_from_range_var(range_var);
        let alias = match &range_var.alias {
            Some(a) => Some(a.aliasname.clone()),
            None if qualified_name.name.as_str() != range_var.relname => {
                Some(range_var.relname.clone())
            }
            None => None,
        }
        .map(|name| ast::As::Elided(ast::Name::from_string(name)));

        Ok(ast::SelectTable::Table(qualified_name, alias, None))
    }

    fn translate_range_subselect(
        &self,
        range_sub: &pg_query::protobuf::RangeSubselect,
    ) -> Result<ast::SelectTable, ParseError> {
        let subquery_node = range_sub
            .subquery
            .as_ref()
            .ok_or_else(|| ParseError::ParseError("RangeSubselect missing subquery".to_string()))?;
        let select_stmt = match &subquery_node.node {
            Some(pg_query::protobuf::node::Node::SelectStmt(s)) => s,
            _ => {
                return Err(ParseError::ParseError(
                    "RangeSubselect subquery is not a SelectStmt".to_string(),
                ));
            }
        };
        let mut select = self.translate_select(select_stmt)?;
        if let Some(alias) = &range_sub.alias {
            rename_result_columns(&mut select, &alias_column_names(alias)?)?;
        }
        let alias = range_sub
            .alias
            .as_ref()
            .map(|a| ast::As::Elided(ast::Name::from_string(a.aliasname.clone())));
        Ok(ast::SelectTable::Select {
            select,
            alias,
            lateral: range_sub.lateral,
        })
    }

    fn translate_range_function(
        &self,
        range_func: &pg_query::protobuf::RangeFunction,
    ) -> Result<ast::SelectTable, ParseError> {
        // RangeFunction.functions is a list of function-call items.
        // Each item is a List node whose first element is the FuncCall.
        let func_item = range_func
            .functions
            .first()
            .ok_or_else(|| ParseError::ParseError("RangeFunction has no functions".into()))?;

        let func_call = match &func_item.node {
            Some(pg_query::protobuf::node::Node::List(list)) => {
                // The first element of the list is the FuncCall node
                let first = list
                    .items
                    .first()
                    .ok_or_else(|| ParseError::ParseError("RangeFunction list is empty".into()))?;
                match &first.node {
                    Some(pg_query::protobuf::node::Node::FuncCall(fc)) => fc,
                    _ => {
                        return Err(ParseError::ParseError(
                            "RangeFunction list item is not a FuncCall".into(),
                        ));
                    }
                }
            }
            Some(pg_query::protobuf::node::Node::FuncCall(fc)) => fc,
            _ => {
                return Err(ParseError::ParseError(format!(
                    "Unsupported RangeFunction item: {:?}",
                    func_item.node
                )));
            }
        };

        // Extract function name (use last component to strip schema qualifiers)
        let func_name = func_call
            .funcname
            .iter()
            .filter_map(|node| match &node.node {
                Some(pg_query::protobuf::node::Node::String(s)) => Some(s.sval.as_str()),
                _ => None,
            })
            .next_back()
            .ok_or_else(|| ParseError::ParseError("RangeFunction: missing function name".into()))?;

        // Translate arguments
        let args: Vec<Box<ast::Expr>> = func_call
            .args
            .iter()
            .map(|arg| Ok(Box::new(self.translate_expr(arg)?)))
            .collect::<Result<Vec<_>, ParseError>>()?;

        let alias = range_func
            .alias
            .as_ref()
            .map(|a| ast::As::Elided(ast::Name::from_string(a.aliasname.clone())));

        // PostgreSQL exposes scalar functions in FROM position as one-row,
        // one-column tables (clients issue `SELECT * FROM current_schema()`),
        // but core's TableCall path resolves only table-valued functions, so
        // desugar these zero-argument session functions into a subselect.
        // The TEXT cast keeps the wire column text-typed; a bare subselect
        // column comes back as bytea.
        if args.is_empty() && matches!(func_name, "current_schema" | "current_database" | "version")
        {
            let column_name = match &alias {
                Some(ast::As::As(name) | ast::As::Elided(name)) => name.clone(),
                _ => ast::Name::from_string(func_name),
            };
            let call = ast::Expr::FunctionCall {
                name: ast::Name::from_string(func_name),
                distinctness: None,
                args: vec![],
                order_by: vec![],
                within_group: vec![],
                filter_over: ast::FunctionTail {
                    filter_clause: None,
                    over_clause: None,
                },
            };
            let cast = ast::Expr::Cast {
                expr: Box::new(call),
                type_name: Some(ast::Type {
                    name: "TEXT".to_string(),
                    size: None,
                    array_dimensions: 0,
                }),
            };
            let select = ast::Select {
                with: None,
                body: ast::SelectBody {
                    select: ast::OneSelect::Select {
                        distinctness: None,
                        columns: vec![ast::ResultColumn::Expr(
                            Box::new(cast),
                            Some(ast::As::Elided(column_name)),
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            };
            return Ok(ast::SelectTable::Select {
                select,
                alias,
                lateral: false,
            });
        }

        let name = ast::Name::from_string(func_name);
        let qualified_name = if is_catalog_table_name(func_name) {
            ast::QualifiedName::fullname(ast::Name::from_string("main"), name)
        } else {
            ast::QualifiedName::single(name)
        };
        if func_name == "unnest" {
            if args.len() != 1 || range_func.ordinality || range_func.functions.len() != 1 {
                return Err(ParseError::ParseError(
                    "unnest currently requires one array without WITH ORDINALITY or ROWS FROM"
                        .into(),
                ));
            }
            let column_name = match range_func.alias.as_ref() {
                Some(alias) if alias.colnames.len() > 1 => {
                    return Err(ParseError::ParseError(
                        "unnest has only one output column".into(),
                    ));
                }
                Some(alias) => match alias.colnames.first().and_then(|node| node.node.as_ref()) {
                    Some(pg_query::protobuf::node::Node::String(name)) => name.sval.clone(),
                    None => alias.aliasname.clone(),
                    _ => {
                        return Err(ParseError::ParseError("invalid unnest column alias".into()));
                    }
                },
                None => "unnest".into(),
            };
            return Ok(ast::SelectTable::Select {
                select: ast::Select {
                    with: None,
                    body: ast::SelectBody {
                        select: ast::OneSelect::Select {
                            distinctness: None,
                            columns: vec![ast::ResultColumn::Expr(
                                Box::new(ast::Expr::Id(ast::Name::from_string("unnest"))),
                                Some(ast::As::Elided(ast::Name::from_string(column_name))),
                            )],
                            from: Some(ast::FromClause {
                                select: Box::new(ast::SelectTable::TableCall(
                                    qualified_name,
                                    args,
                                    None,
                                )),
                                joins: vec![],
                            }),
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                },
                alias,
                lateral: true,
            });
        }
        if matches!(
            func_name,
            "json_to_record" | "json_to_recordset" | "jsonb_to_record" | "jsonb_to_recordset"
        ) {
            return self.translate_json_to_record(func_name, range_func, &args, alias);
        }
        // generate_series has a single output column, `value`, so a one-name
        // alias list renames it through a wrapping subselect, as for unnest.
        if func_name == "generate_series" {
            if let Some(range_alias) = &range_func.alias {
                if let [column_name] = alias_column_names(range_alias)?.as_slice() {
                    let mut select = ast::Select {
                        with: None,
                        body: ast::SelectBody {
                            select: ast::OneSelect::Select {
                                distinctness: None,
                                columns: vec![ast::ResultColumn::Expr(
                                    Box::new(ast::Expr::Id(ast::Name::from_string("value"))),
                                    None,
                                )],
                                from: Some(ast::FromClause {
                                    select: Box::new(ast::SelectTable::TableCall(
                                        qualified_name,
                                        args,
                                        None,
                                    )),
                                    joins: vec![],
                                }),
                                where_clause: None,
                                group_by: None,
                                window_clause: vec![],
                            },
                            compounds: vec![],
                        },
                        order_by: vec![],
                        limit: None,
                    };
                    rename_result_columns(&mut select, std::slice::from_ref(column_name))?;
                    return Ok(ast::SelectTable::Select {
                        select,
                        alias,
                        lateral: true,
                    });
                }
            }
        }
        Ok(ast::SelectTable::TableCall(qualified_name, args, alias))
    }

    fn translate_json_to_record(
        &self,
        func_name: &str,
        range_func: &pg_query::protobuf::RangeFunction,
        args: &[Box<ast::Expr>],
        alias: Option<ast::As>,
    ) -> Result<ast::SelectTable, ParseError> {
        if range_func.ordinality || range_func.functions.len() != 1 {
            return Err(ParseError::ParseError(format!(
                "{func_name} does not support WITH ORDINALITY or ROWS FROM"
            )));
        }
        let [arg] = args else {
            return Err(ParseError::ParseError(format!(
                "{func_name} requires exactly one argument"
            )));
        };
        if range_func.coldeflist.is_empty() {
            return Err(ParseError::ParseError(
                "a column definition list is required for functions returning \"record\"".into(),
            ));
        }
        let is_recordset = func_name.ends_with("recordset");
        let (arg_type, other_type_errors, scalar_error) = match func_name {
            "json_to_recordset" => (
                "array",
                vec![("object", format!("cannot call {func_name} on an object"))],
                format!("cannot call {func_name} on a scalar"),
            ),
            "jsonb_to_recordset" => (
                "array",
                vec![],
                format!("cannot call {func_name} on a non-array"),
            ),
            _ => (
                "object",
                vec![("array", "cannot call populate_composite on an array".into())],
                "cannot call populate_composite on a scalar".into(),
            ),
        };
        let json_type = |args: Vec<Box<ast::Expr>>| ast::Expr::FunctionCall {
            name: ast::Name::from_string("json_type"),
            distinctness: None,
            args,
            order_by: vec![],
            within_group: vec![],
            filter_over: ast::FunctionTail {
                filter_clause: None,
                over_clause: None,
            },
        };
        let checked_arg = json_type_or_raise(
            json_type(vec![arg.clone()]),
            (**arg).clone(),
            arg_type,
            &other_type_errors,
            &scalar_error,
        );
        let arg_subquery = ast::SelectTable::Select {
            select: ast::Select {
                with: None,
                body: ast::SelectBody {
                    select: ast::OneSelect::Select {
                        distinctness: None,
                        columns: vec![ast::ResultColumn::Expr(
                            Box::new(checked_arg),
                            Some(ast::As::Elided(ast::Name::from_string("json"))),
                        )],
                        from: None,
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            },
            alias: Some(ast::As::Elided(ast::Name::from_string("arg"))),
            lateral: false,
        };
        let arg_json = ast::Expr::Qualified(
            ast::Name::from_string("arg"),
            ast::Name::from_string("json"),
        );
        let (source, joins) = if is_recordset {
            let json_each = ast::SelectTable::TableCall(
                ast::QualifiedName::single(ast::Name::from_string("json_each")),
                vec![Box::new(arg_json)],
                Some(ast::As::Elided(ast::Name::from_string("element"))),
            );
            let element = |column: &str| {
                ast::Expr::Qualified(
                    ast::Name::from_string("element"),
                    ast::Name::from_string(column),
                )
            };
            (
                json_type_or_raise(
                    element("type"),
                    element("value"),
                    "object",
                    &[],
                    &format!("argument of {func_name} must be an array of objects"),
                ),
                vec![ast::JoinedSelectTable {
                    operator: ast::JoinOperator::Comma,
                    table: Box::new(json_each),
                    constraint: None,
                }],
            )
        } else {
            (arg_json, vec![])
        };
        let columns = range_func
            .coldeflist
            .iter()
            .map(|node| {
                let Some(pg_query::protobuf::node::Node::ColumnDef(col_def)) = &node.node else {
                    return Err(ParseError::ParseError(
                        "invalid column definition list".into(),
                    ));
                };
                let type_name = col_def.type_name.as_ref().ok_or_else(|| {
                    ParseError::ParseError(format!("column {} missing type", col_def.colname))
                })?;
                let is_json = matches!(
                    extract_type_name_from_typename(type_name)?
                        .to_lowercase()
                        .as_str(),
                    "json" | "jsonb"
                );
                let cast_type = pg_type_name_to_ast_type(type_name);
                let path = Box::new(ast::Expr::Literal(ast::Literal::String(format!(
                    "'$.\"{}\"'",
                    col_def
                        .colname
                        .replace('\\', "\\\\")
                        .replace('"', "\\\"")
                        .replace('\'', "''")
                ))));
                let extract =
                    |operator| ast::Expr::Binary(Box::new(source.clone()), operator, path.clone());
                let reads_json_text =
                    is_json || cast_type.as_ref().is_some_and(|t| t.name == "TEXT");
                let value = if reads_json_text {
                    let mut when_then_pairs = vec![(
                        Box::new(ast::Expr::Literal(ast::Literal::String("'null'".into()))),
                        Box::new(ast::Expr::Literal(ast::Literal::Null)),
                    )];
                    if !is_json {
                        when_then_pairs.push((
                            Box::new(ast::Expr::Literal(ast::Literal::String("'text'".into()))),
                            Box::new(extract(ast::Operator::ArrowRightShift)),
                        ));
                    }
                    ast::Expr::Case {
                        base: Some(Box::new(json_type(vec![
                            Box::new(source.clone()),
                            path.clone(),
                        ]))),
                        when_then_pairs,
                        else_expr: Some(Box::new(extract(ast::Operator::ArrowRight))),
                    }
                } else {
                    extract(ast::Operator::ArrowRightShift)
                };
                let cast = ast::Expr::Cast {
                    expr: Box::new(value),
                    type_name: cast_type,
                };
                Ok(ast::ResultColumn::Expr(
                    Box::new(cast),
                    Some(ast::As::Elided(ast::Name::from_string(
                        col_def.colname.clone(),
                    ))),
                ))
            })
            .collect::<Result<Vec<_>, ParseError>>()?;
        Ok(ast::SelectTable::Select {
            select: ast::Select {
                with: None,
                body: ast::SelectBody {
                    select: ast::OneSelect::Select {
                        distinctness: None,
                        columns,
                        from: Some(ast::FromClause {
                            select: Box::new(arg_subquery),
                            joins,
                        }),
                        where_clause: None,
                        group_by: None,
                        window_clause: vec![],
                    },
                    compounds: vec![],
                },
                order_by: vec![],
                limit: None,
            },
            alias: Some(
                alias.unwrap_or_else(|| ast::As::Elided(ast::Name::from_string(func_name))),
            ),
            lateral: true,
        })
    }

    fn translate_join_expr(
        &self,
        join_expr: &pg_query::protobuf::JoinExpr,
    ) -> Result<ast::FromClause, ParseError> {
        // Flatten the left-deep join tree: collect the primary table and all joins
        let mut joins = Vec::new();
        let primary_table = self.flatten_join_tree(join_expr, &mut joins)?;

        Ok(ast::FromClause {
            select: Box::new(primary_table),
            joins,
        })
    }

    fn flatten_join_tree(
        &self,
        join_expr: &pg_query::protobuf::JoinExpr,
        joins: &mut Vec<ast::JoinedSelectTable>,
    ) -> Result<ast::SelectTable, ParseError> {
        // Recursively process the left side
        let primary_table = if let Some(larg) = &join_expr.larg {
            match &larg.node {
                Some(pg_query::protobuf::node::Node::RangeVar(range_var)) => {
                    self.translate_range_var(range_var)?
                }
                Some(pg_query::protobuf::node::Node::JoinExpr(nested_join)) => {
                    self.flatten_join_tree(nested_join, joins)?
                }
                Some(pg_query::protobuf::node::Node::RangeSubselect(range_sub)) => {
                    self.translate_range_subselect(range_sub)?
                }
                Some(pg_query::protobuf::node::Node::RangeFunction(range_func)) => {
                    self.translate_range_function(range_func)?
                }
                _ => {
                    return Err(ParseError::ParseError(format!(
                        "Unsupported left side of JOIN: {:?}",
                        larg.node
                    )));
                }
            }
        } else {
            return Err(ParseError::ParseError(
                "Missing left side of JOIN".to_string(),
            ));
        };

        // Process the right side
        let right_table = if let Some(rarg) = &join_expr.rarg {
            match &rarg.node {
                Some(pg_query::protobuf::node::Node::RangeVar(range_var)) => {
                    self.translate_range_var(range_var)?
                }
                Some(pg_query::protobuf::node::Node::JoinExpr(nested_join)) => {
                    ast::SelectTable::Sub(self.translate_join_expr(nested_join)?, None)
                }
                Some(pg_query::protobuf::node::Node::RangeSubselect(range_sub)) => {
                    self.translate_range_subselect(range_sub)?
                }
                Some(pg_query::protobuf::node::Node::RangeFunction(range_func)) => {
                    self.translate_range_function(range_func)?
                }
                _ => {
                    return Err(ParseError::ParseError(format!(
                        "Unsupported right side of JOIN: {:?}",
                        rarg.node
                    )));
                }
            }
        } else {
            return Err(ParseError::ParseError(
                "Missing right side of JOIN".to_string(),
            ));
        };

        // Map pg_query JoinType to Turso JoinOperator
        let pg_jt = PgJoinType::try_from(join_expr.jointype).unwrap_or(PgJoinType::Undefined);
        let mut jt = match pg_jt {
            PgJoinType::JoinInner => ast::JoinType::INNER,
            PgJoinType::JoinLeft => ast::JoinType::LEFT | ast::JoinType::OUTER,
            PgJoinType::JoinRight => ast::JoinType::RIGHT | ast::JoinType::OUTER,
            PgJoinType::JoinFull => {
                ast::JoinType::LEFT | ast::JoinType::RIGHT | ast::JoinType::OUTER
            }
            _ => ast::JoinType::empty(),
        };
        if join_expr.is_natural {
            jt |= ast::JoinType::NATURAL;
        }
        let operator = ast::JoinOperator::TypedJoin(if jt.is_empty() { None } else { Some(jt) });

        // Translate join constraint (ON / USING)
        let constraint = if let Some(quals) = &join_expr.quals {
            Some(ast::JoinConstraint::On(Box::new(
                self.translate_expr(quals)?,
            )))
        } else if !join_expr.using_clause.is_empty() {
            let names = join_expr
                .using_clause
                .iter()
                .filter_map(|node| match &node.node {
                    Some(pg_query::protobuf::node::Node::String(s)) => {
                        Some(ast::Name::from_string(&s.sval))
                    }
                    _ => None,
                })
                .collect();
            Some(ast::JoinConstraint::Using(names))
        } else {
            None
        };

        joins.push(ast::JoinedSelectTable {
            operator,
            table: Box::new(right_table),
            constraint,
        });

        Ok(primary_table)
    }

    fn translate_target_list(
        &self,
        target_list: &[pg_query::protobuf::Node],
    ) -> Result<Vec<ast::ResultColumn>, ParseError> {
        let mut result_columns = Vec::new();

        for target in target_list {
            match &target.node {
                Some(pg_query::protobuf::node::Node::ResTarget(res_target)) => {
                    if let Some(val) = &res_target.val {
                        // Check if this is a SELECT *
                        if let Some(pg_query::protobuf::node::Node::AStar(_)) = &val.node {
                            result_columns.push(ast::ResultColumn::Star);
                        } else if let Some(pg_query::protobuf::node::Node::ColumnRef(col_ref)) =
                            &val.node
                        {
                            // Check if this is a star reference (* or table.*)
                            if let Some(last) = col_ref.fields.last() {
                                if let Some(pg_query::protobuf::node::Node::AStar(_)) = &last.node {
                                    if col_ref.fields.len() == 1 {
                                        // SELECT *
                                        result_columns.push(ast::ResultColumn::Star);
                                    } else if let Some(first) = col_ref.fields.first() {
                                        // SELECT table.* or alias.*
                                        if let Some(pg_query::protobuf::node::Node::String(s)) =
                                            &first.node
                                        {
                                            result_columns.push(ast::ResultColumn::TableStar(
                                                ast::Name::from_string(&s.sval),
                                            ));
                                        }
                                    }
                                    continue;
                                }
                            }
                            // Regular column reference
                            let expr = self.translate_expr(val)?;
                            let alias: Option<ast::As> = if res_target.name.is_empty() {
                                None
                            } else {
                                Some(ast::As::Elided(ast::Name::from_string(&res_target.name)))
                            };
                            result_columns.push(ast::ResultColumn::Expr(Box::new(expr), alias));
                        } else {
                            let expr = self.translate_expr(val)?;
                            let alias: Option<ast::As> = if res_target.name.is_empty() {
                                Some(match derived_column_name(val) {
                                    Some(name) => ast::As::Elided(ast::Name::from_string(name)),
                                    None => ast::As::ImplicitColumnName(ast::Name::from_string(
                                        "?column?",
                                    )),
                                })
                            } else {
                                Some(ast::As::Elided(ast::Name::from_string(&res_target.name)))
                            };
                            result_columns.push(ast::ResultColumn::Expr(Box::new(expr), alias));
                        }
                    }
                }
                _ => {
                    return Err(ParseError::ParseError(
                        "Unsupported target list item".to_string(),
                    ));
                }
            }
        }

        Ok(result_columns)
    }

    fn translate_expr(&self, node: &pg_query::protobuf::Node) -> Result<ast::Expr, ParseError> {
        match &node.node {
            Some(pg_query::protobuf::node::Node::ColumnRef(col_ref)) => {
                // Extract column name from fields
                if let Some(field) = col_ref.fields.first() {
                    match &field.node {
                        Some(pg_query::protobuf::node::Node::String(s)) => {
                            if col_ref.fields.len() == 1 {
                                // Simple column reference
                                Ok(ast::Expr::Id(ast::Name::from_string(s.sval.clone())))
                            } else {
                                // Qualified column reference (table.column)
                                let mut parts = vec![];
                                for field in &col_ref.fields {
                                    if let Some(pg_query::protobuf::node::Node::String(s)) =
                                        &field.node
                                    {
                                        parts.push(s.sval.clone());
                                    }
                                }
                                match parts.len() {
                                    3 => {
                                        // schema.table.column. The table name is not mapped:
                                        // FROM aliases a renamed table with its original name.
                                        Ok(ast::Expr::DoublyQualified(
                                            ast::Name::from_string(map_schema_name(&parts[0])),
                                            ast::Name::from_string(parts[1].clone()),
                                            ast::Name::from_string(parts[2].clone()),
                                        ))
                                    }
                                    2 => {
                                        // table.column
                                        Ok(ast::Expr::Qualified(
                                            ast::Name::from_string(parts[0].clone()),
                                            ast::Name::from_string(parts[1].clone()),
                                        ))
                                    }
                                    _ => {
                                        Ok(ast::Expr::Id(ast::Name::from_string(parts[0].clone())))
                                    }
                                }
                            }
                        }
                        Some(pg_query::protobuf::node::Node::AStar(_)) => {
                            // SELECT * case - should be handled in translate_target_list, not here
                            Err(ParseError::ParseError(
                                "AStar should be handled in target list, not as expression"
                                    .to_string(),
                            ))
                        }
                        other => Err(ParseError::ParseError(format!(
                            "Invalid column reference, expected String or AStar but got: {other:?}"
                        ))),
                    }
                } else {
                    Err(ParseError::ParseError("Empty column reference".to_string()))
                }
            }
            Some(pg_query::protobuf::node::Node::AConst(a_const)) => self.translate_const(a_const),
            Some(pg_query::protobuf::node::Node::AExpr(a_expr)) => self.translate_a_expr(a_expr),
            Some(pg_query::protobuf::node::Node::BoolExpr(bool_expr)) => {
                self.translate_bool_expr(bool_expr)
            }
            Some(pg_query::protobuf::node::Node::FuncCall(func_call)) => {
                self.translate_func_call(func_call)
            }
            Some(pg_query::protobuf::node::Node::CaseExpr(case_expr)) => {
                self.translate_case_expr(case_expr)
            }
            Some(pg_query::protobuf::node::Node::CollateClause(collate)) => {
                // Strip COLLATE clause, just translate the inner expression
                if let Some(arg) = &collate.arg {
                    self.translate_expr(arg)
                } else {
                    Err(ParseError::ParseError(
                        "COLLATE clause missing inner expression".to_string(),
                    ))
                }
            }
            Some(pg_query::protobuf::node::Node::TypeCast(type_cast)) => {
                let arg = type_cast.arg.as_ref().ok_or_else(|| {
                    ParseError::ParseError("TypeCast missing inner expression".into())
                })?;
                let expr = Box::new(self.translate_expr(arg)?);
                let type_name = type_cast
                    .type_name
                    .as_ref()
                    .and_then(pg_type_name_to_ast_type);
                Ok(ast::Expr::Cast { expr, type_name })
            }
            Some(pg_query::protobuf::node::Node::SubLink(sub_link)) => {
                self.translate_sublink(sub_link)
            }
            Some(pg_query::protobuf::node::Node::NullTest(null_test)) => {
                use pg_query::protobuf::NullTestType;
                let arg = null_test
                    .arg
                    .as_ref()
                    .ok_or_else(|| ParseError::ParseError("NullTest missing arg".to_string()))?;
                let expr = self.translate_operand(arg)?;
                match null_test.nulltesttype() {
                    NullTestType::IsNotNull => Ok(ast::Expr::NotNull(Box::new(expr))),
                    _ => Ok(ast::Expr::IsNull(Box::new(expr))),
                }
            }
            Some(pg_query::protobuf::node::Node::ParamRef(param_ref)) => {
                // $1, $2, etc. — translate to Variable("$N")
                Ok(ast::Expr::Variable(ast::Variable::indexed(
                    std::num::NonZeroU32::new(param_ref.number as u32)
                        .unwrap_or(std::num::NonZeroU32::new(1).unwrap()),
                )))
            }
            Some(pg_query::protobuf::node::Node::BooleanTest(bt)) => {
                use pg_query::protobuf::BoolTestType;
                let arg = bt
                    .arg
                    .as_ref()
                    .ok_or_else(|| ParseError::ParseError("BooleanTest missing arg".into()))?;
                let expr = self.translate_operand(arg)?;
                // Map IS TRUE / IS FALSE / IS NOT TRUE / IS NOT FALSE / IS UNKNOWN / IS NOT UNKNOWN
                match BoolTestType::try_from(bt.booltesttype) {
                    Ok(BoolTestType::IsTrue) => Ok(ast::Expr::Binary(
                        Box::new(expr),
                        ast::Operator::Is,
                        Box::new(ast::Expr::Literal(ast::Literal::Numeric("1".into()))),
                    )),
                    Ok(BoolTestType::IsNotTrue) => Ok(ast::Expr::Binary(
                        Box::new(expr),
                        ast::Operator::IsNot,
                        Box::new(ast::Expr::Literal(ast::Literal::Numeric("1".into()))),
                    )),
                    Ok(BoolTestType::IsFalse) => Ok(ast::Expr::Binary(
                        Box::new(expr),
                        ast::Operator::Is,
                        Box::new(ast::Expr::Literal(ast::Literal::Numeric("0".into()))),
                    )),
                    Ok(BoolTestType::IsNotFalse) => Ok(ast::Expr::Binary(
                        Box::new(expr),
                        ast::Operator::IsNot,
                        Box::new(ast::Expr::Literal(ast::Literal::Numeric("0".into()))),
                    )),
                    Ok(BoolTestType::IsUnknown) => Ok(ast::Expr::IsNull(Box::new(expr))),
                    Ok(BoolTestType::IsNotUnknown) => Ok(ast::Expr::NotNull(Box::new(expr))),
                    _ => Err(ParseError::ParseError(
                        "BooleanTest: unsupported test type".into(),
                    )),
                }
            }
            Some(pg_query::protobuf::node::Node::CoalesceExpr(coalesce)) => {
                let args = coalesce
                    .args
                    .iter()
                    .map(|a| Ok(Box::new(self.translate_expr(a)?)))
                    .collect::<Result<Vec<_>, ParseError>>()?;
                Ok(ast::Expr::FunctionCall {
                    name: ast::Name::from_string("COALESCE"),
                    distinctness: None,
                    args,
                    order_by: vec![],
                    within_group: vec![],
                    filter_over: ast::FunctionTail {
                        filter_clause: None,
                        over_clause: None,
                    },
                })
            }
            Some(pg_query::protobuf::node::Node::MinMaxExpr(minmax)) => {
                use pg_query::protobuf::MinMaxOp;
                let func_name = match MinMaxOp::try_from(minmax.op) {
                    Ok(MinMaxOp::IsGreatest) => "MAX",
                    Ok(MinMaxOp::IsLeast) => "MIN",
                    _ => return Err(ParseError::ParseError("MinMaxExpr: unsupported op".into())),
                };
                let args = minmax
                    .args
                    .iter()
                    .map(|a| Ok(Box::new(self.translate_expr(a)?)))
                    .collect::<Result<Vec<_>, ParseError>>()?;
                Ok(ast::Expr::FunctionCall {
                    name: ast::Name::from_string(func_name),
                    distinctness: None,
                    args,
                    order_by: vec![],
                    within_group: vec![],
                    filter_over: ast::FunctionTail {
                        filter_clause: None,
                        over_clause: None,
                    },
                })
            }
            Some(pg_query::protobuf::node::Node::SqlvalueFunction(svf)) => {
                use pg_query::protobuf::SqlValueFunctionOp;
                match SqlValueFunctionOp::try_from(svf.op) {
                    Ok(SqlValueFunctionOp::SvfopCurrentDate) => {
                        Ok(ast::Expr::Literal(ast::Literal::CurrentDate))
                    }
                    Ok(
                        SqlValueFunctionOp::SvfopCurrentTime
                        | SqlValueFunctionOp::SvfopCurrentTimeN
                        | SqlValueFunctionOp::SvfopLocaltime
                        | SqlValueFunctionOp::SvfopLocaltimeN,
                    ) => Ok(ast::Expr::Literal(ast::Literal::CurrentTime)),
                    Ok(
                        SqlValueFunctionOp::SvfopCurrentTimestamp
                        | SqlValueFunctionOp::SvfopCurrentTimestampN
                        | SqlValueFunctionOp::SvfopLocaltimestamp
                        | SqlValueFunctionOp::SvfopLocaltimestampN,
                    ) => Ok(ast::Expr::Literal(ast::Literal::CurrentTimestamp)),
                    Ok(
                        SqlValueFunctionOp::SvfopCurrentUser
                        | SqlValueFunctionOp::SvfopSessionUser
                        | SqlValueFunctionOp::SvfopUser
                        | SqlValueFunctionOp::SvfopCurrentRole,
                    ) => {
                        // Return empty string stub for user functions
                        Ok(ast::Expr::Literal(ast::Literal::String("''".into())))
                    }
                    // The bare keywords route through the frontend scalars so both
                    // syntaxes share one implementation and agree with pg_catalog.
                    Ok(SqlValueFunctionOp::SvfopCurrentSchema) => Ok(ast::Expr::FunctionCall {
                        name: ast::Name::from_string("current_schema"),
                        distinctness: None,
                        args: vec![],
                        order_by: vec![],
                        within_group: vec![],
                        filter_over: ast::FunctionTail {
                            filter_clause: None,
                            over_clause: None,
                        },
                    }),
                    Ok(SqlValueFunctionOp::SvfopCurrentCatalog) => Ok(ast::Expr::FunctionCall {
                        name: ast::Name::from_string("current_database"),
                        distinctness: None,
                        args: vec![],
                        order_by: vec![],
                        within_group: vec![],
                        filter_over: ast::FunctionTail {
                            filter_clause: None,
                            over_clause: None,
                        },
                    }),
                    _ => Err(ParseError::ParseError(format!(
                        "Unsupported SqlValueFunction op: {}",
                        svf.op
                    ))),
                }
            }
            Some(pg_query::protobuf::node::Node::SetToDefault(_)) => {
                // DEFAULT in a VALUES row — core's resolve_defaults_in_row()
                // replaces Expr::Default with the column's schema default.
                Ok(ast::Expr::Default)
            }
            Some(pg_query::protobuf::node::Node::AArrayExpr(array_expr)) => {
                // Desugar ARRAY[...] into array(...) function call
                let args = array_expr
                    .elements
                    .iter()
                    .map(|e| Ok(Box::new(self.translate_expr(e)?)))
                    .collect::<Result<Vec<_>, ParseError>>()?;
                Ok(ast::Expr::FunctionCall {
                    name: ast::Name::from_string("array"),
                    distinctness: None,
                    args,
                    order_by: vec![],
                    within_group: vec![],
                    filter_over: ast::FunctionTail {
                        filter_clause: None,
                        over_clause: None,
                    },
                })
            }
            Some(pg_query::protobuf::node::Node::AIndirection(indirection)) => {
                let arg = indirection
                    .arg
                    .as_ref()
                    .ok_or_else(|| ParseError::ParseError("AIndirection missing arg".into()))?;
                let mut expr = self.translate_expr(arg)?;
                // Each indirection step is either an index (AIndices) or a field name (String)
                for step in &indirection.indirection {
                    match &step.node {
                        Some(pg_query::protobuf::node::Node::AIndices(indices)) => {
                            if indices.is_slice {
                                // Slice: expr[start:end] → array_slice(expr, start, end)
                                let start = indices.lidx.as_ref().ok_or_else(|| {
                                    ParseError::ParseError("Array slice missing lower index".into())
                                })?;
                                let end = indices.uidx.as_ref().ok_or_else(|| {
                                    ParseError::ParseError("Array slice missing upper index".into())
                                })?;
                                let start_expr = self.translate_expr(start)?;
                                let end_expr = self.translate_expr(end)?;
                                expr = ast::Expr::FunctionCall {
                                    name: ast::Name::from_string("array_slice"),
                                    distinctness: None,
                                    args: vec![
                                        Box::new(expr),
                                        Box::new(start_expr),
                                        Box::new(end_expr),
                                    ],
                                    order_by: vec![],
                                    within_group: vec![],
                                    filter_over: ast::FunctionTail {
                                        filter_clause: None,
                                        over_clause: None,
                                    },
                                };
                            } else {
                                // Subscript: expr[index] → array_element(expr, index)
                                let index_node = indices.uidx.as_ref().ok_or_else(|| {
                                    ParseError::ParseError("Array subscript missing index".into())
                                })?;
                                let index_expr = self.translate_expr(index_node)?;
                                expr = ast::Expr::FunctionCall {
                                    name: ast::Name::from_string("array_element"),
                                    distinctness: None,
                                    args: vec![Box::new(expr), Box::new(index_expr)],
                                    order_by: vec![],
                                    within_group: vec![],
                                    filter_over: ast::FunctionTail {
                                        filter_clause: None,
                                        over_clause: None,
                                    },
                                };
                            }
                        }
                        Some(pg_query::protobuf::node::Node::String(s)) => {
                            // Field access: expr.field — treat as qualified name
                            expr = ast::Expr::Qualified(
                                match expr {
                                    ast::Expr::Id(name) => name,
                                    _ => {
                                        return Err(ParseError::ParseError(
                                            "Field access on non-identifier expression".into(),
                                        ))
                                    }
                                },
                                ast::Name::from_string(s.sval.clone()),
                            );
                        }
                        other => {
                            return Err(ParseError::ParseError(format!(
                                "Unsupported indirection step: {other:?}"
                            )));
                        }
                    }
                }
                Ok(expr)
            }
            Some(pg_query::protobuf::node::Node::AStar(_)) => {
                // SELECT * - this should be handled as ResultColumn::Star in translate_target_list
                Err(ParseError::ParseError(
                    "AStar should not be translated as expression".to_string(),
                ))
            }
            _ => Err(ParseError::ParseError(format!(
                "Unsupported expression type: {:?}",
                node.node
            ))),
        }
    }

    fn translate_const(
        &self,
        a_const: &pg_query::protobuf::AConst,
    ) -> Result<ast::Expr, ParseError> {
        if a_const.isnull {
            return Ok(ast::Expr::Literal(ast::Literal::Null));
        }
        if let Some(val) = &a_const.val {
            match val {
                pg_query::protobuf::a_const::Val::Ival(i) => Ok(ast::Expr::Literal(
                    ast::Literal::Numeric(i.ival.to_string()),
                )),
                pg_query::protobuf::a_const::Val::Sval(s) => {
                    // Turso's AST expects string literals to include surrounding single quotes
                    // (sanitize_string strips them during bytecode emission)
                    let quoted = format!("'{}'", s.sval.replace('\'', "''"));
                    Ok(ast::Expr::Literal(ast::Literal::String(quoted)))
                }
                pg_query::protobuf::a_const::Val::Fval(f) => {
                    Ok(ast::Expr::Literal(ast::Literal::Numeric(f.fval.clone())))
                }
                pg_query::protobuf::a_const::Val::Boolval(b) => {
                    // SQLite uses 0/1 for booleans
                    Ok(ast::Expr::Literal(ast::Literal::Numeric(
                        if b.boolval { "1" } else { "0" }.to_string(),
                    )))
                }
                _ => Err(ParseError::ParseError(
                    "Unsupported constant type".to_string(),
                )),
            }
        } else {
            Err(ParseError::ParseError("Empty constant value".to_string()))
        }
    }

    fn translate_a_expr(
        &self,
        a_expr: &pg_query::protobuf::AExpr,
    ) -> Result<ast::Expr, ParseError> {
        use pg_query::protobuf::AExprKind;

        match &a_expr.kind() {
            AExprKind::AexprOp => {
                // Regular binary operators
                self.translate_binary_expr(a_expr)
            }
            AExprKind::AexprIn => {
                // IN operator
                self.translate_in_expr(a_expr)
            }
            AExprKind::AexprLike | AExprKind::AexprIlike => self.translate_like_expr(a_expr),
            AExprKind::AexprOpAny => self.translate_any_all_expr(a_expr, false),
            AExprKind::AexprOpAll => self.translate_any_all_expr(a_expr, true),
            AExprKind::AexprBetween | AExprKind::AexprBetweenSym => {
                self.translate_between_expr(a_expr, false)
            }
            AExprKind::AexprNotBetween | AExprKind::AexprNotBetweenSym => {
                self.translate_between_expr(a_expr, true)
            }
            AExprKind::AexprDistinct => {
                // IS DISTINCT FROM → lhs IS NOT rhs
                let lhs = a_expr
                    .lexpr
                    .as_ref()
                    .ok_or_else(|| ParseError::ParseError("DISTINCT FROM: missing lhs".into()))?;
                let rhs = a_expr
                    .rexpr
                    .as_ref()
                    .ok_or_else(|| ParseError::ParseError("DISTINCT FROM: missing rhs".into()))?;
                Ok(ast::Expr::Binary(
                    Box::new(self.translate_operand(lhs)?),
                    ast::Operator::IsNot,
                    Box::new(self.translate_operand(rhs)?),
                ))
            }
            AExprKind::AexprNotDistinct => {
                // IS NOT DISTINCT FROM → lhs IS rhs
                let lhs = a_expr.lexpr.as_ref().ok_or_else(|| {
                    ParseError::ParseError("NOT DISTINCT FROM: missing lhs".into())
                })?;
                let rhs = a_expr.rexpr.as_ref().ok_or_else(|| {
                    ParseError::ParseError("NOT DISTINCT FROM: missing rhs".into())
                })?;
                Ok(ast::Expr::Binary(
                    Box::new(self.translate_operand(lhs)?),
                    ast::Operator::Is,
                    Box::new(self.translate_operand(rhs)?),
                ))
            }
            AExprKind::AexprNullif => {
                // NULLIF(a, b) → function call
                let mut args = Vec::new();
                if let Some(lexpr) = &a_expr.lexpr {
                    args.push(Box::new(self.translate_expr(lexpr)?));
                }
                if let Some(rexpr) = &a_expr.rexpr {
                    args.push(Box::new(self.translate_expr(rexpr)?));
                }
                Ok(ast::Expr::FunctionCall {
                    name: ast::Name::from_string("NULLIF"),
                    distinctness: None,
                    args,
                    order_by: vec![],
                    within_group: vec![],
                    filter_over: ast::FunctionTail {
                        filter_clause: None,
                        over_clause: None,
                    },
                })
            }
            AExprKind::AexprSimilar => {
                // SIMILAR TO → convert pattern to regex and use REGEXP
                // pg_query wraps the rhs in similar_to_escape(pattern[, escape_char])
                let op_name = a_expr
                    .name
                    .first()
                    .and_then(|n| match &n.node {
                        Some(pg_query::protobuf::node::Node::String(s)) => Some(s.sval.as_str()),
                        _ => None,
                    })
                    .unwrap_or("~");
                let not = op_name == "!~";

                let lhs = a_expr
                    .lexpr
                    .as_ref()
                    .ok_or_else(|| ParseError::ParseError("SIMILAR TO: missing lhs".into()))?;

                // The rhs is wrapped in a FuncCall to similar_to_escape(pattern).
                // Extract the actual pattern from inside that wrapper.
                let rhs_node = a_expr
                    .rexpr
                    .as_ref()
                    .ok_or_else(|| ParseError::ParseError("SIMILAR TO: missing rhs".into()))?;

                let pattern_node = match &rhs_node.node {
                    Some(pg_query::protobuf::node::Node::FuncCall(fc)) => {
                        // Extract the first argument (the pattern) from similar_to_escape()
                        fc.args.first().unwrap_or(rhs_node)
                    }
                    _ => rhs_node,
                };

                let rhs_expr = self.translate_operand(pattern_node)?;
                let regex_rhs = match rhs_expr {
                    ast::Expr::Literal(ast::Literal::String(ref pat)) => {
                        // String literals in our AST include surrounding quotes
                        let unquoted = pat
                            .strip_prefix('\'')
                            .and_then(|s| s.strip_suffix('\''))
                            .unwrap_or(pat);
                        let regex = similar_to_regex(unquoted);
                        // Re-wrap with quotes for the AST
                        ast::Expr::Literal(ast::Literal::String(format!("'{regex}'")))
                    }
                    other => other,
                };

                Ok(ast::Expr::Like {
                    lhs: Box::new(self.translate_operand(lhs)?),
                    not,
                    op: ast::LikeOperator::Regexp,
                    rhs: Box::new(regex_rhs),
                    escape: None,
                })
            }
            _ => Err(ParseError::ParseError(format!(
                "Unsupported AExpr kind: {:?}",
                a_expr.kind()
            ))),
        }
    }

    fn translate_binary_expr(
        &self,
        a_expr: &pg_query::protobuf::AExpr,
    ) -> Result<ast::Expr, ParseError> {
        // Extract operator name — use the last String node to handle
        // schema-qualified operators like OPERATOR(pg_catalog.~)
        let op_name = a_expr
            .name
            .iter()
            .rev()
            .find_map(|name| match &name.node {
                Some(pg_query::protobuf::node::Node::String(s)) => Some(s.sval.as_str()),
                _ => None,
            })
            .ok_or_else(|| ParseError::ParseError("Missing operator name".to_string()))?;

        // Handle unary operators (no left expression)
        if a_expr.lexpr.is_none() {
            let rhs = a_expr
                .rexpr
                .as_ref()
                .ok_or_else(|| ParseError::ParseError("Missing right expression".into()))?;
            let operand = self.translate_operand(rhs)?;
            return match op_name {
                "+" => Ok(ast::Expr::Unary(
                    ast::UnaryOperator::Positive,
                    Box::new(operand),
                )),
                "-" => Ok(ast::Expr::Unary(
                    ast::UnaryOperator::Negative,
                    Box::new(operand),
                )),
                "~" => Ok(ast::Expr::Unary(
                    ast::UnaryOperator::BitwiseNot,
                    Box::new(operand),
                )),
                _ => Err(ParseError::ParseError(format!(
                    "Unsupported unary operator: {op_name}"
                ))),
            };
        }

        // Translate left and right expressions
        let left = Box::new(self.translate_operand(a_expr.lexpr.as_ref().unwrap())?);

        let right = if let Some(rexpr) = &a_expr.rexpr {
            Box::new(self.translate_operand(rexpr)?)
        } else {
            return Err(ParseError::ParseError(
                "Missing right expression".to_string(),
            ));
        };

        let right = if matches!(op_name, "~*" | "!~*") {
            Box::new(ast::Expr::Binary(
                Box::new(ast::Expr::Literal(ast::Literal::String(
                    "'(?i)'".to_string(),
                ))),
                ast::Operator::Concat,
                right,
            ))
        } else {
            right
        };

        // Handle regex operators (~, !~) which map to REGEXP expressions
        match op_name {
            "~" => {
                return Ok(ast::Expr::Like {
                    lhs: left,
                    not: false,
                    op: ast::LikeOperator::Regexp,
                    rhs: right,
                    escape: None,
                });
            }
            "!~" => {
                return Ok(ast::Expr::Like {
                    lhs: left,
                    not: true,
                    op: ast::LikeOperator::Regexp,
                    rhs: right,
                    escape: None,
                });
            }
            "~*" => {
                return Ok(ast::Expr::Like {
                    lhs: left,
                    not: false,
                    op: ast::LikeOperator::Regexp,
                    rhs: right,
                    escape: None,
                });
            }
            "!~*" => {
                return Ok(ast::Expr::Like {
                    lhs: left,
                    not: true,
                    op: ast::LikeOperator::Regexp,
                    rhs: right,
                    escape: None,
                });
            }
            _ => {}
        }

        // Map PostgreSQL operators to Turso operators
        let binary_op = match op_name {
            "=" => ast::Operator::Equals,
            "!=" | "<>" => ast::Operator::NotEquals,
            "<" => ast::Operator::Less,
            "<=" => ast::Operator::LessEquals,
            ">" => ast::Operator::Greater,
            ">=" => ast::Operator::GreaterEquals,
            "+" => ast::Operator::Add,
            "-" => ast::Operator::Subtract,
            "*" => ast::Operator::Multiply,
            "/" => ast::Operator::Divide,
            "%" => ast::Operator::Modulus,
            "||" => ast::Operator::Concat,
            "->" => ast::Operator::ArrowRight,
            "->>" => ast::Operator::ArrowRightShift,
            "&" => ast::Operator::BitwiseAnd,
            "|" => ast::Operator::BitwiseOr,
            "<<" => ast::Operator::LeftShift,
            ">>" => ast::Operator::RightShift,
            "AND" => ast::Operator::And,
            "OR" => ast::Operator::Or,
            // Array operators → translate to native array function calls
            "@>" | "<@" | "&&" => {
                let (func_name, args) = match op_name {
                    "@>" => ("array_contains_all", vec![left, right]),
                    "<@" => ("array_contains_all", vec![right, left]), // swap: b contains all of a
                    "&&" => ("array_overlap", vec![left, right]),
                    _ => unreachable!(),
                };
                return Ok(ast::Expr::FunctionCall {
                    name: ast::Name::from_string(func_name),
                    distinctness: None,
                    args,
                    order_by: vec![],
                    within_group: vec![],
                    filter_over: ast::FunctionTail {
                        filter_clause: None,
                        over_clause: None,
                    },
                });
            }
            _ => {
                return Err(ParseError::ParseError(format!(
                    "Unsupported operator: {op_name}"
                )));
            }
        };

        Ok(ast::Expr::Binary(left, binary_op, right))
    }

    fn translate_in_expr(
        &self,
        a_expr: &pg_query::protobuf::AExpr,
    ) -> Result<ast::Expr, ParseError> {
        // Get the left expression (the column/expression being tested)
        let lhs = if let Some(lexpr) = &a_expr.lexpr {
            Box::new(self.translate_operand(lexpr)?)
        } else {
            return Err(ParseError::ParseError(
                "Missing left expression for IN operator".to_string(),
            ));
        };

        // Get the right expression (should be a list)
        let rhs = if let Some(rexpr) = &a_expr.rexpr {
            match &rexpr.node {
                Some(pg_query::protobuf::node::Node::List(list)) => {
                    let mut values = Vec::new();
                    for item in &list.items {
                        values.push(Box::new(self.translate_expr(item)?));
                    }
                    values
                }
                _ => {
                    return Err(ParseError::ParseError(
                        "Expected list for IN operator right side".to_string(),
                    ));
                }
            }
        } else {
            return Err(ParseError::ParseError(
                "Missing right expression for IN operator".to_string(),
            ));
        };

        // Check if it's NOT IN
        let not = a_expr
            .name
            .first()
            .and_then(|name| name.node.as_ref())
            .map(|node| matches!(node, pg_query::protobuf::node::Node::String(s) if s.sval == "<>"))
            .unwrap_or(false);

        Ok(ast::Expr::InList { lhs, not, rhs })
    }

    /// Translate `<lhs> <op> ANY(<array>)` / `<lhs> <op> ALL(<array>)`.
    /// Only the membership forms are supported: `= ANY` (IN) and `<> ALL`
    /// (NOT IN); other operators error as not supported. A literal ARRAY[...]
    /// operand lowers to IN/NOT IN. Any other operand (bound parameter, column,
    /// expression) lowers to array_contains(array, elem). `= ANY(<subquery>)`
    /// parses as a SubLink and lowers to InSelect, so it never reaches here.
    fn translate_any_all_expr(
        &self,
        a_expr: &pg_query::protobuf::AExpr,
        is_all: bool,
    ) -> Result<ast::Expr, ParseError> {
        let kw = if is_all { "ALL" } else { "ANY" };
        let op_name = a_expr
            .name
            .first()
            .and_then(|n| match &n.node {
                Some(pg_query::protobuf::node::Node::String(s)) => Some(s.sval.as_str()),
                _ => None,
            })
            .ok_or_else(|| ParseError::ParseError(format!("{kw}: missing operator name")))?;

        // The PG lexer rewrites != to <>.
        let not = match (op_name, is_all) {
            ("=", false) => false,
            ("<>", true) => true,
            _ => {
                return Err(ParseError::ParseError(format!(
                    "{op_name} {kw} (array) is not supported"
                )));
            }
        };

        let lhs_node = a_expr
            .lexpr
            .as_ref()
            .ok_or_else(|| ParseError::ParseError(format!("{kw}: missing lhs")))?;
        let rhs_node = a_expr
            .rexpr
            .as_ref()
            .ok_or_else(|| ParseError::ParseError(format!("{kw}: missing rhs")))?;
        let lhs = self.translate_operand(lhs_node)?;

        if let Some(pg_query::protobuf::node::Node::AArrayExpr(arr)) = &rhs_node.node {
            let rhs = arr
                .elements
                .iter()
                .map(|e| Ok(Box::new(self.translate_expr(e)?)))
                .collect::<Result<Vec<_>, ParseError>>()?;
            return Ok(ast::Expr::InList {
                lhs: Box::new(lhs),
                not,
                rhs,
            });
        }

        let array = self.translate_expr(rhs_node)?;
        let call = ast::Expr::FunctionCall {
            name: ast::Name::from_string("array_contains"),
            distinctness: None,
            args: vec![Box::new(array), Box::new(lhs)],
            order_by: vec![],
            within_group: vec![],
            filter_over: ast::FunctionTail {
                filter_clause: None,
                over_clause: None,
            },
        };
        Ok(if not {
            ast::Expr::Unary(ast::UnaryOperator::Not, Box::new(call))
        } else {
            call
        })
    }

    fn translate_like_expr(
        &self,
        a_expr: &pg_query::protobuf::AExpr,
    ) -> Result<ast::Expr, ParseError> {
        let op_name = a_expr
            .name
            .first()
            .and_then(|n| match &n.node {
                Some(pg_query::protobuf::node::Node::String(s)) => Some(s.sval.as_str()),
                _ => None,
            })
            .ok_or_else(|| ParseError::ParseError("Missing LIKE operator name".to_string()))?;

        let (not, ignore_case) = match op_name {
            "~~" => (false, false),
            "!~~" => (true, false),
            "~~*" => (false, true),
            "!~~*" => (true, true),
            _ => {
                return Err(ParseError::ParseError(format!(
                    "Unsupported LIKE operator: {op_name}"
                )));
            }
        };

        let Some(lexpr) = &a_expr.lexpr else {
            return Err(ParseError::ParseError(
                "Missing left expression for LIKE operator".to_string(),
            ));
        };
        let Some(rexpr) = &a_expr.rexpr else {
            return Err(ParseError::ParseError(
                "Missing right expression for LIKE operator".to_string(),
            ));
        };
        let fold_case = |expr: ast::Expr| {
            if ignore_case {
                function_call("lower", vec![expr])
            } else {
                expr
            }
        };
        let lhs = fold_case(self.translate_operand(lexpr)?);

        // A literal pattern becomes GLOB, a core operator that also works in
        // index expressions, where frontend functions such as textlike() cannot.
        if let Some(pg_query::protobuf::node::Node::AConst(pg_query::protobuf::AConst {
            val: Some(pg_query::protobuf::a_const::Val::Sval(pattern)),
            ..
        })) = &rexpr.node
        {
            if let Some(glob) = like_pattern_to_glob(&pattern.sval) {
                return Ok(ast::Expr::Like {
                    lhs: Box::new(lhs),
                    not,
                    op: ast::LikeOperator::Glob,
                    rhs: Box::new(fold_case(ast::Expr::Literal(ast::Literal::String(
                        format!("'{}'", glob.replace('\'', "''")),
                    )))),
                    escape: None,
                });
            }
        }

        if self.translating_index.get() {
            return Err(ParseError::ParseError(
                "CREATE INDEX supports LIKE and ILIKE only with a string-literal pattern that does not use ESCAPE or end with the escape character".to_string(),
            ));
        }

        // SQLite's LIKE ignores case and has no default escape character, so
        // other PostgreSQL LIKE patterns run through the frontend's textlike() function.
        let call = function_call(
            "textlike",
            vec![lhs, fold_case(self.translate_operand(rexpr)?)],
        );
        Ok(if not {
            ast::Expr::Unary(ast::UnaryOperator::Not, Box::new(call))
        } else {
            call
        })
    }

    fn translate_between_expr(
        &self,
        a_expr: &pg_query::protobuf::AExpr,
        not: bool,
    ) -> Result<ast::Expr, ParseError> {
        // BETWEEN: lexpr is the test expression, rexpr is a List of [low, high]
        let lhs = a_expr
            .lexpr
            .as_ref()
            .ok_or_else(|| ParseError::ParseError("BETWEEN: missing test expression".into()))?;
        let lhs = self.translate_operand(lhs)?;

        let rexpr = a_expr
            .rexpr
            .as_ref()
            .ok_or_else(|| ParseError::ParseError("BETWEEN: missing range".into()))?;
        let items = match &rexpr.node {
            Some(pg_query::protobuf::node::Node::List(list)) => &list.items,
            _ => {
                return Err(ParseError::ParseError(
                    "BETWEEN: expected list for range".into(),
                ));
            }
        };
        if items.len() != 2 {
            return Err(ParseError::ParseError(
                "BETWEEN: expected exactly 2 range bounds".into(),
            ));
        }
        let start = self.translate_operand(&items[0])?;
        let end = self.translate_operand(&items[1])?;

        Ok(ast::Expr::Between {
            lhs: Box::new(lhs),
            not,
            start: Box::new(start),
            end: Box::new(end),
        })
    }

    fn translate_func_call(
        &self,
        func_call: &pg_query::protobuf::FuncCall,
    ) -> Result<ast::Expr, ParseError> {
        // Extract function name
        let func_name = func_call
            .funcname
            .iter()
            .filter_map(|node| {
                if let Some(pg_query::protobuf::node::Node::String(s)) = &node.node {
                    Some(s.sval.clone())
                } else {
                    None
                }
            })
            .next_back()
            .ok_or_else(|| ParseError::ParseError("Missing function name".to_string()))?;

        // Translate FILTER clause
        let filter_clause = if let Some(ref filter) = func_call.agg_filter {
            Some(Box::new(self.translate_expr(filter)?))
        } else {
            None
        };

        // Translate OVER clause (window function)
        let over_clause = if let Some(ref window_def) = func_call.over {
            Some(Box::new(self.translate_window_def(window_def)?))
        } else {
            None
        };

        let filter_over = ast::FunctionTail {
            filter_clause,
            over_clause,
        };

        // COUNT(*) and similar aggregate star calls
        if func_call.agg_star {
            return Ok(ast::Expr::FunctionCallStar {
                name: ast::Name::from_string(func_name),
                filter_over,
            });
        }

        // Translate function arguments
        let args = func_call
            .args
            .iter()
            .map(|arg| Ok(Box::new(self.translate_expr(arg)?)))
            .collect::<Result<Vec<_>, ParseError>>()?;

        let distinctness = if func_call.agg_distinct {
            Some(ast::Distinctness::Distinct)
        } else {
            None
        };

        // Strip pg_catalog. schema prefix from function names
        let func_name = func_name
            .strip_prefix("pg_catalog.")
            .unwrap_or(&func_name)
            .to_string();

        // Rewrite PG-specific function-name spellings to their core-native
        // equivalents so the core resolver doesn't need to carry aliases
        // that only matter when a query came through the PG parser. This
        // keeps core's resolve_function focused on functions Turso itself
        // implements; PG-name routing lives here in the translator.
        //
        // `position(needle IN haystack)` is parsed by libpg_query into a
        // regular function call with operands already in `(haystack, needle)`
        // order — the same shape SQLite's `instr` and PostgreSQL's `strpos`
        // take — so the rewrite is purely a name change.
        let func_name = match func_name.as_str() {
            "position" => "strpos".to_string(),
            _ => func_name,
        };

        // PG type-cast functions: float8(x) → CAST(x AS REAL), int4(x) → CAST(x AS INTEGER), etc.
        let cast_type = match func_name.to_uppercase().as_str() {
            "FLOAT8" | "FLOAT4" => Some("REAL"),
            "INT4" | "INT2" | "INT8" => Some("INTEGER"),
            "BOOL" => Some("BOOLEAN"),
            "TEXT" => Some("TEXT"),
            _ => None,
        };
        if let Some(type_name) = cast_type {
            if args.len() == 1 {
                return Ok(ast::Expr::Cast {
                    expr: args.into_iter().next().unwrap(),
                    type_name: Some(ast::Type {
                        name: type_name.to_string(),
                        size: None,
                        array_dimensions: 0,
                    }),
                });
            }
        }

        let translated_order = self.translate_order_by(&func_call.agg_order)?;
        let (order_by, within_group) = if func_call.agg_within_group {
            let within_group = translated_order
                .into_iter()
                .map(|column| ast::SortedColumn {
                    nulls: None,
                    ..column
                })
                .collect();
            (vec![], within_group)
        } else {
            (translated_order, vec![])
        };

        Ok(ast::Expr::FunctionCall {
            name: ast::Name::from_string(func_name),
            distinctness,
            args,
            order_by,
            within_group,
            filter_over,
        })
    }

    /// Translate the WINDOW clause (named window definitions) from a SELECT statement.
    fn translate_window_clause(
        &self,
        window_clause: &[pg_query::protobuf::Node],
    ) -> Result<Vec<ast::WindowDef>, ParseError> {
        use pg_query::protobuf::node::Node;

        window_clause
            .iter()
            .map(|node| {
                let wd = match &node.node {
                    Some(Node::WindowDef(wd)) => wd,
                    _ => {
                        return Err(ParseError::ParseError(
                            "WINDOW clause: expected WindowDef node".into(),
                        ));
                    }
                };
                let window = self.translate_window_spec(wd)?;
                Ok(ast::WindowDef {
                    name: ast::Name::from_string(&wd.name),
                    window,
                })
            })
            .collect()
    }

    /// Translate a WindowDef used as an OVER clause on a function call.
    /// If the WindowDef is just a reference to a named window (OVER w),
    /// returns Over::Name. Otherwise returns Over::Window with the full spec.
    fn translate_window_def(
        &self,
        window_def: &pg_query::protobuf::WindowDef,
    ) -> Result<ast::Over, ParseError> {
        // When pg_query parses `OVER window_name`, the WindowDef has:
        // - name = "window_name" (the referenced name)
        // - partition_clause = []
        // - order_clause = []
        // If name is set and there's no inline spec, it's a reference.
        if !window_def.name.is_empty()
            && window_def.partition_clause.is_empty()
            && window_def.order_clause.is_empty()
        {
            return Ok(ast::Over::Name(ast::Name::from_string(&window_def.name)));
        }

        let window = self.translate_window_spec(window_def)?;
        Ok(ast::Over::Window(window))
    }

    /// Translate a WindowDef protobuf into an ast::Window (shared by both
    /// WINDOW clause definitions and inline OVER specifications).
    fn translate_window_spec(
        &self,
        window_def: &pg_query::protobuf::WindowDef,
    ) -> Result<ast::Window, ParseError> {
        // Base window reference (for window inheritance)
        let base = if !window_def.refname.is_empty() {
            Some(ast::Name::from_string(&window_def.refname))
        } else {
            None
        };

        // PARTITION BY
        let partition_by: Vec<Box<ast::Expr>> = window_def
            .partition_clause
            .iter()
            .map(|node| Ok(Box::new(self.translate_expr(node)?)))
            .collect::<Result<Vec<_>, ParseError>>()?;

        // ORDER BY
        let order_by = self.translate_order_by(&window_def.order_clause)?;

        // Frame clause from frame_options bitmask
        let frame_clause = self.translate_frame_options(
            window_def.frame_options,
            &window_def.start_offset,
            &window_def.end_offset,
        )?;

        Ok(ast::Window {
            base,
            partition_by,
            order_by,
            frame_clause,
        })
    }

    fn translate_frame_options(
        &self,
        frame_options: i32,
        start_offset: &Option<Box<pg_query::protobuf::Node>>,
        end_offset: &Option<Box<pg_query::protobuf::Node>>,
    ) -> Result<Option<ast::FrameClause>, ParseError> {
        const NONDEFAULT: i32 = 0x00001;
        // RANGE = 0x00002 is the default mode (else branch)
        const ROWS: i32 = 0x00004;
        const GROUPS: i32 = 0x00008;
        const BETWEEN: i32 = 0x00010;
        const START_UNBOUNDED_PRECEDING: i32 = 0x00020;
        const END_UNBOUNDED_FOLLOWING: i32 = 0x00100;
        const START_CURRENT_ROW: i32 = 0x00200;
        const END_CURRENT_ROW: i32 = 0x00400;
        const START_OFFSET_PRECEDING: i32 = 0x00800;
        const END_OFFSET_PRECEDING: i32 = 0x01000;
        const START_OFFSET_FOLLOWING: i32 = 0x02000;
        const END_OFFSET_FOLLOWING: i32 = 0x04000;
        const EXCLUDE_CURRENT_ROW: i32 = 0x08000;
        const EXCLUDE_GROUP: i32 = 0x10000;
        const EXCLUDE_TIES: i32 = 0x20000;

        // If not explicitly specified, use default (no frame clause in AST)
        if frame_options & NONDEFAULT == 0 {
            return Ok(None);
        }

        let mode = if frame_options & ROWS != 0 {
            ast::FrameMode::Rows
        } else if frame_options & GROUPS != 0 {
            ast::FrameMode::Groups
        } else {
            ast::FrameMode::Range
        };

        let start = if frame_options & START_UNBOUNDED_PRECEDING != 0 {
            ast::FrameBound::UnboundedPreceding
        } else if frame_options & START_CURRENT_ROW != 0 {
            ast::FrameBound::CurrentRow
        } else if frame_options & START_OFFSET_PRECEDING != 0 {
            let expr = start_offset
                .as_ref()
                .map(|n| self.translate_expr(n))
                .transpose()?
                .unwrap_or_else(|| ast::Expr::Literal(ast::Literal::Numeric("1".into())));
            ast::FrameBound::Preceding(Box::new(expr))
        } else if frame_options & START_OFFSET_FOLLOWING != 0 {
            let expr = start_offset
                .as_ref()
                .map(|n| self.translate_expr(n))
                .transpose()?
                .unwrap_or_else(|| ast::Expr::Literal(ast::Literal::Numeric("1".into())));
            ast::FrameBound::Following(Box::new(expr))
        } else {
            ast::FrameBound::UnboundedPreceding
        };

        let end = if frame_options & BETWEEN != 0 {
            let end_bound = if frame_options & END_UNBOUNDED_FOLLOWING != 0 {
                ast::FrameBound::UnboundedFollowing
            } else if frame_options & END_CURRENT_ROW != 0 {
                ast::FrameBound::CurrentRow
            } else if frame_options & END_OFFSET_PRECEDING != 0 {
                let expr = end_offset
                    .as_ref()
                    .map(|n| self.translate_expr(n))
                    .transpose()?
                    .unwrap_or_else(|| ast::Expr::Literal(ast::Literal::Numeric("1".into())));
                ast::FrameBound::Preceding(Box::new(expr))
            } else if frame_options & END_OFFSET_FOLLOWING != 0 {
                let expr = end_offset
                    .as_ref()
                    .map(|n| self.translate_expr(n))
                    .transpose()?
                    .unwrap_or_else(|| ast::Expr::Literal(ast::Literal::Numeric("1".into())));
                ast::FrameBound::Following(Box::new(expr))
            } else {
                ast::FrameBound::CurrentRow
            };
            Some(end_bound)
        } else {
            None
        };

        let exclude = if frame_options & EXCLUDE_CURRENT_ROW != 0 {
            Some(ast::FrameExclude::CurrentRow)
        } else if frame_options & EXCLUDE_GROUP != 0 {
            Some(ast::FrameExclude::Group)
        } else if frame_options & EXCLUDE_TIES != 0 {
            Some(ast::FrameExclude::Ties)
        } else {
            None
        };

        Ok(Some(ast::FrameClause {
            mode,
            start,
            end,
            exclude,
        }))
    }

    fn translate_case_expr(
        &self,
        case_expr: &pg_query::protobuf::CaseExpr,
    ) -> Result<ast::Expr, ParseError> {
        // Translate optional base expression (CASE <expr> WHEN ...)
        let base = if let Some(arg) = &case_expr.arg {
            Some(Box::new(self.translate_expr(arg)?))
        } else {
            None
        };

        // Translate WHEN/THEN pairs
        let mut when_then_pairs = Vec::new();
        for arg in &case_expr.args {
            match &arg.node {
                Some(pg_query::protobuf::node::Node::CaseWhen(case_when)) => {
                    let when_expr = if let Some(expr) = &case_when.expr {
                        Box::new(self.translate_expr(expr)?)
                    } else {
                        return Err(ParseError::ParseError(
                            "CASE WHEN missing condition".to_string(),
                        ));
                    };
                    let then_expr = if let Some(result) = &case_when.result {
                        Box::new(self.translate_expr(result)?)
                    } else {
                        return Err(ParseError::ParseError(
                            "CASE WHEN missing THEN result".to_string(),
                        ));
                    };
                    when_then_pairs.push((when_expr, then_expr));
                }
                _ => {
                    return Err(ParseError::ParseError(
                        "Expected CaseWhen node in CASE expression".to_string(),
                    ));
                }
            }
        }

        // Translate optional ELSE expression
        let else_expr = if let Some(defresult) = &case_expr.defresult {
            Some(Box::new(self.translate_expr(defresult)?))
        } else {
            None
        };

        Ok(ast::Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        })
    }

    fn translate_group_by(
        &self,
        group_clause: &[pg_query::protobuf::Node],
        having_clause: &Option<Box<pg_query::protobuf::Node>>,
    ) -> Result<Option<GroupBy>, ParseError> {
        if group_clause.is_empty() && having_clause.is_none() {
            return Ok(None);
        }

        let exprs: Vec<Box<ast::Expr>> = group_clause
            .iter()
            .map(|node| Ok(Box::new(self.translate_expr(node)?)))
            .collect::<Result<Vec<_>, ParseError>>()?;

        let having = if let Some(having_node) = having_clause {
            Some(Box::new(self.translate_expr(having_node)?))
        } else {
            None
        };

        Ok(Some(GroupBy { exprs, having }))
    }

    fn translate_sublink(
        &self,
        sub_link: &pg_query::protobuf::SubLink,
    ) -> Result<ast::Expr, ParseError> {
        use pg_query::protobuf::SubLinkType;

        let subselect_node = sub_link
            .subselect
            .as_ref()
            .ok_or_else(|| ParseError::ParseError("SubLink missing subselect".to_string()))?;
        let select_stmt = match &subselect_node.node {
            Some(pg_query::protobuf::node::Node::SelectStmt(s)) => s,
            _ => {
                return Err(ParseError::ParseError(
                    "SubLink subselect is not a SelectStmt".to_string(),
                ));
            }
        };
        let select = Box::new(self.translate_select(select_stmt)?);

        match sub_link.sub_link_type() {
            SubLinkType::ExistsSublink => Ok(ast::Expr::Exists(select)),
            SubLinkType::ExprSublink => Ok(ast::Expr::Subquery(select)),
            SubLinkType::ArraySublink => Ok(Self::translate_array_subquery(*select)),
            SubLinkType::AnySublink => {
                // ANY/IN subquery: testexpr IN (SELECT ...)
                let test_node = sub_link.testexpr.as_ref().ok_or_else(|| {
                    ParseError::ParseError("ANY SubLink missing testexpr".to_string())
                })?;
                let lhs = self.translate_operand(test_node)?;
                Ok(ast::Expr::InSelect {
                    lhs: Box::new(lhs),
                    not: false,
                    rhs: select,
                })
            }
            other => Err(ParseError::ParseError(format!(
                "Unsupported SubLink type: {other:?}",
            ))),
        }
    }

    fn translate_array_subquery(select: ast::Select) -> ast::Expr {
        // PostgreSQL: ARRAY(SELECT v FROM items ORDER BY v LIMIT 2)
        // Becomes this scalar subquery in the Turso AST:
        // (
        //     WITH _turso_array_rows(_turso_array_value) AS MATERIALIZED (
        //         SELECT v FROM items ORDER BY v LIMIT 2
        //     )
        //     SELECT coalesce(array_agg(_turso_array_value), array())
        //     FROM _turso_array_rows
        // )
        // array() constructs an empty array when the query returns no rows.
        let query = select.to_string().to_ascii_lowercase();
        let mut table_name = "_turso_array_rows".to_owned();
        while query.contains(&table_name) {
            table_name.push('_');
        }
        let table = ast::Name::from_string(table_name);
        let column = ast::Name::from_string("_turso_array_value");
        let call = |name: &str, args: Vec<Box<ast::Expr>>| ast::Expr::FunctionCall {
            name: ast::Name::from_string(name),
            distinctness: None,
            args,
            order_by: vec![],
            within_group: vec![],
            filter_over: ast::FunctionTail {
                filter_clause: None,
                over_clause: None,
            },
        };

        ast::Expr::Subquery(Box::new(ast::Select {
            with: Some(ast::With {
                recursive: false,
                ctes: vec![ast::CommonTableExpr {
                    tbl_name: table.clone(),
                    columns: vec![ast::IndexedColumn {
                        col_name: column.clone(),
                        collation_name: None,
                        order: None,
                    }],
                    materialized: ast::Materialized::Yes,
                    select,
                }],
            }),
            body: ast::SelectBody {
                select: ast::OneSelect::Select {
                    distinctness: None,
                    columns: vec![ast::ResultColumn::Expr(
                        Box::new(call(
                            "coalesce",
                            vec![
                                Box::new(call("array_agg", vec![Box::new(ast::Expr::Id(column))])),
                                Box::new(call("array", vec![])),
                            ],
                        )),
                        None,
                    )],
                    from: Some(ast::FromClause {
                        select: Box::new(ast::SelectTable::Table(
                            ast::QualifiedName::single(table),
                            None,
                            None,
                        )),
                        joins: vec![],
                    }),
                    where_clause: None,
                    group_by: None,
                    window_clause: vec![],
                },
                compounds: vec![],
            },
            order_by: vec![],
            limit: None,
        }))
    }

    fn translate_with_clause(
        &self,
        with_clause: &Option<pg_query::protobuf::WithClause>,
    ) -> Result<Option<ast::With>, ParseError> {
        use pg_query::protobuf::node::Node;

        let with_clause = match with_clause {
            Some(w) => w,
            None => return Ok(None),
        };

        let mut ctes = Vec::new();
        for cte_node in &with_clause.ctes {
            let cte = match &cte_node.node {
                Some(Node::CommonTableExpr(cte)) => cte,
                _ => {
                    return Err(ParseError::ParseError(
                        "Expected CommonTableExpr in WITH clause".into(),
                    ));
                }
            };

            let tbl_name = ast::Name::from_string(&cte.ctename);

            // PostgreSQL evaluates SEARCH/CYCLE by adding computed columns to
            // the recursion; silently dropping them would change results (and
            // a CYCLE clause is often what makes the query terminate at all).
            if cte.search_clause.is_some() {
                return Err(ParseError::ParseError(
                    "SEARCH clause on a common table expression is not supported".into(),
                ));
            }
            if cte.cycle_clause.is_some() {
                return Err(ParseError::ParseError(
                    "CYCLE clause on a common table expression is not supported".into(),
                ));
            }

            // Translate column aliases if specified
            let columns: Vec<ast::IndexedColumn> = cte
                .aliascolnames
                .iter()
                .filter_map(|n| match &n.node {
                    Some(Node::String(s)) => Some(ast::IndexedColumn {
                        col_name: ast::Name::from_string(&s.sval),
                        collation_name: None,
                        order: None,
                    }),
                    _ => None,
                })
                .collect();

            // Translate the CTE query
            let cte_query = cte
                .ctequery
                .as_ref()
                .ok_or_else(|| ParseError::ParseError("CTE missing query body".into()))?;
            let select = match &cte_query.node {
                Some(Node::SelectStmt(select_stmt)) => self.translate_select(select_stmt)?,
                _ => {
                    return Err(ParseError::ParseError(
                        "CTE query is not a SELECT statement".into(),
                    ));
                }
            };

            let materialized = match cte.ctematerialized() {
                pg_query::protobuf::CteMaterialize::Default
                | pg_query::protobuf::CteMaterialize::CtematerializeUndefined => {
                    ast::Materialized::Any
                }
                pg_query::protobuf::CteMaterialize::Always => ast::Materialized::Yes,
                pg_query::protobuf::CteMaterialize::Never => ast::Materialized::No,
            };

            ctes.push(ast::CommonTableExpr {
                tbl_name,
                columns,
                materialized,
                select,
            });
        }

        Ok(Some(ast::With {
            recursive: with_clause.recursive,
            ctes,
        }))
    }

    fn translate_limit(
        &self,
        limit_count: &Option<Box<pg_query::protobuf::Node>>,
        limit_offset: &Option<Box<pg_query::protobuf::Node>>,
    ) -> Result<Option<ast::Limit>, ParseError> {
        let count = match limit_count {
            Some(node) => Some(Box::new(self.translate_expr(node)?)),
            None => None,
        };

        let offset = match limit_offset {
            Some(node) => Some(Box::new(self.translate_expr(node)?)),
            None => None,
        };

        match (count, offset) {
            (Some(expr), offset) => Ok(Some(ast::Limit { expr, offset })),
            (None, Some(off)) => {
                // OFFSET without LIMIT: use LIMIT -1 (unlimited) with the offset
                let unlimited =
                    Box::new(ast::Expr::Literal(ast::Literal::Numeric("-1".to_string())));
                Ok(Some(ast::Limit {
                    expr: unlimited,
                    offset: Some(off),
                }))
            }
            (None, None) => Ok(None),
        }
    }

    fn translate_returning(
        &self,
        returning_list: &[pg_query::protobuf::Node],
        target: &pg_query::protobuf::RangeVar,
    ) -> Result<Vec<ast::ResultColumn>, ParseError> {
        if returning_list.is_empty() {
            return Ok(vec![]);
        }
        let target_qualifiers: &[&str] = match target.alias.as_ref() {
            Some(alias) if !alias.aliasname.is_empty() => &[&alias.aliasname],
            _ if target.schemaname.is_empty() => &[&target.relname],
            _ => &[&target.relname, &target.schemaname],
        };
        let mut columns = Vec::with_capacity(returning_list.len());
        for node in returning_list {
            if is_star_qualified_by(node, target_qualifiers) {
                columns.push(ast::ResultColumn::Star);
            } else {
                columns.extend(self.translate_target_list(std::slice::from_ref(node))?);
            }
        }
        Ok(columns)
    }

    fn translate_on_conflict(
        &self,
        on_conflict: &Option<Box<pg_query::protobuf::OnConflictClause>>,
    ) -> Result<Option<Box<ast::Upsert>>, ParseError> {
        use pg_query::protobuf::node::Node;
        use pg_query::protobuf::OnConflictAction;

        let clause = match on_conflict {
            Some(c) => c,
            None => return Ok(None),
        };

        let action = OnConflictAction::try_from(clause.action)
            .map_err(|_| ParseError::ParseError("Invalid ON CONFLICT action".into()))?;

        let do_clause = match action {
            OnConflictAction::OnconflictNothing => ast::UpsertDo::Nothing,
            OnConflictAction::OnconflictUpdate => {
                // Translate SET assignments from target_list
                let mut sets = Vec::new();
                for target_node in &clause.target_list {
                    match &target_node.node {
                        Some(Node::ResTarget(res_target)) => {
                            let col_name = ast::Name::from_string(&res_target.name);
                            let expr = if let Some(val) = &res_target.val {
                                Box::new(self.translate_expr(val)?)
                            } else {
                                return Err(ParseError::ParseError(
                                    "ON CONFLICT SET missing value".into(),
                                ));
                            };
                            sets.push(ast::Set {
                                col_names: vec![col_name],
                                expr,
                            });
                        }
                        _ => {
                            return Err(ParseError::ParseError(
                                "Unexpected node in ON CONFLICT SET".into(),
                            ));
                        }
                    }
                }

                let where_clause = if let Some(wc) = &clause.where_clause {
                    Some(Box::new(self.translate_expr(wc)?))
                } else {
                    None
                };

                ast::UpsertDo::Set { sets, where_clause }
            }
            _ => return Ok(None),
        };

        // Translate conflict target (the columns in ON CONFLICT (col1, col2))
        let index = if let Some(infer) = &clause.infer {
            if !infer.index_elems.is_empty() {
                let targets: Vec<ast::SortedColumn> = infer
                    .index_elems
                    .iter()
                    .filter_map(|elem| match &elem.node {
                        Some(Node::IndexElem(idx_elem)) => {
                            if !idx_elem.name.is_empty() {
                                Some(ast::SortedColumn {
                                    expr: Box::new(ast::Expr::Id(ast::Name::from_string(
                                        &idx_elem.name,
                                    ))),
                                    order: None,
                                    nulls: None,
                                })
                            } else {
                                None
                            }
                        }
                        _ => None,
                    })
                    .collect();

                let where_clause = if let Some(wc) = &infer.where_clause {
                    Some(Box::new(self.translate_expr(wc)?))
                } else {
                    None
                };

                Some(ast::UpsertIndex {
                    targets,
                    where_clause,
                })
            } else {
                None
            }
        } else {
            None
        };

        Ok(Some(Box::new(ast::Upsert {
            index,
            do_clause,
            next: None,
        })))
    }

    fn translate_order_by(
        &self,
        sort_clause: &[pg_query::protobuf::Node],
    ) -> Result<Vec<ast::SortedColumn>, ParseError> {
        let mut sorted_columns = Vec::new();
        for node in sort_clause {
            match &node.node {
                Some(pg_query::protobuf::node::Node::SortBy(sort_by)) => {
                    let expr = if let Some(ref sort_node) = sort_by.node {
                        Box::new(self.translate_expr(sort_node)?)
                    } else {
                        return Err(ParseError::ParseError(
                            "Missing sort expression".to_string(),
                        ));
                    };

                    let order = match pg_query::protobuf::SortByDir::try_from(sort_by.sortby_dir) {
                        Ok(pg_query::protobuf::SortByDir::SortbyAsc) => Some(ast::SortOrder::Asc),
                        Ok(pg_query::protobuf::SortByDir::SortbyDesc) => Some(ast::SortOrder::Desc),
                        _ => None, // Default or undefined
                    };

                    let nulls =
                        match pg_query::protobuf::SortByNulls::try_from(sort_by.sortby_nulls) {
                            Ok(pg_query::protobuf::SortByNulls::SortbyNullsFirst) => {
                                Some(ast::NullsOrder::First)
                            }
                            Ok(pg_query::protobuf::SortByNulls::SortbyNullsLast) => {
                                Some(ast::NullsOrder::Last)
                            }
                            _ => match order {
                                Some(ast::SortOrder::Desc) => Some(ast::NullsOrder::First),
                                _ => Some(ast::NullsOrder::Last),
                            },
                        };

                    sorted_columns.push(ast::SortedColumn { expr, order, nulls });
                }
                _ => {
                    return Err(ParseError::ParseError(format!(
                        "Unsupported ORDER BY clause item: {:?}",
                        node.node
                    )));
                }
            }
        }
        Ok(sorted_columns)
    }

    fn translate_bool_expr(
        &self,
        bool_expr: &pg_query::protobuf::BoolExpr,
    ) -> Result<ast::Expr, ParseError> {
        use pg_query::protobuf::BoolExprType;

        if bool_expr.args.is_empty() {
            return Err(ParseError::ParseError(
                "BoolExpr must have at least 1 argument".to_string(),
            ));
        }

        // Map PostgreSQL boolean operators to Turso operators
        match &bool_expr.boolop() {
            BoolExprType::NotExpr => {
                // NOT is unary, handle differently
                if bool_expr.args.len() != 1 {
                    return Err(ParseError::ParseError(
                        "NOT expression must have exactly 1 argument".to_string(),
                    ));
                }
                let operand = Box::new(self.translate_operand(&bool_expr.args[0])?);
                Ok(ast::Expr::Unary(ast::UnaryOperator::Not, operand))
            }
            BoolExprType::AndExpr => {
                if bool_expr.args.len() < 2 {
                    return Err(ParseError::ParseError(
                        "AND expression must have at least 2 arguments".to_string(),
                    ));
                }
                // Combine all arguments into a binary tree with AND
                let mut result = self.translate_operand(&bool_expr.args[0])?;
                for arg in &bool_expr.args[1..] {
                    let right = self.translate_operand(arg)?;
                    result =
                        ast::Expr::Binary(Box::new(result), ast::Operator::And, Box::new(right));
                }
                Ok(result)
            }
            BoolExprType::OrExpr => {
                if bool_expr.args.len() < 2 {
                    return Err(ParseError::ParseError(
                        "OR expression must have at least 2 arguments".to_string(),
                    ));
                }
                // Combine all arguments into a binary tree with OR
                let mut result = self.translate_operand(&bool_expr.args[0])?;
                for arg in &bool_expr.args[1..] {
                    let right = self.translate_operand(arg)?;
                    result =
                        ast::Expr::Binary(Box::new(result), ast::Operator::Or, Box::new(right));
                }
                Ok(result)
            }
            _ => Err(ParseError::ParseError(format!(
                "Unsupported BoolExpr type: {:?}",
                bool_expr.boolop()
            ))),
        }
    }

    /// PostgreSQL's parse tree has no nodes for parentheses, so an operand that
    /// is itself an operator expression gets wrapped here. Without this the
    /// grouping is lost when the expression is written back as SQL, for example
    /// `(1+2)*3` in a DEFAULT stored in the table SQL would become `1 + 2 * 3`.
    fn translate_operand(&self, node: &pg_query::protobuf::Node) -> Result<ast::Expr, ParseError> {
        let expr = self.translate_expr(node)?;
        Ok(match expr {
            ast::Expr::Binary(..)
            | ast::Expr::Unary(..)
            | ast::Expr::Between { .. }
            | ast::Expr::Like { .. }
            | ast::Expr::IsNull(_)
            | ast::Expr::NotNull(_)
            | ast::Expr::InList { .. }
            | ast::Expr::InSelect { .. }
            | ast::Expr::InTable { .. } => ast::Expr::Parenthesized(vec![Box::new(expr)]),
            _ => expr,
        })
    }

    fn translate_create_sequence(
        &self,
        seq: &pg_query::protobuf::CreateSeqStmt,
    ) -> Result<ast::Stmt, ParseError> {
        let relation = seq
            .sequence
            .as_ref()
            .ok_or_else(|| ParseError::ParseError("CREATE SEQUENCE missing name".into()))?;
        let seq_name = self.qualified_name_from_range_var(relation);

        let mut start = None;
        let mut increment = None;
        let mut min_value = None;
        let mut max_value = None;
        let mut cycle = false;

        let mut as_type = None;
        for opt_node in &seq.options {
            if let Some(pg_query::protobuf::node::Node::DefElem(elem)) = &opt_node.node {
                match elem.defname.as_str() {
                    "as" => {
                        let Some(pg_query::protobuf::node::Node::TypeName(tn)) =
                            elem.arg.as_ref().and_then(|n| n.node.as_ref())
                        else {
                            return Err(ParseError::ParseError(
                                "CREATE SEQUENCE AS: missing type".into(),
                            ));
                        };
                        as_type = Some(extract_type_name_from_typename(tn)?);
                    }
                    "start" => {
                        start = extract_def_elem_int(elem);
                    }
                    "increment" => {
                        increment = extract_def_elem_int(elem);
                    }
                    "minvalue" => {
                        min_value = extract_def_elem_int(elem);
                    }
                    "maxvalue" => {
                        max_value = extract_def_elem_int(elem);
                    }
                    "cycle" => {
                        // pg_query emits Boolean(true) for CYCLE, Boolean(false) for NO CYCLE
                        cycle = extract_def_elem_bool(elem);
                    }
                    "cache" => {
                        // Turso's disk-only sequence implementation never
                        // caches values, so a `CACHE n` clause is otherwise
                        // a no-op. We still validate the value against the
                        // PostgreSQL contract (`CACHE` must be >= 1) so
                        // that `CACHE 0` errors with the same message a
                        // real PG server would produce. Surfacing the
                        // requested value through to the runtime would
                        // require resurrecting per-sequence in-memory
                        // descriptor state we have intentionally removed
                        // (see feedback-sequences-disk-only memory).
                        if let Some(cache) = extract_def_elem_int(elem) {
                            if cache < 1 {
                                return Err(ParseError::ParseError(format!(
                                    "CACHE ({cache}) must be greater than zero"
                                )));
                            }
                        }
                    }
                    _ => {} // ignore other unknown options
                }
            }
        }

        // AS smallint/integer narrows the default bound in the sequence's
        // direction; bigint is the default range.
        if let Some(as_type) = as_type {
            let (type_min, type_max) = match as_type.to_lowercase().as_str() {
                "int2" | "smallint" => (i16::MIN as i64, i16::MAX as i64),
                "int4" | "integer" | "int" => (i32::MIN as i64, i32::MAX as i64),
                "int8" | "bigint" => (i64::MIN, i64::MAX),
                other => {
                    return Err(ParseError::ParseError(format!(
                        "CREATE SEQUENCE AS {other} is not supported"
                    )))
                }
            };
            if increment.unwrap_or(1) > 0 {
                max_value.get_or_insert(type_max);
            } else {
                min_value.get_or_insert(type_min);
            }
        }

        Ok(ast::Stmt::CreateSequence {
            if_not_exists: seq.if_not_exists,
            seq_name,
            start,
            increment,
            min_value,
            max_value,
            cycle,
        })
    }

    fn translate_create_domain(
        &self,
        domain: &pg_query::protobuf::CreateDomainStmt,
    ) -> Result<ast::Stmt, ParseError> {
        use pg_query::protobuf::node::Node;
        use pg_query::protobuf::ConstrType;

        // Extract domain name (skip schema qualifiers like "pg_catalog")
        let mut domain_name = String::new();
        for name_node in &domain.domainname {
            if let Some(Node::String(s)) = &name_node.node {
                if s.sval != "pg_catalog" {
                    domain_name.clone_from(&s.sval);
                }
            }
        }
        if domain_name.is_empty() {
            return Err(ParseError::ParseError(
                "CREATE DOMAIN missing domain name".into(),
            ));
        }

        // Extract base type from type_name and map PG type names to Turso names
        let type_name_node = domain
            .type_name
            .as_ref()
            .ok_or_else(|| ParseError::ParseError("CREATE DOMAIN missing base type".into()))?;
        let pg_type = extract_type_name_from_typename(type_name_node)?;
        let base_type = match map_pg_type(&pg_type, &[]) {
            Some(mapping) => mapping.type_name,
            None => pg_type, // custom type or domain — pass through
        };

        // Process constraints
        let mut default = None;
        let mut not_null = false;
        let mut constraints = Vec::new();

        for constraint_node in &domain.constraints {
            let Some(Node::Constraint(constraint)) = &constraint_node.node else {
                continue;
            };
            reject_constraint_attributes(constraint, "")?;
            let contype = ConstrType::try_from(constraint.contype).unwrap_or(ConstrType::Undefined);
            match contype {
                ConstrType::ConstrDefault => {
                    if let Some(ref raw_expr) = constraint.raw_expr {
                        default = Some(Box::new(self.translate_expr(raw_expr)?));
                    }
                }
                ConstrType::ConstrNotnull => {
                    not_null = true;
                }
                ConstrType::ConstrCheck => {
                    if let Some(ref raw_expr) = constraint.raw_expr {
                        let check_expr = self.translate_expr(raw_expr)?;
                        let name = if constraint.conname.is_empty() {
                            None
                        } else {
                            Some(constraint.conname.clone())
                        };
                        constraints.push(ast::DomainConstraint {
                            name,
                            check: Box::new(check_expr),
                        });
                    }
                }
                _ => {}
            }
        }

        Ok(ast::Stmt::CreateDomain {
            if_not_exists: false,
            domain_name,
            base_type,
            default,
            not_null,
            constraints,
        })
    }
}

/// The names of a FROM-item column alias list (`AS s(a, b)`).
fn alias_column_names(alias: &pg_query::protobuf::Alias) -> Result<Vec<String>, ParseError> {
    alias
        .colnames
        .iter()
        .map(|n| match &n.node {
            Some(pg_query::protobuf::node::Node::String(s)) => Ok(s.sval.clone()),
            _ => Err(ParseError::ParseError("invalid column alias".into())),
        })
        .collect()
}

/// Applies a FROM-item column alias list (`AS s(a, b)`) by renaming the
/// leading result columns of `select`. SQLite names a compound's columns
/// after its first SELECT, so only that one is renamed.
fn rename_result_columns(select: &mut ast::Select, names: &[String]) -> Result<(), ParseError> {
    if names.is_empty() {
        return Ok(());
    }
    let ast::OneSelect::Select { columns, .. } = &mut select.body.select else {
        return Err(ParseError::ParseError(
            "a column alias list on a VALUES subquery is not supported".into(),
        ));
    };
    // A * expands to an unknown number of columns, so positions after it
    // cannot be matched to names.
    if columns
        .iter()
        .take(names.len())
        .any(|c| !matches!(c, ast::ResultColumn::Expr(..)))
    {
        return Err(ParseError::ParseError(
            "a column alias list on a subquery that selects * is not supported".into(),
        ));
    }
    if names.len() > columns.len() {
        return Err(ParseError::ParseError(format!(
            "table has {} columns available but {} columns specified",
            columns.len(),
            names.len()
        )));
    }
    for (column, name) in columns.iter_mut().zip(names) {
        if let ast::ResultColumn::Expr(_, alias) = column {
            *alias = Some(ast::As::Elided(ast::Name::from_string(name)));
        }
    }
    Ok(())
}

fn join_keeps_unmatched_right_rows(join: &ast::JoinedSelectTable) -> bool {
    matches!(
        join.operator,
        ast::JoinOperator::TypedJoin(Some(join_type)) if join_type.contains(ast::JoinType::RIGHT)
    )
}

/// Maps a PostgreSQL schema name to the Turso schema that holds its tables.
fn map_schema_name(schema: &str) -> &str {
    match schema.to_ascii_lowercase().as_str() {
        "pg_catalog" | "public" | "information_schema" => "main",
        _ => schema,
    }
}

pub fn is_catalog_table_name(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "pg_class"
            | "pg_namespace"
            | "pg_attribute"
            | "pg_roles"
            | "pg_proc"
            | "pg_database"
            | "pg_am"
            | "pg_type"
            | "pg_collation"
            | "pg_attrdef"
            | "pg_description"
            | "pg_publication"
            | "pg_publication_namespace"
            | "pg_publication_rel"
            | "pg_sequences"
            | "pg_constraint"
            | "pg_index"
            | "pg_inherits"
            | "pg_rewrite"
            | "pg_foreign_table"
            | "pg_partitioned_table"
            | "pg_trigger"
            | "pg_policy"
            | "pg_input_error_info"
            | "pg_options_to_table"
            | "unnest"
            | "pg_get_tabledef"
            | "pg_settings"
            | "pg_extension"
            | "pg_depend"
            | "pg_tablespace"
            | "pg_init_privs"
            | "pg_cast"
            | "pg_transform"
            | "pg_language"
            | "pg_operator"
            | "pg_opclass"
            | "pg_opfamily"
            | "pg_ts_parser"
            | "pg_ts_template"
            | "pg_ts_dict"
            | "pg_ts_config"
            | "pg_foreign_data_wrapper"
            | "pg_foreign_server"
            | "pg_default_acl"
            | "pg_conversion"
            | "pg_range"
            | "pg_event_trigger"
            | "pg_subscription"
            | "pg_largeobject_metadata"
            | "pg_amop"
            | "pg_amproc"
            | "pg_tables"
            | "pg_indexes"
    )
}

/// PostgreSQL derives a name for result columns without an explicit alias
/// (FigureColname in the server): function calls are named after the function
/// and SQL value functions after their keyword. Clients read columns by these
/// names, e.g. knex reads rows[0].version from `select version()`.
/// Casts and CASE pass through the name of the expression they wrap, and only
/// when that expression has no name are they named after the outermost target
/// type or `case`, so `1::int::text` is named `text`.
fn derived_column_name(node: &pg_query::protobuf::Node) -> Option<String> {
    expression_name(node).or_else(|| cast_or_case_name(node))
}

fn expression_name(node: &pg_query::protobuf::Node) -> Option<String> {
    use pg_query::protobuf::node::Node;
    let name = match &node.node {
        Some(Node::FuncCall(func_call)) => return last_string(&func_call.funcname),
        Some(Node::ColumnRef(col_ref)) => return last_string(&col_ref.fields),
        Some(Node::TypeCast(type_cast)) => return expression_name(type_cast.arg.as_deref()?),
        Some(Node::CaseExpr(case_expr)) => return expression_name(case_expr.defresult.as_deref()?),
        Some(Node::CoalesceExpr(_)) => "coalesce",
        Some(Node::MinMaxExpr(min_max)) => {
            use pg_query::protobuf::MinMaxOp;
            match MinMaxOp::try_from(min_max.op) {
                Ok(MinMaxOp::IsGreatest) => "greatest",
                Ok(MinMaxOp::IsLeast) => "least",
                _ => return None,
            }
        }
        Some(Node::AExpr(a_expr))
            if a_expr.kind == pg_query::protobuf::AExprKind::AexprNullif as i32 =>
        {
            "nullif"
        }
        Some(Node::SqlvalueFunction(svf)) => {
            use pg_query::protobuf::SqlValueFunctionOp as Op;
            match Op::try_from(svf.op) {
                Ok(Op::SvfopCurrentDate) => "current_date",
                Ok(Op::SvfopCurrentTime | Op::SvfopCurrentTimeN) => "current_time",
                Ok(Op::SvfopCurrentTimestamp | Op::SvfopCurrentTimestampN) => "current_timestamp",
                Ok(Op::SvfopLocaltime | Op::SvfopLocaltimeN) => "localtime",
                Ok(Op::SvfopLocaltimestamp | Op::SvfopLocaltimestampN) => "localtimestamp",
                Ok(Op::SvfopCurrentRole) => "current_role",
                Ok(Op::SvfopCurrentUser) => "current_user",
                Ok(Op::SvfopUser) => "user",
                Ok(Op::SvfopSessionUser) => "session_user",
                Ok(Op::SvfopCurrentCatalog) => "current_catalog",
                Ok(Op::SvfopCurrentSchema) => "current_schema",
                _ => return None,
            }
        }
        _ => return None,
    };
    Some(name.to_string())
}

fn cast_or_case_name(node: &pg_query::protobuf::Node) -> Option<String> {
    use pg_query::protobuf::node::Node;
    match &node.node {
        Some(Node::TypeCast(type_cast)) => last_string(&type_cast.type_name.as_ref()?.names),
        Some(Node::CaseExpr(_)) => Some("case".to_string()),
        _ => None,
    }
}

/// Whether `node` is a `qualifier.*` RETURNING target whose qualifier, read
/// from the table name outwards, is a non-empty prefix of `target_qualifiers`.
fn is_star_qualified_by(node: &pg_query::protobuf::Node, target_qualifiers: &[&str]) -> bool {
    use pg_query::protobuf::node::Node;
    let Some(Node::ResTarget(res_target)) = &node.node else {
        return false;
    };
    let Some(Node::ColumnRef(col_ref)) = res_target.val.as_ref().and_then(|v| v.node.as_ref())
    else {
        return false;
    };
    let Some((last, qualifier)) = col_ref.fields.split_last() else {
        return false;
    };
    if !matches!(last.node, Some(Node::AStar(_)))
        || qualifier.is_empty()
        || qualifier.len() > target_qualifiers.len()
    {
        return false;
    }
    qualifier.iter().rev().zip(target_qualifiers).all(
        |(field, expected)| matches!(&field.node, Some(Node::String(s)) if s.sval == *expected),
    )
}

fn last_string(names: &[pg_query::protobuf::Node]) -> Option<String> {
    names
        .iter()
        .filter_map(|n| match &n.node {
            Some(pg_query::protobuf::node::Node::String(s)) => Some(s.sval.clone()),
            _ => None,
        })
        .next_back()
}

/// Extract a string value from a DefElem's arg.
fn def_elem_string_val(def: &pg_query::protobuf::DefElem) -> Option<String> {
    let arg = def.arg.as_deref()?;
    match &arg.node {
        Some(pg_query::protobuf::node::Node::String(s)) => Some(s.sval.clone()),
        _ => None,
    }
}

/// Extract a boolean value from a DefElem's arg.
/// If arg is None (bare keyword like HEADER), returns None (caller defaults to true).
fn def_elem_bool_val(def: &pg_query::protobuf::DefElem) -> Option<bool> {
    let arg = def.arg.as_deref()?;
    match &arg.node {
        Some(pg_query::protobuf::node::Node::Integer(i)) => Some(i.ival != 0),
        Some(pg_query::protobuf::node::Node::String(s)) => Some(matches!(
            s.sval.to_lowercase().as_str(),
            "true" | "on" | "1"
        )),
        _ => None,
    }
}

/// Result of mapping a PostgreSQL type: the Turso type name, array dimensions,
/// and type parameters (e.g. `[4]` for `varchar(4)`, `[10, 2]` for `numeric(10, 2)`).
#[derive(Debug, Clone, PartialEq)]
pub struct PgTypeMapping {
    pub type_name: String,
    pub array_dimensions: u32,
    pub type_params: Vec<i64>,
}

impl PgTypeMapping {
    pub fn scalar(name: impl Into<String>) -> Self {
        Self {
            type_name: name.into(),
            array_dimensions: 0,
            type_params: vec![],
        }
    }

    pub fn with_params(name: impl Into<String>, params: Vec<i64>) -> Self {
        Self {
            type_name: name.into(),
            array_dimensions: 0,
            type_params: params,
        }
    }

    pub fn array(name: impl Into<String>, dims: u32) -> Self {
        Self {
            type_name: name.into(),
            array_dimensions: dims,
            type_params: vec![],
        }
    }
}

/// PostgreSQL to Turso type mapping.
/// Returns Turso custom type names (e.g. "boolean", "varchar(100)") when a
/// Returns true if the given PG type name is a serial variant (auto-incrementing integer).
/// Covers all PostgreSQL serial aliases: serial, serial2, serial4, serial8,
/// smallserial, bigserial.
fn is_serial_type(pg_type: &str) -> bool {
    matches!(
        pg_type.to_uppercase().as_str(),
        "SERIAL" | "SERIAL2" | "SERIAL4" | "SERIAL8" | "SMALLSERIAL" | "BIGSERIAL"
    )
}

/// built-in Turso type exists, otherwise returns the base SQLite type.
/// For array types (e.g. `INTEGER[]`, `_int4`), returns the base scalar type
/// with `array_dimensions > 0` so native Turso arrays are used.
pub fn map_pg_type(pg_type: &str, params: &[i64]) -> Option<PgTypeMapping> {
    // Check for array types first
    if pg_type.ends_with("[]") || pg_type.starts_with('_') {
        let (base, dims) = if pg_type.ends_with("[]") {
            // Count and strip all trailing [] (handles text[][] etc.)
            let trimmed = pg_type.trim_end_matches("[]");
            let dims = (pg_type.len() - trimmed.len()) / 2;
            (trimmed, dims as u32)
        } else {
            // Strip leading _ (PG internal array notation) → 1 dimension
            (&pg_type[1..], 1u32)
        };
        // Recursively map the base type as a scalar
        let scalar = map_pg_type(base, params)?;
        return Some(PgTypeMapping {
            type_name: scalar.type_name,
            array_dimensions: scalar.array_dimensions + dims,
            type_params: scalar.type_params,
        });
    }

    let type_name = match pg_type.to_uppercase().as_str() {
        // Types with Turso custom type equivalents
        "BOOLEAN" | "BOOL" => "boolean".into(),
        "SMALLINT" | "INT2" => "smallint".into(),
        "BIGINT" | "INT8" => "bigint".into(),
        "UUID" => "uuid".into(),
        "DATE" => "date".into(),
        "TIME" | "TIMETZ" => "time".into(),
        "TIMESTAMP" => "timestamp".into(),
        "TIMESTAMPTZ" => "timestamptz".into(),
        "BYTEA" => "bytea".into(),
        "INET" => "inet".into(),
        "JSON" => "json".into(),
        "JSONB" => "jsonb".into(),

        // Parametric types — return base name + params separately
        "VARCHAR" | "CHAR" => {
            return match params.first() {
                Some(_) => Some(PgTypeMapping::with_params("varchar", params.to_vec())),
                None => Some(PgTypeMapping::scalar("TEXT")),
            };
        }
        "NUMERIC" | "DECIMAL" => {
            return match params {
                [p, s] => Some(PgTypeMapping::with_params("numeric", vec![*p, *s])),
                [p] => Some(PgTypeMapping::with_params("numeric", vec![*p, 0])),
                _ => Some(PgTypeMapping::scalar("REAL")),
            };
        }

        // Base types (no Turso custom type needed)
        "INTEGER" | "INT" | "INT4" | "SERIAL" | "SERIAL4" | "BIGSERIAL" | "SERIAL8"
        | "SMALLSERIAL" | "SERIAL2" => "INTEGER".into(),
        "REAL" | "FLOAT4" | "DOUBLE PRECISION" | "FLOAT8" => "REAL".into(),
        "TEXT" | "BPCHAR" | "NAME" => "TEXT".into(),
        "BLOB" => "BLOB".into(),

        // PG types without Turso equivalents → base SQLite types
        "INTERVAL" => "TEXT".into(),
        // Network types → custom types for proper PG wire OIDs
        "CIDR" => "cidr".into(),
        "MACADDR" => "macaddr".into(),
        "MACADDR8" => "macaddr8".into(),
        "POINT" | "LINE" | "LSEG" | "BOX" | "PATH" | "POLYGON" | "CIRCLE" => "TEXT".into(),
        "XML" | "TSVECTOR" | "TSQUERY" => "TEXT".into(),
        "MONEY" => "REAL".into(),
        "BIT" | "VARBIT" => "TEXT".into(),
        "OID" | "REGCLASS" | "REGTYPE" | "REGPROC" | "REGPROCEDURE" | "REGOPER" | "REGOPERATOR"
        | "REGCONFIG" | "REGDICTIONARY" | "REGNAMESPACE" | "REGROLE" => "INTEGER".into(),
        "VOID" => "TEXT".into(),

        // Unknown types pass through as-is (e.g. user-defined enums).
        // The custom type system will validate at CREATE TABLE time.
        _ => pg_type.to_lowercase(),
    };

    Some(PgTypeMapping {
        type_name,
        array_dimensions: 0,
        type_params: vec![],
    })
}

/// Internal DDL plan for FK constraints — used during CREATE TABLE translation.
struct PgForeignKey {
    ref_table: String,
    ref_columns: Vec<String>,
    on_delete: Option<String>,
    on_update: Option<String>,
}

/// Translate `CREATE TYPE <name> AS ENUM (...)` to a Turso `CREATE TYPE` with
/// an ENCODE expression that validates values against the enum labels.
fn translate_create_enum(
    enum_stmt: &pg_query::protobuf::CreateEnumStmt,
) -> Result<ast::Stmt, ParseError> {
    use pg_query::protobuf::node::Node;

    // Extract type name (skip "pg_catalog" or schema qualifiers)
    let mut type_name = String::new();
    for name_node in &enum_stmt.type_name {
        if let Some(Node::String(s)) = &name_node.node {
            if s.sval != "pg_catalog" {
                type_name.clone_from(&s.sval);
            }
        }
    }
    if type_name.is_empty() {
        return Err(ParseError::ParseError(
            "CREATE TYPE AS ENUM missing type name".into(),
        ));
    }

    // Extract enum labels
    let mut labels = Vec::new();
    for val_node in &enum_stmt.vals {
        if let Some(Node::String(s)) = &val_node.node {
            labels.push(s.sval.clone());
        } else {
            return Err(ParseError::ParseError(
                "ENUM label must be a string literal".into(),
            ));
        }
    }
    if labels.is_empty() {
        return Err(ParseError::ParseError(
            "ENUM type must have at least one label".into(),
        ));
    }

    // Build: CASE WHEN value IN ('a','b','c') THEN value
    //        ELSE RAISE(ABORT, 'invalid input value for enum <name>') END
    let in_list: Vec<Box<ast::Expr>> = labels
        .iter()
        .map(|l| Box::new(ast::Expr::Literal(ast::Literal::String(format!("'{l}'")))))
        .collect();

    let error_msg = format!("invalid input value for enum {type_name}");
    let encode = ast::Expr::Case {
        base: None,
        when_then_pairs: vec![(
            Box::new(ast::Expr::InList {
                lhs: Box::new(ast::Expr::Id(ast::Name::from_string("value"))),
                not: false,
                rhs: in_list,
            }),
            Box::new(ast::Expr::Id(ast::Name::from_string("value"))),
        )],
        else_expr: Some(Box::new(ast::Expr::Raise(
            ast::ResolveType::Abort,
            Some(Box::new(ast::Expr::Literal(ast::Literal::String(format!(
                "'{error_msg}'"
            ))))),
        ))),
    };

    let decode = ast::Expr::Id(ast::Name::from_string("value"));

    Ok(ast::Stmt::CreateType {
        if_not_exists: false,
        type_name,
        body: ast::CreateTypeBody::CustomType {
            params: vec![ast::TypeParam {
                name: "value".into(),
                ty: Some("text".into()),
            }],
            base: "text".into(),
            encode: Some(Box::new(encode)),
            decode: Some(Box::new(decode)),
            operators: vec![ast::TypeOperator {
                op: "<".into(),
                func_name: None,
            }],
            default: None,
        },
    })
}

fn json_type_or_raise(
    json_type: ast::Expr,
    value: ast::Expr,
    expected_type: &str,
    other_type_errors: &[(&str, String)],
    error: &str,
) -> ast::Expr {
    let string = |text: &str| {
        Box::new(ast::Expr::Literal(ast::Literal::String(format!(
            "'{text}'"
        ))))
    };
    let raise = |message: &str| {
        Box::new(ast::Expr::Raise(
            ast::ResolveType::Abort,
            Some(string(message)),
        ))
    };
    let expected_type = string(expected_type);
    ast::Expr::Case {
        base: Some(Box::new(ast::Expr::FunctionCall {
            name: ast::Name::from_string("coalesce"),
            distinctness: None,
            args: vec![Box::new(json_type), expected_type.clone()],
            order_by: vec![],
            within_group: vec![],
            filter_over: ast::FunctionTail {
                filter_clause: None,
                over_clause: None,
            },
        })),
        when_then_pairs: std::iter::once((expected_type, Box::new(value)))
            .chain(
                other_type_errors
                    .iter()
                    .map(|(json_type, message)| (string(json_type), raise(message))),
            )
            .collect(),
        else_expr: Some(raise(error)),
    }
}

/// Convert a pg_query TypeName to a Turso AST Type for use in CAST expressions.
/// Maps PG types to their base SQLite storage types. Other names are kept as
/// written so that enums and domains resolve to their custom type.
fn pg_type_name_to_ast_type(type_name: &pg_query::protobuf::TypeName) -> Option<ast::Type> {
    use pg_query::protobuf::node::Node;

    let mut parts = Vec::new();
    for name_node in &type_name.names {
        if let Some(Node::String(s)) = &name_node.node {
            if s.sval != "pg_catalog" {
                parts.push(s.sval.clone());
            }
        }
    }
    if parts.is_empty() {
        return None;
    }
    let pg_type = parts.join(" ");

    let name = match pg_type.to_uppercase().as_str() {
        "INTEGER" | "INT" | "INT4" | "SMALLINT" | "INT2" | "BIGINT" | "INT8" | "SERIAL"
        | "BIGSERIAL" | "SMALLSERIAL" | "OID" | "REGCLASS" | "REGTYPE" => "INTEGER",
        "REAL" | "FLOAT4" | "DOUBLE PRECISION" | "FLOAT8" | "NUMERIC" | "DECIMAL" | "MONEY" => {
            "REAL"
        }
        // For CAST expressions, map all text-like PG types to TEXT and
        // boolean to INTEGER for SQLite VDBE compatibility
        "BOOLEAN" | "BOOL" => "INTEGER",
        "DATE" => "date",
        "TIME" | "TIMETZ" => "time",
        "TIMESTAMP" => "timestamp",
        "TIMESTAMPTZ" => "timestamptz",
        "TEXT" | "VARCHAR" | "CHAR" | "BPCHAR" | "NAME" | "UUID" | "INTERVAL" | "INET" | "JSON"
        | "JSONB" | "XML" | "CIDR" | "MACADDR" | "BIT" | "VARBIT" | "TSVECTOR" | "TSQUERY" => {
            "TEXT"
        }
        "BYTEA" | "BLOB" => "BLOB",
        _ => &pg_type,
    };

    Some(ast::Type {
        name: name.to_string(),
        size: None,
        array_dimensions: type_name.array_bounds.len() as u32,
    })
}

fn extract_type_name_from_typename(
    type_name: &pg_query::protobuf::TypeName,
) -> Result<String, ParseError> {
    use pg_query::protobuf::node::Node;

    let mut parts = Vec::new();
    for name_node in &type_name.names {
        if let Some(Node::String(s)) = &name_node.node {
            if s.sval != "pg_catalog" {
                parts.push(s.sval.clone());
            }
        }
    }
    if parts.is_empty() {
        return Err(ParseError::ParseError("empty type name".into()));
    }
    let mut name = parts.join(" ");
    if !type_name.array_bounds.is_empty() {
        for _ in &type_name.array_bounds {
            name.push_str("[]");
        }
    }
    Ok(name)
}

/// Extract an integer value from a DefElem's arg node.
fn extract_def_elem_int(elem: &pg_query::protobuf::DefElem) -> Option<i64> {
    use pg_query::protobuf::node::Node;
    match elem.arg.as_ref().and_then(|n| n.node.as_ref()) {
        Some(Node::Integer(i)) => Some(i.ival as i64),
        Some(Node::Float(f)) => f.fval.parse::<i64>().ok(),
        _ => None,
    }
}

fn extract_def_elem_bool(elem: &pg_query::protobuf::DefElem) -> bool {
    use pg_query::protobuf::node::Node;
    match elem.arg.as_ref().and_then(|n| n.node.as_ref()) {
        Some(Node::Boolean(b)) => b.boolval,
        Some(Node::Integer(i)) => i.ival != 0,
        _ => false,
    }
}

fn extract_type_name(col_def: &pg_query::protobuf::ColumnDef) -> Result<String, ParseError> {
    use pg_query::protobuf::node::Node;

    let type_name = col_def
        .type_name
        .as_ref()
        .ok_or_else(|| ParseError::ParseError("column missing type".into()))?;

    // TypeName.names is a list of String nodes (e.g., ["pg_catalog", "int4"])
    // Skip "pg_catalog" prefix if present
    let mut parts = Vec::new();
    for name_node in &type_name.names {
        if let Some(Node::String(s)) = &name_node.node {
            if s.sval != "pg_catalog" {
                parts.push(s.sval.clone());
            }
        }
    }
    if parts.is_empty() {
        return Err(ParseError::ParseError("column has empty type name".into()));
    }
    let mut name = parts.join(" ");

    // PG represents array types like `integer[]` with array_bounds on the TypeName,
    // not by appending `[]` to the type name. Detect and append it.
    if !type_name.array_bounds.is_empty() {
        for _ in &type_name.array_bounds {
            name.push_str("[]");
        }
    }

    Ok(name)
}

/// Extract integer type modifiers from a column definition.
/// For `varchar(50)` this returns `[50]`, for `numeric(10, 2)` returns `[10, 2]`.
fn extract_integer_typmods(
    col_def: &pg_query::protobuf::ColumnDef,
) -> Result<Vec<i64>, ParseError> {
    use pg_query::protobuf::a_const::Val;
    use pg_query::protobuf::node::Node;

    let Some(type_name) = &col_def.type_name else {
        return Ok(vec![]);
    };
    type_name
        .typmods
        .iter()
        .map(|node| match &node.node {
            Some(Node::Integer(i)) => Ok(i.ival as i64),
            Some(Node::AConst(a_const)) => match &a_const.val {
                Some(Val::Ival(i)) => Ok(i.ival as i64),
                _ => Err(not_supported("non-integer type modifier")),
            },
            _ => Err(not_supported("non-integer type modifier")),
        })
        .collect()
}

/// Maps a column's PG type, rejecting type modifiers the mapping would drop
/// (e.g. the precision in `timestamp(3)`).
fn map_column_type(pg_type: &str, typmods: &[i64]) -> Result<PgTypeMapping, ParseError> {
    let mapping = map_pg_type(pg_type, typmods)
        .ok_or_else(|| ParseError::ParseError(format!("unsupported PostgreSQL type: {pg_type}")))?;
    if !typmods.is_empty() && mapping.type_params.is_empty() {
        return Err(not_supported(format!("type modifiers on {pg_type}")));
    }
    Ok(mapping)
}

fn extract_key_columns(keys: &[pg_query::protobuf::Node]) -> Result<Vec<String>, ParseError> {
    use pg_query::protobuf::node::Node;

    let mut cols = Vec::new();
    for key_node in keys {
        if let Some(Node::String(s)) = &key_node.node {
            cols.push(s.sval.clone());
        }
    }
    Ok(cols)
}

/// Extract a foreign key constraint from a PG Constraint node.
fn extract_foreign_key(constraint: &pg_query::protobuf::Constraint) -> Option<PgForeignKey> {
    use pg_query::protobuf::node::Node;

    let ref_table = constraint.pktable.as_ref()?.relname.clone();

    let ref_columns: Vec<String> = constraint
        .pk_attrs
        .iter()
        .filter_map(|n| match &n.node {
            Some(Node::String(s)) => Some(s.sval.clone()),
            _ => None,
        })
        .collect();

    let on_delete = pg_fk_action_to_string(&constraint.fk_del_action);
    let on_update = pg_fk_action_to_string(&constraint.fk_upd_action);

    Some(PgForeignKey {
        ref_table,
        ref_columns,
        on_delete,
        on_update,
    })
}

/// Convert a PG FK action character to a SQL string.
/// PG uses: 'a' = NO ACTION, 'r' = RESTRICT, 'c' = CASCADE, 'n' = SET NULL, 'd' = SET DEFAULT
fn pg_fk_action_to_string(action: &str) -> Option<String> {
    match action {
        "c" => Some("CASCADE".into()),
        "n" => Some("SET NULL".into()),
        "d" => Some("SET DEFAULT".into()),
        "r" => Some("RESTRICT".into()),
        _ => None, // 'a' (NO ACTION) is default, no need to emit
    }
}

/// Represents a parsed SET statement from the PostgreSQL protocol.
pub struct PgSetStmt {
    pub name: String,
    pub values: Vec<PgSetValue>,
    pub is_local: bool,
}

#[derive(Clone)]
pub enum PgSetValue {
    Identifier(String),
    StringLiteral(String),
    Number(String),
    Bool(bool),
    RawSql(String),
    Null,
}

impl PgSetValue {
    fn from_node(node: &pg_query::protobuf::Node) -> Option<Self> {
        use pg_query::protobuf::{a_const::Val, node::Node};

        match &node.node {
            Some(Node::Integer(i)) => Some(Self::Number(i.ival.to_string())),
            Some(Node::Float(f)) => Some(Self::Number(f.fval.clone())),
            Some(Node::String(s)) => Some(Self::Identifier(s.sval.clone())),
            Some(Node::AConst(a_const)) => {
                if a_const.isnull {
                    return Some(Self::Null);
                }

                match a_const.val.as_ref()? {
                    Val::Ival(i) => Some(Self::Number(i.ival.to_string())),
                    Val::Fval(f) => Some(Self::Number(f.fval.clone())),
                    Val::Sval(s) => Some(Self::StringLiteral(s.sval.clone())),
                    Val::Boolval(b) => Some(Self::Bool(b.boolval)),
                    Val::Bsval(_) => deparse_default_expr(node).map(Self::RawSql),
                }
            }
            _ => deparse_default_expr(node).map(Self::RawSql),
        }
    }

    pub fn to_sql_string(&self) -> String {
        match self {
            PgSetValue::Identifier(value) => value.clone(),
            PgSetValue::StringLiteral(value) => format!("'{}'", value.replace('\'', "''")),
            PgSetValue::Number(value) => value.clone(),
            PgSetValue::Bool(value) => {
                if *value {
                    "1".to_string()
                } else {
                    "0".to_string()
                }
            }
            PgSetValue::Null => "NULL".to_string(),
            PgSetValue::RawSql(value) => value.clone(),
        }
    }

    pub fn as_search_path_name(&self) -> Option<&str> {
        match self {
            PgSetValue::Identifier(value) => Some(value),
            PgSetValue::StringLiteral(value) => Some(value),
            _ => None,
        }
    }
}

/// Extracted SHOW statement: `SHOW name`
pub struct PgShowStmt {
    pub name: String,
}

/// Try to extract a SET statement from a PG parse result.
/// Returns None if the statement is not a SET.
pub fn try_extract_set(parse_result: &ParseResult) -> Option<PgSetStmt> {
    use pg_query::NodeRef;

    if parse_result.protobuf.nodes().is_empty() {
        return None;
    }

    let node = &parse_result.protobuf.nodes()[0];
    let set_stmt = match &node.0 {
        NodeRef::VariableSetStmt(s) => s,
        _ => return None,
    };

    // Only handle VAR_SET_VALUE (kind == 1)
    if set_stmt.kind != 1 {
        return None;
    }

    // Extract the value from args
    let values = set_stmt
        .args
        .iter()
        .map(PgSetValue::from_node)
        .collect::<Option<Vec<_>>>()?;

    Some(PgSetStmt {
        name: set_stmt.name.clone(),
        values,
        is_local: set_stmt.is_local,
    })
}

pub fn is_set_transaction(parse_result: &ParseResult) -> bool {
    use pg_query::{protobuf::VariableSetKind, NodeRef};

    matches!(
        parse_result.protobuf.nodes().first().map(|node| &node.0),
        Some(NodeRef::VariableSetStmt(stmt))
            if stmt.kind == VariableSetKind::VarSetMulti as i32
                && stmt.name.eq_ignore_ascii_case("transaction")
    )
}

/// The byte offset of the table name in a CREATE TABLE statement that names
/// its table without a schema. None for temporary tables and for other
/// statements.
pub fn unqualified_create_table_name_location(parse_result: &ParseResult) -> Option<usize> {
    use pg_query::NodeRef;

    let nodes = parse_result.protobuf.nodes();
    let NodeRef::CreateStmt(create) = &nodes.first()?.0 else {
        return None;
    };
    let relation = create.relation.as_ref()?;
    if !relation.schemaname.is_empty() || relation.relpersistence == "t" {
        return None;
    }
    Some(relation.location as usize)
}

/// Try to extract a SHOW statement from a PG parse result.
/// Returns None if the statement is not a SHOW.
pub fn try_extract_show(parse_result: &ParseResult) -> Option<PgShowStmt> {
    use pg_query::NodeRef;

    if parse_result.protobuf.nodes().is_empty() {
        return None;
    }

    let node = &parse_result.protobuf.nodes()[0];
    let show_stmt = match &node.0 {
        NodeRef::VariableShowStmt(s) => s,
        _ => return None,
    };

    Some(PgShowStmt {
        name: show_stmt.name.clone(),
    })
}

#[derive(Debug, Clone)]
pub struct PgCreateSchemaStmt {
    pub name: String,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone)]
pub struct PgDropSchemaStmt {
    pub name: String,
    pub if_exists: bool,
    pub cascade: bool,
}

/// Try to extract a CREATE SCHEMA statement from pg_query parse output.
pub fn try_extract_create_schema(parse_result: &ParseResult) -> Option<PgCreateSchemaStmt> {
    use pg_query::NodeRef;

    let nodes = parse_result.protobuf.nodes();
    if nodes.is_empty() {
        return None;
    }
    let NodeRef::CreateSchemaStmt(cs) = &nodes[0].0 else {
        return None;
    };
    Some(PgCreateSchemaStmt {
        if_not_exists: cs.if_not_exists,
        name: cs.schemaname.clone(),
    })
}

/// Try to extract a DROP SCHEMA statement from pg_query parse output.
pub fn try_extract_drop_schema(parse_result: &ParseResult) -> Option<PgDropSchemaStmt> {
    use pg_query::protobuf::{DropBehavior, ObjectType};
    use pg_query::NodeRef;

    let nodes = parse_result.protobuf.nodes();
    if nodes.is_empty() {
        return None;
    }
    let NodeRef::DropStmt(drop) = &nodes[0].0 else {
        return None;
    };
    let remove_type = ObjectType::try_from(drop.remove_type).ok()?;
    if remove_type != ObjectType::ObjectSchema {
        return None;
    }

    // Extract schema name from first object (String node)
    let obj = drop.objects.first()?;
    let obj_node = obj.node.as_ref()?;
    let name = match obj_node.to_ref() {
        NodeRef::String(s) => s.sval.clone(),
        _ => return None,
    };

    let cascade = DropBehavior::try_from(drop.behavior).ok() == Some(DropBehavior::DropCascade);

    Some(PgDropSchemaStmt {
        name,
        if_exists: drop.missing_ok,
        cascade,
    })
}

/// Returns true if the parse result is a REFRESH MATERIALIZED VIEW statement.
/// Turso materialized views are live (auto-updating), so REFRESH is a no-op.
pub fn is_refresh_matview(parse_result: &ParseResult) -> bool {
    use pg_query::NodeRef;

    let nodes = parse_result.protobuf.nodes();
    if nodes.is_empty() {
        return false;
    }
    matches!(&nodes[0].0, NodeRef::RefreshMatViewStmt(_))
}

/// Returns true if the parse result is a COMMENT ON statement.
/// Comments are accepted for PostgreSQL compatibility but are not persisted.
pub fn is_comment_on(parse_result: &ParseResult) -> bool {
    use pg_query::NodeRef;

    let nodes = parse_result.protobuf.nodes();
    if nodes.is_empty() {
        return false;
    }
    matches!(&nodes[0].0, NodeRef::CommentStmt(_))
}

/// Extracted COPY FROM statement info for use by the connection layer.
#[derive(Debug, Clone)]
pub struct PgCopyFromStmt {
    pub table_name: String,
    pub schema_name: Option<String>,
    pub columns: Option<Vec<String>>,
    pub filename: String,
    pub delimiter: Option<String>,
    pub header: bool,
    pub null_string: Option<String>,
}

/// Try to extract a COPY FROM file statement from pg_query parse output.
/// Returns None if the statement is not a COPY FROM with a filename.
pub fn try_extract_copy_from(parse_result: &ParseResult) -> Option<PgCopyFromStmt> {
    use pg_query::NodeRef;

    let nodes = parse_result.protobuf.nodes();
    if nodes.is_empty() {
        return None;
    }
    let NodeRef::CopyStmt(copy) = &nodes[0].0 else {
        return None;
    };

    // Only handle COPY FROM with a file path (not STDIN, not COPY TO)
    if !copy.is_from || copy.filename.is_empty() || copy.is_program {
        return None;
    }

    let relation = copy.relation.as_ref()?;
    let table_name = relation.relname.clone();
    let schema_name = if relation.schemaname.is_empty()
        || matches!(
            relation.schemaname.to_lowercase().as_str(),
            "public" | "pg_catalog"
        ) {
        None
    } else {
        Some(relation.schemaname.clone())
    };

    let columns = if copy.attlist.is_empty() {
        None
    } else {
        let cols: Vec<String> = copy
            .attlist
            .iter()
            .filter_map(|n| match &n.node {
                Some(pg_query::protobuf::node::Node::String(s)) => Some(s.sval.clone()),
                _ => None,
            })
            .collect();
        Some(cols)
    };

    let mut delimiter = None;
    let mut header = false;
    let mut null_string = None;

    for opt in &copy.options {
        let Some(pg_query::protobuf::node::Node::DefElem(def)) = &opt.node else {
            continue;
        };
        match def.defname.as_str() {
            "format" => {
                // Only support text format for now
                if let Some(val) = def_elem_string_val(def) {
                    if val.to_lowercase() != "text" {
                        return None; // unsupported format
                    }
                }
            }
            "delimiter" => delimiter = def_elem_string_val(def),
            "header" => header = def_elem_bool_val(def).unwrap_or(true),
            "null" => null_string = def_elem_string_val(def),
            _ => {}
        }
    }

    Some(PgCopyFromStmt {
        table_name,
        schema_name,
        columns,
        filename: copy.filename.clone(),
        delimiter,
        header,
        null_string,
    })
}

/// Parse a SQL referential action string to an AST RefAct.
fn parse_ref_act(action: &str) -> Option<ast::RefAct> {
    match action.to_uppercase().as_str() {
        "CASCADE" => Some(ast::RefAct::Cascade),
        "SET NULL" => Some(ast::RefAct::SetNull),
        "SET DEFAULT" => Some(ast::RefAct::SetDefault),
        "RESTRICT" => Some(ast::RefAct::Restrict),
        "NO ACTION" => Some(ast::RefAct::NoAction),
        _ => None,
    }
}

/// Deparse a PG expression node into a SQL string.
/// Handles literals, column refs, comparisons, boolean ops, and function calls.
fn deparse_default_expr(node: &pg_query::protobuf::Node) -> Option<String> {
    use pg_query::protobuf::node::Node;

    match &node.node {
        Some(Node::Integer(i)) => Some(i.ival.to_string()),
        Some(Node::Float(f)) => Some(f.fval.clone()),
        Some(Node::String(s)) => Some(format!("'{}'", s.sval.replace('\'', "''"))),
        Some(Node::AConst(a_const)) => {
            if let Some(ref val) = a_const.val {
                use pg_query::protobuf::a_const::Val;
                match val {
                    Val::Ival(i) => Some(i.ival.to_string()),
                    Val::Fval(f) => Some(f.fval.clone()),
                    Val::Sval(s) => Some(format!("'{}'", s.sval.replace('\'', "''"))),
                    Val::Bsval(s) => Some(format!("'{}'", s.bsval.clone())),
                    Val::Boolval(b) => Some(if b.boolval { "1" } else { "0" }.to_string()),
                }
            } else if a_const.isnull {
                Some("NULL".to_string())
            } else {
                None
            }
        }
        Some(Node::ColumnRef(col_ref)) => {
            let parts: Vec<String> = col_ref
                .fields
                .iter()
                .filter_map(|f| match &f.node {
                    Some(Node::String(s)) => Some(s.sval.clone()),
                    _ => None,
                })
                .collect();
            Some(parts.join("."))
        }
        Some(Node::AExpr(a_expr)) => {
            let op = a_expr.name.iter().rev().find_map(|n| match &n.node {
                Some(Node::String(s)) => Some(s.sval.clone()),
                _ => None,
            })?;
            let left = a_expr.lexpr.as_deref().and_then(deparse_default_expr)?;
            let right = a_expr.rexpr.as_deref().and_then(deparse_default_expr)?;
            Some(format!("{left} {op} {right}"))
        }
        Some(Node::BoolExpr(bool_expr)) => {
            use pg_query::protobuf::BoolExprType;
            let parts: Vec<String> = bool_expr
                .args
                .iter()
                .filter_map(deparse_default_expr)
                .collect();
            match bool_expr.boolop() {
                BoolExprType::AndExpr => Some(parts.join(" AND ")),
                BoolExprType::OrExpr => Some(parts.join(" OR ")),
                BoolExprType::NotExpr => parts.first().map(|p| format!("NOT {p}")),
                _ => None,
            }
        }
        Some(Node::FuncCall(func_call)) => {
            let name: Vec<String> = func_call
                .funcname
                .iter()
                .filter_map(|n| match &n.node {
                    Some(Node::String(s)) => Some(s.sval.clone()),
                    _ => None,
                })
                .collect();
            let func_name = name.join(".");
            // Strip pg_catalog schema prefix — functions are registered
            // without it.
            let func_name = func_name.strip_prefix("pg_catalog.").unwrap_or(&func_name);
            let args: Vec<String> = func_call
                .args
                .iter()
                .filter_map(deparse_default_expr)
                .collect();
            Some(format!("{func_name}({args})", args = args.join(", ")))
        }
        Some(Node::SqlvalueFunction(svf)) => {
            use pg_query::protobuf::SqlValueFunctionOp;
            match SqlValueFunctionOp::try_from(svf.op) {
                Ok(
                    SqlValueFunctionOp::SvfopCurrentTimestamp
                    | SqlValueFunctionOp::SvfopCurrentTimestampN
                    | SqlValueFunctionOp::SvfopLocaltimestamp
                    | SqlValueFunctionOp::SvfopLocaltimestampN,
                ) => Some("strftime('%Y-%m-%d %H:%M:%f', 'now')".to_string()),
                Ok(SqlValueFunctionOp::SvfopCurrentDate) => Some("CURRENT_DATE".to_string()),
                Ok(
                    SqlValueFunctionOp::SvfopCurrentTime
                    | SqlValueFunctionOp::SvfopCurrentTimeN
                    | SqlValueFunctionOp::SvfopLocaltime
                    | SqlValueFunctionOp::SvfopLocaltimeN,
                ) => Some("CURRENT_TIME".to_string()),
                _ => None,
            }
        }
        Some(Node::TypeCast(type_cast)) => {
            // Handle DEFAULT expressions with type casts like '0'::integer
            type_cast.arg.as_deref().and_then(deparse_default_expr)
        }
        _ => None,
    }
}

fn function_call(name: &str, args: Vec<ast::Expr>) -> ast::Expr {
    ast::Expr::FunctionCall {
        name: ast::Name::from_string(name),
        distinctness: None,
        args: args.into_iter().map(Box::new).collect(),
        order_by: vec![],
        within_group: vec![],
        filter_over: ast::FunctionTail {
            filter_clause: None,
            over_clause: None,
        },
    }
}

/// SQLite's default SQLITE_MAX_LIKE_PATTERN_LENGTH, which core enforces on
/// GLOB patterns.
const SQLITE_MAX_LIKE_PATTERN_LENGTH: usize = 50000;

/// PostgreSQL's LIKE is case-sensitive like GLOB. `%` and `_` become `*` and
/// `?`, and a character escaped with `\` or special to GLOB is matched literally.
/// Returns None when GLOB cannot express the pattern: it ends with the escape
/// character, which is an error only once matching reaches it, or it grows past
/// SQLite's pattern length limit because each literal `*`, `?` and `[` takes
/// three bytes in GLOB.
fn like_pattern_to_glob(pattern: &str) -> Option<String> {
    let mut glob = String::with_capacity(pattern.len());
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        let literal = match c {
            '%' => {
                glob.push('*');
                continue;
            }
            '_' => {
                glob.push('?');
                continue;
            }
            '\\' => chars.next()?,
            c => c,
        };
        if matches!(literal, '*' | '?' | '[') {
            glob.push('[');
            glob.push(literal);
            glob.push(']');
        } else {
            glob.push(literal);
        }
    }
    (glob.len() <= SQLITE_MAX_LIKE_PATTERN_LENGTH).then_some(glob)
}

/// Convert a SQL SIMILAR TO pattern to a POSIX regex anchored with ^...$
/// `%` → `.*`, `_` → `.`, other regex metacharacters are kept as-is since
/// SIMILAR TO patterns are already regex-like in SQL standard.
fn similar_to_regex(pattern: &str) -> String {
    let mut regex = String::with_capacity(pattern.len() + 2);
    regex.push('^');
    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '%' => regex.push_str(".*"),
            '_' => regex.push('.'),
            '\\' => {
                // Escaped character: pass through literally
                if let Some(next) = chars.next() {
                    regex.push('\\');
                    regex.push(next);
                }
            }
            _ => regex.push(c),
        }
    }
    regex.push('$');
    regex
}

/// Converts a CamelCase identifier to UPPER CASE SQL keywords.
/// e.g. "CreateExtension" → "CREATE EXTENSION", "AlterRole" → "ALTER ROLE"
fn camel_to_sql(s: &str) -> String {
    let mut result = String::with_capacity(s.len() + 4);
    for ch in s.chars() {
        if ch.is_uppercase() && !result.is_empty() {
            result.push(' ');
        }
        result.push(ch.to_ascii_uppercase());
    }
    result
}

/// Returns a human-readable SQL name for a `NodeRef` statement type.
/// Strips the "Stmt" suffix and converts CamelCase → "CREATE EXTENSION" etc.
fn node_ref_name(node: &NodeRef) -> String {
    let debug = format!("{node:?}");
    let variant = debug.split('(').next().unwrap_or("unknown");
    camel_to_sql(variant.trim_end_matches("Stmt"))
}

/// Returns a human-readable name for an `AlterTableType` variant.
/// Strips the "At" prefix and converts CamelCase → SQL words.
fn alter_subtype_name(subtype: pg_query::protobuf::AlterTableType) -> String {
    let debug = format!("{subtype:?}");
    let trimmed = debug.strip_prefix("At").unwrap_or(&debug);
    camel_to_sql(trimmed)
}

/// Returns a human-readable name for a `DROP` object type.
/// Strips the "Object" prefix and uppercases.
fn drop_object_type_name(obj_type: pg_query::protobuf::ObjectType) -> String {
    let debug = format!("{obj_type:?}");
    debug
        .strip_prefix("Object")
        .unwrap_or(&debug)
        .to_ascii_uppercase()
}

fn not_supported(what: impl std::fmt::Display) -> ParseError {
    ParseError::ParseError(format!("{what} is not supported"))
}

/// Rejects constraint clauses that have no Turso equivalent. The parser
/// accepts the full PostgreSQL grammar, so a clause that is not translated
/// must fail here instead of being dropped.
fn reject_constraint_attributes(
    constraint: &pg_query::protobuf::Constraint,
    table_schema: &str,
) -> Result<(), ParseError> {
    use pg_query::protobuf::ConstrType;

    let contype = ConstrType::try_from(constraint.contype).unwrap_or(ConstrType::Undefined);
    // Table-level constraints carry deferral as flags; column-level ones get
    // separate attribute nodes after the constraint they modify.
    if constraint.deferrable
        || constraint.initdeferred
        || matches!(
            contype,
            ConstrType::ConstrAttrDeferrable | ConstrType::ConstrAttrDeferred
        )
    {
        return Err(not_supported("DEFERRABLE / INITIALLY DEFERRED constraints"));
    }
    // A foreign key can only reference a table in the referencing table's own
    // schema, because the translated clause carries no schema.
    if let Some(pktable) = &constraint.pktable {
        let schema_of =
            |s: &str| map_schema_name(if s.is_empty() { "public" } else { s }).to_string();
        if !pktable.catalogname.is_empty()
            || (!pktable.schemaname.is_empty()
                && schema_of(&pktable.schemaname) != schema_of(table_schema))
        {
            return Err(not_supported(
                "foreign key referencing a table in another schema",
            ));
        }
    }
    if !constraint.including.is_empty() {
        return Err(not_supported("INCLUDE on a constraint"));
    }
    if !constraint.options.is_empty() || !constraint.indexspace.is_empty() {
        return Err(not_supported(
            "WITH (storage parameters) / USING INDEX TABLESPACE on a constraint",
        ));
    }
    if !constraint.indexname.is_empty() {
        return Err(not_supported("constraint USING INDEX"));
    }
    if constraint.nulls_not_distinct {
        return Err(not_supported("NULLS NOT DISTINCT"));
    }
    if !constraint.fk_del_set_cols.is_empty() {
        return Err(not_supported(
            "ON DELETE SET NULL / SET DEFAULT with a column list",
        ));
    }
    // An empty match type is the default, and "s" is an explicit MATCH SIMPLE.
    if !matches!(constraint.fk_matchtype.as_str(), "" | "s") {
        return Err(not_supported("MATCH FULL / MATCH PARTIAL foreign keys"));
    }
    if constraint.is_no_inherit {
        return Err(not_supported("NO INHERIT constraints"));
    }
    if constraint.skip_validation {
        return Err(not_supported("NOT VALID constraints"));
    }
    Ok(())
}

fn constr_type_name(contype: pg_query::protobuf::ConstrType) -> String {
    use pg_query::protobuf::ConstrType;
    match contype {
        ConstrType::ConstrIdentity => "GENERATED ... AS IDENTITY".into(),
        ConstrType::ConstrGenerated => "GENERATED ALWAYS AS (...) STORED".into(),
        ConstrType::ConstrExclusion => "EXCLUDE".into(),
        ConstrType::ConstrCheck => "CHECK".into(),
        ConstrType::ConstrForeign => "REFERENCES".into(),
        other => {
            let debug = format!("{other:?}");
            debug
                .strip_prefix("Constr")
                .unwrap_or(&debug)
                .to_ascii_uppercase()
        }
    }
}

/// Rejects `TEMP`/`TEMPORARY` relations: the translator would otherwise
/// create a permanent, globally visible object.
fn reject_temporary(relation: &pg_query::protobuf::RangeVar, what: &str) -> Result<(), ParseError> {
    if relation.relpersistence == "t" {
        return Err(not_supported(what));
    }
    Ok(())
}

/// Rejects the physical-storage clauses of CREATE TABLE and its relatives.
fn reject_table_storage_clauses(
    options: &[pg_query::protobuf::Node],
    tablespace: &str,
    access_method: &str,
) -> Result<(), ParseError> {
    if !options.is_empty() {
        return Err(not_supported("WITH (storage parameters)"));
    }
    if !tablespace.is_empty() {
        return Err(not_supported("TABLESPACE"));
    }
    if !access_method.is_empty() && !access_method.eq_ignore_ascii_case("heap") {
        return Err(not_supported(format!(
            "table access method {access_method}"
        )));
    }
    Ok(())
}

/// Rejects column-definition clauses that are not translated.
fn reject_column_def_clauses(col_def: &pg_query::protobuf::ColumnDef) -> Result<(), ParseError> {
    if col_def.coll_clause.is_some() {
        return Err(not_supported("COLLATE on a column definition"));
    }
    Ok(())
}

/// Rejects per-column index clauses that are not translated.
fn reject_index_elem_clauses(
    elem: &pg_query::protobuf::IndexElem,
    context: &str,
) -> Result<(), ParseError> {
    if !elem.collation.is_empty() {
        return Err(not_supported(format!("COLLATE in {context}")));
    }
    if !elem.opclass.is_empty() || !elem.opclassopts.is_empty() {
        return Err(not_supported(format!("operator classes in {context}")));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_sequence_as_type_bounds_the_range() {
        let bounds = |sql: &str| {
            let stmt = PostgreSQLTranslator::new()
                .translate(&crate::parse(sql).unwrap())
                .unwrap();
            let ast::Stmt::CreateSequence {
                min_value,
                max_value,
                ..
            } = stmt
            else {
                panic!("expected CREATE SEQUENCE, got {stmt:?}");
            };
            (min_value, max_value)
        };
        // pg_dump emits AS integer for serial sequences.
        assert_eq!(
            bounds("CREATE SEQUENCE s AS integer START WITH 1"),
            (None, Some(i32::MAX as i64))
        );
        assert_eq!(
            bounds("CREATE SEQUENCE s AS smallint INCREMENT BY -1"),
            (Some(i16::MIN as i64), None)
        );
        assert_eq!(
            bounds("CREATE SEQUENCE s AS integer MAXVALUE 10"),
            (None, Some(10))
        );
        assert_eq!(
            bounds("CREATE SEQUENCE s AS bigint"),
            (None, Some(i64::MAX))
        );
        let err = PostgreSQLTranslator::new()
            .translate(&crate::parse("CREATE SEQUENCE s AS numeric").unwrap())
            .unwrap_err()
            .to_string();
        assert!(err.contains("AS numeric"), "{err}");
    }

    #[test]
    fn from_column_alias_lists_rename_columns() {
        let translate = |sql: &str| {
            PostgreSQLTranslator::new()
                .translate(&crate::parse(sql).unwrap())
                .map(|stmt| stmt.to_string())
        };
        let sql = translate("SELECT s.p, s.q FROM (SELECT 1, 2) AS s(p, q)").unwrap();
        assert!(sql.contains("1 p") && sql.contains("2 q"), "{sql}");
        let sql = translate("SELECT g.n FROM generate_series(1, 3) AS g(n)").unwrap();
        assert!(sql.contains("value n"), "{sql}");
        for (sql, needle) in [
            ("SELECT * FROM (SELECT * FROM t) AS s(a, b)", "selects *"),
            (
                "SELECT * FROM (VALUES (1, 2)) AS v(a, b)",
                "VALUES subquery",
            ),
            (
                "SELECT * FROM (SELECT 1) AS s(a, b)",
                "1 columns available but 2",
            ),
        ] {
            let err = translate(sql).unwrap_err().to_string();
            assert!(err.contains(needle), "{sql}: {err}");
        }
    }

    #[test]
    fn test_type_mapping() {
        let no_params: &[i64] = &[];
        let s = PgTypeMapping::scalar;
        let a = PgTypeMapping::array;

        // Base types (no Turso custom type)
        assert_eq!(map_pg_type("INTEGER", no_params), Some(s("INTEGER")));
        assert_eq!(map_pg_type("SERIAL", no_params), Some(s("INTEGER")));
        assert_eq!(map_pg_type("REAL", no_params), Some(s("REAL")));
        assert_eq!(map_pg_type("TEXT", no_params), Some(s("TEXT")));
        assert_eq!(map_pg_type("BLOB", no_params), Some(s("BLOB")));

        // Turso custom type equivalents
        assert_eq!(map_pg_type("BOOLEAN", no_params), Some(s("boolean")));
        assert_eq!(map_pg_type("SMALLINT", no_params), Some(s("smallint")));
        assert_eq!(map_pg_type("BIGINT", no_params), Some(s("bigint")));
        assert_eq!(map_pg_type("UUID", no_params), Some(s("uuid")));
        assert_eq!(map_pg_type("DATE", no_params), Some(s("date")));
        assert_eq!(map_pg_type("TIME", no_params), Some(s("time")));
        assert_eq!(map_pg_type("TIMESTAMP", no_params), Some(s("timestamp")));
        assert_eq!(
            map_pg_type("TIMESTAMPTZ", no_params),
            Some(s("timestamptz"))
        );
        assert_eq!(map_pg_type("BYTEA", no_params), Some(s("bytea")));
        assert_eq!(map_pg_type("INET", no_params), Some(s("inet")));
        assert_eq!(map_pg_type("JSON", no_params), Some(s("json")));
        assert_eq!(map_pg_type("JSONB", no_params), Some(s("jsonb")));

        // Parametric types — base name + params separated
        assert_eq!(
            map_pg_type("VARCHAR", &[100]),
            Some(PgTypeMapping::with_params("varchar", vec![100]))
        );
        assert_eq!(map_pg_type("VARCHAR", no_params), Some(s("TEXT")));
        assert_eq!(
            map_pg_type("NUMERIC", &[10, 2]),
            Some(PgTypeMapping::with_params("numeric", vec![10, 2]))
        );
        assert_eq!(
            map_pg_type("NUMERIC", &[10]),
            Some(PgTypeMapping::with_params("numeric", vec![10, 0]))
        );
        assert_eq!(map_pg_type("NUMERIC", no_params), Some(s("REAL")));

        // Network types → custom types
        assert_eq!(map_pg_type("CIDR", no_params), Some(s("cidr")));
        assert_eq!(map_pg_type("MACADDR", no_params), Some(s("macaddr")));
        assert_eq!(map_pg_type("MACADDR8", no_params), Some(s("macaddr8")));

        // Array types → base type + dimensions
        assert_eq!(map_pg_type("INTEGER[]", no_params), Some(a("INTEGER", 1)));
        assert_eq!(map_pg_type("TEXT[]", no_params), Some(a("TEXT", 1)));
        assert_eq!(map_pg_type("TEXT[][]", no_params), Some(a("TEXT", 2)));
        assert_eq!(map_pg_type("BOOLEAN[]", no_params), Some(a("boolean", 1)));
        assert_eq!(map_pg_type("BIGINT[]", no_params), Some(a("bigint", 1)));
        assert_eq!(map_pg_type("VARCHAR[]", no_params), Some(a("TEXT", 1)));
        // PG internal array notation
        assert_eq!(map_pg_type("_int4", no_params), Some(a("INTEGER", 1)));
        assert_eq!(map_pg_type("_text", no_params), Some(a("TEXT", 1)));

        // Unknown types pass through as-is (for user-defined enums etc.)
        assert_eq!(
            map_pg_type("SOMECUSTOMTYPE", no_params),
            Some(s("somecustomtype"))
        );
    }

    #[test]
    fn test_varchar_column_type() {
        let translator = PostgreSQLTranslator::new();
        let sql = "CREATE TABLE t(f1 varchar(4))";
        let parse_result = crate::parse(sql).unwrap();
        let stmt = translator.translate(&parse_result).unwrap();
        if let ast::Stmt::CreateTable { body, .. } = &stmt {
            if let ast::CreateTableBody::ColumnsAndConstraints { columns, .. } = body {
                let col = &columns[0];
                let col_type = col.col_type.as_ref().unwrap();
                assert_eq!(col_type.name, "varchar");
                assert!(
                    matches!(col_type.size, Some(ast::TypeSize::MaxSize(_))),
                    "expected MaxSize, got {:?}",
                    col_type.size
                );
            } else {
                panic!("expected ColumnsAndConstraints");
            }
        } else {
            panic!("expected CreateTable, got {stmt:?}");
        }
    }

    #[test]
    fn test_numeric_column_type() {
        let translator = PostgreSQLTranslator::new();
        let sql = "CREATE TABLE t(f1 numeric(10, 2))";
        let parse_result = crate::parse(sql).unwrap();
        let stmt = translator.translate(&parse_result).unwrap();
        if let ast::Stmt::CreateTable { body, .. } = &stmt {
            if let ast::CreateTableBody::ColumnsAndConstraints { columns, .. } = body {
                let col = &columns[0];
                let col_type = col.col_type.as_ref().unwrap();
                assert_eq!(col_type.name, "numeric");
                assert!(
                    matches!(col_type.size, Some(ast::TypeSize::TypeSize(_, _))),
                    "expected TypeSize, got {:?}",
                    col_type.size
                );
            } else {
                panic!("expected ColumnsAndConstraints");
            }
        } else {
            panic!("expected CreateTable, got {stmt:?}");
        }
    }

    #[test]
    fn test_unary_plus() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT +42";
        let parse_result = crate::parse(sql).unwrap();
        let stmt = translator.translate(&parse_result).unwrap();
        assert!(matches!(stmt, ast::Stmt::Select(_)));
    }

    #[test]
    fn test_unary_minus() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT -42";
        let parse_result = crate::parse(sql).unwrap();
        let stmt = translator.translate(&parse_result).unwrap();
        assert!(matches!(stmt, ast::Stmt::Select(_)));
    }

    #[test]
    fn test_basic_translation() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT * FROM users WHERE id = 1";
        let parse_result = crate::parse(sql).unwrap();
        let translated = translator.translate(&parse_result);

        assert!(translated.is_ok());

        if let Ok(ast::Stmt::Select(select)) = translated {
            // Check the select body
            if let ast::OneSelect::Select {
                columns,
                from,
                where_clause,
                ..
            } = &select.body.select
            {
                // Should have one result column (*)
                assert_eq!(columns.len(), 1);
                matches!(columns[0], ast::ResultColumn::Star);

                // Should have FROM clause
                assert!(from.is_some());

                // Should have WHERE clause
                assert!(where_clause.is_some());
            } else {
                panic!("Expected OneSelect::Select");
            }
        }
    }

    #[test]
    fn test_table_name_mapping() {
        let translator = PostgreSQLTranslator::new();

        // Test pg_tables passes through as a virtual table (not mapped to sqlite_master)
        let sql = "SELECT * FROM pg_tables";
        let parse_result = crate::parse(sql).unwrap();
        let translated = translator.translate(&parse_result).unwrap();

        if let ast::Stmt::Select(select) = translated {
            if let ast::OneSelect::Select { from, .. } = &select.body.select {
                if let Some(from_clause) = from {
                    if let ast::SelectTable::Table(qualified_name, _, _) = &*from_clause.select {
                        assert_eq!(qualified_name.name.as_str(), "pg_tables");
                    } else {
                        panic!("Expected table reference");
                    }
                } else {
                    panic!("Expected FROM clause");
                }
            } else {
                panic!("Expected OneSelect::Select");
            }
        } else {
            panic!("Expected select query");
        }
    }

    #[test]
    fn test_simple_select_star() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT * FROM sqlite_master";
        let parse_result = crate::parse(sql).unwrap();
        let translated = translator.translate(&parse_result);
        assert!(translated.is_ok());

        if let Ok(ast::Stmt::Select(select)) = translated {
            if let ast::OneSelect::Select { columns, from, .. } = &select.body.select {
                // Should have one result column: *
                assert_eq!(columns.len(), 1);
                assert!(
                    matches!(columns[0], ast::ResultColumn::Star),
                    "Expected ResultColumn::Star but got {:?}",
                    columns[0]
                );

                // Should have FROM clause
                if let Some(from_clause) = from {
                    if let ast::SelectTable::Table(qualified_name, alias, _) = &*from_clause.select
                    {
                        assert_eq!(qualified_name.name.as_str(), "sqlite_master");
                        assert!(alias.is_none());
                    } else {
                        panic!("Expected table reference");
                    }
                } else {
                    panic!("Expected FROM clause");
                }
            } else {
                panic!("Expected OneSelect::Select");
            }
        } else {
            panic!("Expected select query");
        }
    }

    #[test]
    fn test_column_expressions() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT id, name FROM users";
        let parse_result = crate::parse(sql).unwrap();
        let translated = translator.translate(&parse_result);
        assert!(translated.is_ok());

        if let Ok(ast::Stmt::Select(select)) = translated {
            if let ast::OneSelect::Select { columns, from, .. } = &select.body.select {
                // Should have two result columns: id, name
                assert_eq!(columns.len(), 2);

                // First column should be 'id'
                if let ast::ResultColumn::Expr(expr, alias) = &columns[0] {
                    assert!(
                        matches!(**expr, ast::Expr::Id(_)),
                        "Expected Name expression but got {expr:?}"
                    );
                    if let ast::Expr::Id(name) = &**expr {
                        assert_eq!(name.as_str(), "id");
                    }
                    assert!(alias.is_none());
                } else {
                    panic!("Expected expression result column for first column");
                }

                // Second column should be 'name'
                if let ast::ResultColumn::Expr(expr, alias) = &columns[1] {
                    assert!(
                        matches!(**expr, ast::Expr::Id(_)),
                        "Expected Name expression but got {expr:?}"
                    );
                    if let ast::Expr::Id(name) = &**expr {
                        assert_eq!(name.as_str(), "name");
                    }
                    assert!(alias.is_none());
                } else {
                    panic!("Expected expression result column for second column");
                }

                // Should have FROM clause
                assert!(from.is_some());
            } else {
                panic!("Expected OneSelect::Select");
            }
        } else {
            panic!("Expected select query");
        }
    }

    #[test]
    fn test_qualified_column_expressions() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT users.id, t.name FROM users t";
        let parse_result = crate::parse(sql).unwrap();
        let translated = translator.translate(&parse_result);
        assert!(translated.is_ok());

        if let Ok(ast::Stmt::Select(select)) = translated {
            if let ast::OneSelect::Select { columns, from, .. } = &select.body.select {
                // Should have two result columns: users.id, t.name
                assert_eq!(columns.len(), 2);

                // First column should be 'users.id'
                if let ast::ResultColumn::Expr(expr, alias) = &columns[0] {
                    assert!(
                        matches!(**expr, ast::Expr::Qualified(_, _)),
                        "Expected Qualified expression but got {expr:?}"
                    );
                    if let ast::Expr::Qualified(table_name, col_name) = &**expr {
                        assert_eq!(table_name.as_str(), "users");
                        assert_eq!(col_name.as_str(), "id");
                    }
                    assert!(alias.is_none());
                } else {
                    panic!("Expected expression result column for first qualified column");
                }

                // Second column should be 't.name'
                if let ast::ResultColumn::Expr(expr, alias) = &columns[1] {
                    assert!(
                        matches!(**expr, ast::Expr::Qualified(_, _)),
                        "Expected Qualified expression but got {expr:?}"
                    );
                    if let ast::Expr::Qualified(table_name, col_name) = &**expr {
                        assert_eq!(table_name.as_str(), "t");
                        assert_eq!(col_name.as_str(), "name");
                    }
                    assert!(alias.is_none());
                } else {
                    panic!("Expected expression result column for second qualified column");
                }

                // Should have FROM clause
                assert!(from.is_some());
            } else {
                panic!("Expected OneSelect::Select");
            }
        } else {
            panic!("Expected select query");
        }
    }

    #[test]
    fn test_select_with_where_clause() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT * FROM users WHERE id = 1";
        let parse_result = crate::parse(sql).unwrap();
        let translated = translator.translate(&parse_result);
        assert!(translated.is_ok());

        if let Ok(ast::Stmt::Select(select)) = translated {
            if let ast::OneSelect::Select {
                columns,
                from,
                where_clause,
                ..
            } = &select.body.select
            {
                // Should have SELECT *
                assert_eq!(columns.len(), 1);
                assert!(matches!(columns[0], ast::ResultColumn::Star));

                // Should have FROM clause
                assert!(from.is_some());

                // Should have WHERE clause
                assert!(where_clause.is_some());
                if let Some(where_expr) = where_clause {
                    // WHERE id = 1 should be a binary expression
                    assert!(
                        matches!(**where_expr, ast::Expr::Binary(_, _, _)),
                        "Expected Binary expression but got {where_expr:?}"
                    );
                    if let ast::Expr::Binary(left, op, right) = &**where_expr {
                        // Left side should be column 'id'
                        assert!(
                            matches!(**left, ast::Expr::Id(_)),
                            "Expected Name expression for left side"
                        );
                        if let ast::Expr::Id(name) = &**left {
                            assert_eq!(name.as_str(), "id");
                        }

                        // Operator should be Equals
                        assert!(
                            matches!(op, ast::Operator::Equals),
                            "Expected Equals operator"
                        );

                        // Right side should be literal 1
                        assert!(
                            matches!(**right, ast::Expr::Literal(_)),
                            "Expected Literal expression for right side"
                        );
                        if let ast::Expr::Literal(literal) = &**right {
                            assert!(
                                matches!(literal, ast::Literal::Numeric(_)),
                                "Expected numeric literal"
                            );
                            if let ast::Literal::Numeric(num_str) = literal {
                                assert_eq!(num_str, "1");
                            }
                        }
                    }
                }
            } else {
                panic!("Expected OneSelect::Select");
            }
        } else {
            panic!("Expected select query");
        }
    }

    #[test]
    fn test_comprehensive_translation() {
        let translator = PostgreSQLTranslator::new();

        // Test various PostgreSQL to Turso AST translations
        let test_cases = vec![
            ("SELECT * FROM sqlite_master", "SELECT * with no WHERE"),
            (
                "SELECT name FROM pg_tables WHERE name = 'users'",
                "SELECT with WHERE and table mapping",
            ),
            (
                "SELECT id, name, age FROM users WHERE age > 18",
                "SELECT multiple columns with WHERE",
            ),
            ("SELECT 'hello', 42 FROM users", "SELECT with literals"),
        ];

        for (sql, description) in test_cases {
            println!("Testing: {description}");
            let parse_result = crate::parse(sql).unwrap();
            let translated = translator.translate(&parse_result);
            assert!(
                translated.is_ok(),
                "Failed to translate: {sql} ({description})"
            );

            if let Ok(ast::Stmt::Select(select)) = translated {
                // Verify it's a valid Select AST
                match &select.body.select {
                    ast::OneSelect::Select { columns, .. } => {
                        assert!(!columns.is_empty(), "No columns in result for: {sql}");
                    }
                    _ => panic!("Expected OneSelect::Select for: {sql}"),
                }
            }
        }
    }

    #[test]
    fn test_literal_expressions() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT 'hello', 42, 3.14 FROM users";
        let parse_result = crate::parse(sql).unwrap();
        let translated = translator.translate(&parse_result);
        assert!(translated.is_ok());

        if let Ok(ast::Stmt::Select(select)) = translated {
            if let ast::OneSelect::Select { columns, .. } = &select.body.select {
                assert_eq!(columns.len(), 3);

                // Check string literal (wrapped in single quotes for Turso AST convention)
                if let ast::ResultColumn::Expr(expr, _) = &columns[0] {
                    if let ast::Expr::Literal(ast::Literal::String(s)) = &**expr {
                        assert_eq!(s, "'hello'");
                    } else {
                        panic!("Expected string literal");
                    }
                } else {
                    panic!("Expected expression result column");
                }

                // Check integer literal
                if let ast::ResultColumn::Expr(expr, _) = &columns[1] {
                    if let ast::Expr::Literal(ast::Literal::Numeric(n)) = &**expr {
                        assert_eq!(n, "42");
                    } else {
                        panic!("Expected numeric literal");
                    }
                } else {
                    panic!("Expected expression result column");
                }

                // Check float literal
                if let ast::ResultColumn::Expr(expr, _) = &columns[2] {
                    if let ast::Expr::Literal(ast::Literal::Numeric(n)) = &**expr {
                        assert_eq!(n, "3.14");
                    } else {
                        panic!("Expected numeric literal");
                    }
                } else {
                    panic!("Expected expression result column");
                }
            } else {
                panic!("Expected OneSelect::Select");
            }
        } else {
            panic!("Expected select query");
        }
    }

    #[test]
    fn test_bool_expr_and_translation() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT * FROM users WHERE age > 18 AND name = 'John'";
        let parse_result = crate::parse(sql).unwrap();
        let translated = translator.translate(&parse_result);
        assert!(translated.is_ok());

        if let Ok(ast::Stmt::Select(select)) = translated {
            if let ast::OneSelect::Select { where_clause, .. } = &select.body.select {
                assert!(where_clause.is_some());
                if let Some(where_expr) = where_clause {
                    // Should be a binary AND expression
                    assert!(
                        matches!(**where_expr, ast::Expr::Binary(_, ast::Operator::And, _)),
                        "Expected AND expression"
                    );
                }
            }
        }
    }

    #[test]
    fn test_bool_expr_or_translation() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT * FROM users WHERE age > 18 OR name = 'John'";
        let parse_result = crate::parse(sql).unwrap();
        let translated = translator.translate(&parse_result);
        assert!(translated.is_ok());

        if let Ok(ast::Stmt::Select(select)) = translated {
            if let ast::OneSelect::Select { where_clause, .. } = &select.body.select {
                assert!(where_clause.is_some());
                if let Some(where_expr) = where_clause {
                    // Should be a binary OR expression
                    assert!(
                        matches!(**where_expr, ast::Expr::Binary(_, ast::Operator::Or, _)),
                        "Expected OR expression"
                    );
                }
            }
        }
    }

    #[test]
    fn test_in_list_translation() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT * FROM users WHERE type IN ('admin', 'user', 'guest')";
        let parse_result = crate::parse(sql).unwrap();
        let translated = translator.translate(&parse_result);
        assert!(translated.is_ok());

        if let Ok(ast::Stmt::Select(select)) = translated {
            if let ast::OneSelect::Select { where_clause, .. } = &select.body.select {
                assert!(where_clause.is_some());
                if let Some(where_expr) = where_clause {
                    // Should be an InList expression
                    if let ast::Expr::InList { lhs, not, rhs } = &**where_expr {
                        assert!(!not, "Should not be NOT IN");
                        assert_eq!(rhs.len(), 3, "Should have 3 values in the IN list");

                        // Check that lhs is a column reference
                        assert!(
                            matches!(**lhs, ast::Expr::Id(_)),
                            "Left side should be a column name"
                        );

                        // Check that the list values are literals
                        for value in rhs {
                            assert!(
                                matches!(**value, ast::Expr::Literal(_)),
                                "IN list values should be literals"
                            );
                        }
                    } else {
                        panic!("Expected InList expression but got: {where_expr:?}");
                    }
                }
            }
        }
    }

    #[test]
    fn test_like_translation() {
        let where_expr = where_clause_of("SELECT * FROM users WHERE name LIKE 'John%'");
        let ast::Expr::Like {
            lhs,
            not: false,
            op: ast::LikeOperator::Glob,
            rhs,
            escape: None,
        } = &where_expr
        else {
            panic!("Expected GLOB expression but got: {where_expr:?}");
        };
        assert!(matches!(**lhs, ast::Expr::Id(_)));
        assert!(matches!(&**rhs, ast::Expr::Literal(ast::Literal::String(p)) if p == "'John*'"));
    }

    #[test]
    fn test_not_like_translation() {
        let where_expr = where_clause_of("SELECT * FROM users WHERE name NOT LIKE 'sqlite\\_%'");
        let ast::Expr::Like {
            not: true,
            op: ast::LikeOperator::Glob,
            rhs,
            ..
        } = &where_expr
        else {
            panic!("Expected NOT GLOB expression but got: {where_expr:?}");
        };
        assert!(matches!(&**rhs, ast::Expr::Literal(ast::Literal::String(p)) if p == "'sqlite_*'"));
    }

    #[test]
    fn test_like_with_column_pattern_translation() {
        let where_expr = where_clause_of("SELECT * FROM users WHERE name NOT LIKE pattern");
        let ast::Expr::Unary(ast::UnaryOperator::Not, inner) = &where_expr else {
            panic!("Expected NOT textlike() but got: {where_expr:?}");
        };
        assert!(
            matches!(&**inner, ast::Expr::FunctionCall { name, .. } if name.as_str() == "textlike"),
            "Expected textlike() call but got: {inner:?}"
        );
    }

    #[test]
    fn test_like_pattern_too_long_for_glob_uses_textlike() {
        let sql = format!(
            "SELECT * FROM users WHERE name LIKE '{}'",
            "*".repeat(16667)
        );
        let where_expr = where_clause_of(&sql);
        assert!(
            matches!(&where_expr, ast::Expr::FunctionCall { name, .. } if name.as_str() == "textlike"),
            "Expected textlike() call but got: {where_expr:?}"
        );
    }

    #[test]
    fn test_complex_schema_query_translation() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT type, name FROM sqlite_schema WHERE type IN ('table', 'index', 'view') AND name NOT LIKE 'sqlite_%'";
        let parse_result = crate::parse(sql).unwrap();
        let translated = translator.translate(&parse_result);
        assert!(translated.is_ok());

        if let Ok(ast::Stmt::Select(select)) = translated {
            if let ast::OneSelect::Select {
                columns,
                from,
                where_clause,
                ..
            } = &select.body.select
            {
                // Check columns
                assert_eq!(columns.len(), 2, "Should have 2 columns");

                // Check FROM clause
                assert!(from.is_some(), "Should have FROM clause");

                // Check WHERE clause structure
                assert!(where_clause.is_some(), "Should have WHERE clause");
                if let Some(where_expr) = where_clause {
                    // Should be an AND expression
                    if let ast::Expr::Binary(left, op, right) = &**where_expr {
                        assert!(matches!(op, ast::Operator::And), "Top level should be AND");

                        let (ast::Expr::Parenthesized(left), ast::Expr::Parenthesized(right)) =
                            (&**left, &**right)
                        else {
                            panic!("Both sides of AND should be parenthesized");
                        };

                        // Left side should be IN expression
                        assert!(
                            matches!(&left[..], [inner] if matches!(**inner, ast::Expr::InList { .. })),
                            "Left side should be IN list"
                        );

                        // Right side should be NOT LIKE expression
                        assert!(
                            matches!(&right[..], [inner] if matches!(**inner, ast::Expr::Like { not: true, .. })),
                            "Right side should be NOT LIKE"
                        );
                    } else {
                        panic!("WHERE clause should be Binary expression");
                    }
                }
            }
        } else {
            panic!("Translation should succeed");
        }
    }

    #[test]
    fn test_group_by_translation() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT age, COUNT(*) FROM users GROUP BY age";
        let parse_result = crate::parse(sql).unwrap();
        let translated = translator.translate(&parse_result).unwrap();

        if let ast::Stmt::Select(select) = translated {
            if let ast::OneSelect::Select { group_by, .. } = &select.body.select {
                let gb = group_by.as_ref().expect("Should have GROUP BY");
                assert_eq!(gb.exprs.len(), 1);
                assert!(gb.having.is_none());
            } else {
                panic!("Expected OneSelect::Select");
            }
        } else {
            panic!("Expected Select");
        }
    }

    #[test]
    fn test_group_by_having_translation() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT age, COUNT(*) FROM users GROUP BY age HAVING COUNT(*) > 1";
        let parse_result = crate::parse(sql).unwrap();
        let translated = translator.translate(&parse_result).unwrap();

        if let ast::Stmt::Select(select) = translated {
            if let ast::OneSelect::Select { group_by, .. } = &select.body.select {
                let gb = group_by.as_ref().expect("Should have GROUP BY");
                assert_eq!(gb.exprs.len(), 1);
                assert!(gb.having.is_some(), "Should have HAVING clause");
            } else {
                panic!("Expected OneSelect::Select");
            }
        } else {
            panic!("Expected Select");
        }
    }

    #[test]
    fn test_distinct_translation() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT DISTINCT name FROM users";
        let parse_result = crate::parse(sql).unwrap();
        let translated = translator.translate(&parse_result).unwrap();

        if let ast::Stmt::Select(select) = translated {
            if let ast::OneSelect::Select { distinctness, .. } = &select.body.select {
                assert!(
                    matches!(distinctness, Some(ast::Distinctness::Distinct)),
                    "Should have DISTINCT"
                );
            } else {
                panic!("Expected OneSelect::Select");
            }
        } else {
            panic!("Expected Select");
        }
    }

    #[test]
    fn test_limit_offset_translation() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT * FROM users LIMIT 10 OFFSET 5";
        let parse_result = crate::parse(sql).unwrap();
        let translated = translator.translate(&parse_result).unwrap();

        if let ast::Stmt::Select(select) = translated {
            let limit = select.limit.as_ref().expect("Should have LIMIT");
            assert!(limit.offset.is_some(), "Should have OFFSET");
        } else {
            panic!("Expected Select");
        }
    }

    #[test]
    fn test_begin_commit_rollback_translation() {
        let translator = PostgreSQLTranslator::new();

        let begin = crate::parse("BEGIN").unwrap();
        assert!(matches!(
            translator.translate(&begin).unwrap(),
            ast::Stmt::Begin { .. }
        ));

        let commit = crate::parse("COMMIT").unwrap();
        assert!(matches!(
            translator.translate(&commit).unwrap(),
            ast::Stmt::Commit { .. }
        ));

        let rollback = crate::parse("ROLLBACK").unwrap();
        assert!(matches!(
            translator.translate(&rollback).unwrap(),
            ast::Stmt::Rollback { .. }
        ));
    }

    #[test]
    fn test_drop_table_translation() {
        let translator = PostgreSQLTranslator::new();

        let sql = "DROP TABLE users";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();
        if let ast::Stmt::DropTable {
            if_exists,
            tbl_name,
        } = translated
        {
            assert!(!if_exists);
            assert_eq!(tbl_name.name.as_str(), "users");
        } else {
            panic!("Expected DropTable");
        }

        let sql = "DROP TABLE IF EXISTS users";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();
        if let ast::Stmt::DropTable { if_exists, .. } = translated {
            assert!(if_exists);
        } else {
            panic!("Expected DropTable");
        }
    }

    #[test]
    fn test_insert_on_conflict_do_nothing() {
        let translator = PostgreSQLTranslator::new();
        let sql = "INSERT INTO users (id, name) VALUES (1, 'Alice') ON CONFLICT (id) DO NOTHING";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        if let ast::Stmt::Insert { body, .. } = translated {
            if let ast::InsertBody::Select(_, upsert) = body {
                let upsert = upsert.expect("Should have ON CONFLICT");
                assert!(matches!(upsert.do_clause, ast::UpsertDo::Nothing));
                let index = upsert.index.as_ref().expect("Should have conflict target");
                assert_eq!(index.targets.len(), 1);
            } else {
                panic!("Expected InsertBody::Select");
            }
        } else {
            panic!("Expected Insert");
        }
    }

    #[test]
    fn test_insert_on_conflict_do_update() {
        let translator = PostgreSQLTranslator::new();
        let sql = "INSERT INTO users (id, name) VALUES (1, 'Alice') ON CONFLICT (id) DO UPDATE SET name = 'Bob'";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        if let ast::Stmt::Insert { body, .. } = translated {
            if let ast::InsertBody::Select(_, upsert) = body {
                let upsert = upsert.expect("Should have ON CONFLICT");
                if let ast::UpsertDo::Set { sets, .. } = &upsert.do_clause {
                    assert_eq!(sets.len(), 1);
                    assert_eq!(sets[0].col_names[0].as_str(), "name");
                } else {
                    panic!("Expected UpsertDo::Set");
                }
            } else {
                panic!("Expected InsertBody::Select");
            }
        } else {
            panic!("Expected Insert");
        }
    }

    #[test]
    fn test_insert_returning() {
        let translator = PostgreSQLTranslator::new();
        let sql = "INSERT INTO users (name) VALUES ('Alice') RETURNING id, name";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        if let ast::Stmt::Insert { returning, .. } = translated {
            assert_eq!(returning.len(), 2);
        } else {
            panic!("Expected Insert");
        }
    }

    #[test]
    fn test_update_returning() {
        let translator = PostgreSQLTranslator::new();
        let sql = "UPDATE users SET name = 'Bob' WHERE id = 1 RETURNING *";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        if let ast::Stmt::Update(update) = translated {
            assert_eq!(update.returning.len(), 1);
            assert!(matches!(update.returning[0], ast::ResultColumn::Star));
        } else {
            panic!("Expected Update");
        }
    }

    #[test]
    fn test_delete_returning() {
        let translator = PostgreSQLTranslator::new();
        let sql = "DELETE FROM users WHERE id = 1 RETURNING id, name";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        if let ast::Stmt::Delete { returning, .. } = translated {
            assert_eq!(returning.len(), 2);
        } else {
            panic!("Expected Delete");
        }
    }

    #[test]
    fn test_parameterized_insert() {
        let translator = PostgreSQLTranslator::new();
        let sql = "INSERT INTO users (id, name, age) VALUES ($1, $2, $3)";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        if let ast::Stmt::Insert {
            tbl_name,
            columns,
            body,
            ..
        } = translated
        {
            assert_eq!(tbl_name.name.as_str(), "users");
            assert_eq!(columns.len(), 3);
            // Verify the body has VALUES with $1, $2, $3 parameters
            if let ast::InsertBody::Select(select, _) = body {
                if let ast::OneSelect::Values(rows) = &select.body.select {
                    assert_eq!(rows.len(), 1);
                    assert_eq!(rows[0].len(), 3);
                } else {
                    panic!("Expected Values");
                }
            } else {
                panic!("Expected Select body");
            }
        } else {
            panic!("Expected Insert");
        }
    }

    #[test]
    fn test_parameterized_select() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT * FROM users WHERE age > $1 AND name = $2";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        // Should translate successfully without errors
        assert!(matches!(translated, ast::Stmt::Select(_)));
    }

    #[test]
    fn test_parameterized_update() {
        let translator = PostgreSQLTranslator::new();
        let sql = "UPDATE users SET age = $1 WHERE id = $2";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        assert!(matches!(translated, ast::Stmt::Update { .. }));
    }

    #[test]
    fn test_parameterized_delete() {
        let translator = PostgreSQLTranslator::new();
        let sql = "DELETE FROM users WHERE id = $1";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        assert!(matches!(translated, ast::Stmt::Delete { .. }));
    }

    #[test]
    fn test_insert_with_default_values() {
        let translator = PostgreSQLTranslator::new();
        // Drizzle generates DEFAULT for columns with default values
        let sql = "INSERT INTO users (id, name, verified, jsonb) VALUES ($1, $2, default, default)";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        if let ast::Stmt::Insert { columns, body, .. } = translated {
            // DEFAULT columns should be stripped
            assert_eq!(columns.len(), 2, "should only have id and name columns");
            assert_eq!(columns[0].as_str(), "id");
            assert_eq!(columns[1].as_str(), "name");
            // VALUES should also have 2 entries per row
            if let ast::InsertBody::Select(select, _) = body {
                if let ast::OneSelect::Values(rows) = &select.body.select {
                    assert_eq!(rows.len(), 1);
                    assert_eq!(rows[0].len(), 2, "should only have 2 values per row");
                } else {
                    panic!("Expected Values");
                }
            }
        } else {
            panic!("Expected Insert");
        }
    }

    #[test]
    fn test_insert_multi_row_with_defaults() {
        let translator = PostgreSQLTranslator::new();
        let sql = "INSERT INTO t (a, b, c) VALUES ($1, default, $2), ($3, default, $4)";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        if let ast::Stmt::Insert { columns, body, .. } = translated {
            assert_eq!(columns.len(), 2, "column b should be stripped");
            assert_eq!(columns[0].as_str(), "a");
            assert_eq!(columns[1].as_str(), "c");
            if let ast::InsertBody::Select(select, _) = body {
                if let ast::OneSelect::Values(rows) = &select.body.select {
                    assert_eq!(rows.len(), 2);
                    assert_eq!(rows[0].len(), 2);
                    assert_eq!(rows[1].len(), 2);
                } else {
                    panic!("Expected Values");
                }
            }
        } else {
            panic!("Expected Insert");
        }
    }

    #[test]
    fn test_cte_simple() {
        let translator = PostgreSQLTranslator::new();
        let sql = "WITH sq AS (SELECT 1 AS val) SELECT val FROM sq";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        if let ast::Stmt::Select(select) = translated {
            assert!(select.with.is_some(), "WITH clause should be present");
            let with = select.with.unwrap();
            assert_eq!(with.ctes.len(), 1);
            assert_eq!(with.ctes[0].tbl_name.as_str(), "sq");
        } else {
            panic!("Expected Select");
        }
    }

    #[test]
    fn test_cte_multiple() {
        let translator = PostgreSQLTranslator::new();
        let sql = "WITH a AS (SELECT 1 AS x), b AS (SELECT 2 AS y) SELECT x, y FROM a, b";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        if let ast::Stmt::Select(select) = translated {
            let with = select.with.unwrap();
            assert_eq!(with.ctes.len(), 2);
            assert_eq!(with.ctes[0].tbl_name.as_str(), "a");
            assert_eq!(with.ctes[1].tbl_name.as_str(), "b");
        } else {
            panic!("Expected Select");
        }
    }

    #[test]
    fn test_exists_subquery() {
        let translator = PostgreSQLTranslator::new();
        let sql =
            r#"SELECT name FROM users WHERE EXISTS (SELECT 1 FROM users WHERE name = 'Alice')"#;
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        if let ast::Stmt::Select(select) = translated {
            let one_select = &select.body.select;
            if let ast::OneSelect::Select { where_clause, .. } = one_select {
                let where_expr = where_clause.as_ref().expect("Should have WHERE clause");
                assert!(
                    matches!(where_expr.as_ref(), ast::Expr::Exists(_)),
                    "Expected Exists expression"
                );
            } else {
                panic!("Expected Select variant");
            }
        } else {
            panic!("Expected Select");
        }
    }

    #[test]
    fn test_scalar_subquery() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT name FROM users WHERE salary > (SELECT AVG(salary) FROM users)";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        if let ast::Stmt::Select(select) = translated {
            let one_select = &select.body.select;
            if let ast::OneSelect::Select { where_clause, .. } = one_select {
                let where_expr = where_clause.as_ref().expect("Should have WHERE clause");
                // Should be BinaryOp(salary > Subquery(...))
                if let ast::Expr::Binary(_, op, rhs) = where_expr.as_ref() {
                    assert_eq!(*op, ast::Operator::Greater);
                    assert!(
                        matches!(rhs.as_ref(), ast::Expr::Subquery(_)),
                        "Expected Subquery expression on RHS"
                    );
                } else {
                    panic!("Expected Binary expression, got: {where_expr:?}");
                }
            } else {
                panic!("Expected Select variant");
            }
        } else {
            panic!("Expected Select");
        }
    }

    #[test]
    fn test_in_subquery() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT name FROM users WHERE id IN (SELECT user_id FROM orders)";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        if let ast::Stmt::Select(select) = translated {
            let one_select = &select.body.select;
            if let ast::OneSelect::Select { where_clause, .. } = one_select {
                let where_expr = where_clause.as_ref().expect("Should have WHERE clause");
                assert!(
                    matches!(where_expr.as_ref(), ast::Expr::InSelect { not: false, .. }),
                    "Expected InSelect expression"
                );
            } else {
                panic!("Expected Select variant");
            }
        } else {
            panic!("Expected Select");
        }
    }

    #[test]
    fn test_join_after_a_comma_keeps_its_order() {
        let translator = PostgreSQLTranslator::new();
        for (sql, expected) in [
            (
                "SELECT * FROM a, b JOIN c ON b.y < c.z",
                "SELECT * FROM a, b INNER JOIN c ON b.y < c.z",
            ),
            (
                "SELECT * FROM a, b CROSS JOIN c",
                "SELECT * FROM a, b INNER JOIN c",
            ),
            (
                "SELECT * FROM a, b CROSS JOIN LATERAL (SELECT a.x, b.y) s",
                "SELECT * FROM a, b INNER JOIN LATERAL (SELECT a.x, b.y) s",
            ),
        ] {
            let parsed = crate::parse(sql).unwrap();
            let translated = translator.translate(&parsed).unwrap();
            assert_eq!(translated.to_string(), expected);
        }
    }

    #[test]
    fn test_lateral_subquery() {
        let translator = PostgreSQLTranslator::new();
        for (sql, expected) in [
            (
                "SELECT * FROM t1, LATERAL (SELECT t1.a) s",
                "SELECT * FROM t1, LATERAL (SELECT t1.a) s",
            ),
            (
                "SELECT * FROM t1 LEFT JOIN LATERAL (SELECT t1.a) s ON true",
                "SELECT * FROM t1 LEFT OUTER JOIN LATERAL (SELECT t1.a) s ON 1",
            ),
            (
                "SELECT * FROM t1, (SELECT 1) s",
                "SELECT * FROM t1, (SELECT 1) s",
            ),
        ] {
            let parsed = crate::parse(sql).unwrap();
            let translated = translator.translate(&parsed).unwrap();
            assert_eq!(translated.to_string(), expected);
        }
    }

    #[test]
    fn test_right_or_full_join_after_a_comma_is_rejected() {
        let translator = PostgreSQLTranslator::new();
        for sql in [
            "SELECT * FROM a, b RIGHT JOIN c ON true",
            "SELECT * FROM a, b FULL JOIN c ON true",
            "SELECT * FROM a, b JOIN c ON true RIGHT JOIN d ON true",
        ] {
            let parsed = crate::parse(sql).unwrap();
            let err = translator.translate(&parsed).unwrap_err();
            assert_eq!(
                err.to_string(),
                "RIGHT and FULL joins after a comma in FROM are not supported",
                "{sql}"
            );
        }
    }

    #[test]
    fn test_join_subquery() {
        let translator = PostgreSQLTranslator::new();
        let sql = r#"SELECT c.name, sq.cnt FROM cities c LEFT JOIN (SELECT city_id, count(*) as cnt FROM users GROUP BY city_id) sq ON c.id = sq.city_id"#;
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        if let ast::Stmt::Select(select) = translated {
            let one_select = &select.body.select;
            if let ast::OneSelect::Select { from, .. } = one_select {
                let from_clause = from.as_ref().expect("Should have FROM clause");
                assert_eq!(from_clause.joins.len(), 1, "Should have one join");
                let join = &from_clause.joins[0];
                assert!(
                    matches!(
                        join.table.as_ref(),
                        ast::SelectTable::Select { alias: Some(_), .. }
                    ),
                    "Join RHS should be a subquery with alias"
                );
            } else {
                panic!("Expected Select variant");
            }
        } else {
            panic!("Expected Select");
        }
    }

    #[test]
    fn test_offset_without_limit() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT * FROM users OFFSET 5";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        if let ast::Stmt::Select(select) = translated {
            let lim = select
                .limit
                .as_ref()
                .expect("Should have limit (LIMIT -1 OFFSET 5)");
            // LIMIT should be -1 (unlimited)
            if let ast::Expr::Literal(ast::Literal::Numeric(n)) = lim.expr.as_ref() {
                assert_eq!(n, "-1");
            } else {
                panic!("Expected Numeric(-1) limit");
            }
            // OFFSET should be present
            assert!(lim.offset.is_some(), "Should have OFFSET");
        } else {
            panic!("Expected Select");
        }
    }

    #[test]
    fn test_between_translation() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT * FROM users WHERE age BETWEEN 18 AND 65";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        if let ast::Stmt::Select(select) = translated {
            if let ast::OneSelect::Select { where_clause, .. } = &select.body.select {
                let w = where_clause.as_ref().expect("should have WHERE");
                assert!(
                    matches!(&**w, ast::Expr::Between { not: false, .. }),
                    "Expected BETWEEN expression, got: {w:?}"
                );
            } else {
                panic!("Expected Select variant");
            }
        }
    }

    #[test]
    fn test_not_between_translation() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT * FROM t WHERE x NOT BETWEEN 1 AND 10";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        if let ast::Stmt::Select(select) = translated {
            if let ast::OneSelect::Select { where_clause, .. } = &select.body.select {
                let w = where_clause.as_ref().unwrap();
                assert!(matches!(&**w, ast::Expr::Between { not: true, .. }));
            }
        }
    }

    #[test]
    fn test_is_distinct_from_translation() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT * FROM t WHERE a IS DISTINCT FROM b";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        if let ast::Stmt::Select(select) = translated {
            if let ast::OneSelect::Select { where_clause, .. } = &select.body.select {
                let w = where_clause.as_ref().unwrap();
                assert!(
                    matches!(&**w, ast::Expr::Binary(_, ast::Operator::IsNot, _)),
                    "IS DISTINCT FROM should map to IS NOT, got: {w:?}"
                );
            }
        }
    }

    #[test]
    fn test_is_not_distinct_from_translation() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT * FROM t WHERE a IS NOT DISTINCT FROM b";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        if let ast::Stmt::Select(select) = translated {
            if let ast::OneSelect::Select { where_clause, .. } = &select.body.select {
                let w = where_clause.as_ref().unwrap();
                assert!(matches!(&**w, ast::Expr::Binary(_, ast::Operator::Is, _)));
            }
        }
    }

    #[test]
    fn test_nullif_translation() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT NULLIF(a, 0) FROM t";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        if let ast::Stmt::Select(select) = translated {
            if let ast::OneSelect::Select { columns, .. } = &select.body.select {
                assert_eq!(columns.len(), 1);
                if let ast::ResultColumn::Expr(expr, _) = &columns[0] {
                    if let ast::Expr::FunctionCall { name, args, .. } = &**expr {
                        assert_eq!(name.as_str(), "NULLIF");
                        assert_eq!(args.len(), 2);
                    } else {
                        panic!("Expected FunctionCall, got: {expr:?}");
                    }
                }
            }
        }
    }

    #[test]
    fn test_coalesce_translation() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT COALESCE(a, b, 0) FROM t";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        if let ast::Stmt::Select(select) = translated {
            if let ast::OneSelect::Select { columns, .. } = &select.body.select {
                if let ast::ResultColumn::Expr(expr, _) = &columns[0] {
                    if let ast::Expr::FunctionCall { name, args, .. } = &**expr {
                        assert_eq!(name.as_str(), "COALESCE");
                        assert_eq!(args.len(), 3);
                    } else {
                        panic!("Expected COALESCE function call");
                    }
                }
            }
        }
    }

    #[test]
    fn test_boolean_test_is_true() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT * FROM t WHERE active IS TRUE";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        if let ast::Stmt::Select(select) = translated {
            if let ast::OneSelect::Select { where_clause, .. } = &select.body.select {
                let w = where_clause.as_ref().unwrap();
                assert!(
                    matches!(&**w, ast::Expr::Binary(_, ast::Operator::Is, _)),
                    "IS TRUE should map to IS 1, got: {w:?}"
                );
            }
        }
    }

    #[test]
    fn test_boolean_test_is_false() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT * FROM t WHERE active IS FALSE";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        if let ast::Stmt::Select(select) = translated {
            if let ast::OneSelect::Select { where_clause, .. } = &select.body.select {
                let w = where_clause.as_ref().unwrap();
                assert!(matches!(&**w, ast::Expr::Binary(_, ast::Operator::Is, _)));
            }
        }
    }

    #[test]
    fn test_boolean_test_is_unknown() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT * FROM t WHERE x IS UNKNOWN";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        if let ast::Stmt::Select(select) = translated {
            if let ast::OneSelect::Select { where_clause, .. } = &select.body.select {
                let w = where_clause.as_ref().unwrap();
                assert!(
                    matches!(&**w, ast::Expr::IsNull(_)),
                    "IS UNKNOWN should map to IS NULL"
                );
            }
        }
    }

    #[test]
    fn test_drop_view_translation() {
        let translator = PostgreSQLTranslator::new();
        let sql = "DROP VIEW IF EXISTS my_view";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        assert!(
            matches!(
                translated,
                ast::Stmt::DropView {
                    if_exists: true,
                    ..
                }
            ),
            "Expected DropView with IF EXISTS"
        );
    }

    #[test]
    fn test_savepoint_translation() {
        let translator = PostgreSQLTranslator::new();

        let sql = "SAVEPOINT sp1";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();
        if let ast::Stmt::Savepoint { name } = &translated {
            assert_eq!(name.as_str(), "sp1");
        } else {
            panic!("Expected Savepoint, got: {translated:?}");
        }

        let sql = "RELEASE SAVEPOINT sp1";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();
        if let ast::Stmt::Release { name } = &translated {
            assert_eq!(name.as_str(), "sp1");
        } else {
            panic!("Expected Release, got: {translated:?}");
        }

        let sql = "ROLLBACK TO SAVEPOINT sp1";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();
        if let ast::Stmt::Rollback { savepoint_name, .. } = &translated {
            assert_eq!(savepoint_name.as_ref().unwrap().as_str(), "sp1");
        } else {
            panic!("Expected Rollback with savepoint, got: {translated:?}");
        }
    }

    #[test]
    fn test_bitwise_operators() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT a & b, a | b, a << 2, a >> 1 FROM t";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        if let ast::Stmt::Select(select) = translated {
            if let ast::OneSelect::Select { columns, .. } = &select.body.select {
                assert_eq!(columns.len(), 4);
                // a & b
                if let ast::ResultColumn::Expr(expr, _) = &columns[0] {
                    assert!(matches!(
                        &**expr,
                        ast::Expr::Binary(_, ast::Operator::BitwiseAnd, _)
                    ));
                }
                // a | b
                if let ast::ResultColumn::Expr(expr, _) = &columns[1] {
                    assert!(matches!(
                        &**expr,
                        ast::Expr::Binary(_, ast::Operator::BitwiseOr, _)
                    ));
                }
                // a << 2
                if let ast::ResultColumn::Expr(expr, _) = &columns[2] {
                    assert!(matches!(
                        &**expr,
                        ast::Expr::Binary(_, ast::Operator::LeftShift, _)
                    ));
                }
                // a >> 1
                if let ast::ResultColumn::Expr(expr, _) = &columns[3] {
                    assert!(matches!(
                        &**expr,
                        ast::Expr::Binary(_, ast::Operator::RightShift, _)
                    ));
                }
            }
        }
    }

    #[test]
    fn test_current_timestamp_translation() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT CURRENT_TIMESTAMP, CURRENT_DATE, CURRENT_TIME FROM t";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        let ast::Stmt::Select(selected) = translated else {
            panic!("Expected SELECT statement");
        };

        let ast::OneSelect::Select { columns, .. } = &selected.body.select else {
            panic!("Expected SELECT body");
        };

        let expected = [
            ast::Expr::Literal(ast::Literal::CurrentTimestamp),
            ast::Expr::Literal(ast::Literal::CurrentDate),
            ast::Expr::Literal(ast::Literal::CurrentTime),
        ];

        assert_eq!(columns.len(), expected.len());

        for (col, expected_expr) in columns.iter().zip(&expected) {
            let ast::ResultColumn::Expr(expr, _) = col else {
                panic!("Expected column expression");
            };
            assert_eq!(expr.as_ref(), expected_expr);
        }
    }

    #[test]
    fn test_greatest_least_translation() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT GREATEST(a, b, c), LEAST(x, y) FROM t";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        if let ast::Stmt::Select(select) = translated {
            if let ast::OneSelect::Select { columns, .. } = &select.body.select {
                assert_eq!(columns.len(), 2);
                if let ast::ResultColumn::Expr(expr, _) = &columns[0] {
                    if let ast::Expr::FunctionCall { name, args, .. } = &**expr {
                        assert_eq!(name.as_str(), "MAX");
                        assert_eq!(args.len(), 3);
                    } else {
                        panic!("Expected MAX function call");
                    }
                }
                if let ast::ResultColumn::Expr(expr, _) = &columns[1] {
                    if let ast::Expr::FunctionCall { name, args, .. } = &**expr {
                        assert_eq!(name.as_str(), "MIN");
                        assert_eq!(args.len(), 2);
                    } else {
                        panic!("Expected MIN function call");
                    }
                }
            }
        }
    }

    #[test]
    fn test_truncate_translation() {
        let translator = PostgreSQLTranslator::new();
        let sql = "TRUNCATE TABLE users";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        if let ast::Stmt::Delete {
            tbl_name,
            where_clause,
            ..
        } = translated
        {
            assert_eq!(tbl_name.name.as_str(), "users");
            assert!(where_clause.is_none(), "TRUNCATE should have no WHERE");
        } else {
            panic!("Expected Delete for TRUNCATE, got: {translated:?}");
        }
    }

    #[test]
    fn test_create_view_translation() {
        let translator = PostgreSQLTranslator::new();
        let sql = "CREATE VIEW active_users AS SELECT * FROM users WHERE active = true";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        if let ast::Stmt::CreateView {
            view_name,
            temporary,
            ..
        } = translated
        {
            assert_eq!(view_name.name.as_str(), "active_users");
            assert!(!temporary);
        } else {
            panic!("Expected CreateView, got: {translated:?}");
        }
    }

    #[test]
    fn test_create_view_with_columns() {
        let translator = PostgreSQLTranslator::new();
        let sql = "CREATE VIEW v (col1, col2) AS SELECT a, b FROM t";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        if let ast::Stmt::CreateView {
            view_name, columns, ..
        } = translated
        {
            assert_eq!(view_name.name.as_str(), "v");
            assert_eq!(columns.len(), 2);
            assert_eq!(columns[0].col_name.as_str(), "col1");
            assert_eq!(columns[1].col_name.as_str(), "col2");
        } else {
            panic!("Expected CreateView");
        }
    }

    #[test]
    fn test_function_within_group_order_is_translated() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x) FROM test";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        let ast::Stmt::Select(select) = translated else {
            panic!("expected select statement");
        };
        let ast::OneSelect::Select { columns, .. } = &select.body.select else {
            panic!("expected select body");
        };
        let ast::ResultColumn::Expr(expr, _) = &columns[0] else {
            panic!("expected result column");
        };
        let ast::Expr::FunctionCall {
            order_by,
            within_group,
            ..
        } = expr.as_ref()
        else {
            panic!("expected function call");
        };
        let [sorted_column] = within_group.as_slice() else {
            panic!("expected single sorted column");
        };
        let ast::Expr::Id(name) = sorted_column.expr.as_ref() else {
            panic!("expected id");
        };

        assert_eq!(name.as_str(), "x");
        assert!(order_by.is_empty());
    }

    #[test]
    fn test_function_argument_order_is_translated() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT array_agg(x ORDER BY y) FROM test";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        let ast::Stmt::Select(select) = translated else {
            panic!("expected select statement");
        };
        let ast::OneSelect::Select { columns, .. } = &select.body.select else {
            panic!("expected select body");
        };
        let ast::ResultColumn::Expr(expr, _) = &columns[0] else {
            panic!("expected result column");
        };
        let ast::Expr::FunctionCall {
            order_by,
            within_group,
            ..
        } = expr.as_ref()
        else {
            panic!("expected function call");
        };
        let [sorted_column] = order_by.as_slice() else {
            panic!("expected single sorted column");
        };
        let ast::Expr::Id(name) = sorted_column.expr.as_ref() else {
            panic!("expected id");
        };

        assert_eq!(name.as_str(), "y");
        assert!(within_group.is_empty());
    }

    #[test]
    fn test_function_passthrough_string_agg() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT string_agg(name, ', ') FROM users";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        if let ast::Stmt::Select(select) = translated {
            if let ast::OneSelect::Select { columns, .. } = &select.body.select {
                if let ast::ResultColumn::Expr(expr, _) = &columns[0] {
                    if let ast::Expr::FunctionCall { name, .. } = &**expr {
                        assert_eq!(
                            name.as_str(),
                            "string_agg",
                            "string_agg should pass through (resolved in core/function.rs)"
                        );
                    } else {
                        panic!("Expected FunctionCall");
                    }
                }
            }
        }
    }

    #[test]
    fn test_function_passthrough_concat() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT concat(a, b, c) FROM t";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        if let ast::Stmt::Select(select) = translated {
            if let ast::OneSelect::Select { columns, .. } = &select.body.select {
                if let ast::ResultColumn::Expr(expr, _) = &columns[0] {
                    if let ast::Expr::FunctionCall { name, args, .. } = &**expr {
                        assert_eq!(
                            name.as_str(),
                            "concat",
                            "concat should pass through (resolved in core/function.rs)"
                        );
                        assert_eq!(args.len(), 3);
                    } else {
                        panic!("Expected FunctionCall, got: {expr:?}");
                    }
                }
            }
        }
    }

    #[test]
    fn test_function_passthrough_now() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT now()";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        if let ast::Stmt::Select(select) = translated {
            if let ast::OneSelect::Select { columns, .. } = &select.body.select {
                if let ast::ResultColumn::Expr(expr, _) = &columns[0] {
                    if let ast::Expr::FunctionCall { name, .. } = &**expr {
                        assert_eq!(
                            name.as_str(),
                            "now",
                            "now() should pass through as registered scalar"
                        );
                    } else {
                        panic!("Expected FunctionCall");
                    }
                }
            }
        }
    }

    #[test]
    fn test_function_passthrough_char_length() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT char_length(name) FROM t";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        if let ast::Stmt::Select(select) = translated {
            if let ast::OneSelect::Select { columns, .. } = &select.body.select {
                if let ast::ResultColumn::Expr(expr, _) = &columns[0] {
                    if let ast::Expr::FunctionCall { name, .. } = &**expr {
                        assert_eq!(
                            name.as_str(),
                            "char_length",
                            "char_length should pass through (resolved in core/function.rs)"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn test_function_passthrough_gen_random_uuid() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT gen_random_uuid()";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        if let ast::Stmt::Select(select) = translated {
            if let ast::OneSelect::Select { columns, .. } = &select.body.select {
                if let ast::ResultColumn::Expr(expr, _) = &columns[0] {
                    if let ast::Expr::FunctionCall { name, .. } = &**expr {
                        // gen_random_uuid is registered as a scalar function,
                        // not remapped at the AST level.
                        assert_eq!(name.as_str(), "gen_random_uuid");
                    }
                }
            }
        }
    }

    #[test]
    fn test_alter_table_alter_column_type() {
        let translator = PostgreSQLTranslator::new();
        let sql = "ALTER TABLE users ALTER COLUMN age TYPE bigint";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        if let ast::Stmt::AlterTable(alter) = translated {
            assert_eq!(alter.name.name.as_str(), "users");
            if let ast::AlterTableBody::AlterColumn { old, .. } = &alter.body {
                assert_eq!(old.as_str(), "age");
            } else {
                panic!("Expected AlterColumn, got: {:?}", alter.body);
            }
        } else {
            panic!("Expected AlterTable");
        }
    }

    #[test]
    fn test_alter_table_set_not_null_unsupported() {
        let translator = PostgreSQLTranslator::new();
        let sql = "ALTER TABLE users ALTER COLUMN name SET NOT NULL";
        let parsed = crate::parse(sql).unwrap();
        let err = translator.translate(&parsed).unwrap_err();
        assert!(
            err.to_string().contains("not supported"),
            "expected unsupported error, got: {err}"
        );
    }

    #[test]
    fn test_alter_table_set_default_unsupported() {
        let translator = PostgreSQLTranslator::new();
        let sql = "ALTER TABLE users ALTER COLUMN created_at SET DEFAULT now()";
        let parsed = crate::parse(sql).unwrap();
        let err = translator.translate(&parsed).unwrap_err();
        assert!(
            err.to_string().contains("not supported"),
            "expected unsupported error, got: {err}"
        );
    }

    #[test]
    fn test_array_contains_operator() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT id FROM posts WHERE tags @> '{\"ORM\"}'";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();
        if let ast::Stmt::Select(select) = translated {
            if let ast::OneSelect::Select { where_clause, .. } = &select.body.select {
                let wc = where_clause.as_ref().expect("Expected WHERE clause");
                if let ast::Expr::FunctionCall { name, args, .. } = &**wc {
                    assert_eq!(name.as_str(), "array_contains_all");
                    assert_eq!(args.len(), 2);
                } else {
                    panic!("Expected FunctionCall for @>, got: {wc:?}");
                }
            } else {
                panic!("Expected Select variant");
            }
        } else {
            panic!("Expected Select statement");
        }
    }

    #[test]
    fn test_array_contained_operator() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT id FROM posts WHERE tags <@ '{\"ORM\"}'";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();
        if let ast::Stmt::Select(select) = translated {
            if let ast::OneSelect::Select { where_clause, .. } = &select.body.select {
                let wc = where_clause.as_ref().expect("Expected WHERE clause");
                if let ast::Expr::FunctionCall { name, .. } = &**wc {
                    assert_eq!(name.as_str(), "array_contains_all");
                } else {
                    panic!("Expected FunctionCall for <@, got: {wc:?}");
                }
            } else {
                panic!("Expected Select variant");
            }
        } else {
            panic!("Expected Select statement");
        }
    }

    /// Extract the WHERE clause of a translated single SELECT.
    fn where_clause_of(sql: &str) -> ast::Expr {
        let translator = PostgreSQLTranslator::new();
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();
        let ast::Stmt::Select(select) = translated else {
            panic!("Expected Select statement");
        };
        let ast::OneSelect::Select { where_clause, .. } = &select.body.select else {
            panic!("Expected Select variant");
        };
        *where_clause
            .as_ref()
            .expect("Expected WHERE clause")
            .clone()
    }

    #[test]
    fn test_eq_any_array_literal_becomes_in_list() {
        let wc = where_clause_of("SELECT id FROM t WHERE id = ANY(ARRAY[1, 2, 3])");
        let ast::Expr::InList { not, rhs, .. } = wc else {
            panic!("Expected InList for = ANY(array literal), got: {wc:?}");
        };
        assert!(!not);
        assert_eq!(rhs.len(), 3);
    }

    #[test]
    fn test_eq_any_param_becomes_array_contains() {
        let wc = where_clause_of("SELECT id FROM t WHERE id = ANY($1)");
        let ast::Expr::FunctionCall { name, args, .. } = wc else {
            panic!("Expected FunctionCall for = ANY(param), got: {wc:?}");
        };
        assert_eq!(name.as_str(), "array_contains");
        assert_eq!(args.len(), 2);
        assert!(
            matches!(&*args[0], ast::Expr::Variable(_)),
            "array argument must come first, got: {:?}",
            args[0]
        );
    }

    #[test]
    fn test_ne_all_array_literal_becomes_not_in_list() {
        let wc = where_clause_of("SELECT id FROM t WHERE id <> ALL(ARRAY[1, 2])");
        let ast::Expr::InList { not, rhs, .. } = wc else {
            panic!("Expected InList for != ALL(array literal), got: {wc:?}");
        };
        assert!(not);
        assert_eq!(rhs.len(), 2);
    }

    #[test]
    fn test_ne_all_param_becomes_not_array_contains() {
        let wc = where_clause_of("SELECT id FROM t WHERE id != ALL($1)");
        let ast::Expr::Unary(ast::UnaryOperator::Not, inner) = wc else {
            panic!("Expected NOT(...) for != ALL(param), got: {wc:?}");
        };
        let ast::Expr::FunctionCall { name, .. } = &*inner else {
            panic!("Expected FunctionCall under NOT, got: {inner:?}");
        };
        assert_eq!(name.as_str(), "array_contains");
    }

    #[test]
    fn test_unsupported_any_all_operators_error() {
        let translator = PostgreSQLTranslator::new();
        for sql in [
            "SELECT 1 WHERE 2 > ANY(ARRAY[1, 5])",
            "SELECT 1 WHERE 2 <> ANY(ARRAY[1, 5])",
            "SELECT 1 WHERE 2 = ALL(ARRAY[2, 2])",
        ] {
            let parsed = crate::parse(sql).unwrap();
            let err = translator.translate(&parsed).unwrap_err();
            assert!(
                err.to_string().contains("not supported"),
                "expected loud error for {sql}, got: {err}"
            );
        }
    }

    #[test]
    fn test_simple_explain_translates_to_query_plan() {
        let translator = PostgreSQLTranslator::new();
        let parsed = crate::parse("EXPLAIN SELECT 1").unwrap();
        let translated = translator.translate_with_prereqs(&parsed).unwrap();
        assert!(translated.prereqs.is_empty());
        assert!(matches!(translated.cmd, ast::Cmd::ExplainQueryPlan { .. }));
    }

    #[test]
    fn test_explain_options_error() {
        let translator = PostgreSQLTranslator::new();
        for sql in [
            "EXPLAIN ANALYZE SELECT 1",
            "EXPLAIN (VERBOSE) SELECT 1",
            "EXPLAIN (COSTS OFF) SELECT 1",
        ] {
            let parsed = crate::parse(sql).unwrap();
            let err = translator.translate_with_prereqs(&parsed).unwrap_err();
            assert!(
                err.to_string().contains("EXPLAIN options"),
                "expected EXPLAIN option error for {sql}, got: {err}"
            );
        }
    }

    #[test]
    fn test_explain_missing_statement_error() {
        let translator = PostgreSQLTranslator::new();
        for query in [
            None,
            Some(Box::new(pg_query::protobuf::Node { node: None })),
        ] {
            let explain = pg_query::protobuf::ExplainStmt {
                query,
                options: vec![],
            };
            let err = translator.translate_explain(&explain).unwrap_err();
            assert!(
                err.to_string().contains("EXPLAIN missing statement"),
                "expected missing statement error, got: {err}"
            );
        }
    }

    #[test]
    fn test_array_overlaps_operator() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT id FROM posts WHERE tags && '{\"ORM\"}'";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();
        if let ast::Stmt::Select(select) = translated {
            if let ast::OneSelect::Select { where_clause, .. } = &select.body.select {
                let wc = where_clause.as_ref().expect("Expected WHERE clause");
                if let ast::Expr::FunctionCall { name, .. } = &**wc {
                    assert_eq!(name.as_str(), "array_overlap");
                } else {
                    panic!("Expected FunctionCall for &&, got: {wc:?}");
                }
            } else {
                panic!("Expected Select variant");
            }
        } else {
            panic!("Expected Select statement");
        }
    }

    #[test]
    fn test_array_constructor() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT ARRAY['a', 'b', 'c']";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();
        if let ast::Stmt::Select(select) = translated {
            if let ast::OneSelect::Select { columns, .. } = &select.body.select {
                let col = &columns[0];
                if let ast::ResultColumn::Expr(expr, _) = col {
                    if let ast::Expr::FunctionCall { name, args, .. } = &**expr {
                        assert_eq!(name.as_str(), "array");
                        assert_eq!(args.len(), 3);
                    } else {
                        panic!("Expected FunctionCall for ARRAY[...], got: {expr:?}");
                    }
                } else {
                    panic!("Expected Expr column");
                }
            } else {
                panic!("Expected Select variant");
            }
        } else {
            panic!("Expected Select statement");
        }
    }

    #[test]
    fn test_array_subscript() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT tags[1] FROM t";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();
        if let ast::Stmt::Select(select) = translated {
            if let ast::OneSelect::Select { columns, .. } = &select.body.select {
                let col = &columns[0];
                if let ast::ResultColumn::Expr(expr, _) = col {
                    if let ast::Expr::FunctionCall { name, args, .. } = &**expr {
                        assert_eq!(name.as_str(), "array_element");
                        assert_eq!(args.len(), 2);
                    } else {
                        panic!("Expected FunctionCall for tags[1], got: {expr:?}");
                    }
                } else {
                    panic!("Expected Expr column");
                }
            } else {
                panic!("Expected Select variant");
            }
        } else {
            panic!("Expected Select statement");
        }
    }

    #[test]
    fn test_array_slice() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT tags[1:3] FROM t";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();
        if let ast::Stmt::Select(select) = translated {
            if let ast::OneSelect::Select { columns, .. } = &select.body.select {
                let col = &columns[0];
                if let ast::ResultColumn::Expr(expr, _) = col {
                    if let ast::Expr::FunctionCall { name, args, .. } = &**expr {
                        assert_eq!(name.as_str(), "array_slice");
                        assert_eq!(args.len(), 3);
                    } else {
                        panic!("Expected FunctionCall for tags[1:3], got: {expr:?}");
                    }
                } else {
                    panic!("Expected Expr column");
                }
            } else {
                panic!("Expected Select variant");
            }
        } else {
            panic!("Expected Select statement");
        }
    }

    #[test]
    fn test_create_materialized_view() {
        let translator = PostgreSQLTranslator::new();
        let sql = "CREATE MATERIALIZED VIEW totals AS SELECT category, SUM(price) FROM products GROUP BY category";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        match translated {
            ast::Stmt::CreateMaterializedView {
                if_not_exists,
                view_name,
                ..
            } => {
                assert!(!if_not_exists);
                assert_eq!(view_name.name.as_str(), "totals");
            }
            other => panic!("Expected CreateMaterializedView, got: {other:?}"),
        }
    }

    #[test]
    fn test_create_materialized_view_if_not_exists() {
        let translator = PostgreSQLTranslator::new();
        let sql = "CREATE MATERIALIZED VIEW IF NOT EXISTS mv AS SELECT 1";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        match translated {
            ast::Stmt::CreateMaterializedView { if_not_exists, .. } => {
                assert!(if_not_exists);
            }
            other => panic!("Expected CreateMaterializedView, got: {other:?}"),
        }
    }

    #[test]
    fn test_drop_materialized_view() {
        let translator = PostgreSQLTranslator::new();
        let sql = "DROP MATERIALIZED VIEW my_view";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        match translated {
            ast::Stmt::DropView {
                if_exists,
                view_name,
            } => {
                assert!(!if_exists);
                assert_eq!(view_name.name.as_str(), "my_view");
            }
            other => panic!("Expected DropView, got: {other:?}"),
        }
    }

    #[test]
    fn test_refresh_materialized_view() {
        let sql = "REFRESH MATERIALIZED VIEW my_view";
        let parsed = crate::parse(sql).unwrap();
        // REFRESH is intercepted in pg_dispatch, but the translator should not error
        // if it encounters it — it falls to the catch-all arm.
        // For this test, just verify parsing succeeds.
        assert!(!parsed.protobuf.nodes().is_empty());
    }

    // -----------------------------------------------------------------------
    // Named windows
    // -----------------------------------------------------------------------

    #[test]
    fn test_named_window_basic() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT SUM(x) OVER w FROM t WINDOW w AS (PARTITION BY y)";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        if let ast::Stmt::Select(select) = translated {
            if let ast::OneSelect::Select {
                window_clause,
                columns,
                ..
            } = &select.body.select
            {
                // WINDOW clause should have one definition
                assert_eq!(window_clause.len(), 1);
                assert_eq!(window_clause[0].name.as_str(), "w");
                assert_eq!(window_clause[0].window.partition_by.len(), 1);
                assert!(window_clause[0].window.order_by.is_empty());

                // The function should reference the named window
                if let ast::ResultColumn::Expr(expr, _) = &columns[0] {
                    if let ast::Expr::FunctionCall { filter_over, .. } = &**expr {
                        assert!(
                            matches!(filter_over.over_clause.as_deref(), Some(ast::Over::Name(n)) if n.as_str() == "w"),
                            "Expected Over::Name(\"w\"), got: {:?}",
                            filter_over.over_clause
                        );
                    } else {
                        panic!("Expected FunctionCall");
                    }
                }
            } else {
                panic!("Expected Select");
            }
        } else {
            panic!("Expected Select statement");
        }
    }

    #[test]
    fn test_order_by_nulls_ordering() {
        let translator = PostgreSQLTranslator::new();

        let cases = [
            ("SELECT * FROM t ORDER BY a", Some(ast::NullsOrder::Last)),
            (
                "SELECT * FROM t ORDER BY a ASC",
                Some(ast::NullsOrder::Last),
            ),
            (
                "SELECT * FROM t ORDER BY a DESC",
                Some(ast::NullsOrder::First),
            ),
            (
                "SELECT * FROM t ORDER BY a NULLS FIRST",
                Some(ast::NullsOrder::First),
            ),
            (
                "SELECT * FROM t ORDER BY a NULLS LAST",
                Some(ast::NullsOrder::Last),
            ),
            (
                "SELECT * FROM t ORDER BY a DESC NULLS FIRST",
                Some(ast::NullsOrder::First),
            ),
            (
                "SELECT * FROM t ORDER BY a ASC NULLS LAST",
                Some(ast::NullsOrder::Last),
            ),
        ];

        for (sql, expected_nulls) in cases {
            let parsed = crate::parse(sql).unwrap();
            let translated = translator.translate(&parsed).unwrap();
            let ast::Stmt::Select(select) = translated else {
                panic!("expected Select statement for {sql}");
            };
            assert_eq!(select.order_by.len(), 1, "for {sql}");
            assert_eq!(select.order_by[0].nulls, expected_nulls, "for {sql}");
        }
    }

    #[test]
    fn test_compound_select_order_by_nulls_ordering() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT a FROM t1 UNION SELECT a FROM t2 ORDER BY a NULLS LAST";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        let ast::Stmt::Select(select) = translated else {
            panic!("expected Select statement");
        };
        assert_eq!(select.body.compounds.len(), 1, "expected one UNION arm");
        assert_eq!(select.order_by.len(), 1);
        assert_eq!(select.order_by[0].nulls, Some(ast::NullsOrder::Last));
    }

    #[test]
    fn test_values_order_by_nulls_ordering() {
        let translator = PostgreSQLTranslator::new();
        let sql = "VALUES (1), (NULL), (2) ORDER BY 1 NULLS LAST";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        let ast::Stmt::Select(select) = translated else {
            panic!("expected Select statement");
        };
        assert!(
            matches!(select.body.select, ast::OneSelect::Values(_)),
            "expected a VALUES body, got {:?}",
            select.body.select
        );
        assert_eq!(select.order_by.len(), 1);
        assert_eq!(select.order_by[0].nulls, Some(ast::NullsOrder::Last));
    }

    #[test]
    fn test_window_order_by_nulls_ordering() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT ROW_NUMBER() OVER (ORDER BY salary NULLS LAST) FROM t";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        let ast::Stmt::Select(select) = translated else {
            panic!("expected Select statement");
        };
        let ast::OneSelect::Select { columns, .. } = &select.body.select else {
            panic!("expected Select body");
        };
        let ast::ResultColumn::Expr(expr, _) = &columns[0] else {
            panic!("expected expression result column");
        };
        let ast::Expr::FunctionCall { filter_over, .. } = expr.as_ref() else {
            panic!("expected function call expression, got {expr:?}");
        };
        let Some(ast::Over::Window(window)) = filter_over.over_clause.as_deref() else {
            panic!(
                "expected inline OVER window, got {:?}",
                filter_over.over_clause
            );
        };
        assert_eq!(window.order_by.len(), 1);
        assert_eq!(window.order_by[0].nulls, Some(ast::NullsOrder::Last));
    }

    #[test]
    fn test_named_window_with_order_by() {
        let translator = PostgreSQLTranslator::new();
        let sql =
            "SELECT ROW_NUMBER() OVER w FROM t WINDOW w AS (PARTITION BY dept ORDER BY salary)";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        if let ast::Stmt::Select(select) = translated {
            if let ast::OneSelect::Select { window_clause, .. } = &select.body.select {
                assert_eq!(window_clause.len(), 1);
                assert_eq!(window_clause[0].name.as_str(), "w");
                assert_eq!(window_clause[0].window.partition_by.len(), 1);
                assert_eq!(window_clause[0].window.order_by.len(), 1);
            }
        }
    }

    #[test]
    fn test_multiple_named_windows() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT SUM(x) OVER w1, AVG(x) OVER w2 FROM t \
                   WINDOW w1 AS (PARTITION BY a), w2 AS (ORDER BY b)";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        if let ast::Stmt::Select(select) = translated {
            if let ast::OneSelect::Select { window_clause, .. } = &select.body.select {
                assert_eq!(window_clause.len(), 2);
                assert_eq!(window_clause[0].name.as_str(), "w1");
                assert_eq!(window_clause[0].window.partition_by.len(), 1);
                assert!(window_clause[0].window.order_by.is_empty());
                assert_eq!(window_clause[1].name.as_str(), "w2");
                assert!(window_clause[1].window.partition_by.is_empty());
                assert_eq!(window_clause[1].window.order_by.len(), 1);
            }
        }
    }

    #[test]
    fn test_named_window_with_frame() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT SUM(x) OVER w FROM t \
                   WINDOW w AS (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        if let ast::Stmt::Select(select) = translated {
            if let ast::OneSelect::Select { window_clause, .. } = &select.body.select {
                assert_eq!(window_clause.len(), 1);
                assert!(window_clause[0].window.frame_clause.is_some());
                let frame = window_clause[0].window.frame_clause.as_ref().unwrap();
                assert_eq!(frame.mode, ast::FrameMode::Rows);
            }
        }
    }

    #[test]
    fn test_named_window_inheritance() {
        let translator = PostgreSQLTranslator::new();
        // w2 inherits from w1 and adds ORDER BY
        let sql = "SELECT SUM(x) OVER w2 FROM t \
                   WINDOW w1 AS (PARTITION BY dept), w2 AS (w1 ORDER BY salary)";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        if let ast::Stmt::Select(select) = translated {
            if let ast::OneSelect::Select { window_clause, .. } = &select.body.select {
                assert_eq!(window_clause.len(), 2);
                // w1 has no base
                assert!(window_clause[0].window.base.is_none());
                // w2 inherits from w1
                assert_eq!(
                    window_clause[1].window.base.as_ref().map(|n| n.as_str()),
                    Some("w1")
                );
                assert_eq!(window_clause[1].window.order_by.len(), 1);
            }
        }
    }

    #[test]
    fn test_named_window_empty_over() {
        let translator = PostgreSQLTranslator::new();
        let sql = "SELECT COUNT(*) OVER w FROM t WINDOW w AS ()";
        let parsed = crate::parse(sql).unwrap();
        let translated = translator.translate(&parsed).unwrap();

        if let ast::Stmt::Select(select) = translated {
            if let ast::OneSelect::Select { window_clause, .. } = &select.body.select {
                assert_eq!(window_clause.len(), 1);
                assert_eq!(window_clause[0].name.as_str(), "w");
                assert!(window_clause[0].window.partition_by.is_empty());
                assert!(window_clause[0].window.order_by.is_empty());
                assert!(window_clause[0].window.frame_clause.is_none());
            }
        }
    }

    // -----------------------------------------------------------------------
    // COPY statement translation
    // -----------------------------------------------------------------------

    #[test]
    fn test_copy_translation_is_rejected() {
        let translator = PostgreSQLTranslator::new();
        let sql = "COPY users FROM '/path/to/file.tsv'";
        let parsed = crate::parse(sql).unwrap();
        let err = translator.translate(&parsed).unwrap_err();
        assert!(err
            .to_string()
            .contains("COPY is handled at the postgres frontend layer"));
    }

    #[test]
    fn test_try_extract_copy_from() {
        let parsed = crate::parse("COPY users FROM '/tmp/data.tsv'").unwrap();
        let copy = try_extract_copy_from(&parsed).unwrap();
        assert_eq!(copy.table_name, "users");
        assert!(copy.schema_name.is_none());
        assert!(copy.columns.is_none());
        assert_eq!(copy.filename, "/tmp/data.tsv");
        assert!(!copy.header);
        assert!(copy.delimiter.is_none());
        assert!(copy.null_string.is_none());
    }

    #[test]
    fn test_try_extract_copy_from_not_to() {
        let parsed = crate::parse("COPY users TO '/tmp/out.tsv'").unwrap();
        assert!(try_extract_copy_from(&parsed).is_none());
    }

    #[test]
    fn test_try_extract_copy_from_not_stdin() {
        let parsed = crate::parse("COPY users FROM STDIN").unwrap();
        assert!(try_extract_copy_from(&parsed).is_none());
    }

    #[test]
    fn test_try_extract_copy_from_with_columns() {
        let parsed = crate::parse("COPY users (id, name) FROM '/tmp/data.tsv'").unwrap();
        let copy = try_extract_copy_from(&parsed).unwrap();
        let cols = copy.columns.unwrap();
        assert_eq!(cols, vec!["id", "name"]);
    }

    #[test]
    fn test_create_sequence_cycle() {
        let translator = PostgreSQLTranslator::new();
        let sql = "CREATE SEQUENCE cyc_seq MINVALUE 1 MAXVALUE 3 CYCLE";
        let parse_result = crate::parse(sql).unwrap();
        let stmt = translator.translate(&parse_result).unwrap();
        match stmt {
            ast::Stmt::CreateSequence { cycle, .. } => {
                assert!(cycle, "CYCLE should be true");
            }
            other => panic!("Expected CreateSequence, got {other:?}"),
        }
    }

    #[test]
    fn test_recursive_cte_cycle_clause_rejected() {
        let translator = PostgreSQLTranslator::new();
        let sql = "WITH RECURSIVE walk(dst) AS (
            SELECT 1 UNION ALL SELECT w.dst + 1 FROM walk w WHERE w.dst < 3
        ) CYCLE dst SET is_cycle USING path
        SELECT * FROM walk";
        let parse_result = crate::parse(sql).unwrap();
        let err = translator.translate(&parse_result).unwrap_err();
        assert!(
            err.to_string().contains("CYCLE clause"),
            "expected CYCLE clause rejection, got: {err}"
        );
    }

    #[test]
    fn test_recursive_cte_search_clause_rejected() {
        let translator = PostgreSQLTranslator::new();
        let sql = "WITH RECURSIVE walk(dst) AS (
            SELECT 1 UNION ALL SELECT w.dst + 1 FROM walk w WHERE w.dst < 3
        ) SEARCH DEPTH FIRST BY dst SET ordercol
        SELECT * FROM walk";
        let parse_result = crate::parse(sql).unwrap();
        let err = translator.translate(&parse_result).unwrap_err();
        assert!(
            err.to_string().contains("SEARCH clause"),
            "expected SEARCH clause rejection, got: {err}"
        );
    }

    /// Asserts that `sql` is rejected with a "not supported" error that
    /// mentions `needle`, instead of being translated with part of its
    /// meaning silently dropped.
    fn assert_not_supported(sql: &str, needle: &str) {
        let parsed = crate::parse(sql).unwrap();
        let err = PostgreSQLTranslator::new()
            .translate_with_prereqs(&parsed)
            .expect_err(sql)
            .to_string();
        assert!(
            err.contains("not supported") && err.contains(needle),
            "{sql}: expected a not-supported error mentioning {needle:?}, got: {err}"
        );
    }

    /// Asserts that `sql` translates without error.
    fn assert_translates(sql: &str) {
        let parsed = crate::parse(sql).unwrap();
        if let Err(err) = PostgreSQLTranslator::new().translate_with_prereqs(&parsed) {
            panic!("{sql}: {err}");
        }
    }

    #[test]
    fn fail_closed_deferrable_constraints() {
        for sql in [
            "CREATE TABLE c (pid int REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
            "CREATE TABLE c (pid int REFERENCES p(id) DEFERRABLE)",
            "CREATE TABLE c (pid int, FOREIGN KEY (pid) REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)",
            "CREATE TABLE c (id int, UNIQUE (id) DEFERRABLE)",
            "ALTER TABLE c ADD COLUMN pid int UNIQUE DEFERRABLE",
        ] {
            assert_not_supported(sql, "DEFERRABLE");
        }
        // Restating the defaults is fine.
        assert_translates(
            "CREATE TABLE c (pid int REFERENCES p(id) NOT DEFERRABLE INITIALLY IMMEDIATE)",
        );
    }

    #[test]
    fn fail_closed_not_valid_constraints() {
        assert_not_supported(
            "CREATE TABLE c (pid int, FOREIGN KEY (pid) REFERENCES p(id) NOT VALID)",
            "NOT VALID",
        );
        assert_not_supported(
            "CREATE TABLE c (a int, CHECK (a > 0) NOT VALID)",
            "NOT VALID",
        );
    }

    #[test]
    fn fail_closed_no_inherit_constraints() {
        assert_not_supported(
            "CREATE TABLE c (a int CHECK (a > 0) NO INHERIT)",
            "NO INHERIT",
        );
        assert_not_supported(
            "CREATE TABLE c (a int, CHECK (a > 0) NO INHERIT)",
            "NO INHERIT",
        );
    }

    #[test]
    fn fail_closed_fk_match_type() {
        assert_not_supported(
            "CREATE TABLE c (a int, b int, FOREIGN KEY (a, b) REFERENCES p(x, y) MATCH FULL)",
            "MATCH FULL",
        );
        assert_not_supported(
            "CREATE TABLE c (a int REFERENCES p(x) MATCH FULL)",
            "MATCH FULL",
        );
        assert_translates("CREATE TABLE c (a int REFERENCES p(x) MATCH SIMPLE)");
    }

    #[test]
    fn fail_closed_fk_delete_set_columns() {
        assert_not_supported(
            "CREATE TABLE c (t int, a int, FOREIGN KEY (t, a) REFERENCES p(t, a) ON DELETE SET NULL (a))",
            "column list",
        );
        assert_translates(
            "CREATE TABLE c (t int, a int, FOREIGN KEY (t, a) REFERENCES p(t, a) ON DELETE SET NULL)",
        );
    }

    #[test]
    fn fail_closed_nulls_not_distinct_constraint() {
        assert_not_supported(
            "CREATE TABLE c (a int UNIQUE NULLS NOT DISTINCT)",
            "NULLS NOT DISTINCT",
        );
        assert_not_supported(
            "CREATE TABLE c (a int, UNIQUE NULLS NOT DISTINCT (a))",
            "NULLS NOT DISTINCT",
        );
    }

    #[test]
    fn fail_closed_constraint_index_clauses() {
        assert_not_supported(
            "CREATE TABLE c (a int, b int, UNIQUE (a) INCLUDE (b))",
            "INCLUDE",
        );
        assert_not_supported(
            "CREATE TABLE c (a int, PRIMARY KEY (a) WITH (fillfactor = 70))",
            "storage parameters",
        );
        assert_not_supported(
            "CREATE TABLE c (a int, UNIQUE (a) USING INDEX TABLESPACE ts)",
            "TABLESPACE",
        );
    }

    #[test]
    fn fail_closed_fk_other_schema() {
        assert_not_supported(
            "CREATE TABLE c (a int REFERENCES other.p(id))",
            "another schema",
        );
        assert_not_supported(
            "CREATE TABLE s.c (a int, FOREIGN KEY (a) REFERENCES p2.p(id))",
            "another schema",
        );
        assert_not_supported(
            "ALTER TABLE c ADD COLUMN a int REFERENCES other.p(id)",
            "another schema",
        );
        // pg_dump qualifies everything with public; same-schema references are fine.
        assert_translates("CREATE TABLE public.c (a int REFERENCES public.p(id))");
        assert_translates("CREATE TABLE c (a int REFERENCES public.p(id))");
        assert_translates("CREATE TABLE s.c (a int REFERENCES s.p(id))");
    }

    #[test]
    fn fail_closed_create_table_column_constraint_kinds() {
        assert_not_supported(
            "CREATE TABLE t (id int GENERATED ALWAYS AS IDENTITY)",
            "AS IDENTITY",
        );
        assert_not_supported(
            "CREATE TABLE t (id int GENERATED BY DEFAULT AS IDENTITY)",
            "AS IDENTITY",
        );
        assert_not_supported(
            "CREATE TABLE t (a int, b int GENERATED ALWAYS AS (a * 2) STORED)",
            "GENERATED ALWAYS AS",
        );
        assert_translates("CREATE TABLE t (a int NULL)");
    }

    #[test]
    fn fail_closed_alter_table_column_constraint_kinds() {
        assert_not_supported("ALTER TABLE t ADD COLUMN a int CHECK (a > 0)", "CHECK");
        assert_not_supported(
            "ALTER TABLE t ADD COLUMN a int REFERENCES p(id)",
            "REFERENCES",
        );
        assert_not_supported(
            "ALTER TABLE t ADD COLUMN a int GENERATED ALWAYS AS IDENTITY",
            "AS IDENTITY",
        );
        assert_translates("ALTER TABLE t ADD COLUMN a int NOT NULL DEFAULT 0");
    }

    #[test]
    fn fail_closed_create_table_elements() {
        assert_not_supported(
            "CREATE TABLE t (a int, EXCLUDE USING gist (a WITH =))",
            "EXCLUDE",
        );
        assert_not_supported("CREATE TABLE t (LIKE other)", "LIKE");
        assert_not_supported("CREATE TABLE t (a int, LIKE other INCLUDING ALL)", "LIKE");
    }

    #[test]
    fn fail_closed_temporary_table() {
        assert_not_supported("CREATE TEMP TABLE t (a int)", "TEMPORARY");
        assert_not_supported("CREATE TEMPORARY TABLE t (a int)", "TEMPORARY");
        // UNLOGGED only weakens durability, so a logged table is a safe substitute.
        assert_translates("CREATE UNLOGGED TABLE t (a int)");
    }

    #[test]
    fn fail_closed_inheritance_and_partitioning() {
        assert_not_supported("CREATE TABLE c (b int) INHERITS (p)", "INHERITS");
        assert_not_supported(
            "CREATE TABLE t (a int) PARTITION BY RANGE (a)",
            "partitioning",
        );
        assert_not_supported(
            "CREATE TABLE t1 PARTITION OF t FOR VALUES FROM (1) TO (10)",
            "partitioning",
        );
        assert_not_supported("CREATE TABLE t OF mytype", "OF type");
    }

    #[test]
    fn fail_closed_create_table_storage_clauses() {
        assert_not_supported(
            "CREATE TABLE t (a int) WITH (fillfactor = 70)",
            "storage parameters",
        );
        assert_not_supported("CREATE TABLE t (a int) TABLESPACE ts", "TABLESPACE");
        assert_not_supported("CREATE TABLE t (a int) USING columnar", "access method");
        assert_translates("CREATE TABLE t (a int) USING heap");
    }

    #[test]
    fn fail_closed_on_commit() {
        assert_not_supported("CREATE TEMP TABLE t (a int) ON COMMIT DROP", "TEMPORARY");
    }

    #[test]
    fn fail_closed_column_collate() {
        assert_not_supported("CREATE TABLE t (a text COLLATE \"C\")", "COLLATE");
        assert_not_supported("ALTER TABLE t ADD COLUMN a text COLLATE \"C\"", "COLLATE");
        assert_not_supported(
            "ALTER TABLE t ALTER COLUMN a TYPE text COLLATE \"C\"",
            "COLLATE",
        );
    }

    #[test]
    fn fail_closed_multiple_references() {
        assert_not_supported(
            "CREATE TABLE t (a int REFERENCES p(id) REFERENCES q(id))",
            "multiple REFERENCES",
        );
    }

    #[test]
    fn fail_closed_column_type_modifiers() {
        assert_not_supported("CREATE TABLE t (a timestamp(3))", "type modifiers");
        assert_not_supported("CREATE TABLE t (a time(0))", "type modifiers");
        assert_not_supported("CREATE TABLE t (a bit(8))", "type modifiers");
        assert_not_supported(
            "ALTER TABLE t ADD COLUMN a timestamptz(0)",
            "type modifiers",
        );
        assert_not_supported(
            "CREATE TABLE t (a mytype('x'))",
            "non-integer type modifier",
        );
        // char(n) arrives as bpchar, which maps to unbounded TEXT.
        assert_not_supported("CREATE TABLE t (a char(2))", "type modifiers on bpchar");
        assert_translates("CREATE TABLE t (a varchar(3), b numeric(10, 2), c text, d int[])");
    }

    #[test]
    fn fail_closed_add_column_serial() {
        assert_not_supported(
            "ALTER TABLE t ADD COLUMN id serial",
            "serial in ALTER TABLE",
        );
        assert_not_supported(
            "ALTER TABLE t ALTER COLUMN id TYPE bigserial",
            "bigserial in ALTER TABLE",
        );
    }

    #[test]
    fn fail_closed_alter_table_multiple_subcommands() {
        assert_not_supported(
            "ALTER TABLE t ADD COLUMN a int, ADD COLUMN b int",
            "multiple subcommands",
        );
    }

    #[test]
    fn fail_closed_alter_table_if_exists_and_cascade() {
        assert_not_supported("ALTER TABLE IF EXISTS t ADD COLUMN a int", "IF EXISTS");
        assert_not_supported(
            "ALTER TABLE t ADD COLUMN IF NOT EXISTS a int",
            "IF [NOT] EXISTS",
        );
        assert_not_supported("ALTER TABLE t DROP COLUMN IF EXISTS a", "IF [NOT] EXISTS");
        assert_not_supported("ALTER TABLE t DROP COLUMN a CASCADE", "CASCADE");
        assert_translates("ALTER TABLE t DROP COLUMN a RESTRICT");
    }

    #[test]
    fn fail_closed_alter_non_table() {
        assert_not_supported("ALTER VIEW v ALTER COLUMN a SET DEFAULT 1", "ALTER VIEW");
        assert_not_supported(
            "ALTER FOREIGN TABLE f ADD COLUMN a int",
            "ALTER FOREIGNTABLE",
        );
        assert_not_supported(
            "ALTER MATERIALIZED VIEW m ALTER COLUMN a SET STATISTICS 1",
            "ALTER MATVIEW",
        );
    }

    #[test]
    fn fail_closed_alter_column_type_using() {
        assert_not_supported(
            "ALTER TABLE t ALTER COLUMN a TYPE int USING a::int * 10",
            "USING",
        );
        assert_translates("ALTER TABLE t ALTER COLUMN a TYPE bigint");
    }

    #[test]
    fn fail_closed_rename() {
        assert_not_supported(
            "ALTER VIEW v RENAME COLUMN a TO b",
            "ALTER VIEW ... RENAME COLUMN",
        );
        assert_not_supported("ALTER TABLE IF EXISTS t RENAME TO u", "IF EXISTS");
        assert_not_supported("ALTER TABLE IF EXISTS t RENAME COLUMN a TO b", "IF EXISTS");
        assert_translates("ALTER TABLE t RENAME COLUMN a TO b");
        assert_translates("ALTER TABLE t RENAME TO u");
    }

    #[test]
    fn fail_closed_transaction_options() {
        assert_not_supported("BEGIN READ ONLY", "READ ONLY");
        assert_not_supported(
            "START TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY",
            "READ ONLY",
        );
        assert_not_supported("COMMIT AND CHAIN", "AND CHAIN");
        assert_not_supported("ROLLBACK AND CHAIN", "AND CHAIN");
        assert_translates("BEGIN ISOLATION LEVEL READ COMMITTED");
        assert_translates("BEGIN ISOLATION LEVEL SERIALIZABLE, READ WRITE");
        assert_translates("START TRANSACTION DEFERRABLE");
        assert_translates("COMMIT AND NO CHAIN");
    }

    #[test]
    fn fail_closed_unnamed_index() {
        assert_not_supported("CREATE INDEX ON t (a)", "without an index name");
        assert_translates("CREATE INDEX t_a ON t (a)");
    }

    #[test]
    fn fail_closed_index_access_method() {
        assert_not_supported("CREATE INDEX i ON t USING gin (a)", "USING gin");
        assert_not_supported("CREATE INDEX i ON t USING hash (a)", "USING hash");
        assert_translates("CREATE INDEX i ON t USING btree (a)");
    }

    #[test]
    fn fail_closed_index_include() {
        assert_not_supported("CREATE INDEX i ON t (a) INCLUDE (b)", "INCLUDE");
    }

    #[test]
    fn fail_closed_index_nulls_not_distinct() {
        assert_not_supported(
            "CREATE UNIQUE INDEX i ON t (a) NULLS NOT DISTINCT",
            "NULLS NOT DISTINCT",
        );
    }

    #[test]
    fn fail_closed_index_storage_clauses() {
        assert_not_supported(
            "CREATE INDEX i ON t (a) WITH (fillfactor = 70)",
            "storage parameters",
        );
        assert_not_supported("CREATE INDEX i ON t (a) TABLESPACE ts", "TABLESPACE");
    }

    #[test]
    fn fail_closed_index_element_clauses() {
        assert_not_supported(
            "CREATE INDEX i ON t (a COLLATE \"C\")",
            "COLLATE in CREATE INDEX",
        );
        assert_not_supported(
            "CREATE INDEX i ON t (a text_pattern_ops)",
            "operator classes in CREATE INDEX",
        );
        assert_translates("CREATE INDEX i ON t (a DESC NULLS LAST, (lower(b)))");
    }
}
