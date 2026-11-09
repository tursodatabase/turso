//! The checks of the body of a LANGUAGE sql function, as PostgreSQL's
//! fmgr_sql_validator() and check_sql_fn_retval() do them.
//!
//! PostgreSQL analyzes each statement of the body as a statement with
//! parameters. tursopg prepares each statement with `$n` in the place of
//! each parameter name, and gives the error of the first statement that
//! does not prepare. Then it compares the result columns of the last
//! statement with the result type of the function.

use super::expr::{self, missing_parameter};
use super::function::{invalid_function, Parameter, ResultType};
use super::types::{declared_type, TypeRef};
use super::{error, strings, Relation, RoutineCatalog};
use crate::builtins::{self, Coercion};
use crate::sqlstate::{
    SQLSTATE_FEATURE_NOT_SUPPORTED, SQLSTATE_INVALID_TEXT_REPRESENTATION,
    SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE, SQLSTATE_SYNTAX_ERROR,
};
use crate::ParseError;
use pg_query::protobuf::node::Node;
use pg_query::protobuf::{self as pb, a_const, LockClauseStrength, Token};
use pg_query::NodeRef;

/// The parts of a function definition that the checks of its body use.
pub(super) struct Signature<'a> {
    pub function_name: &'a str,
    /// All parameters: input and output.
    pub parameters: &'a [Parameter<'a>],
    pub result: &'a ResultType,
    pub is_procedure: bool,
}

impl Signature<'_> {
    fn inputs(&self) -> impl Iterator<Item = &Parameter<'_>> {
        self.parameters
            .iter()
            .filter(|parameter| parameter.is_input())
    }

    /// The number of the input parameter with the name `name`, from 1.
    fn parameter_number(&self, name: &str) -> Option<usize> {
        self.inputs()
            .position(|parameter| !parameter.name.is_empty() && parameter.name == name)
            .map(|index| index + 1)
    }

    fn parameter_type(&self, number: usize) -> Option<&'static str> {
        self.inputs().nth(number.checked_sub(1)?)?.ty.builtin_name()
    }
}

/// The last statement of a body, with the number of its result columns
/// when the statement prepared.
struct LastStatement {
    /// The statement, parsed from its own text.
    parsed: pg_query::ParseResult,
    columns: Option<usize>,
}

impl LastStatement {
    fn statement(&self) -> Option<&Node> {
        crate::first_statement(&self.parsed)
    }
}

/// Checks a body string, with check_function_bodies on.
pub(super) fn check_sql_body(
    body: &str,
    signature: &Signature,
    catalog: &dyn RoutineCatalog,
) -> Result<(), ParseError> {
    let parsed = pg_query::parse(body).map_err(syntax_error)?;
    // PostgreSQL only parses the body when a parameter is polymorphic.
    if signature
        .inputs()
        .any(|parameter| parameter.ty.is_polymorphic())
    {
        return Ok(());
    }
    let mut last = None;
    for raw in &parsed.protobuf.stmts {
        last = Some(analyze_statement(
            crate::statement_text(body, raw),
            signature,
            catalog,
        )?);
    }
    check_result(last, signature, catalog)
}

/// Analyzes a RETURN or BEGIN ATOMIC body as PostgreSQL's
/// interpret_AS_clause() does when it creates the function.
pub(super) fn analyze_sql_standard_body(
    sql_body: &pb::Node,
    signature: &Signature,
    catalog: &dyn RoutineCatalog,
) -> Result<(), ParseError> {
    for text in sql_standard_body_statements(sql_body)? {
        let analyzed = analyze_statement(&text, signature, catalog)?;
        let Some(statement) = analyzed.statement() else {
            continue;
        };
        if !is_optimizable(statement) && !matches!(statement, Node::CallStmt(_)) {
            let tag = crate::cmdtag::CommandTag::of(&analyzed.parsed).name();
            return Err(error(
                SQLSTATE_FEATURE_NOT_SUPPORTED,
                format!("{tag} is not yet supported in unquoted SQL function body"),
            ));
        }
    }
    Ok(())
}

/// Checks the result of a RETURN or BEGIN ATOMIC body, with
/// check_function_bodies on.
pub(super) fn check_sql_standard_body_result(
    sql_body: &pb::Node,
    signature: &Signature,
    catalog: &dyn RoutineCatalog,
) -> Result<(), ParseError> {
    let mut last = None;
    for text in sql_standard_body_statements(sql_body)? {
        last = Some(analyze_statement(&text, signature, catalog)?);
    }
    check_result(last, signature, catalog)
}

/// The text of each statement of a RETURN or BEGIN ATOMIC body. RETURN
/// `expr` is `SELECT expr`.
fn sql_standard_body_statements(sql_body: &pb::Node) -> Result<Vec<String>, ParseError> {
    let deparse = |node: &Node| {
        node.deparse()
            .map_err(|error| ParseError::ParseError(error.to_string()))
    };
    match sql_body.node.as_ref() {
        Some(Node::ReturnStmt(stmt)) => match stmt.returnval.as_deref() {
            Some(value) => Ok(vec![expr::select_of(&[value])?]),
            None => Ok(Vec::new()),
        },
        Some(Node::List(list)) => {
            let mut texts = Vec::new();
            for item in &list.items {
                match item.node.as_ref() {
                    Some(Node::List(statements)) => {
                        for statement in &statements.items {
                            if let Some(node) = &statement.node {
                                texts.push(deparse(node)?);
                            }
                        }
                    }
                    Some(node) => texts.push(deparse(node)?),
                    None => {}
                }
            }
            Ok(texts)
        }
        Some(node) => Ok(vec![deparse(node)?]),
        None => Ok(Vec::new()),
    }
}

/// Analyzes one statement of a body: its `$n` parameters, then the errors
/// of preparing it.
fn analyze_statement(
    text: &str,
    signature: &Signature,
    catalog: &dyn RoutineCatalog,
) -> Result<LastStatement, ParseError> {
    let parsed = pg_query::parse(text).map_err(syntax_error)?;
    let Some(statement) = crate::first_statement(&parsed) else {
        return Ok(LastStatement {
            parsed,
            columns: None,
        });
    };
    let inputs = signature.inputs().count();
    let missing = statement
        .nodes()
        .into_iter()
        .filter_map(|(node, ..)| match node {
            NodeRef::ParamRef(param) if param.number as usize > inputs || param.number < 1 => {
                Some((param.location, param.number))
            }
            _ => None,
        })
        .min();
    if let Some((_, number)) = missing {
        return Err(missing_parameter(number));
    }
    let columns = match statement {
        Node::SelectStmt(select) if select.into_clause.is_some() => None,
        Node::CallStmt(call) => {
            super::lookup::check_call(call, catalog)?;
            None
        }
        statement if is_optimizable(statement) => {
            let text = substitute_parameters(text, statement, signature);
            super::prepare(catalog, &text)?
        }
        _ => None,
    };
    Ok(LastStatement { parsed, columns })
}

/// Whether PostgreSQL analyzes the statement in a function body: a SELECT,
/// INSERT, UPDATE, DELETE or MERGE.
fn is_optimizable(statement: &Node) -> bool {
    matches!(
        statement,
        Node::SelectStmt(_)
            | Node::InsertStmt(_)
            | Node::UpdateStmt(_)
            | Node::DeleteStmt(_)
            | Node::MergeStmt(_)
    )
}

/// `text` with `$n` in the place of each reference to a parameter by its
/// name `a` or `f.a`, where `f` is the name of the function.
fn substitute_parameters(text: &str, statement: &Node, signature: &Signature) -> String {
    let mut references: Vec<(usize, usize, usize)> = statement
        .nodes()
        .into_iter()
        .filter_map(|(node, ..)| {
            let NodeRef::ColumnRef(column) = node else {
                return None;
            };
            let names = strings(&column.fields);
            if names.len() != column.fields.len() {
                return None;
            }
            let (parts, number) = match names.as_slice() {
                [name] => (1, signature.parameter_number(name)?),
                [function, name] if *function == signature.function_name => {
                    (2, signature.parameter_number(name)?)
                }
                _ => return None,
            };
            Some((usize::try_from(column.location).ok()?, parts, number))
        })
        .collect();
    if references.is_empty() {
        return text.to_string();
    }
    let Ok(scanned) = pg_query::scan(text) else {
        return text.to_string();
    };
    let tokens: Vec<(usize, usize)> = scanned
        .tokens
        .iter()
        .filter(|token| {
            token.token != Token::SqlComment as i32 && token.token != Token::CComment as i32
        })
        .map(|token| (token.start as usize, token.end as usize))
        .collect();
    references.sort_unstable();
    let mut result = text.to_string();
    for (location, parts, number) in references.into_iter().rev() {
        let Some(first) = tokens.iter().position(|(start, _)| *start == location) else {
            continue;
        };
        // A name of two parts is three tokens: `f`, `.` and `a`.
        let Some((_, end)) = tokens.get(first + 2 * (parts - 1)) else {
            continue;
        };
        result.replace_range(location..*end, &format!("${number}"));
    }
    result
}

/// Compares the result columns of the last statement with the result type,
/// as PostgreSQL's check_sql_fn_retval() does.
fn check_result(
    last: Option<LastStatement>,
    signature: &Signature,
    catalog: &dyn RoutineCatalog,
) -> Result<(), ParseError> {
    let result = signature.result;
    if result.ty.is("void") {
        return Ok(());
    }
    let mismatch = || {
        invalid_function(format!(
            "return type mismatch in function declared to return {}",
            result.ty.display_name()
        ))
    };
    let Some(last) = last else {
        return Err(mismatch());
    };
    let (targets, from) = match last.statement() {
        Some(Node::SelectStmt(select)) if select.into_clause.is_none() => {
            // The columns of a UNION have the types of all its queries.
            let targets =
                (select.op() == pb::SetOperation::SetopNone).then_some(&select.target_list);
            (targets, single_relation(&select.from_clause, catalog))
        }
        Some(Node::InsertStmt(insert)) if !insert.returning_list.is_empty() => {
            (Some(&insert.returning_list), None)
        }
        Some(Node::UpdateStmt(update)) if !update.returning_list.is_empty() => {
            (Some(&update.returning_list), None)
        }
        Some(Node::DeleteStmt(delete)) if !delete.returning_list.is_empty() => {
            (Some(&delete.returning_list), None)
        }
        Some(Node::MergeStmt(merge)) if !merge.returning_list.is_empty() => {
            (Some(&merge.returning_list), None)
        }
        _ => return Err(mismatch()),
    };
    let Some(columns) = last.columns else {
        return Ok(());
    };
    let targets: Option<Vec<&pb::Node>> = targets.and_then(|targets| {
        let values: Vec<&pb::Node> = targets.iter().filter_map(target_value).collect();
        let has_star = values
            .iter()
            .any(|value| matches!(&value.node, Some(Node::ColumnRef(column)) if column.fields.iter().any(|f| matches!(f.node, Some(Node::AStar(_))))));
        (!has_star && values.len() == columns).then_some(values)
    });
    let target_type = |index: usize| {
        let value = targets.as_ref()?.get(index)?;
        value_type(value, signature, from.as_ref())
    };
    let row_types = row_types(signature, catalog);
    let Some(row_types) = row_types else {
        if result.ty.is("record") || result.ty.is_composite() {
            // A record without output parameters, or a composite type
            // whose columns tursopg does not know.
            return Ok(());
        }
        if columns != 1 {
            return Err(mismatch());
        }
        return check_value_type(target_type(0), &result.ty, mismatch);
    };
    if columns == 1 && !signature.is_procedure {
        // A single column of the composite type is the result.
        match target_type(0) {
            Some(ty) if ty != "record" => {}
            _ => return Ok(()),
        }
    }
    for index in 0..columns {
        let Some(column_type) = row_types.get(index) else {
            return Err(mismatch());
        };
        let Some(column_type) = column_type else {
            continue;
        };
        check_value_type(target_type(index), column_type, mismatch)?;
    }
    if columns < row_types.len() {
        return Err(mismatch());
    }
    Ok(())
}

/// Checks that a value of the type `actual` converts to `expected` on
/// assignment.
fn check_value_type(
    actual: Option<&'static str>,
    expected: &TypeRef,
    mismatch: impl Fn() -> ParseError,
) -> Result<(), ParseError> {
    let (Some(actual), Some(expected_name)) = (actual, expected.builtin_name()) else {
        return Ok(());
    };
    if builtins::can_coerce(actual, expected_name, Coercion::Assignment) {
        Ok(())
    } else {
        Err(mismatch())
    }
}

/// The types of the columns of a composite result: the columns of a
/// relation, or the output parameters of a record. None for a scalar
/// result. A column of a type that tursopg does not know is None.
fn row_types(signature: &Signature, catalog: &dyn RoutineCatalog) -> Option<Vec<Option<TypeRef>>> {
    let result = &signature.result.ty;
    if result.is("record") {
        let outputs: Vec<Option<TypeRef>> = signature
            .parameters
            .iter()
            .filter(|parameter| parameter.is_output())
            .map(|parameter| Some(parameter.ty.clone()))
            .collect();
        return (!outputs.is_empty()).then_some(outputs);
    }
    let TypeRef::User { name, kind } = result else {
        return None;
    };
    if *kind != super::types::UserKind::Row {
        return None;
    }
    let relation = catalog.relation(None, name)?;
    Some(
        relation
            .columns
            .iter()
            .map(|(_, declared)| declared_type(declared))
            .collect(),
    )
}

/// The relation of a FROM clause with one table, with its name or alias.
fn single_relation(from: &[pb::Node], catalog: &dyn RoutineCatalog) -> Option<(String, Relation)> {
    let [item] = from else {
        return None;
    };
    let Some(Node::RangeVar(range)) = &item.node else {
        return None;
    };
    let schema = (!range.schemaname.is_empty()).then_some(range.schemaname.as_str());
    let relation = catalog.relation(schema, &range.relname)?;
    let name = range
        .alias
        .as_ref()
        .map(|alias| alias.aliasname.clone())
        .unwrap_or_else(|| range.relname.clone());
    Some((name, relation))
}

fn target_value(target: &pb::Node) -> Option<&pb::Node> {
    match &target.node {
        Some(Node::ResTarget(target)) => target.val.as_deref(),
        _ => None,
    }
}

/// The type of a result column: a parameter, a column of the FROM table or
/// a simple expression.
fn value_type(
    value: &pb::Node,
    signature: &Signature,
    from: Option<&(String, Relation)>,
) -> Option<&'static str> {
    match &value.node {
        Some(Node::ParamRef(param)) => signature.parameter_type(param.number as usize),
        Some(Node::ColumnRef(column)) => {
            let names = strings(&column.fields);
            let column_of = |table: Option<&str>, name: &str| {
                let (relation_name, relation) = from?;
                if table.is_some_and(|table| table != relation_name) {
                    return None;
                }
                let (_, declared) = relation.columns.iter().find(|(column, _)| column == name)?;
                declared_type(declared)?.builtin_name()
            };
            match names.as_slice() {
                [name] => column_of(None, name)
                    .or_else(|| signature.parameter_type(signature.parameter_number(name)?)),
                [table, name] => column_of(Some(table), name).or_else(|| {
                    (*table == signature.function_name)
                        .then(|| signature.parameter_type(signature.parameter_number(name)?))
                        .flatten()
                }),
                _ => None,
            }
        }
        // PostgreSQL gives a string literal and NULL in a select list the
        // type text.
        _ => match expr::expression_type(value)? {
            "unknown" => Some("text"),
            ty => Some(ty),
        },
    }
}

/// Checks that a string literal is a valid value of the type `target`, as
/// the input function of the type does when PostgreSQL converts the
/// literal. Only the numeric and boolean types are checked.
pub(super) fn check_literal(value: &pb::Node, target: &str) -> Result<(), ParseError> {
    let Some(Node::AConst(constant)) = &value.node else {
        return Ok(());
    };
    let Some(a_const::Val::Sval(text)) = &constant.val else {
        return Ok(());
    };
    check_literal_text(&text.sval, target)
}

/// Checks that `text` is a valid value of the type `target`. Only the
/// numeric and boolean types are checked.
pub(super) fn check_literal_text(text: &str, target: &str) -> Result<(), ParseError> {
    let display = builtins::type_display_name(target);
    let invalid = || {
        error(
            SQLSTATE_INVALID_TEXT_REPRESENTATION,
            format!("invalid input syntax for type {display}: \"{text}\""),
        )
    };
    let range = match target {
        "int2" => Some(i64::from(i16::MIN)..=i64::from(i16::MAX)),
        "int4" => Some(i64::from(i32::MIN)..=i64::from(i32::MAX)),
        "int8" => Some(i64::MIN..=i64::MAX),
        _ => None,
    };
    if let Some(range) = range {
        let value = parse_integer(text).ok_or_else(invalid)?;
        return match value {
            Some(value) if range.contains(&value) => Ok(()),
            _ => Err(error(
                SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
                format!("value \"{text}\" is out of range for type {display}"),
            )),
        };
    }
    let valid = match target {
        "numeric" | "float4" | "float8" => {
            let trimmed = text.trim().replace('_', "");
            trimmed.parse::<f64>().is_ok()
                || [
                    "nan",
                    "infinity",
                    "+infinity",
                    "-infinity",
                    "inf",
                    "+inf",
                    "-inf",
                ]
                .contains(&trimmed.to_ascii_lowercase().as_str())
        }
        "bool" => is_boolean(text),
        _ => true,
    };
    if valid {
        Ok(())
    } else {
        Err(invalid())
    }
}

/// The value of an integer literal as PostgreSQL reads it: with an optional
/// sign, digits with single underscores between them, or a 0x, 0o or 0b
/// prefix. None when the text is not an integer, and Some(None) when the
/// value does not fit in 64 bits.
fn parse_integer(text: &str) -> Option<Option<i64>> {
    let text = text.trim();
    let (negative, digits) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    let (radix, digits) = match digits.get(..2).map(str::to_ascii_lowercase).as_deref() {
        Some("0x") => (16, &digits[2..]),
        Some("0o") => (8, &digits[2..]),
        Some("0b") => (2, &digits[2..]),
        _ => (10, digits),
    };
    if digits.is_empty()
        || digits.starts_with('_')
        || digits.ends_with('_')
        || digits.contains("__")
        || !digits.chars().all(|c| c == '_' || c.is_digit(radix))
    {
        return None;
    }
    let digits = digits.replace('_', "");
    let magnitude = i128::from_str_radix(&digits, radix).ok();
    Some(magnitude.and_then(|magnitude| {
        let value = if negative { -magnitude } else { magnitude };
        i64::try_from(value).ok()
    }))
}

/// Whether PostgreSQL's boolin() accepts the text.
fn is_boolean(text: &str) -> bool {
    let text = text.trim().to_ascii_lowercase();
    let is_prefix_of = |word: &str| !text.is_empty() && word.starts_with(&text);
    is_prefix_of("true")
        || is_prefix_of("false")
        || is_prefix_of("yes")
        || is_prefix_of("no")
        || (text.len() >= 2 && (is_prefix_of("on") || is_prefix_of("off")))
        || text == "1"
        || text == "0"
}

/// Whether a statement defines, changes or drops an object or a privilege.
/// SELECT INTO creates a table.
pub(super) fn is_ddl(statement: &Node) -> bool {
    match statement {
        Node::SelectStmt(select) => select.into_clause.is_some(),
        Node::InsertStmt(_)
        | Node::UpdateStmt(_)
        | Node::DeleteStmt(_)
        | Node::MergeStmt(_)
        | Node::TruncateStmt(_)
        | Node::TransactionStmt(_)
        | Node::VariableSetStmt(_)
        | Node::VariableShowStmt(_)
        | Node::ExplainStmt(_)
        | Node::CopyStmt(_)
        | Node::LockStmt(_)
        | Node::NotifyStmt(_)
        | Node::ListenStmt(_)
        | Node::UnlistenStmt(_)
        | Node::CheckPointStmt(_)
        | Node::VacuumStmt(_)
        | Node::DiscardStmt(_)
        | Node::PrepareStmt(_)
        | Node::ExecuteStmt(_)
        | Node::DeallocateStmt(_)
        | Node::DeclareCursorStmt(_)
        | Node::FetchStmt(_)
        | Node::ClosePortalStmt(_)
        | Node::DoStmt(_)
        | Node::CallStmt(_)
        | Node::LoadStmt(_)
        | Node::ClusterStmt(_)
        | Node::ReindexStmt(_)
        | Node::ConstraintsSetStmt(_) => false,
        _ => true,
    }
}

/// The command name of a statement that changes data, as PostgreSQL names
/// it in its errors. A SELECT changes data with a data-modifying WITH query
/// or locks rows with FOR UPDATE or FOR SHARE.
pub(super) fn data_changing_command(statement: &Node) -> Option<&'static str> {
    match statement {
        Node::InsertStmt(_) => Some("INSERT"),
        Node::UpdateStmt(_) => Some("UPDATE"),
        Node::DeleteStmt(_) => Some("DELETE"),
        Node::MergeStmt(_) => Some("MERGE"),
        Node::TruncateStmt(_) => Some("TRUNCATE TABLE"),
        Node::SelectStmt(select) => {
            let has_modifying_query = select
                .with_clause
                .iter()
                .flat_map(|with| &with.ctes)
                .filter_map(|cte| match &cte.node {
                    Some(Node::CommonTableExpr(cte)) => cte.ctequery.as_deref()?.node.as_ref(),
                    _ => None,
                })
                .any(|query| data_changing_command(query).is_some());
            if has_modifying_query {
                return Some("SELECT");
            }
            select
                .locking_clause
                .iter()
                .find_map(|clause| match &clause.node {
                    Some(Node::LockingClause(clause)) => Some(match clause.strength() {
                        LockClauseStrength::LcsForkeyshare => "SELECT FOR KEY SHARE",
                        LockClauseStrength::LcsForshare => "SELECT FOR SHARE",
                        LockClauseStrength::LcsFornokeyupdate => "SELECT FOR NO KEY UPDATE",
                        _ => "SELECT FOR UPDATE",
                    }),
                    _ => None,
                })
        }
        _ => None,
    }
}

fn syntax_error(error: pg_query::Error) -> ParseError {
    ParseError::Sqlstate {
        sqlstate: SQLSTATE_SYNTAX_ERROR,
        message: match error {
            pg_query::Error::Parse(message) => message,
            error => error.to_string(),
        },
    }
}
