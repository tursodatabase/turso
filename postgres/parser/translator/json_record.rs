use super::{extract_type_name_from_typename, pg_type_name_to_ast_type};
use crate::ParseError;
use pg_query::protobuf::node::Node;
use pg_query::protobuf::RangeFunction;
use turso_parser::ast;

const INPUT_TABLE: &str = "json_input";
const INPUT_COLUMN: &str = "document";
const ELEMENT_TABLE: &str = "json_element";

pub(super) fn translate_json_to_record(
    function: JsonToRecordFunction,
    range_func: &RangeFunction,
    args: &[Box<ast::Expr>],
) -> Result<ast::SelectTable, ParseError> {
    let function_name = function.name();
    if range_func.is_rowsfrom {
        return Err(ParseError::ParseError(format!(
            "ROWS FROM is not supported with {function_name}"
        )));
    }
    if range_func.coldeflist.is_empty() {
        return Err(ParseError::ParseError(
            "a column definition list is required for functions returning \"record\"".into(),
        ));
    }
    if range_func.ordinality {
        return Err(ParseError::ParseError(
            "WITH ORDINALITY cannot be used with a column definition list".into(),
        ));
    }
    let [argument] = args else {
        return Err(ParseError::ParseError(format!(
            "function {function_name} takes one argument, not {}",
            args.len()
        )));
    };
    let columns = range_func
        .coldeflist
        .iter()
        .map(|node| JsonRecordColumn::from_node(function_name, node))
        .collect::<Result<Vec<_>, _>>()?;
    for (index, column) in columns.iter().enumerate() {
        if columns[..index]
            .iter()
            .any(|other| other.name == column.name)
        {
            return Err(ParseError::ParseError(format!(
                "column name \"{}\" specified more than once",
                column.name
            )));
        }
    }

    let input = ast::SelectTable::Select {
        select: select(
            vec![ast::ResultColumn::Expr(
                argument.clone(),
                Some(alias(INPUT_COLUMN)),
            )],
            None,
            None,
        ),
        alias: Some(alias(INPUT_TABLE)),
        lateral: false,
    };
    let document = column_reference(INPUT_TABLE, INPUT_COLUMN);
    let select = match function {
        JsonToRecordFunction::JsonToRecord | JsonToRecordFunction::JsonbToRecord => select(
            columns
                .iter()
                .map(|column| column.result_column(&document))
                .collect(),
            Some(ast::FromClause {
                select: Box::new(input),
                joins: vec![],
            }),
            Some(input_type_check(function, &document, integer_literal(1))),
        ),
        JsonToRecordFunction::JsonToRecordset | JsonToRecordFunction::JsonbToRecordset => {
            let elements = ast::SelectTable::TableCall(
                ast::QualifiedName::single(ast::Name::exact("json_each".to_string())),
                vec![Box::new(input_type_check(
                    function,
                    &document,
                    document.clone(),
                ))],
                Some(alias(ELEMENT_TABLE)),
            );
            let element = column_reference(ELEMENT_TABLE, "value");
            select(
                columns
                    .iter()
                    .map(|column| column.result_column(&element))
                    .collect(),
                Some(ast::FromClause {
                    select: Box::new(input),
                    joins: vec![ast::JoinedSelectTable {
                        operator: ast::JoinOperator::Comma,
                        table: Box::new(elements),
                        constraint: None,
                    }],
                }),
                Some(element_type_check(function_name)),
            )
        }
    };
    let alias_name = range_func
        .alias
        .as_ref()
        .map_or(function_name, |alias| alias.aliasname.as_str());
    Ok(ast::SelectTable::Select {
        select,
        alias: Some(alias(alias_name)),
        lateral: true,
    })
}

#[derive(Clone, Copy)]
pub(super) enum JsonToRecordFunction {
    JsonToRecord,
    JsonbToRecord,
    JsonToRecordset,
    JsonbToRecordset,
}

impl JsonToRecordFunction {
    pub(super) fn from_name(name: &str) -> Option<Self> {
        match name {
            "json_to_record" => Some(Self::JsonToRecord),
            "jsonb_to_record" => Some(Self::JsonbToRecord),
            "json_to_recordset" => Some(Self::JsonToRecordset),
            "jsonb_to_recordset" => Some(Self::JsonbToRecordset),
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::JsonToRecord => "json_to_record",
            Self::JsonbToRecord => "jsonb_to_record",
            Self::JsonToRecordset => "json_to_recordset",
            Self::JsonbToRecordset => "jsonb_to_recordset",
        }
    }
}

struct JsonRecordColumn {
    name: String,
    conversion: ColumnConversion,
    cast_type: ast::Type,
}

enum ColumnConversion {
    JsonValue,
    Boolean,
    Text,
    Scalar,
}

impl JsonRecordColumn {
    fn from_node(function_name: &str, node: &pg_query::protobuf::Node) -> Result<Self, ParseError> {
        let Some(Node::ColumnDef(column_def)) = &node.node else {
            return Err(ParseError::ParseError(format!(
                "{function_name}: column definition is not a ColumnDef"
            )));
        };
        let type_name = column_def.type_name.as_ref().ok_or_else(|| {
            ParseError::ParseError(format!(
                "{function_name}: column \"{}\" has no type",
                column_def.colname
            ))
        })?;
        let pg_type = extract_type_name_from_typename(type_name)?;
        let unsupported = || {
            ParseError::ParseError(format!(
                "{function_name} does not support the column type {pg_type}"
            ))
        };
        let cast_type = pg_type_name_to_ast_type(type_name).ok_or_else(unsupported)?;
        if cast_type.array_dimensions > 0 {
            return Err(unsupported());
        }
        let conversion = match pg_type.to_ascii_lowercase().as_str() {
            "json" | "jsonb" => ColumnConversion::JsonValue,
            "bool" | "boolean" => ColumnConversion::Boolean,
            _ => match cast_type.name.as_str() {
                "TEXT" => ColumnConversion::Text,
                "INTEGER" | "REAL" | "date" | "time" | "timestamp" | "timestamptz" => {
                    ColumnConversion::Scalar
                }
                _ => return Err(unsupported()),
            },
        };
        Ok(Self {
            name: column_def.colname.clone(),
            conversion,
            cast_type,
        })
    }

    fn result_column(&self, object: &ast::Expr) -> ast::ResultColumn {
        let path = string_literal(&json_key_path(&self.name));
        let value = match self.conversion {
            ColumnConversion::JsonValue => function_call(
                "nullif",
                vec![
                    binary(object, ast::Operator::ArrowRight, &path),
                    string_literal("null"),
                ],
            ),
            ColumnConversion::Boolean => function_call(
                "boolean_to_int",
                vec![binary(object, ast::Operator::ArrowRightShift, &path)],
            ),
            ColumnConversion::Text => json_value_text(object, &path),
            ColumnConversion::Scalar => binary(object, ast::Operator::ArrowRightShift, &path),
        };
        ast::ResultColumn::Expr(
            Box::new(ast::Expr::Cast {
                expr: Box::new(value),
                type_name: Some(self.cast_type.clone()),
            }),
            Some(alias(&self.name)),
        )
    }
}

fn json_key_path(key: &str) -> String {
    format!("$.\"{}\"", key.replace('\\', "\\\\").replace('"', "\\\""))
}

fn json_value_text(object: &ast::Expr, path: &ast::Expr) -> ast::Expr {
    ast::Expr::Case {
        base: Some(Box::new(function_call(
            "json_type",
            vec![object.clone(), path.clone()],
        ))),
        when_then_pairs: vec![
            (
                Box::new(string_literal("text")),
                Box::new(binary(object, ast::Operator::ArrowRightShift, path)),
            ),
            (
                Box::new(string_literal("null")),
                Box::new(ast::Expr::Literal(ast::Literal::Null)),
            ),
        ],
        else_expr: Some(Box::new(binary(object, ast::Operator::ArrowRight, path))),
    }
}

fn select(
    columns: Vec<ast::ResultColumn>,
    from: Option<ast::FromClause>,
    where_clause: Option<ast::Expr>,
) -> ast::Select {
    ast::Select {
        with: None,
        body: ast::SelectBody {
            select: ast::OneSelect::Select {
                distinctness: None,
                columns,
                from,
                where_clause: where_clause.map(Box::new),
                group_by: None,
                window_clause: vec![],
            },
            compounds: vec![],
        },
        order_by: vec![],
        limit: None,
    }
}

fn alias(name: &str) -> ast::As {
    ast::As::Elided(ast::Name::exact(name.to_string()))
}

fn input_type_check(
    function: JsonToRecordFunction,
    document: &ast::Expr,
    value_for_valid_type: ast::Expr,
) -> ast::Expr {
    let function_name = function.name();
    let (valid_type, other_type_error, default_error) = match function {
        JsonToRecordFunction::JsonToRecord | JsonToRecordFunction::JsonbToRecord => (
            "object",
            Some((
                "array",
                "cannot call populate_composite on an array".to_string(),
            )),
            "cannot call populate_composite on a scalar".to_string(),
        ),
        JsonToRecordFunction::JsonToRecordset => (
            "array",
            Some((
                "object",
                format!("cannot call {function_name} on an object"),
            )),
            format!("cannot call {function_name} on a scalar"),
        ),
        JsonToRecordFunction::JsonbToRecordset => (
            "array",
            None,
            format!("cannot call {function_name} on a non-array"),
        ),
    };
    let mut when_then_pairs = vec![(
        Box::new(string_literal(valid_type)),
        Box::new(value_for_valid_type),
    )];
    when_then_pairs.extend(other_type_error.map(|(json_type, message)| {
        (
            Box::new(string_literal(json_type)),
            Box::new(raise(&message)),
        )
    }));
    ast::Expr::Case {
        base: Some(Box::new(function_call(
            "coalesce",
            vec![
                function_call("json_type", vec![document.clone()]),
                string_literal(valid_type),
            ],
        ))),
        when_then_pairs,
        else_expr: Some(Box::new(raise(&default_error))),
    }
}

fn element_type_check(function_name: &str) -> ast::Expr {
    ast::Expr::Case {
        base: Some(Box::new(column_reference(ELEMENT_TABLE, "type"))),
        when_then_pairs: vec![(
            Box::new(string_literal("object")),
            Box::new(integer_literal(1)),
        )],
        else_expr: Some(Box::new(raise(&format!(
            "argument of {function_name} must be an array of objects"
        )))),
    }
}

fn column_reference(table: &str, column: &str) -> ast::Expr {
    ast::Expr::Qualified(
        ast::Name::exact(table.to_string()),
        ast::Name::exact(column.to_string()),
    )
}

fn integer_literal(value: i64) -> ast::Expr {
    ast::Expr::Literal(ast::Literal::Numeric(value.to_string()))
}

fn raise(message: &str) -> ast::Expr {
    ast::Expr::Raise(
        ast::ResolveType::Abort,
        Some(Box::new(string_literal(message))),
    )
}

fn function_call(name: &str, args: Vec<ast::Expr>) -> ast::Expr {
    ast::Expr::FunctionCall {
        name: ast::Name::exact(name.to_string()),
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

fn binary(left: &ast::Expr, operator: ast::Operator, right: &ast::Expr) -> ast::Expr {
    ast::Expr::Binary(Box::new(left.clone()), operator, Box::new(right.clone()))
}

fn string_literal(value: &str) -> ast::Expr {
    ast::Expr::Literal(ast::Literal::String(format!(
        "'{}'",
        value.replace('\'', "''")
    )))
}
