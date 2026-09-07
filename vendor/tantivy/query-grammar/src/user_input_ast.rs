use std::fmt;
use std::fmt::{Debug, Formatter};

use serde::Serialize;

use crate::Occur;

#[derive(PartialEq, Eq, Hash, Clone, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum UserInputLeaf {
    Literal(UserInputLiteral),
    All,
    Range {
        field: Option<String>,
        lower: UserInputBound,
        upper: UserInputBound,
    },
    Set {
        field: Option<String>,
        elements: Vec<String>,
    },
    Exists {
        field: String,
    },
    Regex {
        field: Option<String>,
        pattern: String,
    },
}

impl UserInputLeaf {
    pub(crate) fn set_field(self, field: Option<String>) -> Self {
        match self {
            UserInputLeaf::Literal(mut literal) => {
                literal.field_name = field;
                UserInputLeaf::Literal(literal)
            }
            UserInputLeaf::All => UserInputLeaf::All,
            UserInputLeaf::Range {
                field: _,
                lower,
                upper,
            } => UserInputLeaf::Range {
                field,
                lower,
                upper,
            },
            UserInputLeaf::Set { field: _, elements } => UserInputLeaf::Set { field, elements },
            UserInputLeaf::Exists { field: _ } => UserInputLeaf::Exists {
                field: field.expect("Exist query without a field isn't allowed"),
            },
            UserInputLeaf::Regex { field: _, pattern } => UserInputLeaf::Regex { field, pattern },
        }
    }

    pub(crate) fn set_default_field(&mut self, default_field: String) {
        match self {
            UserInputLeaf::Literal(literal) if literal.field_name.is_none() => {
                literal.field_name = Some(default_field)
            }
            UserInputLeaf::All => {
                *self = UserInputLeaf::Exists {
                    field: default_field,
                }
            }
            UserInputLeaf::Range { field, .. } if field.is_none() => *field = Some(default_field),
            UserInputLeaf::Set { field, .. } if field.is_none() => *field = Some(default_field),
            UserInputLeaf::Regex { field, .. } if field.is_none() => *field = Some(default_field),
            _ => (), // field was already set, do nothing
        }
    }
}

impl Debug for UserInputLeaf {
    fn fmt(&self, formatter: &mut Formatter) -> Result<(), fmt::Error> {
        match self {
            UserInputLeaf::Literal(literal) => literal.fmt(formatter),
            UserInputLeaf::Range {
                field,
                lower,
                upper,
            } => {
                if let Some(field) = field {
                    // TODO properly escape field (in case of \")
                    write!(formatter, "\"{field}\":")?;
                }
                lower.display_lower(formatter)?;
                write!(formatter, " TO ")?;
                upper.display_upper(formatter)?;
                Ok(())
            }
            UserInputLeaf::Set { field, elements } => {
                if let Some(field) = field {
                    // TODO properly escape field (in case of \")
                    write!(formatter, "\"{field}\": ")?;
                }
                write!(formatter, "IN [")?;
                for (i, text) in elements.iter().enumerate() {
                    if i != 0 {
                        write!(formatter, " ")?;
                    }
                    // TODO properly escape element
                    write!(formatter, "\"{text}\"")?;
                }
                write!(formatter, "]")
            }
            UserInputLeaf::All => write!(formatter, "*"),
            UserInputLeaf::Exists { field } => {
                write!(formatter, "$exists(\"{field}\")")
            }
            UserInputLeaf::Regex { field, pattern } => {
                if let Some(field) = field {
                    // TODO properly escape field (in case of \")
                    write!(formatter, "\"{field}\":")?;
                }
                // TODO properly escape pattern (in case of \")
                write!(formatter, "/{pattern}/")
            }
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Delimiter {
    SingleQuotes,
    DoubleQuotes,
    None,
}

#[derive(PartialEq, Eq, Hash, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct UserInputLiteral {
    pub field_name: Option<String>,
    pub phrase: String,
    pub delimiter: Delimiter,
    pub slop: u32,
    pub prefix: bool,
}

impl fmt::Debug for UserInputLiteral {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        if let Some(ref field) = self.field_name {
            // TODO properly escape field (in case of \")
            write!(formatter, "\"{field}\":")?;
        }
        match self.delimiter {
            Delimiter::SingleQuotes => {
                // TODO properly escape element (in case of \')
                write!(formatter, "'{}'", self.phrase)?;
            }
            Delimiter::DoubleQuotes => {
                // TODO properly escape element (in case of \")
                write!(formatter, "\"{}\"", self.phrase)?;
            }
            Delimiter::None => {
                // TODO properly escape element
                write!(formatter, "{}", self.phrase)?;
            }
        }
        if self.slop > 0 {
            write!(formatter, "~{}", self.slop)?;
        } else if self.prefix {
            write!(formatter, "*")?;
        }
        Ok(())
    }
}

#[derive(PartialEq, Eq, Hash, Debug, Clone, Serialize)]
#[serde(tag = "type", content = "value")]
#[serde(rename_all = "snake_case")]
pub enum UserInputBound {
    Inclusive(String),
    Exclusive(String),
    Unbounded,
}

impl UserInputBound {
    fn display_lower(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            // TODO properly escape word if required
            UserInputBound::Inclusive(ref word) => write!(formatter, "[\"{word}\""),
            UserInputBound::Exclusive(ref word) => write!(formatter, "{{\"{word}\""),
            UserInputBound::Unbounded => write!(formatter, "{{\"*\""),
        }
    }

    fn display_upper(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            // TODO properly escape word if required
            UserInputBound::Inclusive(ref word) => write!(formatter, "\"{word}\"]"),
            UserInputBound::Exclusive(ref word) => write!(formatter, "\"{word}\"}}"),
            UserInputBound::Unbounded => write!(formatter, "\"*\"}}"),
        }
    }

    pub fn term_str(&self) -> &str {
        match *self {
            UserInputBound::Inclusive(ref contents) => contents,
            UserInputBound::Exclusive(ref contents) => contents,
            UserInputBound::Unbounded => "*",
        }
    }
}

#[derive(PartialEq, Eq, Hash, Clone, Serialize)]
#[serde(into = "UserInputAstSerde")]
pub enum UserInputAst {
    Clause(Vec<(Option<Occur>, UserInputAst)>),
    Boost(Box<UserInputAst>, ordered_float::OrderedFloat<f64>),
    Leaf(Box<UserInputLeaf>),
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum UserInputAstSerde {
    Bool {
        clauses: Vec<(Option<Occur>, UserInputAst)>,
    },
    Boost {
        underlying: Box<UserInputAst>,
        boost: f64,
    },
    #[serde(untagged)]
    Leaf(Box<UserInputLeaf>),
}

impl From<UserInputAst> for UserInputAstSerde {
    fn from(ast: UserInputAst) -> Self {
        match ast {
            UserInputAst::Clause(clause) => UserInputAstSerde::Bool { clauses: clause },
            UserInputAst::Boost(underlying, boost) => UserInputAstSerde::Boost {
                underlying,
                boost: boost.into_inner(),
            },
            UserInputAst::Leaf(leaf) => UserInputAstSerde::Leaf(leaf),
        }
    }
}

impl UserInputAst {
    #[must_use]
    pub fn unary(self, occur: Occur) -> UserInputAst {
        UserInputAst::Clause(vec![(Some(occur), self)])
    }

    fn compose(occur: Occur, asts: Vec<UserInputAst>) -> UserInputAst {
        assert_ne!(occur, Occur::MustNot);
        assert!(!asts.is_empty());
        if asts.len() == 1 {
            asts.into_iter().next().unwrap() //< safe
        } else {
            UserInputAst::Clause(
                asts.into_iter()
                    .map(|ast: UserInputAst| (Some(occur), ast))
                    .collect::<Vec<_>>(),
            )
        }
    }

    pub fn empty_query() -> UserInputAst {
        UserInputAst::Clause(Vec::default())
    }

    pub fn and(asts: Vec<UserInputAst>) -> UserInputAst {
        UserInputAst::compose(Occur::Must, asts)
    }

    pub fn or(asts: Vec<UserInputAst>) -> UserInputAst {
        UserInputAst::compose(Occur::Should, asts)
    }

    pub(crate) fn set_default_field(&mut self, field: String) {
        match self {
            UserInputAst::Clause(clauses) => clauses
                .iter_mut()
                .for_each(|(_, ast)| ast.set_default_field(field.clone())),
            UserInputAst::Leaf(leaf) => leaf.set_default_field(field),
            UserInputAst::Boost(ast, _) => ast.set_default_field(field),
        }
    }
}

impl From<UserInputLiteral> for UserInputLeaf {
    fn from(literal: UserInputLiteral) -> UserInputLeaf {
        UserInputLeaf::Literal(literal)
    }
}

impl From<UserInputLeaf> for UserInputAst {
    fn from(leaf: UserInputLeaf) -> UserInputAst {
        UserInputAst::Leaf(Box::new(leaf))
    }
}

fn print_occur_ast(
    occur_opt: Option<Occur>,
    ast: &UserInputAst,
    formatter: &mut fmt::Formatter,
) -> fmt::Result {
    if let Some(occur) = occur_opt {
        write!(formatter, "{occur}{ast:?}")?;
    } else {
        write!(formatter, "*{ast:?}")?;
    }
    Ok(())
}

impl fmt::Debug for UserInputAst {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            UserInputAst::Clause(ref subqueries) => {
                if subqueries.is_empty() {
                    // TODO this will break ast reserialization, is writing "( )" enough?
                    write!(formatter, "<emptyclause>")?;
                } else {
                    write!(formatter, "(")?;
                    print_occur_ast(subqueries[0].0, &subqueries[0].1, formatter)?;
                    for subquery in &subqueries[1..] {
                        write!(formatter, " ")?;
                        print_occur_ast(subquery.0, &subquery.1, formatter)?;
                    }
                    write!(formatter, ")")?;
                }
                Ok(())
            }
            UserInputAst::Leaf(ref subquery) => write!(formatter, "{subquery:?}"),
            UserInputAst::Boost(ref leaf, boost) => write!(formatter, "({leaf:?})^{boost}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_leaf_serialization() {
        let ast = UserInputAst::Leaf(Box::new(UserInputLeaf::All));
        let json = serde_json::to_string(&ast).unwrap();
        assert_eq!(json, r#"{"type":"all"}"#);
    }

    #[test]
    fn test_literal_leaf_serialization() {
        let literal = UserInputLiteral {
            field_name: Some("title".to_string()),
            phrase: "hello".to_string(),
            delimiter: Delimiter::None,
            slop: 0,
            prefix: false,
        };
        let ast = UserInputAst::Leaf(Box::new(UserInputLeaf::Literal(literal)));
        let json = serde_json::to_string(&ast).unwrap();
        assert_eq!(
            json,
            r#"{"type":"literal","field_name":"title","phrase":"hello","delimiter":"none","slop":0,"prefix":false}"#
        );
    }

    #[test]
    fn test_range_leaf_serialization() {
        let range = UserInputLeaf::Range {
            field: Some("price".to_string()),
            lower: UserInputBound::Inclusive("10".to_string()),
            upper: UserInputBound::Exclusive("100".to_string()),
        };
        let ast = UserInputAst::Leaf(Box::new(range));
        let json = serde_json::to_string(&ast).unwrap();
        assert_eq!(
            json,
            r#"{"type":"range","field":"price","lower":{"type":"inclusive","value":"10"},"upper":{"type":"exclusive","value":"100"}}"#
        );
    }

    #[test]
    fn test_range_leaf_unbounded_serialization() {
        let range = UserInputLeaf::Range {
            field: Some("price".to_string()),
            lower: UserInputBound::Inclusive("10".to_string()),
            upper: UserInputBound::Unbounded,
        };
        let ast = UserInputAst::Leaf(Box::new(range));
        let json = serde_json::to_string(&ast).unwrap();
        assert_eq!(
            json,
            r#"{"type":"range","field":"price","lower":{"type":"inclusive","value":"10"},"upper":{"type":"unbounded"}}"#
        );
    }

    #[test]
    fn test_boost_serialization() {
        let inner_ast = UserInputAst::Leaf(Box::new(UserInputLeaf::All));
        let boost_ast = UserInputAst::Boost(Box::new(inner_ast), 2.5.into());
        let json = serde_json::to_string(&boost_ast).unwrap();
        assert_eq!(
            json,
            r#"{"type":"boost","underlying":{"type":"all"},"boost":2.5}"#
        );
    }

    #[test]
    fn test_boost_serialization2() {
        let boost_ast = UserInputAst::Boost(
            Box::new(UserInputAst::Clause(vec![
                (
                    Some(Occur::Must),
                    UserInputAst::Leaf(Box::new(UserInputLeaf::All)),
                ),
                (
                    Some(Occur::Should),
                    UserInputAst::Leaf(Box::new(UserInputLeaf::Literal(UserInputLiteral {
                        field_name: Some("title".to_string()),
                        phrase: "hello".to_string(),
                        delimiter: Delimiter::None,
                        slop: 0,
                        prefix: false,
                    }))),
                ),
            ])),
            2.5.into(),
        );
        let json = serde_json::to_string(&boost_ast).unwrap();
        assert_eq!(
            json,
            r#"{"type":"boost","underlying":{"type":"bool","clauses":[["must",{"type":"all"}],["should",{"type":"literal","field_name":"title","phrase":"hello","delimiter":"none","slop":0,"prefix":false}]]},"boost":2.5}"#
        );
    }

    #[test]
    fn test_clause_serialization() {
        let clause = UserInputAst::Clause(vec![
            (
                Some(Occur::Must),
                UserInputAst::Leaf(Box::new(UserInputLeaf::All)),
            ),
            (
                Some(Occur::Should),
                UserInputAst::Leaf(Box::new(UserInputLeaf::Literal(UserInputLiteral {
                    field_name: Some("title".to_string()),
                    phrase: "hello".to_string(),
                    delimiter: Delimiter::None,
                    slop: 0,
                    prefix: false,
                }))),
            ),
        ]);
        let json = serde_json::to_string(&clause).unwrap();
        assert_eq!(
            json,
            r#"{"type":"bool","clauses":[["must",{"type":"all"}],["should",{"type":"literal","field_name":"title","phrase":"hello","delimiter":"none","slop":0,"prefix":false}]]}"#
        );
    }
}
