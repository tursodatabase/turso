use std::num::NonZero;

use limbo_sqlite3_parser::ast::{Expr, Update};

#[derive(Clone, Debug)]
pub enum Parameter {
    Anonymous(NonZero<usize>),
    Indexed(NonZero<usize>),
    Named(String, NonZero<usize>),
}

impl PartialEq for Parameter {
    fn eq(&self, other: &Self) -> bool {
        self.index() == other.index()
    }
}

impl Parameter {
    pub fn index(&self) -> NonZero<usize> {
        match self {
            Parameter::Anonymous(index) => *index,
            Parameter::Indexed(index) => *index,
            Parameter::Named(_, index) => *index,
        }
    }
}

#[derive(Debug)]
pub struct Parameters {
    index: NonZero<usize>,
    pub list: Vec<Parameter>,
    update_ctx: Option<UpdateContext>,
}

impl Default for Parameters {
    fn default() -> Self {
        Self::new()
    }
}

impl Parameters {
    pub fn new() -> Self {
        Self {
            index: 1.try_into().unwrap(),
            list: vec![],
            update_ctx: None,
        }
    }

    /// Initialize the context for an update query
    pub fn init_update_context(&mut self, body: &mut Update) {
        self.update_ctx = Some(gather_parameter_count(body));
    }

    /// Set the location and position of the tranlsator in the update stmt.
    pub fn set_update_position(&mut self, pos: UpdatePos) {
        if let Some(ref mut ctx) = self.update_ctx {
            if let UpdatePos::Where = pos {
                if ctx.where_pos.is_none() {
                    ctx.where_pos = Some(0);
                }
            }
            ctx.current_position = pos;
        }
    }

    pub fn count(&self) -> usize {
        let mut params = self.list.clone();
        params.dedup();
        params.len()
    }

    pub fn name(&self, index: NonZero<usize>) -> Option<String> {
        self.list.iter().find_map(|p| match p {
            Parameter::Anonymous(i) if *i == index => Some("?".to_string()),
            Parameter::Indexed(i) if *i == index => Some(format!("?{i}")),
            Parameter::Named(name, i) if *i == index => Some(name.to_owned()),
            _ => None,
        })
    }

    pub fn index(&self, name: impl AsRef<str>) -> Option<NonZero<usize>> {
        self.list
            .iter()
            .find_map(|p| match p {
                Parameter::Named(n, index) if n == name.as_ref() => Some(index),
                _ => None,
            })
            .copied()
    }

    pub fn next_index(&mut self) -> NonZero<usize> {
        let index = self.index;
        self.index = self.index.checked_add(1).unwrap();
        index
    }

    pub fn push(&mut self, name: impl AsRef<str>) -> NonZero<usize> {
        match name.as_ref() {
            "" => {
                let index = self.next_index();
                self.list.push(Parameter::Anonymous(index));
                if let Some(ctx) = &mut self.update_ctx {
                    return ctx.get_index().unwrap_or(index);
                };
                tracing::trace!("anonymous parameter at {index}");
                index
            }
            name if name.starts_with(['$', ':', '@', '#']) => {
                match self
                    .list
                    .iter()
                    .find(|p| matches!(p, Parameter::Named(n, _) if name == n))
                {
                    Some(t) => {
                        let index = t.index();
                        self.list.push(t.clone());
                        tracing::trace!("named parameter at {index} as {name}");
                        index
                    }
                    None => {
                        let index = self.next_index();
                        self.list.push(Parameter::Named(name.to_owned(), index));
                        tracing::trace!("named parameter at {index} as {name}");
                        index
                    }
                }
            }
            index => {
                // SAFETY: Guaranteed from parser that the index is bigger than 0.
                let index: NonZero<usize> = index.parse().unwrap();
                if index > self.index {
                    self.index = index.checked_add(1).unwrap();
                }
                self.list.push(Parameter::Indexed(index));
                tracing::trace!("indexed parameter at {index}");
                index
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct UpdateContext {
    current_position: UpdatePos,
    // overall parameter index, index into set clause
    set_clause: Vec<(usize, usize)>,
    // there is only one where clause, but can contain multiple variables
    // so we store the index of the overall parameter # and then keep track of the
    // current index into the where clause
    where_clause: Vec<usize>,
    where_pos: Option<usize>,
}

#[derive(Clone, Copy, Debug)]
pub enum UpdatePos {
    Where,
    Set(usize),
}

impl UpdateContext {
    fn new() -> Self {
        UpdateContext {
            current_position: UpdatePos::Where,
            set_clause: vec![],
            where_clause: vec![],
            where_pos: None,
        }
    }

    /// Since we stored the index of the overall parameter # associated with the position
    /// in the clause, we can look it up here because we have the current translation position.
    /// For 'set' clauses, it's passed in from the translator, for 'where' clauses it's initialized
    /// in the translator and then kept track of internally.
    fn get_index(&mut self) -> Option<NonZero<usize>> {
        match self.current_position {
            UpdatePos::Set(idx) => self.set_clause.iter().find(|s| s.1 == idx).map(|s| s.0),
            UpdatePos::Where => {
                if self.where_pos.is_none() {
                    self.where_pos = Some(0);
                }
                let res = self.where_clause.get(self.where_pos.unwrap()).copied();
                if let Some(pos) = self.where_pos.as_mut() {
                    *pos += 1; // increment position for next parameter
                }
                res
            }
        }
        .and_then(NonZero::new)
    }
}

fn gather_parameter_count(body: &mut Update) -> UpdateContext {
    let mut params = UpdateContext::new();
    let mut idx = 1;
    body.sets.iter().enumerate().for_each(|(i, set)| {
        if let Expr::Variable(_) = set.expr {
            params.set_clause.push((idx, i));
            idx += 1;
        }
    });
    if let Some(where_clause) = &body.where_clause {
        traverse_expr_counting_variables(&mut params.where_clause, &mut idx, where_clause);
    }
    params
}

fn traverse_expr_counting_variables(params: &mut Vec<usize>, pos: &mut usize, expr: &Expr) {
    match expr {
        Expr::Variable(s) => {
            // only count anonymous variables
            if s.is_empty() {
                params.push(*pos);
                *pos += 1;
            }
        }
        Expr::Binary(lhs, _, rhs) => {
            traverse_expr_counting_variables(params, pos, lhs);
            traverse_expr_counting_variables(params, pos, rhs);
        }
        Expr::Unary(_, expr) => {
            traverse_expr_counting_variables(params, pos, expr);
        }
        Expr::Parenthesized(expr) => {
            traverse_expr_counting_variables(params, pos, &expr[0]);
        }
        Expr::InList { lhs, rhs, .. } => {
            traverse_expr_counting_variables(params, pos, lhs);
            if let Some(rhs) = rhs {
                for expr in rhs.iter() {
                    traverse_expr_counting_variables(params, pos, expr);
                }
            }
        }
        Expr::FunctionCall {
            args: Some(args), ..
        } => {
            for expr in args.iter() {
                traverse_expr_counting_variables(params, pos, expr);
            }
        }
        Expr::Between {
            lhs, start, end, ..
        } => {
            traverse_expr_counting_variables(params, pos, lhs);
            traverse_expr_counting_variables(params, pos, start);
            traverse_expr_counting_variables(params, pos, end);
        }
        Expr::Like { lhs, rhs, .. } => {
            traverse_expr_counting_variables(params, pos, lhs);
            traverse_expr_counting_variables(params, pos, rhs);
        }
        _ => {}
    }
}
