//! Recursive-descent parser for the Starlark subset, producing the
//! [`UdfStmt`]/[`UdfExpr`] IR. Operator precedence follows the Starlark spec
//! (which matches Python for the supported operators). Comparison operators
//! may not be chained, per the Starlark spec.

use super::lexer::{SpannedTok, Tok};
use super::{StarlarkBody, UdfBinOp, UdfExpr, UdfStmt, UdfUnOp};
use crate::{LimboError, Result};

pub fn parse(toks: Vec<SpannedTok>) -> Result<StarlarkBody> {
    let mut p = Parser {
        toks,
        pos: 0,
        loop_depth: 0,
    };
    p.skip_newlines();
    // Allow the whole body to be uniformly indented (common when the SQL
    // statement itself is indented): consume one wrapping Indent/Dedent pair.
    let stmts = if p.eat(&Tok::Indent) {
        let stmts = p.parse_stmts_until(&Tok::Dedent)?;
        p.expect(&Tok::Dedent, "end of block")?;
        stmts
    } else {
        p.parse_stmts_until(&Tok::Eof)?
    };
    p.skip_newlines();
    p.expect(&Tok::Eof, "end of function body")?;
    Ok(StarlarkBody { stmts })
}

struct Parser {
    toks: Vec<SpannedTok>,
    pos: usize,
    loop_depth: usize,
}

impl Parser {
    fn peek(&self) -> &Tok {
        &self.toks[self.pos].tok
    }

    fn peek2(&self) -> &Tok {
        if self.pos + 1 < self.toks.len() {
            &self.toks[self.pos + 1].tok
        } else {
            &Tok::Eof
        }
    }

    fn line(&self) -> usize {
        self.toks[self.pos].line
    }

    fn bump(&mut self) -> Tok {
        let tok = self.toks[self.pos].tok.clone();
        if self.pos + 1 < self.toks.len() {
            self.pos += 1;
        }
        tok
    }

    fn eat(&mut self, tok: &Tok) -> bool {
        if self.peek() == tok {
            self.bump();
            true
        } else {
            false
        }
    }

    fn expect(&mut self, tok: &Tok, what: &str) -> Result<()> {
        if self.eat(tok) {
            Ok(())
        } else {
            Err(self.err(format_args!("expected {what}, found {:?}", self.peek())))
        }
    }

    fn err(&self, msg: impl std::fmt::Display) -> LimboError {
        LimboError::ParseError(format!("starlark (line {}): {msg}", self.line()))
    }

    fn skip_newlines(&mut self) {
        while self.eat(&Tok::Newline) {}
    }

    fn parse_stmts_until(&mut self, end: &Tok) -> Result<Vec<UdfStmt>> {
        let mut stmts = Vec::new();
        loop {
            self.skip_newlines();
            if self.peek() == end || self.peek() == &Tok::Eof {
                break;
            }
            self.parse_statement(&mut stmts)?;
        }
        if stmts.is_empty() {
            return Err(self.err("expected at least one statement"));
        }
        Ok(stmts)
    }

    fn parse_statement(&mut self, out: &mut Vec<UdfStmt>) -> Result<()> {
        match self.peek() {
            Tok::If => {
                let stmt = self.parse_if()?;
                out.push(stmt);
            }
            Tok::While => {
                self.bump();
                let cond = self.parse_expr()?;
                self.expect(&Tok::Colon, "':' after while condition")?;
                self.loop_depth += 1;
                let body = self.parse_block();
                self.loop_depth -= 1;
                out.push(UdfStmt::While { cond, body: body? });
            }
            Tok::For => {
                let stmt = self.parse_for()?;
                out.push(stmt);
            }
            Tok::Indent => return Err(self.err("unexpected indent")),
            _ => self.parse_simple_line(out)?,
        }
        Ok(())
    }

    fn parse_if(&mut self) -> Result<UdfStmt> {
        self.expect(&Tok::If, "'if'")?;
        let mut arms = Vec::new();
        let cond = self.parse_expr()?;
        self.expect(&Tok::Colon, "':' after if condition")?;
        arms.push((cond, self.parse_block()?));
        let mut else_body = None;
        loop {
            if self.eat(&Tok::Elif) {
                let cond = self.parse_expr()?;
                self.expect(&Tok::Colon, "':' after elif condition")?;
                arms.push((cond, self.parse_block()?));
            } else if self.eat(&Tok::Else) {
                self.expect(&Tok::Colon, "':' after else")?;
                else_body = Some(self.parse_block()?);
                break;
            } else {
                break;
            }
        }
        Ok(UdfStmt::If { arms, else_body })
    }

    fn parse_for(&mut self) -> Result<UdfStmt> {
        self.expect(&Tok::For, "'for'")?;
        let var = match self.bump() {
            Tok::Ident(name) => name,
            other => return Err(self.err(format_args!("expected loop variable, found {other:?}"))),
        };
        self.expect(&Tok::In, "'in'")?;
        match self.bump() {
            Tok::Ident(name) if name == "range" => {}
            _ => {
                return Err(self.err("only 'for <var> in range(...)' iteration is supported"));
            }
        }
        self.expect(&Tok::LParen, "'(' after range")?;
        let mut args = vec![self.parse_expr()?];
        while self.eat(&Tok::Comma) {
            args.push(self.parse_expr()?);
        }
        self.expect(&Tok::RParen, "')' after range arguments")?;
        let (start, stop, step_expr) = match args.len() {
            1 => {
                let mut it = args.into_iter();
                (None, it.next().expect("one range arg"), None)
            }
            2 => {
                let mut it = args.into_iter();
                (
                    Some(it.next().expect("two range args")),
                    it.next().expect("two range args"),
                    None,
                )
            }
            3 => {
                let mut it = args.into_iter();
                (
                    Some(it.next().expect("three range args")),
                    it.next().expect("three range args"),
                    Some(it.next().expect("three range args")),
                )
            }
            n => return Err(self.err(format_args!("range() takes 1 to 3 arguments, got {n}"))),
        };
        let step = match step_expr {
            None => 1,
            Some(UdfExpr::Int(k)) => k,
            Some(UdfExpr::Unary(UdfUnOp::Neg, inner)) => match *inner {
                UdfExpr::Int(k) => -k,
                _ => return Err(self.err("range() step must be an integer literal")),
            },
            Some(_) => return Err(self.err("range() step must be an integer literal")),
        };
        if step == 0 {
            return Err(self.err("range() step must not be zero"));
        }
        self.expect(&Tok::Colon, "':' after for clause")?;
        self.loop_depth += 1;
        let body = self.parse_block();
        self.loop_depth -= 1;
        Ok(UdfStmt::For {
            var,
            start,
            stop,
            step,
            body: body?,
        })
    }

    /// Parse a suite after `:`: either an indented block or one or more
    /// simple statements on the same line.
    fn parse_block(&mut self) -> Result<Vec<UdfStmt>> {
        if self.eat(&Tok::Newline) {
            self.expect(&Tok::Indent, "an indented block")?;
            let stmts = self.parse_stmts_until(&Tok::Dedent)?;
            self.expect(&Tok::Dedent, "end of block")?;
            Ok(stmts)
        } else {
            let mut stmts = Vec::new();
            self.parse_simple_line(&mut stmts)?;
            Ok(stmts)
        }
    }

    /// One or more simple statements separated by `;`, terminated by a
    /// newline (or end of input).
    fn parse_simple_line(&mut self, out: &mut Vec<UdfStmt>) -> Result<()> {
        loop {
            out.push(self.parse_simple_stmt()?);
            if !self.eat(&Tok::Semi) {
                break;
            }
            if matches!(self.peek(), Tok::Newline | Tok::Eof) {
                break;
            }
        }
        if !self.eat(&Tok::Newline) && self.peek() != &Tok::Eof {
            return Err(self.err(format_args!(
                "expected end of line, found {:?}",
                self.peek()
            )));
        }
        Ok(())
    }

    fn parse_simple_stmt(&mut self) -> Result<UdfStmt> {
        match self.peek() {
            Tok::Return => {
                self.bump();
                if matches!(self.peek(), Tok::Newline | Tok::Semi | Tok::Eof) {
                    Ok(UdfStmt::Return(None))
                } else {
                    Ok(UdfStmt::Return(Some(self.parse_expr()?)))
                }
            }
            Tok::Break => {
                if self.loop_depth == 0 {
                    return Err(self.err("'break' outside of a loop"));
                }
                self.bump();
                Ok(UdfStmt::Break)
            }
            Tok::Continue => {
                if self.loop_depth == 0 {
                    return Err(self.err("'continue' outside of a loop"));
                }
                self.bump();
                Ok(UdfStmt::Continue)
            }
            Tok::Pass => {
                self.bump();
                Ok(UdfStmt::Pass)
            }
            Tok::Ident(_) => {
                let augmented = match self.peek2() {
                    Tok::Assign => None,
                    Tok::PlusEq => Some(UdfBinOp::Add),
                    Tok::MinusEq => Some(UdfBinOp::Sub),
                    Tok::StarEq => Some(UdfBinOp::Mul),
                    Tok::SlashEq => Some(UdfBinOp::Div),
                    Tok::SlashSlashEq => Some(UdfBinOp::FloorDiv),
                    Tok::PercentEq => Some(UdfBinOp::Mod),
                    _ => return Ok(UdfStmt::Expr(self.parse_expr()?)),
                };
                let name = match self.bump() {
                    Tok::Ident(name) => name,
                    _ => unreachable!("peeked an identifier"),
                };
                self.bump(); // the assignment operator
                let rhs = self.parse_expr()?;
                let value = match augmented {
                    None => rhs,
                    Some(op) => {
                        UdfExpr::Binary(op, Box::new(UdfExpr::Var(name.clone())), Box::new(rhs))
                    }
                };
                Ok(UdfStmt::Assign { name, value })
            }
            _ => Ok(UdfStmt::Expr(self.parse_expr()?)),
        }
    }

    fn parse_expr(&mut self) -> Result<UdfExpr> {
        self.parse_ternary()
    }

    fn parse_ternary(&mut self) -> Result<UdfExpr> {
        let then = self.parse_or()?;
        if self.eat(&Tok::If) {
            let cond = self.parse_or()?;
            self.expect(&Tok::Else, "'else' in conditional expression")?;
            let else_ = self.parse_ternary()?;
            Ok(UdfExpr::Ternary {
                cond: Box::new(cond),
                then: Box::new(then),
                else_: Box::new(else_),
            })
        } else {
            Ok(then)
        }
    }

    fn parse_or(&mut self) -> Result<UdfExpr> {
        let mut lhs = self.parse_and()?;
        while self.eat(&Tok::Or) {
            let rhs = self.parse_and()?;
            lhs = UdfExpr::Binary(UdfBinOp::Or, Box::new(lhs), Box::new(rhs));
        }
        Ok(lhs)
    }

    fn parse_and(&mut self) -> Result<UdfExpr> {
        let mut lhs = self.parse_not()?;
        while self.eat(&Tok::And) {
            let rhs = self.parse_not()?;
            lhs = UdfExpr::Binary(UdfBinOp::And, Box::new(lhs), Box::new(rhs));
        }
        Ok(lhs)
    }

    fn parse_not(&mut self) -> Result<UdfExpr> {
        if self.eat(&Tok::Not) {
            let inner = self.parse_not()?;
            Ok(UdfExpr::Unary(UdfUnOp::Not, Box::new(inner)))
        } else {
            self.parse_cmp()
        }
    }

    fn cmp_op(&self) -> Option<UdfBinOp> {
        match self.peek() {
            Tok::EqEq => Some(UdfBinOp::Eq),
            Tok::NotEq => Some(UdfBinOp::NotEq),
            Tok::Lt => Some(UdfBinOp::Lt),
            Tok::LtEq => Some(UdfBinOp::LtEq),
            Tok::Gt => Some(UdfBinOp::Gt),
            Tok::GtEq => Some(UdfBinOp::GtEq),
            _ => None,
        }
    }

    fn parse_cmp(&mut self) -> Result<UdfExpr> {
        let lhs = self.parse_bitor()?;
        if let Some(op) = self.cmp_op() {
            self.bump();
            let rhs = self.parse_bitor()?;
            if self.cmp_op().is_some() {
                return Err(self.err("comparison operators may not be chained"));
            }
            return Ok(UdfExpr::Binary(op, Box::new(lhs), Box::new(rhs)));
        }
        Ok(lhs)
    }

    fn parse_bitor(&mut self) -> Result<UdfExpr> {
        let mut lhs = self.parse_bitand()?;
        while self.eat(&Tok::Pipe) {
            let rhs = self.parse_bitand()?;
            lhs = UdfExpr::Binary(UdfBinOp::BitOr, Box::new(lhs), Box::new(rhs));
        }
        Ok(lhs)
    }

    fn parse_bitand(&mut self) -> Result<UdfExpr> {
        let mut lhs = self.parse_shift()?;
        while self.eat(&Tok::Amp) {
            let rhs = self.parse_shift()?;
            lhs = UdfExpr::Binary(UdfBinOp::BitAnd, Box::new(lhs), Box::new(rhs));
        }
        Ok(lhs)
    }

    fn parse_shift(&mut self) -> Result<UdfExpr> {
        let mut lhs = self.parse_addsub()?;
        loop {
            let op = match self.peek() {
                Tok::LtLt => UdfBinOp::Shl,
                Tok::GtGt => UdfBinOp::Shr,
                _ => break,
            };
            self.bump();
            let rhs = self.parse_addsub()?;
            lhs = UdfExpr::Binary(op, Box::new(lhs), Box::new(rhs));
        }
        Ok(lhs)
    }

    fn parse_addsub(&mut self) -> Result<UdfExpr> {
        let mut lhs = self.parse_muldiv()?;
        loop {
            let op = match self.peek() {
                Tok::Plus => UdfBinOp::Add,
                Tok::Minus => UdfBinOp::Sub,
                _ => break,
            };
            self.bump();
            let rhs = self.parse_muldiv()?;
            lhs = UdfExpr::Binary(op, Box::new(lhs), Box::new(rhs));
        }
        Ok(lhs)
    }

    fn parse_muldiv(&mut self) -> Result<UdfExpr> {
        let mut lhs = self.parse_unary()?;
        loop {
            let op = match self.peek() {
                Tok::Star => UdfBinOp::Mul,
                Tok::Slash => UdfBinOp::Div,
                Tok::SlashSlash => UdfBinOp::FloorDiv,
                Tok::Percent => UdfBinOp::Mod,
                _ => break,
            };
            self.bump();
            let rhs = self.parse_unary()?;
            lhs = UdfExpr::Binary(op, Box::new(lhs), Box::new(rhs));
        }
        Ok(lhs)
    }

    fn parse_unary(&mut self) -> Result<UdfExpr> {
        let op = match self.peek() {
            Tok::Minus => Some(UdfUnOp::Neg),
            Tok::Plus => Some(UdfUnOp::Pos),
            Tok::Tilde => Some(UdfUnOp::BitNot),
            _ => None,
        };
        if let Some(op) = op {
            self.bump();
            let inner = self.parse_unary()?;
            return Ok(UdfExpr::Unary(op, Box::new(inner)));
        }
        self.parse_atom()
    }

    fn parse_atom(&mut self) -> Result<UdfExpr> {
        match self.peek().clone() {
            Tok::Int(v) => {
                self.bump();
                Ok(UdfExpr::Int(v))
            }
            Tok::Float(v) => {
                self.bump();
                Ok(UdfExpr::Float(v))
            }
            Tok::Str(_) => {
                let Tok::Str(s) = self.bump() else {
                    unreachable!("peeked a string");
                };
                Ok(UdfExpr::Str(s))
            }
            Tok::True => {
                self.bump();
                Ok(UdfExpr::Bool(true))
            }
            Tok::False => {
                self.bump();
                Ok(UdfExpr::Bool(false))
            }
            Tok::NoneLit => {
                self.bump();
                Ok(UdfExpr::None)
            }
            Tok::LParen => {
                self.bump();
                let inner = self.parse_expr()?;
                self.expect(&Tok::RParen, "')'")?;
                Ok(inner)
            }
            Tok::Ident(name) => {
                self.bump();
                if self.eat(&Tok::LParen) {
                    let mut args = Vec::new();
                    if self.peek() != &Tok::RParen {
                        loop {
                            if matches!(self.peek(), Tok::Ident(_)) && self.peek2() == &Tok::Assign
                            {
                                return Err(self.err("keyword arguments are not supported"));
                            }
                            args.push(self.parse_expr()?);
                            if !self.eat(&Tok::Comma) {
                                break;
                            }
                        }
                    }
                    self.expect(&Tok::RParen, "')' after call arguments")?;
                    Ok(UdfExpr::Call { name, args })
                } else {
                    Ok(UdfExpr::Var(name))
                }
            }
            Tok::StarStar => Err(self.err("the '**' operator is not supported")),
            other => Err(self.err(format_args!("unexpected token {other:?}"))),
        }
    }
}
