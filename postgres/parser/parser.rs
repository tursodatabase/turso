//! PostgreSQL parser implementation
//!
//! This parser builds an AST from PostgreSQL SQL input using recursive descent.

use crate::ast::*;
use crate::error::{Error, Result};
use crate::lexer::Lexer;
use crate::token::{Token, TokenType};

pub struct Parser<'a> {
    lexer: Lexer<'a>,
    current_token: Token,
}

impl<'a> Parser<'a> {
    pub fn new(input: &'a [u8]) -> Self {
        let mut lexer = Lexer::new(input);
        let current_token =
            lexer
                .next_token()
                .unwrap_or(Token::new(TokenType::Eof, String::new(), 0));
        Parser {
            lexer,
            current_token,
        }
    }

    /// Parse multiple statements
    pub fn parse_statements(&mut self) -> Result<Vec<Stmt>> {
        let mut statements = Vec::new();

        while !self.is_eof() {
            statements.push(self.parse_statement()?);

            // Optional semicolon between statements
            if self.current_token.token_type == TokenType::Semicolon {
                self.advance()?;
            }
        }

        Ok(statements)
    }

    /// Parse a single statement
    pub fn parse_statement(&mut self) -> Result<Stmt> {
        match self.current_token.token_type {
            TokenType::Select | TokenType::With => self.parse_select_statement(),
            TokenType::Insert => self.parse_insert_statement(),
            TokenType::Update => self.parse_update_statement(),
            TokenType::Delete => self.parse_delete_statement(),
            TokenType::Create => self.parse_create_statement(),
            TokenType::Alter => self.parse_alter_statement(),
            TokenType::Drop => self.parse_drop_statement(),
            TokenType::Begin => self.parse_begin_statement(),
            TokenType::Commit => {
                self.advance()?;
                Ok(Stmt::Commit)
            }
            TokenType::Rollback => {
                self.advance()?;
                Ok(Stmt::Rollback)
            }

            // Administrative commands
            TokenType::Explain => self.parse_explain_statement(),
            TokenType::Copy => self.parse_copy_statement(),
            TokenType::Set => self.parse_set_statement(),
            TokenType::Show => self.parse_show_statement(),
            TokenType::Reset => self.parse_reset_statement(),
            TokenType::Analyze => self.parse_analyze_statement(),
            TokenType::Vacuum => self.parse_vacuum_statement(),
            TokenType::Checkpoint => {
                self.advance()?;
                Ok(Stmt::Checkpoint)
            }
            TokenType::Discard => self.parse_discard_statement(),
            TokenType::Deallocate => self.parse_deallocate_statement(),

            // Session/Transaction
            TokenType::Savepoint => self.parse_savepoint_statement(),
            TokenType::Release => self.parse_release_statement(),
            TokenType::Prepare => self.parse_prepare_statement(),
            TokenType::Listen => self.parse_listen_statement(),
            TokenType::Notify => self.parse_notify_statement(),
            TokenType::Lock => self.parse_lock_statement(),
            TokenType::Grant => self.parse_grant_statement(),
            TokenType::Revoke => self.parse_revoke_statement(),

            // DML Extensions
            TokenType::Truncate => self.parse_truncate_statement(),

            _ => Err(Error::SyntaxError(format!(
                "Expected statement, got {:?}",
                self.current_token.token_type
            ))),
        }
    }

    /// Parse SELECT statement
    fn parse_select_statement(&mut self) -> Result<Stmt> {
        // Handle WITH clause if present
        if self.current_token.token_type == TokenType::With {
            return self.parse_with_statement();
        }

        self.expect(TokenType::Select)?;

        // Parse DISTINCT
        let distinct = if self.current_token.token_type == TokenType::Distinct {
            self.advance()?;
            if self.consume(TokenType::On)? {
                self.expect(TokenType::LeftParen)?;
                let exprs = self.parse_expression_list()?;
                self.expect(TokenType::RightParen)?;
                Some(Distinct::On(exprs))
            } else {
                Some(Distinct::All)
            }
        } else {
            None
        };

        // Parse columns
        let columns = self.parse_select_columns()?;

        // Parse FROM clause
        let from = if self.consume(TokenType::From)? {
            Some(self.parse_table_refs()?)
        } else {
            None
        };

        // Parse WHERE clause
        let where_clause = if self.consume(TokenType::Where)? {
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Parse GROUP BY clause
        let group_by = if self.consume_keywords(&[TokenType::Group, TokenType::By])? {
            Some(self.parse_expression_list()?)
        } else {
            None
        };

        // Parse HAVING clause
        let having = if self.consume(TokenType::Having)? {
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Parse ORDER BY clause
        let order_by = if self.consume_keywords(&[TokenType::Order, TokenType::By])? {
            Some(self.parse_order_by_list()?)
        } else {
            None
        };

        // Parse LIMIT clause
        let limit = if self.consume(TokenType::Limit)? {
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Parse OFFSET clause
        let offset = if self.consume(TokenType::Offset)? {
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Parse FOR clause (PostgreSQL locking)
        let for_clause = if self.consume(TokenType::For)? {
            Some(self.parse_for_clause()?)
        } else {
            None
        };

        Ok(Stmt::Select(Box::new(SelectStmt {
            distinct,
            columns,
            from,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
            for_clause,
        })))
    }

    /// Parse INSERT statement
    fn parse_insert_statement(&mut self) -> Result<Stmt> {
        self.expect(TokenType::Insert)?;
        self.expect(TokenType::Into)?;

        let table = self.parse_table_ref()?;

        // Parse optional column list
        let columns = if self.current_token.token_type == TokenType::LeftParen {
            self.advance()?;
            let cols = self.parse_column_spec_list()?;
            self.expect(TokenType::RightParen)?;
            Some(cols)
        } else {
            None
        };

        // Parse VALUES or SELECT
        let values = if self.consume(TokenType::Values)? {
            let mut rows = Vec::new();
            loop {
                // Handle both (expr1, expr2, ...) and ROW(expr1, expr2, ...) syntax
                if self.current_token.token_type == TokenType::Row {
                    // ROW(expr1, expr2, ...) syntax
                    self.advance()?; // consume ROW
                    self.expect(TokenType::LeftParen)?;
                    let row = self.parse_expression_list()?;
                    self.expect(TokenType::RightParen)?;
                    rows.push(row);
                } else {
                    // (expr1, expr2, ...) syntax
                    self.expect(TokenType::LeftParen)?;
                    let row = self.parse_expression_list()?;
                    self.expect(TokenType::RightParen)?;
                    rows.push(row);
                }

                if !self.consume(TokenType::Comma)? {
                    break;
                }
            }
            InsertValues::Values(rows)
        } else if self.current_token.token_type == TokenType::Select {
            InsertValues::Select(Box::new(self.parse_select_inner()?))
        } else {
            return Err(Error::SyntaxError("Expected VALUES or SELECT".to_string()));
        };

        // Parse ON CONFLICT clause
        let on_conflict = if self.consume_keywords(&[TokenType::On, TokenType::Conflict])? {
            Some(self.parse_on_conflict()?)
        } else {
            None
        };

        // Parse RETURNING clause
        let returning = if self.consume(TokenType::Returning)? {
            Some(self.parse_select_columns()?)
        } else {
            None
        };

        Ok(Stmt::Insert(InsertStmt {
            table,
            columns,
            values,
            on_conflict,
            returning,
        }))
    }

    /// Parse UPDATE statement
    fn parse_update_statement(&mut self) -> Result<Stmt> {
        self.expect(TokenType::Update)?;

        let table = self.parse_table_ref()?;

        self.expect(TokenType::Set)?;
        let set = self.parse_assignment_list()?;

        // Parse FROM clause (PostgreSQL extension)
        let from = if self.consume(TokenType::From)? {
            Some(self.parse_table_refs()?)
        } else {
            None
        };

        // Parse WHERE clause
        let where_clause = if self.consume(TokenType::Where)? {
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Parse RETURNING clause
        let returning = if self.consume(TokenType::Returning)? {
            Some(self.parse_select_columns()?)
        } else {
            None
        };

        Ok(Stmt::Update(UpdateStmt {
            table,
            set,
            from,
            where_clause,
            returning,
        }))
    }

    /// Parse DELETE statement
    fn parse_delete_statement(&mut self) -> Result<Stmt> {
        self.expect(TokenType::Delete)?;
        self.expect(TokenType::From)?;

        let table = self.parse_table_ref()?;

        // Parse USING clause (PostgreSQL extension)
        let using = if self.consume(TokenType::Using)? {
            Some(self.parse_table_refs()?)
        } else {
            None
        };

        // Parse WHERE clause
        let where_clause = if self.consume(TokenType::Where)? {
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Parse RETURNING clause
        let returning = if self.consume(TokenType::Returning)? {
            Some(self.parse_select_columns()?)
        } else {
            None
        };

        Ok(Stmt::Delete(DeleteStmt {
            table,
            using,
            where_clause,
            returning,
        }))
    }

    /// Parse CREATE statement
    fn parse_create_statement(&mut self) -> Result<Stmt> {
        self.expect(TokenType::Create)?;

        match self.current_token.token_type {
            TokenType::Table => {
                self.advance()?; // consume TABLE
                self.parse_create_table(false)
            },
            TokenType::Temporary | TokenType::Temp => {
                self.advance()?; // consume TEMPORARY/TEMP
                self.expect(TokenType::Table)?;
                self.parse_create_table(true)
            }
            TokenType::Index => self.parse_create_index(),
            _ => Err(Error::SyntaxError(format!(
                "CREATE {:?} not yet supported",
                self.current_token.token_type
            ))),
        }
    }

    /// Parse CREATE TABLE
    fn parse_create_table(&mut self, temporary: bool) -> Result<Stmt> {
        // TABLE keyword already consumed in parse_create_statement

        let if_not_exists =
            self.consume_keywords(&[TokenType::If, TokenType::Not, TokenType::Exists])?;

        // Parse table name (without alias support to avoid AS conflict)
        let table_name = self.parse_identifier()?;
        let table = if self.consume(TokenType::Dot)? {
            let name = self.parse_identifier()?;
            TableRef {
                schema: Some(table_name),
                name,
                alias: None,
            }
        } else {
            TableRef {
                schema: None,
                name: table_name,
                alias: None,
            }
        };

        // Check if this is CREATE TABLE AS SELECT
        if self.consume(TokenType::As)? {
            let select = Box::new(self.parse_select_statement_inner()?);
            return Ok(Stmt::CreateTable(CreateTableStmt {
                temporary,
                if_not_exists,
                table,
                columns: Vec::new(),
                constraints: Vec::new(),
                as_select: Some(select),
                partition_of: None,
            }));
        }

        // Check if this is CREATE TABLE PARTITION OF
        if self.consume(TokenType::Partition)? {
            self.expect(TokenType::Of)?;
            let partition_spec = self.parse_partition_spec()?;
            return Ok(Stmt::CreateTable(CreateTableStmt {
                temporary,
                if_not_exists,
                table,
                columns: Vec::new(),
                constraints: Vec::new(),
                as_select: None,
                partition_of: Some(partition_spec),
            }));
        }

        self.expect(TokenType::LeftParen)?;

        let mut columns = Vec::new();
        let mut constraints = Vec::new();

        loop {
            // Try to parse as column definition or table constraint
            if self.is_constraint_keyword() {
                constraints.push(self.parse_table_constraint()?);
            } else {
                columns.push(self.parse_column_definition()?);
            }

            if !self.consume(TokenType::Comma)? {
                break;
            }
        }

        self.expect(TokenType::RightParen)?;

        Ok(Stmt::CreateTable(CreateTableStmt {
            temporary,
            if_not_exists,
            table,
            columns,
            constraints,
            as_select: None,
            partition_of: None,
        }))
    }

    fn parse_partition_spec(&mut self) -> Result<PartitionSpec> {
        use crate::ast::{PartitionSpec, PartitionBound};

        // Parse the parent table name
        let parent_table = self.parse_table_ref()?;

        // Parse FOR VALUES clause
        self.expect(TokenType::For)?;
        self.expect(TokenType::Values)?;

        let partition_bound = if self.consume(TokenType::In)? {
            // FOR VALUES IN (value1, value2, ...)
            self.expect(TokenType::LeftParen)?;
            let values = self.parse_expression_list()?;
            self.expect(TokenType::RightParen)?;
            PartitionBound::In(values)
        } else if self.consume(TokenType::From)? {
            // FOR VALUES FROM (start_values...) TO (end_values...)
            self.expect(TokenType::LeftParen)?;
            let from_values = self.parse_expression_list()?;
            self.expect(TokenType::RightParen)?;
            self.expect(TokenType::To)?;
            self.expect(TokenType::LeftParen)?;
            let to_values = self.parse_expression_list()?;
            self.expect(TokenType::RightParen)?;
            PartitionBound::Range {
                from: from_values,
                to: to_values,
            }
        } else {
            return Err(Error::SyntaxError("Expected IN or FROM in partition bound".to_string()));
        };

        Ok(PartitionSpec {
            parent_table,
            partition_bound,
        })
    }

    /// Parse CREATE INDEX
    fn parse_create_index(&mut self) -> Result<Stmt> {
        self.expect(TokenType::Index)?;

        let unique = false; // TODO: Parse UNIQUE INDEX
        let if_not_exists =
            self.consume_keywords(&[TokenType::If, TokenType::Not, TokenType::Exists])?;

        let name = self.parse_identifier()?;

        self.expect(TokenType::On)?;
        let table = self.parse_table_ref()?;

        self.expect(TokenType::LeftParen)?;
        let columns = self.parse_index_columns()?;
        self.expect(TokenType::RightParen)?;

        let where_clause = if self.consume(TokenType::Where)? {
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok(Stmt::CreateIndex(CreateIndexStmt {
            unique,
            if_not_exists,
            name,
            table,
            columns,
            where_clause,
        }))
    }

    /// Parse ALTER statement
    fn parse_alter_statement(&mut self) -> Result<Stmt> {
        self.expect(TokenType::Alter)?;
        self.expect(TokenType::Table)?;

        let table = self.parse_table_ref()?;
        let action = self.parse_alter_table_action()?;

        Ok(Stmt::AlterTable(AlterTableStmt { table, action }))
    }

    /// Parse DROP statement
    fn parse_drop_statement(&mut self) -> Result<Stmt> {
        self.expect(TokenType::Drop)?;
        self.expect(TokenType::Table)?;

        let if_exists = self.consume_keywords(&[TokenType::If, TokenType::Exists])?;

        let mut tables = vec![self.parse_table_ref()?];
        while self.consume(TokenType::Comma)? {
            tables.push(self.parse_table_ref()?);
        }

        let cascade = self.consume(TokenType::Cascade)?;

        Ok(Stmt::DropTable(DropTableStmt {
            if_exists,
            tables,
            cascade,
        }))
    }

    /// Parse BEGIN statement
    fn parse_begin_statement(&mut self) -> Result<Stmt> {
        self.expect(TokenType::Begin)?;

        // TODO: Parse isolation level and read/write mode

        Ok(Stmt::Begin(BeginStmt {
            isolation_level: None,
            read_write: None,
        }))
    }

    /// Parse WITH statement (CTEs)
    fn parse_with_statement(&mut self) -> Result<Stmt> {
        self.expect(TokenType::With)?;

        let _recursive = self.consume(TokenType::Recursive)?;

        // Parse CTE list
        loop {
            let _name = self.parse_identifier()?;
            if self.current_token.token_type == TokenType::LeftParen {
                // Column list
                self.advance()?;
                let _columns = self.parse_identifier_list()?;
                self.expect(TokenType::RightParen)?;
            }
            self.expect(TokenType::As)?;
            self.expect(TokenType::LeftParen)?;
            let _query = self.parse_query_with_set_operations()?;
            self.expect(TokenType::RightParen)?;

            if !self.consume(TokenType::Comma)? {
                break;
            }
        }

        // Parse main query
        self.parse_select_statement()
    }

    /// Parse a query that can include set operations (UNION, INTERSECT, EXCEPT)
    fn parse_query_with_set_operations(&mut self) -> Result<SelectStmt> {
        let left = self.parse_single_select_query()?;

        // Check for set operations
        if self.current_token.token_type == TokenType::Union
            || self.current_token.token_type == TokenType::Intersect
            || self.current_token.token_type == TokenType::Except
        {
            // For now, we'll parse this as a complex expression
            // In a full implementation, we'd need to extend the AST to support set operations
            // For the CTE case, let's just parse both sides and return the left for now
            let _op = self.current_token.token_type;
            self.advance()?; // consume the set operation

            // Handle optional ALL
            let _all = self.consume(TokenType::All)?;

            // Parse the right side
            let _right = self.parse_single_select_query()?;

            // For now, just return the left side
            // TODO: Properly handle set operations in AST
            return Ok(left);
        }

        Ok(left)
    }

    /// Parse a single SELECT query (without set operations)
    fn parse_single_select_query(&mut self) -> Result<SelectStmt> {
        self.expect(TokenType::Select)?;

        // Parse DISTINCT
        let distinct = if self.current_token.token_type == TokenType::Distinct {
            self.advance()?;
            if self.consume(TokenType::On)? {
                self.expect(TokenType::LeftParen)?;
                let exprs = self.parse_expression_list()?;
                self.expect(TokenType::RightParen)?;
                Some(Distinct::On(exprs))
            } else {
                Some(Distinct::All)
            }
        } else {
            None
        };

        // Parse columns
        let columns = self.parse_select_columns()?;

        // Parse FROM clause
        let from = if self.consume(TokenType::From)? {
            Some(self.parse_table_refs()?)
        } else {
            None
        };

        // Parse WHERE clause
        let where_clause = if self.consume(TokenType::Where)? {
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Parse GROUP BY clause
        let group_by = if self.consume_keywords(&[TokenType::Group, TokenType::By])? {
            Some(self.parse_expression_list()?)
        } else {
            None
        };

        // Parse HAVING clause
        let having = if self.consume(TokenType::Having)? {
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Parse ORDER BY clause
        let order_by = if self.consume_keywords(&[TokenType::Order, TokenType::By])? {
            Some(self.parse_order_by_list()?)
        } else {
            None
        };

        // Parse LIMIT clause
        let limit = if self.consume(TokenType::Limit)? {
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Parse OFFSET clause
        let offset = if self.consume(TokenType::Offset)? {
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok(SelectStmt {
            distinct,
            columns,
            from,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
            for_clause: None, // FOR clause not relevant in CTE context
        })
    }

    // Helper methods for parsing expressions

    /// Parse an expression (precedence climbing)
    fn parse_expression(&mut self) -> Result<Expr> {
        self.parse_or_expression()
    }

    fn parse_or_expression(&mut self) -> Result<Expr> {
        let mut left = self.parse_and_expression()?;

        while self.current_token.token_type == TokenType::Or {
            self.advance()?;
            let right = self.parse_and_expression()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::Or,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_and_expression(&mut self) -> Result<Expr> {
        let mut left = self.parse_not_expression()?;

        while self.current_token.token_type == TokenType::And {
            self.advance()?;
            let right = self.parse_not_expression()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::And,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_not_expression(&mut self) -> Result<Expr> {
        if self.current_token.token_type == TokenType::Not {
            self.advance()?;
            let expr = self.parse_not_expression()?;
            Ok(Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(expr),
            })
        } else {
            self.parse_comparison_expression()
        }
    }

    fn parse_comparison_expression(&mut self) -> Result<Expr> {
        let mut left = self.parse_additive_expression()?;

        loop {
            let op = match self.current_token.token_type {
                TokenType::Equal => BinaryOperator::Equal,
                TokenType::NotEqual => BinaryOperator::NotEqual,
                TokenType::Less => BinaryOperator::Less,
                TokenType::Greater => BinaryOperator::Greater,
                TokenType::LessEqual => BinaryOperator::LessEqual,
                TokenType::GreaterEqual => BinaryOperator::GreaterEqual,
                TokenType::Like => BinaryOperator::Like,
                TokenType::Ilike => BinaryOperator::Ilike,
                TokenType::Contains => BinaryOperator::Contains,
                TokenType::ContainedBy => BinaryOperator::ContainedBy,
                TokenType::Overlap => BinaryOperator::Overlap,
                _ => break,
            };

            self.advance()?;
            let right = self.parse_additive_expression()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_additive_expression(&mut self) -> Result<Expr> {
        let mut left = self.parse_multiplicative_expression()?;

        while let Some(op) = match self.current_token.token_type {
            TokenType::Plus => Some(BinaryOperator::Plus),
            TokenType::Minus => Some(BinaryOperator::Minus),
            TokenType::Concat => Some(BinaryOperator::Concat),
            _ => None,
        } {
            self.advance()?;
            let right = self.parse_multiplicative_expression()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_multiplicative_expression(&mut self) -> Result<Expr> {
        let mut left = self.parse_unary_expression()?;

        while let Some(op) = match self.current_token.token_type {
            TokenType::Star => Some(BinaryOperator::Multiply),
            TokenType::Slash => Some(BinaryOperator::Divide),
            TokenType::Percent => Some(BinaryOperator::Modulo),
            _ => None,
        } {
            self.advance()?;
            let right = self.parse_unary_expression()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_unary_expression(&mut self) -> Result<Expr> {
        match self.current_token.token_type {
            TokenType::Plus => {
                self.advance()?;
                Ok(Expr::UnaryOp {
                    op: UnaryOperator::Plus,
                    expr: Box::new(self.parse_unary_expression()?),
                })
            }
            TokenType::Minus => {
                self.advance()?;
                Ok(Expr::UnaryOp {
                    op: UnaryOperator::Minus,
                    expr: Box::new(self.parse_unary_expression()?),
                })
            }
            _ => self.parse_postfix_expression(),
        }
    }

    fn parse_postfix_expression(&mut self) -> Result<Expr> {
        let mut expr = self.parse_primary_expression()?;

        loop {
            match self.current_token.token_type {
                TokenType::TypeCast => {
                    self.advance()?;
                    let type_name = self.parse_type_name_string()?;
                    expr = Expr::TypeCast {
                        expr: Box::new(expr),
                        type_name,
                    };
                }
                TokenType::LeftBracket => {
                    self.advance()?;

                    // Check for slice syntax [start:end]
                    if self.current_token.token_type == TokenType::Colon {
                        // [:end] syntax
                        self.advance()?; // consume ':'
                        let end = if self.current_token.token_type == TokenType::RightBracket {
                            None
                        } else {
                            Some(Box::new(self.parse_expression()?))
                        };
                        self.expect(TokenType::RightBracket)?;
                        expr = Expr::ArraySlice {
                            array: Box::new(expr),
                            start: None,
                            end,
                        };
                    } else {
                        // Parse first expression
                        let first_expr = self.parse_expression()?;

                        if self.current_token.token_type == TokenType::Colon {
                            // [start:end] syntax
                            self.advance()?; // consume ':'
                            let end = if self.current_token.token_type == TokenType::RightBracket {
                                None
                            } else {
                                Some(Box::new(self.parse_expression()?))
                            };
                            self.expect(TokenType::RightBracket)?;
                            expr = Expr::ArraySlice {
                                array: Box::new(expr),
                                start: Some(Box::new(first_expr)),
                                end,
                            };
                        } else {
                            // [index] syntax
                            self.expect(TokenType::RightBracket)?;
                            expr = Expr::ArraySubscript {
                                array: Box::new(expr),
                                index: Box::new(first_expr),
                            };
                        }
                    }
                }
                TokenType::Arrow
                | TokenType::LongArrow
                | TokenType::HashArrow
                | TokenType::HashLongArrow => {
                    let op = match self.current_token.token_type {
                        TokenType::Arrow => JsonOperator::Arrow,
                        TokenType::LongArrow => JsonOperator::LongArrow,
                        TokenType::HashArrow => JsonOperator::HashArrow,
                        TokenType::HashLongArrow => JsonOperator::HashLongArrow,
                        _ => unreachable!(),
                    };
                    self.advance()?;
                    let path = self.parse_primary_expression()?;
                    expr = Expr::JsonAccess {
                        expr: Box::new(expr),
                        op,
                        path: Box::new(path),
                    };
                }
                TokenType::Dot => {
                    self.advance()?;
                    let field = self.parse_identifier()?;

                    let mut parts = match expr {
                        Expr::Identifier(s) => vec![s],
                        Expr::QualifiedIdentifier(parts) => parts,
                        _ => {
                            return Err(Error::SyntaxError(
                                "Invalid qualified identifier".to_string(),
                            ))
                        }
                    };
                    parts.push(field);
                    expr = Expr::QualifiedIdentifier(parts);
                }
                _ => break,
            }
        }

        Ok(expr)
    }

    fn parse_primary_expression(&mut self) -> Result<Expr> {
        match self.current_token.token_type {
            TokenType::IntegerLiteral => {
                let value =
                    self.current_token
                        .value
                        .parse::<i64>()
                        .map_err(|_| Error::InvalidNumber {
                            value: self.current_token.value.clone(),
                        })?;
                self.advance()?;
                Ok(Expr::Integer(value))
            }
            TokenType::FloatLiteral => {
                let value =
                    self.current_token
                        .value
                        .parse::<f64>()
                        .map_err(|_| Error::InvalidNumber {
                            value: self.current_token.value.clone(),
                        })?;
                self.advance()?;
                Ok(Expr::Float(value))
            }
            TokenType::String | TokenType::DollarQuotedString => {
                let value = self.current_token.value.clone();
                self.advance()?;
                Ok(Expr::String(value))
            }
            TokenType::True => {
                self.advance()?;
                Ok(Expr::Boolean(true))
            }
            TokenType::False => {
                self.advance()?;
                Ok(Expr::Boolean(false))
            }
            TokenType::Null => {
                self.advance()?;
                Ok(Expr::Null)
            }
            TokenType::DollarParameter => {
                let param_str = &self.current_token.value[1..]; // Skip $
                let param_num = param_str
                    .parse::<u32>()
                    .map_err(|_| Error::InvalidParameter(param_str.to_string()))?;
                self.advance()?;
                Ok(Expr::DollarParameter(param_num))
            }
            TokenType::Array => {
                self.advance()?;
                self.expect(TokenType::LeftBracket)?;
                let elements = if self.current_token.token_type == TokenType::RightBracket {
                    Vec::new()
                } else {
                    self.parse_expression_list()?
                };
                self.expect(TokenType::RightBracket)?;
                Ok(Expr::Array(elements))
            }
            TokenType::LeftBracket => {
                // Raw array literal [1,2,3] (used in nested array contexts)
                self.advance()?;
                let elements = if self.current_token.token_type == TokenType::RightBracket {
                    Vec::new()
                } else {
                    self.parse_expression_list()?
                };
                self.expect(TokenType::RightBracket)?;
                Ok(Expr::Array(elements))
            }
            TokenType::LeftParen => {
                self.advance()?;
                // Could be subquery, tuple/row constructor, or grouped expression
                if self.current_token.token_type == TokenType::Select {
                    let subquery = self.parse_select_inner()?;
                    self.expect(TokenType::RightParen)?;
                    Ok(Expr::Subquery(Box::new(subquery)))
                } else if self.current_token.token_type == TokenType::RightParen {
                    // Empty tuple: ()
                    self.advance()?;
                    Ok(Expr::Row(Vec::new()))
                } else {
                    let first_expr = self.parse_expression()?;

                    if self.current_token.token_type == TokenType::Comma {
                        // This is a tuple/row constructor: (expr1, expr2, ...)
                        let mut elements = vec![first_expr];
                        while self.consume(TokenType::Comma)? {
                            elements.push(self.parse_expression()?);
                        }
                        self.expect(TokenType::RightParen)?;
                        Ok(Expr::Row(elements))
                    } else {
                        // This is a grouped expression: (expr)
                        self.expect(TokenType::RightParen)?;
                        Ok(first_expr)
                    }
                }
            }
            TokenType::Row => {
                self.advance()?;
                self.expect(TokenType::LeftParen)?;
                let elements = if self.current_token.token_type == TokenType::RightParen {
                    Vec::new()
                } else {
                    self.parse_expression_list()?
                };
                self.expect(TokenType::RightParen)?;
                Ok(Expr::Row(elements))
            }
            TokenType::Cast => {
                self.advance()?;
                self.expect(TokenType::LeftParen)?;
                let expr = self.parse_expression()?;
                self.expect(TokenType::As)?;
                let data_type = self.parse_data_type()?;
                self.expect(TokenType::RightParen)?;
                Ok(Expr::Cast {
                    expr: Box::new(expr),
                    data_type,
                })
            }
            TokenType::Exists => {
                self.advance()?;
                self.expect(TokenType::LeftParen)?;
                let subquery = self.parse_select_inner()?;
                self.expect(TokenType::RightParen)?;
                Ok(Expr::Exists(Box::new(subquery)))
            }
            TokenType::Interval => {
                self.advance()?;
                // Parse the interval value (e.g., '1', '30')
                let value = self.parse_primary_expression()?;
                // Parse optional interval unit (DAY, HOUR, MINUTE, etc.)
                let unit = if self.current_token.token_type.can_be_identifier() {
                    let unit_name = self.current_token.value.clone();
                    self.advance()?;
                    Some(unit_name)
                } else {
                    None
                };
                Ok(Expr::Interval {
                    value: Box::new(value),
                    unit,
                })
            }
            TokenType::Case => self.parse_case_expression(),
            TokenType::Identifier | TokenType::QuotedIdentifier => {
                let name = self.current_token.value.clone();
                self.advance()?;

                // Check if it's a function call
                if self.current_token.token_type == TokenType::LeftParen {
                    self.advance()?;
                    let (args, distinct, order_by) =
                        if self.current_token.token_type == TokenType::RightParen {
                            (Vec::new(), false, None)
                        } else if self.current_token.token_type == TokenType::Star {
                            self.advance()?;
                            (vec![Expr::Identifier("*".to_string())], false, None)
                        } else {
                            let distinct = self.consume(TokenType::Distinct)?;
                            let args = self.parse_expression_list()?;
                            let order_by =
                                if self.consume_keywords(&[TokenType::Order, TokenType::By])? {
                                    Some(self.parse_order_by_list()?)
                                } else {
                                    None
                                };
                            (args, distinct, order_by)
                        };
                    self.expect(TokenType::RightParen)?;

                    // Parse FILTER clause
                    let filter = if self.current_token.token_type == TokenType::Identifier
                        && self.current_token.value.to_uppercase() == "FILTER"
                    {
                        self.advance()?;
                        self.expect(TokenType::LeftParen)?;
                        self.expect(TokenType::Where)?;
                        let filter_expr = self.parse_expression()?;
                        self.expect(TokenType::RightParen)?;
                        Some(Box::new(filter_expr))
                    } else {
                        None
                    };

                    // Parse OVER clause
                    let over = if self.consume(TokenType::Over)? {
                        Some(self.parse_window_spec()?)
                    } else {
                        None
                    };

                    Ok(Expr::Function {
                        name,
                        args,
                        distinct,
                        order_by,
                        filter,
                        over,
                    })
                } else {
                    Ok(Expr::Identifier(name))
                }
            }
            // Handle keywords that can be used as identifiers in expression context
            _token_type if self.current_token.token_type.can_be_identifier() => {
                let name = self.current_token.value.clone();
                self.advance()?;

                // Check if it's a function call
                if self.current_token.token_type == TokenType::LeftParen {
                    self.advance()?;
                    let (args, distinct, order_by) =
                        if self.current_token.token_type == TokenType::RightParen {
                            (Vec::new(), false, None)
                        } else if self.current_token.token_type == TokenType::Star {
                            self.advance()?;
                            (vec![Expr::Identifier("*".to_string())], false, None)
                        } else {
                            let distinct = self.consume(TokenType::Distinct)?;
                            let args = self.parse_expression_list()?;
                            let order_by =
                                if self.consume_keywords(&[TokenType::Order, TokenType::By])? {
                                    Some(self.parse_order_by_list()?)
                                } else {
                                    None
                                };
                            (args, distinct, order_by)
                        };
                    self.expect(TokenType::RightParen)?;

                    // Parse FILTER clause
                    let filter = if self.current_token.token_type == TokenType::Identifier
                        && self.current_token.value.to_uppercase() == "FILTER"
                    {
                        self.advance()?;
                        self.expect(TokenType::LeftParen)?;
                        self.expect(TokenType::Where)?;
                        let filter_expr = self.parse_expression()?;
                        self.expect(TokenType::RightParen)?;
                        Some(Box::new(filter_expr))
                    } else {
                        None
                    };

                    // Parse OVER clause
                    let over = if self.consume(TokenType::Over)? {
                        Some(self.parse_window_spec()?)
                    } else {
                        None
                    };

                    Ok(Expr::Function {
                        name,
                        args,
                        distinct,
                        order_by,
                        filter,
                        over,
                    })
                } else {
                    Ok(Expr::Identifier(name))
                }
            }
            _ => Err(Error::SyntaxError(format!(
                "Unexpected token in expression: {:?}",
                self.current_token.token_type
            ))),
        }
    }

    fn parse_case_expression(&mut self) -> Result<Expr> {
        self.expect(TokenType::Case)?;

        // Check if there's a case expression
        let expr = if self.current_token.token_type != TokenType::When {
            Some(Box::new(self.parse_expression()?))
        } else {
            None
        };

        let mut when_clauses = Vec::new();
        while self.consume(TokenType::When)? {
            let condition = self.parse_expression()?;
            self.expect(TokenType::Then)?;
            let result = self.parse_expression()?;
            when_clauses.push(WhenClause { condition, result });
        }

        let else_clause = if self.consume(TokenType::Else)? {
            Some(Box::new(self.parse_expression()?))
        } else {
            None
        };

        self.expect(TokenType::End)?;

        Ok(Expr::Case {
            expr,
            when_clauses,
            else_clause,
        })
    }

    fn parse_window_spec(&mut self) -> Result<WindowSpec> {
        self.expect(TokenType::LeftParen)?;

        let partition_by = if self.consume_keywords(&[TokenType::Partition, TokenType::By])? {
            Some(self.parse_expression_list()?)
        } else {
            None
        };

        let order_by = if self.consume_keywords(&[TokenType::Order, TokenType::By])? {
            Some(self.parse_order_by_list()?)
        } else {
            None
        };

        // Parse frame clause (ROWS/RANGE BETWEEN ... AND ...)
        let frame = if self.current_token.token_type == TokenType::Rows
            || self.current_token.token_type == TokenType::Range
            || self.current_token.token_type == TokenType::Groups {
            Some(self.parse_window_frame()?)
        } else {
            None
        };

        self.expect(TokenType::RightParen)?;

        Ok(WindowSpec {
            partition_by,
            order_by,
            frame,
        })
    }

    fn parse_window_frame(&mut self) -> Result<WindowFrame> {
        // Parse frame mode (ROWS, RANGE, or GROUPS)
        let mode = match self.current_token.token_type {
            TokenType::Rows => {
                self.advance()?;
                WindowFrameMode::Rows
            }
            TokenType::Range => {
                self.advance()?;
                WindowFrameMode::Range
            }
            TokenType::Groups => {
                self.advance()?;
                WindowFrameMode::Groups
            }
            _ => {
                return Err(Error::SyntaxError(
                    "Expected ROWS, RANGE, or GROUPS".to_string(),
                ));
            }
        };

        // Check for BETWEEN clause
        if self.consume(TokenType::Between)? {
            let start = self.parse_window_frame_bound()?;
            self.expect(TokenType::And)?;
            let end = self.parse_window_frame_bound()?;
            Ok(WindowFrame { mode, start, end: Some(end) })
        } else {
            // Single bound (start only)
            let start = self.parse_window_frame_bound()?;
            Ok(WindowFrame { mode, start, end: None })
        }
    }

    fn parse_window_frame_bound(&mut self) -> Result<WindowFrameBound> {
        match self.current_token.token_type {
            TokenType::Unbounded => {
                self.advance()?;
                if self.consume(TokenType::Preceding)? {
                    Ok(WindowFrameBound::UnboundedPreceding)
                } else if self.consume(TokenType::Following)? {
                    Ok(WindowFrameBound::UnboundedFollowing)
                } else {
                    Err(Error::SyntaxError(
                        "Expected PRECEDING or FOLLOWING after UNBOUNDED".to_string(),
                    ))
                }
            }
            TokenType::Current => {
                self.advance()?;
                self.expect(TokenType::Row)?;
                Ok(WindowFrameBound::CurrentRow)
            }
            _ => {
                // Parse expression followed by PRECEDING or FOLLOWING
                // This may include complex expressions like INTERVAL '1' DAY
                let expr = self.parse_expression()?;
                if self.consume(TokenType::Preceding)? {
                    Ok(WindowFrameBound::Preceding(Box::new(expr)))
                } else if self.consume(TokenType::Following)? {
                    Ok(WindowFrameBound::Following(Box::new(expr)))
                } else {
                    Err(Error::SyntaxError(format!(
                        "Expected PRECEDING or FOLLOWING after expression, got {:?}",
                        self.current_token.token_type
                    )))
                }
            }
        }
    }


    // Helper methods for parsing lists and common constructs

    fn parse_select_inner(&mut self) -> Result<SelectStmt> {
        match self.parse_select_statement()? {
            Stmt::Select(s) => Ok(*s),
            _ => unreachable!(),
        }
    }

    fn parse_select_columns(&mut self) -> Result<Vec<SelectColumn>> {
        let mut columns = Vec::new();

        loop {
            if self.current_token.token_type == TokenType::Star {
                self.advance()?;
                columns.push(SelectColumn {
                    expr: Expr::Identifier("*".to_string()),
                    alias: None,
                });
            } else {
                let expr = self.parse_expression()?;
                let alias = if self.consume(TokenType::As)? {
                    Some(self.parse_identifier()?)
                } else if self.current_token.token_type == TokenType::Identifier {
                    // Implicit alias
                    Some(self.parse_identifier()?)
                } else {
                    None
                };
                columns.push(SelectColumn { expr, alias });
            }

            if !self.consume(TokenType::Comma)? {
                break;
            }
        }

        Ok(columns)
    }

    fn parse_table_refs(&mut self) -> Result<Vec<TableRef>> {
        let mut tables = vec![self.parse_table_ref()?];

        // TODO: Parse JOIN clauses
        while self.consume(TokenType::Comma)? {
            tables.push(self.parse_table_ref()?);
        }

        Ok(tables)
    }

    fn parse_table_ref(&mut self) -> Result<TableRef> {
        let mut parts = vec![self.parse_identifier()?];

        while self.current_token.token_type == TokenType::Dot {
            self.advance()?;
            parts.push(self.parse_identifier()?);
        }

        let (schema, name) = if parts.len() == 2 {
            (Some(parts[0].clone()), parts[1].clone())
        } else {
            (None, parts[0].clone())
        };

        let alias = if self.consume(TokenType::As)? {
            Some(self.parse_identifier()?)
        } else if self.current_token.token_type == TokenType::Identifier {
            // Check if this looks like an alias (not a keyword)
            if !self.current_token.token_type.is_keyword() {
                Some(self.parse_identifier()?)
            } else {
                None
            }
        } else {
            None
        };

        Ok(TableRef {
            schema,
            name,
            alias,
        })
    }

    fn parse_expression_list(&mut self) -> Result<Vec<Expr>> {
        let mut exprs = vec![self.parse_expression()?];

        while self.consume(TokenType::Comma)? {
            exprs.push(self.parse_expression()?);
        }

        Ok(exprs)
    }

    fn parse_identifier_list(&mut self) -> Result<Vec<String>> {
        let mut identifiers = vec![self.parse_identifier()?];

        while self.consume(TokenType::Comma)? {
            identifiers.push(self.parse_identifier()?);
        }

        Ok(identifiers)
    }

    fn parse_order_by_list(&mut self) -> Result<Vec<OrderByExpr>> {
        let mut items = Vec::new();

        loop {
            let expr = self.parse_expression()?;
            let asc = if self.consume(TokenType::Asc)? {
                true
            } else if self.consume(TokenType::Desc)? {
                false
            } else {
                true // Default to ASC
            };

            let nulls_first = if self.current_token.token_type == TokenType::Null {
                self.advance()?;
                if self.current_token.token_type == TokenType::Identifier {
                    let nulls_pos = self.current_token.value.to_uppercase();
                    self.advance()?;
                    if nulls_pos == "FIRST" {
                        Some(true)
                    } else if nulls_pos == "LAST" {
                        Some(false)
                    } else {
                        return Err(Error::SyntaxError(
                            "Expected FIRST or LAST after NULLS".to_string(),
                        ));
                    }
                } else {
                    return Err(Error::SyntaxError(
                        "Expected FIRST or LAST after NULLS".to_string(),
                    ));
                }
            } else {
                None
            };

            items.push(OrderByExpr {
                expr,
                asc,
                nulls_first,
            });

            if !self.consume(TokenType::Comma)? {
                break;
            }
        }

        Ok(items)
    }

    fn parse_assignment_list(&mut self) -> Result<Vec<Assignment>> {
        let mut assignments = Vec::new();

        loop {
            let target = self.parse_assignment_target()?;
            self.expect(TokenType::Equal)?;
            let value = self.parse_expression()?;
            assignments.push(Assignment { target, value });

            if !self.consume(TokenType::Comma)? {
                break;
            }
        }

        Ok(assignments)
    }

    fn parse_assignment_target(&mut self) -> Result<AssignmentTarget> {
        use crate::ast::{AssignmentTarget, FieldAccessor};

        // Check for tuple assignment: (col1, col2, ...) = ...
        if self.current_token.token_type == TokenType::LeftParen {
            self.advance()?; // consume '('

            let mut columns = Vec::new();
            loop {
                columns.push(self.parse_identifier()?);
                if !self.consume(TokenType::Comma)? {
                    break;
                }
            }

            self.expect(TokenType::RightParen)?;
            return Ok(AssignmentTarget::Tuple(columns));
        }

        // Parse a base column name
        let column = self.parse_identifier()?;

        // Check for accessors (array indices and field access)
        let mut accessors = Vec::new();

        loop {
            match self.current_token.token_type {
                TokenType::LeftBracket => {
                    // Array access: column[index]
                    self.advance()?; // consume '['
                    let index = self.parse_expression()?;
                    self.expect(TokenType::RightBracket)?;
                    accessors.push(FieldAccessor::ArrayIndex(index));
                }
                TokenType::Dot => {
                    // Field access: column.field
                    self.advance()?; // consume '.'
                    let field = self.parse_identifier()?;
                    accessors.push(FieldAccessor::Field(field));
                }
                _ => break,
            }
        }

        // Return the appropriate target type based on what we found
        match accessors.len() {
            0 => Ok(AssignmentTarget::Column(column)),
            1 => match &accessors[0] {
                FieldAccessor::ArrayIndex(index) => Ok(AssignmentTarget::ArrayElement {
                    column,
                    index: index.clone(),
                }),
                FieldAccessor::Field(field) => Ok(AssignmentTarget::Field {
                    column,
                    field: field.clone(),
                }),
            },
            _ => Ok(AssignmentTarget::Complex { column, accessors }),
        }
    }

    fn parse_column_spec_list(&mut self) -> Result<Vec<ColumnSpec>> {
        use crate::ast::{ColumnSpec, FieldAccessor};

        let mut columns = Vec::new();

        loop {
            // Parse a base column name
            let column = self.parse_identifier()?;

            // Check for accessors (array indices and field access)
            let mut accessors = Vec::new();

            loop {
                match self.current_token.token_type {
                    TokenType::LeftBracket => {
                        // Array access: column[index]
                        self.advance()?; // consume '['
                        let index = self.parse_expression()?;
                        self.expect(TokenType::RightBracket)?;
                        accessors.push(FieldAccessor::ArrayIndex(index));
                    }
                    TokenType::Dot => {
                        // Field access: column.field
                        self.advance()?; // consume '.'
                        let field = self.parse_identifier()?;
                        accessors.push(FieldAccessor::Field(field));
                    }
                    _ => break,
                }
            }

            // Create the appropriate ColumnSpec based on what we found
            let column_spec = match accessors.len() {
                0 => ColumnSpec::Simple(column),
                1 => match &accessors[0] {
                    FieldAccessor::ArrayIndex(index) => ColumnSpec::ArrayElement {
                        column,
                        index: index.clone(),
                    },
                    FieldAccessor::Field(field) => ColumnSpec::Field {
                        column,
                        field: field.clone(),
                    },
                },
                _ => ColumnSpec::Complex { column, accessors },
            };

            columns.push(column_spec);

            if !self.consume(TokenType::Comma)? {
                break;
            }
        }

        Ok(columns)
    }

    fn parse_for_clause(&mut self) -> Result<ForClause> {
        match self.current_token.token_type {
            TokenType::Update => {
                self.advance()?;
                // Handle optional NOWAIT or SKIP LOCKED
                if self.consume(TokenType::Nowait)? {
                    // FOR UPDATE NOWAIT
                } else if self.consume_keywords(&[TokenType::Skip, TokenType::Locked])? {
                    // FOR UPDATE SKIP LOCKED
                }
                Ok(ForClause::Update)
            }
            TokenType::Share => {
                self.advance()?;
                // Handle optional NOWAIT or SKIP LOCKED
                if self.consume(TokenType::Nowait)? {
                    // FOR SHARE NOWAIT
                } else if self.consume_keywords(&[TokenType::Skip, TokenType::Locked])? {
                    // FOR SHARE SKIP LOCKED
                }
                Ok(ForClause::Share)
            }
            TokenType::No => {
                self.advance()?;
                self.expect(TokenType::Key)?;
                self.expect(TokenType::Update)?;
                // Handle optional NOWAIT or SKIP LOCKED
                if self.consume(TokenType::Nowait)? {
                    // FOR NO KEY UPDATE NOWAIT
                } else if self.consume_keywords(&[TokenType::Skip, TokenType::Locked])? {
                    // FOR NO KEY UPDATE SKIP LOCKED
                }
                Ok(ForClause::NoKeyUpdate)
            }
            TokenType::Key => {
                self.advance()?;
                self.expect(TokenType::Share)?;
                // Handle optional NOWAIT or SKIP LOCKED
                if self.consume(TokenType::Nowait)? {
                    // FOR KEY SHARE NOWAIT
                } else if self.consume_keywords(&[TokenType::Skip, TokenType::Locked])? {
                    // FOR KEY SHARE SKIP LOCKED
                }
                Ok(ForClause::KeyShare)
            }
            _ => Err(Error::SyntaxError(format!(
                "Expected UPDATE, SHARE, NO, or KEY after FOR, got {:?}",
                self.current_token.token_type
            ))),
        }
    }

    fn parse_on_conflict(&mut self) -> Result<OnConflict> {
        // Parse optional conflict target
        let target = if self.current_token.token_type == TokenType::LeftParen {
            self.advance()?;
            let columns = self.parse_identifier_list()?;
            self.expect(TokenType::RightParen)?;
            Some(OnConflictTarget::Columns(columns))
        } else if self.consume(TokenType::On)? {
            self.expect(TokenType::Constraint)?;
            let name = self.parse_identifier()?;
            Some(OnConflictTarget::Constraint(name))
        } else {
            None
        };

        // Parse conflict action
        self.expect(TokenType::Do)?;
        let action = if self.consume(TokenType::Nothing)? {
            OnConflictAction::DoNothing
        } else if self.consume(TokenType::Update)? {
            self.expect(TokenType::Set)?;
            let assignments = self.parse_assignment_list()?;
            OnConflictAction::DoUpdate(assignments)
        } else {
            return Err(Error::SyntaxError(
                "Expected NOTHING or UPDATE after DO".to_string(),
            ));
        };

        Ok(OnConflict { target, action })
    }

    fn parse_column_definition(&mut self) -> Result<ColumnDef> {
        let name = self.parse_identifier()?;
        let data_type = self.parse_data_type()?;
        let mut constraints = Vec::new();

        // Parse column constraints
        while self.is_column_constraint() {
            constraints.push(self.parse_column_constraint()?);
        }

        Ok(ColumnDef {
            name,
            data_type,
            constraints,
        })
    }

    fn parse_data_type(&mut self) -> Result<DataType> {
        let base_type = match self.current_token.token_type {
            TokenType::Smallint => {
                self.advance()?;
                DataType::SmallInt
            }
            TokenType::Integer | TokenType::Int => {
                self.advance()?;
                DataType::Integer
            }
            TokenType::Bigint => {
                self.advance()?;
                DataType::BigInt
            }
            TokenType::Decimal | TokenType::Numeric => {
                let is_decimal = self.current_token.token_type == TokenType::Decimal;
                self.advance()?;
                let (precision, scale) = if self.current_token.token_type == TokenType::LeftParen {
                    self.advance()?;
                    let p = self.parse_integer()? as u32;
                    let s = if self.consume(TokenType::Comma)? {
                        Some(self.parse_integer()? as u32)
                    } else {
                        None
                    };
                    self.expect(TokenType::RightParen)?;
                    (Some(p), s)
                } else {
                    (None, None)
                };
                if is_decimal {
                    DataType::Decimal(precision, scale)
                } else {
                    DataType::Numeric(precision, scale)
                }
            }
            TokenType::Real => {
                self.advance()?;
                DataType::Real
            }
            TokenType::Float => {
                self.advance()?;
                DataType::Real  // PostgreSQL FLOAT maps to REAL
            }
            TokenType::Double => {
                self.advance()?;
                self.expect(TokenType::Precision)?;
                DataType::DoublePrecision
            }
            TokenType::Smallserial => {
                self.advance()?;
                DataType::SmallSerial
            }
            TokenType::Serial => {
                self.advance()?;
                DataType::Serial
            }
            TokenType::Bigserial => {
                self.advance()?;
                DataType::BigSerial
            }
            TokenType::Char => {
                self.advance()?;
                let len = if self.current_token.token_type == TokenType::LeftParen {
                    self.advance()?;
                    let n = self.parse_integer()? as u32;
                    self.expect(TokenType::RightParen)?;
                    Some(n)
                } else {
                    None
                };
                DataType::Char(len)
            }
            TokenType::Varchar => {
                self.advance()?;
                let len = if self.current_token.token_type == TokenType::LeftParen {
                    self.advance()?;
                    let n = self.parse_integer()? as u32;
                    self.expect(TokenType::RightParen)?;
                    Some(n)
                } else {
                    None
                };
                DataType::Varchar(len)
            }
            TokenType::Text => {
                self.advance()?;
                DataType::Text
            }
            TokenType::Boolean => {
                self.advance()?;
                DataType::Boolean
            }
            TokenType::Date => {
                self.advance()?;
                DataType::Date
            }
            TokenType::Time => {
                self.advance()?;
                // TODO: Parse precision
                DataType::Time(None)
            }
            TokenType::Timestamp => {
                self.advance()?;
                // TODO: Parse precision and timezone
                DataType::Timestamp(None)
            }
            TokenType::Timestamptz => {
                self.advance()?;
                DataType::TimestampTz(None)
            }
            TokenType::Interval => {
                self.advance()?;
                DataType::Interval
            }
            TokenType::Uuid => {
                self.advance()?;
                DataType::Uuid
            }
            TokenType::Json => {
                self.advance()?;
                DataType::Json
            }
            TokenType::Jsonb => {
                self.advance()?;
                DataType::Jsonb
            }
            TokenType::Inet => {
                self.advance()?;
                DataType::Inet
            }
            TokenType::Identifier | TokenType::QuotedIdentifier => {
                let name = self.current_token.value.clone();
                self.advance()?;
                DataType::Custom(name)
            }
            _ => {
                return Err(Error::SyntaxError(format!(
                    "Expected data type, got {:?}",
                    self.current_token.token_type
                )))
            }
        };

        // Check for array type(s) - handle multidimensional arrays
        let mut current_type = base_type;
        while self.current_token.token_type == TokenType::LeftBracket {
            self.advance()?;
            self.expect(TokenType::RightBracket)?;
            current_type = DataType::Array(Box::new(current_type));
        }
        Ok(current_type)
    }

    fn parse_column_constraint(&mut self) -> Result<ColumnConstraint> {
        match self.current_token.token_type {
            TokenType::Not => {
                self.advance()?;
                self.expect(TokenType::Null)?;
                Ok(ColumnConstraint::NotNull)
            }
            TokenType::Null => {
                self.advance()?;
                Ok(ColumnConstraint::Null)
            }
            TokenType::Primary => {
                self.advance()?;
                self.expect(TokenType::Key)?;
                Ok(ColumnConstraint::PrimaryKey)
            }
            TokenType::Unique => {
                self.advance()?;
                Ok(ColumnConstraint::Unique)
            }
            TokenType::References => {
                self.advance()?;
                let table = self.parse_table_ref()?;
                let column = if self.current_token.token_type == TokenType::LeftParen {
                    self.advance()?;
                    let col = self.parse_identifier()?;
                    self.expect(TokenType::RightParen)?;
                    Some(col)
                } else {
                    None
                };
                Ok(ColumnConstraint::References(table, column))
            }
            TokenType::Check => {
                self.advance()?;
                self.expect(TokenType::LeftParen)?;
                let expr = self.parse_expression()?;
                self.expect(TokenType::RightParen)?;
                Ok(ColumnConstraint::Check(expr))
            }
            TokenType::Default => {
                self.advance()?;
                let expr = self.parse_expression()?;
                Ok(ColumnConstraint::Default(expr))
            }
            _ => Err(Error::SyntaxError(format!(
                "Unexpected column constraint: {:?}",
                self.current_token.token_type
            ))),
        }
    }

    fn parse_table_constraint(&mut self) -> Result<TableConstraint> {
        if self.consume(TokenType::Constraint)? {
            // Named constraint - skip the name for now
            let _name = self.parse_identifier()?;
        }

        match self.current_token.token_type {
            TokenType::Primary => {
                self.advance()?;
                self.expect(TokenType::Key)?;
                self.expect(TokenType::LeftParen)?;
                let columns = self.parse_identifier_list()?;
                self.expect(TokenType::RightParen)?;
                Ok(TableConstraint::PrimaryKey(columns))
            }
            TokenType::Unique => {
                self.advance()?;
                self.expect(TokenType::LeftParen)?;
                let columns = self.parse_identifier_list()?;
                self.expect(TokenType::RightParen)?;
                Ok(TableConstraint::Unique(columns))
            }
            TokenType::Foreign => {
                self.advance()?;
                self.expect(TokenType::Key)?;
                self.expect(TokenType::LeftParen)?;
                let columns = self.parse_identifier_list()?;
                self.expect(TokenType::RightParen)?;
                self.expect(TokenType::References)?;
                let references = self.parse_table_ref()?;
                self.expect(TokenType::LeftParen)?;
                let ref_columns = self.parse_identifier_list()?;
                self.expect(TokenType::RightParen)?;
                Ok(TableConstraint::ForeignKey {
                    columns,
                    references,
                    ref_columns,
                })
            }
            TokenType::Check => {
                self.advance()?;
                self.expect(TokenType::LeftParen)?;
                let expr = self.parse_expression()?;
                self.expect(TokenType::RightParen)?;
                Ok(TableConstraint::Check(expr))
            }
            _ => Err(Error::SyntaxError(format!(
                "Unexpected table constraint: {:?}",
                self.current_token.token_type
            ))),
        }
    }

    fn parse_alter_table_action(&mut self) -> Result<AlterTableAction> {
        if self.consume(TokenType::Add)? {
            if self.consume(TokenType::Column)?
                || self.current_token.token_type == TokenType::Identifier
            {
                let column = self.parse_column_definition()?;
                Ok(AlterTableAction::AddColumn(column))
            } else if self.is_constraint_keyword() {
                let constraint = self.parse_table_constraint()?;
                Ok(AlterTableAction::AddConstraint(constraint))
            } else {
                Err(Error::SyntaxError(
                    "Expected COLUMN or constraint after ADD".to_string(),
                ))
            }
        } else if self.consume(TokenType::Drop)? {
            if self.consume(TokenType::Column)? {
                let name = self.parse_identifier()?;
                Ok(AlterTableAction::DropColumn(name))
            } else if self.consume(TokenType::Constraint)? {
                let name = self.parse_identifier()?;
                Ok(AlterTableAction::DropConstraint(name))
            } else {
                Err(Error::SyntaxError(
                    "Expected COLUMN or CONSTRAINT after DROP".to_string(),
                ))
            }
        } else if self.consume(TokenType::Alter)? {
            self.consume(TokenType::Column)?;
            let column = self.parse_identifier()?;
            // TODO: Parse alter column actions
            Ok(AlterTableAction::AlterColumn(
                column,
                AlterColumnAction::DropDefault,
            ))
        } else if self.consume(TokenType::Attach)? {
            use crate::ast::PartitionBound;

            self.expect(TokenType::Partition)?;
            let partition_table = self.parse_table_ref()?;
            self.expect(TokenType::For)?;
            self.expect(TokenType::Values)?;

            let partition_bound = if self.consume(TokenType::In)? {
                // FOR VALUES IN (value1, value2, ...)
                self.expect(TokenType::LeftParen)?;
                let values = self.parse_expression_list()?;
                self.expect(TokenType::RightParen)?;
                PartitionBound::In(values)
            } else if self.consume(TokenType::From)? {
                // FOR VALUES FROM (start_values...) TO (end_values...)
                self.expect(TokenType::LeftParen)?;
                let from_values = self.parse_expression_list()?;
                self.expect(TokenType::RightParen)?;
                self.expect(TokenType::To)?;
                self.expect(TokenType::LeftParen)?;
                let to_values = self.parse_expression_list()?;
                self.expect(TokenType::RightParen)?;
                PartitionBound::Range {
                    from: from_values,
                    to: to_values,
                }
            } else {
                return Err(Error::SyntaxError("Expected IN or FROM in partition bound".to_string()));
            };

            Ok(AlterTableAction::AttachPartition {
                partition_table,
                partition_bound
            })
        } else {
            Err(Error::SyntaxError(
                "Expected ADD, DROP, ALTER, or ATTACH".to_string(),
            ))
        }
    }

    fn parse_index_columns(&mut self) -> Result<Vec<IndexColumn>> {
        let mut columns = Vec::new();

        loop {
            let expr = self.parse_expression()?;
            let asc = if self.consume(TokenType::Asc)? {
                true
            } else {
                !self.consume(TokenType::Desc)?
            };

            // TODO: Parse NULLS FIRST/LAST
            let nulls_first = None;

            columns.push(IndexColumn {
                expr,
                asc,
                nulls_first,
            });

            if !self.consume(TokenType::Comma)? {
                break;
            }
        }

        Ok(columns)
    }

    fn parse_identifier(&mut self) -> Result<String> {
        match self.current_token.token_type {
            TokenType::Identifier | TokenType::QuotedIdentifier => {
                let name = self.current_token.value.clone();
                self.advance()?;
                Ok(name)
            }
            // Allow keywords that can be used as identifiers (PostgreSQL is lenient)
            tt if tt.can_be_identifier() => {
                let name = self.current_token.value.clone();
                self.advance()?;
                Ok(name)
            }
            _ => Err(Error::SyntaxError(format!(
                "Expected identifier, got {:?}",
                self.current_token.token_type
            ))),
        }
    }

    fn parse_integer(&mut self) -> Result<i64> {
        if self.current_token.token_type != TokenType::IntegerLiteral {
            return Err(Error::SyntaxError(format!(
                "Expected integer, got {:?}",
                self.current_token.token_type
            )));
        }
        let value = self
            .current_token
            .value
            .parse::<i64>()
            .map_err(|_| Error::InvalidNumber {
                value: self.current_token.value.clone(),
            })?;
        self.advance()?;
        Ok(value)
    }

    /// Parse a type name string for TypeCast (::) syntax
    /// This handles simple types like "integer" and array types like "int[]"
    fn parse_type_name_string(&mut self) -> Result<String> {
        let mut type_name = self.parse_identifier()?;

        // Handle array syntax
        while self.current_token.token_type == TokenType::LeftBracket {
            self.advance()?; // consume '['
            type_name.push_str("[]");
            self.expect(TokenType::RightBracket)?; // consume ']'
        }

        Ok(type_name)
    }

    // Utility methods

    fn advance(&mut self) -> Result<()> {
        self.current_token = self.lexer.next_token()?;
        Ok(())
    }

    fn expect(&mut self, token_type: TokenType) -> Result<()> {
        if self.current_token.token_type != token_type {
            return Err(Error::SyntaxError(format!(
                "Expected {:?}, got {:?}",
                token_type, self.current_token.token_type
            )));
        }
        self.advance()
    }

    fn consume(&mut self, token_type: TokenType) -> Result<bool> {
        if self.current_token.token_type == token_type {
            self.advance()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn consume_keywords(&mut self, keywords: &[TokenType]) -> Result<bool> {
        let saved_token = self.current_token.clone();
        let saved_pos = self.lexer.position;

        for &keyword in keywords {
            if self.current_token.token_type != keyword {
                // Restore position
                self.current_token = saved_token;
                self.lexer.position = saved_pos;
                return Ok(false);
            }
            self.advance()?;
        }

        Ok(true)
    }

    fn is_eof(&self) -> bool {
        self.current_token.token_type == TokenType::Eof
    }

    fn is_constraint_keyword(&self) -> bool {
        matches!(
            self.current_token.token_type,
            TokenType::Constraint
                | TokenType::Primary
                | TokenType::Unique
                | TokenType::Foreign
                | TokenType::Check
        )
    }

    fn is_column_constraint(&self) -> bool {
        matches!(
            self.current_token.token_type,
            TokenType::Not
                | TokenType::Null
                | TokenType::Primary
                | TokenType::Unique
                | TokenType::References
                | TokenType::Check
                | TokenType::Default
        )
    }

    // Administrative command parsers

    fn parse_explain_statement(&mut self) -> Result<Stmt> {
        self.expect(TokenType::Explain)?;

        let mut analyze = false;
        let mut verbose = false;
        let mut costs = None;
        let mut buffers = false;

        // Parse options
        if self.consume(TokenType::LeftParen)? {
            loop {
                match self.current_token.token_type {
                    TokenType::Analyze => {
                        self.advance()?;
                        analyze = true;
                    }
                    TokenType::Verbose => {
                        self.advance()?;
                        verbose = true;
                    }
                    TokenType::Costs => {
                        self.advance()?;
                        if self.consume(TokenType::On)? {
                            costs = Some(true);
                        } else if self.consume_keyword("OFF")? {
                            costs = Some(false);
                        }
                    }
                    TokenType::Buffers => {
                        self.advance()?;
                        buffers = true;
                    }
                    _ => break,
                }

                if !self.consume(TokenType::Comma)? {
                    break;
                }
            }
            self.expect(TokenType::RightParen)?;
        } else {
            // Handle simple EXPLAIN ANALYZE
            if self.current_token.token_type == TokenType::Analyze {
                self.advance()?;
                analyze = true;
            }
        }

        let statement = Box::new(self.parse_statement()?);

        Ok(Stmt::Explain(ExplainStmt {
            analyze,
            verbose,
            costs,
            buffers,
            statement,
        }))
    }

    fn parse_copy_statement(&mut self) -> Result<Stmt> {
        self.expect(TokenType::Copy)?;

        // Parse source
        let source = if self.consume(TokenType::LeftParen)? {
            let query = self.parse_select_statement_inner()?;
            self.expect(TokenType::RightParen)?;
            CopySource::Query(Box::new(query))
        } else {
            let table = self.parse_table_ref()?;
            let columns = if self.consume(TokenType::LeftParen)? {
                let cols = self.parse_identifier_list()?;
                self.expect(TokenType::RightParen)?;
                Some(cols)
            } else {
                None
            };
            CopySource::Table { table, columns }
        };

        // Parse direction
        let direction = if self.consume(TokenType::From)? {
            CopyDirection::From
        } else {
            self.expect(TokenType::To)?;
            CopyDirection::To
        };

        // Parse target
        let target = match self.current_token.token_type {
            TokenType::Stdin => {
                self.advance()?;
                CopyTarget::Stdin
            }
            TokenType::Stdout => {
                self.advance()?;
                CopyTarget::Stdout
            }
            TokenType::String => {
                let path = self.current_token.value.clone();
                self.advance()?;
                CopyTarget::File(path)
            }
            _ => return Err(Error::SyntaxError("Expected file path, STDIN, or STDOUT".to_string())),
        };

        // Parse options
        let mut options = Vec::new();
        if self.consume_keyword("WITH")? {
            loop {
                match self.current_token.token_type {
                    TokenType::Delimiter => {
                        self.advance()?;
                        if let TokenType::String = self.current_token.token_type {
                            options.push(CopyOption::Delimiter(self.current_token.value.clone()));
                            self.advance()?;
                        }
                    }
                    TokenType::Csv => {
                        self.advance()?;
                        options.push(CopyOption::Csv);
                    }
                    TokenType::Header => {
                        self.advance()?;
                        options.push(CopyOption::Header);
                    }
                    _ => break,
                }
            }
        }

        Ok(Stmt::Copy(CopyStmt {
            source,
            direction,
            target,
            options,
        }))
    }

    fn parse_set_statement(&mut self) -> Result<Stmt> {
        self.expect(TokenType::Set)?;

        let parameter = self.parse_identifier()?;

        // Handle different SET syntaxes
        let value = if self.consume(TokenType::Equal)? || self.consume(TokenType::To)? {
            // Standard SET param = value or SET param TO value
            self.parse_set_value()?
        } else if self.current_token.token_type == TokenType::String
            || self.current_token.token_type == TokenType::FloatLiteral
            || self.current_token.token_type == TokenType::IntegerLiteral
            || self.current_token.token_type == TokenType::Identifier
            || self.current_token.token_type == TokenType::Default
        {
            // SET param value (no = or TO)
            self.parse_set_value()?
        } else {
            // Special case for some SET statements like "SET ROLE admin" which don't have explicit values
            SetValue::Default
        };

        Ok(Stmt::Set(SetStmt { parameter, value }))
    }

    fn parse_set_value(&mut self) -> Result<SetValue> {
        if self.consume(TokenType::Default)? {
            Ok(SetValue::Default)
        } else if self.current_token.token_type == TokenType::String {
            let val = self.current_token.value.clone();
            self.advance()?;
            Ok(SetValue::String(val))
        } else if self.current_token.token_type == TokenType::FloatLiteral {
            let val = self.current_token.value.parse().unwrap_or(0.0);
            self.advance()?;
            Ok(SetValue::Number(val))
        } else if self.current_token.token_type == TokenType::IntegerLiteral {
            let val = self.current_token.value.parse::<i64>().unwrap_or(0) as f64;
            self.advance()?;
            Ok(SetValue::Number(val))
        } else {
            let val = self.parse_identifier()?;
            Ok(SetValue::Identifier(val))
        }
    }

    fn parse_show_statement(&mut self) -> Result<Stmt> {
        self.expect(TokenType::Show)?;

        let parameter = if self.consume(TokenType::All)? {
            ShowParameter::All
        } else {
            let name = self.parse_identifier()?;
            ShowParameter::Specific(name)
        };

        Ok(Stmt::Show(ShowStmt { parameter }))
    }

    fn parse_reset_statement(&mut self) -> Result<Stmt> {
        self.expect(TokenType::Reset)?;

        let parameter = if self.consume(TokenType::All)? {
            None
        } else {
            Some(self.parse_identifier()?)
        };

        Ok(Stmt::Reset(ResetStmt { parameter }))
    }

    fn parse_analyze_statement(&mut self) -> Result<Stmt> {
        self.expect(TokenType::Analyze)?;

        let verbose = self.consume(TokenType::Verbose)?;

        let tables = if self.is_eof() || self.current_token.token_type == TokenType::Semicolon {
            None
        } else {
            let mut tables = Vec::new();
            loop {
                let table = self.parse_table_ref()?;
                let columns = if self.consume(TokenType::LeftParen)? {
                    let cols = self.parse_identifier_list()?;
                    self.expect(TokenType::RightParen)?;
                    Some(cols)
                } else {
                    None
                };

                tables.push(AnalyzeTable { table, columns });

                if !self.consume(TokenType::Comma)? {
                    break;
                }
            }
            Some(tables)
        };

        Ok(Stmt::Analyze(AnalyzeStmt { verbose, tables }))
    }

    fn parse_vacuum_statement(&mut self) -> Result<Stmt> {
        self.expect(TokenType::Vacuum)?;

        let mut full = false;
        let mut freeze = false;
        let mut verbose = false;
        let mut analyze = false;

        // Parse options
        loop {
            match self.current_token.token_type {
                TokenType::Full => {
                    self.advance()?;
                    full = true;
                }
                TokenType::Freeze => {
                    self.advance()?;
                    freeze = true;
                }
                TokenType::Verbose => {
                    self.advance()?;
                    verbose = true;
                }
                TokenType::Analyze => {
                    self.advance()?;
                    analyze = true;
                }
                _ => break,
            }
        }

        let tables = if self.is_eof() || self.current_token.token_type == TokenType::Semicolon {
            None
        } else {
            let mut tables = Vec::new();
            loop {
                tables.push(self.parse_table_ref()?);
                if !self.consume(TokenType::Comma)? {
                    break;
                }
            }
            Some(tables)
        };

        Ok(Stmt::Vacuum(VacuumStmt {
            full,
            freeze,
            verbose,
            analyze,
            tables,
        }))
    }

    fn parse_discard_statement(&mut self) -> Result<Stmt> {
        self.expect(TokenType::Discard)?;

        let target = match self.current_token.token_type {
            TokenType::All => {
                self.advance()?;
                DiscardTarget::All
            }
            TokenType::Plans => {
                self.advance()?;
                DiscardTarget::Plans
            }
            TokenType::Sequences => {
                self.advance()?;
                DiscardTarget::Sequences
            }
            TokenType::Temp | TokenType::Temporary => {
                self.advance()?;
                DiscardTarget::Temp
            }
            _ => return Err(Error::SyntaxError("Expected ALL, PLANS, SEQUENCES, or TEMP/TEMPORARY".to_string())),
        };

        Ok(Stmt::Discard(DiscardStmt { target }))
    }

    fn parse_deallocate_statement(&mut self) -> Result<Stmt> {
        self.expect(TokenType::Deallocate)?;
        self.consume(TokenType::Prepare)?; // Optional PREPARE keyword

        let name = if self.consume(TokenType::All)? {
            None
        } else {
            Some(self.parse_identifier()?)
        };

        Ok(Stmt::Deallocate(DeallocateStmt { name }))
    }

    // Session/Transaction parsers (simplified implementations for now)

    fn parse_savepoint_statement(&mut self) -> Result<Stmt> {
        self.expect(TokenType::Savepoint)?;
        let name = self.parse_identifier()?;
        Ok(Stmt::Savepoint(SavepointStmt { name }))
    }

    fn parse_release_statement(&mut self) -> Result<Stmt> {
        self.expect(TokenType::Release)?;
        self.consume(TokenType::Savepoint)?; // Optional SAVEPOINT keyword
        let name = self.parse_identifier()?;
        Ok(Stmt::ReleaseSavepoint(ReleaseSavepointStmt { name }))
    }

    fn parse_prepare_statement(&mut self) -> Result<Stmt> {
        self.expect(TokenType::Prepare)?;
        let name = self.parse_identifier()?;

        let parameter_types = if self.consume(TokenType::LeftParen)? {
            let mut types = Vec::new();
            if self.current_token.token_type != TokenType::RightParen {
                loop {
                    types.push(self.parse_data_type()?);
                    if !self.consume(TokenType::Comma)? {
                        break;
                    }
                }
            }
            self.expect(TokenType::RightParen)?;
            Some(types)
        } else {
            None
        };

        self.expect(TokenType::As)?;
        let statement = Box::new(self.parse_statement()?);

        Ok(Stmt::Prepare(PrepareStmt {
            name,
            parameter_types,
            statement,
        }))
    }

    fn parse_listen_statement(&mut self) -> Result<Stmt> {
        self.expect(TokenType::Listen)?;
        let channel = self.parse_identifier()?;
        Ok(Stmt::Listen(ListenStmt { channel }))
    }

    fn parse_notify_statement(&mut self) -> Result<Stmt> {
        self.expect(TokenType::Notify)?;
        let channel = self.parse_identifier()?;
        let payload = if self.consume(TokenType::Comma)? {
            if self.current_token.token_type == TokenType::String {
                let p = self.current_token.value.clone();
                self.advance()?;
                Some(p)
            } else {
                None
            }
        } else {
            None
        };
        Ok(Stmt::Notify(NotifyStmt { channel, payload }))
    }

    fn parse_lock_statement(&mut self) -> Result<Stmt> {
        self.expect(TokenType::Lock)?;
        self.expect(TokenType::Table)?;

        let mut tables = Vec::new();
        loop {
            tables.push(self.parse_table_ref()?);
            if !self.consume(TokenType::Comma)? {
                break;
            }
        }

        self.consume(TokenType::In)?; // Optional IN
        let mode = self.parse_lock_mode()?;
        let nowait = self.consume(TokenType::Nowait)?;

        Ok(Stmt::Lock(LockStmt {
            tables,
            mode,
            nowait,
        }))
    }

    fn parse_lock_mode(&mut self) -> Result<LockMode> {
        // Simplified - just parse a few common modes
        match self.current_token.token_type {
            TokenType::Share => {
                self.advance()?;
                Ok(LockMode::Share)
            }
            _ => Ok(LockMode::AccessShare), // Default
        }
    }

    fn parse_grant_statement(&mut self) -> Result<Stmt> {
        self.expect(TokenType::Grant)?;
        // Simplified GRANT parsing
        let privileges = vec![Privilege::All]; // Simplified
        let objects = vec![self.parse_identifier()?]; // Simplified
        self.expect(TokenType::To)?;
        let grantees = vec![self.parse_identifier()?]; // Simplified
        let with_grant_option = false; // Simplified

        Ok(Stmt::Grant(GrantStmt {
            privileges,
            objects,
            grantees,
            with_grant_option,
        }))
    }

    fn parse_revoke_statement(&mut self) -> Result<Stmt> {
        self.expect(TokenType::Revoke)?;
        // Simplified REVOKE parsing
        let privileges = vec![Privilege::All]; // Simplified
        let objects = vec![self.parse_identifier()?]; // Simplified
        self.expect(TokenType::From)?;
        let grantees = vec![self.parse_identifier()?]; // Simplified
        let cascade = false; // Simplified

        Ok(Stmt::Revoke(RevokeStmt {
            privileges,
            objects,
            grantees,
            cascade,
        }))
    }

    fn parse_truncate_statement(&mut self) -> Result<Stmt> {
        self.expect(TokenType::Truncate)?;
        self.consume(TokenType::Table)?; // Optional TABLE keyword

        let mut tables = Vec::new();
        loop {
            tables.push(self.parse_table_ref()?);
            if !self.consume(TokenType::Comma)? {
                break;
            }
        }

        let restart_identity = self.consume_keyword("RESTART")? && self.consume_keyword("IDENTITY")?;
        let cascade = self.consume(TokenType::Cascade)?;

        Ok(Stmt::Truncate(TruncateStmt {
            tables,
            restart_identity,
            cascade,
        }))
    }

    // Helper methods

    fn consume_keyword(&mut self, keyword: &str) -> Result<bool> {
        if self.current_token.token_type == TokenType::Identifier
            && self.current_token.value.to_uppercase() == keyword
        {
            self.advance()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }


    // We need to expose parse_select_statement_inner for COPY (SELECT ...) parsing
    fn parse_select_statement_inner(&mut self) -> Result<SelectStmt> {
        if let Stmt::Select(select_stmt) = self.parse_select_statement()? {
            Ok(*select_stmt)
        } else {
            Err(Error::SyntaxError("Expected SELECT statement".to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_select() {
        let sql = b"SELECT * FROM users";
        let mut parser = Parser::new(sql);
        let stmt = parser.parse_statement().unwrap();

        match stmt {
            Stmt::Select(select) => {
                assert_eq!(select.columns.len(), 1);
                assert!(select.from.is_some());
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_dollar_parameter() {
        let sql = b"SELECT * FROM users WHERE id = $1";
        let mut parser = Parser::new(sql);
        let stmt = parser.parse_statement().unwrap();

        match stmt {
            Stmt::Select(select) => {
                assert!(select.where_clause.is_some());
                match &select.where_clause.unwrap() {
                    Expr::BinaryOp { right, .. } => {
                        assert!(matches!(right.as_ref(), Expr::DollarParameter(1)));
                    }
                    _ => panic!("Expected binary operation"),
                }
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_type_cast() {
        let sql = b"SELECT '123'::integer";
        let mut parser = Parser::new(sql);
        let stmt = parser.parse_statement().unwrap();

        match stmt {
            Stmt::Select(select) => {
                assert_eq!(select.columns.len(), 1);
                match &select.columns[0].expr {
                    Expr::TypeCast { expr, type_name } => {
                        assert!(matches!(expr.as_ref(), Expr::String(_)));
                        assert_eq!(type_name, "integer");
                    }
                    _ => panic!("Expected type cast"),
                }
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_insert_with_returning() {
        let sql =
            b"INSERT INTO users (name, email) VALUES ('John', 'john@example.com') RETURNING id";
        let mut parser = Parser::new(sql);
        let stmt = parser.parse_statement().unwrap();

        match stmt {
            Stmt::Insert(insert) => {
                assert!(insert.columns.is_some());
                assert!(insert.returning.is_some());
                assert_eq!(insert.returning.unwrap().len(), 1);
            }
            _ => panic!("Expected INSERT statement"),
        }
    }

    #[test]
    fn test_parse_on_conflict() {
        let sql =
            b"INSERT INTO users (email) VALUES ('test@example.com') ON CONFLICT (email) DO NOTHING";
        let mut parser = Parser::new(sql);
        let stmt = parser.parse_statement().unwrap();

        match stmt {
            Stmt::Insert(insert) => {
                assert!(insert.on_conflict.is_some());
                let conflict = insert.on_conflict.unwrap();
                assert!(matches!(conflict.target, Some(OnConflictTarget::Columns(_))));
                assert!(matches!(conflict.action, OnConflictAction::DoNothing));
            }
            _ => panic!("Expected INSERT statement"),
        }
    }

    #[test]
    fn test_parse_array() {
        let sql = b"SELECT ARRAY[1, 2, 3]";
        let mut parser = Parser::new(sql);
        let stmt = parser.parse_statement().unwrap();

        match stmt {
            Stmt::Select(select) => {
                assert_eq!(select.columns.len(), 1);
                assert!(matches!(&select.columns[0].expr, Expr::Array(_)));
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_json_operators() {
        let sql = b"SELECT data->'field' FROM json_table";
        let mut parser = Parser::new(sql);
        let stmt = parser.parse_statement().unwrap();

        match stmt {
            Stmt::Select(select) => {
                assert_eq!(select.columns.len(), 1);
                match &select.columns[0].expr {
                    Expr::JsonAccess { op, .. } => {
                        assert_eq!(*op, JsonOperator::Arrow);
                    }
                    _ => panic!("Expected JSON access"),
                }
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_distinct_on() {
        let sql = b"SELECT DISTINCT ON (department) name, department FROM employees";
        let mut parser = Parser::new(sql);
        let stmt = parser.parse_statement().unwrap();

        match stmt {
            Stmt::Select(select) => {
                assert!(matches!(&select.distinct, Some(Distinct::On(_))));
            }
            _ => panic!("Expected SELECT statement"),
        }
    }
}
