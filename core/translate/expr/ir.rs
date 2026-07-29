//! Declarative value IR for expression translation.
//!
//! Instead of eagerly emitting bytecode while walking the AST, supported
//! expression shapes are first decomposed into a tree of [VExpr] nodes stored
//! in an arena. Building the tree is pure — no registers are allocated and no
//! instructions are emitted — so nodes can be freely constructed, chained and
//! inspected before anything is committed to the program. A separate lowering
//! pass then walks the tree and emits instructions.
//!
//! The lowering pass is deterministic and mirrors the eager path exactly: for
//! the supported subset it produces bytecode that is instruction-for-instruction
//! identical to `translate_expr`, which is validated by the equivalence tests
//! below. Unsupported subexpressions become [VExpr::Opaque] leaves that lower
//! by delegating to the eager path, so coverage can grow node kind by node kind
//! (strangler pattern) without ever changing emitted programs.
//!
//! This is the first rung of moving expression translation from "eager SQL
//! compiler" to a conventional build-IR-then-lower compiler pipeline. Once the
//! traversal is IR-driven, passes like common-subexpression elimination,
//! constant hoisting as scheduling, and liveness-based register allocation can
//! be implemented on the arena instead of on the emitted instruction stream.

use super::*;

use std::sync::atomic::{AtomicBool, Ordering};

/// Runtime toggle so differential tests can force the eager path and compare
/// the two paths' bytecode. Enabled by default.
static EXPR_IR_ENABLED: AtomicBool = AtomicBool::new(true);

#[cfg(test)]
pub(crate) fn set_expr_ir_enabled(enabled: bool) {
    EXPR_IR_ENABLED.store(enabled, Ordering::Relaxed);
}

fn expr_ir_enabled() -> bool {
    EXPR_IR_ENABLED.load(Ordering::Relaxed)
}

/// Cheap pre-check inlined into the hot [translate_expr] path: only composite
/// roots engage the IR, so leaf-heavy workloads (e.g. parameter-only VALUES
/// lists) pay a single discriminant test and nothing else.
#[inline]
pub(super) fn expr_ir_applicable(expr: &ast::Expr) -> bool {
    matches!(
        expr,
        ast::Expr::Binary(..)
            | ast::Expr::Case { .. }
            | ast::Expr::IsNull(_)
            | ast::Expr::NotNull(_)
            | ast::Expr::Unary(..)
            | ast::Expr::Parenthesized(_)
    ) && expr_ir_enabled()
}

/// Index of a node in [ExprIr::nodes].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) struct VId(u32);

/// A value-producing expression node. Children are referenced by [VId], so a
/// node describes *what* to compute without committing to registers or
/// instruction order.
enum VExpr<'a> {
    /// A literal value; leaf node.
    Literal(&'a ast::Literal),
    /// Unary `+`: evaluates its child directly into the target register.
    Passthrough(VId),
    /// A parenthesized single expression.
    Paren(VId),
    /// General unary operator application. Constant-folded shapes
    /// (e.g. `-1`, `~1`) stay on their bespoke eager emission as Opaque.
    Unary(ast::UnaryOperator, VId),
    /// Scalar binary operator. `shared` means both operands are equivalent
    /// expressions and evaluate once into a single shared register.
    Binary {
        lhs: VId,
        rhs: VId,
        op: ast::Operator,
        shared: bool,
    },
    IsNull(VId),
    NotNull(VId),
    Case {
        base: Option<VId>,
        when_then: Vec<(VId, VId)>,
        else_expr: Option<VId>,
    },
    /// Unsupported shape: lowered by delegating to the eager `translate_expr`.
    Opaque,
}

struct VNode<'a> {
    kind: VExpr<'a>,
    /// Back-pointer to the AST node, used for the per-node translation preamble
    /// (constant classification, expression-register cache, expression indexes),
    /// for emission helpers that inspect operand shapes (affinity, collation),
    /// and for delegating Opaque nodes to the eager path.
    ast: &'a ast::Expr,
}

/// An expression decomposed into value nodes, ready to be lowered into a
/// program. `nodes[root]` is the root of the tree.
pub(super) struct ExprIr<'a> {
    nodes: Vec<VNode<'a>>,
    root: VId,
}

impl<'a> ExprIr<'a> {
    /// Decompose `expr` into the value IR. The caller must have checked
    /// [expr_ir_applicable] first. Returns `None` when the root decomposes to
    /// an unsupported shape (the caller then uses the eager path, which is
    /// equivalent); unsupported *sub*expressions become [VExpr::Opaque] leaves
    /// lowered via the eager path.
    pub(super) fn build(
        expr: &'a ast::Expr,
        referenced_tables: Option<&TableReferences>,
        resolver: &Resolver,
    ) -> Result<Option<ExprIr<'a>>> {
        let mut ir = ExprIr {
            nodes: Vec::with_capacity(8),
            root: VId(0),
        };
        let root = ir.build_node(expr, referenced_tables, resolver)?;
        if matches!(ir.node(root).kind, VExpr::Opaque) {
            return Ok(None);
        }
        ir.root = root;
        #[cfg(test)]
        tests::EXPR_IR_BUILT.fetch_add(1, Ordering::Relaxed);
        Ok(Some(ir))
    }

    fn node(&self, id: VId) -> &VNode<'a> {
        &self.nodes[id.0 as usize]
    }

    fn push(&mut self, kind: VExpr<'a>, ast: &'a ast::Expr) -> VId {
        let id = VId(u32::try_from(self.nodes.len()).expect("expression IR node count overflow"));
        self.nodes.push(VNode { kind, ast });
        id
    }

    fn build_node(
        &mut self,
        expr: &'a ast::Expr,
        referenced_tables: Option<&TableReferences>,
        resolver: &Resolver,
    ) -> Result<VId> {
        let kind = match expr {
            ast::Expr::Literal(lit) => VExpr::Literal(lit),
            ast::Expr::Parenthesized(exprs) if exprs.len() == 1 => {
                VExpr::Paren(self.build_node(&exprs[0], referenced_tables, resolver)?)
            }
            ast::Expr::IsNull(e) => {
                VExpr::IsNull(self.build_node(e, referenced_tables, resolver)?)
            }
            ast::Expr::NotNull(e) => {
                VExpr::NotNull(self.build_node(e, referenced_tables, resolver)?)
            }
            ast::Expr::Unary(op, e) => match (op, e.as_ref()) {
                (UnaryOperator::Positive, _) => {
                    VExpr::Passthrough(self.build_node(e, referenced_tables, resolver)?)
                }
                // Constant-folded shapes keep their bespoke eager emission.
                (UnaryOperator::Negative, ast::Expr::Literal(ast::Literal::Numeric(_)))
                | (UnaryOperator::BitwiseNot, ast::Expr::Literal(ast::Literal::Numeric(_)))
                | (UnaryOperator::BitwiseNot, ast::Expr::Literal(ast::Literal::Null)) => {
                    VExpr::Opaque
                }
                _ => VExpr::Unary(*op, self.build_node(e, referenced_tables, resolver)?),
            },
            ast::Expr::Binary(e1, op, e2) => {
                self.build_binary(e1, *op, e2, referenced_tables, resolver)?
            }
            ast::Expr::Case {
                base,
                when_then_pairs,
                else_expr,
            } => {
                let base = base
                    .as_ref()
                    .map(|b| self.build_node(b, referenced_tables, resolver))
                    .transpose()?;
                let when_then = when_then_pairs
                    .iter()
                    .map(|(when_expr, then_expr)| {
                        Ok((
                            self.build_node(when_expr, referenced_tables, resolver)?,
                            self.build_node(then_expr, referenced_tables, resolver)?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let else_expr = else_expr
                    .as_ref()
                    .map(|e| self.build_node(e, referenced_tables, resolver))
                    .transpose()?;
                VExpr::Case {
                    base,
                    when_then,
                    else_expr,
                }
            }
            _ => VExpr::Opaque,
        };
        Ok(self.push(kind, expr))
    }

    fn build_binary(
        &mut self,
        e1: &'a ast::Expr,
        op: ast::Operator,
        e2: &'a ast::Expr,
        referenced_tables: Option<&TableReferences>,
        resolver: &Resolver,
    ) -> Result<VExpr<'a>> {
        // IS TRUE / IS FALSE / IS NOT TRUE / IS NOT FALSE use truth-semantics
        // emission; custom type operators dispatch to user functions. Both keep
        // their bespoke eager emission.
        if matches!(
            (op, e2),
            (
                ast::Operator::Is | ast::Operator::IsNot,
                ast::Expr::Literal(ast::Literal::True | ast::Literal::False)
            )
        ) {
            return Ok(VExpr::Opaque);
        }
        if find_custom_type_operator(e1, e2, &op, referenced_tables, resolver).is_some() {
            return Ok(VExpr::Opaque);
        }
        // Row-valued comparisons keep the eager path.
        if expr_vector_size(e1)? != 1 || expr_vector_size(e2)? != 1 {
            return Ok(VExpr::Opaque);
        }
        if exprs_are_equivalent(e1, e2) {
            let child = self.build_node(e1, referenced_tables, resolver)?;
            return Ok(VExpr::Binary {
                lhs: child,
                rhs: child,
                op,
                shared: true,
            });
        }
        let lhs = self.build_node(e1, referenced_tables, resolver)?;
        let rhs = self.build_node(e2, referenced_tables, resolver)?;
        Ok(VExpr::Binary {
            lhs,
            rhs,
            op,
            shared: false,
        })
    }

    /// Lower the root node. The caller ([translate_expr]) has already run the
    /// translation preamble (constant span, cached register, expression index)
    /// for the root expression, so only the node body is emitted here.
    pub(super) fn lower_root(
        &self,
        program: &mut ProgramBuilder,
        referenced_tables: Option<&TableReferences>,
        resolver: &Resolver,
        target_register: usize,
    ) -> Result<()> {
        self.lower_kind(
            program,
            referenced_tables,
            resolver,
            self.root,
            target_register,
        )
    }

    /// Lower a non-root node, mirroring a `translate_expr` call on its AST:
    /// run the translation preamble, then emit the node body.
    fn lower_node(
        &self,
        program: &mut ProgramBuilder,
        referenced_tables: Option<&TableReferences>,
        resolver: &Resolver,
        id: VId,
        target_register: usize,
    ) -> Result<()> {
        let node = self.node(id);
        if matches!(node.kind, VExpr::Opaque) {
            translate_expr(
                program,
                referenced_tables,
                node.ast,
                target_register,
                resolver,
            )?;
            return Ok(());
        }
        let constant_span = open_expr_constant_span(program, node.ast, resolver);
        if try_emit_cached_expr_reg(
            program,
            referenced_tables,
            node.ast,
            target_register,
            resolver,
        )? || try_emit_expression_index_lookup(
            program,
            referenced_tables,
            node.ast,
            target_register,
        )? {
            if let Some(span) = constant_span {
                program.constant_span_end(span);
            }
            return Ok(());
        }
        self.lower_kind(program, referenced_tables, resolver, id, target_register)?;
        if let Some(span) = constant_span {
            program.constant_span_end(span);
        }
        Ok(())
    }

    /// Lower a node like [translate_expr_no_constant_opt] would translate it:
    /// any constant spans opened while emitting it are invalidated so its
    /// registers are never hoisted.
    fn lower_node_no_constant_opt(
        &self,
        program: &mut ProgramBuilder,
        referenced_tables: Option<&TableReferences>,
        resolver: &Resolver,
        id: VId,
        target_register: usize,
    ) -> Result<()> {
        let next_span_idx = program.constant_spans_next_idx();
        self.lower_node(program, referenced_tables, resolver, id, target_register)?;
        program.constant_spans_invalidate_after(next_span_idx);
        Ok(())
    }

    fn lower_kind(
        &self,
        program: &mut ProgramBuilder,
        referenced_tables: Option<&TableReferences>,
        resolver: &Resolver,
        id: VId,
        target_register: usize,
    ) -> Result<()> {
        let node = self.node(id);
        match &node.kind {
            VExpr::Opaque => {
                unreachable!("Opaque nodes are lowered via translate_expr in lower_node")
            }
            VExpr::Literal(lit) => {
                emit_literal(program, lit, target_register)?;
            }
            VExpr::Passthrough(child) => {
                self.lower_node(
                    program,
                    referenced_tables,
                    resolver,
                    *child,
                    target_register,
                )?;
            }
            VExpr::Paren(child) => {
                assert_register_range_allocated(program, target_register, 1)?;
                self.lower_node(
                    program,
                    referenced_tables,
                    resolver,
                    *child,
                    target_register,
                )?;
            }
            VExpr::Unary(op, child) => match op {
                UnaryOperator::Positive => {
                    unreachable!("unary + is decomposed into Passthrough")
                }
                UnaryOperator::Negative => {
                    let value = 0;
                    let reg = program.alloc_register();
                    self.lower_node(program, referenced_tables, resolver, *child, reg)?;
                    let zero_reg = program.alloc_register();
                    program.emit_insn(Insn::Integer {
                        value,
                        dest: zero_reg,
                    });
                    program.mark_last_insn_constant();
                    program.emit_insn(Insn::Subtract {
                        lhs: zero_reg,
                        rhs: reg,
                        dest: target_register,
                    });
                }
                UnaryOperator::BitwiseNot => {
                    let reg = program.alloc_register();
                    self.lower_node(program, referenced_tables, resolver, *child, reg)?;
                    program.emit_insn(Insn::BitNot {
                        reg,
                        dest: target_register,
                    });
                }
                UnaryOperator::Not => {
                    let reg = program.alloc_register();
                    self.lower_node(program, referenced_tables, resolver, *child, reg)?;
                    program.emit_insn(Insn::Not {
                        reg,
                        dest: target_register,
                    });
                }
            },
            VExpr::IsNull(child) => {
                let reg = program.alloc_register();
                self.lower_node(program, referenced_tables, resolver, *child, reg)?;
                program.emit_insn(Insn::Integer {
                    value: 1,
                    dest: target_register,
                });
                let label = program.allocate_label();
                program.emit_insn(Insn::IsNull {
                    reg,
                    target_pc: label,
                });
                program.emit_insn(Insn::Integer {
                    value: 0,
                    dest: target_register,
                });
                program.preassign_label_to_next_insn(label);
            }
            VExpr::NotNull(child) => {
                let reg = program.alloc_register();
                self.lower_node(program, referenced_tables, resolver, *child, reg)?;
                program.emit_insn(Insn::Integer {
                    value: 1,
                    dest: target_register,
                });
                let label = program.allocate_label();
                program.emit_insn(Insn::NotNull {
                    reg,
                    target_pc: label,
                });
                program.emit_insn(Insn::Integer {
                    value: 0,
                    dest: target_register,
                });
                program.preassign_label_to_next_insn(label);
            }
            VExpr::Binary { .. } => {
                self.lower_binary(program, referenced_tables, resolver, id, target_register)?;
            }
            VExpr::Case { .. } => {
                self.lower_case(program, referenced_tables, resolver, id, target_register)?;
            }
        }
        Ok(())
    }

    /// Mirrors [emit_binary_expr_scalar] with value emission mode.
    fn lower_binary(
        &self,
        program: &mut ProgramBuilder,
        referenced_tables: Option<&TableReferences>,
        resolver: &Resolver,
        id: VId,
        target_register: usize,
    ) -> Result<()> {
        let node = self.node(id);
        let VExpr::Binary {
            lhs,
            rhs,
            op,
            shared,
        } = node.kind
        else {
            unreachable!("lower_binary requires a Binary node")
        };
        // The emission helpers inspect the operand ASTs for affinity and
        // collation; recover them from the node's own AST back-pointer.
        let ast::Expr::Binary(e1, _, e2) = node.ast else {
            unreachable!("Binary node must point at a Binary AST expression")
        };
        if shared {
            let shared_reg = program.alloc_register();
            self.lower_node(program, referenced_tables, resolver, lhs, shared_reg)?;
            emit_binary_insn(
                program,
                &op,
                shared_reg,
                shared_reg,
                target_register,
                e1,
                e2,
                referenced_tables,
                Some(resolver),
            )?;
        } else {
            let e1_reg = program.alloc_registers(2);
            let e2_reg = e1_reg + 1;

            self.lower_node(program, referenced_tables, resolver, lhs, e1_reg)?;
            let left_collation_ctx = program.curr_collation_ctx();
            program.reset_collation();

            self.lower_node(program, referenced_tables, resolver, rhs, e2_reg)?;
            let right_collation_ctx = program.curr_collation_ctx();
            program.reset_collation();

            let collation_ctx = merge_binary_collation_ctx(left_collation_ctx, right_collation_ctx);
            program.set_collation(collation_ctx);

            emit_binary_insn(
                program,
                &op,
                e1_reg,
                e2_reg,
                target_register,
                e1,
                e2,
                referenced_tables,
                Some(resolver),
            )?;
        }
        // Only reset collation for comparison operators, which consume it.
        // Non-comparison operators propagate the collation to the parent.
        if op.is_comparison() {
            program.reset_collation();
        }
        Ok(())
    }

    fn lower_case(
        &self,
        program: &mut ProgramBuilder,
        referenced_tables: Option<&TableReferences>,
        resolver: &Resolver,
        id: VId,
        target_register: usize,
    ) -> Result<()> {
        let VExpr::Case {
            base,
            when_then,
            else_expr,
        } = &self.node(id).kind
        else {
            unreachable!("lower_case requires a Case node")
        };
        let (base, else_expr) = (*base, *else_expr);
        let return_label = program.allocate_label();
        let mut next_case_label = program.allocate_label();
        // Only allocate a reg to hold the base expression if one was provided;
        // base_reg doubles as the flag for which CASE form is being lowered.
        let base_reg = base.map(|_| program.alloc_register());
        let expr_reg = program.alloc_register();
        if let Some(base_id) = base {
            self.lower_node(
                program,
                referenced_tables,
                resolver,
                base_id,
                base_reg.expect("base_reg allocated for base expression"),
            )?;
        }
        for (when_id, then_id) in when_then {
            // WHEN/THEN reuse expr_reg/target_register per arm, so their
            // registers must never be hoisted.
            self.lower_node_no_constant_opt(
                program,
                referenced_tables,
                resolver,
                *when_id,
                expr_reg,
            )?;
            match base_reg {
                // CASE base WHEN value: compare and jump to next arm on mismatch.
                Some(base_reg) => program.emit_insn(Insn::Ne {
                    lhs: base_reg,
                    rhs: expr_reg,
                    target_pc: next_case_label,
                    // A NULL result is considered untrue when evaluating WHEN terms.
                    flags: CmpInsFlags::default().jump_if_null(),
                    collation: program.curr_collation(),
                }),
                // CASE WHEN predicate: jump to next arm when untrue.
                None => program.emit_insn(Insn::IfNot {
                    reg: expr_reg,
                    target_pc: next_case_label,
                    jump_if_null: true,
                }),
            };
            self.lower_node_no_constant_opt(
                program,
                referenced_tables,
                resolver,
                *then_id,
                target_register,
            )?;
            program.emit_insn(Insn::Goto {
                target_pc: return_label,
            });
            program.preassign_label_to_next_insn(next_case_label);
            next_case_label = program.allocate_label();
        }
        match else_expr {
            Some(else_id) => {
                self.lower_node_no_constant_opt(
                    program,
                    referenced_tables,
                    resolver,
                    else_id,
                    target_register,
                )?;
            }
            // If ELSE isn't specified, it means ELSE null.
            None => {
                program.emit_insn(Insn::Null {
                    dest: target_register,
                    dest_end: None,
                });
            }
        }
        program.preassign_label_to_next_insn(return_label);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Database, MemoryIO};
    use std::sync::atomic::AtomicUsize;
    use std::sync::Mutex;

    /// Number of times ExprIr::build returned a usable IR. Lets tests assert
    /// the IR path actually engaged, so equivalence checks cannot pass vacuously.
    pub(super) static EXPR_IR_BUILT: AtomicUsize = AtomicUsize::new(0);

    /// EXPR_IR_ENABLED is process-global; serialize tests that toggle it.
    static TOGGLE_LOCK: Mutex<()> = Mutex::new(());

    fn open_test_db() -> crate::sync::Arc<crate::Connection> {
        let io = crate::sync::Arc::new(MemoryIO::new());
        let db = Database::open_file(
            io,
            ":memory:",
            crate::sync::Arc::new(crate::dialect::SqliteDialect),
        )
        .unwrap();
        let conn = db.connect().unwrap();
        for schema_sql in [
            "CREATE TABLE t (a, b, c TEXT COLLATE NOCASE, d INTEGER, e)",
            "CREATE INDEX t_d ON t (d)",
        ] {
            let mut stmt = conn.prepare(schema_sql).unwrap();
            stmt.run_with_row_callback(|_| Ok(())).unwrap();
        }
        conn
    }

    /// Compile `sql` under EXPLAIN and return the rendered program listing.
    fn explain(conn: &crate::sync::Arc<crate::Connection>, sql: &str) -> Vec<String> {
        let mut stmt = conn.prepare(format!("EXPLAIN {sql}")).unwrap();
        let mut listing = Vec::new();
        stmt.run_with_row_callback(|row| {
            listing.push(
                row.get_values()
                    .map(|v| format!("{v:?}"))
                    .collect::<Vec<_>>()
                    .join("|"),
            );
            Ok(())
        })
        .unwrap();
        assert!(!listing.is_empty(), "EXPLAIN produced no rows for: {sql}");
        listing
    }

    /// The IR lowering must produce bytecode identical to the eager path,
    /// instruction for instruction, for every statement in the corpus.
    #[test]
    fn expr_ir_bytecode_matches_eager_path() {
        // (sql, expect_ir_engagement): entries whose expressions are all Opaque
        // roots with leaf children (e.g. bare IN / IS TRUE) never build an IR,
        // but their bytecode must still be compared.
        let corpus: &[(&str, bool)] = &[
            // pure literal arithmetic and operator zoo
            ("SELECT 1 + 2 * 3 - 4", true),
            (
                "SELECT 1 < 2, 1 <= 2, 1 > 2, 1 >= 2, 1 = 2, 1 != 2, 1 <> 2",
                true,
            ),
            ("SELECT 7 % 3, 7 / 2, 1 << 4, 256 >> 2, 5 & 3, 5 | 2", true),
            ("SELECT 'a' || 'b' || 'c'", true),
            ("SELECT -(1 + 2), ~(1 + 1), NOT 1, +(2 + 3)", true),
            ("SELECT NULL IS NULL, 1 IS NOT NULL, NULL NOT NULL", true),
            // column-based expressions incl. collation interactions
            ("SELECT a + b, a - d, a * 2 + 1 FROM t", true),
            (
                "SELECT a < b, a <= 1, a != b, a = 'x', a IS b, a IS NOT b FROM t",
                true,
            ),
            ("SELECT c = 'x', c > 'y' FROM t", true),
            ("SELECT c || 'x' <> 'admin' FROM t", true),
            (
                "SELECT a IS NULL, b NOT NULL, NOT a, -a, ~a, +a FROM t",
                true,
            ),
            // shared-operand binary path
            ("SELECT a + a, a = a FROM t", true),
            // nested composites and parentheses
            ("SELECT (a + b) * (a - b) FROM t", true),
            ("SELECT ((a)) + ((1)) FROM t", true),
            // both CASE forms, nested in expressions
            (
                "SELECT CASE WHEN a > 1 THEN 'big' WHEN a > 0 THEN 'small' ELSE 'neg' END FROM t",
                true,
            ),
            (
                "SELECT CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' END FROM t",
                true,
            ),
            (
                "SELECT 1 + CASE WHEN a IS NULL THEN 0 ELSE a END FROM t",
                true,
            ),
            // opaque leaves (functions, subqueries, IN, BETWEEN, LIKE) under
            // and over supported composites
            ("SELECT length(c) + 1, abs(a - b) FROM t", true),
            ("SELECT a + (SELECT max(b) FROM t) FROM t", true),
            (
                "SELECT 1 IN (1, 2, 3), a IN (SELECT b FROM t) FROM t",
                false,
            ),
            // BETWEEN desugars into binary comparisons during translation,
            // so it engages the IR even though its own root is Opaque.
            (
                "SELECT a BETWEEN 1 AND 10, c LIKE 'x%', c NOT LIKE 'y%' FROM t",
                true,
            ),
            ("SELECT a IS TRUE, a IS NOT FALSE FROM t", false),
            ("SELECT (a IN (1, 2)) + (a BETWEEN 0 AND 9) FROM t", true),
            // conditions, joins, aggregates, DML
            (
                "SELECT * FROM t WHERE a + 1 > b * 2 AND (c = 'x' OR d IS NULL)",
                true,
            ),
            ("SELECT * FROM t WHERE CASE WHEN a THEN b ELSE d END", true),
            ("SELECT * FROM t WHERE d = 3 + 1", true),
            ("SELECT * FROM t x JOIN t y ON x.a + 1 = y.b - 1", true),
            (
                "SELECT sum(a + b), count(*) FROM t GROUP BY d HAVING sum(a) > 1",
                true,
            ),
            ("SELECT DISTINCT a + 1 FROM t ORDER BY a + 1 LIMIT 3", true),
            ("INSERT INTO t VALUES (1 + 2, 'x', 'y', 4, NULL)", true),
            (
                "UPDATE t SET a = a + 1, b = CASE WHEN b IS NULL THEN 0 ELSE b + 2 END WHERE d < 5",
                true,
            ),
            ("DELETE FROM t WHERE a * 2 >= 10", true),
        ];

        let _guard = TOGGLE_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let conn = open_test_db();
        for (sql, expect_engagement) in corpus {
            set_expr_ir_enabled(false);
            let eager = explain(&conn, sql);
            set_expr_ir_enabled(true);
            let built_before = EXPR_IR_BUILT.load(Ordering::Relaxed);
            let lowered = explain(&conn, sql);
            let built_after = EXPR_IR_BUILT.load(Ordering::Relaxed);
            assert_eq!(
                eager, lowered,
                "IR lowering diverged from eager bytecode for: {sql}"
            );
            assert_eq!(
                built_after > built_before,
                *expect_engagement,
                "unexpected expression IR engagement for: {sql}"
            );
        }
    }

    /// Composite roots engage the IR; leaves and unsupported roots skip it.
    #[test]
    fn expr_ir_engagement() {
        let _guard = TOGGLE_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        set_expr_ir_enabled(true);
        let conn = open_test_db();

        let engages = |sql: &str| {
            let before = EXPR_IR_BUILT.load(Ordering::Relaxed);
            let mut stmt = conn.prepare(sql).unwrap();
            stmt.run_with_row_callback(|_| Ok(())).unwrap();
            EXPR_IR_BUILT.load(Ordering::Relaxed) > before
        };

        assert!(engages("SELECT 1 + 2"));
        assert!(engages("SELECT a IS NULL FROM t"));
        assert!(engages("SELECT CASE WHEN a THEN 1 ELSE 2 END FROM t"));
        // Bare leaves never allocate an IR.
        assert!(!engages("SELECT 17"));
        assert!(!engages("SELECT a FROM t"));
    }
}
