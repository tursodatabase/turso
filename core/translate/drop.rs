use std::rc::Rc;

use crate::schema::{Index, Schema, Table};
use crate::vdbe::insn::Insn;

use super::delete::prepare_delete_plan;
use super::emitter::{emit_delete_insns, epilogue, prologue, OperationMode, TranslateCtx};
use super::main_loop::{close_loop, init_loop, open_loop};
use super::optimizer::optimize_plan;
use super::plan::{DeletePlan, Plan, WhereTerm};

use crate::{bail_parse_error, Result, SymbolTable};
use crate::{
    vdbe::builder::{ProgramBuilder, ProgramBuilderOpts, QueryMode},
    LimboError,
};
use limbo_sqlite3_parser::ast::{self, Expr, Id, Literal, Name, QualifiedName, Stmt};

/**
 * sqlite> EXPLAIN DROP TABLE examp;
addr  opcode         p1    p2    p3    p4             p5  comment
----  -------------  ----  ----  ----  -------------  --  -------------
0     Init           0     38    0                    0
1     Null           0     1     0                    0
2     OpenWrite      0     1     0     5              0
3     Rewind         0     11    0                    0
4       Column         0     2     2                    0
5       Ne             3     10    2     BINARY-8       82
6       Column         0     0     2                    0
7       Eq             4     10    2     BINARY-8       82
8       Rowid          0     5     0                    0
9       Delete         0     0     0                    2
10    Next           0     4     0                    1
11    Destroy        2     2     0                    0
12    Null           0     6     7                    0
13    OpenEphemeral  2     0     6                    0
14    IfNot          2     22    1                    0
15    OpenRead       1     1     0     4              0
16    Rewind         1     22    0                    0
17      Column         1     3     13                   0
18      Ne             2     21    13    BINARY-8       84
19      Rowid          1     7     0                    0
20      Insert         2     6     7                    0
21    Next           1     17    0                    1
22    OpenWrite      1     1     0     5              0
23    Rewind         2     35    0                    0
24      Rowid          2     7     0                    0
25      NotExists      1     34    7                    0
26      Column         1     0     8                    0
27      Column         1     1     9                    0
28      Column         1     2     10                   0
29      Integer        2     11    0                    0
30      Column         1     4     12                   0
31      MakeRecord     8     5     15    BBBDB          0
32      Delete         1     68    7                    0
33      Insert         1     15    7                    0
34    Next           2     24    0                    0
35    DropTable      0     0     0     examp          0
36    SetCookie      0     1     2                    0
37    Halt           0     0     0                    0
38    Transaction    0     1     1     0              1
39    String8        0     3     0     examp          0
40    String8        0     4     0     trigger        0
41    Goto           0     1     0                    0
 */

pub fn translate_drop_table(
    query_mode: QueryMode,
    tbl_name: ast::QualifiedName,
    schema: &Schema,
    if_exists: bool,
    syms: &SymbolTable,
) -> Result<ProgramBuilder> {
    let mut program = ProgramBuilder::new(ProgramBuilderOpts {
        query_mode,
        num_cursors: 1,
        approx_num_insns: 30,
        approx_num_labels: 5,
    });

    if schema.get_table(tbl_name.name.0.as_str()).is_none() {
        if if_exists {
            let init_lael = program.emit_init();
            let start_offset = program.offset();
            program.emit_halt();
            program.resolve_label(init_lael, program.offset());
            program.emit_transaction(true);
            program.emit_constant_insns();
            program.emit_goto(start_offset);
            return Ok(program);
        }
        bail_parse_error!("Table {} does not exist", tbl_name)
    }

    //TODO: virtual table check
    //TODO: authorization check
    //TODO: view check

    let (mut t_ctx, init_label, start_offset) = prologue(&mut program, syms, 1, 0)?;

    //TODO: virtual table check, drop table stats and fk if not virtual table
    //TODO: remove entries from sqlite_statN table
    emit_drop_tables(&mut program, schema, &tbl_name, &mut t_ctx);
    emit_fk_drop_tables(schema, tbl_name.to_owned());

    epilogue(&mut program, init_label, start_offset);

    Ok(program)
}

fn emit_fk_drop_tables(schema: &Schema, tbl_name: ast::QualifiedName) {}

fn emit_drop_tables(
    program: &mut ProgramBuilder,
    schema: &Schema,
    tbl_name: &ast::QualifiedName,
    t_ctx: &mut TranslateCtx<'_>,
) -> Result<()> {
    // > TODO: omit virtual table
    // > TODO: drop all triggers
    // > TODO: delete entries from sqlite_sequence table
    emit_schema_delete(program, schema, &tbl_name.name.0, t_ctx);
    emit_destroy(program, schema, tbl_name);
    // > TODO: if virtual table then update op6 and abort
    // > TODO: update cookies
    // > TODO: clear column name from every view in databse (basically yeet all colums that belong to this table)
    Ok(())
}

fn emit_destroy(
    program: &mut ProgramBuilder,
    schema: &Schema,
    tbl_name: &ast::QualifiedName,
) -> Result<()> {
    let table = schema.get_table(&tbl_name.name.0).unwrap();

    let root_page = table.root_page;
    let mut i_destroyed: usize = 0;

    loop {
        let mut i_largest: usize = 0;
        let mut d_idx: Option<Rc<Index>> = None;

        if i_destroyed == 0 || root_page < i_destroyed {
            i_largest = root_page;
        }

        match schema.indexes.get(&tbl_name.to_string()) {
            Some(indexes) => {
                for index in indexes {
                    let i_idx = index.root_page;
                    if (i_destroyed == 0 || (i_idx < i_destroyed)) && i_idx > i_largest {
                        i_largest = i_idx;
                        d_idx = Some(index.clone());
                    }
                }
            }
            None => {}
        }
        if i_largest == 0 {
            return Ok(());
        } else {
            let cursor_id = match i_largest {
                _root_page => program.alloc_cursor_id(
                    Some(table.name.clone()),
                    crate::vdbe::builder::CursorType::BTreeTable(table.clone()),
                ),
                _ => program.alloc_cursor_id(
                    Some(d_idx.to_owned().unwrap().name.clone()),
                    crate::vdbe::builder::CursorType::BTreeIndex(d_idx.to_owned().unwrap()),
                ),
            };
            program.emit_insn(Insn::DestroyAsync {
                root_page: i_largest,
                cursor_id: cursor_id,
                p3: 0,
            });
            program.emit_insn(Insn::DestroyAwait {
                root_page: i_largest,
                cursor_id: cursor_id,
                p3: 0,
            });
            i_destroyed = i_largest
        }
    }
}

fn emit_schema_delete(
    program: &mut ProgramBuilder,
    schema: &Schema,
    tbl_name: &str,
    t_ctx: &mut TranslateCtx<'_>,
) -> Result<()> {
    let type_column_value = "trigger";
    let table_id = "sqlite_schema".to_string();
    let table_name = QualifiedName {
        name: Name(table_id.clone()),
        db_name: None,
        alias: None,
    };

    let delete_predicate = Some(Box::new(Expr::Binary(
        Box::new(Expr::Binary(
            Box::new(Expr::Id(Id("tbl_name".to_string()))),
            ast::Operator::Equals,
            Box::new(Expr::Literal(Literal::String(tbl_name.to_string()))),
        )),
        ast::Operator::And,
        Box::new(Expr::Binary(
            Box::new(Expr::Id(Id("type".to_string()))),
            ast::Operator::NotEquals,
            Box::new(Expr::Literal(Literal::String(
                type_column_value.to_string(),
            ))),
        )),
    )));

    let mut p = prepare_delete_plan(schema, &table_name, delete_predicate, None)?;
    optimize_plan(&mut p, schema);
    let plan = match p {
        Plan::Delete(plan) => plan,
        _ => panic!("adwa"),
    };

    let after_main_loop_label = program.allocate_label();
    init_loop(
        program,
        t_ctx,
        &plan.table_references,
        &OperationMode::DELETE,
    )?;

    open_loop(program, t_ctx, &plan.table_references, &plan.where_clause)?;

    emit_delete_insns(program, t_ctx, &plan.table_references, &plan.limit)?;

    close_loop(program, t_ctx, &plan.table_references)?;

    program.resolve_label(after_main_loop_label, program.offset());

    Ok(())
}
