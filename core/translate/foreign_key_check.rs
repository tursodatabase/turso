use std::num::NonZeroUsize;

use crate::alloc::{vec, Arc, TursoIteratorExt, Vec};
use crate::namespace::NamespaceId;
use crate::schema::{BTreeTable, FkCheckItem, FkCheckParent, Schema, Table};
use crate::translate::emitter::{emit_columns_and_dependencies, Resolver};
use crate::translate::fkeys::{build_index_affinity_string, open_read_index, open_read_table};
use crate::vdbe::{builder::ProgramBuilder, insn::Insn};
use crate::{LimboError, Result};

struct ChildToCheck {
    table: Arc<BTreeTable>,
    items: Vec<FkCheckItem>,
}

pub fn translate_foreign_key_check(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    database_id: usize,
    namespace_id: NamespaceId,
    table_name: Option<&str>,
) -> Result<()> {
    let children = resolver.with_schema(database_id, |schema| match table_name {
        Some(table_name) => children_of_table(schema, namespace_id, table_name),
        None => children_of_all_tables(schema),
    })?;

    for child in &children {
        emit_check_of_child(program, resolver, database_id, child)?;
    }
    Ok(())
}

fn children_of_table(
    schema: &Schema,
    namespace_id: NamespaceId,
    table_name: &str,
) -> Result<Vec<ChildToCheck>> {
    let table = match schema.get_table(namespace_id, table_name) {
        Some(table) => table.btree(),
        None if schema.get_view(namespace_id, table_name).is_some()
            || schema
                .get_materialized_view(namespace_id, table_name)
                .is_some() =>
        {
            None
        }
        None => {
            return Err(LimboError::ParseError(format!(
                "no such table: {table_name}"
            )))
        }
    };
    let Some(table) = table else {
        return Ok(vec![]);
    };
    Ok(child_to_check(schema, namespace_id, table)?
        .into_iter()
        .try_collect()?)
}

fn children_of_all_tables(schema: &Schema) -> Result<Vec<ChildToCheck>> {
    let mut children = vec![];
    for namespace_id in schema.namespace_ids() {
        for table in schema.tables(namespace_id) {
            let Table::BTree(table) = table.as_ref() else {
                continue;
            };
            children.extend(child_to_check(schema, namespace_id, table.clone())?);
        }
    }
    Ok(children)
}

fn child_to_check(
    schema: &Schema,
    namespace_id: NamespaceId,
    table: Arc<BTreeTable>,
) -> Result<Option<ChildToCheck>> {
    if table.foreign_keys.is_empty() {
        return Ok(None);
    }
    let items = schema.resolved_fks_for_check(namespace_id, &table)?;
    Ok(Some(ChildToCheck { table, items }))
}

enum OpenParent {
    Missing,
    Rowid {
        cursor_id: usize,
    },
    Index {
        cursor_id: usize,
        affinities: String,
    },
}

fn emit_check_of_child(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    database_id: usize,
    child: &ChildToCheck,
) -> Result<()> {
    let child_cursor_id = open_read_table(program, &child.table, database_id);
    let parents: Vec<OpenParent> = child
        .items
        .iter()
        .map(|item| match &item.parent {
            FkCheckParent::Missing => OpenParent::Missing,
            FkCheckParent::Rowid { parent_tbl } => OpenParent::Rowid {
                cursor_id: open_read_table(program, parent_tbl, database_id),
            },
            FkCheckParent::Index { parent_tbl, index } => OpenParent::Index {
                cursor_id: open_read_index(program, index, database_id),
                affinities: build_index_affinity_string(index, parent_tbl),
            },
        })
        .try_collect()?;

    let result_base = program.alloc_registers(4);
    let rowid_reg = program.alloc_register();
    let loop_end = program.allocate_label();
    let loop_start = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: child_cursor_id,
        pc_if_empty: loop_end,
    });
    program.preassign_label_to_next_insn(loop_start);

    program.emit_insn(Insn::RowId {
        cursor_id: child_cursor_id,
        dest: rowid_reg,
    });
    let key_columns = child
        .items
        .iter()
        .flat_map(|item| item.child_pos.iter().copied());
    let columns = emit_columns_and_dependencies(
        program,
        &child.table,
        child_cursor_id,
        rowid_reg,
        key_columns,
        resolver,
    )?;

    for (item, parent) in child.items.iter().zip(&parents) {
        let next_fk = program.allocate_label();
        for &pos in item.child_pos.iter() {
            program.emit_insn(Insn::IsNull {
                reg: columns.to_column_reg(pos),
                target_pc: next_fk,
            });
        }

        match parent {
            OpenParent::Missing => {}
            OpenParent::Rowid { cursor_id } => {
                let violation = program.allocate_label();
                let key_reg = program.alloc_register();
                program.emit_insn(Insn::Copy {
                    src_reg: columns.to_column_reg(item.child_pos[0]),
                    dst_reg: key_reg,
                    extra_amount: 0,
                });
                program.emit_insn(Insn::MustBeInt {
                    reg: key_reg,
                    target_pc: Some(violation),
                });
                program.emit_insn(Insn::NotExists {
                    cursor: *cursor_id,
                    rowid_reg: key_reg,
                    target_pc: violation,
                });
                program.emit_insn(Insn::Goto { target_pc: next_fk });
                program.preassign_label_to_next_insn(violation);
            }
            OpenParent::Index {
                cursor_id,
                affinities,
            } => {
                let key_len = item.child_pos.len();
                let key_start = program.alloc_registers(key_len);
                for (i, &pos) in item.child_pos.iter().enumerate() {
                    program.emit_insn(Insn::Copy {
                        src_reg: columns.to_column_reg(pos),
                        dst_reg: key_start + i,
                        extra_amount: 0,
                    });
                }
                program.emit_insn(Insn::Affinity {
                    start_reg: key_start,
                    count: NonZeroUsize::new(key_len).expect("a foreign key has columns"),
                    affinities: affinities.clone(),
                });
                program.emit_insn(Insn::Found {
                    cursor_id: *cursor_id,
                    target_pc: next_fk,
                    record_reg: key_start,
                    num_regs: key_len,
                });
            }
        }

        program.emit_string8(child.table.name.clone(), result_base);
        program.emit_insn(Insn::Copy {
            src_reg: rowid_reg,
            dst_reg: result_base + 1,
            extra_amount: 0,
        });
        program.emit_string8(item.parent_table.clone(), result_base + 2);
        program.emit_int(item.fkid as i64, result_base + 3);
        program.emit_result_row(result_base, 4);
        program.preassign_label_to_next_insn(next_fk);
    }

    program.emit_insn(Insn::Next {
        cursor_id: child_cursor_id,
        pc_if_next: loop_start,
        fullscan: false,
        is_index: false,
    });
    program.preassign_label_to_next_insn(loop_end);

    program.emit_insn(Insn::Close {
        cursor_id: child_cursor_id,
    });
    for parent in &parents {
        match parent {
            OpenParent::Missing => {}
            OpenParent::Rowid { cursor_id } | OpenParent::Index { cursor_id, .. } => {
                program.emit_insn(Insn::Close {
                    cursor_id: *cursor_id,
                });
            }
        }
    }
    Ok(())
}
