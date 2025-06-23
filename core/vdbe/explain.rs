use turso_sqlite3_parser::ast::SortOrder;

use crate::vdbe::{builder::CursorType, insn::RegisterOrLiteral};

use super::{Insn, InsnReference, Program, Value};
use crate::function::{Func, ScalarFunc};

pub fn insn_to_str(
    program: &Program,
    addr: InsnReference,
    insn: &Insn,
    indent: String,
    manual_comment: Option<&'static str>,
) -> String {
    let get_table_or_index_name = |cursor_id: usize| {
        let cursor_type = &program.cursor_ref[cursor_id].1;
        match cursor_type {
            CursorType::BTreeTable(table) => &table.name,
            CursorType::BTreeIndex(index) => &index.name,
            CursorType::Pseudo(_) => "pseudo",
            CursorType::VirtualTable(virtual_table) => &virtual_table.name,
            CursorType::Sorter => "sorter",
        }
    };
    let (opcode, p1, p2, p3, p4, p5, comment): (&str, i32, i32, i32, Value, u16, String) =
        match insn {
            Insn::Init { target_pc } => (
                "Init",
                0,
                target_pc.as_debug_int(),
                0,
                Value::build_text(""),
                0,
                format!("Start at {}", target_pc.as_debug_int()),
            ),
            Insn::Add { lhs, rhs, dest } => (
                "Add",
                *lhs as i32,
                *rhs as i32,
                *dest as i32,
                Value::build_text(""),
                0,
                format!("r[{dest}]=r[{lhs}]+r[{rhs}]"),
            ),
            Insn::Subtract { lhs, rhs, dest } => (
                "Subtract",
                *lhs as i32,
                *rhs as i32,
                *dest as i32,
                Value::build_text(""),
                0,
                format!("r[{dest}]=r[{lhs}]-r[{rhs}]"),
            ),
            Insn::Multiply { lhs, rhs, dest } => (
                "Multiply",
                *lhs as i32,
                *rhs as i32,
                *dest as i32,
                Value::build_text(""),
                0,
                format!("r[{dest}]=r[{lhs}]*r[{rhs}]"),
            ),
            Insn::Divide { lhs, rhs, dest } => (
                "Divide",
                *lhs as i32,
                *rhs as i32,
                *dest as i32,
                Value::build_text(""),
                0,
                format!("r[{dest}]=r[{lhs}]/r[{rhs}]"),
            ),
            Insn::BitAnd { lhs, rhs, dest } => (
                "BitAnd",
                *lhs as i32,
                *rhs as i32,
                *dest as i32,
                Value::build_text(""),
                0,
                format!("r[{dest}]=r[{lhs}]&r[{rhs}]"),
            ),
            Insn::BitOr { lhs, rhs, dest } => (
                "BitOr",
                *lhs as i32,
                *rhs as i32,
                *dest as i32,
                Value::build_text(""),
                0,
                format!("r[{dest}]=r[{lhs}]|r[{rhs}]"),
            ),
            Insn::BitNot { reg, dest } => (
                "BitNot",
                *reg as i32,
                *dest as i32,
                0,
                Value::build_text(""),
                0,
                format!("r[{dest}]=~r[{reg}]"),
            ),
            Insn::Checkpoint {
                database,
                checkpoint_mode: _,
                dest,
            } => (
                "Checkpoint",
                *database as i32,
                *dest as i32,
                0,
                Value::build_text(""),
                0,
                format!("r[{dest}]=~r[{database}]"),
            ),
            Insn::Remainder { lhs, rhs, dest } => (
                "Remainder",
                *lhs as i32,
                *rhs as i32,
                *dest as i32,
                Value::build_text(""),
                0,
                format!("r[{dest}]=r[{lhs}]%r[{rhs}]"),
            ),
            Insn::Null { dest, dest_end } => (
                "Null",
                0,
                *dest as i32,
                dest_end.map_or(0, |end| end as i32),
                Value::build_text(""),
                0,
                dest_end.map_or(format!("r[{dest}]=NULL"), |end| {
                    format!("r[{dest}..{end}]=NULL")
                }),
            ),
            Insn::NullRow { cursor_id } => (
                "NullRow",
                *cursor_id as i32,
                0,
                0,
                Value::build_text(""),
                0,
                format!("Set cursor {cursor_id} to a (pseudo) NULL row"),
            ),
            Insn::NotNull { reg, target_pc } => (
                "NotNull",
                *reg as i32,
                target_pc.as_debug_int(),
                0,
                Value::build_text(""),
                0,
                format!("r[{}]!=NULL -> goto {}", reg, target_pc.as_debug_int()),
            ),
            Insn::Compare {
                start_reg_a,
                start_reg_b,
                count,
                collation,
            } => (
                "Compare",
                *start_reg_a as i32,
                *start_reg_b as i32,
                *count as i32,
                Value::build_text(format!("k({count}, {})", collation.unwrap_or_default())),
                0,
                format!(
                    "r[{}..{}]==r[{}..{}]",
                    start_reg_a,
                    start_reg_a + (count - 1),
                    start_reg_b,
                    start_reg_b + (count - 1)
                ),
            ),
            Insn::Jump {
                target_pc_lt,
                target_pc_eq,
                target_pc_gt,
            } => (
                "Jump",
                target_pc_lt.as_debug_int(),
                target_pc_eq.as_debug_int(),
                target_pc_gt.as_debug_int(),
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::Move {
                source_reg,
                dest_reg,
                count,
            } => (
                "Move",
                *source_reg as i32,
                *dest_reg as i32,
                *count as i32,
                Value::build_text(""),
                0,
                format!(
                    "r[{}..{}]=r[{}..{}]",
                    dest_reg,
                    dest_reg + (count - 1),
                    source_reg,
                    source_reg + (count - 1)
                ),
            ),
            Insn::IfPos {
                reg,
                target_pc,
                decrement_by,
            } => (
                "IfPos",
                *reg as i32,
                target_pc.as_debug_int(),
                0,
                Value::build_text(""),
                0,
                format!(
                    "r[{}]>0 -> r[{}]-={}, goto {}",
                    reg,
                    reg,
                    decrement_by,
                    target_pc.as_debug_int()
                ),
            ),
            Insn::Eq {
                lhs,
                rhs,
                target_pc,
                collation,
                ..
            } => (
                "Eq",
                *lhs as i32,
                *rhs as i32,
                target_pc.as_debug_int(),
                Value::build_text(collation.map_or("".to_string(), |c| c.to_string())),
                0,
                format!(
                    "if r[{}]==r[{}] goto {}",
                    lhs,
                    rhs,
                    target_pc.as_debug_int()
                ),
            ),
            Insn::Ne {
                lhs,
                rhs,
                target_pc,
                collation,
                ..
            } => (
                "Ne",
                *lhs as i32,
                *rhs as i32,
                target_pc.as_debug_int(),
                Value::build_text(collation.map_or("".to_string(), |c| c.to_string())),
                0,
                format!(
                    "if r[{}]!=r[{}] goto {}",
                    lhs,
                    rhs,
                    target_pc.as_debug_int()
                ),
            ),
            Insn::Lt {
                lhs,
                rhs,
                target_pc,
                collation,
                ..
            } => (
                "Lt",
                *lhs as i32,
                *rhs as i32,
                target_pc.as_debug_int(),
                Value::build_text(collation.map_or("".to_string(), |c| c.to_string())),
                0,
                format!("if r[{}]<r[{}] goto {}", lhs, rhs, target_pc.as_debug_int()),
            ),
            Insn::Le {
                lhs,
                rhs,
                target_pc,
                collation,
                ..
            } => (
                "Le",
                *lhs as i32,
                *rhs as i32,
                target_pc.as_debug_int(),
                Value::build_text(collation.map_or("".to_string(), |c| c.to_string())),
                0,
                format!(
                    "if r[{}]<=r[{}] goto {}",
                    lhs,
                    rhs,
                    target_pc.as_debug_int()
                ),
            ),
            Insn::Gt {
                lhs,
                rhs,
                target_pc,
                collation,
                ..
            } => (
                "Gt",
                *lhs as i32,
                *rhs as i32,
                target_pc.as_debug_int(),
                Value::build_text(collation.map_or("".to_string(), |c| c.to_string())),
                0,
                format!("if r[{}]>r[{}] goto {}", lhs, rhs, target_pc.as_debug_int()),
            ),
            Insn::Ge {
                lhs,
                rhs,
                target_pc,
                collation,
                ..
            } => (
                "Ge",
                *lhs as i32,
                *rhs as i32,
                target_pc.as_debug_int(),
                Value::build_text(collation.map_or("".to_string(), |c| c.to_string())),
                0,
                format!(
                    "if r[{}]>=r[{}] goto {}",
                    lhs,
                    rhs,
                    target_pc.as_debug_int()
                ),
            ),
            Insn::If {
                reg,
                target_pc,
                jump_if_null,
            } => (
                "If",
                *reg as i32,
                target_pc.as_debug_int(),
                *jump_if_null as i32,
                Value::build_text(""),
                0,
                format!("if r[{}] goto {}", reg, target_pc.as_debug_int()),
            ),
            Insn::IfNot {
                reg,
                target_pc,
                jump_if_null,
            } => (
                "IfNot",
                *reg as i32,
                target_pc.as_debug_int(),
                *jump_if_null as i32,
                Value::build_text(""),
                0,
                format!("if !r[{}] goto {}", reg, target_pc.as_debug_int()),
            ),
            Insn::OpenRead {
                cursor_id,
                root_page,
            } => (
                "OpenRead",
                *cursor_id as i32,
                *root_page as i32,
                0,
                Value::build_text(""),
                0,
                {
                    let cursor_type =
                        program.cursor_ref[*cursor_id]
                            .0
                            .as_ref()
                            .map_or("", |cursor_key| {
                                if cursor_key.index.is_some() {
                                    "index"
                                } else {
                                    "table"
                                }
                            });
                    format!(
                        "{}={}, root={}",
                        cursor_type,
                        get_table_or_index_name(*cursor_id),
                        root_page
                    )
                },
            ),
            Insn::VOpen { cursor_id } => (
                "VOpen",
                *cursor_id as i32,
                0,
                0,
                Value::build_text(""),
                0,
                {
                    let cursor_type =
                        program.cursor_ref[*cursor_id]
                            .0
                            .as_ref()
                            .map_or("", |cursor_key| {
                                if cursor_key.index.is_some() {
                                    "index"
                                } else {
                                    "table"
                                }
                            });
                    format!("{} {}", cursor_type, get_table_or_index_name(*cursor_id),)
                },
            ),
            Insn::VCreate {
                table_name,
                module_name,
                args_reg,
            } => (
                "VCreate",
                *table_name as i32,
                *module_name as i32,
                args_reg.unwrap_or(0) as i32,
                Value::build_text(""),
                0,
                format!("table={table_name}, module={module_name}"),
            ),
            Insn::VFilter {
                cursor_id,
                pc_if_empty,
                arg_count,
                ..
            } => (
                "VFilter",
                *cursor_id as i32,
                pc_if_empty.as_debug_int(),
                *arg_count as i32,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::VColumn {
                cursor_id,
                column,
                dest,
            } => (
                "VColumn",
                *cursor_id as i32,
                *column as i32,
                *dest as i32,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::VUpdate {
                cursor_id,
                arg_count,       // P2: Number of arguments in argv[]
                start_reg,       // P3: Start register for argv[]
                conflict_action, // P4: Conflict resolution flags
            } => (
                "VUpdate",
                *cursor_id as i32,
                *arg_count as i32,
                *start_reg as i32,
                Value::build_text(""),
                *conflict_action,
                format!("args=r[{}..{}]", start_reg, start_reg + arg_count - 1),
            ),
            Insn::VNext {
                cursor_id,
                pc_if_next,
            } => (
                "VNext",
                *cursor_id as i32,
                pc_if_next.as_debug_int(),
                0,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::VDestroy { db, table_name } => (
                "VDestroy",
                *db as i32,
                0,
                0,
                Value::build_text(table_name),
                0,
                "".to_string(),
            ),
            Insn::OpenPseudo {
                cursor_id,
                content_reg,
                num_fields,
            } => (
                "OpenPseudo",
                *cursor_id as i32,
                *content_reg as i32,
                *num_fields as i32,
                Value::build_text(""),
                0,
                format!("{num_fields} columns in r[{content_reg}]"),
            ),
            Insn::Rewind {
                cursor_id,
                pc_if_empty,
            } => (
                "Rewind",
                *cursor_id as i32,
                pc_if_empty.as_debug_int(),
                0,
                Value::build_text(""),
                0,
                {
                    let cursor_type =
                        program.cursor_ref[*cursor_id]
                            .0
                            .as_ref()
                            .map_or("", |cursor_key| {
                                if cursor_key.index.is_some() {
                                    "index"
                                } else {
                                    "table"
                                }
                            });
                    format!(
                        "Rewind {} {}",
                        cursor_type,
                        get_table_or_index_name(*cursor_id),
                    )
                },
            ),
            Insn::Column {
                cursor_id,
                column,
                dest,
                default,
            } => {
                let cursor_type = &program.cursor_ref[*cursor_id].1;
                let column_name: Option<&String> = match cursor_type {
                    CursorType::BTreeTable(table) => {
                        let name = table.columns.get(*column).and_then(|v| v.name.as_ref());
                        name
                    }
                    CursorType::BTreeIndex(index) => {
                        let name = &index.columns.get(*column).unwrap().name;
                        Some(name)
                    }
                    CursorType::Pseudo(_) => None,
                    CursorType::Sorter => None,
                    CursorType::VirtualTable(v) => v.columns.get(*column).unwrap().name.as_ref(),
                };
                (
                    "Column",
                    *cursor_id as i32,
                    *column as i32,
                    *dest as i32,
                    default.clone().unwrap_or_else(|| Value::build_text("")),
                    0,
                    format!(
                        "r[{}]={}.{}",
                        dest,
                        get_table_or_index_name(*cursor_id),
                        column_name.unwrap_or(&format!("column {}", *column))
                    ),
                )
            }
            Insn::TypeCheck {
                start_reg,
                count,
                check_generated,
                ..
            } => (
                "TypeCheck",
                *start_reg as i32,
                *count as i32,
                *check_generated as i32,
                Value::build_text(""),
                0,
                String::from(""),
            ),
            Insn::MakeRecord {
                start_reg,
                count,
                dest_reg,
                index_name,
            } => {
                let for_index = index_name.as_ref().map(|name| format!("; for {name}"));
                (
                    "MakeRecord",
                    *start_reg as i32,
                    *count as i32,
                    *dest_reg as i32,
                    Value::build_text(""),
                    0,
                    format!(
                        "r[{}]=mkrec(r[{}..{}]){}",
                        dest_reg,
                        start_reg,
                        start_reg + count - 1,
                        for_index.unwrap_or("".to_string())
                    ),
                )
            }
            Insn::ResultRow { start_reg, count } => (
                "ResultRow",
                *start_reg as i32,
                *count as i32,
                0,
                Value::build_text(""),
                0,
                if *count == 1 {
                    format!("output=r[{start_reg}]")
                } else {
                    format!("output=r[{}..{}]", start_reg, start_reg + count - 1)
                },
            ),
            Insn::Next {
                cursor_id,
                pc_if_next,
            } => (
                "Next",
                *cursor_id as i32,
                pc_if_next.as_debug_int(),
                0,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::Halt {
                err_code,
                description,
            } => (
                "Halt",
                *err_code as i32,
                0,
                0,
                Value::build_text(description),
                0,
                "".to_string(),
            ),
            Insn::HaltIfNull {
                err_code,
                target_reg,
                description,
            } => (
                "HaltIfNull",
                *err_code as i32,
                0,
                *target_reg as i32,
                Value::build_text(description),
                0,
                "".to_string(),
            ),
            Insn::Transaction { write } => (
                "Transaction",
                0,
                *write as i32,
                0,
                Value::build_text(""),
                0,
                format!("write={write}"),
            ),
            Insn::Goto { target_pc } => (
                "Goto",
                0,
                target_pc.as_debug_int(),
                0,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::Gosub {
                target_pc,
                return_reg,
            } => (
                "Gosub",
                *return_reg as i32,
                target_pc.as_debug_int(),
                0,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::Return {
                return_reg,
                can_fallthrough,
            } => (
                "Return",
                *return_reg as i32,
                0,
                *can_fallthrough as i32,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::Integer { value, dest } => (
                "Integer",
                *value as i32,
                *dest as i32,
                0,
                Value::build_text(""),
                0,
                format!("r[{dest}]={value}"),
            ),
            Insn::Real { value, dest } => (
                "Real",
                0,
                *dest as i32,
                0,
                Value::Float(*value),
                0,
                format!("r[{dest}]={value}"),
            ),
            Insn::RealAffinity { register } => (
                "RealAffinity",
                *register as i32,
                0,
                0,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::String8 { value, dest } => (
                "String8",
                0,
                *dest as i32,
                0,
                Value::build_text(value),
                0,
                format!("r[{dest}]='{value}'"),
            ),
            Insn::Blob { value, dest } => (
                "Blob",
                0,
                *dest as i32,
                0,
                Value::Blob(value.clone()),
                0,
                format!(
                    "r[{}]={} (len={})",
                    dest,
                    String::from_utf8_lossy(value),
                    value.len()
                ),
            ),
            Insn::RowId { cursor_id, dest } => (
                "RowId",
                *cursor_id as i32,
                *dest as i32,
                0,
                Value::build_text(""),
                0,
                format!("r[{}]={}.rowid", dest, get_table_or_index_name(*cursor_id)),
            ),
            Insn::IdxRowId { cursor_id, dest } => (
                "IdxRowId",
                *cursor_id as i32,
                *dest as i32,
                0,
                Value::build_text(""),
                0,
                format!(
                    "r[{}]={}.rowid",
                    dest,
                    program.cursor_ref[*cursor_id]
                        .0
                        .as_ref()
                        .map(|k| format!(
                            "cursor {} for {} {}",
                            cursor_id,
                            if k.index.is_some() { "index" } else { "table" },
                            get_table_or_index_name(*cursor_id),
                        ))
                        .unwrap_or(format!("cursor {cursor_id}"))
                ),
            ),
            Insn::SeekRowid {
                cursor_id,
                src_reg,
                target_pc,
            } => (
                "SeekRowid",
                *cursor_id as i32,
                *src_reg as i32,
                target_pc.as_debug_int(),
                Value::build_text(""),
                0,
                format!(
                    "if (r[{}]!={}.rowid) goto {}",
                    src_reg,
                    &program.cursor_ref[*cursor_id]
                        .0
                        .as_ref()
                        .map(|k| format!(
                            "cursor {} for {} {}",
                            cursor_id,
                            if k.index.is_some() { "index" } else { "table" },
                            get_table_or_index_name(*cursor_id),
                        ))
                        .unwrap_or(format!("cursor {cursor_id}")),
                    target_pc.as_debug_int()
                ),
            ),
            Insn::DeferredSeek {
                index_cursor_id,
                table_cursor_id,
            } => (
                "DeferredSeek",
                *index_cursor_id as i32,
                *table_cursor_id as i32,
                0,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::SeekGT {
                is_index: _,
                cursor_id,
                start_reg,
                num_regs,
                target_pc,
            }
            | Insn::SeekGE {
                is_index: _,
                cursor_id,
                start_reg,
                num_regs,
                target_pc,
                ..
            }
            | Insn::SeekLE {
                is_index: _,
                cursor_id,
                start_reg,
                num_regs,
                target_pc,
                ..
            }
            | Insn::SeekLT {
                is_index: _,
                cursor_id,
                start_reg,
                num_regs,
                target_pc,
            } => (
                match insn {
                    Insn::SeekGT { .. } => "SeekGT",
                    Insn::SeekGE { .. } => "SeekGE",
                    Insn::SeekLE { .. } => "SeekLE",
                    Insn::SeekLT { .. } => "SeekLT",
                    _ => unreachable!(),
                },
                *cursor_id as i32,
                target_pc.as_debug_int(),
                *start_reg as i32,
                Value::build_text(""),
                0,
                format!("key=[{}..{}]", start_reg, start_reg + num_regs - 1),
            ),
            Insn::SeekEnd { cursor_id } => (
                "SeekEnd",
                *cursor_id as i32,
                0,
                0,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::IdxInsert {
                cursor_id,
                record_reg,
                unpacked_start,
                flags,
                ..
            } => (
                "IdxInsert",
                *cursor_id as i32,
                *record_reg as i32,
                unpacked_start.unwrap_or(0) as i32,
                Value::build_text(""),
                flags.0 as u16,
                format!("key=r[{record_reg}]"),
            ),
            Insn::IdxGT {
                cursor_id,
                start_reg,
                num_regs,
                target_pc,
            }
            | Insn::IdxGE {
                cursor_id,
                start_reg,
                num_regs,
                target_pc,
            }
            | Insn::IdxLE {
                cursor_id,
                start_reg,
                num_regs,
                target_pc,
            }
            | Insn::IdxLT {
                cursor_id,
                start_reg,
                num_regs,
                target_pc,
            } => (
                match insn {
                    Insn::IdxGT { .. } => "IdxGT",
                    Insn::IdxGE { .. } => "IdxGE",
                    Insn::IdxLE { .. } => "IdxLE",
                    Insn::IdxLT { .. } => "IdxLT",
                    _ => unreachable!(),
                },
                *cursor_id as i32,
                target_pc.as_debug_int(),
                *start_reg as i32,
                Value::build_text(""),
                0,
                format!("key=[{}..{}]", start_reg, start_reg + num_regs - 1),
            ),
            Insn::DecrJumpZero { reg, target_pc } => (
                "DecrJumpZero",
                *reg as i32,
                target_pc.as_debug_int(),
                0,
                Value::build_text(""),
                0,
                format!("if (--r[{}]==0) goto {}", reg, target_pc.as_debug_int()),
            ),
            Insn::AggStep {
                func,
                acc_reg,
                delimiter: _,
                col,
            } => (
                "AggStep",
                0,
                *col as i32,
                *acc_reg as i32,
                Value::build_text(func.to_string()),
                0,
                format!("accum=r[{}] step(r[{}])", *acc_reg, *col),
            ),
            Insn::AggFinal { register, func } => (
                "AggFinal",
                0,
                *register as i32,
                0,
                Value::build_text(func.to_string()),
                0,
                format!("accum=r[{}]", *register),
            ),
            Insn::SorterOpen {
                cursor_id,
                columns,
                order,
                collations,
            } => {
                let _p4 = String::new();
                let to_print: Vec<String> = order
                    .iter()
                    .zip(collations.iter())
                    .map(|(v, collation)| {
                        let sign = match v {
                            SortOrder::Asc => "",
                            SortOrder::Desc => "-",
                        };
                        if collation.is_some() {
                            format!("{sign}{}", collation.unwrap())
                        } else {
                            format!("{sign}B")
                        }
                    })
                    .collect();
                (
                    "SorterOpen",
                    *cursor_id as i32,
                    *columns as i32,
                    0,
                    Value::build_text(format!("k({},{})", order.len(), to_print.join(","))),
                    0,
                    format!("cursor={cursor_id}"),
                )
            }
            Insn::SorterData {
                cursor_id,
                dest_reg,
                pseudo_cursor,
            } => (
                "SorterData",
                *cursor_id as i32,
                *dest_reg as i32,
                *pseudo_cursor as i32,
                Value::build_text(""),
                0,
                format!("r[{dest_reg}]=data"),
            ),
            Insn::SorterInsert {
                cursor_id,
                record_reg,
            } => (
                "SorterInsert",
                *cursor_id as i32,
                *record_reg as i32,
                0,
                Value::Integer(0),
                0,
                format!("key=r[{record_reg}]"),
            ),
            Insn::SorterSort {
                cursor_id,
                pc_if_empty,
            } => (
                "SorterSort",
                *cursor_id as i32,
                pc_if_empty.as_debug_int(),
                0,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::SorterNext {
                cursor_id,
                pc_if_next,
            } => (
                "SorterNext",
                *cursor_id as i32,
                pc_if_next.as_debug_int(),
                0,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::Function {
                constant_mask,
                start_reg,
                dest,
                func,
            } => (
                "Function",
                *constant_mask,
                *start_reg as i32,
                *dest as i32,
                {
                    let s = if matches!(&func.func, Func::Scalar(ScalarFunc::Like)) {
                        format!("like({})", func.arg_count)
                    } else {
                        func.func.to_string()
                    };
                    Value::build_text(s)
                },
                0,
                if func.arg_count == 0 {
                    format!("r[{dest}]=func()")
                } else if *start_reg == *start_reg + func.arg_count - 1 {
                    format!("r[{dest}]=func(r[{start_reg}])")
                } else {
                    format!(
                        "r[{}]=func(r[{}..{}])",
                        dest,
                        start_reg,
                        start_reg + func.arg_count - 1
                    )
                },
            ),
            Insn::InitCoroutine {
                yield_reg,
                jump_on_definition,
                start_offset,
            } => (
                "InitCoroutine",
                *yield_reg as i32,
                jump_on_definition.as_debug_int(),
                start_offset.as_debug_int(),
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::EndCoroutine { yield_reg } => (
                "EndCoroutine",
                *yield_reg as i32,
                0,
                0,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::Yield {
                yield_reg,
                end_offset,
            } => (
                "Yield",
                *yield_reg as i32,
                end_offset.as_debug_int(),
                0,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::Insert {
                cursor,
                key_reg,
                record_reg,
                flag,
                table_name,
            } => (
                "Insert",
                *cursor as i32,
                *record_reg as i32,
                *key_reg as i32,
                Value::build_text(table_name),
                flag.0 as u16,
                format!("intkey=r[{key_reg}] data=r[{record_reg}]"),
            ),
            Insn::Delete { cursor_id } => (
                "Delete",
                *cursor_id as i32,
                0,
                0,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::IdxDelete {
                cursor_id,
                start_reg,
                num_regs,
                raise_error_if_no_matching_entry,
            } => (
                "IdxDelete",
                *cursor_id as i32,
                *start_reg as i32,
                *num_regs as i32,
                Value::build_text(""),
                *raise_error_if_no_matching_entry as u16,
                "".to_string(),
            ),
            Insn::NewRowid {
                cursor,
                rowid_reg,
                prev_largest_reg,
            } => (
                "NewRowid",
                *cursor as i32,
                *rowid_reg as i32,
                *prev_largest_reg as i32,
                Value::build_text(""),
                0,
                format!("r[{rowid_reg}]=rowid"),
            ),
            Insn::MustBeInt { reg } => (
                "MustBeInt",
                *reg as i32,
                0,
                0,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::SoftNull { reg } => (
                "SoftNull",
                *reg as i32,
                0,
                0,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::NoConflict {
                cursor_id,
                target_pc,
                record_reg,
                num_regs,
            } => {
                let key = if *num_regs > 0 {
                    format!("key=r[{}..{}]", record_reg, record_reg + num_regs - 1)
                } else {
                    format!("key=r[{record_reg}]")
                };
                (
                    "NoConflict",
                    *cursor_id as i32,
                    target_pc.as_debug_int(),
                    *record_reg as i32,
                    Value::build_text(format!("{num_regs}")),
                    0,
                    key,
                )
            }
            Insn::NotExists {
                cursor,
                rowid_reg,
                target_pc,
            } => (
                "NotExists",
                *cursor as i32,
                target_pc.as_debug_int(),
                *rowid_reg as i32,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::OffsetLimit {
                limit_reg,
                combined_reg,
                offset_reg,
            } => (
                "OffsetLimit",
                *limit_reg as i32,
                *combined_reg as i32,
                *offset_reg as i32,
                Value::build_text(""),
                0,
                format!(
                    "if r[{limit_reg}]>0 then r[{combined_reg}]=r[{limit_reg}]+max(0,r[{offset_reg}]) else r[{combined_reg}]=(-1)"
                ),
            ),
            Insn::OpenWrite {
                cursor_id,
                root_page,
                name,
                ..
            } => (
                "OpenWrite",
                *cursor_id as i32,
                match root_page {
                    RegisterOrLiteral::Literal(i) => *i as _,
                    RegisterOrLiteral::Register(i) => *i as _,
                },
                0,
                Value::build_text(""),
                0,
                format!("root={root_page}; {name}"),
            ),
            Insn::Copy {
                src_reg,
                dst_reg,
                amount,
            } => (
                "Copy",
                *src_reg as i32,
                *dst_reg as i32,
                *amount as i32,
                Value::build_text(""),
                0,
                format!("r[{dst_reg}]=r[{src_reg}]"),
            ),
            Insn::CreateBtree { db, root, flags } => (
                "CreateBtree",
                *db as i32,
                *root as i32,
                flags.get_flags() as i32,
                Value::build_text(""),
                0,
                format!("r[{}]=root iDb={} flags={}", root, db, flags.get_flags()),
            ),
            Insn::Destroy {
                root,
                former_root_reg,
                is_temp,
            } => (
                "Destroy",
                *root as i32,
                *former_root_reg as i32,
                *is_temp as i32,
                Value::build_text(""),
                0,
                format!(
                    "root iDb={root} former_root={former_root_reg} is_temp={is_temp}"
                ),
            ),
            Insn::DropTable {
                db,
                _p2,
                _p3,
                table_name,
            } => (
                "DropTable",
                *db as i32,
                0,
                0,
                Value::build_text(table_name),
                0,
                format!("DROP TABLE {table_name}"),
            ),
            Insn::DropIndex { db: _, index } => (
                "DropIndex",
                0,
                0,
                0,
                Value::build_text(index.name.clone()),
                0,
                format!("DROP INDEX {}", index.name),
            ),
            Insn::Close { cursor_id } => (
                "Close",
                *cursor_id as i32,
                0,
                0,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::Last {
                cursor_id,
                pc_if_empty,
            } => (
                "Last",
                *cursor_id as i32,
                pc_if_empty.as_debug_int(),
                0,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::IsNull { reg, target_pc } => (
                "IsNull",
                *reg as i32,
                target_pc.as_debug_int(),
                0,
                Value::build_text(""),
                0,
                format!("if (r[{}]==NULL) goto {}", reg, target_pc.as_debug_int()),
            ),
            Insn::ParseSchema { db, where_clause } => (
                "ParseSchema",
                *db as i32,
                0,
                0,
                Value::build_text(where_clause.clone().unwrap_or("NULL".to_string())),
                0,
                where_clause.clone().unwrap_or("NULL".to_string()),
            ),
            Insn::Prev {
                cursor_id,
                pc_if_prev,
            } => (
                "Prev",
                *cursor_id as i32,
                pc_if_prev.as_debug_int(),
                0,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::ShiftRight { lhs, rhs, dest } => (
                "ShiftRight",
                *rhs as i32,
                *lhs as i32,
                *dest as i32,
                Value::build_text(""),
                0,
                format!("r[{dest}]=r[{lhs}] >> r[{rhs}]"),
            ),
            Insn::ShiftLeft { lhs, rhs, dest } => (
                "ShiftLeft",
                *rhs as i32,
                *lhs as i32,
                *dest as i32,
                Value::build_text(""),
                0,
                format!("r[{dest}]=r[{lhs}] << r[{rhs}]"),
            ),
            Insn::Variable { index, dest } => (
                "Variable",
                usize::from(*index) as i32,
                *dest as i32,
                0,
                Value::build_text(""),
                0,
                format!("r[{}]=parameter({})", *dest, *index),
            ),
            Insn::ZeroOrNull { rg1, rg2, dest } => (
                "ZeroOrNull",
                *rg1 as i32,
                *dest as i32,
                *rg2 as i32,
                Value::build_text(""),
                0,
                format!(
                    "((r[{rg1}]=NULL)|(r[{rg2}]=NULL)) ? r[{dest}]=NULL : r[{dest}]=0"
                ),
            ),
            Insn::Not { reg, dest } => (
                "Not",
                *reg as i32,
                *dest as i32,
                0,
                Value::build_text(""),
                0,
                format!("r[{dest}]=!r[{reg}]"),
            ),
            Insn::Concat { lhs, rhs, dest } => (
                "Concat",
                *rhs as i32,
                *lhs as i32,
                *dest as i32,
                Value::build_text(""),
                0,
                format!("r[{dest}]=r[{lhs}] + r[{rhs}]"),
            ),
            Insn::And { lhs, rhs, dest } => (
                "And",
                *rhs as i32,
                *lhs as i32,
                *dest as i32,
                Value::build_text(""),
                0,
                format!("r[{dest}]=(r[{lhs}] && r[{rhs}])"),
            ),
            Insn::Or { lhs, rhs, dest } => (
                "Or",
                *rhs as i32,
                *lhs as i32,
                *dest as i32,
                Value::build_text(""),
                0,
                format!("r[{dest}]=(r[{lhs}] || r[{rhs}])"),
            ),
            Insn::Noop => ("Noop", 0, 0, 0, Value::build_text(""), 0, String::new()),
            Insn::PageCount { db, dest } => (
                "Pagecount",
                *db as i32,
                *dest as i32,
                0,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::ReadCookie { db, dest, cookie } => (
                "ReadCookie",
                *db as i32,
                *dest as i32,
                *cookie as i32,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::SetCookie {
                db,
                cookie,
                value,
                p5,
            } => (
                "SetCookie",
                *db as i32,
                *cookie as i32,
                *value,
                Value::build_text(""),
                *p5,
                "".to_string(),
            ),
            Insn::AutoCommit {
                auto_commit,
                rollback,
            } => (
                "AutoCommit",
                *auto_commit as i32,
                *rollback as i32,
                0,
                Value::build_text(""),
                0,
                format!("auto_commit={auto_commit}, rollback={rollback}"),
            ),
            Insn::OpenEphemeral {
                cursor_id,
                is_table,
            } => (
                "OpenEphemeral",
                *cursor_id as i32,
                *is_table as i32,
                0,
                Value::build_text(""),
                0,
                format!(
                    "cursor={} is_table={}",
                    cursor_id,
                    if *is_table { "true" } else { "false" }
                ),
            ),
            Insn::OpenAutoindex { cursor_id } => (
                "OpenAutoindex",
                *cursor_id as i32,
                0,
                0,
                Value::build_text(""),
                0,
                format!("cursor={cursor_id}"),
            ),
            Insn::Once {
                target_pc_when_reentered,
            } => (
                "Once",
                target_pc_when_reentered.as_debug_int(),
                0,
                0,
                Value::build_text(""),
                0,
                format!("goto {}", target_pc_when_reentered.as_debug_int()),
            ),
            Insn::BeginSubrtn { dest, dest_end } => (
                "BeginSubrtn",
                *dest as i32,
                dest_end.map_or(0, |end| end as i32),
                0,
                Value::build_text(""),
                0,
                dest_end.map_or(format!("r[{dest}]=NULL"), |end| {
                    format!("r[{dest}..{end}]=NULL")
                }),
            ),
            Insn::NotFound {
                cursor_id,
                target_pc,
                record_reg,
                ..
            }
            | Insn::Found {
                cursor_id,
                target_pc,
                record_reg,
                ..
            } => (
                if matches!(insn, Insn::NotFound { .. }) {
                    "NotFound"
                } else {
                    "Found"
                },
                *cursor_id as i32,
                target_pc.as_debug_int(),
                *record_reg as i32,
                Value::build_text(""),
                0,
                format!(
                    "if {}found goto {}",
                    if matches!(insn, Insn::NotFound { .. }) {
                        "not "
                    } else {
                        ""
                    },
                    target_pc.as_debug_int()
                ),
            ),
            Insn::Affinity {
                start_reg,
                count,
                affinities,
            } => (
                "Affinity",
                *start_reg as i32,
                count.get() as i32,
                0,
                Value::build_text(""),
                0,
                format!(
                    "r[{}..{}] = {}",
                    start_reg,
                    start_reg + count.get(),
                    affinities
                        .chars()
                        .map(|a| a.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            ),
            Insn::Count {
                cursor_id,
                target_reg,
                exact,
            } => (
                "Count",
                *cursor_id as i32,
                *target_reg as i32,
                if *exact { 0 } else { 1 },
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::Int64 {
                _p1,
                out_reg,
                _p3,
                value,
            } => (
                "Int64",
                0,
                *out_reg as i32,
                0,
                Value::Integer(*value),
                0,
                format!("r[{}]={}", *out_reg, *value),
            ),
            Insn::IntegrityCk {
                max_errors,
                roots,
                message_register,
            } => (
                "IntegrityCk",
                *max_errors as i32,
                0,
                0,
                Value::build_text(""),
                0,
                format!("roots={roots:?} message_register={message_register}"),
            ),
            Insn::RowData { cursor_id, dest } => (
                "RowData",
                *cursor_id as i32,
                *dest as i32,
                0,
                Value::build_text(""),
                0,
                format!("r[{}] = data", *dest),
            ),
        };
    format!(
        "{:<4}  {:<17}  {:<4}  {:<4}  {:<4}  {:<13}  {:<2}  {}",
        addr,
        &(indent + opcode),
        p1,
        p2,
        p3,
        p4.to_string(),
        p5,
        manual_comment.map_or(comment.to_string(), |mc| format!("{comment}; {mc}"))
    )
}
