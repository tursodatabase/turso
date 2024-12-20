use super::{Insn, InsnReference, OwnedValue, Program};
use crate::types::LimboText;
use std::rc::Rc;

use super::Explain;

pub fn insn_to_str(
    program: &Program,
    addr: InsnReference,
    insn: &Insn,
    indent: String,
    manual_comment: Option<&'static str>,
) -> String {
    let opcode: &str = insn.explain_name();
    let p1 = insn.explain_p1();
    let p2 = insn.explain_p2();
    let p3 = insn.explain_p3();

    let p4 = match insn {
        Insn::SorterInsert { .. } => OwnedValue::Integer(0),
        Insn::Real { value, .. } => OwnedValue::Float(*value),
        Insn::String8 { value, .. } => OwnedValue::build_text(Rc::new(value.clone())),
        Insn::Blob { value, .. } => OwnedValue::Blob(Rc::new(value.clone())),
        Insn::AggStep { func, .. } => OwnedValue::build_text(Rc::new(func.to_string().into())),
        Insn::AggFinal { func, .. } => OwnedValue::build_text(Rc::new(func.to_string().into())),
        Insn::SorterOpen { order, .. } => {
            let _p4 = String::new();
            let to_print: Vec<String> = order
                .values
                .iter()
                .map(|v| match v {
                    OwnedValue::Integer(i) => {
                        if *i == 0 {
                            "B".to_string()
                        } else {
                            "-B".to_string()
                        }
                    }
                    _ => unreachable!(),
                })
                .collect();
            OwnedValue::build_text(Rc::new(format!(
                "k({},{})",
                order.values.len(),
                to_print.join(",")
            )))
        }
        _ => OwnedValue::build_text(Rc::new("".to_string())),
    };

    let p5 = match insn {
        Insn::InsertAsync { flag, .. } => *flag as u16,
        _ => 0,
    };

    let comment: String = match insn {
        Insn::Init { target_pc } => format!("Start at {}", target_pc),
        Insn::Add { lhs, rhs, dest } => format!("r[{}]=r[{}]+r[{}]", dest, lhs, rhs),
        Insn::Subtract { lhs, rhs, dest } => format!("r[{}]=r[{}]-r[{}]", dest, lhs, rhs),
        Insn::Multiply { lhs, rhs, dest } => format!("r[{}]=r[{}]*r[{}]", dest, lhs, rhs),
        Insn::Divide { lhs, rhs, dest } => format!("r[{}]=r[{}]/r[{}]", dest, lhs, rhs),
        Insn::BitAnd { lhs, rhs, dest } => format!("r[{}]=r[{}]&r[{}]", dest, lhs, rhs),
        Insn::BitOr { lhs, rhs, dest } => format!("r[{}]=r[{}]|r[{}]", dest, lhs, rhs),
        Insn::BitNot { reg, dest } => format!("r[{}]=~r[{}]", dest, reg),
        Insn::Null { dest, dest_end } => dest_end.map_or(format!("r[{}]=NULL", dest), |end| {
            format!("r[{}..{}]=NULL", dest, end)
        }),
        Insn::NullRow { cursor_id } => {
            format!("Set cursor {} to a (pseudo) NULL row", cursor_id)
        }
        Insn::NotNull { reg, target_pc } => format!("r[{}]!=NULL -> goto {}", reg, target_pc),
        Insn::Compare {
            start_reg_a,
            start_reg_b,
            count,
        } => format!(
            "r[{}..{}]==r[{}..{}]",
            start_reg_a,
            start_reg_a + (count - 1),
            start_reg_b,
            start_reg_b + (count - 1)
        ),
        Insn::Move {
            source_reg,
            dest_reg,
            count,
        } => format!(
            "r[{}..{}]=r[{}..{}]",
            dest_reg,
            dest_reg + (count - 1),
            source_reg,
            source_reg + (count - 1)
        ),
        Insn::IfPos {
            reg,
            target_pc,
            decrement_by,
        } => format!(
            "r[{}]>0 -> r[{}]-={}, goto {}",
            reg, reg, decrement_by, target_pc
        ),
        Insn::Eq {
            lhs,
            rhs,
            target_pc,
        } => format!("if r[{}]==r[{}] goto {}", lhs, rhs, target_pc),
        Insn::Ne {
            lhs,
            rhs,
            target_pc,
        } => format!("if r[{}]!=r[{}] goto {}", lhs, rhs, target_pc),
        Insn::Lt {
            lhs,
            rhs,
            target_pc,
        } => format!("if r[{}]<r[{}] goto {}", lhs, rhs, target_pc),
        Insn::Le {
            lhs,
            rhs,
            target_pc,
        } => format!("if r[{}]<=r[{}] goto {}", lhs, rhs, target_pc),
        Insn::Gt {
            lhs,
            rhs,
            target_pc,
        } => format!("if r[{}]>r[{}] goto {}", lhs, rhs, target_pc),
        Insn::Ge {
            lhs,
            rhs,
            target_pc,
        } => format!("if r[{}]>=r[{}] goto {}", lhs, rhs, target_pc),
        Insn::If {
            reg,
            target_pc,
            null_reg: _,
        } => format!("if r[{}] goto {}", reg, target_pc),
        Insn::IfNot {
            reg,
            target_pc,
            null_reg: _,
        } => format!("if !r[{}] goto {}", reg, target_pc),
        Insn::OpenReadAsync {
            cursor_id,
            root_page,
        } => format!(
            "table={}, root={}",
            program.cursor_ref[*cursor_id]
                .0
                .as_ref()
                .unwrap_or(&format!("cursor {}", cursor_id)),
            root_page
        ),
        Insn::OpenPseudo {
            cursor_id: _,
            content_reg,
            num_fields,
        } => format!("{} columns in r[{}]", num_fields, content_reg),
        Insn::RewindAwait {
            cursor_id,
            pc_if_empty: _,
        } => format!(
            "Rewind table {}",
            program.cursor_ref[*cursor_id]
                .0
                .as_ref()
                .unwrap_or(&format!("cursor {}", cursor_id))
        ),
        Insn::Column {
            cursor_id,
            column,
            dest,
        } => {
            let (table_identifier, table) = &program.cursor_ref[*cursor_id];
            format!(
                "r[{}]={}.{}",
                dest,
                table_identifier
                    .as_ref()
                    .unwrap_or(&format!("cursor {}", cursor_id)),
                table
                    .as_ref()
                    .and_then(|x| x.column_index_to_name(*column))
                    .unwrap_or(format!("column {}", *column).as_str())
            )
        }
        Insn::MakeRecord {
            start_reg,
            count,
            dest_reg,
        } => format!(
            "r[{}]=mkrec(r[{}..{}])",
            dest_reg,
            start_reg,
            start_reg + count - 1,
        ),
        Insn::ResultRow { start_reg, count } => {
            if *count == 1 {
                format!("output=r[{}]", start_reg)
            } else {
                format!("output=r[{}..{}]", start_reg, start_reg + count - 1)
            }
        }
        Insn::Integer { value, dest } => format!("r[{}]={}", dest, value),
        Insn::Real { value, dest } => format!("r[{}]={}", dest, value),
        Insn::String8 { value, dest } => format!("r[{}]='{}'", dest, value),
        Insn::Blob { value, dest } => format!(
            "r[{}]={} (len={})",
            dest,
            String::from_utf8_lossy(value),
            value.len()
        ),
        Insn::RowId { cursor_id, dest } => format!(
            "r[{}]={}.rowid",
            dest,
            &program.cursor_ref[*cursor_id]
                .0
                .as_ref()
                .unwrap_or(&format!("cursor {}", cursor_id))
        ),
        Insn::SeekRowid {
            cursor_id,
            src_reg,
            target_pc,
        } => format!(
            "if (r[{}]!={}.rowid) goto {}",
            src_reg,
            &program.cursor_ref[*cursor_id]
                .0
                .as_ref()
                .unwrap_or(&format!("cursor {}", cursor_id)),
            target_pc
        ),
        Insn::DecrJumpZero { reg, target_pc } => {
            format!("if (--r[{}]==0) goto {}", reg, target_pc)
        }
        Insn::AggStep {
            func: _,
            acc_reg,
            delimiter: _,
            col,
        } => format!("accum=r[{}] step(r[{}])", *acc_reg, *col),
        Insn::AggFinal { register, func: _ } => format!("accum=r[{}]", *register),
        Insn::SorterOpen {
            cursor_id,
            columns: _,
            order: _,
        } => {
            format!("cursor={}", cursor_id)
        }
        Insn::SorterData {
            cursor_id: _,
            dest_reg,
            pseudo_cursor: _,
        } => format!("r[{}]=data", dest_reg),
        Insn::SorterInsert {
            cursor_id: _,
            record_reg,
        } => format!("key=r[{}]", record_reg),
        Insn::Function {
            constant_mask: _,
            start_reg,
            dest,
            func,
        } => {
            if func.arg_count == 0 {
                format!("r[{}]=func()", dest)
            } else if *start_reg == *start_reg + func.arg_count - 1 {
                format!("r[{}]=func(r[{}])", dest, start_reg)
            } else {
                format!(
                    "r[{}]=func(r[{}..{}])",
                    dest,
                    start_reg,
                    start_reg + func.arg_count - 1
                )
            }
        }
        Insn::Copy {
            src_reg,
            dst_reg,
            amount: _,
        } => format!("r[{}]=r[{}]", dst_reg, src_reg),
        Insn::CreateBtree { db, root, flags } => {
            format!("r[{}]=root iDb={} flags={}", root, db, flags)
        }
        Insn::IsNull { src, target_pc } => format!("if (r[{}]==NULL) goto {}", src, target_pc),
        Insn::ParseSchema {
            db: _,
            where_clause,
        } => where_clause.clone(),
        _ => "".to_string(),
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
        manual_comment.map_or(comment.to_string(), |mc| format!("{}; {}", comment, mc))
    )
}
