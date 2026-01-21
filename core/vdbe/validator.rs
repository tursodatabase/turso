//! Bytecode validator for the VDBE.
//!
//! This module validates VM bytecode programs by:
//! 1. Building a control flow graph from instructions
//! 2. Performing dataflow analysis to track register initialization
//! 3. Detecting uses of uninitialized registers

use std::collections::{BTreeSet, HashSet, VecDeque};

use crate::function::{AggFunc, Func};

use super::insn::Insn;
use super::BranchOffset;

/// Register access information for an instruction.
#[derive(Debug, Clone, Default)]
pub struct RegisterAccess {
    /// Registers read by this instruction.
    pub reads: Vec<usize>,
    /// Registers written by this instruction.
    pub writes: Vec<usize>,
}

impl RegisterAccess {
    fn new() -> Self {
        Self::default()
    }

    fn read(mut self, reg: usize) -> Self {
        self.reads.push(reg);
        self
    }

    fn read_range(mut self, start: usize, count: usize) -> Self {
        for i in 0..count {
            self.reads.push(start + i);
        }
        self
    }

    fn write(mut self, reg: usize) -> Self {
        self.writes.push(reg);
        self
    }

    fn write_range(mut self, start: usize, count: usize) -> Self {
        for i in 0..count {
            self.writes.push(start + i);
        }
        self
    }
}

/// Get register access information for an instruction.
pub fn get_register_access(insn: &Insn) -> RegisterAccess {
    match insn {
        Insn::Init { .. } => RegisterAccess::new(),

        Insn::Null { dest, dest_end } => {
            let mut access = RegisterAccess::new().write(*dest);
            if let Some(end) = dest_end {
                for reg in (*dest + 1)..=*end {
                    access = access.write(reg);
                }
            }
            access
        }

        Insn::BeginSubrtn { dest, dest_end } => {
            let mut access = RegisterAccess::new().write(*dest);
            if let Some(end) = dest_end {
                for reg in (*dest + 1)..=*end {
                    access = access.write(reg);
                }
            }
            access
        }

        Insn::NullRow { .. } => RegisterAccess::new(),

        Insn::Add { lhs, rhs, dest } => RegisterAccess::new().read(*lhs).read(*rhs).write(*dest),

        Insn::Subtract { lhs, rhs, dest } => {
            RegisterAccess::new().read(*lhs).read(*rhs).write(*dest)
        }

        Insn::Multiply { lhs, rhs, dest } => {
            RegisterAccess::new().read(*lhs).read(*rhs).write(*dest)
        }

        Insn::Divide { lhs, rhs, dest } => RegisterAccess::new().read(*lhs).read(*rhs).write(*dest),

        Insn::Remainder { lhs, rhs, dest } => {
            RegisterAccess::new().read(*lhs).read(*rhs).write(*dest)
        }

        Insn::BitAnd { lhs, rhs, dest } => RegisterAccess::new().read(*lhs).read(*rhs).write(*dest),

        Insn::BitOr { lhs, rhs, dest } => RegisterAccess::new().read(*lhs).read(*rhs).write(*dest),

        Insn::BitNot { reg, dest } => RegisterAccess::new().read(*reg).write(*dest),

        Insn::ShiftRight { lhs, rhs, dest } => {
            RegisterAccess::new().read(*lhs).read(*rhs).write(*dest)
        }

        Insn::ShiftLeft { lhs, rhs, dest } => {
            RegisterAccess::new().read(*lhs).read(*rhs).write(*dest)
        }

        Insn::Concat { lhs, rhs, dest } => RegisterAccess::new().read(*lhs).read(*rhs).write(*dest),

        Insn::And { lhs, rhs, dest } => RegisterAccess::new().read(*lhs).read(*rhs).write(*dest),

        Insn::Or { lhs, rhs, dest } => RegisterAccess::new().read(*lhs).read(*rhs).write(*dest),

        Insn::Not { reg, dest } => RegisterAccess::new().read(*reg).write(*dest),

        Insn::MemMax { dest_reg, src_reg } => RegisterAccess::new()
            .read(*src_reg)
            .read(*dest_reg)
            .write(*dest_reg),

        Insn::Compare {
            start_reg_a,
            start_reg_b,
            count,
            ..
        } => RegisterAccess::new()
            .read_range(*start_reg_a, *count)
            .read_range(*start_reg_b, *count),

        Insn::Jump { .. } => RegisterAccess::new(),

        Insn::Move {
            source_reg,
            dest_reg,
            count,
        } => RegisterAccess::new()
            .read_range(*source_reg, *count)
            .write_range(*dest_reg, *count)
            .write_range(*source_reg, *count), // source becomes NULL

        Insn::IfPos { reg, .. } => RegisterAccess::new().read(*reg).write(*reg),

        Insn::IfNeg { reg, .. } => RegisterAccess::new().read(*reg),

        Insn::NotNull { reg, .. } => RegisterAccess::new().read(*reg),

        Insn::Eq { lhs, rhs, .. } => RegisterAccess::new().read(*lhs).read(*rhs),
        Insn::Ne { lhs, rhs, .. } => RegisterAccess::new().read(*lhs).read(*rhs),
        Insn::Lt { lhs, rhs, .. } => RegisterAccess::new().read(*lhs).read(*rhs),
        Insn::Le { lhs, rhs, .. } => RegisterAccess::new().read(*lhs).read(*rhs),
        Insn::Gt { lhs, rhs, .. } => RegisterAccess::new().read(*lhs).read(*rhs),
        Insn::Ge { lhs, rhs, .. } => RegisterAccess::new().read(*lhs).read(*rhs),

        Insn::If { reg, .. } => RegisterAccess::new().read(*reg),
        Insn::IfNot { reg, .. } => RegisterAccess::new().read(*reg),

        Insn::IsNull { reg, .. } => RegisterAccess::new().read(*reg),
        Insn::IsTrue { reg, dest, .. } => RegisterAccess::new().read(*reg).write(*dest),

        Insn::Filter {
            key_reg, num_keys, ..
        } => RegisterAccess::new().read_range(*key_reg, *num_keys),

        Insn::FilterAdd {
            key_reg, num_keys, ..
        } => RegisterAccess::new().read_range(*key_reg, *num_keys),

        Insn::OpenRead { .. } => RegisterAccess::new(),
        Insn::OpenWrite { .. } => RegisterAccess::new(),
        Insn::OpenPseudo { .. } => RegisterAccess::new(), // tursodb do not use content_reg for now
        Insn::OpenEphemeral { .. } => RegisterAccess::new(),
        Insn::OpenAutoindex { .. } => RegisterAccess::new(),
        Insn::OpenDup { .. } => RegisterAccess::new(),

        Insn::Close { .. } => RegisterAccess::new(),

        Insn::Rewind { .. } => RegisterAccess::new(),
        Insn::Last { .. } => RegisterAccess::new(),
        Insn::Next { .. } => RegisterAccess::new(),
        Insn::Prev { .. } => RegisterAccess::new(),

        Insn::Column { dest, .. } => RegisterAccess::new().write(*dest),

        Insn::TypeCheck {
            start_reg, count, ..
        } => RegisterAccess::new().read_range(*start_reg, *count),

        Insn::MakeRecord {
            start_reg,
            count,
            dest_reg,
            ..
        } => RegisterAccess::new()
            .read_range(*start_reg as usize, *count as usize)
            .write(*dest_reg as usize),

        Insn::ResultRow { start_reg, count } => {
            RegisterAccess::new().read_range(*start_reg, *count)
        }

        Insn::Halt { .. } => RegisterAccess::new(),
        Insn::HaltIfNull { target_reg, .. } => RegisterAccess::new().read(*target_reg),

        Insn::Transaction { .. } => RegisterAccess::new(),
        Insn::AutoCommit { .. } => RegisterAccess::new(),

        Insn::Goto { .. } => RegisterAccess::new(),

        Insn::Gosub { return_reg, .. } => RegisterAccess::new().write(*return_reg),

        Insn::Return { return_reg, .. } => RegisterAccess::new().read(*return_reg),

        Insn::Integer { dest, .. } => RegisterAccess::new().write(*dest),
        Insn::Int64 { out_reg, .. } => RegisterAccess::new().write(*out_reg),
        Insn::Real { dest, .. } => RegisterAccess::new().write(*dest),
        Insn::String8 { dest, .. } => RegisterAccess::new().write(*dest),
        Insn::Blob { dest, .. } => RegisterAccess::new().write(*dest),

        Insn::RealAffinity { register } => RegisterAccess::new().read(*register).write(*register),

        Insn::RowData { dest, .. } => RegisterAccess::new().write(*dest),
        Insn::RowId { dest, .. } => RegisterAccess::new().write(*dest),
        Insn::IdxRowId { dest, .. } => RegisterAccess::new().write(*dest),

        Insn::SeekRowid { src_reg, .. } => RegisterAccess::new().read(*src_reg),
        Insn::SeekEnd { .. } => RegisterAccess::new(),

        Insn::DeferredSeek { .. } => RegisterAccess::new(),

        Insn::SeekGE {
            start_reg,
            num_regs,
            ..
        } => RegisterAccess::new().read_range(*start_reg, *num_regs),

        Insn::SeekGT {
            start_reg,
            num_regs,
            ..
        } => RegisterAccess::new().read_range(*start_reg, *num_regs),

        Insn::SeekLE {
            start_reg,
            num_regs,
            ..
        } => RegisterAccess::new().read_range(*start_reg, *num_regs),

        Insn::SeekLT {
            start_reg,
            num_regs,
            ..
        } => RegisterAccess::new().read_range(*start_reg, *num_regs),

        Insn::IdxGE {
            start_reg,
            num_regs,
            ..
        } => RegisterAccess::new().read_range(*start_reg, *num_regs),

        Insn::IdxGT {
            start_reg,
            num_regs,
            ..
        } => RegisterAccess::new().read_range(*start_reg, *num_regs),

        Insn::IdxLE {
            start_reg,
            num_regs,
            ..
        } => RegisterAccess::new().read_range(*start_reg, *num_regs),

        Insn::IdxLT {
            start_reg,
            num_regs,
            ..
        } => RegisterAccess::new().read_range(*start_reg, *num_regs),

        Insn::IdxInsert {
            record_reg,
            unpacked_start,
            unpacked_count,
            ..
        } => {
            let mut access = RegisterAccess::new().read(*record_reg);
            if let (Some(start), Some(count)) = (unpacked_start, unpacked_count) {
                access = access.read_range(*start, *count as usize);
            }
            access
        }

        Insn::IdxDelete {
            start_reg,
            num_regs,
            ..
        } => RegisterAccess::new().read_range(*start_reg, *num_regs),

        Insn::DecrJumpZero { reg, .. } => RegisterAccess::new().read(*reg).write(*reg),

        Insn::AggStep {
            acc_reg,
            col,
            delimiter,
            func,
        } => {
            let use_delimiter = match func {
                AggFunc::GroupConcat | AggFunc::StringAgg => true,
                #[cfg(feature = "json")]
                AggFunc::JsonGroupObject | AggFunc::JsonbGroupObject => true,
                _ => false,
            };
            let mut access = RegisterAccess::new()
                .read(*col)
                .read(*acc_reg)
                .write(*acc_reg);
            if use_delimiter {
                access = access.read(*delimiter)
            }
            access
        }

        Insn::AggFinal { register, .. } => RegisterAccess::new().read(*register).write(*register),

        Insn::AggValue {
            acc_reg, dest_reg, ..
        } => RegisterAccess::new().read(*acc_reg).write(*dest_reg),

        Insn::SorterOpen { .. } => RegisterAccess::new(),

        Insn::SorterInsert { record_reg, .. } => RegisterAccess::new().read(*record_reg),

        Insn::SorterCompare {
            sorted_record_reg, ..
        } => RegisterAccess::new().read(*sorted_record_reg),

        Insn::SorterSort { .. } => RegisterAccess::new(),

        Insn::SorterData { dest_reg, .. } => RegisterAccess::new().write(*dest_reg),

        Insn::SorterNext { .. } => RegisterAccess::new(),

        Insn::RowSetAdd {
            rowset_reg,
            value_reg,
        } => RegisterAccess::new().read(*rowset_reg).read(*value_reg),

        Insn::RowSetRead {
            rowset_reg,
            dest_reg,
            ..
        } => RegisterAccess::new().read(*rowset_reg).write(*dest_reg),

        Insn::RowSetTest {
            rowset_reg,
            value_reg,
            ..
        } => RegisterAccess::new().read(*rowset_reg).read(*value_reg),

        Insn::Function {
            start_reg,
            dest,
            func,
            ..
        } => {
            let arg_count = func.arg_count;
            let mut access = RegisterAccess::new()
                .read_range(*start_reg, arg_count)
                .write(*dest);
            if matches!(func.func, Func::AlterTable(..)) {
                access = access.write_range(*dest, 5);
            }
            access
        }

        Insn::Cast { reg, .. } => RegisterAccess::new().read(*reg).write(*reg),

        Insn::InitCoroutine { yield_reg, .. } => RegisterAccess::new().write(*yield_reg),

        Insn::EndCoroutine { yield_reg } => RegisterAccess::new().read(*yield_reg),

        Insn::Yield { yield_reg, .. } => RegisterAccess::new().read(*yield_reg).write(*yield_reg),

        Insn::Insert {
            key_reg,
            record_reg,
            ..
        } => RegisterAccess::new().read(*key_reg).read(*record_reg),

        Insn::Delete { .. } => RegisterAccess::new(),

        Insn::NewRowid {
            rowid_reg,
            prev_largest_reg,
            ..
        } => {
            let mut access = RegisterAccess::new().write(*rowid_reg);
            if *prev_largest_reg > 0 {
                access = access.write(*prev_largest_reg);
            }
            access
        }

        Insn::MustBeInt { reg } => RegisterAccess::new().read(*reg),

        Insn::SoftNull { reg } => RegisterAccess::new().write(*reg),

        Insn::NoConflict {
            record_reg,
            num_regs,
            ..
        } => RegisterAccess::new().read_range(*record_reg, *num_regs),

        Insn::NotExists { rowid_reg, .. } => RegisterAccess::new().read(*rowid_reg),

        Insn::OffsetLimit {
            limit_reg,
            combined_reg,
            offset_reg,
        } => RegisterAccess::new()
            .read(*limit_reg)
            .read(*offset_reg)
            .write(*combined_reg),

        Insn::Copy {
            src_reg,
            dst_reg,
            extra_amount,
        } => RegisterAccess::new()
            .read_range(*src_reg, *extra_amount + 1)
            .write_range(*dst_reg, *extra_amount + 1),

        Insn::CreateBtree { root, .. } => RegisterAccess::new().write(*root),

        Insn::Destroy {
            former_root_reg, ..
        } => RegisterAccess::new().write(*former_root_reg),

        Insn::ResetSorter { .. } => RegisterAccess::new(),

        Insn::DropTable { .. } => RegisterAccess::new(),
        Insn::DropView { .. } => RegisterAccess::new(),
        Insn::DropIndex { .. } => RegisterAccess::new(),
        Insn::DropTrigger { .. } => RegisterAccess::new(),

        Insn::CollSeq { reg, .. } => {
            if let Some(r) = reg {
                RegisterAccess::new().write(*r)
            } else {
                RegisterAccess::new()
            }
        }

        Insn::ParseSchema { .. } => RegisterAccess::new(),
        Insn::PopulateMaterializedViews { .. } => RegisterAccess::new(),

        Insn::AddImm { register, .. } => RegisterAccess::new().read(*register).write(*register),

        Insn::Variable { dest, .. } => RegisterAccess::new().write(*dest),

        Insn::ZeroOrNull { rg1, rg2, dest } => {
            RegisterAccess::new().read(*rg1).read(*rg2).write(*dest)
        }

        Insn::Noop => RegisterAccess::new(),

        Insn::PageCount { dest, .. } => RegisterAccess::new().write(*dest),

        Insn::ReadCookie { dest, .. } => RegisterAccess::new().write(*dest),

        Insn::SetCookie { .. } => RegisterAccess::new(),

        Insn::Once { .. } => RegisterAccess::new(),

        Insn::Found {
            record_reg,
            num_regs,
            ..
        } => RegisterAccess::new().read_range(*record_reg, *num_regs),

        Insn::NotFound {
            record_reg,
            num_regs,
            ..
        } => RegisterAccess::new().read_range(*record_reg, *num_regs),

        Insn::Affinity {
            start_reg, count, ..
        } => {
            let count_val = count.get();
            RegisterAccess::new()
                .read_range(*start_reg, count_val)
                .write_range(*start_reg, count_val)
        }

        Insn::Count { target_reg, .. } => RegisterAccess::new().write(*target_reg),

        Insn::IntegrityCk {
            message_register, ..
        } => RegisterAccess::new().write(*message_register),

        Insn::RenameTable { .. } => RegisterAccess::new(),
        Insn::DropColumn { .. } => RegisterAccess::new(),
        Insn::AddColumn { .. } => RegisterAccess::new(),
        Insn::AlterColumn { .. } => RegisterAccess::new(),

        Insn::MaxPgcnt { dest, .. } => RegisterAccess::new().write(*dest),
        Insn::JournalMode { dest, .. } => RegisterAccess::new().write(*dest),

        Insn::Checkpoint { dest, .. } => RegisterAccess::new().write_range(*dest, 3),

        Insn::Sequence { target_reg, .. } => RegisterAccess::new().write(*target_reg),
        Insn::SequenceTest { value_reg, .. } => RegisterAccess::new().read(*value_reg),

        Insn::Explain { .. } => RegisterAccess::new(),

        Insn::FkCounter { .. } => RegisterAccess::new(),
        Insn::FkIfZero { .. } => RegisterAccess::new(),

        Insn::Program { .. } => RegisterAccess::new(),

        // Virtual table operations
        Insn::VOpen { .. } => RegisterAccess::new(),
        Insn::VCreate { args_reg, .. } => {
            if let Some(reg) = args_reg {
                RegisterAccess::new().read(*reg)
            } else {
                RegisterAccess::new()
            }
        }
        Insn::VFilter { args_reg, .. } => RegisterAccess::new().read(*args_reg),
        Insn::VColumn { dest, .. } => RegisterAccess::new().write(*dest),
        Insn::VUpdate {
            start_reg,
            arg_count,
            ..
        } => RegisterAccess::new().read_range(*start_reg, *arg_count),
        Insn::VNext { .. } => RegisterAccess::new(),
        Insn::VDestroy { .. } => RegisterAccess::new(),
        Insn::VBegin { .. } => RegisterAccess::new(),
        Insn::VRename { new_name_reg, .. } => RegisterAccess::new().read(*new_name_reg),

        // Index method operations
        Insn::IndexMethodCreate { .. } => RegisterAccess::new(),
        Insn::IndexMethodDestroy { .. } => RegisterAccess::new(),
        Insn::IndexMethodOptimize { .. } => RegisterAccess::new(),
        Insn::IndexMethodQuery {
            start_reg,
            count_reg,
            ..
        } => RegisterAccess::new().read(*start_reg).read(*count_reg),

        // Hash operations
        Insn::HashBuild { data } => {
            let mut access = RegisterAccess::new().read_range(data.key_start_reg, data.num_keys);
            if let Some(payload_start) = data.payload_start_reg {
                access = access.read_range(payload_start, data.num_payload);
            }
            access
        }

        Insn::HashDistinct { data } => {
            RegisterAccess::new().read_range(data.key_start_reg, data.num_keys)
        }

        Insn::HashBuildFinalize { .. } => RegisterAccess::new(),

        Insn::HashProbe {
            key_start_reg,
            num_keys,
            dest_reg,
            payload_dest_reg,
            num_payload,
            ..
        } => {
            let mut access = RegisterAccess::new()
                .read_range(*key_start_reg as usize, *num_keys as usize)
                .write(*dest_reg as usize);
            if let Some(payload_dest) = payload_dest_reg {
                access = access.write_range(*payload_dest as usize, *num_payload as usize);
            }
            access
        }

        Insn::HashNext {
            dest_reg,
            payload_dest_reg,
            num_payload,
            ..
        } => {
            let mut access = RegisterAccess::new().write(*dest_reg);
            if let Some(payload_dest) = payload_dest_reg {
                access = access.write_range(*payload_dest, *num_payload);
            }
            access
        }

        Insn::HashClose { .. } => RegisterAccess::new(),
        Insn::HashClear { .. } => RegisterAccess::new(),
    }
}

/// Get successor instruction addresses for control flow.
fn get_successors(insn: &Insn, pc: usize) -> Vec<usize> {
    fn offset_to_usize(offset: &BranchOffset) -> Option<usize> {
        match offset {
            BranchOffset::Offset(o) => Some(*o as usize),
            BranchOffset::Label(_) | BranchOffset::Placeholder => None,
        }
    }

    fn branch(pc: usize, target: &BranchOffset) -> Vec<usize> {
        let mut result = vec![pc + 1];
        if let Some(t) = offset_to_usize(target) {
            result.push(t);
        }
        result
    }

    match insn {
        // Unconditional jumps - no fallthrough
        Insn::Goto { target_pc } | Insn::Init { target_pc } | Insn::Gosub { target_pc, .. } => {
            offset_to_usize(target_pc).into_iter().collect()
        }

        // Terminal instructions
        Insn::Halt { .. } | Insn::EndCoroutine { .. } => vec![],

        // Return - dynamic target from register
        Insn::Return {
            can_fallthrough, ..
        } => {
            if *can_fallthrough {
                vec![pc + 1]
            } else {
                vec![]
            }
        }

        // Conditional jumps - fallthrough + jump target
        Insn::Eq { target_pc, .. }
        | Insn::Ne { target_pc, .. }
        | Insn::Lt { target_pc, .. }
        | Insn::Le { target_pc, .. }
        | Insn::Gt { target_pc, .. }
        | Insn::Ge { target_pc, .. }
        | Insn::If { target_pc, .. }
        | Insn::IfNot { target_pc, .. }
        | Insn::IfPos { target_pc, .. }
        | Insn::IfNeg { target_pc, .. }
        | Insn::NotNull { target_pc, .. }
        | Insn::IsNull { target_pc, .. }
        | Insn::SeekRowid { target_pc, .. }
        | Insn::SeekGE { target_pc, .. }
        | Insn::SeekGT { target_pc, .. }
        | Insn::SeekLE { target_pc, .. }
        | Insn::SeekLT { target_pc, .. }
        | Insn::IdxGE { target_pc, .. }
        | Insn::IdxGT { target_pc, .. }
        | Insn::IdxLE { target_pc, .. }
        | Insn::IdxLT { target_pc, .. }
        | Insn::DecrJumpZero { target_pc, .. }
        | Insn::Found { target_pc, .. }
        | Insn::NotFound { target_pc, .. }
        | Insn::NoConflict { target_pc, .. }
        | Insn::NotExists { target_pc, .. }
        | Insn::Filter { target_pc, .. }
        | Insn::FkIfZero { target_pc, .. }
        | Insn::HashProbe { target_pc, .. }
        | Insn::HashNext { target_pc, .. }
        | Insn::SequenceTest { target_pc, .. } => branch(pc, target_pc),

        // Three-way jump based on comparison result
        Insn::Jump {
            target_pc_lt,
            target_pc_eq,
            target_pc_gt,
        } => [target_pc_lt, target_pc_eq, target_pc_gt]
            .iter()
            .filter_map(|t| offset_to_usize(t))
            .collect(),

        // Loop control
        Insn::Rewind { pc_if_empty, .. }
        | Insn::Last { pc_if_empty, .. }
        | Insn::SorterSort { pc_if_empty, .. }
        | Insn::RowSetRead { pc_if_empty, .. }
        | Insn::VFilter { pc_if_empty, .. }
        | Insn::IndexMethodQuery { pc_if_empty, .. } => branch(pc, pc_if_empty),

        Insn::Next { pc_if_next, .. }
        | Insn::SorterNext { pc_if_next, .. }
        | Insn::VNext { pc_if_next, .. } => branch(pc, pc_if_next),

        Insn::Prev { pc_if_prev, .. } => branch(pc, pc_if_prev),

        Insn::SorterCompare {
            pc_when_nonequal, ..
        } => branch(pc, pc_when_nonequal),

        // Coroutine operations
        Insn::InitCoroutine {
            jump_on_definition, ..
        } => branch(pc, jump_on_definition),

        Insn::Yield { end_offset, .. } => branch(pc, end_offset),

        // Once - first time fallthrough, otherwise jump
        Insn::Once {
            target_pc_when_reentered,
        } => branch(pc, target_pc_when_reentered),

        // RowSet test
        Insn::RowSetTest { pc_if_found, .. } => branch(pc, pc_if_found),

        // Hash operations
        Insn::HashDistinct { data } => branch(pc, &data.target_pc),

        // All other instructions just fall through
        _ => vec![pc + 1],
    }
}

/// A validation error indicating use of an uninitialized register.
#[derive(Debug, Clone)]
pub struct UninitializedRegisterError {
    /// The instruction address where the error occurred.
    pub pc: usize,
    /// The register that was read before being initialized.
    pub register: usize,
    /// The instruction that attempted to read the uninitialized register.
    pub insn: String,
}

impl std::fmt::Display for UninitializedRegisterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Uninitialized register r{} read at pc={} ({})",
            self.register, self.pc, self.insn
        )
    }
}

/// Validates that all registers are initialized before use.
///
/// Uses forward dataflow analysis to track which registers are definitely
/// initialized at each program point. Reports any reads of potentially
/// uninitialized registers.
pub fn validate_register_initialization(
    insns: &[(Insn, usize)],
) -> Result<(), Vec<UninitializedRegisterError>> {
    if insns
        .iter()
        .any(|(i, _)| matches!(i, Insn::InitCoroutine { .. } | Insn::BeginSubrtn { .. }))
    {
        // coroutines are hard to analyze - skip them for now
        return Ok(());
    }
    if insns.is_empty() {
        return Ok(());
    }

    let num_insns = insns.len();

    // initialized_at[pc] = set of registers that are definitely initialized
    // when reaching pc (before executing the instruction at pc)
    let mut initialized_at: Vec<Option<BTreeSet<usize>>> = vec![None; num_insns];

    // Worklist algorithm for forward dataflow analysis
    let mut worklist: VecDeque<usize> = VecDeque::new();
    let mut in_worklist: HashSet<usize> = HashSet::new();

    // Start from instruction 0
    initialized_at[0] = Some(BTreeSet::new());
    worklist.push_back(0);
    in_worklist.insert(0);

    while let Some(pc) = worklist.pop_front() {
        in_worklist.remove(&pc);

        if pc >= num_insns {
            continue;
        }

        let (insn, _) = &insns[pc];
        let access = get_register_access(insn);

        // Compute the set of initialized registers after this instruction
        let mut after = initialized_at[pc].clone().unwrap();
        for reg in &access.writes {
            after.insert(*reg);
        }
        tracing::info!(
            "pc: {}, before: {:?}, after: {:?}",
            pc,
            initialized_at[pc],
            after
        );

        tracing::info!("insn: {:?}, succ={:?}", insn, get_successors(insn, pc));
        // Propagate to successors
        for target in get_successors(insn, pc) {
            if target >= num_insns {
                continue;
            }
            tracing::info!("target: {target}");

            // Merge: intersection of initialized registers
            // A register is initialized at target only if it's initialized
            // on ALL paths leading to target
            let old_set = initialized_at[target].clone();

            if initialized_at[target].is_none() {
                // First time visiting this target
                initialized_at[target] = Some(after.clone());
            } else {
                // Intersection with existing
                initialized_at[target] = Some(
                    initialized_at[target]
                        .as_ref()
                        .unwrap()
                        .intersection(&after)
                        .copied()
                        .collect(),
                );
            }

            // If the set changed, add to worklist
            if initialized_at[target] != old_set {
                if !in_worklist.contains(&target) {
                    worklist.push_back(target);
                    in_worklist.insert(target);
                }
            }
        }
    }

    // Now check for uninitialized reads
    let mut errors = Vec::new();

    // Track which instructions are reachable
    let mut reachable: HashSet<usize> = HashSet::new();
    let mut reach_worklist: VecDeque<usize> = VecDeque::new();
    reach_worklist.push_back(0);
    reachable.insert(0);

    while let Some(pc) = reach_worklist.pop_front() {
        if pc >= num_insns {
            continue;
        }

        let (insn, _) = &insns[pc];
        for target in get_successors(insn, pc) {
            if target < num_insns && !reachable.contains(&target) {
                reachable.insert(target);
                reach_worklist.push_back(target);
            }
        }
    }

    // Check each reachable instruction
    for pc in 0..num_insns {
        if !reachable.contains(&pc) {
            continue;
        }

        let (insn, _) = &insns[pc];
        let access = get_register_access(insn);

        for reg in &access.reads {
            if !initialized_at[pc].as_ref().unwrap().contains(reg) {
                errors.push(UninitializedRegisterError {
                    pc,
                    register: *reg,
                    insn: format!("{:?}", insn),
                });
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

/// Debug assertion that validates register initialization.
/// Only performs validation in debug builds.
/// Panics with a detailed error message if validation fails.
#[cfg(debug_assertions)]
pub fn debug_assert_valid_program(insns: &[(Insn, usize)], sql: &str) {
    if sql.contains("LIMIT") {
        if let Err(errors) = validate_register_initialization(insns) {
            let mut msg = format!(
                "Bytecode validation failed for SQL: {}\n\nUninitialized register errors:\n",
                sql
            );
            for err in &errors {
                msg.push_str(&format!("  - {}\n", err));
            }

            // Also print the bytecode for debugging
            msg.push_str("\nBytecode:\n");
            for (i, (insn, _)) in insns.iter().enumerate() {
                msg.push_str(&format!("  {:4}: {:?}\n", i, insn));
            }

            panic!("{}", msg);
        }
    }
}

/// No-op in release builds.
#[cfg(not(debug_assertions))]
#[inline]
pub fn debug_assert_valid_program(_insns: &[(Insn, usize)], _sql: &str) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_valid_program() {
        // Integer 42 -> r0
        // ResultRow r0, 1
        // Halt
        let insns = vec![
            (Insn::Integer { value: 42, dest: 0 }, 0),
            (
                Insn::ResultRow {
                    start_reg: 0,
                    count: 1,
                },
                1,
            ),
            (
                Insn::Halt {
                    err_code: 0,
                    description: String::new(),
                },
                2,
            ),
        ];

        assert!(validate_register_initialization(&insns).is_ok());
    }

    #[test]
    fn test_uninitialized_register() {
        // ResultRow r0, 1  -- r0 not initialized!
        // Halt
        let insns = vec![
            (
                Insn::ResultRow {
                    start_reg: 0,
                    count: 1,
                },
                0,
            ),
            (
                Insn::Halt {
                    err_code: 0,
                    description: String::new(),
                },
                1,
            ),
        ];

        let result = validate_register_initialization(&insns);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].pc, 0);
        assert_eq!(errors[0].register, 0);
    }

    #[test]
    fn test_conditional_both_paths_init() {
        // Integer 1 -> r0
        // If r0, jump to 4
        // Integer 10 -> r1  (fallthrough path)
        // Goto 5
        // Integer 20 -> r1  (jump path)
        // ResultRow r1, 1
        // Halt
        let insns = vec![
            (Insn::Integer { value: 1, dest: 0 }, 0),
            (
                Insn::If {
                    reg: 0,
                    target_pc: BranchOffset::Offset(4),
                    jump_if_null: false,
                },
                1,
            ),
            (Insn::Integer { value: 10, dest: 1 }, 2),
            (
                Insn::Goto {
                    target_pc: BranchOffset::Offset(5),
                },
                3,
            ),
            (Insn::Integer { value: 20, dest: 1 }, 4),
            (
                Insn::ResultRow {
                    start_reg: 1,
                    count: 1,
                },
                5,
            ),
            (
                Insn::Halt {
                    err_code: 0,
                    description: String::new(),
                },
                6,
            ),
        ];

        assert!(validate_register_initialization(&insns).is_ok());
    }

    #[test]
    fn test_conditional_one_path_missing() {
        // Integer 1 -> r0
        // If r0, jump to 4
        // Integer 10 -> r1  (fallthrough path initializes r1)
        // Goto 4
        // ResultRow r1, 1  (but jump path doesn't initialize r1!)
        // Halt
        let insns = vec![
            (Insn::Integer { value: 1, dest: 0 }, 0),
            (
                Insn::If {
                    reg: 0,
                    target_pc: BranchOffset::Offset(4),
                    jump_if_null: false,
                },
                1,
            ),
            (Insn::Integer { value: 10, dest: 1 }, 2),
            (
                Insn::Goto {
                    target_pc: BranchOffset::Offset(4),
                },
                3,
            ),
            (
                Insn::ResultRow {
                    start_reg: 1,
                    count: 1,
                },
                4,
            ),
            (
                Insn::Halt {
                    err_code: 0,
                    description: String::new(),
                },
                5,
            ),
        ];

        let result = validate_register_initialization(&insns);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].pc, 4);
        assert_eq!(errors[0].register, 1);
    }

    #[test]
    fn test_null_initializes() {
        // Null -> r0, r2  (range)
        // Add r0, r1 -> r2  -- r1 not initialized!
        // Halt
        let insns = vec![
            (
                Insn::Null {
                    dest: 0,
                    dest_end: Some(2),
                },
                0,
            ),
            (
                Insn::Add {
                    lhs: 0,
                    rhs: 1,
                    dest: 2,
                },
                1,
            ),
            (
                Insn::Halt {
                    err_code: 0,
                    description: String::new(),
                },
                2,
            ),
        ];

        // r0, r1, r2 should all be initialized by Null
        assert!(validate_register_initialization(&insns).is_ok());
    }

    #[test]
    fn test_loop_invariant() {
        // Integer 10 -> r0
        // Integer 0 -> r1
        // loop: If r0 <= 0, jump to end
        // Add r1, r0 -> r1
        // Subtract r0, 1 -> r0  (simulate decrement)
        // Goto loop
        // end: ResultRow r1, 1
        // Halt
        let insns = vec![
            (Insn::Integer { value: 10, dest: 0 }, 0),
            (Insn::Integer { value: 0, dest: 1 }, 1),
            (Insn::Integer { value: 1, dest: 2 }, 2), // constant 1 for subtraction
            (
                Insn::IfPos {
                    reg: 0,
                    target_pc: BranchOffset::Offset(7),
                    decrement_by: 0,
                },
                3,
            ), // if r0 <= 0, jump to end
            (
                Insn::Add {
                    lhs: 1,
                    rhs: 0,
                    dest: 1,
                },
                4,
            ),
            (
                Insn::Subtract {
                    lhs: 0,
                    rhs: 2,
                    dest: 0,
                },
                5,
            ),
            (
                Insn::Goto {
                    target_pc: BranchOffset::Offset(3),
                },
                6,
            ),
            (
                Insn::ResultRow {
                    start_reg: 1,
                    count: 1,
                },
                7,
            ),
            (
                Insn::Halt {
                    err_code: 0,
                    description: String::new(),
                },
                8,
            ),
        ];

        assert!(validate_register_initialization(&insns).is_ok());
    }

    #[test]
    fn test_empty_program() {
        let insns: Vec<(Insn, usize)> = vec![];
        assert!(validate_register_initialization(&insns).is_ok());
    }

    #[test]
    fn test_move_instruction() {
        // Integer 42 -> r0
        // Move r0 -> r1 (also nullifies r0)
        // Add r0, r1 -> r2  -- r0 is now null but still "initialized"
        // Halt
        let insns = vec![
            (Insn::Integer { value: 42, dest: 0 }, 0),
            (
                Insn::Move {
                    source_reg: 0,
                    dest_reg: 1,
                    count: 1,
                },
                1,
            ),
            (
                Insn::Add {
                    lhs: 0,
                    rhs: 1,
                    dest: 2,
                },
                2,
            ),
            (
                Insn::Halt {
                    err_code: 0,
                    description: String::new(),
                },
                3,
            ),
        ];

        // Move writes to both source (as NULL) and dest, so this should be valid
        assert!(validate_register_initialization(&insns).is_ok());
    }
}
