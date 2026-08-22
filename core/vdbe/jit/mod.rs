//! JIT compiler for VDBE programs.
//!
//! This is a "subroutine-threading" JIT: each bytecode program is compiled to
//! one native function whose body is a sequence of direct calls to the
//! existing opcode implementations, with bytecode-level control flow lowered
//! to native branches. The opcode bodies keep all their semantics (register
//! typing, I/O state machines, errors); the JIT removes the interpreter's
//! per-instruction costs: instruction fetch, bounds check, opcode dispatch,
//! interrupt/tracing/metrics bookkeeping and the dispatch-loop branches.
//!
//! Safety model:
//! - Generated code only ever calls the `extern "C-unwind"` wrappers below,
//!   which convert opcode results to integer codes. A panicking opcode
//!   unwinds through the generated frames like any native frame:
//!   cranelift-jit registers unwind info for its code on the platforms
//!   [`compile`] allows, and the `unwinding_through_jit_frames_works` test
//!   proves the full panic path.
//! - Whenever an instruction produces anything other than a plain "step
//!   completed" result (a row, I/O, completion, an error), the wrapper stashes
//!   the result in [`ProgramState::jit_exit_result`] and the native code
//!   returns to the interpreter loop, which handles it exactly as it would an
//!   interpreted instruction result.
//! - After every opcode call the generated code re-reads `state.pc` and
//!   branches on it. Statically-known jump targets are compared inline; any
//!   other value goes through a `br_table` over every instruction. A
//!   mis-predicted static target therefore only costs time, never
//!   correctness.
//! - Interrupt and connection-closed checks run on every backward branch and
//!   on every dynamic dispatch, so every possible bytecode cycle contains a
//!   check. Straight-line code between checks is bounded by the program
//!   length.
//!
//! The compiled artifact is cached on the [`PreparedProgram`] and shared by
//! all executions of that statement. Compilation is triggered only after a
//! statement has interpreted enough instructions to be worth compiling (see
//! [`JitConfig::threshold`]).

use std::sync::Arc;
use std::sync::OnceLock;

use cranelift_codegen::ir::{types, AbiParam, BlockArg, InstBuilder, MemFlags, UserFuncName};
use cranelift_codegen::settings::{self, Configurable};
use cranelift_frontend::{FunctionBuilder, FunctionBuilderContext};
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{FuncId, Linkage, Module};
use strum::EnumCount;

use crate::vdbe::execute::{InsnFunction, InsnFunctionStepResult};
use crate::vdbe::insn::{Insn, InsnVariants};
use crate::vdbe::{BranchOffset, Program, ProgramState};
use crate::{Pager, Result};

/// Result codes returned by generated code and the opcode wrappers.
/// `STEP` is internal to the generated code (fall through to the next
/// instruction); the other codes make the native function return.
const JIT_STEP: u32 = 0;
/// A result (row, I/O, done, or error) was stored in `state.jit_exit_result`.
const JIT_EXIT_RESULT: u32 = 1;
/// No result: the interpreter loop must re-run its own checks (interrupt
/// requested, connection closed, or a PC the compiled code cannot enter).
const JIT_EXIT_LOOP: u32 = 2;

/// What `JitCode::run` tells the interpreter loop to do next.
pub enum JitRunResult {
    /// A step result was produced; handle it like an interpreted instruction.
    Result(Result<InsnFunctionStepResult>),
    /// Nothing happened; re-run the interpreter loop checks (interrupt,
    /// closed connection) and continue.
    Loop,
}

pub struct JitConfig {
    enabled: bool,
    /// Number of interpreted VM steps before a statement is compiled.
    threshold: u64,
    /// Programs with more instructions than this are never compiled.
    max_insns: usize,
}

fn config() -> &'static JitConfig {
    static CONFIG: OnceLock<JitConfig> = OnceLock::new();
    CONFIG.get_or_init(|| {
        let enabled = std::env::var("TURSO_JIT")
            .map(|v| v != "0" && !v.eq_ignore_ascii_case("off"))
            .unwrap_or(true);
        let threshold = std::env::var("TURSO_JIT_THRESHOLD")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10_000);
        let max_insns = std::env::var("TURSO_JIT_MAX_INSNS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10_000);
        JitConfig {
            enabled,
            threshold,
            max_insns,
        }
    })
}

/// Cheap global check: JIT compiled in and not disabled via `TURSO_JIT=0`.
#[inline]
pub fn runtime_enabled() -> bool {
    config().enabled
}

/// Returns the compiled code for this program if it is available or worth
/// producing now. Called from the interpreter loop.
#[inline]
pub fn maybe_jit<'a>(program: &'a Program, state: &ProgramState) -> Option<&'a JitCode> {
    let slot = &program.prepared().jit_code;
    if let Some(compiled) = slot.get() {
        return compiled.as_deref();
    }
    if state.metrics.vm_steps < config().threshold {
        return None;
    }
    slot.get_or_init(|| compile(program).map(Arc::new))
        .as_deref()
}

/// Signature of the generated entry function.
type JitEntry = extern "C-unwind" fn(*const Program, *mut ProgramState, *const Arc<Pager>) -> u32;

// System unwinder registration for generated code. On linux-gnu, libgcc's
// __register_frame takes a pointer to a whole `.eh_frame` section terminated
// by a zero-length entry. Other platforms have different semantics (macOS
// registers one FDE at a time) and are not wired up yet; `compile` refuses
// to produce code there so opcode panics always stay unwindable.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
extern "C" {
    fn __register_frame(ptr: *const u8);
    fn __deregister_frame(ptr: *const u8);
}

/// `.eh_frame` data for one generated function, registered with the system
/// unwinder for as long as this value lives. Without this, a panic unwinding
/// out of an opcode called from generated code would abort the process.
struct EhFrameRegistration {
    /// The serialized `.eh_frame`; its address is what was registered, so the
    /// allocation must stay pinned until deregistration.
    bytes: Box<[u8]>,
}

impl EhFrameRegistration {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    fn new(
        isa: &dyn cranelift_codegen::isa::TargetIsa,
        info: &cranelift_codegen::isa::unwind::systemv::UnwindInfo,
        code_addr: *const u8,
    ) -> Option<Self> {
        let mut table = gimli::write::FrameTable::default();
        let cie_id = table.add_cie(isa.create_systemv_cie()?);
        table.add_fde(
            cie_id,
            info.to_fde(gimli::write::Address::Constant(code_addr as u64)),
        );
        let mut eh_frame =
            gimli::write::EhFrame(gimli::write::EndianVec::new(gimli::RunTimeEndian::default()));
        table.write_eh_frame(&mut eh_frame).ok()?;
        let mut bytes = eh_frame.0.into_vec();
        // Zero-length terminator entry expected by __register_frame.
        bytes.extend_from_slice(&[0u8; 4]);
        let bytes = bytes.into_boxed_slice();
        // SAFETY: the bytes are a well-formed .eh_frame section and stay
        // alive (and pinned) until Drop deregisters them.
        unsafe { __register_frame(bytes.as_ptr()) };
        Some(Self { bytes })
    }

    #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
    fn new(
        _isa: &dyn cranelift_codegen::isa::TargetIsa,
        _info: &cranelift_codegen::isa::unwind::systemv::UnwindInfo,
        _code_addr: *const u8,
    ) -> Option<Self> {
        None
    }
}

impl Drop for EhFrameRegistration {
    fn drop(&mut self) {
        #[cfg(all(target_os = "linux", target_env = "gnu"))]
        // SAFETY: deregisters exactly what Self::new registered.
        unsafe {
            __deregister_frame(self.bytes.as_ptr())
        };
        let _ = &self.bytes;
    }
}

/// A compiled program. Owns the executable memory; dropping frees it.
/// Stored on the `PreparedProgram`, so it cannot outlive the instruction
/// array its generated code holds pointers into, and it is only dropped once
/// no execution can be inside the generated code (executions hold the
/// `Arc<PreparedProgram>` alive and are synchronous).
pub struct JitCode {
    entry: JitEntry,
    /// Number of instructions covered by the entry dispatch table.
    insn_count: u32,
    /// Keeps panic unwinding through the generated frames working; must be
    /// dropped before the executable memory is freed.
    eh_frame: Option<EhFrameRegistration>,
    module: Option<JITModule>,
}

// The JITModule is only used to keep the executable memory alive and free it
// on drop; the memory is immutable after finalization, so sharing the handle
// across threads is sound.
unsafe impl Send for JitCode {}
unsafe impl Sync for JitCode {}

impl std::fmt::Debug for JitCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JitCode")
            .field("insn_count", &self.insn_count)
            .finish_non_exhaustive()
    }
}

impl Drop for JitCode {
    fn drop(&mut self) {
        // Deregister unwind info before the code memory goes away.
        self.eh_frame = None;
        if let Some(module) = self.module.take() {
            // SAFETY: no generated code can be running: executions keep the
            // owning PreparedProgram (and thus this JitCode) alive for the
            // whole synchronous step call.
            unsafe { module.free_memory() };
        }
    }
}

impl JitCode {
    /// Run compiled code starting at `state.pc` until it produces a result
    /// or wants the interpreter loop to take over.
    #[inline]
    pub fn run(
        &self,
        program: &Program,
        state: &mut ProgramState,
        pager: &Arc<Pager>,
    ) -> Option<JitRunResult> {
        if state.pc >= self.insn_count {
            // Let the interpreter produce the identical out-of-bounds panic.
            return None;
        }
        let code = (self.entry)(program, state, pager);
        match code {
            JIT_EXIT_RESULT => {
                let result = state
                    .jit_exit_result
                    .take()
                    .expect("jit exit code promised a result");
                Some(JitRunResult::Result(result))
            }
            JIT_EXIT_LOOP => Some(JitRunResult::Loop),
            other => unreachable!("invalid jit exit code {other}"),
        }
    }
}

/// One wrapper per opcode discriminant. The discriminant is a const generic,
/// so `to_function()` resolves to a direct (usually inlined) call in each
/// instantiation.
///
/// The wrappers are `extern "C-unwind"`: a panicking opcode unwinds straight
/// through the generated frames, which works because [`compile`] registers
/// `.eh_frame` unwind info for every generated function (see
/// [`EhFrameRegistration`]). The `unwinding_through_jit_frames_works` test
/// exercises exactly this path.
extern "C-unwind" fn insn_wrapper<const V: usize>(
    program: *const Program,
    state: *mut ProgramState,
    insn: *const Insn,
    pager: *const Arc<Pager>,
) -> u32 {
    // Resolved at compile time so each wrapper calls its opcode directly.
    let f: InsnFunction = const {
        match InsnVariants::from_repr(V as u8) {
            Some(variant) => variant.to_function(),
            // Unused instantiation (V >= number of opcodes); never called.
            None => |_, _, _, _| unreachable!(),
        }
    };
    // SAFETY: the generated code passes the same references the interpreter
    // loop holds when calling an opcode function: a live Program, an
    // exclusively-borrowed ProgramState and a live Pager.
    let (program, state, insn, pager) = unsafe { (&*program, &mut *state, &*insn, &*pager) };
    // Mirrors the interpreter loop: every attempt counts as a VM step.
    state.metrics.vm_steps = state.metrics.vm_steps.saturating_add(1);
    match f(program, state, insn, pager) {
        Ok(InsnFunctionStepResult::Step) => {
            // Mirrors the interpreter loop's Step arm; completed instructions
            // that make the native code exit are instead counted by the
            // interpreter arm that handles the stashed result.
            state.metrics.insn_executed = state.metrics.insn_executed.saturating_add(1);
            JIT_STEP
        }
        other => {
            state.jit_exit_result = Some(other);
            JIT_EXIT_RESULT
        }
    }
}

/// Interrupt / connection-closed poll, run on backward branches and dynamic
/// dispatches. Returns nonzero when the interpreter loop must take over.
extern "C-unwind" fn check_interrupt_wrapper(
    program: *const Program,
    state: *mut ProgramState,
    pager: *const Arc<Pager>,
) -> u32 {
    let (program, state, pager) = unsafe { (&*program, &mut *state, &*pager) };
    let interrupted =
        program.connection.is_closed() || program.maybe_request_interrupt(state, pager.io.as_ref());
    u32::from(interrupted)
}

/// Panics when called from generated code; used by the unwind test below.
#[cfg(test)]
extern "C-unwind" fn unwind_probe_wrapper(
    _program: *const Program,
    _state: *mut ProgramState,
    _pager: *const Arc<Pager>,
) -> u32 {
    panic!("jit unwind probe")
}

const _: () = assert!(
    InsnVariants::COUNT <= 256,
    "Insn discriminant is read as a u8; grow the wrapper table"
);

/// Table of the per-discriminant wrappers, indexed by `Insn::discriminant()`.
static INSN_WRAPPERS: [extern "C-unwind" fn(
    *const Program,
    *mut ProgramState,
    *const Insn,
    *const Arc<Pager>,
) -> u32; 256] = {
    macro_rules! wrapper_table {
        ($($n:literal)*) => {
            [$(insn_wrapper::<$n>,)*]
        };
    }
    wrapper_table!(
        0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31
        32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63
        64 65 66 67 68 69 70 71 72 73 74 75 76 77 78 79 80 81 82 83 84 85 86 87 88 89 90 91 92 93 94 95
        96 97 98 99 100 101 102 103 104 105 106 107 108 109 110 111 112 113 114 115 116 117 118 119 120 121 122 123 124 125 126 127
        128 129 130 131 132 133 134 135 136 137 138 139 140 141 142 143 144 145 146 147 148 149 150 151 152 153 154 155 156 157 158 159
        160 161 162 163 164 165 166 167 168 169 170 171 172 173 174 175 176 177 178 179 180 181 182 183 184 185 186 187 188 189 190 191
        192 193 194 195 196 197 198 199 200 201 202 203 204 205 206 207 208 209 210 211 212 213 214 215 216 217 218 219 220 221 222 223
        224 225 226 227 228 229 230 231 232 233 234 235 236 237 238 239 240 241 242 243 244 245 246 247 248 249 250 251 252 253 254 255
    )
};

/// Statically-known successor PCs for an instruction, most likely first.
/// This is purely a branch-prediction hint for the generated code: after the
/// opcode runs, its actual `state.pc` is compared against these and anything
/// else goes through the generic dispatch table, so a wrong or missing entry
/// costs time, not correctness.
fn static_successors(insn: &Insn, pc: u32, out: &mut Vec<u32>) {
    fn off(b: &BranchOffset, out: &mut Vec<u32>) {
        if let BranchOffset::Offset(o) = b {
            out.push(*o);
        }
    }
    let fall = pc + 1;
    match insn {
        // Unconditional jumps.
        Insn::Init { target_pc } | Insn::Goto { target_pc } | Insn::Gosub { target_pc, .. } => {
            off(target_pc, out)
        }
        // Loop steppers: continuing the loop is the hot path.
        Insn::Next { pc_if_next, .. } | Insn::SorterNext { pc_if_next, .. } => {
            off(pc_if_next, out);
            out.push(fall);
        }
        Insn::Prev { pc_if_prev, .. } => {
            off(pc_if_prev, out);
            out.push(fall);
        }
        Insn::VNext { pc_if_next, .. } => {
            off(pc_if_next, out);
            out.push(fall);
        }
        // Loop entries: a non-empty table is the hot path.
        Insn::Rewind { pc_if_empty, .. }
        | Insn::Last { pc_if_empty, .. }
        | Insn::SorterSort { pc_if_empty, .. }
        | Insn::VFilter { pc_if_empty, .. }
        | Insn::IndexMethodQuery { pc_if_empty, .. }
        | Insn::RowSetRead { pc_if_empty, .. } => {
            out.push(fall);
            off(pc_if_empty, out);
        }
        // Conditional jumps.
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
        | Insn::DecrJumpZero { target_pc, .. }
        | Insn::SeekRowid { target_pc, .. }
        | Insn::SeekGE { target_pc, .. }
        | Insn::SeekGT { target_pc, .. }
        | Insn::SeekLE { target_pc, .. }
        | Insn::SeekLT { target_pc, .. }
        | Insn::IdxGE { target_pc, .. }
        | Insn::IdxGT { target_pc, .. }
        | Insn::IdxLE { target_pc, .. }
        | Insn::IdxLT { target_pc, .. }
        | Insn::Found { target_pc, .. }
        | Insn::NotFound { target_pc, .. }
        | Insn::NotExists { target_pc, .. }
        | Insn::NoConflict { target_pc, .. }
        | Insn::Filter { target_pc, .. }
        | Insn::ColumnHasField { target_pc, .. }
        | Insn::SequenceTest { target_pc, .. }
        | Insn::FkIfZero { target_pc, .. }
        | Insn::HashProbe { target_pc, .. }
        | Insn::HashNext { target_pc, .. }
        | Insn::HashScanUnmatched { target_pc, .. }
        | Insn::HashNextUnmatched { target_pc, .. }
        | Insn::HashGraceInit { target_pc, .. }
        | Insn::HashGraceLoadPartition { target_pc, .. }
        | Insn::HashGraceNextProbe { target_pc, .. }
        | Insn::HashGraceAdvancePartition { target_pc, .. } => {
            out.push(fall);
            off(target_pc, out);
        }
        Insn::MustBeInt { target_pc, .. } => {
            out.push(fall);
            if let Some(t) = target_pc {
                off(t, out);
            }
        }
        Insn::Jump {
            target_pc_lt,
            target_pc_eq,
            target_pc_gt,
        } => {
            off(target_pc_lt, out);
            off(target_pc_eq, out);
            off(target_pc_gt, out);
        }
        Insn::Once {
            target_pc_when_reentered,
        } => {
            out.push(fall);
            off(target_pc_when_reentered, out);
        }
        Insn::SorterCompare {
            pc_when_nonequal, ..
        } => {
            out.push(fall);
            off(pc_when_nonequal, out);
        }
        Insn::RowSetTest { pc_if_found, .. } => {
            out.push(fall);
            off(pc_if_found, out);
        }
        Insn::InitCoroutine {
            jump_on_definition,
            start_offset,
            ..
        } => {
            off(jump_on_definition, out);
            off(start_offset, out);
        }
        Insn::Yield { end_offset, .. } => {
            // The main successor comes from a register; only the end target
            // is static. Leave the rest to the dynamic dispatch.
            off(end_offset, out);
        }
        // Straight-line opcodes: everything that always advances to the next
        // instruction when it returns Step. Opcodes not listed here (Return,
        // Halt, coroutine and subprogram machinery, ...) go through the
        // dynamic dispatch table.
        Insn::Null { .. }
        | Insn::NullRow { .. }
        | Insn::Add { .. }
        | Insn::Subtract { .. }
        | Insn::Multiply { .. }
        | Insn::Divide { .. }
        | Insn::Remainder { .. }
        | Insn::BitAnd { .. }
        | Insn::BitOr { .. }
        | Insn::BitNot { .. }
        | Insn::ShiftLeft { .. }
        | Insn::ShiftRight { .. }
        | Insn::Concat { .. }
        | Insn::Compare { .. }
        | Insn::Move { .. }
        | Insn::Copy { .. }
        | Insn::Integer { .. }
        | Insn::Real { .. }
        | Insn::RealAffinity { .. }
        | Insn::String8 { .. }
        | Insn::Blob { .. }
        | Insn::Column { .. }
        | Insn::ColumnRange { .. }
        | Insn::TypeCheck { .. }
        | Insn::MakeRecord { .. }
        | Insn::RowId { .. }
        | Insn::IdxRowId { .. }
        | Insn::AggStep { .. }
        | Insn::AggFinal { .. }
        | Insn::AggValue { .. }
        | Insn::Function { .. }
        | Insn::Cast { .. }
        | Insn::Affinity { .. }
        | Insn::ZeroOrNull { .. }
        | Insn::SorterData { .. }
        | Insn::SorterInsert { .. }
        | Insn::SorterOpen { .. }
        | Insn::DeferredSeek { .. }
        | Insn::SeekEnd { .. }
        | Insn::Insert { .. }
        | Insn::IdxInsert { .. }
        | Insn::Delete { .. }
        | Insn::IdxDelete { .. }
        | Insn::NewRowid { .. }
        | Insn::RowSetAdd { .. }
        | Insn::HashBuild { .. }
        | Insn::OpenRead { .. }
        | Insn::OpenPseudo { .. }
        | Insn::Close { .. }
        | Insn::ResultRow { .. } => out.push(fall),
        _ => {}
    }
    out.retain(|t| *t != pc);
    out.dedup();
}

/// Instructions whose opcode implementations re-enter program execution or
/// otherwise assume they run directly under the interpreter loop.
fn insn_supported(insn: &Insn) -> bool {
    !matches!(insn, Insn::Program { .. })
}

type OwnedIsa = std::sync::Arc<dyn cranelift_codegen::isa::TargetIsa>;

fn make_module() -> Option<(JITModule, cranelift_codegen::isa::CallConv, OwnedIsa)> {
    let mut flag_builder = settings::builder();
    // Generated code is calls and short branches; compile speed matters more
    // than code quality ("speed" was measured to make no difference here).
    flag_builder.set("opt_level", "none").ok()?;
    flag_builder.set("use_colocated_libcalls", "false").ok()?;
    flag_builder.set("is_pic", "false").ok()?;
    // Needed to build the .eh_frame that lets panics unwind through
    // generated frames.
    flag_builder.set("unwind_info", "true").ok()?;
    let isa_builder = cranelift_native::builder().ok()?;
    let isa = isa_builder
        .finish(settings::Flags::new(flag_builder))
        .ok()?;
    let call_conv = isa.default_call_conv();
    let mut builder = JITBuilder::with_isa(isa.clone(), cranelift_module::default_libcall_names());
    builder.symbol("check_interrupt", check_interrupt_wrapper as *const u8);
    for (i, wrapper) in INSN_WRAPPERS.iter().enumerate() {
        builder.symbol(format!("insn_wrapper_{i}"), *wrapper as *const u8);
    }
    Some((JITModule::new(builder), call_conv, isa))
}

/// Compile a program. Returns None when the program contains unsupported
/// instructions, is too large, or code generation fails; the caller then
/// keeps interpreting it forever.
pub fn compile(program: &Program) -> Option<JitCode> {
    // Opcode panics unwind through generated frames (the wrappers are
    // `extern "C-unwind"`), which requires registering unwind info for the
    // generated code with the system unwinder. That registration is only
    // wired up for platforms where the `unwinding_through_jit_frames_works`
    // test proves the full panic path; everywhere else the interpreter is
    // used unconditionally.
    if !cfg!(all(
        target_os = "linux",
        target_env = "gnu",
        any(target_arch = "x86_64", target_arch = "aarch64")
    )) {
        return None;
    }
    let start = std::time::Instant::now();
    let insns = &program.prepared().insns;
    if insns.is_empty() || insns.len() > config().max_insns {
        return None;
    }
    if !insns.iter().all(|(insn, _)| insn_supported(insn)) {
        return None;
    }
    let (mut module, call_conv, isa) = make_module()?;

    let ptr_type = module.target_config().pointer_type();

    let mut insn_sig = module.make_signature();
    insn_sig.call_conv = call_conv;
    for _ in 0..4 {
        insn_sig.params.push(AbiParam::new(ptr_type));
    }
    insn_sig.returns.push(AbiParam::new(types::I32));

    let mut check_sig = module.make_signature();
    check_sig.call_conv = call_conv;
    for _ in 0..3 {
        check_sig.params.push(AbiParam::new(ptr_type));
    }
    check_sig.returns.push(AbiParam::new(types::I32));

    // Declare only the wrappers this program needs.
    let mut wrapper_ids: [Option<FuncId>; 256] = [None; 256];
    for (insn, _) in insns.iter() {
        let d = insn.discriminant() as usize;
        if wrapper_ids[d].is_none() {
            wrapper_ids[d] = Some(
                module
                    .declare_function(&format!("insn_wrapper_{d}"), Linkage::Import, &insn_sig)
                    .ok()?,
            );
        }
    }
    let check_id = module
        .declare_function("check_interrupt", Linkage::Import, &check_sig)
        .ok()?;

    let mut entry_sig = module.make_signature();
    entry_sig.call_conv = call_conv;
    for _ in 0..3 {
        entry_sig.params.push(AbiParam::new(ptr_type));
    }
    entry_sig.returns.push(AbiParam::new(types::I32));
    let entry_id = module
        .declare_function("entry", Linkage::Export, &entry_sig)
        .ok()?;

    let mut ctx = module.make_context();
    ctx.func.signature = entry_sig;
    ctx.func.name = UserFuncName::user(0, 0);

    let pc_offset = std::mem::offset_of!(ProgramState, pc) as i32;

    {
        let mut fb_ctx = FunctionBuilderContext::new();
        let mut fb = FunctionBuilder::new(&mut ctx.func, &mut fb_ctx);

        let check_ref = module.declare_func_in_func(check_id, fb.func);

        let var_program = fb.declare_var(ptr_type);
        let var_state = fb.declare_var(ptr_type);
        let var_pager = fb.declare_var(ptr_type);

        let entry_block = fb.create_block();
        let dispatch_block = fb.create_block();
        let exit_block = fb.create_block();
        // One block per instruction, plus a continuation block used after the
        // opcode call for successor selection.
        let insn_blocks: Vec<_> = insns.iter().map(|_| fb.create_block()).collect();

        fb.append_block_params_for_function_params(entry_block);
        fb.switch_to_block(entry_block);
        let params = fb.block_params(entry_block).to_vec();
        fb.def_var(var_program, params[0]);
        fb.def_var(var_state, params[1]);
        fb.def_var(var_pager, params[2]);
        fb.ins().jump(dispatch_block, &[] as &[BlockArg]);

        // exit_block: return the code passed as block param.
        fb.append_block_param(exit_block, types::I32);
        fb.switch_to_block(exit_block);
        let code_param = fb.block_params(exit_block)[0];
        fb.ins().return_(&[code_param]);

        // dispatch_block: poll for interrupts, then jump through the table.
        // This is the target of every dynamic branch and of function entry,
        // and together with the checks on static backward branches it
        // guarantees every bytecode cycle polls for interrupts.
        fb.switch_to_block(dispatch_block);
        let program_v = fb.use_var(var_program);
        let state_v = fb.use_var(var_state);
        let pager_v = fb.use_var(var_pager);
        let call = fb.ins().call(check_ref, &[program_v, state_v, pager_v]);
        let check_code = fb.inst_results(call)[0];
        let cont_block = fb.create_block();
        let check_exit_block = fb.create_block();
        fb.ins().brif(
            check_code,
            check_exit_block,
            &[] as &[BlockArg],
            cont_block,
            &[] as &[BlockArg],
        );

        fb.switch_to_block(check_exit_block);
        // Interrupt requested or connection closed: rerun the loop checks.
        let loop_code = fb.ins().iconst(types::I32, i64::from(JIT_EXIT_LOOP));
        fb.ins().jump(exit_block, &[BlockArg::Value(loop_code)]);

        fb.switch_to_block(cont_block);
        let state_v = fb.use_var(var_state);
        let pc_val = fb
            .ins()
            .load(types::I32, MemFlags::trusted(), state_v, pc_offset);
        let table_entries: Vec<_> = insn_blocks
            .iter()
            .map(|b| fb.func.dfg.block_call(*b, &[] as &[BlockArg]))
            .collect();
        let loop_exit_for_default = fb.create_block();
        let default_call = fb
            .func
            .dfg
            .block_call(loop_exit_for_default, &[] as &[BlockArg]);
        let jt_data = cranelift_codegen::ir::JumpTableData::new(default_call, &table_entries);
        let jt = fb.create_jump_table(jt_data);
        fb.ins().br_table(pc_val, jt);

        fb.switch_to_block(loop_exit_for_default);
        let loop_code2 = fb.ins().iconst(types::I32, i64::from(JIT_EXIT_LOOP));
        fb.ins().jump(exit_block, &[BlockArg::Value(loop_code2)]);

        // Per-instruction blocks.
        let mut successors = Vec::new();
        for (pc, (insn, _)) in insns.iter().enumerate() {
            let pc = pc as u32;
            fb.switch_to_block(insn_blocks[pc as usize]);

            let wrapper_id = wrapper_ids[insn.discriminant() as usize]
                .expect("wrapper declared for every discriminant in the program");
            let wrapper_ref = module.declare_func_in_func(wrapper_id, fb.func);

            let program_v = fb.use_var(var_program);
            let state_v = fb.use_var(var_state);
            let pager_v = fb.use_var(var_pager);
            let insn_ptr = fb
                .ins()
                .iconst(ptr_type, insn as *const Insn as usize as i64);
            let call = fb
                .ins()
                .call(wrapper_ref, &[program_v, state_v, insn_ptr, pager_v]);
            let code = fb.inst_results(call)[0];

            let stepped_block = fb.create_block();
            fb.ins().brif(
                code,
                exit_block,
                &[BlockArg::Value(code)],
                stepped_block,
                &[] as &[BlockArg],
            );

            // The opcode advanced state.pc; route to the next block.
            fb.switch_to_block(stepped_block);
            successors.clear();
            static_successors(insn, pc, &mut successors);
            successors.retain(|t| (*t as usize) < insns.len());
            if successors.is_empty() {
                fb.ins().jump(dispatch_block, &[] as &[BlockArg]);
            } else {
                let state_v = fb.use_var(var_state);
                let new_pc = fb
                    .ins()
                    .load(types::I32, MemFlags::trusted(), state_v, pc_offset);
                for &target in successors.iter() {
                    let match_block = if target <= pc {
                        // Backward branch: poll for interrupts on the way.
                        let check_block = fb.create_block();
                        let after = fb.create_block();
                        let expected = fb.ins().iconst(types::I32, i64::from(target));
                        let is_match = fb.ins().icmp(
                            cranelift_codegen::ir::condcodes::IntCC::Equal,
                            new_pc,
                            expected,
                        );
                        fb.ins().brif(
                            is_match,
                            check_block,
                            &[] as &[BlockArg],
                            after,
                            &[] as &[BlockArg],
                        );
                        fb.switch_to_block(check_block);
                        let program_v = fb.use_var(var_program);
                        let state_v2 = fb.use_var(var_state);
                        let pager_v = fb.use_var(var_pager);
                        let call = fb.ins().call(check_ref, &[program_v, state_v2, pager_v]);
                        let check_code = fb.inst_results(call)[0];
                        let go_block = fb.create_block();
                        let check_exit = fb.create_block();
                        fb.ins().brif(
                            check_code,
                            check_exit,
                            &[] as &[BlockArg],
                            go_block,
                            &[] as &[BlockArg],
                        );
                        fb.switch_to_block(check_exit);
                        let loop_code = fb.ins().iconst(types::I32, i64::from(JIT_EXIT_LOOP));
                        fb.ins().jump(exit_block, &[BlockArg::Value(loop_code)]);
                        fb.switch_to_block(go_block);
                        fb.ins()
                            .jump(insn_blocks[target as usize], &[] as &[BlockArg]);
                        fb.switch_to_block(after);
                        continue;
                    } else {
                        insn_blocks[target as usize]
                    };
                    let after = fb.create_block();
                    let expected = fb.ins().iconst(types::I32, i64::from(target));
                    let is_match = fb.ins().icmp(
                        cranelift_codegen::ir::condcodes::IntCC::Equal,
                        new_pc,
                        expected,
                    );
                    fb.ins().brif(
                        is_match,
                        match_block,
                        &[] as &[BlockArg],
                        after,
                        &[] as &[BlockArg],
                    );
                    fb.switch_to_block(after);
                }
                fb.ins().jump(dispatch_block, &[] as &[BlockArg]);
            }
        }

        fb.seal_all_blocks();
        fb.finalize();
    }

    if module.define_function(entry_id, &mut ctx).is_err() {
        return None;
    }
    // Panics must be able to unwind through the generated function, so a
    // missing unwind description disables the JIT for this program.
    let unwind_info = match ctx
        .compiled_code()
        .and_then(|code| code.create_unwind_info(isa.as_ref()).ok())
    {
        Some(Some(cranelift_codegen::isa::unwind::UnwindInfo::SystemV(info))) => info,
        _ => return None,
    };
    module.clear_context(&mut ctx);
    if module.finalize_definitions().is_err() {
        return None;
    }

    let entry_ptr = module.get_finalized_function(entry_id);
    let eh_frame = EhFrameRegistration::new(isa.as_ref(), &unwind_info, entry_ptr)?;
    // SAFETY: the finalized function was built with exactly this signature.
    let entry: JitEntry = unsafe { std::mem::transmute(entry_ptr) };
    tracing::debug!(
        "jit-compiled program ({} insns) in {:?}: {}",
        insns.len(),
        start.elapsed(),
        program.prepared().sql.chars().take(80).collect::<String>()
    );
    Some(JitCode {
        entry,
        insn_count: insns.len() as u32,
        eh_frame: Some(eh_frame),
        module: Some(module),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Database, MemoryIO, SqliteDialect, StepResult};

    fn memory_db() -> (Arc<Database>, Arc<crate::Connection>) {
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let conn = db.connect().unwrap();
        (db, conn)
    }

    fn run_to_completion(
        db: &Arc<Database>,
        conn: &Arc<crate::Connection>,
        sql: &str,
        force_jit: bool,
    ) -> Vec<String> {
        let mut stmt = conn.prepare(sql).unwrap();
        if force_jit {
            let code = compile(&stmt.program).expect("query should be JIT-compilable");
            assert!(
                stmt.program
                    .prepared()
                    .jit_code
                    .set(Some(Arc::new(code)))
                    .is_ok(),
                "fresh statement has no jit code yet"
            );
        }
        let mut rows = Vec::new();
        loop {
            match stmt.step().unwrap() {
                StepResult::Row => {
                    let row = stmt.row().unwrap();
                    let vals: Vec<String> = row.get_values().map(|v| format!("{v:?}")).collect();
                    rows.push(vals.join("|"));
                }
                StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                    db.io.step().unwrap();
                }
                StepResult::Done => break,
                other => panic!("unexpected step result {other:?}"),
            }
        }
        rows
    }

    /// Runs a set of queries interpreted and JIT-compiled and requires
    /// identical rows. The JIT statement gets its compiled code injected
    /// directly so the test does not depend on hotness thresholds or
    /// environment variables (though it still requires the JIT not to be
    /// disabled via TURSO_JIT=0).
    #[test]
    fn jit_matches_interpreter() {
        if !runtime_enabled() {
            return;
        }
        let (db, conn) = memory_db();
        run_to_completion(
            &db,
            &conn,
            "CREATE TABLE t(a INTEGER, b TEXT, c REAL)",
            false,
        );
        run_to_completion(
            &db,
            &conn,
            "WITH RECURSIVE g(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM g WHERE x < 2000) \
             INSERT INTO t SELECT x, 'v' || x, x * 0.5 FROM g",
            false,
        );
        let queries = [
            "SELECT count(*), sum(a), avg(c) FROM t WHERE a % 3 = 0",
            "SELECT b, count(*) FROM t WHERE a < 1500 GROUP BY a % 7 ORDER BY 2 DESC, 1 LIMIT 5",
            "SELECT t1.a, t2.b FROM t t1 JOIN t t2 ON t1.a = t2.a \
             WHERE t1.a BETWEEN 100 AND 120 ORDER BY t1.a",
            "SELECT max(c), min(b), sum(a * c) FROM t WHERE b <> 'v7'",
            "SELECT a, c FROM t WHERE c > 900.0 ORDER BY c DESC LIMIT 3",
        ];
        for sql in queries {
            let interpreted = run_to_completion(&db, &conn, sql, false);
            let jitted = run_to_completion(&db, &conn, sql, true);
            assert_eq!(interpreted, jitted, "row mismatch for query: {sql}");
        }
    }

    /// Opcode panics must unwind through generated frames: build a native
    /// function that calls a panicking wrapper and require catch_unwind to
    /// observe the payload. If this fails on some platform, that platform
    /// must be removed from the allowlist in [`compile`] (falling back to
    /// the interpreter) rather than shipped.
    #[test]
    fn unwinding_through_jit_frames_works() {
        let Some((_, call_conv, isa)) = make_module() else {
            panic!("jit module construction must work on test platforms");
        };
        let mut builder = cranelift_jit::JITBuilder::with_isa(
            isa.clone(),
            cranelift_module::default_libcall_names(),
        );
        builder.symbol("probe", unwind_probe_wrapper as *const u8);
        let mut module = JITModule::new(builder);
        let ptr_type = module.target_config().pointer_type();
        let mut sig = module.make_signature();
        sig.call_conv = call_conv;
        for _ in 0..3 {
            sig.params.push(AbiParam::new(ptr_type));
        }
        sig.returns.push(AbiParam::new(types::I32));
        let probe_id = module
            .declare_function("probe", Linkage::Import, &sig)
            .unwrap();
        let entry_id = module
            .declare_function("entry", Linkage::Export, &sig)
            .unwrap();
        let mut ctx = module.make_context();
        ctx.func.signature = sig;
        {
            let mut fb_ctx = FunctionBuilderContext::new();
            let mut fb = FunctionBuilder::new(&mut ctx.func, &mut fb_ctx);
            let block = fb.create_block();
            fb.append_block_params_for_function_params(block);
            fb.switch_to_block(block);
            let params = fb.block_params(block).to_vec();
            let probe_ref = module.declare_func_in_func(probe_id, fb.func);
            let call = fb.ins().call(probe_ref, &params);
            let res = fb.inst_results(call)[0];
            fb.ins().return_(&[res]);
            fb.seal_all_blocks();
            fb.finalize();
        }
        module.define_function(entry_id, &mut ctx).unwrap();
        let unwind_info = match ctx
            .compiled_code()
            .unwrap()
            .create_unwind_info(isa.as_ref())
            .unwrap()
        {
            Some(cranelift_codegen::isa::unwind::UnwindInfo::SystemV(info)) => info,
            other => panic!("expected SystemV unwind info, got {other:?}"),
        };
        module.clear_context(&mut ctx);
        module.finalize_definitions().unwrap();
        let entry_ptr = module.get_finalized_function(entry_id);
        let eh_frame = EhFrameRegistration::new(isa.as_ref(), &unwind_info, entry_ptr)
            .expect("unwind registration must work on test platforms");
        let entry: JitEntry = unsafe { std::mem::transmute(entry_ptr) };

        let result = std::panic::catch_unwind(|| {
            entry(std::ptr::null(), std::ptr::null_mut(), std::ptr::null())
        });
        let payload = result.expect_err("probe must panic");
        let message = payload.downcast_ref::<&str>().copied().unwrap_or_default();
        assert_eq!(message, "jit unwind probe");
        drop(eh_frame);
        unsafe { module.free_memory() };
    }

    /// Programs with subprogram instructions (triggers) must refuse to
    /// compile and stay on the interpreter.
    #[test]
    fn trigger_programs_are_not_compiled() {
        let (db, conn) = memory_db();
        run_to_completion(&db, &conn, "CREATE TABLE t(a INTEGER)", false);
        run_to_completion(&db, &conn, "CREATE TABLE log(x INTEGER)", false);
        run_to_completion(
            &db,
            &conn,
            "CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO log VALUES (new.a); END",
            false,
        );
        let stmt = conn.prepare("INSERT INTO t VALUES (1)").unwrap();
        assert!(
            compile(&stmt.program).is_none(),
            "trigger-firing programs must not be JIT-compiled"
        );
    }
}
