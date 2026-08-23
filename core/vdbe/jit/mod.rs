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

use crate::alloc::TryClone;
use crate::vdbe::execute::{self, InsnFunction, InsnFunctionStepResult};
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

/// A clonable atomic execution counter for [`PreparedProgram`]; a clone
/// starts from the current count.
///
/// [`PreparedProgram`]: crate::vdbe::PreparedProgram
#[derive(Debug, Default)]
pub struct ExecutionCounter(pub std::sync::atomic::AtomicU32);

impl Clone for ExecutionCounter {
    fn clone(&self) -> Self {
        Self(std::sync::atomic::AtomicU32::new(
            self.0.load(std::sync::atomic::Ordering::Relaxed),
        ))
    }
}

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
    /// Minimum interpreted VM steps before a statement may be compiled.
    /// 0 forces compilation on the first step, bypassing every other
    /// gate below (used by tests).
    threshold: u64,
    /// Programs with more instructions than this are never compiled;
    /// cranelift compile time grows superlinearly with function size, and
    /// huge programs are usually straight-line multi-row DML where dispatch
    /// overhead is irrelevant anyway.
    max_insns: usize,
}

/// Compile only once the current execution alone is this long: such a
/// statement keeps a compile worthwhile even if it is never run again.
const MIN_SINGLE_EXECUTION_STEPS: u64 = 1_000_000;
/// Alternatively, compile once the same prepared program has completed this
/// many executions: reuse amortizes the compile even for shorter statements.
const MIN_COMPLETED_EXECUTIONS: u32 = 4;
/// And in either case, require the program to have executed this many steps
/// per instruction over its lifetime, so straight-line programs that visit
/// each instruction a handful of times never compile.
const MIN_STEPS_PER_INSN: u64 = 64;

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
            .unwrap_or(2_000);
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
///
/// Compilation costs whole milliseconds, so it must provably amortize:
/// either the current execution alone is long enough to repay it (a big
/// scan/aggregation), or the same prepared program keeps being re-executed.
/// One-shot statements — even huge ones like multi-row inserts — stay on
/// the interpreter, where the per-instruction overhead they would save is
/// dwarfed by the compile they would pay.
#[inline]
pub fn maybe_jit<'a>(program: &'a Program, state: &mut ProgramState) -> Option<&'a JitCode> {
    let prepared = program.prepared();
    let slot = &prepared.jit_code;
    if let Some(compiled) = slot.get() {
        return compiled.as_deref();
    }
    let lifetime_steps = state.metrics.vm_steps;
    // Covers both the base threshold and the retry backoff below, so the
    // common not-yet-hot case is a single compare.
    if lifetime_steps < state.jit_next_compile_check.max(config().threshold) {
        return None;
    }
    if config().threshold != 0 {
        let current_execution_steps =
            lifetime_steps.saturating_sub(state.jit_steps_at_execution_start);
        let reused = prepared
            .jit_completed_executions
            .0
            .load(std::sync::atomic::Ordering::Relaxed)
            >= MIN_COMPLETED_EXECUTIONS;
        let insn_count = prepared.insns.len() as u64;
        if (current_execution_steps < MIN_SINGLE_EXECUTION_STEPS && !reused)
            || lifetime_steps < insn_count.saturating_mul(MIN_STEPS_PER_INSN)
        {
            // Not worth compiling yet; don't re-evaluate every instruction.
            state.jit_next_compile_check = lifetime_steps + (lifetime_steps / 2).max(8_192);
            return None;
        }
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

/// Specialized helpers: slimmer entry points for the hottest simple opcodes.
/// The JIT bakes the instruction's operands in as immediate arguments, so
/// these skip the generic wrapper's instruction pointer load and the opcode's
/// `load_insn!` destructuring, and they report taken/not-taken branches
/// through their return code so the generated code can branch natively
/// instead of re-reading `state.pc`.
///
/// Helpers never touch `state.pc`; the generated code stores the
/// instruction's pc before every call so opcodes and error paths observe the
/// same program state as under the interpreter.
///
/// Return codes: see SPEC_FALL / SPEC_JUMP / SPEC_RESULT.
type SpecHelperFn = extern "C-unwind" fn(*const Program, *mut ProgramState, u64, u64, u64) -> u32;

/// The instruction completed and control falls through to pc + 1.
const SPEC_FALL: u32 = 0;
/// The instruction completed and control goes to its static branch target.
const SPEC_JUMP: u32 = 1;
/// A result (an error) was stored in `state.jit_exit_result`.
const SPEC_RESULT: u32 = 2;
/// The helper does not cover this case; run the generic wrapper instead.
/// The helper must not have modified any state before returning this.
const SPEC_BAIL: u32 = 3;

/// How the generated code should call an instruction's specialized helper.
/// `name` must be registered in [`SPEC_HELPERS`].
struct SpecCall {
    name: &'static str,
    args: [u64; 3],
    /// Static branch target for helpers that can return SPEC_JUMP.
    jump_target: Option<u32>,
    /// Whether the helper may return SPEC_BAIL, in which case the generated
    /// code re-runs the instruction through the generic wrapper.
    can_bail: bool,
    /// Reserves a comparison cache site; `compile` patches the site index
    /// into args[2].
    needs_cmp_site: bool,
}

/// Mirrors the interpreter loop: every attempt counts as a VM step.
#[inline(always)]
fn spec_count_attempt(state: &mut ProgramState) {
    state.metrics.vm_steps = state.metrics.vm_steps.saturating_add(1);
}

/// Mirrors the interpreter loop's Step arm: the instruction completed.
#[inline(always)]
fn spec_completed(state: &mut ProgramState, code: u32) -> u32 {
    state.metrics.insn_executed = state.metrics.insn_executed.saturating_add(1);
    code
}

/// Store an error outcome for the interpreter loop, mirroring the generic
/// wrapper's non-Step handling (the interpreter arm does not count
/// completed instructions for errors).
#[inline(never)]
fn spec_store_error(state: &mut ProgramState, err: crate::LimboError) -> u32 {
    state.jit_exit_result = Some(Err(err));
    SPEC_RESULT
}

/// `Insn::Integer`: r[dest] = value.
extern "C-unwind" fn spec_integer(
    _program: *const Program,
    state: *mut ProgramState,
    value: u64,
    dest: u64,
    _c: u64,
) -> u32 {
    let state = unsafe { &mut *state };
    spec_count_attempt(state);
    state.registers[dest as usize].set_int(value as i64);
    spec_completed(state, SPEC_FALL)
}

/// `Insn::Copy`: r[dst..=dst+extra] = r[src..=src+extra] (clones).
extern "C-unwind" fn spec_copy(
    _program: *const Program,
    state: *mut ProgramState,
    src_reg: u64,
    dst_reg: u64,
    extra_amount: u64,
) -> u32 {
    use crate::types::Value;
    use crate::vdbe::Register;
    let state = unsafe { &mut *state };
    spec_count_attempt(state);
    for i in 0..=extra_amount as usize {
        let (src, dst) = (src_reg as usize + i, dst_reg as usize + i);
        if src == dst {
            continue;
        }
        let [src, dst] = state
            .registers
            .get_disjoint_mut([src, dst])
            .expect("Copy source and destination registers are distinct");
        // Cloning a numeric into a register that owns no allocation is a
        // plain copy; everything else goes through the allocation-reusing
        // clone the interpreter opcode uses.
        if let (
            Register::Value(src_val @ (Value::Numeric(_) | Value::Null)),
            Register::Value(dst_val @ (Value::Numeric(_) | Value::Null)),
        ) = (&*src, &mut *dst)
        {
            *dst_val = match src_val {
                Value::Numeric(n) => Value::Numeric(*n),
                _ => Value::Null,
            };
        } else if let Err(err) = dst.try_clone_from(src) {
            return spec_store_error(state, err.into());
        }
    }
    spec_completed(state, SPEC_FALL)
}

/// `Insn::Move`: r[dest..dest+count] = r[source..source+count], sources
/// become NULL.
extern "C-unwind" fn spec_move(
    _program: *const Program,
    state: *mut ProgramState,
    source_reg: u64,
    dest_reg: u64,
    count: u64,
) -> u32 {
    let state = unsafe { &mut *state };
    spec_count_attempt(state);
    for i in 0..count as usize {
        state.registers[dest_reg as usize + i] = std::mem::replace(
            &mut state.registers[source_reg as usize + i],
            crate::vdbe::Register::Value(crate::types::Value::Null),
        );
    }
    spec_completed(state, SPEC_FALL)
}

macro_rules! spec_arith {
    ($name:ident, $exec:ident) => {
        /// Arithmetic: r[dest] = r[lhs] op r[rhs] via the shared exec
        /// primitive the interpreter opcode uses.
        extern "C-unwind" fn $name(
            _program: *const Program,
            state: *mut ProgramState,
            lhs: u64,
            rhs: u64,
            dest: u64,
        ) -> u32 {
            let state = unsafe { &mut *state };
            spec_count_attempt(state);
            state.registers[dest as usize].set_value(
                state.registers[lhs as usize]
                    .get_value()
                    .$exec(state.registers[rhs as usize].get_value()),
            );
            spec_completed(state, SPEC_FALL)
        }
    };
}

spec_arith!(spec_add, exec_add);
spec_arith!(spec_subtract, exec_subtract);
spec_arith!(spec_multiply, exec_multiply);

/// `Insn::If` / `Insn::IfNot`: branch on the truthiness of r[reg].
extern "C-unwind" fn spec_if(
    _program: *const Program,
    state: *mut ProgramState,
    reg: u64,
    jump_if_null: u64,
    negate: u64,
) -> u32 {
    let state = unsafe { &mut *state };
    spec_count_attempt(state);
    let jump = state.registers[reg as usize]
        .get_value()
        .exec_if(jump_if_null != 0, negate != 0);
    spec_completed(state, if jump { SPEC_JUMP } else { SPEC_FALL })
}

/// `Insn::IfPos`: if r[reg] > 0 { r[reg] -= decrement_by; jump }.
extern "C-unwind" fn spec_if_pos(
    _program: *const Program,
    state: *mut ProgramState,
    reg: u64,
    decrement_by: u64,
    _c: u64,
) -> u32 {
    use crate::numeric::Numeric;
    use crate::types::Value;
    let state = unsafe { &mut *state };
    spec_count_attempt(state);
    match state.registers[reg as usize].get_value() {
        Value::Numeric(Numeric::Integer(n)) if *n > 0 => {
            let n = *n;
            state.registers[reg as usize].set_int(n - decrement_by as i64);
            spec_completed(state, SPEC_JUMP)
        }
        Value::Numeric(Numeric::Integer(_)) => spec_completed(state, SPEC_FALL),
        _ => spec_store_error(
            state,
            crate::LimboError::InternalError(
                "IfPos: the value in the register is not an integer".into(),
            ),
        ),
    }
}

/// `Insn::AggStep`: forwards to the shared implementation with the payload
/// pointer baked in, skipping instruction decode. The payload lives in the
/// program's instruction array, which outlives the compiled code.
extern "C-unwind" fn spec_agg_step(
    program: *const Program,
    state: *mut ProgramState,
    data: u64,
    _b: u64,
    _c: u64,
) -> u32 {
    let program = unsafe { &*program };
    let state = unsafe { &mut *state };
    spec_count_attempt(state);
    let data = unsafe { &*(data as usize as *const crate::vdbe::insn::AggStepData) };
    match execute::agg_step_impl(program, state, data) {
        Ok(InsnFunctionStepResult::Step) => spec_completed(state, SPEC_FALL),
        other => {
            state.jit_exit_result = Some(other);
            SPEC_RESULT
        }
    }
}

/// Per-site cache of a text value known not to convert under numeric
/// affinity, so repeated comparisons against the same non-numeric text (a
/// date-string constant, most commonly) skip the failed parse each row.
/// Content-keyed, so buffer reuse or register rewrites can never produce a
/// stale hit; lives in the ProgramState, so concurrent executions of one
/// program never share it.
#[derive(Clone)]
pub(crate) struct CmpCacheSlot {
    len: u32,
    bytes: [u8; Self::CAP],
}

impl CmpCacheSlot {
    const CAP: usize = 24;
    const EMPTY: u32 = u32::MAX;

    fn matches(&self, text: &[u8]) -> bool {
        self.len as usize == text.len() && &self.bytes[..text.len()] == text
    }

    fn store(&mut self, text: &[u8]) {
        if text.len() <= Self::CAP {
            self.len = text.len() as u32;
            self.bytes[..text.len()].copy_from_slice(text);
        }
    }
}

impl Default for CmpCacheSlot {
    fn default() -> Self {
        Self {
            len: Self::EMPTY,
            bytes: [0; Self::CAP],
        }
    }
}

/// Numeric-affinity conversion attempt for one comparison operand, memoized
/// for texts that are known not to convert.
#[inline]
fn cached_numeric_attempt<'a>(
    slot: &mut CmpCacheSlot,
    text: crate::types::ValueRef<'a>,
) -> Option<crate::types::ValueRef<'a>> {
    let crate::types::ValueRef::Text(t) = text else {
        return None;
    };
    let bytes = t.as_str().as_bytes();
    if slot.matches(bytes) {
        return None;
    }
    match crate::vdbe::affinity::apply_numeric_affinity(text, false) {
        Some(converted) => Some(converted),
        None => {
            slot.store(bytes);
            None
        }
    }
}

/// `Insn::Eq`..`Insn::Ge` with binary collation and no array comparison:
/// handles NULLs, numeric-numeric and text-text operands exactly like
/// `op_comparison` (including writing converted values back to the
/// registers), and bails to the generic wrapper for everything else.
extern "C-unwind" fn spec_cmp(
    _program: *const Program,
    state: *mut ProgramState,
    regs: u64,
    packed: u64,
    site: u64,
) -> u32 {
    use crate::translate::collate::CollationSeq;
    use crate::types::{AsValueRef, Value};
    use crate::vdbe::value::ComparisonOp;

    enum Outcome {
        Bail,
        Jump(bool),
        JumpWriteBack {
            jump: bool,
            lhs: Option<Value>,
            rhs: Option<Value>,
        },
        Error(crate::LimboError),
    }

    let state = unsafe { &mut *state };
    let (lhs, rhs) = ((regs & 0xffff_ffff) as usize, (regs >> 32) as usize);
    let op = match packed & 0x7 {
        0 => ComparisonOp::Eq,
        1 => ComparisonOp::Ne,
        2 => ComparisonOp::Lt,
        3 => ComparisonOp::Le,
        4 => ComparisonOp::Gt,
        _ => ComparisonOp::Ge,
    };
    let null_eq = packed & 0x8 != 0;
    let jump_if_null = packed & 0x10 != 0;
    let numeric_affinity = packed & 0x20 != 0;
    let text_affinity = packed & 0x40 != 0;
    let collation = CollationSeq::Binary;

    // Grow the cache before borrowing registers; sites are compile-time
    // constants, so this settles after the first execution.
    let cache_base = site as usize * 2;
    if numeric_affinity && state.jit_cmp_caches.len() < cache_base + 2 {
        state
            .jit_cmp_caches
            .resize(cache_base + 2, Default::default());
    }

    let outcome = {
        let lhs_value = state.registers[lhs].get_value();
        let rhs_value = state.registers[rhs].get_value();

        // Mirrors op_comparison's match order exactly.
        match (lhs_value, rhs_value) {
            (Value::Null, _) | (_, Value::Null) => {
                let jump = if null_eq {
                    let order = crate::types::compare_immutable_single(
                        lhs_value.as_value_ref(),
                        rhs_value.as_value_ref(),
                        collation,
                    );
                    comparison_matches_order_jit(op, order)
                } else {
                    jump_if_null
                };
                Outcome::Jump(jump)
            }
            (
                Value::Numeric(crate::numeric::Numeric::Integer(_)),
                Value::Numeric(crate::numeric::Numeric::Integer(_)),
            ) => Outcome::Jump(op.compare(lhs_value, rhs_value, collation)),
            (Value::Numeric(_), Value::Numeric(_)) if !text_affinity => {
                // Numeric and no-op affinities leave numerics untouched
                // (convert_for_compare returns None for both sides).
                Outcome::Jump(op.compare(lhs_value, rhs_value, collation))
            }
            (Value::Text(_), Value::Text(_)) if !text_affinity => {
                let (l_conv, r_conv) = if numeric_affinity {
                    let [l_slot, r_slot] = state
                        .jit_cmp_caches
                        .get_disjoint_mut([cache_base, cache_base + 1])
                        .expect("distinct cache slots");
                    (
                        cached_numeric_attempt(l_slot, lhs_value.as_value_ref()),
                        cached_numeric_attempt(r_slot, rhs_value.as_value_ref()),
                    )
                } else {
                    (None, None)
                };
                let jump = op.compare(
                    l_conv.unwrap_or_else(|| lhs_value.as_value_ref()),
                    r_conv.unwrap_or_else(|| rhs_value.as_value_ref()),
                    collation,
                );
                // Mirror op_comparison: converted operands replace the
                // register contents.
                let owned = |conv: Option<crate::types::ValueRef<'_>>| match conv {
                    None => Ok(None),
                    Some(v) => v.to_owned().map(Some),
                };
                match (owned(l_conv), owned(r_conv)) {
                    (Ok(l), Ok(r)) => Outcome::JumpWriteBack {
                        jump,
                        lhs: l,
                        rhs: r,
                    },
                    (Err(e), _) | (_, Err(e)) => Outcome::Error(e.into()),
                }
            }
            // Mixed shapes (text vs numeric, blobs, ...) are rarer and often
            // transient thanks to the write-back above; keep them generic.
            _ => Outcome::Bail,
        }
    };

    match outcome {
        Outcome::Bail => SPEC_BAIL,
        Outcome::Jump(jump) => {
            spec_count_attempt(state);
            spec_completed(state, if jump { SPEC_JUMP } else { SPEC_FALL })
        }
        Outcome::JumpWriteBack {
            jump,
            lhs: l,
            rhs: r,
        } => {
            spec_count_attempt(state);
            if let Some(l) = l {
                state.registers[lhs].set_value(l);
            }
            if let Some(r) = r {
                state.registers[rhs].set_value(r);
            }
            spec_completed(state, if jump { SPEC_JUMP } else { SPEC_FALL })
        }
        Outcome::Error(err) => {
            spec_count_attempt(state);
            spec_store_error(state, err)
        }
    }
}

/// Same as execute::comparison_matches_order, private there.
fn comparison_matches_order_jit(
    op: crate::vdbe::value::ComparisonOp,
    order: std::cmp::Ordering,
) -> bool {
    use crate::vdbe::value::ComparisonOp;
    match op {
        ComparisonOp::Eq => order.is_eq(),
        ComparisonOp::Ne => !order.is_eq(),
        ComparisonOp::Lt => order.is_lt(),
        ComparisonOp::Le => order.is_le(),
        ComparisonOp::Gt => order.is_gt(),
        ComparisonOp::Ge => order.is_ge(),
    }
}

/// Chooses a specialized helper for an instruction, or None to use the
/// generic wrapper. Only instructions whose full semantics the helpers
/// reproduce may be specialized; branch targets must be resolved offsets and
/// in bounds, and fall-through must stay in bounds.
fn specialize(insn: &Insn, pc: u32, insn_count: usize) -> Option<SpecCall> {
    let fall_ok = (pc as usize + 1) < insn_count;
    let target_of = |b: &BranchOffset| -> Option<u32> {
        match b {
            BranchOffset::Offset(o) if (*o as usize) < insn_count => Some(*o),
            _ => None,
        }
    };
    debug_assert!(
        SPEC_HELPERS.len() == 10,
        "keep SPEC_HELPERS in sync with specialize()"
    );
    let spec = match insn {
        Insn::Integer { value, dest } if fall_ok => SpecCall {
            name: "spec_integer",
            args: [*value as u64, *dest as u64, 0],
            jump_target: None,
            can_bail: false,
            needs_cmp_site: false,
        },
        Insn::Copy {
            src_reg,
            dst_reg,
            extra_amount,
        } if fall_ok => SpecCall {
            name: "spec_copy",
            args: [*src_reg as u64, *dst_reg as u64, *extra_amount as u64],
            jump_target: None,
            can_bail: false,
            needs_cmp_site: false,
        },
        Insn::Move {
            source_reg,
            dest_reg,
            count,
        } if fall_ok => SpecCall {
            name: "spec_move",
            args: [*source_reg as u64, *dest_reg as u64, *count as u64],
            jump_target: None,
            can_bail: false,
            needs_cmp_site: false,
        },
        Insn::Add { lhs, rhs, dest } if fall_ok => SpecCall {
            name: "spec_add",
            args: [*lhs as u64, *rhs as u64, *dest as u64],
            jump_target: None,
            can_bail: false,
            needs_cmp_site: false,
        },
        Insn::Subtract { lhs, rhs, dest } if fall_ok => SpecCall {
            name: "spec_subtract",
            args: [*lhs as u64, *rhs as u64, *dest as u64],
            jump_target: None,
            can_bail: false,
            needs_cmp_site: false,
        },
        Insn::Multiply { lhs, rhs, dest } if fall_ok => SpecCall {
            name: "spec_multiply",
            args: [*lhs as u64, *rhs as u64, *dest as u64],
            jump_target: None,
            can_bail: false,
            needs_cmp_site: false,
        },
        Insn::If {
            reg,
            target_pc,
            jump_if_null,
        } if fall_ok => SpecCall {
            name: "spec_if",
            args: [*reg as u64, u64::from(*jump_if_null), 0],
            jump_target: Some(target_of(target_pc)?),
            can_bail: false,
            needs_cmp_site: false,
        },
        Insn::IfNot {
            reg,
            target_pc,
            jump_if_null,
        } if fall_ok => SpecCall {
            name: "spec_if",
            args: [*reg as u64, u64::from(*jump_if_null), 1],
            jump_target: Some(target_of(target_pc)?),
            can_bail: false,
            needs_cmp_site: false,
        },
        Insn::IfPos {
            reg,
            target_pc,
            decrement_by,
        } if fall_ok => SpecCall {
            name: "spec_if_pos",
            args: [*reg as u64, *decrement_by as u64, 0],
            jump_target: Some(target_of(target_pc)?),
            can_bail: false,
            needs_cmp_site: false,
        },
        Insn::Eq { .. }
        | Insn::Ne { .. }
        | Insn::Lt { .. }
        | Insn::Le { .. }
        | Insn::Gt { .. }
        | Insn::Ge { .. }
            if fall_ok =>
        {
            let (lhs, rhs, target_pc, flags, collation, op_code) = match insn {
                Insn::Eq {
                    lhs,
                    rhs,
                    target_pc,
                    flags,
                    collation,
                } => (lhs, rhs, target_pc, flags, collation, 0u64),
                Insn::Ne {
                    lhs,
                    rhs,
                    target_pc,
                    flags,
                    collation,
                } => (lhs, rhs, target_pc, flags, collation, 1),
                Insn::Lt {
                    lhs,
                    rhs,
                    target_pc,
                    flags,
                    collation,
                } => (lhs, rhs, target_pc, flags, collation, 2),
                Insn::Le {
                    lhs,
                    rhs,
                    target_pc,
                    flags,
                    collation,
                } => (lhs, rhs, target_pc, flags, collation, 3),
                Insn::Gt {
                    lhs,
                    rhs,
                    target_pc,
                    flags,
                    collation,
                } => (lhs, rhs, target_pc, flags, collation, 4),
                Insn::Ge {
                    lhs,
                    rhs,
                    target_pc,
                    flags,
                    collation,
                } => (lhs, rhs, target_pc, flags, collation, 5),
                _ => unreachable!(),
            };
            use crate::translate::collate::CollationSeq;
            use crate::vdbe::affinity::Affinity;
            if !matches!(collation.unwrap_or_default(), CollationSeq::Binary)
                || flags.has_array_cmp()
            {
                return None;
            }
            let affinity = flags.get_affinity();
            let packed = op_code
                | u64::from(flags.has_nulleq()) << 3
                | u64::from(flags.has_jump_if_null()) << 4
                | u64::from(matches!(
                    affinity,
                    Affinity::Numeric | Affinity::Integer | Affinity::Real
                )) << 5
                | u64::from(matches!(affinity, Affinity::Text)) << 6;
            SpecCall {
                name: "spec_cmp",
                args: [(*lhs as u64) | ((*rhs as u64) << 32), packed, 0],
                jump_target: Some(target_of(target_pc)?),
                can_bail: true,
                needs_cmp_site: true,
            }
        }
        Insn::AggStep { data } if fall_ok => {
            // Window functions route through different state handling; keep
            // them on the generic path.
            if matches!(data.func, crate::function::AccumulatorFunc::Window(_)) {
                return None;
            }
            SpecCall {
                name: "spec_agg_step",
                args: [data.as_ref() as *const _ as usize as u64, 0, 0],
                jump_target: None,
                can_bail: false,
                needs_cmp_site: false,
            }
        }
        _ => return None,
    };
    Some(spec)
}

/// All specialized helpers, for symbol registration.
const SPEC_HELPERS: &[(&str, SpecHelperFn)] = &[
    ("spec_integer", spec_integer),
    ("spec_copy", spec_copy),
    ("spec_move", spec_move),
    ("spec_add", spec_add),
    ("spec_subtract", spec_subtract),
    ("spec_multiply", spec_multiply),
    ("spec_if", spec_if),
    ("spec_if_pos", spec_if_pos),
    ("spec_agg_step", spec_agg_step),
    ("spec_cmp", spec_cmp),
];

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
    // than code quality ("speed" was measured to help neither instruction
    // counts nor the L1i footprint of the generated glue).
    flag_builder.set("opt_level", "none").ok()?;
    flag_builder.set("use_colocated_libcalls", "false").ok()?;
    flag_builder.set("is_pic", "false").ok()?;
    // Needed to build the .eh_frame that lets panics unwind through
    // generated frames.
    flag_builder.set("unwind_info", "true").ok()?;
    // The IR verifier is a compiler-development tool and costs a double-digit
    // share of compile time.
    flag_builder.set("enable_verifier", "false").ok()?;
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
    for (name, helper) in SPEC_HELPERS {
        builder.symbol(*name, *helper as *const u8);
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

    let mut spec_sig = module.make_signature();
    spec_sig.call_conv = call_conv;
    spec_sig.params.push(AbiParam::new(ptr_type));
    spec_sig.params.push(AbiParam::new(ptr_type));
    for _ in 0..3 {
        spec_sig.params.push(AbiParam::new(types::I64));
    }
    spec_sig.returns.push(AbiParam::new(types::I32));

    // Pick specialized helpers up front and declare the ones in use.
    let mut specs: Vec<Option<SpecCall>> = insns
        .iter()
        .enumerate()
        .map(|(pc, (insn, _))| specialize(insn, pc as u32, insns.len()))
        .collect();
    // Assign each comparison site its cache index.
    let mut cmp_sites = 0u64;
    for spec in specs.iter_mut().flatten() {
        if spec.needs_cmp_site {
            spec.args[2] = cmp_sites;
            cmp_sites += 1;
        }
    }
    let specs = specs;
    let mut spec_ids: std::collections::HashMap<&'static str, FuncId> =
        std::collections::HashMap::new();
    for spec in specs.iter().flatten() {
        if !spec_ids.contains_key(spec.name) {
            let id = module
                .declare_function(spec.name, Linkage::Import, &spec_sig)
                .ok()?;
            spec_ids.insert(spec.name, id);
        }
    }

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
        // Shared cold exit for specialized helpers that stored a result.
        let spec_result_block = fb.create_block();
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
        fb.switch_to_block(spec_result_block);
        let result_code = fb.ins().iconst(types::I32, i64::from(JIT_EXIT_RESULT));
        fb.ins().jump(exit_block, &[BlockArg::Value(result_code)]);

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

            // Opcodes and their error paths observe the interpreter's
            // invariant that state.pc names the executing instruction, so
            // materialize it before every call: specialized helpers never
            // write pc, and after one runs the stale value must not leak
            // into the next opcode.
            let state_v = fb.use_var(var_state);
            let pc_const = fb.ins().iconst(types::I32, i64::from(pc));
            fb.ins()
                .store(MemFlags::trusted(), pc_const, state_v, pc_offset);

            if let Some(spec) = &specs[pc as usize] {
                // Specialized call: operands are immediates and the branch
                // decision comes back in the return code, so no pc reload.
                let spec_ref = module.declare_func_in_func(spec_ids[spec.name], fb.func);
                let program_v = fb.use_var(var_program);
                let state_v = fb.use_var(var_state);
                let a = fb.ins().iconst(types::I64, spec.args[0] as i64);
                let b = fb.ins().iconst(types::I64, spec.args[1] as i64);
                let c = fb.ins().iconst(types::I64, spec.args[2] as i64);
                let call = fb.ins().call(spec_ref, &[program_v, state_v, a, b, c]);
                let code = fb.inst_results(call)[0];

                let fall_block = insn_blocks[pc as usize + 1];
                // Bailing helpers re-run the instruction through the generic
                // wrapper, emitted below in this same iteration.
                let generic_block = spec.can_bail.then(|| fb.create_block());
                match spec.jump_target {
                    None => {
                        // SPEC_FALL falls through; anything else stored a
                        // result (fall-only helpers never bail).
                        fb.ins().brif(
                            code,
                            spec_result_block,
                            &[] as &[BlockArg],
                            fall_block,
                            &[] as &[BlockArg],
                        );
                    }
                    Some(target) => {
                        let jump_const = fb.ins().iconst(types::I32, i64::from(SPEC_JUMP));
                        let is_jump = fb.ins().icmp(
                            cranelift_codegen::ir::condcodes::IntCC::Equal,
                            code,
                            jump_const,
                        );
                        let taken_block = fb.create_block();
                        let not_jump_block = fb.create_block();
                        fb.ins().brif(
                            is_jump,
                            taken_block,
                            &[] as &[BlockArg],
                            not_jump_block,
                            &[] as &[BlockArg],
                        );

                        fb.switch_to_block(not_jump_block);
                        match generic_block {
                            None => {
                                fb.ins().brif(
                                    code,
                                    spec_result_block,
                                    &[] as &[BlockArg],
                                    fall_block,
                                    &[] as &[BlockArg],
                                );
                            }
                            Some(generic_block) => {
                                let not_fall_block = fb.create_block();
                                fb.ins().brif(
                                    code,
                                    not_fall_block,
                                    &[] as &[BlockArg],
                                    fall_block,
                                    &[] as &[BlockArg],
                                );
                                fb.switch_to_block(not_fall_block);
                                let bail_const = fb.ins().iconst(types::I32, i64::from(SPEC_BAIL));
                                let is_bail = fb.ins().icmp(
                                    cranelift_codegen::ir::condcodes::IntCC::Equal,
                                    code,
                                    bail_const,
                                );
                                fb.ins().brif(
                                    is_bail,
                                    generic_block,
                                    &[] as &[BlockArg],
                                    spec_result_block,
                                    &[] as &[BlockArg],
                                );
                            }
                        }

                        fb.switch_to_block(taken_block);
                        // Branch taken: pc must name the next instruction
                        // before any interrupt exit or opcode call.
                        let state_v = fb.use_var(var_state);
                        let target_const = fb.ins().iconst(types::I32, i64::from(target));
                        fb.ins()
                            .store(MemFlags::trusted(), target_const, state_v, pc_offset);
                        if target <= pc {
                            // Backward branch: poll for interrupts.
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
                        }
                        fb.ins()
                            .jump(insn_blocks[target as usize], &[] as &[BlockArg]);
                    }
                }
                match generic_block {
                    // Fully handled by the helper.
                    None => continue,
                    // Emit the generic path below as the bail target. The pc
                    // store at block entry still holds (helpers that bail
                    // change nothing), so the wrapper sees the same state as
                    // under the interpreter.
                    Some(generic_block) => fb.switch_to_block(generic_block),
                }
            }

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

    /// Comparison-heavy queries exercising the specialized compare helper:
    /// numeric-affinity text operands (the cached no-conversion path),
    /// text-affinity comparisons (bail path), NULLs, and mixed
    /// numeric/text operands.
    #[test]
    fn jit_comparisons_match_interpreter() {
        if !runtime_enabled() {
            return;
        }
        let (db, conn) = memory_db();
        run_to_completion(
            &db,
            &conn,
            "CREATE TABLE ord(id INTEGER, ship DATE, note TEXT, qty REAL, disc REAL)",
            false,
        );
        run_to_completion(
            &db,
            &conn,
            "WITH RECURSIVE g(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM g WHERE x < 3000) \
             INSERT INTO ord SELECT x, \
               printf('19%02d-%02d-%02d', 94 + (x % 3), 1 + (x * 5) % 12, 1 + (x * 9) % 28), \
               CASE x % 5 WHEN 0 THEN NULL WHEN 1 THEN 'n' || x ELSE CAST(x AS TEXT) END, \
               (x * 7) % 50 + 0.5, \
               ((x * 13) % 11) / 100.0 \
             FROM g",
            false,
        );
        let queries = [
            // Numeric affinity, text column vs text constant: the cached
            // failed-conversion path on both sides.
            "SELECT count(*), sum(qty) FROM ord \
             WHERE ship >= '1994-06-01' AND ship < '1996-02-15'",
            // Numeric affinity where the column text DOES convert (note
            // holds plain digits for most rows) plus rows where it doesn't.
            "SELECT count(*) FROM ord WHERE note > '150'",
            // Text-affinity comparison between two text operands (bail).
            "SELECT count(*) FROM ord WHERE note < 'n2000'",
            // Numeric-numeric with floats and BETWEEN.
            "SELECT count(*) FROM ord WHERE disc BETWEEN 0.02 AND 0.07 AND qty < 24.5",
            // NULLs flowing through comparisons.
            "SELECT count(*) FROM ord WHERE note IS NULL OR note > 'zzz'",
            // Mixed: integer column against text constant under numeric
            // affinity (constant converts and is written back).
            "SELECT count(*) FROM ord WHERE id > '2500'",
        ];
        for sql in queries {
            let interpreted = run_to_completion(&db, &conn, sql, false);
            let jitted = run_to_completion(&db, &conn, sql, true);
            assert_eq!(interpreted, jitted, "row mismatch for query: {sql}");
        }
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
