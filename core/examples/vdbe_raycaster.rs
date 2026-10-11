//! A first-person raycaster hand-assembled in raw VDBE bytecode — no SQL in the hot path.
//!
//! This is a proof of concept for using Turso's bytecode engine as a general-purpose
//! compilation target. The game's RAM is a 16KB blob held in a single VDBE register and
//! accessed with the MemLoad/MemStore/MemCopy linear-memory opcodes. Persistence works
//! the way a database treats its own buffer pool:
//!
//!   - each game tic is one write transaction, hand-assembled with ProgramBuilder;
//!   - at tic start the program boot-loads RAM from the `mem` table with the engine's
//!     ordinary cursor opcodes (SeekRowid/Column);
//!   - every MemStore marks a card-table bitmap, and at tic end only the dirty 4KB
//!     pages are written back (MakeRecord/Insert) and committed;
//!   - the rendered frame is returned as the statement's result row.
//!
//! Because every committed state is complete and consistent, you can kill the process
//! at any point and resume from the database file — the default demo does exactly that
//! halfway through. Run with:
//!
//! ```sh
//! cargo run -p turso_core --features bench,fs --example vdbe_raycaster            # scripted demo
//! cargo run -p turso_core --features bench,fs --example vdbe_raycaster -- --play  # live: wasd moves, f fires
//! cargo run -p turso_core --features bench,fs --example vdbe_raycaster -- --bench # interpreter MIPS
//! ```

use std::io::{BufRead, Write as _};
use std::os::unix::io::AsRawFd;
use std::sync::Arc;

use turso_core::vdbe::builder::{CursorType, ProgramBuilder, ProgramBuilderOpts};
use turso_core::vdbe::insn::{CmpInsFlags, InsertFlags, Insn, MemWidth, RegisterOrLiteral};
use turso_core::vdbe::BranchOffset;
use turso_core::{Connection, Database, Numeric, QueryMode, Statement, UnixIO, Value, IO};

// ---------------------------------------------------------------------------
// Memory layout: 16KB of "RAM" as four 4KB pages, rows 1..=4 of the mem table.
// ---------------------------------------------------------------------------
const PAGE_SIZE: i64 = 4096;
const PAGE_SHIFT: u8 = 12;
const N_PAGES: i64 = 4;
const RAM_BYTES: usize = (N_PAGES * PAGE_SIZE) as usize;

// Page 0: player state, all 8-byte little-endian slots (16.16 fixed point).
const ST_PX: i64 = 0x00;
const ST_PY: i64 = 0x08;
const ST_DX: i64 = 0x10;
const ST_DY: i64 = 0x18;
const ST_TIC: i64 = 0x20;
const ST_HP: i64 = 0x28;
const ST_KILLS: i64 = 0x30;
// Muzzle-flash frames left; doubles as the refire cooldown.
const ST_FLASH: i64 = 0x38;
// World-format marker at the end of the state block. Bump WORLD_MAGIC whenever the
// RAM layout changes: resuming an old-layout save would read garbage state.
const ST_MAGIC: i64 = 0xF8;
// "VDBE", layout v2.
const WORLD_MAGIC: i64 = 0x5644_4245_0000_0002;
// Monsters follow at 0x40: N_MON entries of (x, y, hp), 8 bytes each.
const MON_BASE: i64 = 0x40;
const MON_STRIDE: i64 = 24;
const MON_X: i64 = 0;
const MON_Y: i64 = 8;
const MON_HP: i64 = 16;
const N_MON: i64 = 4;
const PLAYER_MAX_HP: i64 = 10;
// Page 1: the world map, 16x16 cells, one byte each (0 = empty, 1 = wall).
const MAP_BASE: i64 = 0x1000;
const MAP_W: i64 = 16;
// Page 2: framebuffer (64x20 palette bytes) and the per-column wall-depth buffer
// the sprite pass tests against (64 fixed-point distances).
const FB_BASE: i64 = 0x2000;
const FB_W: i64 = 64;
const FB_H: i64 = 20;
const DEPTH_BASE: i64 = 0x2600;
const FAR: f64 = 999.0; // depth value for "no wall within range"

// 16.16 fixed point.
const F_ONE: i64 = 65536;
fn fp(v: f64) -> i64 {
    (v * F_ONE as f64).round() as i64
}

// Input key codes (rows in the input table).
const KEY_LEFT: i64 = 1;
const KEY_RIGHT: i64 = 2;
const KEY_FWD: i64 = 3;
const KEY_BACK: i64 = 4;
const KEY_FIRE: i64 = 5;

const MAP: [&str; 16] = [
    "################",
    "#..............#",
    "#..##....##....#",
    "#..##....##..###",
    "#..............#",
    "#......##......#",
    "#......##......#",
    "#..###.....#...#",
    "#..###.....#...#",
    "#..............#",
    "#....#....#....#",
    "#....#....#....#",
    "#....######....#",
    "#..............#",
    "#..............#",
    "################",
];

// ---------------------------------------------------------------------------
// A tiny assembler over ProgramBuilder.
// ---------------------------------------------------------------------------
struct Asm {
    b: ProgramBuilder,
    /// Register permanently holding integer 0; used as the source offset for MemCopy
    /// from freshly-created blobs.
    zero: usize,
    /// Register permanently holding 16, the fixed-point shift amount.
    sixteen: usize,
    /// Scratch register used by the addressed-load/store helpers.
    scratch_addr: usize,
    /// Scratch registers used by the compound helpers (rotate, load_map_cell).
    t1: usize,
    t2: usize,
    t3: usize,
}

impl Asm {
    fn new(mut b: ProgramBuilder) -> Self {
        b.prologue();
        b.begin_write_operation().unwrap();
        let zero = b.alloc_register();
        let sixteen = b.alloc_register();
        let scratch_addr = b.alloc_register();
        let t1 = b.alloc_register();
        let t2 = b.alloc_register();
        let t3 = b.alloc_register();
        b.emit_insn(Insn::Integer {
            value: 0,
            dest: zero,
        });
        b.emit_insn(Insn::Integer {
            value: 16,
            dest: sixteen,
        });
        Self {
            b,
            zero,
            sixteen,
            scratch_addr,
            t1,
            t2,
            t3,
        }
    }

    fn emit(&mut self, insn: Insn) {
        self.b.emit_insn(insn);
    }
    fn reg(&mut self) -> usize {
        self.b.alloc_register()
    }
    fn int(&mut self, value: i64) -> usize {
        let dest = self.reg();
        self.emit(Insn::Integer { value, dest });
        dest
    }
    fn set_int(&mut self, dest: usize, value: i64) {
        self.emit(Insn::Integer { value, dest });
    }
    fn label(&mut self) -> BranchOffset {
        self.b.allocate_label()
    }
    fn here(&mut self, label: BranchOffset) {
        self.b.preassign_label_to_next_insn(label);
    }
    fn goto(&mut self, label: BranchOffset) {
        self.emit(Insn::Goto { target_pc: label });
    }
    fn copy(&mut self, src: usize, dst: usize) {
        self.emit(Insn::Copy {
            src_reg: src,
            dst_reg: dst,
            extra_amount: 0,
        });
    }
    fn add(&mut self, lhs: usize, rhs: usize, dest: usize) {
        self.emit(Insn::Add { lhs, rhs, dest });
    }
    fn sub(&mut self, lhs: usize, rhs: usize, dest: usize) {
        self.emit(Insn::Subtract { lhs, rhs, dest });
    }
    fn mul(&mut self, lhs: usize, rhs: usize, dest: usize) {
        self.emit(Insn::Multiply { lhs, rhs, dest });
    }
    /// dest = (lhs * rhs) >> 16 — fixed-point multiply.
    fn fmul(&mut self, lhs: usize, rhs: usize, dest: usize) {
        self.mul(lhs, rhs, dest);
        self.emit(Insn::ShiftRight {
            lhs: dest,
            rhs: self.sixteen,
            dest,
        });
    }
    /// dest = lhs >> 16 — fixed point to integer.
    fn shr16(&mut self, lhs: usize, dest: usize) {
        self.emit(Insn::ShiftRight {
            lhs,
            rhs: self.sixteen,
            dest,
        });
    }
    /// Jump to `target` if lhs < rhs.
    fn jump_lt(&mut self, lhs: usize, rhs: usize, target: BranchOffset) {
        self.emit(Insn::Lt {
            lhs,
            rhs,
            target_pc: target,
            flags: CmpInsFlags::default(),
            collation: None,
        });
    }
    /// Jump to `target` if lhs == rhs.
    fn jump_eq(&mut self, lhs: usize, rhs: usize, target: BranchOffset) {
        self.emit(Insn::Eq {
            lhs,
            rhs,
            target_pc: target,
            flags: CmpInsFlags::default(),
            collation: None,
        });
    }
    /// Jump to `target` if lhs > rhs.
    fn jump_gt(&mut self, lhs: usize, rhs: usize, target: BranchOffset) {
        self.emit(Insn::Gt {
            lhs,
            rhs,
            target_pc: target,
            flags: CmpInsFlags::default(),
            collation: None,
        });
    }
    /// Jump to `target` if lhs >= rhs.
    fn jump_ge(&mut self, lhs: usize, rhs: usize, target: BranchOffset) {
        self.emit(Insn::Ge {
            lhs,
            rhs,
            target_pc: target,
            flags: CmpInsFlags::default(),
            collation: None,
        });
    }
    /// Jump to `target` if reg is true (non-zero).
    fn jump_if(&mut self, reg: usize, target: BranchOffset) {
        self.emit(Insn::If {
            reg,
            target_pc: target,
            jump_if_null: false,
        });
    }
    /// dest = |src|.
    fn abs_into(&mut self, src: usize, dest: usize) {
        let l_done = self.label();
        self.copy(src, dest);
        self.jump_ge(src, self.zero, l_done);
        self.sub(self.zero, src, dest);
        self.here(l_done);
    }

    /// dest = RAM[slot], where slot is a constant byte offset (8-byte slots).
    fn load_slot(&mut self, mem: usize, slot: i64, dest: usize) {
        self.set_int(self.scratch_addr, slot);
        self.emit(Insn::MemLoad {
            mem,
            addr: self.scratch_addr,
            width: MemWidth::W8,
            signed: true,
            dest,
        });
    }
    /// RAM[slot] = src, marking the card table.
    fn store_slot(&mut self, mem: usize, cards: usize, slot: i64, src: usize) {
        self.set_int(self.scratch_addr, slot);
        self.emit(Insn::MemStore {
            mem,
            addr: self.scratch_addr,
            width: MemWidth::W8,
            src,
            card_table: Some(cards),
            page_shift: PAGE_SHIFT,
        });
    }
    /// dest = RAM[base_reg + off]: an 8-byte load at a runtime-computed address.
    fn load_at(&mut self, mem: usize, base_reg: usize, off: i64, dest: usize) {
        self.set_int(self.scratch_addr, off);
        self.add(base_reg, self.scratch_addr, self.scratch_addr);
        self.emit(Insn::MemLoad {
            mem,
            addr: self.scratch_addr,
            width: MemWidth::W8,
            signed: true,
            dest,
        });
    }
    /// RAM[base_reg + off] = src: an 8-byte store at a runtime-computed address.
    fn store_at(&mut self, mem: usize, cards: usize, base_reg: usize, off: i64, src: usize) {
        self.set_int(self.scratch_addr, off);
        self.add(base_reg, self.scratch_addr, self.scratch_addr);
        self.emit(Insn::MemStore {
            mem,
            addr: self.scratch_addr,
            width: MemWidth::W8,
            src,
            card_table: Some(cards),
            page_shift: PAGE_SHIFT,
        });
    }
    /// dest = map[(y >> 16) * 16 + (x >> 16)], one byte. Clobbers the scratch registers,
    /// so `dest` and the operands must not be one of them.
    fn load_map_cell(&mut self, mem: usize, x_fp: usize, y_fp: usize, dest: usize) {
        let (t1, t2) = (self.t1, self.t2);
        let map_w = self.int(MAP_W);
        let map_base = self.int(MAP_BASE);
        self.shr16(y_fp, t1);
        self.mul(t1, map_w, t1);
        self.shr16(x_fp, t2);
        self.add(t1, t2, t1);
        self.add(t1, map_base, t1);
        self.emit(Insn::MemLoad {
            mem,
            addr: t1,
            width: MemWidth::W1,
            signed: false,
            dest,
        });
    }
    /// Rotate the fixed-point vector (dx, dy) in place by the angle whose cosine and
    /// sine live in registers `cos_r`/`sin_r`. Clobbers the scratch registers.
    fn rotate(&mut self, dx: usize, dy: usize, cos_r: usize, sin_r: usize) {
        let (t1, t2, t3) = (self.t1, self.t2, self.t3);
        // t3 = dx*cos - dy*sin; dy = dx*sin + dy*cos; dx = t3
        self.fmul(dx, cos_r, t1);
        self.fmul(dy, sin_r, t2);
        self.sub(t1, t2, t3);
        self.fmul(dx, sin_r, t1);
        self.fmul(dy, cos_r, t2);
        self.add(t1, t2, dy);
        self.copy(t3, dx);
    }
}

// ---------------------------------------------------------------------------
// The per-tic program: boot from the mem table, simulate + render one tic,
// flush dirty pages, return the frame.
// ---------------------------------------------------------------------------
fn build_tic_program(conn: &Arc<Connection>) -> Statement {
    let (mem_table, input_table) = conn.with_schema_mut(|schema| {
        (
            schema.get_btree_table("mem").expect("mem table"),
            schema.get_btree_table("input").expect("input table"),
        )
    });

    let builder = ProgramBuilder::new(
        QueryMode::Normal,
        None,
        ProgramBuilderOpts::new(2, 1024, 96),
    );
    let mut a = Asm::new(builder);

    // Result row registers must be contiguous:
    // [frame_blob, tic, dirty_pages, px, py, hp, kills].
    let r_out = a.b.alloc_registers(7);

    let r_mem = a.reg(); // the 16KB RAM blob
    let r_cards = a.reg(); // the dirty-page bitmap
    a.emit(Insn::Blob {
        value: vec![0u8; RAM_BYTES],
        dest: r_mem,
    });
    a.emit(Insn::Blob {
        value: vec![0u8; 1],
        dest: r_cards,
    });

    // --- Boot: load all four RAM pages from the mem table. -----------------
    let root_page = mem_table.root_page;
    let c_mem = a.b.alloc_cursor_id(CursorType::BTreeTable(mem_table));
    a.emit(Insn::OpenWrite {
        cursor_id: c_mem,
        root_page: RegisterOrLiteral::Literal(root_page),
        db: 0,
    });
    let l_missing = a.label();
    let r_rowid = a.reg();
    let r_page_blob = a.reg();
    let r_dst_addr = a.reg();
    let r_page_len = a.int(PAGE_SIZE);
    for page in 0..N_PAGES {
        a.set_int(r_rowid, page + 1);
        a.emit(Insn::SeekRowid {
            cursor_id: c_mem,
            src_reg: r_rowid,
            target_pc: l_missing,
        });
        a.emit(Insn::Column {
            cursor_id: c_mem,
            column: 1,
            dest: r_page_blob,
            default: None,
        });
        a.set_int(r_dst_addr, page * PAGE_SIZE);
        a.emit(Insn::MemCopy {
            dest_mem: r_mem,
            dest_addr: r_dst_addr,
            src_mem: r_page_blob,
            src_addr: a.zero,
            len: r_page_len,
            card_table: None,
            page_shift: PAGE_SHIFT,
        });
    }

    // --- Load player state from RAM. ---------------------------------------
    let r_px = a.reg();
    let r_py = a.reg();
    let r_dx = a.reg();
    let r_dy = a.reg();
    let r_tic = a.reg();
    let r_hp = a.reg();
    let r_kills = a.reg();
    let r_flash = a.reg();
    a.load_slot(r_mem, ST_PX, r_px);
    a.load_slot(r_mem, ST_PY, r_py);
    a.load_slot(r_mem, ST_DX, r_dx);
    a.load_slot(r_mem, ST_DY, r_dy);
    a.load_slot(r_mem, ST_TIC, r_tic);
    a.load_slot(r_mem, ST_HP, r_hp);
    a.load_slot(r_mem, ST_KILLS, r_kills);
    a.load_slot(r_mem, ST_FLASH, r_flash);

    let t1 = a.reg();
    let t2 = a.reg();

    // --- Input: look up this tic in the input table and apply it. ----------
    let c_in =
        a.b.alloc_cursor_id(CursorType::BTreeTable(input_table.clone()));
    a.emit(Insn::OpenRead {
        cursor_id: c_in,
        root_page: input_table.root_page,
        db: 0,
    });
    let l_no_input = a.label();
    let l_applied = a.label();
    // Initialized before the seek: the no-input path skips the whole dispatch block.
    let r_fire = a.int(0);
    a.emit(Insn::SeekRowid {
        cursor_id: c_in,
        src_reg: r_tic,
        target_pc: l_no_input,
    });
    let r_key = a.reg();
    a.emit(Insn::Column {
        cursor_id: c_in,
        column: 1,
        dest: r_key,
        default: None,
    });

    // Turn constants: 12 degrees per key press.
    let rot = 12.0f64.to_radians();
    let r_cos_rot = a.int(fp(rot.cos()));
    let r_sin_rot = a.int(fp(rot.sin()));
    let r_sin_rot_n = a.int(fp(-rot.sin()));

    let l_left = a.label();
    let l_right = a.label();
    let l_fwd = a.label();
    let l_back = a.label();
    let l_fire = a.label();
    let r_k = a.int(KEY_LEFT);
    a.jump_eq(r_key, r_k, l_left);
    a.set_int(r_k, KEY_RIGHT);
    a.jump_eq(r_key, r_k, l_right);
    a.set_int(r_k, KEY_FWD);
    a.jump_eq(r_key, r_k, l_fwd);
    a.set_int(r_k, KEY_BACK);
    a.jump_eq(r_key, r_k, l_back);
    a.set_int(r_k, KEY_FIRE);
    a.jump_eq(r_key, r_k, l_fire);
    a.goto(l_applied);

    a.here(l_fire);
    a.set_int(r_fire, 1);
    a.goto(l_applied);

    a.here(l_left);
    a.rotate(r_dx, r_dy, r_cos_rot, r_sin_rot_n);
    a.goto(l_applied);
    a.here(l_right);
    a.rotate(r_dx, r_dy, r_cos_rot, r_sin_rot);
    a.goto(l_applied);

    // Forward/back: candidate position, walk only into empty cells.
    let r_speed = a.reg();
    let r_nx = a.reg();
    let r_ny = a.reg();
    let r_cell = a.reg();
    let emit_move = |a: &mut Asm, speed: i64| {
        a.set_int(r_speed, speed);
        a.fmul(r_dx, r_speed, t1);
        a.add(r_px, t1, r_nx);
        a.fmul(r_dy, r_speed, t1);
        a.add(r_py, t1, r_ny);
        a.load_map_cell(r_mem, r_nx, r_ny, r_cell);
        a.jump_if(r_cell, l_applied); // wall: stay put
        a.copy(r_nx, r_px);
        a.copy(r_ny, r_py);
        a.goto(l_applied);
    };
    a.here(l_fwd);
    emit_move(&mut a, fp(0.30));
    a.here(l_back);
    emit_move(&mut a, fp(-0.30));

    a.here(l_no_input);
    a.here(l_applied);

    let r_one = a.int(1);
    let r_ten = a.int(10);

    // --- Monster AI: chase the player, claw when adjacent. ------------------
    // Monsters move axis-separated (walls block them), stop within attack range,
    // and scratch the player for 1 hp every 8th tic while adjacent.
    let r_mi = a.reg();
    let r_maddr = a.reg();
    let r_mx = a.reg();
    let r_my = a.reg();
    let r_mhp = a.reg();
    let r_adx = a.reg();
    let r_ady = a.reg();
    let r_ms = a.reg();
    let r_stride = a.int(MON_STRIDE);
    let r_monb = a.int(MON_BASE);
    let r_nmon = a.int(N_MON);
    let r_c7 = a.int(7);
    let r_atk = a.int(fp(0.8));
    let r_dead = a.int(fp(0.15));
    let mon_speed = fp(0.06);
    a.set_int(r_mi, 0);
    let l_mon = a.label();
    a.here(l_mon);
    {
        let l_next = a.label();
        let l_chase = a.label();
        let l_chase_y = a.label();
        let l_mstore = a.label();
        a.mul(r_mi, r_stride, r_maddr);
        a.add(r_maddr, r_monb, r_maddr);
        a.load_at(r_mem, r_maddr, MON_HP, r_mhp);
        a.jump_lt(r_mhp, r_one, l_next); // dead monsters rest
        a.load_at(r_mem, r_maddr, MON_X, r_mx);
        a.load_at(r_mem, r_maddr, MON_Y, r_my);
        a.sub(r_px, r_mx, t1);
        a.abs_into(t1, r_adx);
        a.sub(r_py, r_my, t1);
        a.abs_into(t1, r_ady);
        // Adjacent on both axes: attack instead of moving.
        a.jump_ge(r_adx, r_atk, l_chase);
        a.jump_ge(r_ady, r_atk, l_chase);
        a.emit(Insn::BitAnd {
            lhs: r_tic,
            rhs: r_c7,
            dest: t1,
        });
        a.jump_if(t1, l_next);
        a.sub(r_hp, r_one, r_hp);
        a.jump_ge(r_hp, a.zero, l_next);
        a.set_int(r_hp, 0);
        a.goto(l_next);

        a.here(l_chase);
        let l_step_x = a.label();
        a.jump_lt(r_adx, r_dead, l_chase_y);
        a.set_int(r_ms, mon_speed);
        a.jump_lt(r_mx, r_px, l_step_x);
        a.set_int(r_ms, -mon_speed);
        a.here(l_step_x);
        a.add(r_mx, r_ms, r_nx);
        a.load_map_cell(r_mem, r_nx, r_my, r_cell);
        a.jump_if(r_cell, l_chase_y);
        a.copy(r_nx, r_mx);
        a.here(l_chase_y);
        let l_step_y = a.label();
        a.jump_lt(r_ady, r_dead, l_mstore);
        a.set_int(r_ms, mon_speed);
        a.jump_lt(r_my, r_py, l_step_y);
        a.set_int(r_ms, -mon_speed);
        a.here(l_step_y);
        a.add(r_my, r_ms, r_ny);
        a.load_map_cell(r_mem, r_mx, r_ny, r_cell);
        a.jump_if(r_cell, l_mstore);
        a.copy(r_ny, r_my);
        a.here(l_mstore);
        a.store_at(r_mem, r_cards, r_maddr, MON_X, r_mx);
        a.store_at(r_mem, r_cards, r_maddr, MON_Y, r_my);
        a.here(l_next);
    }
    a.add(r_mi, r_one, r_mi);
    a.jump_lt(r_mi, r_nmon, l_mon);

    // --- Render: one ray per framebuffer column. ----------------------------
    // Start with the leftmost ray (player direction rotated by -FOV/2), and rotate
    // it by FOV/64 for each successive column.
    let half_fov = 30.0f64.to_radians();
    let delta = (60.0f64 / FB_W as f64).to_radians();
    let r_cos_hfov = a.int(fp(half_fov.cos()));
    let r_sin_hfov_n = a.int(fp(-half_fov.sin()));
    let r_cos_delta = a.int(fp(delta.cos()));
    let r_sin_delta = a.int(fp(delta.sin()));

    let r_rx = a.reg();
    let r_ry = a.reg();
    a.copy(r_dx, r_rx);
    a.copy(r_dy, r_ry);
    a.rotate(r_rx, r_ry, r_cos_hfov, r_sin_hfov_n);

    let march_step = fp(0.08);
    let max_steps = 170; // ~13.6 cells of range
    let r_step_fp = a.int(march_step);
    let r_max_steps = a.int(max_steps);
    let r_col = a.int(0);
    let r_fb_w = a.int(FB_W);
    let r_fb_h = a.int(FB_H);

    // Per-column state.
    let r_sx = a.reg(); // ray step x/y (ray dir * march step)
    let r_sy = a.reg();
    let r_x = a.reg(); // marching position
    let r_y = a.reg();
    let r_steps = a.reg();
    let r_h = a.reg(); // half-height of the wall slice, in rows
    let r_wall_ch = a.reg();
    let r_row = a.reg();
    let r_top = a.reg();
    let r_bot = a.reg();
    let r_ch = a.reg();
    let r_dist = a.reg();
    // h = (10 << 32) / dist_fp: at 1 cell the wall fills the screen.
    let r_h_num = a.int(10 * F_ONE * F_ONE);
    let r_ch_space = a.int(b' ' as i64);
    let r_ch_floor = a.int(b'.' as i64);

    let l_col_loop = a.label();
    a.here(l_col_loop);
    {
        // March the ray until it leaves empty space.
        a.fmul(r_rx, r_step_fp, r_sx);
        a.fmul(r_ry, r_step_fp, r_sy);
        a.copy(r_px, r_x);
        a.copy(r_py, r_y);
        a.set_int(r_steps, 0);
        let l_march = a.label();
        let l_hit = a.label();
        let l_no_hit = a.label();
        let l_draw = a.label();
        a.here(l_march);
        a.add(r_x, r_sx, r_x);
        a.add(r_y, r_sy, r_y);
        a.add(r_steps, r_one, r_steps);
        a.load_map_cell(r_mem, r_x, r_y, r_cell);
        a.jump_if(r_cell, l_hit);
        a.jump_lt(r_steps, r_max_steps, l_march);

        a.here(l_no_hit);
        a.set_int(r_h, 0);
        a.set_int(r_wall_ch, b'-' as i64);
        a.set_int(r_dist, fp(FAR));
        a.goto(l_draw);

        a.here(l_hit);
        // dist = steps * march_step (fixed point); h = wall half-height in rows.
        a.mul(r_steps, r_step_fp, r_dist);
        a.emit(Insn::Divide {
            lhs: r_h_num,
            rhs: r_dist,
            dest: r_h,
        });
        a.emit(Insn::ShiftRight {
            lhs: r_h,
            rhs: a.sixteen,
            dest: r_h,
        });
        let l_clamped = a.label();
        let l_shade = a.label();
        a.jump_lt(r_h, r_ten, l_clamped);
        a.copy(r_ten, r_h);
        a.here(l_clamped);
        // Shade by distance: near '#', mid '+', far '-'.
        a.set_int(r_wall_ch, b'#' as i64);
        a.set_int(t1, fp(3.0));
        a.jump_lt(r_dist, t1, l_shade);
        a.set_int(r_wall_ch, b'+' as i64);
        a.set_int(t1, fp(7.0));
        a.jump_lt(r_dist, t1, l_shade);
        a.set_int(r_wall_ch, b'-' as i64);
        a.here(l_shade);

        a.here(l_draw);
        // Record this column's wall distance for the sprite pass depth test.
        a.set_int(t1, 8);
        a.mul(r_col, t1, t1);
        a.set_int(t2, DEPTH_BASE);
        a.add(t1, t2, t1);
        a.emit(Insn::MemStore {
            mem: r_mem,
            addr: t1,
            width: MemWidth::W8,
            src: r_dist,
            card_table: Some(r_cards),
            page_shift: PAGE_SHIFT,
        });
        // top = 10 - h, bot = 10 + h; rows [top, bot) get the wall character.
        a.sub(r_ten, r_h, r_top);
        a.add(r_ten, r_h, r_bot);
        a.set_int(r_row, 0);
        let l_row_loop = a.label();
        let l_ceiling = a.label();
        let l_floor = a.label();
        let l_put = a.label();
        a.here(l_row_loop);
        a.jump_lt(r_row, r_top, l_ceiling);
        a.jump_gt(r_row, r_bot, l_floor);
        a.copy(r_wall_ch, r_ch);
        a.goto(l_put);
        a.here(l_ceiling);
        a.copy(r_ch_space, r_ch);
        a.goto(l_put);
        a.here(l_floor);
        a.copy(r_ch_floor, r_ch);
        a.here(l_put);
        // fb[row * 64 + col] = ch
        a.mul(r_row, r_fb_w, t1);
        a.add(t1, r_col, t1);
        a.set_int(t2, FB_BASE);
        a.add(t1, t2, t1);
        a.emit(Insn::MemStore {
            mem: r_mem,
            addr: t1,
            width: MemWidth::W1,
            src: r_ch,
            card_table: Some(r_cards),
            page_shift: PAGE_SHIFT,
        });
        a.add(r_row, r_one, r_row);
        a.jump_lt(r_row, r_fb_h, l_row_loop);
    }
    // Rotate the ray toward the next column and continue.
    a.rotate(r_rx, r_ry, r_cos_delta, r_sin_delta);
    a.add(r_col, r_one, r_col);
    a.jump_lt(r_col, r_fb_w, l_col_loop);

    // Shared sprite/fire registers: the perpendicular projection of a monster onto
    // the view — forward = rel·dir, right = rel·perp(dir).
    let r_relx = a.reg();
    let r_rely = a.reg();
    let r_fwd = a.reg();
    let r_rgt = a.reg();
    let r_minf = a.int(fp(0.2));
    let emit_projection = |a: &mut Asm| {
        a.sub(r_mx, r_px, r_relx);
        a.sub(r_my, r_py, r_rely);
        a.fmul(r_relx, r_dx, t1);
        a.fmul(r_rely, r_dy, t2);
        a.add(t1, t2, r_fwd);
        a.fmul(r_rely, r_dx, t1);
        a.fmul(r_relx, r_dy, t2);
        a.sub(t1, t2, r_rgt);
    };

    // --- Fire: hitscan straight ahead. ---------------------------------------
    // The nearest live monster inside the aim cone and closer than the wall at the
    // center column takes one damage. Damage goes through a runtime-computed RAM
    // address (best_addr + MON_HP).
    let l_fire_done = a.label();
    let r_best = a.reg();
    let r_best_addr = a.reg();
    let r_hitf = a.reg();
    let r_aim = a.int(fp(0.35));
    a.emit(Insn::IfNot {
        reg: r_fire,
        target_pc: l_fire_done,
        jump_if_null: true,
    });
    a.jump_gt(r_flash, a.zero, l_fire_done); // still cooling down
    a.set_int(r_flash, 3);
    // Wall distance straight ahead = the depth buffer at the center column.
    let r_wall = a.reg();
    a.set_int(t1, DEPTH_BASE + (FB_W / 2) * 8);
    a.emit(Insn::MemLoad {
        mem: r_mem,
        addr: t1,
        width: MemWidth::W8,
        signed: true,
        dest: r_wall,
    });
    a.set_int(r_best, fp(FAR));
    a.set_int(r_hitf, 0);
    a.set_int(r_mi, 0);
    let l_fmon = a.label();
    a.here(l_fmon);
    {
        let l_next = a.label();
        a.mul(r_mi, r_stride, r_maddr);
        a.add(r_maddr, r_monb, r_maddr);
        a.load_at(r_mem, r_maddr, MON_HP, r_mhp);
        a.jump_lt(r_mhp, r_one, l_next);
        a.load_at(r_mem, r_maddr, MON_X, r_mx);
        a.load_at(r_mem, r_maddr, MON_Y, r_my);
        emit_projection(&mut a);
        a.jump_lt(r_fwd, r_minf, l_next);
        a.abs_into(r_rgt, t1);
        a.jump_ge(t1, r_aim, l_next);
        a.jump_ge(r_fwd, r_wall, l_next); // a wall is in the way
        a.jump_ge(r_fwd, r_best, l_next);
        a.copy(r_fwd, r_best);
        a.copy(r_maddr, r_best_addr);
        a.set_int(r_hitf, 1);
        a.here(l_next);
    }
    a.add(r_mi, r_one, r_mi);
    a.jump_lt(r_mi, r_nmon, l_fmon);
    a.emit(Insn::IfNot {
        reg: r_hitf,
        target_pc: l_fire_done,
        jump_if_null: true,
    });
    a.load_at(r_mem, r_best_addr, MON_HP, r_mhp);
    a.sub(r_mhp, r_one, r_mhp);
    a.store_at(r_mem, r_cards, r_best_addr, MON_HP, r_mhp);
    a.jump_gt(r_mhp, a.zero, l_fire_done);
    a.add(r_kills, r_one, r_kills);
    a.here(l_fire_done);

    // --- Sprites: draw live monsters, z-tested against the depth buffer. -----
    let r_ratio = a.reg();
    let r_scol = a.reg();
    let r_sh = a.reg();
    let r_sw = a.reg();
    let r_sc = a.reg();
    let r_send = a.reg();
    // Sprite half-height: shorter than a wall at the same distance.
    let r_spr_num = a.int(6 * F_ONE * F_ONE);
    let r_eight = a.int(8);
    a.set_int(r_mi, 0);
    let l_smon = a.label();
    a.here(l_smon);
    {
        let l_next = a.label();
        a.mul(r_mi, r_stride, r_maddr);
        a.add(r_maddr, r_monb, r_maddr);
        a.load_at(r_mem, r_maddr, MON_HP, r_mhp);
        a.jump_lt(r_mhp, r_one, l_next);
        a.load_at(r_mem, r_maddr, MON_X, r_mx);
        a.load_at(r_mem, r_maddr, MON_Y, r_my);
        emit_projection(&mut a);
        a.jump_lt(r_fwd, r_minf, l_next);
        // Screen column: 32 + (right/forward) / tan(FOV/2) * 32 ≈ 32 + ratio*55.
        a.emit(Insn::ShiftLeft {
            lhs: r_rgt,
            rhs: a.sixteen,
            dest: t1,
        });
        a.emit(Insn::Divide {
            lhs: t1,
            rhs: r_fwd,
            dest: r_ratio,
        });
        a.set_int(t1, 55);
        a.mul(r_ratio, t1, t1);
        a.shr16(t1, t1);
        a.set_int(t2, FB_W / 2);
        a.add(t1, t2, r_scol);
        // Height and width scale with 1/distance.
        a.emit(Insn::Divide {
            lhs: r_spr_num,
            rhs: r_fwd,
            dest: r_sh,
        });
        a.shr16(r_sh, r_sh);
        let l_hmin = a.label();
        let l_hmax = a.label();
        a.jump_ge(r_sh, r_one, l_hmin);
        a.copy(r_one, r_sh);
        a.here(l_hmin);
        a.jump_lt(r_sh, r_eight, l_hmax);
        a.copy(r_eight, r_sh);
        a.here(l_hmax);
        let l_wmin = a.label();
        a.emit(Insn::ShiftRight {
            lhs: r_sh,
            rhs: r_one,
            dest: r_sw,
        });
        a.jump_ge(r_sw, r_one, l_wmin);
        a.copy(r_one, r_sw);
        a.here(l_wmin);
        a.sub(r_scol, r_sw, r_sc);
        a.add(r_scol, r_sw, r_send);
        let l_scol = a.label();
        let l_scnext = a.label();
        a.here(l_scol);
        a.jump_gt(r_sc, r_send, l_next);
        a.jump_lt(r_sc, a.zero, l_scnext);
        a.jump_ge(r_sc, r_fb_w, l_next);
        // Depth test against the wall (and previously drawn sprites).
        a.set_int(t1, 8);
        a.mul(r_sc, t1, t1);
        a.set_int(t2, DEPTH_BASE);
        a.add(t1, t2, t1);
        a.emit(Insn::MemLoad {
            mem: r_mem,
            addr: t1,
            width: MemWidth::W8,
            signed: true,
            dest: t2,
        });
        a.jump_ge(r_fwd, t2, l_scnext);
        a.emit(Insn::MemStore {
            mem: r_mem,
            addr: t1,
            width: MemWidth::W8,
            src: r_fwd,
            card_table: Some(r_cards),
            page_shift: PAGE_SHIFT,
        });
        a.sub(r_ten, r_sh, r_top);
        a.add(r_ten, r_sh, r_bot);
        a.copy(r_top, r_row);
        let l_srow = a.label();
        a.here(l_srow);
        a.jump_gt(r_row, r_bot, l_scnext);
        a.mul(r_row, r_fb_w, t1);
        a.add(t1, r_sc, t1);
        a.set_int(t2, FB_BASE);
        a.add(t1, t2, t1);
        a.set_int(r_ch, b'M' as i64);
        a.emit(Insn::MemStore {
            mem: r_mem,
            addr: t1,
            width: MemWidth::W1,
            src: r_ch,
            card_table: Some(r_cards),
            page_shift: PAGE_SHIFT,
        });
        a.add(r_row, r_one, r_row);
        a.goto(l_srow);
        a.here(l_scnext);
        a.add(r_sc, r_one, r_sc);
        a.goto(l_scol);
        a.here(l_next);
    }
    a.add(r_mi, r_one, r_mi);
    a.jump_lt(r_mi, r_nmon, l_smon);

    // --- HUD overlay: crosshair, or muzzle flash while the gun cools down. ---
    let l_flash = a.label();
    let l_putx = a.label();
    a.set_int(r_ch, b'o' as i64);
    a.jump_gt(r_flash, a.zero, l_flash);
    a.goto(l_putx);
    a.here(l_flash);
    a.set_int(r_ch, b'*' as i64);
    a.sub(r_flash, r_one, r_flash);
    a.here(l_putx);
    a.set_int(t1, FB_BASE + 10 * FB_W + FB_W / 2);
    a.emit(Insn::MemStore {
        mem: r_mem,
        addr: t1,
        width: MemWidth::W1,
        src: r_ch,
        card_table: Some(r_cards),
        page_shift: PAGE_SHIFT,
    });

    // --- Write player state back to RAM (dirties page 0). -------------------
    a.add(r_tic, r_one, r_tic);
    a.store_slot(r_mem, r_cards, ST_PX, r_px);
    a.store_slot(r_mem, r_cards, ST_PY, r_py);
    a.store_slot(r_mem, r_cards, ST_DX, r_dx);
    a.store_slot(r_mem, r_cards, ST_DY, r_dy);
    a.store_slot(r_mem, r_cards, ST_TIC, r_tic);
    a.store_slot(r_mem, r_cards, ST_HP, r_hp);
    a.store_slot(r_mem, r_cards, ST_KILLS, r_kills);
    a.store_slot(r_mem, r_cards, ST_FLASH, r_flash);

    // --- Checkpoint: flush only the dirty pages back to the mem table. ------
    let r_dirty_count = a.int(0);
    let r_card_bits = a.reg();
    a.emit(Insn::MemLoad {
        mem: r_cards,
        addr: a.zero,
        width: MemWidth::W1,
        signed: false,
        dest: r_card_bits,
    });
    // MakeRecord needs contiguous registers: (rowid-alias NULL, page blob).
    let r_rec_start = a.b.alloc_registers(2);
    let r_rec = a.reg();
    let r_bit = a.reg();
    for page in 0..N_PAGES {
        let l_skip = a.label();
        a.set_int(r_bit, 1 << page);
        a.emit(Insn::BitAnd {
            lhs: r_card_bits,
            rhs: r_bit,
            dest: t1,
        });
        a.emit(Insn::IfNot {
            reg: t1,
            target_pc: l_skip,
            jump_if_null: true,
        });
        a.set_int(r_rowid, page + 1);
        a.emit(Insn::SeekRowid {
            cursor_id: c_mem,
            src_reg: r_rowid,
            target_pc: l_missing,
        });
        a.emit(Insn::Null {
            dest: r_rec_start,
            dest_end: None,
        });
        a.emit(Insn::Blob {
            value: vec![0u8; PAGE_SIZE as usize],
            dest: r_rec_start + 1,
        });
        a.set_int(r_dst_addr, page * PAGE_SIZE);
        a.emit(Insn::MemCopy {
            dest_mem: r_rec_start + 1,
            dest_addr: a.zero,
            src_mem: r_mem,
            src_addr: r_dst_addr,
            len: r_page_len,
            card_table: None,
            page_shift: PAGE_SHIFT,
        });
        a.emit(Insn::MakeRecord {
            start_reg: r_rec_start as u16,
            count: 2,
            dest_reg: r_rec as u16,
            index_name: None,
            affinity_str: None,
        });
        a.emit(Insn::Insert {
            cursor: c_mem,
            key_reg: r_rowid,
            record_reg: r_rec,
            flag: InsertFlags(InsertFlags::SKIP_LAST_ROWID),
            table_name: "mem".to_string(),
        });
        a.add(r_dirty_count, r_one, r_dirty_count);
        a.here(l_skip);
    }

    // --- Result row: [frame, tic, dirty pages flushed, px, py]. -------------
    let r_fb_len = a.int(FB_W * FB_H);
    a.emit(Insn::Blob {
        value: vec![0u8; (FB_W * FB_H) as usize],
        dest: r_out,
    });
    a.set_int(r_dst_addr, FB_BASE);
    a.emit(Insn::MemCopy {
        dest_mem: r_out,
        dest_addr: a.zero,
        src_mem: r_mem,
        src_addr: r_dst_addr,
        len: r_fb_len,
        card_table: None,
        page_shift: PAGE_SHIFT,
    });
    a.copy(r_tic, r_out + 1);
    a.copy(r_dirty_count, r_out + 2);
    a.copy(r_px, r_out + 3);
    a.copy(r_py, r_out + 4);
    a.copy(r_hp, r_out + 5);
    a.copy(r_kills, r_out + 6);
    a.b.emit_result_row(r_out, 7);
    let l_end = a.label();
    a.goto(l_end);

    a.here(l_missing);
    a.b.emit_halt_err(1, "raycaster RAM page missing from mem table".to_string());

    a.here(l_end);
    let Asm { mut b, .. } = a;
    conn.with_schema_mut(|schema| b.epilogue(schema));
    let program = b
        .build(conn.clone(), false, "raycaster tic (hand-assembled)")
        .expect("build tic program");
    Statement::new(program, conn.get_pager(), QueryMode::Normal, 0)
}

// ---------------------------------------------------------------------------
// Interpreter throughput measurement: a tight add-compare-branch loop.
// ---------------------------------------------------------------------------
fn run_bench(conn: &Arc<Connection>) {
    const ITERS: i64 = 20_000_000;
    let builder = ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 16, 4));
    let mut a = Asm::new(builder);
    let r_i = a.int(0);
    let r_n = a.int(ITERS);
    let r_one = a.int(1);
    let l_loop = a.label();
    a.here(l_loop);
    a.add(r_i, r_one, r_i);
    a.jump_lt(r_i, r_n, l_loop);
    a.b.emit_result_row(r_i, 1);
    let Asm { mut b, .. } = a;
    conn.with_schema_mut(|schema| b.epilogue(schema));
    let program = b
        .build(conn.clone(), false, "vdbe throughput loop")
        .expect("build bench program");
    let mut stmt = Statement::new(program, conn.get_pager(), QueryMode::Normal, 0);

    let insns = ITERS * 2; // Add + Lt per iteration
    let start = std::time::Instant::now();
    let rows = stmt.run_collect_rows().expect("bench run");
    let elapsed = start.elapsed();
    assert_eq!(rows.len(), 1);
    println!(
        "executed {} VDBE instructions in {:.2?}: {:.1}M insn/s",
        insns,
        elapsed,
        insns as f64 / elapsed.as_secs_f64() / 1e6
    );
    println!(
        "(build without optimizations is the floor; a release build runs several times faster)"
    );
}

// ---------------------------------------------------------------------------
// Host: schema setup, frame loop, crash-and-resume.
// ---------------------------------------------------------------------------
fn open_db(path: &str) -> (Arc<Database>, Arc<Connection>) {
    let io: Arc<dyn IO> = Arc::new(UnixIO::new().expect("io"));
    let db = Database::open_file(io, path).expect("open database");
    let conn = db.connect().expect("connect");
    (db, conn)
}

fn query_one(conn: &Arc<Connection>, sql: &str) -> Option<Vec<Value>> {
    let mut stmt = conn.query(sql).expect("query").expect("statement");
    stmt.run_collect_rows().expect("rows").into_iter().next()
}

fn initial_ram_image() -> Vec<u8> {
    let mut ram = vec![0u8; RAM_BYTES];
    let mut put = |slot: i64, v: i64| {
        ram[slot as usize..slot as usize + 8].copy_from_slice(&v.to_le_bytes());
    };
    put(ST_PX, fp(2.5));
    put(ST_PY, fp(4.5));
    put(ST_DX, fp(1.0));
    put(ST_DY, fp(0.0));
    put(ST_TIC, 0);
    put(ST_HP, PLAYER_MAX_HP);
    put(ST_KILLS, 0);
    put(ST_FLASH, 0);
    put(ST_MAGIC, WORLD_MAGIC);
    let spawns = [(12.5, 2.5), (8.5, 9.5), (3.5, 10.5), (13.5, 13.5)];
    for (i, (mx, my)) in spawns.iter().enumerate() {
        let base = MON_BASE + i as i64 * MON_STRIDE;
        put(base + MON_X, fp(*mx));
        put(base + MON_Y, fp(*my));
        put(base + MON_HP, 1);
    }
    for (row, line) in MAP.iter().enumerate() {
        for (col, ch) in line.bytes().enumerate() {
            ram[(MAP_BASE + row as i64 * MAP_W + col as i64) as usize] =
                if ch == b'#' { 1 } else { 0 };
        }
    }
    ram
}

fn setup_fresh_world(conn: &Arc<Connection>, seed_script: bool) {
    conn.execute("CREATE TABLE IF NOT EXISTS mem (page INTEGER PRIMARY KEY, data BLOB)")
        .expect("create mem");
    conn.execute("CREATE TABLE IF NOT EXISTS input (tic INTEGER PRIMARY KEY, key INTEGER)")
        .expect("create input");
    let page_count = match query_one(conn, "SELECT count(*) FROM mem") {
        Some(row) => match row[0] {
            Value::Numeric(Numeric::Integer(n)) => n,
            _ => 0,
        },
        None => 0,
    };
    if page_count == N_PAGES {
        // Only resume a save whose RAM layout matches this binary.
        let magic =
            query_one(conn, "SELECT data FROM mem WHERE page = 1").and_then(|row| match &row[0] {
                Value::Blob(data) if data.len() >= (ST_MAGIC + 8) as usize => {
                    Some(i64::from_le_bytes(
                        data[ST_MAGIC as usize..(ST_MAGIC + 8) as usize]
                            .try_into()
                            .unwrap(),
                    ))
                }
                _ => None,
            });
        if magic == Some(WORLD_MAGIC) {
            println!("resuming the saved world (use --reset to start over)...");
            return;
        }
        println!("saved world uses an older RAM layout — reseeding...");
    } else if page_count > 0 {
        println!("saved world is incomplete — reseeding...");
    } else {
        println!("seeding a fresh world into the database...");
    }
    conn.execute("DELETE FROM mem").expect("clear mem");
    conn.execute("DELETE FROM input").expect("clear input");
    let ram = initial_ram_image();
    for page in 0..N_PAGES {
        let start = (page * PAGE_SIZE) as usize;
        let hex: String = ram[start..start + PAGE_SIZE as usize]
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        conn.execute(format!("INSERT INTO mem VALUES ({}, x'{hex}')", page + 1))
            .expect("insert page");
    }
    if !seed_script {
        return;
    }
    // A scripted demo path through the map: forward runs, turns, and the occasional
    // trigger pull at whatever wandered into the crosshair.
    let script: Vec<i64> = [
        (KEY_FWD, 10),
        (KEY_FIRE, 1),
        (KEY_RIGHT, 4),
        (KEY_FWD, 8),
        (KEY_FIRE, 1),
        (KEY_FWD, 4),
        (KEY_LEFT, 5),
        (KEY_FIRE, 1),
        (KEY_FWD, 6),
        (KEY_FIRE, 1),
        (KEY_FWD, 4),
        (KEY_RIGHT, 8),
        (KEY_FIRE, 1),
        (KEY_FWD, 10),
        (KEY_FIRE, 1),
        (KEY_FWD, 4),
        (KEY_RIGHT, 5),
        (KEY_FWD, 8),
        (KEY_FIRE, 1),
        (KEY_LEFT, 6),
        (KEY_FIRE, 1),
        (KEY_FWD, 8),
    ]
    .iter()
    .flat_map(|&(key, n)| std::iter::repeat_n(key, n))
    .collect();
    for (tic, key) in script.iter().enumerate() {
        conn.execute(format!("INSERT INTO input VALUES ({tic}, {key})"))
            .expect("insert input");
    }
}

struct TicResult {
    frame: Vec<u8>,
    tic: i64,
    dirty_pages: i64,
    px: i64,
    py: i64,
    hp: i64,
    kills: i64,
}

fn run_tic(conn: &Arc<Connection>) -> TicResult {
    let mut stmt = build_tic_program(conn);
    let rows = stmt.run_collect_rows().expect("tic");
    let row = rows.into_iter().next().expect("tic result row");
    let as_int = |v: &Value| match v {
        Value::Numeric(Numeric::Integer(n)) => *n,
        other => panic!("expected integer, got {other:?}"),
    };
    let Value::Blob(frame) = &row[0] else {
        panic!("expected frame blob");
    };
    TicResult {
        frame: frame.clone(),
        tic: as_int(&row[1]),
        dirty_pages: as_int(&row[2]),
        px: as_int(&row[3]),
        py: as_int(&row[4]),
        hp: as_int(&row[5]),
        kills: as_int(&row[6]),
    }
}

/// Set by --ascii: draw with plain ASCII glyphs for terminals that render the
/// Unicode blocks badly. Colors are kept either way.
static ASCII_MODE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// The framebuffer stores one palette byte per cell; the host maps it to a colored
/// glyph, exactly the way Doom's palettized framebuffer met the VGA hardware.
fn cell_style(b: u8) -> (&'static str, u8) {
    let ascii = ASCII_MODE.load(std::sync::atomic::Ordering::Relaxed);
    match b {
        b'#' => (if ascii { "#" } else { "\u{2588}" }, 166), // near wall, burnt orange
        b'+' => (if ascii { "%" } else { "\u{2593}" }, 130), // mid wall
        b'-' => (if ascii { "-" } else { "\u{2591}" }, 94),  // far wall
        b'.' => (if ascii { "." } else { "\u{00b7}" }, 240), // floor
        b'M' => ("@", 196),                                  // monster, bright red
        b'*' => ("*", 226),                                  // muzzle flash, yellow
        b'o' => ("+", 250),                                  // crosshair
        _ => (" ", 0),
    }
}

fn draw(result: &TicResult, note: &str) {
    let mut out = String::from("\x1b[H");
    let mut fg = 0u8;
    for row in 0..FB_H as usize {
        let line = &result.frame[row * FB_W as usize..(row + 1) * FB_W as usize];
        for &b in line {
            let (glyph, color) = cell_style(b);
            if color != fg {
                out.push_str(&format!("\x1b[38;5;{color}m"));
                fg = color;
            }
            out.push_str(glyph);
        }
        out.push_str("\x1b[0m\x1b[K\n");
        fg = 0;
    }
    let hp_color = if result.hp <= 3 { 196 } else { 46 };
    out.push_str(&format!(
        "\x1b[38;5;{hp_color}mHP {:2}\x1b[0m  \x1b[38;5;226mkills {}/{}\x1b[0m  tic {:4}  pos ({:.2}, {:.2})  dirty pages: {}  {}\x1b[K\n",
        result.hp,
        result.kills,
        N_MON,
        result.tic,
        result.px as f64 / F_ONE as f64,
        result.py as f64 / F_ONE as f64,
        result.dirty_pages,
        note,
    ));
    print!("{out}");
    std::io::stdout().flush().ok();
}

/// Puts the terminal in non-canonical, no-echo, non-blocking mode so the game loop can
/// poll the keyboard between frames. The saved settings are restored on drop.
struct RawTty {
    fd: i32,
    saved: libc::termios,
}

impl RawTty {
    fn enable() -> Option<RawTty> {
        let fd = std::io::stdin().as_raw_fd();
        unsafe {
            if libc::isatty(fd) == 0 {
                return None;
            }
            let mut saved: libc::termios = std::mem::zeroed();
            if libc::tcgetattr(fd, &mut saved) != 0 {
                return None;
            }
            let mut raw = saved;
            raw.c_lflag &= !(libc::ICANON | libc::ECHO);
            raw.c_cc[libc::VMIN] = 0; // read() returns immediately even with no input
            raw.c_cc[libc::VTIME] = 0;
            if libc::tcsetattr(fd, libc::TCSANOW, &raw) != 0 {
                return None;
            }
            Some(RawTty { fd, saved })
        }
    }

    /// Drain whatever keystrokes arrived since the last frame.
    fn pending_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        let mut buf = [0u8; 64];
        loop {
            let n =
                unsafe { libc::read(self.fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
            if n <= 0 {
                break;
            }
            out.extend_from_slice(&buf[..n as usize]);
            if (n as usize) < buf.len() {
                break;
            }
        }
        out
    }
}

impl Drop for RawTty {
    fn drop(&mut self) {
        unsafe {
            libc::tcsetattr(self.fd, libc::TCSANOW, &self.saved);
        }
    }
}

/// Live game loop: frames advance on a fixed tic cadence; the keyboard is polled each
/// frame, and the most recent key pressed becomes that tic's input. Holding a key works
/// through the terminal's auto-repeat.
fn play_live(conn: &Arc<Connection>, tty: &RawTty) {
    const FRAME: std::time::Duration = std::time::Duration::from_millis(45);
    println!("live mode: hold or tap w/s to move, a/d to turn, f or space to fire, q to quit");
    let mut result = run_tic(conn);
    loop {
        let frame_start = std::time::Instant::now();
        let mut key = None;
        for b in tty.pending_bytes() {
            match b.to_ascii_lowercase() {
                b'w' => key = Some(KEY_FWD),
                b's' => key = Some(KEY_BACK),
                b'a' => key = Some(KEY_LEFT),
                b'd' => key = Some(KEY_RIGHT),
                b'f' | b' ' => key = Some(KEY_FIRE),
                b'q' => return,
                _ => {}
            }
        }
        if let Some(key) = key {
            conn.execute(format!(
                "INSERT OR REPLACE INTO input VALUES ({}, {key})",
                result.tic
            ))
            .expect("record input");
        }
        result = run_tic(conn);
        let note = if result.kills == N_MON {
            "AREA CLEARED  [wasd move, f fire, q quit]"
        } else {
            "[wasd move, f fire, q quit]"
        };
        draw(&result, note);
        if result.hp <= 0 {
            println!("\n\x1b[38;5;196mYOU DIED.\x1b[0m your death was durably committed. --play --reset to respawn.");
            return;
        }
        std::thread::sleep(FRAME.saturating_sub(frame_start.elapsed()));
    }
}

/// Fallback for non-TTY stdin (pipes, CI): type a burst of moves and press enter;
/// each recognized key advances one tic.
fn play_line_buffered(conn: &Arc<Connection>) {
    println!("controls: type moves and press enter — e.g. 'wwwwfff'.");
    println!("          w/s move, a/d turn, f fires, plain enter coasts one tic, q quits.");
    let stdin = std::io::stdin();
    let mut result = run_tic(conn);
    draw(&result, "(state persists in vdbe-raycaster.db)");
    loop {
        print!("> ");
        std::io::stdout().flush().ok();
        let mut line = String::new();
        if stdin.lock().read_line(&mut line).unwrap_or(0) == 0 {
            return;
        }
        let mut keys = Vec::new();
        for ch in line.trim().chars() {
            match ch.to_ascii_lowercase() {
                'w' => keys.push(KEY_FWD),
                's' => keys.push(KEY_BACK),
                'a' => keys.push(KEY_LEFT),
                'd' => keys.push(KEY_RIGHT),
                'f' | ' ' => keys.push(KEY_FIRE),
                'q' => return,
                _ => {}
            }
        }
        if keys.is_empty() {
            result = run_tic(conn);
            draw(&result, "(state persists in vdbe-raycaster.db)");
            continue;
        }
        // Each key becomes the input for one tic, so 'wwww' walks four tics.
        for key in keys {
            conn.execute(format!(
                "INSERT OR REPLACE INTO input VALUES ({}, {key})",
                result.tic
            ))
            .expect("record input");
            result = run_tic(conn);
            draw(&result, "(state persists in vdbe-raycaster.db)");
            std::thread::sleep(std::time::Duration::from_millis(35));
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let interactive = args.iter().any(|a| a == "--play");
    let bench = args.iter().any(|a| a == "--bench");
    let reset = args.iter().any(|a| a == "--reset");
    if args.iter().any(|a| a == "--ascii") {
        ASCII_MODE.store(true, std::sync::atomic::Ordering::Relaxed);
    }
    let db_path = "vdbe-raycaster.db";

    if bench {
        let (_db, conn) = open_db(":memory:");
        run_bench(&conn);
        return;
    }

    // The scripted demo always starts from a fresh world: it depends on its input
    // script, which a previous run would already have consumed. Interactive mode
    // owns the saved world and resumes it.
    if reset || !interactive {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{db_path}{suffix}"));
        }
    }

    let (db, conn) = open_db(db_path);
    setup_fresh_world(&conn, !interactive);
    print!("\x1b[2J");

    if interactive {
        if let Some(tty) = RawTty::enable() {
            play_live(&conn, &tty);
        } else {
            play_line_buffered(&conn);
        }
        println!(
            "\nsaved. run --play again to resume exactly here, or --play --reset to start over."
        );
        return;
    }

    // Scripted demo: run half the tics, "crash", reopen the database file, resume.
    const TICS_PER_SESSION: i64 = 46;
    for _ in 0..TICS_PER_SESSION {
        let result = run_tic(&conn);
        draw(&result, "(session 1)");
        std::thread::sleep(std::time::Duration::from_millis(45));
    }
    drop(conn);
    drop(db);
    println!("\n-- process 'crashed'; every tic was a committed transaction --");
    println!("-- reopening {db_path} and resuming from the last committed tic --\n");
    std::thread::sleep(std::time::Duration::from_millis(1200));

    let (_db, conn) = open_db(db_path);
    print!("\x1b[2J");
    for _ in 0..TICS_PER_SESSION {
        let result = run_tic(&conn);
        draw(&result, "(session 2: resumed from the database)");
        std::thread::sleep(std::time::Duration::from_millis(45));
    }
    println!("\ndone. the whole game state lives in {db_path}; copy it to another machine and keep going.");
}
