# SIMD and Turso's decode paths

An investigation into whether Lemire-style SIMD techniques (masked VByte,
simdjson, vectorized UTF-8 validation) can speed up varint decoding and the
other decode paths in Turso.

**Verdict: no for varints, yes for three other things.** The varint decoder is
already within 2x of a hard architectural floor, and the two most valuable
findings that came out of the investigation are a soundness bug and a quadratic
algorithm — neither of which has anything to do with SIMD.

All measurements below are from an Intel Xeon (family 6 model 207, Emerald
Rapids class, `avx2`/`avx512f`/`bmi2`, core clock calibrated to ~3.14 GHz).

## 1. Why varints are the wrong target

### The format is not the obstacle

The intuitive objection is that SQLite's varint is MSB-first (`v = (v << 7) +
(c & 0x7f)`) while all published SIMD varint work targets LEB128, which is
LSB-first. That objection is wrong, and it is worth recording why so nobody
re-derives it.

Segmentation (finding value boundaries via the continuation bits) is *identical*
between the two formats. Only payload assembly differs, and that difference is
absorbed entirely into the `PSHUFB` control mask — ascending byte order for
LEB128, descending for SQLite — at zero instruction cost, because you are
already paying for an arbitrary byte permutation.

This was verified by taking masked VByte's recombination arithmetic verbatim,
changing only the shuffle mask, and checking against the scalar reference over
all 8,323,072 three-byte SQLite varints: 0 wrong. The control (LEB128 mask on
SQLite data) passed only on the 0.78% of values that are byte-palindromes.

simdutf already ships this exact design for UTF-8, which is likewise MSB-first,
with tables of the same shape.

The one genuine format difference is SQLite's 9th byte, which carries 8 payload
bits with no continuation flag. It costs one `AND` and one `CMOV`:

```rust
let t = (!mask) & 0xFF;
let len = if t != 0 { t.trailing_zeros() + 1 } else { 9 };
```

### The value distribution kills it

Histogram of every varint in every b-tree leaf page across four schemas
(60-column analytics, TPC-H `lineitem`-shaped, narrow key/value, large-blob),
2,152,032 varints total:

| population | 1 byte | 2 byte | 3 byte | 4+ byte |
|---|---|---|---|---|
| record header (serial types) — 1.95M | **99.79%** | 0.21% | 0% | **0%** |
| all varints incl. cell headers — 2.15M | 92.71% | 4.92% | 2.36% | **0%** |

Nothing in a real database exceeded 3 bytes. The elaborate 9-byte path is
effectively dead code outside large rowids.

Note the two populations have *opposite* distributions. Record-header serial
types are almost entirely 1 byte. Cell-header varints (`payload_size`, `rowid`)
are mostly 2–3 bytes, because `payload_size` is a sum over the whole row and
rowids grow with table size. Only the first population sits in a contiguous run.

### LLVM already generates the optimal code

This is the decisive finding. Disassembly of the current `read_varint` inlined
into a header walk:

```asm
.Ltop:  mov    %rsi,%r9
        sub    %rdx,%r9              ; remaining = len - pos
        je     .Lnone
        movzbl (%rdi,%rdx,1),%r11d   ; load buf[pos]
        mov    %r11d,%r8d
        and    $0x7f,%r8d            ; value
        mov    $0x1,%r10d            ; n = 1 as an IMMEDIATE, before the branch
        test   %r11b,%r11b
        jns    .Ladvance             ; taken 99.8% of the time
        ...unrolled 2/3/.../9-byte branch tree...
.Ladvance:
        add    %r8,%rcx              ; acc += v
        add    %r10,%rdx             ; pos += n
```

LLVM fully unrolls `for i in 0..8` into a straight-line branch tree with the
1-byte case first, and the length is a compile-time immediate on every exit
edge. So `pos += n` is a **1-cycle loop-carried recurrence**, and the branch
predictor completely hides the ~5-cycle L1 load latency — the load feeds only a
predicted branch, never the address chain.

Any wide-load scheme derives the length *from the loaded bytes*, which puts
`load(5) + bswap(2) + andn(1) + lzcnt(3) ≈ 12+ cycles` **into** that recurrence.

Measured, on the real 99.79/0.21 record-header distribution. All variants were
verified against the reference on 702,074 cases (every 7-bit boundary ±1, every
truncation of every encoding, invalid 9-byte encodings, exhaustive 1-byte and
2-byte buffers, 500k random) before timing, and all wide loads were kept
strictly in bounds via zero-padded stack copies:

| variant | cycles/decode | vs baseline |
|---|---|---|
| **baseline (current code)** | **2.07** | **1.00x** |
| fast-path peel + `#[cold]` tail | 2.05 | 1.01x |
| unrolled 1/2/3-byte tree | 2.05 | 1.01x |
| branchless 8-byte load + shift | 18.71 | 0.11x |
| branchless `bswap64` + `PEXT` | 17.30 | 0.12x |
| u32 load, resume from index 4 | 15.74 | 0.13x |

The baseline is 1.97 cycles on 100%-1-byte input, against a hard 1-cycle floor
set by the pointer recurrence.

### The economics, in one line

Misprediction is expensive — the same 50/50 1-byte/2-byte mix costs 2.69
cycles/decode when the pattern alternates predictably and 13.81 when it is
shuffled, so a mispredict is worth ~22 cycles. That is the prize branchless
decoding competes for.

But on *real* headers, everything non-1-byte costs **0.10 cycles/decode — 4.8%
of total**. Branchless schemes spend 15+ extra cycles to chase 0.10.

### This was already tried here

PR #4363 ("Optimized RecordCursor, Remove read_varint_fast", merged
2025-12-30) removed a pre-existing `read_varint_fast`. The author's conclusion:

> I tried many benchmarks and found out LLVM is generating better code for
> `read_varint` than anything that I wrote or was there before [...] and
> `read_varint_fast` was actually only slower for longer than 2 varints.

The struck-through section of that PR describes trying exactly the u32-load
approach, including "don't worry about the over reads." Reproduced here at
**0.13x**. The over-read premise was also wrong — see §4.

### Run length, for completeness

Even setting all the above aside, published SIMD varint decoders need long runs.
Masked VByte's own code refuses to enter its SIMD loop below ~112 values. Turso
decodes runs of 2 (cell headers) to ~30 (record headers).

## 2. What is actually wrong: `op_column` is quadratic

`op_column_fetch` (`core/vdbe/execute.rs:1910`) creates a **fresh**
`ValueIterator` for every `Insn::Column`:

```rust
let mut payload_iterator = record.iter()?;
```

and `nth_into_register(n)` (`core/vdbe/mod.rs:3048`) re-walks the header from
byte zero every time:

```rust
for _ in 0..n {
    let (serial_type, bytes_read) = read_varint(header)?;
    header = &header[bytes_read..];
    data_sum += get_serial_type_size(serial_type)?;
}
```

There is no per-row offset cache anywhere in the tree (the incremental-blob path
in `core/storage/btree.rs` is the sole exception, and it caches one column).
The emitter issues one `Insn::Column` per projected column
(`core/translate/expr/emission.rs:229`), so a `SELECT *` over N columns costs

    N(N+3)/2 varint decodes per row, where N+1 would do.

| table | varints/row now | minimum | redundancy |
|---|---|---|---|
| 10 columns | 65 | 11 | 5.9x |
| TPC-H `lineitem` (16) | 152 | 17 | 8.9x |
| TPC-C `customer` (21) | 252 | 22 | 11.5x |
| ClickBench `hits` (106) | **5,777** | 107 | **54x** |

Measured header-walk cost: 56–68 ns/row at 10 columns, 306–382 ns at 21, and
**5.7–7.7 µs/row at 106** — where it dwarfs value materialization by ~10x.

Fixing this is portable safe Rust and is worth **26–31x** on the header walk for
wide tables. It dominates anything a decoder micro-optimization could achieve by
three orders of magnitude.

Two design cautions, both measured:

- **Do not cache an offset array.** Memory-resident loop state roughly doubles
  the per-column walk cost via store-to-load forwarding. A naive always-on memo
  *regresses* the narrow-projection case that dominates OLTP: 1 column from a
  10-column table went 7.3 ns → 13.6 ns (forward-cursor memo) → 29.7 ns (full
  offset array).
- **Keep the walk state in registers.** The best-measured shape is a peephole
  pass fusing the run of ascending `Insn::Column` on one cursor into a single
  `ColumnRange` opcode — best in class at every width, with no narrow-projection
  regression because K=1 keeps today's path.

Related redundant work found along the way:

- `ImmutableRecord::last_value()` (`core/types.rs:1402`) calls `iter()?.last()`.
  `ValueIterator` overrides `next`/`nth`/`count`/`fold`/`size_hint` but **not
  `last`**, so the default implementation materializes a `ValueRef` for every
  column to reach the last one. Hit once per row on every index-driven scan.
  *Fixed*: `last` now walks the header and decodes exactly one value.
- **A corrupt record header hung the query.** `ValueIterator::next` returned a
  truncated-varint error *without consuming any input*, so every subsequent call
  reported the same error and `while let Some(..) = next()` never terminated.
  The default `Iterator::last` is exactly that shape, so a header whose final
  serial-type varint is cut off by `header_size` — payload `[03][80][80]` is
  enough — spun forever instead of surfacing corruption. Reachable from any
  index scan on a malformed file, with no write path involved. *Fixed*: `next`
  consumes the remaining header before reporting, fusing the iterator for all
  consumers. This was found while implementing the `last` fix, not during the
  original survey.
- An index scan parses the same header 3+ times per row (seek comparison,
  `last_value()` for the rowid, then per-column `op_column`).
- `op_idx_insert`'s uniqueness check does three parses and two heap `Vec`
  allocations per inserted row, via functions whose own doc comments say
  "Don't use this in performance critical paths".
- `op_column_has_field` walks the entire header via `column_count()` to answer a
  bounds question needing `column+1` varints.

## 3. The soundness bug

`core/storage/sqlite3_ondisk.rs:1091` (and identically at `:1219`):

```rust
// SAFETY: SerialTypeKind is Text so this buffer is a valid string
let val = unsafe { std::str::from_utf8_unchecked(data) };
```

The premise is false. `SerialTypeKind::Text` means the serial type is odd and
≥13 — it encodes the declared type and the length, and says nothing whatsoever
about byte validity. The bytes come from a file that may be corrupt or
attacker-supplied.

This is reachable through **plain SQL, with no corruption required**:

```sql
INSERT INTO t VALUES (CAST(x'fffe8041' AS TEXT));
-- typeof=text, length=3, hex=FFFE8041
```

which lands on disk as serial type 21 — odd, ≥13, so `Text`, length
`(21-13)/2 = 4`. Those bytes then flow into `from_utf8_unchecked`, producing a
`&str` that violates its safety invariant, which is then handed to `chars()`,
`char_indices()` and `find()` throughout the VDBE.

The codebase already knows. `core/vdbe/execute.rs:2327`:

```rust
// The blob may not be a real record — text fields could
// contain invalid UTF-8 (from_utf8_unchecked in the
// record decoder). Validate and demote to blob if needed.
```

with a regression test titled "Reproduces fuzzer bug at seed 27035". That patch
fixed one opcode; every other consumer still trusts the broken invariant.
`PRAGMA integrity_check` does not cover text encoding.

The SIMD connection is real but inverted: the reason not to validate was cost.
`std::str::from_utf8` is a scalar DFA at ~1.15 GB/s on 2-byte text; `simdutf8`
does ~12 GB/s, and ~83 GB/s on the ASCII path (a 2x gain over std even there).
Vectorized UTF-8 validation makes it affordable to turn validation *on*. That is
Lemire's technique applied as a correctness fix rather than a speedup, which is
the stronger argument in a codebase whose first principle is "crash > corrupt".

## 4. Why wide loads are unsafe here regardless

Any 8- or 16-byte load for a varint near the end of its buffer is unsafe in this
codebase, at both the language and the mapping level.

The page arena is allocated with exactly `rounded_slots * slot_size` bytes and
no tail padding (`core/storage/buffer_pool.rs:377`). Page sizes are powers of
two ≥512, so `rounded_bytes` is always an exact multiple of the OS page size —
the arena has **zero tail slack**, and the bitmap does hand out the last slot.
Slots are packed contiguously, so over-reading a non-terminal slot silently
reads an adjacent **live page buffer** owned by another thread.

That last case is the disqualifying one: no fault, no ASAN report (the arena is
an anonymous `mmap`, uninstrumented), just silently wrong data. Compare the
three cases:

| location | result of a K-byte over-read |
|---|---|
| non-terminal slot | **silent cross-buffer read** — no crash, no sanitizer hit |
| terminal slot | SIGSEGV (unmapped page) |
| heap fallback (`Pin<Box<[u8]>>`, exact-sized) | ASAN heap-buffer-overflow / Miri UB |

And varints legitimately live at `page_size - 1`: default `reserved_bytes = 0`,
so content runs to the physical last byte, and `read_btree_cell` deliberately
extends the non-overflowing payload slice to the page end.

simdjson's padding argument ("you can safely read beyond an allocated buffer as
long as you remain within an allocated page") is not merely unproven here — it
is *guaranteed false*.

Separately, `read_varint_partial`'s `Ok(None)` return is load-bearing control
flow, not just error reporting. The sorter (`core/vdbe/sorter.rs:691`), hash
table (`core/vdbe/hash_table.rs:2465`), and MVCC logical log
(`core/mvcc/persistent_storage/logical_log.rs:1315`) all use it to mean "torn
record, need more data". A branchless decoder returning a garbage length instead
of `None` converts a clean truncated-tail recovery into replaying a fabricated
op.

## 5. If the header walk is ever batched

The one shape that *does* work is checking eight bytes at a time for
all-single-byte varints, where the bytes then *are* the values:

```rust
if x & 0x8080_8080_8080_8080 == 0 { /* eight 1-byte varints, zero-extend */ }
```

Measured crossover is **exactly N=8** — mechanically obvious, since that is
where an 8-byte group first fits in bounds. Below 8 a naive bulk implementation
is a real **regression** (0.60–0.69x at N=4–7): the never-taken wide loop plus a
separate tail loop makes LLVM generate worse code than the plain scalar walk. Any
production version must leave the scalar loop untouched and gate the wide path so
short headers never enter it.

| N | scalar (today) | bulk u64 | AVX2 |
|---|---|---|---|
| 4 | 1.00x | 0.60x | 0.57x |
| 8 | 1.00x | 1.50x | 2.72x |
| 17 | 1.00x | 1.74x | 1.39x |
| 24 | 1.00x | 2.71x | 5.77x |
| 64 | 1.00x | 2.12x | 4.04x |

**Scalar `u64` arithmetic captures ~65–75% of what AVX2 achieves** (at N=64,
28.2 of 40.2 ns/header saved). AVX2 only pays from N≈24, i.e. ClickBench and
nothing else in `perf/`.

Two further cautions:

- Resuming the wide path after a multi-byte varint beats bailing to scalar for
  the rest of the header by 1.2–1.4x at N≥10.
- A whole-header "all single byte" check is too brittle. P(all N single-byte) is
  ~87% at N=64 and only ~50% for a large-text table. Three multi-byte serial
  types out of 106 erased the entire AVX2 gain in one measurement. It must be
  chunked per 8/16/32 bytes with per-chunk fallback.

Also note `get_serial_type_size` (`core/types.rs:3080`): the branchy `match`
measures 0.54–0.61 ns/element and a hand-written branchless version was *slower*
(0.62–0.71). A 128-entry byte table wins at 0.31–0.37 ns — **1.7x for ~5 lines,
zero risk**, independent of any batching.

## 6. Where SIMD would actually pay

None of these are varints. Ranked by (payoff x tractability):

1. **JSON *text* parsing** — `core/json/jsonb.rs:1798`. The "simple string" scan
   loop is `memchr3(quote, backslash, control)` written out longhand. Measured
   7.8x at 40 bytes, 27x at 2000. String literals are ~87% of bytes in typical
   row-payload JSON. The 4-entry `JsonCacheCell` misses every row on distinct
   payloads, so `SELECT json_extract(payload, '$.x') FROM t` re-parses per row.
   Note the *binary* JSONB path is a pointer chase and is anti-SIMD — it is the
   text format that vectorizes.
2. **LIKE/GLOB** — `core/vdbe/value.rs:1563`. The `%` lookahead scans
   char-by-char where `memchr2` would do; measured 8.9x at 32 bytes, 39x at 1024.
   `compare_chars` decodes to `char` then only does `eq_ignore_ascii_case`, so
   the decode buys nothing. Bigger still: there is **no compiled-pattern cache** —
   `constant_mask` is computed by the translator and then destructured away as
   `constant_mask: _` at `core/vdbe/execute.rs:7864`, so every row re-runs the
   whole preamble.
3. **UTF-8 validation** — as a correctness fix, per §3.

Non-SIMD findings worth more than most of the SIMD work:

- `extensions/regexp/src/lib.rs` and `core/regexp.rs` call `Regex::new` **per
  row**. Compilation dwarfs matching; an LRU cache is the highest
  value-per-line change found.
- `core/vdbe/hash_table.rs:47` feeds rapidhash **one byte at a time** via
  `write_u8`, defeating its 48-byte bulk path entirely.
- There is **no `.is_ascii()` call anywhere in `core/`**. `length()` does
  `chars().count()` per row purely to count; `substr()` makes three passes.

## 7. House style for any future SIMD

There are **zero hand-written SIMD intrinsics in the tree** — no
`is_x86_feature_detected!`, no `core::arch`, no `target_feature` anywhere
outside `target/`. `core/vector/operations/distance_*.rs` matches a "simd" grep
only because of the *crate name* `simsimd`.

The established pattern is: delegate to a crate, gate on platform at compile
time, keep a scalar twin, and prove agreement with a quickcheck property test.
`simsimd`, `crc32c`, `aegis` (AES-NI/VAES), `twox_hash`, and `fastbloom` all
follow it.

The portability tax for hand-rolled intrinsics is steep:

- ≥3 arch-specific unsafe kernels plus a scalar oracle, per routine converted.
- `armv7-linux-androideabi` cannot do it on stable at all —
  `is_arm_feature_detected!` is nightly-gated, and NEON is optional on ARMv7-A.
- `armv7-linux-androideabi`, `i686-linux-android` and `wasm32-wasip1-threads`
  are **build-only in CI** — no `cargo test` execution at all, so a
  wrong-but-compiling kernel ships undetected.
- The `clippy --deny=warnings` gate runs once on x86_64 Linux, so non-x86 `cfg`
  branches are never linted.
- Miri cannot execute most SIMD intrinsics, and Miri is already manual-only here.
- CodSpeed runs in `mode: simulation` — **instruction counts, not cycles** — and
  every job is `continue-on-error: true`. A SIMD change that trades instructions
  for ILP registers as a *regression*, and wall-clock tracking
  (`rust_perf.yml` → Nyrkiö) does not run on PRs at all.

Meanwhile `memchr` is already a direct dependency of `sqlite/parser` and is
already linked into `turso_core` transitively via `regex`. It provides
SIMD-accelerated `memchr2`/`memchr3`/`memmem` on every target Turso ships, with
no C toolchain, no unsafe code in-tree, and zero new compilation units. That is
where items 1 and 2 in §6 should come from.

## 8. Recommendations

1. **Do not SIMD the varint decoder.** It is at 2.07 cycles/decode against a
   1-cycle floor, LLVM already generates the optimal shape, and it was tried and
   reverted once already.
2. **Fix `op_column`'s quadratic header re-walk.** Portable safe Rust, 26–31x on
   the header walk for wide tables. Prefer a `ColumnRange` opcode over an offset
   cache; measure the narrow-projection case, which a naive memo regresses.
   *Not done* — this is an architectural change to the hottest opcode in the
   engine and wants its own PR with end-to-end benchmarks. Note for whoever
   picks it up: `ImmutableRecord` already has an `invalidate` /
   `start_serialization` protocol and only three `as_blob_mut` call sites (two
   of which *are* those methods), so a memo is more fenceable than it first
   looks — but a `ColumnRange` opcode decided at translation time needs no
   invalidation reasoning at all, and measured better besides.
3. **Fix the `from_utf8_unchecked` soundness bug.** Validate with `simdutf8`,
   ideally once per page rather than per value. *Not done* — needs a decision
   first on what to do with invalid bytes, since SQLite itself stores and
   returns them, so rejecting would break compatibility. The likely shape is
   holding `&[u8]` in `TextRef` and validating only where `&str` semantics are
   actually required.
4. **Use `memchr` for the JSON string scanner and the LIKE `%` lookahead**, with
   a short-input guard (crossover ~16–32 bytes). *Not done.*
5. **Cache compiled regexes and LIKE patterns.** Highest value per line found.
   *Not done.*
6. ~~Swap `get_serial_type_size`'s match for a 128-entry table.~~ **Done** — it
   runs in the same quadratic loop as `read_varint`, so it benefits from the
   same call-count reduction item 2 would deliver.

Items 1, and the two fixes marked *Done*, are on this branch. Everything else is
open.

Before any of it, note that there is **no varint benchmark in the repo** (19
benches in `core/benches/`, none touch varints) and `tpc_h_benchmark` is tracked
by nothing — excluded from `make bench-exclude-tpc-h`, excluded from CodSpeed
discovery, and its CI job is commented out.
