---
name: janitor
description: Clean up one directory of the codebase without changing behavior, as a series of small, independent git commits. Untangles structure (duplicated logic, hidden coupling, dead code, one-off special cases), makes the Rust idiomatic, and removes comments and tests that check nothing. Run as "/janitor <directory> [focus]", for example "/janitor core/security".
disable-model-invocation: true
argument-hint: "<directory> [optional focus, e.g. 'errors' or 'dead code']"
---

# Janitor

Clean up the directory given in the arguments so the next person (or agent) who changes
it reads less, and breaks less without noticing. Behavior stays the same. The result is
a branch of small commits, each one a single cleanup a reviewer can understand and
accept or drop on its own.

If no directory was given, stop and ask for one. If a focus was given (for example
"errors" or "dead code"), only make cleanups of that kind.

AGENTS.md is the authority. Where anything in this skill disagrees with it, or with the
`code-quality` skill, follow AGENTS.md.

## 0. Set up

1. Run `git status`. If the working tree has changes to tracked files, stop and ask: the
   janitor's commits must contain only its own changes. Untracked files are fine; never
   `git add` them.
2. If on `main`, create a branch: `git switch -c janitor/<directory with / replaced by ->`.
3. Find which crate owns the directory (nearest `Cargo.toml` upward) and note its package
   name. Per-commit checks use `-p <crate>`.
4. Run `cargo check -p <crate>` and the crate's tests once now, so you know which
   failures, if any, existed before you started. Don't fix unrelated failures.

## 1. Read and list problems

Don't change anything yet.

Read every file in the directory in full. Then read the code outside the directory that
calls into it (`rg` for its public items) and the code it calls, enough to know how it is
used. Run `git log --oneline -- <dir>` and look at a few recent commits to see which
files change together.

Write a list of problems to `.janitor/<directory-slug>/LOG.md` in your scratchpad
directory (not in the repo). Each entry is `file:line` plus one plain sentence. Look for
the following.

### Structure: should these pieces change together?

**Hidden links**: pieces that must change together, but nothing in the code shows it.

- The same logic, constant, or list written in two places.
- A module that depends on state another module sets up, with nothing in the types
  saying so (a call order callers just have to know, a flag set elsewhere).
- Closely related code split across files or modules so one change needs all of them.

**Needless links**: pieces tied together that change for different reasons.

- Callers reaching into another module's internals (`pub` fields, `pub(crate)` helpers
  that only make sense inside one module).
- A general function with a parameter only one caller understands.
- A module whose interface is about as large as its implementation, so it hides nothing.

**Surplus**: pieces with no reason of their own to exist.

- Dead code: unused functions, fields, enum variants, imports, `#[allow(dead_code)]`.
- Pass-through wrappers that only forward to another function.
- Traits with one implementation, generics with one instantiation, config or options
  that are never set to anything but the default.

**Special cases**: code that exists for one situation instead of following from how the
system works: an `if` for one value, a parameter for one caller, a swallowed error,
a retry, a sleep, an ignored test. Each one usually points at a missing piece of design.
Don't patch them one by one; group them under the fix that makes them unnecessary.

### Rust

- `unsafe` without a `// SAFETY:` comment; `unsafe fn` without a `# Safety` doc section.
- Reachable panics on input from outside (user SQL, file contents, network): `unwrap()`,
  indexing, `as` casts that truncate, unchecked arithmetic. Panics on states that are
  truly impossible are correct here (AGENTS.md: assert invariants); make sure they carry a
  message with the relevant values, and prefer `expect("why")` to a bare `unwrap()`.
- `if x.is_some() { x.unwrap() }` and similar check-then-unwrap; use `if let` or
  `let ... else`.
- Errors that lose information: `map_err(|_| ...)`, messages without the value that
  failed, `Result<_, String>` inside the engine where `LimboError` fits.
- "Shouldn't happen" branches that silently continue (`else { return Ok(()) }`,
  `_ => {}`) where an assertion or `unreachable!` belongs.
- `.clone()` that exists only to satisfy the borrow checker; try `std::mem::take`,
  `Option::take`, a shorter borrow, or splitting the struct first.
- `&String`, `&Vec<T>`, `&Box<T>`, `&Option<T>` parameters instead of `&str`, `&[T]`,
  `&T`, `Option<&T>`.
- `bool` parameters on functions called from more than one place; use a two-variant enum.
- Strings or integer sentinels (`-1`, `""`, `0` meaning "none") where an enum or `Option`
  fits. "Field X is only valid when Y" where a data-carrying enum fits.
- `get_` prefixes on getters; `as_`/`to_`/`into_` names that don't match the cost.
  `Manager`/`Helper`/`Util` names. Names that use invented jargon instead of saying what
  the thing is (AGENTS.md "Use plain language").
- Hand loops that read better as an iterator chain, and iterator chains long enough that
  a loop reads better.
- `HashMap` iteration order leaking into output, errors, or tests.
- `#[allow(...)]` without a reason; prefer `#[expect(lint, reason = "...")]`.
- Wider visibility than needed: `pub` that could be `pub(crate)`, `pub(crate)` that could
  be private.
- Allocation in hot paths that can be avoided (AGENTS.md: microsecond = long time).

### Layout and comments

- Functions not in top-to-bottom order: a helper should sit below its callers
  (AGENTS.md "Code flows from top to bottom").
- Single-use helpers that mean nothing outside their caller; move them inside it
  (`code-quality` skill).
- Comments that restate the code, describe who calls something, or narrate history
  ("previously", "now", "changed to"). Remove them. Keep comments that explain why, that
  describe something outside the code (an SQLite quirk, a file-format rule), and every
  `// SAFETY:` comment.
- Doc comments that only restate the name.

### Tests

- Tests that can't fail, or that only repeat the implementation (restated constants,
  mirrored branches). Remove them.
- Tests that depend on internals when the behavior they care about is observable
  through a public API. Rewrite them against the API.
- Never weaken a test that checks real behavior, and never delete one because it fails.

### Not in scope

- New features, new public API, or behavior changes, including changes to error
  messages or SQLSTATEs users can see.
- Changes to the on-disk format or anything that affects existing databases
  (AGENTS.md "Backward compatibility").
- `#[non_exhaustive]`, new `Debug`/`Default` derives, or new doc comments "for
  completeness". Only add what a concrete problem on the list calls for.
- Code outside the directory, except call sites that must change because a signature in
  the directory changed. Those go in the same commit.

## 2. Decide the fixes

Group the problems under fixes. Several problems often share one fix that looks nothing
like any of them: design it as if today's requirements had been known from the start. A
good fix puts pieces that change together in one place, removes ties between pieces that
don't, and makes everything depend on things that change less often than it does.

Write each fix in the log above the problems it solves:

```md
- Move the permission bit constants into `permission.rs` and derive the role table
  from them, so a new permission is added in one place.
  - `roles.rs:40` repeats the bit values from `permission.rs:12`.
  - `policy.rs:88` has its own copy of the "all permissions" mask.
```

Then order the fixes into commits:

- One fix per commit. A commit that needs "and" in its subject is two commits.
- Mechanical, low-risk cleanups first (dead code, imports, visibility, comments, ordering),
  so the structural commits that follow have less noise in their diffs.
- A commit must not depend on a later one. Each commit must build and pass the crate's
  tests on its own.
- Moving code and changing code are separate commits. A reviewer can check a pure move
  quickly, but not a move mixed with edits.

If a problem can't be fixed without changing behavior, or the fix is large and
risky (for example, a redesign that touches many callers outside the directory), don't
make it. Put it under "Not done" in the final report with the reason.

## 3. Make the commits

For each planned commit:

1. Make the change. Follow the `code-quality` skill and AGENTS.md. Code in `core/` that
   does I/O follows the `async-io-model` skill.
2. `cargo fmt`.
3. `cargo check -p <crate> --all-targets`, then run the crate's tests (or the tests
   that exercise the files you changed, when the crate's suite is very slow). If a test
   fails, your change caused it: debug it, don't revert it and move on.
4. Look at `git diff` and confirm it contains only this one cleanup.
5. Commit with a message in the AGENTS.md format: a `<scope>: ` prefix matching the
   directory (for example `core/security: `), a lowercase imperative summary, and a body
   that says why the change makes the code easier to change. Don't narrate the diff.

   ```text
   core/security: define permission bits in one place

   roles.rs and policy.rs each kept their own copy of the permission bit
   values, so adding a permission meant editing three files and nothing
   failed if one was missed. The role table and the all-permissions mask
   are now derived from the constants in permission.rs.
   ```

### Bugs found along the way

The janitor does not change behavior, but it will sometimes find a real bug. Don't mix a
bug fix into a cleanup commit. Instead, give it its own commit containing a regression
test, and confirm that the test fails without the fix and passes with it (AGENTS.md
"Prove changes with tests"). If you can't write such a test, don't fix the bug; list it
in the final report.

## 4. Check and repeat

After all planned commits:

1. Run `cargo clippy -p <crate> --all-features --all-targets -- --deny=warnings` and fix
   anything your commits introduced. Put each fix into the commit that introduced the
   problem: `git commit --fixup <sha>`, then
   `GIT_SEQUENCE_EDITOR=true git rebase -i --autosquash main`.
2. Start one fresh subagent (`general-purpose`) and give it the directory, the list of
   problem categories from step 1, and the `git diff main...HEAD`. Ask it to report
   remaining problems with `file:line`, and to say if any commit changes behavior.
   Don't give it your log; you want a reading that isn't shaped by yours.
3. Fix what it found that is worth fixing, as new commits. Repeat until a pass finds
   nothing worth fixing.
4. Run the crate's full test suite once more. If the directory is SQL-facing (translate,
   vdbe, parser), also run the relevant `.sqltest` corpus as described in the `testing`
   skill.

## 5. Report

End with a short report to the user:

- The branch name, and `git log --oneline main..HEAD`.
- For each commit, one line on what it fixed.
- Any bug-fix commits, with the test that proves each one.
- "Not done": problems you found but left alone, and why.
- What you ran to check the result, and whether anything failed.

Do not push or open a PR unless the user asks.
