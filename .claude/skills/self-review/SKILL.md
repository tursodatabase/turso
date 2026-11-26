---
name: self-review
description: How to prepare a turso pull request and review it yourself before you publish it. Use when you start a fix, before you open or push a PR, or when the user asks you to "review my PR" or "self-review". Covers comparing with SQLite and PostgreSQL, the test that fails without the fix, the risk check, and a subagent that tries to break the change.
---
# Self-review

Review rounds on agent-written PRs find the same things again and again: a rule fixed in one path but not in the others, a test that passes without the fix, a result that SQLite or PostgreSQL would not give, a helper that already exists, a claim in the description that nothing ran. Find these yourself before you publish.

## Light or full pass

Do the full pass if the change touches type rules, conversions, comparisons, storage, the planner, MVCC, or a public API, or if it adds more than about 300 lines that are not tests (`git diff --stat origin/main...`).

For all other changes (an error message, a narrow fix inside one function, a doc), do steps 0, 1, 2 and 5.

Steps 3 and 4 run once, on the final diff. Fix what they find in one batch; do not repeat them after each fix.

## 0. Set up SQLite and PostgreSQL to compare with

```bash
scripts/oracles.sh            # sqlite3 at our version, PostgreSQL in Docker
scripts/oracles.sh --sources  # also clone both sources into ~/.cache/turso-oracles
```

Run the `export` lines it prints: `PATH`, so `scripts/diff.sh` uses our sqlite3 version, and `PG_DSN` for PostgreSQL. PostgreSQL is needed only for changes to the PostgreSQL frontend (`postgres/`); without Docker, ask the user for a server of the version in `postgres/version` and set `PG_DSN` to it. `scripts/oracles.sh --stop` removes the container.

## 1. Before the fix

- Reproduce the bug on `main` (for a `main` build next to your branch: `git worktree add ../turso-main origin/main`):
  - SQLite frontend: `scripts/diff.sh "SQL"` runs the SQL in `sqlite3` and `tursodb` and prints PASS, or FAIL with a diff.
  - PostgreSQL frontend: `psql "$PG_DSN" -c "SQL"` and `cargo run -q -p tursopg -- :memory: "SQL"`.
- Do not answer from what you remember SQLite or PostgreSQL does. Run it. Different wording of an error is not a bug. A different result is, and so is a different error class: on the PostgreSQL frontend the SQLSTATE is part of the result, and an error where SQLite or PostgreSQL succeeds, or the reverse, is a wrong result.
- Read their source for the behaviour (`--sources` above; SQLite: `src/`, PostgreSQL: `src/backend/`). Find the function that does this. Put your fix at the matching place in Turso, in the same layer, not wherever your example happens to pass through, and not a different way that gives the same output on your example.
- Where SQLite and PostgreSQL have no equivalent (sync, MVCC, CDC, encryption, the CLI), the reference is what the feature promises: the remote database after a pull, the same rows as without MVCC, `main`'s output. Test sequences of operations (a change, then a rename, a drop, a second pull), not single statements.
- If neither SQLite, PostgreSQL nor current Turso behaviour decides the answer, stop and ask the user. With no user to ask, leave it unfixed and say so in the PR. Do not decide it inside a fix.

## 2. The fix and its test

- Fix the bug for every input, not only the reported query. Find every input that breaks the same rule: other statement types (INSERT, UPDATE, DELETE, UPSERT), other query shapes (joins, subqueries, ORDER BY, GROUP BY, indexes, views, triggers), and the other callers of each function you change (`rg` for them).
- Fix the other defects in the same feature (the same function, cast or statement) in the same PR if the risk is low.
- If one rule is checked in many places, move it to the one place that should decide it. A check that every new caller must remember will be missed.
- If your change adds data or a function that must stay in step with another (a list kept beside the struct it describes, a `_mut` twin of a reader, a match that repeats another's arms), merge them so one source answers. Two copies that must agree will drift.
- Before you write a helper, type or constant, search for code that already does the job, by what it does, not only by name. Reuse it or extend it. Delete the code you replace.
- Keep the scope to the bug. PostgreSQL-only behaviour goes in `postgres/`, not in `core`. A fix for a rare case must not change code that every query runs through.
- If a value is already wrong when it reaches your code, fix the code that produces it, not the code that reads it.
- Weaken no existing check: no changed expected output, removed or relaxed assertion, ignored test, allowed lint, swallowed error, or error the simulator or fuzzer now accepts. If an old check is wrong, prove it with SQLite or PostgreSQL output, or a seed, and show that in the PR.
- Write the test with the fix. Then prove that it fails without the fix: revert the non-test change, run the test, see it fail, restore the change, see it pass. Then break each part of the fix on its own (revert one hunk, or skip one call site) and run the tests. For each part, some test must fail. If no test notices a part, add a test for it or delete the part. Use `.sqltest` files where you can (the `testing` skill says how to run them). A test that passes on `main` does not test this change.
- The test uses the reported input, or a smaller input that still fails the same way.

## 3. Risk check (full pass only)

For each area, ask if the change can reach it. If yes, run the check.

| Area | Check |
|------|-------|
| On-disk format | Does a database file change shape? Open a file made by a `main` build with your build, and the other way. The results match and `PRAGMA integrity_check` passes. |
| Durability, crash recovery | Does the change touch WAL, checkpoint, fsync or the pager? Run the simulator (the `debugging` skill) with several seeds. |
| Concurrency, MVCC, locks | Run the concurrent tests for the code (the `mvcc` and `transaction-correctness` skills). Try two connections that interleave. |
| Public API, bindings | Did a signature, an error or a default change? Search the bindings for each caller. |
| SQLite or PostgreSQL compatibility | Compare with them on the reported input and on its neighbours: NULL, empty, boundary values, signs, affinity, very large values. |
| Speed of common queries | Does the change add work to code every query runs? Benchmark before and after. |

## 4. Try to break it, in a subagent (full pass only)

You wrote the change, so your context is full of reasons it is right, and you will not see where it is wrong. Spawn a fresh subagent, or start a new session. Do not fork this conversation: a fork has your reasoning. Give it only:

- the diff (`git diff origin/main...HEAD`) and the PR description
- a worktree with your branch, and one with `origin/main`
- the commands from step 0 and 1, with your `PATH` and `PG_DSN` values

Do not give it your notes, your plan or your explanation. Give it this brief:

> Find inputs on which the branch gives a wrong result. Do not review style or run the test suite. Say what must be true for the change to be right. List the other ways to reach the same code. Run each one on the branch, on `origin/main`, and on SQLite or PostgreSQL; do not answer from memory. Shrink each failure to the smallest input, then say which kinds of input fail. Stop any run that takes longer than 60 seconds. Report a failure only with the commands you ran and their output.

Check each failure it reports by running its commands yourself. A failure that `main` also has, in the same feature or in code you changed, belongs in this PR. A failure far from the change goes in a new issue that names its cause, not only its symptom.

## 5. Before you push

- Read `git diff --stat`. Delete what the fix does not need.
- Ask of the diff:
  - What one fact makes this change right? Show it with a command or cite `file:line`.
  - Does any comment say what the code does, instead of why? Delete it or fix it.
  - Does a skip, a catch-all or a default value hide a case the change knows is wrong?
- Run what CI runs for the crates you changed, with the same flags (`.github/workflows/`): `cargo fmt`, clippy, and the tests near your change. Once, on the final tree (the fail-without-the-fix run in step 2 is separate).
- Re-read the PR description against the final diff. Delete every claim about a part the diff no longer has.
- In the PR description:
  - state what you ran and what it printed. Do not call the change correct, safe or complete; the reviewer judges that from the evidence.
  - the test's failure on `main` (the command and the failing line of output);
  - on a full pass, which risk areas from step 3 you checked, and which you could not and why;
  - the weakest part of the change in one sentence: the input you are least sure of, the risk you could not check, or the choice a reviewer might dispute.
