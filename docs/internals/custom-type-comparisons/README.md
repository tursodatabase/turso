# Custom type comparisons

Custom types (`CREATE TYPE ... ENCODE ... DECODE ...`, and the PostgreSQL
types tursopg builds on them) compare inconsistently today: the same query
can return different rows on different plans, and `ORDER BY`, `<` and
indexes can disagree on order. This folder holds the plan to fix that.

- `requirements.md`: what the result must satisfy, checked against
  PostgreSQL 17.
- `design.md`: part A, the design being built. Section 8 lists the steps.
  Part B (Rust-defined PostgreSQL types, PostgreSQL typing rules) is
  listed in section 9 and not designed yet.
- `code-map.md`: where custom-type values were read, written and
  compared before the redesign.

## Rules for working on it

> # ⚠️ FAIL CLOSED ⚠️
>
> **If the rules don't say exactly what something means, it is an
> error, never a default or a guess.** See the top of `design.md`.
> Reviewers list every fallback or implicit behavior they find.

- Correctness is the only criterion. Custom types are experimental: no
  backward compatibility with databases made by earlier versions, and
  "main does X" is not a reason for anything. Where part A can't give
  PostgreSQL's answer it gives an error, never a different answer.
- Each step merges on its own with `cargo test` and both conformance
  suites green, and with tests that fail before the step and pass after.
- Each step gets its detailed design written when it starts, and a
  hostile review of that design and of the code (fresh reviewers, told to
  assume it is bad, given only the requirements, the design and the
  code).

## Progress

Branch: `custom-types-compare-redesign`.

| Step | What | Status |
|---|---|---|
| 1 | NULL checks and determinism | done (6566745d55) |
| 2 | `comparison_for` and friends | designed and reviewed (`step2-comparison-for.md`) |
| 3 | Type traits and capability errors | not started |
| 4 | Values in sorters, hash payloads, CHECK, trigger and FK copies | not started |
| 5 | One funnel for index writes, probes and reads | not started |
| 6 | VACUUM copies records byte for byte | not started |
| 7 | Index entries become values | not started |
| 8 | Pair rules, result types, persisted expressions, schema changes | not started |

Separate bugs found during review (not part of this design): design.md
section 10.
