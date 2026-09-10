---
name: update-changelog
description: Update CHANGELOG.md with merged PRs since the last changelog update, categorized by type
---

# Update Changelog

This skill updates CHANGELOG.md with merged PRs that aren't already listed.

## Step 1: Determine the changelog scope

Read `CHANGELOG.md` to identify the current unreleased version section at the top (e.g., `Tantivy 0.26 (Unreleased)`).

Collect all PR numbers already mentioned in the unreleased section by extracting `#NNNN` references.

## Step 2: Find merged PRs not yet in the changelog

Use `gh` to list recently merged PRs from the upstream repo:

```bash
gh pr list --repo quickwit-oss/tantivy --state merged --limit 100 --json number,title,author,labels,mergedAt
```

Filter out any PRs whose number already appears in the unreleased section of the changelog.

## Step 3: Consolidate related PRs

Before categorizing, group PRs that belong to the same logical change. This is critical for producing a clean changelog. Use PR descriptions, titles, cross-references, and the files touched to identify relationships.

**Merge follow-up PRs into the original:**
- If a PR is a bugfix, refinement, or follow-up to another PR in the same unreleased cycle, combine them into a single changelog entry with multiple `[#N](url)` links.
- Also consolidate PRs that touch the same feature area even if not explicitly linked — e.g., a PR fixing an edge case in a new API should be folded into the entry for the PR that introduced that API.

**Filter out bugfixes on unreleased features:**
- If a bugfix PR fixes something introduced by another PR in the **same unreleased version**, it must NOT appear as a separate Bugfixes entry. Instead, silently fold it into the original feature/improvement entry. The changelog should describe the final shipped state, not the development history.
- To detect this: check if the bugfix PR references or reverts changes from another PR in the same release cycle, or if it touches code that was newly added (not present in the previous release).

## Step 4: Review the actual code diff

**Do not rely on PR titles or descriptions alone.** For every candidate PR, run `gh pr diff <number> --repo quickwit-oss/tantivy` and read the actual changes. PR titles are often misleading — the diff is the source of truth.

**What to look for in the diff:**
- Does it change observable behavior, public API surface, or performance characteristics?
- Is the change something a user of the library would notice or need to know about?
- Could the change break existing code (API changes, removed features)?

**Skip PRs where the diff reveals the change is not meaningful enough for the changelog** — e.g., cosmetic renames, trivial visibility tweaks, test-only changes, etc.

## Step 5: Categorize each PR group

For each PR (or consolidated group) that survived the diff review, determine its category:

- **Bugfixes** — fixes to behavior that existed in the **previous release**. NOT fixes to features introduced in this release cycle.
- **Features/Improvements** — new features, API additions, new options, improvements that change user-facing behavior or add new capabilities.
- **Performance** — optimizations, speed improvements, memory reductions. **If a PR adds new API whose primary purpose is enabling a performance optimization, categorize it as Performance, not Features.** The deciding question is: does a user benefit from this because of new functionality, or because things got faster/leaner? For example, a new trait method that exists solely to enable cheaper intersection ordering is Performance, not a Feature.

If a PR doesn't clearly fit any category (e.g., CI-only changes, internal refactors with no user-facing impact, dependency bumps with no behavior change), skip it — not everything belongs in the changelog.

When unclear, use your best judgment or ask the user.

## Step 6: Format entries

Each entry must follow this exact format:

```
- Description [#NUMBER](https://github.com/quickwit-oss/tantivy/pull/NUMBER)(@author)
```

Rules:
- The description should be concise and describe the user-facing change (not the implementation). Describe the final shipped state, not the incremental development steps.
- Use sub-categories with bold headers when multiple entries relate to the same area (e.g., `- **Aggregation**` with indented entries beneath). Follow the existing grouping style in the changelog.
- Author is the GitHub username from the PR, prefixed with `@`. For consolidated entries, include all contributing authors.
- For consolidated PRs, list all PR links in a single entry: `[#100](url) [#110](url)` (see existing entries for examples).

## Step 7: Present changes to the user

Show the user the proposed changelog entries grouped by category **before** editing the file. Ask for confirmation or adjustments.

## Step 8: Update CHANGELOG.md

Insert the new entries into the appropriate sections of the unreleased version block. If a section doesn't exist yet, create it following the order: Bugfixes, Features/Improvements, Performance.

Append new entries at the end of each section (before the next section header or version header).

## Step 9: Verify

Read back the updated unreleased section and display it to the user for final review.
