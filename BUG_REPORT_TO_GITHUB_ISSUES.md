# Prompt: Transform Bug Reports to GitHub Issues Script

Use this prompt when you have bug report markdown files and need to create a script that files GitHub issues.

---

## Task

Given bug report files (e.g., `CORRUPTION_BUGS_ROUNDX.md`), create a bash script that files verified, well-documented GitHub issues.

## Input Files

The bug report files contain:
- Bug descriptions with reproducers
- Root cause analysis
- File paths and line numbers
- Campaign information (ignore this)

## Output

A bash script named `create_bug_issues.sh` that creates GitHub issues with `gh issue create`.

## Process Steps

### 1. Verify Bug Reproducibility

**First, spawn a sub-agent to verify each bug:**
- Test all reproducers on current codebase
- Document exact observed behavior (not "crashes" - get actual error messages)
- Identify which bugs are already fixed
- Get exact panic messages for all crashes
- Use `/tmp/claude/` for test databases

**Exclude bugs that are already fixed from the script.**

### 2. Verify GitHub Labels

**Spawn a sub-agent to:**
- Fetch all available labels from `https://github.com/tursodatabase/turso/labels`
- Return complete list of valid label names
- Only use labels that actually exist in the repo

### 3. Find SQLite Documentation

**Spawn a sub-agent to:**
- For any claim about SQLite behavior, find official documentation
- Search https://sqlite.org for relevant pages
- Extract direct quotes from documentation
- Return URL + quote for each behavioral claim

Common documentation pages:
- https://sqlite.org/lang_aggfunc.html (aggregate functions)
- https://sqlite.org/foreignkeys.html (foreign keys)
- https://sqlite.org/limits.html (limits like MAX_TRIGGER_DEPTH)
- https://sqlite.org/stricttables.html (STRICT tables)
- https://sqlite.org/expridx.html (expression indexes)
- https://sqlite.org/deterministic.html (deterministic functions)
- https://sqlite.org/printf.html (printf function)

### 4. Write the Script

Create `create_bug_issues.sh` with these requirements:

#### Script Structure
```bash
#!/bin/bash
set -e

REPO="tursodatabase/turso"

create_issue() {
    local title="$1"
    local body="$2"
    local labels="$3"

    issue_url=$(gh issue create --repo "$REPO" --title "$title" --body "$body" --label "$labels")
    issue_number=$(echo "$issue_url" | grep -oE '[0-9]+$')
    echo "Created issue #$issue_number: $issue_url"
    sleep 2  # Rate limiting
}
```

#### Issue Format

Each issue should follow this structure:

```markdown
## Description
[One sentence describing what happens]

## Reproducer
```sql
[Minimal SQL that demonstrates the bug]
-- Turso: [actual behavior]
-- SQLite: [expected behavior]
```

[If there's a panic, include "## Panic Message" or "## Crash Message" section with exact output]

[If claiming SQLite behavior, add:]
Per [SQLite documentation](URL): "Direct quote from docs"

[File path inline with description:]
`path/to/file.rs:line-numbers` - Brief explanation of root cause.

---
*This issue brought to you by Mikaël and Claude Code.*
```

#### Content Guidelines

**DO:**
- Be concise and factual
- Use "Turso" not "Limbo"
- Include exact panic/error messages in code blocks
- Link to SQLite documentation with direct quotes
- Put file paths inline: `` `core/file.rs:123` - Description ``
- Use valid labels only (verified to exist)
- Show comparison: `-- Turso: X` vs `-- SQLite: Y`

**DON'T:**
- Use "bug numbers" in titles (no "Bug #27")
- Mention "campaigns" (users don't care how you found it)
- Use judgmental words: "critical", "corruption", "impact", "severity"
- Say "this may be fixed" - verify first and exclude if fixed
- Say "panics or crashes" - get exact error message
- Add "Verification Status" sections
- Include separate "File" sections
- Have rust code blocks with only comments
- Use redundant "Expected/Actual/Observed" when obvious
- Use the "bug" label (it's not in the label list)

#### Label Selection

Choose from verified labels like:
- `affinity`, `atomicity`, `correctness`
- `foreign keys`, `indexes`, `query planner`
- `panic`, `regression`, `performance`
- `schema changes`, `schema management`
- `vdbe`, `cli`, `compatibility`
- `strict tables`, `translation/planning`

Use empty string `""` if no appropriate label exists.

#### Quoting

Use heredoc for issue bodies:
```bash
create_issue \
"Issue Title" \
'## Description
...SQL with '\''escaped single quotes'\''...
---
*This issue brought to you by Mikaël and Claude Code.*' \
"label1,label2"
```

### 5. Script Summary

End script with:
```bash
echo "=== All issues created successfully ==="
echo "Summary: N verified issues filed"
echo "Note: X bugs were found to be already fixed and excluded"
```

## Example Issue Transformation

**Input (from bug report):**
```
## Bug #27: Foreign Key Transaction Partial Commit

**Severity**: CRITICAL — Violates ACID atomicity
**Campaign**: 82 (Foreign Key Deferred Checking)
**File**: core/vdbe/mod.rs:1522-1526

[reproducer SQL...]
```

**Output (in script):**
```bash
create_issue \
"Foreign Key Constraint Violation Does Not Roll Back Transaction" \
'## Description
Foreign key constraint violations inside transactions do not roll back the transaction. Error is reported but execution continues.

## Reproducer
```sql
[reproducer...]
-- Turso: 2, 3 (partial commit)
-- SQLite: (empty - full rollback)
```

Per [SQLite documentation](https://sqlite.org/foreignkeys.html): "If a statement modifies the contents of the database so that an immediate foreign key constraint is in violation at the conclusion the statement, an exception is thrown and the effects of the statement are reverted."

`core/vdbe/mod.rs:1522-1526` - Rollback only happens in autocommit mode.

---
*This issue brought to you by Mikaël and Claude Code.*' \
"foreign keys,atomicity,correctness"
```

## Quality Checklist

Before finalizing the script, verify:

- [ ] All bugs tested and reproducible on current codebase
- [ ] Fixed bugs excluded from script
- [ ] All panic messages are exact (not "crashes")
- [ ] All SQLite behavioral claims have documentation links
- [ ] All labels verified to exist in repository
- [ ] No "bug numbers" in titles or bodies
- [ ] No "campaign" mentions
- [ ] All references say "Turso" not "Limbo"
- [ ] File paths integrated inline, not separate sections
- [ ] Concise descriptions without redundancy
- [ ] Script is executable: `chmod +x create_bug_issues.sh`

## Running the Script

```bash
./create_bug_issues.sh
```

The script will:
1. Create each issue via `gh issue create`
2. Apply appropriate labels
3. Print issue URLs as created
4. Rate limit with 2-second delays
5. Report summary at end
