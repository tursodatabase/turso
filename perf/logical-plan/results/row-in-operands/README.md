# Evaluate all row-IN operands before NULL handling

The row-IN emitter jumped to its NULL comparison scan immediately after reading
a NULL left operand. Later left operands had not been evaluated. For
`(NULL,400) IN (SELECT 50,500)`, it returned NULL instead of false. A later
`abs(-9223372036854775808)` operand could also skip its integer-overflow error.

The emitter now evaluates every left operand before any NULL branch. SQLite
3.50.4 EXPLAIN reads both columns before its first IsNull instruction. The two
regressions fail before this change and pass after it. The full row-value-IN
file and seven draft correlated membership cases pass (39 cases total).
Formatting and the subsequent 42 relational tests and forced/disabled corpus
also pass, with other workspace drafts present. The latter checks are recorded
with the following membership change; they are not isolated commit validation.

The deferred UPDATE FROM, empty automatic-index and nested EXISTS work is
unchanged and excluded from this commit. No targeted deferred reproduction ran.
