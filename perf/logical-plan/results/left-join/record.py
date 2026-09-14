import collections
import hashlib
import json
from pathlib import Path
import re
import shutil


root = Path('/workspace')
scratch = root / 'target/logical-plan-commits/left-join'
output = root / 'perf/logical-plan/results/left-join'
output.mkdir(exist_ok=True)
source = json.loads((scratch / 'source.json').read_text())
for name, digest in source['source_sha256'].items():
    assert hashlib.sha256((root / name).read_bytes()).hexdigest() == digest, name
for name in ['before.json', 'source.json', 'test-before.json', 'test-after.json', 'focused-first.json', 'focused-second.json', 'focused.json', 'validation.json', 'validation-commands.json', 'builds.json', 'deferred-preservation.json', 'sql-cases.json', 'original-build-pause.json', 'execution-check-pause.json', 'test-before.txt', 'test-after.txt', 'focused-first.txt', 'focused.txt', 'declines-first.txt', 'declines.txt', 'relational.txt', 'optimizer.txt', 'forced.txt', 'integration.txt', 'sql.txt', 'clippy.txt', 'fmt.txt', 'capture.py', 'build.py', 'build-original.py', 'pause-benchmark.py', 'check-execution.py', 'validate.py', 'measure.py', 'compare.py', 'sql-cases.py', 'record.py']:
    shutil.copy2(scratch / name, output / name)
execution = json.loads((scratch / 'execution/comparison.json').read_text())
assert execution['cases'] == 129
assert not execution['existing_changed_plans']
(output / 'execution').mkdir(exist_ok=True)
(output / 'execution/comparison.json').write_text(json.dumps(execution, indent=2) + '\n')
for kind, original in [('candidate', scratch / 'execution'), ('original', scratch / 'execution/original')]:
    destination = output / 'execution' / kind
    (destination / 'plans').mkdir(parents=True, exist_ok=True)
    for name in ['run.json', 'run.txt']:
        shutil.copy2(original / name, destination / name)
    for path in (original / 'plans').glob('left_join_*.json'):
        shutil.copy2(path, destination / 'plans' / path.name)
fuzz = scratch / 'seed-57291020'
text = re.sub(r'\x1b\[[0-9;]*m', '', (fuzz / 'run.txt').read_text())
counts = {match[1].strip(): int(match[2]) for match in re.finditer(r'^\| ([^|]+)\| (\d+)\s*\|$', text, re.M)}
assert counts['Statements Executed'] == 1000
assert all(counts[name] == 0 for name in ['Statements Skipped', 'Warnings', 'Oracle Failures', 'Errors'])
rules = collections.Counter(re.findall(r'applied logical rule rule="([^"]+)"', text))
history = root / 'perf/logical-plan/results/membership-projection-generator/seed-57291020-final/statements.sql'
assert history.read_bytes() == (fuzz / 'simulator-output/test.sql').read_bytes()
destination = output / 'seed-57291020'
destination.mkdir(exist_ok=True)
for name, target in [('run.json', 'run.json'), ('run.txt', 'run.txt'), ('simulator-output/schema.json', 'schema.json'), ('simulator-output/coverage.txt', 'coverage.txt')]:
    shutil.copy2(fuzz / name, destination / target)
outcome = {
    'implementation': 'LEFT JOIN keeps both input column sets, attaches ON predicates to the join, preserves nullable result metadata, and lowers the right side as one table or FROM subquery. Existing rewrites can transform its inputs without crossing the join.',
    'failing_before_test': 'test-before.json',
    'optimizer_and_relational_tests': 67,
    'query_processing_integration_tests': 493,
    'sql_cases': 1729,
    'new_sql_cases': 10,
    'new_forced_disabled_sqlite_cases': 10,
    'forced_disabled_comparisons_with_distinct_plans': 'at least 2, asserted by the oracle test',
    'candidate_execution_fixture_modes': 129,
    'original_new_execution_fixture_modes': 6,
    'previous_physical_plan_changes': [],
    'fuzz': counts,
    'rule_trace_events': dict(sorted(rules.items())),
    'trace_scope': 'Events include inspection and repeated preparation; they are not unique query counts.',
    'statement_history': str(history.relative_to(root)),
    'statement_history_sha256': hashlib.sha256(history.read_bytes()).hexdigest(),
    'statement_history_matches_previous_run': True,
    'performance': 'pending: saved original and candidate binaries, two new prepare and two new execution fixtures; all fixed acceptance limits remain unchanged',
    'deferred_changes_preserved': True,
    'known_unresolved_validation': ['host denies io_uring setup', 'passive-MVCC transfer total mismatch from the earlier full-core run remains unresolved'],
    'remaining': ['full outer join adapter', 'subqueries within ON', 'outer-join domain propagation', 'general unnesting and full integration', 'preparation and execution performance failures'],
}
(output / 'outcome.json').write_text(json.dumps(outcome, indent=2) + '\n')
for name in ['original-build-pause.json', 'execution-check-pause.json']:
    pause = json.loads((scratch / name).read_text())
    assert not pause['active'] and pause['exit_code'] == 0
    shutil.copy2(scratch / name, root / 'perf/logical-plan/results/mark-projections' / ('left-join-' + name))
print(json.dumps(outcome, indent=2))
