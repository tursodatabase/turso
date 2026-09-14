import collections
import hashlib
import json
from pathlib import Path
import re
import shutil

root = Path('/workspace')
scratch = root / 'target/logical-plan-commits/lazy-legacy-copy'
results = root / 'perf/logical-plan/results'
out = results / 'lazy-legacy-copy'
out.mkdir(exist_ok=True)
for name in ['before.json', 'source.json', 'validation.json', 'regression.json', 'eager-interface.json', 'measurements.json', 'isolation.json', 'first-round-self-costs.json', 'validate.py', 'measure.py', 'test-eager.txt', 'test-lazy.txt', 'optimizer.txt', 'relational.txt', 'forced.txt', 'integration.txt', 'sql.txt', 'clippy.txt', 'fmt.txt']:
    shutil.copy2(scratch / name, out / name)
execution = json.loads((scratch / 'execution/comparison.json').read_text())
assert execution['cases'] == 102
assert execution['changed_plans'] == []
execution['unchanged_plan_reference'] = 'perf/logical-plan/results/subquery-cache-copies/execution/after/plans'
execution['plan_sha256'] = {}
for path in sorted((scratch / 'execution/after/plans').glob('*.json')):
    before = scratch / 'execution/before/plans' / path.name
    reference = results / 'subquery-cache-copies/execution/after/plans' / path.name
    assert path.read_bytes() == before.read_bytes() == reference.read_bytes(), path.name
    execution['plan_sha256'][path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
(out / 'execution').mkdir(exist_ok=True)
(out / 'execution/comparison.json').write_text(json.dumps(execution, indent=2) + '\n')
for phase in ['before', 'after']:
    (out / 'execution' / phase).mkdir(exist_ok=True)
    for name in ['run.json', 'run.txt']:
        shutil.copy2(scratch / 'execution' / phase / name, out / 'execution' / phase / name)
fuzz = scratch / 'seed-57291020'
text = re.sub(r'\x1b\[[0-9;]*m', '', (fuzz / 'run.txt').read_text())
counts = {match[1].strip(): int(match[2]) for match in re.finditer(r'^\| ([^|]+)\| (\d+)\s*\|$', text, re.M)}
assert counts['Statements Executed'] == 1000
assert all(counts[name] == 0 for name in ['Statements Skipped', 'Warnings', 'Oracle Failures', 'Errors'])
traces = collections.Counter(re.findall(r'applied logical rule rule="([^"]+)"', text))
history = root / 'perf/logical-plan/results/membership-projection-generator/seed-57291020-final/statements.sql'
assert history.read_bytes() == (fuzz / 'simulator-output/test.sql').read_bytes()
(out / 'seed-57291020').mkdir(exist_ok=True)
for source, target in [('run.json', 'run.json'), ('run.txt', 'run.txt'), ('simulator-output/schema.json', 'schema.json'), ('simulator-output/coverage.txt', 'coverage.txt')]:
    shutil.copy2(fuzz / source, out / 'seed-57291020' / target)
outcome = {
    'statements': counts,
    'rule_trace_events': dict(sorted(traces.items())),
    'trace_scope': 'Events include inspection and repeated preparation; they are not unique query counts.',
    'statement_history': str(history.relative_to(root)),
    'statement_history_sha256': hashlib.sha256(history.read_bytes()).hexdigest(),
    'history_matches_preceding_run': True,
    'execution_fixture_cases': execution['cases'],
    'changed_execution_plans': execution['changed_plans'],
    'prepare_comparison': '../prepare-lazy-legacy-copy/outcome.json',
    'remaining_work': 'Sixteen of twenty-nine focused prepares still exceed original instruction limits, and seven exceed original native uncertainty. General unnesting, broader integration and the full final corpus comparison remain unfinished.',
}
(out / 'outcome.json').write_text(json.dumps(outcome, indent=2) + '\n')
print(json.dumps(outcome, indent=2))
