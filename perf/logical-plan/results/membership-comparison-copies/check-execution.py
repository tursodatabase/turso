import collections
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time

root = Path('/workspace')
scratch = root / 'target/logical-plan-commits/membership-comparison-copies'
saved = root / 'target/logical-plan-membership-comparison-copies'
results = root / 'perf/logical-plan/results'
source = json.loads((scratch / 'source.json').read_text())
execution = scratch / 'execution'
execution.mkdir(exist_ok=True)
command = [str(saved / 'unnesting_execution'), '--color', 'never', '--test']
env = dict(os.environ, TURSO_BENCH_PLAN_DIR=str(execution / 'plans'))
started = time.monotonic()
with (execution / 'run.txt').open('w') as stream:
    result = subprocess.run(command, cwd=root, env=env, stdout=stream, stderr=subprocess.STDOUT)
record = {'command': command, 'env': {'TURSO_BENCH_PLAN_DIR': env['TURSO_BENCH_PLAN_DIR']}, 'exit_code': result.returncode, 'elapsed_seconds': time.monotonic() - started, 'binary': source['binaries']['unnesting_execution']}
(execution / 'run.json').write_text(json.dumps(record, indent=2) + '\n')
result.check_returncode()
files = sorted((execution / 'plans').glob('*.json'))
assert len(files) == 114, len(files)
comparison = {'cases': len(files), 'changed_plans': [], 'plan_sha256': {}, 'references': {}}
for path in files:
    previous = results / ('membership-joined-projections/execution/after/plans' if 'joined_projection' in path.name else 'subquery-cache-copies/execution/after/plans') / path.name
    comparison['plan_sha256'][path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
    comparison['references'][path.name] = str(previous.relative_to(root))
    if path.read_bytes() != previous.read_bytes():
        comparison['changed_plans'].append(path.name)
(execution / 'comparison.json').write_text(json.dumps(comparison, indent=2) + '\n')
print(json.dumps({'cases': len(files), 'changed_plans': comparison['changed_plans']}), flush=True)
fuzz = scratch / 'seed-57291020'
fuzz.mkdir(exist_ok=True)
command = [str(saved / 'differential_fuzzer'), '--seed', '57291020', '--profile', 'correlated-selects', '--max-subquery-depth', '1', '-n', '1000', '--coverage', '--keep-files']
env = dict(os.environ, RUST_LOG='info,logical_optimizer=trace')
started = time.monotonic()
with (fuzz / 'run.txt').open('w') as stream:
    result = subprocess.run(command, cwd=fuzz, env=env, stdout=stream, stderr=subprocess.STDOUT)
record = {'command': command, 'cwd': str(fuzz), 'env': {'RUST_LOG': env['RUST_LOG']}, 'exit_code': result.returncode, 'elapsed_seconds': time.monotonic() - started, 'binary': source['binaries']['differential_fuzzer']}
(fuzz / 'run.json').write_text(json.dumps(record, indent=2) + '\n')
result.check_returncode()
text = re.sub(r'\x1b\[[0-9;]*m', '', (fuzz / 'run.txt').read_text())
counts = {match[1].strip(): int(match[2]) for match in re.finditer(r'^\| ([^|]+)\| (\d+)\s*\|$', text, re.M)}
assert counts['Statements Executed'] == 1000
assert all(counts[name] == 0 for name in ['Statements Skipped', 'Warnings', 'Oracle Failures', 'Errors'])
history = results / 'membership-projection-generator/seed-57291020-final/statements.sql'
assert history.read_bytes() == (fuzz / 'simulator-output/test.sql').read_bytes()
record = {'counts': counts, 'rule_trace_events': dict(sorted(collections.Counter(re.findall(r'applied logical rule rule="([^"]+)"', text)).items())), 'trace_scope': 'Events include inspection and repeated preparation; they are not unique query counts.', 'statement_history': str(history.relative_to(root)), 'statement_history_sha256': hashlib.sha256(history.read_bytes()).hexdigest(), 'history_matches_preceding_run': True}
(fuzz / 'outcome.json').write_text(json.dumps(record, indent=2) + '\n')
print(json.dumps(record), flush=True)
