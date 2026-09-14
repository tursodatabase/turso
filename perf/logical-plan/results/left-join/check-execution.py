import hashlib
import json
import os
from pathlib import Path
import subprocess
import time


root = Path('/workspace')
scratch = root / 'target/logical-plan-commits/left-join'
source = json.loads((scratch / 'source.json').read_text())
binary = source['binaries']['unnesting_execution']
assert hashlib.sha256(Path(binary['path']).read_bytes()).hexdigest() == binary['sha256']
output = scratch / 'execution'
output.mkdir(exist_ok=False)
command = [binary['path'], '--color', 'never', '--test']
env = dict(os.environ, TURSO_BENCH_PLAN_DIR=str(output / 'plans'))
started = time.monotonic()
with (output / 'run.txt').open('w') as log:
    result = subprocess.run(command, cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT)
(output / 'run.json').write_text(json.dumps({'command': command, 'binary': binary, 'exit_code': result.returncode, 'elapsed_seconds': time.monotonic() - started}, indent=2) + '\n')
result.check_returncode()
plans = sorted((output / 'plans').glob('*.json'))
assert len(plans) == 129, len(plans)
comparison = {'cases': len(plans), 'existing_changed_plans': [], 'new_plans': [], 'references': {}}
for path in plans:
    if path.name.startswith('left_join_'):
        comparison['new_plans'].append(path.name)
        continue
    if 'result_with_exists_filter' in path.name:
        previous = root / 'perf/logical-plan/results/mark-projections/execution/candidate/plans' / path.name
    else:
        parent = 'membership-joined-projections' if 'joined_projection' in path.name else 'subquery-cache-copies'
        previous = root / 'perf/logical-plan/results' / parent / 'execution/after/plans' / path.name
    comparison['references'][path.name] = str(previous.relative_to(root))
    if previous.read_bytes() != path.read_bytes():
        comparison['existing_changed_plans'].append(path.name)
assert len(comparison['new_plans']) == 6
original_binary = root / 'target/logical-plan-original-left-join-fixtures/unnesting_execution'
original = output / 'original'
original.mkdir()
command = [str(original_binary), '--color', 'never', '--test', 'left_join_']
env['TURSO_BENCH_PLAN_DIR'] = str(original / 'plans')
started = time.monotonic()
with (original / 'run.txt').open('w') as log:
    result = subprocess.run(command, cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT)
(original / 'run.json').write_text(json.dumps({'command': command, 'binary_sha256': hashlib.sha256(original_binary.read_bytes()).hexdigest(), 'exit_code': result.returncode, 'elapsed_seconds': time.monotonic() - started}, indent=2) + '\n')
result.check_returncode()
assert len(list((original / 'plans').glob('*.json'))) == 6
(output / 'comparison.json').write_text(json.dumps(comparison, indent=2) + '\n')
print(json.dumps({key: value for key, value in comparison.items() if key != 'references'}), flush=True)
assert not comparison['existing_changed_plans'], comparison['existing_changed_plans']
