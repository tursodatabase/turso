import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

root = Path('/workspace')
out = root / 'target/logical-plan-commits/lazy-legacy-copy'
saved = root / 'target/logical-plan-lazy-legacy-copy'
results = root / 'perf/logical-plan/results'
prepare = results / 'prepare-lazy-legacy-copy'
previous = json.loads((results / 'prepare-subquery-cache-copies/native-environment.json').read_text())
validation = json.loads((out / 'validation.json').read_text())
assert validation[-1]['name'] == 'execution-build'
assert all(step['exit_code'] == 0 for step in validation)
steps = []
for phase in ['native', 'callgrind']:
    command = ['python3', str(root / 'perf/logical-plan/measure.py'), str(saved / 'prepare_benchmark'), str(prepare), '--phase', phase, '--cpu', str(previous['cpu']), '--filter', previous['filter'], '--source-revision', '4266a6367 plus delayed legacy-plan copying; source.json records exact source']
    started = time.monotonic()
    result = subprocess.run(command, cwd=root)
    steps.append({'phase': phase, 'command': command, 'exit_code': result.returncode, 'elapsed_seconds': time.monotonic() - started})
    (out / 'measurements.json').write_text(json.dumps(steps, indent=2) + '\n')
    if result.returncode:
        raise SystemExit(result.returncode)
subprocess.run(['python3', str(prepare / 'comparison.py')], cwd=root, check=True)
execution = out / 'execution'
execution.mkdir()
for name, binary in [
    ('before', root / 'target/logical-plan-subquery-cache-copies/unnesting_execution'),
    ('after', saved / 'unnesting_execution'),
]:
    destination = execution / name
    destination.mkdir()
    command = [str(binary), '--color', 'never', '--test']
    env = dict(os.environ, TURSO_BENCH_PLAN_DIR=str(destination / 'plans'))
    started = time.monotonic()
    with (destination / 'run.txt').open('w') as stream:
        result = subprocess.run(command, cwd=root, env=env, stdout=stream, stderr=subprocess.STDOUT)
    record = {'command': command, 'exit_code': result.returncode, 'elapsed_seconds': time.monotonic() - started, 'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(), 'env': {'TURSO_BENCH_PLAN_DIR': env['TURSO_BENCH_PLAN_DIR']}}
    (destination / 'run.json').write_text(json.dumps(record, indent=2) + '\n')
    print(json.dumps(record), flush=True)
    if result.returncode:
        raise SystemExit(result.returncode)
plans = [{path.name: json.loads(path.read_text()) for path in (execution / phase / 'plans').glob('*.json')} for phase in ['before', 'after']]
assert plans[0].keys() == plans[1].keys()
assert len(plans[0]) == 102, len(plans[0])
changed = [name for name in plans[0] if plans[0][name] != plans[1][name]]
(execution / 'comparison.json').write_text(json.dumps({'cases': len(plans[0]), 'changed_plans': changed}, indent=2) + '\n')
print(json.dumps({'execution_cases': len(plans[0]), 'changed_plans': changed}), flush=True)
fuzz = out / 'seed-57291020'
fuzz.mkdir()
command = [str(saved / 'differential_fuzzer'), '--seed', '57291020', '--profile', 'correlated-selects', '--max-subquery-depth', '1', '-n', '1000', '--coverage', '--keep-files']
env = dict(os.environ, RUST_LOG='info,logical_optimizer=trace')
started = time.monotonic()
with (fuzz / 'run.txt').open('w') as stream:
    result = subprocess.run(command, cwd=fuzz, env=env, stdout=stream, stderr=subprocess.STDOUT)
record = {'command': command, 'cwd': str(fuzz), 'exit_code': result.returncode, 'elapsed_seconds': time.monotonic() - started, 'env': {'RUST_LOG': env['RUST_LOG']}, 'source': '../source.json'}
(fuzz / 'run.json').write_text(json.dumps(record, indent=2) + '\n')
print(json.dumps(record), flush=True)
if result.returncode:
    raise SystemExit(result.returncode)
