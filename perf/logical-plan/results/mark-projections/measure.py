import hashlib
import json
from pathlib import Path
import subprocess
import time


root = Path('/workspace')
scratch = root / 'target/logical-plan-commits/mark-projections'
results = root / 'perf/logical-plan/results'
previous = json.loads((root / 'target/logical-plan-commits/membership-comparison-copies/measurements.json').read_text())
assert len(previous) == 2 and all(step['exit_code'] == 0 for step in previous)
validation = json.loads((results / 'mark-projections/validation.json').read_text())
assert validation and all(step['exit_code'] == 0 for step in validation)
checks = json.loads((results / 'mark-projections/outcome.json').read_text())
assert checks['existing_plan_changes'] == 0
assert checks['execution_fixture_modes'] == 123
assert checks['fuzz']['Oracle Failures'] == 0
assert all(checks['original_same_plans_by_mode'].values())
source = json.loads((scratch / 'source.json').read_text())
for name in ['prepare_benchmark', 'unnesting_execution']:
    binary = source['binaries'][name]
    assert hashlib.sha256(Path(binary['path']).read_bytes()).hexdigest() == binary['sha256']
original = {}
for kind, directory in [('prepare', 'prepare-original-mark-result-fixtures'), ('execution', 'execution-original-mark-results')]:
    manifest = json.loads((results / directory / 'binary.json').read_text())
    assert hashlib.sha256(Path(manifest['binary']).read_bytes()).hexdigest() == manifest['binary_sha256']
    original[kind] = manifest
processes = subprocess.check_output(['ps', '-eo', 'pid,comm'], text=True)
assert not any(any(name in line for name in ['cargo', 'rustc', 'valgrind', 'callgrind', 'differential_f', 'prepare_bench', 'unnesting_exe', 'sqltest', 'integration_t']) for line in processes.splitlines())
(scratch / 'isolation.json').write_text(json.dumps({'processes_before_measurements': processes, 'saved_source': 'source.json', 'all_builds_and_checks_completed_before_native_measurements': True}, indent=2) + '\n')
filters = json.loads((root / 'target/logical-plan-commits/membership-joined-projections/filters.json').read_text())
new_prepare = 'subquery_(exists|in|row_not_in)_result_with_exists_filter$'
new_execution = '(exists|in|row_not_in)_result_with_exists_filter$'
jobs = [
    ('prepare-original-mark-result-fixtures', original['prepare']['binary'], 'prepare', new_prepare, 'a9a8779c1906247ae3ae78cd098ba713c27d8c9b plus identical mark result fixtures; binary.json records source'),
    ('prepare-mark-projections', source['binaries']['prepare_benchmark']['path'], 'prepare', filters['prepare'] + '|' + new_prepare, '19cd13a2fc2d77741ba012a5a76f6ff5456ef45b implementation plus preserved deferred drafts; mark-projections/source.json records exact saved source'),
    ('execution-original-mark-results', original['execution']['binary'], 'execution', 'automatic::' + new_execution, 'a9a8779c1906247ae3ae78cd098ba713c27d8c9b plus identical mark result fixtures; binary.json records source'),
    ('execution-mark-projections', source['binaries']['unnesting_execution']['path'], 'execution', new_execution, '19cd13a2fc2d77741ba012a5a76f6ff5456ef45b implementation plus preserved deferred drafts; mark-projections/source.json records exact saved source'),
]
(scratch / 'measurement-jobs.json').write_text(json.dumps(jobs, indent=2) + '\n')
records = []
for phase in ['native', 'callgrind']:
    for directory, binary, kind, pattern, revision in jobs:
        command = ['python3', str(root / 'perf/logical-plan/measure.py'), binary, str(results / directory), '--phase', phase, '--kind', kind, '--cpu', str(filters['cpu']), '--filter', pattern, '--source-revision', revision]
        started = time.monotonic()
        result = subprocess.run(command, cwd=root)
        records.append({'directory': directory, 'phase': phase, 'command': command, 'exit_code': result.returncode, 'elapsed_seconds': time.monotonic() - started})
        (scratch / 'measurements.json').write_text(json.dumps(records, indent=2) + '\n')
        result.check_returncode()
print('All mark-result measurements completed.', flush=True)
