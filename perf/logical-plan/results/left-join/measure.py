import hashlib
import json
from pathlib import Path
import subprocess
import time


root = Path('/workspace')
scratch = root / 'target/logical-plan-commits/left-join'
results = root / 'perf/logical-plan/results'
previous = json.loads((root / 'target/logical-plan-commits/mark-projections/measurements.json').read_text())
assert len(previous) == 8 and all(step['exit_code'] == 0 for step in previous)
validation = json.loads((scratch / 'validation.json').read_text())
assert len(validation) == 7 and all(step['exit_code'] == 0 for step in validation)
assert not json.loads((scratch / 'execution/comparison.json').read_text())['existing_changed_plans']
source = json.loads((scratch / 'source.json').read_text())
for name in ['prepare_benchmark', 'unnesting_execution']:
    binary = source['binaries'][name]
    assert hashlib.sha256(Path(binary['path']).read_bytes()).hexdigest() == binary['sha256']
original = {}
for kind, directory in [('prepare', 'prepare-original-left-join-fixtures'), ('execution', 'execution-original-left-joins')]:
    manifest = json.loads((results / directory / 'binary.json').read_text())
    assert hashlib.sha256(Path(manifest['binary']).read_bytes()).hexdigest() == manifest['binary_sha256']
    original[kind] = manifest
processes = subprocess.check_output(['ps', '-eo', 'pid,comm'], text=True)
assert not any(any(name in line for name in ['cargo', 'rustc', 'valgrind', 'callgrind', 'differential_f', 'prepare_bench', 'unnesting_exe', 'sqltest', 'integration_t']) for line in processes.splitlines())
(scratch / 'isolation.json').write_text(json.dumps({'processes_before_measurements': processes, 'saved_source': 'source.json', 'builds_and_checks_finished_before_native_measurements': True}, indent=2) + '\n')
filters = json.loads((root / 'target/logical-plan-commits/membership-joined-projections/filters.json').read_text())
prior = filters['prepare'] + '|subquery_(exists|in|row_not_in)_result_with_exists_filter$'
new_prepare = 'subquery_left_join_(with_exists_filter|rewritten_input)$'
execution = 'left_join_(with_exists_filter|rewritten_input)$'
jobs = [
    ('prepare-original-left-join-fixtures', original['prepare']['binary'], 'prepare', new_prepare, 'a9a8779c1906247ae3ae78cd098ba713c27d8c9b plus identical LEFT JOIN fixtures; binary.json records source'),
    ('prepare-left-join', source['binaries']['prepare_benchmark']['path'], 'prepare', prior + '|' + new_prepare + '|join_four_way_mixed$', source['head'] + ' plus LEFT JOIN adapter; left-join/source.json records the exact saved source'),
    ('execution-original-left-joins', original['execution']['binary'], 'execution', execution, 'a9a8779c1906247ae3ae78cd098ba713c27d8c9b plus identical LEFT JOIN fixtures; binary.json records source'),
    ('execution-left-join', source['binaries']['unnesting_execution']['path'], 'execution', execution, source['head'] + ' plus LEFT JOIN adapter; left-join/source.json records the exact saved source'),
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
print('All LEFT JOIN measurements completed.', flush=True)
