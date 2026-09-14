import hashlib
import json
from pathlib import Path
import subprocess
import time

root = Path('/workspace')
scratch = root / 'target/logical-plan-commits/membership-comparison-copies'
saved = root / 'target/logical-plan-membership-comparison-copies/prepare_benchmark'
source = json.loads((scratch / 'source.json').read_text())
assert hashlib.sha256(saved.read_bytes()).hexdigest() == source['binaries']['prepare_benchmark']['sha256']
previous = json.loads((root / 'target/logical-plan-commits/membership-joined-projections/measurements.json').read_text())
assert len(previous) == 12 and all(step['exit_code'] == 0 for step in previous), 'Finish the joined-projection benchmark series first.'
assert not json.loads((scratch / 'execution/comparison.json').read_text())['changed_plans']
assert json.loads((scratch / 'seed-57291020/outcome.json').read_text())['counts']['Oracle Failures'] == 0
processes = subprocess.check_output(['ps', '-eo', 'pid,comm'], text=True)
assert not any(any(name in line for name in ['cargo', 'rustc', 'valgrind', 'callgrind', 'differential_f', 'prepare_bench', 'unnesting_exe', 'sqltest', 'integration_t']) for line in processes.splitlines())
(scratch / 'isolation.json').write_text(json.dumps({'processes_before_measurement': processes, 'saved_binary_source': 'source.json', 'previous_benchmark_series_completed': True}, indent=2) + '\n')
filters = json.loads((root / 'target/logical-plan-commits/membership-joined-projections/filters.json').read_text())
output = root / 'perf/logical-plan/results/prepare-membership-comparison-copies'
records = []
for phase in ['native', 'callgrind']:
    command = ['python3', str(root / 'perf/logical-plan/measure.py'), str(saved), str(output), '--phase', phase, '--kind', 'prepare', '--cpu', str(filters['cpu']), '--filter', filters['prepare'], '--source-revision', source['head'] + ' plus moved membership operands; membership-comparison-copies/source.json records the saved executable']
    started = time.monotonic()
    result = subprocess.run(command, cwd=root)
    records.append({'phase': phase, 'command': command, 'exit_code': result.returncode, 'elapsed_seconds': time.monotonic() - started})
    (scratch / 'measurements.json').write_text(json.dumps(records, indent=2) + '\n')
    result.check_returncode()
