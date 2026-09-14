import gzip
import hashlib
import json
from pathlib import Path
import subprocess
import time

root = Path('/workspace')
output = root / 'target/logical-plan-commits/prepare-components'
binary = root / 'target/logical-plan-membership-outer-projection/prepare_benchmark'
scope = 'prepare_benchmark::measure_prepare'
phases = {
    'normalize': 'turso_core::translate::relational::rewrite::normalize_only',
    'explore': 'turso_core::translate::relational::rewrite::explore',
    'validate': 'turso_core::translate::relational::LogicalPlan::validate',
    'lower': 'turso_core::translate::relational::lower::rewrite_select',
}
steps = json.loads((output / 'steps.json').read_text())
for phase, boundary in phases.items():
    directory = output / phase
    directory.mkdir()
    command = [
        'taskset', '-c', '0', 'valgrind', '--tool=callgrind',
        '--collect-atstart=no', f'--toggle-collect={scope}',
        f'--dump-before={scope}', f'--dump-after={scope}',
        f'--zero-before={boundary}', f'--dump-after={boundary}',
        f'--callgrind-out-file={directory / "callgrind.out"}',
        str(binary), '--color', 'never', '--test', 'subquery_in_correlated_filter$',
    ]
    started = time.monotonic()
    with (directory / 'run.txt').open('w') as stream:
        result = subprocess.run(command, cwd=root, stdout=stream, stderr=subprocess.STDOUT)
    step = {
        'phase': phase, 'boundary': boundary, 'collection_scope': scope,
        'command': command, 'exit_code': result.returncode,
        'elapsed_seconds': time.monotonic() - started,
        'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
    }
    steps.append(step)
    (output / 'steps.json').write_text(json.dumps(steps, indent=2) + '\n')
    dumps = []
    for path in sorted(directory.glob('callgrind.out*')):
        data = path.read_bytes()
        fields = [line for line in data.decode().splitlines()
                  if line.startswith(('desc:', 'events:', 'summary:', 'totals:'))]
        with gzip.open(str(path) + '.gz', 'wb') as stream:
            stream.write(data)
        path.unlink()
        dumps.append({'file': path.name + '.gz', 'raw': fields})
    (directory / 'dumps.json').write_text(json.dumps(dumps, indent=2) + '\n')
    print(json.dumps({'phase': phase, 'exit_code': result.returncode, 'dumps': dumps}), flush=True)
    if result.returncode != 0:
        raise SystemExit(result.returncode)
