import json
import os
from pathlib import Path
import subprocess
import time

root = Path('/workspace')
out = root / 'target/logical-plan-commits/membership-joined-projections'
env = dict(os.environ, CARGO_TARGET_DIR=str(root / 'target/logical-plan-build'), CARGO_BUILD_JOBS='4')
previous = json.loads((out / 'validation.json').read_text())
records = []
for name in ['fmt', 'forced', 'sql', 'clippy']:
    command = next(step['command'] for step in previous if step['name'] == name)
    log = 'boundary-' + name + '.txt'
    print(name, flush=True)
    started = time.monotonic()
    with (out / log).open('w') as stream:
        result = subprocess.run(command, cwd=root, env=env, stdout=stream, stderr=subprocess.STDOUT)
    records.append({'name': name, 'command': command, 'exit_code': result.returncode, 'elapsed_seconds': time.monotonic() - started, 'log': log})
    (out / 'boundary-validation.json').write_text(json.dumps(records, indent=2) + '\n')
    print(json.dumps(records[-1]), flush=True)
    if result.returncode:
        raise SystemExit(result.returncode)
