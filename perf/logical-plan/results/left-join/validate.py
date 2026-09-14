import json
import os
from pathlib import Path
import subprocess
import time


root = Path('/workspace')
scratch = root / 'target/logical-plan-commits/left-join'
env = dict(os.environ, CARGO_TARGET_DIR=str(root / 'target/logical-plan-build'), CARGO_BUILD_JOBS='4')
focused = json.loads((scratch / 'focused.json').read_text())
assert len(focused) == 3 and all(step['exit_code'] == 0 for step in focused)
steps = []
for step in json.loads((scratch / 'validation-commands.json').read_text()):
    name, command = step['name'], step['command']
    print(name, flush=True)
    started = time.monotonic()
    with (scratch / (name + '.txt')).open('w') as stream:
        result = subprocess.run(command, cwd=root, env=env, stdout=stream, stderr=subprocess.STDOUT)
    record = dict(name=name, command=command, exit_code=result.returncode,
                  elapsed_seconds=time.monotonic() - started, log=name + '.txt')
    steps.append(record)
    (scratch / 'validation.json').write_text(json.dumps(steps, indent=2) + '\n')
    print(json.dumps(record), flush=True)
    if result.returncode:
        print((scratch / (name + '.txt')).read_text()[-8000:], flush=True)
        raise SystemExit(result.returncode)
