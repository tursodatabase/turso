import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

root = Path('/workspace')
out = root / 'target/logical-plan-commits/lazy-legacy-copy/fixtures'
out.mkdir()
parent = out.parent
before = json.loads((parent / 'before.json').read_text())
source = json.loads((parent / 'source.json').read_text())
owned = ['core/translate/optimizer/mod.rs', 'core/translate/optimizer/unnest.rs']
for name, digest in source['source_sha256'].items():
    assert hashlib.sha256((root / name).read_bytes()).hexdigest() == digest, name
restore = {name: (root / name).read_bytes() for name in owned}
for name, content in restore.items():
    (out / ('restore-' + Path(name).name)).write_bytes(content)
env = dict(os.environ, CARGO_TARGET_DIR=str(root / 'target/logical-plan-build'), CARGO_BUILD_JOBS='4')
command = ['cargo', 'build', '-p', 'turso_core', '--features', 'bench', '--bench', 'prepare_benchmark', '--profile', 'dev', '--message-format=json']
records = []
try:
    for name, old in zip(owned, ['before-mod.rs', 'before-unnest.rs']):
        content = (parent / old).read_bytes()
        assert hashlib.sha256(content).hexdigest() == before['sha256'][name]
        (root / name).write_bytes(content)
    phases = ['before', 'after']
    for phase in phases:
        if phase == 'after':
            for name, content in restore.items():
                (root / name).write_bytes(content)
        destination = out / phase
        destination.mkdir()
        print('building fixtures:', phase, flush=True)
        started = time.monotonic()
        with (destination / 'build.txt').open('w') as stream:
            result = subprocess.run(command, cwd=root, env=env, stdout=stream, stderr=subprocess.STDOUT)
        record = {'phase': phase, 'command': command, 'exit_code': result.returncode, 'elapsed_seconds': time.monotonic()-started}
        records.append(record)
        (out / 'builds.json').write_text(json.dumps(records, indent=2) + '\n')
        if result.returncode:
            raise SystemExit(result.returncode)
        artifacts = []
        for line in (destination / 'build.txt').read_text().splitlines():
            try:
                message = json.loads(line)
            except json.JSONDecodeError:
                continue
            if message.get('reason') == 'compiler-artifact' and message.get('target',{}).get('name') == 'prepare_benchmark' and message.get('executable'):
                artifacts.append(message['executable'])
        assert len(artifacts) == 1
        binary = destination / 'prepare_benchmark'
        shutil.copy2(artifacts[0], binary)
        record['binary_sha256'] = hashlib.sha256(binary.read_bytes()).hexdigest()
        record['source_sha256'] = {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in source['source_sha256']}
        record['benchmark_sha256'] = hashlib.sha256((root / 'core/benches/prepare_benchmark.rs').read_bytes()).hexdigest()
        record['benchmark_diff'] = subprocess.check_output(['git', 'diff', '--', 'core/benches/prepare_benchmark.rs'], cwd=root, text=True)
        record['source'] = '4266a6367 with the same deferred drafts and the three new fixtures' if phase == 'before' else 'source.json plus the same three new fixtures'
        (out / 'builds.json').write_text(json.dumps(records, indent=2) + '\n')
        print(json.dumps({key: value for key, value in record.items() if key not in ['benchmark_diff', 'source_sha256']}), flush=True)
finally:
    for name, content in restore.items():
        (root / name).write_bytes(content)
    for name, digest in source['source_sha256'].items():
        assert hashlib.sha256((root / name).read_bytes()).hexdigest() == digest, name
    (out / 'restored.json').write_text(json.dumps({'source_restored': True, 'head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip()}, indent=2) + '\n')
