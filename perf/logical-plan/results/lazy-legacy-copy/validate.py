import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

root = Path('/workspace')
out = root / 'target/logical-plan-commits/lazy-legacy-copy'
env = dict(os.environ, CARGO_TARGET_DIR=str(root / 'target/logical-plan-build'), CARGO_BUILD_JOBS='4')
previous = json.loads((root / 'target/logical-plan-commits/membership-outer-projection/validation.json').read_text())
commands = [('fmt', ['cargo', 'fmt', '--all'])]
base = ['cargo', 'test', '-p', 'turso_core', '-p', 'core_tester', '-p', 'sqltest', '-p', 'differential-fuzzer', '--features', 'turso_core/fts,turso_core/bench', '--lib', '--test', 'integration_tests']
commands += [('optimizer', base + ['translate::optimizer::tests::'])]
commands += [(step['name'], step['command']) for step in previous if step['name'] in ['relational', 'forced', 'integration', 'runner-build', 'sql', 'clippy', 'prepare-build', 'execution-build']]
steps = []
saved = root / 'target/logical-plan-lazy-legacy-copy'
saved.mkdir(exist_ok=True)
for name, command in commands:
    print(name, flush=True)
    started = time.monotonic()
    with (out / f'{name}.txt').open('w') as stream:
        result = subprocess.run(command, cwd=root, env=env, stdout=stream, stderr=subprocess.STDOUT)
    step = {'name': name, 'command': command, 'exit_code': result.returncode, 'elapsed_seconds': time.monotonic() - started, 'log': f'{name}.txt'}
    steps.append(step)
    (out / 'validation.json').write_text(json.dumps(steps, indent=2) + '\n')
    print(json.dumps(step), flush=True)
    if result.returncode:
        raise SystemExit(result.returncode)
    if name in ['prepare-build', 'execution-build']:
        target = 'prepare_benchmark' if name == 'prepare-build' else 'unnesting_execution'
        artifacts = []
        for line in (out / f'{name}.txt').read_text().splitlines():
            try:
                message = json.loads(line)
            except json.JSONDecodeError:
                continue
            if message.get('reason') == 'compiler-artifact' and message.get('target', {}).get('name') == target and message.get('executable'):
                artifacts.append(message['executable'])
        assert len(artifacts) == 1, artifacts
        shutil.copy2(artifacts[0], saved / target)
        step['binary'] = str(saved / target)
        step['binary_sha256'] = hashlib.sha256((saved / target).read_bytes()).hexdigest()
        (out / 'validation.json').write_text(json.dumps(steps, indent=2) + '\n')
shutil.copy2(root / 'target/logical-plan-build/debug/differential_fuzzer', saved / 'differential_fuzzer')
paths = ['core/translate/optimizer/mod.rs', 'core/translate/optimizer/unnest.rs', 'core/translate/relational/lower.rs', 'core/translate/main_loop/close.rs']
manifest = {
    'head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip(),
    'source_sha256': {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in paths},
    'working_diff': subprocess.check_output(['git', 'diff', '--', *paths], cwd=root, text=True),
    'saved_binaries': {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in saved.iterdir()},
    'deferred_drafts': 'Previously deferred UPDATE FROM and empty automatic-index drafts remain unchanged in the working tree and are excluded from this implementation commit.',
}
(out / 'source.json').write_text(json.dumps(manifest, indent=2) + '\n')
