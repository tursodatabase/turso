import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

root = Path('/workspace')
scratch = root / 'target/logical-plan-commits/membership-comparison-copies'
saved = root / 'target/logical-plan-membership-comparison-copies'
saved.mkdir(exist_ok=True)
env = dict(os.environ, CARGO_TARGET_DIR=str(root / 'target/logical-plan-build'), CARGO_BUILD_JOBS='4')
validation = json.loads((scratch / 'validation.json').read_text())
assert validation[-1]['name'] == 'clippy' and all(step['exit_code'] == 0 for step in validation)
before = json.loads((scratch / 'before.json').read_text())
owned = {'core/translate/relational/scalar.rs', 'core/translate/relational/rules/tests.rs'}
for name, digest in before['sha256'].items():
    if name not in owned:
        assert hashlib.sha256((root / name).read_bytes()).hexdigest() == digest, name
assert subprocess.check_output(['git', 'ls-files', '--stage', '--', 'prompt2.md', 'Neumann-Unnesting-1.pdf', 'Neumann-Unnesting-2.pdf'], cwd=root, text=True) == before['input_index']
paths = [*owned, 'core/translate/optimizer/mod.rs', 'core/translate/optimizer/unnest.rs', 'core/translate/relational/membership.rs', 'core/translate/relational/lower.rs', 'core/translate/main_loop/close.rs', 'core/benches/prepare_benchmark.rs', 'core/benches/unnesting_execution.rs']
source = {'head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip(), 'source_sha256': {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in paths}, 'diff': subprocess.check_output(['git', 'diff', 'HEAD', '--', *paths], cwd=root, text=True), 'binaries': {}}
fuzzer = saved / 'differential_fuzzer'
shutil.copy2(root / 'target/logical-plan-build/debug/differential_fuzzer', fuzzer)
source['binaries']['differential_fuzzer'] = {'path': str(fuzzer), 'sha256': hashlib.sha256(fuzzer.read_bytes()).hexdigest(), 'build': 'validation.json runner-build'}
records = []
for name, features in [('prepare_benchmark', 'bench'), ('unnesting_execution', 'bench,simulator')]:
    command = ['cargo', 'build', '-p', 'turso_core', '--features', features, '--bench', name, '--profile', 'dev', '--message-format=json']
    started = time.monotonic()
    print('building', name, flush=True)
    with (scratch / (name + '-build.jsonl')).open('w') as stdout, (scratch / (name + '-build.txt')).open('w') as stderr:
        result = subprocess.run(command, cwd=root, env=env, stdout=stdout, stderr=stderr)
    record = {'name': name, 'command': command, 'exit_code': result.returncode, 'elapsed_seconds': time.monotonic() - started}
    records.append(record)
    (scratch / 'builds.json').write_text(json.dumps(records, indent=2) + '\n')
    if result.returncode:
        raise SystemExit(result.returncode)
    executables = [Path(message['executable']) for line in (scratch / (name + '-build.jsonl')).read_text().splitlines() if (message := json.loads(line)).get('reason') == 'compiler-artifact' and message.get('executable') and message['target']['name'] == name]
    assert len(executables) == 1, executables
    binary = saved / name
    shutil.copy2(executables[0], binary)
    source['binaries'][name] = {'path': str(binary), 'sha256': hashlib.sha256(binary.read_bytes()).hexdigest(), 'features': features, 'profile': 'dev'}
    (scratch / 'source.json').write_text(json.dumps(source, indent=2) + '\n')
    print(json.dumps(record), flush=True)
for name, digest in source['source_sha256'].items():
    assert hashlib.sha256((root / name).read_bytes()).hexdigest() == digest, name
print('Source and saved binaries recorded.', flush=True)
