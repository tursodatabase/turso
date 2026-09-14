import hashlib,json,os,shutil,subprocess,time
from pathlib import Path
root=Path("/workspace")
scratch=root/"target/logical-plan-commits/left-join"
saved=root/"target/logical-plan-left-join"
env=dict(os.environ,CARGO_TARGET_DIR=str(root/"target/logical-plan-build"),CARGO_BUILD_JOBS="4")
source=json.loads((scratch/"source.json").read_text())
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
