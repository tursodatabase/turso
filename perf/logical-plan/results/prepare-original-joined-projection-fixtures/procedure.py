from pathlib import Path
import difflib
import hashlib
import json
import os
import shutil
import signal
import subprocess
import time


def main():
    root = Path('/workspace')
    branch = 'logical-plan-codex'
    original = 'a9a8779c1906247ae3ae78cd098ba713c27d8c9b'
    storage = root / 'target/logical-plan-original-joined-projection-fixtures'
    storage.mkdir(exist_ok=False)
    harness_paths = {
        'prepare_benchmark': 'core/benches/prepare_benchmark.rs',
        'unnesting_execution': 'core/benches/unnesting_execution.rs',
    }
    user_inputs = ['prompt2.md', 'Neumann-Unnesting-1.pdf', 'Neumann-Unnesting-2.pdf']

    def git(*arguments):
        return subprocess.check_output(['git', *arguments], cwd=root, text=True)

    def mutate(*arguments):
        subprocess.run(['git', *arguments], cwd=root, check=True)

    assert git('branch', '--show-current').strip() == branch
    candidate = git('rev-parse', 'HEAD').strip()
    protected = set(git('diff', '--name-only').splitlines())
    assert set(harness_paths.values()).issubset(protected)
    staged = git('ls-files', '--stage', '--', *user_inputs)
    staged_diff = git('diff', '--cached', '--name-status')
    hashes = {
        name: hashlib.sha256((root / name).read_bytes()).hexdigest()
        for name in sorted(protected) + user_inputs
    }
    harnesses = {name: (root / path).read_bytes() for name, path in harness_paths.items()}
    for name, content in harnesses.items():
        (storage / f'{name}.rs').write_bytes(content)
    draft_patch = storage / 'workspace-changes.patch'
    draft_patch.write_text(git('diff', '--binary'))
    (storage / 'before.json').write_text(json.dumps({
        'branch': branch,
        'candidate_revision': candidate,
        'original_revision': original,
        'file_sha256': hashes,
        'staged_entries': staged,
        'staged_diff': staged_diff,
    }, indent=2) + '\n')
    removed_drafts = False
    changed_revision = False
    changed_harness = False
    process = None
    source_diff = None
    steps = []
    started = time.monotonic()
    try:
        mutate('apply', '--reverse', '--check', str(draft_patch))
        mutate('apply', '--reverse', str(draft_patch))
        removed_drafts = True
        mutate('switch', '--detach', original)
        changed_revision = True
        assert not (root / harness_paths['unnesting_execution']).exists()
        changed_harness = True
        for name, path in harness_paths.items():
            (root / path).write_bytes(harnesses[name])
        cargo = root / 'core/Cargo.toml'
        cargo.write_text(cargo.read_text() + '\n[[bench]]\nname = "unnesting_execution"\nharness = false\nrequired-features = ["bench"]\n')
        expected_paths = {'core/Cargo.toml', harness_paths['prepare_benchmark']}
        assert set(git('diff', '--name-only').splitlines()) == expected_paths
        source_diff = git('diff', '--', *sorted(expected_paths))
        source_diff += ''.join(difflib.unified_diff(
            [], harnesses['unnesting_execution'].decode().splitlines(keepends=True),
            fromfile='/dev/null', tofile='b/' + harness_paths['unnesting_execution']))
        env = dict(os.environ, CARGO_TARGET_DIR=str(root / 'target/logical-plan-build'), CARGO_BUILD_JOBS='4')
        for name, features in [('prepare_benchmark', 'bench'), ('unnesting_execution', 'bench,simulator')]:
            executable = None
            command = ['cargo', 'build', '--locked', '-p', 'turso_core', '--features', features,
                       '--bench', name, '--profile', 'dev', '--message-format=json']
            print(f'Building original engine: {name}.', flush=True)
            step_started = time.monotonic()
            with (storage / f'{name}-build.txt').open('w') as log:
                process = subprocess.Popen(command, cwd=root, env=env, text=True,
                                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                           start_new_session=True)
                for line in process.stdout:
                    log.write(line)
                    log.flush()
                    try:
                        message = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if (message.get('reason') == 'compiler-artifact'
                            and message.get('target', {}).get('name') == name
                            and message.get('executable')):
                        executable = Path(message['executable'])
                result = process.wait()
            steps.append({'name': name, 'command': command, 'exit_code': result,
                          'elapsed_seconds': time.monotonic() - step_started,
                          'log': f'{name}-build.txt'})
            (storage / 'builds.json').write_text(json.dumps(steps, indent=2) + '\n')
            assert result == 0, f'{name} build failed; see {storage}'
            assert executable is not None and executable.is_file()
            shutil.copy2(executable, storage / name)
        assert set(git('diff', '--name-only').splitlines()) == expected_paths
    finally:
        if process is not None and process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
        if changed_harness:
            mutate('restore', '--source=HEAD', '--worktree', '--',
                   'core/Cargo.toml', harness_paths['prepare_benchmark'])
            (root / harness_paths['unnesting_execution']).unlink()
        if changed_revision:
            mutate('switch', branch)
        if removed_drafts:
            mutate('apply', '--check', str(draft_patch))
            mutate('apply', str(draft_patch))
        assert git('branch', '--show-current').strip() == branch
        assert git('rev-parse', 'HEAD').strip() == candidate
        assert git('ls-files', '--stage', '--', *user_inputs) == staged
        assert git('diff', '--cached', '--name-status') == staged_diff
        for name, expected in hashes.items():
            assert hashlib.sha256((root / name).read_bytes()).hexdigest() == expected, name
        assert set(git('diff', '--name-only').splitlines()) == protected
        (storage / 'restored.json').write_text(json.dumps({
            'branch': branch, 'revision': candidate, 'file_sha256': hashes,
            'staged_entries': staged, 'staged_diff': staged_diff,
            'elapsed_seconds': time.monotonic() - started,
        }, indent=2) + '\n')
        print('Implementation branch, workspace files and staged inputs restored.', flush=True)
    for name, directory, features in [
        ('prepare_benchmark', 'prepare-original-joined-projection-fixtures', 'default,bench'),
        ('unnesting_execution', 'execution-original-joined-projections', 'default,bench,simulator'),
    ]:
        binary = storage / name
        assert binary.is_file()
        output = root / 'perf/logical-plan/results' / directory
        output.mkdir(exist_ok=False)
        manifest = {
            'base_revision': original,
            'harness_revision': candidate + '+joined-projection-fixtures',
            'harness_sha256': hashlib.sha256(harnesses[name]).hexdigest(),
            'binary': str(binary),
            'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
            'profile': 'dev', 'features': features, 'source_diff': source_diff,
            'engine_changes': False, 'deferred_drafts_in_binary': False,
            'native_isolation': 'The build and workspace restoration finish before native measurements.',
        }
        (output / 'binary.json').write_text(json.dumps(manifest, indent=2) + '\n')
        shutil.copyfile(storage / f'{name}-build.txt', output / 'build.txt')
        shutil.copyfile(storage / 'restored.json', output / 'restored.json')
        shutil.copyfile(storage / 'builds.json', output / 'builds.json')
        shutil.copyfile(Path(__file__), output / 'procedure.py')


if __name__ == '__main__':
    main()
