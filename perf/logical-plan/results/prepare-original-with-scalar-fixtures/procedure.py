from pathlib import Path
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
    storage = root / 'target/logical-plan-original-scalar-fixtures'
    storage.mkdir(exist_ok=False)
    benchmark_path = 'core/benches/prepare_benchmark.rs'
    deferred = {
        'core/translate/main_loop/close.rs',
        'core/translate/optimizer/mod.rs',
        'core/translate/relational/lower.rs',
        'sqlite/conformance/sqlite-sqltests/join/memory.sqltest',
        'sqlite/conformance/sqlite-sqltests/update-from.sqltest',
    }
    user_inputs = ['prompt2.md', 'Neumann-Unnesting-1.pdf', 'Neumann-Unnesting-2.pdf']

    def git(*arguments):
        return subprocess.check_output(['git', *arguments], cwd=root, text=True)

    def mutate(*arguments):
        subprocess.run(['git', *arguments], cwd=root, check=True)

    assert git('branch', '--show-current').strip() == branch
    candidate = git('rev-parse', 'HEAD').strip()
    assert set(git('diff', '--name-only').splitlines()) == deferred
    staged = git('diff', '--cached', '--name-status')
    hashes = {
        name: hashlib.sha256((root / name).read_bytes()).hexdigest()
        for name in sorted(deferred) + user_inputs
    }
    harness = (root / benchmark_path).read_bytes()
    (storage / 'prepare_benchmark.rs').write_bytes(harness)
    draft_patch = storage / 'deferred-drafts.patch'
    draft_patch.write_text(git('diff', '--binary'))
    (storage / 'before.json').write_text(json.dumps({
        'branch': branch,
        'candidate_revision': candidate,
        'original_revision': original,
        'file_sha256': hashes,
        'staged_entries': staged,
    }, indent=2) + '\n')
    removed_drafts = False
    changed_revision = False
    changed_harness = False
    process = None
    source_diff = None
    executable = None
    started = time.monotonic()
    try:
        mutate('apply', '--reverse', '--check', str(draft_patch))
        mutate('apply', '--reverse', str(draft_patch))
        removed_drafts = True
        mutate('switch', '--detach', original)
        changed_revision = True
        (root / benchmark_path).write_bytes(harness)
        changed_harness = True
        assert git('diff', '--name-only').splitlines() == [benchmark_path]
        source_diff = git('diff', '--', benchmark_path)
        env = dict(os.environ, CARGO_TARGET_DIR=str(root / 'target/logical-plan-build'), CARGO_BUILD_JOBS='4')
        command = ['cargo', 'build', '--locked', '-p', 'turso_core', '--features', 'bench', '--bench', 'prepare_benchmark', '--profile', 'dev', '--message-format=json']
        print('Building the original engine with the current prepare fixtures.', flush=True)
        with (storage / 'build.txt').open('w') as log:
            log.write('Command: ' + repr(command) + '\n')
            process = subprocess.Popen(command, cwd=root, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=True)
            for line in process.stdout:
                log.write(line)
                log.flush()
                try:
                    message = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if message.get('reason') == 'compiler-artifact' and message.get('target', {}).get('name') == 'prepare_benchmark' and message.get('executable'):
                    executable = Path(message['executable'])
            result = process.wait()
        assert result == 0, f'Original build failed with exit {result}; see {storage / "build.txt"}'
        assert executable is not None and executable.is_file()
        assert git('diff', '--name-only').splitlines() == [benchmark_path]
        shutil.copy2(executable, storage / 'prepare_benchmark')
        print('Original executable saved; restoring the implementation branch.', flush=True)
    finally:
        if process is not None and process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
        if changed_harness:
            mutate('restore', '--source=HEAD', '--worktree', '--', benchmark_path)
        if changed_revision:
            mutate('switch', branch)
        if removed_drafts:
            mutate('apply', '--check', str(draft_patch))
            mutate('apply', str(draft_patch))
        assert git('branch', '--show-current').strip() == branch
        assert git('rev-parse', 'HEAD').strip() == candidate
        assert git('diff', '--cached', '--name-status') == staged
        for name, expected in hashes.items():
            assert hashlib.sha256((root / name).read_bytes()).hexdigest() == expected, name
        assert set(git('diff', '--name-only').splitlines()) == deferred
        (storage / 'restored.json').write_text(json.dumps({
            'branch': branch,
            'revision': candidate,
            'file_sha256': hashes,
            'staged_entries': staged,
            'elapsed_seconds': time.monotonic() - started,
        }, indent=2) + '\n')
        print('Implementation branch, deferred drafts, and staged inputs restored unchanged.', flush=True)
    binary = storage / 'prepare_benchmark'
    assert binary.is_file()
    output = root / 'perf/logical-plan/results/prepare-original-with-scalar-fixtures'
    output.mkdir(exist_ok=False)
    manifest = {
        'base_revision': original,
        'harness_revision': candidate,
        'binary': str(binary),
        'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
        'profile': 'dev',
        'features': 'default,bench',
        'source_diff': source_diff,
        'engine_changes': False,
        'deferred_drafts_in_binary': False,
        'native_isolation': 'The build completed and the implementation branch was restored before measurements; no concurrent builds or other benchmarks.',
        'purpose': 'Original-engine measurements for ten newer fixtures; the five existing fixtures are additional diagnostics and do not replace their fixed historical baseline.',
    }
    (output / 'binary.json').write_text(json.dumps(manifest, indent=2) + '\n')
    shutil.copyfile(storage / 'build.txt', output / 'build.txt')
    shutil.copyfile(storage / 'restored.json', output / 'restored.json')
    shutil.copyfile(Path(__file__), output / 'procedure.py')
    filter_expr = 'subquery_(repeated_filters|distinct_filters|scalar_correlated|exists_correlated|scalar_first_row_with_exists|scalar_empty_result_with_not_exists)|select_point_lookup_pk|insert_single_row_params|insert_upsert_on_conflict'
    for phase in ['native', 'callgrind']:
        subprocess.run(['python3', 'perf/logical-plan/measure.py', str(binary), str(output), '--phase', phase, '--cpu', '0', '--filter', filter_expr, '--source-revision', original + '+current-prepare-fixtures'], cwd=root, check=True)


if __name__ == '__main__':
    main()
