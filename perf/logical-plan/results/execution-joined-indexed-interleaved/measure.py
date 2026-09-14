import hashlib
import importlib.util
import json
import os
from pathlib import Path
import statistics
import subprocess
import time


def main():
    root = Path('/workspace')
    base = root / 'perf/logical-plan/results'
    scratch = root / 'target/logical-plan-commits/joined-indexed-timing'
    output = base / 'execution-joined-indexed-interleaved'
    output.mkdir(exist_ok=True)
    assert not list(output.glob('native-*.txt'))
    spec = importlib.util.spec_from_file_location('summary', root / 'perf/logical-plan/summarize.py')
    summary = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(summary)
    manifests = {label: json.loads((base / directory / 'native-environment.json').read_text()) for label, directory in [('original', 'execution-original-joined-projections'), ('candidate', 'execution-joined-projections')]}
    for label, manifest in manifests.items():
        assert hashlib.sha256(Path(manifest['binary']).read_bytes()).hexdigest() == manifest['binary_sha256'], label
    processes = subprocess.check_output(['ps', '-eo', 'pid,stat,comm'], text=True)
    for line in processes.splitlines()[1:]:
        _, state, name = line.split()
        assert 'T' in state or not any(part in name for part in ['cargo', 'rustc', 'valgrind', 'callgrind', 'differential_f', 'prepare_bench', 'unnesting_exe', 'sqltest', 'integration_t']), line
    metadata = {'purpose': 'Diagnose the retained automatic indexed plan timing failure with alternating original/candidate order. Historical acceptance limits are unchanged.', 'source_manifests': {label: {key: manifest[key] for key in ['binary', 'binary_sha256', 'source_revision', 'features', 'profile']} for label, manifest in manifests.items()}, 'processes': processes, 'cpu': 0, 'rounds_per_binary': 7, 'samples_per_round': 10, 'sample_size': 1}
    (output / 'environment.json').write_text(json.dumps(metadata, indent=2) + '\n')
    records = []
    for repeat in range(1, 8):
        order = ['original', 'candidate'] if repeat % 2 else ['candidate', 'original']
        for label in order:
            command = ['taskset', '-c', '0', manifests[label]['binary'], '--color', 'never', '--bench', '--sample-count', '10', '--sample-size', '1', '--timer', 'os', 'automatic::membership_in_joined_projection_small_indexed$']
            env = dict(os.environ, TURSO_BENCH_PLAN_DIR=str(output / 'plans' / label))
            log = output / f'native-{repeat}-{label}.txt'
            started = time.monotonic()
            with log.open('w') as stream:
                result = subprocess.run(command, cwd=root, env=env, stdout=stream, stderr=subprocess.STDOUT)
            result.check_returncode()
            values = summary.native_run(log)
            assert set(values) == {'automatic/membership_in_joined_projection_small_indexed'}
            record = {'round': repeat, 'binary': label, 'command': command, 'exit_code': result.returncode, 'elapsed_seconds': time.monotonic() - started, 'median_ns': next(iter(values.values())), 'log': log.name}
            records.append(record)
            (output / 'measurements.json').write_text(json.dumps(records, indent=2) + '\n')
            print(label, repeat, record['median_ns'], flush=True)
    historical = json.loads((base / 'execution-original-joined-projections/summary.json').read_text())['automatic/membership_in_joined_projection_small_indexed']
    values = {label: [record['median_ns'] for record in records if record['binary'] == label] for label in manifests}
    paired = [candidate - original for original, candidate in zip(values['original'], values['candidate'])]
    outcome = {'original_ns': values['original'], 'candidate_ns': values['candidate'], 'paired_delta_ns': paired, 'original_median_ns': statistics.median(values['original']), 'candidate_median_ns': statistics.median(values['candidate']), 'paired_delta_median_ns': statistics.median(paired), 'fixed_original_median_ns': historical['native_median_ns'], 'fixed_original_uncertainty_ns': historical['native_uncertainty_ns'], 'candidate_exceeds_fixed_limit': statistics.median(values['candidate']) - historical['native_median_ns'] > historical['native_uncertainty_ns'], 'paired_delta_exceeds_fixed_uncertainty': statistics.median(paired) > historical['native_uncertainty_ns'], 'historical_results_preserved': True}
    (output / 'outcome.json').write_text(json.dumps(outcome, indent=2) + '\n')
    print(json.dumps(outcome, indent=2), flush=True)


main()
