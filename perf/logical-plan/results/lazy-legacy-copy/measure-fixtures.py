import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import time

root = Path('/workspace')
scratch = root / 'target/logical-plan-commits/lazy-legacy-copy'
fixtures = scratch / 'fixtures'
assert json.loads((fixtures / 'restored.json').read_text())['source_restored']
assert all(step['exit_code'] == 0 for step in json.loads((fixtures / 'builds.json').read_text()))
assert all(step['exit_code'] == 0 for step in json.loads((fixtures / 'validation.json').read_text()))
source = json.loads((scratch / 'source.json').read_text())
for name, digest in source['source_sha256'].items():
    assert hashlib.sha256((root / name).read_bytes()).hexdigest() == digest, name
processes = subprocess.check_output(['ps', '-eo', 'pid,comm'], text=True)
assert not any(any(name in line for name in ['cargo', 'rustc', 'valgrind', 'callgrind', 'differential_f', 'prepare_bench', 'unnesting_exe', 'sqltest']) for line in processes.splitlines())
(fixtures / 'isolation.json').write_text(json.dumps({'processes_before_measurements': processes, 'builds_complete': True, 'native_runs_are_sequential': True}, indent=2) + '\n')
base = root / 'perf/logical-plan/results'
pattern = 'subquery_(scalar_first_row|scalar_sum_projection|in_projection|scalar_correlated)$|corpus_tpcds::30$'
metadata = {}
for phase in ['before', 'after']:
    out = base / ('prepare-lazy-legacy-declined-' + phase)
    out.mkdir()
    env = json.loads((base / 'prepare-lazy-legacy-copy/native-environment.json').read_text())
    binary = fixtures / phase / 'prepare_benchmark'
    env.update({'binary': str(binary), 'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(), 'filter': pattern, 'source_revision': phase + ' engine with identical declined-rewrite fixtures; lazy-legacy-copy/fixtures/builds.json records source', 'commands': [], 'exit_codes': [], 'pairing': 'Seven pairs; before first in odd-numbered pairs and after first in even-numbered pairs.'})
    metadata[phase] = (out, env)
for repeat in range(1, 8):
    for phase in (['before', 'after'] if repeat % 2 else ['after', 'before']):
        out, env = metadata[phase]
        command = ['taskset', '-c', str(env['cpu']), env['binary'], '--color', 'never', '--bench', '--sample-count', '10', '--sample-size', '1', '--timer', 'os', pattern]
        print('native pair', repeat, phase, flush=True)
        with (out / f'native-{repeat}.txt').open('w') as stream:
            result = subprocess.run(command, cwd=root, stdout=stream, stderr=subprocess.STDOUT)
        env['commands'].append(command)
        env['exit_codes'].append(result.returncode)
        (out / 'native-environment.json').write_text(json.dumps(env, indent=2) + '\n')
        assert result.returncode == 0, (phase, repeat)
for phase in ['before', 'after']:
    out, env = metadata[phase]
    command = ['python3', str(root / 'perf/logical-plan/measure.py'), env['binary'], str(out), '--phase', 'callgrind', '--cpu', str(env['cpu']), '--filter', pattern, '--source-revision', env['source_revision']]
    subprocess.run(command, cwd=root, check=True)
spec = importlib.util.spec_from_file_location('summary', root / 'perf/logical-plan/summarize.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
compared = {phase: module.summarize(data[0]) for phase, data in metadata.items()}
assert compared['before'].keys() == compared['after'].keys()
rows = []
for name, before in compared['before'].items():
    after = compared['after'][name]
    rows.append({'workload': name, 'before_max_instructions': max(before['instructions']), 'after_max_instructions': max(after['instructions']), 'instructions_delta_percent': 100*(after['instructions_median']/before['instructions_median']-1), 'before_median_ns': before['native_median_ns'], 'after_median_ns': after['native_median_ns'], 'before_uncertainty_ns': before['native_uncertainty_ns'], 'native_failed': after['native_median_ns']-before['native_median_ns'] > before['native_uncertainty_ns']})
result = {'comparisons': rows, 'instruction_rule': 'For these before/after measurements, compare maximum instruction counts across three rounds.', 'historical_limits': 'The fixed original-engine limits and the formal twenty-nine-workload outcome remain unchanged. These interleaved timing checks are diagnostic.', 'new_fixtures_without_original_engine_measurements': ['subquery_scalar_first_row', 'subquery_scalar_sum_projection', 'subquery_in_projection']}
(metadata['after'][0] / 'comparison.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result, indent=2), flush=True)
