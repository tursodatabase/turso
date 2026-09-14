import importlib.util
import json
from pathlib import Path
import statistics

root = Path('/workspace')
results = root / 'perf/logical-plan/results'
scratch = root / 'target/logical-plan-commits/membership-joined-projections'
spec = importlib.util.spec_from_file_location('summary', root / 'perf/logical-plan/summarize.py')
summary = importlib.util.module_from_spec(spec)
spec.loader.exec_module(summary)
data = {}
for name in ['execution-original-joined-projections', 'execution-joined-projections-before', 'execution-joined-projections']:
    runs = [summary.native_run(results / name / f'native-{repeat}.txt') for repeat in range(1, 8)]
    assert all(run.keys() == runs[0].keys() for run in runs)
    data[name] = {}
    for workload in runs[0]:
        samples = [run[workload] for run in runs]
        median = statistics.median(samples)
        mad = statistics.median(abs(value - median) for value in samples)
        data[name][workload] = {'native_ns': samples, 'median_ns': median, 'uncertainty_ns': max(max(samples) - min(samples), 3 * mad)}
rows = []
for name, after in data['execution-joined-projections'].items():
    original_name = 'automatic/' + name.split('/', 1)[1]
    before = data['execution-joined-projections-before'][original_name]
    original = data['execution-original-joined-projections'][original_name]
    rows.append({'workload': name, 'original': original, 'before': before, 'after': after, 'original_native_failed': after['median_ns'] - original['median_ns'] > original['uncertainty_ns'], 'before_native_failed': after['median_ns'] - before['median_ns'] > before['uncertainty_ns']})
(scratch / 'native-execution.json').write_text(json.dumps(rows, indent=2) + '\n')
for row in rows:
    print(row['workload'], {name: round(row[name]['median_ns'] / 1e6, 3) for name in ['original', 'before', 'after']}, 'original_failed', row['original_native_failed'], 'before_failed', row['before_native_failed'])
