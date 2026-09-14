from pathlib import Path
import importlib.util
import json
import statistics

root = Path(__file__).resolve().parents[4]
base = root / 'perf/logical-plan/results'
spec = importlib.util.spec_from_file_location('summary', root / 'perf/logical-plan/summarize.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
fixed = json.loads((base / 'prepare-membership-null-checks/summary.json').read_text())
runs = {
    variant: [module.native_run(base / directory / f'native-{n}.txt') for n in range(1, 8)]
    for variant, directory in [
        ('before', 'prepare-membership-null-checks-native-repeat'),
        ('after', 'prepare-membership-result-cost-native-repeat'),
    ]
}
for variant in runs:
    assert all(run.keys() == fixed.keys() for run in runs[variant])
rows = []
for name, before_fixed in fixed.items():
    before = [run[name] for run in runs['before']]
    after = [run[name] for run in runs['after']]
    row = {
        'workload': name,
        'before_ns': before,
        'after_ns': after,
        'before_median_ns': statistics.median(before),
        'after_median_ns': statistics.median(after),
        'frozen_uncertainty_ns': before_fixed['native_uncertainty_ns'],
    }
    row['delta_ns'] = row['after_median_ns'] - row['before_median_ns']
    row['native_failed'] = row['delta_ns'] > row['frozen_uncertainty_ns']
    row['paired_delta_ns'] = [a - b for a, b in zip(after, before)]
    rows.append(row)
result = {
    'diagnostic_only': True,
    'formal_measurements_unchanged': True,
    'uncertainty_source': 'prepare-membership-null-checks/summary.json',
    'rounds_per_variant': 7,
    'workloads': len(rows),
    'native_failures': [row['workload'] for row in rows if row['native_failed']],
    'comparisons': rows,
}
(Path(__file__).parent / 'native-repeat-comparison.json').write_text(json.dumps(result, indent=2) + '\n')
