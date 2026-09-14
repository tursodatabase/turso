from pathlib import Path
import importlib.util
import json

root = Path(__file__).resolve().parents[4]
base = root / 'perf/logical-plan/results'
out = Path(__file__).parent
spec = importlib.util.spec_from_file_location('summary', root / 'perf/logical-plan/summarize.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
summary = module.summarize(out)
cases = sorted(name.removeprefix('automatic/') for name in summary if name.startswith('automatic/'))
assert len(cases) == 8
comparisons = []
for case in cases:
    disabled = summary[f'disabled/{case}']
    plans = {
        mode: json.loads((out / 'plans' / f'{case}-{mode}.json').read_text())
        for mode in ['auto', 'forced', 'disabled']
    }
    assert any(node['op'].get('join') in ['semi', 'anti'] for node in plans['forced']['plan']['nodes']), case
    assert not any(node['op'].get('join') in ['semi', 'anti'] for node in plans['disabled']['plan']['nodes']), case
    assert len({plan['ordered_result_rows'] for plan in plans.values()}) == 1
    for mode in ['automatic', 'forced']:
        candidate = summary[f'{mode}/{case}']
        comparisons.append({
            'case': case,
            'mode': mode,
            'disabled_max_instructions': max(disabled['instructions']),
            'candidate_max_instructions': max(candidate['instructions']),
            'instructions_delta_percent': 100 * (candidate['instructions_median'] / disabled['instructions_median'] - 1),
            'disabled_median_ns': disabled['native_median_ns'],
            'candidate_median_ns': candidate['native_median_ns'],
            'native_delta_percent': 100 * (candidate['native_median_ns'] / disabled['native_median_ns'] - 1),
            'native_failed': candidate['native_median_ns'] - disabled['native_median_ns'] > disabled['native_uncertainty_ns'],
            'ordered_result_rows': plans['disabled']['ordered_result_rows'],
        })
(out / 'comparison-disabled.json').write_text(json.dumps(comparisons, indent=2) + '\n')
previous = json.loads((base / 'execution-membership-result-cost/summary.json').read_text())
original = json.loads((base / 'execution-original-membership/summary.json').read_text())
assert previous.keys() < summary.keys()
assert original.keys() == {f'automatic/{case}' for case in cases}
for source, data, filename in [
    ('execution-membership-result-cost', previous, 'comparison-before-outer-projection.json'),
    ('execution-original-membership', original, 'comparison-original.json'),
]:
    rows = []
    for name, after in summary.items():
        key = 'automatic/' + name.split('/', 1)[1] if source == 'execution-original-membership' else name
        if key not in data:
            continue
        before = data[key]
        rows.append({
            'workload': name,
            'baseline_source': source,
            'baseline_workload': key,
            'before_max_instructions': max(before['instructions']),
            'candidate_max_instructions': max(after['instructions']),
            'instructions_delta_percent': 100 * (after['instructions_median'] / before['instructions_median'] - 1),
            'instructions_increased': max(after['instructions']) > max(before['instructions']),
            'before_median_ns': before['native_median_ns'],
            'candidate_median_ns': after['native_median_ns'],
            'before_uncertainty_ns': before['native_uncertainty_ns'],
            'native_failed': after['native_median_ns'] - before['native_median_ns'] > before['native_uncertainty_ns'],
        })
    (out / filename).write_text(json.dumps(rows, indent=2) + '\n')
    print(json.dumps({
        'source': source,
        'workloads': len(rows),
        'native_failures': [row['workload'] for row in rows if row['native_failed']],
        'instruction_increases': [row['workload'] for row in rows if row['instructions_increased']],
    }, indent=2))
