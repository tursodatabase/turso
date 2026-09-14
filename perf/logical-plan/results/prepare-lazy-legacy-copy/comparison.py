from pathlib import Path
import importlib.util
import json


def main():
    root = Path(__file__).resolve().parents[4]
    base = root / 'perf/logical-plan/results'
    out = Path(__file__).parent
    spec = importlib.util.spec_from_file_location('summary', root / 'perf/logical-plan/summarize.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    candidate = module.summarize(out)
    originals = {
        name: json.loads((base / name / 'summary.json').read_text())
        for name in [
            'baseline', 'prepare-original-with-scalar-fixtures',
            'prepare-original-scalar-parent-fixtures', 'prepare-original-membership-fixtures',
            'prepare-original-outer-projection-fixtures',
        ]
    }
    preceding = json.loads((base / 'prepare-subquery-cache-copies/summary.json').read_text())
    assert candidate.keys() == preceding.keys()

    fixed = []
    previous = []
    for name, after in candidate.items():
        source, original = next((source, data[name]) for source, data in originals.items() if name in data)
        fixed.append(compare(name, original, after, source))
        if name in preceding:
            previous.append(compare(name, preceding[name], after, 'prepare-subquery-cache-copies'))
    for name, rows in [('comparison-fixed-original.json', fixed), ('comparison-before-lazy-copy.json', previous)]:
        (out / name).write_text(json.dumps(rows, indent=2) + '\n')
    result = {
        'workloads': len(candidate),
        'fixed_original_instruction_failures': [row['workload'] for row in fixed if row['instructions_failed']],
        'fixed_original_native_failures': [row['workload'] for row in fixed if row['native_failed']],
        'preceding_candidate_native_failures': [row['workload'] for row in previous if row['native_failed']],
        'original_measurements_missing': [],
        'remaining_work': 'Resolve the prepare and execution regressions and complete the full corpus comparison. Historical limits are unchanged.',
        'validation': '../lazy-legacy-copy/validation.json',
    }
    (out / 'outcome.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))


def compare(name, before, after, source):
    return {
        'workload': name,
        'baseline_source': source,
        'original_max_instructions': max(before['instructions']),
        'candidate_max_instructions': max(after['instructions']),
        'instructions_delta_percent': 100 * (after['instructions_median'] / before['instructions_median'] - 1),
        'instructions_failed': max(after['instructions']) > max(before['instructions']),
        'original_median_ns': before['native_median_ns'],
        'candidate_median_ns': after['native_median_ns'],
        'original_uncertainty_ns': before['native_uncertainty_ns'],
        'native_failed': after['native_median_ns'] - before['native_median_ns'] > before['native_uncertainty_ns'],
    }


if __name__ == '__main__':
    main()
