import importlib.util
import json
from pathlib import Path


def main():
    root = Path('/workspace')
    base = root / 'perf/logical-plan/results'
    spec = importlib.util.spec_from_file_location('summary', root / 'perf/logical-plan/summarize.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    names = ['prepare-original-mark-result-fixtures', 'prepare-mark-projections', 'execution-original-mark-results', 'execution-mark-projections']
    data = {name: module.summarize(base / name) for name in names}
    assert len(data[names[0]]) == 3
    assert len(data[names[1]]) == 38
    assert len(data[names[2]]) == 3
    assert len(data[names[3]]) == 9
    sources = ['baseline', 'prepare-original-with-scalar-fixtures', 'prepare-original-scalar-parent-fixtures', 'prepare-original-membership-fixtures', 'prepare-original-outer-projection-fixtures', 'prepare-original-joined-projection-fixtures', names[0]]
    originals = {name: json.loads((base / name / 'summary.json').read_text()) for name in sources}
    before = json.loads((base / 'prepare-membership-comparison-copies/summary.json').read_text())
    assert len(before) == 35
    assert before.keys() <= data[names[1]].keys()
    rows = []
    for name, candidate in data[names[1]].items():
        source, original = next((source, workloads[name]) for source, workloads in originals.items() if name in workloads)
        rows.append(compare(name, candidate, source, name, original))
        if name in before:
            rows.append(compare(name, candidate, 'prepare-membership-comparison-copies', name, before[name]))
    (base / names[1] / 'comparison.json').write_text(json.dumps(rows, indent=2) + '\n')
    prepare = {'candidate_workloads': 38, 'existing_workloads_compared_with_previous_candidate': 35, 'fixed_original_instruction_failures': [row for row in rows if row['reference'] != 'prepare-membership-comparison-copies' and row['instructions_failed']], 'fixed_original_native_failures': [row for row in rows if row['reference'] != 'prepare-membership-comparison-copies' and row['native_failed']], 'previous_candidate_native_failures': [row for row in rows if row['reference'] == 'prepare-membership-comparison-copies' and row['native_failed']], 'limits': 'Historical per-workload limits remain unchanged.'}
    (base / names[1] / 'outcome.json').write_text(json.dumps(prepare, indent=2) + '\n')
    rows = []
    for name, candidate in data[names[3]].items():
        _, case = name.split('/', 1)
        original_name = 'automatic/' + case
        rows.append(compare(name, candidate, names[2], original_name, data[names[2]][original_name]))
    (base / names[3] / 'comparison.json').write_text(json.dumps(rows, indent=2) + '\n')
    execution = {'candidate_workloads': 9, 'original_workloads': 3, 'automatic_failures_against_original': [row for row in rows if row['workload'].startswith('automatic/') and (row['instructions_failed'] or row['native_failed'])], 'forced_alternatives_slower_than_original': [row for row in rows if row['workload'].startswith('forced/') and (row['instructions_failed'] or row['native_failed'])], 'original_modes_have_identical_plans': True, 'scope': 'The sibling EXISTS filter can decorrelate; the mark result still uses dependent execution. Automatic and forced costs are reported separately.'}
    (base / names[3] / 'outcome.json').write_text(json.dumps(execution, indent=2) + '\n')
    print(json.dumps({'prepare': {key: len(value) if isinstance(value, list) else value for key, value in prepare.items()}, 'execution': execution}, indent=2))


def compare(name, candidate, source, original_name, original):
    return {'workload': name, 'reference': source, 'reference_workload': original_name, 'original_max_instructions': max(original['instructions']), 'candidate_max_instructions': max(candidate['instructions']), 'instructions_delta_percent': 100 * (candidate['instructions_median'] / original['instructions_median'] - 1), 'instructions_failed': max(candidate['instructions']) > max(original['instructions']), 'original_median_ns': original['native_median_ns'], 'candidate_median_ns': candidate['native_median_ns'], 'original_uncertainty_ns': original['native_uncertainty_ns'], 'native_failed': candidate['native_median_ns'] - original['native_median_ns'] > original['native_uncertainty_ns']}


main()
