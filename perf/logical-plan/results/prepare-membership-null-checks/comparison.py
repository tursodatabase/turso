from pathlib import Path
import importlib.util
import json

root=Path('/workspace')
base=root/'perf/logical-plan/results'
spec=importlib.util.spec_from_file_location('summarize',root/'perf/logical-plan/summarize.py')
module=importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
candidate=module.summarize(base/'prepare-membership-null-checks')
membership_original=json.loads((base/'prepare-original-membership-fixtures/summary.json').read_text())
original_parent=json.loads((base/'prepare-original-scalar-parent-fixtures/summary.json').read_text())
historical=json.loads((base/'baseline/summary.json').read_text())
newer=json.loads((base/'prepare-original-with-scalar-fixtures/summary.json').read_text())
preceding=json.loads((base/'prepare-correlated-membership/summary.json').read_text())
assert preceding.keys() == candidate.keys()

def compare(name,before,after,source):
    return {
        'workload':name,
        'baseline_source':source,
        'original_max_instructions':max(before['instructions']),
        'candidate_max_instructions':max(after['instructions']),
        'instructions_delta_percent':100*(after['instructions_median']/before['instructions_median']-1),
        'instructions_failed':max(after['instructions'])>max(before['instructions']),
        'original_median_ns':before['native_median_ns'],
        'candidate_median_ns':after['native_median_ns'],
        'original_uncertainty_ns':before['native_uncertainty_ns'],
        'native_failed':after['native_median_ns']-before['native_median_ns']>before['native_uncertainty_ns'],
    }
fixed=[]
previous=[]
for name,after in candidate.items():
    if name in historical:
        before=historical[name]
        source='baseline'
    elif name in newer:
        before=newer[name]
        source='prepare-original-with-scalar-fixtures'
    elif name in membership_original:
        before=membership_original[name]
        source='prepare-original-membership-fixtures'
    else:
        before=original_parent[name]
        source='prepare-original-scalar-parent-fixtures'
    fixed.append(compare(name,before,after,source))
    if name in preceding:
        previous.append(compare(name,preceding[name],after,'prepare-correlated-membership'))
out=base/'prepare-membership-null-checks'
for name,data in [('comparison-fixed-original.json',fixed),('comparison-projected-membership.json',previous)]:
    (out/name).write_text(json.dumps(data,indent=2)+'\n')
result={
    'workloads':len(candidate),
    'fixed_original_instruction_failures':[row['workload'] for row in fixed if row['instructions_failed']],
    'fixed_original_native_failures':[row['workload'] for row in fixed if row['native_failed']],
    'preceding_candidate_native_failures':[row['workload'] for row in previous if row['native_failed']],
    'original_measurements_missing':[],
    'remaining_work':'Resolve the prepare and execution regressions and complete the full corpus comparison. Historical limits are unchanged.',
    'validation':'../membership-null-checks/validation.json',
}
(out/'outcome.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result,indent=2))
for row in fixed:
    print(row['workload'],row['candidate_max_instructions'],round(row['instructions_delta_percent'],2),row['candidate_median_ns'])
