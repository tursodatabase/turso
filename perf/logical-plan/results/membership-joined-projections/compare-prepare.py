from pathlib import Path
import importlib.util
import json

root=Path('/workspace')
base=root/'perf/logical-plan/results'
spec=importlib.util.spec_from_file_location('summary',root/'perf/logical-plan/summarize.py')
module=importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
for name in ['prepare-original-joined-projection-fixtures','prepare-joined-projections-before','prepare-joined-projections']:
    module.summarize(base/name)
originals={name:json.loads((base/name/'summary.json').read_text()) for name in ['baseline','prepare-original-with-scalar-fixtures','prepare-original-scalar-parent-fixtures','prepare-original-membership-fixtures','prepare-original-outer-projection-fixtures','prepare-original-joined-projection-fixtures']}
before=json.loads((base/'prepare-joined-projections-before/summary.json').read_text())
after=json.loads((base/'prepare-joined-projections/summary.json').read_text())
assert before.keys()==after.keys()
assert len(after)==35,len(after)
fixed=[]
previous=[]
for name,new in after.items():
    origin,old=next((source,data[name]) for source,data in originals.items() if name in data)
    for rows,old,source in [(fixed,old,origin),(previous,before[name],'prepare-joined-projections-before')]:
        rows.append({'workload':name,'baseline_source':source,'original_max_instructions':max(old['instructions']),'candidate_max_instructions':max(new['instructions']),'instructions_delta_percent':100*(new['instructions_median']/old['instructions_median']-1),'instructions_failed':max(new['instructions'])>max(old['instructions']),'original_median_ns':old['native_median_ns'],'candidate_median_ns':new['native_median_ns'],'original_uncertainty_ns':old['native_uncertainty_ns'],'native_failed':new['native_median_ns']-old['native_median_ns']>old['native_uncertainty_ns']})
out=base/'prepare-joined-projections'
for filename,rows in [('comparison-fixed-original.json',fixed),('comparison-before-joined-projections.json',previous)]:
    (out/filename).write_text(json.dumps(rows,indent=2)+'\n')
outcome={'workloads':len(after),'fixed_original_instruction_failures':[row['workload'] for row in fixed if row['instructions_failed']],'fixed_original_native_failures':[row['workload'] for row in fixed if row['native_failed']],'preceding_candidate_native_failures':[row['workload'] for row in previous if row['native_failed']],'original_measurements_missing':[],'validation':'../membership-joined-projections/validation.json','remaining_work':'Resolve the recorded prepare failures, complete general unnesting and integration, and compare the full final corpus. Historical limits remain unchanged.'}
(out/'outcome.json').write_text(json.dumps(outcome,indent=2)+'\n')
print(json.dumps(outcome,indent=2))
