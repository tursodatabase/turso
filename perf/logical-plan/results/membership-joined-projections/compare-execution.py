from pathlib import Path
import importlib.util
import json

root=Path('/workspace')
base=root/'perf/logical-plan/results'
scratch=root/'target/logical-plan-commits/membership-joined-projections'
spec=importlib.util.spec_from_file_location('summary',root/'perf/logical-plan/summarize.py')
module=importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
names={'original':'execution-original-joined-projections','before':'execution-joined-projections-before','after':'execution-joined-projections'}
data={phase:module.summarize(base/name) for phase,name in names.items()}
assert len(data['original'])==4
assert len(data['after'])==12
same_plans=json.loads((scratch/'execution/comparison.json').read_text())['before_new_cases_have_the_same_plan_in_every_mode']
rows=[]
for name,after in data['after'].items():
    mode,case=name.split('/',1)
    original=data['original']['automatic/'+case]
    before_name=name if name in data['before'] else 'automatic/'+case
    if before_name!=name:
        assert same_plans[case],case
    before=data['before'][before_name]
    for reference,old in [('original',original),('before',before)]:
        rows.append({'workload':name,'reference':names[reference],'reference_workload':'automatic/'+case if reference=='original' else before_name,'original_max_instructions':max(old['instructions']),'candidate_max_instructions':max(after['instructions']),'instructions_delta_percent':100*(after['instructions_median']/old['instructions_median']-1),'instructions_failed':max(after['instructions'])>max(old['instructions']),'original_median_ns':old['native_median_ns'],'candidate_median_ns':after['native_median_ns'],'original_uncertainty_ns':old['native_uncertainty_ns'],'native_failed':after['native_median_ns']-old['native_median_ns']>old['native_uncertainty_ns']})
output=base/names['after']
(output/'comparison.json').write_text(json.dumps(rows,indent=2)+'\n')
outcome={'candidate_workloads':len(data['after']),'original_automatic_workloads':len(data['original']),'before_workloads':len(data['before']),'before_modes_with_identical_plans':same_plans,'automatic_failures_against_original':[row for row in rows if row['reference']==names['original'] and row['workload'].startswith('automatic/') and (row['instructions_failed'] or row['native_failed'])],'automatic_failures_against_before':[row for row in rows if row['reference']==names['before'] and row['workload'].startswith('automatic/') and (row['instructions_failed'] or row['native_failed'])],'forced_alternatives_slower_than_original':[row for row in rows if row['reference']==names['original'] and row['workload'].startswith('forced/') and (row['instructions_failed'] or row['native_failed'])],'selection_note':'Compare automatic selection with the original automatic plan. Forced modes show the cost of an available alternative, including when indexed correlated execution is cheaper. Per-workload results and all fixed limits are retained.'}
(output/'outcome.json').write_text(json.dumps(outcome,indent=2)+'\n')
print(json.dumps(outcome,indent=2))
