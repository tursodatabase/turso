import collections
import hashlib
import json
from pathlib import Path
import re
import shutil

root=Path('/workspace')
scratch=root/'target/logical-plan-commits/membership-joined-projections'
results=root/'perf/logical-plan/results'
out=results/'membership-joined-projections'
out.mkdir(exist_ok=True)
for name in ['before.json','source.json','regression.json','first-validation.json','final-tests.json','validation.json','builds.json','restored.json','isolation.json','filters.json','measurements.json','sqlite-expectations.json','test-before.txt','test-after.txt','joined-test.txt','optimizer.txt','relational.txt','forced.txt','integration.txt','sql.txt','clippy.txt','fmt.txt','declined-json.txt','final-fmt.txt','validate.py','build-benchmarks.py','build-original.py','measure.py','compare-prepare.py','compare-execution.py','record.py','add-sql-tests.py','boundary-validation.json','boundary-sqlite-expectations.json','boundary-fmt.txt','boundary-forced.txt','boundary-sql.txt','boundary-clippy.txt','add-boundary-tests.py','add-boundary-oracle.py','validate-boundaries.py','compare-native.py','native-execution.json','overlap.json']:
    shutil.copy2(scratch/name,out/name)
comparison=json.loads((scratch/'execution/comparison.json').read_text())
assert comparison['cases']==114
assert comparison['existing_cases_with_changed_plans']==[]
comparison['existing_plan_reference']='perf/logical-plan/results/subquery-cache-copies/execution/after/plans'
comparison['plan_sha256']={}
(out/'execution').mkdir(exist_ok=True)
for phase in ['before','after']:
    destination=out/'execution'/phase
    (destination/'plans').mkdir(parents=True,exist_ok=True)
    for name in ['run.json','run.txt']:
        shutil.copy2(scratch/'execution'/phase/name,destination/name)
    for path in sorted((scratch/'execution'/phase/'plans').glob('*.json')):
        digest=hashlib.sha256(path.read_bytes()).hexdigest()
        comparison['plan_sha256'].setdefault(path.name,{})[phase]=digest
        if 'joined_projection' in path.name:
            shutil.copy2(path,destination/'plans'/path.name)
        else:
            prior=results/'subquery-cache-copies/execution/after/plans'/path.name
            assert path.read_bytes()==prior.read_bytes(),path.name
(out/'execution/comparison.json').write_text(json.dumps(comparison,indent=2)+'\n')
fuzz=scratch/'seed-57291020'
text=re.sub(r'\x1b\[[0-9;]*m','',(fuzz/'run.txt').read_text())
counts={match[1].strip():int(match[2]) for match in re.finditer(r'^\| ([^|]+)\| (\d+)\s*\|$',text,re.M)}
assert counts['Statements Executed']==1000
assert all(counts[name]==0 for name in ['Statements Skipped','Warnings','Oracle Failures','Errors'])
traces=collections.Counter(re.findall(r'applied logical rule rule="([^"]+)"',text))
history=root/'perf/logical-plan/results/membership-projection-generator/seed-57291020-final/statements.sql'
assert history.read_bytes()==(fuzz/'simulator-output/test.sql').read_bytes()
(out/'seed-57291020').mkdir(exist_ok=True)
for source,target in [('run.json','run.json'),('run.txt','run.txt'),('simulator-output/schema.json','schema.json'),('simulator-output/coverage.txt','coverage.txt')]:
    shutil.copy2(fuzz/source,out/'seed-57291020'/target)
prepare_path=results/'prepare-joined-projections/outcome.json'
execution_path=results/'execution-joined-projections/outcome.json'
prepare=json.loads(prepare_path.read_text()) if prepare_path.exists() else None
execution=json.loads(execution_path.read_text()) if execution_path.exists() else None
outcome={'statements':counts,'rule_trace_events':dict(sorted(traces.items())),'trace_scope':'Events include inspection and repeated preparation; they are not unique query counts.','statement_history':str(history.relative_to(root)),'statement_history_sha256':hashlib.sha256(history.read_bytes()).hexdigest(),'history_matches_preceding_run':True,'execution_fixture_cases':114,'changed_execution_plans':comparison['changed_plans'],'existing_cases_with_changed_plans':comparison['existing_cases_with_changed_plans'],'prepare_comparison':'../prepare-joined-projections/outcome.json','execution_comparison':'../execution-joined-projections/outcome.json','fixed_original_prepare_instruction_failures':len(prepare['fixed_original_instruction_failures']) if prepare else None,'fixed_original_prepare_native_failures':len(prepare['fixed_original_native_failures']) if prepare else None,'automatic_execution_failures_against_original':execution['automatic_failures_against_original'] if execution else None,'remaining_work':'Resolve the recorded preparation and selected-execution regressions. General unnesting, broader integration and the full final corpus comparison remain unfinished.'}
outcome['prepare_instruction_comparison_status']='complete' if prepare else 'pending'
outcome['execution_instruction_comparison_status']='complete' if execution else 'pending'
outcome['native_execution_comparison']='native-execution.json'
outcome['validation_counts']={'optimizer_and_relational':64,'query_processing_integration':487,'sql':1484,'new_sql_cases':16,'new_forced_disabled_cases':13}
(out/'outcome.json').write_text(json.dumps(outcome,indent=2)+'\n')
print(json.dumps(outcome,indent=2))
