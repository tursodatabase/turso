import importlib.util
import json
from pathlib import Path
import re
import shutil

root=Path('/workspace')
scratch=root/'target/logical-plan-commits/lazy-legacy-copy'
base=root/'perf/logical-plan/results'
out=base/'lazy-legacy-copy'
fixtures=out/'fixtures'
fixtures.mkdir(exist_ok=True)
for name in ['builds.json','restored.json','validation.json','isolation.json','fmt.txt','clippy.txt']:
    shutil.copy2(scratch/'fixtures'/name,fixtures/name)
for name in ['build-fixtures.py','measure-fixtures.py','record-fixtures.py','record.py']:
    shutil.copy2(scratch/name,out/name)
comparison=json.loads((base/'prepare-lazy-legacy-declined-after/comparison.json').read_text())
(fixtures/'comparison.json').write_text(json.dumps(comparison,indent=2)+'\n')
allocations={}
for phase in ['before','after']:
    rounds=[]
    for repeat in range(1,8):
        stack=[]
        reading=False
        values={}
        for line in (base/f'prepare-lazy-legacy-declined-{phase}/native-{repeat}.txt').read_text().splitlines():
            match=re.match(r'^([│ ]*)[├╰]─ (.*)$',line)
            if match:
                depth=len(match[1])//3
                name=re.split(r'\s{2,}',match[2].split('│')[0].strip())[0]
                stack=stack[:depth]+[name]
                reading=False
            elif re.search(r'\balloc:',line) and 'max alloc:' not in line:
                reading=True
            elif reading:
                match=re.search(r'(\d+)\s*│',line)
                assert match,line
                name='/'.join(stack)
                assert name not in values,name
                values[name]=int(match[1])
                reading=False
        assert len(values)==5,values
        rounds.append(values)
    for name in rounds[0]:
        allocations.setdefault(name,{})[phase]=[run[name] for run in rounds]
(fixtures/'allocation-counts.json').write_text(json.dumps({'field':'alloc','workloads':allocations},indent=2)+'\n')
print(json.dumps(allocations,indent=2))
