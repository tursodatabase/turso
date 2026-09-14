import json
from pathlib import Path
import re
import subprocess

path=Path('/workspace/sqlite/conformance/sqlite-sqltests/row-value-in.sqltest')
original=path.read_text()
setup=re.search(r'^setup logical_correlated_membership \{\n(.*?)^\}',original,re.M|re.S)[1]
cases=[]
for operator,label in [('IN','in'),('NOT IN','not-in')]:
    cases.append((label+'-outer-projection-materialized-input',f'WITH c AS MATERIALIZED (SELECT k,x FROM inner_rows WHERE enabled=1) SELECT o.id FROM outer_rows o WHERE o.a+1 {operator} (SELECT c.x+o.k FROM c WHERE c.k=o.k) ORDER BY o.id;'))
    cases.append((label+'-outer-projection-derived-values',f'SELECT o.id FROM outer_rows o WHERE o.a+1 {operator} (SELECT i.column2+o.k FROM (VALUES (1,10),(1,10),(1,NULL),(2,20),(NULL,30),(4,50)) AS i WHERE i.column1=o.k) ORDER BY o.id;'))
blocks=[]
records=[]
for name,query in cases:
    assert 'test '+name+' {' not in original
    result=subprocess.run(['/tmp/sqlite-version-3.50.4/sqlite3',':memory:'],input=setup+'\n'+query+'\n',text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,check=True)
    rows=result.stdout.strip('\n').splitlines()
    blocks.append('\n@setup logical_correlated_membership\ntest '+name+' {\n    '+query+'\n}\nexpect {\n'+''.join('    '+row+'\n' for row in rows)+'}\n')
    records.append({'name':name,'sql':query,'expected':rows})
path.write_text(original+''.join(blocks))
Path('/workspace/target/logical-plan-commits/membership-joined-projections/boundary-sqlite-expectations.json').write_text(json.dumps(records,indent=2)+'\n')
print(json.dumps(records,indent=2))
