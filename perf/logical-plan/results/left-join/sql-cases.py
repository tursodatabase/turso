import json
from pathlib import Path
import subprocess


root = Path('/workspace')
destination = root / 'sqlite/conformance/sqlite-sqltests/left-join-null-rejecting-terms.sqltest'
scratch = root / 'target/logical-plan-commits/left-join'
cases = [
    ('duplicates-and-collation', 'SELECT l.id,r.id,typeof(r.id),r.v FROM {l} l LEFT JOIN {r} r ON l.k=r.k {where} ORDER BY l.id,r.id'),
    ('where-keeps-unmatched', 'SELECT l.id,r.id FROM {l} l LEFT JOIN {r} r ON l.k=r.k {where} AND r.id IS NULL ORDER BY l.id'),
    ('left-only-on', 'SELECT l.id,r.id FROM {l} l LEFT JOIN {r} r ON l.id=2 {where} ORDER BY l.id,r.id'),
    ('null-on', 'SELECT l.id,r.id FROM {l} l LEFT JOIN {r} r ON NULL {where} ORDER BY l.id'),
    ('empty-right', 'DELETE FROM {r}; SELECT l.id,r.id FROM {l} l LEFT JOIN {r} r ON l.k=r.k {where} ORDER BY l.id'),
    ('using-columns', 'SELECT l.id,k,r.id FROM {l} l LEFT JOIN {r} r USING(k) {where} ORDER BY l.id,r.id'),
    ('nested-null-where', 'SELECT l.id,r.id FROM {l} l LEFT JOIN {r} r ON l.k=r.k {where} AND CASE WHEN r.v>5 THEN r.v ELSE 1 END>0 ORDER BY l.id,r.id'),
    ('two-left-joins', 'SELECT l.id,r.id,s.id FROM {l} l LEFT JOIN {r} r ON l.k=r.k LEFT JOIN {r} s ON s.id=r.id+1 {where} ORDER BY l.id,r.id,s.id'),
    ('limited-right', 'SELECT l.id,r.id FROM {l} l LEFT JOIN (SELECT id,k FROM {r} ORDER BY id DESC LIMIT 2) r ON l.k=r.k {where} ORDER BY l.id,r.id'),
    ('grouped-results', 'SELECT l.id,count(r.id),sum(r.v) FROM {l} l LEFT JOIN {r} r ON l.k=r.k {where} GROUP BY l.id ORDER BY l.id'),
]
records = []
text = destination.read_text()
for index, (name, template) in enumerate(cases):
    left = f'logical_left_{index}'
    right = f'logical_right_{index}'
    setup = f'''CREATE TABLE {left}(id INTEGER PRIMARY KEY,k TEXT COLLATE NOCASE);
    CREATE TABLE {right}(id INTEGER PRIMARY KEY,k TEXT NOT NULL,v INTEGER NOT NULL);
    INSERT INTO {left} VALUES (1,'A'),(2,'b'),(3,NULL),(4,'x'),(5,'A');
    INSERT INTO {right} VALUES (11,'a',5),(12,'A',6),(13,'b',7),(14,'z',8);'''
    where = f'WHERE l.id>0 AND l.id>0 AND EXISTS (SELECT 1 FROM {left} w WHERE w.id>=l.id)'
    sql = setup + '\n    ' + template.format(l=left, r=right, where=where) + ';'
    command = ['/tmp/sqlite-version-3.50.4/sqlite3', ':memory:']
    result = subprocess.run(command, input=sql, text=True, capture_output=True, check=True)
    records.append({'name': name, 'sql': sql, 'sqlite_output': result.stdout, 'command': command})
    expected = ''.join('    ' + line + '\n' for line in result.stdout.splitlines())
    text += '\ntest logical-left-join-' + name + ' {\n    ' + sql + '\n}\nexpect {\n' + expected + '}\n'
destination.write_text(text)
(scratch / 'sql-cases.json').write_text(json.dumps(records, indent=2) + '\n')
print(f'Added {len(records)} cases with SQLite results.')
