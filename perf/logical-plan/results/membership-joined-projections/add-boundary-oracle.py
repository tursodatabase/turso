from pathlib import Path
path=Path('/workspace/testing/differential-oracle/fuzzer/oracle.rs')
s=path.read_text()
marker='            (\n                "duplicate filters before EXISTS",'
assert s.count(marker)==1
new='''            (
                "IN outer projection over a materialized input",
                "WITH c AS MATERIALIZED (SELECT key1,amount FROM inner_rows)
                 SELECT o.id FROM outer_rows o WHERE o.amount+o.key1 IN (
                    SELECT c.amount+o.key1 FROM c WHERE c.key1=o.key1
                 ) ORDER BY o.id",
            ),
            (
                "NOT IN outer projection over a materialized input",
                "WITH c AS MATERIALIZED (SELECT key1,amount FROM inner_rows)
                 SELECT o.id FROM outer_rows o WHERE o.amount+o.key1 NOT IN (
                    SELECT c.amount+o.key1 FROM c WHERE c.key1=o.key1
                 ) ORDER BY o.id",
            ),
            (
                "IN outer projection over derived VALUES",
                "SELECT o.id FROM outer_rows o WHERE o.amount+o.key1 IN (
                    SELECT i.column2+o.key1
                    FROM (VALUES (1,10),(1,10),(1,NULL),(2,20),(NULL,30)) AS i
                    WHERE i.column1=o.key1
                 ) ORDER BY o.id",
            ),
            (
                "NOT IN outer projection over derived VALUES",
                "SELECT o.id FROM outer_rows o WHERE o.amount+o.key1 NOT IN (
                    SELECT i.column2+o.key1
                    FROM (VALUES (1,10),(1,10),(1,NULL),(2,20),(NULL,30)) AS i
                    WHERE i.column1=o.key1
                 ) ORDER BY o.id",
            ),
'''
path.write_text(s.replace(marker,new+marker))
