"""WASM UDF example for Turso Python bindings."""

import turso
from turso import WasmtimeRuntime

ADD_WASM_HEX = (
    "0061736d01000000010c0260017f017f60027f7f017e030302000105030100020607017f014180080b"
    "071f03066d656d6f727902000c747572736f5f6d616c6c6f6300000361646400010a24021101017f23"
    "002101230020006a240020010b10002001290300200141086a2903007c0b002c046e616d65021c0200"
    "02000473697a6501037074720102000461726763010461726776070701000462756d70"
)

conn = turso.connect(":memory:", unstable_wasm_runtime=WasmtimeRuntime())

# Register the WASM UDF
conn.execute(f"CREATE FUNCTION add2 LANGUAGE wasm AS X'{ADD_WASM_HEX}' EXPORT 'add'")

# Basic calls
cur = conn.execute("SELECT add2(40, 2) AS r")
print(f"add2(40, 2)    = {cur.fetchone()[0]}")

cur = conn.execute("SELECT add2(100, 200) AS r")
print(f"add2(100, 200) = {cur.fetchone()[0]}")

cur = conn.execute("SELECT add2(-10, 3) AS r")
print(f"add2(-10, 3)   = {cur.fetchone()[0]}")

# Nested
cur = conn.execute("SELECT add2(add2(1,2), add2(3,4)) AS r")
print(f"nested         = {cur.fetchone()[0]}")

# UDF on table data
conn.execute("CREATE TABLE orders (item TEXT, price INT, tax INT)")
conn.execute("INSERT INTO orders VALUES ('Widget',1000,80),('Gadget',2500,200),('Doohickey',500,40)")

cur = conn.execute("SELECT item, price, tax, add2(price, tax) AS total FROM orders ORDER BY total DESC")
print("\nOrders:")
for r in cur.fetchall():
    print(f"  {r[0]:12} price={r[1]:>4}  tax={r[2]:>3}  total={r[3]}")

# Drop and verify
conn.execute("DROP FUNCTION add2")
try:
    conn.execute("SELECT add2(1,2)")
except Exception as e:
    print(f"\nAfter DROP: {e}")

conn.close()
