/**
 * Full WASM UDF example for Turso on Bun.
 *
 * Build:
 *   cd bindings/javascript
 *   yarn build
 *
 * Run:
 *   bun examples/wasm_udf.ts
 */

// In a published package this would be:
//   import { connect, createUnstableNativeWasmRuntime } from "@tursodatabase/database";
import { connect, createUnstableNativeWasmRuntime } from "../packages/native/dist/promise.js";

// ── WASM module (compiled from WAT) ─────────────────────────────────────────
// Exports: memory, turso_malloc (bump allocator), add(argc, argv) → a + b
const ADD_WASM_HEX =
  "0061736d01000000010c0260017f017f60027f7f017e030302000105030100020607017f014180080b" +
  "071f03066d656d6f727902000c747572736f5f6d616c6c6f6300000361646400010a24021101017f23" +
  "002101230020006a240020010b10002001290300200141086a2903007c0b002c046e616d65021c0200" +
  "02000473697a6501037074720102000461726763010461726776070701000462756d70";

const db = await connect(":memory:", { unstableWasmRuntime: createUnstableNativeWasmRuntime() });

// Register the WASM UDF
await db.exec(
  `CREATE FUNCTION add2 LANGUAGE wasm AS X'${ADD_WASM_HEX}' EXPORT 'add'`
);

// Basic calls
console.log("add2(40, 2)    =", (await db.prepare("SELECT add2(40, 2) AS r").get()).r);
console.log("add2(100, 200) =", (await db.prepare("SELECT add2(100, 200) AS r").get()).r);
console.log("add2(-10, 3)   =", (await db.prepare("SELECT add2(-10, 3) AS r").get()).r);

// Bound parameters
console.log("add2(?, ?)     =", (await db.prepare("SELECT add2(?, ?) AS r").get(17, 25)).r, " [17, 25]");

// Nested / expressions
console.log("nested         =", (await db.prepare("SELECT add2(add2(1,2), add2(3,4)) AS r").get()).r);
console.log("scaled         =", (await db.prepare("SELECT add2(10, 20) * 3 AS r").get()).r);

// UDF on table data
await db.exec("CREATE TABLE orders (item TEXT, price INT, tax INT)");
await db.exec("INSERT INTO orders VALUES ('Widget',1000,80),('Gadget',2500,200),('Doohickey',500,40)");

const rows = await db
  .prepare("SELECT item, price, tax, add2(price, tax) AS total FROM orders ORDER BY total DESC")
  .all();

console.log("\nOrders:");
for (const r of rows) {
  console.log(
    `  ${(r.item as string).padEnd(12)} price=${String(r.price).padStart(4)}  tax=${String(r.tax).padStart(3)}  total=${r.total}`
  );
}

// Lifecycle
await db.exec("DROP FUNCTION add2");
try {
  db.prepare("SELECT add2(1,2)");
} catch (e: any) {
  console.log("\nAfter DROP:", e.message);
}

await db.close();
