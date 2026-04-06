use std::sync::Arc;
use turso::Builder;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let runtime = turso_wasm_wasmtime::WasmtimeRuntime::new()?;

    let db = Builder::new_local(":memory:")
        .with_unstable_wasm_runtime(Arc::new(runtime))
        .build()
        .await?;

    let conn = db.connect()?;

    // Register the WASM UDF
    conn.execute("CREATE FUNCTION add2 LANGUAGE wasm AS X'0061736d01000000010c0260017f017f60027f7f017e030302000105030100020607017f014180080b071f03066d656d6f727902000c747572736f5f6d616c6c6f6300000361646400010a24021101017f23002101230020006a240020010b10002001290300200141086a2903007c0b002c046e616d65021c020002000473697a6501037074720102000461726763010461726776070701000462756d70' EXPORT 'add'", ()).await?;

    // Basic calls
    let row = conn
        .query("SELECT add2(40, 2) AS r", ())
        .await?
        .next()
        .await?
        .unwrap();
    println!("add2(40, 2)    = {:?}", row.get_value(0)?);

    let row = conn
        .query("SELECT add2(100, 200) AS r", ())
        .await?
        .next()
        .await?
        .unwrap();
    println!("add2(100, 200) = {:?}", row.get_value(0)?);

    let row = conn
        .query("SELECT add2(-10, 3) AS r", ())
        .await?
        .next()
        .await?
        .unwrap();
    println!("add2(-10, 3)   = {:?}", row.get_value(0)?);

    // Nested calls
    let row = conn
        .query("SELECT add2(add2(1,2), add2(3,4)) AS r", ())
        .await?
        .next()
        .await?
        .unwrap();
    println!("nested         = {:?}", row.get_value(0)?);

    // UDF on table data
    conn.execute("CREATE TABLE orders (item TEXT, price INT, tax INT)", ())
        .await?;
    conn.execute(
        "INSERT INTO orders VALUES ('Widget',1000,80),('Gadget',2500,200),('Doohickey',500,40)",
        (),
    )
    .await?;

    let mut rows = conn
        .query(
            "SELECT item, price, tax, add2(price, tax) AS total FROM orders ORDER BY total DESC",
            (),
        )
        .await?;
    println!("\nOrders:");
    while let Some(row) = rows.next().await? {
        println!(
            "  {:12} price={:>4?}  tax={:>3?}  total={:?}",
            format!("{:?}", row.get_value(0)?),
            row.get_value(1)?,
            row.get_value(2)?,
            row.get_value(3)?
        );
    }

    // Drop and verify
    conn.execute("DROP FUNCTION add2", ()).await?;
    match conn.prepare("SELECT add2(1,2)").await {
        Err(e) => println!("\nAfter DROP: {e}"),
        Ok(_) => println!("\nAfter DROP: unexpectedly succeeded"),
    }

    Ok(())
}
