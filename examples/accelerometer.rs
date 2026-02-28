use std::fs::File;
use tempfile::tempdir;
use arrow::util::pretty::print_batches;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use fletch::{fletch_schema, FletchType};

fletch_schema! {
    AccelerometerTelemetry {
        accel_x: f64,
        accel_y: f64,
        accel_z: f64,
    }
}

fletch_schema! {
    PowerSupplyTelemetry {
        voltage: f64,
        current_consumption: f64,
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let uri = format!("file:///{}", dir.path().to_string_lossy().replace("\\", "/"));
    let run_id = "run_001";

    println!("Initializing Zero-Config HIL Logging to: {}", uri);

    // ==========================================
    // 1. ASYNC SETUP: Initialize Iceberg Schemas
    // ==========================================
    let mut accel_stream = AccelerometerTelemetry::try_new(&uri, run_id).await?;
    let mut pwr_stream = PowerSupplyTelemetry::try_new(&uri, run_id).await?;

    // ==========================================
    // 2. SYNCHRONOUS LOGGING (The Fast Path)
    // ==========================================
    println!("Generating 100,000 samples at mixed rates...");
    let start_ts: i64 = 1_718_000_000_000;
    let num_samples = 100_000;

    for i in 0..num_samples {
        let current_ts = start_ts + i;
        let t = i as f64 * 0.01;
        accel_stream.accel_x(current_ts, t.sin() * 2.0)?;
        accel_stream.accel_y(current_ts, t.cos() * 2.0)?;
        accel_stream.accel_z(current_ts, 9.81 + (t * 5.0).sin())?;

        if i % 10 == 0 {
            let voltage = 3.3 + (i % 100) as f64 * 0.001;
            let current = 1.2 + ((t.sin() * 2.0).abs() * 0.05);
            pwr_stream.voltage(current_ts, voltage)?;
            pwr_stream.current_consumption(current_ts, current)?;
        }
    }

    // ==========================================
    // 3. CLEAN SHUTDOWN
    // ==========================================
    accel_stream.close()?;
    pwr_stream.close()?;
    println!("Successfully wrote telemetry and committed Iceberg transactions.\n");

    // ==========================================
    // 4. PROOF OF DATA INTEGRITY (Parquet)
    // ==========================================
    println!("--- Parquet Data Inspection (Accelerometer) ---");
    let accel_file_path = dir.path().join(format!("AccelerometerTelemetry/data/{}.parquet", run_id));
    let accel_file = File::open(&accel_file_path)?;
    let accel_builder = ParquetRecordBatchReaderBuilder::try_new(accel_file)?;
    let mut accel_reader = accel_builder.build()?;

    if let Some(record_batch_result) = accel_reader.next() {
        let batch = record_batch_result?;
        let top_10 = batch.slice(0, 10.min(batch.num_rows())); // Safely grab up to 10
        print_batches(&[top_10])?;
    }

    println!("\n--- Parquet Data Inspection (Power Supply) ---");
    let pwr_file_path = dir.path().join(format!("PowerSupplyTelemetry/data/{}.parquet", run_id));
    let pwr_file = File::open(&pwr_file_path)?;
    let pwr_builder = ParquetRecordBatchReaderBuilder::try_new(pwr_file)?;
    let mut pwr_reader = pwr_builder.build()?;

    if let Some(record_batch_result) = pwr_reader.next() {
        let batch = record_batch_result?;
        let top_10 = batch.slice(0, 10.min(batch.num_rows()));
        print_batches(&[top_10])?;
    }

    // ==========================================
    // 5. PROOF OF ICEBERG METADATA (Catalog)
    // ==========================================
    println!("\n--- Iceberg Metadata Inspection ---");
    use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent}; // FIXED: Added CatalogBuilder
    use iceberg_catalog_sql::{SqlCatalogBuilder, SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE};
    use std::collections::HashMap;
    let os_path = if cfg!(windows) {
        uri.strip_prefix("file:///").unwrap().to_string()
    } else {
        format!("/{}", uri.strip_prefix("file:///").unwrap())
    };
    let catalog_url = format!("sqlite:{}", std::path::Path::new(&os_path).join("iceberg_catalog.db").to_string_lossy());

    let catalog = SqlCatalogBuilder::default()
        .load(
            "local_hil_inspection",
            HashMap::from([
                (SQL_CATALOG_PROP_URI.to_string(), catalog_url),
                (SQL_CATALOG_PROP_WAREHOUSE.to_string(), uri.clone()),
            ])
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to connect to catalog for inspection: {}", e))?;

    let namespace = NamespaceIdent::from_strs(vec!["hil_telemetry"]).map_err(|e| anyhow::anyhow!("{}", e))?;
    for table_name in &["AccelerometerTelemetry", "PowerSupplyTelemetry"] {
        let table_ident = TableIdent::new(namespace.clone(), table_name.to_string());
        let table = catalog.load_table(&table_ident).await.map_err(|e| anyhow::anyhow!("{}", e))?;

        println!("\n>>> Table: {}", table_name);
        if let Some(snapshot) = table.metadata().current_snapshot() {
            println!("  Snapshot ID: {}", snapshot.snapshot_id());
            println!("  Manifest List: {}", snapshot.manifest_list());
            println!("  Commit Summary:");
            for (key, value) in &snapshot.summary().additional_properties {
                println!("    {}: {}", key, value);
            }
        } else {
            println!("  No snapshots found. Commit may have failed.");
        }
    }

    Ok(())
}