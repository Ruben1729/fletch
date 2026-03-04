use std::fs::File;
use tempfile::tempdir;
use arrow::util::pretty::print_batches;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use fletch::{fletch_schema, FletchType, FletchWorkspace, FletchViewBuilder};

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
    let workspace = FletchWorkspace::builder()
        .uri(&uri)
        .namespace(&["some_project", "some_test"])
        .build()?;
    let mut accel_stream = AccelerometerTelemetry::try_new(&workspace, run_id).await?;
    let mut pwr_stream = PowerSupplyTelemetry::try_new(&workspace, run_id).await?;

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
    // 4. DATA EXPLORATION (The View Builder)
    // ==========================================
    println!("Building Analytical Views using Polars...\n");
    let view_accel = FletchViewBuilder::new(&workspace)
        .run_id(run_id)
        .add_source("AccelerometerTelemetry", &["accel_x", "accel_z"])
        .build()
        .await?;

    let df_accel = view_accel.collect()?;
    println!("--- View 1: Accelerometer (X, Z only) ---");
    println!("{}", df_accel.head(Some(10)));
    println!("\n");

    let view_pwr = FletchViewBuilder::new(&workspace)
        .run_id(run_id)
        .add_source("PowerSupplyTelemetry", &["voltage", "current_consumption"])
        .build()
        .await?;

    let df_pwr = view_pwr.collect()?;
    println!("--- View 2: Power Supply ---");
    println!("{}", df_pwr.head(Some(10)));
    println!("\n");

    let view_fusion = FletchViewBuilder::new(&workspace)
        .run_id(run_id)
        .add_source("AccelerometerTelemetry", &["accel_z"])
        .add_source("PowerSupplyTelemetry", &["voltage"])
        .build()
        .await?;

    let df_fusion = view_fusion.collect()?;
    println!("--- View 3: Sensor Fusion (Accel Z + Voltage) ---");
    println!("{}", df_fusion.head(Some(10)));
    println!("\n");

    Ok(())
}