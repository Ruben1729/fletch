use std::collections::HashMap;
use std::fs::File;
use tempfile::tempdir;
use arrow::util::pretty::print_batches;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use fletch::{fletch_schema, ParquetSink};

// Define the schema
fletch_schema! {
    AccelerometerTelemetry {
        accel_x: f64,
        accel_y: f64,
        accel_z: f64,
        supply_voltage: f64,
        current_ma: f64,
    }
}

fn main() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let file_path = dir.path().join("data.parquet");
    let mut metadata = HashMap::new();
    metadata.insert("project".to_string(), "drone_stabilization".to_string());
    metadata.insert("dut".to_string(), "IMU_MPU6050_Rev2".to_string());
    metadata.insert("accel_rate".to_string(), "1000Hz".to_string());
    metadata.insert("power_rate".to_string(), "100Hz".to_string());
    let mut stream = AccelerometerTelemetry::try_new(metadata, |schema| {
        Ok(Box::new(ParquetSink::try_new(&file_path, schema)?))
    })?;

    println!("Generating 100,000 samples at mixed rates...");

    let start_ts: i64 = 1718000000000;
    let num_samples = 100_000;
    for i in 0..num_samples {
        let current_ts = start_ts + i; // 1ms per iteration
        let t = i as f64 * 0.01;

        // ACCELEROMETER: Sampled every 1ms (1kHz)
        stream.accel_x(current_ts, t.sin() * 2.0)?;
        stream.accel_y(current_ts, t.cos() * 2.0)?;
        stream.accel_z(current_ts, 9.81 + (t * 5.0).sin())?;

        // POWER METRICS: Sampled every 10ms (100Hz)
        if i % 10 == 0 {
            let voltage = 3.3 + (i % 100) as f64 * 0.001;
            let current = 1.2 + ((t.sin() * 2.0).abs() * 0.05);
            stream.supply_voltage(current_ts, voltage)?;
            stream.current_ma(current_ts, current)?;
        }
    }

    stream.close()?;
    println!("Successfully wrote sparse telemetry to temporary file.\n");


    // Read the Parquet file back into memory
    println!("Reading the file back to verify sparse data layout:");
    let file = File::open(&file_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mut reader = builder.build()?;
    if let Some(record_batch_result) = reader.next() {
        let batch = record_batch_result?;
        let top_10 = batch.slice(0, 20);
        print_batches(&[top_10])?;
    }

    Ok(())
}