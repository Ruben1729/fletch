use anyhow::Result;
use arrow::array::{Array, Float64Array, Int64Array};
use fletch::{fletch_schema, FletchType};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use tempfile::tempdir;

fletch_schema! {
    TestTelemetry {
        sensor_a: f64,
        sensor_b: f64,
    }
}

fn read_parquet_batch(dir: &std::path::Path, run_id: &str) -> Result<arrow::record_batch::RecordBatch> {
    let file_path = dir.join(format!("TestTelemetry/data/{}.parquet", run_id));
    let file = File::open(&file_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mut reader = builder.build()?;
    let batch = reader.next().unwrap()?;
    Ok(batch)
}

#[tokio::test]
async fn test_out_of_order_timestamps_are_sorted() -> Result<()> {
    let dir = tempdir()?;
    let uri = format!("file:///{}", dir.path().to_string_lossy().replace("\\", "/"));
    let run_id = "test_run_out_of_order";
    let mut stream = TestTelemetry::try_new(&uri, run_id).await?;
    stream.sensor_a(150, 1.5)?;
    stream.sensor_a(100, 1.0)?;
    stream.sensor_a(200, 2.0)?;
    stream.sensor_a(50,  0.5)?;
    stream.close()?;
    let batch = read_parquet_batch(dir.path(), run_id)?;
    let ts_array = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let val_array = batch.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(batch.num_rows(), 4);
    assert_eq!(ts_array.value(0), 50);
    assert_eq!(ts_array.value(1), 100);
    assert_eq!(ts_array.value(2), 150);
    assert_eq!(ts_array.value(3), 200);
    assert_eq!(val_array.value(0), 0.5);
    assert_eq!(val_array.value(1), 1.0);
    assert_eq!(val_array.value(2), 1.5);
    assert_eq!(val_array.value(3), 2.0);
    Ok(())
}

#[tokio::test]
async fn test_sparse_data_with_nulls() -> Result<()> {
    let dir = tempdir()?;
    let uri = format!("file:///{}", dir.path().to_string_lossy().replace("\\", "/"));
    let run_id = "test_run_sparse";
    let mut stream = TestTelemetry::try_new(&uri, run_id).await?;
    stream.sensor_a(100, 10.0)?;
    stream.sensor_b(100, 20.0)?;
    stream.sensor_a(110, 11.0)?;
    stream.sensor_b(120, 22.0)?;
    stream.close()?;
    let batch = read_parquet_batch(dir.path(), run_id)?;
    let a_array = batch.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
    let b_array = batch.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
    assert!(a_array.is_valid(1));
    assert!(b_array.is_null(1));
    assert!(a_array.is_null(2));
    assert!(b_array.is_valid(2));
    Ok(())
}

#[tokio::test]
async fn test_duplicate_timestamp_overwrites() -> Result<()> {
    let dir = tempdir()?;
    let uri = format!("file:///{}", dir.path().to_string_lossy().replace("\\", "/"));
    let run_id = "test_run_duplicates";

    let mut stream = TestTelemetry::try_new(&uri, run_id).await?;
    stream.sensor_a(100, 1.0)?;
    stream.sensor_a(100, 9.9)?;
    stream.sensor_a(110, 2.0)?;
    stream.close()?;
    let batch = read_parquet_batch(dir.path(), run_id)?;
    let ts_array = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let val_array = batch.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(ts_array.value(0), 100);
    assert_eq!(val_array.value(0), 9.9);
    Ok(())
}

#[tokio::test]
async fn test_empty_flush() -> Result<()> {
    let dir = tempdir()?;
    let uri = format!("file:///{}", dir.path().to_string_lossy().replace("\\", "/"));
    let run_id = "test_run_empty";
    let stream = TestTelemetry::try_new(&uri, run_id).await?;
    let result = stream.close();
    assert!(result.is_ok(), "Closing an empty stream should not fail");
    let file_path = dir.path().join(format!("TestTelemetry/data/{}.parquet", run_id));
    assert!(!file_path.exists(), "No parquet file should be written for empty streams");
    Ok(())
}