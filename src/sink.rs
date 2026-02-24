use std::fs::{self, File};
use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use anyhow::{anyhow, Result};

/// A trait defining where the Arrow memory batches ultimately go.
/// This decouples the macro's memory buffering from the storage layer.
pub trait FletchSink {
    /// Consumes a finalized Arrow RecordBatch and writes it to the underlying storage.
    fn write_batch(&mut self, batch: RecordBatch) -> Result<()>;

    /// Flushes any remaining bytes and cleanly closes the connection/file.
    fn close(&mut self) -> Result<()>;
}

/// A local file storage backend that writes Fletch streams to Snappy-compressed Parquet files.
pub struct ParquetSink {
    writer: Option<ArrowWriter<File>>,
}

impl ParquetSink {
    /// Creates a new Parquet file at the given path, automatically creating parent directories.
    pub fn try_new(file_path: impl Into<PathBuf>, schema: Arc<Schema>) -> Result<Self> {
        let file_path = file_path.into();
        if let Some(parent) = file_path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }
        let file = File::create(&file_path)?;
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let writer = ArrowWriter::try_new(file, schema, Some(props))?;
        Ok(Self {
            writer: Some(writer)
        })
    }
}

impl FletchSink for ParquetSink {
    fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        if let Some(writer) = &mut self.writer {
            writer.write(&batch)?;
            Ok(())
        } else {
            Err(anyhow!("Cannot write batch: ParquetSink is already closed"))
        }
    }

    fn close(&mut self) -> Result<()> {
        if let Some(writer) = self.writer.take() {
            writer.close()?;
        }
        Ok(())
    }
}