use anyhow::{anyhow, Result};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat};
use iceberg::transaction::{Transaction, ApplyTransactionAction};
use object_store::{path::Path as ObjectPath, ObjectStoreExt, PutPayload};
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::sync::{mpsc, Arc};
use std::thread::{self, JoinHandle};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs::File as TokioFile;
use tokio::runtime::Runtime;

use crate::config::FletchConfig;

pub struct BackgroundSink {
    sender: mpsc::SyncSender<Option<RecordBatch>>,
    worker_handle: Option<JoinHandle<Result<()>>>,
}

impl BackgroundSink {
    pub fn spawn(config: FletchConfig, schema: Arc<Schema>) -> Result<Self> {
        let (sender, receiver) = mpsc::sync_channel::<Option<RecordBatch>>(100);

        let worker_handle = thread::spawn(move || -> Result<()> {
            let rt = Runtime::new()?;

            rt.block_on(async move {
                let path = ObjectPath::from(config.store_path.as_str());
                let timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos();
                let temp_path = std::env::temp_dir()
                    .join(format!("fletch_spool_{}.parquet", timestamp));
                let file = TokioFile::create(&temp_path)
                    .await
                    .map_err(|e| anyhow!("{}", e))?;
                let props = WriterProperties::builder()
                    .set_compression(Compression::SNAPPY)
                    .build();
                let mut writer = AsyncArrowWriter::try_new(file, schema.clone(), Some(props))
                    .map_err(|e| anyhow!("{}", e))?;
                let mut record_count = 0;
                while let Ok(Some(batch)) = receiver.recv() {
                    record_count += batch.num_rows() as u64;
                    writer.write(&batch)
                        .await
                        .map_err(|e| anyhow!("{}", e))?;
                }
                let metadata = writer.close().await.map_err(|e| anyhow!("{}", e))?;
                let data = tokio::fs::read(&temp_path).await.map_err(|e| anyhow!("{}", e))?;
                let file_size = data.len() as u64;
                let payload = PutPayload::from(data);
                config.store.put(&path, payload).await.map_err(|e| anyhow!("{}", e))?;
                let _ = tokio::fs::remove_file(&temp_path).await;
                let mut column_sizes: std::collections::HashMap<i32, u64> = std::collections::HashMap::new();
                let mut value_counts: std::collections::HashMap<i32, u64> = std::collections::HashMap::new();
                let mut null_value_counts: std::collections::HashMap<i32, u64> = std::collections::HashMap::new();
                for row_group in metadata.row_groups() {
                    for (col_idx, column) in row_group.columns().iter().enumerate() {
                        let field_id = (col_idx + 1) as i32;
                        *column_sizes.entry(field_id).or_insert(0) += column.compressed_size() as u64;
                        *value_counts.entry(field_id).or_insert(0) += row_group.num_rows() as u64;
                        if let Some(stats) = column.statistics() {
                            if let Some(null_count) = stats.null_count_opt() {
                                *null_value_counts.entry(field_id).or_insert(0) += null_count;
                            }
                        }
                    }
                }
                let data_file = DataFileBuilder::default()
                    .content(DataContentType::Data)
                    .file_path(config.file_uri.clone())
                    .file_format(DataFileFormat::Parquet)
                    .record_count(record_count)
                    .file_size_in_bytes(file_size)
                    .column_sizes(column_sizes)
                    .value_counts(value_counts)
                    .null_value_counts(null_value_counts)
                    .build()
                    .map_err(|e| anyhow!("{}", e))?;
                let tx = Transaction::new(&config.table);
                let action = tx.fast_append().add_data_files(vec![data_file]);
                let tx = action.apply(tx).map_err(|e| anyhow!("{}", e))?;
                tx.commit(config.catalog.as_ref()).await.map_err(|e| anyhow!("{}", e))?;
                Ok::<(), anyhow::Error>(())
            })
        });

        Ok(Self {
            sender,
            worker_handle: Some(worker_handle),
        })
    }

    pub fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        self.sender
            .send(Some(batch))
            .map_err(|e| anyhow!("Background worker died: {}", e))
    }

    pub fn close(&mut self) -> Result<()> {
        self.sender.send(None).ok();
        if let Some(handle) = self.worker_handle.take() {
            match handle.join() {
                Ok(res) => res?,
                Err(_) => return Err(anyhow!("Storage worker thread panicked")),
            }
        }
        Ok(())
    }
}