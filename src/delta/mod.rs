use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use deltalake::parquet::file::properties::WriterProperties;
use deltalake::{
    DeltaTable,
    writer::{DeltaWriter, RecordBatchWriter, WriteMode},
};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::handlers::{AppError, AppResult, DeltaError};

// ‑‑ in tests we automatically get `MockDeltaIo` thanks to `automock`
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait DeltaIo: Send + Sync {
    /// Flush one Arrow batch, return number of `add` actions committed.
    async fn write_batch(&self, batch: RecordBatch, mode: WriteMode) -> AppResult<()>;
}

/// Production implementation --------------------------------------------------
pub struct DeltaWriterIo {
    table: Arc<Mutex<DeltaTable>>,
    properties: WriterProperties,
}

impl DeltaWriterIo {
    pub fn new(table: Arc<Mutex<DeltaTable>>, properties: WriterProperties) -> Self {
        Self { table, properties }
    }
}

#[async_trait]
impl DeltaIo for DeltaWriterIo {
    async fn write_batch(&self, batch: RecordBatch, mode: WriteMode) -> AppResult<()> {
        let mut tbl_guard = self.table.lock().await;

        let mut writer = RecordBatchWriter::for_table(&tbl_guard)
            .map_err(|e| {
                log::error!("Failed to create RecordBatchWriter: {e}");
                AppError::Delta(DeltaError::TableError(format!(
                    "Failed to create RecordBatchWriter: {e}"
                )))
            })?
            .with_writer_properties(self.properties.clone());

        // write the batch to the table
        writer.write_with_mode(batch, mode).await.map_err(|e| {
            log::error!("Failed to write batch to Delta table: {e}");
            AppError::Delta(DeltaError::TableError(e.to_string()))
        })?;

        // flush and commit the batch to the table
        let adds = writer.flush_and_commit(&mut tbl_guard).await.map_err(|e| {
            log::error!("Failed to flush and commit batch to Delta table: {e}");
            AppError::Delta(DeltaError::TableError(format!(
                "Failed to flush and commit batch to Delta table: {e}"
            )))
        })?;
        log::info!("Flush completed. {} adds written", adds);

        Ok(())
    }
}
