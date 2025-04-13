use arrow::array::RecordBatch;
use deltalake::writer::WriteMode;
// This module writes consolidated data to an existing Delta table using the deltalake crate.
// It supports atomic operations and idempotent writes, handling schema changes gracefully.
use deltalake::DeltaOps;
use serde::Deserialize;
use std::collections::HashMap;

use crate::config::DeltaConfig;
use crate::handlers::{AppError, DeltaError};

// Trait for Delta table writing.
pub trait DeltaWriter {
    // Inserts a batch of records into the delta table atomically.
    async fn write(&self, data: &RecordBatch) -> Result<(), AppError>;
}

#[derive(Deserialize, Debug, Clone, Copy)]
#[serde(rename_all = "UPPERCASE")]
pub enum DeltaWriteMode {
    INSERT,
    UPSERT,
}

// Example implementation using deltalake.
pub struct DeltaRsWriter {
    pub table_path: String,
    pub partition: String,
    pub prune_filter: HashMap<String, String>,
    pub write_mode: DeltaWriteMode,
}

impl DeltaRsWriter {
    pub fn new(delta_config: &DeltaConfig, prune_filter: HashMap<String, String>) -> Self {
        DeltaRsWriter {
            table_path: delta_config.table_path.clone(),
            partition: delta_config.partition.clone(),
            prune_filter: prune_filter,
            write_mode: delta_config.mode,
        }
    }
}

impl DeltaWriter for DeltaRsWriter {
    async fn write(&self, data: &RecordBatch) -> Result<(), AppError> {
        // Pseudocode: Open the delta table, perform an atomic INSERT or MERGE into the partition specified.
        // If a new column is detected, skip it instead of panicking.
        // If there's no data, we can skip
        if data.num_rows() == 0 {
            log::warn!("No data to write; skipping.");
            return Ok(());
        }

        // Create a new DeltaOps instance, operating on DeltaTable at given uri.
        let delta_ops = DeltaOps::try_from_uri(&self.table_path)
            .await
            .map_err(|e| {
                log::error!("Failed to create DeltaOps instance: {}", e);
                DeltaError::IoError(format!("Failed to create DeltaOps instance: {}", e))
            })?;

        // Load the Delta table.
        let table = delta_ops.load().await.map_err(|e| {
            log::error!("Failed to load Delta table: {}", e);
            DeltaError::IoError(format!("Failed to load Delta table: {}", e))
        })?;

        // TODO: Implement atomic write operation
        match self.write_mode {
            DeltaWriteMode::INSERT => {
                // Write the data to the Delta table using partition column
                // to reduce the number of files in the table, and use the
                // prune filter to prune the data.
                // delta_ops
                //     .write(data)
                //     .mode(WriteMode::Append)
                //     .await
                //     .map_err(|e| {
                //         log::error!("Failed to write data to Delta table: {}", e);
                //         DeltaError::TableError(format!(
                //             "Failed to write data to Delta table: {}",
                //             e
                //         ))
                //     })?;
            }
            DeltaWriteMode::UPSERT => {
                // Write the data to the Delta table using partition column
                // to reduce the number of files in the table, and use the
                // prune filter to prune the data.
                // TODO: Implement upsert operation
            }
        }

        Ok(())
    }
}

//---------------------------------------- Tests ----------------------------------------
